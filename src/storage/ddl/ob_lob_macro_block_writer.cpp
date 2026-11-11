/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

ERRSIM_POINT_DEF(FAIL_LOB_PIECE_WRITE);
ERRSIM_POINT_DEF(FAIL_LOB_STAGING_REPLAY_AFTER_APPEND);

int ObLobAuxMacroSeqGenerator::bind(ObMacroIncSeqGenerator &shared_seq_generator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(shared_seq_generator_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob macro sequence generator is bound twice", K(ret), K(*this));
  } else {
    shared_seq_generator_ = &shared_seq_generator;
    current_ = -1;
    reserved_current_ = -1;
    reserved_next_ = -1;
    has_reserved_next_ = false;
  }
  return ret;
}

void ObLobAuxMacroSeqGenerator::reset()
{
  shared_seq_generator_ = nullptr;
  current_ = -1;
  reserved_current_ = -1;
  reserved_next_ = -1;
  has_reserved_next_ = false;
}

int ObLobAuxMacroSeqGenerator::init(const ObMacroSeqParam &seq_param)
{
  UNUSED(seq_param);
  return OB_NOT_SUPPORTED;
}

int ObLobAuxMacroSeqGenerator::get_next(int64_t &seq_val)
{
  int ret = OB_SUCCESS;
  seq_val = -1;
  if (OB_ISNULL(shared_seq_generator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob macro sequence generator is not bound", K(ret), K(*this));
  } else if (has_reserved_next_) {
    if (OB_UNLIKELY(reserved_current_ != current_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("reserved lob macro sequence does not match current sequence",
               K(ret), K(*this));
    } else {
      // 同一列级 writer 切换到预初始化缓冲区时只消费预留序号，
      // 不能为同一个宏块再次向共享 generator 申请。
      seq_val = reserved_next_;
      current_ = seq_val;
      reserved_current_ = -1;
      reserved_next_ = -1;
      has_reserved_next_ = false;
    }
  } else if (OB_FAIL(shared_seq_generator_->get_next(seq_val))) {
    LOG_WARN("allocate next lob macro sequence failed", K(ret), K(*this));
  } else {
    current_ = seq_val;
  }
  return ret;
}

int ObLobAuxMacroSeqGenerator::preview_next(
    const int64_t current_val,
    int64_t &next_val) const
{
  int ret = OB_SUCCESS;
  next_val = -1;
  if (OB_ISNULL(shared_seq_generator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob macro sequence generator is not bound", K(ret), K(*this));
  } else if (OB_UNLIKELY(current_val != current_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("preview base does not match current lob macro sequence",
             K(ret), K(current_val), K(*this));
  } else if (has_reserved_next_) {
    if (OB_UNLIKELY(reserved_current_ != current_val)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("reserved lob macro sequence does not match preview base",
               K(ret), K(current_val), K(*this));
    } else {
      // 同一缓冲区重复预取时返回同一个序号，避免重复占用共享 sequence。
      next_val = reserved_next_;
    }
  } else if (OB_FAIL(shared_seq_generator_->get_next(next_val))) {
    LOG_WARN("reserve next lob macro sequence failed", K(ret), K(current_val), K(*this));
  } else {
    // preview 必须立即占用共享序号，防止其它列级 writer 取得相同的下一个序号。
    reserved_current_ = current_val;
    reserved_next_ = next_val;
    has_reserved_next_ = true;
  }
  return ret;
}

void ObLobMacroBlockWriter::ObLobAuxRunState::reset(
    const uint64_t expected_lob_id_start,
    const uint64_t expected_lob_id_end)
{
  expected_lob_id_start_ = expected_lob_id_start;
  expected_lob_id_end_ = expected_lob_id_end;
  last_written_lob_id_ = 0;
  has_last_written_lob_id_ = false;
}

int ObLobMacroBlockWriter::ObLobAuxRunState::check_and_record(
    const ObLobId &lob_id,
    const int64_t lob_col_idx)
{
  int ret = OB_SUCCESS;
  const uint64_t auto_inc_id = lob_id.get_auto_inc_id();
  if (OB_UNLIKELY(!lob_id.is_valid()
      || lob_id.get_column_idx() != static_cast<uint64_t>(lob_col_idx)
      || expected_lob_id_start_ > expected_lob_id_end_
      || auto_inc_id < expected_lob_id_start_
      || auto_inc_id > expected_lob_id_end_
      || (has_last_written_lob_id_ && auto_inc_id < last_written_lob_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob id does not match current auxiliary run",
             K(ret), K(lob_id), K(lob_col_idx), K(*this));
  } else {
    last_written_lob_id_ = auto_inc_id;
    has_last_written_lob_id_ = true;
  }
  return ret;
}

ObLobMacroBlockWriter::ObLobMacroBlockWriter()
  : is_inited_(false),
    slice_idx_(0), lob_inrow_threshold_(0),
    lob_id_interval_start_(0), lob_id_interval_end_(0),
    lob_column_count_(0), use_staging_(false),
    lob_arena_("lob_write_iter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    lob_write_iter_(), staging_row_buffer_("LobStageRow"),
    replay_macro_writer_(nullptr), staging_dir_id_(-1),
    column_write_ctxs_(), lob_col_idx_to_write_ctx_idx_(),
    total_lob_cell_count_(0), inrow_lob_cell_count_(0)
{
  lob_id_cache_.set(1/*start*/, 0/*end*/); // to calculate remain count correctly
}

ObLobMacroBlockWriter::~ObLobMacroBlockWriter()
{
  destroy_staging_stores();
  destroy_macro_block_writers();
}

int ObLobMacroBlockWriter::init(const ObWriteMacroParam &param,
                                const ObTabletID &data_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(
        !param.is_valid()
        || !data_tablet_id.is_valid()
        || data_tablet_id != param.tablet_id_
        || !param.lob_meta_tablet_id_.is_valid()
        || !param.lob_piece_tablet_id_.is_valid()
        || OB_ISNULL(param.ddl_table_schema_.lob_meta_storage_schema_)
        || OB_ISNULL(param.ddl_table_schema_.lob_piece_storage_schema_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(data_tablet_id));
  } else {
    lob_meta_tablet_id_ = param.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = param.lob_piece_tablet_id_;
    slice_idx_ = param.slice_idx_;
    lob_column_count_ = param.ddl_table_schema_.lob_column_idxs_.count();
    use_staging_ = lob_column_count_ > 4;
    lob_id_cache_.tablet_id_ = lob_meta_tablet_id_;
    lob_inrow_threshold_ = param.ddl_table_schema_.table_item_.lob_inrow_threshold_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    param_ = param;
    // init lob id generator if need
    if (lob_column_count_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lob column count is invalid", K(ret), K(lob_column_count_), K(param));
    } else if (param.slice_count_ > 0) {
      if (slice_idx_ >= param.slice_count_) { // idem mode, for table with primary key
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid slice idx", K(ret), K(slice_idx_), K(param));
      } else if (OB_FAIL(lob_id_generator_.init(slice_idx_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // start
                                                ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // interval
                                                param.slice_count_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL))) { // step
        LOG_WARN("init lob id generator failed", KR(ret), K(param));
      }
    }

    // init lob meta row
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lob_meta_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT + ObLobMetaUtil::SKIP_INVALID_COLUMN))) {
        LOG_WARN("init lob meta row failed", K(ret));
      } else if (OB_FAIL(lob_piece_row_.init(
          ObLobPieceUtil::LOB_PIECE_COLUMN_CNT + ObLobPieceUtil::SKIP_INVALID_COLUMN))) {
        LOG_WARN("init lob piece row failed", K(ret));
      } else {
        lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 1].set_int(-param.snapshot_version_);
        lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(0);
        lob_meta_row_.set_trans_id(param_.tx_info_.trans_id_);
        lob_meta_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        lob_meta_row_.mvcc_row_flag_.set_compacted_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_first_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_last_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_uncommitted_row(
            param_.tx_info_.trans_id_.is_valid() && is_incremental_minor_direct_load(param_.direct_load_type_));

        lob_piece_row_.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID + 1].set_int(
            -param.snapshot_version_);
        lob_piece_row_.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID + 2].set_int(0);
        lob_piece_row_.set_trans_id(param_.tx_info_.trans_id_);
        lob_piece_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        lob_piece_row_.mvcc_row_flag_.set_compacted_multi_version_row(true);
        lob_piece_row_.mvcc_row_flag_.set_first_multi_version_row(true);
        lob_piece_row_.mvcc_row_flag_.set_last_multi_version_row(true);
        lob_piece_row_.mvcc_row_flag_.set_uncommitted_row(
            param_.tx_info_.trans_id_.is_valid()
            && is_incremental_minor_direct_load(param_.direct_load_type_));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(init_shared_macro_seq_generator(
        lob_meta_tablet_id_, meta_shared_seq_generator_, meta_start_sequence_))) {
      LOG_WARN("init shared lob meta macro sequence generator failed", K(ret), K(param));
    } else if (OB_SUCC(ret) && OB_FAIL(init_shared_macro_seq_generator(
        lob_piece_tablet_id_, piece_shared_seq_generator_, piece_start_sequence_))) {
      LOG_WARN("init shared lob piece macro sequence generator failed", K(ret), K(param));
    } else if (OB_SUCC(ret) && OB_FAIL(init_column_write_ctxs())) {
      LOG_WARN("init lob column write contexts failed", K(ret), K(param));
    } else if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::write(const ObColumnSchemaItem &column_schema, const int64_t lob_col_idx,
                                 ObIAllocator &row_allocator, blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  bool is_target_outrow = false;
  bool skip_meta_write = false;
  transaction::ObTxSEQ target_lob_seq_no;
  ObLobColumnWriteCtx *column_ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(get_column_write_ctx(lob_col_idx, column_ctx))) {
    LOG_WARN("get lob column write context failed", K(ret), K(lob_col_idx));
  } else {
    lob_write_iter_.reset();
    lob_arena_.reuse();
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = lob_inrow_threshold_;
    lob_storage_param.is_index_table_ = param_.ddl_table_schema_.table_item_.is_index_table_;
    lob_storage_param.is_rowkey_col_ = column_schema.is_rowkey_column_;
    // External references consume a LOB ID and a separate sequence from the same cache.
    const int64_t required_id_count = 2;  // ObLobID, External Lob Marco Seq ID
    if (lob_id_cache_.remain_count() < lob_column_count_ * required_id_count
        && OB_FAIL(switch_lob_id_cache())) {
      LOG_WARN("switch lob id cache failed", K(ret), K(lob_id_cache_), K(lob_column_count_), K(lob_id_generator_));
    } else {
      const int64_t timeout_ts = ObTimeUtility::fast_current_time()
          + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
      if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(row_allocator,
                                                             lob_arena_/*lob_allocator*/,
                                                             param_.tx_info_.tx_desc_,
                                                             lob_id_cache_,
                                                             ls_id_,
                                                             tablet_id_,
                                                             lob_meta_tablet_id_,
                                                             lob_piece_tablet_id_,
                                                             lob_col_idx,
                                                             column_schema.col_type_.get_type(),
                                                             column_schema.col_type_.get_collation_type(),
                                                             lob_storage_param,
                                                             datum,
                                                             timeout_ts,
                                                             param_.ddl_table_schema_.src_tenant_id_,
                                                             transaction::ObTxSEQ::cast_from_int(
                                                                 param_.tx_info_.seq_no_),
                                                             target_lob_seq_no,
                                                             lob_write_iter_,
                                                             is_target_outrow))) {
        LOG_WARN("insert lob column failed", K(ret), K(param_));
      }
    }
    ++total_lob_cell_count_;
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, lob_storage_param.inrow_threshold_) OB_SUCCESS;
      if (OB_FAIL(ret) && param_.tablet_id_.is_user_tablet()) {
        LOG_ERROR("due to error injection, lob meta not being inserted into the auxiliary table",
                  K(ret), K(param_));
        ret = OB_SUCCESS;
        skip_meta_write = true;
        ++inrow_lob_cell_count_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_target_outrow) {
      if (!skip_meta_write) {
        ++inrow_lob_cell_count_;
      }
    } else if (OB_FAIL(drain_lob_iter(*column_ctx, target_lob_seq_no, skip_meta_write))) {
      LOG_WARN("drain lob write iterator failed",
               K(ret), K(lob_col_idx), K(target_lob_seq_no), KPC(column_ctx));
    } else {
      // meta/piece 全部写入成功后，主表 datum 才能引用新的 out-row LOB。
      ObLobLocatorV2 pending_locator;
      ObString disk_locator;
      if (OB_FAIL(lob_write_iter_.get_pending_locator(pending_locator))) {
        LOG_WARN("get pending lob locator failed", K(ret), K(lob_col_idx));
      } else if (OB_FAIL(pending_locator.get_disk_locator(disk_locator))) {
        LOG_WARN("get disk lob locator failed", K(ret), K(pending_locator));
      } else if (OB_UNLIKELY(disk_locator.length() < sizeof(ObLobCommon))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("disk lob locator is too short", K(ret), K(disk_locator.length()));
      } else {
        ObLobCommon *lob_common =
            reinterpret_cast<ObLobCommon *>(const_cast<char *>(disk_locator.ptr()));
        datum.set_lob_data(*lob_common, disk_locator.length());
      }
    }
  }

  return ret;
}

int ObLobMacroBlockWriter::init_shared_macro_seq_generator(
    const ObTabletID &tablet_id,
    ObMacroIncSeqGenerator &seq_generator,
    ObMacroDataSeq &start_sequence)
{
  int ret = OB_SUCCESS;
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  if (OB_FAIL(ObDDLUtil::init_macro_block_seq(
      param_.direct_load_type_, tablet_id, slice_idx_, start_sequence))) {
    LOG_WARN("init lob macro sequence failed",
             K(ret), K(tablet_id), K(slice_idx_), K(param_));
  } else {
    seq_param.start_ = start_sequence.macro_data_seq_;
#ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()
        && is_incremental_major_direct_load(param_.direct_load_type_)
        && OB_FAIL(ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
            slice_idx_, seq_param.start_, seq_param.end_))) {
      LOG_WARN("calculate lob inc major sequence end failed",
               K(ret), K(tablet_id), K(slice_idx_), K(seq_param));
    }
#endif
    if (OB_SUCC(ret) && OB_FAIL(seq_generator.init(seq_param))) {
      LOG_WARN("init shared lob auxiliary macro sequence generator failed",
               K(ret), K(tablet_id), K(slice_idx_), K(seq_param));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::init_column_write_ctxs()
{
  int ret = OB_SUCCESS;
  const int64_t store_column_count = param_.ddl_table_schema_.column_items_.count();
  if (OB_UNLIKELY(lob_column_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob column count is invalid", K(ret), K(lob_column_count_));
  } else if (OB_UNLIKELY(store_column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("store column count is invalid", K(ret), K(store_column_count));
  } else if (OB_FAIL(column_write_ctxs_.prepare_allocate(lob_column_count_))) {
    LOG_WARN("allocate lob column write contexts failed", K(ret), K(lob_column_count_));
  } else if (OB_FAIL(lob_col_idx_to_write_ctx_idx_.prepare_allocate(store_column_count))) {
    LOG_WARN("allocate lob column write context mapping failed", K(ret), K(store_column_count));
  } else {
    for (int64_t i = 0; i < store_column_count; ++i) {
      lob_col_idx_to_write_ctx_idx_.at(i) = OB_INVALID_INDEX_INT64;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_count_; ++i) {
      ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
      const int64_t lob_col_idx = param_.ddl_table_schema_.lob_column_idxs_.at(i);
      if (OB_UNLIKELY(lob_col_idx < 0
          || lob_col_idx >= store_column_count
          || (i > 0
              && param_.ddl_table_schema_.lob_column_idxs_.at(i - 1) >= lob_col_idx))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob column writer identity", K(ret), K(i), K(lob_col_idx),
                 K(store_column_count));
      } else {
        column_ctx.lob_col_idx_ = lob_col_idx;
        lob_col_idx_to_write_ctx_idx_.at(lob_col_idx) = i;
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::get_column_write_ctx(
    const int64_t lob_col_idx,
    ObLobColumnWriteCtx *&column_ctx)
{
  int ret = OB_SUCCESS;
  column_ctx = nullptr;
  int64_t write_ctx_idx = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(lob_col_idx < 0
      || lob_col_idx >= lob_col_idx_to_write_ctx_idx_.count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("lob column write context does not exist", K(ret), K(lob_col_idx),
             "mapping_count", lob_col_idx_to_write_ctx_idx_.count());
  } else {
    write_ctx_idx = lob_col_idx_to_write_ctx_idx_.at(lob_col_idx);
    if (OB_UNLIKELY(write_ctx_idx < 0 || write_ctx_idx >= column_write_ctxs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob column write context mapping", K(ret), K(lob_col_idx),
               K(write_ctx_idx), "write_ctx_count", column_write_ctxs_.count());
    } else if (OB_UNLIKELY(column_write_ctxs_.at(write_ctx_idx).lob_col_idx_ != lob_col_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob column write context mapping does not match", K(ret), K(lob_col_idx),
               K(write_ctx_idx), K(column_write_ctxs_.at(write_ctx_idx).lob_col_idx_));
    } else {
      column_ctx = &column_write_ctxs_.at(write_ctx_idx);
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::drain_lob_iter(
    ObLobColumnWriteCtx &column_ctx,
    const transaction::ObTxSEQ &target_lob_seq_no,
    const bool skip_meta_write)
{
  int ret = OB_SUCCESS;
  int64_t meta_result_count = 0;
  bool skip_remaining_meta = skip_meta_write;
  ObLobWriteResult result;
  const bool is_incremental = is_incremental_direct_load(param_.direct_load_type_);
  if (OB_UNLIKELY(is_incremental != target_lob_seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("target lob sequence does not match direct load type",
             K(ret), K(is_incremental), K(target_lob_seq_no), K(param_));
  } else {
    const int64_t seq_no = target_lob_seq_no.is_valid()
                           ? static_cast<int64_t>(target_lob_seq_no.cast_to_int())
                           : 0;
    // DDL 框架序号只标识 DDL KV，辅助行使用当前 LOB 单独申请的子序号。
    lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-seq_no);
    lob_piece_row_.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID + 2].set_int(-seq_no);
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(lob_write_iter_.get_next_row(result))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next lob write result failed", K(ret), K(column_ctx));
      }
    } else if (result.is_piece()) {
      if (OB_UNLIKELY(result.piece_info_.lob_col_idx_ != column_ctx.lob_col_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob piece column does not match writer", K(ret), K(result), K(column_ctx));
      } else if (OB_FAIL(append_piece_row(column_ctx, result.piece_info_))) {
        LOG_WARN("append lob piece row failed", K(ret), K(result), K(column_ctx));
      }
    } else if (result.is_meta()) {
      if (OB_UNLIKELY(result.meta_info_.lob_col_idx_ != column_ctx.lob_col_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob meta column does not match writer", K(ret), K(result), K(column_ctx));
      } else {
        if (!skip_remaining_meta && meta_result_count > 0) {
          ret = OB_E(
              EventTable::EN_LOB_META_DML_FAILED, lob_inrow_threshold_ + 1) OB_SUCCESS;
          if (OB_FAIL(ret) && param_.tablet_id_.is_user_tablet()) {
            LOG_ERROR("due to error injection, make err len meta data", K(ret), K(param_));
            ret = OB_SUCCESS;
            skip_remaining_meta = true;
          }
        }
        // 故障注入只跳过 meta 落盘，仍需 drain 完整 iterator 以完成 locator 构造。
        if (OB_FAIL(ret)) {
        } else if (!skip_remaining_meta
                   && OB_FAIL(append_meta_row(column_ctx, result.meta_info_))) {
          LOG_WARN("append lob meta row failed", K(ret), K(result), K(column_ctx));
        }
      }
      ++meta_result_count;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob write result", K(ret), K(result), K(column_ctx));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::append_meta_row(
    ObLobColumnWriteCtx &column_ctx,
    const ObLobMetaInfo &meta_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_ctx.meta_run_state_.check_and_record(
      meta_info.lob_id_, column_ctx.lob_col_idx_))) {
    LOG_WARN("record lob meta run failed", K(ret), K(meta_info), K(column_ctx));
  } else if (OB_FAIL(transform_lob_meta_row(meta_info))) {
    LOG_WARN("transform lob meta row failed", K(ret), K(meta_info));
  } else if (use_staging_) {
    if (OB_FAIL(append_staging_row(
        column_ctx, ObDirectLoadTabletTarget::LOB_META, lob_meta_row_))) {
      LOG_WARN("stage lob meta row failed", K(ret), K(lob_meta_row_), K(column_ctx));
    }
  } else if (OB_FAIL(prepare_macro_block_writer(
      column_ctx, ObDirectLoadTabletTarget::LOB_META))) {
    LOG_WARN("prepare lob meta macro block writer failed", K(ret), K(column_ctx));
  } else if (OB_FAIL(column_ctx.meta_macro_writer_->append_row(lob_meta_row_))) {
    LOG_WARN("append lob meta macro row failed", K(ret), K(lob_meta_row_));
  }
  return ret;
}

int ObLobMacroBlockWriter::append_piece_row(
    ObLobColumnWriteCtx &column_ctx,
    const ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(FAIL_LOB_PIECE_WRITE)) {
    ret = FAIL_LOB_PIECE_WRITE;
    LOG_WARN("inject lob piece write failure", K(ret), K(piece_info), K(column_ctx));
  } else if (OB_FAIL(column_ctx.piece_run_state_.check_and_record(
      piece_info.lob_id_, column_ctx.lob_col_idx_))) {
    LOG_WARN("record lob piece run failed", K(ret), K(piece_info), K(column_ctx));
  } else if (OB_FAIL(transform_lob_piece_row(piece_info))) {
    LOG_WARN("transform lob piece row failed", K(ret), K(piece_info));
  } else if (use_staging_) {
    if (OB_FAIL(append_staging_row(
        column_ctx, ObDirectLoadTabletTarget::LOB_PIECE, lob_piece_row_))) {
      LOG_WARN("stage lob piece row failed", K(ret), K(piece_info), K(column_ctx));
    }
  } else if (OB_FAIL(prepare_macro_block_writer(
      column_ctx, ObDirectLoadTabletTarget::LOB_PIECE))) {
    LOG_WARN("prepare lob piece macro block writer failed", K(ret), K(column_ctx));
  } else if (OB_FAIL(column_ctx.piece_macro_writer_->append_row(lob_piece_row_))) {
    LOG_WARN("append lob piece macro row failed", K(ret), K(piece_info));
  }
  return ret;
}

int ObLobMacroBlockWriter::transform_lob_meta_row(const ObLobMetaInfo &meta_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObLobMetaUtil::transform_from_info_to_row(
      meta_info, &lob_meta_row_, true/*with_extra_rowkey*/))) {
    LOG_WARN("transform lob meta row failed", K(ret), K(meta_info));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!lob_meta_row_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob meta row", KR(ret), K(lob_meta_row_));
    } else if (OB_FAIL(ObDDLUtil::check_null_and_length(false/*is_index_table*/,
                                                        false/*has_lob_rowkey*/,
                                                        false/*is_table_with_clustering_key*/,
                                                        ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT,
                                                        lob_meta_row_))) {
      LOG_WARN("fail to check rowkey null value and length in row", KR(ret), K(lob_meta_row_));
    }
  }

  return ret;
}

int ObLobMacroBlockWriter::transform_lob_piece_row(const ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObLobPieceUtil::transform_from_info_to_row(
      piece_info, &lob_piece_row_, true/*with_extra_rowkey*/))) {
    LOG_WARN("transform lob piece row failed", K(ret), K(piece_info));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!lob_piece_row_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob piece row", KR(ret), K(piece_info));
    } else if (OB_FAIL(ObDDLUtil::check_null_and_length(
        false/*is_index_table*/,
        false/*has_lob_rowkey*/,
        false/*is_table_with_clustering_key*/,
        ObLobPieceUtil::LOB_PIECE_SCHEMA_ROWKEY_COL_CNT,
        lob_piece_row_))) {
      LOG_WARN("fail to check rowkey null value and length in row", KR(ret), K(piece_info));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::append_staging_row(
    ObLobColumnWriteCtx &column_ctx,
    const ObDirectLoadTabletTarget target,
    const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore *staging_store = nullptr;
  ObDatum serialized_row;
  staging_row_buffer_.reuse();
  if (OB_UNLIKELY(!use_staging_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("lob staging is disabled", K(ret), K(target), K(column_ctx));
  } else if (OB_FAIL(get_or_create_staging_store(
      column_ctx, target, staging_store))) {
    LOG_WARN("get lob staging store failed", K(ret), K(target), K(column_ctx));
  } else if (OB_FAIL(staging_row_buffer_.write_serialize(row))) {
    LOG_WARN("serialize lob staging row failed", K(ret), K(target), K(row));
  } else {
    serialized_row.set_string(
        staging_row_buffer_.data(), static_cast<uint32_t>(staging_row_buffer_.length()));
    if (OB_FAIL(staging_store->add_row(&serialized_row, 1, 0, nullptr))) {
      LOG_WARN("append lob staging row failed", K(ret), K(target), K(column_ctx));
    } else if (OB_FAIL(spill_staging_store_if_needed(*staging_store))) {
      LOG_WARN("spill lob staging store failed", K(ret), K(target), K(column_ctx));
    }
  }
  // DatumStore 已深拷贝序列化数据。普通缓冲区跨行复用，避免逐行申请释放；
  // 异常大的缓冲区及时释放，避免其在整个 slice 生命周期内常驻。
  if (staging_row_buffer_.capacity() > LOB_STAGING_ROW_BUFFER_REUSE_LIMIT) {
    staging_row_buffer_.reset();
  } else {
    staging_row_buffer_.reuse();
  }
  return ret;
}

int ObLobMacroBlockWriter::get_or_create_staging_store(
    ObLobColumnWriteCtx &column_ctx,
    const ObDirectLoadTabletTarget target,
    ObChunkDatumStore *&staging_store)
{
  int ret = OB_SUCCESS;
  staging_store = nullptr;
  if (ObDirectLoadTabletTarget::LOB_META == target) {
    staging_store = column_ctx.meta_staging_store_;
  } else if (ObDirectLoadTabletTarget::LOB_PIECE == target) {
    staging_store = column_ctx.piece_staging_store_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob staging target", K(ret), K(target));
  }
  if (OB_SUCC(ret) && OB_ISNULL(staging_store)) {
    if (OB_ISNULL(staging_store = OB_NEW(
        ObChunkDatumStore, ObMemAttr(MTL_ID(), "LobStage"), "LobStage"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate lob staging store failed", K(ret), K(target), K(column_ctx));
    }
    if (OB_SUCC(ret) && OB_FAIL(staging_store->init(
        0, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID, "LobStage", true,
        0, LOB_STAGING_BLOCK_SIZE))) {
      LOG_WARN("init lob staging store failed", K(ret), K(target), K(column_ctx));
    }
    if (OB_SUCC(ret)) {
      // 关闭 DatumStore 内部的自动溢写阈值，由 append 后的单 store 水位统一控制。
      staging_store->set_mem_limit(0);
      if (staging_dir_id_ >= 0) {
        staging_store->set_dir_id(staging_dir_id_);
      } else if (OB_FAIL(staging_store->alloc_dir_id())) {
        LOG_WARN("allocate lob staging directory failed", K(ret), K(target), K(column_ctx));
      } else {
        staging_dir_id_ = staging_store->get_file_dir_id();
      }
    }
    if (OB_SUCC(ret)) {
      if (ObDirectLoadTabletTarget::LOB_META == target) {
        column_ctx.meta_staging_store_ = staging_store;
      } else {
        column_ctx.piece_staging_store_ = staging_store;
      }
    }
    if (OB_SUCCESS != ret && OB_NOT_NULL(staging_store)) {
      destroy_staging_store(staging_store);
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::spill_staging_store_if_needed(
    ObChunkDatumStore &staging_store)
{
  int ret = OB_SUCCESS;
  const int64_t memory_hold = staging_store.get_mem_hold();
  if (memory_hold > LOB_STAGING_STORE_MEMORY_LIMIT
      && OB_FAIL(staging_store.dump(false, true))) {
    LOG_WARN("dump lob staging store failed", K(ret), K(memory_hold));
  }
  return ret;
}

int ObLobMacroBlockWriter::prepare_macro_block_writer(
    ObLobColumnWriteCtx &column_ctx,
    const ObDirectLoadTabletTarget target)
{
  int ret = OB_SUCCESS;
  ObCgMacroBlockWriter **macro_block_writer = nullptr;
  ObMacroIncSeqGenerator *shared_seq_generator = nullptr;
  ObLobAuxMacroSeqGenerator *seq_generator = nullptr;
  ObLobAuxRunState *run_state = nullptr;
  const ObMacroDataSeq *start_sequence = nullptr;
  ObTabletID target_tablet_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDirectLoadTabletTarget::LOB_META == target) {
    macro_block_writer = use_staging_ ? &replay_macro_writer_
                                      : &column_ctx.meta_macro_writer_;
    shared_seq_generator = &meta_shared_seq_generator_;
    seq_generator = &column_ctx.meta_seq_generator_;
    run_state = &column_ctx.meta_run_state_;
    start_sequence = &meta_start_sequence_;
    target_tablet_id = lob_meta_tablet_id_;
  } else if (ObDirectLoadTabletTarget::LOB_PIECE == target) {
    macro_block_writer = use_staging_ ? &replay_macro_writer_
                                      : &column_ctx.piece_macro_writer_;
    shared_seq_generator = &piece_shared_seq_generator_;
    seq_generator = &column_ctx.piece_seq_generator_;
    run_state = &column_ctx.piece_run_state_;
    start_sequence = &piece_start_sequence_;
    target_tablet_id = lob_piece_tablet_id_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob auxiliary target", K(ret), K(target));
  }
  if (OB_SUCC(ret) && OB_ISNULL(*macro_block_writer)) {
    if (OB_ISNULL(*macro_block_writer =
        OB_NEW(ObCgMacroBlockWriter, ObMemAttr(MTL_ID(), "lob_mb_writer")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for lob macro block writer failed", K(ret), K(target));
    }
  }
  if (OB_SUCC(ret) && use_staging_ && (*macro_block_writer)->is_inited()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("lob replay macro block writer is still active",
        K(ret), K(target), K(column_ctx));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!(*macro_block_writer)->is_inited())) {
    ObITable::TableKey lob_table_key;
    lob_table_key.tablet_id_ = target_tablet_id;
    if (is_incremental_minor_direct_load(param_.direct_load_type_)) { // 增量
      lob_table_key.table_type_ = ObITable::MINI_SSTABLE;
      lob_table_key.scn_range_.start_scn_.convert_for_tx(1);
      lob_table_key.scn_range_.end_scn_.convert_for_tx(param_.snapshot_version_); // for logic version
    } else if (is_incremental_major_direct_load(param_.direct_load_type_)) { // 增量
      lob_table_key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
      lob_table_key.column_group_idx_ = 0;
      // slice idx is not the same order with the rowkey of lob. set slice idx 0 here to compare rowkey when merge major sstable
      lob_table_key.slice_range_.start_slice_idx_ = 0;
      lob_table_key.slice_range_.end_slice_idx_ = 0;
      lob_table_key.version_range_.snapshot_version_ = param_.snapshot_version_;
    } else { // 全量
      lob_table_key.table_type_ = ObITable::MAJOR_SSTABLE;
      lob_table_key.column_group_idx_ = 0;
      // slice idx is not the same order with the rowkey of lob. set slice idx 0 here to compare rowkey when merge major sstable
      lob_table_key.slice_range_.start_slice_idx_ = 0;
      lob_table_key.slice_range_.end_slice_idx_ = 0;
      lob_table_key.version_range_.snapshot_version_ = param_.snapshot_version_;
    }
    if (OB_UNLIKELY(lob_id_interval_start_ > lob_id_interval_end_
        || run_state->expected_lob_id_start_ != lob_id_interval_start_
        || run_state->expected_lob_id_end_ != lob_id_interval_end_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob id interval does not match current run",
               K(ret), K_(lob_id_interval_start), K_(lob_id_interval_end),
               KPC(run_state), K(column_ctx), K(target));
    } else if (OB_FAIL(seq_generator->bind(*shared_seq_generator))) {
      LOG_WARN("bind lob macro sequence generator failed",
               K(ret), K(target), K(column_ctx));
    } else {
      ObLobMacroWriteParam lob_write_param;
      lob_write_param.lob_col_idx_ = column_ctx.lob_col_idx_;
      lob_write_param.lob_id_interval_start_ = lob_id_interval_start_;
      lob_write_param.lob_id_interval_end_ = lob_id_interval_end_;
      lob_write_param.macro_seq_generator_ = seq_generator;
      if (OB_FAIL((*macro_block_writer)->init_lob(
          param_, target, lob_table_key, *start_sequence,
          0/*row_offset*/, lob_write_param))) {
        LOG_WARN("init lob macro block writer failed", K(ret), K(target),
                 K(lob_table_key), K(column_ctx), K(lob_write_param));
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::switch_lob_id_cache()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(close_macro_block_writers())) {
    LOG_WARN("close lob macro block writers failed", K(ret));
  } else {
    uint64_t old_value = 0;
    (void) lob_id_cache_.get_value(old_value);
    if (lob_id_generator_.is_inited()) {
      int64_t lob_id_start = -1;
      int64_t lob_id_end = -1;
      if (OB_FAIL(lob_id_generator_.get_next_interval(lob_id_start, lob_id_end))) {
        LOG_WARN("get lob id cache from ddl sequence generator fail", K(ret), K(lob_id_generator_));
      } else {
        lob_id_cache_.cache_size_ = lob_id_generator_.get_interval_size();
        lob_id_cache_.set(max(lob_id_start, 1), lob_id_end);
      }
    } else {
      static const int64_t AUTO_INC_CACHE_INTERVAL = 5000000L; // 500w.
      ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
      lob_id_cache_.cache_size_ = AUTO_INC_CACHE_INTERVAL;
      if (OB_FAIL(auto_inc.get_tablet_cache_interval(MTL_ID(), lob_id_cache_))) {
        LOG_WARN("autoinc service get tablet cache failed", K(ret), K(MTL_ID()));
      }
    }
    uint64_t interval_start = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(lob_id_cache_.get_value(interval_start))) {
      LOG_WARN("get lob id interval start failed", K(ret), K(lob_id_cache_));
    } else if (OB_UNLIKELY(0 == lob_id_cache_.count()
        || interval_start > UINT64_MAX - lob_id_cache_.count() + 1)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("lob id interval overflows",
               K(ret), K(interval_start), K(lob_id_cache_));
    } else {
      lob_id_interval_start_ = interval_start;
      lob_id_interval_end_ = interval_start + lob_id_cache_.count() - 1;
      for (int64_t i = 0; i < column_write_ctxs_.count(); ++i) {
        ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
        column_ctx.meta_run_state_.reset(
            lob_id_interval_start_, lob_id_interval_end_);
        column_ctx.piece_run_state_.reset(
            lob_id_interval_start_, lob_id_interval_end_);
      }
    }
    FLOG_INFO("switch lob id cache", K(ret), K(tablet_id_), K(slice_idx_), "is_idem", lob_id_generator_.is_inited(), K(old_value), K(total_lob_cell_count_), K(inrow_lob_cell_count_), "new_cache", lob_id_cache_);
    total_lob_cell_count_ = 0;
    inrow_lob_cell_count_ = 0;
  }
  return ret;
}

int ObLobMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(close_macro_block_writers())) {
    LOG_WARN("close lob macro block writers failed", K(ret));
  } else {
    uint64_t last_lob_id = 0;
    if (OB_FAIL(lob_id_cache_.get_value(last_lob_id))) {
      LOG_WARN("get last lob id failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::set_tablet_autoinc_seq(ls_id_, lob_meta_tablet_id_, last_lob_id))) {
      LOG_WARN("update max lob id failed", K(ret), K(last_lob_id));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::close_macro_block_writers()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (use_staging_) {
    if (OB_FAIL(replay_staging_stores(ObDirectLoadTabletTarget::LOB_META))) {
      LOG_WARN("replay lob meta staging stores failed", K(ret));
    } else if (OB_FAIL(replay_staging_stores(ObDirectLoadTabletTarget::LOB_PIECE))) {
      LOG_WARN("replay lob piece staging stores failed", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_write_ctxs_.count(); ++i) {
      ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
      if (OB_FAIL(close_macro_block_writer(
          column_ctx.meta_macro_writer_, column_ctx.meta_seq_generator_))) {
        LOG_WARN("close lob meta macro block writer failed", K(ret), K(column_ctx));
      } else if (OB_FAIL(close_macro_block_writer(
          column_ctx.piece_macro_writer_, column_ctx.piece_seq_generator_))) {
        LOG_WARN("close lob piece macro block writer failed", K(ret), K(column_ctx));
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::replay_staging_stores(
    const ObDirectLoadTabletTarget target)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDirectLoadTabletTarget::LOB_META != target
      && ObDirectLoadTabletTarget::LOB_PIECE != target)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob staging replay target", K(ret), K(target));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_write_ctxs_.count(); ++i) {
    ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
    ObChunkDatumStore *&staging_store =
        ObDirectLoadTabletTarget::LOB_META == target
            ? column_ctx.meta_staging_store_
            : column_ctx.piece_staging_store_;
    ObLobAuxMacroSeqGenerator &seq_generator =
        ObDirectLoadTabletTarget::LOB_META == target
            ? column_ctx.meta_seq_generator_
            : column_ctx.piece_seq_generator_;
    if (OB_NOT_NULL(staging_store)) {
      if (OB_FAIL(replay_staging_store(column_ctx, target, *staging_store, seq_generator))) {
        LOG_WARN("replay lob staging store failed", K(ret), K(target), K(column_ctx));
      } else {
        destroy_staging_store(staging_store);
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::replay_staging_store(
    ObLobColumnWriteCtx &column_ctx,
    const ObDirectLoadTabletTarget target,
    ObChunkDatumStore &staging_store,
    ObLobAuxMacroSeqGenerator &seq_generator)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::Iterator iterator;
  const ObChunkDatumStore::StoredRow *stored_row = nullptr;
  ObDatumRow replay_row;
  int64_t replayed_row_count = 0;
  if (OB_FAIL(staging_store.finish_add_row(false))) {
    LOG_WARN("finish lob staging store failed", K(ret), K(column_ctx));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_macro_block_writer(column_ctx, target))) {
      LOG_WARN("prepare lob replay macro block writer failed", K(ret), K(target), K(column_ctx));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(staging_store.begin(iterator))) {
      LOG_WARN("begin lob staging iterator failed", K(ret), K(column_ctx));
    }
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(iterator.get_next_row(stored_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next lob staging row failed", K(ret), K(column_ctx));
      }
    } else if (OB_UNLIKELY(OB_ISNULL(stored_row) || 1 != stored_row->cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob staging row", K(ret), K(column_ctx), KP(stored_row));
    } else {
      const ObDatum &serialized_row = stored_row->cells()[0];
      int64_t pos = 0;
      replay_row.reuse();
      if (OB_FAIL(replay_row.deserialize(
          serialized_row.ptr_, serialized_row.len_, pos))) {
        LOG_WARN("deserialize lob staging row failed", K(ret), K(column_ctx));
      } else if (OB_UNLIKELY(pos != serialized_row.len_ || !replay_row.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid deserialized lob staging row", K(ret),
            K(column_ctx), K(pos), K(serialized_row.len_));
      } else if (OB_FAIL(replay_macro_writer_->append_row(replay_row))) {
        LOG_WARN("append replayed lob staging row failed", K(ret), K(column_ctx));
      } else {
        ++replayed_row_count;
        ret = OB_E(FAIL_LOB_STAGING_REPLAY_AFTER_APPEND, replayed_row_count) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          LOG_WARN("inject lob staging replay failure after append",
                   K(ret), K(target), K(replayed_row_count), K(column_ctx));
        }
      }
    }
  }
  iterator.reset();
  if (OB_SUCC(ret)
      && OB_NOT_NULL(replay_macro_writer_)
      && replay_macro_writer_->is_inited()) {
    if (OB_FAIL(close_macro_block_writer(replay_macro_writer_, seq_generator))) {
      LOG_WARN("close lob replay macro block writer failed", K(ret), K(column_ctx));
    }
  }
  if (OB_SUCCESS != ret) {
    // Do not close on replay failure because it may flush an incomplete tail block
    // and cause a checksum conflict with retry.
    if (OB_NOT_NULL(replay_macro_writer_) && replay_macro_writer_->is_inited()) {
      replay_macro_writer_->reset();
    }
    seq_generator.reset();
  }
  return ret;
}

int ObLobMacroBlockWriter::close_macro_block_writer(
    ObCgMacroBlockWriter *macro_block_writer,
    ObLobAuxMacroSeqGenerator &seq_generator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(macro_block_writer) && macro_block_writer->is_inited()) {
    if (OB_FAIL(macro_block_writer->close())) {
      LOG_WARN("macro block writer close failed", K(ret), KP(macro_block_writer));
    } else {
      macro_block_writer->reset();
      seq_generator.reset();
    }
  }
  return ret;
}

void ObLobMacroBlockWriter::destroy_macro_block_writers()
{
  for (int64_t i = 0; i < column_write_ctxs_.count(); ++i) {
    ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
    ob_delete(column_ctx.meta_macro_writer_);
    ob_delete(column_ctx.piece_macro_writer_);
  }
  ob_delete(replay_macro_writer_);
}

void ObLobMacroBlockWriter::destroy_staging_store(
    ObChunkDatumStore *&staging_store)
{
  ob_delete(staging_store);
}

void ObLobMacroBlockWriter::destroy_staging_stores()
{
  for (int64_t i = 0; i < column_write_ctxs_.count(); ++i) {
    ObLobColumnWriteCtx &column_ctx = column_write_ctxs_.at(i);
    destroy_staging_store(column_ctx.meta_staging_store_);
    destroy_staging_store(column_ctx.piece_staging_store_);
  }
}
