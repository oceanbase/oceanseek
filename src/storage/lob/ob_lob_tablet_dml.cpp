/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "object/ob_object.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_util.h"

#include "ob_lob_tablet_dml.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/lob/ob_ext_info_callback.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

namespace
{
bool is_outrow_lob_unsupported_index(const ObDMLRunningCtx &run_ctx)
{
  const share::schema::ObTableSchemaParam &table_param =
      run_ctx.effective_table_param_->get_data_table();
  // A regular index has no LOB auxiliary table and cannot contain outrow LOBs.
  return table_param.is_index_table()
      && !share::schema::is_index_type_has_lob(table_param.get_index_type());
}
} // namespace

int ObLobTabletDmlHelper::insert_before(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &row,
    const int16_t row_idx,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObLobInsertContext insert_ctx;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  bool outrow = false;
  bool force_inrow = false;

  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mngr));
  } else if (FALSE_IT(force_inrow = is_force_inrow_(run_ctx, col_idx))) {
  } else if (OB_FAIL(insert_ctx.build_for_dml(run_ctx, row, col_idx, datum))) {
    LOG_WARN("build_common_lob_param_for_dml fail", K(ret), K(col_idx));
  } else if (!force_inrow) {
    if (OB_FAIL(lob_mngr->insert_before(insert_ctx, outrow))) {
      LOG_WARN("prepare_insert_task fail", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (outrow) {
    ObLobDmlTask task;
    if (OB_FAIL(task.from_ctx(insert_ctx, row_idx, col_idx))) {
      LOG_WARN("failed build insert task from insert ctx", KR(ret), K(insert_ctx));
    } else if (OB_FAIL(run_ctx.lob_dml_ctx_.push_insert_task(task))) {
      LOG_WARN("failed push back insert task", KR(ret), K(task));
    } else {
      datum.set_string(insert_ctx.dest_loc_.ptr_, insert_ctx.dest_loc_.size_);
    }
  } else {
    if (OB_FAIL(insert_inrow_helper_(insert_ctx))) {
      LOG_WARN("failed insert inrow", KR(ret), K(insert_ctx));
    } else {
      datum.set_string(insert_ctx.dest_loc_.ptr_, insert_ctx.dest_loc_.size_);
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::insert_after(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &row,
    const ObLobDmlTask &task)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = nullptr;
  ObLobInsertContext insert_ctx;

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(lob_mgr = MTL(ObLobManager *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "LOB: failed to get lob manager handle", KR(ret), K(row), K(task));
  } else if (OB_UNLIKELY(!row.is_valid() || !task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(row), K(task));
  } else if (OB_FAIL(insert_ctx.build(run_ctx, row, task.col_idx_, task.src_loc_))) {
    STORAGE_LOG(WARN, "failed to build lob param for dml", KR(ret), K(row), K(task));
  } else if (FALSE_IT(insert_ctx.dest_loc_ = task.dest_loc_)) {
  } else if (FALSE_IT(insert_ctx.try_flush_redo_ = true)) {
  } else if (OB_FAIL(lob_mgr->insert(insert_ctx)))  {
    STORAGE_LOG(WARN, "failed to insert lob", KR(ret), K(row), K(task), K(insert_ctx));
  } 

  return ret;
}

int ObLobTabletDmlHelper::prepare_old_outrow_(
    blocksstable::ObStorageDatum &old_datum,
    ObLobUpdateContext &upd_ctx)
{
  int ret = OB_SUCCESS;
  if (old_datum.is_null() || old_datum.is_nop_value()) {
    // old value is null, no lob id to reuse and nothing to delete
  } else {
    ObLobLocatorV2 old_loc(old_datum.get_string());
    ObLobDiskLocatorWrapper old_wrapper;
    if (OB_UNLIKELY(!old_loc.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid old locator", KR(ret), K(old_loc));
    } else if (old_loc.is_inrow()) {
      // old value is inrow, no lob id to reuse and nothing to delete
    } else if (OB_FAIL(old_wrapper.init(old_loc.ptr_, old_loc.size_))) {
      LOG_WARN("init old locator wrapper fail", KR(ret), K(old_loc));
    } else if (OB_FAIL(old_wrapper.get_lob_id(upd_ctx.lob_id_))) {
      LOG_WARN("get old lob id fail", KR(ret), K(old_wrapper));
    } else {
      upd_ctx.old_loc_ = old_loc;
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::update_before(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const int16_t row_idx,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &old_datum,
    blocksstable::ObStorageDatum &new_datum)
{
  int ret = OB_SUCCESS;
  ObLobUpdateContext upd_ctx;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  bool outrow = false;
  bool force_inrow = false;
  // 当主表的 rowkey 有变化，或者主表是 delete_insert 表时，update 会被拆成 delete 和 insert 两个操作
  // 这种情况下，到 lob helper 的 update_before 部分，只需要处理 insert
  bool is_delete_and_insert = data_tbl_rowkey_change || run_ctx.is_delete_insert_table_;

  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
  } else if (new_datum.is_nop_value()) {
    // do nothing
  } else {

    // Step 1: old datum 为 outrow 时，构建 old datum 的删除任务
    if (is_delete_and_insert) {
      // do nothing 
    } else if (OB_FAIL(prepare_old_outrow_(old_datum, upd_ctx))) {
      LOG_WARN("prepare old outrow fail", K(ret), K(row_idx), K(col_idx));
    } 

    // Step 2：构建 new datum 的插入任务
    if (OB_FAIL(ret)) {
    } else if (new_datum.is_null()) {
      // do nothing
    } else if (FALSE_IT(force_inrow = is_force_inrow_(run_ctx, col_idx))) {
    } else if (OB_FAIL(upd_ctx.build_for_dml(run_ctx, new_row, col_idx, new_datum))) {
      LOG_WARN("build update ctx for dml fail", K(ret), K(row_idx), K(col_idx));
    } else {
      if (!force_inrow) {
        if (OB_FAIL(lob_mgr->update_before(upd_ctx, outrow))) {
          // reuse lob id if old_datum is outrow
          LOG_WARN("update_before fail", K(ret), K(upd_ctx));
        }
      }
      
      if (OB_FAIL(ret)) {
      } else if (!outrow) {
        if (OB_FAIL(insert_inrow_helper_(upd_ctx))) {
          LOG_WARN("failed to insert inrow", KR(ret), K(upd_ctx));
        }
      }

      if (OB_SUCC(ret)) {
        new_datum.set_string(upd_ctx.dest_loc_.ptr_, upd_ctx.dest_loc_.size_);
      }
    }

    // Step 3: 若 old_loc_ 是 outrow，或者 dest_loc_ 是 outrow，则创建 ObLobDmlTask 添加到队列中，after 阶段执行
    if (OB_FAIL(ret)) {
    } else if (upd_ctx.old_loc_.is_outrow_disk_lob_locator() || outrow) {
      ObLobDmlTask task;
      if (OB_FAIL(task.from_ctx(upd_ctx, row_idx, col_idx))) {
        LOG_WARN("failed build update task from update ctx", KR(ret), K(upd_ctx));
      } else if (OB_FAIL(run_ctx.lob_dml_ctx_.push_insert_task(task))) {
        LOG_WARN("failed push back update delete task", KR(ret), K(task));
      }
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::update_after(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const ObLobDmlTask &task)
{
  int ret = OB_SUCCESS;
  ObLobInsertContext insert_ctx;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  // 当主表的 rowkey 有变化，或者主表是 delete_insert 表时，update 会被拆成 delete 和 insert 两个操作
  // 这种情况下，到 lob helper 的 update_after 部分，只需要处理 insert
  bool is_delete_and_insert = data_tbl_rowkey_change || run_ctx.is_delete_insert_table_;

  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
  } else if (OB_UNLIKELY(!task.is_valid() || task.op_ != ObLobDmlOp::UPDATE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  }

  // delete 
  if (OB_FAIL(ret)) {
  } else if (!task.need_delete_old_outrow()) {
    // do nothing
  } else if (is_delete_and_insert) {
    // do nothing
  } else if (is_outrow_lob_unsupported_index(run_ctx)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow lob in index table");
    LOG_WARN("outrow lob in index table is not supported", KR(ret),
             "table_id", run_ctx.effective_table_param_->get_data_table().get_table_id(),
             "index_type", run_ctx.effective_table_param_->get_data_table().get_index_type());
  } else {
    ObLobDeleteContext del_ctx;    
    if (OB_FAIL(del_ctx.build(run_ctx, old_row, task.col_idx_, task.old_loc_))) {
      LOG_WARN("failed to build delete context", KR(ret), K(task));
    } else if (FALSE_IT(del_ctx.try_flush_redo_ = true)) {
    } else if (OB_FAIL(lob_mgr->del(del_ctx))) {
      LOG_WARN("failed delete lob in update", KR(ret), K(task));
    }
  }

  // insert
  if (OB_FAIL(ret)) {
  } else if (!task.need_insert_outrow()) {
    // do nothing
  } else if (OB_FAIL(insert_ctx.build(run_ctx, new_row, task.col_idx_, task.src_loc_))) {
    LOG_WARN("failed to build insert context", KR(ret), K(task));
  } else if (FALSE_IT(insert_ctx.dest_loc_ = task.dest_loc_)) {
  } else if (FALSE_IT(insert_ctx.try_flush_redo_ = true)) {
  } else if (OB_FAIL(lob_mgr->insert(insert_ctx)))  {
    STORAGE_LOG(WARN, "failed to insert lob", KR(ret), K(new_row), K(task), K(insert_ctx));
  }

  return ret;
}

int ObLobTabletDmlHelper::append_before(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const int16_t row_idx,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &old_datum,
    blocksstable::ObStorageDatum &new_datum)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  ObLobAppendContext append_ctx;
  bool outrow = false;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
  } else if (old_datum.is_null() || old_datum.is_nop_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "LOB_WRITEAPPEND target must be a non-NULL LOB");
    LOG_WARN("invalid old lob for append", KR(ret), K(old_datum));
  } else if (data_tbl_rowkey_change || run_ctx.is_delete_insert_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_WRITEAPPEND with rowkey change or delete-insert update");
    LOG_WARN("lob append does not support delete-insert update", KR(ret),
             K(data_tbl_rowkey_change), K(run_ctx.is_delete_insert_table_));
  } else if (OB_FAIL(append_ctx.build_for_dml(
                 run_ctx, new_row, col_idx, old_datum, new_datum))) {
    LOG_WARN("failed to build lob append context", KR(ret), K(row_idx), K(col_idx));
  } else if (OB_FAIL(lob_mgr->append_before(append_ctx, outrow))) {
    LOG_WARN("failed to prepare lob append", KR(ret), K(append_ctx));
  } else {
    new_datum.set_string(append_ctx.dest_loc_.ptr_, append_ctx.dest_loc_.size_);
    if (outrow) {
      ObLobDmlTask task;
      if (OB_FAIL(task.from_ctx(append_ctx, row_idx, col_idx))) {
        LOG_WARN("failed to build lob append task", KR(ret), K(append_ctx));
      } else if (OB_FAIL(run_ctx.lob_dml_ctx_.push_insert_task(task))) {
        LOG_WARN("failed to enqueue lob append task", KR(ret), K(task));
      }
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::append_after(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const ObLobDmlTask &task)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  ObLobAppendContext append_ctx;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
  } else if (OB_UNLIKELY(!task.is_valid() || !task.is_append()
                         || data_tbl_rowkey_change || run_ctx.is_delete_insert_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob append task", KR(ret), K(task), K(data_tbl_rowkey_change),
             K(run_ctx.is_delete_insert_table_));
  } else if (OB_FAIL(append_ctx.build(run_ctx, new_row, task.col_idx_,
                                      task.old_loc_, task.src_loc_))) {
    LOG_WARN("failed to rebuild lob append context", KR(ret), K(task));
  } else {
    append_ctx.dest_loc_ = task.dest_loc_;
    append_ctx.try_flush_redo_ = true;
    if (OB_FAIL(lob_mgr->append(append_ctx))) {
      LOG_WARN("failed to append lob auxiliary rows", KR(ret), K(task));
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::delete_lob_col(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &datum,
    ObLobDeleteContext &del_ctx,
    const bool try_flush_redo)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = nullptr;
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  if (OB_ISNULL(lob_mgr = MTL(ObLobManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager instance failed", KR(ret), KP(lob_mgr));
  } else if (!column.col_type_.is_lob_storage() || datum.is_nop_value() || datum.is_null()) {
    // do nothing
  } else {
    ObLobLocatorV2 locator(datum.get_string());
    if (OB_UNLIKELY(!locator.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid locator", KR(ret), K(locator));
    } else if (locator.is_inrow()) {
      // do nothing
    } else if (is_outrow_lob_unsupported_index(run_ctx)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow lob in index table");
      LOG_WARN("outrow lob in index table is not supported", KR(ret),
               "table_id", run_ctx.effective_table_param_->get_data_table().get_table_id(),
               "index_type", run_ctx.effective_table_param_->get_data_table().get_index_type());
    } else if (OB_FAIL(del_ctx.build_for_dml(run_ctx, data_row, col_idx, datum))) {
      LOG_WARN("failed to build param for dml", KR(ret), K(col_idx), K(datum));
    } else if (FALSE_IT(del_ctx.try_flush_redo_ = try_flush_redo)) {
    } else if (OB_FAIL(lob_mgr->del(del_ctx))) {
      LOG_WARN("failed to delete lob", KR(ret), K(del_ctx));
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::prepare_lob_ext_info_log_task(
    ObDMLRunningCtx &run_ctx,
    const int16_t row_idx,
    const int16_t col_idx)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = run_ctx.relative_table_.get_table_id();
  if (OB_UNLIKELY((row_idx < 0) || (col_idx < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ext info log task argument", KR(ret), K(row_idx), K(col_idx));
  } else if (is_sys_table(table_id) || run_ctx.relative_table_.is_index_table()) {
    // 系统表和索引表不需要为未更新 LOB 列补充完整镜像日志；FULL 和 MINIMAL 模式均需要补日志。
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_2_2) {
    // 混部期间不生成 ObLobExtInfoLogTask，避免向旧 observer 发送 GET_LOB_SUMMARY_INFO RPC 导致报错。
    // CDC 仅支持同步所有 observer 升级到 4.6.2.2 后执行的变更，
    // 升级前begin的事务，升级后才commit的跨升级事务，也不支持同步。
    LOG_INFO("prepare lob ext info log task is not supported", KR(ret), K(row_idx), K(col_idx));
  } else {
    transaction::ObTxSEQ lob_operation_seq_no;
    if (OB_FAIL(run_ctx.store_ctx_.mvcc_acc_ctx_.get_write_seq(lob_operation_seq_no))) {
      LOG_WARN("get main update operation seq no failed", KR(ret), K(row_idx),
          K(col_idx), K(table_id));
    } else if (OB_UNLIKELY(!lob_operation_seq_no.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid main update operation seq no", KR(ret), K(row_idx),
          K(col_idx), K(table_id), K(lob_operation_seq_no));
    } else {
      const ObLobExtInfoLogTask task(row_idx, col_idx, lob_operation_seq_no);
      if (OB_FAIL(run_ctx.lob_dml_ctx_.push_ext_info_log_task(task))) {
        LOG_WARN("push lob ext info log task failed", KR(ret), K(task), K(table_id));
      }
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::register_lob_summary_ext_info_log_(
    ObDMLRunningCtx &run_ctx,
    const ObLobExtInfoLogTask &task,
    const ObLobId &lob_id,
    const ObLobMetaInfo &summary,
    const uint64_t lob_meta_table_id)
{
  int ret = OB_SUCCESS;
  memtable::ObIMvccCtx *mem_ctx = run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_;
  ObExtInfoCbRegister cb_register;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable context is null when registering lob summary ext info log",
        KR(ret), K(task), K(lob_id), K(summary));
  } else {
    ObLobSummaryExtInfoLog summary_log;
    summary_log.lob_aux_table_id_ = lob_meta_table_id;
    summary_log.lob_operation_seq_no_ = task.lob_operation_seq_no_;
    summary_log.seq_id_ = summary.seq_id_;
    summary_log.byte_len_ = summary.byte_len_;
    summary_log.char_len_ = summary.char_len_;
    summary_log.lob_info_ = summary.lob_extra_info_;
    if (OB_FAIL(cb_register.register_lob_summary_cb(
        mem_ctx, run_ctx.store_ctx_, run_ctx.dml_param_.timeout_, run_ctx.dml_flag_,
        lob_id, summary_log))) {
      LOG_WARN("register lob full image summary callback failed", KR(ret),
          K(task), K(lob_id), K(summary_log));
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::register_lob_data_ext_info_log_(
    ObDMLRunningCtx &run_ctx,
    const ObLobExtInfoLogTask &task,
    const ObLobId &lob_id,
    const ObLobMetaInfo &summary,
    const ObLobMetaExtraInfo &extra_info,
    const uint64_t lob_piece_table_id,
    ObLobReadContext &read_ctx,
    ObIAllocator &tmp_allocator)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  memtable::ObIMvccCtx *mem_ctx = run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_;
  ObLobQueryIter *query_iter = nullptr;
  ObExtInfoCbRegister cb_register;
  ObLobSeqIdGenerator seq_id_generator;
  uint64_t scanned_chunk_count = 0;
  uint64_t scanned_byte_len = 0;
  uint64_t scanned_char_len = 0;

  if (OB_ISNULL(lob_mgr) || OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid runtime context when registering lob data ext info log",
        KR(ret), KP(lob_mgr), KP(mem_ctx), K(task), K(lob_id));
  } else if (extra_info.chunk_count_ > 0) {
    ObString summary_seq_id = summary.seq_id_;
    seq_id_generator.set_allocator(&tmp_allocator);
    seq_id_generator.set_seq_id(summary_seq_id);
    if (OB_FAIL(lob_mgr->query(read_ctx, query_iter))) {
      LOG_WARN("open lob full image query iterator failed", KR(ret),
          K(task), K(lob_id), K(read_ctx));
    } else if (OB_ISNULL(query_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob full image query iterator is null", KR(ret),
          K(task), K(lob_id), K(read_ctx));
    }

    for (uint64_t i = 0; OB_SUCC(ret) && (i < extra_info.chunk_count_); ++i) {
      ObLobChunk chunk;
      ObString expected_seq_id;
      if (OB_ISNULL(query_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob full image query iterator is null", KR(ret),
            K(i), K(task), K(lob_id));
      } else if (OB_FAIL(query_iter->get_next_chunk(
          chunk, ObLobMetaUtil::LOB_PIECE_DATA_SIZE))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get lob full image chunk failed", KR(ret),
            K(i), K(task), K(lob_id));
      } else if (OB_FAIL(seq_id_generator.get_next_seq_id(expected_seq_id))) {
        LOG_WARN("generate expected lob full image seq id failed", KR(ret),
            K(i), K(task), K(lob_id));
      } else if (OB_UNLIKELY(chunk.lob_data_.empty()
          || (chunk.byte_len_ != static_cast<uint64_t>(chunk.lob_data_.length()))
          || (chunk.byte_len_ > ObLobMetaUtil::LOB_PIECE_DATA_SIZE)
          || (chunk.char_len_ > chunk.byte_len_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob full image chunk", KR(ret),
            K(i), K(task), K(lob_id), K(chunk));
      } else if (OB_UNLIKELY(
          (scanned_byte_len > UINT64_MAX - chunk.byte_len_)
          || (scanned_char_len > UINT64_MAX - chunk.char_len_)
          || (UINT64_MAX == scanned_chunk_count))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("lob full image length overflow", KR(ret),
            K(i), K(task), K(lob_id), K(scanned_chunk_count),
            K(scanned_byte_len), K(scanned_char_len), K(chunk));
      } else {
        ObLobDataExtInfoLog data_log;
        data_log.lob_aux_table_id_ = lob_piece_table_id;
        data_log.lob_operation_seq_no_ = task.lob_operation_seq_no_;
        data_log.seq_id_ = expected_seq_id;
        data_log.lob_data_ = chunk.lob_data_;
        if (OB_FAIL(cb_register.register_lob_data_cb(
            mem_ctx, run_ctx.store_ctx_, run_ctx.dml_param_.timeout_, run_ctx.dml_flag_,
            lob_id, data_log))) {
          LOG_WARN("register lob full image data callback failed", KR(ret), K(i),
              K(task), K(lob_id), K(data_log));
        } else {
          ++scanned_chunk_count;
          scanned_byte_len += chunk.byte_len_;
          scanned_char_len += chunk.char_len_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObLobChunk extra_chunk;
      const int tmp_ret = query_iter->get_next_chunk(
          extra_chunk, ObLobMetaUtil::LOB_PIECE_DATA_SIZE);
      if (OB_SUCCESS == tmp_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra lob full image chunk", KR(ret),
            K(task), K(lob_id), K(extra_chunk), K(extra_info));
      } else if (OB_ITER_END != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("check lob full image query end failed", KR(ret), K(task), K(lob_id));
      } else if (OB_UNLIKELY((scanned_chunk_count != extra_info.chunk_count_)
          || (scanned_byte_len != summary.byte_len_)
          || (scanned_char_len != summary.char_len_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob full image summary and chunks mismatch", KR(ret),
            K(task), K(lob_id), K(scanned_chunk_count), K(scanned_byte_len),
            K(scanned_char_len), K(summary), K(extra_info));
      }
    }
  }

  if (OB_NOT_NULL(query_iter)) {
    OB_DELETE(ObLobQueryIter, "unused", query_iter);
    query_iter = nullptr;
  }
  return ret;
}

int ObLobTabletDmlHelper::register_lob_ext_info_log(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &old_row,
    const ObLobExtInfoLogTask &task,
    const uint64_t lob_meta_table_id,
    const uint64_t lob_piece_table_id)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  memtable::ObIMvccCtx *mem_ctx = run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_;
  // build_for_dml() 将 mem locator 保存在 DML LOB allocator 中；后续把
  // read_ctx 的工作 allocator 切换到这里，使 Summary、RPC buffer 和 seq_id
  // 临时内存在单个 LOB image 处理完成后整体释放。
  ObArenaAllocator tmp_allocator("LobExtInfoLog", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObLobReadContext read_ctx;
  ObLobMetaInfo summary;
  ObLobMetaExtraInfo extra_info;
  ObLobId lob_id;
  char summary_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
  bool need_register_ext_info_log = false;

  if (OB_ISNULL(lob_mgr) || OB_ISNULL(mem_ctx) || OB_ISNULL(run_ctx.col_descs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob full image runtime context", KR(ret), KP(lob_mgr),
        KP(mem_ctx), KP(run_ctx.col_descs_), K(task));
  } else if (OB_UNLIKELY(!task.is_valid()
      || (task.row_idx_ < 0)
      || (task.col_idx_ < 0)
      || (task.col_idx_ >= old_row.count_)
      || (task.col_idx_ >= run_ctx.col_descs_->count())
      || (OB_INVALID_ID == lob_meta_table_id)
      || (OB_INVALID_ID == lob_piece_table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob full image log argument", KR(ret), K(task), K(old_row.count_),
        K(lob_meta_table_id), K(lob_piece_table_id));
  } else {
    const blocksstable::ObStorageDatum &old_datum = old_row.storage_datums_[task.col_idx_];
    if (OB_UNLIKELY(old_datum.is_null() || old_datum.is_nop_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old lob datum is null or nop for full image log", KR(ret), K(task), K(old_datum));
    }
    // 注：只有 INSERT 才有可能跳过提前获取全局快照，使用 none_read。而 register_lob_ext_info_log 补充
    // 未更新 lob 列前镜像的日志，不会用在 INSERT 场景下，因此这里 allow_none_read_snapshot 固定传 false。
    else if (OB_FAIL(read_ctx.build_for_dml(
        run_ctx, task.col_idx_, old_datum, false/*allow_none_read_snapshot*/))) {
      LOG_WARN("build lob full image read context failed", KR(ret), K(task), K(old_datum));
    } else if (FALSE_IT(read_ctx.allocator_ = &tmp_allocator)) {
    } else if (OB_FAIL(read_ctx.src_loc_wrapper_.get_lob_id(lob_id))) {
      LOG_WARN("get lob id from full image locator failed", KR(ret), K(task), K(read_ctx));
    } else if (OB_FAIL(lob_mgr->get_lob_summary_info(read_ctx, summary))) {
      LOG_WARN("get lob full image summary failed", KR(ret), K(task), K(lob_id), K(read_ctx));
    } else if (OB_FAIL(ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
      LOG_WARN("generate lob summary seq id failed", KR(ret), K(task), K(lob_id));
    } else if (OB_FAIL(extra_info.deserialize(summary.lob_extra_info_))) {
      LOG_WARN("deserialize lob full image summary failed", KR(ret), K(task), K(lob_id), K(summary));
    } else if (OB_UNLIKELY((summary.lob_col_idx_ != read_ctx.column_idx())
        || (summary.lob_id_ != lob_id)
        || (summary.seq_id_ != summary_seq_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lob full image summary row", KR(ret), K(task), K(lob_id),
          K(summary), K(read_ctx.column_idx()));
    } else if (OB_UNLIKELY(ObLobMetaExtraInfo::LOB_META_SUMMARY != extra_info.extra_info_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lob full image extra info type", KR(ret), K(task),
          K(lob_id), K(extra_info));
    } else if (OB_UNLIKELY((summary.byte_len_ > static_cast<uint64_t>(INT64_MAX - 1))
        || (summary.char_len_ > summary.byte_len_)
        || ((0 == summary.byte_len_) != (0 == extra_info.chunk_count_))
        || ((summary.byte_len_ > 0) && (extra_info.chunk_count_ > summary.byte_len_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected in-table lob full image summary", KR(ret), K(task),
          K(lob_id), K(summary), K(extra_info));
    } else if (OB_UNLIKELY(ObLobLastWriteType::INSERT != extra_info.last_write_type_)) {
      // 当前只为 INSERT 类型的完整 LOB 镜像补日志，其他写入类型（例如 APPEND）不支持 CDC，跳过。
      LOG_DEBUG("skip non-insert lob last write type", K(task), K(lob_id),
          K(extra_info));
    } else if (OB_UNLIKELY(!extra_info.lob_chunk_type_.in_table())) {
      // TODO(binifei.bnf): 目前仅支持 IN_TABLE，后续支持 OUT_TABLE 时再增加处理逻辑。
      LOG_DEBUG("skip non-in-table lob chunk type", K(task), K(lob_id),
          K(extra_info));
    } else {
      read_ctx.byte_size_ = summary.byte_len_;
      need_register_ext_info_log = true;
    }
  }

  if (OB_SUCC(ret) && need_register_ext_info_log) {
    if (OB_FAIL(register_lob_summary_ext_info_log_(
        run_ctx, task, lob_id, summary, lob_meta_table_id))) {
      LOG_WARN("register lob summary ext info log failed", KR(ret), K(task), K(lob_id));
    } else if (OB_FAIL(register_lob_data_ext_info_log_(
        run_ctx, task, lob_id, summary, extra_info, lob_piece_table_id,
        read_ctx, tmp_allocator))) {
      LOG_WARN("register lob data ext info log failed", KR(ret), K(task), K(lob_id));
    }
  }
  return ret;
}

bool ObLobTabletDmlHelper::is_force_inrow_(ObDMLRunningCtx &run_ctx, const int16_t col_idx)
{
  // 通常 lob 列不能作为主键，也不能建索引。例外是 string 类型，string 做主键时会强制转
  // inrow 存储，主键 string 建索引也会强制转 inrow 存储，但非主键 string 建索引时不强制
  // 若判定为 outrow 则报错 not supported
  // 
  // 强制 inrow 存储的情况：
  // 1. 插入 string 列的索引表，并且该列在主表是主键，则强制转 inrow 存储
  // 2. 插入的是主表（非索引表），并且该列是主键，则强制转 inrow 存储
  return run_ctx.is_main_table_rowkey_col(col_idx)
         || (!run_ctx.relative_table_.is_index_table() && col_idx < run_ctx.relative_table_.get_rowkey_column_num());
}

int ObLobTabletDmlHelper::insert_inrow_helper_(ObLobInsertContext &ctx)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = nullptr;
  if (OB_UNLIKELY(!ctx.src_loc_.is_valid()) || OB_ISNULL(ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx));
  } else if (OB_ISNULL(lob_mgr = MTL(ObLobManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
  } else if (ctx.src_loc_.is_inrow_disk_lob_locator()) {
    ctx.dest_loc_ = ctx.src_loc_;
  } else {
    ObString data;
    char *buf = nullptr;
    uint64_t byte_length = 0;
    if (OB_FAIL(lob_mgr->get_byte_length(ctx.src_loc_, byte_length))) {
      LOG_WARN("failed to get byte length", KR(ret), K(ctx));
    } else if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(byte_length + sizeof(ObLobCommon))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc dest lob buffer", KR(ret), K(ctx));
    } else if (FALSE_IT(data.assign_buffer(buf + sizeof(ObLobCommon), byte_length))) {
    } else if (OB_FAIL(lob_mgr->query(ctx.src_loc_, data))) {
      LOG_WARN("failed query data from src loc", KR(ret), K(ctx));
    } else {
      ObLobCommon *lob_common = new(buf) ObLobCommon();
      lob_common->in_row_ = 1;
      lob_common->is_mem_loc_ = 0;
      ctx.dest_loc_.assign_buffer(buf, byte_length + sizeof(ObLobCommon));
    }
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
