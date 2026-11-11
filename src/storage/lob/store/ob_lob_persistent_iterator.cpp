/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_range.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_store_common.h"

#include <type_traits>
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "ob_lob_persistent_iterator.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
namespace storage
{

//==================== ObLobAuxTableRowKey ====================
void ObLobAuxTableRowKey::make_min_row_key()
{
  for (int64_t i = 0; i < ROW_KEY_COLUMN_CNT; i++) {
    row_key_[i].set_min_value();
  }
}

void ObLobAuxTableRowKey::make_max_row_key()
{
  for (int64_t i = 0; i < ROW_KEY_COLUMN_CNT; i++) {
    row_key_[i].set_max_value();
  }
}

void ObLobAuxTableRowKey::set_lob_col_idx(int64_t lob_col_idx)
{
  row_key_[LOB_COL_IDX_IDX].set_int(lob_col_idx);
}

void ObLobAuxTableRowKey::set_lob_id(const char *lob_id_str, const int64_t lob_id_len)
{
  row_key_[LOB_ID_IDX].reset();
  row_key_[LOB_ID_IDX].set_varchar(lob_id_str, static_cast<int32_t>(lob_id_len));
  row_key_[LOB_ID_IDX].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
}

void ObLobAuxTableRowKey::set_seq_id(const char *seq_id_str, const int64_t seq_id_len)
{
  row_key_[SEQ_ID_IDX].reset();
  row_key_[SEQ_ID_IDX].set_varchar(seq_id_str, static_cast<int32_t>(seq_id_len));
  row_key_[SEQ_ID_IDX].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
}

//==================== ObLobAuxTableRangeBuilder ====================
int ObLobAuxTableRangeBuilder::build(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  // objs are referenced (not copied), min_row_key_/max_row_key_ must outlive range usage
  range.table_id_ = 0;
  range.start_key_ = min_row_key_.to_row_key();
  range.end_key_ = max_row_key_.to_row_key();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

int ObLobAuxTableRangeBuilder::build_from_start_seq(
    int64_t column_idx,
    const ObString &lob_id,
    const ObString &start_seq_id,
    const ObString &end_seq_id,
    ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_idx == OB_INVALID_INDEX || lob_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(column_idx), K(lob_id));
  } else {
    min_row_key_.make_min_row_key();
    min_row_key_.set_lob_col_idx(column_idx);
    min_row_key_.set_lob_id(lob_id.ptr(), lob_id.length());
    if (!start_seq_id.empty()) {
      min_row_key_.set_seq_id(start_seq_id.ptr(), start_seq_id.length());
    }

    max_row_key_.make_max_row_key();
    max_row_key_.set_lob_col_idx(column_idx);
    max_row_key_.set_lob_id(lob_id.ptr(), lob_id.length());
    if (!end_seq_id.empty()) {
      max_row_key_.set_seq_id(end_seq_id.ptr(), end_seq_id.length());
    }
    ret = build(range);
  }
  return ret;
}

int ObLobAuxTableRangeBuilder::build(
    int64_t column_idx,
    const ObString &lob_id,
    const ObString &seq_id,
    ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_idx == OB_INVALID_INDEX || lob_id.empty() || seq_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(column_idx), K(lob_id), K(seq_id));
  } else {
    min_row_key_.set_lob_col_idx(column_idx);
    min_row_key_.set_lob_id(lob_id.ptr(), lob_id.length());
    min_row_key_.set_seq_id(seq_id.ptr(), seq_id.length());

    max_row_key_.set_lob_col_idx(column_idx);
    max_row_key_.set_lob_id(lob_id.ptr(), lob_id.length());
    max_row_key_.set_seq_id(seq_id.ptr(), seq_id.length());
    ret = build(range);
  }
  return ret;
}

int ObLobAuxTableRangeBuilder::build_full_range(ObNewRange &range)
{
  min_row_key_.make_min_row_key();
  max_row_key_.make_max_row_key();
  return build(range);
}

//==================== ObLobAuxTableBaseIterator ====================

int ObLobAuxTableBaseIterator::open(
    const LobAuxTarget target,
    ObLobReadContext &read_ctx,
    ObPersistentLobAdaptor *adaptor,
    ObIAllocator *stmt_allocator)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  adaptor_ = adaptor;
  bool is_get = read_ctx.is_get();
  scan_param_.key_ranges_.reuse();
  if (!read_ctx.tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id invalid", KR(ret), K(read_ctx));
  } else if (OB_FAIL(build_range(read_ctx, range))) {
    LOG_WARN("build_range fail", KR(ret), K(read_ctx));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param_), K(range), K(read_ctx));
  } else if (OB_FAIL(scan(target, read_ctx, is_get, stmt_allocator, &scan_allocator_))) {
    LOG_WARN("scan fail", K(ret), K(read_ctx), KPC(this));
  } else {
    is_opened_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObLobAuxTableBaseIterator::open(
    ObTabletID &main_tablet_id, 
    ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  main_tablet_id_ = main_tablet_id;
  lob_meta_tablet_id_ = scan_param_.tablet_id_;
  lob_piece_tablet_id_ = lob_piece_tablet_id;
  scan_param_.scan_allocator_ = &scan_allocator_;
  if (OB_NOT_NULL(adaptor_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor is not null", K(ret));
  } else if (OB_FAIL(MTL(ObLobManager *)->get_table_param(scan_param_.table_param_))) {
    LOG_WARN("fail to get table param", K(ret));
  } else if (OB_FAIL(oas->table_scan(scan_param_, row_iter_))) {
    LOG_WARN("do table scan fail", K(ret), KPC(this));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObLobAuxTableBaseIterator::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(revert_scan_iter())) {
    LOG_WARN("revert_scan_iter fail", K(ret), KPC(this));
  } else {
    main_tablet_id_.reset();
    lob_meta_tablet_id_.reset();
    lob_piece_tablet_id_.reset();
    MEMSET(seq_id_local_buf_, 0, sizeof(seq_id_local_buf_));
    row_iter_ = nullptr;
    adaptor_ = nullptr;
    scan_allocator_.reset();
    is_opened_ = false;
  }
  return ret;
}

int ObLobAuxTableBaseIterator::rescan(ObLobReadContext &read_ctx)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  read_ctx.lob_meta_tablet_id_ =  lob_meta_tablet_id_;
  read_ctx.lob_piece_tablet_id_ =  lob_piece_tablet_id_;
  scan_param_.key_ranges_.reuse();
  scan_param_.is_get_ = read_ctx.is_get();

  // update scan order
  scan_param_.scan_flag_.scan_order_ = read_ctx.scan_backward_ ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  // update timeout
  scan_param_.timeout_ = read_ctx.timeout_;
  scan_param_.for_update_wait_timeout_ = scan_param_.timeout_;
  scan_allocator_.reuse();
  if (OB_FAIL(build_range(read_ctx, range))) {
    LOG_WARN("build_range fail", K(ret), K(read_ctx), KPC(this));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param_), K(range));
  } else if (OB_FAIL(do_rescan(read_ctx))) {
    LOG_WARN("rescan fail", K(ret), K(read_ctx), KPC(this));
  }
  return ret;
}


int ObLobAuxTableBaseIterator::rescan(ObNewRange &range) {
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  
  if (!main_tablet_id_.is_valid() || !lob_meta_tablet_id_.is_valid() || !lob_piece_tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id is invalid", K(ret), K(main_tablet_id_), K(lob_meta_tablet_id_), K(lob_piece_tablet_id_));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_ERROR("access service is null", K(ret));
  } else {
    // 清空旧的 key_ranges
    scan_param_.key_ranges_.reuse();
    if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
      LOG_WARN("push key range fail", K(ret), K(range));
    } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, row_iter_))) {
      LOG_WARN("reuse scan iter fail", K(ret), KPC(this));
    } else if (OB_FAIL(oas->table_rescan(scan_param_, row_iter_))) {
      LOG_WARN("do table rescan fail", K(ret), KPC(this));
    } else {
      LOG_DEBUG("rescan lob meta table for lob_id success", KPC(this));
    }
  }
  return ret;
}
int ObLobAuxTableBaseIterator::build_range(const ObLobReadContext &read_ctx, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  const ObLobId *lob_id = nullptr;
  if (read_ctx.is_full_table_scan_) {
    if (OB_FAIL(range_builder_.build_full_range(range))) {
      LOG_WARN("build_full_range fail", K(ret), K(read_ctx));
    } else {
      LOG_INFO("full scan lob meta table", K(read_ctx));
    }
  } else if (OB_UNLIKELY(OB_INVALID_ID == read_ctx.column_idx() || !read_ctx.src_loc_wrapper_.is_valid())
      || OB_ISNULL(lob_id = read_ctx.src_loc_wrapper_.get_lob_id_ptr())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(read_ctx.column_idx()), K(read_ctx.src_loc_wrapper_));
  } else {
    if (OB_FAIL(range_builder_.build_from_start_seq(
        read_ctx.column_idx(), lob_id->get_string(), read_ctx.seq_id_start_, read_ctx.seq_id_end_, range))) {
      LOG_WARN("build range fail", K(ret), K(read_ctx), K(read_ctx.seq_id_start_), K(read_ctx.seq_id_end_));
    }
  }
  return ret;
}

int ObLobAuxTableBaseIterator::revert_scan_iter()
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = nullptr;
  if (OB_ISNULL(row_iter_)) { // skip when is null
  } else if (OB_ISNULL(oas = MTL(ObAccessService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get access service fail", K(ret));
  } else if (OB_FALSE_IT(row_iter_->reset())) {
  } else if (OB_FAIL(oas->revert_scan_iter(row_iter_))) {
    LOG_ERROR("revert scan iterator failed", K(ret), KPC(row_iter_));
  } else {
    row_iter_ = nullptr;
    scan_allocator_.reset();
    LOG_DEBUG("release lob meta tablet scan iter success", K_(main_tablet_id), K_(lob_meta_tablet_id), K_(lob_piece_tablet_id));
  }
  return ret;
}

int ObLobAuxTableBaseIterator::scan(
    const LobAuxTarget target,
    ObLobReadContext &read_ctx,
    const bool is_get,
    ObIAllocator *stmt_allocator,
    ObIAllocator *scan_allocator)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_ERROR("access service is null", K(ret), K(read_ctx), KPC(this));
  } else if (OB_FAIL(ObLobUtil::refresh_lob_tablet_id(read_ctx))) {
    LOG_WARN("prepare_lob_tablet_id fail", K(ret));
  } else if (OB_FAIL(adaptor_->prepare_scan_param_schema_version(target, read_ctx, scan_param_))) {
    LOG_WARN("prepare_scan_param_schema_version fail", K(ret));
  } else if (OB_FAIL(adaptor_->prepare_table_scan_param(
      target, read_ctx, is_get, scan_param_, stmt_allocator, scan_allocator))) {
    LOG_WARN("build common scan param fail", K(ret), K(is_get), K(read_ctx), KPC(this));
  } else if (OB_FAIL(oas->table_scan(scan_param_, row_iter_))) {
    LOG_WARN("do table scan fail", K(ret), K(is_get), K(read_ctx), KPC(this));
  } else {
    main_tablet_id_ = read_ctx.tablet_id_;
    lob_meta_tablet_id_ = read_ctx.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = read_ctx.lob_piece_tablet_id_;
    LOG_DEBUG("scan lob meta table sucess",  K_(main_tablet_id),
        K_(lob_meta_tablet_id), K_(lob_piece_tablet_id),
        K(is_get), K(read_ctx), KPC(this));
  }
  return ret;
}

int ObLobAuxTableBaseIterator::do_rescan(ObLobReadContext &read_ctx)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (read_ctx.tablet_id_ != main_tablet_id_
      || read_ctx.lob_meta_tablet_id_ != lob_meta_tablet_id_
      || read_ctx.lob_piece_tablet_id_ != lob_piece_tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id not match", K(ret), K(read_ctx), KPC(this));
  } else if (! main_tablet_id_.is_valid() || ! lob_meta_tablet_id_.is_valid() || ! lob_piece_tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id is invalid", K(ret), K(read_ctx), KPC(this));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_ERROR("access service is null", K(ret), K(read_ctx), KPC(this));
  } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, row_iter_))) {
    LOG_WARN("reuse scan iter fail", K(ret), KPC(this), K(read_ctx));
  } else if (OB_FAIL(oas->table_rescan(scan_param_, row_iter_))) {
    LOG_WARN("do table rescan fail", K(ret), K(read_ctx), KPC(this));
  } else {
    LOG_DEBUG("rescan lob meta table sucess", K(read_ctx), KPC(this));
  }
  return ret;
}

//==================== ObLobAuxTableIterator ====================
template<typename T>
int ObLobAuxTableIterator<T>::get_next_row(T &row)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *datum_row = nullptr;
  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter_);
  if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob aux scan iter is null", K(ret), KPC(this));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next lob aux row fail", K(ret), KPC(this));
    }
  } else if (OB_ISNULL(datum_row)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob aux row is null", K(ret), KPC(this));
  } else {
    // pick the row->info transform by element type
    if constexpr (std::is_same<T, ObLobMetaInfo>::value) {
      ret = ObLobMetaUtil::transform_from_row_to_info(datum_row, row, false/*with_extra_rowkey*/);
    } else {
      ret = ObLobPieceUtil::transform_from_row_to_info(datum_row, row, false/*with_extra_rowkey*/);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("get aux info from row fail", K(ret), K(datum_row), KPC(this));
    }
  }
  return ret;
}

// explicit instantiation: only these two element types are supported
template class ObLobAuxTableIterator<ObLobMetaInfo>;
template class ObLobAuxTableIterator<ObLobPieceInfo>;


//==================== ObLobPersistUpdateSingleRowIter ====================
int ObLobPersistUpdateSingleRowIter::init(ObLobUpdateContext *update_ctx,
                                          blocksstable::ObDatumRow *old_row,
                                          blocksstable::ObDatumRow *new_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(update_ctx) || OB_ISNULL(old_row) || OB_ISNULL(new_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context or row is null", K(ret), KP(update_ctx), KP(old_row), KP(new_row));
  } else {
    write_ctx_ = update_ctx;
    old_row_ = old_row;
    new_row_ = new_row;
  }
  return ret;
}

int ObLobPersistUpdateSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_row_) || OB_ISNULL(new_row_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get next row failed, null val.", K(ret), K(old_row_), K(new_row_));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (!got_old_row_) {
    row = old_row_;
    got_old_row_ = true;
  } else {
    row = new_row_;
    got_old_row_ = false;
    is_iter_end_ = true;
  }
  return ret;
}

//==================== ObLobPersistInsertSingleRowIter ====================
int ObLobPersistInsertSingleRowIter::init(
    ObLobInsertContext *insert_ctx,
    const LobAuxTarget target,
    blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_ctx) || OB_ISNULL(row) || OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(insert_ctx), KP(row), "target", get_lob_aux_target_str(target));
  } else {
    write_ctx_ = insert_ctx;
    target_ = target;
    row_ = row;
  }
  return ret;
}

int ObLobPersistInsertSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_ctx_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(write_ctx_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(write_ctx_->update_seq_no(target_))) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::init(ObLobDeleteContext *delete_ctx, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(delete_ctx) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("context or row is null", K(ret), KP(delete_ctx), KP(row));
  } else {
    write_ctx_ = delete_ctx;
    row_ = row;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_ctx_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(write_ctx_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

//==================== ObLobWriteStats ====================
void ObLobWriteStats::reset()
{
  byte_len_ = 0;
  char_len_ = 0;
  chunk_count_ = 0;
}

int ObLobWriteStats::add_chunk(const ObLobChunk &chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == chunk.byte_len_
                  || 0 == chunk.char_len_
                  || chunk.char_len_ > chunk.byte_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob chunk", KR(ret), K(chunk));
  } else if (OB_UNLIKELY(UINT64_MAX - byte_len_ < chunk.byte_len_
                         || UINT64_MAX - char_len_ < chunk.char_len_
                         || UINT64_MAX == chunk_count_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("lob write stats overflow", KR(ret), KPC(this), K(chunk));
  } else {
    byte_len_ += chunk.byte_len_;
    char_len_ += chunk.char_len_;
    ++chunk_count_;
  }
  return ret;
}

//==================== ObLobInTablePieceInsertIter ====================
ObLobInTablePieceInsertIter::ObLobInTablePieceInsertIter()
  : is_inited_(false),
    is_end_(false),
    query_iter_(nullptr),
    allocator_(nullptr),
    chunk_size_(0),
    lob_col_idx_(OB_INVALID_INDEX_INT64),
    lob_id_(),
    meta_array_(nullptr),
    write_stats_(nullptr),
    reuse_start_seq_id_(false),
    data_meta_lob_info_(),
    seq_id_gen_(),
    chunk_(),
    piece_info_(),
    row_()
{}

int ObLobInTablePieceInsertIter::init(
    ObLobQueryIter *query_iter,
    common::ObIAllocator *allocator,
    const int64_t chunk_size,
    const int64_t lob_col_idx,
    const common::ObLobId &lob_id,
    common::ObIArray<ObLobMetaInfo> *meta_array,
    ObLobWriteStats *write_stats)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init in-table piece insert iterator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(query_iter) || OB_ISNULL(allocator)
             || OB_ISNULL(meta_array) || OB_ISNULL(write_stats)
             || OB_UNLIKELY(chunk_size < OB_MIN_LOB_CHUNK_SIZE
                            || chunk_size > OB_MAX_LOB_CHUNK_SIZE)
             || OB_UNLIKELY(lob_col_idx < 0)
             || OB_UNLIKELY(!lob_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in-table piece insert iterator argument", KR(ret),
                KP(query_iter), KP(allocator), K(chunk_size), K(lob_col_idx),
                K(lob_id), KP(meta_array), KP(write_stats));
  } else if (OB_FAIL(row_.init(ObLobPieceInfo::COLUMN_CNT))) {
    LOG_WARN("initialize piece datum row failed", KR(ret));
  } else {
    meta_array->reset();
    write_stats->reset();
    is_inited_ = true;
    is_end_ = false;
    query_iter_ = query_iter;
    allocator_ = allocator;
    chunk_size_ = chunk_size;
    lob_col_idx_ = lob_col_idx;
    lob_id_ = lob_id;
    meta_array_ = meta_array;
    write_stats_ = write_stats;
    seq_id_gen_.set_allocator(allocator_);
  }
  return ret;
}

int ObLobInTablePieceInsertIter::set_start_seq_id(
    const common::ObString &start_seq_id,
    const bool reuse_start_seq_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("piece insert iterator is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!ObLobSeqId(start_seq_id).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid append start seq id", KR(ret), K(start_seq_id));
  } else {
    ObString mutable_start_seq_id(start_seq_id);
    seq_id_gen_.set_seq_id(mutable_start_seq_id);
    reuse_start_seq_id_ = reuse_start_seq_id;
  }
  return ret;
}

int ObLobInTablePieceInsertIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("in-table piece insert iterator is not initialized", KR(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    ObLobMetaInfo meta_info;
    if (OB_FAIL(query_iter_->get_next_chunk(chunk_, chunk_size_))) {
      if (OB_ITER_END == ret) {
        is_end_ = true;
      } else {
        LOG_WARN("get next source lob chunk failed", KR(ret), KPC(this));
      }
    } else {
      chunk_.lob_col_idx_ = lob_col_idx_;
      chunk_.lob_id_ = lob_id_;
      chunk_.lob_extra_info_.lob_chunk_type_.set_in_table();
      chunk_.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
      if (reuse_start_seq_id_) {
        seq_id_gen_.get_seq_id(chunk_.seq_id_);
        reuse_start_seq_id_ = false;
      } else if (OB_FAIL(seq_id_gen_.get_next_seq_id(chunk_.seq_id_))) {
        LOG_WARN("generate lob chunk seq id failed", KR(ret), KPC(this));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(write_stats_->add_chunk(chunk_))) {
        LOG_WARN("update lob write stats failed", KR(ret), K(chunk_), KPC(this));
      } else if (OB_FAIL(chunk_.to_meta(*allocator_, meta_info))) {
        LOG_WARN("transform lob chunk to meta failed", KR(ret), K(chunk_), KPC(this));
      } else if (OB_FAIL(meta_array_->push_back(meta_info))) {
        LOG_WARN("append lob meta failed", KR(ret), KPC(this));
      } else if (OB_FAIL(chunk_.to_piece(piece_info_))) {
        LOG_WARN("transform lob chunk to piece failed", KR(ret), K(chunk_), KPC(this));
      } else {
        ObPersistentLobAdaptor::set_lob_row(row_, piece_info_);
        row = &row_;
      }
    }
  }
  return ret;
}

void ObLobInTablePieceInsertIter::reset()
{
  seq_id_gen_.reset();
  seq_id_gen_.set_allocator(nullptr);
  is_inited_ = false;
  is_end_ = false;
  query_iter_ = nullptr;
  allocator_ = nullptr;
  chunk_size_ = 0;
  lob_col_idx_ = OB_INVALID_INDEX_INT64;
  lob_id_.reset();
  meta_array_ = nullptr;
  write_stats_ = nullptr;
  reuse_start_seq_id_ = false;
  data_meta_lob_info_.reset();
  chunk_.reset();
  piece_info_.reset();
  row_.reset();
}

//==================== OutTable LOB Write Iterator ====================
ObLobOutTableInsertIter::ObLobOutTableInsertIter()
  : is_inited_(false),
    is_end_(false),
    insert_ctx_(nullptr),
    query_iter_(nullptr),
    allocator_(nullptr),
    chunk_size_(0),
    chunk_count_(0),
    lob_col_idx_(OB_INVALID_INDEX_INT64),
    lob_id_(),
    meta_array_(nullptr),
    idx_(0),
    write_stats_(nullptr),
    reuse_start_seq_id_(false),
    seq_id_gen_(),
    chunk_()
{
  reset();
}

ObLobOutTableInsertIter::~ObLobOutTableInsertIter()
{
  reset();
}

int ObLobOutTableInsertIter::init(
    ObLobInsertContext *write_ctx,
    ObLobQueryIter *query_iter,
    ObIAllocator *allocator,
    const int64_t chunk_size,
    const int64_t chunk_count,
    const int64_t lob_col_idx,
    const ObLobId &lob_id,
    ObIArray<ObLobMetaInfo> *meta_array,
    ObLobWriteStats *write_stats)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init outtable write iterator twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(write_ctx) || OB_ISNULL(query_iter) || OB_ISNULL(allocator)
             || OB_ISNULL(meta_array) || OB_ISNULL(write_stats)
             || OB_UNLIKELY(chunk_size != ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE)
             || OB_UNLIKELY(lob_col_idx < 0)
             || OB_UNLIKELY(chunk_count <= 0)
             || OB_UNLIKELY(!lob_id.is_valid())
             || OB_UNLIKELY(!write_ctx->lob_meta_tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable write iterator argument", KR(ret),
                KP(write_ctx), KP(query_iter), KP(allocator), K(chunk_size), K(lob_col_idx),
                K(lob_id), KP(meta_array), KP(write_stats));
  } else {
    meta_array->reset();
    write_stats->reset();
    is_inited_ = true;
    is_end_ = false;
    insert_ctx_ = write_ctx;
    query_iter_ = query_iter;
    allocator_ = allocator;
    chunk_size_ = chunk_size;
    chunk_count_ = chunk_count;
    lob_col_idx_ = lob_col_idx;
    lob_id_ = lob_id;
    meta_array_ = meta_array;
    write_stats_ = write_stats;
    seq_id_gen_.set_allocator(allocator_);
    macro_id_array_.set_attr(ObMemAttr(MTL_ID(), "LobMacroIds"));
  }
  return ret;
}

int ObLobOutTableInsertIter::set_start_seq_id(
    const common::ObString &start_seq_id,
    const bool reuse_start_seq_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable write iterator is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!ObLobSeqId(start_seq_id).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable write start seq id", KR(ret), K(start_seq_id));
  } else {
    ObString mutable_start_seq_id(start_seq_id);
    seq_id_gen_.set_seq_id(mutable_start_seq_id);
    reuse_start_seq_id_ = reuse_start_seq_id;
  }
  return ret;
}

int ObLobOutTableInsertIter::prepare_batch_(
    ObIAllocator &batch_allocator,
    ObIArray<ObString> &chunk_data,
    ObIArray<ObOuttableLobControlKey> &control_keys)
{
  int ret = OB_SUCCESS;
  chunk_data.reuse();
  control_keys.reuse();
  while (OB_SUCC(ret) && !is_end_
      && control_keys.count() < ObLobConcurrentPuter::DEFAULT_CAPACITY) {
    if (OB_FAIL(query_iter_->get_next_chunk(chunk_, chunk_size_))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_end_ = true;
      } else {
        LOG_WARN("get next outtable lob chunk failed", KR(ret), KPC(this));
      }
    } else if (OB_UNLIKELY(idx_ >= macro_id_array_.count()
        || chunk_.lob_data_.empty() || chunk_.lob_data_.length() > chunk_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid outtable lob chunk or macro id count", KR(ret), K(chunk_), KPC(this));
    } else {
      ObLobMetaInfo meta_info;
      ObOuttableLobControlKey key;
      ObString data;
      key.macro_id_ = macro_id_array_.at(idx_);
      chunk_.lob_col_idx_ = lob_col_idx_;
      chunk_.lob_id_ = lob_id_;
      chunk_.lob_extra_info_.lob_chunk_type_.set_out_table();
      chunk_.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
      if (reuse_start_seq_id_) {
        seq_id_gen_.get_seq_id(chunk_.seq_id_);
        reuse_start_seq_id_ = false;
      } else if (OB_FAIL(seq_id_gen_.get_next_seq_id(chunk_.seq_id_))) {
        LOG_WARN("generate outtable lob chunk seq id failed", KR(ret), KPC(this));
      }
      if (FAILEDx(ObLobOutTableIO::serialize_macro_id(
          *allocator_, key.macro_id_, chunk_.lob_block_id_))) {
        LOG_WARN("serialize outtable lob macro id failed", KR(ret), K(key));
      } else if (OB_FAIL(ob_write_string(batch_allocator, chunk_.lob_data_, data))) {
        LOG_WARN("failed to copy outtable lob batch data", KR(ret), K(chunk_));
      } else {
        key.hash_ = data.hash();
        // Query and sequence buffers are reused; copy the data and Meta before the next read.
        if (OB_FAIL(chunk_data.push_back(data))) {
          LOG_WARN("failed to append outtable lob batch data", KR(ret));
        } else if (OB_FAIL(control_keys.push_back(key))) {
          LOG_WARN("failed to append outtable lob control key", KR(ret), K(key));
        } else if (OB_FAIL(write_stats_->add_chunk(chunk_))) {
          LOG_WARN("update outtable lob write stats failed", KR(ret), K(chunk_));
        } else if (OB_FAIL(chunk_.to_meta(*allocator_, meta_info))) {
          LOG_WARN("transform outtable lob chunk to meta failed", KR(ret), K(chunk_));
        } else if (OB_FAIL(meta_array_->push_back(meta_info))) {
          LOG_WARN("append outtable lob meta failed", KR(ret));
        } else {
          ++idx_;
        }
      }
    }
  }
  return ret;
}

int ObLobOutTableInsertIter::write_batch_(
    ObLobConcurrentPuter &puter,
    ObOuttableLobControlManager &control_manager)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator batch_allocator(ObMemAttr(insert_ctx_->tenant_id_, "LobCtrlBatch"));
  ObSEArray<ObString, ObLobConcurrentPuter::DEFAULT_CAPACITY> chunk_data;
  ObSEArray<ObOuttableLobControlKey, ObLobConcurrentPuter::DEFAULT_CAPACITY> control_keys;
  if (OB_FAIL(prepare_batch_(batch_allocator, chunk_data, control_keys))) {
    LOG_WARN("failed to prepare outtable lob batch", KR(ret), KPC(this));
  } else if (!control_keys.empty()) {
    if (OB_FAIL(control_manager.insert(control_keys))) {
      LOG_WARN("failed to insert outtable lob control batch", KR(ret), K(control_keys));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < control_keys.count(); ++i) {
      if (OB_FAIL(insert_ctx_->outtable_lob_control_keys_->push_back(control_keys.at(i)))) {
        LOG_WARN("failed to collect outtable lob control key", KR(ret), "key", control_keys.at(i));
      }
    }
    // Upload only after this batch's control rows commit. submit owns a data copy,
    // so there is no need to wait for all uploads in the batch to finish.
    for (int64_t i = 0; OB_SUCC(ret) && i < control_keys.count(); ++i) {
      if (OB_FAIL(puter.submit(control_keys.at(i).macro_id_, chunk_data.at(i)))) {
        LOG_WARN("failed to submit outtable lob batch object", KR(ret), "key", control_keys.at(i));
      }
    }
  }
  return ret;
}

int ObLobOutTableInsertIter::write_all()
{
  int ret = OB_SUCCESS;
  ObLobConcurrentPuter puter;
  ObOuttableLobControlManager control_manager;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable write iterator is not initialized", KR(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (!insert_ctx_->allow_outtable_lob_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("outtable lob writes are not supported by this write path", KR(ret), KPC(insert_ctx_));
  } else if (OB_ISNULL(insert_ctx_->outtable_lob_control_keys_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable lob control key collector is null", KR(ret), KPC(insert_ctx_));
  } else if (OB_FAIL(control_manager.init(insert_ctx_->tenant_id_))) {
    LOG_WARN("initialize outtable lob control manager failed", KR(ret), KPC(insert_ctx_));
  } else if (OB_FAIL(puter.init(insert_ctx_->timeout_, chunk_size_, chunk_count_))) {
    LOG_WARN("initialize outtable concurrent uploader failed", KR(ret), KPC(this));
  } else if (OB_FAIL(ObLobOutTableIO::alloc_macro_ids(*insert_ctx_, chunk_count_, macro_id_array_))) {
    LOG_WARN("failed to alloc macro ids", KR(ret), KPC(this));
  } else {
    while (OB_SUCC(ret) && !is_end_) {
      if (OB_FAIL(write_batch_(puter, control_manager))) {
        LOG_WARN("failed to write outtable lob batch", KR(ret), KPC(this));
      }
    }

    // finish() must be called to wait for all in-flight requests.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(puter.finish())) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("finish outtable lob object writes failed", KR(tmp_ret), KR(ret), KPC(this));
    }
    if (OB_SUCC(ret)
        && OB_FAIL(OB_E(common::EventTable::ERRSIM_LOB_OUTTABLE_AFTER_PUT) OB_SUCCESS)) {
      LOG_WARN("injected failure after outtable lob object writes", KR(ret), KPC(this));
    }
  }
  return ret;
}

void ObLobOutTableInsertIter::reset()
{
  seq_id_gen_.reset();
  seq_id_gen_.set_allocator(nullptr);
  is_inited_ = false;
  is_end_ = false;
  insert_ctx_ = nullptr;
  query_iter_ = nullptr;
  allocator_ = nullptr;
  chunk_size_ = 0;
  chunk_count_ = 0;
  lob_col_idx_ = OB_INVALID_INDEX_INT64;
  lob_id_.reset();
  meta_array_ = nullptr;
  write_stats_ = nullptr;
  reuse_start_seq_id_ = false;
  chunk_.reset();
  macro_id_array_.reset();
  idx_ = 0;
}

//==================== ObLobMetaTableBatchInsertIter ====================
int ObLobMetaTableBatchInsertIter::init(ObIArray<ObLobMetaInfo> *array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(array));
  } else if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(array));
  } else if (OB_FAIL(row_.init(ObLobMetaInfo::COLUMN_CNT))) {
    LOG_WARN("init datum row fail", KR(ret));
  } else {
    array_ = array;
    is_inited_ = true;
    idx_ = 0;
  }
  return ret;
}

void ObLobMetaTableBatchInsertIter::reset()
{
  array_ = nullptr;
  is_inited_ = false;
  idx_ = 0;
}

int ObLobMetaTableBatchInsertIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_ISNULL(array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array should not be nullptr", KR(ret), KPC(this));
  } else if (idx_ >= array_->count()) {
    ret = OB_ITER_END;
  } else {
    ObPersistentLobAdaptor::set_lob_row(row_, array_->at(idx_));
    OB_LOG(DEBUG, "debug insert lob info", KR(ret), K(array_->at(idx_)));
    row = &row_;
    idx_++;
  }
  return ret;
}

//==================== ObLobAuxTableBatchDeleteIterator ====================

template<typename T>
void ObLobAuxTableBatchDeleteIterator<T>::reset()
{
  is_inited_ = false;
  delete_ctx_ = nullptr;
  scan_iter_ = nullptr;
  row_.reset();
}

template<typename T>
int ObLobAuxTableBatchDeleteIterator<T>::init(
    ObLobDeleteContext *delete_ctx,
    ObLobAuxTableIterator<T> *scan_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(delete_ctx) || OB_ISNULL(scan_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(delete_ctx), KP(scan_iter));
  } else {
    if (OB_FAIL(row_.init(T::COLUMN_CNT))) {
      LOG_WARN("failed init row", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    delete_ctx_ = delete_ctx;
    scan_iter_ = scan_iter;
  }
  return ret;
}

template<typename T>
int ObLobAuxTableBatchDeleteIterator<T>::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(scan_iter_->get_next_row(row_info_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    // pick the row->info transform by element type
    if constexpr (std::is_same<T, ObLobMetaInfo>::value) {
      if (OB_FAIL(ObLobMetaUtil::transform_from_info_to_row(row_info_, &row_, false/*with_extra_rowkey*/))) {
        LOG_WARN("failed transform row from info", K(ret), K(row_info_));
      }
    } else {
      if (OB_FAIL(ObLobPieceUtil::transform_from_info_to_row(row_info_, &row_, false/*with_extra_rowkey*/))) {
        LOG_WARN("failed transform row from info", K(ret), K(row_info_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_ctx_->update_seq_no(
        std::is_same<T, ObLobMetaInfo>::value ? LobAuxTarget::META : LobAuxTarget::PIECE))) {
      LOG_WARN("update seq no fail", K(ret), KPC(delete_ctx_));
    } else {
      row = &row_;
    }
  }
  return ret;
}


} // storage
} // oceanbase
