/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <algorithm>

#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

namespace
{
void cancel_and_reset_range_data_handle(ObMicroBlockDataHandle &data_handle)
{
  if (data_handle.io_handle_.is_valid() && !data_handle.io_handle_.is_finished()) {
    data_handle.io_handle_.cancel();
  }
  data_handle.reset();
}
} // namespace

bool ObInvSSTableRangeIter::is_valid_range_key(
    const ObDatumRowkey &key,
    const int64_t term_col_cnt,
    const int64_t schema_rowkey_cnt,
    const bool is_start_key)
{
  const int64_t datum_cnt = key.get_datum_cnt();
  bool is_valid = key.is_static_rowkey();
  if (!is_valid && datum_cnt == term_col_cnt) {
    is_valid = true;
  } else if (!is_valid && datum_cnt == schema_rowkey_cnt) {
    const ObStorageDatum &doc_id = key.get_datum(schema_rowkey_cnt - 1);
    is_valid = is_start_key ? doc_id.is_min() : doc_id.is_max();
  }
  return is_valid;
}

int ObInvSSTableRangeIter::validate_scan_range(
    const ObDatumRange &scan_range,
    const int64_t term_col_cnt,
    const int64_t schema_rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(term_col_cnt < 0
      || schema_rowkey_cnt <= 0
      || term_col_cnt + 1 != schema_rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted index rowkey shape", K(ret),
        K(term_col_cnt), K(schema_rowkey_cnt));
  } else if (OB_UNLIKELY(!is_valid_range_key(
      scan_range.get_start_key(), term_col_cnt, schema_rowkey_cnt,
      true /*is_start_key*/)
      || !is_valid_range_key(
          scan_range.get_end_key(), term_col_cnt, schema_rowkey_cnt,
          false /*is_start_key*/))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted index scan range", K(ret),
        K(term_col_cnt), K(schema_rowkey_cnt), K(scan_range));
  }
  return ret;
}

int ObInvSSTableRangeIter::prepare_index_scan_range(bool &is_empty_range)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  const ObDatumRowkey start_key = scan_range_.get_start_key();
  const ObDatumRowkey end_key = scan_range_.get_end_key();
  const bool pad_start = !scan_range_.get_start_key().is_static_rowkey()
      && scan_range_.get_start_key().get_datum_cnt() == term_col_cnt_;
  const bool pad_end = !scan_range_.get_end_key().is_static_rowkey()
      && scan_range_.get_end_key().get_datum_cnt() == term_col_cnt_;
  is_empty_range = false;
  if (pad_start && OB_FAIL(start_key.to_multi_version_rowkey(
      scan_range_.is_left_closed(), *allocator_, scan_range_.get_start_key()))) {
    LOG_WARN("failed to append doc id to inverted index range start", K(ret));
  } else if (pad_end && OB_FAIL(end_key.to_multi_version_rowkey(
        scan_range_.is_right_open(), *allocator_, scan_range_.get_end_key()))) {
    LOG_WARN("failed to append doc id to inverted index range end", K(ret));
  } else {
    if (pad_start) {
      scan_range_.set_left_closed();
    }
    if (pad_end) {
      scan_range_.set_right_closed();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scan_range_.get_start_key().compare(
      scan_range_.get_end_key(), read_info_->get_datum_utils(), cmp_ret))) {
    LOG_WARN("failed to compare physical inverted index range", K(ret), K_(scan_range));
  } else {
    is_empty_range = cmp_ret > 0 || (0 == cmp_ret && (scan_range_.is_left_open() || scan_range_.is_right_open()));
  }
  return ret;
}

int ObInvSSTableRangeIter::check_index_row_in_range(
    const ObMicroIndexInfo &index_info,
    const bool is_right_boundary,
    bool &iter_end) const
{
  int ret = OB_SUCCESS;
  iter_end = false;
  if (is_right_boundary) {
    int cmp_ret = 0;
    if (OB_FAIL(index_info.endkey_.compare(
        scan_range_.get_end_key(), read_info_->get_datum_utils(), cmp_ret))) {
      LOG_WARN("failed to compare trailing inverted index row", K(ret), K(index_info));
    } else {
      iter_end = cmp_ret > 0 || (0 == cmp_ret && scan_range_.is_right_open());
    }
  }
  return ret;
}

ObInvSSTableRangeIter::ObInvSSTableRangeIter()
  : sstable_(nullptr),
    read_info_(nullptr),
    allocator_(nullptr),
    tablet_handle_(),
    table_handle_(),
    scan_range_(),
    index_scan_param_(),
    index_scanner_(),
    prefetched_group_(),
    prefetched_first_block_idx_(0),
    prefetched_last_block_idx_(-1),
    curr_index_info_(),
    meta_scanner_(),
    curr_meta_(),
    data_io_allocator_(),
    doc_id_prefetcher_(),
    posting_data_handle_(),
    posting_micro_reader_(),
    curr_posting_micro_block_(),
    curr_posting_micro_locator_(),
    curr_posting_block_loaded_(false),
    posting_block_data_(),
    posting_value_data_handle_(),
    posting_value_micro_reader_(),
    curr_value_micro_block_(),
    curr_value_micro_locator_(),
    curr_value_block_loaded_(false),
    posting_value_block_reader_(),
    value_row_header_(),
    need_posting_value_(false),
    enable_doc_id_drain_(false),
    curr_row_idx_(-1),
    curr_id_datum_(),
    curr_value_datum_(),
    curr_id_storage_(0),
    curr_id_(nullptr),
    term_buf_(),
    curr_term_(),
    endkey_buf_(),
    materialized_endkey_(),
    term_col_cnt_(0),
    doc_id_col_idx_(-1),
    term_materialized_(false),
    prepared_doc_ids_(nullptr),
    prepared_doc_count_(0),
    next_doc_offset_(0),
    is_opened_(false),
    is_end_(false)
{
}

ObInvSSTableRangeIter::~ObInvSSTableRangeIter()
{
  reset();
}

void ObInvSSTableRangeIter::reset()
{
  prepared_doc_ids_ = nullptr;
  prepared_doc_count_ = 0;
  next_doc_offset_ = 0;
  prefetched_group_.reset();
  prefetched_first_block_idx_ = 0;
  prefetched_last_block_idx_ = -1;
  posting_block_data_.reset();
  posting_value_block_reader_.reuse();
  posting_micro_reader_.reuse();
  posting_value_micro_reader_.reuse();
  curr_posting_micro_block_.reset();
  curr_value_micro_block_.reset();
  reset_row_cursor();
  doc_id_prefetcher_.quiesce();
  cancel_and_reset_range_data_handle(posting_data_handle_);
  cancel_and_reset_range_data_handle(posting_value_data_handle_);
  data_io_allocator_.reset();
  scan_range_.reset();
  index_scan_param_ = ObSSTableIndexScanParam();
  index_scanner_.reset();
  curr_index_info_.reset();
  meta_scanner_.reset();
  curr_meta_.reset();
  curr_posting_micro_locator_.reset();
  curr_posting_block_loaded_ = false;
  curr_value_micro_locator_.reset();
  curr_value_block_loaded_ = false;
  need_posting_value_ = false;
  enable_doc_id_drain_ = false;
  curr_term_.reset();
  term_buf_.reset();
  materialized_endkey_.reset();
  endkey_buf_.reset();
  term_col_cnt_ = 0;
  doc_id_col_idx_ = -1;
  term_materialized_ = false;
  table_handle_.reset();
  tablet_handle_.reset();
  sstable_ = nullptr;
  read_info_ = nullptr;
  allocator_ = nullptr;
  is_opened_ = false;
  is_end_ = false;
}

int ObInvSSTableRangeIter::open(
    const ObInvRangeScanParam &param,
    const ObITableReadInfo &read_info,
    ObInvSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  bool is_empty_range = false;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("range iter already opened", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range scan param", K(ret), K(param));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv sstable", K(ret));
  } else if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get inv sstable meta", K(ret));
  } else {
    sstable_ = &sstable;
    read_info_ = &read_info;
    allocator_ = param.allocator_;
    scan_range_ = *param.scan_range_;
    need_posting_value_ = param.need_posting_value_;
    enable_doc_id_drain_ = param.enable_doc_id_drain_;
    const int64_t schema_rowkey_cnt = meta_handle.get_sstable_meta().get_schema_rowkey_column_count();
    doc_id_col_idx_ = schema_rowkey_cnt - 1;
    term_col_cnt_ = doc_id_col_idx_;
    if (OB_UNLIKELY(doc_id_col_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected schema rowkey count", K(ret), K(schema_rowkey_cnt));
    } else if (OB_FAIL(validate_scan_range(
        scan_range_, term_col_cnt_, schema_rowkey_cnt))) {
      LOG_WARN("failed to validate inverted index scan range", K(ret),
          K(schema_rowkey_cnt), K_(scan_range));
    } else if (OB_FAIL(prepare_index_scan_range(is_empty_range))) {
      LOG_WARN("failed to prepare physical inverted index scan range", K(ret), K_(scan_range));
    } else if (is_empty_range) {
      is_opened_ = true;
      is_end_ = true;
    } else if (!enable_doc_id_drain_ && OB_FAIL(term_buf_.init(*allocator_, term_col_cnt_))) {
      LOG_WARN("failed to init term buffer", K(ret), K_(term_col_cnt));
    } else if (!enable_doc_id_drain_
               && OB_FAIL(curr_term_.assign(term_buf_.storage_datums_, term_col_cnt_))) {
      LOG_WARN("failed to assign curr term rowkey", K(ret), K_(term_col_cnt));
    } else if (!enable_doc_id_drain_
               && OB_FAIL(endkey_buf_.init(*allocator_, read_info_->get_rowkey_count()))) {
      LOG_WARN("failed to init endkey buffer", K(ret), "rowkey_count", read_info_->get_rowkey_count());
    } else if (!enable_doc_id_drain_ && OB_FAIL(materialized_endkey_.assign(
        endkey_buf_.storage_datums_, read_info_->get_rowkey_count()))) {
      LOG_WARN("failed to assign materialized endkey", K(ret));
    } else if (OB_FAIL(data_io_allocator_.init(
        nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE, lib::ObMemAttr(MTL_ID(), "InvRangeDataIO")))) {
      LOG_WARN("failed to init data io allocator", K(ret));
    } else if (OB_FAIL(index_scan_param_.init(
        *read_info_,
        param.query_flag_,
        param.tablet_id_,
        ObSSTableIndexScanParam::ScanLevel::LEAF))) {
      LOG_WARN("failed to init index scan param", K(ret), K(param.tablet_id_));
    } else if (OB_FAIL(index_scanner_.init(scan_range_, index_scan_param_, *sstable_, *allocator_))) {
      LOG_WARN("failed to init index scanner", K(ret), K_(scan_range));
    } else if (OB_FAIL(meta_scanner_.init(false /*has_max_score*/))) {
      LOG_WARN("failed to init meta scanner", K(ret));
    } else {
      if (enable_doc_id_drain_) {
        doc_id_prefetcher_.init(data_io_allocator_);
      }
      is_opened_ = true;
      // Row iteration positions the first (term, doc_id) here. Raw doc-id drain
      // asks the index scanner for its first physical group lazily.
      if (!enable_doc_id_drain_ && OB_FAIL(move_to_next_index_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_end_ = true;
        } else {
          LOG_WARN("failed to locate first row", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvSSTableRangeIter::set_table_handles(
    const ObTabletHandle &tablet_handle,
    const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid() || !table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet/table handle", K(ret), K(tablet_handle), K(table_handle));
  } else if (OB_FAIL(tablet_handle_.assign(tablet_handle))) {
    LOG_WARN("failed to assign tablet handle", K(ret));
  } else {
    table_handle_ = table_handle;
  }
  return ret;
}

int ObInvSSTableRangeIter::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened", K(ret));
  } else if (OB_UNLIKELY(enable_doc_id_drain_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row iteration is disabled for raw doc-id drain route", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (!curr_posting_block_loaded_) {
    if (OB_FAIL(load_current_block())) {
      LOG_WARN("failed to load current posting block", K(ret));
    } else if (OB_FAIL(materialize_row(0))) {
      LOG_WARN("failed to materialize first row", K(ret));
    }
  } else if (curr_row_idx_ + 1 < curr_meta_.doc_count_) {
    if (OB_FAIL(materialize_row(curr_row_idx_ + 1))) {
      LOG_WARN("failed to materialize next row", K(ret), K_(curr_row_idx));
    }
  } else if (OB_FAIL(move_to_next_posting_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to move to next posting block", K(ret));
    } else {
      is_end_ = true;
      reset_row_cursor();
    }
  } else if (OB_FAIL(load_current_block())) {
    LOG_WARN("failed to load next posting block", K(ret));
  } else if (OB_FAIL(materialize_row(0))) {
    LOG_WARN("failed to materialize first row of next block", K(ret));
  }
  return ret;
}

int ObInvSSTableRangeIter::prepare_physical_doc_id_group()
{
  int ret = OB_SUCCESS;
  bool consume_prefetched_handle = false;
  ObInvSSTableDocIdPrefetcher::FrontView front;
  int64_t first_block_idx = 0;
  int64_t last_block_idx = -1;
  const uint64_t *doc_ids = nullptr;
  int64_t doc_count = 0;
  if (!doc_id_prefetcher_.is_empty()) {
    curr_index_info_.reset();
    if (OB_FAIL(doc_id_prefetcher_.peek_front(front))) {
      LOG_WARN("failed to get prefetched physical doc-id group", K(ret));
    } else {
      first_block_idx = front.first_block_idx_;
      last_block_idx = front.last_block_idx_;
      curr_posting_micro_block_ = front.block_data_;
      if (OB_FAIL(open_posting_micro_reader(front.macro_id_, front.block_offset_, front.block_size_))) {
        LOG_WARN("failed to open prefetched physical doc-id group", K(ret));
      } else {
        consume_prefetched_handle = true;
      }
    }
  } else if (OB_FAIL(ensure_prefetched_group())) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to prepare next physical index row group", K(ret));
  } else {
    first_block_idx = prefetched_first_block_idx_;
    last_block_idx = prefetched_last_block_idx_;
    curr_index_info_ = prefetched_group_.first_row_;
    if (OB_FAIL(load_current_micro_block())) {
      LOG_WARN("failed to load physical doc-id group", K(ret), K_(curr_index_info));
    }
  }
  if (OB_SUCC(ret)) {
    if (last_block_idx < 0) {
      last_block_idx = posting_micro_reader_.get_block_count() - 1;
    }
    if (OB_FAIL(posting_micro_reader_.get_doc_id_view(
        first_block_idx, last_block_idx, doc_ids, doc_count))) {
      LOG_WARN("failed to get physical doc-id group view", K(ret),
               K(first_block_idx), K(last_block_idx));
    } else {
      if (consume_prefetched_handle) {
        if (OB_FAIL(doc_id_prefetcher_.take_front_handle(posting_data_handle_))) {
          LOG_WARN("failed to consume prefetched physical doc-id group", K(ret));
        }
      } else {
        reset_prefetched_group();
      }
    }
    if (OB_SUCC(ret)) {
      prepared_doc_ids_ = doc_ids;
      prepared_doc_count_ = doc_count;
      next_doc_offset_ = 0;
      if (OB_FAIL(fill_doc_id_prefetch_ring())) {
        LOG_WARN("failed to fill physical doc-id prefetch ring", K(ret));
      }
    }
  }
  return ret;
}

int ObInvSSTableRangeIter::ensure_prefetched_group()
{
  int ret = OB_SUCCESS;
  const bool need_prepare = !prefetched_group_.is_valid();
  if (need_prepare && OB_FAIL(index_scanner_.get_next_block_group(prefetched_group_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next physical index row group", K(ret));
  } else if (need_prepare && OB_FAIL(get_doc_id_block_idx_range(
                                 prefetched_group_, prefetched_first_block_idx_, prefetched_last_block_idx_))) {
    reset_prefetched_group();
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get physical doc-id block range", K(ret));
  }
  return ret;
}

void ObInvSSTableRangeIter::reset_prefetched_group()
{
  prefetched_group_.reset();
  prefetched_first_block_idx_ = 0;
  prefetched_last_block_idx_ = -1;
}

int ObInvSSTableRangeIter::get_doc_id_block_idx_range(
    const ObIndexRowGroup &group,
    int64_t &first_block_idx,
    int64_t &last_block_idx)
{
  int ret = OB_SUCCESS;
  int64_t boundary_block_idx = -1;
  bool trailing_probe = false;
  const bool need_left_range = group.need_agg_range_ || group.is_left_boundary_;
  const bool need_right_range = group.need_agg_range_ || group.is_right_boundary_;
  first_block_idx = 0;
  last_block_idx = -1;
  if (need_left_range) {
    meta_scanner_.reuse();
    if (OB_FAIL(meta_scanner_.open(
        group.first_row_.agg_row_buf_, group.first_row_.agg_buf_size_))) {
      LOG_WARN("failed to open left boundary index row", K(ret));
    } else if (OB_FAIL(meta_scanner_.get_block_idx_range(
        first_block_idx, boundary_block_idx))) {
      LOG_WARN("failed to get left boundary block range", K(ret));
    }
  }
  if (OB_SUCC(ret) && group.is_right_boundary_) {
    if (OB_FAIL(check_index_row_in_range(
        group.last_row_, group.is_right_boundary_, trailing_probe))) {
      LOG_WARN("failed to check trailing index row of group", K(ret), K(group));
    }
  }
  if (OB_SUCC(ret) && need_right_range) {
    if (need_left_range && 1 == group.row_cnt_) {
      last_block_idx = trailing_probe ? first_block_idx - 1 : boundary_block_idx;
    } else {
      meta_scanner_.reuse();
      if (OB_FAIL(meta_scanner_.open(
          group.last_row_.agg_row_buf_, group.last_row_.agg_buf_size_))) {
        LOG_WARN("failed to open right boundary index row", K(ret));
      } else if (OB_FAIL(meta_scanner_.get_block_idx_range(
          boundary_block_idx, last_block_idx))) {
        LOG_WARN("failed to get right boundary block range", K(ret));
      } else if (trailing_probe) {
        last_block_idx = boundary_block_idx - 1;
      }
    }
  }
  if (OB_SUCC(ret) && trailing_probe && last_block_idx < first_block_idx) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObInvSSTableRangeIter::fill_doc_id_prefetch_ring()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !doc_id_prefetcher_.is_full()) {
    const int group_ret = ensure_prefetched_group();
    if (OB_ITER_END == group_ret) {
      break;
    } else if (OB_SUCCESS != group_ret) {
      ret = group_ret;
      LOG_WARN("failed to prepare next physical group for prefetch", K(ret));
    } else {
      const int issue_ret = doc_id_prefetcher_.issue(
          prefetched_group_.first_row_, index_scan_param_.get_tablet_id(),
          index_scan_param_.get_query_flag(), prefetched_first_block_idx_,
          prefetched_last_block_idx_);
      if (OB_EAGAIN == issue_ret) {
        break;
      } else if (OB_SUCCESS != issue_ret) {
        ret = issue_ret;
        LOG_WARN("failed to issue physical doc-id group prefetch", K(ret));
      } else {
        reset_prefetched_group();
      }
    }
  }
  return ret;
}

int ObInvSSTableRangeIter::get_next_doc_ids(
    const int64_t capacity,
    const uint64_t *&doc_ids,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  count = 0;
  if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw doc-id batch capacity", K(ret), K(capacity));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened for raw doc-id batch", K(ret));
  } else if (OB_UNLIKELY(!enable_doc_id_drain_
             || need_posting_value_
             || OB_ISNULL(sstable_)
             || INV_IDX_TYPE_SEARCH != sstable_->get_inv_meta().index_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("raw doc-id batch route is not enabled", K(ret),
             K_(enable_doc_id_drain), K_(need_posting_value));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    if (OB_NOT_NULL(prepared_doc_ids_) && next_doc_offset_ >= prepared_doc_count_) {
      reset_prepared_doc_id_batch();
    }
    if (OB_ISNULL(prepared_doc_ids_)) {
      if (OB_FAIL(prepare_physical_doc_id_group())) {
        if (OB_ITER_END == ret) {
          is_end_ = true;
        }
        LOG_WARN_IGNORE_ITER_END(ret, "failed to prepare physical doc-id group", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      count = std::min(capacity, prepared_doc_count_ - next_doc_offset_);
      doc_ids = prepared_doc_ids_ + next_doc_offset_;
      next_doc_offset_ += count;
    }
  }
  return ret;
}

int ObInvSSTableRangeIter::get_cur_id(const ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  id_datum = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened", K(ret));
  } else if (OB_UNLIKELY(enable_doc_id_drain_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current row access is disabled for raw doc-id drain route", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(curr_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current doc id materialized", K(ret));
  } else {
    id_datum = curr_id_;
  }
  return ret;
}

int ObInvSSTableRangeIter::get_cur_term(const ObDatumRowkey *&term_rowkey)
{
  int ret = OB_SUCCESS;
  term_rowkey = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened", K(ret));
  } else if (OB_UNLIKELY(enable_doc_id_drain_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current row access is disabled for raw doc-id drain route", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!curr_posting_block_loaded_ || curr_row_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row materialized", K(ret), K_(curr_posting_block_loaded), K_(curr_row_idx));
  } else if (!term_materialized_ && OB_FAIL(materialize_term())) {
    LOG_WARN("failed to materialize term on demand", K(ret), K_(curr_index_info));
  } else {
    term_materialized_ = true;
    term_rowkey = &curr_term_;
  }
  return ret;
}

int ObInvSSTableRangeIter::get_cur_pos_list(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  datum = nullptr;
  if (OB_FAIL(validate_posting_value_row_state())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to validate posting value row state", K(ret));
    }
  } else if (OB_UNLIKELY(!sstable_->get_inv_meta().has_position_list_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pos list not available", K(ret), "inv_meta", sstable_->get_inv_meta());
  } else if (OB_FAIL(load_current_value_block())) {
    LOG_WARN("failed to load current value block", K(ret));
  } else if (OB_FAIL(posting_value_block_reader_.get_position_list(
      curr_row_idx_, curr_value_datum_))) {
    LOG_WARN("failed to get position list", K(ret), K_(curr_row_idx));
  } else {
    datum = &curr_value_datum_;
  }
  return ret;
}

int ObInvSSTableRangeIter::get_cur_nested_key_list(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  datum = nullptr;
  if (OB_FAIL(validate_posting_value_row_state())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to validate posting value row state", K(ret));
    }
  } else if (OB_UNLIKELY(!sstable_->get_inv_meta().has_nested_key_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nested key list not available", K(ret), "inv_meta", sstable_->get_inv_meta());
  } else if (OB_FAIL(load_current_value_block())) {
    LOG_WARN("failed to load current value block", K(ret));
  } else if (OB_FAIL(posting_value_block_reader_.get_nested_key(
      curr_row_idx_, curr_value_datum_))) {
    LOG_WARN("failed to get nested key", K(ret), K_(curr_row_idx));
  } else {
    datum = &curr_value_datum_;
  }
  return ret;
}

int ObInvSSTableRangeIter::move_to_next_index_row()
{
  int ret = OB_SUCCESS;
  bool iter_end = false;
  curr_index_info_.reset();
  if (OB_FAIL(index_scanner_.get_next(curr_index_info_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next index row", K(ret));
    }
  } else if (OB_FAIL(check_index_row_in_range(
      curr_index_info_, index_scanner_.reached_last_row_in_range(), iter_end))) {
    LOG_WARN("failed to check inverted index row range", K(ret));
  } else if (iter_end) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!curr_index_info_.has_agg_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index row has no agg data", K(ret), K_(curr_index_info));
  } else if (OB_FAIL(meta_scanner_.open(curr_index_info_.agg_row_buf_, curr_index_info_.agg_buf_size_))) {
    LOG_WARN("failed to open meta scanner on index row agg data", K(ret), K_(curr_index_info));
  } else if (OB_FAIL(load_current_block_meta())) {
    LOG_WARN("failed to load first block meta of index row", K(ret));
  } else {
    term_materialized_ = false;
  }
  return ret;
}

int ObInvSSTableRangeIter::move_to_next_posting_block()
{
  int ret = OB_SUCCESS;
  reset_loaded_block();
  if (OB_FAIL(meta_scanner_.next_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to next posting block", K(ret));
    } else if (OB_FAIL(move_to_next_index_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to move to next index row", K(ret));
      }
    }
  } else if (OB_FAIL(load_current_block_meta())) {
    LOG_WARN("failed to load next posting block meta", K(ret));
  }
  return ret;
}

int ObInvSSTableRangeIter::load_current_block_meta()
{
  int ret = OB_SUCCESS;
  curr_meta_.reset();
  if (OB_FAIL(meta_scanner_.get_cur_block_meta(curr_meta_))) {
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to get current posting block meta", K(ret));
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current posting block meta", K(ret), K_(curr_meta));
  }
  return ret;
}

int ObInvSSTableRangeIter::load_current_micro_block()
{
  int ret = OB_SUCCESS;
  if (need_load_micro_block()) {
    posting_data_handle_.reset();
    ObMicroBlockCacheKey cache_key(MTL_ID(), curr_index_info_);
    ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
    posting_data_handle_.allocator_ = &data_io_allocator_;
    posting_data_handle_.tenant_id_ = MTL_ID();
    posting_data_handle_.macro_block_id_ = curr_index_info_.get_macro_id();
    posting_data_handle_.micro_info_.set(curr_index_info_.get_block_offset(),
                                         curr_index_info_.get_block_size(),
                                         curr_index_info_.get_logic_micro_id(),
                                         curr_index_info_.get_data_checksum());
    if (OB_FAIL(cache.get_cache_block(cache_key, posting_data_handle_.cache_handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("failed to get data cache block", K(ret), K_(curr_index_info));
      } else if (OB_FAIL(cache.prefetch(
                 MTL_ID(), curr_index_info_.get_macro_id(), curr_index_info_,
                 index_scan_param_.get_query_flag().is_use_block_cache(),
                 index_scan_param_.get_tablet_id(),
                 posting_data_handle_.io_handle_, &data_io_allocator_, false /*is_preread*/))) {
        LOG_WARN("failed to read posting data micro block", K(ret), K_(curr_index_info));
      } else {
        posting_data_handle_.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      }
    } else {
      posting_data_handle_.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
    }
    if (OB_SUCC(ret) && OB_FAIL(posting_data_handle_.get_micro_block_data(
                            nullptr, curr_posting_micro_block_, true))) {
      LOG_WARN("failed to get posting data micro block data", K(ret), K_(posting_data_handle));
    } else if (OB_SUCC(ret) && OB_FAIL(open_posting_micro_reader(
                                  posting_data_handle_.macro_block_id_,
                                  posting_data_handle_.micro_info_.offset_,
                                  posting_data_handle_.micro_info_.size_))) {
      LOG_WARN("failed to open current posting micro block", K(ret), K_(curr_index_info));
    }
  }
  return ret;
}

int ObInvSSTableRangeIter::open_posting_micro_reader(
    const MacroBlockId &macro_id,
    const int64_t block_offset,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_posting_micro_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid posting data micro block", K(ret), K_(curr_posting_micro_block));
  } else if (OB_FAIL(posting_micro_reader_.init_flat_cache(
      curr_posting_micro_block_.get_buf(), curr_posting_micro_block_.get_buf_size()))) {
    LOG_WARN("failed to init posting micro block reader", K(ret), K_(curr_posting_micro_block));
  } else {
    curr_posting_micro_locator_.set(macro_id, block_offset, block_size);
  }
  return ret;
}

int ObInvSSTableRangeIter::load_current_block()
{
  int ret = OB_SUCCESS;
  if (curr_posting_block_loaded_) {
    // already loaded for the current meta
  } else if (OB_ISNULL(sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is null when loading posting block", K(ret));
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current meta not positioned before loading block", K(ret), K_(curr_meta));
  } else if (OB_UNLIKELY(!curr_index_info_.is_data_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-data block index info", K(ret), K_(curr_index_info));
  } else if (OB_FAIL(load_current_micro_block())) {
    LOG_WARN("failed to load current posting micro block", K(ret), K_(curr_index_info));
  } else if (FALSE_IT(posting_block_data_.reset())) {
  } else if (OB_FAIL(posting_micro_reader_.get_posting_block_data(curr_meta_.locator_.block_idx_, posting_block_data_))) {
    LOG_WARN("failed to init posting block data view", K(ret), K_(curr_meta));
  } else if (OB_UNLIKELY(posting_block_data_.get_row_count() != curr_meta_.doc_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("posting block row count does not match meta", K(ret),
             "actual_row_count", posting_block_data_.get_row_count(), K_(curr_meta));
  } else {
    curr_posting_block_loaded_ = true;
  }
  return ret;
}

int ObInvSSTableRangeIter::load_current_value_block()
{
  int ret = OB_SUCCESS;
  const char *block_buf = nullptr;
  int64_t block_size = 0;
  if (curr_value_block_loaded_) {
    // Same logical value block already inited; nothing to do.
  } else if (OB_UNLIKELY(!curr_posting_block_loaded_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("load value block before data block", K(ret));
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current meta not positioned before loading value block", K(ret), K_(curr_meta));
  } else if (OB_UNLIKELY(0 == curr_meta_.locator_.value_micro_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no posting value micro block in current segment", K(ret), K_(curr_meta));
  } else if (OB_ISNULL(curr_index_info_.row_header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing row header for value locator", K(ret), K_(curr_index_info));
  } else {
    if (need_load_value_micro_block()) {
      value_row_header_ = *curr_index_info_.row_header_;
      value_row_header_.block_offset_ =
          static_cast<int32_t>(curr_meta_.locator_.value_micro_offset_);
      value_row_header_.block_size_ =
          static_cast<int32_t>(curr_meta_.locator_.value_micro_size_);
      // Force PHYSICAL_KEY_MODE so the value block's cache key (macro_id, offset,
      // size) does not collide with the paired posting block's logical key.
      value_row_header_.unset_has_logic_micro_id();
      value_row_header_.unset_has_shared_data_macro_id();
      ObMicroIndexInfo value_index_info = curr_index_info_;
      value_index_info.row_header_ = &value_row_header_;
      ObMicroBlockCacheKey cache_key(MTL_ID(), value_index_info);
      ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
      posting_value_data_handle_.reset();
      posting_value_data_handle_.allocator_ = &data_io_allocator_;
      posting_value_data_handle_.tenant_id_ = MTL_ID();
      posting_value_data_handle_.macro_block_id_ = value_index_info.get_macro_id();
      posting_value_data_handle_.micro_info_.set(
          value_index_info.get_block_offset(), value_index_info.get_block_size(),
          value_index_info.get_logic_micro_id(), value_index_info.get_data_checksum());
      if (OB_FAIL(cache.get_cache_block(cache_key, posting_value_data_handle_.cache_handle_))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("failed to get value cache block", K(ret), K(value_index_info));
        } else if (OB_FAIL(cache.prefetch(
            MTL_ID(), value_index_info.get_macro_id(), value_index_info,
            index_scan_param_.get_query_flag().is_use_block_cache(), index_scan_param_.get_tablet_id(),
            posting_value_data_handle_.io_handle_, &data_io_allocator_))) {
          LOG_WARN("failed to read posting value micro block", K(ret), K(value_index_info));
        } else {
          posting_value_data_handle_.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        }
      } else {
        posting_value_data_handle_.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      }
      if (FAILEDx(posting_value_data_handle_.get_micro_block_data(
          nullptr, curr_value_micro_block_, true))) {
        LOG_WARN("failed to get posting value micro block data", K(ret), K(value_index_info));
      } else if (OB_UNLIKELY(!curr_value_micro_block_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid posting value micro block", K(ret), K_(curr_value_micro_block));
      } else if (FALSE_IT(posting_value_micro_reader_.reuse())) {
      } else if (OB_FAIL(posting_value_micro_reader_.init(
          curr_value_micro_block_.get_buf(),
          curr_value_micro_block_.get_buf_size(),
          true /*is_posting_value*/))) {
        LOG_WARN("failed to init posting value micro block reader", K(ret), K_(curr_value_micro_block));
      } else {
        curr_value_micro_locator_.set(
            curr_index_info_.get_macro_id(),
            static_cast<int64_t>(curr_meta_.locator_.value_micro_offset_),
            static_cast<int64_t>(curr_meta_.locator_.value_micro_size_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!curr_value_micro_block_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("posting value micro block not loaded before payload relocation", K(ret), K_(curr_meta));
    } else if (FALSE_IT(posting_value_block_reader_.reuse())) {
    } else if (OB_FAIL(posting_value_micro_reader_.get_payload(
        curr_meta_.locator_.block_idx_, block_buf, block_size))) {
      LOG_WARN("failed to locate posting value block payload", K(ret), K_(curr_meta),
          "value_block_count", posting_value_micro_reader_.get_block_count());
    } else if (OB_UNLIKELY(block_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty posting value block payload", K(ret), K_(curr_meta), K(block_size));
    } else if (OB_FAIL(posting_value_block_reader_.init(block_buf, block_size))) {
      LOG_WARN("failed to init posting value block reader", K(ret), K(block_size), K_(curr_meta));
    } else {
      curr_value_block_loaded_ = true;
    }
  }
  return ret;
}

int ObInvSSTableRangeIter::validate_posting_value_row_state() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("range iter not opened", K(ret));
  } else if (OB_UNLIKELY(enable_doc_id_drain_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("posting value access is disabled for raw doc-id drain route", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(
      !curr_posting_block_loaded_ || curr_row_idx_ < 0 || OB_ISNULL(curr_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row for posting value access", K(ret),
        K_(curr_posting_block_loaded), K_(curr_row_idx));
  } else if (OB_UNLIKELY(!need_posting_value_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("posting value access is disabled for this scan", K(ret));
  }
  return ret;
}

int ObInvSSTableRangeIter::materialize_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  uint64_t doc_id = 0;
  if (OB_UNLIKELY(!curr_posting_block_loaded_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("materialize row before block loaded", K(ret), K(row_idx));
  } else if (OB_UNLIKELY(row_idx < 0 || row_idx >= curr_meta_.doc_count_
      || (curr_posting_block_loaded_
          && posting_block_data_.is_valid()
          && row_idx >= posting_block_data_.get_row_count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx out of range", K(ret), K(row_idx), K_(curr_meta),
        "posting_row_count", posting_block_data_.get_row_count());
  } else if (OB_FAIL(posting_block_data_.get_doc_id(row_idx, doc_id))) {
    LOG_WARN("failed to get doc id", K(ret), K(row_idx));
  } else {
    curr_id_datum_.ptr_ = reinterpret_cast<const char *>(&curr_id_storage_);
    curr_id_datum_.set_uint(doc_id);
    curr_id_ = &curr_id_datum_;
    curr_row_idx_ = row_idx;
  }
  return ret;
}

int ObInvSSTableRangeIter::materialize_term()
{
  int ret = OB_SUCCESS;
  const ObCommonDatumRowkey &endkey = curr_index_info_.endkey_;
  const ObDatumRowkey *endkey_rowkey = nullptr;
  if (endkey.is_compact_rowkey()) {
    if (OB_ISNULL(endkey_rowkey = endkey.get_compact_rowkey())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey", K(ret), K(endkey));
    }
  } else if (endkey.is_discrete_rowkey()) {
    const ObDiscreteDatumRowkey *discrete_rowkey = endkey.get_discrete_rowkey();
    if (OB_ISNULL(discrete_rowkey) || OB_ISNULL(discrete_rowkey->rowkey_vector_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected discrete rowkey or rowkey vector", K(ret), K(discrete_rowkey));
    } else if (OB_FAIL(discrete_rowkey->rowkey_vector_->get_rowkey(discrete_rowkey->row_idx_, materialized_endkey_))) {
      LOG_WARN("failed to materialize discrete endkey", K(ret), K_(curr_index_info));
    } else {
      endkey_rowkey = &materialized_endkey_;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected endkey type", K(ret), K(endkey));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(term_col_cnt_ > endkey_rowkey->get_datum_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("term column count exceeds endkey", K(ret), K_(term_col_cnt),
        K(endkey_rowkey->get_datum_cnt()));
  } else {
    for (int64_t i = 0; i < term_col_cnt_; ++i) {
      term_buf_.storage_datums_[i] = endkey_rowkey->get_datum(i);
    }
  }
  return ret;
}

void ObInvSSTableRangeIter::reset_loaded_block()
{
  posting_block_data_.reset();
  curr_posting_block_loaded_ = false;
  posting_value_block_reader_.reuse();
  curr_value_block_loaded_ = false;
  term_materialized_ = false;
  reset_row_cursor();
}

void ObInvSSTableRangeIter::reset_row_cursor()
{
  curr_id_ = nullptr;
  curr_id_storage_ = 0;
  curr_id_datum_.reset();
  curr_value_datum_.reset();
  curr_row_idx_ = -1;
}

void ObInvSSTableRangeIter::reset_prepared_doc_id_batch()
{
  prepared_doc_ids_ = nullptr;
  prepared_doc_count_ = 0;
  next_doc_offset_ = 0;
  posting_block_data_.reset();
  posting_micro_reader_.reuse();
  curr_posting_micro_block_.reset();
  posting_data_handle_.reset();
  curr_posting_micro_locator_.reset();
}

} // namespace blocksstable
} // namespace oceanbase
