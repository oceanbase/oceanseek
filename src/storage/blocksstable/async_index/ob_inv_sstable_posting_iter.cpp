/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <algorithm>
#include "lib/function/ob_function.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/engine/expr/ob_expr_bm25.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

class ObInvSSTablePostingIter::ObRangeMaxScoreSpanVisitor final
{
public:
  ObRangeMaxScoreSpanVisitor(
      const ObInvSSTablePostingIter &posting_iter,
      const bool reuse_current_score,
      double &candidate_max_score)
    : posting_iter_(posting_iter),
      candidate_max_score_(candidate_max_score),
      first_span_(true),
      reuse_current_score_(reuse_current_score)
  {}

  int operator()(const ObPostingFrontierSpan &span)
  {
    int ret = OB_SUCCESS;
    const ObInvIdxBM25ParamPair *pairs = span.pairs_;
    int64_t pair_count = span.pair_count_;
    if (first_span_ && reuse_current_score_) {
      // Reuse requires a valid cache for an intersecting current block, whose
      // frontier must be the prefix of the first span.
      const int64_t current_pair_count = posting_iter_.curr_meta_.block_max_.pair_count_;
      if (OB_UNLIKELY(pairs != posting_iter_.curr_meta_.block_max_.pairs_
          || pair_count < current_pair_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current block frontier is not the first span prefix", K(ret),
            KP(pairs), KP(posting_iter_.curr_meta_.block_max_.pairs_),
            K(pair_count), K(current_pair_count));
      } else {
        candidate_max_score_ = std::max(
            candidate_max_score_, posting_iter_.curr_block_max_score_);
        pairs += current_pair_count;
        pair_count -= current_pair_count;
      }
    }
    first_span_ = false;
    if (OB_SUCC(ret) && pair_count > 0) {
      candidate_max_score_ = posting_iter_.bm25_frontier_kernels_.max_score(
          pairs, pair_count, posting_iter_.bm25_calc_params_, candidate_max_score_);
    }
    return ret;
  }

private:
  const ObInvSSTablePostingIter &posting_iter_;
  double &candidate_max_score_;
  bool first_span_;
  bool reuse_current_score_;
};

void ObInvPostingTableCache::reset()
{
  table_handle_.reset();
  tablet_handle_.reset();
  range_.reset();
}

const ObTableHandleV2 *ObInvPostingTableCache::find(
    const ObTabletHandle &tablet_handle,
    const ObNewVersionRange &range) const
{
  const ObTableHandleV2 *handle = nullptr;
  if (tablet_handle.is_valid() && tablet_handle_.is_valid() && table_handle_.is_valid()
      && range.is_valid()
      && tablet_handle.get_obj() == tablet_handle_.get_obj()
      && range == range_) {
    handle = &table_handle_;
  }
  return handle;
}

int ObInvPostingTableCache::store(
    const ObTabletHandle &tablet_handle,
    const ObNewVersionRange &range,
    const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!tablet_handle.is_valid() || !table_handle.is_valid()
      || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting table cache binding", K(ret), K(range));
  } else if (OB_FAIL(tablet_handle_.assign(tablet_handle))) {
    LOG_WARN("failed to pin posting cache tablet", K(ret));
  } else {
    table_handle_ = table_handle;
    range_ = range;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

ObInvSSTablePostingIter::ObInvSSTablePostingIter()
  : sstable_(nullptr),
    allocator_(nullptr),
    doc_freq_(0),
    total_doc_cnt_(0),
    avg_doc_length_(0.0),
    bm25_calc_params_(),
    bm25_frontier_kernels_(),
    has_relevance_params_(false),
    tablet_handle_(),
    table_handle_(),
    scanner_(),
    curr_meta_(),
    curr_data_micro_block_(),
    curr_value_micro_block_(),
    need_load_micro_block_(true),
    need_load_value_micro_block_(true),
    posting_micro_reader_(),
    posting_block_data_(),
    legacy_relevance_buf_(nullptr),
    legacy_doc_ids_buf_(nullptr),
    posting_value_micro_reader_(),
    posting_value_block_reader_(),
    curr_id_datum_(),
    curr_value_datum_(),
    curr_id_storage_(0),
    curr_id_(nullptr),
    curr_token_freq_(0),
    curr_doc_length_(0),
    curr_row_idx_(-1),
    curr_relevance_materialized_(false),
    curr_block_max_score_(0.0),
    curr_block_max_score_valid_(false),
    frontier_min_id_(0),
    min_doc_id_(0),
    max_doc_id_(UINT64_MAX),
    curr_block_loaded_(false),
    curr_value_block_loaded_(false),
    need_posting_value_(false),
    is_end_(false),
    is_opened_(false),
    in_shallow_status_(false)
{
}

ObInvSSTablePostingIter::~ObInvSSTablePostingIter()
{
  reset();
}

void ObInvSSTablePostingIter::reset()
{
  // Drop local views that may reference scanner-owned pins before releasing handles.
  posting_value_block_reader_.reuse();
  posting_value_micro_reader_.reuse();
  curr_value_micro_block_.reset();
  posting_block_data_.reset();
  posting_micro_reader_.reset();
  curr_data_micro_block_.reset();
  scanner_.reset();
  need_load_micro_block_ = true;
  need_load_value_micro_block_ = true;
  curr_meta_.reset();
  reset_row_cursor();
  sstable_ = nullptr;
  if (nullptr != legacy_relevance_buf_ && nullptr != allocator_) {
    allocator_->free(legacy_relevance_buf_);
  }
  legacy_relevance_buf_ = nullptr;
  if (nullptr != legacy_doc_ids_buf_ && nullptr != allocator_) {
    allocator_->free(legacy_doc_ids_buf_);
  }
  legacy_doc_ids_buf_ = nullptr;
  allocator_ = nullptr;
  doc_freq_ = 0;
  total_doc_cnt_ = 0;
  avg_doc_length_ = 0.0;
  bm25_calc_params_.reset();
  bm25_frontier_kernels_ = {};
  has_relevance_params_ = false;
  invalidate_current_block_max_score();
  frontier_min_id_ = 0;
  min_doc_id_ = 0;
  max_doc_id_ = UINT64_MAX;
  need_posting_value_ = false;
  // table before tablet: table may hold refs into tablet lifetime
  table_handle_.reset();
  tablet_handle_.reset();
  curr_block_loaded_ = false;
  curr_value_block_loaded_ = false;
  is_end_ = false;
  is_opened_ = false;
  in_shallow_status_ = false;
}

void ObInvSSTablePostingIter::reset_row_cursor()
{
  curr_id_ = nullptr;
  curr_id_storage_ = 0;
  curr_id_datum_.reset();
  curr_token_freq_ = 0;
  curr_doc_length_ = 0;
  curr_row_idx_ = -1;
  curr_relevance_materialized_ = false;
  curr_value_datum_.reset();
}

void ObInvSSTablePostingIter::reset_loaded_block()
{
  invalidate_current_block_max_score();
  // Drop logical posting / value block views only.
  // Leaf micros (data + value) stay cached when need_load_*_micro_block_ remains false
  // (same leaf hosts multiple logical blocks) — value lifecycle aligns with data.
  posting_block_data_.reset();
  posting_value_block_reader_.reuse();
  curr_block_loaded_ = false;
  curr_value_block_loaded_ = false;
  reset_row_cursor();
}

int ObInvSSTablePostingIter::open(
    const ObInvPostingScanParam &param,
    const ObITableReadInfo &rowkey_read_info,
    ObInvSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("posting iter already opened", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting scan param", K(ret), K(param));
  } else if (OB_UNLIKELY(!rowkey_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey read info", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv sstable", K(ret));
  } else if (OB_FAIL(scanner_.open(
      rowkey_read_info,
      *param.search_key_,
      param.tablet_id_,
      param.query_flag_,
      sstable,
      *param.allocator_,
      param.need_posting_value_))) {
    LOG_WARN("failed to open posting scanner", K(ret), K(param));
  } else {
    sstable_ = &sstable;
    allocator_ = param.allocator_;
    doc_freq_ = param.doc_freq_;
    total_doc_cnt_ = param.total_doc_cnt_;
    avg_doc_length_ = param.avg_doc_length_;
    has_relevance_params_ = sstable.get_inv_meta().get_stats().has_relevance_params_;
    need_posting_value_ = param.need_posting_value_;
    min_doc_id_ = param.min_doc_id_;
    max_doc_id_ = param.max_doc_id_;
    // Block-max scoring uses score_multiplier = 1.0; precompute its constants before opening.
    // Search indexes have no BM25 relevance params; skip precomputation to avoid the
    // avg_doc_length_ > 0 requirement in init_precomputed_params().
    if (has_relevance_params_) {
      if (OB_FAIL(sql::ObExprBM25::init_precomputed_params(
          sql::ObExprBM25::query_token_weight(doc_freq_, total_doc_cnt_),
          avg_doc_length_, 1.0 /*score_multiplier*/, bm25_calc_params_))) {
        LOG_WARN("failed to init bm25 precomputed params", K(ret), K_(doc_freq), K_(total_doc_cnt), K_(avg_doc_length));
      } else {
        bm25_frontier_kernels_ = sql::get_bm25_frontier_max_kernels();
      }
    }
    if (OB_SUCC(ret)) {
      is_opened_ = true;
      // Position the first posting block meta. An absent/empty term leaves the iterator valid with
      // is_end() == true and returns OB_SUCCESS for easier caller ownership handling.
      if (OB_FAIL(load_current_block_meta())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_end_ = true;
        } else {
          LOG_WARN("failed to load first block meta", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_end_ && curr_meta_.max_id_ < min_doc_id_) {
        // Keep block-max inspection inside the slice even before the first row is read.
        bool block_changed = false;
        if (OB_FAIL(scanner_.advance_to(min_doc_id_, true /*inclusive*/, block_changed))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_end_ = true;
          } else {
            LOG_WARN("failed to locate first posting block in range", K(ret), K_(min_doc_id));
          }
        } else if (OB_FAIL(load_current_block_meta())) {
          LOG_WARN("failed to load first posting block in range", K(ret), K_(min_doc_id));
        }
      }
      if (OB_SUCC(ret) && !is_end_ && curr_meta_.min_id_ > max_doc_id_) {
        is_end_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvSSTablePostingIter::set_table_handles(
    const ObTabletHandle &tablet_handle,
    const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
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

int ObInvSSTablePostingIter::next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (in_shallow_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next called in shallow status", K(ret));
  } else if (nullptr == curr_id_ && min_doc_id_ > 0) {
    // The first next() must seek the slice start without consuming that row twice.
    uint64_t target_storage = min_doc_id_;
    ObDatum target;
    target.ptr_ = reinterpret_cast<const char *>(&target_storage);
    target.set_uint(target_storage);
    if (OB_FAIL(advance_to(target))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to seek posting range start", K(ret), K_(min_doc_id));
    }
  } else if (!curr_block_loaded_) {
    // Fresh block (e.g. right after open): load and materialize row 0.
    if (OB_FAIL(load_current_block())) {
      LOG_WARN("failed to load current block", K(ret));
    } else if (OB_FAIL(materialize_row(0))) {
      LOG_WARN("failed to materialize first row", K(ret));
    }
  } else if (curr_row_idx_ + 1 < curr_meta_.doc_count_) {
    // Next row within the loaded block.
    if (OB_FAIL(materialize_row(curr_row_idx_ + 1))) {
      LOG_WARN("failed to materialize next row", K(ret), K_(curr_row_idx));
    }
  } else if (OB_FAIL(move_to_next_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to move to next block", K(ret));
    } else {
      is_end_ = true;
      reset_row_cursor();
    }
  } else if (OB_FAIL(load_current_block())) {
    LOG_WARN("failed to load next block", K(ret));
  } else if (OB_FAIL(materialize_row(0))) {
    LOG_WARN("failed to materialize first row of next block", K(ret));
  }
  if (OB_SUCC(ret) && nullptr != curr_id_ && curr_id_storage_ > max_doc_id_) {
    ret = OB_ITER_END;
  }
  if (OB_ITER_END == ret) {
    is_end_ = true;
    reset_row_cursor();
  }
  return ret;
}

int ObInvSSTablePostingIter::position_drain_start(int64_t &start_idx)
{
  int ret = OB_SUCCESS;
  start_idx = 0;
  if (nullptr == curr_id_ && min_doc_id_ > 0) {
    // Position before projection: drain includes the first row at the slice start.
    if (OB_FAIL(locate_target(min_doc_id_, start_idx))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_end_ = true;
        reset_row_cursor();
      } else {
        LOG_WARN("failed to position posting range start for drain", K(ret), K_(min_doc_id));
      }
    }
  } else if (!curr_block_loaded_) {
    // Fresh block (e.g. right after open): start at row 0.
    if (OB_FAIL(load_current_block())) {
      LOG_WARN("failed to load current block in drain", K(ret));
    } else {
      start_idx = 0;
    }
  } else if (curr_row_idx_ + 1 < curr_meta_.doc_count_) {
    // Next candidate within the loaded block.
    start_idx = curr_row_idx_ + 1;
  } else if (OB_FAIL(move_to_next_block())) {
    // Current block exhausted. Normal EOF becomes count == 0 && is_end().
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      is_end_ = true;
      reset_row_cursor();
    } else {
      LOG_WARN("failed to move to next block in drain", K(ret));
    }
  } else if (OB_FAIL(load_current_block())) {
    LOG_WARN("failed to load next block in drain", K(ret));
  } else {
    start_idx = 0;
  }
  return ret;
}

int ObInvSSTablePostingIter::drain_until(
    const uint64_t boundary, const int64_t max_count,
    const uint64_t *&doc_ids, const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths, int64_t &count)
{
  return drain_until_legacy(boundary, max_count, doc_ids, token_freqs, doc_lengths, count, false);
}

int ObInvSSTablePostingIter::drain_until_from_current(
    const uint64_t boundary, const int64_t max_count,
    const uint64_t *&doc_ids, const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths, int64_t &count)
{
  return drain_until_legacy(boundary, max_count, doc_ids, token_freqs, doc_lengths, count, true);
}

int ObInvSSTablePostingIter::drain_until_legacy(
    const uint64_t boundary, const int64_t max_count,
    const uint64_t *&doc_ids, const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths, int64_t &count, const bool include_current)
{
  int ret = OB_SUCCESS;
  ObPostingDocIdView view;
  ObPostingRelevanceView relevance;
  doc_ids = nullptr;
  token_freqs = nullptr;
  doc_lengths = nullptr;
  if (OB_FAIL(drain_until_impl(boundary, max_count, view, relevance, count, include_current))) {
    LOG_WARN("failed to project posting batch", K(ret), K(boundary), K(max_count), K(include_current));
  } else if (OB_FAIL(export_legacy_doc_ids(view, count, doc_ids))) {
    LOG_WARN("failed to export legacy doc IDs", K(ret), K(count));
  } else if (OB_FAIL(export_legacy_relevance(relevance, count, token_freqs, doc_lengths))) {
    LOG_WARN("failed to export legacy relevance", K(ret), K(count));
  }
  if (OB_FAIL(ret)) {
    doc_ids = nullptr;
    token_freqs = nullptr;
    doc_lengths = nullptr;
    count = 0;
  }
  return ret;
}

int ObInvSSTablePostingIter::drain_until_typed(
    const uint64_t boundary, const int64_t max_count,
    ObPostingDocIdView &doc_ids, ObPostingRelevanceView &relevance, int64_t &count)
{
  return drain_until_impl(boundary, max_count, doc_ids, relevance, count, false);
}

int ObInvSSTablePostingIter::drain_until_typed_run(
    const uint64_t boundary, const int64_t max_count,
    ObPostingDocIdView &doc_ids, ObPostingRelevanceView &relevance,
    int64_t &count, bool &finished, const bool include_current)
{
  int ret = OB_SUCCESS;
  finished = false;
  if (OB_FAIL(drain_until_impl(boundary, max_count, doc_ids, relevance, count, include_current))) {
    LOG_WARN("failed to project typed posting run", K(ret), K(boundary), K(max_count), K(include_current));
  } else if (0 == count) {
    finished = true;
  } else {
    const int64_t next_idx = curr_row_idx_ + 1;
    if (next_idx >= curr_meta_.doc_count_) {
      // 后继行需要跨逻辑块定位，暂不探测，保持 finished=false。
      // 调用方消费本批视图后再次调用，由正常读取流程判断是否到达边界或 EOF。
    } else {
      const uint64_t next_id = posting_block_data_.doc_id_at(next_idx);
      if (next_id > MIN(boundary, max_doc_id_)) {
        if (next_id > max_doc_id_) {
          is_end_ = true;
          reset_row_cursor();
          finished = true;
        } else if (OB_FAIL(materialize_doc_id(next_idx))) {
          LOG_WARN("failed to position known in-block lookahead", K(ret), K(next_idx));
        } else {
          // 仅更新当前行位置，不切换逻辑块或微块引用，本批列视图仍然有效。
          finished = true;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    doc_ids.reset();
    relevance.reset();
    count = 0;
    finished = false;
  }
  return ret;
}

int ObInvSSTablePostingIter::export_legacy_doc_ids(
    const ObPostingDocIdView &view, const int64_t count, const uint64_t *&doc_ids)
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  if (count <= 0) {
  } else if (!view.is_uint32()) {
    doc_ids = view.get_uint64();
  } else if (count > DEFAULT_POSTING_BLOCK_CAPACITY || nullptr == allocator_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid legacy doc id projection", K(ret), K(count), KP_(allocator));
  } else {
    if (nullptr == legacy_doc_ids_buf_) {
      legacy_doc_ids_buf_ = static_cast<uint64_t *>(allocator_->alloc(
          DEFAULT_POSTING_BLOCK_CAPACITY * sizeof(uint64_t)));
      if (nullptr == legacy_doc_ids_buf_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate legacy doc id scratch", K(ret),
            "size", DEFAULT_POSTING_BLOCK_CAPACITY * sizeof(uint64_t));
      }
    }
    if (OB_SUCC(ret)) {
      const uint32_t *source = view.get_uint32();
      for (int64_t i = 0; i < count; ++i) { legacy_doc_ids_buf_[i] = source[i]; }
      doc_ids = legacy_doc_ids_buf_;
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::export_legacy_relevance(
    const ObPostingRelevanceView &relevance,
    const int64_t count,
    const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths)
{
  int ret = OB_SUCCESS;
  token_freqs = nullptr;
  doc_lengths = nullptr;
  if (count <= 0 || !relevance.has_relevance_params()) {
  } else if (!relevance.is_uint32()) {
    token_freqs = relevance.get_token_freqs64();
    doc_lengths = relevance.get_doc_lengths64();
  } else if (OB_UNLIKELY(count > DEFAULT_POSTING_BLOCK_CAPACITY || nullptr == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid legacy relevance projection", K(ret), K(count), KP_(allocator));
  } else {
    if (nullptr == legacy_relevance_buf_) {
      legacy_relevance_buf_ = static_cast<uint64_t *>(allocator_->alloc(
          2 * DEFAULT_POSTING_BLOCK_CAPACITY * sizeof(uint64_t)));
      if (nullptr == legacy_relevance_buf_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate legacy relevance scratch", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      uint64_t *frequencies = legacy_relevance_buf_;
      uint64_t *lengths = legacy_relevance_buf_ + DEFAULT_POSTING_BLOCK_CAPACITY;
      const uint32_t *source_frequencies = relevance.get_token_freqs32();
      const uint32_t *source_lengths = relevance.get_doc_lengths32();
      for (int64_t i = 0; i < count; ++i) {
        frequencies[i] = source_frequencies[i];
        lengths[i] = source_lengths[i];
      }
      token_freqs = frequencies;
      doc_lengths = lengths;
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::drain_until_impl(
    const uint64_t boundary,
    const int64_t max_count,
    ObPostingDocIdView &doc_ids,
    ObPostingRelevanceView &relevance,
    int64_t &count,
    const bool include_current)
{
  int ret = OB_SUCCESS;
  doc_ids.reset();
  relevance.reset();
  count = 0;
  int64_t start_idx = include_current ? curr_row_idx_ : 0;
  const uint64_t range_boundary = MIN(boundary, max_doc_id_);
  if (OB_UNLIKELY(max_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drain_until max_count", K(ret), K(max_count));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drain_until called in shallow status", K(ret));
  } else if (is_end_) {
    // Exhausted: count == 0 && is_end() signals EOF. Not an error.
  } else if (include_current && OB_UNLIKELY(!curr_block_loaded_ || curr_row_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is not positioned for drain", K(ret),
        K_(curr_block_loaded), K_(curr_row_idx));
  } else if (!include_current && OB_FAIL(position_drain_start(start_idx))) {
    LOG_WARN("failed to position drain start", K(ret), K(boundary));
  } else if (is_end_) {
    // move_to_next_block hit EOF while positioning: count == 0 && is_end().
  } else if (posting_block_data_.doc_id_at(start_idx) > max_doc_id_) {
    is_end_ = true;
    reset_row_cursor();
  } else {
    // 计算当前 posting block 从起始位置到末尾的剩余行数。
    const int64_t remaining = curr_meta_.doc_count_ - start_idx;
    // 按调用方容量截取本次 drain 最多可返回的 capped slice。
    const int64_t capped_count = MIN(remaining, max_count);
    int64_t boundary_end_idx = start_idx;
    if (posting_block_data_.doc_id_at(start_idx) > range_boundary) {
      // capped slice 首行已越过 boundary，本次不返回任何行。
    } else if (posting_block_data_.doc_id_at(start_idx + capped_count - 1) <= range_boundary) {
      // capped slice 末行仍在 boundary 内，直接返回整个 slice。
      boundary_end_idx = start_idx + capped_count;
    } else if (OB_FAIL(posting_block_data_.upper_bound(
        range_boundary, start_idx, start_idx + capped_count, boundary_end_idx))) {
      LOG_WARN("failed to find posting boundary", K(ret), K(start_idx), K(capped_count));
    }
    const int64_t available = boundary_end_idx - start_idx;
    if (OB_FAIL(ret)) {
    } else if (available <= 0) {
      // 仅定位 boundary lookahead 行，不读取该行的相关性列。
      if (OB_FAIL(materialize_doc_id(start_idx))) {
        LOG_WARN("failed to position boundary lookahead row", K(ret), K(start_idx));
      }
    } else {
      const int64_t take = available;
      // Project direct pointers into the decoded block; no copy.
      if (OB_FAIL(posting_block_data_.get_doc_id_view(start_idx, doc_ids))) {
        LOG_WARN("failed to project typed doc IDs", K(ret), K(start_idx), K(take));
      } else if (OB_FAIL(posting_block_data_.get_relevance_view(start_idx, relevance))) {
        LOG_WARN("failed to project typed relevance", K(ret), K(start_idx), K(take));
      } else {
        count = take;
      }
      // 仅把 cursor 定位到本批最后一行，批量投影已经直接提供相关性列，无需重复读取。
      if (FAILEDx(materialize_doc_id(start_idx + take - 1))) {
        LOG_WARN("failed to position last projected row", K(ret), K(start_idx), K(take));
      }
    }
  }
  if (OB_FAIL(ret)) {
    doc_ids.reset();
    relevance.reset();
    count = 0;
  }
  return ret;
}

int ObInvSSTablePostingIter::locate_target(
    const uint64_t target_doc_id,
    int64_t &target_idx)
{
  int ret = OB_SUCCESS;
  bool located_in_curr = false;
  target_idx = 0;

  // current block still contains the target.
  if (curr_meta_.is_valid() && curr_meta_.max_id_ >= target_doc_id) {
    if (OB_FAIL(load_current_block())) {
      LOG_WARN("failed to load current block for advance", K(ret));
    } else {
      int64_t search_idx = curr_row_idx_ < 0 ? 0 : curr_row_idx_ + 1;
      if (OB_UNLIKELY(search_idx >= curr_meta_.doc_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target beyond block range despite covering meta", K(ret),
            K(target_doc_id), K_(curr_meta));
      } else if (OB_FAIL(posting_block_data_.lower_bound(
          target_doc_id, search_idx, curr_meta_.doc_count_, target_idx))) {
        if (OB_BEYOND_THE_RANGE == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("target beyond block range despite covering meta", K(ret),
              K(target_doc_id), K_(curr_meta));
        } else {
          LOG_WARN("failed to lower bound target in current block", K(ret), K(target_doc_id));
        }
      } else {
        located_in_curr = true;
      }
    }
  }
  if (OB_SUCC(ret) && !located_in_curr) {
    // Skip blocks via the scanner, then lower_bound inside the located block.
    bool block_changed = false;
    reset_loaded_block();
    if (OB_FAIL(scanner_.advance_to(target_doc_id, true /*inclusive*/, block_changed))) {
      // A physical-row change may already have released the old pin.
      if (block_changed) {
        curr_meta_.reset();
      }
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance scanner", K(ret), K(target_doc_id));
      }
    } else if (FALSE_IT(need_load_micro_block_ = need_load_micro_block_ || block_changed)) {
    } else if (FALSE_IT(need_load_value_micro_block_ =
        need_load_value_micro_block_ || block_changed)) {
    } else if (OB_FAIL(load_current_block_meta())) {
      LOG_WARN("failed to load block meta after advance", K(ret), K(target_doc_id));
    } else if (OB_FAIL(load_current_block())) {
      LOG_WARN("failed to load block after advance", K(ret), K(target_doc_id));
    } else if (OB_FAIL(posting_block_data_.lower_bound(
        target_doc_id, 0, curr_meta_.doc_count_, target_idx))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target beyond block range after scanner advance", K(ret),
            K(target_doc_id), K_(curr_meta));
      } else {
        LOG_WARN("failed to lower bound target after advance", K(ret), K(target_doc_id));
      }
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::advance_to(const ObDatum &target)
{
  int ret = OB_SUCCESS;
  uint64_t target_doc_id = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(decode_target_doc_id(target, target_doc_id))) {
    LOG_WARN("failed to decode target doc id", K(ret));
    // Never seek below the monotone scan frontier: after a shallow reposition the
    // row cursor is reset and locate_target() searches from the block start, so a
    // stale target smaller than the already emitted doc id would rewind the
    // iterator and break stateful consumers (e.g. ReqExcl must_not filtering).
  } else if (FALSE_IT(target_doc_id = MAX(target_doc_id, MAX(min_doc_id_, frontier_min_id_)))) {
  } else if (target_doc_id > max_doc_id_) {
    ret = OB_ITER_END;
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (!in_shallow_status_ && nullptr != curr_id_ && target_doc_id <= curr_id_storage_) {
    if (OB_FAIL(materialize_curr_relevance())) {
      LOG_WARN("failed to materialize current relevance params", K(ret));
    }
  } else {
    int64_t target_idx = 0;
    if (OB_FAIL(locate_target(target_doc_id, target_idx))) {
      // The failure has been logged by the target locator.
    } else if (OB_FAIL(materialize_row(target_idx))) {
      LOG_WARN("failed to materialize advanced row", K(ret), K(target_idx));
    }
  }
  if (OB_SUCC(ret) && nullptr != curr_id_ && curr_id_storage_ > max_doc_id_) {
    ret = OB_ITER_END;
  }
  if (OB_ITER_END == ret) {
    is_end_ = true;
    reset_row_cursor();
  }
  return ret;
}

int ObInvSSTablePostingIter::advance_to_id(const ObDatum &target)
{
  int ret = OB_SUCCESS;
  uint64_t target_doc_id = 0;
  uint64_t curr_doc_id = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(decode_target_doc_id(target, target_doc_id))) {
    LOG_WARN("failed to decode target doc id", K(ret));
  } else if (OB_FAIL(advance_to_id(target_doc_id, curr_doc_id))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to decoded doc id", K(ret), K(target_doc_id));
  }
  return ret;
}

int ObInvSSTablePostingIter::advance_to_id(
    const uint64_t target_doc_id,
    uint64_t &curr_doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(advance_to_id_impl(target_doc_id))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to doc id", K(ret), K(target_doc_id));
  } else {
    curr_doc_id = curr_id_storage_;
  }
  return ret;
}

int ObInvSSTablePostingIter::advance_to_id_impl(const uint64_t target_doc_id)
{
  int ret = OB_SUCCESS;
  const uint64_t seek_doc_id = MAX(target_doc_id, MAX(min_doc_id_, frontier_min_id_));
  if (seek_doc_id > max_doc_id_) {
    ret = OB_ITER_END;
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (!in_shallow_status_ && nullptr != curr_id_ && seek_doc_id <= curr_id_storage_) {
    // The current doc id already satisfies the target.
  } else {
    int64_t target_idx = 0;
    if (OB_FAIL(locate_target(seek_doc_id, target_idx))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to locate target", K(ret), K(seek_doc_id));
    } else if (OB_FAIL(materialize_doc_id(target_idx))) {
      LOG_WARN("failed to materialize advanced doc id", K(ret), K(target_idx));
    }
  }
  if (OB_SUCC(ret) && nullptr != curr_id_ && curr_id_storage_ > max_doc_id_) {
    ret = OB_ITER_END;
  }
  if (OB_ITER_END == ret) {
    is_end_ = true;
    reset_row_cursor();
  }
  return ret;
}

int ObInvSSTablePostingIter::probe_local_block_group(
    const uint64_t *ids, const int64_t count,
    uint64_t *token_freqs, uint64_t *doc_lengths,
    uint64_t &matches, int64_t &consumed)
{
  int ret = OB_SUCCESS;
  matches = 0;
  consumed = 0;
  if (count >= 8 && curr_block_loaded_ && !in_shallow_status_
      && curr_row_idx_ >= 0 && curr_row_idx_ < curr_meta_.doc_count_
      && posting_block_data_.has_relevance_params()
      && curr_meta_.max_id_ <= max_doc_id_
      && ids[0] > curr_id_storage_ && ids[7] <= curr_meta_.max_id_) {
    const int64_t end = std::upper_bound(ids + 8, ids + count, curr_meta_.max_id_) - ids;
    ObPostingRelevanceView relevance;
    if (OB_FAIL(posting_block_data_.get_relevance_view(0, relevance))) {
      LOG_WARN("failed to project local probe relevance", K(ret));
    }
    int64_t row_idx = curr_row_idx_;
    uint64_t head = curr_id_storage_;
    bool final_matched = false;
    for (int64_t c = 0; OB_SUCC(ret) && c < end; ++c) {
      if (ids[c] > head) {
        if (OB_FAIL(posting_block_data_.lower_bound(
            ids[c], row_idx + 1, curr_meta_.doc_count_, row_idx))) {
          LOG_WARN("failed to locate local sorted probe id", K(ret), K(c));
        } else {
          head = posting_block_data_.doc_id_at(row_idx);
          final_matched = false;
        }
      }
      if (OB_SUCC(ret) && head == ids[c]) {
        if (relevance.is_uint32()) {
          token_freqs[c] = relevance.get_token_freqs32()[row_idx];
          doc_lengths[c] = relevance.get_doc_lengths32()[row_idx];
        } else {
          token_freqs[c] = relevance.get_token_freqs64()[row_idx];
          doc_lengths[c] = relevance.get_doc_lengths64()[row_idx];
        }
        matches |= uint64_t(1) << c;
        final_matched = true;
      } else if (OB_SUCC(ret) && c + 1 < end && ids[c + 1] < head) {
        c = head > ids[end - 1] ? end - 1
            : std::lower_bound(ids + c + 2, ids + end, head) - ids - 1;
      }
    }
    // Commit the final head and lazy relevance state after copying all matches.
    if (OB_SUCC(ret)) {
      if (OB_FAIL(materialize_doc_id(row_idx))) {
        LOG_WARN("failed to commit local sorted probe head", K(ret), K(row_idx));
      } else if (final_matched && OB_FAIL(materialize_curr_relevance())) {
        LOG_WARN("failed to commit local sorted probe relevance", K(ret));
      } else {
        consumed = end;
      }
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::probe_sorted_ids(
    const uint64_t *ids, const int64_t count,
    uint64_t *token_freqs, uint64_t *doc_lengths,
    uint64_t &matches, uint64_t &last_id)
{
  int ret = OB_SUCCESS;
  matches = 0;
  if (OB_UNLIKELY(!can_probe_sorted_ids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("posting cursor cannot probe sorted ids", K(ret));
  } else if (OB_ISNULL(ids) || OB_ISNULL(token_freqs) || OB_ISNULL(doc_lengths)
      || OB_UNLIKELY(count <= 0 || count > MAX_SORTED_PROBE_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sorted posting probe arguments", K(ret), K(count));
  } else {
    last_id = curr_id_storage_;
    for (int64_t c = 0; OB_SUCC(ret) && !is_end_ && c < count; ++c) {
      uint64_t group_matches = 0;
      int64_t consumed = 0;
      if (count - c >= 8 && ids[c + 7] <= curr_meta_.max_id_
          && OB_FAIL(probe_local_block_group(ids + c, count - c,
              token_freqs + c, doc_lengths + c, group_matches, consumed))) {
        LOG_WARN("failed to probe local sorted group", K(ret), K(c));
      } else if (consumed > 0) {
        matches |= group_matches << c;
        last_id = curr_id_storage_;
        c += consumed - 1;
      } else {
        if (OB_FAIL(advance_to_id_impl(ids[c]))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to advance to sorted probe id", K(ret), K(c), K(ids[c]));
          }
        } else {
          last_id = curr_id_storage_;
        }
        if (OB_FAIL(ret) || is_end_) {
        } else if (curr_id_storage_ == ids[c]) {
          int64_t token_freq = 0;
          int64_t doc_length = 0;
          if (OB_FAIL(load_curr_bm25_param(token_freq, doc_length))) {
            LOG_WARN("failed to load sorted probe relevance", K(ret), K(c));
          } else {
            token_freqs[c] = static_cast<uint64_t>(token_freq);
            doc_lengths[c] = static_cast<uint64_t>(doc_length);
            matches |= uint64_t(1) << c;
          }
        } else if (c + 1 < count && ids[c + 1] < curr_id_storage_) {
          c = curr_id_storage_ > ids[count - 1]
              ? count - 1
              : std::lower_bound(ids + c + 2, ids + count, curr_id_storage_) - ids - 1;
        }
      }
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::advance_shallow(const ObDatum &target, const bool inclusive)
{
  int ret = OB_SUCCESS;
  uint64_t target_doc_id = 0;
  bool target_inclusive = inclusive;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(decode_target_doc_id(target, target_doc_id))) {
    LOG_WARN("failed to decode target doc id", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    if (target_doc_id < min_doc_id_) {
      target_doc_id = min_doc_id_;
      target_inclusive = true;
    }
    bool block_changed = false;
    const bool covered_by_curr_block = UINT64_MAX != target_doc_id
        && curr_meta_.is_valid()
        && (target_inclusive ? curr_meta_.max_id_ >= target_doc_id
                      : curr_meta_.max_id_ > target_doc_id);
    if (target_doc_id > max_doc_id_
        || (!target_inclusive && target_doc_id == max_doc_id_)) {
      ret = OB_ITER_END;
    } else if (!covered_by_curr_block) {
      if (OB_FAIL(scanner_.advance_to(target_doc_id, target_inclusive, block_changed))) {
        // A physical-row change may already have released the old pin.
        if (block_changed) {
          curr_meta_.reset();
          invalidate_current_block_max_score();
        }
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to shallow advance scanner", K(ret), K(target_doc_id));
        }
      } else if (FALSE_IT(need_load_micro_block_ = need_load_micro_block_ || block_changed)) {
      } else if (FALSE_IT(need_load_value_micro_block_ =
          need_load_value_micro_block_ || block_changed)) {
      } else if (FALSE_IT(reset_loaded_block())) {
      } else if (OB_FAIL(load_current_block_meta())) {
        LOG_WARN("failed to load block meta after shallow advance", K(ret), K(target_doc_id));
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t min_iter_id = target_inclusive ? target_doc_id : target_doc_id + 1;
      if (min_iter_id > frontier_min_id_) {
        frontier_min_id_ = min_iter_id;
      }
      in_shallow_status_ = true;
      reset_row_cursor();
    }
  }
  if (OB_ITER_END == ret) {
    is_end_ = true;
    reset_row_cursor();
  }
  return ret;
}

int ObInvSSTablePostingIter::get_cur_id(const ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  id_datum = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_cur_id called in shallow status", K(ret));
  } else if (OB_ISNULL(curr_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current doc id materialized", K(ret));
  } else {
    id_datum = curr_id_;
  }
  return ret;
}

int ObInvSSTablePostingIter::get_curr_bm25_param(
    int64_t &token_freq, int64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_curr_bm25_param called in shallow status", K(ret));
  } else if (OB_UNLIKELY(!curr_block_loaded_ || curr_row_idx_ < 0 || OB_ISNULL(curr_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row materialized", K(ret), K_(curr_block_loaded), K_(curr_row_idx));
  } else if (OB_UNLIKELY(!posting_block_data_.has_relevance_params())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current posting block has no relevance params", K(ret));
  } else if (OB_UNLIKELY(!curr_relevance_materialized_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current relevance params not materialized", K(ret), K_(curr_row_idx));
  } else {
    token_freq = curr_token_freq_;
    doc_length = curr_doc_length_;
  }
  return ret;
}

int ObInvSSTablePostingIter::load_curr_bm25_param(
    int64_t &token_freq, int64_t &doc_length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("load_curr_bm25_param called in shallow status", K(ret));
  } else if (OB_UNLIKELY(!curr_block_loaded_ || curr_row_idx_ < 0 || OB_ISNULL(curr_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row materialized", K(ret), K_(curr_block_loaded), K_(curr_row_idx));
  } else if (OB_UNLIKELY(!posting_block_data_.has_relevance_params())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current posting block has no relevance params", K(ret));
  } else if (OB_FAIL(materialize_curr_relevance())) {
    LOG_WARN("failed to materialize current relevance params", K(ret), K_(curr_row_idx));
  } else {
    token_freq = curr_token_freq_;
    doc_length = curr_doc_length_;
  }
  return ret;
}

int ObInvSSTablePostingIter::get_curr_phrase_block_max_ranking(double &max_ranking) const
{
  int ret = OB_SUCCESS;
  max_ranking = 0.0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current posting block meta is invalid", K(ret), K_(curr_meta));
  } else if (OB_UNLIKELY(!curr_meta_.block_max_.has_max_score())) {
    ret = OB_NOT_SUPPORTED;
  } else {
    const ObPostingBlockMaxScore &block_max = curr_meta_.block_max_;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_max.pair_count_; ++i) {
      const ObInvIdxBM25ParamPair &pair = block_max.pairs_[i];
      const double ranking = static_cast<double>(pair.token_freq_)
          / sql::ObExprBM25::length_normalizer(pair.doc_length_, avg_doc_length_);
      if (ranking > max_ranking) {
        max_ranking = ranking;
      }
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::get_block_max_tuple(
    uint64_t &min_id, uint64_t &max_id, double &max_score)
{
  int ret = OB_SUCCESS;
  min_id = 0;
  max_id = 0;
  max_score = 0.0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current posting block meta is invalid", K(ret), K_(curr_meta));
  } else {
    min_id = MAX(MAX(frontier_min_id_, curr_meta_.min_id_), min_doc_id_);
    max_id = MIN(curr_meta_.max_id_, max_doc_id_);
    if (min_id > max_id) {
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(!curr_meta_.block_max_.has_max_score())) {
      // No block-max frontier: leave max_score unset and signal the caller to
      // substitute its own conservative bound. min_id/max_id stay populated.
      ret = OB_NOT_SUPPORTED;
    } else {
      max_score = get_current_block_max_score();
    }
  }
  return ret;
}

// 使用主 scanner 已固定的 metadata 计算 BM25 上界，且不推进其游标。
int ObInvSSTablePostingIter::get_range_max_score(
    const uint64_t start_id,
    const bool start_inclusive,
    const uint64_t end_id,
    double &max_score,
    bool &can_calc)
{
  int ret = OB_SUCCESS;
  can_calc = false;
  bool cached_meta_reusable = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(end_id < start_id || (!start_inclusive && end_id == start_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting range", K(ret), K(start_id), K(start_inclusive), K(end_id));
  } else if (OB_UNLIKELY(!has_relevance_params_)) {
  } else if (is_end_) {
    max_score = 0.0;
    can_calc = true;
  } else if (OB_FAIL(scanner_.check_cached_range_meta(curr_meta_,
      start_inclusive ? start_id : start_id + 1, end_id, cached_meta_reusable))) {
    invalidate_current_block_max_score();
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to check cached range metadata", K(ret));
  } else if (cached_meta_reusable && curr_meta_.min_id_ > end_id) {
    max_score = 0.0;
    can_calc = true;
  } else if (cached_meta_reusable && curr_block_max_score_valid_
      && curr_meta_.block_max_.has_max_score() && curr_meta_.max_id_ >= end_id) {
    max_score = std::max(0.0, curr_block_max_score_);
    can_calc = true;
  } else {
    const uint64_t first_id = start_inclusive ? start_id : start_id + 1;
    double candidate_max_score = 0.0;
    bool score_available = true;
    bool range_covered = false;
    const bool reuse_current_score = cached_meta_reusable && curr_block_max_score_valid_
        && curr_meta_.max_id_ >= first_id && curr_meta_.min_id_ <= end_id
        && curr_meta_.block_max_.has_max_score();
    ObFunction<int(const ObPostingFrontierSpan &)> span_visitor(
        ObRangeMaxScoreSpanVisitor(*this, reuse_current_score, candidate_max_score));
    if (OB_FAIL(scanner_.visit_prefetched_frontier_spans(
        first_id, end_id, span_visitor, range_covered, score_available))) {
      LOG_WARN("failed to visit posting frontier spans", K(ret), K(first_id), K(end_id));
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to evaluate posting frontier range", K(ret), K(start_id), K(end_id));
    } else if (score_available && range_covered) {
      max_score = candidate_max_score;
      can_calc = true;
    }
  }
  return ret;
}

// Value lifecycle aligned with data: reuse paired value micro across logical blocks
// when need_load_value_micro_block_ stays false.
int ObInvSSTablePostingIter::validate_posting_value_row_state()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_
      || !curr_block_loaded_ || curr_row_idx_ < 0 || OB_ISNULL(curr_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row for posting value access", K(ret), K_(in_shallow_status),
        K_(curr_block_loaded), K_(curr_row_idx));
  } else if (OB_UNLIKELY(!need_posting_value_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("posting value access is disabled for this scan", K(ret));
  }
  return ret;
}

int ObInvSSTablePostingIter::get_cur_pos_list(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  datum = nullptr;
  if (OB_FAIL(validate_posting_value_row_state())) {
    if (OB_UNLIKELY(OB_ITER_END != ret && OB_NOT_SUPPORTED != ret)) {
      LOG_WARN("failed to validate posting value row state", K(ret));
    }
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

int ObInvSSTablePostingIter::get_cur_nested_key_list(const ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  datum = nullptr;
  if (OB_FAIL(validate_posting_value_row_state())) {
    if (OB_UNLIKELY(OB_ITER_END != ret && OB_NOT_SUPPORTED != ret)) {
      LOG_WARN("failed to validate posting value row state", K(ret));
    }
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

int ObInvSSTablePostingIter::load_current_value_block()
{
  int ret = OB_SUCCESS;
  const char *block_buf = nullptr;
  int64_t block_size = 0;
  if (curr_value_block_loaded_) {
    // Same logical value block already inited; nothing to do.
  } else if (OB_UNLIKELY(!curr_block_loaded_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("load value block before data block", K(ret));
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    // curr_block_loaded_ already implies meta was valid when data block loaded.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current meta not positioned before loading value block", K(ret), K_(curr_meta));
  } else if (need_load_value_micro_block_
      && OB_FAIL(scanner_.get_cur_value_micro_block(curr_value_micro_block_))) {
    LOG_WARN("failed to get current posting value micro block", K(ret));
  } else if (OB_UNLIKELY(!curr_value_micro_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current segment has no posting value micro block", K(ret));
  } else if (need_load_value_micro_block_ && FALSE_IT(posting_value_micro_reader_.reuse())) {
  } else if (need_load_value_micro_block_
      && OB_FAIL(posting_value_micro_reader_.init(
          curr_value_micro_block_.get_buf(),
          curr_value_micro_block_.get_buf_size(),
          true /*is_posting_value*/))) {
    LOG_WARN("failed to init posting value micro block reader", K(ret), K(curr_value_micro_block_));
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
    need_load_value_micro_block_ = false;
    curr_value_block_loaded_ = true;
  }
  return ret;
}

int ObInvSSTablePostingIter::decode_target_doc_id(
    const ObDatum &target, uint64_t &target_doc_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(target.is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("target doc id datum is null", K(ret));
  } else if (OB_UNLIKELY(sizeof(uint64_t) != target.len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("target doc id datum is not a uint64", K(ret), K(target.len_));
  } else {
    target_doc_id = target.get_uint64();
  }
  return ret;
}

void ObInvSSTablePostingIter::assign_current_block_meta(const ObPostingBlockMeta &meta)
{
  // A new pin/image can have the same maximum DocID. Never use only max_id as
  // the identity of a lazily evaluated bound.
  invalidate_current_block_max_score();
  curr_meta_ = meta;
}

double ObInvSSTablePostingIter::get_current_block_max_score()
{
  if (!curr_block_max_score_valid_) {
    const ObPostingBlockMaxScore &block_max = curr_meta_.block_max_;
    curr_block_max_score_ = bm25_frontier_kernels_.max_score(
        block_max.pairs_, block_max.pair_count_, bm25_calc_params_, 0.0);
    curr_block_max_score_valid_ = true;
  }
  return curr_block_max_score_;
}

int ObInvSSTablePostingIter::load_current_block_meta()
{
  int ret = OB_SUCCESS;
  ObPostingBlockMeta meta;
  if (OB_FAIL(scanner_.get_cur_block_meta(meta))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get current block meta", K(ret));
    }
  } else if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current posting block meta", K(ret), K(meta));
  } else {
    assign_current_block_meta(meta);
  }
  if (OB_FAIL(ret)) {
    curr_meta_.reset();
    invalidate_current_block_max_score();
  }
  return ret;
}

int ObInvSSTablePostingIter::load_current_block()
{
  int ret = OB_SUCCESS;
  if (curr_block_loaded_) {
    // already loaded for the current meta
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current meta not positioned before loading block", K(ret), K_(curr_meta));
  } else if (need_load_micro_block_
      && OB_FAIL(scanner_.get_cur_micro_block(curr_data_micro_block_))) {
    LOG_WARN("failed to get current posting data micro block", K(ret));
  } else if (OB_UNLIKELY(!curr_data_micro_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid posting data micro block", K(ret));
  } else if (FALSE_IT(posting_block_data_.reset())) {
  } else if (need_load_micro_block_
      && OB_FAIL(posting_micro_reader_.init(
         curr_data_micro_block_.get_buf(),
         curr_data_micro_block_.get_buf_size()))) {
    LOG_WARN("failed to init posting data micro block reader", K(ret), K(curr_data_micro_block_));
  } else if (FALSE_IT(need_load_micro_block_ = false)) {
  } else if (OB_FAIL(posting_micro_reader_.get_posting_block_data(curr_meta_.locator_.block_idx_, posting_block_data_))) {
    LOG_WARN("failed to init posting block data view", K(ret), K_(curr_meta));
  } else if (OB_UNLIKELY(posting_block_data_.get_row_count() != curr_meta_.doc_count_)) {
    // Keep direct decoded-column ranges consistent with the posting block metadata.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("posting block row count mismatches meta", K(ret),
        K_(posting_block_data), K_(curr_meta));
  } else {
    curr_block_loaded_ = true;
  }
  return ret;
}

int ObInvSSTablePostingIter::materialize_doc_id(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_block_loaded_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("materialize doc id before block loaded", K(ret), K(row_idx));
  } else if (OB_UNLIKELY(row_idx < 0 || row_idx >= curr_meta_.doc_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx out of range", K(ret), K(row_idx), K_(curr_meta));
  } else {
    // 直接从已解码 doc-id 连续数组读取目标行，并建立当前 datum 指针。
    curr_id_storage_ = posting_block_data_.doc_id_at(row_idx);
    curr_id_datum_.ptr_ = reinterpret_cast<const char *>(&curr_id_storage_);
    curr_id_datum_.set_uint(curr_id_storage_);
    curr_id_ = &curr_id_datum_;
    curr_row_idx_ = row_idx;
    curr_token_freq_ = 0;
    curr_doc_length_ = 0;
    curr_relevance_materialized_ = false;
    in_shallow_status_ = false;
    frontier_min_id_ = curr_id_storage_;
  }
  return ret;
}

int ObInvSSTablePostingIter::materialize_curr_relevance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!curr_block_loaded_ || curr_row_idx_ < 0 || OB_ISNULL(curr_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no current row materialized", K(ret), K_(curr_block_loaded), K_(curr_row_idx));
  } else if (curr_relevance_materialized_) {
    // Relevance params have already been decoded for the current row.
  } else if (!posting_block_data_.has_relevance_params()) {
    // Keep curr_relevance_materialized_ false because no params are available.
  } else {
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    if (OB_FAIL(posting_block_data_.get_relevance_params(curr_row_idx_, token_freq, doc_length))) {
      LOG_WARN("failed to get relevance params", K(ret), K_(curr_row_idx));
    } else {
      curr_token_freq_ = static_cast<int64_t>(token_freq);
      curr_doc_length_ = static_cast<int64_t>(doc_length);
      curr_relevance_materialized_ = true;
    }
  }
  return ret;
}

int ObInvSSTablePostingIter::materialize_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(materialize_doc_id(row_idx))) {
    LOG_WARN("failed to materialize doc id", K(ret), K(row_idx));
  } else if (OB_FAIL(materialize_curr_relevance())) {
    LOG_WARN("failed to materialize relevance params", K(ret), K(row_idx));
  }
  return ret;
}

int ObInvSSTablePostingIter::move_to_next_block()
{
  int ret = OB_SUCCESS;
  bool block_changed = false;
  reset_loaded_block();
  if (OB_FAIL(scanner_.next_block(block_changed))) {
    // A physical-row change may already have released the old pin.
    if (block_changed) {
      curr_meta_.reset();
    }
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to next posting block", K(ret));
    }
  } else if (FALSE_IT(need_load_micro_block_ = need_load_micro_block_ || block_changed)) {
  } else if (FALSE_IT(need_load_value_micro_block_ =
      need_load_value_micro_block_ || block_changed)) {
  } else if (OB_FAIL(load_current_block_meta())) {
    LOG_WARN("failed to load next block meta", K(ret));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
