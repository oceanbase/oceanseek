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
#include "sql/engine/expr/ob_expr_bm25.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

class ObInvSSTablePostingIter::ObRangeMaxScoreVisitor final
{
public:
  ObRangeMaxScoreVisitor(
      const ObInvSSTablePostingIter &posting_iter,
      const uint64_t first_id,
      double &candidate_max_score,
      bool &score_available)
    : posting_iter_(posting_iter),
      first_id_(first_id),
      candidate_max_score_(candidate_max_score),
      score_available_(score_available),
      is_current_block_(true)
  {}

  int operator()(const ObPostingBlockMeta &block_meta, bool &stop)
  {
    int ret = OB_SUCCESS;
    stop = false;
    if (block_meta.max_id_ >= first_id_) {
      if (OB_UNLIKELY(!block_meta.block_max_.has_max_score())) {
        score_available_ = false;
        stop = true;
      } else if (is_current_block_) {
        // 复用定位当前 block 时已经计算出的分数。
        candidate_max_score_ = std::max(
            candidate_max_score_, posting_iter_.curr_block_max_score_);
      } else {
        // 当前区间内每个后续 block 的 frontier 最多计算一次。
        for (int64_t i = 0; i < block_meta.block_max_.pair_count_; ++i) {
          const ObInvIdxBM25ParamPair &pair = block_meta.block_max_.pairs_[i];
          candidate_max_score_ = std::max(candidate_max_score_,
              posting_iter_.calc_bm25_score(pair.token_freq_, pair.doc_length_));
        }
      }
    }
    is_current_block_ = false;
    return ret;
  }

private:
  const ObInvSSTablePostingIter &posting_iter_;
  uint64_t first_id_;
  double &candidate_max_score_;
  bool &score_available_;
  bool is_current_block_;
};

ObInvSSTablePostingIter::ObInvSSTablePostingIter()
  : sstable_(nullptr),
    allocator_(nullptr),
    doc_freq_(0),
    total_doc_cnt_(0),
    avg_doc_length_(0.0),
    bm25_calc_params_(),
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
  posting_micro_reader_.reuse();
  curr_data_micro_block_.reset();
  scanner_.reset();
  need_load_micro_block_ = true;
  need_load_value_micro_block_ = true;
  curr_meta_.reset();
  reset_row_cursor();
  sstable_ = nullptr;
  allocator_ = nullptr;
  doc_freq_ = 0;
  total_doc_cnt_ = 0;
  avg_doc_length_ = 0.0;
  bm25_calc_params_.reset();
  has_relevance_params_ = false;
  curr_block_max_score_ = 0.0;
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
    has_relevance_params_ = sstable.get_inv_meta().has_relevance_params_;
    need_posting_value_ = param.need_posting_value_;
    min_doc_id_ = param.min_doc_id_;
    max_doc_id_ = param.max_doc_id_;
    // Precompute scoring constants (block-max path uses score_multiplier = 1.0) before
    // is_opened_ / the first load_current_block_meta(), which needs them via calc_bm25_score().
    // Search indexes have no BM25 relevance params; skip precomputation to avoid the
    // avg_doc_length_ > 0 requirement in init_precomputed_params().
    if (has_relevance_params_) {
      if (OB_FAIL(sql::ObExprBM25::init_precomputed_params(
          sql::ObExprBM25::query_token_weight(doc_freq_, total_doc_cnt_),
          avg_doc_length_, 1.0 /*score_multiplier*/, bm25_calc_params_))) {
        LOG_WARN("failed to init bm25 precomputed params", K(ret), K_(doc_freq), K_(total_doc_cnt), K_(avg_doc_length));
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
    const uint64_t boundary,
    const int64_t max_count,
    const uint64_t *&doc_ids,
    const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths,
    int64_t &count)
{
  return drain_until_impl(
      boundary, max_count, doc_ids, token_freqs, doc_lengths, count, false);
}

int ObInvSSTablePostingIter::drain_until_from_current(
    const uint64_t boundary,
    const int64_t max_count,
    const uint64_t *&doc_ids,
    const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths,
    int64_t &count)
{
  return drain_until_impl(
      boundary, max_count, doc_ids, token_freqs, doc_lengths, count, true);
}

int ObInvSSTablePostingIter::drain_until_impl(
    const uint64_t boundary,
    const int64_t max_count,
    const uint64_t *&doc_ids,
    const uint64_t *&token_freqs,
    const uint64_t *&doc_lengths,
    int64_t &count,
    const bool include_current)
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  token_freqs = nullptr;
  doc_lengths = nullptr;
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
  } else if (posting_block_data_.get_doc_ids()[start_idx] > max_doc_id_) {
    is_end_ = true;
    reset_row_cursor();
  } else {
    const uint64_t *ids = posting_block_data_.get_doc_ids();
    // 计算当前 posting block 从起始位置到末尾的剩余行数。
    const int64_t remaining = curr_meta_.doc_count_ - start_idx;
    // 按调用方容量截取本次 drain 最多可返回的 capped slice。
    const int64_t capped_count = MIN(remaining, max_count);
    int64_t boundary_end_idx = start_idx;
    if (ids[start_idx] > range_boundary) {
      // capped slice 首行已越过 boundary，本次不返回任何行。
    } else if (ids[start_idx + capped_count - 1] <= range_boundary) {
      // capped slice 末行仍在 boundary 内，直接返回整个 slice。
      boundary_end_idx = start_idx + capped_count;
    } else {
      // boundary 切入 capped slice，仅在该 slice 内执行标准 upper-bound。
      const uint64_t *upper = std::upper_bound(
          ids + start_idx, ids + start_idx + capped_count, range_boundary);
      boundary_end_idx = static_cast<int64_t>(upper - ids);
    }
    const int64_t available = boundary_end_idx - start_idx;
    if (available <= 0) {
      // 仅定位 boundary lookahead 行，不读取该行的相关性列。
      if (OB_FAIL(materialize_doc_id(start_idx))) {
        LOG_WARN("failed to position boundary lookahead row", K(ret), K(start_idx));
      }
    } else {
      const int64_t take = available;
      // Project direct pointers into the decoded block; no copy.
      doc_ids = ids + start_idx;
      if (posting_block_data_.has_relevance_params()) {
        token_freqs = posting_block_data_.get_token_freqs() + start_idx;
        doc_lengths = posting_block_data_.get_doc_lengths() + start_idx;
      }
      count = take;
      // 仅把 cursor 定位到本批最后一行，批量投影已经直接提供相关性列，无需重复读取。
      if (OB_FAIL(materialize_doc_id(start_idx + take - 1))) {
        LOG_WARN("failed to position last projected row", K(ret), K(start_idx), K(take));
      }
    }
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
  } else if (FALSE_IT(target_doc_id = MAX(target_doc_id, min_doc_id_))) {
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
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(decode_target_doc_id(target, target_doc_id))) {
    LOG_WARN("failed to decode target doc id", K(ret));
  } else if (FALSE_IT(target_doc_id = MAX(target_doc_id, min_doc_id_))) {
  } else if (target_doc_id > max_doc_id_) {
    ret = OB_ITER_END;
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else if (!in_shallow_status_ && nullptr != curr_id_ && target_doc_id <= curr_id_storage_) {
    // The current doc id already satisfies the target.
  } else {
    int64_t target_idx = 0;
    if (OB_FAIL(locate_target(target_doc_id, target_idx))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to locate target", K(ret), K(target_doc_id));
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

double ObInvSSTablePostingIter::calc_bm25_score(
    const uint64_t token_freq, const uint64_t doc_length) const
{
  return sql::ObExprBM25::eval_with_precomputed_params(token_freq, doc_length, bm25_calc_params_);
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
    uint64_t &min_id, uint64_t &max_id, double &max_score) const
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
      max_score = curr_block_max_score_;
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
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(end_id < start_id || (!start_inclusive && end_id == start_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting range", K(ret), K(start_id), K(start_inclusive), K(end_id));
  } else if (OB_UNLIKELY(!has_relevance_params_)) {
    // 不包含 BM25 metadata 的搜索索引由调用方使用保守上界回退。
  } else if (is_end_) {
    max_score = 0.0;
    can_calc = true;
  } else {
    const uint64_t first_id = start_inclusive ? start_id : start_id + 1;
    double candidate_max_score = 0.0;
    bool score_available = true;
    bool range_covered = false;
    // visitor 只累计区间内 block 的 BM25 上界，metadata 有效性、顺序和覆盖由 scanner 保证。
    ObFunction<int(const ObPostingBlockMeta &, bool &)> visitor(
        ObRangeMaxScoreVisitor(*this, first_id, candidate_max_score, score_available));
    if (OB_FAIL(scanner_.visit_prefetched_block_meta(end_id, visitor, range_covered))) {
      if (OB_UNLIKELY(OB_ITER_END == ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected end of prefetched posting block metadata",
            K(ret), K(start_id), K(start_inclusive), K(end_id));
      } else {
        LOG_WARN("failed to visit prefetched posting block meta", K(ret), K(end_id));
      }
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

int ObInvSSTablePostingIter::load_current_block_meta()
{
  int ret = OB_SUCCESS;
  const bool had_valid_meta = curr_meta_.is_valid();
  const uint64_t prev_max_id = curr_meta_.max_id_;
  curr_meta_.reset();
  if (OB_FAIL(scanner_.get_cur_block_meta(curr_meta_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get current block meta", K(ret));
    }
  } else if (OB_UNLIKELY(!curr_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current posting block meta", K(ret), K_(curr_meta));
  } else if (!had_valid_meta || prev_max_id != curr_meta_.max_id_) {
    // block switched
    curr_block_max_score_ = 0.0;
    const ObPostingBlockMaxScore &block_max = curr_meta_.block_max_;
    for (int64_t i = 0; i < block_max.pair_count_; ++i) {
      const ObInvIdxBM25ParamPair &pair = block_max.pairs_[i];
      const double score = calc_bm25_score(pair.token_freq_, pair.doc_length_);
      if (score > curr_block_max_score_) {
        curr_block_max_score_ = score;
      }
    }
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
      && OB_FAIL(posting_micro_reader_.init_flat_cache(
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
    curr_id_storage_ = posting_block_data_.get_doc_ids()[row_idx];
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
