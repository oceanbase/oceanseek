/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/ob_eval_bound.h"
#include "share/diagnosis/ob_profile_name_def.h"
#include "share/vector/ob_fixed_length_base.h"

namespace oceanbase
{
namespace sql
{
int ObIDASSearchOp::collect_bitmap_input_range(
    int64_t child_count, bool &available, uint64_t &lo, uint64_t &hi, int64_t &estimated_count)
{
  int ret = OB_SUCCESS;
  available = false;
  lo = UINT64_MAX;
  hi = 0;
  estimated_count = 0;
  if (child_count < 0 || child_count > children_cnt_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (DAS_SCOPE_BASE == scope_ && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
             && !search_ctx_.is_logical_table() && child_count > 0) {
    available = true;
    for (int64_t i = 0; OB_SUCC(ret) && available && i < child_count; ++i) {
      bool child_available = false;
      uint64_t child_lo = UINT64_MAX;
      uint64_t child_hi = 0;
      int64_t child_estimate = 0;
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(children_[i]->get_bitmap_input_range(
                     child_available, child_lo, child_hi, child_estimate))) {
        LOG_WARN("failed to collect bitmap input range", K(ret), K(i));
      } else if (!child_available) {
        available = false;
      } else if (child_lo <= child_hi) {
        lo = std::min(lo, child_lo);
        hi = std::max(hi, child_hi);
        // Cloned Segment/term leaves may share one scalar RtDef estimate.
        // Do not multiply that estimate by the number of consumers.
        estimated_count = std::max(estimated_count, child_estimate);
      }
    }
  }
  return ret;
}

int UInt64RowIDStore::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != search_ctx_.scopes_[scope_].rowid_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowid store expects uint64 rowid type for scope",
             K(ret), K(scope_), K(search_ctx_.scopes_[scope_].rowid_type_));
  } else if (OB_NOT_NULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_ is not null, store already initialized", KR(ret));
  } else if (OB_ISNULL(data_ = static_cast<uint64_t *>(
                 search_ctx_.allocator_.alloc(capacity * sizeof(uint64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for data", K(ret), K(capacity));
  } else {
    capacity_ = capacity;
  }
  return ret;
}

void UInt64RowIDStore::reset()
{
  if (OB_NOT_NULL(data_)) {
    search_ctx_.allocator_.free(data_);
    data_ = nullptr;
  }
  capacity_ = 0;
  count_ = 0;
}

int UInt64RowIDStore::fill(const int64_t begin_idx, const int64_t end_idx, const common::ObIArray<ObExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (OB_UNLIKELY(begin_idx < 0 || end_idx <= begin_idx || end_idx - begin_idx > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid begin_idx or end_idx", KR(ret), K(begin_idx), K(end_idx), K(capacity_));
  } else {
    ObExpr *expr = rowid_exprs.empty() ? nullptr : rowid_exprs.at(0);
    ObIVector *vec = nullptr;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid expr is null", KR(ret));
    } else if (OB_ISNULL(vec = expr->get_vector(*search_ctx_.eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector is null", KR(ret));
    } else {
      const common::VectorFormat rowid_hdr_fmt = expr->get_format(*search_ctx_.eval_ctx_);
      const char *frame_base = search_ctx_.eval_ctx_->frames_[expr->frame_idx_];
      const uintptr_t frame_base_addr = reinterpret_cast<uintptr_t>(frame_base);
      const uintptr_t vec_addr = reinterpret_cast<uintptr_t>(vec);
      LOG_TRACE("[LT hybrid DAS] UInt64RowIDStore::fill",
                K(begin_idx),
                K(end_idx),
                KP(expr),
                K(rowid_hdr_fmt),
                KP(vec),
                "frame_idx", expr->frame_idx_,
                "vector_header_off", expr->vector_header_off_,
                "datum_off", expr->datum_off_,
                "res_buf_off", expr->res_buf_off_,
                "frame_base", reinterpret_cast<void *>(frame_base_addr),
                "frame_base_mod8", static_cast<int>(frame_base_addr & 7),
                "vec_off_mod8", static_cast<int>(expr->vector_header_off_ & 7),
                "vec_addr_mod8", static_cast<int>(vec_addr & 7));
      if (OB_UNLIKELY(common::VEC_INVALID == rowid_hdr_fmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid DAS] UInt64RowIDStore::fill rowid ObExpr vector header is VEC_INVALID "
                 "(init_vector likely not run on THIS expr)",
                 K(ret),
                 KP(expr),
                 K(begin_idx),
                 K(end_idx));
      } else {
        count_ = 0;
      }
      // Fixed format: Only for Uint64, no need to check the length
      if (OB_FAIL(ret)) {
      } else if (OB_LIKELY(vec->get_format() == common::VEC_FIXED)) {
        const void *payload = vec->get_payload(begin_idx);
        if (OB_ISNULL(payload)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec get_payload returns null", KR(ret), K(begin_idx));
        } else {
          count_ = end_idx - begin_idx;
          MEMCPY(data_, payload, count_ * sizeof(uint64_t));
        }
      } else {
        for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
          data_[i - begin_idx] = vec->get_uint(i);
        }
        if (OB_SUCC(ret)) {
          count_ = end_idx - begin_idx;
        }
      }
    }
  }
  return ret;
}

int UInt64RowIDStore::get_rowid(int64_t idx, ObDASRowID &rowid) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (idx < 0 || idx >= count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", KR(ret), K(idx), K(count_));
  } else {
    rowid.set_uint64(data_[idx]);
  }
  return ret;
}

int UInt64RowIDStore::lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (OB_UNLIKELY(count_ == 0 || start_idx < 0 || start_idx >= count_)) {
    ret = OB_ITER_END;
  } else if (target.is_min()) {
    idx = start_idx;
  } else if (target.is_max()) {
    idx = count_;
  } else {
    uint64_t target_val = target.get_uint64();
    uint64_t* pos = std::lower_bound(data_ + start_idx, data_ + count_, target_val);
    idx = pos - data_;
  }
  // OB_ITER_END indicates no rowid >= target found
  if (OB_SUCC(ret) && idx == count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int UInt64RowIDStore::upper_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (OB_UNLIKELY(count_ == 0 || start_idx < 0 || start_idx >= count_)) {
    ret = OB_ITER_END;
  } else if (target.is_max()) {
    idx = count_;
  } else if (target.is_min()) {
    idx = start_idx;
  } else {
    const uint64_t target_val = target.get_uint64();
    const uint64_t *pos = std::upper_bound(data_ + start_idx, data_ + count_, target_val);
    idx = pos - data_;
  }
  if (OB_SUCC(ret) && idx == count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int UInt64RowIDStore::drain_until(
    int64_t &iter_idx,
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after,
    bool &drained) const
{
  int ret = OB_SUCCESS;
  const int64_t begin_idx = iter_idx;
  int64_t upper_bound_idx = count_;
  if (OB_ISNULL(data_) || OB_ISNULL(out_ids)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr data or out_ids", KR(ret), KP(data_), KP(out_ids));
  } else if (OB_UNLIKELY(begin_idx < 0 || begin_idx > count_ || count < 0 || count > capacity)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drain_until argument", K(ret), K(begin_idx), K(count_), K(count), K(capacity));
  } else if (begin_idx == count_) {
    // no cached row remains in this store
  } else if (OB_FAIL(upper_bound(boundary, begin_idx, upper_bound_idx))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      upper_bound_idx = count_;
    } else {
      LOG_WARN("failed to find rowid upper bound", K(ret), K(boundary), K(begin_idx));
    }
  }

  if (OB_SUCC(ret) && begin_idx < count_) {
    const int64_t available_count = upper_bound_idx - begin_idx;
    const int64_t remaining_capacity = capacity - count;
    const int64_t emit_count = std::min(available_count, remaining_capacity);
    if (emit_count > 0) {
      MEMCPY(out_ids + count, data_ + begin_idx, emit_count * sizeof(uint64_t));
      if (nullptr != out_scores) {
        MEMSET(out_scores + count, 0, emit_count * sizeof(double));
      }
      count += emit_count;
      iter_idx = begin_idx + emit_count - 1;
    }
    if (count == capacity) {
      // Buffer is full. Keep iterator on the last emitted row; next drain starts after it.
    } else if (upper_bound_idx < count_) {
      next_id_after.set_uint64(data_[upper_bound_idx]);
      next_score_after = 0.0;
      iter_idx = upper_bound_idx;
      drained = true;
    } else {
      iter_idx = count_;
    }
  }
  return ret;
}

int CompactRowIDStore::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &allocator = search_ctx_.allocator_;
  const common::ObIArray<ObExpr *> *rowid_exprs = search_ctx_.scopes_[scope_].rowid_exprs_;
  if (OB_ISNULL(rowid_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid exprs is null", K(scope_), KR(ret));
  } else if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_FAIL(store_.init(*rowid_exprs,
                                 capacity,
                                 ObMemAttr(MTL_ID(), "DASRowIDStore"),
                                 INT64_MAX, /* never dump */
                                 false,
                                 0,
                                 ObCompressorType::NONE_COMPRESSOR))) {
    LOG_WARN("failed to init temp row store", K(ret));
  } else if (OB_ISNULL(rowids_ = static_cast<ObCompactRow **>(allocator.alloc(capacity * sizeof(ObCompactRow *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowids", K(ret), K(capacity));
  } else {
    store_.set_allocator(allocator);
    capacity_ = capacity;
    count_ = 0;
    MEMSET(rowids_, 0, capacity * sizeof(ObCompactRow *));
  }
  return ret;
}

int CompactRowIDStore::fill(const int64_t begin_idx, const int64_t end_idx, const common::ObIArray<ObExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_) || OB_ISNULL(search_ctx_.mock_skip_) || OB_ISNULL(search_ctx_.eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("store or search context not initialized", KR(ret), KP(rowids_), KP(search_ctx_.mock_skip_), KP(search_ctx_.eval_ctx_));
  } else if (OB_UNLIKELY(begin_idx < 0 || end_idx <= begin_idx || end_idx - begin_idx > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid begin_idx or end_idx", KR(ret), K(begin_idx), K(end_idx), K(capacity_));
  } else {
    store_.reuse();
    count_ = 0;
    search_ctx_.mock_skip_->unset_all(begin_idx, end_idx);
    const uint16_t batch_size = static_cast<uint16_t>(search_ctx_.eval_ctx_->max_batch_size_);
    const EvalBound bound(batch_size, static_cast<uint16_t>(begin_idx), static_cast<uint16_t>(end_idx), false);
    if (OB_FAIL(store_.add_batch(rowid_exprs, *search_ctx_.eval_ctx_, bound,
                                 *search_ctx_.mock_skip_, count_, rowids_))) {
      LOG_WARN("failed to add batch to row store", KR(ret));
    }
  }
  return ret;
}

void CompactRowIDStore::reset()
{
  store_.reset();
  if (OB_NOT_NULL(rowids_)) {
    search_ctx_.allocator_.free(rowids_);
  }
  rowids_ = nullptr;
  capacity_ = 0;
  count_ = 0;
}

void CompactRowIDStore::reuse()
{
  store_.reuse();
  count_ = 0;
}

int CompactRowIDStore::get_rowid(int64_t idx, ObDASRowID &rowid) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowids_ is null, store not initialized", KR(ret));
  } else if (idx < 0 || idx >= count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(idx), K(count_));
  } else if (OB_ISNULL(rowids_[idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rowid", K(ret), K(idx));
  } else {
    rowid.set_compact_row(rowids_[idx]);
  }
  return ret;
}

int CompactRowIDStore::lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowids_ is null, store not initialized", KR(ret));
  } else if (count_ == 0 || start_idx < 0 || start_idx >= count_) {
    ret = OB_ITER_END;
  } else if (target.is_min()) {
    idx = start_idx;
  } else if (target.is_max()) {
    idx = count_;
  } else {
    int64_t lo = start_idx;
    int64_t hi = count_;
    while (OB_SUCC(ret) && lo < hi) {
      int64_t mid = lo + (hi - lo) / 2;
      ObDASRowID mid_rowid;
      mid_rowid.set_compact_row(rowids_[mid]);
      int cmp = 0;
      if (OB_FAIL(search_ctx_.compare_rowid(scope_, mid_rowid, target, cmp))) {
        LOG_WARN("failed to compare rowids", K(ret), K(mid), K(scope_));
      } else if (cmp < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    if (OB_SUCC(ret)) {
      idx = lo;
    }
  }

  // OB_ITER_END indicates no rowid >= target found
  if (OB_SUCC(ret) && idx == count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIDASSearchOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  // Default implementation for op without score
  // for scoring op without block max iteration ability, need to set max score to infinity
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, 0.0);
  } else {
    ObDASRowID tmp_id;
    //
    // uncomment this and add ObIDASSearchOp as ObDASRowID's friend if proven necessary
    //
    // if (search_ctx_.rowid_type_ == DAS_ROWID_TYPE_UINT64) {
    //   tmp_id.set_uint64(target.get_uint64() + 1);
    //   max_score_tuple_.set(tmp_id, tmp_id, 0.0);
    // } else {
      double score = 0.0;
      int cmp_ret = 0;
      if (OB_FAIL(do_advance_to(target, tmp_id, score))) {
        LOG_WARN("failed to advance to", K(ret), K(target));
      } else if (OB_FAIL(compare_rowid(tmp_id, target, cmp_ret))) {
        LOG_WARN("failed to compare rowids", K(ret), K(tmp_id), K(target));
      } else if (0 != cmp_ret) {
        // skip
      } else if (OB_FAIL(do_next_rowid(tmp_id, score))) {
        LOG_WARN("failed to get next rowid", K(ret));
      }

      if (OB_SUCC(ret)) {
        max_score_tuple_.set(tmp_id, tmp_id, score);
      }
    // }
  }
  if (OB_SUCC(ret)) {
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObIDASSearchOp::do_advance_to_id(const ObDASRowID &target, ObDASRowID &curr_id)
{
  UNUSED(target);
  UNUSED(curr_id);
  return OB_NOT_SUPPORTED;
}

int ObIDASSearchOp::do_load_curr_score(double &score)
{
  UNUSED(score);
  return OB_NOT_SUPPORTED;
}

int ObIDASSearchOp::do_next_rowids(int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

// default implementation for operators that can not propagate score to children
int ObIDASSearchOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(threshold < min_competitive_score_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid threshold", K(ret), K(threshold), K_(min_competitive_score));
  } else {
    min_competitive_score_ = threshold;
  }
  return ret;
}

// default implementation for operators that collect max score from all children operators
int ObIDASSearchOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  double curr_threshold = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    double child_max_score = 0.0;
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->calc_max_score(child_max_score))) {
      LOG_WARN("failed to calc max score", K(ret));
    } else {
      curr_threshold += child_max_score;
    }
  }
  if (OB_SUCC(ret)) {
    threshold = curr_threshold;
  }
  return ret;
}

int ObIDASSearchOp::prepare_parallel_work_estimate()
{
  int ret = OB_SUCCESS;
  if (DAS_SEARCH_OP_BITMAP == op_type_ || is_probe_mode()) {
    // Bitmap PREPARE 和 probe-mode 子树不参与可按 Range 切分的 ROR 准入估算。
  } else if (is_parallel_ror_op()
             && OB_FAIL(do_prepare_parallel_work_estimate())) {
    LOG_TRACE("failed to prepare search op parallel work estimate",
              K(ret), K(op_type_), K(op_id_));
  } else if (DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == op_type_) {
    if (OB_UNLIKELY(children_cnt_ <= 0) || OB_ISNULL(children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_TRACE("async segment merge has no representative child for work estimate",
                K(ret), K(op_type_), K(op_id_), K(children_cnt_));
    } else if (OB_FAIL(children_[0]->prepare_parallel_work_estimate())) {
      LOG_TRACE("failed to prepare representative async segment work estimate",
                K(ret), K(op_type_), K(op_id_));
    } else {
      // 代表性的逻辑查询树估算已经准备完成。
    }
  } else if (!is_parallel_ror_op()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_TRACE("search op child is null when preparing parallel work estimate",
                  K(ret), K(op_type_), K(op_id_), K(i), K(children_cnt_));
      } else if (OB_FAIL(children_[i]->prepare_parallel_work_estimate())) {
        LOG_TRACE("failed to prepare child search op parallel work estimate",
                  K(ret), K(op_type_), K(op_id_), K(i));
      } else {
        // 子算子的估算已经准备完成。
      }
    }
  } else {
    // 当前 ROR 叶子算子的估算已经准备完成。
  }
  return ret;
}

int ObIDASSearchOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_NOT_NULL(children_[i])) {
      if (OB_FAIL(children_[i]->rescan_segment(range))) {
        LOG_WARN("failed to propagate rescan_segment to child", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObIDASSearchOp::do_drain_until(
  const ObDASRowID &boundary,
  int64_t capacity,
  uint64_t *out_ids,
  double *out_scores,
  int64_t &count,
  ObDASRowID &next_id_after,
  double &next_score_after)
{
  int ret = OB_SUCCESS;
  bool drained = false;
  count = 0;
  while (OB_SUCC(ret) && count < capacity && !drained) {
    ObDASRowID next_id;
    double next_score = 0.0;
    int cmp_ret = 0;
    if (OB_FAIL(do_next_rowid(next_id, next_score))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        next_id_after.set_max();
        drained = true;
      } else {
        LOG_WARN("failed to next rowid in drain_until", K(ret));
      }
    } else if (OB_FAIL(compare_rowid(next_id, boundary, cmp_ret))) {
      LOG_WARN("failed to compare rowid in drain_until", K(ret));
    } else if (cmp_ret > 0) {
      next_id_after = next_id;
      next_score_after = next_score;
      drained = true;
    } else {
      out_ids[count] = next_id.get_uint64();
      if (nullptr != out_scores) {
        out_scores[count] = next_score;
      }
      ++count;
    }
  }
  return ret;
}

int ObIDASSearchOp::drain_until_active(
    const ObDASRowID &boundary,
    DocScoreAccumulator &accumulator)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_UNLIKELY(!accumulator.is_valid() || boundary.is_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid active drain arguments", K(ret), K(accumulator), K(boundary));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("active drain only supports uint64 rowid type", K(ret), K(get_rowid_type()));
  } else if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_drain_until_active(boundary, accumulator, count))) {
      LOG_WARN("failed to drain active candidates", KR(ret));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    if (OB_FAIL(do_drain_until_active(boundary, accumulator, count))) {
      LOG_WARN("failed to drain active candidates", KR(ret));
    } else {
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(count < 0 || count > accumulator.get_capacity())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected active drain output count", K(ret), K(count), K(accumulator));
  }
  return ret;
}

int ObIDASSearchOp::do_drain_until_active(
    const ObDASRowID &boundary,
    DocScoreAccumulator &accumulator,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  bool drained = false;
  count = 0;
  const uint64_t active_start_id = accumulator.get_start_id();
  const int64_t active_capacity = accumulator.get_capacity();
  const uint64_t *active_words = accumulator.get_bitset_words();
  ObDASRowID curr_id;
  ObDASRowID start_id;
  double curr_score = 0.0;
  start_id.set_uint64(active_start_id);
  // The generic fallback positions itself from the accumulator window. Async token
  // overrides this method so its first row can stay on the batch scoring path.
  if (OB_FAIL(do_advance_to(start_id, curr_id, curr_score))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      drained = true;
    } else {
      LOG_WARN("failed to position active drain", K(ret), K(start_id));
    }
  }
  while (OB_SUCC(ret) && !drained) {
    int cmp_ret = 0;
    if (OB_FAIL(compare_rowid(curr_id, boundary, cmp_ret))) {
      LOG_WARN("failed to compare rowid in active drain", K(ret));
    } else if (cmp_ret > 0) {
      drained = true;
    } else {
      const uint64_t doc_id = curr_id.get_uint64();
      if (doc_id >= active_start_id
          && doc_id - active_start_id < static_cast<uint64_t>(active_capacity)) {
        const uint64_t offset = doc_id - active_start_id;
        if (0 != (active_words[offset >> 6] & (1ULL << (offset & 63)))) {
          if (OB_FAIL(accumulator.add_existing_dim(doc_id, curr_score))) {
            LOG_WARN("failed to add active score", K(ret), K(doc_id));
          } else if (OB_LIKELY(nullptr == profile_) || is_new_profile_output_row(curr_id)) {
            ++count;
          }
        }
      }
    }

    if (OB_SUCC(ret) && !drained && OB_FAIL(do_next_rowid(curr_id, curr_score))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        drained = true;
      } else {
        LOG_WARN("failed to next rowid in active drain", K(ret));
      }
    }
  }
  return ret;
}

int ObIDASSearchOp::get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(search_ctx_.get_related_tablet_id(scalar_ctdef, tablet_id))) {
    LOG_WARN("failed to get tablet id");
  }
  return ret;
}

void ObIDASSearchOp::switch_tablet_id(const share::ObLSID &new_ls_id, const common::ObTabletID &new_tablet_id, storage::ObTableScanParam &scan_param)
{
  const common::ObTabletID &old_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ =
    scan_param.need_switch_param_ || ((old_tablet_id.is_valid() && old_tablet_id != new_tablet_id));
  scan_param.ls_id_ = new_ls_id;
  scan_param.tablet_id_ = new_tablet_id;
}

int ObIDASSearchOp::init_children_ops_array(const ObIArray<ObIDASSearchOp *> &children)
{
  int ret = OB_SUCCESS;
  children_cnt_ = children.count();
  if (children_cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative children count", KR(ret), K(children_cnt_));
  } else if (children_cnt_ == 0) {
    // do nothing
  } else if (OB_ISNULL(children_ = OB_NEW_ARRAY(ObIDASSearchOp *, &ctx_allocator(), children_cnt_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for children ops array", KR(ret), K(children_cnt_));
  } else {
    for (int64_t i = 0; i < children_cnt_; ++i) {
      children_[i] = children.at(i);
    }
  }
  return ret;
}

common::ObProfileId ObIDASSearchOp::get_profile_id() const
{
  common::ObProfileId profile_id = common::ObProfileId::PHY_INVALID;
  switch (op_type_) {
    case DAS_SEARCH_OP_CONJUNCTION:
      profile_id = common::ObProfileId::HYBRID_SEARCH_CONJUNCTION;
      break;
    case DAS_SEARCH_OP_DISJUNCTION:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTION;
      break;
    case DAS_SEARCH_OP_DISJUNCTION_FILTER:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTION_FILTER;
      break;
    case DAS_SEARCH_OP_REQ_OPT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_REQ_OPT;
      break;
    case DAS_SEARCH_OP_REQ_EXCL:
      profile_id = common::ObProfileId::HYBRID_SEARCH_REQ_EXCL;
      break;
    case DAS_SEARCH_OP_SCALAR_INDEX_ROR:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_INDEX_ROR;
      break;
    case DAS_SEARCH_OP_SCALAR_PRIMARY_ROR:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_PRIMARY_ROR;
      break;
    case DAS_SEARCH_OP_SCALAR_SCAN:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_SCAN;
      break;
    case DAS_SEARCH_OP_BITMAP:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BITMAP;
      break;
    case DAS_SEARCH_OP_SORT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SORT;
      break;
    case DAS_SEARCH_OP_TOPK_COLLECT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_TOPK_COLLECT;
      break;
    case DAS_SEARCH_OP_DISJUNCTIVE_MAX:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTIVE_MAX;
      break;
    case DAS_SEARCH_OP_TOKEN:
      profile_id = common::ObProfileId::HYBRID_SEARCH_TOKEN;
      break;
    case DAS_SEARCH_OP_MATCH_PHRASE:
      profile_id = common::ObProfileId::HYBRID_SEARCH_MATCH_PHRASE;
      break;
    case DAS_SEARCH_OP_DUMMY:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DUMMY;
      break;
    case DAS_SEARCH_OP_BMW:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BMW;
      break;
    case DAS_SEARCH_OP_BMM:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BMM;
      break;
    case DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE:
      profile_id = common::ObProfileId::HYBRID_SEARCH_ASYNC_SEGMENT_MERGE;
      break;
    case DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE:
      profile_id = common::ObProfileId::HYBRID_SEARCH_ASYNC_SEGMENT_GROUP_MERGE;
      break;
    case DAS_SEARCH_OP_ASYNC_TOKEN:
      profile_id = common::ObProfileId::HYBRID_SEARCH_ASYNC_TOKEN;
      break;
    case DAS_SEARCH_OP_ASYNC_SCALAR_ROR:
      profile_id = common::ObProfileId::HYBRID_SEARCH_ASYNC_SCALAR_ROR;
      break;
    case DAS_SEARCH_OP_ASYNC_SCALAR_RANGE:
      profile_id = common::ObProfileId::HYBRID_SEARCH_ASYNC_SCALAR_RANGE;
      break;
    case DAS_SEARCH_OP_NESTED:
      profile_id = common::ObProfileId::HYBRID_SEARCH_NESTED;
      break;
    default:
      profile_id = common::ObProfileId::PHY_INVALID;
      break;
  }
  return profile_id;
}

int ObIDASSearchOp::open_profile()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current = common::get_current_profile();
  if (OB_NOT_NULL(current)) {
    if (OB_FAIL(current->register_child(get_profile_id(), profile_))) {
      LOG_WARN("failed to register child profile", K(ret), "op_type", op_type_);
    } else {
      common::ObProfileSwitcher switcher(profile_);
      SET_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 0);
      SET_METRIC_VAL(common::ObMetricId::HS_TOTAL_TIME, 0);
      SET_METRIC_VAL(common::ObMetricId::HS_OP_ID, op_id_);
      SET_METRIC_VAL(common::ObMetricId::HS_SAME_OP_SCAN_TIMES, 1);
    }
  }
  return ret;
}

void ObIDASSearchOp::close_profile()
{
  profile_ = nullptr;
  reset_profile_output_row();
}

void ObIDASSearchOp::reset_profile()
{
  profile_ = nullptr;
  reset_profile_output_row();
  for (int64_t i = 0; i < children_cnt_; ++i) {
    if (nullptr != children_[i]) {
      children_[i]->reset_profile();
    }
  }
}



} // namespace sql
} // namespace oceanbase
