/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include <algorithm>
#include "ob_das_disjunction_filter_op.h"
#include "sql/das/ob_das_def_reg.h"

namespace oceanbase
{
namespace sql
{

int ObDASDisjunctionFilterOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optional_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("optional ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*optional_ops_))) {
    LOG_WARN("failed to assign optional ops", KR(ret));
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASDisjunctionFilterOpParam &disjunction_filter_op_param =
    static_cast<const ObDASDisjunctionFilterOpParam &>(op_param);
  const ObIArray<ObIDASSearchOp *> *optional_ops = disjunction_filter_op_param.get_optional_ops();
  if (OB_ISNULL(optional_ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr optional ops", KR(ret));
  } else if (OB_UNLIKELY(disjunction_filter_op_param.get_minimum_should_match() > optional_ops->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minimum should match must be less than the number of should clauses", KR(ret), 
      K(disjunction_filter_op_param.get_minimum_should_match()), KPC(optional_ops));
  } else {
    lead_cost_ = disjunction_filter_op_param.get_lead_cost();
    min_should_match_ = disjunction_filter_op_param.get_minimum_should_match();
    match_score_ = disjunction_filter_op_param.get_match_score();
  }
  return ret;
}

int ObDASDisjunctionFilterOp::reset_merge_cursor()
{
  int ret = OB_SUCCESS;
  rowid_allocators_[0].reset();
  rowid_allocators_[1].reset();
  rowid_allocator_idx_ = 0;
  cur_row_id_.set_min();
  reset_batch_cursor();
  if (use_batch_merge_) {
    // The uint64 heap keeps its own compact cursor state. Do not populate the
    // generic ObDASRowID array as a second O(children) head cache.
    child_row_ids_.reuse();
    if (OB_FAIL(init_batch_cursor())) {
      LOG_WARN("failed to initialize disjunction batch cursor", K(ret));
    }
  } else if (child_row_ids_.count() == children_cnt_) {
    for (int64_t i = 0; i < child_row_ids_.count(); ++i) {
      child_row_ids_.at(i).set_min();
    }
  } else {
    child_row_ids_.reset();
    if (OB_FAIL(child_row_ids_.prepare_allocate(children_cnt_, cur_row_id_))) {
      LOG_WARN("failed to resize disjunction child row ids", K(ret), K_(children_cnt));
    }
  }
  if (OB_SUCC(ret) && is_async_nested_scope(scope_)
      && OB_FAIL(child_nested_keys_.prepare_allocate(children_cnt_))) {
    LOG_WARN("failed to initialize disjunction child nested keys", K(ret), K_(children_cnt));
  }
  return ret;
}

int64_t ObDASDisjunctionFilterOp::calc_child_batch_size(
    const int64_t child_count)
{
  const int64_t batch_row_budget =
      BATCH_BUFFER_BYTE_BUDGET / static_cast<int64_t>(sizeof(uint64_t));
  // Preserve one head per child at high fan-out, then divide the remaining
  // budget evenly without exceeding the normal per-child batch size.
  return child_count <= 0
      ? 0
      : MIN(CHILD_BATCH_SIZE,
            MAX(1L, batch_row_budget / child_count));
}

void ObDASDisjunctionFilterOp::reset_batch_cursor()
{
  iter_end_ = false;
  batch_cursor_primed_ = false;
  child_batch_size_ = 0;
  batch_cursors_.reuse();
  heap_child_indices_.reuse();
}

int ObDASDisjunctionFilterOp::ensure_batch_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t required_row_capacity = children_cnt_ * child_batch_size_;
  if (batch_row_capacity_ < required_row_capacity || OB_ISNULL(batch_ids_)) {
    batch_allocator_.reset();
    batch_ids_ = nullptr;
    batch_row_capacity_ = 0;
    batch_ids_ = static_cast<uint64_t *>(batch_allocator_.alloc(
        required_row_capacity * static_cast<int64_t>(sizeof(uint64_t))));
    if (OB_ISNULL(batch_ids_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate disjunction batch buffer", K(ret),
          K(required_row_capacity), K_(children_cnt), K_(child_batch_size));
    } else {
      batch_row_capacity_ = required_row_capacity;
    }
  }
  return ret;
}

uint64_t ObDASDisjunctionFilterOp::batch_child_head(
    const int64_t child_idx) const
{
  const BatchCursor &cursor = batch_cursors_.at(child_idx);
  return batch_ids_[child_idx * child_batch_size_ + cursor.position_];
}

bool ObDASDisjunctionFilterOp::heap_less(
    const int64_t left_child_idx,
    const int64_t right_child_idx) const
{
  const uint64_t left_id = batch_child_head(left_child_idx);
  const uint64_t right_id = batch_child_head(right_child_idx);
  return left_id < right_id
      || (left_id == right_id && left_child_idx < right_child_idx);
}

int ObDASDisjunctionFilterOp::heap_push(const int64_t child_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = heap_child_indices_.count();
  if (OB_FAIL(heap_child_indices_.push_back(child_idx))) {
    LOG_WARN("failed to append disjunction heap child", K(ret), K(child_idx));
  }
  while (OB_SUCC(ret) && pos > 0) {
    const int64_t parent_pos = (pos - 1) / 2;
    if (!heap_less(heap_child_indices_.at(pos),
                   heap_child_indices_.at(parent_pos))) {
      break;
    } else {
      const int64_t tmp = heap_child_indices_.at(pos);
      heap_child_indices_.at(pos) = heap_child_indices_.at(parent_pos);
      heap_child_indices_.at(parent_pos) = tmp;
      pos = parent_pos;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::heap_sift_down(const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  int64_t pos = start_pos;
  while (OB_SUCC(ret) && 2 * pos + 1 < heap_child_indices_.count()) {
    int64_t min_pos = 2 * pos + 1;
    const int64_t right_pos = min_pos + 1;
    if (right_pos < heap_child_indices_.count()
        && heap_less(heap_child_indices_.at(right_pos),
                     heap_child_indices_.at(min_pos))) {
      min_pos = right_pos;
    }
    if (!heap_less(heap_child_indices_.at(min_pos),
                   heap_child_indices_.at(pos))) {
      break;
    } else {
      const int64_t tmp = heap_child_indices_.at(pos);
      heap_child_indices_.at(pos) = heap_child_indices_.at(min_pos);
      heap_child_indices_.at(min_pos) = tmp;
      pos = min_pos;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::heap_pop(int64_t &child_idx)
{
  int ret = OB_SUCCESS;
  child_idx = -1;
  if (heap_child_indices_.empty()) {
    ret = OB_ITER_END;
  } else {
    child_idx = heap_child_indices_.at(0);
    heap_child_indices_.at(0) =
        heap_child_indices_.at(heap_child_indices_.count() - 1);
    heap_child_indices_.pop_back();
    if (!heap_child_indices_.empty() && OB_FAIL(heap_sift_down(0))) {
      LOG_WARN("failed to sift disjunction heap", K(ret));
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::build_heap()
{
  int ret = OB_SUCCESS;
  heap_child_indices_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    const BatchCursor &cursor = batch_cursors_.at(i);
    if (cursor.position_ < cursor.count_
        && OB_FAIL(heap_child_indices_.push_back(i))) {
      LOG_WARN("failed to append disjunction heap child", K(ret), K(i));
    }
  }
  // Initialization builds the complete heap. Bottom-up heapify is linear in
  // child count; incremental insertion would add avoidable
  // O(children * log(children)) first-row work for a large IN predicate.
  for (int64_t i = heap_child_indices_.count() / 2;
       OB_SUCC(ret) && i > 0;) {
    --i;
    if (OB_FAIL(heap_sift_down(i))) {
      LOG_WARN("failed to heapify disjunction children", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::init_batch_cursor()
{
  int ret = OB_SUCCESS;
  child_batch_size_ = calc_child_batch_size(children_cnt_);
  if (OB_FAIL(ensure_batch_buffer())) {
    LOG_WARN("failed to initialize disjunction batch buffer", K(ret));
  } else if (OB_FAIL(batch_cursors_.prepare_allocate(
      children_cnt_, BatchCursor()))) {
    LOG_WARN("failed to initialize disjunction batch cursors", K(ret),
        K_(children_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    batch_cursors_.at(i).reset();
  }
  return ret;
}

int ObDASDisjunctionFilterOp::prime_batch_cursor()
{
  int ret = OB_SUCCESS;
  if (!batch_cursor_primed_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(refill_batch_child(i, 1))) {
        LOG_WARN("failed to prime disjunction batch child", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(build_heap())) {
      LOG_WARN("failed to initialize disjunction heap", K(ret));
    } else if (OB_SUCC(ret)) {
      batch_cursor_primed_ = true;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::refill_batch_child(const int64_t child_idx, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  const int64_t effective_capacity = MIN(child_batch_size_, MAX(1L, capacity));
  if (batch_cursors_.at(child_idx).source_exhausted_) {
    batch_cursors_.at(child_idx).position_ = 0;
    batch_cursors_.at(child_idx).count_ = 0;
  } else {
    ObDASRowID boundary;
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    uint64_t *child_ids =
        batch_ids_ + child_idx * child_batch_size_;
    boundary.set_max();
    if (OB_FAIL(children_[child_idx]->drain_until(
        boundary, effective_capacity, child_ids, nullptr,
        count, next_id_after, next_score_after))) {
      LOG_WARN("failed to batch drain disjunction child", K(ret), K(child_idx));
    } else if (OB_UNLIKELY(count < 0 || count > effective_capacity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid disjunction child batch count", K(ret), K(child_idx),
          K(count), K(effective_capacity));
    } else if (count < effective_capacity && !next_id_after.is_max()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lookahead for unbounded disjunction child drain", K(ret),
          K(child_idx), K(count), K(next_id_after), K(next_score_after));
    } else {
      BatchCursor &cursor = batch_cursors_.at(child_idx);
      cursor.position_ = 0;
      cursor.count_ = static_cast<int16_t>(count);
      cursor.source_exhausted_ = count < effective_capacity;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::advance_batch_child_to(
    const int64_t child_idx,
    const uint64_t target)
{
  int ret = OB_SUCCESS;
  BatchCursor &cursor = batch_cursors_.at(child_idx);
  uint64_t *child_ids = batch_ids_ + child_idx * child_batch_size_;
  if (cursor.position_ < cursor.count_) {
    const uint64_t *found = std::lower_bound(
        child_ids + cursor.position_, child_ids + cursor.count_, target);
    cursor.position_ = static_cast<int16_t>(found - child_ids);
  }
  if (cursor.position_ >= cursor.count_ && !cursor.source_exhausted_) {
    ObDASRowID target_id;
    ObDASRowID advanced_id;
    double advanced_score = 0.0;
    target_id.set_uint64(target);
    ret = children_[child_idx]->advance_to(target_id, advanced_id, advanced_score);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      cursor.position_ = 0;
      cursor.count_ = 0;
      cursor.source_exhausted_ = true;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to seek disjunction batch child", K(ret), K(child_idx), K(target));
    } else if (OB_UNLIKELY(!advanced_id.is_normal()
                           || advanced_id.get_uint64() < target)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid disjunction batch seek result", K(ret), K(child_idx),
          K(target), K(advanced_id));
    } else {
      child_ids[0] = advanced_id.get_uint64();
      cursor.position_ = 0;
      cursor.count_ = 1;
      cursor.source_exhausted_ = false;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::requeue_batch_child(
    const int64_t child_idx,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  BatchCursor &cursor = batch_cursors_.at(child_idx);
  if (cursor.position_ >= cursor.count_ && !cursor.source_exhausted_
      && OB_FAIL(refill_batch_child(child_idx, capacity))) {
    LOG_WARN("failed to refill consumed disjunction child", K(ret), K(child_idx));
  }
  if (OB_SUCC(ret) && cursor.position_ < cursor.count_
      && OB_FAIL(heap_push(child_idx))) {
    LOG_WARN("failed to requeue disjunction batch child", K(ret), K(child_idx));
  }
  return ret;
}

int ObDASDisjunctionFilterOp::next_batch_rowid(
    ObDASRowID &next_id,
    double &score,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prime_batch_cursor())) {
    LOG_WARN("failed to prime disjunction batch cursor", K(ret));
  } else if (heap_child_indices_.empty()) {
    iter_end_ = true;
    cur_row_id_.set_max();
    ret = OB_ITER_END;
  } else {
    const uint64_t min_id =
        batch_child_head(heap_child_indices_.at(0));
    while (OB_SUCC(ret) && !heap_child_indices_.empty()
           && batch_child_head(heap_child_indices_.at(0)) == min_id) {
      int64_t child_idx = -1;
      if (OB_FAIL(heap_pop(child_idx))) {
        LOG_WARN("failed to pop disjunction batch child", K(ret));
      } else {
        ++batch_cursors_.at(child_idx).position_;
        if (OB_FAIL(requeue_batch_child(child_idx, capacity))) {
          LOG_WARN("failed to advance disjunction batch child", K(ret), K(child_idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_row_id_.set_uint64(min_id);
      next_id = cur_row_id_;
      score = is_scoring_ ? match_score_ : 0.0;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::advance_batch_rowid(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (target.is_max()) {
    iter_end_ = true;
    cur_row_id_.set_max();
    ret = OB_ITER_END;
  } else if (OB_FAIL(prime_batch_cursor())) {
    LOG_WARN("failed to prime disjunction batch cursor", K(ret));
  } else if (cur_row_id_.is_normal()
             && (target.is_min() || cur_row_id_.get_uint64() >= target.get_uint64())) {
    curr_id = cur_row_id_;
    score = is_scoring_ ? match_score_ : 0.0;
  } else {
    const uint64_t target_u64 = target.is_min() ? 0 : target.get_uint64();
    // Only children whose current head is below target need to move. Keeping
    // the other heap entries intact avoids an O(children) scan and full heap
    // rebuild for every seek issued by a parent conjunction.
    while (OB_SUCC(ret) && !heap_child_indices_.empty()
           && batch_child_head(heap_child_indices_.at(0)) < target_u64) {
      int64_t child_idx = -1;
      if (OB_FAIL(heap_pop(child_idx))) {
        LOG_WARN("failed to pop disjunction batch child for seek", K(ret), K(target));
      } else if (OB_FAIL(advance_batch_child_to(child_idx, target_u64))) {
        LOG_WARN("failed to advance disjunction batch child", K(ret), K(child_idx), K(target));
      } else if (OB_FAIL(requeue_batch_child(child_idx, 1))) {
        LOG_WARN("failed to requeue advanced disjunction child", K(ret), K(child_idx), K(target));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (heap_child_indices_.empty()) {
      iter_end_ = true;
      cur_row_id_.set_max();
      ret = OB_ITER_END;
    } else if (OB_FAIL(next_batch_rowid(curr_id, score, 1))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to emit advanced disjunction batch row", K(ret), K(target));
      }
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::open_children(bool &can_batch_merge)
{
  int ret = OB_SUCCESS;
  can_batch_merge = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", KR(ret));
    } else {
      can_batch_merge = can_batch_merge
          && children_[i]->can_enumerate_rowids();
      if (OB_FAIL(children_[i]->open())) {
        LOG_WARN("failed to open child", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_open()
{
  int ret = OB_SUCCESS;
  bool can_batch_merge = true;
  if (OB_FAIL(open_children(can_batch_merge))) {
    LOG_WARN("failed to open disjunction children", K(ret));
  } else {
    // children_cnt_ is finalized by ObIDASSearchOp::init() before open(). Keep
    // one merge mode for the whole operator lifetime, as segment merge does.
    // A candidate-relative child (for example minimum_should_match=0) cannot
    // be pre-materialized through drain_until(), so retain the probe path.
    use_batch_merge_ = !is_async_nested_scope(scope_)
        && 1 == min_should_match_
        && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
        && children_cnt_ > 0
        && can_batch_merge;
    if (OB_FAIL(reset_merge_cursor())) {
      LOG_WARN("failed to reset disjunction cursor on open", K(ret));
    } else if (is_async_nested_scope(scope_) && nested_compare_bytes() > sizeof(uint64_t)
        && OB_ISNULL(current_nk_buf_ = static_cast<char *>(ctx_allocator().alloc(nested_compare_bytes())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate disjunction nested key buffer", KR(ret), K(nesting_depth_));
    } else {
      is_first_nk_ = true;
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::rescan_children(const int64_t opened_children_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(opened_children_cnt < 0 || opened_children_cnt > children_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid opened child count", K(ret), K(opened_children_cnt), K_(children_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", KR(ret));
    } else if (i < opened_children_cnt && OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan", KR(ret), K(i));
    } else if (i >= opened_children_cnt && OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open appended child", KR(ret), K(i));
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rescan_children(children_cnt_))) {
    LOG_WARN("failed to rescan disjunction children", K(ret));
  } else if (OB_FAIL(reset_merge_cursor())) {
    LOG_WARN("failed to reset disjunction cursor on rescan", K(ret));
  } else {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_rescan_segment(
    const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  // Segment switching is lighter than a partition rescan, but this operator's
  // merge cursors are segment-local. Propagate the version range to every leaf
  // and reset the cached child rowids before reading the new segment.
  if (OB_FAIL(ObIDASSearchOp::do_rescan_segment(range))) {
    LOG_WARN("failed to rescan disjunction children for async segment", K(ret), K(range));
  } else if (OB_FAIL(reset_merge_cursor())) {
    LOG_WARN("failed to reset disjunction cursor for async segment", K(ret));
  } else {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_close()
{
  int ret = OB_SUCCESS;
  rowid_allocators_[0].reset();
  rowid_allocators_[1].reset();
  rowid_allocator_idx_ = 0;
  cur_row_id_.reset();
  child_row_ids_.reset();
  child_nested_keys_.reset();
  if (OB_NOT_NULL(current_nk_buf_)) {
    ctx_allocator().free(current_nk_buf_);
    current_nk_buf_ = nullptr;
  }
  batch_allocator_.reset();
  batch_ids_ = nullptr;
  batch_row_capacity_ = 0;
  child_batch_size_ = 0;
  batch_cursors_.reset();
  heap_child_indices_.reset();
  use_batch_merge_ = false;
  batch_cursor_primed_ = false;
  iter_end_ = false;
  is_first_nk_ = true;
  return ret;
}

int ObDASDisjunctionFilterOp::init_nested_key_cursor()
{
  int ret = OB_SUCCESS;
  curr_nk_.set_min();
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObDASNestedKey &key = child_nested_keys_.at(i);
    int cmp = 0;
    if (OB_FAIL(compare_rowid(child_row_ids_.at(i), cur_row_id_, cmp))) {
      LOG_WARN("failed to compare nested candidate documents", K(ret), K(i));
    } else if (0 == cmp) {
      key.set_min();
    } else {
      key.set_max();
    }
  }
  if (OB_SUCC(ret)) {
    is_first_nk_ = false;
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  const bool has_nested_cursor = !is_first_nk_ && cur_row_id_.is_normal();
  int cmp = 0;
  if (has_nested_cursor && OB_FAIL(compare_rowid(target, cur_row_id_, cmp))) {
    LOG_WARN("failed to compare nested document target", KR(ret), K(target), K_(cur_row_id));
  } else if (has_nested_cursor && cmp <= 0
             && (min_should_match_ > 0 || 0 == cmp)) {
    // Keep the current document and its partially consumed or exhausted NK cursor.
    curr_id = cur_row_id_;
    score = is_scoring_ ? match_score_ : 0.0;
  } else {
    // MSM=0 accepts a different caller-supplied candidate rather than the cached document.
    is_first_nk_ = true;
    if (use_batch_merge_) {
      ret = advance_batch_rowid(target, curr_id, score);
    } else {
      ret = advance_impl<ObDASRowID>(target, curr_id, score);
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize nested key cursor", KR(ret));
  } else if (target.is_max()
             || (curr_nk_.is_max() && (min_should_match_ > 0 || target.is_min()))) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(compare_rowid(target, curr_nk_, cmp))) {
    LOG_WARN("failed to compare nested key target", KR(ret));
  } else if (curr_nk_.is_normal() && cmp <= 0) {
    curr_nk = curr_nk_;
  } else if (target.is_min() && curr_nk_.is_min()) {
    ret = next_impl<ObDASNestedKey>(curr_nk, score);
  } else {
    ret = advance_impl<ObDASNestedKey>(target, curr_nk, score);
  }
  if (OB_SUCC(ret)) {
    curr_nk_ = curr_nk;
  } else if (OB_ITER_END == ret) {
    curr_nk_.set_max();
  }
  return ret;
}

template <typename RowType>
int ObDASDisjunctionFilterOp::advance_impl(const RowType &target, RowType &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  ObArray<RowType> *child_ids = nullptr;
  if constexpr (std::is_same_v<RowType, ObDASNestedKey>) {
    child_ids = &child_nested_keys_;
  } else {
    child_ids = &child_row_ids_;
  }
  RowType candidate_target;
  RowType next_possible_target = target;
  RowType matched_row_id;
  int cmp = 0;
  bool enough_matched = false;
  bool has_matched_row_id = false;
  while (OB_SUCC(ret) && !enough_matched) {
    RowType tmp_id;
    double tmp_score = 0.0;
    int64_t matched_count = 0;
    enough_matched = false;
    candidate_target = next_possible_target;
    matched_row_id.reset();
    has_matched_row_id = false;
    next_possible_target.set_max();
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      ObIDASSearchOp *op = children_[i];
      RowType &child_id = child_ids->at(i);
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", KR(ret), K(i));
      } else if (OB_FAIL(compare_rowid(candidate_target, child_id, cmp))) {
        LOG_WARN("failed to compare rowids", KR(ret), K(i), K(candidate_target), K(child_id));
      } else if (cmp <= 0) {
        // already advanced to the candidate_target in the previous iteration, do nothing
      } else if (FALSE_IT(child_id.reset())) {
      } else if (OB_FAIL(op->advance_to(candidate_target, tmp_id, tmp_score))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          child_id.set_max();
        } else {
          LOG_WARN("failed to advance to", KR(ret), K(candidate_target));
        }
      } else {
        child_id = tmp_id;
      }
      if (OB_SUCC(ret)) {
        bool is_seek_prefix = false;
        if constexpr (std::is_same_v<RowType, ObDASNestedKey>) {
          // A seek prefix is not yet a candidate at the children's common depth.
          is_seek_prefix = candidate_target.is_normal() && child_id.is_normal()
              && candidate_target.length() < child_id.length();
        }
        if (OB_FAIL(compare_rowid(candidate_target, child_id, cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(child_id), K(candidate_target));
        } else if (cmp != 0 || is_seek_prefix) {
          if (cmp > 0) {
            // result should not be less than the target
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child_id < candidate_target", KR(ret), K(i), K(child_id), K(candidate_target));
          } else {
            if (OB_FAIL(compare_rowid(next_possible_target, child_id, cmp))) {
              LOG_WARN("failed to compare rowids", KR(ret), K(next_possible_target), K(child_id));
            } else if (cmp > 0) {
              next_possible_target = child_id;
            }
          }
        } else {
          if (0 == matched_count) {
            matched_row_id = child_id;
            has_matched_row_id = true;
          }
          matched_count++;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (matched_count >= min_should_match_) {
      enough_matched = true;
    } else if (next_possible_target.is_max()) {
      ret = OB_ITER_END;
    }
  }
  if (OB_SUCC(ret)) {
    const RowType &source_row_id =
        has_matched_row_id ? matched_row_id : candidate_target;
    if constexpr (std::is_same_v<RowType, ObDASNestedKey>) {
      if (!has_matched_row_id && source_row_id.is_normal()
          && source_row_id.length() > sizeof(uint64_t)) {
        if (OB_UNLIKELY(source_row_id.length() > nested_compare_bytes())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("nested key exceeds disjunction buffer length", KR(ret), K(source_row_id), K(nesting_depth_));
        } else {
          // An accepted zero-match target can borrow the caller's temporary buffer.
          if (source_row_id.ptr() != current_nk_buf_) {
            MEMCPY(current_nk_buf_, source_row_id.ptr(), source_row_id.length());
          }
          curr_id.assign(current_nk_buf_, source_row_id.length());
        }
      } else {
        curr_id = source_row_id;
      }
    } else {
      if (DAS_ROWID_TYPE_UINT64 == get_rowid_type()) {
        cur_row_id_ = source_row_id;
      } else {
        const int64_t next_allocator_idx = 1 - rowid_allocator_idx_;
        ObDASRowID stable_row_id;
        rowid_allocators_[next_allocator_idx].reuse();
        if (OB_FAIL(deep_copy_rowid(
            source_row_id, stable_row_id, rowid_allocators_[next_allocator_idx]))) {
          LOG_WARN("failed to preserve disjunction output rowid", K(ret));
        } else {
          rowid_allocators_[rowid_allocator_idx_].reuse();
          rowid_allocator_idx_ = next_allocator_idx;
          cur_row_id_ = stable_row_id;
        }
      }
      if (OB_SUCC(ret)) {
        curr_id = cur_row_id_;
        score = is_scoring_ ? match_score_ : 0.0;
      }
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  is_first_nk_ = true;
  if (use_batch_merge_) {
    ret = next_batch_rowid(next_id, score, 1);
  } else {
    ret = next_impl<ObDASRowID>(next_id, score);
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_next_rowid(ObDASNestedKey &next_nk, double &score)
{
  INIT_SUCC(ret);
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize nested key cursor", KR(ret));
  } else if (curr_nk_.is_max()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(next_impl<ObDASNestedKey>(next_nk, score))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next nested key", K(ret));
    } else {
      curr_nk_.set_max();
    }
  } else {
    curr_nk_ = next_nk;
  }
  return ret;
}

template <typename RowType>
int ObDASDisjunctionFilterOp::next_impl(RowType &next_id, double &score)
{
  int ret = OB_SUCCESS;
  ObArray<RowType> *child_ids = nullptr;
  RowType current;
  if constexpr (std::is_same_v<RowType, ObDASNestedKey>) {
    child_ids = &child_nested_keys_;
    current = curr_nk_;
    // Preserve the current key before its child can reuse the output buffer.
    if (current.is_normal() && current.length() > sizeof(uint64_t)) {
      if (OB_UNLIKELY(current.length() > nested_compare_bytes())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nested key exceeds disjunction buffer length", KR(ret), K(current), K(nesting_depth_));
      } else if (current.ptr() != current_nk_buf_) {
        MEMCPY(current_nk_buf_, current.ptr(), current.length());
        current.assign(current_nk_buf_, current.length());
      }
    }
  } else {
    child_ids = &child_row_ids_;
    current = cur_row_id_;
  }
  RowType candidate;
  candidate.set_max();
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    RowType &head = child_ids->at(i);
    RowType tmp_id;
    double child_score = 0.0;
    int cmp = 0;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret), K(i));
    } else if (OB_FAIL(compare_rowid(current, head, cmp))) {
      LOG_WARN("failed to compare rowids", KR(ret), K(i), K(current), K(head));
    } else if (0 == cmp) {
      head.reset();
      if (OB_FAIL(op->next_rowid(tmp_id, child_score))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          head.set_max();
        } else {
          LOG_WARN("failed to next rowid", KR(ret), K(i));
        }
      } else {
        head = tmp_id;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compare_rowid(head, candidate, cmp))) {
        LOG_WARN("failed to compare next child", K(ret));
      } else if (cmp < 0) {
        candidate = head;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (candidate.is_max()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(advance_impl(candidate, next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret), K(candidate));
  }
  return ret;
}

template int ObDASDisjunctionFilterOp::advance_impl<ObDASRowID>(
    const ObDASRowID &, ObDASRowID &, double &);
template int ObDASDisjunctionFilterOp::advance_impl<ObDASNestedKey>(
    const ObDASNestedKey &, ObDASNestedKey &, double &);
template int ObDASDisjunctionFilterOp::next_impl<ObDASRowID>(ObDASRowID &, double &);
template int ObDASDisjunctionFilterOp::next_impl<ObDASNestedKey>(ObDASNestedKey &, double &);

int ObDASDisjunctionFilterOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  is_first_nk_ = true;
  int ret = OB_SUCCESS;
  const int64_t fetch_capacity = child_batch_size_ > 0
      ? MIN(child_batch_size_, MAX(1L, capacity)) : 1;
  if (!use_batch_merge_) {
    ret = ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after);
  } else if (iter_end_) {
    next_id_after.set_max();
  } else if (OB_FAIL(prime_batch_cursor())) {
    LOG_WARN("failed to prime disjunction batch cursor", K(ret));
  } else if (!boundary.is_max()) {
    bool done = false;
    count = 0;
    next_id_after.reset();
    next_score_after = 0.0;
    const uint64_t boundary_u64 = boundary.get_uint64();
    if (iter_end_) {
      next_id_after.set_max();
      done = true;
    }
    while (OB_SUCC(ret) && count < capacity && !done) {
      ObDASRowID row_id;
      double score = 0.0;
      if (OB_FAIL(next_batch_rowid(row_id, score, fetch_capacity))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          next_id_after.set_max();
          done = true;
        } else {
          LOG_WARN("failed to get next disjunction batch row", K(ret));
        }
      } else if (row_id.get_uint64() > boundary_u64) {
        next_id_after = row_id;
        next_score_after = score;
        done = true;
      } else {
        out_ids[count] = row_id.get_uint64();
        if (nullptr != out_scores) {
          out_scores[count] = score;
        }
        ++count;
      }
    }
  } else {
    bool done = false;
    count = 0;
    next_id_after.reset();
    next_score_after = 0.0;
    if (iter_end_) {
      next_id_after.set_max();
      done = true;
    }
    while (OB_SUCC(ret) && count < capacity && !done) {
      if (heap_child_indices_.empty()) {
        iter_end_ = true;
        cur_row_id_.set_max();
        next_id_after.set_max();
        done = true;
      } else {
        const uint64_t min_id =
            batch_child_head(heap_child_indices_.at(0));
        int64_t min_child_idx = -1;
        if (OB_FAIL(heap_pop(min_child_idx))) {
          LOG_WARN("failed to pop batch disjunction child", K(ret));
        } else {
          const bool has_competitor = !heap_child_indices_.empty();
          const uint64_t competitor_id = has_competitor
              ? batch_child_head(heap_child_indices_.at(0)) : 0;
          if (has_competitor && competitor_id == min_id) {
            // Consume every equal heap head, including repeated equal values
            // from one child, and emit the union value exactly once.
            ++batch_cursors_.at(min_child_idx).position_;
            if (OB_FAIL(requeue_batch_child(min_child_idx, fetch_capacity))) {
              LOG_WARN("failed to advance duplicate disjunction child", K(ret),
                  K(min_child_idx));
            }
            while (OB_SUCC(ret) && !heap_child_indices_.empty()
                   && batch_child_head(heap_child_indices_.at(0)) == min_id) {
              int64_t duplicate_child_idx = -1;
              if (OB_FAIL(heap_pop(duplicate_child_idx))) {
                LOG_WARN("failed to pop duplicate disjunction child", K(ret));
              } else {
                ++batch_cursors_.at(duplicate_child_idx).position_;
                if (OB_FAIL(requeue_batch_child(
                    duplicate_child_idx, fetch_capacity))) {
                  LOG_WARN("failed to advance duplicate disjunction child", K(ret),
                      K(duplicate_child_idx));
                }
              }
            }
            if (OB_SUCC(ret)) {
              out_ids[count] = min_id;
              if (nullptr != out_scores) {
                out_scores[count] = is_scoring_ ? match_score_ : 0.0;
              }
              ++count;
              cur_row_id_.set_uint64(min_id);
            }
          } else {
            BatchCursor &cursor = batch_cursors_.at(min_child_idx);
            uint64_t *child_ids = batch_ids_
                + min_child_idx * child_batch_size_;
            const uint64_t *run_begin = child_ids + cursor.position_;
            const uint64_t *run_end = child_ids + cursor.count_;
            if (has_competitor) {
              run_end = std::lower_bound(run_begin, run_end, competitor_id);
            }
            const int64_t run_count = static_cast<int64_t>(run_end - run_begin);
            const int64_t copy_count = MIN(run_count, capacity - count);
            if (OB_UNLIKELY(copy_count <= 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid batch disjunction sorted run", K(ret),
                  K(min_child_idx), K(min_id), K(competitor_id),
                  K(has_competitor), K(cursor.position_), K(cursor.count_));
            } else {
              MEMCPY(out_ids + count, run_begin,
                  copy_count * static_cast<int64_t>(sizeof(uint64_t)));
              if (nullptr != out_scores) {
                std::fill_n(out_scores + count, copy_count,
                    is_scoring_ ? match_score_ : 0.0);
              }
              cursor.position_ = static_cast<int16_t>(cursor.position_ + copy_count);
              count += copy_count;
              cur_row_id_.set_uint64(out_ids[count - 1]);
              if (OB_FAIL(requeue_batch_child(min_child_idx, fetch_capacity))) {
                LOG_WARN("failed to requeue batch disjunction child", K(ret),
                    K(min_child_idx));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, is_scoring_ ? match_score_ : 0.0);
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(
      target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = is_scoring_ ? match_score_ : 0.0;
  return ret;
}

} // namespace sql
} // namespace oceanbase
