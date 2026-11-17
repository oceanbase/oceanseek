/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "common/ob_smart_call.h"
#include <algorithm>
#include <utility>
#include "lib/utility/ob_sort.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASAsyncSegmentMergeOpParam::get_children_ops(
    ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(segment_ops_)) {
    if (OB_FAIL(children.assign(*segment_ops_))) {
      LOG_WARN("failed to assign async segment children", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::record_mode_metric()
{
  int ret = OB_SUCCESS;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    SET_METRIC_VAL(common::ObMetricId::HS_ASYNC_SEGMENT_MODE_DIRECT_MERGE, 1);
  } else {
    SET_METRIC_VAL(common::ObMetricId::HS_ASYNC_SEGMENT_MODE_SORT_MERGE, 1);
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::init_direct_rowid_output_exprs(
    const ObIArray<ObIDASSearchOp *> &segment_ops)
{
  int ret = OB_SUCCESS;
  rowid_output_exprs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < segment_ops.count(); ++i) {
    const ObIDASSearchOp *child = segment_ops.at(i);
    const ObIArray<ObExpr *> *child_output_exprs = nullptr;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null direct merge child", K(ret), K(i));
    } else if (OB_ISNULL(child_output_exprs = child->get_rowid_output_exprs())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct merge child has no RowID output expressions", K(ret), K(i),
          K(child->get_op_type()));
    } else if (OB_UNLIKELY(child_output_exprs->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct merge child has empty RowID output expressions", K(ret), K(i));
    } else if (0 == i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < child_output_exprs->count(); ++j) {
        if (OB_ISNULL(child_output_exprs->at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("direct merge child has null RowID output expression", K(ret), K(i), K(j));
        } else if (OB_FAIL(rowid_output_exprs_.push_back(child_output_exprs->at(j)))) {
          LOG_WARN("failed to cache direct merge RowID output expression", K(ret), K(i), K(j));
        }
      }
    } else if (OB_UNLIKELY(child_output_exprs->count() != rowid_output_exprs_.count())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("direct merge children have different RowID output expression counts", K(ret),
          K(i), "child_expr_count", child_output_exprs->count(),
          "expected_expr_count", rowid_output_exprs_.count());
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < child_output_exprs->count(); ++j) {
        if (OB_UNLIKELY(child_output_exprs->at(j) != rowid_output_exprs_.at(j))) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("direct merge children write different RowID output expressions", K(ret),
              K(i), K(j), KP(child_output_exprs->at(j)), KP(rowid_output_exprs_.at(j)));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    rowid_output_exprs_.reset();
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncSegmentMergeOpParam &param =
      static_cast<const ObDASAsyncSegmentMergeOpParam &>(op_param);
  if (OB_UNLIKELY(param.get_segment_op_cnt() <= 0 || !param.segment_domain_.is_valid()
                  || (ObDASAsyncSegmentMergeMode::SORT_MERGE != param.mode_
                      && ObDASAsyncSegmentMergeMode::DIRECT_MERGE != param.mode_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async segment merge op requires children and a segment domain", K(ret),
             K(param.get_segment_op_cnt()), K(param.segment_domain_),
             "mode", static_cast<int>(param.mode_));
  } else {
    segment_domain_ = param.segment_domain_;
    force_merge_ = param.force_merge_;
    mode_ = param.mode_;
    direct_drain_enabled_ = ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_;
    for (int64_t i = 0;
         direct_drain_enabled_ && i < param.get_segment_op_cnt();
         ++i) {
      direct_drain_enabled_ = OB_NOT_NULL(param.segment_ops_->at(i))
          && param.segment_ops_->at(i)->supports_direct_rowid_drain();
    }
    if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_
        && !direct_drain_enabled_
        && OB_FAIL(init_direct_rowid_output_exprs(*param.segment_ops_))) {
      LOG_WARN("failed to initialize direct merge RowID output expressions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.get_segment_op_cnt(); ++i) {
      if (OB_FAIL(child_opened_.push_back(false))) {
        LOG_WARN("failed to initialize segment opened state", K(ret), K(i));
      } else if (direct_drain_enabled_) {
        ObDASRowID next_rowid;
        next_rowid.set_min();
        if (OB_FAIL(direct_child_next_rowids_.push_back(next_rowid))) {
          LOG_WARN("failed to initialize direct segment cursor", K(ret), K(i));
        }
      } else if (ObDASAsyncSegmentMergeMode::SORT_MERGE == mode_) {
        ObDASRowID rowid;
        rowid.set_max();
        if (OB_FAIL(head_rowids_.push_back(rowid))) {
          LOG_WARN("failed to initialize segment head rowid", K(ret), K(i));
        } else if (OB_FAIL(head_scores_.push_back(0.0))) {
          LOG_WARN("failed to initialize segment head score", K(ret), K(i));
        } else if (OB_FAIL(child_ended_.push_back(true))) {
          LOG_WARN("failed to initialize segment ended state", K(ret), K(i));
        } else if (OB_FAIL(batch_cursors_.push_back(BatchCursor()))) {
          LOG_WARN("failed to initialize segment batch cursor", K(ret), K(i));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    head_rowids_.reset();
    head_scores_.reset();
    child_ended_.reset();
    batch_cursors_.reset();
    child_opened_.reset();
    direct_child_next_rowids_.reset();
    direct_active_child_indices_.reset();
    rowid_output_exprs_.reset();
    direct_drain_enabled_ = false;
  }
  return ret;
}

void ObDASAsyncSegmentMergeOp::reset_direct_state()
{
  segment_ranges_.reset();
  active_segment_cnt_ = 0;
  current_batch_child_idx_ = 0;
  direct_active_child_indices_.reuse();
  for (int64_t i = 0; i < direct_child_next_rowids_.count(); ++i) {
    direct_child_next_rowids_.at(i).set_min();
  }
}

int ObDASAsyncSegmentMergeOp::bind_direct_active_children()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null direct async segment child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan_segment(segment_ranges_.at(i)))) {
      LOG_WARN("failed to bind direct async segment child", K(ret), K(i),
          K(segment_ranges_.at(i)));
    } else if (direct_drain_enabled_
               && OB_FAIL(direct_active_child_indices_.push_back(i))) {
      LOG_WARN("failed to add active direct async segment child", K(ret), K(i));
    }
  }
  return ret;
}

void ObDASAsyncSegmentMergeOp::reset_merge_state()
{
  ordered_child_indices_.reuse();
  ordered_child_pos_ = 0;
  use_ordered_concat_ = false;
  concat_head_consumed_ = false;
  active_segment_cnt_ = 0;
  shallow_target_ = 0;
  shallow_inclusive_ = true;
  in_shallow_status_ = false;
  iter_end_ = false;
  heap_child_indices_.reset();
  pending_child_indices_.reset();
  nested_heap_child_indices_.reuse();
  nested_pending_child_indices_.reuse();
  for (int64_t i = 0; i < head_rowids_.count(); ++i) {
    head_rowids_.at(i).set_max();
  }
  for (int64_t i = 0; i < head_scores_.count(); ++i) {
    head_scores_.at(i) = 0.0;
  }
  for (int64_t i = 0; i < child_ended_.count(); ++i) {
    child_ended_.at(i) = true;
  }
  for (int64_t i = 0; i < batch_cursors_.count(); ++i) {
    batch_cursors_.at(i).reset();
  }
}

int ObDASAsyncSegmentMergeOp::load_current_partition_segments()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  segment_ranges_.reset();
  if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null current async index partition", K(ret));
  } else {
    int64_t tablet_idx = -1;
    const storage::ObAsyncIndexSegmentGroupCtx &storage_ctx =
        partition->get_storage_group_ctx();
    if (OB_FAIL(partition->find_index_tablet_idx(
            segment_domain_.index_table_id_, tablet_idx))) {
      LOG_WARN("failed to find per-index async segment domain", K(ret), K_(segment_domain));
    }
    const int64_t group_begin = segment_domain_.group_slot_ >= 0
        ? segment_domain_.group_slot_ : 0;
    const int64_t group_end = segment_domain_.group_slot_ >= 0
        ? segment_domain_.group_slot_ + 1 : storage_ctx.get_group_count();
    if (OB_SUCC(ret)
        && OB_UNLIKELY(group_begin < 0 || group_end > storage_ctx.get_group_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async segment group slot exceeds current partition",
               K(ret), K_(segment_domain), "group_count", storage_ctx.get_group_count());
    }
    for (int64_t g = group_begin; OB_SUCC(ret) && g < group_end; ++g) {
      const storage::ObAsyncIndexSegmentGroup *group = storage_ctx.get_group(g);
      const ObIArray<ObNewVersionRange> &version_ranges =
          group->get_tablet_version_ranges(tablet_idx);
      for (int64_t r = 0; OB_SUCC(ret) && r < version_ranges.count(); ++r) {
        if (OB_FAIL(segment_ranges_.push_back(version_ranges.at(r)))) {
          LOG_WARN("failed to append per-index async segment range", K(ret), K(g), K(r));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(segment_ranges_.count() > children_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current partition has more segments than the fixed tree",
             K(ret), K(segment_ranges_.count()), K_(children_cnt));
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::check_segment_rowid_ranges(
    bool &disjoint,
    ObIArray<int64_t> &ordered_child_indices) const
{
  int ret = OB_SUCCESS;
  bool all_available = true;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  ObSEArray<std::pair<uint64_t, int64_t>, 4> sorted_ranges;
  // Indexed by original child index, including empty Segments omitted from sorted_ranges.
  // Every available child must append its maximum before the empty-range check.
  ObSEArray<uint64_t, 4> max_rowids;
  disjoint = false;
  ordered_child_indices.reuse();
  if (ObDASAsyncSegmentMergeMode::SORT_MERGE != mode_
      || DAS_ROWID_TYPE_UINT64 != get_rowid_type() || active_segment_cnt_ <= 1) {
    // Keep DIRECT_MERGE and the existing single-Segment behavior unchanged.
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null partition for async segment range selection", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && all_available && i < active_segment_cnt_; ++i) {
      bool available = false;
      uint64_t min_rowid = UINT64_MAX;
      uint64_t max_rowid = 0;
      if (OB_FAIL(partition->get_segment_rowid_range(segment_domain_.index_table_id_,
              segment_ranges_.at(i), available, min_rowid, max_rowid))) {
        LOG_WARN("failed to get async segment rowid range", K(ret), K(i));
      } else if (!available) {
        all_available = false;
      } else if (OB_FAIL(max_rowids.push_back(max_rowid))) {
        LOG_WARN("failed to save async segment maximum rowid", K(ret), K(i));
      } else if (min_rowid > max_rowid) {
        // Known empty Segments do not participate in ordering or overlap checks.
      } else if (OB_FAIL(sorted_ranges.push_back(std::make_pair(min_rowid, i)))) {
        LOG_WARN("failed to save async segment range order", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && all_available) {
      lib::ob_sort(sorted_ranges.begin(), sorted_ranges.end());
      disjoint = true;
      for (int64_t i = 1; disjoint && i < sorted_ranges.count(); ++i) {
        disjoint = max_rowids.at(sorted_ranges.at(i - 1).second) < sorted_ranges.at(i).first;
      }
      if (disjoint) {
        for (int64_t i = 0; OB_SUCC(ret) && i < sorted_ranges.count(); ++i) {
          if (OB_FAIL(ordered_child_indices.push_back(sorted_ranges.at(i).second))) {
            LOG_WARN("failed to save ordered async segment child", K(ret), K(i));
          }
        }
        disjoint = OB_SUCC(ret);
      }
    }
  }
  if (!disjoint) {
    ordered_child_indices.reuse();
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::ensure_batch_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t required_child_capacity = active_segment_cnt_;
  const int64_t child_id_batch_bytes = CHILD_BATCH_SIZE * sizeof(uint64_t);
  const int64_t child_score_batch_bytes = CHILD_BATCH_SIZE * sizeof(double);
  const int64_t child_score_suffix_bytes = BATCH_SCORE_BLOCK_COUNT * sizeof(double);
  if (batch_child_capacity_ < required_child_capacity
      || OB_ISNULL(batch_ids_)
      || (is_scoring_
          && (OB_ISNULL(batch_scores_) || OB_ISNULL(batch_score_suffix_maxes_)))) {
    batch_allocator_.reset();
    batch_ids_ = nullptr;
    batch_scores_ = nullptr;
    batch_score_suffix_maxes_ = nullptr;
    batch_child_capacity_ = 0;
    batch_ids_ = static_cast<uint64_t *>(
        batch_allocator_.alloc(required_child_capacity * child_id_batch_bytes));
    if (OB_ISNULL(batch_ids_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async segment batch buffer", K(ret),
          K(required_child_capacity), K_(children_cnt));
    } else if (is_scoring_
               && OB_ISNULL(batch_scores_ = static_cast<double *>(
                   batch_allocator_.alloc(
                       required_child_capacity * child_score_batch_bytes)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async segment score batch buffer", K(ret),
          K(required_child_capacity), K_(children_cnt));
    } else if (is_scoring_
               && OB_ISNULL(batch_score_suffix_maxes_ = static_cast<double *>(
                   batch_allocator_.alloc(
                       required_child_capacity * child_score_suffix_bytes)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async segment score suffix buffer", K(ret),
          K(required_child_capacity), K_(children_cnt));
    } else {
      batch_child_capacity_ = required_child_capacity;
    }
    if (OB_FAIL(ret)) {
      batch_allocator_.reset();
      batch_ids_ = nullptr;
      batch_scores_ = nullptr;
      batch_score_suffix_maxes_ = nullptr;
      batch_child_capacity_ = 0;
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::refill_batch_child(const int64_t child_idx, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  BatchCursor &cursor = batch_cursors_.at(child_idx);
  const int64_t effective_capacity = MIN(CHILD_BATCH_SIZE, MAX(1L, capacity));
  if (cursor.source_exhausted_) {
    cursor.position_ = 0;
    cursor.count_ = 0;
  } else {
    ObDASRowID boundary;
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    uint64_t *child_ids = batch_ids_ + child_idx * CHILD_BATCH_SIZE;
    double *child_scores = is_scoring_
        ? batch_scores_ + child_idx * CHILD_BATCH_SIZE : nullptr;
    const bool has_range_hi = search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value();
    if (has_range_hi) {
      boundary.set_uint64(search_ctx_.get_docid_range_hi().get_uint64());
    } else {
      boundary.set_max();
    }
    // A lookahead beyond DOCID_RANGE hi belongs to another worker and is
    // terminal for this segment child; unbounded drains must not return one.
    if (OB_FAIL(children_[child_idx]->drain_until(
        boundary, effective_capacity, child_ids, child_scores,
        count, next_id_after, next_score_after))) {
      LOG_WARN("failed to batch drain async segment child", K(ret), K(child_idx));
    } else if (OB_UNLIKELY(count < 0 || count > effective_capacity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async segment child batch count", K(ret), K(child_idx),
          K(count), K(effective_capacity));
    } else if (count < effective_capacity && !next_id_after.is_max()
               && !has_range_hi) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lookahead for unbounded async segment child drain", K(ret),
          K(child_idx), K(count), K(next_id_after), K(next_score_after));
    } else {
      cursor.position_ = 0;
      cursor.count_ = static_cast<int16_t>(count);
      cursor.source_exhausted_ = count < effective_capacity;
      cursor.source_in_shallow_status_ = false;
      if (is_scoring_ && count > 0) {
        // Store the maximum from each block boundary through the batch end.
        // A shallow lookup then scans only its leading partial block.
        double suffix_max = child_scores[count - 1];
        double *child_suffix_maxes = batch_score_suffix_maxes_
            + child_idx * BATCH_SCORE_BLOCK_COUNT;
        for (int64_t i = count - 1; i >= 0; --i) {
          suffix_max = MAX(suffix_max, child_scores[i]);
          if (0 == i % BATCH_SCORE_BLOCK_SIZE) {
            child_suffix_maxes[i / BATCH_SCORE_BLOCK_SIZE] = suffix_max;
          }
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::fetch_next_batch_head(const int64_t child_idx, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  BatchCursor &cursor = batch_cursors_.at(child_idx);
  if (cursor.position_ >= cursor.count_ && !cursor.source_exhausted_
      && OB_FAIL(refill_batch_child(child_idx, capacity))) {
    LOG_WARN("failed to refill async segment batch head", K(ret), K(child_idx));
  }
  if (OB_FAIL(ret)) {
  } else if (cursor.position_ >= cursor.count_) {
    child_ended_.at(child_idx) = true;
    head_rowids_.at(child_idx).set_max();
    head_scores_.at(child_idx) = 0.0;
  } else {
    const int16_t position = cursor.position_;
    head_rowids_.at(child_idx).set_uint64(
        batch_ids_[child_idx * CHILD_BATCH_SIZE + position]);
    head_scores_.at(child_idx) = is_scoring_
        ? batch_scores_[child_idx * CHILD_BATCH_SIZE + position] : 0.0;
    ++cursor.position_;
    child_ended_.at(child_idx) = false;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::advance_batch_head_to(
    const int64_t child_idx,
    const ObDASRowID &target,
    const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (target.is_max()) {
    BatchCursor &cursor = batch_cursors_.at(child_idx);
    child_ended_.at(child_idx) = true;
    head_rowids_.at(child_idx).set_max();
    head_scores_.at(child_idx) = 0.0;
    cursor.position_ = cursor.count_;
    cursor.source_exhausted_ = true;
    cursor.source_in_shallow_status_ = false;
  } else {
    const uint64_t target_value = target.is_min() ? 0 : target.get_uint64();
    BatchCursor &cursor = batch_cursors_.at(child_idx);
    uint64_t *child_ids = batch_ids_ + child_idx * CHILD_BATCH_SIZE;
    double *child_scores = is_scoring_
        ? batch_scores_ + child_idx * CHILD_BATCH_SIZE : nullptr;
    const bool head_matches = head_rowids_.at(child_idx).is_normal()
        && (head_rowids_.at(child_idx).get_uint64() > target_value
            || (inclusive && head_rowids_.at(child_idx).get_uint64() == target_value));
    const uint64_t *found = inclusive
        ? std::lower_bound(
            child_ids + cursor.position_, child_ids + cursor.count_, target_value)
        : std::upper_bound(
            child_ids + cursor.position_, child_ids + cursor.count_, target_value);
    cursor.position_ = static_cast<int16_t>(found - child_ids);
    if (head_matches) {
      child_ended_.at(child_idx) = false;
    } else if (cursor.position_ < cursor.count_) {
      const int16_t position = cursor.position_++;
      head_rowids_.at(child_idx).set_uint64(child_ids[position]);
      head_scores_.at(child_idx) = is_scoring_ ? child_scores[position] : 0.0;
      child_ended_.at(child_idx) = false;
    } else if (cursor.source_exhausted_) {
      child_ended_.at(child_idx) = true;
      head_rowids_.at(child_idx).set_max();
      head_scores_.at(child_idx) = 0.0;
    } else {
      ret = children_[child_idx]->advance_to(
          target, head_rowids_.at(child_idx), head_scores_.at(child_idx));
      cursor.position_ = 0;
      cursor.count_ = 0;
      cursor.source_in_shallow_status_ = false;
      if (OB_SUCC(ret) && !inclusive && head_rowids_.at(child_idx).is_normal()
          && head_rowids_.at(child_idx).get_uint64() == target_value) {
        ret = children_[child_idx]->next_rowid(
            head_rowids_.at(child_idx), head_scores_.at(child_idx));
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        child_ended_.at(child_idx) = true;
        cursor.source_exhausted_ = true;
        head_rowids_.at(child_idx).set_max();
        head_scores_.at(child_idx) = 0.0;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to advance async segment batch child", K(ret),
            K(child_idx), K(target));
      } else {
        child_ended_.at(child_idx) = false;
        cursor.source_exhausted_ = false;
        if (!is_scoring_) {
          head_scores_.at(child_idx) = 0.0;
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::get_batch_child_shallow_tuple(
    const int64_t child_idx,
    const ObDASRowID &target,
    const bool inclusive,
    bool &has_tuple,
    ObDASRowID &min_id,
    ObDASRowID &max_id,
    double &max_score)
{
  int ret = OB_SUCCESS;
  has_tuple = false;
  max_score = 0.0;
  if (child_ended_.at(child_idx)) {
    // skip exhausted child
  } else {
    BatchCursor &cursor = batch_cursors_.at(child_idx);
    const uint64_t target_value = target.get_uint64();
    uint64_t *child_ids = batch_ids_ + child_idx * CHILD_BATCH_SIZE;
    double *child_scores = batch_scores_ + child_idx * CHILD_BATCH_SIZE;
    const uint64_t head_id = head_rowids_.at(child_idx).get_uint64();
    const bool head_matches = !cursor.source_in_shallow_status_
        && (head_id > target_value || (inclusive && head_id == target_value));
    int64_t first_buffered_pos = cursor.count_;
    if (!cursor.source_in_shallow_status_ && !head_matches) {
      const uint64_t *found = inclusive
          ? std::lower_bound(
              child_ids + cursor.position_, child_ids + cursor.count_, target_value)
          : std::upper_bound(
              child_ids + cursor.position_, child_ids + cursor.count_, target_value);
      first_buffered_pos = static_cast<int64_t>(found - child_ids);
    }

    if (head_matches || first_buffered_pos < cursor.count_) {
      const int64_t score_begin = head_matches ? cursor.position_ : first_buffered_pos;
      const uint64_t first_id = head_matches ? head_id : child_ids[first_buffered_pos];
      const uint64_t last_id = score_begin < cursor.count_
          ? child_ids[cursor.count_ - 1] : first_id;
      max_score = head_matches ? head_scores_.at(child_idx) : child_scores[first_buffered_pos];
      const int64_t score_scan_begin = head_matches ? score_begin : first_buffered_pos + 1;
      // Scan up to the next block boundary and use its precomputed suffix for
      // the remainder, bounding the per-child scan below one score block.
      const int64_t next_score_block =
          (score_scan_begin + BATCH_SCORE_BLOCK_SIZE - 1)
          / BATCH_SCORE_BLOCK_SIZE * BATCH_SCORE_BLOCK_SIZE;
      const int64_t score_scan_end = MIN(next_score_block, cursor.count_);
      for (int64_t i = score_scan_begin; i < score_scan_end; ++i) {
        max_score = MAX(max_score, child_scores[i]);
      }
      if (score_scan_end < cursor.count_) {
        const double *child_suffix_maxes = batch_score_suffix_maxes_
            + child_idx * BATCH_SCORE_BLOCK_COUNT;
        max_score = MAX(max_score,
            child_suffix_maxes[score_scan_end / BATCH_SCORE_BLOCK_SIZE]);
      }
      min_id.set_uint64(first_id);
      max_id.set_uint64(last_id);
      has_tuple = true;
    } else if (cursor.source_exhausted_) {
      cursor.position_ = cursor.count_;
      cursor.source_in_shallow_status_ = false;
      child_ended_.at(child_idx) = true;
      head_rowids_.at(child_idx).set_max();
      head_scores_.at(child_idx) = 0.0;
    } else {
      const MaxScoreTuple *child_tuple = nullptr;
      ret = children_[child_idx]->advance_shallow(target, inclusive, child_tuple);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        cursor.position_ = cursor.count_;
        cursor.source_exhausted_ = true;
        cursor.source_in_shallow_status_ = false;
        child_ended_.at(child_idx) = true;
        head_rowids_.at(child_idx).set_max();
        head_scores_.at(child_idx) = 0.0;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to shallow advance async segment batch child",
            K(ret), K(child_idx), K(target), K(inclusive));
      } else if (OB_ISNULL(child_tuple)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null async segment batch child max score tuple", K(ret), K(child_idx));
      } else {
        min_id = child_tuple->get_min_id();
        max_id = child_tuple->get_max_id();
        max_score = child_tuple->get_max_score();
        cursor.source_in_shallow_status_ = true;
        has_tuple = true;
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::fetch_next_head(const int64_t child_idx, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(child_idx < 0 || child_idx >= active_segment_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment child index", K(ret), K(child_idx), K_(active_segment_cnt));
  } else if (OB_ISNULL(children_[child_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async segment child", K(ret), K(child_idx));
  } else if (use_batch_merge_) {
    if (OB_FAIL(fetch_next_batch_head(child_idx, capacity))) {
      LOG_WARN("failed to fetch async segment batch head", K(ret), K(child_idx));
    }
  } else {
    ret = children_[child_idx]->next_rowid(head_rowids_.at(child_idx), head_scores_.at(child_idx));
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      child_ended_.at(child_idx) = true;
      head_rowids_.at(child_idx).set_max();
      head_scores_.at(child_idx) = 0.0;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to fetch async segment head", K(ret), K(child_idx));
    } else {
      child_ended_.at(child_idx) = false;
    }
  }
  return ret;
}

int64_t ObDASAsyncSegmentMergeOp::current_ordered_child_idx()
{
  while (ordered_child_pos_ < ordered_child_indices_.count()
      && child_ended_.at(ordered_child_indices_.at(ordered_child_pos_))) {
    ++ordered_child_pos_;
    concat_head_consumed_ = false;
  }
  return ordered_child_pos_ < ordered_child_indices_.count()
      ? ordered_child_indices_.at(ordered_child_pos_) : -1;
}

int ObDASAsyncSegmentMergeOp::emit_ordered_concat_head(ObDASRowID &rowid, double &score)
{
  int ret = OB_SUCCESS;
  const int64_t child_idx = current_ordered_child_idx();
  if (child_idx < 0
      || (search_ctx_.has_docid_range()
          && !search_ctx_.get_docid_range_hi().is_max_value()
          && head_rowids_.at(child_idx).get_uint64()
              > search_ctx_.get_docid_range_hi().get_uint64())) {
    iter_end_ = true;
    ret = OB_ITER_END;
  } else {
    rowid = head_rowids_.at(child_idx);
    score = is_scoring_ ? head_scores_.at(child_idx) : 0.0;
    concat_head_consumed_ = true;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::next_ordered_concat(
    const int64_t capacity, ObDASRowID &rowid, double &score)
{
  int ret = OB_SUCCESS;
  const int64_t child_idx = current_ordered_child_idx();
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (in_shallow_status_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("next after concat shallow advance is unsupported", K(ret));
  } else if (child_idx >= 0 && concat_head_consumed_
      && OB_FAIL(fetch_next_head(child_idx, capacity))) {
    LOG_WARN("failed to fetch concat head", K(ret), K(child_idx));
  } else {
    concat_head_consumed_ = false;
    ret = emit_ordered_concat_head(rowid, score);
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::advance_ordered_concat(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  bool reached = false;
  if (target.is_max()) {
    ordered_child_pos_ = ordered_child_indices_.count();
  }
  while (OB_SUCC(ret) && !reached) {
    const int64_t child_idx = current_ordered_child_idx();
    int cmp = 0;
    if (child_idx < 0) {
      reached = true;
    } else if (OB_FAIL(compare_rowid(head_rowids_.at(child_idx), target, cmp))) {
      LOG_WARN("failed to compare concat head", K(ret), K(child_idx));
    } else if (cmp >= 0) {
      reached = true;
      concat_head_consumed_ = false;
    } else if (use_batch_merge_) {
      ret = advance_batch_head_to(child_idx, target);
    } else {
      is_first_nk_ = true;
      ret = children_[child_idx]->advance_to(
          target, head_rowids_.at(child_idx), head_scores_.at(child_idx));
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        child_ended_.at(child_idx) = true;
        head_rowids_.at(child_idx).set_max();
        head_scores_.at(child_idx) = 0.0;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to seek concat child", K(ret), K(child_idx));
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::drain_ordered_concat(
    const ObDASRowID &boundary, const int64_t capacity,
    uint64_t *out_ids, double *out_scores, int64_t &count,
    ObDASRowID &next_id_after, double &next_score_after)
{
  int ret = OB_SUCCESS;
  bool done = false;
  uint64_t limit = boundary.is_max() ? UINT64_MAX : boundary.get_uint64();
  if (search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value()) {
    limit = MIN(limit, search_ctx_.get_docid_range_hi().get_uint64());
  }
  while (OB_SUCC(ret) && count < capacity && !done) {
    ObDASRowID rowid;
    double score = 0.0;
    ret = next_ordered_concat(MIN(CHILD_BATCH_SIZE, capacity - count), rowid, score);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      next_id_after.set_max();
      done = true;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to drain concat child", K(ret));
    } else if (!boundary.is_max() && rowid.get_uint64() > boundary.get_uint64()) {
      next_id_after = rowid;
      next_score_after = score;
      done = true;
    } else {
      out_ids[count] = rowid.get_uint64();
      if (nullptr != out_scores) {
        out_scores[count] = score;
      }
      ++count;
      if (use_batch_merge_ && count < capacity) {
        const int64_t child_idx = current_ordered_child_idx();
        BatchCursor &cursor = batch_cursors_.at(child_idx);
        const uint64_t *ids = batch_ids_ + child_idx * CHILD_BATCH_SIZE;
        const double *scores = is_scoring_
            ? batch_scores_ + child_idx * CHILD_BATCH_SIZE : nullptr;
        const int64_t start = cursor.position_;
        while (cursor.position_ < cursor.count_ && count + cursor.position_ - start < capacity
            && ids[cursor.position_] <= limit) {
          ++cursor.position_;
        }
        const int64_t copied = cursor.position_ - start;
        if (copied > 0) {
          MEMCPY(out_ids + count, ids + start, copied * sizeof(*out_ids));
          if (nullptr != out_scores) {
            if (is_scoring_) {
              MEMCPY(out_scores + count, scores + start, copied * sizeof(*out_scores));
            } else {
              MEMSET(out_scores + count, 0, copied * sizeof(*out_scores));
            }
          }
          count += copied;
          head_rowids_.at(child_idx).set_uint64(ids[cursor.position_ - 1]);
          head_scores_.at(child_idx) = is_scoring_ ? scores[cursor.position_ - 1] : 0.0;
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::bind_and_prime_active_children()
{
  int ret = OB_SUCCESS;
  ObDASRowID range_lo;
  active_segment_cnt_ = segment_ranges_.count();
  if (OB_FAIL(check_segment_rowid_ranges(use_ordered_concat_, ordered_child_indices_))) {
    LOG_WARN("failed to check async segment physical ranges", K(ret));
  } else if (use_batch_merge_ && active_segment_cnt_ > 0
             && OB_FAIL(ensure_batch_buffer())) {
    LOG_WARN("failed to initialize async segment batch buffer", K(ret),
        K_(active_segment_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    child_ended_.at(i) = false;
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async segment child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan_segment(segment_ranges_.at(i)))) {
      LOG_WARN("failed to bind async segment child", K(ret), K(i), K(segment_ranges_.at(i)));
    } else if (threshold_cached_
               && OB_FAIL(children_[i]->set_min_competitive_score(cached_threshold_))) {
      LOG_WARN("failed to restore async segment threshold", K(ret), K(i), K_(cached_threshold));
    } else if (OB_FAIL(fetch_next_head(i, 1))) {
      LOG_WARN("failed to prime async segment child", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (search_ctx_.has_docid_range()
             && !search_ctx_.get_docid_range_lo().is_min_value()) {
    range_lo.set_uint64(search_ctx_.get_docid_range_lo().get_uint64());
    if (OB_FAIL(use_ordered_concat_ ? advance_ordered_concat(range_lo)
                                   : advance_heads_to(range_lo))) {
      LOG_WARN("failed to advance async segment heads to docid range lower bound",
               K(ret), K(range_lo));
    }
  } else if (!use_ordered_concat_ && OB_FAIL(build_heap())) {
    LOG_WARN("failed to build async segment merge heap", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::reset_children_for_partition(const bool open_children)
{
  int ret = OB_SUCCESS;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    reset_direct_state();
  } else {
    reset_merge_state();
  }
  if (OB_FAIL(load_current_partition_segments())) {
    LOG_WARN("failed to load current async index segments", K(ret));
  }
  // Empty domains leave every fixed child unopened and never read physical index metadata.
  active_segment_cnt_ = segment_ranges_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async segment child", K(ret), K(i));
    } else if (!child_opened_.at(i) && OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open async segment child", K(ret), K(i));
    } else if (!child_opened_.at(i)) {
      child_opened_.at(i) = true;
    } else if (!open_children && OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan async segment child", K(ret), K(i));
    } else if (open_children) {
      ret = OB_INIT_TWICE;
      LOG_WARN("async segment child was opened before parent open", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_
             && OB_FAIL(bind_direct_active_children())) {
    LOG_WARN("failed to bind active direct async segment children", K(ret));
  } else if (OB_SUCC(ret) && ObDASAsyncSegmentMergeMode::SORT_MERGE == mode_
             && OB_FAIL(bind_and_prime_active_children())) {
    LOG_WARN("failed to bind active sort async segment children", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::try_apply_sort_bottom(const common::ObDatum &sort_bottom, bool &applied)
{
  int ret = OB_SUCCESS;
  applied = false;
  if (mode_ != ObDASAsyncSegmentMergeMode::DIRECT_MERGE) {
    INC_METRIC_VAL(common::ObMetricId::HS_SORT_BOTTOM_BLOCKED, 1);
    LOG_TRACE("hybrid sort bottom skipped segment merge", "reason", "unsupported merge mode");
  } else if (OB_UNLIKELY(active_segment_cnt_ < 0 || active_segment_cnt_ > children_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sort bottom segment count", K(ret), K_(active_segment_cnt), K_(children_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
      bool child_applied = false;
      if (OB_ISNULL(children_) || OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort bottom segment child is null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(children_[i]->try_apply_sort_bottom(sort_bottom, child_applied)))) {
        LOG_WARN("failed to apply sort bottom to segment child", K(ret), K(i));
      }
      applied = applied || child_applied;
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_open()
{
  int ret = OB_SUCCESS;
  if (is_async_nested_scope(scope_)
      && ObDASAsyncSegmentMergeMode::SORT_MERGE == mode_) {
    if (OB_FAIL(nested_key_heads_.prepare_allocate(children_cnt_))) {
      LOG_WARN("failed to allocate segment nested key heads", KR(ret), K_(children_cnt));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(record_mode_metric())) {
    LOG_WARN("failed to record async segment merge mode metric", K(ret));
  } else if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    if (OB_FAIL(reset_children_for_partition(true))) {
      LOG_WARN("failed to open direct async segment merge", K(ret));
    }
  } else {
    // children_cnt_ and is_scoring_ are finalized by ObIDASSearchOp::init()
    // before open(). Use their worst-case buffer footprint to select one stable
    // merge mode for the whole operator lifetime.
    const int64_t child_batch_bytes = CHILD_BATCH_SIZE
        * (sizeof(uint64_t) + (is_scoring_ ? sizeof(double) : 0))
        + (is_scoring_ ? BATCH_SCORE_BLOCK_COUNT * sizeof(double) : 0);
    threshold_cached_ = false;
    cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
    use_batch_merge_ = !is_async_nested_scope(scope_)
        && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
        && children_cnt_ > 0
        && children_cnt_ <= BATCH_BUFFER_BYTE_BUDGET / child_batch_bytes;
    // Select the mode before binding the partition. DOCID_RANGE uses the same
    // batch merge: binding clamps the lower bound and draining clamps the upper.
    if (OB_FAIL(reset_children_for_partition(true))) {
      LOG_WARN("failed to open async segment merge", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(record_mode_metric())) {
    LOG_WARN("failed to record async segment merge mode metric", K(ret));
  } else if (ObDASAsyncSegmentMergeMode::SORT_MERGE == mode_) {
    threshold_cached_ = false;
    cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
  }
  if (OB_SUCC(ret) && OB_FAIL(reset_children_for_partition(false))) {
    LOG_WARN("failed to rescan async segment merge", K(ret));
  } else {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_rescan_segment(
    const ObNewVersionRange &range)
{
  is_first_nk_ = true;
  UNUSED(range);
  // SegmentMerge is a hard logical-index boundary; outer operators must not propagate
  // another per-Segment rescan into it.
  return OB_SUCCESS;
}

int ObDASAsyncSegmentMergeOp::do_close()
{
  int ret = OB_SUCCESS;
  segment_ranges_.reset();
  reset_merge_state();
  head_rowids_.reset();
  head_scores_.reset();
  child_ended_.reset();
  child_opened_.reset();
  pending_child_indices_.reset();
  nested_key_heads_.reset();
  curr_nk_.set_min();
  nested_heap_child_indices_.reset();
  nested_pending_child_indices_.reset();
  batch_allocator_.reset();
  batch_ids_ = nullptr;
  batch_scores_ = nullptr;
  batch_score_suffix_maxes_ = nullptr;
  batch_child_capacity_ = 0;
  batch_cursors_.reset();
  direct_child_next_rowids_.reset();
  direct_active_child_indices_.reset();
  rowid_output_exprs_.reset();
  target_allocator_.reset();
  force_merge_ = false;
  direct_drain_enabled_ = false;
  threshold_cached_ = false;
  cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
  use_batch_merge_ = false;
  is_first_nk_ = true;
  return ret;
}

int ObDASAsyncSegmentMergeOp::init_nested_key_cursor()
{
  int ret = OB_SUCCESS;
  curr_nk_.set_min();
  if (current_doc_has_single_nk_source()) {
    // The document phase has already selected the sole source segment.
    is_first_nk_ = false;
  } else {
    nested_heap_child_indices_.reuse();
    nested_pending_child_indices_.reuse();
    for (int64_t i = 0; i < children_cnt_; ++i) {
      nested_key_heads_.at(i).set_max();
    }
    for (int64_t i = 0; i < pending_child_indices_.count(); ++i) {
      nested_key_heads_.at(pending_child_indices_.at(i)).set_min();
    }
    // The first NK request reads all document contributors; later requests only
    // advance the contributors of the last deduplicated NK.
    if (OB_FAIL(nested_pending_child_indices_.assign(pending_child_indices_))) {
      LOG_WARN("failed to initialize segment nested contributors", K(ret));
    } else {
      is_first_nk_ = false;
    }
  }
  return ret;
}

// Call next_rowid for every pending child; batch DocIDs use the buffered next record.
template <typename RowType>
int ObDASAsyncSegmentMergeOp::advance_pending_children(
    MergeCursor<RowType> &cursor, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cursor.pending_.count(); ++i) {
    const int64_t child_idx = cursor.pending_.at(i);
    RowType &head = cursor.heads_.at(child_idx);
    if (OB_FAIL(fetch_next_head(child_idx, capacity, head))) {
      LOG_WARN("failed to advance consumed async segment head", K(ret), K(child_idx));
    } else if (!head.is_max() && cursor.use_heap_
               && OB_FAIL(heap_push(cursor, child_idx))) {
      LOG_WARN("failed to restore async segment child to heap", K(ret), K(child_idx));
    }
  }
  cursor.pending_.reuse();
  return ret;
}

int ObDASAsyncSegmentMergeOp::advance_pending_children(const int64_t capacity)
{
  MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
  return advance_pending_children(cursor, capacity);
}

int ObDASAsyncSegmentMergeOp::advance_heads_to(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    int cmp = 0;
    if (child_ended_.at(i)) {
      // skip exhausted child
    } else if (OB_FAIL(compare_rowid(head_rowids_.at(i), target, cmp))) {
      LOG_WARN("failed to compare async segment head with target", K(ret), K(i));
    } else if (cmp < 0) {
      if (OB_FAIL(seek_merge_head(i, target, head_rowids_.at(i)))) {
        LOG_WARN("failed to advance async segment child to target", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(build_heap())) {
    LOG_WARN("failed to rebuild async segment merge heap", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::try_advance_single_live(
    const ObDASRowID &target,
    ObDASRowID &rowid,
    double &score,
    bool &handled)
{
  int ret = OB_SUCCESS;
  handled = false;
  if (!force_merge_
      && (use_batch_merge_ || is_async_nested_scope(scope_))
      && (segment_domain_.group_slot_ >= 0 || is_async_nested_scope(scope_))
      && heap_child_indices_.empty()
      && 1 == pending_child_indices_.count()) {
    const int64_t child_idx = pending_child_indices_.at(0);
    handled = true;
    if (OB_UNLIKELY(child_idx < 0 || child_idx >= active_segment_cnt_ || child_ended_.at(child_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid single live async segment owner",
               K(ret), K(child_idx), K_(active_segment_cnt));
    } else {
      int cmp = 0;
      if (OB_FAIL(compare_rowid(head_rowids_.at(child_idx), target, cmp))) {
        LOG_WARN("failed to compare single live async segment head",
                 K(ret), K(child_idx), K(target));
      } else if (cmp < 0) {
        // Keep NK position for a same-document seek; reload only after moving.
        is_first_nk_ = true;
        if (OB_FAIL(seek_merge_head(child_idx, target, head_rowids_.at(child_idx)))) {
          LOG_WARN("failed to advance single live async segment child",
                   K(ret), K(child_idx), K(target));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (child_ended_.at(child_idx)) {
        iter_end_ = true;
        ret = OB_ITER_END;
      } else if (search_ctx_.has_docid_range()
                 && !search_ctx_.get_docid_range_hi().is_max_value()
                 && head_rowids_.at(child_idx).get_uint64()
                    > search_ctx_.get_docid_range_hi().get_uint64()) {
        iter_end_ = true;
        ret = OB_ITER_END;
      } else {
        rowid = head_rowids_.at(child_idx);
        score = head_scores_.at(child_idx);
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::fetch_next_head(
    const int64_t child_idx, const int64_t capacity, ObDASRowID &head)
{
  UNUSED(head);
  return fetch_next_head(child_idx, capacity);
}

int ObDASAsyncSegmentMergeOp::fetch_next_head(
    const int64_t child_idx, const int64_t capacity, ObDASNestedKey &head)
{
  UNUSED(capacity);
  double score = 0.0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(children_[child_idx]->next_rowid(head, score))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      head.set_max();
    } else {
      LOG_WARN("failed to fetch segment nested key", K(ret), K(child_idx));
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::seek_merge_head(
    const int64_t child_idx, const ObDASRowID &target, ObDASRowID &head)
{
  int ret = OB_SUCCESS;
  if (use_batch_merge_) {
    ret = advance_batch_head_to(child_idx, target);
  } else {
    if (OB_FAIL(children_[child_idx]->advance_to(target, head, head_scores_.at(child_idx)))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        child_ended_.at(child_idx) = true;
        head.set_max();
        head_scores_.at(child_idx) = 0.0;
      } else {
        LOG_WARN("failed to advance async segment child to target", K(ret), K(child_idx));
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::seek_merge_head(
    const int64_t child_idx, const ObDASNestedKey &target, ObDASNestedKey &head)
{
  double score = 0.0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(children_[child_idx]->advance_to(target, head, score))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      head.set_max();
    } else {
      LOG_WARN("failed to advance segment nested key", K(ret), K(child_idx));
    }
  }
  return ret;
}

bool ObDASAsyncSegmentMergeOp::merge_head_in_range(const ObDASRowID &head) const
{
  return !search_ctx_.has_docid_range()
      || search_ctx_.get_docid_range_hi().is_max_value()
      || head.get_uint64() <= search_ctx_.get_docid_range_hi().get_uint64();
}

// Ensure all child heads reach target by calling advance_to only for lagging children.
template <typename RowType>
int ObDASAsyncSegmentMergeOp::advance_merge_heads(
    MergeCursor<RowType> &cursor, const RowType &target)
{
  int ret = OB_SUCCESS;
  // The target supplies the comparison length for a short NK seek prefix.
  if (!cursor.use_heap_) {
    RowType &head = cursor.heads_.at(0);
    int cmp = 0;
    if (head.is_max()) {
      // The sole child is exhausted.
    } else if (OB_FAIL(compare_rowid(target, head, cmp))) {
      LOG_WARN("failed to compare single async segment head with target", K(ret));
    } else if (!head.is_min() && cmp <= 0) {
      // Keep the sole child at its qualifying head.
    } else if (OB_FAIL(seek_merge_head(0, target, head))) {
      LOG_WARN("failed to advance single async segment head", K(ret));
    }
  } else {
    bool reached = false;
    while (OB_SUCC(ret) && !reached && !cursor.heap_.empty()) {
      int64_t child_idx = cursor.heap_.at(0);
      RowType &head = cursor.heads_.at(child_idx);
      int cmp = 0;
      if (head.is_max()) {
        reached = true;
      } else if (OB_FAIL(compare_rowid(target, head, cmp))) {
        LOG_WARN("failed to compare async segment heap root with target", K(ret), K(child_idx));
      } else if (!head.is_min() && cmp <= 0) {
        reached = true;
      } else if (OB_FAIL(heap_pop(cursor, child_idx))) {
        LOG_WARN("failed to pop lagging async segment heap root", K(ret), K(child_idx));
      } else if (OB_FAIL(seek_merge_head(child_idx, target, head))) {
        LOG_WARN("failed to advance lagging async segment head", K(ret), K(child_idx));
      } else if (!head.is_max() && OB_FAIL(heap_push(cursor, child_idx))) {
        LOG_WARN("failed to restore advanced async segment heap root", K(ret), K(child_idx));
      }
    }
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::merge_next(
    MergeCursor<RowType> &cursor, const int64_t capacity,
    RowType &result, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(advance_pending_children(cursor, capacity))) {
    LOG_WARN("failed to advance pending async segment children", K(ret));
  } else {
    ret = emit_min_head(cursor, result, score);
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::merge_advance(
    MergeCursor<RowType> &cursor, const RowType &target,
    RowType &result, double &score)
{
  int ret = OB_SUCCESS;
  // Restore the last output's contributors without moving qualifying child cursors.
  for (int64_t i = 0; OB_SUCC(ret) && i < cursor.pending_.count(); ++i) {
    const int64_t child_idx = cursor.pending_.at(i);
    if (cursor.use_heap_ && OB_FAIL(heap_push(cursor, child_idx))) {
      LOG_WARN("failed to restore pending async segment head", K(ret), K(child_idx));
    }
  }
  cursor.pending_.reuse();
  if (OB_FAIL(ret)) {
    // pass
  } else if (OB_FAIL(advance_merge_heads(cursor, target))) {
    LOG_WARN("failed to advance async segment merge heads", K(ret));
  } else {
    ret = emit_min_head(cursor, result, score);
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::heap_less(
    const MergeCursor<RowType> &cursor,
    const int64_t left_child_idx,
    const int64_t right_child_idx,
    bool &less) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  less = false;
  if (OB_UNLIKELY(left_child_idx < 0 || left_child_idx >= active_segment_cnt_
                  || right_child_idx < 0 || right_child_idx >= active_segment_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment heap child index", K(ret),
             K(left_child_idx), K(right_child_idx), K_(active_segment_cnt));
  } else if (OB_FAIL(compare_rowid(
                 cursor.heads_.at(left_child_idx), cursor.heads_.at(right_child_idx), cmp))) {
    LOG_WARN("failed to compare async segment heap heads", K(ret),
             K(left_child_idx), K(right_child_idx));
  } else {
    less = cmp < 0;
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::heap_push(MergeCursor<RowType> &cursor, const int64_t child_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = cursor.heap_.count();
  if (OB_UNLIKELY(child_idx < 0 || child_idx >= active_segment_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid child to push into async segment heap", K(ret), K(child_idx));
  } else if (OB_FAIL(cursor.heap_.push_back(child_idx))) {
    LOG_WARN("failed to append async segment heap child", K(ret), K(child_idx));
  }
  while (OB_SUCC(ret) && pos > 0) {
    const int64_t parent_pos = (pos - 1) / 2;
    bool less = false;
    if (OB_FAIL(heap_less(cursor, cursor.heap_.at(pos),
                          cursor.heap_.at(parent_pos), less))) {
      LOG_WARN("failed to compare async segment heap nodes", K(ret), K(pos), K(parent_pos));
    } else if (!less) {
      break;
    } else {
      const int64_t tmp = cursor.heap_.at(pos);
      cursor.heap_.at(pos) = cursor.heap_.at(parent_pos);
      cursor.heap_.at(parent_pos) = tmp;
      pos = parent_pos;
    }
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::heap_sift_down(
    MergeCursor<RowType> &cursor, const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  int64_t pos = start_pos;
  if (OB_UNLIKELY(start_pos < 0 || start_pos >= cursor.heap_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async segment heap position", K(ret), K(start_pos));
  }
  while (OB_SUCC(ret) && 2 * pos + 1 < cursor.heap_.count()) {
    int64_t min_pos = 2 * pos + 1;
    const int64_t right_pos = min_pos + 1;
    bool less = false;
    if (right_pos < cursor.heap_.count()
        && OB_FAIL(heap_less(cursor, cursor.heap_.at(right_pos),
                                  cursor.heap_.at(min_pos), less))) {
      LOG_WARN("failed to compare async segment heap children", K(ret), K(pos));
    } else if (right_pos < cursor.heap_.count() && less) {
      min_pos = right_pos;
    }
    if (OB_SUCC(ret)
        && OB_FAIL(heap_less(cursor, cursor.heap_.at(min_pos),
                                  cursor.heap_.at(pos), less))) {
      LOG_WARN("failed to compare async segment heap parent", K(ret), K(pos), K(min_pos));
    } else if (OB_SUCC(ret) && !less) {
      break;
    } else if (OB_SUCC(ret)) {
      const int64_t tmp = cursor.heap_.at(pos);
      cursor.heap_.at(pos) = cursor.heap_.at(min_pos);
      cursor.heap_.at(min_pos) = tmp;
      pos = min_pos;
    }
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::heap_pop(
    MergeCursor<RowType> &cursor, int64_t &child_idx)
{
  int ret = OB_SUCCESS;
  child_idx = -1;
  if (cursor.heap_.empty()) {
    ret = OB_ITER_END;
  } else {
    child_idx = cursor.heap_.at(0);
    cursor.heap_.at(0) = cursor.heap_.at(cursor.heap_.count() - 1);
    cursor.heap_.pop_back();
    if (!cursor.heap_.empty() && OB_FAIL(heap_sift_down(cursor, 0))) {
      LOG_WARN("failed to sift down async segment heap", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::heap_push(const int64_t child_idx)
{
  MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
  return heap_push(cursor, child_idx);
}

int ObDASAsyncSegmentMergeOp::heap_pop(int64_t &child_idx)
{
  MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
  return heap_pop(cursor, child_idx);
}

int ObDASAsyncSegmentMergeOp::build_heap()
{
  int ret = OB_SUCCESS;
  heap_child_indices_.reset();
  if (!use_single_child_fast_path()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
      if (!child_ended_.at(i) && OB_FAIL(heap_push(i))) {
        LOG_WARN("failed to build async segment heap", K(ret), K(i));
      }
    }
  }
  return ret;
}

template <typename RowType>
int ObDASAsyncSegmentMergeOp::emit_min_head(
    MergeCursor<RowType> &cursor, RowType &result, double &score)
{
  int ret = OB_SUCCESS;
  int64_t min_idx = cursor.use_heap_ && !cursor.heap_.empty() ? cursor.heap_.at(0) : 0;
  cursor.pending_.reuse();
  if ((cursor.use_heap_ && cursor.heap_.empty()) ||
      cursor.heads_.at(min_idx).is_max() ||
      !merge_head_in_range(cursor.heads_.at(min_idx))) {
    ret = OB_ITER_END;
  } else if (cursor.use_heap_ && OB_FAIL(heap_pop(cursor, min_idx))) {
    LOG_WARN("failed to pop minimum async segment head", K(ret));
  } else if (OB_FAIL(cursor.pending_.push_back(min_idx))) {
    LOG_WARN("failed to save pending async segment child", K(ret), K(min_idx));
  } else {
    result = cursor.heads_.at(min_idx);
    score = merge_head_score(min_idx, result);
    bool duplicate = true;
    while (OB_SUCC(ret) && duplicate && cursor.use_heap_ && !cursor.heap_.empty()) {
      int cmp = 0;
      const int64_t child_idx = cursor.heap_.at(0);
      if (OB_FAIL(compare_rowid(result, cursor.heads_.at(child_idx), cmp))) {
        LOG_WARN("failed to compare duplicate async segment head", K(ret), K(child_idx));
      } else if (0 != cmp) {
        duplicate = false;
      } else if (OB_FAIL(heap_pop(cursor, min_idx))) {
        LOG_WARN("failed to pop duplicate async segment head", K(ret), K(child_idx));
      } else if (OB_FAIL(cursor.pending_.push_back(min_idx))) {
        LOG_WARN("failed to save duplicate async segment child", K(ret), K(min_idx));
      } else {
        score = MAX(score, merge_head_score(min_idx, result));
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::emit_min_head(ObDASRowID &rowid, double &score)
{
  MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
  int ret = OB_SUCCESS;
  if (OB_FAIL(emit_min_head(cursor, rowid, score))) {
    if (OB_ITER_END == ret) {
      iter_end_ = true;
    } else {
      LOG_WARN("failed to emit async segment head", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::next_rowid_with_capacity(
    const int64_t capacity,
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (in_shallow_status_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("next rowid after async segment shallow advance is not supported", K(ret));
  } else if (use_ordered_concat_) {
    ret = next_ordered_concat(capacity, next_id, score);
  } else {
    MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
    if (OB_FAIL(merge_next(cursor, capacity, next_id, score))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to emit next async segment row", K(ret));
      } else {
        iter_end_ = true;
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  is_first_nk_ = true;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ordered next rowid is unsupported by direct async segment merge",
        K(ret), K_(op_id));
  } else {
    const int64_t fetch_capacity =
        use_batch_merge_ && segment_domain_.group_slot_ >= 0 ? CHILD_BATCH_SIZE : 1;
    ret = next_rowid_with_capacity(fetch_capacity, next_id, score);
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_next_rowid(ObDASNestedKey &next_nk, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!is_async_nested_scope(scope_)
      || ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize segment nested key cursor", KR(ret));
  } else if (curr_nk_.is_max()) {
    ret = OB_ITER_END;
  } else if (current_doc_has_single_nk_source()) {
    const int64_t child_idx = use_ordered_concat_
        ? current_ordered_child_idx() : pending_child_indices_.at(0);
    if (OB_FAIL(children_[child_idx]->next_rowid(next_nk, score))) {
      if (OB_ITER_END == ret) {
        curr_nk_.set_max();
      } else {
        LOG_WARN("failed to fetch next async segment nested key", K(ret));
      }
    }
  } else {
    MergeCursor<ObDASNestedKey> cursor = nested_merge_cursor();
    if (OB_FAIL(merge_next(cursor, 1, next_nk, score))) {
      if (OB_ITER_END == ret) {
        curr_nk_.set_max();
      } else {
        LOG_WARN("failed to fetch next async segment nested key", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_nk_ = next_nk;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::get_next_doc_ids(
    const int64_t capacity,
    const uint64_t *&doc_ids,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  count = 0;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE != mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("raw doc ids are unsupported by sort async segment merge", K(ret), K_(op_id));
  }
  while (OB_SUCC(ret) && current_batch_child_idx_ < active_segment_cnt_) {
    ObIDASSearchOp *child = children_[current_batch_child_idx_];
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null direct async segment raw child", K(ret),
          K_(current_batch_child_idx), K_(active_segment_cnt));
    } else if (DAS_SEARCH_OP_ASYNC_SCALAR_RANGE != child->get_op_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct async segment child has no raw doc ids", K(ret),
          K_(current_batch_child_idx), "child_type", child->get_op_type());
    } else {
      ret = static_cast<ObDASAsyncScalarRangeOp *>(child)->get_next_doc_ids(
          capacity, doc_ids, count);
      if (OB_ITER_END == ret) {
        ++current_batch_child_idx_;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        count = 0;
      }
    }
    if (OB_SUCC(ret) && count > 0) {
      break;
    }
  }
  if (OB_SUCC(ret) && 0 == count && current_batch_child_idx_ >= active_segment_cnt_) {
    ret = OB_ITER_END;
  }
  INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  return ret;
}

int ObDASAsyncSegmentMergeOp::drain_direct_child(
    const int64_t child_idx,
    const uint64_t range_start,
    const uint64_t range_end,
    ObDASRowIDBatchConsumer &consumer,
    ObDASRowID &child_next_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(child_idx < 0 || child_idx >= active_segment_cnt_
                  || child_idx >= direct_child_next_rowids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid direct-drain async segment child index", K(ret), K(child_idx),
        K_(active_segment_cnt), K(direct_child_next_rowids_.count()));
  } else if (OB_ISNULL(children_[child_idx])
             || OB_UNLIKELY(!children_[child_idx]->supports_direct_rowid_drain())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid direct-drain async segment child", K(ret), K(child_idx),
        KP(children_[child_idx]));
  } else {
    ObDASRowID &cursor = direct_child_next_rowids_.at(child_idx);
    if (cursor.is_max()) {
      child_next_id.set_max();
    } else if (cursor.is_normal() && cursor.get_uint64() > range_end) {
      child_next_id = cursor;
    } else if (OB_FAIL(children_[child_idx]->direct_drain_rowids(
                   range_start, range_end, consumer, child_next_id))) {
      LOG_WARN("failed to drain async segment child", K(ret), K(child_idx),
          K(range_start), K(range_end));
    } else if (OB_UNLIKELY(!child_next_id.is_max()
                           && (!child_next_id.is_normal()
                               || child_next_id.get_uint64() <= range_end))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async segment direct-drain lookahead", K(ret), K(child_idx),
          K(range_end), K(child_next_id));
    } else {
      cursor = child_next_id;
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_direct_drain_rowids(
    const uint64_t range_start,
    const uint64_t range_end,
    ObDASRowIDBatchConsumer &consumer,
    ObDASRowID &next_id_after)
{
  int ret = OB_SUCCESS;
  next_id_after.set_max();
  if (OB_UNLIKELY(!direct_drain_enabled_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct rowid drain is unsupported by async segment merge", K(ret), K_(op_id));
  } else if (OB_UNLIKELY(direct_child_next_rowids_.count() < active_segment_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct segment cursors are not initialized", K(ret),
        K_(active_segment_cnt), K(direct_child_next_rowids_.count()));
  } else if (1 == direct_active_child_indices_.count()) {
    const int64_t child_idx = direct_active_child_indices_.at(0);
    if (OB_FAIL(drain_direct_child(
            child_idx, range_start, range_end, consumer, next_id_after))) {
      LOG_WARN("failed to drain sole live async segment child", K(ret), K(child_idx));
    } else if (next_id_after.is_max()) {
      direct_active_child_indices_.pop_back();
    }
  } else {
    int64_t active_idx = 0;
    while (OB_SUCC(ret) && active_idx < direct_active_child_indices_.count()) {
      const int64_t child_idx = direct_active_child_indices_.at(active_idx);
      ObDASRowID child_next_id;
      if (OB_FAIL(drain_direct_child(
              child_idx, range_start, range_end, consumer, child_next_id))) {
        LOG_WARN("failed to schedule direct async segment child", K(ret), K(child_idx));
      } else if (child_next_id.is_max()) {
        // Callback order is unrestricted. Revisit this slot after moving the
        // unvisited last child into it so removal neither shifts nor skips children.
        direct_active_child_indices_.at(active_idx) =
            direct_active_child_indices_.at(direct_active_child_indices_.count() - 1);
        direct_active_child_indices_.pop_back();
      } else {
        if (next_id_after.is_max()
            || child_next_id.get_uint64() < next_id_after.get_uint64()) {
          next_id_after = child_next_id;
        }
        ++active_idx;
      }
    }
  }
  return ret;
}

bool ObDASAsyncSegmentMergeOp::can_forward_raw_doc_ids() const
{
  bool can_forward = ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_
      && active_segment_cnt_ > 0;
  for (int64_t i = 0; can_forward && i < active_segment_cnt_; ++i) {
    can_forward = OB_NOT_NULL(children_[i])
        && DAS_SEARCH_OP_ASYNC_SCALAR_RANGE == children_[i]->get_op_type()
        && DAS_SCOPE_BASE == children_[i]->get_scope();
  }
  return can_forward;
}

int ObDASAsyncSegmentMergeOp::do_next_rowids(int64_t capacity, int64_t &count)
{
  is_first_nk_ = true;
  int ret = OB_SUCCESS;
  count = 0;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE != mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("batch rowids are unsupported by sort async segment merge", K(ret), K_(op_id));
  }
  while (OB_SUCC(ret) && current_batch_child_idx_ < active_segment_cnt_) {
    ObIDASSearchOp *child = children_[current_batch_child_idx_];
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null direct async segment batch child", K(ret),
          K_(current_batch_child_idx), K_(active_segment_cnt));
    } else {
      ret = child->next_rowids(capacity, count);
      if (OB_ITER_END == ret) {
        ++current_batch_child_idx_;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        count = 0;
      } else if (0 == count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("direct async segment child returned an empty successful batch", K(ret),
            K_(current_batch_child_idx), K(capacity));
      }
    }
    if (OB_SUCC(ret) && count > 0) {
      break;
    }
  }
  if (OB_SUCC(ret) && 0 == count && current_batch_child_idx_ >= active_segment_cnt_) {
    ret = OB_ITER_END;
  }
  INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  return ret;
}

int ObDASAsyncSegmentMergeOp::drain_bounded_single_child_batch(
    const ObDASRowID &boundary,
    const int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  const int64_t child_idx = 0;
  const int64_t fetch_capacity = MIN(CHILD_BATCH_SIZE, capacity);
  const bool use_heap = !use_single_child_fast_path();
  const bool has_range_hi = search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value();
  const uint64_t request_boundary = boundary.get_uint64();
  const uint64_t effective_boundary = has_range_hi ? MIN(request_boundary, search_ctx_.get_docid_range_hi().get_uint64()) : request_boundary;
  const bool range_limited = has_range_hi && search_ctx_.get_docid_range_hi().get_uint64() <= request_boundary;
  BatchCursor &cursor = batch_cursors_.at(child_idx);
  uint64_t *child_ids = batch_ids_ + child_idx * CHILD_BATCH_SIZE;
  double *child_scores = is_scoring_ ? batch_scores_ + child_idx * CHILD_BATCH_SIZE : nullptr;
  bool done = false;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (OB_FAIL(advance_pending_children(fetch_capacity))) {
    LOG_WARN("failed to advance bounded single segment head", K(ret));
  } else if (child_ended_.at(child_idx)) {
    iter_end_ = true;
    next_id_after.set_max();
    done = true;
  } else if (use_heap) {
    int64_t popped_child_idx = -1;
    if (OB_FAIL(heap_pop(popped_child_idx))) {
      LOG_WARN("failed to detach bounded single segment heap owner", K(ret));
    } else if (OB_UNLIKELY(child_idx != popped_child_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected bounded single segment heap owner", K(ret), K(child_idx), K(popped_child_idx));
    }
  }
  if (OB_SUCC(ret) && !done) {
    if (head_rowids_.at(child_idx).get_uint64() > effective_boundary) {
      if (range_limited) {
        cursor.position_ = cursor.count_;
        cursor.source_exhausted_ = true;
        child_ended_.at(child_idx) = true;
        head_rowids_.at(child_idx).set_max();
        iter_end_ = true;
        next_id_after.set_max();
      } else if (OB_FAIL(pending_child_indices_.push_back(child_idx))) {
        LOG_WARN("failed to save bounded single segment lookahead", K(ret));
      } else {
        next_id_after = head_rowids_.at(child_idx);
        next_score_after = head_scores_.at(child_idx);
      }
      done = true;
    } else {
      out_ids[count] = head_rowids_.at(child_idx).get_uint64();
      if (OB_NOT_NULL(out_scores)) {
        out_scores[count] = is_scoring_ ? head_scores_.at(child_idx) : 0.0;
      }
      ++count;
    }
  }
  while (OB_SUCC(ret) && !done && count < capacity) {
    const int16_t run_position = cursor.position_;
    const uint64_t *run_begin = child_ids + cursor.position_;
    const uint64_t *run_limit = child_ids + cursor.count_;
    const uint64_t *run_end = std::upper_bound(run_begin, run_limit, effective_boundary);
    const int64_t copy_count = MIN(static_cast<int64_t>(run_end - run_begin), capacity - count);
    if (copy_count > 0) {
      MEMCPY(out_ids + count, run_begin, copy_count * sizeof(uint64_t));
      if (OB_NOT_NULL(out_scores)) {
        if (is_scoring_) {
          MEMCPY(out_scores + count, child_scores + run_position, copy_count * sizeof(double));
        } else {
          std::fill_n(out_scores + count, copy_count, 0.0);
        }
      }
      cursor.position_ = static_cast<int16_t>(cursor.position_ + copy_count);
      count += copy_count;
      head_rowids_.at(child_idx).set_uint64(out_ids[count - 1]);
      head_scores_.at(child_idx) = is_scoring_ ? child_scores[cursor.position_ - 1] : 0.0;
    }
    if (count == capacity) {
      if (OB_FAIL(pending_child_indices_.push_back(child_idx))) {
        LOG_WARN("failed to save bounded single segment pending head", K(ret));
      } else {
        done = true;
      }
    } else if (cursor.position_ < cursor.count_) {
      if (range_limited) {
        cursor.position_ = cursor.count_;
        cursor.source_exhausted_ = true;
        child_ended_.at(child_idx) = true;
        head_rowids_.at(child_idx).set_max();
        iter_end_ = true;
        next_id_after.set_max();
      } else {
        const int16_t position = cursor.position_++;
        head_rowids_.at(child_idx).set_uint64(child_ids[position]);
        head_scores_.at(child_idx) = is_scoring_ ? child_scores[position] : 0.0;
        if (OB_FAIL(pending_child_indices_.push_back(child_idx))) {
          LOG_WARN("failed to save bounded single segment lookahead", K(ret));
        } else {
          next_id_after = head_rowids_.at(child_idx);
          next_score_after = head_scores_.at(child_idx);
        }
      }
      done = true;
    } else if (cursor.source_exhausted_) {
      child_ended_.at(child_idx) = true;
      head_rowids_.at(child_idx).set_max();
      iter_end_ = true;
      next_id_after.set_max();
      done = true;
    } else if (OB_FAIL(refill_batch_child(child_idx, fetch_capacity))) {
      LOG_WARN("failed to refill bounded single segment batch", K(ret));
    }
  }
  if (OB_SUCC(ret) && !done && count == capacity && OB_FAIL(pending_child_indices_.push_back(child_idx))) {
    LOG_WARN("failed to save full bounded single segment batch", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_drain_until(
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
  const int64_t fetch_capacity = MIN(CHILD_BATCH_SIZE, MAX(1L, capacity));
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ordered drain is unsupported by direct async segment merge", K(ret), K_(op_id));
  } else if (iter_end_) {
    next_id_after.set_max();
  } else if (in_shallow_status_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drain after async segment shallow advance is not supported", K(ret));
  } else if (use_ordered_concat_) {
    ret = drain_ordered_concat(boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after);
  } else if (!boundary.is_max()) {
    if (use_batch_merge_ && 1 == active_segment_cnt_) {
      if (OB_FAIL(drain_bounded_single_child_batch(
          boundary, capacity, out_ids, out_scores, count,
          next_id_after, next_score_after))) {
        LOG_WARN("failed to drain bounded single async segment batch", K(ret));
      }
    } else {
      bool done = false;
      const uint64_t boundary_value = boundary.get_uint64();
      while (OB_SUCC(ret) && count < capacity && !done) {
        ObDASRowID rowid;
        double score = 0.0;
        if (OB_FAIL(next_rowid_with_capacity(fetch_capacity, rowid, score))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            next_id_after.set_max();
            done = true;
          } else {
            LOG_WARN("failed to get next buffered async segment row", K(ret));
          }
        } else if (rowid.get_uint64() > boundary_value) {
          next_id_after = rowid;
          next_score_after = score;
          done = true;
        } else {
          out_ids[count] = rowid.get_uint64();
          if (OB_NOT_NULL(out_scores)) {
            out_scores[count] = score;
          }
          ++count;
        }
      }
    }
  } else if (use_batch_merge_) {
    bool done = false;
    const bool use_single_child_batch_fast_path = use_single_child_fast_path();
    const bool has_range_hi = search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value();
    const uint64_t range_hi = has_range_hi
        ? search_ctx_.get_docid_range_hi().get_uint64() : 0;
    while (OB_SUCC(ret) && count < capacity && !done) {
      if (OB_FAIL(advance_pending_children(fetch_capacity))) {
        LOG_WARN("failed to advance batch segment children", K(ret));
      } else if ((use_single_child_batch_fast_path && child_ended_.at(0))
                 || (!use_single_child_batch_fast_path && heap_child_indices_.empty())) {
        iter_end_ = true;
        next_id_after.set_max();
        done = true;
      } else {
        const int64_t min_child_idx = use_single_child_batch_fast_path
            ? 0 : heap_child_indices_.at(0);
        const uint64_t min_id = head_rowids_.at(min_child_idx).get_uint64();
        bool has_competitor = false;
        uint64_t competitor_id = 0;
        if (!use_single_child_batch_fast_path && heap_child_indices_.count() > 1) {
          competitor_id = head_rowids_.at(heap_child_indices_.at(1)).get_uint64();
          has_competitor = true;
        }
        if (!use_single_child_batch_fast_path && heap_child_indices_.count() > 2) {
          const uint64_t right_id =
              head_rowids_.at(heap_child_indices_.at(2)).get_uint64();
          competitor_id = has_competitor ? MIN(competitor_id, right_id) : right_id;
          has_competitor = true;
        }
        if (has_range_hi && min_id > range_hi) {
          iter_end_ = true;
          next_id_after.set_max();
          done = true;
        } else if (OB_UNLIKELY(has_competitor && competitor_id < min_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async segment merge heap order", K(ret),
              K(min_child_idx), K(min_id), K(competitor_id));
        } else if (has_competitor && competitor_id == min_id) {
          ObDASRowID rowid;
          double score = 0.0;
          if (OB_FAIL(emit_min_head(rowid, score))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to emit duplicate batch segment head", K(ret));
            }
          } else {
            out_ids[count] = rowid.get_uint64();
            if (nullptr != out_scores) {
              out_scores[count] = score;
            }
            ++count;
          }
        } else {
          int64_t popped_child_idx = min_child_idx;
          if (!use_single_child_batch_fast_path && OB_FAIL(heap_pop(popped_child_idx))) {
            LOG_WARN("failed to pop batch segment run", K(ret), K(min_child_idx));
          } else if (OB_UNLIKELY(popped_child_idx != min_child_idx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected batch segment heap root", K(ret),
                K(popped_child_idx), K(min_child_idx));
          } else if (OB_FAIL(pending_child_indices_.push_back(min_child_idx))) {
            LOG_WARN("failed to save batch segment run child", K(ret), K(min_child_idx));
          } else {
            BatchCursor &cursor = batch_cursors_.at(min_child_idx);
            uint64_t *child_ids = batch_ids_ + min_child_idx * CHILD_BATCH_SIZE;
            double *child_scores = is_scoring_
                ? batch_scores_ + min_child_idx * CHILD_BATCH_SIZE : nullptr;
            const int16_t run_position = cursor.position_;
            const uint64_t *run_begin = child_ids + cursor.position_;
            const uint64_t *run_end = child_ids + cursor.count_;
            if (has_competitor) {
              run_end = std::lower_bound(run_begin, run_end, competitor_id);
            }
            if (has_range_hi) {
              run_end = std::upper_bound(run_begin, run_end, range_hi);
            }
            out_ids[count] = min_id;
            if (nullptr != out_scores) {
              out_scores[count] = head_scores_.at(min_child_idx);
            }
            ++count;
            const int64_t run_count = static_cast<int64_t>(run_end - run_begin);
            const int64_t copy_count = MIN(run_count, capacity - count);
            if (copy_count > 0) {
              MEMCPY(out_ids + count, run_begin, copy_count * sizeof(uint64_t));
              if (nullptr != out_scores) {
                if (is_scoring_) {
                  MEMCPY(out_scores + count, child_scores + run_position,
                      copy_count * sizeof(double));
                } else {
                  std::fill_n(out_scores + count, copy_count, 0.0);
                }
              }
              cursor.position_ = static_cast<int16_t>(cursor.position_ + copy_count);
              count += copy_count;
            }
            head_rowids_.at(min_child_idx).set_uint64(out_ids[count - 1]);
            if (is_scoring_ && copy_count > 0) {
              head_scores_.at(min_child_idx) = child_scores[cursor.position_ - 1];
            } else if (!is_scoring_) {
              head_scores_.at(min_child_idx) = 0.0;
            }
          }
        }
      }
    }
  } else {
    // The unbounded fast path above copies directly from per-child batch
    // buffers. Fall back to row-at-a-time draining when those buffers were
    // intentionally disabled by the memory budget.
    ret = ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after);
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::materialize_shallow_children(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  is_first_nk_ = true;
  ObDASRowID effective_target = target;
  bool inclusive = true;
  if ((use_batch_merge_ || use_ordered_concat_) && !shallow_inclusive_
      && (target.is_min()
          || (target.is_normal() && target.get_uint64() <= shallow_target_))) {
    effective_target.set_uint64(shallow_target_);
    inclusive = false;
  }

  if (!use_ordered_concat_) {
    pending_child_indices_.reuse();
    heap_child_indices_.reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    if (child_ended_.at(i)) {
      // skip exhausted child
    } else if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async segment child", K(ret), K(i));
    } else if (use_batch_merge_) {
      BatchCursor &cursor = batch_cursors_.at(i);
      if (cursor.source_in_shallow_status_) {
        ret = children_[i]->advance_to(
            effective_target, head_rowids_.at(i), head_scores_.at(i));
        cursor.position_ = 0;
        cursor.count_ = 0;
        cursor.source_in_shallow_status_ = false;
        if (OB_SUCC(ret) && !inclusive && head_rowids_.at(i).is_normal()
            && head_rowids_.at(i).get_uint64() == effective_target.get_uint64()) {
          ret = children_[i]->next_rowid(head_rowids_.at(i), head_scores_.at(i));
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          cursor.source_exhausted_ = true;
          child_ended_.at(i) = true;
          head_rowids_.at(i).set_max();
          head_scores_.at(i) = 0.0;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to materialize shallow async segment batch child",
              K(ret), K(i), K(effective_target), K(inclusive));
        } else {
          cursor.source_exhausted_ = false;
          child_ended_.at(i) = false;
          if (!is_scoring_) {
            head_scores_.at(i) = 0.0;
          }
        }
      } else if (OB_FAIL(advance_batch_head_to(i, effective_target, inclusive))) {
        LOG_WARN("failed to materialize buffered async segment child",
            K(ret), K(i), K(effective_target), K(inclusive));
      }
    } else {
      ret = children_[i]->advance_to(
          effective_target, head_rowids_.at(i), head_scores_.at(i));
      if (OB_SUCC(ret) && !inclusive && head_rowids_.at(i).is_normal()
          && head_rowids_.at(i).get_uint64() == effective_target.get_uint64()) {
        ret = children_[i]->next_rowid(head_rowids_.at(i), head_scores_.at(i));
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        child_ended_.at(i) = true;
        head_rowids_.at(i).set_max();
        head_scores_.at(i) = 0.0;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to materialize shallow async segment child",
            K(ret), K(i), K(effective_target));
      }
    }
  }
  if (OB_SUCC(ret) && !use_ordered_concat_ && OB_FAIL(build_heap())) {
    LOG_WARN("failed to rebuild async segment heap after shallow advance", K(ret));
  }
  if (OB_SUCC(ret)) {
    shallow_target_ = 0;
    concat_head_consumed_ = false;
    shallow_inclusive_ = true;
    in_shallow_status_ = false;
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID stable_target;
  bool keep_pending_head = false;
  bool single_live_handled = false;
  target_allocator_.reuse();
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ordered advance is unsupported by direct async segment merge",
        K(ret), K_(op_id));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(deep_copy_rowid(target, stable_target, target_allocator_))) {
    LOG_WARN("failed to preserve async segment advance target", K(ret));
  } else {
    if (search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value()
        && (stable_target.is_max()
            || (stable_target.is_normal()
                && stable_target.get_uint64()
                       > search_ctx_.get_docid_range_hi().get_uint64()))) {
      iter_end_ = true;
      ret = OB_ITER_END;
    } else if (search_ctx_.has_docid_range()
               && !search_ctx_.get_docid_range_lo().is_min_value()
               && (stable_target.is_min()
                   || (stable_target.is_normal()
                       && stable_target.get_uint64()
                              < search_ctx_.get_docid_range_lo().get_uint64()))) {
      stable_target.set_uint64(
          search_ctx_.get_docid_range_lo().get_uint64());
    } else {
      // The caller-provided target does not need adjustment.
    }
  }
  if (OB_SUCC(ret)) {
    if (use_ordered_concat_) {
      if (in_shallow_status_ && OB_FAIL(materialize_shallow_children(stable_target))) {
        LOG_WARN("failed to materialize concat children", K(ret));
      } else if (OB_FAIL(advance_ordered_concat(stable_target))) {
        LOG_WARN("failed to advance concat children", K(ret));
      } else {
        ret = emit_ordered_concat_head(curr_id, score);
      }
    } else if (in_shallow_status_) {
      if (OB_FAIL(materialize_shallow_children(stable_target))) {
        LOG_WARN("failed to materialize async segment children", K(ret), K(stable_target));
      } else if (OB_FAIL(emit_min_head(curr_id, score))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to emit shallow advanced async segment row", K(ret));
        }
      }
    } else if (OB_FAIL(try_advance_single_live(
               stable_target, curr_id, score, single_live_handled))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to directly advance single live async segment child", K(ret), K(stable_target));
    } else if (!single_live_handled) {
      if (!pending_child_indices_.empty()) {
        int cmp = 0;
        const int64_t pending_child_idx = pending_child_indices_.at(0);
        if (OB_FAIL(compare_rowid(
                head_rowids_.at(pending_child_idx), stable_target, cmp))) {
          LOG_WARN("failed to compare pending async segment head", K(ret), K(pending_child_idx), K(stable_target));
        } else {
          keep_pending_head = cmp >= 0;
        }
      }
      if (OB_SUCC(ret) && !keep_pending_head) {
        is_first_nk_ = true;
        if (OB_FAIL(advance_pending_children(1))) {
          LOG_WARN("failed to advance pending async segment children", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        MergeCursor<ObDASRowID> cursor = rowid_merge_cursor();
        if (OB_FAIL(merge_advance(cursor, stable_target, curr_id, score))) {
          if (OB_ITER_END == ret) {
            iter_end_ = true;
          } else {
            LOG_WARN("failed to advance async segment merge", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_advance_to(
    const ObDASNestedKey &target,
    ObDASNestedKey &curr_nk,
    double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  score = 0.0;
  if (!is_async_nested_scope(scope_)
      || ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize segment nested key cursor", KR(ret));
  } else if (curr_nk_.is_max() || target.is_max()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(compare_rowid(target, curr_nk_, cmp))) {
    LOG_WARN("failed to compare segment nested key target", KR(ret));
  } else if (curr_nk_.is_normal() && cmp <= 0) {
    curr_nk = curr_nk_;
  } else if (target.is_min() && curr_nk_.is_min()) {
    ret = do_next_rowid(curr_nk, score);
  } else {
    ObDASNestedKey stable_target = target;
    target_allocator_.reuse();
    if (target.is_normal() && target.length() > sizeof(uint64_t)) {
      char *target_buf = static_cast<char *>(target_allocator_.alloc(target.length()));
      if (OB_ISNULL(target_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to preserve segment nested seek target", K(ret), K(target));
      } else {
        // A child can reuse the bytes backing the caller's long NK target.
        MEMCPY(target_buf, target.ptr(), target.length());
        stable_target.assign(target_buf, target.length());
      }
    }
    if (OB_SUCC(ret)) {
      if (current_doc_has_single_nk_source()) {
        const int64_t child_idx = use_ordered_concat_
            ? current_ordered_child_idx() : pending_child_indices_.at(0);
        ret = children_[child_idx]->advance_to(stable_target, curr_nk, score);
      } else if (!force_merge_ && nested_heap_child_indices_.empty()
                 && 1 == nested_pending_child_indices_.count()) {
        // Multiple segments contributed the document, but only this NK source
        // remains. Keep it pending for the next NK call without rebuilding a heap.
        const int64_t child_idx = nested_pending_child_indices_.at(0);
        ObDASNestedKey &head = nested_key_heads_.at(child_idx);
        if (OB_FAIL(seek_merge_head(child_idx, stable_target, head))) {
          LOG_WARN("failed to advance sole live segment nested key", K(ret), K(child_idx));
        } else if (head.is_max()) {
          ret = OB_ITER_END;
        } else {
          curr_nk = head;
        }
      } else {
        MergeCursor<ObDASNestedKey> cursor = nested_merge_cursor();
        ret = merge_advance(cursor, stable_target, curr_nk, score);
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_nk_ = curr_nk;
  } else if (OB_ITER_END == ret) {
    curr_nk_.set_max();
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("shallow advance is unsupported by direct async segment merge", K(ret), K_(op_id));
  } else if (0 == active_segment_cnt_) {
    max_score_tuple = nullptr;
    iter_end_ = true;
    ret = OB_ITER_END;
  } else if (is_scoring_) {
    ObDASRowID min_id;
    ObDASRowID max_id;
    double max_score = 0.0;
    bool has_active_tuple = false;
    ObDASRowID effective_target = target;
    max_score_tuple = nullptr;
    min_id.set_max();
    max_id.set_max();
    if (iter_end_) {
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(!target.is_normal())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async segment shallow target", K(ret), K(target));
    } else if (search_ctx_.has_docid_range()
              && !search_ctx_.get_docid_range_hi().is_max_value()
              && target.get_uint64()
                      > search_ctx_.get_docid_range_hi().get_uint64()) {
      iter_end_ = true;
      ret = OB_ITER_END;
    } else if (search_ctx_.has_docid_range()
              && !search_ctx_.get_docid_range_lo().is_min_value()
              && target.get_uint64()
                      < search_ctx_.get_docid_range_lo().get_uint64()) {
      effective_target.set_uint64(
          search_ctx_.get_docid_range_lo().get_uint64());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
      ObDASRowID child_min_id;
      ObDASRowID child_max_id;
      double child_max_score = 0.0;
      bool has_child_tuple = false;
      int min_cmp = 0;
      int max_cmp = 0;
      if (child_ended_.at(i)) {
        // skip exhausted child
      } else if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null async segment child", K(ret), K(i));
      } else if (use_batch_merge_) {
        if (OB_FAIL(get_batch_child_shallow_tuple(
            i, effective_target, inclusive, has_child_tuple,
            child_min_id, child_max_id, child_max_score))) {
          LOG_WARN("failed to get buffered async segment child max score tuple",
              K(ret), K(i), K(effective_target), K(inclusive));
        }
      } else {
        const MaxScoreTuple *child_tuple = nullptr;
        ret = children_[i]->advance_shallow(
            effective_target, inclusive, child_tuple);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          child_ended_.at(i) = true;
          head_rowids_.at(i).set_max();
          head_scores_.at(i) = 0.0;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to shallow advance async segment child",
                  K(ret), K(i), K(effective_target));
        } else if (OB_ISNULL(child_tuple)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async segment max score tuple", K(ret), K(i));
        } else {
          child_min_id = child_tuple->get_min_id();
          child_max_id = child_tuple->get_max_id();
          child_max_score = child_tuple->get_max_score();
          has_child_tuple = true;
        }
      }
      if (OB_FAIL(ret) || !has_child_tuple) {
      } else if (OB_FAIL(compare_rowid(child_min_id, min_id, min_cmp))) {
        LOG_WARN("failed to compare async segment block minimum", K(ret), K(i));
      } else if (OB_FAIL(compare_rowid(child_max_id, max_id, max_cmp))) {
        LOG_WARN("failed to compare async segment block maximum", K(ret), K(i));
      } else {
        has_active_tuple = true;
        if (min_cmp < 0) {
          min_id = child_min_id;
        }
        if (max_cmp < 0) {
          max_id = child_max_id;
        }
        max_score = MAX(max_score, child_max_score);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!has_active_tuple) {
      iter_end_ = true;
      ret = OB_ITER_END;
    } else {
      max_score_tuple_.set(min_id, max_id, max_score);
      max_score_tuple = &max_score_tuple_;
      if (use_batch_merge_ || use_ordered_concat_) {
        shallow_target_ = effective_target.get_uint64();
        shallow_inclusive_ = inclusive;
      }
      in_shallow_status_ = true;
    }
  } else {
    // A scoreless segment merge has no block-max score to aggregate across children.
    // The generic fallback returns a conservative zero-score tuple and materializes
    // through this merge op only when an exclusive boundary needs an exact successor.
    ret = ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple);
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("score threshold is unsupported by direct async segment merge",
        K(ret), K_(op_id), K(threshold));
  } else if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to update async segment threshold", K(ret), K(threshold));
  } else {
    threshold_cached_ = true;
    cached_threshold_ = threshold;
    for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
      if (OB_NOT_NULL(children_[i])
          && OB_FAIL(children_[i]->set_min_competitive_score(threshold))) {
        LOG_WARN("failed to forward threshold to async segment child", K(ret), K(i), K(threshold));
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentMergeOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = 0.0;
  if (ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("max score is unsupported by direct async segment merge", K(ret), K_(op_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < active_segment_cnt_; ++i) {
    double child_max_score = 0.0;
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async segment child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->calc_max_score(child_max_score))) {
      LOG_WARN("failed to calculate async segment max score", K(ret), K(i));
    } else {
      threshold = MAX(threshold, child_max_score);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
