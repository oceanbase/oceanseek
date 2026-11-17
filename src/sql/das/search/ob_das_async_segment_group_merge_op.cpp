/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or
 * disclosed without authorization.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_async_segment_group_merge_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASAsyncSegmentGroupMergeOpParam::get_children_ops(
    ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(group_ops_) && OB_FAIL(children.assign(*group_ops_))) {
    LOG_WARN("failed to assign async segment group children", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncSegmentGroupMergeOpParam &param =
      static_cast<const ObDASAsyncSegmentGroupMergeOpParam &>(op_param);
  if (OB_UNLIKELY(param.get_group_op_cnt() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async segment group merge requires children", K(ret),
             K(param.get_group_op_cnt()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.get_group_op_cnt(); ++i) {
    if (OB_FAIL(child_opened_.push_back(false))) {
      LOG_WARN("failed to initialize async group child state", K(ret), K(i));
    } else if (OB_FAIL(child_prepared_.push_back(false))) {
      LOG_WARN("failed to initialize async group scan state", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
    child_opened_.reset();
    child_prepared_.reset();
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::load_current_partition_groups()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition =
      search_ctx_.get_current_async_partition();
  if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null current async index partition", K(ret));
  } else {
    active_group_cnt_ = partition->get_storage_group_ctx().get_group_count();
    if (OB_UNLIKELY(active_group_cnt_ <= 0 || active_group_cnt_ > children_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current partition group count exceeds fixed tree capacity",
               K(ret), K_(active_group_cnt), K_(children_cnt));
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::reset_children_for_partition()
{
  int ret = OB_SUCCESS;
  active_group_cnt_ = 0;
  current_group_idx_ = 0;
  iter_end_ = false;
  drain_mode_ = DRAIN_NONE;
  pending_group_end_ = false;
  bottom_checked_group_ = -1;
  for (int64_t i = 0; i < child_prepared_.count(); ++i) {
    if (child_opened_.at(i)) {
      children_[i]->release_scan_resources();
    }
    child_prepared_.at(i) = false;
  }
  if (OB_FAIL(load_current_partition_groups())) {
    LOG_WARN("failed to load current async segment groups", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::ensure_child_ready(const int64_t group_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(group_idx < 0 || group_idx >= active_group_cnt_)
      || OB_ISNULL(children_[group_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async group child", K(ret), K(group_idx), K_(active_group_cnt));
  } else if (!child_prepared_.at(group_idx)) {
    if (!child_opened_.at(group_idx)) {
      if (OB_FAIL(children_[group_idx]->open())) {
        LOG_WARN("failed to lazily open async group", K(ret), K(group_idx));
      } else {
        child_opened_.at(group_idx) = true;
      }
    } else if (OB_FAIL(children_[group_idx]->rescan())) {
      LOG_WARN("failed to lazily rescan async group", K(ret), K(group_idx));
    }
    if (OB_SUCC(ret) && threshold_cached_ && is_scoring_
        && OB_FAIL(children_[group_idx]->set_min_competitive_score(cached_threshold_))) {
      LOG_WARN("failed to apply cached group threshold", K(ret), K(group_idx));
    }
    if (OB_SUCC(ret)) {
      child_prepared_.at(group_idx) = true;
    }
  }
  return ret;
}


int ObDASAsyncSegmentGroupMergeOp::do_open()
{
  int ret = OB_SUCCESS;
  if (active_group_cnt_ > 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("async group merge is already open", K(ret));
  } else {
    threshold_cached_ = false;
    cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
    if (OB_FAIL(reset_children_for_partition())) {
      LOG_WARN("failed to open async segment group merge", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_rescan()
{
  int ret = OB_SUCCESS;
  threshold_cached_ = false;
  cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
  if (OB_FAIL(reset_children_for_partition())) {
    LOG_WARN("failed to rescan async segment group merge", K(ret));
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_close()
{
  active_group_cnt_ = 0;
  current_group_idx_ = 0;
  threshold_cached_ = false;
  cached_threshold_ = DAS_SEARCH_INITIAL_COMPETITIVE_SCORE;
  iter_end_ = false;
  child_opened_.reset();
  child_prepared_.reset();
  return OB_SUCCESS;
}

int ObDASAsyncSegmentGroupMergeOp::do_next_rowid(
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (drain_mode_ == DRAIN_GROUP) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ordinary row reads cannot follow async group draining",
             K(ret), K_(drain_mode), K_(current_group_idx), K_(active_group_cnt));
  } else {
    drain_mode_ = DRAIN_ORDINARY;
    if (iter_end_) {
      ret = OB_ITER_END;
    }
  }
  while (OB_SUCC(ret) && current_group_idx_ < active_group_cnt_) {
    ObIDASSearchOp *child = children_[current_group_idx_];
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null current async segment group child", K(ret),
               K_(current_group_idx));
    } else if (OB_FAIL(ensure_child_ready(current_group_idx_))) {
      LOG_WARN("failed to prepare current async group", K(ret), K_(current_group_idx));
    } else if (OB_FAIL(child->next_rowid(next_id, score))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        child->release_scan_resources();
        ++current_group_idx_;
      } else {
        LOG_WARN("failed to get row from async segment group child",
                 K(ret), K_(current_group_idx));
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && current_group_idx_ >= active_group_cnt_) {
    iter_end_ = true;
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  if (drain_mode_ == DRAIN_GROUP) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ordinary batch reads cannot follow async group draining",
             K(ret), K(capacity), K_(drain_mode), K_(current_group_idx), K_(active_group_cnt));
  } else {
    drain_mode_ = DRAIN_ORDINARY;
    count = 0;
    next_id_after.reset();
    next_score_after = 0.0;
    if (iter_end_) {
      next_id_after.set_max();
    } else if (!boundary.is_max()) {
      // Group-slot order is not a globally sorted RowID order. Preserve the
      // existing row-wise boundary semantics and only batch unbounded drains.
      ret = ObIDASSearchOp::do_drain_until(
          boundary, capacity, out_ids, out_scores, count,
          next_id_after, next_score_after);
    } else {
      while (OB_SUCC(ret) && count < capacity
             && current_group_idx_ < active_group_cnt_) {
        ObIDASSearchOp *child = children_[current_group_idx_];
        const int64_t remaining_capacity = capacity - count;
        int64_t child_count = 0;
        ObDASRowID child_next_id;
        double child_next_score = 0.0;
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null current async segment group child", K(ret),
                   K_(current_group_idx));
        } else if (OB_FAIL(ensure_child_ready(current_group_idx_))) {
          LOG_WARN("failed to prepare batch async group", K(ret), K_(current_group_idx));
        } else if (OB_FAIL(child->drain_until(
            boundary, remaining_capacity, out_ids + count,
            nullptr == out_scores ? nullptr : out_scores + count,
            child_count, child_next_id, child_next_score))) {
          LOG_WARN("failed to batch drain async segment group child", K(ret),
                   K_(current_group_idx), K(remaining_capacity));
        } else {
          count += child_count;
          if (child_count == remaining_capacity) {
            // A full batch has no lookahead contract. Continue on the next call.
          } else if (child_next_id.is_max()) {
            child->release_scan_resources();
            ++current_group_idx_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected lookahead for unbounded async group drain", K(ret),
                     K_(current_group_idx), K(child_count), K(remaining_capacity),
                     K(child_next_id), K(child_next_score));
          }
        }
      }
      if (OB_SUCC(ret) && current_group_idx_ >= active_group_cnt_) {
        iter_end_ = true;
        next_id_after.set_max();
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  UNUSED(target);
  UNUSED(curr_id);
  UNUSED(score);
  return OB_NOT_SUPPORTED;
}

int ObDASAsyncSegmentGroupMergeOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  UNUSED(target);
  UNUSED(inclusive);
  max_score_tuple = nullptr;
  return OB_NOT_SUPPORTED;
}

int ObDASAsyncSegmentGroupMergeOp::do_set_min_competitive_score(
    const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to update async group threshold", K(ret), K(threshold));
  } else {
    threshold_cached_ = true;
    cached_threshold_ = threshold;
    if (is_scoring_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < active_group_cnt_; ++i) {
        if (OB_ISNULL(children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async segment group child", K(ret), K(i));
        } else if (child_prepared_.at(i) && i >= current_group_idx_
                   && OB_FAIL(children_[i]->set_min_competitive_score(threshold))) {
          LOG_WARN("failed to forward threshold to async segment group child",
                   K(ret), K(i), K(threshold));
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = 0.0;
  // Exhausted groups have returned their posting iterators and cannot add scores.
  for (int64_t i = current_group_idx_; OB_SUCC(ret) && i < active_group_cnt_; ++i) {
    double child_max_score = 0.0;
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async segment group child", K(ret), K(i));
    } else if (OB_FAIL(ensure_child_ready(i))) {
      LOG_WARN("failed to prepare async group max score", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->calc_max_score(child_max_score))) {
      LOG_WARN("failed to calculate async segment group max score", K(ret), K(i));
    } else {
      threshold = MAX(threshold, child_max_score);
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::apply_sort_bottom(const ObDatum &sort_bottom)
{
  int ret = OB_SUCCESS;
  bool applied = false;
  if (search_ctx_.is_sort_topn_enabled()) {
    if (current_group_idx_ < 0 || current_group_idx_ >= active_group_cnt_
        || OB_ISNULL(children_[current_group_idx_])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async segment group index for hybrid sort pruning or child is null",
               K(ret), K_(current_group_idx), K_(active_group_cnt));
    } else if (OB_FAIL(children_[current_group_idx_]->try_apply_sort_bottom(sort_bottom, applied))) {
      LOG_WARN("failed to rebind current group hybrid sort ranges", K(ret), K_(current_group_idx));
    }
  }
  return ret;
}

int ObDASAsyncSegmentGroupMergeOp::drain_current_group(
    int64_t capacity, uint64_t *ids, double *scores, const ObDatum *sort_bottom,
    int64_t &count, ObDASGroupBatchInfo &info)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer read_timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
  INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
  count = 0;
  info = ObDASGroupBatchInfo();
  if (OB_FAIL(ret)) {
  } else if (drain_mode_ == DRAIN_ORDINARY || capacity <= 0 || ids == nullptr || scores == nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid hybrid sort group read state", K(ret), K(capacity), KP(ids), KP(scores),
             K_(drain_mode), K_(current_group_idx));
  } else {
    drain_mode_ = DRAIN_GROUP;
    if (iter_end_) {
      info.all_end_ = true;
      info.group_end_ = true;
    } else {
      if (pending_group_end_) {
        ++current_group_idx_;
        pending_group_end_ = false;
      }
      if (current_group_idx_ >= active_group_cnt_) {
        iter_end_ = true;
        info.all_end_ = true;
        info.group_end_ = true;
      } else if (OB_FAIL(ensure_child_ready(current_group_idx_))) {
        LOG_WARN("failed to prepare hybrid sort group", K(ret), K_(current_group_idx));
      } else {
        if (bottom_checked_group_ != current_group_idx_) {
          INC_METRIC_VAL(common::ObMetricId::HS_SORT_BOTTOM_GROUPS, 1);
          LOG_TRACE("hybrid sort bottom at group boundary", K_(current_group_idx),
                    K_(active_group_cnt), "has_bottom", nullptr != sort_bottom);
          if (OB_SUCC(ret) && sort_bottom != nullptr && OB_FAIL(apply_sort_bottom(*sort_bottom))) {
            LOG_WARN("failed to apply TopN threshold to async segment group", K(ret), K_(current_group_idx));
          }
          if (OB_SUCC(ret)) {
            bottom_checked_group_ = current_group_idx_;
          }
        }
        ObDASRowID boundary;
        ObDASRowID next;
        double next_score = 0;
        boundary.set_max();
        if (OB_SUCC(ret) && OB_FAIL(children_[current_group_idx_]->drain_until(boundary, capacity, ids, scores, count, next, next_score))) {
          LOG_WARN("failed to drain hybrid sort segment group", K(ret), K_(current_group_idx), K(count), K(capacity));
        }
        if (OB_SUCC(ret) && (count < 0 || count > capacity)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected hybrid sort group batch size", K(ret), K(count), K(capacity), K_(current_group_idx));
        } else if (OB_SUCC(ret) && count < capacity) {
          if (!next.is_max()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hybrid sort segment group returned a short batch before exhaustion", K(ret),
                     K(count), K(capacity), K_(current_group_idx));
          } else {
            // ids/scores belong to the caller. Release the exhausted scan now,
            // but enter the next group only after TopN processes this batch.
            children_[current_group_idx_]->release_scan_resources();
            pending_group_end_ = true;
            info.group_end_ = true;
            if (current_group_idx_ + 1 == active_group_cnt_) {
              iter_end_ = true;
              info.all_end_ = true;
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // This entry returns no lookahead row. Empty groups count as calls, but contribute no output rows.
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
