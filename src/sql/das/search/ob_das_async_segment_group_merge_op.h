/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or
 * disclosed without authorization.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_GROUP_MERGE_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_GROUP_MERGE_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

struct ObDASAsyncSegmentGroupMergeOpParam : public ObIDASSearchOpParam
{
public:
  explicit ObDASAsyncSegmentGroupMergeOpParam(
      const common::ObIArray<ObIDASSearchOp *> &group_ops)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE),
      group_ops_(&group_ops)
  {}
  virtual ~ObDASAsyncSegmentGroupMergeOpParam() {}

  int get_children_ops(common::ObIArray<ObIDASSearchOp *> &children) const override;
  int64_t get_group_op_cnt() const
  { return OB_NOT_NULL(group_ops_) ? group_ops_->count() : 0; }

  const common::ObIArray<ObIDASSearchOp *> *group_ops_;
};

// Each child is a complete Group query tree with its Boolean/filter/deletion processing done.
// The supported domain relies on DML/refresh to keep visible RowIDs unique across Groups.
// Del-insert tables are excluded until that invariant is enforced, so this operator emits in
// Group-slot order without sorting or deduplication.
class ObDASAsyncSegmentGroupMergeOp : public ObIDASSearchOp
{
public:
  explicit ObDASAsyncSegmentGroupMergeOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      child_opened_(),
      child_prepared_(),
      active_group_cnt_(0),
      current_group_idx_(0),
      threshold_cached_(false),
      cached_threshold_(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE),
      iter_end_(false)
  {
    op_type_ = DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE;
  }
  virtual ~ObDASAsyncSegmentGroupMergeOp() {}
  /**
   * @brief Emits one batch from the current group and preserves its boundary for TopN feedback.
   *
   * Group draining cannot be mixed with ordinary row-ID draining before a reset. A completed group
   * advances on the next call, after the consumer has processed the batch and updated the TopN bound.
   * @param[in] capacity Positive capacity of both output arrays.
   * @param[out] ids Document IDs from one group only; the caller owns the array.
   * @param[out] scores Scores paired with ids; the caller owns the array.
   * @param[in] sort_bottom Borrowed leading key of a full TopN, or NULL; valid only for this call.
   * @param[out] count Number of emitted rows, possibly zero at a group boundary.
   * @param[out] info End flags for the current group and partition; exhaustion returns OB_SUCCESS.
   * @return OB_SUCCESS on success; mode/buffer/capacity violations return OB_STATE_NOT_MATCH; child errors propagate.
   */
  int drain_current_group(int64_t capacity, uint64_t *ids, double *scores,
                          const common::ObDatum *sort_bottom, int64_t &count, ObDASGroupBatchInfo &info);

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &curr_id,
      double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_set_min_competitive_score(const double &threshold) override;
  int do_calc_max_score(double &threshold) override;

  int load_current_partition_groups();
  int reset_children_for_partition();
  int ensure_child_ready(const int64_t group_idx);

private:
  common::ObSEArray<bool, 4> child_opened_; // Opened at least once during this plan's lifetime.
  common::ObSEArray<bool, 4> child_prepared_; // Prepared by open/rescan and threshold setup for the current partition scan.
  int64_t active_group_cnt_;
  int64_t current_group_idx_;
  bool threshold_cached_;
  double cached_threshold_;
  bool iter_end_;
  /**
   * @brief Prevents mixing ordinary iteration with reads limited to the current group.
   * Once selected, the read mode remains fixed until the partition state is reset.
   */
  enum DrainMode
  {
    // initial value
    DRAIN_NONE,
    // Use next_rowid()/drain_until(); group boundaries are handled internally.
    DRAIN_ORDINARY,
    // Use drain_current_group(); do not drain data over groups so TopN can update bottom before the next group.
    DRAIN_GROUP
  };
  DrainMode drain_mode_ = DRAIN_NONE; // Read interface selected for the current partition binding.
  bool pending_group_end_ = false; // Defers moving to the next group until the consumer processes the last batch.
  int64_t bottom_checked_group_ = -1; // Last group considered for pruning; each group is checked before its first read.
  /** @brief Applies the current TopN boundary to supported unread required inputs of the current group. */
  int apply_sort_bottom(const common::ObDatum &sort_bottom);
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncSegmentGroupMergeOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_GROUP_MERGE_OP_H_
