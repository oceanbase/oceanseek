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
      active_group_cnt_(0),
      current_group_idx_(0),
      threshold_cached_(false),
      cached_threshold_(0.0),
      iter_end_(false)
  {
    op_type_ = DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE;
  }
  virtual ~ObDASAsyncSegmentGroupMergeOp() {}

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
  int reset_children_for_partition(const bool open_children);

private:
  common::ObSEArray<bool, 4> child_opened_;
  int64_t active_group_cnt_;
  int64_t current_group_idx_;
  bool threshold_cached_;
  double cached_threshold_;
  bool iter_end_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncSegmentGroupMergeOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_GROUP_MERGE_OP_H_
