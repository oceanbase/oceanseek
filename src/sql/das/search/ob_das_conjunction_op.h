/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


/*
 * Overview
 * - Conjunction search operator.
 *
 * Key Responsibilities
 * - 
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{

class ObDASConjunctionOpParam : public ObIDASSearchOpParam
{
public:
  ObDASConjunctionOpParam(const ObIArray<ObIDASSearchOp *> &required_ops)
      : ObIDASSearchOpParam(DAS_SEARCH_OP_CONJUNCTION), required_ops_(&required_ops) {}
  ~ObDASConjunctionOpParam() {}
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_required_ops() const { return required_ops_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASConjunctionOpParam", ObIDASSearchOpParam,
                       KPC_(required_ops));
private:
  const ObIArray<ObIDASSearchOp *> *required_ops_;
};

class ObDASConjunctionOp : public ObIDASSearchOp
{
public:
  ObDASConjunctionOp(ObDASSearchCtx &search_ctx)
      : ObIDASSearchOp(search_ctx) {}
  virtual ~ObDASConjunctionOp() {}
  ScoreCapability get_score_capability() const override
  {
    return !is_scoring_ ? ScoreCapability::NONE
        : (is_fixed_score_ ? ScoreCapability::CONSTANT : ScoreCapability::ARRAY);
  }
  double get_constant_score() const override
  { return is_scoring_ ? fixed_score_bound_ : 0.0; }

  bool can_enumerate_rowids() const override
  {
    bool can_enumerate = true;
    bool has_driver = false;
    for (int64_t i = 0; can_enumerate && i < children_cnt_; ++i) {
      if (OB_ISNULL(children_[i])) {
        can_enumerate = false;
      } else if (!children_[i]->is_probe_mode()) {
        has_driver = true;
        can_enumerate = children_[i]->can_enumerate_rowids();
      }
    }
    return has_driver && can_enumerate;
  }
  virtual int try_apply_sort_bottom(const common::ObDatum &sort_bottom, bool &applied) override;
  
private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_next_rowid(ObDASNestedKey &next_nk, double &score) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  int do_set_min_competitive_score(const double &threshold) override;
  int do_calc_max_score(double &threshold) override;

private:
  template <typename RowType>
  int do_advance_to_impl(const RowType &target, RowType &curr_id, double &score);
  template <typename RowType>
  int next_impl(RowType &next_id, double &score);
  template <typename RowType>
  int inner_advance_to(const RowType &target, RowType &curr_id, double &score);
  template <typename RowType>
  int advance_driver_ops_to(const RowType &target, RowType &curr_id, double &score);
  // Splits children into driver_ops_ and probe_ops_ based on is_probe_mode().
  // Called from do_open / do_rescan so the conjunction can coordinate probe-capable children.
  int classify_children();
  int init_fixed_score_bound();
  bool has_fixed_score_bound() const { return fixed_score_bound_ >= 0.0; }
  int inner_advance_to_with_probe(const ObDASRowID &target, ObDASRowID &curr_id, double &score);

private:
  static constexpr double INVALID_FIXED_SCORE_BOUND = -1.0;
  int64_t last_idx_ = 0;
  int64_t nested_last_idx_ = 0;
  bool is_first_nk_ = true;
  bool has_probe_child_ = false;
  // True only when nonnegative integer driver scores have an exact sum <= 2^53.
  bool is_fixed_score_ = false;
  // Valid bounds are finite and nonnegative; the sentinel also means no cached bound.
  // Rebuilt after child rescans; accepted sums are independent of driver order.
  double fixed_score_bound_ = INVALID_FIXED_SCORE_BOUND;
  // Probe-capable children; the driver is never in this list.
  ObSEArray<ObIDASSearchOp *, 4> probe_ops_;
  ObSEArray<ObIDASSearchOp *, 4> driver_ops_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_
