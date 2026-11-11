/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_log_select_into.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogSelectInto::set_input_ordering(const ObLogicalOperator &input)
{
  int ret = OB_SUCCESS;
  input_ordering_column_idxs_.reset();
  input_ordering_directions_.reset();
  const ObIArray<OrderItem> &input_ordering = input.get_op_ordering();
  const EqualSets &input_equal_sets = input.get_output_equal_sets();
  for (int64_t i = 0; OB_SUCC(ret) && i < input_ordering.count(); ++i) {
    const OrderItem &order_item = input_ordering.at(i);
    int64_t select_expr_idx = OB_INVALID_INDEX_INT64;
    // Ordering is described by expressions, while Result Catalog metadata is
    // described by result columns. Match the final child's order expression
    // against this SELECT INTO's select_exprs_; the matched select-expression
    // index is the result-column index used by the Parquet schema.
    if (OB_ISNULL(order_item.expr_)
        || order_item.order_type_ < NULLS_FIRST_ASC
        || order_item.order_type_ >= UNORDERED
        || !ObOptimizerUtil::find_equal_expr(select_exprs_,
                                              order_item.expr_,
                                              input_equal_sets,
                                              select_expr_idx)) {
      // Only an ordering prefix represented by the result schema can be
      // advertised. Once a leading key is absent, later keys do not
      // independently describe a sorted result.
      break;
    } else {
      bool duplicate = false;
      for (int64_t j = 0; !duplicate && j < input_ordering_column_idxs_.count(); ++j) {
        duplicate = input_ordering_column_idxs_.at(j) == select_expr_idx;
      }
      if (duplicate) {
        // Repeating an already-recorded key does not strengthen the ordering
        // contract exposed by the result schema.
      } else if (OB_FAIL(input_ordering_column_idxs_.push_back(select_expr_idx))) {
        LOG_WARN("failed to save async result ordering column", K(ret), K(i), K(select_expr_idx));
      } else if (OB_FAIL(input_ordering_directions_.push_back(
                     static_cast<int64_t>(order_item.order_type_)))) {
        LOG_WARN("failed to save async result ordering direction", K(ret), K(i),
                 K(order_item.order_type_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    input_ordering_column_idxs_.reset();
    input_ordering_directions_.reset();
  }
  return ret;
}

int ObLogSelectInto::est_cost()
{
  int ret = OB_SUCCESS;
  int parallel = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret)); 
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    set_op_cost(ObOptEstCost::cost_get_rows(child->get_card() / parallel, 
                                            opt_ctx));
    set_cost(op_cost_ + child->get_cost());
    set_card(child->get_card());
  }
  return ret;
}

int ObLogSelectInto::compute_plan_type()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_plan_type())) {
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (LOG_EXCHANGE == child->get_type()) {
    location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSelectInto::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, select_exprs_))) {
    LOG_WARN("failed to push back select exprs", K(ret));
  } else if (file_partition_expr_ != NULL && OB_FAIL(all_exprs.push_back(file_partition_expr_))) {
    LOG_WARN("failed to push back file partition expr", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSelectInto::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, select_exprs_))) {
    LOG_WARN("failed to replace select exprs", K(ret));
  }
  return ret;
}
