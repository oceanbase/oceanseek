/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_merge_table.h"
#include "sql/engine/dml/ob_dml_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprMergeValueEqual::ObExprMergeValueEqual(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_MERGE_VALUE_EQUAL, N_MERGE_VALUE_EQUAL, 2,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprMergeValueEqual::calc_result_type2(ObExprResType &type,
                                             ObExprResType &left,
                                             ObExprResType &right,
                                             ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  // MERGE validates compatible column types before constructing this call.
  // Do not silently introduce a cast if this helper is used with unlike types.
  if (!left.is_null() && !right.is_null()
      && left.get_obj_meta() != right.get_obj_meta()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("merge value comparison requires identical types", K(ret), K(left), K(right));
  } else {
    left.set_calc_meta(left.get_obj_meta());
    left.set_calc_accuracy(left.get_accuracy());
    right.set_calc_meta(right.get_obj_meta());
    right.set_calc_accuracy(right.get_accuracy());
    type.set_int();
    type.set_precision(1);
    type.set_scale(0);
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return ret;
}

int ObExprMergeValueEqual::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_)
      || OB_ISNULL(rt_expr.args_)
      || OB_ISNULL(rt_expr.args_[0])
      || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge value comparison arguments", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = eval_merge_value_equal;
  }
  return ret;
}

int ObExprMergeValueEqual::eval_merge_value_equal(const ObExpr &expr,
                                                 ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *left = nullptr;
  ObDatum *right = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, left, right))) {
    LOG_WARN("failed to evaluate merge value comparison", K(ret));
  } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge value comparison datum is null", K(ret));
  } else if (left->is_null() || right->is_null()) {
    // Always return a non-NULL boolean, including one-sided NULL differences.
    res.set_int(left->is_null() == right->is_null());
  } else if (is_lob_storage(expr.args_[0]->datum_meta_.type_)
             && is_lob_storage(expr.args_[1]->datum_meta_.type_)) {
    int64_t changed = 0;
    if (OB_FAIL(ObDMLService::check_lob_column_changed(
            ctx, *expr.args_[0], *left, *expr.args_[1], *right, changed))) {
      LOG_WARN("failed to compare merge LOB contents", K(ret));
    } else {
      res.set_int(0 == changed);
    }
  } else {
    res.set_int(ObDatum::binary_equal(*left, *right));
  }
  return ret;
}

ObExprMergeConflictCheck::ObExprMergeConflictCheck(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_MERGE_CONFLICT_CHECK, N_MERGE_CONFLICT_CHECK, 1,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprMergeConflictCheck::calc_result_type1(ObExprResType &type,
                                                ObExprResType &type1,
                                                ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObIntType);
  type.set_int();
  type.set_precision(1);
  type.set_scale(0);
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprMergeConflictCheck::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)
      || OB_ISNULL(rt_expr.args_)
      || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge conflict check arguments", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = eval_merge_conflict_check;
  }
  return ret;
}

int ObExprMergeConflictCheck::eval_merge_conflict_check(const ObExpr &expr,
                                                       ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("failed to evaluate merge conflict check", K(ret));
  } else if (OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge conflict check datum is null", K(ret));
  } else if (!arg->is_null() && 0 != arg->get_int()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("merge conflict detected", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "MERGE TABLE with conflicting rows under FAIL strategy");
  } else {
    res.set_int(1);
  }
  return ret;
}

ObExprMergeUniqueCheck::ObExprMergeUniqueCheck(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_MERGE_UNIQUE_CHECK, N_MERGE_UNIQUE_CHECK, 2,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

// Returns the second argument unchanged. Copying its type keeps collection types
// such as sparsevector intact, which IF(..., VALUES(c), c) fails to deduce.
int ObExprMergeUniqueCheck::calc_result_type2(ObExprResType &type,
                                              ObExprResType &type1,
                                              ObExprResType &type2,
                                              ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObIntType);
  type = type2;
  return ret;
}

int ObExprMergeUniqueCheck::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_)
      || OB_ISNULL(rt_expr.args_)
      || OB_ISNULL(rt_expr.args_[0])
      || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge unique check arguments", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = eval_merge_unique_check;
  }
  return ret;
}

int ObExprMergeUniqueCheck::eval_merge_unique_check(const ObExpr &expr,
                                                   ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *conflict = nullptr;
  ObDatum *value = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, conflict))) {
    LOG_WARN("failed to evaluate merge unique check", K(ret));
  } else if (OB_ISNULL(conflict)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge unique check datum is null", K(ret));
  } else if (!conflict->is_null() && 0 != conflict->get_int()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("merge row conflicts with another row on a unique key", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "MERGE TABLE: an incoming row conflicts with a row of "
                   "a different primary key on a unique key, overwriting that row is");
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, value))) {
    LOG_WARN("failed to evaluate merge unique check value", K(ret));
  } else {
    res.set_datum(*value);
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
