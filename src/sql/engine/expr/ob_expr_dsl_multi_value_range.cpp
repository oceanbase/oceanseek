/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_dsl_multi_value_range.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
bool is_range_scalar(const ObIJsonBase &value)
{
  const ObJsonNodeType type = value.json_type();
  return value.is_json_scalar(type)
         && type != ObJsonNodeType::J_NULL
         && type != ObJsonNodeType::J_ERROR;
}

int eval_range_bound(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
                     uint16_t index, ObIJsonBase *&bound, uint8_t &encode_type)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  bound = nullptr;
  if (OB_FAIL(allocator.eval_arg(expr.args_[index], ctx, datum))) {
    LOG_WARN("failed to evaluate range bound", K(ret), K(index));
  } else if (datum->is_null()) {
    // An absent SQL bound is different from the JSON scalar null.
  } else if (OB_FAIL(allocator.add_baseline_size(datum))) {
    LOG_WARN("failed to add range bound baseline size", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &allocator, index, bound))) {
    LOG_WARN("failed to get JSON range bound", K(ret), K(index));
  } else if (OB_ISNULL(bound)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null range bound", K(ret), K(index));
  } else if (!is_range_scalar(*bound)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE bounds, expected non-null JSON scalar");
  } else if (OB_FAIL(share::ObSearchIndexPathEncoder::encode_type(bound->json_type(), encode_type))) {
    LOG_WARN("failed to get range bound encoding type", K(ret), K(index));
  }
  return ret;
}

int match_range_scalar(const ObIJsonBase &value, const ObIJsonBase *lower,
                       const ObIJsonBase *upper, uint8_t range_type,
                       bool include_lower, bool include_upper, bool &matched)
{
  int ret = OB_SUCCESS;
  uint8_t encode_type = 0;
  int cmp = 0;
  matched = false;
  if (!is_range_scalar(value)) {
    // Ignore nulls and objects, including arrays inside object members.
  } else if (OB_FAIL(share::ObSearchIndexPathEncoder::encode_type(value.json_type(), encode_type))) {
    LOG_WARN("failed to get value encoding type", K(ret));
  } else if (encode_type != range_type) {
    // Do not compare JSON values across encoding types or cast strings to numbers.
  } else if (lower != nullptr && OB_FAIL(value.compare(*lower, cmp))) {
    LOG_WARN("failed to compare lower bound", K(ret));
  } else if (lower != nullptr && (cmp < 0 || (cmp == 0 && !include_lower))) {
  } else if (upper != nullptr && OB_FAIL(value.compare(*upper, cmp))) {
    LOG_WARN("failed to compare upper bound", K(ret));
  } else if (upper != nullptr && (cmp > 0 || (cmp == 0 && !include_upper))) {
  } else {
    matched = true;
  }
  return ret;
}

struct ArrayCursor
{
  explicit ArrayCursor(ObIJsonBase *array = nullptr) : array_(array), next_(0) {}
  TO_STRING_KV(KP_(array), K_(next));
  ObIJsonBase *array_;
  uint64_t next_;
};

int match_range(ObIJsonBase &target, const ObIJsonBase *lower, const ObIJsonBase *upper,
                uint8_t range_type, bool include_lower, bool include_upper,
                ObEvalCtx &ctx, bool &matched)
{
  int ret = OB_SUCCESS;
  ObSEArray<ArrayCursor, 16> cursors;
  int64_t visited = 0;
  matched = false;
  if (target.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = match_range_scalar(target, lower, upper, range_type, include_lower, include_upper, matched);
  } else if (OB_FAIL(cursors.push_back(ArrayCursor(&target)))) {
    LOG_WARN("failed to push root array", K(ret));
  }
  // Keep array cursors, rather than all children, so traversal storage grows only
  // with depth. The JSON document helper owns the tree nodes for this evaluation.
  while (OB_SUCC(ret) && !matched && !cursors.empty()) {
    ArrayCursor &cursor = cursors.at(cursors.count() - 1);
    ObIJsonBase *member = nullptr;
    if ((visited++ & 1023) == 0 && OB_FAIL(ctx.exec_ctx_.fast_check_status())) {
      LOG_WARN("JSON range evaluation interrupted", K(ret));
    } else if (cursor.next_ == cursor.array_->element_count()) {
      cursors.pop_back();
    } else if (OB_FAIL(cursor.array_->get_array_element(cursor.next_++, member))) {
      LOG_WARN("failed to get array member", K(ret));
    } else if (OB_ISNULL(member)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null array member", K(ret));
    } else if (member->json_type() == ObJsonNodeType::J_ARRAY) {
      if (OB_FAIL(cursors.push_back(ArrayCursor(member)))) {
        LOG_WARN("failed to push nested array", K(ret));
      }
    } else if (OB_FAIL(match_range_scalar(*member, lower, upper, range_type,
                                         include_lower, include_upper, matched))) {
      LOG_WARN("failed to match array member", K(ret));
    }
  }
  return ret;
}
} // namespace

ObExprDslMultiValueRange::ObExprDslMultiValueRange(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_DSL_MULTI_VALUE_RANGE, N_DSL_MULTI_VALUE_RANGE,
                      5, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                      INTERNAL_IN_MYSQL_MODE)
{
}

int ObExprDslMultiValueRange::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num != 5)) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name(N_DSL_MULTI_VALUE_RANGE);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, 0, N_DSL_MULTI_VALUE_RANGE))) {
    LOG_WARN("invalid JSON range target type", K(ret));
  } else {
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(0);
    type.set_result_flag(NOT_NULL_FLAG);
    for (int64_t i = 3; OB_SUCC(ret) && i <= 4; ++i) {
      if (!ob_is_integer_type(types[i].get_type()) && types[i].get_type() != ObNullType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE inclusion flags, expected 0 or 1");
      }
    }
    for (int64_t i = 1; OB_SUCC(ret) && i <= 2; ++i) {
      // Use the standard SQL-to-JSON conversion, including binary and temporal
      // scalars. Validate the resulting JSON scalar in eval_range_bound().
      ObJsonExprHelper::set_type_for_value(types, i);
    }
  }
  return ret;
}

int ObExprDslMultiValueRange::eval_dsl_multi_value_range(const ObExpr &expr,
                                                         ObEvalCtx &ctx,
                                                         ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  const uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor allocator(alloc_guard.get_allocator(), expr.type_, tenant_id, ret);
  ObDatum *include_lower_datum = nullptr;
  ObDatum *include_upper_datum = nullptr;
  ObIJsonBase *lower = nullptr;
  ObIJsonBase *upper = nullptr;
  ObIJsonBase *target = nullptr;
  uint8_t lower_type = 0;
  uint8_t upper_type = 0;
  int64_t include_lower = 0;
  int64_t include_upper = 0;
  bool is_null = false;
  bool matched = false;
  if (OB_FAIL(allocator.eval_arg(expr.args_[3], ctx, include_lower_datum))) {
    LOG_WARN("failed to evaluate lower inclusion flag", K(ret));
  } else if (include_lower_datum->is_null()
             || (include_lower = include_lower_datum->get_int()) < 0 || include_lower > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE include_lower, expected 0 or 1");
  } else if (OB_FAIL(allocator.eval_arg(expr.args_[4], ctx, include_upper_datum))) {
    LOG_WARN("failed to evaluate upper inclusion flag", K(ret));
  } else if (include_upper_datum->is_null()
             || (include_upper = include_upper_datum->get_int()) < 0 || include_upper > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE include_upper, expected 0 or 1");
  } else if (OB_FAIL(eval_range_bound(expr, ctx, allocator, 1, lower, lower_type))) {
    LOG_WARN("failed to evaluate lower bound", K(ret));
  } else if (OB_FAIL(eval_range_bound(expr, ctx, allocator, 2, upper, upper_type))) {
    LOG_WARN("failed to evaluate upper bound", K(ret));
  } else if (lower == nullptr && upper == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE, at least one bound is required");
  } else if (lower != nullptr && upper != nullptr && lower_type != upper_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DSL_MULTI_VALUE_RANGE bounds must have the same encoding type");
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 0, target, is_null))) {
    LOG_WARN("failed to get JSON range target", K(ret));
  } else if (is_null) {
  } else if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null JSON range target", K(ret));
  } else if (OB_FAIL(match_range(*target, lower, upper, lower != nullptr ? lower_type : upper_type,
                                include_lower != 0, include_upper != 0, ctx, matched))) {
    LOG_WARN("failed to match JSON range", K(ret));
  }
  if (OB_SUCC(ret)) {
    res.set_int(matched);
  }
  return ret;
}

int ObExprDslMultiValueRange::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_dsl_multi_value_range;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
