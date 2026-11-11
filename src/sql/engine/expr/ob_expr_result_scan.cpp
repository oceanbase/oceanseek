/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_result_scan.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExprResultScan::extract_query_id(const ObRawExpr *expr, ObString &query_id)
{
  int64_t chunk_no = OB_INVALID_INDEX_INT64;
  return extract_query_id_and_chunk_no(expr, query_id, chunk_no);
}

int ObExprResultScan::extract_query_id_and_chunk_no(const ObRawExpr *expr,
                                                     ObString &query_id,
                                                     int64_t &chunk_no)
{
  int ret = OB_SUCCESS;
  query_id.reset();
  chunk_no = OB_INVALID_INDEX_INT64;
  const ObRawExpr *arg0 = nullptr;
  const ObRawExpr *arg1 = nullptr;
  if (OB_ISNULL(expr) || T_FUN_SYS_RESULT_SCAN != expr->get_expr_type()
      || OB_UNLIKELY(expr->get_param_count() != 1 && expr->get_param_count() != 2)
      || OB_ISNULL(arg0 = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result_scan expression has unexpected shape", KR(ret));
  } else {
    while (OB_NOT_NULL(arg0)
           && T_FUN_SYS_CAST == arg0->get_expr_type()
           && arg0->get_param_count() > 0) {
      arg0 = arg0->get_param_expr(0);
    }
    if (OB_ISNULL(arg0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result_scan argument is null after unwrap", KR(ret));
    } else if (T_VARCHAR == arg0->get_expr_type() && !arg0->has_flag(CNT_STATIC_PARAM)) {
      const ObString &value = static_cast<const ObConstRawExpr *>(arg0)->get_value().get_string();
      if (OB_UNLIKELY(value.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "RESULT_SCAN query_id must not be empty");
      } else {
        query_id = value;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                     "RESULT_SCAN query_id must be a literal string");
      LOG_WARN("result_scan argument must be a literal string", KR(ret),
               "expr_type", static_cast<int>(arg0->get_expr_type()));
    }
  }
  if (OB_SUCC(ret) && expr->get_param_count() == 2) {
    arg1 = expr->get_param_expr(1);
    while (OB_NOT_NULL(arg1)
           && T_FUN_SYS_CAST == arg1->get_expr_type()
           && arg1->get_param_count() > 0) {
      arg1 = arg1->get_param_expr(0);
    }
    if (OB_ISNULL(arg1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result_scan chunk_id argument is null after unwrap", KR(ret));
    } else if (T_INT == arg1->get_expr_type() && !arg1->has_flag(CNT_STATIC_PARAM)) {
      chunk_no = static_cast<const ObConstRawExpr *>(arg1)->get_value().get_int();
      if (chunk_no < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "RESULT_SCAN chunk_id must be a non-negative literal integer");
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                     "RESULT_SCAN chunk_id must be a non-negative literal integer");
      LOG_WARN("result_scan chunk_id must be a literal integer", KR(ret),
               "expr_type", static_cast<int>(arg1->get_expr_type()));
    }
  }
  return ret;
}

ObExprResultScan::ObExprResultScan(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_RESULT_SCAN,
                         "result_scan",
                         ONE_OR_TWO,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprResultScan::~ObExprResultScan()
{
}

int ObExprResultScan::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num != 1 && param_num != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result_scan has invalid parameter count", KR(ret), K(param_num));
  } else {
    types[0].set_calc_type(ObVarcharType);
    if (param_num == 2) {
      types[1].set_calc_type(ObIntType);
    }
    type.set_int();
  }
  return ret;
}

int ObExprResultScan::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  ObString query_id;
  int64_t chunk_no = OB_INVALID_INDEX_INT64;
  if (OB_FAIL(extract_query_id_and_chunk_no(&raw_expr, query_id, chunk_no))) {
    LOG_WARN("invalid result_scan arguments", KR(ret));
  } else {
    rt_expr.eval_func_ = ObExprResultScan::eval_result_scan;
  }
  return ret;
}

int ObExprResultScan::eval_result_scan(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res_datum);
  return OB_ERR_UNEXPECTED;
}

} // namespace sql
} // namespace oceanbase
