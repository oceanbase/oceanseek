/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExprArrowStream::extract_stream_id(const ObRawExpr *expr, uint64_t &stream_id)
{
  int ret = OB_SUCCESS;
  stream_id = 0;
  const ObRawExpr *arg0 = nullptr;
  if (OB_ISNULL(expr) || T_FUN_SYS_ARROW_STREAM != expr->get_expr_type()
      || OB_UNLIKELY(expr->get_param_count() != 1)
      || OB_ISNULL(arg0 = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] arrow_stream expression has unexpected shape", K(ret));
  } else {
    // The resolver may wrap the constant in CAST(... AS UINT64).
    while (OB_NOT_NULL(arg0)
           && T_FUN_SYS_CAST == arg0->get_expr_type()
           && arg0->get_param_count() > 0) {
      arg0 = arg0->get_param_expr(0);
    }
    if (OB_ISNULL(arg0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] arrow_stream argument is null after unwrap", K(ret));
    } else if (T_INT == arg0->get_expr_type()) {
      stream_id = static_cast<uint64_t>(
          static_cast<const ObConstRawExpr *>(arg0)->get_value().get_int());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                     "arrow_stream: stream_id must be a literal integer");
      LOG_WARN("[FlightSQL] arrow_stream argument must be a literal integer",
               K(ret), "expr_type", static_cast<int>(arg0->get_expr_type()), KPC(arg0));
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObExprArrowStream
// ---------------------------------------------------------------------------

ObExprArrowStream::ObExprArrowStream(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ARROW_STREAM,
                         "arrow_stream",
                         1,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprArrowStream::~ObExprArrowStream()
{
}

int ObExprArrowStream::calc_result_type1(ObExprResType &type,
                                          ObExprResType &stream_id,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  // Accept the argument as-is (int). Do NOT set_calc_type(ObUInt64Type) —
  // that would add an implicit CAST wrapper, making it hard to read the
  // stream_id constant back in the resolver.
  stream_id.set_calc_type(ObIntType);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprArrowStream::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(1 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] arrow_stream expects exactly one argument (stream_id)", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] arrow_stream stream_id argument is null", K(ret));
  } else if (!raw_expr.get_param_expr(0)->is_const_expr()
             || raw_expr.get_param_expr(0)->has_flag(CNT_STATIC_PARAM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "arrow_stream: stream_id must be a literal integer");
  } else {
    rt_expr.eval_func_ = ObExprArrowStream::eval_sentinel;
  }
  return ret;
}

int ObExprArrowStream::eval_sentinel(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res_datum)
{
#ifndef OB_BUILD_OBSERVER_FLIGHT_SQL
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res_datum);
  return OB_NOT_SUPPORTED;
#else
  UNUSED(expr);
  UNUSED(ctx);
  res_datum.set_int(0);
  return OB_SUCCESS;
#endif
}

// ---------------------------------------------------------------------------
// ObExprArrowStreams — V2 marker for TABLE(arrow_streams('ingest_id'))
// ---------------------------------------------------------------------------

ObExprArrowStreams::ObExprArrowStreams(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ARROW_STREAMS,
                         "arrow_streams",
                         1,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprArrowStreams::~ObExprArrowStreams()
{
}

int ObExprArrowStreams::calc_result_type1(ObExprResType &type,
                                          ObExprResType &ingest_id,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  ingest_id.set_calc_type(ObVarcharType);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprArrowStreams::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(1 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] arrow_streams expects exactly one argument (ingest_id)", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] arrow_streams ingest_id argument is null", K(ret));
  } else if (!raw_expr.get_param_expr(0)->is_const_expr()
             || raw_expr.get_param_expr(0)->has_flag(CNT_STATIC_PARAM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "arrow_streams: ingest_id must be a literal string");
  } else {
    rt_expr.eval_func_ = ObExprArrowStreams::eval_sentinel;
  }
  return ret;
}

int ObExprArrowStreams::eval_sentinel(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &res_datum)
{
#ifndef OB_BUILD_OBSERVER_FLIGHT_SQL
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res_datum);
  return OB_NOT_SUPPORTED;
#else
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res_datum);
  return OB_ERR_UNEXPECTED;
#endif
}

} // namespace sql
} // namespace oceanbase
