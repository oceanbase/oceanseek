/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_vec_ivf.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprVecIVFBase::ObExprVecIVFBase(
    ObIAllocator &allocator,
    const ObItemType item_type,
    const char *name,
    const ObVecIVFExprRole role)
  : ObFuncExprOperator(allocator, item_type, name, MORE_THAN_ZERO,
                       VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    role_(role)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFBase::calc_result_typeN(
    ObExprResType &type,
    ObExprResType *types,
    int64_t param_num,
    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num != 1) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF generated expression arguments", K(ret), K(param_num), KP(types));
  } else if (ObVecIVFExprRole::DATA == role_) {
    type.set_varchar();
    type.set_length(OB_MAX_VARCHAR_LENGTH);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    type.set_int();
  }
  return ret;
}

int ObExprVecIVFBase::calc_resultN(
    ObObj &result,
    const ObObj *objs_array,
    int64_t param_num,
    ObExprCtx &expr_ctx) const
{
  UNUSEDx(result, objs_array, param_num, expr_ctx);
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFBase::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(expr_cg_ctx, raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF runtime expression", K(ret), K(rt_expr.arg_cnt_), KP(rt_expr.args_));
  } else {
    switch (role_) {
      case ObVecIVFExprRole::TYPE:
        rt_expr.eval_func_ = eval_vec_ivf_type;
        break;
      case ObVecIVFExprRole::CID:
        rt_expr.eval_func_ = eval_vec_ivf_cid;
        break;
      case ObVecIVFExprRole::DATA:
        rt_expr.eval_func_ = eval_vec_ivf_data;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected VEC_IVF expression role", K(ret), K(role_));
        break;
    }
  }
  return ret;
}

int ObExprVecIVFBase::eval_vec_ivf_type(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObDatum &res)
{
  // The physical build operator fills the final block type. The generated
  // expression remains a stable projection hook for that operator.
  UNUSEDx(expr, ctx);
  res.set_null();
  return OB_SUCCESS;
}

int ObExprVecIVFBase::eval_vec_ivf_cid(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObDatum &res)
{
  // The physical build operator assigns the centroid id after routing.
  UNUSEDx(expr, ctx);
  res.set_null();
  return OB_SUCCESS;
}

int ObExprVecIVFBase::eval_vec_ivf_data(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *source = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, source))) {
    LOG_WARN("failed to evaluate VEC_IVF_DATA source", K(ret), KPC(expr.args_[0]));
  } else if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("VEC_IVF_DATA source datum is null", K(ret), KPC(expr.args_[0]));
  } else if (source->is_null()) {
    res.set_null();
  } else {
    const ObObjType source_type = expr.args_[0]->datum_meta_.type_;
    const bool is_lob = is_lob_storage(source_type);
    ObString data;
    ObEvalCtx::TempAllocGuard tmp_alloc_guard(ctx);
    ObArenaAllocator &tmp_allocator = tmp_alloc_guard.get_allocator();
    ObExprStrResAlloc res_allocator(expr, ctx);
    ObTextStringIter source_iter(source_type, CS_TYPE_BINARY, source->get_string());
    if (is_lob) {
      ObTextStringHelper::reset_lob_iter_if_timeout(&ctx.exec_ctx_);
    }
    // Fast path for a disk in-row LOB. The iterator handles other locator forms
    // and materializes out-row data directly into the expression result buffer.
    if (is_lob
        && 0 != source->len_
        && !source->get_lob_data().is_mem_loc_
        && source->get_lob_data().in_row_) {
      const ObLobCommon &lob = source->get_lob_data();
      data.assign_ptr(lob.get_inrow_data_ptr(),
                      static_cast<int32_t>(lob.get_byte_size(source->len_)));
    } else if (OB_FAIL(ObTextStringHelper::build_text_iter(
                   source_iter, &ctx.exec_ctx_, ctx.exec_ctx_.get_my_session(),
                   &res_allocator, &tmp_allocator))) {
      LOG_WARN("failed to initialize VEC_IVF_DATA source iterator", K(ret), K(source_type));
    } else if (OB_FAIL(source_iter.get_full_data(data))) {
      LOG_WARN("failed to materialize VEC_IVF_DATA source", K(ret), K(source_type));
    }
    if (OB_SUCC(ret)) {
      res.set_string(data);
    }
  }
  return ret;
}

ObExprVecIVFType::ObExprVecIVFType(ObIAllocator &allocator)
  : ObExprVecIVFBase(
      allocator, T_FUN_SYS_VEC_IVF_TYPE, N_VEC_IVF_TYPE, ObVecIVFExprRole::TYPE)
{}

ObExprVecIVFCid::ObExprVecIVFCid(ObIAllocator &allocator)
  : ObExprVecIVFBase(
      allocator, T_FUN_SYS_VEC_IVF_CID, N_VEC_IVF_CID, ObVecIVFExprRole::CID)
{}

ObExprVecIVFData::ObExprVecIVFData(ObIAllocator &allocator)
  : ObExprVecIVFBase(
      allocator, T_FUN_SYS_VEC_IVF_DATA, N_VEC_IVF_DATA, ObVecIVFExprRole::DATA)
{}

} // namespace sql
} // namespace oceanbase
