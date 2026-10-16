/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_length.h"

#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/common/ob_lob_context.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprLength::ObExprLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LENGTH, N_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLength::~ObExprLength()
{
}

int ObExprLength::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (lib::is_oracle_mode()) {
      const ObAccuracy &acc =
        ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
      type.set_number();
      type.set_scale(acc.get_scale());
      type.set_precision(acc.get_precision());
      if (!text.is_string_type()) {
        text.set_calc_type(ObVarcharType);
        text.set_calc_collation_type(session->get_nls_collation());
        text.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      }
    } else {
      type.set_int();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
      if (ob_is_text_tc(text.get_type()) && (session->get_exec_min_cluster_version() >= CLUSTER_VERSION_4_1_0_0)) {
        // no need to do cast, save memory
      } else {
        text.set_calc_type(common::ObVarcharType);
      }
    }
    OX(ObExprOperator::calc_result_flag1(type, text));
  }
  return ret;
}

int ObExprLength::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("length expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of length expr is null", K(ret), K(rt_expr.args_));
  } else {
    ObObjType text_type = rt_expr.args_[0]->datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(text_type);

    if (!ob_is_castable_type_class(type_class)) {
      rt_expr.eval_func_ = ObExprLength::calc_null;
    } else if (lib::is_oracle_mode()) {
      rt_expr.eval_func_ = ObExprLength::calc_oracle_mode;
      rt_expr.eval_vector_func_ = ObExprLength::calc_oracle_length_vector;
    } else {
      if (op_cg_ctx.session_->get_exec_min_cluster_version() < CLUSTER_VERSION_4_1_0_0) {
        CK(ObVarcharType == text_type);
      }
      rt_expr.eval_func_ = ObExprLength::calc_mysql_mode;
      rt_expr.eval_vector_func_ = ObExprLength::calc_mysql_length_vector;
    }
  }
  return ret;
}

int ObExprLength::calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return OB_SUCCESS;
}
int ObExprLength::calc_oracle_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString m_text = text_datum->get_string();
    int64_t c_len = 0;
    if (!is_lob_storage(expr.args_[0]->datum_meta_.type_)) {
      c_len = ObCharset::strlen_char(expr.args_[0]->datum_meta_.cs_type_, m_text.ptr(),
                                    static_cast<int64_t>(m_text.length()));
    } else if (OB_FAIL(ObTextStringHelper::get_char_len(ctx, *text_datum, expr.args_[0]->datum_meta_,
                       c_len))) {
      LOG_WARN("failed to get char len for lob type", K(ret), K(expr.args_[0]->datum_meta_.type_));
    }
    if (OB_SUCC(ret)) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(static_cast<int64_t>(c_len), tmp_alloc))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        expr_datum.set_number(num);
      }
    }
  }
  return ret;
}
int ObExprLength::calc_mysql_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else if (!is_lob_storage(expr.args_[0]->datum_meta_.type_)) {
    expr_datum.set_int(static_cast<int64_t>(text_datum->len_));
  } else { // text tc only
    ObLobLocatorV2 locator(text_datum->get_string());
    uint64_t lob_data_byte_len = 0;
    if (OB_FAIL(ObLobManager::get_byte_length(locator, lob_data_byte_len))) {
      LOG_WARN("get lob data byte length failed", K(ret), K(locator));
    } else {
      expr_datum.set_int(static_cast<int64_t>(lob_data_byte_len));
    }
  }
  return ret;
}

int ObExprLength::get_outrow_lob_byte_length(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             const ObString &lob_str,
                                             int64_t &byte_length)
{
  int ret = OB_SUCCESS;
  byte_length = 0;
  ObLobLocatorV2 locator(lob_str);
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObLobReadContext read_ctx;
  uint64_t lob_byte_len = 0;
  int64_t timeout = 0;
  if (OB_ISNULL(lob_mngr) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null lob manager or session", K(ret), KP(lob_mngr), KP(session));
  } else if (OB_FAIL(session->get_query_timeout(timeout))) {
    LOG_WARN("failed to get query timeout", K(ret));
  } else {
    // build a query param from the result-row mem locator, then ask the lob
    // manager for the byte length (scans the lob meta table for outrow lob).
    timeout += session->get_query_start_time();
    if (OB_FAIL(lob_mngr->build_lob_read_context(read_ctx, tmp_alloc_g.get_allocator(),
                expr.args_[0]->datum_meta_.cs_type_, 0/*offset*/, UINT64_MAX/*len*/,
                timeout, locator, session->get_tx_desc(), nullptr/*lob_access_ctx*/))) {
      LOG_WARN("failed to build lob read context", K(ret), K(locator));
    } else {
      byte_length = static_cast<int64_t>(read_ctx.byte_size_);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprLength::calc_mysql_length_vector_dispatch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound) {
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = reinterpret_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool is_lob_storage = common::is_lob_storage(expr.args_[0]->datum_meta_.type_);
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      int64_t res_length = 0;
      if (!is_lob_storage) {
        res_length = arg_vec->get_length(idx);
      } else {
        const char *ptr = NULL;
        ObLength len = 0;
        arg_vec->get_payload(idx, ptr, len);
        const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(ptr);
        if (len != 0 && !lob->is_mem_loc_ && lob->in_row_) {
          // inrow disk locator: byte size is stored in the locator header
          res_length = lob->get_byte_size(len);
        } else {
          ObLobLocatorV2 locator(ObString(len, ptr));
          if (locator.has_inrow_data()) {
            // inrow (mem) locator: byte length can be read from the locator directly
            if (OB_FAIL(locator.get_lob_inrow_data_byte_len(res_length))) {
              LOG_WARN("get inrow lob byte length failed", K(ret), K(locator), K(len));
            }
          } else if (OB_FAIL(get_outrow_lob_byte_length(
                         expr, ctx, ObString(len, ptr), res_length))) {
            // outrow lob: the byte length cannot be read from the locator header
            // alone, it must be queried from the lob storage (lob meta table).
            LOG_WARN("failed to get outrow lob byte length", K(ret), K(idx), K(len));
          }
        }
      }
      if (OB_SUCC(ret)) {
        res_vec->set_int(idx, res_length);
      }
    }
  } // for end
  return ret;
}

int ObExprLength::calc_mysql_length_vector(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           const ObBitVector &skip,
                                           const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sin param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = calc_mysql_length_vector_dispatch<ObDiscreteFormat, ObFixedLengthFormat<int64_t>>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = calc_mysql_length_vector_dispatch<UniformFormat, ObFixedLengthFormat<int64_t>>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = calc_mysql_length_vector_dispatch<ObContinuousFormat, ObFixedLengthFormat<int64_t>>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_mysql_length_vector_dispatch<ObDiscreteFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_mysql_length_vector_dispatch<UniformFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_mysql_length_vector_dispatch<ObContinuousFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = calc_mysql_length_vector_dispatch<ObVectorBase, ObVectorBase>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprLength::calc_oracle_length_vector_dispatch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound) {
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = reinterpret_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString str = arg_vec->get_string(idx);
      int64_t c_len = 0;
      if (!is_lob_storage(expr.args_[0]->datum_meta_.type_)) {
        c_len = ObCharset::strlen_char(cs_type, str.ptr(), str.length());
      } else if (OB_FAIL(ObTextStringHelper::get_char_len(ctx, str, expr.args_[0]->datum_meta_,
                         c_len))) {
        LOG_WARN("failed to get char len for lob type", K(ret), K(expr.args_[0]->datum_meta_.type_));
      }
      if (OB_SUCC(ret)) {
        ObNumStackOnceAlloc tmp_alloc;
        number::ObNumber num;
        if (OB_FAIL(num.from(static_cast<int64_t>(c_len), tmp_alloc))) {
          LOG_WARN("copy number fail", K(ret));
        } else {
          res_vec->set_number(idx, num);
        }
      }
    }
  } // for end
  return ret;
}

int ObExprLength::calc_oracle_length_vector(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           const ObBitVector &skip,
                                           const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sin param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = calc_oracle_length_vector_dispatch<ObDiscreteFormat, ObDiscreteFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_oracle_length_vector_dispatch<ObDiscreteFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = calc_oracle_length_vector_dispatch<UniformFormat, ObDiscreteFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_oracle_length_vector_dispatch<UniformFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = calc_oracle_length_vector_dispatch<ObContinuousFormat, ObDiscreteFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = calc_oracle_length_vector_dispatch<ObContinuousFormat, UniformFormat>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = calc_oracle_length_vector_dispatch<ObVectorBase, ObVectorBase>(
        VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

}
}
