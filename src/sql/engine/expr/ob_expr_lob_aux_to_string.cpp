/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_lob_aux_to_string.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/object/ob_object.h"
#include "share/ob_cluster_version.h"
#include "storage/lob/common/ob_lob_seq.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace sql
{

static int calc_lob_aux_to_string_result_type(ObExprResType &type,
                                              ObExprResType &input_type,
                                              ObExprTypeCtx &type_ctx,
                                              const char *func_name)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  uint64_t min_data_version = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret), K(func_name));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), min_data_version))) {
    LOG_WARN("failed to get minimum data version", K(ret), K(func_name));
  } else if (min_data_version < DATA_VERSION_4_6_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, func_name);
    LOG_WARN("LOB auxiliary key formatter is disabled before tenant upgrade completes",
             K(ret), K(func_name), K(min_data_version), K(DATA_VERSION_4_6_2_1));
  } else if (!input_type.is_null() && !input_type.is_varbinary()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("LOB auxiliary key should be VARBINARY type", K(ret), K(input_type));
  } else {
    input_type.set_calc_collation_type(CS_TYPE_BINARY);
    input_type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }
  return ret;
}

static int set_string_result(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res,
                             const char *buf,
                             const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  if (OB_FAIL(str_result.init(len))) {
    LOG_WARN("failed to init LOB auxiliary key string result", K(ret), K(len));
  } else if (OB_FAIL(str_result.append(buf, len))) {
    LOG_WARN("failed to append LOB auxiliary key string result", K(ret), K(len));
  } else {
    str_result.set_result();
  }
  return ret;
}

ObExprLobIdToString::ObExprLobIdToString(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOB_ID_TO_STRING, N_LOB_ID_TO_STRING, 1,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprLobIdToString::calc_result_type1(ObExprResType &type,
                                           ObExprResType &input_type,
                                           common::ObExprTypeCtx &type_ctx) const
{
  return calc_lob_aux_to_string_result_type(type, input_type, type_ctx, get_name());
}

int ObExprLobIdToString::eval_lob_id_to_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid LOB ID runtime arguments", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate LOB ID", K(ret));
  } else if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LOB ID datum is null", K(ret));
  } else if (input->is_null()) {
    res.set_null();
  } else {
    const ObString lob_id_buf = input->get_string();
    ObLobId lob_id;
    char str_buf[ObLobId::MAX_TO_STRING_BUF_SIZE];
    int64_t str_len = 0;
    if (OB_FAIL(lob_id.from_string(lob_id_buf))) {
      LOG_WARN("failed to parse LOB ID", K(ret), K(lob_id_buf.length()));
    } else if (OB_UNLIKELY(!lob_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid LOB ID", K(ret), K(lob_id));
    } else if (OB_UNLIKELY(0 >= (str_len = lob_id.to_string(str_buf, sizeof(str_buf))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to format LOB ID", K(ret), K(lob_id));
    } else if (OB_FAIL(set_string_result(expr, ctx, res, str_buf, str_len))) {
      LOG_WARN("failed to set LOB ID string result", K(ret), K(lob_id));
    }
  }
  return ret;
}

int ObExprLobIdToString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprLobIdToString::eval_lob_id_to_string;
  return OB_SUCCESS;
}

ObExprLobSeqIdToString::ObExprLobSeqIdToString(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOB_SEQ_ID_TO_STRING, N_LOB_SEQ_ID_TO_STRING, 1,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprLobSeqIdToString::calc_result_type1(ObExprResType &type,
                                              ObExprResType &input_type,
                                              common::ObExprTypeCtx &type_ctx) const
{
  return calc_lob_aux_to_string_result_type(type, input_type, type_ctx, get_name());
}

int ObExprLobSeqIdToString::eval_lob_seq_id_to_string(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid LOB sequence ID runtime arguments", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate LOB sequence ID", K(ret));
  } else if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LOB sequence ID datum is null", K(ret));
  } else if (input->is_null()) {
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    ObIAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    const ObString seq_id_buf = input->get_string();
    char *str_buf = nullptr;
    int64_t str_len = 0;
    const ObLobSeqId seq_id(seq_id_buf);
    const int64_t str_buf_len = seq_id.needed_to_string_buffer_size();
    if (OB_UNLIKELY(!seq_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid LOB sequence ID", K(ret), K(seq_id_buf.length()), K(seq_id));
    } else if (OB_ISNULL(str_buf = static_cast<char *>(tmp_allocator.alloc(str_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate LOB sequence ID string buffer", K(ret), K(str_buf_len));
    } else if (OB_UNLIKELY(0 >= (str_len = seq_id.to_string(str_buf, str_buf_len)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to format LOB sequence ID", K(ret), K(seq_id));
    } else if (OB_FAIL(set_string_result(expr, ctx, res, str_buf, str_len))) {
      LOG_WARN("failed to set LOB sequence ID string result", K(ret), K(seq_id));
    }
  }
  return ret;
}

int ObExprLobSeqIdToString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprLobSeqIdToString::eval_lob_seq_id_to_string;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
