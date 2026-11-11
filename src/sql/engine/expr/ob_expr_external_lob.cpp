/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/ob_cluster_version.h"
#include "ob_expr_external_lob.h"
#include "common/object/ob_object.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprExternalLob::ObExprExternalLob(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_EXTERNAL_LOB,
                         N_EXTERNAL_LOB,
                         MORE_THAN_TWO,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprExternalLob::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY((4 != param_num && 5 != param_num) || OB_ISNULL(types))) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString function_name(N_EXTERNAL_LOB);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, function_name.length(), function_name.ptr());
  } else if (OB_UNLIKELY(
                 (!types[0].is_string_type() && !types[0].is_null()
                  && ObUnknownType != types[0].get_type())
                 || (!types[1].is_numeric_type() && !types[1].is_null()
                     && ObUnknownType != types[1].get_type())
                 || (!types[2].is_numeric_type() && !types[2].is_null()
                     && ObUnknownType != types[2].get_type())
                 || (!types[3].is_string_type() && !types[3].is_null()
                     && ObUnknownType != types[3].get_type())
                 || (5 == param_num && !types[4].is_string_type()
                     && !types[4].is_null()
                     && ObUnknownType != types[4].get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_EXTERNAL_LOB);
  } else {
    type.set_type(ObExtendBLobType);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObExtendBLobType]);

    types[0].set_calc_type(ObVarcharType); // url
    types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types[1].set_calc_type(ObNumberType); // offset
    types[2].set_calc_type(ObNumberType); // length
    types[3].set_calc_type(ObVarcharType); // kind
    types[3].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (5 == param_num) {
      types[4].set_calc_type(ObVarcharType); // etag
      types[4].set_calc_collation_type(CS_TYPE_BINARY);
    }
  }
  return ret;
}

int ObExprExternalLob::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((4 != rt_expr.arg_cnt_ && 5 != rt_expr.arg_cnt_)
                  || OB_ISNULL(rt_expr.args_)
                  || OB_ISNULL(rt_expr.args_[0])
                  || OB_ISNULL(rt_expr.args_[1])
                  || OB_ISNULL(rt_expr.args_[2])
                  || OB_ISNULL(rt_expr.args_[3])
                  || (5 == rt_expr.arg_cnt_
                      && OB_ISNULL(rt_expr.args_[4])))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXTERNAL_LOB runtime arguments", K(ret),
             K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(T_FUN_SYS_EXTERNAL_LOB != raw_expr.get_expr_type()
             || ObRawExpr::EXPR_SYS_FUNC != raw_expr.get_expr_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected EXTERNAL_LOB raw expression", K(ret), K(raw_expr));
  } else if (OB_UNLIKELY(expr_cg_ctx.cur_cluster_version_ < CLUSTER_VERSION_4_6_3_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "EXTERNAL_LOB before cluster version 4.6.3.1");
  } else {
    rt_expr.eval_func_ = &ObExprExternalLob::eval_external_lob;
  }
  return ret;
}

int ObExprExternalLob::get_uint64_argument_(const ObDatum &datum,
                                            uint64_t &value)
{
  int ret = OB_SUCCESS;
  const number::ObNumber number(datum.get_number());
  value = 0;
  if (OB_UNLIKELY(!number.is_valid_uint64(value) || value > static_cast<uint64_t>(INT64_MAX))) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObExprExternalLob::parse_kind_(const ObString &kind_str,
                                   ObExternalBlobKind &kind)
{
  int ret = OB_SUCCESS;
  kind = ObExternalBlobKind::MAX;
  if (0 == kind_str.case_compare("reference")) {
    kind = ObExternalBlobKind::REFERENCE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_EXTERNAL_LOB);
  }
  return ret;
}

int ObExprExternalLob::eval_external_lob(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *url_datum = nullptr;
  ObDatum *offset_datum = nullptr;
  ObDatum *length_datum = nullptr;
  ObDatum *type_datum = nullptr;
  ObDatum *etag_datum = nullptr;
  // The expression requires 4 or 5 arguments and a non-null argument array.
  if (OB_UNLIKELY((4 != expr.arg_cnt_ && 5 != expr.arg_cnt_) || OB_ISNULL(expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXTERNAL_LOB expression", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, url_datum, offset_datum,
                     length_datum, type_datum, etag_datum))) {
    LOG_WARN("failed to evaluate EXTERNAL_LOB arguments", K(ret), K(expr.arg_cnt_));
  } else if (OB_ISNULL(url_datum) || OB_ISNULL(offset_datum)
             || OB_ISNULL(length_datum) || OB_ISNULL(type_datum)
             || (5 == expr.arg_cnt_ && OB_ISNULL(etag_datum))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("EXTERNAL_LOB argument datum is null", K(ret), KP(url_datum),
             KP(offset_datum), KP(length_datum), KP(type_datum), KP(etag_datum));
  } else if (OB_UNLIKELY(url_datum->is_null() || type_datum->is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_EXTERNAL_LOB);
    LOG_WARN("EXTERNAL_LOB argument is null", KR(ret), "url_is_null", url_datum->is_null(),
             "type_is_null", type_datum->is_null());
  } else if (OB_UNLIKELY(offset_datum->is_null() != length_datum->is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "external_lob: offset and length must be both NULL "
                   "(whole object) or both non-NULL (byte range)");
    LOG_WARN("EXTERNAL_LOB offset and length null states invalid", KR(ret), "offset_is_null",
             offset_datum->is_null(), "length_is_null", length_datum->is_null());
  } else {
    const ObString url = url_datum->get_string();
    const ObString etag = (5 == expr.arg_cnt_ && !etag_datum->is_null())
                          ? etag_datum->get_string()
                          : ObString::make_string("");
    uint64_t offset = 0;
    uint64_t length = 0;
    ObExternalBlobKind kind = ObExternalBlobKind::MAX;
    ObExternalBlobInfo external_lob_info;
    const bool is_null_length = length_datum->is_null();
    if (!is_null_length
        && (OB_FAIL(get_uint64_argument_(*offset_datum, offset))
        || OB_FAIL(get_uint64_argument_(*length_datum, length))
        || OB_UNLIKELY(length > INT64_MAX - offset)
        || OB_UNLIKELY(0 == length))) {
      // get_uint64_argument's errcode has only OB_INVALID_ARGUMENT
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_EXTERNAL_LOB);
      LOG_WARN("invalid EXTERNAL_LOB range", K(ret), K(offset), K(length));
    } else if (OB_FAIL(parse_kind_(type_datum->get_string(), kind))) {
      LOG_WARN("invalid EXTERNAL_LOB kind", K(ret), K(type_datum->get_string()));
    } else {
      // The builder copies argument bytes into a self-contained locator encoding.
      external_lob_info.url_ = url;
      external_lob_info.etag_ = etag;
      external_lob_info.offset_ = offset;
      external_lob_info.length_ = length;
      external_lob_info.has_length_ = !is_null_length;
      external_lob_info.kind_ = kind;
      // Allocate from the expression result area.
      // Keep the locator valid after the expression returns.
      ObExprStrResAlloc allocator(expr, ctx);
      storage::ObMemLobLocatorBuilder builder;
      ObLobLocatorV2 locator;
      if (OB_UNLIKELY(!external_lob_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_EXTERNAL_LOB);
        LOG_WARN("invalid EXTERNAL_LOB description", K(ret), K(external_lob_info));
      } else if (OB_FAIL(builder.init(allocator))) {
        LOG_WARN("failed to initialize EXTERNAL_LOB locator builder", K(ret), K(external_lob_info));
      } else if (OB_FAIL(builder.set_external_lob_info(external_lob_info))) {
        LOG_WARN("failed to set EXTERNAL_LOB info", K(ret), K(external_lob_info));
      } else if (OB_FAIL(builder.build(locator))) {
        LOG_WARN("failed to build EXTERNAL_LOB locator", K(ret), K(external_lob_info));
      } else {
        res_datum.set_string(locator.ptr_, locator.size_);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
