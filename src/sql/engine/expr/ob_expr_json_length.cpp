/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_length.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonLength::ObExprJsonLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_LENGTH, N_JSON_LENGTH,
                         ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonLength::~ObExprJsonLength()
{
}

int ObExprJsonLength::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  UNUSED(type_ctx);

  // set result type to int32
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  // 0 position is json doc
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_LENGTH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  } else if (param_num > 1 && OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, 1))) {
    LOG_WARN("wrong type for json path.", K(ret), K(types_stack[1].get_type()));
  }

  return ret;
}

// for new sql engine
int ObExprJsonLength::eval_json_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObJsonMemCtx *json_json_ctx = NULL;
  if (OB_ISNULL(json_json_ctx =
                    static_cast<ObJsonMemCtx *>(ctx.exec_ctx_.get_expr_op_ctx(expr_ctx_id)))) {
    if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr_ctx_id, json_json_ctx))) {
      LOG_WARN("failed to create expr op ctx", K(ret), K(expr_ctx_id));
    } else {
      json_json_ctx->set_json_max_depth_config(ObJsonExprHelper::get_json_max_depth_config(ctx));
    }
  } else {
    json_json_ctx->reuse();
  }
  if (OB_SUCC(ret) && OB_ISNULL(json_json_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json json ctx is null", K(ret), K(expr_ctx_id));
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    if (OB_FAIL(json_json_ctx->init(expr.type_, tenant_id, true))) {
      LOG_WARN("failed to init json length ctx", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    MultimodeAlloctor *allocator = json_json_ctx->get_allocator();
    int32_t json_max_depth_config = json_json_ctx->get_json_max_depth_config();
    ObJsonPathCache *path_cache = json_json_ctx->get_path_cache();
    bool is_null = false;
    int32_t res_len = 0;
    ObJsonWrapper wrapper;
    ObDatum *json_doc = NULL;
    ObDatum *path = NULL;
    ObDatumMeta path_meta;
    ObExpr *arg0 = expr.args_[0];
    ObString json_bin;
    ObString path_bin;
    bool is_const = false;
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    bool enable_json_bin_view =
        OB_NOT_NULL(session) ? session->is_enable_json_bin_view() : true;

    if (OB_FAIL(allocator->eval_arg(arg0, ctx, json_doc))) {
      LOG_WARN("fail to eval json arg", K(ret), K(arg0->datum_meta_));
    } else if (expr.arg_cnt_ > 1) {
      ObExpr *arg1 = expr.args_[1];
      is_const = arg1->is_static_const_expr();
      path_meta = arg1->datum_meta_;
      if (OB_FAIL(allocator->eval_arg(arg1, ctx, path))) {
        LOG_WARN("fail to eval path arg", K(ret), K(path_meta));
      }
    }

    if (OB_SUCC(ret)) {
      ObObjType json_doc_type = arg0->datum_meta_.type_;
      ObCollationType json_doc_cs_type = arg0->datum_meta_.cs_type_;
      ObJsonInType j_in_type = ObJsonInType::JSON_BIN;

      if (json_doc_type == ObNullType || json_doc->is_null()) {
        is_null = true;
      } else if (json_doc_type != ObJsonType && !ob_is_string_type(json_doc_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("input type error", K(ret), K(json_doc_type));
      } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(json_doc_type, json_doc_cs_type))) {
        LOG_WARN("fail to ensure collation", K(ret), K(json_doc_type), K(json_doc_cs_type));
      } else if (OB_FALSE_IT(json_bin = json_doc->get_string())) {
      } else if (json_bin.length() == 0) {
        ret = OB_ERR_INVALID_JSON_TEXT;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                   *allocator, *json_doc, arg0->datum_meta_, json_bin))) {
        LOG_WARN("fail to get real data.", K(ret), K(json_bin));
      }

      if (OB_SUCC(ret) && !is_null) {
        allocator->add_baseline_size(json_bin.length());
        j_in_type = ObJsonExprHelper::get_json_internal_type(json_doc_type);
        if (OB_FAIL(common::get_json_wrapper(
                json_bin, j_in_type, *allocator, wrapper, 0,
                json_max_depth_config, enable_json_bin_view))) {
          LOG_WARN("fail to get json wrapper", K(ret), K(json_doc_type), K(json_bin));
        }
      }

      bool valid_path = false;
      if (OB_SUCC(ret) && !is_null && !OB_ISNULL(path)) {
        if (path->is_null() || path_meta.type_ == ObNullType) {
          is_null = true;
        } else {
          path_bin = path->get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  *allocator, *path, path_meta, path_bin))) {
            LOG_WARN("fail to get real data.", K(ret), K(path_bin));
          } else {
            valid_path = true;
          }
        }
      }

      if (OB_SUCC(ret) && !is_null && !valid_path) {
        uint64_t member_count = wrapper.member_count();
        if (OB_UNLIKELY(member_count > static_cast<uint64_t>(INT32_MAX))) {
          ret = OB_NUMERIC_OVERFLOW;
          LOG_WARN("member count overflow int32", K(ret), K(member_count));
        } else {
          res_len = static_cast<int32_t>(member_count);
        }
      } else if (OB_SUCC(ret) && !is_null) {
        ObSEArray<ObJsonWrapper, 4> hits;
        ObJsonPath *j_path = NULL;
        if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(
                       json_json_ctx->get_ctx_allocator(), path_cache, j_path, path_bin, 1, true, is_const))) {
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
          LOG_WARN("fail to parse json path", K(ret), K(path_meta.type_), K(path_bin));
        } else if (OB_FAIL(wrapper.seek(*j_path, *allocator, hits, true, false))) {
          LOG_WARN("fail to seek json node", K(ret), K(path_bin));
        } else if (hits.count() == 0) {
          is_null = true;
        } else if (hits.count() > 1) {
          res_len = static_cast<int32_t>(hits.count());
        } else {
          uint64_t member_count = hits.at(0).member_count();
          if (OB_UNLIKELY(member_count > static_cast<uint64_t>(INT32_MAX))) {
            ret = OB_NUMERIC_OVERFLOW;
            LOG_WARN("member count overflow int32", K(ret), K(member_count));
          } else {
            res_len = static_cast<int32_t>(member_count);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (is_null) {
          res.set_null();
        } else {
          res.set_int32(res_len);
        }
      }
    }
  }
  return ret;
}

int ObExprJsonLength::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_length;
  return OB_SUCCESS;
}


}
}
