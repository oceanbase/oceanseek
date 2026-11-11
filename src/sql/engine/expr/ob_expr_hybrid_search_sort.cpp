/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_hybrid_search_sort.h"
#include "lib/json_type/ob_json_bin_agg_serializer.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/utility/ob_print_utils.h"
#include <inttypes.h>

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObExprHybridUniqueShardDoc::normalize_shard_doc(ObIAllocator &alloc, const ObString &value,
                                                   ObString &normalized)
{
  int ret = OB_SUCCESS;
  char *out = nullptr;
  if (value.length() != 32) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid __shard_doc length", K(ret), K(value.length()));
  } else if (nullptr == (out = static_cast<char *>(alloc.alloc(32)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate normalized hybrid __shard_doc", K(ret), K(value.length()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < 32; ++i) {
      char c = value.ptr()[i];
      if (c >= 'A' && c <= 'F') {
        c += 'a' - 'A';
      }
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid hexadecimal digit in hybrid __shard_doc", K(ret), K(i));
      } else {
        out[i] = c;
      }
    }
    if (OB_SUCC(ret)) {
      normalized.assign_ptr(out, 32);
    }
  }
  return ret;
}

int ObExprHybridUniqueShardDoc::encode_shard_doc(uint64_t tablet_id, uint64_t increment, char *out)
{
  int ret = OB_SUCCESS;
  // Format each uint64_t as 16 lowercase hex digits with leading zeros; reserve one extra byte for '\0'.
  // Example: tablet_id=7, increment=100 -> "00000000000000070000000000000064".
  char encoded[33];
  int64_t pos = 0;
  if (OB_ISNULL(out)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hybrid __shard_doc output buffer is null", K(ret));
  } else if (OB_FAIL(databuff_printf(encoded, sizeof(encoded), pos,
                                   "%016" PRIx64 "%016" PRIx64, tablet_id, increment))) {
    LOG_WARN("failed to format hybrid __shard_doc", K(ret));
  } else if (pos != 32) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hybrid __shard_doc length", K(ret), K(pos));
  } else {
    // ObDatum uses an explicit length: copy the 32 value bytes without the trailing '\0'.
    MEMCPY(out, encoded, 32);
  }
  return ret;
}

ObExprHybridUniqueShardDoc::ObExprHybridUniqueShardDoc(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC,
                       "hybrid_unique_shard_doc", 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  is_internal_for_mysql_ = true;
  need_charset_convert_ = false;
}

int ObExprHybridUniqueShardDoc::calc_result_type2(ObExprResType &type, ObExprResType &tablet,
                                                ObExprResType &increment, ObExprTypeCtx &ctx) const
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (tablet.get_type() != ObUInt64Type || increment.get_type() != ObUInt64Type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid __shard_doc input types", K(ret), K(tablet.get_type()), K(increment.get_type()));
  } else {
    type.set_varchar();
    type.set_length(32);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
  }
  return ret;
}

int ObExprHybridUniqueShardDoc::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(ctx, raw);
  expr.eval_func_ = eval_unique_shard_doc;
  if (expr.arg_cnt_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid __shard_doc expression argument count", K(ret), K(expr.arg_cnt_));
  }
  return ret;
}

int ObExprHybridUniqueShardDoc::eval_unique_shard_doc(const ObExpr &expr, ObEvalCtx &ctx,
                                                    ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *tablet = nullptr;
  ObDatum *increment = nullptr;
  char *buffer = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, tablet))) {
    LOG_WARN("failed to evaluate hybrid __shard_doc tablet ID", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, increment))) {
    LOG_WARN("failed to evaluate hybrid __shard_doc heap increment", K(ret));
  } else if (tablet->is_null() || increment->is_null() || tablet->get_uint() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null hybrid __shard_doc input or zero tablet ID", K(ret),
             "tablet_is_null", OB_ISNULL(tablet) || tablet->is_null(),
             "increment_is_null", OB_ISNULL(increment) || increment->is_null());
  } else if (nullptr == (buffer = expr.get_str_res_mem(ctx, 32))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid __shard_doc expression result", K(ret));
  } else if (OB_FAIL(ObExprHybridUniqueShardDoc::encode_shard_doc(tablet->get_uint(), increment->get_uint(), buffer))) {
    LOG_WARN("failed to encode hybrid search __shard_doc", K(ret));
  } else {
    result.set_string(buffer, 32);
  }
  return ret;
}

ObExprHybridSearchSortValues::ObExprHybridSearchSortValues(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_HYBRID_SEARCH_SORT_VALUES,
                       "hybrid_search_sort_values", PARAM_NUM_UNKNOWN, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  is_internal_for_mysql_ = true;
  need_charset_convert_ = false;
}

int ObExprHybridSearchSortValues::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                                  int64_t count, ObExprTypeCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid cursor sort value argument count", K(ret), K(count));
  } else {
    type.set_json();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length());
    const ObRawExpr *raw_expr = ctx.get_raw_expr();
    for (int64_t i = 0; i < count; ++i) {
      const ObRawExpr *arg = raw_expr != nullptr && i < raw_expr->get_param_count()
          ? raw_expr->get_param_expr(i) : nullptr;
      if (arg != nullptr && arg->get_expr_type() == T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC) {
        // __shard_doc is hexadecimal text despite its binary comparison collation.
      } else if (types[i].get_type() == ObTimeType) {
        // SQL CAST preserves the sign and fractional seconds without JSON's binary TIME encoding.
        types[i].set_calc_type(ObVarcharType);
        types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        types[i].set_calc_length(OB_MAX_TIME_STR_LENGTH);
      } else {
        ObJsonExprHelper::set_type_for_value(types, i);
        if (types[i].get_calc_type() == ObHexStringType) {
          // CAST produces HEX_STRING with binary collation; keep later formalize calls consistent.
          types[i].set_calc_collation_type(CS_TYPE_BINARY);
        }
      }
    }
  }
  return ret;
}

int ObExprHybridSearchSortValues::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const
{
  UNUSEDx(ctx, raw);
  expr.eval_func_ = eval_sort_values;
  return OB_SUCCESS;
}

int ObExprHybridSearchSortValues::eval_sort_values(const ObExpr &expr, ObEvalCtx &ctx,
                                                 ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard guard(ctx);
  MultimodeAlloctor allocator(guard.get_allocator(), expr.type_, ret, ctx, "HybridSort");
  ObJsonBinAggSerializer array(allocator, ObJsonNodeType::J_ARRAY);
  for (uint32_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum *datum = nullptr;
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(allocator.eval_arg(expr.args_[i], ctx, datum))) {
      LOG_WARN("failed to evaluate hybrid cursor output argument", K(ret), K(i), K(expr.arg_cnt_));
    } else if (expr.args_[i]->type_ == T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC) {
      // CS_TYPE_BINARY is the comparison collation, not a request for an opaque
      // JSON value. The __shard_doc value is always an ordinary 32-character string.
      ObString normalized;
      if (datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hybrid cursor __shard_doc argument is null", K(ret), K(i));
      } else if (OB_FAIL(ObExprHybridUniqueShardDoc::normalize_shard_doc(
                     allocator, datum->get_string(), normalized))) {
        LOG_WARN("failed to normalize hybrid cursor __shard_doc", K(ret), K(i));
      } else {
        ObJsonString shard_doc_value(normalized);
        if (OB_FAIL(array.append_value(ObJsonWrapper(&shard_doc_value)))) {
          LOG_WARN("failed to append __shard_doc to hybrid __sort", K(ret), K(i));
        }
      }
    } else if (datum->is_null()) {
      ObJsonNull null_value;
      if (OB_FAIL(array.append_value(ObJsonWrapper(&null_value)))) {
        LOG_WARN("failed to append null to hybrid __sort", K(ret), K(i));
      }
    } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &allocator, i, value))) {
      LOG_WARN("failed to convert hybrid sort argument to JSON", K(ret), K(i), K(expr.args_[i]->datum_meta_.type_));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid cursor JSON conversion returned a null value",
               K(ret), K(i), K(expr.args_[i]->datum_meta_.type_));
    } else if (OB_FAIL(array.append_value(ObJsonWrapper(value)))) {
      LOG_WARN("failed to append value to hybrid __sort", K(ret), K(i), K(value->json_type()));
    }
  }
  if (OB_SUCC(ret)) {
    ObStringBuffer binary(&allocator);
    if (OB_FAIL(array.serialize(binary))) {
      LOG_WARN("failed to serialize hybrid __sort JSON result", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, result, binary.string()))) {
      LOG_WARN("failed to pack hybrid __sort JSON result", K(ret), K(binary.length()));
    }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
