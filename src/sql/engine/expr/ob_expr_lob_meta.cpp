/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_lob_meta.h"
#include "common/object/ob_object.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/ob_cluster_version.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static int format_internal_lob_meta(ObIAllocator &allocator, uint64_t length, ObString &result)
{
  int ret = OB_SUCCESS;
  char buf[64];
  int64_t pos = 0;
  result.reset();
  if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
              "{\"kind\":\"internal\",\"length\":%lu}", length))) {
    LOG_WARN("failed to format internal lob metadata", KR(ret), K(length), K(pos));
  } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), result))) {
    LOG_WARN("failed to copy internal lob metadata", KR(ret), K(length), K(pos));
  }
  return ret;
}

static int format_external_lob_meta(ObIAllocator &allocator,
                                    const ObExternalBlobInfo &info,
                                    ObEvalCtx &ctx, ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob info", KR(ret), K(info.url_.length()), K(info.etag_.length()), K(info.has_length_));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc(ctx);
    ObJsonBuffer json_buf(&tmp_alloc.get_allocator());
    ObString formatted;
    // Format fixed fields in output order; the JSON API escapes URL and ETag.
    char fields[128];
    if (info.has_length_) {
      ret = databuff_printf(fields, sizeof(fields), ",\"offset\":%lu,\"length\":%lu",
                                                    info.offset_, info.length_);
    } else {
      ret = databuff_printf(fields, sizeof(fields), ",\"offset\":null,\"length\":null");
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to format external lob fields", KR(ret), K(info.offset_), K(info.length_), K(info.has_length_));
    } else if (OB_FAIL(json_buf.append("{\"kind\":\"external\",\"url\":"))) {
      LOG_WARN("failed to append external lob URL key", KR(ret), K(info.url_.length()));
    } else if (OB_FAIL(ObJsonBaseUtil::add_double_quote(json_buf, info.url_.ptr(),
                                                        info.url_.length()))) {
      LOG_WARN("failed to format external lob URL", KR(ret), K(info.url_.length()));
    } else if (OB_FAIL(json_buf.append(fields))) {
      LOG_WARN("failed to append external lob fields", KR(ret), K(info.offset_), K(info.length_), K(info.has_length_));
    } else if (!info.etag_.empty()) {
      if (OB_FAIL(json_buf.append(",\"etag\":"))) {
        LOG_WARN("failed to append external lob ETag key", KR(ret), K(info.etag_.length()));
      } else if (OB_FAIL(ObJsonBaseUtil::add_double_quote(json_buf, info.etag_.ptr(),
                                                          info.etag_.length()))) {
        LOG_WARN("failed to format external lob ETag", KR(ret), K(info.etag_.length()));
      }
    }
    // Copy full JSON to the caller's allocator, avoiding dependence on temporary memory.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_buf.append("}"))) {
      LOG_WARN("failed to finish external lob JSON", KR(ret), K(info.url_.length()), K(info.etag_.length()));
    } else if (OB_FAIL(json_buf.get_result_string(formatted))) {
      LOG_WARN("failed to get external lob metadata", KR(ret), K(info.url_.length()), K(info.etag_.length()));
    } else if (OB_FAIL(ob_write_string(allocator, formatted, result))) {
      LOG_WARN("failed to copy external lob metadata", KR(ret), K(formatted.length()));
    }
  }
  return ret;
}

ObExprLobMeta::ObExprLobMeta(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOB_META, N_LOB_META, 1,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprLobMeta::calc_result_type1(ObExprResType &type, ObExprResType &arg,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BYTE_PER_CHAR = 6;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(!ob_is_text_tc(arg.get_type()) && !arg.is_null())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("LOB_META requires a LOB argument", KR(ret), K(arg));
  } else {
    // Keep the input type to prevent an implicit CAST from materializing the LOB early.
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    // JSON escaping uses up to six bytes per input byte; add space for fixed keys and integers.
    type.set_length(MAX_BYTE_PER_CHAR * (OB_MAX_URI_LENGTH + OB_ETAG_CAPACITY) + 128);
  }
  return ret;
}

int ObExprLobMeta::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid LOB_META argument count", KR(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(expr_cg_ctx.cur_cluster_version_ < CLUSTER_VERSION_4_6_3_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_META before cluster version 4.6.3.1");
  } else {
    rt_expr.eval_func_ = eval_lob_meta;
  }
  return ret;
}

int ObExprLobMeta::eval_lob_meta(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("failed to evaluate LOB_META argument", KR(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    ObLobLocatorV2 locator(arg->get_string());
    ObExprStrResAlloc allocator(expr, ctx);
    ObString metadata;
    if (OB_UNLIKELY(!locator.is_valid() || locator.is_outline_disk_lob_locator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid LOB_META locator", KR(ret), K(locator.size_), K(expr.args_[0]->datum_meta_.type_));
    } else if (OB_UNLIKELY(locator.is_append_delta_lob())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_META on an append delta LOB");
    } else if (locator.is_mem_external_lob()) {
      ObExternalBlobInfo info;
      if (OB_FAIL(locator.get_external_lob_info(info))) {
        LOG_WARN("failed to get external lob metadata", KR(ret), K(locator.size_));
      } else if (OB_FAIL(format_external_lob_meta(allocator, info, ctx, metadata))) {
        LOG_WARN("failed to format external lob metadata", KR(ret));
      }
    } else {
      uint64_t length = 0;
      if (OB_FAIL(storage::ObLobManager::get_byte_length(locator, length))) {
        LOG_WARN("failed to get internal lob byte length", KR(ret), K(locator.size_));
      } else if (OB_FAIL(format_internal_lob_meta(allocator, length, metadata))) {
        LOG_WARN("failed to format internal lob metadata", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      res.set_string(metadata);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
