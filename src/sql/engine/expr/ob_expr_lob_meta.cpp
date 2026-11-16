/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_lob_meta.h"
#include "common/object/ob_object.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_cluster_version.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// Copy scalar nodes; callers keep string buffers alive until serialization.
static int add_lob_meta_field(ObJsonObject &object, const char *key, const ObJsonScalar &value)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = object.get_allocator();
  ObJsonNode *node = value.clone(allocator);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate LOB_META JSON field", KR(ret), KCSTRING(key));
  }
  // Fields are unique and printed directly; preserve insertion order without sorted-key lookup.
  if (OB_SUCC(ret) && OB_FAIL(object.add(key, node, false, true, false))) {
    LOG_WARN("failed to add LOB_META JSON field", KR(ret), KCSTRING(key));
  }
  return ret;
}

static int add_lob_meta_hex_field(ObJsonObject &object, const char *key, const ObString &value)
{
  int ret = OB_SUCCESS;
  const int64_t buffer_length = 2 + static_cast<int64_t>(value.length()) * 2;
  char *buffer = static_cast<char *>(object.get_allocator()->alloc(buffer_length));
  int64_t pos = 2;
  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate LOB_META hex field", KR(ret), KCSTRING(key), K(buffer_length));
  } else {
    buffer[0] = '0';
    buffer[1] = 'x';
    if (OB_FAIL(hex_print(value.ptr(), value.length(), buffer, buffer_length, pos))) {
      LOG_WARN("failed to encode LOB_META hex field", KR(ret), KCSTRING(key), K(value.length()));
    } else if (OB_FAIL(add_lob_meta_field(object, key, ObJsonString(buffer, pos)))) {
      LOG_WARN("failed to add LOB_META hex field", KR(ret), KCSTRING(key));
    }
  }
  return ret;
}

static int append_lob_summary_fields(
    ObJsonObject &object,
    const storage::ObLobMetaInfo &summary,
    const storage::ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  const ObString raw_id = summary.lob_id_.get_string();
  const char *last_write_type = nullptr;
  if (OB_UNLIKELY(raw_id.length() != sizeof(ObLobId)
      || summary.lob_col_idx_ < 0
      || static_cast<uint64_t>(summary.lob_col_idx_) != summary.lob_id_.get_column_idx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid LOB_META summary identity", KR(ret), K(summary.lob_id_), K(summary.lob_col_idx_));
  } else {
    last_write_type = lob_last_write_type_to_string(extra_info.last_write_type_);
    ObIAllocator *allocator = object.get_allocator();
    ObJsonObject *lob_id_parsed = nullptr;
    // JSON string nodes borrow these buffers until the full object is serialized.
    const ObFastFormatInt *tablet_id =
        OB_NEWx(ObFastFormatInt, allocator, summary.lob_id_.get_tablet_id());
    const ObFastFormatInt *auto_inc_id =
        OB_NEWx(ObFastFormatInt, allocator, summary.lob_id_.get_auto_inc_id());
    if (OB_ISNULL(tablet_id) || OB_ISNULL(auto_inc_id)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate LOB_META identity strings", KR(ret));
    } else if (OB_ISNULL(lob_id_parsed = OB_NEWx(ObJsonObject, allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate LOB_META identity object", KR(ret));
    } else if (OB_FAIL(add_lob_meta_field(*lob_id_parsed, "column_idx",
        ObJsonUint(summary.lob_id_.get_column_idx())))) {
      LOG_WARN("failed to add LOB_META column_idx field", KR(ret), K(summary.lob_id_));
    } else if (OB_FAIL(add_lob_meta_field(*lob_id_parsed, "tablet_id",
        ObJsonString(tablet_id->ptr(), tablet_id->length())))) {
      LOG_WARN("failed to add LOB_META tablet_id field", KR(ret), K(summary.lob_id_));
    } else if (OB_FAIL(add_lob_meta_field(*lob_id_parsed, "auto_inc_id",
        ObJsonString(auto_inc_id->ptr(), auto_inc_id->length())))) {
      LOG_WARN("failed to add LOB_META auto_inc_id field", KR(ret), K(summary.lob_id_));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
        add_lob_meta_field(object, "lob_col_idx", ObJsonInt(summary.lob_col_idx_)))) {
      LOG_WARN("failed to add LOB_META lob_col_idx field", KR(ret), K(summary.lob_id_));
    } else if (OB_FAIL(add_lob_meta_hex_field(object, "lob_id", raw_id))) {
      LOG_WARN("failed to add LOB_META lob_id field", KR(ret), K(summary.lob_id_));
    } else if (OB_FAIL(object.add("lob_id_parsed", lob_id_parsed, false, true, false))) {
      LOG_WARN("failed to add LOB_META lob_id_parsed field", KR(ret), K(summary.lob_id_));
    } else if (OB_FAIL(add_lob_meta_field(object, "lob_chunk_type",
        ObJsonString(extra_info.lob_chunk_type_.to_string())))) {
      LOG_WARN("failed to add LOB_META lob_chunk_type field", KR(ret), K(summary.lob_id_));
    } else if (!extra_info.is_external_lob()) {
      if (OB_FAIL(add_lob_meta_field(object, "chunk_count", ObJsonUint(extra_info.chunk_count_)))) {
        LOG_WARN("failed to add LOB_META chunk_count field", KR(ret), K(summary.lob_id_));
      } else if (OB_FAIL(
          add_lob_meta_field(object, "last_write_type", ObJsonString(last_write_type)))) {
        LOG_WARN("failed to add LOB_META last_write_type field", KR(ret), K(summary.lob_id_));
      }
    }
  }
  return ret;
}

static int format_lob_meta(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObLobLocatorV2 &locator,
    const bool is_full,
    ObString &result)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc(ctx);
  ObIAllocator &allocator = tmp_alloc.get_allocator();
  ObExprStrResAlloc result_allocator(expr, ctx);
  ObJsonObject object(&allocator);
  ObJsonBuffer json_buf(&allocator);
  ObString formatted;
  const bool is_inrow = locator.is_inrow();
  result.reset();
  if (is_inrow) {
    uint64_t length = 0;
    if (OB_FAIL(storage::ObLobManager::get_byte_length(locator, length))) {
      LOG_WARN("failed to get inrow LOB_META byte length", KR(ret), K(locator.size_));
    } else if (OB_FAIL(add_lob_meta_field(object, "kind", ObJsonString("internal", 8)))) {
      LOG_WARN("failed to add inrow LOB_META kind", KR(ret));
    } else if (OB_FAIL(add_lob_meta_field(object, "length", ObJsonUint(length)))) {
      LOG_WARN("failed to add inrow LOB_META length", KR(ret), K(length));
    } else if (OB_FAIL(add_lob_meta_field(object, "storage", ObJsonString("inrow", 5)))) {
      LOG_WARN("failed to add inrow LOB_META storage", KR(ret));
    }
  } else if (OB_UNLIKELY(locator.is_lob_disk_locator() || !locator.is_persist_lob())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_META on a non-persistent or disk LOB");
    LOG_WARN("invalid LOB_META locator", KR(ret), K(locator));
  } else {
    const bool is_external = locator.is_mem_external_lob();
    if (is_external) {
      ObExternalBlobInfo external_info;
      ObJsonNull json_null;
      if (OB_FAIL(locator.get_external_lob_info(external_info))) {
        LOG_WARN("failed to decode LOB_META external descriptor", KR(ret), K(locator.size_));
      } else if (!external_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid LOB_META external descriptor", KR(ret), K(external_info));
      } else if (OB_FAIL(add_lob_meta_field(object, "kind", ObJsonString("external", 8)))) {
        LOG_WARN("failed to add external LOB_META kind", KR(ret));
      } else if (OB_FAIL(add_lob_meta_field(object, "url", ObJsonString(external_info.url_)))) {
        LOG_WARN("failed to add external LOB_META URL", KR(ret), K(external_info.url_.length()));
      }

      if (OB_FAIL(ret)) {
      } else if (external_info.has_length_) {
        if (OB_FAIL(add_lob_meta_field(object, "offset", ObJsonUint(external_info.offset_)))) {
          LOG_WARN("failed to add external LOB_META offset", KR(ret), K(external_info.has_length_));
        } else if (OB_FAIL(add_lob_meta_field(
            object, "length", ObJsonUint(external_info.length_)))) {
          LOG_WARN("failed to add external LOB_META length", KR(ret), K(external_info.has_length_));
        }
      } else {
        if (OB_FAIL(add_lob_meta_field(object, "offset", json_null))) {
          LOG_WARN("failed to add external LOB_META offset", KR(ret), K(external_info.has_length_));
        } else if (OB_FAIL(add_lob_meta_field(object, "length", json_null))) {
          LOG_WARN("failed to add external LOB_META length", KR(ret), K(external_info.has_length_));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!external_info.etag_.empty()) {
        if (OB_FAIL(add_lob_meta_field(object, "etag", ObJsonString(external_info.etag_)))) {
          LOG_WARN("failed to add external LOB_META ETag", KR(ret), K(external_info.etag_.length()));
        }
      }
    } else {
      uint64_t length = 0;
      if (OB_FAIL(storage::ObLobManager::get_byte_length(locator, length))) {
        LOG_WARN("failed to get outrow LOB_META byte length", KR(ret), K(locator.size_));
      } else if (OB_FAIL(add_lob_meta_field(object, "kind", ObJsonString("internal", 8)))) {
        LOG_WARN("failed to add outrow LOB_META kind", KR(ret));
      } else if (OB_FAIL(add_lob_meta_field(object, "length", ObJsonUint(length)))) {
        LOG_WARN("failed to add outrow LOB_META length", KR(ret), K(length));
      } else if (OB_FAIL(add_lob_meta_field(object, "storage", ObJsonString("outrow", 6)))) {
        LOG_WARN("failed to add outrow LOB_META storage", KR(ret));
      }
    }
    if (OB_SUCC(ret) && is_full) {
      storage::ObLobMetaInfo summary;
      storage::ObLobMetaExtraInfo extra_info;
      storage::ObLobReadContext read_ctx;
      storage::ObLobManager *lob_mgr = MTL(storage::ObLobManager *);
      ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(lob_mgr) || OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing LOB_META manager or session", KR(ret), KP(lob_mgr), KP(session));
      } else if (OB_FAIL(read_ctx.build(
          locator, allocator, 0, UINT64_MAX, session->get_tx_desc()))) {
        LOG_WARN("failed to build LOB_META read context", KR(ret), K(locator.size_));
      } else if (OB_FAIL(lob_mgr->get_lob_summary_info(read_ctx, summary))) {
        LOG_WARN("failed to read LOB_META summary", KR(ret), K(locator.size_));
      } else if (OB_FAIL(extra_info.deserialize(allocator, summary.lob_extra_info_))) {
        LOG_WARN("failed to decode LOB_META summary", KR(ret), K(summary.lob_id_));
      } else if (OB_FAIL(append_lob_summary_fields(object, summary, extra_info))) {
        LOG_WARN("failed to format LOB_META outrow fields", KR(ret), K(summary.lob_id_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(object.print(json_buf, false))) {
      LOG_WARN("failed to serialize LOB_META JSON", KR(ret), K(is_full), K(is_inrow));
    } else if (OB_FAIL(json_buf.get_result_string(formatted))) {
      LOG_WARN("failed to get LOB_META JSON result", KR(ret));
    } else if (OB_FAIL(ob_write_string(result_allocator, formatted, result))) {
      LOG_WARN("failed to copy LOB_META result", KR(ret), K(formatted.length()));
    }
  }
  return ret;
}

ObExprLobMeta::ObExprLobMeta(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOB_META, N_LOB_META, ONE_OR_TWO,
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
  if (OB_UNLIKELY(!is_lob_storage(arg.get_type()) && !arg.is_null())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("LOB_META requires a LOB argument", KR(ret), K(arg));
  } else {
    // Keep the input type to prevent an implicit CAST from materializing the LOB early.
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    // JSON escaping uses up to six bytes per input byte; add space for fixed keys and integers.
    type.set_length(MAX_BYTE_PER_CHAR * (OB_MAX_URI_LENGTH + OB_ETAG_CAPACITY) + 1024);
  }
  return ret;
}

int ObExprLobMeta::calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1 || param_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid LOB_META argument count", KR(ret), K(param_num));
  } else if (OB_FAIL(calc_result_type1(type, types[0], type_ctx))) {
    LOG_WARN("failed to deduce LOB_META result type", KR(ret));
  } else if (2 == param_num && !types[1].is_null()) {
    if (!ob_is_string_type(types[1].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("LOB_META mode requires a string", KR(ret), K(types[1]));
    } else {
      types[1].set_calc_type(ObVarcharType);
      types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  return ret;
}

int ObExprLobMeta::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
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
  ObDatum *mode = nullptr;
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 2)
      || OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[0])
      || (2 == expr.arg_cnt_ && OB_ISNULL(expr.args_[1]))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("failed to evaluate LOB_META argument", KR(ret));
  } else if (2 == expr.arg_cnt_ && OB_FAIL(expr.args_[1]->eval(ctx, mode))) {
    LOG_WARN("failed to evaluate LOB_META mode", KR(ret));
  } else if (arg->is_null() || (OB_NOT_NULL(mode) && mode->is_null())) {
    res.set_null();
  } else if (OB_NOT_NULL(mode) && 0 != mode->get_string().case_compare("FULL")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid LOB_META mode", KR(ret));
  } else {
    ObLobLocatorV2 locator(arg->get_string());
    ObString metadata;
    if (OB_UNLIKELY(!locator.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid LOB_META locator",
               KR(ret), K(locator.size_), K(expr.args_[0]->datum_meta_.type_));
    } else if (OB_UNLIKELY(locator.is_append_delta_lob())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_META on an append delta LOB");
    } else if (OB_FAIL(format_lob_meta(expr, ctx, locator, 2 == expr.arg_cnt_, metadata))) {
      LOG_WARN("failed to format LOB_META", KR(ret), K(locator.size_), K(expr.arg_cnt_));
    }
    if (OB_SUCC(ret)) {
      res.set_string(metadata);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
