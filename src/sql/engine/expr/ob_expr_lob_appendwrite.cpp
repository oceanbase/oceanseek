/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_lob_appendwrite.h"
#include "common/object/ob_object.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_lob_access_utils.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprLobAppendWrite::ObExprLobAppendWrite(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_LOB_APPENDWRITE,
                         N_LOB_APPENDWRITE,
                         3,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprLobAppendWrite::calc_result_type3(ObExprResType &type,
                                            ObExprResType &lob,
                                            ObExprResType &amount,
                                            ObExprResType &buffer,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(ObLongTextType != lob.get_type() && ObExtendBLobType != lob.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("LOB_WRITEAPPEND requires LONGTEXT, LONGBLOB or EXTBLOB", K(ret), K(lob));
  } else if (OB_UNLIKELY(!(ObNullType == buffer.get_type()
                           || ob_is_string_type(buffer.get_type())
                           || ob_is_raw(buffer.get_type()) || ob_is_large_text(buffer.get_type())))) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("LOB_WRITEAPPEND requires a string or binary buffer", K(ret), K(buffer));
  } else {
    type = lob;
    amount.set_calc_type(ObIntType);
    buffer.set_calc_collation_type(lob.get_collation_type());
    buffer.set_calc_collation_level(lob.get_collation_level());
  }
  return ret;
}

int ObExprLobAppendWrite::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(T_FUN_SYS_LOB_APPENDWRITE != raw_expr.get_expr_type()
                         || ObRawExpr::EXPR_SYS_FUNC != raw_expr.get_expr_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected LOB_WRITEAPPEND raw expression", K(ret), K(raw_expr));
  } else if (OB_UNLIKELY(expr_cg_ctx.cur_cluster_version_ < CLUSTER_VERSION_4_6_2_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_WRITEAPPEND before cluster version 4.6.2.1");
    LOG_WARN("LOB_WRITEAPPEND is not supported before all observers "
              "are upgraded to 4.6.2.1",
              K(ret),
              "min_cluster_version", expr_cg_ctx.cur_cluster_version_,
              "required_version", CLUSTER_VERSION_4_6_2_1);
  } else {
    const ObSysFunRawExpr &func_expr =
        static_cast<const ObSysFunRawExpr &>(raw_expr);
    if (OB_UNLIKELY(!func_expr.is_lob_writeappend_assignment())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(
          OB_NOT_SUPPORTED,
          "LOB_WRITEAPPEND must be used directly as the value assigned "
          "to a column in an UPDATE statement");
      LOG_WARN("LOB_WRITEAPPEND is used outside a valid update assignment", K(ret),
               K(raw_expr));
    } else {
      rt_expr.eval_func_ = &ObExprLobAppendWrite::eval_lob_appendwrite;
    }
  }
  return ret;
}

int ObExprLobAppendWrite::eval_lob_appendwrite(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  char *result_buf = nullptr;
  if (OB_UNLIKELY(3 != expr.arg_cnt_)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])
      || OB_ISNULL(expr.args_[1])
      || OB_ISNULL(expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid LOB_WRITEAPPEND runtime arguments",
             K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed to evaluate LOB_WRITEAPPEND arguments", K(ret));
  } else {
    ObDatum &lob = expr.locate_param_datum(ctx, 0);
    ObDatum &amount_datum = expr.locate_param_datum(ctx, 1);
    ObDatum &buffer = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(lob.is_null() || amount_datum.is_null() || buffer.is_null())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("LOB_WRITEAPPEND arguments must not be null",
               KR(ret),
               "lob_is_null", lob.is_null(),
               "amount_is_null", amount_datum.is_null(),
               "buffer_is_null", buffer.is_null());
    } else {
      const int64_t amount = amount_datum.get_int();
      const ObString raw_buffer = buffer.get_string();
      ObString append_data = raw_buffer;
      const ObDatumMeta &lob_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &buffer_meta = expr.args_[2]->datum_meta_;
      const bool is_blob = ob_is_blob(lob_meta.type_, lob_meta.cs_type_);
      ObEvalCtx::TempAllocGuard tmp_alloc_guard(ctx);
      ObArenaAllocator &tmp_allocator = tmp_alloc_guard.get_allocator();
      ObTextStringIter buffer_iter(buffer_meta.type_,
                                   buffer_meta.cs_type_,
                                   raw_buffer);
      // 普通 VARCHAR/VARBINARY 参数的 Datum 中已经是实际数据，append_data 直接
      // 引用 raw_buffer 即可；LOB 参数的 Datum 中可能是 Locator，需要先初始化
      // 迭代器，再读取 Locator 所指向的完整 LOB 数据。
      //
      // buffer 为 VARCHAR 的示例：
      //   LOB_WRITEAPPEND(data, 3, 'ABC')
      // 此时 'ABC' 作为 buffer，类型为 VARCHAR。
      //
      // buffer 为 LOB 的示例：
      //   UPDATE t
      //      SET data = LOB_WRITEAPPEND(data, 6, REPEAT('!', 262139))
      //    WHERE id = 1;
      if (is_lob_storage(buffer_meta.type_)
          && OB_FAIL(buffer_iter.init(0,
                                     ctx.exec_ctx_.get_my_session(),
                                     &tmp_allocator))) {
        LOG_WARN("failed to initialize LOB_WRITEAPPEND buffer iterator", K(ret));
      } else if (is_lob_storage(buffer_meta.type_)
                 && OB_FAIL(buffer_iter.get_full_data(append_data))) {
        // TODO: 这里会物化所有的追加内容，因此会产生较大内存开销, 后期改成流式写
        LOG_WARN("failed to read LOB_WRITEAPPEND buffer", K(ret));
      }

      // BLOB 的 amount 以字节数为单位，字符型 LOB（如 LONGTEXT/CLOB）的
      // amount 以字符数为单位。
      // 前面的 LOB 读取失败时不再计算长度，交由后续 OB_FAIL(ret) 保留原错误。
      const int64_t actual_amount = OB_SUCCESS != ret ? 0 : (is_blob
          ? append_data.length()
          : ObCharset::strlen_char(buffer_meta.cs_type_,
                                   append_data.ptr(), append_data.length()));
      int64_t total_size = 0;

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(amount < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("LOB_WRITEAPPEND amount must not be negative", K(ret), K(amount));
      } else if (OB_UNLIKELY(amount != actual_amount)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("LOB_WRITEAPPEND amount does not match buffer length", K(ret), K(amount), K(actual_amount));
      } else if (OB_FAIL(ObLobLocatorV2::calc_append_delta_locator_size(
                     append_data.length(), total_size))) {
        LOG_WARN("LOB_WRITEAPPEND delta locator exceeds datum length limit",
                 K(ret), K(total_size), K(append_data.length()),
                 "header_len", ObLobLocatorV2::get_append_delta_header_len(),
                 K(OB_MAX_LONGTEXT_LENGTH));
      } else if (OB_ISNULL(result_buf = expr.get_str_res_mem(ctx, total_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate LOB_WRITEAPPEND result", K(ret), K(total_size));
      } else {
        ObMemLobCommon *mem_lob = new (result_buf) ObMemLobCommon(ObMemLobType::APPEND_DELTA_LOB);
        mem_lob->lob_common_.in_row_ = 0;
        mem_lob->set_read_only(true);
        mem_lob->set_has_extern(true);
        mem_lob->set_has_payload(true);
        mem_lob->disk_locator_size_ = 0;
        mem_lob->payload_offset_ = ObLobLocatorV2::get_append_delta_header_len();
        mem_lob->payload_size_ = append_data.length();

        ObMemLobExternHeader *extern_header =
            new (mem_lob->buffer_) ObMemLobExternHeader();
        extern_header->cs_type_ = lob_meta.cs_type_;
        if (!append_data.empty()) {
          MEMCPY(result_buf + mem_lob->payload_offset_,
                 append_data.ptr(), append_data.length());
        }
        expr_datum.set_string(result_buf, total_size);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
