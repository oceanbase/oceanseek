/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_calc_urowid.h"
#include "lib/encode/ob_base64_encode.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprCalcURowID::ObExprCalcURowID(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_CALC_UROWID, N_CALC_UROWID, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                   INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprCalcURowID::~ObExprCalcURowID() {}

int ObExprCalcURowID::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num < 1)
      || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for calc_result_typeN",
             K(ret), K(param_num), K(types), K(lib::is_mysql_mode()));
  } else {
    type.set_type(ObURowIDType);
    if (!lib::is_mysql_mode()) {
      // Oracle模式保持原有UROWID长度语义
      type.set_length(OB_MAX_USER_ROW_KEY_LENGTH);
    } else if (1 == param_num) {
      // Query Range为估行构造的单参数模拟表达式没有version，
      // 无法推导实际ROWID长度
      type.set_length(OB_MAX_USER_ROW_KEY_LENGTH);
    } else {
      ObLength max_content_length = 0;
      if (OB_FAIL(get_max_rowid_content_length(types, param_num, max_content_length))) {
        LOG_WARN("failed to get max rowid content length", K(ret), K(param_num));
      } else {
        type.set_length(max_content_length);
      }
    }
  }
  return ret;
}

int ObExprCalcURowID::get_max_rowid_content_length(ObExprResType *types,
                                                   const int64_t param_num,
                                                   ObLength &max_content_length)
{
  int ret = OB_SUCCESS;
  const int64_t max_persistable_content_length = ObBase64Encoder::needed_decoded_length(
      OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER - 1);
  int64_t version = ObURowIDData::INVALID_ROWID_VERSION;
  int64_t content_length = 0;
  int64_t real_rowkey_count = param_num - 1;
  int64_t value_start_idx = 1;
  int64_t remaining_rowkey_length = OB_MAX_USER_ROW_KEY_LENGTH;
  max_content_length = 0;
  if (OB_UNLIKELY(param_num < 2) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param_num), KP(types));
  } else {
    const bool has_literal_version = types[0].is_literal()
        && types[0].get_param().is_number()
        && types[0].get_param().get_number().is_valid_int64(version);
    const bool is_physical_heap_rowid = has_literal_version
        && ObURowIDData::HEAP_TABLE_ROWID_VERSION == version;
    const bool is_physical_ext_heap_rowid = has_literal_version
        && ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION == version;
    if (is_physical_heap_rowid || is_physical_ext_heap_rowid) {
      if (OB_UNLIKELY(3 != param_num)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid physical rowid argument count", K(ret), K(version), K(param_num));
      } else {
        content_length = is_physical_heap_rowid
            ? ObURowIDData::HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE
            : ObURowIDData::EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE;
      }
    } else {
      // 主键逻辑ROWID分别编码真实主键段和生成分区列段的列数。
      if (has_literal_version && ObURowIDData::PK_ROWID_VERSION == version) {
        value_start_idx = 2;
        content_length = ObURowIDData::PK_LOGICAL_ROWID_FIXED_SIZE;
        if (OB_UNLIKELY(param_num < 3)
            || OB_UNLIKELY(!types[1].is_literal() || !types[1].get_param().is_number())
            || OB_UNLIKELY(!types[1].get_param().get_number().is_valid_int64(real_rowkey_count))
            || OB_UNLIKELY(!ObURowIDData::is_valid_pk_count(real_rowkey_count))
            || OB_UNLIKELY(real_rowkey_count > param_num - value_start_idx)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid rowid real pk count",
                   K(ret), K(param_num), K(real_rowkey_count));
        } else {
          const int64_t generated_column_count =
              param_num - value_start_idx - real_rowkey_count;
          if (OB_UNLIKELY(!ObURowIDData::is_valid_generated_partition_column_count(
              generated_column_count))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid generated partition column count",
                     K(ret), K(param_num), K(real_rowkey_count), K(generated_column_count));
          }
        }
      } else {
        content_length = ObURowIDData::LOGICAL_ROWID_HEADER_SIZE;
      }
      for (int64_t i = value_start_idx; OB_SUCC(ret) && i < param_num; ++i) {
        const int64_t value_idx = i - value_start_idx;
        ObLength data_length = -1;
        const ObObjType data_type = types[i].get_type();
        if (lib::is_mysql_mode() && ObLongTextType == data_type) {
          // STRING的accuracy表示payload上界，ROWID中的规范化locator还需一字节LOB头。
          data_length = types[i].get_accuracy().get_length();
        } else if (OB_FAIL(types[i].get_length_for_meta_in_bytes(data_length))) {
          LOG_WARN("failed to get rowid parameter length", K(ret), K(i), K(types[i]));
        }
        if (OB_SUCC(ret)) {
          if (ObURowIDData::is_variable_length_pk_type(data_type)) {
            if (value_idx < real_rowkey_count) {
              if (data_length < 0) { // 推导阶段可能仍是未知值的长度预估
                data_length = remaining_rowkey_length;
              } else {
                data_length = MIN(data_length, remaining_rowkey_length);
              }
              remaining_rowkey_length -= data_length;
            } else if (data_length < 0) {
              // 生成分区列不受主键总长度限制，未知长度按其可表达的最大值保守推导
              data_length = OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER;
            }
            if (lib::is_mysql_mode()
                && value_idx >= real_rowkey_count
                && ObLongTextType == data_type) {
              data_length = MIN(data_length, static_cast<ObLength>(
                  OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER))
                  + static_cast<ObLength>(sizeof(ObLobCommon));
            }
          } else {
            data_length = 0;
          }
          const int64_t value_length = ObURowIDData::get_max_serialized_pk_value_size(
              data_type, data_length);
          if (OB_UNLIKELY(value_length < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid rowid value length", K(ret), K(value_length), K(i), K(data_type));
          } else if (value_length >= max_persistable_content_length - content_length) {
            // 理论上界超过LONGTEXT可保存范围时采用饱和值，实际值仍在运行时按需生成
            content_length = max_persistable_content_length;
          } else {
            content_length += value_length;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    max_content_length = static_cast<ObLength>(content_length);
  }
  return ret;
}

int ObExprCalcURowID::cg_expr(ObExprCGCtx &cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)
      || OB_UNLIKELY(ObURowIDType != rt_expr.datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_urowid;
  }
  return ret;
}

int ObExprCalcURowID::calc_urowid(const ObExpr &rt_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 > rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  } else {
    int64_t version = -1;
    int64_t real_rowkey_count = rt_expr.arg_cnt_ - 1;
    int64_t value_start_idx = 1;
    ObDatum *param = NULL;
    if (OB_FAIL(rt_expr.args_[0]->eval(eval_ctx, param))) {
      LOG_WARN("eval version arg failed", K(ret));
    } else {
      number::ObNumber ver_nmb(param->get_number());
      if (OB_UNLIKELY(!ver_nmb.is_valid_int64(version))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first arg must be version info(number type)", K(ret), K(ver_nmb));
      }
    }

    if (OB_SUCC(ret) && ObURowIDData::PK_ROWID_VERSION == version) {
      value_start_idx = 2;
      if (OB_UNLIKELY(rt_expr.arg_cnt_ < 3)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowid arg count", K(ret), K(rt_expr.arg_cnt_));
      } else if (OB_FAIL(rt_expr.args_[1]->eval(eval_ctx, param))) {
        LOG_WARN("eval rowid real pk count failed", K(ret));
      } else {
        number::ObNumber count_nmb(param->get_number());
        if (OB_UNLIKELY(!count_nmb.is_valid_int64(real_rowkey_count))
            || OB_UNLIKELY(!ObURowIDData::is_valid_pk_count(real_rowkey_count))
            || OB_UNLIKELY(real_rowkey_count > rt_expr.arg_cnt_ - value_start_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowid real pk count",
                   K(ret), K(real_rowkey_count), K(rt_expr.arg_cnt_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObObj> pk_vals;
      ObURowIDData urowid_data;
      ObArenaAllocator lob_allocator(
          ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (OB_FAIL(pk_vals.prepare_allocate(rt_expr.arg_cnt_ - value_start_idx))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
      for (int i = value_start_idx; OB_SUCC(ret) && i < rt_expr.arg_cnt_; i++) {
        const int64_t value_idx = i - value_start_idx;
        if (OB_FAIL(rt_expr.args_[i]->eval(eval_ctx, param))) {
          LOG_WARN("failed to eval", K(ret));
        } else if (OB_FAIL(param->to_obj(pk_vals.at(value_idx), rt_expr.args_[i]->obj_meta_))) {
          LOG_WARN("failed to transform datum to obj", K(ret));
        } else if (ObURowIDData::PK_ROWID_VERSION == version
                   && value_idx >= real_rowkey_count
                   && ObLongTextType == rt_expr.args_[i]->datum_meta_.type_
                   && !param->is_null()) {
          // 先从任意locator形态读取无头payload，再补固定的disk in-row头，
          // 保证相同生成分区STRING在ROWID中具有唯一且合法的LOB表示。
          ObString payload;
          if (0 == param->len_) {
            payload.assign_ptr("", 0);
          } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              lob_allocator, *param, rt_expr.args_[i]->datum_meta_,
              payload, &eval_ctx.exec_ctx_))) {
            LOG_WARN("failed to read lob rowid payload", K(ret), K(i));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObTextStringHelper::pack_to_disk_inrow_lob(
                lob_allocator, payload, ObLongTextType, pk_vals.at(value_idx)))) {
              LOG_WARN("failed to pack lob rowid payload", K(ret), K(i), K(payload.length()));
            } else {
              pk_vals.at(value_idx).set_collation_type(
                  rt_expr.args_[i]->obj_meta_.get_collation_type());
            }
          }
        }
      } // end for
      ObDataBuffer data_alloc;
      int64_t data_size = ObURowIDData::needed_content_buf_size(pk_vals, version);
      char *data_buf = NULL;
      if (OB_SUCC(ret)) {
        const int64_t rowid_string_size =
            ObURowIDData::needed_max_base64_buffer_size(data_size);
        if (OB_UNLIKELY(rowid_string_size > OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("rowid string size overflow", K(ret), K(data_size), K(rowid_string_size));
        } else if (OB_ISNULL(data_buf = rt_expr.get_str_res_mem(eval_ctx, data_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          data_alloc.set_data(data_buf, data_size);
          if (all_pk_is_null(version, pk_vals)) {
            // trigger before-row or outer join add extra null for rowid column will occur pks all are null.
            expr_datum.set_null();
          } else if (OB_FAIL(urowid_data.set_rowid_content(
              pk_vals, version, real_rowkey_count, data_alloc))) {
            LOG_WARN("failed to set rowid content", K(ret));
          } else {
            expr_datum.set_urowid(urowid_data);
          }
        }
      }
    }
  }
  return ret;
}

bool ObExprCalcURowID::all_pk_is_null(int64_t version, ObIArray<ObObj> &pk_vals)
{
  bool is_all = !pk_vals.empty();
  if (ObURowIDData::HEAP_TABLE_ROWID_VERSION == version ||
      ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION == version) {
    is_all = pk_vals.at(0).is_null();
  } else {
    for (int64_t i = 0; is_all && i < pk_vals.count(); ++i) {
      is_all &= pk_vals.at(i).is_null();
    }
  }
  return is_all;
}

} // end namespace sql
} // end namespace oceanbase
