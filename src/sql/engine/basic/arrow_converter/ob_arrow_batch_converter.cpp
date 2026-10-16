/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/arrow_converter/ob_arrow_batch_converter.h"

#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_continuous_format.h"
#include "share/vector/ob_discrete_format.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_uniform_format.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

// 分派表：命中时走向量化转换，miss 时回退到逐行 datum 路径。
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_register.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"

namespace oceanbase
{
namespace sql
{

template <typename CppType, typename Bld>
static int append_fixed_list_elems(arrow::FixedSizeListBuilder *list_bld,
                                   arrow::ArrayBuilder *elem_bld,
                                   const common::ObString &raw,
                                   int32_t dim)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (raw.length() >= static_cast<int64_t>(dim * static_cast<int32_t>(sizeof(CppType)))) {
    if (OB_ARROW_FAIL(list_bld->Append())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append list cell failed", K(ret), "st", status.ToString().c_str());
    }
    const CppType *vals = reinterpret_cast<const CppType *>(raw.ptr());
    auto *eb = static_cast<Bld *>(elem_bld);
    for (int32_t i = 0; OB_SUCC(ret) && i < dim; ++i) {
      if (OB_ARROW_FAIL(eb->Append(vals[i]))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append list element failed", K(ret), "st", status.ToString().c_str(), K(i));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collection payload shorter than declared layout", K(ret), K(raw.length()), K(dim));
  }
  return ret;
}

template <typename CppType, typename Bld>
static int append_var_list_elems(arrow::ListBuilder *list_bld,
                                 arrow::ArrayBuilder *elem_bld,
                                 const common::ObString &raw)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (raw.length() < static_cast<int64_t>(sizeof(uint32_t))) {
    ret = OB_ERR_UNEXPECTED;  // 集合列 raw 字节数 < 4（放不下 uint32 元素计数头）
  } else {
    const uint32_t arr_cnt = *reinterpret_cast<const uint32_t *>(raw.ptr());
    const char *body = raw.ptr() + sizeof(uint32_t);
    const int64_t body_len = raw.length() - static_cast<int64_t>(sizeof(uint32_t));
    const int64_t need = static_cast<int64_t>(arr_cnt)
                         * (1 + static_cast<int64_t>(sizeof(CppType)));
    if (body_len < need) {
      ret = OB_ERR_UNEXPECTED;  // 头之后的字节数 < 元素个数 × 每元素至少需要的字节
    } else {
      if (OB_ARROW_FAIL(list_bld->Append())) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append list cell failed", K(ret), "st", status.ToString().c_str());
      }
      const uint8_t *nulls = reinterpret_cast<const uint8_t *>(body);
      const CppType *vals = reinterpret_cast<const CppType *>(body + arr_cnt);
      for (uint32_t i = 0; OB_SUCC(ret) && i < arr_cnt; ++i) {
        if (nulls[i]) {
          if (OB_ARROW_FAIL(elem_bld->AppendNull())) {
            ret = arrow_status_to_ob_ret(status);
            LOG_WARN("append list null element failed", K(ret), "st", status.ToString().c_str(), K(i));
          }
        } else {
          if (OB_ARROW_FAIL(static_cast<Bld *>(elem_bld)->Append(vals[i]))) {
            ret = arrow_status_to_ob_ret(status);
            LOG_WARN("append list element failed", K(ret), "st", status.ToString().c_str(), K(i));
          }
        }
      }
    }
  }
  return ret;
}

// 按元素类型分发 FIXED_SIZE_LIST 追加逻辑。
static int dispatch_fixed_list_elem(arrow::Type::type elem_tid,
                                    arrow::FixedSizeListBuilder *list_bld,
                                    arrow::ArrayBuilder *elem_bld,
                                    const common::ObString &raw,
                                    int32_t dim)
{
  int ret = OB_SUCCESS;
  switch (elem_tid) {
    case arrow::Type::FLOAT:
      ret = append_fixed_list_elems<float, arrow::FloatBuilder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::DOUBLE:
      ret = append_fixed_list_elems<double, arrow::DoubleBuilder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT8:
      ret = append_fixed_list_elems<int8_t, arrow::Int8Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT8:
      ret = append_fixed_list_elems<uint8_t, arrow::UInt8Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT16:
      ret = append_fixed_list_elems<int16_t, arrow::Int16Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT16:
      ret = append_fixed_list_elems<uint16_t, arrow::UInt16Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT32:
      ret = append_fixed_list_elems<int32_t, arrow::Int32Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT32:
      ret = append_fixed_list_elems<uint32_t, arrow::UInt32Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::INT64:
      ret = append_fixed_list_elems<int64_t, arrow::Int64Builder>(list_bld, elem_bld, raw, dim);
      break;
    case arrow::Type::UINT64:
      ret = append_fixed_list_elems<uint64_t, arrow::UInt64Builder>(list_bld, elem_bld, raw, dim);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

// 按元素类型分发 LIST 追加逻辑。
static int dispatch_var_list_elem(arrow::Type::type elem_tid,
                                  arrow::ListBuilder *list_bld,
                                  arrow::ArrayBuilder *elem_bld,
                                  const common::ObString &raw)
{
  int ret = OB_SUCCESS;
  switch (elem_tid) {
    case arrow::Type::FLOAT:
      ret = append_var_list_elems<float, arrow::FloatBuilder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::DOUBLE:
      ret = append_var_list_elems<double, arrow::DoubleBuilder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT8:
      ret = append_var_list_elems<int8_t, arrow::Int8Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT8:
      ret = append_var_list_elems<uint8_t, arrow::UInt8Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT16:
      ret = append_var_list_elems<int16_t, arrow::Int16Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT16:
      ret = append_var_list_elems<uint16_t, arrow::UInt16Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT32:
      ret = append_var_list_elems<int32_t, arrow::Int32Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT32:
      ret = append_var_list_elems<uint32_t, arrow::UInt32Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::INT64:
      ret = append_var_list_elems<int64_t, arrow::Int64Builder>(list_bld, elem_bld, raw);
      break;
    case arrow::Type::UINT64:
      ret = append_var_list_elems<uint64_t, arrow::UInt64Builder>(list_bld, elem_bld, raw);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

static int append_collection_raw_to_builder(const common::ObString &raw,
                                            arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_ISNULL(raw.ptr())) {
    if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str());
    }
  } else {
    const arrow::Type::type tid = builder.type()->id();
    if (tid == arrow::Type::FIXED_SIZE_LIST) {
      auto *list_bld = static_cast<arrow::FixedSizeListBuilder *>(&builder);
      const std::shared_ptr<arrow::DataType> type_sptr = builder.type();
      const arrow::FixedSizeListType *list_type =
          static_cast<const arrow::FixedSizeListType *>(type_sptr.get());
      const int32_t dim = list_type->list_size();
      const arrow::Type::type elem_tid = list_type->value_type()->id();
      arrow::ArrayBuilder *elem_bld = list_bld->value_builder();
      if (OB_FAIL(dispatch_fixed_list_elem(elem_tid, list_bld, elem_bld, raw, dim))) {
        LOG_WARN("append fixed-size-list failed", K(ret), K(elem_tid), K(dim), K(raw.length()));
      }
    } else if (tid == arrow::Type::LIST) {
      auto *list_bld = static_cast<arrow::ListBuilder *>(&builder);
      const std::shared_ptr<arrow::DataType> type_sptr = builder.type();
      const arrow::ListType *list_type = static_cast<const arrow::ListType *>(type_sptr.get());
      const arrow::Type::type elem_tid = list_type->value_type()->id();
      arrow::ArrayBuilder *elem_bld = list_bld->value_builder();
      if (OB_FAIL(dispatch_var_list_elem(elem_tid, list_bld, elem_bld, raw))) {
        LOG_WARN("append list failed", K(ret), K(elem_tid), K(raw.length()));
      }
    } else if (tid == arrow::Type::LARGE_BINARY) {
      if (OB_ARROW_FAIL(static_cast<arrow::LargeBinaryBuilder &>(builder).Append(
              reinterpret_cast<const uint8_t *>(raw.ptr()), static_cast<int64_t>(raw.length())))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append large binary failed", K(ret), "st", status.ToString().c_str());
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported collection builder type", K(ret), K(tid));
    }
  }
  return ret;
}

static int append_collection_datum_to_builder(const ObExpr &expr,
                                              const common::ObDatum &datum,
                                              arrow::ArrayBuilder &builder,
                                              common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  common::ObString raw;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          arena, datum, expr.datum_meta_, raw))) {
    LOG_WARN("read collection payload failed", K(ret), K(expr.datum_meta_.type_));
  } else {
    ret = append_collection_raw_to_builder(raw, builder);
  }
  return ret;
}

static int append_string_datum_to_builder(const ObExpr &expr,
                                          const common::ObDatum &datum,
                                          arrow::ArrayBuilder &builder,
                                          common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  common::ObString s;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          arena, datum, expr.datum_meta_, s))) {
    LOG_WARN("read string payload failed", K(ret), K(expr.datum_meta_.type_));
  } else if (builder.type()->id() == arrow::Type::LARGE_BINARY) {
    if (OB_NOT_NULL(s.ptr())) {
      if (OB_ARROW_FAIL(static_cast<arrow::LargeBinaryBuilder &>(builder).Append(
              reinterpret_cast<const uint8_t *>(s.ptr()), static_cast<int64_t>(s.length())))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append binary failed", K(ret), "st", status.ToString().c_str());
      }
    } else if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str());
    }
  } else if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder).Append(
                 s.ptr(), static_cast<int32_t>(s.length())))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("append string failed", K(ret), "st", status.ToString().c_str());
  }
  return ret;
}

static int append_json_datum_to_builder(const ObExpr &expr,
                                        const common::ObDatum &datum,
                                        arrow::ArrayBuilder &builder,
                                        common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  common::ObString raw;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          arena, datum, expr.datum_meta_, raw))) {
    LOG_WARN("read json payload failed", K(ret));
  } else if (OB_ISNULL(raw.ptr()) || raw.length() == 0) {
    if (OB_ARROW_FAIL(builder.AppendNull())) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str());
    }
  } else {
    common::ObJsonBin j_bin(raw.ptr(), raw.length(), &arena);
    common::ObJsonBuffer jbuf(&arena);
    j_bin.set_seek_flag(true);
    if (OB_SUCCESS != j_bin.reset_iter()
        || OB_SUCCESS != j_bin.print(jbuf, true, raw.length())
        || jbuf.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json bin print failed", K(ret));
    } else if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder).Append(
                   jbuf.ptr(), static_cast<int32_t>(jbuf.length())))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append json failed", K(ret), "st", status.ToString().c_str());
    }
  }
  return ret;
}

static void arrow_get_decimal_meta(arrow::ArrayBuilder &builder,
                                   int32_t &arrow_prec,
                                   int32_t &arrow_scale,
                                   int &arrow_type_id)
{
  arrow_prec = -1;
  arrow_scale = -1;
  arrow_type_id = -1;
  if (OB_ISNULL(builder.type())) {
    return;
  }
  arrow_type_id = static_cast<int>(builder.type()->id());
  if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
    const arrow::Decimal128Type *t =
        static_cast<const arrow::Decimal128Type *>(builder.type().get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
    const arrow::Decimal256Type *t =
        static_cast<const arrow::Decimal256Type *>(builder.type().get());
    arrow_prec = t->precision();
    arrow_scale = t->scale();
  }
}

template <typename ArrowDecimalType, typename ArrowBuilderType>
static int append_scaled_decimal_int_to_arrow(ArrowBuilderType &builder,
                                              const common::ObDecimalInt *decint,
                                              const int32_t int_bytes)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  const int32_t arrow_bytes = static_cast<int32_t>(ArrowDecimalType::kByteWidth);
  common::ObDecimalIntBuilder extended;
  extended.from(decint, int_bytes);
  if (extended.get_int_bytes() < arrow_bytes) {
    extended.extend(arrow_bytes);
  }
  if (extended.get_int_bytes() > arrow_bytes) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decimal width too large", K(ret), "bytes", extended.get_int_bytes(), K(arrow_bytes));
  } else {
    const int32_t final_bytes = extended.get_int_bytes();
    const uint8_t *buffer = reinterpret_cast<const uint8_t *>(extended.get_decimal_int());
    uint8_t buf[ArrowDecimalType::kByteWidth] = {0};
    for (int32_t i = 0; i < final_bytes; ++i) {
      buf[arrow_bytes - 1 - i] = buffer[i];
    }
    const arrow::Result<ArrowDecimalType> dec_res =
        ArrowDecimalType::FromBigEndian(buf, arrow_bytes);
    if (!dec_res.ok()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("FromBigEndian failed", K(ret), "st", dec_res.status().ToString().c_str());
    } else if (OB_ARROW_FAIL(builder.Append(dec_res.ValueOrDie()))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("append decimal failed", K(ret), "st", status.ToString().c_str());
    }
  }
  return ret;
}

static int append_decimal_datum_to_builder(const ObExpr &expr,
                                           const common::ObDatum &datum,
                                           arrow::ArrayBuilder &builder)
{
  int ret = OB_SUCCESS;
  using namespace common;
  int32_t tgt_prec = -1;
  int32_t tgt_scale = -1;
  int arrow_type_id = -1;
  arrow_get_decimal_meta(builder, tgt_prec, tgt_scale, arrow_type_id);
  if (tgt_prec <= 0 || tgt_scale < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid target decimal meta", K(ret), K(tgt_prec), K(tgt_scale));
  } else {
    ObDecimalIntBuilder work;
    ObDecimalIntBuilder scaled;
    const ObDecimalInt *src_decint = nullptr;
    int32_t src_int_bytes = 0;
    ObScale src_scale = 0;

    if (ObNumberType == expr.datum_meta_.type_ || ObUNumberType == expr.datum_meta_.type_) {
      const number::ObNumber &nmb = datum.get_number();
      src_scale = nmb.get_scale();
      ObDecimalInt *tmp_decint = nullptr;
      int32_t tmp_int_bytes = 0;
      if (OB_FAIL(wide::from_number(nmb, work, src_scale, tmp_decint, tmp_int_bytes))) {
        LOG_WARN("from_number failed", K(ret));
      } else {
        src_decint = tmp_decint;
        src_int_bytes = tmp_int_bytes;
      }
    } else if (ObDecimalIntType == expr.datum_meta_.type_) {
      src_decint = datum.get_decimal_int();
      src_int_bytes = static_cast<int32_t>(datum.len_);
      src_scale = static_cast<ObScale>(expr.datum_meta_.scale_);
      if (OB_ISNULL(src_decint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null decimal int payload", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported decimal source type", K(ret), K(expr.datum_meta_.type_));
    }

    if (OB_SUCC(ret)) {
      const ObPrecision out_prec = static_cast<ObPrecision>(tgt_prec);
      const ObScale out_scale = static_cast<ObScale>(tgt_scale);
      const int32_t out_int_bytes =
          wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
      const ObDecimalInt *out_decint = src_decint;
      int32_t out_bytes = src_int_bytes;

      if (sql::ObDatumCast::need_scale_decimalint(src_scale, src_int_bytes, out_scale,
                                                  out_int_bytes)) {
        if (OB_FAIL(sql::ObDatumCast::common_scale_decimalint(
                src_decint, src_int_bytes, src_scale, out_scale, out_prec, CM_NONE, scaled))) {
          LOG_WARN("common_scale_decimalint failed", K(ret));
        } else {
          out_decint = scaled.get_decimal_int();
          out_bytes = scaled.get_int_bytes();
        }
      }

      if (OB_SUCC(ret)) {
        if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL128)) {
          ret = append_scaled_decimal_int_to_arrow<arrow::Decimal128, arrow::Decimal128Builder>(
              static_cast<arrow::Decimal128Builder &>(builder), out_decint, out_bytes);
        } else if (arrow_type_id == static_cast<int>(arrow::Type::DECIMAL256)) {
          ret = append_scaled_decimal_int_to_arrow<arrow::Decimal256, arrow::Decimal256Builder>(
              static_cast<arrow::Decimal256Builder &>(builder), out_decint, out_bytes);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow decimal type id", K(ret), K(arrow_type_id));
        }
      }
    }
  }
  return ret;
}

template <typename VecRef>
static int append_vec_by_datum(const ObExpr &expr, VecRef &vec,
                               const ObBitVector &skip, const int64_t size,
                               arrow::ArrayBuilder &builder,
                               common::ObIAllocator &arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  bool is_null = false;
  const char *payload = nullptr;
  common::ObLength length = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    if (skip.at(i)) {
      continue;
    }
    vec.get_payload(i, is_null, payload, length);
    if (is_null) {
      if (OB_ARROW_FAIL(builder.AppendNull())) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str(), K(i));
      }
    } else {
      const common::ObDatum datum(payload, static_cast<uint32_t>(length), false);
      if (OB_FAIL(ObArrowBatchConverter::append_datum_to_builder(expr, datum, builder, arena))) {
        LOG_WARN("append datum to builder failed", K(ret), K(i));
      }
    }
  }
  return ret;
}


int ObArrowBatchConverter::append_datum_to_builder(const ObExpr &expr,
                                                   const ObDatum &datum,
                                                   arrow::ArrayBuilder &builder,
                                                   common::ObIAllocator &lob_arena)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  using namespace common;
  if (datum.is_null()) {
    ret = arrow_status_to_ob_ret(builder.AppendNull());
  } else {
    switch (expr.datum_meta_.type_) {
      case ObNullType:
        ret = arrow_status_to_ob_ret(builder.AppendNull());
        break;
      case ObTinyIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int8Builder&>(builder).Append(
            datum.get_int8()));
        break;
      case ObSmallIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int16Builder&>(builder).Append(
            static_cast<int16_t>(datum.get_int())));
        break;
      case ObInt32Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int32Builder&>(builder).Append(
            static_cast<int32_t>(datum.get_int())));
        break;
      case ObIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int64Builder&>(builder).Append(
            datum.get_int()));
        break;
      case ObUTinyIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt8Builder&>(builder).Append(
            static_cast<uint8_t>(datum.get_uint())));
        break;
      case ObUSmallIntType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt16Builder&>(builder).Append(
            static_cast<uint16_t>(datum.get_uint())));
        break;
      case ObUInt32Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt32Builder&>(builder).Append(
            static_cast<uint32_t>(datum.get_uint())));
        break;
      case ObUInt64Type:
        ret = arrow_status_to_ob_ret(static_cast<arrow::UInt64Builder&>(builder).Append(
            datum.get_uint()));
        break;
      case ObFloatType:
      case ObUFloatType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::FloatBuilder&>(builder).Append(
            datum.get_float()));
        break;
      case ObDoubleType:
      case ObUDoubleType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::DoubleBuilder&>(builder).Append(
            datum.get_double()));
        break;
      case ObDateTimeType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder&>(builder).Append(
            datum.get_int()));
        break;
      case ObTimestampType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::TimestampBuilder&>(builder).Append(
            datum.get_int()));
        break;
      case ObDateType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Date32Builder&>(builder).Append(
            datum.get_date()));
        break;
      case ObTimeType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Time64Builder&>(builder).Append(
            datum.get_time()));
        break;
      case ObYearType: {
        const uint8_t raw = datum.get_year();
        const int16_t year_val = (raw == 0) ? 0 : static_cast<int16_t>(raw + 1900);
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int16Builder&>(builder).Append(year_val));
        break;
      }
      case ObNumberType:
      case ObUNumberType:
      case ObDecimalIntType:
        ret = append_decimal_datum_to_builder(expr, datum, builder);
        break;
      case ObBitType:
        ret = arrow_status_to_ob_ret(static_cast<arrow::Int64Builder&>(builder).Append(
            static_cast<int64_t>(datum.get_bit())));
        break;
      case ObMySQLDateType: {
        const ObMySQLDate md = datum.get_mysql_date();
        int32_t d_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdate_to_date(md, d_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdate_to_date failed", K(ret));
        } else {
          ret = arrow_status_to_ob_ret(static_cast<arrow::Date32Builder&>(builder).Append(d_value));
        }
        break;
      }
      case ObMySQLDateTimeType: {
        const ObMySQLDateTime mdt = datum.get_mysql_datetime();
        int64_t dt_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdatetime_to_datetime(mdt, dt_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdatetime_to_datetime failed", K(ret));
        } else {
          ret = arrow_status_to_ob_ret(
              static_cast<arrow::TimestampBuilder&>(builder).Append(dt_value));
        }
        break;
      }
      case ObVarcharType:
      case ObCharType:
      case ObNVarchar2Type:
      case ObNCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObEnumType:
      case ObSetType:
        ret = append_string_datum_to_builder(expr, datum, builder, lob_arena);
        break;
      case ObJsonType:
        ret = append_json_datum_to_builder(expr, datum, builder, lob_arena);
        break;
      case ObLobType: {
        common::ObString s;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                lob_arena, datum, expr.datum_meta_, s))) {
          LOG_WARN("read lob payload failed", K(ret));
        } else if (OB_NOT_NULL(s.ptr())) {
          ret = arrow_status_to_ob_ret(static_cast<arrow::LargeBinaryBuilder&>(builder).Append(
              reinterpret_cast<const uint8_t *>(s.ptr()), static_cast<int64_t>(s.length())));
        } else {
          ret = arrow_status_to_ob_ret(builder.AppendNull());
        }
        break;
      }
      case ObCollectionSQLType:
        ret = append_collection_datum_to_builder(expr, datum, builder, lob_arena);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported ob type for arrow conversion", K(ret), K(expr.datum_meta_.type_));
        break;
    }
  }
  return ret;
}

int ObArrowBatchConverter::append_brs_to_builders(
    const ExprFixedArray &output_exprs,
    ObEvalCtx &eval_ctx,
    const ObBatchRows &brs,
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders,
    common::ObIAllocator &lob_arena)
{
  int ret = OB_SUCCESS;
  const int64_t ncols = output_exprs.count();
  const int64_t nrows = brs.size_;
  if (OB_UNLIKELY(static_cast<int64_t>(builders.size()) != ncols)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder count mismatch", K(ret), "builders", builders.size(), K(ncols));
    // The callers construct builders from the result schema and expressions
    // from the physical operator.  They must remain one-to-one.  Do not
    // attempt conversion when that invariant has been broken: Arrow builders
    // use unchecked append paths internally, so continuing can corrupt memory.
  } else if (OB_SUCC(ret)) {
    for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
      ObExpr *expr = output_exprs.at(col);
      common::ObIVector *vec = nullptr;
      if (OB_ISNULL(expr) || OB_ISNULL(builders[col])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr or builder", K(ret), K(col));
      } else if (OB_FAIL(expr->eval_vector(eval_ctx, brs))) {
        LOG_WARN("eval_vector failed", K(ret), K(col));
      } else if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_vector failed", K(ret), K(col));
      } else {
        const common::ObObjType ob_t = expr->datum_meta_.type_;
        const arrow::Type::type ar_t = builders[col]->type()->id();
        // 从静态分发表中获取处理函数。
        ArrowConvFunc fn = lookup_conv(ob_t, ar_t);

        if (fn != nullptr) {
          // 命中静态分发表时走向量化路径。
          if (OB_FAIL(fn(*expr, *vec, *brs.skip_, nrows, brs.all_rows_active_,
                         *builders[col], &lob_arena))) {
            LOG_WARN("arrow converter fn failed — NOT retrying datum path; check LUT/group invariant",
                     K(ret), K(col), K(ob_t), K(ar_t));
          }
        } else {
          // 当前 (ob_t, ar_t) 未注册时回退到 datum 路径。
          if (OB_FAIL(append_vec_by_datum(*expr, *vec, *brs.skip_, nrows,
                                          *builders[col], lob_arena))) {
            LOG_WARN("datum fallback failed", K(ret), K(col));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
