/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_GROUPS_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_GROUPS_H_

#include <type_traits>
#include <arrow/api.h>
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_traits.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_impl.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
namespace sql
{


// 数值组。直接从向量里读取定宽数值，不需要额外解码；
// OB/Arrow 底层类型一致时直接 UnsafeAppend，不一致时做 static_cast。
template <common::ObObjType OT, arrow::Type::type AT>
struct NumericGroup
{
  using Tr           = ObArrowConvTraits<OT, AT>;
  using ObCppType    = typename Tr::ObCppType;
  using ArrowCppType = typename Tr::ArrowCppType;
  using Builder      = typename Tr::Builder;
  static constexpr bool SAME_CPP_TYPE = std::is_same_v<ObCppType, ArrowCppType>;

  template <typename VEC>
  struct ConvFn
  {
    VEC     &vec_;
    Builder &builder_;

    ConvFn(const ObExpr & /*expr*/, VEC &vec, Builder &builder,
           common::ObIAllocator * /*lob_arena*/)
        : vec_(vec), builder_(builder)
    {}

    OB_INLINE int operator()(const ObExpr & /*expr*/, int64_t i)
    {
      const ObCppType val = *reinterpret_cast<const ObCppType *>(vec_.get_payload(i));
      if constexpr (SAME_CPP_TYPE) {
        builder_.UnsafeAppend(val);
      } else {
        builder_.UnsafeAppend(static_cast<ArrowCppType>(val));
      }
      return OB_SUCCESS;
    }
  };
};

// 日期时间组。普通类型直接透传，YEAR 需要补 1900，MySQLDate/MySQLDateTime
// 需要经过 ObTimeConverter 做历法转换，失败时直接返回错误。
template <common::ObObjType OT, arrow::Type::type AT>
struct DateGroup
{
  using Tr        = ObArrowConvTraits<OT, AT>;
  using ObCppType = typename Tr::ObCppType;
  using Builder   = typename Tr::Builder;

  template <typename VEC>
  struct ConvFn
  {
    VEC     &vec_;
    Builder &builder_;

    ConvFn(const ObExpr & /*expr*/, VEC &vec, Builder &builder,
           common::ObIAllocator * /*lob_arena*/)
        : vec_(vec), builder_(builder)
    {}

    OB_INLINE int operator()(const ObExpr & /*expr*/, int64_t i)
    {
      int ret = OB_SUCCESS;
      const ObCppType *raw = reinterpret_cast<const ObCppType *>(vec_.get_payload(i));
      if constexpr (OT == common::ObYearType) {
        const uint8_t raw_year = static_cast<uint8_t>(*raw);
        const int16_t arrow_val =
            (raw_year == 0) ? 0 : static_cast<int16_t>(raw_year + 1900);
        builder_.UnsafeAppend(arrow_val);
      } else if constexpr (OT == common::ObMySQLDateType) {
        int32_t d_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdate_to_date(*raw, d_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdate_to_date failed", K(ret), K(i));
        } else {
          builder_.UnsafeAppend(d_value);
        }
      } else if constexpr (OT == common::ObMySQLDateTimeType) {
        int64_t dt_value = 0;
        if (OB_SUCCESS != common::ObTimeConverter::mdatetime_to_datetime(*raw, dt_value, 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mdatetime_to_datetime failed", K(ret), K(i));
        } else {
          builder_.UnsafeAppend(dt_value);
        }
      } else {
        builder_.UnsafeAppend(*raw);
      }
      return ret;
    }
  };
};

// 字符串组。统一通过 ObTextStringHelper 解码 inrow/outrow/lob payload，
// 只有真实 payload 为空时才补 null；builder 类型在构造时预先判定。
template <common::ObObjType OT, arrow::Type::type AT>
struct StrGroup
{
  using Tr      = ObArrowConvTraits<OT, AT>;
  using Builder = typename Tr::Builder;

  template <typename VEC>
  struct ConvFn
  {
    VEC                    &vec_;
    arrow::ArrayBuilder    &builder_;
    common::ObIAllocator   *arena_;
    ObDatumMeta             datum_meta_;
    bool                    is_large_binary_;

    ConvFn(const ObExpr &expr, VEC &vec, Builder &builder, common::ObIAllocator *arena)
        : vec_(vec), builder_(builder), arena_(arena),
          datum_meta_(expr.datum_meta_),
          is_large_binary_(builder.type()->id() == arrow::Type::LARGE_BINARY)
    {}

    OB_INLINE int operator()(const ObExpr & /*expr*/, int64_t i)
    {
      int ret = OB_SUCCESS;
      arrow::Status status;
      common::ObString s;
      if (OB_ISNULL(arena_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null arena", K(ret), K(i));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     *arena_, &vec_, datum_meta_, s, i))) {
        LOG_WARN("read string data failed", K(ret), K(i));
      } else if (OB_ISNULL(s.ptr())) {
        if (OB_ARROW_FAIL(builder_.AppendNull())) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append null failed", K(ret), "st", status.ToString().c_str(), K(i));
        }
      } else if (is_large_binary_) {
        if (OB_ARROW_FAIL(static_cast<arrow::LargeBinaryBuilder &>(builder_).Append(
                reinterpret_cast<const uint8_t *>(s.ptr()),
                static_cast<int64_t>(s.length())))) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append binary failed", K(ret), "st", status.ToString().c_str(), K(i));
        }
      } else {
        if (OB_ARROW_FAIL(static_cast<arrow::StringBuilder &>(builder_).Append(
                s.ptr(), static_cast<int32_t>(s.length())))) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append string failed", K(ret), "st", status.ToString().c_str(), K(i));
        }
      }
      return ret;
    }
  };
};

// Json 组。先解码原始 payload，再用 ObJsonBin 打印成文本写入 Arrow；
// 解码或打印失败直接返回错误，只有空 payload 才补 null。
template <common::ObObjType OT, arrow::Type::type AT>
struct JsonGroup
{
  using Tr      = ObArrowConvTraits<OT, AT>;
  using Builder = typename Tr::Builder;

  template <typename VEC>
  struct ConvFn
  {
    VEC                  &vec_;
    Builder              &builder_;
    common::ObIAllocator *arena_;
    ObDatumMeta           datum_meta_;

    ConvFn(const ObExpr &expr, VEC &vec, Builder &builder, common::ObIAllocator *arena)
        : vec_(vec), builder_(builder), arena_(arena),
          datum_meta_(expr.datum_meta_)
    {}

    OB_INLINE int operator()(const ObExpr & /*expr*/, int64_t i)
    {
      int ret = OB_SUCCESS;
      arrow::Status status;
      common::ObString raw;
      if (OB_ISNULL(arena_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null arena", K(ret), K(i));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     *arena_, &vec_, datum_meta_, raw, i))) {
        LOG_WARN("read json data failed", K(ret), K(i));
      } else if (OB_ISNULL(raw.ptr()) || raw.length() == 0) {
        builder_.UnsafeAppendNull();
      } else {
        common::ObJsonBin j_bin(raw.ptr(), raw.length(), arena_);
        common::ObJsonBuffer jbuf(arena_);
        j_bin.set_seek_flag(true);
        if (OB_SUCCESS != j_bin.reset_iter()
            || OB_SUCCESS != j_bin.print(jbuf, true, raw.length())
            || jbuf.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json bin print failed", K(ret), K(i));
        } else if (OB_ARROW_FAIL(builder_.Append(
                       jbuf.ptr(), static_cast<int32_t>(jbuf.length())))) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append json failed", K(ret), "st", status.ToString().c_str(), K(i));
        }
      }
      return ret;
    }
  };
};

// Decimal 组。先把不同来源统一转成 decimal int，再按目标 decimal 的
// precision/scale 做换算，最后构造 Arrow 的 Decimal128/Decimal256 值。
template <common::ObObjType OT, arrow::Type::type AT>
struct DecimalGroup
{
  using Tr      = ObArrowConvTraits<OT, AT>;
  using Builder = typename Tr::Builder;

  template <typename VEC>
  struct ConvFn
  {
    const ObExpr         &expr_;
    VEC                  &vec_;
    arrow::ArrayBuilder  &builder_;   // 基类（运行期 Decimal128 或 256）
    int32_t               tgt_prec_;
    int32_t               tgt_scale_;
    int                   arrow_type_id_;

    ConvFn(const ObExpr &expr, VEC &vec, Builder &builder, common::ObIAllocator * /*arena*/)
        : expr_(expr), vec_(vec), builder_(builder),
          tgt_prec_(-1), tgt_scale_(-1), arrow_type_id_(-1)
    {
      if (OB_NOT_NULL(builder.type())) {
        arrow_type_id_ = static_cast<int>(builder.type()->id());
        if (arrow_type_id_ == static_cast<int>(arrow::Type::DECIMAL128)) {
          const arrow::Decimal128Type *t =
              static_cast<const arrow::Decimal128Type *>(builder.type().get());
          tgt_prec_ = t->precision();
          tgt_scale_ = t->scale();
        } else if (arrow_type_id_ == static_cast<int>(arrow::Type::DECIMAL256)) {
          const arrow::Decimal256Type *t =
              static_cast<const arrow::Decimal256Type *>(builder.type().get());
          tgt_prec_ = t->precision();
          tgt_scale_ = t->scale();
        }
      }
    }

    // Decimal128 直接按 LE word 构造，避免走通用字节倒序路径。
    OB_INLINE int emit_decimal128(arrow::Decimal128Builder &bld,
                                  const common::ObDecimalInt *decint,
                                  const int32_t out_bytes)
    {
      int ret = OB_SUCCESS;
      arrow::Status status;
      if (OB_ISNULL(decint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null decimal int", K(ret));
      } else if (out_bytes != 4 && out_bytes != 8 && out_bytes != 16) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("decimal width too large for decimal128", K(ret), K(out_bytes));
      } else {
        arrow::Decimal128 dec;
        switch (out_bytes) {
          case 4: {
            const int64_t v = static_cast<int64_t>(decint->int32_v_[0]);
            dec = arrow::Decimal128(v);
            break;
          }
          case 8: {
            dec = arrow::Decimal128(decint->int64_v_[0]);
            break;
          }
          case 16:
          default: {
            const uint64_t low  = decint->int128_v_[0].items_[0];
            const int64_t  high = static_cast<int64_t>(decint->int128_v_[0].items_[1]);
            dec = arrow::Decimal128(high, low);
            break;
          }
        }
        if (OB_ARROW_FAIL(bld.Append(dec))) {
          ret = arrow_status_to_ob_ret(status);
          LOG_WARN("append decimal128 failed", K(ret), "st", status.ToString().c_str());
        }
      }
      return ret;
    }

    // Decimal256 先扩到 32B，再按 LE word 直接构造。
    OB_INLINE int emit_decimal256(arrow::Decimal256Builder &bld,
                                  const common::ObDecimalInt *decint,
                                  const int32_t int_bytes)
    {
      int ret = OB_SUCCESS;
      arrow::Status status;
      constexpr int32_t arrow_bytes = 32;
      if (OB_ISNULL(decint)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null decimal int", K(ret));
      } else if (int_bytes > arrow_bytes) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("decimal width too large for decimal256", K(ret), K(int_bytes));
      } else {
        common::ObDecimalIntBuilder extended;
        extended.from(decint, int_bytes);
        if (extended.get_int_bytes() < arrow_bytes) {
          extended.extend(arrow_bytes);
        }
        if (OB_UNLIKELY(extended.get_int_bytes() != arrow_bytes)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("decimal extend to 32B failed", K(ret), "bytes", extended.get_int_bytes());
        } else {
          const common::int256_t &v = *extended.get_decimal_int()->int256_v_;
          const arrow::Decimal256::WordArray words = {
              v.items_[0], v.items_[1], v.items_[2], v.items_[3]};
          const arrow::Decimal256 dec(arrow::Decimal256::LittleEndianArray, words);
          if (OB_ARROW_FAIL(bld.Append(dec))) {
            ret = arrow_status_to_ob_ret(status);
            LOG_WARN("append decimal256 failed", K(ret), "st", status.ToString().c_str());
          }
        }
      }
      return ret;
    }

    OB_INLINE int operator()(const ObExpr & /*expr*/, int64_t i)
    {
      int ret = OB_SUCCESS;
      if (tgt_prec_ <= 0 || tgt_scale_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid target decimal meta", K(ret), K(i), K_(tgt_prec), K_(tgt_scale));
        return ret;
      }

      const char *ptr = nullptr;
      common::ObLength len = 0;
      vec_.get_payload(i, ptr, len);
      common::ObDatum datum(ptr, static_cast<uint32_t>(len), false);

      common::ObDecimalIntBuilder work, scaled;
      const common::ObDecimalInt *src_decint = nullptr;
      int32_t src_int_bytes = 0;
      common::ObScale src_scale = 0;

      if constexpr (OT == common::ObNumberType || OT == common::ObUNumberType) {
        const number::ObNumber &nmb = datum.get_number();
        src_scale = nmb.get_scale();
        common::ObDecimalInt *tmp_decint = nullptr;
        int32_t tmp_int_bytes = 0;
        if (OB_FAIL(wide::from_number(nmb, work, src_scale, tmp_decint, tmp_int_bytes))) {
          LOG_WARN("from_number failed", K(ret), K(i));
        } else {
          src_decint = tmp_decint;
          src_int_bytes = tmp_int_bytes;
        }
      } else if constexpr (OT == common::ObDecimalIntType) {
        src_decint = datum.get_decimal_int();
        src_int_bytes = static_cast<int32_t>(datum.len_);
        src_scale = static_cast<common::ObScale>(expr_.datum_meta_.scale_);
        if (OB_ISNULL(src_decint)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null decimal int payload", K(ret), K(i));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported decimal source type", K(ret), K(i));
      }

      if (OB_SUCC(ret)) {
        const common::ObPrecision out_prec = static_cast<common::ObPrecision>(tgt_prec_);
        const common::ObScale out_scale = static_cast<common::ObScale>(tgt_scale_);
        const int32_t out_int_bytes =
            wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
        const common::ObDecimalInt *out_decint = src_decint;
        int32_t out_bytes = src_int_bytes;

        if (sql::ObDatumCast::need_scale_decimalint(src_scale, src_int_bytes,
                                                    out_scale, out_int_bytes)) {
          if (OB_FAIL(sql::ObDatumCast::common_scale_decimalint(
                  src_decint, src_int_bytes, src_scale, out_scale, out_prec,
                  CM_NONE, scaled))) {
            LOG_WARN("common_scale_decimalint failed", K(ret), K(i));
          } else {
            out_decint = scaled.get_decimal_int();
            out_bytes = scaled.get_int_bytes();
          }
        }

        if (OB_SUCC(ret)) {
          if (arrow_type_id_ == static_cast<int>(arrow::Type::DECIMAL128)) {
            ret = emit_decimal128(
                static_cast<arrow::Decimal128Builder &>(builder_), out_decint, out_bytes);
          } else if (arrow_type_id_ == static_cast<int>(arrow::Type::DECIMAL256)) {
            ret = emit_decimal256(
                static_cast<arrow::Decimal256Builder &>(builder_), out_decint, out_bytes);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected arrow decimal type id", K(ret), K(i), K_(arrow_type_id));
          }
        }
      }
      return ret;
    }
  };
};

// Null 组与 VectorFormat 无关，直接批量追加 null。
template <common::ObObjType OT, arrow::Type::type AT>
inline int null_conv_fn(const ObExpr & /*expr*/, common::ObIVector & /*vec*/,
                        const ObBitVector & /*skip*/, const int64_t size,
                        const bool /*all_rows_active*/,
                        arrow::ArrayBuilder &builder, common::ObIAllocator * /*arena*/)
{
  using Builder = typename ObArrowConvTraits<OT, AT>::Builder;
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_ARROW_FAIL(static_cast<Builder &>(builder).AppendNulls(size))) {
    ret = arrow_status_to_ob_ret(status);
    LOG_WARN("append nulls failed", K(ret), "st", status.ToString().c_str(), K(size));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_GROUPS_H_
