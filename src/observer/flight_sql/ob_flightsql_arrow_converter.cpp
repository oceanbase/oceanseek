/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_arrow_converter.h"
#include "observer/flight_sql/ob_flightsql_ingest_registry.h"
#include "observer/flight_sql/ob_flightsql_session.h"

#include <climits>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "lib/ob_errno.h"
#include "lib/number/ob_number_v2.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/udt/ob_array_fixed_size.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_subschema_ctx.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace observer
{
namespace sql = ::oceanbase::sql;

using sql::ObExpr;
using sql::ObEvalCtx;
using common::ObIAllocator;
using common::ObDatum;
using common::ObString;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static OB_INLINE ObDatum *init_uniform(ObExpr &expr, ObEvalCtx &ctx, int64_t nrows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, nrows))) {
    LOG_WARN("[FlightSQL] init_vector_for_write failed", K(ret));
    return nullptr;
  }
  return expr.locate_batch_datums(ctx);
}

// ---------------------------------------------------------------------------
// Integer converters (all OB integer types store as int64 in ObDatum)
// ---------------------------------------------------------------------------

// Fast path for fixed-width numeric Arrow arrays with no nulls: skip the
// per-element IsNull() bitmap lookup (common for analytics workloads).
// arr.null_count() is O(1) — Arrow caches it on the array object.
#define CONVERT_BATCH_FIXED_NO_NULL_(ArrType, DatumSetter, CppType)            \
  if (arr.null_count() == 0) {                                                  \
    const CppType *vals = arr.raw_values();                                     \
    for (int64_t i = 0; i < nrows; ++i) { datums[i].DatumSetter(vals[i]); }    \
  } else {                                                                      \
    for (int64_t i = 0; i < nrows; ++i) {                                       \
      if (arr.IsNull(i)) { datums[i].set_null(); }                              \
      else               { datums[i].DatumSetter(arr.Value(i)); }               \
    }                                                                            \
  }

static int convert_batch_int8(const arrow::Array &src, ObExpr &expr,
                               ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Int8Array &arr = static_cast<const arrow::Int8Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::Int8Array, set_int, int8_t)
  return ret;
}

static int convert_batch_int16(const arrow::Array &src, ObExpr &expr,
                                ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Int16Array &arr = static_cast<const arrow::Int16Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::Int16Array, set_int, int16_t)
  return ret;
}

static int convert_batch_int32(const arrow::Array &src, ObExpr &expr,
                                ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Int32Array &arr = static_cast<const arrow::Int32Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::Int32Array, set_int, int32_t)
  return ret;
}

static int convert_batch_int64(const arrow::Array &src, ObExpr &expr,
                                ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Int64Array &arr = static_cast<const arrow::Int64Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::Int64Array, set_int, int64_t)
  return ret;
}

static int convert_batch_uint8(const arrow::Array &src, ObExpr &expr,
                                ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::UInt8Array &arr = static_cast<const arrow::UInt8Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::UInt8Array, set_uint, uint8_t)
  return ret;
}

static int convert_batch_uint16(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::UInt16Array &arr = static_cast<const arrow::UInt16Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::UInt16Array, set_uint, uint16_t)
  return ret;
}

static int convert_batch_uint32(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::UInt32Array &arr = static_cast<const arrow::UInt32Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::UInt32Array, set_uint, uint32_t)
  return ret;
}

static int convert_batch_uint64(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::UInt64Array &arr = static_cast<const arrow::UInt64Array &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::UInt64Array, set_uint, uint64_t)
  return ret;
}

static int convert_batch_bool(const arrow::Array &src, ObExpr &expr,
                               ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::BooleanArray &arr = static_cast<const arrow::BooleanArray &>(src);
  // BooleanArray has no raw_values() pointer (bit-packed), keep element loop.
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) { datums[i].set_null(); }
    else               { datums[i].set_int(arr.Value(i) ? 1 : 0); }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Float converters
// ---------------------------------------------------------------------------

static int convert_batch_float32(const arrow::Array &src, ObExpr &expr,
                                  ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::FloatArray &arr = static_cast<const arrow::FloatArray &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::FloatArray, set_float, float)
  return ret;
}

static int convert_batch_float64(const arrow::Array &src, ObExpr &expr,
                                  ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::DoubleArray &arr = static_cast<const arrow::DoubleArray &>(src);
  CONVERT_BATCH_FIXED_NO_NULL_(arrow::DoubleArray, set_double, double)
  return ret;
}

#undef CONVERT_BATCH_FIXED_NO_NULL_

static int check_scalar_row_idx(const arrow::Array &src, const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= src.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] scalar Arrow row index is out of range",
             K(ret), K(row_idx), "array_length", src.length());
  }
  return ret;
}

#define DECL_CONVERT_DATUM_FIXED_(fname, ArrowArrayClass, DatumSetter)                  \
static int fname(const arrow::Array &src, const int64_t row_idx,                       \
                 const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,                    \
                 ObIAllocator &str_arena)                                                \
{                                                                                       \
  UNUSED(expr);                                                                          \
  UNUSED(ctx);                                                                           \
  UNUSED(str_arena);                                                                      \
  int ret = check_scalar_row_idx(src, row_idx);                                           \
  if (OB_SUCC(ret)) {                                                                     \
    const ArrowArrayClass &arr = static_cast<const ArrowArrayClass &>(src);               \
    if (arr.IsNull(row_idx)) { datum.set_null(); }                                        \
    else                     { datum.DatumSetter(arr.Value(row_idx)); }                   \
  }                                                                                       \
  return ret;                                                                             \
}

DECL_CONVERT_DATUM_FIXED_(convert_datum_int8,    arrow::Int8Array,   set_int)
DECL_CONVERT_DATUM_FIXED_(convert_datum_int16,   arrow::Int16Array,  set_int)
DECL_CONVERT_DATUM_FIXED_(convert_datum_int32,   arrow::Int32Array,  set_int)
DECL_CONVERT_DATUM_FIXED_(convert_datum_int64,   arrow::Int64Array,  set_int)
DECL_CONVERT_DATUM_FIXED_(convert_datum_uint8,   arrow::UInt8Array,  set_uint)
DECL_CONVERT_DATUM_FIXED_(convert_datum_uint16,  arrow::UInt16Array, set_uint)
DECL_CONVERT_DATUM_FIXED_(convert_datum_uint32,  arrow::UInt32Array, set_uint)
DECL_CONVERT_DATUM_FIXED_(convert_datum_uint64,  arrow::UInt64Array, set_uint)
DECL_CONVERT_DATUM_FIXED_(convert_datum_float32, arrow::FloatArray,  set_float)
DECL_CONVERT_DATUM_FIXED_(convert_datum_float64, arrow::DoubleArray, set_double)

#undef DECL_CONVERT_DATUM_FIXED_

static int convert_datum_bool(const arrow::Array &src, const int64_t row_idx,
                              const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                              ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::BooleanArray &arr = static_cast<const arrow::BooleanArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      datum.set_int(arr.Value(row_idx) ? 1 : 0);
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Decimal128 converters
// ---------------------------------------------------------------------------

// Arrow Decimal stores an unscaled integer plus the scale carried by its type.
// Before materializing an OB DecimalInt, align that source scale to the target
// expression scale, validate the target precision range, and only then resize
// the integer representation.  Resizing first can silently discard high bits.
static int rescale_decimal_int_to_target(const common::ObDecimalInt *source,
                                         const int32_t source_int_bytes,
                                         const int32_t source_precision,
                                         const int32_t source_scale,
                                         const ObExpr &target_expr,
                                         common::ObDecimalIntBuilder &result)
{
  int ret = OB_SUCCESS;
  const common::ObPrecision target_precision = target_expr.datum_meta_.precision_;
  const common::ObScale target_scale = target_expr.datum_meta_.scale_;
  if (OB_ISNULL(source) || source_int_bytes <= 0 || source_precision <= 0
      || source_scale < 0 || source_scale > source_precision
      || source_scale < INT16_MIN || source_scale > INT16_MAX
      || target_precision < OB_MIN_DECIMAL_PRECISION
      || target_precision > number::ObNumber::MAX_PRECISION
      || target_scale < 0 || target_scale > target_precision) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow or target decimal metadata", K(ret), KP(source),
             K(source_int_bytes), K(source_precision), K(source_scale),
             K(target_precision), K(target_scale));
  } else {
    common::ObDecimalIntBuilder scaled;
    if (OB_FAIL(wide::common_scale_decimalint(
            source, source_int_bytes, static_cast<common::ObScale>(source_scale),
            target_scale, scaled))) {
      LOG_WARN("[FlightSQL] failed to rescale Arrow decimal", K(ret),
               K(source_precision), K(source_scale), K(target_precision), K(target_scale));
    } else {
      const int32_t target_int_bytes =
          wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
      common::ObDecimalIntBuilder min_value;
      common::ObDecimalIntBuilder max_value;
      min_value.from(wide::ObDecimalIntConstValue::get_min_value(target_precision),
                     target_int_bytes);
      max_value.from(wide::ObDecimalIntConstValue::get_max_value(target_precision),
                     target_int_bytes);
      int cmp_min = 0;
      int cmp_max = 0;
      if (OB_FAIL(wide::compare(scaled, min_value, cmp_min))) {
        LOG_WARN("[FlightSQL] failed to compare rescaled decimal lower bound", K(ret));
      } else if (OB_FAIL(wide::compare(scaled, max_value, cmp_max))) {
        LOG_WARN("[FlightSQL] failed to compare rescaled decimal upper bound", K(ret));
      } else if (cmp_min < 0 || cmp_max > 0) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("[FlightSQL] Arrow decimal exceeds target precision", K(ret),
                 K(source_precision), K(source_scale), K(target_precision), K(target_scale));
      } else {
        result.from(scaled);
        if (result.get_int_bytes() < target_int_bytes) {
          result.extend(target_int_bytes);
        } else if (result.get_int_bytes() > target_int_bytes) {
          // Range validation above makes this narrowing lossless.
          result.truncate(target_int_bytes);
        }
      }
    }
  }
  return ret;
}

// The formatted decimal is only needed while ObNumber parses it. Keep this
// buffer in the caller's batch frame instead of materializing a C++ string
// for every row.
static int decimal_int_to_number(const common::ObDecimalInt *decimal_int,
                                 const int32_t int_bytes,
                                 const int32_t scale,
                                 char *buf,
                                 const int64_t buf_len,
                                 ObDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(decimal_int) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] decimal conversion buffer is null", K(ret), KP(decimal_int), KP(buf));
  } else if (OB_FAIL(wide::to_string(decimal_int, int_bytes, scale, buf, buf_len, len))) {
    LOG_WARN("[FlightSQL] failed to format decimal integer", K(ret), K(int_bytes), K(scale));
  } else {
    number::ObNumber nmb;
    sql::ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(nmb.from(buf, len, tmp_alloc))) {
      const ObString in_str(static_cast<int32_t>(len), buf);
      LOG_WARN("[FlightSQL] failed to parse decimal string to number", K(ret), K(in_str));
    } else {
      // ObDatum::set_number() deep-copies nmb, so tmp_alloc can remain stack-local.
      datum.set_number(nmb);
    }
  }
  return ret;
}

static int convert_batch_decimal128_to_decint(const arrow::Array &src, ObExpr &expr,
                                               ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Decimal128Array &arr = static_cast<const arrow::Decimal128Array &>(src);
  const arrow::Decimal128Type &type =
      static_cast<const arrow::Decimal128Type &>(*arr.type());
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      // Arrow Decimal128 is little-endian 128-bit integer.
      const int128_t v = *reinterpret_cast<const int128_t *>(arr.GetValue(i));
      ObDecimalIntBuilder source;
      ObDecimalIntBuilder result;
      source.from(v);
      if (OB_FAIL(rescale_decimal_int_to_target(
              source.get_decimal_int(), source.get_int_bytes(), type.precision(), type.scale(),
              expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal128 to target DecimalInt", K(ret), K(i));
      } else {
        datums[i].set_decimal_int(result.get_decimal_int(), result.get_int_bytes());
      }
    }
  }
  return ret;
}

static int convert_batch_decimal128_to_number(const arrow::Array &src, ObExpr &expr,
                                               ObEvalCtx &ctx, int64_t nrows,
                                               ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Decimal128Array &arr = static_cast<const arrow::Decimal128Array &>(src);
  const arrow::Decimal128Type &type =
      static_cast<const arrow::Decimal128Type &>(*arr.type());
  char decimal_buf[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      const int128_t value = *reinterpret_cast<const int128_t *>(arr.GetValue(i));
      ObDecimalIntBuilder decimal_builder;
      decimal_builder.from(value);
      if (OB_FAIL(decimal_int_to_number(
              decimal_builder.get_decimal_int(), decimal_builder.get_int_bytes(), type.scale(),
              decimal_buf, sizeof(decimal_buf), datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal128 to Number", K(ret), K(i));
      }
    }
  }
  return ret;
}

// Arrow Decimal256 and OceanBase int256_t both use a 32-byte, little-endian
// two's-complement integer representation.  MySQL DECIMAL precision above 38
// therefore stays on a binary DecimalInt path; do not format and re-parse it.
static int convert_batch_decimal256_to_decint(const arrow::Array &src, ObExpr &expr,
                                               ObEvalCtx &ctx, int64_t nrows,
                                               ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Decimal256Array &arr = static_cast<const arrow::Decimal256Array &>(src);
  const arrow::Decimal256Type &type =
      static_cast<const arrow::Decimal256Type &>(*arr.type());
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int256_t value;
      MEMCPY(value.items_, arr.GetValue(i), sizeof(value));
      ObDecimalIntBuilder source;
      ObDecimalIntBuilder result;
      source.from(value);
      if (OB_FAIL(rescale_decimal_int_to_target(
              source.get_decimal_int(), source.get_int_bytes(), type.precision(), type.scale(),
              expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal256 to target DecimalInt", K(ret), K(i));
      } else {
        datums[i].set_decimal_int(result.get_decimal_int(), result.get_int_bytes());
      }
    }
  }
  return ret;
}

static int convert_batch_decimal256_to_number(const arrow::Array &src, ObExpr &expr,
                                               ObEvalCtx &ctx, int64_t nrows,
                                               ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Decimal256Array &arr = static_cast<const arrow::Decimal256Array &>(src);
  const arrow::Decimal256Type &type =
      static_cast<const arrow::Decimal256Type &>(*arr.type());
  char decimal_buf[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int256_t value;
      MEMCPY(value.items_, arr.GetValue(i), sizeof(value));
      ObDecimalIntBuilder decimal_builder;
      decimal_builder.from(value);
      if (OB_FAIL(decimal_int_to_number(
              decimal_builder.get_decimal_int(), decimal_builder.get_int_bytes(), type.scale(),
              decimal_buf, sizeof(decimal_buf), datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal256 to Number", K(ret), K(i));
      }
    }
  }
  return ret;
}

template <typename DecimalArray, typename DecimalType, typename ValueType>
static int convert_datum_decimal_to_decint(const arrow::Array &src,
                                           const int64_t row_idx,
                                           const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &datum,
                                           ObIAllocator &str_arena)
{
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const DecimalArray &arr = static_cast<const DecimalArray &>(src);
    const DecimalType &type = static_cast<const DecimalType &>(*arr.type());
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      ValueType value;
      MEMCPY(&value, arr.GetValue(row_idx), sizeof(value));
      ObDecimalIntBuilder source;
      ObDecimalIntBuilder result;
      source.from(value);
      if (OB_FAIL(rescale_decimal_int_to_target(
              source.get_decimal_int(), source.get_int_bytes(),
              type.precision(), type.scale(), expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert scalar Arrow decimal to DecimalInt",
                 K(ret), K(row_idx));
      } else {
        datum.set_decimal_int(result.get_decimal_int(), result.get_int_bytes());
      }
    }
  }
  return ret;
}

template <typename DecimalArray, typename DecimalType, typename ValueType>
static int convert_datum_decimal_to_number(const arrow::Array &src,
                                           const int64_t row_idx,
                                           const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &datum,
                                           ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const DecimalArray &arr = static_cast<const DecimalArray &>(src);
    const DecimalType &type = static_cast<const DecimalType &>(*arr.type());
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      ValueType value;
      MEMCPY(&value, arr.GetValue(row_idx), sizeof(value));
      ObDecimalIntBuilder decimal_builder;
      decimal_builder.from(value);
      char decimal_buf[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
      if (OB_FAIL(decimal_int_to_number(
              decimal_builder.get_decimal_int(), decimal_builder.get_int_bytes(),
              type.scale(), decimal_buf, sizeof(decimal_buf), datum))) {
        LOG_WARN("[FlightSQL] failed to convert scalar Arrow decimal to Number",
                 K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

static int convert_datum_decimal128_to_decint(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_decint<
      arrow::Decimal128Array, arrow::Decimal128Type, int128_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal128_to_number(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_number<
      arrow::Decimal128Array, arrow::Decimal128Type, int128_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal256_to_decint(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_decint<
      arrow::Decimal256Array, arrow::Decimal256Type, int256_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal256_to_number(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_number<
      arrow::Decimal256Array, arrow::Decimal256Type, int256_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

// ---------------------------------------------------------------------------
// String / Binary converters (deep copy into str_arena)
// ---------------------------------------------------------------------------

static int convert_batch_string(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::StringArray &arr = static_cast<const arrow::StringArray &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int32_t len = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(i, &len));
      ObString src_str(len, raw);
      ObString dst_str;
      if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
        LOG_WARN("[FlightSQL] failed to copy string datum", K(ret), K(len), K(i));
      } else {
        datums[i].set_string(dst_str);
      }
    }
  }
  return ret;
}

static int convert_batch_large_string(const arrow::Array &src, ObExpr &expr,
                                       ObEvalCtx &ctx, int64_t nrows, ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::LargeStringArray &arr = static_cast<const arrow::LargeStringArray &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int64_t len64 = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(i, &len64));
      if (OB_UNLIKELY(len64 > INT32_MAX)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[FlightSQL] string value too large", K(ret), K(len64), K(i));
      } else {
        ObString src_str(static_cast<int32_t>(len64), raw);
        ObString dst_str;
        if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
          LOG_WARN("[FlightSQL] failed to copy large string datum", K(ret), K(len64), K(i));
        } else {
          datums[i].set_string(dst_str);
        }
      }
    }
  }
  return ret;
}

static int convert_batch_binary(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::BinaryArray &arr = static_cast<const arrow::BinaryArray &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int32_t len = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(i, &len));
      ObString src_str(len, raw);
      ObString dst_str;
      if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
        LOG_WARN("[FlightSQL] failed to copy binary datum", K(ret), K(len), K(i));
      } else {
        datums[i].set_string(dst_str);
      }
    }
  }
  return ret;
}

static int convert_batch_large_binary(const arrow::Array &src, ObExpr &expr,
                                       ObEvalCtx &ctx, int64_t nrows, ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::LargeBinaryArray &arr = static_cast<const arrow::LargeBinaryArray &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      int64_t len64 = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(i, &len64));
      if (OB_UNLIKELY(len64 > INT32_MAX
                      || (expr.max_length_ >= 0 && len64 > expr.max_length_))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[FlightSQL] large binary value exceeds target length", K(ret),
                 K(len64), "target_max_length", expr.max_length_, K(i));
      } else {
        ObString src_str(static_cast<int32_t>(len64), raw);
        ObString dst_str;
        if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
          LOG_WARN("[FlightSQL] failed to copy large binary datum", K(ret), K(len64), K(i));
        } else {
          datums[i].set_string(dst_str);
        }
      }
    }
  }
  return ret;
}

static int set_scalar_string_datum(const char *raw, const int64_t len,
                                   const int64_t max_len,
                                   ObIAllocator &str_arena, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(len < 0 || len > INT32_MAX || (max_len >= 0 && len > max_len))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[FlightSQL] scalar Arrow string exceeds target length",
             K(ret), K(len), K(max_len));
  } else {
    ObString src_str(static_cast<int32_t>(len), raw);
    ObString dst_str;
    if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
      LOG_WARN("[FlightSQL] failed to copy scalar Arrow string", K(ret), K(len));
    } else {
      datum.set_string(dst_str);
    }
  }
  return ret;
}

template <typename ArrowArray, typename OffsetType, bool CHECK_EXPR_MAX_LENGTH>
static int convert_datum_variable_width(const arrow::Array &src,
                                        const int64_t row_idx,
                                        const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &datum,
                                        ObIAllocator &str_arena)
{
  UNUSED(ctx);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const ArrowArray &arr = static_cast<const ArrowArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      OffsetType len = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(row_idx, &len));
      const int64_t max_len = CHECK_EXPR_MAX_LENGTH ? expr.max_length_ : -1;
      if (OB_FAIL(set_scalar_string_datum(
              raw, static_cast<int64_t>(len), max_len, str_arena, datum))) {
        LOG_WARN("[FlightSQL] failed to convert scalar variable-width Arrow value",
                 K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

static int convert_datum_string(const arrow::Array &src, const int64_t row_idx,
                                const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                ObIAllocator &str_arena)
{
  return convert_datum_variable_width<arrow::StringArray, int32_t, false>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_large_string(const arrow::Array &src, const int64_t row_idx,
                                      const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                      ObIAllocator &str_arena)
{
  return convert_datum_variable_width<arrow::LargeStringArray, int64_t, false>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_binary(const arrow::Array &src, const int64_t row_idx,
                                const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                ObIAllocator &str_arena)
{
  return convert_datum_variable_width<arrow::BinaryArray, int32_t, false>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_large_binary(const arrow::Array &src, const int64_t row_idx,
                                      const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                      ObIAllocator &str_arena)
{
  return convert_datum_variable_width<arrow::LargeBinaryArray, int64_t, true>(
      src, row_idx, expr, ctx, datum, str_arena);
}

// ---------------------------------------------------------------------------
// Date32 converters
// Arrow DATE32 = days since Unix epoch.
// ObDateType (Oracle mode) = same representation (days since epoch).
// ObMySQLDateType (MySQL mode) = packed YYYYMMDD format via ObMySQLDate.
// ---------------------------------------------------------------------------

static int convert_batch_date32(const arrow::Array &src, ObExpr &expr,
                                 ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Date32Array &arr = static_cast<const arrow::Date32Array &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) { datums[i].set_null(); }
    else               { datums[i].set_date(arr.Value(i)); }
  }
  return ret;
}

static int convert_batch_date32_to_mysql_date(const arrow::Array &src, ObExpr &expr,
                                               ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Date32Array &arr = static_cast<const arrow::Date32Array &>(src);
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      common::ObMySQLDate mdate;
      const int32_t date = arr.Value(i);
      const int32_t mysql_date_min =
          static_cast<int32_t>(DATETIME_MIN_VAL / common::USECS_PER_DAY);
      if (OB_UNLIKELY(date < mysql_date_min || date > DATE_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("[FlightSQL] Date32 is outside MySQL DATE range",
                 K(ret), K(date), K(mysql_date_min), K(DATE_MAX_VAL), K(i));
      } else if (OB_FAIL(common::ObTimeConverter::date_to_mdate(date, mdate))) {
        LOG_WARN("[FlightSQL] date_to_mdate failed", K(ret), "days", arr.Value(i), K(i));
      } else {
        datums[i].set_mysql_date(mdate);
      }
    }
  }
  return ret;
}

static int convert_datum_date32(const arrow::Array &src, const int64_t row_idx,
                                const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::Date32Array &arr = static_cast<const arrow::Date32Array &>(src);
    if (arr.IsNull(row_idx)) { datum.set_null(); }
    else                     { datum.set_date(arr.Value(row_idx)); }
  }
  return ret;
}

static int convert_datum_date32_to_mysql_date(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::Date32Array &arr = static_cast<const arrow::Date32Array &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      common::ObMySQLDate mdate;
      const int32_t date = arr.Value(row_idx);
      const int32_t mysql_date_min =
          static_cast<int32_t>(DATETIME_MIN_VAL / common::USECS_PER_DAY);
      if (OB_UNLIKELY(date < mysql_date_min || date > DATE_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("[FlightSQL] scalar Date32 is outside MySQL DATE range",
                 K(ret), K(date), K(mysql_date_min), K(DATE_MAX_VAL), K(row_idx));
      } else if (OB_FAIL(common::ObTimeConverter::date_to_mdate(date, mdate))) {
        LOG_WARN("[FlightSQL] scalar date_to_mdate failed",
                 K(ret), "days", arr.Value(row_idx), K(row_idx));
      } else {
        datum.set_mysql_date(mdate);
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Timestamp converters → ObDateTimeType / ObMySQLDateTimeType / ObTimestampType.
//
// Arrow timestamps without timezone metadata represent timezone-naive wall
// time, so their epoch-shaped integer is stored without adjustment.  Arrow
// timestamps with nonempty timezone metadata represent UTC-normalized instants;
// those values are converted to wall time through the active OB session
// timezone.  The Arrow timezone string is semantic metadata, not the output
// timezone: SQL DATETIME values follow the session timezone.  MySQL TIMESTAMP
// has the inverse materialization rule: aware Arrow values are already UTC,
// while naive Arrow wall time must be converted through the session timezone.
// ---------------------------------------------------------------------------

static int arrow_timestamp_to_microseconds(const int64_t value,
                                           const arrow::TimeUnit::type unit,
                                           int64_t &timestamp_us)
{
  int ret = OB_SUCCESS;
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      if (OB_UNLIKELY(IS_MULTI_OVERFLOW64(value, USECS_PER_SEC))) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        timestamp_us = value * USECS_PER_SEC;
      }
      break;
    case arrow::TimeUnit::MILLI:
      if (OB_UNLIKELY(IS_MULTI_OVERFLOW64(value, MSECS_PER_SEC))) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        timestamp_us = value * MSECS_PER_SEC;
      }
      break;
    case arrow::TimeUnit::MICRO:
      timestamp_us = value;
      break;
    case arrow::TimeUnit::NANO:
      timestamp_us = value / NSECS_PER_USEC;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  return ret;
}

static int get_session_timezone_info(ObEvalCtx &ctx,
                                     const common::ObTimeZoneInfo *&tz_info)
{
  int ret = OB_SUCCESS;
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] session is null for Arrow timestamp timezone conversion", K(ret));
  } else if (OB_ISNULL(tz_info = sql::get_timezone_info(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] session timezone is null for Arrow timestamp conversion",
             K(ret));
  }
  return ret;
}

template <bool IS_MYSQL_DATETIME, bool IS_MYSQL_TIMESTAMP, bool HAS_TIMEZONE>
static int set_timestamp_datum(const int64_t timestamp_us,
                               const common::ObTimeZoneInfo *tz_info,
                               ObDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t datetime_us = timestamp_us;
  if (IS_MYSQL_TIMESTAMP) {
    int64_t mysql_timestamp_us = timestamp_us;
    if (!HAS_TIMEZONE
        && OB_UNLIKELY(timestamp_us < DATETIME_MIN_VAL
                       || timestamp_us > DATETIME_MAX_VAL)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (!HAS_TIMEZONE && OB_ISNULL(tz_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] timezone info is null for naive MySQL TIMESTAMP", K(ret));
    } else if (!HAS_TIMEZONE
               && OB_FAIL(common::ObTimeConverter::datetime_to_timestamp(
                      timestamp_us, tz_info, mysql_timestamp_us))) {
      ret = OB_ERR_UNEXPECTED_TZ_TRANSITION == ret ? OB_INVALID_DATE_VALUE : ret;
      LOG_WARN("[FlightSQL] failed to convert naive Arrow wall time to MySQL TIMESTAMP",
               K(ret), K(timestamp_us));
    }
    if (OB_SUCC(ret)
        && OB_UNLIKELY(mysql_timestamp_us < MYSQL_TIMESTAMP_MIN_VAL
                       || mysql_timestamp_us > MYSQL_TIMESTAMP_MAX_VAL)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (OB_SUCC(ret)) {
      datum.set_timestamp(mysql_timestamp_us);
    }
  } else {
    // A timezone offset cannot legitimately exceed one day.  This wider input
    // guard protects ObTimeConverter's internal signed addition, while the final
    // check below decides whether the resulting local DATETIME is valid.
    if (HAS_TIMEZONE
        && OB_UNLIKELY(timestamp_us < DATETIME_MIN_VAL - common::USECS_PER_DAY
                       || timestamp_us > DATETIME_MAX_VAL + common::USECS_PER_DAY)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (HAS_TIMEZONE && OB_ISNULL(tz_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] timezone info is null for aware timestamp", K(ret));
    } else if (HAS_TIMEZONE
               && OB_FAIL(common::ObTimeConverter::timestamp_to_datetime(
                      timestamp_us, tz_info, datetime_us))) {
      LOG_WARN("[FlightSQL] failed to convert aware Arrow timestamp through session timezone",
               K(ret), K(timestamp_us));
    }

    if (OB_SUCC(ret)
        && OB_UNLIKELY(datetime_us < DATETIME_MIN_VAL || datetime_us > DATETIME_MAX_VAL)) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else if (OB_SUCC(ret) && IS_MYSQL_DATETIME) {
      common::ObMySQLDateTime mdatetime;
      if (OB_FAIL(common::ObTimeConverter::datetime_to_mdatetime(
              datetime_us, mdatetime))) {
        LOG_WARN("[FlightSQL] datetime_to_mdatetime failed", K(ret), K(datetime_us));
      } else {
        datum.set_mysql_datetime(mdatetime);
      }
    } else if (OB_SUCC(ret)) {
      datum.set_datetime(datetime_us);
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT, bool IS_MYSQL_DATETIME,
          bool IS_MYSQL_TIMESTAMP, bool HAS_TIMEZONE>
static int convert_batch_timestamp_to_datetime(const arrow::Array &src,
                                               ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               int64_t nrows,
                                               ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const common::ObTimeZoneInfo *tz_info = nullptr;
    const bool needs_timezone = IS_MYSQL_TIMESTAMP ? !HAS_TIMEZONE : HAS_TIMEZONE;
    if (needs_timezone && OB_FAIL(get_session_timezone_info(ctx, tz_info))) {
      LOG_WARN("[FlightSQL] failed to get session timezone for Arrow timestamp batch",
               K(ret));
    }
    const arrow::TimestampArray &arr = static_cast<const arrow::TimestampArray &>(src);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        const int64_t value = arr.Value(i);
        int64_t timestamp_us = 0;
        if (OB_FAIL(arrow_timestamp_to_microseconds(value, UNIT, timestamp_us))) {
          LOG_WARN("[FlightSQL] Arrow timestamp is out of DATETIME range", K(ret), K(value),
                   K(timestamp_us), K(i));
        } else if (OB_FAIL((set_timestamp_datum<IS_MYSQL_DATETIME,
                                               IS_MYSQL_TIMESTAMP, HAS_TIMEZONE>(
                       timestamp_us, tz_info, datums[i])))) {
          LOG_WARN("[FlightSQL] failed to materialize Arrow timestamp datum",
                   K(ret), K(value), K(timestamp_us), K(i), K(HAS_TIMEZONE));
        }
      }
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT, bool IS_MYSQL_DATETIME,
          bool IS_MYSQL_TIMESTAMP, bool HAS_TIMEZONE>
static int convert_datum_timestamp_to_datetime(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  const common::ObTimeZoneInfo *tz_info = nullptr;
  const bool needs_timezone = IS_MYSQL_TIMESTAMP ? !HAS_TIMEZONE : HAS_TIMEZONE;
  if (OB_SUCC(ret) && needs_timezone
      && OB_FAIL(get_session_timezone_info(ctx, tz_info))) {
    LOG_WARN("[FlightSQL] failed to get session timezone for scalar Arrow timestamp",
             K(ret), K(row_idx));
  }
  if (OB_SUCC(ret)) {
    const arrow::TimestampArray &arr = static_cast<const arrow::TimestampArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const int64_t value = arr.Value(row_idx);
      int64_t timestamp_us = 0;
      if (OB_FAIL(arrow_timestamp_to_microseconds(value, UNIT, timestamp_us))) {
        LOG_WARN("[FlightSQL] scalar Arrow timestamp is out of DATETIME range",
                 K(ret), K(value), K(timestamp_us), K(row_idx));
      } else if (OB_FAIL((set_timestamp_datum<IS_MYSQL_DATETIME,
                                             IS_MYSQL_TIMESTAMP, HAS_TIMEZONE>(
                     timestamp_us, tz_info, datum)))) {
        LOG_WARN("[FlightSQL] failed to materialize scalar Arrow timestamp datum",
                 K(ret), K(value), K(timestamp_us), K(row_idx), K(HAS_TIMEZONE));
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Time32 / Time64 converters → ObTimeType (microseconds since midnight)
// ---------------------------------------------------------------------------

static int set_time_datum(const int64_t time_us, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const int64_t time_max_val = TIME_MAX_VAL;
  if (OB_UNLIKELY(time_us < -time_max_val || time_us > time_max_val)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("[FlightSQL] Arrow time exceeds MySQL TIME range",
             K(ret), K(time_us), K(time_max_val));
  } else {
    datum.set_time(time_us);
  }
  return ret;
}

static int convert_batch_time32_s(const arrow::Array &src, ObExpr &expr,
                                   ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Time32Array &arr = static_cast<const arrow::Time32Array &>(src);
  if (arr.null_count() == 0) {
    const int32_t *vals = arr.raw_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (OB_FAIL(set_time_datum(static_cast<int64_t>(vals[i]) * 1000000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert TIME32 seconds", K(ret), K(i), K(vals[i]));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) { datums[i].set_null(); }
      else if (OB_FAIL(set_time_datum(
                   static_cast<int64_t>(arr.Value(i)) * 1000000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert nullable TIME32 seconds",
                 K(ret), K(i), "value", arr.Value(i));
      }
    }
  }
  return ret;
}

static int convert_batch_time32_ms(const arrow::Array &src, ObExpr &expr,
                                    ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Time32Array &arr = static_cast<const arrow::Time32Array &>(src);
  if (arr.null_count() == 0) {
    const int32_t *vals = arr.raw_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (OB_FAIL(set_time_datum(static_cast<int64_t>(vals[i]) * 1000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert TIME32 milliseconds", K(ret), K(i), K(vals[i]));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) { datums[i].set_null(); }
      else if (OB_FAIL(set_time_datum(
                   static_cast<int64_t>(arr.Value(i)) * 1000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert nullable TIME32 milliseconds",
                 K(ret), K(i), "value", arr.Value(i));
      }
    }
  }
  return ret;
}

static int convert_batch_time64_us(const arrow::Array &src, ObExpr &expr,
                                    ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Time64Array &arr = static_cast<const arrow::Time64Array &>(src);
  if (arr.null_count() == 0) {
    const int64_t *vals = arr.raw_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (OB_FAIL(set_time_datum(vals[i], datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert TIME64 microseconds", K(ret), K(i), K(vals[i]));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) { datums[i].set_null(); }
      else if (OB_FAIL(set_time_datum(arr.Value(i), datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert nullable TIME64 microseconds",
                 K(ret), K(i), "value", arr.Value(i));
      }
    }
  }
  return ret;
}

static int convert_batch_time64_ns(const arrow::Array &src, ObExpr &expr,
                                    ObEvalCtx &ctx, int64_t nrows, ObIAllocator &)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::Time64Array &arr = static_cast<const arrow::Time64Array &>(src);
  if (arr.null_count() == 0) {
    const int64_t *vals = arr.raw_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (OB_FAIL(set_time_datum(vals[i] / 1000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert TIME64 nanoseconds", K(ret), K(i), K(vals[i]));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) { datums[i].set_null(); }
      else if (OB_FAIL(set_time_datum(arr.Value(i) / 1000LL, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert nullable TIME64 nanoseconds",
                 K(ret), K(i), "value", arr.Value(i));
      }
    }
  }
  return ret;
}

template <typename ArrowArray, int64_t MULTIPLIER, int64_t DIVISOR>
static int convert_datum_time(const arrow::Array &src, const int64_t row_idx,
                              const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                              ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const ArrowArray &arr = static_cast<const ArrowArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const int64_t value = static_cast<int64_t>(arr.Value(row_idx));
      if (OB_FAIL(set_time_datum(value * MULTIPLIER / DIVISOR, datum))) {
        LOG_WARN("[FlightSQL] failed to convert scalar Arrow time",
                 K(ret), K(row_idx), K(value), K(MULTIPLIER), K(DIVISOR));
      }
    }
  }
  return ret;
}

static int convert_datum_time32_s(const arrow::Array &src, const int64_t row_idx,
                                  const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                  ObIAllocator &str_arena)
{
  return convert_datum_time<arrow::Time32Array, 1000000LL, 1LL>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_time32_ms(const arrow::Array &src, const int64_t row_idx,
                                   const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                   ObIAllocator &str_arena)
{
  return convert_datum_time<arrow::Time32Array, 1000LL, 1LL>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_time64_us(const arrow::Array &src, const int64_t row_idx,
                                   const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                   ObIAllocator &str_arena)
{
  return convert_datum_time<arrow::Time64Array, 1LL, 1LL>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_time64_ns(const arrow::Array &src, const int64_t row_idx,
                                   const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                   ObIAllocator &str_arena)
{
  return convert_datum_time<arrow::Time64Array, 1LL, 1000LL>(
      src, row_idx, expr, ctx, datum, str_arena);
}

#undef CONVERT_BATCH_TS_NO_NULL_

// ---------------------------------------------------------------------------
// Collection converters: Arrow FIXED_SIZE_LIST / LIST → ObCollectionSQLType
//
// OB wire formats:
//   OB_VECTOR_TYPE : raw packed floats, no null bitmap, no length prefix.
//   OB_ARRAY_TYPE  : [uint32_t N][uint8_t nulls[N]][T vals[N]], nulls[i]==1 → NULL.
//
// Both formats are wrapped with a LOB locator header via set_array_res so that
// downstream operators (INSERT, storage) handle them uniformly.
// ---------------------------------------------------------------------------

// FIXED_SIZE_LIST<T> → VECTOR: raw packed bytes, no null bitmap.  Dense OB
// VECTOR values cannot contain null elements, so reject a nullable Arrow child
// instead of silently copying its unspecified raw value bytes.
template <typename CppType, typename ArrowValsArray>
static int convert_batch_fixed_list_to_vector(const arrow::Array &src, ObExpr &expr,
                                              ObEvalCtx &ctx, int64_t nrows,
                                              ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }
  const arrow::FixedSizeListArray &arr = static_cast<const arrow::FixedSizeListArray &>(src);
  const int32_t dim = arr.list_type()->list_size();
  const ArrowValsArray &vals = static_cast<const ArrowValsArray &>(*arr.values());
  const int32_t raw_len = dim * static_cast<int32_t>(sizeof(CppType));
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
    if (arr.IsNull(i)) {
      datums[i].set_null();
    } else {
      const int64_t value_offset = arr.value_offset(i);
      for (int32_t j = 0; OB_SUCC(ret) && j < dim; ++j) {
        if (vals.IsNull(value_offset + j)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("[FlightSQL] VECTOR does not support null elements", K(ret), K(i), K(j));
        }
      }
      if (OB_SUCC(ret)) {
        const CppType *raw_ptr = vals.raw_values() + value_offset;
        ObString dst;
        if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
                nullptr, raw_len, str_arena, dst,
                reinterpret_cast<const char *>(raw_ptr)))) {
          LOG_WARN("[FlightSQL] set_array_res failed for VECTOR", K(ret), K(i), K(dim));
        } else {
          datums[i].set_string(dst);
        }
      }
    }
  }
  return ret;
}

static int convert_batch_fixed_list_float32_to_vector(const arrow::Array &src, ObExpr &expr,
                                                       ObEvalCtx &ctx, int64_t nrows,
                                                       ObIAllocator &str_arena)
{
  return convert_batch_fixed_list_to_vector<float, arrow::FloatArray>(
      src, expr, ctx, nrows, str_arena);
}

static int convert_batch_fixed_list_uint8_to_vector(const arrow::Array &src, ObExpr &expr,
                                                     ObEvalCtx &ctx, int64_t nrows,
                                                     ObIAllocator &str_arena)
{
  return convert_batch_fixed_list_to_vector<uint8_t, arrow::UInt8Array>(
      src, expr, ctx, nrows, str_arena);
}

template <typename CppType, typename ArrowValsArray>
static int convert_datum_fixed_list_to_vector(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::FixedSizeListArray &arr =
        static_cast<const arrow::FixedSizeListArray &>(src);
    const int32_t dim = arr.list_type()->list_size();
    const ArrowValsArray &vals = static_cast<const ArrowValsArray &>(*arr.values());
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const int64_t value_offset = arr.value_offset(row_idx);
      for (int32_t j = 0; OB_SUCC(ret) && j < dim; ++j) {
        if (vals.IsNull(value_offset + j)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("[FlightSQL] VECTOR does not support null elements",
                   K(ret), K(row_idx), K(j));
        }
      }
      if (OB_SUCC(ret)) {
        const int32_t raw_len = dim * static_cast<int32_t>(sizeof(CppType));
        const CppType *raw_ptr = vals.raw_values() + value_offset;
        ObString dst;
        if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
                nullptr, raw_len, str_arena, dst,
                reinterpret_cast<const char *>(raw_ptr)))) {
          LOG_WARN("[FlightSQL] set_array_res failed for scalar VECTOR",
                   K(ret), K(row_idx), K(dim));
        } else {
          datum.set_string(dst);
        }
      }
    }
  }
  return ret;
}

static int convert_datum_fixed_list_float32_to_vector(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_fixed_list_to_vector<float, arrow::FloatArray>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_fixed_list_uint8_to_vector(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_fixed_list_to_vector<uint8_t, arrow::UInt8Array>(
      src, row_idx, expr, ctx, datum, str_arena);
}

// Helper: serialize one row into OB ARRAY format and wrap with LOB header.
// raw layout: [uint32_t n][uint8_t nulls[n]][CppType vals[n]]
template <typename CppType, typename ArrowValsArray, bool CHECK_UNSIGNED_RANGE>
static int write_ob_array_row(const ArrowValsArray &vals, int64_t start, int64_t n,
                               ObIAllocator &str_arena, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const int64_t element_binary_size = sizeof(uint8_t) + sizeof(CppType);
  const int64_t max_elements_by_binary_size =
      (common::MAX_ARRAY_SIZE - static_cast<int64_t>(sizeof(uint32_t))) /
      element_binary_size;
  int64_t raw_len = 0;
  if (OB_UNLIKELY(start < 0 || n < 0 || n > vals.length() || start > vals.length() - n)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow ARRAY value range",
             K(ret), K(start), K(n), "value_count", vals.length());
  } else if (OB_UNLIKELY(n > common::MAX_ARRAY_ELEMENT_SIZE ||
                         n > max_elements_by_binary_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[FlightSQL] Arrow ARRAY value exceeds OceanBase ARRAY limits",
             K(ret), K(n), K(common::MAX_ARRAY_ELEMENT_SIZE),
             K(common::MAX_ARRAY_SIZE));
  } else {
    raw_len = static_cast<int64_t>(sizeof(uint32_t)) + n * element_binary_size;
  }

  common::ObArrayData<CppType> array_data(str_arena);
  common::ObArrayFixedSize<CppType> array;
  array.set_array_data(&array_data);
  for (int64_t j = 0; OB_SUCC(ret) && j < n; ++j) {
    const int64_t idx = start + j;
    if (vals.IsNull(idx)) {
      if (OB_FAIL(array.push_null())) {
        LOG_WARN("[FlightSQL] push null ARRAY element failed", K(ret), K(j), K(n));
      }
    } else {
      const CppType value = static_cast<CppType>(vals.Value(idx));
      if (CHECK_UNSIGNED_RANGE
          && OB_UNLIKELY(static_cast<long double>(value) < 0.0L)) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("[FlightSQL] negative Arrow value for unsigned ARRAY element",
                 K(ret), K(value), K(j), K(n));
      } else if (OB_FAIL(array.push_back(value))) {
        LOG_WARN("[FlightSQL] push ARRAY element failed", K(ret), K(j), K(n));
      }
    }
  }

  char *buf = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf = static_cast<char *>(str_arena.alloc(raw_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FlightSQL] alloc OB ARRAY buffer failed", K(ret), K(raw_len));
    } else if (OB_FAIL(array.get_raw_binary(buf, raw_len))) {
      LOG_WARN("[FlightSQL] serialize OB ARRAY row failed", K(ret), K(n), K(raw_len));
    } else {
      ObString dst;
      if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
              nullptr, raw_len, str_arena, dst, buf))) {
        LOG_WARN("[FlightSQL] set_array_res failed for ARRAY row", K(ret), K(n));
      } else {
        datum.set_string(dst);
      }
    }
  }
  return ret;
}

// LIST<T> → OB ARRAY, one function per element type.
// Note: the template call must be a plain statement (not wrapped in OB_FAIL/OB_UNLIKELY)
// because the C preprocessor does not treat <> as grouping — a comma inside template
// parameters would be mistaken for a macro argument separator.
#define DECL_CONVERT_LIST_(CppType, ArrowArrayClass, CheckUnsignedRange, fname)          \
static int fname(const arrow::Array &src, ObExpr &expr, ObEvalCtx &ctx,                 \
                 int64_t nrows, ObIAllocator &str_arena)                                 \
{                                                                                         \
  int ret = OB_SUCCESS;                                                                   \
  ObDatum *datums = init_uniform(expr, ctx, nrows);                                      \
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }                          \
  const arrow::ListArray &arr = static_cast<const arrow::ListArray &>(src);             \
  const ArrowArrayClass &vals = static_cast<const ArrowArrayClass &>(*arr.values());    \
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {                                \
    if (arr.IsNull(i)) {                                                                  \
      datums[i].set_null();                                                               \
    } else {                                                                              \
      ret = write_ob_array_row<CppType, ArrowArrayClass, CheckUnsignedRange>(            \
                vals, arr.value_offset(i),                                                \
                arr.value_length(i),                                                      \
                str_arena, datums[i]);                                                    \
      if (OB_UNLIKELY(OB_SUCCESS != ret)) {                                              \
        LOG_WARN("[FlightSQL] write_ob_array_row failed", K(ret), K(i));                 \
      }                                                                                   \
    }                                                                                     \
  }                                                                                       \
  return ret;                                                                             \
}

DECL_CONVERT_LIST_(float,    arrow::FloatArray,  false, convert_batch_list_float32)
DECL_CONVERT_LIST_(double,   arrow::DoubleArray, false, convert_batch_list_float64)
DECL_CONVERT_LIST_(float,    arrow::FloatArray,  true,  convert_batch_list_ufloat32)
DECL_CONVERT_LIST_(double,   arrow::DoubleArray, true,  convert_batch_list_udouble64)
DECL_CONVERT_LIST_(int8_t,   arrow::Int8Array,   false, convert_batch_list_int8)
DECL_CONVERT_LIST_(uint8_t,  arrow::UInt8Array,  false, convert_batch_list_uint8)
DECL_CONVERT_LIST_(int16_t,  arrow::Int16Array,  false, convert_batch_list_int16)
DECL_CONVERT_LIST_(uint16_t, arrow::UInt16Array, false, convert_batch_list_uint16)
DECL_CONVERT_LIST_(int32_t,  arrow::Int32Array,  false, convert_batch_list_int32)
DECL_CONVERT_LIST_(uint32_t, arrow::UInt32Array, false, convert_batch_list_uint32)
DECL_CONVERT_LIST_(int64_t,  arrow::Int64Array,  false, convert_batch_list_int64)
DECL_CONVERT_LIST_(uint64_t, arrow::UInt64Array, false, convert_batch_list_uint64)

#define DECL_CONVERT_DATUM_LIST_(CppType, ArrowArrayClass, CheckUnsignedRange, fname)   \
static int fname(const arrow::Array &src, const int64_t row_idx,                        \
                 const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,                    \
                 ObIAllocator &str_arena)                                                \
{                                                                                       \
  UNUSED(expr);                                                                          \
  UNUSED(ctx);                                                                           \
  int ret = check_scalar_row_idx(src, row_idx);                                           \
  if (OB_SUCC(ret)) {                                                                     \
    const arrow::ListArray &arr = static_cast<const arrow::ListArray &>(src);             \
    const ArrowArrayClass &vals = static_cast<const ArrowArrayClass &>(*arr.values());    \
    if (arr.IsNull(row_idx)) {                                                            \
      datum.set_null();                                                                    \
    } else {                                                                              \
      ret = write_ob_array_row<CppType, ArrowArrayClass, CheckUnsignedRange>(             \
          vals, arr.value_offset(row_idx), arr.value_length(row_idx),                     \
          str_arena, datum);                                                               \
      if (OB_UNLIKELY(OB_SUCCESS != ret)) {                                               \
        LOG_WARN("[FlightSQL] write scalar OB ARRAY row failed", K(ret), K(row_idx));    \
      }                                                                                   \
    }                                                                                     \
  }                                                                                       \
  return ret;                                                                             \
}

DECL_CONVERT_DATUM_LIST_(float,    arrow::FloatArray,  false, convert_datum_list_float32)
DECL_CONVERT_DATUM_LIST_(double,   arrow::DoubleArray, false, convert_datum_list_float64)
DECL_CONVERT_DATUM_LIST_(float,    arrow::FloatArray,  true,  convert_datum_list_ufloat32)
DECL_CONVERT_DATUM_LIST_(double,   arrow::DoubleArray, true,  convert_datum_list_udouble64)
DECL_CONVERT_DATUM_LIST_(int8_t,   arrow::Int8Array,   false, convert_datum_list_int8)
DECL_CONVERT_DATUM_LIST_(uint8_t,  arrow::UInt8Array,  false, convert_datum_list_uint8)
DECL_CONVERT_DATUM_LIST_(int16_t,  arrow::Int16Array,  false, convert_datum_list_int16)
DECL_CONVERT_DATUM_LIST_(uint16_t, arrow::UInt16Array, false, convert_datum_list_uint16)
DECL_CONVERT_DATUM_LIST_(int32_t,  arrow::Int32Array,  false, convert_datum_list_int32)
DECL_CONVERT_DATUM_LIST_(uint32_t, arrow::UInt32Array, false, convert_datum_list_uint32)
DECL_CONVERT_DATUM_LIST_(int64_t,  arrow::Int64Array,  false, convert_datum_list_int64)
DECL_CONVERT_DATUM_LIST_(uint64_t, arrow::UInt64Array, false, convert_datum_list_uint64)

#undef DECL_CONVERT_DATUM_LIST_

// FIXED_SIZE_LIST<non-float32 T> → OB ARRAY (with null bitmap, not VECTOR format).
// Same preprocessor constraint as DECL_CONVERT_LIST_: template call must be a plain
// statement, not wrapped in OB_FAIL/OB_UNLIKELY.
#define DECL_CONVERT_FIXED_LIST_(CppType, ArrowArrayClass, CheckUnsignedRange, fname)    \
static int fname(const arrow::Array &src, ObExpr &expr, ObEvalCtx &ctx,                 \
                 int64_t nrows, ObIAllocator &str_arena)                                 \
{                                                                                         \
  int ret = OB_SUCCESS;                                                                   \
  ObDatum *datums = init_uniform(expr, ctx, nrows);                                      \
  if (OB_ISNULL(datums)) { return OB_ALLOCATE_MEMORY_FAILED; }                          \
  const arrow::FixedSizeListArray &arr =                                                \
      static_cast<const arrow::FixedSizeListArray &>(src);                              \
  const int32_t dim = arr.list_type()->list_size();                                      \
  const ArrowArrayClass &vals = static_cast<const ArrowArrayClass &>(*arr.values());    \
  for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {                                \
    if (arr.IsNull(i)) {                                                                  \
      datums[i].set_null();                                                               \
    } else {                                                                              \
      ret = write_ob_array_row<CppType, ArrowArrayClass, CheckUnsignedRange>(            \
                vals, arr.value_offset(i), dim, str_arena, datums[i]);                   \
      if (OB_UNLIKELY(OB_SUCCESS != ret)) {                                              \
        LOG_WARN("[FlightSQL] write_ob_array_row failed for fixed list", K(ret), K(i));  \
      }                                                                                   \
    }                                                                                     \
  }                                                                                       \
  return ret;                                                                             \
}

DECL_CONVERT_FIXED_LIST_(float,    arrow::FloatArray,  false, convert_batch_fixed_list_float32)
DECL_CONVERT_FIXED_LIST_(double,   arrow::DoubleArray, false, convert_batch_fixed_list_float64)
DECL_CONVERT_FIXED_LIST_(float,    arrow::FloatArray,  true,  convert_batch_fixed_list_ufloat32)
DECL_CONVERT_FIXED_LIST_(double,   arrow::DoubleArray, true,  convert_batch_fixed_list_udouble64)
DECL_CONVERT_FIXED_LIST_(int8_t,   arrow::Int8Array,   false, convert_batch_fixed_list_int8)
DECL_CONVERT_FIXED_LIST_(uint8_t,  arrow::UInt8Array,  false, convert_batch_fixed_list_uint8)
DECL_CONVERT_FIXED_LIST_(int16_t,  arrow::Int16Array,  false, convert_batch_fixed_list_int16)
DECL_CONVERT_FIXED_LIST_(uint16_t, arrow::UInt16Array, false, convert_batch_fixed_list_uint16)
DECL_CONVERT_FIXED_LIST_(int32_t,  arrow::Int32Array,  false, convert_batch_fixed_list_int32)
DECL_CONVERT_FIXED_LIST_(uint32_t, arrow::UInt32Array, false, convert_batch_fixed_list_uint32)
DECL_CONVERT_FIXED_LIST_(int64_t,  arrow::Int64Array,  false, convert_batch_fixed_list_int64)
DECL_CONVERT_FIXED_LIST_(uint64_t, arrow::UInt64Array, false, convert_batch_fixed_list_uint64)

#define DECL_CONVERT_DATUM_FIXED_LIST_(CppType, ArrowArrayClass, CheckUnsignedRange, fname) \
static int fname(const arrow::Array &src, const int64_t row_idx,                        \
                 const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,                    \
                 ObIAllocator &str_arena)                                                \
{                                                                                       \
  UNUSED(expr);                                                                          \
  UNUSED(ctx);                                                                           \
  int ret = check_scalar_row_idx(src, row_idx);                                           \
  if (OB_SUCC(ret)) {                                                                     \
    const arrow::FixedSizeListArray &arr =                                                \
        static_cast<const arrow::FixedSizeListArray &>(src);                              \
    const ArrowArrayClass &vals = static_cast<const ArrowArrayClass &>(*arr.values());    \
    if (arr.IsNull(row_idx)) {                                                            \
      datum.set_null();                                                                    \
    } else {                                                                              \
      ret = write_ob_array_row<CppType, ArrowArrayClass, CheckUnsignedRange>(             \
          vals, arr.value_offset(row_idx), arr.list_type()->list_size(),                  \
          str_arena, datum);                                                               \
      if (OB_UNLIKELY(OB_SUCCESS != ret)) {                                               \
        LOG_WARN("[FlightSQL] write scalar fixed OB ARRAY row failed",                  \
                 K(ret), K(row_idx));                                                      \
      }                                                                                   \
    }                                                                                     \
  }                                                                                       \
  return ret;                                                                             \
}

DECL_CONVERT_DATUM_FIXED_LIST_(float,    arrow::FloatArray,  false, convert_datum_fixed_list_float32)
DECL_CONVERT_DATUM_FIXED_LIST_(double,   arrow::DoubleArray, false, convert_datum_fixed_list_float64)
DECL_CONVERT_DATUM_FIXED_LIST_(float,    arrow::FloatArray,  true,  convert_datum_fixed_list_ufloat32)
DECL_CONVERT_DATUM_FIXED_LIST_(double,   arrow::DoubleArray, true,  convert_datum_fixed_list_udouble64)
DECL_CONVERT_DATUM_FIXED_LIST_(int8_t,   arrow::Int8Array,   false, convert_datum_fixed_list_int8)
DECL_CONVERT_DATUM_FIXED_LIST_(uint8_t,  arrow::UInt8Array,  false, convert_datum_fixed_list_uint8)
DECL_CONVERT_DATUM_FIXED_LIST_(int16_t,  arrow::Int16Array,  false, convert_datum_fixed_list_int16)
DECL_CONVERT_DATUM_FIXED_LIST_(uint16_t, arrow::UInt16Array, false, convert_datum_fixed_list_uint16)
DECL_CONVERT_DATUM_FIXED_LIST_(int32_t,  arrow::Int32Array,  false, convert_datum_fixed_list_int32)
DECL_CONVERT_DATUM_FIXED_LIST_(uint32_t, arrow::UInt32Array, false, convert_datum_fixed_list_uint32)
DECL_CONVERT_DATUM_FIXED_LIST_(int64_t,  arrow::Int64Array,  false, convert_datum_fixed_list_int64)
DECL_CONVERT_DATUM_FIXED_LIST_(uint64_t, arrow::UInt64Array, false, convert_datum_fixed_list_uint64)

#undef DECL_CONVERT_DATUM_FIXED_LIST_

// ---------------------------------------------------------------------------
// build_convert_fn: select the right ConvertBatchFn for one column.
// ---------------------------------------------------------------------------

template <arrow::TimeUnit::type UNIT>
static void set_timestamp_convert_fns(const bool is_mysql_datetime,
                                      const bool is_mysql_timestamp,
                                      const bool has_timezone,
                                      ObFlightSqlColumnDesc &desc)
{
  if (is_mysql_timestamp) {
    if (has_timezone) {
      desc.convert_batch =
          convert_batch_timestamp_to_datetime<UNIT, false, true, true>;
      desc.convert_datum =
          convert_datum_timestamp_to_datetime<UNIT, false, true, true>;
    } else {
      desc.convert_batch =
          convert_batch_timestamp_to_datetime<UNIT, false, true, false>;
      desc.convert_datum =
          convert_datum_timestamp_to_datetime<UNIT, false, true, false>;
    }
  } else if (is_mysql_datetime) {
    if (has_timezone) {
      desc.convert_batch =
          convert_batch_timestamp_to_datetime<UNIT, true, false, true>;
      desc.convert_datum =
          convert_datum_timestamp_to_datetime<UNIT, true, false, true>;
    } else {
      desc.convert_batch =
          convert_batch_timestamp_to_datetime<UNIT, true, false, false>;
      desc.convert_datum =
          convert_datum_timestamp_to_datetime<UNIT, true, false, false>;
    }
  } else if (has_timezone) {
    desc.convert_batch =
        convert_batch_timestamp_to_datetime<UNIT, false, false, true>;
    desc.convert_datum =
        convert_datum_timestamp_to_datetime<UNIT, false, false, true>;
  } else {
    desc.convert_batch =
        convert_batch_timestamp_to_datetime<UNIT, false, false, false>;
    desc.convert_datum =
        convert_datum_timestamp_to_datetime<UNIT, false, false, false>;
  }
}

static bool collection_element_type_matches(const arrow::Type::type arrow_type,
                                            const common::ObObjType ob_type)
{
  using namespace common;
  switch (ob_type) {
    case ObTinyIntType:   return arrow_type == arrow::Type::INT8;
    case ObSmallIntType:  return arrow_type == arrow::Type::INT16;
    case ObInt32Type:     return arrow_type == arrow::Type::INT32;
    case ObIntType:       return arrow_type == arrow::Type::INT64;
    case ObUTinyIntType:  return arrow_type == arrow::Type::UINT8;
    case ObUSmallIntType: return arrow_type == arrow::Type::UINT16;
    case ObUInt32Type:    return arrow_type == arrow::Type::UINT32;
    case ObUInt64Type:    return arrow_type == arrow::Type::UINT64;
    case ObFloatType:
    case ObUFloatType:    return arrow_type == arrow::Type::FLOAT;
    case ObDoubleType:
    case ObUDoubleType:   return arrow_type == arrow::Type::DOUBLE;
    default:               return false;
  }
}

static int select_array_convert_fn(const arrow::Type::type arrow_type,
                                   const common::ObObjType target_element_type,
                                   const bool is_fixed_size_list,
                                   ObFlightSqlColumnDesc &desc)
{
  int ret = OB_SUCCESS;
  if (is_fixed_size_list) {
    switch (arrow_type) {
      case arrow::Type::FLOAT:
        if (target_element_type == common::ObUFloatType) {
          desc.convert_batch = convert_batch_fixed_list_ufloat32;
          desc.convert_datum = convert_datum_fixed_list_ufloat32;
        } else {
          desc.convert_batch = convert_batch_fixed_list_float32;
          desc.convert_datum = convert_datum_fixed_list_float32;
        }
        break;
      case arrow::Type::DOUBLE:
        if (target_element_type == common::ObUDoubleType) {
          desc.convert_batch = convert_batch_fixed_list_udouble64;
          desc.convert_datum = convert_datum_fixed_list_udouble64;
        } else {
          desc.convert_batch = convert_batch_fixed_list_float64;
          desc.convert_datum = convert_datum_fixed_list_float64;
        }
        break;
      case arrow::Type::INT8:    desc.convert_batch = convert_batch_fixed_list_int8;    desc.convert_datum = convert_datum_fixed_list_int8;    break;
      case arrow::Type::UINT8:   desc.convert_batch = convert_batch_fixed_list_uint8;   desc.convert_datum = convert_datum_fixed_list_uint8;   break;
      case arrow::Type::INT16:   desc.convert_batch = convert_batch_fixed_list_int16;   desc.convert_datum = convert_datum_fixed_list_int16;   break;
      case arrow::Type::UINT16:  desc.convert_batch = convert_batch_fixed_list_uint16;  desc.convert_datum = convert_datum_fixed_list_uint16;  break;
      case arrow::Type::INT32:   desc.convert_batch = convert_batch_fixed_list_int32;   desc.convert_datum = convert_datum_fixed_list_int32;   break;
      case arrow::Type::UINT32:  desc.convert_batch = convert_batch_fixed_list_uint32;  desc.convert_datum = convert_datum_fixed_list_uint32;  break;
      case arrow::Type::INT64:   desc.convert_batch = convert_batch_fixed_list_int64;   desc.convert_datum = convert_datum_fixed_list_int64;   break;
      case arrow::Type::UINT64:  desc.convert_batch = convert_batch_fixed_list_uint64;  desc.convert_datum = convert_datum_fixed_list_uint64;  break;
      default:                    ret = OB_NOT_SUPPORTED; break;
    }
  } else {
    switch (arrow_type) {
      case arrow::Type::FLOAT:
        if (target_element_type == common::ObUFloatType) {
          desc.convert_batch = convert_batch_list_ufloat32;
          desc.convert_datum = convert_datum_list_ufloat32;
        } else {
          desc.convert_batch = convert_batch_list_float32;
          desc.convert_datum = convert_datum_list_float32;
        }
        break;
      case arrow::Type::DOUBLE:
        if (target_element_type == common::ObUDoubleType) {
          desc.convert_batch = convert_batch_list_udouble64;
          desc.convert_datum = convert_datum_list_udouble64;
        } else {
          desc.convert_batch = convert_batch_list_float64;
          desc.convert_datum = convert_datum_list_float64;
        }
        break;
      case arrow::Type::INT8:    desc.convert_batch = convert_batch_list_int8;    desc.convert_datum = convert_datum_list_int8;    break;
      case arrow::Type::UINT8:   desc.convert_batch = convert_batch_list_uint8;   desc.convert_datum = convert_datum_list_uint8;   break;
      case arrow::Type::INT16:   desc.convert_batch = convert_batch_list_int16;   desc.convert_datum = convert_datum_list_int16;   break;
      case arrow::Type::UINT16:  desc.convert_batch = convert_batch_list_uint16;  desc.convert_datum = convert_datum_list_uint16;  break;
      case arrow::Type::INT32:   desc.convert_batch = convert_batch_list_int32;   desc.convert_datum = convert_datum_list_int32;   break;
      case arrow::Type::UINT32:  desc.convert_batch = convert_batch_list_uint32;  desc.convert_datum = convert_datum_list_uint32;  break;
      case arrow::Type::INT64:   desc.convert_batch = convert_batch_list_int64;   desc.convert_datum = convert_datum_list_int64;   break;
      case arrow::Type::UINT64:  desc.convert_batch = convert_batch_list_uint64;  desc.convert_datum = convert_datum_list_uint64;  break;
      default:                    ret = OB_NOT_SUPPORTED; break;
    }
  }
  return ret;
}

static int build_collection_convert_fn(const arrow::Field &arrow_field,
                                       const common::ObCollectionTypeBase *collection_meta,
                                       ObFlightSqlColumnDesc &desc)
{
  using namespace common;
  int ret = OB_SUCCESS;
  const arrow::Type::type arrow_id = arrow_field.type()->id();
  if (OB_ISNULL(collection_meta)
      || (collection_meta->type_id_ != OB_ARRAY_TYPE
          && collection_meta->type_id_ != OB_VECTOR_TYPE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] unsupported target collection metadata", K(ret), KP(collection_meta));
  } else if (arrow_id != arrow::Type::FIXED_SIZE_LIST && arrow_id != arrow::Type::LIST) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] collection target requires Arrow LIST or FIXED_SIZE_LIST",
             K(ret), "arrow_type_id", static_cast<int>(arrow_id));
  } else {
    const ObCollectionArrayType *target =
        static_cast<const ObCollectionArrayType *>(collection_meta);
    const ObCollectionBasicType *target_elem =
        dynamic_cast<const ObCollectionBasicType *>(target->element_type_);
    const bool is_fixed_size_list = arrow_id == arrow::Type::FIXED_SIZE_LIST;
    const arrow::Type::type arrow_elem_type = is_fixed_size_list
        ? static_cast<const arrow::FixedSizeListType &>(*arrow_field.type()).value_type()->id()
        : static_cast<const arrow::ListType &>(*arrow_field.type()).value_type()->id();
    if (OB_ISNULL(target_elem)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] nested collection elements are not supported for write",
               K(ret), K(collection_meta->type_id_));
    } else if (!collection_element_type_matches(
                   arrow_elem_type, target_elem->basic_meta_.get_obj_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] Arrow collection element type does not match target",
               K(ret), "arrow_element_type", static_cast<int>(arrow_elem_type),
               "target_element_type", target_elem->basic_meta_.get_obj_type());
    } else if (collection_meta->type_id_ == OB_VECTOR_TYPE) {
      const int32_t input_dim = is_fixed_size_list
          ? static_cast<const arrow::FixedSizeListType &>(*arrow_field.type()).list_size()
          : -1;
      if (!is_fixed_size_list) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] VECTOR requires Arrow FIXED_SIZE_LIST", K(ret));
      } else if (target->dim_cnt_ != static_cast<uint32_t>(input_dim)) {
        ret = OB_ERR_INVALID_VECTOR_DIM;
        LOG_WARN("[FlightSQL] Arrow vector dimension does not match target", K(ret),
                 "target_dim", target->dim_cnt_, K(input_dim));
        LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, target->dim_cnt_, input_dim);
      } else if (target_elem->basic_meta_.get_obj_type() == ObFloatType) {
        desc.convert_batch = convert_batch_fixed_list_float32_to_vector;
        desc.convert_datum = convert_datum_fixed_list_float32_to_vector;
      } else if (target_elem->basic_meta_.get_obj_type() == ObUTinyIntType) {
        desc.convert_batch = convert_batch_fixed_list_uint8_to_vector;
        desc.convert_datum = convert_datum_fixed_list_uint8_to_vector;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported VECTOR element type", K(ret),
                 "target_element_type", target_elem->basic_meta_.get_obj_type());
      }
    } else if (OB_FAIL(select_array_convert_fn(
                   arrow_elem_type, target_elem->basic_meta_.get_obj_type(),
                   is_fixed_size_list, desc))) {
      LOG_WARN("[FlightSQL] unsupported Arrow ARRAY element type", K(ret),
               "arrow_element_type", static_cast<int>(arrow_elem_type));
    }
  }
  return ret;
}

static void set_output_desc(sql::ObArrowStreamColDesc &out_desc,
                            const common::ObObjType output_type,
                            const common::ObAccuracy &accuracy,
                            const common::ObCollationType cs_type,
                            const uint16_t subschema_id = UINT_MAX16)
{
  out_desc.ob_type = output_type;
  out_desc.accuracy = accuracy;
  out_desc.cs_type = cs_type;
  out_desc.subschema_id = subschema_id;
}

int ob_flight_sql_get_output_col_desc(
    const arrow::Field &arrow_field,
    const share::schema::ObColumnSchemaV2 &ob_col,
    const uint16_t collection_subschema_id,
    sql::ObArrowStreamColDesc &out_desc)
{
  using namespace common;
  int ret = OB_SUCCESS;
  const ObObjType target_type = ob_col.get_data_type();
  const ObAccuracy &target_accuracy = ob_col.get_accuracy();
  const ObCollationType target_cs = ob_col.get_collation_type();
  const arrow::Type::type arrow_id = arrow_field.type()->id();

  // This descriptor is the type contract between the catalog and VecOp.  It
  // must name the representation written by the selected converter, rather
  // than blindly repeating the target table type.  column_conv then receives
  // the true source type and installs the normal SQL cast where it is needed.
  switch (arrow_id) {
    case arrow::Type::BOOL:
      set_output_desc(out_desc, ObTinyIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObTinyIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::INT8:
      set_output_desc(out_desc, ObTinyIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObTinyIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::INT16:
      set_output_desc(out_desc, ObSmallIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObSmallIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::INT32:
      set_output_desc(out_desc, ObInt32Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type], CS_TYPE_INVALID);
      break;
    case arrow::Type::INT64:
      set_output_desc(out_desc, ObIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT8:
      set_output_desc(out_desc, ObUTinyIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObUTinyIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT16:
      set_output_desc(out_desc, ObUSmallIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObUSmallIntType], CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT32:
      set_output_desc(out_desc, ObUInt32Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt32Type], CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT64:
      set_output_desc(out_desc, ObUInt64Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type], CS_TYPE_INVALID);
      break;
    case arrow::Type::FLOAT:
      set_output_desc(out_desc, ObFloatType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObFloatType], CS_TYPE_INVALID);
      break;
    case arrow::Type::DOUBLE:
      set_output_desc(out_desc, ObDoubleType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObDoubleType], CS_TYPE_INVALID);
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      // Arrow STRING is UTF-8 regardless of the target column's charset.  Do
      // not expose the target's VARCHAR/CHAR/TEXT metadata here: doing so can
      // suppress the normal column_conv charset/length conversion.  The
      // function table describes the datum we write (an unconstrained UTF-8
      // VARCHAR); the INSERT target is converted later by the SQL plan.
      set_output_desc(out_desc, ObVarcharType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObVarcharType],
                      CS_TYPE_UTF8MB4_BIN);
      break;
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
      set_output_desc(out_desc, ObVarcharType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY[ObVarcharType],
                      CS_TYPE_BINARY);
      break;
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256:
      if (target_type == ObDecimalIntType || target_type == ObNumberType
          || target_type == ObUNumberType) {
        set_output_desc(out_desc, target_type, target_accuracy, target_cs);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] Decimal target type is not supported", K(ret), K(target_type));
      }
      break;
    case arrow::Type::DATE32:
      if (ob_is_date_or_mysql_date(target_type)) {
        set_output_desc(out_desc, target_type, target_accuracy, target_cs);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DATE32 target type is not supported", K(ret), K(target_type));
      }
      break;
    case arrow::Type::TIMESTAMP:
      if (ob_is_datetime_or_mysql_datetime_tc(target_type)
          || target_type == ObDateTimeType) {
        set_output_desc(out_desc, target_type, target_accuracy, target_cs);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] TIMESTAMP target type is not supported", K(ret), K(target_type));
      }
      break;
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
      if (target_type == ObTimeType) {
        set_output_desc(out_desc, target_type, target_accuracy, target_cs);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] Arrow time target type is not supported", K(ret), K(target_type));
      }
      break;
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::LIST:
      if (target_type == ObCollectionSQLType) {
        set_output_desc(out_desc, target_type, target_accuracy, target_cs,
                        collection_subschema_id);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] Arrow collection target type is not supported", K(ret), K(target_type));
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] unsupported Arrow type for output descriptor", K(ret),
               "arrow_type_id", static_cast<int>(arrow_id), K(target_type));
      break;
  }
  return ret;
}

static int build_convert_fn(const arrow::Field &arrow_field,
                            const common::ObObjType ob_type,
                            const common::ObPrecision target_precision,
                            const common::ObCollectionTypeBase *collection_meta,
                            ObFlightSqlColumnDesc &desc)
{
  int ret = OB_SUCCESS;
  const arrow::Type::type arrow_id = arrow_field.type()->id();

  desc.convert_batch = nullptr;
  desc.convert_datum = nullptr;
  desc.kind          = ConversionKind::EXACT;
  desc.ts_unit_factor = 0;
  desc.decimal_int_bytes = 0;

  switch (arrow_id) {
    // ---- integers ----
    case arrow::Type::BOOL:
      if (ob_type != common::ObTinyIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] BOOL converter output must be TINYINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_bool;
        desc.convert_datum = convert_datum_bool;
        desc.kind = ConversionKind::EXACT;
      }
      break;
    case arrow::Type::INT8:
      if (ob_type != common::ObTinyIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] INT8 converter output must be TINYINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_int8;
        desc.convert_datum = convert_datum_int8;
      }
      break;
    case arrow::Type::INT16:
      if (ob_type != common::ObSmallIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] INT16 converter output must be SMALLINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_int16;
        desc.convert_datum = convert_datum_int16;
      }
      break;
    case arrow::Type::INT32:
      if (ob_type != common::ObInt32Type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] INT32 converter output must be INT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_int32;
        desc.convert_datum = convert_datum_int32;
      }
      break;
    case arrow::Type::INT64:
      if (ob_type != common::ObIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] INT64 converter output must be BIGINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_int64;
        desc.convert_datum = convert_datum_int64;
      }
      break;
    case arrow::Type::UINT8:
      if (ob_type != common::ObUTinyIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] UINT8 converter output must be UNSIGNED TINYINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_uint8;
        desc.convert_datum = convert_datum_uint8;
      }
      break;
    case arrow::Type::UINT16:
      if (ob_type != common::ObUSmallIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] UINT16 converter output must be UNSIGNED SMALLINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_uint16;
        desc.convert_datum = convert_datum_uint16;
      }
      break;
    case arrow::Type::UINT32:
      if (ob_type != common::ObUInt32Type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] UINT32 converter output must be UNSIGNED INT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_uint32;
        desc.convert_datum = convert_datum_uint32;
      }
      break;
    case arrow::Type::UINT64:
      if (ob_type != common::ObUInt64Type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] UINT64 converter output must be UNSIGNED BIGINT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_uint64;
        desc.convert_datum = convert_datum_uint64;
      }
      break;

    // ---- float ----
    case arrow::Type::FLOAT:
      if (ob_type != common::ObFloatType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] FLOAT converter output must be FLOAT", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_float32;
        desc.convert_datum = convert_datum_float32;
      }
      break;
    case arrow::Type::DOUBLE:
      if (ob_type != common::ObDoubleType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] DOUBLE converter output must be DOUBLE", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_float64;
        desc.convert_datum = convert_datum_float64;
      }
      break;

    // ---- decimal ----
    case arrow::Type::DECIMAL128:
      desc.kind = ConversionKind::TRANSFORM;
      if (ob_type == common::ObDecimalIntType) {
        desc.decimal_int_bytes =
            wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
        desc.convert_batch = convert_batch_decimal128_to_decint;
        desc.convert_datum = convert_datum_decimal128_to_decint;
      } else if (ob_type == common::ObNumberType || ob_type == common::ObUNumberType) {
        desc.convert_batch = convert_batch_decimal128_to_number;
        desc.convert_datum = convert_datum_decimal128_to_number;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DECIMAL128 must map to DecimalInt or Number", K(ret), K(ob_type));
      }
      break;
    case arrow::Type::DECIMAL256:
      desc.kind = ConversionKind::TRANSFORM;
      if (ob_type == common::ObDecimalIntType) {
        desc.decimal_int_bytes =
            wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
        desc.convert_batch = convert_batch_decimal256_to_decint;
        desc.convert_datum = convert_datum_decimal256_to_decint;
      } else if (ob_type == common::ObNumberType || ob_type == common::ObUNumberType) {
        desc.convert_batch = convert_batch_decimal256_to_number;
        desc.convert_datum = convert_datum_decimal256_to_number;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DECIMAL256 must map to DecimalInt or Number", K(ret), K(ob_type));
      }
      break;

    // ---- string / binary ----
    case arrow::Type::STRING:
      if (ob_type != common::ObVarcharType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] STRING converter output must be VARCHAR", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_string;
        desc.convert_datum = convert_datum_string;
      }
      break;
    case arrow::Type::LARGE_STRING:
      if (ob_type != common::ObVarcharType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] LARGE_STRING converter output must be VARCHAR", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_large_string;
        desc.convert_datum = convert_datum_large_string;
      }
      break;
    case arrow::Type::BINARY:
      if (ob_type != common::ObVarcharType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] BINARY converter output must be VARCHAR", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_binary;
        desc.convert_datum = convert_datum_binary;
      }
      break;
    case arrow::Type::LARGE_BINARY:
      if (ob_type != common::ObVarcharType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] LARGE_BINARY converter output must be VARCHAR", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_large_binary;
        desc.convert_datum = convert_datum_large_binary;
      }
      break;

    // ---- date ----
    case arrow::Type::DATE32:
      if (ob_type == common::ObMySQLDateType) {
        desc.kind = ConversionKind::TRANSFORM;
        desc.convert_batch = convert_batch_date32_to_mysql_date;
        desc.convert_datum = convert_datum_date32_to_mysql_date;
      } else if (common::ob_is_date_or_mysql_date(ob_type)) {
        desc.convert_batch = convert_batch_date32;
        desc.convert_datum = convert_datum_date32;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DATE32 must map to ObDateType or ObMySQLDateType",
                 K(ret), K(ob_type));
      }
      break;

    // ---- timestamp ----
    case arrow::Type::TIMESTAMP: {
      desc.kind = ConversionKind::TRANSFORM;
      const arrow::TimestampType &ts_type =
          static_cast<const arrow::TimestampType &>(*arrow_field.type());
      if (common::ob_is_datetime_or_mysql_datetime_tc(ob_type)
          || ob_type == common::ObDateTimeType) {
        const bool is_mysql_datetime = ob_type == common::ObMySQLDateTimeType;
        const bool is_mysql_timestamp = ob_type == common::ObTimestampType;
        const bool has_timezone = !ts_type.timezone().empty();
        switch (ts_type.unit()) {
          case arrow::TimeUnit::SECOND:
            set_timestamp_convert_fns<arrow::TimeUnit::SECOND>(
                is_mysql_datetime, is_mysql_timestamp, has_timezone, desc);
            break;
          case arrow::TimeUnit::MILLI:
            set_timestamp_convert_fns<arrow::TimeUnit::MILLI>(
                is_mysql_datetime, is_mysql_timestamp, has_timezone, desc);
            break;
          case arrow::TimeUnit::MICRO:
            set_timestamp_convert_fns<arrow::TimeUnit::MICRO>(
                is_mysql_datetime, is_mysql_timestamp, has_timezone, desc);
            break;
          case arrow::TimeUnit::NANO:
            set_timestamp_convert_fns<arrow::TimeUnit::NANO>(
                is_mysql_datetime, is_mysql_timestamp, has_timezone, desc);
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("[FlightSQL] unknown timestamp unit", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] TIMESTAMP must map to ObDateTimeType", K(ret), K(ob_type));
      }
      break;
    }

    // ---- time ----
    case arrow::Type::TIME32: {
      desc.kind = ConversionKind::TRANSFORM;
      const arrow::Time32Type &t_type =
          static_cast<const arrow::Time32Type &>(*arrow_field.type());
      if (ob_type != common::ObTimeType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] TIME32 must map to ObTimeType", K(ret), K(ob_type));
      } else if (t_type.unit() == arrow::TimeUnit::SECOND) {
        desc.convert_batch = convert_batch_time32_s;
        desc.convert_datum = convert_datum_time32_s;
      } else {
        desc.convert_batch = convert_batch_time32_ms;
        desc.convert_datum = convert_datum_time32_ms;
      }
      break;
    }
    case arrow::Type::TIME64: {
      desc.kind = ConversionKind::TRANSFORM;
      const arrow::Time64Type &t_type =
          static_cast<const arrow::Time64Type &>(*arrow_field.type());
      if (ob_type != common::ObTimeType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] TIME64 must map to ObTimeType", K(ret), K(ob_type));
      } else if (t_type.unit() == arrow::TimeUnit::MICRO) {
        desc.convert_batch = convert_batch_time64_us;
        desc.convert_datum = convert_datum_time64_us;
      } else {
        desc.convert_batch = convert_batch_time64_ns;
        desc.convert_datum = convert_datum_time64_ns;
      }
      break;
    }

    // ---- collection (FIXED_SIZE_LIST / LIST) ----
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::LIST: {
      if (ob_type != common::ObCollectionSQLType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] Arrow collection must map to ObCollectionSQLType", K(ret), K(ob_type));
      } else {
      desc.kind = ConversionKind::TRANSFORM;
        if (OB_FAIL(build_collection_convert_fn(arrow_field, collection_meta, desc))) {
          LOG_WARN("[FlightSQL] failed to build collection converter", K(ret));
        }
      }
      break;
    }

    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] unsupported Arrow type for write",
               K(ret), "arrow_type_id", static_cast<int>(arrow_id), K(ob_type));
  }
  if (OB_SUCC(ret)
      && (OB_ISNULL(desc.convert_batch) || OB_ISNULL(desc.convert_datum))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] Arrow converter mapping is incomplete",
             K(ret), "arrow_type_id", static_cast<int>(arrow_id), K(ob_type),
             KP(desc.convert_batch), KP(desc.convert_datum));
  }
  return ret;
}

int ob_flight_sql_build_convert_fn(const arrow::Field &arrow_field,
                                    const share::schema::ObColumnSchemaV2 &ob_col,
                                    const common::ObCollectionTypeBase *collection_meta,
                                    ObFlightSqlColumnDesc &desc)
{
  int ret = OB_SUCCESS;
  sql::ObArrowStreamColDesc output_desc;
  if (OB_FAIL(ob_flight_sql_get_output_col_desc(
          arrow_field, ob_col, UINT_MAX16, output_desc))) {
    return ret;
  }
  return build_convert_fn(arrow_field,
                          output_desc.ob_type,
                          output_desc.accuracy.get_precision(),
                          collection_meta,
                          desc);
}

static int resolve_target_collection_meta(const sql::ObExpr &target_expr,
                                          const sql::ObExecContext &exec_ctx,
                                          const common::ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  collection_meta = nullptr;
  const uint16_t subschema_id = target_expr.obj_meta_.get_subschema_id();
  sql::ObSubSchemaValue sub_meta;
  if (UINT_MAX16 == subschema_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] target collection expression has no subschema", K(ret),
             K(target_expr.obj_meta_));
  } else if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(subschema_id, sub_meta))) {
    LOG_WARN("[FlightSQL] failed to resolve target collection subschema", K(ret),
             K(subschema_id));
  } else if (sub_meta.type_ != sql::OB_SUBSCHEMA_COLLECTION_TYPE
             || OB_ISNULL(sub_meta.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid target collection subschema", K(ret), K(subschema_id),
             K(sub_meta.type_), KP(sub_meta.value_));
  } else {
    const common::ObSqlCollectionInfo *collection_info =
        reinterpret_cast<const common::ObSqlCollectionInfo *>(sub_meta.value_);
    if (OB_ISNULL(collection_info->collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] target collection metadata is null", K(ret), K(subschema_id));
    } else {
      collection_meta = collection_info->collection_meta_;
    }
  }
  return ret;
}

int ob_flight_sql_build_convert_fn(const arrow::Field &arrow_field,
                                    const sql::ObExpr &output_expr,
                                    const sql::ObExecContext &exec_ctx,
                                    ObFlightSqlColumnDesc &desc)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  // The V3 physical plan is built from the catalog descriptor returned by
  // ob_flight_sql_get_output_col_desc().  It is therefore the converter
  // output type, not the INSERT target type.  Reusing it here keeps the datum
  // layout and the resolver-visible schema identical on every PX node.
  if (output_expr.datum_meta_.type_ == common::ObCollectionSQLType
      && OB_FAIL(resolve_target_collection_meta(output_expr, exec_ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] failed to resolve V3 output collection metadata", K(ret));
  } else if (OB_FAIL(build_convert_fn(arrow_field,
                                      output_expr.datum_meta_.type_,
                                      output_expr.datum_meta_.precision_,
                                      collection_meta,
                                      desc))) {
    LOG_WARN("[FlightSQL] failed to build V3 Arrow converter from output type", K(ret));
  }
  return ret;
}

int ob_flight_sql_init_streams_conversion_mapping(
    ObFlightSqlIngestEntry &entry,
    const common::ObIArray<sql::ObExpr *> &column_exprs,
    const sql::ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  std::vector<ObFlightSqlSlotInfo> slots;
  {
    std::unique_lock<std::mutex> lock(entry.mu);
    while (entry.mapping_initializing) {
      entry.mapping_cv.wait(lock);
    }
    if (entry.mapping_ready) {
      return entry.mapping_ret;
    } else if (entry.aborted) {
      return OB_CANCELED;
    }
    entry.mapping_initializing = true;
    slots = entry.slots;
  }

  if (OB_UNLIKELY(column_exprs.count() <= 0 || slots.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid V3 conversion mapping inputs", K(ret),
             "column_count", column_exprs.count(), "slot_count", slots.size());
  }

  for (size_t slot_idx = 0; OB_SUCC(ret) && slot_idx < slots.size(); ++slot_idx) {
    const ObFlightSqlSlotInfo &slot = slots[slot_idx];
    ObArrowStreamEntryPtr stream;
    if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] Flight SQL session manager is null", K(ret));
    } else if (OB_FAIL(GCTX.flight_sql_session_mgr_->get_active_stream_entry(
                   slot.sess_id, slot.stream_id, stream))) {
      LOG_WARN("[FlightSQL] local V3 stream is unavailable for conversion mapping",
               K(ret), K(slot_idx), K(slot.sess_id), K(slot.stream_id));
    } else if (OB_ISNULL(stream->arrow_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FlightSQL] local V3 stream has no Arrow schema",
               K(ret), K(slot_idx), K(slot.sess_id), K(slot.stream_id));
    } else if (OB_UNLIKELY(stream->column_mapping.count() != column_exprs.count()
                           || stream->arrow_schema->num_fields() != column_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] V3 stream column count mismatch", K(ret), K(slot_idx),
               "mapping_count", stream->column_mapping.count(),
               "arrow_field_count", stream->arrow_schema->num_fields(),
               "plan_column_count", column_exprs.count());
    } else {
      std::shared_ptr<arrow::Schema> advertised_schema;
      {
        std::lock_guard<std::mutex> lock(entry.mu);
        advertised_schema = entry.advertised_arrow_schema;
      }
      if (OB_ISNULL(advertised_schema)
          || !advertised_schema->Equals(*stream->arrow_schema, false)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] V3 DoPut schema differs from coordinator schema",
                 K(ret), K(slot_idx),
                 "has_advertised_schema", OB_NOT_NULL(advertised_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
        ObFlightSqlColumnDesc &col_desc = stream->column_mapping.at(i);
        sql::ObExpr *target_expr = column_exprs.at(i);
        const int field_idx = col_desc.arrow_field_idx;
        if (OB_ISNULL(target_expr) || field_idx < 0
            || field_idx >= stream->arrow_schema->num_fields()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] invalid V3 conversion column", K(ret), K(slot_idx), K(i),
                   K(field_idx), KP(target_expr));
        } else {
          std::shared_ptr<arrow::Field> arrow_field =
              stream->arrow_schema->field(field_idx);
          if (OB_ISNULL(arrow_field)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] V3 Arrow schema field is null", K(ret),
                     K(slot_idx), K(i), K(field_idx));
          } else if (OB_ISNULL(col_desc.convert_batch)
                     && OB_FAIL(ob_flight_sql_build_convert_fn(
                            *arrow_field, *target_expr, exec_ctx, col_desc))) {
            LOG_WARN("[FlightSQL] failed to build V3 conversion mapping", K(ret),
                     K(slot_idx), K(i), K(target_expr->datum_meta_), K(target_expr->obj_meta_));
          }
        }
      }
    }
  }

  {
    std::lock_guard<std::mutex> lock(entry.mu);
    entry.mapping_ret = ret;
    entry.mapping_ready = true;
    entry.mapping_initializing = false;
    entry.mapping_cv.notify_all();
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
