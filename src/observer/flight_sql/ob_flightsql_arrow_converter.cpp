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
#include <vector>

#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/key_value_metadata.h>

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "lib/ob_errno.h"
#include "lib/number/ob_number_v2.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/udt/ob_array_fixed_size.h"
#include "lib/udt/ob_array_binary.h"
#include "lib/udt/ob_array_nested.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_map_type.h"
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
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
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
using common::ObSqlString;

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
// The Flight converter's expression is the Arrow-derived middle DECIMAL/NUMBER
// carrier, so its p,s must exactly equal the source declaration.  Do not
// rescale here: the normal column conversion in the INSERT owns any later
// destination-specific scale conversion.
static int copy_decimal_int_to_middle(const common::ObDecimalInt *source,
                                      const int32_t source_int_bytes,
                                      const int32_t source_precision,
                                      const int32_t source_scale,
                                      const ObExpr &middle_expr,
                                      common::ObDecimalIntBuilder &result)
{
  int ret = OB_SUCCESS;
  const common::ObPrecision middle_precision = middle_expr.datum_meta_.precision_;
  const common::ObScale middle_scale = middle_expr.datum_meta_.scale_;
  if (OB_ISNULL(source) || source_int_bytes <= 0 || source_precision <= 0
      || source_scale < INT16_MIN || source_scale > INT16_MAX
      || middle_precision != source_precision || middle_scale != source_scale
      || middle_precision < OB_MIN_DECIMAL_PRECISION
      || middle_precision > number::ObNumber::MAX_PRECISION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow or middle decimal metadata", K(ret), KP(source),
             K(source_int_bytes), K(source_precision), K(source_scale),
             K(middle_precision), K(middle_scale));
  } else {
    const int32_t middle_int_bytes =
        wide::ObDecimalIntConstValue::get_int_bytes_by_precision(middle_precision);
    common::ObDecimalIntBuilder source_value;
    common::ObDecimalIntBuilder min_value;
    common::ObDecimalIntBuilder max_value;
    source_value.from(source, source_int_bytes);
    min_value.from(wide::ObDecimalIntConstValue::get_min_value(middle_precision),
                   middle_int_bytes);
    max_value.from(wide::ObDecimalIntConstValue::get_max_value(middle_precision),
                   middle_int_bytes);
    int cmp_min = 0;
    int cmp_max = 0;
    if (OB_FAIL(wide::compare(source_value, min_value, cmp_min))) {
      LOG_WARN("[FlightSQL] failed to compare Arrow decimal lower bound", K(ret));
    } else if (OB_FAIL(wide::compare(source_value, max_value, cmp_max))) {
      LOG_WARN("[FlightSQL] failed to compare Arrow decimal upper bound", K(ret));
    } else if (cmp_min < 0 || cmp_max > 0) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("[FlightSQL] Arrow decimal exceeds declared middle precision", K(ret),
               K(source_precision), K(source_scale), K(middle_precision), K(middle_scale));
    } else {
      result.from(source_value);
      if (result.get_int_bytes() < middle_int_bytes) {
        result.extend(middle_int_bytes);
      } else if (result.get_int_bytes() > middle_int_bytes) {
        // The range check above makes this representation-only narrowing lossless.
        result.truncate(middle_int_bytes);
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

template <typename DecimalArray, typename DecimalType, typename ValueType>
static int convert_batch_decimal_to_decint(const arrow::Array &src,
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
    const DecimalArray &arr = static_cast<const DecimalArray &>(src);
    const DecimalType &type = static_cast<const DecimalType &>(*arr.type());
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        ValueType value;
        MEMCPY(&value, arr.GetValue(i), sizeof(value));
        ObDecimalIntBuilder source;
        ObDecimalIntBuilder result;
        source.from(value);
        if (OB_FAIL(copy_decimal_int_to_middle(
                source.get_decimal_int(), source.get_int_bytes(),
                type.precision(), type.scale(), expr, result))) {
          LOG_WARN("[FlightSQL] failed to convert Arrow decimal to middle DecimalInt",
                   K(ret), K(i));
        } else {
          datums[i].set_decimal_int(result.get_decimal_int(), result.get_int_bytes());
        }
      }
    }
  }
  return ret;
}

template <typename DecimalArray, typename DecimalType, typename ValueType>
static int convert_batch_decimal_to_number(const arrow::Array &src,
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
    const DecimalArray &arr = static_cast<const DecimalArray &>(src);
    const DecimalType &type = static_cast<const DecimalType &>(*arr.type());
    char decimal_buf[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        ValueType value;
        MEMCPY(&value, arr.GetValue(i), sizeof(value));
        ObDecimalIntBuilder decimal_builder;
        decimal_builder.from(value);
        if (OB_FAIL(decimal_int_to_number(
                decimal_builder.get_decimal_int(), decimal_builder.get_int_bytes(),
                type.scale(), decimal_buf, sizeof(decimal_buf), datums[i]))) {
          LOG_WARN("[FlightSQL] failed to convert Arrow decimal to Number", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

static int convert_batch_decimal32_to_decint(const arrow::Array &src,
                                              ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              int64_t nrows,
                                              ObIAllocator &str_arena)
{
  return convert_batch_decimal_to_decint<
      arrow::Decimal32Array, arrow::Decimal32Type, int32_t>(
          src, expr, ctx, nrows, str_arena);
}

static int convert_batch_decimal32_to_number(const arrow::Array &src,
                                              ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              int64_t nrows,
                                              ObIAllocator &str_arena)
{
  return convert_batch_decimal_to_number<
      arrow::Decimal32Array, arrow::Decimal32Type, int32_t>(
          src, expr, ctx, nrows, str_arena);
}

static int convert_batch_decimal64_to_decint(const arrow::Array &src,
                                              ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              int64_t nrows,
                                              ObIAllocator &str_arena)
{
  return convert_batch_decimal_to_decint<
      arrow::Decimal64Array, arrow::Decimal64Type, int64_t>(
          src, expr, ctx, nrows, str_arena);
}

static int convert_batch_decimal64_to_number(const arrow::Array &src,
                                              ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              int64_t nrows,
                                              ObIAllocator &str_arena)
{
  return convert_batch_decimal_to_number<
      arrow::Decimal64Array, arrow::Decimal64Type, int64_t>(
          src, expr, ctx, nrows, str_arena);
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
      if (OB_FAIL(copy_decimal_int_to_middle(
              source.get_decimal_int(), source.get_int_bytes(), type.precision(), type.scale(),
              expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal128 to middle DecimalInt", K(ret), K(i));
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
      if (OB_FAIL(copy_decimal_int_to_middle(
              source.get_decimal_int(), source.get_int_bytes(), type.precision(), type.scale(),
              expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert Decimal256 to middle DecimalInt", K(ret), K(i));
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
      if (OB_FAIL(copy_decimal_int_to_middle(
              source.get_decimal_int(), source.get_int_bytes(),
              type.precision(), type.scale(), expr, result))) {
        LOG_WARN("[FlightSQL] failed to convert scalar Arrow decimal to middle DecimalInt",
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

static int convert_datum_decimal32_to_decint(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_decint<
      arrow::Decimal32Array, arrow::Decimal32Type, int32_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal32_to_number(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_number<
      arrow::Decimal32Array, arrow::Decimal32Type, int32_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal64_to_decint(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_decint<
      arrow::Decimal64Array, arrow::Decimal64Type, int64_t>(
          src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_decimal64_to_number(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  return convert_datum_decimal_to_number<
      arrow::Decimal64Array, arrow::Decimal64Type, int64_t>(
          src, row_idx, expr, ctx, datum, str_arena);
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
// String / Binary converters
//
// The middle carrier is a text or binary LOB.  A LOB datum must contain its
// temporary locator header; retaining an Arrow buffer through set_string()
// would both advertise the wrong type and leave an invalid datum after DoPut
// advances to the next RecordBatch.
// ---------------------------------------------------------------------------

static int set_lob_datum(const char *raw,
                         const int64_t len,
                         const ObExpr &expr,
                         ObEvalCtx &ctx,
                         ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(len < 0
                  || len > common::OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[FlightSQL] Arrow LOB value exceeds the temporary LOB payload limit",
             K(ret), K(len), "max_length",
             common::OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER);
  } else if (OB_UNLIKELY(len > 0 && OB_ISNULL(raw))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow LOB value has a null data pointer", K(ret), K(len));
  } else {
    sql::ObTextStringDatumResult result(expr.datum_meta_.type_, &expr, &ctx, &datum);
    if (OB_FAIL(result.init(len))) {
      LOG_WARN("[FlightSQL] failed to initialize temporary LOB datum", K(ret), K(len),
               "lob_type", expr.datum_meta_.type_);
    } else if (OB_FAIL(result.append(raw == nullptr ? "" : raw, len))) {
      LOG_WARN("[FlightSQL] failed to materialize temporary LOB datum", K(ret), K(len),
               "lob_type", expr.datum_meta_.type_);
    } else {
      result.set_result();
    }
  }
  return ret;
}

template <typename ArrowArray, typename OffsetType>
static int convert_batch_variable_width_lob(const arrow::Array &src,
                                            ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            int64_t nrows,
                                            ObIAllocator &str_arena)
{
  UNUSED(str_arena);
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const ArrowArray &arr = static_cast<const ArrowArray &>(src);
    // ObTextStringDatumResult obtains expression-result memory using the
    // current batch index.  Scope it to each output datum so one LOB row
    // cannot overwrite another row's temporary locator/payload.
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(nrows);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      batch_info_guard.set_batch_idx(i);
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        OffsetType len = 0;
        const char *raw = reinterpret_cast<const char *>(arr.GetValue(i, &len));
        if (OB_FAIL(set_lob_datum(raw, static_cast<int64_t>(len), expr, ctx, datums[i]))) {
          LOG_WARN("[FlightSQL] failed to convert variable-width Arrow LOB value", K(ret),
                   K(i), K(len));
        }
      }
    }
  }
  return ret;
}

static int convert_batch_string(const arrow::Array &src,
                                ObExpr &expr,
                                ObEvalCtx &ctx,
                                int64_t nrows,
                                ObIAllocator &str_arena)
{
  return convert_batch_variable_width_lob<arrow::StringArray, int32_t>(
      src, expr, ctx, nrows, str_arena);
}

static int convert_batch_large_string(const arrow::Array &src,
                                      ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      int64_t nrows,
                                      ObIAllocator &str_arena)
{
  return convert_batch_variable_width_lob<arrow::LargeStringArray, int64_t>(
      src, expr, ctx, nrows, str_arena);
}

static int convert_batch_binary(const arrow::Array &src,
                                ObExpr &expr,
                                ObEvalCtx &ctx,
                                int64_t nrows,
                                ObIAllocator &str_arena)
{
  return convert_batch_variable_width_lob<arrow::BinaryArray, int32_t>(
      src, expr, ctx, nrows, str_arena);
}

static int convert_batch_large_binary(const arrow::Array &src,
                                      ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      int64_t nrows,
                                      ObIAllocator &str_arena)
{
  return convert_batch_variable_width_lob<arrow::LargeBinaryArray, int64_t>(
      src, expr, ctx, nrows, str_arena);
}

static int convert_batch_fixed_size_binary(const arrow::Array &src,
                                           ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           int64_t nrows,
                                           ObIAllocator &str_arena)
{
  UNUSED(str_arena);
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const arrow::FixedSizeBinaryArray &arr =
        static_cast<const arrow::FixedSizeBinaryArray &>(src);
    const int32_t byte_width = arr.byte_width();
    if (OB_UNLIKELY(byte_width < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] Arrow fixed-size binary has an invalid byte width", K(ret),
               K(byte_width));
    }
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(nrows);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      batch_info_guard.set_batch_idx(i);
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else if (OB_FAIL(set_lob_datum(
                     reinterpret_cast<const char *>(arr.GetValue(i)), byte_width,
                     expr, ctx, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert fixed-size binary Arrow LOB value", K(ret),
                 K(i), K(byte_width));
      }
    }
  }
  return ret;
}

template <typename ArrowArray, typename OffsetType>
static int convert_datum_variable_width_lob(const arrow::Array &src,
                                             const int64_t row_idx,
                                             const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &datum,
                                             ObIAllocator &str_arena)
{
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const ArrowArray &arr = static_cast<const ArrowArray &>(src);
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(row_idx);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      OffsetType len = 0;
      const char *raw = reinterpret_cast<const char *>(arr.GetValue(row_idx, &len));
      if (OB_FAIL(set_lob_datum(raw, static_cast<int64_t>(len), expr, ctx, datum))) {
        LOG_WARN("[FlightSQL] failed to convert scalar variable-width Arrow LOB value",
                 K(ret), K(row_idx), K(len));
      }
    }
  }
  return ret;
}

static int convert_datum_string(const arrow::Array &src,
                                const int64_t row_idx,
                                const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &datum,
                                ObIAllocator &str_arena)
{
  return convert_datum_variable_width_lob<arrow::StringArray, int32_t>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_large_string(const arrow::Array &src,
                                      const int64_t row_idx,
                                      const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &datum,
                                      ObIAllocator &str_arena)
{
  return convert_datum_variable_width_lob<arrow::LargeStringArray, int64_t>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_binary(const arrow::Array &src,
                                const int64_t row_idx,
                                const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &datum,
                                ObIAllocator &str_arena)
{
  return convert_datum_variable_width_lob<arrow::BinaryArray, int32_t>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_large_binary(const arrow::Array &src,
                                      const int64_t row_idx,
                                      const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &datum,
                                      ObIAllocator &str_arena)
{
  return convert_datum_variable_width_lob<arrow::LargeBinaryArray, int64_t>(
      src, row_idx, expr, ctx, datum, str_arena);
}

static int convert_datum_fixed_size_binary(const arrow::Array &src,
                                           const int64_t row_idx,
                                           const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &datum,
                                           ObIAllocator &str_arena)
{
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::FixedSizeBinaryArray &arr =
        static_cast<const arrow::FixedSizeBinaryArray &>(src);
    const int32_t byte_width = arr.byte_width();
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(row_idx);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else if (OB_UNLIKELY(byte_width < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] Arrow fixed-size binary has an invalid byte width", K(ret),
               K(byte_width));
    } else if (OB_FAIL(set_lob_datum(
                   reinterpret_cast<const char *>(arr.GetValue(row_idx)), byte_width,
                   expr, ctx, datum))) {
      LOG_WARN("[FlightSQL] failed to convert scalar fixed-size binary Arrow LOB value",
               K(ret), K(row_idx), K(byte_width));
    }
  }
  return ret;
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

// Arrow DATE64 is milliseconds since the Unix epoch.  The agreed middle
// carrier keeps that instant-shaped integer as microseconds: MySQL uses its
// TIMESTAMP carrier and Oracle uses timezone-free TIMESTAMP.  Do not decompose
// it into calendar fields or apply a session-time-zone adjustment here.
static int set_date64_datum(const int64_t milliseconds,
                            const common::ObObjType middle_type,
                            ObDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t value_us = 0;
  if (OB_UNLIKELY(IS_MULTI_OVERFLOW64(milliseconds, 1000L))) {
    ret = OB_DATA_OUT_OF_RANGE;
  } else {
    value_us = milliseconds * 1000L;
    if (middle_type == common::ObTimestampType) {
      if (OB_UNLIKELY(value_us < MYSQL_TIMESTAMP_MIN_VAL
                      || value_us > MYSQL_TIMESTAMP_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        datum.set_timestamp(value_us);
      }
    } else if (middle_type == common::ObTimestampNanoType) {
      if (OB_UNLIKELY(value_us < DATETIME_MIN_VAL || value_us > DATETIME_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        common::ObOTimestampData timestamp;
        timestamp.time_us_ = value_us;
        datum.set_otimestamp_tiny(timestamp);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] DATE64 has no representable middle timestamp value", K(ret),
             K(milliseconds), K(value_us), K(middle_type));
  }
  return ret;
}

static int convert_batch_date64(const arrow::Array &src,
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
    const arrow::Date64Array &arr = static_cast<const arrow::Date64Array &>(src);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else if (OB_FAIL(set_date64_datum(
                     arr.Value(i), expr.datum_meta_.type_, datums[i]))) {
        LOG_WARN("[FlightSQL] failed to convert Arrow DATE64", K(ret), K(i),
                 "milliseconds", arr.Value(i));
      }
    }
  }
  return ret;
}

static int convert_datum_date64(const arrow::Array &src,
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
    const arrow::Date64Array &arr = static_cast<const arrow::Date64Array &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else if (OB_FAIL(set_date64_datum(
                   arr.Value(row_idx), expr.datum_meta_.type_, datum))) {
      LOG_WARN("[FlightSQL] failed to convert scalar Arrow DATE64", K(ret), K(row_idx),
               "milliseconds", arr.Value(row_idx));
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
      // timestamp[ns] is deliberately rejected at schema registration.  Keep
      // the runtime helper strict as well so a stale or malformed mapping can
      // never silently truncate nanoseconds in the execution loop.
      ret = OB_NOT_SUPPORTED;
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

// Oracle TIMESTAMP middle carriers do not consult the session timezone.
// A timezone-naive Arrow timestamp is already a wall-clock value, so its
// microsecond integer is copied unchanged into TIMESTAMP.  A timezone-aware
// Arrow timestamp is an instant, so the same integer is stored with the
// canonical +00:00 offset in TIMESTAMP WITH TIME ZONE.
template <bool WITH_TIMEZONE>
static int set_oracle_timestamp_datum(const int64_t timestamp_us, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(timestamp_us < DATETIME_MIN_VAL || timestamp_us > DATETIME_MAX_VAL)) {
    ret = OB_DATA_OUT_OF_RANGE;
  } else {
    common::ObOTimestampData value;
    value.time_us_ = timestamp_us;
    if (WITH_TIMEZONE) {
      value.time_ctx_.set_offset_min(0);
      datum.set_otimestamp_tz(value);
    } else {
      datum.set_otimestamp_tiny(value);
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT, bool WITH_TIMEZONE>
static int convert_batch_timestamp_to_oracle_middle(const arrow::Array &src,
                                                    ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    int64_t nrows,
                                                    ObIAllocator &)
{
  UNUSED(expr);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObDatum *datums = init_uniform(expr, ctx, nrows);
  if (OB_ISNULL(datums)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const arrow::TimestampArray &arr = static_cast<const arrow::TimestampArray &>(src);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        const int64_t source_value = arr.Value(i);
        int64_t timestamp_us = 0;
        if (OB_FAIL(arrow_timestamp_to_microseconds(source_value, UNIT, timestamp_us))) {
          LOG_WARN("[FlightSQL] failed to normalize Arrow Oracle timestamp", K(ret),
                   K(source_value), K(i));
        } else if (OB_FAIL((set_oracle_timestamp_datum<WITH_TIMEZONE>(
                       timestamp_us, datums[i])))) {
          LOG_WARN("[FlightSQL] failed to materialize Arrow Oracle timestamp middle datum",
                   K(ret), K(source_value), K(timestamp_us), K(i), K(WITH_TIMEZONE));
        }
      }
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT, bool WITH_TIMEZONE>
static int convert_datum_timestamp_to_oracle_middle(
    const arrow::Array &src, const int64_t row_idx, const ObExpr &expr,
    ObEvalCtx &ctx, ObDatum &datum, ObIAllocator &str_arena)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(str_arena);
  int ret = check_scalar_row_idx(src, row_idx);
  if (OB_SUCC(ret)) {
    const arrow::TimestampArray &arr = static_cast<const arrow::TimestampArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const int64_t source_value = arr.Value(row_idx);
      int64_t timestamp_us = 0;
      if (OB_FAIL(arrow_timestamp_to_microseconds(source_value, UNIT, timestamp_us))) {
        LOG_WARN("[FlightSQL] failed to normalize scalar Arrow Oracle timestamp", K(ret),
                 K(source_value), K(row_idx));
      } else if (OB_FAIL((set_oracle_timestamp_datum<WITH_TIMEZONE>(
                     timestamp_us, datum)))) {
        LOG_WARN("[FlightSQL] failed to materialize scalar Arrow Oracle timestamp middle datum",
                 K(ret), K(source_value), K(timestamp_us), K(row_idx), K(WITH_TIMEZONE));
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

static int arrow_time_value_to_microseconds(const int64_t value,
                                            const arrow::TimeUnit::type unit,
                                            int64_t &value_us)
{
  int ret = OB_SUCCESS;
  int64_t multiplier = 1;
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      multiplier = USECS_PER_SEC;
      break;
    case arrow::TimeUnit::MILLI:
      multiplier = 1000L;
      break;
    case arrow::TimeUnit::MICRO:
      multiplier = 1;
      break;
    case arrow::TimeUnit::NANO:
      // The schema path rejects nanoseconds.  Keep this row helper strict so
      // an already-created converter can never truncate a value silently.
      ret = OB_NOT_SUPPORTED;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(IS_MULTI_OVERFLOW64(value, multiplier))) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      value_us = value * multiplier;
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT>
static int convert_batch_duration(const arrow::Array &src,
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
    const arrow::DurationArray &arr = static_cast<const arrow::DurationArray &>(src);
    for (int64_t i = 0; OB_SUCC(ret) && i < nrows; ++i) {
      if (arr.IsNull(i)) {
        datums[i].set_null();
      } else {
        int64_t value_us = 0;
        if (OB_FAIL(arrow_time_value_to_microseconds(arr.Value(i), UNIT, value_us))) {
          LOG_WARN("[FlightSQL] failed to scale Arrow duration to microseconds", K(ret),
                   K(i), "value", arr.Value(i), K(UNIT));
        } else if (OB_FAIL(set_time_datum(value_us, datums[i]))) {
          LOG_WARN("[FlightSQL] Arrow duration exceeds the TIME middle carrier", K(ret),
                   K(i), "value", arr.Value(i), K(value_us), K(UNIT));
        }
      }
    }
  }
  return ret;
}

template <arrow::TimeUnit::type UNIT>
static int convert_datum_duration(const arrow::Array &src,
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
    const arrow::DurationArray &arr = static_cast<const arrow::DurationArray &>(src);
    if (arr.IsNull(row_idx)) {
      datum.set_null();
    } else {
      int64_t value_us = 0;
      if (OB_FAIL(arrow_time_value_to_microseconds(arr.Value(row_idx), UNIT, value_us))) {
        LOG_WARN("[FlightSQL] failed to scale scalar Arrow duration to microseconds",
                 K(ret), K(row_idx), "value", arr.Value(row_idx), K(UNIT));
      } else if (OB_FAIL(set_time_datum(value_us, datum))) {
        LOG_WARN("[FlightSQL] scalar Arrow duration exceeds the TIME middle carrier",
                 K(ret), K(row_idx), "value", arr.Value(row_idx), K(value_us), K(UNIT));
      }
    }
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

template <typename CppType, typename ArrowArray>
static int append_arrow_basic_values_to_array(const arrow::Array &values,
                                              const int64_t start,
                                              const int64_t count,
                                              common::ObIArrayType &output)
{
  int ret = OB_SUCCESS;
  using namespace common;
  ObArrayFixedSize<CppType> *target =
      dynamic_cast<ObArrayFixedSize<CppType> *>(&output);
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY object does not match basic target metadata", K(ret),
             "native_format", output.get_format(),
             "native_element_type", output.get_element_type());
  } else if (OB_UNLIKELY(start < 0 || count < 0 || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow ARRAY child range", K(ret), K(start), K(count),
             "value_count", values.length());
  } else {
    const ArrowArray &source = static_cast<const ArrowArray &>(values);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const int64_t value_idx = start + idx;
      if (source.IsNull(value_idx)) {
        if (OB_FAIL(target->push_null())) {
          LOG_WARN("append null native ARRAY element failed", K(ret), K(value_idx));
        }
      } else if (OB_FAIL(target->push_back(
                     static_cast<CppType>(source.Value(value_idx))))) {
        LOG_WARN("append native ARRAY element failed", K(ret), K(value_idx));
      }
    }
  }
  return ret;
}

static int append_arrow_bool_values_to_array(const arrow::Array &values,
                                             const int64_t start,
                                             const int64_t count,
                                             common::ObIArrayType &output)
{
  int ret = OB_SUCCESS;
  common::ObArrayFixedSize<int8_t> *target =
      dynamic_cast<common::ObArrayFixedSize<int8_t> *>(&output);
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY object does not match BOOLEAN middle carrier", K(ret),
             "native_format", output.get_format(),
             "native_element_type", output.get_element_type());
  } else if (OB_UNLIKELY(start < 0 || count < 0 || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow BOOLEAN collection child range", K(ret), K(start), K(count),
             "value_count", values.length());
  } else {
    const arrow::BooleanArray &source = static_cast<const arrow::BooleanArray &>(values);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const int64_t value_idx = start + idx;
      if (source.IsNull(value_idx)) {
        if (OB_FAIL(target->push_null())) {
          LOG_WARN("append null BOOLEAN ARRAY element failed", K(ret), K(value_idx));
        }
      } else if (OB_FAIL(target->push_back(source.Value(value_idx) ? 1 : 0))) {
        LOG_WARN("append BOOLEAN ARRAY element failed", K(ret), K(value_idx));
      }
    }
  }
  return ret;
}

template <typename ArrowArray, typename OffsetType>
static int append_arrow_variable_values_to_array(const arrow::Array &values,
                                                 const int64_t start,
                                                 const int64_t count,
                                                 common::ObIArrayType &output)
{
  int ret = OB_SUCCESS;
  common::ObArrayBinary *target = dynamic_cast<common::ObArrayBinary *>(&output);
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY object does not match VARCHAR middle carrier", K(ret),
             "native_format", output.get_format(),
             "native_element_type", output.get_element_type());
  } else if (OB_UNLIKELY(start < 0 || count < 0 || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow variable-width collection child range", K(ret), K(start), K(count),
             "value_count", values.length());
  } else {
    const ArrowArray &source = static_cast<const ArrowArray &>(values);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const int64_t value_idx = start + idx;
      if (source.IsNull(value_idx)) {
        if (OB_FAIL(target->push_null())) {
          LOG_WARN("append null VARCHAR ARRAY element failed", K(ret), K(value_idx));
        }
      } else {
        OffsetType len = 0;
        const char *raw = reinterpret_cast<const char *>(source.GetValue(value_idx, &len));
        if (OB_UNLIKELY(len < 0 || len > INT32_MAX)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("Arrow variable-width collection value is too large", K(ret),
                   K(value_idx), K(len));
        } else {
          const common::ObString value(static_cast<int32_t>(len), raw);
          if (OB_FAIL(target->push_back(value))) {
            LOG_WARN("append VARCHAR ARRAY element failed", K(ret), K(value_idx), K(len));
          }
        }
      }
    }
  }
  return ret;
}

static int append_arrow_fixed_size_binary_values_to_array(
    const arrow::Array &values,
    const int64_t start,
    const int64_t count,
    common::ObIArrayType &output)
{
  int ret = OB_SUCCESS;
  common::ObArrayBinary *target = dynamic_cast<common::ObArrayBinary *>(&output);
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY object does not match binary middle carrier", K(ret),
             "native_format", output.get_format(),
             "native_element_type", output.get_element_type());
  } else if (OB_UNLIKELY(start < 0 || count < 0 || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow fixed-size-binary collection child range", K(ret),
             K(start), K(count), "value_count", values.length());
  } else {
    const arrow::FixedSizeBinaryArray &source =
        static_cast<const arrow::FixedSizeBinaryArray &>(values);
    const int32_t byte_width = source.byte_width();
    if (OB_UNLIKELY(byte_width < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Arrow fixed-size-binary collection child has an invalid byte width", K(ret),
               K(byte_width));
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const int64_t value_idx = start + idx;
      if (source.IsNull(value_idx)) {
        if (OB_FAIL(target->push_null())) {
          LOG_WARN("append null binary ARRAY element failed", K(ret), K(value_idx));
        }
      } else {
        const common::ObString value(
            byte_width, reinterpret_cast<const char *>(source.GetValue(value_idx)));
        if (OB_FAIL(target->push_back(value))) {
          LOG_WARN("append binary ARRAY element failed", K(ret), K(value_idx), K(byte_width));
        }
      }
    }
  }
  return ret;
}

static int append_arrow_basic_values_to_array(
    const arrow::Array &values,
    const int64_t start,
    const int64_t count,
    const common::ObCollectionBasicType &target_type,
    common::ObIArrayType &output)
{
  int ret = OB_SUCCESS;
  using namespace common;
  const arrow::Type::type arrow_type = values.type()->id();
  switch (target_type.basic_meta_.get_obj_type()) {
    case ObTinyIntType:
      if (arrow_type == arrow::Type::BOOL) {
        ret = append_arrow_bool_values_to_array(values, start, count, output);
      } else {
        ret = arrow_type == arrow::Type::INT8
            ? append_arrow_basic_values_to_array<int8_t, arrow::Int8Array>(
                  values, start, count, output)
            : OB_NOT_SUPPORTED;
      }
      break;
    case ObSmallIntType:
      ret = arrow_type == arrow::Type::INT16
          ? append_arrow_basic_values_to_array<int16_t, arrow::Int16Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObInt32Type:
      ret = arrow_type == arrow::Type::INT32
          ? append_arrow_basic_values_to_array<int32_t, arrow::Int32Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObIntType:
      ret = arrow_type == arrow::Type::INT64
          ? append_arrow_basic_values_to_array<int64_t, arrow::Int64Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObUTinyIntType:
      ret = arrow_type == arrow::Type::UINT8
          ? append_arrow_basic_values_to_array<uint8_t, arrow::UInt8Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObUSmallIntType:
      ret = arrow_type == arrow::Type::UINT16
          ? append_arrow_basic_values_to_array<uint16_t, arrow::UInt16Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObUInt32Type:
      ret = arrow_type == arrow::Type::UINT32
          ? append_arrow_basic_values_to_array<uint32_t, arrow::UInt32Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObUInt64Type:
      ret = arrow_type == arrow::Type::UINT64
          ? append_arrow_basic_values_to_array<uint64_t, arrow::UInt64Array>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObFloatType:
    case ObUFloatType:
      ret = arrow_type == arrow::Type::FLOAT
          ? append_arrow_basic_values_to_array<float, arrow::FloatArray>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObDoubleType:
    case ObUDoubleType:
      ret = arrow_type == arrow::Type::DOUBLE
          ? append_arrow_basic_values_to_array<double, arrow::DoubleArray>(
                values, start, count, output)
          : OB_NOT_SUPPORTED;
      break;
    case ObVarcharType:
      switch (arrow_type) {
        case arrow::Type::STRING:
          ret = append_arrow_variable_values_to_array<arrow::StringArray, int32_t>(
              values, start, count, output);
          break;
        case arrow::Type::LARGE_STRING:
          ret = append_arrow_variable_values_to_array<arrow::LargeStringArray, int64_t>(
              values, start, count, output);
          break;
        case arrow::Type::BINARY:
          ret = append_arrow_variable_values_to_array<arrow::BinaryArray, int32_t>(
              values, start, count, output);
          break;
        case arrow::Type::LARGE_BINARY:
          ret = append_arrow_variable_values_to_array<arrow::LargeBinaryArray, int64_t>(
              values, start, count, output);
          break;
        case arrow::Type::FIXED_SIZE_BINARY:
          ret = append_arrow_fixed_size_binary_values_to_array(
              values, start, count, output);
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          break;
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  if (OB_NOT_SUPPORTED == ret) {
    LOG_WARN("Arrow ARRAY child type does not match native collection leaf", K(ret),
             "arrow_type", static_cast<int>(arrow_type),
             "target_type", target_type.basic_meta_.get_obj_type());
  }
  return ret;
}

static int append_arrow_list_value_to_array(
    const arrow::ListArray &source,
    const int64_t row_idx,
    const common::ObCollectionArrayType &target_type,
    common::ObIArrayType &output,
    ObIAllocator &work_arena);

static int get_arrow_list_value_range(
    const arrow::Array &source,
    const int64_t row_idx,
    std::shared_ptr<arrow::Array> &values,
    int64_t &start,
    int64_t &count);

static int append_arrow_list_or_fixed_value_to_array(
    const arrow::Array &source,
    const int64_t row_idx,
    const common::ObCollectionArrayType &target_type,
    common::ObIArrayType &output,
    ObIAllocator &work_arena);

static int append_arrow_collection_elements_to_array(
    const arrow::Array &values,
    const int64_t start,
    const int64_t count,
    const common::ObCollectionTypeBase &target_element_type,
    common::ObIArrayType &output,
    ObIAllocator &work_arena)
{
  int ret = OB_SUCCESS;
  using namespace common;
  if (OB_UNLIKELY(start < 0 || count < 0 || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow collection child range", K(ret), K(start), K(count),
             "value_count", values.length());
  } else if (target_element_type.type_id_ == OB_BASIC_TYPE) {
    ret = append_arrow_basic_values_to_array(
        values, start, count,
        static_cast<const ObCollectionBasicType &>(target_element_type), output);
  } else if (target_element_type.type_id_ == OB_ARRAY_TYPE) {
    ObArrayNested *nested_output = dynamic_cast<ObArrayNested *>(&output);
    if ((values.type()->id() != arrow::Type::LIST
         && values.type()->id() != arrow::Type::LARGE_LIST
         && values.type()->id() != arrow::Type::FIXED_SIZE_LIST)
        || OB_ISNULL(nested_output)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested ARRAY requires Arrow LIST/LARGE_LIST/FIXED_SIZE_LIST and native ObArrayNested", K(ret),
               "arrow_type", static_cast<int>(values.type()->id()), KP(nested_output));
    } else {
      const ObCollectionArrayType &nested_type =
          static_cast<const ObCollectionArrayType &>(target_element_type);
      for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
        const int64_t value_idx = start + idx;
        if (values.IsNull(value_idx)) {
          if (OB_FAIL(nested_output->push_null())) {
            LOG_WARN("append null nested ARRAY element failed", K(ret), K(value_idx));
          }
        } else {
          ObIArrayType *child_output = nullptr;
          std::shared_ptr<arrow::Array> child_values;
          int64_t child_start = 0;
          int64_t child_count = 0;
          if (OB_FAIL(get_arrow_list_value_range(
                  values, value_idx, child_values, child_start, child_count))) {
            LOG_WARN("validate Arrow nested ARRAY child failed", K(ret), K(value_idx));
          } else if (OB_FAIL(ObArrayTypeObjFactory::construct(
                  work_arena, nested_type, child_output, false))) {
            LOG_WARN("construct native nested ARRAY child failed", K(ret), K(value_idx));
          } else if (OB_FAIL(append_arrow_list_or_fixed_value_to_array(
                         values, value_idx, nested_type,
                         *child_output, work_arena))) {
            LOG_WARN("append Arrow nested ARRAY child failed", K(ret), K(value_idx));
          } else if (OB_FAIL(child_output->init())) {
            // ObArrayFixedSize stores appended values in its mutable container
            // until init() publishes the data/null pointers used by
            // ObArrayNested::push_back().  Without this, push_back() passes a
            // null source buffer to insert_from(), which can crash the server.
            LOG_WARN("finalize native nested ARRAY child failed", K(ret), K(value_idx));
          } else if (OB_FAIL(nested_output->push_back(*child_output))) {
            LOG_WARN("append native nested ARRAY child failed", K(ret), K(value_idx));
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported nested ARRAY element metadata", K(ret),
             "element_type", target_element_type.type_id_);
  }
  return ret;
}

static int append_arrow_list_value_to_array(
    const arrow::ListArray &source,
    const int64_t row_idx,
    const common::ObCollectionArrayType &target_type,
    common::ObIArrayType &output,
    ObIAllocator &work_arena)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<arrow::Array> values;
  int64_t start = 0;
  int64_t count = 0;
  if (OB_ISNULL(target_type.element_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY metadata is null", K(ret), KP(target_type.element_type_));
  } else if (OB_FAIL(get_arrow_list_value_range(source, row_idx, values, start, count))) {
    LOG_WARN("validate Arrow LIST value range failed", K(ret), K(row_idx));
  } else {
    ret = append_arrow_collection_elements_to_array(
        *values, start, count, *target_type.element_type_, output, work_arena);
  }
  return ret;
}

static int get_arrow_list_value_range(
    const arrow::Array &source,
    const int64_t row_idx,
    std::shared_ptr<arrow::Array> &values,
    int64_t &start,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  values.reset();
  start = 0;
  count = 0;
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= source.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow collection row index", K(ret), K(row_idx),
             "row_count", source.length());
  } else if (source.IsNull(row_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null Arrow collection must be handled by its parent", K(ret), K(row_idx));
  } else if (source.type()->id() == arrow::Type::LIST) {
    const arrow::ListArray &list = static_cast<const arrow::ListArray &>(source);
    values = list.values();
    start = list.value_offset(row_idx);
    count = list.value_length(row_idx);
  } else if (source.type()->id() == arrow::Type::LARGE_LIST) {
    const arrow::LargeListArray &list =
        static_cast<const arrow::LargeListArray &>(source);
    values = list.values();
    start = list.value_offset(row_idx);
    count = list.value_length(row_idx);
  } else if (source.type()->id() == arrow::Type::FIXED_SIZE_LIST) {
    const arrow::FixedSizeListArray &list =
        static_cast<const arrow::FixedSizeListArray &>(source);
    values = list.values();
    start = list.value_offset(row_idx);
    count = list.value_length(row_idx);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("native ARRAY requires Arrow LIST, LARGE_LIST, or FIXED_SIZE_LIST", K(ret),
             "arrow_type", static_cast<int>(source.type()->id()));
  }
  if (OB_SUCC(ret) && !values) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Arrow collection has no values child", K(ret), K(row_idx),
             "arrow_type", static_cast<int>(source.type()->id()));
  } else if (OB_SUCC(ret)
             && (start < 0 || count < 0 || start > values->length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow collection value range", K(ret), K(row_idx), K(start), K(count),
             "value_count", values->length());
  } else if (OB_SUCC(ret) && count > common::MAX_ARRAY_ELEMENT_SIZE) {
    // Arrow exposes this count before native ARRAY construction.  Reject it
    // here so even nested LARGE_LIST values cannot trigger a large workspace
    // allocation or child conversion.
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Arrow collection value exceeds OceanBase ARRAY element limit", K(ret),
             K(row_idx), K(count), K(common::MAX_ARRAY_ELEMENT_SIZE));
  }
  return ret;
}

static int append_arrow_list_or_fixed_value_to_array(
    const arrow::Array &source,
    const int64_t row_idx,
    const common::ObCollectionArrayType &target_type,
    common::ObIArrayType &output,
    ObIAllocator &work_arena)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<arrow::Array> values;
  int64_t start = 0;
  int64_t count = 0;
  if (OB_ISNULL(target_type.element_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native ARRAY metadata is null", K(ret), KP(target_type.element_type_));
  } else if (OB_FAIL(get_arrow_list_value_range(source, row_idx, values, start, count))) {
    LOG_WARN("validate Arrow collection value range failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret)) {
    ret = append_arrow_collection_elements_to_array(
        *values, start, count, *target_type.element_type_, output, work_arena);
  }
  return ret;
}

static bool is_recursive_array_type(const common::ObCollectionTypeBase *collection_type)
{
  bool is_recursive = false;
  if (OB_NOT_NULL(collection_type)
      && collection_type->type_id_ == common::OB_ARRAY_TYPE) {
    const common::ObCollectionArrayType *array_type =
        static_cast<const common::ObCollectionArrayType *>(collection_type);
    is_recursive = OB_NOT_NULL(array_type->element_type_)
                   && array_type->element_type_->type_id_ == common::OB_ARRAY_TYPE;
  }
  return is_recursive;
}

static int write_nested_ob_array_row(const arrow::ListArray &source,
                                     const int64_t row_idx,
                                     const common::ObCollectionArrayType &target_type,
                                     ObIAllocator &work_arena,
                                     ObIAllocator &str_arena,
                                     ObDatum &datum)
{
  int ret = OB_SUCCESS;
  common::ObIArrayType *array = nullptr;
  if (OB_FAIL(common::ObArrayTypeObjFactory::construct(
          work_arena, target_type, array, false))) {
    LOG_WARN("construct native nested ARRAY failed", K(ret));
  } else if (OB_FAIL(append_arrow_list_value_to_array(
                 source, row_idx, target_type, *array, work_arena))) {
    LOG_WARN("convert Arrow nested ARRAY failed", K(ret), K(row_idx));
  } else {
    const int32_t raw_len = array->get_raw_binary_len();
    ObString dst;
    if (OB_UNLIKELY(raw_len <= 0 || raw_len > common::MAX_ARRAY_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("nested ARRAY serialized size is invalid", K(ret), K(raw_len),
               K(common::MAX_ARRAY_SIZE));
    } else if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
                   array, raw_len, str_arena, dst))) {
      LOG_WARN("serialize nested ARRAY into owned datum memory failed", K(ret),
               K(raw_len));
    } else {
      // set_array_res copies the native object into str_arena.  work_arena can
      // be reset immediately after this callback, and no Arrow RecordBatch
      // buffer is retained after DoPut advances to its next batch.
      datum.set_string(dst);
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

static int resolve_expr_collection_meta(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const common::ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  collection_meta = nullptr;
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  common::ObCollectionTypeBase *resolved_meta = nullptr;
  if (UINT_MAX16 == subschema_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] collection output expression has no subschema", K(ret),
             K(expr.obj_meta_));
  } else if (OB_FAIL(sql::ObArrayExprUtils::get_coll_type_by_subschema_id(
                 &ctx.exec_ctx_, subschema_id, resolved_meta))) {
    LOG_WARN("[FlightSQL] resolve collection output subschema failed", K(ret),
             K(subschema_id));
  } else if (OB_ISNULL(resolved_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] collection output metadata is null", K(ret),
             K(subschema_id));
  } else {
    collection_meta = resolved_meta;
  }
  return ret;
}

static int write_ob_collection_array_row(
    const arrow::Array &source,
    const int64_t row_idx,
    const common::ObCollectionArrayType &middle_type,
    ObIAllocator &work_arena,
    ObIAllocator &str_arena,
    ObDatum &datum)
{
  int ret = OB_SUCCESS;
  common::ObIArrayType *array = nullptr;
  std::shared_ptr<arrow::Array> values;
  int64_t start = 0;
  int64_t count = 0;
  if (OB_FAIL(get_arrow_list_value_range(source, row_idx, values, start, count))) {
    LOG_WARN("validate Arrow middle ARRAY row failed", K(ret), K(row_idx));
  } else if (OB_FAIL(common::ObArrayTypeObjFactory::construct(
          work_arena, middle_type, array, false))) {
    LOG_WARN("construct native middle ARRAY failed", K(ret));
  } else if (OB_FAIL(append_arrow_list_or_fixed_value_to_array(
                 source, row_idx, middle_type, *array, work_arena))) {
    LOG_WARN("convert Arrow value to native middle ARRAY failed", K(ret), K(row_idx));
  } else {
    const int32_t raw_len = array->get_raw_binary_len();
    ObString dst;
    if (OB_UNLIKELY(raw_len <= 0 || raw_len > common::MAX_ARRAY_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("middle ARRAY serialized size is invalid", K(ret), K(raw_len),
               K(common::MAX_ARRAY_SIZE));
    } else if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
                   array, raw_len, str_arena, dst))) {
      LOG_WARN("serialize middle ARRAY into owned datum memory failed", K(ret), K(raw_len));
    } else {
      datum.set_string(dst);
    }
  }
  return ret;
}

static int convert_batch_array(const arrow::Array &src,
                               ObExpr &expr,
                               ObEvalCtx &ctx,
                               int64_t nrows,
                               ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  ObDatum *datums = nullptr;
  if (OB_UNLIKELY((src.type()->id() != arrow::Type::LIST
                   && src.type()->id() != arrow::Type::LARGE_LIST
                   && src.type()->id() != arrow::Type::FIXED_SIZE_LIST)
                  || nrows < 0 || nrows > src.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow ARRAY batch", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()), K(nrows),
             "array_length", src.length());
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve middle ARRAY metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_ARRAY_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] Arrow LIST/LARGE_LIST/FIXED_SIZE_LIST does not map to middle ARRAY", K(ret),
             KP(collection_meta));
  } else if (OB_ISNULL(datums = init_uniform(expr, ctx, nrows))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const common::ObCollectionArrayType &middle_type =
        static_cast<const common::ObCollectionArrayType &>(*collection_meta);
    common::ObArenaAllocator work_arena(
        "FlSqlArrW", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < nrows; ++row_idx) {
      if (src.IsNull(row_idx)) {
        datums[row_idx].set_null();
      } else {
        ret = write_ob_collection_array_row(
            src, row_idx, middle_type, work_arena, str_arena, datums[row_idx]);
        work_arena.reuse();
        if (OB_FAIL(ret)) {
          LOG_WARN("[FlightSQL] convert Arrow middle ARRAY row failed", K(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}

static int convert_datum_array(const arrow::Array &src,
                               const int64_t row_idx,
                               const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &datum,
                               ObIAllocator &str_arena)
{
  int ret = check_scalar_row_idx(src, row_idx);
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(src.type()->id() != arrow::Type::LIST
                         && src.type()->id() != arrow::Type::LARGE_LIST
                         && src.type()->id() != arrow::Type::FIXED_SIZE_LIST)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] middle ARRAY requires Arrow LIST, LARGE_LIST, or FIXED_SIZE_LIST", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()));
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve scalar middle ARRAY metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_ARRAY_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] scalar Arrow list does not map to middle ARRAY", K(ret),
             KP(collection_meta));
  } else if (src.IsNull(row_idx)) {
    datum.set_null();
  } else {
    const common::ObCollectionArrayType &middle_type =
        static_cast<const common::ObCollectionArrayType &>(*collection_meta);
    common::ObArenaAllocator work_arena(
        "FlSqlArrW", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ret = write_ob_collection_array_row(
        src, row_idx, middle_type, work_arena, str_arena, datum);
    if (OB_FAIL(ret)) {
      LOG_WARN("[FlightSQL] convert scalar Arrow middle ARRAY failed", K(ret), K(row_idx));
    }
  }
  return ret;
}

static int convert_batch_nested_list(const arrow::Array &src,
                                     ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     int64_t nrows,
                                     ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  ObDatum *datums = nullptr;
  if (OB_UNLIKELY(src.type()->id() != arrow::Type::LIST
                  || nrows < 0 || nrows > src.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow nested ARRAY batch", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()), K(nrows),
             "array_length", src.length());
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve nested ARRAY metadata failed", K(ret));
  } else if (!is_recursive_array_type(collection_meta)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] nested Arrow LIST does not target nested ARRAY", K(ret),
             "collection_type", collection_meta->type_id_);
  } else if (OB_ISNULL(datums = init_uniform(expr, ctx, nrows))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const arrow::ListArray &source = static_cast<const arrow::ListArray &>(src);
    const common::ObCollectionArrayType &target_type =
        static_cast<const common::ObCollectionArrayType &>(*collection_meta);
    common::ObArenaAllocator work_arena(
        "FlSqlNestArr", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < nrows; ++row_idx) {
      if (source.IsNull(row_idx)) {
        datums[row_idx].set_null();
      } else {
        ret = write_nested_ob_array_row(
            source, row_idx, target_type, work_arena, str_arena, datums[row_idx]);
        // The datum was serialized into str_arena above.  Reuse just one
        // native-object workspace instead of retaining every row's tree.
        work_arena.reuse();
        if (OB_FAIL(ret)) {
          LOG_WARN("[FlightSQL] convert Arrow nested ARRAY row failed", K(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}

static int convert_datum_nested_list(const arrow::Array &src,
                                     const int64_t row_idx,
                                     const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &datum,
                                     ObIAllocator &str_arena)
{
  int ret = check_scalar_row_idx(src, row_idx);
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(src.type()->id() != arrow::Type::LIST)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] nested ARRAY requires Arrow LIST", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()));
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve scalar nested ARRAY metadata failed", K(ret));
  } else if (!is_recursive_array_type(collection_meta)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] scalar nested Arrow LIST does not target nested ARRAY", K(ret),
             "collection_type", collection_meta->type_id_);
  } else {
    const arrow::ListArray &source = static_cast<const arrow::ListArray &>(src);
    if (source.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const common::ObCollectionArrayType &target_type =
          static_cast<const common::ObCollectionArrayType &>(*collection_meta);
      common::ObArenaAllocator work_arena(
          "FlSqlNestArr", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ret = write_nested_ob_array_row(
          source, row_idx, target_type, work_arena, str_arena, datum);
      if (OB_FAIL(ret)) {
        LOG_WARN("[FlightSQL] convert scalar Arrow nested ARRAY failed", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

static int get_map_child_element_type(
    const common::ObCollectionTypeBase *map_child_type,
    const common::ObCollectionTypeBase *&element_type)
{
  int ret = OB_SUCCESS;
  element_type = nullptr;
  if (OB_ISNULL(map_child_type)
      || map_child_type->type_id_ != common::OB_ARRAY_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("native MAP child must be an ARRAY", K(ret), KP(map_child_type));
  } else {
    const common::ObCollectionArrayType *array_type =
        static_cast<const common::ObCollectionArrayType *>(map_child_type);
    if (OB_ISNULL(array_type->element_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("native MAP child ARRAY has no element metadata", K(ret));
    } else {
      element_type = array_type->element_type_;
    }
  }
  return ret;
}

static int write_ob_middle_map_elements(
    const arrow::Array &keys,
    const arrow::Array &values,
    const int64_t start,
    const int64_t count,
    const int64_t row_idx,
    const bool allow_null_keys,
    const common::ObCollectionMapType &middle_type,
    ObIAllocator &work_arena,
    ObIAllocator &str_arena,
    ObDatum &datum)
{
  int ret = OB_SUCCESS;
  using namespace common;
  const ObCollectionTypeBase *key_element_type = nullptr;
  const ObCollectionTypeBase *value_element_type = nullptr;
  ObIArrayType *native_map = nullptr;
  ObMapType *map = nullptr;
  ObIArrayType *canonical_map = nullptr;
  if (OB_UNLIKELY(start < 0 || count < 0 || count > UINT32_MAX
                  || start > keys.length() - count
                  || start > values.length() - count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow MAP child range", K(ret), K(start), K(count),
             "key_count", keys.length(), "value_count", values.length());
  } else if (OB_FAIL(get_map_child_element_type(middle_type.key_type_, key_element_type))) {
    LOG_WARN("failed to resolve native middle MAP key metadata", K(ret));
  } else if (OB_FAIL(get_map_child_element_type(middle_type.value_type_, value_element_type))) {
    LOG_WARN("failed to resolve native middle MAP value metadata", K(ret));
  } else if (!allow_null_keys) {
    // Arrow MapType declares keys non-null.  Reject an invalid record batch
    // before it reaches the native map, which otherwise canonicalizes a null
    // key like any other duplicate key.
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      if (keys.IsNull(start + idx)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Arrow MAP key must not be null", K(ret), K(row_idx), K(idx));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObArrayTypeObjFactory::construct(
          work_arena, middle_type, native_map, false))) {
    LOG_WARN("construct native middle MAP failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret) && OB_ISNULL(map = dynamic_cast<ObMapType *>(native_map))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native middle MAP object has an unexpected type", K(ret), KP(native_map));
  } else if (OB_SUCC(ret)
             && (OB_ISNULL(map->get_key_array()) || OB_ISNULL(map->get_value_array()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("native middle MAP child arrays are null", K(ret),
             KP(map->get_key_array()), KP(map->get_value_array()));
  } else if (OB_SUCC(ret) && OB_FAIL(append_arrow_collection_elements_to_array(
                 keys, start, count, *key_element_type, *map->get_key_array(), work_arena))) {
    LOG_WARN("append Arrow middle MAP keys failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret) && OB_FAIL(append_arrow_collection_elements_to_array(
                 values, start, count, *value_element_type, *map->get_value_array(), work_arena))) {
    LOG_WARN("append Arrow middle MAP values failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret) && OB_FAIL(map->init())) {
    LOG_WARN("finalize native middle MAP children failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret) && OB_FAIL(map->distinct(work_arena, canonical_map))) {
    LOG_WARN("canonicalize native middle MAP failed", K(ret), K(row_idx));
  } else if (OB_SUCC(ret) && OB_ISNULL(canonical_map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("canonical native middle MAP is null", K(ret), K(row_idx));
  } else if (OB_SUCC(ret)) {
    const int32_t raw_len = canonical_map->get_raw_binary_len();
    ObString dst;
    if (OB_UNLIKELY(raw_len <= 0 || raw_len > common::MAX_ARRAY_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("middle MAP serialized size is invalid", K(ret), K(raw_len), K(row_idx));
    } else if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
                   canonical_map, raw_len, str_arena, dst))) {
      LOG_WARN("serialize middle MAP into owned datum memory failed", K(ret),
               K(raw_len), K(row_idx));
    } else {
      datum.set_string(dst);
    }
  }
  return ret;
}

static int write_ob_middle_map_row(const arrow::MapArray &source,
                                   const int64_t row_idx,
                                   const common::ObCollectionMapType &middle_type,
                                   ObIAllocator &work_arena,
                                   ObIAllocator &str_arena,
                                   ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const std::shared_ptr<arrow::Array> keys = source.keys();
  const std::shared_ptr<arrow::Array> values = source.items();
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= source.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow MAP row index", K(ret), K(row_idx),
             "row_count", source.length());
  } else if (source.IsNull(row_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null Arrow MAP must be handled by its parent", K(ret), K(row_idx));
  } else if (!keys || !values) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Arrow MAP key/value child is null", K(ret), KP(keys.get()), KP(values.get()));
  } else {
    ret = write_ob_middle_map_elements(
        *keys, *values, source.value_offset(row_idx), source.value_length(row_idx), row_idx,
        false, middle_type, work_arena, str_arena, datum);
  }
  return ret;
}

static int convert_batch_map(const arrow::Array &src,
                             ObExpr &expr,
                             ObEvalCtx &ctx,
                             int64_t nrows,
                             ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  ObDatum *datums = nullptr;
  if (OB_UNLIKELY(src.type()->id() != arrow::Type::MAP
                  || nrows < 0 || nrows > src.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow MAP batch", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()), K(nrows),
             "array_length", src.length());
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve MAP metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_MAP_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] Arrow MAP does not map to native middle MAP", K(ret),
             KP(collection_meta));
  } else if (OB_ISNULL(datums = init_uniform(expr, ctx, nrows))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const arrow::MapArray &source = static_cast<const arrow::MapArray &>(src);
    const common::ObCollectionMapType &middle_type =
        static_cast<const common::ObCollectionMapType &>(*collection_meta);
    common::ObArenaAllocator work_arena(
        "FlSqlMapW", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < nrows; ++row_idx) {
      if (source.IsNull(row_idx)) {
        datums[row_idx].set_null();
      } else {
        ret = write_ob_middle_map_row(
            source, row_idx, middle_type, work_arena, str_arena, datums[row_idx]);
        // Only the final compact payload belongs to str_arena.  Reuse the
        // native construction workspace per row to cap peak memory.
        work_arena.reuse();
        if (OB_FAIL(ret)) {
          LOG_WARN("[FlightSQL] convert Arrow MAP row failed", K(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}

static int convert_datum_map(const arrow::Array &src,
                             const int64_t row_idx,
                             const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &datum,
                             ObIAllocator &str_arena)
{
  int ret = check_scalar_row_idx(src, row_idx);
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(src.type()->id() != arrow::Type::MAP)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] MAP requires Arrow MAP", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()));
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve scalar MAP metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_MAP_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] scalar Arrow MAP does not map to native middle MAP", K(ret),
             KP(collection_meta));
  } else {
    const arrow::MapArray &source = static_cast<const arrow::MapArray &>(src);
    if (source.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const common::ObCollectionMapType &middle_type =
          static_cast<const common::ObCollectionMapType &>(*collection_meta);
      common::ObArenaAllocator work_arena(
          "FlSqlMapW", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ret = write_ob_middle_map_row(source, row_idx, middle_type,
                             work_arena, str_arena, datum);
      if (OB_FAIL(ret)) {
        LOG_WARN("[FlightSQL] convert scalar Arrow MAP failed", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

static int write_ob_struct_as_map_row(
    const arrow::ListArray &source,
    const int64_t row_idx,
    const common::ObCollectionMapType &middle_type,
    ObIAllocator &work_arena,
    ObIAllocator &str_arena,
    ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const std::shared_ptr<arrow::Array> entries_array = source.values();
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= source.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Arrow struct_as_map row index", K(ret), K(row_idx),
             "row_count", source.length());
  } else if (source.IsNull(row_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null Arrow struct_as_map list must be handled by its parent", K(ret), K(row_idx));
  } else if (!entries_array || entries_array->type()->id() != arrow::Type::STRUCT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("struct_as_map requires LIST<STRUCT>", K(ret), KP(entries_array.get()));
  } else {
    const arrow::StructArray &entries =
        static_cast<const arrow::StructArray &>(*entries_array);
    const int64_t start = source.value_offset(row_idx);
    const int64_t count = source.value_length(row_idx);
    const int64_t entry_field_count =
        static_cast<int64_t>(entries.fields().size());
    std::shared_ptr<arrow::Array> keys;
    std::shared_ptr<arrow::Array> values;
    if (entry_field_count != 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("struct_as_map entries must have exactly key and value children", K(ret),
               K(entry_field_count));
    } else {
      keys = entries.field(0);
      values = entries.field(1);
      if (!keys || !values) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("struct_as_map entry children are null", K(ret),
                 KP(keys.get()), KP(values.get()));
      } else if (OB_UNLIKELY(start < 0 || count < 0 || start > entries.length() - count)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid Arrow struct_as_map entry range", K(ret), K(start), K(count),
                 "entry_count", entries.length());
      } else {
        // A nullable key is the reason this opt-in physical encoding exists.
        // A null entry itself has no key/value pair semantics and is rejected.
        for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
          if (entries.IsNull(start + idx)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("struct_as_map entry must not be null", K(ret), K(row_idx), K(idx));
          }
        }
        if (OB_SUCC(ret)) {
          ret = write_ob_middle_map_elements(
              *keys, *values, start, count, row_idx, true, middle_type,
              work_arena, str_arena, datum);
        }
      }
    }
  }
  return ret;
}

static int convert_batch_struct_as_map(const arrow::Array &src,
                                       ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       int64_t nrows,
                                       ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  ObDatum *datums = nullptr;
  if (OB_UNLIKELY(src.type()->id() != arrow::Type::LIST
                  || nrows < 0 || nrows > src.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid Arrow struct_as_map batch", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()), K(nrows),
             "array_length", src.length());
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve struct_as_map metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_MAP_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] struct_as_map does not map to native middle MAP", K(ret),
             KP(collection_meta));
  } else if (OB_ISNULL(datums = init_uniform(expr, ctx, nrows))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const arrow::ListArray &source = static_cast<const arrow::ListArray &>(src);
    const common::ObCollectionMapType &middle_type =
        static_cast<const common::ObCollectionMapType &>(*collection_meta);
    common::ObArenaAllocator work_arena(
        "FlSqlMapEnt", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < nrows; ++row_idx) {
      if (source.IsNull(row_idx)) {
        datums[row_idx].set_null();
      } else {
        ret = write_ob_struct_as_map_row(
            source, row_idx, middle_type, work_arena, str_arena, datums[row_idx]);
        work_arena.reuse();
        if (OB_FAIL(ret)) {
          LOG_WARN("[FlightSQL] convert Arrow struct_as_map row failed", K(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}

static int convert_datum_struct_as_map(const arrow::Array &src,
                                       const int64_t row_idx,
                                       const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &datum,
                                       ObIAllocator &str_arena)
{
  int ret = check_scalar_row_idx(src, row_idx);
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(src.type()->id() != arrow::Type::LIST)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] struct_as_map requires Arrow LIST", K(ret),
             "arrow_type", static_cast<int>(src.type()->id()));
  } else if (OB_FAIL(resolve_expr_collection_meta(expr, ctx, collection_meta))) {
    LOG_WARN("[FlightSQL] resolve scalar struct_as_map metadata failed", K(ret));
  } else if (OB_ISNULL(collection_meta)
             || collection_meta->type_id_ != common::OB_MAP_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] scalar struct_as_map does not map to native middle MAP", K(ret),
             KP(collection_meta));
  } else {
    const arrow::ListArray &source = static_cast<const arrow::ListArray &>(src);
    if (source.IsNull(row_idx)) {
      datum.set_null();
    } else {
      const common::ObCollectionMapType &middle_type =
          static_cast<const common::ObCollectionMapType &>(*collection_meta);
      common::ObArenaAllocator work_arena(
          "FlSqlMapEnt", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ret = write_ob_struct_as_map_row(
          source, row_idx, middle_type, work_arena, str_arena, datum);
      if (OB_FAIL(ret)) {
        LOG_WARN("[FlightSQL] convert scalar Arrow struct_as_map failed", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

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

template <arrow::TimeUnit::type UNIT>
static void set_oracle_timestamp_middle_convert_fns(const bool has_timezone,
                                                     ObFlightSqlColumnDesc &desc)
{
  if (has_timezone) {
    desc.convert_batch = convert_batch_timestamp_to_oracle_middle<UNIT, true>;
    desc.convert_datum = convert_datum_timestamp_to_oracle_middle<UNIT, true>;
  } else {
    desc.convert_batch = convert_batch_timestamp_to_oracle_middle<UNIT, false>;
    desc.convert_datum = convert_datum_timestamp_to_oracle_middle<UNIT, false>;
  }
}

static int get_struct_as_map_metadata(const arrow::Field &arrow_field,
                                      bool &struct_as_map);
static int get_struct_as_map_entry_types(const arrow::Field &arrow_field,
                                         const arrow::DataType *&key_type,
                                         const arrow::DataType *&value_type);

static int build_collection_convert_fn(const arrow::Field &arrow_field,
                                       const common::ObCollectionTypeBase *collection_meta,
                                       ObFlightSqlColumnDesc &desc)
{
  using namespace common;
  int ret = OB_SUCCESS;
  const arrow::Type::type arrow_id = arrow_field.type()->id();
  bool struct_as_map = false;
  if (OB_ISNULL(collection_meta)
      || (collection_meta->type_id_ != OB_ARRAY_TYPE
          && collection_meta->type_id_ != OB_MAP_TYPE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] unsupported middle collection metadata", K(ret), KP(collection_meta));
  } else if (OB_FAIL(get_struct_as_map_metadata(arrow_field, struct_as_map))) {
    LOG_WARN("[FlightSQL] invalid struct_as_map metadata", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (struct_as_map) {
    const arrow::DataType *key_type = nullptr;
    const arrow::DataType *value_type = nullptr;
    if (collection_meta->type_id_ != OB_MAP_TYPE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] struct_as_map middle carrier must be MAP", K(ret),
               KP(collection_meta));
    } else if (OB_FAIL(get_struct_as_map_entry_types(arrow_field, key_type, value_type))) {
      LOG_WARN("[FlightSQL] invalid struct_as_map entry schema", K(ret),
               "field_name", arrow_field.name().c_str());
    } else {
      desc.convert_batch = convert_batch_struct_as_map;
      desc.convert_datum = convert_datum_struct_as_map;
    }
  } else if (arrow_id == arrow::Type::MAP) {
    if (collection_meta->type_id_ != OB_MAP_TYPE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] Arrow MAP middle carrier must be MAP", K(ret),
               "arrow_type_id", static_cast<int>(arrow_id));
    } else {
      desc.convert_batch = convert_batch_map;
      desc.convert_datum = convert_datum_map;
    }
  } else if (arrow_id != arrow::Type::FIXED_SIZE_LIST
             && arrow_id != arrow::Type::LIST
             && arrow_id != arrow::Type::LARGE_LIST) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] middle ARRAY requires Arrow LIST, LARGE_LIST, or FIXED_SIZE_LIST",
             K(ret), "arrow_type_id", static_cast<int>(arrow_id));
  } else if (collection_meta->type_id_ != OB_ARRAY_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow LIST/LARGE_LIST/FIXED_SIZE_LIST middle carrier must be ARRAY", K(ret),
             KP(collection_meta));
  } else {
    // The collection subschema is generated solely from the Arrow field.  Do
    // not select a VECTOR fast path or compare it to the destination type here;
    // normal SQL execution owns ARRAY-to-VECTOR and other target casts.
    desc.convert_batch = convert_batch_array;
    desc.convert_datum = convert_datum_array;
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

static int get_struct_as_map_metadata(const arrow::Field &arrow_field,
                                      bool &struct_as_map)
{
  int ret = OB_SUCCESS;
  struct_as_map = false;
  const std::shared_ptr<const arrow::KeyValueMetadata> metadata =
      arrow_field.metadata();
  if (!metadata) {
    // no metadata: this remains an ordinary LIST/STRUCT field
  } else {
    for (int i = 0; OB_SUCC(ret) && i < metadata->size(); ++i) {
      if (metadata->key(i) == "struct_as_map") {
        const auto &arrow_value = metadata->value(i);
        if (OB_UNLIKELY(arrow_value.size() > static_cast<size_t>(INT32_MAX))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[FlightSQL] struct_as_map metadata value is too long", K(ret),
                   "field_name", arrow_field.name().c_str(),
                   "metadata_value_length", arrow_value.size());
        } else {
          const ObString value(static_cast<int32_t>(arrow_value.size()),
                               arrow_value.c_str());
          if (value == ObString::make_string("true")) {
            struct_as_map = true;
          } else if (value == ObString::make_string("false")) {
            struct_as_map = false;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("[FlightSQL] struct_as_map metadata must be boolean", K(ret),
                     "field_name", arrow_field.name().c_str(), K(value));
          }
        }
      }
    }
  }
  return ret;
}

static int get_struct_as_map_entry_types(const arrow::Field &arrow_field,
                                         const arrow::DataType *&key_type,
                                         const arrow::DataType *&value_type)
{
  int ret = OB_SUCCESS;
  key_type = nullptr;
  value_type = nullptr;
  bool struct_as_map = false;
  if (OB_FAIL(get_struct_as_map_metadata(arrow_field, struct_as_map))) {
  } else if (!struct_as_map || arrow_field.type()->id() != arrow::Type::LIST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] struct_as_map requires Arrow LIST", K(ret),
             "field_name", arrow_field.name().c_str(),
             "arrow_type", static_cast<int>(arrow_field.type()->id()));
  } else {
    const arrow::ListType &list_type =
        static_cast<const arrow::ListType &>(*arrow_field.type());
    const std::shared_ptr<arrow::DataType> entries_type = list_type.value_type();
    if (!entries_type || entries_type->id() != arrow::Type::STRUCT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] struct_as_map requires list<struct<key,value>>", K(ret),
               "field_name", arrow_field.name().c_str(),
               KP(entries_type.get()));
    } else {
      const arrow::StructType &entries =
          static_cast<const arrow::StructType &>(*entries_type);
      if (entries.num_fields() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] struct_as_map entries must have key and value fields", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "entry_field_count", entries.num_fields());
      } else {
        const std::shared_ptr<arrow::Field> key_field = entries.field(0);
        const std::shared_ptr<arrow::Field> value_field = entries.field(1);
        if (!key_field || !value_field
            || key_field->name() != "key" || value_field->name() != "value") {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[FlightSQL] struct_as_map entries must be named key and value", K(ret),
                   "field_name", arrow_field.name().c_str());
        } else {
          key_type = key_field->type().get();
          value_type = value_field->type().get();
        }
      }
    }
  }
  return ret;
}

static bool is_legal_arrow_map_key_type(const arrow::Type::type arrow_type)
{
  return arrow_type == arrow::Type::BOOL
      || arrow_type == arrow::Type::INT8
      || arrow_type == arrow::Type::INT16
      || arrow_type == arrow::Type::INT32
      || arrow_type == arrow::Type::INT64
      || arrow_type == arrow::Type::UINT8
      || arrow_type == arrow::Type::UINT16
      || arrow_type == arrow::Type::UINT32
      || arrow_type == arrow::Type::UINT64
      || arrow_type == arrow::Type::FLOAT
      || arrow_type == arrow::Type::DOUBLE
      || arrow_type == arrow::Type::STRING
      || arrow_type == arrow::Type::LARGE_STRING
      || arrow_type == arrow::Type::BINARY
      || arrow_type == arrow::Type::LARGE_BINARY
      || arrow_type == arrow::Type::FIXED_SIZE_BINARY;
}

int ob_flight_sql_validate_arrow_decimal_schema(
    const arrow::DataType &arrow_type,
    const bool is_oracle_mode,
    int32_t &precision,
    int32_t &scale)
{
  int ret = OB_SUCCESS;
  precision = 0;
  scale = 0;
  int32_t physical_precision_limit = 0;
  switch (arrow_type.id()) {
    case arrow::Type::DECIMAL32:
      physical_precision_limit = 9;
      break;
    case arrow::Type::DECIMAL64:
      physical_precision_limit = 18;
      break;
    case arrow::Type::DECIMAL128:
      physical_precision_limit = 38;
      break;
    case arrow::Type::DECIMAL256:
      physical_precision_limit = 76;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  if (OB_SUCC(ret)) {
    const arrow::DecimalType &decimal_type =
        static_cast<const arrow::DecimalType &>(arrow_type);
    precision = decimal_type.precision();
    scale = decimal_type.scale();
    if (precision <= 0 || precision > physical_precision_limit) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!is_oracle_mode
               && (precision > 65 || scale < 0 || scale > precision || scale > 30)) {
      ret = OB_NOT_SUPPORTED;
    } else if (is_oracle_mode
               && (precision > 38 || scale < -84 || scale > 127)) {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] Arrow decimal schema has no supported middle carrier", K(ret),
             "arrow_type", static_cast<int>(arrow_type.id()),
             K(is_oracle_mode), K(precision), K(scale));
  }
  return ret;
}

static int get_middle_collection_type_string(const arrow::DataType &arrow_type,
                                             const bool is_oracle_mode,
                                             const bool allow_top_level_map,
                                             ObSqlString &type_string)
{
  int ret = OB_SUCCESS;
  const char *scalar_type = nullptr;
  ObSqlString child_type;
  type_string.reuse();
  switch (arrow_type.id()) {
    case arrow::Type::BOOL:
    case arrow::Type::INT8:
      scalar_type = "TINYINT";
      break;
    case arrow::Type::INT16:
      scalar_type = "SMALLINT";
      break;
    case arrow::Type::INT32:
      scalar_type = "INT";
      break;
    case arrow::Type::INT64:
      scalar_type = "BIGINT";
      break;
    case arrow::Type::UINT8:
      scalar_type = "TINYINT UNSIGNED";
      break;
    case arrow::Type::UINT16:
      scalar_type = "SMALLINT UNSIGNED";
      break;
    case arrow::Type::UINT32:
      scalar_type = "INT UNSIGNED";
      break;
    case arrow::Type::UINT64:
      scalar_type = "BIGINT UNSIGNED";
      break;
    case arrow::Type::FLOAT:
      scalar_type = "FLOAT";
      break;
    case arrow::Type::DOUBLE:
      scalar_type = "DOUBLE";
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
      // Native ARRAY's variable-width leaf codec is VARCHAR-shaped.  It
      // preserves the byte value here; top-level scalar fields use the LOB
      // middle carrier selected by the normal descriptor path.
      scalar_type = "VARCHAR";
      break;
    case arrow::Type::DECIMAL32:
    case arrow::Type::DECIMAL64:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
      int32_t precision = 0;
      int32_t scale = 0;
      if (OB_FAIL(ob_flight_sql_validate_arrow_decimal_schema(
              arrow_type, is_oracle_mode, precision, scale))) {
      } else {
        // Native collection metadata has no DecimalInt/NUMBER carrier that
        // preserves Arrow's declared p,s without a target-derived conversion.
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] decimal collection middle carrier is not supported", K(ret),
                 K(is_oracle_mode), K(precision), K(scale));
      }
      break;
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      const std::shared_ptr<arrow::DataType> value_type =
          arrow_type.id() == arrow::Type::LIST
              ? static_cast<const arrow::ListType &>(arrow_type).value_type()
              : static_cast<const arrow::LargeListType &>(arrow_type).value_type();
      if (!value_type) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(get_middle_collection_type_string(
                     *value_type, is_oracle_mode, false, child_type))) {
      } else if (OB_FAIL(type_string.append("ARRAY("))
                 || OB_FAIL(type_string.append(child_type.string()))
                 || OB_FAIL(type_string.append(")"))) {
        LOG_WARN("[FlightSQL] failed to build Arrow ARRAY middle type", K(ret));
      }
      break;
    }
    case arrow::Type::FIXED_SIZE_LIST: {
      const arrow::FixedSizeListType &list_type =
          static_cast<const arrow::FixedSizeListType &>(arrow_type);
      const std::shared_ptr<arrow::DataType> value_type = list_type.value_type();
      // The middle carrier is always ARRAY.  A destination VECTOR conversion
      // and its dimension check belong to normal SQL execution.
      if (!value_type) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(get_middle_collection_type_string(
                     *value_type, is_oracle_mode, false, child_type))) {
      } else if (OB_FAIL(type_string.append("ARRAY("))
                 || OB_FAIL(type_string.append(child_type.string()))
                 || OB_FAIL(type_string.append(")"))) {
        LOG_WARN("[FlightSQL] failed to build Arrow fixed-size-list middle type", K(ret));
      }
      break;
    }
    case arrow::Type::MAP: {
      const arrow::MapType &map_type =
          static_cast<const arrow::MapType &>(arrow_type);
      const std::shared_ptr<arrow::DataType> key_type = map_type.key_type();
      const std::shared_ptr<arrow::DataType> value_type = map_type.item_type();
      ObSqlString key_string;
      ObSqlString value_string;
      if (!allow_top_level_map) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] nested Arrow MAP has no native middle carrier", K(ret));
      } else if (!key_type || !value_type
                 || !is_legal_arrow_map_key_type(key_type->id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] Arrow MAP key has no legal native middle carrier", K(ret),
                 KP(key_type.get()), KP(value_type.get()));
      } else if (OB_FAIL(get_middle_collection_type_string(
                     *key_type, is_oracle_mode, false, key_string))) {
      } else if (OB_FAIL(get_middle_collection_type_string(
                     *value_type, is_oracle_mode, false, value_string))) {
      } else if (OB_FAIL(type_string.append("MAP("))
                 || OB_FAIL(type_string.append(key_string.string()))
                 || OB_FAIL(type_string.append(","))
                 || OB_FAIL(type_string.append(value_string.string()))
                 || OB_FAIL(type_string.append(")"))) {
        LOG_WARN("[FlightSQL] failed to build Arrow MAP middle type", K(ret));
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] Arrow type has no native collection middle carrier", K(ret),
               "arrow_type", static_cast<int>(arrow_type.id()));
      break;
  }
  if (OB_SUCC(ret) && nullptr != scalar_type
      && OB_FAIL(type_string.assign(scalar_type))) {
    LOG_WARN("[FlightSQL] failed to build Arrow scalar collection middle type", K(ret));
  }
  return ret;
}

int ob_flight_sql_get_middle_collection_type_string(
    const arrow::Field &arrow_field,
    const bool is_oracle_mode,
    ObSqlString &type_string)
{
  int ret = OB_SUCCESS;
  bool struct_as_map = false;
  if (OB_FAIL(get_struct_as_map_metadata(arrow_field, struct_as_map))) {
  } else if (struct_as_map) {
    const arrow::DataType *key_type = nullptr;
    const arrow::DataType *value_type = nullptr;
    ObSqlString key_string;
    ObSqlString value_string;
    if (OB_FAIL(get_struct_as_map_entry_types(arrow_field, key_type, value_type))) {
    } else if (OB_ISNULL(key_type) || OB_ISNULL(value_type)
               || !is_legal_arrow_map_key_type(key_type->id())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] struct_as_map key has no legal native middle carrier", K(ret),
               KP(key_type), KP(value_type));
    } else if (OB_FAIL(get_middle_collection_type_string(
                   *key_type, is_oracle_mode, false, key_string))) {
    } else if (OB_FAIL(get_middle_collection_type_string(
                   *value_type, is_oracle_mode, false, value_string))) {
    } else if (OB_FAIL(type_string.assign("MAP("))
               || OB_FAIL(type_string.append(key_string.string()))
               || OB_FAIL(type_string.append(","))
               || OB_FAIL(type_string.append(value_string.string()))
               || OB_FAIL(type_string.append(")"))) {
      LOG_WARN("[FlightSQL] failed to build struct_as_map middle type", K(ret));
    }
  } else if (arrow_field.type()->id() != arrow::Type::LIST
             && arrow_field.type()->id() != arrow::Type::LARGE_LIST
             && arrow_field.type()->id() != arrow::Type::FIXED_SIZE_LIST
             && arrow_field.type()->id() != arrow::Type::MAP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow field is not a collection", K(ret),
             "field_name", arrow_field.name().c_str(),
             "arrow_type", static_cast<int>(arrow_field.type()->id()));
  } else if (OB_FAIL(get_middle_collection_type_string(
                 *arrow_field.type(), is_oracle_mode, true, type_string))) {
    LOG_WARN("[FlightSQL] failed to derive Arrow collection middle type", K(ret),
             "field_name", arrow_field.name().c_str());
  }
  return ret;
}

int ob_flight_sql_get_prepared_collection_type_string(
    const arrow::Field &arrow_field,
    const bool is_oracle_mode,
    ObSqlString &type_string)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arrow_field.type())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (arrow_field.type()->id() == arrow::Type::FIXED_SIZE_LIST) {
    const arrow::FixedSizeListType &list_type =
        static_cast<const arrow::FixedSizeListType &>(*arrow_field.type());
    const std::shared_ptr<arrow::DataType> value_type = list_type.value_type();
    if (list_type.list_size() <= 0 || !value_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FlightSQL] invalid prepared fixed-size-list parameter", K(ret),
               "field_name", arrow_field.name().c_str(),
               "list_size", list_type.list_size());
    } else if (value_type->id() == arrow::Type::FLOAT) {
      if (OB_FAIL(type_string.append_fmt("VECTOR(%d)", list_type.list_size()))) {
        LOG_WARN("[FlightSQL] failed to build prepared FLOAT VECTOR type", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "list_size", list_type.list_size());
      }
    } else if (value_type->id() == arrow::Type::UINT8) {
      if (OB_FAIL(type_string.append_fmt(
              "VECTOR(%d, UTINYINT)", list_type.list_size()))) {
        LOG_WARN("[FlightSQL] failed to build prepared UTINYINT VECTOR type", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "list_size", list_type.list_size());
      }
    } else if (OB_FAIL(ob_flight_sql_get_middle_collection_type_string(
                   arrow_field, is_oracle_mode, type_string))) {
      // A fixed-size list with a non-VECTOR leaf remains a native ARRAY.  This
      // preserves the existing fixed-size ARRAY parameter behavior without
      // claiming a VECTOR representation OceanBase does not support.
      LOG_WARN("[FlightSQL] failed to derive prepared fixed-size ARRAY type", K(ret),
               "field_name", arrow_field.name().c_str());
    }
  } else if (OB_FAIL(ob_flight_sql_get_middle_collection_type_string(
                 arrow_field, is_oracle_mode, type_string))) {
    LOG_WARN("[FlightSQL] failed to derive prepared collection parameter type", K(ret),
             "field_name", arrow_field.name().c_str());
  }
  return ret;
}

static int resolve_collection_param_meta(
    const arrow::Field &arrow_field,
    const bool is_oracle_mode,
    sql::ObExecContext &exec_ctx,
    uint16_t &subschema_id,
    const common::ObSqlCollectionInfo *&collection_info,
    const common::ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  subschema_id = UINT_MAX16;
  collection_info = nullptr;
  collection_meta = nullptr;
  ObSqlString type_string;
  sql::ObSubSchemaValue sub_meta;
  if (OB_FAIL(ob_flight_sql_get_prepared_collection_type_string(
          arrow_field, is_oracle_mode, type_string))) {
    LOG_WARN("[FlightSQL] failed to derive prepared collection parameter type", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (OB_UNLIKELY(type_string.empty() || type_string.length() > INT32_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid prepared collection parameter type string", K(ret),
             "field_name", arrow_field.name().c_str(),
             "type_string_length", type_string.length());
  } else {
    const ObString type_str = type_string.string();
    if (OB_FAIL(exec_ctx.get_subschema_id_by_type_string(type_str, subschema_id))) {
      LOG_WARN("[FlightSQL] failed to register prepared collection parameter subschema", K(ret),
               K(type_str));
    } else if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(subschema_id, sub_meta))) {
      LOG_WARN("[FlightSQL] failed to resolve prepared collection parameter subschema", K(ret),
               K(subschema_id));
    } else if (sub_meta.type_ != sql::OB_SUBSCHEMA_COLLECTION_TYPE
               || OB_ISNULL(sub_meta.value_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid prepared collection parameter subschema", K(ret),
               K(subschema_id), K(sub_meta.type_), KP(sub_meta.value_));
    } else {
      collection_info =
          reinterpret_cast<const common::ObSqlCollectionInfo *>(sub_meta.value_);
      if (OB_ISNULL(collection_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] prepared collection parameter metadata is null", K(ret),
                 K(subschema_id));
      } else {
        collection_meta = collection_info->collection_meta_;
      }
    }
  }
  return ret;
}

template <typename CppType, typename ArrowArray>
static int write_prepared_vector_row(
    const arrow::FixedSizeListArray &array,
    const int64_t row_idx,
    const common::ObCollectionArrayType &vector_type,
    const common::ObObjType expected_element_type,
    ObIAllocator &allocator,
    ObDatum &datum)
{
  int ret = OB_SUCCESS;
  const int32_t dim = array.list_type()->list_size();
  const int64_t value_offset = array.value_offset(row_idx);
  const std::shared_ptr<arrow::Array> values = array.values();
  const common::ObCollectionBasicType *element_type = nullptr;
  if (OB_ISNULL(vector_type.element_type_)
      || vector_type.element_type_->type_id_ != common::OB_BASIC_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] prepared VECTOR has invalid element metadata", K(ret));
  } else if (FALSE_IT(element_type =
                 static_cast<const common::ObCollectionBasicType *>(
                     vector_type.element_type_))) {
  } else if (vector_type.dim_cnt_ != static_cast<uint32_t>(dim)
             || element_type->basic_meta_.get_obj_type() != expected_element_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow fixed-size list does not match prepared VECTOR", K(ret),
             K(dim), "vector_dim", vector_type.dim_cnt_,
             "vector_element_type", element_type->basic_meta_.get_obj_type(),
             K(expected_element_type));
  } else if (!values || value_offset < 0
             || value_offset + dim > values->length()
             || dim > INT32_MAX / static_cast<int32_t>(sizeof(CppType))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid prepared VECTOR Arrow storage", K(ret),
             K(value_offset), K(dim), "values_length", values ? values->length() : -1);
  } else {
    const ArrowArray &typed_values = static_cast<const ArrowArray &>(*values);
    for (int32_t i = 0; OB_SUCC(ret) && i < dim; ++i) {
      if (typed_values.IsNull(value_offset + i)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] VECTOR parameter does not support null elements", K(ret),
                 K(row_idx), "element_idx", i);
      }
    }
    if (OB_SUCC(ret)) {
      const int32_t raw_length = dim * static_cast<int32_t>(sizeof(CppType));
      const CppType *raw_values = typed_values.raw_values() + value_offset;
      ObString serialized;
      if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(
              nullptr, raw_length, allocator, serialized,
              reinterpret_cast<const char *>(raw_values)))) {
        LOG_WARN("[FlightSQL] failed to serialize prepared VECTOR parameter", K(ret),
                 K(row_idx), K(dim), K(raw_length));
      } else {
        datum.set_string(serialized);
      }
    }
  }
  return ret;
}

int ob_flight_sql_convert_collection_param(
    const arrow::Field &arrow_field,
    const arrow::Array &array,
    const int64_t row_idx,
    const bool is_oracle_mode,
    sql::ObExecContext &exec_ctx,
    ObIAllocator &allocator,
    common::ObObjParam &param,
    sql::ObArrowStreamColDesc &out_desc)
{
  int ret = OB_SUCCESS;
  bool struct_as_map = false;
  uint16_t subschema_id = UINT_MAX16;
  const common::ObSqlCollectionInfo *collection_info = nullptr;
  const common::ObCollectionTypeBase *collection_meta = nullptr;
  const arrow::Type::type arrow_type = array.type_id();
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= array.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid prepared collection row index", K(ret), K(row_idx),
             "array_length", array.length());
  } else if (OB_ISNULL(arrow_field.type()) || !array.type()
             || !arrow_field.type()->Equals(*array.type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepared collection field and array types differ", K(ret),
             "field_name", arrow_field.name().c_str(),
             "field_type", arrow_field.type() ? arrow_field.type()->ToString().c_str() : "null",
             "array_type", array.type() ? array.type()->ToString().c_str() : "null");
  } else if (OB_FAIL(get_struct_as_map_metadata(arrow_field, struct_as_map))) {
    LOG_WARN("[FlightSQL] invalid prepared struct_as_map metadata", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (!struct_as_map && arrow_type != arrow::Type::LIST
             && arrow_type != arrow::Type::LARGE_LIST
             && arrow_type != arrow::Type::FIXED_SIZE_LIST
             && arrow_type != arrow::Type::MAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] unsupported prepared Arrow collection type", K(ret),
             "field_name", arrow_field.name().c_str(),
             "arrow_type", static_cast<int>(arrow_type));
  } else if (OB_FAIL(resolve_collection_param_meta(
                 arrow_field, is_oracle_mode, exec_ctx, subschema_id,
                 collection_info, collection_meta))) {
    LOG_WARN("[FlightSQL] failed to resolve prepared collection parameter metadata", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (OB_FAIL(ob_flight_sql_get_output_col_desc(
                 arrow_field, is_oracle_mode, subschema_id, out_desc))) {
    LOG_WARN("[FlightSQL] failed to derive prepared collection parameter descriptor", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (OB_ISNULL(collection_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] prepared collection parameter metadata is null", K(ret),
             "field_name", arrow_field.name().c_str(), K(subschema_id));
  } else if (array.IsNull(row_idx)) {
    // A null Arrow collection is SQL NULL, not a collection value with an
    // empty payload.  Keep the runtime parameter untyped just like native
    // prepared-statement NULL binding.  Giving a NULL ObObjParam collection
    // metadata makes first execution plan generation treat it as a concrete
    // collection even though there is no serialized value to materialize.
    param.set_null();
    param.set_param_meta();
  } else {
    ObDatum datum;
    common::ObArenaAllocator work_arena(
        "FlSqlPSColl", common::OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (collection_meta->type_id_ == common::OB_VECTOR_TYPE) {
      if (arrow_type != arrow::Type::FIXED_SIZE_LIST) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] prepared VECTOR parameter requires Arrow FIXED_SIZE_LIST", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "arrow_type", static_cast<int>(arrow_type));
      } else {
        const arrow::FixedSizeListArray &fixed_array =
            static_cast<const arrow::FixedSizeListArray &>(array);
        const std::shared_ptr<arrow::DataType> value_type =
            fixed_array.list_type()->value_type();
        const common::ObCollectionArrayType &vector_type =
            static_cast<const common::ObCollectionArrayType &>(*collection_meta);
        if (!value_type) {
          ret = OB_INVALID_ARGUMENT;
        } else if (value_type->id() == arrow::Type::FLOAT) {
          ret = write_prepared_vector_row<float, arrow::FloatArray>(
              fixed_array, row_idx, vector_type, common::ObFloatType,
              allocator, datum);
        } else if (value_type->id() == arrow::Type::UINT8) {
          ret = write_prepared_vector_row<uint8_t, arrow::UInt8Array>(
              fixed_array, row_idx, vector_type, common::ObUTinyIntType,
              allocator, datum);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("[FlightSQL] unsupported prepared VECTOR Arrow element type", K(ret),
                   "field_name", arrow_field.name().c_str(),
                   "element_type", value_type->ToString().c_str());
        }
      }
    } else if (collection_meta->type_id_ == common::OB_ARRAY_TYPE) {
      if (arrow_type != arrow::Type::LIST
          && arrow_type != arrow::Type::LARGE_LIST
          && arrow_type != arrow::Type::FIXED_SIZE_LIST) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] prepared ARRAY parameter requires Arrow LIST, LARGE_LIST, or FIXED_SIZE_LIST", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "arrow_type", static_cast<int>(arrow_type));
      } else {
        const common::ObCollectionArrayType &parameter_type =
            static_cast<const common::ObCollectionArrayType &>(*collection_meta);
        ret = write_ob_collection_array_row(
            array, row_idx, parameter_type, work_arena, allocator, datum);
      }
    } else if (collection_meta->type_id_ == common::OB_MAP_TYPE) {
      const common::ObCollectionMapType &parameter_type =
          static_cast<const common::ObCollectionMapType &>(*collection_meta);
      if (struct_as_map) {
        if (arrow_type != arrow::Type::LIST) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[FlightSQL] prepared struct_as_map requires Arrow LIST", K(ret),
                   "field_name", arrow_field.name().c_str(),
                   "arrow_type", static_cast<int>(arrow_type));
        } else {
          ret = write_ob_struct_as_map_row(
              static_cast<const arrow::ListArray &>(array), row_idx, parameter_type,
              work_arena, allocator, datum);
        }
      } else if (arrow_type != arrow::Type::MAP) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] prepared MAP parameter requires Arrow MAP", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "arrow_type", static_cast<int>(arrow_type));
      } else {
        ret = write_ob_middle_map_row(
            static_cast<const arrow::MapArray &>(array), row_idx, parameter_type,
            work_arena, allocator, datum);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] unknown prepared collection parameter type", K(ret),
               "field_name", arrow_field.name().c_str(), KP(collection_meta));
    }

    if (OB_SUCC(ret)) {
      const ObString serialized = datum.get_string();
      if (OB_UNLIKELY(serialized.length() < 0 || serialized.length() > INT32_MAX)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[FlightSQL] prepared collection payload is too large", K(ret),
                 "field_name", arrow_field.name().c_str(),
                 "payload_length", serialized.length());
      } else {
        param.set_sql_collection(
            serialized.ptr(), static_cast<int32_t>(serialized.length()), subschema_id);
        param.set_udt_id(static_cast<uint64_t>(collection_info->get_signature()));
        param.set_param_meta();
      }
    }
  }
  return ret;
}

int ob_flight_sql_get_output_col_desc(
    const arrow::Field &arrow_field,
    const bool is_oracle_mode,
    const uint16_t collection_subschema_id,
    sql::ObArrowStreamColDesc &out_desc)
{
  using namespace common;
  int ret = OB_SUCCESS;
  const int compatibility_mode = is_oracle_mode ? ORACLE_MODE : MYSQL_MODE;
  const arrow::Type::type arrow_id = arrow_field.type()->id();
  const std::shared_ptr<const arrow::KeyValueMetadata> metadata = arrow_field.metadata();
  if (metadata && metadata->Contains("ARROW:extension:name")) {
    // IPC exposes an unregistered extension as its storage type with metadata.
    // Reject it before treating that storage type as a supported middle type.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] Arrow extension ingest type is not supported", K(ret),
             "field_name", arrow_field.name().c_str());
    return ret;
  }

  // This descriptor is the Arrow-plus-compatibility-mode middle type.  It is
  // intentionally independent of the table column selected by the INSERT.
  switch (arrow_id) {
    case arrow::Type::BOOL:
    case arrow::Type::INT8:
      set_output_desc(out_desc, ObTinyIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObTinyIntType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::INT16:
      set_output_desc(out_desc, ObSmallIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObSmallIntType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::INT32:
      set_output_desc(out_desc, ObInt32Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObInt32Type],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::INT64:
      set_output_desc(out_desc, ObIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObIntType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT8:
      set_output_desc(out_desc, ObUTinyIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObUTinyIntType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT16:
      set_output_desc(out_desc, ObUSmallIntType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObUSmallIntType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT32:
      set_output_desc(out_desc, ObUInt32Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObUInt32Type],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::UINT64:
      set_output_desc(out_desc, ObUInt64Type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObUInt64Type],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::FLOAT:
      set_output_desc(out_desc, ObFloatType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObFloatType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::DOUBLE:
      set_output_desc(out_desc, ObDoubleType,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObDoubleType],
                      CS_TYPE_INVALID);
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      const ObObjType output_type = is_oracle_mode ? ObLobType : ObLongTextType;
      set_output_desc(out_desc, output_type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][output_type],
                      CS_TYPE_UTF8MB4_BIN);
      break;
    }
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY: {
      const ObObjType output_type = is_oracle_mode ? ObLobType : ObLongTextType;
      set_output_desc(out_desc, output_type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][output_type],
                      CS_TYPE_BINARY);
      break;
    }
    case arrow::Type::DECIMAL32:
    case arrow::Type::DECIMAL64:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
      int32_t precision = 0;
      int32_t scale = 0;
      if (OB_FAIL(ob_flight_sql_validate_arrow_decimal_schema(
              *arrow_field.type(), is_oracle_mode, precision, scale))) {
      } else {
        set_output_desc(out_desc,
                        is_oracle_mode ? ObNumberType : ObDecimalIntType,
                        ObAccuracy(static_cast<ObPrecision>(precision),
                                   static_cast<ObScale>(scale)),
                        CS_TYPE_INVALID);
      }
      break;
    }
    case arrow::Type::DATE32: {
      const ObObjType output_type = is_oracle_mode ? ObDateType : ObMySQLDateType;
      set_output_desc(out_desc, output_type,
                      ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][output_type],
                      CS_TYPE_INVALID);
      break;
    }
    case arrow::Type::DATE64: {
      const ObObjType output_type = is_oracle_mode ? ObTimestampNanoType : ObTimestampType;
      ObAccuracy accuracy =
          ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][output_type];
      accuracy.set_scale(3);
      set_output_desc(out_desc, output_type, accuracy, CS_TYPE_INVALID);
      break;
    }
    case arrow::Type::TIMESTAMP: {
      const arrow::TimestampType &timestamp_type =
          static_cast<const arrow::TimestampType &>(*arrow_field.type());
      if (timestamp_type.unit() == arrow::TimeUnit::NANO) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "Flight SQL ingest does not support Arrow timestamp[ns]; "
                       "cast it explicitly to timestamp[us], timestamp[ms], or timestamp[s]");
        LOG_WARN("[FlightSQL] timestamp[ns] requires an explicit client cast", K(ret),
                 "field_name", arrow_field.name().c_str());
      } else if (timestamp_type.unit() != arrow::TimeUnit::SECOND
                 && timestamp_type.unit() != arrow::TimeUnit::MILLI
                 && timestamp_type.unit() != arrow::TimeUnit::MICRO) {
        ret = OB_NOT_SUPPORTED;
      } else {
        const bool has_timezone = !timestamp_type.timezone().empty();
        const ObObjType output_type = is_oracle_mode
            ? (has_timezone ? ObTimestampTZType : ObTimestampNanoType)
            : (has_timezone ? ObTimestampType : ObMySQLDateTimeType);
        ObAccuracy accuracy =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][output_type];
        accuracy.set_scale(MAX_SCALE_FOR_TEMPORAL);
        set_output_desc(out_desc, output_type, accuracy, CS_TYPE_INVALID);
      }
      break;
    }
    case arrow::Type::TIME32: {
      const arrow::Time32Type &time_type =
          static_cast<const arrow::Time32Type &>(*arrow_field.type());
      if (time_type.unit() != arrow::TimeUnit::SECOND
          && time_type.unit() != arrow::TimeUnit::MILLI) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow TIME32 unit", K(ret),
                 "unit", static_cast<int>(time_type.unit()));
      } else {
        ObAccuracy accuracy =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObTimeType];
        accuracy.set_scale(MAX_SCALE_FOR_TEMPORAL);
        set_output_desc(out_desc, ObTimeType, accuracy, CS_TYPE_INVALID);
      }
      break;
    }
    case arrow::Type::TIME64: {
      const arrow::Time64Type &time_type =
          static_cast<const arrow::Time64Type &>(*arrow_field.type());
      if (time_type.unit() == arrow::TimeUnit::NANO) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "Flight SQL ingest does not support Arrow time64[ns]; "
                       "cast it explicitly to time64[us]");
        LOG_WARN("[FlightSQL] time64[ns] requires an explicit client cast", K(ret),
                 "field_name", arrow_field.name().c_str());
      } else if (time_type.unit() != arrow::TimeUnit::MICRO) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow TIME64 unit", K(ret),
                 "unit", static_cast<int>(time_type.unit()));
      } else {
        ObAccuracy accuracy =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObTimeType];
        accuracy.set_scale(MAX_SCALE_FOR_TEMPORAL);
        set_output_desc(out_desc, ObTimeType, accuracy, CS_TYPE_INVALID);
      }
      break;
    }
    case arrow::Type::DURATION: {
      const arrow::DurationType &duration_type =
          static_cast<const arrow::DurationType &>(*arrow_field.type());
      if (duration_type.unit() == arrow::TimeUnit::NANO) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "Flight SQL ingest does not support Arrow duration[ns]; "
                       "cast it explicitly to duration[us], duration[ms], or duration[s]");
        LOG_WARN("[FlightSQL] duration[ns] requires an explicit client cast", K(ret),
                 "field_name", arrow_field.name().c_str());
      } else if (duration_type.unit() != arrow::TimeUnit::SECOND
                 && duration_type.unit() != arrow::TimeUnit::MILLI
                 && duration_type.unit() != arrow::TimeUnit::MICRO) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow duration unit", K(ret),
                 "unit", static_cast<int>(duration_type.unit()));
      } else {
      ObAccuracy accuracy =
          ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObTimeType];
      accuracy.set_scale(MAX_SCALE_FOR_TEMPORAL);
      set_output_desc(out_desc, ObTimeType, accuracy, CS_TYPE_INVALID);
      }
      break;
    }
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::MAP:
      if (collection_subschema_id == UINT_MAX16) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] Arrow collection has no middle subschema", K(ret),
                 "field_name", arrow_field.name().c_str());
      } else {
        set_output_desc(out_desc, ObCollectionSQLType,
                        ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObCollectionSQLType],
                        CS_TYPE_INVALID, collection_subschema_id);
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[FlightSQL] unsupported Arrow type for output descriptor", K(ret),
               "arrow_type_id", static_cast<int>(arrow_id));
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
    case arrow::Type::DECIMAL32:
      desc.kind = ConversionKind::TRANSFORM;
      if (ob_type == common::ObDecimalIntType) {
        desc.decimal_int_bytes =
            wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
        desc.convert_batch = convert_batch_decimal32_to_decint;
        desc.convert_datum = convert_datum_decimal32_to_decint;
      } else if (ob_type == common::ObNumberType || ob_type == common::ObUNumberType) {
        desc.convert_batch = convert_batch_decimal32_to_number;
        desc.convert_datum = convert_datum_decimal32_to_number;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DECIMAL32 must map to DecimalInt or Number", K(ret), K(ob_type));
      }
      break;
    case arrow::Type::DECIMAL64:
      desc.kind = ConversionKind::TRANSFORM;
      if (ob_type == common::ObDecimalIntType) {
        desc.decimal_int_bytes =
            wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
        desc.convert_batch = convert_batch_decimal64_to_decint;
        desc.convert_datum = convert_datum_decimal64_to_decint;
      } else if (ob_type == common::ObNumberType || ob_type == common::ObUNumberType) {
        desc.convert_batch = convert_batch_decimal64_to_number;
        desc.convert_datum = convert_datum_decimal64_to_number;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DECIMAL64 must map to DecimalInt or Number", K(ret), K(ob_type));
      }
      break;
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
      if (ob_type != common::ObLongTextType && ob_type != common::ObLobType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] STRING converter output must be a text LOB", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_string;
        desc.convert_datum = convert_datum_string;
      }
      break;
    case arrow::Type::LARGE_STRING:
      if (ob_type != common::ObLongTextType && ob_type != common::ObLobType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] LARGE_STRING converter output must be a text LOB", K(ret),
                 K(ob_type));
      } else {
        desc.convert_batch = convert_batch_large_string;
        desc.convert_datum = convert_datum_large_string;
      }
      break;
    case arrow::Type::BINARY:
      if (ob_type != common::ObLongTextType && ob_type != common::ObLobType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] BINARY converter output must be a binary LOB", K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_binary;
        desc.convert_datum = convert_datum_binary;
      }
      break;
    case arrow::Type::LARGE_BINARY:
      if (ob_type != common::ObLongTextType && ob_type != common::ObLobType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] LARGE_BINARY converter output must be a binary LOB", K(ret),
                 K(ob_type));
      } else {
        desc.convert_batch = convert_batch_large_binary;
        desc.convert_datum = convert_datum_large_binary;
      }
      break;
    case arrow::Type::FIXED_SIZE_BINARY:
      if (ob_type != common::ObLongTextType && ob_type != common::ObLobType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] FIXED_SIZE_BINARY converter output must be a binary LOB",
                 K(ret), K(ob_type));
      } else {
        desc.convert_batch = convert_batch_fixed_size_binary;
        desc.convert_datum = convert_datum_fixed_size_binary;
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
    case arrow::Type::DATE64:
      desc.kind = ConversionKind::TRANSFORM;
      if (ob_type != common::ObTimestampType
          && ob_type != common::ObTimestampNanoType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DATE64 must map to a TIMESTAMP middle carrier", K(ret),
                 K(ob_type));
      } else {
        desc.convert_batch = convert_batch_date64;
        desc.convert_datum = convert_datum_date64;
      }
      break;

    // ---- timestamp ----
    case arrow::Type::TIMESTAMP: {
      desc.kind = ConversionKind::TRANSFORM;
      const arrow::TimestampType &ts_type =
          static_cast<const arrow::TimestampType &>(*arrow_field.type());
      const bool has_timezone = !ts_type.timezone().empty();
      const bool is_oracle_middle = ob_type == common::ObTimestampNanoType
                                    || ob_type == common::ObTimestampTZType;
      if (ts_type.unit() == arrow::TimeUnit::NANO) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] timestamp[ns] requires an explicit client cast", K(ret),
                 "field_name", arrow_field.name().c_str());
      } else if (ts_type.unit() != arrow::TimeUnit::SECOND
                 && ts_type.unit() != arrow::TimeUnit::MILLI
                 && ts_type.unit() != arrow::TimeUnit::MICRO) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow timestamp unit", K(ret),
                 "unit", static_cast<int>(ts_type.unit()));
      } else if (is_oracle_middle
                 && ((ob_type == common::ObTimestampTZType) != has_timezone)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] Oracle timestamp middle type disagrees with Arrow timezone",
                 K(ret), K(ob_type), K(has_timezone));
      } else if (is_oracle_middle) {
        switch (ts_type.unit()) {
          case arrow::TimeUnit::SECOND:
            set_oracle_timestamp_middle_convert_fns<arrow::TimeUnit::SECOND>(
                has_timezone, desc);
            break;
          case arrow::TimeUnit::MILLI:
            set_oracle_timestamp_middle_convert_fns<arrow::TimeUnit::MILLI>(
                has_timezone, desc);
            break;
          case arrow::TimeUnit::MICRO:
            set_oracle_timestamp_middle_convert_fns<arrow::TimeUnit::MICRO>(
                has_timezone, desc);
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            break;
        }
      } else if (common::ob_is_datetime_or_mysql_datetime_tc(ob_type)
                 || ob_type == common::ObDateTimeType) {
        const bool is_mysql_datetime = ob_type == common::ObMySQLDateTimeType;
        const bool is_mysql_timestamp = ob_type == common::ObTimestampType;
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
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("[FlightSQL] unsupported Arrow timestamp unit", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] TIMESTAMP has no supported middle type", K(ret), K(ob_type));
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
      } else if (t_type.unit() == arrow::TimeUnit::MILLI) {
        desc.convert_batch = convert_batch_time32_ms;
        desc.convert_datum = convert_datum_time32_ms;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow TIME32 unit", K(ret),
                 "unit", static_cast<int>(t_type.unit()));
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
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] time64[ns] requires an explicit client cast", K(ret),
                 "field_name", arrow_field.name().c_str());
      }
      break;
    }

    case arrow::Type::DURATION: {
      desc.kind = ConversionKind::TRANSFORM;
      const arrow::DurationType &duration_type =
          static_cast<const arrow::DurationType &>(*arrow_field.type());
      if (ob_type != common::ObTimeType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] DURATION must map to ObTimeType", K(ret), K(ob_type));
      } else {
        switch (duration_type.unit()) {
          case arrow::TimeUnit::SECOND:
            desc.convert_batch = convert_batch_duration<arrow::TimeUnit::SECOND>;
            desc.convert_datum = convert_datum_duration<arrow::TimeUnit::SECOND>;
            break;
          case arrow::TimeUnit::MILLI:
            desc.convert_batch = convert_batch_duration<arrow::TimeUnit::MILLI>;
            desc.convert_datum = convert_datum_duration<arrow::TimeUnit::MILLI>;
            break;
          case arrow::TimeUnit::MICRO:
            desc.convert_batch = convert_batch_duration<arrow::TimeUnit::MICRO>;
            desc.convert_datum = convert_datum_duration<arrow::TimeUnit::MICRO>;
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("[FlightSQL] duration[ns] requires an explicit client cast", K(ret),
                     "field_name", arrow_field.name().c_str());
            break;
        }
      }
      break;
    }

    // ---- collection (FIXED_SIZE_LIST / LIST / LARGE_LIST / MAP) ----
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::MAP: {
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
                                    const sql::ObArrowStreamColDesc &output_desc,
                                    const common::ObCollectionTypeBase *middle_collection_meta,
                                    ObFlightSqlColumnDesc &desc)
{
  return build_convert_fn(arrow_field,
                          output_desc.ob_type,
                          output_desc.accuracy.get_precision(),
                          middle_collection_meta,
                          desc);
}

static int resolve_output_collection_meta(const sql::ObExpr &output_expr,
                                          const sql::ObExecContext &exec_ctx,
                                          const common::ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  collection_meta = nullptr;
  const uint16_t subschema_id = output_expr.obj_meta_.get_subschema_id();
  sql::ObSubSchemaValue sub_meta;
  if (UINT_MAX16 == subschema_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] converter-output collection expression has no subschema", K(ret),
             K(output_expr.obj_meta_));
  } else if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(subschema_id, sub_meta))) {
    LOG_WARN("[FlightSQL] failed to resolve converter-output collection subschema", K(ret),
             K(subschema_id));
  } else if (sub_meta.type_ != sql::OB_SUBSCHEMA_COLLECTION_TYPE
             || OB_ISNULL(sub_meta.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid converter-output collection subschema", K(ret), K(subschema_id),
             K(sub_meta.type_), KP(sub_meta.value_));
  } else {
    const common::ObSqlCollectionInfo *collection_info =
        reinterpret_cast<const common::ObSqlCollectionInfo *>(sub_meta.value_);
    if (OB_ISNULL(collection_info->collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] converter-output collection metadata is null", K(ret),
               K(subschema_id));
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
      && OB_FAIL(resolve_output_collection_meta(output_expr, exec_ctx, collection_meta))) {
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
