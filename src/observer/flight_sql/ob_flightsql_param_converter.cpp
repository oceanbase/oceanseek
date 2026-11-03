/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_param_converter.h"

#include <limits>

#include "common/object/ob_object.h"
#include "lib/number/ob_number_v2.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/utility/utility.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "observer/flight_sql/ob_flightsql_arrow_converter.h"
#include "observer/flight_sql/ob_flightsql_session.h"
#include "share/ob_errno.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"

// Arrow headers after OceanBase headers to avoid namespace collisions.
#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace oceanbase
{
namespace observer
{
namespace
{

bool flight_sql_is_arrow_collection_type(const arrow::Type::type type_id)
{
  return type_id == arrow::Type::LIST
      || type_id == arrow::Type::LARGE_LIST
      || type_id == arrow::Type::FIXED_SIZE_LIST
      || type_id == arrow::Type::MAP;
}

bool flight_sql_has_arrow_extension_metadata(const arrow::Field &arrow_field)
{
  bool has_extension = false;
  const std::shared_ptr<const arrow::KeyValueMetadata> metadata = arrow_field.metadata();
  if (metadata) {
    for (int i = 0; !has_extension && i < metadata->size(); ++i) {
      // An Arrow C Data importer can expose an unregistered extension as its
      // storage type while preserving this standard field metadata. Prepared
      // parameters must not silently bind that storage value as a supported
      // OceanBase scalar.
      has_extension = metadata->key(i) == "ARROW:extension:name";
    }
  }
  return has_extension;
}

int flight_sql_time_to_microseconds(const int64_t value,
                                    const arrow::TimeUnit::type unit,
                                    int64_t &microseconds)
{
  int ret = OB_SUCCESS;
  int64_t multiplier = 1;
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      multiplier = USECS_PER_SEC;
      break;
    case arrow::TimeUnit::MILLI:
      multiplier = MSECS_PER_SEC;
      break;
    case arrow::TimeUnit::MICRO:
      multiplier = 1;
      break;
    case arrow::TimeUnit::NANO:
      // Nanosecond input is rejected during schema validation.  Keep the row
      // binder strict so it cannot silently discard precision after prepare.
      ret = OB_NOT_SUPPORTED;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  if (OB_SUCC(ret)) {
    if ((value > 0 && value > std::numeric_limits<int64_t>::max() / multiplier)
        || (value < 0 && value < std::numeric_limits<int64_t>::min() / multiplier)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      microseconds = value * multiplier;
    }
  }
  return ret;
}

int flight_sql_timestamp_to_microseconds(const int64_t value,
                                         const arrow::TimeUnit::type unit,
                                         int64_t &microseconds)
{
  int ret = OB_SUCCESS;
  if (unit == arrow::TimeUnit::NANO) {
    // timestamp[ns] is rejected at schema validation. Keep the row converter
    // strict so an already-open stream can never silently truncate it.
    ret = OB_NOT_SUPPORTED;
  } else {
    ret = flight_sql_time_to_microseconds(value, unit, microseconds);
  }
  return ret;
}

int flight_sql_set_lob_param(const char *data,
                             const int64_t length,
                             const sql::ObArrowStreamColDesc &middle_desc,
                             common::ObIAllocator &allocator,
                             common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (middle_desc.ob_type != common::ObLongTextType
      && middle_desc.ob_type != common::ObLobType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] variable-width parameter has a non-LOB middle descriptor", K(ret),
             K(middle_desc.ob_type));
  } else if (OB_UNLIKELY(length < 0
                         || length > common::OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[FlightSQL] Arrow LOB parameter exceeds the temporary LOB payload limit", K(ret),
             K(length), "max_length", common::OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER);
  } else if (OB_UNLIKELY(length > 0 && OB_ISNULL(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] Arrow LOB parameter has a null data pointer", K(ret), K(length));
  } else {
    sql::ObTextStringObObjResult result(middle_desc.ob_type, nullptr, &param);
    if (OB_FAIL(result.init(length, &allocator))) {
      LOG_WARN("[FlightSQL] failed to initialize prepared temporary LOB", K(ret), K(length),
               "lob_type", middle_desc.ob_type);
    } else if (OB_FAIL(result.append(data == nullptr ? "" : data, length))) {
      LOG_WARN("[FlightSQL] failed to materialize prepared temporary LOB", K(ret), K(length),
               "lob_type", middle_desc.ob_type);
    } else {
      result.set_result();
      param.set_collation_type(middle_desc.cs_type);
      param.set_collation_level(common::CS_LEVEL_COERCIBLE);
    }
  }
  return ret;
}

int flight_sql_set_text_param(const char *data,
                              const int64_t length,
                              const sql::ObArrowStreamColDesc &middle_desc,
                              common::ObIAllocator &allocator,
                              common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (middle_desc.ob_type != common::ObVarcharType) {
    ret = flight_sql_set_lob_param(data, length, middle_desc, allocator, param);
  } else if (OB_UNLIKELY(length < 0)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[FlightSQL] prepared VARCHAR parameter has a negative byte length", K(ret), K(length));
  } else if (OB_UNLIKELY(length > common::OB_MAX_ORACLE_VARCHAR_LENGTH)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "Oracle prepared VARCHAR parameters exceeding 32767 bytes or CLOB parameters");
    LOG_WARN("[FlightSQL] prepared VARCHAR parameter exceeds its byte limit", K(ret), K(length));
  } else if (OB_UNLIKELY(length > 0 && OB_ISNULL(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepared VARCHAR parameter has a null data pointer", K(ret), K(length));
  } else {
    // convert_cell() deep-copies the completed parameter before this Arrow
    // batch can be released, matching the existing scalar ownership contract.
    param.set_varchar(common::ObString(static_cast<int32_t>(length), data == nullptr ? "" : data));
    param.set_collation_type(middle_desc.cs_type);
    param.set_collation_level(common::CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int flight_sql_set_decimal_param(const arrow::Array &array,
                                 const int64_t row_idx,
                                 const sql::ObArrowStreamColDesc &middle_desc,
                                 common::ObIAllocator &allocator,
                                 common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  int32_t precision = 0;
  int32_t scale = 0;
  common::ObDecimalIntBuilder decimal_builder;
  const bool is_oracle_mode = middle_desc.ob_type == common::ObNumberType;
  if (middle_desc.ob_type != common::ObNumberType
      && middle_desc.ob_type != common::ObDecimalIntType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] decimal middle descriptor has an unexpected type", K(ret),
             K(middle_desc.ob_type));
  } else if (arrow::Type::DECIMAL32 == array.type_id()) {
    const auto &decimal_array = static_cast<const arrow::Decimal32Array &>(array);
    const auto &decimal_type = static_cast<const arrow::Decimal32Type &>(*array.type());
    precision = decimal_type.precision();
    scale = decimal_type.scale();
    int32_t value = 0;
    MEMCPY(&value, decimal_array.GetValue(row_idx), sizeof(value));
    decimal_builder.from(value);
  } else if (arrow::Type::DECIMAL64 == array.type_id()) {
    const auto &decimal_array = static_cast<const arrow::Decimal64Array &>(array);
    const auto &decimal_type = static_cast<const arrow::Decimal64Type &>(*array.type());
    precision = decimal_type.precision();
    scale = decimal_type.scale();
    int64_t value = 0;
    MEMCPY(&value, decimal_array.GetValue(row_idx), sizeof(value));
    decimal_builder.from(value);
  } else if (arrow::Type::DECIMAL128 == array.type_id()) {
    const auto &decimal_array = static_cast<const arrow::Decimal128Array &>(array);
    const auto &decimal_type = static_cast<const arrow::Decimal128Type &>(*array.type());
    precision = decimal_type.precision();
    scale = decimal_type.scale();
    common::int128_t value = 0;
    MEMCPY(&value, decimal_array.GetValue(row_idx), sizeof(value));
    decimal_builder.from(value);
  } else if (arrow::Type::DECIMAL256 == array.type_id()) {
    const auto &decimal_array = static_cast<const arrow::Decimal256Array &>(array);
    const auto &decimal_type = static_cast<const arrow::Decimal256Type &>(*array.type());
    precision = decimal_type.precision();
    scale = decimal_type.scale();
    common::int256_t value;
    MEMCPY(value.items_, decimal_array.GetValue(row_idx), sizeof(value));
    decimal_builder.from(value);
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCC(ret) && OB_FAIL(ob_flight_sql_validate_arrow_decimal_schema(
                         *array.type(), is_oracle_mode, precision, scale))) {
    LOG_WARN("[FlightSQL] Arrow decimal parameter has no supported middle carrier", K(ret),
             K(is_oracle_mode), K(precision), K(scale));
  } else if (OB_SUCC(ret)
             && (precision != middle_desc.accuracy.get_precision()
                 || scale != middle_desc.accuracy.get_scale())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] Arrow decimal and middle descriptor disagree", K(ret),
             K(precision), K(scale), "middle_precision",
             middle_desc.accuracy.get_precision(), "middle_scale",
             middle_desc.accuracy.get_scale());
  } else if (OB_SUCC(ret) && is_oracle_mode) {
    char decimal_buf[common::number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
    int64_t decimal_len = 0;
    common::number::ObNumber number;
    if (OB_FAIL(common::wide::to_string(decimal_builder.get_decimal_int(),
                                        decimal_builder.get_int_bytes(), scale,
                                        decimal_buf, sizeof(decimal_buf), decimal_len))) {
      LOG_WARN("[FlightSQL] failed to format Arrow decimal parameter", K(ret),
               K(precision), K(scale));
    } else if (OB_FAIL(number.from(decimal_buf, decimal_len, allocator))) {
      LOG_WARN("[FlightSQL] failed to materialize Arrow NUMBER parameter", K(ret),
               K(precision), K(scale));
    } else {
      param.set_number(number);
      param.set_precision(static_cast<common::ObPrecision>(precision));
      param.set_scale(static_cast<common::ObScale>(scale));
    }
  } else if (OB_SUCC(ret)) {
    const int32_t middle_int_bytes =
        common::wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    common::ObDecimalIntBuilder min_value;
    common::ObDecimalIntBuilder max_value;
    common::ObDecimalIntBuilder middle_value;
    min_value.from(common::wide::ObDecimalIntConstValue::get_min_value(precision),
                   middle_int_bytes);
    max_value.from(common::wide::ObDecimalIntConstValue::get_max_value(precision),
                   middle_int_bytes);
    int cmp_min = 0;
    int cmp_max = 0;
    if (OB_FAIL(common::wide::compare(decimal_builder, min_value, cmp_min))) {
      LOG_WARN("[FlightSQL] failed to compare Arrow decimal lower bound", K(ret),
               K(precision), K(scale));
    } else if (OB_FAIL(common::wide::compare(decimal_builder, max_value, cmp_max))) {
      LOG_WARN("[FlightSQL] failed to compare Arrow decimal upper bound", K(ret),
               K(precision), K(scale));
    } else if (cmp_min < 0 || cmp_max > 0) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("[FlightSQL] Arrow decimal exceeds declared middle precision", K(ret),
               K(precision), K(scale));
    } else {
      middle_value.from(decimal_builder);
      if (middle_value.get_int_bytes() < middle_int_bytes) {
        middle_value.extend(middle_int_bytes);
      } else if (middle_value.get_int_bytes() > middle_int_bytes) {
        // The bounds check above makes this representation-only narrowing lossless.
        middle_value.truncate(middle_int_bytes);
      }
      param.set_decimal_int(middle_value.get_int_bytes(),
                            static_cast<common::ObScale>(scale),
                            const_cast<common::ObDecimalInt *>(
                                middle_value.get_decimal_int()));
      param.set_precision(static_cast<common::ObPrecision>(precision));
      param.set_scale(static_cast<common::ObScale>(scale));
    }
  }
  return ret;
}

int flight_sql_set_date_param(const int64_t days,
                              const sql::ObArrowStreamColDesc &middle_desc,
                              common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (middle_desc.ob_type == common::ObDateType) {
    if (OB_UNLIKELY(days < std::numeric_limits<int32_t>::min()
                    || days > std::numeric_limits<int32_t>::max())) {
      ret = OB_DATA_OUT_OF_RANGE;
    } else {
      param.set_date(static_cast<int32_t>(days));
    }
  } else if (middle_desc.ob_type == common::ObMySQLDateType) {
    const int64_t mysql_date_min = DATETIME_MIN_VAL / common::USECS_PER_DAY;
    if (OB_UNLIKELY(days < mysql_date_min || days > DATE_MAX_VAL)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("[FlightSQL] Arrow date parameter is outside MySQL DATE range",
               K(ret), K(days), K(mysql_date_min), K(DATE_MAX_VAL));
    } else {
      common::ObMySQLDate mysql_date;
      if (OB_FAIL(common::ObTimeConverter::date_to_mdate(
              static_cast<int32_t>(days), mysql_date))) {
        LOG_WARN("[FlightSQL] failed to materialize Arrow MySQL DATE parameter", K(ret),
                 K(days));
      } else {
        param.set_mysql_date(mysql_date);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] DATE32 middle descriptor has an unexpected type", K(ret),
             K(middle_desc.ob_type));
  }
  return ret;
}

int flight_sql_set_date64_param(const int64_t milliseconds,
                                const sql::ObArrowStreamColDesc &middle_desc,
                                common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  int64_t value_us = 0;
  if (OB_UNLIKELY(IS_MULTI_OVERFLOW64(milliseconds, 1000L))) {
    ret = OB_DATA_OUT_OF_RANGE;
  } else {
    value_us = milliseconds * 1000L;
    if (middle_desc.ob_type == common::ObTimestampType) {
      if (OB_UNLIKELY(value_us < MYSQL_TIMESTAMP_MIN_VAL
                      || value_us > MYSQL_TIMESTAMP_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        param.set_timestamp(value_us);
      }
    } else if (middle_desc.ob_type == common::ObTimestampNanoType) {
      if (OB_UNLIKELY(value_us < DATETIME_MIN_VAL || value_us > DATETIME_MAX_VAL)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        common::ObOTimestampData timestamp;
        timestamp.time_us_ = value_us;
        param.set_timestamp_nano(timestamp);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] DATE64 middle descriptor has an unexpected type", K(ret),
               K(middle_desc.ob_type));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] DATE64 parameter has no representable middle timestamp value",
             K(ret), K(milliseconds), K(value_us), K(middle_desc.ob_type));
  }
  return ret;
}

int flight_sql_set_time_param(const int64_t value,
                              const arrow::TimeUnit::type unit,
                              common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  int64_t value_us = 0;
  if (OB_FAIL(flight_sql_time_to_microseconds(value, unit, value_us))) {
    LOG_WARN("[FlightSQL] failed to convert Arrow time parameter to microseconds",
             K(ret), K(value), K(unit));
  } else if (OB_UNLIKELY(value_us < -TIME_MAX_VAL || value_us > TIME_MAX_VAL)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("[FlightSQL] Arrow time parameter exceeds MySQL TIME range",
             K(ret), K(value), K(value_us));
  } else {
    param.set_time(value_us);
  }
  return ret;
}

int flight_sql_set_timestamp_param(const int64_t value,
                                   const arrow::TimestampType &type,
                                   const sql::ObArrowStreamColDesc &middle_desc,
                                   common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  int64_t value_us = 0;
  const bool has_timezone = !type.timezone().empty();
  if (OB_FAIL(flight_sql_timestamp_to_microseconds(value, type.unit(), value_us))) {
    LOG_WARN("[FlightSQL] failed to convert Arrow timestamp parameter to microseconds",
             K(ret), K(value), "unit", type.unit());
  } else if (middle_desc.ob_type == common::ObTimestampNanoType) {
    if (OB_UNLIKELY(has_timezone || value_us < DATETIME_MIN_VAL
                    || value_us > DATETIME_MAX_VAL)) {
      ret = has_timezone ? OB_ERR_UNEXPECTED : OB_DATA_OUT_OF_RANGE;
    } else {
      common::ObOTimestampData timestamp;
      timestamp.time_us_ = value_us;
      param.set_timestamp_nano(timestamp);
    }
  } else if (middle_desc.ob_type == common::ObTimestampTZType) {
    if (OB_UNLIKELY(!has_timezone || value_us < DATETIME_MIN_VAL
                    || value_us > DATETIME_MAX_VAL)) {
      ret = !has_timezone ? OB_ERR_UNEXPECTED : OB_DATA_OUT_OF_RANGE;
    } else {
      common::ObOTimestampData timestamp;
      timestamp.time_us_ = value_us;
      // Arrow's timezone is field-level semantic metadata. The middle carrier
      // stores the instant with the agreed canonical UTC offset.
      timestamp.time_ctx_.set_offset_min(0);
      param.set_timestamp_tz(timestamp);
    }
  } else if (middle_desc.ob_type == common::ObMySQLDateTimeType) {
    if (OB_UNLIKELY(has_timezone || value_us < DATETIME_MIN_VAL
                    || value_us > DATETIME_MAX_VAL)) {
      ret = has_timezone ? OB_ERR_UNEXPECTED : OB_DATA_OUT_OF_RANGE;
    } else {
      common::ObMySQLDateTime mysql_datetime;
      if (OB_FAIL(common::ObTimeConverter::datetime_to_mdatetime(
              value_us, mysql_datetime))) {
        LOG_WARN("[FlightSQL] failed to materialize Arrow MySQL DATETIME parameter", K(ret),
                 K(value), K(value_us));
      } else {
        param.set_mysql_datetime(mysql_datetime);
      }
    }
  } else if (middle_desc.ob_type == common::ObTimestampType) {
    if (OB_UNLIKELY(!has_timezone || value_us < MYSQL_TIMESTAMP_MIN_VAL
                    || value_us > MYSQL_TIMESTAMP_MAX_VAL)) {
      ret = !has_timezone ? OB_ERR_UNEXPECTED : OB_DATA_OUT_OF_RANGE;
    } else {
      // A timezone-aware Arrow timestamp is already a UTC-normalized instant.
      param.set_timestamp(value_us);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] timestamp middle descriptor has an unexpected type", K(ret),
             K(middle_desc.ob_type));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] failed to materialize Arrow timestamp middle value", K(ret),
             K(value), K(value_us), K(has_timezone), K(middle_desc.ob_type));
  }
  return ret;
}

int flight_sql_set_null_param(const sql::ObArrowStreamColDesc &middle_desc,
                              common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  common::ObObjMeta meta;
  if (middle_desc.ob_type == common::ObCollectionSQLType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] collection null must use the collection materializer", K(ret));
  } else if (middle_desc.ob_type == common::ObDecimalIntType) {
    meta.set_decimal_int(middle_desc.accuracy.get_scale());
  } else {
    meta.set_type(middle_desc.ob_type);
    if (middle_desc.ob_type == common::ObVarcharType
        || middle_desc.ob_type == common::ObLongTextType
        || middle_desc.ob_type == common::ObLobType) {
      meta.set_collation_type(middle_desc.cs_type);
      meta.set_collation_level(common::CS_LEVEL_COERCIBLE);
    }
  }
  if (OB_SUCC(ret)) {
    param.set_null();
    param.set_accuracy(middle_desc.accuracy);
    param.set_param_meta(meta);
  }
  return ret;
}

int flight_sql_finish_scalar_param(const sql::ObArrowStreamColDesc &middle_desc,
                                   common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_null() && param.get_type() != middle_desc.ob_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] parameter value does not match Arrow middle descriptor", K(ret),
             "value_type", param.get_type(), "middle_type", middle_desc.ob_type);
  } else {
    param.set_accuracy(middle_desc.accuracy);
    if (!param.is_null()
        && (middle_desc.ob_type == common::ObNumberType
            || middle_desc.ob_type == common::ObDecimalIntType)) {
      param.set_precision(middle_desc.accuracy.get_precision());
      param.set_scale(middle_desc.accuracy.get_scale());
    }
    param.set_param_meta();
  }
  return ret;
}

int flight_sql_convert_scalar_param(const arrow::Array &array,
                                    const int64_t row_idx,
                                    const sql::ObArrowStreamColDesc &middle_desc,
                                    common::ObIAllocator &allocator,
                                    common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  if (array.IsNull(row_idx)) {
    ret = flight_sql_set_null_param(middle_desc, param);
  } else {
    switch (array.type_id()) {
      case arrow::Type::BOOL:
        param.set_tinyint(static_cast<int8_t>(
            static_cast<const arrow::BooleanArray &>(array).Value(row_idx) ? 1 : 0));
        break;
      case arrow::Type::INT8:
        param.set_tinyint(static_cast<const arrow::Int8Array &>(array).Value(row_idx));
        break;
      case arrow::Type::INT16:
        param.set_smallint(static_cast<const arrow::Int16Array &>(array).Value(row_idx));
        break;
      case arrow::Type::INT32:
        param.set_int32(static_cast<const arrow::Int32Array &>(array).Value(row_idx));
        break;
      case arrow::Type::INT64:
        param.set_int(static_cast<const arrow::Int64Array &>(array).Value(row_idx));
        break;
      case arrow::Type::UINT8:
        param.set_utinyint(static_cast<const arrow::UInt8Array &>(array).Value(row_idx));
        break;
      case arrow::Type::UINT16:
        param.set_usmallint(static_cast<const arrow::UInt16Array &>(array).Value(row_idx));
        break;
      case arrow::Type::UINT32:
        param.set_uint32(static_cast<const arrow::UInt32Array &>(array).Value(row_idx));
        break;
      case arrow::Type::UINT64:
        param.set_uint64(static_cast<const arrow::UInt64Array &>(array).Value(row_idx));
        break;
      case arrow::Type::FLOAT:
        param.set_float(static_cast<const arrow::FloatArray &>(array).Value(row_idx));
        break;
      case arrow::Type::DOUBLE:
        param.set_double(static_cast<const arrow::DoubleArray &>(array).Value(row_idx));
        break;
      case arrow::Type::STRING: {
        int32_t length = 0;
        const char *data = reinterpret_cast<const char *>(
            static_cast<const arrow::StringArray &>(array).GetValue(row_idx, &length));
        ret = flight_sql_set_text_param(data, length, middle_desc, allocator, param);
        break;
      }
      case arrow::Type::LARGE_STRING: {
        int64_t length = 0;
        const char *data = reinterpret_cast<const char *>(
            static_cast<const arrow::LargeStringArray &>(array).GetValue(row_idx, &length));
        ret = flight_sql_set_text_param(data, length, middle_desc, allocator, param);
        break;
      }
      case arrow::Type::BINARY: {
        int32_t length = 0;
        const char *data = reinterpret_cast<const char *>(
            static_cast<const arrow::BinaryArray &>(array).GetValue(row_idx, &length));
        ret = flight_sql_set_lob_param(data, length, middle_desc, allocator, param);
        break;
      }
      case arrow::Type::LARGE_BINARY: {
        int64_t length = 0;
        const char *data = reinterpret_cast<const char *>(
            static_cast<const arrow::LargeBinaryArray &>(array).GetValue(row_idx, &length));
        ret = flight_sql_set_lob_param(data, length, middle_desc, allocator, param);
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        const auto &binary_array =
            static_cast<const arrow::FixedSizeBinaryArray &>(array);
        const int32_t length = binary_array.byte_width();
        ret = flight_sql_set_lob_param(
            reinterpret_cast<const char *>(binary_array.GetValue(row_idx)), length,
            middle_desc, allocator, param);
        break;
      }
      case arrow::Type::DECIMAL32:
      case arrow::Type::DECIMAL64:
      case arrow::Type::DECIMAL128:
      case arrow::Type::DECIMAL256:
        ret = flight_sql_set_decimal_param(array, row_idx, middle_desc, allocator, param);
        break;
      case arrow::Type::DATE32:
        ret = flight_sql_set_date_param(
            static_cast<const arrow::Date32Array &>(array).Value(row_idx),
            middle_desc, param);
        break;
      case arrow::Type::DATE64:
        ret = flight_sql_set_date64_param(
            static_cast<const arrow::Date64Array &>(array).Value(row_idx),
            middle_desc, param);
        break;
      case arrow::Type::TIME32: {
        const auto &time_type = static_cast<const arrow::Time32Type &>(*array.type());
        ret = flight_sql_set_time_param(
            static_cast<const arrow::Time32Array &>(array).Value(row_idx),
            time_type.unit(), param);
        break;
      }
      case arrow::Type::TIME64: {
        const auto &time_type = static_cast<const arrow::Time64Type &>(*array.type());
        ret = flight_sql_set_time_param(
            static_cast<const arrow::Time64Array &>(array).Value(row_idx),
            time_type.unit(), param);
        break;
      }
      case arrow::Type::DURATION: {
        const auto &duration_type = static_cast<const arrow::DurationType &>(*array.type());
        ret = flight_sql_set_time_param(
            static_cast<const arrow::DurationArray &>(array).Value(row_idx),
            duration_type.unit(), param);
        break;
      }
      case arrow::Type::TIMESTAMP: {
        const auto &timestamp_type = static_cast<const arrow::TimestampType &>(*array.type());
        ret = flight_sql_set_timestamp_param(
            static_cast<const arrow::TimestampArray &>(array).Value(row_idx),
            timestamp_type, middle_desc, param);
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        break;
    }
  }
  if (OB_SUCC(ret)) {
    ret = flight_sql_finish_scalar_param(middle_desc, param);
  }
  return ret;
}

int flight_sql_convert_cell(const arrow::Field &arrow_field,
                            const arrow::Array &array,
                            const int64_t row_idx,
                            ObFlightSqlSession &session,
                            sql::ObExecContext &exec_ctx,
                            common::ObIAllocator &allocator,
                            common::ObObjParam &param)
{
  int ret = OB_SUCCESS;
  common::ObObjParam converted_param;
  if (OB_UNLIKELY(row_idx < 0 || row_idx >= array.length())
      || OB_ISNULL(arrow_field.type()) || !array.type()
      || !arrow_field.type()->Equals(*array.type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepared parameter field and array are inconsistent", K(ret),
             K(row_idx), "field_name", arrow_field.name().c_str(),
             "field_type", arrow_field.type() ? arrow_field.type()->ToString().c_str() : "null",
             "array_type", array.type() ? array.type()->ToString().c_str() : "null");
  } else if (flight_sql_has_arrow_extension_metadata(arrow_field)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] prepared Arrow extension parameter is not supported", K(ret),
             K(row_idx), "field_name", arrow_field.name().c_str());
  } else if (flight_sql_is_arrow_collection_type(array.type_id())
             && array.IsNull(row_idx)) {
    // SQL NULL has no collection payload or source subschema.  Registering a
    // subschema here would leave it unreferenced by the runtime ParamStore,
    // which build_subschema_ctx_by_param_store() rejects on a plan cache miss.
    converted_param.set_null();
    converted_param.set_param_meta();
  } else if (flight_sql_is_arrow_collection_type(array.type_id())) {
    sql::ObArrowStreamColDesc parameter_desc;
    if (OB_FAIL(ob_flight_sql_convert_collection_param(
            arrow_field, array, row_idx, session.is_oracle_mode, exec_ctx,
            allocator, converted_param, parameter_desc))) {
      LOG_WARN("[FlightSQL] failed to materialize prepared collection parameter", K(ret),
               K(row_idx), "field_name", arrow_field.name().c_str());
    }
  } else {
    sql::ObArrowStreamColDesc middle_desc;
    if (OB_FAIL(ob_flight_sql_get_output_col_desc(
            arrow_field, session.is_oracle_mode, UINT_MAX16, middle_desc))) {
      LOG_WARN("[FlightSQL] Arrow parameter has no supported middle descriptor", K(ret),
               K(row_idx), "field_name", arrow_field.name().c_str(),
               "arrow_type", array.type()->ToString().c_str());
    } else {
      if (session.is_oracle_mode
          && (array.type_id() == arrow::Type::STRING
              || array.type_id() == arrow::Type::LARGE_STRING)) {
        // Oracle SQL equality rejects CLOB operands. Bind prepared text,
        // including NULL, as VARCHAR and reject values beyond its byte limit.
        middle_desc.ob_type = common::ObVarcharType;
        middle_desc.accuracy = common::ObAccuracy::DDL_DEFAULT_ACCURACY2[
            ORACLE_MODE][common::ObVarcharType];
        middle_desc.accuracy.set_length(common::OB_MAX_ORACLE_VARCHAR_LENGTH);
        middle_desc.accuracy.set_length_semantics(common::LS_BYTE);
      }
      if (OB_FAIL(flight_sql_convert_scalar_param(
              array, row_idx, middle_desc, allocator, converted_param))) {
        LOG_WARN("[FlightSQL] failed to materialize prepared scalar middle value", K(ret),
                 K(row_idx), "field_name", arrow_field.name().c_str(),
                 "arrow_type", array.type()->ToString().c_str());
      }
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(common::deep_copy_objparam(allocator, converted_param, param))) {
    LOG_WARN("[FlightSQL] failed to own prepared parameter value", K(ret),
             K(row_idx), "field_name", arrow_field.name().c_str());
  }
  return ret;
}

int flight_sql_validate_field(const arrow::Field &arrow_field,
                              const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arrow_field.type())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (flight_sql_has_arrow_extension_metadata(arrow_field)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] prepared Arrow extension parameter schema is not supported", K(ret),
             "field_name", arrow_field.name().c_str());
  } else if (flight_sql_is_arrow_collection_type(arrow_field.type()->id())) {
    common::ObSqlString parameter_type;
    if (OB_FAIL(ob_flight_sql_get_prepared_collection_type_string(
            arrow_field, is_oracle_mode, parameter_type))) {
      LOG_WARN("[FlightSQL] prepared collection field has no supported parameter type", K(ret),
               "field_name", arrow_field.name().c_str());
    }
  } else {
    sql::ObArrowStreamColDesc middle_desc;
    if (OB_FAIL(ob_flight_sql_get_output_col_desc(
            arrow_field, is_oracle_mode, UINT_MAX16, middle_desc))) {
      LOG_WARN("[FlightSQL] prepared field has no supported middle descriptor", K(ret),
               "field_name", arrow_field.name().c_str(),
               "arrow_type", arrow_field.type()->ToString().c_str());
    }
  }
  return ret;
}

} // namespace

int ObFlightSqlParamConverter::validate_schema(const arrow::Schema &schema,
                                                const ObFlightSqlSession &session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session.ob_session)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < schema.num_fields(); ++i) {
      const std::shared_ptr<arrow::Field> field = schema.field(i);
      if (!field) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] prepared parameter schema has a null field", K(ret), K(i));
      } else if (OB_FAIL(flight_sql_validate_field(
                     *field, session.is_oracle_mode))) {
        LOG_WARN("[FlightSQL] prepared parameter schema is not supported", K(ret), K(i),
                 "field_name", field->name().c_str());
      }
    }
  }
  return ret;
}

int ObFlightSqlParamConverter::convert_row(const arrow::RecordBatch &batch,
                                           const int64_t row_idx,
                                           ObFlightSqlSession &session,
                                           sql::ObExecContext &exec_ctx,
                                           common::ObIAllocator &allocator,
                                           sql::ParamStore &params,
                                           int64_t &failed_column)
{
  int ret = OB_SUCCESS;
  failed_column = -1;
  const std::shared_ptr<arrow::Schema> schema = batch.schema();
  if (OB_ISNULL(session.ob_session) || !schema || row_idx < 0 || row_idx >= batch.num_rows()
      || schema->num_fields() != batch.num_columns()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(params.reserve(batch.num_columns()))) {
    LOG_WARN("[FlightSQL] failed to reserve prepared parameter store", K(ret),
             "parameter_count", batch.num_columns());
  } else {
    for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < batch.num_columns(); ++column_idx) {
      const std::shared_ptr<arrow::Array> array = batch.column(static_cast<int>(column_idx));
      const std::shared_ptr<arrow::Field> field = schema->field(static_cast<int>(column_idx));
      common::ObObjParam param;
      failed_column = column_idx;
      if (!array || !field) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FlightSQL] prepared parameter column or field is null", K(ret),
                 K(column_idx), KP(array.get()), KP(field.get()));
      } else if (flight_sql_is_arrow_collection_type(array->type_id())
                 && !array->IsNull(row_idx)
                 && OB_ISNULL(exec_ctx.get_physical_plan_ctx())
                 && OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
        // Native stmt_execute() normally creates this context itself.  Arrow
        // ARRAY/MAP binding needs it a little earlier to register the source
        // middle subschema; init_exec_context() will retain that context.
        LOG_WARN("[FlightSQL] failed to create prepared collection bind context", K(ret),
                 K(row_idx), K(column_idx));
      } else if (OB_FAIL(flight_sql_convert_cell(
                     *field, *array, row_idx, session, exec_ctx, allocator, param))) {
        LOG_WARN("[FlightSQL] failed to convert prepared parameter", K(ret),
                 K(row_idx), K(column_idx),
                 "arrow_type_id", static_cast<int>(array->type_id()));
      } else if (OB_FAIL(params.push_back(param))) {
        LOG_WARN("[FlightSQL] failed to append prepared parameter", K(ret),
                 K(row_idx), K(column_idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    failed_column = -1;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
