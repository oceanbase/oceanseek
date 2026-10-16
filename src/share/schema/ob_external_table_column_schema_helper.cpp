/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_external_table_column_schema_helper.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

void ObExternalTableColumnSchemaHelper::set_column_accuracy(
    ObCompatibilityMode mode, ObObjType accuracy_type, ObColumnSchemaV2 &column_schema)
{
  if (ob_is_accuracy_length_valid_tc(accuracy_type)) {
    // logic is different: length set by external table settings, precision and scale -1
    if (mode == ORACLE_MODE) {
      column_schema.set_length_semantics(LS_CHAR);
      column_schema.set_data_scale(-1);
    } else {
      column_schema.set_data_precision(-1);
      column_schema.set_data_scale(-1);
    }
  } else {
    column_schema.set_data_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[mode][accuracy_type].get_precision());
    column_schema.set_data_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[mode][accuracy_type].get_scale());
    column_schema.set_data_length(-1);
  }
}

int ObExternalTableColumnSchemaHelper::setup_bool(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
  } else {
    column_schema.set_data_type(ObTinyIntType);
  }
  ObAccuracy accuracy;
  accuracy.set_precision(1);
  accuracy.set_scale(0);
  column_schema.set_accuracy(accuracy);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_tinyint(const bool &is_oracle_mode,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObTinyIntType, column_schema);
  } else {
    column_schema.set_data_type(ObTinyIntType);
    set_column_accuracy(MYSQL_MODE, ObTinyIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_utinyint(const bool &is_oracle_mode,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObUTinyIntType, column_schema);
  } else {
    column_schema.set_data_type(ObUTinyIntType);
    set_column_accuracy(MYSQL_MODE, ObUTinyIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_smallint(const bool &is_oracle_mode,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObSmallIntType, column_schema);
  } else {
    column_schema.set_data_type(ObSmallIntType);
    set_column_accuracy(MYSQL_MODE, ObSmallIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_usmallint(const bool &is_oracle_mode,
                                                       ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObUSmallIntType, column_schema);
  } else {
    column_schema.set_data_type(ObUSmallIntType);
    set_column_accuracy(MYSQL_MODE, ObUSmallIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_mediumint(const bool &is_oracle_mode,
                                                       ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObMediumIntType, column_schema);
  } else {
    column_schema.set_data_type(ObMediumIntType);
    set_column_accuracy(MYSQL_MODE, ObMediumIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_umediumint(const bool &is_oracle_mode,
                                                        ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObUMediumIntType, column_schema);
  } else {
    column_schema.set_data_type(ObUMediumIntType);
    set_column_accuracy(MYSQL_MODE, ObUMediumIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_int(const bool &is_oracle_mode,
                                                 ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObInt32Type, column_schema);
  } else {
    column_schema.set_data_type(ObInt32Type);
    set_column_accuracy(MYSQL_MODE, ObInt32Type, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_uint(const bool &is_oracle_mode,
                                                 ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObUInt32Type, column_schema);
  } else {
    column_schema.set_data_type(ObUInt32Type);
    set_column_accuracy(MYSQL_MODE, ObUInt32Type, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_bigint(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObIntType, column_schema);
  } else {
    column_schema.set_data_type(ObIntType);
    set_column_accuracy(MYSQL_MODE, ObIntType, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_ubigint(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    set_column_accuracy(ORACLE_MODE, ObUInt64Type, column_schema);
  } else {
    column_schema.set_data_type(ObUInt64Type);
    set_column_accuracy(MYSQL_MODE, ObUInt64Type, column_schema);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_float(const bool &is_oracle_mode,
                                                   ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObFloatType);
  column_schema.set_accuracy(
      ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObFloatType]);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_ufloat(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = setup_float(is_oracle_mode, column_schema);
  if (OB_SUCC(ret)) {
    column_schema.set_data_type(ObUFloatType);
    column_schema.set_accuracy(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObUFloatType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_double(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObDoubleType);
  column_schema.set_accuracy(
      ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObDoubleType]);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_udouble(const bool &is_oracle_mode,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = setup_double(is_oracle_mode, column_schema);
  if (OB_SUCC(ret)) {
    column_schema.set_data_type(ObUDoubleType);
    column_schema.set_accuracy(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObUDoubleType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_decimal(const bool &is_oracle_mode,
                                                     const int16_t &precision,
                                                     const int16_t &scale,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(precision > OB_MAX_DECIMAL_PRECISION)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
  } else if (OB_UNLIKELY(scale > OB_MAX_DECIMAL_SCALE)) {
    ret = OB_ERR_TOO_BIG_SCALE;
    LOG_WARN("scale of number overflow", K(ret), K(scale), K(precision));
  } else if (OB_UNLIKELY(precision < scale)) {
    ret = OB_ERR_M_BIGGER_THAN_D;
    LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
  } else {
    if (precision <= 0 && scale <= 0) {
      column_schema.set_accuracy(
          ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE]
                                           [ObDecimalIntType]);
    } else {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_data_precision(precision);
      column_schema.set_data_scale(scale);
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_udecimal(const bool &is_oracle_mode,
                                                      const int16_t &precision,
                                                      const int16_t &scale,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = setup_decimal(is_oracle_mode, precision, scale, column_schema);
  if (OB_SUCC(ret)) {
    // MySQL unsigned DECIMAL uses ObUNumberType rather than ObDecimalIntType because
    // ObDecimalIntType has no separate unsigned variant.
    column_schema.set_data_type(ObUNumberType);
    if (precision <= 0 && scale <= 0) {
      column_schema.set_accuracy(
          ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE]
                                           [ObUNumberType]);
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_varchar(const bool &is_oracle_mode,
                                                     const int64_t &length,
                                                     ObCharsetType cs_type,
                                                     ObCollationType collation,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObVarcharType);
  if (length <= 0) {
    column_schema.set_data_length(is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                                 : OB_MAX_MYSQL_VARCHAR_LENGTH);
  } else {
    column_schema.set_data_length(length);
  }
  column_schema.set_charset_type(cs_type);
  column_schema.set_collation_type(collation);
  // Later, we need to choose nvarchar2 based on the external column semantics.
  set_column_accuracy(is_oracle_mode ? ORACLE_MODE : MYSQL_MODE, ObVarcharType, column_schema);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_string(const bool &is_oracle_mode,
                                                    ObCharsetType cs_type,
                                                    ObCollationType collation,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(is_oracle_mode ? ObVarcharType : ObLongTextType);
  if (is_oracle_mode) {
  } else {
    column_schema.set_is_string_lob(); // 默认为ob的string类型
  }
  column_schema.set_data_length(is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                               : OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER);
  column_schema.set_collation_type(collation);
  column_schema.set_charset_type(cs_type);
  set_column_accuracy(is_oracle_mode ? ORACLE_MODE : MYSQL_MODE,
                      is_oracle_mode ? ObVarcharType : ObLongTextType,
                      column_schema);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_text(const bool &is_oracle_mode,
                                                  const ObObjType text_type,
                                                  ObCharsetType cs_type,
                                                  ObCollationType collation,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "text/blob type in oracle mode");
  } else {
    int64_t data_length = 0;
    switch (text_type) {
      case ObTinyTextType:
        data_length = OB_MAX_TINYTEXT_LENGTH - 1;
        break;
      case ObTextType:
        data_length = OB_MAX_TEXT_LENGTH - 1;
        break;
      case ObMediumTextType:
        data_length = OB_MAX_MEDIUMTEXT_LENGTH - 1;
        break;
      case ObLongTextType:
        data_length = OB_MAX_LONGTEXT_LENGTH;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected text type", K(ret), K(text_type));
        break;
    }
    if (OB_SUCC(ret)) {
      column_schema.set_data_type(text_type);
      column_schema.set_data_length(data_length);
      column_schema.set_charset_type(cs_type);
      column_schema.set_collation_type(collation);
      set_column_accuracy(MYSQL_MODE, text_type, column_schema);
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_char(const bool &is_oracle_mode,
                                                  const int64_t &length,
                                                  ObCharsetType cs_type,
                                                  ObCollationType collation,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObCharType);
  if (length <= 0) {  
    column_schema.set_data_length(is_oracle_mode ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                                 : OB_MAX_CHAR_LENGTH);
  } else {
    column_schema.set_data_length(length);
  }
  column_schema.set_charset_type(cs_type);
  column_schema.set_collation_type(collation);
  // must put here, stmt before will set data_precision and data_scale
  set_column_accuracy(is_oracle_mode ? ORACLE_MODE : MYSQL_MODE, ObCharType, column_schema);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_timestamp(const bool &is_oracle_mode,
                                                        ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObTimestampLTZType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTimestampLTZType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  } else {
    column_schema.set_data_type(ObTimestampType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObTimestampType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_datetime(const bool &is_oracle_mode,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObTimestampNanoType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTimestampNanoType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  } else {
    column_schema.set_data_type(ObDateTimeType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObDateTimeType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_date(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDateTimeType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObDateTimeType]);
    column_schema.set_data_scale(0);
  } else {
    column_schema.set_data_type(ObDateType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObDateType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_time(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "time type in oracle mode");
  } else {
    column_schema.set_data_type(ObTimeType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObTimeType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_year(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "year type in oracle mode");
  } else {
    column_schema.set_data_type(ObYearType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObYearType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_bit(const bool &is_oracle_mode,
                                                 const int64_t &precision,
                                                 ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "bit type in oracle mode");
  } else {
    column_schema.set_data_type(ObBitType);
    if (precision > 0) {
      column_schema.set_data_precision(static_cast<int16_t>(precision));
      column_schema.set_data_scale(0);
    } else {
      column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObBitType]);
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_json(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "json type in oracle mode");
  } else {
    column_schema.set_data_type(ObJsonType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObJsonType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_geometry(const bool &is_oracle_mode,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "geometry type in oracle mode");
  } else {
    column_schema.set_data_type(ObGeometryType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObGeometryType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_roaringbitmap(const bool &is_oracle_mode,
                                                           ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "roaringbitmap type in oracle mode");
  } else {
    column_schema.set_data_type(ObRoaringBitmapType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObRoaringBitmapType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_mysql_date(const bool &is_oracle_mode,
                                                        ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "mysql date type in oracle mode");
  } else {
    column_schema.set_data_type(ObMySQLDateType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObMySQLDateType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_mysql_datetime(const bool &is_oracle_mode,
                                                            ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "mysql datetime type in oracle mode");
  } else {
    column_schema.set_data_type(ObMySQLDateTimeType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObMySQLDateTimeType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_enum(const bool &is_oracle_mode,
                                                  const common::ObIArray<common::ObString> &type_info,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "enum type in oracle mode");
  } else if (OB_UNLIKELY(type_info.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("enum type without value list", K(ret));
  } else {
    column_schema.set_data_type(ObEnumType);
    column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column_schema.set_charset_type(CHARSET_UTF8MB4);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObEnumType]);
    if (OB_FAIL(column_schema.set_extended_type_info(type_info))) {
      LOG_WARN("failed to set enum type info", K(ret));
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_set(const bool &is_oracle_mode,
                                                 const common::ObIArray<common::ObString> &type_info,
                                                 ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set type in oracle mode");
  } else if (OB_UNLIKELY(type_info.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set type without value list", K(ret));
  } else {
    column_schema.set_data_type(ObSetType);
    column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column_schema.set_charset_type(CHARSET_UTF8MB4);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObSetType]);
    if (OB_FAIL(column_schema.set_extended_type_info(type_info))) {
      LOG_WARN("failed to set set type info", K(ret));
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_timestamp_ns(const bool &is_oracle_mode,
                                                          ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObTimestampNanoType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTimestampNanoType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "timestamp_ns not supported yet");
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_timestamp_tz_ns(const bool &is_oracle_mode,
                                                             ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObTimestampLTZType);
    ObAccuracy default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTimestampLTZType];
    default_accuracy.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + static_cast<int16_t>(default_accuracy.get_scale())));
    column_schema.set_accuracy(default_accuracy);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "timestamp_tz_ns not supported yet");
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase