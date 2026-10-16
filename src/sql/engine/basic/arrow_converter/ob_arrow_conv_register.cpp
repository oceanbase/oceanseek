/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_register.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_impl.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_conv_groups.h"

namespace oceanbase
{
namespace sql
{

ArrowConvFunc g_arrow_conv_table[common::ObMaxType][ARROW_TYPE_MAX] = {{nullptr}};

#define REG_CONV(OB_TYPE, ARROW_TYPE, GROUP)                                  \
  g_arrow_conv_table[common::OB_TYPE][arrow::Type::ARROW_TYPE] =             \
      &VecConvFormatWrapper<common::OB_TYPE, arrow::Type::ARROW_TYPE,        \
                    GROUP<common::OB_TYPE, arrow::Type::ARROW_TYPE>::template ConvFn>

#define REG_NULL_CONV(OB_TYPE, ARROW_TYPE)                                    \
  g_arrow_conv_table[common::OB_TYPE][arrow::Type::ARROW_TYPE] =             \
      &null_conv_fn<common::OB_TYPE, arrow::Type::ARROW_TYPE>

static void init_arrow_conv_table()
{
  REG_CONV(ObTinyIntType,   INT8,   NumericGroup);
  REG_CONV(ObSmallIntType,  INT16,  NumericGroup);
  REG_CONV(ObInt32Type,     INT32,  NumericGroup);
  REG_CONV(ObIntType,       INT64,  NumericGroup);
  REG_CONV(ObUTinyIntType,  UINT8,  NumericGroup);
  REG_CONV(ObUSmallIntType, UINT16, NumericGroup);
  REG_CONV(ObUInt32Type,    UINT32, NumericGroup);
  REG_CONV(ObUInt64Type,    UINT64, NumericGroup);
  REG_CONV(ObFloatType,     FLOAT,  NumericGroup);
  REG_CONV(ObUFloatType,    FLOAT,  NumericGroup);
  REG_CONV(ObDoubleType,    DOUBLE, NumericGroup);
  REG_CONV(ObUDoubleType,   DOUBLE, NumericGroup);
  REG_CONV(ObBitType,       INT64,  NumericGroup);

  REG_CONV(ObDateTimeType,      TIMESTAMP, DateGroup);
  REG_CONV(ObTimestampType,     TIMESTAMP, DateGroup);
  REG_CONV(ObDateType,          DATE32,    DateGroup);
  REG_CONV(ObTimeType,          TIME64,    DateGroup);
  REG_CONV(ObYearType,          INT16,     DateGroup);
  REG_CONV(ObMySQLDateType,     DATE32,    DateGroup);
  REG_CONV(ObMySQLDateTimeType, TIMESTAMP, DateGroup);

#define REG_TEXT_DUAL(OB_TYPE)                 \
  REG_CONV(OB_TYPE, STRING,       StrGroup);   \
  REG_CONV(OB_TYPE, LARGE_BINARY, StrGroup)

  REG_TEXT_DUAL(ObVarcharType);
  REG_TEXT_DUAL(ObCharType);
  REG_TEXT_DUAL(ObNVarchar2Type);
  REG_TEXT_DUAL(ObNCharType);
  REG_TEXT_DUAL(ObTinyTextType);
  REG_TEXT_DUAL(ObTextType);
  REG_TEXT_DUAL(ObMediumTextType);
  REG_TEXT_DUAL(ObLongTextType);
#undef REG_TEXT_DUAL

  REG_CONV(ObEnumType, STRING, StrGroup);
  REG_CONV(ObSetType,  STRING, StrGroup);
  REG_CONV(ObLobType,  LARGE_BINARY, StrGroup);

  REG_CONV(ObJsonType, STRING, JsonGroup);

#define REG_DECIMAL_DUAL(OB_TYPE)              \
  REG_CONV(OB_TYPE, DECIMAL128, DecimalGroup); \
  REG_CONV(OB_TYPE, DECIMAL256, DecimalGroup)

  REG_DECIMAL_DUAL(ObNumberType);
  REG_DECIMAL_DUAL(ObUNumberType);
  REG_DECIMAL_DUAL(ObDecimalIntType);
#undef REG_DECIMAL_DUAL

  REG_NULL_CONV(ObNullType, NA);
}

#undef REG_CONV
#undef REG_NULL_CONV

static struct ConvTableInitializer
{
  ConvTableInitializer() { init_arrow_conv_table(); }
} g_conv_table_init_;

}  // namespace sql
}  // namespace oceanbase
