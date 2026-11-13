/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * 编译期 traits，只描述 (ObObjType, ArrowType) 到 payload 类型和 Builder 类型的映射。
 * 具体转换行为放在各 Group functor。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_TRAITS_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_TRAITS_H_

#include <arrow/api.h>
#include <type_traits>
#include "lib/timezone/ob_time_convert.h"  // ObMySQLDate / ObMySQLDateTime

namespace oceanbase
{
namespace sql
{

// 主模板留空，未注册的类型对会在使用时尽早暴露出来。
template <common::ObObjType OT, arrow::Type::type AT>
struct ObArrowConvTraits
{
  // 故意留空：未覆盖的类型对会让 has_ob_cpp_type_v 为 false，且任何对
  // ObCppType/Builder 的引用都会在编译期失败，尽早暴露静态分发表注册错误。
};

// DIRECT traits 对应定宽直读路径。
#define OB_ARROW_TRAITS_DIRECT(OB_TYPE, ARROW_TYPE, OB_CPP_TYPE, ARROW_CPP_TYPE, BUILDER) \
  template <>                                                                    \
  struct ObArrowConvTraits<common::OB_TYPE, arrow::Type::ARROW_TYPE>             \
  {                                                                              \
    using ObCppType    = OB_CPP_TYPE;                                            \
    using ArrowCppType = ARROW_CPP_TYPE;                                         \
    using Builder      = arrow::BUILDER;                                         \
  }

// CUSTOM traits 只给出目标 Builder，具体转换由 Group 自己实现。
#define OB_ARROW_TRAITS_CUSTOM(OB_TYPE, ARROW_TYPE, BUILDER)        \
  template <>                                                       \
  struct ObArrowConvTraits<common::OB_TYPE, arrow::Type::ARROW_TYPE> \
  {                                                                 \
    using Builder = arrow::BUILDER;                                 \
  }

// Numeric：OB integer datums/vectors use 64-bit slots; cast to the advertised
// Arrow width while appending.
OB_ARROW_TRAITS_DIRECT(ObTinyIntType,   INT8,   int64_t,  int8_t,   Int8Builder);
OB_ARROW_TRAITS_DIRECT(ObSmallIntType,  INT16,  int64_t,  int16_t,  Int16Builder);
OB_ARROW_TRAITS_DIRECT(ObMediumIntType, INT32,  int64_t,  int32_t,  Int32Builder);
OB_ARROW_TRAITS_DIRECT(ObInt32Type,     INT32,  int64_t,  int32_t,  Int32Builder);
OB_ARROW_TRAITS_DIRECT(ObIntType,       INT64,  int64_t,  int64_t,  Int64Builder);
OB_ARROW_TRAITS_DIRECT(ObUTinyIntType,  UINT8,  uint64_t, uint8_t,  UInt8Builder);
OB_ARROW_TRAITS_DIRECT(ObUSmallIntType, UINT16, uint64_t, uint16_t, UInt16Builder);
OB_ARROW_TRAITS_DIRECT(ObUMediumIntType, INT32, uint64_t, int32_t,  Int32Builder);
OB_ARROW_TRAITS_DIRECT(ObUInt32Type,    UINT32, uint64_t, uint32_t, UInt32Builder);
OB_ARROW_TRAITS_DIRECT(ObUInt64Type,    UINT64, uint64_t, uint64_t, UInt64Builder);
OB_ARROW_TRAITS_DIRECT(ObFloatType,     FLOAT,  float,    float,    FloatBuilder);
OB_ARROW_TRAITS_DIRECT(ObUFloatType,    FLOAT,  float,    float,    FloatBuilder);
OB_ARROW_TRAITS_DIRECT(ObDoubleType,    DOUBLE, double,   double,   DoubleBuilder);
OB_ARROW_TRAITS_DIRECT(ObUDoubleType,   DOUBLE, double,   double,   DoubleBuilder);
OB_ARROW_TRAITS_DIRECT(ObBitType,       UINT64, uint64_t, uint64_t, UInt64Builder);

// Date：普通类型直传，Year 补 1900，MySQLDate* 经过 ObTimeConverter 转换。
OB_ARROW_TRAITS_DIRECT(ObDateTimeType,     TIMESTAMP, int64_t, int64_t, TimestampBuilder);
OB_ARROW_TRAITS_DIRECT(ObTimestampType,    TIMESTAMP, int64_t, int64_t, TimestampBuilder);
OB_ARROW_TRAITS_DIRECT(ObDateType,         DATE32,    int32_t, int32_t, Date32Builder);
OB_ARROW_TRAITS_DIRECT(ObTimeType,         DURATION,  int64_t, int64_t, DurationBuilder);
OB_ARROW_TRAITS_DIRECT(ObYearType,         INT16,     uint8_t, int16_t, Int16Builder);
OB_ARROW_TRAITS_DIRECT(ObMySQLDateType,    DATE32,    common::ObMySQLDate,     int32_t, Date32Builder);
OB_ARROW_TRAITS_DIRECT(ObMySQLDateTimeType, TIMESTAMP, common::ObMySQLDateTime, int64_t, TimestampBuilder);

// Decimal：由 Group 完成来源归一、rescale 和最终写出。
OB_ARROW_TRAITS_CUSTOM(ObNumberType,     DECIMAL128, Decimal128Builder);
OB_ARROW_TRAITS_CUSTOM(ObNumberType,     DECIMAL256, Decimal256Builder);
OB_ARROW_TRAITS_CUSTOM(ObUNumberType,    DECIMAL128, Decimal128Builder);
OB_ARROW_TRAITS_CUSTOM(ObUNumberType,    DECIMAL256, Decimal256Builder);
OB_ARROW_TRAITS_CUSTOM(ObDecimalIntType, DECIMAL128, Decimal128Builder);
OB_ARROW_TRAITS_CUSTOM(ObDecimalIntType, DECIMAL256, Decimal256Builder);

// Str：是否走 STRING/LARGE_BINARY 由 schema 路径决定。
OB_ARROW_TRAITS_CUSTOM(ObVarcharType,    STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObVarcharType,    LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObCharType,       STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObCharType,       LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObNVarchar2Type,  STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObNVarchar2Type,  LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObNCharType,      STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObNCharType,      LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObTinyTextType,   STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObTinyTextType,   LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObTextType,       STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObTextType,       LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObMediumTextType, STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObMediumTextType, LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObLongTextType,   STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObLongTextType,   LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObExtendBLobType, STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObExtendBLobType, LARGE_BINARY, LargeBinaryBuilder);
OB_ARROW_TRAITS_CUSTOM(ObEnumType,       STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObSetType,        STRING,       StringBuilder);
OB_ARROW_TRAITS_CUSTOM(ObLobType,        LARGE_BINARY, LargeBinaryBuilder);

// Json：恒 STRING。
OB_ARROW_TRAITS_CUSTOM(ObJsonType,       STRING,       StringBuilder);

// GEOMETRY: public EWKB bytes, including per-value SRID and Z flags.
OB_ARROW_TRAITS_CUSTOM(ObGeometryType,   BINARY,       BinaryBuilder);

// Null：恒 NA。
OB_ARROW_TRAITS_CUSTOM(ObNullType,       NA,           NullBuilder);

#undef OB_ARROW_TRAITS_DIRECT
#undef OB_ARROW_TRAITS_CUSTOM

// 判断 traits 是否带 ObCppType，供 FIXED 分支做编译期剪枝。
template <typename Tr, typename = void>
struct has_ob_cpp_type : std::false_type {};

template <typename Tr>
struct has_ob_cpp_type<Tr, std::void_t<typename Tr::ObCppType>> : std::true_type {};

template <typename Tr>
inline constexpr bool has_ob_cpp_type_v = has_ob_cpp_type<Tr>::value;

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_CONV_TRAITS_H_
