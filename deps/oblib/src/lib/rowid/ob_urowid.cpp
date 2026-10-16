/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "ob_urowid.h"
#include "lib/charset/ob_charset.h"
#include "common/ob_smart_call.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace common
{

ObURowIDData::ObPkValueIterator::ObPkValueIterator(const ObURowIDData &rowid)
  : rowid_(rowid),
    pos_(0),
    rowkey_count_(0),
    rowkey_idx_(0),
    generated_column_count_(-1),
    generated_column_idx_(0)
{
}

int ObURowIDData::ObPkValueIterator::init()
{
  int ret = OB_SUCCESS;
  const int64_t count_offset = rowid_.get_real_pk_count_offset();
  if (OB_UNLIKELY(PK_ROWID_VERSION != rowid_.get_version()
                  || count_offset >= rowid_.get_buf_len()
                  || !is_valid_pk_count(rowid_.rowid_content_[count_offset]))) {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid primary key rowid header",
               K(ret), K(count_offset), "rowid_len", rowid_.get_buf_len());
  } else {
    pos_ = count_offset + 1;
    rowkey_count_ = rowid_.rowid_content_[count_offset];
  }
  return ret;
}

int ObURowIDData::ObPkValueIterator::decode_value(ObObj &value)
{
  int ret = OB_SUCCESS;
  const int64_t value_offset = pos_;
  if (OB_UNLIKELY(pos_ >= rowid_.get_buf_len())) {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "rowid value is truncated", K(ret), K(pos_), "rowid_len", rowid_.get_buf_len());
  } else {
    const ObObjType obj_type = rowid_.get_pk_type(pos_);
    if (OB_UNLIKELY(ob_is_invalid_obj_type(obj_type))) {
      ret = OB_INVALID_ROWID;
      COMMON_LOG(WARN, "invalid obj type in rowid",
                 K(ret), K(obj_type), K(value_offset), "rowid_len", rowid_.get_buf_len());
    } else {
      const int tmp_ret = rowid_.get_pk_value(obj_type, pos_, value);
      if (OB_SUCCESS != tmp_ret) {
        // 对外统一返回非法ROWID，同时在日志中保留底层解码错误。
        ret = (OB_NOT_SUPPORTED == tmp_ret) ? OB_INVALID_ROWID : tmp_ret;
        COMMON_LOG(WARN, "failed to get rowid value",
                   K(ret), K(tmp_ret), K(obj_type), K(value_offset),
                   "decode_pos", pos_, "rowid_len", rowid_.get_buf_len());
      }
    }
  }
  return ret;
}

int ObURowIDData::ObPkValueIterator::get_next_value(ObObj &value)
{
  int ret = OB_SUCCESS;
  if (rowkey_idx_ < rowkey_count_) {
    if (OB_FAIL(decode_value(value))) {
      COMMON_LOG(WARN, "failed to decode rowkey value", K(ret), K(rowkey_idx_));
    } else {
      ++rowkey_idx_;
    }
  } else { // rowkey_idx_ >= rowkey_count_, 说明遍历到了generated_column_count_
    if (generated_column_count_ < 0) {
      int16_t generated_column_count = 0;
      const int tmp_ret = serialization::decode_i16(
          reinterpret_cast<const char *>(rowid_.rowid_content_),
          rowid_.get_buf_len(), pos_, &generated_column_count);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        ret = OB_INVALID_ROWID;
        COMMON_LOG(WARN, "failed to decode generated partition column count",
                   K(ret), K(tmp_ret), K(pos_), "rowid_len", rowid_.get_buf_len());
      } else {
        generated_column_count_ = generated_column_count;
        if (OB_UNLIKELY(!is_valid_generated_partition_column_count(generated_column_count_))) {
          ret = OB_INVALID_ROWID;
          COMMON_LOG(WARN, "invalid generated partition column count",
                     K(ret), K(generated_column_count_), "rowid_len", rowid_.get_buf_len());
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (generated_column_idx_ < generated_column_count_) {
        if (OB_FAIL(decode_value(value))) {
          COMMON_LOG(WARN, "failed to decode generated partition column value",
                     K(ret), K(generated_column_idx_));
        } else {
          ++generated_column_idx_;
        }
      } else if (OB_UNLIKELY(pos_ != rowid_.get_buf_len())) {
        ret = OB_INVALID_ROWID;
        COMMON_LOG(WARN, "unexpected trailing data in rowid",
                   K(ret), K(pos_), "rowid_len", rowid_.get_buf_len());
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

// ObURowID只有在内部包含的PK类型一致时比较才有意义
// rowid itself needs to support comparison between any types, such as the following scenario:
//   create table t2(c1 urowid(100));
//   select * from t2 order by c1;
// So take advantage of rowid reconstruction, let go of restrictions, and be compatible with
// oracle's rowid comparison behavior in all scenarios (no error is reported)
// compare rule:
// 1、Heap table and iot table
// 2、iot table (all converted to pk_objs), comparison dimensions: type, value, number
int ObURowIDData::compare(const ObURowIDData &other) const
{
  int ret = OB_SUCCESS;
  int compare_ret = 0;
  const uint8_t version = get_version();
  const uint8_t other_version = other.get_version();
  if (is_physical_rowid() && other.is_physical_rowid()) {
    uint64_t tuple[2] = {0};
    uint64_t other_tuple[2] = {0};
    if (OB_FAIL(parse_physical_rowid(tuple[0], tuple[1]))) {
      COMMON_LOG(ERROR, "failed to parse rowid", K(ret));
    } else if (OB_FAIL(other.parse_physical_rowid(other_tuple[0], other_tuple[1]))) {
      COMMON_LOG(ERROR, "failed to parse rowid", K(ret));
    }
    if (OB_FAIL(ret)) {
      right_to_die_or_duty_to_live();
    } else if (tuple[0] != other_tuple[0]) {
      compare_ret = tuple[0] < other_tuple[0] ? -1 : 1;
    } else if (tuple[1] != other_tuple[1]) {
      compare_ret = tuple[1] < other_tuple[1] ? -1 : 1;
    } else {
      compare_ret = 0;
    }
  } else if (version != other_version) {
    compare_ret = version < other_version ? -1 : 1;
  } else if (INVALID_ROWID_VERSION == version) {
    compare_ret = 0;
  } else if (PK_ROWID_VERSION == version) {
    uint8_t left_real_pk_count = 0;
    uint8_t right_real_pk_count = 0;
    ObPkValueIterator left_iter(*this);
    ObPkValueIterator right_iter(other);
    if (OB_FAIL(left_iter.init())) {
      COMMON_LOG(ERROR, "failed to init left rowid iterator", K(ret));
    } else if (OB_FAIL(right_iter.init())) {
      COMMON_LOG(ERROR, "failed to init right rowid iterator", K(ret));
    } else {
      left_real_pk_count = left_iter.get_rowkey_count();
      right_real_pk_count = right_iter.get_rowkey_count();
    }
    bool is_iter_end = false;
    while (OB_SUCC(ret) && 0 == compare_ret && !is_iter_end) {
      ObObj left_value;
      ObObj right_value;
      const int left_ret = left_iter.get_next_value(left_value);
      const int right_ret = right_iter.get_next_value(right_value);
      if (OB_UNLIKELY(OB_SUCCESS != left_ret && OB_ITER_END != left_ret)) {
        ret = left_ret;
        COMMON_LOG(ERROR, "failed to get left rowid value", K(ret));
      } else if (OB_UNLIKELY(OB_SUCCESS != right_ret && OB_ITER_END != right_ret)) {
        ret = right_ret;
        COMMON_LOG(ERROR, "failed to get right rowid value", K(ret));
      } else if (OB_ITER_END == left_ret && OB_ITER_END == right_ret) {
        is_iter_end = true;
      } else if (OB_ITER_END == left_ret) {
        compare_ret = -1;
      } else if (OB_ITER_END == right_ret) {
        compare_ret = 1;
      } else {
        const ObObjType left_type = left_value.get_type();
        const ObObjType right_type = right_value.get_type();
        // decimal int and number types are synonymous
        const bool all_decimals = (ObDecimalIntType == left_type && ObNumberType == right_type)
                                  || (ObNumberType == left_type && ObDecimalIntType == right_type);
        compare_ret = ((left_type != right_type) && !all_decimals)
            ? (left_type < right_type ? -1 : 1) : left_value.compare(right_value);
      }
    }
    if (OB_FAIL(ret)) {
      right_to_die_or_duty_to_live();
    } else if (0 == compare_ret && left_real_pk_count != right_real_pk_count) {
      // 主键值完全相同时，再用真实主键列数区分不同的主键/生成分区列布局。
      compare_ret = left_real_pk_count < right_real_pk_count ? -1 : 1;
    }
  } else {
    int64_t this_pos = get_pk_content_offset();
    int64_t that_pos = other.get_pk_content_offset();
    while (OB_SUCC(ret) && 0 == compare_ret
          && this_pos < get_buf_len() && that_pos < other.get_buf_len()) {
      OB_ASSERT(this_pos + 1 <= get_buf_len());
      OB_ASSERT(that_pos + 1 <= other.get_buf_len());

      ObObjType type1 = get_pk_type(this_pos);
      ObObjType type2 = other.get_pk_type(that_pos);
      // decimal int and number types are synonymous
      bool all_decimals = (type1 == ObDecimalIntType && type2 == ObNumberType)
                          || (type1 == ObNumberType && type2 == ObDecimalIntType);
      if (type1 != type2 && !all_decimals) {
        compare_ret = type1 < type2 ? -1 : 1;
      } else {
        ObObj pk_val1;
        ObObj pk_val2;
        if (OB_FAIL(get_pk_value(type1, this_pos, pk_val1))
            || OB_FAIL(other.get_pk_value(type2, that_pos, pk_val2))) {
          COMMON_LOG(ERROR, "failed to get pk value", K(ret));
          right_to_die_or_duty_to_live();
        } else {
          compare_ret = pk_val1.compare(pk_val2);
        }
      }
    }
    if (OB_SUCC(ret) && 0 == compare_ret && (this_pos != get_buf_len() || that_pos != other.get_buf_len())) {
      compare_ret = ((get_buf_len() - this_pos) < (other.get_buf_len() - that_pos)) ? -1 : 1;
    }
  }
  return compare_ret;
}

template<>
int ObURowIDData::inner_get_pk_value<ObNullType>(const uint8_t *rowid_buf,
                                                 const int64_t rowid_buf_len,
                                                 int64_t &pos, ObObj &pk_val)
{
  int ret = OB_SUCCESS;
  UNUSED(pos);
  UNUSED(rowid_buf);
  UNUSED(rowid_buf_len);
  pk_val.set_null();
  return ret;
}

template<>
int ObURowIDData::inner_get_pk_value<ObIntervalDSType>(const uint8_t *rowid_buf,
                                                       const int64_t rowid_buf_len,
                                                       int64_t &pos, ObObj &pk_val)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(pos + 12 <= rowid_buf_len)) {
    int64_t nsecond = *(reinterpret_cast<const int64_t *>(rowid_buf + pos));
    pos += 8;
    int32_t fractional_second = *(reinterpret_cast<const int32_t *>(rowid_buf + pos));
    pos += 4;
    pk_val.set_interval_ds(ObIntervalDSValue(nsecond, fractional_second));
  } else {
    ret = OB_INVALID_ROWID;
  }
  return ret;
}

template<>
int ObURowIDData::inner_get_pk_value<ObURowIDType>(const uint8_t *rowid_buf,
                                                   const int64_t rowid_buf_len,
                                                   int64_t &pos, ObObj &pk_val)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(pos + 4 <= rowid_buf_len);
  uint32_t rowid_len = *(reinterpret_cast<const uint32_t *>(rowid_buf + pos));
  if (OB_LIKELY(pos + 4 + rowid_len <= rowid_buf_len)) {
    pos += 4;
    const char *rowid_content = (const char *)rowid_buf + pos;
    pk_val.set_urowid(rowid_content, rowid_len);
    pos += rowid_len;
  } else {
    ret = OB_INVALID_ROWID;
  }
  return ret;
}

template <>
int ObURowIDData::inner_get_pk_value<ObDecimalIntType>(const uint8_t *rowid_buf,
                                                       const int64_t rowid_buf_len, int64_t &pos,
                                                       ObObj &pk_val)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(pos + sizeof(ObScale) + sizeof(uint32_t) <= rowid_buf_len)) {
    ObScale scale = *reinterpret_cast<const ObScale *>(rowid_buf + pos);
    pos += sizeof(ObScale);
    int32_t int_bytes = *reinterpret_cast<const int32_t *>(rowid_buf + pos);
    pos += sizeof(int32_t);
    if (OB_LIKELY(pos + int_bytes <= rowid_buf_len)) {
      pk_val.set_decimal_int(int_bytes, scale, (ObDecimalInt *)(rowid_buf + pos));
      pos += int_bytes;
    } else {
      ret = OB_INVALID_ROWID;
    }
  } else {
    ret = OB_INVALID_ROWID;
  }
  return ret;
}

#define DEF_GET_POD_PK_VALUE(obj_type, type, pod_type)                              \
  template <>                                                                       \
  int ObURowIDData::inner_get_pk_value<obj_type>(const uint8_t *rowid_buf,          \
                                                 const int64_t rowid_buf_len,       \
                                                 int64_t &pos,                      \
                                                 ObObj &pk_val) {                   \
    int ret = OB_SUCCESS;                                                           \
    if (OB_LIKELY(pos + sizeof(pod_type) <= rowid_buf_len)) {                       \
      pod_type pod_v = *(reinterpret_cast<const pod_type *>(rowid_buf + pos));      \
      pos += sizeof(pod_type);                                                      \
      pk_val.set_##type(pod_v);                                                     \
    } else {                                                                        \
      ret = OB_INVALID_ROWID;                                                       \
    }                                                                               \
    return ret;                                                                     \
  }

#define DEF_GET_NUMBER_PK_VALUE(num_type, type)                                \
  template <>                                                                  \
  int ObURowIDData::inner_get_pk_value<num_type>(const uint8_t *rowid_buf,     \
                                                 const int64_t rowid_buf_len,  \
                                                 int64_t & pos,                \
                                                 ObObj & pk_val) {             \
    int ret = OB_SUCCESS;                                                      \
    if (OB_LIKELY(sizeof(ObNumberDesc) + pos <= rowid_buf_len)) {              \
      ObNumberDesc num_desc =                                                  \
          *(reinterpret_cast<const ObNumberDesc *>(rowid_buf + pos));          \
      pos += sizeof(ObNumberDesc);                                             \
      int64_t digits_len = sizeof(uint32_t) * num_desc.len_;                   \
      if (OB_LIKELY(0 <= digits_len && digits_len <= rowid_buf_len             \
              && pos + digits_len <= rowid_buf_len)) {                         \
        const uint32_t *digits = reinterpret_cast<const uint32_t *>(rowid_buf + pos); \
        pos += digits_len;                                                     \
        pk_val.set_##type(num_desc, const_cast<uint32_t *>(digits));           \
        if (OB_FAIL(pk_val.get_number().sanity_check())) {                     \
          ret = OB_INVALID_ROWID;                                               \
          COMMON_LOG(WARN, "invalid rowid", K(ret));                           \
        }                                                                      \
      } else {                                                                 \
        ret = OB_INVALID_ROWID;                                                \
      }                                                                        \
    } else {                                                                   \
      ret = OB_INVALID_ROWID;                                                  \
    }                                                                          \
    return ret;                                                                \
  }

#define DEF_GET_STRING_PK_VALUE(obj_type)                                      \
  template <>                                                                  \
  int ObURowIDData::inner_get_pk_value<obj_type>(const uint8_t *rowid_buf,     \
                                                 const int64_t rowid_buf_len,  \
                                                 int64_t &pos,                 \
                                                 ObObj &pk_val)                \
  {                                                                            \
    return get_string_pk_value(obj_type, rowid_buf, rowid_buf_len, pos, pk_val); \
  }

int ObURowIDData::get_string_pk_value(const ObObjType obj_type,
                                      const uint8_t *rowid_buf,
                                      const int64_t rowid_buf_len,
                                      int64_t &pos,
                                      ObObj &pk_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0 || pos + sizeof(uint16_t) + sizeof(uint32_t) > rowid_buf_len)) {
    ret = OB_INVALID_ROWID;
  } else {
    const ObCollationType coll_type = static_cast<ObCollationType>(
        *reinterpret_cast<const uint16_t *>(rowid_buf + pos));
    pos += sizeof(uint16_t);
    const uint32_t string_len = *reinterpret_cast<const uint32_t *>(rowid_buf + pos);
    pos += sizeof(uint32_t);
    if (OB_UNLIKELY(!ObCharset::is_valid_collation(coll_type)
                    || string_len > INT32_MAX
                    || string_len > static_cast<uint64_t>(rowid_buf_len - pos))) {
      ret = OB_INVALID_ROWID;
    } else {
      const ObString string_value(static_cast<int32_t>(string_len),
                                  reinterpret_cast<const char *>(rowid_buf + pos));
      pos += string_len;
      switch (obj_type) {
        case ObVarcharType:
          pk_val.set_varchar(string_value);
          break;
        case ObCharType:
          pk_val.set_char(string_value);
          break;
        case ObNVarchar2Type:
          pk_val.set_nvarchar2(string_value);
          break;
        case ObNCharType:
          pk_val.set_nchar(string_value);
          break;
        case ObRawType:
          pk_val.set_raw(string_value);
          break;
        case ObLongTextType:
          pk_val.set_lob_value(ObLongTextType, string_value.ptr(), string_value.length());
          break;
        default:
          ret = OB_INVALID_ROWID;
          break;
      }
      if (OB_SUCC(ret)) {
        pk_val.set_collation_type(coll_type);
      }
    }
  }
  return ret;
}

#define DEF_GET_OTIME_PK_VALUE(obj_type, type, desc_type)                               \
  template <>                                                                           \
  int ObURowIDData::inner_get_pk_value<obj_type>(const uint8_t *rowid_buf,              \
                                                 const int64_t rowid_buf_len,           \
                                                 int64_t &pos, ObObj &pk_val) {         \
    int ret = OB_SUCCESS;                                                               \
    if (OB_LIKELY(pos + 8 + sizeof(desc_type) <= rowid_buf_len)) {                      \
      int64_t time_us = *(reinterpret_cast<const int64_t *>(rowid_buf + pos));          \
      pos += 8;                                                                         \
      desc_type desc_val =                                                              \
          *(reinterpret_cast<const desc_type *>(rowid_buf + pos));                      \
      pos += sizeof(desc_type);                                                         \
      pk_val.set_##type(time_us, desc_val);                                             \
    } else {                                                                            \
      ret = OB_INVALID_ROWID;                                                           \
    }                                                                                   \
    return ret;                                                                         \
  }

DEF_GET_POD_PK_VALUE(ObUTinyIntType, utinyint, uint8_t);
DEF_GET_POD_PK_VALUE(ObUSmallIntType, usmallint, uint16_t);
DEF_GET_POD_PK_VALUE(ObUMediumIntType, umediumint, uint32_t);
DEF_GET_POD_PK_VALUE(ObUInt32Type, uint32, uint32_t);
DEF_GET_POD_PK_VALUE(ObUInt64Type, uint64, uint64_t);
DEF_GET_POD_PK_VALUE(ObTinyIntType, tinyint, int8_t);
DEF_GET_POD_PK_VALUE(ObSmallIntType, smallint, int16_t);
DEF_GET_POD_PK_VALUE(ObMediumIntType, mediumint, int32_t);
DEF_GET_POD_PK_VALUE(ObInt32Type, int32, int32_t);
DEF_GET_POD_PK_VALUE(ObIntType, int, int64_t);
DEF_GET_POD_PK_VALUE(ObFloatType, float, float);
DEF_GET_POD_PK_VALUE(ObDoubleType, double, double);
// 定长类型统一沿用POD格式，保证size/set/get三端字节宽度一致。
DEF_GET_POD_PK_VALUE(ObUFloatType, ufloat, float);
DEF_GET_POD_PK_VALUE(ObUDoubleType, udouble, double);
DEF_GET_POD_PK_VALUE(ObDateTimeType, datetime, int64_t);
DEF_GET_POD_PK_VALUE(ObTimestampType, timestamp, int64_t);
DEF_GET_POD_PK_VALUE(ObDateType, date, int32_t);
DEF_GET_POD_PK_VALUE(ObTimeType, time, int64_t);
DEF_GET_POD_PK_VALUE(ObYearType, year, uint8_t);
DEF_GET_POD_PK_VALUE(ObBitType, bit, uint64_t);
DEF_GET_POD_PK_VALUE(ObEnumType, enum, uint64_t);
DEF_GET_POD_PK_VALUE(ObSetType, set, uint64_t);
DEF_GET_POD_PK_VALUE(ObMySQLDateType, mysql_date, int32_t);
DEF_GET_POD_PK_VALUE(ObMySQLDateTimeType, mysql_datetime, int64_t);
DEF_GET_POD_PK_VALUE(ObIntervalYMType, interval_ym, int64_t);

DEF_GET_NUMBER_PK_VALUE(ObNumberType, number);
DEF_GET_NUMBER_PK_VALUE(ObUNumberType, unumber);
DEF_GET_NUMBER_PK_VALUE(ObNumberFloatType, number_float);

DEF_GET_STRING_PK_VALUE(ObVarcharType);
DEF_GET_STRING_PK_VALUE(ObCharType);
DEF_GET_STRING_PK_VALUE(ObNVarchar2Type);
DEF_GET_STRING_PK_VALUE(ObNCharType);
DEF_GET_STRING_PK_VALUE(ObRawType);
DEF_GET_STRING_PK_VALUE(ObLongTextType);
DEF_GET_OTIME_PK_VALUE(ObTimestampTZType, timestamp_tz, uint32_t);
DEF_GET_OTIME_PK_VALUE(ObTimestampLTZType, timestamp_ltz, uint16_t);
DEF_GET_OTIME_PK_VALUE(ObTimestampNanoType, timestamp_nano, uint16_t);

#define ALL_TYPES_USED_IN_INNER_FUNC \
  ObNullType,                        \
                                     \
  ObTinyIntType,                     \
  ObSmallIntType,                    \
  ObMediumIntType,                   \
  ObInt32Type,                       \
  ObIntType,                         \
                                     \
  ObUTinyIntType,                    \
  ObUSmallIntType,                   \
  ObUMediumIntType,                  \
  ObUInt32Type,                      \
  ObUInt64Type,                      \
                                     \
  ObFloatType,                       \
  ObDoubleType,                      \
                                     \
  ObUFloatType,                      \
  ObUDoubleType,                     \
                                     \
  ObNumberType,                      \
  ObUNumberType,                     \
                                     \
  ObDateTimeType,                    \
  ObTimestampType,                   \
  ObDateType,                        \
  ObTimeType,                        \
  ObYearType,                        \
                                     \
  ObVarcharType,                     \
  ObCharType,                        \
                                     \
  ObHexStringType,                   \
                                     \
  ObExtendType,                      \
  ObUnknownType,                     \
                                     \
  ObTinyTextType,                    \
  ObTextType,                        \
  ObMediumTextType,                  \
  ObLongTextType,                    \
                                     \
  ObBitType,                         \
  ObEnumType,                        \
  ObSetType,                         \
  ObEnumInnerType,                   \
  ObSetInnerType,                    \
                                     \
  ObTimestampTZType,                 \
  ObTimestampLTZType,                \
  ObTimestampNanoType,               \
                                     \
  ObRawType,                         \
                                     \
  ObIntervalYMType,                  \
  ObIntervalDSType,                  \
                                     \
  ObNumberFloatType,                 \
                                     \
  ObNVarchar2Type,                   \
  ObNCharType,                       \
                                     \
  ObURowIDType,                      \
  ObLobType,                         \
  ObJsonType,                        \
  ObGeometryType,                    \
  ObUserDefinedSQLType,              \
  ObDecimalIntType,                  \
  ObCollectionSQLType,               \
  ObMySQLDateType,                   \
  ObMySQLDateTimeType,               \
  ObRoaringBitmapType

#define DEF_GET_PK_FUNC(obj_type) ObURowIDData::inner_get_pk_value<obj_type>

ObURowIDData::get_pk_val_func ObURowIDData::inner_get_funcs_[ObMaxType] = {
  LST_DO(DEF_GET_PK_FUNC, (,), ALL_TYPES_USED_IN_INNER_FUNC)
};

inline int ObURowIDData::get_pk_value(ObObjType obj_type, int64_t &pos, ObObj &pk_val) const
{
  int ret = OB_SUCCESS;
  const uint8_t version = get_version();
  if (OB_UNLIKELY(PK_ROWID_VERSION != version
                  && EXTERNAL_TABLE_ROWID_VERSION != version)) {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid rowid version", K(ret), K(version));
  } else if (OB_UNLIKELY(obj_type >= ObMaxType)
             || OB_UNLIKELY(NULL == inner_get_funcs_[obj_type])) {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid type or get null for get pk function", K(ret), K(obj_type));
  } else {
    ret = inner_get_funcs_[obj_type](rowid_content_, rowid_len_, pos, pk_val);
  }
  return ret;
}

bool ObURowIDData::operator==(const ObURowIDData &other) const
{
  return 0 == compare(other);
}

bool ObURowIDData::operator !=(const ObURowIDData &other) const
{
  return 0 != compare(other);
}

bool ObURowIDData::operator >(const ObURowIDData &other) const
{
  return 1 == compare(other);
}

bool ObURowIDData::operator <(const ObURowIDData &other) const
{
  return -1 == compare(other);
}

bool ObURowIDData::operator >=(const ObURowIDData &other) const
{
  int cmp_res = compare(other);
  return cmp_res >= 0;
}

bool ObURowIDData::operator <=(const ObURowIDData &other) const
{
  int cmp_res = compare(other);
  return cmp_res <= 0;
}

int64_t ObURowIDData::needed_base64_buffer_size() const
{
  return needed_base64_buffer_size(get_buf_len(), is_physical_rowid());
}

int64_t ObURowIDData::needed_base64_buffer_size(const int64_t content_size,
                                                const bool is_physical_rowid)
{
  int64_t rowid_str_len = ObBase64Encoder::needed_encoded_length(content_size);
  if (!is_physical_rowid) {
    rowid_str_len += 1; // extra 1 byte for '*'
  }
  return rowid_str_len;
}

int64_t ObURowIDData::needed_max_base64_buffer_size(const int64_t content_size)
{
  return needed_base64_buffer_size(content_size, false/*is_physical_rowid*/);
}

bool ObURowIDData::is_variable_length_pk_type(const ObObjType type)
{
  return ObRawType == type
      || ObVarcharType == type
      || ObCharType == type
      || ObNVarchar2Type == type
      || ObNCharType == type
      || ObLongTextType == type
      || ObURowIDType == type;
}

bool ObURowIDData::is_string_pk_type(const ObObjType type)
{
  return ObRawType == type
      || ObVarcharType == type
      || ObCharType == type
      || ObNVarchar2Type == type
      || ObNCharType == type
      || ObLongTextType == type;
}

int64_t ObURowIDData::get_max_serialized_pk_value_size(const ObObjType type,
                                                       const int64_t data_length)
{
  const int64_t string_collation_size = is_string_pk_type(type) ? sizeof(uint16_t) : 0;
  const int64_t variable_value_size = data_length + sizeof(uint8_t)
      + string_collation_size + sizeof(uint32_t);
  const int64_t decimal_int512_size = sizeof(uint8_t) + sizeof(ObScale) + sizeof(uint32_t) + sizeof(int512_t);
  return is_variable_length_pk_type(type) ? variable_value_size : decimal_int512_size;
}

int ObURowIDData::get_base64_str(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (pos + 1 > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough", K(ret));
  } else {
    if (!is_physical_rowid()) {
      buf[pos++] = '*';
    }
    ret = ObBase64Encoder::encode(rowid_content_, get_buf_len(), buf, buf_len, pos);
  }
  return ret;
}

int ObURowIDData::decode2urowid(const char* input, const int64_t input_len,
                                ObIAllocator &allocator, ObURowIDData &urowid_data)
{
  int ret = OB_SUCCESS;
  uint8_t *decoded_buf = NULL;
  int64_t decoded_buf_len = 0;
  int64_t pos = 0;
  int64_t base64_input_len = 0;
  if (OB_UNLIKELY(OB_NOT_NULL(urowid_data.rowid_content_))) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(input) || input_len < 1)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(input), K(input_len));
  } else {
    if (input[0] == '*') {
      input += 1;
      base64_input_len = input_len - 1;
    } else {
      base64_input_len = input_len;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(decoded_buf_len = ObBase64Encoder::needed_decoded_length(base64_input_len))) {
  } else if (OB_UNLIKELY(decoded_buf_len <= 0 || decoded_buf_len % 3 != 0)) {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid base64 str for rowid", K(ret), K(decoded_buf_len));
  } else if (OB_ISNULL(decoded_buf = (uint8_t *)allocator.alloc(decoded_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate memory", K(ret), K(decoded_buf_len));
  } else if (OB_FAIL(ObBase64Encoder::decode(input, base64_input_len, decoded_buf, decoded_buf_len, pos))) {
    COMMON_LOG(WARN, "failed to decode base64_str to urowid", K(ret));
    ret = OB_INVALID_ROWID;
  } else {
    urowid_data.rowid_content_ = decoded_buf;
    urowid_data.rowid_len_ = pos;
    // check validity after decoding
    if (OB_FAIL(urowid_data.check_is_valid_urowid())) {
      urowid_data.rowid_content_ = NULL;
      urowid_data.rowid_len_ = 0;
      COMMON_LOG(WARN, "invalid rowid", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ret) && nullptr != decoded_buf) {
    allocator.free(decoded_buf);
  }
  return ret;
}

int ObURowIDData::build_invalid_rowid_obj(ObIAllocator &alloc, ObObj *&rowid_obj)
{
  int ret = OB_SUCCESS;
  ObURowIDData data;
  // 12 * 4/3 + 1(star) = 17; we want base64 char to be 17 chars, so rowid_len_ is 12.
  // Oracle physical rowid got 18 chars.
  data.rowid_len_ = 12;
  data.rowid_content_ = (uint8_t*)(alloc.alloc(data.rowid_len_));
  void *buf = alloc.alloc(sizeof(ObObj));
  if (OB_ISNULL(data.rowid_content_) || OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "alloc mem failed", K(ret), KP(buf), KP(data.rowid_content_));
  } else {
    MEMSET((void*)(data.rowid_content_), '\0', data.rowid_len_);
    rowid_obj = new (buf) ObObj();
    rowid_obj->set_urowid(data);
  }
  if (OB_FAIL(ret) && nullptr != data.rowid_content_) {
    alloc.free((void *)data.rowid_content_);
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    alloc.free(buf);
  }
  return ret;
}

int ObURowIDData::check_is_valid_urowid() const
{
  int ret = OB_SUCCESS;
  const uint8_t version = get_version();
  if (HEAP_TABLE_ROWID_VERSION == version) {
    if (rowid_len_ != HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
      ret = OB_INVALID_ROWID;
    }
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    if (rowid_len_ != EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
      ret = OB_INVALID_ROWID;
    }
  } else if (PK_ROWID_VERSION == version) {
    ObPkValueIterator iter(*this);
    ObObj obj;
    if (OB_FAIL(iter.init())) {
      COMMON_LOG(WARN, "failed to init primary key rowid iterator", K(ret), KPC(this));
    }
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      const int tmp_ret = iter.get_next_value(obj);
      if (OB_ITER_END == tmp_ret) {
        is_iter_end = true;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        COMMON_LOG(WARN, "failed to get primary key rowid value", K(ret), KPC(this));
      } else if (obj.is_urowid()
                 && OB_FAIL(SMART_CALL(obj.get_urowid().check_is_valid_urowid()))) {
        COMMON_LOG(WARN, "failed to check nested urowid", K(ret), KPC(this));
      }
    }
  } else if (EXTERNAL_TABLE_ROWID_VERSION == version) {
    int64_t pos = get_pk_content_offset();
    ObObj obj;
    for (; OB_SUCC(ret) && pos < rowid_len_; ) {
      ObObjType obj_type = get_pk_type(pos);
      if (OB_UNLIKELY(ob_is_invalid_obj_type(obj_type))) {
        ret = OB_INVALID_ROWID;
        COMMON_LOG(WARN, "invalid obj type in rowid", K(ret), K(obj_type), K(pos), KPC(this));
      } else if (OB_FAIL(get_pk_value(obj_type, pos, obj))) {
        if (OB_NOT_SUPPORTED == ret) {
          ret = OB_INVALID_ROWID; // oracle doesn't have such kind of rowkey
        }
        COMMON_LOG(WARN, "failed to get pk value", K(ret), K(obj_type), K(pos), KPC(this));
      } else if (obj.is_urowid()) {
        if (OB_FAIL(SMART_CALL(obj.get_urowid().check_is_valid_urowid()))) {
          COMMON_LOG(WARN, "failed to check is valid urowid", K(ret), K(pos));
        }
      }
    }
  } else {
    ret = OB_INVALID_ROWID;
  }
  return ret;
}

bool ObURowIDData::is_valid_version(int64_t v)
{
  bool bret = true;
  if (INVALID_ROWID_VERSION == v) {
    bret = false;
  } else if (PK_ROWID_VERSION != v
      && EXTERNAL_TABLE_ROWID_VERSION != v
      && HEAP_TABLE_ROWID_VERSION != v
      && EXT_HEAP_TABLE_ROWID_VERSION != v) {
    bret = false;
  }
  return bret;
}

uint8_t ObURowIDData::get_version() const
{
  uint8_t version = INVALID_ROWID_VERSION;
  if (OB_NOT_NULL(rowid_content_) && 0 < rowid_len_) {
    uint8_t raw_version = rowid_content_[0];
    if (HEAP_TABLE_ROWID_VERSION <= raw_version) {
      version = raw_version & 0xE0;
      if (HEAP_TABLE_ROWID_VERSION == version && rowid_len_ == HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
        // valid, do nothing
      } else if (EXT_HEAP_TABLE_ROWID_VERSION == version && rowid_len_ == EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
        // valid, do nothing
      } else {
        version = INVALID_ROWID_VERSION;
      }
    } else {
      int64_t offset = get_pk_version_offset();
      if (offset < rowid_len_) {
        raw_version = rowid_content_[offset];
        if (PK_ROWID_VERSION == raw_version
            || EXTERNAL_TABLE_ROWID_VERSION == raw_version) {
          version = raw_version;
        }
      }
    }
  }
  return version;
}

int64_t ObURowIDData::get_obj_size(const ObObj &pk_val)
{
  int64_t ret_size = 1; // for obj type
  switch (pk_val.get_type()) {
  case ObNullType:
    ret_size += 0;
    break;
  case ObUTinyIntType:
    ret_size += sizeof(uint8_t);
    break;
  case ObTinyIntType:
    ret_size += sizeof(int8_t);
    break;
  case ObSmallIntType:
    ret_size += sizeof(int16_t);
    break;
  case ObUSmallIntType:
    ret_size += sizeof(uint16_t);
    break;
  case ObMediumIntType:
    ret_size += sizeof(int32_t);
    break;
  case ObUMediumIntType:
    ret_size += sizeof(uint32_t);
    break;
  case ObInt32Type:
    ret_size += sizeof(int32_t);
    break;
  case ObUInt32Type:
    ret_size += sizeof(uint32_t);
    break;
  case ObIntType:
    ret_size += sizeof(int64_t);
    break;
  case ObUInt64Type:
    ret_size += sizeof(uint64_t);
    break;
  case ObFloatType:
  case ObUFloatType:
    ret_size += sizeof(float);
    break;
  case ObDoubleType:
  case ObUDoubleType:
    ret_size += sizeof(double);
    break;
  case ObNumberType:
  case ObUNumberType:
  case ObNumberFloatType: {
    ret_size += sizeof(uint32_t);
    ret_size += pk_val.get_number_digit_length() * sizeof(uint32_t);
  } break;
  case ObDateTimeType:
  case ObTimestampType:
  case ObTimeType:
  case ObMySQLDateTimeType:
    ret_size += sizeof(int64_t);
    break;
  case ObDateType:
  case ObMySQLDateType:
    ret_size += sizeof(int32_t);
    break;
  case ObYearType:
    ret_size += sizeof(uint8_t);
    break;
  case ObBitType:
  case ObEnumType:
  case ObSetType:
    ret_size += sizeof(uint64_t);
    break;
  case ObRawType:
  case ObVarcharType:
  case ObCharType:
  case ObNVarchar2Type:
  case ObNCharType:
  case ObLongTextType: {
    ret_size += sizeof(uint16_t);
    ret_size += sizeof(uint32_t);
    ret_size += pk_val.get_string_len();
  } break;
  case ObTimestampTZType:
    ret_size += sizeof(int64_t) + sizeof(uint32_t);
    break;
  case ObTimestampLTZType:
  case ObTimestampNanoType:
    ret_size += sizeof(int64_t) + sizeof(uint16_t);
    break;
  case ObIntervalYMType:
    ret_size += sizeof(int64_t);
    break;
  case ObIntervalDSType:
    ret_size += sizeof(int64_t) + sizeof(int32_t);
    break;
  case ObURowIDType:
    ret_size += sizeof(uint32_t) + pk_val.get_string_len();
    break;
  case ObDecimalIntType:
    ret_size += sizeof(ObScale); // scale
    ret_size += sizeof(uint32_t) + pk_val.get_int_bytes(); // length & decimal int data
    break;
  default:
    ret_size = 0;
    break;
  }
  return ret_size;
}

template<>
int ObURowIDData::inner_set_pk_value<ObNullType>(const ObObj &pk_val, uint8_t *buffer,
                                                 const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != buffer);
  OB_ASSERT(ObNullType == pk_val.get_type());
  OB_ASSERT(pos + 1 <= buf_len && pos >= 0);
  buffer[pos++] = static_cast<uint8_t>(ObNullType);
  return ret;
}

template<>
int ObURowIDData::inner_set_pk_value<ObIntervalDSType>(const ObObj &pk_val, uint8_t *buffer,
                                                       const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t needed_size = 13;
  OB_ASSERT(NULL != buffer);
  OB_ASSERT(ObIntervalDSType == pk_val.get_type());
  OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);
  buffer[pos++] = static_cast<uint8_t>(ObIntervalDSType);
  *(reinterpret_cast<int64_t *>(buffer + pos)) = pk_val.get_interval_ds().nsecond_;
  pos += 8;
  *(reinterpret_cast<int32_t *>(buffer + pos)) = pk_val.get_interval_ds().fractional_second_;
  pos += 4;
  return ret;
}

template<>
int ObURowIDData::inner_set_pk_value<ObIntervalYMType>(const ObObj &pk_val, uint8_t *buffer,
                                                       const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t needed_size = 9;
  OB_ASSERT(NULL != buffer);
  OB_ASSERT(ObIntervalYMType == pk_val.get_type());
  OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);
  buffer[pos++] = static_cast<uint8_t>(ObIntervalYMType);
  *(reinterpret_cast<int64_t *>(buffer + pos)) = pk_val.get_interval_ym().get_nmonth();
  pos += 8;
  return ret;
}

template<>
int ObURowIDData::inner_set_pk_value<ObURowIDType>(const ObObj &pk_val, uint8_t *buffer,
                                                   const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t needed_size = 1 + 4 + pk_val.get_string_len();
  OB_ASSERT(NULL != buffer);
  OB_ASSERT(ObURowIDType == pk_val.get_type());
  OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);
  buffer[pos++] = static_cast<uint8_t>(ObURowIDType);
  *(reinterpret_cast<uint32_t *>(buffer + pos)) = pk_val.get_string_len();
  pos += 4;
  MEMCPY(buffer + pos, pk_val.get_string_ptr(), pk_val.get_string_len());
  pos += pk_val.get_string_len();
  return ret;
}

#define DEF_SET_POD_PK_VALUE(obj_type, pod_type, type)                                  \
  template <>                                                                           \
  int ObURowIDData::inner_set_pk_value<obj_type>(const ObObj &pk_val, uint8_t *buffer,  \
                                                 const int64_t buf_len, int64_t &pos) { \
    int ret = OB_SUCCESS;                                                               \
    int64_t needed_size = sizeof(pod_type) + 1;                                         \
    OB_ASSERT(NULL != buffer);                                                          \
    OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);                                \
    OB_ASSERT(obj_type == pk_val.get_type());                                           \
    buffer[pos++] = static_cast<uint8_t>(obj_type);                                     \
    *(reinterpret_cast<pod_type *>(buffer + pos)) =                                     \
      static_cast<pod_type>(pk_val.get_##type());                                       \
    pos += sizeof(pod_type);                                                            \
    return ret;                                                                         \
  }

#define DEF_SET_STRING_PK_VALUE(obj_type)                                              \
  template<>                                                                           \
  int ObURowIDData::inner_set_pk_value<obj_type>(const ObObj &pk_val, uint8_t *buffer, \
                                                 const int64_t buf_len, int64_t &pos)  \
  {                                                                                    \
    return set_string_pk_value(pk_val, buffer, buf_len, pos);                          \
  }

#define DEF_SET_NUMBER_PK_VALUE(obj_type)                                                  \
  template <>                                                                              \
  int ObURowIDData::inner_set_pk_value<obj_type>(const ObObj &pk_val,                      \
                                                 uint8_t *buffer, const int64_t buf_len,   \
                                                 int64_t &pos) {                           \
    int ret = OB_SUCCESS;                                                                  \
    if (OB_FAIL(pk_val.get_number().sanity_check())) {                                     \
      ret = OB_INVALID_NUMERIC;                                                            \
      COMMON_LOG(WARN, "invalid number", K(ret));                                          \
    } else {                                                                               \
      int64_t needed_size =                                                                \
          1 + sizeof(ObNumberDesc) + sizeof(uint32_t) * pk_val.get_number_digit_length();  \
      OB_ASSERT(NULL != buffer);                                                           \
      OB_ASSERT(obj_type == pk_val.get_type());                                            \
      OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);                                 \
      buffer[pos++] = static_cast<uint8_t>(obj_type);                                      \
      *(reinterpret_cast<ObNumberDesc *>(buffer + pos)) =                                  \
          pk_val.get_number_desc();                                                        \
      pos += sizeof(ObNumberDesc);                                                         \
      MEMCPY(buffer + pos, pk_val.get_number_digits(),                                     \
            pk_val.get_number_digit_length() * sizeof(uint32_t));                          \
      pos += sizeof(uint32_t) * pk_val.get_number_digit_length();                          \
    }                                                                                      \
    return ret;                                                                            \
  }

#define DEF_SET_OTIME_PK_VALUE(obj_type, desc_macro_flag)                                  \
  template<>                                                                               \
  int ObURowIDData::inner_set_pk_value<obj_type>(const ObObj &pk_val, uint8_t *buffer,     \
                                                 const int64_t buf_len, int64_t &pos)      \
  {                                                                                        \
    int ret = OB_SUCCESS;                                                                  \
    int64_t needed_size = 1 + sizeof(int64_t) + (desc_macro_flag > 0 ? 2 : 4);             \
    OB_ASSERT(buffer != NULL);                                                             \
    OB_ASSERT(obj_type == pk_val.get_type());                                              \
    OB_ASSERT(needed_size + pos <= buf_len && pos >= 0);                                   \
    buffer[pos++] = static_cast<uint8_t>(obj_type);                                        \
    *(reinterpret_cast<int64_t *>(buffer + pos)) = pk_val.get_otimestamp_value().time_us_; \
    pos += 8;                                                                              \
    if (0 == desc_macro_flag) {                                                            \
      *(reinterpret_cast<uint32_t *>(buffer + pos))                                        \
        = pk_val.get_otimestamp_value().time_ctx_.desc_;                                   \
      pos += 4;                                                                            \
    } else if (1 == desc_macro_flag) {                                                     \
      *(reinterpret_cast<uint16_t *>(buffer + pos))                                        \
        = pk_val.get_otimestamp_value().time_ctx_.time_desc_;                              \
      pos += 2;                                                                            \
    } else {                                                                               \
      *(reinterpret_cast<uint16_t *>(buffer + pos))                                        \
        = pk_val.get_otimestamp_value().time_ctx_.tz_desc_;                                \
      pos += 2;                                                                            \
    }                                                                                      \
    return ret;                                                                            \
  }

DEF_SET_POD_PK_VALUE(ObUTinyIntType, uint8_t, utinyint);
DEF_SET_POD_PK_VALUE(ObUSmallIntType, uint16_t, usmallint);
DEF_SET_POD_PK_VALUE(ObUMediumIntType, uint32_t, umediumint);
DEF_SET_POD_PK_VALUE(ObUInt32Type, uint32_t, uint32);
DEF_SET_POD_PK_VALUE(ObUInt64Type, uint64_t, uint64);
DEF_SET_POD_PK_VALUE(ObTinyIntType, int8_t, tinyint);
DEF_SET_POD_PK_VALUE(ObSmallIntType, int16_t, smallint);
DEF_SET_POD_PK_VALUE(ObMediumIntType, int32_t, mediumint);
DEF_SET_POD_PK_VALUE(ObInt32Type, int32_t, int32);
DEF_SET_POD_PK_VALUE(ObIntType, int64_t, int);

DEF_SET_POD_PK_VALUE(ObFloatType, float, float);
DEF_SET_POD_PK_VALUE(ObDoubleType, double, double);
// 与解码侧使用同一ObObj getter/setter，避免有符号属性或时间宽度丢失。
DEF_SET_POD_PK_VALUE(ObUFloatType, float, ufloat);
DEF_SET_POD_PK_VALUE(ObUDoubleType, double, udouble);

DEF_SET_POD_PK_VALUE(ObDateTimeType, int64_t, datetime);
DEF_SET_POD_PK_VALUE(ObTimestampType, int64_t, timestamp);
DEF_SET_POD_PK_VALUE(ObDateType, int32_t, date);
DEF_SET_POD_PK_VALUE(ObTimeType, int64_t, time);
DEF_SET_POD_PK_VALUE(ObYearType, uint8_t, year);
DEF_SET_POD_PK_VALUE(ObBitType, uint64_t, bit);
DEF_SET_POD_PK_VALUE(ObEnumType, uint64_t, enum);
DEF_SET_POD_PK_VALUE(ObSetType, uint64_t, set);

// MySQL日期包装类型直接保存内部位模式，零日期等兼容值不会被标准日期语义改写。
template<>
int ObURowIDData::inner_set_pk_value<ObMySQLDateType>(const ObObj &pk_val,
                                                      uint8_t *buffer,
                                                      const int64_t buf_len,
                                                      int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t needed_size = 1 + sizeof(int32_t);
  const ObObjType obj_type = pk_val.get_type();
  if (OB_UNLIKELY(NULL == buffer
                  || ObMySQLDateType != obj_type
                  || pos < 0
                  || pos > buf_len
                  || needed_size > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid mysql date rowid serialize state",
               K(ret), KP(buffer), K(pos), K(buf_len), K(needed_size), K(obj_type));
  } else {
    buffer[pos++] = static_cast<uint8_t>(ObMySQLDateType);
    *(reinterpret_cast<int32_t *>(buffer + pos)) = pk_val.get_mysql_date().date_;
    pos += sizeof(int32_t);
  }
  return ret;
}

template<>
int ObURowIDData::inner_set_pk_value<ObMySQLDateTimeType>(const ObObj &pk_val,
                                                          uint8_t *buffer,
                                                          const int64_t buf_len,
                                                          int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t needed_size = 1 + sizeof(int64_t);
  const ObObjType obj_type = pk_val.get_type();
  if (OB_UNLIKELY(NULL == buffer
                  || ObMySQLDateTimeType != obj_type
                  || pos < 0
                  || pos > buf_len
                  || needed_size > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid mysql datetime rowid serialize state",
               K(ret), KP(buffer), K(pos), K(buf_len), K(needed_size), K(obj_type));
  } else {
    buffer[pos++] = static_cast<uint8_t>(ObMySQLDateTimeType);
    *(reinterpret_cast<int64_t *>(buffer + pos)) = pk_val.get_mysql_datetime().datetime_;
    pos += sizeof(int64_t);
  }
  return ret;
}

DEF_SET_NUMBER_PK_VALUE(ObNumberType);
DEF_SET_NUMBER_PK_VALUE(ObUNumberType);
DEF_SET_NUMBER_PK_VALUE(ObNumberFloatType);

DEF_SET_STRING_PK_VALUE(ObCharType);
DEF_SET_STRING_PK_VALUE(ObVarcharType);
DEF_SET_STRING_PK_VALUE(ObNVarchar2Type);
DEF_SET_STRING_PK_VALUE(ObNCharType);
DEF_SET_STRING_PK_VALUE(ObRawType);
DEF_SET_STRING_PK_VALUE(ObLongTextType);

int ObURowIDData::set_string_pk_value(const ObObj &pk_val,
                                      uint8_t *buffer,
                                      const int64_t buf_len,
                                      int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObObjType obj_type = pk_val.get_type();
  const int64_t string_len = pk_val.get_string_len();
  const int64_t coll_type = pk_val.get_collation_type();
  const int64_t needed_size = sizeof(uint8_t) + sizeof(uint16_t)
      + sizeof(uint32_t) + string_len;
  if (OB_UNLIKELY(!is_string_pk_type(obj_type)
                  || string_len < 0
                  || string_len > UINT32_MAX
                  || coll_type > UINT16_MAX
                  || !ObCharset::is_valid_collation(coll_type))) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "invalid string rowid value",
               K(ret), K(string_len), K(coll_type), K(obj_type));
  } else if (OB_UNLIKELY(NULL == buffer
                         || pos < 0
                         || pos > buf_len
                         || needed_size > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid string rowid serialize state",
               K(ret), KP(buffer), K(pos), K(buf_len), K(needed_size), K(obj_type));
  } else {
    buffer[pos++] = static_cast<uint8_t>(obj_type);
    *reinterpret_cast<uint16_t *>(buffer + pos) = static_cast<uint16_t>(coll_type);
    pos += sizeof(uint16_t);
    *reinterpret_cast<uint32_t *>(buffer + pos) = static_cast<uint32_t>(string_len);
    pos += sizeof(uint32_t);
    MEMCPY(buffer + pos, pk_val.get_string_ptr(), string_len);
    pos += string_len;
  }
  return ret;
}

DEF_SET_OTIME_PK_VALUE(ObTimestampTZType, 0);
DEF_SET_OTIME_PK_VALUE(ObTimestampLTZType, 1);
DEF_SET_OTIME_PK_VALUE(ObTimestampNanoType, 2);

template<>
int ObURowIDData::inner_set_pk_value<ObDecimalIntType>(const ObObj &pk_val, uint8_t *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t needed_size = 1 + sizeof(int32_t) + sizeof(ObScale) + pk_val.get_int_bytes();
  OB_ASSERT(NULL != buf);
  OB_ASSERT(pk_val.is_decimal_int());
  OB_ASSERT(pos + needed_size <= buf_len && pos >= 0);
  buf[pos++] = static_cast<uint8_t>(ObDecimalIntType);
  *reinterpret_cast<ObScale *>(buf + pos) = pk_val.get_scale();
  pos += sizeof(ObScale);
  *reinterpret_cast<int32_t *>(buf + pos) = pk_val.get_int_bytes();
  pos += sizeof(int32_t);
  MEMCPY(buf + pos, pk_val.get_decimal_int(), pk_val.get_int_bytes());
  pos += pk_val.get_int_bytes();
  return ret;
}

#define DEF_SET_PK_FUNC(obj_type) ObURowIDData::inner_set_pk_value<obj_type>

ObURowIDData::set_pk_val_func ObURowIDData::inner_set_funcs_[ObMaxType] = {
  LST_DO(DEF_SET_PK_FUNC, (,), ALL_TYPES_USED_IN_INNER_FUNC)
};

int ObURowIDData::set_rowid_content(const ObIArray<ObObj> &args,
                                    const int64_t version,
                                    const int64_t real_pk_count,
                                    ObIAllocator &allocator,
                                    const int64_t dba_len /* 0 */,
                                    const uint8_t *dba_addr /* NULL*/)
{
  int ret = OB_SUCCESS;
  UNUSED(dba_addr);
  UNUSED(dba_len);
  int64_t generated_column_count = 0;
  int64_t rowid_buf_len = needed_content_buf_size(args, version);
  uint8_t *rowid_buf = NULL;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_NOT_NULL(rowid_content_))) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (PK_ROWID_VERSION == version
             && OB_UNLIKELY(!is_valid_pk_count(real_pk_count)
                            || real_pk_count > args.count())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid rowid real pk count",
               K(ret), K(real_pk_count), "value_count", args.count());
  } else if (PK_ROWID_VERSION == version
             && OB_FALSE_IT(generated_column_count = args.count() - real_pk_count)) {
  } else if (PK_ROWID_VERSION == version
             && OB_UNLIKELY(!is_valid_generated_partition_column_count(
                 generated_column_count))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid generated partition column count",
               K(ret), K(generated_column_count), "value_count", args.count());
  } else if (OB_ISNULL(rowid_buf = (uint8_t *)allocator.alloc(rowid_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate memory", K(ret));
  } else if (OB_UNLIKELY(!is_valid_version(version))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid rowid version", K(ret), K(version));
  } else if (HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(set_heap_organized_table_rowid_content(args, rowid_buf, rowid_buf_len, pos))) {
      COMMON_LOG(WARN, "failed to set heap organized table rowid content", K(ret));
    }
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(set_ext_heap_organized_table_rowid_content(args, rowid_buf, rowid_buf_len, pos))) {
      COMMON_LOG(WARN, "failed to set ext heap organized table rowid content", K(ret));
    }
  } else {
    rowid_buf[pos++] = 0; // guess dba is not used for now
    rowid_buf[pos++] = version;
    if (PK_ROWID_VERSION == version) {
      rowid_buf[pos++] = static_cast<uint8_t>(real_pk_count);
    }
    const int64_t rowkey_value_count = PK_ROWID_VERSION == version
        ? real_pk_count : args.count();
    for (int i = 0; OB_SUCC(ret) && i < rowkey_value_count; i++) {
      set_pk_val_func func_ptr = inner_set_funcs_[args.at(i).get_type()];
      OB_ASSERT(NULL != func_ptr);
      ret = func_ptr(args.at(i), rowid_buf, rowid_buf_len, pos);
      if (OB_FAIL(ret)) {
        COMMON_LOG(WARN, "failed to set pk value", K(ret));
      }
    } // for end
    if (OB_SUCC(ret) && PK_ROWID_VERSION == version) {
      if (OB_FAIL(serialization::encode_i16(
          reinterpret_cast<char *>(rowid_buf), rowid_buf_len, pos,
          static_cast<int16_t>(generated_column_count)))) {
        COMMON_LOG(WARN, "failed to encode generated partition column count",
                   K(ret), K(generated_column_count), K(pos), K(rowid_buf_len));
      }
      for (int64_t i = real_pk_count; OB_SUCC(ret) && i < args.count(); ++i) {
        set_pk_val_func func_ptr = inner_set_funcs_[args.at(i).get_type()];
        OB_ASSERT(NULL != func_ptr);
        ret = func_ptr(args.at(i), rowid_buf, rowid_buf_len, pos);
        if (OB_FAIL(ret)) {
          COMMON_LOG(WARN, "failed to set generated partition column value", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    rowid_len_ = pos;
    rowid_content_ = rowid_buf;
  }
  if (OB_FAIL(ret) && nullptr != rowid_buf) {
    allocator.free(rowid_buf);
  }
  return ret;
}

int ObURowIDData::set_heap_organized_table_rowid_content(const ObIArray<ObObj> &args,
                                                         uint8_t *buf,
                                                         const int64_t buf_len,
                                                         int64_t &pos)
{
  int ret = OB_SUCCESS;
  uint64_t auto_inc = 0;
  uint64_t tablet_id = 0;
  if (OB_UNLIKELY(buf_len != HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected content buf size", K(ret));
  } else if (OB_UNLIKELY(args.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected argument count", K(ret), K(args));
  } else if (OB_UNLIKELY(args.at(0).get_type() != ObObjType::ObUInt64Type)
          || OB_UNLIKELY(args.at(1).get_type() != ObObjType::ObIntType)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument type", K(ret), K(args));
  } else if (OB_FALSE_IT(auto_inc = args.at(0).get_uint64())) {
  } else if (OB_FALSE_IT(tablet_id = static_cast<uint64_t>(args.at(1).get_int()))) {
  } else if (OB_UNLIKELY(ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID < tablet_id)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid tablet id", K(ret), K(tablet_id), K(auto_inc));
  } else if (OB_UNLIKELY(static_cast<uint64_t>(OB_MAX_AUTOINC_SEQ_VALUE) < auto_inc)) {
    ret = OB_HEAP_TABLE_EXAUSTED;
    COMMON_LOG(WARN, "invalid auto inc value", K(ret), K(tablet_id), K(auto_inc));
    LOG_DBA_ERROR_V2(OB_SHARE_PRIMARY_KEY_SEQUENCE_EXHAUSTED, OB_HEAP_TABLE_EXAUSTED,
                           "The hidden primary key sequence has exhausted. ",
                           "tablet_id is ", tablet_id, " and current_seq is ", auto_inc, ". ",
                           "[suggestion] If you need a larger key range, ",
                           "you can connect cluster by current tenant and execute sql command: ",
                           "alter table {database_name}.{table_name} set enable_extended_rowid = true; ",
                           "If you don't know the {database_name} or {table_name}, you can get them by executing sql command: ",
                           "(Mysql mode): ",
                           "select distinct database_name, table_name from oceanbase.DBA_OB_TABLE_LOCATIONS where tablet_id = ", tablet_id, "; ",
                           "(Oracle mode): ",
                           "select distinct database_name, table_name from sys.DBA_OB_TABLE_LOCATIONS where tablet_id = ", tablet_id, "; ");
  } else {
    STATIC_ASSERT((HEAP_TABLE_ROWID_VERSION & 0x1F) == 0, "invalid version");
    STATIC_ASSERT(ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID < 1ULL << 37,
                  "max heap table tablet id overflow");
    STATIC_ASSERT(static_cast<uint64_t>(OB_MAX_AUTOINC_SEQ_VALUE) < 1ULL << 40,
                  "max autoinc seq value overflow");
    STATIC_ASSERT((HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS % 8) == 0, "invalid non-embeded bits");

    pos = 0;
    buf[pos++] = HEAP_TABLE_ROWID_VERSION + ((tablet_id >> HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS) & 0x1F);
    for (int i = 1; i <= HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS / 8; i++) {
      buf[pos++] = (tablet_id >> (HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS - 8*i)) & 0xFF;
    }
    for (int i = 4; i >= 0; i--) {
      buf[pos++] = (auto_inc >> (i * 8)) & 0xFF;
    }
  }
  return ret;
}

int ObURowIDData::set_ext_heap_organized_table_rowid_content(const ObIArray<ObObj> &args,
                                                             uint8_t *buf,
                                                             const int64_t buf_len,
                                                             int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len != EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected content buf size", K(ret));
  } else if (OB_UNLIKELY(args.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected argument count", K(ret), K(args));
  } else if (OB_UNLIKELY(args.at(0).get_type() != ObObjType::ObUInt64Type)
          || OB_UNLIKELY(args.at(1).get_type() != ObObjType::ObIntType)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument type", K(ret), K(args));
  } else {
    STATIC_ASSERT((EXT_HEAP_TABLE_ROWID_VERSION & 0x1F) == 0, "invalid version");
    STATIC_ASSERT(ObTabletID::MAX_USER_EXTENDED_ROWID_TABLE_TABLET_ID < 1ULL << 61,
                  "max heap table tablet id overflow");
    STATIC_ASSERT((EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS % 8) == 0, "invalid non-embeded bits");

    uint64_t auto_inc = args.at(0).get_uint64();
    uint64_t tablet_id = static_cast<uint64_t>(args.at(1).get_int());
    pos = 0;
    buf[pos++] = EXT_HEAP_TABLE_ROWID_VERSION + ((tablet_id >> EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS) & 0x1F);
    for (int64_t i = 1; i <= EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS / 8; i++) {
      buf[pos++] = (tablet_id >> (EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS - 8*i)) & 0xFF;
    }
    for (int i = 7; i >= 0; i--) {
      buf[pos++] = (auto_inc >> (i * 8)) & 0xFF;
    }
  }
  return ret;
}

int ObURowIDData::parse_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const
{
  int ret = OB_SUCCESS;
  if (rowid_len_ != HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid rowid buf size", K(ret));
  } else {
    int pos = 0;
    tablet_id = static_cast<uint64_t>(rowid_content_[pos++] & 0x1F) << HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS;
    for (int i = 1; i <= HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS / 8; i++) {
      tablet_id += static_cast<uint64_t>(rowid_content_[pos++]) << (HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS - 8*i);
    }
    auto_inc = 0;
    for (int i = 4; i >= 0; i--) {
      uint64_t x = static_cast<uint64_t>(rowid_content_[pos++]);
      auto_inc += x << (i * 8);
    }
  }
  return ret;
}

int ObURowIDData::parse_ext_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const
{
  int ret = OB_SUCCESS;
  if (rowid_len_ != EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid rowid buf size", K(ret));
  } else {
    int pos = 0;
    tablet_id = static_cast<uint64_t>(rowid_content_[pos++] & 0x1F) << EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS;
    for (int64_t i = 1; i <= EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS / 8; i++) {
      tablet_id += static_cast<uint64_t>(rowid_content_[pos++]) << (EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS - 8*i);
    }
    auto_inc = 0;
    for (int i = 7; i >= 0; i--) {
      uint64_t x = static_cast<uint64_t>(rowid_content_[pos++]);
      auto_inc += x << (i * 8);
    }
  }
  return ret;
}

int ObURowIDData::parse_physical_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const
{
  int ret = OB_SUCCESS;
  const uint8_t version = get_version();
  if (HEAP_TABLE_ROWID_VERSION == version) {
    ret = parse_heap_organized_table_rowid(tablet_id, auto_inc);
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    ret = parse_ext_heap_organized_table_rowid(tablet_id, auto_inc);
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid version", K(ret));
  }
  return ret;
}

int ObURowIDData::get_pk_vals(ObIArray<ObObj> &pk_vals) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(check_is_valid_urowid())) {
    COMMON_LOG(WARN, "invalid rowid", K(ret));
  } else if (is_physical_rowid()) {
    if (OB_FAIL(get_rowkey_for_heap_organized_table(pk_vals))) {
      COMMON_LOG(WARN, "failed to get rowkey for heap organized_table", K(ret));
    } else {/*do nothing*/}
  } else if (EXTERNAL_TABLE_ROWID_VERSION == get_version()) {
    // external table rowid = partition id + file id + line number
    pos = get_pk_content_offset();
    int i = 0;
    while (OB_SUCC(ret) && pos < get_buf_len() && i < 3) {
      if (OB_LIKELY(pos + 1 <= get_buf_len())) {
        ObObjType type = get_pk_type(pos);
        ObObj tmp_obj;
        if (OB_LIKELY(is_valid_obj_type(type))) {
          if (OB_FAIL(get_pk_value(type, pos, tmp_obj))) {
            COMMON_LOG(WARN, "failed to get pk value", K(ret));
          } else if (i > 0) { // skip partition id here
            if (OB_FAIL(pk_vals.push_back(tmp_obj))) {
              COMMON_LOG(WARN, "failed to push back element", K(ret));
            }
          }
          i++;
        } else {
          ret = OB_INVALID_ROWID;
        }
      } else {
        ret = OB_INVALID_ROWID;
      }
    }
  } else if (PK_ROWID_VERSION == get_version()) {
    ObPkValueIterator iter(*this);
    ObObj value;
    if (OB_FAIL(iter.init())) {
      COMMON_LOG(WARN, "failed to init primary key rowid iterator", K(ret), KPC(this));
    }
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      const int tmp_ret = iter.get_next_value(value);
      if (OB_ITER_END == tmp_ret) {
        is_iter_end = true;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        COMMON_LOG(WARN, "failed to get primary key rowid value", K(ret), KPC(this));
      } else if (OB_FAIL(pk_vals.push_back(value))) {
        COMMON_LOG(WARN, "failed to push back rowid value", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid rowid version", K(ret), "version", get_version());
  }
  return ret;
}

int ObURowIDData::get_tablet_id_for_heap_organized_table(ObTabletID &out_tablet_id) const
{
  int ret = OB_SUCCESS;
  uint8_t version = get_version();
  uint64_t tablet_id = 0;
  uint64_t auto_inc = 0;
  if (HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(parse_heap_organized_table_rowid(tablet_id, auto_inc))) {
      COMMON_LOG(WARN, "failed to parse rowid", K(ret));
    }
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(parse_ext_heap_organized_table_rowid(tablet_id, auto_inc))) {
      COMMON_LOG(WARN, "failed to parse rowid", K(ret));
    }
  } else {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid rowid", K(ret));
  }
  if (OB_SUCC(ret)) {
    out_tablet_id = ObTabletID(tablet_id);
  }
  return ret;
}

int ObURowIDData::get_rowkey_for_heap_organized_table(ObIArray<ObObj> &rowkey) const
{
  int ret = OB_SUCCESS;
  uint8_t version = get_version();
  uint64_t tablet_id = 0;
  uint64_t auto_inc = 0;
  if (HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(parse_heap_organized_table_rowid(tablet_id, auto_inc))) {
      COMMON_LOG(WARN, "failed to parse rowid", K(ret));
    }
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    if (OB_FAIL(parse_ext_heap_organized_table_rowid(tablet_id, auto_inc))) {
      COMMON_LOG(WARN, "failed to parse rowid", K(ret));
    }
  } else {
    ret = OB_INVALID_ROWID;
    COMMON_LOG(WARN, "invalid rowid", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObObj tmp_obj;
    tmp_obj.set_uint64(auto_inc);
    if (OB_FAIL(rowkey.push_back(tmp_obj))) {
      COMMON_LOG(WARN, "failed to push back element", K(ret));
    }
  }
  return ret;
}

int64_t ObURowIDData::needed_content_buf_size(const common::ObIArray<ObObj> &pk_vals, const int64_t version)
{
  int64_t buf_size = 0;
  if (HEAP_TABLE_ROWID_VERSION == version) {
    buf_size = HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE;
  } else if (EXT_HEAP_TABLE_ROWID_VERSION == version) {
    buf_size = EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE;
  } else {
    for (int i = 0; i < pk_vals.count(); i++) {
      buf_size += get_obj_size(pk_vals.at(i));
    }
    buf_size += PK_ROWID_VERSION == version
        ? PK_LOGICAL_ROWID_FIXED_SIZE : LOGICAL_ROWID_HEADER_SIZE;
  }
  return buf_size;
}

DEF_TO_STRING(ObURowIDData)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t tablet_id = 0;
  uint64_t auto_inc = 0;
  const int64_t version = get_version();
  J_OBJ_START();
  if (INVALID_ROWID_VERSION == version) {
    J_NAME("Content");
    J_COLON();
    if (OB_FAIL(hex_print(rowid_content_, rowid_len_, buf, buf_len, pos))) {
      // do nothing
    }
  } else if (HEAP_TABLE_ROWID_VERSION == version) {
    ObObj tmp_obj;
    if (OB_FAIL(parse_physical_rowid(tablet_id, auto_inc))) {
      J_KV("VERSION-FAILED", version);
    } else {
      J_KV("VERSION", version);
      J_COMMA();
      J_KV("TABLET ID", tablet_id);
      J_COMMA();
      J_NAME("Content");
      J_COLON();
      J_ARRAY_START();
      tmp_obj.set_uint64(auto_inc);
      BUF_PRINTO(tmp_obj);
      J_COMMA();
      J_ARRAY_END();
    }
 } else {
    J_KV("DBA LEN", rowid_content_[0]);
    J_COMMA();
    J_KV("VERSION", rowid_content_[1]);
    J_COMMA();
    J_NAME("Content");
    J_COLON();
    J_ARRAY_START();
    if (PK_ROWID_VERSION == version) {
      ObPkValueIterator iter(*this);
      ObObj value;
      bool is_first_value = true;
      if (OB_FAIL(iter.init())) {
        // do nothing
      }
      bool is_iter_end = false;
      while (OB_SUCC(ret) && !is_iter_end) {
        const int tmp_ret = iter.get_next_value(value);
        if (OB_ITER_END == tmp_ret) {
          is_iter_end = true;
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        } else {
          if (!is_first_value) {
            J_COMMA();
          }
          BUF_PRINTO(value);
          is_first_value = false;
        }
      }
    } else {
      int64_t rowid_pos = get_pk_content_offset();
      // skip print dba len
      if (OB_LIKELY(rowid_pos < rowid_len_)) {
        ObObj pk_val;
        for(; OB_SUCC(ret) && rowid_pos < rowid_len_;) {
          ObObjType obj_type = get_pk_type(rowid_pos);
          if (OB_UNLIKELY(rowid_pos >= rowid_len_)) {
            // do nothing
          } else if (OB_FAIL(get_pk_value(obj_type, rowid_pos, pk_val))) {
            // do nothing
          } else {
            BUF_PRINTO(pk_val);
            if (rowid_pos < rowid_len_) {
              J_COMMA();
            }
          }
        }
      }
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE_SIZE(ObURowIDData)
{
  int64_t len = 0;
  int32_t rowid_buf_len = static_cast<uint32_t>(rowid_len_);
  OB_UNIS_ADD_LEN(rowid_buf_len);
  len += rowid_buf_len;
  return len;
}

OB_DEF_SERIALIZE(ObURowIDData)
{
  int ret = OB_SUCCESS;
  int32_t rowid_buf_len = static_cast<int32_t>(rowid_len_);
  OB_UNIS_ENCODE(rowid_buf_len);
  if (OB_FAIL(ret)) { // do nothing
  } else if (pos + rowid_buf_len > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "not enough buffer", K(ret), K(rowid_buf_len), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, rowid_content_, rowid_buf_len);
    pos += rowid_buf_len;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObURowIDData)
{
  int ret = OB_SUCCESS;
  int32_t rowid_buf_len = 0;
  OB_UNIS_DECODE(rowid_buf_len);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (pos + rowid_buf_len > data_len) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dirty buffer", K(ret), K(pos), K(rowid_buf_len), K(data_len));
  } else {
    rowid_len_ = rowid_buf_len;
    rowid_content_ = (uint8_t *)buf + pos;
    pos += rowid_buf_len;
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase
