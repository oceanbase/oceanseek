/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB

#include "ob_json_print.h"
#include <algorithm>
#include <type_traits>
#include "ob_json_base.h"
#include "ob_json_bin.h"
#include "ob_json_bin_view.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/utility/ob_fast_convert.h"
#include "common/ob_smart_call.h"
#include "rpc/obmysql/ob_mysql_global.h"

namespace oceanbase {
namespace common {

// only used in json opaque type.
// Notice: varchar has different mapping in mysql 5.7 (var_string)and 8.0(varchar)
const obmysql::EMySQLFieldType ObJsonBaseUtil::OPAQUE_OB_TYPE_TO_MYSQL_TYPE[ObMaxType] =
{
  /* ObMinType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NULL,          /* ObNullType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObUTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObUSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObUMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObUInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObUInt64Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObUFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObUDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObUNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME,      /* ObDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP,     /* ObTimestampType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATE,          /* ObDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIME,          /* ObTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR,          /* ObYearType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING,    /* ObVarcharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR,       /* ObHexStringType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,       /* ObExtendType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObUnknownType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB,     /* ObTinyTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,          /* ObTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB,   /* ObMediumTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,     /* ObLongTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BIT,           /* ObBitType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObEnumType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObSetType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObEnumInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObSetInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,       /* ObTimestampTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE, /* ObTimestampLTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,                 /* ObTimestampNanoType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW,                            /* ObRawType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM,                    /* ObIntervalYMType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS,                    /* ObIntervalDSType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT,                   /* ObNumberFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2,                      /* ObNVarchar2Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR,                          /* ObNCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID,
  obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB,                          /* ObLobType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_JSON,                              /* ObJsonType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY,                          /* ObGeometryType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObUserDefinedSQLType, buf for xml we use long_blob type currently? */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,                        /* ObDecimalIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObCollectionSQLType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,                       /* reserved for ObMySQLDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,                       /* reserved for ObMySQLDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,                              /* ObRoaringBitmapType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,                         /* ObExtendBLobType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,                       /* ObVariantType */
  /* ObMaxType */
};

namespace {

template <typename Json>
constexpr bool is_bin_view()
{
  static_assert(std::is_same_v<Json, ObIJsonBase> || std::is_same_v<Json, ObJsonBinView>,
                "JSON printing supports ObIJsonBase and ObJsonBinView");
  return std::is_same_v<Json, ObJsonBinView>;
}

inline void get_print_data(const ObJsonBinView &node,
                           const char *&data, uint64_t &length)
{
  const ObString str = node.get_string();
  data = str.ptr();
  length = str.length();
}

inline void get_print_data(const ObIJsonBase &node,
                           const char *&data, uint64_t &length)
{
  data = node.get_data();
  length = node.get_data_length();
}

template <typename Integer>
inline int64_t format_integer(Integer value, char *buf)
{
  if constexpr (std::is_signed_v<Integer>) {
    return ObFastFormatInt::format_signed(value, buf);
  } else {
    return ObFastFormatInt::format_unsigned(value, buf);
  }
}

template <typename Integer>
inline int print_integer(ObJsonBuffer &buf, Integer value)
{
  char tmp[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
  const int64_t length = format_integer(value, tmp);
  return buf.append_unterminated(tmp, length);
}

static OB_INLINE bool needs_escape(char character)
{
  const unsigned char byte_value = static_cast<unsigned char>(character);
  return byte_value <= 0x1f || byte_value == '"' || byte_value == '\\';
}

static OB_INLINE int escape_character_impl(char c, ObJsonBuffer &j_buf)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append_unterminated("\\", 1))) {
    LOG_WARN("fail to append \"\\\" to j_buf", K(ret), K(c));
  } else {
    switch (c) {
      case '\b': {
        if (OB_FAIL(j_buf.append_unterminated("b", 1))) {
          LOG_WARN("fail to append 'b' to j_buf", K(ret));
        }
        break;
      }

      case '\t': {
        if (OB_FAIL(j_buf.append_unterminated("t", 1))) {
          LOG_WARN("fail to append 't' to j_buf", K(ret));
        }
        break;
      }

      case '\n': {
        if (OB_FAIL(j_buf.append_unterminated("n", 1))) {
          LOG_WARN("fail to append 'n' to j_buf", K(ret));
        }
        break;
      }

      case '\f': {
        if (OB_FAIL(j_buf.append_unterminated("f", 1))) {
          LOG_WARN("fail to append 'f' to j_buf", K(ret));
        }
        break;
      }

      case '\r': {
        if (OB_FAIL(j_buf.append_unterminated("r", 1))) {
          LOG_WARN("fail to append 'r' to j_buf", K(ret));
        }
        break;
      }

      case '"':
      case '\\': {
        char str[1] = {c};
        if (OB_FAIL(j_buf.append_unterminated(str, 1))) {
          LOG_WARN("fail to append c to j_buf", K(ret), K(c));
        }
        break;
      }

      default: {
        // Write other control chars as hex escapes.
        static char _dig_vec_lower[] = "0123456789abcdefghijklmnopqrstuvwxyz";
        char high[1] = {_dig_vec_lower[(c & 0xf0) >> 4]};
        char low[1] = {_dig_vec_lower[(c & 0x0f)]};
        if (OB_FAIL(j_buf.append_unterminated("u00", 3))) {
          LOG_WARN("fail to append \"u00\" to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append_unterminated(high, 1))) {
          LOG_WARN("fail to append four high bits to j_buf", K(ret));
        } else if (OB_FAIL(j_buf.append_unterminated(low, 1))) {
          LOG_WARN("fail to append four low bits to j_buf", K(ret));
        }
        break;
      }
    }
  }

  return ret;
}

static int add_double_quote_impl(ObJsonBuffer &j_buf, const char *cptr, uint64_t length)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(cptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param is null or length is 0", K(ret), KP(cptr), K(length));
  } else if (OB_FAIL(j_buf.reserve(length + 2))) {
    LOG_WARN("fail to reserve length for j_buf", K(ret), K(length));
  } else {
    const char *const end = cptr + length;
    const char *next_special = std::find_if(cptr, end, needs_escape);
    if (next_special == end) {
      const uint64_t old_length = j_buf.length();
      char *buf = j_buf.ptr() + old_length;
      buf[0] = '"';
      MEMCPY(buf + 1, cptr, length);
      buf[length + 1] = '"';
      if (OB_FAIL(j_buf.set_length_unterminated(old_length + length + 2))) {
        LOG_WARN("fail to set j_buf length", K(ret), K(old_length), K(length));
      }
    } else if (OB_FAIL(j_buf.append_unterminated("\"", 1))) {
      LOG_WARN("fail to append double quote to j_buf", K(ret), K(length));
    } else {
      while (true) {
        // Copy the plain text before the next escape.
        if (OB_FAIL(j_buf.append_unterminated(cptr, next_special - cptr))) {
          LOG_WARN("fail to append common segments to j_buf", K(ret), K(length),
                                                             K(next_special - cptr));
          break;
        }

        cptr = next_special;
        if (cptr == end) {
          break;
        }

        // Escape this character.
        if (OB_FAIL(escape_character_impl(*cptr++, j_buf))) {
          LOG_WARN("fail to escape character", K(ret), K(length), K(*(cptr - 1)));
          break;
        }
        next_special = std::find_if(cptr, end, needs_escape);
      };

      if (OB_SUCC(ret) && OB_FAIL(j_buf.append_unterminated("\"", 1))) {
        LOG_WARN("fail to append end double quote to j_buf", K(ret), K(length));
      }
    }
  }

  return ret;
}

static OB_INLINE int append_string_impl(ObJsonBuffer &buf, bool is_quoted,
                         const char *data, uint64_t length)
{
  return is_quoted ? add_double_quote_impl(buf, data, length)
                   : buf.append_unterminated(data, length);
}

static OB_INLINE int append_newline_and_indent_impl(ObJsonBuffer &buf, uint64_t level)
{
  int ret = OB_SUCCESS;
  const uint64_t indent_size = level << 1;
  const uint64_t append_size = indent_size + 1;
  ret = buf.reserve(append_size);
  if (OB_SUCC(ret)) {
    const uint64_t old_length = buf.length();
    char *out = buf.ptr() + old_length;
    out[0] = '\n';
    MEMSET(out + 1, ' ', indent_size);
    ret = buf.set_length_unterminated(old_length + append_size);
  }
  return ret;
}

static int append_decimal_dot_zero(ObJsonBuffer &j_buf, const char *start, uint64_t len)
{
  int ret = OB_SUCCESS;
  bool has_dot_or_e = false;
  for (uint64_t k = 0; k < len; ++k) {
    if (start[k] == '.' || start[k] == 'e') {
      has_dot_or_e = true;
      break;
    }
  }
  if (!has_dot_or_e) {
    ret = j_buf.append_unterminated(".0", 2);
  }
  return ret;
}

// gcvt-based formatting shared by J_DOUBLE/J_ODOUBLE and J_OFLOAT. Emits ".0"
// when the result has no '.' or 'e' so the literal stays a float, not an int.
static int print_floating_impl(ObJsonBuffer &j_buf, double val,
                               ob_gcvt_arg_type arg_type, int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(j_buf.reserve(buf_size + 1))) {
    LOG_WARN("fail to reserve for float", K(ret));
  } else {
    char *start = j_buf.ptr() + j_buf.length();
    uint64_t len = ob_gcvt(val, arg_type, buf_size, start, NULL);
    if (OB_FAIL(j_buf.set_length_unterminated(j_buf.length() + len))) {
      LOG_WARN("fail to set j_buf len", K(ret));
    } else if (OB_FAIL(append_decimal_dot_zero(j_buf, start, len))) {
      LOG_WARN("fail to append decimal dot zero", K(ret));
    }
  }
  return ret;
}

template <typename Json>
static int print_double_impl(const Json &node, ObJsonBuffer &buf)
{
  return print_floating_impl(buf, node.get_double(), ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                                   DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE);
}

template <typename Json>
static int print_float_impl(const Json &node, ObJsonBuffer &buf)
{
  return print_floating_impl(buf, node.get_float(), ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT,
                                   FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE);
}

template <typename Json>
static int print_decimal_impl(const Json &node, ObJsonBuffer &j_buf)
{
  INIT_SUCC(ret);
  int64_t pos = j_buf.length();

  if (OB_FAIL(j_buf.reserve(number::ObNumber::MAX_PRINTABLE_SIZE))) {
    LOG_WARN("fail to reserve memory for buf", K(ret));
  } else {
    ObScale scale = node.get_decimal_scale();
    number::ObNumber val = node.get_decimal_data();
    if (OB_FAIL(val.format_v2(j_buf.ptr(), j_buf.capacity(), pos, scale))) {
      LOG_WARN("fail to format decimal value", K(ret), K(val), K(j_buf.length()), K(pos));
    } else if (OB_FAIL(j_buf.set_length_unterminated(pos))) {
      LOG_WARN("fail to set buf length", K(ret), K(pos), K(j_buf.length()));
    }
  }

  return ret;
}

template <typename Json>
static int print_jtime_impl(const Json &node, ObJsonBuffer &j_buf, bool is_quoted)
{
  int ret = OB_SUCCESS;
  const ObJsonNodeType j_type = node.json_type();
  // Preserve JsonBase's opening quote even when time conversion fails.
  if (is_quoted) {
    ret = j_buf.append_unterminated("\"", 1, 0);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to append opening quote", K(ret));
    return ret;
  }
  ObDTMode tmode;
  if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type, tmode))) {
    LOG_WARN("fail to get dt mode", K(ret), K(j_type));
  } else {
    ObTime ob_time(tmode);
    if (OB_FAIL(node.get_obtime(ob_time))) {
      LOG_WARN("fail to get obtime", K(ret), K(j_type));
    } else {
      const int64_t tmp_buf_len = DATETIME_MAX_LENGTH + 1;
      char tmp_buf[tmp_buf_len] = {0};
      int64_t pos = 0;
      const int16_t print_scale = 6;
      if (OB_FAIL(ObTimeConverter::ob_time_to_str(ob_time, tmode, print_scale,
                                                   tmp_buf, tmp_buf_len, pos, true))) {
        LOG_WARN("ob_time_to_str fail", K(ret), K(j_type));
      } else {
        if (OB_FAIL(j_buf.append_unterminated(tmp_buf, static_cast<int32_t>(pos), 0))) {
          LOG_WARN("fail to append time str", K(ret));
        } else if (is_quoted && OB_FAIL(j_buf.append_unterminated("\"", 1, 0))) {
          LOG_WARN("fail to append quote", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename Json>
static int print_opaque_impl(const Json &node, ObJsonBuffer &j_buf, bool is_quoted)
{
  int ret = OB_SUCCESS;
  const char *data = nullptr;
  uint64_t data_length = 0;
  get_print_data(node, data, data_length);
  ObObjType f_type = node.field_type();
  if (OB_UNLIKELY(f_type >= ObMaxType)) {
    ret = OB_ERR_MAX_VALUE;
    LOG_WARN("invalid field type for opaque", K(ret), K(f_type));
  } else {
    const uint64_t need_len = ObBase64Encoder::needed_encoded_length(data_length);
    obmysql::EMySQLFieldType mysql_type = ObJsonBaseUtil::OPAQUE_OB_TYPE_TO_MYSQL_TYPE[f_type];
    char field_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
    int64_t field_len = ObFastFormatInt::format_unsigned(mysql_type, field_buf);
    ObArenaAllocator tmp_allocator;
    ObJsonBuffer base64_buf(&tmp_allocator);
    if (OB_FAIL(base64_buf.append("base64:type", 11, 0))) {
      LOG_WARN("fail to append base64:type", K(ret));
    } else if (OB_FAIL(base64_buf.append(field_buf, field_len, 0))) {
      LOG_WARN("fail to append field type", K(ret), K(f_type));
    } else if (OB_FAIL(base64_buf.append(":", 1, 0))) {
      LOG_WARN("fail to append colon", K(ret));
    } else if (OB_FAIL(base64_buf.reserve(need_len))) {
      LOG_WARN("fail to reserve for base64", K(ret), K(need_len));
    } else {
      int64_t pos = 0;
      const uint8_t *input = reinterpret_cast<const uint8_t *>(data);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("opaque data is null", K(ret));
      } else if (OB_FAIL(ObBase64Encoder::encode(input, data_length,
                                                 base64_buf.ptr() + base64_buf.length(),
                                                 need_len, pos))) {
        LOG_WARN("fail to base64 encode", K(ret), K(f_type), K(need_len));
      } else if (OB_FAIL(base64_buf.set_length(base64_buf.length() + need_len))) {
        LOG_WARN("fail to set base64 buf length", K(ret));
      } else if (is_quoted) {
        if (OB_FAIL(add_double_quote_impl(j_buf, base64_buf.ptr(), base64_buf.length()))) {
          LOG_WARN("fail to add double quote", K(ret));
        }
      } else {
        if (OB_FAIL(j_buf.append_unterminated(base64_buf.ptr(), base64_buf.length(), 0))) {
          LOG_WARN("fail to append base64 str", K(ret));
        }
      }
    }
    base64_buf.reset();
  }
  return ret;
}

static int append_comma_impl(ObJsonBuffer &j_buf, bool is_pretty)
{
  INIT_SUCC(ret);
  const bool append_space = !is_pretty && lib::is_mysql_mode();
  if (append_space) {
    if (OB_FAIL(j_buf.append_unterminated(", ", 2, 0))) {
      LOG_WARN("fail to append \", \" to buffer", K(ret), K(is_pretty));
    }
  } else {
    if (OB_FAIL(j_buf.append_unterminated(",", 1, 0))) {
      LOG_WARN("fail to append \",\" to buffer", K(ret), K(is_pretty));
    }
  }

  return ret;
}

static int print_array_impl(const ObIJsonBase &json, ObJsonBuffer &j_buf,
                            uint64_t depth, bool is_pretty)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append_unterminated("[", 1, 0))) {
    LOG_WARN("fail to append [", K(ret), K(depth), K(is_pretty));
  } else {
    uint64_t arr_size = json.element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < arr_size; ++i) {
      if (i > 0 && OB_FAIL(append_comma_impl(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else if (is_pretty && OB_FAIL(append_newline_and_indent_impl(j_buf, depth))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth));
      } else {
        ObIJsonBase *jb_ptr = NULL;
        ObJsonBin j_bin(json.get_allocator());
        jb_ptr = &j_bin;
        if (OB_FAIL(json.get_array_element(i, jb_ptr))) {
          LOG_WARN("fail to get array element", K(ret), K(depth), K(i));
        } else if (OB_FAIL(jb_ptr->print(j_buf, true, 0, is_pretty, depth))) {
          LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && arr_size > 0
          && OB_FAIL(append_newline_and_indent_impl(j_buf, depth - 1))) {
        LOG_WARN("fail to append newline and indent", K(ret), K(depth), K(arr_size));
      } else if (OB_FAIL(j_buf.append_unterminated("]", 1, 0))) {
        LOG_WARN("fail to append [", K(ret), K(depth), K(arr_size));
      }
    }
  }

  return ret;
}

static int append_colon_impl(ObJsonBuffer &j_buf, bool is_pretty)
{
  INIT_SUCC(ret);
  if (lib::is_oracle_mode() && is_pretty) {
    if (OB_FAIL(j_buf.append_unterminated(" : ", 3, 0))) {
      LOG_WARN("fail to append \" : \"", K(ret));
    }
  } else if (OB_FAIL(j_buf.append_unterminated(":", 1, 0))) {
    LOG_WARN("fail to append \":\"", K(ret));
  }
  return ret;
}

static int print_object_impl(const ObIJsonBase &json, ObJsonBuffer &j_buf,
                             uint64_t depth, bool is_pretty)
{
  INIT_SUCC(ret);

  if (OB_FAIL(j_buf.append_unterminated("{", 1, 0))) {
    LOG_WARN("fail to append '{' to buffer", K(ret));
  } else {
    bool is_first = true;
    uint64_t count = json.element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (!is_first && OB_FAIL(append_comma_impl(j_buf, is_pretty))) {
        LOG_WARN("fail to append comma", K(ret), K(is_pretty));
      } else {
        is_first = false;
        ObString key;
        if (OB_FAIL(json.get_key(i, key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (is_pretty && OB_FAIL(append_newline_and_indent_impl(j_buf, depth))) {
          LOG_WARN("fail to newline and indent", K(ret), K(depth), K(i), K(key));
        } else if (key.empty() && OB_FAIL(append_string_impl(j_buf, false, "\"\"", 2))) {
          LOG_WARN("fail to newline and indent", K(ret), K(depth), K(i), K(key));
        } else if (!key.empty() && OB_FAIL(append_string_impl(j_buf, true, key.ptr(), key.length()))) { // key
          LOG_WARN("fail to print string", K(ret), K(depth), K(i), K(key));
        } else if (OB_FAIL(append_colon_impl(j_buf, is_pretty))) {
          LOG_WARN("fail to append \":\"", K(ret), K(depth), K(i), K(key));
        } else if (lib::is_mysql_mode() && OB_FAIL(j_buf.append_unterminated(" ", 1, 0))) {
          LOG_WARN("fail to append \" \"", K(ret), K(depth), K(i), K(key));
        } else {
          ObIJsonBase *jb_ptr = NULL;
          ObJsonBin j_bin(json.get_allocator());
          jb_ptr = &j_bin;
          if (OB_FAIL(json.get_object_value(i, jb_ptr))) {
            LOG_WARN("fail to get object value", K(ret), K(i), K(is_pretty), K(depth));
          } else if (OB_FAIL(jb_ptr->print(j_buf, true, 0, is_pretty, depth))) { // value
            LOG_WARN("fail to print json value to string", K(ret), K(i), K(is_pretty), K(depth));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pretty && count > 0
          && OB_FAIL(append_newline_and_indent_impl(j_buf, depth - 1))) {
        LOG_WARN("fail to newline and indent", K(ret), K(is_pretty), K(depth));
      } else if (OB_FAIL(j_buf.append_unterminated("}", 1, 0))) {
        LOG_WARN("fail to append \"}\" to buffer", K(ret), K(is_pretty), K(depth));
      }
    }
  }

  return ret;
}

} // namespace

// Print children without adding a '\0' after each one.
template<typename Entry>
int ObJsonPrint::print_bin_container(const ObJsonBinView &view, ObJsonNodeType j_type,
                                ObJsonBuffer &j_buf, bool is_pretty,
                                uint64_t depth)
{
  int ret = OB_SUCCESS;
  ++depth;
  const bool is_object = (j_type == ObJsonNodeType::J_OBJECT);
  const bool is_mysql_mode = lib::is_mysql_mode();
  const uint64_t element_count = view.element_count();
  const char *open_bracket = is_object ? "{" : "[";
  const char *close_bracket = is_object ? "}" : "]";
  // Walk the entry tables in order.
  const char *keys = view.data_ + view.key_offset_start_;
  const char *values = view.data_ + view.value_offset_start_;
  if (OB_FAIL(j_buf.append_unterminated(open_bracket, 1, 0))) {
    LOG_WARN("fail to append open bracket", K(ret), K(j_type));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
    if (i > 0 && OB_FAIL(!is_pretty && is_mysql_mode
                            ? j_buf.append_unterminated(", ", 2, 0)
                            : j_buf.append_unterminated(",", 1, 0))) {
      LOG_WARN("fail to append comma", K(ret));
    } else if (is_pretty
               && OB_FAIL(append_newline_and_indent_impl(j_buf, depth))) {
      LOG_WARN("fail to append indent", K(ret));
    } else {
      ObString key;
      if (is_object) {
        const uint64_t key_offset = ReadVar<Entry>::read(keys);
        const uint64_t key_length = ReadVar<Entry>::read(keys + sizeof(Entry));
        key.assign_ptr(view.data_ + key_offset, static_cast<int32_t>(key_length));
        keys += 2 * sizeof(Entry);
      }
      const uint64_t value_offset = ReadVar<Entry>::read(values);
      const uint8_t value_type = static_cast<uint8_t>(values[sizeof(Entry)]);
      values += sizeof(Entry) + OB_JSON_BIN_VALUE_TYPE_LEN;
      const bool is_inline = OB_JSON_TYPE_IS_INLINE(value_type);
      const ObJBVerType raw_type = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type));
      const bool inline_scalar = is_inline
                                 && (raw_type == J_INT_V0 || raw_type == J_UINT_V0
                                 || raw_type == J_BOOLEAN_V0 || raw_type == J_NULL_V0);
      // Check the actual prefix too; other types keep the old parser.
      const bool string_scalar = !is_inline
                                 && raw_type == J_STRING_V0
                                 && value_offset < view.length_
                                 && static_cast<uint8_t>(view.data_[value_offset]) == value_type;
      ObString string_value;
      ObJsonBinView child;
      if (string_scalar) {
        int64_t pos = static_cast<int64_t>(value_offset) + 1;
        int64_t size = 0;
        if (OB_FAIL(serialization::decode_vi64(view.data_, view.length_, pos, &size))) {
          LOG_WARN("fail to read string length", K(ret), K(i));
        } else if (OB_UNLIKELY(pos < 0 || size < 0 || size > static_cast<int64_t>(view.length_) - pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("string data out of bounds", K(ret), K(pos), K(size), K(view.length_));
        } else {
          string_value.assign_ptr(view.data_ + pos, static_cast<int32_t>(size));
        }
      } else if (!inline_scalar) {
        ret = is_inline
              ? view.populate_inline_child(value_offset, value_type, static_cast<int>(i), child)
              : view.populate_child(value_offset, value_type, child);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to read child", K(ret), K(i), K(value_type));
      } else {
        if (is_object) {
          if (key.empty()
              ? OB_FAIL(j_buf.append_unterminated("\"\"", 2, 0))
              : OB_FAIL(append_string_impl(j_buf, true, key.ptr(), key.length()))) {
            LOG_WARN("print key fail", K(ret), K(i));
          } else if (OB_FAIL(is_mysql_mode ? j_buf.append_unterminated(": ", 2, 0)
                                           : (is_pretty ? j_buf.append_unterminated(" : ", 3, 0)
                                                       : j_buf.append_unterminated(":", 1, 0)))) {
            LOG_WARN("fail to append colon", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (inline_scalar) {
            switch (raw_type) {
              case J_INT_V0:
              case J_UINT_V0: {
                using SignedEntry = typename std::make_signed<Entry>::type;
                ret = raw_type == J_INT_V0
                    ? print_integer(
                        j_buf, static_cast<int64_t>(static_cast<SignedEntry>(value_offset)))
                    : print_integer(j_buf, value_offset);
                break;
              }
              case J_BOOLEAN_V0:
                ret = value_offset ? j_buf.append_unterminated("true", 4, 0)
                                   : j_buf.append_unterminated("false", 5, 0);
                break;
              case J_NULL_V0:
                ret = j_buf.append_unterminated("null", 4, 0);
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                break;
            }
          } else if (string_scalar) {
            ret = string_value.empty()
                  ? j_buf.append_unterminated("\"\"", 2, 0)
                  : append_string_impl(j_buf, true, string_value.ptr(), string_value.length());
          } else {
            ret = print_impl(child, j_buf, true, is_pretty, depth);
          }
          if (OB_FAIL(ret)) {
            LOG_WARN("print child fail", K(ret), K(i), K(value_type));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_pretty && element_count > 0
        && OB_FAIL(append_newline_and_indent_impl(j_buf, depth - 1))) {
      LOG_WARN("fail to append indent", K(ret));
    } else if (OB_FAIL(j_buf.append_unterminated(close_bracket, 1, 0))) {
      LOG_WARN("fail to append close bracket", K(ret), K(j_type));
    }
  }
  return ret;
}

// One type dispatch for both representations. BinView children stay on this
// internal path so that only the public entry point terminates continuous output.
template <typename Json>
int ObJsonPrint::print_impl(const Json &json, ObJsonBuffer &buf, bool is_quoted,
                           bool is_pretty, uint64_t depth)
{
  int ret = OB_SUCCESS;
  const ObJsonNodeType j_type = json.json_type();
  switch (j_type) {
    case ObJsonNodeType::J_NULL: {
      if constexpr (is_bin_view<Json>()) {
        ret = buf.append_unterminated("null", 4);
      } else if (json.is_real_json_null(&json)) {
        ret = buf.append_unterminated("null", 4);
      } else {
        ret = buf.append_unterminated("", 0);
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      ret = json.get_boolean() ? buf.append_unterminated("true", 4)
                               : buf.append_unterminated("false", 5);
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      ret = print_integer(buf, json.get_int());
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      ret = print_integer(buf, json.get_uint());
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      ret = print_double_impl(json, buf);
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      ret = print_float_impl(json, buf);
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      ret = print_decimal_impl(json, buf);
      break;
    }
    case ObJsonNodeType::J_STRING:
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH: {
      const char *data = nullptr;
      uint64_t length = 0;
      get_print_data(json, data, length);
      if (is_quoted && length == 0) {
        ret = buf.append_unterminated("\"\"", 2);
      } else if (OB_ISNULL(data) && length != 0) {
        ret = OB_ERR_NULL_VALUE;
      } else {
        ret = append_string_impl(buf, is_quoted, data, length);
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_MYSQL_DATETIME:
    case ObJsonNodeType::J_MYSQL_DATE:
    case ObJsonNodeType::J_ORACLEDATE:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      ret = print_jtime_impl(json, buf, is_quoted);
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      ret = print_opaque_impl(json, buf, is_quoted);
      break;
    }
    case ObJsonNodeType::J_ARRAY:
    case ObJsonNodeType::J_SEMI_HETE_COL:
    case ObJsonNodeType::J_OBJECT: {
      if constexpr (is_bin_view<Json>()) {
        // Select entry width once per container, preserving sequential reads.
        switch (json.entry_var_type_) {
          case JBLS_UINT8:
            ret = SMART_CALL(print_bin_container<uint8_t>(json, j_type, buf, is_pretty, depth));
            break;
          case JBLS_UINT16:
            ret = SMART_CALL(print_bin_container<uint16_t>(json, j_type, buf, is_pretty, depth));
            break;
          case JBLS_UINT32:
            ret = SMART_CALL(print_bin_container<uint32_t>(json, j_type, buf, is_pretty, depth));
            break;
          case JBLS_UINT64:
            ret = SMART_CALL(print_bin_container<uint64_t>(json, j_type, buf, is_pretty, depth));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            break;
        }
      } else {
        ++depth;
        if (j_type == ObJsonNodeType::J_OBJECT) {
          ret = SMART_CALL(print_object_impl(json, buf, depth, is_pretty));
        } else {
          ret = SMART_CALL(print_array_impl(json, buf, depth, is_pretty));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to print json value", K(ret), K(j_type), K(is_quoted), K(is_pretty), K(depth));
  }
  return ret;
}

template <typename Json>
int ObJsonPrint::print(const Json &json, ObJsonBuffer &buf, bool is_quoted,
                      uint64_t reserve_len, bool is_pretty, uint64_t depth)
{
  int ret = OB_SUCCESS;
  if (reserve_len > 0 && OB_FAIL(buf.reserve(reserve_len * 1.2))) {
    LOG_WARN("failed to reserve json buffer", K(ret), K(reserve_len));
  } else {
    ret = print_impl(json, buf, is_quoted, is_pretty, depth);
  }
  // Also terminate partial output on errors, retaining the original print error.
  const int terminate_ret = buf.set_length(buf.length());
  if (OB_SUCC(ret)) {
    ret = terminate_ret;
  }
  return ret;
}

template int ObJsonPrint::print<ObIJsonBase>(const ObIJsonBase &, ObJsonBuffer &, bool,
                                           uint64_t, bool, uint64_t);
template int ObJsonPrint::print<ObJsonBinView>(const ObJsonBinView &, ObJsonBuffer &, bool,
                                             uint64_t, bool, uint64_t);

int ObJsonBaseUtil::append_comma(ObJsonBuffer &j_buf, bool is_pretty)
{
  const int ret = append_comma_impl(j_buf, is_pretty);
  const int terminate_ret = j_buf.set_length(j_buf.length());
  return OB_SUCCESS == ret ? terminate_ret : ret;
}

int ObJsonBaseUtil::append_newline_and_indent(ObJsonBuffer &j_buf, uint64_t level)
{
  const int ret = append_newline_and_indent_impl(j_buf, level);
  const int terminate_ret = j_buf.set_length(j_buf.length());
  return OB_SUCCESS == ret ? terminate_ret : ret;
}

int ObJsonBaseUtil::escape_character(char c, ObJsonBuffer &j_buf)
{
  const int ret = escape_character_impl(c, j_buf);
  const int terminate_ret = j_buf.set_length(j_buf.length());
  return OB_SUCCESS == ret ? terminate_ret : ret;
}

int ObJsonBaseUtil::add_double_quote(ObJsonBuffer &j_buf, const char *cptr, uint64_t length)
{
  const int ret = add_double_quote_impl(j_buf, cptr, length);
  const int terminate_ret = j_buf.set_length(j_buf.length());
  return OB_SUCCESS == ret ? terminate_ret : ret;
}

int ObJsonBaseUtil::append_string(ObJsonBuffer &j_buf, bool is_quoted,
                                  const char *data, uint64_t length)
{
  const int ret = append_string_impl(j_buf, is_quoted, data, length);
  const int terminate_ret = j_buf.set_length(j_buf.length());
  return OB_SUCCESS == ret ? terminate_ret : ret;
}


} // namespace common
} // namespace oceanbase
