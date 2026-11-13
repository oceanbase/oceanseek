/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_

#include "lib/ob_define.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_common.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_order_perserving_encoder.h"
#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
class ObJsonWrapper;
}

namespace share
{

enum ObSearchIndexKeyIndex
{
  SEARCH_INDEX_COL_IDX         = 0,
  SEARCH_INDEX_PATH            = 1,
  SEARCH_INDEX_VALUE           = 2,
  SEARCH_INDEX_DOC_ID          = 3,
  SEARCH_INDEX_KEY_CNT         = 4,  // posting-key column count, and non-nested generated column count
  SEARCH_INDEX_NESTED_KEY      = 4,  // scalar nested-key input column (5th generated column)
  SEARCH_INDEX_KEY_CNT_WITH_NK = 5,  // generated column count when nested_key is present
};

// The synchronous search-index rowkey is limited to 16KB and contains PATH, VALUE and nested_key.
// nested_key (80B) is reserved unconditionally so the value-length budget is uniform: the
// scalar nested-key column is only added when NESTED_PATHS is configured, but VALUE always leaves
// room for it. Async search uses the same generated VALUE budget, then folds scalar nested keys
// into a non-rowkey posting value before persistence.
//
// Maximum length for the encoded JSON path in search index.
// This includes the encoded path expression used to locate values in JSON documents.
// If the encoded path length exceeds this limit (2KB), DML operations (INSERT/UPDATE/DELETE)
// or index creation will fail with an error.
static const int64_t SEARCH_INDEX_PATH_LENGTH = 2 * 1024; // 2KB
//
// nested_key column length: 20 levels * 4 bytes/level = 80 bytes.
// Reserved in the generated search key budget whether or not a nested_key column is present.
static const int64_t SEARCH_INDEX_NESTED_KEY_LENGTH = 80; // 20 * 4B
//
// Maximum length in bytes for the indexed value in search index.
// This is the actual content extracted from JSON documents at the specified path.
// If the value length exceeds this limit, the value will be silently truncated to fit within
// the limit. No error will be reported in this case. 80B is reserved for the nested_key column
// so the rowkey stays within 16KB regardless of NESTED_PATHS config.
static const int64_t SEARCH_INDEX_VALUE_LENGTH = 14 * 1024 - SEARCH_INDEX_NESTED_KEY_LENGTH; // 14256B

// Search index path encoder
class ObSearchIndexPathEncoder
{
public:
  enum JsonEncodeType : uint8_t {
    ESCAPE_MARKER       = 0x00,
    LOWER_BOUND         = 0x01,

    // mysql types
    TYPE_NULL           = 0x10,  // J_NULL
    TYPE_DECIMAL        = 0x14,  // J_DECIMAL/ J_INT/ J_UINT/ J_DOUBLE
    TYPE_STRING         = 0x18,  // J_STRING
    TYPE_OBJECT         = 0x1c,  // J_OBJECT
    TYPE_ARRAY          = 0x20,  // J_ARRAY
    TYPE_BOOLEAN        = 0x24,  // J_BOOLEAN
    TYPE_DATE           = 0x28,  // J_DATE
    TYPE_TIME           = 0x2c,  // J_TIME
    TYPE_DATETIME       = 0x30,  // J_DATETIME/ J_TIMESTAMP
    TYPE_OPAQUE         = 0x34,  // J_OPAQUE
    TYPE_MYSQL_DATE     = 0x38,  // J_MYSQL_DATE
    TYPE_MYSQL_DATETIME = 0x3c,  // J_MYSQL_DATETIME

    UPPER_BOUND         = 0xE0,

    ESCAPED_NULL        = 0xF0   // ESCAPED_NULL is the escaped version of ESCAPE_MARKER
  };

  struct JsonPathItem
  {
    JsonPathItem() : type(ESCAPE_MARKER), path() {}
    JsonPathItem(JsonEncodeType type, ObString path) : type(type), path(path) {}
    bool is_array_path() const { return type == TYPE_ARRAY; }

    TO_STRING_KV(K(type), K(path));

    JsonEncodeType type;
    ObString path;
  };

  static JsonPathItem make_object_path(ObString path) { return JsonPathItem(TYPE_OBJECT, path); }
  static JsonPathItem make_array_path() { return JsonPathItem(TYPE_ARRAY, ObString()); }

private:
  static int64_t calc_prefix_path_size(const ObIArray<JsonPathItem> &path_items,
                                       bool &has_escaped_null,
                                       const bool preserve_array_levels = false);
  static int encode_prefix_path(const ObIArray<JsonPathItem> &path_items,
                                const bool has_escaped_null,
                                uint8_t *mem, int64_t &pos,
                                const bool preserve_array_levels = false);
  static int build_path(ObIAllocator &allocator,
                        const ObString &prefix,
                        const ObString &suffix,
                        ObString &path);

public:
  // encode json type as a part of path
  static int encode_type(common::ObJsonNodeType json_type, uint8_t &encode_type);
  // path = basic_path + type
  static int encode_path_prefix(ObIAllocator &allocator, const ObIArray<JsonPathItem> &path_items,
                                ObString &path);
  static int encode_path(ObIAllocator &allocator, const ObIArray<JsonPathItem> &path_items,
                         const ObJsonNodeType scalar_type, ObString &path);
  // Fulltext path retains every array level and omits the scalar type suffix.
  static int encode_text_path(ObIAllocator &allocator, const ObIArray<JsonPathItem> &path_items,
                              ObString &path);
  static int encode_pick_path(const ObItemType pick_type, uint8_t &enc_type);

  // build path for query range.
  static int generate_upper_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_lower_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_array_path(ObIAllocator &allocator, const ObString &prefix, ObString &path);
  static int generate_multi_array_path(ObIAllocator &allocator, const ObString &prefix,
                                       ObString &path);
  static int generate_pick_path(ObIAllocator &allocator, const ObString &prefix,
                                const ObItemType pick_type, ObString &path);
  static int generate_enc_path(ObIAllocator &allocator, const ObString &prefix,
                               const uint8_t enc_type, ObString &path);

  // encode scalar path.
  // Encode search index path from path_prefix and json_base
  // Result format: path_prefix + ESCAPE_MARKER + encode_type
  static int encode_path(common::ObIAllocator &allocator,
                         const common::ObString &path_prefix,
                         const common::ObIJsonBase *j_base,
                         common::ObString &result);

  // Enocde search index path with array type (for matching array elements)
  static int encode_array_path(common::ObIAllocator &allocator,
                               const common::ObString &path_prefix,
                               const common::ObIJsonBase *j_base,
                               common::ObString &result);

  // Encode search index path with multi-dimension array type
  // (for matching multi-dimension array elements)
  static int encode_multi_array_path(common::ObIAllocator &allocator,
                                     const common::ObString &path_prefix,
                                     const common::ObIJsonBase *j_base,
                                     common::ObString &result);
};

// Search index value encoder
class ObSearchIndexValueEncoder {
public:
  template<typename T>
  static int encode_int(ObIAllocator &allocator, T int_value, ObString &value);
  template<typename T>
  static int encode_uint(ObIAllocator &allocator, T uint_value, ObString &value);
  static int encode_double(ObIAllocator &allocator, double double_value, ObString &value);
  static int encode_float(ObIAllocator &allocator, float float_value, ObString &value);
  static int encode_number(ObIAllocator &allocator, const ObNumber &number, ObString &value);
  static int encode_decimal_int(ObIAllocator &allocator,
                                const ObDecimalInt *decint,
                                const int32_t int_bytes,
                                ObString &value);
  static int encode_int32(ObIAllocator &allocator, int32_t int32_value, ObString &value);
  static int encode_uint8(ObIAllocator &allocator, uint8_t uint8_value, ObString &value);
  static int encode_string(ObIAllocator &allocator, const ObString &string_value,
                           ObString &value, ObEncParam &enc_param,
                           const bool is_allow_truncate = true);
  static int init_string_enc_param(const ObObjType obj_type,
                                   const ObCollationType cs_type,
                                   const ObSQLMode sql_mode,
                                   const bool is_null_first,
                                   const bool is_asc,
                                   ObEncParam &enc_param);
  static int encode_json_scalar(ObIAllocator &allocator, const ObJsonWrapper &json,
                                ObString &value, ObEncParam &enc_param);
  // Compatibility entry for callers that still hold a legacy JSON node.
  static int encode_json_scalar(ObIAllocator &allocator, const ObIJsonBase *j_base,
                                ObString &value, ObEncParam &enc_param);
  static bool is_json_scalar(const ObIJsonBase *j_base) {
    return OB_ISNULL(j_base) ? false : j_base->is_json_scalar(j_base->json_type());
  }
  static int init_json_string_enc_param(ObEncParam &enc_param);

  // Encode search index value from scalar datum (non-JSON type)
  static int encode_value(common::ObIAllocator &allocator, const ObDatum &datum,
                          const common::ObObjMeta &obj_meta, ObString &value);

  // Encode search index value from scalar object (non-JSON type)
  static int encode_value(common::ObIAllocator &allocator,
                          const common::ObObj &obj,
                          common::ObString &value);

  // Encode search index value from JSON scalar
  static int encode_value(common::ObIAllocator &allocator,
                          const common::ObJsonWrapper &json,
                          common::ObString &value);
  // Compatibility entry; delegates to the wrapper encoder.
  static int encode_value(common::ObIAllocator &allocator,
                          const common::ObIJsonBase *j_base,
                          common::ObString &value);

  // FULLTEXT grouping and encoded keys must agree on token equality. Only allow
  // implemented encodings and the UTF16/UTF16LE general_ci/bin equivalents.
  static bool is_supported_search_index_collation(const common::ObCollationType cs_type);

  // Legacy VALUE normalization maps every non-binary UTF16 collation to general_ci.
  // FULLTEXT support checks is_supported_search_index_collation(): UCA
  // collations do not preserve their equality/order under this mapping.
  static int normalize_string_for_encoding(common::ObIAllocator &allocator,
                                           common::ObString &str_value,
                                           common::ObCollationType &cs_type);

  static bool string_safety_to_compare(const ObString &str);
  static bool string_column_may_truncate(const ObObjMeta &column_meta,
                                         const ObLength column_length);

  // Encodes the search index value with the literal's own obj_meta at query time
  // (see ob_expr_search_index_inner.cpp::calc_search_index_inner_value), but with
  // the column's obj_meta at insert time (see ob_search_index_generator.cpp). If
  // the literal's encoder method differs from the column's, the byte strings diverge
  // and the search index lookup silently misses. The methods are grouped by the
  // underlying encoder function called in encode_value() so that any two types
  // mapped to the same method produce identical byte strings for the same value.
  enum SearchIndexEncodeMethod {
    SEARCH_INDEX_ENCODE_METHOD_INVALID = 0,
    SEARCH_INDEX_ENCODE_METHOD_INT_SIGNED,    // encode_int (64-bit, sign-bit XOR)
    SEARCH_INDEX_ENCODE_METHOD_INT_UNSIGNED,  // encode_uint (64-bit, no XOR)
    SEARCH_INDEX_ENCODE_METHOD_INT32,         // encode_int32 (32-bit, sign-bit XOR)
    SEARCH_INDEX_ENCODE_METHOD_UINT8,         // encode_uint8 (8-bit)
    SEARCH_INDEX_ENCODE_METHOD_FLOAT,         // encode_float
    SEARCH_INDEX_ENCODE_METHOD_DOUBLE,        // encode_double
    SEARCH_INDEX_ENCODE_METHOD_STRING,        // encode_string
    SEARCH_INDEX_ENCODE_METHOD_NUMBER,        // encode_number
    SEARCH_INDEX_ENCODE_METHOD_DECIMAL_INT,   // encode_decimal_int
    SEARCH_INDEX_ENCODE_METHOD_JSON,          // encode_json_scalar
    SEARCH_INDEX_ENCODE_METHOD_UNKNOWN,
  };
  static SearchIndexEncodeMethod get_search_index_encode_method(common::ObObjType obj_type);
  static bool is_search_index_encode_compatible(common::ObObjType col_type, common::ObObjType val_type);
};

// Encoder for nested_key column: encodes a stack of per-level array indices as
// big-endian 4-byte integers concatenated in outer-to-inner order.
// Example: comments[0].replies[1] -> 0x00000000 0x00000001 (8 bytes)
class ObSearchIndexNestedKeyEncoder {
public:
  static const int64_t BYTES_PER_LEVEL = 4;
  static const int64_t MAX_DEPTH = 20;

  // Encodes nested_key_stack into a big-endian byte string stored in out.
  // stack.count() must be <= MAX_DEPTH.
  static int encode(common::ObIAllocator &allocator,
                    const common::ObIArray<uint32_t> &stack,
                    common::ObString &out);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_ENCODER_H_
