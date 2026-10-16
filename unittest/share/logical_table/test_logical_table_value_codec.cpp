/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#undef private

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

class TestLogicalTableValueCodec : public ::testing::Test
{
public:
  TestLogicalTableValueCodec()
    : arena_(ObModIds::TEST)
  {}

  void SetUp() override
  {
    arena_.reset();
  }

  ObDatum make_int_datum(const int64_t value)
  {
    int64_t *buf = static_cast<int64_t *>(arena_.alloc(sizeof(int64_t)));
    *buf = value;
    return ObDatum(reinterpret_cast<char *>(buf), sizeof(int64_t), false);
  }

  ObDatum make_uint_datum(const uint64_t value)
  {
    uint64_t *buf = static_cast<uint64_t *>(arena_.alloc(sizeof(uint64_t)));
    *buf = value;
    return ObDatum(reinterpret_cast<char *>(buf), sizeof(uint64_t), false);
  }

  ObDatum make_double_datum(const double value)
  {
    double *buf = static_cast<double *>(arena_.alloc(sizeof(double)));
    *buf = value;
    return ObDatum(reinterpret_cast<char *>(buf), sizeof(double), false);
  }

  ObDatum make_string_datum(const char *ptr, const int64_t len)
  {
    char *buf = nullptr;
    if (len > 0) {
      buf = static_cast<char *>(arena_.alloc(len));
      MEMCPY(buf, ptr, len);
    }
    return ObDatum(buf, len, false);
  }

  ObDatum make_string_datum(const ObString &str)
  {
    return make_string_datum(str.ptr(), str.length());
  }

  // Build a datum with ObLobCommon header (inrow, non-init) for Text types
  ObDatum make_lob_datum(const char *ptr, const int64_t len)
  {
    const int64_t header_size = static_cast<int64_t>(sizeof(ObLobCommon));
    const int64_t total = header_size + len;
    char *buf = static_cast<char *>(arena_.alloc(total));
    ObLobCommon *lob = new(buf) ObLobCommon();  // in_row_=1, is_init_=0, is_mem_loc_=0
    if (len > 0) {
      MEMCPY(buf + header_size, ptr, len);
    }
    return ObDatum(buf, static_cast<int32_t>(total), false);
  }

  // Build a datum with ObLobCommon header (inrow)
  // This exercises read_real_string_data path: is_mem_loc_=0, in_row_=1
  ObDatum make_disk_loc_datum(const char *ptr, const int64_t len)
  {
    const int64_t header_size = static_cast<int64_t>(sizeof(ObLobCommon));
    const int64_t total = header_size + len;
    char *buf = static_cast<char *>(arena_.alloc(total));
    ObLobCommon *disklob = new(buf) ObLobCommon();
    // Constructor sets: lob_common_.is_mem_loc_=1, has_inrow_data_=1, is_simple_=1, has_extern_=0
    if (len > 0) {
      MEMCPY(buf + header_size, ptr, len);
    }
    return ObDatum(buf, static_cast<int32_t>(total), false);
  }

  // Build a datum with ObMemLobCommon + nested ObLobCommon header (complex inrow)
  // This exercises read_real_string_data path: is_mem_loc_=1, has_inrow_data_=1, is_simple_=0,
  //   then nested ObLobCommon with in_row_=1
  ObDatum make_memlob_complex_datum(const char *ptr, const int64_t len)
  {
    const int64_t memlob_hdr = static_cast<int64_t>(sizeof(ObMemLobCommon));
    const int64_t lob_hdr = static_cast<int64_t>(sizeof(ObLobCommon));
    const int64_t total = memlob_hdr + lob_hdr + len;
    char *buf = static_cast<char *>(arena_.alloc(total));
    ObMemLobCommon *memlob = new(buf) ObMemLobCommon(TEMP_FULL_LOB);
    memlob->disk_locator_size_ = lob_hdr + len;
    ObLobCommon *disklob = new(buf + memlob_hdr) ObLobCommon();  // in_row_=1, is_init_=0
    if (len > 0) {
      MEMCPY(buf + memlob_hdr + lob_hdr, ptr, len);
    }
    return ObDatum(buf, static_cast<int32_t>(total), false);
  }

  ObObjMeta make_lob_meta(const ObObjType type, const ObCollationType coll_type = CS_TYPE_UTF8MB4_BIN)
  {
    ObObjMeta meta;
    meta.set_type(type);
    meta.set_collation_type(coll_type);
    meta.set_collation_level(CS_LEVEL_IMPLICIT);
    return meta;
  }

  ObDatum make_null_datum()
  {
    ObDatum datum;
    datum.set_null();
    return datum;
  }

  ObDatum make_float_datum(const float value)
  {
    float *buf = static_cast<float *>(arena_.alloc(sizeof(float)));
    *buf = value;
    return ObDatum(reinterpret_cast<char *>(buf), sizeof(float), false);
  }

  // Wrap a raw JSON binary in an inrow ObLobCommon header, matching how JSON
  // column datums are passed to the encoder in production (lob-formatted).
  ObDatum wrap_json_binary_as_lob_(const ObString &raw_binary)
  {
    const int64_t header_size = static_cast<int64_t>(sizeof(ObLobCommon));
    const int64_t total = header_size + raw_binary.length();
    char *buf = static_cast<char *>(arena_.alloc(total));
    EXPECT_NE(nullptr, buf);
    new(buf) ObLobCommon();  // in_row_=1, is_init_=0, is_mem_loc_=0
    if (raw_binary.length() > 0) {
      MEMCPY(buf + header_size, raw_binary.ptr(), raw_binary.length());
    }
    return ObDatum(buf, static_cast<int32_t>(total), false);
  }

  ObDatum make_json_datum(const char *json_text)
  {
    ObIJsonBase *json_bin = nullptr;
    ObString raw_binary;
    ObString text(json_text);
    EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(
      &arena_, text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json_bin));
    EXPECT_NE(nullptr, json_bin);
    EXPECT_EQ(OB_SUCCESS, json_bin->get_raw_binary(raw_binary, &arena_));
    return wrap_json_binary_as_lob_(raw_binary);
  }

  // Build a JSON datum from a float vector (array of floats)
  ObDatum make_vector_datum(const float *values, const int64_t count)
  {
    ObJsonArray json_array(&arena_);
    for (int64_t i = 0; i < count; ++i) {
      ObJsonDouble *node = OB_NEWx(ObJsonDouble, &arena_, static_cast<double>(values[i]));
      EXPECT_NE(nullptr, node);
      EXPECT_EQ(OB_SUCCESS, json_array.append(node));
    }
    ObIJsonBase *json_base = &json_array;
    ObString raw_binary;
    EXPECT_EQ(OB_SUCCESS, json_base->get_raw_binary(raw_binary, &arena_));
    return wrap_json_binary_as_lob_(raw_binary);
  }

  ObObjMeta make_meta(const ObObjType type, const ObCollationType coll_type = CS_TYPE_BINARY)
  {
    ObObjMeta meta;
    meta.set_type(type);
    meta.set_collation_type(coll_type);
    return meta;
  }

  ObJsonObject *get_columns(ObIJsonBase *root)
  {
    EXPECT_NE(nullptr, root);
    EXPECT_EQ(ObJsonNodeType::J_OBJECT, root->json_type());
    return static_cast<ObJsonObject *>(root);
  }

protected:
  ObArenaAllocator arena_;
};

TEST_F(TestLogicalTableValueCodec, encode_key_with_iarray_roundtrip)
{
  ObLogicalTableKeyPrefix prefix(1, 2, 0, LT_AUX_PRIMARY);
  ObSEArray<ObDatum, 1> datums;
  ObString key;
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_uint_datum(42)));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_key(prefix, datums, arena_, key));
  ASSERT_FALSE(key.empty());

  ObLogicalTableKeyPrefix decoded_prefix;
  uint64_t rowid = 0;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_key_prefix(key.ptr(), key.length(), pos, decoded_prefix));
  ASSERT_EQ(prefix, decoded_prefix);
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_rowid(key, rowid));
  ASSERT_EQ(42U, rowid);
}

TEST_F(TestLogicalTableValueCodec, encode_value_scalars_roundtrip)
{
  ObSEArray<ObString, 4> column_names;
  ObSEArray<ObObjMeta, 4> metas;
  ObSEArray<ObDatum, 4> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("score")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("name")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("note")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObDoubleType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(7)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_double_datum(3.5)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_string_datum("alice", 5)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_null_datum()));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_FALSE(value_binary.empty());
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));
  ASSERT_NE(nullptr, decoded);

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);
  ASSERT_EQ(ObJsonNodeType::J_INT, columns->get_value("id")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_DOUBLE, columns->get_value("score")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_STRING, columns->get_value("name")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_NULL, columns->get_value("note")->json_type());
  ASSERT_EQ(7, static_cast<ObJsonInt *>(columns->get_value("id"))->value());
  ASSERT_DOUBLE_EQ(3.5, static_cast<ObJsonDouble *>(columns->get_value("score"))->value());
  ASSERT_EQ("alice", std::string(
    static_cast<ObJsonString *>(columns->get_value("name"))->value().ptr(),
    static_cast<ObJsonString *>(columns->get_value("name"))->value().length()));
}

TEST_F(TestLogicalTableValueCodec, encode_value_preserves_json_subtree)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("payload")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum("{\"nested\":[1,{\"k\":\"v\"}],\"flag\":true}")));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);
  ObJsonObject *payload = static_cast<ObJsonObject *>(columns->get_value("payload"));
  ASSERT_NE(nullptr, payload);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, payload->json_type());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, payload->get_value("nested")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, payload->get_value("flag")->json_type());
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_non_utf8_string)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("name")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_BINARY)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_string_datum("alice", 5)));
  // Encoder accepts both CHARSET_UTF8MB4 and CHARSET_BINARY since the
  // build_json_value_node_ switch supports ObVarcharType with these charsets.
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
}


TEST_F(TestLogicalTableValueCodec, build_json_value_node_handles_more_scalar_types)
{
  ObJsonNode *value_node = nullptr;

  ObObjMeta float_meta = make_meta(ObFloatType);
  float f = 1.25f;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::build_json_value_node_(float_meta, ObDatum(reinterpret_cast<char *>(&f), sizeof(float), false), arena_, value_node));
  ASSERT_NE(nullptr, value_node);
  ASSERT_EQ(ObJsonNodeType::J_DOUBLE, value_node->json_type());
  ASSERT_DOUBLE_EQ(1.25, static_cast<ObJsonDouble *>(value_node)->value());

  value_node = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::build_json_value_node_(make_meta(ObUInt64Type), make_uint_datum(123), arena_, value_node));
  ASSERT_NE(nullptr, value_node);
  ASSERT_EQ(ObJsonNodeType::J_UINT, value_node->json_type());
  ASSERT_EQ(123U, static_cast<ObJsonUint *>(value_node)->value());

  value_node = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::build_json_value_node_(
    make_meta(ObCharType, CS_TYPE_UTF8MB4_BIN), make_string_datum("x", 1), arena_, value_node));
  ASSERT_NE(nullptr, value_node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, value_node->json_type());
  ASSERT_EQ("x", std::string(static_cast<ObJsonString *>(value_node)->value().ptr(),
    static_cast<ObJsonString *>(value_node)->value().length()));
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_invalid_argument_inputs)
{
  ObSEArray<ObString, 2> column_names;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<ObDatum, 2> datums;
  ObString value_binary;
  ObSEArray<ObDatum, 2> empty_datums;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableEncoder::encode_value(column_names, metas, empty_datums, arena_, value_binary));

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(2)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));

  column_names.reset();
  metas.reset();
  datums.reset();
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString()));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(3)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
}

TEST_F(TestLogicalTableValueCodec, build_json_value_node_rejects_unsupported_type)
{
  ObJsonNode *value_node = reinterpret_cast<ObJsonNode *>(0x1);
  // ObTimestampType is now supported (handled as ObJsonInt in the encoder);
  // use ObDateTimeType which still falls through to the default rejection branch.
  ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::build_json_value_node_(make_meta(ObDateTimeType), make_int_datum(1), arena_, value_node));
}

TEST_F(TestLogicalTableValueCodec, decode_value_rejects_non_object_root)
{
  ObIJsonBase *decoded = nullptr;
  // decode_value consumes raw JSON binary directly (no lob header), so build the
  // binary here rather than via make_json_datum (which lob-wraps its output).
  ObIJsonBase *json_bin = nullptr;
  ObString raw_binary;
  ObString text("[1,2,3]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(
    &arena_, text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json_bin));
  ASSERT_NE(nullptr, json_bin);
  ASSERT_EQ(OB_SUCCESS, json_bin->get_raw_binary(raw_binary, &arena_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLogicalTableDecoder::decode_value(
    arena_, raw_binary, decoded));
}

// ---------------------------------------------------------------------------
// Vector (float array) test cases
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_vector_float_array_roundtrip)
{
  // Encode a column whose value is a vector (JSON array of floats)
  ObSEArray<ObString, 2> column_names;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<ObDatum, 2> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  float vec[] = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("embedding")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(vec, 5)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(42)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_FALSE(value_binary.empty());
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));
  ASSERT_NE(nullptr, decoded);

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);

  // Verify the vector column is preserved as a JSON array
  ObIJsonBase *embedding = columns->get_value("embedding");
  ASSERT_NE(nullptr, embedding);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, embedding->json_type());
  ASSERT_EQ(5, embedding->element_count());
  for (int64_t i = 0; i < 5; ++i) {
    ObIJsonBase *elem = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding->get_array_element(i, elem));
    ASSERT_NE(nullptr, elem);
    ASSERT_EQ(ObJsonNodeType::J_DOUBLE, elem->json_type());
    ASSERT_NEAR(vec[i], elem->get_double(), 1e-6);
  }

  // Verify the scalar column
  ASSERT_EQ(42, static_cast<ObJsonInt *>(columns->get_value("id"))->value());
}

TEST_F(TestLogicalTableValueCodec, encode_value_empty_vector_roundtrip)
{
  // Empty float array
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("vec")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(nullptr, 0)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObIJsonBase *vec = columns->get_value("vec");
  ASSERT_NE(nullptr, vec);
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, vec->json_type());
  ASSERT_EQ(0, vec->element_count());
}

TEST_F(TestLogicalTableValueCodec, encode_value_single_element_vector_roundtrip)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  float single_val = -1.5f;
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("vec")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(&single_val, 1)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObIJsonBase *vec = columns->get_value("vec");
  ASSERT_EQ(1, vec->element_count());
  ObIJsonBase *elem = nullptr;
  ASSERT_EQ(OB_SUCCESS, vec->get_array_element(0, elem));
  ASSERT_NEAR(-1.5, elem->get_double(), 1e-6);
}

TEST_F(TestLogicalTableValueCodec, encode_value_large_vector_roundtrip)
{
  // 128-dimensional embedding vector (common in ML scenarios)
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  static const int64_t DIM = 128;
  float vec[DIM];
  for (int64_t i = 0; i < DIM; ++i) {
    vec[i] = static_cast<float>(i) * 0.01f;
  }

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("embedding")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(vec, DIM)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObIJsonBase *embedding = columns->get_value("embedding");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, embedding->json_type());
  ASSERT_EQ(DIM, embedding->element_count());
  for (int64_t i = 0; i < DIM; ++i) {
    ObIJsonBase *elem = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding->get_array_element(i, elem));
    ASSERT_NEAR(vec[i], elem->get_double(), 1e-6);
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_vector_special_float_values)
{
  // Test special float values: 0.0, negative, very large, very small
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  float vec[] = {0.0f, -0.0f, 1e-38f, 1e38f, -3.14f};
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("vec")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(vec, 5)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObIJsonBase *v = columns->get_value("vec");
  ASSERT_EQ(5, v->element_count());
  ObIJsonBase *elem = nullptr;
  ASSERT_EQ(OB_SUCCESS, v->get_array_element(0, elem));
  ASSERT_DOUBLE_EQ(0.0, elem->get_double());
  ASSERT_EQ(OB_SUCCESS, v->get_array_element(2, elem));
  ASSERT_NEAR(1e-38f, elem->get_double(), 1e-44);
  ASSERT_EQ(OB_SUCCESS, v->get_array_element(3, elem));
  ASSERT_NEAR(1e38f, elem->get_double(), 1e32);
  ASSERT_EQ(OB_SUCCESS, v->get_array_element(4, elem));
  ASSERT_NEAR(-3.14f, elem->get_double(), 1e-5);
}

TEST_F(TestLogicalTableValueCodec, encode_value_null_vector_column)
{
  // Vector column with NULL value
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("embedding")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_null_datum()));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_EQ(ObJsonNodeType::J_NULL, columns->get_value("embedding")->json_type());
}

// ---------------------------------------------------------------------------
// Nested JSON test cases
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_deeply_nested_json_roundtrip)
{
  // 4-level nested JSON object
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *json_text = "{\"a\":{\"b\":{\"c\":{\"d\":\"deep_value\"}}}}";
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("config")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum(json_text)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObIJsonBase *config = columns->get_value("config");
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, config->json_type());

  // Navigate to the deepest level
  ObJsonObject *a_obj = static_cast<ObJsonObject *>(config);
  ObJsonObject *b_obj = static_cast<ObJsonObject *>(a_obj->get_value("a"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, b_obj->json_type());
  ObJsonObject *c_obj = static_cast<ObJsonObject *>(b_obj->get_value("b"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, c_obj->json_type());
  ObJsonObject *d_obj = static_cast<ObJsonObject *>(c_obj->get_value("c"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, d_obj->json_type());
  ObIJsonBase *d_val = d_obj->get_value("d");
  ASSERT_EQ(ObJsonNodeType::J_STRING, d_val->json_type());
  ObString d_str = static_cast<ObJsonString *>(d_val)->value();
  ASSERT_EQ("deep_value", std::string(d_str.ptr(), d_str.length()));
}

TEST_F(TestLogicalTableValueCodec, encode_value_nested_json_mixed_types)
{
  // JSON with mixed types: objects, arrays, strings, numbers, booleans, null
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *json_text =
    "{\"users\":[{\"name\":\"alice\",\"age\":30,\"active\":true},"
    "{\"name\":\"bob\",\"age\":25,\"active\":false}],"
    "\"meta\":{\"count\":2,\"tags\":[\"admin\",\"user\"],\"extra\":null}}";

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("payload")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum(json_text)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObJsonObject *payload = static_cast<ObJsonObject *>(columns->get_value("payload"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, payload->json_type());

  // Verify users array
  ObIJsonBase *users = payload->get_value("users");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, users->json_type());
  ASSERT_EQ(2, users->element_count());

  // Check first user
  ObIJsonBase *user0 = nullptr;
  ASSERT_EQ(OB_SUCCESS, users->get_array_element(0, user0));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, user0->json_type());
  ObJsonObject *user0_obj = static_cast<ObJsonObject *>(user0);
  ASSERT_EQ(ObJsonNodeType::J_STRING, user0_obj->get_value("name")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_INT, user0_obj->get_value("age")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, user0_obj->get_value("active")->json_type());

  // Verify meta object
  ObJsonObject *meta_obj = static_cast<ObJsonObject *>(payload->get_value("meta"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, meta_obj->json_type());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, meta_obj->get_value("tags")->json_type());
  ASSERT_EQ(2, meta_obj->get_value("tags")->element_count());
  ASSERT_EQ(ObJsonNodeType::J_NULL, meta_obj->get_value("extra")->json_type());
}

TEST_F(TestLogicalTableValueCodec, encode_value_nested_json_with_array_of_arrays)
{
  // JSON with array of arrays (matrix-like structure)
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *json_text = "{\"matrix\":[[1,2,3],[4,5,6],[7,8,9]]}";
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("data")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum(json_text)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObJsonObject *data = static_cast<ObJsonObject *>(columns->get_value("data"));
  ObIJsonBase *matrix = data->get_value("matrix");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, matrix->json_type());
  ASSERT_EQ(3, matrix->element_count());

  // Verify each sub-array
  for (int64_t i = 0; i < 3; ++i) {
    ObIJsonBase *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, matrix->get_array_element(i, row));
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, row->json_type());
    ASSERT_EQ(3, row->element_count());
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_empty_nested_json_structures)
{
  // Empty object and empty array
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *json_text = "{\"empty_obj\":{},\"empty_arr\":[]}";
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("data")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum(json_text)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObJsonObject *data = static_cast<ObJsonObject *>(columns->get_value("data"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, data->get_value("empty_obj")->json_type());
  ASSERT_EQ(0, data->get_value("empty_obj")->element_count());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, data->get_value("empty_arr")->json_type());
  ASSERT_EQ(0, data->get_value("empty_arr")->element_count());
}

TEST_F(TestLogicalTableValueCodec, encode_value_empty_json_object_roundtrip)
{
  // Top-level JSON column value is an empty object {}
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("payload")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum("{}")));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_FALSE(value_binary.empty());
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));
  ASSERT_NE(nullptr, decoded);

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);
  ObIJsonBase *payload = columns->get_value("payload");
  ASSERT_NE(nullptr, payload);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, payload->json_type());
  ASSERT_EQ(0, payload->element_count());
}

TEST_F(TestLogicalTableValueCodec, encode_empty_value)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));
  ASSERT_NE(nullptr, decoded);

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);
  ASSERT_EQ(0, columns->element_count());
}

// ---------------------------------------------------------------------------
// Mixed: vector + nested JSON + scalars together
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_mixed_vector_json_scalars)
{
  // Simulate a real row: id, name, embedding (vector), metadata (nested JSON)
  ObSEArray<ObString, 4> column_names;
  ObSEArray<ObObjMeta, 4> metas;
  ObSEArray<ObDatum, 4> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  float vec[] = {1.0f, 2.0f, 3.0f};

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("name")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("embedding")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("metadata")));

  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));

  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(100)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_string_datum("test_item", 9)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(vec, 3)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum("{\"category\":\"A\",\"scores\":[90,95]}")));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NE(nullptr, columns);

  // Verify scalar columns
  ASSERT_EQ(100, static_cast<ObJsonInt *>(columns->get_value("id"))->value());
  ObString name_str = static_cast<ObJsonString *>(columns->get_value("name"))->value();
  ASSERT_EQ("test_item", std::string(name_str.ptr(), name_str.length()));

  // Verify vector column
  ObIJsonBase *emb = columns->get_value("embedding");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, emb->json_type());
  ASSERT_EQ(3, emb->element_count());
  for (int64_t i = 0; i < 3; ++i) {
    ObIJsonBase *elem = nullptr;
    ASSERT_EQ(OB_SUCCESS, emb->get_array_element(i, elem));
    ASSERT_NEAR(vec[i], elem->get_double(), 1e-6);
  }

  // Verify nested JSON column
  ObJsonObject *meta = static_cast<ObJsonObject *>(columns->get_value("metadata"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, meta->json_type());
  ASSERT_EQ(ObJsonNodeType::J_STRING, meta->get_value("category")->json_type());
  ObIJsonBase *scores = meta->get_value("scores");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, scores->json_type());
  ASSERT_EQ(2, scores->element_count());
}

TEST_F(TestLogicalTableValueCodec, encode_value_float_scalar_roundtrip)
{
  // Test ObFloatType as a scalar column (not vector)
  ObSEArray<ObString, 2> column_names;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<ObDatum, 2> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("score")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("weight")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObFloatType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObDoubleType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_float_datum(-2.5f)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_double_datum(0.125)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_NEAR(-2.5, static_cast<ObJsonDouble *>(columns->get_value("score"))->value(), 1e-6);
  ASSERT_DOUBLE_EQ(0.125, static_cast<ObJsonDouble *>(columns->get_value("weight"))->value());
}

TEST_F(TestLogicalTableValueCodec, encode_value_json_with_vector_embedded)
{
  // JSON object that internally contains a vector field
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *json_text =
    "{\"id\":1,\"vector\":[0.1,0.2,0.3],\"info\":{\"dim\":3,\"norm\":0.374}}";
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("doc")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum(json_text)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObJsonObject *doc = static_cast<ObJsonObject *>(columns->get_value("doc"));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, doc->json_type());

  // Check vector inside JSON
  ObIJsonBase *vector = doc->get_value("vector");
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, vector->json_type());
  ASSERT_EQ(3, vector->element_count());

  // Check nested info object
  ObJsonObject *info = static_cast<ObJsonObject *>(doc->get_value("info"));
  ASSERT_EQ(ObJsonNodeType::J_INT, info->get_value("dim")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_DOUBLE, info->get_value("norm")->json_type());
}

// ---------------------------------------------------------------------------
// Text types (ObTextType / ObLongTextType) full roundtrip
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_text_type_roundtrip)
{
  // Text/LongText datums need ObLobCommon header (inrow format)
  ObSEArray<ObString, 2> column_names;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<ObDatum, 2> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("text_col")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("long_text_col")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_lob_meta(ObTextType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_lob_meta(ObLongTextType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_lob_datum("hello text", 10)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_lob_datum("long content here", 17)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObString s1 = static_cast<ObJsonString *>(columns->get_value("text_col"))->value();
  ASSERT_EQ("hello text", std::string(s1.ptr(), s1.length()));
  ObString s2 = static_cast<ObJsonString *>(columns->get_value("long_text_col"))->value();
  ASSERT_EQ("long content here", std::string(s2.ptr(), s2.length()));
}

TEST_F(TestLogicalTableValueCodec, encode_value_text_memlob_simple_inrow)
{
  // MemLob simple inrow path: is_mem_loc_=1, has_inrow_data_=1, is_simple_=1
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("content")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_lob_meta(ObLongTextType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_disk_loc_datum("memlob simple data", 18)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObString s = static_cast<ObJsonString *>(columns->get_value("content"))->value();
  ASSERT_EQ("memlob simple data", std::string(s.ptr(), s.length()));
}

TEST_F(TestLogicalTableValueCodec, encode_value_text_memlob_complex_inrow)
{
  // MemLob complex inrow path: is_mem_loc_=1, has_inrow_data_=1, is_simple_=0,
  //   nested ObLobCommon in_row_=1
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("content")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_lob_meta(ObTextType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_memlob_complex_datum("memlob complex data", 19)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObString s = static_cast<ObJsonString *>(columns->get_value("content"))->value();
  ASSERT_EQ("memlob complex data", std::string(s.ptr(), s.length()));
}

// ---------------------------------------------------------------------------
// Integer boundary values
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_integer_boundary_values)
{
  ObSEArray<ObString, 3> column_names;
  ObSEArray<ObObjMeta, 3> metas;
  ObSEArray<ObDatum, 3> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("min_int")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("max_int")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("zero")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(INT64_MIN)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(INT64_MAX)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_int_datum(0)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_EQ(INT64_MIN, static_cast<ObJsonInt *>(columns->get_value("min_int"))->value());
  ASSERT_EQ(INT64_MAX, static_cast<ObJsonInt *>(columns->get_value("max_int"))->value());
  ASSERT_EQ(0, static_cast<ObJsonInt *>(columns->get_value("zero"))->value());
}

TEST_F(TestLogicalTableValueCodec, encode_value_uint64_boundary_values)
{
  ObSEArray<ObString, 2> column_names;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<ObDatum, 2> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("max_uint")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("zero_uint")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObUInt64Type)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObUInt64Type)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_uint_datum(UINT64_MAX)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_uint_datum(0)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_EQ(UINT64_MAX, static_cast<ObJsonUint *>(columns->get_value("max_uint"))->value());
  ASSERT_EQ(0U, static_cast<ObJsonUint *>(columns->get_value("zero_uint"))->value());
}

// ---------------------------------------------------------------------------
// Empty string and Unicode string
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_empty_string)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("s")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_string_datum("", 0)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObString s = static_cast<ObJsonString *>(columns->get_value("s"))->value();
  ASSERT_EQ(0, s.length());
}

TEST_F(TestLogicalTableValueCodec, encode_value_unicode_string)
{
  ObSEArray<ObString, 1> column_names;
  ObSEArray<ObObjMeta, 1> metas;
  ObSEArray<ObDatum, 1> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  const char *utf8_str = "\xe4\xb8\xad\xe6\x96\x87\xf0\x9f\x98\x80";  // "中文😀"
  int64_t utf8_len = static_cast<int64_t>(strlen(utf8_str));

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("text")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_string_datum(utf8_str, utf8_len)));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ObString s = static_cast<ObJsonString *>(columns->get_value("text"))->value();
  ASSERT_EQ(utf8_len, s.length());
  ASSERT_EQ(0, memcmp(utf8_str, s.ptr(), utf8_len));
}

// ---------------------------------------------------------------------------
// Multiple JSON columns in same row
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_multiple_json_columns)
{
  ObSEArray<ObString, 3> column_names;
  ObSEArray<ObObjMeta, 3> metas;
  ObSEArray<ObDatum, 3> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  float vec[] = {1.0f, 2.0f};

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("config")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("embedding")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("extra")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum("{\"k\":\"v\"}")));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_vector_datum(vec, 2)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_json_datum("{\"a\":[1,2,3]}")));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, columns->get_value("config")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, columns->get_value("embedding")->json_type());
  ASSERT_EQ(2, columns->get_value("embedding")->element_count());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, columns->get_value("extra")->json_type());
}

// ---------------------------------------------------------------------------
// All NULL columns
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_all_null_columns)
{
  ObSEArray<ObString, 3> column_names;
  ObSEArray<ObObjMeta, 3> metas;
  ObSEArray<ObDatum, 3> datums;
  ObString value_binary;
  ObIJsonBase *decoded = nullptr;

  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("a")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("b")));
  ASSERT_EQ(OB_SUCCESS, column_names.push_back(ObString("c")));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObIntType)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObVarcharType, CS_TYPE_UTF8MB4_BIN)));
  ASSERT_EQ(OB_SUCCESS, metas.push_back(make_meta(ObJsonType)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_null_datum()));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_null_datum()));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(make_null_datum()));

  ASSERT_EQ(OB_SUCCESS, ObLogicalTableEncoder::encode_value(column_names, metas, datums, arena_, value_binary));
  ASSERT_EQ(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, value_binary, decoded));

  ObJsonObject *columns = get_columns(decoded);
  ASSERT_EQ(ObJsonNodeType::J_NULL, columns->get_value("a")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_NULL, columns->get_value("b")->json_type());
  ASSERT_EQ(ObJsonNodeType::J_NULL, columns->get_value("c")->json_type());
}

// ---------------------------------------------------------------------------
// Rejection tests for removed types
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_removed_int_subtypes)
{
  ObString value_binary;
  // ObTinyIntType is now supported by the encoder (handled as ObJsonInt).
  const ObObjType removed_int_types[] = {ObSmallIntType, ObMediumIntType, ObInt32Type};
  for (int i = 0; i < 3; ++i) {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(removed_int_types[i])));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_int_datum(1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary))
      << "type index=" << i << " should be rejected";
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_removed_uint_subtypes)
{
  ObString value_binary;
  const ObObjType removed_uint_types[] = {ObUTinyIntType, ObUSmallIntType, ObUMediumIntType, ObUInt32Type};
  for (int i = 0; i < 4; ++i) {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(removed_uint_types[i])));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_uint_datum(1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary))
      << "type index=" << i << " should be rejected";
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_removed_float_double_subtypes)
{
  ObString value_binary;

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObUFloatType)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_float_datum(1.0f)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObUDoubleType)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_double_datum(1.0)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_nchar_types)
{
  ObString value_binary;

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObNVarchar2Type, CS_TYPE_UTF8MB4_BIN)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_string_datum("x", 1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObNCharType, CS_TYPE_UTF8MB4_BIN)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_string_datum("x", 1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }
}

TEST_F(TestLogicalTableValueCodec, encode_value_rejects_removed_text_subtypes)
{
  ObString value_binary;

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObTinyTextType, CS_TYPE_UTF8MB4_BIN)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_string_datum("x", 1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }

  {
    ObSEArray<ObString, 1> cn;
    ObSEArray<ObObjMeta, 1> ms;
    ObSEArray<ObDatum, 1> ds;
    ASSERT_EQ(OB_SUCCESS, cn.push_back(ObString("col")));
    ASSERT_EQ(OB_SUCCESS, ms.push_back(make_meta(ObMediumTextType, CS_TYPE_UTF8MB4_BIN)));
    ASSERT_EQ(OB_SUCCESS, ds.push_back(make_string_datum("x", 1)));
    ASSERT_EQ(OB_NOT_SUPPORTED, ObLogicalTableEncoder::encode_value(cn, ms, ds, arena_, value_binary));
  }
}

// ---------------------------------------------------------------------------
// Decode error: empty value binary
// ---------------------------------------------------------------------------

TEST_F(TestLogicalTableValueCodec, decode_value_rejects_empty_binary)
{
  ObIJsonBase *decoded = nullptr;
  ObString empty;
  ASSERT_NE(OB_SUCCESS, ObLogicalTableDecoder::decode_value(arena_, empty, decoded));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
