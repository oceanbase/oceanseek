/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include "share/search_index/ob_search_index_config.h"
#include "lib/json_type/ob_json_wrapper.h"
#define private public
#include "share/search_index/ob_search_index_encoder.h"
#include "share/search_index/ob_search_index_generator.h"
#include "share/search_index/ob_search_index_nested_key_list.h"
#include "sql/das/search/ob_das_nested_key.h"
#include "sql/das/search/ob_das_search_define.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/udt/ob_array_binary.h"
#include "lib/udt/ob_collection_type.h"
#include "storage/lob/ob_lob_manager.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#undef private

using namespace std;
namespace oceanbase {
namespace share {

// Typedef for test code simplicity
typedef ObSearchIndexRow ObIndexRow;

TEST(TestNestedKey, independent_key_and_rowid_states)
{
  sql::ObDASNestedKey position;
  EXPECT_EQ(16, sizeof(sql::ObDASRowID));
  EXPECT_EQ(16, sizeof(sql::ObDASNestedKey));
  EXPECT_TRUE(position.is_min());
  const char bytes[12] = {0, 0, 0, 1};
  position.assign(bytes, sizeof(bytes));
  EXPECT_TRUE(position.is_normal());
  EXPECT_EQ(bytes, position.ptr());
  EXPECT_EQ(12, position.length());
  sql::ObDASNestedKey target;
  target.assign(position, 4);
  EXPECT_EQ(1ULL << 32, target.value());
  EXPECT_EQ(4, target.length());
  position.set_max();
  EXPECT_TRUE(position.is_max());
  position.set_min();
  EXPECT_TRUE(position.is_min());
  position.set_value(0, 8);
  EXPECT_TRUE(position.is_normal());
  position.set_value(UINT64_MAX, 8);
  EXPECT_TRUE(position.is_normal());
  sql::ObDASRowID document;
  document.set_uint64(UINT64_MAX);
  EXPECT_TRUE(document.is_normal());
  EXPECT_EQ(UINT64_MAX, document.get_uint64());
  position.assign(bytes, sizeof(bytes));
  EXPECT_EQ(sizeof(bytes), position.length());
  position.reset();
  EXPECT_TRUE(position.is_min());
  EXPECT_EQ(0, position.length());
  EXPECT_EQ(UINT64_MAX, document.get_uint64());
  document.reset();
  EXPECT_TRUE(document.is_normal());
  EXPECT_EQ(0, document.get_uint64());
  document.set_uint64(UINT64_MAX);
  EXPECT_EQ(UINT64_MAX, document.get_uint64());
}

TEST(TestNestedKey, inline_unsigned_order)
{
  // Both low-byte carry and the sign bit must preserve storage byte-string order.
  const unsigned char keys[][8] = {
    {0, 0, 0, 0, 0, 0, 0, 255},
    {0, 0, 0, 0, 0, 0, 1, 0},
    {0, 0, 0, 1, 0, 0, 0, 0},
    {128, 0, 0, 0, 0, 0, 0, 0},
    {255, 255, 255, 255, 255, 255, 255, 255}};
  const uint64_t values[] = {255, 256, 1ULL << 32, 1ULL << 63, UINT64_MAX};
  sql::ObDASNestedKey previous;
  for (int64_t i = 0; i < 5; ++i) {
    const char *key = reinterpret_cast<const char *>(keys[i]);
    EXPECT_EQ(values[i], sql::ObDASNestedKey::load(key, 8));
    char encoded[8];
    sql::ObDASNestedKey::store(encoded, values[i], 8);
    EXPECT_EQ(0, MEMCMP(encoded, key, 8));
    sql::ObDASNestedKey row;
    row.assign(key, 8);
    EXPECT_TRUE(row.is_normal()); // UINT64_MAX is a valid key, distinct from EOF.
    EXPECT_EQ(values[i], row.value());
    EXPECT_LT(previous.compare(row, 8), 0);
    previous = row;
    row.set_max();
    EXPECT_TRUE(row.is_max());
  }
  char prefix[4];
  sql::ObDASNestedKey::store(prefix, 1ULL << 63, 4);
  EXPECT_EQ(0, MEMCMP(prefix, keys[3], 4));
  EXPECT_EQ(1ULL << 63, sql::ObDASNestedKey::load(prefix, 4));
}

TEST(TestNestedKey, short_targets_do_not_read_past_their_prefix)
{
  const char key[16] = {0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 1};
  sql::ObDASNestedKey candidate;
  candidate.assign(key, sizeof(key));
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(page_size, 0);
  void *pages = mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(MAP_FAILED, pages);
  ASSERT_EQ(0, mprotect(static_cast<char *>(pages) + page_size, page_size, PROT_NONE));
  for (int32_t bytes : {4, 8, 12}) {
    // Any read beyond this prefix hits the inaccessible page, even without ASan.
    char *target = static_cast<char *>(pages) + page_size - bytes;
    MEMCPY(target, key, bytes);
    sql::ObDASNestedKey seek_target;
    seek_target.assign(target, bytes);
    EXPECT_EQ(bytes, seek_target.length());
    EXPECT_EQ(0, candidate.compare(seek_target, bytes));
    EXPECT_EQ(16, candidate.length());
  }
  EXPECT_EQ(0, munmap(pages, 2 * page_size));
}

TEST(TestNestedKey, writer_rejects_invalid_order_and_reader_rejects_invalid_layout)
{
  const char bytes[] = {1, 0, 0, 0, 2, 4, 0, 0, 0, 2, 0, 0, 0, 1};
  common::ObArenaAllocator allocator;
  common::ObStringBuffer buffer(&allocator);
  ObSearchIndexNestedKeyListWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(buffer));
  const common::ObDatum first_key(bytes + 6, 4, false);
  const common::ObDatum smaller_key(bytes + 10, 4, false);
  ASSERT_EQ(OB_SUCCESS, writer.append(first_key));
  EXPECT_EQ(OB_ROWKEY_ORDER_ERROR, writer.append(smaller_key));
  // The reader trusts the writer's ordering check and only validates the layout.
  ObSearchIndexNestedKeyListReader reader;
  common::ObDatum truncated(bytes, sizeof(bytes) - 1, false);
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.init(truncated));
}

class TestSearchIndexEncoder : public ::testing::Test {
public:
  TestSearchIndexEncoder()
  {}
  ~TestSearchIndexEncoder()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSearchIndexEncoder);
};

static int make_binary_wrapper(ObIAllocator &allocator, ObIJsonBase &node, ObJsonWrapper &json)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *bin = nullptr;
  ObString raw;
  if (OB_FAIL(ObJsonBaseFactory::transform(&allocator, &node, ObJsonInType::JSON_BIN, bin))) {
  } else if (OB_FAIL(bin->get_raw_binary(raw, &allocator))) {
  } else {
    ret = json.parse_bin(raw.ptr(), raw.length());
  }
  return ret;
}

TEST_F(TestSearchIndexEncoder, json_wrapper_number_conversion)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObNumber decimal;
  ASSERT_EQ(OB_SUCCESS, decimal.from("12345678901234567890.123456789", allocator));
  ObJsonDecimal j_decimal(decimal);
  ObJsonInt j_int(INT64_MIN);
  ObJsonUint j_uint(UINT64_MAX);
  ObJsonDouble j_double(0.1);
  ObJsonDouble j_large(-1e100);
  ObJsonDouble j_zero(-0.0);
  ObJsonInt j_inline(-7);
  ObJsonString j_string(ObString("12.5"));
  ObJsonBoolean j_bool(true);
  ObJsonNode *nodes[] = {&j_decimal, &j_int, &j_uint, &j_double, &j_large,
                         &j_zero, &j_inline, &j_string, &j_bool};
  const char *expected[] = {"12345678901234567890.123456789", "-9223372036854775808",
                            "18446744073709551615", "0.1", "-1e100", "0", "-7", "12.5", "1"};
  ObJsonArray array(&allocator);
  for (ObJsonNode *node : nodes) {
    ASSERT_EQ(OB_SUCCESS, array.append(node));
  }
  ObJsonWrapper array_view;
  ASSERT_EQ(OB_SUCCESS, make_binary_wrapper(allocator, array, array_view));
  for (int64_t i = 0; i < ARRAYSIZEOF(nodes); ++i) {
    SCOPED_TRACE(expected[i]);
    ObNumber expected_number;
    ASSERT_EQ(OB_SUCCESS, expected_number.from_sci_opt(expected[i], strlen(expected[i]), allocator));
    ObJsonWrapper wrappers[3] = {ObJsonWrapper(nodes[i]), ObJsonWrapper(), ObJsonWrapper()};
    ASSERT_EQ(OB_SUCCESS, make_binary_wrapper(allocator, *nodes[i], wrappers[1]));
    ASSERT_EQ(OB_SUCCESS, array_view.element(i, wrappers[2]));
    for (const ObJsonWrapper &json : wrappers) {
      ObNumber actual;
      ASSERT_EQ(OB_SUCCESS, json.to_number(&allocator, actual));
      EXPECT_EQ(0, expected_number.compare(actual));
      EXPECT_EQ(OB_ERR_NULL_VALUE, json.to_number(nullptr, actual));
    }
  }
  ObNumber actual;
  EXPECT_EQ(OB_OPERATE_OVERFLOW, array_view.to_number(&allocator, actual));
  EXPECT_EQ(OB_OPERATE_OVERFLOW, ObJsonWrapper(&array).to_number(&allocator, actual));
}

TEST_F(TestSearchIndexEncoder, json_wrapper_null_and_invalid_encoding)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonNull j_null;
  ObJsonNull missing(true);
  ObJsonWrapper bin_null;
  ASSERT_EQ(OB_SUCCESS, make_binary_wrapper(allocator, j_null, bin_null));
  const ObJsonWrapper wrappers[] = {ObJsonWrapper(&j_null), bin_null};
  for (const ObJsonWrapper &json : wrappers) {
    ObString value;
    EXPECT_TRUE(json.is_real_json_null());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator, json, value));
    ASSERT_EQ(1, value.length());
    EXPECT_EQ('\0', value.ptr()[0]);
  }
  ObJsonWrapper invalid;
  ObJsonWrapper missing_wrapper(&missing);
  EXPECT_FALSE(invalid.is_real_json_null());
  EXPECT_FALSE(missing_wrapper.is_real_json_null());
  ObString value("stale");
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, invalid, value));
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, missing_wrapper, value));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, &missing, value));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, nullptr, value));
  EXPECT_TRUE(value.empty());
  ObNumber number;
  ObTime time;
  EXPECT_EQ(OB_ERR_UNEXPECTED, invalid.to_number(&allocator, number));
  EXPECT_EQ(OB_ERR_UNEXPECTED, invalid.get_obtime(time));
  ObEncParam enc_param;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc_param));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObSearchIndexValueEncoder::encode_json_scalar(allocator, invalid, value, enc_param));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObSearchIndexValueEncoder::encode_json_scalar(allocator, nullptr, value, enc_param));
  ObJsonArray array(&allocator);
  const ObJsonWrapper container(&array);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, container, value));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexValueEncoder::encode_value(allocator, &array, value));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObSearchIndexValueEncoder::encode_json_scalar(allocator, container, value, enc_param));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObSearchIndexValueEncoder::encode_json_scalar(allocator, &array, value, enc_param));
}

TEST_F(TestSearchIndexEncoder, json_scalar_encoding_bytes)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObNumber decimal;
  ASSERT_EQ(OB_SUCCESS, decimal.from("12345678901234567890.123456789", allocator));
  ObJsonDecimal j_decimal(decimal);
  ObJsonInt j_int(INT64_MIN);
  ObJsonUint j_uint(UINT64_MAX);
  ObJsonDouble j_double(0.1);
  ObJsonDouble j_large(-1e100);
  ObJsonDouble j_zero(-0.0);
  ObJsonInt j_inline(-7);
  ObJsonString j_string(ObString(3, "a\0b"));
  ObJsonBoolean j_bool(true);
  ObJsonNull j_null;
  ObTime date, time, datetime, mdate, mdatetime;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, date));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(-1234567, time));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429056000123456, nullptr, datetime));
  mdate = date;
  mdatetime = datetime;
  ObJsonDatetime j_date(date, ObDateType);
  ObJsonDatetime j_time(time, ObTimeType);
  ObJsonDatetime j_datetime(datetime, ObDateTimeType);
  ObJsonDatetime j_timestamp(datetime, ObTimestampType);
  ObJsonDatetime j_mdate(mdate, ObMySQLDateType);
  ObJsonDatetime j_mdatetime(mdatetime, ObMySQLDateTimeType);
  ObJsonOpaque j_opaque(ObString(3, "A\0B"), ObBitType);
  ObJsonNode *nodes[] = {&j_decimal, &j_int, &j_uint, &j_double, &j_large, &j_zero, &j_inline,
    &j_string, &j_bool, &j_null, &j_date, &j_time, &j_datetime, &j_timestamp, &j_mdate, &j_mdatetime, &j_opaque};
  ObJsonArray array(&allocator);
  for (ObJsonNode *node : nodes) {
    ASSERT_EQ(OB_SUCCESS, array.append(node));
  }
  ObJsonWrapper array_view;
  ASSERT_EQ(OB_SUCCESS, make_binary_wrapper(allocator, array, array_view));
  // Fixed bytes captured from the pre-wrapper encoder; query and write paths must agree.
  const char *expected[] = {
    "c20000000d149aa4360dfb38d3075bcd1600000000",
    "3efffffff5f2af9cfacd0d27feffffffff",
    "c2000000131aa0c60a2a4ae60000000000",
    "bf05f5e10100000000",
    "35fffffff4ffffffff",
    "8000000000",
    "40fffffff7ffffffff",
    "610001620000",
    "01",
    "00",
    "8000409c",
    "800000000012d687",
    "800513b807406240",
    "800513b807406240",
    "800fbe8f",
    "9995de000001e240",
    "410001420000"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(nodes); ++i) {
    SCOPED_TRACE(i);
    ObJsonWrapper wrappers[3] = {ObJsonWrapper(nodes[i]), ObJsonWrapper(), ObJsonWrapper()};
    ASSERT_EQ(OB_SUCCESS, make_binary_wrapper(allocator, *nodes[i], wrappers[1]));
    ASSERT_EQ(OB_SUCCESS, array_view.element(i, wrappers[2]));
    for (const ObJsonWrapper &json : wrappers) {
      ObString value;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator, json, value));
      std::ostringstream hex;
      for (int32_t j = 0; j < value.length(); ++j) {
        hex << std::hex << std::setfill('0') << std::setw(2)
            << static_cast<unsigned int>(static_cast<unsigned char>(value.ptr()[j]));
      }
      EXPECT_EQ(expected[i], hex.str());
    }
    ObString legacy_value, wrapper_value;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator, nodes[i], legacy_value));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator, wrappers[1], wrapper_value));
    EXPECT_EQ(legacy_value, wrapper_value);
    if (nodes[i] == &j_double) {
      // Cover the Datum JSON entry with a LOB header using the verified bytes above.
      ObString raw_binary;
      ASSERT_EQ(OB_SUCCESS, wrappers[1].get_raw_binary(allocator, raw_binary));
      ASSERT_EQ(OB_SUCCESS, ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary));
      ObDatum datum;
      datum.set_string(raw_binary);
      ObObjMeta json_meta;
      json_meta.set_type(ObJsonType);
      json_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      ObString datum_value;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(allocator, datum, json_meta, datum_value));
      EXPECT_EQ(wrapper_value, datum_value);
    }
    ObEncParam enc_param;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc_param));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_json_scalar(
        allocator, nodes[i], legacy_value, enc_param));
    EXPECT_EQ(legacy_value, wrapper_value);
  }
}

TEST_F(TestSearchIndexEncoder, test_char_column_and_query_value_encoding)
{
  using Encoder = ObSearchIndexValueEncoder;
  ASSERT_EQ(Encoder::SEARCH_INDEX_ENCODE_METHOD_STRING,
            Encoder::get_search_index_encode_method(ObCharType));
  ASSERT_TRUE(Encoder::is_search_index_encode_compatible(ObCharType, ObVarcharType));
  ASSERT_FALSE(Encoder::is_search_index_encode_compatible(ObVarcharType, ObCharType));
  ASSERT_FALSE(Encoder::is_search_index_encode_compatible(ObCharType, ObCharType));

  const ObCollationType collations[] = {
      CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_BIN, CS_TYPE_BINARY};
  const ObString values[] = {
      ObString(""), ObString("a"), ObString("A"), ObString("a "), ObString(4, "a\0\0\0")};
  for (const ObCollationType collation : collations) {
    ObArenaAllocator allocator(ObModIds::TEST);
    ObObjMeta column_meta;
    column_meta.set_type(ObCharType);
    column_meta.set_collation_type(collation);
    ObObjMeta query_meta = column_meta;
    query_meta.set_type(ObVarcharType);
    ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
    ASSERT_EQ(OB_SUCCESS, generator.init(0, column_meta, nullptr, nullptr, ObSearchIndexModeConfig(), false /* is_ddl_mode */));
    for (const ObString &input : values) {
      ObDatum datum;
      datum.set_string(input);
      ObSearchIndexRows rows;
      ASSERT_EQ(OB_SUCCESS, generator.generate_rows(0, datum, rows));
      ASSERT_EQ(1, rows.count());
      ObString query_value;
      ASSERT_EQ(OB_SUCCESS, Encoder::encode_value(allocator, datum, query_meta, query_value));
      const ObString column_value = rows.at(0).value_.get_string();
      ASSERT_EQ(column_value.length(), query_value.length());
      ASSERT_EQ(0, MEMCMP(column_value.ptr(), query_value.ptr(), column_value.length()));
      // A CHAR column is encodable, but a CHAR query input is not.
      ASSERT_EQ(OB_NOT_SUPPORTED, Encoder::encode_value(allocator, datum, column_meta, query_value));
    }
  }
}

TEST_F(TestSearchIndexEncoder, string_encoding_preserves_binary_escaping)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObEncParam enc;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
  ObString output;
  const ObString input(3, "a\0b");
  const ObString expected(6, "a\0\1b\0\0");
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(allocator, input, output, enc));
  EXPECT_EQ(expected, output);
}

TEST_F(TestSearchIndexEncoder, string_encoding_truncates_escaped_bytes)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObEncParam enc;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
  ObString output;
  // Each NUL is escaped to two bytes, followed by a two-byte terminator.
  std::string token(SEARCH_INDEX_VALUE_LENGTH / 2 - 1, '\0');
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
      allocator, ObString(token.size(), token.data()), output, enc));
  EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
  token += '\0';
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
      allocator, ObString(token.size(), token.data()), output, enc));
  EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
}

TEST_F(TestSearchIndexEncoder, string_encoding_truncation_policy)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObEncParam enc;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
  for (char byte : {'a', '\0'}) {
    SCOPED_TRACE(static_cast<int>(byte));
    const int64_t bytes_per_char = byte == '\0' ? 2 : 1;
    std::string token((SEARCH_INDEX_VALUE_LENGTH - 2) / bytes_per_char, byte);
    for (bool is_allow_truncate : {false, true}) {
      ObString output;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
          allocator, ObString(token.size(), token.data()), output, enc, is_allow_truncate));
      EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
    }
    token += 'a';
    ObString output;
    EXPECT_EQ(OB_SIZE_OVERFLOW, ObSearchIndexValueEncoder::encode_string(
        allocator, ObString(token.size(), token.data()), output, enc, false));
    EXPECT_TRUE(output.empty());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
        allocator, ObString(token.size(), token.data()), output, enc, true));
    EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
        allocator, ObString(token.size(), token.data()), output, enc));
    EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
  }
}

TEST_F(TestSearchIndexEncoder, test_array_string_encoding)
{
  const ObCollationType collations[] = {
      CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_BIN, CS_TYPE_BINARY,
      CS_TYPE_GBK_CHINESE_CI, CS_TYPE_UTF16_GENERAL_CI, CS_TYPE_UTF16LE_BIN};
  const ObString inputs[] = {
      ObString(""), ObString("a"), ObString("A"), ObString("a "), ObString("a  "),
      ObString("e"), ObString("\xc3\xa9"), ObString("\xe4\xb8\xad"), ObString(" "), ObString("  "),
      ObString(1, "\0"), ObString(2, "\0\0"), ObString(2, "a\0"),
      ObString(3, "a\0\0"), ObString(3, "a\0b")};
  for (const ObCollationType collation : collations) {
    SCOPED_TRACE(static_cast<int>(collation));
    ObArenaAllocator allocator(ObModIds::TEST);
    ObSqlCollectionInfo type_info(allocator);
    type_info.set_name(ObString("ARRAY(VARCHAR(32))"));
    ASSERT_EQ(OB_SUCCESS, type_info.parse_type_info());
    auto *arr_type = static_cast<ObCollectionArrayType *>(type_info.collection_meta_);
    auto *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
    elem_type->basic_meta_.set_collation_type(collation);
    ObIArrayType *arr_obj = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type, arr_obj));
    auto *arr = static_cast<ObArrayBinary *>(arr_obj);
    ASSERT_EQ(OB_SUCCESS, arr->push_null());
    ObString values[ARRAYSIZEOF(inputs)];
    for (int64_t i = 0; i < ARRAYSIZEOF(inputs); ++i) {
      ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(allocator, inputs[i], CS_TYPE_UTF8MB4_BIN,
                                                       collation, values[i]));
      ASSERT_EQ(OB_SUCCESS, arr->push_back(values[i]));
      ASSERT_EQ(OB_SUCCESS, arr->push_back(values[i]));
    }
    ASSERT_EQ(OB_SUCCESS, arr->init());

    ObObjMeta array_meta;
    array_meta.set_type(ObCollectionSQLType);
    ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
    ASSERT_EQ(OB_SUCCESS, generator.init(
        0, array_meta, arr_type, nullptr, ObSearchIndexModeConfig(), false /* is_ddl_mode */));
    ObSearchIndexRows rows;
    ASSERT_EQ(OB_SUCCESS, generator.generate_rows_for_array_obj(42, arr, rows));
    ASSERT_EQ(ARRAYSIZEOF(inputs) + 1, rows.count());
    ASSERT_TRUE(rows.at(0).value_.is_null());

    for (int64_t i = 0; i < ARRAYSIZEOF(inputs); ++i) {
      for (int64_t j = 0; j < ARRAYSIZEOF(inputs); ++j) {
        int cmp = 0;
        ASSERT_EQ(OB_SUCCESS, arr->compare_at(i * 2 + 1, 1, j * 2 + 1, 1, *arr, cmp));
        const int encoded_cmp = rows.at(i + 1).value_.get_string().compare(
            rows.at(j + 1).value_.get_string());
        EXPECT_EQ(cmp < 0, encoded_cmp < 0);
        EXPECT_EQ(cmp == 0, encoded_cmp == 0);
        EXPECT_EQ(cmp > 0, encoded_cmp > 0);
      }
    }
  }
}

class AlwaysFailAllocator : public ObIAllocator
{
public:
  AlwaysFailAllocator() : alloc_count_(0) {}
  virtual ~AlwaysFailAllocator() {}

  virtual void *alloc(const int64_t size) override
  {
    UNUSED(size);
    ++alloc_count_;
    return nullptr;
  }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(size);
    UNUSED(attr);
    ++alloc_count_;
    return nullptr;
  }

  virtual void free(void *ptr) override
  {
    UNUSED(ptr);
  }

  int64_t alloc_count_;
};

static const int32_t TEST_SEARCH_INDEX_COLUMN_IDX = 7;
static const uint64_t TEST_SEARCH_INDEX_DOC_ID = 42;
static const int64_t TEST_SEARCH_INDEX_INPUT_COLUMN_CNT = 2;
static const int64_t TEST_SEARCH_INDEX_STORED_ROW_DATA_OFFSET =
    sizeof(sql::ObChunkDatumStore::StoredRow)
    + sizeof(ObDatum) * TEST_SEARCH_INDEX_INPUT_COLUMN_CNT;
static const int64_t TEST_SEARCH_INDEX_STORED_ROW_SIZE =
    TEST_SEARCH_INDEX_STORED_ROW_DATA_OFFSET + sizeof(uint64_t);

static int init_null_row_generator(ObSearchIndexRowGenerator &generator)
{
  int ret = OB_SUCCESS;
  ObSEArray<int32_t, 1> included_cid_idxes;
  ObSEArray<ObObjMeta, 1> included_obj_metas;
  ObSEArray<int64_t, 2> row_projector;
  ObSEArray<ObString, 1> column_comments;
  ObObjMeta obj_meta;
  obj_meta.set_type(ObIntType);
  if (OB_FAIL(included_cid_idxes.push_back(TEST_SEARCH_INDEX_COLUMN_IDX))) {
    COMMON_LOG(WARN, "failed to add included column index", K(ret));
  } else if (OB_FAIL(included_obj_metas.push_back(obj_meta))) {
    COMMON_LOG(WARN, "failed to add included column meta", K(ret));
  } else if (OB_FAIL(row_projector.push_back(0))) {
    COMMON_LOG(WARN, "failed to add index column projector", K(ret));
  } else if (OB_FAIL(row_projector.push_back(1))) {
    COMMON_LOG(WARN, "failed to add document id projector", K(ret));
  } else if (OB_FAIL(column_comments.push_back(ObString()))) {
    COMMON_LOG(WARN, "failed to add column comment", K(ret));
  } else if (OB_FAIL(generator.init(included_cid_idxes, included_obj_metas, row_projector,
                                    column_comments, false, nullptr))) {
    COMMON_LOG(WARN, "failed to init search index row generator", K(ret));
  }
  return ret;
}

static sql::ObChunkDatumStore::StoredRow *build_null_stored_row(void *buf)
{
  sql::ObChunkDatumStore::StoredRow *stored_row =
      new (buf) sql::ObChunkDatumStore::StoredRow();
  stored_row->cnt_ = TEST_SEARCH_INDEX_INPUT_COLUMN_CNT;
  stored_row->row_size_ = static_cast<uint32_t>(TEST_SEARCH_INDEX_STORED_ROW_SIZE);
  for (int64_t i = 0; i < TEST_SEARCH_INDEX_INPUT_COLUMN_CNT; ++i) {
    new (&stored_row->cells()[i]) ObDatum();
  }
  stored_row->cells()[0].set_null();
  stored_row->cells()[1].ptr_ =
      static_cast<char *>(buf) + TEST_SEARCH_INDEX_STORED_ROW_DATA_OFFSET;
  stored_row->cells()[1].set_uint(TEST_SEARCH_INDEX_DOC_ID);
  return stored_row;
}

static void use_page_allocator(ObSearchIndexRowGenerator &generator, ObIAllocator &allocator)
{
  common::ModulePageAllocator &page_allocator =
      const_cast<common::ModulePageAllocator &>(
          generator.row_allocator_.get_arena().get_page_allocator());
  page_allocator.set_allocator(&allocator);
}

TEST_F(TestSearchIndexEncoder, test_generate_rows_stored_row_oom)
{
  alignas(uint64_t) char stored_row_buf[TEST_SEARCH_INDEX_STORED_ROW_SIZE];
  sql::ObChunkDatumStore::StoredRow *stored_row = build_null_stored_row(stored_row_buf);

  AlwaysFailAllocator failing_allocator;
  ObSearchIndexRowGenerator generator;
  ASSERT_EQ(OB_SUCCESS, init_null_row_generator(generator));
  use_page_allocator(generator, failing_allocator);

  blocksstable::ObDatumRow *rows = reinterpret_cast<blocksstable::ObDatumRow *>(1);
  int64_t row_cnt = 1;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, generator.generate_rows(stored_row, rows, row_cnt));
  ASSERT_EQ(nullptr, rows);
  ASSERT_EQ(0, row_cnt);
  ASSERT_EQ(1, failing_allocator.alloc_count_);
}

TEST_F(TestSearchIndexEncoder, test_generate_rows_datum_row_oom)
{
  ObArenaAllocator input_allocator(ObModIds::TEST);
  blocksstable::ObDatumRow stored_row;
  ASSERT_EQ(OB_SUCCESS, stored_row.init(input_allocator, TEST_SEARCH_INDEX_INPUT_COLUMN_CNT));
  stored_row.storage_datums_[0].set_null();
  stored_row.storage_datums_[1].set_uint(TEST_SEARCH_INDEX_DOC_ID);

  AlwaysFailAllocator failing_allocator;
  ObSearchIndexRowGenerator generator;
  ASSERT_EQ(OB_SUCCESS, init_null_row_generator(generator));
  use_page_allocator(generator, failing_allocator);

  blocksstable::ObDatumRow *rows = reinterpret_cast<blocksstable::ObDatumRow *>(1);
  int64_t row_cnt = 1;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, generator.generate_rows(stored_row, rows, row_cnt));
  ASSERT_EQ(nullptr, rows);
  ASSERT_EQ(0, row_cnt);
  ASSERT_EQ(1, failing_allocator.alloc_count_);
}

TEST_F(TestSearchIndexEncoder, test_generate_rows_normal)
{
  ObArenaAllocator input_allocator(ObModIds::TEST);
  alignas(uint64_t) char stored_row_buf[TEST_SEARCH_INDEX_STORED_ROW_SIZE];
  sql::ObChunkDatumStore::StoredRow *stored_row = build_null_stored_row(stored_row_buf);
  blocksstable::ObDatumRow datum_row;
  ASSERT_EQ(OB_SUCCESS, datum_row.init(input_allocator, TEST_SEARCH_INDEX_INPUT_COLUMN_CNT));
  datum_row.storage_datums_[0].set_null();
  datum_row.storage_datums_[1].set_uint(TEST_SEARCH_INDEX_DOC_ID);

  ObSearchIndexRowGenerator generator;
  ASSERT_EQ(OB_SUCCESS, init_null_row_generator(generator));

  blocksstable::ObDatumRow *rows = nullptr;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, generator.generate_rows(stored_row, rows, row_cnt));
  ASSERT_NE(nullptr, rows);
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, rows[0].get_column_count());
  ASSERT_EQ(TEST_SEARCH_INDEX_COLUMN_IDX,
            rows[0].storage_datums_[SEARCH_INDEX_COL_IDX].get_uint());
  ASSERT_TRUE(rows[0].storage_datums_[SEARCH_INDEX_PATH].is_null());
  ASSERT_TRUE(rows[0].storage_datums_[SEARCH_INDEX_VALUE].is_null());
  ASSERT_EQ(TEST_SEARCH_INDEX_DOC_ID,
            rows[0].storage_datums_[SEARCH_INDEX_DOC_ID].get_uint());

  rows = nullptr;
  row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, generator.generate_rows(datum_row, rows, row_cnt));
  ASSERT_NE(nullptr, rows);
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, rows[0].get_column_count());
  ASSERT_EQ(TEST_SEARCH_INDEX_COLUMN_IDX,
            rows[0].storage_datums_[SEARCH_INDEX_COL_IDX].get_uint());
  ASSERT_TRUE(rows[0].storage_datums_[SEARCH_INDEX_PATH].is_null());
  ASSERT_TRUE(rows[0].storage_datums_[SEARCH_INDEX_VALUE].is_null());
  ASSERT_EQ(TEST_SEARCH_INDEX_DOC_ID,
            rows[0].storage_datums_[SEARCH_INDEX_DOC_ID].get_uint());
}

// Helper function to convert JSON string to ObDatum with binary format
static int json_str_to_datum(ObIAllocator &allocator, const char *json_text, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_bin = nullptr;
  ObString json_str(json_text);
  ObString raw_binary;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin))) {
    COMMON_LOG(WARN, "get_json_base fail", K(ret), K(json_text));
  } else if (OB_FAIL(j_bin->get_raw_binary(raw_binary, &allocator))) {
    COMMON_LOG(WARN, "get_raw_binary fail", K(ret), K(json_text), KPC(j_bin));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    COMMON_LOG(WARN, "fill_lob_header fail", K(ret), K(json_text));
  } else {
    // Since ObDatumPtr is a union, ptr_ and lob_data_ share the same memory
    // set_string will set ptr_ which can be accessed as lob_data_ via get_lob_data()
    datum.set_string(raw_binary);
  }
  return ret;
}

// Helper function to build expected encoded path for testing
// Path encoding format: path_segment\x00\x1c ... \x00type
static ObString build_expected_path(ObIAllocator &allocator,
                                    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                                    ObJsonNodeType value_type)
{
  ObString result;
  int ret = ObSearchIndexPathEncoder::encode_path(allocator, path_items, value_type, result);
  if (OB_SUCCESS != ret) {
    COMMON_LOG(WARN, "failed to encode path", K(ret));
  }
  return result;
}

// Helper to check if a path exists in the generated rows
static bool find_path_in_rows(const ObSEArray<ObIndexRow, 16> &rows, const ObString &expected_path)
{
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    if (!row.path_.is_null()) {
      ObString actual_path = row.path_.get_string();
      if (actual_path.length() == expected_path.length() &&
          0 == MEMCMP(actual_path.ptr(), expected_path.ptr(), expected_path.length())) {
        return true;
      }
    }
  }
  return false;
}

// Helper to build expected nested_key from array index stack
static ObString build_expected_nested_key(ObIAllocator &allocator,
                                          const uint32_t *indices,
                                          const int64_t count)
{
  ObString result;
  ObSEArray<uint32_t, 4> stack;
  for (int64_t i = 0; i < count; ++i) {
    (void)stack.push_back(indices[i]);
  }
  int ret = ObSearchIndexNestedKeyEncoder::encode(allocator, stack, result);
  if (OB_SUCCESS != ret) {
    COMMON_LOG(WARN, "failed to encode nested key", K(ret));
  }
  return result;
}

// Helper to format binary data as uppercase hex (same as SQL HEX())
static string format_bytes_hex(const ObString &bytes)
{
  ostringstream oss;
  oss << hex << uppercase << setfill('0');
  for (int64_t i = 0; i < bytes.length(); ++i) {
    oss << setw(2) << static_cast<int>(static_cast<unsigned char>(bytes.ptr()[i]));
  }
  return oss.str();
}

// Helper to format nested_key for inspection: NULL or HEX + decoded indices
static string format_nested_key_for_print(const ObDatum &nested_key)
{
  if (nested_key.is_null()) {
    return "NULL";
  }
  ObString nk = nested_key.get_string();
  ostringstream oss;
  oss << format_bytes_hex(nk);
  if (nk.length() > 0 && nk.length() % ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL == 0) {
    oss << " (";
    for (int64_t i = 0; i < nk.length(); i += ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL) {
      if (i > 0) {
        oss << ", ";
      }
      uint32_t idx = 0;
      for (int64_t j = 0; j < ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL; ++j) {
        idx = (idx << 8) | static_cast<unsigned char>(nk.ptr()[i + j]);
      }
      oss << "[" << (i / ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL) << "]=" << idx;
    }
    oss << ")";
  }
  return oss.str();
}

// Decode encoded path prefix into readable form, e.g. $.comments[].author
static string decode_encoded_path(const ObString &path)
{
  const uint8_t *data = reinterpret_cast<const uint8_t *>(path.ptr());
  const int64_t len = path.length();
  string result = "$";
  int64_t i = 0;
  while (i + 1 < len) {
    if (data[i] != ObSearchIndexPathEncoder::ESCAPE_MARKER) {
      break;
    }
    const uint8_t enc_type = data[i + 1];
    i += 2;
    if (enc_type == ObSearchIndexPathEncoder::TYPE_OBJECT) {
      string segment;
      while (i < len && data[i] != ObSearchIndexPathEncoder::ESCAPE_MARKER) {
        segment += static_cast<char>(data[i++]);
      }
      result += "." + segment;
    } else if (enc_type == ObSearchIndexPathEncoder::TYPE_ARRAY) {
      result += "[]";
    } else {
      i -= 2;
      break;
    }
  }
  return result;
}

// Print all generated rows with path and nested_key for manual inspection
static void print_search_index_rows(const char *label, const ObSEArray<ObIndexRow, 16> &rows)
{
  cout << "========== " << label << " (" << rows.count() << " rows) ==========" << endl;
  cout << left << setw(6) << "ROW"
       << setw(36) << "PATH"
       << setw(34) << "NESTED_KEY"
       << "VALUE_NULL" << endl;
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    string path_str = row.path_.is_null() ? "<null>" : decode_encoded_path(row.path_.get_string());
    string nk_str = format_nested_key_for_print(row.nested_key_);
    cout << left << setw(6) << i
         << setw(36) << path_str
         << setw(34) << nk_str
         << (row.value_.is_null() ? "true" : "false") << endl;
  }
  cout << "==============================================================" << endl;
}

// Helper to compare nested_key datum with expected (nullptr expected_key means null nested_key)
static bool nested_key_matches(const ObDatum &nested_key, const ObString *expected_key)
{
  if (expected_key == nullptr) {
    return nested_key.is_null();
  } else if (nested_key.is_null()) {
    return false;
  } else {
    ObString actual = nested_key.get_string();
    return actual.length() == expected_key->length()
        && 0 == MEMCMP(actual.ptr(), expected_key->ptr(), actual.length());
  }
}

// Helper to find a row matching path and nested_key
static bool find_row_with_nested_key(const ObSEArray<ObIndexRow, 16> &rows,
                                     const ObString &expected_path,
                                     const ObString *expected_nested_key)
{
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    if (row.path_.is_null()) {
      continue;
    }
    ObString actual_path = row.path_.get_string();
    if (actual_path.length() == expected_path.length()
        && 0 == MEMCMP(actual_path.ptr(), expected_path.ptr(), expected_path.length())
        && nested_key_matches(row.nested_key_, expected_nested_key)) {
      return true;
    }
  }
  return false;
}

// Helper to init filter from NESTED_PATHS column comment and create generator
static int init_nested_path_generator(ObArenaAllocator &allocator,
                                      const char *column_comment_str,
                                      ObSearchIndexConfigFilter &filter,
                                      bool &has_nested_key,
                                      ObSearchIndexRowGenerator::Generator *&generator)
{
  int ret = OB_SUCCESS;
  ObString column_comment(column_comment_str);
  ObSearchIndexConfig config;
  if (OB_FAIL(config.decode(column_comment))) {
    COMMON_LOG(WARN, "failed to decode column comment", K(ret));
  } else if (OB_FAIL(filter.init_from_comment(config.get_comment()))) {
    COMMON_LOG(WARN, "failed to init filter from comment", K(ret), K(column_comment_str));
  } else {
    has_nested_key = filter.has_nested_paths();
    ObObjMeta obj_meta;
    obj_meta.set_type(ObJsonType);
    obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    void *mem = allocator.alloc(sizeof(ObSearchIndexRowGenerator::Generator));
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      generator = new (mem) ObSearchIndexRowGenerator::Generator(allocator, allocator);
      if (OB_FAIL(generator->init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */))) {
        COMMON_LOG(WARN, "failed to init generator", K(ret));
      } else {
        generator->set_has_nested_key(has_nested_key);
      }
    }
  }
  return ret;
}

// Test ObSearchIndexRowGenerator with basic nested object
// JSON: {"a": {"b": 3}}
// Expected: $.a.b -> 3
TEST_F(TestSearchIndexEncoder, test_json_row_generator_basic_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"a": {"b": 3}})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row";

  // Build expected path: $.a.b
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path_items;
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("b")));
  ObString expected_path = build_expected_path(allocator, path_items, ObJsonNodeType::J_DECIMAL);

  const ObIndexRow &row = rows.at(0);
  ASSERT_FALSE(row.path_.is_null());
  ObString actual_path = row.path_.get_string();
  ASSERT_EQ(expected_path.length(), actual_path.length()) << "Path length mismatch";
  ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
      << "Path encoding should match $.a.b";
  ASSERT_FALSE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with multiple leaf nodes
// JSON: {"name": "Alice", "age": 25}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_multiple_leaves)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"name": "Alice", "age": 25})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }
  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows";

  // Build expected paths: $.name and $.age
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> name_path_items;
  name_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("name")));
  ObString expected_name_path = build_expected_path(allocator, name_path_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> age_path_items;
  age_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("age")));
  ObString expected_age_path = build_expected_path(allocator, age_path_items, ObJsonNodeType::J_DECIMAL);

  // Verify both paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_name_path)) << "Should find $.name path";
  ASSERT_TRUE(find_path_in_rows(rows, expected_age_path)) << "Should find $.age path";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

// Test array elements with scalar values
TEST_F(TestSearchIndexEncoder, test_json_row_generator_array_scalars)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"arr": [1, 2]})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }
  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows for array scalars";

  // Build expected path: $.arr[] (array element path)
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> arr_path_items;
  arr_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  arr_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  ObString expected_path = build_expected_path(allocator, arr_path_items, ObJsonNodeType::J_DECIMAL);

  // All array elements should have the same path encoding
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    ASSERT_FALSE(row.path_.is_null());
    ObString actual_path = row.path_.get_string();
    ASSERT_EQ(expected_path.length(), actual_path.length()) << "Array element path length mismatch";
    ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
        << "Array element path should match $.arr[]";
    ASSERT_FALSE(row.value_.is_null());
  }
}

// Test array elements that contain nested objects
TEST_F(TestSearchIndexEncoder, test_json_row_generator_array_nested_objects)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"arr": [{"x": 1}, {"y": "foo"}]})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows for array nested objects";

  // Build expected paths: $.arr[].x and $.arr[].y
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> x_path_items;
  x_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  x_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  x_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("x")));
  ObString expected_x_path = build_expected_path(allocator, x_path_items, ObJsonNodeType::J_DECIMAL);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> y_path_items;
  y_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  y_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  y_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("y")));
  ObString expected_y_path = build_expected_path(allocator, y_path_items, ObJsonNodeType::J_STRING);

  // Verify both paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_x_path)) << "Should find $.arr[].x path";
  ASSERT_TRUE(find_path_in_rows(rows, expected_y_path)) << "Should find $.arr[].y path";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

// Test ObSearchIndexRowGenerator with deeply nested objects
// JSON: {"a": {"b": {"c": 42}}}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_deeply_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"a": {"b": {"c": 42}}})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row";

  // Build expected path: $.a.b.c
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path_items;
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("b")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("c")));
  ObString expected_path = build_expected_path(allocator, path_items, ObJsonNodeType::J_DECIMAL);

  const ObIndexRow &row = rows.at(0);
  ASSERT_FALSE(row.path_.is_null());
  ObString actual_path = row.path_.get_string();
  ASSERT_EQ(expected_path.length(), actual_path.length()) << "Path length mismatch";
  ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
      << "Path encoding should match $.a.b.c";
  ASSERT_FALSE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with empty object
// JSON: {}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_empty_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row for empty object";
}

// Test ObSearchIndexRowGenerator with null datum
TEST_F(TestSearchIndexEncoder, test_json_row_generator_null_datum)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObDatum datum;
  datum.set_null();

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  int ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row for null";

  const ObIndexRow &row = rows.at(0);
  ASSERT_TRUE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with complex nested structure
// JSON: {"user": {"name": "John", "age": 30, "address": {"city": "Beijing", "zip": 100000}}}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_complex_structure)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "user": {
      "name": "John",
      "age": 30,
      "address": {
        "city": "Beijing",
        "zip": 100000
      }
    }
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObString column_comment;
  ObSearchIndexConfigFilter filter(MTL_ID());
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(column_comment));
  ret = filter.init_from_comment(config.get_comment());
  ASSERT_EQ(OB_SUCCESS, ret);
  bool has_nested_key = false;
  if (filter.has_nested_paths()) {
    has_nested_key = true;
  }

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ret = generator.init(0, obj_meta, nullptr, &filter, ObSearchIndexModeConfig(), false /* is_ddl_mode */);
  ASSERT_EQ(OB_SUCCESS, ret);
  generator.set_has_nested_key(has_nested_key);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, rows.count()) << "Expected 4 search index rows (user.name, user.age, user.address.city, user.address.zip)";

  // Build expected paths
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> name_items;
  name_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  name_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("name")));
  ObString expected_name = build_expected_path(allocator, name_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> age_items;
  age_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  age_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("age")));
  ObString expected_age = build_expected_path(allocator, age_items, ObJsonNodeType::J_DECIMAL);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> city_items;
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("address")));
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("city")));
  ObString expected_city = build_expected_path(allocator, city_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> zip_items;
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("address")));
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("zip")));
  ObString expected_zip = build_expected_path(allocator, zip_items, ObJsonNodeType::J_DECIMAL);

  // Verify all paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_name)) << "Should find $.user.name";
  ASSERT_TRUE(find_path_in_rows(rows, expected_age)) << "Should find $.user.age";
  ASSERT_TRUE(find_path_in_rows(rows, expected_city)) << "Should find $.user.address.city";
  ASSERT_TRUE(find_path_in_rows(rows, expected_zip)) << "Should find $.user.address.zip";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

// NESTED_PATHS comment format: length-prefixed paths after "NESTED_PATHS="
// "$.comments" -> "10$.comments"
// "$.comments.replies" -> "18$.comments.replies"
static const char *NESTED_COMMENT_SINGLE = "NESTED_PATHS=10$.comments";
static const char *NESTED_COMMENT_REPLIES = "NESTED_PATHS=18$.comments.replies";
static const char *NESTED_COMMENT_MULTI = "NESTED_PATHS=10$.comments18$.comments.replies";

// Test NESTED_PATHS with single-level nested array ($.comments)
// nested_key encodes the comments[] array index; top-level paths have null nested_key
TEST_F(TestSearchIndexEncoder, test_json_row_generator_nested_path_single_level)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "title": "doc1",
    "comments": [
      {"author": "alice", "rating": 5},
      {"author": "bob", "rating": 4}
    ]
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSearchIndexConfigFilter filter(MTL_ID());
  bool has_nested_key = false;
  ObSearchIndexRowGenerator::Generator *generator = nullptr;
  ret = init_nested_path_generator(allocator, NESTED_COMMENT_SINGLE, filter, has_nested_key, generator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(filter.has_nested_paths());

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator->generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, rows.count()) << "Expected 5 rows: title + 2*(author,rating) under comments[]";
  print_search_index_rows("nested_path_single_level", rows);

  // Build expected paths
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> title_items;
  title_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("title")));
  ObString expected_title = build_expected_path(allocator, title_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> author_items;
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  author_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("author")));
  ObString expected_author = build_expected_path(allocator, author_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> rating_items;
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  rating_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("rating")));
  ObString expected_rating = build_expected_path(allocator, rating_items, ObJsonNodeType::J_DECIMAL);

  const uint32_t idx0[] = {0};
  const uint32_t idx1[] = {1};
  ObString nk0 = build_expected_nested_key(allocator, idx0, 1);
  ObString nk1 = build_expected_nested_key(allocator, idx1, 1);

  // Top-level path: nested_key is null
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_title, nullptr))
      << "Should find $.title with null nested_key";
  // Under comments[]: nested_key encodes array index
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_author, &nk0))
      << "Should find $.comments[].author at index 0";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_author, &nk1))
      << "Should find $.comments[].author at index 1";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_rating, &nk0))
      << "Should find $.comments[].rating at index 0";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_rating, &nk1))
      << "Should find $.comments[].rating at index 1";

  for (int64_t i = 0; i < rows.count(); ++i) {
    ASSERT_TRUE(rows.at(i).has_nested_key_) << "All rows should have nested_key column";
  }
}

// Test NESTED_PATHS with nested array at $.comments.replies (depth=2 declared, depth=1 encoded)
// When comments is an object (not array), nested_key encodes only the replies[] index.
// When comments is an array, nested_key is null for replies descendants (see object_not_array test).
TEST_F(TestSearchIndexEncoder, test_json_row_generator_nested_path_two_levels)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "comments": {
      "replies": [
        {"text": "good"},
        {"text": "well"}
      ]
    }
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSearchIndexConfigFilter filter(MTL_ID());
  bool has_nested_key = false;
  ObSearchIndexRowGenerator::Generator *generator = nullptr;
  ret = init_nested_path_generator(allocator, NESTED_COMMENT_REPLIES, filter, has_nested_key, generator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator->generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 rows for replies[].text";
  print_search_index_rows("nested_path_two_levels", rows);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 6> text_items;
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("replies")));
  text_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("text")));
  ObString expected_text = build_expected_path(allocator, text_items, ObJsonNodeType::J_STRING);

  const uint32_t idx0[] = {0};
  const uint32_t idx1[] = {1};
  ObString nested_0 = build_expected_nested_key(allocator, idx0, 1);
  ObString nested_1 = build_expected_nested_key(allocator, idx1, 1);

  ASSERT_TRUE(find_row_with_nested_key(rows, expected_text, &nested_0))
      << "Should find text at comments.replies[0]";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_text, &nested_1))
      << "Should find text at comments.replies[1]";
}

// Test multiple NESTED_PATHS: both $.comments and $.comments.replies
// comments[] level gets 1-level nested_key; replies[] level gets 2-level nested_key
TEST_F(TestSearchIndexEncoder, test_json_row_generator_nested_path_multiple)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "title": "doc1",
    "comments": [
      {
        "author": "alice",
        "replies": [
          {"text": "good"},
          {"text": "well"}
        ]
      },
      {
        "author": "bob",
        "replies": []
      }
    ]
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSearchIndexConfigFilter filter(MTL_ID());
  bool has_nested_key = false;
  ObSearchIndexRowGenerator::Generator *generator = nullptr;
  ret = init_nested_path_generator(allocator, NESTED_COMMENT_MULTI, filter, has_nested_key, generator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator->generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, rows.count()) << "Expected 6 rows: title + 2*author + 2*text + 1 empty replies";
  print_search_index_rows("nested_path_multiple", rows);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> title_items;
  title_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("title")));
  ObString expected_title = build_expected_path(allocator, title_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> author_items;
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  author_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("author")));
  ObString expected_author = build_expected_path(allocator, author_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 6> text_items;
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  text_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("replies")));
  text_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  text_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("text")));
  ObString expected_text = build_expected_path(allocator, text_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 5> replies_items;
  replies_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  replies_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  replies_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("replies")));
  ObString expected_replies = build_expected_path(allocator, replies_items, ObJsonNodeType::J_ARRAY);

  const uint32_t idx0[] = {0};
  const uint32_t idx1[] = {1};
  ObString nk0 = build_expected_nested_key(allocator, idx0, 1);
  ObString nk1 = build_expected_nested_key(allocator, idx1, 1);

  const uint32_t nk_0_0[] = {0, 0};
  const uint32_t nk_0_1[] = {0, 1};
  ObString nested_0_0 = build_expected_nested_key(allocator, nk_0_0, 2);
  ObString nested_0_1 = build_expected_nested_key(allocator, nk_0_1, 2);

  ASSERT_TRUE(find_row_with_nested_key(rows, expected_title, nullptr))
      << "Top-level $.title should have null nested_key";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_author, &nk0))
      << "Should find author at comments[0]";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_author, &nk1))
      << "Should find author at comments[1]";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_text, &nested_0_0))
      << "Should find text at comments[0].replies[0]";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_text, &nested_0_1))
      << "Should find text at comments[0].replies[1]";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_replies, &nk1))
      << "Empty replies array at comments[1] should have 1-level nested_key";
}

// Test that same (path, value) from different nested elements are not deduplicated
TEST_F(TestSearchIndexEncoder, test_json_row_generator_nested_path_dedup_by_key)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "comments": [
      {"rating": 5},
      {"rating": 5}
    ]
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSearchIndexConfigFilter filter(MTL_ID());
  bool has_nested_key = false;
  ObSearchIndexRowGenerator::Generator *generator = nullptr;
  ret = init_nested_path_generator(allocator, NESTED_COMMENT_SINGLE, filter, has_nested_key, generator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator->generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Same rating value at different indices should produce 2 rows";
  print_search_index_rows("nested_path_dedup_by_key", rows);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> rating_items;
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  rating_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("rating")));
  ObString expected_rating = build_expected_path(allocator, rating_items, ObJsonNodeType::J_DECIMAL);

  const uint32_t idx0[] = {0};
  const uint32_t idx1[] = {1};
  ObString nk0 = build_expected_nested_key(allocator, idx0, 1);
  ObString nk1 = build_expected_nested_key(allocator, idx1, 1);

  ASSERT_TRUE(find_row_with_nested_key(rows, expected_rating, &nk0));
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_rating, &nk1));
}

// Test that non-array comments (object instead of array) does not get nested_key
TEST_F(TestSearchIndexEncoder, test_json_row_generator_nested_path_object_not_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "comments": {
      "author": "alice",
      "rating": 5
    }
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSearchIndexConfigFilter filter(MTL_ID());
  bool has_nested_key = false;
  ObSearchIndexRowGenerator::Generator *generator = nullptr;
  ret = init_nested_path_generator(allocator, NESTED_COMMENT_SINGLE, filter, has_nested_key, generator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator->generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 rows for comments.author and comments.rating";
  print_search_index_rows("nested_path_object_not_array", rows);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> author_items;
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  author_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("author")));
  ObString expected_author = build_expected_path(allocator, author_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> rating_items;
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("comments")));
  rating_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("rating")));
  ObString expected_rating = build_expected_path(allocator, rating_items, ObJsonNodeType::J_DECIMAL);

  // Object-shaped comments: no array index, nested_key should be null
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_author, nullptr))
      << "Object comments.author should have null nested_key";
  ASSERT_TRUE(find_row_with_nested_key(rows, expected_rating, nullptr))
      << "Object comments.rating should have null nested_key";
}

// Regression: the JsonPathMatcher's ObArenaAllocator must be charged to the
// requested tenant, not the server tenant (500), and tenant_id must not leak
// into the page_size argument.
TEST_F(TestSearchIndexEncoder, test_json_path_matcher_arena_tenant)
{
  const uint64_t tid = MTL_ID();
  ObSearchIndexJsonPathMatcher matcher(tid);
  ASSERT_EQ(tid, matcher.allocator_.get_arena().get_tenant_id());
  ASSERT_NE(tid, matcher.allocator_.get_arena().page_size())
      << "tenant_id must not be passed as page_size";
}

// Regression: init() builds trie nodes (charging the arena), reset() must really
// release that arena memory, and reset() must be idempotent (all_nodes_ nulled)
// so a repeated reset (filter reuse, or destructor after an explicit reset) never
// walks a dangling all_nodes_ list.
TEST_F(TestSearchIndexEncoder, test_json_path_matcher_release_and_idempotent)
{
  const uint64_t tid = MTL_ID();
  ObSearchIndexJsonPathMatcher matcher(tid);
  ObSEArray<ObString, 1> paths;
  ASSERT_EQ(OB_SUCCESS, paths.push_back(ObString::make_string("$.comments")));
  ASSERT_EQ(OB_SUCCESS, matcher.init(paths));
  ASSERT_TRUE(matcher.all_nodes_ != nullptr);
  ASSERT_GT(matcher.allocator_.used(), 0);
  ASSERT_GT(matcher.allocator_.total(), 0);
  matcher.reset();
  // arena memory is really reclaimed
  ASSERT_EQ(0, matcher.allocator_.used());
  ASSERT_EQ(0, matcher.allocator_.total());
  // all_nodes_ is nulled so a second reset is a no-op, not a UAF
  ASSERT_TRUE(matcher.all_nodes_ == nullptr);
  matcher.reset();
  ASSERT_TRUE(matcher.all_nodes_ == nullptr);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
