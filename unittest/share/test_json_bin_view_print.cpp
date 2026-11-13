/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>
#define private public
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_bin_view.h"
#include "lib/json_type/ob_json_wrapper.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#undef private
#include "observer/mysql/obsm_row.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace common {

class TestJsonBinViewPrint : public ::testing::Test {
public:
  virtual void SetUp()
  {
    set_compat_mode(lib::Worker::CompatMode::MYSQL);
  }
  virtual void TearDown() {}

  void compare_print(const char *json_text, bool is_pretty = false);
  void compare_print_with_tree(ObJsonNode *tree, bool is_pretty = false);

  // Flexible DOM-vs-view consistency helper: prints the same tree through both
  // ObIJsonBase (DOM path via ObJsonBin) and ObJsonBinView with identical print
  // options and asserts the two outputs are byte-for-byte equal.
  void compare_print_opts(ObJsonNode *tree, bool is_quoted, bool is_pretty,
                          uint64_t reserve_len = 0);
  void compare_print_opts(const char *json_text, bool is_quoted, bool is_pretty,
                          uint64_t reserve_len = 0);

  void expect_print_text(ObJsonNode *tree, const char *expected,
                         bool quoted = true, bool pretty = false);
};

// Check independent expected text, since both representations now share formatters.
void TestJsonBinViewPrint::expect_print_text(
    ObJsonNode *tree, const char *expected, bool quoted, bool pretty)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  const std::string wanted = std::string("prefix:") + expected;
  ObJsonBuffer output(&allocator);
  for (ObIJsonBase *node : {static_cast<ObIJsonBase *>(tree), static_cast<ObIJsonBase *>(&bin)}) {
    output.reuse();
    ASSERT_EQ(OB_SUCCESS, output.append("prefix:"));
    ASSERT_EQ(OB_SUCCESS, node->print(output, quoted, 0, pretty));
    EXPECT_EQ(wanted, std::string(output.ptr(), output.length()));
    EXPECT_EQ('\0', output.ptr()[output.length()]);
  }
  output.reuse();
  ASSERT_EQ(OB_SUCCESS, output.append("prefix:"));
  ASSERT_EQ(OB_SUCCESS, view.print(output, quoted, 0, pretty));
  EXPECT_EQ(wanted, std::string(output.ptr(), output.length()));
  EXPECT_EQ('\0', output.ptr()[output.length()]);
}

void TestJsonBinViewPrint::compare_print(const char *json_text, bool is_pretty)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString j_text(json_text);

  // text -> tree -> bin
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ASSERT_NE(nullptr, json_tree);

  compare_print_with_tree(json_tree, is_pretty);
}

void TestJsonBinViewPrint::compare_print_with_tree(ObJsonNode *tree, bool is_pretty)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  // tree -> binary
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));

  // DOM path: ObIJsonBase::print
  ObJsonBuffer dom_buf(&allocator);
  ObIJsonBase *j_base = &bin;
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true, 0, is_pretty));

  // get raw binary for bin view
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ASSERT_TRUE(raw.length() > 0);

  // Bin View path: ObJsonBinView::print
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true, 0, is_pretty));

  // compare
  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str)
      << "\nDOM  print: " << dom_str
      << "\nView print: " << view_str
      << "\nis_pretty: " << is_pretty;
}

void TestJsonBinViewPrint::compare_print_opts(ObJsonNode *tree, bool is_quoted,
                                              bool is_pretty, uint64_t reserve_len)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  // tree -> binary
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));

  // DOM path: ObIJsonBase::print
  ObJsonBuffer dom_buf(&allocator);
  ObIJsonBase *j_base = &bin;
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, is_quoted, reserve_len, is_pretty));

  // Bin View path: ObJsonBinView::print
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ASSERT_TRUE(raw.length() > 0);
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, is_quoted, reserve_len, is_pretty));

  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str)
      << "\nDOM  print: " << dom_str
      << "\nView print: " << view_str
      << "\nis_quoted=" << is_quoted << " is_pretty=" << is_pretty
      << " reserve_len=" << reserve_len;
}

void TestJsonBinViewPrint::compare_print_opts(const char *json_text, bool is_quoted,
                                              bool is_pretty, uint64_t reserve_len)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString j_text(json_text);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ASSERT_NE(nullptr, json_tree);
  compare_print_opts(json_tree, is_quoted, is_pretty, reserve_len);
}

// ====================== scalar types ======================

TEST_F(TestJsonBinViewPrint, result_row_bin_view_text_and_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString raw[2];
  const char *texts[] = {"{\"a\":[1,\"x\"]}", "\"hello\""};
  ObObj cells[3];
  for (int i = 0; i < 2; ++i) {
    ObIJsonBase *json = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, ObString(texts[i]),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json));
    ASSERT_EQ(OB_SUCCESS, json->get_raw_binary(raw[i], &allocator));
    cells[i].set_json_value(ObJsonType, raw[i].ptr(), raw[i].length());
  }
  cells[2].set_null();
  ObNewRow row(cells, 3);
  sql::ObSQLSessionInfo session;
  ObDataTypeCastParams dtc_params;
  for (obmysql::MYSQL_PROTOCOL_TYPE protocol : {obmysql::TEXT, obmysql::BINARY}) {
    std::string expected;
    for (bool enabled : {false, true}) {
      char packet[256] = {};
      int64_t pos = 0;
      ObSMRow result(protocol, row, dtc_params, session, nullptr, nullptr,
                     OB_SERVER_TENANT_ID, enabled);
      ASSERT_EQ(OB_SUCCESS, result.serialize(packet, sizeof(packet), pos));
      if (!enabled) {
        expected.assign(packet, pos);
      } else {
        EXPECT_EQ(expected, std::string(packet, pos));
      }
      if (protocol == obmysql::BINARY) {
        EXPECT_EQ(0, packet[0]);
        EXPECT_EQ(0x10, packet[1]);
      } else {
        EXPECT_EQ(0xfb, static_cast<unsigned char>(packet[pos - 1]));
      }
      // Packed rows already contain the protocol bytes.
      ObObj packed_cell;
      packed_cell.set_varchar(ObString(static_cast<int32_t>(pos), packet));
      ObNewRow packed_row(&packed_cell, 1);
      ObSMRow packed(protocol, packed_row, dtc_params, session, nullptr, nullptr,
                     OB_SERVER_TENANT_ID, enabled);
      packed.set_packed(true);
      char forwarded[256] = {};
      int64_t forwarded_pos = 0;
      ASSERT_EQ(OB_SUCCESS, packed.serialize(forwarded, sizeof(forwarded), forwarded_pos));
      EXPECT_EQ(expected, std::string(forwarded, forwarded_pos));
    }
  }
}

TEST_F(TestJsonBinViewPrint, null)
{
  compare_print("null");
}

TEST_F(TestJsonBinViewPrint, bool_true)
{
  compare_print("true");
}

TEST_F(TestJsonBinViewPrint, bool_false)
{
  compare_print("false");
}

TEST_F(TestJsonBinViewPrint, int_positive)
{
  compare_print("12345");
}

TEST_F(TestJsonBinViewPrint, int_negative)
{
  compare_print("-99999");
}

TEST_F(TestJsonBinViewPrint, int_zero)
{
  compare_print("0");
}

TEST_F(TestJsonBinViewPrint, uint_large)
{
  compare_print("18446744073709551615");
}

TEST_F(TestJsonBinViewPrint, double_value)
{
  compare_print("3.14159");
}

TEST_F(TestJsonBinViewPrint, double_integer_form)
{
  compare_print("100.0");
}

TEST_F(TestJsonBinViewPrint, double_scientific)
{
  compare_print("1.23e10");
}

TEST_F(TestJsonBinViewPrint, double_negative)
{
  compare_print("-0.001");
}

TEST_F(TestJsonBinViewPrint, string_simple)
{
  compare_print("\"hello world\"");
}

TEST_F(TestJsonBinViewPrint, string_empty)
{
  compare_print("\"\"");
}

TEST_F(TestJsonBinViewPrint, string_with_escapes)
{
  compare_print("\"line1\\nline2\\ttab\\\"quoted\\\"\"");
}

TEST_F(TestJsonBinViewPrint, string_unicode)
{
  compare_print("\"\\u4f60\\u597d\"");
}

// ====================== containers ======================

TEST_F(TestJsonBinViewPrint, empty_array)
{
  compare_print("[]");
}

TEST_F(TestJsonBinViewPrint, empty_object)
{
  compare_print("{}");
}

TEST_F(TestJsonBinViewPrint, simple_array)
{
  compare_print("[1, 2, 3]");
}

TEST_F(TestJsonBinViewPrint, mixed_array)
{
  compare_print("[1, \"two\", true, null, 3.14]");
}

TEST_F(TestJsonBinViewPrint, nested_array)
{
  compare_print("[[1, 2], [3, [4, 5]], []]");
}

TEST_F(TestJsonBinViewPrint, simple_object)
{
  compare_print("{\"a\": 1, \"b\": 2}");
}

TEST_F(TestJsonBinViewPrint, nested_object)
{
  compare_print("{\"outer\": {\"inner\": {\"deep\": true}}}");
}

TEST_F(TestJsonBinViewPrint, object_mixed_values)
{
  compare_print("{\"str\": \"hello\", \"num\": 42, \"bool\": false, \"nil\": null, \"arr\": [1,2]}");
}

TEST_F(TestJsonBinViewPrint, complex_nested)
{
  compare_print(
    "{\"users\": [{\"name\": \"Alice\", \"age\": 30, \"tags\": [\"admin\", \"active\"]},"
    " {\"name\": \"Bob\", \"age\": 25, \"tags\": []}],"
    " \"count\": 2, \"active\": true}");
}

// ====================== pretty print ======================

TEST_F(TestJsonBinViewPrint, pretty_simple_object)
{
  compare_print("{\"a\": 1, \"b\": 2}", true);
}

TEST_F(TestJsonBinViewPrint, pretty_nested_object)
{
  compare_print("{\"outer\": {\"inner\": 1}}", true);
}

TEST_F(TestJsonBinViewPrint, pretty_array)
{
  compare_print("[1, 2, 3]", true);
}

TEST_F(TestJsonBinViewPrint, pretty_complex)
{
  compare_print(
    "{\"users\": [{\"name\": \"Alice\", \"scores\": [90, 85, 92]}],"
    " \"meta\": {\"version\": 1}}", true);
}

TEST_F(TestJsonBinViewPrint, pretty_empty_containers)
{
  compare_print("{\"a\": [], \"b\": {}}", true);
}

// ====================== time types (built from tree nodes) ======================

TEST_F(TestJsonBinViewPrint, datetime_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 6;
  ob_time.parts_[DT_MDAY] = 15;
  ob_time.parts_[DT_HOUR] = 10;
  ob_time.parts_[DT_MIN] = 30;
  ob_time.parts_[DT_SEC] = 45;
  ob_time.parts_[DT_USEC] = 123456;
  ObJsonDatetime *dt_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, dt_node);
  dt_node = new (dt_node) ObJsonDatetime(ObJsonNodeType::J_DATETIME, ob_time);
  compare_print_with_tree(dt_node);
}

TEST_F(TestJsonBinViewPrint, date_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 12;
  ob_time.parts_[DT_MDAY] = 25;
  ObJsonDatetime *date_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, date_node);
  date_node = new (date_node) ObJsonDatetime(ObJsonNodeType::J_DATE, ob_time);
  compare_print_with_tree(date_node);
}

TEST_F(TestJsonBinViewPrint, time_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_HOUR] = 14;
  ob_time.parts_[DT_MIN] = 30;
  ob_time.parts_[DT_SEC] = 59;
  ob_time.parts_[DT_USEC] = 500000;
  ob_time.mode_ = DT_TYPE_TIME;
  ObJsonDatetime *time_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, time_node);
  time_node = new (time_node) ObJsonDatetime(ObJsonNodeType::J_TIME, ob_time);
  compare_print_with_tree(time_node);
}

TEST_F(TestJsonBinViewPrint, timestamp_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 1;
  ob_time.parts_[DT_MDAY] = 1;
  ob_time.parts_[DT_HOUR] = 0;
  ob_time.parts_[DT_MIN] = 0;
  ob_time.parts_[DT_SEC] = 0;
  ob_time.parts_[DT_USEC] = 0;
  ObJsonDatetime *ts_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, ts_node);
  ts_node = new (ts_node) ObJsonDatetime(ObJsonNodeType::J_TIMESTAMP, ob_time);
  compare_print_with_tree(ts_node);
}

// ====================== opaque type ======================

TEST_F(TestJsonBinViewPrint, opaque_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *data = "\x01\x02\x03\x04";
  ObString opaque_data(4, data);
  ObJsonOpaque *opaque_node = static_cast<ObJsonOpaque *>(
      allocator.alloc(sizeof(ObJsonOpaque)));
  ASSERT_NE(nullptr, opaque_node);
  opaque_node = new (opaque_node) ObJsonOpaque(opaque_data, ObBitType);
  compare_print_with_tree(opaque_node);
}

// ====================== decimal type ======================

TEST_F(TestJsonBinViewPrint, decimal_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber num;
  ASSERT_EQ(OB_SUCCESS, num.from("123.456", allocator));
  ObJsonDecimal *dec_node = static_cast<ObJsonDecimal *>(
      allocator.alloc(sizeof(ObJsonDecimal)));
  ASSERT_NE(nullptr, dec_node);
  dec_node = new (dec_node) ObJsonDecimal(num, -1, 3);
  compare_print_with_tree(dec_node);
}

TEST_F(TestJsonBinViewPrint, decimal_integer)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber num;
  ASSERT_EQ(OB_SUCCESS, num.from("999", allocator));
  ObJsonDecimal *dec_node = static_cast<ObJsonDecimal *>(
      allocator.alloc(sizeof(ObJsonDecimal)));
  ASSERT_NE(nullptr, dec_node);
  dec_node = new (dec_node) ObJsonDecimal(num, -1, 0);
  compare_print_with_tree(dec_node);
}

// ====================== containers with time/opaque values ======================

TEST_F(TestJsonBinViewPrint, array_with_datetime)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonArray *arr = static_cast<ObJsonArray *>(
      allocator.alloc(sizeof(ObJsonArray)));
  ASSERT_NE(nullptr, arr);
  arr = new (arr) ObJsonArray(&allocator);

  // add an int
  ObJsonInt *int_node = static_cast<ObJsonInt *>(
      allocator.alloc(sizeof(ObJsonInt)));
  int_node = new (int_node) ObJsonInt(42);
  ASSERT_EQ(OB_SUCCESS, arr->append(int_node));

  // add a datetime
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 6;
  ob_time.parts_[DT_MDAY] = 15;
  ob_time.parts_[DT_HOUR] = 12;
  ob_time.parts_[DT_MIN] = 0;
  ob_time.parts_[DT_SEC] = 0;
  ob_time.parts_[DT_USEC] = 0;
  ObJsonDatetime *dt_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  dt_node = new (dt_node) ObJsonDatetime(ObJsonNodeType::J_DATETIME, ob_time);
  ASSERT_EQ(OB_SUCCESS, arr->append(dt_node));

  // add a string
  ObJsonString *str_node = static_cast<ObJsonString *>(
      allocator.alloc(sizeof(ObJsonString)));
  str_node = new (str_node) ObJsonString("hello", 5);
  ASSERT_EQ(OB_SUCCESS, arr->append(str_node));

  compare_print_with_tree(arr);
}

TEST_F(TestJsonBinViewPrint, object_with_mixed_types)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject *obj = static_cast<ObJsonObject *>(
      allocator.alloc(sizeof(ObJsonObject)));
  ASSERT_NE(nullptr, obj);
  obj = new (obj) ObJsonObject(&allocator);

  // key "num" -> int
  ObJsonInt *int_val = static_cast<ObJsonInt *>(
      allocator.alloc(sizeof(ObJsonInt)));
  int_val = new (int_val) ObJsonInt(100);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("num"), int_val));

  // key "date" -> date
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 3;
  ob_time.parts_[DT_MDAY] = 14;
  ObJsonDatetime *date_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  date_node = new (date_node) ObJsonDatetime(ObJsonNodeType::J_DATE, ob_time);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("date"), date_node));

  // key "flag" -> bool
  ObJsonBoolean *bool_val = static_cast<ObJsonBoolean *>(
      allocator.alloc(sizeof(ObJsonBoolean)));
  bool_val = new (bool_val) ObJsonBoolean(true);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("flag"), bool_val));

  // key "data" -> opaque
  const char *raw = "\xDE\xAD";
  ObJsonOpaque *opaque_val = static_cast<ObJsonOpaque *>(
      allocator.alloc(sizeof(ObJsonOpaque)));
  opaque_val = new (opaque_val) ObJsonOpaque(ObString(2, raw), ObBitType);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("data"), opaque_val));

  compare_print_with_tree(obj);
  compare_print_with_tree(obj, true);
}

// ====================== edge cases ======================

TEST_F(TestJsonBinViewPrint, deeply_nested)
{
  compare_print("{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": 1}}}}}");
  compare_print("{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": 1}}}}}", true);
}

TEST_F(TestJsonBinViewPrint, large_array)
{
  std::string json = "[";
  for (int i = 0; i < 100; ++i) {
    if (i > 0) json += ", ";
    json += std::to_string(i);
  }
  json += "]";
  compare_print(json.c_str());
}

TEST_F(TestJsonBinViewPrint, string_special_chars)
{
  compare_print("{\"k\": \"backslash\\\\ tab\\t newline\\n quote\\\" slash/\"}");
}

TEST_F(TestJsonBinViewPrint, multiple_nesting_levels)
{
  compare_print(
    "[{\"a\": [1, {\"b\": [true, null, {\"c\": \"deep\"}]}]}, "
    "{\"x\": [[1,2],[3,4]]}]");
  compare_print(
    "[{\"a\": [1, {\"b\": [true, null, {\"c\": \"deep\"}]}]}, "
    "{\"x\": [[1,2],[3,4]]}]", true);
}

// ====================== is_quoted = false tests ======================

TEST_F(TestJsonBinViewPrint, unquoted_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString j_text("\"hello world\"");

  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));

  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));

  // DOM path: is_quoted=false
  ObJsonBuffer dom_buf(&allocator);
  ObIJsonBase *j_base = &bin;
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, false));

  // Bin View path: is_quoted=false
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, false));

  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str);
  EXPECT_EQ(dom_str, "hello world");
}

// ====================== nested container printing ======================

TEST_F(TestJsonBinViewPrint, nested_object_print)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("{\"a\":{\"b\":{\"c\":1}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true));
  EXPECT_EQ(std::string("{\"a\": {\"b\": {\"c\": 1}}}"),
            std::string(view_buf.ptr(), view_buf.length()));
}

TEST_F(TestJsonBinViewPrint, nested_object_print_with_initial_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("{\"a\":{\"b\":{\"c\":1}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  // The initial depth controls pretty indentation in both paths.
  ObJsonBuffer dom_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true, 0, true, 2));

  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true, 0, true, 2));

  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str);
}

TEST_F(TestJsonBinViewPrint, wrapper_nested_object_print)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("{\"a\":{\"b\":{\"c\":1}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonWrapper wrapper(view);

  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, wrapper.print(buf, true));
  ObJsonBuffer dom_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true));
  EXPECT_EQ(std::string(dom_buf.ptr(), dom_buf.length()), std::string(buf.ptr(), buf.length()));
}

// -------------------- floating-point boundaries --------------------

// P1: -0.0 — ob_gcvt output and the append_decimal_dot_zero check on "-0".
TEST_F(TestJsonBinViewPrint, double_negative_zero)
{
  compare_print("-0.0");
}

// P2: near DBL_MAX — buffer-size boundary for ob_gcvt.
TEST_F(TestJsonBinViewPrint, double_extreme_large)
{
  compare_print("1e308");
}

// P3: negative exponent — append_decimal_dot_zero checks for 'e' presence.
TEST_F(TestJsonBinViewPrint, double_extreme_small)
{
  compare_print("1e-300");
}

// P4: integer-valued double in scientific form — ob_gcvt may emit "1e+20";
// the bin path's append_decimal_dot_zero and the DOM path's
// ObFindDoubleEscapeFunc must both recognize the 'e'.
TEST_F(TestJsonBinViewPrint, double_integer_valued_scientific)
{
  compare_print("1e20");
}

// P5: smallest positive subnormal double.
TEST_F(TestJsonBinViewPrint, double_min_subnormal)
{
  compare_print("5e-324");
}

// P6: largest subnormal double.
TEST_F(TestJsonBinViewPrint, double_max_subnormal)
{
  compare_print("2.225073858507201e-308");
}

// -------------------- special object keys --------------------

// P7: empty key "" — bin path has an explicit key.empty() branch.
TEST_F(TestJsonBinViewPrint, object_empty_key)
{
  compare_print("{\"\":1}");
}

// P8: key containing a double-quote — exercises append_string escaping.
TEST_F(TestJsonBinViewPrint, object_key_with_quote)
{
  compare_print("{\"a\\\"b\":1}");
}

// P9: key containing a backslash — exercises append_string escaping.
TEST_F(TestJsonBinViewPrint, object_key_with_backslash)
{
  compare_print("{\"a\\\\b\":1}");
}

// P10: key containing a control character via unicode escape.
TEST_F(TestJsonBinViewPrint, object_key_with_control_char)
{
  compare_print("{\"a\\u0001b\":1}");
}

// -------------------- deep nesting / large containers --------------------

// P11: 90-level nested object — near the JSON_DOCUMENT_MAX_DEPTH (100) limit,
// well past the ObSEArray<Leg,64> inline capacity. Tests both plain and pretty,
// since pretty indentation may diverge at scale.
TEST_F(TestJsonBinViewPrint, deeply_nested_90_levels)
{
  std::string json;
  for (int i = 0; i < 90; ++i) json += "{\"a\":";
  json += "1";
  for (int i = 0; i < 90; ++i) json += "}";
  compare_print(json.c_str());
  compare_print(json.c_str(), true);
}

// P12: 90-level nested array — same depth stress on the array branch.
TEST_F(TestJsonBinViewPrint, deeply_nested_array_90_levels)
{
  std::string json;
  for (int i = 0; i < 90; ++i) json += "[";
  json += "1";
  for (int i = 0; i < 90; ++i) json += "]";
  compare_print(json.c_str());
  compare_print(json.c_str(), true);
}

// P13: large array with pretty printing — existing large_array is non-pretty;
// this stresses pretty newline/indent with multi-digit indices.
TEST_F(TestJsonBinViewPrint, large_array_pretty)
{
  std::string json = "[";
  for (int i = 0; i < 100; ++i) {
    if (i > 0) json += ", ";
    json += std::to_string(i);
  }
  json += "]";
  compare_print(json.c_str(), true);
}

// P14: large object with pretty printing — stresses newline/indent for many keys.
TEST_F(TestJsonBinViewPrint, large_object_pretty)
{
  std::string json = "{";
  for (int i = 0; i < 50; ++i) {
    if (i > 0) json += ", ";
    json += "\"k" + std::to_string(i) + "\": " + std::to_string(i);
  }
  json += "}";
  compare_print(json.c_str(), true);
}

// P15: nested empty containers — pretty formatting of [[[]],{},[{}]].
TEST_F(TestJsonBinViewPrint, nested_empty_containers)
{
  compare_print("[[[]],{},[{}]]");
  compare_print("[[[]],{},[{}]]", true);
}

// -------------------- string escapes / unicode --------------------

// P16: all JSON string escape sequences in one string.
TEST_F(TestJsonBinViewPrint, string_all_escapes)
{
  compare_print("\"\\b\\f\\n\\r\\t\\\"\\\\\\/\"");
}

// P17: non-BMP unicode (surrogate pair emoji U+1F600).
TEST_F(TestJsonBinViewPrint, string_non_bmp_unicode)
{
  compare_print("\"\\uD83D\\uDE00\"");
}

// -------------------- decimal boundaries --------------------

// P18: decimal with negative scale.
TEST_F(TestJsonBinViewPrint, decimal_negative_scale)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber num;
  ASSERT_EQ(OB_SUCCESS, num.from("12345", allocator));
  ObJsonDecimal *dec_node = static_cast<ObJsonDecimal *>(
      allocator.alloc(sizeof(ObJsonDecimal)));
  ASSERT_NE(nullptr, dec_node);
  dec_node = new (dec_node) ObJsonDecimal(num, -1, -2);
  compare_print_with_tree(dec_node);
}

// P19: high-precision decimal.
TEST_F(TestJsonBinViewPrint, decimal_high_precision)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber num;
  ASSERT_EQ(OB_SUCCESS, num.from("12345678901234567890.123456789", allocator));
  ObJsonDecimal *dec_node = static_cast<ObJsonDecimal *>(
      allocator.alloc(sizeof(ObJsonDecimal)));
  ASSERT_NE(nullptr, dec_node);
  dec_node = new (dec_node) ObJsonDecimal(num, -1, 9);
  compare_print_with_tree(dec_node);
}

// P20: zero-valued decimal.
TEST_F(TestJsonBinViewPrint, decimal_zero)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber num;
  ASSERT_EQ(OB_SUCCESS, num.from("0", allocator));
  ObJsonDecimal *dec_node = static_cast<ObJsonDecimal *>(
      allocator.alloc(sizeof(ObJsonDecimal)));
  ASSERT_NE(nullptr, dec_node);
  dec_node = new (dec_node) ObJsonDecimal(num, -1, 0);
  compare_print_with_tree(dec_node);
}

// -------------------- mixed / container is_quoted --------------------

// P21: array mixing int and double values — type distinction (1 vs 1.0) must
// be preserved by both print paths.
TEST_F(TestJsonBinViewPrint, mixed_int_and_double_array)
{
  compare_print("[1, 1.0, 2, 2.0]");
}

// P22: container printed with is_quoted=false — existing unquoted_string only
// covers a scalar; containers with is_quoted=false must also agree.
TEST_F(TestJsonBinViewPrint, container_is_quoted_false)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("[1, \"two\", {\"k\": 3}]");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));

  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));

  // DOM path: is_quoted=false
  ObJsonBuffer dom_buf(&allocator);
  ObIJsonBase *j_base = &bin;
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, false));

  // Bin View path: is_quoted=false
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, false));

  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str);
}

// ====================== is_quoted=false x time/opaque (gap 1) ======================
// print_jtime_impl / print_opaque_impl have an explicit is_quoted branch that
// was only ever exercised on the is_quoted=true path.

TEST_F(TestJsonBinViewPrint, unquoted_datetime)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 6;
  ob_time.parts_[DT_MDAY] = 15;
  ob_time.parts_[DT_HOUR] = 10;
  ob_time.parts_[DT_MIN] = 30;
  ob_time.parts_[DT_SEC] = 45;
  ob_time.parts_[DT_USEC] = 123456;
  ObJsonDatetime *dt_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, dt_node);
  dt_node = new (dt_node) ObJsonDatetime(ObJsonNodeType::J_DATETIME, ob_time);
  compare_print_opts(dt_node, false, false);
}

TEST_F(TestJsonBinViewPrint, unquoted_date)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 12;
  ob_time.parts_[DT_MDAY] = 25;
  ObJsonDatetime *date_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, date_node);
  date_node = new (date_node) ObJsonDatetime(ObJsonNodeType::J_DATE, ob_time);
  compare_print_opts(date_node, false, false);
}

TEST_F(TestJsonBinViewPrint, unquoted_time)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_HOUR] = 14;
  ob_time.parts_[DT_MIN] = 30;
  ob_time.parts_[DT_SEC] = 59;
  ob_time.parts_[DT_USEC] = 500000;
  ob_time.mode_ = DT_TYPE_TIME;
  ObJsonDatetime *time_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, time_node);
  time_node = new (time_node) ObJsonDatetime(ObJsonNodeType::J_TIME, ob_time);
  compare_print_opts(time_node, false, false);
}

TEST_F(TestJsonBinViewPrint, unquoted_timestamp)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 1;
  ob_time.parts_[DT_MDAY] = 1;
  ob_time.parts_[DT_HOUR] = 0;
  ob_time.parts_[DT_MIN] = 0;
  ob_time.parts_[DT_SEC] = 0;
  ob_time.parts_[DT_USEC] = 0;
  ObJsonDatetime *ts_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  ASSERT_NE(nullptr, ts_node);
  ts_node = new (ts_node) ObJsonDatetime(ObJsonNodeType::J_TIMESTAMP, ob_time);
  compare_print_opts(ts_node, false, false);
}

TEST_F(TestJsonBinViewPrint, unquoted_opaque)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *data = "\x01\x02\x03\x04";
  ObString opaque_data(4, data);
  ObJsonOpaque *opaque_node = static_cast<ObJsonOpaque *>(
      allocator.alloc(sizeof(ObJsonOpaque)));
  ASSERT_NE(nullptr, opaque_node);
  opaque_node = new (opaque_node) ObJsonOpaque(opaque_data, ObBitType);
  compare_print_opts(opaque_node, false, false);
}

// is_quoted=false x container holding time/opaque values: the container itself
// prints unquoted at top level, and the time/opaque children (whose recursion
// hardcodes is_quoted=true for scalar children) still carry quotes — both paths
// must agree on this.
TEST_F(TestJsonBinViewPrint, unquoted_object_with_time_and_opaque)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject *obj = static_cast<ObJsonObject *>(
      allocator.alloc(sizeof(ObJsonObject)));
  ASSERT_NE(nullptr, obj);
  obj = new (obj) ObJsonObject(&allocator);

  ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  ob_time.parts_[DT_YEAR] = 2025;
  ob_time.parts_[DT_MON] = 6;
  ob_time.parts_[DT_MDAY] = 15;
  ob_time.parts_[DT_HOUR] = 12;
  ob_time.parts_[DT_MIN] = 0;
  ob_time.parts_[DT_SEC] = 0;
  ObJsonDatetime *dt_node = static_cast<ObJsonDatetime *>(
      allocator.alloc(sizeof(ObJsonDatetime)));
  dt_node = new (dt_node) ObJsonDatetime(ObJsonNodeType::J_DATETIME, ob_time);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("dt"), dt_node));

  const char *raw = "\xDE\xAD";
  ObJsonOpaque *opaque_val = static_cast<ObJsonOpaque *>(
      allocator.alloc(sizeof(ObJsonOpaque)));
  opaque_val = new (opaque_val) ObJsonOpaque(ObString(2, raw), ObBitType);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("op"), opaque_val));

  ObJsonInt *int_val = static_cast<ObJsonInt *>(
      allocator.alloc(sizeof(ObJsonInt)));
  int_val = new (int_val) ObJsonInt(7);
  ASSERT_EQ(OB_SUCCESS, obj->add(ObString("n"), int_val));

  compare_print_opts(obj, false, false);
  compare_print_opts(obj, false, true);
}

// ====================== is_quoted=false x is_pretty=true (gap 3) ======================

TEST_F(TestJsonBinViewPrint, unquoted_pretty_object)
{
  compare_print_opts("{\"a\": 1, \"b\": {\"c\": 2}}", false, true);
}

TEST_F(TestJsonBinViewPrint, unquoted_pretty_array)
{
  compare_print_opts("[1, {\"k\": 2}, 3]", false, true);
}

// ====================== empty string x is_quoted=false (gap 5) ======================
// print_scalar_impl has a dedicated `is_quoted && str.length()==0` branch; the
// complementary unquoted-empty path goes through append_string.

TEST_F(TestJsonBinViewPrint, unquoted_empty_string_scalar)
{
  compare_print_opts("\"\"", false, false);
}

TEST_F(TestJsonBinViewPrint, unquoted_empty_string_in_object)
{
  compare_print_opts("{\"k\": \"\"}", false, false);
}

// ====================== oracle mode pretty colon " : " (gap 4) ======================
// print_container_impl (view) and pint_colon (DOM) both branch on
// is_oracle_mode() && is_pretty, emitting " : " instead of ": ".

TEST_F(TestJsonBinViewPrint, oracle_pretty_colon_spacing)
{
  set_compat_mode(lib::Worker::CompatMode::ORACLE);
  compare_print_opts("{\"a\": 1, \"b\": 2}", true, true);
  compare_print_opts("{\"outer\": {\"inner\": {\"deep\": true}}}", true, true);
  // non-pretty in oracle mode must keep the ": " form too
  compare_print_opts("{\"a\": 1, \"b\": 2}", true, false);
  set_compat_mode(lib::Worker::CompatMode::MYSQL);
}

// ====================== pretty nested objects ======================

TEST_F(TestJsonBinViewPrint, pretty_nested_object_matches_jsonbase)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("{\"a\":{\"b\":{\"c\":1}}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  ObJsonBuffer dom_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true, 0, true));

  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true, 0, true));
  std::string dom_str(dom_buf.ptr(), dom_buf.length());
  std::string view_str(view_buf.ptr(), view_buf.length());
  EXPECT_EQ(dom_str, view_str)
      << "\nDOM  print: " << dom_str
      << "\nView print: " << view_str;
}

// ====================== nested arrays ======================

TEST_F(TestJsonBinViewPrint, nested_array_matches_jsonbase)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("[[[[1]]]]"); // 4-level nested array
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, j_text.ptr(),
      j_text.length(), syntaxerr, NULL, json_tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  for (bool pretty : {false, true}) {
    ObJsonBuffer dom_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true, 0, pretty));
    ObJsonBuffer view_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true, 0, pretty));
    EXPECT_EQ(std::string(dom_buf.ptr(), dom_buf.length()),
              std::string(view_buf.ptr(), view_buf.length()));
  }
}

// ====================== J_NULL is_real_json_null semantics ======================
// DOM path distinguishes "real JSON null" (ObJsonNull with is_not_null=false,
// i.e. true JSON null) from "SQL NULL" (ObJsonNull with is_not_null=true).
// The binary format does NOT serialize the is_not_null flag — see
// ObJsonBinSerializer::serialize_json_value for J_NULL (writes a single \0 byte)
// and ObJsonBin::deserialize_json_value (constructs ObJsonNull() with default
// is_not_null=false).  So after tree→bin→view, both DOM (via ObJsonBin) and
// bin_view always see a real null and print "null".  These tests lock that
// behaviour in so a future change to the binary format or the print path does
// not silently break the SQL-NULL-vs-JSON-null distinction.

// P23: real JSON null — both paths print "null".
TEST_F(TestJsonBinViewPrint, null_real_json_null)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonNull *null_node = static_cast<ObJsonNull *>(
      allocator.alloc(sizeof(ObJsonNull)));
  ASSERT_NE(nullptr, null_node);
  null_node = new (null_node) ObJsonNull();  // is_not_null=false → real JSON null
  compare_print_with_tree(null_node);
}

// P24: SQL NULL (ObJsonNull with is_not_null=true) — the is_not_null flag is
// lost during tree→bin serialization, so the DOM path (via ObJsonBin) and the
// bin_view path both print "null".  When the same node is printed directly as
// a DOM tree (ObIJsonBase::print on the ObJsonNull itself, NOT via ObJsonBin),
// it prints "" — but that path never reaches ObJsonBinView.  This test asserts
// the bin/vew-vs-DOM(via ObJsonBin) agreement and documents the design fact
// that the binary format does not carry the SQL-NULL semantic.
TEST_F(TestJsonBinViewPrint, null_sql_null_in_tree)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonNull *sql_null = static_cast<ObJsonNull *>(
      allocator.alloc(sizeof(ObJsonNull)));
  ASSERT_NE(nullptr, sql_null);
  sql_null = new (sql_null) ObJsonNull(true);  // is_not_null=true → SQL NULL

  // tree -> binary
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(sql_null));

  // DOM path (via ObJsonBin): is_bin()==true → is_real_json_null()==true → "null"
  ObJsonBuffer dom_buf(&allocator);
  ObIJsonBase *j_base = &bin;
  ASSERT_EQ(OB_SUCCESS, j_base->print(dom_buf, true, 0, false));
  EXPECT_EQ(std::string("null"), std::string(dom_buf.ptr(), dom_buf.length()));

  // bin_view path: binary J_NULL carries no is_not_null flag → "null"
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonBuffer view_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true, 0, false));
  EXPECT_EQ(std::string("null"), std::string(view_buf.ptr(), view_buf.length()));

  // DOM-tree path (ObJsonNull directly, NOT via ObJsonBin): is_not_null=true → ""
  // This path never reaches ObJsonBinView; documented here for completeness.
  ObJsonBuffer tree_buf(&allocator);
  ObIJsonBase *j_tree = sql_null;
  ASSERT_EQ(OB_SUCCESS, j_tree->print(tree_buf, true, 0, false));
  EXPECT_EQ(std::string(""), std::string(tree_buf.ptr(), tree_buf.length()));
}

// ====================== reserve_len > 0 (gap 3) ======================
// Both printers reserve reserve_len*1.2 and auto-grow on demand, so the outputs
// must agree regardless of whether the reservation is sufficient.

// P25: reserve_len sufficient — no regrowth needed on either path.
TEST_F(TestJsonBinViewPrint, reserve_len_nonzero)
{
  compare_print_opts("{\"a\": 1, \"b\": [2, 3]}", true, false, 50);
}

// P26: reserve_len far too small — both paths must auto-grow and still agree.
TEST_F(TestJsonBinViewPrint, reserve_len_insufficient)
{
  compare_print_opts("[1,2,3,4,5,6,7,8,9,10]", true, false, 4);
}

TEST_F(TestJsonBinViewPrint, sequential_print_all_entry_widths)
{
  for (uint8_t var_type = JBLS_UINT8; var_type <= JBLS_UINT64; ++var_type) {
    const uint64_t width = ObJsonVar::get_var_size(var_type);
    const uint64_t maximum = width == 8 ? UINT64_MAX : ((1ULL << (width * 8)) - 1);
    const uint64_t sign_bit = 1ULL << (width * 8 - 1);
    const int64_t minimum = width == 8 ? INT64_MIN : -static_cast<int64_t>(sign_bit);
    const uint64_t values[] = {maximum, sign_bit, 0, maximum, 1, 0};
    const uint8_t types[] = {J_INT_V0, J_INT_V0, J_INT_V0, J_UINT_V0, J_BOOLEAN_V0, J_NULL_V0};
    std::string raw(OB_JSON_BIN_HEADER_LEN + 2 + 6 * (width + 1), '\0');
    ObJsonBinHeader header;
    header.type_ = J_ARRAY_V0;
    header.entry_size_ = var_type;
    header.count_size_ = JBLS_UINT8;
    header.obj_size_size_ = JBLS_UINT8;
    header.is_continuous_ = 1;
    MEMCPY(&raw[0], &header, OB_JSON_BIN_HEADER_LEN);
    raw[OB_JSON_BIN_HEADER_LEN] = 6;
    raw[OB_JSON_BIN_HEADER_LEN + 1] = static_cast<char>(raw.size());
    for (int i = 0; i < 6; ++i) {
      char *entry = &raw[0] + OB_JSON_BIN_HEADER_LEN + 2 + i * (width + 1);
      ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(values[i], var_type, entry));
      entry[width] = static_cast<char>(types[i] | OB_JSON_TYPE_INLINE_MASK);
    }
    ObJsonBinView view;
    ASSERT_EQ(OB_SUCCESS, view.init(raw.data(), raw.size()));
    ObJsonBinView child;
    ASSERT_EQ(OB_SUCCESS, view.element(1, child));
    EXPECT_EQ(minimum, child.get_int());
    ASSERT_EQ(OB_SUCCESS, view.element(3, child));
    EXPECT_EQ(maximum, child.get_uint());
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer output(&allocator);
    ASSERT_EQ(OB_SUCCESS, view.print(output, false));
    EXPECT_EQ("[-1, " + std::to_string(minimum) + ", 0, " + std::to_string(maximum)
              + ", true, null]", std::string(output.ptr(), output.length()));
    output.reuse();
    ASSERT_EQ(OB_SUCCESS, view.print(output, true, 0, true));
    EXPECT_EQ("[\n  -1,\n  " + std::to_string(minimum) + ",\n  0,\n  "
              + std::to_string(maximum) + ",\n  true,\n  null\n]",
              std::string(output.ptr(), output.length()));
  }
}

TEST_F(TestJsonBinViewPrint, sequential_print_object_keys_and_strings_all_widths)
{
  for (uint8_t var_type = JBLS_UINT8; var_type <= JBLS_UINT64; ++var_type) {
    const uint64_t width = ObJsonVar::get_var_size(var_type);
    const uint64_t keys = OB_JSON_BIN_HEADER_LEN + 2;
    const uint64_t values = keys + 4 * width;
    const uint64_t key_data = values + 2 * (width + 1);
    const uint64_t string_data = key_data + 1;
    std::string raw(string_data + 5, '\0');
    ObJsonBinHeader header;
    header.type_ = J_OBJECT_V0;
    header.entry_size_ = var_type;
    header.count_size_ = JBLS_UINT8;
    header.obj_size_size_ = JBLS_UINT8;
    header.is_continuous_ = 1;
    MEMCPY(&raw[0], &header, OB_JSON_BIN_HEADER_LEN);
    raw[OB_JSON_BIN_HEADER_LEN] = 2;
    raw[OB_JSON_BIN_HEADER_LEN + 1] = static_cast<char>(raw.size());
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(key_data, var_type, &raw[keys + i * 2 * width]));
      ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(i, var_type, &raw[keys + (i * 2 + 1) * width]));
    }
    const uint64_t minus_one = width == 8 ? UINT64_MAX : ((1ULL << (width * 8)) - 1);
    ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(minus_one, var_type, &raw[values]));
    raw[values + width] = static_cast<char>(J_INT_V0 | OB_JSON_TYPE_INLINE_MASK);
    ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(string_data, var_type, &raw[values + width + 1]));
    raw[values + 2 * width + 1] = J_STRING_V0;
    raw[key_data] = 'k';
    raw[string_data] = J_STRING_V0;
    int64_t pos = string_data + 1;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(&raw[0], raw.size(), pos, 3));
    ASSERT_EQ(string_data + 2, pos);
    MEMCPY(&raw[pos], "abc", 3);
    ObJsonBinView view;
    ASSERT_EQ(OB_SUCCESS, view.init(raw.data(), raw.size()));
    ObString key;
    ObJsonBinView child;
    ASSERT_EQ(OB_SUCCESS, view.get_key_value(1, key, child));
    EXPECT_EQ(std::string("k"), std::string(key.ptr(), key.length()));
    ObArenaAllocator allocator(ObModIds::TEST);
    ObJsonBuffer output(&allocator);
    ASSERT_EQ(OB_SUCCESS, view.print(output, false));
    EXPECT_EQ(std::string("{\"\": -1, \"k\": \"abc\"}"),
              std::string(output.ptr(), output.length()));
  }
}

TEST_F(TestJsonBinViewPrint, sequential_print_string_validation_and_fallback)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *text = "{\"k\":\"abc\"}";
  const char *syntaxerr = nullptr;
  ObJsonNode *tree = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, text, strlen(text),
      syntaxerr, nullptr, tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObString key;
  ObJsonBinView child;
  ASSERT_EQ(OB_SUCCESS, view.get_key_value(0, key, child));
  char *prefix = const_cast<char *>(child.data_);
  ASSERT_EQ(J_STRING_V0, static_cast<uint8_t>(prefix[0]));
  // Use the actual prefix when the entry tag disagrees.
  prefix[0] = J_INT_V0;
  ObJsonBuffer output(&allocator);
  ASSERT_EQ(OB_SUCCESS, view.print(output, true));
  EXPECT_EQ(std::string("{\"k\": 3}"), std::string(output.ptr(), output.length()));
  prefix[0] = J_STRING_V0;
  prefix[1] = 127; // The string goes past the container.
  EXPECT_EQ(OB_ERR_UNEXPECTED, view.get_key_value(0, key, child));
  output.reuse();
  EXPECT_EQ(OB_ERR_UNEXPECTED, view.print(output, true));
  EXPECT_EQ(std::string("{"), std::string(output.ptr(), output.length()));
  EXPECT_EQ('\0', output.ptr()[output.length()]);
}

TEST_F(TestJsonBinViewPrint, buffer_continuous_write_preserves_append_contract)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  char storage[32];
  MEMSET(storage, '#', sizeof(storage));
  ObJsonBuffer output(storage, sizeof(storage), &allocator);
  ASSERT_EQ(OB_SUCCESS, output.append("prefix:", 7, 0));
  EXPECT_EQ('\0', storage[7]);
  ASSERT_EQ(OB_SUCCESS, output.append_unterminated("abc", 3));
  EXPECT_EQ(10, output.length());
  EXPECT_EQ('#', storage[10]); // No '\0' yet.
  ASSERT_EQ(OB_SUCCESS, output.reserve(2));
  MEMCPY(output.ptr() + output.length(), "de", 2);
  ASSERT_EQ(OB_SUCCESS, output.set_length_unterminated(output.length() + 2));
  EXPECT_EQ('#', storage[12]);
  ASSERT_EQ(OB_SUCCESS, output.set_length(output.length()));
  EXPECT_STREQ("prefix:abcde", output.ptr());
  ASSERT_EQ(OB_SUCCESS, output.append("!", 1, 0));
  EXPECT_STREQ("prefix:abcde!", output.ptr());

  // Grow the buffer and check that earlier text survives.
  const std::string suffix(100, 'x');
  ASSERT_EQ(OB_SUCCESS, output.append_unterminated("?", 1));
  ASSERT_EQ(OB_SUCCESS, output.append_unterminated(suffix.data(), suffix.size()));
  ASSERT_EQ(OB_SUCCESS, output.set_length(output.length()));
  EXPECT_EQ(std::string("prefix:abcde!?") + suffix,
            std::string(output.ptr(), output.length()));
  EXPECT_EQ('\0', output.ptr()[output.length()]);
}

TEST_F(TestJsonBinViewPrint, buffer_continuous_write_errors_preserve_prefix)
{
  class RejectAllocator : public ObIAllocator
  {
  public:
    void *alloc(const int64_t) override { return nullptr; }
    void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
    void free(void *) override {}
  } allocator;
  char storage[16];
  MEMSET(storage, '#', sizeof(storage));
  ObJsonBuffer output(storage, sizeof(storage), &allocator);
  ASSERT_EQ(OB_SUCCESS, output.append_unterminated("abc", 3));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, output.append_unterminated("0123456789", 10));
  EXPECT_EQ(3, output.length());
  EXPECT_EQ(std::string("abc"), std::string(output.ptr(), output.length()));
  EXPECT_EQ('#', storage[3]); // The caller still needs to add the '\0'.
  EXPECT_EQ(OB_SIZE_OVERFLOW, output.append_unterminated("x", INT64_MAX));
  EXPECT_EQ(OB_SIZE_OVERFLOW, output.append_unterminated("x", INT64_MAX, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, output.append_unterminated("x", UINT64_MAX));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, output.set_length_unterminated(sizeof(storage)));
  EXPECT_EQ(3, output.length());
  ASSERT_EQ(OB_SUCCESS, output.set_length(output.length()));
  EXPECT_STREQ("abc", output.ptr());
  ObJsonBuffer no_allocator;
  EXPECT_EQ(OB_ERR_NULL_VALUE, no_allocator.reserve(1));
  EXPECT_EQ(OB_ERR_NULL_VALUE, no_allocator.append_unterminated("x", 1));
  EXPECT_EQ(OB_SUCCESS, no_allocator.set_length(no_allocator.length()));
}

TEST_F(TestJsonBinViewPrint, buffer_terminates_partial_output_on_escape_allocation_failure)
{
  class RejectAllocator : public ObIAllocator
  {
  public:
    void *alloc(const int64_t) override { return nullptr; }
    void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
    void free(void *) override {}
  } failing;
  ObArenaAllocator allocator(ObModIds::TEST);
  const char control = '\x01';
  ObJsonString node(&control, 1);
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(&node));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(page_size, 0);
  ASSERT_LE(raw.length(), page_size);
  void *pages = mmap(nullptr, page_size * 2, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(MAP_FAILED, pages);
  struct PageGuard {
    void *ptr_;
    size_t size_;
    ~PageGuard() { munmap(ptr_, size_); }
  } guard{pages, static_cast<size_t>(page_size * 2)};
  ASSERT_EQ(0, mprotect(static_cast<char *>(pages) + page_size, page_size, PROT_NONE));
  char *input = static_cast<char *>(pages) + page_size - raw.length();
  MEMCPY(input, raw.ptr(), raw.length());
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(input, raw.length()));
  // Fits the input and quotes, but not the escape.
  // Reading past the input hits the protected page.
  const auto check_failure = [&](const auto &json) {
    char storage[11];
    MEMSET(storage, '#', sizeof(storage));
    ObJsonBuffer output(storage, sizeof(storage), &failing);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, json.print(output, true));
    EXPECT_EQ(std::string("\"\\"), std::string(output.ptr(), output.length()));
    EXPECT_EQ('\0', output.ptr()[output.length()]);
  };
  check_failure(node);
  check_failure(bin);
  check_failure(view);
}

TEST_F(TestJsonBinViewPrint, buffer_growth_and_reuse_preserve_prefix_and_terminator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  std::string text = "{\"\":\"";
  for (int i = 0; i < 700; ++i) { text += "a\\n\\u0000\\\\\\\""; }
  text += "\",\"nested\":[true,123,{\"x\":null}]}";
  const char *syntaxerr = NULL;
  ObJsonNode *tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, text.data(),
      text.size(), syntaxerr, NULL, tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  for (bool pretty : {false, true}) {
    ObJsonBuffer reference(&allocator);
    ASSERT_EQ(OB_SUCCESS, bin.print(reference, true, 0, pretty));
    for (uint64_t initial : {1, 8, 31, 512}) {
      ObJsonBuffer output(&allocator, initial);
      ASSERT_EQ(OB_SUCCESS, output.append("prefix:", 7, 0));
      ASSERT_EQ(OB_SUCCESS, view.print(output, true, 0, pretty));
      EXPECT_EQ(std::string("prefix:") + std::string(reference.ptr(), reference.length()),
                std::string(output.ptr(), output.length()));
      EXPECT_EQ('\0', output.ptr()[output.length()]);
      output.reuse();
      ASSERT_EQ(OB_SUCCESS, view.print(output, true, 0, pretty));
      EXPECT_EQ(std::string(reference.ptr(), reference.length()),
                std::string(output.ptr(), output.length()));
      EXPECT_EQ('\0', output.ptr()[output.length()]);
    }
  }
}

TEST_F(TestJsonBinViewPrint, buffer_terminates_partial_output_on_child_error)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *text = "{\"a\":[{\"b\":1}]}";
  const char *syntaxerr = NULL;
  ObJsonNode *tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, text,
      strlen(text), syntaxerr, NULL, tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonBinView array;
  ASSERT_EQ(OB_SUCCESS, view.element(0, array));
  char *value_entry = const_cast<char *>(array.data_) + array.value_offset_start_;
  uint64_t value_offset = 0;
  ASSERT_EQ(OB_SUCCESS, ObJsonVar::read_var(value_entry, array.entry_var_type_, &value_offset));
  // Trigger a child-read error after the outer object and array have started.
  ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(0, array.entry_var_type_, value_entry));
  ObJsonBuffer output(&allocator);
  ASSERT_EQ(OB_ERR_UNEXPECTED, view.print(output, true));
  EXPECT_EQ(std::string("{\"a\": ["), std::string(output.ptr(), output.length()));
  EXPECT_EQ('\0', output.ptr()[output.length()]);
  ASSERT_EQ(OB_SUCCESS, ObJsonVar::set_var(value_offset, array.entry_var_type_, value_entry));
  output.reuse();
  ASSERT_EQ(OB_SUCCESS, view.print(output, true, 0, false));
  EXPECT_EQ(std::string("{\"a\": [{\"b\": 1}]}"), std::string(output.ptr(), output.length()));
}

TEST_F(TestJsonBinViewPrint, shared_print_expected_text)
{
  struct Case {
    const char *input;
    const char *expected;
    bool pretty;
    bool quoted;
    bool oracle;
  };
  const Case cases[] = {
    {R"([null,true,false,-9223372036854775808,18446744073709551615,1.0,"a\nb\t\"\\\u0001",""])",
     R"([null, true, false, -9223372036854775808, 18446744073709551615, 1.0, "a\nb\t\"\\\u0001", ""])",
     false, true, false},
    {R"({"a":[1,"x\ny"],"b":{}})",
     "{\n  \"a\": [\n    1,\n    \"x\\ny\"\n  ],\n  \"b\": {}\n}", true, false, false},
    {R"({"a":[1,"x\ny"],"b":{}})",
     R"({"a":[1,"x\ny"],"b":{}})", false, true, true},
    {R"({"a":[1,"x\ny"],"b":{}})",
     "{\n  \"a\" : [\n    1,\n    \"x\\ny\"\n  ],\n  \"b\" : {}\n}", true, true, true},
    {R"("a\nb\t\"\\\u0001")", "a\nb\t\"\\\x01", false, false, false},
    {R"({"":"","\"":"\\"})", R"({"": "", "\"": "\\"})", false, true, false}
  };
  for (const auto &test : cases) {
    SCOPED_TRACE(test.input);
    set_compat_mode(test.oracle ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    ObArenaAllocator allocator(ObModIds::TEST);
    const char *error = nullptr;
    ObJsonNode *tree = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(&allocator, test.input, STRLEN(test.input),
        error, nullptr, tree));
    expect_print_text(tree, test.expected, test.quoted, test.pretty);
  }
}

TEST_F(TestJsonBinViewPrint, shared_print_typed_expected_text)
{
  ObJsonOFloat float_node(1.0f);
  expect_print_text(&float_node, "1.0");
  ObJsonDouble double_node(-0.0);
  expect_print_text(&double_node, "-0.0");

  ObArenaAllocator allocator(ObModIds::TEST);
  number::ObNumber number;
  ASSERT_EQ(OB_SUCCESS, number.from("123.450", allocator));
  ObJsonDecimal decimal(number, -1, 3);
  expect_print_text(&decimal, "123.450");

  ObTime time(DT_TYPE_DATETIME);
  time.parts_[DT_YEAR] = 2025;
  time.parts_[DT_MON] = 6;
  time.parts_[DT_MDAY] = 15;
  time.parts_[DT_HOUR] = 10;
  time.parts_[DT_MIN] = 30;
  time.parts_[DT_SEC] = 45;
  time.parts_[DT_USEC] = 123456;
  time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(time);
  ObJsonDatetime datetime(ObJsonNodeType::J_DATETIME, time);
  expect_print_text(&datetime, "\"2025-06-15 10:30:45.123456\"");
  expect_print_text(&datetime, "2025-06-15 10:30:45.123456", false);

  const char payload[] = {'A', '\0', 'B'};
  ObJsonOpaque opaque(ObString(sizeof(payload), payload), ObBitType);
  expect_print_text(&opaque, "\"base64:type16:QQBC\"");
  expect_print_text(&opaque, "base64:type16:QQBC", false);
}

TEST_F(TestJsonBinViewPrint, shared_print_integer_allocation_contract)
{
  class RejectAllocator : public ObIAllocator
  {
  public:
    void *alloc(const int64_t) override { return nullptr; }
    void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
    void free(void *) override {}
  } failing;
  ObJsonInt node(7);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(&node));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, bin.get_raw_binary(raw, &allocator));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  // Both printers append only the actual digits, so the existing buffer is
  // sufficient even when the allocator rejects every allocation.
  char tree_storage[16] = {};
  ObJsonBuffer tree_buf(tree_storage, sizeof(tree_storage), &failing);
  ASSERT_EQ(OB_SUCCESS, tree_buf.append("P"));
  ASSERT_EQ(OB_SUCCESS, node.print(tree_buf, true));
  EXPECT_STREQ("P7", tree_buf.ptr());
  EXPECT_EQ(2, tree_buf.length());
  char view_storage[16] = {};
  ObJsonBuffer view_buf(view_storage, sizeof(view_storage), &failing);
  ASSERT_EQ(OB_SUCCESS, view_buf.append("P"));
  ASSERT_EQ(OB_SUCCESS, view.print(view_buf, true));
  EXPECT_STREQ("P7", view_buf.ptr());
  EXPECT_EQ(2, view_buf.length());
}

TEST_F(TestJsonBinViewPrint, shared_print_time_error_keeps_opening_quote)
{
  class FailingTime : public ObJsonDatetime
  {
  public:
    FailingTime() : ObJsonDatetime(ObTime(DT_TYPE_DATETIME), ObDateTimeType) {}
    int get_obtime(ObTime &) const override { return OB_ERR_UNEXPECTED; }
  } node;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer output(&allocator);
  ASSERT_EQ(OB_SUCCESS, output.append("prefix:"));
  EXPECT_EQ(OB_ERR_UNEXPECTED, node.print(output, true));
  EXPECT_STREQ("prefix:\"", output.ptr());
  EXPECT_EQ(8, output.length());
  output.reuse();
  ASSERT_EQ(OB_SUCCESS, output.append("prefix:"));
  EXPECT_EQ(OB_ERR_UNEXPECTED, node.print(output, false));
  EXPECT_STREQ("prefix:", output.ptr());
  EXPECT_EQ(7, output.length());
}

TEST_F(TestJsonBinViewPrint, shared_print_invalid_date_keeps_opening_quote)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  // A binary date can be decoded even when its year cannot be printed.
  char raw[1 + sizeof(int32_t)];
  raw[0] = J_DATE_V0;
  const int32_t days = 4000000; // Beyond year 9999.
  MEMCPY(raw + 1, &days, sizeof(days));
  ObJsonBin bin(raw, sizeof(raw), &allocator);
  ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw, sizeof(raw)));
  for (bool quoted : {false, true}) {
    for (bool use_view : {false, true}) {
      SCOPED_TRACE(quoted);
      SCOPED_TRACE(use_view);
      char storage[64];
      MEMSET(storage, '#', sizeof(storage));
      ObJsonBuffer output(storage, sizeof(storage), &allocator);
      ASSERT_EQ(OB_SUCCESS, output.append("prefix:", 7, 0));
      const int ret = use_view ? view.print(output, quoted) : bin.print(output, quoted);
      EXPECT_EQ(OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR, ret);
      EXPECT_EQ(quoted ? 8 : 7, output.length());
      EXPECT_STREQ(quoted ? "prefix:\"" : "prefix:", output.ptr());
    }
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
