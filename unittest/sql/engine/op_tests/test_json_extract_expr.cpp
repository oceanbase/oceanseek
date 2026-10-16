/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_EXTRACT, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_extract_no_table_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/  (and r/mysql/).
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// The JSON document is fed through a varchar column; OceanBase auto-converts it
// to JSON, so the JSON value (number vs string vs array) is preserved exactly
// as in mysqltest.
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors (wrong param count, bare non-string literal, parse
// errors) cannot be asserted through this framework: prepare_resolve() does not
// forward expected_error_ to TestSqlUtils::do_resolve(), which carries its own
// ASSERT_EQ(expect_error=0, -ret). Those mysqltest cases stay in the mysqltest
// suite; the error cases below fire when the column value / path is parsed at
// eval time and surface through result.get_ret_code().

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonExtractExprTest : public OpTestKit {};

// ============================================================================
// Scenarios ported from mysqltest suite
//   tools/deploy/mysql_test/test_suite/json/t/json_extract_no_table_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/json_extract_no_table_mysql.result
//
// These are pure-expression cases (no table DML), so they map naturally onto
// expr_unit_test(). The first argument is a JSON document; in mysqltest it is
// a string literal that OceanBase auto-converts to JSON, so here we feed the
// same JSON text through a varchar column and let JSON_EXTRACT coerce it.
// ============================================================================

// M1: quoted member keys — $.a and $."a" are equivalent.
TEST_F(JsonExtractExprTest, MysqlQuotedKey)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.a')")
      .with_data({
          {R"({ "a": 1})"},                 // $.a      -> 1
          {R"({ "a": 1})"},                  // $.a      -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}}));
}

// M2: path with a quoted key containing a dot/space — only $."b.c" / $."one potato".
TEST_F(JsonExtractExprTest, MysqlQuotedKeyWithSpecialChars)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.\"one potato\"')")
      .with_data({
          {R"({ "a": [ [ 3, 2 ], [ { "c" : "d" }, 1 ] ], "b": { "c" : 6 }, "one potato": 7, "b.c" : 8 })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"7"}}));
}

// M3: dotted key path — $.b.c vs $."b.c" select different members.
TEST_F(JsonExtractExprTest, MysqlDottedKey)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.\"b.c\"')")
      .with_data({
          {R"({ "a": [ [ 3, 2 ], [ { "c" : "d" }, 1 ] ], "b": { "c" : 6 }, "one potato": 7, "b.c" : 8 })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"8"}}));
}

// M4: nested array indexing — $.a[0], $.a[0][1], $.a[1], $.a[1][0], $.a[1][0].c, $.b.c
TEST_F(JsonExtractExprTest, MysqlArrayIndexing)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.a[0]')")
      .with_data({
          {R"({ "a": [ [ 3, 2 ], [ { "c" : "d" }, 1 ] ], "b": { "c" : 6 }, "one potato": 7, "b.c" : 8 })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[3, 2]"}}));
}

// M5: array index then member — $.a[1][0].c returns the JSON string "d".
TEST_F(JsonExtractExprTest, MysqlArrayIndexThenMember)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.a[1][0].c')")
      .with_data({
          {R"({ "a": [ [ 3, 2 ], [ { "c" : "d" }, 1 ] ], "b": { "c" : 6 }, "one potato": 7, "b.c" : 8 })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("d")"}}));
}

// M6: wildcard $.* over an object — returns an array of all member values.
TEST_F(JsonExtractExprTest, MysqlWildcardAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.*')")
      .with_data({
          {R"({"a": 1, "b": {"e": "foo", "b": 3}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([1, {"b": 3, "e": "foo"}])"}}));
}

// M7: multiple paths in one call — returns a JSON array of the matches.
TEST_F(JsonExtractExprTest, MysqlMultiplePaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.a', '$.b.e')")
      .with_data({
          {R"({"a": 1, "b": {"e": "foo", "b": 3}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([1, "foo"])"}}));
}

// M8: array element access by index — $.b[2] of [1,2,3] is 3.
TEST_F(JsonExtractExprTest, MysqlArrayElement)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.b[2]')")
      .with_data({
          {R"({"a": 1, "b": [1,2,3]})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"3"}}));
}

// M9: NULL document — json_extract(NULL, path) -> NULL.
TEST_F(JsonExtractExprTest, MysqlNullDocument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.b')")
      .with_data({
          {TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M10: any NULL path argument makes the whole call return NULL.
TEST_F(JsonExtractExprTest, MysqlNullPathArgument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.a', NULL)")
      .with_data({
          {R"({"a": 1, "b": {"e": "foo", "b": 3}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M11: a valid path that identifies no value returns NULL.
TEST_F(JsonExtractExprTest, MysqlAbsentPathReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.b[ 1 ].c[ 1 ]')")
      .with_data({
          {R"({ "a" : "foo", "b" : [ true, { "c" : 123 } ] })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M12: auto-wrapping — extracting an array index off a scalar wraps it as [scalar].
TEST_F(JsonExtractExprTest, MysqlAutoWrapScalar)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.b[ 1 ].c[ 0 ]')")
      .with_data({
          {R"({ "a" : "foo", "b" : [ true, { "c" : 123 } ] })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"123"}}));
}

// M13: duplicate-key normalization — last value wins (456, not 123).
TEST_F(JsonExtractExprTest, MysqlDuplicateKeyNormalization)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.b[ 1 ].c')")
      .with_data({
          {R"({ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] })"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"456"}}));
}

// M14: wildcard over array members — $[*].a collects matches into an array.
TEST_F(JsonExtractExprTest, MysqlWildcardArrayMembers)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$[*].a')")
      .with_data({
          {R"([ { "a": 1 }, { "a": 2 } ])"},    // -> [1, 2]
          {R"([ { "a": 1 }, { "b": 2 } ])"},    // -> [1]
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[1, 2]"}, {"[1]"}}));
}

// M15: extracting from a scalar document — '$.width' on '1' returns NULL
// (numeric scalar is not auto-wrapped into an object member).
TEST_F(JsonExtractExprTest, MysqlScalarDocumentNoMember)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_extract(doc, '$.width')")
      .with_data({
          {R"(1)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// ============================================================================
// Error scenarios ported from mysqltest
//   tools/deploy/mysql_test/test_suite/json/t/json_extract_no_table_mysql.test
// Expected error codes are confirmed against the matching
//   tools/deploy/mysql_test/test_suite/json/r/json_extract_no_table_mysql.result
//
// Only RUNTIME (eval-stage) errors are covered here. The framework's
// prepare_resolve() does not forward expected_error_ to the underlying
// TestSqlUtils::do_resolve(), which carries its own ASSERT_EQ(expect_error=0,
// -ret); a resolve/type-deduction-stage error (wrong param count
// OB_ERR_PARAM_SIZE, or invalid doc type OB_ERR_INVALID_TYPE_FOR_JSON) trips
// that assertion and, because do_resolve is void and does not propagate the
// error, leaves the pipeline running on a bad state (the latter even crashes
// get_vec_value_tc(ObMaxType)). Those two mysqltest cases are therefore left
// to the mysqltest suite. The cases below fire when the column value / path is
// parsed at eval time, so they surface through result.get_ret_code() just like
// a normal run, and .expect_error(CODE) is the assertion.
// ============================================================================

// E3: invalid JSON text in the document argument — '{ "a" : [ }' is malformed.
// mysqltest line 93: JSON_EXTRACT('{ "a" : [ }', '$.b[ 1 ].c').
// Fires at runtime: the varchar column value is parsed as a JSON document
// during eval.
TEST_F(JsonExtractExprTest, MysqlInvalidJsonTextInDoc)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_extract(doc, '$.b[ 1 ].c')")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM)
      .with_data({{R"({ "a" : [ })"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM, result.get_ret_code());
}

// E4: invalid JSON path — trailing dot '$.b[ 1 ].' is not a legal path.
// mysqltest line 98: JSON_EXTRACT('{ "a" : "foo", ... }', '$.b[ 1 ].').
// Fires at runtime: the path string is parsed into an ObJsonPath during eval.
TEST_F(JsonExtractExprTest, MysqlInvalidJsonPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_extract(doc, '$.b[ 1 ].')")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({{R"({ "a" : "foo", "b" : [ true, { "c" : 123 } ] })"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
