/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_LENGTH, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_length_mysql.test
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

// ============================================================================
// JSON_LENGTH — number of elements in an array / members in an object, 1 for a
// scalar, NULL for a NULL document. Optional path restricts the target.
//   mysqltest: json_length_mysql.test
// ============================================================================
class JsonLengthExprTest : public OpTestKit {};

TEST_F(JsonLengthExprTest, MysqlNullDocument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc)")
      .with_data({{TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

TEST_F(JsonLengthExprTest, MysqlScalarsAndContainers)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc)")
      .with_data({
          {R"(1)"},                                    // -> 1
          {R"("abc")"},                                // -> 1
          {R"(true)"},                                  // -> 1
          {R"(false)"},                                 // -> 1
          {R"(null)"},                                  // -> 1
          {R"({})"},                                   // -> 0
          {R"({ "a" : 100, "b" : 200 })"},             // -> 2
          {R"({ "a" : 100, "b" : [ 300, 400, 500 ] })"},// -> 2
          {R"([])"},                                   // -> 0
          {R"([ null, "foo", true, 1.1 ])"},           // -> 4
          {R"([ null, "foo", true, { "a" : "b", "c" : "d" } ])"}, // -> 4
          {R"("foo")"},                                // -> 1
          {R"(1.2)"},                                  // -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {"1"}, {"1"}, {"1"}, {"1"}, {"1"},
      {"0"}, {"2"}, {"2"},
      {"0"}, {"4"}, {"4"},
      {"1"}, {"1"},
  }));
}

// json_length with a path: selects the sub-document and takes its length.
TEST_F(JsonLengthExprTest, MysqlWithPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc, '$[1]')")
      .with_data({
          {R"([ 1, [ 2, 3, 4 ], 5 ])"},     // $[1] = [2,3,4] -> 3
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"3"}}));
}

// json_length with a path: auto-wrap scalar ($[2][0] on scalar 5 -> [5] -> 1).
TEST_F(JsonLengthExprTest, MysqlPathAutoWrapScalar)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc, '$[2][0]')")
      .with_data({
          {R"([ 1, [ 2, 3, 4 ], 5 ])"},     // $[2]=5, [0] auto-wraps -> [5] -> 1
          {R"([ 1, [ 2, 3, 4 ], {"a": 1} ])"}, // $[2]={"a":1}, [0] auto-wraps -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}}));
}

// Non-existent path -> NULL (not an error).
TEST_F(JsonLengthExprTest, MysqlNonExistentPathReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc, '$[2][1]')")
      .with_data({{R"([ 1, [ 2, 3, 4 ], 5 ])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Wildcard / ellipsis path that matches nothing on a scalar -> NULL.
TEST_F(JsonLengthExprTest, MysqlWildcardPathOnScalarReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_length(doc, '$.*')")
      .with_data({{R"(true)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// E: invalid JSON text in the document.
// mysqltest: select json_length( 'abc' ); select json_length( "" ).
TEST_F(JsonLengthExprTest, MysqlInvalidJsonText)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_length(doc)")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT)
      .with_data({{R"(abc)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT, result.get_ret_code());
}

// E: invalid JSON path expression.
// mysqltest: select json_length( 'true', 'c$' );
//            select json_length( '{ "foo" : [ true, false ] }', '$.foo[bar]' ).
TEST_F(JsonLengthExprTest, MysqlInvalidJsonPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_length(doc, 'c$')")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({{R"(true)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
