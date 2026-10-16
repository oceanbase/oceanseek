/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_DEPTH, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_depth_mysql.test
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
// JSON_DEPTH — depth of a JSON document (null/empty/scalar=1, +1 per nesting).
//   mysqltest: json_depth_mysql.test
// ============================================================================
class JsonDepthExprTest : public OpTestKit {};

// NULL document -> NULL.
TEST_F(JsonDepthExprTest, MysqlNullDocument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({{TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Empty array / empty object / JSON null scalar all have depth 1.
TEST_F(JsonDepthExprTest, MysqlEmptyContainersAndJsonNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({
          {R"([])"},
          {R"({})"},
          {R"(null)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}}));
}

// Scalars (string/number/bool) have depth 1.
TEST_F(JsonDepthExprTest, MysqlScalarDepthOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({
          {R"("abc")"},
          {R"(3)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}}));
}

// Nested arrays: '[[2], 3, [[[4]]]]' -> 5.
TEST_F(JsonDepthExprTest, MysqlNestedArrayDepth)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({{R"([[2], 3, [[[4]]]])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"5"}}));
}

// Nested object: '{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}' -> 6.
TEST_F(JsonDepthExprTest, MysqlNestedObjectDepth)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({{R"({"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"6"}}));
}

// Mixed scalars + one level of container -> depth 2.
TEST_F(JsonDepthExprTest, MysqlDepthTwo)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({
          {R"({ "a" : true, "b" : false, "c" : null })"},
          {R"([ "a", true, "b" , false, "c" , null ])"},
          {R"({ "a" : true, "b" : {}, "c" : null })"},
          {R"([ "a", true, "b" , {}, "c" , null ])"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"2"}, {"2"}, {"2"}, {"2"}}));
}

// One nested object inside a container -> depth 3.
TEST_F(JsonDepthExprTest, MysqlDepthThree)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_depth(doc)")
      .with_data({
          {R"({ "a" : true, "b" : { "e" : false }, "c" : null })"},
          {R"([ "a", true, "b" , { "e" : false }, "c" , null ])"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"3"}, {"3"}}));
}

// E: invalid JSON text in the document argument — 'abc' is not a JSON document.
// mysqltest: SELECT JSON_DEPTH( 'abc' ). Fires at eval when the varchar value is
// parsed as a JSON document.
TEST_F(JsonDepthExprTest, MysqlInvalidJsonText)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_depth(doc)")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM)
      .with_data({{R"(abc)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM, result.get_ret_code());
}

// E: empty string is not a valid JSON document.
// mysqltest: SELECT JSON_DEPTH("").
TEST_F(JsonDepthExprTest, MysqlEmptyStringInvalid)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_depth(doc)")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM)
      .with_data({{R"()"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM, result.get_ret_code());
}

// E: malformed array (missing closing bracket).
// mysqltest: SELECT JSON_DEPTH('[ "a", true, "b" , { "e" : false }, "c" , null').
TEST_F(JsonDepthExprTest, MysqlMalformedArray)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128)")
      .with_expr("json_depth(doc)")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM)
      .with_data({{R"([ "a", true, "b" , { "e" : false }, "c" , null)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM, result.get_ret_code());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
