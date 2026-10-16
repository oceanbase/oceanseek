/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_VALUE, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_value_mysql.test
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
// JSON_VALUE — extract a scalar value at a path, optionally cast with a
// RETURNING clause. Without RETURNING the result is a varchar; with a typed
// RETURNING clause the value is converted and may be NULL on overflow/error.
// The doc is fed through a varchar column and auto-converted to JSON.
//   mysqltest: json_value_mysql.test
// ============================================================================
class JsonValueExprTest : public OpTestKit {};

// Default (no RETURNING) -> varchar of the scalar.
TEST_F(JsonValueExprTest, MysqlDefaultReturning)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data')")
      .with_data({
          {R"({"data": "asdf"})"},     // -> asdf
          {R"({"data": 123.45})"},     // -> 123.45
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"asdf"}, {"123.45"}}));
}

// The whole document at '$'.
TEST_F(JsonValueExprTest, MysqlRootPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$')")
      .with_data({
          {R"([1,2,3])"},             // -> [1, 2, 3]
          {R"({"a":"b"})"},          // -> {"a": "b"}
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[1, 2, 3]"}, {R"({"a": "b"})"}}));
}

// RETURNING CHAR / DECIMAL.
TEST_F(JsonValueExprTest, MysqlReturningCharAndDecimal)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning char)")
      .with_data({
          {R"({"data": "asdf"})"},     // -> asdf
          {R"({"data": 123.45})"},    // -> 123.45
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"asdf"}, {"123.45"}}));
}

// RETURNING DECIMAL truncates to integer part.
TEST_F(JsonValueExprTest, MysqlReturningDecimal)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning decimal)")
      .with_data({{R"({"data": 123.45})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"123"}}));
}

// RETURNING DECIMAL(6,4) overflows the 6,4 precision -> NULL.
TEST_F(JsonValueExprTest, MysqlReturningDecimalOverflow)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning decimal(6,4))")
      .with_data({{R"({"data": 123.45})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// RETURNING SIGNED / UNSIGNED (truncates toward zero; negative unsigned -> NULL).
TEST_F(JsonValueExprTest, MysqlReturningSignedUnsigned)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning signed)")
      .with_data({
          {R"({"data": 123.45})"},    // -> 123
          {R"({"data": -123.45})"},   // -> -123
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"123"}, {"-123"}}));
}

TEST_F(JsonValueExprTest, MysqlReturningUnsignedNegativeIsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning unsigned)")
      .with_data({
          {R"({"data": 123.45})"},    // -> 123
          {R"({"data": -123.45})"},   // -> NULL
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"123"}, {TestValue::null()}}));
}

// RETURNING DOUBLE.
TEST_F(JsonValueExprTest, MysqlReturningDouble)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning double)")
      .with_data({{R"({"data": 123.45})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"123.45"}}));
}

// RETURNING JSON passes the sub-document through as JSON text.
TEST_F(JsonValueExprTest, MysqlReturningJson)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning json)")
      .with_data({
          {R"({"data": [123]})"},        // -> [123]
          {R"({"data": {"val":123}})"},  // -> {"val": 123}
          {R"({"data": "123"})"},        // -> "123" (JSON string, quotes kept)
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[123]"}, {R"({"val": 123})"}, {R"("123")"}}));
}

// RETURNING SIGNED over a non-scalar (array/object) -> NULL (cannot convert).
TEST_F(JsonValueExprTest, MysqlReturningSignedOverNonScalarIsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning signed)")
      .with_data({
          {R"({"data": [123]})"},        // -> NULL
          {R"({"data": {"x":"y"}})"},     // -> NULL
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}, {TestValue::null()}}));
}

// DEFAULT <value> ON ERROR supplies the default when conversion fails.
TEST_F(JsonValueExprTest, MysqlDefaultOnError)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data' returning signed default 456 on error)")
      .with_data({{R"({"data": [123]})"}})   // [123] cannot -> SIGNED -> 456
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"456"}}));
}

// NULL document -> NULL.
TEST_F(JsonValueExprTest, MysqlNullDocument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_value(doc, '$.data')")
      .with_data({{TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Invalid JSON document with NO ON ERROR clause: json_value swallows the
// doc-parse error and returns NULL (its default ON EMPTY / ON ERROR behavior is
// NULL), unlike json_depth/json_length/json_extract which raise
// OB_ERR_INVALID_JSON_TEXT(_IN_PARAM). Confirmed against OB behaviour.
TEST_F(JsonValueExprTest, MysqlInvalidJsonDocReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_value(doc, '$.x')")
      .with_data({{R"(abc)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
