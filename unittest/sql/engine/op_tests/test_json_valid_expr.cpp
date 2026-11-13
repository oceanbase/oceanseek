/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_VALID, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_valid_mysql.test
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
// JSON_VALID — 1 if the argument is a valid JSON document, 0 if not, NULL if
// the argument is NULL. Unlike json_depth/type/length, json_valid does NOT
// raise OB_ERR_INVALID_TYPE_FOR_JSON on non-string types — it returns 0.
//   mysqltest: json_valid_mysql.test
// ============================================================================
class JsonValidExprTest : public OpTestKit {};

// NULL argument -> NULL.
TEST_F(JsonValidExprTest, MysqlNullArgument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_valid(doc)")
      .with_data({{TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Valid JSON documents -> 1.
TEST_F(JsonValidExprTest, MysqlValidDocuments)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_valid(doc)")
      .with_data({
          {R"([123])"},
          {R"(123)"},
          {R"(-123)"},
          {R"(5000000000)"},
          {R"(-5000000000)"},
          {R"(1.23)"},
          {R"("123")"},
          {R"(true)"},
          {R"(false)"},
          {R"(null)"},
          {R"({"address": "Trondheim"})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"},
      {"1"}, {"1"}, {"1"}, {"1"}, {"1"},
  }));
}

// Invalid JSON documents -> 0 (NOT an error).
TEST_F(JsonValidExprTest, MysqlInvalidDocuments)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_valid(doc)")
      .with_data({
          {R"([123)"},        // missing closing bracket
          {R"()"},            // empty string
          {R"(12 3)"},        // space inside number
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}, {"0"}, {"0"}}));
}

// A non-string/non-JSON typed argument returns 0 (json_valid is lenient: no
// OB_ERR_INVALID_TYPE_FOR_JSON). mysqltest: select JSON_VALID(d) from utf8_t
// where d is a TIMESTAMP column returns 0. Here an int column stands in for a
// non-string type.
TEST_F(JsonValidExprTest, MysqlNonStringTypeReturnsZero)
{
  auto result = expr_unit_test()
      .columns("c int")
      .with_expr("json_valid(c)")
      .with_data({{1}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}}));
}

// ============================================================================
// _enable_json_bin_view switch comparison tests.
//
// JSON_VALID with a JSON type input (cast(s as json)) calls
// common::get_json_wrapper with ObJsonInType::JSON_BIN, constructing either
// an ObJsonBinView (switch ON) or ObJsonBin DOM (switch OFF).  If
// construction succeeds, json_valid returns 1.  The cast already validates
// the JSON text, so these tests focus on verifying the wrapper construction
// path is consistent between bin_view and DOM.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: switch ON — validate JSON binary via cast(s as json).
TEST_F(JsonValidExprTest, BinViewSwitchOnJsonBinary)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256)")
        .with_expr("json_valid(cast(s as json))")
        .with_data({
            {R"({"a": 1, "b": [2, 3]})"},
            {R"([1, 2, 3])"},
            {R"("hello")"},
            {R"(42)"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}, {"1"}}));
  }
}

// B2: switch OFF — same input as B1, expect identical results.
TEST_F(JsonValidExprTest, BinViewSwitchOffJsonBinary)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256)")
        .with_expr("json_valid(cast(s as json))")
        .with_data({
            {R"({"a": 1, "b": [2, 3]})"},
            {R"([1, 2, 3])"},
            {R"("hello")"},
            {R"(42)"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}, {"1"}}));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
