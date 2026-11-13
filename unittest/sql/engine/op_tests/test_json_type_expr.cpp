/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_TYPE, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_type_mysql.test
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
// JSON_TYPE — type name of the top-level JSON value.
//   mysqltest: json_type_mysql.test
// ============================================================================
class JsonTypeExprTest : public OpTestKit {};

TEST_F(JsonTypeExprTest, MysqlScalarTypes)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_type(doc)")
      .with_data({
          {R"(null)"},                       // -> NULL
          {R"(123)"},                         // -> INTEGER
          {R"(12333333333333333333)"},        // -> UNSIGNED INTEGER
          {R"({})"},                          // -> OBJECT
          {R"([])"},                          // -> ARRAY
          {R"(3.14)"},                        // -> DOUBLE
          {R"(true)"},                         // -> BOOLEAN
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {TestValue::null()},
      {"INTEGER"},
      {"UNSIGNED INTEGER"},
      {"OBJECT"},
      {"ARRAY"},
      {"DOUBLE"},
      {"BOOLEAN"},
  }));
}

TEST_F(JsonTypeExprTest, MysqlMoreScalarTypes)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_type(doc)")
      .with_data({
          {R"({"a": 2})"},                   // -> OBJECT
          {R"([1,2])"},                      // -> ARRAY
          {R"("scalar string")"},            // -> STRING
          {R"(false)"},                       // -> BOOLEAN
          {R"(1)"},                           // -> INTEGER
          {R"(-0)"},                          // -> INTEGER
          {R"(-0.0)"},                        // -> DOUBLE
          {R"(32767)"},                       // -> INTEGER
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {"OBJECT"},
      {"ARRAY"},
      {"STRING"},
      {"BOOLEAN"},
      {"INTEGER"},
      {"INTEGER"},
      {"DOUBLE"},
      {"INTEGER"},
  }));
}

// E: 'hahaha' / 'abc' / '' are not valid JSON text.
// mysqltest: SELECT JSON_TYPE('hahaha'); select json_type('abc'); select json_type('').
TEST_F(JsonTypeExprTest, MysqlInvalidJsonText)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_type(doc)")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT)
      .with_data({
          {R"(hahaha)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT, result.get_ret_code());
}

// Note: OB has two distinct "invalid json" codes. ER_INVALID_JSON_TEXT (-5411)
// fires for json_type of a non-JSON string; ER_INVALID_JSON_TEXT_IN_PARAM
// (-5412) fires for json_depth/json_length/json_extract. Both are confirmed
// against the matching .result files (ERROR 22032 with different messages).

// ============================================================================
// _enable_json_bin_view switch comparison tests.
//
// JSON_TYPE calls wrapper.json_type() and wrapper.field_type().  When
// _enable_json_bin_view is ON and the input is cast(... as json), the wrapper
// holds an ObJsonBinView and type dispatch runs on the zero-copy bin path.
// When OFF, the wrapper holds an ObJsonBin DOM.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: switch ON — various types via cast(s as json).
TEST_F(JsonTypeExprTest, BinViewSwitchOnVariousTypes)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256)")
        .with_expr("json_type(cast(s as json))")
        .with_data({
            {R"({"a": 2})"},
            {R"([1, 2, 3])"},
            {R"("scalar string")"},
            {R"(true)"},
            {R"(123)"},
            {R"(3.14)"},
            {R"(null)"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({
        {"OBJECT"},
        {"ARRAY"},
        {"STRING"},
        {"BOOLEAN"},
        {"INTEGER"},
        {"DOUBLE"},
        {"NULL"},
    }));
  }
}

// B2: switch OFF — same input as B1, expect identical results.
TEST_F(JsonTypeExprTest, BinViewSwitchOffVariousTypes)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256)")
        .with_expr("json_type(cast(s as json))")
        .with_data({
            {R"({"a": 2})"},
            {R"([1, 2, 3])"},
            {R"("scalar string")"},
            {R"(true)"},
            {R"(123)"},
            {R"(3.14)"},
            {R"(null)"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({
        {"OBJECT"},
        {"ARRAY"},
        {"STRING"},
        {"BOOLEAN"},
        {"INTEGER"},
        {"DOUBLE"},
        {"NULL"},
    }));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
