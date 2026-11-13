/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Expression-level tests for JSON_PRETTY, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_pretty_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_pretty_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// The JSON document is fed through a varchar column; OceanBase auto-converts it
// to JSON, so the pretty-printed output (with two-space indentation) is
// preserved exactly as in mysqltest. The pretty output contains literal
// newlines, so expected values use C++11 raw string literals.
//
// Only RUNTIME (eval-stage) errors are covered here. The resolve-stage error
// cases in json_pretty_mysql.test — JSON_PRETTY(EXTRACT(...)) rejected by
// calc_result_type1 via is_valid_for_json with OB_ERR_INVALID_TYPE_FOR_JSON —
// cannot be asserted through this framework: prepare_resolve() does not
// forward expected_error_ to TestSqlUtils::do_resolve(), which carries its own
// ASSERT_EQ(expect_error=0, -ret) and would trip on the resolve error. Those
// cases stay in the mysqltest suite. The eval-stage charset rejection
// (json_pretty(UNHEX(""))) is likewise left to mysqltest: reproducing a binary
// empty string requires a blob column or UNHEX nested call that the columns()
// helper does not surface cleanly, and the coverage is incidental.
//
// The mysqltest cases that use a persisted JSON column (pretty_tab with j JSON)
// are also not ported: they exercise the LOB-header path which requires a real
// table. The same coverage is achieved here through varchar auto-convert and
// through cast(... as json), matching the mysqltest behavior row-for-row.

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonPrettyExprTest : public OpTestKit {};

// ============================================================================
// Scenarios ported from mysqltest suite
//   tools/deploy/mysql_test/test_suite/json/t/json_pretty_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_pretty_mysql.result
//
// The mysqltest INSERT list (uc column) drives a 14-row SELECT JSON_PRETTY(uc);
// the same 14 rows are fed here through a varchar column. The pretty output
// is two-space indented with literal newlines.
// ============================================================================

// P1: NULL document — json_pretty(NULL) -> NULL.
TEST_F(JsonPrettyExprTest, MysqlNullDoc)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// P2: empty object and empty array — pretty-printed as-is.
TEST_F(JsonPrettyExprTest, MysqlEmptyObjectAndArray)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {"{}"},
          {"[]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"{}"}, {"[]"}}));
}

// P3: flat array of integers.
TEST_F(JsonPrettyExprTest, MysqlFlatArray)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {"[1,2,3]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([
  1,
  2,
  3
])"}}));
}

// P4: flat object of integers.
TEST_F(JsonPrettyExprTest, MysqlFlatObject)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"({"a":1,"b":2})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({
  "a": 1,
  "b": 2
})"}}));
}

// P5: nested array containing an empty object.
TEST_F(JsonPrettyExprTest, MysqlNestedEmptyObject)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"({"a":[1,{}],"b":[]})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({
  "a": [
    1,
    {}
  ],
  "b": []
})"}}));
}

// P6: deeply nested object.
TEST_F(JsonPrettyExprTest, MysqlDeeplyNestedObject)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"({"a":{"b":{"c":{"d":{"e":{"f":null}}}}}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({
  "a": {
    "b": {
      "c": {
        "d": {
          "e": {
            "f": null
          }
        }
      }
    }
  }
})"}}));
}

// P7: scalar literals — true / false / null.
TEST_F(JsonPrettyExprTest, MysqlScalarLiterals)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {"true"},
          {"false"},
          {"null"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"true"}, {"false"}, {"null"}}));
}

// P8: numeric scalars — 1 and 1.1.
TEST_F(JsonPrettyExprTest, MysqlNumericScalars)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {"1"},
          {"1.1"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1.1"}}));
}

// P9: string scalar — "hello".
TEST_F(JsonPrettyExprTest, MysqlStringScalar)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"("hello")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("hello")"}}));
}

// P10: array of two strings, one of which contains an escaped quote.
// The second element is the JSON string "\"abc\"" (a string whose value is
// "abc" with the quotes), pretty-printed with the inner quotes escaped.
TEST_F(JsonPrettyExprTest, MysqlArrayWithEscapedQuote)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(cast(doc as json))")
      .with_data({
          {R"(["abcdef", "\"abc\""])"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([
  "abcdef",
  "\"abc\""
])"}}));
}

// P11: floating point normalization — large doubles collapse to 1e15 form.
// The mysqltest SET @floating_points list is normalized by the JSON printer:
// 1000000000000000.0 -> 1e15, -1000000000000000.0 -> -1e15. The small floats
// keep their trailing .0.
TEST_F(JsonPrettyExprTest, MysqlFloatingPointNormalization)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"([0.0, -0.0, 1.0, -1.0, 3.14, 999999999999999.0, -999999999999999.0, 1000000000000000.0, -1000000000000000.0, 1e200, -1e200])"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([
  0.0,
  -0.0,
  1.0,
  -1.0,
  3.14,
  999999999999999.0,
  -999999999999999.0,
  1e15,
  -1e15,
  1e200,
  -1e200
])"}}));
}

// A4: blob column carrying an empty binary string — ensure_collation rejects
// ob_is_string_type && charset != CHARSET_UTF8MB4 with
// OB_ERR_INVALID_JSON_CHARSET. mysqltest lines 192-193:
//   select json_pretty(UNHEX(""));  -> ERROR 22032.
TEST_F(JsonPrettyExprTest, MysqlPrettyEmptyBlob)
{
  auto result = expr_unit_test()
      .columns("b blob")
      .with_expr("json_pretty(b)")
      .expect_error(common::OB_ERR_INVALID_JSON_CHARSET)
      .with_data({
          {""},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_CHARSET, result.get_ret_code());
}

// ============================================================================
// _enable_json_bin_view switch comparison tests.
//
// JSON_PRETTY calls ObJsonWrapper::print with is_pretty=true.  The actual
// path depends on input type: varchar input is auto-converted to JSON_TREE
// (get_json_internal_type returns JSON_TREE for non-ObJsonType), so the
// switch has no effect on B1/B2.  cast(doc as json) produces ObJsonType,
// so get_json_internal_type returns JSON_BIN; with the switch ON the wrapper
// holds an ObJsonBinView (print runs print_container_impl), and with the
// switch OFF it holds an ObJsonBin DOM (print delegates to ObIJsonBase::print
// in ob_json_base.cpp).  B3/B4 exercise both switch states on the JSON_BIN
// path and assert the results match byte-for-byte.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: varchar input — auto-convert to JSON_TREE path. Baseline for B3/B4
// (cast as json -> JSON_BIN path).
TEST_F(JsonPrettyExprTest, AutoConvertFlatObject)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"({"a":1,"b":2})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({
  "a": 1,
  "b": 2
})"}}));
}

// B2: varchar input — auto-convert to JSON_TREE path. Baseline for B3/B4
// (cast as json -> JSON_BIN path).
TEST_F(JsonPrettyExprTest, AutoConvertDeeplyNested)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256)")
      .with_expr("json_pretty(doc)")
      .with_data({
          {R"({"a":{"b":{"c":{"d":{"e":{"f":null}}}}}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({
  "a": {
    "b": {
      "c": {
        "d": {
          "e": {
            "f": null
          }
        }
      }
    }
  }
})"}}));
}

// B3: cast(doc as json) — JSON_BIN path, switch ON -> ObJsonBinView.
TEST_F(JsonPrettyExprTest, BinViewSwitchOnCastAsJsonArray)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(256)")
        .with_expr("json_pretty(cast(doc as json))")
        .with_data({
            {R"(["abcdef", "\"abc\""])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"([
  "abcdef",
  "\"abc\""
])"}}));
  }
}

// B4: cast(doc as json) — JSON_BIN path, switch OFF -> ObJsonBin DOM. Compare with B3.
TEST_F(JsonPrettyExprTest, BinViewSwitchOffCastAsJsonArray)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(256)")
        .with_expr("json_pretty(cast(doc as json))")
        .with_data({
            {R"(["abcdef", "\"abc\""])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"([
  "abcdef",
  "\"abc\""
])"}}));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
