/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Expression-level tests for JSON_UNQUOTE, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_unquote_quote_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_unquote_quote_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test()
// via the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed. The implementation under test is
// ObExprJsonUnquote (src/sql/engine/expr/ob_expr_json_unquote.cpp).
//
// The string argument is fed through a varchar column; OceanBase auto-converts
// it as needed, so the JSON-unquoted output is preserved exactly as in
// mysqltest. JSON_UNQUOTE returns the raw SQL string (abc) when the input is a
// JSON string scalar, and returns the input unchanged for objects/arrays.
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors (wrong param count OB_ERR_PARAM_SIZE fired by the
// function arity check, unknown-column resolve errors) cannot be asserted
// through this framework: prepare_resolve() does not forward expected_error_
// to TestSqlUtils::do_resolve(), which carries its own ASSERT_EQ(expect_error=0,
// -ret). Those mysqltest cases stay in the mysqltest suite. The eval-stage
// type rejection (JSON_UNQUOTE(123) / JSON_UNQUOTE(CURDATE())) fires in calc()
// at eval time with OB_ERR_INVALID_TYPE_FOR_ARGUMENT, so those are ported here
// using int / date columns. The eval-stage charset rejection
// (JSON_UNQUOTE(UNHEX(""))) is covered here through both the UNHEX nested call
// and a blob-column fallback (ensure_collation rejection).
//
// The mysqltest cases that need a persisted JSON column (json_test_quote_unquote
// and the t1 table with escape sequences) are not ported: they exercise the
// LOB-header / JSON_BIN path which requires a real table. The same escape
// coverage is achieved here through varchar auto-convert and through
// cast(... as json) subexpressions.
//
// Two round-trip cases (U7, U11) nest json_quote inside json_unquote; the
// outermost function is json_unquote and the test asserts unquote's ability to
// undo quote's wrapping, so they belong here. There is no
// json_quote(json_unquote(...)) case (quote wrapping an unquoted value) in the
// mysqltest source.

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonUnquoteExprTest : public OpTestKit {};

// ============================================================================
// JSON_UNQUOTE scenarios ported from
//   tools/deploy/mysql_test/test_suite/json/t/json_unquote_quote_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_unquote_quote_mysql.result
// ============================================================================

// U1: NULL argument — json_unquote(NULL) -> NULL.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteNull)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// U2: unquoted string passes through unchanged — json_unquote('abc') -> abc.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteNoQuotes)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// U3: quoted string is unwrapped — json_unquote('"abc"') -> abc.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteQuoted)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {R"("abc")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// U4: lone quote is not a complete quoted string — json_unquote('"') -> ".
TEST_F(JsonUnquoteExprTest, MysqlUnquoteLoneQuote)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {R"(")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(")"}}));
}

// U5: empty quoted string — json_unquote('""') -> empty string.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteEmptyQuoted)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {R"("")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{""}}));
}

// U6: trailing data after the closing quote leaves the string unchanged —
// json_unquote('"" ') -> "" (the space breaks the quote-strip).
TEST_F(JsonUnquoteExprTest, MysqlUnquoteTrailingData)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {R"("" )"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("" )"}}));
}

// U7: round trip — json_unquote(cast(json_quote('abc') as json)) -> abc.
// The outermost function is json_unquote; json_quote only constructs the input.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteRoundTrip)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(cast(json_quote(s) as json))")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// U8: object is not a quoted string — json_unquote(cast('{"abc": "foo"}' as json))
// returns the object unchanged.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteObjectUnchanged)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(cast(s as json))")
      .with_data({
          {R"({"abc": "foo"})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"abc": "foo"})"}}));
}

// U9: array is not a quoted string — json_unquote(cast('["a","b","c"]' as json))
// returns the array unchanged.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteArrayUnchanged)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s), json_unquote(cast(s as json))")
      .with_data({
          {R"(["a", "b", "c"])"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["a", "b", "c"])", R"(["a", "b", "c"])"}}));
}

// U10: extract then unquote — json_unquote(json_extract(cast(... as json), '$.abc'))
// unwraps the JSON string scalar "foo" to the SQL string foo.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteExtractedString)
{
  auto result = expr_unit_test()
      .columns("s varchar(64), p varchar(16)")
      .with_expr("json_unquote(json_extract(cast(s as json), p))")
      .with_data({
          {R"({"abc": "foo"})", "$.abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"foo"}}));
}

// U11: deeply nested quote/unquote round trip — three quotes then three unquotes
// collapses back to the plain string. The outermost functions are json_unquote.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteDeepRoundTrip)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(json_unquote(json_unquote(json_quote(json_quote(json_quote(s))))))")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// U12: datetime cast to JSON then unquoted — the JSON datetime literal
// "2015-01-15 23:24:25.000000" is unwrapped to the bare timestamp string.
// The datetime is produced by casting a string literal (the data-source layer
// only accepts int/double for datetime columns, so we let SQL parse the value).
TEST_F(JsonUnquoteExprTest, MysqlUnquoteDatetimeJson)
{
  auto result = expr_unit_test()
      .columns("s varchar(32)")
      .with_expr("json_unquote(cast(cast(s as datetime) as json))")
      .with_data({
          {"2015-01-15 23:24:25"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"2015-01-15 23:24:25.000000"}}));
}

// U13: cast JSON string through CHAR then unquote — same result as direct
// unquote of '"abc"'.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteCastCharJson)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(cast(cast(s as json) as char))")
      .with_data({
          {R"("abc")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// U14: leading quote without a closing quote — json_unquote('"abc') -> "abc.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteLeadingQuoteOnly)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {R"("abc)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("abc)"}}));
}

// U15: integer argument rejected at eval — json_unquote(123) -> error.
// mysqltest line 92: --error ER_INCORRECT_TYPE; select json_unquote(123).
TEST_F(JsonUnquoteExprTest, MysqlUnquoteIntRejected)
{
  auto result = expr_unit_test()
      .columns("n int")
      .with_expr("json_unquote(n)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {123},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// U16: date argument rejected at eval — json_unquote(CURDATE()) -> error.
// mysqltest line 75: --error ER_INCORRECT_TYPE; select json_unquote(CURDATE()).
TEST_F(JsonUnquoteExprTest, MysqlUnquoteDateRejected)
{
  auto result = expr_unit_test()
      .columns("d date")
      .with_expr("json_unquote(d)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {"2021-11-11"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// B3: single-character non-quote string — length<2 fast path of
// ObExprJsonUnquote::calc (ob_expr_json_unquote.cpp:84). A 1-byte string whose
// only byte is not '"' takes the "copy original string" branch verbatim, never
// reaching the JSON parser. Distinct from U4 ('"') which is also length 1 but
// whose first byte IS '"': that one also short-circuits, but via the
// j_str[length-1] != '"' clause (length 1 means j_str[0]==j_str[length-1]).
TEST_F(JsonUnquoteExprTest, MysqlUnquoteShortFastPath)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(s)")
      .with_data({
          {"a"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"a"}}));
}

// A1: json_unquote(convert(... using utf8)) — exercises the charset_type !=
// CHARSET_UTF8MB4 branch of ObExprJsonUnquote::calc_result_type1
// (ob_expr_json_unquote.cpp:46-49), which sets calc_collation_type to
// CS_TYPE_UTF8MB4_BIN for non-utf8mb4 input.
// mysqltest lines 65-66.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteConvertUtf8)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(convert(s using utf8))")
      .with_data({
          {R"("abc")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// A1: json_unquote(convert(... using utf8mb4)) — same charset branch.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteConvertUtf8mb4)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_unquote(convert(s using utf8mb4))")
      .with_data({
          {R"("abc")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"abc"}}));
}

// A2: same as MysqlQuoteUnhexEmpty but for JSON_UNQUOTE.
// mysqltest lines 351-352: select JSON_UNQUOTE(UNHEX("")); -> ERROR 22032.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteUnhexEmpty)
{
  auto result = expr_unit_test()
      .columns("s varchar(16)")
      .with_expr("json_unquote(unhex(s))")
      .expect_error(common::OB_ERR_INVALID_JSON_CHARSET)
      .with_data({
          {""},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_CHARSET, result.get_ret_code());
}

// A2: blob-column fallback for JSON_UNQUOTE — covers
// ob_expr_json_unquote.cpp:75 ensure_collation rejection.
TEST_F(JsonUnquoteExprTest, MysqlUnquoteEmptyBlob)
{
  auto result = expr_unit_test()
      .columns("b blob")
      .with_expr("json_unquote(b)")
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
// JSON_UNQUOTE calls ObJsonWrapper::print with is_quoted=false.  When the
// switch is ON (tenant config "true"), the wrapper holds an ObJsonBinView and
// print runs on the zero-copy bin path (print_scalar_impl /
// print_container_impl).  When OFF ("false"), the wrapper holds an ObJsonBin
// DOM and print delegates to ObIJsonBase::print (ob_json_base.cpp).  These
// tests exercise both switch states and assert the results match
// byte-for-byte.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: switch ON — object unquote.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOnUnquoteObject)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64)")
        .with_expr("json_unquote(cast(s as json))")
        .with_data({
            {R"({"abc": "foo"})"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"({"abc": "foo"})"}}));
  }
}

// B2: switch OFF — same input as B1, expect identical results.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOffUnquoteObject)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64)")
        .with_expr("json_unquote(cast(s as json))")
        .with_data({
            {R"({"abc": "foo"})"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"({"abc": "foo"})"}}));
  }
}

// B3: switch ON — array unquote.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOnUnquoteArray)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64)")
        .with_expr("json_unquote(cast(s as json))")
        .with_data({
            {R"(["a", "b", "c"])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"(["a", "b", "c"])"}}));
  }
}

// B4: switch OFF — same input as B3, expect identical results.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOffUnquoteArray)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64)")
        .with_expr("json_unquote(cast(s as json))")
        .with_data({
            {R"(["a", "b", "c"])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"(["a", "b", "c"])"}}));
  }
}

// B5: switch ON — extract then unquote.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOnUnquoteExtract)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64), p varchar(16)")
        .with_expr("json_unquote(json_extract(cast(s as json), p))")
        .with_data({
            {R"({"abc": "foo"})", "$.abc"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"foo"}}));
  }
}

// B6: switch OFF — same input as B5, expect identical results.
TEST_F(JsonUnquoteExprTest, BinViewSwitchOffUnquoteExtract)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(64), p varchar(16)")
        .with_expr("json_unquote(json_extract(cast(s as json), p))")
        .with_data({
            {R"({"abc": "foo"})", "$.abc"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"foo"}}));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
