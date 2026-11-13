/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Expression-level tests for JSON_QUOTE, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_unquote_quote_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_unquote_quote_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test()
// via the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed. The implementation under test is
// ObExprJsonQuote (src/sql/engine/expr/ob_expr_json_quote.cpp).
//
// The string argument is fed through a varchar column; OceanBase auto-converts
// it as needed, so the JSON-quoted output is preserved exactly as in mysqltest.
// JSON_QUOTE returns a JSON string scalar ("abc").
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors (wrong param count OB_ERR_PARAM_SIZE fired by the
// function arity check, unknown-column resolve errors) cannot be asserted
// through this framework: prepare_resolve() does not forward expected_error_
// to TestSqlUtils::do_resolve(), which carries its own ASSERT_EQ(expect_error=0,
// -ret). Those mysqltest cases stay in the mysqltest suite. The eval-stage
// type rejection (JSON_QUOTE(123) / JSON_QUOTE(CURDATE())) fires in calc()
// at eval time with OB_ERR_INVALID_TYPE_FOR_ARGUMENT, so those are ported here
// using int / double / date columns. The eval-stage charset rejection
// (JSON_QUOTE(UNHEX(""))) is covered here through both the UNHEX nested call
// and a blob-column fallback (ensure_collation rejection).
//
// The mysqltest cases that need a persisted JSON column (json_test_quote_unquote
// and the t1 table with escape sequences) are also not ported: they exercise
// the LOB-header / JSON_BIN path which requires a real table. The same escape
// coverage is achieved here through varchar auto-convert and through
// cast(... as json) subexpressions.
//
// JSON_UNQUOTE cases from the same mysqltest file live in
// test_json_unquote_expr.cpp. Two round-trip cases there (U7, U11) nest
// json_quote inside json_unquote; they belong with unquote because the
// outermost function is json_unquote. There is no
// json_quote(json_unquote(...)) case in the mysqltest source.

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonQuoteExprTest : public OpTestKit {};

// ============================================================================
// JSON_QUOTE scenarios ported from
//   tools/deploy/mysql_test/test_suite/json/t/json_unquote_quote_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_unquote_quote_mysql.result
// ============================================================================

// Q1: NULL argument — json_quote(NULL) -> NULL.
TEST_F(JsonQuoteExprTest, MysqlQuoteNull)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(s)")
      .with_data({
          {TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Q2: plain string — json_quote('abc') -> the JSON string "abc".
TEST_F(JsonQuoteExprTest, MysqlQuotePlainString)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(s)")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("abc")"}}));
}

// Q3: string containing a quote — json_quote('"') -> "\"".
TEST_F(JsonQuoteExprTest, MysqlQuoteQuoteChar)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(s)")
      .with_data({
          {R"(")"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("\"")"}}));
}

// Q4: empty string argument — json_quote('') -> "".
TEST_F(JsonQuoteExprTest, MysqlQuoteEmptyString)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(s)")
      .with_data({
          {""},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("")"}}));
}

// Q5: deep quoting — json_quote(json_quote(json_quote('abc'))).
TEST_F(JsonQuoteExprTest, MysqlQuoteDeep)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(json_quote(json_quote(s)))")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("\"\\\"abc\\\"\"")"}}));
}

// Q6: null propagation through mathematical functions — json_quote(ACOS(null))
// etc. all return NULL because the inner function yields NULL.
TEST_F(JsonQuoteExprTest, MysqlQuoteNullFromMathFns)
{
  auto result = expr_unit_test()
      .columns("a double, b double")
      .with_expr("json_quote(acos(a)), json_quote(asin(a)), json_quote(atan(a)), "
                 "json_quote(cot(a)), json_quote(radians(a)), json_quote(round(a)), "
                 "json_quote(ceil(a)), json_quote(floor(a)), json_quote(abs(a)), "
                 "json_quote(sign(a))")
      .with_data({
          {TestValue::null(), TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{
      TestValue::null(), TestValue::null(), TestValue::null(), TestValue::null(),
      TestValue::null(), TestValue::null(), TestValue::null(), TestValue::null(),
      TestValue::null(), TestValue::null(),
  }}));
}

// Q7: null through LN(-4) and LOG(-4,-8) — return NULL with a warning.
TEST_F(JsonQuoteExprTest, MysqlQuoteNullFromLog)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("json_quote(ln(a)), json_quote(log(a, b))")
      .with_data({
          {-4, -8},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null(), TestValue::null()}}));
}

// Q8: null through MOD and POW — json_quote(mod(80,null)) and json_quote(pow(16,null)).
TEST_F(JsonQuoteExprTest, MysqlQuoteNullFromModPow)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("json_quote(mod(a, b)), json_quote(pow(a, b))")
      .with_data({
          {80, TestValue::null()},
          {16, TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{
      TestValue::null(), TestValue::null(),
  }, {
      TestValue::null(), TestValue::null(),
  }}));
}

// Q9: cast result back to JSON — CAST(JSON_QUOTE('123') AS JSON) -> "123".
TEST_F(JsonQuoteExprTest, MysqlQuoteCastAsJson)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("cast(json_quote(s) as json)")
      .with_data({
          {"123"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("123")"}}));
}

// Q10: integer argument rejected at eval — json_quote(123) -> error.
// mysqltest line 89: --error ER_INCORRECT_TYPE; select json_quote(123).
TEST_F(JsonQuoteExprTest, MysqlQuoteIntRejected)
{
  auto result = expr_unit_test()
      .columns("n int")
      .with_expr("json_quote(n)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {123},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// Q11: double argument rejected at eval — json_quote(POWER(16,4)) -> error.
// mysqltest line 180: --error ER_INCORRECT_TYPE; SELECT JSON_QUOTE(POWER(16,4)).
TEST_F(JsonQuoteExprTest, MysqlQuoteDoubleRejected)
{
  auto result = expr_unit_test()
      .columns("d double")
      .with_expr("json_quote(d)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {65536.0},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// Q12: date argument rejected at eval — json_quote(CURDATE()) -> error.
// mysqltest line 72: --error ER_INCORRECT_TYPE; select json_quote(CURDATE()).
TEST_F(JsonQuoteExprTest, MysqlQuoteDateRejected)
{
  auto result = expr_unit_test()
      .columns("d date")
      .with_expr("json_quote(d)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {"2021-11-11"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// Q13: datetime argument rejected at eval — json_quote(from_unixtime(...)) -> error.
// mysqltest line 8: --error ER_INCORRECT_TYPE; select json_quote(from_unixtime(1218169800)).
TEST_F(JsonQuoteExprTest, MysqlQuoteDatetimeRejected)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("json_quote(dt)")
      .expect_error(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT)
      .with_data({
          {"2008-08-08 00:00:00"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT, result.get_ret_code());
}

// A1: convert(... using utf8) — exercises the charset_type != CHARSET_UTF8MB4
// branch of ObExprJsonQuote::calc_result_type1 (ob_expr_json_quote.cpp:43-46),
// which sets calc_collation_type to CS_TYPE_UTF8MB4_BIN for non-utf8mb4 input.
// mysqltest lines 42-43.
TEST_F(JsonQuoteExprTest, MysqlQuoteConvertUtf8)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(convert(s using utf8))")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("abc")"}}));
}

// A1: convert(... using utf8mb4) — same charset branch, explicit utf8mb4.
// mysqltest line 43.
TEST_F(JsonQuoteExprTest, MysqlQuoteConvertUtf8mb4)
{
  auto result = expr_unit_test()
      .columns("s varchar(64)")
      .with_expr("json_quote(convert(s using utf8mb4))")
      .with_data({
          {"abc"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("abc")"}}));
}

// A2: UNHEX("") produces an empty binary-string value, whose charset is
// CS_TYPE_BINARY. ensure_collation (ob_expr_json_func_helper.cpp:109-119)
// rejects ob_is_string_type + charset != CHARSET_UTF8MB4 with
// OB_ERR_INVALID_JSON_CHARSET. mysqltest lines 353-354:
//   select JSON_QUOTE(UNHEX(""));  -> ERROR 22032.
// If UNHEX resolves but yields NULL (or the framework refuses the construct),
// the blob-column fallback below covers the same ensure_collation branch.
TEST_F(JsonQuoteExprTest, MysqlQuoteUnhexEmpty)
{
  auto result = expr_unit_test()
      .columns("s varchar(16)")
      .with_expr("json_quote(unhex(s))")
      .expect_error(common::OB_ERR_INVALID_JSON_CHARSET)
      .with_data({
          {""},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_CHARSET, result.get_ret_code());
}

// A2: blob column carrying an empty binary string. The blob type forces
// CS_TYPE_BINARY at schema level (ob_op_test_engine.cpp:630-644), so the value
// reaches ensure_collation with charset=BINARY and triggers the same
// OB_ERR_INVALID_JSON_CHARSET path as UNHEX(""). Fallback / equivalent coverage
// for ob_expr_json_quote.cpp:73.
TEST_F(JsonQuoteExprTest, MysqlQuoteEmptyBlob)
{
  auto result = expr_unit_test()
      .columns("b blob")
      .with_expr("json_quote(b)")
      .expect_error(common::OB_ERR_INVALID_JSON_CHARSET)
      .with_data({
          {""},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_CHARSET, result.get_ret_code());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
