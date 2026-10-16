/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_ARRAY, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_array_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_array_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// JSON_ARRAY takes a variable-length argument list and builds a JSON array out
// of the arguments. The mysqltest cases feed literal arguments directly in the
// SELECT list; here we port them by putting the same literal expression in
// with_expr() and feeding a single dummy row through the virtual table. For
// cases that read a column (e.g., json_array(col_json,col_json,217)) the column
// value is supplied through with_data().
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors (wrong param count, bare non-string literal, parse
// errors) cannot be asserted through this framework: prepare_resolve() does not
// forward expected_error_ to TestSqlUtils::do_resolve(), which carries its own
// ASSERT_EQ(expect_error=0, -ret). Those mysqltest cases stay in the mysqltest
// suite; the error cases below fire when a column value is parsed at eval time
// and surface through result.get_ret_code().

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonArrayExprTest : public OpTestKit {};

// ============================================================================
// Scenarios ported from mysqltest suite
//   tools/deploy/mysql_test/test_suite/json/t/json_array_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_array_mysql.result
//
// These are pure-expression cases (no table DML for most), so they map naturally
// onto expr_unit_test(). Literal arguments stay in with_expr(); a single dummy
// row is supplied so the SELECT emits exactly one row.
// ============================================================================

// M1: NULL arg followed by a path-like string and cast(1 as json) — NULL becomes
// JSON null, '$.b' stays a JSON string, cast(1 as json) becomes the JSON number 1.
// mysqltest line 6: select json_array(NULL, '$.b', cast(1 as json));
TEST_F(JsonArrayExprTest, MysqlNullStringAndJsonCast)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(NULL, '$.b', cast(1 as json))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"([null, "$.b", 1])"}}));
}

// M2: '[1,2,3]' is a varchar literal that becomes a JSON string (not a JSON array),
// NULL becomes JSON null, cast(1 as json) is JSON number 1.
// mysqltest line 7: select json_array('[1,2,3]', NULL, cast(1 as json));
TEST_F(JsonArrayExprTest, MysqlStringNullAndJsonCast)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array('[1,2,3]', NULL, cast(1 as json))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["[1,2,3]", null, 1])"}}));
}

// M3: '[1,2,3]' and '$[3]' are both string literals (kept as JSON strings),
// NULL becomes JSON null.
// mysqltest line 8: select json_array('[1,2,3]', '$[3]', NULL);
TEST_F(JsonArrayExprTest, MysqlStringPathAndNull)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array('[1,2,3]', '$[3]', NULL)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["[1,2,3]", "$[3]", null])"}}));
}

// M4: single integer literal 38793 -> [38793].
// mysqltest line 9: select json_array(38793);
TEST_F(JsonArrayExprTest, MysqlSingleInt38793)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(38793)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[38793]"}}));
}

// M5: single integer literal 129 -> [129].
// mysqltest line 10: select json_array(129);
TEST_F(JsonArrayExprTest, MysqlSingleInt129)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(129)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[129]"}}));
}

// M6: single negative integer -8388607 -> [-8388607].
// mysqltest line 11: select json_array(-8388607);
TEST_F(JsonArrayExprTest, MysqlSingleNegativeInt)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(-8388607)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[-8388607]"}}));
}

// M7: single integer literal 65543 -> [65543].
// mysqltest line 12: select json_array(65543);
TEST_F(JsonArrayExprTest, MysqlSingleInt65543)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(65543)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[65543]"}}));
}

// M8: CONCAT inside JSON_ARRAY — the concatenated string (with embedded double
// quotes) is placed as a single JSON string element. Expected value is the
// JSON-escaped form taken verbatim from the result file.
// mysqltest line 13: SELECT JSON_ARRAY(CONCAT('"SQL "', '"汉字 "', '"Gooogle "', '"123456"')) from dual;
TEST_F(JsonArrayExprTest, MysqlConcatInsideJsonArray)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("JSON_ARRAY(CONCAT('\"SQL \"', '\"汉字 \"', '\"Gooogle \"', '\"123456\"'))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["\"SQL \"\"汉字 \"\"Gooogle \"\"123456\""])"}}));
}

// M9: CONCAT_WS inside JSON_ARRAY — like M8 but join with the first arg as sep.
// mysqltest line 14: SELECT JSON_ARRAY(CONCAT_WS('"SQL "', '"汉字 "', '"Gooogle "', '"123456"')) from dual;
TEST_F(JsonArrayExprTest, MysqlConcatWsInsideJsonArray)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("JSON_ARRAY(CONCAT_WS('\"SQL \"', '\"汉字 \"', '\"Gooogle \"', '\"123456\"'))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["\"汉字 \"\"SQL \"\"Gooogle \"\"SQL \"\"123456\""])"}}));
}

// M10: SUBSTR("zhangsan", 0, 3) returns '' -> [""].
// mysqltest line 15: SELECT JSON_ARRAY(SUBSTR("zhangsan", 0, 3)) from dual;
TEST_F(JsonArrayExprTest, MysqlSubstrEmpty)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(SUBSTR("zhangsan", 0, 3)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M11: SUBSTRING is an alias for SUBSTR — same empty-string result.
// mysqltest line 16: SELECT JSON_ARRAY(SUBSTRING("zhangsan", 0, 3)) from dual;
TEST_F(JsonArrayExprTest, MysqlSubstringEmpty)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(SUBSTRING("zhangsan", 0, 3)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M12: REPEAT("1", -1) returns '' -> [""].
// mysqltest line 17: SELECT JSON_ARRAY(REPEAT("1", -1)) from dual;
TEST_F(JsonArrayExprTest, MysqlRepeatNegative)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(REPEAT("1", -1)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M13: SPACE("n") — "n" is cast to 0, so SPACE returns '' -> [""].
// mysqltest line 19: SELECT JSON_ARRAY(SPACE("n")) from dual;
TEST_F(JsonArrayExprTest, MysqlSpaceNonNumeric)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(SPACE("n")))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M14: LPAD("san", 0, "zhang") returns '' -> [""].
// mysqltest line 21: SELECT JSON_ARRAY(LPAD("san",0,"zhang")) from dual;
TEST_F(JsonArrayExprTest, MysqlLpadZeroLength)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(LPAD("san",0,"zhang")))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M15: SUBSTR("abcdefg", 3, -2) returns '' -> [""].
// mysqltest line 22: SELECT JSON_ARRAY(SUBSTR("abcdefg",3,-2)) from dual;
TEST_F(JsonArrayExprTest, MysqlSubstrNegativeLength)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(JSON_ARRAY(SUBSTR("abcdefg",3,-2)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X([""])X"}}));
}

// M16: POW(16, 4.5) -> 262144.0 (a DOUBLE kept verbatim per result file).
// mysqltest line 23: SELECT JSON_ARRAY(POW(16,4.5)) from dual;
TEST_F(JsonArrayExprTest, MysqlPowResult)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("JSON_ARRAY(POW(16,4.5))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[262144.0]"}}));
}

// M17: POWER(6, 4.5) -> 3174.538706646999 (DOUBLE).
// mysqltest line 24: SELECT JSON_ARRAY(POWER(6,4.5)) from dual;
TEST_F(JsonArrayExprTest, MysqlPowerResult)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("JSON_ARRAY(POWER(6,4.5))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[3174.538706646999]"}}));
}

// M18: empty argument list -> [].
// mysqltest line 27 / 50: select json_array();  SELECT JSON_ARRAY();
TEST_F(JsonArrayExprTest, MysqlEmptyArgs)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array()")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[]"}}));
}

// M19: single double 3.14 -> [3.14].
// mysqltest line 28: select json_array(3.14);
TEST_F(JsonArrayExprTest, MysqlSingleDouble)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(3.14)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[3.14]"}}));
}

// M20: varchar literal '[1,2,3]' stays a JSON string (not auto-parsed as array).
// mysqltest line 29: select json_array('[1,2,3]');
TEST_F(JsonArrayExprTest, MysqlVarcharLiteralStaysString)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array('[1,2,3]')")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["[1,2,3]"])"}}));
}

// M21: cast('[1,2,3]' as json) embeds a real JSON array as the element.
// mysqltest line 30: select json_array(cast('[1,2,3]' as json));
TEST_F(JsonArrayExprTest, MysqlCastAsJsonEmbedsArray)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(cast('[1,2,3]' as json))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[[1, 2, 3]]"}}));
}

// M22: three integer literals -> [1, 2, 3].
// mysqltest line 31: select json_array(1,2,3);
TEST_F(JsonArrayExprTest, MysqlThreeInts)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(1,2,3)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[1, 2, 3]"}}));
}

// M23: bit-string literals b'0', b'1', b'10' are encoded as base64 binary
// elements (type 15). Expected values taken verbatim from result file.
// mysqltest line 32: select json_array(b'0', b'1', b'10');
TEST_F(JsonArrayExprTest, MysqlBitStringLiterals)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(b'0', b'1', b'10')")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered(
      {{R"(["base64:type15:AA==", "base64:type15:AQ==", "base64:type15:Ag=="])"}}));
}

// M24: many repeated high-precision decimal literals — each is kept as-is.
// mysqltest lines 35-41: select json_array(122222222.12222222333, ... x28);
TEST_F(JsonArrayExprTest, MysqlRepeatedDecimal)
{
  // Build the expected "[v, v, ..., v]" string with 28 copies of the literal.
  const char *literal = "122222222.12222222333";
  std::string expected = "[";
  for (int i = 0; i < 28; ++i) {
    if (i > 0) expected += ", ";
    expected += literal;
  }
  expected += "]";

  // Build the with_expr string "json_array(literal, literal, ...)".
  std::string expr = "json_array(";
  for (int i = 0; i < 28; ++i) {
    if (i > 0) expr += ", ";
    expr += literal;
  }
  expr += ")";

  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(expr.c_str())
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{expected.c_str()}}));
}

// M25: json_array over a JSON column — col_json is itself a JSON array, so it is
// embedded as a JSON array element (not wrapped in a string). The 217 is a plain
// integer element.
// mysqltest lines 44-48:
//   create table t1 (col_json json);
//   insert into t1 values ('["sfsdf...","sdfsdf..."]');
//   select json_array(col_json,col_json,217) from t1;
TEST_F(JsonArrayExprTest, MysqlJsonColumnEmbedsArray)
{
  auto result = expr_unit_test()
      .columns("col_json json")
      .with_expr("json_array(col_json,col_json,217)")
      .with_data({{R"(["sfsdfasdfasdfasdfasdfasdfasdfasdf","sdfsdfsdfsdfsdfsdsdfsfsdfsdfsdfsd"])"}}
)
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered(
      {{R"([["sfsdfasdfasdfasdfasdfasdfasdfasdf", "sdfsdfsdfsdfsdfsdsdfsfsdfsdfsdfsd"], ["sfsdfasdfasdfasdfasdfasdfasdfasdf", "sdfsdfsdfsdfsdfsdsdfsfsdfsdfsdfsd"], 217])"}}));
}

// ============================================================================
// Error scenarios ported from mysqltest
//   tools/deploy/mysql_test/test_suite/json/t/json_array_mysql.test
//
// json_array_mysql.test has no --error cases: JSON_ARRAY coerces each argument
// to a JSON value via get_json_val_wrapper. A varchar argument that is not a
// valid JSON document is kept as a JSON *string* (the literal text wrapped in
// quotes), so no error is raised. This differs from json_extract/json_depth/
// json_length which parse the argument as a JSON document and surface
// OB_ERR_INVALID_JSON_TEXT_IN_PARAM on malformed input. The only error paths
// in ObExprJsonArray are:
//   - OB_ERR_PARAM_SIZE / WRONG_PARAMCOUNT (resolve stage — not portable here)
//   - OB_ERR_INVALID_JSON_CHARSET (output collation, set up at CG — not portable)
//   - OB_ERR_JSON_OUT_OF_DEPTH (only for very large inputs)
// All of these stay in mysqltest. The eval-stage case below documents the
// "malformed JSON text stays a string" behavior so the difference from
// json_extract is explicit.
// ============================================================================

// E1 (non-error documentation): a malformed JSON text fed through a varchar
// column is NOT an error for JSON_ARRAY — it becomes a JSON string element.
// mysqltest has no matching case; this documents the OB behavior.
TEST_F(JsonArrayExprTest, MysqlMalformedJsonTextStaysString)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64)")
      .with_expr("json_array(doc)")
      .with_data({{R"({ "a" : [ })"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X(["{ \"a\" : [ }"])X"}}));
}

// R1: boolean values (false/true) go through the DOM wrapper path; false must
// survive as false (regression for ObJsonWrapper::get_boolean).
TEST_F(JsonArrayExprTest, MysqlBooleanValues)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_array(false, true, 0, 1)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"[false, true, 0, 1]"}}));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
