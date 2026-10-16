/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_OBJECT, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_object_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_object_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// JSON_OBJECT takes a variable-length list of key/value pairs (arg count must
// be even, checked at resolve time). The mysqltest cases feed literal
// arguments directly in the SELECT list; here we port them by putting the same
// literal expression in with_expr() and feeding a single dummy row through the
// virtual table. Cases that read a column (e.g., json_object('id', `time`)) put
// the column value in with_data().
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors cannot be asserted through this framework:
// prepare_resolve() does not forward expected_error_ to TestSqlUtils::do_resolve(),
// which carries its own ASSERT_EQ(expect_error=0, -ret). Specifically these
// stay in mysqltest:
//   - ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT (odd arg count): line 8 / 11 / 49
//   - JSON document with NULL member name when NULL is a literal: line 14
//     (select json_object(null, 1)) is resolved at CG stage with type deduction
//     flagging the null literal — the eval-stage null-key check only fires for
//     a *column* value that evaluates to NULL at runtime.
// The error cases below fire when a column value is NULL at eval time and
// surface through result.get_ret_code().

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

#include <cstdio>
#include <string>

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonObjectExprTest : public OpTestKit {};

// ============================================================================
// Scenarios ported from mysqltest suite
//   tools/deploy/mysql_test/test_suite/json/t/json_object_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_object_mysql.result
// ============================================================================

// M1: empty argument list -> {}.
// mysqltest line 17 / 39: select json_object();  SELECT JSON_OBJECT();
TEST_F(JsonObjectExprTest, MysqlEmptyObject)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object()")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"{}"}}));
}

// M2: NULL value (not NULL key) — the value slot is NULL literal which becomes
// JSON null. This differs from json_object(null, 1) where the *key* is NULL.
// mysqltest line 18: select json_object( 'a', null );
TEST_F(JsonObjectExprTest, MysqlNullValue)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('a', null)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": null})"}}));
}

// M3: single key/value pair with a string value.
// mysqltest line 19: select json_object( 'a', 1 );
TEST_F(JsonObjectExprTest, MysqlSinglePairInt)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('a', 1)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": 1})"}}));
}

// M4: two key/value pairs, second value a string.
// mysqltest line 20: select json_object( 'a', 1, 'b', 'foo' );
TEST_F(JsonObjectExprTest, MysqlTwoPairs)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('a', 1, 'b', 'foo')")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": 1, "b": "foo"})"}}));
}

// M5: three pairs, third value is cast(...as json) — embedded as a JSON object.
// mysqltest line 21: select json_object( 'a', 1, 'b', 'foo', 'c', cast( '{ "d": "wibble" }' as json ) );
TEST_F(JsonObjectExprTest, MysqlThreePairsJsonCast)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(json_object('a', 1, 'b', 'foo', 'c', cast('{ "d": "wibble" }' as json)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": 1, "b": "foo", "c": {"d": "wibble"}})"}}));
}

// M6: boolean true/false literals and cast('null' as json) as the third value.
// mysqltest line 22: select json_object( 'a', true, 'b', false, 'c', cast( 'null' as json) );
TEST_F(JsonObjectExprTest, MysqlBooleansAndJsonNull)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(json_object('a', true, 'b', false, 'c', cast('null' as json)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": true, "b": false, "c": null})"}}));
}

// M7: json_valid(json_object('"a"', 1)) — the key is a JSON-encoded string '"a"',
// which after unwrapping becomes the plain key `a`. json_valid returns 1.
// mysqltest line 23: select json_valid( json_object( '"a"', 1 ) );
TEST_F(JsonObjectExprTest, MysqlValidOnQuotedKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(R"(json_valid(json_object('"a"', 1)))")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// M8: REPEAT('a', 64*1024) produces a 64KB key. We verify the result is a
// valid JSON object with the long key (the result file itself truncates the
// display, so we check prefix/suffix and that the key length is ~64K).
// mysqltest line 26: select json_object( REPEAT('a', 64 * 1024), 1 );
TEST_F(JsonObjectExprTest, MysqlLongKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object(REPEAT('a', 64 * 1024), 1)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  ASSERT_EQ(1, result.row_count());
  // The value is a JSON object {"<64K a's>": 1}. OB may emit a space after the
  // colon ({"aaa...": 1}) so we check prefix/suffix rather than exact length.
  const std::string &row = result.get_row(0)[0];
  EXPECT_EQ(0, row.rfind(R"({")", 0));                  // starts with {"
  EXPECT_EQ(R"(": 1})", row.substr(row.size() - 5));    // ends with ": 1}
  // The key body is 64*1024 'a' chars between the opening {" and ": 1}.
  const size_t expected_key_len = static_cast<size_t>(64 * 1024);
  const std::string key_body = row.substr(2, row.size() - 2 - 5);
  EXPECT_EQ(expected_key_len, key_body.size());
  // Verify all key chars are 'a'.
  EXPECT_EQ(key_body.find_first_not_of('a'), std::string::npos);
}

// M9: json_array() as the key — the empty array's string form "[]" is used as
// the key name (no quote, the array is cast to CHAR).
// mysqltest line 29: select json_object(json_array(), json_array());
TEST_F(JsonObjectExprTest, MysqlEmptyArrayAsKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object(json_array(), json_array())")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"[]": []})"}}));
}

// M10: cast(json_array() as char) — explicit char cast of an empty array, the
// key name is "[]" (same as M9 via a different cast path).
// mysqltest line 30: select json_object( cast(json_array() as char), json_array());
TEST_F(JsonObjectExprTest, MysqlCastArrayAsCharKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object(cast(json_array() as char), json_array())")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"[]": []})"}}));
}

// M11: integer key 1 — cast to CHAR, so the key name is "1".
// mysqltest line 32: select json_object( 1, json_array());
TEST_F(JsonObjectExprTest, MysqlIntKeyCastToChar)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object(1, json_array())")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"1": []})"}}));
}

// M12: cast(1 as char) explicitly — same result as M11.
// mysqltest line 33: select json_object( cast(1 as char), json_array());
TEST_F(JsonObjectExprTest, MysqlExplicitIntCastKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object(cast(1 as char), json_array())")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"1": []})"}}));
}

// M13: duplicate keys — last value wins, keys are sorted in insertion order
// except that the duplicate 'a' is replaced (result shows {"a": 3, "b": 2}).
// mysqltest line 36: SELECT JSON_OBJECT('a', 1, 'b', 2, 'a', 3);
TEST_F(JsonObjectExprTest, MysqlDuplicateKeysLastWins)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("JSON_OBJECT('a', 1, 'b', 2, 'a', 3)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": 3, "b": 2})"}}));
}

// M14: json_object over a bigint pk + a timestamp(6) column — pk becomes a string
// key, timestamp value is the literal timestamp string.
// DISABLED: the op_tests framework's MockDataSource does not parse string values
// into the datetime/timestamp internal int64 encoding (VEC_TC_DATETIME only
// accepts is_int/is_double, see ob_op_test_data_source.cpp). Feeding a string
// like "2000-06-24 20:27:45.013867" yields 0 (epoch), so the result mismatches.
// mysqltest lines 55-66:
//   create table t1 (pk bigint, col_timestamp_6 timestamp(6), col_json json, primary key (pk));
//   insert into t1 values (1, '2000-06-24 20:27:45.013867', 'false'), ...;
//   select json_object(pk, col_timestamp_6) from t1 group by pk;
// TODO: re-enable once the framework supports string→datetime/timestamp
// conversion (planned as a separate framework-enhancement commit).
/*
TEST_F(JsonObjectExprTest, MysqlBigIntKeyTimestampValue)
{
  auto result = expr_unit_test()
      .columns("pk bigint, ts timestamp(6)")
      .with_expr("json_object(pk, ts)")
      .with_data({
          {1, "2000-06-24 20:27:45.013867"},
          {2, "2005-03-20 11:50:50.059548"},
          {3, "2000-01-05 23:38:26.009805"},
          {4, "2007-12-26 19:58:06.056211"},
          {5, "2007-10-09 16:52:21.001669"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"({"1": "2000-06-24 20:27:45.013867"})"},
      {R"({"2": "2005-03-20 11:50:50.059548"})"},
      {R"({"3": "2000-01-05 23:38:26.009805"})"},
      {R"({"4": "2007-12-26 19:58:06.056211"})"},
      {R"({"5": "2007-10-09 16:52:21.001669"})"},
  }));
}
*/

// M15: json_object('year', c1) where c1 is a YEAR column holding 12 — YEAR(2)
// interprets 12 as 2012. The value is emitted as the integer 2012.
// DISABLED: the framework's MockDataSource stores the raw int 12 into the YEAR
// uint8 slot without the YEAR(2)→2012 expansion that the resolver applies for a
// real INSERT. The emitted value is 1912 instead of 2012. This is a framework
// data-injection limitation (YEAR column int expansion), not a json_object bug.
// mysqltest lines 99-102:
//   create table test1(c1 year);
//   insert into test1 values(12);
//   select json_object('year', c1) from test1;
// TODO: re-enable once the framework applies YEAR(2) expansion on data injection.
/*
TEST_F(JsonObjectExprTest, MysqlYearColumnValue)
{
  auto result = expr_unit_test()
      .columns("c1 year")
      .with_expr("json_object('year', c1)")
      .with_data({{12}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"year": 2012})"}}));
}
*/

// M16: json_object('id', `time`) over a DATETIME column. The datetime value is
// emitted with full microsecond precision regardless of session time_zone
// (the DATETIME type is timezone-independent in OB, matching the result file).
// DISABLED: the op_tests framework's MockDataSource does not parse string values
// into the datetime internal int64 encoding (VEC_TC_DATETIME only accepts
// is_int/is_double, see ob_op_test_data_source.cpp).
// mysqltest lines 111-115 + 116-119 (time_zone='+12:00' re-read, same output):
//   create table test_time(id int primary key, time DATETIME);
//   insert into test_time values(1, "2023-09-21 08:16:00.000000");
//   SELECT json_object('id', `time`) AS json FROM test_time;
// TODO: re-enable once the framework supports string→datetime conversion
// (planned as a separate framework-enhancement commit).
/*
TEST_F(JsonObjectExprTest, MysqlDatetimeValue)
{
  auto result = expr_unit_test()
      .columns("id int, dt datetime")
      .with_expr("json_object('id', dt)")
      .with_data({{1, "2023-09-21 08:16:00.000000"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"id": "2023-09-21 08:16:00.000000"})"}}));
}
*/

// M17: json_object('id', `time`) over a TIMESTAMP column — TIMESTAMP is
// timezone-aware. With session time_zone='+8:00' the stored instant is rendered
// as '2023-09-21 08:16:00.000000'; with '+12:00' the same instant renders as
// '2023-09-21 12:16:00.000000'.
// DISABLED: same as M16 — the framework's MockDataSource does not parse string
// values into the timestamp internal encoding, and the TIMESTAMP path also needs
// the session time zone applied during conversion.
// mysqltest lines 123-133:
//   create table test_time(id int primary key, time TIMESTAMP);
//   insert into test_time values(1, "2023-09-21 08:16:00.000000");
//   SELECT json_object('id', `time`) AS json FROM test_time;      -- +8:00
//   set @@time_zone = '+12:00';
//   SELECT json_object('id', `time`) AS json FROM test_time;      -- +12:00
// TODO: re-enable once the framework supports string→timestamp conversion
// (planned as a separate framework-enhancement commit).
/*
TEST_F(JsonObjectExprTest, MysqlTimestampValueTimezone)
{
  // Default session time_zone is +8:00 — stored instant renders as-is.
  {
    auto result = expr_unit_test()
        .columns("id int, ts timestamp")
        .with_expr("json_object('id', ts)")
        .with_data({{1, "2023-09-21 08:16:00.000000"}})
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{R"({"id": "2023-09-21 08:16:00.000000"})"}}));
  }
  // With time_zone='+12:00' the same instant renders 4 hours later.
  {
    WITH_SESSION_VARS({"time_zone", "+12:00"}) {
      auto result = expr_unit_test()
          .columns("id int, ts timestamp")
          .with_expr("json_object('id', ts)")
          .with_data({{1, "2023-09-21 08:16:00.000000"}})
          .run(engine_);
      EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
      EXPECT_TRUE(result.verify_ordered({{R"({"id": "2023-09-21 12:16:00.000000"})"}}));
    }
  }
}
*/

// ============================================================================
// Error scenarios ported from mysqltest
//   tools/deploy/mysql_test/test_suite/json/t/json_object_mysql.test
//
// Only RUNTIME (eval-stage) errors are covered here. The framework's
// prepare_resolve() does not forward expected_error_ to the underlying
// TestSqlUtils::do_resolve(), which carries its own ASSERT_EQ(expect_error=0,
// -ret); a resolve/type-deduction-stage error trips that assertion. The
// resolve-stage errors that stay in mysqltest are:
//   - ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT (odd arg count, lines 8/11/49)
//   - ER_JSON_DOCUMENT_NULL_KEY when NULL is a literal key (line 14): the NULL
//     literal is flagged at type-deduction time, not eval time.
// The eval-stage case below fires when a *column* value for the key slot
// evaluates to NULL at runtime, which hits the null-key check in
// ObExprJsonObject::eval_json_object.
// ============================================================================

// E1: NULL key via a varchar column set to NULL. mysqltest line 14 only covers
// the literal-null case (resolve-stage); this is the column-value analog. The
// eval-stage null-key check returns OB_ERR_JSON_DOCUMENT_NULL_KEY.
TEST_F(JsonObjectExprTest, MysqlNullKeyFromColumn)
{
  auto result = expr_unit_test()
      .columns("k varchar(16), v int")
      .with_expr("json_object(k, v)")
      .expect_error(common::OB_ERR_JSON_DOCUMENT_NULL_KEY)
      .with_data({{TestValue::null(), 1}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_JSON_DOCUMENT_NULL_KEY, result.get_ret_code());
}

// E2 (non-error documentation): a malformed JSON text fed as the *value*
// through a varchar column is NOT an error for JSON_OBJECT — the value is
// wrapped as a JSON string element, just like json_array does. This differs
// from json_extract/json_depth which parse the argument as a JSON document.
TEST_F(JsonObjectExprTest, MysqlMalformedJsonTextInValueStaysString)
{
  auto result = expr_unit_test()
      .columns("k varchar(16), v varchar(64)")
      .with_expr("json_object(k, v)")
      .with_data({{"a", R"({ "a" : [ })"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"X({"a": "{ \"a\" : [ }"})X"}}));
}

// ============================================================================
// Regression cases for P1 fixes (commit 20a44272 review).
// ============================================================================

// R1: empty-string key must be accepted (not treated as NULL).
// SQL NULL is distinguished by json_datum->is_null() in the expression layer;
// a zero-length string is a valid JSON member name.
// Before fix: ObJsonBinAggSerializer::append_key_value rejected key.empty()
// with OB_ERR_JSON_DOCUMENT_NULL_KEY. After fix: {"": 1}.
TEST_F(JsonObjectExprTest, MysqlEmptyStringKey)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('', 1)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"": 1})"}}));
}

// R2: many equal-length keys inserted in reverse lexicographic order.
// Regression for the O(n^2) insertion-sort degradation: the previous hand-
// written insertion sort hit O(n^2) on equal-length reverse-sorted keys.
// After fix (std::stable_sort) the result must be sorted ascending by key
// (length-first, then lex), with duplicate keys keeping the last value.
// N is kept modest (50) so the expr_unit_test framework stays fast.
TEST_F(JsonObjectExprTest, MysqlLargeReverseEqualLenKeys)
{
  // Build: json_object('k49', 49, 'k48', 48, ..., 'k000', 0)
  // Expected: {"k000": 0, "k001": 1, ..., "k049": 49}
  std::string expr = "json_object(";
  for (int i = 49; i >= 0; --i) {
    char key[8];
    snprintf(key, sizeof(key), "k%03d", i);  // equal-length 4-char keys
    if (i != 49) {
      expr += ", ";
    }
    expr += "'";
    expr += key;
    expr += "', ";
    expr += std::to_string(i);
  }
  expr += ")";
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr(expr.c_str())
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  std::string expected = "{";
  for (int i = 0; i < 50; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "\"k%03d\": %d", i, i);
    if (i != 0) {
      expected += ", ";
    }
    expected += buf;
  }
  expected += "}";
  EXPECT_TRUE(result.verify_ordered({{expected}}));
}

// R3: duplicate empty-string keys keep the last value (last-wins), exercising
// both the empty-key fix and the sort+unique path on empty keys.
TEST_F(JsonObjectExprTest, MysqlDuplicateEmptyStringKeysLastWins)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('', 1, '', 2, '', 3)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"": 3})"}}));
}

// R4: boolean values (true/false) go through the DOM wrapper path; a false
// value must survive as false (regression for ObJsonWrapper::get_boolean
// reading ObJsonBoolean via get_int(), which could misread padding as true).
TEST_F(JsonObjectExprTest, MysqlBooleanValues)
{
  auto result = expr_unit_test()
      .columns("dummy int")
      .with_expr("json_object('a', false, 'b', true)")
      .with_data({{0}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"({"a": false, "b": true})"}}));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
