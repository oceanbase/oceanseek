/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for MEMBER OF, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_member_of_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/  (and r/mysql/).
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// The JSON array is fed through a varchar column; OceanBase auto-converts it to
// JSON, so the JSON value (number vs string vs array) is preserved exactly as
// in mysqltest.
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
// MEMBER OF — 1 if the candidate is an element of the JSON array, 0 otherwise,
// NULL if the candidate or array is NULL. The candidate's JSON type matters:
// a number literal 17 differs from a string literal '17'.
//
// To preserve the number-vs-string distinction of the mysqltest literals, the
// candidate is written as a literal inside the expression and only the array is
// fed through a varchar column.
//   mysqltest: json_member_of_mysql.test
// ============================================================================
class JsonMemberOfExprTest : public OpTestKit {};

// Number vs string candidate against the same array.
TEST_F(JsonMemberOfExprTest, MysqlNumberVsStringCandidate)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("17 member of (arr)")
      .with_data({
          {R"([23, "abc", 17, "ab", 10])"},   // 17 (number) present -> 1
          {R"([23, "abc", "17", "ab", 10])"},  // only "17" (string) -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
}

// String candidate '17' (single-quoted) against an array holding number 17.
TEST_F(JsonMemberOfExprTest, MysqlStringCandidateAgainstNumberArray)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("'17' member of (arr)")
      .with_data({{R"([23, "abc", 17, "ab", 10])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}}));
}

// 'ab' is an element; 'a' is not.
TEST_F(JsonMemberOfExprTest, MysqlStringPresentAndAbsent)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("'ab' member of (arr)")
      .with_data({{R"([23, "abc", 17, "ab", 10])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

TEST_F(JsonMemberOfExprTest, MysqlNumberAbsent)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("7 member of (arr)")
      .with_data({{R"([23, "abc", 17, "ab", 10])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}}));
}

// Boolean literals true/false match JSON boolean elements.
TEST_F(JsonMemberOfExprTest, MysqlBooleanCandidate)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("true member of (arr)")
      .with_data({{R"(["abc", null, true, false])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// NULL candidate -> NULL (not 0, not 1).
TEST_F(JsonMemberOfExprTest, MysqlNullCandidateReturnsNull)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("null member of (arr)")
      .with_data({{R"([23, "abc", 17, "ab", 10])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// NULL array -> NULL.
TEST_F(JsonMemberOfExprTest, MysqlNullArrayReturnsNull)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("17 member of (arr)")
      .with_data({{TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Int column as candidate: number-typed, matches array numbers.
TEST_F(JsonMemberOfExprTest, MysqlIntColumnCandidate)
{
  auto result = expr_unit_test()
      .columns("cand int, arr varchar(512)")
      .with_expr("cand member of (arr)")
      .with_data({
          {1, R"([1,2])"},   // -> 1
          {2, R"([1,2])"},   // -> 1
          {3, R"([1,2])"},   // -> 0
          {4, R"([1,2])"},   // -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"0"}, {"0"}}));
}

// CAST('[4,5]' AS JSON) / JSON_ARRAY(4,5) as candidate against nested arrays.
TEST_F(JsonMemberOfExprTest, MysqlJsonCandidateAgainstNestedArray)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("cast('[4,5]' as json) member of (arr)")
      .with_data({{R"([[3,4],[4,5]])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

TEST_F(JsonMemberOfExprTest, MysqlJsonArrayCandidateAgainstNestedArray)
{
  auto result = expr_unit_test()
      .columns("arr varchar(512)")
      .with_expr("json_array(4,5) member of (arr)")
      .with_data({{R"([[3,4],[4,5]])"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
