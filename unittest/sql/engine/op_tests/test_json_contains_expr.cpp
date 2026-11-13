/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_CONTAINS and JSON_CONTAINS_PATH.
// JSON_CONTAINS cases were ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_contains_mysql.test
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
// JSON_CONTAINS — 1 if the candidate is contained in the document (optionally
// at a path), 0 otherwise, NULL if any of doc/candidate/path is NULL.
// Both doc and candidate are fed as varchar and auto-converted to JSON, which
// preserves their JSON value type exactly as in mysqltest.
//   mysqltest: json_contains_mysql.test
// ============================================================================
class JsonContainsExprTest : public OpTestKit {};

// NULL in any argument -> NULL.
TEST_F(JsonContainsExprTest, MysqlNullArguments)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand)")
      .with_data({
          {TestValue::null(), TestValue::null()},                       // NULL,NULL -> NULL
          {R"({"a": 1, "b": 2})", TestValue::null()},                   // doc, NULL -> NULL
          {TestValue::null(), R"(null)"},                               // NULL, cand -> NULL
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {TestValue::null()},
      {TestValue::null()},
      {TestValue::null()},
  }));
}

// A NULL path (3rd arg) makes the whole call return NULL.
TEST_F(JsonContainsExprTest, MysqlNullPathReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512), path varchar(32)")
      .with_expr("json_contains(doc, cand, path)")
      .with_data({{R"([1])", R"([1])", TestValue::null()}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// Subset / equality cases that should give 1.
TEST_F(JsonContainsExprTest, MysqlContainedTrue)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand)")
      .with_data({
          {R"(null)", R"(null)"},                                            // -> 1
          {R"({"a": 1, "b": 2})", R"({"a": 1})"},                           // object subset -> 1
          {R"([1, 2, 3])", R"([1, 3])"},                                    // vector subset -> 1
          {R"([1, 2, 3])", R"(3)"},                                         // auto-wrap -> 1
          {R"([1,3,5])", R"([5,3,1,5])"},                                   // reorder+dup -> 1
          {R"({"b": 7})", R"({"b": 7})"},                                   // equal object -> 1
          {R"({})", R"({})"},                                               // -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}}));
}

// Cases that should give 0.
TEST_F(JsonContainsExprTest, MysqlContainedFalse)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand)")
      .with_data({
          {R"(3.14)", R"(3)"},                                              // no type conv -> 0
          {R"({"a": {"b": 7}})", R"({"b": 7})"},                            // not top level -> 0
          {R"({"a": 1, "b": 2})", R"("a")"},                                 // just a key -> 0
          {R"([1])", R"([1,2])"},                                           // extra cand elt -> 0
          {R"({})", R"({"a":1})"},                                          // empty vs non -> 0
          {R"({"a":1})", R"({"a":1,"b":2})"},                               // superset cand -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}, {"0"}, {"0"}, {"0"}, {"0"}, {"0"}}));
}

// Arrays "introspect": candidate [2.0] is contained in [1,[2.0,3.0]].
TEST_F(JsonContainsExprTest, MysqlArrayIntrospect)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand)")
      .with_data({
          {R"([1,[2.0, 3.0]])", R"([2.0])"},        // -> 1
          {R"([1, 2, [3, [4, 5]], 6, 7])", R"(5)"},// -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}}));
}

// The path argument selects where to look: {"b":7} is contained at $.a.
TEST_F(JsonContainsExprTest, MysqlPathSelectsSubDocument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand, '$.a')")
      .with_data({{R"({"a": {"b": 7}})", R"({"b": 7})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}}));
}

// Non-existent path -> NULL.
// mysqltest: SELECT JSON_CONTAINS('[1,2]', '1', '$.abc').
TEST_F(JsonContainsExprTest, MysqlNonExistentPathReturnsNull)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand, '$.abc')")
      .with_data({{R"([1,2])", R"(1)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// E: wildcard path is not allowed in json_contains.
// mysqltest: select json_contains('{"width": "800","image": 800}', '800', '$.*').
TEST_F(JsonContainsExprTest, MysqlWildcardPathNotAllowed)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_contains(doc, cand, '$.*')")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH_WILDCARD)
      .with_data({{R"({"width":  "800","image":  800})", R"(800)"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH_WILDCARD, result.get_ret_code());
}

// Check the same inputs with BinView on and off.
TEST_F(JsonContainsExprTest, BinViewSwitchOnArrayContains)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(64)")
        .with_expr("json_contains(cast(s as json), cast(c as json))")
        .with_data({
            {R"([1, 2, 3, 4, 5])", "3"},
            {R"([1, 2, 3, 4, 5])", "6"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

TEST_F(JsonContainsExprTest, BinViewSwitchOffArrayContains)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(64)")
        .with_expr("json_contains(cast(s as json), cast(c as json))")
        .with_data({
            {R"([1, 2, 3, 4, 5])", "3"},
            {R"([1, 2, 3, 4, 5])", "6"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

TEST_F(JsonContainsExprTest, BinViewSwitchOnObjectPathContains)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(64), p varchar(16)")
        .with_expr("json_contains(cast(s as json), cast(c as json), p)")
        .with_data({
            {R"({"a": [1, 2, {"b": 3}], "c": {"d": 4}})", R"({"b": 3})", "$.a"},
            {R"({"a": [1, 2, {"b": 3}], "c": {"d": 4}})", R"({"d": 4})", "$.a"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

TEST_F(JsonContainsExprTest, BinViewSwitchOffObjectPathContains)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(64), p varchar(16)")
        .with_expr("json_contains(cast(s as json), cast(c as json), p)")
        .with_data({
            {R"({"a": [1, 2, {"b": 3}], "c": {"d": 4}})", R"({"b": 3})", "$.a"},
            {R"({"a": [1, 2, {"b": 3}], "c": {"d": 4}})", R"({"d": 4})", "$.a"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

class JsonContainsPathExprTest : public OpTestKit {};

// Reuse one execution context while changing the path at the same argument index.
TEST_F(JsonContainsPathExprTest, DynamicPathChangesAcrossRows)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), path varchar(32)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'all', '$.A.a', '$.A.b', path)")
      .with_data({
          {R"({"A":{"a":1,"b":2,"c":3}})", "$.A.c"},
          {R"({"A":{"a":1,"b":2,"c":3}})", "$.A.missing"},
          {R"({"A":{"a":1,"b":2,"c":3}})", "$.A.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}, {"1"}}));
}

TEST_F(JsonContainsPathExprTest, SharedPrefixAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'all', '$.A.a', '$.A.b', '$.A.c')")
      .with_data({
          {R"({"A":{"a":1,"b":2,"c":3}})"},
          {R"({"A":{"a":1,"b":2}})"},
          {R"({"A":1})"},
          {R"({"A":null})"},
          {R"({"A":{"a":1,"b":2,"c":3}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}, {"0"}, {"0"}, {"1"}}));
}

TEST_F(JsonContainsPathExprTest, SharedPrefixOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'one', '$.A.m', '$.A.a', '$.A.g')")
      .with_data({
          {R"({"A":{}})"},
          {R"({"A":{"g":1}})"},
          {R"({"A":{"m":1}})"},
          {R"({"B":{"a":1}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"0"}, {"1"}, {"1"}, {"0"}}));
}

TEST_F(JsonContainsPathExprTest, DuplicateAndTerminalPrefix)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'all', '$.A', '$.A.b', '$.A.b')")
      .with_data({
          {R"({"A":{"b":1}})"},
          {R"({"A":{"b":1}})"},
          {R"({"A":1})"},
          {R"({"A":null})"},
          {R"({"A":{"b":null}})"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"0"}, {"0"}, {"1"}}));
}

TEST_F(JsonContainsPathExprTest, HandlesTextOneAllArgument)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), mode text")
      .with_expr("json_contains_path(CAST(doc AS JSON), mode, '$.A.a', '$.A.b', '$.A.c')")
      .with_data({
          {R"({"A":{"a":1,"b":2,"c":3}})", "all"},
          {R"({"A":{"a":1,"b":2,"c":3}})", "all"},
          {R"({"A":{"a":1}})", "one"},
          {R"({"A":{"a":1,"b":2}})", "all"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}, {"0"}}));
}

TEST_F(JsonContainsPathExprTest, LaterNullKeepsLazyOneAllSemantics)
{
  auto one_result = expr_unit_test()
      .columns("doc varchar(512), path varchar(32)")
      .with_expr("json_contains_path(doc, 'one', '$.A', path)")
      .with_data({
          {R"({"A":1})", TestValue::null()},
          {R"({"B":1})", TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, one_result.get_ret_code());
  EXPECT_TRUE(one_result.verify_ordered({{"1"}, {TestValue::null()}}));

  auto all_result = expr_unit_test()
      .columns("doc varchar(512), path varchar(32)")
      .with_expr("json_contains_path(doc, 'all', '$.missing', path)")
      .with_data({
          {R"({"A":1})", TestValue::null()},
          {R"({"missing":1})", TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, all_result.get_ret_code());
  EXPECT_TRUE(all_result.verify_ordered({{"0"}, {TestValue::null()}}));
}

TEST_F(JsonContainsPathExprTest, LaterInvalidPathKeepsLazyOneAllSemantics)
{
  auto skipped_result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'one', '$.A', '$[', '$.B')")
      .with_data({{R"({"A":1})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, skipped_result.get_ret_code());
  EXPECT_TRUE(skipped_result.verify_ordered({{"1"}}));

  auto reached_result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_contains_path(CAST(doc AS JSON), 'one', '$.A', '$[', '$.B')")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({{R"({"B":1})"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, reached_result.get_ret_code());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
