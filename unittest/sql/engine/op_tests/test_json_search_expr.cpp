/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Expression-level tests for JSON_SEARCH, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_search_mysql.test
// with expected values taken verbatim from the matching file under
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_search_mysql.result
//
// This is a per-row expression (ObExpr), exercised through expr_unit_test() via
// the real resolve + CG + eval path with a bypass Material operator — no
// create_spec/create_op is needed.
//
// The JSON document and the string arguments (one_or_all / target / escape /
// paths) are fed through varchar columns; OceanBase auto-converts the doc to
// JSON, so the search result (a JSON string path or array of paths) is
// preserved exactly as in mysqltest.
//
// Only RUNTIME (eval-stage) errors are covered here. resolve/parse/type-
// deduction-stage errors (wrong param count OB_ERR_PARAM_SIZE fired by
// calc_result_typeN, unknown-column resolve errors) cannot be asserted through
// this framework: prepare_resolve() does not forward expected_error_ to
// TestSqlUtils::do_resolve(), which carries its own ASSERT_EQ(expect_error=0,
// -ret). Those mysqltest cases stay in the mysqltest suite; the error cases
// below fire when the column value / path is parsed at eval time and surface
// through result.get_ret_code().
//
// The mysqltest cases that need a real table / JSON column (the second half of
// json_search_mysql.test after "JSON-typed input") are also not ported: they
// exercise the LOB-header / JSON_BIN fast path which requires a persisted JSON
// column. The same coverage is already achieved here through varchar columns
// (the auto-convert path) and through cast(... as json) subexpressions; the
// persisted-column path stays in mysqltest.
//
// The cast(... as json) cases from the mysqltest file ARE ported: in
// expr_unit_test the cast is a nested expression resolved + CG'd alongside the
// json_search call, so the JSON_BIN fast path is exercised the same way.

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class JsonSearchExprTest : public OpTestKit {};

TEST_F(JsonSearchExprTest, ConstantSearchParametersAcrossRows)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      for (const char *expr : {
          "json_search(cast(doc as json), 'all', 'needle')",
          "json_search(cast(doc as json), 'all', 'needle', '!', '$.*')",
          "json_search(cast(doc as json), 'all', 'needle', null, '$.*')"}) {
        auto result = expr_unit_test()
            .columns("doc varchar(128)")
            .with_expr(expr)
            .with_rescan_times(2)
            .with_data({
                {TestValue::null()},
                {R"({"a":"needle","b":"needle"})"},
                {R"({"c":"miss"})"},
                {R"({"d":["needle"]})"},
                {R"({"a":"needle"})"},
            })
            .run(engine_);
        EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
        EXPECT_TRUE(result.verify_ordered({
            {TestValue::null()}, {R"(["$.a", "$.b"])"}, {TestValue::null()},
            {R"("$.d[0]")"}, {R"("$.a")"},
        }));
      }
    }
  }
}

TEST_F(JsonSearchExprTest, ConstantTargetWithDynamicEscapeAndPaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), escape varchar(4), p varchar(32)")
      .with_expr("json_search(cast(doc as json), 'all', 'a!b', escape, '$.a', p)")
      .with_data({
          {R"({"a":"ab","b":"a!b"})", "!", "$.b"},
          {R"({"a":"ab","b":"a!b"})", "#", "$.b"},
          {R"({"a":"ab","b":"a!b"})", "#", "$.missing"},
          {R"({"a":"ab","b":"a!b"})", "!", TestValue::null()},
          {R"({"a":"ab","b":"a!b"})", "!", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"("$.a")"}, {R"("$.b")"}, {TestValue::null()},
      {TestValue::null()}, {R"("$.a")"},
  }));
}

TEST_F(JsonSearchExprTest, ConstantEscapeWithDynamicTargetAndMode)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, '!', '$[*]')")
      .with_data({
          {R"(["ab","axb"])", "all", "a%b"},
          {R"(["ab","axb"])", "all", "ab"},
          {R"(["ab","axb"])", "one", "a%b"},
          {R"(["ab","axb"])", "all", TestValue::null()},
          {R"(["ab","axb"])", "all", "a_b"},
          {R"(["ab","axb"])", "all", "a%b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"(["$[0]", "$[1]"])"}, {R"("$[0]")"}, {R"("$[0]")"},
      {TestValue::null()}, {R"("$[1]")"}, {R"(["$[0]", "$[1]"])"},
  }));
}

TEST_F(JsonSearchExprTest, ConstantPatternCacheDoesNotCrossExecutions)
{
  const char *cases[][2] = {
      {"json_search(cast(doc as json), 'all', 'a!b', '!')", R"("$[0]")"},
      {"json_search(cast(doc as json), 'all', 'a!b', '#')", R"("$[1]")"},
      {"json_search(cast(doc as json), 'all', 'ab', '!')", R"("$[0]")"},
      {"json_search(cast(doc as json), 'all', '', '!')", R"("$[2]")"},
  };
  for (const auto &test_case : cases) {
    auto result = expr_unit_test()
        .columns("doc varchar(128)")
        .with_expr(test_case[0])
        .with_data({{R"(["ab","a!b",""])"}, {R"(["ab","a!b",""])"}})
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{test_case[1]}, {test_case[1]}}));
  }
}

TEST_F(JsonSearchExprTest, CachedParametersPreserveNullAndErrorOrder)
{
  for (const char *expr : {
      "json_search(cast(doc as json), null, 'x', 'invalid')",
      "json_search(cast(doc as json), 'all', null, '!')",
      "json_search(cast(doc as json), 'all', 'x', null, '$.a', null)",
      "json_search(cast(doc as json), 'all', 'x', null, '')"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(64)")
        .with_expr(expr)
        .with_data({{R"({"a":"x"})"}, {R"({"a":"x"})"}})
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{TestValue::null()}, {TestValue::null()}}));
  }
}

TEST_F(JsonSearchExprTest, CachedParametersValidateDynamicTrailingPath)
{
  for (const char *expr : {
      "json_search(cast(doc as json), 'one', 'x', '!', '$.a', p)",
      "json_search(cast(doc as json), 'one', 'x', null, '$.a', p)"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(64), p varchar(32)")
        .with_expr(expr)
        .expect_error(common::OB_ERR_INVALID_JSON_PATH)
        .with_data({{R"({"a":"x"})", "$.b"}, {R"({"a":"x"})", "$bad"}})
        .run(engine_);
    EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
  }
}

TEST_F(JsonSearchExprTest, ConstantSearchArgumentsWithDynamicPaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), p varchar(32)")
      .with_expr("json_search(cast(doc as json), 'all', 'a!%b', '!', p)")
      .with_rescan_times(2)
      .with_data({
          {R"({"a":"a%b","b":"a%b"})", "$.a"},
          {R"({"a":"a%b","b":"a%b"})", "$.b"},
          {R"({"a":"a%b","b":"a%b"})", TestValue::null()},
          {R"({"a":"a%b","b":"a%b"})", ""},
          {R"({"a":"a%b","b":"a%b"})", "$.*"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"("$.a")"}, {R"("$.b")"}, {TestValue::null()},
      {TestValue::null()}, {R"(["$.a", "$.b"])"},
  }));
}

TEST_F(JsonSearchExprTest, NullTargetStillValidatesDynamicEscape)
{
  auto escape_result = expr_unit_test()
      .columns("doc varchar(64), escape varchar(16)")
      .with_expr("json_search(cast(doc as json), 'all', null, escape)")
      .expect_error(common::OB_INVALID_ARGUMENT)
      .with_data({{R"({"a":"x"})", "!"}, {R"({"a":"x"})", "invalid"}})
      .run(engine_);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, escape_result.get_ret_code());
}

TEST_F(JsonSearchExprTest, LiteralMatching)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      for (const char *expr : {"json_search(doc, one_or_all, target)",
                               "json_search(cast(doc as json), one_or_all, target)"}) {
        auto result = expr_unit_test()
            .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
            .with_expr(expr)
            .with_data({
                {R"(["abc","ABC","abc ","abcd","xabc"])", "all", "abc"},
                {R"(["abc","abc "])", "all", "abc "},
                {R"([""," ","x"])", "all", ""},
                {R"(["abcd","ab"])", "all", "abc"},
                {R"(["中文","中文 ","中","😀"])", "all", "中文"},
                {R"(["中文","😀"])", "all", "😀"},
                {R"(["a\u0000b","a","a\u0000c"])", "all", std::string("a\0b", 3)},
                {R"({"a":["miss",{"k":"needle"}],"b":"needle"})", "all", "needle"},
                {R"({"a":["miss",{"k":"needle"}],"b":"needle"})", "one", "needle"},
            })
            .run(engine_);
        EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
        EXPECT_TRUE(result.verify_ordered({
            {R"("$[0]")"},
            {R"("$[1]")"},
            {R"("$[0]")"},
            {TestValue::null()},
            {R"("$[0]")"},
            {R"("$[1]")"},
            {R"("$[0]")"},
            {R"(["$.a[1].k", "$.b"])"},
            {R"("$.a[1].k")"},
        }));
      }
    }
  }
}

TEST_F(JsonSearchExprTest, EscapedLiteralMatching)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      auto result = expr_unit_test()
          .columns("doc varchar(128), target varchar(32), escape varchar(4)")
          .with_expr("json_search(cast(doc as json), 'all', target, escape)")
          .with_data({
              {R"(["a!b","a!!b"])", "a!!b", "!"},
              {R"(["ab","a!b"])", "a!b", "!"},
              {R"(["a%b","axxb"])", "a!%b", "!"},
              {R"(["a_b","acb"])", "a!_b", "!"},
              {R"(["a\\b","a\\\\b"])", R"(a\\b)", TestValue::null()},
              {R"(["ab","a\\b"])", R"(a\b)", TestValue::null()},
              {R"(["ab","axb"])", "a%b", "!"},
              {R"(["ab","axb"])", "ab", "!"},
              {R"(["ab","axb"])", "a_b", "!"},
              {R"(["ab","axb"])", "ab", "中"},
          })
          .run(engine_);
      EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
      EXPECT_TRUE(result.verify_ordered({
          {R"("$[0]")"},
          {R"("$[0]")"},
          {R"("$[0]")"},
          {R"("$[0]")"},
          {R"("$[0]")"},
          {R"("$[0]")"},
          {R"(["$[0]", "$[1]"])"},
          {R"("$[0]")"},
          {R"("$[1]")"},
          {R"("$[0]")"},
      }));
    }
  }
}

TEST_F(JsonSearchExprTest, PreparedSearchPatterns)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      for (const char *expr : {"json_search(doc, 'all', target, escape)",
                               "json_search(cast(doc as json), 'all', target, escape)"}) {
        auto result = expr_unit_test()
            .columns("doc varchar(256), target varchar(64), escape varchar(4)")
            .with_expr(expr)
            .with_data({
                {R"(["a_b%b","axbxb","a_bxb"])", R"(a\_b\%b)", "\\"},
                {R"(["a_x","abx","a_"])", R"(a\_%)", "\\"},
                {R"(["a%x","a%","axx"])", "a!%_", "!"},
                {R"(["a\\xxx","axxx"])", R"(a\\%)", "\\"},
                {R"(["a\\","a"])", R"(a\)", "\\"},
                {R"(["a_b","axb"])", "a%_b", "%"},
                {R"(["a%b","axb"])", "a_%b", "_"},
                {R"(["a_b","axb"])", "a__b", "_"},
                {R"(["a%","a"])", "a%", "%"},
                {R"(["a_","a"])", "a_", "_"},
                {R"(["a中","a!中"])", "a!中", "!"},
                {R"(["a😀%","a😀x"])", "a!😀!%", "!"},
                {R"(["a中b","ab"])", "a中b", "中"},
                {R"(["a中xb","axb"])", "a中_b", "中"},
                {R"(["a\u0000b","ab"])", std::string("a!\0b", 4), "!"},
                {R"(["ab","a\u0000b"])", std::string("a\0b", 3), std::string("\0", 1)},
                {R"(["","!"])", "", "!"},
                {R"(["!",""])", "!", "!"},
            })
            .run(engine_);
        EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
        EXPECT_TRUE(result.verify_ordered({
            {R"("$[0]")"}, {R"(["$[0]", "$[2]"])"},
            {R"("$[0]")"}, {R"("$[0]")"}, {R"("$[0]")"},
            {R"("$[0]")"}, {R"("$[0]")"}, {R"("$[0]")"},
            {R"("$[0]")"}, {R"("$[0]")"}, {R"("$[0]")"},
            {R"("$[0]")"}, {R"("$[0]")"}, {R"("$[0]")"},
            {R"("$[0]")"}, {R"("$[0]")"}, {R"("$[0]")"},
            {R"("$[0]")"},
        }));
      }
    }
  }
}

TEST_F(JsonSearchExprTest, CachedPreparedSearchPatterns)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      for (const char *pattern : {"a!_b!%b", "a!_b%b"}) {
        const std::string expr = std::string("json_search(cast(doc as json), 'all', '")
            + pattern + "', '!')";
        auto result = expr_unit_test()
            .columns("doc varchar(128)")
            .with_expr(expr.c_str())
            .with_rescan_times(2)
            .with_data({
                {R"(["a_b%b","axbxb"])"},
                {R"(["miss"])"},
                {R"(["other","a_b%b"])"},
            })
            .run(engine_);
        EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
        EXPECT_TRUE(result.verify_ordered({
            {R"("$[0]")"}, {TestValue::null()}, {R"("$[1]")"},
        }));
      }
    }
  }
}

TEST_F(JsonSearchExprTest, IteratorSearchCandidates)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      auto result = expr_unit_test()
          .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), p varchar(64)")
          .with_expr("json_search(cast(doc as json), one_or_all, target, null, p)")
          .with_data({
              {R"([{"k":"miss"},{"k":"needle"},{"k":"needle"}])", "one", "needle", "$[*].k"},
              {R"([{"k":"miss"},{"k":"needle"},{"k":"needle"}])", "all", "needle", "$[*].k"},
              {R"([{"k":"miss"},{"k":"needle"}])", "one", "absent", "$[*].k"},
              {R"({"a":{"a":{"k":"x"},"k":"x"}})", "all", "x", "$**.a**.k"},
              {R"({"a":{"a":{"k":"x"},"k":"x"}})", "one", "x", "$**.a**.k"},
              {R"(["miss","x","x","x"])", "all", "x", "$[last-1 to last]"},
              {R"({"a.b":["miss","x"]})", "one", "x", "$.*[*]"},
              {R"(["miss","x"])", "one", "x", "$[last]"},
              {R"(["x","miss"])", "one", "x", "$[last-1]"},
              {R"([{"k":"miss"},{"k":"x"}])", "all", "x", "$[last]"},
              {R"([["x","x"],["miss"]])", "all", "x", "$[last-1]"},
              {R"([["miss","x"],["x"]])", "all", "x", "$[*][last]"},
              {R"({"a":["x","x"]})", "one", "x", "$.a"},
              {R"({"a":["x","x"]})", "all", "x", "$.a"},
              {R"({"a":"x"})", "all", "x", "$.missing"},
              {R"({"a":"x"})", "all", "x", "$.a[0]"},
              {R"({"a":"x"})", "all", "x", "$"},
          })
          .run(engine_);
      EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
      EXPECT_TRUE(result.verify_ordered({
          {R"("$[1].k")"},
          {R"(["$[1].k", "$[2].k"])"},
          {TestValue::null()},
          {R"(["$.a.k", "$.a.a.k"])"},
          {R"("$.a.k")"},
          {R"(["$[2]", "$[3]"])"},
          {R"("$.\"a.b\"[1]")"},
          {R"("$[last]")"},
          {R"("$[last-1]")"},
          {R"("$[last].k")"},
          {R"(["$[last-1][0]", "$[last-1][1]"])"},
          {R"(["$[0][1]", "$[1][0]"])"},
          {R"("$.a[0]")"},
          {R"(["$.a[0]", "$.a[1]"])"},
          {TestValue::null()},
          {TestValue::null()},
          {R"("$.a")"},
      }));
    }
  }
}

TEST_F(JsonSearchExprTest, IteratorOneValidatesTrailingPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), p1 varchar(32), p2 varchar(32)")
      .with_expr("json_search(cast(doc as json), 'one', 'x', null, p1, p2)")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({{R"({"a":"x"})", "$.*", "$bad"}})
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
}

TEST_F(JsonSearchExprTest, IteratorOverlappingSearchRoots)
{
  for (const char *enabled : {"true", "false"}) {
    WITH_TENANT_CONFS({"_enable_json_bin_view", enabled}) {
      auto result = expr_unit_test()
          .columns("doc varchar(128), p1 varchar(32), p2 varchar(32)")
          .with_expr("json_search(cast(doc as json), 'all', 'x', null, p1, p2)")
          .with_data({{R"({"a":{"a":{"k":"x"},"k":"x"}})", "$**.a", "$**.k"}})
          .run(engine_);
      EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
      EXPECT_TRUE(result.verify_ordered({{R"(["$.a.a.k", "$.a.k"])"}}));
    }
  }
}

// ============================================================================
// Scenarios ported from mysqltest suite
//   tools/deploy/mysql_test/test_suite/json/t/json_search_mysql.test
// with expected values taken verbatim from
//   tools/deploy/mysql_test/test_suite/json/r/mysql/json_search_mysql.result
//
// These are pure-expression cases (no table DML), so they map naturally onto
// expr_unit_test(). The first argument is a JSON document; in mysqltest it is a
// string literal that OceanBase auto-converts to JSON, so here we feed the
// same JSON text through a varchar column and let JSON_SEARCH coerce it.
// ============================================================================

// M1: null doc argument — json_search(null, 'one', 'foo') -> NULL.
TEST_F(JsonSearchExprTest, MysqlNullDoc)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {TestValue::null(), "one", "foo"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M2: null one_or_all argument — json_search('{...}', null, 'foo') -> NULL.
TEST_F(JsonSearchExprTest, MysqlNullOneOrAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a": "foo" })", TestValue::null(), "foo"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M3: null escape + null trailing path — whole call returns NULL.
TEST_F(JsonSearchExprTest, MysqlNullEscapeAndPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"({ "a": "foo" })", "one", "foo", TestValue::null(), TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M4: null escape, valid path, then null trailing path — returns NULL.
TEST_F(JsonSearchExprTest, MysqlNullTrailingPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": "foo" })", "one", "foo", TestValue::null(), "$.a", TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M5: simple search — one match returns a single path.
TEST_F(JsonSearchExprTest, MysqlSimpleOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a": "foobar" })", "one", "foo%"},
          {R"({ "a": "foobar", "b": "focus", "c": [ "arm", "foot", "shoulder" ] })", "one", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a")"}, {R"("$.a")"}}));
}

// M6: simple search — all matches return an array of paths.
TEST_F(JsonSearchExprTest, MysqlSimpleAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a": "foobar", "b": "focus", "c": [ "arm", "foot", "shoulder" ] })", "all", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a", "$.c[1]"])"}}));
}

// M7: underscore wildcard — f__us matches "focus".
TEST_F(JsonSearchExprTest, MysqlUnderscoreWildcard)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a": "foobar", "b": "focus", "c": [ "arm", "foot", "shoulder" ] })", "all", "f__us"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.b")"}}));
}

// M8: path-scoped search — all over $.a, matches inside the array.
TEST_F(JsonSearchExprTest, MysqlPathScopedAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "all", "foo%", TestValue::null(), "$.a"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]"])"}}));
}

// M9: multiple path args — all over $.a and $.b.
TEST_F(JsonSearchExprTest, MysqlMultiplePathsAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "all", "foo%", TestValue::null(), "$.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]", "$.b"])"}}));}

// M10: one with multiple path args — returns first match only.
TEST_F(JsonSearchExprTest, MysqlMultiplePathsOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "one", "foo%", TestValue::null(), "$.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a[0]")"}}));
}

// M11: case-insensitive one_or_all keyword — ALL / aLl / ONE.
TEST_F(JsonSearchExprTest, MysqlCaseInsensitiveKeyword)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .with_data({
          // mysqltest line 52 uses a 5-arg call (ALL + $.a). The 6-arg column
          // layout repeats $.a as p2 so the extra path arg does not change
          // the result (duplicates are de-duped by add_path_unique).
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "ALL", "foo%", TestValue::null(), "$.a", "$.a"},
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "aLl", "foo%", TestValue::null(), "$.a", "$.b"},
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "ONE", "foo%", TestValue::null(), "$.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"(["$.a[0]", "$.a[2]"])"},
      {R"(["$.a[0]", "$.a[2]", "$.b"])"},
      {R"("$.a[0]")"},
  }));
}

// M12: wildcard $**.c — recursive descent over array elements.
TEST_F(JsonSearchExprTest, MysqlRecursiveWildcard)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} } ])",
           "all", "foo%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%e", TestValue::null(), "$**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"(["$[0].a.b.c", "$[2].c.c"])"},
      {R"(["$[0].a.b.c", "$[1].b.c", "$[2].c.c"])"},
      {R"(["$[0].a.b.c", "$[2].c.c"])"},
  }));
}

// M13: wildcard $[*].c — array element member; only one match here.
TEST_F(JsonSearchExprTest, MysqlArrayWildcardMember)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%", TestValue::null(), "$[*].c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[2].c.c")"}}));
}

// M14: wildcard $[1]**.c — recursive descent from array index 1.
TEST_F(JsonSearchExprTest, MysqlIndexRecursiveWildcard)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "showtime" } } }, [ { "b": { "c": "shout" } }, { "c": { "c": "shoe"} } ] ])",
           "all", "sho%", TestValue::null(), "$[1]**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[1][0].b.c", "$[1][1].c.c"])"}}));}

// M15: escape character — 'foo%bar' with % as wildcard matches both.
TEST_F(JsonSearchExprTest, MysqlEscapeDefault)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo%bar"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[0]", "$[1]"])"}}));
}

// M16: escape with backslash — 'foo\%bar' escapes %, only $[1] matches.
TEST_F(JsonSearchExprTest, MysqlBackslashEscape)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", R"(foo\%bar)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// M17: custom escape '|' — 'foo|%bar' with | escaping %.
TEST_F(JsonSearchExprTest, MysqlCustomEscape)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(doc, one_or_all, target, escape)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// M18: custom escape + path scope — path selects no matching element -> NULL.
TEST_F(JsonSearchExprTest, MysqlCustomEscapePathScoped)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[0]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M19: custom escape + two path args — only $[1] is selected and matches.
TEST_F(JsonSearchExprTest, MysqlCustomEscapeTwoPaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[0]", "$[1]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// M20: custom escape + three path args — $[2] does not exist, still $[1].
TEST_F(JsonSearchExprTest, MysqlCustomEscapeThreePaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16), p3 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2, p3)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[0]", "$[1]", "$[2]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// M21: null escape uses default backslash — 'foo\%bar' with null escape.
TEST_F(JsonSearchExprTest, MysqlNullEscapeDefaultBackslash)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(doc, one_or_all, target, escape)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", R"(foo\%bar)", TestValue::null()},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// M22: null escape + path scope — $[0] does not match 'foo\%bar' -> NULL.
TEST_F(JsonSearchExprTest, MysqlNullEscapePathScoped)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", R"(foo\%bar)", TestValue::null(), "$[0]"},
          {R"([ "footbar", "foo%bar" ])", "all", R"(foo\%bar)", TestValue::null(), "$[1]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}, {R"("$[1]")"}}));
}

// M23: custom escape + single path arg.
TEST_F(JsonSearchExprTest, MysqlCustomEscapeSinglePath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[0]"},
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[1]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}, {R"("$[1]")"}}));
}

// M24: search is case-sensitive — 'aBc' matches nothing.
TEST_F(JsonSearchExprTest, MysqlCaseSensitive)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"([ "abc", "ABC" ])", "all", "aBc"},
          {R"([ "abc", "ABC" ])", "all", "abc"},
          {R"([ "abc", "ABC" ])", "all", "ABC"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {TestValue::null()},
      {R"("$[0]")"},
      {R"("$[1]")"},
  }));
}

// M25: only strings match — numeric values are ignored.
TEST_F(JsonSearchExprTest, MysqlOnlyStringsMatch)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"([ 10, "10", 1.0, "1.0" ])", "all", "1%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[1]", "$[3]"])"}}));
}

// M26: wl7909 spec example — numeric 123 does not match string '123'.
TEST_F(JsonSearchExprTest, MysqlNumericNoMatch)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a" : 123, "b" : [ 123, 456 ] })", "one", "123"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M27: one match inside $.b array — returns "$.b[2]".
TEST_F(JsonSearchExprTest, MysqlOneInArray)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"({ "a" : "123", "b" : [ 123, "789", "123", "456", "123" ] })", "one", "123", TestValue::null(), "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.b[2]")"}}));
}

// M28: one match in object — either $.a or $.b.key; here $.a.
TEST_F(JsonSearchExprTest, MysqlOneInObject)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a" : "123", "b" : { "key" : "123" } })", "one", "123"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a")"}}));
}

// M29: prefix wildcard in nested object — $.b.key matches '123%'.
TEST_F(JsonSearchExprTest, MysqlPrefixWildcardNested)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .with_data({
          {R"({ "a" : "1243", "b" : { "key" : "1234" } })", "one", "123%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.b.key")"}}));
}

// M30: custom escape '@' — 'dir%torysub@%dir%' matches $.b.c.
TEST_F(JsonSearchExprTest, MysqlCustomAtEscape)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(doc, one_or_all, target, escape)")
      .with_data({
          {R"({ "a" : "1243", "b" : { "key" : "1234", "c": "directorysub%directoryabc" } })",
           "one", R"(dir%torysub@%dir%)", "@"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.b.c")"}}));
}

// M31: non-existent path — $.c does not exist -> NULL.
TEST_F(JsonSearchExprTest, MysqlNonExistentPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"({ "a" : "1243", "b" : { "key" : "1234" } })", "one", "123%", TestValue::null(), "$.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{TestValue::null()}}));
}

// M32: quoted member key with space — $."one potato" via JSON_UNQUOTE(JSON_SEARCH).
TEST_F(JsonSearchExprTest, MysqlQuotedKeyWithSpace)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_unquote(json_search(doc, one_or_all, target))")
      .with_data({
          {R"({ "onepotato": "foot", "one potato": "food" , "one \"potato": "fool" })", "all", "food"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"($."one potato")"}}));
}

// C1: cast as json — simple one match.
TEST_F(JsonSearchExprTest, MysqlCastJsonSimpleOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(cast(doc as json), one_or_all, target)")
      .with_data({
          {R"({ "a": "foobar", "b": "focus", "c": [ "arm", "foot", "shoulder" ] })", "one", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a")"}}));
}

// C2: cast as json — all matches.
TEST_F(JsonSearchExprTest, MysqlCastJsonSimpleAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(cast(doc as json), one_or_all, target)")
      .with_data({
          {R"({ "a": "foobar", "b": "focus", "c": [ "arm", "foot", "shoulder" ] })", "all", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a", "$.c[1]"])"}}));
}

// C3: cast as json — path-scoped all over $.a.
TEST_F(JsonSearchExprTest, MysqlCastJsonPathScopedAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "all", "foo%", TestValue::null(), "$.a"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]"])"}}));
}

// C4: cast as json — all over $.a and $.b.
TEST_F(JsonSearchExprTest, MysqlCastJsonMultiplePathsAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "all", "foo%", TestValue::null(), "$.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]", "$.b"])"}}));}

// C5: cast as json — one over $.a and $.b.
TEST_F(JsonSearchExprTest, MysqlCastJsonMultiplePathsOne)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "one", "foo%", TestValue::null(), "$.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a[0]")"}}));
}

// C6: cast as json — wildcard $**.c recursive descent.
TEST_F(JsonSearchExprTest, MysqlCastJsonRecursiveWildcard)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} } ])",
           "all", "foo%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%", TestValue::null(), "$[*].c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, [ { "b": { "c": "shout" } }, { "c": { "c": "shoe"} } ] ])",
           "all", "sho%", TestValue::null(), "$[1]**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"(["$[0].a.b.c", "$[2].c.c"])"},
      {R"(["$[0].a.b.c", "$[1].b.c", "$[2].c.c"])"},
      {R"("$[2].c.c")"},
      {R"(["$[1][0].b.c", "$[1][1].c.c"])"},
  }));
}

// C7: cast as json — escape character + array index path.
TEST_F(JsonSearchExprTest, MysqlCastJsonEscapeAndPath)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// C8: cast as json — escape character + two array index paths.
TEST_F(JsonSearchExprTest, MysqlCastJsonEscapeTwoPaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|", "$[0]", "$[1]"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// C9: cast as json — key with space (exercises bin get_location escaping).
TEST_F(JsonSearchExprTest, MysqlCastJsonKeyWithSpace)
{
  auto result = expr_unit_test()
      .columns("doc varchar(128), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(cast(doc as json), one_or_all, target)")
      .with_data({
          {R"({ "onepotato": "foot", "one potato": "food" , "one \"potato": "fool" })", "all", "food"},
          {R"({ "onepotato": "foot", "one potato": "food" , "one \"potato": "fool" })", "all", "fool"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.\"one potato\"")"}, {R"("$.\"one \\\"potato\"")"}}));
}

// E1: bad one_or_all value — 'twof' is not 'one' or 'all'.
// mysqltest line 25: json_search('{ "a": "foo" }', 'twof', 'foo').
TEST_F(JsonSearchExprTest, MysqlBadOneOrAllTwof)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .expect_error(common::OB_ERR_JSON_BAD_ONE_OR_ALL_ARG)
      .with_data({
          {R"({ "a": "foo" })", "twof", "foo"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_JSON_BAD_ONE_OR_ALL_ARG, result.get_ret_code());
}

// E2: bad one_or_all value — 'two' is not 'one' or 'all'.
// mysqltest line 27: json_search('{ "a": "foo" }', 'two', 'foo').
TEST_F(JsonSearchExprTest, MysqlBadOneOrAllTwo)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, target)")
      .expect_error(common::OB_ERR_JSON_BAD_ONE_OR_ALL_ARG)
      .with_data({
          {R"({ "a": "foo" })", "two", "foo"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_JSON_BAD_ONE_OR_ALL_ARG, result.get_ret_code());
}

// E3: bad escape argument — 'ab' is not a single-character escape.
// mysqltest line 31: json_search('{ "a": "foo" }', 'one', 'foo', 'ab').
TEST_F(JsonSearchExprTest, MysqlBadEscape)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(doc, one_or_all, target, escape)")
      .expect_error(common::OB_INVALID_ARGUMENT)
      .with_data({
          {R"({ "a": "foo" })", "one", "foo", "ab"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, result.get_ret_code());
}

// E4: invalid JSON path — '$a' is not a legal path.
// mysqltest line 35: json_search('{ "a": "foo" }', 'one', 'foo', null, '$a').
TEST_F(JsonSearchExprTest, MysqlInvalidPathDollarA)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({
          {R"({ "a": "foo" })", "one", "foo", TestValue::null(), "$a"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
}

// E5: invalid JSON path in a later position — '$b' is not a legal path.
// mysqltest line 37: json_search('{ "a": "foo" }', 'all', 'foo', null, '$.a', '$b').
TEST_F(JsonSearchExprTest, MysqlInvalidPathLaterPosition)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1, p2)")
      .expect_error(common::OB_ERR_INVALID_JSON_PATH)
      .with_data({
          {R"({ "a": "foo" })", "all", "foo", TestValue::null(), "$.a", "$b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_PATH, result.get_ret_code());
}

// B1-a: one_or_all argument in non-utf8mb4 charset.
// Covers ob_expr_json_search.cpp:266-271 (convert_string_collation_type of
// one_or_all). Mirrors M5's one-match case.
TEST_F(JsonSearchExprTest, MysqlSearchNonUtf8OneOrAll)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, convert(one_or_all using latin1), target)")
      .with_data({
          {R"({ "a": "foobar" })", "one", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a")"}}));
}

// B1-b: escape argument in non-utf8mb4 charset.
// Covers ob_expr_json_search.cpp:301-307 (convert_string_collation_type of
// escape). Mirrors M17's custom-escape single-match case.
TEST_F(JsonSearchExprTest, MysqlSearchNonUtf8Escape)
{
  auto result = expr_unit_test()
      .columns("doc varchar(64), one_or_all varchar(8), target varchar(32), escape varchar(4)")
      .with_expr("json_search(doc, one_or_all, target, convert(escape using latin1))")
      .with_data({
          {R"([ "footbar", "foo%bar" ])", "all", "foo|%bar", "|"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$[1]")"}}));
}

// B1-c: target argument in non-utf8mb4 charset.
// Covers ob_expr_json_search.cpp:337-343 (convert_string_collation_type of
// target). Mirrors M5's one-match case with the target converted.
TEST_F(JsonSearchExprTest, MysqlSearchNonUtf8Target)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32)")
      .with_expr("json_search(doc, one_or_all, convert(target using latin1))")
      .with_data({
          {R"({ "a": "foobar" })", "one", "foo%"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a")"}}));
}

// B1-d: path argument in non-utf8mb4 charset.
// Covers ob_expr_json_search.cpp:385-390 (convert_string_collation_type of
// path). Mirrors M8's path-scoped all case with the path converted.
TEST_F(JsonSearchExprTest, MysqlSearchNonUtf8Path)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, convert(p1 using latin1))")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })", "all", "foo%", TestValue::null(), "$.a"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]"])"}}));
}

// S1: cast-as-json + deeply nested doc + $**.c recursive wildcard + all.
// Mirrors M12 but with cast-as-json and a 4th element nested 4 levels deep,
// exercising the bin-view recursive-descent seek path against the DOM seek
// path. Uses VEC_CONTINUOUS for the doc column because the JSON text (135
// bytes) exceeds the framework's 128-byte per-row varchar buffer limit
// (check_var_len_limit); other columns use VEC_DISCRETE (short strings).
TEST_F(JsonSearchExprTest, MysqlCastJsonRecursiveWildcardDeeplyNested)
{
  auto result = expr_unit_test()
      .columns("doc varchar(2048), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
      .with_col_formats(VEC_CONTINUOUS, VEC_DISCRETE, VEC_DISCRETE, VEC_DISCRETE, VEC_DISCRETE)
      .with_data({
          {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} }, { "d": { "e": { "f": { "c": "foo" } } } } ])",
           "all", "foo%", TestValue::null(), "$**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[0].a.b.c", "$[2].c.c", "$[3].d.e.f.c"])"}}));
}

// S1b: minimal repro — same as C6 row 1 but with a 4th element added at 3
// levels deep (one less than S1). Verifies the seek works when the doc fits
// within 128 bytes.
TEST_F(JsonSearchExprTest, MysqlCastJsonRecursiveWildcardFourthElem)
{
  auto result = expr_unit_test()
      .columns("doc varchar(2048), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} }, { "d": { "c": "foo" } } ])",
           "all", "foo%", TestValue::null(), "$**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[0].a.b.c", "$[2].c.c", "$[3].d.c"])"}}));
}

// S2: cast-as-json + mixed can_match_many then non-can_match_many paths.
// First path $**.a is a wildcard (can_match_many=true), second path $.b is
// non-wildcard. After the first seek the seek_wrapper state must not corrupt
// the second path's seek.
TEST_F(JsonSearchExprTest, MysqlCastJsonMixedCanMatchMany)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool", "x": { "a": "foot" } })",
           "all", "foo%", TestValue::null(), "$**.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$.a[0]", "$.a[2]", "$.x.a", "$.b"])"}}));
}

// S3: cast-as-json + array with $[*].c array wildcard + multiple hits.
// Stresses the bin-view seek_member_wildcard path over array elements with
// 4 matches across 10 elements. Doc kept under 128 bytes to fit the
// framework's per-row varchar buffer limit.
TEST_F(JsonSearchExprTest, MysqlCastJsonArrayWildcardLarge)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
      .with_data({
          {R"([{"c":"one"},{"c":"two"},{"c":"foo"},{"c":"bar"},{"c":"foo"},{"c":"x"},{"c":"foo"},{"c":"z"},{"c":"foo"},{"c":"r"}])",
           "all", "foo%", TestValue::null(), "$[*].c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"(["$[2].c", "$[4].c", "$[6].c", "$[8].c"])"}}));
}

// S4: cast-as-json + one mode + multiple can_match_many paths.
// 'one' must short-circuit after the first hit across bin-view seeks.
TEST_F(JsonSearchExprTest, MysqlCastJsonOneMultiplePaths)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16), p2 varchar(16)")
      .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1, p2)")
      .with_data({
          {R"({ "a": [ "foolish", "folly", "foolhardy"  ], "b" : "fool" })",
           "one", "foo%", TestValue::null(), "$**.a", "$.b"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{R"("$.a[0]")"}}));
}

// ============================================================================
// _enable_json_bin_view switch comparison tests.
//
// JSON_SEARCH with explicit paths uses ObJsonSeekIterator::init/next to yield
// candidate nodes and their actual paths, then calls find_matches on each
// candidate subtree. The underlying representation depends on input type:
// varchar input is auto-converted to JSON_TREE (get_json_internal_type
// returns JSON_TREE for non-ObJsonType), so the switch has no effect on B1.
// cast(doc as json) produces ObJsonType, so get_json_internal_type returns
// JSON_BIN; with the switch ON the iterator traverses ObJsonBinView sub-views
// without materializing a tree. With the switch OFF the wrapper holds a legacy
// ObJsonBin, which JSON_SEARCH converts to a DOM once before traversing any path.
// B2/B3 exercise both switch states on the JSON_BIN path and assert the
// results match byte-for-byte, checking that iterator traversal and string
// matching agree across the binary-view and DOM representations.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: varchar input — auto-convert to JSON_TREE path. Baseline for B2/B3
// (cast as json -> JSON_BIN path).
TEST_F(JsonSearchExprTest, AutoConvertRecursiveWildcard)
{
  auto result = expr_unit_test()
      .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
               "escape varchar(4), p1 varchar(16)")
      .with_expr("json_search(doc, one_or_all, target, escape, p1)")
      .with_data({
          {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} } ])",
           "all", "foo%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%", TestValue::null(), "$**.c"},
          {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
           "all", "sho%e", TestValue::null(), "$**.c"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {R"(["$[0].a.b.c", "$[2].c.c"])"},
      {R"(["$[0].a.b.c", "$[1].b.c", "$[2].c.c"])"},
      {R"(["$[0].a.b.c", "$[2].c.c"])"},
  }));
}

// B2: cast(doc as json) — JSON_BIN path, switch ON -> ObJsonBinView.
TEST_F(JsonSearchExprTest, BinViewSwitchOnCastAsJsonWildcard)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
                 "escape varchar(4), p1 varchar(16)")
        .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
        .with_data({
            {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} } ])",
             "all", "foo%", TestValue::null(), "$**.c"},
            {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
             "all", "sho%", TestValue::null(), "$**.c"},
            {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
             "all", "sho%e", TestValue::null(), "$**.c"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({
        {R"(["$[0].a.b.c", "$[2].c.c"])"},
        {R"(["$[0].a.b.c", "$[1].b.c", "$[2].c.c"])"},
        {R"(["$[0].a.b.c", "$[2].c.c"])"},
    }));
  }
}

// B3: cast(doc as json) — JSON_BIN path, switch OFF -> DOM. Compare with B2.
TEST_F(JsonSearchExprTest, BinViewSwitchOffCastAsJsonWildcard)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("doc varchar(256), one_or_all varchar(8), target varchar(32), "
                 "escape varchar(4), p1 varchar(16)")
        .with_expr("json_search(cast(doc as json), one_or_all, target, escape, p1)")
        .with_data({
            {R"([ { "a": { "b": { "c": "fool" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "food"} } ])",
             "all", "foo%", TestValue::null(), "$**.c"},
            {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
             "all", "sho%", TestValue::null(), "$**.c"},
            {R"([ { "a": { "b": { "c": "showtime" } } }, { "b": { "c": "shoulder" } }, { "c": { "c": "shoe"} } ])",
             "all", "sho%e", TestValue::null(), "$**.c"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({
        {R"(["$[0].a.b.c", "$[2].c.c"])"},
        {R"(["$[0].a.b.c", "$[1].b.c", "$[2].c.c"])"},
        {R"(["$[0].a.b.c", "$[2].c.c"])"},
    }));
  }
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
