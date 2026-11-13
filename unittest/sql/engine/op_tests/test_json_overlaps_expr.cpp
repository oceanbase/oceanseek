/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Expression-level tests for JSON_OVERLAPS, ported from the mysqltest suite:
//   tools/deploy/mysql_test/test_suite/json/t/json_overlaps_mysql.test
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
// JSON_OVERLAPS — 1 if two JSON documents share any common value, 0 otherwise,
// NULL if either argument is NULL.
//   mysqltest: json_overlaps_mysql.test
// ============================================================================
class JsonOverlapsExprTest : public OpTestKit {};

TEST_F(JsonOverlapsExprTest, MysqlArrayOverlap)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_overlaps(doc, cand)")
      .with_data({
          {R"([1,2,3])", R"([3,4,5])"},     // -> 1
          {R"([1,2,3])", R"([4,4,5])"},     // -> 0
          {R"([1,2,3])", R"([4,5])"},       // -> 0
          {R"([1,2])", R"([3,4,5])"},       // -> 0
          {R"([1,2])", R"([2,4,5])"},       // -> 1
          {R"([1,3,5,7])", R"([2,5,7])"},   // -> 1
          {R"([1,3,5,7])", R"([2,6,8])"},   // -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}, {"0"}, {"0"}, {"1"}, {"1"}, {"0"}}));
}

// Scalar-vs-array / scalar-vs-scalar: auto-wrap applies; type must match.
TEST_F(JsonOverlapsExprTest, MysqlScalarAutoWrap)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_overlaps(doc, cand)")
      .with_data({
          {R"(1)", R"([3,4,5])"},          // -> 0
          {R"([3,4,5])", R"(1)"},           // -> 0
          {R"([4,5,6,7])", R"(6)"},         // -> 1
          {R"([4,5,"6",7])", R"(6)"},       // string "6" != number 6 -> 0
          {R"([4,5,6,7])", R"("6")"},       // -> 0
          {R"([4,5,"6",7])", R"("6")"},     // -> 1
          {R"(5)", R"(5)"},                 // -> 1
          {R"(5)", R"(6)"},                 // -> 0
          {R"("1")", R"("1")"},            // -> 1
          {R"(true)", R"(false)"},         // -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {"0"}, {"0"}, {"1"}, {"0"}, {"0"}, {"1"},
      {"1"}, {"0"}, {"1"}, {"0"},
  }));
}

// Object overlap: shared key with equal value.
TEST_F(JsonOverlapsExprTest, MysqlObjectOverlap)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_overlaps(doc, cand)")
      .with_data({
          {R"({"a":1, "b":2})", R"({"a":1,"c":3})"},                          // -> 1
          {R"({"a":1, "b":2})", R"({"a":2,"c":3})"},                          // -> 0
          {R"({"a":1,"b":10,"d":10})", R"({"c":1,"e":10,"f":1,"d":10})"},    // -> 1
          {R"({"a":1,"b":10,"d":10})", R"({"a":5,"e":10,"f":1,"d":20})"},    // -> 0
          {R"({"a":1,"b":10,"d":null})", R"({"c":1,"e":10,"f":1,"d":null})"},// -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}, {"1"}, {"0"}, {"1"}}));
}

// Special values: null, empty containers, object inside array.
TEST_F(JsonOverlapsExprTest, MysqlSpecialValues)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_overlaps(doc, cand)")
      .with_data({
          {R"(null)", R"([null])"},                 // -> 1
          {R"(null)", R"(null)"},                   // -> 1
          {R"([{}])", R"({})"},                    // -> 1
          {R"({})", R"([{}])"},                    // -> 1
          {R"([{}])", R"({"a":1, "b":2})"},        // -> 0
          {R"([{}])", R"(1)"},                     // -> 0
          {R"([{"a":1}])", R"({"a":1})"},          // -> 1
          {R"([1,null])", R"(null)"},              // -> 1
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"1"}, {"1"}, {"0"}, {"0"}, {"1"}, {"1"}}));
}

// NULL in either argument -> NULL.
TEST_F(JsonOverlapsExprTest, MysqlNullArguments)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512), cand varchar(512)")
      .with_expr("json_overlaps(doc, cand)")
      .with_data({
          {TestValue::null(), TestValue::null()},     // -> NULL
          {R"(1234)", TestValue::null()},             // -> NULL
          {R"([1,null])", TestValue::null()},         // -> NULL
          {TestValue::null(), R"(0)"},                // -> NULL
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {TestValue::null()},
      {TestValue::null()},
      {TestValue::null()},
      {TestValue::null()},
  }));
}

// Constant candidate: parsed once and cached across rows (arg1 is a string literal,
// is_static_const_expr() == true). Exercises get_json_candidate_wrapper's cache hit
// branch and verifies results stay correct when the same candidate is reused per row.
TEST_F(JsonOverlapsExprTest, ConstCandidateCache)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_overlaps(doc, '[3,4,5]')")
      .with_data({
          {R"([1,2,3])"},    // -> 1  (3 overlaps)
          {R"([4,5])"},      // -> 1  (4,5 overlap)
          {R"([6,7])"},      // -> 0
          {R"([3])"},        // -> 1
          {R"([])"},         // -> 0
          {R"(3)"},          // -> 1  (scalar auto-wrap)
          {R"(6)"},          // -> 0
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({{"1"}, {"1"}, {"0"}, {"1"}, {"0"}, {"1"}, {"0"}}));
}

// Constant NULL candidate: cached cand_is_null_ == true -> result NULL every row,
// even though doc is non-null. Verifies the cache's null-state is honored and that
// the OR-semantics in get_json_candidate_wrapper_with_cache does not reset is_null
// to false when the caller's is_null was already false but the candidate is null.
TEST_F(JsonOverlapsExprTest, ConstNullCandidateCache)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_overlaps(doc, null)")
      .with_data({
          {R"([1,2,3])"},
          {R"({"a":1})"},
          {R"(5)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
  EXPECT_TRUE(result.verify_ordered({
      {TestValue::null()},
      {TestValue::null()},
      {TestValue::null()},
  }));
}

// arg0 is NULL but arg1 is an invalid JSON constant: the error must still surface
// (no short-circuit on arg0 null). Verifies MySQL semantics that all args are parsed
// even when the result is doomed to NULL, and guards against future regressions that
// might short-circuit arg0-null. Relies on eval-stage parsing of the string literal
// (is_valid_for_json only checks type, not JSON content, so the malformed text is
// rejected by get_json_doc_wrapper / get_candidate_wrapper_by_semantics at eval time).
TEST_F(JsonOverlapsExprTest, NullDocWithInvalidConstCandidateStillErrors)
{
  auto result = expr_unit_test()
      .columns("doc varchar(512)")
      .with_expr("json_overlaps(doc, '[3,4,5')")
      .expect_error(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM)
      .with_data({
          {R"(null)"},
      })
      .run(engine_);
  EXPECT_EQ(common::OB_ERR_INVALID_JSON_TEXT_IN_PARAM, result.get_ret_code());
}

// ============================================================================
// _enable_json_bin_view switch comparison tests.
//
// JSON_OVERLAPS calls element_count()/element()/get_key()/lookup()/
// json_type() on both wrappers.  When _enable_json_bin_view is ON and the
// input is cast(... as json), the wrappers hold ObJsonBinView and all these
// operations run on the zero-copy bin path.  When OFF, they hold ObJsonBin
// DOM.
//
// Note: _enable_json_bin_view is an OB_TENANT_PARAMETER, not a session
// variable, so with_session_vars() cannot set it.  We use WITH_TENANT_CONFS
// (TestParameterGuard RAII) which pushes into TestDefaultParameterConf TLS,
// consumed by OpTestEngine::apply_tenant_config_overrides() during prepare().
// ============================================================================

// B1: switch ON — array overlap via cast(s as json).
TEST_F(JsonOverlapsExprTest, BinViewSwitchOnArrayOverlap)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(256)")
        .with_expr("json_overlaps(cast(s as json), cast(c as json))")
        .with_data({
            {R"([1, 2, 3])", R"([3, 4, 5])"},
            {R"([1, 2, 3])", R"([4, 5, 6])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

// B2: switch OFF — same input as B1, expect identical results.
TEST_F(JsonOverlapsExprTest, BinViewSwitchOffArrayOverlap)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(256)")
        .with_expr("json_overlaps(cast(s as json), cast(c as json))")
        .with_data({
            {R"([1, 2, 3])", R"([3, 4, 5])"},
            {R"([1, 2, 3])", R"([4, 5, 6])"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

// B3: switch ON — object overlap via cast(s as json).
TEST_F(JsonOverlapsExprTest, BinViewSwitchOnObjectOverlap)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "true"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(256)")
        .with_expr("json_overlaps(cast(s as json), cast(c as json))")
        .with_data({
            {R"({"a": 1, "b": 2})", R"({"b": 2, "c": 3})"},
            {R"({"a": 1, "b": 2})", R"({"c": 3, "d": 4})"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

// B4: switch OFF — same input as B3, expect identical results.
TEST_F(JsonOverlapsExprTest, BinViewSwitchOffObjectOverlap)
{
  WITH_TENANT_CONFS({"_enable_json_bin_view", "false"}) {
    auto result = expr_unit_test()
        .columns("s varchar(256), c varchar(256)")
        .with_expr("json_overlaps(cast(s as json), cast(c as json))")
        .with_data({
            {R"({"a": 1, "b": 2})", R"({"b": 2, "c": 3})"},
            {R"({"a": 1, "b": 2})", R"({"c": 3, "d": 4})"},
        })
        .run(engine_);
    EXPECT_EQ(common::OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{"1"}, {"0"}}));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
