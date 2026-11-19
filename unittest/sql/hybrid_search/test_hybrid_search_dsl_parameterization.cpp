/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include "lib/worker.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/config/ob_server_config.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_parameterizer.h"
#include "sql/hybrid_search/ob_expr_dsl_param_value.h"
#include "sql/hybrid_search/ob_hybrid_search_plan_cache.h"
#include "sql/parser/ob_fast_parser.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/session/ob_sql_session_info.h"

#include "sql/test_sql_utils.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#undef private
#undef protected
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/ob_sql_utils.h"
#include "sql/rewrite/ob_expr_range_converter.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "lib/ob_name_def.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestHybridDslParameterization : public ::testing::Test
{
public:
  TestHybridDslParameterization()
      : compat_guard_(lib::Worker::CompatMode::MYSQL),
        old_fast_parser_(GCONF._ob_enable_fast_parser) {}
  void SetUp() override
  {
    GCONF._ob_enable_fast_parser = true;
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, nullptr));
    ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, session_.update_sys_variable(
        share::SYS_VAR_COLLATION_CONNECTION, static_cast<int64_t>(CS_TYPE_UTF8MB4_BIN)));
  }
  void TearDown() override { GCONF._ob_enable_fast_parser = old_fast_parser_; }

  int extract(const char *text, ObHybridDslRequestInfo *&request)
  {
    int ret = OB_SUCCESS;
    extracted_.reset();
    ObHybridDslSource source;
    source.present_ = source.supported_ = true;
    source.kind_ = SOURCE_LITERAL;
    OZ(ObHybridSearchDslParameterizer::extract(allocator_, ObString::make_string(text),
        source, request, extracted_));
    return ret;
  }

  int parse(const char *text, ObFastParserResult &result, const bool is_batch = false,
      const bool is_enabled = true, const ObSQLMode mode = DEFAULT_MYSQL_MODE)
  {
    FPContext context;
    context.sql_mode_ = mode;
    context.enable_batched_multi_stmt_ = is_batch;
    const ObString sql = ObString::make_string(text);
    int ret = OB_SUCCESS;
    ObHybridSearchTokenHandler handler;
    OZ(ObHybridSearchPlanCache::init_fast_parser(handler, &session_, is_enabled, context));
    if (OB_SUCC(ret)) {
      ret = ObSqlParameterization::fast_parser(allocator_, context, sql, result);
    }
    return ret;
  }

  ObSEArray<ObPCParam *, 8> extracted_;
  ObArenaAllocator allocator_;
  lib::CompatModeGuard compat_guard_;
  bool old_fast_parser_;
  ObSQLSessionInfo session_;
};

TEST_F(TestHybridDslParameterization, token_gate_skips_ordinary_tokens)
{
  ObHybridSearchTokenHandler handler;
  const char *ordinary[] = {
    "SELECT", "hash_column", "hybrid_search_extra", "xhybrid_search",
    "`hybrid_search`", "\"hybrid_search\"", "'hybrid_search'",
    "/* hybrid_search */", "-- hybrid_search", "@hybrid_search", ""
  };
  for (const char *text : ordinary) {
    const ObString token = ObString::make_string(text);
    EXPECT_FALSE(handler.need_process_token(token.ptr(), token.length(), true)) << text;
  }
  const ObString name("HyBrId_SeArCh");
  EXPECT_TRUE(handler.need_process_token(name.ptr(), name.length(), true));
  EXPECT_FALSE(handler.need_process_token(name.ptr(), name.length(), false));
  const ObString comment("/*!50000 SQL_NO_CACHE */");
  EXPECT_TRUE(handler.need_process_token(comment.ptr(), comment.length(), true));

  ASSERT_EQ(OB_SUCCESS, handler.process_token(name, 0, 0, true, false, false, nullptr, 0));
  EXPECT_TRUE(handler.need_process_token("(", 1, true));
  ASSERT_EQ(OB_SUCCESS, handler.process_token(ObString("("), 0, 0, true, false, false, nullptr, 0));
  EXPECT_TRUE(handler.need_process_token("TABLE", 5, true));
  ASSERT_EQ(OB_SUCCESS, handler.process_token(
      ObString("ordinary_arg"), 0, 0, true, false, false, nullptr, 0));
  EXPECT_FALSE(handler.has_carrier());
  EXPECT_FALSE(handler.need_process_token("SELECT", 6, true));
}

TEST_F(TestHybridDslParameterization, ordinary_sql_token_hook_preserves_parameters)
{
  const char *inputs[] = {
    "SELECT 42",
    "SELECT h, hash_col FROM docs WHERE id IN (1, 2, 3)",
    "SELECT 'HYBRID_SEARCH(TABLE docs, @dsl)' FROM docs WHERE id = 42",
    "SELECT `hybrid_search`, hybrid_search_extra FROM docs WHERE id = 42",
    "SELECT /* hybrid_search */ id FROM docs WHERE id = 42",
    "SELECT /*+ NO_REWRITE */ id FROM docs WHERE id = 42",
    "SELECT id FROM docs WHERE note = '/*! hybrid_search */'",
    "INSERT INTO docs(id, note) VALUES (1, 'hybrid_search'), (2, 'ordinary')",
    "UPDATE docs SET note = 'hybrid_search' WHERE id = 42",
    "DELETE FROM docs WHERE id = 42"
  };
  for (const char *sql : inputs) {
    ObFastParserResult observed;
    ObFastParserResult ordinary;
    ASSERT_EQ(OB_SUCCESS, parse(sql, observed)) << sql;
    ASSERT_EQ(OB_SUCCESS, parse(sql, ordinary, false, false)) << sql;
    EXPECT_EQ(nullptr, observed.hybrid_dsl_request_) << sql;
    EXPECT_EQ(ordinary.pc_key_.name_, observed.pc_key_.name_) << sql;
    ASSERT_EQ(ordinary.raw_params_.count(), observed.raw_params_.count()) << sql;
    for (int64_t i = 0; i < observed.raw_params_.count(); ++i) {
      const ParseNode *expected = ordinary.raw_params_.at(i)->node_;
      const ParseNode *actual = observed.raw_params_.at(i)->node_;
      ASSERT_NE(nullptr, expected);
      ASSERT_NE(nullptr, actual);
      EXPECT_EQ(expected->type_, actual->type_);
      EXPECT_EQ(expected->pos_, actual->pos_);
      EXPECT_EQ(expected->raw_sql_offset_, actual->raw_sql_offset_);
      EXPECT_EQ(ObString(expected->str_len_, expected->str_value_),
                ObString(actual->str_len_, actual->str_value_));
    }
  }
}

TEST_F(TestHybridDslParameterization, mixed_case_carrier_keeps_parameter_offsets)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT 7 FROM HyBrId_SeArCh(TABLE `db`.`docs`, )SQL"
      R"SQL('{"query":{"term":{"category":"tech"}}}') WHERE id = 8)SQL", result));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
  ASSERT_NE(nullptr, request);
  EXPECT_EQ(3, request->get_sql_raw_param_count());
  EXPECT_EQ(1, request->get_dsl_param_count());
  EXPECT_EQ(1, request->get_source().raw_param_idx_);
  ASSERT_EQ(4, result.raw_params_.count());
  const ParseNode *value = result.raw_params_.at(3)->node_;
  EXPECT_EQ(T_VARCHAR, value->type_);
  EXPECT_EQ(ObString("tech"), ObString(value->str_len_, value->str_value_));
}

TEST_F(TestHybridDslParameterization, missing_session_falls_back_before_charset_conversion)
{
  const ObString sql(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":"tech"}}}'))SQL");
  ObHybridSearchTokenHandler handler;
  FPContext context;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, nullptr, true, context));
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(allocator_, context, sql, result));
  EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(result));
  ASSERT_EQ(1, result.raw_params_.count());
  const ParseNode *carrier = result.raw_params_.at(0)->node_;
  ASSERT_NE(nullptr, carrier);
  EXPECT_EQ(T_VARCHAR, carrier->type_);
  EXPECT_EQ(ObString(R"({"query":{"term":{"category":"tech"}}})"),
      ObString(carrier->str_len_, carrier->str_value_));
}

TEST_F(TestHybridDslParameterization, token_hook_respects_parser_modes)
{
  ObHybridSearchTokenHandler handler;
  FPContext context;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, true, context));
  EXPECT_EQ(&handler, context.hybrid_search_token_handler_);
  context.is_format_ = true;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, true, context));
  EXPECT_EQ(nullptr, context.hybrid_search_token_handler_);
  context.is_format_ = false;
  context.is_udr_mode_ = true;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, true, context));
  EXPECT_EQ(nullptr, context.hybrid_search_token_handler_);
  context.is_udr_mode_ = false;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, false, context));
  EXPECT_EQ(nullptr, context.hybrid_search_token_handler_);
  {
    lib::CompatModeGuard guard(lib::Worker::CompatMode::ORACLE);
    ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, true, context));
    EXPECT_EQ(nullptr, context.hybrid_search_token_handler_);
  }
  GCONF._ob_enable_fast_parser = false;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(handler, &session_, true, context));
  EXPECT_EQ(nullptr, context.hybrid_search_token_handler_);
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse("SELECT 'hybrid_search' WHERE id = 42", result));
  EXPECT_EQ(nullptr, result.hybrid_dsl_request_);
  EXPECT_EQ(2, result.raw_params_.count());
}

TEST_F(TestHybridDslParameterization, token_gate_keeps_executable_comment_fallback)
{
  const char *inputs[] = {
    R"SQL(SELECT /*!50000 SQL_NO_CACHE */ id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"tech"}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"tech"}}}') /*!50000 SQL_NO_CACHE */)SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH /*!50000 SQL_NO_CACHE */ (TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"tech"}}}'))SQL"
  };
  for (const char *sql : inputs) {
    ObFastParserResult result;
    ASSERT_EQ(OB_SUCCESS, parse(sql, result)) << sql;
    ASSERT_NE(nullptr, result.hybrid_dsl_request_) << sql;
    EXPECT_FALSE(result.hybrid_dsl_request_->can_cache()) << sql;
    EXPECT_EQ(DSL_FALLBACK_EXECUTABLE_COMMENT,
        result.hybrid_dsl_request_->get_source().fallback_reason_) << sql;
  }
}

TEST_F(TestHybridDslParameterization, token_handler_reset_between_requests)
{
  const char *inputs[] = {
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"tech"}}}'))SQL",
    "SELECT id FROM /*! HYBRID_SEARCH(TABLE docs, @dsl) */",
    "SELECT 'hybrid_search', 42",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"news"}}}'))SQL",
    "SELECT 7"
  };
  ObHybridSearchTokenHandler handler;
  FPContext context;
  ObFastParserResult results[5];
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session_, true, context));
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(
        allocator_, context, ObString::make_string(inputs[i]), results[i])) << inputs[i];
  }
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(results[0]));
  EXPECT_EQ(nullptr, results[1].hybrid_dsl_request_);
  EXPECT_EQ(nullptr, results[2].hybrid_dsl_request_);
  EXPECT_EQ(2, results[2].raw_params_.count());
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(results[3]));
  EXPECT_TRUE(results[0].pc_key_.is_equal(results[3].pc_key_));
  ASSERT_EQ(2, results[3].raw_params_.count());
  const ParseNode *value = results[3].raw_params_.at(1)->node_;
  EXPECT_EQ(ObString("news"), ObString(value->str_len_, value->str_value_));
  EXPECT_EQ(nullptr, results[4].hybrid_dsl_request_);
  EXPECT_FALSE(handler.has_carrier());
}

TEST_F(TestHybridDslParameterization, values_and_object_key_order)
{
  ObFastParserResult a;
  ObFastParserResult b;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":{"query":"cold","operator":"or"}}},"size":10}'))SQL", a));
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"size":10,)SQL"
      R"SQL("query":{"match":{"content":{"operator":"or","query":"hot"}}}}'))SQL", b));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(a));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(b));
  EXPECT_TRUE(a.pc_key_.is_equal(b.pc_key_));
  EXPECT_EQ(a.pc_key_.hash(), b.pc_key_.hash());
  ASSERT_EQ(1, ObHybridSearchPlanCache::get_request(b)->get_sql_raw_param_count());
  ASSERT_EQ(2, b.raw_params_.count());
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(b);
  ASSERT_EQ(1, request->get_dsl_param_count());
  int64_t raw_idx = -1;
  ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(0, b.raw_params_, raw_idx));
  ASSERT_EQ(1, raw_idx);
  const ParseNode *node = b.raw_params_.at(raw_idx)->node_;
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(T_VARCHAR, node->type_);
  EXPECT_EQ(ObString("hot"), ObString(node->str_len_, node->str_value_));
  EXPECT_EQ(0, ObHybridSearchPlanCache::get_request(b)->get_source().raw_param_idx_);
}

TEST_F(TestHybridDslParameterization, fixed_options_change_key)
{
  const char *inputs[] = {
    R"({"query":{"match":{"content":{"query":"x","operator":"or"}}},"size":10})",
    R"({"query":{"match":{"content":{"query":"x","operator":"and"}}},"size":10})",
    R"({"query":{"match":{"title":{"query":"x","operator":"or"}}},"size":10})",
    R"({"query":{"match":{"content":{"query":"x","operator":"or"}}},"size":20})"
  };
  ObHybridDslRequestInfo *first = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(inputs[0], first));
  for (int64_t i = 1; i < 4; ++i) {
    ObHybridDslRequestInfo *other = nullptr;
    ASSERT_EQ(OB_SUCCESS, extract(inputs[i], other));
    EXPECT_NE(first->get_dsl_template(), other->get_dsl_template());
  }
}

TEST_F(TestHybridDslParameterization, vector_is_one_slot_without_dimension)
{
  ObHybridDslRequestInfo *a = nullptr;
  ObHybridDslRequestInfo *b = nullptr;
  ObHybridDslRequestInfo *c = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(
      R"({"knn":{"field":"embedding","query_vector":[0.1,0.2,0.3,0.4],"k":100}})", a));
  ASSERT_EQ(OB_SUCCESS, extract(
      R"({"knn":{"k":100,"query_vector":[1,2,3],"field":"embedding"}})", b));
  ASSERT_EQ(OB_SUCCESS, extract(
      R"({"knn":{"field":"embedding","query_vector":[1,2,3],"k":10}})", c));
  EXPECT_EQ(a->get_dsl_template(), b->get_dsl_template());
  EXPECT_NE(a->get_dsl_template(), c->get_dsl_template());
  ASSERT_EQ(1, extracted_.count());
  EXPECT_EQ(T_VARCHAR, extracted_.at(0)->node_->type_);
  EXPECT_EQ(ObString("[1, 2, 3]"), ObString(extracted_.at(0)->node_->str_len_,
                                            extracted_.at(0)->node_->str_value_));
}

TEST_F(TestHybridDslParameterization, terms_length_and_input_types)
{
  ObHybridDslRequestInfo *a = nullptr;
  ObHybridDslRequestInfo *b = nullptr;
  ObHybridDslRequestInfo *c = nullptr;
  ObHybridDslRequestInfo *d = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"terms":{"tag":[1,2]}}})", a));
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"terms":{"tag":[3,4]}}})", b));
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"terms":{"tag":[1,2,3]}}})", c));
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"terms":{"tag":[1,"2"]}}})", d));
  EXPECT_EQ(a->get_dsl_template(), b->get_dsl_template());
  EXPECT_NE(a->get_dsl_template(), c->get_dsl_template());
  // PlanSet checks the restored SQL parameter types.
  EXPECT_EQ(a->get_dsl_template(), d->get_dsl_template());
  ASSERT_EQ(2, extracted_.count());
  EXPECT_EQ(T_INT, extracted_.at(0)->node_->type_);
  EXPECT_EQ(T_VARCHAR, extracted_.at(1)->node_->type_);
}

TEST_F(TestHybridDslParameterization, nested_bool_literal_types)
{
  ObHybridDslRequestInfo *request = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"bool":{"filter":[
    {"term":{"enabled":true}}, {"range":{"price":{"gte":10,"lt":20}}},
    {"match_phrase":{"content":{"query":"a b","slop":2}}},
    {"query_string":{"query":"c AND d","fields":["title"]}}
  ]}}})", request));
  ASSERT_EQ(5, extracted_.count());
  EXPECT_EQ(T_BOOL, extracted_.at(0)->node_->type_);
  EXPECT_EQ(T_INT, extracted_.at(1)->node_->type_);
  EXPECT_EQ(T_INT, extracted_.at(2)->node_->type_);
  EXPECT_EQ(T_VARCHAR, extracted_.at(3)->node_->type_);
  EXPECT_EQ(T_VARCHAR, extracted_.at(4)->node_->type_);
}

TEST_F(TestHybridDslParameterization, lexer_carrier_mapping_and_sql_escapes)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT 17 FROM HYBRID_SEARCH /* x */ (TABLE `db`.`docs`, )SQL"
      R"SQL('{"query":{"match":{"content":"a''b"}}}'))SQL", result));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(result));
  EXPECT_EQ(2, ObHybridSearchPlanCache::get_request(result)->get_sql_raw_param_count());
  ASSERT_EQ(3, result.raw_params_.count());
  EXPECT_EQ(1, ObHybridSearchPlanCache::get_request(result)->get_source().raw_param_idx_);
  const ParseNode *node = result.raw_params_.at(2)->node_;
  EXPECT_EQ(ObString("a'b"), ObString(node->str_len_, node->str_value_));
  EXPECT_GT(ObHybridSearchPlanCache::get_request(result)->get_source().sql_offset_, 0);
}

TEST_F(TestHybridDslParameterization, ordinary_sql_and_opt_out)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      "SELECT 'HYBRID_SEARCH(TABLE docs, x)' /* HYBRID_SEARCH */ FROM docs", result));
  EXPECT_EQ(nullptr, result.hybrid_dsl_request_);
  EXPECT_TRUE((result.hybrid_dsl_request_ == nullptr
        || result.hybrid_dsl_request_->can_cache()));
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":"x"}}}'))SQL", result, false, false));
  EXPECT_EQ(nullptr, result.hybrid_dsl_request_);
  EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(result));
}

TEST_F(TestHybridDslParameterization, unsupported_or_invalid_dsl_bypasses_cache)
{
  const char *inputs[] = {
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, '{broken')",

        R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"match":{"content":"a",)SQL"
        R"SQL("content":"b"}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"terms":{"tag":[{"a":1}]}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"term":{"tag":null}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"match_all":{}}}'))SQL",
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, concat('{}', '{}'))",
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, @dsl)", // Restoring a variable requires a session.
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, '{}'), HYBRID_SEARCH(TABLE docs, '{}')"
  };
  for (const char *text : inputs) {
    ObFastParserResult result;
    ASSERT_EQ(OB_SUCCESS, parse(text, result));
    ASSERT_NE(nullptr, result.hybrid_dsl_request_);
    EXPECT_TRUE(result.hybrid_dsl_request_->get_source().present_);
    EXPECT_FALSE((result.hybrid_dsl_request_ == nullptr
        || result.hybrid_dsl_request_->can_cache()));
    EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(result));
  }
  ObFastParserResult batch;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":"x"}}}'))SQL", batch, true));
  EXPECT_FALSE((batch.hybrid_dsl_request_ == nullptr || batch.hybrid_dsl_request_->can_cache()));
}

TEST_F(TestHybridDslParameterization, user_variable_locator)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      "SELECT id FROM HYBRID_SEARCH(TABLE docs, @dsl)", result));
  ASSERT_NE(nullptr, result.hybrid_dsl_request_);
  EXPECT_TRUE(result.hybrid_dsl_request_->get_source().supported_);
  EXPECT_EQ(SOURCE_USER_VARIABLE, result.hybrid_dsl_request_->get_source().kind_);
  EXPECT_EQ(ObString("dsl"), result.hybrid_dsl_request_->get_source().user_var_name_);
  // The lexer records the insertion position even when no session can supply the carrier.
  EXPECT_EQ(0, result.hybrid_dsl_request_->get_source().raw_param_idx_);
  EXPECT_EQ(0, result.raw_params_.count());
}

TEST_F(TestHybridDslParameterization, signature_ownership_and_key_lifecycle)
{
  ObArenaAllocator cache_allocator;
  ObPlanCacheKey cached;
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":"x"}}}'))SQL", result));
  ASSERT_EQ(OB_SUCCESS, cached.deep_copy(cache_allocator, result.pc_key_));
  EXPECT_TRUE(cached.is_equal(result.pc_key_));
  EXPECT_EQ(cached.hash(), result.pc_key_.hash());
  EXPECT_NE(cached.name_.ptr(), result.pc_key_.name_.ptr());
  ObFastParserResult copy;
  ASSERT_EQ(OB_SUCCESS, copy.assign(result));
  EXPECT_EQ(ObHybridSearchPlanCache::get_request(copy),
      ObHybridSearchPlanCache::get_request(result));
  EXPECT_TRUE(copy.pc_key_.is_equal(result.pc_key_));
  result.reset();
  EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(result));
  allocator_.reset();
  EXPECT_FALSE(cached.name_.empty());
  EXPECT_FALSE(cached.is_equal(result.pc_key_));
  cached.destory(cache_allocator);
  cached.reset();
  EXPECT_TRUE(cached.name_.empty());
}

TEST_F(TestHybridDslParameterization, user_variable_is_a_request_snapshot)
{
  ObSQLSessionInfo &session = session_;
  FPContext context;
  ObSessionVariable variable;
  variable.value_.set_varchar(R"({"query":{"match":{"content":"first"}}})");
  variable.value_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, session.replace_user_variable(ObString("dsl"), variable));
  const ObString sql("SELECT id FROM HYBRID_SEARCH(TABLE docs, @dsl)");
  ObHybridSearchTokenHandler handler;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session, true, context));
  ObFastParserResult first;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(allocator_, context, sql, first));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(first));
  variable.value_.set_varchar(R"({"query":{"match":{"content":"second"}}})");
  ASSERT_EQ(OB_SUCCESS, session.replace_user_variable(ObString("dsl"), variable));
  ObFastParserResult second;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(allocator_, context, sql, second));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(second));
  EXPECT_TRUE(first.pc_key_.is_equal(second.pc_key_));
  const ParseNode *old_value = first.raw_params_.at(1)->node_;
  const ParseNode *new_value = second.raw_params_.at(1)->node_;
  EXPECT_EQ(ObString("first"), ObString(old_value->str_len_, old_value->str_value_));
  EXPECT_EQ(ObString("second"), ObString(new_value->str_len_, new_value->str_value_));
  EXPECT_EQ(1, ObHybridSearchPlanCache::get_request(first)->get_sql_raw_param_count());
  EXPECT_EQ(2, first.raw_params_.count());
  EXPECT_EQ(0, ObHybridSearchPlanCache::get_request(first)->get_source().raw_param_idx_);
  ObFastParserResult literal;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":"literal"}}}'))SQL", literal));
  EXPECT_TRUE(first.pc_key_.is_equal(literal.pc_key_));
}

TEST_F(TestHybridDslParameterization, executable_comment_fallback)
{
  ObFastParserResult comment;
  const char *sql = "SELECT id FROM /*! HYBRID_SEARCH(TABLE docs, @dsl) */";
  ASSERT_EQ(OB_SUCCESS, parse(sql, comment));
  EXPECT_EQ(nullptr, comment.hybrid_dsl_request_);
  EXPECT_EQ(ObString::make_string(sql), comment.pc_key_.name_);
  EXPECT_FALSE(ObHybridSearchPlanCache::can_add_plan(comment));
  ObFastParserResult ordinary;
  ASSERT_EQ(OB_SUCCESS, parse(
      "SELECT /*! SQL_NO_CACHE */ 1", ordinary));
  EXPECT_TRUE((ordinary.hybrid_dsl_request_ == nullptr
      || ordinary.hybrid_dsl_request_->can_cache()));
}

TEST_F(TestHybridDslParameterization, legacy_parser_preserves_sql_parameters)
{
  GCONF._ob_enable_fast_parser = false;
  const char *inputs[] = {
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"match":{"content":"x"}}}') WHERE id = 42)SQL",
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, @dsl) WHERE id = 42",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":'))SQL",
    "SELECT 'hybrid_search' FROM docs WHERE id = 42"
  };
  for (const char *text : inputs) {
    const ObString sql = ObString::make_string(text);
    ObHybridSearchTokenHandler handler;
    FPContext context;
    ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
        handler, &session_, true, context));
    ASSERT_EQ(nullptr, context.hybrid_search_token_handler_);
    ObFastParserResult observed;
    ObFastParserResult ordinary;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(
        allocator_, context, sql, observed)) << text;
    ASSERT_EQ(OB_SUCCESS, parse(text, ordinary, false, false)) << text;
    EXPECT_EQ(nullptr, observed.hybrid_dsl_request_) << text;
    EXPECT_EQ(ordinary.pc_key_.name_, observed.pc_key_.name_) << text;
    ASSERT_EQ(ordinary.raw_params_.count(), observed.raw_params_.count()) << text;
    for (int64_t i = 0; i < observed.raw_params_.count(); ++i) {
      const ParseNode *expected = ordinary.raw_params_.at(i)->node_;
      const ParseNode *actual = observed.raw_params_.at(i)->node_;
      ASSERT_NE(nullptr, expected);
      ASSERT_NE(nullptr, actual);
      EXPECT_EQ(expected->type_, actual->type_);
      EXPECT_EQ(expected->pos_, actual->pos_);
      EXPECT_EQ(expected->raw_sql_offset_, actual->raw_sql_offset_);
      EXPECT_EQ(ObString(expected->str_len_, expected->str_value_),
                ObString(actual->str_len_, actual->str_value_));
    }
  }
}

TEST_F(TestHybridDslParameterization, fast_parser_toggle_resets_hybrid_hook)
{
  const ObString sql(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"match":{"content":"x"}}}'))SQL");
  ObHybridSearchTokenHandler handler;
  FPContext context;
  ObFastParserResult enabled;
  ObFastParserResult disabled;
  ObFastParserResult reenabled;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session_, true, context));
  ASSERT_EQ(&handler, context.hybrid_search_token_handler_);
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(
      allocator_, context, sql, enabled));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(enabled));
  ASSERT_EQ(2, enabled.raw_params_.count());

  GCONF._ob_enable_fast_parser = false;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session_, true, context));
  ASSERT_EQ(nullptr, context.hybrid_search_token_handler_);
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(
      allocator_, context, sql, disabled));
  EXPECT_EQ(nullptr, disabled.hybrid_dsl_request_);
  ASSERT_EQ(1, disabled.raw_params_.count());
  const ParseNode *carrier = disabled.raw_params_.at(0)->node_;
  ASSERT_NE(nullptr, carrier);
  EXPECT_EQ(ObString(R"({"query":{"match":{"content":"x"}}})"),
            ObString(carrier->str_len_, carrier->str_value_));
  EXPECT_FALSE(enabled.pc_key_.is_equal(disabled.pc_key_));

  GCONF._ob_enable_fast_parser = true;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session_, true, context));
  ASSERT_EQ(&handler, context.hybrid_search_token_handler_);
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(
      allocator_, context, sql, reenabled));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(reenabled));
  EXPECT_EQ(enabled.raw_params_.count(), reenabled.raw_params_.count());
  EXPECT_TRUE(enabled.pc_key_.is_equal(reenabled.pc_key_));
}

TEST_F(TestHybridDslParameterization, sql_key_embeds_dsl_template)
{
  ObFastParserResult a;
  ObFastParserResult b;
  ObFastParserResult c;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"terms":{"category":["tech","news"]}}}') WHERE id >= 100)SQL", a));
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"terms":{"category":["music","sport"]}}}') WHERE id >= 200)SQL", b));
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"terms":{"category":["tech","news","art"]}}}') WHERE id >= 100)SQL", c));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(a));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(b));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(c));
  EXPECT_TRUE(a.pc_key_.is_equal(b.pc_key_));
  EXPECT_FALSE(a.pc_key_.is_equal(c.pc_key_));
  ASSERT_EQ(2, ObHybridSearchPlanCache::get_request(a)->get_sql_raw_param_count());
  ASSERT_EQ(4, a.raw_params_.count());
  EXPECT_EQ(0, ObHybridSearchPlanCache::get_request(a)->get_source().raw_param_idx_);
  EXPECT_EQ(2, ObHybridSearchPlanCache::get_request(a)->get_dsl_param_count());
  EXPECT_EQ(ObString("{\"query\":{\"terms\":{\"category\":[?,?]}}}"),
      ObHybridSearchPlanCache::get_request(a)->get_dsl_template());
  const ObString key = a.pc_key_.name_;
  EXPECT_NE(std::string::npos, std::string(key.ptr(), key.length()).find("[?,?]"));
  EXPECT_EQ('?', key.ptr()[a.raw_params_.at(1)->node_->pos_]);
  a.raw_params_.at(0)->flag_ = NOT_PARAM;
  ObSEArray<ObPCParam *, 1> special;
  ASSERT_EQ(OB_SUCCESS, special.push_back(a.raw_params_.at(0)));
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchDslParameterizer::remove_carrier_param(
      *ObHybridSearchPlanCache::get_request(a), a.raw_params_, special));
  EXPECT_EQ(0, special.count());
  char buffer[1024];
  int32_t length = 0;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::construct_sql(
      key, special, buffer, sizeof(buffer), length));
  EXPECT_EQ(key, ObString(length, buffer));
}

TEST_F(TestHybridDslParameterization, duplicate_keyword_templates_cannot_reuse_valid_plans)
{
  const char *valid[] = {
    R"({"query":{"term":{"n":1}}})",
    R"({"query":{"term":{"n":{"value":1}}}})",
    R"({"query":{"range":{"n":{"gte":1}}}})",
    R"({"query":{"bool":{"filter":[{"term":{"n":1}}]}}})",
    R"({"query":{"match":{"body":{"query":"alpha"}}}})",
    R"({"query":{"term":{"n":1},"search_options":{"multi_value":true}}})"
  };
  const char *duplicate[] = {
    R"({"query":{"term":{"n":2}},"QUERY":{"term":{"n":3}}})",
    R"({"query":{"term":{"n":{"value":2,"VALUE":3}}}})",
    R"({"query":{"range":{"n":{"gte":2,"GTE":3}}}})",
    R"({"query":{"bool":{"filter":[{"term":{"n":2}}],"FILTER":[{"term":{"n":3}}]}}})",
    R"({"query":{"match":{"body":{"query":"beta","QUERY":"gamma"}}}})",
    R"({"query":{"term":{"n":2},"search_options":{"multi_value":true,"MULTI_VALUE":false}}})"
  };
  ASSERT_EQ(ARRAYSIZEOF(valid), ARRAYSIZEOF(duplicate));
  for (int64_t i = 0; i < ARRAYSIZEOF(valid); ++i) {
    SCOPED_TRACE(valid[i]);
    const std::string prefix = "SELECT id FROM HYBRID_SEARCH(TABLE docs, '";
    const std::string valid_sql = prefix + valid[i] + "')";
    const std::string duplicate_sql = prefix + duplicate[i] + "')";
    ObFastParserResult valid_result;
    ObFastParserResult duplicate_result;
    ASSERT_EQ(OB_SUCCESS, parse(valid_sql.c_str(), valid_result));
    ASSERT_EQ(OB_SUCCESS, parse(duplicate_sql.c_str(), duplicate_result));
    ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(valid_result));
    ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(duplicate_result));
    // A duplicate request must reach resolver validation instead of a valid cached plan.
    EXPECT_FALSE(valid_result.pc_key_.is_equal(duplicate_result.pc_key_));
  }
}

TEST_F(TestHybridDslParameterization, uppercase_keywords_preserve_parameterized_cache_keys)
{
  ObFastParserResult a;
  ObFastParserResult b;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"QuErY":{"TeRm":{"n":{"VaLuE":1}}},"SiZe":10}'))SQL", a));
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"QuErY":{"TeRm":{"n":{"VaLuE":2}}},"SiZe":10}'))SQL", b));
  const ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(a);
  ASSERT_NE(nullptr, request);
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(b));
  EXPECT_EQ(1, request->get_dsl_param_count());
  EXPECT_TRUE(a.pc_key_.is_equal(b.pc_key_));
  const ObString dsl_template = request->get_dsl_template();
  const std::string text(dsl_template.ptr(), dsl_template.length());
  EXPECT_NE(std::string::npos, text.find("\"VaLuE\":?"));
  EXPECT_NE(std::string::npos, text.find("\"QuErY\""));
}

TEST_F(TestHybridDslParameterization, carrier_filter_preserves_other_special_params)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT 7, id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":"tech"}}}') WHERE id = 9)SQL", result));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(result));
  ASSERT_EQ(3, ObHybridSearchPlanCache::get_request(result)->get_sql_raw_param_count());
  ASSERT_EQ(4, result.raw_params_.count());
  ASSERT_EQ(1, ObHybridSearchPlanCache::get_request(result)->get_source().raw_param_idx_);
  ObSEArray<ObPCParam *, 3> special;
  const int64_t raw_count = ObHybridSearchPlanCache::get_request(result)->get_sql_raw_param_count();
  for (int64_t i = 0; i < raw_count; ++i) {
    result.raw_params_.at(i)->flag_ = NOT_PARAM;
    ASSERT_EQ(OB_SUCCESS, special.push_back(result.raw_params_.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchDslParameterizer::remove_carrier_param(
      *ObHybridSearchPlanCache::get_request(result), result.raw_params_, special));
  ASSERT_EQ(2, special.count());
  EXPECT_EQ(result.raw_params_.at(0), special.at(0));
  EXPECT_EQ(result.raw_params_.at(2), special.at(1));
  EXPECT_EQ(NOT_PARAM, result.raw_params_.at(1)->flag_);
  char buffer[1024];
  int32_t length = 0;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::construct_sql(
      result.pc_key_.name_, special, buffer, sizeof(buffer), length));
  EXPECT_EQ(ObString(
      R"SQL(SELECT 7, id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":?}}}') WHERE id = 9)SQL"),
            ObString(length, buffer));
}

TEST_F(TestHybridDslParameterization, node_mapping_matches_raw_parameter_order)
{
  std::string text = R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"terms":{"category":[)SQL";
  const int64_t count = 128;
  for (int64_t i = 0; i < count; ++i) {
    if (i > 0) {
      text += ',';
    }
    text += std::to_string(i);
  }
  text += "]}}}')";
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(text.c_str(), result));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
  ASSERT_NE(nullptr, request);
  ASSERT_EQ(count, request->get_dsl_param_count());
  ASSERT_EQ(count + 1, result.raw_params_.count());
  ObIJsonBase *query = nullptr;
  ObIJsonBase *terms = nullptr;
  ObIJsonBase *values = nullptr;
  ASSERT_NE(nullptr, request->get_json_root());
  ASSERT_EQ(OB_SUCCESS, request->get_json_root()->get_object_value(ObString("query"), query));
  ASSERT_NE(nullptr, query);
  ASSERT_EQ(OB_SUCCESS, query->get_object_value(ObString("terms"), terms));
  ASSERT_NE(nullptr, terms);
  ASSERT_EQ(OB_SUCCESS, terms->get_object_value(ObString("category"), values));
  ASSERT_NE(nullptr, values);
  ASSERT_EQ(count, values->element_count());
  for (int64_t i = 0; i < count; ++i) {
    ObIJsonBase *node = nullptr;
    int64_t dsl_idx = -1;
    int64_t raw_idx = -1;
    ASSERT_EQ(OB_SUCCESS, values->get_array_element(i, node));
    ASSERT_NE(nullptr, node);
    ASSERT_EQ(ObJsonNodeType::J_INT, node->json_type());
    EXPECT_EQ(i, node->get_int());
    ASSERT_EQ(OB_SUCCESS, request->find_dsl_param_idx(*node, dsl_idx));
    EXPECT_EQ(i, dsl_idx);
    ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(dsl_idx, result.raw_params_, raw_idx));
    ASSERT_EQ(i + 1, raw_idx);
    ASSERT_NE(nullptr, result.raw_params_.at(raw_idx)->node_);
    EXPECT_EQ(T_INT, result.raw_params_.at(raw_idx)->node_->type_);
    EXPECT_EQ(node->get_int(), result.raw_params_.at(raw_idx)->node_->value_);
  }
}

TEST_F(TestHybridDslParameterization, json_strings_remain_sql_strings)
{
  ObHybridDslRequestInfo *request = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(R"({"query":{"terms":{"doc":["20",20,true,"a\"b"]}}})", request));
  ASSERT_EQ(4, extracted_.count());
  const ParseNode *str = extracted_.at(0)->node_;
  EXPECT_EQ(T_VARCHAR, str->type_);
  EXPECT_EQ(ObString("20"), ObString(str->str_len_, str->str_value_));
  EXPECT_EQ(T_INT, extracted_.at(1)->node_->type_);
  EXPECT_EQ(20, extracted_.at(1)->node_->value_);
  EXPECT_EQ(T_BOOL, extracted_.at(2)->node_->type_);
  const ParseNode *escaped = extracted_.at(3)->node_;
  EXPECT_EQ(ObString("a\"b"), ObString(escaped->str_len_, escaped->str_value_));
}

TEST_F(TestHybridDslParameterization, template_literal_round_trip)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"cat''egory":"tech"}}}'))SQL", result));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(result));
  ASSERT_EQ(1, ObHybridSearchPlanCache::get_request(result)->get_sql_raw_param_count());
  ASSERT_EQ(2, result.raw_params_.count());
  const ParseNode *carrier = result.raw_params_.at(0)->node_;
  EXPECT_EQ(ObHybridSearchPlanCache::get_request(result)->get_dsl_template(),
      ObString(carrier->str_len_, carrier->str_value_));
}


TEST_F(TestHybridDslParameterization, raw_input_preserves_scalar_types)
{
  ObHybridDslRequestInfo *request = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(
      R"({"query":{"terms":{"doc":["20",20,true,false,9007199254740993,)"
      R"(18446744073709551615]}}})", request));
  // Values within int64 range use signed SQL integer literals, regardless of the JSON tag.
  const ObItemType types[] = {T_VARCHAR, T_INT, T_BOOL, T_BOOL, T_INT, T_UINT64};
  const char *expected[] = {"20", "20", "true", "false",
                            "9007199254740993", "18446744073709551615"};
  ASSERT_EQ(6, extracted_.count());
  ASSERT_EQ(6, request->get_dsl_param_count());
  for (int64_t i = 0; i < extracted_.count(); ++i) {
    const ParseNode *node = extracted_.at(i)->node_;
    ASSERT_NE(nullptr, node);
    EXPECT_EQ(types[i], node->type_);
    // Extraction has not applied syntax-tree parameter properties.
    EXPECT_EQ(INVALID_PARAM, extracted_.at(i)->flag_);
    EXPECT_EQ(ObString::make_string(expected[i]), ObString(node->str_len_, node->str_value_));
  }
  EXPECT_EQ(20, extracted_.at(1)->node_->value_);
  EXPECT_EQ(1, extracted_.at(2)->node_->value_);
  EXPECT_EQ(0, extracted_.at(3)->node_->value_);
  EXPECT_EQ(INT64_C(9007199254740993), extracted_.at(4)->node_->value_);
  EXPECT_EQ(UINT64_MAX, static_cast<uint64_t>(extracted_.at(5)->node_->value_));
}

TEST_F(TestHybridDslParameterization, raw_strings_preserve_decoded_bytes)
{
  ObHybridDslRequestInfo *request = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract(
      R"({"query":{"terms":{"doc":["a\"b","a\\b","a\nb","[20]","true","a\u0000b"]}}})", request));
  const char *expected[] = {"a\"b", "a\\b", "a\nb", "[20]", "true", "a\0b"};
  const int32_t lengths[] = {3, 3, 3, 4, 4, 3};
  ASSERT_EQ(6, extracted_.count());
  for (int64_t i = 0; i < extracted_.count(); ++i) {
    const ParseNode *raw = extracted_.at(i)->node_;
    ASSERT_NE(nullptr, raw);
    EXPECT_EQ(T_VARCHAR, raw->type_);
    EXPECT_EQ(lengths[i], raw->str_len_);
    EXPECT_EQ(ObString(lengths[i], expected[i]), ObString(raw->str_len_, raw->str_value_));
  }
}

TEST_F(TestHybridDslParameterization, hybrid_compile_reset_preserves_request)
{
  const ObString sql(
      "SELECT 42 FROM HYBRID_SEARCH(TABLE docs, "
                     "'{\"query\":{\"term\":{\"doc\":\"20\"}}}')");
  ObSqlCtx sql_ctx;
  ObArenaAllocator exec_allocator;
  ObExecContext exec_ctx(exec_allocator);
  ObPlanCacheCtx ctx(sql, PC_TEXT_MODE, allocator_, sql_ctx, exec_ctx, OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, parse(sql.ptr(), ctx.fp_result_));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(ctx.fp_result_);
  ASSERT_NE(nullptr, request);
  ParseNode carrier = {};
  ParseNode view_carrier = {};
  carrier.type_ = T_DSL_QUERY;
  view_carrier.type_ = T_DSL_QUERY;
  EXPECT_FALSE(request->is_carrier_node(carrier));
  request->set_carrier_node(&carrier);
  EXPECT_TRUE(request->is_carrier_node(carrier));
  EXPECT_FALSE(request->is_carrier_node(view_carrier));
  ParamStore params((ObWrapperAllocator(&allocator_)));
  ObObjParam value;
  value.set_int(42);
  ASSERT_EQ(OB_SUCCESS, params.push_back(value));
  value.set_varchar(ObString("20"));
  ASSERT_EQ(OB_SUCCESS, params.push_back(value));
  request->set_compile_params(ctx.fp_result_.raw_params_, params, 1);
  EXPECT_EQ(1, request->get_sql_param_count());
  EXPECT_TRUE(request->is_params_ready());
  EXPECT_EQ(&ctx.fp_result_.raw_params_, request->get_raw_params());
  EXPECT_EQ(&params, request->get_param_store());
  request->set_params_bound(true);
  ASSERT_EQ(OB_SUCCESS, request->mark_param_resolved(0));
  ObIJsonBase *json_root = request->get_json_root();
  ObFastParserResult copy;
  ASSERT_EQ(OB_SUCCESS, copy.assign(ctx.fp_result_));
  ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(copy));
  EXPECT_TRUE(ObHybridSearchPlanCache::get_request(copy)->is_carrier_node(carrier));
  request->reset_compile();
  EXPECT_FALSE(request->is_carrier_node(carrier));
  EXPECT_FALSE(ObHybridSearchPlanCache::get_request(copy)->is_carrier_node(carrier));
  EXPECT_EQ(2, request->get_sql_raw_param_count());
  EXPECT_EQ(3, ctx.fp_result_.raw_params_.count());
  EXPECT_EQ(1, request->get_dsl_param_count());
  EXPECT_EQ(0, request->get_sql_param_count());
  EXPECT_FALSE(request->is_params_ready());
  EXPECT_FALSE(request->params_bound());
  EXPECT_EQ(nullptr, request->get_raw_params());
  EXPECT_EQ(nullptr, request->get_param_store());
  EXPECT_TRUE(request->get_resolved_param_idxs().is_empty());
  EXPECT_EQ(json_root, request->get_json_root());
  EXPECT_EQ(2, params.count());
  EXPECT_EQ(request, ObHybridSearchPlanCache::get_request(copy));
  ctx.fp_result_.reset();
  EXPECT_EQ(request, ObHybridSearchPlanCache::get_request(copy));
  EXPECT_FALSE(request->get_dsl_snapshot().empty());
}

TEST_F(TestHybridDslParameterization, large_dsl_layout_and_binding_progress)
{
  const int64_t last_dsl_idx = 4096;
  std::string text("SELECT id FROM HYBRID_SEARCH(TABLE docs, "
                   "'{\"query\":{\"terms\":{\"doc\":[");
  for (int64_t i = 0; i <= last_dsl_idx; ++i) {
    if (i > 0) {
      text += ",";
    }
    text += "20";
  }
  text += "]}}}')";
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(text.c_str(), result));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
  ASSERT_NE(nullptr, request);
  ASSERT_EQ(last_dsl_idx + 1, request->get_dsl_param_count());
  ASSERT_EQ(1, request->get_sql_raw_param_count());
  ASSERT_EQ(last_dsl_idx + 2, result.raw_params_.count());
  EXPECT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::check_raw_param_count(result, 1));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      ObHybridSearchPlanCache::check_raw_param_count(result, result.raw_params_.count()));
  int64_t raw_idx = -1;
  ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(last_dsl_idx, result.raw_params_, raw_idx));
  EXPECT_EQ(last_dsl_idx + 1, raw_idx);
  ASSERT_EQ(OB_SUCCESS, request->mark_param_resolved(0));
  ASSERT_EQ(OB_SUCCESS, request->mark_param_resolved(last_dsl_idx));
  EXPECT_TRUE(request->get_resolved_param_idxs().has_member(0));
  EXPECT_TRUE(request->get_resolved_param_idxs().has_member(last_dsl_idx));
  request->reset_compile();
  EXPECT_TRUE(request->get_resolved_param_idxs().is_empty());
  EXPECT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::check_raw_param_count(result, 1));
  result.raw_params_.pop_back();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObHybridSearchPlanCache::check_raw_param_count(result, 1));
}

TEST_F(TestHybridDslParameterization, rewritten_sql_disables_dsl_mapping)
{
  const ObString sql("SELECT id FROM HYBRID_SEARCH(TABLE docs, "
                     "'{\"query\":{\"term\":{\"doc\":\"20\"}}}')");
  ObSqlCtx sql_ctx;
  ObArenaAllocator exec_allocator;
  ObExecContext exec_ctx(exec_allocator);
  ObPlanCacheCtx ctx(sql, PC_TEXT_MODE, allocator_, sql_ctx, exec_ctx, OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, parse(sql.ptr(), ctx.fp_result_));
  bool add_plan = true;
  bool transform_tree = true;
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(ctx.fp_result_);
  ASSERT_NE(nullptr, request);
  ObHybridSearchPlanCache::prepare_parse(ctx, sql, add_plan, transform_tree);
  EXPECT_TRUE(add_plan);
  EXPECT_TRUE(transform_tree);
  const ObString rewritten("SELECT id FROM docs");
  ObHybridSearchPlanCache::prepare_parse(ctx, rewritten, add_plan, transform_tree);
  EXPECT_FALSE(add_plan);
  EXPECT_FALSE(transform_tree);
  EXPECT_FALSE(ctx.should_add_plan_);
  EXPECT_FALSE((ctx.fp_result_.hybrid_dsl_request_ == nullptr
      || ctx.fp_result_.hybrid_dsl_request_->can_cache()));
  EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(ctx.fp_result_));
  EXPECT_EQ(DSL_FALLBACK_SQL_REWRITE, request->get_source().fallback_reason_);
  EXPECT_EQ(sql, ctx.raw_sql_);
}

TEST_F(TestHybridDslParameterization, ordinary_sql_does_not_create_hybrid_context)
{
  const ObString sql("SELECT 42 WHERE 'abc' = 'abc'");
  FPContext context;
  ObHybridSearchTokenHandler handler;
  ASSERT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::init_fast_parser(
      handler, &session_, true, context));
  EXPECT_EQ(&handler, context.hybrid_search_token_handler_);
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::fast_parser(allocator_, context, sql, result));
  EXPECT_EQ(nullptr, result.hybrid_dsl_request_);
  ASSERT_EQ(3, result.raw_params_.count());
  EXPECT_EQ(ObString("SELECT ? WHERE ? = ?"), result.pc_key_.name_);
}

TEST_F(TestHybridDslParameterization, hybrid_search_identifier_keeps_ordinary_sql_path)
{
  const char *inputs[] = {
    "SELECT 42 AS hybrid_search",
    "SELECT hybrid_search FROM docs WHERE id = 42",
    "SELECT id FROM hybrid_search WHERE id = 42",
    "SELECT id FROM docs hybrid_search WHERE id = 42"
  };
  for (const char *sql : inputs) {
    ObFastParserResult observed;
    ObFastParserResult ordinary;
    ASSERT_EQ(OB_SUCCESS, parse(sql, observed));
    ASSERT_EQ(OB_SUCCESS, parse(sql, ordinary, false, false));
    EXPECT_EQ(nullptr, observed.hybrid_dsl_request_);
    EXPECT_EQ(ordinary.pc_key_.name_, observed.pc_key_.name_);
    EXPECT_EQ(ordinary.raw_params_.count(), observed.raw_params_.count());
  }
}

TEST_F(TestHybridDslParameterization, identifiers_do_not_hide_or_duplicate_a_carrier)
{
  const char *inputs[] = {

        R"SQL(SELECT id AS hybrid_search FROM (SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
        R"SQL('{"query":{"term":{"category":"tech"}}}')) hybrid_search)SQL",

        R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE hybrid_search, )SQL"
        R"SQL('{"query":{"term":{"category":"tech"}}}'))SQL"
  };
  for (const char *sql : inputs) {
    ObFastParserResult result;
    ASSERT_EQ(OB_SUCCESS, parse(sql, result));
    ASSERT_NE(nullptr, ObHybridSearchPlanCache::get_request(result));
    EXPECT_EQ(1, ObHybridSearchPlanCache::get_request(result)->get_dsl_param_count());
    EXPECT_EQ(2, result.raw_params_.count());
  }
  ObFastParserResult multiple;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT a.id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":"tech"}}}') a, HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":"news"}}}') b)SQL", multiple));
  ASSERT_NE(nullptr, multiple.hybrid_dsl_request_);
  EXPECT_FALSE(multiple.hybrid_dsl_request_->can_cache());
}

TEST_F(TestHybridDslParameterization, outer_sql_input_preserves_all_raw_parameters)
{
  const char *sql =
      R"SQL(SELECT 42 FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"terms":{"category":["tech","news"]}}}') WHERE id > 100)SQL";
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(sql, result));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
  ASSERT_NE(nullptr, request);
  ASSERT_EQ(3, request->get_sql_raw_param_count());
  ASSERT_EQ(2, request->get_dsl_param_count());
  ASSERT_EQ(5, result.raw_params_.count());
  EXPECT_EQ(OB_SUCCESS, ObHybridSearchPlanCache::check_raw_param_count(result, 3));
  EXPECT_EQ(1, request->get_source().raw_param_idx_);
  EXPECT_EQ(T_INT, result.raw_params_.at(0)->node_->type_);
  EXPECT_EQ(42, result.raw_params_.at(0)->node_->value_);
  EXPECT_EQ(T_INT, result.raw_params_.at(2)->node_->type_);
  EXPECT_EQ(100, result.raw_params_.at(2)->node_->value_);
  int64_t raw_idx = -1;
  ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(0, result.raw_params_, raw_idx));
  ASSERT_EQ(3, raw_idx);
  const ParseNode *node = result.raw_params_.at(raw_idx)->node_;
  EXPECT_EQ(ObString("tech"), ObString(node->str_len_, node->str_value_));
}

TEST_F(TestHybridDslParameterization, dsl_indices_with_and_without_outer_parameters)
{
  const char *inputs[] = {
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"terms":{"category":["tech","news"]}}}'))SQL",
    R"SQL(SELECT 42, id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"terms":{"category":["tech","news"]}}}') WHERE id >= 1)SQL"
  };
  const char *values[] = {"tech", "news"};
  for (int64_t n = 0; n < 2; ++n) {
    ObFastParserResult result;
    ASSERT_EQ(OB_SUCCESS, parse(inputs[n], result));
    ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
    ASSERT_NE(nullptr, request);
    const int64_t sql_param_count = 2 * n;
    ASSERT_EQ(2, request->get_dsl_param_count());
    ASSERT_EQ(sql_param_count + 1, request->get_sql_raw_param_count());
    ObIJsonBase *query = nullptr;
    ObIJsonBase *terms = nullptr;
    ObIJsonBase *array = nullptr;
    ASSERT_NE(nullptr, request->get_json_root());
    ASSERT_EQ(OB_SUCCESS, request->get_json_root()->get_object_value(ObString("query"), query));
    ASSERT_NE(nullptr, query);
    ASSERT_EQ(OB_SUCCESS, query->get_object_value(ObString("terms"), terms));
    ASSERT_NE(nullptr, terms);
    ASSERT_EQ(OB_SUCCESS, terms->get_object_value(ObString("category"), array));
    ASSERT_NE(nullptr, array);
    for (int64_t i = 0; i < 2; ++i) {
      int64_t raw_idx = -1;
      int64_t param_idx = -1;
      int64_t found_idx = -1;
      ObIJsonBase *node = nullptr;
      ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(i, result.raw_params_, raw_idx));
      ASSERT_EQ(OB_SUCCESS, request->get_dsl_exec_param_idx(i, sql_param_count, param_idx));
      EXPECT_EQ(sql_param_count + 1 + i, raw_idx);
      EXPECT_EQ(sql_param_count + i, param_idx);
      const ParseNode *raw_node = result.raw_params_.at(raw_idx)->node_;
      EXPECT_EQ(ObString::make_string(values[i]),
          ObString(raw_node->str_len_, raw_node->str_value_));
      ASSERT_EQ(OB_SUCCESS, array->get_array_element(i, node));
      ASSERT_NE(nullptr, node);
      ASSERT_EQ(OB_SUCCESS, request->find_dsl_param_idx(*node, found_idx));
      EXPECT_EQ(i, found_idx);
      ASSERT_EQ(OB_SUCCESS, request->mark_param_resolved(i));
      EXPECT_TRUE(request->get_resolved_param_idxs().has_member(i));
    }
    int64_t fixed_idx = 0;
    ASSERT_EQ(OB_SUCCESS, request->find_dsl_param_idx(*request->get_json_root(), fixed_idx));
    EXPECT_EQ(-1, fixed_idx);
  }
}

TEST_F(TestHybridDslParameterization, invalid_dsl_indices_do_not_mutate_layout)
{
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, parse(
      R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"terms":{"category":["tech","news"]}}}'))SQL", result));
  ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
  ASSERT_NE(nullptr, request);
  for (const int64_t bad_idx : {-1, 2}) {
    int64_t idx = 100;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
        request->get_dsl_raw_param_idx(bad_idx, result.raw_params_, idx));
    EXPECT_EQ(-1, idx);
    EXPECT_EQ(OB_INVALID_ARGUMENT, request->get_dsl_exec_param_idx(bad_idx, 0, idx));
    EXPECT_EQ(-1, idx);
    EXPECT_EQ(OB_INVALID_ARGUMENT, request->mark_param_resolved(bad_idx));
  }
  EXPECT_TRUE(request->get_resolved_param_idxs().is_empty());
  int64_t idx = 100;
  EXPECT_EQ(OB_INVALID_ARGUMENT, request->get_dsl_exec_param_idx(1, INT64_MAX, idx));
  EXPECT_EQ(-1, idx);
  EXPECT_EQ(OB_INVALID_ARGUMENT, request->get_dsl_exec_param_idx(0, -1, idx));
  EXPECT_EQ(OB_ERR_UNEXPECTED, request->append_param_node(*request->get_json_root()));
  EXPECT_EQ(2, request->get_dsl_param_count());
  result.raw_params_.pop_back();
  EXPECT_EQ(OB_ERR_UNEXPECTED, request->check_param_layout(result.raw_params_));
  EXPECT_EQ(OB_ERR_UNEXPECTED, request->get_dsl_raw_param_idx(0, result.raw_params_, idx));
  EXPECT_EQ(-1, idx);
}

TEST_F(TestHybridDslParameterization, fallback_reason_identifies_expected_bypass)
{
  const char *inputs[] = {
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"wildcard":{"category":"te*"}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, concat('a','b')))SQL",
    R"SQL(SELECT a.id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"a"}}}') a, HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"b"}}}') b)SQL",
    R"SQL(SELECT /*!50000 SQL_NO_CACHE */ id FROM HYBRID_SEARCH(TABLE docs, )SQL"
    R"SQL('{"query":{"term":{"category":"a"}}}'))SQL",
    "SELECT id FROM HYBRID_SEARCH(TABLE docs, @dsl)",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":'))SQL"
  };
  const ObHybridDslFallbackReason reasons[] = {
    DSL_FALLBACK_DSL_EXTRACTION, DSL_FALLBACK_CARRIER_SYNTAX,
    DSL_FALLBACK_MULTIPLE_CARRIERS, DSL_FALLBACK_EXECUTABLE_COMMENT,
    DSL_FALLBACK_USER_VARIABLE, DSL_FALLBACK_DSL_EXTRACTION
  };
  for (int64_t i = 0; i < 6; ++i) {
    ObFastParserResult result;
    ASSERT_EQ(OB_SUCCESS, parse(inputs[i], result));
    ASSERT_NE(nullptr, result.hybrid_dsl_request_);
    EXPECT_EQ(nullptr, ObHybridSearchPlanCache::get_request(result));
    EXPECT_EQ(reasons[i], result.hybrid_dsl_request_->get_source().fallback_reason_);
    EXPECT_STRNE("unknown", ObHybridSearchPlanCache::fallback_reason_name(reasons[i]));
  }
  const char *supported = R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, )SQL"
      R"SQL('{"query":{"term":{"category":"a"}}}'))SQL";
  ObFastParserResult batch;
  ASSERT_EQ(OB_SUCCESS, parse(supported, batch, true));
  ASSERT_NE(nullptr, batch.hybrid_dsl_request_);
  EXPECT_EQ(DSL_FALLBACK_BATCHED_MULTI_STMT,
      batch.hybrid_dsl_request_->get_source().fallback_reason_);
}

TEST_F(TestHybridDslParameterization, adjacent_literals_are_merged_before_parameterization)
{
  const char *inputs[] = {
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '{"query":{"term":{"id":' '1}}}'))SQL",
    R"SQL(SELECT id FROM HYBRID_SEARCH(TABLE docs, '' '{"query":' '{"term":{"id":' '2}}}' ''))SQL"
  };
  const char *snapshots[] = {
    R"({"query":{"term":{"id":1}}})",
    R"({"query":{"term":{"id":2}}})"
  };
  ObFastParserResult results[2];
  for (int64_t i = 0; i < 2; ++i) {
    SCOPED_TRACE(inputs[i]);
    ObFastParserResult &result = results[i];
    ASSERT_EQ(OB_SUCCESS, parse(inputs[i], result));
    const ObHybridDslRequestInfo *request = ObHybridSearchPlanCache::get_request(result);
    ASSERT_NE(nullptr, request);
    EXPECT_EQ(DSL_FALLBACK_NONE, request->get_source().fallback_reason_);
    EXPECT_EQ(ObString::make_string(snapshots[i]), request->get_dsl_snapshot());
    EXPECT_EQ(ObString(R"({"query":{"term":{"id":?}}})"), request->get_dsl_template());
    ASSERT_EQ(1, request->get_dsl_param_count());
    int64_t raw_idx = -1;
    ASSERT_EQ(OB_SUCCESS, request->get_dsl_raw_param_idx(0, result.raw_params_, raw_idx));
    const ParseNode *value = result.raw_params_.at(raw_idx)->node_;
    ASSERT_NE(nullptr, value);
    EXPECT_EQ(T_INT, value->type_);
    EXPECT_EQ(i + 1, value->value_);
  }
  EXPECT_TRUE(results[0].pc_key_.is_equal(results[1].pc_key_));
}

TEST_F(TestHybridDslParameterization, ordinary_parser_entry_remains_available)
{
  const ObString sql(
      "SELECT 42 FROM HYBRID_SEARCH(TABLE docs, '{\"query\":{\"term\":{\"doc\":20}}}')");
  FPContext context;
  char *text = nullptr;
  int64_t length = 0;
  int64_t count = 0;
  int64_t values_pos = 0;
  ParamList *params = nullptr;
  ObFastParserResult result;
  ASSERT_EQ(OB_SUCCESS, ObFastParser::parse(sql, context, allocator_, text, length,
                                          params, count, result, values_pos));
  EXPECT_EQ(2, count);
  EXPECT_EQ(nullptr, result.hybrid_dsl_request_);
  EXPECT_EQ(ObString("SELECT ? FROM HYBRID_SEARCH(TABLE docs, ?)"), ObString(length, text));
}


TEST_F(TestHybridDslParameterization, integer_terms_native_integer_boundaries)
{
  ObObj value;
  ObJsonInt signed_min(INT64_MIN);
  ObJsonInt signed_max(INT64_MAX);
  ObJsonUint unsigned_max(UINT64_MAX);

  ASSERT_EQ(OB_SUCCESS, ObExprDslParamValue::convert_integer_terms(
      allocator_, signed_min, ObIntType, value));
  EXPECT_EQ(ObIntType, value.get_type());
  EXPECT_EQ(INT64_MIN, value.get_int());
  ASSERT_EQ(OB_SUCCESS, ObExprDslParamValue::convert_integer_terms(
      allocator_, signed_max, ObIntType, value));
  EXPECT_EQ(INT64_MAX, value.get_int());
  ASSERT_EQ(OB_SUCCESS, ObExprDslParamValue::convert_integer_terms(
      allocator_, signed_max, ObUInt64Type, value));
  EXPECT_EQ(ObUInt64Type, value.get_type());
  EXPECT_EQ(static_cast<uint64_t>(INT64_MAX), value.get_uint64());
  ASSERT_EQ(OB_SUCCESS, ObExprDslParamValue::convert_integer_terms(
      allocator_, unsigned_max, ObUInt64Type, value));
  EXPECT_EQ(ObUInt64Type, value.get_type());
  EXPECT_EQ(UINT64_MAX, value.get_uint64());
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprDslParamValue::convert_integer_terms(
      allocator_, signed_min, ObUInt64Type, value));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprDslParamValue::convert_integer_terms(
      allocator_, unsigned_max, ObIntType, value));
}

TEST_F(TestHybridDslParameterization, integer_terms_numeric_string_semantics)
{
  struct Case
  {
    const char *text_;
    ObObjType target_type_;
    int error_;
    bool discarded_;
    int64_t int_value_;
    uint64_t uint_value_;
  };
  const Case cases[] = {
    {"9007199254740993", ObIntType, OB_SUCCESS, false, INT64_C(9007199254740993), 0},
    {"9223372036854775807", ObIntType, OB_SUCCESS, false, INT64_MAX, 0},
    {"-9223372036854775808", ObIntType, OB_SUCCESS, false, INT64_MIN, 0},
    {"18446744073709551615", ObUInt64Type, OB_SUCCESS, false, 0, UINT64_MAX},
    {"9223372036854775808", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"-9223372036854775809", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"18446744073709551616", ObUInt64Type, OB_INVALID_ARGUMENT, false, 0, 0},
    {"1e3", ObIntType, OB_SUCCESS, false, 1000, 0},
    {"2.0", ObIntType, OB_SUCCESS, false, 2, 0},
    {"1.9", ObIntType, OB_SUCCESS, true, 0, 0},
    {"9007199254740993.5", ObIntType, OB_SUCCESS, true, 0, 0},
    {"1e-400", ObIntType, OB_SUCCESS, true, 0, 0},
    {"-1e-400", ObUInt64Type, OB_SUCCESS, true, 0, 0},
    {"0e-400", ObIntType, OB_SUCCESS, false, 0, 0},
    {"-0.5", ObUInt64Type, OB_SUCCESS, true, 0, 0},
    {"-1", ObUInt64Type, OB_INVALID_ARGUMENT, false, 0, 0},
    {"127", ObTinyIntType, OB_SUCCESS, false, 127, 0},
    {"-128", ObTinyIntType, OB_SUCCESS, false, -128, 0},
    {"128", ObTinyIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"-129", ObTinyIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"255", ObUTinyIntType, OB_SUCCESS, false, 0, 255},
    {"256", ObUTinyIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"32767", ObSmallIntType, OB_SUCCESS, false, 32767, 0},
    {"32768", ObSmallIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"2147483647", ObInt32Type, OB_SUCCESS, false, INT64_C(2147483647), 0},
    {"2147483648", ObInt32Type, OB_INVALID_ARGUMENT, false, 0, 0},
    {"", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"+", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"-", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"1x", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"true", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0},
    {"1e400", ObIntType, OB_INVALID_ARGUMENT, false, 0, 0}
  };
  for (const Case &test : cases) {
    SCOPED_TRACE(test.text_);
    ObJsonString node(ObString::make_string(test.text_));
    ObObj value;
    value.set_int(37);
    ASSERT_EQ(test.error_, ObExprDslParamValue::convert_integer_terms(
        allocator_, node, test.target_type_, value));
    if (test.error_ == OB_SUCCESS) {
      ASSERT_EQ(test.discarded_, value.is_null());
      if (!test.discarded_) {
        ASSERT_EQ(test.target_type_, value.get_type());
        if (ob_is_uint_tc(test.target_type_)) {
          EXPECT_EQ(test.uint_value_, value.get_uint64());
        } else {
          EXPECT_EQ(test.int_value_, value.get_int());
        }
      }
    }
  }
}

TEST_F(TestHybridDslParameterization, integer_terms_double_fraction_and_bounds)
{
  struct Case
  {
    double input_;
    ObObjType target_type_;
    int error_;
    bool discarded_;
    int64_t expected_;
  };
  const Case cases[] = {
    {1.9, ObIntType, OB_SUCCESS, true, 0},
    {-1.9, ObIntType, OB_SUCCESS, true, 0},
    {-0.5, ObUInt64Type, OB_SUCCESS, true, 0},
    {-1.0, ObUInt64Type, OB_INVALID_ARGUMENT, false, 0},
    {2.0, ObIntType, OB_SUCCESS, false, 2},
    {-0.0, ObUInt64Type, OB_SUCCESS, false, 0},
    {-9223372036854775808.0, ObIntType, OB_SUCCESS, false, INT64_MIN},
    {9223372036854775808.0, ObIntType, OB_INVALID_ARGUMENT, false, 0},
    {18446744073709551616.0, ObUInt64Type, OB_INVALID_ARGUMENT, false, 0},
    {127.0, ObTinyIntType, OB_SUCCESS, false, 127},
    {128.0, ObTinyIntType, OB_INVALID_ARGUMENT, false, 0},
    {128.5, ObTinyIntType, OB_SUCCESS, true, 0}
  };
  for (const Case &test : cases) {
    SCOPED_TRACE(test.input_);
    ObJsonDouble node(test.input_);
    ObObj value;
    value.set_int(37);
    ASSERT_EQ(test.error_, ObExprDslParamValue::convert_integer_terms(
        allocator_, node, test.target_type_, value));
    if (test.error_ == OB_SUCCESS) {
      ASSERT_EQ(test.discarded_, value.is_null());
      if (!test.discarded_) {
        ASSERT_EQ(test.target_type_, value.get_type());
        if (ob_is_uint_tc(test.target_type_)) {
          EXPECT_EQ(static_cast<uint64_t>(test.expected_), value.get_uint64());
        } else {
          EXPECT_EQ(test.expected_, value.get_int());
        }
      }
    }
  }
}

// Match DSL-produced ranges: SQL scalar parameters and fixed inclusion flags.
class TestHybridDslRange : public test::TestSqlUtils, public ::testing::Test
{
public:
  TestHybridDslRange()
      : compat_guard_(lib::Worker::CompatMode::MYSQL),
        range_ctx_(&exec_ctx_), range_expr_(nullptr) {}

  void SetUp() override
  {
    ASSERT_NO_FATAL_FAILURE(test::TestSqlUtils::init());
    ASSERT_NE(nullptr, exec_ctx_.get_physical_plan_ctx());
    range_ctx_.column_cnt_ = share::SEARCH_INDEX_KEY_CNT;
    range_ctx_.expr_factory_ = &expr_factory_;
    range_ctx_.session_info_ = &session_info_;
    range_ctx_.search_index_range_ctx_ = &search_ctx_;
    range_ctx_.expr_constraints_ = &constraints_;
    params().reset();
    for (const int64_t value : {10, 20}) {
      ObObjParam param;
      param.set_int(value);
      param.set_param_meta();
      ASSERT_EQ(OB_SUCCESS, params().push_back(param));
    }
    exec_ctx_.get_physical_plan_ctx()->set_original_param_cnt(params().count());
  }

  void TearDown() override
  {
    test::TestSqlUtils::destroy();
  }

  ParamStore &params()
  {
    return exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update();
  }

  int make_range_expr()
  {
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    int ret = OB_SUCCESS;
    ObColumnRefRawExpr *column = nullptr;
    OZ(expr_factory_.create_raw_expr(T_REF_COLUMN, column));
    if (OB_SUCC(ret)) {
      column->set_ref_id(3003, 16);
      column->set_data_type(ObJsonType);
      column->set_collation_type(CS_TYPE_UTF8MB4_BIN);
      OZ(column->extract_info());
      ObSearchIndexRangeCtx::ObColCtxInfo info;
      info.column_idx_ = 0;
      info.column_meta_ = ObRangeColumnMeta(column->get_result_type(), column->get_column_id());
      OZ(search_ctx_.col_ctxs_.push_back(info));
    }
    OZ(expr_factory_.create_raw_expr(T_FUN_SYS_DSL_MULTI_VALUE_RANGE, range_expr_));
    if (OB_SUCC(ret)) {
      range_expr_->set_func_name(N_DSL_MULTI_VALUE_RANGE);
    }
    OZ(range_expr_->init_param_exprs(5));
    OZ(range_expr_->add_param_expr(column));
    for (int64_t i = 0; OB_SUCC(ret) && i < params().count(); ++i) {
      ObConstRawExpr *constant = nullptr;
      OZ(expr_factory_.create_raw_expr(T_INT, constant));
      if (OB_SUCC(ret)) {
        constant->set_value(params().at(i));
        ObRawExpr *parameter = constant;
        OZ(ObRawExprUtils::create_param_expr(expr_factory_, i, parameter));
        OZ(range_expr_->add_param_expr(parameter));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      ObRawExpr *inclusive = nullptr;
      OZ(ObRawExprUtils::build_const_bool_expr(&expr_factory_, inclusive, true));
      OZ(range_expr_->add_param_expr(inclusive));
    }
    OZ(range_expr_->formalize(&session_info_));
    return ret;
  }

  int extract_range(ObRangeNode *&node)
  {
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    range_ctx_.final_exprs_.reuse();
    range_ctx_.final_exprs_flag_.reuse();
    range_ctx_.cur_is_precise_ = false;
    constraints_.reuse();
    ObExprRangeConverter converter(allocator_, range_ctx_);
    bool is_precise = false;
    return converter.convert_expr_to_range_node(range_expr_, node, 0, is_precise);
  }

  void expect_constraints_match(const bool expected)
  {
    LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
    bool matched = true;
    ASSERT_EQ(OB_SUCCESS, exec_ctx_.get_physical_plan_ctx()->init_datum_param_store());
    for (int64_t i = 0; matched && i < constraints_.count(); ++i) {
      const ObExprConstraint &constraint = constraints_.at(i);
      ObObj value;
      ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_raw_expr_without_row(exec_ctx_,
          constraint.pre_calc_expr_, value, &params(), allocator_));
      ObObjParam param;
      static_cast<ObObj &>(param) = value;
      param.set_param_meta();
      ObDatumObjParam datum;
      ASSERT_EQ(OB_SUCCESS, datum.alloc_datum_reserved_buff(
          param.get_meta(), param.get_precision(), allocator_));
      ASSERT_EQ(OB_SUCCESS, datum.from_objparam(param, &allocator_));
      ObPreCalcExprConstraint checker(allocator_);
      checker.expect_result_ = constraint.expect_result_;
      ObConstraintExtra extra = constraint.cons_extra_;
      ASSERT_EQ(OB_SUCCESS, checker.check_is_match(datum, exec_ctx_, &extra, matched));
    }
    EXPECT_EQ(expected, matched);
  }

  lib::CompatModeGuard compat_guard_;
  ObQueryRangeCtx range_ctx_;
  ObSearchIndexRangeCtx search_ctx_;
  ObSEArray<ObExprConstraint, 4> constraints_;
  ObSysFunRawExpr *range_expr_;
};

TEST_F(TestHybridDslRange, extracts_without_constraint_container)
{
  params().at(0).set_varchar("a");
  params().at(1).set_varchar("z");
  for (int64_t i = 0; i < params().count(); ++i) {
    params().at(i).set_collation_type(CS_TYPE_UTF8MB4_BIN);
    params().at(i).set_param_meta();
  }
  ASSERT_EQ(OB_SUCCESS, make_range_expr());
  range_ctx_.expr_constraints_ = nullptr;
  ObRangeNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract_range(node));
  ASSERT_NE(nullptr, node);
  EXPECT_FALSE(node->always_true_);
  EXPECT_TRUE(node->is_search_node_);
  EXPECT_TRUE(range_ctx_.cur_is_precise_);
  EXPECT_EQ(0, constraints_.count());
  EXPECT_EQ(OB_RANGE_MIN_VALUE, node->start_keys_[share::SEARCH_INDEX_DOC_ID]);
  EXPECT_EQ(OB_RANGE_MAX_VALUE, node->end_keys_[share::SEARCH_INDEX_DOC_ID]);
}

TEST_F(TestHybridDslRange, scalar_values_need_no_extra_constraints)
{
  ASSERT_EQ(OB_SUCCESS, make_range_expr());
  ObRangeNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract_range(node));
  ASSERT_NE(nullptr, node);
  EXPECT_FALSE(node->always_true_);
  EXPECT_TRUE(node->is_search_node_);
  EXPECT_TRUE(range_ctx_.cur_is_precise_);
  EXPECT_EQ(0, constraints_.count());
  params().at(0).set_int(11);
  params().at(1).set_int(19);
  ASSERT_EQ(OB_SUCCESS, extract_range(node));
  ASSERT_NE(nullptr, node);
  EXPECT_FALSE(node->always_true_);
  EXPECT_TRUE(node->is_search_node_);
  EXPECT_TRUE(range_ctx_.cur_is_precise_);
  EXPECT_EQ(0, constraints_.count());
}

TEST_F(TestHybridDslRange, scalar_string_length_constraint)
{
  params().at(0).set_varchar("a");
  params().at(1).set_varchar("z");
  for (int64_t i = 0; i < params().count(); ++i) {
    params().at(i).set_collation_type(CS_TYPE_UTF8MB4_BIN);
    params().at(i).set_param_meta();
  }
  ASSERT_EQ(OB_SUCCESS, make_range_expr());
  ObRangeNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, extract_range(node));
  ASSERT_NE(nullptr, node);
  ASSERT_FALSE(node->always_true_);
  ASSERT_EQ(2, constraints_.count());
  expect_constraints_match(true);
  params().at(0).set_varchar("not a JSON document");
  params().at(0).set_param_meta();
  expect_constraints_match(true);
  const std::string long_string(3000, 'm');
  params().at(0).set_varchar(ObString(long_string.length(), long_string.c_str()));
  params().at(0).set_param_meta();
  expect_constraints_match(false);
  ASSERT_EQ(OB_SUCCESS, extract_range(node));
  ASSERT_NE(nullptr, node);
  EXPECT_TRUE(node->always_true_);
}

TEST_F(TestHybridDslParameterization, integer_candidate_conversion_kinds)
{
  using Convert = ObExprDslParamValue;
  ObObj value;
  value.set_int(INT64_MAX);
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_int(-1);
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_uint64(UINT64_MAX);
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObUInt64Type));
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObIntType));
  value.set_int(INT32_MAX);
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObInt32Type));
  value.set_int(static_cast<int64_t>(INT32_MAX) + 1);
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObInt32Type));
  value.set_int(INT32_MIN);
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObInt32Type));
  value.set_int(static_cast<int64_t>(INT32_MIN) - 1);
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObInt32Type));
  value.set_double(2.0);
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_double(1.5);
  EXPECT_EQ(Convert::FRACTIONAL_VALUE, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_double(9223372036854775808.0);
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObIntType));
  EXPECT_EQ(Convert::INTEGER_VALUE, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_double(18446744073709551616.0);
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObUInt64Type));
  value.set_varchar("01");
  EXPECT_EQ(Convert::SQL_FALLBACK, Convert::classify_integer_candidate(value, ObIntType));
}

TEST_F(TestHybridDslRange, fulltext_uses_sql_implicit_cast_for_literals_and_parameters)
{
  session_info_.set_cur_exec_ctx(&exec_ctx_);
  ParamStore input_params((ObWrapperAllocator(&allocator_)));
  ObResolverParams resolver_params;
  resolver_params.allocator_ = &allocator_;
  resolver_params.expr_factory_ = &expr_factory_;
  resolver_params.session_info_ = &session_info_;
  resolver_params.param_list_ = &input_params;
  TableItem table_item(allocator_);
  ObDSLResolver resolver(resolver_params, nullptr, nullptr, table_item);
  ObJsonString text(ObString("hello😀"));
  ObHybridDslRequestInfo request(allocator_);
  ASSERT_EQ(OB_SUCCESS, request.append_param_node(text));
  request.set_sql_raw_param_count(0);
  ParseNode raw_node = {};
  ObPCParam raw;
  raw.node_ = &raw_node;
  ObSEArray<ObPCParam *, 1> raw_params;
  ASSERT_EQ(OB_SUCCESS, raw_params.push_back(&raw));
  for (const bool parameterized : {false, true}) {
    SCOPED_TRACE(parameterized);
    input_params.reset();
    if (parameterized) {
      ObObjParam value;
      value.set_varchar("hello😀");
      value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      value.set_collation_level(CS_LEVEL_COERCIBLE);
      value.set_param_meta();
      ASSERT_EQ(OB_SUCCESS, input_params.push_back(value));
      request.set_compile_params(raw_params, input_params, 0);
      resolver.dsl_request_ = &request;
    }
    ObRawExpr *expr = nullptr;
    ASSERT_EQ(OB_SUCCESS, resolver.resolve_query_string_expr(text, CS_TYPE_GBK_BIN, expr));
    ASSERT_NE(nullptr, expr);
    ASSERT_EQ(T_FUN_SYS_CAST, expr->get_expr_type());
    EXPECT_TRUE(CM_IS_IGNORE_CHARSET_CONVERT_ERR(expr->get_cast_mode()));
    EXPECT_EQ(CS_TYPE_GBK_BIN, expr->get_collation_type());
    ASSERT_GT(expr->get_param_count(), 0);
    ObRawExpr *input = expr->get_param_expr(0);
    ASSERT_NE(nullptr, input);
    EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, input->get_collation_type());
    if (parameterized) {
      ASSERT_EQ(T_QUESTIONMARK, input->get_expr_type());
      const ObConstRawExpr *param = static_cast<const ObConstRawExpr *>(input);
      EXPECT_EQ(0, param->get_value().get_unknown());
      EXPECT_TRUE(request.get_resolved_param_idxs().has_member(0));
    } else {
      ObObj result;
      ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_const_expr(&session_info_, *expr, result,
          allocator_, input_params, &exec_ctx_));
      EXPECT_EQ(ObString("hello?"), result.get_string());
    }
  }
}

TEST_F(TestHybridDslRange, sort_missing_uses_sql_truncate_and_cast_with_es_boundaries)
{
  session_info_.set_cur_exec_ctx(&exec_ctx_);
  ObResolverParams resolver_params;
  resolver_params.allocator_ = &allocator_;
  resolver_params.expr_factory_ = &expr_factory_;
  resolver_params.session_info_ = &session_info_;
  TableItem table_item(allocator_);
  ObDSLResolver resolver(resolver_params, nullptr, nullptr, table_item);
  ParamStore empty_params;
  const auto expect_value = [&](ObRawExpr *expr, const int64_t expected) {
    ObObj result;
    ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_const_expr(&session_info_, *expr, result,
        allocator_, empty_params, &exec_ctx_));
    if (expr->get_result_type().get_type() == ObUInt64Type) {
      EXPECT_EQ(static_cast<uint64_t>(expected) ^ (uint64_t(1) << 63), result.get_uint64());
    } else {
      EXPECT_EQ(expected, result.get_int());
    }
  };
  const double values[] = {49.9, -49.9, 0.9, -0.9, 1e19, -1e19};
  const int64_t expected[] = {49, -49, 0, 0, INT64_MAX, INT64_MIN};
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    SCOPED_TRACE(values[i]);
    ObJsonDouble input(values[i]);
    ObRawExpr *expr = nullptr;
    ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(input, ObIntType, expr));
    ASSERT_NE(nullptr, expr);
    ASSERT_EQ(T_FUN_SYS_CAST, expr->get_expr_type());
    EXPECT_FALSE(expr->has_flag(IS_INNER_ADDED_EXPR));
    ASSERT_GT(expr->get_param_count(), 0);
    ASSERT_NE(nullptr, expr->get_param_expr(0));
    EXPECT_EQ(T_FUN_SYS_TRUNCATE, expr->get_param_expr(0)->get_expr_type());
    expect_value(expr, expected[i]);
    ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(input, ObUInt64Type, expr));
    ASSERT_NE(nullptr, expr);
    ASSERT_EQ(T_OP_BIT_XOR, expr->get_expr_type());
    EXPECT_EQ(ObUInt64Type, expr->get_result_type().get_type());
    ASSERT_EQ(2, expr->get_param_count());
    ASSERT_NE(nullptr, expr->get_param_expr(0));
    ObRawExpr *unsigned_cast = expr->get_param_expr(0);
    ASSERT_EQ(T_FUN_SYS_CAST, unsigned_cast->get_expr_type());
    EXPECT_EQ(ObUInt64Type, unsigned_cast->get_result_type().get_type());
    EXPECT_FALSE(unsigned_cast->has_flag(IS_INNER_ADDED_EXPR));
    ASSERT_GT(unsigned_cast->get_param_count(), 0);
    ASSERT_NE(nullptr, unsigned_cast->get_param_expr(0));
    EXPECT_EQ(T_FUN_SYS_CAST, unsigned_cast->get_param_expr(0)->get_expr_type());
    EXPECT_EQ(ObIntType, unsigned_cast->get_param_expr(0)->get_result_type().get_type());
    expect_value(expr, expected[i]);
  }
  ObJsonUint max_unsigned(UINT64_MAX);
  ObRawExpr *expr = nullptr;
  ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(max_unsigned, ObIntType, expr));
  ASSERT_NE(nullptr, expr);
  EXPECT_EQ(T_FUN_SYS_CAST, expr->get_expr_type());
  expect_value(expr, -1);
  ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(max_unsigned, ObUInt64Type, expr));
  ASSERT_NE(nullptr, expr);
  EXPECT_EQ(T_OP_BIT_XOR, expr->get_expr_type());
  expect_value(expr, -1);

  // Missing is a general expression and must survive sort-item deep copying.
  ObDSLQueryInfo source;
  ObDSLQueryInfo copied;
  ObDSLSortItem item;
  item.missing_mode_ = ObDSLSortItem::MissingMode::LITERAL;
  item.field_expr_ = expr;
  item.output_expr_ = expr;
  item.missing_expr_ = expr;
  ASSERT_EQ(OB_SUCCESS, source.sort_items_.push_back(item));
  ObRawExprCopier copier(expr_factory_);
  ASSERT_EQ(OB_SUCCESS, copied.deep_copy_sort_items(source, copier));
  ASSERT_EQ(1, copied.sort_items_.count());
  ASSERT_NE(nullptr, copied.sort_items_.at(0).missing_expr_);
  EXPECT_EQ(T_OP_BIT_XOR, copied.sort_items_.at(0).missing_expr_->get_expr_type());
  expect_value(copied.sort_items_.at(0).missing_expr_, -1);

  ObJsonInt integer(49);
  ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(integer, ObIntType, expr));
  ASSERT_NE(nullptr, expr);
  EXPECT_TRUE(expr->is_const_raw_expr());
  expect_value(expr, 49);
  ObJsonUint small_unsigned(49);
  ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(small_unsigned, ObIntType, expr));
  ASSERT_NE(nullptr, expr);
  EXPECT_TRUE(expr->is_const_raw_expr());
  EXPECT_EQ(ObIntType, expr->get_result_type().get_type());
  expect_value(expr, 49);
  ObJsonString integer_string(ObString("49"));
  ASSERT_EQ(OB_SUCCESS, resolver.build_sort_missing_integer_expr(integer_string, ObIntType, expr));
  ASSERT_NE(nullptr, expr);
  EXPECT_TRUE(expr->is_const_raw_expr());
  expect_value(expr, 49);
  for (const bool value : {false, true}) {
    ObJsonBoolean input(value);
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolver.build_sort_missing_integer_expr(input, ObIntType, expr));
    EXPECT_EQ(nullptr, expr);
  }
  for (const char *text : {"49.9", "49e0", "49.9e0", "18446744073709551615"}) {
    SCOPED_TRACE(text);
    ObJsonString input(ObString::make_string(text));
    EXPECT_NE(OB_SUCCESS, resolver.build_sort_missing_integer_expr(input, ObIntType, expr));
    EXPECT_EQ(nullptr, expr);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
