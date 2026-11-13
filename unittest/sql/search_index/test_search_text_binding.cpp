/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <string>
#include <vector>
#define private public
#define protected public
#include "sql/hybrid_search/ob_fulltext_search_query.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_join_order_enum_permutation.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/mock_schema_service.h"
#undef protected
#undef private
#include "common/ob_tenant_data_version_mgr.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/rewrite/ob_search_index_query_range_utils.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "storage/fts/ob_fts_parser_property.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

class SearchTextBindingTest : public ::testing::Test
{
protected:
  ObArenaAllocator alloc_;
  ObTableSchema main_, def_, match_, phrase_;
  ObSearchIndexTextInfo info_;
  uint64_t selected_text_id_ = OB_INVALID_ID;
  uint64_t saved_tenant_id_ = OB_INVALID_TENANT_ID;

  void SetUp() override
  {
    saved_tenant_id_ = ObTenantEnv::get_tenant_local()->id_;
    ObTenantEnv::get_tenant_local()->id_ = OB_SYS_TENANT_ID;
    init(main_, 1001, 0, INDEX_TYPE_IS_NOT);
    add_column(main_, 16, "id", ObUInt64Type, 1);
    main_.set_rowkey_column_num(1);
    add_column(main_, 42, "doc", ObJsonType, 0);
    init(def_, 2001, 1001, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
    ASSERT_EQ(OB_SUCCESS, def_.set_table_name("__idx_1001_ft"));
    add_column(def_, 42, "doc", ObJsonType, 1);
    init_text(match_, 3001, false);
    init_text(phrase_, 3002, true);
    set_config(def_, OB_FTS_INDEX_TYPE_MATCH);
  }

  void resolve_native(ObSQLSessionInfo &session, ObSchemaChecker &checker,
                      ObStmtScope scope, ObMatchAgainstMode mode,
                      int expected_ret, bool expected_search_text, bool with_json_path = false,
                      ObObjType second_type = ObMaxType)
  {
    ObRawExprFactory factory(alloc_);
    ObQueryCtx query_ctx(alloc_);
    ObStmtFactory statements(alloc_);
    ObSelectStmt *stmt = nullptr;
    ASSERT_EQ(OB_SUCCESS, statements.create_stmt(stmt));
    stmt->set_query_ctx(&query_ctx);
    TableItem table(alloc_);
    table.table_id_ = main_.get_table_id();
    table.ref_id_ = main_.get_table_id();
    table.type_ = TableItem::BASE_TABLE;
    ASSERT_EQ(OB_SUCCESS, stmt->add_table_item(&session, &table));
    ParamStore param_store;
    ObResolverParams params;
    params.allocator_ = &alloc_;
    params.expr_factory_ = &factory;
    params.session_info_ = &session;
    params.schema_checker_ = &checker;
    params.query_ctx_ = &query_ctx;
    params.param_list_ = &param_store;
    ObSelectResolver resolver(params);
    resolver.stmt_ = stmt;
    ObMatchFunRawExpr *match = nullptr;
    ObColumnRefRawExpr *column = nullptr;
    ObConstRawExpr *query = nullptr;
    ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
    ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
    column->set_ref_id(table.table_id_, 42);
    column->set_data_type(main_.get_column_schema(42)->get_data_type());
    column->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
    if (second_type != ObMaxType) {
      ObColumnRefRawExpr *second_column = nullptr;
      main_.get_column_schema(43)->set_data_type(second_type);
      ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, second_column));
      second_column->set_ref_id(table.table_id_, 43);
      second_column->set_data_type(second_type);
      second_column->set_collation_type(CS_TYPE_UTF8MB4_BIN);
      ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(second_column));
    }
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
        factory, ObVarcharType, ObString("alpha"), CS_TYPE_UTF8MB4_BIN, query));
    match->set_search_key(query);
    match->set_mode_flag(mode);
    if (with_json_path) {
      ObConstRawExpr *path = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
          factory, ObVarcharType, ObString("$.title"), CS_TYPE_UTF8MB4_BIN, path));
      match->set_json_path_expr(path);
    }
    ObSEArray<ObMatchFunRawExpr *, 1> matches;
    ASSERT_EQ(OB_SUCCESS, matches.push_back(match));
    ObRawExpr *expr = match;
    EXPECT_EQ(expected_ret, resolver.resolve_match_against_exprs(expr, matches, scope));
    EXPECT_EQ(expected_search_text, match->is_search_text());
  }

  void TearDown() override
  {
    ObTenantEnv::get_tenant_local()->id_ = saved_tenant_id_;
  }

  void init(ObTableSchema &s, uint64_t id, uint64_t parent, ObIndexType type)
  {
    s.set_tenant_id(1);
    s.set_database_id(1002);
    s.set_charset_type(CHARSET_UTF8MB4);
    s.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    s.set_table_id(id);
    s.set_data_table_id(parent);
    s.set_table_type(type == INDEX_TYPE_IS_NOT ? USER_TABLE : USER_INDEX);
    s.set_index_type(type);
    s.set_index_status(INDEX_STATUS_AVAILABLE);
    s.set_schema_version(7);
    ASSERT_EQ(OB_SUCCESS, s.set_table_name("t"));
  }

  void add_column(ObTableSchema &s, uint64_t id, const char *name, ObObjType type,
                  int64_t key)
  {
    ObColumnSchemaV2 c;
    c.set_tenant_id(1);
    c.set_table_id(s.get_table_id());
    c.set_column_id(id);
    ASSERT_EQ(OB_SUCCESS, c.set_column_name(name));
    c.set_data_type(type);
    c.set_collation_type(CS_TYPE_BINARY);
    c.set_data_length(2048);
    c.set_nullable(false);
    c.set_rowkey_position(key);
    c.set_index_position(s.is_index_table() ? key : 0);
    ASSERT_EQ(OB_SUCCESS, s.add_column(c));
    s.set_max_used_column_id(id);
    if (key > 0) {
      s.set_rowkey_column_num(key);
      if (s.is_index_table()) {
        s.set_index_column_num(key);
      }
    }
  }

  void init_text(ObTableSchema &s, uint64_t id, bool phrase)
  {
    init(s, id, 2001, phrase ? INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
                            : INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL);
    // Ordinary columns with non-contiguous IDs, unrelated to the source column.
    add_column(s, 70, "col", ObUInt64Type, 1);
    add_column(s, 80, "path", ObVarcharType, 2);
    add_column(s, 90, "token", ObVarcharType, 3);
    add_column(s, 100, "docid", ObUInt64Type, 4);
    add_column(s, 110, "tf", ObUInt64Type, 0);
    add_column(s, 120, "dl", ObUInt64Type, 0);
    if (phrase) {
      add_column(s, 130, "pos", ObVarcharType, 0);
    }
    // DDL keeps the common nullable default and the source charset for binary strings.
    for (int64_t i = 0; i < s.get_column_count(); ++i) {
      ObColumnSchemaV2 *column = s.get_column_schema_by_idx(i);
      column->set_nullable(true);
      if (column->get_data_type() == ObVarcharType) {
        column->set_charset_type(ObCharset::charset_type_by_coll(main_.get_collation_type()));
      }
    }
    s.set_rowkey_column_num(4);
  }

  void set_config(ObTableSchema &s, ObFTSIndexType type,
                  const char *filter = "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string",
                  const char *parser = "space.1", const char *properties = "{}",
                  const char *nested = "")
  {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig mode;
    mode.mode_ = type == OB_FTS_INDEX_TYPE_MATCH
        ? ObSearchIndexModeConfig::FULLTEXT_MATCH : ObSearchIndexModeConfig::FULLTEXT_PHRASE;
    mode.filter_ = ObString::make_string(filter);
    mode.parser_name_ = ObString::make_string(parser);
    mode.parser_properties_ = ObString::make_string(properties);
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ASSERT_EQ(OB_SUCCESS, config.init(modes, ObString::make_string(nested)));
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
    ASSERT_EQ(OB_SUCCESS, s.get_column_schema(42)->set_comment(encoded.string()));
  }

  int check_candidate(const ObTableSchema &text, const ObString &path,
                      ObFTSIndexType type, bool &found)
  {
    found = false;
    info_ = ObSearchIndexTextInfo();
    selected_text_id_ = OB_INVALID_ID;
    ObJsonPath parsed(path, &alloc_);
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> items;
    bool path_supported = true;
    int ret = OB_SUCCESS;
    if (main_.get_column_schema(42)->get_meta_type().is_json()) {
      ret = ObSearchTextBinder::prepare_query(
          main_, 42, parsed, alloc_, items, path_supported);
    }
    if (OB_SUCC(ret) && path_supported) {
      ret = ObSearchTextBinder::check_search_fulltext_index_match_column(
          main_, 42, type, items, def_, text, alloc_, info_, found);
    }
    if (OB_SUCC(ret) && found) {
      selected_text_id_ = text.get_table_id();
    }
    return ret;
  }

  int check(const ObTableSchema &text, const char *path = "$.title",
            ObFTSIndexType type = OB_FTS_INDEX_TYPE_MATCH)
  {
    bool found = false;
    int ret = check_candidate(text, ObString::make_string(path), type, found);
    EXPECT_EQ(found, info_.has_search_index_fulltext());
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("no matched Search Text index in binding fixture", K(ret), K(path), K(type));
    }
    return ret;
  }

  void register_candidate(ObSqlSchemaGuard &guard, ObTableSchema &def, ObTableSchema &text)
  {
    def.set_table_id(guard.get_next_mocked_schema_id());
    def.set_data_table_id(main_.get_table_id());
    text.set_table_id(guard.get_next_mocked_schema_id());
    text.set_data_table_id(def.get_table_id());
    ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(
        ObAuxTableMetaInfo(text.get_table_id(), text.get_table_type(), text.get_index_type())));
    ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
        ObAuxTableMetaInfo(def.get_table_id(), def.get_table_type(), def.get_index_type())));
    ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&def));
    ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&text));
  }

  int resolve(ObSqlSchemaGuard &guard, bool need_pos,
              const ObIArray<uint64_t> *valid_index_ids = nullptr)
  {
    int ret = OB_SUCCESS;
    ObColumnRefRawExpr field;
    field.set_ref_id(main_.get_table_id(), 42);
    field.set_data_type(main_.get_column_schema(42)->get_data_type());
    field.set_collation_type(main_.get_column_schema(42)->get_collation_type());
    ObFullTextIndexBinding binding;
    ObSEArray<uint64_t, 4> candidates;
    if (nullptr == valid_index_ids) {
      ObSEArray<ObAuxTableMetaInfo, 4> defs;
      if (OB_FAIL(main_.get_simple_index_infos(defs))) {
        LOG_WARN("failed to get index definitions for binding fixture", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < defs.count(); ++i) {
        if (OB_FAIL(candidates.push_back(defs.at(i).table_id_))) {
          LOG_WARN("failed to append binding fixture candidate", K(ret), K(i));
        }
      }
      valid_index_ids = &candidates;
    }
    if (OB_SUCC(ret)) {
      binding.search_info_.path_ = field.get_result_type().is_json() ? ObString("$.title") : ObString();
      if (OB_FAIL(ObDSLFullTextQuery::resolve_single_field_index(
          field, need_pos, main_, guard, *valid_index_ids, alloc_, binding))) {
        LOG_WARN("failed to resolve binding fixture index", K(ret));
      } else {
        EXPECT_TRUE(binding.is_async_);
        EXPECT_NE(OB_INVALID_ID, binding.inv_idx_tid_);
      }
      info_ = binding.search_info_;
      selected_text_id_ = binding.inv_idx_tid_;
    }
    return ret;
  }

  void expect_space_properties(const ObSearchIndexTextInfo &binding, int64_t expected_max = 84)
  {
    oceanbase::storage::ObFTParserJsonProps props;
    ASSERT_EQ(OB_SUCCESS, props.init());
    ASSERT_EQ(OB_SUCCESS, props.parse_from_valid_str(binding.parser_properties_));
    int64_t min_size = 0;
    int64_t max_size = 0;
    ASSERT_EQ(OB_SUCCESS, props.config_get_min_token_size(min_size));
    ASSERT_EQ(OB_SUCCESS, props.config_get_max_token_size(max_size));
    EXPECT_EQ(3, min_size);
    EXPECT_EQ(expected_max, max_size);
  }
};

TEST_F(SearchTextBindingTest, binds_physical_child_and_owned_config)
{
  bool found = false;
  ASSERT_EQ(OB_SUCCESS, check_candidate(
      match_, ObString("$.title"), OB_FTS_INDEX_TYPE_MATCH, found));
  ASSERT_TRUE(found);
  // Selected parser strings must outlive the schema/config buffers.
  def_.reset();
  EXPECT_EQ(2001, info_.def_table_id_);
  EXPECT_EQ(3001, selected_text_id_);
  int64_t store_idx = -1;
  ASSERT_EQ(OB_SUCCESS, main_.get_store_column_idx(42, store_idx));
  EXPECT_EQ(store_idx, info_.column_idx_);
  EXPECT_EQ(ObString("space.1"), info_.parser_name_);
  EXPECT_FALSE(info_.parser_properties_.empty());
  ASSERT_NO_FATAL_FAILURE(expect_space_properties(info_));
  ObMatchQueryNode node(alloc_);
  node.search_index_fulltext_info_ = info_;
  node.inv_idx_tid_ = selected_text_id_;
  node.search_index_fulltext_info_.path_ = ObString("$.title");
  node.index_name_ = ObString("ft");
  char buf[2048] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, node.explain_info(buf, sizeof(buf), pos, EXPLAIN_BASIC, 0));
  EXPECT_STREQ("\nmatch node: index name=ft", buf);
  ObSEArray<uint64_t, 4> ids;
  ASSERT_EQ(OB_SUCCESS, node.get_all_index_ids(ids));
  EXPECT_EQ(2, ids.count());
  EXPECT_EQ(2001, ids.at(0));
  EXPECT_EQ(3001, ids.at(1));
}

TEST_F(SearchTextBindingTest, coverage_and_capability)
{
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.body"));
  EXPECT_FALSE(info_.has_search_index_fulltext());
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(phrase_, "$.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_EQ(OB_SUCCESS, check(phrase_, "$.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(3002, selected_text_id_);
  ASSERT_EQ(OB_SUCCESS, check(phrase_));
  EXPECT_EQ(3002, selected_text_id_);
  EXPECT_EQ(OB_FTS_INDEX_TYPE_PHRASE_MATCH, info_.index_type_);
  phrase_.set_index_status(INDEX_STATUS_UNAVAILABLE);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(phrase_));
}

TEST_F(SearchTextBindingTest, invalid_and_uncovered_paths)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_, ""));
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_, "$."));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.*"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$**.title"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.title[0]"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.title[*]"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.Title"));
  EXPECT_EQ(OB_SUCCESS, check(match_, "$.\"title\""));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$"));
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "EXCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string");
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_));
  EXPECT_EQ(OB_SUCCESS, check(match_, "$.body"));
}

TEST_F(SearchTextBindingTest, validates_query_before_candidate_lookup)
{
  ObSqlSchemaGuard guard;
  ObSEArray<uint64_t, 4> candidates;
  ObColumnRefRawExpr field;
  field.set_ref_id(main_.get_table_id(), 42);
  field.set_data_type(ObJsonType);
  ObFullTextIndexBinding binding;
  // Invalid input must be rejected even when there are no candidate indexes.
  binding.search_info_.path_ = ObString("$.");
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDSLFullTextQuery::resolve_single_field_index(
      field, false, main_, guard, candidates, alloc_, binding));
  field.set_ref_id(main_.get_table_id(), 9999);
  binding.search_info_.path_ = ObString("$.title");
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDSLFullTextQuery::resolve_single_field_index(
      field, false, main_, guard, candidates, alloc_, binding));
  field.set_ref_id(main_.get_table_id(), 42);
  binding.search_info_.path_ = ObString("$.*");
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, ObDSLFullTextQuery::resolve_single_field_index(
      field, false, main_, guard, candidates, alloc_, binding));
  EXPECT_FALSE(binding.search_info_.has_search_index_fulltext());
  EXPECT_EQ(OB_INVALID_ID, binding.inv_idx_tid_);
}

TEST_F(SearchTextBindingTest, value_and_text_share_member_path_semantics)
{
  ObRawExprFactory factory(alloc_);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObExecContext exec(alloc_);
  exec.set_my_session(&session);
  ASSERT_EQ(OB_SUCCESS, exec.create_physical_plan_ctx());
  ObSearchIndexRangeCtx range;
  ObQueryRangeCtx ctx(&exec);
  ctx.session_info_ = &session;
  ctx.search_index_range_ctx_ = &range;
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "");
  struct PathCase {
    const char *path;
    bool supported;
    std::string encoded;
  };
  const PathCase cases[] = {
    {"$", true, ""},
    {"$.Title", true, std::string("\0\x1c" "Title", 7)},
    {R"($."a.b")", true, std::string("\0\x1c" "a.b", 5)},
    {R"($."a\"b")", true, std::string("\0\x1c" "a\"b", 5)},
    {"$.title[0]", false, ""},
    {"$.*", false, ""},
    {"$.title[*]", false, ""}
  };
  for (const PathCase &item : cases) {
    ObConstRawExpr *expr = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
        ObString(item.path), CS_TYPE_UTF8MB4_BIN, expr));
    ASSERT_EQ(OB_SUCCESS, expr->formalize(&session));
    bool can_extract = false;
    ObString prefix;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
        alloc_, ctx, *expr, prefix, false, can_extract)) << item.path;
    EXPECT_EQ(item.supported, can_extract) << item.path;
    if (can_extract) {
      EXPECT_EQ(ObString(item.encoded.size(), item.encoded.data()), prefix) << item.path;
    }
    EXPECT_EQ(item.supported ? OB_SUCCESS : OB_ERR_FT_COLUMN_NOT_INDEXED,
        check(match_, item.path)) << item.path;
  }

  // VALUE retains its explicit nested scope; declaring a path alone does not open it.
  ObSearchIndexConfigFilter filter(1);
  ObSearchIndexConfig value_config;
  ASSERT_EQ(OB_SUCCESS, value_config.decode(ObString("NESTED_PATHS=7$.items")));
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(value_config.get_comment()));
  range.json_filter_ = &filter;
  ObConstRawExpr *expr = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
      ObString("$.items.Title"), CS_TYPE_UTF8MB4_BIN, expr));
  ASSERT_EQ(OB_SUCCESS, expr->formalize(&session));
  bool can_extract = false;
  ObString plain, nested;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
      alloc_, ctx, *expr, plain, false, can_extract));
  ASSERT_TRUE(can_extract);
  EXPECT_EQ(ObString(14, "\0\x1c" "items\0\x1c" "Title"), plain);
  ASSERT_EQ(OB_SUCCESS, range.enclosing_nested_paths_.push_back(ObString("$.items")));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
      alloc_, ctx, *expr, nested, false, can_extract));
  ASSERT_TRUE(can_extract);
  EXPECT_NE(plain, nested);
  filter.reset();
  ASSERT_EQ(OB_SUCCESS, value_config.decode(ObString("INCLUDE_PATHS=13$.items.Title")));
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(value_config.get_comment()));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
      alloc_, ctx, *expr, nested, false, can_extract));
  EXPECT_FALSE(can_extract);
}

TEST_F(SearchTextBindingTest, legacy_and_malformed_config)
{
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(ObString("")));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_));
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(ObString("SEARCH_INDEX_CONFIG=99:1:")));
  EXPECT_EQ(OB_NOT_SUPPORTED, check(match_));
  EXPECT_FALSE(info_.has_search_index_fulltext());
}

TEST_F(SearchTextBindingTest, persisted_removed_mode_and_duplicate_value_are_rejected)
{
  const ObString comment = def_.get_column_schema(42)->get_comment_str();
  std::string removed(comment.ptr(), comment.length());
  const std::string prefix = "SEARCH_INDEX_CONFIG=1:1:0:";
  ASSERT_EQ(0, removed.compare(0, prefix.size(), prefix));
  ASSERT_EQ('1', removed[prefix.size()]);
  removed[prefix.size()] = '3';
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString(removed.size(), removed.data())));
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_));
  EXPECT_FALSE(info_.has_search_index_fulltext());

  ObSearchIndexConfig config;
  ObSEArray<ObSearchIndexModeConfig, 4> modes;
  ObSearchIndexModeConfig value;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(value));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  const std::string wire(encoded.ptr(), encoded.length());
  ASSERT_EQ(0, wire.compare(0, prefix.size(), prefix));
  const std::string block = wire.substr(prefix.size());
  const std::string duplicated = "SEARCH_INDEX_CONFIG=1:2:0:" + block + block;
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString(duplicated.size(), duplicated.data())));
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_));
  EXPECT_FALSE(info_.has_search_index_fulltext());
}

TEST_F(SearchTextBindingTest, parent_validation)
{
  match_.set_data_table_id(9999);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_));
  EXPECT_FALSE(info_.has_search_index_fulltext());
  match_.set_data_table_id(2001);
  EXPECT_EQ(OB_SUCCESS, check(match_));
}

TEST_F(SearchTextBindingTest, single_candidate_mismatch_is_not_an_error)
{
  bool found = true;
  ASSERT_EQ(OB_SUCCESS, check_candidate(
      match_, ObString("$.body"), OB_FTS_INDEX_TYPE_MATCH, found));
  EXPECT_FALSE(found);
  EXPECT_FALSE(info_.has_search_index_fulltext());
  ASSERT_EQ(OB_SUCCESS, check_candidate(
      match_, ObString("$.title"), OB_FTS_INDEX_TYPE_MATCH, found));
  ASSERT_TRUE(found);
  ASSERT_EQ(OB_SUCCESS, check_candidate(
      match_, ObString("$.title"), OB_FTS_INDEX_TYPE_PHRASE_MATCH, found));
  EXPECT_FALSE(found);
  EXPECT_FALSE(info_.has_search_index_fulltext());
}

TEST_F(SearchTextBindingTest, multiple_indexes_stop_at_first_match)
{
  ObSqlSchemaGuard guard;
  ObTableSchema def2, text2;
  ASSERT_EQ(OB_SUCCESS, def2.assign(def_));
  ASSERT_EQ(OB_SUCCESS, text2.assign(match_));
  set_config(def2, OB_FTS_INDEX_TYPE_MATCH,
      "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string", "ngram.1");
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, match_));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def2, text2));
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(match_.get_table_id(), selected_text_id_);
  EXPECT_EQ(ObString("space.1"), info_.parser_name_);
  // A later malformed candidate must not be inspected after MATCH succeeds.
  ASSERT_EQ(OB_SUCCESS, def2.get_column_schema(42)->set_comment(ObString("SEARCH_INDEX_CONFIG=99:1:")));
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(match_.get_table_id(), selected_text_id_);
}

TEST_F(SearchTextBindingTest, match_prefers_match_then_falls_back_to_phrase)
{
  ObSqlSchemaGuard guard;
  ObTableSchema def2;
  ASSERT_EQ(OB_SUCCESS, def2.assign(def_));
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  set_config(def2, OB_FTS_INDEX_TYPE_MATCH,
      "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string", "ngram.1");
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, phrase_));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def2, match_));
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(match_.get_table_id(), selected_text_id_);
  EXPECT_EQ(ObString("ngram.1"), info_.parser_name_);
  EXPECT_EQ(OB_FTS_INDEX_TYPE_MATCH, info_.index_type_);
  ASSERT_EQ(OB_SUCCESS, resolve(guard, true));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  // An available MATCH table is insufficient if its configuration does not cover the path.
  set_config(def2, OB_FTS_INDEX_TYPE_MATCH, "INCLUDE_PATHS=6$.body", "ngram.1");
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  EXPECT_EQ(OB_FTS_INDEX_TYPE_PHRASE_MATCH, info_.index_type_);
  set_config(def2, OB_FTS_INDEX_TYPE_MATCH);
  match_.set_index_visibility(1);
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  phrase_.set_index_status(INDEX_STATUS_UNAVAILABLE);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false));
  EXPECT_FALSE(info_.has_search_index_fulltext());
  match_.set_index_visibility(0);
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(match_.get_table_id(), selected_text_id_);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, true));
}

TEST_F(SearchTextBindingTest, text_selection_respects_effective_candidates)
{
  ObSqlSchemaGuard guard;
  ObTableSchema def2, text2;
  ASSERT_EQ(OB_SUCCESS, def2.assign(def_));
  ASSERT_EQ(OB_SUCCESS, text2.assign(match_));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, match_));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def2, text2));
  ObSEArray<uint64_t, 4> ids;
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false, &ids));
  // Restricting the candidate set must override the table's first matching Def.
  ASSERT_EQ(OB_SUCCESS, ids.push_back(def2.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false, &ids));
  EXPECT_EQ(text2.get_table_id(), selected_text_id_);
  // No Data child is required. A hidden Text must not fall back outside the set.
  text2.set_index_visibility(1);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false, &ids));
  text2.set_index_visibility(0);
  def2.set_index_visibility(1);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false, &ids));
}

TEST_F(SearchTextBindingTest, ordinary_fulltext_binds_during_node_creation)
{
  // Both ordinary groups work independently; mixed sync/async fields are rejected.
  for (int async_fields : {0, 1, 2}) {
    SCOPED_TRACE(async_fields);
    ObSqlSchemaGuard guard;
    ObTableSchema main, fts[2], deletes;
    ASSERT_EQ(OB_SUCCESS, main.assign(main_));
    ObColumnRefRawExpr fields[2];
    for (int64_t i = 0; i < 2; ++i) {
      const uint64_t column_id = 60 + i * 2;
      init(fts[i], guard.get_next_mocked_schema_id(), main.get_table_id(),
          i < async_fields ? INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL : INDEX_TYPE_FTS_INDEX_LOCAL);
      ASSERT_EQ(OB_SUCCESS, fts[i].set_table_name(i == 0 ? "__idx_1001_title_ft" : "__idx_1001_body_ft"));
      add_column(main, column_id, i == 0 ? "title" : "body", ObVarcharType, 0);
      add_column(main, column_id + 1, i == 0 ? "title_word" : "body_word", ObVarcharType, 0);
      add_column(fts[i], column_id + 1, "word", ObVarcharType, 1);
      main.get_column_schema(column_id + 1)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
      ASSERT_EQ(OB_SUCCESS, main.get_column_schema(column_id + 1)->add_cascaded_column_id(column_id));
      fts[i].get_column_schema(column_id + 1)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
      fields[i].set_ref_id(main.get_table_id(), column_id);
      fields[i].set_data_type(ObVarcharType);
    }
    ObRawExprFactory factory(alloc_);
    ObConstRawExpr *query = nullptr;
    ObConstRawExpr *one = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
        ObString("alpha beta"), CS_TYPE_UTF8MB4_BIN, query));
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
    ObDSLMatchQuery *dsl_match = nullptr;
    ObDSLMatchPhraseQuery *dsl_phrase = nullptr;
    ObDSLMultiMatchQuery *dsl_multi = nullptr;
    ObDSLQueryStringQuery *dsl_string = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObDSLMatchQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, dsl_match));
    ASSERT_EQ(OB_SUCCESS, ObDSLMatchPhraseQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, dsl_phrase));
    ASSERT_EQ(OB_SUCCESS, ObDSLMultiMatchQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, dsl_multi));
    ASSERT_EQ(OB_SUCCESS, ObDSLQueryStringQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, dsl_string));
    dsl_match->field_ = &fields[0];
    dsl_match->minimum_should_match_ = one;
    dsl_match->operator_ = one;
    dsl_phrase->field_ = &fields[0];
    dsl_phrase->slop_ = one;
    for (ObDSLFullTextMultiFieldQueryParam *params : {&dsl_multi->fields_param_, &dsl_string->fields_param_}) {
      params->minimum_should_match_ = one;
      params->operator_ = one;
      params->field_type_ = one;
      ASSERT_EQ(OB_SUCCESS, params->fields_.reserve(2));
      ASSERT_EQ(OB_SUCCESS, params->field_paths_.reserve(2));
      ASSERT_EQ(OB_SUCCESS, params->field_boosts_.reserve(2));
      for (int64_t i = 0; i < 2; ++i) {
        ASSERT_EQ(OB_SUCCESS, params->fields_.push_back(&fields[i]));
        ASSERT_EQ(OB_SUCCESS, params->field_paths_.push_back(ObString()));
        ASSERT_EQ(OB_SUCCESS, params->field_boosts_.push_back(one));
      }
    }
    ObDSLFullTextQuery *queries[] = {dsl_match, dsl_phrase, dsl_multi, dsl_string};
    ObSEArray<uint64_t, 1> candidates;
    for (ObDSLFullTextQuery *dsl : queries) {
      dsl->query_ = query;
      dsl->boost_ = one;
      ObFullTextQueryNode *node = nullptr;
      EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED,
          dsl->produce_query_node(main, guard, candidates, alloc_, node));
      EXPECT_EQ(nullptr, node);
    }
    for (ObTableSchema &index : fts) {
      ASSERT_EQ(OB_SUCCESS, main.add_simple_index_info(
          ObAuxTableMetaInfo(index.get_table_id(), index.get_table_type(), index.get_index_type())));
      ASSERT_EQ(OB_SUCCESS, index.set_index_params("FTS_INDEX_TYPE = PHRASE_MATCH"));
      ASSERT_EQ(OB_SUCCESS, candidates.push_back(index.get_table_id()));
      ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&index));
    }
    if (async_fields > 0) {
      // Node creation must also resolve the async delete dependency.
      for (ObDSLFullTextQuery *dsl : queries) {
        ObFullTextQueryNode *node = nullptr;
        const bool mixed = 1 == async_fields && (dsl == dsl_multi || dsl == dsl_string);
        EXPECT_EQ(mixed ? OB_NOT_SUPPORTED : OB_ERR_UNEXPECTED,
            dsl->produce_query_node(main, guard, candidates, alloc_, node));
        EXPECT_EQ(nullptr, node);
      }
      init(deletes, guard.get_next_mocked_schema_id(), main.get_table_id(), INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
      ASSERT_EQ(OB_SUCCESS, main.add_simple_index_info(
          ObAuxTableMetaInfo(deletes.get_table_id(), deletes.get_table_type(), deletes.get_index_type())));
    }
    for (ObDSLFullTextQuery *dsl : queries) {
      ObFullTextQueryNode *node = nullptr;
      if (1 == async_fields && (dsl == dsl_multi || dsl == dsl_string)) {
        EXPECT_EQ(OB_NOT_SUPPORTED, dsl->produce_query_node(main, guard, candidates, alloc_, node));
        EXPECT_EQ(nullptr, node);
        continue;
      }
      ASSERT_EQ(OB_SUCCESS, dsl->produce_query_node(main, guard, candidates, alloc_, node));
      ASSERT_NE(nullptr, node);
      EXPECT_FALSE(node->search_index_fulltext_info_.has_search_index_fulltext());
      EXPECT_TRUE(node->search_index_fulltext_info_.path_.empty());
      EXPECT_EQ(query, node->query_text_);
      EXPECT_EQ(one, node->boost_);
      EXPECT_EQ(async_fields > 0, node->is_async_);
      EXPECT_EQ(async_fields > 0, node->async_delete_info_.has_delete_index());
      if (async_fields > 0) {
        EXPECT_EQ(deletes.get_table_id(), node->async_delete_info_.get_delete_idx_tid());
      }
      if (node->query_type_ == ObFullTextQueryNode::OB_FULLTEXT_QUERY_TYPE_MATCH) {
        EXPECT_EQ(fts[0].get_table_id(), static_cast<ObMatchQueryNode *>(node)->inv_idx_tid_);
        EXPECT_EQ(ObString("title_ft"), node->index_name_);
      } else if (node->query_type_ == ObFullTextQueryNode::OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE) {
        EXPECT_EQ(fts[0].get_table_id(), static_cast<ObMatchPhraseQueryNode *>(node)->inv_idx_tid_);
        EXPECT_EQ(ObString("title_ft"), node->index_name_);
      } else {
        const bool multi = node->query_type_ == ObFullTextQueryNode::OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH;
        const auto &ids = multi ? static_cast<ObMultiMatchQueryNode *>(node)->inv_idx_tids_
                                : static_cast<ObQueryStringQueryNode *>(node)->inv_idx_tids_;
        const auto &names = multi ? static_cast<ObMultiMatchQueryNode *>(node)->index_names_
                                  : static_cast<ObQueryStringQueryNode *>(node)->index_names_;
        ASSERT_EQ(2, names.count());
        EXPECT_EQ(fts[0].get_table_id(), ids.at(0));
        EXPECT_EQ(fts[1].get_table_id(), ids.at(1));
        EXPECT_EQ(ObString("title_ft"), names.at(0));
        EXPECT_EQ(ObString("body_ft"), names.at(1));
      }
    }
  }
}

TEST_F(SearchTextBindingTest, phrase_stops_only_when_positions_are_required)
{
  ObSqlSchemaGuard guard;
  ObTableSchema def2, text2;
  ASSERT_EQ(OB_SUCCESS, def2.assign(def_));
  ASSERT_EQ(OB_SUCCESS, text2.assign(phrase_));
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_EQ(OB_SUCCESS, def2.get_column_schema(42)->set_comment(ObString("SEARCH_INDEX_CONFIG=99:1:")));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, phrase_));
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def2, text2));
  ASSERT_EQ(OB_SUCCESS, resolve(guard, true));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  // A PHRASE fallback does not stop a MATCH query from checking later candidates.
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve(guard, false));
  EXPECT_FALSE(info_.has_search_index_fulltext());
  set_config(def2, OB_FTS_INDEX_TYPE_PHRASE_MATCH,
      "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string", "ngram.1");
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(text2.get_table_id(), selected_text_id_);
  EXPECT_EQ(ObString("ngram.1"), info_.parser_name_);
  EXPECT_EQ(OB_FTS_INDEX_TYPE_PHRASE_MATCH, info_.index_type_);
  ASSERT_EQ(OB_SUCCESS, resolve(guard, true));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  // A later nonmatching candidate must not clear the saved PHRASE fallback.
  set_config(def2, OB_FTS_INDEX_TYPE_PHRASE_MATCH, "INCLUDE_PATHS=6$.body", "ngram.1");
  ASSERT_EQ(OB_SUCCESS, resolve(guard, false));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
}

TEST_F(SearchTextBindingTest, multiple_blocks_bind_the_covering_configuration)
{
  ObSearchIndexConfig config;
  ObSEArray<ObSearchIndexModeConfig, 4> modes;
  ObSearchIndexModeConfig value;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(value));
  ObSearchIndexModeConfig ft;
  ft.mode_ = ObSearchIndexModeConfig::FULLTEXT_MATCH;
  ft.filter_ = "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string";
  ft.parser_name_ = "space.1";
  ft.parser_properties_ = "{}";
  ASSERT_EQ(OB_SUCCESS, modes.push_back(ft));
  ft.filter_ = "INCLUDE_PATHS=6$.bodyINCLUDE_TYPES=11json_string";
  ft.parser_name_ = "ngram.1";
  ft.mode_ = ObSearchIndexModeConfig::FULLTEXT_PHRASE;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(ft));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(encoded.string()));
  ASSERT_EQ(OB_SUCCESS, check(match_));
  EXPECT_EQ(3001, selected_text_id_);
  ASSERT_EQ(OB_SUCCESS, check(phrase_, "$.body", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(3002, selected_text_id_);
  EXPECT_EQ(ObString("ngram.1"), info_.parser_name_);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(phrase_, "$.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
}

TEST_F(SearchTextBindingTest, persisted_parser_and_path_conflicts_are_rejected)
{
  ObSearchIndexModeConfig first;
  first.mode_ = ObSearchIndexModeConfig::FULLTEXT_MATCH;
  first.filter_ = "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string";
  first.parser_name_ = "space.1";
  first.parser_properties_ = "{}";
  ObSearchIndexModeConfig second = first;
  second.filter_ = "INCLUDE_PATHS=7$.otherINCLUDE_TYPES=11json_string";
  second.parser_name_ = "ngram.1";
  second.mode_ = ObSearchIndexModeConfig::FULLTEXT_PHRASE;
  ObSearchIndexConfig config;
  ObSEArray<ObSearchIndexModeConfig, 4> modes;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(first));
  ASSERT_EQ(OB_SUCCESS, modes.push_back(second));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  const std::string wire(encoded.ptr(), encoded.length());
  const size_t parser_pos = wire.find("ngram.1");
  ASSERT_NE(std::string::npos, parser_pos);
  for (const char *parser : {"space.1", "SPACE.1", "space.2"}) {
    SCOPED_TRACE(parser);
    std::string duplicate = wire;
    duplicate.replace(parser_pos, strlen("ngram.1"), parser);
    ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
        ObString(duplicate.size(), duplicate.data())));
    EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_));
    EXPECT_FALSE(info_.has_search_index_fulltext());
  }
  std::string overlap = wire;
  const size_t path_pos = overlap.find("7$.other");
  ASSERT_NE(std::string::npos, path_pos);
  overlap.replace(path_pos, strlen("7$.other"), "7$.title");
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString(overlap.size(), overlap.data())));
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(match_));
  EXPECT_FALSE(info_.has_search_index_fulltext());
}

TEST_F(SearchTextBindingTest, invisible_definition_and_child_are_not_candidates)
{
  ObSqlSchemaGuard guard;
  register_candidate(guard, def_, match_);
  def_.set_index_visibility(1);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false));
  def_.set_index_visibility(0);
  match_.set_index_visibility(1);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false));
  match_.set_index_visibility(0);
  EXPECT_EQ(OB_SUCCESS, resolve(guard, false));
}

TEST_F(SearchTextBindingTest, resolver_fields_and_query_parameters)
{
  ObRawExprFactory factory(alloc_);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObResolverParams params;
  params.allocator_ = &alloc_;
  params.expr_factory_ = &factory;
  params.session_info_ = &session;
  ObDSLQueryInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init_default_params(factory));
  TableItem table(alloc_);
  table.table_id_ = 1001;
  table.dsl_query_ = &info;
  ObStmtFactory statements(alloc_);
  ObSelectStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, statements.create_stmt(stmt));
  ASSERT_EQ(OB_SUCCESS, stmt->set_table_bit_index(table.table_id_));
  add_column(main_, 60, "literal.field", ObVarcharType, 0);
  main_.get_column_schema(60)->set_collation_type(CS_TYPE_GBK_BIN);
  add_column(main_, 61, "doc.a.b", ObVarcharType, 0);
  add_column(main_, 62, "gbk_text", ObVarcharType, 0);
  main_.get_column_schema(62)->set_collation_type(CS_TYPE_GBK_BIN);
  ObDSLResolver resolver(params, &main_, stmt, table);
  ASSERT_EQ(OB_SUCCESS, resolver.init_col_schema_map());
  ObColumnRefRawExpr *column = nullptr;
  ObString path;
  ObString field_name("DOC.Title");
  ASSERT_EQ(OB_SUCCESS, resolver.resolve_field_expr_and_path(field_name, column, path));
  EXPECT_EQ(ObString("doc"), field_name);
  EXPECT_EQ(42, column->get_column_id());
  EXPECT_EQ(ObString("$.Title"), path);
  field_name = ObString("LITERAL.FIELD");
  EXPECT_EQ(OB_HASH_NOT_EXIST, resolver.resolve_field_expr_and_path(field_name, column, path));
  EXPECT_EQ(ObString("LITERAL.FIELD"), field_name);
  field_name = ObString("ID.foo");
  EXPECT_EQ(OB_INVALID_ARGUMENT, resolver.resolve_field_expr_and_path(field_name, column, path));
  EXPECT_EQ(ObString("ID.foo"), field_name);
  for (bool value : {false, true}) {
    const auto resolve_field = [&](const char *name) {
      ObString input(name);
      path.reset();
      int ret = value ? resolver.get_field_expr_and_path(input, column, path)
                      : resolver.resolve_field_expr_and_path(input, column, path);
      if (value || OB_FAIL(ret)) {
        EXPECT_EQ(ObString(name), input);
      }
      return ret;
    };
    ASSERT_EQ(OB_SUCCESS, resolve_field("DOC.a.b"));
    EXPECT_EQ(42, column->get_column_id());
    EXPECT_EQ(ObString("$.a.b"), path);
    ASSERT_EQ(OB_SUCCESS, resolve_field("DOC.a.c"));
    EXPECT_EQ(42, column->get_column_id());
    EXPECT_EQ(ObString("$.a.c"), path);
    ASSERT_EQ(OB_SUCCESS, resolve_field("DOC.Title"));
    EXPECT_EQ(ObString("$.Title"), path);
    ASSERT_EQ(OB_SUCCESS, resolve_field("id"));
    EXPECT_TRUE(path.empty());
    EXPECT_EQ(value ? OB_INVALID_ARGUMENT : OB_HASH_NOT_EXIST, resolve_field("missing"));
    EXPECT_EQ(value ? OB_INVALID_ARGUMENT : OB_HASH_NOT_EXIST, resolve_field("LITERAL.FIELD"));
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve_field(".doc"));
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve_field("doc."));
  }
  ObString retained_path("$.old");
  ASSERT_EQ(OB_SUCCESS, resolver.get_field_expr_and_path(ObString("id"), column, retained_path));
  EXPECT_EQ(ObString("$.old"), retained_path);

  const char *requests[] = {
      R"({"doc.title":"alpha"})",
      R"({"doc.title":{"query":"alpha beta","operator":"and","minimum_should_match":2,"boost":2.0}})",
      R"({"doc.title":{"query":12}})",
      R"({"doc.title":{"boost":2.0}})",
      R"({"doc.*":"alpha"})",
      R"({"missing":"alpha"})",
      R"({"missing.path":"alpha"})"
  };
  const int expected[] = {OB_SUCCESS, OB_SUCCESS, OB_INVALID_ARGUMENT,
      OB_INVALID_ARGUMENT, OB_SUCCESS, OB_ERR_BAD_FIELD_ERROR, OB_ERR_BAD_FIELD_ERROR};
  for (int i = 0; i < ARRAYSIZEOF(requests); ++i) {
    ObJsonNode *json = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_, ObString::make_string(requests[i]), json));
    ObDSLQuery *query = nullptr;
    ASSERT_EQ(expected[i], resolver.resolve_match(*json, query, nullptr, QUERY_ITEM_QUERY)) << requests[i];
    if (OB_SUCCESS == expected[i]) {
      auto *match = static_cast<ObDSLMatchQuery *>(query);
      EXPECT_EQ(ObString(i == 4 ? "$.*" : "$.title"), match->path_);
      EXPECT_EQ(42, match->field_->get_column_id());
      EXPECT_TRUE(match->is_valid());
    }
  }
  ObJsonNode *json = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_,
      ObString(R"({"doc.title":{"query":"alpha beta","slop":2}})"), json));
  ObDSLQuery *query = nullptr;
  ASSERT_EQ(OB_SUCCESS, resolver.resolve_match_phrase(*json, query, nullptr, QUERY_ITEM_QUERY));
  auto *phrase = static_cast<ObDSLMatchPhraseQuery *>(query);
  EXPECT_EQ(2, phrase->slop_->get_value().get_int());
  EXPECT_EQ(ObString("$.title"), phrase->path_);
  json = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_,
      ObString(R"({"missing.path":"alpha"})"), json));
  query = nullptr;
  EXPECT_EQ(OB_ERR_BAD_FIELD_ERROR, resolver.resolve_match_phrase(*json, query, nullptr, QUERY_ITEM_QUERY));
  json = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_,
      ObString(R"({"gbk_text":"数据库"})"), json));
  for (bool need_positions : {false, true}) {
    query = nullptr;
    ASSERT_EQ(OB_SUCCESS, need_positions
        ? resolver.resolve_match_phrase(*json, query, nullptr, QUERY_ITEM_QUERY)
        : resolver.resolve_match(*json, query, nullptr, QUERY_ITEM_QUERY));
    const ObDSLFullTextQuery *fulltext = static_cast<ObDSLFullTextQuery *>(query);
    ObString expected_query;
    ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(alloc_, ObString("数据库"),
        CS_TYPE_UTF8MB4_BIN, CS_TYPE_GBK_BIN, expected_query));
    EXPECT_EQ(CS_TYPE_GBK_BIN, fulltext->query_->get_collation_type());
    EXPECT_EQ(expected_query, static_cast<const ObConstRawExpr *>(fulltext->query_)->get_value().get_string());
  }
}

TEST_F(SearchTextBindingTest, multi_field_paths_survive_resolve_and_copy)
{
  ObRawExprFactory factory(alloc_);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObResolverParams params;
  params.allocator_ = &alloc_;
  params.expr_factory_ = &factory;
  params.session_info_ = &session;
  ObDSLQueryInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init_default_params(factory));
  TableItem table(alloc_);
  table.table_id_ = main_.get_table_id();
  table.dsl_query_ = &info;
  ObStmtFactory statements(alloc_);
  ObSelectStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, statements.create_stmt(stmt));
  ASSERT_EQ(OB_SUCCESS, stmt->set_table_bit_index(table.table_id_));
  ObDSLResolver resolver(params, &main_, stmt, table);
  ASSERT_EQ(OB_SUCCESS, resolver.init_col_schema_map());
  for (bool multi : {false, true}) {
    ObJsonNode *json = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_, ObString(
        R"({"fields":["doc.title^2","doc.body","doc.title^3","doc"],"query":"alpha beta"})"), json));
    ObDSLQuery *query = nullptr;
    ASSERT_EQ(OB_SUCCESS, multi
        ? resolver.resolve_multi_match(*json, query, nullptr, QUERY_ITEM_QUERY)
        : resolver.resolve_query_string(*json, query, nullptr, QUERY_ITEM_QUERY));
    const ObDSLFullTextMultiFieldQueryParam &fields = multi ? static_cast<ObDSLMultiMatchQuery *>(query)->fields_param_
                               : static_cast<ObDSLQueryStringQuery *>(query)->fields_param_;
    ASSERT_EQ(3, fields.field_count());
    EXPECT_EQ(ObString("$.title"), fields.field_paths_.at(0));
    EXPECT_EQ(ObString("$.body"), fields.field_paths_.at(1));
    EXPECT_EQ(ObString("$"), fields.field_paths_.at(2));
    EXPECT_EQ(3.0, fields.field_boosts_.at(0)->get_value().get_double());
    EXPECT_EQ(fields.fields_.at(0), fields.fields_.at(1));
    ObRawExprCopier copier(factory);
    ObArenaAllocator copied_allocator;
    ObDSLFullTextMultiFieldQueryParam copy(copied_allocator);
    ASSERT_EQ(OB_SUCCESS, fields.deep_copy(copier, copied_allocator, copy));
    ASSERT_EQ(3, copy.field_paths_.count());
    for (int64_t i = 0; i < copy.field_paths_.count(); ++i) {
      EXPECT_EQ(fields.field_paths_.at(i), copy.field_paths_.at(i));
      EXPECT_NE(fields.field_paths_.at(i).ptr(), copy.field_paths_.at(i).ptr());
    }
  }
}

TEST_F(SearchTextBindingTest, multi_field_search_binding_preserves_each_path_and_source_kind)
{
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "");
  main_.get_column_schema(42)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  for (int64_t id : {43, 44}) {
    add_column(main_, id, id == 43 ? "title" : "body", ObVarcharType, 0);
    main_.get_column_schema(id)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    add_column(def_, id, id == 43 ? "title" : "body", ObVarcharType, id - 41);
    def_.get_column_schema(id)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(id)->set_comment(def_.get_column_schema(42)->get_comment()));
  }
  ObSqlSchemaGuard guard;
  register_candidate(guard, def_, match_);
  ObTableSchema deletes;
  init(deletes, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
      ObAuxTableMetaInfo(deletes.get_table_id(), deletes.get_table_type(), deletes.get_index_type())));
  ObSEArray<uint64_t, 4> candidates;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(def_.get_table_id()));
  ObRawExprFactory factory(alloc_);
  ObConstRawExpr *one = nullptr;
  ObConstRawExpr *query = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
      ObString("alpha beta"), CS_TYPE_UTF8MB4_BIN, query));
  for (bool multi : {false, true}) {
    // all JSON, all strings, and both orders of mixed source kinds.
    for (int scenario = 0; scenario < 5; ++scenario) {
      SCOPED_TRACE(scenario);
      ObDSLMultiMatchQuery *multi_query = nullptr;
      ObDSLQueryStringQuery *string_query = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObDSLMultiMatchQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, multi_query));
      ASSERT_EQ(OB_SUCCESS, ObDSLQueryStringQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, string_query));
      ObDSLFullTextQuery *dsl = multi ? static_cast<ObDSLFullTextQuery *>(multi_query)
                                    : static_cast<ObDSLFullTextQuery *>(string_query);
      ObDSLFullTextMultiFieldQueryParam &params = multi ? multi_query->fields_param_ : string_query->fields_param_;
      params.minimum_should_match_ = one;
      params.operator_ = one;
      params.field_type_ = one;
      dsl->query_ = query;
      dsl->boost_ = one;
      ASSERT_EQ(OB_SUCCESS, params.fields_.reserve(2));
      ASSERT_EQ(OB_SUCCESS, params.field_paths_.reserve(2));
      ASSERT_EQ(OB_SUCCESS, params.field_boosts_.reserve(2));
      ObColumnRefRawExpr fields[2];
      for (int64_t i = 0; i < 2; ++i) {
        const bool json = 0 == scenario || (2 == scenario && 0 == i) || (3 == scenario && 1 == i);
        fields[i].set_ref_id(main_.get_table_id(), json ? 42 : 43 + i);
        fields[i].set_data_type(json ? ObJsonType : ObVarcharType);
        fields[i].set_collation_type(4 == scenario && 1 == i ? CS_TYPE_UTF8MB4_GENERAL_CI : CS_TYPE_UTF8MB4_BIN);
        ASSERT_EQ(OB_SUCCESS, params.fields_.push_back(&fields[i]));
        ASSERT_EQ(OB_SUCCESS, params.field_paths_.push_back(json ? ObString(0 == i ? "$.title" : "$.body") : ObString()));
        ASSERT_EQ(OB_SUCCESS, params.field_boosts_.push_back(one));
      }
      ObFullTextQueryNode *node = nullptr;
      const int expected = scenario < 2 ? OB_SUCCESS : scenario < 4 ? OB_NOT_SUPPORTED : OB_INVALID_ARGUMENT;
      ASSERT_EQ(expected, dsl->produce_query_node(main_, guard, candidates, alloc_, node));
      if (OB_SUCCESS == expected) {
        ASSERT_NE(nullptr, node);
        EXPECT_TRUE(node->is_async());
        const ObFixedArray<ObString, ObIAllocator> &names = multi ? static_cast<ObMultiMatchQueryNode *>(node)->index_names_
                                 : static_cast<ObQueryStringQueryNode *>(node)->index_names_;
        const ObFixedArray<uint64_t, ObIAllocator> &ids = multi ? static_cast<ObMultiMatchQueryNode *>(node)->inv_idx_tids_
                               : static_cast<ObQueryStringQueryNode *>(node)->inv_idx_tids_;
        ASSERT_EQ(1, names.count());
        EXPECT_EQ(ObString("ft"), names.at(0));
        ASSERT_EQ(2, ids.count());
        EXPECT_EQ(match_.get_table_id(), ids.at(0));
        EXPECT_EQ(match_.get_table_id(), ids.at(1));
        char buf[2048] = {};
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, node->explain_info(buf, sizeof(buf), pos, EXPLAIN_BASIC, 0));
        EXPECT_STREQ(multi ? "\nmulti match node: index name=ft"
                          : "\nquery string node: index name=(ft)", buf);
        ObSEArray<uint64_t, 4> dependencies;
        ASSERT_EQ(OB_SUCCESS, node->get_all_index_ids(dependencies));
        EXPECT_TRUE(has_exist_in_array(dependencies, def_.get_table_id()));
        EXPECT_TRUE(has_exist_in_array(dependencies, match_.get_table_id()));
        ObDASSearchIndexTextInfo execution[2];
        for (int64_t i = 0; i < 2; ++i) {
          const ObSearchIndexTextInfo &bound = multi ? static_cast<ObMultiMatchQueryNode *>(node)->search_index_fulltext_infos_.at(i)
                                   : static_cast<ObQueryStringQueryNode *>(node)->search_index_fulltext_infos_.at(i);
          const ObColumnRefRawExpr *field = multi ? static_cast<ObMultiMatchQueryNode *>(node)->fields_.at(i)
                                   : static_cast<ObQueryStringQueryNode *>(node)->fields_.at(i);
          ASSERT_EQ(OB_SUCCESS, execution[i].assign(*field, bound, alloc_));
          EXPECT_EQ(0 == scenario, execution[i].is_json_);
          EXPECT_EQ(ObString("space.1"), execution[i].parser_name_);
          EXPECT_EQ(def_.get_table_id(), bound.def_table_id_);
          EXPECT_EQ(ObString(0 == scenario ? (0 == i ? "$.title" : "$.body") : "$"), bound.path_);
        }
        if (0 == scenario) {
          EXPECT_EQ(execution[0].column_idx_, execution[1].column_idx_);
          EXPECT_NE(execution[0].encoded_path_, execution[1].encoded_path_);
        } else {
          EXPECT_NE(execution[0].column_idx_, execution[1].column_idx_);
          EXPECT_EQ(execution[0].encoded_path_, execution[1].encoded_path_);
        }
      }
    }
  }
}

TEST_F(SearchTextBindingTest, multi_field_mixed_async_indexes_require_compatible_sources)
{
  ObSqlSchemaGuard guard;
  ObTableSchema fts, deletes;
  main_.get_column_schema(42)->set_data_type(ObVarcharType);
  def_.get_column_schema(42)->set_data_type(ObVarcharType);
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "");
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, match_));
  init(fts, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, fts.set_table_name("__idx_1001_ordinary_ft"));
  add_column(main_, 60, "title", ObVarcharType, 0);
  add_column(main_, 61, "title_word", ObVarcharType, 0);
  add_column(fts, 61, "word", ObVarcharType, 1);
  main_.get_column_schema(61)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ASSERT_EQ(OB_SUCCESS, main_.get_column_schema(61)->add_cascaded_column_id(60));
  fts.get_column_schema(61)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&fts));
  ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
      ObAuxTableMetaInfo(fts.get_table_id(), fts.get_table_type(), fts.get_index_type())));
  init(deletes, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
      ObAuxTableMetaInfo(deletes.get_table_id(), deletes.get_table_type(), deletes.get_index_type())));
  ObSEArray<uint64_t, 2> candidates;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(def_.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(fts.get_table_id()));
  ObRawExprFactory factory(alloc_);
  ObConstRawExpr *one = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  struct Scenario {
    bool sync_;
    bool json_;
    ObCollationType search_coll_;
    ObCollationType ordinary_coll_;
    const char *ordinary_parser_;
    const char *ordinary_properties_;
    int expected_;
  };
  const Scenario scenarios[] = {
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "space.1", "{}", OB_SUCCESS},
    {false, false, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI, "space.1", "{}", OB_SUCCESS},
    {false, false, CS_TYPE_GBK_BIN, CS_TYPE_GBK_BIN, "space.1", "{}", OB_SUCCESS},
    {true, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "space.1", "{}", OB_NOT_SUPPORTED},
    {false, true, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "space.1", "{}", OB_NOT_SUPPORTED},
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_GENERAL_CI, "space.1", "{}", OB_INVALID_ARGUMENT},
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_LATIN1_BIN, "space.1", "{}", OB_INVALID_ARGUMENT},
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "ngram.1", "{}", OB_INVALID_ARGUMENT},
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "space.2", "{}", OB_INVALID_ARGUMENT},
    {false, false, CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_BIN, "space.1", "{\"min_token_size\":2}", OB_INVALID_ARGUMENT},
  };
  for (const Scenario &scenario : scenarios) {
    SCOPED_TRACE(scenario.ordinary_parser_);
    SCOPED_TRACE(scenario.ordinary_properties_);
    SCOPED_TRACE(scenario.search_coll_);
    SCOPED_TRACE(scenario.ordinary_coll_);
    SCOPED_TRACE(scenario.sync_);
    SCOPED_TRACE(scenario.json_);
    fts.set_index_type(scenario.sync_ ? INDEX_TYPE_FTS_INDEX_LOCAL : INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
    // Both index kinds persist complete properties during DDL; this fixture's Search config uses "{}".
    oceanbase::storage::ObFTParserJsonProps props;
    ObString properties;
    ASSERT_EQ(OB_SUCCESS, props.init());
    ASSERT_EQ(OB_SUCCESS, props.parse_from_valid_str(ObString::make_string(scenario.ordinary_properties_)));
    ASSERT_EQ(OB_SUCCESS, props.rebuild_props_for_ddl(ObString("space.1"),
        scenario.ordinary_coll_, false, main_.get_tenant_id()));
    ASSERT_EQ(OB_SUCCESS, props.to_format_json(alloc_, properties));
    ASSERT_EQ(OB_SUCCESS, fts.set_parser_name_and_properties(
        ObString::make_string(scenario.ordinary_parser_), properties));
    main_.get_column_schema(42)->set_data_type(scenario.json_ ? ObJsonType : ObVarcharType);
    def_.get_column_schema(42)->set_data_type(scenario.json_ ? ObJsonType : ObVarcharType);
    main_.get_column_schema(42)->set_collation_type(scenario.search_coll_);
    def_.get_column_schema(42)->set_collation_type(scenario.search_coll_);
    main_.get_column_schema(60)->set_collation_type(scenario.ordinary_coll_);
    ObColumnRefRawExpr search_field, ordinary_field;
    search_field.set_ref_id(main_.get_table_id(), 42);
    search_field.set_data_type(scenario.json_ ? ObJsonType : ObVarcharType);
    search_field.set_collation_type(scenario.search_coll_);
    ordinary_field.set_ref_id(main_.get_table_id(), 60);
    ordinary_field.set_data_type(ObVarcharType);
    ordinary_field.set_collation_type(scenario.ordinary_coll_);
    for (bool multi : {false, true}) {
      for (bool search_first : {false, true}) {
        SCOPED_TRACE(multi);
        SCOPED_TRACE(search_first);
        ObDSLMultiMatchQuery *multi_query = nullptr;
        ObDSLQueryStringQuery *string_query = nullptr;
        ASSERT_EQ(OB_SUCCESS, ObDSLMultiMatchQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, multi_query));
        ASSERT_EQ(OB_SUCCESS, ObDSLQueryStringQuery::create(QUERY_ITEM_QUERY, nullptr, alloc_, string_query));
        ObDSLFullTextQuery *dsl = multi ? static_cast<ObDSLFullTextQuery *>(multi_query)
                                      : static_cast<ObDSLFullTextQuery *>(string_query);
        ObDSLFullTextMultiFieldQueryParam &params = multi ? multi_query->fields_param_ : string_query->fields_param_;
        params.minimum_should_match_ = one;
        params.operator_ = one;
        params.field_type_ = one;
        ObConstRawExpr *query = nullptr;
        ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
            ObString("alpha beta"), search_first ? scenario.search_coll_ : scenario.ordinary_coll_, query));
        dsl->query_ = query;
        dsl->boost_ = one;
        ASSERT_EQ(OB_SUCCESS, params.fields_.reserve(2));
        ASSERT_EQ(OB_SUCCESS, params.field_paths_.reserve(2));
        ASSERT_EQ(OB_SUCCESS, params.field_boosts_.reserve(2));
        for (int64_t i = 0; i < 2; ++i) {
          const bool search = (0 == i) == search_first;
          ASSERT_EQ(OB_SUCCESS, params.fields_.push_back(search ? &search_field : &ordinary_field));
          ASSERT_EQ(OB_SUCCESS, params.field_paths_.push_back(search && scenario.json_ ? ObString("$.title") : ObString()));
          ASSERT_EQ(OB_SUCCESS, params.field_boosts_.push_back(one));
        }
        ObFullTextQueryNode *node = nullptr;
        ASSERT_EQ(scenario.expected_, dsl->produce_query_node(main_, guard, candidates, alloc_, node));
        if (OB_SUCCESS == scenario.expected_) {
          ASSERT_NE(nullptr, node);
          EXPECT_TRUE(node->is_async());
          ObSEArray<uint64_t, 4> dependencies;
          ASSERT_EQ(OB_SUCCESS, node->get_all_index_ids(dependencies));
          EXPECT_TRUE(has_exist_in_array(dependencies, def_.get_table_id()));
          EXPECT_TRUE(has_exist_in_array(dependencies, match_.get_table_id()));
          EXPECT_TRUE(has_exist_in_array(dependencies, fts.get_table_id()));
          EXPECT_TRUE(has_exist_in_array(dependencies, deletes.get_table_id()));
          for (int64_t i = 0; i < 2; ++i) {
            const bool search = (0 == i) == search_first;
            const ObSearchIndexTextInfo &info = multi ? static_cast<ObMultiMatchQueryNode *>(node)->search_index_fulltext_infos_.at(i)
                                    : static_cast<ObQueryStringQueryNode *>(node)->search_index_fulltext_infos_.at(i);
            EXPECT_EQ(search, info.has_search_index_fulltext());
            const ObColumnRefRawExpr *field = multi ? static_cast<ObMultiMatchQueryNode *>(node)->fields_.at(i)
                                     : static_cast<ObQueryStringQueryNode *>(node)->fields_.at(i);
            EXPECT_EQ(search ? &search_field : &ordinary_field, field);
            const ObFixedArray<uint64_t, ObIAllocator> &ids = multi ? static_cast<ObMultiMatchQueryNode *>(node)->inv_idx_tids_
                                    : static_cast<ObQueryStringQueryNode *>(node)->inv_idx_tids_;
            EXPECT_EQ(search ? match_.get_table_id() : fts.get_table_id(), ids.at(i));
          }
        } else {
          EXPECT_EQ(nullptr, node);
        }
      }
    }
  }
}

TEST_F(SearchTextBindingTest, access_expressions_use_physical_column_identity)
{
  // Schema reload must preserve binding and expression roles without FTS flags.
  for (ObTableSchema *schema : {&match_, &phrase_}) {
    const int64_t size = schema->get_serialize_size();
    char *buffer = static_cast<char *>(alloc_.alloc(size));
    ASSERT_NE(nullptr, buffer);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, schema->serialize(buffer, size, pos));
    ObTableSchema restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(buffer, size, pos));
    ASSERT_EQ(OB_SUCCESS, schema->assign(restored));
    for (int64_t i = 0; i < schema->get_column_count(); ++i) {
      EXPECT_EQ(0, schema->get_column_schema_by_idx(i)->get_column_flags());
    }
  }
  ASSERT_EQ(OB_SUCCESS, check(match_));
  EXPECT_EQ(match_.get_table_id(), selected_text_id_);
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_EQ(OB_SUCCESS, check(phrase_, "$.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  ObRawExprFactory factory(alloc_);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  TableItem table(alloc_);
  table.table_id_ = main_.get_table_id();
  table.database_name_ = "test";
  ObSqlSchemaGuard guard;
  // The guard's in-memory schema facility avoids a live schema service while
  // retaining the production lookup, scan-ID allocation and expression path.
  match_.set_table_id(guard.get_next_mocked_schema_id());
  phrase_.set_table_id(guard.get_next_mocked_schema_id());
  ASSERT_EQ(OB_SUCCESS, guard.add_mocked_table_schema(match_));
  ASSERT_EQ(OB_SUCCESS, guard.add_mocked_table_schema(phrase_));
  ObQueryCtx query_ctx(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  // BM25 inserts casts, whose type deduction requires a session execution context.
  ObExecContext exec_ctx(alloc_);
  exec_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&exec_ctx);
  auto unregister_exec_ctx = MakeScopedLambda([&]() { session.set_cur_exec_ctx(nullptr); });
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, nullptr, false, &query_ctx);
  ObTextRetrievalIndexInfo match, phrase, second_match;
  ASSERT_EQ(OB_SUCCESS, match.generate_exprs(match_.get_table_id(), true, table, opt_ctx, true));
  ASSERT_EQ(OB_SUCCESS, phrase.generate_exprs(phrase_.get_table_id(), true, table, opt_ctx, true));
  TableItem alias(alloc_);
  alias.table_id_ = query_ctx.available_tb_id_--;
  alias.database_name_ = table.database_name_;
  ASSERT_EQ(OB_SUCCESS, second_match.generate_exprs(match_.get_table_id(), true, alias, opt_ctx, true));
  EXPECT_EQ(90, match.token_column_->get_column_id());
  EXPECT_EQ(100, match.domain_id_column_->get_column_id());
  EXPECT_EQ(110, match.token_cnt_column_->get_column_id());
  EXPECT_EQ(120, match.doc_length_column_->get_column_id());
  EXPECT_EQ(nullptr, match.pos_list_column_);
  ASSERT_NE(nullptr, phrase.pos_list_column_);
  EXPECT_EQ(130, phrase.pos_list_column_->get_column_id());
  EXPECT_NE(table.table_id_, match.token_column_->get_table_id());
  EXPECT_NE(alias.table_id_, second_match.token_column_->get_table_id());
  EXPECT_NE(match_.get_table_id(), match.token_column_->get_table_id());
  EXPECT_NE(phrase_.get_table_id(), phrase.token_column_->get_table_id());
  EXPECT_FALSE(match.token_column_->same_as(*phrase.token_column_));
  EXPECT_FALSE(match.token_column_->same_as(*second_match.token_column_));
  EXPECT_FALSE(phrase.token_column_->same_as(*second_match.token_column_));
  // Reusing an already-generated retrieval must preserve its expression identity.
  ObColumnRefRawExpr *token = match.token_column_;
  const uint64_t next_table_id = query_ctx.available_tb_id_;
  ASSERT_EQ(OB_SUCCESS, match.generate_exprs(match_.get_table_id(), true, table, opt_ctx, true));
  EXPECT_EQ(token, match.token_column_);
  EXPECT_EQ(next_table_id, query_ctx.available_tb_id_);
}

TEST_F(SearchTextBindingTest, missing_members_do_not_match_a_later_path_component)
{
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.a.title"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.title.a.title"));
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "INCLUDE_PATHS=5$.a.b");
  EXPECT_EQ(OB_SUCCESS, check(match_, "$.a.b"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.a.missing.b"));
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "EXCLUDE_PATHS=7$.title");
  EXPECT_EQ(OB_SUCCESS, check(match_, "$.a.title"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.title"));
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "INCLUDE_PATHS=5$.a.*");
  EXPECT_EQ(OB_SUCCESS, check(match_, "$.a.title"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.a"));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, check(match_, "$.missing.a.title"));
}

TEST_F(SearchTextBindingTest, rejects_empty_parser_in_fulltext_config)
{
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:2:0:0:0:")));
  EXPECT_EQ(OB_INVALID_ARGUMENT, check(phrase_, "$", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
}

TEST_F(SearchTextBindingTest, final_v1_defaults_and_other_column_nested)
{
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:2:0:7:space.10:")));
  ASSERT_EQ(OB_SUCCESS, check(phrase_, "$", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(3002, selected_text_id_);
  EXPECT_EQ(ObString("space.1"), info_.parser_name_);
  const std::string defaults(info_.parser_properties_.ptr(), info_.parser_properties_.length());
  add_column(main_, 43, "other", ObJsonType, 0);
  add_column(def_, 43, "other", ObJsonType, 2);
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(43)->set_comment(ObString("NESTED_PATHS=7$.items")));
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH, "", "space.1", "");
  ASSERT_EQ(OB_SUCCESS, check(phrase_, "$.items.title", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
  EXPECT_EQ(defaults, std::string(info_.parser_properties_.ptr(), info_.parser_properties_.length()));
}

TEST_F(SearchTextBindingTest, effective_defaults_match_explicit_and_partial_properties)
{
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "", "space.1", "");
  ASSERT_EQ(OB_SUCCESS, check(match_));
  const ObString defaults = info_.parser_properties_;
  for (const char *properties : {"", "{}", "{\"max_token_size\":84}",
      "{\"max_token_size\":84,\"min_token_size\":3}",
      "{\"min_token_size\":3,\"max_token_size\":84}"}) {
    SCOPED_TRACE(properties);
    set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "", "SPACE.1", properties);
    ASSERT_EQ(OB_SUCCESS, check(match_));
    EXPECT_EQ(ObString("space.1"), info_.parser_name_);
    EXPECT_EQ(defaults, info_.parser_properties_);
    ASSERT_NO_FATAL_FAILURE(expect_space_properties(info_));
  }
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "", "space.1", "{\"max_token_size\":12}");
  ASSERT_EQ(OB_SUCCESS, check(match_));
  ASSERT_NO_FATAL_FAILURE(expect_space_properties(info_, 12));
}

TEST_F(SearchTextBindingTest, shared_def_candidates_expose_only_value_columns)
{
  add_column(main_, 43, "price", ObIntType, 0);
  add_column(def_, 43, "price", ObIntType, 2);
  ObColumnSchemaV2 primary_key;
  primary_key.set_tenant_id(def_.get_tenant_id());
  primary_key.set_table_id(def_.get_table_id());
  primary_key.set_column_id(16);
  primary_key.set_rowkey_position(3);
  primary_key.set_data_type(ObUInt64Type);
  ASSERT_EQ(OB_SUCCESS, primary_key.set_column_name("id"));
  ASSERT_EQ(OB_SUCCESS, def_.add_column(primary_key));
  def_.set_rowkey_column_num(3);

  ObSqlSchemaGuard guard;
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, match_));
  ObSQLSessionInfo session;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObSelectStmt stmt(alloc_);
  ObOptimizerContext opt_ctx(&session, nullptr, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  TableItem table(alloc_);
  table.table_id_ = main_.get_table_id();
  table.ref_id_ = main_.get_table_id();
  ObIndexMergeHint merge_hint(alloc_);
  LogTableHint table_hint(alloc_);
  table_hint.table_ = &table;
  table_hint.hybrid_search_index_merge_hint_ = &merge_hint;
  ASSERT_EQ(OB_SUCCESS, table_hint.hybrid_search_index_merge_index_ids_.push_back(def_.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, plan.log_plan_hint_.table_hints_.reserve(1));
  ASSERT_EQ(OB_SUCCESS, plan.log_plan_hint_.table_hints_.push_back(table_hint));
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObSEArray<uint64_t, 4> ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> columns;
  ObSEArray<bool, 4> ignore_prefix;

  for (bool has_value : {false, true}) {
    SCOPED_TRACE(has_value);
    if (has_value) {
      ObSearchIndexConfig config;
      ASSERT_EQ(OB_SUCCESS, config.decode(def_.get_column_schema(42)->get_comment_str()));
      ObSEArray<ObSearchIndexModeConfig, 2> modes;
      ASSERT_EQ(OB_SUCCESS, modes.assign(config.get_modes()));
      ASSERT_EQ(OB_SUCCESS, modes.push_back(ObSearchIndexModeConfig()));
      ASSERT_EQ(OB_SUCCESS, config.init(modes));
      ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(config.get_comment()));
    }
    ASSERT_EQ(OB_SUCCESS, join_order.get_valid_index_merge_indexes(table.table_id_, table.ref_id_,
        ids, columns, ignore_prefix, true /* use_force_hint */, true /* is_hybrid_search */));
    ASSERT_EQ(1, ids.count());
    EXPECT_EQ(def_.get_table_id(), ids.at(0));
    ASSERT_EQ(1, columns.count());
    ASSERT_EQ(has_value ? 2 : 1, columns.at(0).count());
    ASSERT_EQ(1, ignore_prefix.count());
    EXPECT_TRUE(ignore_prefix.at(0));
    ObHybridSearchGenerator generator(&alloc_, &plan, &guard, &main_, ids, columns,
        ignore_prefix, &table, false, nullptr);
    for (uint64_t column_id : {42, 43, 16}) {
      SCOPED_TRACE(column_id);
      ObColumnRefRawExpr field(table.table_id_, column_id, T_REF_COLUMN);
      ObObj value;
      value.set_int(1);
      ObConstRawExpr constant(value, T_INT);
      ObOpRawExpr filter(alloc_);
      filter.set_expr_type(T_OP_EQ);
      ASSERT_EQ(OB_SUCCESS, filter.set_param_exprs(&field, &constant));
      bool matched = false;
      uint64_t target_id = OB_INVALID_ID;
      ASSERT_EQ(OB_SUCCESS, generator.check_filter_has_index(&filter, matched, target_id));
      EXPECT_EQ(column_id == 43 || (column_id == 42 && has_value), matched);
      if (matched) {
        EXPECT_EQ(def_.get_table_id(), target_id);
      }
    }
    ASSERT_EQ(OB_SUCCESS, resolve(guard, false, &ids));
    EXPECT_EQ(match_.get_table_id(), selected_text_id_);
  }
  ASSERT_EQ(OB_SUCCESS, def_.get_column_schema(42)->set_comment(
      ObString("SEARCH_INDEX_CONFIG=2:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_NOT_SUPPORTED, join_order.get_valid_index_merge_indexes(table.table_id_, table.ref_id_,
      ids, columns, ignore_prefix, true /* use_force_hint */, true /* is_hybrid_search */));
}

TEST_F(SearchTextBindingTest, production_dsl_to_optimizer_path_preserves_if05)
{
  ObRawExprFactory factory(alloc_);
  // TableParam read-info uses the system tenant's real data-version lookup.
  const int version_ret = ODV_MGR.init(true);
  ASSERT_TRUE(OB_SUCCESS == version_ret || OB_INIT_TWICE == version_ret);
  uint64_t tenant_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, ODV_MGR.get(OB_SYS_TENANT_ID, tenant_data_version));
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  const uint64_t saved_tenant = oceanbase::share::ObTenantEnv::get_tenant_local()->id_;
  oceanbase::share::ObTenantEnv::get_tenant_local()->id_ = OB_SYS_TENANT_ID;
  auto restore_tenant = MakeScopedLambda([&]() {
    oceanbase::share::ObTenantEnv::get_tenant_local()->id_ = saved_tenant;
  });
  const int tenant_ret = oceanbase::omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == tenant_ret || OB_ENTRY_EXIST == tenant_ret);
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ObExecContext exec_ctx(alloc_);
  exec_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&exec_ctx);
  auto unregister_exec_ctx = MakeScopedLambda([&]() { session.set_cur_exec_ctx(nullptr); });
  MockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  ObSchemaGetterGuard schema_guard;
  ObSqlSchemaGuard guard;
  guard.set_schema_guard(&schema_guard);
  // Codegen also requests schema versions, which do not use SQL's mocked schema list.
  main_.set_table_id(500001);
  def_.set_table_id(500002);
  def_.set_data_table_id(main_.get_table_id());
  match_.set_table_id(500003);
  match_.set_data_table_id(def_.get_table_id());
  phrase_.set_table_id(500004);
  phrase_.set_data_table_id(def_.get_table_id());
  ObTableSchema deletes;
  init(deletes, 500005, main_.get_table_id(), INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, def_.set_table_name("__idx_500001_ft"));
  ASSERT_EQ(OB_SUCCESS, match_.set_table_name("__idx_500002_match"));
  ASSERT_EQ(OB_SUCCESS, phrase_.set_table_name("__idx_500002_phrase"));
  ASSERT_EQ(OB_SUCCESS, deletes.set_table_name("__idx_500001_delete"));
  add_column(deletes, 16, "id", ObUInt64Type, 1);
  for (ObTableSchema *child : {&match_, &phrase_}) {
    ASSERT_EQ(OB_SUCCESS, def_.add_simple_index_info(
        ObAuxTableMetaInfo(child->get_table_id(), child->get_table_type(), child->get_index_type())));
  }
  for (ObTableSchema *child : {&def_, &deletes}) {
    ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
        ObAuxTableMetaInfo(child->get_table_id(), child->get_table_type(), child->get_index_type())));
  }
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH, "", "space.1", "{\"max_token_size\":12}");
  for (ObTableSchema *schema : {&main_, &def_, &match_, &phrase_, &deletes}) {
    for (int64_t i = 0; i < schema->get_column_count(); ++i) {
      schema->get_column_schema_by_idx(i)->set_table_id(schema->get_table_id());
    }
    ASSERT_EQ(OB_SUCCESS, schema_service.add_table_schema(*schema, schema->get_schema_version()));
  }
  ASSERT_EQ(OB_SUCCESS, schema_service.get_schema_guard(schema_guard));
  int64_t text_schema_version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema_version(phrase_.get_table_id(), text_schema_version));
  ASSERT_EQ(phrase_.get_schema_version(), text_schema_version);
  ObQueryCtx query_ctx(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, nullptr, false, &query_ctx);
  ObStmtFactory statements(alloc_);
  ObSelectStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, statements.create_stmt(stmt));
  stmt->set_query_ctx(&query_ctx);
  opt_ctx.set_root_stmt(stmt);
  TableItem table(alloc_);
  table.table_id_ = main_.get_table_id();
  table.ref_id_ = main_.get_table_id();
  table.table_name_ = "t";
  table.database_name_ = "test";
  table.type_ = TableItem::BASE_TABLE;
  ObDSLQueryInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init_default_params(factory));
  table.dsl_query_ = &info;
  ASSERT_EQ(OB_SUCCESS, stmt->add_table_item(&session, &table));
  ObSchemaChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(guard));
  ObResolverParams params;
  params.allocator_ = &alloc_;
  params.expr_factory_ = &factory;
  params.session_info_ = &session;
  params.schema_checker_ = &checker;
  ObDSLResolver resolver(params, &main_, stmt, table);
  ASSERT_EQ(OB_SUCCESS, resolver.init_col_schema_map());
  ObJsonNode *json = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_, ObString(R"({"bool":{
    "must":[{"match":{"doc.Title":{"query":"alpha beta","operator":"and","minimum_should_match":2,"boost":2.0}}}],
    "filter":[{"match_phrase":{"doc.Title":{"query":"alpha beta","slop":3}}}]
  }})"), json));
  ASSERT_EQ(OB_SUCCESS, resolver.resolve_query(*json));
  ASSERT_EQ(1, info.queries_.count());
  ObSelectLogPlan plan(opt_ctx, stmt);
  ObSEArray<uint64_t, 4> ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> columns;
  ObSEArray<bool, 4> ignore_prefix;
  // A table may own Text indexes, but an empty effective candidate set forbids them.
  {
    ObHybridSearchGenerator rejected_generator(&alloc_, &plan, &guard, &main_, ids, columns,
        ignore_prefix, &table, false, nullptr);
    ObIndexMergeNode *rejected_root = nullptr;
    EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, rejected_generator.generate(&info, rejected_root));
  }
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObTablePartitionInfo part_info(alloc_);
  part_info.get_table_location().get_loc_meta().table_loc_id_ = table.table_id_;
  part_info.get_table_location().get_loc_meta().ref_table_id_ = table.ref_id_;
  join_order.table_partition_info_ = &part_info;
  ObJoinOrder::PathHelper helper;
  bool valid = false;
  ObIndexMergeHint merge_hint(alloc_);
  LogTableHint table_hint(alloc_);
  table_hint.table_ = &table;
  table_hint.hybrid_search_index_merge_hint_ = &merge_hint;
  ASSERT_EQ(OB_SUCCESS, table_hint.hybrid_search_index_merge_index_ids_.push_back(main_.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, table_hint.hybrid_search_index_merge_index_ids_.push_back(def_.get_table_id()));
  ASSERT_EQ(OB_SUCCESS, plan.log_plan_hint_.table_hints_.reserve(1));
  ASSERT_EQ(OB_SUCCESS, plan.log_plan_hint_.table_hints_.push_back(table_hint));
  ASSERT_EQ(OB_SUCCESS, join_order.get_valid_index_merge_indexes(table.table_id_, table.ref_id_,
      ids, columns, ignore_prefix, true /* use_force_hint */, true /* is_hybrid_search */));
  ASSERT_EQ(2, ids.count());
  EXPECT_EQ(def_.get_table_id(), ids.at(1));
  ASSERT_EQ(2, columns.count());
  EXPECT_TRUE(columns.at(1).empty()); // FULLTEXT-only Def is retained for Text binding.
  ObHybridSearchGenerator generator(&alloc_, &plan, &guard, &main_, ids, columns,
      ignore_prefix, &table, false, nullptr);
  ObIndexMergeNode *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, generator.generate(&info, root));
  ASSERT_NE(nullptr, root);
  auto *logical_fusion = static_cast<ObFusionNode *>(root);
  ASSERT_EQ(1, logical_fusion->children_.count());
  auto *logical_bool = static_cast<ObBooleanQueryNode *>(logical_fusion->children_.at(0));
  auto *logical_match = static_cast<ObMatchQueryNode *>(logical_bool->must_nodes_.at(0));
  auto *logical_phrase = static_cast<ObMatchPhraseQueryNode *>(logical_bool->filter_nodes_.at(0));
  for (ObFullTextQueryNode *node : {static_cast<ObFullTextQueryNode *>(logical_match),
                                  static_cast<ObFullTextQueryNode *>(logical_phrase)}) {
    EXPECT_TRUE(node->search_index_fulltext_info_.has_search_index_fulltext());
    EXPECT_TRUE(node->async_delete_info_.has_delete_index());
    EXPECT_EQ(ObString("$.Title"), node->search_index_fulltext_info_.path_);
  }
  EXPECT_EQ(phrase_.get_table_id(), logical_match->inv_idx_tid_);
  EXPECT_EQ(phrase_.get_table_id(), logical_phrase->inv_idx_tid_);
  ASSERT_EQ(OB_SUCCESS, join_order.create_hybrid_search_node_path(table.table_id_, table.ref_id_,
      helper, ids, columns, ignore_prefix, root, valid));
  ASSERT_TRUE(valid);
  EXPECT_TRUE(logical_match->search_index_fulltext_info_.has_search_index_fulltext());
  EXPECT_TRUE(logical_phrase->search_index_fulltext_info_.has_search_index_fulltext());
  EXPECT_TRUE(logical_match->async_delete_info_.has_delete_index());
  EXPECT_TRUE(logical_phrase->async_delete_info_.has_delete_index());
  ASSERT_EQ(OB_SUCCESS, generator.move_bool_nodes_to_children(root));
  // A remaining SQL predicate must survive path construction alongside the DSL bool/filter.
  ObDSLQuery *scalar = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&alloc_, ObString(R"({"term":{"id":7}})"), json));
  ASSERT_EQ(OB_SUCCESS, resolver.resolve_single_term(*json, scalar, nullptr, QUERY_ITEM_QUERY));
  ObRawExpr *residual = static_cast<ObDSLScalarQuery *>(scalar)->scalar_expr_;
  ASSERT_NE(nullptr, residual);
  ObSEArray<ObRawExpr *, 4> residuals;
  ASSERT_EQ(OB_SUCCESS, residuals.push_back(residual));
  ObSEArray<AccessPath *, 4> paths;
  ASSERT_EQ(OB_SUCCESS, join_order.do_create_index_merge_path(table.table_id_, table.ref_id_,
      helper, residuals, root, paths));
  ASSERT_EQ(1, paths.count());
  ASSERT_TRUE(paths.at(0)->is_index_merge_path());
  IndexMergePath *path = static_cast<IndexMergePath *>(paths.at(0));
  ASSERT_EQ(1, path->filter_.count());
  EXPECT_EQ(residual, path->filter_.at(0));
  EXPECT_EQ(root, path->root_);
  ObLogTableScan scan(plan);
  scan.set_table_id(table.table_id_);
  scan.set_ref_table_id(table.ref_id_);
  scan.set_table_type(main_.get_table_type());
  scan.set_access_path(path);
  ASSERT_EQ(OB_SUCCESS, scan.collect_hybrid_search_exprs(path));
  ObFusionNode *fusion = static_cast<ObFusionNode *>(path->root_);
  ASSERT_EQ(1, fusion->children_.count());
  ObBooleanQueryNode *boolean = static_cast<ObBooleanQueryNode *>(fusion->children_.at(0));
  ASSERT_EQ(1, boolean->must_nodes_.count());
  ASSERT_EQ(1, boolean->filter_nodes_.count());
  ObMatchQueryNode *match = static_cast<ObMatchQueryNode *>(boolean->must_nodes_.at(0));
  ObMatchPhraseQueryNode *phrase = static_cast<ObMatchPhraseQueryNode *>(boolean->filter_nodes_.at(0));
  for (ObFullTextQueryNode *node : {static_cast<ObFullTextQueryNode *>(match),
                                  static_cast<ObFullTextQueryNode *>(phrase)}) {
    const ObSearchIndexTextInfo &bound = node->search_index_fulltext_info_;
    EXPECT_EQ(main_.get_table_id(), bound.main_table_id_);
    EXPECT_EQ(def_.get_table_id(), bound.def_table_id_);
    int64_t store_idx = -1;
    ASSERT_EQ(OB_SUCCESS, main_.get_store_column_idx(42, store_idx));
    EXPECT_EQ(store_idx, bound.column_idx_);
    EXPECT_EQ(ObString("$.Title"), node->search_index_fulltext_info_.path_);
    EXPECT_EQ(ObString("space.1"), bound.parser_name_);
    ASSERT_NO_FATAL_FAILURE(expect_space_properties(bound, 12));
    EXPECT_EQ(OB_FTS_INDEX_TYPE_PHRASE_MATCH, bound.index_type_);
    EXPECT_TRUE(node->async_delete_info_.is_valid());
    EXPECT_EQ(deletes.get_table_id(), node->async_delete_info_.get_delete_idx_tid());
    EXPECT_EQ(ObString("alpha beta"), static_cast<ObConstRawExpr *>(node->query_text_)->get_value().get_string());
  }
  EXPECT_EQ(ObFullTextQueryNode::OB_FULLTEXT_QUERY_TYPE_MATCH, match->get_query_type());
  EXPECT_EQ(phrase_.get_table_id(), match->inv_idx_tid_);
  EXPECT_EQ(phrase_.get_table_id(), phrase->inv_idx_tid_);
  EXPECT_EQ(42, match->field_->get_column_id());
  EXPECT_EQ(42, phrase->field_->get_column_id());
  EXPECT_EQ(2, match->minimum_should_match_->get_value().get_int());
  EXPECT_EQ(MATCH_OPERATOR_AND, match->operator_->get_value().get_int());
  EXPECT_EQ(2.0, static_cast<ObConstRawExpr *>(match->boost_)->get_value().get_double());
  EXPECT_EQ(3, phrase->slop_->get_value().get_int());
  EXPECT_FALSE(phrase->is_scoring_);
  EXPECT_EQ(90, match->index_info_.token_column_->get_column_id());
  EXPECT_EQ(130, phrase->index_info_.pos_list_column_->get_column_id());
  EXPECT_NE(match->index_info_.token_column_->get_table_id(), phrase_.get_table_id());
  EXPECT_FALSE(match->index_info_.token_column_->same_as(*phrase->index_info_.token_column_));
  ids.reuse();
  ASSERT_EQ(OB_SUCCESS, path->get_all_index_ids(ids));
  EXPECT_TRUE(has_exist_in_array(ids, def_.get_table_id()));
  EXPECT_TRUE(has_exist_in_array(ids, phrase_.get_table_id()));
  EXPECT_TRUE(has_exist_in_array(ids, deletes.get_table_id()));

  // Exercise the Codegen builders and scoring/filtering entries. Runtime
  // expression slots stand in for the earlier expression-frame generation phase.
  ObColumnRefRawExpr *primary_key = nullptr;
  ObString primary_path;
  ObString primary_name("id");
  ASSERT_EQ(OB_SUCCESS, resolver.resolve_field_expr_and_path(primary_name, primary_key, primary_path));
  ASSERT_EQ(OB_SUCCESS, scan.rowkey_exprs_.push_back(primary_key));
  auto prepare_expr = [&](auto &&self, ObRawExpr *raw) -> void {
    if (nullptr != raw && nullptr == raw->rt_expr_) {
      ObExpr *expr = OB_NEWx(ObExpr, &alloc_);
      ASSERT_NE(nullptr, expr);
      expr->type_ = raw->get_expr_type();
      expr->obj_meta_ = raw->get_result_type();
      raw->set_rt_expr(expr);
      for (int64_t i = 0; i < raw->get_param_count(); ++i) {
        self(self, raw->get_param_expr(i));
      }
    }
  };
  prepare_expr(prepare_expr, primary_key);
  for (ObFullTextQueryNode *node : {static_cast<ObFullTextQueryNode *>(match),
                                  static_cast<ObFullTextQueryNode *>(phrase)}) {
    ObSEArray<ObRawExpr *, 16> query_exprs, score_exprs;
    ASSERT_EQ(OB_SUCCESS, node->collect_exprs(query_exprs, score_exprs));
    for (auto *expr : query_exprs) { prepare_expr(prepare_expr, expr); }
    for (auto *expr : score_exprs) { prepare_expr(prepare_expr, expr); }
  }
  ObPhysicalPlan physical_plan;
  ObStaticEngineCG cg(CLUSTER_VERSION_4_1_0_0);
  cg.phy_plan_ = &physical_plan;
  cg.opt_ctx_ = &opt_ctx;
  ObHybridSearchCgService service(cg);
  ObDASMatchCtDef *match_def = nullptr;
  ObDASMatchPhraseCtDef *phrase_def = nullptr;
  ASSERT_EQ(OB_SUCCESS, service.generate_fulltext_query_ctdef(scan, match, match_def));
  ASSERT_EQ(OB_SUCCESS, service.generate_match_phrase_query_ctdef(scan, phrase, phrase_def));
  ASSERT_NE(nullptr, match_def);
  ASSERT_NE(nullptr, phrase_def);
  EXPECT_EQ(match->query_text_->rt_expr_, match_def->query_text_);
  EXPECT_EQ(match->boost_->rt_expr_, match_def->boost_);
  EXPECT_EQ(match->minimum_should_match_->rt_expr_, match_def->minimum_should_match_);
  EXPECT_EQ(match->operator_->rt_expr_, match_def->match_operator_);
  EXPECT_EQ(phrase->slop_->rt_expr_, phrase_def->slop_);
  for (auto *ir : {match_def->get_ir_ctdef(), phrase_def->get_ir_ctdef()}) {
    ASSERT_NE(nullptr, ir);
    EXPECT_TRUE(ir->is_async_index_);
    EXPECT_FALSE(ir->has_inv_agg_);
    EXPECT_FALSE(ir->has_block_max_scan_);
    EXPECT_EQ(1, ir->children_cnt_);
    EXPECT_EQ(phrase_.get_table_id(), ir->get_inv_idx_scan_scalar_ctdef()->ref_table_id_);
    EXPECT_TRUE(ir->search_index_fulltext_info_.has_search_index_fulltext());
    EXPECT_EQ(match->search_index_fulltext_info_.column_idx_, ir->search_index_fulltext_info_.column_idx_);
    EXPECT_EQ(ObString("space.1"), ir->search_index_fulltext_info_.parser_name_);
    EXPECT_EQ(match->search_index_fulltext_info_.parser_properties_, ir->search_index_fulltext_info_.parser_properties_);
    EXPECT_NE(match->search_index_fulltext_info_.parser_properties_.ptr(),
              ir->search_index_fulltext_info_.parser_properties_.ptr());
  }
  EXPECT_TRUE(match_def->get_ir_ctdef()->need_calc_relevance());
  EXPECT_TRUE(phrase_def->get_ir_ctdef()->need_calc_relevance());
  EXPECT_NE(nullptr, phrase_def->get_ir_ctdef()->inv_scan_pos_list_col_);
  ObDASBaseCtDef *generated = nullptr;
  ASSERT_EQ(OB_SUCCESS, service.generate_ctdef(scan, match, generated));
  ASSERT_NE(nullptr, generated);
  EXPECT_TRUE(static_cast<ObDASMatchCtDef *>(generated)->is_scoring());
  EXPECT_TRUE(static_cast<ObDASMatchCtDef *>(generated)->get_ir_ctdef()->need_calc_relevance());
  ASSERT_EQ(OB_SUCCESS, service.generate_ctdef(scan, phrase, generated));
  ASSERT_NE(nullptr, generated);
  EXPECT_EQ(DAS_OP_MATCH_PHRASE_QUERY, generated->op_type_);
  EXPECT_FALSE(static_cast<ObDASMatchPhraseCtDef *>(generated)->is_scoring());
  EXPECT_TRUE(static_cast<ObDASMatchPhraseCtDef *>(generated)->get_ir_ctdef()->need_calc_relevance());
  phrase->is_scoring_ = true;
  ASSERT_EQ(OB_SUCCESS, service.generate_ctdef(scan, phrase, generated));
  ASSERT_NE(nullptr, generated);
  EXPECT_TRUE(static_cast<ObDASMatchPhraseCtDef *>(generated)->is_scoring());
  EXPECT_TRUE(static_cast<ObDASMatchPhraseCtDef *>(generated)->get_ir_ctdef()->need_calc_relevance());
  match->is_scoring_ = false;
  ASSERT_EQ(OB_SUCCESS, service.generate_ctdef(scan, match, generated));
  ASSERT_NE(nullptr, generated);
  ASSERT_EQ(DAS_OP_MATCH_QUERY, generated->op_type_);
  EXPECT_TRUE(static_cast<ObDASMatchCtDef *>(generated)->get_ir_ctdef()->need_calc_relevance());
}


TEST_F(SearchTextBindingTest, string_root_binding_respects_candidate_order_visibility_and_mode)
{
  main_.get_column_schema(42)->set_data_type(ObVarcharType);
  main_.get_column_schema(42)->set_collation_type(CS_TYPE_GBK_BIN);
  def_.get_column_schema(42)->set_data_type(ObVarcharType);
  def_.get_column_schema(42)->set_collation_type(CS_TYPE_GBK_BIN);
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH, "");
  EXPECT_EQ(OB_SUCCESS, check(phrase_, "$"));
  ObSqlSchemaGuard guard;
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, phrase_));
  ObTableSchema ordinary;
  init(ordinary, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, ordinary.set_table_name("__idx_1001_ordinary"));
  ASSERT_EQ(OB_SUCCESS, ordinary.set_index_params("FTS_INDEX_TYPE = MATCH"));
  add_column(main_, 60, "word", ObVarcharType, 0);
  add_column(ordinary, 60, "word", ObVarcharType, 1);
  main_.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ordinary.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ASSERT_EQ(OB_SUCCESS, main_.get_column_schema(60)->add_cascaded_column_id(42));
  ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(ObAuxTableMetaInfo(
      ordinary.get_table_id(), ordinary.get_table_type(), ordinary.get_index_type())));
  ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&ordinary));
  ObSEArray<uint64_t, 2> candidates;
  for (bool search_first : {false, true}) {
    candidates.reuse();
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(search_first ? def_.get_table_id() : ordinary.get_table_id()));
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(search_first ? ordinary.get_table_id() : def_.get_table_id()));
    ASSERT_EQ(OB_SUCCESS, resolve(guard, false, &candidates));
    EXPECT_EQ(search_first ? phrase_.get_table_id() : ordinary.get_table_id(), selected_text_id_);
    EXPECT_EQ(search_first, info_.has_search_index_fulltext());
    ASSERT_EQ(OB_SUCCESS, resolve(guard, true, &candidates));
    EXPECT_EQ(phrase_.get_table_id(), selected_text_id_);
  }
  candidates.reuse();
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(ordinary.get_table_id()));
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, true, &candidates));
  EXPECT_EQ(OB_SUCCESS, resolve(guard, false, &candidates));
  candidates.reuse();
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(def_.get_table_id()));
  def_.set_index_visibility(1);
  EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, resolve(guard, false, &candidates));
}

TEST_F(SearchTextBindingTest, ordinary_candidates_do_not_replace_native_search_fulltext)
{
  main_.get_column_schema(42)->set_data_type(ObVarcharType);
  main_.get_column_schema(42)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  def_.get_column_schema(42)->set_data_type(ObVarcharType);
  def_.get_column_schema(42)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "");
  add_column(main_, 60, "word", ObVarcharType, 0);
  main_.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ASSERT_EQ(OB_SUCCESS, main_.get_column_schema(60)->add_cascaded_column_id(42));
  ObSqlSchemaGuard guard;
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, match_));
  ObTableSchema ordinary_indexes[2];
  for (int64_t i = 0; i < 2; ++i) {
    const ObIndexType type = i == 0 ? INDEX_TYPE_FTS_INDEX_LOCAL : INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
    SCOPED_TRACE(type);
    ObTableSchema &ordinary = ordinary_indexes[i];
    init(ordinary, guard.get_next_mocked_schema_id(), main_.get_table_id(), type);
    ASSERT_EQ(OB_SUCCESS, ordinary.set_table_name("__idx_1001_ordinary"));
    add_column(ordinary, 60, "word", ObVarcharType, 1);
    ordinary.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
    ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&ordinary));
    ObColumnRefRawExpr field;
    field.set_ref_id(main_.get_table_id(), 42);
    field.set_data_type(ObVarcharType);
    field.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObSEArray<uint64_t, 2> candidates;
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(ordinary.get_table_id()));
    ObFullTextIndexBinding binding;
    ASSERT_EQ(OB_SUCCESS, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    EXPECT_EQ(ordinary.get_table_id(), binding.inv_idx_tid_);
    EXPECT_EQ(type == INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, binding.is_async_);
    EXPECT_FALSE(binding.search_info_.has_search_index_fulltext());
    binding.search_info_.path_ = ObString("$");
    EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(def_.get_table_id()));
    ASSERT_EQ(OB_SUCCESS, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    EXPECT_EQ(match_.get_table_id(), binding.inv_idx_tid_);
    EXPECT_TRUE(binding.is_async_);
    EXPECT_TRUE(binding.search_info_.has_search_index_fulltext());
    EXPECT_EQ(ObString("$"), binding.search_info_.path_);

    // Neither ordinary FTS family can serve a JSON path or replace an ineligible Text index.
    field.set_data_type(ObJsonType);
    main_.get_column_schema(42)->set_data_type(ObJsonType);
    def_.get_column_schema(42)->set_data_type(ObJsonType);
    set_config(def_, OB_FTS_INDEX_TYPE_MATCH);
    binding.search_info_.path_ = ObString("$.body");
    EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    binding.search_info_.path_ = ObString("$.title");
    ASSERT_EQ(OB_SUCCESS, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    EXPECT_EQ(match_.get_table_id(), binding.inv_idx_tid_);
    match_.set_index_status(INDEX_STATUS_UNAVAILABLE);
    EXPECT_EQ(OB_ERR_FT_COLUMN_NOT_INDEXED, ObDSLFullTextQuery::resolve_single_field_index(
        field, false, main_, guard, candidates, alloc_, binding));
    match_.set_index_status(INDEX_STATUS_AVAILABLE);
    main_.get_column_schema(42)->set_data_type(ObVarcharType);
    def_.get_column_schema(42)->set_data_type(ObVarcharType);
    set_config(def_, OB_FTS_INDEX_TYPE_MATCH, "");
  }
}

TEST_F(SearchTextBindingTest, native_string_match_prefers_ordinary_fulltext)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  ObSqlSchemaGuard guard;
  guard.set_schema_guard(&schema_guard);
  main_.set_table_id(guard.get_next_mocked_schema_id());
  ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(&main_));
  main_.get_column_schema(42)->set_data_type(ObVarcharType);
  main_.get_column_schema(42)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  def_.get_column_schema(42)->set_data_type(ObVarcharType);
  def_.get_column_schema(42)->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  set_config(def_, OB_FTS_INDEX_TYPE_PHRASE_MATCH, "");
  // Put Search Def first: native SQL priority must not depend on schema order.
  ASSERT_NO_FATAL_FAILURE(register_candidate(guard, def_, phrase_));
  ObTableSchema ordinary, forward;
  init(ordinary, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_FTS_INDEX_LOCAL);
  init(forward, guard.get_next_mocked_schema_id(), main_.get_table_id(), INDEX_TYPE_FTS_DOC_WORD_LOCAL);
  ASSERT_EQ(OB_SUCCESS, ordinary.set_table_name("__idx_ordinary"));
  ASSERT_EQ(OB_SUCCESS, forward.set_table_name("__idx_ordinary_doc_word"));
  add_column(main_, 60, "word", ObVarcharType, 0);
  add_column(ordinary, 60, "word", ObVarcharType, 1);
  main_.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ordinary.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  ASSERT_EQ(OB_SUCCESS, main_.get_column_schema(60)->add_cascaded_column_id(42));
  for (ObTableSchema *index : {&ordinary, &forward}) {
    ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
        ObAuxTableMetaInfo(index->get_table_id(), index->get_table_type(), index->get_index_type())));
    ASSERT_EQ(OB_SUCCESS, guard.get_mocked_table_schemas().push_back(index));
  }
  ObSchemaChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(guard));
  add_column(main_, 43, "other", ObVarcharType, 0);

  for (ObObjType type : {ObCharType, ObVarcharType, ObTextType}) {
    SCOPED_TRACE(type);
    main_.get_column_schema(42)->set_data_type(type);
    def_.get_column_schema(42)->set_data_type(type);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, false);
    resolve_native(session, checker, T_FIELD_LIST_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, false);
    resolve_native(session, checker, T_WHERE_SCOPE, BOOLEAN_MODE, OB_SUCCESS, false);
    // An explicit path skips ordinary FULLTEXT and keeps Search FULLTEXT restrictions.
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_INVALID_ARGUMENT, true, true);
    resolve_native(session, checker, T_FIELD_LIST_SCOPE, NATURAL_LANGUAGE_MODE, OB_NOT_SUPPORTED, true, true);
    resolve_native(session, checker, T_WHERE_SCOPE, BOOLEAN_MODE, OB_NOT_SUPPORTED, true, true);
    resolve_native(session, checker, T_WHERE_SCOPE, MATCH_PHRASE_MODE, OB_INVALID_ARGUMENT, true, true);
    def_.set_index_visibility(1);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, false);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false, true);
    def_.set_index_visibility(0);
    ordinary.set_index_visibility(1);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, true);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_INVALID_ARGUMENT, true, true);
    resolve_native(session, checker, T_WHERE_SCOPE, MATCH_PHRASE_MODE, OB_SUCCESS, true);
    resolve_native(session, checker, T_FIELD_LIST_SCOPE, NATURAL_LANGUAGE_MODE, OB_NOT_SUPPORTED, true);
    resolve_native(session, checker, T_WHERE_SCOPE, BOOLEAN_MODE, OB_NOT_SUPPORTED, true);
    def_.set_index_visibility(1);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false);
    def_.set_index_visibility(0);
    ordinary.set_index_visibility(0);
  }
  // An unavailable auxiliary table also makes the ordinary index unusable.
  forward.set_index_visibility(1);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, true);
  forward.set_index_visibility(0);
  // A malformed ordinary schema is a real error, not permission to fall back.
  ordinary.get_column_schema(60)->del_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_UNEXPECTED, false);
  // With a path, the ordinary schema is not inspected at all.
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_INVALID_ARGUMENT, true, true);
  // JSON skips ordinary FTS and requires an available Search FULLTEXT definition.
  main_.get_column_schema(42)->set_data_type(ObJsonType);
  def_.get_column_schema(42)->set_data_type(ObJsonType);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, true);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, true, true);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_NOT_SUPPORTED, false, false, ObJsonType);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_NOT_SUPPORTED, false, false, ObVarcharType);
  def_.set_index_visibility(1);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false, true);
  def_.set_index_visibility(0);
  main_.get_column_schema(42)->set_data_type(ObVarcharType);
  def_.get_column_schema(42)->set_data_type(ObVarcharType);
  // Detect JSON even when it is not the first MATCH column.
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_NOT_SUPPORTED, false, false, ObJsonType);
  ordinary.get_column_schema(60)->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  // A multi-column string MATCH cannot fall back to a single-column Search Def.
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false, false, ObVarcharType);
  // Ordinary asynchronous FTS remains a DSL-only candidate for this SQL entry.
  ordinary.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  main_.reset_simple_index_infos();
  for (ObTableSchema *index : {&def_, &ordinary}) {
    ASSERT_EQ(OB_SUCCESS, main_.add_simple_index_info(
        ObAuxTableMetaInfo(index->get_table_id(), index->get_table_type(), index->get_index_type())));
  }
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_SUCCESS, true);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_INVALID_ARGUMENT, true, true);
  def_.set_index_visibility(1);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false);
  resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false, true);
  // An empty candidate list must fail for both string and JSON sources.
  main_.reset_simple_index_infos();
  for (ObObjType type : {ObVarcharType, ObJsonType}) {
    SCOPED_TRACE(type);
    main_.get_column_schema(42)->set_data_type(type);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false);
    resolve_native(session, checker, T_WHERE_SCOPE, NATURAL_LANGUAGE_MODE, OB_ERR_FT_COLUMN_NOT_INDEXED, false, true);
  }
}

TEST_F(SearchTextBindingTest, native_string_match_copy_identity_reset_and_charset)
{
  ObRawExprFactory factory(alloc_);
  ObMatchFunRawExpr *match = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObConstRawExpr *query = nullptr;
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObTextType);
  column->set_collation_type(CS_TYPE_UTF16_BIN);
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("alpha"), CS_TYPE_UTF8MB4_BIN, query));
  match->set_search_key(query);
  EXPECT_FALSE(match->is_search_text());
  match->set_search_text(true);
  ObRawExprResType type;
  ASSERT_EQ(OB_SUCCESS, match->get_match_column_type(type));
  EXPECT_EQ(ObVarcharType, type.get_type());
  EXPECT_EQ(CS_TYPE_UTF16_BIN, type.get_collation_type());
  ObRawExpr *copy = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprCopier::copy_expr(factory, match, copy));
  ObMatchFunRawExpr *copied = static_cast<ObMatchFunRawExpr *>(copy);
  EXPECT_TRUE(copied->is_search_text());
  EXPECT_TRUE(match->same_as(*copy));
  EXPECT_EQ(match->hash_internal(0), copy->hash_internal(0));
  copied->set_search_text(false);
  EXPECT_FALSE(match->same_as(*copy));
  EXPECT_NE(match->hash_internal(0), copy->hash_internal(0));
  copied->set_search_text(true);
  copied->reset();
  ASSERT_EQ(OB_SUCCESS, copied->get_match_columns().push_back(column));
  EXPECT_FALSE(copied->is_search_text());
}

TEST_F(SearchTextBindingTest, native_match_copy_and_identity_include_json_path)
{
  ObRawExprFactory factory(alloc_);
  ObMatchFunRawExpr *match = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObConstRawExpr *query = nullptr;
  ObConstRawExpr *path = nullptr;
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  match->set_search_text(true);
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("alpha"), CS_TYPE_UTF8MB4_GENERAL_CI, query));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("$.title"), CS_TYPE_UTF8MB4_BIN, path));
  match->set_search_key(query);
  match->set_json_path_expr(path);
  ASSERT_TRUE(match->is_search_text());
  ASSERT_EQ(3, match->get_param_count());
  ObRawExpr *copy = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprCopier::copy_expr(factory, match, copy));
  ASSERT_NE(nullptr, copy);
  auto *copied_match = static_cast<ObMatchFunRawExpr *>(copy);
  EXPECT_TRUE(match->same_as(*copy));
  EXPECT_NE(path, copied_match->get_json_path_expr());
  ObConstRawExpr *other_path = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("$.body"), CS_TYPE_UTF8MB4_BIN, other_path));
  ASSERT_EQ(OB_SUCCESS, copied_match->replace_param_expr(2, other_path));
  EXPECT_FALSE(match->same_as(*copy));
  EXPECT_EQ(path, match->get_json_path_expr());
  copied_match->set_json_path_expr(nullptr);
  EXPECT_FALSE(match->same_as(*copy));
  EXPECT_EQ(2, copied_match->get_param_count());
}

TEST_F(SearchTextBindingTest, native_match_uses_json_string_collation)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObSqlCtx sql_ctx;
  ObExecContext exec_ctx(alloc_);
  exec_ctx.set_sql_ctx(&sql_ctx);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  exec_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&exec_ctx);
  auto unregister_exec_ctx = MakeScopedLambda([&]() { session.set_cur_exec_ctx(nullptr); });
  ObSqlSchemaGuard guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObSelectStmt stmt(alloc_);
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *one = nullptr;
  ObConstRawExpr *query = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObMatchFunRawExpr *match = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("cafe café"), CS_TYPE_UTF8MB4_GENERAL_CI, query));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  match->set_search_text(true);
  match->set_search_key(query);
  ObRawExprResType target_type;
  ASSERT_EQ(OB_SUCCESS, match->get_match_column_type(target_type));
  EXPECT_EQ(ObVarcharType, target_type.get_type());
  EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, target_type.get_collation_type());
  const struct {
    const char *input_;
    const char *normalized_;
  } paths[] = {
    {nullptr, "$"},
    {"$", "$"},
    {"$.aaaa", "$.aaaa"},
    {"$.aaaA", "$.aaaA"},
    {"$.\"title\"", "$.title"},
    {"$.\"title.text\".a", "$.\"title.text\".a"},
    {"$.\"aaaA\"", "$.aaaA"},
    {"$.\"\\u0074itle\"", "$.title"}
  };
  for (auto mode : {NATURAL_LANGUAGE_MODE, MATCH_PHRASE_MODE}) {
    match->set_mode_flag(mode);
    for (const auto &item : paths) {
      SCOPED_TRACE(item.normalized_);
      ObConstRawExpr *path = nullptr;
      if (nullptr != item.input_) {
        ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
            factory, ObVarcharType, ObString(item.input_), CS_TYPE_UTF8MB4_GENERAL_CI, path));
      }
      match->set_json_path_expr(path);
      ASSERT_EQ(OB_SUCCESS, match->formalize(&session));
      ObDSLQuery *dsl = nullptr;
      ASSERT_EQ(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
          *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
      ASSERT_NE(nullptr, dsl);
      const auto *fulltext = static_cast<ObDSLFullTextQuery *>(dsl);
      ASSERT_NE(nullptr, fulltext->query_);
      EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, fulltext->query_->get_collation_type());
      EXPECT_EQ(match->get_search_key(), fulltext->query_);
      EXPECT_EQ(ObString(item.normalized_), fulltext->path_);
    }
  }
  for (const char *invalid_path : {"", "title", "$..", "$.\"unterminated"}) {
    SCOPED_TRACE(invalid_path);
    ObConstRawExpr *path = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
        factory, ObVarcharType, ObString(invalid_path), CS_TYPE_UTF8MB4_BIN, path));
    match->set_json_path_expr(path);
    ASSERT_EQ(OB_SUCCESS, match->extract_info());
    ObDSLQuery *dsl = nullptr;
    EXPECT_NE(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
        *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
    EXPECT_EQ(nullptr, dsl);
  }
  // Ordinary FULLTEXT continues to use the indexed column's collation.
  match->set_search_text(false);
  column->set_data_type(ObVarcharType);
  column->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, match->get_match_column_type(target_type));
  EXPECT_EQ(CS_TYPE_UTF8MB4_GENERAL_CI, target_type.get_collation_type());
  match->set_search_text(true);
  match->set_json_path_expr(nullptr);
  for (ObCollationType coll : {CS_TYPE_LATIN1_BIN, CS_TYPE_GBK_BIN, CS_TYPE_UTF16_BIN, CS_TYPE_UTF16LE_GENERAL_CI}) {
    column->set_collation_type(coll);
    match->set_search_key(query);
    ASSERT_EQ(OB_SUCCESS, match->formalize(&session));
    ObDSLQuery *dsl = nullptr;
    ASSERT_EQ(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
        *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
    const ObDSLFullTextQuery *fulltext = static_cast<ObDSLFullTextQuery *>(dsl);
    ObString expected;
    ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(alloc_, ObString("cafe café"),
        CS_TYPE_UTF8MB4_BIN, coll, expected));
    EXPECT_EQ(coll, fulltext->query_->get_collation_type());
    EXPECT_EQ(match->get_search_key(), fulltext->query_);
    ObObj actual;
    bool got_value = false;
    ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_const_or_calculable_expr(
        &exec_ctx, fulltext->query_, actual, got_value, alloc_, false /* ignore_failure */));
    ASSERT_TRUE(got_value);
    EXPECT_EQ(expected, actual.get_string());
  }
}

TEST_F(SearchTextBindingTest, native_match_path_charset_matches_json_extract)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObSchemaGetterGuard schema_guard;
  ObSqlCtx sql_ctx;
  sql_ctx.session_info_ = &session;
  sql_ctx.schema_guard_ = &schema_guard;
  ObExecContext exec_ctx(alloc_);
  exec_ctx.set_my_session(&session);
  exec_ctx.set_sql_ctx(&sql_ctx);
  exec_ctx.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  session.set_cur_exec_ctx(&exec_ctx);
  auto unregister_exec_ctx = MakeScopedLambda([&]() { session.set_cur_exec_ctx(nullptr); });
  ObSqlSchemaGuard guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObSelectStmt stmt(alloc_);
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *one = nullptr;
  ObConstRawExpr *query = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("alpha beta"), CS_TYPE_UTF8MB4_BIN, query));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  ObSearchIndexRangeCtx range;
  ObQueryRangeCtx range_ctx(&exec_ctx);
  range_ctx.session_info_ = &session;
  range_ctx.search_index_range_ctx_ = &range;
  const struct {
    const char *input_;
    ObCollationType collation_;
    const char *member_;
  } cases[] = {
    {"$.Title", CS_TYPE_UTF8MB4_BIN, "Title"},
    {"$.\"é\"", CS_TYPE_UTF8MB4_GENERAL_CI, "é"},
    {"$.\"\xE9\"", CS_TYPE_LATIN1_SWEDISH_CI, "é"},
    // C3 A9 is also valid UTF-8, but latin1 names a different JSON member.
    {"$.\"\xC3\xA9\"", CS_TYPE_LATIN1_SWEDISH_CI, "Ã©"},
    {"$.\"\xD6\xD0\"", CS_TYPE_GBK_CHINESE_CI, "中"}
  };
  for (auto mode : {NATURAL_LANGUAGE_MODE, MATCH_PHRASE_MODE}) {
    for (const auto &item : cases) {
      SCOPED_TRACE(item.member_);
      SCOPED_TRACE(item.collation_);
      SCOPED_TRACE(mode);
      ObConstRawExpr *path = nullptr;
      ObMatchFunRawExpr *match = nullptr;
      ObSysFunRawExpr *json_extract = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
          factory, ObVarcharType, ObString(item.input_), item.collation_, path));
      ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
      ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
      match->set_search_text(true);
      match->set_search_key(query);
      match->set_json_path_expr(path);
      match->set_mode_flag(mode);
      ASSERT_EQ(OB_SUCCESS, match->formalize(&session));
      ASSERT_EQ(CHARSET_UTF8MB4, match->get_json_path_expr()->get_result_type().get_charset_type());
      if (ObCharset::charset_type_by_coll(item.collation_) == CHARSET_UTF8MB4) {
        EXPECT_EQ(path, match->get_json_path_expr());
      } else {
        EXPECT_EQ(T_FUN_SYS_CAST, match->get_json_path_expr()->get_expr_type());
      }
      ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract));
      json_extract->set_func_name(N_JSON_EXTRACT);
      ASSERT_EQ(OB_SUCCESS, json_extract->set_param_exprs(column, path));
      ASSERT_EQ(OB_SUCCESS, json_extract->formalize(&session));
      // Compare converted paths independently of range eligibility for CAST expressions.
      ObObj path_value;
      bool got_value = false;
      ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_const_or_calculable_expr(
          &exec_ctx, json_extract->get_param_expr(1), path_value, got_value, alloc_,
          false /* ignore_failure */));
      ASSERT_TRUE(got_value);
      ASSERT_TRUE(path_value.is_string_type());
      ObConstRawExpr *converted_path = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
          factory, ObVarcharType, path_value.get_string(), path_value.get_collation_type(),
          converted_path));
      bool can_extract = false;
      ObString value_prefix;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
          alloc_, range_ctx, *converted_path, value_prefix, false, can_extract));
      ASSERT_TRUE(can_extract);
      const std::string expected_prefix = std::string("\0\x1c", 2) + item.member_;
      EXPECT_EQ(ObString(expected_prefix.size(), expected_prefix.data()), value_prefix);

      ObDSLQuery *dsl = nullptr;
      ASSERT_EQ(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
          *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
      ASSERT_NE(nullptr, dsl);
      ObJsonPath bound_path(static_cast<ObDSLFullTextQuery *>(dsl)->path_, &alloc_);
      ASSERT_EQ(OB_SUCCESS, bound_path.parse_path());
      ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> items;
      bool supported = false;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::build_json_path_items(
          alloc_, bound_path, items, supported));
      ASSERT_TRUE(supported);
      ASSERT_EQ(1, items.count());
      EXPECT_EQ(ObString(item.member_), items.at(0).path);
      ObString text_prefix;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::encode_path_prefix(alloc_, items, text_prefix));
      EXPECT_EQ(value_prefix, text_prefix);
    }
  }
}

TEST_F(SearchTextBindingTest, native_match_keeps_runtime_query_parameters)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObExecContext exec_ctx(alloc_);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  exec_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&exec_ctx);
  auto unregister_exec_ctx = MakeScopedLambda([&]() { session.set_cur_exec_ctx(nullptr); });
  ObPhysicalPlanCtx &plan_ctx = *exec_ctx.get_physical_plan_ctx();
  ObObjParam value;
  value.set_varchar(ObString("foo"));
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  value.set_param_meta();
  ASSERT_EQ(OB_SUCCESS, plan_ctx.get_param_store_for_update().push_back(value));
  plan_ctx.set_original_param_cnt(1);
  ASSERT_EQ(OB_SUCCESS, plan_ctx.init_datum_param_store());
  ObSqlSchemaGuard guard;
  ObSchemaGetterGuard schema_guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObQueryCtx query_ctx(alloc_);
  ObSelectStmt stmt(alloc_);
  stmt.set_query_ctx(&query_ctx);
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false, &query_ctx);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *one = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  const struct {
    ObMatchAgainstMode mode_;
    ObCollationType collation_;
  } cases[] = {
    {NATURAL_LANGUAGE_MODE, CS_TYPE_UTF8MB4_BIN},
    {NATURAL_LANGUAGE_MODE, CS_TYPE_UTF8MB4_GENERAL_CI},
    {MATCH_PHRASE_MODE, CS_TYPE_UTF8MB4_BIN},
    {MATCH_PHRASE_MODE, CS_TYPE_UTF8MB4_GENERAL_CI}
  };
  for (const auto &item : cases) {
    ObConstRawExpr *literal = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
        factory, ObVarcharType, ObString("foo"), item.collation_, literal));
    ObRawExpr *param = literal;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::create_param_expr(factory, 0, param));
    ObMatchFunRawExpr *match = nullptr;
    ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
    ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
    match->set_search_text(true);
    match->set_search_key(param);
    match->set_mode_flag(item.mode_);
    ASSERT_EQ(OB_SUCCESS, match->formalize(&session));
    // Keep the parameter itself, or its implicit CI -> BIN cast.
    EXPECT_EQ(item.collation_ == CS_TYPE_UTF8MB4_BIN, param == match->get_search_key());
    ObDSLQuery *dsl = nullptr;
    ASSERT_EQ(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
        *match, QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
    ASSERT_NE(nullptr, dsl);
    EXPECT_EQ(match->get_search_key(), static_cast<ObDSLFullTextQuery *>(dsl)->query_);
    EXPECT_EQ(0, query_ctx.all_plan_const_param_constraints_.count());
    ObRawExprCopier copier(factory);
    ObDSLFullTextQuery *copy = nullptr;
    ASSERT_EQ(OB_SUCCESS, static_cast<ObDSLFullTextQuery *>(dsl)->deep_copy(
        nullptr, copier, alloc_, copy));
    ASSERT_NE(nullptr, copy);
    ObSEArray<ObRawExpr *, 1> params;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::extract_params(copy->query_, params));
    ASSERT_EQ(1, params.count());
    EXPECT_EQ(0, static_cast<ObConstRawExpr *>(params.at(0))->get_value().get_unknown());
    EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, copy->query_->get_collation_type());

    // Compile once, then change only the parameter slot across executions.
    ObStaticEngineExprCG expr_cg(alloc_, &session, &schema_guard, 1, 1, CLUSTER_CURRENT_VERSION);
    ObRawExprUniqueSet flattened(false);
    ObExprFrameInfo frames(alloc_);
    ASSERT_EQ(OB_SUCCESS, expr_cg.generate(copy->query_, flattened, frames));
    ASSERT_EQ(OB_SUCCESS, frames.pre_alloc_exec_memory(exec_ctx));
    ObEvalCtx eval_ctx(exec_ctx);
    ObExpr *runtime_query = copy->query_->rt_expr_;
    ASSERT_NE(nullptr, runtime_query);
    ObDASScalarScanCtDef inv_scan(alloc_);
    ObDASIRScanCtDef ir(alloc_);
    ir.use_scalar_scan_def_ = true;
    ObDASBaseCtDef *ir_children[] = {&inv_scan};
    ir.children_ = ir_children;
    ir.children_cnt_ = 1;
    auto &info = ir.search_index_fulltext_info_;
    info.column_idx_ = 0;
    info.parser_name_ = ObString("analyzer.1");
    info.parser_properties_ = ObString(
        R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword"}}})");
    ObDASMatchPhraseCtDef phrase(alloc_);
    ObDASBaseCtDef *phrase_children[] = {&ir};
    phrase.children_ = phrase_children;
    phrase.children_cnt_ = 1;
    phrase.ir_ctdef_idx_ = 0;
    phrase.query_text_ = runtime_query;
    ObDASMatchPhraseRtDef phrase_rt;
    phrase_rt.ctdef_ = &phrase;
    phrase_rt.eval_ctx_ = &eval_ctx;
    const char *terms[] = {"foo", "foo ", "bar", "cafe", "café", "", nullptr, "foo"};
    for (const char *term : terms) {
      SCOPED_TRACE(term == nullptr ? "NULL" : term);
      for (int64_t i = 0; i < frames.rt_exprs_.count(); ++i) {
        frames.rt_exprs_.at(i).get_eval_info(eval_ctx).clear_evaluated_flag();
      }
      ObDatum &datum = params.at(0)->rt_expr_->locate_expr_datum(eval_ctx);
      if (nullptr == term) {
        datum.set_null();
      } else {
        datum.set_string(ObString::make_string(term));
      }
      const int64_t expected_count = nullptr == term || '\0' == *term ? 0 : 1;
      ObSEArray<ObString, 1> tokens;
      ObSEArray<int64_t, 1> counts;
      ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::eval_and_tokenize_query_text(
          alloc_, eval_ctx, *runtime_query, inv_scan, tokens, &ir));
      ASSERT_EQ(expected_count, tokens.count());
      if (expected_count > 0) { EXPECT_EQ(ObString::make_string(term), tokens.at(0)); }
      ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::eval_and_tokenize_query_text(
          alloc_, eval_ctx, *runtime_query, inv_scan, tokens, counts, &ir));
      ASSERT_EQ(expected_count, tokens.count());
      EXPECT_EQ(expected_count, counts.count());
      if (expected_count > 0) {
        EXPECT_EQ(ObString::make_string(term), tokens.at(0));
        EXPECT_EQ(1, counts.at(0));
      }
      phrase_rt.reset_cached_runtime_state();
      ASSERT_EQ(OB_SUCCESS, phrase_rt.prepare_query_tokens());
      ASSERT_EQ(expected_count, phrase_rt.query_tokens_.count());
      if (expected_count > 0) {
        EXPECT_EQ(ObString::make_string(term), phrase_rt.query_tokens_.at(0));
      }
    }
  }
}

TEST_F(SearchTextBindingTest, native_match_preserves_query_error_with_path)
{
  ObSQLSessionInfo session;
  ObExecContext exec_ctx(alloc_);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObSqlSchemaGuard guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObSelectStmt stmt(alloc_);
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *query = nullptr;
  ObConstRawExpr *path = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObMatchFunRawExpr *match = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, query));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("$.title"), CS_TYPE_UTF8MB4_BIN, path));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  match->set_search_text(true);
  match->set_search_key(query);
  match->set_json_path_expr(path);

  ObDSLQuery *dsl = nullptr;
  EXPECT_EQ(OB_NOT_SUPPORTED, join_order.convert_search_text_filter_to_dsl(
      *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, query, dsl));
  EXPECT_EQ(nullptr, dsl);
}

TEST_F(SearchTextBindingTest, native_match_field_syntax_follows_design)
{
  const char *supported[] = {
    "SELECT id FROM docs WHERE MATCH(doc->'$.title') AGAINST('alpha')",
    "SELECT id FROM docs WHERE MATCH(JSON_EXTRACT(doc, '$.title')) AGAINST('alpha')",
    "SELECT id FROM docs WHERE MATCH(doc->'$') AGAINST('alpha')",
    "SELECT id FROM docs WHERE MATCH(doc->'$.title') AGAINST('alpha beta' IN MATCH PHRASE MODE)",
    "SELECT id FROM docs WHERE MATCH(body) AGAINST('alpha')"
  };
  for (const char *sql : supported) {
    SCOPED_TRACE(sql);
    ObParser parser(alloc_, DEFAULT_MYSQL_MODE);
    ParseResult result;
    EXPECT_EQ(OB_SUCCESS, parser.parse(sql, result));
    parser.free_result(result);
  }
  const char *unsupported[] = {
    "SELECT id FROM docs WHERE MATCH(doc->>'$.title') AGAINST('alpha')",
    "SELECT id FROM docs WHERE MATCH(JSON_UNQUOTE(JSON_EXTRACT(doc, '$.title'))) AGAINST('alpha')",
    "SELECT id FROM docs WHERE MATCH(CAST(doc->'$.title' AS CHAR)) AGAINST('alpha')"
  };
  for (const char *sql : unsupported) {
    SCOPED_TRACE(sql);
    ObParser parser(alloc_, DEFAULT_MYSQL_MODE);
    ParseResult result;
    EXPECT_EQ(OB_ERR_PARSE_SQL, parser.parse(sql, result));
    parser.free_result(result);
  }
}

TEST_F(SearchTextBindingTest, native_match_path_preserves_case_without_parameterization)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObExecContext exec_ctx(alloc_);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ParamStore &params = exec_ctx.get_physical_plan_ctx()->get_param_store_for_update();
  const struct {
    const char *sql_;
    const char *path_;
    const char *expected_sql_;
  } cases[] = {
    {"SELECT id FROM docs WHERE MATCH(doc->'$.aaaa') AGAINST('alpha')", "$.aaaa",
     "SELECT id FROM docs WHERE MATCH(doc->'$.aaaa') AGAINST(?)"},
    {"SELECT id FROM docs WHERE MATCH(doc->'$.aaaA') AGAINST('alpha')", "$.aaaA",
     "SELECT id FROM docs WHERE MATCH(doc->'$.aaaA') AGAINST(?)"},
    {"SELECT id FROM docs WHERE MATCH(JSON_EXTRACT(doc, '$.aaaa')) AGAINST('alpha')", "$.aaaa",
     "SELECT id FROM docs WHERE MATCH(JSON_EXTRACT(doc, '$.aaaa')) AGAINST(?)"},
    {"SELECT id FROM docs WHERE MATCH(JSON_EXTRACT(doc, '$.aaaA')) AGAINST('alpha')", "$.aaaA",
     "SELECT id FROM docs WHERE MATCH(JSON_EXTRACT(doc, '$.aaaA')) AGAINST(?)"}
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.sql_);
    const ObString sql = ObString::make_string(item.sql_);
    ObString no_param_sql;
    ObSEArray<ObPCParam *, 4> raw_params;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::raw_fast_parameterize_sql(
        alloc_, session, sql, no_param_sql, raw_params));
    ASSERT_EQ(2, raw_params.count());
    ObParser parser(alloc_, DEFAULT_MYSQL_MODE);
    ParseResult result;
    ASSERT_EQ(OB_SUCCESS, parser.parse(sql, result));
    std::vector<ParseNode *> nodes{result.result_tree_};
    ParseNode *match = nullptr;
    for (size_t i = 0; i < nodes.size() && match == nullptr; ++i) {
      ParseNode *node = nodes.at(i);
      if (node != nullptr) {
        if (node->type_ == T_FUN_MATCH_AGAINST) {
          match = node;
        } else {
          for (int64_t j = 0; j < node->num_child_; ++j) {
            nodes.push_back(node->children_[j]);
          }
        }
      }
    }
    ASSERT_NE(nullptr, match);
    ASSERT_EQ(3, match->num_child_);
    EXPECT_EQ(ObString(item.path_), ObString(match->children_[1]->str_len_, match->children_[1]->str_value_));
    EXPECT_EQ(ObString("alpha"), ObString(match->children_[2]->str_len_, match->children_[2]->str_value_));
    SqlInfo sql_info;
    sql_info.need_check_fp_ = true;
    SelectItemParamInfoArray select_items;
    ObMaxConcurrentParam::FixParamStore fixed_params(
        OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&alloc_));
    params.reset();
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::transform_syntax_tree(
        alloc_, session, &raw_params, result.result_tree_, sql_info, params,
        &select_items, fixed_params, false, TEXT_MODE));
    EXPECT_EQ(1, params.count());
    EXPECT_TRUE(sql_info.not_param_index_.has_member(0));
    EXPECT_FALSE(sql_info.not_param_index_.has_member(1));
    EXPECT_EQ(ObString(item.path_), ObString(match->children_[1]->str_len_, match->children_[1]->str_value_));
    ObSEArray<ObPCParam *, 4> special_params;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::check_and_generate_param_info(
        raw_params, sql_info, special_params));
    ASSERT_EQ(1, special_params.count());
    EXPECT_EQ(NOT_PARAM, special_params.at(0)->flag_);
    char sql_buf[256];
    int32_t sql_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::construct_sql(
        no_param_sql, special_params, sql_buf, sizeof(sql_buf), sql_len));
    EXPECT_EQ(ObString(item.expected_sql_), ObString(sql_len, sql_buf));
    parser.free_result(result);
  }
}


TEST_F(SearchTextBindingTest, native_string_match_reads_current_query_bytes)
{
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, oceanbase::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  session.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ObSqlCtx sql_ctx;
  ObExecContext exec_ctx(alloc_);
  exec_ctx.set_sql_ctx(&sql_ctx);
  exec_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&exec_ctx);
  struct ExecContextGuard
  {
    explicit ExecContextGuard(ObSQLSessionInfo &session) : session_(session) {}
    ~ExecContextGuard() { session_.set_cur_exec_ctx(nullptr); }
    ObSQLSessionInfo &session_;
  } unregister(session);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ParamStore &params = exec_ctx.get_physical_plan_ctx()->get_param_store_for_update();
  ObObjParam param;
  param.set_varchar("cafe");
  param.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  param.set_param_meta();
  ASSERT_EQ(OB_SUCCESS, params.push_back(param));
  ObSqlSchemaGuard guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObQueryCtx query_ctx(alloc_);
  ObSelectStmt stmt(alloc_);
  stmt.set_query_ctx(&query_ctx);
  ObOptimizerContext opt_ctx(&session, &exec_ctx, &guard, nullptr, alloc_, &params,
      addr, nullptr, hint, factory, &stmt, false, &query_ctx);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *one = nullptr;
  ObConstRawExpr *literal = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObMatchFunRawExpr *match = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
      ObString("cafe"), CS_TYPE_UTF8MB4_GENERAL_CI, literal));
  ObRawExpr *query = literal;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::create_param_expr(factory, 0, query));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObVarcharType);
  column->set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  match->set_search_key(query);
  match->set_search_text(true);
  ASSERT_EQ(OB_SUCCESS, match->formalize(&session));
  ObDSLQuery *dsl = nullptr;
  ASSERT_EQ(OB_SUCCESS, join_order.convert_search_text_filter_to_dsl(
      *match, ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
  exec_ctx.get_physical_plan_ctx()->set_original_param_cnt(params.count());
  const ObDSLFullTextQuery *fulltext = static_cast<ObDSLFullTextQuery *>(dsl);
  EXPECT_EQ(match->get_search_key(), fulltext->query_);
  EXPECT_EQ(0, query_ctx.all_plan_const_param_constraints_.count());
  EXPECT_EQ(0, query_ctx.all_expr_constraints_.count());
  for (const char *value : {"cafe", "café", "CAFE", "cafe "}) {
    params.at(0).set_varchar(value);
    params.at(0).set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, exec_ctx.get_physical_plan_ctx()->init_datum_param_store());
    ObObj result;
    ASSERT_EQ(OB_SUCCESS, ObSQLUtils::calc_const_expr(
        exec_ctx, fulltext->query_, result, alloc_, params));
    EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, result.get_collation_type());
    EXPECT_EQ(ObString::make_string(value), result.get_string());
  }
}

TEST_F(SearchTextBindingTest, native_match_rejects_sql_or_but_preserves_scalar_or)
{
  ObSQLSessionInfo session;
  ObSqlSchemaGuard guard;
  ObRawExprFactory factory(alloc_);
  ObGlobalHint hint(alloc_);
  ObAddr addr;
  ObSelectStmt stmt(alloc_);
  ObOptimizerContext opt_ctx(&session, nullptr, &guard, nullptr, alloc_, nullptr,
      addr, nullptr, hint, factory, &stmt, false);
  ObSelectLogPlan plan(opt_ctx, &stmt);
  ObSEArray<ObRawExpr *, 4> quals;
  ObJoinOrderEnumPermutation enumeration(plan, quals);
  ObJoinOrder join_order(enumeration, ACCESS);
  ObConstRawExpr *one = nullptr;
  ObConstRawExpr *query = nullptr;
  ObColumnRefRawExpr *column = nullptr;
  ObMatchFunRawExpr *match = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_int_expr(factory, ObIntType, 1, one));
  ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(
      factory, ObVarcharType, ObString("alpha"), CS_TYPE_UTF8MB4_GENERAL_CI, query));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, column));
  column->set_ref_id(1001, 42);
  column->set_data_type(ObJsonType);
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_MATCH_AGAINST, match));
  ASSERT_EQ(OB_SUCCESS, match->get_match_columns().push_back(column));
  match->set_search_text(true);
  match->set_search_key(query);
  ASSERT_EQ(OB_SUCCESS, match->extract_info());
  for (bool with_match : {true, false}) {
    SCOPED_TRACE(with_match);
    ObOpRawExpr *disjunction = nullptr;
    ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_OP_OR, disjunction));
    ASSERT_EQ(OB_SUCCESS, disjunction->set_param_exprs(
        with_match ? static_cast<ObRawExpr *>(match) : one, one));
    ASSERT_EQ(OB_SUCCESS, disjunction->extract_info());
    ObDSLQuery *dsl = nullptr;
    EXPECT_EQ(with_match ? OB_NOT_SUPPORTED : OB_SUCCESS,
        join_order.convert_filter_to_dsl_query(disjunction,
            ObEsQueryItem::QUERY_ITEM_UNKNOWN, nullptr, one, dsl));
    EXPECT_EQ(with_match, dsl == nullptr);
  }
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  int result = 1;
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_search_text_binding.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::plugin::ObPluginMgr plugins;
  if (OB_FAIL(oceanbase::common::ObCharset::init_charset())) {
    LOG_WARN("failed to initialize charsets", K(ret));
  } else if (OB_FAIL(init_sql_factories())) {
    LOG_WARN("failed to initialize SQL factories", K(ret));
  } else if (OB_FAIL(plugins.init(ObString()))) {
    LOG_WARN("failed to initialize plugin manager", K(ret));
  } else if (OB_FAIL(plugins.load_builtin_plugins())) {
    LOG_WARN("failed to load builtin plugins", K(ret));
  } else {
    auto *saved_plugins = GCTX.plugin_mgr_;
    GCTX.plugin_mgr_ = &plugins;
    auto restore_plugins = MakeScopedLambda([&]() { GCTX.plugin_mgr_ = saved_plugins; });
    result = RUN_ALL_TESTS();
  }
  return result;
}
