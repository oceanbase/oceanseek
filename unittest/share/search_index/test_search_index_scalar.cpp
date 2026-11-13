/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_OPT
#include <gtest/gtest.h>
#include <initializer_list>
#include <utility>
#define private public
#define protected public
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_join_order_enum_idp.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/rewrite/ob_expr_range_converter.h"
#undef protected
#undef private
#include "share/search_index/ob_search_index_config.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/ob_get_compat_mode.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace
{
constexpr uint64_t PRIMARY_KEY_ID = OB_APP_MIN_COLUMN_ID + 10;

class SearchIndexScalarTest : public ::testing::Test
{
protected:
  void make_comment(std::initializer_list<ObSearchIndexModeConfig::Mode> types, ObSqlString &comment)
  {
    ObSEArray<ObSearchIndexModeConfig, 2> modes;
    for (auto type : types) {
      ObSearchIndexModeConfig mode;
      mode.mode_ = type;
      if (mode.is_fulltext()) {
        mode.parser_name_ = "space.1";
        mode.parser_properties_ = "{}";
      }
      ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    }
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.init(modes));
    ASSERT_EQ(OB_SUCCESS, config.encode(comment));
  }

  void add_candidate(std::initializer_list<ObString> comments, uint64_t &index_id,
                     ObIndexType type = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL,
                     bool with_data = false, bool json_columns = true)
  {
    // Use the guard's in-memory Schema lookup; the candidate checks themselves are production code.
    index_id = schema_guard_.get_next_mocked_schema_id();
    if (type == INDEX_TYPE_IS_NOT) {
      ref_table_id_ = index_id;
    }
    ObTableSchema schema;
    schema.set_tenant_id(1001);
    schema.set_database_id(1002);
    schema.set_table_id(index_id);
    schema.set_data_table_id(type == INDEX_TYPE_IS_NOT ? 0 : 500001);
    schema.set_table_type(type == INDEX_TYPE_IS_NOT ? USER_TABLE : USER_INDEX);
    schema.set_index_type(type);
    schema.set_charset_type(CHARSET_UTF8MB4);
    schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    schema.set_schema_version(1);
    ASSERT_EQ(OB_SUCCESS, schema.set_table_name("__idx_500001_scalar_candidate"));
    ObSEArray<uint64_t, 4> column_ids;
    for (const auto &comment : comments) {
      ObColumnSchemaV2 column;
      const uint64_t column_id = OB_APP_MIN_COLUMN_ID + column_ids.count();
      column.set_tenant_id(1001);
      column.set_table_id(index_id);
      column.set_column_id(column_id);
      column.set_rowkey_position(column_ids.count() + 1);
      column.set_index_position(schema.is_index_table() ? column_ids.count() + 1 : 0);
      column.set_data_type(schema.is_search_def_index() && json_columns ? ObJsonType : ObIntType);
      if (column.get_data_type() == ObJsonType) {
        column.set_data_length(ObAccuracy::MAX_ACCURACY[ObJsonType].get_length());
      }
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      char name[32];
      snprintf(name, sizeof(name), "c%lu", column_id);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
      ASSERT_EQ(OB_SUCCESS, column.set_comment(comment));
      ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
      ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column_id));
    }
    schema.set_index_column_num(schema.is_index_table() ? comments.size() : 0);
    if (schema.is_search_def_index()) {
      // Def rowkeys also carry the base primary key, which is not a declared search column.
      ObColumnSchemaV2 primary_key;
      primary_key.set_tenant_id(1001);
      primary_key.set_table_id(index_id);
      primary_key.set_column_id(PRIMARY_KEY_ID);
      primary_key.set_rowkey_position(column_ids.count() + 1);
      primary_key.set_data_type(ObIntType);
      primary_key.set_collation_type(CS_TYPE_BINARY);
      ASSERT_EQ(OB_SUCCESS, primary_key.set_column_name("pk"));
      ASSERT_EQ(OB_SUCCESS, schema.add_column(primary_key));
      ASSERT_EQ(OB_SUCCESS, column_ids.push_back(PRIMARY_KEY_ID));
    }
    schema.set_rowkey_column_num(column_ids.count());
    schema.set_max_used_column_id(schema.is_search_def_index()
        ? PRIMARY_KEY_ID : OB_APP_MIN_COLUMN_ID + comments.size() - 1);
    if (schema.is_search_def_index()) {
      bool needs_data = !json_columns || with_data;
      bool needs_match = false;
      bool needs_phrase = false;
      for (const auto &comment : comments) {
        ObSearchIndexConfig config;
        if (OB_SUCCESS != config.decode(comment)) {
          needs_data = true; // Keep malformed-config validation in the production collector.
        } else {
          for (int64_t i = 0; i < config.get_modes().count(); ++i) {
            const auto &mode = config.get_modes().at(i);
            needs_data |= mode.mode_ == ObSearchIndexModeConfig::VALUE;
            needs_match |= mode.mode_ == ObSearchIndexModeConfig::FULLTEXT_MATCH;
            needs_phrase |= mode.mode_ == ObSearchIndexModeConfig::FULLTEXT_PHRASE;
          }
        }
      }
      if (needs_data) {
        ASSERT_NO_FATAL_FAILURE(add_child(schema, schema.is_async_search_def_index()
            ? INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL : INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL));
      }
      if (needs_match) {
        ASSERT_NO_FATAL_FAILURE(add_child(schema, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL));
      }
      if (needs_phrase) {
        ASSERT_NO_FATAL_FAILURE(add_child(schema, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL));
      }
    }
    ASSERT_EQ(OB_SUCCESS, schema_guard_.add_mocked_table_schema(schema));
    ASSERT_EQ(OB_SUCCESS, index_ids_.push_back(index_id));
  }

  void add_child(ObTableSchema &def, ObIndexType type)
  {
    const uint64_t child_id = schema_guard_.get_next_mocked_schema_id();
    ObTableSchema child;
    child.set_tenant_id(1001);
    child.set_database_id(1002);
    child.set_table_id(child_id);
    child.set_data_table_id(def.get_table_id());
    child.set_table_type(USER_INDEX);
    child.set_index_type(type);
    child.set_index_status(INDEX_STATUS_AVAILABLE);
    child.set_charset_type(CHARSET_UTF8MB4);
    child.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    child.set_schema_version(1);
    ASSERT_EQ(OB_SUCCESS, child.set_table_name("__idx_500001_physical_search"));
    const char *names[] = {"__COL_IDX", "__PATH", "__VALUE_OR_TOKEN", "__DOC_ID"};
    for (int64_t i = 0; i < 4; ++i) {
      ObColumnSchemaV2 column;
      column.set_tenant_id(1001);
      column.set_table_id(child_id);
      // Deliberately overlap numeric IDs with source columns: they belong to different schemas.
      column.set_column_id(OB_APP_MIN_COLUMN_ID + i);
      column.set_index_position(i + 1);
      column.set_rowkey_position(i + 1);
      column.set_data_type(i == 1 || i == 2 ? ObVarcharType : ObUInt64Type);
      column.set_data_length(1024);
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(names[i]));
      ASSERT_EQ(OB_SUCCESS, child.add_column(column));
    }
    child.set_index_column_num(4);
    child.set_rowkey_column_num(4);
    child.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + 3);
    ASSERT_EQ(OB_SUCCESS, schema_guard_.add_mocked_table_schema(child));
    ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(ObAuxTableMetaInfo(child_id, USER_INDEX, type)));
  }

  uint64_t get_data_id(uint64_t id)
  {
    const ObTableSchema *schema = nullptr;
    EXPECT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(id, schema));
    if (schema != nullptr && schema->is_search_def_index()) {
      EXPECT_EQ(OB_SUCCESS, schema->get_search_data_index_tid(id));
    }
    return id;
  }

  int collect_indexes(ObIArray<uint64_t> &index_ids,
                      ObIArray<ObSEArray<uint64_t, 4>> &index_columns,
                      ObIArray<bool> &ignore_prefix)
  {
    int ret = OB_SUCCESS;
    ObSQLSessionInfo session;
    ObRawExprFactory expr_factory(allocator_);
    ObGlobalHint hint(allocator_);
    ObAddr addr;
    ObSelectStmt stmt(allocator_);
    ObOptimizerContext ctx(&session, nullptr, &schema_guard_, nullptr, allocator_, nullptr,
        addr, nullptr, hint, expr_factory, &stmt, false);
    ObSelectLogPlan plan(ctx, &stmt);
    TableItem table(allocator_);
    table.table_id_ = 500001;
    ObIndexMergeHint merge_hint(allocator_);
    LogTableHint table_hint(allocator_);
    table_hint.table_ = &table;
    table_hint.hybrid_search_index_merge_hint_ = &merge_hint;
    // Supply the candidate whitelist through a real hint; production code builds the column lists.
    if (OB_FAIL(table_hint.hybrid_search_index_merge_index_ids_.assign(index_ids_))) {
      LOG_WARN("failed to assign search index test candidates", K(ret));
    } else if (OB_FAIL(plan.log_plan_hint_.table_hints_.reserve(1))) {
      LOG_WARN("failed to reserve search index test hints", K(ret));
    } else if (OB_FAIL(plan.log_plan_hint_.table_hints_.push_back(table_hint))) {
      LOG_WARN("failed to add search index test hint", K(ret));
    } else {
      ObSEArray<ObRawExpr *, 1> filters;
      ObJoinOrderEnumIDP enumeration(plan, filters);
      ObJoinOrder order(enumeration, sql::INVALID);
      if (OB_FAIL(order.get_valid_index_merge_indexes(500001, ref_table_id_, index_ids,
          index_columns, ignore_prefix, true /* use_force_hint */, true /* is_hybrid_search */))) {
        LOG_WARN("failed to collect search index test candidates", K(ret), K_(ref_table_id));
      }
    }
    return ret;
  }

  void expect_candidates(ObRawExpr &filter, std::initializer_list<uint64_t> expected,
                         int expected_ret = OB_SUCCESS)
  {
    ObSEArray<uint64_t, 4> index_ids;
    ObSEArray<ObSEArray<uint64_t, 4>, 4> index_columns;
    ObSEArray<bool, 4> ignore_prefix;
    ASSERT_EQ(expected_ret, collect_indexes(index_ids, index_columns, ignore_prefix));
    if (expected_ret != OB_SUCCESS) {
      return;
    }
    // Candidate IDs retain the hint whitelist; only their scalar column lists are filtered.
    ASSERT_EQ(index_ids_.count(), index_ids.count());
    ASSERT_EQ(index_ids.count(), index_columns.count());
    ASSERT_EQ(index_ids.count(), ignore_prefix.count());
    for (int64_t i = 0; i < index_ids.count(); ++i) {
      EXPECT_EQ(index_ids_.at(i), index_ids.at(i));
    }
    ObHybridSearchGenerator generator(&allocator_, nullptr, &schema_guard_, nullptr,
        index_ids, index_columns, ignore_prefix, nullptr, false, nullptr);
    bool has_index = false;
    uint64_t first_id = OB_INVALID_ID;
    ASSERT_EQ(expected_ret, generator.check_filter_has_index(&filter, has_index, first_id));
    if (expected_ret == OB_SUCCESS) {
      EXPECT_EQ(expected.size() != 0, has_index);
      EXPECT_EQ(expected.size() == 0 ? OB_INVALID_ID : *expected.begin(), first_id);
    }

    ObRawExprFactory expr_factory(allocator_);
    ObGlobalHint hint(allocator_);
    ObAddr addr;
    ObOptimizerContext ctx(nullptr, nullptr, &schema_guard_, nullptr, allocator_, nullptr,
        addr, nullptr, hint, expr_factory, nullptr, false);
    ObSelectLogPlan plan(ctx, nullptr);
    ObSEArray<ObRawExpr *, 1> filters;
    ASSERT_EQ(OB_SUCCESS, filters.push_back(&filter));
    ObJoinOrderEnumIDP enumeration(plan, filters);
    ObJoinOrder order(enumeration, sql::INVALID);
    ObSEArray<uint64_t, 4> candidates;
    ASSERT_EQ(expected_ret, order.collect_candicate_indexes(ref_table_id_, filters, index_ids,
        index_columns, ignore_prefix, candidates));
    if (expected_ret == OB_SUCCESS) {
      ASSERT_EQ(expected.size(), candidates.count());
      int64_t i = 0;
      for (uint64_t id : expected) {
        EXPECT_EQ(id, candidates.at(i++));
      }
    }
  }

  void expect_column_candidates(uint64_t column_id, std::initializer_list<uint64_t> expected,
                                int expected_ret = OB_SUCCESS)
  {
    ObColumnRefRawExpr column(500001, column_id, T_REF_COLUMN);
    ObObj value;
    value.set_int(1);
    ObConstRawExpr constant(value, T_INT);
    ObOpRawExpr filter(allocator_);
    filter.set_expr_type(T_OP_EQ);
    ASSERT_EQ(OB_SUCCESS, filter.set_param_exprs(&column, &constant));
    ASSERT_NO_FATAL_FAILURE(expect_candidates(filter, expected, expected_ret));
  }

  void init_range_ctx(uint64_t index_id, ObSearchIndexRangeCtx &range_ctx)
  {
    const ObTableSchema *index_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(index_id, index_schema));
    ASSERT_NE(nullptr, index_schema);
    ObTableSchema base;
    base.set_tenant_id(1001);
    base.set_database_id(1002);
    base.set_table_id(500001);
    base.set_table_type(USER_TABLE);
    base.set_charset_type(CHARSET_UTF8MB4);
    base.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    base.set_schema_version(1);
    base.set_rowkey_column_num(1);
    base.set_max_used_column_id(PRIMARY_KEY_ID);
    ASSERT_EQ(OB_SUCCESS, base.set_table_name("docs"));
    ObRawExprFactory expr_factory(allocator_);
    ObSEArray<ColumnItem, 4> items;
    for (auto it = index_schema->column_begin(); it != index_schema->column_end(); ++it) {
      ASSERT_NE(nullptr, *it);
      ObColumnSchemaV2 column;
      ASSERT_EQ(OB_SUCCESS, column.assign(**it));
      column.set_table_id(base.get_table_id());
      column.set_index_position(0);
      column.set_rowkey_position(column.get_column_id() == PRIMARY_KEY_ID ? 1 : 0);
      column.set_prev_column_id(UINT64_MAX);
      column.set_next_column_id(UINT64_MAX);
      ASSERT_EQ(OB_SUCCESS, base.add_column(column));
      if ((*it)->is_index_column()) {
        ObColumnRefRawExpr *expr = nullptr;
        ASSERT_EQ(OB_SUCCESS, expr_factory.create_raw_expr(T_REF_COLUMN, expr));
        expr->set_ref_id(base.get_table_id(), column.get_column_id());
        expr->set_data_type(column.get_data_type());
        expr->set_collation_type(column.get_collation_type());
        ColumnItem item;
        item.table_id_ = base.get_table_id();
        item.column_id_ = column.get_column_id();
        item.expr_ = expr;
        ASSERT_EQ(OB_SUCCESS, items.push_back(item));
      }
    }
    ObSEArray<const ColumnItem *, 4> column_items;
    for (int64_t i = 0; i < items.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, column_items.push_back(&items.at(i)));
    }
    ObExecContext exec_ctx(allocator_);
    ASSERT_EQ(OB_SUCCESS, range_ctx.init(&base, column_items, &exec_ctx, index_schema, allocator_));
  }

  ObArenaAllocator allocator_;
  ObSqlSchemaGuard schema_guard_;
  ObSEArray<uint64_t, 4> index_ids_;
  uint64_t ref_table_id_ = 500001;
};

TEST_F(SearchIndexScalarTest, fulltext_only_is_not_a_scalar_candidate)
{
  for (auto mode : {ObSearchIndexModeConfig::FULLTEXT_MATCH, ObSearchIndexModeConfig::FULLTEXT_PHRASE}) {
    ObSqlString comment;
    ASSERT_NO_FATAL_FAILURE(make_comment({mode}, comment));
    uint64_t index_id = OB_INVALID_ID;
    ASSERT_NO_FATAL_FAILURE(add_candidate({comment.string()}, index_id));
  }
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {}));

  // The review example: JSON_EXTRACT(b, '$.x') = 1 also bypasses the contains-splitting helper.
  ObColumnRefRawExpr column(500001, OB_APP_MIN_COLUMN_ID, T_REF_COLUMN);
  ObObj path_value;
  path_value.set_varchar("$.x");
  ObConstRawExpr path(path_value, T_VARCHAR);
  ObSysFunRawExpr extract(allocator_);
  extract.set_expr_type(T_FUN_SYS_JSON_EXTRACT);
  ASSERT_EQ(OB_SUCCESS, extract.set_param_exprs(&column, &path));
  ObObj value;
  value.set_int(1);
  ObConstRawExpr constant(value, T_INT);
  ObOpRawExpr filter(allocator_);
  filter.set_expr_type(T_OP_EQ);
  ASSERT_EQ(OB_SUCCESS, filter.set_param_exprs(&extract, &constant));
  ASSERT_NO_FATAL_FAILURE(expect_candidates(filter, {}));
}

TEST_F(SearchIndexScalarTest, value_and_fulltext_on_same_column_remains_eligible)
{
  ObSqlString comment;
  ASSERT_NO_FATAL_FAILURE(make_comment(
      {ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_PHRASE}, comment));
  uint64_t index_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({comment.string()}, index_id));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {index_id}));
}

TEST_F(SearchIndexScalarTest, collection_filters_search_columns_and_preserves_fulltext_candidates)
{
  ObSqlString text_comment;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_MATCH}, text_comment));
  uint64_t text_id = OB_INVALID_ID;
  uint64_t mixed_id = OB_INVALID_ID;
  uint64_t fts_id = OB_INVALID_ID;
  uint64_t async_fts_id = OB_INVALID_ID;
  uint64_t numeric_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({text_comment.string()}, text_id));
  ASSERT_NO_FATAL_FAILURE(add_candidate({text_comment.string(), ObString()}, mixed_id));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, fts_id, INDEX_TYPE_FTS_INDEX_LOCAL));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, async_fts_id, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, numeric_id,
      INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL, false /* with_data */, false /* json_columns */));
  ObSEArray<uint64_t, 4> ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> columns;
  ObSEArray<bool, 4> ignore_prefix;
  ASSERT_EQ(OB_SUCCESS, collect_indexes(ids, columns, ignore_prefix));
  ASSERT_EQ(5, ids.count());
  ASSERT_EQ(ids.count(), columns.count());
  ASSERT_EQ(ids.count(), ignore_prefix.count());
  EXPECT_EQ(text_id, ids.at(0));
  EXPECT_EQ(mixed_id, ids.at(1));
  EXPECT_EQ(fts_id, ids.at(2));
  EXPECT_EQ(async_fts_id, ids.at(3));
  EXPECT_EQ(numeric_id, ids.at(4));
  for (int64_t i = 0; i < ids.count(); ++i) {
    if (i == 0) {
      EXPECT_TRUE(columns.at(i).empty());
    } else {
      ASSERT_EQ(1, columns.at(i).count());
      EXPECT_EQ(OB_APP_MIN_COLUMN_ID + (i == 1 ? 1 : 0), columns.at(i).at(0));
    }
    EXPECT_EQ(i != 2 && i != 3, ignore_prefix.at(i));
  }
}


TEST_F(SearchIndexScalarTest, string_fulltext_only_columns_cannot_be_scalar_candidates)
{
  ObSqlString text;
  ObSqlString mixed;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_MATCH}, text));
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_PHRASE}, mixed));
  uint64_t id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({text.string(), mixed.string()}, id));
  ObTableSchema *schema = nullptr;
  const ObIArray<const ObTableSchema *> &schemas = schema_guard_.get_mocked_table_schemas();
  for (int64_t i = 0; i < schemas.count(); ++i) {
    const ObTableSchema *candidate = schemas.at(i);
    if (candidate->get_table_id() == id) { schema = const_cast<ObTableSchema *>(candidate); }
  }
  ASSERT_NE(nullptr, schema);
  schema->get_column_schema(OB_APP_MIN_COLUMN_ID)->set_data_type(ObVarcharType);
  schema->get_column_schema(OB_APP_MIN_COLUMN_ID + 1)->set_data_type(ObTextType);
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {}));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 1, {id}));
}

TEST_F(SearchIndexScalarTest, appended_primary_key_is_not_a_scalar_search_column)
{
  ObSqlString comment;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_PHRASE}, comment));
  uint64_t text_id = OB_INVALID_ID;
  uint64_t value_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({comment.string()}, text_id));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, value_id));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(PRIMARY_KEY_ID, {}));
}

TEST_F(SearchIndexScalarTest, definition_candidate_keeps_only_value_columns)
{
  ObSqlString match;
  ObSqlString phrase;
  ObSqlString mixed;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_MATCH}, match));
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_PHRASE}, phrase));
  ASSERT_NO_FATAL_FAILURE(make_comment(
      {ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_PHRASE}, mixed));
  uint64_t def_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString(), match.string(), phrase.string(), mixed.string()}, def_id));
  ObSEArray<uint64_t, 4> ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> columns;
  ObSEArray<bool, 4> ignore_prefix;
  ASSERT_EQ(OB_SUCCESS, collect_indexes(ids, columns, ignore_prefix));
  ASSERT_EQ(1, ids.count());
  EXPECT_EQ(def_id, ids.at(0));
  ASSERT_EQ(1, columns.count());
  ASSERT_EQ(1, ignore_prefix.count());
  EXPECT_TRUE(ignore_prefix.at(0));
  ASSERT_EQ(2, columns.at(0).count());
  EXPECT_EQ(OB_APP_MIN_COLUMN_ID, columns.at(0).at(0));
  EXPECT_EQ(OB_APP_MIN_COLUMN_ID + 3, columns.at(0).at(1));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 1, {}));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 2, {}));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 3, {def_id}));
}

TEST_F(SearchIndexScalarTest, definition_candidate_reads_value_configuration)
{
  ObSqlString text;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_MATCH}, text));
  uint64_t text_def = OB_INVALID_ID;
  uint64_t value_def = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({text.string()}, text_def));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString("INCLUDE_TYPES=11json_number")}, value_def));
  ObSEArray<uint64_t, 4> ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> columns;
  ObSEArray<bool, 4> ignore_prefix;
  ASSERT_EQ(OB_SUCCESS, collect_indexes(ids, columns, ignore_prefix));
  ObHybridSearchGenerator generator(&allocator_, nullptr, &schema_guard_, nullptr,
      ids, columns, ignore_prefix, nullptr, false, nullptr);
  ObColumnRefRawExpr column(500001, OB_APP_MIN_COLUMN_ID, T_REF_COLUMN);
  bool has_search_index = false;
  uint8_t mask = 0;
  ASSERT_EQ(OB_SUCCESS, generator.get_search_index_cons_encode_type(&column, has_search_index, mask));
  EXPECT_TRUE(has_search_index);
  EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_NUMBER), mask);

  // A FULLTEXT-only Def remains in the whitelist but provides no scalar type coverage.
  index_ids_.reuse();
  ASSERT_EQ(OB_SUCCESS, index_ids_.push_back(text_def));
  ASSERT_EQ(OB_SUCCESS, collect_indexes(ids, columns, ignore_prefix));
  ASSERT_EQ(1, ids.count());
  ASSERT_EQ(OB_SUCCESS, generator.get_search_index_cons_encode_type(&column, has_search_index, mask));
  EXPECT_FALSE(has_search_index);
  EXPECT_EQ(0, mask);
}

TEST_F(SearchIndexScalarTest, definition_access_info_resolves_data_and_preserves_index_name)
{
  uint64_t def_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, def_id));
  const uint64_t data_id = get_data_id(def_id);
  ObRawExprFactory factory(allocator_);
  ObGlobalHint hint(allocator_);
  ObAddr addr;
  ObOptimizerContext ctx(nullptr, nullptr, &schema_guard_, nullptr, allocator_, nullptr,
      addr, nullptr, hint, factory, nullptr, false);
  ObSelectStmt stmt(allocator_);
  ObSelectLogPlan plan(ctx, &stmt);
  ObSEArray<ObRawExpr *, 1> filters;
  ObJoinOrderEnumIDP enumeration(plan, filters);
  ObJoinOrder order(enumeration, sql::INVALID);
  QueryRangeInfo range_info(allocator_);
  AccessPath path(500001, 500001, def_id, &order, default_asc_direction(), allocator_);
  ASSERT_EQ(OB_SUCCESS, order.set_search_index_access_info(path, range_info));
  EXPECT_EQ(def_id, path.get_index_table_id());
  EXPECT_EQ(data_id, path.search_data_index_id_);
  path.est_cost_info_.index_meta_info_.is_search_index_ = true;
  ObScalarQueryNode node(allocator_);
  node.has_valid_index_ = true;
  node.ap_ = &path;
  IndexMergePath merge_path(500001, 500001, &order, allocator_);
  merge_path.root_ = &node;
  ObSEArray<uint64_t, 4> ids;
  ASSERT_EQ(OB_SUCCESS, merge_path.get_all_index_ids(ids));
  ASSERT_EQ(2, ids.count());
  EXPECT_EQ(def_id, ids.at(0));
  EXPECT_EQ(data_id, ids.at(1));
  ObLogTableScan scan(plan);
  scan.set_table_id(500001);
  scan.set_ref_table_id(500001);
  scan.set_access_path(&merge_path);
  ObSEArray<ObString, 4> names;
  ASSERT_EQ(OB_SUCCESS, scan.get_index_merge_name_list(names));
  ASSERT_EQ(1, names.count());
  EXPECT_EQ(ObString("scalar_candidate"), names.at(0));
}

TEST_F(SearchIndexScalarTest, direct_data_table_rowkeys_keep_logical_table_ids)
{
  uint64_t def_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, def_id));
  const uint64_t data_id = get_data_id(def_id);
  const ObTableSchema *data_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(data_id, data_schema));
  ASSERT_NE(nullptr, data_schema);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ObSqlCtx sql_ctx;
  ObExecContext exec_ctx(allocator_);
  exec_ctx.set_my_session(&session);
  exec_ctx.set_sql_ctx(&sql_ctx);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  ObRawExprFactory factory(allocator_);
  ObGlobalHint hint(allocator_);
  ObAddr addr;
  ParamStore params;
  ObStmtFactory *stmt_factory = exec_ctx.get_stmt_factory();
  ASSERT_NE(nullptr, stmt_factory);
  ObQueryCtx *query_ctx = stmt_factory->get_query_ctx();
  ASSERT_NE(nullptr, query_ctx);
  ObSelectStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, stmt_factory->create_stmt(stmt));
  stmt->set_query_ctx(query_ctx);
  TableItem left(allocator_);
  TableItem right(allocator_);
  left.table_id_ = 500001;
  right.table_id_ = 500002;
  left.alias_name_ = "d1";
  right.alias_name_ = "d2";
  for (auto *table : {&left, &right}) {
    table->ref_id_ = data_id;
    table->type_ = TableItem::ALIAS_TABLE;
    table->table_name_ = "search_data";
    ASSERT_EQ(OB_SUCCESS, stmt->add_table_item(&session, table));
  }
  ObOptimizerContext ctx(&session, &exec_ctx, &schema_guard_, nullptr, allocator_, &params,
      addr, nullptr, hint, factory, stmt, false, query_ctx);
  ObSelectLogPlan plan(ctx, stmt);
  ObSEArray<ObRawExpr *, 1> predicates;
  ObJoinOrderEnumIDP enumeration(plan, predicates);
  ObJoinOrder order(enumeration, sql::INVALID);
  // Direct scans of the same physical Data table must retain each SQL reference's identity.
  for (auto *table : {&left, &right}) {
    ObSEArray<ColumnItem, 4> range_columns;
    ASSERT_EQ(OB_SUCCESS, plan.get_index_column_items(factory, table->table_id_,
        *data_schema, range_columns));
    ASSERT_EQ(4, range_columns.count());
    ObSEArray<ObRawExpr *, 4> keys;
    ObSEArray<ObRawExpr *, 4> ordering;
    ObOrderDirection direction = UNORDERED;
    bool force_direction = false;
    ASSERT_EQ(OB_SUCCESS, order.get_access_path_ordering(table->table_id_, data_id, data_id,
        keys, ordering, direction, force_direction, false, false));
    ASSERT_EQ(4, keys.count());
    for (auto *key : keys) {
      ASSERT_TRUE(key->is_column_ref_expr());
      EXPECT_EQ(table->table_id_, static_cast<ObColumnRefRawExpr *>(key)->get_table_id());
    }
  }
  const ColumnItem *left_column = plan.get_column_item_by_id(left.table_id_, OB_APP_MIN_COLUMN_ID);
  const ColumnItem *right_column = plan.get_column_item_by_id(right.table_id_, OB_APP_MIN_COLUMN_ID);
  ASSERT_NE(nullptr, left_column);
  ASSERT_NE(nullptr, right_column);
  EXPECT_NE(left_column->expr_, right_column->expr_);
}

TEST_F(SearchIndexScalarTest, data_table_for_another_column_does_not_cover_fulltext_column)
{
  ObSqlString text_comment;
  ObSqlString value_comment;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_PHRASE}, text_comment));
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::VALUE}, value_comment));
  uint64_t index_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({text_comment.string(), value_comment.string()}, index_id,
      INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL, true));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {}));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 1, {index_id}));
  ObColumnRefRawExpr text_col(500001, OB_APP_MIN_COLUMN_ID, T_REF_COLUMN);
  ObColumnRefRawExpr value_col(500001, OB_APP_MIN_COLUMN_ID + 1, T_REF_COLUMN);
  ObOpRawExpr filter(allocator_);
  filter.set_expr_type(T_OP_EQ);
  ASSERT_EQ(OB_SUCCESS, filter.set_param_exprs(&text_col, &value_col));
  ASSERT_NO_FATAL_FAILURE(expect_candidates(filter, {}));
}

TEST_F(SearchIndexScalarTest, skip_fulltext_candidate_and_keep_other_value_index)
{
  ObSqlString text_comment;
  ObSqlString value_comment;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_MATCH}, text_comment));
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::VALUE}, value_comment));
  uint64_t text_id = OB_INVALID_ID;
  uint64_t value_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({text_comment.string()}, text_id));
  ASSERT_NO_FATAL_FAILURE(add_candidate({value_comment.string()}, value_id));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {value_id}));
  // With VALUE first, the early check stops there; collection must still skip the later FULLTEXT.
  std::swap(index_ids_.at(0), index_ids_.at(1));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {value_id}));
}

TEST_F(SearchIndexScalarTest, legacy_value_comments_remain_eligible)
{
  uint64_t empty_id = OB_INVALID_ID;
  uint64_t filter_id = OB_INVALID_ID;
  uint64_t plain_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, empty_id, INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString("INCLUDE_PATHS=3$.x")}, filter_id));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString("user column comment")}, plain_id));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {empty_id, filter_id, plain_id}));
}

TEST_F(SearchIndexScalarTest, normal_index_and_primary_table_keep_prefix_rules)
{
  uint64_t normal_id = OB_INVALID_ID;
  uint64_t primary_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString(), ObString()}, normal_id, INDEX_TYPE_NORMAL_LOCAL));
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString(), ObString()}, primary_id, INDEX_TYPE_IS_NOT));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {normal_id, primary_id}));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 1, {}));
  ObColumnRefRawExpr first(500001, OB_APP_MIN_COLUMN_ID, T_REF_COLUMN);
  ObColumnRefRawExpr second(500001, OB_APP_MIN_COLUMN_ID + 1, T_REF_COLUMN);
  ObOpRawExpr filter(allocator_);
  filter.set_expr_type(T_OP_EQ);
  ASSERT_EQ(OB_SUCCESS, filter.set_param_exprs(&first, &second));
  ASSERT_NO_FATAL_FAILURE(expect_candidates(filter, {normal_id, primary_id}));
}

TEST_F(SearchIndexScalarTest, malformed_config_is_an_error)
{
  uint64_t index_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString("SEARCH_INDEX_CONFIG=1:")}, index_id));
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {}, OB_INVALID_ARGUMENT));
}

TEST_F(SearchIndexScalarTest, missing_schema_or_column_is_an_error)
{
  uint64_t index_id = OB_INVALID_ID;
  ASSERT_NO_FATAL_FAILURE(add_candidate({ObString()}, index_id));
  const ObTableSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, schema_guard_.get_table_schema(index_id, schema));
  ASSERT_NE(nullptr, schema);
  ObRowkeyColumn *index_column = const_cast<ObRowkeyColumn *>(schema->get_index_info().get_column(0));
  ASSERT_NE(nullptr, index_column);
  // Simulate an inconsistent schema whose declared index column cannot be found.
  index_column->column_id_ = OB_APP_MIN_COLUMN_ID + 1;
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID + 1, {}, OB_ERR_UNEXPECTED));
  index_ids_.at(0) = schema_guard_.get_next_mocked_schema_id();
  ASSERT_NO_FATAL_FAILURE(expect_column_candidates(OB_APP_MIN_COLUMN_ID, {}, OB_ERR_UNEXPECTED));
}

TEST_F(SearchIndexScalarTest, fulltext_only_column_cannot_activate_range_key)
{
  for (auto mode : {ObSearchIndexModeConfig::FULLTEXT_MATCH, ObSearchIndexModeConfig::FULLTEXT_PHRASE}) {
    ObSqlString comment;
    ASSERT_NO_FATAL_FAILURE(make_comment({mode}, comment));
    uint64_t index_id = OB_INVALID_ID;
    ASSERT_NO_FATAL_FAILURE(add_candidate({comment.string()}, index_id));
    ObSearchIndexRangeCtx range_ctx;
    ASSERT_NO_FATAL_FAILURE(init_range_ctx(index_id, range_ctx));
    ASSERT_EQ(1, range_ctx.col_ctxs_.count());
    EXPECT_EQ(OB_INVALID_INDEX, range_ctx.col_ctxs_.at(0).column_idx_);
    ObQueryRangeCtx query_ctx(nullptr);
    query_ctx.search_index_range_ctx_ = &range_ctx;
    ObExprRangeConverter converter(allocator_, query_ctx);
    int64_t key_idx = OB_INVALID_INDEX;
    EXPECT_FALSE(converter.is_range_key(OB_APP_MIN_COLUMN_ID, key_idx));
    EXPECT_EQ(OB_INVALID_INDEX, range_ctx.column_idx_);
  }
}

TEST_F(SearchIndexScalarTest, range_key_activation_skips_fulltext_between_value_columns)
{
  ObSqlString value_comment;
  ObSqlString text_comment;
  ObSqlString mixed_comment;
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::VALUE}, value_comment));
  ASSERT_NO_FATAL_FAILURE(make_comment({ObSearchIndexModeConfig::FULLTEXT_PHRASE}, text_comment));
  ASSERT_NO_FATAL_FAILURE(make_comment(
      {ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_MATCH}, mixed_comment));
  // Exercise both persisted VALUE and legacy default VALUE through the real range-context initializer.
  for (const ObString &first_comment : {value_comment.string(), ObString()}) {
    uint64_t index_id = OB_INVALID_ID;
    ASSERT_NO_FATAL_FAILURE(add_candidate({first_comment, text_comment.string(), mixed_comment.string()},
        index_id, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL, true));
    ObSearchIndexRangeCtx range_ctx;
    ASSERT_NO_FATAL_FAILURE(init_range_ctx(index_id, range_ctx));
    ASSERT_EQ(3, range_ctx.col_ctxs_.count());
    EXPECT_EQ(OB_INVALID_INDEX, range_ctx.col_ctxs_.at(1).column_idx_);
    ObQueryRangeCtx query_ctx(nullptr);
    query_ctx.search_index_range_ctx_ = &range_ctx;
    ObExprRangeConverter converter(allocator_, query_ctx);
    int64_t key_idx = OB_INVALID_INDEX;
    ASSERT_TRUE(converter.is_range_key(OB_APP_MIN_COLUMN_ID, key_idx));
    EXPECT_EQ(1, range_ctx.column_idx_); // The base primary key occupies store position 0.
    EXPECT_FALSE(converter.is_range_key(OB_APP_MIN_COLUMN_ID + 1, key_idx));
    EXPECT_EQ(OB_APP_MIN_COLUMN_ID, range_ctx.column_id());
    EXPECT_EQ(1, range_ctx.column_idx_);
    ASSERT_TRUE(converter.is_range_key(OB_APP_MIN_COLUMN_ID + 2, key_idx));
    EXPECT_EQ(OB_APP_MIN_COLUMN_ID + 2, range_ctx.column_id());
    EXPECT_EQ(3, range_ctx.column_idx_);
    EXPECT_FALSE(converter.is_range_key(OB_APP_MIN_COLUMN_ID + 3, key_idx));
    EXPECT_EQ(OB_APP_MIN_COLUMN_ID + 2, range_ctx.column_id());
    EXPECT_EQ(3, range_ctx.column_idx_);
  }
}
} // namespace

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_file_name("test_search_index_scalar.log", true);
  ObLogger::get_logger().set_log_level("WARN");
  ObLogger::get_logger().set_enable_async_log(false);
  ObCompatModeGetter::instance().init(nullptr);
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
  ObCompatModeGetter::instance().set_tenant_compat_mode(1001, mode);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
