/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <memory>
#include <utility>
#define private public
#define protected public
#include "common/ob_tenant_data_version_mgr.h"
#include "rootserver/ddl_task/ob_drop_search_index_task.h"
#include "rootserver/ddl_task/ob_fts_index_build_task.h"
#include "rootserver/parallel_ddl/ob_create_table_helper.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_ddl_service_launcher.h"
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/search_index/ob_search_index_config.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "share/ob_index_builder_util.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "share/ob_get_compat_mode.h"
#undef protected
#undef private
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/string/ob_hex_utils_base.h"
#include "share/search_index/ob_search_index_generator.h"
#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/ob_i_store.h"
#include "lib/utility/ob_defer.h"
#include "rootserver/ob_root_service.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

namespace
{
void make_config(ObSqlString &comment, const bool phrase, const char *path = nullptr,
                 const char *parser_name = "space.1")
{
  ObSearchIndexConfig config;
  ObSearchIndexModeConfig mode;
  mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE
                      : ObSearchIndexModeConfig::FULLTEXT_MATCH;
  mode.parser_name_ = ObString::make_string(parser_name);
  mode.parser_properties_ = "{}";
  ObSqlString filter;
  if (path != nullptr) {
    ASSERT_EQ(OB_SUCCESS, filter.append_fmt("INCLUDE_PATHS=%ld%s", strlen(path), path));
  }
  ASSERT_EQ(OB_SUCCESS, filter.append("INCLUDE_TYPES=11json_string"));
  mode.filter_ = filter.string();
  ObSEArray<ObSearchIndexModeConfig, 1> modes;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ASSERT_EQ(OB_SUCCESS, config.encode(comment));
}

void add_column_arg(ObCreateIndexArg &arg, const char *name, const ObString &comment)
{
  ObColumnSortItem column;
  column.column_name_ = ObString::make_string(name);
  column.column_comment_ = comment;
  ASSERT_EQ(OB_SUCCESS, arg.index_columns_.push_back(column));
}

void make_table(ObTableSchema &table, const uint64_t id, const ObIndexType type)
{
  table.reset();
  table.set_tenant_id(1001);
  table.set_database_id(1002);
  table.set_tablegroup_id(1003);
  table.set_table_id(id);
  table.set_schema_version(1);
  table.set_table_type(type == INDEX_TYPE_IS_NOT ? USER_TABLE : USER_INDEX);
  table.set_index_type(type);
  table.set_data_table_id(type == INDEX_TYPE_IS_NOT ? 0 : 500001);
  table.set_charset_type(CHARSET_UTF8MB4);
  table.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table.set_max_used_column_id(OB_APP_MIN_COLUMN_ID - 1);
  ASSERT_EQ(OB_SUCCESS, table.set_table_name("test_search_text"));
  if (type == INDEX_TYPE_IS_NOT) {
    ObColumnSchemaV2 pk;
    pk.set_tenant_id(1001);
    pk.set_table_id(id);
    pk.set_column_id(OB_APP_MIN_COLUMN_ID);
    ASSERT_EQ(OB_SUCCESS, pk.set_column_name("id"));
    pk.set_data_type(ObUInt64Type);
    pk.set_collation_type(CS_TYPE_BINARY);
    pk.set_nullable(false);
    pk.set_rowkey_position(1);
    ASSERT_EQ(OB_SUCCESS, table.add_column(pk));
    table.set_rowkey_column_num(1);
  }
}

TEST(SearchIndexDDL, required_children_are_deduplicated_across_columns)
{
  for (int mask = 1; mask < 8; ++mask) {
    ObArenaAllocator allocator;
    ObCreateIndexArg arg;
    arg.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
    arg.index_name_ = "idx";
    ObSqlString match;
    ObSqlString phrase;
    ASSERT_NO_FATAL_FAILURE(make_config(match, false));
    ASSERT_NO_FATAL_FAILURE(make_config(phrase, true));
    SCOPED_TRACE(mask);
    if (mask & 4) {
      ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "value", ObString()));
    }
    if (mask & 1) {
      ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "m1", match.string()));
      ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "m2", match.string()));
    }
    if (mask & 2) {
      ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "p1", phrase.string()));
      ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "p2", phrase.string()));
    }
    ObSearchIndexRequirements requirements;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
    ObSEArray<ObCreateIndexArg, 3> children;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::append_search_index_arg(arg, requirements, &allocator, children));
    ASSERT_EQ(!!(mask & 4) + !!(mask & 1) + !!(mask & 2), children.count());
    int64_t child_pos = 0;
    if (mask & 4) {
      EXPECT_EQ(INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, children.at(child_pos++).index_type_);
    }
    if (mask & 1) {
      EXPECT_EQ(INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, children.at(child_pos).index_type_);
      EXPECT_EQ(ObString("idx_search_text_match"), children.at(child_pos++).index_name_);
    }
    if (mask & 2) {
      EXPECT_EQ(INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, children.at(children.count() - 1).index_type_);
      EXPECT_EQ(ObString("idx_search_text_match_phrase"), children.at(children.count() - 1).index_name_);
    }
    EXPECT_EQ((mask & 4) != 0, requirements.has_data_);
    EXPECT_EQ((mask & 1) != 0, requirements.has_text_match_);
    EXPECT_EQ((mask & 2) != 0, requirements.has_text_match_phrase_);
    ObFtsIndexBuildTask task;
    task.task_type_ = DDL_CREATE_ASYNC_SEARCH_INDEX;
    task.parallelism_ = 1;
    task.data_format_version_ = DATA_VERSION_4_5_1_0;
    task.target_object_id_ = 500001;
    ASSERT_EQ(OB_SUCCESS, task.create_index_arg_.assign(arg));
    for (int64_t i = 0; i < children.count(); ++i) {
      ObCreateIndexArg child;
      ASSERT_EQ(OB_SUCCESS, task.construct_create_index_arg(children.at(i).index_type_, child, allocator));
      EXPECT_EQ(children.at(i).index_type_, child.index_type_);
      EXPECT_EQ(children.at(i).index_name_, child.index_name_);
      EXPECT_EQ(500001, child.def_index_id_);
    }
    EXPECT_EQ(OB_SUCCESS,
        ObSearchIndexBuilderUtil::resolve_search_index_args(arg));
    // FULLTEXT is gated independently of the existing VALUE-only Search Index.
    const uint64_t data_versions[] = {
      DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_2_2, DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1
    };
    for (const uint64_t data_version : data_versions) {
      SCOPED_TRACE(data_version);
      const int expected = (mask & 3) != 0 && data_version < DATA_VERSION_4_6_3_1
          ? OB_NOT_SUPPORTED : OB_SUCCESS;
      EXPECT_EQ(expected, ObSearchIndexBuilderUtil::check_fulltext_data_version(arg, data_version));
      for (int64_t i = 0; i < children.count(); ++i) {
        EXPECT_EQ(expected, ObSearchIndexBuilderUtil::check_fulltext_data_version(children.at(i), data_version));
      }
    }
  }
}

TEST(SearchIndexDDL, effective_modes_across_blocks)
{
  ObSqlString match;
  ObSqlString phrase;
  ASSERT_NO_FATAL_FAILURE(make_config(match, false, "$.title"));
  ASSERT_NO_FATAL_FAILURE(make_config(phrase, true, "$.body", "ngram.1"));
  ObSearchIndexConfig config;
  ObSearchIndexConfig second;
  ASSERT_EQ(OB_SUCCESS, config.decode(match.string()));
  ASSERT_EQ(OB_SUCCESS, second.decode(phrase.string()));
  ObSEArray<ObSearchIndexModeConfig, 3> modes;
  ASSERT_EQ(OB_SUCCESS, modes.assign(config.get_modes()));
  ASSERT_EQ(OB_SUCCESS, modes.push_back(second.get_modes().at(0)));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  ObCreateIndexArg arg;
  arg.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
  ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", encoded.string()));
  ObSearchIndexRequirements requirements;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  EXPECT_FALSE(requirements.has_data_);
  EXPECT_TRUE(requirements.has_text_match_);
  EXPECT_TRUE(requirements.has_text_match_phrase_);

  ObSearchIndexModeConfig value;
  value.mode_ = ObSearchIndexModeConfig::VALUE;
  // init replaces the owned comment, so refresh views before building the next configuration.
  ASSERT_EQ(OB_SUCCESS, modes.assign(config.get_modes()));
  ASSERT_EQ(OB_SUCCESS, modes.push_back(value));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  encoded.reset();
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  arg.index_columns_.reset();
  ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", encoded.string()));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  EXPECT_TRUE(requirements.has_data_);
  EXPECT_TRUE(requirements.has_text_match_);
  EXPECT_TRUE(requirements.has_text_match_phrase_);

  ObSearchIndexConfig numeric;
  ObSearchIndexModeConfig mode;
  mode.mode_ = ObSearchIndexModeConfig::VALUE;
  mode.filter_ = "INCLUDE_TYPES=11json_number";
  modes.reset();
  ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
  ASSERT_EQ(OB_SUCCESS, numeric.init(modes));
  encoded.reset();
  ASSERT_EQ(OB_SUCCESS, numeric.encode(encoded));
  arg.index_columns_.reset();
  ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", encoded.string()));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  EXPECT_TRUE(requirements.has_data_);
  EXPECT_FALSE(requirements.has_text_match_);
  EXPECT_FALSE(requirements.has_text_match_phrase_);
  EXPECT_FALSE(requirements.has_fulltext_);
}

TEST(SearchIndexDDL, text_schema_columns_and_layout)
{
  ObTableSchema base;
  ASSERT_NO_FATAL_FAILURE(make_table(base, 500000, INDEX_TYPE_IS_NOT));
  // JSON tokens must not inherit the base table's default charset or collation.
  base.set_charset_type(CHARSET_LATIN1);
  base.set_collation_type(CS_TYPE_LATIN1_SWEDISH_CI);
  ASSERT_TRUE(base.is_valid());
  for (bool phrase : {false, true}) {
    ObCreateIndexArg arg;
    arg.index_type_ = phrase ? INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
                            : INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL;
    ObSqlString comment;
    ASSERT_NO_FATAL_FAILURE(make_config(comment, phrase));
    ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", comment.string()));
    ObTableSchema text;
    ASSERT_NO_FATAL_FAILURE(make_table(text, 500002, arg.index_type_));
    // Simple schemas identify the layout before physical columns are generated.
    ObSimpleTableSchemaV2 simple;
    simple.set_index_type(arg.index_type_);
    EXPECT_EQ(phrase, simple.is_async_search_text_match_phrase_index());
    EXPECT_EQ(!phrase, simple.is_async_search_text_match_index());
    EXPECT_TRUE(simple.is_search_aux_index());
    EXPECT_TRUE(simple.should_not_validate_data_index_ckm());
    EXPECT_EQ(phrase, text.is_async_search_text_match_phrase_index());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(arg, base, text));
    ASSERT_EQ(OB_SUCCESS, text.check_valid(true));
    ASSERT_EQ(phrase ? 7 : 6, text.get_column_count());
    ASSERT_EQ(4, text.get_rowkey_column_num());
    for (int64_t i = 0; i < text.get_column_count(); ++i) {
      const ObColumnSchemaV2 *column = text.get_column_schema_by_idx(i);
      ASSERT_NE(nullptr, column);
      EXPECT_EQ(OB_APP_MIN_COLUMN_ID + i, column->get_column_id());
      EXPECT_EQ(i < 4 ? i + 1 : 0, column->get_rowkey_position());
      EXPECT_EQ(ObOrderType::ASC, column->get_order_in_rowkey());
      EXPECT_EQ(0, column->get_column_flags());
      EXPECT_FALSE(column->is_generated_column());
      EXPECT_EQ(i == 1 || i == 2 || i == 6 ? ObVarcharType : ObUInt64Type, column->get_data_type());
      EXPECT_EQ(CS_TYPE_BINARY,
                column->get_collation_type());
      if (i == blocksstable::SEARCH_TEXT_TOKEN) {
        EXPECT_EQ(14256, column->get_data_length());
        int64_t token_bytes = 0;
        ASSERT_EQ(OB_SUCCESS, column->get_byte_length(token_bytes, false, false));
        EXPECT_EQ(14256, token_bytes);
        EXPECT_LE(token_bytes + SEARCH_INDEX_PATH_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
      }
    }
    EXPECT_TRUE(text.is_async_search_text_index());
    EXPECT_EQ(phrase, text.is_async_search_text_match_phrase_index());
    EXPECT_TRUE(text.is_search_aux_index());
    EXPECT_TRUE(text.is_async_search_aux_index());
    EXPECT_TRUE(text.is_async_index());
    EXPECT_TRUE(text.is_index_local_storage());
    EXPECT_TRUE(text.should_not_validate_data_index_ckm());
    EXPECT_FALSE(text.is_search_data_index());
    EXPECT_EQ(500001, text.get_data_table_id());
    text.set_part_level(PARTITION_LEVEL_ONE);
    text.get_part_option().set_part_num(2);
    EXPECT_TRUE(text.need_partition_key_for_build_local_index(base));
    text.set_part_level(PARTITION_LEVEL_ZERO);
    text.get_part_option().set_part_num(1);
    const int64_t size = text.get_serialize_size();
    std::vector<char> buffer(size);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, text.serialize(buffer.data(), size, pos));
    ObTableSchema restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(buffer.data(), size, pos));
    EXPECT_EQ(OB_SUCCESS, restored.check_valid(true));
    EXPECT_EQ(arg.index_type_, restored.get_index_type());
    EXPECT_EQ(phrase, restored.is_async_search_text_match_phrase_index());
    EXPECT_EQ(text.get_data_table_id(), restored.get_data_table_id());
    EXPECT_EQ(text.get_column_count(), restored.get_column_count());
    for (int64_t i = 0; i < restored.get_column_count(); ++i) {
      EXPECT_EQ(0, restored.get_column_schema_by_idx(i)->get_column_flags());
      EXPECT_EQ(OB_APP_MIN_COLUMN_ID + i, restored.get_column_schema_by_idx(i)->get_column_id());
      EXPECT_EQ(CS_TYPE_BINARY,
                restored.get_column_schema_by_idx(i)->get_collation_type());
      if (i == blocksstable::SEARCH_TEXT_TOKEN) {
        EXPECT_EQ(text.get_column_schema_by_idx(i)->get_charset_type(),
                  restored.get_column_schema_by_idx(i)->get_charset_type());
        EXPECT_EQ(14256, restored.get_column_schema_by_idx(i)->get_data_length());
      }
    }
    // The actual DDL Schema must be accepted by the generator and writer after serialization.
    for (const ObTableSchema *schema : {&text, &restored}) {
      ObSearchIndexRowTarget target;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexRowGenerator::get_target(*schema, target));
      EXPECT_EQ(phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH, target);
      blocksstable::ObInvIdxRowDesc row_desc;
      ASSERT_EQ(OB_SUCCESS, row_desc.init(*schema, base));
      const int64_t value_start = blocksstable::SEARCH_TEXT_ROWKEY_CNT
          + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      EXPECT_EQ(blocksstable::SEARCH_TEXT_DOC_ID, row_desc.doc_id_col_idx_);
      EXPECT_EQ(value_start, row_desc.token_freq_col_idx_);
      EXPECT_EQ(value_start + 1, row_desc.doc_length_col_idx_);
      EXPECT_EQ(phrase ? value_start + 2 : -1, row_desc.pos_list_col_idx_);
    }
  }
}

TEST(SearchIndexDDL, inline_mapping_keeps_def_ownership_and_shared_delete)
{
  // Seven async combinations plus the legacy synchronous VALUE case.
  for (int combination = 0; combination < 8; ++combination) {
    const bool sync = combination == 0;
    const int mask = sync ? 4 : combination;
    // -1 keeps every child; other cases omit one required child from the second Def.
    for (int omitted = -1; omitted < 3; ++omitted) {
      const int bits[] = {4, 1, 2};
      if (omitted >= 0 && !(mask & bits[omitted])) {
        continue;
      }
      SCOPED_TRACE(::testing::Message() << "sync=" << sync << " mask=" << mask << " omitted=" << omitted);
      ObCreateTableArg arg;
      ObCreateTableRes res;
      ObCreateTableHelper helper(nullptr, 1001, arg, res);
      ObTableSchema table;
      ObSqlString match;
      ObSqlString phrase;
      ASSERT_NO_FATAL_FAILURE(make_config(match, false));
      ASSERT_NO_FATAL_FAILURE(make_config(phrase, true));
      ASSERT_NO_FATAL_FAILURE(make_table(table, 500000, INDEX_TYPE_IS_NOT));
      ASSERT_EQ(OB_SUCCESS, helper.new_tables_.push_back(table));
      const char *names[] = {"first", "second"};
      const char *suffixes[] = {"_search_data", "_search_text_match", "_search_text_match_phrase"};
      const ObIndexType types[] = {
        sync ? INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL : INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
        INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL};
      for (int64_t i = 0; i < 2; ++i) {
        ObCreateIndexArg def_arg;
        def_arg.index_type_ = sync ? INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL : INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
        def_arg.index_name_ = ObString::make_string(names[i]);
        if (mask & 4) {
          ASSERT_NO_FATAL_FAILURE(add_column_arg(def_arg, "value", ObString()));
        }
        if (mask & 1) {
          ASSERT_NO_FATAL_FAILURE(add_column_arg(def_arg, "match", match.string()));
        }
        if (mask & 2) {
          ASSERT_NO_FATAL_FAILURE(add_column_arg(def_arg, "phrase", phrase.string()));
        }
        ASSERT_EQ(OB_SUCCESS, arg.index_arg_list_.push_back(def_arg));
        ASSERT_NO_FATAL_FAILURE(make_table(table, 500001 + i * 4, def_arg.index_type_));
        table.set_data_table_id(500000);
        ObString stored_name;
        ASSERT_EQ(OB_SUCCESS, ObTableSchema::build_index_table_name(
            helper.allocator_, 500000, def_arg.index_name_, stored_name));
        ASSERT_EQ(OB_SUCCESS, table.set_table_name(stored_name));
        ASSERT_EQ(OB_SUCCESS, table.generate_origin_index_name());
        ASSERT_EQ(OB_SUCCESS, helper.new_tables_.push_back(table));
        for (int64_t j = 0; j < 3; ++j) {
          if (!(mask & bits[j]) || (i == 1 && j == omitted)) {
            continue;
          }
          ASSERT_NO_FATAL_FAILURE(make_table(table, 500002 + i * 4 + j, types[j]));
          if (j > 0) {
            ObCreateIndexArg text_arg;
            ASSERT_EQ(OB_SUCCESS, text_arg.assign(def_arg));
            text_arg.index_type_ = types[j];
            ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(
                text_arg, helper.new_tables_.at(0), table));
          }
          table.set_data_table_id(500000); // before mapping every inline index references the base
          const std::string name = std::string(names[i]) + suffixes[j];
          ASSERT_EQ(OB_SUCCESS, ObTableSchema::build_index_table_name(
              helper.allocator_, 500000, ObString::make_string(name.c_str()), stored_name));
          ASSERT_EQ(OB_SUCCESS, table.set_table_name(stored_name));
          ASSERT_EQ(OB_SUCCESS, table.generate_origin_index_name());
          ASSERT_EQ(OB_SUCCESS, helper.new_tables_.push_back(table));
        }
      }
      if (!sync) {
        ASSERT_NO_FATAL_FAILURE(make_table(table, 500009, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
        table.set_data_table_id(500000);
        ASSERT_EQ(OB_SUCCESS, helper.new_tables_.push_back(table));
      }
      if (omitted >= 0) {
        EXPECT_EQ(OB_ERR_UNEXPECTED, helper.build_search_index_mapping_(arg.index_arg_list_));
      } else {
        ASSERT_EQ(OB_SUCCESS, helper.build_search_index_mapping_(arg.index_arg_list_));
        for (int64_t i = 1; i < helper.new_tables_.count(); ++i) {
          const ObTableSchema &child = helper.new_tables_.at(i);
          if (child.is_search_aux_index()) {
            const bool first = child.get_table_id() < 500005;
            EXPECT_EQ(first ? 500001 : 500005, child.get_data_table_id());
            uint64_t host = OB_INVALID_ID;
            EXPECT_EQ(OB_SUCCESS, helper.resolve_host_id_(child, host));
            EXPECT_EQ(500000, host);
            const int role = child.is_search_data_index() ? 0 : child.is_async_search_text_match_index() ? 1 : 2;
            ObString name;
            ASSERT_EQ(OB_SUCCESS, child.get_index_name(name));
            EXPECT_EQ(std::string(names[first ? 0 : 1]) + suffixes[role],
                      std::string(name.ptr(), name.length()));
          } else {
            EXPECT_EQ(500000, child.get_data_table_id());
          }
        }
      }
    }
  }
}

TEST(SearchIndexDDL, drop_recognizes_all_children_and_partial_creation)
{
  ObDDLService service;
  ObIndexBuilder builder(service);
  ObSEArray<ObTableSchema, 5> schemas;
  const ObIndexType types[] = {INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL,
    INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
    INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL};
  int64_t index = -1, def = -1, data = -1, del = -1;
  int64_t text_match = -1, text_match_phrase = -1;
  for (int64_t i = 0; i < 5; ++i) {
    ObTableSchema schema;
    ASSERT_NO_FATAL_FAILURE(make_table(schema, 500001 + i, types[i]));
    if (i < 2) {
      ObTableSchema base;
      ASSERT_NO_FATAL_FAILURE(make_table(base, 500000, INDEX_TYPE_IS_NOT));
      ObCreateIndexArg text_arg;
      text_arg.index_type_ = types[i];
      ObSqlString config;
      ASSERT_NO_FATAL_FAILURE(make_config(config, i == 1));
      ASSERT_NO_FATAL_FAILURE(add_column_arg(text_arg, "payload", config.string()));
      // Real heap-table DDL may reserve hidden column IDs before adding Text columns.
      schema.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + 1);
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(text_arg, base, schema));
      // Reload ordinary columns from persisted flags; roles follow the layout,
      // independent of column names and the starting column ID.
      for (int64_t j = 0; j < schema.get_column_count(); ++j) {
        ObColumnSchemaV2 *column = schema.get_column_schema_by_idx(j);
        ASSERT_NE(nullptr, column);
        column->set_column_flags(column->get_stored_column_flags());
        ASSERT_EQ(OB_SUCCESS, ObSchemaUtils::cascaded_generated_column(schema, *column, true));
        EXPECT_EQ(0, column->get_column_flags());
        EXPECT_FALSE(column->is_generated_column());
        EXPECT_EQ(OB_APP_MIN_COLUMN_ID + 2 + j, column->get_column_id());
      }
      EXPECT_EQ(i == 1, schema.is_async_search_text_match_phrase_index());
      EXPECT_EQ(ObUInt64Type, schema.get_column_schema_by_idx(blocksstable::SEARCH_TEXT_TOKEN_FREQ)->get_data_type());
      EXPECT_EQ(ObUInt64Type, schema.get_column_schema_by_idx(blocksstable::SEARCH_TEXT_DOC_LENGTH)->get_data_type());
      if (i == 1) {
        EXPECT_EQ(ObVarcharType, schema.get_column_schema_by_idx(blocksstable::SEARCH_TEXT_POSITION_LIST)->get_data_type());
      }
    }
    ASSERT_EQ(OB_SUCCESS, schemas.push_back(schema));
  }
  ASSERT_EQ(OB_SUCCESS, builder.recognize_search_index_schemas(
      schemas, false, index, def, data, del, text_match, text_match_phrase));
  EXPECT_EQ(3, index);
  EXPECT_EQ(3, def);
  EXPECT_EQ(2, data);
  EXPECT_EQ(4, del);
  EXPECT_EQ(0, text_match);
  EXPECT_EQ(1, text_match_phrase);
  schemas.pop_back(); // shared Delete is still in use by another index
  ASSERT_EQ(OB_SUCCESS, builder.recognize_search_index_schemas(
      schemas, false, index, def, data, del, text_match, text_match_phrase));
  EXPECT_EQ(-1, del);
  ASSERT_EQ(OB_SUCCESS, schemas.at(1).assign(schemas.at(0)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, builder.recognize_search_index_schemas(
      schemas, true, index, def, data, del, text_match, text_match_phrase));
  schemas.reset();
  ObTableSchema only_def;
  ASSERT_NO_FATAL_FAILURE(make_table(only_def, 500001, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL));
  ASSERT_EQ(OB_SUCCESS, schemas.push_back(only_def));
  EXPECT_EQ(OB_SUCCESS, builder.recognize_search_index_schemas(
      schemas, true, index, def, data, del, text_match, text_match_phrase));
  EXPECT_EQ(0, def);
  EXPECT_EQ(-1, data);
  EXPECT_EQ(-1, del);
  EXPECT_EQ(-1, text_match);
  EXPECT_EQ(-1, text_match_phrase);
}

TEST(SearchIndexDDL, data_lookup_does_not_select_text)
{
  ObTableSchema def;
  def.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(ObAuxTableMetaInfo(10, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(ObAuxTableMetaInfo(11, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL)));
  ObSEArray<uint64_t, 3> text_only_ids;
  ASSERT_EQ(OB_SUCCESS, def.get_search_aux_index_tids(text_only_ids));
  ASSERT_EQ(2, text_only_ids.count());
  EXPECT_EQ(10, text_only_ids.at(0));
  EXPECT_EQ(11, text_only_ids.at(1));
  uint64_t id = 1;
  EXPECT_EQ(OB_SCHEMA_EAGAIN, def.get_search_data_index_tid(id));
  ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(ObAuxTableMetaInfo(12, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL)));
  EXPECT_EQ(OB_SUCCESS, def.get_search_data_index_tid(id));
  EXPECT_EQ(12, id);
  EXPECT_EQ(OB_SUCCESS, def.get_search_aux_index_tid(INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, id));
  EXPECT_EQ(10, id);
  EXPECT_EQ(OB_SUCCESS, def.get_search_aux_index_tid(INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, id));
  EXPECT_EQ(11, id);
  ObSEArray<uint64_t, 3> ids;
  EXPECT_EQ(OB_SUCCESS, def.get_search_aux_index_tids(ids));
  EXPECT_EQ(3, ids.count());
}

TEST(SearchIndexDDL, legacy_data_and_nested_are_preserved)
{
  ObTableSchema base;
  ASSERT_NO_FATAL_FAILURE(make_table(base, 500000, INDEX_TYPE_IS_NOT));
  for (bool async : {false, true}) {
    ObCreateIndexArg arg;
    arg.index_type_ = async ? INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL : INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL;
    ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", ObString("NESTED_PATHS=3$.a")));
    ObSearchIndexRequirements requirements;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
    EXPECT_TRUE(requirements.has_data_);
    EXPECT_TRUE(requirements.has_nested_);
    EXPECT_FALSE(requirements.has_text_match_);
    EXPECT_FALSE(requirements.has_text_match_phrase_);
    ObArenaAllocator allocator;
    ObSEArray<ObCreateIndexArg, 3> children;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::append_search_index_arg(arg, requirements, &allocator, children));
    ASSERT_EQ(1, children.count());
    EXPECT_EQ(async ? INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL : INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL,
        children.at(0).index_type_);
    arg.index_type_ = async ? INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL : INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL;
    ObTableSchema data;
    ASSERT_NO_FATAL_FAILURE(make_table(data, 500002, arg.index_type_));
    EXPECT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(arg, base, data));
    EXPECT_EQ(5, data.get_column_count());
    EXPECT_EQ(async ? 4 : 5, data.get_rowkey_column_num());
    ObTableSchema def;
    def.set_index_type(async ? INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL : INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL);
    ASSERT_EQ(OB_SUCCESS, def.add_simple_index_info(ObAuxTableMetaInfo(500002, USER_INDEX, arg.index_type_)));
    uint64_t id = OB_INVALID_ID;
    EXPECT_EQ(OB_SUCCESS, def.get_search_data_index_tid(id));
    EXPECT_EQ(500002, id);
    EXPECT_EQ(async ? OB_SCHEMA_EAGAIN : OB_INVALID_ARGUMENT,
        def.get_search_aux_index_tid(INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, id));
  }
}

TEST(SearchIndexDDL, new_definition_schemas_persist_version_compatible_comments)
{
  const uint64_t saved_data_version = ODV_MGR.mock_data_version_;
  DEFER(ODV_MGR.set_mock_data_version(saved_data_version));
  const char *default_v1 = "SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:";
  const char *nested_v1 = "SEARCH_INDEX_CONFIG=1:1:17:NESTED_PATHS=3$.a0:0:0:0:";
  const char *match_v1 = "SEARCH_INDEX_CONFIG=1:1:0:1:0:7:space.1"
      "43:{\"max_token_size\": 84, \"min_token_size\": 3}";
  const char *phrase_v1 = "SEARCH_INDEX_CONFIG=1:1:0:2:0:7:space.1"
      "43:{\"max_token_size\": 84, \"min_token_size\": 3}";
  const struct {
    const char *raw;
    const char *expected;
    const char *nested;
    ObSearchIndexModeConfig::Mode mode;
  } cases[] = {
    {"", default_v1, "", ObSearchIndexModeConfig::VALUE},
    {"ordinary user comment", default_v1, "", ObSearchIndexModeConfig::VALUE},
    {"NESTED_PATHS=3$.a", nested_v1, "NESTED_PATHS=3$.a", ObSearchIndexModeConfig::VALUE},
    {default_v1, default_v1, "", ObSearchIndexModeConfig::VALUE},
    {nested_v1, nested_v1, "NESTED_PATHS=3$.a", ObSearchIndexModeConfig::VALUE},
    {match_v1, match_v1, "", ObSearchIndexModeConfig::FULLTEXT_MATCH},
    {phrase_v1, phrase_v1, "", ObSearchIndexModeConfig::FULLTEXT_PHRASE}
  };
  for (bool async : {false, true}) {
    for (bool json : {false, true}) {
      for (const auto &item : cases) {
        const bool has_fulltext = item.mode != ObSearchIndexModeConfig::VALUE;
        if ((!json && item.nested[0] != '\0') || (has_fulltext && (!async || !json))) {
          continue;
        }
        SCOPED_TRACE(::testing::Message() << "async=" << async << " json=" << json << " raw=" << item.raw);
        ObTableSchema base;
        // No base rowkeys: isolate the version-dependent Search Def comment write.
        base.set_tenant_id(1001);
        base.set_table_id(500000);
        base.set_table_type(USER_TABLE);
        base.set_charset_type(CHARSET_UTF8MB4);
        base.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        ASSERT_EQ(OB_SUCCESS, base.set_table_name("value_config_source"));
        ObColumnSchemaV2 column;
        column.set_tenant_id(base.get_tenant_id());
        column.set_table_id(base.get_table_id());
        column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
        column.set_data_type(json ? ObJsonType : ObIntType);
        if (json) {
          column.set_data_length(ObAccuracy::MAX_ACCURACY[ObJsonType].get_length());
        }
        column.set_collation_type(json ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY);
        ASSERT_EQ(OB_SUCCESS, column.set_column_name("payload"));
        ASSERT_EQ(OB_SUCCESS, column.set_comment(ObString("base user comment")));
        ASSERT_EQ(OB_SUCCESS, base.add_column(column));
        ObCreateIndexArg arg;
        arg.index_type_ = async ? INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL : INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL;
        ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", ObString(item.raw)));
        ObSearchIndexRequirements requirements;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
        EXPECT_EQ(!has_fulltext, requirements.has_data_);
        EXPECT_EQ(has_fulltext, requirements.has_fulltext_);
        EXPECT_EQ(item.nested[0] != '\0', requirements.has_nested_);
        for (const uint64_t data_version : {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
          SCOPED_TRACE(data_version);
          ODV_MGR.set_mock_data_version(data_version);
          ObTableSchema def;
          ASSERT_NO_FATAL_FAILURE(make_table(def, 500001, arg.index_type_));
          const int ret = ObIndexBuilderUtil::set_index_table_columns(arg, base, def, false);
          if (has_fulltext && data_version < DATA_VERSION_4_6_3_1) {
            EXPECT_EQ(OB_NOT_SUPPORTED, ret);
          } else {
            ASSERT_EQ(OB_SUCCESS, ret);
            const ObColumnSchemaV2 *stored = def.get_column_schema(OB_APP_MIN_COLUMN_ID + 1);
            ASSERT_NE(nullptr, stored);
            EXPECT_EQ(ObString(data_version < DATA_VERSION_4_6_3_1 ? item.nested : item.expected),
                      stored->get_comment_str());
            ObSearchIndexConfig config;
            ASSERT_EQ(OB_SUCCESS, config.decode(stored->get_comment_str()));
            ASSERT_EQ(1, config.get_modes().count());
            EXPECT_EQ(item.mode, config.get_modes().at(0).mode_);
            EXPECT_TRUE(config.get_modes().at(0).filter_.empty());
            EXPECT_EQ(ObString(item.nested), config.get_nested_paths_filter());
            ObSqlString encoded;
            ASSERT_EQ(OB_SUCCESS, config.encode(encoded, data_version));
            EXPECT_EQ(stored->get_comment_str(), encoded.string());
            ObCreateIndexArg restored_arg;
            ASSERT_EQ(OB_SUCCESS, restored_arg.assign(arg));
            restored_arg.index_columns_.at(0).column_comment_ = encoded.string();
            EXPECT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(restored_arg));
          }
        }
      }
    }
  }
}

TEST(SearchIndexDDL, cross_column_value_nested_with_fulltext_and_synchronous_restriction)
{
  ObSqlString text;
  ASSERT_NO_FATAL_FAILURE(make_config(text, false));
  ObCreateIndexArg arg;
  arg.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
  ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "a", text.string()));
  ObSearchIndexRequirements requirements;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  EXPECT_TRUE(requirements.has_text_match_);
  EXPECT_FALSE(requirements.has_nested_);
  arg.index_type_ = INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL;
  EXPECT_EQ(OB_NOT_SUPPORTED, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  arg.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
  ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "b", ObString("NESTED_PATHS=3$.a")));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  EXPECT_TRUE(requirements.has_data_);
  EXPECT_TRUE(requirements.has_nested_);
  EXPECT_TRUE(requirements.has_text_match_);
  ObArenaAllocator allocator;
  ObSEArray<ObCreateIndexArg, 3> children;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::append_search_index_arg(arg, requirements, &allocator, children));
  ASSERT_EQ(2, children.count());
  ObTableSchema base;
  ASSERT_NO_FATAL_FAILURE(make_table(base, 500000, INDEX_TYPE_IS_NOT));
  for (const auto &child : children) {
    const bool text_child = is_search_text_index(child.index_type_);
    ObTableSchema aux;
    ASSERT_NO_FATAL_FAILURE(make_table(aux, 500002, child.index_type_));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(child, base, aux));
    EXPECT_EQ(text_child ? 6 : 5, aux.get_column_count());
    EXPECT_EQ(4, aux.get_rowkey_column_num());
  }
}

TEST(SearchIndexDDL, fulltext_only_does_not_create_data)
{
  for (bool phrase : {false, true}) {
    ObSqlString fulltext;
    ASSERT_NO_FATAL_FAILURE(make_config(fulltext, phrase));
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.decode(fulltext.string()));
    ObSqlString comment;
    ASSERT_EQ(OB_SUCCESS, config.encode(comment));
    ObCreateIndexArg arg;
    arg.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
    ASSERT_NO_FATAL_FAILURE(add_column_arg(arg, "payload", comment.string()));
    ObSearchIndexRequirements requirements;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
    EXPECT_FALSE(requirements.has_data_);
    EXPECT_FALSE(requirements.has_nested_);
    EXPECT_EQ(!phrase, requirements.has_text_match_);
    EXPECT_EQ(phrase, requirements.has_text_match_phrase_);
    ObArenaAllocator allocator;
    ObSEArray<ObCreateIndexArg, 3> children;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::append_search_index_arg(arg, requirements, &allocator, children));
    ASSERT_EQ(1, children.count());
    EXPECT_TRUE(is_search_text_index(children.at(0).index_type_));
    ObTableSchema base;
    ASSERT_NO_FATAL_FAILURE(make_table(base, 500000, INDEX_TYPE_IS_NOT));
    ObTableSchema aux;
    ASSERT_NO_FATAL_FAILURE(make_table(aux, 500002, children.at(0).index_type_));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::set_search_index_table_columns(children.at(0), base, aux));
    EXPECT_EQ(phrase ? 7 : 6, aux.get_column_count());
    EXPECT_EQ(4, aux.get_rowkey_column_num());
    arg.index_type_ = INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL;
    EXPECT_EQ(OB_NOT_SUPPORTED, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  }
}

// Keep the production record reader, type dispatch and message writer. Only the
// SQL boundary is replaced; every read sees the message saved by the last write.
class ParentTaskResult : public sqlclient::ObMySQLResultImpl
{
public:
  ParentTaskResult(sqlclient::ObMySQLStatement &stmt, const ObDDLTaskRecord &record)
      : ObMySQLResultImpl(stmt), record_(record), read_(false)
  {
    record.trace_id_.to_string(trace_id_, sizeof(trace_id_));
  }
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_uint;
  using ObMySQLResultImpl::get_varchar;
  int next() override
  {
    const int ret = read_ ? OB_ITER_END : OB_SUCCESS;
    read_ = true;
    return ret;
  }
  int get_int(const char *column, int64_t &value) const override
  {
    int ret = OB_SUCCESS;
    bool found = false;
    const std::pair<const char *, int64_t> fields[] = {
      {"create_time", static_cast<int64_t>(record_.gmt_create_)},
      {"task_id", record_.task_id_},
      {"parent_task_id", record_.parent_task_id_},
      {"tenant_id", OB_INVALID_TENANT_ID}, // tenant-local inner table
      {"object_id", static_cast<int64_t>(record_.object_id_)},
      {"schema_version", record_.schema_version_},
      {"target_object_id", static_cast<int64_t>(record_.target_object_id_)},
      {"ddl_type", record_.ddl_type_},
      {"status", record_.task_status_},
      {"task_version", record_.task_version_},
      {"ret_code", record_.ret_code_},
      {"execution_id", record_.execution_id_},
      {"consensus_schema_version", record_.consensus_schema_version_}
    };
    for (const auto &field : fields) {
      if (0 == strcmp(column, field.first)) {
        value = field.second;
        found = true;
        break;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected integer column in parent task test result", K(ret), K(column));
      ADD_FAILURE() << "Unexpected integer column: " << column;
    }
    return ret;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    EXPECT_STREQ("snapshot_version", column);
    value = record_.snapshot_version_;
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == strcmp(column, "trace_id")) {
      value = ObString::make_string(trace_id_);
    } else if (0 == strcmp(column, "message_unhex")) {
      value = record_.message_;
    } else if (0 == strcmp(column, "ddl_stmt_str_unhex")) {
      value = record_.ddl_stmt_str_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected string column in parent task test result", K(ret), K(column));
      ADD_FAILURE() << "Unexpected string column: " << column;
    }
    return ret;
  }
private:
  const ObDDLTaskRecord &record_;
  bool read_;
  char trace_id_[128] = {};
};

class ParentTaskResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  explicit ParentTaskResultHandler(const ObDDLTaskRecord &record) : result_(stmt_, record) {}
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
private:
  sqlclient::ObMySQLStatement stmt_;
  ParentTaskResult result_;
};

class ParentTaskProxy : public ObMySQLProxy
{
public:
  using ObMySQLProxy::read;
  using ObMySQLProxy::write;
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    EXPECT_EQ(record_.tenant_id_, tenant_id);
    ObSqlString expected_sql;
    EXPECT_EQ(OB_SUCCESS, expected_sql.assign_fmt(GET_DDL_TASK_SQL " WHERE task_id=%lu ",
        OB_ALL_DDL_TASK_STATUS_TNAME, record_.task_id_));
    EXPECT_STREQ(expected_sql.ptr(), sql);
    ++read_count_;
    ParentTaskResultHandler *handler = nullptr;
    return res.create_handler(handler, record_);
  }
  int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override
  {
    int ret = OB_SUCCESS;
    EXPECT_EQ(record_.tenant_id_, tenant_id);
    const std::string statement(sql);
    const std::string prefix = " UPDATE __all_ddl_task_status SET message=\"";
    const std::string suffix = "\" WHERE task_id=" + std::to_string(record_.task_id_);
    if (statement.size() <= prefix.size() + suffix.size()
        || statement.compare(0, prefix.size(), prefix) != 0
        || statement.compare(statement.size() - suffix.size(), suffix.size(), suffix) != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected parent task test message update", K(ret), K(sql));
      ADD_FAILURE() << "Unexpected parent message update: " << statement;
    } else {
      ObString hex_message;
      hex_message.assign_ptr(statement.data() + prefix.size(),
          statement.size() - prefix.size() - suffix.size());
      char *message = nullptr;
      int64_t length = 0;
      if (OB_FAIL(ObHexUtilsBase::unhex(hex_message, allocator_, message, length))) {
        LOG_WARN("failed to decode parent task test message", K(ret), K(hex_message));
      } else {
        record_.message_.assign(message, length);
        affected_rows = 1;
        ++write_count_;
      }
    }
    return ret;
  }
  ObDDLTaskRecord record_;
  ObArenaAllocator allocator_;
  int64_t read_count_ = 0;
  int64_t write_count_ = 0;
};

class SearchIndexParentMessage : public ::testing::Test
{
protected:
  void SetUp() override
  {
    saved_root_service_ = GCTX.root_service_;
    saved_sql_proxy_ = GCTX.sql_proxy_;
    saved_ddl_started_ = ObDDLServiceLauncher::is_ddl_service_started();
    root_service_.reset(new ObRootService());
    GCTX.root_service_ = root_service_.get();
    GCTX.sql_proxy_ = &proxy_;
    ATOMIC_SET(&ObDDLServiceLauncher::is_ddl_service_started_, true);
    ASSERT_EQ(OB_SUCCESS, proxy_.init(&pool_));
  }
  void TearDown() override
  {
    ATOMIC_SET(&ObDDLServiceLauncher::is_ddl_service_started_, saved_ddl_started_);
    GCTX.sql_proxy_ = saved_sql_proxy_;
    GCTX.root_service_ = saved_root_service_;
  }
  void save_parent(ObDDLTask &task)
  {
    task.tenant_id_ = task.dst_tenant_id_ = 1001;
    task.task_id_ = 123;
    task.object_id_ = 500000;
    task.target_object_id_ = 500001;
    task.schema_version_ = task.dst_schema_version_ = 10;
    task.snapshot_version_ = 1;
    task.trace_id_.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 8080));
    ASSERT_EQ(OB_SUCCESS, task.convert_to_record(proxy_.record_, allocator_));
    ASSERT_TRUE(proxy_.record_.is_valid());
  }
  void update_child(const ObIndexType type, const uint64_t table_id,
                    const int64_t task_id, const ObDDLUpdateParentTaskIDType update_type)
  {
    ObTableSchema child;
    ASSERT_NO_FATAL_FAILURE(make_table(child, table_id, type));
    const int64_t reads = proxy_.read_count_;
    const int64_t writes = proxy_.write_count_;
    ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::update_parent_task_message(
        1001, 123, child, table_id, task_id, update_type, allocator_, proxy_));
    EXPECT_EQ(reads + 1, proxy_.read_count_);
    EXPECT_EQ(writes + 1, proxy_.write_count_);
  }
  void read_parent(ObDDLTaskRecord &record)
  {
    ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::get_ddl_task_record(
        1001, 123, proxy_, allocator_, record));
  }
  ParentTaskProxy proxy_;
  ObArenaAllocator allocator_;
private:
  sqlclient::ObMySQLConnectionPool pool_;
  std::unique_ptr<ObRootService> root_service_;
  ObRootService *saved_root_service_ = nullptr;
  ObMySQLProxy *saved_sql_proxy_ = nullptr;
  bool saved_ddl_started_ = false;
};

TEST_F(SearchIndexParentMessage, create_dispatch_persists_independent_text_dependencies)
{
  for (bool has_data : {false, true}) {
    for (bool phrase_first : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "has_data=" << has_data << ", phrase_first=" << phrase_first);
      ObFtsIndexBuildTask source;
      source.task_type_ = DDL_CREATE_ASYNC_SEARCH_INDEX;
      source.task_version_ = 1;
      source.create_index_arg_.tenant_id_ = 1001;
      source.create_index_arg_.exec_tenant_id_ = 1001;
      source.create_index_arg_.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
      ASSERT_NO_FATAL_FAILURE(save_parent(source));
      if (has_data) {
        ASSERT_NO_FATAL_FAILURE(update_child(INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
            10000, 111, UPDATE_CREATE_INDEX_ID));
      }
      for (bool phrase : {phrase_first, !phrase_first}) {
        ASSERT_NO_FATAL_FAILURE(update_child(phrase ? INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
                                                   : INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL,
            phrase ? 10002 : 10001, phrase ? 654 : 321, UPDATE_CREATE_INDEX_ID));
      }
      ObDDLTaskRecord record;
      ASSERT_NO_FATAL_FAILURE(read_parent(record));
      ObFtsIndexBuildTask recovered;
      ASSERT_EQ(OB_SUCCESS, recovered.init(record));
      EXPECT_EQ(1, recovered.task_version_);
      EXPECT_EQ(10001, recovered.search_text_match_aux_table_id_);
      EXPECT_EQ(10002, recovered.search_text_match_phrase_aux_table_id_);
      EXPECT_EQ(321, recovered.search_text_match_task_id_);
      EXPECT_EQ(654, recovered.search_text_match_phrase_task_id_);
      EXPECT_TRUE(recovered.search_text_match_task_submitted_);
      EXPECT_TRUE(recovered.search_text_match_phrase_task_submitted_);
      EXPECT_EQ(has_data ? 10000 : OB_INVALID_ID, recovered.domain_index_aux_table_id_);
      EXPECT_EQ(has_data ? 111 : 0, recovered.domain_index_aux_task_id_);
      EXPECT_EQ(has_data, recovered.domain_index_aux_task_submitted_);
      EXPECT_EQ(has_data ? 3 : 2, recovered.dependent_task_result_map_.size());
      ObDomainDependTaskStatus status;
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10001, status));
      EXPECT_EQ(321, status.task_id_);
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10002, status));
      EXPECT_EQ(654, status.task_id_);
      EXPECT_EQ(has_data ? OB_SUCCESS : OB_HASH_NOT_EXIST,
          recovered.dependent_task_result_map_.get_refactored(10000, status));
      if (has_data) {
        EXPECT_EQ(111, status.task_id_);
      }
    }
  }
}

TEST_F(SearchIndexParentMessage, drop_dispatch_persists_independent_text_task_ids)
{
  for (bool has_data : {false, true}) {
    for (bool phrase_first : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "has_data=" << has_data << ", phrase_first=" << phrase_first);
      ObDropSearchIndexTask source;
      ObString data_name("data");
      ObString match_name("match");
      ObString phrase_name("phrase");
      const ObFTSDDLChildTaskInfo data = has_data
          ? ObFTSDDLChildTaskInfo(data_name, 10000, 0) : ObFTSDDLChildTaskInfo();
      ASSERT_EQ(OB_SUCCESS, source.init(1001, 123, 500000, DDL_DROP_ASYNC_SEARCH_INDEX,
          ObFTSDDLChildTaskInfo(), data, ObFTSDDLChildTaskInfo(),
          ObFTSDDLChildTaskInfo(match_name, 10001, 0),
          ObFTSDDLChildTaskInfo(phrase_name, 10002, 0),
          ObString("DROP INDEX idx ON t"), 10, 0, 500001));
      ASSERT_NO_FATAL_FAILURE(save_parent(source));
      if (has_data) {
        ASSERT_NO_FATAL_FAILURE(update_child(INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
            10000, 111, UPDATE_DROP_INDEX_TASK_ID));
      }
      for (bool phrase : {phrase_first, !phrase_first}) {
        ASSERT_NO_FATAL_FAILURE(update_child(phrase ? INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
                                                   : INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL,
            phrase ? 10002 : 10001, phrase ? 654 : 321, UPDATE_DROP_INDEX_TASK_ID));
      }
      ObDDLTaskRecord record;
      ASSERT_NO_FATAL_FAILURE(read_parent(record));
      ObDropSearchIndexTask recovered;
      ASSERT_EQ(OB_SUCCESS, recovered.init(record));
      EXPECT_EQ(3, recovered.task_version_);
      EXPECT_EQ(ObString("match"), recovered.search_text_match_index_.index_name_);
      EXPECT_EQ(ObString("phrase"), recovered.search_text_match_phrase_index_.index_name_);
      EXPECT_EQ(10001, recovered.search_text_match_index_.table_id_);
      EXPECT_EQ(10002, recovered.search_text_match_phrase_index_.table_id_);
      EXPECT_EQ(321, recovered.search_text_match_index_.task_id_);
      EXPECT_EQ(654, recovered.search_text_match_phrase_index_.task_id_);
      EXPECT_EQ(has_data, recovered.search_data_index_.is_valid());
      EXPECT_EQ(has_data ? 10000 : OB_INVALID_ID, recovered.search_data_index_.table_id_);
      EXPECT_EQ(has_data ? 111 : 0, recovered.search_data_index_.task_id_);
    }
  }
}

TEST(SearchIndexDDL, drop_task_recovers_partial_completion_and_legacy_versions)
{
  for (int64_t version = 1; version <= 3; ++version) {
    for (int64_t finished = 0; finished < 4; ++finished) {
      ObDropSearchIndexTask source;
      source.task_type_ = version == 1 ? DDL_DROP_SEARCH_INDEX : DDL_DROP_ASYNC_SEARCH_INDEX;
      source.task_version_ = version;
      ObString name("idx_search_text_match");
      ObString phrase("idx_search_text_match_phrase");
      if (version >= 3) {
        ASSERT_EQ(OB_SUCCESS, source.search_text_match_index_.deep_copy_from_other(
            ObFTSDDLChildTaskInfo(name, 10001, 111), source.allocator_));
        ASSERT_EQ(OB_SUCCESS, source.search_text_match_phrase_index_.deep_copy_from_other(
            ObFTSDDLChildTaskInfo(phrase, 10002, 123), source.allocator_));
      }
      source.drop_data_index_finish_ = true;
      source.drop_def_index_finish_ = false;
      source.drop_text_match_index_finish_ = 0 != (finished & 1);
      source.drop_text_match_phrase_index_finish_ = 0 != (finished & 2);
      const int64_t size = source.get_serialize_param_size();
      std::vector<char> buffer(size);
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
      ASSERT_EQ(size, pos);
      ObDropSearchIndexTask recovered;
      recovered.task_type_ = source.task_type_;
      recovered.task_version_ = version;
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, recovered.deserialize_params_from_message(1001, buffer.data(), size, pos));
      EXPECT_EQ(size, pos);
      EXPECT_TRUE(recovered.drop_data_index_finish_);
      EXPECT_FALSE(recovered.drop_def_index_finish_);
      EXPECT_EQ(version >= 3 && 0 != (finished & 1), recovered.drop_text_match_index_finish_);
      EXPECT_EQ(version >= 3 && 0 != (finished & 2), recovered.drop_text_match_phrase_index_finish_);
      EXPECT_EQ(version >= 3, recovered.search_text_match_index_.is_valid());
      EXPECT_EQ(version >= 3, recovered.search_text_match_phrase_index_.is_valid());
      EXPECT_EQ(version, recovered.task_version_);
      if (version >= 3) {
        ObDropSearchIndexTask truncated;
        truncated.task_type_ = source.task_type_;
        pos = 0;
        EXPECT_NE(OB_SUCCESS, truncated.deserialize_params_from_message(1001, buffer.data(), size - 1, pos));
        EXPECT_EQ(111, recovered.search_text_match_index_.task_id_);
        EXPECT_EQ(123, recovered.search_text_match_phrase_index_.task_id_);
        std::fill(buffer.begin(), buffer.end(), 0); // recovery must own names
        EXPECT_EQ(phrase, recovered.search_text_match_phrase_index_.index_name_);
        recovered.set_drop_text_match_phrase_index_task_id(456);
        EXPECT_EQ(456, recovered.search_text_match_phrase_index_.task_id_);
        EXPECT_EQ(111, recovered.search_text_match_index_.task_id_);
      }
    }
  }
}

TEST(SearchIndexDDL, drop_text_members_recover_optional_tables)
{
  std::unique_ptr<ObRootService> root_service(new ObRootService());
  auto *saved_root_service = GCTX.root_service_;
  GCTX.root_service_ = root_service.get();
  DEFER(GCTX.root_service_ = saved_root_service);
  for (int64_t modes = 0; modes < 8; ++modes) {
    ObDropSearchIndexTask source;
    std::string match_buffer("match");
    std::string phrase_buffer("phrase");
    ObString match(match_buffer.size(), match_buffer.data());
    ObString phrase(phrase_buffer.size(), phrase_buffer.data());
    const ObFTSDDLChildTaskInfo match_info = modes & 1
        ? ObFTSDDLChildTaskInfo(match, 10001, -1) : ObFTSDDLChildTaskInfo();
    const ObFTSDDLChildTaskInfo phrase_info = modes & 2
        ? ObFTSDDLChildTaskInfo(phrase, 10002, 123) : ObFTSDDLChildTaskInfo();
    ObString data_name("data");
    const ObFTSDDLChildTaskInfo data_info = modes & 4
        ? ObFTSDDLChildTaskInfo(data_name, 10000, 0) : ObFTSDDLChildTaskInfo();
    ASSERT_EQ(OB_SUCCESS, source.init(1001, 123, 500000, DDL_DROP_ASYNC_SEARCH_INDEX,
        ObFTSDDLChildTaskInfo(), data_info, ObFTSDDLChildTaskInfo(), match_info, phrase_info,
        ObString("DROP INDEX idx ON t"), 10, 0, 500001));
    EXPECT_EQ((modes & 3) == 0 ? 2 : 3, source.task_version_);
    // Initialization owns the names, just as it does for the scalar Data child.
    std::fill(match_buffer.begin(), match_buffer.end(), 'x');
    std::fill(phrase_buffer.begin(), phrase_buffer.end(), 'x');
    if (modes & 1) {
      EXPECT_EQ(ObString("match"), source.search_text_match_index_.index_name_);
    }
    if (modes & 2) {
      EXPECT_EQ(ObString("phrase"), source.search_text_match_phrase_index_.index_name_);
    }
    const int64_t size = source.get_serialize_param_size();
    std::vector<char> buffer(size);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
    ASSERT_EQ(size, pos);
    ObDropSearchIndexTask recovered;
    recovered.task_type_ = source.task_type_;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, recovered.deserialize_params_from_message(1001, buffer.data(), size, pos));
    EXPECT_EQ(size, pos);
    EXPECT_EQ((modes & 3) == 0 ? 2 : 3, recovered.task_version_);
    EXPECT_EQ(0 != (modes & 4), recovered.search_data_index_.is_valid());
    if (!(modes & 4)) {
      ObSchemaGetterGuard guard;
      bool finished = false;
      ASSERT_EQ(OB_SUCCESS, recovered.send_and_wait_drop_index_task(
          recovered.search_data_index_, guard, finished));
      EXPECT_TRUE(finished);
    }
    EXPECT_EQ(0 != (modes & 1), recovered.search_text_match_index_.is_valid());
    EXPECT_EQ(0 != (modes & 2), recovered.search_text_match_phrase_index_.is_valid());
    EXPECT_EQ(modes & 1 ? 10001 : OB_INVALID_ID, recovered.search_text_match_index_.table_id_);
    EXPECT_EQ(modes & 2 ? 10002 : OB_INVALID_ID, recovered.search_text_match_phrase_index_.table_id_);
    EXPECT_EQ(modes & 1 ? -1 : 0, recovered.search_text_match_index_.task_id_);
    EXPECT_EQ(modes & 2 ? 123 : 0, recovered.search_text_match_phrase_index_.task_id_);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, recovered.serialize_params_to_message(buffer.data(), size, pos));
    EXPECT_EQ(size, pos);
  }
}

TEST(SearchIndexDDL, create_task_preserves_optional_tail_defaults)
{
  ObFtsIndexBuildTask source;
  source.task_type_ = DDL_CREATE_ASYNC_SEARCH_INDEX;
  source.task_version_ = 1;
  source.create_index_arg_.tenant_id_ = 1001;
  source.create_index_arg_.exec_tenant_id_ = 1001;
  source.create_index_arg_.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
  source.set_rowkey_doc_aux_table_id(9999);
  source.set_rowkey_doc_aux_task_id(222);
  source.set_rowkey_doc_task_submitted(true);
  source.set_fts_index_aux_table_id(10000);
  source.set_fts_index_aux_task_id(111);
  source.set_fts_index_aux_task_submitted(true);
  source.use_doc_id_ = false;
  source.rowkey_doc_schema_version_ = 456;
  source.refresh_done_ = true;
  source.set_search_text_match_aux_table_id(10001);
  source.set_search_text_match_aux_task_id(321);
  source.set_search_text_match_task_submitted(true);
  source.set_search_text_match_phrase_aux_table_id(10002);
  source.set_search_text_match_phrase_aux_task_id(654);
  source.set_search_text_match_phrase_task_submitted(true);
  const int64_t size = source.get_serialize_param_size();
  std::vector<char> buffer(size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
  ASSERT_EQ(size, pos);
  // Preserve the existing optional tail boundaries after the fixed auxiliary-table fields.
  const int64_t before_ids = size - serialization::encoded_length_i64(0);
  const int64_t before_refresh = before_ids - serialization::encoded_length_i8(1);
  const int64_t before_use_doc_id = before_refresh
      - serialization::encoded_length_i64(456) - serialization::encoded_length_i8(0);
  const int64_t message_sizes[] = {before_use_doc_id, before_refresh, before_ids};
  for (const int64_t message_size : message_sizes) {
    SCOPED_TRACE(message_size);
    ObFtsIndexBuildTask recovered;
    recovered.task_type_ = source.task_type_;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        recovered.deserialize_params_from_message(1001, buffer.data(), message_size, pos));
    EXPECT_EQ(message_size, pos);
    EXPECT_EQ(1, recovered.task_version_);
    EXPECT_EQ(message_size == before_use_doc_id, recovered.use_doc_id_);
    if (message_size != before_use_doc_id) {
      EXPECT_EQ(456, recovered.rowkey_doc_schema_version_);
    }
    EXPECT_EQ(message_size == before_ids, recovered.refresh_done_);
    EXPECT_TRUE(recovered.existing_async_index_table_ids_.empty());
    EXPECT_EQ(3, recovered.dependent_task_result_map_.size());
    EXPECT_EQ(10001, recovered.search_text_match_aux_table_id_);
    EXPECT_EQ(10002, recovered.search_text_match_phrase_aux_table_id_);
    EXPECT_EQ(321, recovered.search_text_match_task_id_);
    EXPECT_EQ(654, recovered.search_text_match_phrase_task_id_);
    EXPECT_TRUE(recovered.search_text_match_task_submitted_);
    EXPECT_TRUE(recovered.search_text_match_phrase_task_submitted_);
  }
}

TEST(SearchIndexDDL, create_task_recovers_optional_text_dependencies)
{
  for (int64_t modes = 0; modes < 8; ++modes) {
    SCOPED_TRACE(modes);
    ObFtsIndexBuildTask source;
    source.task_type_ = DDL_CREATE_ASYNC_SEARCH_INDEX;
    source.task_version_ = 1;
    source.create_index_arg_.tenant_id_ = 1001;
    source.create_index_arg_.exec_tenant_id_ = 1001;
    source.create_index_arg_.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
    source.set_rowkey_doc_aux_table_id(9999); // Delete
    source.set_rowkey_doc_aux_task_id(222);
    source.set_rowkey_doc_task_submitted(true);
    if (modes & 4) {
      source.set_fts_index_aux_table_id(10000); // Data
      source.set_fts_index_aux_task_id(111);
      source.set_fts_index_aux_task_submitted(true);
    }
    if (modes & 1) {
      source.set_search_text_match_aux_table_id(10001);
      source.set_search_text_match_aux_task_id(321);
      source.set_search_text_match_task_submitted(true);
    }
    if (modes & 2) {
      source.set_search_text_match_phrase_aux_table_id(10002);
      source.set_search_text_match_phrase_aux_task_id(654);
      source.set_search_text_match_phrase_task_submitted(true);
    }
    source.refresh_done_ = true;
    ASSERT_EQ(OB_SUCCESS, source.existing_async_index_table_ids_.push_back(9001));
    ASSERT_EQ(OB_SUCCESS, source.existing_async_index_table_ids_.push_back(9002));
    EXPECT_EQ(1, source.task_version_);
    const int64_t size = source.get_serialize_param_size();
    std::vector<char> buffer(size);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
    ASSERT_EQ(size, pos);
    // Verify the fixed wire layout: six table IDs, six submitted flags, six task IDs.
    ObFtsIndexBuildTask header;
    ObCreateIndexArg arg;
    int64_t field_pos = 0;
    ASSERT_EQ(OB_SUCCESS, header.ObDDLTask::deserialize_params_from_message(
        1001, buffer.data(), size, field_pos));
    EXPECT_EQ(1, header.task_version_);
    ASSERT_EQ(OB_SUCCESS, arg.deserialize(buffer.data(), size, field_pos));
    const uint64_t table_ids[] = {9999, OB_INVALID_ID,
        modes & 4 ? 10000UL : OB_INVALID_ID, OB_INVALID_ID,
        modes & 1 ? 10001UL : OB_INVALID_ID, modes & 2 ? 10002UL : OB_INVALID_ID};
    for (const uint64_t expected_id : table_ids) {
      uint64_t table_id = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer.data(), size, field_pos, table_id));
      EXPECT_EQ(expected_id, table_id);
    }
    const int8_t submitted_flags[] = {1, 0, static_cast<int8_t>(!!(modes & 4)), 0,
        static_cast<int8_t>(!!(modes & 1)), static_cast<int8_t>(!!(modes & 2))};
    for (const int8_t expected_flag : submitted_flags) {
      int8_t submitted = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_i8(buffer.data(), size, field_pos, &submitted));
      EXPECT_EQ(expected_flag, submitted);
    }
    const int64_t task_ids[] = {222, 0, modes & 4 ? 111 : 0, 0,
        modes & 1 ? 321 : 0, modes & 2 ? 654 : 0};
    for (const int64_t expected_id : task_ids) {
      int64_t task_id = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buffer.data(), size, field_pos, &task_id));
      EXPECT_EQ(expected_id, task_id);
    }
    ObFtsIndexBuildTask recovered;
    recovered.task_type_ = source.task_type_;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, recovered.deserialize_params_from_message(1001, buffer.data(), size, pos));
    EXPECT_EQ(size, pos);
    EXPECT_EQ(1, recovered.task_version_);
    EXPECT_TRUE(recovered.refresh_done_);
    ASSERT_EQ(2, recovered.existing_async_index_table_ids_.count());
    EXPECT_EQ(9002, recovered.existing_async_index_table_ids_.at(1));
    EXPECT_EQ(OB_INVALID_ID, recovered.doc_rowkey_aux_table_id_);
    EXPECT_EQ(OB_INVALID_ID, recovered.fts_doc_word_aux_table_id_);
    EXPECT_EQ(9999, recovered.rowkey_doc_aux_table_id_);
    EXPECT_EQ(222, recovered.rowkey_doc_task_id_);
    EXPECT_TRUE(recovered.rowkey_doc_task_submitted_);
    EXPECT_EQ(modes & 4 ? 10000 : OB_INVALID_ID, recovered.domain_index_aux_table_id_);
    EXPECT_EQ(modes & 4 ? 111 : 0, recovered.domain_index_aux_task_id_);
    EXPECT_EQ(0 != (modes & 4), recovered.domain_index_aux_task_submitted_);
    ObDomainDependTaskStatus data_status;
    EXPECT_EQ(modes & 4 ? OB_SUCCESS : OB_HASH_NOT_EXIST,
        recovered.dependent_task_result_map_.get_refactored(10000, data_status));
    if (modes & 4) {
      EXPECT_EQ(111, data_status.task_id_);
    }
    // Delete retains the existing async path: it is not added to this wait map.
    EXPECT_EQ(OB_HASH_NOT_EXIST, recovered.dependent_task_result_map_.get_refactored(9999, data_status));
    EXPECT_EQ(modes & 1 ? 10001 : OB_INVALID_ID, recovered.search_text_match_aux_table_id_);
    EXPECT_EQ(modes & 2 ? 10002 : OB_INVALID_ID, recovered.search_text_match_phrase_aux_table_id_);
    EXPECT_EQ(modes & 1 ? 321 : 0, recovered.search_text_match_task_id_);
    EXPECT_EQ(modes & 2 ? 654 : 0, recovered.search_text_match_phrase_task_id_);
    EXPECT_EQ(0 != (modes & 1), recovered.search_text_match_task_submitted_);
    EXPECT_EQ(0 != (modes & 2), recovered.search_text_match_phrase_task_submitted_);
    EXPECT_EQ(!!(modes & 4) + !!(modes & 1) + !!(modes & 2), recovered.dependent_task_result_map_.size());
    if (modes & 1) {
      ObDomainDependTaskStatus status;
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10001, status));
      EXPECT_EQ(321, status.task_id_);
    }
    if (modes & 2) {
      ObDomainDependTaskStatus status;
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10002, status));
      EXPECT_EQ(654, status.task_id_);
    }
  }
}

TEST(SearchIndexDDL, create_task_text_members_preserve_other_task_recovery)
{
  const ObDDLType task_types[] = {DDL_CREATE_FTS_INDEX, DDL_CREATE_ASYNC_FTS_INDEX};
  for (const ObDDLType task_type : task_types) {
    SCOPED_TRACE(task_type);
    const bool is_fts = task_type == DDL_CREATE_FTS_INDEX;
    ObFtsIndexBuildTask source;
    source.task_type_ = task_type;
    source.task_version_ = 1;
    source.create_index_arg_.tenant_id_ = 1001;
    source.create_index_arg_.exec_tenant_id_ = 1001;
    source.create_index_arg_.index_type_ = is_fts
        ? INDEX_TYPE_FTS_INDEX_LOCAL : INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
    if (is_fts) {
      source.set_doc_rowkey_aux_table_id(10001);
      source.set_doc_rowkey_aux_task_id(321);
      source.set_doc_rowkey_task_submitted(true);
      source.set_fts_doc_word_aux_table_id(10002);
      source.set_fts_doc_word_aux_task_id(654);
      source.set_fts_doc_word_task_submitted(true);
    }
    const int64_t size = source.get_serialize_param_size();
    std::vector<char> buffer(size);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
    ASSERT_EQ(size, pos);
    ObFtsIndexBuildTask recovered;
    recovered.task_type_ = task_type;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, recovered.deserialize_params_from_message(1001, buffer.data(), size, pos));
    EXPECT_EQ(size, pos);
    EXPECT_EQ(1, recovered.task_version_);
    EXPECT_EQ(is_fts, recovered.doc_rowkey_task_submitted_);
    // Async FTS still skips the unused DocWord table.
    EXPECT_TRUE(recovered.fts_doc_word_task_submitted_);
    EXPECT_EQ(is_fts ? 2 : 0, recovered.dependent_task_result_map_.size());
    if (is_fts) {
      ObDomainDependTaskStatus status;
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10001, status));
      EXPECT_EQ(321, status.task_id_);
      ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10002, status));
      EXPECT_EQ(654, status.task_id_);
    }
  }
}

TEST(SearchIndexDDL, create_task_recovers_both_text_dependencies)
{
  ObFtsIndexBuildTask source;
  source.task_type_ = DDL_CREATE_ASYNC_SEARCH_INDEX;
  source.task_version_ = 1;
  source.create_index_arg_.tenant_id_ = 1001;
  source.create_index_arg_.exec_tenant_id_ = 1001;
  source.create_index_arg_.index_type_ = INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL;
  source.set_search_text_match_aux_table_id(10001);
  source.set_search_text_match_aux_task_id(321);
  source.set_search_text_match_task_submitted(true);
  source.set_search_text_match_phrase_aux_table_id(10002);
  source.set_search_text_match_phrase_aux_task_id(654);
  source.set_search_text_match_phrase_task_submitted(true);
  const int64_t size = source.get_serialize_param_size();
  std::vector<char> buffer(size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize_params_to_message(buffer.data(), size, pos));
  ASSERT_EQ(size, pos);
  ObFtsIndexBuildTask recovered;
  recovered.task_type_ = source.task_type_;
  recovered.task_version_ = source.task_version_;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, recovered.deserialize_params_from_message(1001, buffer.data(), size, pos));
  EXPECT_EQ(size, pos);
  EXPECT_EQ(10001, recovered.search_text_match_aux_table_id_);
  EXPECT_EQ(10002, recovered.search_text_match_phrase_aux_table_id_);
  EXPECT_TRUE(recovered.search_text_match_task_submitted_);
  EXPECT_TRUE(recovered.search_text_match_phrase_task_submitted_);
  ObDomainDependTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, recovered.dependent_task_result_map_.get_refactored(10002, status));
  EXPECT_EQ(654, status.task_id_);
  // Restore both Text dependencies through the existing auxiliary-table wait path,
  // preserving an already delivered child completion.
  ObFtsIndexBuildTask retry;
  retry.task_type_ = source.task_type_;
  retry.task_version_ = 1;
  retry.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, retry.refresh_task_depend_map_context(recovered));
  ASSERT_EQ(OB_SUCCESS, retry.on_child_task_finish(10001, OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, retry.refresh_task_depend_map_context(recovered));
  EXPECT_EQ(2, retry.dependent_task_result_map_.size());
  ASSERT_EQ(OB_SUCCESS, retry.dependent_task_result_map_.get_refactored(10001, status));
  EXPECT_EQ(OB_SUCCESS, status.ret_code_);
  ObFtsIndexBuildTask truncated;
  truncated.task_type_ = source.task_type_;
  truncated.task_version_ = source.task_version_;
  pos = 0;
  EXPECT_NE(OB_SUCCESS, truncated.deserialize_params_from_message(1001, buffer.data(), size - 1, pos));
}
} // namespace

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_file_name("test_search_index_ddl.log", true);
  ObLogger::get_logger().set_log_level("WARN");
  ObCompatModeGetter::instance().init(nullptr);
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
  ObCompatModeGetter::instance().set_tenant_compat_mode(1001, mode);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
