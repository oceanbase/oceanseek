/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>
#include "lib/utility/ob_defer.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/search_index/ob_search_index_config.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ddl/ob_search_index_config_resolver.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_init.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "common/ob_version_def.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace unittest
{
class SearchIndexColumnResolver : public ObDDLResolver
{
public:
  explicit SearchIndexColumnResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  int resolve(const ParseNode &) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("generic resolve is not supported by search index column test resolver", K(ret));
    return ret;
  }
  using ObDDLResolver::resolve_search_index_constraint;
};

static const ParseNode *find_node(const ParseNode *node, ObItemType type)
{
  const ParseNode *found = nullptr;
  if (node != nullptr) {
    if (node->type_ == type) {
      found = node;
    } else {
      for (int64_t i = 0; found == nullptr && i < node->num_child_; ++i) {
        found = find_node(node->children_[i], type);
      }
    }
  }
  return found;
}

static ObObjMeta json_source_meta()
{
  ObObjMeta meta;
  meta.set_json();
  meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  return meta;
}

static int resolve_preview_sql(const std::string &query, ObSearchIndexConfig &config,
                        const ObObjMeta &source_meta = json_source_meta(),
                        ObCollationType collation = CS_TYPE_UTF8MB4_BIN,
                        ObSQLMode sql_mode = DEFAULT_MYSQL_MODE)
{
  ObArenaAllocator allocator;
  ObCharsets4Parser charsets;
  charsets.string_collation_ = collation;
  ObParser parser(allocator, sql_mode, charsets);
  ParseResult result = {};
  int ret = OB_SUCCESS;
  if (OB_FAIL(parser.parse(ObString(query.size(), query.data()), result))) {
    LOG_WARN("failed to parse search index test SQL", K(ret), K(query.c_str()));
  } else {
    const ParseNode *columns = find_node(result.result_tree_, T_INDEX_COLUMN_LIST);
    if (columns == nullptr || columns->num_child_ != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("expected one search index column in test SQL", K(ret), KP(columns), K(query.c_str()));
    } else {
      ObSqlString comment;
      if (OB_FAIL(ObSearchIndexConfigResolver::resolve(
          find_node(result.result_tree_, T_SEARCH_INDEX_COLUMN_PARAMS),
          ObString("test"), OB_SYS_TENANT_ID, DATA_VERSION_4_6_3_1, nullptr, source_meta, collation, comment))) {
        LOG_WARN("failed to resolve search index test configuration", K(ret));
      } else if (OB_FAIL(config.decode(comment.string()))) {
        LOG_WARN("failed to decode search index test configuration", K(ret));
      }
    }
  }
  parser.free_result(result);
  return ret;
}

class SearchIndexConfigTest : public ::testing::Test
{
public:
  int resolve_sql(const std::string &query, ObSearchIndexConfig &config)
  {
    return resolve_preview_sql(query, config);
  }

  int resolve(const std::string &modes, ObSearchIndexConfig &config)
  {
    return resolve_sql("CREATE ASYNC SEARCH INDEX idx ON docs (payload WITH (" + modes + "))", config);
  }

  void expect_parser(const ObSearchIndexModeConfig &mode, const char *name, ObFTSIndexType type)
  {
    ASSERT_FALSE(mode.parser_name_.empty());
    storage::ObFTParser parser;
    ASSERT_EQ(OB_SUCCESS, parser.parse_from_str(mode.parser_name_.ptr(), mode.parser_name_.length()));
    EXPECT_STREQ(name, parser.get_parser_name().str());
    EXPECT_EQ(type == OB_FTS_INDEX_TYPE_MATCH ? ObSearchIndexModeConfig::FULLTEXT_MATCH
                                            : ObSearchIndexModeConfig::FULLTEXT_PHRASE, mode.mode_);
  }

  void expect_token_sizes(const ObSearchIndexModeConfig &mode, int64_t min_size, int64_t max_size)
  {
    storage::ObFTParserJsonProps props;
    ASSERT_FALSE(mode.parser_properties_.empty());
    ASSERT_EQ(OB_SUCCESS, props.init());
    ASSERT_EQ(OB_SUCCESS, props.parse_from_valid_str(mode.parser_properties_));
    int64_t size = 0;
    ASSERT_EQ(OB_SUCCESS, props.config_get_min_token_size(size));
    EXPECT_EQ(min_size, size);
    ASSERT_EQ(OB_SUCCESS, props.config_get_max_token_size(size));
    EXPECT_EQ(max_size, size);
  }

  void expect_round_trip(ObSearchIndexConfig &config)
  {
    ObColumnSchemaV2 column;
    column.set_tenant_id(OB_SYS_TENANT_ID);
    column.set_table_id(500001);
    column.set_column_id(16);
    column.set_data_type(ObJsonType);
    column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("payload"));
    ObSqlString comment;
    ASSERT_EQ(OB_SUCCESS, config.encode(comment));
    ASSERT_EQ(OB_SUCCESS, column.set_comment(comment.string()));
    comment.reset();
    const std::string encoded(column.get_comment_str().ptr(), column.get_comment_str().length());
    ObColumnSchemaV2 copied;
    ASSERT_EQ(OB_SUCCESS, copied.assign(column));
    ASSERT_NE(column.get_comment_str().ptr(), copied.get_comment_str().ptr());
    column.reset();
    config.reset();
    std::vector<char> wire(copied.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, copied.serialize(wire.data(), wire.size(), pos));
    ObColumnSchemaV2 restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(wire.data(), wire.size(), pos));
    copied.reset();
    std::fill(wire.begin(), wire.end(), '\0');
    wire.clear();
    const ObString encoded_view(encoded.size(), encoded.data());
    EXPECT_EQ(encoded_view, restored.get_comment_str());
    ObString restored_comment = restored.get_comment_str();
    ASSERT_EQ(OB_SUCCESS, config.decode(restored_comment));
    std::fill_n(restored_comment.ptr(), restored_comment.length(), '\0');
    restored.reset(); // the decoded config must own its strings
    ObSqlString reencoded;
    ASSERT_EQ(OB_SUCCESS, config.encode(reencoded));
    EXPECT_EQ(encoded, std::string(reencoded.ptr(), reencoded.length()));

    std::vector<char> ddl(65536);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(config.get_comment(), ddl.data(), ddl.size(), pos));
    const std::string sql = "CREATE ASYNC SEARCH INDEX idx ON docs (payload" + std::string(ddl.data(), pos) + ")";
    const bool has_printed_options = pos > 0;
    const int64_t config_len = config.to_string(ddl.data(), ddl.size());
    std::cout << "Resolved config: " << std::string(ddl.data(), config_len)
              << "\nSHOW CREATE: " << sql << std::endl;
    ObSearchIndexConfig reparsed;
    ASSERT_EQ(OB_SUCCESS, resolve_sql(sql, reparsed));
    ObSqlString replayed;
    ASSERT_EQ(OB_SUCCESS, reparsed.encode(replayed));
    EXPECT_EQ(encoded, std::string(replayed.ptr(), replayed.length()));
    char small[8];
    pos = 0;
    if (has_printed_options) {
      EXPECT_NE(OB_SUCCESS, ObSearchIndexConfig::print_schema(config.get_comment(), small, sizeof(small), pos));
    } else {
      EXPECT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(config.get_comment(), small, sizeof(small), pos));
      EXPECT_EQ(0, pos);
    }
  }

};

TEST_F(SearchIndexConfigTest, final_config_wire_layout)
{
  // Shared golden bytes for the DDL encoder and data-generation decoder.
  const ObString wire("SEARCH_INDEX_CONFIG=1:2:0:0:0:0:0:2:27:INCLUDE_TYPES=11json_string7:space.12:{}");
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(wire));
  ASSERT_EQ(2, config.get_modes().count());
  EXPECT_EQ(ObSearchIndexModeConfig::VALUE, config.get_modes().at(0).mode_);
  EXPECT_EQ(ObSearchIndexModeConfig::FULLTEXT_PHRASE, config.get_modes().at(1).mode_);
  EXPECT_TRUE(config.get_nested_paths_filter().empty());
  EXPECT_TRUE(config.get_modes().at(0).filter_.empty());
  EXPECT_EQ(ObString("INCLUDE_TYPES=11json_string"), config.get_modes().at(1).filter_);
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  EXPECT_EQ(wire, encoded.string());

  // Exercise the writer independently: encode() alone copies the decoded comment.
  ObSearchIndexModeConfig value;
  ObSearchIndexModeConfig phrase;
  phrase.mode_ = ObSearchIndexModeConfig::FULLTEXT_PHRASE;
  phrase.filter_ = "INCLUDE_TYPES=11json_string";
  phrase.parser_name_ = "space.1";
  phrase.parser_properties_ = "{}";
  ObSEArray<ObSearchIndexModeConfig, 2> modes;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(value));
  ASSERT_EQ(OB_SUCCESS, modes.push_back(phrase));
  ASSERT_EQ(OB_SUCCESS, config.init(modes));
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  EXPECT_EQ(wire, encoded.string());
}

TEST_F(SearchIndexConfigTest, persisted_schema_comments_accept_all_three_formats)
{
  const char *default_v1 = "SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:";
  const char *value_v1 = "SEARCH_INDEX_CONFIG=1:1:17:NESTED_PATHS=3$.a"
      "0:45:INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number0:0:";
  const char *phrase_v1 = "SEARCH_INDEX_CONFIG=1:1:0:2:0:7:space.1"
      "43:{\"max_token_size\": 84, \"min_token_size\": 3}";
  const struct {
    const char *stored;
    const char *normalized;
    ObSearchIndexModeConfig::Mode mode;
    const char *filter;
    const char *nested;
  } cases[] = {
    {"", default_v1, ObSearchIndexModeConfig::VALUE, "", ""},
    {"业务数据", default_v1, ObSearchIndexModeConfig::VALUE, "", ""},
    {"INCLUDE_PATHS=3$.aNESTED_PATHS=3$.aINCLUDE_TYPES=11json_number", value_v1,
      ObSearchIndexModeConfig::VALUE, "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number", "NESTED_PATHS=3$.a"},
    {default_v1, default_v1, ObSearchIndexModeConfig::VALUE, "", ""},
    {value_v1, value_v1, ObSearchIndexModeConfig::VALUE,
      "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number", "NESTED_PATHS=3$.a"},
    {phrase_v1, phrase_v1, ObSearchIndexModeConfig::FULLTEXT_PHRASE, "", ""}
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.stored);
    ObColumnSchemaV2 column;
    ASSERT_EQ(OB_SUCCESS, column.set_comment(ObString(item.stored)));
    std::vector<char> wire(column.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, column.serialize(wire.data(), wire.size(), pos));
    const int64_t length = pos;
    column.reset();
    ObColumnSchemaV2 restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(wire.data(), length, pos));
    EXPECT_EQ(length, pos);
    EXPECT_EQ(ObString(item.stored), restored.get_comment_str());
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.decode(restored.get_comment_str()));
    restored.reset();
    std::fill(wire.begin(), wire.end(), '\0');
    ASSERT_EQ(OB_SUCCESS, config.validate());
    ASSERT_EQ(1, config.get_modes().count());
    EXPECT_EQ(item.mode, config.get_modes().at(0).mode_);
    EXPECT_EQ(ObString(item.filter), config.get_modes().at(0).filter_);
    EXPECT_EQ(ObString(item.nested), config.get_nested_paths_filter());
    if (config.get_modes().at(0).is_fulltext()) {
      EXPECT_EQ(ObString("space.1"), config.get_modes().at(0).parser_name_);
      EXPECT_EQ(ObString("{\"max_token_size\": 84, \"min_token_size\": 3}"),
          config.get_modes().at(0).parser_properties_);
    }
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
    EXPECT_EQ(ObString(item.normalized), encoded.string());
  }
}

TEST_F(SearchIndexConfigTest, ddl_generates_expected_persisted_comment_bytes)
{
  // These literals are the persistence contract, independent of the production encoder/decoder.
  const struct {
    const char *options;
    const char *expected;
    const char *legacy;
  } cases[] = {
    {"", "SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:", ""},
    {"VALUE", "SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:", ""},
    {"INCLUDE_PATHS=('$.a'), INCLUDE_TYPES=(JSON_NUMBER)",
      "SEARCH_INDEX_CONFIG=1:1:0:0:45:INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number0:0:",
      "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number"},
    {"NESTED_PATHS=('$.items'), VALUE(INCLUDE_PATHS=('$.items.price'), INCLUDE_TYPES=(JSON_NUMBER))",
      "SEARCH_INDEX_CONFIG=1:1:21:NESTED_PATHS=7$.items"
      "0:56:INCLUDE_PATHS=13$.items.priceINCLUDE_TYPES=11json_number0:0:",
      "INCLUDE_PATHS=13$.items.priceNESTED_PATHS=7$.itemsINCLUDE_TYPES=11json_number"},
    {"VALUE(INCLUDE_PATHS=('$.\"标题:123\"'))",
      "SEARCH_INDEX_CONFIG=1:1:0:0:30:INCLUDE_PATHS=14$.\"标题:123\"0:0:",
      "INCLUDE_PATHS=14$.\"标题:123\""},
    {"FULLTEXT", "SEARCH_INDEX_CONFIG=1:1:0:2:0:7:space.1"
      "43:{\"max_token_size\": 84, \"min_token_size\": 3}", nullptr},
    {"FULLTEXT(PARSER_PROPERTIES=(min_token_size=2), FTS_INDEX_TYPE=MATCH)",
      "SEARCH_INDEX_CONFIG=1:1:0:1:0:7:space.143:{\"max_token_size\": 84, \"min_token_size\": 2}", nullptr},
    {"FULLTEXT(INCLUDE_PATHS=('$.title'), PARSER='space', "
      "PARSER_PROPERTIES=(min_token_size=2, max_token_size=32), FTS_INDEX_TYPE=MATCH)",
      "SEARCH_INDEX_CONFIG=1:1:0:1:22:INCLUDE_PATHS=7$.title"
      "7:space.143:{\"max_token_size\": 32, \"min_token_size\": 2}", nullptr},
    {"FULLTEXT(INCLUDE_PATHS=('$.title'), PARSER='ngram', "
      "PARSER_PROPERTIES=(ngram_token_size=3), FTS_INDEX_TYPE=MATCH)",
      "SEARCH_INDEX_CONFIG=1:1:0:1:22:INCLUDE_PATHS=7$.title7:ngram.123:{\"ngram_token_size\": 3}", nullptr},
    {"VALUE, FULLTEXT(PARSER='ngram')",
      "SEARCH_INDEX_CONFIG=1:2:0:"
      "0:0:0:0:2:0:7:ngram.123:{\"ngram_token_size\": 2}", nullptr}
  };
  const char *prefixes[] = {
    "CREATE TABLE docs (payload JSON, ASYNC SEARCH INDEX idx ",
    "CREATE ASYNC SEARCH INDEX idx ON docs ",
    "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx "
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(prefixes); ++i) {
    for (const auto &item : cases) {
      const std::string options = item.options[0] == '\0' ? "" : std::string(" WITH (") + item.options + ")";
      const std::string sql = std::string(prefixes[i]) + "(payload" + options + ")"
          + (i == 0 ? ") ORGANIZATION=HEAP" : "");
      SCOPED_TRACE(sql);
      ObArenaAllocator allocator;
      ObParser parser(allocator, DEFAULT_MYSQL_MODE);
      ParseResult result;
      ASSERT_EQ(OB_SUCCESS, parser.parse(ObString(sql.size(), sql.data()), result));
      DEFER(parser.free_result(result));
      for (const uint64_t data_version : {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
        SCOPED_TRACE(data_version);
        ObSqlString comment;
        const int ret = ObSearchIndexConfigResolver::resolve(
            find_node(result.result_tree_, T_SEARCH_INDEX_COLUMN_PARAMS),
            ObString("test"), OB_SYS_TENANT_ID, data_version, nullptr, json_source_meta(), CS_TYPE_UTF8MB4_BIN, comment);
        if (data_version < DATA_VERSION_4_6_3_1 && item.legacy == nullptr) {
          EXPECT_EQ(OB_NOT_SUPPORTED, ret);
          EXPECT_TRUE(comment.empty());
        } else {
          ASSERT_EQ(OB_SUCCESS, ret);
          // Assert write-side bytes before decode() normalizes the result.
          EXPECT_EQ(ObString(data_version < DATA_VERSION_4_6_3_1 ? item.legacy : item.expected), comment.string());
          ObSearchIndexConfig config;
          ASSERT_EQ(OB_SUCCESS, config.decode(comment.string()));
          ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
        }
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, persisted_defaults_are_independent_across_blocks)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve(
      "FULLTEXT (INCLUDE_PATHS=('$.title'), PARSER='ngram', "
          "PARSER_PROPERTIES=(ngram_token_size=3), FTS_INDEX_TYPE=MATCH), "
      "FULLTEXT (INCLUDE_PATHS=('$.summary'), FTS_INDEX_TYPE=MATCH), "
      "FULLTEXT (INCLUDE_PATHS=('$.body'), PARSER='ik', PARSER_PROPERTIES=(ik_mode='max_word')), "
      "VALUE (INCLUDE_PATHS=('$.price'))", config));
  const auto &modes = config.get_modes();
  ASSERT_EQ(4, modes.count());
  expect_parser(modes.at(0), "ngram", OB_FTS_INDEX_TYPE_MATCH);
  expect_parser(modes.at(1), "space", OB_FTS_INDEX_TYPE_MATCH);
  expect_parser(modes.at(2), "ik", OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  storage::ObFTParserJsonProps props;
  ASSERT_EQ(OB_SUCCESS, props.init());
  ASSERT_EQ(OB_SUCCESS, props.parse_from_valid_str(modes.at(0).parser_properties_));
  int64_t token_size = 0;
  EXPECT_EQ(OB_SUCCESS, props.config_get_ngram_token_size(token_size));
  EXPECT_EQ(3, token_size);
  ASSERT_NO_FATAL_FAILURE(expect_token_sizes(modes.at(1), 3, 84));
  ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), 3));
  EXPECT_FALSE(filter.has_types());
  EXPECT_TRUE(modes.at(3).parser_name_.empty());
  EXPECT_TRUE(modes.at(3).parser_properties_.empty());
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, default_parser_and_partial_properties_are_persisted)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT, VALUE", config));
  ASSERT_EQ(2, config.get_modes().count());
  expect_parser(config.get_modes().at(0), "space", OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(0), 3, 84));
  EXPECT_TRUE(config.get_modes().at(1).parser_name_.empty());
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER_PROPERTIES=(min_token_size=2))", config));
  expect_parser(config.get_modes().at(0), "space", OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(0), 2, 84));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER_PROPERTIES=(max_token_size=32))", config));
  ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(0), 3, 32));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, defaults_are_persisted_in_all_ddl_entries)
{
  const char *prefixes[] = {
    "CREATE TABLE docs (payload JSON, ASYNC SEARCH INDEX idx ",
    "CREATE ASYNC SEARCH INDEX idx ON docs ",
    "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx "
  };
  ObSearchIndexConfig explicit_config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER='space', "
      "PARSER_PROPERTIES=(min_token_size=3, max_token_size=84), FTS_INDEX_TYPE=PHRASE_MATCH)",
      explicit_config));
  for (int64_t i = 0; i < ARRAYSIZEOF(prefixes); ++i) {
    for (const char *options : {"FULLTEXT", "TYPE=FULLTEXT"}) {
      const std::string sql = std::string(prefixes[i]) + "(payload WITH (" + options + "))"
          + (i == 0 ? ") ORGANIZATION=HEAP" : "");
      SCOPED_TRACE(sql);
      ObSearchIndexConfig config;
      ASSERT_EQ(OB_SUCCESS, resolve_sql(sql, config));
      EXPECT_EQ(explicit_config.get_comment(), config.get_comment());
      ASSERT_NO_FATAL_FAILURE(expect_parser(config.get_modes().at(0), "space", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
      ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(0), 3, 84));
      ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
    }
  }
}

TEST_F(SearchIndexConfigTest, omitted_parser_defaults_but_explicit_empty_parser_is_rejected)
{
  const char *prefixes[] = {
    "CREATE TABLE docs (payload JSON, ASYNC SEARCH INDEX idx ",
    "CREATE ASYNC SEARCH INDEX idx ON docs ",
    "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx "
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(prefixes); ++i) {
    for (const char *options : {"FULLTEXT", "FULLTEXT(PARSER='space')", "FULLTEXT(PARSER='')"}) {
      const std::string sql = std::string(prefixes[i]) + "(payload WITH (" + options + "))"
          + (i == 0 ? ") ORGANIZATION=HEAP" : "");
      SCOPED_TRACE(sql);
      ObSearchIndexConfig config;
      if (0 == strcmp(options, "FULLTEXT(PARSER='')")) {
        EXPECT_EQ(OB_INVALID_ARGUMENT, resolve_sql(sql, config));
      } else {
        ASSERT_EQ(OB_SUCCESS, resolve_sql(sql, config));
        ASSERT_EQ(1, config.get_modes().count());
        EXPECT_EQ(ObString("space.1"), config.get_modes().at(0).parser_name_);
        EXPECT_EQ(OB_SUCCESS, config.validate());
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, fulltext_config_requires_parser_name)
{
  for (const char *wire : {"SEARCH_INDEX_CONFIG=1:1:0:1:0:0:0:",
                           "SEARCH_INDEX_CONFIG=1:1:0:2:0:0:0:"}) {
    SCOPED_TRACE(wire);
    ObSearchIndexConfig config;
    // Decoding checks the wire format; semantic validation rejects the empty name.
    ASSERT_EQ(OB_SUCCESS, config.decode(ObString(wire)));
    EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfig::validate(ObString(wire)));
    ObSqlString encoded;
    EXPECT_EQ(OB_INVALID_ARGUMENT, config.encode(encoded));
  }
  ObSearchIndexConfig value;
  ASSERT_EQ(OB_SUCCESS, value.decode(ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_SUCCESS, value.validate());
}

TEST_F(SearchIndexConfigTest, parser_only_persists_property_and_layout_defaults)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER='ngram')", config));
  const auto &mode = config.get_modes().at(0);
  expect_parser(mode, "ngram", OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  storage::ObFTParserJsonProps props;
  ASSERT_EQ(OB_SUCCESS, props.init());
  ASSERT_EQ(OB_SUCCESS, props.parse_from_valid_str(mode.parser_properties_));
  int64_t size = 0;
  EXPECT_EQ(OB_SUCCESS, props.config_get_ngram_token_size(size));
  EXPECT_EQ(2, size);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, other_builtin_parser_defaults_are_persisted)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER='beng')", config));
  ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(0), 3, 84));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER='ngram2')", config));
  storage::ObFTParserJsonProps ngram_props;
  ASSERT_EQ(OB_SUCCESS, ngram_props.init());
  ASSERT_EQ(OB_SUCCESS, ngram_props.parse_from_valid_str(config.get_modes().at(0).parser_properties_));
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ngram_props.config_get_min_ngram_token_size(size));
  EXPECT_EQ(2, size);
  ASSERT_EQ(OB_SUCCESS, ngram_props.config_get_max_ngram_token_size(size));
  EXPECT_EQ(3, size);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));

  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (PARSER='ik')", config));
  storage::ObFTParserJsonProps ik_props;
  ASSERT_EQ(OB_SUCCESS, ik_props.init());
  ASSERT_EQ(OB_SUCCESS, ik_props.parse_from_valid_str(config.get_modes().at(0).parser_properties_));
  ObString value;
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_ik_mode(value));
  EXPECT_EQ(ObString("smart"), value);
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_dict_table_name(value));
  EXPECT_EQ(ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE), value);
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_stopword_table_name(value));
  EXPECT_EQ(ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE), value);
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_quantifier_table_name(value));
  EXPECT_EQ(ObString(storage::ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE), value);
  uint64_t table_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_dict_table_id(table_id));
  EXPECT_NE(OB_INVALID_ID, table_id);
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_stopword_table_id(table_id));
  EXPECT_NE(OB_INVALID_ID, table_id);
  ASSERT_EQ(OB_SUCCESS, ik_props.config_get_quantifier_table_id(table_id));
  EXPECT_NE(OB_INVALID_ID, table_id);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, bare_modes_materialize_fulltext_defaults)
{
  const char *configs[] = {
    "VALUE", "FULLTEXT", "FULLTEXT, VALUE", "VALUE, FULLTEXT",
    "TYPE=VALUE", "TYPE=FULLTEXT",
    "FULLTEXT(INCLUDE_PATHS=('$.title'), PARSER='ngram'), VALUE, "
        "FULLTEXT(EXCLUDE_PATHS=('$.title'))"
  };
  for (const char *options : configs) {
    SCOPED_TRACE(options);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(options, config));
    char buf[8192];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(config.get_comment(), buf, sizeof(buf), pos));
    const std::string shown(buf, pos);
    EXPECT_EQ(std::string::npos, shown.find("VALUE ("));
    for (int64_t i = 0; i < config.get_modes().count(); ++i) {
      const ObSearchIndexModeConfig &mode = config.get_modes().at(i);
      if (mode.is_fulltext()) {
        char block[4096];
        int64_t block_pos = 0;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigFilter::print_schema(mode, block, sizeof(block), block_pos));
        const std::string block_sql(block, block_pos);
        EXPECT_FALSE(mode.parser_name_.empty());
        EXPECT_FALSE(mode.parser_properties_.empty());
        EXPECT_NE(std::string::npos, block_sql.find("PARSER = "));
        EXPECT_NE(std::string::npos, block_sql.find("PARSER_PROPERTIES"));
        ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
        ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), i));
        EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_STRING), filter.get_type_mask());
      } else {
        EXPECT_TRUE(mode.filter_.empty());
        if (config.get_modes().count() == 1 && config.get_nested_paths_filter().empty()) {
          EXPECT_TRUE(shown.empty());
        } else {
          EXPECT_NE(std::string::npos, shown.find("VALUE"));
        }
        ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
        ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), i));
        ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> root;
        for (ObJsonNodeType type : {ObJsonNodeType::J_STRING, ObJsonNodeType::J_BOOLEAN,
                                   ObJsonNodeType::J_NULL}) {
          bool passed = false;
          ASSERT_EQ(OB_SUCCESS, filter.is_indexed(root, type, passed));
          EXPECT_TRUE(passed);
        }
      }
    }
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
  ObSearchIndexConfig config;
  for (const char *options : {"FULLTEXT(PARSER='ngram'), VALUE, FULLTEXT",
                              "FULLTEXT, FULLTEXT", "VALUE, VALUE"}) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve(options, config)) << options;
  }
}

TEST_F(SearchIndexConfigTest, typed_options_match_named_blocks)
{
  ObSearchIndexConfig implicit_value;
  ASSERT_EQ(OB_SUCCESS, resolve_sql("CREATE ASYNC SEARCH INDEX idx ON docs (payload)", implicit_value));
  const char *modes[] = {"VALUE", "FULLTEXT"};
  const char *options[] = {"", "INCLUDE_PATHS=('$.title')", "EXCLUDE_PATHS=('$.private')"};
  for (const char *mode : modes) {
    for (const char *option : options) {
      const std::string typed = std::string("TYPE=") + mode + (*option == '\0' ? "" : ", ") + option;
      SCOPED_TRACE(typed);
      ObSearchIndexConfig config;
      ObSearchIndexConfig named;
      ASSERT_EQ(OB_SUCCESS, resolve(typed, config));
      ASSERT_EQ(OB_SUCCESS, resolve(std::string(mode) + (*option == '\0' ? "" : std::string("(") + option + ")"), named));
      ObSqlString encoded;
      ObSqlString named_encoded;
      ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
      ASSERT_EQ(OB_SUCCESS, named.encode(named_encoded));
      EXPECT_EQ(named_encoded.string(), encoded.string());
      bool equal = false;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(named_encoded.string(), encoded.string(), equal));
      EXPECT_TRUE(equal);
      if (*option == '\0' && 0 == strcmp(mode, "VALUE")) {
        EXPECT_EQ(implicit_value.get_comment(), encoded.string());
      }
      ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
    }
  }
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("TYPE=FULLTEXT, PARSER='ngram', "
      "PARSER_PROPERTIES=(ngram_token_size=3), FTS_INDEX_TYPE=MATCH", config));
  expect_parser(config.get_modes().at(0), "ngram", OB_FTS_INDEX_TYPE_MATCH);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  ASSERT_EQ(OB_SUCCESS, resolve("TYPE=VALUE, INCLUDE_TYPES=(JSON_NUMBER), NESTED_PATHS=('$.items')", config));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, flat_type_position_and_implicit_value)
{
  ObSearchIndexConfig implicit_value;
  ASSERT_EQ(OB_SUCCESS, resolve("INCLUDE_PATHS=('$.items.price', '$.items.name'), NESTED_PATHS=('$.items'), "
      "INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING, JSON_NUMBER)", implicit_value));
  const char *value_options[] = {
    "TYPE=VALUE, INCLUDE_PATHS=('$.items.price', '$.items.name'), NESTED_PATHS=('$.items'), "
        "INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING, JSON_NUMBER)",
    "INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING), TYPE=VALUE, NESTED_PATHS=('$.items'), "
        "INCLUDE_PATHS=('$.items.price', '$.items.name')",
    "NESTED_PATHS=('$.items'), INCLUDE_PATHS=('$.items.price', '$.items.name'), INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER), "
        "TYPE=VALUE",
    "VALUE(INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING, JSON_NUMBER), INCLUDE_PATHS=('$.items.price', '$.items.name')), "
        "NESTED_PATHS=('$.items')",
    "NESTED_PATHS=('$.items'), VALUE(INCLUDE_PATHS=('$.items.price', '$.items.name'), "
        "INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER, JSON_STRING))",
    "VALUE(INCLUDE_PATHS=('$.items.price', '$.items.name'), INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING)), "
        "NESTED_PATHS=('$.items')"
  };
  for (const char *options : value_options) {
    SCOPED_TRACE(options);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(options, config));
    EXPECT_EQ(implicit_value.get_comment(), config.get_comment());
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
  ObSearchIndexConfig named_fulltext;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT(INCLUDE_PATHS=('$.title', '$.body'), PARSER='ngram', "
      "PARSER_PROPERTIES=(ngram_token_size=2), FTS_INDEX_TYPE=MATCH)", named_fulltext));
  const char *fulltext_options[] = {
    "TYPE=FULLTEXT, INCLUDE_PATHS=('$.title', '$.body'), PARSER='ngram', "
        "PARSER_PROPERTIES=(ngram_token_size=2), FTS_INDEX_TYPE=MATCH",
    "PARSER_PROPERTIES=(ngram_token_size=2), INCLUDE_PATHS=('$.title', '$.body'), TYPE=FULLTEXT, "
        "FTS_INDEX_TYPE=MATCH, PARSER='ngram'",
    "FTS_INDEX_TYPE=MATCH, PARSER='ngram', INCLUDE_PATHS=('$.title', '$.body'), "
        "PARSER_PROPERTIES=(ngram_token_size=2), TYPE=FULLTEXT",
    "FULLTEXT(PARSER_PROPERTIES=(ngram_token_size=2), FTS_INDEX_TYPE=MATCH, "
        "PARSER='ngram', INCLUDE_PATHS=('$.title', '$.body'))"
  };
  for (const char *options : fulltext_options) {
    SCOPED_TRACE(options);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(options, config));
    EXPECT_EQ(named_fulltext.get_comment(), config.get_comment());
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
  ObSearchIndexConfig invalid;
  for (const char *options : {"PARSER='space'", "FTS_INDEX_TYPE=MATCH",
                              "PARSER='space', TYPE=VALUE", "INCLUDE_TYPES=(JSON_STRING), TYPE=FULLTEXT"}) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve(options, invalid)) << options;
  }
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve("NESTED_PATHS=('$.items'), TYPE=FULLTEXT", invalid));
}

TEST_F(SearchIndexConfigTest, fulltext_paths_and_implicit_type_round_trip)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (INCLUDE_PATHS=('$.a.b'))", config));
  char buf[4096];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(config.get_comment(), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string::npos, std::string(buf, pos).find("INCLUDE_TYPES"));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  std::string wildcard(encoded.ptr(), encoded.length());
  const size_t path_pos = wildcard.find("$.a.b");
  ASSERT_NE(std::string::npos, path_pos);
  wildcard.replace(path_pos, strlen("$.a.b"), "$.a.*");
  ObSearchIndexConfig decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(ObString(wildcard.size(), wildcard.data())));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(decoded));
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));

  for (const char *options : {"FULLTEXT (INCLUDE_PATHS=('$.a.*'))",
                            "FULLTEXT (EXCLUDE_PATHS=('$.a.*'))",
                            "TYPE=FULLTEXT, INCLUDE_PATHS=('$.a.*')",
                            "TYPE=FULLTEXT, EXCLUDE_PATHS=('$.*')"}) {
    SCOPED_TRACE(options);
    ASSERT_EQ(OB_SUCCESS, resolve(options, config));
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }

  // VALUE retains its existing wildcard and explicit type support.
  for (const char *options : {"VALUE (INCLUDE_PATHS=('$.a.*'), INCLUDE_TYPES=(JSON_STRING))",
                            "TYPE=VALUE, EXCLUDE_PATHS=('$.a.*'), INCLUDE_TYPES=(JSON_NUMBER)",
                            "INCLUDE_PATHS=('$.a.*'), INCLUDE_TYPES=(JSON_STRING)"}) {
    SCOPED_TRACE(options);
    ASSERT_EQ(OB_SUCCESS, resolve(options, config));
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
}

TEST_F(SearchIndexConfigTest, value_is_optional_but_unique)
{
  const char *valid[] = {"FULLTEXT", "VALUE", "FULLTEXT, VALUE"};
  for (const char *modes : valid) {
    SCOPED_TRACE(modes);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(modes, config));
  }
  const char *invalid[] = {
    "VALUE, VALUE",
    "VALUE (INCLUDE_PATHS=('$.a')), VALUE (INCLUDE_PATHS=('$.b'))",
    "VALUE (INCLUDE_TYPES=(JSON_STRING)), FULLTEXT, VALUE (INCLUDE_TYPES=(JSON_NUMBER))"
  };
  for (const char *modes : invalid) {
    SCOPED_TRACE(modes);
    ObSearchIndexConfig config;
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve(modes, config));
  }

  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("VALUE", config));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  const std::string single(encoded.ptr(), encoded.length());
  const std::string payload = single.substr(strlen("SEARCH_INDEX_CONFIG=1:1:0:"));
  const std::string duplicate = "SEARCH_INDEX_CONFIG=1:2:0:" + payload + payload;
  ObSearchIndexConfig decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(ObString(duplicate.size(), duplicate.data())));
  EXPECT_EQ(2, decoded.get_modes().count());
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.validate());
}

TEST_F(SearchIndexConfigTest, effective_parser_name_is_unique)
{
  // Every pair has disjoint paths: parser identity is an independent constraint.
  const char *invalid[] = {
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (INCLUDE_PATHS=('$.b'), PARSER='space')",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), PARSER='SPACE'), FULLTEXT (INCLUDE_PATHS=('$.b'), PARSER='space')",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), FTS_INDEX_TYPE=MATCH), "
      "FULLTEXT (INCLUDE_PATHS=('$.b'), FTS_INDEX_TYPE=PHRASE_MATCH)",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), PARSER='ik', PARSER_PROPERTIES=(ik_mode='max_word')), "
      "FULLTEXT (INCLUDE_PATHS=('$.b'), PARSER='ik', PARSER_PROPERTIES=(ik_mode='smart'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=2)), "
      "FULLTEXT (INCLUDE_PATHS=('$.b'), PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=3))",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), PARSER='analyzer', "
      "PARSER_PROPERTIES=(analysis='{\"analyzer\":\"standard\"}')), "
      "FULLTEXT (INCLUDE_PATHS=('$.b'), PARSER='analyzer', "
      "PARSER_PROPERTIES=(analysis='{\"analyzer\":\"english\"}'))"
  };
  for (const char *modes : invalid) {
    SCOPED_TRACE(modes);
    ObSearchIndexConfig config;
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve(modes, config));
  }
}


TEST_F(SearchIndexConfigTest, column_modes_validate_source_types_after_config_resolution)
{
  ODV_MGR.set_mock_data_version(DATA_CURRENT_VERSION);
  DEFER(ODV_MGR.set_mock_data_version(0));
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ASSERT_EQ(OB_SUCCESS, session.set_tenant(ObString("sys"), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, session.switch_tenant(OB_SYS_TENANT_ID));
  ObArenaAllocator allocator;
  ObResolverParams params;
  params.allocator_ = &allocator;
  params.session_info_ = &session;
  SearchIndexColumnResolver resolver(params);
  const ObObjType types[] = {ObIntType, ObVarcharType, ObCharType, ObTextType};
  const char *names[] = {"INT", "VARCHAR(200)", "CHAR(200)", "TEXT"};
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    ObColumnSchemaV2 column;
    column.set_tenant_id(OB_SYS_TENANT_ID);
    column.set_table_id(1001);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("payload"));
    column.set_data_type(types[i]);
    column.set_data_length(200);
    column.set_collation_type(types[i] == ObIntType ? CS_TYPE_BINARY : CS_TYPE_UTF8MB4_BIN);
    ASSERT_TRUE(column.is_valid());
    for (const char *options : {"", " WITH (VALUE)", " WITH (TYPE=VALUE)",
        " WITH (FULLTEXT)", " WITH (TYPE=FULLTEXT)", " WITH (VALUE, FULLTEXT)",
        " WITH (FULLTEXT(PARSER='space', PARSER_PROPERTIES=(min_token_size=2)))"}) {
      const bool has_fulltext = strstr(options, "FULLTEXT") != nullptr;
      const std::string columns = std::string("(payload") + options + ")";
      const std::string queries[] = {
        std::string("CREATE TABLE docs (payload ") + names[i] + ", ASYNC SEARCH INDEX idx " + columns + ") ORGANIZATION=HEAP",
        "CREATE ASYNC SEARCH INDEX idx ON docs " + columns,
        "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx " + columns};
      for (const std::string &sql : queries) {
        SCOPED_TRACE(sql);
        ObParser parser(allocator, DEFAULT_MYSQL_MODE);
        ParseResult result;
        ASSERT_EQ(OB_SUCCESS, parser.parse(ObString(sql.size(), sql.data()), result));
        DEFER(parser.free_result(result));
        ObString comment;
        ASSERT_EQ(OB_SUCCESS, resolver.resolve_search_index_column_comment(
            find_node(result.result_tree_, T_SEARCH_INDEX_COLUMN_PARAMS), &column,
            column.get_column_name_str(), &allocator, comment));
        const int expected = types[i] == ObIntType && has_fulltext ? OB_NOT_SUPPORTED : OB_SUCCESS;
        EXPECT_EQ(expected, resolver.resolve_search_index_constraint(
            column, static_cast<int64_t>(ObDDLResolver::ASYNC_SEARCH_KEY), comment));
        ObSearchIndexConfig config;
        ASSERT_EQ(OB_SUCCESS, config.decode(comment));
        bool actual_fulltext = false;
        for (int64_t j = 0; j < config.get_modes().count(); ++j) {
          actual_fulltext |= config.get_modes().at(j).is_fulltext();
        }
        EXPECT_EQ(has_fulltext, actual_fulltext);
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, fulltext_collation_constraints_preserve_value_modes)
{
  ODV_MGR.set_mock_data_version(DATA_CURRENT_VERSION);
  DEFER(ODV_MGR.set_mock_data_version(0));
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  ASSERT_EQ(OB_SUCCESS, session.set_tenant(ObString("sys"), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, session.switch_tenant(OB_SYS_TENANT_ID));
  ObArenaAllocator allocator;
  ObResolverParams params;
  params.allocator_ = &allocator;
  params.session_info_ = &session;
  SearchIndexColumnResolver resolver(params);
  std::vector<ObCollationType> collations = {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF16_BIN,
      CS_TYPE_UTF16_GENERAL_CI, CS_TYPE_UTF16LE_BIN, CS_TYPE_UTF16LE_GENERAL_CI,
      CS_TYPE_LATIN1_BIN, CS_TYPE_LATIN1_SWEDISH_CI,
      CS_TYPE_UTF8MB4_UNICODE_CI, CS_TYPE_UTF8MB4_UNICODE_520_CI};
  for (int coll = CS_TYPE_UTF16_UNICODE_CI; coll <= CS_TYPE_UTF16_VIETNAMESE_CI; ++coll) {
    collations.push_back(static_cast<ObCollationType>(coll));
  }
  for (ObObjType type : {ObVarcharType, ObCharType, ObTinyTextType, ObTextType, ObMediumTextType, ObLongTextType}) {
    for (ObCollationType coll : collations) {
      SCOPED_TRACE(type);
      SCOPED_TRACE(coll);
      ObColumnSchemaV2 column;
      column.set_tenant_id(OB_SYS_TENANT_ID);
      column.set_table_id(1001);
      column.set_column_id(OB_APP_MIN_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name("payload"));
      column.set_data_type(type);
      column.set_data_length(200);
      column.set_collation_type(coll);
      ASSERT_TRUE(column.is_valid());
      const bool unsupported = ObCharset::charset_type_by_coll(coll) == CHARSET_LATIN1
          || (coll >= CS_TYPE_UTF16_UNICODE_CI && coll <= CS_TYPE_UTF16_VIETNAMESE_CI)
          || coll == CS_TYPE_UTF8MB4_UNICODE_CI || coll == CS_TYPE_UTF8MB4_UNICODE_520_CI;
      EXPECT_EQ(!unsupported, ObSearchIndexConfig::supports_fulltext(column.get_meta_type()));
      for (const char *options : {"VALUE", "FULLTEXT", "VALUE, FULLTEXT",
          "FULLTEXT(FTS_INDEX_TYPE=PHRASE_MATCH)"}) {
        const bool fulltext = strstr(options, "FULLTEXT") != nullptr;
        const std::string columns = std::string("(payload WITH (") + options + "))";
        const std::string queries[] = {
          "CREATE TABLE docs (payload VARCHAR(200), ASYNC SEARCH INDEX idx " + columns + ") ORGANIZATION=HEAP",
          "CREATE ASYNC SEARCH INDEX idx ON docs " + columns,
          "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx " + columns};
        for (const std::string &sql : queries) {
          SCOPED_TRACE(sql);
          ObParser parser(allocator, DEFAULT_MYSQL_MODE);
          ParseResult result;
          ASSERT_EQ(OB_SUCCESS, parser.parse(ObString(sql.size(), sql.data()), result));
          DEFER(parser.free_result(result));
          ObString comment;
          ASSERT_EQ(OB_SUCCESS, resolver.resolve_search_index_column_comment(
              find_node(result.result_tree_, T_SEARCH_INDEX_COLUMN_PARAMS), &column,
              column.get_column_name_str(), &allocator, comment));
          EXPECT_EQ(fulltext && unsupported ? OB_NOT_SUPPORTED : OB_SUCCESS,
              resolver.resolve_search_index_constraint(
                  column, static_cast<int64_t>(ObDDLResolver::ASYNC_SEARCH_KEY), comment));
        }
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, string_types_across_ddl_entries_and_charsets)
{
  const ObObjType types[] = {ObVarcharType, ObCharType, ObTinyTextType, ObTextType, ObMediumTextType, ObLongTextType};
  const char *names[] = {"VARCHAR(200)", "CHAR(200)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"};
  const ObCollationType collations[] = {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_GENERAL_CI,
      CS_TYPE_LATIN1_BIN, CS_TYPE_LATIN1_SWEDISH_CI, CS_TYPE_GBK_BIN, CS_TYPE_GBK_CHINESE_CI,
      CS_TYPE_UTF16_BIN, CS_TYPE_UTF16_GENERAL_CI, CS_TYPE_UTF16LE_BIN, CS_TYPE_UTF16LE_GENERAL_CI};
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    for (ObCollationType coll : collations) {
      ObObjMeta meta;
      meta.set_type(types[i]);
      meta.set_collation_type(coll);
      EXPECT_EQ(ObCharset::charset_type_by_coll(coll) != CHARSET_LATIN1,
          ObSearchIndexConfig::supports_fulltext(meta));
      for (bool value : {false, true}) {
        const std::string columns = std::string("(payload WITH (") + (value ? "VALUE, " : "")
            + "FULLTEXT(PARSER='space', FTS_INDEX_TYPE=PHRASE_MATCH)))";
        const std::string queries[] = {
          std::string("CREATE TABLE docs (payload ") + names[i] + ", ASYNC SEARCH INDEX idx " + columns + ") ORGANIZATION=HEAP",
          "CREATE ASYNC SEARCH INDEX idx ON docs " + columns,
          "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx " + columns};
        for (const std::string &sql : queries) {
          SCOPED_TRACE(sql);
          SCOPED_TRACE(coll);
          ObSearchIndexConfig config;
          ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(sql, config, meta));
          ASSERT_EQ(value ? 2 : 1, config.get_modes().count());
          const ObSearchIndexModeConfig &mode = config.get_modes().at(value ? 1 : 0);
          EXPECT_TRUE(mode.filter_.empty());
          EXPECT_TRUE(config.get_nested_paths_filter().empty());
          ASSERT_NO_FATAL_FAILURE(expect_parser(mode, "space", OB_FTS_INDEX_TYPE_PHRASE_MATCH));
          ObSqlString encoded;
          ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
          bool has_value = !value;
          ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::has_value_mode(encoded.string(), has_value));
          EXPECT_EQ(value, has_value);
          EXPECT_EQ(OB_NOT_SUPPORTED, config.encode(encoded, DATA_VERSION_4_6_3_0));
        }
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, strings_reject_json_options_and_binary_sources)
{
  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  for (const char *options : {"FULLTEXT(INCLUDE_PATHS=('$'))", "FULLTEXT(EXCLUDE_PATHS=('$.x'))",
      "FULLTEXT(INCLUDE_TYPES=(JSON_STRING))", "VALUE(INCLUDE_TYPES=(JSON_STRING)), FULLTEXT",
      "VALUE(INCLUDE_PATHS=('$')), FULLTEXT",
      "NESTED_PATHS=('$.items'), FULLTEXT", "TYPE=FULLTEXT, INCLUDE_PATHS=('$')"}) {
    SCOPED_TRACE(options);
    ObSearchIndexConfig config;
    const int expected = strstr(options, "FULLTEXT(INCLUDE_TYPES=") != nullptr ? OB_INVALID_ARGUMENT : OB_NOT_SUPPORTED;
    EXPECT_EQ(expected, resolve_preview_sql(std::string("CREATE ASYNC SEARCH INDEX idx ON docs "
        "(payload WITH (") + options + "))", config, meta));
  }
  for (ObObjType type : {ObVarcharType, ObCharType, ObTextType, ObIntType}) {
    meta.set_type(type);
    meta.set_collation_type(CS_TYPE_BINARY);
    EXPECT_FALSE(ObSearchIndexConfig::supports_fulltext(meta));
  }
}

TEST_F(SearchIndexConfigTest, resolver_returns_comments_independent_of_parse_tree)
{
  ObSqlString comments[2];
  {
    ObArenaAllocator allocator;
    ObParser parser(allocator, DEFAULT_MYSQL_MODE);
    ParseResult result;
    ASSERT_EQ(OB_SUCCESS, parser.parse(ObString("CREATE ASYNC SEARCH INDEX idx ON t ("
        "col1 WITH (VALUE, FULLTEXT), "
        "col2 WITH (VALUE(INCLUDE_PATHS=('$.a')), FULLTEXT(FTS_INDEX_TYPE=MATCH)))"), result));
    const ParseNode *columns = find_node(result.result_tree_, T_INDEX_COLUMN_LIST);
    ASSERT_NE(nullptr, columns);
    ASSERT_EQ(2, columns->num_child_);
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigResolver::resolve(
          find_node(columns->children_[i], T_SEARCH_INDEX_COLUMN_PARAMS),
          ObString("test"), OB_SYS_TENANT_ID, DATA_VERSION_4_6_3_1, nullptr, json_source_meta(), CS_TYPE_UTF8MB4_BIN, comments[i]));
    }
    parser.free_result(result);
  }
  for (int i = 0; i < 2; ++i) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.decode(comments[i].string()));
    ASSERT_EQ(OB_SUCCESS, config.validate());
    ASSERT_EQ(2, config.get_modes().count());
    EXPECT_TRUE(config.get_modes().at(1).filter_.empty());
    ASSERT_NO_FATAL_FAILURE(expect_parser(config.get_modes().at(1), "space",
        i == 0 ? OB_FTS_INDEX_TYPE_PHRASE_MATCH : OB_FTS_INDEX_TYPE_MATCH));
    ASSERT_NO_FATAL_FAILURE(expect_token_sizes(config.get_modes().at(1), 3, 84));
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
}

TEST_F(SearchIndexConfigTest, comment_owns_all_column_mode_views)
{
  ObSearchIndexConfig columns[2];
  for (int i = 0; i < 2; ++i) {
    ObSqlString value_filter;
    ObSqlString fulltext_filter;
    ObSqlString properties;
    ASSERT_EQ(OB_SUCCESS, value_filter.append(i == 0 ? "INCLUDE_PATHS=7$.price" : "INCLUDE_PATHS=7$.score"));
    ASSERT_EQ(OB_SUCCESS, fulltext_filter.append(i == 0
        ? "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string"
        : "INCLUDE_PATHS=6$.bodyINCLUDE_TYPES=11json_string"));
    // Exceed the temporary string's initial allocation to exercise ownership after destruction.
    ASSERT_EQ(OB_SUCCESS, properties.append_fmt("{\"test\":\"%s\"}", std::string(1024, 'x').c_str()));
    ObSearchIndexModeConfig value;
    value.filter_ = value_filter.string();
    ObSearchIndexModeConfig fulltext;
    fulltext.mode_ = i == 0 ? ObSearchIndexModeConfig::FULLTEXT_MATCH : ObSearchIndexModeConfig::FULLTEXT_PHRASE;
    fulltext.filter_ = fulltext_filter.string();
    fulltext.parser_name_ = "space.1";
    fulltext.parser_properties_ = properties.string();
    ObSEArray<ObSearchIndexModeConfig, 2> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(value));
    ASSERT_EQ(OB_SUCCESS, modes.push_back(fulltext));
    ASSERT_EQ(OB_SUCCESS, columns[i].init(modes));
  }
  for (auto &config : columns) {
    ASSERT_EQ(2, config.get_modes().count());
    const ObString comment = config.get_comment();
    const uintptr_t begin = reinterpret_cast<uintptr_t>(comment.ptr());
    const uintptr_t end = begin + comment.length();
    auto check_view = [&](const ObString &view) {
      const uintptr_t ptr = reinterpret_cast<uintptr_t>(view.ptr());
      EXPECT_GE(ptr, begin);
      EXPECT_LE(ptr + view.length(), end);
    };
    EXPECT_TRUE(config.get_nested_paths_filter().empty());
    for (int64_t i = 0; i < config.get_modes().count(); ++i) {
      const auto &mode = config.get_modes().at(i);
      check_view(mode.filter_);
      check_view(mode.parser_name_);
      check_view(mode.parser_properties_);
    }
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
    EXPECT_EQ(comment, encoded.string());
    ObSearchIndexConfig restored;
    ASSERT_EQ(OB_SUCCESS, restored.decode(encoded.string()));
    encoded.reset();
    EXPECT_EQ(comment, restored.get_comment());
    ASSERT_EQ(OB_SUCCESS, restored.decode(restored.get_comment()));
    ASSERT_EQ(OB_SUCCESS, restored.init(restored.get_modes(), restored.get_nested_paths_filter()));
    EXPECT_EQ(comment, restored.get_comment());
    EXPECT_EQ(OB_INVALID_ARGUMENT, restored.decode(ObString("SEARCH_INDEX_CONFIG=1:2:0:")));
  }
  EXPECT_EQ(ObString("INCLUDE_PATHS=7$.price"), columns[0].get_modes().at(0).filter_);
  EXPECT_EQ(ObString("INCLUDE_PATHS=7$.score"), columns[1].get_modes().at(0).filter_);
  columns[0].reset();
  EXPECT_EQ(ObString("INCLUDE_PATHS=7$.score"), columns[1].get_modes().at(0).filter_);
}

TEST_F(SearchIndexConfigTest, legacy_comment_is_owned_as_v1)
{
  ObSearchIndexConfig config;
  {
    std::string legacy = "INCLUDE_PATHS=5$.a.*NESTED_PATHS=3$.aINCLUDE_TYPES=11json_string";
    ASSERT_EQ(OB_SUCCESS, config.decode(ObString(legacy.size(), legacy.data())));
    legacy.assign(legacy.size(), 'x');
  }
  EXPECT_TRUE(ObSearchIndexConfig::is_versioned(config.get_comment()));
  ASSERT_EQ(1, config.get_modes().count());
  EXPECT_EQ(ObString("INCLUDE_PATHS=5$.a.*INCLUDE_TYPES=11json_string"), config.get_modes().at(0).filter_);
  EXPECT_EQ(ObString("NESTED_PATHS=3$.a"), config.get_nested_paths_filter());
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  EXPECT_EQ(config.get_comment(), encoded.string());
  ASSERT_EQ(OB_SUCCESS, config.decode(ObString()));
  ASSERT_EQ(1, config.get_modes().count());
  EXPECT_TRUE(config.get_nested_paths_filter().empty());
  EXPECT_TRUE(config.get_modes().at(0).filter_.empty());
  EXPECT_EQ(ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:"), config.get_comment());
}

TEST_F(SearchIndexConfigTest, print_schema_directly_from_comment)
{
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, sizeof(buf), pos, "column"));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(ObString(), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string("column"), std::string(buf, pos));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:"), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string("column"), std::string(buf, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:2:0:0:0:"), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string(" WITH (FULLTEXT)"), std::string(buf, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
      ObString("SEARCH_INDEX_CONFIG=1:2:0:0:0:0:0:2:0:0:0:"), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string(" WITH (VALUE, FULLTEXT)"), std::string(buf, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
      ObString("SEARCH_INDEX_CONFIG=1:2:0:0:18:INCLUDE_PATHS=3$.a0:0:2:0:0:0:"), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string(" WITH (VALUE (INCLUDE_PATHS = ('$.a')), FULLTEXT)"), std::string(buf, pos));
  const char *legacy[] = {"", "INCLUDE_PATHS=3$.aNESTED_PATHS=3$.a"
                            "INCLUDE_TYPES=11json_number11json_string11json_string"};
  for (int64_t i = 0; i < 2; ++i) {
    ObSearchIndexConfig normalized;
    ASSERT_EQ(OB_SUCCESS, normalized.decode(ObString(legacy[i])));
    char expected[1024];
    int64_t expected_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
        normalized.get_comment(), expected, sizeof(expected), expected_pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
        ObString(legacy[i]), buf, sizeof(buf), pos));
    EXPECT_EQ(std::string(expected, expected_pos), std::string(buf, pos));
  }
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, sizeof(buf), pos, "column"));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfig::print_schema(
      ObString("SEARCH_INDEX_CONFIG=1:2:0:0:0:0:0:2:0:0:"), buf, sizeof(buf), pos));
  EXPECT_EQ(std::string("column"), std::string(buf, pos));
}

TEST_F(SearchIndexConfigTest, single_value_preserves_legacy_formats)
{
  const struct {
    const char *options_;
    const char *legacy_;
    const char *expected_;
  } cases[] = {
    {"VALUE", "", ""},
    {"VALUE(INCLUDE_PATHS=('$.a'))", "INCLUDE_PATHS=3$.a",
     " WITH (INCLUDE_PATHS = ('$.a'))"},
    {"VALUE(INCLUDE_TYPES=(JSON_STRING))", "INCLUDE_TYPES=11json_string",
     " WITH (INCLUDE_TYPES = (json_string))"},
    {"VALUE(EXCLUDE_PATHS=('$.a'), INCLUDE_TYPES=(JSON_NUMBER))",
     "EXCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number",
     " WITH (EXCLUDE_PATHS = ('$.a'), INCLUDE_TYPES = (json_number))"},
    {"NESTED_PATHS=('$.a'), VALUE", "NESTED_PATHS=3$.a",
     " WITH (NESTED_PATHS = ('$.a'))"},
    {"NESTED_PATHS=('$.a'), VALUE(INCLUDE_TYPES=(JSON_NUMBER))",
     "NESTED_PATHS=3$.aINCLUDE_TYPES=11json_number",
     " WITH (NESTED_PATHS = ('$.a'), INCLUDE_TYPES = (json_number))"},
    {"NESTED_PATHS=('$.a'), VALUE(INCLUDE_PATHS=('$.a.b'), INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER))",
     "INCLUDE_PATHS=5$.a.bNESTED_PATHS=3$.aINCLUDE_TYPES=11json_string11json_number",
     " WITH (INCLUDE_PATHS = ('$.a.b'), NESTED_PATHS = ('$.a'), INCLUDE_TYPES = (json_string, json_number))"}
  };
  for (const auto &test : cases) {
    SCOPED_TRACE(test.options_);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(test.options_, config));
    for (const uint64_t data_version : {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
      SCOPED_TRACE(data_version);
      ObSqlString encoded;
      ASSERT_EQ(OB_SUCCESS, config.encode(encoded, data_version));
      EXPECT_EQ(data_version < DATA_VERSION_4_6_3_1 ? ObString(test.legacy_) : config.get_comment(),
                encoded.string());
    }
    const ObString comments[] = {config.get_comment(), ObString(test.legacy_)};
    for (const ObString &comment : comments) {
      char buf[1024];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, sizeof(buf), pos, "column"));
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(comment, buf, sizeof(buf), pos));
      EXPECT_EQ(std::string("column") + test.expected_, std::string(buf, pos));
    }
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
  }
}

TEST_F(SearchIndexConfigTest, value_mode_comment_contains_column_nested)
{
  ObSearchIndexModeConfig value;
  value.filter_ = "INCLUDE_PATHS=9$.items.*INCLUDE_TYPES=11json_number11json_string";
  ObSearchIndexModeConfig fulltext;
  fulltext.mode_ = ObSearchIndexModeConfig::FULLTEXT_PHRASE;
  fulltext.filter_ = "INCLUDE_PATHS=7$.title";
  for (int64_t value_position = 0; value_position < 2; ++value_position) {
    ObSEArray<ObSearchIndexModeConfig, 2> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(value_position == 0 ? value : fulltext));
    ASSERT_EQ(OB_SUCCESS, modes.push_back(value_position == 0 ? fulltext : value));
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.init(modes, ObString("NESTED_PATHS=7$.items")));
    ObSqlString comment;
    bool has_value = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
        config.get_comment(), comment, has_value));
    config.reset();
    ASSERT_TRUE(has_value);
    ObSearchIndexConfig extracted;
    ASSERT_EQ(OB_SUCCESS, extracted.decode(comment.string()));
    ASSERT_EQ(1, extracted.get_modes().count());
    EXPECT_EQ(value.filter_, extracted.get_modes().at(0).filter_);
    EXPECT_EQ(ObString("NESTED_PATHS=7$.items"), extracted.get_nested_paths_filter());
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(comment.string()));
    EXPECT_TRUE(filter.has_nested_paths());
    EXPECT_EQ(ObSearchIndexConfigFilter::TYPE_MASK_STRING | ObSearchIndexConfigFilter::TYPE_MASK_NUMBER,
              filter.get_type_mask());
  }
}

TEST_F(SearchIndexConfigTest, value_mode_comment_defaults_absence_and_invalid_wire)
{
  ObSqlString comment;
  bool has_value = false;
  const ObString legacy("INCLUDE_PATHS=3$.aNESTED_PATHS=3$.aINCLUDE_TYPES=11json_string");
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(legacy, comment, has_value));
  EXPECT_TRUE(has_value);
  ObSearchIndexConfig normalized;
  ASSERT_EQ(OB_SUCCESS, normalized.decode(legacy));
  EXPECT_EQ(normalized.get_comment(), comment.string());
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:"), comment, has_value));
  EXPECT_TRUE(has_value);
  EXPECT_EQ(ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:"), comment.string());
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
      ObString("SEARCH_INDEX_CONFIG=1:1:0:2:0:0:0:"), comment, has_value));
  EXPECT_FALSE(has_value);
  EXPECT_TRUE(comment.empty());
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(ObString(), comment, has_value));
  EXPECT_TRUE(has_value);
  EXPECT_EQ(ObString("SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:"), comment.string());
  const char *invalid[] = {
      "SEARCH_INDEX_CONFIG=2:1:0:0:0:0:0:",
      "SEARCH_INDEX_CONFIG=1:2:0:0:0:0:0:2:0:0:",
      "SEARCH_INDEX_CONFIG=1:1:0:0:0:0:0:trailing"};
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, comment.append("previous result"));
    EXPECT_NE(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
        ObString(invalid[i]), comment, has_value));
  }
}

TEST_F(SearchIndexConfigTest, parser_identity_validation_ignores_case_and_version)
{
  ObSearchIndexModeConfig first;
  first.mode_ = ObSearchIndexModeConfig::FULLTEXT_MATCH;
  first.filter_ = "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string";
  first.parser_name_ = "space.1";
  first.parser_properties_ = "{}";
  ObSearchIndexModeConfig second = first;
  second.filter_ = "INCLUDE_PATHS=3$.bINCLUDE_TYPES=11json_string";
  second.parser_name_ = "ngram.1";
  ObSearchIndexConfig valid;
  ObSEArray<ObSearchIndexModeConfig, 2> modes;
  ASSERT_EQ(OB_SUCCESS, modes.push_back(first));
  ASSERT_EQ(OB_SUCCESS, modes.push_back(second));
  ASSERT_EQ(OB_SUCCESS, valid.init(modes));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, valid.encode(encoded));
  const std::string wire(encoded.ptr(), encoded.length());
  std::string overlapping = wire;
  const size_t path_pos = overlapping.find("3$.b");
  ASSERT_NE(std::string::npos, path_pos);
  overlapping.replace(path_pos, strlen("3$.b"), "3$.a");
  ObSearchIndexConfig decoded_paths;
  ASSERT_EQ(OB_SUCCESS, decoded_paths.decode(ObString(overlapping.size(), overlapping.data())));
  EXPECT_EQ(2, decoded_paths.get_modes().count());
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded_paths.validate());
  const size_t parser_pos = wire.find("ngram.1");
  ASSERT_NE(std::string::npos, parser_pos);
  for (const char *name : {"space.2", "SPACE.1", "SPACE.2"}) {
    SCOPED_TRACE(name);
    second.parser_name_ = ObString::make_string(name);
    ObSearchIndexConfig invalid;
    modes.at(1) = second;
    ASSERT_EQ(OB_SUCCESS, invalid.init(modes));
    EXPECT_EQ(OB_INVALID_ARGUMENT, invalid.validate());
    ObSqlString rejected;
    EXPECT_EQ(OB_INVALID_ARGUMENT, invalid.encode(rejected));
    std::string duplicate = wire;
    duplicate.replace(parser_pos, strlen("ngram.1"), name);
    ObSearchIndexConfig decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(ObString(duplicate.size(), duplicate.data())));
    EXPECT_EQ(2, decoded.get_modes().count());
    EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.validate());
  }
}

TEST_F(SearchIndexConfigTest, invalid_options)
{
  const char *invalid[] = {
    "FULLTEXT (INCLUDE_TYPES=(JSON_STRING))",
    "TYPE=FULLTEXT, INCLUDE_TYPES=(JSON_STRING)",
    "TYPE=VALUE, PARSER='space'",
    "TYPE=VALUE, FTS_INDEX_TYPE=MATCH",
    "TYPE=FULLTEXT, NESTED_PATHS=('$.a')",
    "FULLTEXT (INCLUDE_TYPES=(JSON_NUMBER))",
    "FULLTEXT (INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER))",
    "VALUE (PARSER='space')",
    "VALUE (FTS_INDEX_TYPE=MATCH)",
    "FULLTEXT (PARSER='space', PARSER='ngram')",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), EXCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a'), INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (PARSER='analyzer')",
    "FULLTEXT (PARSER='analyzer', PARSER_PROPERTIES=(min_token_size=1))",
    "FULLTEXT (PARSER='space', PARSER_PROPERTIES=(analysis='{\"analyzer\":\"standard\"}'))",
    "FULLTEXT (PARSER='no_such_parser')",
    "FULLTEXT (PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=0))",
    "FULLTEXT (PARSER='space', PARSER_PROPERTIES=(min_token_size=5,max_token_size=2))",
    "FULLTEXT (PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=2,ngram_token_size=3))",
    "FULLTEXT (FTS_INDEX_TYPE=FILTER)",
    "FULLTEXT (NESTED_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.**'))",
    "FULLTEXT (INCLUDE_PATHS=('$**.a'))",
    "TYPE=FULLTEXT, EXCLUDE_PATHS=('$.a.*.b')",
    "FULLTEXT (INCLUDE_PATHS=('$.a[*]'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*.b'))"
  };
  for (const char *sql : invalid) {
    SCOPED_TRACE(sql);
    ObSearchIndexConfig config;
    EXPECT_NE(OB_SUCCESS, resolve(sql, config));
  }

}

TEST_F(SearchIndexConfigTest, path_set_conflicts)
{
  // The parser names differ so these cases exercise path-set validation itself.
  const char *conflicts[] = {
    "VALUE, FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), VALUE, FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a')), VALUE",
    "FULLTEXT, FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$'))",
    "FULLTEXT (EXCLUDE_PATHS=('$')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a'))",
    "FULLTEXT, FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.*'))",
    "FULLTEXT, FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a'))",
    "FULLTEXT, FULLTEXT (PARSER='ngram')",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.\"a\"'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.\"a\".*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.b.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.b.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a'))",
    "FULLTEXT (EXCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.b'))",
    "FULLTEXT (EXCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.b.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.b.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (EXCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.b')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a', '$.b')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a'))"
  };
  for (const char *sql : conflicts) {
    SCOPED_TRACE(sql);
    ObSearchIndexConfig config;
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolve(sql, config));
  }
  const char *valid[] = {
    "VALUE, FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), VALUE, FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b')), VALUE",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.b')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.b'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.b')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.b.*')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (EXCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.ab.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.b.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a.c.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a.*')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b.*'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', EXCLUDE_PATHS=('$.\"a\"'))",
    "FULLTEXT (EXCLUDE_PATHS=('$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.a'))",
    "FULLTEXT (INCLUDE_PATHS=('$.a', '$.a')), FULLTEXT (PARSER='ngram', INCLUDE_PATHS=('$.b'))",
    "FULLTEXT, VALUE"
  };
  for (const char *sql : valid) {
    SCOPED_TRACE(sql);
    ObSearchIndexConfig config;
    EXPECT_EQ(OB_SUCCESS, resolve(sql, config));
  }
}

TEST_F(SearchIndexConfigTest, fulltext_root_overlap_matches_runtime_filters)
{
  for (const char *excluded : {"$", "$.*"}) {
    for (const char *first_type : {"MATCH", "PHRASE_MATCH"}) {
      for (const char *second_type : {"MATCH", "PHRASE_MATCH"}) {
        const std::string first = std::string("FULLTEXT(EXCLUDE_PATHS=('") + excluded
            + "'), FTS_INDEX_TYPE=" + first_type + ")";
        for (const char *option : {"", "EXCLUDE_PATHS=('$'), ", "EXCLUDE_PATHS=('$.*'), ",
                                   "INCLUDE_PATHS=('$'), ", "INCLUDE_PATHS=('$.*'), ",
                                   "INCLUDE_PATHS=('$.a'), "}) {
          const bool conflict = std::string(option).find("INCLUDE_PATHS") == std::string::npos;
          const std::string second = std::string("FULLTEXT(") + option
              + "PARSER='ngram', FTS_INDEX_TYPE=" + second_type + ")";
          ObSearchIndexConfig left;
          ObSearchIndexConfig right;
          ASSERT_EQ(OB_SUCCESS, resolve(first, left));
          ASSERT_EQ(OB_SUCCESS, resolve(second, right));
          ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> root;
          ObSearchIndexConfigFilter left_filter(OB_SYS_TENANT_ID);
          ObSearchIndexConfigFilter right_filter(OB_SYS_TENANT_ID);
          ASSERT_EQ(OB_SUCCESS, left_filter.init_from_comment(left.get_comment(), 0));
          ASSERT_EQ(OB_SUCCESS, right_filter.init_from_comment(right.get_comment(), 0));
          bool indexed = false;
          ASSERT_EQ(OB_SUCCESS, left_filter.is_indexed(root, ObJsonNodeType::J_STRING, indexed));
          EXPECT_TRUE(indexed);
          ASSERT_EQ(OB_SUCCESS, right_filter.is_indexed(root, ObJsonNodeType::J_STRING, indexed));
          EXPECT_EQ(conflict, indexed);
          ObSqlString left_wire;
          ObSqlString right_wire;
          ASSERT_EQ(OB_SUCCESS, left.encode(left_wire));
          ASSERT_EQ(OB_SUCCESS, right.encode(right_wire));
          const size_t prefix_len = strlen("SEARCH_INDEX_CONFIG=1:1:0:");
          const std::string left_mode = std::string(left_wire.ptr(), left_wire.length()).substr(prefix_len);
          const std::string right_mode = std::string(right_wire.ptr(), right_wire.length()).substr(prefix_len);
          for (bool reversed : {false, true}) {
            const std::string sql = reversed ? second + ", " + first : first + ", " + second;
            SCOPED_TRACE(sql);
            const int expected = conflict ? OB_INVALID_ARGUMENT : OB_SUCCESS;
            ObSearchIndexConfig config;
            EXPECT_EQ(expected, resolve(sql, config));
            const std::string wire = "SEARCH_INDEX_CONFIG=1:2:0:"
                + (reversed ? right_mode + left_mode : left_mode + right_mode);
            ASSERT_EQ(OB_SUCCESS, config.decode(ObString(wire.size(), wire.data())));
            EXPECT_EQ(expected, config.validate());
          }
        }
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, fulltext_wildcard_complements_match_runtime_paths)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (INCLUDE_PATHS=('$.a.*')), "
      "FULLTEXT (EXCLUDE_PATHS=('$.a.*'), PARSER='ngram')", config));
  struct Case
  {
    std::vector<const char *> members;
    bool included;
  };
  // nullptr represents an array marker in the generator's encoded path.
  const Case cases[] = {
    {{"a"}, false}, {{"a", "b"}, true}, {{"a", "b", "c"}, true},
    {{"ab", "b"}, false}, {{"x", "a", "b"}, false},
    {{"a", nullptr}, true}, {{"a", nullptr, "b"}, true}
  };
  for (int64_t i = 0; i < config.get_modes().count(); ++i) {
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), i));
    for (const auto &item : cases) {
      ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path;
      for (const char *member : item.members) {
        ASSERT_EQ(OB_SUCCESS, path.push_back(member == nullptr
            ? ObSearchIndexPathEncoder::make_array_path()
            : ObSearchIndexPathEncoder::make_object_path(ObString(member))));
      }
      bool passed = false;
      ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_STRING, passed));
      EXPECT_EQ(i == 0 ? item.included : !item.included, passed);
      ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_INT, passed));
      EXPECT_FALSE(passed);
    }
  }
}

TEST_F(SearchIndexConfigTest, analyzer_schema_round_trip)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve(
      "FULLTEXT (PARSER='analyzer', PARSER_PROPERTIES=(analysis='{\"analyzer\":\"standard\"}'))", config));
  expect_parser(config.get_modes().at(0), "analyzer", OB_FTS_INDEX_TYPE_PHRASE_MATCH);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, large_schema_config)
{
  std::string paths;
  for (int i = 0; i < 150; ++i) {
    if (i > 0) { paths += ","; }
    paths += "'$.long_field_name_" + std::to_string(i) + "'";
  }
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (INCLUDE_PATHS=(" + paths + "))", config));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  ASSERT_GT(encoded.length(), MAX_COLUMN_COMMENT_LENGTH);
  ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
}

TEST_F(SearchIndexConfigTest, old_and_new_syntax_encode_the_same_v1)
{
  const char *old_options[] = {"", "INCLUDE_PATHS=('$.a'), INCLUDE_TYPES=(JSON_NUMBER)",
      "NESTED_PATHS=('$.items'), INCLUDE_PATHS=('$.items.price'), INCLUDE_TYPES=(JSON_NUMBER)"};
  const char *new_options[] = {"VALUE", "VALUE(INCLUDE_PATHS=('$.a'), INCLUDE_TYPES=(JSON_NUMBER))",
      "NESTED_PATHS=('$.items'), VALUE(INCLUDE_PATHS=('$.items.price'), INCLUDE_TYPES=(JSON_NUMBER))"};
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(old_options[i]);
    std::string sql = "CREATE ASYNC SEARCH INDEX idx ON docs (payload";
    if (old_options[i][0] != '\0') {
      sql += std::string(" WITH (") + old_options[i] + ")";
    }
    sql += ")";
    ObSearchIndexConfig old_config;
    ObSearchIndexConfig new_config;
    ASSERT_EQ(OB_SUCCESS, resolve_sql(sql, old_config));
    ASSERT_EQ(OB_SUCCESS, resolve(new_options[i], new_config));
    ObSqlString old_encoded;
    ObSqlString new_encoded;
    ASSERT_EQ(OB_SUCCESS, old_config.encode(old_encoded));
    ASSERT_EQ(OB_SUCCESS, new_config.encode(new_encoded));
    EXPECT_TRUE(ObSearchIndexConfig::is_versioned(old_encoded.string()));
    EXPECT_EQ(old_encoded.string(), new_encoded.string());
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(old_config));
  }
}

TEST_F(SearchIndexConfigTest, compare_legacy_and_v1_comments)
{
  const struct {
    const char *legacy;
    const char *options;
  } cases[] = {
    {"", "VALUE"},
    {"INCLUDE_TYPES=11json_string11json_number",
      "TYPE=VALUE, INCLUDE_TYPES=(JSON_NUMBER, JSON_STRING, JSON_NUMBER)"},
    {"INCLUDE_TYPES=11json_number11json_string",
      "VALUE(INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER))"},
    {"INCLUDE_TYPES=11json_number11json_string11json_number",
      "TYPE=VALUE, INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER)"},
    {"INCLUDE_PATHS=3$.a3$.bNESTED_PATHS=3$.aINCLUDE_TYPES=11json_number11json_string11json_string",
      "NESTED_PATHS=('$.a'), VALUE(INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER), INCLUDE_PATHS=('$.a', '$.b'))"},
    {"EXCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number11json_number",
      "TYPE=VALUE, INCLUDE_TYPES=(JSON_NUMBER), EXCLUDE_PATHS=('$.a')"}
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.legacy);
    ObSearchIndexConfig current;
    ObSearchIndexConfig converted;
    ASSERT_EQ(OB_SUCCESS, resolve(item.options, current));
    ASSERT_EQ(OB_SUCCESS, converted.decode(ObString(item.legacy)));
    EXPECT_EQ(current.get_comment(), converted.get_comment());
    const ObString comments[] = {ObString(item.legacy), current.get_comment()};
    for (const ObString &left : comments) {
      for (const ObString &right : comments) {
        bool equal = false;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(left, right, equal));
        EXPECT_TRUE(equal);
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, compare_legacy_comments_preserves_byte_comparison)
{
  const ObString ordered("INCLUDE_TYPES=11json_string11json_number");
  const char *legacy[] = {
    "INCLUDE_TYPES=11json_number11json_string",
    "INCLUDE_TYPES=11json_string11json_number11json_number"
  };
  for (const char *raw : legacy) {
    SCOPED_TRACE(raw);
    const ObString comment(raw);
    bool equal = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(comment, comment, equal));
    EXPECT_TRUE(equal);
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(comment, ordered, equal));
    EXPECT_FALSE(equal);
    equal = true;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(ordered, comment, equal));
    EXPECT_FALSE(equal);
  }
}

TEST_F(SearchIndexConfigTest, compare_comments_preserves_definition_differences)
{
  const char *options[][2] = {
    {"VALUE", "FULLTEXT"},
    {"VALUE", "VALUE(INCLUDE_TYPES=(JSON_STRING, JSON_NUMBER))"},
    {"VALUE(INCLUDE_TYPES=(JSON_STRING))", "VALUE(INCLUDE_TYPES=(JSON_NUMBER))"},
    {"VALUE(INCLUDE_PATHS=('$.a'))", "VALUE(EXCLUDE_PATHS=('$.a'))"},
    {"VALUE(INCLUDE_PATHS=('$.a'))", "VALUE(INCLUDE_PATHS=('$.b'))"},
    {"VALUE(INCLUDE_PATHS=('$.a', '$.b'))", "VALUE(INCLUDE_PATHS=('$.b', '$.a'))"},
    {"NESTED_PATHS=('$.a'), VALUE", "NESTED_PATHS=('$.b'), VALUE"},
    {"FULLTEXT", "FULLTEXT(FTS_INDEX_TYPE=MATCH)"},
    {"FULLTEXT(PARSER='space')", "FULLTEXT(PARSER='ngram')"},
    {"FULLTEXT(PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=2))",
      "FULLTEXT(PARSER='ngram', PARSER_PROPERTIES=(ngram_token_size=3))"},
    {"VALUE", "VALUE, FULLTEXT"},
    {"VALUE, FULLTEXT", "FULLTEXT, VALUE"}
  };
  for (const auto &pair : options) {
    SCOPED_TRACE(::testing::Message() << pair[0] << " / " << pair[1]);
    ObSearchIndexConfig left;
    ObSearchIndexConfig right;
    ASSERT_EQ(OB_SUCCESS, resolve(pair[0], left));
    ASSERT_EQ(OB_SUCCESS, resolve(pair[1], right));
    bool equal = true;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(left.get_comment(), right.get_comment(), equal));
    EXPECT_FALSE(equal);
  }
}

TEST_F(SearchIndexConfigTest, compare_mixed_comments_propagates_legacy_decode_errors)
{
  ObSearchIndexConfig current;
  ASSERT_EQ(OB_SUCCESS, resolve("VALUE", current));
  const char *invalid[] = {
    "INCLUDE_TYPES=100unknown", "NESTED_PATHS=100$.a"
  };
  for (const char *comment : invalid) {
    SCOPED_TRACE(comment);
    bool equal = true;
    EXPECT_NE(OB_SUCCESS, ObSearchIndexConfig::is_equal(ObString(comment), current.get_comment(), equal));
    EXPECT_FALSE(equal);
    equal = true;
    EXPECT_NE(OB_SUCCESS, ObSearchIndexConfig::is_equal(current.get_comment(), ObString(comment), equal));
    EXPECT_FALSE(equal);
  }
}

TEST_F(SearchIndexConfigTest, legacy_and_scalar_consumer)
{
  const char *legacy[] = {"", "INCLUDE_PATHS=3$.a", "NESTED_PATHS=3$.b",
      "INCLUDE_TYPES=11json_number", "INCLUDE_PATHS=3$.aNESTED_PATHS=3$.b",
      "NESTED_PATHS=3$.bINCLUDE_TYPES=11json_number",
      "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string",
      "EXCLUDE_PATHS=3$.aNESTED_PATHS=3$.bINCLUDE_TYPES=11json_number",
      "INCLUDE_PATHS=3$.aNESTED_PATHS=3$.bINCLUDE_TYPES=11json_number11json_string11json_string"};
  for (const char *raw : legacy) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.decode(ObString(raw)));
    ASSERT_EQ(1, config.get_modes().count());
    EXPECT_EQ(ObSearchIndexModeConfig::VALUE, config.get_modes().at(0).mode_);
    ObSearchIndexConfigFilter legacy_filter(OB_SYS_TENANT_ID);
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, legacy_filter.init_from_comment(config.get_comment()));
    for (bool reencode : {false, true}) {
      if (reencode) {
        ObSqlString encoded;
        ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
        EXPECT_TRUE(ObSearchIndexConfig::is_versioned(encoded.string()));
        ASSERT_EQ(OB_SUCCESS, config.decode(encoded.string()));
      }
      ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), 0));
      EXPECT_EQ(legacy_filter.get_type_mask(), filter.get_type_mask());
      EXPECT_EQ(legacy_filter.has_nested_paths(), filter.has_nested_paths());
      for (const char *member : {"a", "b"}) {
        ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> path;
        ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(member))));
        bool expected = false;
        bool actual = false;
        ASSERT_EQ(OB_SUCCESS, legacy_filter.is_nested_path(path, expected));
        ASSERT_EQ(OB_SUCCESS, filter.is_nested_path(path, actual));
        EXPECT_EQ(expected, actual);
        for (ObJsonNodeType type : {ObJsonNodeType::J_STRING, ObJsonNodeType::J_INT}) {
          ASSERT_EQ(OB_SUCCESS, legacy_filter.is_indexed(path, type, expected));
          ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, type, actual));
          EXPECT_EQ(expected, actual);
        }
      }
    }
  }
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve_sql("CREATE ASYNC SEARCH INDEX idx ON docs "
      "(payload WITH (INCLUDE_PATHS=('$.a'), INCLUDE_TYPES=(JSON_NUMBER)))", config));
  EXPECT_EQ(ObSearchIndexModeConfig::VALUE, config.get_modes().at(0).mode_);
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT", config));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  ObSearchIndexConfigFilter scalar(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, scalar.init_from_comment(encoded.string()));
  EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_STRING), scalar.get_type_mask());
  EXPECT_EQ(OB_INVALID_ARGUMENT, scalar.init_from_comment(encoded.string(), -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scalar.init_from_comment(encoded.string(), 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scalar.init_from_comment(ObString("INCLUDE_TYPES=11json_string")));
  ObSEArray<ObString, 4> nested;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigFilter::parse_nested_paths(
      config.get_nested_paths_filter(), nested));
  EXPECT_TRUE(nested.empty());
}

TEST_F(SearchIndexConfigTest, decode_does_not_validate_parser_properties)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT(PARSER_PROPERTIES=(min_token_size=2))", config));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  const ObString &properties = config.get_modes().at(0).parser_properties_;
  ASSERT_FALSE(properties.empty());
  std::string wire(encoded.ptr(), encoded.length());
  const size_t properties_pos = wire.rfind(std::string(properties.ptr(), properties.length()));
  ASSERT_NE(std::string::npos, properties_pos);
  wire.at(properties_pos) = '#'; // Keep the wire lengths intact, but invalidate the JSON.
  ObSearchIndexConfig decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(ObString(wire.size(), wire.data())));
  ASSERT_EQ(1, decoded.get_modes().count());
  EXPECT_EQ('#', decoded.get_modes().at(0).parser_properties_.ptr()[0]);
  EXPECT_NE(OB_SUCCESS, decoded.validate());
  ObSqlString rejected;
  EXPECT_NE(OB_SUCCESS, decoded.encode(rejected));
}

TEST_F(SearchIndexConfigTest, malformed_or_future_encoding)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT", config));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  ObSearchIndexConfig decoded;
  for (int64_t len = strlen("SEARCH_INDEX_CONFIG="); len < encoded.length(); ++len) {
    SCOPED_TRACE(len);
    EXPECT_NE(OB_SUCCESS, decoded.decode(ObString(len, encoded.ptr())));
  }
  // Legacy comments have no prefix; version 0 in a versioned comment is invalid.
  EXPECT_EQ(OB_NOT_SUPPORTED, decoded.decode(ObString("SEARCH_INDEX_CONFIG=0:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_NOT_SUPPORTED, decoded.decode(ObString("SEARCH_INDEX_CONFIG=2:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_NOT_SUPPORTED, decoded.decode(ObString("SEARCH_INDEX_CONFIG=3:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(ObString("SEARCH_INDEX_CONFIG=1:999999999999999999999:")));
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(ObString("SEARCH_INDEX_CONFIG=1:1:0:9:0:0:0:0:")));
  // Only the three final mode IDs are accepted.
  std::string removed_mode(encoded.ptr(), encoded.length());
  removed_mode.at(strlen("SEARCH_INDEX_CONFIG=1:1:0:")) = '3';
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(ObString(removed_mode.size(), removed_mode.data())));
  const std::string trailing = std::string(encoded.ptr(), encoded.length()) + "extra";
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.decode(ObString(trailing.size(), trailing.data())));
}

TEST_F(SearchIndexConfigTest, column_nested_schema_round_trip_and_conflicts)
{
  ObSearchIndexConfig config;
  const char *valid[] = {
    "NESTED_PATHS=('$.comments'), VALUE",
    "TYPE=VALUE, NESTED_PATHS=('$.comments')"
  };
  for (const char *modes : valid) {
    SCOPED_TRACE(modes);
    ASSERT_EQ(OB_SUCCESS, resolve(modes, config));
    ASSERT_NO_FATAL_FAILURE(expect_round_trip(config));
    for (int64_t i = 0; i < config.get_modes().count(); ++i) {
      const ObSearchIndexModeConfig &mode = config.get_modes().at(i);
      ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
      EXPECT_EQ(std::string::npos, std::string(mode.filter_.empty() ? "" : mode.filter_.ptr(), mode.filter_.length()).find("NESTED_PATHS"));
      ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), i));
      EXPECT_EQ(!mode.is_fulltext(), filter.has_nested_paths());
    }
  }
  const char *invalid[] = {
    "FULLTEXT, NESTED_PATHS=('$.comments'), VALUE",
    "VALUE, FULLTEXT, NESTED_PATHS=('$.comments')",
    "NESTED_PATHS=('$.comments'), FULLTEXT",
    "FULLTEXT, NESTED_PATHS=('$.comments')",
    "VALUE (NESTED_PATHS=('$.comments'))",
    "VALUE (NESTED_PATHS=('$.comments')), FULLTEXT",
    "FULLTEXT, VALUE (NESTED_PATHS=('$.comments'))",
    "FULLTEXT (NESTED_PATHS=('$.comments'))",
    "TYPE=FULLTEXT, NESTED_PATHS=('$.comments')",
    "VALUE, FULLTEXT (NESTED_PATHS=('$.comments'))"
  };
  for (const char *modes : invalid) {
    SCOPED_TRACE(modes);
    EXPECT_EQ(OB_NOT_SUPPORTED, resolve(modes, config));
  }
  // Decoding restores the fields; DDL validation enforces the block-local restriction.
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT", config));
  ObSearchIndexModeConfig mode = config.get_modes().at(0);
  mode.filter_ = "NESTED_PATHS=10$.commentsINCLUDE_TYPES=11json_string";
  ObSearchIndexConfig invalid_config;
  ObSEArray<ObSearchIndexModeConfig, 1> invalid_modes;
  ASSERT_EQ(OB_SUCCESS, invalid_modes.push_back(mode));
  ASSERT_EQ(OB_SUCCESS, invalid_config.init(invalid_modes));
  EXPECT_EQ(OB_NOT_SUPPORTED, invalid_config.validate());
  ObSqlString raw;
  ASSERT_EQ(OB_SUCCESS, raw.append_fmt("SEARCH_INDEX_CONFIG=1:1:0:%d:%d:%.*s%d:%.*s%d:%.*s",
      mode.mode_, mode.filter_.length(), mode.filter_.length(), mode.filter_.ptr(),
      mode.parser_name_.length(), mode.parser_name_.length(), mode.parser_name_.ptr(),
      mode.parser_properties_.length(), mode.parser_properties_.length(), mode.parser_properties_.ptr()));
  ASSERT_EQ(OB_SUCCESS, invalid_config.decode(raw.string()));
  EXPECT_EQ(OB_NOT_SUPPORTED, invalid_config.validate());
}

TEST_F(SearchIndexConfigTest, column_nested_rejects_fulltext_for_all_ddl_forms)
{
  const char *prefixes[] = {
    "CREATE TABLE docs (payload JSON, ASYNC SEARCH INDEX idx ",
    "CREATE ASYNC SEARCH INDEX idx ON docs ",
    "ALTER TABLE docs ADD ASYNC SEARCH INDEX idx "
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(prefixes); ++i) {
    for (const char *fts_type : {"MATCH", "PHRASE_MATCH"}) {
      for (const char *value : {"", "VALUE, "}) {
        const std::string sql = std::string(prefixes[i])
            + "(payload WITH (NESTED_PATHS=('$.items'), " + value
            + "FULLTEXT(INCLUDE_PATHS=('$.title'), FTS_INDEX_TYPE=" + fts_type + ")))"
            + (i == 0 ? ") ORGANIZATION=HEAP" : "");
        SCOPED_TRACE(sql);
        ObSearchIndexConfig config;
        EXPECT_EQ(OB_NOT_SUPPORTED, resolve_sql(sql, config));
      }
    }
  }
  // Decoding remains independent of DDL validation for persisted configurations.
  for (const char *fts_type : {"MATCH", "PHRASE_MATCH"}) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve(std::string("FULLTEXT(FTS_INDEX_TYPE=") + fts_type + ")", config));
    ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
    EXPECT_EQ(OB_NOT_SUPPORTED, config.validate());
    EXPECT_EQ(OB_NOT_SUPPORTED, ObSearchIndexConfig::validate(config.get_comment()));
    ObSqlString encoded;
    EXPECT_EQ(OB_NOT_SUPPORTED, config.encode(encoded));
  }
  ObSearchIndexConfig config;
  EXPECT_NE(OB_SUCCESS, resolve("NESTED_PATHS=('$.items'), VALUE, NESTED_PATHS=('$.other')", config));
  for (const char *path : {"$", "$.items.*", "$.items[*]"}) {
    EXPECT_NE(OB_SUCCESS, resolve(std::string("NESTED_PATHS=('") + path + "'), VALUE", config));
  }
}

TEST_F(SearchIndexConfigTest, column_nested_config_and_legacy_decode)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("NESTED_PATHS=('$.items'), VALUE", config));
  EXPECT_EQ(1, config.get_modes().count());
  EXPECT_EQ(ObSearchIndexModeConfig::VALUE, config.get_modes().at(0).mode_);
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
  EXPECT_EQ(0, std::string(encoded.ptr(), encoded.length()).find("SEARCH_INDEX_CONFIG=1:"));
  ObSearchIndexConfig decoded;
  for (int64_t len = strlen("SEARCH_INDEX_CONFIG="); len < encoded.length(); ++len) {
    SCOPED_TRACE(len);
    EXPECT_NE(OB_SUCCESS, decoded.decode(ObString(len, encoded.ptr())));
  }
  const std::string trailing = std::string(encoded.ptr(), encoded.length()) + "extra";
  EXPECT_NE(OB_SUCCESS, decoded.decode(ObString(trailing.size(), trailing.data())));
  ObSqlString no_nested;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT", config));
  ASSERT_EQ(OB_SUCCESS, config.encode(no_nested));
  EXPECT_EQ(0, std::string(no_nested.ptr(), no_nested.length()).find("SEARCH_INDEX_CONFIG=1:1:0:"));
  ASSERT_EQ(OB_SUCCESS, decoded.decode(no_nested.string()));
  EXPECT_TRUE(decoded.get_nested_paths_filter().empty());
  const std::string raw(encoded.ptr(), encoded.length());
  // Other development versions are not supported recovery formats.
  std::string discarded_version = raw;
  discarded_version.at(strlen("SEARCH_INDEX_CONFIG=")) = '3';
  EXPECT_EQ(OB_NOT_SUPPORTED, decoded.decode(
      ObString(discarded_version.size(), discarded_version.data())));
  const size_t nested_pos = raw.find("NESTED_PATHS=7$.items");
  ASSERT_NE(std::string::npos, nested_pos);
  const std::string mode_bytes = raw.substr(nested_pos + strlen("NESTED_PATHS=7$.items"));
  for (const char *nested : {"INCLUDE_PATHS=7$.items", "NESTED_PATHS=1$", "NESTED_PATHS=9$.items.*"}) {
    const std::string invalid = "SEARCH_INDEX_CONFIG=1:1:" + std::to_string(strlen(nested))
        + ":" + nested + mode_bytes;
    SCOPED_TRACE(invalid);
    ASSERT_EQ(OB_SUCCESS, decoded.decode(ObString(invalid.size(), invalid.data())));
    EXPECT_NE(OB_SUCCESS, decoded.validate());
  }
  // Only the pre-FULLTEXT unversioned VALUE filter remains a compatibility input.
  const ObString old_filter("INCLUDE_PATHS=7$.titleNESTED_PATHS=7$.itemsINCLUDE_TYPES=11json_string");
  ASSERT_EQ(OB_SUCCESS, decoded.decode(old_filter));
  EXPECT_EQ(ObString("NESTED_PATHS=7$.items"), decoded.get_nested_paths_filter());
  EXPECT_EQ(ObString("INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string"),
      decoded.get_modes().at(0).filter_);
  encoded.reset();
  ASSERT_EQ(OB_SUCCESS, decoded.encode(encoded));
  EXPECT_TRUE(ObSearchIndexConfig::is_versioned(encoded.string()));
  ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(decoded.get_comment(), 0));
  EXPECT_TRUE(filter.has_nested_paths());
  EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_STRING), filter.get_type_mask());
}

TEST_F(SearchIndexConfigTest, decoded_filters_preserve_runtime_matching)
{
  struct Case
  {
    const char *filter;
    const char *first;
    const char *second;
    ObJsonNodeType type;
    bool expected;
  };
  const Case cases[] = {
    {"INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string", "a", nullptr, ObJsonNodeType::J_STRING, true},
    {"INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string", "a", nullptr, ObJsonNodeType::J_INT, false},
    {"INCLUDE_PATHS=3$.a", "a", "b", ObJsonNodeType::J_STRING, false},
    {"INCLUDE_PATHS=3$.a", "x", "a", ObJsonNodeType::J_STRING, false},
    {"INCLUDE_PATHS=5$.a.*", "a", "b", ObJsonNodeType::J_STRING, true},
    {"EXCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number", "a", nullptr, ObJsonNodeType::J_INT, false},
    {"EXCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number", "b", nullptr, ObJsonNodeType::J_INT, true},
    {"EXCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number", "b", nullptr, ObJsonNodeType::J_STRING, false}
  };
  ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
  for (const auto &item : cases) {
    SCOPED_TRACE(item.filter);
    ObSearchIndexModeConfig mode;
    mode.filter_ = ObString(item.filter);
    ObSearchIndexConfig config;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ASSERT_EQ(OB_SUCCESS, config.init(modes));
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, config.encode(encoded));
    ObSearchIndexConfig decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(encoded.string()));
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(decoded.get_comment(), 0));
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 2> path;
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(item.first))));
    if (item.second != nullptr) {
      ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(item.second))));
    }
    bool passed = false;
    ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, item.type, passed));
    EXPECT_EQ(item.expected, passed);
  }
  // Both modes consume the same single-block filter; FULLTEXT fixes the type to JSON_STRING.
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, resolve("FULLTEXT (INCLUDE_PATHS=('$.a')), VALUE (INCLUDE_PATHS=('$.a'))", config));
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> path;
  ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a"))));
  for (int64_t i = 0; i < config.get_modes().count(); ++i) {
    const auto &mode = config.get_modes().at(i);
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment(), i));
    bool passed = false;
    ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_STRING, passed));
    EXPECT_TRUE(passed);
    ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_INT, passed));
    EXPECT_EQ(!mode.is_fulltext(), passed);
  }
}

TEST_F(SearchIndexConfigTest, legacy_plain_comments_use_default_value)
{
  ObSearchIndexConfig defaults;
  ASSERT_EQ(OB_SUCCESS, defaults.decode(ObString()));
  for (const char *raw : {"业务数据", "unrelated comment", "prefix INCLUDE_PATHS=3$.a", "INCLUDE_PATHS"}) {
    SCOPED_TRACE(raw);
    const ObString comment(raw);
    ObSearchIndexConfig legacy;
    ASSERT_EQ(OB_SUCCESS, legacy.decode(comment));
    EXPECT_EQ(defaults.get_comment(), legacy.get_comment());

    ObSqlString value_comment;
    bool has_value = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(comment, value_comment, has_value));
    EXPECT_TRUE(has_value);
    EXPECT_EQ(legacy.get_comment(), value_comment.string());
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(value_comment.string()));
    EXPECT_FALSE(filter.has_nested_paths());
    EXPECT_EQ(0, filter.get_type_mask());
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> path;
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a"))));
    for (ObJsonNodeType type : {ObJsonNodeType::J_STRING, ObJsonNodeType::J_INT}) {
      bool indexed = false;
      ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, type, indexed));
      EXPECT_TRUE(indexed);
    }

    ObSEArray<ObString, 1> nested;
    ASSERT_EQ(OB_SUCCESS, nested.push_back(ObString("previous")));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_nested_paths(comment, nested));
    EXPECT_TRUE(nested.empty());
    bool equal = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(comment, defaults.get_comment(), equal));
    EXPECT_TRUE(equal);
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(defaults.get_comment(), comment, equal));
    EXPECT_TRUE(equal);

    // The same text inside a versioned filter must still fail validation and consumption.
    ObSearchIndexModeConfig mode;
    mode.filter_ = comment;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ObSearchIndexConfig versioned;
    ASSERT_EQ(OB_SUCCESS, versioned.init(modes));
    EXPECT_EQ(OB_INVALID_ARGUMENT, versioned.validate());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
        versioned.get_comment(), value_comment, has_value));
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init_from_comment(value_comment.string()));
  }
}

TEST_F(SearchIndexConfigTest, legacy_filter_normalization_and_runtime_behavior)
{
  const struct {
    const char *comment;
    const char *filter;
    const char *nested;
    uint8_t type_mask;
    bool indexes_a;
    bool indexes_b;
  } cases[] = {
    {"INCLUDE_TYPES=", "", "", 0, true, true},
    {"INCLUDE_TYPES=7unknown", "", "", 0, true, true},
    {"INCLUDE_TYPES=7unknown11json_stringtrailing", "INCLUDE_TYPES=11json_string", "", 1, true, true},
    {"INCLUDE_PATHS=3$.atrailing", "INCLUDE_PATHS=3$.a", "", 0, true, false},
    {"EXCLUDE_PATHS=3$.atrailing", "EXCLUDE_PATHS=3$.a", "", 0, false, true},
    {"NESTED_PATHS=3$.btrailing", "", "NESTED_PATHS=3$.b", 0, true, true},
    // Empty legacy sections are omitted, so an empty INCLUDE list imposes no restriction.
    {"INCLUDE_PATHS=NESTED_PATHS=3$.b", "", "NESTED_PATHS=3$.b", 0, true, true},
    {"EXCLUDE_PATHS=INCLUDE_TYPES=11json_string", "INCLUDE_TYPES=11json_string", "", 1, true, true},
    {"NESTED_PATHS=INCLUDE_TYPES=11json_string", "INCLUDE_TYPES=11json_string", "", 1, true, true},
    {"INCLUDE_PATHS=", "", "", 0, true, true},
    {"EXCLUDE_PATHS=", "", "", 0, true, true},
    {"NESTED_PATHS=", "", "", 0, true, true},
    {"INCLUDE_PATHS=NESTED_PATHS=INCLUDE_TYPES=", "", "", 0, true, true},
    {"INCLUDE_PATHS=3$.aNESTED_PATHS=INCLUDE_TYPES=11json_string", "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string", "", 1, true, false}
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.comment);
    const ObString comment(item.comment);
    ObSearchIndexConfigFilter::ParsedFilter parsed;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfigFilter::parse_filter(comment, parsed, true));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigFilter::parse_filter(comment, parsed, false));
    EXPECT_EQ(item.type_mask, parsed.type_mask_);

    ObSearchIndexConfig legacy;
    ASSERT_EQ(OB_SUCCESS, legacy.decode(comment));
    ASSERT_EQ(1, legacy.get_modes().count());
    EXPECT_EQ(ObString(item.filter), legacy.get_modes().at(0).filter_);
    EXPECT_EQ(ObString(item.nested), legacy.get_nested_paths_filter());
    EXPECT_EQ(OB_SUCCESS, legacy.validate());
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, legacy.encode(encoded));
    EXPECT_EQ(legacy.get_comment(), encoded.string());
    bool equal = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::is_equal(comment, encoded.string(), equal));
    EXPECT_TRUE(equal);
    ObSqlString value_comment;
    bool has_value = false;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(comment, value_comment, has_value));
    ASSERT_TRUE(has_value);
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(value_comment.string()));
    EXPECT_EQ(item.type_mask, filter.get_type_mask());
    EXPECT_EQ(item.nested[0] != '\0', filter.has_nested_paths());
    for (const char *member : {"a", "b"}) {
      ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> path;
      ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(member))));
      bool indexed = false;
      ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_STRING, indexed));
      EXPECT_EQ(member[0] == 'a' ? item.indexes_a : item.indexes_b, indexed);
    }

    // Building from legacy and normalized definitions must yield the same requirements.
    obrpc::ObCreateIndexArg arg;
    arg.index_type_ = INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL;
    obrpc::ObColumnSortItem column;
    column.column_name_ = ObString("payload");
    column.column_comment_ = comment;
    ASSERT_EQ(OB_SUCCESS, arg.index_columns_.push_back(column));
    ObSearchIndexRequirements requirements;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
    EXPECT_TRUE(requirements.has_data_);
    EXPECT_EQ(item.nested[0] != '\0', requirements.has_nested_);
    EXPECT_FALSE(requirements.has_fulltext_);
    arg.index_columns_.at(0).column_comment_ = legacy.get_comment();
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
    EXPECT_TRUE(requirements.has_data_);
    EXPECT_EQ(item.nested[0] != '\0', requirements.has_nested_);
    EXPECT_FALSE(requirements.has_fulltext_);

    ObSearchIndexModeConfig mode;
    mode.filter_ = comment;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ObSearchIndexConfig versioned;
    ASSERT_EQ(OB_SUCCESS, versioned.init(modes));
    EXPECT_EQ(OB_INVALID_ARGUMENT, versioned.validate());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_value_mode_comment(
        versioned.get_comment(), value_comment, has_value));
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init_from_comment(value_comment.string()));
    arg.index_columns_.at(0).column_comment_ = versioned.get_comment();
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexBuilderUtil::resolve_search_index_args(arg, requirements));
  }
}

TEST_F(SearchIndexConfigTest, legacy_filter_still_rejects_invalid_lengths)
{
  for (const char *raw : {"INCLUDE_PATHS=100$.a", "NESTED_PATHS=100$.a", "INCLUDE_TYPES=100unknown",
      "INCLUDE_TYPES=999999999999999999999json_string"}) {
    SCOPED_TRACE(raw);
    for (bool strict : {false, true}) {
      ObSearchIndexConfigFilter::ParsedFilter parsed;
      EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfigFilter::parse_filter(ObString(raw), parsed, strict));
    }
    ObSearchIndexConfig config;
    EXPECT_EQ(OB_INVALID_ARGUMENT, config.decode(ObString(raw)));
  }
}

TEST_F(SearchIndexConfigTest, filter_validation_rejects_malformed_comments)
{
  const char *invalid[] = {
    "INCLUDE_TYPES=", "INCLUDE_TYPES=7unknown",
    "INCLUDE_TYPES=11json_stringtrailing", "INCLUDE_PATHS=3$.atrailing"
  };
  for (const char *raw : invalid) {
    SCOPED_TRACE(raw);
    ObSearchIndexConfigFilter::ParsedFilter parsed;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfigFilter::parse_filter(ObString(raw), parsed));
    ObSearchIndexModeConfig mode;
    mode.filter_ = ObString(raw);
    ObSearchIndexConfig config;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ASSERT_EQ(OB_SUCCESS, config.init(modes));
    EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init_from_comment(config.get_comment()));
  }
  const char *malformed[] = {
    "INCLUDE_PATHS=", "EXCLUDE_PATHS=", "NESTED_PATHS=", "INCLUDE_PATHS=0",
    "NESTED_PATHS=10$.a", "INCLUDE_PATHS=999999999999999999999$.a",
    "INCLUDE_TYPES=999999999999999999999json_string", "INCLUDE_TYPES=20json_string"
  };
  for (const char *raw : malformed) {
    SCOPED_TRACE(raw);
    ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
    EXPECT_NE(OB_SUCCESS, filter.init_from_comment(ObString(raw)));
    ObSearchIndexModeConfig mode;
    mode.filter_ = ObString(raw);
    ObSearchIndexConfig config;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ASSERT_EQ(OB_SUCCESS, config.init(modes));
    EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
    EXPECT_NE(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
  }
}

TEST_F(SearchIndexConfigTest, nested_paths_borrow_legacy_and_versioned_comments)
{
  const std::string nested = "NESTED_PATHS=3$.a3$.b";
  const std::string header = "SEARCH_INDEX_CONFIG=1:1:" + std::to_string(nested.size()) + ":" + nested;
  const std::string comments[] = {
    nested, "INCLUDE_PATHS=3$.a" + nested + "INCLUDE_TYPES=11json_string",
    nested + "INCLUDE_TYPES=7unknown", nested + "INCLUDE_TYPES=100bad",
    header + "0:0:0:0:", header + "2:0:0:0:"
  };
  ObSEArray<ObString, 4> paths;
  for (const std::string &comment : comments) {
    SCOPED_TRACE(comment);
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_nested_paths(
        ObString(comment.size(), comment.data()), paths));
    ASSERT_EQ(2, paths.count());
    EXPECT_EQ(ObString("$.a"), paths.at(0));
    EXPECT_EQ(ObString("$.b"), paths.at(1));
    const size_t nested_begin = comment.find("NESTED_PATHS=");
    EXPECT_EQ(comment.data() + comment.find("$.a", nested_begin), paths.at(0).ptr());
    EXPECT_EQ(comment.data() + comment.find("$.b", nested_begin), paths.at(1).ptr());
  }
  const char *without_nested[] = {
    "", "INCLUDE_TYPES=11json_number", "SEARCH_INDEX_CONFIG=1:1:0:2:0:0:0:"
  };
  for (const char *comment : without_nested) {
    ASSERT_EQ(OB_SUCCESS, paths.push_back(ObString("previous")));
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::get_nested_paths(ObString(comment), paths));
    EXPECT_TRUE(paths.empty());
  }
  const std::string invalid[] = {
    "NESTED_PATHS=100$.a", header + "2:0:",
    header + "2:0:0:0:trailing", "SEARCH_INDEX_CONFIG=2:1:0:2:0:0:0:"
  };
  for (const std::string &comment : invalid) {
    SCOPED_TRACE(comment);
    ASSERT_EQ(OB_SUCCESS, paths.push_back(ObString("previous")));
    EXPECT_NE(OB_SUCCESS, ObSearchIndexConfig::get_nested_paths(
        ObString(comment.size(), comment.data()), paths));
  }
}

TEST_F(SearchIndexConfigTest, nested_filter_enumeration_and_matching)
{
  const ObString raw("INCLUDE_PATHS=5$.a.*NESTED_PATHS=3$.aINCLUDE_TYPES=11json_string");
  ObSEArray<ObString, 4> nested;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigFilter::parse_nested_paths(raw, nested));
  ASSERT_EQ(1, nested.count());
  EXPECT_EQ(ObString("$.a"), nested.at(0));
  ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(raw));
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
  EXPECT_TRUE(filter.has_nested_paths());
  EXPECT_TRUE(filter.get_type_mask() == ObSearchIndexConfigFilter::TYPE_MASK_STRING);
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> path;
  ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a"))));
  bool matched = false;
  ASSERT_EQ(OB_SUCCESS, filter.is_nested_path(path, matched));
  EXPECT_TRUE(matched);
  // Enumeration does not validate unrelated type sections, as in the legacy reader.
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfigFilter::parse_nested_paths(
      ObString("EXCLUDE_PATHS=3$.bNESTED_PATHS=3$.aINCLUDE_TYPES=100bad"), nested));
  ASSERT_EQ(1, nested.count());
  EXPECT_EQ(ObString("$.a"), nested.at(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexConfigFilter::parse_nested_paths(
      ObString("NESTED_PATHS=100$.a"), nested));
}

TEST_F(SearchIndexConfigTest, test_search_index_config_sql_roundtrip)
{
  const char *options[] = {"INCLUDE_PATHS", "EXCLUDE_PATHS", "NESTED_PATHS"};
  const char *literals[] = {R"('$."a''b"')", R"('$."a\\\\b"')", R"('$."é"')"};
  const char *paths[] = {R"($."a'b")", R"($."a\\b")", R"($."é")"};
  for (const char *option : options) {
    for (int64_t i = 0; i < ARRAYSIZEOF(literals); ++i) {
      SCOPED_TRACE(option);
      SCOPED_TRACE(literals[i]);
      ObSqlString ddl;
      ObSearchIndexConfig config;
      ObSqlString expected;
      ASSERT_EQ(OB_SUCCESS, ddl.append_fmt(
          "create table t(j json, search index si(j with(%s=(%s)))) organization=heap",
          option, literals[i]));
      ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(
          std::string(ddl.ptr(), ddl.length()), config));
      ASSERT_EQ(OB_SUCCESS, expected.append_fmt("%s=%ld%s", option, strlen(paths[i]), paths[i]));
      ASSERT_EQ(1, config.get_modes().count());
      ASSERT_EQ(expected.string(), 0 == strcmp(option, "NESTED_PATHS")
          ? config.get_nested_paths_filter() : config.get_modes().at(0).filter_);

      // Replay the actual printer output through the SQL parser, without repairing it.
      const ObSQLMode modes[] = {SMO_DEFAULT, SMO_NO_BACKSLASH_ESCAPES};
      for (const ObSQLMode sql_mode : modes) {
        SCOPED_TRACE(sql_mode);
        char buf[1024];
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
            config.get_comment(), buf, sizeof(buf), pos, sql_mode));
        if (i == 0) {
          ObSqlString expected_sql;
          ASSERT_EQ(OB_SUCCESS, expected_sql.append_fmt(" WITH (%s = ('$.\"a''b\"'))", option));
          ASSERT_EQ(expected_sql.string(), ObString(static_cast<int32_t>(pos), buf));
        }
        ObSqlString replay;
        ASSERT_EQ(OB_SUCCESS, replay.append_fmt("create search index si on t(j%.*s)", static_cast<int>(pos), buf));
        ObSearchIndexConfig replay_config;
        ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(
            std::string(replay.ptr(), replay.length()), replay_config, json_source_meta(),
            CS_TYPE_UTF8MB4_BIN, sql_mode));
        ASSERT_EQ(config.get_comment(), replay_config.get_comment());
        ObSearchIndexConfigFilter filter(MTL_ID());
        ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(replay_config.get_comment()));

        if (0 == strcmp(option, "INCLUDE_PATHS") && i == 1) {
          ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> items;
          ASSERT_EQ(OB_SUCCESS, items.push_back(
              ObSearchIndexPathEncoder::make_object_path(ObString(R"(a\b)"))));
          bool passed = false;
          ASSERT_EQ(OB_SUCCESS, filter.is_indexed(items, ObJsonNodeType::J_DECIMAL, passed));
          ASSERT_TRUE(passed);
        }
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, test_search_index_config_latin1)
{
  const char *options[] = {"INCLUDE_PATHS", "EXCLUDE_PATHS", "NESTED_PATHS"};
  for (const char *option : options) {
    SCOPED_TRACE(option);
    ObSqlString ddl;
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, ddl.append_fmt(
        "create search index si on t(j with(%s=('$.\x22\xe9\x22', '$.\x22\xe8\x22')))", option));
    ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(
        std::string(ddl.ptr(), ddl.length()), config, json_source_meta(), CS_TYPE_LATIN1_SWEDISH_CI));
    ObSqlString expected;
    // Both members grow from one to two bytes, and remain distinct.
    ASSERT_EQ(OB_SUCCESS, expected.append_fmt("%s=6$.\"é\"6$.\"è\"", option));
    ASSERT_EQ(1, config.get_modes().count());
    ASSERT_EQ(expected.string(), 0 == strcmp(option, "NESTED_PATHS")
        ? config.get_nested_paths_filter() : config.get_modes().at(0).filter_);
    ObSearchIndexConfigFilter filter(MTL_ID());
    ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
    if (0 != strcmp(option, "NESTED_PATHS")) {
      const char *members[] = {"é", "è", "e"};
      for (int64_t i = 0; i < ARRAYSIZEOF(members); ++i) {
        ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> items;
        ASSERT_EQ(OB_SUCCESS, items.push_back(
            ObSearchIndexPathEncoder::make_object_path(ObString(members[i]))));
        bool passed = false;
        ASSERT_EQ(OB_SUCCESS, filter.is_indexed(items, ObJsonNodeType::J_DECIMAL, passed));
        ASSERT_EQ(0 == strcmp(option, "INCLUDE_PATHS") ? i < 2 : i == 2, passed);
      }
    }
  }
}

TEST_F(SearchIndexConfigTest, mode_paths_charset_and_sql_roundtrip)
{
  // Exercise FULLTEXT and mixed VALUE/FULLTEXT mode printers.
  const char *options[] = {
    "FULLTEXT(INCLUDE_PATHS=(%s))",
    "FULLTEXT(EXCLUDE_PATHS=(%s))",
    "VALUE(INCLUDE_PATHS=(%s)), FULLTEXT(INCLUDE_PATHS=('$.text'))"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(options); ++i) {
    ObSqlString mode_options;
    // The member contains a latin1 character, an apostrophe and a backslash.
    ASSERT_EQ(OB_SUCCESS, mode_options.append_fmt(options[i], "'$.\"\xe9''a\\\\\\\\b\"'"));
    ObSqlString ddl;
    ASSERT_EQ(OB_SUCCESS, ddl.append_fmt(
        "CREATE ASYNC SEARCH INDEX si ON t(j WITH (%s))", mode_options.ptr()));
    SCOPED_TRACE(ddl.ptr());
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(
        std::string(ddl.ptr(), ddl.length()), config, json_source_meta(), CS_TYPE_LATIN1_SWEDISH_CI));
    const char *path = R"($."é'a\\b")";
    ObSqlString expected;
    ASSERT_EQ(OB_SUCCESS, expected.append_fmt("%s=%ld%s",
        i == 1 ? "EXCLUDE_PATHS" : "INCLUDE_PATHS", strlen(path), path));
    ASSERT_EQ(expected.string(), config.get_modes().at(0).filter_);
    for (const ObSQLMode sql_mode : {SMO_DEFAULT, SMO_NO_BACKSLASH_ESCAPES}) {
      char buf[4096];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(
          config.get_comment(), buf, sizeof(buf), pos, sql_mode));
      ObSearchIndexConfig replay;
      ASSERT_EQ(OB_SUCCESS, resolve_preview_sql(
          "CREATE ASYNC SEARCH INDEX si ON t(j" + std::string(buf, pos) + ")",
          replay, json_source_meta(), CS_TYPE_UTF8MB4_BIN, sql_mode));
      ASSERT_EQ(config.get_comment(), replay.get_comment());
    }
  }
}

TEST_F(SearchIndexConfigTest, test_search_index_config_invalid_charset_input)
{
  ParseNode path = {};
  path.type_ = T_VARCHAR;
  path.str_value_ = "$.\"\xff\"";
  path.str_len_ = strlen(path.str_value_);
  ParseNode *paths[] = {&path};
  ParseNode option = {};
  option.type_ = T_SEARCH_INDEX_INCLUDE_PATHS;
  option.num_child_ = 1;
  option.children_ = paths;
  ObSqlString comment;
  ASSERT_EQ(OB_ERR_INCORRECT_STRING_VALUE, ObSearchIndexConfigFilter::print_filter_option(
      &option, CS_TYPE_ASCII_GENERAL_CI, comment));
  // An incomplete multibyte tail must not be dropped to turn the input into "$.a".
  path.str_value_ = "$.a\x81";
  path.str_len_ = strlen(path.str_value_);
  ASSERT_EQ(OB_ERR_INCORRECT_STRING_VALUE, ObSearchIndexConfigFilter::print_filter_option(
      &option, CS_TYPE_GBK_CHINESE_CI, comment));
}

TEST_F(SearchIndexConfigTest, test_search_index_config_nested_validation)
{
  const char *paths[] = {"$", "$.a.*", "$.a[0]", "$."};
  const int errors[] = {OB_NOT_SUPPORTED, OB_NOT_SUPPORTED, OB_NOT_SUPPORTED, OB_INVALID_ARGUMENT};
  for (int64_t i = 0; i < ARRAYSIZEOF(paths); ++i) {
    SCOPED_TRACE(paths[i]);
    ObSqlString ddl;
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, ddl.append_fmt(
        "create search index si on t(j with(nested_paths=('%s')))", paths[i]));
    ASSERT_EQ(errors[i], resolve_preview_sql(
        std::string(ddl.ptr(), ddl.length()), config, json_source_meta(), CS_TYPE_LATIN1_SWEDISH_CI));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_search_index_config.log", true);
  OB_LOGGER.set_log_level("INFO");
  // Builtin FTS plugin state has process lifetime, just as it does in observer.
  plugin::ObPluginMgr plugins;
  int ret = OB_SUCCESS;
  int result = 1;
  if (OB_FAIL(oceanbase::common::ObCharset::init_charset())) {
    LOG_WARN("failed to initialize charsets", K(ret));
  } else if (OB_FAIL(plugins.init(ObString()))) {
    LOG_WARN("failed to initialize plugin manager", K(ret));
  } else if (OB_FAIL(plugins.load_builtin_plugins())) {
    LOG_WARN("failed to load builtin plugins", K(ret));
  } else {
    auto *saved_plugins = GCTX.plugin_mgr_;
    GCTX.plugin_mgr_ = &plugins;
    DEFER(GCTX.plugin_mgr_ = saved_plugins);
    result = RUN_ALL_TESTS();
  }
  return result;
}
