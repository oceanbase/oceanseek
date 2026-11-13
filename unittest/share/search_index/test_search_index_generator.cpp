/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "share/search_index/ob_search_index_generator.h"
#include "share/search_index/ob_search_index_config.h"
#include "unittest/share/search_index/search_index_config_test_util.h"
#include "share/ob_fts_pos_list_codec.h"
#include "storage/fts/ob_fts_tokenizer.h"
#include <initializer_list>
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include <string>
#include <vector>
#include "sql/engine/basic/ob_json_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_init.h"
#include "share/schema/ob_table_schema.h"
#include "sql/rewrite/ob_search_index_query_range_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

class SearchIndexGeneratorTest : public ::testing::Test
{
protected:
  ObArenaAllocator allocator_;
  ObSearchIndexRowGenerator generator_;
  ObSqlString comment_;
  blocksstable::ObDatumRow input_;
  blocksstable::ObDatumRow *rows_ = nullptr;
  int64_t count_ = 0;

  ObSearchIndexModeConfig fulltext(bool phrase, const char *filter = "INCLUDE_TYPES=11json_string",
                                  const char *properties = "{\"min_token_size\":3,\"max_token_size\":84}")
  {
    ObSearchIndexModeConfig mode;
    mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
    mode.filter_ = ObString(filter);
    mode.parser_name_ = ObString("space.1");
    mode.parser_properties_ = ObString(properties);
    return mode;
  }
  int init(const ObSearchIndexConfig &config, ObSearchIndexRowTarget target)
  {
    int ret = OB_SUCCESS;
    // Match the DDL path: only validated configurations are passed to the generator.
    if (OB_FAIL(config.encode(comment_))) {
      LOG_WARN("failed to build test search index configuration", K(ret));
    } else if (OB_FAIL(init(comment_.string(), target))) {
      LOG_WARN("failed to initialize test search index generator", K(ret));
    }
    return ret;
  }
  int init(const ObString &comment, ObSearchIndexRowTarget target)
  {
    int ret = OB_SUCCESS;
    ObSEArray<int32_t, 1> ids;
    ObSEArray<ObObjMeta, 1> metas;
    ObSEArray<int64_t, 2> proj;
    ObSEArray<ObString, 1> comments;
    ObObjMeta meta;
    meta.set_json();
    OZ(ids.push_back(3));
    OZ(metas.push_back(meta));
    OZ(proj.push_back(0));
    OZ(proj.push_back(1));
    OZ(comments.push_back(comment));
    OZ(generator_.init(ids, metas, proj, comments, false, nullptr, target));
    if (input_.get_column_count() == 0) { OZ(input_.init(allocator_, 2)); }
    return ret;
  }
  int generate(const char *json, uint64_t doc_id = 42)
  {
    int ret = OB_SUCCESS;
    ObIJsonBase *tree = nullptr;
    ObString binary;
    if (json == nullptr) {
      input_.storage_datums_[0].set_null();
    } else {
      OZ(ObJsonBaseFactory::get_json_base(&allocator_, ObString(json),
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, tree));
      if (OB_SUCC(ret)) { OZ(tree->get_raw_binary(binary)); }
      ObDatum with_header;
      OZ(sql::ObTextStringHelper::string_to_templob_result(ObJsonType, allocator_, binary, with_header));
      if (OB_SUCC(ret)) { input_.storage_datums_[0].shallow_copy_from_datum(with_header); }
    }
    input_.storage_datums_[1].set_uint(doc_id);
    if (OB_SUCC(ret) && OB_FAIL(generator_.generate_rows(input_, rows_, count_))) {
      LOG_WARN("failed to generate search index rows", K(ret), K(json));
    }
    return ret;
  }
  void expect_positions(const char *word, std::initializer_list<int64_t> expected)
  {
    const blocksstable::ObDatumRow *row = find(word);
    ASSERT_NE(nullptr, row);
    ASSERT_EQ(7, row->get_column_count());
    const ObString encoded = row->storage_datums_[6].get_string();
    ObFTSPositionListStore store;
    ObSEArray<int64_t, 8> positions;
    ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(encoded.ptr(), encoded.length(), positions));
    ASSERT_EQ(expected.size(), positions.count());
    int64_t i = 0;
    for (int64_t position : expected) { EXPECT_EQ(position, positions.at(i++)); }
  }
  blocksstable::ObDatumRow *find(const char *word)
  {
    int ret = OB_SUCCESS;
    blocksstable::ObDatumRow *result = nullptr;
    ObString token(word);
    ObEncParam enc;
    OZ(ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
    OZ(ObSearchIndexValueEncoder::encode_string(allocator_, ObString(word), token, enc));
    for (int64_t i = 0; OB_SUCC(ret) && nullptr == result && i < count_; ++i) {
      if (rows_[i].storage_datums_[2].get_string() == token) { result = &rows_[i]; }
    }
    return result;
  }
  void expect_string_row(const char *word, const std::string &leaf_path,
                         ObSearchIndexRowTarget target, uint64_t length = 1)
  {
    const auto *row = find(word);
    ASSERT_NE(nullptr, row) << word;
    // Golden bytes distinguish the actual array/member path from its INCLUDE_PATHS rule.
    const std::string encoded = leaf_path + (target == SEARCH_DATA ? std::string("\0\x18", 2) : "");
    EXPECT_EQ(ObString(encoded.size(), encoded.data()), row->storage_datums_[1].get_string());
    EXPECT_EQ(42, row->storage_datums_[3].get_uint());
    if (target != SEARCH_DATA) {
      EXPECT_EQ(1, row->storage_datums_[4].get_uint());
      EXPECT_EQ(length, row->storage_datums_[5].get_uint());
    }
  }

  void init_value_range(const ObString &comment, ObSearchIndexRangeCtx &range)
  {
    ObTableSchema base;
    base.set_tenant_id(OB_SYS_TENANT_ID);
    base.set_table_id(50001);
    base.set_database_id(50000);
    base.set_schema_version(1);
    base.set_table_type(USER_TABLE);
    base.set_rowkey_column_num(1);
    base.set_max_used_column_id(17);
    ASSERT_EQ(OB_SUCCESS, base.set_table_name("path_test"));
    ObColumnSchemaV2 key;
    key.set_tenant_id(OB_SYS_TENANT_ID);
    key.set_table_id(50001);
    key.set_column_id(17);
    key.set_prev_column_id(UINT64_MAX);
    key.set_next_column_id(16);
    key.set_data_type(ObIntType);
    key.set_rowkey_position(1);
    ASSERT_EQ(OB_SUCCESS, key.set_column_name("id"));
    ASSERT_EQ(OB_SUCCESS, base.add_column(key));
    ObColumnSchemaV2 col;
    col.set_tenant_id(OB_SYS_TENANT_ID);
    col.set_table_id(50001);
    col.set_column_id(16);
    col.set_prev_column_id(17);
    col.set_next_column_id(UINT64_MAX);
    col.set_data_type(ObJsonType);
    col.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    col.set_data_length(1024);
    ASSERT_EQ(OB_SUCCESS, col.set_column_name("j"));
    ASSERT_EQ(OB_SUCCESS, base.add_column(col));
    ASSERT_TRUE(base.is_valid());
    ObTableSchema index;
    ASSERT_EQ(OB_SUCCESS, index.assign(base));
    index.set_table_type(USER_INDEX);
    index.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
    ASSERT_EQ(OB_SUCCESS, index.get_column_schema(16)->set_comment(comment));
    ObColumnRefRawExpr expr;
    expr.set_ref_id(50001, 16);
    expr.set_data_type(ObJsonType);
    ColumnItem item;
    item.table_id_ = 50001;
    item.column_id_ = 16;
    item.expr_ = &expr;
    ObSEArray<const ColumnItem *, 1> columns;
    ASSERT_EQ(OB_SUCCESS, columns.push_back(&item));
    ObExecContext ctx(allocator_);
    ASSERT_EQ(OB_SUCCESS, range.init(&base, columns, &ctx, &index, allocator_));
  }
};

#ifndef OB_BUILD_PACKAGE
TEST_F(SearchIndexGeneratorTest, fulltext_keyword_token_collation)
{
  for (bool phrase : {false, true}) {
    SCOPED_TRACE(phrase);
    ObSearchIndexModeConfig mode = fulltext(phrase);
    mode.parser_name_ = ObString("analyzer.1");
    mode.parser_properties_ = ObString(
        R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword"}}})");
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
    ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    ASSERT_EQ(OB_SUCCESS, generate(R"(["foo","foo "])"));
    ASSERT_EQ(2, count_);
    EXPECT_EQ(1u, rows_[0].storage_datums_[4].get_uint());
    EXPECT_EQ(1u, rows_[1].storage_datums_[4].get_uint());
    // Different byte sequences remain separate terms; only identical tokens merge.
    const struct {
      const char *json_;
      const char *tokens_[2];
    } cases[] = {
      {R"(["foo","foo ","foo"])", {"foo", "foo "}},
      {R"(["foo ","foo","foo "])", {"foo ", "foo"}},
      {R"(["foo ","foo  ","foo "])", {"foo ", "foo  "}},
      {R"([" ","  "," "])", {" ", "  "}},
      {R"(["中文","中文 ","中文"])", {"中文", "中文 "}},
      {R"(["foo","FOO","foo"])", {"foo", "FOO"}},
      {R"(["cafe","café","cafe"])", {"cafe", "café"}},
      {R"(["foo"," foo","foo"])", {"foo", " foo"}},
      {R"(["foo\t","foo\t ","foo\t"])", {"foo\t", "foo\t "}},
    };
    for (const auto &test_case : cases) {
      SCOPED_TRACE(test_case.json_);
      ASSERT_EQ(OB_SUCCESS, generate(test_case.json_));
      ASSERT_EQ(2, count_);
      for (int64_t t = 0; t < 2; ++t) {
        const std::string expected = std::string(test_case.tokens_[t]) + std::string(2, '\0');
        int64_t row_idx = -1;
        for (int64_t i = 0; i < count_; ++i) {
          if (rows_[i].storage_datums_[2].get_string() == ObString(expected.size(), expected.data())) {
            row_idx = i;
          }
        }
        ASSERT_GE(row_idx, 0);
        EXPECT_EQ(t == 0 ? 2u : 1u, rows_[row_idx].storage_datums_[4].get_uint());
        EXPECT_EQ(3u, rows_[row_idx].storage_datums_[5].get_uint());
        if (phrase) {
          const ObString encoded = rows_[row_idx].storage_datums_[6].get_string();
          ObFTSPositionListStore store;
          ObSEArray<int64_t, 2> positions;
          ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(encoded.ptr(), encoded.length(), positions));
          ASSERT_EQ(t == 0 ? 2 : 1, positions.count());
          EXPECT_EQ(t == 0 ? 0 : 101, positions.at(0));
          if (t == 0) { EXPECT_EQ(202, positions.at(1)); }
        }
        // JSON BIN encoding preserves keyword bytes and appends the key terminator.
        ObObjMeta query_meta;
        query_meta.set_varchar();
        query_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        ObEncParam enc;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
        ObSEArray<ObString, 1> query_tokens;
        ObSEArray<int64_t, 1> token_counts;
        ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_,
            ObString(test_case.tokens_[t]), mode.parser_name_, mode.parser_properties_,
            query_meta, query_tokens, token_counts));
        ASSERT_EQ(1, query_tokens.count());
        EXPECT_EQ(ObString(test_case.tokens_[t]), query_tokens.at(0));
        ObString encoded_query_token;
        ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
            allocator_, query_tokens.at(0), encoded_query_token, enc));
        EXPECT_EQ(ObString(expected.size(), expected.data()), encoded_query_token);
        ASSERT_EQ(1, token_counts.count());
        EXPECT_EQ(1, token_counts.at(0));
        if (phrase) {
          query_tokens.reuse();
          ObSEArray<int64_t, 1> token_ids;
          ObSEArray<int64_t, 1> token_positions;
          bool has_duplicate_tokens = true;
          ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_,
              ObString(test_case.tokens_[t]), mode.parser_name_, mode.parser_properties_,
              query_meta, query_tokens, token_ids, token_positions, has_duplicate_tokens));
          ASSERT_EQ(1, query_tokens.count());
          EXPECT_EQ(ObString(test_case.tokens_[t]), query_tokens.at(0));
          ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
              allocator_, query_tokens.at(0), encoded_query_token, enc));
          EXPECT_EQ(ObString(expected.size(), expected.data()), encoded_query_token);
          ASSERT_EQ(1, token_ids.count());
          ASSERT_EQ(1, token_positions.count());
          EXPECT_EQ(0, token_ids.at(0));
          EXPECT_EQ(0, token_positions.at(0));
          EXPECT_FALSE(has_duplicate_tokens);
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, generate(R"({"a":"foo","a ":"foo"})"));
    ASSERT_EQ(2, count_); // PATH also remains binary.
    generator_.reset();
    comment_.reset();
  }
}
#endif

#ifndef OB_BUILD_PACKAGE
TEST_F(SearchIndexGeneratorTest, fulltext_encoded_token_storage_length_truncation)
{
  ObEncParam enc;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
  std::string token(SEARCH_INDEX_VALUE_LENGTH - 2, 'a');
  for (int64_t i = 0; i < 2; ++i) {
    ObString encoded;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
        allocator_, ObString(token.size(), token.data()), encoded, enc));
    const std::string expected = (token + std::string(2, '\0')).substr(0, SEARCH_INDEX_VALUE_LENGTH);
    EXPECT_EQ(ObString(expected.size(), expected.data()), encoded);
    token += 'a';
  }
}

TEST_F(SearchIndexGeneratorTest, fulltext_keyword_token_length_limit)
{
  // Keyword analyzer filters tokens above 1024 characters before key encoding.
  constexpr int64_t token_limit = 1024;
  for (bool phrase : {false, true}) {
    SCOPED_TRACE(phrase);
    ObSearchIndexModeConfig mode = fulltext(phrase);
    mode.parser_name_ = ObString("analyzer.1");
    mode.parser_properties_ = ObString(
        R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword"}}})");
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
    ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    std::string token(token_limit, 'a');
    std::string json = "\"" + token + "\"";
    ASSERT_EQ(OB_SUCCESS, generate(json.c_str()));
    ASSERT_EQ(1, count_);
    const std::string expected = token + std::string(2, '\0');
    EXPECT_EQ(ObString(expected.size(), expected.data()), rows_[0].storage_datums_[2].get_string());

    token += 'a';
    json = "\"" + token + "\"";
    ASSERT_EQ(OB_SUCCESS, generate(json.c_str()));
    EXPECT_EQ(0, count_);

    token.clear();
    for (int64_t i = 0; i < token_limit; ++i) {
      token += "\xF0\x9F\x98\x80";
    }
    json = "\"" + token + "\"";
    ASSERT_EQ(OB_SUCCESS, generate(json.c_str()));
    ASSERT_EQ(1, count_);
    const std::string multibyte_expected = token + std::string(2, '\0');
    EXPECT_EQ(ObString(multibyte_expected.size(), multibyte_expected.data()),
              rows_[0].storage_datums_[2].get_string());
    generator_.reset();
    comment_.reset();
  }
}

TEST_F(SearchIndexGeneratorTest, fulltext_rejects_truncated_tokens)
{
  // Min/max filtering precedes normalization. U+FDFA expands to 33 UTF-8 bytes,
  // so a token below 1024 characters can still exceed the encoded key limit.
  const std::string ligature("\xEF\xB7\xBA");
  const std::string expanded("صلى الله عليه وسلم");
  ASSERT_EQ(33, expanded.size());
  for (bool phrase : {false, true}) {
    SCOPED_TRACE(phrase);
    ObSearchIndexModeConfig mode = fulltext(phrase);
    mode.parser_name_ = ObString("analyzer.1");
    mode.parser_properties_ = ObString(
        R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword","filter":["icu_normalizer"]}}})");
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
    ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    for (bool with_nul : {false, true}) {
      SCOPED_TRACE(with_nul);
      std::string text;
      std::string expected;
      for (int64_t i = 0; i < 430; ++i) {
        text += ligature;
        expected += expanded;
      }
      const int64_t padding = SEARCH_INDEX_VALUE_LENGTH - 2 - expected.size() - (with_nul ? 2 : 0);
      text.append(padding, 'a');
      expected.append(padding, 'a');
      if (with_nul) {
        text += "\\u0000";
        expected += std::string("\0\1", 2);
      }
      expected += std::string(2, '\0');
      std::string json = "\"" + text + "\"";
      ASSERT_EQ(OB_SUCCESS, generate(json.c_str()));
      ASSERT_EQ(1, count_);
      ASSERT_EQ(SEARCH_INDEX_VALUE_LENGTH, expected.size());
      EXPECT_EQ(ObString(expected.size(), expected.data()), rows_[0].storage_datums_[2].get_string());

      json = "\"" + text + "a\"";
      EXPECT_EQ(OB_SIZE_OVERFLOW, generate(json.c_str()));
    }
    generator_.reset();
    comment_.reset();
  }
}
#endif

TEST_F(SearchIndexGeneratorTest, final_config_wire_layout)
{
  // Shared golden bytes for the DDL encoder and data-generation decoder.
  const ObString wire("SEARCH_INDEX_CONFIG=1:2:21:NESTED_PATHS=7$.items0:0:0:0:2:27:INCLUDE_TYPES=11json_string7:space.12:{}");
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(wire));
  ASSERT_EQ(2, config.get_modes().count());
  EXPECT_EQ(ObSearchIndexModeConfig::VALUE, config.get_modes().at(0).mode_);
  EXPECT_EQ(ObSearchIndexModeConfig::FULLTEXT_PHRASE, config.get_modes().at(1).mode_);
  EXPECT_EQ(ObString("NESTED_PATHS=7$.items"), config.get_nested_paths_filter());
  EXPECT_TRUE(config.get_modes().at(0).filter_.empty());
  EXPECT_EQ(ObString("INCLUDE_TYPES=11json_string"), config.get_modes().at(1).filter_);
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, encoded));
  EXPECT_EQ(wire, encoded.string());
}

TEST_F(SearchIndexGeneratorTest, value_and_fulltext_include_path_matrix)
{
  const char *filters[] = {"INCLUDE_PATHS=3$.a", "INCLUDE_PATHS=5$.a.*", "INCLUDE_PATHS=6$.a[*]"};
  const char *inputs[] = {"{\"a\":\"1\"}", "{\"a\":[\"1\",\"2\"]}", "{\"a\":{\"a\":\"1\",\"b\":\"2\"}}"};
  const int counts[2][3] = {{1, 2, 0}, {0, 2, 2}};
  const std::string a("\0\x1c" "a", 3);
  const std::string array = a + std::string("\0\x20", 2);
  const std::string aa = a + std::string("\0\x1c" "a", 3);
  const std::string ab = a + std::string("\0\x1c" "b", 3);
  for (int f = 0; f < 3; ++f) {
    for (bool phrase : {false, true}) {
      for (bool text_first : {false, true}) {
        SCOPED_TRACE(::testing::Message() << filters[f] << " phrase=" << phrase << " text_first=" << text_first);
        ObSearchIndexConfig config;
        ObSearchIndexModeConfig value;
        value.filter_ = ObString(filters[f]);
        const std::string text_filter = std::string(filters[f]) + "INCLUDE_TYPES=11json_string";
        const auto text = fulltext(phrase, text_filter.c_str(), "{\"min_token_size\":1,\"max_token_size\":84}");
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, text_first ? text : value));
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, text_first ? value : text));
        const auto text_target = phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH;
        for (int d = 0; d < 3; ++d) {
          SCOPED_TRACE(inputs[d]);
          for (auto target : {SEARCH_DATA, text_target}) {
            if (f == 2) {
              // Configuration is rejected before any of the three documents can be indexed.
              EXPECT_EQ(OB_NOT_SUPPORTED, init(config, target));
            } else {
              ASSERT_EQ(OB_SUCCESS, init(config, target));
              ASSERT_EQ(OB_SUCCESS, generate(inputs[d]));
              ASSERT_EQ(counts[f][d], count_);
              if (count_ > 0) {
                expect_string_row("1", d == 0 ? a : d == 1 ? array : aa, target, d == 1 ? 2 : 1);
                if (count_ == 2) { expect_string_row("2", d == 1 ? array : ab, target, d == 1 ? 2 : 1); }
              }
            }
          }
          if (f < 2) {
            // The old unversioned VALUE definition selects the same leaves.
            ObSearchIndexConfig legacy;
            ASSERT_EQ(OB_SUCCESS, legacy.decode(ObString(filters[f])));
            ASSERT_EQ(OB_SUCCESS, init(ObString(filters[f]), SEARCH_DATA));
            ASSERT_EQ(OB_SUCCESS, generate(inputs[d]));
            ASSERT_EQ(counts[f][d], count_);
            if (count_ > 0) {
              expect_string_row("1", d == 0 ? a : d == 1 ? array : aa, SEARCH_DATA);
              if (count_ == 2) { expect_string_row("2", d == 1 ? array : ab, SEARCH_DATA); }
            }
          }
        }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, exact_title_rule_keeps_array_path_and_string_tokens)
{
  for (bool phrase : {false, true}) {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    value.filter_ = ObString("INCLUDE_PATHS=7$.title");
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(phrase,
        "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string")));
    const std::string path("\0\x1c" "title\0\x20", 9);
    const char *input = "{\"title\":[\"OceanBase database\"],\"ignored_key\":\"ignored_value\"}";
    ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_DATA));
    ASSERT_EQ(OB_SUCCESS, generate(input));
    ASSERT_EQ(1, count_);
    expect_string_row("OceanBase database", path, SEARCH_DATA);
    const auto target = phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH;
    ASSERT_EQ(OB_SUCCESS, init(config, target));
    ASSERT_EQ(OB_SUCCESS, generate(input));
    ASSERT_EQ(2, count_);
    expect_string_row("oceanbase", path, target, 2);
    expect_string_row("database", path, target, 2);
    if (phrase) {
      expect_positions("oceanbase", {0});
      expect_positions("database", {1});
    }
  }
}

TEST_F(SearchIndexGeneratorTest, shared_wildcard_and_array_descendant_coverage)
{
  const std::string a("\0\x1c" "a", 3);
  const std::string array("\0\x20", 2);
  const std::string label("\0\x1c" "label", 7);
  const std::string deep("\0\x1c" "deep", 6);
  for (const char *rule : {"INCLUDE_PATHS=3$.a", "INCLUDE_PATHS=5$.a.*"}) {
    ObSearchIndexConfig config;
    const std::string filter = std::string(rule) + "INCLUDE_TYPES=11json_string";
    ObSearchIndexModeConfig value;
    value.filter_ = ObString(filter.c_str());
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true, filter.c_str())));
    for (auto target : {SEARCH_DATA, SEARCH_TEXT_PHRASE}) {
      ASSERT_EQ(OB_SUCCESS, init(config, target));
      ASSERT_EQ(OB_SUCCESS, generate("{\"a\":[{\"label\":\"alpha\"},[\"beta\"],42,true,null,{}],\"outside\":\"gamma\"}"));
      ASSERT_EQ(2, count_);
      expect_string_row("alpha", a + array + label, target);
      expect_string_row("beta", a + array + array, target);
      ASSERT_EQ(OB_SUCCESS, generate("{\"a\":{\"deep\":{\"label\":\"delta\"}}}"));
      const bool wildcard = std::string(rule).find('*') != std::string::npos;
      ASSERT_EQ(wildcard ? 1 : 0, count_);
      if (wildcard) { expect_string_row("delta", a + deep + label, target); }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, value_and_fulltext_exclude_path_matrix)
{
  const char *filters[] = {"EXCLUDE_PATHS=3$.a", "EXCLUDE_PATHS=5$.a.*"};
  const char *inputs[] = {"{\"a\":\"alpha\"}", "{\"a\":[\"alpha\",\"beta\"]}", "{\"a\":{\"a\":\"alpha\",\"b\":\"beta\"}}"};
  const int counts[2][3] = {{0, 0, 2}, {1, 0, 0}};
  const std::string a("\0\x1c" "a", 3);
  for (int f = 0; f < 2; ++f) {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    value.filter_ = ObString(filters[f]);
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    const std::string text_filter = std::string(filters[f]) + "INCLUDE_TYPES=11json_string";
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false, text_filter.c_str())));
    for (int d = 0; d < 3; ++d) {
      for (auto target : {SEARCH_DATA, SEARCH_TEXT_MATCH}) {
        SCOPED_TRACE(::testing::Message() << filters[f] << " " << inputs[d]);
        ASSERT_EQ(OB_SUCCESS, init(config, target));
        ASSERT_EQ(OB_SUCCESS, generate(inputs[d]));
        ASSERT_EQ(counts[f][d], count_);
        if (count_ > 0) { expect_string_row("alpha", d == 0 ? a : a + a, target); }
        if (count_ == 2) { expect_string_row("beta", a + std::string("\0\x1c" "b", 3), target); }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, value_and_fulltext_reject_skipped_path_members)
{
  const char *input = "{\"b\":{\"c\":{\"leaf\":\"right\"},\"x\":{\"c\":{\"leaf\":\"middle\"}}},"
                      "\"a\":{\"b\":{\"c\":{\"leaf\":\"leading\"}}}}";
  const std::string a("\0\x1c" "a", 3);
  const std::string b("\0\x1c" "b", 3);
  const std::string c("\0\x1c" "c", 3);
  const std::string x("\0\x1c" "x", 3);
  const std::string leaf("\0\x1c" "leaf", 6);
  for (const char *paths : {"10$.b.c.leaf", "7$.b.c.*"}) {
    for (bool exclude : {false, true}) {
      const std::string filter = std::string(exclude ? "EXCLUDE_PATHS=" : "INCLUDE_PATHS=") + paths;
      for (bool phrase : {false, true}) {
        ObSearchIndexConfig config;
        ObSearchIndexModeConfig value;
        value.filter_ = ObString(filter.c_str());
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
        const std::string text_filter = filter + "INCLUDE_TYPES=11json_string";
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(phrase, text_filter.c_str())));
        const auto text_target = phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH;
        for (auto target : {SEARCH_DATA, text_target}) {
          SCOPED_TRACE(::testing::Message() << filter << " target=" << target);
          ASSERT_EQ(OB_SUCCESS, init(config, target));
          ASSERT_EQ(OB_SUCCESS, generate(input));
          ASSERT_EQ(exclude ? 2 : 1, count_);
          if (exclude) {
            EXPECT_EQ(nullptr, find("right"));
            expect_string_row("middle", b + x + c + leaf, target);
            expect_string_row("leading", a + b + c + leaf, target);
          } else {
            expect_string_row("right", b + c + leaf, target);
            EXPECT_EQ(nullptr, find("middle"));
            EXPECT_EQ(nullptr, find("leading"));
          }
        }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, query_filter_rejects_skipped_path_members)
{
  const std::initializer_list<const char *> members[] = {
      {"b", "c", "leaf"}, {"a", "b", "c", "leaf"}, {"b", "x", "c", "leaf"}};
  for (bool wildcard : {false, true}) {
    for (bool exclude : {false, true}) {
      const std::string rule = std::string(exclude ? "EXCLUDE_PATHS=" : "INCLUDE_PATHS=")
          + (wildcard ? "7$.b.c.*" : "10$.b.c.leaf");
      ObSearchIndexConfig config;
      ASSERT_EQ(OB_SUCCESS, config.decode(ObString(rule.c_str())));
      ObSearchIndexConfigFilter filter(OB_SERVER_TENANT_ID);
      ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
      for (int i = 0; i < 3; ++i) {
        SCOPED_TRACE(::testing::Message() << rule << " path=" << i);
        ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path;
        for (const char *member : members[i]) {
          ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(member))));
        }
        const bool selected = exclude ? i != 0 : i == 0;
        bool passed = false;
        ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_STRING, passed));
        EXPECT_EQ(selected, passed);
        ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, T_NULL, passed, false));
        EXPECT_EQ(selected, passed);
        ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, T_NULL, passed, true));
        EXPECT_EQ(selected && (exclude || wildcard), passed);
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, nested_override_rejects_skipped_path_members)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, config.decode(ObString("INCLUDE_PATHS=3$.zNESTED_PATHS=5$.b.c")));
  ObSearchIndexConfigFilter filter(OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
  const std::initializer_list<const char *> members[] = {{"b", "c"}, {"a", "b", "c"}, {"b", "x", "c"}};
  for (int i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 5> path;
    for (const char *member : members[i]) {
      ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(member))));
    }
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_array_path()));
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("leaf"))));
    bool passed = false;
    ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, ObJsonNodeType::J_STRING, passed));
    EXPECT_EQ(i == 0, passed);
    ASSERT_EQ(OB_SUCCESS, filter.is_indexed(path, T_NULL, passed, false));
    EXPECT_EQ(i == 0, passed);
  }
}

TEST_F(SearchIndexGeneratorTest, array_configuration_is_rejected_in_each_mode)
{
  ObJsonPath path(ObString("$.a[*]"), &allocator_);
  ASSERT_EQ(OB_SUCCESS, path.parse_path());
  EXPECT_FALSE(ObSearchIndexConfigFilter::is_valid_config_path(&path));
  ObSearchIndexConfig legacy_config;
  ASSERT_EQ(OB_SUCCESS, legacy_config.decode(ObString("INCLUDE_PATHS=6$.a[*]")));
  ObSearchIndexConfigFilter legacy(OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_NOT_SUPPORTED, legacy.init_from_comment(legacy_config.get_comment()));
  for (auto mode_id : {ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_MATCH,
                       ObSearchIndexModeConfig::FULLTEXT_PHRASE}) {
    auto mode = mode_id == ObSearchIndexModeConfig::VALUE ? ObSearchIndexModeConfig()
        : fulltext(mode_id == ObSearchIndexModeConfig::FULLTEXT_PHRASE);
    mode.filter_ = mode.is_fulltext() ? ObString("INCLUDE_PATHS=6$.a[*]INCLUDE_TYPES=11json_string")
                                     : ObString("INCLUDE_PATHS=6$.a[*]");
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
    EXPECT_EQ(OB_NOT_SUPPORTED, config.validate());
    // Decoding checks the wire format; DDL validation rejects the unsupported path.
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, encoded.append_fmt("SEARCH_INDEX_CONFIG=1:1:0:%d:%d:%.*s%d:%.*s%d:%.*s",
        mode.mode_, mode.filter_.length(), mode.filter_.length(), mode.filter_.ptr(),
        mode.parser_name_.length(), mode.parser_name_.length(), mode.parser_name_.ptr(),
        mode.parser_properties_.length(), mode.parser_properties_.length(), mode.parser_properties_.ptr()));
    ASSERT_EQ(OB_SUCCESS, config.decode(encoded.string()));
    EXPECT_EQ(OB_NOT_SUPPORTED, config.validate());
  }
}

TEST_F(SearchIndexGeneratorTest, exact_and_tail_wildcard_fulltext_blocks_overlap_on_arrays)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false, "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string")));
  auto second = fulltext(true, "INCLUDE_PATHS=5$.a.*INCLUDE_TYPES=11json_string");
  second.parser_name_ = ObString("beng.1");
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, second));
  // Both blocks would tokenize $.a[*]; different parsers must not hide that overlap.
  EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
}

TEST_F(SearchIndexGeneratorTest, value_query_coverage_reads_only_value_block)
{
  for (bool text_first : {false, true}) {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    value.filter_ = ObString("INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number");
    auto text = fulltext(false, "INCLUDE_PATHS=3$.bINCLUDE_TYPES=11json_string");
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, text_first ? text : value));
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, text_first ? value : text));
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment_));
    ObSearchIndexRangeCtx range;
    init_value_range(comment_.string(), range);
    ASSERT_TRUE(range.try_activate_range_key(16));
    ASSERT_NE(nullptr, range.get_json_filter());
    EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_NUMBER), range.get_cons_encode_type());
    ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 2> path;
    bool passed = false;
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a"))));
    ASSERT_EQ(OB_SUCCESS, range.get_json_filter()->is_indexed(path, T_NULL, passed, false));
    EXPECT_TRUE(passed);
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_array_path()));
    ASSERT_EQ(OB_SUCCESS, range.get_json_filter()->is_indexed(path, ObJsonNodeType::J_INT, passed));
    EXPECT_TRUE(passed);
    path.reset();
    ASSERT_EQ(OB_SUCCESS, path.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("b"))));
    ASSERT_EQ(OB_SUCCESS, range.get_json_filter()->is_indexed(path, T_NULL, passed, false));
    EXPECT_FALSE(passed);
  }
  ObSearchIndexConfig text_only;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(text_only, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(text_only, comment_));
  ObSearchIndexRangeCtx range;
  init_value_range(comment_.string(), range);
  EXPECT_FALSE(range.try_activate_range_key(16));
  ObSearchIndexRangeCtx legacy_range;
  init_value_range(ObString("INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_number"), legacy_range);
  EXPECT_TRUE(legacy_range.try_activate_range_key(16));
  EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_NUMBER), legacy_range.get_cons_encode_type());
}

TEST_F(SearchIndexGeneratorTest, query_array_range_and_scalar_path_are_distinct)
{
  ObSearchIndexConfig config;
  ObSearchIndexModeConfig value;
  value.filter_ = ObString("INCLUDE_PATHS=3$.a");
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false, "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string")));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_DATA));
  ASSERT_EQ(OB_SUCCESS, generate("{\"a\":[\"alpha\"]}"));
  ASSERT_EQ(1, count_);
  ObSearchIndexRangeCtx range;
  init_value_range(comment_.string(), range);
  ASSERT_TRUE(range.try_activate_range_key(16));
  ObExecContext exec(allocator_);
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, &allocator_));
  ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
  exec.set_my_session(&session);
  ASSERT_EQ(OB_SUCCESS, exec.create_physical_plan_ctx());
  ObQueryRangeCtx ctx(&exec);
  ctx.search_index_range_ctx_ = &range;
  ctx.session_info_ = &session;
  ObRawExprFactory factory(allocator_);
  for (const char *path : {"$.a", "$.a[*]"}) {
    ObConstRawExpr *expr = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObRawExprUtils::build_const_string_expr(factory, ObVarcharType,
        ObString(path), CS_TYPE_UTF8MB4_BIN, expr));
    ASSERT_EQ(OB_SUCCESS, expr->formalize(&session));
    bool can_extract = false;
    ObString prefix;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
        allocator_, ctx, *expr, prefix, false, can_extract));
    if (ObString(path) == "$.a[*]") {
      EXPECT_FALSE(can_extract); // VALUE query-path parsing has not been relaxed for FULLTEXT.
    } else {
      ASSERT_TRUE(can_extract);
      EXPECT_EQ(ObString(3, "\0\x1c" "a"), prefix);
      ObString scalar_path;
      ObString array_prefix;
      ObString array_path;
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::generate_enc_path(
          allocator_, prefix, ObSearchIndexPathEncoder::TYPE_STRING, scalar_path));
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::generate_array_path(allocator_, prefix, array_prefix));
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::generate_enc_path(
          allocator_, array_prefix, ObSearchIndexPathEncoder::TYPE_STRING, array_path));
      EXPECT_NE(scalar_path, rows_[0].storage_datums_[1].get_string());
      EXPECT_EQ(array_path, rows_[0].storage_datums_[1].get_string());
      // Exact INCLUDE coverage does not promise complete descendant coverage for range predicates.
      ASSERT_EQ(OB_SUCCESS, ObSearchIndexQueryRangeUtils::json_prefix_path_encode(
          allocator_, ctx, *expr, prefix, true, can_extract));
      EXPECT_FALSE(can_extract);
    }
  }
}

TEST_F(SearchIndexGeneratorTest, omitted_fulltext_fields_use_fixed_defaults)
{
  for (bool phrase : {false, true}) {
    for (const char *parser_name : {"", "space.1"}) {
      ObSearchIndexModeConfig mode;
      mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
      mode.parser_name_ = ObString(parser_name);
      ObSearchIndexConfig config;
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
      if (mode.parser_name_.empty()) {
        EXPECT_EQ(OB_INVALID_ARGUMENT, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
        continue;
      }
      ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
      ASSERT_EQ(OB_SUCCESS, generate("{\"tags\":[\"alpha alpha\",\"beta\"],\"n\":42,\"b\":true,\"nil\":null}"));
      ASSERT_EQ(2, count_);
      ASSERT_NE(nullptr, find("alpha"));
      ASSERT_NE(nullptr, find("beta"));
      EXPECT_EQ(2, find("alpha")->storage_datums_[4].get_uint());
      EXPECT_EQ(1, find("beta")->storage_datums_[4].get_uint());
      for (int64_t i = 0; i < count_; ++i) {
        EXPECT_EQ(phrase ? 7 : 6, rows_[i].get_column_count());
        EXPECT_EQ(3, rows_[i].storage_datums_[5].get_uint());
      }
      if (phrase) {
        expect_positions("alpha", {0, 1});
        expect_positions("beta", {102});
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, partial_fulltext_properties_keep_fixed_defaults)
{
  for (bool phrase : {false, true}) {
    for (bool min_only : {false, true}) {
      for (const char *parser_name : {"", "space.1"}) {
        SCOPED_TRACE(::testing::Message() << "phrase=" << phrase << " min_only=" << min_only
            << " parser=" << parser_name);
        ObSearchIndexModeConfig mode;
        mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
        mode.parser_name_ = ObString(parser_name);
        mode.parser_properties_ = min_only ? ObString("{\"min_token_size\":1}")
                                          : ObString("{\"max_token_size\":10}");
        ObSearchIndexConfig config;
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
        if (mode.parser_name_.empty()) {
          EXPECT_EQ(OB_INVALID_ARGUMENT, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
          continue;
        }
        ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
        ASSERT_EQ(OB_SUCCESS, generate("{\"title\":\"x alpha alphabetical alpha\"}"));
        ASSERT_EQ(min_only ? 3 : 1, count_);
        ASSERT_NE(nullptr, find("alpha"));
        EXPECT_EQ(2, find("alpha")->storage_datums_[4].get_uint());
        EXPECT_EQ(min_only, nullptr != find("x"));
        EXPECT_EQ(min_only, nullptr != find("alphabetical"));
        for (int64_t i = 0; i < count_; ++i) {
          EXPECT_EQ(min_only ? 4 : 2, rows_[i].storage_datums_[5].get_uint());
        }
        if (phrase) { expect_positions("alpha", {1, 3}); }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, repeated_words_and_reuse)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  ASSERT_EQ(OB_SUCCESS, generate("{\"title\":\"alpha alpha beta\"}"));
  ASSERT_EQ(2, count_);
  auto *alpha = find("alpha");
  ASSERT_NE(nullptr, alpha);
  EXPECT_EQ(6, alpha->get_column_count());
  EXPECT_EQ(3, alpha->storage_datums_[0].get_uint());
  EXPECT_EQ(42, alpha->storage_datums_[3].get_uint());
  EXPECT_EQ(2, alpha->storage_datums_[4].get_uint());
  EXPECT_EQ(3, alpha->storage_datums_[5].get_uint());
  ASSERT_EQ(OB_SUCCESS, generate("{\"title\":\"beta\"}", 43));
  ASSERT_EQ(1, count_);
  EXPECT_EQ(1, rows_[0].storage_datums_[4].get_uint());
  EXPECT_EQ(1, rows_[0].storage_datums_[5].get_uint());
  EXPECT_EQ(43, rows_[0].storage_datums_[3].get_uint());
}

TEST_F(SearchIndexGeneratorTest, phrase_value_position_gap)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("{\"tags\":[\"alpha alpha\",\"beta\",\"alpha\"]}"));
  ASSERT_EQ(2, count_);
  const auto *alpha = find("alpha");
  ASSERT_NE(nullptr, alpha);
  EXPECT_EQ(3, alpha->storage_datums_[4].get_uint());
  EXPECT_EQ(4, alpha->storage_datums_[5].get_uint());
  expect_positions("alpha", {0, 1, 203});
  expect_positions("beta", {102});
  ASSERT_EQ(OB_SUCCESS, generate("{\"tags\":[\"alpha\",\"beta\"]}", 43));
  expect_positions("alpha", {0});
  expect_positions("beta", {101});
}

TEST_F(SearchIndexGeneratorTest, repeated_values_keep_frequency_and_position_gap)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("{\"text\":[\"helo world\",\"helo world\"]}"));
  ASSERT_EQ(2, count_);
  for (int64_t i = 0; i < count_; ++i) {
    EXPECT_EQ(2, rows_[i].storage_datums_[4].get_uint());
    EXPECT_EQ(4, rows_[i].storage_datums_[5].get_uint());
  }
  expect_positions("helo", {0, 102});
  expect_positions("world", {1, 103});
}

TEST_F(SearchIndexGeneratorTest, empty_token_streams_do_not_advance_position)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("{\"tags\":[\"\",\"x\",null,\"alpha\",\"x\",\"\",true,\"beta\"]}"));
  ASSERT_EQ(2, count_);
  expect_positions("alpha", {0});
  expect_positions("beta", {101});
  for (int64_t i = 0; i < count_; ++i) { EXPECT_EQ(2, rows_[i].storage_datums_[5].get_uint()); }
}

TEST_F(SearchIndexGeneratorTest, filtered_position_gaps_are_preserved)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("{\"tags\":[\"alpha x beta\",\"gamma\"]}"));
  ASSERT_EQ(3, count_);
  expect_positions("alpha", {0});
  expect_positions("beta", {2});
  expect_positions("gamma", {103});
  for (int64_t i = 0; i < count_; ++i) { EXPECT_EQ(3, rows_[i].storage_datums_[5].get_uint()); }
}

TEST_F(SearchIndexGeneratorTest, different_paths_have_independent_position_ranges)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("{\"left\":[\"alpha\",\"alpha\"],\"right\":[\"beta\",\"beta\"]}"));
  ASSERT_EQ(2, count_);
  expect_positions("alpha", {0, 101});
  expect_positions("beta", {0, 101});
}

TEST_F(SearchIndexGeneratorTest, empty_and_non_string_inputs)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  const char *inputs[] = {nullptr, "null", "\"\"", "[]", "{}", "{\"x\":42}", "{\"x\":[null,\"\",true]}"};
  for (auto input : inputs) {
    ASSERT_EQ(OB_SUCCESS, generate(input));
    EXPECT_EQ(0, count_);
    EXPECT_EQ(nullptr, rows_);
  }
}

TEST_F(SearchIndexGeneratorTest, target_selection_and_legacy_value)
{
  ObSearchIndexConfig config;
  ObSearchIndexModeConfig value;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_DATA));
  ASSERT_EQ(OB_SUCCESS, generate("[\"alpha alpha\",\"alpha alpha\",2]"));
  EXPECT_EQ(2, count_);
  ASSERT_GT(count_, 0); EXPECT_EQ(4, rows_[0].get_column_count());
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  ASSERT_EQ(OB_SUCCESS, generate("[\"alpha alpha\",\"alpha alpha\",2]"));
  ASSERT_EQ(1, count_);
  EXPECT_EQ(4, rows_[0].storage_datums_[4].get_uint());
  EXPECT_EQ(4, rows_[0].storage_datums_[5].get_uint());
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate("[\"alpha\"]")); EXPECT_EQ(0, count_);
  ObSearchIndexConfig legacy;
  ASSERT_EQ(OB_SUCCESS, legacy.decode(ObString()));
  ASSERT_EQ(OB_SUCCESS, init(ObString(), SEARCH_DATA));
  ASSERT_EQ(OB_SUCCESS, generate("[1,1,2]")); EXPECT_EQ(2, count_);
}

TEST_F(SearchIndexGeneratorTest, three_modes_config_decode_and_schema)
{
  for (auto mode_id : {ObSearchIndexModeConfig::VALUE, ObSearchIndexModeConfig::FULLTEXT_MATCH,
                      ObSearchIndexModeConfig::FULLTEXT_PHRASE}) {
    ObSearchIndexModeConfig mode = mode_id == ObSearchIndexModeConfig::VALUE
        ? ObSearchIndexModeConfig() : fulltext(mode_id == ObSearchIndexModeConfig::FULLTEXT_PHRASE);
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
    ObSqlString encoded;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, encoded));
    ObSearchIndexConfig decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.decode(encoded.string()));
    ASSERT_EQ(1, decoded.get_modes().count());
    const auto &result = decoded.get_modes().at(0);
    EXPECT_EQ(mode_id, result.mode_);
    EXPECT_EQ(mode.is_fulltext(), result.is_fulltext());
    EXPECT_EQ(mode.filter_, result.filter_);
    EXPECT_EQ(mode.parser_name_, result.parser_name_);
    EXPECT_EQ(mode.parser_properties_, result.parser_properties_);

    char buf[4096];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexConfig::print_schema(decoded.get_comment(), buf, sizeof(buf), pos));
    const std::string schema(buf, pos);
    if (mode_id == ObSearchIndexModeConfig::VALUE) {
      EXPECT_TRUE(schema.empty());
      EXPECT_EQ(std::string::npos, schema.find("FTS_INDEX_TYPE"));
    } else {
      EXPECT_NE(std::string::npos, schema.find(mode_id == ObSearchIndexModeConfig::FULLTEXT_MATCH
          ? "FTS_INDEX_TYPE = MATCH" : "FTS_INDEX_TYPE = PHRASE_MATCH"));
    }
  }
}

TEST_F(SearchIndexGeneratorTest, invalid_mode_is_rejected_by_config_decoder)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ObSqlString encoded;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, encoded));
  std::string invalid_config(encoded.ptr(), encoded.length());
  const size_t mode_offset = strlen("SEARCH_INDEX_CONFIG=1:1:0:");
  ASSERT_EQ('1', invalid_config.at(mode_offset));
  invalid_config[mode_offset] = '3';
  EXPECT_EQ(OB_INVALID_ARGUMENT, config.decode(ObString(invalid_config.size(), invalid_config.data())));
  EXPECT_TRUE(config.get_modes().empty());
}

TEST_F(SearchIndexGeneratorTest, phrase_position_cap_keeps_full_statistics_and_range)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  std::string doc = "[\"";
  for (int i = 0; i < 600; ++i) { doc += "alpha "; }
  doc += "\",\"beta\"]";
  ASSERT_EQ(OB_SUCCESS, generate(doc.c_str()));
  ASSERT_EQ(2, count_);
  const auto *alpha = find("alpha");
  ASSERT_NE(nullptr, alpha);
  EXPECT_EQ(600, alpha->storage_datums_[4].get_uint());
  EXPECT_EQ(601, alpha->storage_datums_[5].get_uint());
  ObFTSPositionListStore store;
  ObSEArray<int64_t, 8> positions;
  const ObString encoded = alpha->storage_datums_[6].get_string();
  ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(encoded.ptr(), encoded.length(), positions));
  ASSERT_EQ(512, positions.count());
  for (int64_t i = 0; i < positions.count(); ++i) { EXPECT_EQ(i, positions.at(i)); }
  expect_positions("beta", {700});
  ASSERT_EQ(OB_SUCCESS, generate("\"alpha\""));
  expect_positions("alpha", {0});
}

TEST_F(SearchIndexGeneratorTest, paths_modes_and_parser_properties)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false,
      "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string")));
  auto phrase = fulltext(true,
      "INCLUDE_PATHS=6$.bodyINCLUDE_TYPES=11json_string",
      "{\"min_token_size\":5,\"max_token_size\":84}");
  phrase.parser_name_ = ObString("beng.1");
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, phrase));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  const char *doc = "{\"title\":\"alpha alpha\",\"body\":\"beta gamma\",\"other\":\"delta\"}";
  ASSERT_EQ(OB_SUCCESS, generate(doc));
  ASSERT_EQ(1, count_); ASSERT_NE(nullptr, find("alpha"));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  ASSERT_EQ(OB_SUCCESS, generate(doc));
  ASSERT_EQ(1, count_); ASSERT_NE(nullptr, find("gamma"));
  EXPECT_EQ(1, rows_[0].storage_datums_[5].get_uint());
  ASSERT_EQ(OB_SUCCESS, generate("{\"other\":\"alpha\"}")); EXPECT_EQ(0, count_);
}

TEST_F(SearchIndexGeneratorTest, isolated_paths_and_nested_arrays)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  ASSERT_EQ(OB_SUCCESS, generate("{\"a\":\"alpha alpha\",\"b\":\"alpha\",\"c\":[\"alpha\",[\"alpha\"]]}"));
  ASSERT_EQ(4, count_);
  for (int64_t i = 0; i < count_; ++i) {
    EXPECT_FALSE(rows_[i].storage_datums_[1].is_null());
    for (int64_t j = 0; j < i; ++j) {
      EXPECT_NE(rows_[i].storage_datums_[1].get_string(), rows_[j].storage_datums_[1].get_string());
    }
  }
  ASSERT_EQ(OB_SUCCESS, generate("\"alpha\""));
  ASSERT_EQ(1, count_);
  EXPECT_FALSE(rows_[0].storage_datums_[1].is_null());
  EXPECT_EQ(0, rows_[0].storage_datums_[1].get_string().length());
}

TEST_F(SearchIndexGeneratorTest, multiple_value_blocks_are_rejected)
{
  for (const char *second_path : {"$.a", "$.b"}) {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    value.filter_ = ObString("INCLUDE_PATHS=3$.a");
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    std::string filter = std::string("INCLUDE_PATHS=3") + second_path;
    value.filter_ = ObString(filter.size(), filter.data());
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
    EXPECT_EQ(OB_INVALID_ARGUMENT, init(config, SEARCH_DATA));
  }
  ObSearchIndexConfig decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(
      ObString("SEARCH_INDEX_CONFIG=1:2:0:0:0:0:0:0:0:0:0:")));
  EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.validate());
}

TEST_F(SearchIndexGeneratorTest, parser_name_is_unique_within_one_source_column)
{
  for (const char *parser : {"space.1", "SPACE.2"}) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false,
        "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string")));
    auto second = fulltext(true, "INCLUDE_PATHS=3$.bINCLUDE_TYPES=11json_string",
        "{\"min_token_size\":5,\"max_token_size\":84}");
    second.parser_name_ = ObString(parser);
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, second));
    EXPECT_EQ(OB_INVALID_ARGUMENT, init(config, SEARCH_TEXT_MATCH));
  }
}

TEST_F(SearchIndexGeneratorTest, different_parsers_cannot_cover_the_same_fulltext_path)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false,
      "INCLUDE_PATHS=5$.a.*INCLUDE_TYPES=11json_string")));
  auto second = fulltext(true, "INCLUDE_PATHS=5$.a.bINCLUDE_TYPES=11json_string");
  second.parser_name_ = ObString("beng.1");
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, second));
  EXPECT_EQ(OB_INVALID_ARGUMENT, init(config, SEARCH_TEXT_PHRASE));
}

TEST_F(SearchIndexGeneratorTest, fulltext_exclusions_overlap_on_root_strings)
{
  for (bool phrase : {false, true}) {
    for (const char *excluded : {"EXCLUDE_PATHS=1$INCLUDE_TYPES=11json_string",
                                 "EXCLUDE_PATHS=3$.*INCLUDE_TYPES=11json_string"}) {
      SCOPED_TRACE(::testing::Message() << excluded << " phrase=" << phrase);
      ObSearchIndexConfig single;
      const auto first = fulltext(phrase, excluded);
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(single, first));
      ASSERT_EQ(OB_SUCCESS, init(single, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
      ASSERT_EQ(OB_SUCCESS, generate("\"alpha\""));
      ASSERT_EQ(1, count_);
      ASSERT_NE(nullptr, find("alpha"));
      ASSERT_EQ(OB_SUCCESS, generate("{\"a\":\"alpha\"}"));
      EXPECT_EQ(0, count_);

      for (const char *other : {"INCLUDE_TYPES=11json_string",
                                "EXCLUDE_PATHS=1$INCLUDE_TYPES=11json_string",
                                "EXCLUDE_PATHS=3$.*INCLUDE_TYPES=11json_string"}) {
        auto second = fulltext(phrase, other);
        second.parser_name_ = ObString("beng.1");
        for (bool reversed : {false, true}) {
          SCOPED_TRACE(::testing::Message() << other << " reversed=" << reversed);
          ObSearchIndexConfig config;
          ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, reversed ? second : first));
          ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, reversed ? first : second));
          EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
          ObSqlString comment;
          ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
          ObSearchIndexConfig decoded;
          ASSERT_EQ(OB_SUCCESS, decoded.decode(comment.string()));
          EXPECT_EQ(OB_INVALID_ARGUMENT, decoded.validate());
        }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, fulltext_root_exclusions_and_explicit_includes_are_disjoint)
{
  for (const char *excluded : {"EXCLUDE_PATHS=1$INCLUDE_TYPES=11json_string",
                               "EXCLUDE_PATHS=3$.*INCLUDE_TYPES=11json_string"}) {
    for (const char *included : {"INCLUDE_PATHS=1$INCLUDE_TYPES=11json_string",
                                 "INCLUDE_PATHS=3$.*INCLUDE_TYPES=11json_string",
                                 "INCLUDE_PATHS=3$.aINCLUDE_TYPES=11json_string"}) {
      ObSearchIndexConfig included_config;
      ASSERT_EQ(OB_SUCCESS, included_config.decode(ObString(included)));
      ObSearchIndexConfigFilter filter(OB_SERVER_TENANT_ID);
      ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(included_config.get_comment()));
      ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 1> root_path;
      bool passed = true;
      ASSERT_EQ(OB_SUCCESS, filter.is_indexed(root_path, ObJsonNodeType::J_STRING, passed));
      EXPECT_FALSE(passed);
      const auto first = fulltext(false, excluded);
      auto second = fulltext(true, included);
      second.parser_name_ = ObString("beng.1");
      for (bool reversed : {false, true}) {
        SCOPED_TRACE(::testing::Message() << excluded << " / " << included << " reversed=" << reversed);
        ObSearchIndexConfig config;
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, reversed ? second : first));
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, reversed ? first : second));
        EXPECT_EQ(OB_SUCCESS, config.validate());
        ObSqlString comment;
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
        ObSearchIndexConfig decoded;
        EXPECT_EQ(OB_SUCCESS, decoded.decode(comment.string()));
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, same_parser_on_different_source_columns_and_target_mapping)
{
  ObSqlString encoded[2];
  ObSEArray<int32_t, 2> ids;
  ObSEArray<ObObjMeta, 2> metas;
  ObSEArray<int64_t, 3> projector;
  ObSEArray<ObString, 2> comments;
  ObObjMeta meta;
  meta.set_json();
  blocksstable::ObDatumRow input;
  ASSERT_EQ(OB_SUCCESS, input.init(allocator_, 3));
  for (int64_t i = 0; i < 2; ++i) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(i == 1)));
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, encoded[i]));
    ASSERT_EQ(OB_SUCCESS, ids.push_back(i == 0 ? 3 : 7));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(i));
    ASSERT_EQ(OB_SUCCESS, comments.push_back(encoded[i].string()));
    ASSERT_EQ(OB_SUCCESS, init(config, i == 0 ? SEARCH_TEXT_MATCH : SEARCH_TEXT_PHRASE));
    ASSERT_EQ(OB_SUCCESS, generate(i == 0 ? "\"alpha alpha\"" : "\"beta beta beta\""));
    input.storage_datums_[i] = input_.storage_datums_[0];
  }
  ASSERT_EQ(OB_SUCCESS, projector.push_back(2));
  input.storage_datums_[2].set_uint(42);
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, generator_.init(ids, metas, projector, comments, false, nullptr,
        i == 0 ? SEARCH_TEXT_MATCH : SEARCH_TEXT_PHRASE));
    ASSERT_EQ(OB_SUCCESS, generator_.generate_rows(input, rows_, count_));
    ASSERT_EQ(1, count_);
    EXPECT_EQ(i == 0 ? 3 : 7, rows_[0].storage_datums_[0].get_uint());
    EXPECT_EQ(i + 2, rows_[0].storage_datums_[4].get_uint());
  }
}

TEST_F(SearchIndexGeneratorTest, invalid_input_and_reinitialize)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, generator_.generate_rows(
      static_cast<const ObChunkDatumStore::StoredRow *>(nullptr), rows_, count_));
  EXPECT_EQ(nullptr, rows_); EXPECT_EQ(0, count_);
  generator_.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, generator_.generate_rows(input_, rows_, count_));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  ASSERT_EQ(OB_SUCCESS, generate("\"alpha\"")); EXPECT_EQ(1, count_);
}

TEST_F(SearchIndexGeneratorTest, truncates_oversized_value_encoding)
{
  ObEncParam enc;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::init_json_string_enc_param(enc));
  std::string text(SEARCH_INDEX_VALUE_LENGTH * 2, 'a');
  ObString output;
  EXPECT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_string(
      allocator_, ObString(text.size(), text.data()), output, enc));
  EXPECT_EQ(SEARCH_INDEX_VALUE_LENGTH, output.length());
}

TEST_F(SearchIndexGeneratorTest, fulltext_tokens_use_json_value_encoding)
{
  for (bool phrase : {false, true}) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(phrase)));
    ASSERT_EQ(OB_SUCCESS, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    ASSERT_EQ(OB_SUCCESS, generate("\"alpha 数据库 café\""));
    ASSERT_EQ(3, count_);
    for (const char *token : {"alpha", "数据库", "café"}) {
      const auto *row = find(token);
      ASSERT_NE(nullptr, row);
      const std::string expected = std::string(token) + std::string(2, '\0');
      EXPECT_EQ(ObString(expected.size(), expected.data()), row->storage_datums_[2].get_string());
    }
  }
}

TEST_F(SearchIndexGeneratorTest, position_cap_applies_across_values)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(true)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_PHRASE));
  std::string doc = "[";
  for (int i = 0; i < 1024; ++i) { if (i != 0) { doc += ','; } doc += "\"alpha\""; }
  doc += ",\"beta\"]";
  ASSERT_EQ(OB_SUCCESS, generate(doc.c_str()));
  ASSERT_EQ(2, count_);
  const auto *alpha = find("alpha");
  ASSERT_NE(nullptr, alpha);
  EXPECT_EQ(1024, alpha->storage_datums_[4].get_uint());
  EXPECT_EQ(1025, alpha->storage_datums_[5].get_uint());
  ObFTSPositionListStore store;
  ObSEArray<int64_t, 8> positions;
  const ObString encoded = alpha->storage_datums_[6].get_string();
  EXPECT_LE(encoded.length(), 16 * 1024);
  ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(encoded.ptr(), encoded.length(), positions));
  ASSERT_EQ(512, positions.count());
  for (int64_t i = 0; i < positions.count(); ++i) { EXPECT_EQ(i * 101, positions.at(i)); }
  expect_positions("beta", {1024 * 101});
}

TEST_F(SearchIndexGeneratorTest, legacy_nested_value_identity)
{
  ASSERT_EQ(OB_SUCCESS, init(ObString("NESTED_PATHS=7$.items"), SEARCH_DATA));
  ASSERT_EQ(OB_SUCCESS, generate("{\"items\":[{\"x\":1},{\"x\":1}]}"));
  ASSERT_EQ(2, count_);
  EXPECT_EQ(5, rows_[0].get_column_count());
  EXPECT_NE(rows_[0].storage_datums_[4].get_string(), rows_[1].storage_datums_[4].get_string());
}

TEST_F(SearchIndexGeneratorTest, separate_nested_filter_preserves_format_checks)
{
  ObSearchIndexConfig config;
  ObSearchIndexModeConfig mode;
  mode.filter_ = "INCLUDE_TYPES=11json_string";
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, mode));
  ObSearchIndexConfigFilter filter(OB_SYS_TENANT_ID);
  for (const char *nested : {"NESTED_PATHS=", "NESTED_PATHS=10$.a",
                            "NESTED_PATHS=999999999999999999999$.a"}) {
    SCOPED_TRACE(nested);
    ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString(nested)));
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init_from_comment(config.get_comment()));
  }
  ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
  EXPECT_TRUE(filter.has_nested_paths());
  EXPECT_EQ(static_cast<uint8_t>(ObSearchIndexConfigFilter::TYPE_MASK_STRING), filter.get_type_mask());
  ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString()));
  ASSERT_EQ(OB_SUCCESS, filter.init_from_comment(config.get_comment()));
  EXPECT_FALSE(filter.has_nested_paths());
}

TEST_F(SearchIndexGeneratorTest, legacy_and_versioned_value_generate_identical_rows)
{
  const char *paths[] = {"", "INCLUDE_PATHS=13$.items.price", "EXCLUDE_PATHS=7$.title"};
  const char *types[] = {"", "INCLUDE_TYPES=11json_string", "INCLUDE_TYPES=11json_number"};
  const char *nested[] = {"", "NESTED_PATHS=7$.items", "NESTED_PATHS=7$.items7$.other"};
  const char *documents[] = {
    "\"alpha\"", "42", "null",
    "{\"title\":\"alpha\",\"items\":[{\"price\":1,\"name\":\"alpha\"},{\"price\":1,\"name\":\"beta\"}],"
        "\"other\":[{\"x\":1},{\"x\":2}],\"flag\":true,\"n\":null}",
    "{\"items\":[{\"price\":[1,2]},{\"price\":2}],\"other\":[[1,2],[2,3]]}",
    "{\"items\":[],\"title\":null}"
  };
  for (const char *path_filter : paths) {
    for (const char *type_filter : types) {
      for (const char *nested_filter : nested) {
        const std::string old_comment = std::string(path_filter) + nested_filter + type_filter;
        const std::string mode_filter = std::string(path_filter) + type_filter;
        for (bool with_fulltext : {false, true}) {
          if (with_fulltext && nested_filter[0] != '\0') {
            continue;
          }
          ObSearchIndexConfig config;
          ObSearchIndexModeConfig value;
          value.filter_ = ObString(mode_filter.size(), mode_filter.data());
          ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
          ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString(nested_filter)));
          if (with_fulltext) {
            ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
          }
          for (const char *document : documents) {
            SCOPED_TRACE(::testing::Message() << old_comment << " fulltext=" << with_fulltext
                << " document=" << document);
            std::vector<std::vector<std::string>> expected;
            for (bool legacy : {true, false}) {
              ASSERT_EQ(OB_SUCCESS, legacy
                  ? init(ObString(old_comment.size(), old_comment.data()), SEARCH_DATA)
                  : init(config, SEARCH_DATA));
              EXPECT_EQ(nested_filter[0] != '\0', generator_.has_nested_key());
              ASSERT_EQ(OB_SUCCESS, generate(document));
              std::vector<std::vector<std::string>> actual;
              for (int64_t i = 0; i < count_; ++i) {
                std::vector<std::string> cells;
                for (int64_t j = 0; j < rows_[i].get_column_count(); ++j) {
                  const auto &datum = rows_[i].storage_datums_[j];
                  if (datum.is_null()) {
                    cells.push_back("NULL");
                  } else if (j == 0 || j == 3) {
                    cells.push_back("UINT:" + std::to_string(datum.get_uint()));
                  } else {
                    const ObString bytes = datum.get_string();
                    cells.push_back("BYTES:" + (bytes.empty() ? std::string()
                        : std::string(bytes.ptr(), bytes.length())));
                  }
                }
                actual.push_back(cells);
              }
              if (legacy) {
                expected = actual;
              } else {
                EXPECT_EQ(expected, actual);
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, nested_value_and_fulltext_on_same_column_are_rejected)
{
  for (bool phrase : {false, true}) {
    for (bool value_first : {false, true}) {
      ObSearchIndexConfig config;
      ObSearchIndexModeConfig value;
      auto text = fulltext(phrase);
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value_first ? value : text));
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value_first ? text : value));
      ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
      EXPECT_EQ(OB_NOT_SUPPORTED, init(config, SEARCH_DATA));
      EXPECT_EQ(OB_NOT_SUPPORTED, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    }
  }
}

TEST_F(SearchIndexGeneratorTest, nested_value_and_fulltext_on_different_columns)
{
  for (bool phrase : {false, true}) {
    ObSearchIndexConfig text_config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(text_config, fulltext(phrase)));
    ASSERT_EQ(OB_SUCCESS, init(text_config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    ASSERT_EQ(OB_SUCCESS, generate("{\"items\":[\"alpha\",\"alpha\"]}"));
    blocksstable::ObDatumRow input;
    ASSERT_EQ(OB_SUCCESS, input.init(allocator_, 3));
    input.storage_datums_[0] = input_.storage_datums_[0];
    input.storage_datums_[1] = input_.storage_datums_[0];
    input.storage_datums_[2].set_uint(42);
    ObSEArray<int32_t, 2> ids;
    ObSEArray<ObObjMeta, 2> metas;
    ObSEArray<int64_t, 3> projector;
    ObSEArray<ObString, 2> comments;
    ObObjMeta meta;
    meta.set_json();
    ASSERT_EQ(OB_SUCCESS, ids.push_back(3));
    ASSERT_EQ(OB_SUCCESS, ids.push_back(7));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(2));
    ASSERT_EQ(OB_SUCCESS, comments.push_back(ObString("NESTED_PATHS=7$.items")));
    ASSERT_EQ(OB_SUCCESS, comments.push_back(comment_.string()));
    for (auto target : {SEARCH_DATA, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH}) {
      ASSERT_EQ(OB_SUCCESS, generator_.init(ids, metas, projector, comments, false, nullptr, target));
      EXPECT_EQ(target == SEARCH_DATA, generator_.has_nested_key());
      ASSERT_EQ(OB_SUCCESS, generator_.generate_rows(input, rows_, count_));
      ASSERT_EQ(target == SEARCH_DATA ? 2 : 1, count_);
      for (int64_t i = 0; i < count_; ++i) {
        EXPECT_EQ(target == SEARCH_DATA ? 3 : 7, rows_[i].storage_datums_[0].get_uint());
        EXPECT_EQ(target == SEARCH_DATA ? 5 : phrase ? 7 : 6, rows_[i].get_column_count());
      }
      if (target != SEARCH_DATA) {
        EXPECT_EQ(2, rows_[0].storage_datums_[4].get_uint());
        EXPECT_EQ(2, rows_[0].storage_datums_[5].get_uint());
        if (phrase) { expect_positions("alpha", {0, 101}); }
      }
    }
  }
}

TEST_F(SearchIndexGeneratorTest, column_nested_filter_preserves_value_options)
{
  ObSearchIndexConfig config;
  ObSearchIndexModeConfig value;
  value.filter_ = "INCLUDE_PATHS=7$.titleINCLUDE_TYPES=11json_string";
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value));
  ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
  ObSqlString comment;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::build_search_index_config_comment(config, comment));
  ObSearchIndexConfig decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.decode(comment.string()));
  EXPECT_EQ(value.filter_, decoded.get_modes().at(0).filter_);
  EXPECT_EQ(config.get_nested_paths_filter(), decoded.get_nested_paths_filter());
  ASSERT_EQ(OB_SUCCESS, init(comment.string(), SEARCH_DATA));
  EXPECT_TRUE(generator_.has_nested_key());
  ASSERT_EQ(OB_SUCCESS, generate("{\"items\":[\"alpha\",\"beta\"]}"));
  ASSERT_EQ(2, count_);
  EXPECT_EQ(5, rows_[0].get_column_count());
}

TEST_F(SearchIndexGeneratorTest, column_nested_fulltext_config_cannot_be_encoded)
{
  for (bool phrase : {false, true}) {
    for (bool has_value : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "phrase=" << phrase << " value=" << has_value);
      ObSearchIndexConfig config;
      if (has_value) {
        ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, ObSearchIndexModeConfig()));
      }
      ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(phrase)));
      ASSERT_EQ(OB_SUCCESS, config.init(config.get_modes(), ObString("NESTED_PATHS=7$.items")));
      ObSqlString encoded;
      EXPECT_EQ(OB_NOT_SUPPORTED, config.encode(encoded));
    }
  }
}

TEST_F(SearchIndexGeneratorTest, nested_paths_in_fulltext_block_are_rejected)
{
  for (bool phrase : {false, true}) {
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(phrase,
        "NESTED_PATHS=7$.itemsINCLUDE_TYPES=11json_string")));
    EXPECT_EQ(OB_NOT_SUPPORTED, config.validate());
    EXPECT_EQ(OB_NOT_SUPPORTED, init(config, SEARCH_DATA));
    EXPECT_EQ(OB_NOT_SUPPORTED, init(config, phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
  }
}

TEST_F(SearchIndexGeneratorTest, config_version_and_overlap_validation)
{
  ObSearchIndexConfig config;
  EXPECT_EQ(OB_NOT_SUPPORTED, config.decode(ObString("SEARCH_INDEX_CONFIG=2:1:0:0:0:0:0:")));
  EXPECT_EQ(OB_NOT_SUPPORTED, config.decode(ObString("SEARCH_INDEX_CONFIG=3:1:")));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, config.validate());
  ObSearchIndexConfigFilter legacy(OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_INVALID_ARGUMENT, legacy.init_from_comment(ObString("SEARCH_INDEX_CONFIG=1:1:0:")));
}

TEST_F(SearchIndexGeneratorTest, text_path_golden_bytes_and_length_limit)
{
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> items;
  ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("tags"))));
  ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_array_path()));
  ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_array_path()));
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::encode_text_path(allocator_, items, encoded));
  const char expected[] = {0, 0x1c, 't', 'a', 'g', 's', 0, 0x20, 0, 0x20};
  EXPECT_EQ(ObString(sizeof(expected), expected), encoded);
  items.reset();
  const char zero[] = {'a', 0, 'b'};
  ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(3, zero))));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexPathEncoder::encode_text_path(allocator_, items, encoded));
  const char escaped[] = {0, 0x1c, 'a', 0, static_cast<char>(0xf0), 'b'};
  EXPECT_EQ(ObString(sizeof(escaped), escaped), encoded);
  items.reset();
  std::string long_key(SEARCH_INDEX_PATH_LENGTH, 'a');
  ASSERT_EQ(OB_SUCCESS, items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString(long_key.size(), long_key.data()))));
  EXPECT_EQ(OB_ERR_TOO_LONG_KEY_LENGTH, ObSearchIndexPathEncoder::encode_text_path(allocator_, items, encoded));
}

TEST_F(SearchIndexGeneratorTest, stored_row_entrypoint)
{
  ObSearchIndexConfig config;
  ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(false)));
  ASSERT_EQ(OB_SUCCESS, init(config, SEARCH_TEXT_MATCH));
  ASSERT_EQ(OB_SUCCESS, generate("\"alpha alpha beta\""));
  const int64_t size = sizeof(sql::ObChunkDatumStore::StoredRow) + 2 * sizeof(ObDatum);
  void *buf = allocator_.alloc(size);
  ASSERT_NE(nullptr, buf);
  auto *stored = new (buf) sql::ObChunkDatumStore::StoredRow();
  stored->cnt_ = 2;
  stored->row_size_ = size;
  for (int i = 0; i < 2; ++i) { new (&stored->cells()[i]) ObDatum(input_.storage_datums_[i]); }
  ASSERT_EQ(OB_SUCCESS, generator_.generate_rows(stored, rows_, count_));
  ASSERT_EQ(2, count_);
  ASSERT_NE(nullptr, find("alpha"));
  EXPECT_EQ(2, find("alpha")->storage_datums_[4].get_uint());
}

TEST_F(SearchIndexGeneratorTest, ordinary_schema_columns_with_non_contiguous_ids)
{
  for (int n : {6, 7}) {
    ObTableSchema schema;
    schema.set_tenant_id(OB_SYS_TENANT_ID);
    schema.set_table_id(50001);
    schema.set_table_type(USER_INDEX);
    const ObIndexType type = n == 6 ? INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL
                                   : INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL;
    schema.set_index_type(type);
    schema.set_rowkey_column_num(4);
    for (int64_t i = 0; i < n; ++i) {
      ObColumnSchemaV2 col;
      col.set_tenant_id(OB_SYS_TENANT_ID);
      col.set_table_id(50001);
      col.set_column_id(100 + 3 * i);
      col.set_prev_column_id(i == 0 ? UINT64_MAX : 100 + 3 * (i - 1));
      col.set_next_column_id(i == n - 1 ? UINT64_MAX : 100 + 3 * (i + 1));
      std::string name = "role_" + std::to_string(i);
      ASSERT_EQ(OB_SUCCESS, col.set_column_name(name.c_str()));
      col.set_data_type((i == 1 || i == 2 || i == 6) ? ObVarcharType : ObUInt64Type);
      col.set_collation_type(CS_TYPE_BINARY);
      col.set_data_length(16384);
      col.set_rowkey_position(i < 4 ? i + 1 : 0);
      ASSERT_EQ(OB_SUCCESS, schema.add_column(col));
    }
    ObSearchIndexRowTarget target;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexRowGenerator::get_target(schema, target));
    EXPECT_EQ(n == 6 ? SEARCH_TEXT_MATCH : SEARCH_TEXT_PHRASE, target);
    const int64_t size = schema.get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, schema.serialize(buf, size, pos));
    ObTableSchema restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(buf, size, pos));
    EXPECT_EQ(type, restored.get_index_type());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexRowGenerator::get_target(restored, target));
    EXPECT_EQ(n == 6 ? SEARCH_TEXT_MATCH : SEARCH_TEXT_PHRASE, target);
  }
}

// Drive the real INDEX_DATA_GEN operator with a row source. No DDL/mock-success assertion.
class SearchIndexInputOp : public sql::ObOperator
{
public:
  SearchIndexInputOp(sql::ObExecContext &ctx, const sql::ObOpSpec &spec, const ObDatum &json)
    : ObOperator(ctx, spec, nullptr), json_(json), next_(0) {}
  void destroy() override { this->~SearchIndexInputOp(); }
  int inner_open() override { return OB_SUCCESS; }
  int inner_close() override { return OB_SUCCESS; }
  int inner_rescan() override { next_ = 0; return ObOperator::inner_rescan(); }
  int inner_get_next_row() override
  {
    int ret = OB_SUCCESS;
    if (next_ == 3) { ret = OB_ITER_END; }
    else {
      auto *value = spec_.output_.at(0);
      auto *doc = spec_.output_.at(1);
      value->locate_datum_for_write(eval_ctx_) = json_;
      if (next_ == 1) { value->locate_datum_for_write(eval_ctx_).set_null(); }
      doc->locate_datum_for_write(eval_ctx_).set_uint(42 + next_);
      value->set_evaluated_projected(eval_ctx_);
      doc->set_evaluated_projected(eval_ctx_);
      ++next_;
    }
    return ret;
  }
private:
  ObDatum json_;
  int64_t next_;
};

TEST_F(SearchIndexGeneratorTest, index_data_gen_operator_and_rescan)
{
  for (ObSearchIndexRowTarget target : {SEARCH_DATA, SEARCH_TEXT_MATCH, SEARCH_TEXT_PHRASE}) {
    ObSearchIndexConfig config;
    ObSearchIndexModeConfig value;
    if (target == SEARCH_DATA) { ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, value)); }
    ASSERT_EQ(OB_SUCCESS, oceanbase::unittest::append_search_index_mode(config, fulltext(target == SEARCH_TEXT_PHRASE)));
    ASSERT_EQ(OB_SUCCESS, init(config, target));
    ASSERT_EQ(OB_SUCCESS, generate("{\"title\":\"alpha alpha beta\"}"));
    const int64_t output_count = target == SEARCH_DATA ? 4 : target == SEARCH_TEXT_MATCH ? 6 : 7;
    sql::ObExecContext ctx(allocator_);
    sql::ObSQLSessionInfo session;
    ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, &allocator_));
    ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, session.init_tenant(ObString("test"), OB_SYS_TENANT_ID));
    ctx.set_my_session(&session);
    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(2));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    sql::ObPhysicalPlan plan;
    sql::ObJsonTableSpec spec(allocator_, PHY_JSON_TABLE);
    sql::ObOpSpec child_spec(allocator_, PHY_TABLE_SCAN);
    spec.id_ = 1; child_spec.id_ = 0;
    spec.plan_ = &plan; child_spec.plan_ = &plan;
    spec.table_type_ = sql::OB_INDEX_DATA_GEN_TABLE_TYPE;
    spec.inc_pk_proj_ = 1;
    spec.index_column_cnt_ = 2;
    spec.search_index_target_ = target;
    ASSERT_EQ(OB_SUCCESS, spec.search_idx_included_cid_idxes_.init(1));
    ASSERT_EQ(OB_SUCCESS, spec.column_comments_.init(1));
    ASSERT_EQ(OB_SUCCESS, spec.value_exprs_.init(2));
    ASSERT_EQ(OB_SUCCESS, spec.column_exprs_.init(output_count));
    ASSERT_EQ(OB_SUCCESS, spec.output_.init(output_count));
    ASSERT_EQ(OB_SUCCESS, spec.calc_exprs_.init(output_count));
    ASSERT_EQ(OB_SUCCESS, child_spec.output_.init(2));
    ASSERT_EQ(OB_SUCCESS, spec.search_idx_included_cid_idxes_.push_back(3));
    ASSERT_EQ(OB_SUCCESS, spec.column_comments_.push_back(comment_.string()));
    sql::ObExpr exprs[9];
    char *frames[9];
    for (int64_t i = 0; i < 2 + output_count; ++i) {
      frames[i] = static_cast<char *>(allocator_.alloc(256));
      ASSERT_NE(nullptr, frames[i]);
      MEMSET(frames[i], 0, 256);
      auto &expr = exprs[i];
      expr.frame_idx_ = i;
      expr.datum_off_ = 0;
      expr.eval_info_off_ = sizeof(ObDatum);
      expr.res_buf_off_ = 128;
      expr.res_buf_len_ = 8;
      expr.eval_flags_off_ = 200;
      const ObObjType type = i == 0 ? ObJsonType : (i == 3 || i == 4 || i == 8) ? ObVarcharType : ObUInt64Type;
      expr.obj_meta_.set_type(type);
      expr.obj_meta_.set_collation_type(CS_TYPE_BINARY);
      expr.datum_meta_.type_ = type;
      expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
      auto *datum = new (frames[i]) ObDatum();
      datum->ptr_ = frames[i] + 128;
      if (i < 2) {
        ASSERT_EQ(OB_SUCCESS, child_spec.output_.push_back(&expr));
        ASSERT_EQ(OB_SUCCESS, spec.value_exprs_.push_back(&expr));
      } else {
        ASSERT_EQ(OB_SUCCESS, spec.column_exprs_.push_back(&expr));
        ASSERT_EQ(OB_SUCCESS, spec.output_.push_back(&expr));
        ASSERT_EQ(OB_SUCCESS, spec.calc_exprs_.push_back(&expr));
      }
    }
    ctx.set_frames(frames);
    ctx.set_frame_cnt(2 + output_count);
    SearchIndexInputOp child(ctx, child_spec, input_.storage_datums_[0]);
    sql::ObJsonTableOp op(ctx, spec, nullptr);
    sql::ObOperator *children[] = {&child};
    sql::ObOpSpec *child_specs[] = {&child_spec};
    ASSERT_EQ(OB_SUCCESS, spec.set_children_pointer(child_specs, 1));
    ASSERT_EQ(OB_SUCCESS, op.set_children_pointer(children, 1));
    ASSERT_EQ(OB_SUCCESS, op.open());
    for (int scan = 0; scan < 2; ++scan) {
      int64_t count = 0;
      int ret = OB_SUCCESS;
      while ((ret = op.get_next_row()) == OB_SUCCESS) {
        const auto &doc = exprs[5].locate_expr_datum(op.get_eval_ctx());
        EXPECT_TRUE(doc.get_uint() == 42 || doc.get_uint() == 44 || target == SEARCH_DATA);
        if (target != SEARCH_DATA) {
          EXPECT_EQ(3, exprs[7].locate_expr_datum(op.get_eval_ctx()).get_uint());
        }
        ++count;
      }
      ASSERT_EQ(OB_ITER_END, ret);
      EXPECT_EQ(target == SEARCH_DATA ? 3 : 4, count);
      if (scan == 0) { ASSERT_EQ(OB_SUCCESS, op.rescan()); }
    }
    ASSERT_EQ(OB_SUCCESS, op.close());
  }
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  int result = 1;
  (void)oceanbase::ObPreProcessSysVars::init_sys_var();
  (void)oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_search_index_generator.log", true);
  OB_LOGGER.set_log_level("WARN");
  plugin::ObPluginMgr plugins;
  if (OB_FAIL(oceanbase::common::ObCharset::init_charset())) {
    LOG_WARN("failed to initialize charsets", K(ret));
  } else if (OB_FAIL(plugins.init(ObString()))) {
    LOG_WARN("failed to initialize plugin manager", K(ret));
  } else if (OB_FAIL(plugins.load_builtin_plugins())) {
    LOG_WARN("failed to load builtin plugins", K(ret));
  } else {
    GCTX.plugin_mgr_ = &plugins;
    result = RUN_ALL_TESTS();
    GCTX.plugin_mgr_ = nullptr;
  }
  return result;
}
