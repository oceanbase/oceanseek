/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <memory>
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/search_index/ob_search_index_generator.h"
#include "lib/json_type/ob_json_base.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/ob_sql_init.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "storage/fts/ob_fts_tokenizer.h"
#include "share/ob_fts_pos_list_codec.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::blocksstable;

class SearchTextExecutionTest : public ::testing::Test
{
protected:
  ObArenaAllocator allocator_;

  ObColumnRefRawExpr field_;

  static ObObjMeta token_meta(ObCollationType coll = CS_TYPE_UTF8MB4_BIN)
  {
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(coll);
    return meta;
  }

  std::unique_ptr<ObFullTextQueryNode> query(const char *path = "$.title", int64_t column_idx = 3,
                                          bool phrase = false)
  {
    field_.set_ref_id(1001, 42);
    field_.set_data_type(ObJsonType);
    std::unique_ptr<ObFullTextQueryNode> node;
    if (phrase) {
      auto *match = new ObMatchPhraseQueryNode(allocator_);
      match->field_ = &field_;
      match->inv_idx_tid_ = 3002;
      node.reset(match);
    } else {
      auto *match = new ObMatchQueryNode(allocator_);
      match->field_ = &field_;
      match->inv_idx_tid_ = 3001;
      node.reset(match);
    }
    node->search_index_fulltext_info_.path_ = ObString::make_string(path);
    node->is_async_ = true;
    ObSearchIndexTextInfo &value = node->search_index_fulltext_info_;
    value.main_table_id_ = 1001;
    value.def_table_id_ = 2001;
    value.column_idx_ = column_idx;
    value.index_type_ = phrase ? OB_FTS_INDEX_TYPE_PHRASE_MATCH : OB_FTS_INDEX_TYPE_MATCH;
    value.parser_name_ = ObString("space.1");
    value.parser_properties_ = ObString("{\"min_token_size\":1,\"max_token_size\":84}");
    return node;
  }

  void expect_same_info(const ObDASSearchIndexTextInfo &expected, const ObDASSearchIndexTextInfo &actual)
  {
    EXPECT_EQ(expected.column_idx_, actual.column_idx_);
    EXPECT_EQ(expected.is_json_, actual.is_json_);
    EXPECT_EQ(expected.encoded_path_, actual.encoded_path_);
    EXPECT_EQ(expected.parser_name_, actual.parser_name_);
    EXPECT_EQ(expected.parser_properties_, actual.parser_properties_);
  }

  void compare_generator(const char *json, const char *path, bool phrase, const char *token = "alpha")
  {
    auto selected = query(path, 3, phrase);
    ObDASSearchIndexTextInfo info;
    ASSERT_EQ(OB_SUCCESS, info.assign(field_, selected->search_index_fulltext_info_, allocator_));
    ObObjMeta query_meta;
    query_meta.set_varchar();
    query_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObSEArray<ObString, 1> query_tokens;
    ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_, ObString(token),
        info.parser_name_, info.parser_properties_, query_meta, query_tokens));
    ASSERT_EQ(1, query_tokens.count());
    ObDatumRowkey key;
    ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(info, query_tokens.at(0), query_meta, allocator_, key));
    const std::string expected = std::string(token) + std::string(2, '\0');
    EXPECT_EQ(ObString(expected.size(), expected.data()), key.get_datum(2).get_string());

    ObSearchIndexModeConfig mode;
    mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
    mode.parser_name_ = selected->search_index_fulltext_info_.parser_name_;
    mode.parser_properties_ = selected->search_index_fulltext_info_.parser_properties_;
    ObSEArray<ObSearchIndexModeConfig, 1> modes;
    ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
    ObSearchIndexConfig config;
    ASSERT_EQ(OB_SUCCESS, config.init(modes));
    ObSqlString comment;
    ASSERT_EQ(OB_SUCCESS, config.encode(comment));
    ObSEArray<int32_t, 1> columns;
    ObSEArray<ObObjMeta, 1> metas;
    ObSEArray<int64_t, 2> projector;
    ObSEArray<ObString, 1> comments;
    ObObjMeta meta;
    meta.set_json();
    ASSERT_EQ(OB_SUCCESS, columns.push_back(selected->search_index_fulltext_info_.column_idx_));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
    ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
    ASSERT_EQ(OB_SUCCESS, comments.push_back(comment.string()));
    ObSearchIndexRowGenerator generator;
    ASSERT_EQ(OB_SUCCESS, generator.init(columns, metas, projector, comments, false, nullptr,
        phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
    ObIJsonBase *tree = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString(json),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, tree));
    ObString binary;
    ASSERT_EQ(OB_SUCCESS, tree->get_raw_binary(binary));
    ObDatum with_header;
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::string_to_templob_result(
        ObJsonType, allocator_, binary, with_header));
    ObDatumRow input;
    ASSERT_EQ(OB_SUCCESS, input.init(allocator_, 2));
    input.storage_datums_[0].shallow_copy_from_datum(with_header);
    input.storage_datums_[1].set_uint(77);
    ObDatumRow *rows = nullptr;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, generator.generate_rows(input, rows, count));
    int64_t matched = 0;
    for (int64_t i = 0; i < count; ++i) {
      if (rows[i].storage_datums_[0].get_uint() == key.get_datum(0).get_uint()
          && rows[i].storage_datums_[1].get_string() == key.get_datum(1).get_string()
          && rows[i].storage_datums_[2].get_string() == key.get_datum(2).get_string()) {
        ++matched;
        EXPECT_EQ(77, rows[i].storage_datums_[3].get_uint());
        EXPECT_EQ(phrase ? 7 : 6, rows[i].get_column_count());
      }
    }
    EXPECT_EQ(1, matched);
  }
};

TEST_F(SearchTextExecutionTest, binding_copy_owns_path_and_effective_parser)
{
  char path[] = "$.title";
  char parser[] = "space.1";
  char properties[] = "{\"min_token_size\":1}";
  auto selected = query();
  selected->search_index_fulltext_info_.path_ = ObString(path);
  selected->search_index_fulltext_info_.parser_name_ = ObString(parser);
  selected->search_index_fulltext_info_.parser_properties_ = ObString(properties);
  ObDASSearchIndexTextInfo info;
  ASSERT_EQ(OB_SUCCESS, info.assign(field_, selected->search_index_fulltext_info_, allocator_));
  const std::string encoded_path(info.encoded_path_.ptr(), info.encoded_path_.length());
  MEMSET(path, 'x', sizeof(path));
  MEMSET(parser, 'x', sizeof(parser));
  MEMSET(properties, 'x', sizeof(properties));
  EXPECT_TRUE(info.has_search_index_fulltext());
  EXPECT_EQ(ObString(encoded_path.length(), encoded_path.data()), info.encoded_path_);
  EXPECT_EQ(ObString("space.1"), info.parser_name_);
  EXPECT_EQ(ObString("{\"min_token_size\":1}"), info.parser_properties_);
  EXPECT_EQ(3, info.column_idx_);
  EXPECT_NE(ObString("$.title"), info.encoded_path_);
}

TEST_F(SearchTextExecutionTest, ir_serialization_preserves_binding_and_owns_strings)
{
  for (bool is_json : {true, false}) {
    ObDASIRScanCtDef original(allocator_);
    const std::unique_ptr<ObFullTextQueryNode> selected = query(is_json ? "$.title" : "$", 7, true);
    field_.set_data_type(is_json ? ObJsonType : ObCharType);
    ASSERT_EQ(OB_SUCCESS, original.search_index_fulltext_info_.assign(
        field_, selected->search_index_fulltext_info_, allocator_));
    std::vector<char> wire(original.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, original.serialize(wire.data(), wire.size(), pos));
    EXPECT_EQ(wire.size(), pos);
    ObArenaAllocator destination;
    ObDASIRScanCtDef decoded(destination);
    int64_t decoded_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.data(), pos, decoded_pos));
    EXPECT_EQ(pos, decoded_pos);
    MEMSET(wire.data(), 'x', wire.size());
    expect_same_info(original.search_index_fulltext_info_, decoded.search_index_fulltext_info_);
    EXPECT_EQ(is_json, decoded.search_index_fulltext_info_.is_json_);
    if (!original.search_index_fulltext_info_.encoded_path_.empty()) {
      EXPECT_NE(original.search_index_fulltext_info_.encoded_path_.ptr(), decoded.search_index_fulltext_info_.encoded_path_.ptr());
    }
    EXPECT_NE(original.search_index_fulltext_info_.parser_name_.ptr(), decoded.search_index_fulltext_info_.parser_name_.ptr());
  }
}

TEST_F(SearchTextExecutionTest, unbound_ir_remains_ordinary_fulltext_after_serialization)
{
  ObDASIRScanCtDef original(allocator_);
  std::vector<char> wire(original.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, original.serialize(wire.data(), wire.size(), pos));
  ObDASIRScanCtDef decoded(allocator_);
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.data(), pos, decoded_pos));
  EXPECT_FALSE(decoded.search_index_fulltext_info_.has_search_index_fulltext());
  EXPECT_EQ(-1, decoded.search_index_fulltext_info_.column_idx_);
  EXPECT_TRUE(decoded.search_index_fulltext_info_.parser_name_.empty());
}

TEST_F(SearchTextExecutionTest, root_path_is_an_empty_non_null_term_component)
{
  ObDASSearchIndexTextInfo info;
  ASSERT_EQ(OB_SUCCESS, info.assign(field_, query("$")->search_index_fulltext_info_, allocator_));
  EXPECT_TRUE(info.encoded_path_.empty());
  ObDatumRowkey key;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(info, ObString("alpha"), token_meta(), allocator_, key));
  ASSERT_EQ(3, key.get_datum_cnt());
  EXPECT_FALSE(key.get_datum(1).is_null());
  EXPECT_TRUE(key.get_datum(1).get_string().empty());
}

TEST_F(SearchTextExecutionTest, term_prefix_isolates_columns_and_paths_and_owns_memory)
{
  ObDASSearchIndexTextInfo first;
  ObDASSearchIndexTextInfo other_column;
  ObDASSearchIndexTextInfo other_path;
  ASSERT_EQ(OB_SUCCESS, first.assign(field_, query("$.title", 3)->search_index_fulltext_info_, allocator_));
  ASSERT_EQ(OB_SUCCESS, other_column.assign(field_, query("$.title", 7)->search_index_fulltext_info_, allocator_));
  ASSERT_EQ(OB_SUCCESS, other_path.assign(field_, query("$.body", 3)->search_index_fulltext_info_, allocator_));
  ObArenaAllocator keys_allocator;
  ObDatumRowkey keys[3];
  char raw[] = "alpha";
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(first, ObString(raw), token_meta(), keys_allocator, keys[0]));
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(other_column, ObString(raw), token_meta(), keys_allocator, keys[1]));
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(other_path, ObString(raw), token_meta(), keys_allocator, keys[2]));
  EXPECT_EQ(ObString(7, "alpha\0\0"), keys[0].get_datum(2).get_string());
  EXPECT_NE(keys[0].get_datum(0).get_uint(), keys[1].get_datum(0).get_uint());
  EXPECT_EQ(keys[0].get_datum(1).get_string(), keys[1].get_datum(1).get_string());
  EXPECT_NE(keys[0].get_datum(1).get_string(), keys[2].get_datum(1).get_string());
  EXPECT_EQ(keys[0].get_datum(2).get_string(), keys[1].get_datum(2).get_string());
  EXPECT_EQ(keys[0].get_datum(2).get_string(), keys[2].get_datum(2).get_string());
  ObString expected_path;
  ObString expected_token;
  ASSERT_EQ(OB_SUCCESS, ob_write_string(keys_allocator, keys[0].get_datum(1).get_string(), expected_path));
  ASSERT_EQ(OB_SUCCESS, ob_write_string(keys_allocator, keys[0].get_datum(2).get_string(), expected_token));
  MEMSET(raw, 'x', sizeof(raw));
  allocator_.reset();
  EXPECT_EQ(expected_path, keys[0].get_datum(1).get_string());
  EXPECT_EQ(expected_token, keys[0].get_datum(2).get_string());
}

TEST_F(SearchTextExecutionTest, generated_posting_prefix_matches_query_encoding)
{
  for (bool phrase : {false, true}) {
    ASSERT_NO_FATAL_FAILURE(compare_generator("\"alpha beta alpha\"", "$", phrase));
    ASSERT_NO_FATAL_FAILURE(compare_generator(
        "{\"title\":\"alpha beta alpha\",\"body\":\"alpha\"}", "$.title", phrase));
    ASSERT_NO_FATAL_FAILURE(compare_generator(
        "{\"a.b\":\"alpha\",\"a\":{\"b\":\"beta\"}}", "$.\"a.b\"", phrase));
    ASSERT_NO_FATAL_FAILURE(compare_generator(
        "{\"n\\u0000k\":\"alpha\"}", "$.\"n\\u0000k\"", phrase));
    ASSERT_NO_FATAL_FAILURE(compare_generator(
        "{\"title\":\"数据库 café\"}", "$.title", phrase, "数据库"));
    ASSERT_NO_FATAL_FAILURE(compare_generator(
        "{\"title\":\"数据库 café\"}", "$.title", phrase, "café"));
  }
}


#ifndef OB_BUILD_PACKAGE
TEST_F(SearchTextExecutionTest, keyword_char_tokens_match_value_encoding)
{
  for (const ObCollationType coll : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_GENERAL_CI,
                                   CS_TYPE_GBK_BIN, CS_TYPE_UTF16_BIN, CS_TYPE_UTF16LE_GENERAL_CI}) {
    for (bool phrase : {false, true}) {
      SCOPED_TRACE(coll);
      SCOPED_TRACE(phrase);
      std::unique_ptr<ObFullTextQueryNode> selected = query("$", 3, phrase);
      field_.set_data_type(ObCharType);
      field_.set_collation_type(coll);
      selected->search_index_fulltext_info_.parser_name_ = ObString("analyzer.1");
      selected->search_index_fulltext_info_.parser_properties_ = ObString(
          R"({"analyzer":{"a":{"type":"custom","tokenizer":"keyword"}}})");
      ObDASSearchIndexTextInfo info;
      ASSERT_EQ(OB_SUCCESS, info.assign(field_, selected->search_index_fulltext_info_, allocator_));
      ObObjMeta source = token_meta(coll);
      source.set_type(ObCharType);
      ObSearchIndexModeConfig mode;
      mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
      mode.parser_name_ = info.parser_name_;
      mode.parser_properties_ = info.parser_properties_;
      ObSearchIndexRowGenerator generators[2];
      for (int64_t i = 0; i < 2; ++i) {
        ObSEArray<ObSearchIndexModeConfig, 1> modes;
        ASSERT_EQ(OB_SUCCESS, modes.push_back(i == 0 ? ObSearchIndexModeConfig() : mode));
        ObSearchIndexConfig config;
        ASSERT_EQ(OB_SUCCESS, config.init(modes));
        ObSqlString comment;
        ASSERT_EQ(OB_SUCCESS, config.encode(comment));
        ObSEArray<int32_t, 1> columns;
        ObSEArray<ObObjMeta, 1> metas;
        ObSEArray<int64_t, 2> projector;
        ObSEArray<ObString, 1> comments;
        ASSERT_EQ(OB_SUCCESS, columns.push_back(3));
        ASSERT_EQ(OB_SUCCESS, metas.push_back(source));
        ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
        ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
        ASSERT_EQ(OB_SUCCESS, comments.push_back(comment.string()));
        ASSERT_EQ(OB_SUCCESS, generators[i].init(columns, metas, projector, comments, false, nullptr,
            i == 0 ? SEARCH_DATA : phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
      }
      for (const char *input : {"foo", "foo ", "foo  "}) {
        SCOPED_TRACE(input);
        ObString data;
        ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(allocator_, ObString(input),
            CS_TYPE_UTF8MB4_BIN, coll, data, ObCharset::COPY_STRING_ON_SAME_CHARSET));
        ObSEArray<ObString, 1> tokens;
        ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_, data,
            info.parser_name_, info.parser_properties_, token_meta(coll), tokens));
        ASSERT_EQ(1, tokens.count());
        ASSERT_EQ(data, tokens.at(0));
        ObDatumRow row;
        ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 2));
        row.storage_datums_[0].set_string(data);
        row.storage_datums_[1].set_uint(77);
        ObDatumRow *rows[2] = {nullptr, nullptr};
        for (int64_t i = 0; i < 2; ++i) {
          int64_t count = 0;
          ASSERT_EQ(OB_SUCCESS, generators[i].generate_rows(row, rows[i], count));
          ASSERT_EQ(1, count);
        }
        const ObString encoded_value = rows[0]->storage_datums_[2].get_string();
        EXPECT_EQ(encoded_value, rows[1]->storage_datums_[2].get_string());
        ObDatumRowkey key;
        ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(
            info, tokens.at(0), token_meta(coll), allocator_, key));
        EXPECT_EQ(encoded_value, key.get_datum(2).get_string());
      }
    }
  }
}
#endif

TEST_F(SearchTextExecutionTest, string_postings_match_query_keys_and_preserve_statistics)
{
  const ObObjType types[] = {ObVarcharType, ObCharType, ObTinyTextType, ObTextType, ObMediumTextType, ObLongTextType};
  // Unsupported collations are rejected by DDL and covered in SearchIndexConfigTest.
  const ObCollationType collations[] = {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_GENERAL_CI,
      CS_TYPE_GBK_BIN, CS_TYPE_GBK_CHINESE_CI,
      CS_TYPE_UTF16_BIN, CS_TYPE_UTF16_GENERAL_CI, CS_TYPE_UTF16LE_BIN, CS_TYPE_UTF16LE_GENERAL_CI,
      CS_TYPE_UTF8MB4_0900_BIN, CS_TYPE_GB18030_BIN, CS_TYPE_GB18030_CHINESE_CI,
      CS_TYPE_GB18030_2022_BIN, CS_TYPE_GB18030_2022_PINYIN_CI, CS_TYPE_GB18030_2022_PINYIN_CS,
      CS_TYPE_GB18030_2022_RADICAL_CI, CS_TYPE_GB18030_2022_RADICAL_CS,
      CS_TYPE_GB18030_2022_STROKE_CI, CS_TYPE_GB18030_2022_STROKE_CS};
  for (ObObjType type : types) {
    for (ObCollationType coll : collations) {
      for (bool phrase : {false, true}) {
        SCOPED_TRACE(type);
        SCOPED_TRACE(coll);
        SCOPED_TRACE(phrase);
        std::unique_ptr<ObFullTextQueryNode> selected = query("$", 3, phrase);
        field_.set_data_type(type);
        field_.set_collation_type(coll);
        ObDASSearchIndexTextInfo info;
        ASSERT_EQ(OB_SUCCESS, info.assign(field_, selected->search_index_fulltext_info_, allocator_));
        ObSearchIndexModeConfig mode;
        mode.mode_ = phrase ? ObSearchIndexModeConfig::FULLTEXT_PHRASE : ObSearchIndexModeConfig::FULLTEXT_MATCH;
        mode.parser_name_ = info.parser_name_;
        mode.parser_properties_ = info.parser_properties_;
        ObSEArray<ObSearchIndexModeConfig, 1> modes;
        ASSERT_EQ(OB_SUCCESS, modes.push_back(mode));
        ObSearchIndexConfig config;
        ASSERT_EQ(OB_SUCCESS, config.init(modes));
        ObSqlString comment;
        ASSERT_EQ(OB_SUCCESS, config.encode(comment));
        ObSEArray<int32_t, 1> columns;
        ObSEArray<ObObjMeta, 1> metas;
        ObSEArray<int64_t, 2> projector;
        ObSEArray<ObString, 1> comments;
        ObObjMeta source = token_meta(coll);
        source.set_type(type);
        ASSERT_EQ(OB_SUCCESS, columns.push_back(3));
        ASSERT_EQ(OB_SUCCESS, metas.push_back(source));
        ASSERT_EQ(OB_SUCCESS, projector.push_back(0));
        ASSERT_EQ(OB_SUCCESS, projector.push_back(1));
        ASSERT_EQ(OB_SUCCESS, comments.push_back(comment.string()));
        ObSearchIndexRowGenerator generator;
        ASSERT_EQ(OB_SUCCESS, generator.init(columns, metas, projector, comments, false, nullptr,
            phrase ? SEARCH_TEXT_PHRASE : SEARCH_TEXT_MATCH));
        ObDatumRow input;
        ASSERT_EQ(OB_SUCCESS, input.init(allocator_, 2));
        input.storage_datums_[1].set_uint(77);
        std::vector<std::string> documents = {"cafe café cafe   ", "Alpha alpha ALPHA", "", "NULL"};
        documents.push_back("数据库 搜索 数据库");
        const ObCharsetType charset = ObCharset::charset_type_by_coll(coll);
        if (charset == CHARSET_UTF8MB4 || charset == CHARSET_UTF16 || charset == CHARSET_UTF16LE) {
          documents.push_back("straße strasse");
          documents.push_back("straße strase straße strase");
          documents.push_back("œ oe ﬃ ffi é e\u0301");
        }
        if (ob_is_text_tc(type) && type != ObTinyTextType) {
          documents.push_back(std::string());
          for (int i = 0; i < 1000; ++i) { documents.back() += "alpha "; }
        }
        for (const std::string &document : documents) {
          SCOPED_TRACE(document.substr(0, 30));
          ObString data;
          ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(allocator_, ObString(document.size(), document.data()),
              CS_TYPE_UTF8MB4_BIN, coll, data, ObCharset::COPY_STRING_ON_SAME_CHARSET));
          if (document == "NULL") {
            input.storage_datums_[0].set_null();
          } else if (ob_is_text_tc(type)) {
            ObDatum lob;
            ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::string_to_templob_result(type, allocator_, data, lob));
            input.storage_datums_[0].shallow_copy_from_datum(lob);
          } else {
            input.storage_datums_[0].set_string(data);
          }
          ObDatumRow *rows = nullptr;
          int64_t count = 0;
          ASSERT_EQ(OB_SUCCESS, generator.generate_rows(input, rows, count));
          if (document.empty() || document == "NULL") {
            EXPECT_EQ(0, count);
            continue;
          }
          ObSEArray<ObString, 4> tokens;
          ObSEArray<int64_t, 4> counts;
          ObSEArray<int64_t, 4> ids;
          ObSEArray<int64_t, 4> positions;
          bool duplicates = false;
          ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_, data,
              info.parser_name_, info.parser_properties_, token_meta(coll), tokens, ids, positions, duplicates));
          ASSERT_EQ(tokens.count(), count);
          ObSEArray<ObString, 4> counted_tokens;
          ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_, data,
              info.parser_name_, info.parser_properties_, token_meta(coll), counted_tokens, counts));
          ASSERT_EQ(tokens.count(), counted_tokens.count());
          int64_t expected_length = 0;
          for (int64_t t = 0; t < counts.count(); ++t) {
            ASSERT_EQ(tokens[t], counted_tokens[t]);
            expected_length += counts[t];
          }
          int64_t matched = 0;
          for (int64_t t = 0; t < tokens.count(); ++t) {
            ObDatumRowkey key;
            ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(info, tokens[t], token_meta(coll), allocator_, key));
            for (int64_t r = 0; r < count; ++r) {
              if (key.get_datum(2).get_string() != rows[r].storage_datums_[2].get_string()) { continue; }
              ++matched;
              EXPECT_EQ(3, rows[r].storage_datums_[0].get_uint());
              EXPECT_FALSE(rows[r].storage_datums_[1].is_null());
              EXPECT_TRUE(rows[r].storage_datums_[1].get_string().empty());
              EXPECT_EQ(77, rows[r].storage_datums_[3].get_uint());
              ObSEArray<int64_t, 4> expected_positions;
              for (int64_t i = 0; i < ids.count(); ++i) {
                if (ids[i] == t) { ASSERT_EQ(OB_SUCCESS, expected_positions.push_back(positions[i])); }
              }
              // Position lists are capped; TF and DL retain every occurrence.
              EXPECT_EQ(counts[t], rows[r].storage_datums_[4].get_uint());
              EXPECT_EQ(expected_length, rows[r].storage_datums_[5].get_uint());
              if (phrase) {
                ObFTSPositionListStore store;
                ObSEArray<int64_t, 4> decoded;
                const ObString encoded = rows[r].storage_datums_[6].get_string();
                ASSERT_EQ(OB_SUCCESS, store.deserialize_and_decode(encoded.ptr(), encoded.length(), decoded));
                ASSERT_EQ(expected_positions.count(), decoded.count());
                for (int64_t i = 0; i < decoded.count(); ++i) { EXPECT_EQ(expected_positions[i], decoded[i]); }
              }
            }
          }
          EXPECT_EQ(count, matched);
          if (document == "cafe café cafe   " && ObCharset::charset_type_by_coll(coll) != CHARSET_GBK) {
            EXPECT_EQ(3, ids.count());
            ObString plain_text;
            ObString query_text;
            ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(allocator_, ObString("cafe"), CS_TYPE_UTF8MB4_BIN, coll, plain_text));
            ASSERT_EQ(OB_SUCCESS, ObCharset::charset_convert(allocator_, ObString("café"), CS_TYPE_UTF8MB4_BIN, coll, query_text));
            // Case-insensitive collations need not be accent-insensitive. Use the
            // source comparison as the oracle, independently of token/key encoding.
            EXPECT_EQ(ObCharset::strcmp(coll, plain_text, query_text) == 0 ? 1 : 2, count);
            // An independently tokenized query must find its posting, including
            // when the source collation groups café under the representative cafe.
            ObSEArray<ObString, 1> query_tokens;
            ASSERT_EQ(OB_SUCCESS, storage::ObFTSTokenizer::tokenize(allocator_, query_text,
                info.parser_name_, info.parser_properties_, token_meta(coll), query_tokens));
            ASSERT_EQ(1, query_tokens.count());
            ObDatumRowkey key;
            ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(info, query_tokens[0], token_meta(coll), allocator_, key));
            int64_t found = 0;
            for (int64_t i = 0; i < count; ++i) {
              if (rows[i].storage_datums_[2].get_string() == key.get_datum(2).get_string()) { ++found; }
            }
            EXPECT_EQ(1, found);
          }
        }
      }
    }
  }
}

TEST_F(SearchTextExecutionTest, parses_bound_paths_and_accepts_phrase_fallback)
{
  ObDASSearchIndexTextInfo info;
  for (const char *path : {"", "title", "$."}) {
    SCOPED_TRACE(path);
    EXPECT_EQ(OB_INVALID_ARGUMENT, info.assign(field_, query(path)->search_index_fulltext_info_, allocator_));
    EXPECT_FALSE(info.has_search_index_fulltext());
  }
  auto fallback = query();
  fallback->search_index_fulltext_info_.index_type_ = OB_FTS_INDEX_TYPE_PHRASE_MATCH;
  EXPECT_EQ(OB_SUCCESS, info.assign(field_, fallback->search_index_fulltext_info_, allocator_));
}

TEST_F(SearchTextExecutionTest, rejects_oversized_tokens_and_invalid_keys)
{
  ObDASSearchIndexTextInfo info;
  ObDatumRowkey key;
  ASSERT_EQ(OB_SUCCESS, info.assign(field_, query()->search_index_fulltext_info_, allocator_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDASSearchUtils::build_fulltext_term_key(info, ObString(), token_meta(), allocator_, key));
  EXPECT_EQ(0, key.get_datum_cnt());
  for (const char *character : {"a", "\xF0\xA0\x80\x80"}) {
    std::string token;
    for (int64_t i = 0; i < (SEARCH_INDEX_VALUE_LENGTH - 2) / static_cast<int64_t>(strlen(character)); ++i) {
      token += character;
    }
    ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::build_fulltext_term_key(info, ObString(token.size(), token.data()), token_meta(), allocator_, key));
    const std::string encoded = token + std::string(2, '\0');
    EXPECT_EQ(ObString(encoded.size(), encoded.data()), key.get_datum(2).get_string());
    token += character;
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObDASSearchUtils::build_fulltext_term_key(
        info, ObString(token.size(), token.data()), token_meta(), allocator_, key));
    EXPECT_EQ(0, key.get_datum_cnt());
  }
  std::string path = "$.\"" + std::string(SEARCH_INDEX_PATH_LENGTH + 1, 'a') + "\"";
  EXPECT_EQ(OB_ERR_TOO_LONG_KEY_LENGTH, info.assign(field_, query(path.c_str())->search_index_fulltext_info_, allocator_));
  EXPECT_FALSE(info.has_search_index_fulltext());
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  int result = 1;
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_search_text_execution.log", true);
  OB_LOGGER.set_log_level("INFO");
  plugin::ObPluginMgr plugins;
  if (OB_FAIL(oceanbase::common::ObCharset::init_charset())) {
    LOG_WARN("failed to initialize charsets", K(ret));
  } else if (OB_FAIL(init_sql_factories())) {
    LOG_WARN("failed to initialize SQL factories", K(ret));
  } else if (OB_FAIL(plugins.init(ObString()))) {
    LOG_WARN("failed to initialize plugin manager", K(ret));
  } else if (OB_FAIL(plugins.load_builtin_plugins())) {
    LOG_WARN("failed to load builtin plugins", K(ret));
  } else {
    auto *saved = GCTX.plugin_mgr_;
    GCTX.plugin_mgr_ = &plugins;
    auto restore = MakeScopedLambda([&]() { GCTX.plugin_mgr_ = saved; });
    result = RUN_ALL_TESTS();
  }
  return result;
}
