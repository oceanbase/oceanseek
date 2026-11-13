/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "ob_search_index_config_resolver.h"
#include "ob_fts_parser_resolver.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "plugin/legacy/sys/ob_plugin_helper.h"
#include "plugin/legacy/interface/ob_plugin_ftparser_intf.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;

// Borrow option nodes until the current block is validated and appended.
static int collect_options(const ParseNode *node, bool allow_nested, ObItemType &curr_mode,
                           const ParseNode *&nested_path,
                           const ParseNode *&curr_include_path, const ParseNode *&curr_exclude_path,
                           const ParseNode *&curr_include_types, const ParseNode *&curr_parser,
                           const ParseNode *&curr_properties, const ParseNode *&curr_fts_type)
{
  int ret = OB_SUCCESS;
  if (nullptr == node) {
  } else if (T_SEARCH_INDEX_TYPE == node->type_) {
    if (node->value_ != T_SEARCH_INDEX_VALUE && node->value_ != T_SEARCH_INDEX_FULLTEXT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid search index TYPE option", K(ret), K(node->value_));
    } else {
      curr_mode = static_cast<ObItemType>(node->value_);
    }
  } else if (T_LINK_NODE == node->type_ || T_SEARCH_INDEX_OPTION_LIST == node->type_) {
    if (node->num_child_ <= 0 || nullptr == node->children_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid search index option list", K(ret), K(node->num_child_), KP(node->children_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      OZ(collect_options(node->children_[i], allow_nested, curr_mode, nested_path,
                         curr_include_path, curr_exclude_path, curr_include_types,
                         curr_parser, curr_properties, curr_fts_type));
    }
  } else {
    const ParseNode **slot = nullptr;
    switch (node->type_) {
      case T_SEARCH_INDEX_INCLUDE_PATHS: slot = &curr_include_path; break;
      case T_SEARCH_INDEX_EXCLUDE_PATHS: slot = &curr_exclude_path; break;
      case T_SEARCH_INDEX_INCLUDE_TYPES: slot = &curr_include_types; break;
      case T_PARSER_NAME: slot = &curr_parser; break;
      case T_PARSER_PROPERTIES: slot = &curr_properties; break;
      case T_FTS_INDEX_TYPE: slot = &curr_fts_type; break;
      case T_SEARCH_INDEX_NESTED_PATHS:
        if (!allow_nested) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "NESTED_PATHS inside a mode block or with TYPE=FULLTEXT is");
        } else {
          slot = &nested_path;
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown search index option type", K(ret), K(node->type_));
        break;
    }
    if (OB_SUCC(ret)) {
      if (nullptr != *slot) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate search index option");
      } else if (node->num_child_ <= 0 || nullptr == node->children_ || nullptr == node->children_[0]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid search index option children", K(ret), K(node->type_),
                 K(node->num_child_), KP(node->children_));
      } else {
        *slot = node;
      }
    }
  }
  return ret;
}

static int resolve_fulltext(const ParseNode *parser_node, const ParseNode *properties_node,
                            const ParseNode *fts_type_node, const ObString &database_name,
                            uint64_t tenant_id, ObSchemaChecker *schema_checker,
                            ObCollationType source_cs, const ObString &filter, ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SearchIdxProps");
  ObSearchIndexModeConfig mode;
  mode.filter_ = filter;
  ObString parser_name = ObString::make_string(OB_DEFAULT_FULLTEXT_PARSER_NAME);
  ObString properties;
  if (parser_node != nullptr) {
    const ParseNode *name = parser_node->children_[0];
    parser_name.assign_ptr(name->str_value_, static_cast<int32_t>(name->str_len_));
    if (parser_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "Search FULLTEXT PARSER must not be empty");
    }
  }
  const bool is_analyzer = storage::ObFTParser::is_analyzer_parser_name(parser_name);
  bool has_analysis = false;
  if (OB_SUCC(ret) && properties_node != nullptr) {
    const ParseNode *props = properties_node;
    bool seen_properties[10] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < props->num_child_; ++i) {
      if (nullptr == props->children_[i]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null fulltext parser property", K(ret), K(i), K(parser_name));
      } else {
        has_analysis |= T_PARSER_ANALYSIS == props->children_[i]->type_;
        if (schema_checker == nullptr && (T_PARSER_DICT_TABLE == props->children_[i]->type_
            || T_PARSER_STOPWORD_TABLE == props->children_[i]->type_
            || T_PARSER_QUANTIFIER_TABLE == props->children_[i]->type_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("schema checker is required for fulltext parser table property",
                   K(ret), K(i), K(props->children_[i]->type_), K(parser_name));
        }
        if (OB_SUCC(ret)) {
          int64_t property_idx = -1;
          switch (props->children_[i]->type_) {
            case T_PARSER_MIN_TOKEN_SIZE: property_idx = 0; break;
            case T_PARSER_MAX_TOKEN_SIZE: property_idx = 1; break;
            case T_PARSER_NGRAM_TOKEN_SIZE: property_idx = 2; break;
            case T_PARSER_STOPWORD_TABLE: property_idx = 3; break;
            case T_PARSER_DICT_TABLE: property_idx = 4; break;
            case T_PARSER_QUANTIFIER_TABLE: property_idx = 5; break;
            case T_IK_MODE: property_idx = 6; break;
            case T_PARSER_MIN_NGRAM_SIZE: property_idx = 7; break;
            case T_PARSER_MAX_NGRAM_SIZE: property_idx = 8; break;
            case T_PARSER_ANALYSIS: property_idx = 9; break;
            default:
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("unknown fulltext parser property", K(ret), K(props->children_[i]->type_));
              break;
          }
          if (OB_SUCC(ret) && seen_properties[property_idx]) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate fulltext parser property");
          } else if (OB_SUCC(ret)) {
            seen_properties[property_idx] = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      OZ(ObFTParserResolverHelper::resolve_parser_properties(database_name, *props,
          tenant_id, allocator, schema_checker, properties));
    }
  }
  if (OB_SUCC(ret) && (is_analyzer != has_analysis)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "PARSER analyzer requires analysis and analysis requires PARSER analyzer");
  }
  storage::ObFTParser parser;
  if (OB_SUCC(ret) && is_analyzer) {
    ObPluginName name;
    OZ(name.set_name(storage::ObFTSLiteral::PARSER_NAME_ANALYZER));
    parser.set_name_and_version(name, storage::ANALYZER_PARSER_VERSION_PLACEHOLDER);
  } else if (OB_SUCC(ret)) {
    plugin::ObIFTParserDesc *desc = nullptr;
    plugin::ObPluginParam *param = nullptr;
    OZ(plugin::ObPluginHelper::find_ftparser(parser_name, parser));
    OZ(plugin::ObPluginHelper::find_ftparser(parser_name, desc, param));
    if (OB_SUCC(ret)) {
      if (nullptr == desc) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null fulltext parser descriptor", K(ret), K(parser_name));
      } else {
        OZ(desc->check_if_charset_supported(ObCharset::get_charset(source_cs)));
      }
    }
  }
  char name_buf[OB_PLUGIN_NAME_LENGTH] = {};
  OZ(parser.serialize_to_str(name_buf, sizeof(name_buf)));
  if (OB_SUCC(ret)) {
    mode.parser_name_ = ObString::make_string(name_buf);
  }
  if (OB_SUCC(ret) && !is_analyzer) {
    storage::ObFTParserJsonProps props;
    OZ(props.init());
    OZ(props.parse_from_valid_str(properties));
    // Persist effective defaults with the parser version before encoding the comment.
    OZ(props.rebuild_props_for_ddl(ObString::make_string(name_buf), source_cs, true, tenant_id));
    OZ(props.to_format_json(allocator, properties));
  }
  if (OB_SUCC(ret)) {
    mode.parser_properties_ = properties;
    // JSON FULLTEXT defaults are independent of ordinary FTS build/package defaults.
    mode.mode_ = ObSearchIndexModeConfig::FULLTEXT_PHRASE;
    if (fts_type_node != nullptr) {
      switch (fts_type_node->children_[0]->type_) {
        case T_FTS_INDEX_MATCH: mode.mode_ = ObSearchIndexModeConfig::FULLTEXT_MATCH; break;
        case T_FTS_INDEX_PHRASE_MATCH: break;
        case T_FTS_INDEX_FILTER:
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "FILTER for JSON fulltext index is");
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unknown search index fulltext type", K(ret), K(fts_type_node->children_[0]->type_));
          break;
      }
    }
  }
  OZ(mode.encode(comment));
  return ret;
}

static int check_is_valid_search_index_mode(ObItemType curr_mode,
                                            const ParseNode *curr_include_path,
                                            const ParseNode *curr_exclude_path,
                                            const ParseNode *curr_include_types,
                                            const ParseNode *curr_parser,
                                            const ParseNode *curr_properties,
                                            const ParseNode *curr_fts_type)
{
  int ret = OB_SUCCESS;
  if (curr_include_path != nullptr && curr_exclude_path != nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "search index: cannot set both include and exclude paths");
  } else if (T_SEARCH_INDEX_FULLTEXT == curr_mode && curr_include_types != nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "FULLTEXT cannot specify INCLUDE_TYPES");
  } else if (T_SEARCH_INDEX_VALUE == curr_mode
             && (curr_parser != nullptr || curr_properties != nullptr || curr_fts_type != nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "VALUE blocks cannot specify fulltext options");
  }
  return ret;
}

static int append_mode_comment(ObItemType curr_mode,
                               const ParseNode *curr_include_path, const ParseNode *curr_exclude_path,
                               const ParseNode *curr_include_types, const ParseNode *curr_parser,
                               const ParseNode *curr_properties, const ParseNode *curr_fts_type,
                               const ObString &database_name, uint64_t tenant_id,
                               ObSchemaChecker *schema_checker, const ObObjMeta &source_meta,
                               ObCollationType connection_collation, ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  ObSqlString filter;
  const ParseNode *path_option = curr_include_path != nullptr ? curr_include_path : curr_exclude_path;
  if (!source_meta.is_json() && (path_option != nullptr || curr_include_types != nullptr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "JSON path/type options on a non-JSON search index column");
  } else if (path_option != nullptr
      && OB_FAIL(ObSearchIndexConfigFilter::print_filter_option(path_option, connection_collation, filter))) {
    LOG_WARN("failed to print search index paths", K(ret));
  }
  if (T_SEARCH_INDEX_FULLTEXT == curr_mode) {
    OZ(resolve_fulltext(curr_parser, curr_properties, curr_fts_type, database_name,
                        tenant_id, schema_checker, source_meta.get_collation_type(),
                        filter.string(), comment));
  } else {
    if (OB_SUCC(ret) && curr_include_types != nullptr) {
      ObSqlString types;
      if (OB_FAIL(ObSearchIndexConfigFilter::print_filter_option(curr_include_types, connection_collation, types))) {
        LOG_WARN("failed to print search index include types", K(ret));
      } else {
        // Normalize INCLUDE_TYPES for both flat and named VALUE configurations.
        ObSearchIndexConfigFilter::ParsedFilter parsed;
        OZ(ObSearchIndexConfigFilter::parse_filter(types.string(), parsed));
        OZ(filter.append("INCLUDE_TYPES="));
        if (OB_SUCC(ret) && (parsed.type_mask_ & ObSearchIndexConfigFilter::TYPE_MASK_STRING) != 0) {
          OZ(filter.append("11json_string"));
        }
        if (OB_SUCC(ret) && (parsed.type_mask_ & ObSearchIndexConfigFilter::TYPE_MASK_NUMBER) != 0) {
          OZ(filter.append("11json_number"));
        }
      }
    }
    ObSearchIndexModeConfig mode;
    mode.filter_ = filter.string();
    OZ(mode.encode(comment));
  }
  return ret;
}

int ObSearchIndexConfigResolver::resolve(const ParseNode *column_options,
                                        const ObString &database_name,
                                        uint64_t tenant_id,
                                        uint64_t tenant_data_version,
                                        ObSchemaChecker *schema_checker,
                                        const ObObjMeta &source_meta,
                                        ObCollationType connection_collation,
                                        ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  comment.reset();
  const ParseNode *options = nullptr;
  if (column_options != nullptr) {
    if (column_options->type_ != T_SEARCH_INDEX_COLUMN_PARAMS
        || column_options->num_child_ != 1
        || nullptr == column_options->children_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid search index column options", K(ret), K(column_options->type_),
               K(column_options->num_child_), KP(column_options->children_));
    } else {
      options = column_options->children_[0];
    }
  }
  int64_t mode_count = 0;
  const ParseNode *nested_path = nullptr;
  const ParseNode *curr_include_path = nullptr;
  const ParseNode *curr_exclude_path = nullptr;
  const ParseNode *curr_include_types = nullptr;
  const ParseNode *curr_parser = nullptr;
  const ParseNode *curr_properties = nullptr;
  const ParseNode *curr_fts_type = nullptr;
  ObSqlString mode_comments;
  if (OB_FAIL(ret)) {
  } else if (options == nullptr || T_SEARCH_INDEX_OPTION_LIST == options->type_) {
    // col1 with (TYPE=VALUE, NESTED_PATHS=...), no mode blocks
    ObItemType curr_mode = T_SEARCH_INDEX_VALUE;
    if (OB_FAIL(collect_options(options, true, curr_mode, nested_path, curr_include_path, curr_exclude_path,
                                curr_include_types, curr_parser, curr_properties, curr_fts_type))) {
      LOG_WARN("failed to collect search index options", K(ret));
    } else if (T_SEARCH_INDEX_FULLTEXT == curr_mode && nested_path != nullptr) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "NESTED_PATHS with TYPE=FULLTEXT is");
    } else if (OB_FAIL(check_is_valid_search_index_mode(curr_mode, curr_include_path, curr_exclude_path,
                                                        curr_include_types, curr_parser, curr_properties,
                                                        curr_fts_type))) {
      LOG_WARN("invalid search index mode", K(ret), K(curr_mode));
    } else if (OB_FAIL(append_mode_comment(curr_mode, curr_include_path, curr_exclude_path,
                                           curr_include_types, curr_parser, curr_properties, curr_fts_type,
                                           database_name, tenant_id, schema_checker, source_meta, connection_collation, mode_comments))) {
      LOG_WARN("failed to append search index mode comment", K(ret));
    } else {
      mode_count = 1;
    }
  } else if (T_SEARCH_INDEX_MODE_LIST == options->type_) {
    // col1 with (NESTED_PATHS=..., VALUE, FULLTEXT(...)), multiple mode blocks
    for (int64_t i = 0; OB_SUCC(ret) && i < options->num_child_; ++i) {
      const ParseNode *node = options->children_[i];
      if (nullptr == node) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null search index column configuration item", K(ret), K(i));
      } else if (T_SEARCH_INDEX_NESTED_PATHS == node->type_) {
        if (nested_path != nullptr) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate search index option");
        } else if (node->num_child_ <= 0 || nullptr == node->children_ || nullptr == node->children_[0]) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid search index nested path children", K(ret), K(node->num_child_), KP(node->children_));
        } else {
          nested_path = node;
        }
      } else if (T_SEARCH_INDEX_VALUE == node->type_ || T_SEARCH_INDEX_FULLTEXT == node->type_) {
        ObItemType curr_mode;
        if (node->num_child_ != 1 || nullptr == node->children_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid search index mode node", K(ret), K(i), KP(node));
        } else if (OB_FALSE_IT(curr_mode = node->type_)) {
        } else if (OB_FAIL(collect_options(node->children_[0], false, curr_mode, nested_path,
                                           curr_include_path, curr_exclude_path, curr_include_types,
                                           curr_parser, curr_properties, curr_fts_type))) {
          LOG_WARN("failed to collect search index options", K(ret));
        } else if (OB_FAIL(check_is_valid_search_index_mode(curr_mode, curr_include_path, curr_exclude_path,
                                                            curr_include_types, curr_parser, curr_properties,
                                                            curr_fts_type))) {
          LOG_WARN("invalid search index mode", K(ret), K(curr_mode));
        } else if (OB_FAIL(append_mode_comment(curr_mode, curr_include_path, curr_exclude_path,
                                               curr_include_types, curr_parser, curr_properties, curr_fts_type,
                                               database_name, tenant_id, schema_checker, source_meta, connection_collation, mode_comments))) {
          LOG_WARN("failed to append search index mode comment", K(ret));
        } else {
          ++mode_count;
          curr_include_path = nullptr;
          curr_exclude_path = nullptr;
          curr_include_types = nullptr;
          curr_parser = nullptr;
          curr_properties = nullptr;
          curr_fts_type = nullptr;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown search index mode type", K(ret), K(i), K(node->type_));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown search index column option list", K(ret), K(options->type_));
  }
  ObSqlString column_nested;
  if (OB_FAIL(ret)) {
  } else if (!source_meta.is_json() && nested_path != nullptr) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "NESTED_PATHS on a non-JSON search index column");
  } else if (nested_path != nullptr
             && OB_FAIL(ObSearchIndexConfigFilter::print_filter_option(nested_path, connection_collation, column_nested))) {
    LOG_WARN("failed to print search index nested paths", K(ret));
  } else if (OB_FAIL(comment.append_fmt("SEARCH_INDEX_CONFIG=1:%ld:%ld:",
                                         mode_count, column_nested.length()))) {
    LOG_WARN("failed to append search index header", K(ret));
  } else if (OB_FAIL(comment.append(column_nested.string()))) {
    LOG_WARN("failed to append search index nested paths", K(ret));
  } else if (OB_FAIL(comment.append(mode_comments.string()))) {
    LOG_WARN("failed to append search index mode comments", K(ret));
  } else if (OB_FAIL(ObSearchIndexConfig::validate(comment.string()))) {
    LOG_WARN("failed to validate search index comment", K(ret));
  }
  if (OB_SUCC(ret) && tenant_data_version < DATA_VERSION_4_6_3_1) {
    ObSearchIndexConfig config;
    if (OB_FAIL(config.decode(comment.string()))) {
      LOG_WARN("failed to decode search index comment for legacy encoding", K(ret));
    } else if (OB_FAIL(config.encode(comment, tenant_data_version))) {
      LOG_WARN("failed to encode search index comment for tenant data version", K(ret), K(tenant_data_version));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
