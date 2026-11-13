/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/hybrid_search/ob_fulltext_search_query.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_search_index_query_range_utils.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;

static int resolve_parser_config(const ObSearchIndexModeConfig &mode, uint64_t tenant_id,
                                 ObCollationType source_cs, ObIAllocator &allocator,
                                 ObString &name, ObString &properties)
{
  int ret = OB_SUCCESS;
  storage::ObFTParser parser;
  char name_buf[OB_PLUGIN_NAME_LENGTH] = {};
  OZ(parser.parse_from_str(mode.parser_name_.ptr(), mode.parser_name_.length()));
  OZ(parser.serialize_to_str(name_buf, sizeof(name_buf)));
  OZ(ob_write_string(allocator, ObString::make_string(name_buf), name));
  if (OB_SUCC(ret)) {
    if (parser.is_analyzer_parser()) {
      OZ(ob_write_string(allocator, mode.parser_properties_, properties));
    } else {
      // Use the same fixed defaults as the generator, including partial properties.
      // Never inherit a tenant's mutable fulltext defaults into a Search Text plan.
      storage::ObFTParserJsonProps props;
      OZ(props.init());
      OZ(props.parse_from_valid_str(mode.parser_properties_));
      OZ(props.rebuild_props_for_ddl(name, source_cs, false, tenant_id));
      OZ(props.to_format_json(allocator, properties));
    }
  }
  return ret;
}

int ObSearchTextBinder::prepare_query(
    const ObTableSchema &main_table, uint64_t column_id,
    ObJsonPath &parsed, ObIAllocator &allocator,
    ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &items, bool &path_supported)
{
  int ret = OB_SUCCESS;
  path_supported = false;
  const ObString &path = parsed.get_path_string();
  const ObColumnSchemaV2 *source = main_table.get_column_schema(column_id);
  if (OB_ISNULL(source) || !ob_is_json(source->get_data_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Search FULLTEXT source column", K(ret), K(column_id), KPC(source));
  } else if (OB_FAIL(parsed.parse_path())) {
    if (ret != OB_ALLOCATE_MEMORY_FAILED) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("invalid Search FULLTEXT path", K(ret), K(path));
  } else if (OB_FAIL(ObSearchIndexQueryRangeUtils::build_json_path_items(
      allocator, parsed, items, path_supported))) {
    LOG_WARN("failed to build Search FULLTEXT path", K(ret), K(path));
  }
  return ret;
}

int ObSearchTextBinder::check_search_fulltext_index_match_column(
    const ObTableSchema &main_table, uint64_t column_id,
    ObFTSIndexType query_type,
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &items,
    const ObTableSchema &def, const ObTableSchema &text,
    ObIAllocator &allocator, ObSearchIndexTextInfo &info,
    bool &found_matched_index)
{
  int ret = OB_SUCCESS;
  found_matched_index = false;
  info = ObSearchIndexTextInfo();
  const ObColumnSchemaV2 *column = nullptr;
  if (def.get_data_table_id() != main_table.get_table_id()
      || def.get_tenant_id() != main_table.get_tenant_id()
      || !text.can_read_index() || !text.is_index_visible()
      || text.get_data_table_id() != def.get_table_id()
      || text.get_tenant_id() != def.get_tenant_id()) {
    // This Def/Text pair is not a candidate for the source table.
  } else if (nullptr == (column = def.get_column_schema(column_id))
      || column->get_index_position() <= 0) {
  } else if (query_type == OB_FTS_INDEX_TYPE_PHRASE_MATCH
      && !is_async_search_text_match_phrase_index(text.get_index_type())) {
  } else {
    const ObFTSIndexType index_type = is_async_search_text_match_index(text.get_index_type())
        ? OB_FTS_INDEX_TYPE_MATCH : OB_FTS_INDEX_TYPE_PHRASE_MATCH;
    ObSearchIndexConfig config;
    if (OB_FAIL(config.decode(column->get_comment_str()))) {
      LOG_WARN("failed to decode Search FULLTEXT configuration", K(ret));
    } else if (OB_FAIL(config.validate())) {
      LOG_WARN("invalid Search FULLTEXT configuration", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !found_matched_index && i < config.get_modes().count(); ++i) {
      const ObSearchIndexModeConfig &mode = config.get_modes().at(i);
      if (index_type == OB_FTS_INDEX_TYPE_MATCH
          ? mode.mode_ != ObSearchIndexModeConfig::FULLTEXT_MATCH
          : mode.mode_ != ObSearchIndexModeConfig::FULLTEXT_PHRASE) {
        continue;
      }
      bool covered = false;
      ObSearchIndexConfigFilter filter(main_table.get_tenant_id());
      ObSearchIndexTextInfo matched;
      if (OB_FAIL(filter.init_from_comment(config.get_comment(), i))) {
        LOG_WARN("failed to initialize Search FULLTEXT filter", K(ret));
      } else if (OB_FAIL(filter.is_indexed(items, ObJsonNodeType::J_STRING, covered))) {
        LOG_WARN("failed to check Search FULLTEXT path coverage", K(ret), K(items));
      } else if (!covered) {
      } else if (OB_FAIL(resolve_parser_config(mode, main_table.get_tenant_id(),
          column->get_collation_type(), allocator,
          matched.parser_name_, matched.parser_properties_))) {
        LOG_WARN("invalid Search FULLTEXT parser configuration", K(ret), K(mode));
      } else if (OB_FAIL(main_table.get_store_column_idx(column_id, matched.column_idx_))) {
        LOG_WARN("failed to get Search FULLTEXT source column position", K(ret), K(column_id));
      } else {
        matched.main_table_id_ = main_table.get_table_id();
        matched.def_table_id_ = def.get_table_id();
        matched.index_type_ = index_type;
        info = matched;
        found_matched_index = true;
      }
    }
  }
  return ret;
}

ObDSLFullTextMultiFieldQueryParam::ObDSLFullTextMultiFieldQueryParam(ObIAllocator &alloc)
  : fields_(&alloc),
    field_paths_(&alloc),
    field_boosts_(&alloc),
    minimum_should_match_(nullptr),
    operator_(nullptr),
    field_type_(nullptr)
{}

bool ObDSLFullTextMultiFieldQueryParam::is_valid() const
{
  return nullptr != minimum_should_match_
      && nullptr != operator_
      && nullptr != field_type_
      && fields_.count() == field_boosts_.count()
      && fields_.count() == field_paths_.count()
      && fields_.count() > 0;
}

int ObDSLFullTextMultiFieldQueryParam::deep_copy(
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextMultiFieldQueryParam &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *minimum_should_match_expr = nullptr;
  ObRawExpr *operator_expr = nullptr;
  ObRawExpr *field_type_expr = nullptr;
  const int64_t field_count = fields_.count();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid multi field query param", K(ret));
  } else if (OB_FAIL(expr_copier.copy(minimum_should_match_, minimum_should_match_expr))) {
    LOG_WARN("fail to copy minimum should match expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(operator_, operator_expr))) {
    LOG_WARN("fail to copy operator expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_type_, field_type_expr))) {
    LOG_WARN("fail to copy field type expr", K(ret));
  } else if (OB_FAIL(dst.fields_.reserve(field_count))) {
    LOG_WARN("fail to reserve fields", K(ret));
  } else if (OB_FAIL(dst.field_paths_.reserve(field_count))) {
    LOG_WARN("fail to reserve field paths", K(ret));
  } else if (OB_FAIL(dst.field_boosts_.reserve(field_count))) {
    LOG_WARN("fail to reserve field boosts", K(ret));
  } else {
    dst.minimum_should_match_ = static_cast<ObConstRawExpr*>(minimum_should_match_expr);
    dst.operator_ = static_cast<ObConstRawExpr*>(operator_expr);
    dst.field_type_ = static_cast<ObConstRawExpr*>(field_type_expr);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < field_count; ++i) {
    ObRawExpr *field_expr = nullptr;
    ObRawExpr *boost_expr = nullptr;
    ObString path;
    if (OB_FAIL(ob_write_string(allocator, field_paths_.at(i), path))) {
      LOG_WARN("fail to copy field path", K(ret));
    } else if (OB_FAIL(dst.field_paths_.push_back(path))) {
      LOG_WARN("fail to append field path", K(ret));
    } else if (OB_FAIL(expr_copier.copy(fields_.at(i), field_expr))) {
      LOG_WARN("fail to copy field expr", K(ret));
    } else if (OB_FAIL(expr_copier.copy(field_boosts_.at(i), boost_expr))) {
      LOG_WARN("fail to copy field boost expr", K(ret));
    } else if (OB_FAIL(dst.fields_.push_back(static_cast<ObColumnRefRawExpr*>(field_expr)))) {
      LOG_WARN("fail to push back field expr", K(ret));
    } else if (OB_FAIL(dst.field_boosts_.push_back(static_cast<ObConstRawExpr*>(boost_expr)))) {
      LOG_WARN("fail to push back field boost expr", K(ret));
    }
  }
  return ret;
}

int ObDSLFullTextMultiFieldQueryParam::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exprs.push_back(minimum_should_match_))) {
    LOG_WARN("fail to push back minimum should match expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(operator_))) {
    LOG_WARN("fail to push back operator expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_type_))) {
    LOG_WARN("fail to push back field type expr", K(ret));
  } else if (OB_FAIL(append(exprs, fields_))) {
    LOG_WARN("fail to append fields", K(ret));
  } else if (OB_FAIL(append(exprs, field_boosts_))) {
    LOG_WARN("fail to append field boosts", K(ret));
  }
  return ret;
}

int ObDSLFullTextQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  return OB_NOT_IMPLEMENT;
}

int ObDSLFullTextQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLFullTextQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *query_expr = nullptr;
  ObRawExpr *boost_expr = nullptr;
  if (OB_FAIL(expr_copier.copy(query_, query_expr))) {
    LOG_WARN("fail to copy query expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(boost_, boost_expr))) {
    LOG_WARN("fail to copy boost expr", K(ret));
  } else {
    dst.query_ = query_expr;
    dst.boost_ = static_cast<ObConstRawExpr*>(boost_expr);
  }
  return ret;
}

int ObDSLFullTextQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exprs.push_back(query_))) {
    LOG_WARN("fail to push back query expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(boost_))) {
    LOG_WARN("fail to push back boost expr", K(ret));
  }
  return ret;
}

int ObDSLFullTextQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  return OB_NOT_IMPLEMENT;
}

int ObDSLFullTextQuery::resolve_search_text_index(
    const ObTableSchema &main_table,
    const ObSqlSchemaGuard &guard,
    const ObTableSchema &def,
    uint64_t column_id,
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &items,
    ObIAllocator &alloc,
    const bool need_pos_list,
    const ObTableSchema *&selected_text,
    ObSearchIndexTextInfo &info)
{
  int ret = OB_SUCCESS;
  const ObFTSIndexType query_type = need_pos_list
      ? OB_FTS_INDEX_TYPE_PHRASE_MATCH : OB_FTS_INDEX_TYPE_MATCH;
  selected_text = nullptr;
  ObSEArray<ObAuxTableMetaInfo, 4> children;
  if (OB_FAIL(def.get_simple_index_infos(children))) {
    LOG_WARN("failed to get search index child infos", K(ret));
  }
  // Keep PHRASE as a fallback while looking for an exact MATCH mode in this Def.
  for (int64_t i = 0; OB_SUCC(ret) && i < children.count()
      && (selected_text == nullptr || info.index_type_ != query_type); ++i) {
    if (need_pos_list
        ? is_async_search_text_match_phrase_index(children.at(i).index_type_)
        : is_async_search_text_index(children.at(i).index_type_)) {
      const ObTableSchema *text = nullptr;
      ObSearchIndexTextInfo candidate;
      bool matched = false;
      if (OB_FAIL(guard.get_table_schema(children.at(i).table_id_, text))) {
        LOG_WARN("failed to get search text index schema", K(ret));
      } else if (OB_ISNULL(text)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null search text index schema", K(ret));
      } else if (OB_FAIL(ObSearchTextBinder::check_search_fulltext_index_match_column(
          main_table, column_id, query_type, items, def, *text, alloc, candidate, matched))) {
        LOG_WARN("failed to check search text index with field", K(ret));
      } else if (matched) {
        info = candidate;
        selected_text = text;
      }
    }
  }
  return ret;
}

int ObDSLFullTextQuery::resolve_fields_index(
    const ObIArray<ObColumnRefRawExpr *> &fields,
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &allocator,
    ObIArray<uint64_t> &inv_idx_tids,
    ObIArray<ObString> &index_names,
    ObIArray<ObSearchIndexTextInfo> &search_infos,
    uint64_t &doc_id_idx_tid,
    bool &is_async)
{
  int ret = OB_SUCCESS;
  ObString parser_name;
  ObString parser_properties;
  for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
    ObFullTextIndexBinding binding;
    binding.search_info_.path_ = search_infos.at(i).path_;
    const ObSearchIndexTextInfo &info = binding.search_info_;
    const ObTableSchema *index_schema = nullptr;
    ObString name;
    ObString properties;
    if (OB_FAIL(resolve_single_field_index(
        *fields.at(i), false, main_table_schema, schema_guard,
        valid_index_ids, allocator, binding))) {
      LOG_WARN("failed to resolve fulltext index with field", K(ret), K(i));
    } else if (i > 0 && binding.is_async_ != is_async) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "Mixing synchronous and asynchronous FULLTEXT in a multi-field query");
      LOG_WARN("multi-field query mixes fulltext index groups", K(ret), K(i), K(binding.is_async_), K(info));
    } else if (i > 0 && fields.at(i)->get_result_type().is_json() != fields.at(0)->get_result_type().is_json()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Mixing JSON and non-JSON fields in a FULLTEXT multi-field query");
      LOG_WARN("fulltext query mixes source types", K(ret), K(i));
    } else if (fields.at(i)->get_collation_type() != fields.at(0)->get_collation_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, containing different collation types");
      LOG_WARN("fulltext fields have different charset or collation", K(ret), K(i));
    } else if (info.has_search_index_fulltext()) {
      name = info.parser_name_;
      properties = info.parser_properties_;
    } else if (!info.has_search_index_fulltext()) {
      if (OB_FAIL(schema_guard.get_table_schema(binding.inv_idx_tid_, index_schema))) {
        LOG_WARN("failed to get bound fulltext schema", K(ret), K(i));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null bound fulltext schema", K(ret), K(i));
      } else {
        name = index_schema->get_parser_name_str();
        properties = index_schema->get_parser_property_str();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == i) {
      is_async = binding.is_async_;
      doc_id_idx_tid = binding.doc_id_idx_tid_;
      parser_name = name;
      parser_properties = properties;
    } else if (name != parser_name || properties != parser_properties) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, indexed with different parsers");
      LOG_WARN("bound fulltext fields have incompatible parsers", K(ret), K(i),
          K(name), K(properties), K(parser_name), K(parser_properties));
    }
    if (OB_SUCC(ret)) {
      // Names describe the logical indexes used; execution bindings remain per field.
      if (OB_FAIL(add_var_to_array_no_dup(index_names, binding.index_name_))) {
        LOG_WARN("failed to append fulltext index name", K(ret), K(i));
      } else {
        inv_idx_tids.at(i) = binding.inv_idx_tid_;
        search_infos.at(i) = binding.search_info_;
      }
    }
  }
  return ret;
}

int ObDSLFullTextQuery::resolve_single_field_index(
    const ObColumnRefRawExpr &field,
    const bool need_pos_list,
    const ObTableSchema &main_table,
    const ObSqlSchemaGuard &guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextIndexBinding &binding)
{
  int ret = OB_SUCCESS;
  const ObString &path = binding.search_info_.path_;
  ObFullTextIndexBinding result;
  const bool is_json = field.get_result_type().is_json();
  const ObFTSIndexType query_type = need_pos_list
      ? OB_FTS_INDEX_TYPE_PHRASE_MATCH : OB_FTS_INDEX_TYPE_MATCH;
  const ObTableSchema *selected_index = nullptr;
  const ObTableSchema *selected_def = nullptr;
  ObSearchIndexTextInfo &info = result.search_info_;
  bool stop_search = false;
  bool path_supported = true;
  ObJsonPath parsed(path, &alloc);
  // String columns match the root path, represented by an empty item list.
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> items;
  ColumnReferenceSet column_set;
  if (is_json && OB_FAIL(ObSearchTextBinder::prepare_query(
      main_table, field.get_column_id(), parsed, alloc, items, path_supported))) {
    LOG_WARN("failed to prepare Search FULLTEXT query", K(ret), K(path));
  } else if (OB_FAIL(column_set.add_member(field.get_column_id()))) {
    LOG_WARN("failed to add column to column set", K(ret));
  }
  // Search Def and ordinary FTS share the effective candidate order.
  for (int64_t i = 0; OB_SUCC(ret) && path_supported && !stop_search
      && i < valid_index_ids.count(); ++i) {
    const ObTableSchema *index = nullptr;
    const uint64_t index_id = valid_index_ids.at(i);
    if (OB_FAIL(guard.get_table_schema(index_id, index))) {
      LOG_WARN("failed to get fulltext candidate", K(ret), K(index_id));
    } else if (OB_ISNULL(index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null fulltext candidate", K(ret), K(index_id));
    } else if (!index->can_read_index() || !index->is_index_visible()) {
      // Skip unavailable candidates.
    } else if (index->is_async_search_def_index()) {
      const ObTableSchema *text = nullptr;
      ObSearchIndexTextInfo candidate;
      if (OB_FAIL(resolve_search_text_index(main_table, guard, *index,
                                            field.get_column_id(), items,
                                            alloc, need_pos_list, text, candidate))) {
        LOG_WARN("failed to resolve search text index", K(ret));
      } else if (text != nullptr) {
        info = candidate;
        selected_def = index;
        selected_index = text;
        // String columns use the first matching Def, including a PHRASE fallback.
        stop_search = !is_json || info.index_type_ == query_type;
      }
    } else if (!is_json && path.empty()
        && (index->is_fts_index_aux() || index->is_async_fts_index())) {
      // An explicit path keeps native Search FULLTEXT on its own SQL entry.
      // Ordinary FTS matches source columns and does not check path coverage.
      ObFTSIndexParams params;
      bool matched = false;
      if (need_pos_list && OB_FAIL(index->get_fts_params_from_index_params(params))) {
        LOG_WARN("failed to get fulltext candidate mode", K(ret));
      } else if (need_pos_list && params.fts_index_type_ != OB_FTS_INDEX_TYPE_PHRASE_MATCH) {
        // This candidate cannot serve a phrase query.
      } else if (OB_FAIL(ObTransformUtils::check_fulltext_index_match_column(
          column_set, &main_table, index, matched))) {
        LOG_WARN("failed to check fulltext index match column", K(ret));
      } else if (matched) {
        if (OB_FAIL(resolve_doc_rowkey_index(main_table, guard, result.doc_id_idx_tid_))) {
          if (OB_ERR_FT_COLUMN_NOT_INDEXED == ret) {
            ret = OB_SUCCESS;
            result.doc_id_idx_tid_ = OB_INVALID_ID;
          }
        } else {
          selected_index = index;
          stop_search = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString selected_name;
    if (OB_ISNULL(selected_index)) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("no eligible fulltext candidate", K(ret), K(valid_index_ids), K(path));
    } else if (OB_FAIL((selected_def != nullptr ? selected_def : selected_index)->get_index_name(selected_name))) {
      LOG_WARN("failed to get fulltext index name", K(ret));
    } else if (OB_FAIL(ob_write_string(alloc, selected_name, result.index_name_))) {
      LOG_WARN("failed to copy fulltext index name", K(ret));
    } else {
      if (selected_def != nullptr) {
        info.path_ = is_json ? path : ObString("$");
      }
      result.inv_idx_tid_ = selected_index->get_table_id();
      result.is_async_ = selected_def != nullptr || selected_index->is_async_fts_index();
      binding = result;
    }
  }
  return ret;
}

int ObDSLFullTextQuery::resolve_doc_rowkey_index(
    const ObTableSchema &main_table,
    const ObSqlSchemaGuard &guard,
    uint64_t &doc_id_idx_tid)
{
  int ret = OB_SUCCESS;
  doc_id_idx_tid = OB_INVALID_ID;
  uint64_t doc_id_col_id = OB_INVALID_ID;
  ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
  if (OB_FAIL(main_table.get_docid_col_id(doc_id_col_id))) {
    if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check docid in schema", K(ret));
    }
  } else if (OB_INVALID_ID != doc_id_col_id) {
    if (OB_FAIL(main_table.get_simple_index_infos(index_infos))) {
      LOG_WARN("failed to get index infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      if (is_doc_rowkey_aux(index_infos.at(i).index_type_)) {
        doc_id_idx_tid = index_infos.at(i).table_id_;
        break;
      }
    }
    const ObTableSchema *doc_id_idx_schema = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == doc_id_idx_tid) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("doc rowkey index not found for table with doc id column", K(ret), K(main_table));
    } else if (OB_FAIL(guard.get_table_schema(doc_id_idx_tid, doc_id_idx_schema))) {
      LOG_WARN("failed to get doc_id_idx_table_schema", K(ret));
    } else if (OB_ISNULL(doc_id_idx_schema)) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("unexpected doc_id_idx_table_schema", K(ret));
    } else if (!doc_id_idx_schema->can_read_index() || !doc_id_idx_schema->is_index_visible()) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("doc_id_idx_table_schema is not visible", K(ret), KPC(doc_id_idx_schema));
    }
  }
  return ret;
}

int ObDSLMatchQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMatchQuery *&match_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(match_query = OB_NEWx(ObDSLMatchQuery, &alloc, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMatchQuery", K(ret));
  }
  return ret;
}

int ObDSLMatchQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMatchQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMatchQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create match query", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, path_, tmp_dst->path_))) {
    LOG_WARN("failed to copy fulltext path", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy match query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMatchQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *minimum_should_match_expr = nullptr;
  ObRawExpr *operator_expr = nullptr;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_, field_expr))) {
    LOG_WARN("fail to copy field expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(minimum_should_match_, minimum_should_match_expr))) {
    LOG_WARN("fail to copy minimum should match expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(operator_, operator_expr))) {
    LOG_WARN("fail to copy operator expr", K(ret));
  } else {
    dst.field_ = static_cast<ObColumnRefRawExpr*>(field_expr);
    dst.minimum_should_match_ = static_cast<ObConstRawExpr*>(minimum_should_match_expr);
    dst.operator_ = static_cast<ObConstRawExpr*>(operator_expr);
  }
  return ret;
}

int ObDSLMatchQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_))) {
    LOG_WARN("fail to push back field expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(minimum_should_match_))) {
    LOG_WARN("fail to push back minimum should match expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(operator_))) {
    LOG_WARN("fail to push back operator expr", K(ret));
  }
  return ret;
}

int ObDSLMatchQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMatchQueryNode *match_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid match query", K(ret), KPC(this));
  } else if (OB_ISNULL(match_query_node = OB_NEWx(ObMatchQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMatchQueryNode", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, path_, match_query_node->search_index_fulltext_info_.path_))) {
    LOG_WARN("failed to copy fulltext path", K(ret));
  } else {
    match_query_node->query_text_ = query_;
    match_query_node->boost_ = boost_;
    match_query_node->field_ = field_;
    match_query_node->minimum_should_match_ = minimum_should_match_;
    match_query_node->operator_ = operator_;
    if (OB_FAIL(match_query_node->resolve_index(
        main_table_schema, schema_guard, valid_index_ids, alloc))) {
      LOG_WARN("failed to resolve fulltext index", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != match_query_node) {
      match_query_node->~ObMatchQueryNode();
      alloc.free(match_query_node);
    }
    match_query_node = nullptr;
  } else {
    query_node = match_query_node;
  }
  return ret;
}

bool ObDSLMatchQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid()
      && nullptr != field_
      && nullptr != minimum_should_match_
      && nullptr != operator_;
}

int ObDSLMultiMatchQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMultiMatchQuery *&multi_match_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(multi_match_query = OB_NEWx(ObDSLMultiMatchQuery, &alloc, outer_query_type, parent_query, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMultiMatchQuery", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMultiMatchQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMultiMatchQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create multi match query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy multi match query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMultiMatchQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMultiMatchQuery &dst) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(fields_param_.deep_copy(expr_copier, *dst.fields_param_.fields_.get_allocator(), dst.fields_param_))) {
    LOG_WARN("fail to deep copy fields param", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(fields_param_.collect_exprs(exprs))) {
    LOG_WARN("fail to collect fields param exprs", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMultiMatchQueryNode *multi_match_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid multi match query", K(ret), KPC(this));
  } else if (OB_ISNULL(multi_match_query_node = OB_NEWx(ObMultiMatchQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMultiMatchQueryNode", K(ret));
  } else {
    multi_match_query_node->query_text_ = query_;
    multi_match_query_node->boost_ = boost_;
    multi_match_query_node->minimum_should_match_ = fields_param_.minimum_should_match_;
    multi_match_query_node->operator_ = fields_param_.operator_;
    multi_match_query_node->field_type_ = fields_param_.field_type_;
    multi_match_query_node->fields_.set_allocator(&alloc);
    multi_match_query_node->search_index_fulltext_infos_.set_allocator(&alloc);
    multi_match_query_node->field_boosts_.set_allocator(&alloc);
    multi_match_query_node->inv_idx_tids_.set_allocator(&alloc);
    multi_match_query_node->index_infos_.set_allocator(&alloc);
    multi_match_query_node->index_names_.set_allocator(&alloc);
    if (OB_FAIL(multi_match_query_node->fields_.assign(fields_param_.fields_))) {
      LOG_WARN("fail to assign fields", K(ret));
    } else if (OB_FAIL(multi_match_query_node->search_index_fulltext_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to allocate field bindings", K(ret));
    } else if (OB_FAIL(multi_match_query_node->field_boosts_.assign(fields_param_.field_boosts_))) {
      LOG_WARN("fail to assign field boosts", K(ret));
    } else if (OB_FAIL(multi_match_query_node->inv_idx_tids_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate inv idx tids", K(ret));
    } else if (OB_FAIL(multi_match_query_node->index_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index infos", K(ret));
    } else if (OB_FAIL(multi_match_query_node->index_names_.reserve(fields_param_.field_count()))) {
      LOG_WARN("fail to reserve index names", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields_param_.field_count(); ++i) {
      multi_match_query_node->inv_idx_tids_.at(i) = OB_INVALID_ID;
      if (OB_FAIL(ob_write_string(alloc, fields_param_.field_paths_.at(i),
                                  multi_match_query_node->search_index_fulltext_infos_.at(i).path_))) {
        LOG_WARN("failed to copy fulltext field path", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(multi_match_query_node->resolve_index(
        main_table_schema, schema_guard, valid_index_ids, alloc))) {
      LOG_WARN("failed to resolve fulltext index", K(ret));
    }
    if (OB_FAIL(ret)) {
      multi_match_query_node->~ObMultiMatchQueryNode();
      alloc.free(multi_match_query_node);
      multi_match_query_node = nullptr;
    } else {
      query_node = multi_match_query_node;
    }
  }
  return ret;
}

bool ObDSLMultiMatchQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid() && fields_param_.is_valid();
}

int ObDSLMatchPhraseQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMatchPhraseQuery *&match_phrase_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(match_phrase_query = OB_NEWx(ObDSLMatchPhraseQuery, &alloc, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMatchPhraseQuery", K(ret));
  }
  return ret;
}

int ObDSLMatchPhraseQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMatchPhraseQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMatchPhraseQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create match phrase query", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, path_, tmp_dst->path_))) {
    LOG_WARN("failed to copy fulltext path", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy match phrase query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMatchPhraseQuery::inner_deep_copy(
    ObIRawExprCopier &expr_copier,
    ObDSLMatchPhraseQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *slop_expr = nullptr;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_, field_expr))) {
    LOG_WARN("fail to copy field expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(slop_, slop_expr))) {
    LOG_WARN("fail to copy slop expr", K(ret));
  } else {
    dst.field_ = static_cast<ObColumnRefRawExpr*>(field_expr);
    dst.slop_ = static_cast<ObConstRawExpr*>(slop_expr);
  }
  return ret;
}

int ObDSLMatchPhraseQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_))) {
    LOG_WARN("fail to push back field expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(slop_))) {
    LOG_WARN("fail to push back slop expr", K(ret));
  }
  return ret;
}

int ObDSLMatchPhraseQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMatchPhraseQueryNode *match_phrase_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid match phrase query", K(ret), KPC(this));
  } else if (OB_ISNULL(match_phrase_query_node = OB_NEWx(ObMatchPhraseQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMatchPhraseQueryNode", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, path_, match_phrase_query_node->search_index_fulltext_info_.path_))) {
    LOG_WARN("failed to copy fulltext path", K(ret));
  } else {
    match_phrase_query_node->query_text_ = query_;
    match_phrase_query_node->boost_ = boost_;
    match_phrase_query_node->field_ = field_;
    match_phrase_query_node->slop_ = slop_;
    if (OB_FAIL(match_phrase_query_node->resolve_index(
        main_table_schema, schema_guard, valid_index_ids, alloc))) {
      LOG_WARN("failed to resolve fulltext index", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != match_phrase_query_node) {
      match_phrase_query_node->~ObMatchPhraseQueryNode();
      alloc.free(match_phrase_query_node);
    }
    match_phrase_query_node = nullptr;
  } else {
    query_node = match_phrase_query_node;
  }
  return ret;
}

bool ObDSLMatchPhraseQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid()
      && nullptr != field_
      && nullptr != slop_;
}

int ObDSLQueryStringQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLQueryStringQuery *&query_string_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_string_query = OB_NEWx(ObDSLQueryStringQuery, &alloc, outer_query_type, parent_query, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLQueryStringQuery", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLQueryStringQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLQueryStringQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create query string query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy query string query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLQueryStringQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLQueryStringQuery &dst) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(fields_param_.deep_copy(expr_copier, *dst.fields_param_.fields_.get_allocator(), dst.fields_param_))) {
    LOG_WARN("fail to deep copy fields param", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(fields_param_.collect_exprs(exprs))) {
    LOG_WARN("fail to collect fields param exprs", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    const ObIArray<uint64_t> &valid_index_ids,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObQueryStringQueryNode *query_string_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid query string query", K(ret), KPC(this));
  } else if (OB_ISNULL(query_string_query_node = OB_NEWx(ObQueryStringQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObQueryStringQueryNode", K(ret));
  } else {
    query_string_query_node->query_text_ = query_;
    query_string_query_node->boost_ = boost_;
    query_string_query_node->minimum_should_match_ = fields_param_.minimum_should_match_;
    query_string_query_node->default_operator_ = fields_param_.operator_;
    query_string_query_node->field_type_ = fields_param_.field_type_;
    query_string_query_node->fields_.set_allocator(&alloc);
    query_string_query_node->search_index_fulltext_infos_.set_allocator(&alloc);
    query_string_query_node->field_boosts_.set_allocator(&alloc);
    query_string_query_node->inv_idx_tids_.set_allocator(&alloc);
    query_string_query_node->index_infos_.set_allocator(&alloc);
    query_string_query_node->index_names_.set_allocator(&alloc);
    if (OB_FAIL(query_string_query_node->fields_.assign(fields_param_.fields_))) {
      LOG_WARN("fail to assign fields", K(ret));
    } else if (OB_FAIL(query_string_query_node->search_index_fulltext_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to allocate field bindings", K(ret));
    } else if (OB_FAIL(query_string_query_node->field_boosts_.assign(fields_param_.field_boosts_))) {
      LOG_WARN("fail to assign field boosts", K(ret));
    } else if (OB_FAIL(query_string_query_node->inv_idx_tids_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate inv idx tids", K(ret));
    } else if (OB_FAIL(query_string_query_node->index_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index infos", K(ret));
    } else if (OB_FAIL(query_string_query_node->index_names_.reserve(fields_param_.field_count()))) {
      LOG_WARN("fail to reserve index names", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields_param_.field_count(); ++i) {
      query_string_query_node->inv_idx_tids_.at(i) = OB_INVALID_ID;
      if (OB_FAIL(ob_write_string(alloc, fields_param_.field_paths_.at(i),
                                  query_string_query_node->search_index_fulltext_infos_.at(i).path_))) {
        LOG_WARN("failed to copy fulltext field path", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(query_string_query_node->resolve_index(
        main_table_schema, schema_guard, valid_index_ids, alloc))) {
      LOG_WARN("failed to resolve fulltext index", K(ret));
    }
    if (OB_FAIL(ret)) {
      query_string_query_node->~ObQueryStringQueryNode();
      alloc.free(query_string_query_node);
      query_string_query_node = nullptr;
    } else {
      query_node = query_string_query_node;
    }
  }
  return ret;
}

bool ObDSLQueryStringQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid() && fields_param_.is_valid();
}

} // namespace sql
} // namespace oceanbase
