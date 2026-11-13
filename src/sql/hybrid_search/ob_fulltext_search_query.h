/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_QUERY_H_
#define _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_QUERY_H_

#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_config.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{
class ObFullTextQueryNode;

// Search Text path and metadata selected during index resolution. Query fields, type,
// index ID and name remain on the query node. Path and parser strings are owned by
// the plan allocator; copying this struct only copies string views.
struct ObSearchIndexTextInfo
{
  ObSearchIndexTextInfo()
    : main_table_id_(OB_INVALID_ID), def_table_id_(OB_INVALID_ID),
      column_idx_(-1), index_type_(share::schema::OB_FTS_INDEX_TYPE_INVALID) {}
  bool has_search_index_fulltext() const { return def_table_id_ != OB_INVALID_ID; }
  common::ObString path_; // Logical JSON path used for index matching and execution metadata.
  uint64_t main_table_id_;
  uint64_t def_table_id_;
  int64_t column_idx_;
  share::schema::ObFTSIndexType index_type_;
  // Effective parser/version and fixed properties, resolved through the shared FTS helpers.
  common::ObString parser_name_;
  common::ObString parser_properties_;
  TO_STRING_KV(K_(path), K_(main_table_id), K_(def_table_id), K_(column_idx),
      K_(index_type), K_(parser_name), K_(parser_properties));
};

// Single-field binding input/output. search_info_.path_ is the input path;
// successful resolution replaces the binding. Strings borrow the plan allocator's storage.
struct ObFullTextIndexBinding
{
  ObFullTextIndexBinding()
    : inv_idx_tid_(OB_INVALID_ID), doc_id_idx_tid_(OB_INVALID_ID), is_async_(false) {}
  uint64_t inv_idx_tid_;
  uint64_t doc_id_idx_tid_;
  common::ObString index_name_;
  bool is_async_;
  ObSearchIndexTextInfo search_info_;
};

class ObSearchTextBinder
{
public:
  // Validate query inputs and prepare the path once before enumerating Def/Text candidates.
  // Keep parsed alive while using items, which borrow its member names.
  static int prepare_query(
      const share::schema::ObTableSchema &main_table,
      uint64_t column_id, common::ObJsonPath &parsed,
      common::ObIAllocator &allocator,
      common::ObIArray<share::ObSearchIndexPathEncoder::JsonPathItem> &items,
      bool &path_supported);
  // JSON inputs must pass prepare_query with path_supported=true; SQL strings use empty items.
  // Def must be a readable, visible Search Def and Text one of its Text children.
  // Check one Def/Text pair. An inapplicable candidate succeeds with found_matched_index=false.
  static int check_search_fulltext_index_match_column(
      const share::schema::ObTableSchema &main_table,
      uint64_t column_id, share::schema::ObFTSIndexType query_type,
      const common::ObIArray<share::ObSearchIndexPathEncoder::JsonPathItem> &items,
      const share::schema::ObTableSchema &def,
      const share::schema::ObTableSchema &text,
      common::ObIAllocator &allocator, ObSearchIndexTextInfo &info,
      bool &found_matched_index);
};

struct ObDSLFullTextMultiFieldQueryParam
{
public:
  static constexpr ObMatchFieldsType DEFAULT_FIELD_TYPE = MATCH_BEST_FIELDS;
  ObDSLFullTextMultiFieldQueryParam(ObIAllocator &alloc);
  virtual ~ObDSLFullTextMultiFieldQueryParam() {}
  bool is_valid() const;
  int deep_copy(ObIRawExprCopier &expr_copier, ObIAllocator &allocator,
                ObDSLFullTextMultiFieldQueryParam &dst) const;
  int collect_exprs(ObIArray<ObRawExpr *> &exprs) const;
  int64_t field_count() const { return fields_.count(); }
  TO_STRING_KV(K_(fields), K_(field_paths), K_(field_boosts), KPC_(minimum_should_match), KPC_(operator), KPC_(field_type));
  ObFixedArray<ObColumnRefRawExpr *, ObIAllocator> fields_;
  ObFixedArray<ObString, ObIAllocator> field_paths_;
  ObFixedArray<ObConstRawExpr *, ObIAllocator> field_boosts_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
  ObConstRawExpr *field_type_;
};

class ObDSLFullTextQuery : public ObDSLQuery
{
  friend class ObDSLResolver;
  friend class ObJoinOrder;
public:
  static constexpr double DEFAULT_BOOST = 1.0;
  static constexpr ObMatchOperator DEFAULT_OPERATOR = MATCH_OPERATOR_OR;
  static constexpr int32_t DEFAULT_MINIMUM_SHOULD_MATCH = 1;
public:
  virtual ~ObDSLFullTextQuery() {}
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const;
  bool is_valid() const { return nullptr != query_ && nullptr != boost_; }
  static int resolve_doc_rowkey_index(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      uint64_t &doc_id_idx_tid);
  static int resolve_fields_index(
      const ObIArray<ObColumnRefRawExpr *> &fields,
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &allocator,
      ObIArray<uint64_t> &inv_idx_tids,
      ObIArray<ObString> &index_names,
      ObIArray<ObSearchIndexTextInfo> &search_infos,
      uint64_t &doc_id_idx_tid,
      bool &is_async);
  // Bind one field/path using the same candidate ordering for all FULLTEXT queries.
  static int resolve_single_field_index(
      const ObColumnRefRawExpr &field,
      const bool need_pos_list,
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &allocator,
      ObFullTextIndexBinding &binding);
  INHERIT_TO_STRING_KV("ObDSLQuery", ObDSLQuery, KPC_(query), KPC_(boost));
protected:
  static int resolve_search_text_index(
      const ObTableSchema &main_table,
      const ObSqlSchemaGuard &guard,
      const ObTableSchema &def,
      uint64_t column_id,
      const ObIArray<share::ObSearchIndexPathEncoder::JsonPathItem> &items,
      ObIAllocator &allocator,
      const bool need_pos_list,
      const ObTableSchema *&selected_text,
      ObSearchIndexTextInfo &info);
  ObDSLFullTextQuery(ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLQuery(query_type, outer_query_type, parent_query), query_(nullptr), path_() {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLFullTextQuery &dst) const;
  ObRawExpr *query_;
  ObString path_; // Logical JSON path for Search FULLTEXT; empty for ordinary FTS.
};

class ObDSLMatchQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
  friend class ObJoinOrder;
public:
  virtual ~ObDSLMatchQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMatchQuery *&match_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery,
      KPC_(field), KPC_(minimum_should_match), KPC_(operator));
private:
  ObDSLMatchQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLFullTextQuery(QUERY_ITEM_MATCH, outer_query_type, parent_query),
      field_(nullptr),
      minimum_should_match_(nullptr),
      operator_(nullptr)
  {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchQuery &dst) const;
  ObColumnRefRawExpr *field_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
};

class ObDSLMultiMatchQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  virtual ~ObDSLMultiMatchQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMultiMatchQuery *&multi_match_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, K_(fields_param));
private:
  ObDSLMultiMatchQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query, ObIAllocator &alloc)
    : ObDSLFullTextQuery(QUERY_ITEM_MULTI_MATCH, outer_query_type, parent_query),
      fields_param_(alloc) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMultiMatchQuery &dst) const;
  ObDSLFullTextMultiFieldQueryParam fields_param_;
};

class ObDSLMatchPhraseQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
  friend class ObJoinOrder;
public:
  static constexpr int32_t DEFAULT_SLOP = 0;
  virtual ~ObDSLMatchPhraseQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMatchPhraseQuery *&match_phrase_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, KPC_(field), KPC_(slop));
private:
  ObDSLMatchPhraseQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLFullTextQuery(QUERY_ITEM_MATCH_PHRASE, outer_query_type, parent_query) {}
  int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchPhraseQuery &dst) const;
  ObColumnRefRawExpr *field_;
  ObConstRawExpr *slop_;
};

class ObDSLQueryStringQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  virtual ~ObDSLQueryStringQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLQueryStringQuery *&query_string_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      const ObIArray<uint64_t> &valid_index_ids,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, K_(fields_param));
private:
  ObDSLQueryStringQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query, ObIAllocator &alloc)
    : ObDSLFullTextQuery(QUERY_ITEM_QUERY_STRING, outer_query_type, parent_query),
      fields_param_(alloc) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLQueryStringQuery &dst) const;
private:
  ObDSLFullTextMultiFieldQueryParam fields_param_;
};

} // namespace sql

} // namespace oceanbase

#endif
