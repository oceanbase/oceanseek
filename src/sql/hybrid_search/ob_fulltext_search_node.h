/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_NODE_H_
#define _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_NODE_H_

#include "sql/hybrid_search/ob_hybrid_search_node.h"

namespace oceanbase
{
namespace sql
{

struct ObTextRetrievalIndexInfo
{
public:
  ObTextRetrievalIndexInfo();
  virtual ~ObTextRetrievalIndexInfo() {}
  int generate_exprs(
      const uint64_t inv_idx_tid,
      const bool doc_id_as_domain_id,
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx,
      const bool is_async = false);
  int collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const;
  bool is_valid() const;
  TO_STRING_KV(
      KPC_(token_column), KPC_(token_cnt_column), KPC_(domain_id_column), KPC_(doc_length_column),
      KPC_(related_doc_cnt), KPC_(total_doc_cnt), KPC_(doc_token_cnt), KPC_(avg_doc_token_cnt),
      K_(relevance_expr), KPC_(async_related_doc_cnt), KPC_(async_total_doc_cnt), K_(generated));
private:
  int generate_column_exprs(
      const uint64_t inv_idx_tid,
      const bool doc_id_as_domain_id,
      const TableItem &table_item,
      ObRawExprFactory &expr_factory,
      ObSQLSessionInfo &session_info,
      ObSqlSchemaGuard &schema_guard);
  int generate_calc_exprs(
      ObRawExprFactory &expr_factory,
      ObSQLSessionInfo &session_info,
      const bool is_async = false);
public:
  ObColumnRefRawExpr *token_column_;
  ObColumnRefRawExpr *token_cnt_column_;
  ObColumnRefRawExpr *domain_id_column_;
  ObColumnRefRawExpr *doc_length_column_;
  ObColumnRefRawExpr *pos_list_column_;
  ObAggFunRawExpr *related_doc_cnt_;  // count(token_cnt_column)
  ObAggFunRawExpr *total_doc_cnt_;  // count(doc_id_column) or count(data_table_rows)
  ObAggFunRawExpr *doc_token_cnt_;  // TODO: replace with doc_length_column_ ASAP
  ObRawExpr *avg_doc_token_cnt_; // avg(token_cnt_column) group by doc_id, average doc length
  ObRawExpr *relevance_expr_; // relevace calculation expr(BM25)
  // async fulltext index: BM25 global stats are filled by storage at runtime via pseudo
  // columns instead of aggregation scans
  ObOpPseudoColumnRawExpr *async_related_doc_cnt_;  // document frequency, pseudo column
  ObOpPseudoColumnRawExpr *async_total_doc_cnt_;  // total document count, pseudo column
  bool generated_;
};

class ObFullTextQueryNode : public ObHybridSearchNodeBase
{
public:
  enum QueryType : uint8_t
  {
    OB_FULLTEXT_QUERY_TYPE_MATCH,
    OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH,
    OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE,
    OB_FULLTEXT_QUERY_TYPE_QUERY_STRING,
    OB_FULLTEXT_QUERY_TYPE_MAX
  };
public:
  ObFullTextQueryNode(common::ObIAllocator &allocator, const QueryType query_type = QueryType::OB_FULLTEXT_QUERY_TYPE_MAX);
  virtual ~ObFullTextQueryNode() {}
  bool need_score() const { return nullptr != score_project_expr_; }
  bool need_pos_list() const { return OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE == query_type_; }
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int collect_exprs(
      ObIArray<ObRawExpr *> &query_exprs,
      ObIArray<ObRawExpr *> &score_exprs) const override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  QueryType get_query_type() const { return query_type_; }
  bool is_async() const { return is_async_; }
  virtual const ObAsyncIndexDeleteInfo *get_async_delete_info() const override
  {
    return is_async_ ? &async_delete_info_ : nullptr;
  }
  int resolve_async_delete_index(const share::schema::ObTableSchema &main_table_schema)
  {
    return async_delete_info_.resolve_delete_index(main_table_schema);
  }
  VIRTUAL_TO_STRING_KV(K_(query_type), KPC_(query_text), KPC_(score_project_expr), KPC(boost_),
      K_(is_async), K_(async_delete_info));
public:
  QueryType query_type_;
  uint64_t doc_id_idx_tid_;
  ObRawExpr *query_text_;
  ObRawExpr *score_project_expr_;
  // async fulltext index members
  bool is_async_;
  ObAsyncIndexDeleteInfo async_delete_info_;
};

class ObMatchQueryNode final : public ObFullTextQueryNode
{
public:
  ObMatchQueryNode(common::ObIAllocator &allocator);
  virtual ~ObMatchQueryNode() {}
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int collect_exprs(
      ObIArray<ObRawExpr*> &query_exprs,
      ObIArray<ObRawExpr*> &score_exprs) const override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  
  INHERIT_TO_STRING_KV("ObFullTextQueryNode", ObFullTextQueryNode, KPC_(field), K_(index_info),
      KPC_(minimum_should_match), KPC_(operator));
public:
  ObColumnRefRawExpr *field_;
  uint64_t inv_idx_tid_;
  ObTextRetrievalIndexInfo index_info_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
};

class ObMultiMatchQueryNode final : public ObFullTextQueryNode
{
public:
  ObMultiMatchQueryNode(common::ObIAllocator &allocator);
  virtual ~ObMultiMatchQueryNode() {}
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int collect_exprs(
      ObIArray<ObRawExpr*> &query_exprs,
      ObIArray<ObRawExpr*> &score_exprs) const override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  INHERIT_TO_STRING_KV("ObFullTextQueryNode", ObFullTextQueryNode, K_(fields), K_(field_boosts),
      K_(index_infos), KPC_(minimum_should_match), KPC_(operator), KPC_(field_type));
public:
  ObFixedArray<ObColumnRefRawExpr *, ObIAllocator> fields_;
  ObFixedArray<ObConstRawExpr *, ObIAllocator> field_boosts_;
  ObFixedArray<uint64_t, ObIAllocator> inv_idx_tids_;
  ObFixedArray<ObTextRetrievalIndexInfo, ObIAllocator> index_infos_;
  ObFixedArray<ObString, ObIAllocator> index_names_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
  ObConstRawExpr *field_type_;
};

class ObMatchPhraseQueryNode final : public ObFullTextQueryNode
{
public:
  ObMatchPhraseQueryNode(common::ObIAllocator &allocator);
  virtual ~ObMatchPhraseQueryNode() {}
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int collect_exprs(
      ObIArray<ObRawExpr*> &query_exprs,
      ObIArray<ObRawExpr*> &score_exprs) const override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  INHERIT_TO_STRING_KV("ObFullTextQueryNode", ObFullTextQueryNode, KPC_(field), K_(index_info),
      KPC_(slop));
public:
  ObColumnRefRawExpr *field_;
  uint64_t inv_idx_tid_;
  ObTextRetrievalIndexInfo index_info_;
  ObConstRawExpr *slop_;
};

class ObQueryStringQueryNode final : public ObFullTextQueryNode
{
public:
  ObQueryStringQueryNode(common::ObIAllocator &allocator);
  virtual ~ObQueryStringQueryNode() {}
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int collect_exprs(
      ObIArray<ObRawExpr*> &query_exprs,
      ObIArray<ObRawExpr*> &score_exprs) const override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  INHERIT_TO_STRING_KV("ObFullTextQueryNode", ObFullTextQueryNode, K_(fields), K_(field_boosts),
      K_(index_infos), KPC_(minimum_should_match), KPC_(default_operator), KPC_(field_type));
public:
  ObFixedArray<ObColumnRefRawExpr *, ObIAllocator> fields_;
  ObFixedArray<ObConstRawExpr *, ObIAllocator> field_boosts_;
  ObFixedArray<uint64_t, ObIAllocator> inv_idx_tids_;
  ObFixedArray<ObTextRetrievalIndexInfo, ObIAllocator> index_infos_;
  ObFixedArray<ObString, ObIAllocator> index_names_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *default_operator_;
  ObConstRawExpr *field_type_;
};

} // namespace sql
} // namespace oceanbase

#endif // _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_NODE_H_
