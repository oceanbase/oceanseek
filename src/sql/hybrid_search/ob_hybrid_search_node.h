/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_HYBRID_SEARCH_NODE_H
#define _OB_HYBRID_SEARCH_NODE_H 1
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "sql/hybrid_search/ob_fulltext_search_query.h"

namespace oceanbase
{
namespace sql
{
class ObFullTextQueryNode;

class ObAsyncIndexDeleteInfo
{
public:
  ObAsyncIndexDeleteInfo()
    : delete_idx_tid_(OB_INVALID_ID),
      delete_doc_id_column_(nullptr),
      delete_rowscn_expr_(nullptr)
  {}
  ~ObAsyncIndexDeleteInfo() {}

  int resolve_delete_index(const share::schema::ObTableSchema &main_table_schema);
  int generate_doc_id_column(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx);
  int append_doc_id_expr(ObIArray<ObRawExpr *> &exprs) const;
  int append_index_id(ObIArray<uint64_t> &index_ids) const;

  bool has_delete_index() const { return OB_INVALID_ID != delete_idx_tid_; }
  bool is_valid() const
  {
    return has_delete_index()
        && nullptr != delete_doc_id_column_
        && nullptr != delete_rowscn_expr_;
  }
  uint64_t get_delete_idx_tid() const { return delete_idx_tid_; }
  ObColumnRefRawExpr *get_delete_doc_id_column() const { return delete_doc_id_column_; }
  ObPseudoColumnRawExpr *get_delete_rowscn_expr() const { return delete_rowscn_expr_; }

  TO_STRING_KV(K_(delete_idx_tid), KPC_(delete_doc_id_column), KPC_(delete_rowscn_expr));

private:
  uint64_t delete_idx_tid_;
  ObColumnRefRawExpr *delete_doc_id_column_;
  ObPseudoColumnRawExpr *delete_rowscn_expr_;
};
class ObScalarQueryNode;

enum class ObVecBruteFilterStrategy : uint8_t
{
  NONE = 0,
  PUSHDOWN_ONLY,
  PUSHDOWN_WITH_BITMAP,
  BITMAP_ONLY
};

class ObHybridSearchNodeBase : public ObIndexMergeNode
{
public:
  ObHybridSearchNodeBase(common::ObIAllocator &allocator) : ObIndexMergeNode(allocator), boost_(nullptr), is_scoring_(false),
    is_top_level_scoring_(false), index_name_(), primary_get_ratio_(0) {}
  virtual ~ObHybridSearchNodeBase() {}
  virtual inline bool is_hybrid_scalar_node() const { return false; }
  virtual inline bool is_hybrid_scalar_node_with_index() const { return false; }
  virtual inline bool is_hybrid_search_node() const { return true; }
  virtual const ObAsyncIndexDeleteInfo *get_async_delete_info() const { return nullptr; }
  virtual int generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx);
  virtual int collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count) = 0;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const;
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) {
    UNUSED(expr_factory);
    extracted_expr = nullptr;
    return OB_NOT_SUPPORTED;
  }
  int get_all_scan_node(ObIArray<ObIndexMergeNode*> &scan_nodes);
  int collect_hybrid_search_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const;
  int collect_vec_infos(ObIArray<ObVecIndexInfo*> &vec_infos);
  int get_index_name(ObIArray<ObString> &index_names);
  int print_blank_space(char *buf, int64_t buf_len, int64_t &pos, int n);
  ObConstRawExpr *boost_;
  bool is_scoring_;
  bool is_top_level_scoring_;
  ObString index_name_; // plan info explain
  // Primary-get optimization ratio for this node; 0 means disabled.
  // Stamped at generation time from cur_primary_get_ratio_. The query path uses its explicit
  // DSL value or tenant default, while knn overrides the value for its subtree. Inherited
  // by scalar and boolean ctdefs at CG so the runtime can choose primary probe.
  int64_t primary_get_ratio_;
};

class ObBooleanQueryNode : public ObHybridSearchNodeBase
{
public:
  ObBooleanQueryNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator), must_nodes_(allocator), should_nodes_(allocator),
    filter_nodes_(allocator), must_not_nodes_(allocator), min_should_match_(0), origin_expr_(nullptr),
    minimum_should_match_(nullptr), merged_probe_scalar_node_(nullptr)
    { node_type_ = INDEX_MERGE_HYBRID_BOOLEAN_QUERY; }
  virtual ~ObBooleanQueryNode() {}
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) override;
public:
  ObSqlArray<ObIndexMergeNode*> must_nodes_;
  ObSqlArray<ObIndexMergeNode*> should_nodes_;
  ObSqlArray<ObIndexMergeNode*> filter_nodes_;
  ObSqlArray<ObIndexMergeNode*> must_not_nodes_;
  double min_should_match_;
  ObRawExpr *origin_expr_; // for search index
  ObConstRawExpr *minimum_should_match_;
  // Merged primary-probe scalar node built from the AND of all required (must+filter) clause filters
  ObScalarQueryNode *merged_probe_scalar_node_;
};

class ObFusionNode : public ObHybridSearchNodeBase
{
public:
  ObFusionNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator), method_(ObFusionMethod::WEIGHT_SUM),
    from_(nullptr), size_(nullptr), min_score_(nullptr), window_size_(nullptr), rank_const_(nullptr),
    rowkey_cols_(allocator), score_cols_(allocator), weight_cols_(allocator), path_top_k_limit_(allocator),
    contains_vec_node_(false), search_index_(-1), has_search_subquery_(false), has_vector_subquery_(false),
    is_top_k_query_(true), has_hybrid_fusion_op_(false), query_dop_(0), track_score_(true),
    fusion_iter_exec_mode_(ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS), is_single_partition_(true)
    { node_type_ = INDEX_MERGE_HYBRID_FUSION_SEARCH; }
  virtual ~ObFusionNode() {}
  virtual inline bool contains_vector_node() const { return contains_vec_node_; }
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  ObRawExpr *get_fusion_score_expr() const {return score_cols_.empty() ? nullptr : score_cols_.at(score_cols_.count() - 1);}
  bool has_rerank() const { return rerank_info_.has_rerank(); }
  void set_is_single_partition(bool v) { is_single_partition_ = v; }
  bool get_is_single_partition() const { return is_single_partition_; }

  ObFusionMethod method_;
  ObRawExpr *from_;
  ObRawExpr *size_;
  ObRawExpr *min_score_;
  ObRawExpr *window_size_;
  ObRawExpr *rank_const_;
  ObSqlArray<ObRawExpr*> rowkey_cols_;
  ObSqlArray<ObRawExpr*> score_cols_;
  ObSqlArray<ObRawExpr*> weight_cols_;
  ObSqlArray<ObRawExpr*> path_top_k_limit_;
  bool contains_vec_node_;
  int64_t search_index_;
  bool has_search_subquery_;
  bool has_vector_subquery_;
  bool is_top_k_query_;
  bool has_hybrid_fusion_op_;
  int64_t query_dop_;
  bool track_score_;
  ObFusionIterExecMode fusion_iter_exec_mode_;
  bool is_single_partition_;  // true when non-partitioned table: DAS fusion iter already did fusion
  ObDSLRerankInfo rerank_info_;
};

class ObScalarQueryNode : public ObHybridSearchNodeBase
{
public:
  ObScalarQueryNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator),
                        need_rowkey_order_(true),
                        pri_table_ap_(nullptr),
                        pri_table_query_params_(allocator),
                        index_table_query_params_(allocator),
                        index_table_rowkey_exprs_(allocator),
                        has_valid_index_(false),
                        is_extracted_(false),
                        is_merged_probe_(false),
                        is_async_(false),
                        async_delete_info_()
  { node_type_ = INDEX_MERGE_HYBRID_SCALAR_QUERY; }
  virtual ~ObScalarQueryNode() {}
  virtual inline bool is_hybrid_scalar_node() const { return true; }
  virtual inline bool is_hybrid_scalar_node_with_index() const { return has_index(); }
  virtual const ObAsyncIndexDeleteInfo *get_async_delete_info() const override
  {
    return is_async_ ? &async_delete_info_ : nullptr;
  }
  virtual int generate_access_exprs(
      const TableItem &table_item,
      ObOptimizerContext &opt_ctx) override;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const override;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) override;
  inline bool has_index() const { return has_valid_index_; }
  int resolve_async_delete_index(const share::schema::ObTableSchema &main_table_schema)
  {
    return async_delete_info_.resolve_delete_index(main_table_schema);
  }
  bool can_keep_imprecise_range() const; // only support for wildcard querying varchar column
  int add_pd_filter_for_imprecise_range(); // only support for wildcard querying varchar column

public:
  struct ScalarQueryParams
  {
  public:
    ScalarQueryParams(common::ObIAllocator &allocator) : access_exprs_(allocator), output_col_ids_(allocator),
      pushdown_filters_(allocator), filter_monotonicity_(allocator)
    {}
    virtual ~ScalarQueryParams() {}

    ObSqlArray<ObRawExpr*> access_exprs_;
    ObSqlArray<uint64_t> output_col_ids_;
    ObSqlArray<ObRawExpr*> pushdown_filters_;
    ObSqlArray<ObRawFilterMonotonicity> filter_monotonicity_;
  };

  bool need_rowkey_order_;
  // In hybrid search, scalar queries may not preserve primary key order.
  // Here we provide an optimization: use the index column query conditions as filters
  // and perform a sequential scan on the primary table, so that the results can be streamed in primary key order.
  AccessPath *pri_table_ap_;
  ScalarQueryParams pri_table_query_params_;
  ScalarQueryParams index_table_query_params_;
  // For search index, rowkey exprs are not the same as primary table, store it separately
  ObSqlArray<ObRawExpr*> index_table_rowkey_exprs_;
  bool has_valid_index_;
  bool is_extracted_;  // For vec search: all filter exprs are extracted to generate a new scalar query node
  // The nested() clause paths (internal "$." form, e.g. "$.comments.replies"), outermost
  // first, that this scalar leaf is generated underneath. Empty for a plain (non-nested)
  // predicate. Drives query-side path encoding: an ARRAY marker is inserted only at a path
  // segment equal to one of these explicitly-opened nested paths, so array-vs-object shape
  // follows the QUERY's nesting rather than the index NESTED_PATHS declarations.
  common::ObSEArray<common::ObString, 20> enclosing_nested_paths_;
  // True when this scalar node is the merged primary-probe node built by ObBooleanQueryNode.
  bool is_merged_probe_;
  // Async search-index metadata. The shared delete scan is synthesized at CG time
  // and wrapped around this scalar node as must_not.
  bool is_async_;
  ObAsyncIndexDeleteInfo async_delete_info_;
};

class ObVecSearchNode : public ObHybridSearchNodeBase
{
public:
  ObVecSearchNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator),
                      filter_nodes_(allocator),
                      extracted_scalar_node_(nullptr),
                      brute_pushdown_filter_(nullptr),
                      main_index_table_id_(OB_INVALID_ID),
                      vec_info_(allocator),
                      field_(nullptr),
                      table_schema_(nullptr),
                      search_option_(nullptr),
                      is_brute_force_(false) {
                        node_type_ = INDEX_MERGE_HYBRID_KNN;
                      }
  virtual ~ObVecSearchNode() {};

  int init_vec_info(ObSqlSchemaGuard *schema_guard);
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
public:
  struct VecIndexScanParams
  {
  public:
    VecIndexScanParams() : access_exprs_(), pushdown_filters_(), table_id_(OB_INVALID_ID),
                           scan_type_(ObTSCIRScanType::OB_IR_INV_IDX_SCAN) {}
    virtual ~VecIndexScanParams() {}

    common::ObSEArray<ObRawExpr*, 2, common::ModulePageAllocator, true> access_exprs_;
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> pushdown_filters_;
    ObTableID table_id_;
    ObTSCIRScanType scan_type_;
  };

  const ObVectorIndexQueryParam &get_query_param() const { return vec_info_.query_param_; }
  const ObVectorIndexParam &get_vector_index_param() const { return vec_info_.vector_index_param_; }
  ObVectorIndexAlgorithmType get_algorithm_type() const { return vec_info_.get_vec_algorithm_type(); }
  ObVecIndexType get_vec_type() const { return vec_info_.vec_type_; }
  int64_t get_row_count() const { return vec_info_.row_count_; }
  ObTableID get_data_table_id() const { return vec_info_.main_table_tid_; }
  ObRawExpr* get_target_vec_column() const { return static_cast<ObRawExpr*>(vec_info_.target_vec_column_); }

  OrderItem get_sort_key() const { return vec_info_.sort_key_; }
  ObRawExpr *get_topk_limit_expr() const { return vec_info_.topk_limit_expr_; }
  ObRawExpr *get_topk_offset_expr() const { return vec_info_.topk_offset_expr_; }

  int get_delta_buf_table_params(VecIndexScanParams &params) const;
  int get_index_id_table_params(VecIndexScanParams &params) const;
  int get_snapshot_table_params(VecIndexScanParams &params) const;
  int get_com_aux_vec_table_params(VecIndexScanParams &params) const;
  // IVF auxiliary table params
  int get_brute_force_main_table_params(VecIndexScanParams &params) const;
  int get_centroid_table_params(VecIndexScanParams &params) const;
  int get_cid_vec_table_params(VecIndexScanParams &params) const;
  int get_rowkey_cid_table_params(VecIndexScanParams &params) const;
  int get_special_aux_table_params(VecIndexScanParams &params) const;
  ObVecBruteFilterStrategy get_brute_filter_strategy() const;
  inline bool need_extract_filter() const 
  { 
    return search_option_ == nullptr 
        || search_option_->filter_mode_ == ObKnnFilterMode::POST_FILTER 
        || search_option_->filter_mode_ == ObKnnFilterMode::INVALID_KNN_FILTER_MODE; 
  }

  ObSqlArray<ObIndexMergeNode*> filter_nodes_;
  ObScalarQueryNode *extracted_scalar_node_;  // for iterative filter
  // The complete scalar predicate extracted specifically for brute-force main-table
  // scan pushdown. It is independent of extracted_scalar_node_, which belongs to the
  // vector-index post/iterative-filter path.
  ObRawExpr *brute_pushdown_filter_;
  ObTableID main_index_table_id_;
  ObVecIndexInfo vec_info_;
  ObColumnRefRawExpr *field_;
  const ObTableSchema *table_schema_;
  const ObDSLKnnQuery::SearchOption *search_option_;
  bool is_brute_force_;
};

// Represents a nested.query DSL clause in the optimizer node tree.
// Has exactly one child: the inner query subtree.
class ObNestedQueryNode : public ObHybridSearchNodeBase
{
public:
  explicit ObNestedQueryNode(common::ObIAllocator &allocator)
    : ObHybridSearchNodeBase(allocator),
      inner_node_(nullptr),
      nesting_depth_(0),
      path_(),
      column_idx_(-1)
  { node_type_ = INDEX_MERGE_HYBRID_NESTED_QUERY; }
  virtual ~ObNestedQueryNode() {}
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count) override;

public:
  ObIndexMergeNode *inner_node_; // the single inner subtree
  int32_t nesting_depth_;        // DSL nesting depth (1 for first-level nested, 2 for nested-in-nested, etc.)
  common::ObString path_;        // nested path string (e.g., "$.comments"); added for child-domain must_not synthesis
  int64_t column_idx_;           // JSON column ordinal in base table (= search index rowkey col_idx); -1 if unset
};

class ObHybridSearchGenerator {
public:
  explicit ObHybridSearchGenerator(common::ObIAllocator *alloc,
                                   ObLogPlan *plan,
                                   ObSqlSchemaGuard *schema_guard,
                                   const ObTableSchema *table_schema,
                                   const ObIArray<uint64_t> &valid_index_ids,
                                   const ObIArray<ObSEArray<uint64_t, 4>> &valid_index_cols,
                                   const ObIArray<bool> &index_can_ignore_prefix,
                                   const TableItem *table_item,
                                   const bool has_force_index_merge_hint,
                                   const ObIArray<uint64_t> *index_merge_hint_column_ids)
    : allocator_(alloc), plan_(plan), schema_guard_(schema_guard), table_schema_(table_schema),
      valid_index_ids_(valid_index_ids), valid_index_cols_(valid_index_cols),
      index_can_ignore_prefix_(index_can_ignore_prefix),
      fusion_node_(nullptr), table_item_(table_item), cur_primary_get_ratio_(0),
      has_force_index_merge_hint_(has_force_index_merge_hint),
      index_merge_hint_column_ids_(index_merge_hint_column_ids) {}
  virtual ~ObHybridSearchGenerator() {}
  int generate(const ObDSLQueryInfo *dsl_query, ObIndexMergeNode *&hybrid_search_tree);
  int deal_table_scan_filters(ObIndexMergeNode *hybrid_search_tree,
                              ObIArray<ObRawExpr*> &table_filters,
                              bool &ingore_normal_access_path);
  int move_bool_nodes_to_children(ObIndexMergeNode* node);
  int extract_partition_filters_from_tree(
      ObIndexMergeNode *node,
      const common::ObIArray<uint64_t> &part_col_ids,
      common::ObIArray<ObRawExpr*> &partition_filters);
private:
  static int extract_partition_related_filters(
      const common::ObIArray<ObRawExpr*> &filters,
      const common::ObIArray<uint64_t> &part_col_ids,
      common::ObIArray<ObRawExpr*> &partition_filters);
  static int check_should_required_for_partition_pruning(
      const ObBooleanQueryNode &node,
      bool &should_required);
  int build_or_partition_filter(
      const common::ObIArray<ObRawExpr*> &left_filters,
      const common::ObIArray<ObRawExpr*> &right_filters,
      common::ObIArray<ObRawExpr*> &result);
  int generate_node(const ObDSLQuery *query, ObIndexMergeNode *&node);
  int generate_boolean_node(const ObDSLBoolQuery *dsl_query, ObBooleanQueryNode *&bool_node);
  int generate_nested_node(const ObDSLNestedQuery *nested_query, ObNestedQueryNode *&nested_node);
  int generate_scalar_node(ObRawExpr *filter, ObScalarQueryNode *&scalar_node);
  int generate_fulltext_node(const ObDSLFullTextQuery *fulltext_query, ObFullTextQueryNode *&fulltext_node);
  int generate_knn_node(const ObDSLKnnQuery *knn_query, ObVecSearchNode *&knn_node);
  int try_extract_brute_pushdown_filter(ObVecSearchNode &knn_node);
  int try_merge_nodes(const ObDSLQuery *query, ObIArray<ObIndexMergeNode*> &nodes, bool &merge_happened);
  int check_filter_has_index(ObRawExpr *filter, bool &has_index, uint64_t &target_index_id);
  int get_search_index_cons_encode_type(ObRawExpr *filter,
                                        bool &has_search_index,
                                        uint8_t &cons_encode_type);
  int init_fusion_node(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node);
  int init_fusion_iter_exec_mode(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node);

  bool is_search_subquery(ObIndexMergeNode *node) const;
  bool is_knn_subquery(ObIndexMergeNode *node) const;
  int collect_child_boost_and_update_flags(const ObDSLQueryInfo *dsl_info,
                                           ObFusionNode *fusion_node,
                                           ObIndexMergeNode *sub_node,
                                           ObConstRawExpr *one_boost_expr,
                                           int64_t child_idx);
  int deal_child_node(ObIndexMergeNode *node, bool &prune_happened, double &sel);
  int check_node_match_index_merge_column_hint(const ObIndexMergeNode *node,
                                               bool &is_match) const;
  int recurse_count_index_nodes(const ObIndexMergeNode *node, int64_t &index_scan_count,
                                bool &ingore_normal_access_path) const;
  int split_domain_contains_node(const ObDSLScalarQuery *scalar_query,
                                 ObIndexMergeNode *&split_node);
  int split_json_contains(const ObDSLScalarQuery *scalar_query,
                          ObRawExpr *filter_expr,
                          uint8_t cons_encode_type,
                          ObIndexMergeNode *&split_node);
  int split_array_contains_all(const ObDSLScalarQuery *scalar_query,
                               ObRawExpr *filter_expr,
                               ObIndexMergeNode *&split_node);
  static bool in_merge_expr_whitelist(const ObRawExpr *expr);
  int build_merged_boost_expr(const ObConstRawExpr *boost1,
                              const ObConstRawExpr *boost2,
                              ObConstRawExpr *&merged_boost_expr);
  int build_split_boost_expr(const ObDSLScalarQuery *scalar_query,
                             const int64_t element_count,
                             ObConstRawExpr *&split_boost_expr);
  // Gate for the merged primary-probe optimization on a boolean query.
  // The gate accepts only non-scoring bools without must_not, and SHOULD only for a static msm=1.
  // The generator then extracts one filter from the direct scalar must/filter/should children;
  // nested bool, fulltext, and other non-extractable children abandon the whole optimization.
  // At runtime, each required clause whose prefer_primary_probe
  // (ratio>0 && lead_cost<=cost/ratio) returns true is skipped and folded into the merged probe;
  // the rest run as independent conjunction drivers.
  static bool enable_merged_probe(const ObBooleanQueryNode *node);
  int try_generate_merged_probe_node(ObBooleanQueryNode *node);
private :
  common::ObIAllocator *allocator_;
  ObLogPlan *plan_;
  ObSqlSchemaGuard *schema_guard_;
  const ObTableSchema *table_schema_;
  const ObIArray<uint64_t> &valid_index_ids_;
  const ObIArray<ObSEArray<uint64_t, 4>> &valid_index_cols_;
  const ObIArray<bool> &index_can_ignore_prefix_;
  ObFusionNode *fusion_node_;
  const TableItem *table_item_;
  // Recursion-state stack mirroring the resolver's enclosing_nested_paths_: the nested()
  // clause paths currently open while descending into their inner subtree. A scalar leaf
  // generated here deep-copies this stack into ObScalarQueryNode::enclosing_nested_paths_.
  common::ObSEArray<common::ObString, 20> cur_nested_paths_;
  // Ratio stamped onto every ObHybridSearchNodeBase during generation. Initialized from the
  // explicit or default primary_get_ratio_ in generate(), then save/set/restore
  // around generate_knn_node so the knn subtree overrides it while siblings are unaffected.
  // 0 means off.
  int64_t cur_primary_get_ratio_;
  static const double MAX_INTERSECTION_MERGE_SEL;
  static const double MAX_SEL_GAP_RATIO;
  bool has_force_index_merge_hint_;
  const ObIArray<uint64_t> *index_merge_hint_column_ids_;
};

}
}

#endif
