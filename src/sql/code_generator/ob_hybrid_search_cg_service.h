/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
#define DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_nested_query_op.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "share/logical_table/ob_logical_table_define.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{
class ObStaticEngineCG;
class ObTableScanCtDef;
class ObFullTextQueryNode;
class ObMatchQueryNode;
class ObDASMatchCtDef;
class ObMatchPhraseQueryNode;
class ObDASMatchPhraseCtDef;
class ObMultiMatchQueryNode;
class ObDASMultiMatchCtDef;
class ObQueryStringQueryNode;
class ObDASQueryStringCtDef;
class ObTextRetrievalIndexInfo;
class ObDASIRScanCtDef;

class ObHybridSearchCgService
{
public:
  // M4: logical table rewrite pass for hybrid search
  // Shared rewrite-stage context. Lives only across rewrite_for_logical_table_ and its
  // helpers; no allocation, no virtual functions.
  struct HybridLtRewriteCtx
  {
    HybridLtRewriteCtx()
      : phys_key_expr_(nullptr),
        phys_value_expr_(nullptr),
        kv_key_raw_(nullptr),
        kv_value_raw_(nullptr),
        physical_data_table_id_(common::OB_INVALID_ID),
        logical_ref_table_id_(common::OB_INVALID_ID)
    {}
    ObExpr *phys_key_expr_;
    ObExpr *phys_value_expr_;
    ObColumnRefRawExpr *kv_key_raw_;
    ObColumnRefRawExpr *kv_value_raw_;
    uint64_t physical_data_table_id_;
    uint64_t logical_ref_table_id_;
  };

  ObHybridSearchCgService(ObStaticEngineCG &cg) : cg_(cg), logical_fts_filter_exprs_(nullptr), is_generating_nested_query_(false) { }
  virtual ~ObHybridSearchCgService() { }

  // main function to generate the hybrid search ctdef
  int generate_hybrid_search_ctdef(ObLogTableScan &op,
                                   ObTableScanCtDef &tsc_ctdef,
                                   ObDASBaseCtDef *&root_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObScalarQueryNode* scalar_query_node,
                     ObDASScalarCtDef *&scalar_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObBooleanQueryNode *boolean_query_node,
                     ObDASBooleanQueryCtDef *&boolean_query_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObNestedQueryNode *nested_query_node,
                     ObDASNestedQueryCtDef *&nested_query_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObFusionNode *fusion_node,
                     ObDASFusionCtDef *&fusion_ctdef);
  
  int generate_ctdef(ObLogTableScan &op,
                     const ObVecSearchNode *vec_search_node,
                     ObDASVecIndexDriverCtDef *&vec_index_driver_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObFullTextQueryNode *fulltext_query_node,
                     ObDASBaseCtDef *&fulltext_query_ctdef);

  // In the hybrid search scenario, tsc_ctdef.scan_ctdef_ is not used to perform scans, but it undertakes
  // functions such as partition routing and represents the primary partition corresponding to the DAS SCAN OP.
  // Therefore, it is still necessary to fill the required information.
  int mock_das_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef &scan_ctdef);

  // generate the final lookup to project and possibly filter the result 
  int generate_final_lookup_ctdef(ObLogTableScan &op,
                                  ObTableScanCtDef &tsc_ctdef,
                                  ObDASBaseCtDef &search_ctdef,
                                  ObDASBaseCtDef *&root_ctdef);
  int generate_main_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef *&main_scan_ctdef);
  /** @brief Carry __shard_doc and the search_after flag through DAS lookup. */
  int generate_search_after_ctdef(const ObFusionNode &node, ObDASFusionCtDef &fusion);
  /** @brief Return DSL search_after filters for evaluation after lookup. */
  static int extract_dsl_nonpushdown_filters(const ObLogTableScan &op,
                                             ObIArray<ObRawExpr *> &filters);
  /** @brief Collect filter inputs, treating DAS-produced __shard_doc as one value. */
  static int extract_filter_input_exprs(const ObLogTableScan &op,
                                        const ObIArray<ObRawExpr *> &filters,
                                        ObIArray<ObRawExpr *> &inputs);
  /** @brief Prepare lookup columns and keep search_after filtering above lookup. */
  int prepare_main_scan_access_ctdef(ObLogTableScan &op,
                                     ObDASScanCtDef &scan_ctdef,
                                     ObIArray<ObRawExpr *> &non_pushdown_filters,
                                     ObIArray<ObRawExpr *> &lookup_pushdown_filters);
  int extract_column_ids(const ObIArray<ObRawExpr *> &column_exprs, ObIArray<uint64_t> &column_ids);

  // [LT plain MATCH] Apply hybrid's FTS-aux KV rewrite to the IR scan ctdef sub-tree
  // (inv_idx_scan / inv_idx_agg / fwd_idx_agg). Used by ObTscCgService::generate_text_ir_ctdef
  // when op.is_logical_table().
  int rewrite_text_ir_ctdef_for_logical_table(ObLogTableScan &op,
                                              ObDASIRScanCtDef &ir_ctdef,
                                              const HybridLtRewriteCtx &ctx);

  // [LT plain MATCH] Bake ns/lt ObExpr* extracted from the WHERE predicates onto the FTS
  // inverted-index scan ctdef's lt_ctx_. On plain MATCH (no index merge) the inverted scan
  // ctdef IS the DAS op's main scan_ctdef_, and ObDASScanOp::init_scan_param requires these
  // exprs to resolve lt_rt_ (ns/lt); without them the query fails with -4007. The generic
  // rewrite_for_logical_table_ path is deliberately skipped for FTS IR aux scans in
  // generate_das_scan_ctdef, so lt_ctx_ would otherwise stay empty.
  // NOTE: lt_ctx_.analyzed_ MUST stay false here. Setting it would (1) make
  // init_scan_param build the primary [ns][lt] KV prefix range and overwrite the token
  // ranges, and (2) disable the runtime range rewrite in
  // ObDASScanIter::try_session_lt_range_rewrite_ that FTS aux scans rely on.
  int inject_logical_table_ns_lt_into_fts_inv_scan_ctdef(ObLogTableScan &op,
                                                         ObDASScanCtDef &inv_scan_ctdef);

  // Stage 1: prepare phys_key/phys_value rt exprs and cache table-id metadata.
  // Public so ObTscCgService can drive the same rewrite for plain MATCH on logic tables.
  int prepare_hybrid_lt_rewrite_ctx_(ObLogTableScan &op, HybridLtRewriteCtx &ctx);

  // Stage 3: mark phys_key/phys_value as self-produced (so calc_exprs_ can find them).
  int mark_kv_exprs_self_produced_(ObLogTableScan &op, const HybridLtRewriteCtx &ctx);

  // Rewrite scalar scan / IR aux scan ctdef for FTS aux logical table (FTS_INVERTED / FTS_FORWARD).
  // Templated to accept both ObDASScalarScanCtDef (hybrid) and ObDASScanCtDef (plain MATCH).
  template <class CtDefT>
  int rewrite_fts_aux_ctdef_for_logical_table_(ObLogTableScan &op,
                                               share::ObLogicalTableAuxType aux_type,
                                               CtDefT &scan_ctdef);

  // Plain MATCH path on logical table: produce an ObDASScanCtDef-based primary KV scan
  // for filtered BM25 stats. Mirrors the scalar version. Filter exprs are passed in
  // because the plain MATCH path has no ObHybridSearchNode tree to walk.
  int generate_text_ir_stats_primary_scan_ctdef(
      ObLogTableScan &op,
      const common::ObIArray<ObRawExpr *> &filter_exprs,
      ObDASScanCtDef *&scan_ctdef);

private:
  class DASScalarCGParams
  {
  public:
    DASScalarCGParams(uint64_t data_table_id,
                      uint64_t index_table_id,
                      bool is_new_query_range,
                      bool is_rowkey_order_scan,
                      bool is_primary_table_scan,
                      share::schema::ObTableType table_type,
                      const ObIArray<ObRawExpr*> *access_exprs,
                      const ObIArray<uint64_t> *tsc_out_cols,
                      const ObIArray<ObRawExpr*> *rowkey_exprs,
                      const ObPreRangeGraph *pre_range_graph,
                      const ObQueryRange *pre_query_range,
                      bool has_pre_query_range = true,
                      bool is_get = false,
                      bool need_rowkey_order = true,
                      bool is_use_column_store = false,
                      const ObIArray<ObRawExpr*> *domain_id_expr = nullptr,
                      const ObIArray<uint64_t> *domain_id_col_ids = nullptr,
                      const ObIArray<ObRawExpr*> *pushdown_filters = nullptr,
                      const ObIArray<ObRawExpr*> *pushdown_aggr_exprs = nullptr,
                      const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity = nullptr)
      : data_table_id_(data_table_id),
        index_table_id_(index_table_id),
        is_new_query_range_(is_new_query_range),
        is_rowkey_order_scan_(is_rowkey_order_scan),
        is_primary_table_scan_(is_primary_table_scan),
        table_type_(table_type),
        access_exprs_(access_exprs),
        tsc_out_cols_(tsc_out_cols),
        rowkey_exprs_(rowkey_exprs),
        pre_range_graph_(pre_range_graph),
        pre_query_range_(pre_query_range),
        has_pre_query_range_(has_pre_query_range),
        is_get_(is_get),
        need_rowkey_order_(need_rowkey_order),
        is_use_column_store_(is_use_column_store),
        domain_id_expr_(domain_id_expr),
        domain_id_col_ids_(domain_id_col_ids),
        pushdown_filters_(pushdown_filters),
        pushdown_aggr_exprs_(pushdown_aggr_exprs),
        filter_monotonicity_(filter_monotonicity)
    { }

    ~DASScalarCGParams() { reset(); }

    bool is_valid() const {
      return data_table_id_ != common::OB_INVALID_ID &&
             index_table_id_ != common::OB_INVALID_ID &&
             access_exprs_ != nullptr &&
             tsc_out_cols_ != nullptr &&
             rowkey_exprs_ != nullptr &&
             table_type_ != share::schema::MAX_TABLE_TYPE &&
             (!has_pre_query_range_ ||
                (is_new_query_range_ ? pre_range_graph_ != nullptr : pre_query_range_ != nullptr));
    }

    void init(uint64_t data_table_id,
              uint64_t index_table_id,
              bool is_new_query_range,
              bool is_rowkey_order_scan,
              bool is_primary_table_scan,
              share::schema::ObTableType table_type,
              const ObIArray<ObRawExpr*> *access_exprs,
              const ObIArray<uint64_t> *tsc_out_cols,
              const ObIArray<ObRawExpr*> *rowkey_exprs,
              const ObPreRangeGraph *pre_range_graph,
              const ObQueryRange *pre_query_range,
              bool has_pre_query_range = true,
              bool is_get = false,
              bool need_rowkey_order = true,
              bool is_use_column_store = false,
              const ObIArray<ObRawExpr*> *domain_id_expr = nullptr,
              const ObIArray<uint64_t> *domain_id_col_ids = nullptr,
              const ObIArray<ObRawExpr*> *pushdown_filters = nullptr,
              const ObIArray<ObRawExpr*> *pushdown_aggr_exprs = nullptr,
              const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity = nullptr) {
      data_table_id_ = data_table_id;
      index_table_id_ = index_table_id;
      is_new_query_range_ = is_new_query_range;
      is_rowkey_order_scan_ = is_rowkey_order_scan;
      is_primary_table_scan_ = is_primary_table_scan;
      table_type_ = table_type;
      access_exprs_ = access_exprs;
      tsc_out_cols_ = tsc_out_cols;
      rowkey_exprs_ = rowkey_exprs;
      pre_range_graph_ = pre_range_graph;
      pre_query_range_ = pre_query_range;
      has_pre_query_range_ = has_pre_query_range;
      is_get_ = is_get;
      need_rowkey_order_ = need_rowkey_order;
      is_use_column_store_ = is_use_column_store;
      domain_id_expr_ = domain_id_expr;
      domain_id_col_ids_ = domain_id_col_ids;
      pushdown_filters_ = pushdown_filters;
      pushdown_aggr_exprs_ = pushdown_aggr_exprs;
      filter_monotonicity_ = filter_monotonicity;
    }

    void reset() {
      data_table_id_ = common::OB_INVALID_ID;
      index_table_id_ = common::OB_INVALID_ID;
      is_new_query_range_ = false;
      is_rowkey_order_scan_ = false;
      is_primary_table_scan_ = false;
      table_type_ = share::schema::MAX_TABLE_TYPE;
      access_exprs_ = nullptr;
      tsc_out_cols_ = nullptr;
      rowkey_exprs_ = nullptr;
      pre_range_graph_ = nullptr;
      pre_query_range_ = nullptr;
      has_pre_query_range_ = true;
      is_get_ = false;
      need_rowkey_order_ = true;
      is_use_column_store_ = false;
      domain_id_expr_ = nullptr;
      domain_id_col_ids_ = nullptr;
      pushdown_filters_ = nullptr;
      filter_monotonicity_ = nullptr;
    }

    #define DEF_VAL_ACCESSOR(type, name)                                \
      OB_INLINE type get_##name() const { return name##_; }
    #define DEF_BOOL_ACCESSOR(type, name)                               \
      OB_INLINE type name() const { return name##_; }
    #define DEFINE_GETTER(V, F)                                         \
      V(uint64_t,                             data_table_id)            \
      V(uint64_t,                             index_table_id)           \
      F(bool,                                 is_new_query_range)       \
      F(bool,                                 is_rowkey_order_scan)     \
      F(bool,                                 is_primary_table_scan)    \
      V(share::schema::ObTableType,           table_type)               \
      V(const ObIArray<ObRawExpr*> *,         access_exprs)             \
      V(const ObIArray<uint64_t> *,           tsc_out_cols)             \
      V(const ObIArray<ObRawExpr*> *,         rowkey_exprs)             \
      V(const ObPreRangeGraph *,              pre_range_graph)          \
      V(const ObQueryRange *,                 pre_query_range)          \
      F(bool,                                 has_pre_query_range)      \
      F(bool,                                 is_get)                   \
      F(bool,                                 need_rowkey_order)        \
      F(bool,                                 is_use_column_store)      \
      V(const ObIArray<ObRawExpr*> *,         domain_id_expr)           \
      V(const ObIArray<uint64_t> *,           domain_id_col_ids)        \
      V(const ObIArray<ObRawExpr*> *,         pushdown_filters)         \
      V(const ObIArray<ObRawExpr*> *,         pushdown_aggr_exprs)      \
      V(const ObIArray<ObRawFilterMonotonicity> *, filter_monotonicity)
    DEFINE_GETTER(DEF_VAL_ACCESSOR, DEF_BOOL_ACCESSOR)
    #undef DEF_VAL_ACCESSOR
    #undef DEF_BOOL_ACCESSOR
    #undef DEFINE_GETTER

  private:
    // necessary
    uint64_t data_table_id_;
    uint64_t index_table_id_;
    bool is_new_query_range_;                   // the query range is new type (ObPreRangeGraph) or old (ObQueryRange)
    bool is_rowkey_order_scan_;                 // the query range is rowkey order scan
    bool is_primary_table_scan_;                // scan the primary table
    share::schema::ObTableType table_type_;     // the table type
    const ObIArray<ObRawExpr*> *access_exprs_;  // exprs for accessing columns from the table
    const ObIArray<uint64_t> *tsc_out_cols_;    // col ids required for the table scan output
    const ObIArray<ObRawExpr*> *rowkey_exprs_;  // exprs representing the rowkey of the table
    const ObPreRangeGraph *pre_range_graph_;    // graph representation of query ranges (used if is_new_query_range_ is true)
    const ObQueryRange *pre_query_range_;       // standard representation of query ranges (used if is_new_query_range_ is false)

    // optional
    bool has_pre_query_range_;                    // has pre extracted query range for table scan
    bool is_get_;
    bool need_rowkey_order_;                        // the scan needs to preserve the order of rowkeys
    bool is_use_column_store_;                      // the scan should use column store storage format
    const ObIArray<ObRawExpr*> *domain_id_expr_;    // exprs for domain id
    const ObIArray<uint64_t> *domain_id_col_ids_;   // col ids corresponding to the domain id
    const ObIArray<ObRawExpr*> *pushdown_filters_;  // filters that can be pushed down to the storage layer
    const ObIArray<ObRawExpr*> *pushdown_aggr_exprs_; // aggr exprs pushed down to the storage layer
    const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity_;  // monotonicity information for filters
  };

  int generate_hybrid_search_node_ctdef(ObLogTableScan &op,
                                        ObHybridSearchNodeBase *node,
                                        ObDASBaseCtDef *&root_ctdef);

  // Wrap an async fulltext/search-index subtree with a boolean query whose
  // must_not is the shared delete-index scan.
  int wrap_async_index_with_delete(ObLogTableScan &op,
                                   uint64_t delete_idx_tid,
                                   ObColumnRefRawExpr *delete_doc_id_column,
                                   ObRawExpr *delete_rowscn_expr,
                                   ObDASBaseCtDef *index_ctdef,
                                   ObDASBaseCtDef *&boolean_ctdef);
  // Coalesce compatible async Boolean domains in post-order. Mixed synchronous
  // Filters require a positive async MUST/FILTER that owns each Group's output.
  int normalize_async_fts_boolean_domain(
      ObDASBaseCtDef *&root_ctdef,
      ObDASAsyncIndexExecMode &exec_mode);
  // Normalize each KNN filter as an independent async domain. The vector scan and the
  // ordinary query/boolean domains remain outside this traversal boundary.
  int normalize_async_knn_filter_domains_(ObDASBaseCtDef *root_ctdef);
  struct AsyncFtsDomainRewriteAction
  {
    AsyncFtsDomainRewriteAction()
      : target_(nullptr), replacement_(nullptr)
    {}
    AsyncFtsDomainRewriteAction(
        ObDASBaseCtDef **target,
        ObDASBaseCtDef *replacement)
      : target_(target), replacement_(replacement)
    {}
    TO_STRING_KV(KP_(target), KP_(replacement));
    ObDASBaseCtDef **target_;
    ObDASBaseCtDef *replacement_;
  };
  int normalize_async_fts_boolean_domain_impl(
      ObDASBaseCtDef *&root_ctdef,
      bool &has_domain_rewrite);
  int plan_async_fts_boolean_domain_(
      ObDASBaseCtDef *root_ctdef,
      common::ObIArray<AsyncFtsDomainRewriteAction> &rewrite_actions,
      bool &has_unowned_sync_filter,
      ObDASBaseCtDef *&normalized_ctdef);
  int get_async_fts_wrapper_children_(
      ObDASBaseCtDef *wrapper_ctdef,
      ObDASBaseCtDef *&query_ctdef,
      ObDASScalarScanCtDef *&delete_scan_ctdef) const;
  int build_async_fts_domain_wrapper(ObDASBaseCtDef *query_ctdef,
                                     ObDASScalarScanCtDef *delete_scan_ctdef,
                                     ObDASBaseCtDef *&wrapper_ctdef);
  // Generate the scalar scan definition for the shared delete index (doc_id and ORA_ROWSCN).
  int generate_async_delete_scan_ctdef(ObLogTableScan &op,
                                       uint64_t delete_idx_tid,
                                       ObColumnRefRawExpr *delete_doc_id_column,
                                       ObRawExpr *delete_rowscn_expr,
                                       ObDASScalarScanCtDef *&delete_scan_ctdef);
  // Populate ObDASFusionCtDef::nested_rowid_exprs_ for fusion nodes that actually contain
  // a nested() subtree: the anchor search-data-index is taken from the NESTED-scope leaves
  // (the ones mark_subtree_nested_scope() flips to is_nested_scope_), NOT from any index
  // that merely owns a NESTED_KEY column in schema. When the query has no nested()
  // semantics the whole pipeline stays in BASE scope, so has_nested_paths_ stays false and
  // nested_key never becomes part of the rowid exprs.
  // MUST be called AFTER children ctdefs are generated, so that the doc_id / nested_key
  // column raw exprs have their rt_expr_ pointers populated by the leaf scalar-scan CG path.
  int populate_nested_rowid_exprs(ObLogTableScan &op,
                                  const ObFusionNode *fusion_node,
                                  ObDASFusionCtDef *fusion_ctdef);

  // Recursively flip ObDASScalarScanCtDef::is_nested_scope_ on every leaf scalar-scan
  // ctdef under `root_ctdef`. Called once per nested-query ctdef so its inner subtree's
  // leaves know to route through ObDASSortOp instead of ObDASBitmapOp at generate_op().
  int mark_subtree_nested_scope(ObDASBaseCtDef *root_ctdef);
  // Collect the shared async index delete table from a logical subtree and
  // verify that every async leaf resolves to the same table and column. Nested
  // query generation applies it after the outermost nested boundary has
  // collapsed composite rowids back to BASE scope.
  int collect_async_delete_info(
      const ObHybridSearchNodeBase *node,
      const ObAsyncIndexDeleteInfo *&async_delete_info);

  // For nested(bool(only must_not)): synthesize a full-subtree scan ctdef as the
  // required source, wrap it in a scalar ctdef, and inject it as children_[0] of
  // the boolean query ctdef. This provides the universe of (doc_id, nested_key)
  // pairs against which must_not exclusion can operate.
  int synthesize_child_domain_must_not_required(ObLogTableScan &op, ObDASBaseCtDef *root_ctdef);

  // Parent-domain counterpart of synthesize_child_domain_must_not_required.
  // For a top-level bool of shape {must_not: [ ... nested{...} ... ]} with no
  // positive clause, synthesize a full data-table scan as the parent-doc
  // universe (cloned from tsc_ctdef.scan_ctdef_ so it uses op-level parent-domain
  // rowid ObExprs), wrap it in a scalar ctdef, and inject as must[0].
  // Runtime shape after synthesis:
  //   bool → ReqExclOp
  //     ├─ required: primary-table scan (universe of parent doc_ids)
  //     └─ excluded: original must_not children (e.g. NestedOp emitting
  //        parent doc_ids where the inner query matches).
  int synthesize_parent_domain_must_not_required(ObLogTableScan &op, ObDASBaseCtDef *root_ctdef);

  // Build an ObDASScalarScanCtDef that scans the data table (parent doc_ids),
  // cloning setup from tsc_ctdef.scan_ctdef_. Sets is_primary_table_scan_ and
  // is_rowkey_order_scan_ so the runtime routes to ObDASScalarPrimaryROROp.
  int build_parent_domain_data_universe_scan(
      ObLogTableScan &op,
      const ObDASScanCtDef &src,
      ObDASScalarScanCtDef *&univ_scan);

  // Recursively find the first ObDASScalarScanCtDef leaf in the subtree.
  static ObDASScalarScanCtDef *find_first_scalar_scan_ctdef(ObDASBaseCtDef *ctdef);

  // Recursively find the first ObDASNestedQueryCtDef in the subtree, or nullptr.
  static ObDASNestedQueryCtDef *find_first_nested_query_ctdef(ObDASBaseCtDef *ctdef);

  // Detect must_not-only bool pattern in a CtDef subtree for nested-domain
  // validation. Starting at ctdef (assumed to be or to contain one or more
  // DAS_OP_BOOLEAN_QUERY nodes), returns true if any DAS_OP_BOOLEAN_QUERY
  // node in the subtree has: must_not is non-empty AND must/filter/should
  // are all empty. Used as a guard to reject unsupported nested
  // bool(only must_not) shapes with a clear error instead of runtime -4016.
  static bool subtree_contains_must_not_only_bool(ObDASBaseCtDef *ctdef);

  // used for scalar search ctdef generation
  int generate_scalar_scan_ctdef(const DASScalarCGParams &cg_params,
                                 const ObLogTableScan &op,
                                 ObDASScalarScanCtDef *&scan_ctdef);
  
  // used for vector aux table ctdef generation
  int generate_vec_aux_table_ctdef(ObLogTableScan &op,
                                   const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
                                   ObDASScanCtDef *&scan_ctdef,
                                   bool is_get);
  int generate_vec_scan_pushdown_filters(
      const ObLogTableScan &op,
      const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
      ObDASScanCtDef &scan_ctdef);

  // brute force knn has no vector index table, dim comes from the main table vector column
  int get_brute_force_vector_dim(const share::schema::ObTableSchema &data_table_schema,
                                 const ObVecSearchNode &vec_search_node,
                                 int64_t &dim);

  int try_alloc_topk_collect_ctdef(ObDASFusionCtDef *fusion_ctdef);

  // Do we need to move text retrieval cg code to an independent class
  // since the only dependency interface here is generate_scalar_scan_ctdef()?
  int generate_fulltext_query_ctdef(ObLogTableScan &op,
                                    const ObMatchQueryNode *match_node,
                                    ObDASMatchCtDef *&match_query_ctdef);

  int generate_match_phrase_query_ctdef(ObLogTableScan &op,
                                        const ObMatchPhraseQueryNode *match_phrase_node,
                                        ObDASMatchPhraseCtDef *&match_phrase_query_ctdef);

  int generate_multi_match_query_ctdef(ObLogTableScan &op,
                                       const ObMultiMatchQueryNode *multi_match_node,
                                       ObDASMultiMatchCtDef *&multi_match_query_ctdef);
  
  int generate_query_string_query_ctdef(ObLogTableScan &op,
                                        const ObQueryStringQueryNode *query_string_node,
                                        ObDASQueryStringCtDef *&query_string_query_ctdef);

  int generate_text_ir_ctdef(ObLogTableScan &op,
                             const ObTextRetrievalIndexInfo &index_info,
                             const ObFullTextQueryNode &fulltext_node,
                             const bool is_topk_query,
                             const uint64_t inv_idx_tid,
                             const uint64_t doc_id_idx_tid,
                             ObDASIRScanCtDef *&ir_scan_ctdef);

  int generate_text_ir_sub_scan_ctdef(ObLogTableScan &op,
                                      const ObTextRetrievalIndexInfo &index_info,
                                      const ObTSCIRScanType ir_scan_type,
                                      const bool need_score,
                                      const bool need_pos_list,
                                      const uint64_t inv_idx_tid,
                                      const uint64_t doc_id_idx_tid,
                                      ObDASScalarScanCtDef *&scalar_scan_ctdef);
  int generate_text_ir_stats_primary_scan_ctdef(ObLogTableScan &op,
                                                ObDASScalarScanCtDef *&scalar_scan_ctdef);

  int extract_text_ir_access_columns(const ObTextRetrievalIndexInfo &index_info,
                                     const ObTSCIRScanType ir_scan_type,
                                     const bool need_score,
                                     const bool need_pos_list,
                                     ObIArray<ObRawExpr *> &access_exprs,
                                     ObIArray<uint64_t> &output_cids,
                                     ObIArray<ObRawExpr *> &agg_exprs);

  int generate_text_ir_spec(const ObTextRetrievalIndexInfo &index_info,
                            const ObFullTextQueryNode &fulltext_node,
                            const bool is_topk_query,
                            const bool need_score,
                            const uint64_t inv_idx_tid,
                            ObDASIRScanCtDef &ir_scan_ctdef);

  int mark_text_ir_score_exprs_self_produced_(const ObTextRetrievalIndexInfo &index_info);

  int generate_block_max_scan_spec(const ObTextRetrievalIndexInfo &index_info,
                                   const uint64_t inv_idx_tid,
                                   ObDASIRScanCtDef &ir_scan_ctdef);

  int generate_avg_doc_len_est_spec(const ObTextRetrievalIndexInfo &index_info,
                                   const uint64_t inv_idx_tid,
                                   ObDASIRScanCtDef &ir_scan_ctdef);

  int append_block_max_scan_agg_column(const int64_t column_id,
                                       const ObTableSchema &table_schema,
                                       const ObSkipIndexColType skip_index_type,
                                       const ObIArray<ObColDesc> &col_descs,
                                       const ObIArray<uint64_t> &access_column_ids,
                                       ObIArray<int32_t> &block_max_scan_col_store_idxes,
                                       ObIArray<ObSkipIndexColType> &block_max_scan_col_types,
                                       ObIArray<int32_t> &block_max_scan_col_proj);

  int check_skip_index_validity(const ObTextRetrievalIndexInfo &index_info,
                                const uint64_t inv_idx_tid,
                                bool &is_valid) const;
  int check_column_ids_accessible(
    uint64_t ref_table_id, const ObSqlSchemaGuard &schema_guard, const ObIArray<uint64_t> &column_ids);

  int rewrite_for_logical_table_(ObLogTableScan &op,
                                 ObDASBaseCtDef *root_ctdef,
                                 ObDASScanCtDef &main_scan_ctdef);

  // 混搜走独立 CG，需自行写入 plan scan ns 通道，仅影响限流。
  void record_logical_scan_ns_to_plan_(const ObRawExpr *ns_const_expr);
  // D8: extract ns/lt ObExpr* from hybrid DSL implicit-PK scalar filters.
  int try_extract_lt_pk_rt_expr_from_scalar_filter_(const ObRawExpr *filter,
                                                    ObExpr *&out_ns_expr,
                                                    ObExpr *&out_lt_expr,
                                                    const bool allow_record_plan_ns);
  int collect_logical_table_ns_lt_exprs_from_node_(ObHybridSearchNodeBase *node,
                                                   ObExpr *&ns_expr,
                                                   ObExpr *&lt_expr,
                                                   const bool in_must_not);
  // Fallback: extract ns/lt from table-level WHERE filter exprs when the hybrid
  // search boolean tree lacks them (plain SQL index scan via idx_json).
  int extract_ns_lt_from_table_filters_(const ObLogTableScan &op,
                                        ObExpr *&ns_expr,
                                        ObExpr *&lt_expr);
  int inject_logical_table_ns_lt_into_main_scan_ctdef_(ObLogTableScan &op,
                                                        ObDASScanCtDef &main_scan_ctdef);

  // [LT hybrid CG] Find a skipped implicit-PK scalar node for full-scan degrade (prefer pri_table_ap_).
  int find_logical_table_implicit_pk_scalar_node_(ObHybridSearchNodeBase *node,
                                                  ObScalarQueryNode *&scalar_node);

  // [LT hybrid CG] Build Lookup -> Fusion -> Scalar -> ScalarScan(primary) for ns/lt-only degrade.
  int generate_logical_table_degrade_search_ctdef_(ObLogTableScan &op,
                                                   ObFusionNode *fusion_node,
                                                   ObDASBaseCtDef *&search_ctdef);

  // Stage 2a: rewrite all DAS_OP_SCALAR_SCAN_QUERY storage leaves (FTS-aux + non-FTS).
  int rewrite_storage_scalar_scans_(ObLogTableScan &op,
                                    ObDASBaseCtDef *root_ctdef,
                                    const HybridLtRewriteCtx &ctx);

  // Stage 2b: rewrite IVF-aux DAS_OP_TABLE_SCAN storage leaves under DAS_OP_VEC_INDEX_IVF_SCAN.
  int rewrite_storage_ivf_table_scans_(ObLogTableScan &op,
                                       ObDASBaseCtDef *root_ctdef,
                                       const HybridLtRewriteCtx &ctx);

  // Stage 2c: rewrite the TABLE_LOOKUP / INDEX_PROJ_LOOKUP main scan ctdef (when present).
  int rewrite_lookup_main_scan_(ObLogTableScan &op,
                                ObDASBaseCtDef *root_ctdef,
                                const HybridLtRewriteCtx &ctx);

  int rewrite_scan_ctdef_for_logical_table_(ObLogTableScan &op,
                                            ObDASScalarScanCtDef &scan_ctdef,
                                            ObExpr *phys_key_expr,
                                            ObExpr *phys_value_expr);
  // KV two-column + table_param + clear storage pushdown filters; optional result_output_ sync.
  // Optional extra_col_ids / extra_access_exprs are appended after [kv_key, kv_value] in a
  // single pass (used by the lookup main scan to preserve OB_HIDDEN_GROUP_IDX_COLUMN_ID).
  // Both arrays must be non-null together (or both null) and have equal size.
  int apply_logical_table_kv_access_and_table_param_(ObLogTableScan &op,
                                                     ObExpr *phys_key_expr,
                                                     ObExpr *phys_value_expr,
                                                     UIntFixedArray &access_column_ids,
                                                     ObPushdownExprSpec &pd_expr_spec,
                                                     share::schema::ObTableParam &table_param,
                                                     sql::ExprFixedArray *result_output_opt,
                                                     const common::ObIArray<uint64_t> *extra_col_ids = nullptr,
                                                     const common::ObIArray<ObExpr *> *extra_access_exprs = nullptr,
                                                     sql::ExprFixedArray *logical_residual_filters_out = nullptr,
                                                     const bool skip_kv_value = false);
  int build_hybrid_logical_decode_meta_(ObLogTableScan &op,
                                        const uint64_t logical_schema_table_id,
                                        const common::ObIArray<uint64_t> &orig_access_col_ids,
                                        const common::ObIArray<ObExpr *> &orig_access_exprs,
                                        ObLogicalTableColumnMapping &logical_column_mapping,
                                        sql::ExprFixedArray &logical_output_exprs);
  int rewrite_das_scan_ctdef_for_logical_table_ivf_aux_(ObLogTableScan &op,
                                                        ObDASScanCtDef &scan_ctdef,
                                                        ObExpr *phys_key_expr,
                                                        ObExpr *phys_value_expr);
  void collect_scan_ctdefs_(ObDASBaseCtDef *ctdef,
                            common::ObIArray<ObDASScalarScanCtDef *> &scan_ctdefs);
  void collect_vec_ivf_table_scan_ctdefs_(ObDASBaseCtDef *ctdef,
                                           const bool under_ivf,
                                           common::ObIArray<ObDASScanCtDef *> &scan_ctdefs);
  // Build logical_column_mapping_ + logical_output_exprs_ on lookup ctdef before KV rewrite
  // (align with ObTscCgService::rewrite_for_logical_table_ for plain SQL path).
  int build_hybrid_lookup_logical_decode_meta_(ObLogTableScan &op,
                                                const common::ObIArray<uint64_t> &orig_access_col_ids,
                                                const common::ObIArray<ObExpr *> &orig_access_exprs,
                                                ObDASScanCtDef &mut_lookup);
  // Build logical_column_mapping_ + logical_output_exprs_ on a primary KV scan ctdef
  // (LT_AUX_PRIMARY) for filtered BM25 stats. Both ObDASScalarScanCtDef (hybrid) and
  // ObDASScanCtDef (plain MATCH IR sub-tree) expose the same fields, so we template on CtDefT.
  template <class CtDefT>
  int build_scalar_primary_logical_decode_meta_(ObLogTableScan &op,
                                                const common::ObIArray<uint64_t> &orig_access_col_ids,
                                                const common::ObIArray<ObExpr *> &orig_access_exprs,
                                                CtDefT &mut_scan);
  // IVF aux TABLE_SCAN: probe-only (LOG) decode meta; does not fill mapping/output_exprs in dev phase.
  int build_ivf_aux_logical_decode_meta_(ObLogTableScan &op,
                                         const share::schema::ObTableSchema &aux_schema,
                                         const common::ObIArray<uint64_t> &orig_access_col_ids,
                                         const common::ObIArray<ObExpr *> &orig_access_exprs,
                                         ObDASScanCtDef &mut_scan);
  // FTS aux SCALAR_SCAN: build logical_column_mapping_ + logical_output_exprs_ for FTS inverted/forward aux.
  // Templated to accept both ObDASScalarScanCtDef (hybrid) and ObDASScanCtDef (plain MATCH IR sub-tree).
  template <class CtDefT>
  int build_fts_aux_logical_decode_meta_(
      ObLogTableScan &op,
      const share::schema::ObTableSchema &aux_schema,
      share::ObLogicalTableAuxType aux_type,
      const common::ObIArray<ObExpr *> &access_exprs,
      const common::ObIArray<uint64_t> &access_col_ids,
      CtDefT &mut_scan,
      common::ObIArray<ObExpr *> &out_logical_exprs);
  int collect_logical_fts_filter_exprs_(ObHybridSearchNodeBase *node,
                                        common::ObIArray<ObRawExpr *> &filter_exprs) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHybridSearchCgService);
  ObStaticEngineCG &cg_;
  const common::ObIArray<ObRawExpr *> *logical_fts_filter_exprs_;
  bool is_generating_nested_query_;
};

} // namespace sql
} // namespace oceanbase
#endif // DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
