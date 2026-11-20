/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_CG
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/code_generator/ob_tsc_cg_service.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/schema/ob_schema_struct.h"
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{

// post 抽出 filter 若仅含 namespace_id/ltable_id，与 boolean 层 PK 跳过语义重复，勿再 CG extracted_scalar
// （否则会走 filter_by_index_back，与 POST_ITERATIVE filter_mode 不兼容）。
static bool extracted_scalar_has_non_pk_filter_(const ObScalarQueryNode *scalar_node)
{
  bool has_non_pk = false;
  if (OB_NOT_NULL(scalar_node)) {
    for (int64_t i = 0; !has_non_pk && i < scalar_node->filter_.count(); ++i) {
      ObRawExpr *filter = scalar_node->filter_.at(i);
      ObSEArray<ObRawExpr *, 4> col_exprs;
      if (OB_NOT_NULL(filter)
          && OB_SUCCESS == ObRawExprUtils::extract_column_exprs(filter, col_exprs)) {
        for (int64_t j = 0; !has_non_pk && j < col_exprs.count(); ++j) {
          if (col_exprs.at(j)->is_column_ref_expr()) {
            const ObColumnRefRawExpr *col =
                static_cast<const ObColumnRefRawExpr *>(col_exprs.at(j));
            if (0 != col->get_column_name().case_compare("namespace_id")
                && 0 != col->get_column_name().case_compare("ltable_id")) {
              has_non_pk = true;
            }
          }
        }
      }
    }
  }
  return has_non_pk;
}

// Whether this node is a scan leaf that runs in the NESTED rowid scope, i.e. a scalar
// search-index leaf generated underneath a nested() clause. BOTH conditions must hold:
//   1) it is lexically inside an ObNestedQueryNode subtree (`in_nested`) — exactly the
//      range that mark_subtree_nested_scope() flips to is_nested_scope_, which is what
//      makes the runtime enter its registered nested scope;
//   2) the leaf itself carries enclosing_nested_paths_ (set by the DSL resolver when the
//      predicate is resolved under a nested() clause).
// Only such leaves materialize the search data index's nested_key column, so only they
// may act as the anchor of the fusion ctdef's nested_rowid_exprs_.
static bool is_nested_scope_search_leaf(const ObIndexMergeNode *node, const bool in_nested)
{
  bool bret = false;
  if (in_nested && OB_NOT_NULL(node) && node->is_hybrid_scalar_node()
      && OB_NOT_NULL(node->ap_) && node->ap_->is_search_index_path()) {
    bret = !static_cast<const ObScalarQueryNode *>(node)->enclosing_nested_paths_.empty();
  }
  return bret;
}

// Collect the (deduped) search_data_index_id of every NESTED-scope scan leaf under the
// given subtree (DFS). An empty result means the query has NO nested() semantics at all,
// in which case the whole fusion pipeline stays in BASE scope and nested_key must NOT be
// added to the rowid exprs — regardless of how the referenced search indexes look like in
// schema (a mixed table may well own a nested-capable index that this query does not use
// in nested scope).
// Traversal covers children_ plus the links that are NOT mirrored into children_:
//   - bool clause arrays and the nested inner_node_ are appended to children_ by
//     move_bool_nodes_to_children(), but only when the index-merge path is taken, so walk
//     them when children_ is empty (walking both would traverse the subtree twice);
//   - a vector node's filter_nodes_ / extracted_scalar_node_ are never in children_.
static void collect_nested_scope_index_ids(const ObIndexMergeNode *node,
                                           const bool in_nested,
                                           ObIArray<uint64_t> &ids)
{
  if (OB_NOT_NULL(node)) {
    const bool child_in_nested =
        in_nested || (node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY);
    if (is_nested_scope_search_leaf(node, in_nested)) {
      const uint64_t id = node->ap_->search_data_index_id_;
      if (common::OB_INVALID_ID != id && !common::has_exist_in_array(ids, id)) {
        (void)ids.push_back(id);
      }
    }
    for (int64_t i = 0; i < node->children_.count(); ++i) {
      collect_nested_scope_index_ids(node->children_.at(i), child_in_nested, ids);
    }
    if (node->children_.empty() && node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      const ObBooleanQueryNode *bn = static_cast<const ObBooleanQueryNode *>(node);
      const ObIArray<ObIndexMergeNode *> *clause_arrays[] = {
        &bn->must_nodes_, &bn->should_nodes_, &bn->filter_nodes_, &bn->must_not_nodes_
      };
      for (int64_t a = 0; a < ARRAYSIZEOF(clause_arrays); ++a) {
        const ObIArray<ObIndexMergeNode *> &clause_nodes = *clause_arrays[a];
        for (int64_t i = 0; i < clause_nodes.count(); ++i) {
          collect_nested_scope_index_ids(clause_nodes.at(i), child_in_nested, ids);
        }
      }
    } else if (node->children_.empty() && node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
      const ObNestedQueryNode *nq = static_cast<const ObNestedQueryNode *>(node);
      collect_nested_scope_index_ids(nq->inner_node_, true, ids);
    } else if (node->node_type_ == INDEX_MERGE_HYBRID_KNN) {
      const ObVecSearchNode *vn = static_cast<const ObVecSearchNode *>(node);
      for (int64_t i = 0; i < vn->filter_nodes_.count(); ++i) {
        collect_nested_scope_index_ids(vn->filter_nodes_.at(i), child_in_nested, ids);
      }
      collect_nested_scope_index_ids(vn->extracted_scalar_node_, child_in_nested, ids);
    }
  }
}

// Validate the collected NESTED-scope index ids and return the anchor whose
// [doc_id, nested_key] pair defines the fusion pipeline's NESTED rowid scope.
// Every NESTED-scope leaf must sit on a search data index carrying nested_key: the
// optimizer already guarantees this (check_nested_scalar_index_candidate), so a violation
// is an internal error — reporting it here is far better than leaving
// the registered nested scope expressions null for ObDASNestedQueryOp to dereference.
static int check_and_pick_nested_scope_anchor(ObSqlSchemaGuard *schema_guard,
                                              const ObIArray<uint64_t> &ids,
                                              uint64_t &anchor)
{
  int ret = OB_SUCCESS;
  anchor = common::OB_INVALID_ID;
  if (OB_ISNULL(schema_guard) || OB_UNLIKELY(ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ids.count(); ++i) {
    const share::schema::ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(schema_guard->get_table_schema(ids.at(i), index_schema))) {
      LOG_WARN("failed to get search data index schema", K(ret), K(ids.at(i)));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search data index schema is null", K(ret), K(ids.at(i)));
    } else if (OB_ISNULL(
        share::ObSearchIndexBuilderUtil::get_nested_key_column_schema(*index_schema))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested scope leaf sits on a search data index without nested_key column",
               K(ret), K(ids.at(i)), K(index_schema->get_rowkey_info().get_size()),
               K(index_schema->get_column_count()));
    }
  }
  if (OB_SUCC(ret)) {
    anchor = ids.at(0);
    if (ids.count() > 1) {
      // The NESTED scope owns a single rowid meta, so leaves on different search data
      // indexes cannot share it: their storage-materialized exprs differ from the anchor's.
      LOG_TRACE("multiple search data indexes in nested scope, only the "
                   "anchor rowid exprs are registered", K(ids), K(anchor));
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObFusionNode *fusion_node, ObDASFusionCtDef *&fusion_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASFusionCtDef *new_fusion_ctdef = nullptr;
  ObArray<ObExpr *> rowkey_exprs;
  ObExpr *size_expr = nullptr;
  ObExpr *offset_expr = nullptr;
  ObExpr *window_size_expr = nullptr;
  ObExpr *rank_constant_expr = nullptr;
  ObExpr *min_score_expr = nullptr;
  ObExpr *rerank_window_size_expr = nullptr;
  ObSEArray<ObExpr*, 8> result_output_exprs;
  ObSEArray<ObExpr*, 8> score_exprs;
  ObSEArray<ObExpr*, 8> weight_exprs;
  ObSEArray<ObExpr*, 8> path_top_k_limit_exprs;

  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(fusion_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan or fusion node is null", K(ret), KP(cg_.phy_plan_), KP(fusion_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_FUSION_QUERY, cg_.phy_plan_->get_allocator(), new_fusion_ctdef))) {
    LOG_WARN("failed to allocate fusion ctdef", K(ret));
  } else if (OB_ISNULL(new_fusion_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion ctdef is null", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_rowkey_exprs(), rowkey_exprs))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
  } else if (OB_FAIL(result_output_exprs.assign(rowkey_exprs))) {
    LOG_WARN("failed to assign rowkey exprs to result_output", K(ret));
  } else if (OB_NOT_NULL(fusion_node->size_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->size_, size_expr))) {
    LOG_WARN("failed to generate size rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->from_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->from_, offset_expr))) {
    LOG_WARN("failed to generate offset rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->window_size_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->window_size_, window_size_expr))) {
    LOG_WARN("failed to generate window size rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->min_score_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->min_score_, min_score_expr))) {
    LOG_WARN("failed to generate min score rt expr", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->path_top_k_limit_, path_top_k_limit_exprs))) {
    LOG_WARN("failed to generate path top k limit exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->weight_cols_,weight_exprs))) {
    LOG_WARN("failed to generate weight calc exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->score_cols_, score_exprs))) {
    LOG_WARN("failed to generate score calc exprs", K(ret));
  } else if (fusion_node->has_rerank() && OB_FAIL(cg_.generate_rt_expr(*fusion_node->rerank_info_.rank_window_size_, rerank_window_size_expr))) {
    LOG_WARN("failed to generate ai rerank window size rt expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < score_exprs.count(); ++i) {
      if (OB_FAIL(result_output_exprs.push_back(score_exprs.at(i)))) {
        LOG_WARN("failed to push back score expr to result_output", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && fusion_node->method_ == ObFusionMethod::RRF && OB_FAIL(cg_.generate_rt_expr(*fusion_node->rank_const_, rank_constant_expr))) {
      LOG_WARN("failed to generate rank constant rt expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cg_.opt_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx is null", K(ret));
  } else {
    const bool enable_parallel = cg_.opt_ctx_->is_enable_hybrid_search_parallel_execution();
    const int64_t query_dop = enable_parallel ? fusion_node->query_dop_ : 1;
    if (OB_FAIL(new_fusion_ctdef->init(
        fusion_node->search_index_,
        fusion_node->has_search_subquery_,
        fusion_node->has_vector_subquery_,
        fusion_node->is_top_k_query_,
        fusion_node->method_,
        fusion_node->has_hybrid_fusion_op_,
        fusion_node->get_is_single_partition(),
        enable_parallel,
        query_dop,
        size_expr,
        offset_expr,
        window_size_expr,
        rerank_window_size_expr,
        rank_constant_expr,
        min_score_expr,
        rowkey_exprs,
        score_exprs,
        result_output_exprs,
        weight_exprs,
        path_top_k_limit_exprs,
        fusion_node->track_score_,
        fusion_node->fusion_iter_exec_mode_))) {
      LOG_WARN("failed to init fusion ctdef", K(ret));
    } else {
      fusion_ctdef = new_fusion_ctdef;
    }
  }

  return ret;
}

int ObHybridSearchCgService::populate_nested_rowid_exprs(ObLogTableScan &op,
                                                         const ObFusionNode *fusion_node,
                                                         ObDASFusionCtDef *fusion_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fusion_node) || OB_ISNULL(fusion_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion node or fusion ctdef is null", K(ret), KP(fusion_node), KP(fusion_ctdef));
  } else {
    ObOptimizerContext *opt_ctx = cg_.opt_ctx_;
    ObSqlSchemaGuard *schema_guard =
        OB_NOT_NULL(opt_ctx) ? opt_ctx->get_sql_schema_guard() : nullptr;
    ObLogPlan *plan = op.get_plan();
    // Whether the NESTED rowid scope is needed is a property of the QUERY, not of the
    // referenced index schemas: only leaves generated under a nested() clause run in
    // a nested scope and materialize nested_key. A mixed table may own a nested-capable
    // search index while this query never queries it in nested scope — in that case
    // nested_key must not become part of the rowid exprs. So collect the anchor strictly
    // from the NESTED-scope leaves; an empty result means "no nested key in this query".
    uint64_t search_data_index_id = common::OB_INVALID_ID;
    ObSEArray<uint64_t, 4> nested_scope_index_ids;
    const share::schema::ObTableSchema *data_index_schema = nullptr;
    collect_nested_scope_index_ids(fusion_node, false /*in_nested*/, nested_scope_index_ids);
    if (nested_scope_index_ids.empty()) {
      // No nested() subtree with a search-index leaf in this fusion subtree: the whole
      // pipeline stays in BASE scope, keep has_nested_paths_ = false. Covers plain
      // search / pure-vector / non-search paths as well.
      LOG_TRACE("no nested scope leaf in fusion subtree, skip nested rowid exprs");
    } else if (OB_ISNULL(opt_ctx) || OB_ISNULL(schema_guard) || OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("opt ctx / schema guard / plan is null", K(ret), KP(opt_ctx), KP(plan));
    } else if (OB_FAIL(check_and_pick_nested_scope_anchor(schema_guard, nested_scope_index_ids,
                                                          search_data_index_id))) {
      LOG_WARN("failed to pick nested scope anchor", K(ret), K(nested_scope_index_ids));
    } else if (OB_FAIL(schema_guard->get_table_schema(search_data_index_id, data_index_schema))) {
      LOG_WARN("failed to get search data index schema", K(ret), K(search_data_index_id));
    } else if (OB_ISNULL(data_index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search data index schema is null", K(ret), K(search_data_index_id));
    } else {
      // The anchor is guaranteed to carry nested_key by check_and_pick_nested_scope_anchor().
      // Synchronous search indexes keep it in the rowkey, while asynchronous search indexes
      // materialize it as a regular posting-value column.
      const common::ObRowkeyInfo &rowkey_info = data_index_schema->get_rowkey_info();
      const share::schema::ObColumnSchemaV2 *nested_key_column =
          share::ObSearchIndexBuilderUtil::get_nested_key_column_schema(*data_index_schema);
      uint64_t doc_id_col_id = common::OB_INVALID_ID;
      const uint64_t nested_key_col_id = OB_NOT_NULL(nested_key_column)
          ? nested_key_column->get_column_id()
          : common::OB_INVALID_ID;
      const ColumnItem *doc_id_item = nullptr;
      const ColumnItem *nested_key_item = nullptr;
      ObSEArray<ObRawExpr *, 2> raw_nested_rowid_exprs;
      if (OB_ISNULL(nested_key_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nested key column disappeared after anchor validation",
                 K(ret), K(search_data_index_id));
      } else if (OB_FAIL(rowkey_info.get_column_id(share::SEARCH_INDEX_DOC_ID, doc_id_col_id))) {
        LOG_WARN("failed to get doc id column id", K(ret), K(search_data_index_id));
      } else if (OB_ISNULL(doc_id_item = plan->get_column_item_by_id(search_data_index_id, doc_id_col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("doc id column item is null", K(ret), K(search_data_index_id), K(doc_id_col_id));
      } else if (OB_ISNULL(nested_key_item = plan->get_column_item_by_id(search_data_index_id, nested_key_col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nested_key column item is null", K(ret), K(search_data_index_id), K(nested_key_col_id));
      } else if (OB_ISNULL(doc_id_item->expr_) || OB_ISNULL(nested_key_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("doc_id / nested_key raw expr is null", K(ret),
                 KP(doc_id_item->expr_), KP(nested_key_item->expr_));
      } else if (OB_FAIL(raw_nested_rowid_exprs.push_back(doc_id_item->expr_))) {
        LOG_WARN("failed to push doc_id raw expr", K(ret));
      } else if (OB_FAIL(raw_nested_rowid_exprs.push_back(nested_key_item->expr_))) {
        LOG_WARN("failed to push nested_key raw expr", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_exprs(raw_nested_rowid_exprs,
                                               fusion_ctdef->nested_rowid_exprs_))) {
        LOG_WARN("failed to generate nested_rowid rt exprs", K(ret));
      } else {
        fusion_ctdef->set_has_nested_paths(true);
        LOG_TRACE("nested rowid exprs populated", K(search_data_index_id),
                  K(doc_id_col_id), K(nested_key_col_id));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObVecSearchNode *vec_search_node, ObDASVecIndexDriverCtDef *&res_vec_index_driver_ctdef)
{
  int ret = OB_SUCCESS;
  const bool is_brute_force = OB_NOT_NULL(vec_search_node)
      && vec_search_node->vec_info_.is_brute_force_scan();
  const ObVecBruteFilterStrategy brute_filter_strategy = is_brute_force
      ? vec_search_node->get_brute_filter_strategy()
      : ObVecBruteFilterStrategy::NONE;

  ObHybridSearchNodeBase *filter_node = nullptr;
  // 1. generate filter ctdef
  ObDASBaseCtDef *filter_ctdef = nullptr;
  if (vec_search_node->filter_nodes_.count() > 0) {
    if (vec_search_node->filter_nodes_.count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid filter nodes count", K(ret), K(vec_search_node->filter_nodes_.count()));
    } else {
      filter_node = static_cast<ObHybridSearchNodeBase *>(vec_search_node->filter_nodes_.at(0));
      if (OB_FAIL(generate_hybrid_search_node_ctdef(op, filter_node, filter_ctdef))) {
        LOG_WARN("failed to generate filter ctdef", K(ret));
      } else if (OB_ISNULL(filter_ctdef)) {
        if (op.is_logical_table()) {
          // [LT hybrid CG] filter 全部为隐式 PK 条件被跳过，ns/lt 由 aux scan lt_ctx 承载
          LOG_TRACE("[LT hybrid CG] vec search filter ctdef is null (logical PK filters skipped)");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("filter ctdef is null on non-logical-table", K(ret));
        }
      }
    }
  }

  // boolean 仅含隐式 PK 被跳过时 filter_ctdef 可为 null；仅当 post 抽出非 PK 业务谓词时才 CG extracted_scalar
  const bool filter_fully_degraded = op.is_logical_table()
      && vec_search_node->filter_nodes_.count() > 0
      && OB_ISNULL(filter_ctdef)
      && (OB_ISNULL(vec_search_node->extracted_scalar_node_)
          || !extracted_scalar_has_non_pk_filter_(vec_search_node->extracted_scalar_node_));
  ObDASBaseCtDef *extracted_filter_ctdef = nullptr;
  if (OB_SUCC(ret) && !is_brute_force
      && OB_NOT_NULL(vec_search_node->extracted_scalar_node_)
      && !filter_fully_degraded) {
    ObScalarQueryNode *extracted_node = vec_search_node->extracted_scalar_node_;
    if (OB_FAIL(generate_hybrid_search_node_ctdef(op, extracted_node, extracted_filter_ctdef))) {
      LOG_WARN("failed to generate extracted filter ctdef", K(ret));
    } else if (OB_ISNULL(extracted_filter_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extracted filter ctdef is null", K(ret));
    }
  }

  // 2. generate sort_expr, limit_expr, offset_expr
  ObExpr *sort_expr = nullptr;
  ObExpr *limit_expr = nullptr;
  ObExpr *offset_expr = nullptr;
  if (OB_SUCC(ret)) {
    ObRawExpr *expr = vec_search_node->get_sort_key().expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort key expr is null", K(ret));
    } else if (expr->is_vector_sort_expr()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (expr->get_param_expr(i)->is_column_ref_expr() && OB_FAIL(cg_.mark_expr_self_produced(expr->get_param_expr(i)))) {
          LOG_WARN("mark expr self produced failed", K(ret), KPC(expr->get_param_expr(i)));
        }
      }
    }
  }

  if (FAILEDx(cg_.generate_rt_expr(*vec_search_node->get_sort_key().expr_, sort_expr))) {
    LOG_WARN("generate sort expr failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*vec_search_node->get_topk_limit_expr(), limit_expr))) {
    LOG_WARN("generate limit expr failed", K(ret));
  } else if (OB_NOT_NULL(vec_search_node->get_topk_offset_expr())
             && OB_FAIL(cg_.generate_rt_expr(*vec_search_node->get_topk_offset_expr(), offset_expr))) {
    LOG_WARN("generate offset expr failed", K(ret));
  }
  
  // 3. generate vec index scan ctdef (HNSW or IVF)
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObDASVecIndexScanCtDef *vec_index_scan_ctdef = nullptr;
  if (OB_FAIL(ret)) {
    // skip
  } else if (vec_search_node->vec_info_.is_async_ivf_scan()) {
    ObDASVecIndexAsyncIVFScanCtDef *async_ctdef = nullptr;
    ObDASScanCtDef *index_ctdef = nullptr;
    ObDASScalarScanCtDef *delete_ctdef = nullptr;
    ObVecSearchNode::VecIndexScanParams params;
    params.table_id_ = vec_search_node->main_index_table_id_;
    params.scan_type_ = OB_VEC_ASYNC_IVF_INDEX_SCAN;
    const ObAsyncIndexDeleteInfo &deletion = vec_search_node->async_delete_info_;
    for (int64_t i = 0; OB_SUCC(ret) && i < vec_search_node->vec_info_.aux_table_column_.count(); ++i) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_search_node->vec_info_.aux_table_column_.at(i)))) {
        LOG_WARN("failed to prepare async IVF index expressions", K(ret));
      }
    }
    if (!deletion.is_valid()) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(ret)) { /* Keep expression preparation failure. */ }
    else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN,
        ctdef_alloc, async_ctdef))) {
      LOG_WARN("failed to allocate async IVF definition", K(ret));
    } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, params, index_ctdef, false))) {
      LOG_WARN("failed to generate async IVF index scan", K(ret));
    } else if (OB_FAIL(generate_async_delete_scan_ctdef(op, deletion.get_delete_idx_tid(),
        deletion.get_delete_doc_id_column(), deletion.get_delete_rowscn_expr(), delete_ctdef))) {
      LOG_WARN("failed to generate async IVF deletion scan", K(ret));
    } else if (OB_ISNULL(async_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &ctdef_alloc, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      async_ctdef->children_cnt_ = 2;
      async_ctdef->children_[0] = index_ctdef;
      async_ctdef->children_[1] = delete_ctdef;
      vec_index_scan_ctdef = async_ctdef;
    }
  } else if (vec_search_node->vec_info_.is_brute_force_scan()) {
    // Brute force branch: no auxiliary vector index table, the only child scans the main
    // table to feed the distance expression.
    ObDASVecIndexBruteScanCtDef *brute_scan_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_BRUTE_SCAN, ctdef_alloc, brute_scan_ctdef))) {
      LOG_WARN("allocate vec index brute scan ctdef failed", K(ret));
    } else {
      const int64_t brute_child_cnt = 1;
      ObDASScanCtDef *main_table_ctdef = nullptr;
      ObVecSearchNode::VecIndexScanParams main_table_params;
      if (OB_FAIL(vec_search_node->get_brute_force_main_table_params(main_table_params))) {
        LOG_WARN("failed to get brute force main table params", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, main_table_params, main_table_ctdef, false /*is_get*/))) {
        LOG_WARN("failed to generate brute force main table ctdef", K(ret));
      } else if ((ObVecBruteFilterStrategy::PUSHDOWN_ONLY == brute_filter_strategy
                  || ObVecBruteFilterStrategy::PUSHDOWN_WITH_BITMAP == brute_filter_strategy)
                 && main_table_params.pushdown_filters_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("brute force pushdown strategy has no pushdown filter", K(ret),
                 K(static_cast<int64_t>(brute_filter_strategy)));
      } else if (OB_ISNULL(brute_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, brute_child_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate vec index brute scan ctdef children failed", K(ret));
      } else {
        brute_scan_ctdef->children_cnt_ = brute_child_cnt;
        brute_scan_ctdef->children_[ObDASVecIndexBruteScanCtDef::MAIN_TABLE_IDX] = main_table_ctdef;
        brute_scan_ctdef->filter_pushdown_ = !main_table_params.pushdown_filters_.empty();
        vec_index_scan_ctdef = brute_scan_ctdef;
      }
    }
  } else if (vec_search_node->vec_info_.is_hnsw_vec_scan()) {
    // HNSW branch
    ObDASVecIndexHNSWScanCtDef *hnsw_scan_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_HNSW_SCAN, ctdef_alloc, hnsw_scan_ctdef))) {
      LOG_WARN("allocate vec index hnsw scan ctdef failed", K(ret));
    } else {
      int64_t vec_child_task_cnt = 4;
      ObDASScanCtDef *delta_buf_table_ctdef = nullptr;
      ObDASScanCtDef *index_id_table_ctdef = nullptr;
      ObDASScanCtDef *snapshot_table_ctdef = nullptr;
      ObDASScanCtDef *com_aux_vec_table_ctdef = nullptr;
      ObVecSearchNode::VecIndexScanParams delta_buf_table_params;
      ObVecSearchNode::VecIndexScanParams index_id_table_params;
      ObVecSearchNode::VecIndexScanParams snapshot_table_params;
      ObVecSearchNode::VecIndexScanParams com_aux_vec_table_params;

      if (OB_FAIL(vec_search_node->get_delta_buf_table_params(delta_buf_table_params))) {
        LOG_WARN("failed to get delta buf table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_index_id_table_params(index_id_table_params))) {
        LOG_WARN("failed to get index id table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_snapshot_table_params(snapshot_table_params))) {
        LOG_WARN("failed to get snapshot table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_com_aux_vec_table_params(com_aux_vec_table_params))) {
        LOG_WARN("failed to get com aux vec table params", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, delta_buf_table_params, delta_buf_table_ctdef, false))) {
        LOG_WARN("failed to generate delta buf table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, index_id_table_params, index_id_table_ctdef, false))) {
        LOG_WARN("failed to generate index id table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, snapshot_table_params, snapshot_table_ctdef, false))) {
        LOG_WARN("failed to generate snapshot table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, com_aux_vec_table_params, com_aux_vec_table_ctdef, true))) {
        LOG_WARN("failed to generate com aux vec table ctdef", K(ret));
      } else if (OB_ISNULL(hnsw_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, vec_child_task_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate vec index hnsw scan ctdef children failed", K(ret));
      } else {
        hnsw_scan_ctdef->children_cnt_ = vec_child_task_cnt;
        hnsw_scan_ctdef->children_[0] = delta_buf_table_ctdef;
        hnsw_scan_ctdef->children_[1] = index_id_table_ctdef;
        hnsw_scan_ctdef->children_[2] = snapshot_table_ctdef;
        hnsw_scan_ctdef->children_[3] = com_aux_vec_table_ctdef;
        vec_index_scan_ctdef = hnsw_scan_ctdef;
      }
    }
  } else if (vec_search_node->vec_info_.is_ivf_vec_scan()) {
    // IVF branch (IVF_FLAT / IVF_PQ)
    ObDASVecIndexIVFScanCtDef *ivf_scan_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_IVF_SCAN, ctdef_alloc, ivf_scan_ctdef))) {
      LOG_WARN("allocate vec index ivf scan ctdef failed", K(ret));
    } else {
      bool is_ivf_pq = vec_search_node->vec_info_.is_ivf_pq_scan();
      int64_t ivf_child_cnt = is_ivf_pq ? 5 : 4;  // IVF_PQ has special_aux (pq_id)
      ObDASScanCtDef *centroid_ctdef = nullptr;
      ObDASScanCtDef *cid_vec_ctdef = nullptr;
      ObDASScanCtDef *rowkey_cid_ctdef = nullptr;
      ObDASScanCtDef *com_aux_ctdef = nullptr;
      ObDASScanCtDef *special_aux_ctdef = nullptr;
      ObVecSearchNode::VecIndexScanParams centroid_params;
      ObVecSearchNode::VecIndexScanParams cid_vec_params;
      ObVecSearchNode::VecIndexScanParams rowkey_cid_params;
      ObVecSearchNode::VecIndexScanParams com_aux_params;
      ObVecSearchNode::VecIndexScanParams special_aux_params;

      if (OB_FAIL(vec_search_node->get_centroid_table_params(centroid_params))) {
        LOG_WARN("failed to get centroid table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_cid_vec_table_params(cid_vec_params))) {
        LOG_WARN("failed to get cid vec table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_rowkey_cid_table_params(rowkey_cid_params))) {
        LOG_WARN("failed to get rowkey cid table params", K(ret));
      } else if (OB_FAIL(vec_search_node->get_com_aux_vec_table_params(com_aux_params))) {
        LOG_WARN("failed to get com aux table params", K(ret));
      } else if (is_ivf_pq && OB_FAIL(vec_search_node->get_special_aux_table_params(special_aux_params))) {
        LOG_WARN("failed to get special aux table params", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, centroid_params, centroid_ctdef, false))) {
        LOG_WARN("failed to generate centroid table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, cid_vec_params, cid_vec_ctdef, false))) {
        LOG_WARN("failed to generate cid vec table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, rowkey_cid_params, rowkey_cid_ctdef, false))) {
        LOG_WARN("failed to generate rowkey cid table ctdef", K(ret));
      } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, com_aux_params, com_aux_ctdef, true))) {
        LOG_WARN("failed to generate com aux table ctdef", K(ret));
      } else if (is_ivf_pq && OB_FAIL(generate_vec_aux_table_ctdef(op, special_aux_params, special_aux_ctdef, false))) {
        LOG_WARN("failed to generate special aux table ctdef", K(ret));
      } else if (OB_ISNULL(ivf_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, ivf_child_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate vec index ivf scan ctdef children failed", K(ret));
      } else {
        ivf_scan_ctdef->children_cnt_ = ivf_child_cnt;
        ivf_scan_ctdef->children_[ObDASVecIndexIVFScanCtDef::CENTROID_TABLE_IDX] = centroid_ctdef;
        ivf_scan_ctdef->children_[ObDASVecIndexIVFScanCtDef::CID_VEC_TABLE_IDX] = cid_vec_ctdef;
        ivf_scan_ctdef->children_[ObDASVecIndexIVFScanCtDef::ROWKEY_CID_TABLE_IDX] = rowkey_cid_ctdef;
        ivf_scan_ctdef->children_[ObDASVecIndexIVFScanCtDef::COM_AUX_TABLE_IDX] = com_aux_ctdef;
        if (is_ivf_pq) {
          ivf_scan_ctdef->children_[ObDASVecIndexIVFScanCtDef::SPECIAL_AUX_TABLE_IDX] = special_aux_ctdef;
        }
        vec_index_scan_ctdef = ivf_scan_ctdef;
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported vec index algorithm type in hybrid search", K(ret),
             K(vec_search_node->get_algorithm_type()));
  }

  // 4. set vector query param for vec index scan ctdef
  int64_t dim = 0;
  const bool is_async_ivf = vec_search_node->vec_info_.is_async_ivf_scan();
  const ObColumnSchemaV2 *source_column = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *main_index_table_schema = nullptr;
  ObTableID data_table_id = vec_search_node->get_data_table_id();
  ObTableID main_index_table_id = vec_search_node->main_index_table_id_;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  if (FAILEDx(schema_guard->get_table_schema(data_table_id, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard->get_table_schema(main_index_table_id, main_index_table_schema))) {
    LOG_WARN("get main index table schema failed", K(ret), K(main_index_table_id));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(main_index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (is_async_ivf && (nullptr == vec_search_node->field_
      || nullptr == (source_column = data_table_schema->get_column_schema(vec_search_node->field_->get_column_id())))) {
    ret = OB_SCHEMA_ERROR;
  } else if (is_async_ivf && OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(
      source_column->get_extended_type_info(), dim))) {
    LOG_WARN("failed to resolve async IVF vector dimension", K(ret));
  } else if (vec_search_node->vec_info_.is_brute_force_scan()
             && OB_FAIL(get_brute_force_vector_dim(*data_table_schema, *vec_search_node, dim))) {
    LOG_WARN("fail to get brute force vector column dim", K(ret));
  } else if (!is_async_ivf
             && !vec_search_node->vec_info_.is_brute_force_scan()
             && OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(
                    *main_index_table_schema, *data_table_schema, dim))) {
    LOG_WARN("fail to get vector_index_column dim", K(ret));
  } else if (OB_FAIL(ob_write_string(ctdef_alloc, main_index_table_schema->get_index_params(),
                                     vec_index_scan_ctdef->vec_index_param_))) {
    LOG_WARN("fail to write index params", K(ret));
  } else if (OB_FAIL(vec_index_scan_ctdef->query_param_.assign(vec_search_node->get_query_param()))) {
    LOG_WARN("fail to assign query param", K(ret));
  } else {
    vec_index_scan_ctdef->vector_index_param_ = vec_search_node->get_vector_index_param();
    vec_index_scan_ctdef->algorithm_type_ = vec_search_node->get_algorithm_type();
    vec_index_scan_ctdef->vec_type_ = vec_search_node->get_vec_type();
    vec_index_scan_ctdef->dim_ = dim;
    vec_index_scan_ctdef->sort_expr_ = sort_expr;
  }

  // 5. generate vec index driver ctdef
  ObDASVecIndexDriverCtDef *vec_index_driver_ctdef = nullptr;
  ObDASAsyncIndexExecMode filter_exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ObDASBaseCtDef *driver_filter_ctdef = filter_ctdef;
  if (is_brute_force
      && ObVecBruteFilterStrategy::PUSHDOWN_ONLY == brute_filter_strategy) {
    // The complete scalar predicate is already in the brute-force main-table scan.
    // Keep the logical filter tree for pruning/explain, but omit its physical bitmap child.
    driver_filter_ctdef = nullptr;
  }
  if (OB_SUCC(ret)
      && ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(driver_filter_ctdef)
      && OB_FAIL(finalize_query_root_(op, filter_node, driver_filter_ctdef, filter_exec_mode))) {
    LOG_WARN("failed to finalize KNN filter", K(ret));
  }
  int64_t vec_index_driver_child_cnt = 1;
  if (OB_NOT_NULL(driver_filter_ctdef)) {
    vec_index_driver_child_cnt++;
  }
  if (OB_NOT_NULL(extracted_filter_ctdef)) {
    vec_index_driver_child_cnt++;
  }

  if (FAILEDx(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_DRIVER, ctdef_alloc, vec_index_driver_ctdef))) {
    LOG_WARN("allocate vec index driver ctdef failed", K(ret));
  } else if (OB_ISNULL(vec_index_driver_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, vec_index_driver_child_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate vec index driver ctdef children failed", K(ret));
  } else {
    vec_index_driver_ctdef->children_cnt_ = vec_index_driver_child_cnt;
    int child_idx = 1;
    vec_index_driver_ctdef->children_[0] = vec_index_scan_ctdef;
    if (OB_NOT_NULL(driver_filter_ctdef)) {
      vec_index_driver_ctdef->children_[child_idx++] = driver_filter_ctdef;
    }
    if (OB_NOT_NULL(extracted_filter_ctdef)) {
      vec_index_driver_ctdef->children_[child_idx++] = extracted_filter_ctdef;
    }
    vec_index_driver_ctdef->sort_expr_ = sort_expr;
    vec_index_driver_ctdef->limit_expr_ = limit_expr;
    vec_index_driver_ctdef->offset_expr_ = offset_expr;
  }

  // 6. set vec index param for vec index driver ctdef
  if (FAILEDx(ob_write_string(ctdef_alloc, main_index_table_schema->get_index_params(), vec_index_driver_ctdef->vec_index_param_))) {
    LOG_WARN("fail to write index params", K(ret));
  } else if (OB_FAIL(vec_index_driver_ctdef->query_param_.assign(vec_search_node->get_query_param()))) {
    LOG_WARN("fail to assign query param", K(ret));
  } else {
    vec_index_driver_ctdef->vector_index_param_ = vec_search_node->get_vector_index_param();
    vec_index_driver_ctdef->algorithm_type_ = vec_search_node->get_algorithm_type();
    vec_index_driver_ctdef->vec_type_ = vec_search_node->get_vec_type();
    vec_index_driver_ctdef->use_async_knn_pre_filter_group_alignment_ =
        vec_index_driver_ctdef->need_pre_filter_iter()
        && ASYNC_INDEX_EXEC_GROUP == filter_exec_mode;
    vec_index_driver_ctdef->row_count_ = vec_search_node->get_row_count();
    vec_index_driver_ctdef->dim_ = dim;
    if (vec_search_node->search_option_ != nullptr) {
      vec_index_driver_ctdef->filter_mode_ = vec_search_node->search_option_->filter_mode_;
    }
  }

  // 7. generate vec iter output
  ObSEArray<ObExpr *, 4> result_output;
  ObArray<ObRawExpr *> rowkey_exprs;
  if (FAILEDx(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
    LOG_WARN("failed to assign rowkey exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, result_output))) {
    LOG_WARN("failed to generate main table rowkey exprs", K(ret));
  } else if (OB_FAIL(vec_index_scan_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  } else if (OB_FAIL(vec_index_driver_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  } else if (OB_NOT_NULL(driver_filter_ctdef)) {
    const ObDASAttachCtDef *attach_filter_ctdef = static_cast<const ObDASAttachCtDef*>(driver_filter_ctdef);
    if (OB_FAIL(append_array_no_dup(vec_index_driver_ctdef->result_output_, attach_filter_ctdef->result_output_))) {
      LOG_WARN("failed to append filter output exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    res_vec_index_driver_ctdef = vec_index_driver_ctdef;
  }

  return ret;
}

int ObHybridSearchCgService::generate_vec_aux_table_ctdef(ObLogTableScan &op,
                                                          const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
                                                          ObDASScanCtDef *&scan_ctdef,
                                                          bool is_get)
{
  int ret = OB_SUCCESS;
  const ObTableID &index_table_id = vec_index_scan_params.table_id_;
  const ObTableID &data_table_id = op.get_ref_table_id();
  ObDASScanCtDef *aux_ctdef = nullptr;
  ObOptimizerContext *opt_ctx = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObBasicSessionInfo *session_info = nullptr;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  
  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, aux_ctdef))) {
    LOG_WARN("allocate delta buf table ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aux ctdef is null", K(ret));
  } else if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
             OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard()) ||
             OB_ISNULL(session_info = opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx, schema guard or session info is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(index_table_id, aux_ctdef->schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(index_table_id));
  } else {
    aux_ctdef->ref_table_id_ = index_table_id;
    aux_ctdef->ir_scan_type_ = vec_index_scan_params.scan_type_;
    aux_ctdef->is_get_ = is_get;
  }

  // 1. generate access
  if (OB_FAIL(ret)) {
  } else if (vec_index_scan_params.access_exprs_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access exprs is empty", K(ret));
  } else {
    ObArray<uint64_t> access_col_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < vec_index_scan_params.access_exprs_.count(); ++i) {
      ObRawExpr *expr = vec_index_scan_params.access_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_UNLIKELY(T_REF_COLUMN != expr->get_expr_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported expr type", K(ret), K(*expr));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (OB_FAIL(access_col_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("push back column id failed", K(ret));
        }
      }
    } // end for

    if (FAILEDx(cg_.mark_expr_self_produced(vec_index_scan_params.access_exprs_))) {
      LOG_WARN("mark expr self produced failed", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(vec_index_scan_params.access_exprs_, aux_ctdef->pd_expr_spec_.access_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret));
    } else if (OB_FAIL(aux_ctdef->access_column_ids_.assign(access_col_ids))) {
      LOG_WARN("assign access column ids failed", K(ret));
    }
  }

  // Logical-table vector auxiliary scans use this hybrid-search CTDef path after being
  // rewritten to physical KV scans, so they miss the standard TSC storage flags. Generate
  // the flags here to enable normal blockscan and batched-read policies without changing
  // physical-table hybrid scans. Other scans with pushdown filters initialize their
  // flags in the fallback branch before generating pushdown expressions.
  if (OB_SUCC(ret) && op.is_logical_table()) {
    ObTscCgService tsc_cg_service(cg_);
    if (OB_FAIL(tsc_cg_service.generate_pd_storage_flag(
            op,
            index_table_id,
            vec_index_scan_params.access_exprs_,
            op.get_type(),
            false,
            op.use_column_store(),
            *aux_ctdef))) {
      LOG_WARN("generate pd storage flag for vector auxiliary scan failed",
               K(ret), K(index_table_id), K(vec_index_scan_params.scan_type_));
    }
  } else if (OB_SUCC(ret)
      && !vec_index_scan_params.pushdown_filters_.empty()
      && OB_FAIL(cg_.generate_tsc_flags(op, *aux_ctdef))) {
    LOG_WARN("failed to generate vector scan tsc flags", K(ret));
  }

  // 2. generate table param
  if (OB_SUCC(ret)) {
    int64_t route_policy = 0;
    bool is_cs_replica_query = false;
    const ObTableSchema *table_schema = nullptr;

    if (OB_FAIL(schema_guard->get_table_schema(index_table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(index_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table schema", K(ret), K(index_table_id));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info->get_effective_tenant_id()));
      if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
        LOG_WARN("get route policy failed", K(ret));
      } else {
        is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
        aux_ctdef->table_param_.set_is_partition_table(table_schema->is_partitioned_table());
        aux_ctdef->table_param_.set_is_mlog_table(table_schema->is_mlog_table());
        aux_ctdef->table_param_.set_plan_enable_rich_format(opt_ctx->get_enable_rich_vector_format());
        aux_ctdef->table_param_.get_enable_lob_locator_v2() = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
      }
    }

    if (FAILEDx(aux_ctdef->table_param_.convert(*table_schema,
                                                aux_ctdef->access_column_ids_,
                                                aux_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                &aux_ctdef->access_column_ids_,
                                                is_oracle_mapping_real_virtual_table(data_table_id),
                                                is_cs_replica_query))) {
      LOG_WARN("convert table param failed", K(ret), K(data_table_id), K(index_table_id));
    }
  }

  // 3. generate result output
  const ExprFixedArray &access_exprs = aux_ctdef->pd_expr_spec_.access_exprs_;
  if (FAILEDx(aux_ctdef->result_output_.init(access_exprs.count()))) {
    LOG_WARN("init result output failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(aux_ctdef->result_output_, access_exprs))) {
    LOG_WARN("append array no dup failed", K(ret));
  } else if (!vec_index_scan_params.pushdown_filters_.empty()
             && OB_FAIL(generate_vec_scan_pushdown_filters(
                    op, vec_index_scan_params, *aux_ctdef))) {
    LOG_WARN("failed to generate vector scan pushdown filters", K(ret), K(index_table_id));
  }

  if (OB_SUCC(ret)) {
    scan_ctdef = aux_ctdef;
  }

  return ret;
}

int ObHybridSearchCgService::generate_vec_scan_pushdown_filters(
    const ObLogTableScan &op,
    const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
    ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  if (vec_index_scan_params.pushdown_filters_.empty()) {
    // Nothing to generate. Vector-index auxiliary scans keep their existing shape.
  } else if (vec_index_scan_params.access_exprs_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector scan access exprs are empty", K(ret));
  } else {
    bool pd_blockscan =
        scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_blockscan_pushdown();
    bool pd_filter = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_filter_pushdown();
    bool enable_base_skip_index =
        scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_apply_base_skip_index();
    bool enable_inc_skip_index =
        scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_apply_inc_skip_index();
    const bool enable_column_store =
        scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_use_column_store();

    if (!pd_blockscan) {
      pd_filter = false;
    } else {
      for (int64_t i = 0;
           OB_SUCC(ret) && (pd_blockscan || pd_filter)
               && i < vec_index_scan_params.access_exprs_.count();
           ++i) {
        ObRawExpr *expr = vec_index_scan_params.access_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vector scan access expr is null", K(ret), K(i));
        } else if ((enable_column_store && T_ORA_ROWSCN == expr->get_expr_type())
                   || T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type()
                   || T_PSEUDO_OLD_NEW_COL == expr->get_expr_type()) {
          pd_blockscan = false;
          pd_filter = false;
        } else if (expr->is_column_ref_expr()) {
          const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
          if (col_expr->is_lob_column()
              && cg_.get_cur_cluster_version() < CLUSTER_VERSION_4_1_0_0) {
            pd_filter = false;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      enable_base_skip_index = enable_base_skip_index && pd_filter;
      enable_inc_skip_index = enable_inc_skip_index && pd_filter;
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(pd_filter);
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(enable_base_skip_index);
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(enable_inc_skip_index);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cg_.generate_rt_exprs(
                   vec_index_scan_params.pushdown_filters_,
                   scan_ctdef.pd_expr_spec_.pushdown_filters_))) {
      LOG_WARN("failed to generate vector scan runtime filters", K(ret));
    } else if (scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
      ObPushdownFilterConstructor filter_constructor(
          &cg_.phy_plan_->get_allocator(), cg_, &op.get_filter_monotonicity(),
          scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_use_column_store(),
          op.get_table_type(), scan_ctdef.table_param_.is_enable_semistruct_encoding());
      if (OB_FAIL(filter_constructor.apply(
              vec_index_scan_params.pushdown_filters_,
              scan_ctdef.pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to construct vector scan storage filter", K(ret));
      } else if (OB_FAIL(scan_ctdef.table_param_.check_is_safe_filter_with_di(
                     vec_index_scan_params.pushdown_filters_,
                     *scan_ctdef.pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to check vector scan storage filter safety", K(ret));
      }
    }
    LOG_TRACE("generated vector scan pushdown filters", K(ret),
              K(scan_ctdef.ref_table_id_), K(vec_index_scan_params.pushdown_filters_),
              K(scan_ctdef.pd_expr_spec_.pd_storage_flag_));
  }
  return ret;
}

int ObHybridSearchCgService::get_brute_force_vector_dim(const ObTableSchema &data_table_schema,
                                                        const ObVecSearchNode &vec_search_node,
                                                        int64_t &dim)
{
  int ret = OB_SUCCESS;
  dim = 0;
  const ObColumnRefRawExpr *vec_column = vec_search_node.vec_info_.target_vec_column_;
  const ObColumnSchemaV2 *col_schema = nullptr;
  bool is_sparse_vec = false;
  if (OB_ISNULL(vec_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("brute force vector column is null", K(ret));
  } else if (OB_ISNULL(col_schema = data_table_schema.get_column_schema(vec_column->get_column_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector column schema not found", K(ret), K(vec_column->get_column_id()));
  } else if (OB_FAIL(ObVectorIndexUtil::is_sparse_vec_col(col_schema->get_extended_type_info(), is_sparse_vec))) {
    LOG_WARN("fail to check sparse vector column", K(ret));
  } else if (is_sparse_vec) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "knn brute force search on sparse vector column");
    LOG_WARN("brute force knn does not support sparse vector column", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(),
                                                                             dim))) {
    LOG_WARN("fail to get vector dim from extend type info", K(ret));
  } else if (dim <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector column dim", K(ret), K(dim));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
