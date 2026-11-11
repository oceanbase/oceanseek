/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_hybrid_search_node.h"
#include "ob_fulltext_search_node.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/ob_json_access_utils.h"
#include "lib/udt/ob_array_type.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/ob_cluster_version.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase;
using namespace sql;

int ObAsyncIndexDeleteInfo::resolve_delete_index(
    const share::schema::ObTableSchema &main_table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::schema::ObAuxTableMetaInfo, 4> index_infos;
  delete_idx_tid_ = OB_INVALID_ID;
  delete_doc_id_column_ = nullptr;
  delete_rowscn_expr_ = nullptr;
  if (OB_FAIL(main_table_schema.get_simple_index_infos(index_infos))) {
    LOG_WARN("failed to get index infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count()
        && OB_INVALID_ID == delete_idx_tid_; ++i) {
      if (share::schema::is_async_delete_index(index_infos.at(i).index_type_)) {
        delete_idx_tid_ = index_infos.at(i).table_id_;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID == delete_idx_tid_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async delete index not found for async index query",
          K(ret), K(main_table_schema.get_table_id()));
    }
  }
  return ret;
}

int ObAsyncIndexDeleteInfo::generate_doc_id_column(
    const TableItem &table_item,
    ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  ObSQLSessionInfo *session_info = opt_ctx.get_session_info();
  ObSqlSchemaGuard *schema_guard = opt_ctx.get_sql_schema_guard();
  const share::schema::ObTableSchema *delete_idx_schema = nullptr;
  const share::schema::ObColumnSchemaV2 *doc_id_col_schema = nullptr;
  if (!has_delete_index()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete index tid", K(ret), K_(delete_idx_tid));
  } else if (OB_NOT_NULL(delete_doc_id_column_) && OB_NOT_NULL(delete_rowscn_expr_)) {
    // Already generated.
  } else if (OB_NOT_NULL(delete_doc_id_column_) || OB_NOT_NULL(delete_rowscn_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incomplete async delete access expressions", K(ret),
        KP(delete_doc_id_column_), KP(delete_rowscn_expr_));
  } else if (OB_ISNULL(session_info) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(session_info), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(delete_idx_tid_, delete_idx_schema))) {
    LOG_WARN("failed to get delete index schema", K(ret), K_(delete_idx_tid));
  } else if (OB_ISNULL(delete_idx_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null delete index schema", K(ret), K_(delete_idx_tid));
  } else if (OB_ISNULL(doc_id_col_schema = delete_idx_schema->get_column_schema_by_idx(0))) {
    // Delete index has exactly one rowkey column: __pk_increment(doc_id).
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null doc id column schema in delete index",
        K(ret), K_(delete_idx_tid));
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(
      expr_factory, *doc_id_col_schema, session_info, delete_doc_id_column_))) {
    LOG_WARN("failed to build delete doc id column expr", K(ret));
  } else if (OB_ISNULL(delete_doc_id_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null delete doc id column expr", K(ret));
  } else {
    delete_doc_id_column_->set_ref_id(
        table_item.table_id_, doc_id_col_schema->get_column_id());
    delete_doc_id_column_->set_column_attr(
        delete_idx_schema->get_table_name(), doc_id_col_schema->get_column_name_str());
    delete_doc_id_column_->set_database_name(table_item.database_name_);
    if (OB_FAIL(expr_factory.create_raw_expr(T_ORA_ROWSCN, delete_rowscn_expr_))) {
      LOG_WARN("failed to create delete rowscn expr", K(ret));
    } else if (OB_ISNULL(delete_rowscn_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null delete rowscn expr", K(ret));
    } else {
      delete_rowscn_expr_->set_data_type(ObIntType);
      delete_rowscn_expr_->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
      delete_rowscn_expr_->set_table_id(table_item.table_id_);
      delete_rowscn_expr_->set_table_name(delete_idx_schema->get_table_name());
      if (OB_FAIL(delete_rowscn_expr_->formalize(session_info))) {
        LOG_WARN("failed to formalize delete rowscn expr", K(ret));
      }
    }
  }
  return ret;
}

int ObAsyncIndexDeleteInfo::append_doc_id_expr(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (!has_delete_index()) {
    // Synchronous index path.
  } else if (OB_ISNULL(delete_doc_id_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete doc id column is not generated", K(ret), K_(delete_idx_tid));
  } else if (OB_ISNULL(delete_rowscn_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete rowscn expr is not generated", K(ret), K_(delete_idx_tid));
  } else if (OB_FAIL(add_var_to_array_no_dup(
      exprs, static_cast<ObRawExpr *>(delete_doc_id_column_)))) {
    LOG_WARN("failed to append delete doc id column expr", K(ret), K_(delete_idx_tid));
  } else if (OB_FAIL(add_var_to_array_no_dup(
      exprs, static_cast<ObRawExpr *>(delete_rowscn_expr_)))) {
    LOG_WARN("failed to append delete rowscn expr", K(ret), K_(delete_idx_tid));
  }
  return ret;
}

int ObAsyncIndexDeleteInfo::append_index_id(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (!has_delete_index()) {
    // Synchronous index path.
  } else if (OB_FAIL(add_var_to_array_no_dup(index_ids, delete_idx_tid_))) {
    LOG_WARN("failed to append async delete index id", K(ret), K_(delete_idx_tid));
  }
  return ret;
}

const double ObHybridSearchGenerator::MAX_INTERSECTION_MERGE_SEL = 0.20;
const double ObHybridSearchGenerator::MAX_SEL_GAP_RATIO = 2.0;

bool ObHybridSearchGenerator::in_merge_expr_whitelist(const ObRawExpr *expr)
{
  bool is_match = false;
  if (OB_NOT_NULL(expr) && IS_STRICT_OP(expr->get_expr_type())) {
    for (int64_t i = 0; !is_match && i < expr->get_param_count(); ++i) {
      const ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_NOT_NULL(param_expr) &&
          ObDSLResolver::in_merge_node_whitelist(ObRawExprUtils::skip_inner_added_expr(param_expr))) {
        is_match = true;
      }
    }
  }
  return is_match;
}

int ObHybridSearchGenerator::build_merged_boost_expr(const ObConstRawExpr *boost1,
                                                     const ObConstRawExpr *boost2,
                                                     ObConstRawExpr *&merged_boost_expr)
{
  int ret = OB_SUCCESS;
  double boost1_val = 0.0;
  double boost2_val = 0.0;
  if (OB_ISNULL(boost1) || OB_ISNULL(boost2) || OB_ISNULL(plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("boost1 or boost2 or plan is null", K(ret), KP(boost1), KP(boost2), KP(plan_));
  } else if (OB_FAIL(boost1->get_value().get_double(boost1_val))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get double value from boost1", K(ret), KP(boost1));
  } else if (OB_FAIL(boost2->get_value().get_double(boost2_val))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get double value from boost2", K(ret), KP(boost2));
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(plan_->get_optimizer_context().get_expr_factory(),
                                                             ObDoubleType,
                                                             boost1_val + boost2_val,
                                                             merged_boost_expr))) {
    LOG_WARN("failed to build merged boost", K(ret), K(boost1_val), K(boost2_val));
  }
  return ret;
}

int ObHybridSearchGenerator::build_split_boost_expr(const ObDSLScalarQuery *scalar_query,
                                                    const int64_t element_count,
                                                    ObConstRawExpr *&split_boost_expr)
{
  int ret = OB_SUCCESS;
  split_boost_expr = nullptr;
  if (OB_ISNULL(scalar_query) || OB_ISNULL(plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(scalar_query), KP(plan_));
  } else if (element_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected split element count", K(ret), K(element_count));
  } else if (1 == element_count) {
    split_boost_expr = scalar_query->boost_;
  } else {
    double origin_boost_val = 0.0;
    if (OB_FAIL(scalar_query->boost_->get_value().get_double(origin_boost_val))) {
      LOG_WARN("failed to get double value from boost", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(plan_->get_optimizer_context().get_expr_factory(),
                                                               ObDoubleType,
                                                               origin_boost_val / element_count,
                                                               split_boost_expr))) {
      LOG_WARN("failed to build per-element boost for split", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchGenerator::deal_table_scan_filters(ObIndexMergeNode *hybrid_search_tree,
                                                     ObIArray<ObRawExpr*> &table_filters,
                                                     bool &ingore_normal_access_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hybrid_search_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(hybrid_search_tree));
  } else if (INDEX_MERGE_HYBRID_FUSION_SEARCH != hybrid_search_tree->node_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), K(hybrid_search_tree->node_type_));
  } else {
    ObFusionNode *fusion_node = static_cast<ObFusionNode *>(hybrid_search_tree);
    int64_t index_scan_count = 0;
    ingore_normal_access_path = false; // set to false in sql query.
    if (fusion_node->children_.count() != 1 || OB_ISNULL(fusion_node->children_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children count", K(ret), K(fusion_node->children_.count()));
    } else if (fusion_node->children_.at(0)->is_hybrid_scalar_node()) {
      // if single scan node is not search index path, normal access path is better.
      bool child_prune_happened = false;
      double child_selectivity = 0.0;
      ingore_normal_access_path = false;
      if (has_force_index_merge_hint_ &&
          OB_FAIL(deal_child_node(fusion_node->children_.at(0),
                                  child_prune_happened,
                                  child_selectivity))) {
        LOG_WARN("failed to deal child node", K(ret), KPC(fusion_node->children_.at(0)));
      } else if (has_force_index_merge_hint_ && child_prune_happened) {
        ObScalarQueryNode *scalar_node =
            static_cast<ObScalarQueryNode *>(fusion_node->children_.at(0));
        if (OB_FAIL(append_array_no_dup(table_filters,
                                        scalar_node->pri_table_query_params_.pushdown_filters_))) {
          LOG_WARN("failed to append expr", K(ret));
        }
      } else if (OB_FAIL(recurse_count_index_nodes(fusion_node->children_.at(0), index_scan_count,
                                                   ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret));
      } else if (has_force_index_merge_hint_ && 0 < index_scan_count) {
        ingore_normal_access_path = true;
      }
    } else if (fusion_node->children_.at(0)->node_type_ != INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      // do nothing
    } else {
      ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode *>(fusion_node->children_.at(0));
      double min_selectivity = 1.0;
      ObSEArray<double, 8> children_selectivity;
      double invalid_sel = 100;
      int idx = -1;
      bool root_prune_happened = false;
      const bool is_root_or_node = has_force_index_merge_hint_
                                   && bool_node->filter_nodes_.empty()
                                   && !bool_node->should_nodes_.empty();
      for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        ObIndexMergeNode *child_node = bool_node->filter_nodes_.at(i);
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(deal_child_node(child_node, child_prune_happened, child_sel))) {
          LOG_WARN("failed to deal scalar node", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          child_sel = invalid_sel;
        } 
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(children_selectivity.push_back(child_sel))) {
          LOG_WARN("failed to push back child node selectivity", K(ret));
        } else if (child_sel > 0.0 && child_sel < min_selectivity) {
          min_selectivity = child_sel;
          idx = i;
        }
      }
      common::ObSEArray<ObIndexMergeNode*, 4> filter_nodes_tmp;
      if (OB_FAIL(ret) || children_selectivity.empty()) {
      } else if (OB_FAIL(filter_nodes_tmp.assign(bool_node->filter_nodes_))) {
        LOG_WARN("failed to assign must nodes", K(ret));
      } else if (min_selectivity > MAX_INTERSECTION_MERGE_SEL) {
        bool_node->filter_nodes_.reuse();
        for (int i = 0; i < filter_nodes_tmp.count() && OB_SUCC(ret); i++) {
          ObIndexMergeNode *child_node = filter_nodes_tmp.at(i);
          if (i == idx || children_selectivity.at(i) <= 0.0) {
            if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            }
          } else if (child_node->is_hybrid_scalar_node()) {
            ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(child_node);
            if (OB_FAIL(append_array_no_dup(table_filters, scalar_node->pri_table_query_params_.pushdown_filters_))) {
              LOG_WARN("failed to append expr", K(ret));
            } 
          } else if (child_node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
            if (OB_FAIL(add_var_to_array_no_dup(table_filters, static_cast<ObBooleanQueryNode *>(child_node)->origin_expr_))) {
              LOG_WARN("failed to append expr", K(ret));
            } 
          }
        }
      } else {
        bool_node->filter_nodes_.reuse();
        const double max_sel = std::min(min_selectivity * MAX_SEL_GAP_RATIO, MAX_INTERSECTION_MERGE_SEL);
        for (int64_t i = 0; OB_SUCC(ret) && i < children_selectivity.count(); ++i) {
          ObIndexMergeNode *child_node = filter_nodes_tmp.at(i);
          if (children_selectivity.at(i) <= max_sel) {
            if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            }
          } else {
            if (child_node->is_hybrid_scalar_node()) {
              ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(child_node);
              if (OB_FAIL(append_array_no_dup(table_filters, scalar_node->pri_table_query_params_.pushdown_filters_))) {
                LOG_WARN("failed to append expr", K(ret));
              } 
            } else if (child_node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
              if (OB_FAIL(add_var_to_array_no_dup(table_filters, static_cast<ObBooleanQueryNode *>(child_node)->origin_expr_))) {
                LOG_WARN("failed to append expr", K(ret));
              } 
            }
          }
        }
      }
      if (OB_SUCC(ret) && is_root_or_node) {
        double root_selectivity = 0.0;
        if (OB_FAIL(deal_child_node(bool_node, root_prune_happened, root_selectivity))) {
          LOG_WARN("failed to deal root or node", K(ret), KPC(bool_node));
        } else if (root_prune_happened) {
          LOG_TRACE("do not use hybrid search path because root or node is pruned", KPC(bool_node));
        }
      }
      if (OB_SUCC(ret) && !root_prune_happened) {
        ingore_normal_access_path = false;
        if (OB_FAIL(recurse_count_index_nodes(bool_node, index_scan_count,
                                              ingore_normal_access_path))) {
          LOG_WARN("failed to recurse count index nodes", K(ret));
        } else if (has_force_index_merge_hint_ && 0 < index_scan_count) {
          ingore_normal_access_path = true;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::deal_child_node(ObIndexMergeNode *node, bool &prune_happened, double &sel)
{
  int ret = OB_SUCCESS;
  bool is_match_column_hint = true;
  prune_happened = false;
  sel = 0.0;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index merge node", K(ret), K(node));
  } else if (node->is_hybrid_scalar_node()) {
    if (OB_ISNULL(node->ap_)) {
      prune_happened = true;
      LOG_TRACE("prune node because no available index", KPC(node));
    } else if (!node->ap_->is_new_query_range_with_precise_expr()) {
      prune_happened = true;
      LOG_TRACE("prune node because no precise query range", KPC(node));
    } else if (OB_FAIL(check_node_match_index_merge_column_hint(node, is_match_column_hint))) {
      LOG_WARN("failed to check index merge column hint", K(ret), KPC(node));
    } else if (!is_match_column_hint) {
      prune_happened = true;
      LOG_TRACE("prune node because column is not specified by hint", KPC(node));
    } else if (has_force_index_merge_hint_) {
      // A valid force hint must not be rejected by the default selectivity heuristic.
      sel = 0.0;
    } else {
      if (node->filter_.count() == 1 && (node->filter_.at(0)->is_json_domain_expr() ||
                                       node->filter_.at(0)->is_domain_array_expr())) {
        // Domain expressions (json_contains/array_contains) have high computation cost and their
        // selectivity may not be estimated accurately. When valid index exists, force using index
        // by setting negative selectivity to prevent this node from being pruned.
        sel = -0.0;
      } else {
        sel = node->ap_->est_cost_info_.prefix_filter_sel_;
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(node);
    if (bool_node->filter_nodes_.count() > 0) {
      // and condition
      double min_selectivity = 1.0;
      ObSEArray<double, 8> children_selectivity;
      for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(SMART_CALL(deal_child_node(child_node, child_prune_happened, child_sel)))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          prune_happened = true;
        } else if (OB_FAIL(children_selectivity.push_back(child_sel))) {
          LOG_WARN("failed to append child selectivity", K(ret), K(i), K(child_sel));
        } else if (child_sel > 0.0 && child_sel < min_selectivity) {
          min_selectivity = child_sel;
        }
      }
      if (OB_SUCC(ret) && !prune_happened) {
        if (min_selectivity < 1.0 && min_selectivity > MAX_INTERSECTION_MERGE_SEL) {
          prune_happened = true;
        } else {
          sel = 0.0;
          const double max_sel = std::min(min_selectivity * MAX_SEL_GAP_RATIO, MAX_INTERSECTION_MERGE_SEL);
          for (int i = 0; i < children_selectivity.count() && OB_SUCC(ret) && !prune_happened; i++) {
            if (children_selectivity.at(i) > max_sel) {
              // if prune_happened is true, sel is invalid
              prune_happened = true;
            } else {
              sel += children_selectivity.at(i);
            }
          }
        }
      }
    } else if (bool_node->should_nodes_.count() > 0) {
      // or condition
      for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
        ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(SMART_CALL(deal_child_node(child_node, child_prune_happened, child_sel)))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          prune_happened = true;
        } else {
          sel += child_sel;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::recurse_count_index_nodes(const ObIndexMergeNode *node,
                                                       int64_t &index_scan_count,
                                                       bool &ingore_normal_access_path) const
{
  int ret = OB_SUCCESS;
  if (ingore_normal_access_path) {
    // no need to count index nodes
  } else if (node->is_hybrid_scalar_node()) {
    if (OB_NOT_NULL(node->ap_)) {
      index_scan_count++;
      if (node->ap_->is_search_index_path() || index_scan_count > 1) {
        // if scan node is search index path or has multiple index scan nodes,
        // always use hybrid index merge path.
        ingore_normal_access_path = true;
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    const ObBooleanQueryNode *bool_node = static_cast<const ObBooleanQueryNode*>(node);
    // in sql query, boolean query only has filter(AND) and should(OR) nodes.
    CK (bool_node->must_nodes_.empty());
    CK (bool_node->must_not_nodes_.empty());
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObIndexMergeNode *child_node = static_cast<ObIndexMergeNode *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(recurse_count_index_nodes(child_node, index_scan_count, ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObIndexMergeNode *child_node = static_cast<ObIndexMergeNode *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(recurse_count_index_nodes(child_node, index_scan_count, ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret), K(i), KPC(child_node));
      }
    }
  } else {
    // other node type, set ingore_normal_access_path to true
    ingore_normal_access_path = true;
  }
  return ret;
}

int ObHybridSearchGenerator::generate(const ObDSLQueryInfo *dsl_query,
                                      ObIndexMergeNode *&hybrid_search_tree)
{
  int ret = OB_SUCCESS;
  const ObDSLQueryInfo *dsl_info = NULL;
  ObSQLSessionInfo *session = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  if (OB_ISNULL(plan_)|| OB_ISNULL(allocator_)
      || OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())
      || OB_ISNULL(session = plan_->get_optimizer_context().get_session_info())
      || OB_ISNULL(dsl_info = dsl_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(allocator_), K(plan_));
  } else if (OB_ISNULL(hybrid_search_tree = OB_NEWx(ObFusionNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else {
    cur_primary_get_ratio_ = dsl_info->fts_search_option_.primary_get_ratio_;
    ObFusionNode *fusion_node = static_cast<ObFusionNode *>(hybrid_search_tree);
    if (OB_FAIL(init_fusion_node(dsl_info, fusion_node))) {
      LOG_WARN("failed to init fusion node", K(ret));
    }
    for (int i = 0; i < dsl_info->queries_.count() && OB_SUCC(ret); i++) {
      ObDSLQuery *query =  dsl_info->queries_.at(i);
      ObIndexMergeNode *sub_node = NULL;
      ObConstRawExpr *one_boost_expr = static_cast<ObConstRawExpr*>(dsl_query->one_const_expr_);
      if (OB_FAIL(generate_node(query, sub_node))) {
        LOG_WARN("failed to generate hybrid search node", K(ret));
      } else {
        if (ObFusionIterExecMode::SKIP_FUSION_ITER == fusion_node->fusion_iter_exec_mode_
            && !fusion_node->track_score_
            && sub_node->is_hybrid_scalar_node()) {
          // A scoreless scalar query bypasses the fusion iterator, so its score cannot affect
          // either row membership or the final result. Keep the whole scalar execution path
          // non-scoring instead of carrying the DSL query's default scoring flag into DAS.
          ObHybridSearchNodeBase *scalar_node = static_cast<ObHybridSearchNodeBase *>(sub_node);
          scalar_node->is_scoring_ = false;
          scalar_node->is_top_level_scoring_ = false;
        }
        if (OB_FAIL(fusion_node->children_.push_back(sub_node))) {
          LOG_WARN("failed to append hybrid search node", K(ret));
        } else if (OB_FAIL(collect_child_boost_and_update_flags(
                       dsl_info, fusion_node, sub_node, one_boost_expr, i))) {
          LOG_WARN("failed to collect child boost and update flags", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::init_fusion_node(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node)
{
  int ret = OB_SUCCESS;
  fusion_node->from_ = query_info->from_;
  fusion_node->size_ = query_info->size_;
  fusion_node->min_score_ = query_info->min_score_;
  fusion_node->method_ = query_info->rank_info_.method_;
  fusion_node->window_size_ = query_info->rank_info_.window_size_;
  fusion_node->rank_const_ = query_info->rank_info_.rank_const_;
  fusion_node->is_top_k_query_ = query_info->is_top_k_query_;
  if (OB_NOT_NULL(query_info->rank_info_.rerank_info_)) {
    const ObDSLRerankInfo *src = query_info->rank_info_.rerank_info_;
    fusion_node->rerank_info_.model_ = src->model_;
    fusion_node->rerank_info_.query_ = src->query_;
    fusion_node->rerank_info_.field_ = src->field_;
    fusion_node->rerank_info_.rank_window_size_ = src->rank_window_size_;
    fusion_node->has_hybrid_fusion_op_ = true;
  }
  fusion_node->query_dop_ = query_info->fts_search_option_.query_dop_;
  fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS;
  fusion_node->track_score_ = query_info->track_score_;
  for (int i = 0; i < query_info->rowkey_cols_.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(fusion_node->rowkey_cols_.push_back(query_info->rowkey_cols_.at(i)))) {
      LOG_WARN("failed to append rowkey expr", K(ret));
    }
  }
  for (int i = 0; i < query_info->score_cols_.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(fusion_node->score_cols_.push_back(query_info->score_cols_.at(i)))) {
      LOG_WARN("failed to append score expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_fusion_iter_exec_mode(query_info, fusion_node))) {
      LOG_WARN("failed to init fusion iter exec mode", K(ret));
    }
  }
  if (OB_SUCC(ret) && (query_info->has_unique_shard_doc() || query_info->has_search_after()
                      || !query_info->sort_lookup_base_columns_.empty())) {
    // Preserve SQL ORDER BY expressions, directions and NULL placement for the search_after filter.
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info->sort_items_.count(); ++i) {
      const ObDSLSortItem &item = query_info->sort_items_.at(i);
      ObFusionSortItem out;
      const bool null_first = item.missing_mode_ == ObDSLSortItem::MissingMode::FIRST;
      out.order_item_ = OrderItem(item.field_expr_, null_first
          ? (item.is_asc_ ? NULLS_FIRST_ASC : NULLS_FIRST_DESC)
          : (item.is_asc_ ? NULLS_LAST_ASC : NULLS_LAST_DESC));
      out.is_score_sort_ = item.is_score_sort_;
      out.is_unique_shard_doc_ = item.is_unique_shard_doc_;
      if (OB_FAIL(fusion_node->sort_items_.push_back(out))) {
        LOG_WARN("failed to append fusion sort item", K(ret), K(i), K(query_info->sort_items_.count()));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(fusion_node->search_after_values_.assign(query_info->search_after_values_))) {
      LOG_WARN("failed to assign fusion search_after expressions", K(ret), K(query_info->search_after_values_.count()));
    }
  }
  if (OB_SUCC(ret)) {
    fusion_node_ = fusion_node;
  }
  return ret;
}

int ObHybridSearchGenerator::init_fusion_iter_exec_mode(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node)
{
  int ret = OB_SUCCESS;
  double min_score_value = 0;
  ObConstRawExpr *min_score_expr = nullptr;
  if (OB_ISNULL(query_info) || OB_ISNULL(fusion_node) || OB_ISNULL(plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(query_info), KP(fusion_node), KP(plan_));
  } else if (OB_ISNULL(min_score_expr = static_cast<ObConstRawExpr*>(query_info->min_score_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min score expr is null", K(ret));
  } else if (OB_FAIL(min_score_expr->get_value().get_double(min_score_value))) {
    LOG_WARN("failed to get double value from min score expr", K(ret));
  } else {
    const bool enable_parallel = plan_->get_optimizer_context().is_enable_hybrid_search_parallel_execution();
    fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS;
    bool is_top_k_score_query = !(query_info->result_mode_ == ObDSLResultMode::COUNT_AGG ||
                                  query_info->result_mode_ == ObDSLResultMode::BUCKET_AGG ||
                                  query_info->has_dsl_sort() || query_info->has_dsl_collapse());
    if (fusion_node->has_rerank()) {
      // rerank requires topk candidates, keep SCORE_TOP_K_QUERY_HITS
    } else if (min_score_value > 0) {
      if (!is_top_k_score_query && !query_info->has_dsl_rank()) {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL;
      }
    } else if (query_info->result_mode_ == ObDSLResultMode::COUNT_AGG ||
               query_info->result_mode_ == ObDSLResultMode::BUCKET_AGG) {
      if (query_info->output_score_) {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL;
      } else if (query_info->queries_.count() > 1 || enable_parallel) {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_DISTINCT_ONLY;
      } else {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::SKIP_FUSION_ITER;
      }
    } else if (query_info->has_dsl_sort() ||
               (query_info->has_dsl_collapse() && !query_info->has_dsl_rank())) {
      if (query_info->output_score_ || query_info->queries_.count() > 1 || enable_parallel) {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL;
      } else {
        fusion_node->fusion_iter_exec_mode_ = ObFusionIterExecMode::SKIP_FUSION_ITER;
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::generate_node(const ObDSLQuery *query,
                                           ObIndexMergeNode *&node)
{
  int ret = OB_SUCCESS;
  switch (query->query_type_) {
    case ObEsQueryItem::QUERY_ITEM_MATCH :
    case ObEsQueryItem::QUERY_ITEM_MULTI_MATCH :
    case ObEsQueryItem::QUERY_ITEM_MATCH_PHRASE :
    case ObEsQueryItem::QUERY_ITEM_QUERY_STRING : {
      ObFullTextQueryNode *fulltext_node = nullptr;
      const ObDSLFullTextQuery *fulltext_query = static_cast<const ObDSLFullTextQuery *>(query);
      if (OB_FAIL(generate_fulltext_node(fulltext_query, fulltext_node))) {
        LOG_WARN("failed to allocate hybrid search node", K(ret));
      } else {
        node = fulltext_node;
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_WILDCARD :
    case ObEsQueryItem::QUERY_ITEM_EXISTS :
    case ObEsQueryItem::QUERY_ITEM_RANGE :
    case ObEsQueryItem::QUERY_ITEM_TERM :
    case ObEsQueryItem::QUERY_ITEM_TERMS :
    case ObEsQueryItem::QUERY_ITEM_JSON_CONTAINS :
    case ObEsQueryItem::QUERY_ITEM_JSON_MEMBER_OF :
    case ObEsQueryItem::QUERY_ITEM_JSON_OVERLAPS :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_CONTAINS :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_CONTAINS_ALL :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_OVERLAPS : {
      ObScalarQueryNode *term_node = NULL;
      // json_contains/array_contains need split multi sub exprs to use index merge.
      ObIndexMergeNode *split_node = NULL;
      const ObDSLScalarQuery *scalar_query = static_cast<const ObDSLScalarQuery *>(query);
      if (OB_FAIL(split_domain_contains_node(scalar_query, split_node))) {
        LOG_WARN("failed to split domain contains node", K(ret));
      } else if (split_node != NULL) {
        // split happened, use split node
        node = split_node;
      } else {
        if (OB_FAIL(generate_scalar_node(scalar_query->scalar_expr_, term_node))) {
          LOG_WARN("failed to allocate hybrid search node", K(ret));
        } else {
          node = term_node;
        }
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_BOOL : {
      ObBooleanQueryNode *boolean_node = NULL;
      const ObDSLBoolQuery *bool_query = static_cast<const ObDSLBoolQuery *>(query);
      if (OB_FAIL(generate_boolean_node(bool_query, boolean_node))) {
        LOG_WARN("failed to append hybrid search node", K(ret));
      } else {
        node = boolean_node;
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_KNN : {
      ObVecSearchNode *vec_node = NULL;
      const ObDSLKnnQuery *knn_query = static_cast<const ObDSLKnnQuery *>(query);
      if (OB_FAIL(generate_knn_node(knn_query, vec_node))) {
        LOG_WARN("failed to append hybrid search node", K(ret));
      } else {
        node = vec_node;
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_NESTED : {
      ObNestedQueryNode *nested_node = NULL;
      const ObDSLNestedQuery *nested_query = static_cast<const ObDSLNestedQuery *>(query);
      if (OB_FAIL(generate_nested_node(nested_query, nested_node))) {
        LOG_WARN("failed to generate nested query node", K(ret));
      } else {
        node = nested_node;
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected query type", K(ret), K(query->query_type_));
  }
  
  if (OB_SUCC(ret)) {
    ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(node);
    new_node->is_scoring_ = query->need_cal_score_;
    new_node->is_top_level_scoring_ = query->is_top_level_score_;
    new_node->boost_ = query->boost_;
    new_node->primary_get_ratio_ = cur_primary_get_ratio_;
    if (new_node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode *>(new_node);
      if (OB_FAIL(try_generate_merged_probe_node(bool_node))) {
        LOG_WARN("failed to generate merged probe node", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::generate_knn_node(const ObDSLKnnQuery *knn_query, ObVecSearchNode *&knn_node)
{
  int ret = OB_SUCCESS;
  uint64_t index_id = OB_INVALID_ID;
  if (OB_ISNULL(knn_query)
      || OB_ISNULL(knn_query->field_)
      || OB_ISNULL(knn_query->distance_)
      || OB_ISNULL(schema_guard_)
      || OB_ISNULL(schema_guard_->get_schema_guard())
      || OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(knn_query), KP(schema_guard_), KP(table_schema_));
  } else if (OB_ISNULL(knn_node = OB_NEWx(ObVecSearchNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else if (knn_query->is_brute_force_) {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(table_schema_->get_tenant_id(), data_version))) {
      LOG_WARN("failed to get min data version", K(ret));
    } else if (data_version < DATA_VERSION_4_6_2_1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "knn search without vector index on current cluster version");
      LOG_WARN("brute force knn not supported before DATA_VERSION_4_6_2_1", K(ret), K(data_version));
    } else if (!table_schema_->is_heap_organized_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "hybrid knn brute force search only supports heap tables");
      LOG_WARN("brute force knn requires heap table", K(ret));
    } else {
      index_id = table_schema_->get_table_id();
    }
  } else if (OB_FAIL(ObVectorIndexUtil::get_matched_vector_index_tid(*(schema_guard_->get_schema_guard()),
                                                                      *table_schema_,
                                                                      knn_query->field_->get_column_id(),
                                                                      knn_query->distance_->get_expr_type(),
                                                                      index_id))) {
    LOG_WARN("failed to get matched vector index tid", K(ret), K(knn_query->field_->get_column_id()));
  } else if (index_id == OB_INVALID_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "knn search without vector index");
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(knn_node)) {
    knn_node->vec_info_.sort_key_.expr_ = knn_query->distance_;
    knn_node->vec_info_.topk_limit_expr_ = knn_query->k_;
    // knn search does not support offset
    knn_node->vec_info_.topk_offset_expr_ = nullptr;
    knn_node->field_ = knn_query->field_;
    knn_node->table_schema_ = table_schema_;
    if (OB_NOT_NULL(knn_query->search_option_)
        && (knn_query->search_option_->param_.is_valid()
            || (ObKnnFilterMode::INVALID_KNN_FILTER_MODE
                != knn_query->search_option_->filter_mode_))) {
      knn_node->search_option_ = knn_query->search_option_;
    }
    knn_node->main_index_table_id_ = index_id;
    knn_node->is_brute_force_ = knn_query->is_brute_force_;
    fusion_node_->contains_vec_node_ = true;

    // Save/set/restore cur_primary_get_ratio_ so the knn subtree (its filter scalar nodes and
    // any nested bool/probe) uses the knn-level ratio, while sibling clauses are unaffected.
    const int64_t saved_cur_primary_get_ratio = cur_primary_get_ratio_;
    cur_primary_get_ratio_ = (nullptr != knn_query->search_option_
                              && knn_query->search_option_->primary_get_ratio_ > 0)
        ? knn_query->search_option_->primary_get_ratio_
        : 0;

    if (OB_FAIL(ret)) {
    } else if (knn_query->filter_.count() == 0) {
      // do nothing
    } else if (knn_query->filter_.count() == 1) {
      ObIndexMergeNode *sub_node = NULL;
      if (OB_FAIL(generate_node(knn_query->filter_.at(0), sub_node))) {
        LOG_WARN("failed to generate child node", K(ret), K(0));
      } else if (OB_FAIL(knn_node->filter_nodes_.push_back(sub_node))) {
        LOG_WARN("failed to append child node", K(ret));
      }
    } else {
      ObBooleanQueryNode *bool_node = NULL;
      if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hybrid search node", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < knn_query->filter_.count(); i++) {
          ObIndexMergeNode *sub_node = NULL;
          if (OB_FAIL(generate_node(knn_query->filter_.at(i), sub_node))) {
            LOG_WARN("failed to generate child node", K(ret), K(i));
          } else if (OB_FAIL(bool_node->filter_nodes_.push_back(sub_node))) {
            LOG_WARN("failed to append child node", K(ret), K(i));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FALSE_IT(bool_node->boost_ = knn_query->boost_)) {
        } else if (OB_FALSE_IT(bool_node->primary_get_ratio_ = cur_primary_get_ratio_)) {
        } else if (OB_FAIL(try_generate_merged_probe_node(bool_node))) {
          LOG_WARN("failed to generate merged probe for knn filter array", K(ret));
        } else if (OB_FAIL(knn_node->filter_nodes_.push_back(bool_node))) {
          LOG_WARN("failed to append child node", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && knn_node->is_brute_force_
        && OB_FAIL(try_extract_brute_pushdown_filter(*knn_node))) {
      LOG_WARN("failed to extract brute force pushdown filter", K(ret));
    }

    if (OB_SUCC(ret) && !knn_node->is_brute_force_
        && knn_node->filter_nodes_.count() == 1 && knn_node->need_extract_filter()) {
      ObRawExpr *extracted_filter = nullptr;
      int extracted_ret = OB_SUCCESS;
      ObHybridSearchNodeBase *filter_node = static_cast<ObHybridSearchNodeBase*>(knn_node->filter_nodes_.at(0));
      ObRawExprFactory *expr_factory = &plan_->get_optimizer_context().get_expr_factory();
      
      if (OB_ISNULL(filter_node) || OB_ISNULL(expr_factory)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(filter_node), K(expr_factory));
      } else if (OB_FALSE_IT(extracted_ret = filter_node->extract_filter(*expr_factory, extracted_filter))){
      } else if (OB_UNLIKELY(OB_SUCCESS != extracted_ret)) {
        if (OB_NOT_SUPPORTED != extracted_ret) {
          ret = extracted_ret;
          LOG_WARN("failed to extract filter from filter node", K(ret));
        }
      } else if (OB_NOT_NULL(extracted_filter)) {
        ObScalarQueryNode *scalar_node = nullptr;
        if (OB_FAIL(extracted_filter->formalize(plan_->get_optimizer_context().get_session_info()))) {
          LOG_WARN("failed to formalize extracted filter", K(ret));
        } else if (OB_FAIL(generate_scalar_node(extracted_filter, scalar_node))) {
          LOG_WARN("failed to generate scalar node from extracted filter", K(ret));
        } else {
          scalar_node->has_valid_index_ = false;
          scalar_node->boost_ = knn_query->boost_;
          scalar_node->is_extracted_ = true;
          scalar_node->need_rowkey_order_ = false;
          knn_node->extracted_scalar_node_ = scalar_node;
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      if (knn_node->filter_nodes_.count() > 0 &&
          OB_FAIL(knn_node->children_.assign(knn_node->filter_nodes_))) {
        LOG_WARN("failed to assign children", K(ret));
      } else if (!knn_node->is_brute_force_) {
        const ObTableSchema *index_schema = NULL;
        if (OB_FAIL(schema_guard_->get_table_schema(index_id, index_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_FAIL(index_schema->get_index_name(knn_node->index_name_))) {
          LOG_WARN("fail to get index name", KR(ret));
        }
      }
    }
    // Restore cur_primary_get_ratio_ to its original value.
    cur_primary_get_ratio_ = saved_cur_primary_get_ratio;
  }

  return ret;
}

int ObHybridSearchGenerator::try_extract_brute_pushdown_filter(ObVecSearchNode &knn_node)
{
  int ret = OB_SUCCESS;
  ObRawExpr *extracted_filter = nullptr;
  ObHybridSearchNodeBase *filter_node = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  if (!knn_node.is_brute_force_ || knn_node.filter_nodes_.empty()) {
    // Nothing to extract.
  } else if (OB_UNLIKELY(1 != knn_node.filter_nodes_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected brute force filter node count", K(ret), K(knn_node.filter_nodes_.count()));
  } else if (OB_ISNULL(plan_)
             || OB_ISNULL(filter_node = static_cast<ObHybridSearchNodeBase *>(knn_node.filter_nodes_.at(0)))
             || OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null while extracting brute force filter", K(ret), KP(plan_), KP(filter_node),
             KP(expr_factory));
  } else {
    const int extract_ret = filter_node->extract_filter(*expr_factory, extracted_filter);
    if (OB_NOT_SUPPORTED == extract_ret) {
      // Optimization miss: the original filter tree remains intact and will build the
      // exact pre-filter bitmap as before.
      LOG_TRACE("brute force filter is not eligible for main table pushdown", K(extract_ret),
                KPC(filter_node));
    } else if (OB_SUCCESS != extract_ret) {
      ret = extract_ret;
      LOG_WARN("failed to extract brute force filter", K(ret), KPC(filter_node));
    } else if (OB_ISNULL(extracted_filter)) {
      LOG_TRACE("brute force filter extraction returned no required predicate", KPC(filter_node));
    } else if (OB_FAIL(extracted_filter->formalize(plan_->get_optimizer_context().get_session_info()))) {
      LOG_WARN("failed to formalize brute force pushdown filter", K(ret), KPC(extracted_filter));
    } else {
      knn_node.brute_pushdown_filter_ = extracted_filter;
    }
  }
  return ret;
}

int ObHybridSearchGenerator::generate_scalar_node(ObRawExpr *filter, ObScalarQueryNode *&scalar_node)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> rowkey_exprs;
  ObArray<ObRawExpr *> access_exprs;
  uint64_t index_id = OB_INVALID_ID;
  if (OB_ISNULL(plan_) || OB_ISNULL(table_item_) || OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(plan_), K(table_item_), K(table_schema_));
  } else if (OB_ISNULL(scalar_node = OB_NEWx(ObScalarQueryNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else if (OB_FALSE_IT(scalar_node->primary_get_ratio_ = cur_primary_get_ratio_)) {
  } else if (OB_FAIL(scalar_node->filter_.push_back(filter))) {
    LOG_WARN("failed to append scalar filter", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter, access_exprs))) {
    LOG_WARN("extract column exprs failed", K(ret));
  } else if (OB_FAIL(plan_->get_rowkey_exprs(table_item_->table_id_, *table_schema_, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs, rowkey_exprs))) {
    LOG_WARN("append rowkey expr to access exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->index_table_rowkey_exprs_.assign(rowkey_exprs))) {
    LOG_WARN("assign rowkey exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->index_table_query_params_.access_exprs_.assign(access_exprs))) {
    LOG_WARN("assign access exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->pri_table_query_params_.access_exprs_.assign(access_exprs))) {
    LOG_WARN("assign access exprs failed", K(ret));
  } else if (OB_FAIL(table_schema_->get_rowkey_column_ids(scalar_node->pri_table_query_params_.output_col_ids_))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(table_schema_->get_rowkey_column_ids(scalar_node->index_table_query_params_.output_col_ids_))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(check_filter_has_index(filter, scalar_node->has_valid_index_, index_id))) {
    LOG_WARN("fail to check node has index", K(ret));
  } else if (OB_FAIL(scalar_node->pri_table_query_params_.pushdown_filters_.push_back(filter))) { // generate pushdown filters for primary table
    LOG_WARN("failed to push down filter", K(ret));
  } else {
    // Deep-copy the currently-open nested() clause paths onto the leaf so the query-side
    // path encoder can insert ARRAY markers exactly at the explicitly-nested segments.
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_nested_paths_.count(); ++i) {
      ObString copied;
      if (OB_FAIL(ob_write_string(*allocator_, cur_nested_paths_.at(i), copied))) {
        LOG_WARN("failed to deep-copy enclosing nested path", K(ret), K(i));
      } else if (OB_FAIL(scalar_node->enclosing_nested_paths_.push_back(copied))) {
        LOG_WARN("failed to record enclosing nested path on scalar node", K(ret), K(i));
      }
    }
  }
  return ret;
}


int ObHybridSearchGenerator::generate_fulltext_node(
    const ObDSLFullTextQuery *fulltext_query,
    ObFullTextQueryNode *&fulltext_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fulltext_query)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(fulltext_query));
  } else if (OB_FAIL(fulltext_query->produce_query_node(*table_schema_, *schema_guard_, *allocator_, fulltext_node))) {
    LOG_WARN("failed to produce fulltext query node", K(ret));
  }
  return ret;
}

int ObHybridSearchGenerator::check_filter_has_index(ObRawExpr *filter, bool &has_index, uint64_t &target_index_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> column_ids;
  has_index = false;
  target_index_id = OB_INVALID_ID;
  if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, column_ids))) {
    LOG_WARN("failed to extract column ids", K(filter), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids_.count() && !has_index; ++i) {
      const uint64_t index_id = valid_index_ids_.at(i);
      const ObIArray<uint64_t> &index_column_ids = valid_index_cols_.at(i);
      const bool can_ignore_prefix = index_can_ignore_prefix_.at(i);
      if (ObOptimizerUtil::check_index_match_prefix(column_ids, index_column_ids, can_ignore_prefix)) {
        has_index = true;
        target_index_id = index_id;
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::get_search_index_cons_encode_type(ObRawExpr *filter,
                                                               bool &has_search_index,
                                                               uint8_t &cons_encode_type)
{
  int ret = OB_SUCCESS;
  has_search_index = false;
  cons_encode_type = 0;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_ISNULL(filter) || OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(filter), KP(schema_guard_));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, column_ids))) {
    LOG_WARN("failed to extract column ids", K(ret), KPC(filter));
  } else if (column_ids.count() != 1) {
    // empty or multiple columns, can not use search index
  } else {
    const uint64_t column_id = column_ids.at(0);
    // Keep the first-match behavior for automatic SQL-to-DSL. A forced hint provides a candidate
    // whitelist, so merge their type coverage and let each split leaf choose its own index later.
    for (int64_t i = 0;
         OB_SUCC(ret) && i < valid_index_ids_.count() &&
         (!has_search_index || has_force_index_merge_hint_);
         ++i) {
      const uint64_t index_id = valid_index_ids_.at(i);
      const ObIArray<uint64_t> &index_column_ids = valid_index_cols_.at(i);
      const bool can_ignore_prefix = index_can_ignore_prefix_.at(i);
      if (can_ignore_prefix && ObOptimizerUtil::check_index_match_prefix(
          column_ids, index_column_ids, true /* ignore prefix */)) {
        const ObTableSchema *index_schema = nullptr;
        const ObColumnSchemaV2 *index_column_schema = nullptr;
        uint8_t index_cons_encode_type = 0;
        if (OB_FAIL(schema_guard_->get_table_schema(index_id, index_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(index_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema is null", K(ret), K(index_id));
        } else if (OB_ISNULL(index_column_schema = index_schema->get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index column schema is null", K(ret), K(index_id), K(column_id));
        } else if (ob_is_json(index_column_schema->get_data_type())
                   && !index_column_schema->get_comment_str().empty()) {
          share::ObSearchIndexConfigFilter json_filter(index_schema->get_tenant_id());
          if (OB_FAIL(json_filter.init_from_comment(index_column_schema->get_comment_str()))) {
            LOG_WARN("failed to parse json column filter", K(ret), K(index_id), K(column_id),
                     K(index_column_schema->get_comment_str()));
          } else {
            index_cons_encode_type = json_filter.get_type_mask();
          }
        }
        // With a forced INDEX_MERGE hint, multiple search indexes form one candidate set.
        // For example:
        // si_string：INCLUDE_TYPES(JSON_STRING)
        // si_number：INCLUDE_TYPES(JSON_NUMBER)
        // JSON_CONTAINS(j, '["abc", 100]')
        // using either index's INCLUDE_TYPES alone would reject the original predicate before
        // it can be split. Therefore, merge the supported types of all candidates before
        // splitting, and let each split leaf select an applicable index using the candidate's
        // own configuration.
        if (OB_SUCC(ret)) {
          if (!has_search_index) {
            cons_encode_type = index_cons_encode_type;
          } else if (0 == cons_encode_type || 0 == index_cons_encode_type) {
            // A zero mask means that the Search Index has no type restriction.
            cons_encode_type = 0;
          } else {
            cons_encode_type = static_cast<uint8_t>(cons_encode_type | index_cons_encode_type);
          }
          has_search_index = true;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::move_bool_nodes_to_children(ObIndexMergeNode* node)
{
  int ret = OB_SUCCESS;
  if (node->node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH ||
      node->node_type_ == INDEX_MERGE_HYBRID_KNN ||
      node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    for (int i = 0; i < node->children_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(node->children_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(node);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->must_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->should_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->filter_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->must_not_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bool_node->children_.reserve(bool_node->must_nodes_.count() + bool_node->should_nodes_.count() +
                                                  bool_node->filter_nodes_.count() + bool_node->must_not_nodes_.count()
                                                  + (OB_NOT_NULL(bool_node->merged_probe_scalar_node_) ? 1 : 0)))) {
      LOG_WARN("failed to reserve children", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->must_nodes_))) {
      LOG_WARN("failed to append must nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->should_nodes_))) {
      LOG_WARN("failed to append should nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->filter_nodes_))) {
      LOG_WARN("failed to append filter nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->must_not_nodes_))) {
      LOG_WARN("failed to append must not nodes", K(ret));
    } else if (OB_NOT_NULL(bool_node->merged_probe_scalar_node_)) {
      ObIndexMergeNode *node = static_cast<ObIndexMergeNode*>(bool_node->merged_probe_scalar_node_);
      if (OB_FAIL(bool_node->children_.push_back(node))) {
        LOG_WARN("failed to append merged probe scalar node", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::extract_partition_related_filters(
    const ObIArray<ObRawExpr*> &filters,
    const ObIArray<uint64_t> &part_col_ids,
    ObIArray<ObRawExpr*> &partition_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObRawExpr *filter = filters.at(i);
    if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter", K(ret), K(i));
    } else {
      ObSEArray<uint64_t, 4> col_ids;
      if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, col_ids))) {
        LOG_WARN("failed to extract column ids from filter", K(ret));
      } else {
        bool references_part_col = ObOptimizerUtil::overlap(col_ids, part_col_ids);
        if (references_part_col) {
          if (OB_FAIL(add_var_to_array_no_dup(partition_filters, filter))) {
            LOG_WARN("failed to add partition filter", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::build_or_partition_filter(
    const ObIArray<ObRawExpr*> &left_filters,
    const ObIArray<ObRawExpr*> &right_filters,
    ObIArray<ObRawExpr*> &result)
{
  int ret = OB_SUCCESS;
  result.reuse();
  if (left_filters.empty() || right_filters.empty()) {
    // If either branch has no partition filter, we cannot prune any partition
    // because that branch could match rows in any partition.
  } else {
    ObRawExprFactory &expr_factory = plan_->get_optimizer_context().get_expr_factory();
    ObRawExpr *left_and_expr = nullptr;
    ObRawExpr *right_and_expr = nullptr;
    ObRawExpr *or_expr = nullptr;
    if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory, left_filters, left_and_expr))) {
      LOG_WARN("failed to build and expr for left filters", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory, right_filters, right_and_expr))) {
      LOG_WARN("failed to build and expr for right filters", K(ret));
    } else {
      ObSEArray<ObRawExpr*, 2> or_params;
      if (OB_FAIL(or_params.push_back(left_and_expr))) {
        LOG_WARN("failed to push back left and expr", K(ret));
      } else if (OB_FAIL(or_params.push_back(right_and_expr))) {
        LOG_WARN("failed to push back right and expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(expr_factory, or_params, or_expr))) {
        LOG_WARN("failed to build or expr", K(ret));
      } else if (OB_NOT_NULL(or_expr)) {
        if (OB_FAIL(or_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
          LOG_WARN("failed to formalize or expr", K(ret));
        } else if (OB_FAIL(result.push_back(or_expr))) {
          LOG_WARN("failed to push back or expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::check_should_required_for_partition_pruning(
    const ObBooleanQueryNode &node,
    bool &should_required)
{
  int ret = OB_SUCCESS;
  const int64_t should_count = node.should_nodes_.count();
  int64_t min_should_match = 0;
  should_required = false;
  if (0 == should_count) {
  } else if (node.must_nodes_.empty() && node.filter_nodes_.empty()) {
    should_required = true;
  } else if (OB_ISNULL(node.minimum_should_match_)) {
    should_required = node.min_should_match_ > 0;
  } else if (node.minimum_should_match_->is_param_expr()) {
    // Parameterized MSM cannot constrain partitions during plan generation.
  } else if (ob_is_integer_type(node.minimum_should_match_->get_value().get_type())) {
    if (OB_FAIL(ObDASMinShouldMatchSpec::int_calc(
        should_count, node.minimum_should_match_->get_value().get_int(), min_should_match))) {
      LOG_WARN("failed to calculate integer MSM for partition pruning", K(ret), K(should_count));
    } else {
      should_required = min_should_match > 0;
    }
  } else if (ob_is_string_type(node.minimum_should_match_->get_value().get_type())) {
    ObArenaAllocator msm_allocator(ObModIds::OB_SQL_EXPR_CALC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_FAIL(ObDASMinShouldMatchSpec::string_calc(
        msm_allocator, should_count, node.minimum_should_match_->get_value().get_string(), min_should_match))) {
      LOG_WARN("failed to calculate string MSM for partition pruning", K(ret), K(should_count));
    } else {
      should_required = min_should_match > 0;
    }
  }
  return ret;
}

int ObHybridSearchGenerator::extract_partition_filters_from_tree(
    ObIndexMergeNode *node,
    const ObIArray<uint64_t> &part_col_ids,
    ObIArray<ObRawExpr*> &partition_filters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null node", K(ret));
  } else if (part_col_ids.empty()) {
  } else {
    switch (node->node_type_) {
      case INDEX_MERGE_HYBRID_SCALAR_QUERY: {
        if (OB_FAIL(extract_partition_related_filters(
                node->filter_, part_col_ids, partition_filters))) {
          LOG_WARN("failed to extract partition filters from scalar node", K(ret));
        }
        break;
      }
      case INDEX_MERGE_HYBRID_BOOLEAN_QUERY: {
        ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(node);
        bool should_required = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < bool_node->must_nodes_.count(); ++i) {
          if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                  bool_node->must_nodes_.at(i), part_col_ids, partition_filters)))) {
            LOG_WARN("failed to extract from must node", K(ret), K(i));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < bool_node->filter_nodes_.count(); ++i) {
          if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                  bool_node->filter_nodes_.at(i), part_col_ids, partition_filters)))) {
            LOG_WARN("failed to extract from filter node", K(ret), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_should_required_for_partition_pruning(*bool_node, should_required))) {
          LOG_WARN("failed to check should requirement for partition pruning", K(ret));
        } else if (should_required) {
          // Required should clauses constrain partitions by the union of their branches.
          // A branch without partition filters keeps all partitions for the should group.
          ObSEArray<ObRawExpr*, 4> accumulated_filters;
          if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                  bool_node->should_nodes_.at(0), part_col_ids, accumulated_filters)))) {
            LOG_WARN("failed to extract from first should node", K(ret));
          }
          for (int64_t i = 1; OB_SUCC(ret) && i < bool_node->should_nodes_.count()
                              && accumulated_filters.count() > 0; ++i) {
            ObSEArray<ObRawExpr*, 4> child_filters;
            ObSEArray<ObRawExpr*, 4> or_result;
            if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                    bool_node->should_nodes_.at(i), part_col_ids, child_filters)))) {
              LOG_WARN("failed to extract from should node", K(ret), K(i));
            } else if (OB_FAIL(build_or_partition_filter(
                    accumulated_filters, child_filters, or_result))) {
              LOG_WARN("failed to build or partition filter", K(ret));
            } else if (OB_FAIL(accumulated_filters.assign(or_result))) {
              LOG_WARN("failed to assign or result filters", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(append_array_no_dup(partition_filters, accumulated_filters))) {
              LOG_WARN("failed to append should partition filters", K(ret));
            }
          }
        }
        break;
      }
      case INDEX_MERGE_HYBRID_FUSION_SEARCH: {
        // For fusion nodes, the semantics is the same as should (OR):
        // each child path may match different partitions, so we need the
        // union of all children's partitions.
        ObFusionNode *fusion = static_cast<ObFusionNode*>(node);
        if (fusion->children_.count() > 0) {
          ObSEArray<ObRawExpr*, 4> accumulated_filters;
          if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                  fusion->children_.at(0), part_col_ids, accumulated_filters)))) {
            LOG_WARN("failed to extract from first fusion child", K(ret));
          }
          for (int64_t i = 1; OB_SUCC(ret) && i < fusion->children_.count()
                              && accumulated_filters.count() > 0; ++i) {
            ObSEArray<ObRawExpr*, 4> child_filters;
            ObSEArray<ObRawExpr*, 4> or_result;
            if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                    fusion->children_.at(i), part_col_ids, child_filters)))) {
              LOG_WARN("failed to extract from fusion child", K(ret), K(i));
            } else if (OB_FAIL(build_or_partition_filter(
                    accumulated_filters, child_filters, or_result))) {
              LOG_WARN("failed to build or partition filter for fusion", K(ret));
            } else if (OB_FAIL(accumulated_filters.assign(or_result))) {
              LOG_WARN("failed to assign or result filters", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(append_array_no_dup(partition_filters, accumulated_filters))) {
              LOG_WARN("failed to append fusion partition filters", K(ret));
            }
          }
        }
        break;
      }
      case INDEX_MERGE_HYBRID_KNN: {
        ObVecSearchNode *vec_node = static_cast<ObVecSearchNode*>(node);
        for (int64_t i = 0; OB_SUCC(ret) && i < vec_node->filter_nodes_.count(); ++i) {
          if (OB_FAIL(SMART_CALL(extract_partition_filters_from_tree(
                  vec_node->filter_nodes_.at(i), part_col_ids, partition_filters)))) {
            LOG_WARN("failed to extract from knn filter node", K(ret), K(i));
          }
        }
        break;
      }
      case INDEX_MERGE_HYBRID_FTS_QUERY: {
        break;
      }
      default: {
        LOG_TRACE("skip extracting partition filters from unknown node type",
                  K(node->node_type_));
        break;
      }
    }
  }
  return ret;
}

/**
 * @brief ObHybridSearchGenerator::try_merge_nodes
 *
 * For simple condition and range condition, if there is another child node which
 * contains the same column, we can merge the new filter into the child directly.
 */
int ObHybridSearchGenerator::try_merge_nodes(const ObDSLQuery *query, ObIArray<ObIndexMergeNode*> &nodes, bool &merge_happened)
{
  int ret = OB_SUCCESS;
  const ObDSLScalarQuery *scalar_query = static_cast<const ObDSLScalarQuery *>(query);
  ObSEArray<uint64_t, 4> filter_column_ids;
  merge_happened = false;
  // first check if the query is allowed to be merged
  if (query->query_type_ != QUERY_ITEM_TERM && query->query_type_ != QUERY_ITEM_RANGE) {
    // do nothing
  } else if (!scalar_query->scalar_expr_->has_flag(IS_SIMPLE_COND) &&
             !scalar_query->scalar_expr_->has_flag(IS_RANGE_COND) &&
             !in_merge_expr_whitelist(scalar_query->scalar_expr_)) {
    // only simple condition and range condition can be merged
    LOG_TRACE("only simple condition and range condition can be merged", KPC(scalar_query->scalar_expr_));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(scalar_query->scalar_expr_, filter_column_ids))) {
    LOG_WARN("failed to extract column ids", K(scalar_query->scalar_expr_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nodes.count(); i++) {
      ObScalarQueryNode *scalar_node = nullptr;
      const ObRawExpr *child_filter = nullptr;
      const ObRawExpr *sub_filter = nullptr;
      ObSEArray<uint64_t, 4> child_column_ids;
      if (!nodes.at(i)->is_hybrid_scalar_node()) {
      } else if ((OB_ISNULL(scalar_node = static_cast<ObScalarQueryNode *>(nodes.at(i))))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null scalar node", K(ret));
      } else if (OB_UNLIKELY(scalar_node->filter_.empty()) || OB_ISNULL(child_filter = scalar_node->filter_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected child node filter", K(ret));
      } else if (child_filter->get_expr_type() != T_OP_AND) {
        sub_filter = child_filter;
      } else if (child_filter->get_param_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected child node filter", K(ret));
      } else if (OB_ISNULL(sub_filter = child_filter->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null sub filter", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(sub_filter)) {
      } else if (!IS_RANGE_CMP_OP(sub_filter->get_expr_type()) && sub_filter->get_expr_type() != T_OP_EQ) {
        // do nothing, only merge simple condition and range condition
      } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(sub_filter, child_column_ids))) {
        LOG_WARN("failed to extract column ids", K(ret), KPC(sub_filter));
      } else if (is_array_equal(filter_column_ids, child_column_ids)) {
        // merge if any one of the conditions is satisfied:
        // (1) both filters have no whitelist expression, meaning they have the same simple column reference
        // (2) both filters have the same whitelist expression
        const ObRawExpr *whitelist_expr1 = nullptr;
        const ObRawExpr *whitelist_expr2 = nullptr;
        if (ObDSLResolver::in_merge_node_whitelist(scalar_query->field_)) {
          whitelist_expr1 = scalar_query->field_;
        }
        for (int64_t i = 0 ; OB_SUCC(ret) && i < sub_filter->get_param_count(); ++i) {
          const ObRawExpr *param_expr = sub_filter->get_param_expr(i);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null param expr", K(ret));
          } else if (ObDSLResolver::in_merge_node_whitelist(param_expr)) {
            whitelist_expr2 = param_expr;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          ObConstRawExpr *merged_boost = nullptr;
          if (OB_ISNULL(whitelist_expr1) && OB_ISNULL(whitelist_expr2)) {
            merge_happened = true;
          } else if (OB_NOT_NULL(whitelist_expr1) && OB_NOT_NULL(whitelist_expr2) &&
                     whitelist_expr1->same_as(*whitelist_expr2)) {
            merge_happened = true;
          }
          if (!merge_happened) {
          } else if (OB_FAIL(nodes.at(i)->filter_.push_back(scalar_query->scalar_expr_))) {
            LOG_WARN("failed to push back filter", K(ret));
          // after merging into an existing scalar node, we need to push the new filter condition down to the primary table.
          // this ensures that the condition is properly applied during the scalar query execution.
          } else if (OB_FAIL(scalar_node->pri_table_query_params_.pushdown_filters_.push_back(scalar_query->scalar_expr_))) {
            LOG_WARN("failed to push back pushdown filter", K(ret));
          } else if (OB_FAIL(build_merged_boost_expr(scalar_node->boost_, scalar_query->boost_, merged_boost))) {
            LOG_WARN("failed to build merged boost expr", K(ret));
          } else {
            scalar_node->boost_ = merged_boost;
          }
        }
      }
    }
  }
  return ret;
}


bool ObHybridSearchGenerator::enable_merged_probe(const ObBooleanQueryNode *node)
{
  bool enabled = false;
  if (OB_ISNULL(node)) {
    // do nothing
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    const bool enable_primary_get_merge =
      tenant_config.is_valid() && tenant_config->_enable_hybrid_search_primary_get_merge;
    const bool has_supported_should =
        node->should_nodes_.empty()
        || (OB_NOT_NULL(node->minimum_should_match_)
            && ob_is_integer_type(node->minimum_should_match_->get_value().get_type())
            && 1 == node->minimum_should_match_->get_value().get_int());
    const int64_t required_clauses = node->must_nodes_.count() + node->filter_nodes_.count();
    enabled = enable_primary_get_merge
              && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_6_2_0
              && !node->is_scoring_
              && !node->is_top_level_scoring_
              && node->primary_get_ratio_ > 0
              && node->must_not_nodes_.empty()
              && required_clauses > (node->should_nodes_.empty() ? 1 : 0)
              && has_supported_should;
  }
  return enabled;
}

int ObHybridSearchGenerator::try_generate_merged_probe_node(ObBooleanQueryNode *node)
{
  int ret = OB_SUCCESS;
  ObRawExpr *merged_filter = nullptr;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (!cur_nested_paths_.empty()
             || (OB_NOT_NULL(table_item_) && table_item_->is_logical_table_)
             || !enable_merged_probe(node)) {
    // do nothing
  } else if (OB_FAIL(node->extract_filter(plan_->get_optimizer_context().get_expr_factory(), merged_filter))) {
    if (OB_UNLIKELY(OB_NOT_SUPPORTED != ret)) {
      LOG_WARN("failed to extract merged probe filter from boolean node", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(merged_filter)) {
    ObScalarQueryNode *probe_node = nullptr;
    if (OB_FAIL(merged_filter->formalize(plan_->get_optimizer_context().get_session_info()))) {
      LOG_WARN("failed to formalize merged probe filter", K(ret));
    } else if (OB_FAIL(generate_scalar_node(merged_filter, probe_node))) {
      LOG_WARN("failed to generate merged probe scalar node", K(ret));
    } else {
      probe_node->has_valid_index_ = false;
      probe_node->is_merged_probe_ = true;
      probe_node->is_scoring_ = node->is_scoring_;
      probe_node->is_top_level_scoring_ = node->is_top_level_scoring_;
      probe_node->boost_ = node->boost_;
      probe_node->primary_get_ratio_ = node->primary_get_ratio_;
      node->merged_probe_scalar_node_ = probe_node;
    }
  }
  return ret;
}

int ObHybridSearchGenerator::generate_boolean_node(const ObDSLBoolQuery *dsl_query, ObBooleanQueryNode *&bool_node)
{
  int ret = OB_SUCCESS;
  bool merge_happened = false;
  if (OB_ISNULL(dsl_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr");
  } else if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  }
  for (int i = 0; i < dsl_query->must_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    bool merge_happened = false;
    if (OB_FAIL(try_merge_nodes(dsl_query->must_.at(i), bool_node->must_nodes_, merge_happened))) {
      LOG_WARN("failed to merge bool node filters", K(ret));
    } else if (merge_happened) {
      // do nothing
    } else if (OB_FAIL(generate_node(dsl_query->must_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->must_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->should_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    if (OB_FAIL(generate_node(dsl_query->should_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->should_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->filter_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    bool merge_happened = false;
    if (OB_FAIL(try_merge_nodes(dsl_query->filter_.at(i), bool_node->filter_nodes_, merge_happened))) {
      LOG_WARN("failed to merge bool node filters", K(ret));
    } else if (merge_happened) {
      // do nothing
    } else if (OB_FAIL(generate_node(dsl_query->filter_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->filter_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->must_not_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    if (OB_FAIL(generate_node(dsl_query->must_not_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->must_not_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    bool_node->min_should_match_ = dsl_query->msm_;
    bool_node->minimum_should_match_ = dsl_query->minimum_should_match_;
    bool_node->origin_expr_ = dsl_query->origin_expr_;
  }

  return ret;
}

int ObHybridSearchGenerator::generate_nested_node(const ObDSLNestedQuery *nested_query,
                                                  ObNestedQueryNode *&nested_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nested_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr nested_query", K(ret));
  } else if (OB_ISNULL(nested_node = OB_NEWx(ObNestedQueryNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate nested query node", K(ret));
  } else {
    nested_node->nesting_depth_ = nested_query->nesting_depth_;
    nested_node->column_idx_ = nested_query->column_idx_;
    ObIndexMergeNode *inner_node = NULL;
    if (!nested_query->path_.empty()
        && OB_FAIL(ob_write_string(*allocator_, nested_query->path_, nested_node->path_))) {
      LOG_WARN("failed to deep-copy nested path string", K(ret));
    } else if (OB_ISNULL(nested_query->inner_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested query has no inner query", K(ret));
    } else if (OB_FAIL(cur_nested_paths_.push_back(nested_query->path_))) {
      LOG_WARN("failed to push enclosing nested path", K(ret), K(nested_query->path_));
    } else {
      // Recurse with this clause's path on the stack, then pop unconditionally so the
      // stack stays balanced even when generate_node fails.
      if (OB_FAIL(generate_node(nested_query->inner_query_, inner_node))) {
        LOG_WARN("failed to generate inner node for nested query", K(ret));
      } else if (OB_ISNULL(inner_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner node is null after generate_node", K(ret));
      } else if (OB_FAIL(nested_node->children_.push_back(inner_node))) {
        LOG_WARN("failed to push inner node to children", K(ret));
      } else {
        nested_node->inner_node_ = inner_node;
      }
      cur_nested_paths_.pop_back();
    }
  }
  return ret;
}

int ObNestedQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos,
                                    ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("nested query: depth=%ld", nesting_depth_))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (!path_.empty()
             && OB_FAIL(BUF_PRINTF(", path=%.*s", path_.length(), path_.ptr()))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_NOT_NULL(inner_node_)) {
    ObHybridSearchNodeBase *inner = static_cast<ObHybridSearchNodeBase *>(inner_node_);
    if (OB_FAIL(SMART_CALL(inner->explain_info(buf, buf_len, pos, type, blank_space_count + 2)))) {
      LOG_WARN("failed to explain info for inner node", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::get_index_name(ObIArray<ObString> &index_names)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(this);
    if (scalar_node->is_hybrid_scalar_node_with_index()) {
       ObIndexMergeNode* node = static_cast<ObIndexMergeNode *>(this);
      if (!index_name_.empty() && OB_FAIL(index_names.push_back(index_name_))) {
        LOG_WARN("failed to append node", K(ret));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY) {
    ObFullTextQueryNode *fulltext_node = static_cast<ObFullTextQueryNode *>(this);
    switch (fulltext_node->query_type_) {
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH:
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE: {
        if (!index_name_.empty()) {
          bool exists = false;
          for (int64_t i = 0; !exists && i < index_names.count(); ++i) {
            if (index_names.at(i) == fulltext_node->index_name_) {
              exists = true;
            }
          }
          if (!exists && OB_FAIL(index_names.push_back(fulltext_node->index_name_))) {
            LOG_WARN("failed to append fts index name", K(ret));
          }
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH: {
        ObMultiMatchQueryNode *multi_match_node = static_cast<ObMultiMatchQueryNode *>(this);
        for (int64_t i = 0; OB_SUCC(ret) && i < multi_match_node->index_names_.count(); ++i) {
          if (!multi_match_node->index_names_.at(i).empty()) {
            bool exists = false;
            for (int64_t j = 0; !exists && j < index_names.count(); ++j) {
              if (multi_match_node->index_names_.at(i) == index_names.at(j)) {
                exists = true;
              }
            }
            if (!exists && OB_FAIL(index_names.push_back(multi_match_node->index_names_.at(i)))) {
              LOG_WARN("failed to append fts index name", K(ret));
            }
          }
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_QUERY_STRING: {
        ObQueryStringQueryNode *query_string_node = static_cast<ObQueryStringQueryNode *>(this);
        for (int64_t i = 0; OB_SUCC(ret) && i < query_string_node->index_names_.count(); ++i) {
          if (!query_string_node->index_names_.at(i).empty()) {
            bool exists = false;
            for (int64_t j = 0; !exists && j < index_names.count(); ++j) {
              if (query_string_node->index_names_.at(i) == index_names.at(j)) {
                exists = true;
              }
            }
            if (!exists && OB_FAIL(index_names.push_back(query_string_node->index_names_.at(i)))) {
              LOG_WARN("failed to append fts index name", K(ret));
            }
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fulltext query type", K(ret), K_(fulltext_node->query_type));
        break;
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    if (!index_name_.empty() && OB_FAIL(index_names.push_back(index_name_))) {
      LOG_WARN("failed to append node", K(ret));
    }
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    ObNestedQueryNode *nested_node = static_cast<ObNestedQueryNode *>(this);
    if (OB_NOT_NULL(nested_node->inner_node_)) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(nested_node->inner_node_);
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to get index name for nested inner node", K(ret));
      }
    }
  }
  return ret;
}


int ObHybridSearchNodeBase::get_all_scan_node(ObIArray<ObIndexMergeNode*> &scan_nodes)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(this);
    if (scalar_node->is_hybrid_scalar_node_with_index()) {
       ObIndexMergeNode* node = static_cast<ObIndexMergeNode *>(this);
      if (OB_FAIL(scan_nodes.push_back(node))) {
        LOG_WARN("failed to append node", K(ret));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    ObNestedQueryNode *nested_node = static_cast<ObNestedQueryNode *>(this);
    if (OB_NOT_NULL(nested_node->inner_node_)) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(nested_node->inner_node_);
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to get all scan nodes for nested inner node", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_hybrid_search_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(this);
    if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->filter_))) {
      LOG_WARN("failed to append filter", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->index_table_query_params_.access_exprs_))) {
      LOG_WARN("append rowkey expr to access exprs failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->pri_table_query_params_.access_exprs_))) {
      LOG_WARN("append rowkey expr to access exprs failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->pri_table_query_params_.pushdown_filters_))) {
      LOG_WARN("append pushdown filters to access exprs failed", K(ret));
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY) {
    const ObFullTextQueryNode *fulltext_node = static_cast<const ObFullTextQueryNode *>(this);
    if (OB_FAIL(fulltext_node->collect_exprs(query_exprs, score_exprs))) {
      LOG_WARN("failed to collect exprs", K(ret));
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    const ObVecSearchNode *vec_node = static_cast<const ObVecSearchNode *>(this);
    ObRawExpr *col_expr = static_cast<ObRawExpr *>(vec_node->field_);
    if (OB_FAIL(add_var_to_array_no_dup(query_exprs, col_expr))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_NOT_NULL(vec_node->brute_pushdown_filter_)
               && OB_FAIL(add_var_to_array_no_dup(query_exprs, vec_node->brute_pushdown_filter_))) {
      LOG_WARN("failed to append brute force pushdown filter", K(ret));
    } else {
      for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
        if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(vec_node->extracted_scalar_node_)) {
        if (OB_FAIL(SMART_CALL(vec_node->extracted_scalar_node_->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
          LOG_WARN("failed to collect exprs from extracted scalar node", K(ret));
        }
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    const ObFusionNode *fusion_node = static_cast<const ObFusionNode *>(this);
    if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->from_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->size_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->min_score_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->window_size_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_NOT_NULL(fusion_node->rank_const_) &&
               OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->rank_const_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, fusion_node->rowkey_cols_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(score_exprs, fusion_node->score_cols_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, fusion_node->weight_cols_))) {
      LOG_WARN("failed to append weight_cols_", K(ret));
    } else if (fusion_node->is_top_k_query_ && OB_FAIL(append_array_no_dup(query_exprs, fusion_node->path_top_k_limit_))) {
      LOG_WARN("failed to append path_top_k_limit_", K(ret));
    }
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    const ObBooleanQueryNode *bool_node = static_cast<const ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(bool_node->merged_probe_scalar_node_)) {
      if (OB_FAIL(bool_node->merged_probe_scalar_node_->collect_hybrid_search_exprs(query_exprs, score_exprs))) {
        LOG_WARN("failed to collect exprs from merged probe scalar node", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(bool_node->minimum_should_match_)
        && OB_FAIL(add_var_to_array_no_dup(
            query_exprs, static_cast<ObRawExpr *>(bool_node->minimum_should_match_)))) {
      LOG_WARN("failed to append bool minimum_should_match expr", KR(ret));
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    const ObNestedQueryNode *nested_node = static_cast<const ObNestedQueryNode *>(this);
    if (OB_NOT_NULL(nested_node->inner_node_)) {
      const ObHybridSearchNodeBase *child_node =
          static_cast<const ObHybridSearchNodeBase *>(nested_node->inner_node_);
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to collect exprs for nested inner node", K(ret));
      }
    }
  }

  const ObAsyncIndexDeleteInfo *async_delete_info = get_async_delete_info();
  if (OB_SUCC(ret) && OB_NOT_NULL(async_delete_info)
      && OB_FAIL(async_delete_info->append_doc_id_expr(query_exprs))) {
    LOG_WARN("failed to append async delete doc id expr", K(ret));
  }

  if (OB_SUCC(ret) && node_type_ != INDEX_MERGE_HYBRID_FUSION_SEARCH
      && node_type_ != INDEX_MERGE_HYBRID_NESTED_QUERY) {
    if (OB_ISNULL(boost_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("boost_ is null", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr *>(boost_)))) {
      LOG_WARN("failed to append boost expr", K(ret));
    }
  }

  return ret;
}

int ObHybridSearchNodeBase::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->generate_access_exprs(table_item, opt_ctx)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObScalarQueryNode::generate_access_exprs(
    const TableItem &table_item,
    ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHybridSearchNodeBase::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate scalar child access exprs", K(ret));
  } else if (is_async_
      && OB_FAIL(async_delete_info_.generate_doc_id_column(table_item, opt_ctx))) {
    LOG_WARN("failed to generate async search delete doc id expr", K(ret));
  }
  return ret;
}

int ObScalarQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHybridSearchNodeBase::get_all_index_ids(index_ids))) {
    LOG_WARN("failed to get scalar child index ids", K(ret));
  } else if (is_async_
      && OB_FAIL(async_delete_info_.append_index_id(index_ids))) {
    LOG_WARN("failed to append async search delete index id", K(ret));
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->collect_exprs(query_exprs, score_exprs)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->get_all_index_ids(index_ids)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_vec_infos(ObIArray<ObVecIndexInfo *> &vec_infos)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    if (OB_FAIL(add_var_to_array_no_dup(vec_infos, &vec_node->vec_info_))) {
      LOG_WARN("failed to append vec info", K(ret));
    }
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    ObNestedQueryNode *nested_node = static_cast<ObNestedQueryNode *>(this);
    if (OB_NOT_NULL(nested_node->inner_node_)) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(nested_node->inner_node_);
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to collect vec infos for nested inner node", K(ret));
      }
    }
  }

  return ret;
}

int ObVecSearchNode::init_vec_info(ObSqlSchemaGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (is_brute_force_) {
    vec_info_.main_table_tid_ = table_schema_->get_table_id();
    vec_info_.vec_type_ = ObVecIndexType::VEC_INDEX_BRUTE_FORCE;
    // No vector index exists, so vector_index_param_ stays at default (type_ = VIAT_MAX).
    // All downstream code must check is_brute_force_scan() before reading algorithm type.
    vec_info_.target_vec_column_ = field_;
    if (search_option_ != nullptr) {
      vec_info_.query_param_ = search_option_->param_;
      vec_info_.knn_filter_mode_ = search_option_->filter_mode_;
    }
  } else if (ap_ != NULL && OB_NOT_NULL(ap_->vec_idx_info_)) {
    vec_info_.main_table_tid_ = table_schema_->get_table_id();
    vec_info_.vec_type_ = ObVecIndexType::VEC_INDEX_INVALID;
    vec_info_.selectivity_ = ap_->vec_idx_info_->vec_extra_info_.get_selectivity();
    vec_info_.row_count_ = ap_->vec_idx_info_->vec_extra_info_.get_row_count();
    vec_info_.set_can_use_vec_pri_opt(ap_->vec_idx_info_->vec_extra_info_.can_use_vec_pri_opt());
    vec_info_.vector_index_param_ = ap_->vec_idx_info_->vec_extra_info_.get_vector_index_param();
    vec_info_.adaptive_try_path_ = ap_->vec_idx_info_->vec_extra_info_.adaptive_try_path_;
    vec_info_.can_extract_range_ = ap_->vec_idx_info_->vec_extra_info_.can_extract_range_;
    vec_info_.is_spatial_index_ =  ap_->vec_idx_info_->vec_extra_info_.is_spatial_index_;
    vec_info_.is_multi_value_index_ = ap_->vec_idx_info_->vec_extra_info_.is_multi_value_index_;
    bool is_hybrid = false;
    if (search_option_ != nullptr) {
      vec_info_.query_param_ = search_option_->param_;
      vec_info_.knn_filter_mode_ = search_option_->filter_mode_;
      if (search_option_->filter_mode_ == ObKnnFilterMode::PRE_ADAPTIVE ||
          search_option_->filter_mode_ == ObKnnFilterMode::PRE_KNN ||
          search_option_->filter_mode_ == ObKnnFilterMode::PRE_BRUTE_FORCE) {
        vec_info_.vec_type_ = VEC_INDEX_PRE;
      } else if (search_option_->filter_mode_ == ObKnnFilterMode::POST_FILTER ||
                 search_option_->filter_mode_ == ObKnnFilterMode::POST_INDEX_MERGE) {
        vec_info_.vec_type_ = VEC_INDEX_POST_ITERATIVE_FILTER;
      }
    }
    if (vec_info_.is_hnsw_vec_scan()) {
      uint64_t vid_rowkey_tid = OB_INVALID_ID;
      uint64_t rowkey_vid_tid = OB_INVALID_ID;
      uint64_t delta_buffer_tid = OB_INVALID_ID; // hybrid index log table when is_hybrid is true
      uint64_t index_id_tid = OB_INVALID_ID;
      uint64_t index_snapshot_data_tid = OB_INVALID_ID;
      uint64_t hybrid_index_embedded_tid = OB_INVALID_ID;
      uint64_t vec_idx_tid = main_index_table_id_;
      if (FAILEDx(ObVectorIndexUtil::get_index_tids_for_hnsw_by_prefix(schema_guard->get_schema_guard(),
                                                                                   *table_schema_, // data table schema
                                                                                   vec_idx_tid,
                                                                                   delta_buffer_tid,
                                                                                   index_id_tid,
                                                                                   index_snapshot_data_tid,
                                                                                   hybrid_index_embedded_tid,
                                                                                   is_hybrid))) {
        LOG_WARN("fail to get index tids for hnsw by prefix", K(ret), K(vec_idx_tid), K(*table_schema_));
      } else if (delta_buffer_tid == OB_INVALID_ID || index_id_tid == OB_INVALID_ID || index_snapshot_data_tid == OB_INVALID_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to init aux table id", K(delta_buffer_tid), K(index_id_tid), K(index_snapshot_data_tid), K(ret));
      /* do not change push order, should be same as ObVectorAuxTableIdx */
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(delta_buffer_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(delta_buffer_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(index_id_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(index_id_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(index_snapshot_data_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(index_snapshot_data_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_INVALID_ID != rowkey_vid_tid && OB_FAIL(vec_info_.aux_table_id_.push_back(rowkey_vid_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(rowkey_vid_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_INVALID_ID != vid_rowkey_tid && OB_FAIL(vec_info_.aux_table_id_.push_back(vid_rowkey_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(vid_rowkey_tid), K(vec_info_.aux_table_id_.count()));
      } else if (is_hybrid && OB_FAIL(vec_info_.aux_table_id_.push_back(hybrid_index_embedded_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(hybrid_index_embedded_tid), K(vec_info_.aux_table_id_.count()));
      }
    } else if (vec_info_.is_ivf_vec_scan()) {
      uint64_t centroid_tid = OB_INVALID_ID;
      uint64_t cid_vec_tid = OB_INVALID_ID;
      uint64_t rowkey_cid_tid = OB_INVALID_ID;
      uint64_t extra_tid = OB_INVALID_ID;
      if (OB_ISNULL(field_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("field_ is null for IVF vec scan", K(ret));
      } else if (!table_schema_->is_heap_organized_table()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "IVF vector index hybrid search on non-heap table");
        LOG_WARN("IVF hybrid search only supports heap table (ORGANIZATION=HEAP)", K(ret));
      } else {
        uint64_t vec_col_id = field_->get_column_id();
        ObVectorIndexAlgorithmType algo_type = vec_info_.vector_index_param_.type_;
        if (OB_FAIL(ObVectorIndexUtil::get_latest_avaliable_index_tids_for_ivf(
                schema_guard->get_schema_guard(),
                *table_schema_,
                vec_col_id,
                algo_type,
                centroid_tid,
                cid_vec_tid,
                rowkey_cid_tid,
                extra_tid))) {
          LOG_WARN("fail to get ivf index tids", K(ret), K(vec_col_id), K(algo_type));
        } else if (vec_info_.is_ivf_flat_scan()) {
          if (centroid_tid == OB_INVALID_ID || cid_vec_tid == OB_INVALID_ID || rowkey_cid_tid == OB_INVALID_ID) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to init ivf flat aux table id", K(ret), K(centroid_tid), K(cid_vec_tid), K(rowkey_cid_tid));
          /* do not change push order, should be same as ObVectorAuxTableIdx */
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(centroid_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(centroid_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(cid_vec_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(cid_vec_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(rowkey_cid_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(rowkey_cid_tid));
          }
        } else if (vec_info_.is_ivf_pq_scan()) {
          // For PQ: centroid_tid, pq_code_tid(cid_vec_tid), pq_cid_pid_tid(rowkey_cid_tid), pq_id_tid(extra_tid)
          if (centroid_tid == OB_INVALID_ID || cid_vec_tid == OB_INVALID_ID
              || rowkey_cid_tid == OB_INVALID_ID || extra_tid == OB_INVALID_ID) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to init ivf pq aux table id", K(ret), K(centroid_tid), K(cid_vec_tid), K(rowkey_cid_tid), K(extra_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(centroid_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(centroid_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(cid_vec_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(cid_vec_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(rowkey_cid_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(rowkey_cid_tid));
          } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(extra_tid))) {
            LOG_WARN("fail to push back aux table id", K(ret), K(extra_tid));
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported ivf scan type in hybrid search", K(ret), K(algo_type));
        }
      }
    }
  }
  return ret;
}

int ObVecSearchNode::get_delta_buf_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delta buf table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_DELTA_VID_COL)))) {
      LOG_WARN("failed to add delta vid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_DELTA_TYPE_COL)))) {
      LOG_WARN("failed to add delta type column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_index_id_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index id table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IDX_ID_SCAN;
    
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_SCN_COL)))) {
      LOG_WARN("failed to add index id scn column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_VID_COL)))) {
      LOG_WARN("failed to add index id vid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_TYPE_COL)))) {
      LOG_WARN("failed to add index id type column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_snapshot_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_KEY_COL)))) {
      LOG_WARN("failed to add snapshot key column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_DATA_COL)))) {
      LOG_WARN("failed to add snapshot data column", K(ret));
    } else if (vec_info_.has_get_visible_column() &&
               OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_VISIBLE_COL)))) {
      LOG_WARN("failed to add snapshot visible column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_com_aux_vec_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan() && !vec_info_.is_ivf_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com aux vec table params only for HNSW or IVF", K(ret));
  } else {
    params.table_id_ = vec_info_.main_table_tid_;
    params.scan_type_ = ObTSCIRScanType::OB_VEC_COM_AUX_SCAN;
    params.access_exprs_.reset();
    if (OB_ISNULL(vec_info_.target_vec_column_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target vec column is null", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.target_vec_column_))) {
      LOG_WARN("failed to add target vec column", K(ret));
    } else if (vec_info_.is_ivf_vec_scan()) {
      // IVF COM_AUX scan reads main table directly for brute-force distance computation;
      // rowkey columns are needed to build the result rowkey after distance calculation.
      int64_t rowkey_start = 0;
      if (vec_info_.is_ivf_flat_scan()) {
        rowkey_start = ObVectorIVFFlatColumnIdx::IVF_FLAT_ROWKEY_START;
      } else if (vec_info_.is_ivf_pq_scan()) {
        rowkey_start = ObVectorIVFPQColumnIdx::IVF_PQ_ROWKEY_START;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported ivf scan type in hybrid search", K(ret), K(vec_info_.vector_index_param_.type_));
      }
      for (int64_t i = rowkey_start; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
        if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
          LOG_WARN("failed to add rowkey column to com_aux params", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

// Brute force scans the main table itself: [vector column, rowkey columns...].
// Uses OB_VEC_BRUTE_MAIN_SCAN to distinguish from auxiliary table scans.
int ObVecSearchNode::get_brute_force_main_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> filter_columns;
  if (!vec_info_.is_brute_force_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main table params only for brute force", K(ret));
  } else if (OB_ISNULL(vec_info_.target_vec_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target vec column is null", K(ret));
  } else {
    params.table_id_ = vec_info_.main_table_tid_;
    params.scan_type_ = ObTSCIRScanType::OB_VEC_BRUTE_MAIN_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.target_vec_column_))) {
      LOG_WARN("failed to add target vec column", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
        LOG_WARN("failed to add rowkey column to brute force params", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(brute_pushdown_filter_)) {
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(brute_pushdown_filter_, filter_columns))) {
        LOG_WARN("failed to extract brute force filter columns", K(ret), KPC(brute_pushdown_filter_));
      } else if (OB_FAIL(append_array_no_dup(params.access_exprs_, filter_columns))) {
        LOG_WARN("failed to append brute force filter columns", K(ret), K(filter_columns));
      } else if (OB_FAIL(params.pushdown_filters_.push_back(brute_pushdown_filter_))) {
        LOG_WARN("failed to append brute force pushdown filter", K(ret), KPC(brute_pushdown_filter_));
      }
    }
  }
  return ret;
}

static bool subtree_has_usable_scalar_index(const ObIndexMergeNode *node)
{
  bool has_index = false;
  if (OB_ISNULL(node)) {
    // An invalid tree is rejected by the normal plan generation path.
  } else if (node->is_hybrid_scalar_node()) {
    const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(node);
    has_index = scalar_node->has_index() && OB_NOT_NULL(node->ap_);
  } else {
    for (int64_t i = 0; !has_index && i < node->children_.count(); ++i) {
      has_index = subtree_has_usable_scalar_index(node->children_.at(i));
    }
  }
  return has_index;
}

ObVecBruteFilterStrategy ObVecSearchNode::get_brute_filter_strategy() const
{
  ObVecBruteFilterStrategy strategy = ObVecBruteFilterStrategy::NONE;
  if (!vec_info_.is_brute_force_scan() || filter_nodes_.empty()) {
    // No brute-force filter handling is required.
  } else if (OB_ISNULL(brute_pushdown_filter_)) {
    strategy = ObVecBruteFilterStrategy::BITMAP_ONLY;
  } else {
    bool has_usable_index = false;
    for (int64_t i = 0; !has_usable_index && i < filter_nodes_.count(); ++i) {
      has_usable_index = subtree_has_usable_scalar_index(filter_nodes_.at(i));
    }
    strategy = has_usable_index
        ? ObVecBruteFilterStrategy::PUSHDOWN_WITH_BITMAP
        : ObVecBruteFilterStrategy::PUSHDOWN_ONLY;
  }
  return strategy;
}

int ObVecSearchNode::get_centroid_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_ivf_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid table params only for IVF", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_CENTROID_CID_COL)))) {
      LOG_WARN("failed to add centroid cid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_CENTROID_CENTER_COL)))) {
      LOG_WARN("failed to add centroid center column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_cid_vec_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_ivf_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cid vec table params only for IVF", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN;
    params.access_exprs_.reset();
    if (vec_info_.is_ivf_flat_scan()) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_CID_VEC_CID_COL)))) {
        LOG_WARN("failed to add cid_vec cid column", K(ret));
      } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_CID_VEC_VECTOR_COL)))) {
        LOG_WARN("failed to add cid_vec vector column", K(ret));
      }
      // append main table rowkey columns (from IVF_FLAT_ROWKEY_START)
      for (int64_t i = IVF_FLAT_ROWKEY_START; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
        if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
          LOG_WARN("failed to add rowkey column to cid_vec params", K(ret), K(i));
        }
      }
    } else if (vec_info_.is_ivf_pq_scan()) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_CODE_CID_COL)))) {
        LOG_WARN("failed to add pq code cid column", K(ret));
      } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_CODE_PIDS_COL)))) {
        LOG_WARN("failed to add pq code pids column", K(ret));
      }
      // append main table rowkey columns (from IVF_PQ_ROWKEY_START)
      for (int64_t i = IVF_PQ_ROWKEY_START; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
        if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
          LOG_WARN("failed to add rowkey column to cid_vec params", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObVecSearchNode::get_rowkey_cid_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_ivf_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey cid table params only for IVF", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN;
    params.access_exprs_.reset();
    if (vec_info_.is_ivf_flat_scan()) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_ROWKEY_CID_CID)))) {
        LOG_WARN("failed to add rowkey_cid cid column", K(ret));
      }
      // append main table rowkey columns (from IVF_FLAT_ROWKEY_START)
      for (int64_t i = IVF_FLAT_ROWKEY_START; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
        if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
          LOG_WARN("failed to add rowkey column to rowkey_cid params", K(ret), K(i));
        }
      }
    } else if (vec_info_.is_ivf_pq_scan()) {
      if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_ROWKEY_CID_CID_COL)))) {
        LOG_WARN("failed to add pq rowkey_cid cid column", K(ret));
      } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_ROWKEY_CID_PIDS_COL)))) {
        LOG_WARN("failed to add pq rowkey_cid pids column", K(ret));
      }
      // append main table rowkey columns (from IVF_PQ_ROWKEY_START)
      for (int64_t i = IVF_PQ_ROWKEY_START; OB_SUCC(ret) && i < vec_info_.aux_table_column_.count(); ++i) {
        if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(i)))) {
          LOG_WARN("failed to add rowkey column to rowkey_cid params", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObVecSearchNode::get_special_aux_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_ivf_pq_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("special aux table params only for IVF PQ", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_ID_PID_COL)))) {
      LOG_WARN("failed to add pq id pid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(IVF_PQ_ID_CENTER_COL)))) {
      LOG_WARN("failed to add pq id center column", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::print_blank_space(char *buf, int64_t buf_len, int64_t &pos, int n)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("\n"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  for (int i = 0; i < n && OB_SUCC(ret); i++) {
    if (OB_FAIL(BUF_PRINTF(" "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  return ret;
}

namespace oceanbase
{
namespace sql
{
namespace
{
/** @brief Replace one sort expression and refresh its type only when replacement occurs. */
int replace_hybrid_sort_expr(ObRawExprReplacer &replacer, ObSQLSessionInfo *session,
                              ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (!replacer.empty()) {
    if (OB_FAIL(replacer.replace(expr))) {
      LOG_WARN("failed to replace hybrid sort expression", K(ret));
    } else if (replacer.get_replace_happened()) {
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      } else if (OB_FAIL(expr->formalize(session))) {
        LOG_WARN("failed to formalize replaced hybrid sort expression", K(ret));
      }
    }
  }
  return ret;
}

/** @brief Build a comparison or AND/OR node for the search_after filter. */
int hybrid_after_binary(ObRawExprFactory &factory, ObItemType type,
                         ObRawExpr *left, ObRawExpr *right, ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *expr = nullptr;
  if (OB_FAIL(factory.create_raw_expr(type, expr))) {
    LOG_WARN("failed to create search_after operator expression", K(ret), K(type));
  } else if (OB_FAIL(expr->set_param_exprs(left, right))) {
    LOG_WARN("failed to set search_after operator arguments", K(ret), K(type), KP(left), KP(right));
  } else {
    out = expr;
  }
  return ret;
}

/**
 * @brief Keep rows after search_after using the requested sort order and NULL placement.
 * For non-NULL keys (price ASC, stock DESC): price > p OR (price <=> p AND stock < s).
 */
int build_hybrid_after_filter(ObRawExprFactory &factory, const ObFusionNode &node,
                              ObRawExpr *&filter)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *no = nullptr;
  // Each sort field must have one search_after value.
  if (node.search_after_values_.count() != node.sort_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hybrid search_after and sort item counts differ", K(ret), K(node.search_after_values_.count()),
             K(node.sort_items_.count()));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(factory, ObIntType, 0, no))) {
    LOG_WARN("failed to build search_after false condition", K(ret));
  } else {
    // Start with FALSE: a row equal to the previous page's last row on every key must be excluded.
    filter = no;
  }
  // A field decides the order; equal fields continue with the remaining keys.
  for (int64_t i = node.sort_items_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const OrderItem &item = node.sort_items_.at(i).order_item_;
    ObRawExpr *value = item.expr_;
    ObRawExpr *after = node.search_after_values_.at(i);
    ObRawExpr *equal = nullptr;
    ObRawExpr *comparison = nullptr;
    ObRawExpr *value_null = nullptr;
    ObRawExpr *value_not_null = nullptr;
    ObRawExpr *after_null = nullptr;
    ObRawExpr *after_not_null = nullptr;
    ObRawExpr *nonnull = nullptr;
    ObRawExpr *strict = nullptr;
    ObRawExpr *null_after = nullptr;
    ObRawExpr *same_prefix = nullptr;
    // Use <=> for equal prefixes and >/< for ASC/DESC. Handle NULL placement separately
    // so NULL comparisons do not turn the filter into SQL UNKNOWN.
    if (OB_FAIL(hybrid_after_binary(factory, T_OP_NSEQ, value, after, equal))) {
      LOG_WARN("failed to build search_after null-safe equality", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, item.is_ascending() ? T_OP_GT : T_OP_LT, value, after, comparison))) {
      LOG_WARN("failed to build search_after value comparison", K(ret), K(i), K(item.is_ascending()));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(factory, value, false, value_null))) {
      LOG_WARN("failed to build sort key IS NULL check", K(ret), K(i));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(factory, value, true, value_not_null))) {
      LOG_WARN("failed to build sort key IS NOT NULL check", K(ret), K(i));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(factory, after, false, after_null))) {
      LOG_WARN("failed to build search_after IS NULL check", K(ret), K(i));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(factory, after, true, after_not_null))) {
      LOG_WARN("failed to build search_after IS NOT NULL check", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_AND, value_not_null, after_not_null, nonnull))) {
      LOG_WARN("failed to combine search_after non-null checks", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_AND, nonnull, comparison, strict))) {
      LOG_WARN("failed to guard search_after comparison against NULL", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_AND,
      item.is_null_first() ? after_null : value_null,
      item.is_null_first() ? value_not_null : after_not_null, null_after))) {
      LOG_WARN("failed to build search_after NULL ordering condition", K(ret), K(i), K(item.is_null_first()));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_OR, strict, null_after, strict))) {
      LOG_WARN("failed to combine search_after value and NULL ordering", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_AND, equal, filter, same_prefix))) {
      LOG_WARN("failed to build search_after equal-prefix condition", K(ret), K(i));
    } else if (OB_FAIL(hybrid_after_binary(factory, T_OP_OR, strict, same_prefix, filter))) {
      LOG_WARN("failed to combine search_after field conditions", K(ret), K(i));
    }
  }
  return ret;
}


/** @brief Check whether the chosen query path uses async indexes. */
int inspect_hybrid_sort_query(const ObIndexMergeNode *node, bool &has_async_index)
{
  int ret = OB_SUCCESS;
  if (node == nullptr || !node->is_hybrid_search_node()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid sort query node", K(ret), KP(node));
  } else {
    const ObHybridSearchNodeBase *search = static_cast<const ObHybridSearchNodeBase *>(node);
    // Async-index access carries the information needed to filter deleted rows.
    has_async_index |= search->get_async_delete_info() != nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      if (OB_FAIL(SMART_CALL(inspect_hybrid_sort_query(
          node->children_.at(i), has_async_index)))) {
        LOG_WARN("failed to inspect hybrid sort query child", K(ret), K(node->node_type_), K(i));
      }
    }
    // Bool clauses may not have been copied into children_ yet.
    if (OB_SUCC(ret) && node->children_.empty() && node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      const ObBooleanQueryNode *boolean = static_cast<const ObBooleanQueryNode *>(node);
      const ObIArray<ObIndexMergeNode *> *clauses[] = {
        &boolean->must_nodes_, &boolean->should_nodes_, &boolean->filter_nodes_, &boolean->must_not_nodes_};
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(clauses); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < clauses[i]->count(); ++j) {
          if (OB_FAIL(SMART_CALL(inspect_hybrid_sort_query(
              clauses[i]->at(j), has_async_index)))) {
            LOG_WARN("failed to inspect hybrid sort Boolean clause", K(ret), "clause_group", i, "clause_index", j);
          }
        }
      }
    }
  }
  return ret;
}

} // namespace
} // namespace sql
} // namespace oceanbase

int ObFusionNode::prepare_sort_after(const ObSelectStmt &stmt, const ObDSLQueryInfo &info,
                                    ObRawExprFactory &factory, ObSQLSessionInfo &session,
                                    ObRawExpr *&after_filter)
{
  int ret = OB_SUCCESS;
  bool has_async_index = false;
  after_filter = nullptr;
  if (OB_FAIL(inspect_hybrid_sort_query(this, has_async_index))) {
    LOG_WARN("failed to inspect hybrid sort query", K(ret));
  } else if ((has_after() || has_unique_shard_doc() || !info.sort_lookup_base_columns_.empty())
             && (children_.count() != 1 || has_vector_subquery_
                 || has_hybrid_fusion_op_ || has_rerank()
                 || (!has_after() && stmt.has_group_by()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("candidate query path does not support hybrid cursor sorting", K(ret), K(has_async_index),
              K(children_.count()), K_(has_vector_subquery), K_(has_hybrid_fusion_op), K(has_rerank()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_items_.count(); ++i) {
      const ObFusionSortItem &item = sort_items_.at(i);
      if (!item.is_score_sort_ && !item.is_unique_shard_doc_) {
        // For sort=doc.price, lookup must provide doc even when SELECT omits it.
        ObSEArray<ObRawExpr *, 4> columns;
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(item.order_item_.expr_, columns, false, false))) {
          LOG_WARN("failed to extract hybrid sort lookup columns", K(ret), K(i));
        } else if (OB_FAIL(append_array_no_dup(sort_lookup_access_exprs_, columns))) {
          LOG_WARN("failed to append hybrid sort lookup columns", K(ret), K(i), K(columns.count()));
        }
      }
    }
    if (OB_SUCC(ret) && has_after()) {
      // TableScan filters rows after lookup, before SQL sorting and LIMIT.
      if (OB_FAIL(build_hybrid_after_filter(factory, *this, after_filter))) {
        LOG_WARN("failed to build search_after filter", K(ret), K(sort_items_.count()));
      } else if (OB_FAIL(after_filter->formalize(&session))) {
        LOG_WARN("failed to formalize search_after filter", K(ret));
      }
    }
  }
  return ret;
}

bool ObFusionNode::has_unique_shard_doc() const
{
  bool found = false;
  for (int64_t i = 0; !found && i < sort_items_.count(); ++i) {
    found = sort_items_.at(i).is_unique_shard_doc_;
  }
  return found;
}

ObRawExpr *ObFusionNode::get_shard_doc_expr() const
{
  ObRawExpr *expr = nullptr;
  for (int64_t i = 0; OB_ISNULL(expr) && i < sort_items_.count(); ++i) {
    if (sort_items_.at(i).is_unique_shard_doc_) {
      expr = sort_items_.at(i).order_item_.expr_;
    }
  }
  return expr;
}

int ObFusionNode::replace_sort_exprs(ObRawExprReplacer &replacer, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (!replacer.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_lookup_access_exprs_.count(); ++i) {
      if (OB_FAIL(replace_hybrid_sort_expr(replacer, session, sort_lookup_access_exprs_.at(i)))) {
        LOG_WARN("failed to replace hybrid sort lookup column", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_items_.count(); ++i) {
      if (OB_FAIL(replace_hybrid_sort_expr(replacer, session, sort_items_.at(i).order_item_.expr_))) {
        LOG_WARN("failed to replace hybrid sort key expression", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < search_after_values_.count(); ++i) {
      if (OB_FAIL(replace_hybrid_sort_expr(replacer, session, search_after_values_.at(i)))) {
        LOG_WARN("failed to replace hybrid search_after expression", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObFusionNode::collect_sort_calc_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_items_.count(); ++i) {
    if (OB_FAIL(add_var_to_array_no_dup(exprs, sort_items_.at(i).order_item_.expr_))) {
      LOG_WARN("failed to collect hybrid sort expression", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_array_no_dup(exprs, search_after_values_))) {
      LOG_WARN("failed to collect hybrid search_after expressions", K(ret), K(search_after_values_.count()));
    } else if (OB_FAIL(append_array_no_dup(exprs, sort_lookup_access_exprs_))) {
      LOG_WARN("failed to collect hybrid sort lookup columns", K(ret));
    }
  }
  return ret;
}

int ObFusionNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (is_top_k_query_) {
    if (OB_FAIL(BUF_PRINTF("fusion node:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS) {
      ObRawExpr *limit = size_;
      ObRawExpr *window_size = window_size_;
      ObRawExpr *min_score = min_score_;
      ObRawExpr *rank_const = rank_const_;
      const char *method_str = nullptr;
      if (method_ == ObFusionMethod::WEIGHT_SUM) {
        method_str = "WEIGHT_SUM";
      } else if (method_ == ObFusionMethod::RRF) {
        method_str = "RRF";
      } else if (method_ == ObFusionMethod::MINMAX_NORMALIZER) {
        method_str = "MINMAX_NORMALIZER";
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fusion method", K(ret), K(method_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(" method=%s", method_str))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(limit, type))) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(window_size, type))) {
      } else if (OB_NOT_NULL(rank_const) && OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_NOT_NULL(rank_const) && FALSE_IT(EXPLAIN_PRINT_EXPR(rank_const, type))) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(min_score, type))) {
      }
    }
  } else {
    if (OB_FAIL(BUF_PRINTF("search index:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 2)))) {
      LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObBooleanQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("boolean node:"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (must_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("must:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (should_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("should:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
        }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("filter:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (must_not_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("must not:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}

int ObBooleanQueryNode::extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr)
{
  int ret = OB_SUCCESS;
  extracted_expr = nullptr;
  ObSEArray<ObRawExpr*, 8> all_filters;
  
  if (!must_not_nodes_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("bool node with must_not is not supported for filter extraction", K(ret));
  } else if (!should_nodes_.empty()
             && OB_NOT_NULL(minimum_should_match_)
             && (!ob_is_integer_type(minimum_should_match_->get_value().get_type())
                 || minimum_should_match_->get_value().get_int() < 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("dynamic minimum_should_match is not supported for filter extraction", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < must_nodes_.count(); ++i) {
    ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(must_nodes_.at(i));
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
    } else {
      ObRawExpr *child_filter = nullptr;
      if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
        if (OB_NOT_SUPPORTED != ret) {
          LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
        }
      } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
        LOG_WARN("failed to push back must child filter", K(ret));
      }
    }
  }
  
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_nodes_.count(); ++i) {
    ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(filter_nodes_.at(i));
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
    } else {
      ObRawExpr *child_filter = nullptr;
      if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
        if (OB_NOT_SUPPORTED != ret) {
          LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
        }
      } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
        LOG_WARN("failed to push back filter child filter", K(ret));
      }
    }
  }
  
  if (OB_SUCC(ret) && !should_nodes_.empty()) {
    ObSEArray<ObRawExpr*, 4> should_filters;
    
    if (min_should_match_ == 0) {
      // do nothing
    } else if (min_should_match_ == 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < should_nodes_.count(); ++i) {
        ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(should_nodes_.at(i));
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child is null", K(ret), K(i));
        } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
        } else {
          ObRawExpr *child_filter = nullptr;
          if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
            if (OB_NOT_SUPPORTED != ret) {
              LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
            }
          } else if (OB_NOT_NULL(child_filter) && OB_FAIL(should_filters.push_back(child_filter))) {
            LOG_WARN("failed to push back should child filter", K(ret));
          }
        }
      }
      
      if (OB_SUCC(ret) && !should_filters.empty()) {
        ObRawExpr *or_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_or_exprs(expr_factory, should_filters, or_expr))) {
          LOG_WARN("failed to build or expr from should filters", K(ret));
        } else if (OB_NOT_NULL(or_expr) && OB_FAIL(all_filters.push_back(or_expr))) {
          LOG_WARN("failed to push back should or expr", K(ret));
        }
      }
    } else if (min_should_match_ >= should_nodes_.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < should_nodes_.count(); ++i) {
        ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(should_nodes_.at(i));
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child is null", K(ret), K(i));
        } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
        } else {
          ObRawExpr *child_filter = nullptr;
          if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
            if (OB_NOT_SUPPORTED != ret) {
              LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
            }
          } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
            LOG_WARN("failed to push back should child filter", K(ret));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("minimum_should_match between 1 and count is not supported", 
               K(ret), K(min_should_match_), K(should_nodes_.count()));
    }
  }
  
  if (FAILEDx(ObRawExprUtils::build_and_expr(expr_factory, all_filters, extracted_expr))) {
    LOG_WARN("failed to build final and expr", K(ret), K(all_filters.count()));  
  }
  
  return ret;
}

int ObScalarQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("scalar node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (has_valid_index_) {
    if (OB_FAIL(BUF_PRINTF("index name=%.*s", index_name_.length(), index_name_.ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_valid_index_) {
    ObIArray<ObRawExpr*> &range_cond = filter_;
    EXPLAIN_PRINT_EXPRS(range_cond, type);
  } else {
    ObIArray<ObRawExpr*> &filter = filter_;
    EXPLAIN_PRINT_EXPRS(filter, type);
  }
  return ret;
}

int ObScalarQueryNode::extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr)
{
  int ret = OB_SUCCESS;
  extracted_expr = nullptr;
  
  if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory, filter_, extracted_expr))) {
    LOG_WARN("failed to build and expr from filters", K(ret), K(filter_.count()));
  }
  
  return ret;
}

bool ObScalarQueryNode::can_keep_imprecise_range() const
{
  bool can_keep = false;
  const ObRawExpr *filter = nullptr;
  const ObRawExpr *column_expr = nullptr;
  const ObRawExpr *pattern_expr = nullptr;
  if (ap_->is_search_index_path()) {
    // search index can not extract range for like expression
  } else if (filter_.count() == 1
             && OB_NOT_NULL(filter = filter_.at(0))
             && filter->get_expr_type() == T_OP_LIKE
             && filter->get_param_count() >= 2
             && OB_NOT_NULL(column_expr = ObRawExprUtils::skip_inner_added_expr(filter->get_param_expr(0)))
             && OB_NOT_NULL(pattern_expr = ObRawExprUtils::skip_inner_added_expr(filter->get_param_expr(1)))
             && column_expr->is_column_ref_expr()
             && pattern_expr->is_const_expr()
             && column_expr->get_result_type().is_string_or_lob_locator_type()
             && pattern_expr->get_result_type().is_string_or_lob_locator_type()) {
    can_keep = true;
  }
  return can_keep;
}

int ObScalarQueryNode::add_pd_filter_for_imprecise_range()
{
  int ret = OB_SUCCESS;
  ObRawExpr *filter = nullptr;
  if (OB_UNLIKELY(filter_.count() != 1) || OB_ISNULL(filter = filter_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hybrid scalar filter", K(ret), K(filter_.count()), KPC(filter));
  } else if (OB_FAIL(add_var_to_array_no_dup(index_table_query_params_.pushdown_filters_, filter))) {
    LOG_WARN("failed to add imprecise like filter to index scan", K(ret), KPC(filter));
  }
  return ret;
}

int ObVecSearchNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  const char *metric_name = nullptr;
  const ObRawExpr *distance_expr = vec_info_.sort_key_.expr_;
  if (is_brute_force_ && OB_NOT_NULL(distance_expr)) {
    switch (distance_expr->get_expr_type()) {
      case T_FUN_SYS_COSINE_DISTANCE:
        metric_name = "COSINE";
        break;
      case T_FUN_SYS_L2_DISTANCE:
        metric_name = "L2";
        break;
      case T_FUN_SYS_NEGATIVE_INNER_PRODUCT:
        metric_name = "IP";
        break;
      default:
        break;
    }
  }
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("vector node:"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (is_brute_force_ && OB_FAIL(BUF_PRINTF(" search method=BRUTE_FORCE"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (!is_brute_force_ &&
             OB_FAIL(BUF_PRINTF(" index name=%.*s", index_name_.length(), index_name_.ptr()))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_NOT_NULL(metric_name) && OB_FAIL(BUF_PRINTF(", metric=%s", metric_name))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (search_option_ != nullptr) {
    if (OB_FAIL(BUF_PRINTF(", filter_mode=%d", search_option_->filter_mode_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (search_option_->is_set_num_candidates_ &&
               OB_FAIL(BUF_PRINTF(", num_candidates=%d", search_option_->num_candidates_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (search_option_->param_.is_set_ef_search_ &&
               OB_FAIL(BUF_PRINTF(", ef_search=%d", search_option_->param_.ef_search_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (search_option_->param_.is_set_refine_k_ &&
               OB_FAIL(BUF_PRINTF(", refine_k=%f", search_option_->param_.refine_k_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_nodes_.count() > 0) {
   if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count + 2));
    } else if (OB_FAIL(BUF_PRINTF("filter:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}

bool ObHybridSearchGenerator::is_search_subquery(ObIndexMergeNode *node) const
{
  return node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY ||
         node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY ||
         node->node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY ||
         node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY;
}

bool ObHybridSearchGenerator::is_knn_subquery(ObIndexMergeNode *node) const
{
  return node->node_type_ == INDEX_MERGE_HYBRID_KNN;
}

int ObHybridSearchGenerator::collect_child_boost_and_update_flags(
    const ObDSLQueryInfo *dsl_info,
    ObFusionNode *fusion_node,
    ObIndexMergeNode *sub_node,
    ObConstRawExpr *one_boost_expr,
    int64_t child_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dsl_info) || OB_ISNULL(fusion_node) || OB_ISNULL(sub_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dsl_info, fusion_node or sub_node is null", K(ret), KP(dsl_info), KP(fusion_node), KP(sub_node));
  } else {
    ObHybridSearchNodeBase *top_score_node = static_cast<ObHybridSearchNodeBase *>(sub_node);

    ObRawExpr *path_top_k_limit = nullptr;
    ObRawExpr *weight_expr = is_search_subquery(sub_node) ? dsl_info->query_top_level_boost_ : top_score_node->boost_;
    if (OB_FAIL(fusion_node->weight_cols_.push_back(weight_expr))) {
      LOG_WARN("failed to push back weight expr", K(ret), K(child_idx));
    } else if (is_search_subquery(sub_node)) {
      if (fusion_node->has_search_subquery_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multiple search subquery", K(ret));
      } else {
        fusion_node->has_search_subquery_ = true;
        fusion_node->search_index_ = child_idx;
        path_top_k_limit = fusion_node->window_size_;
      }
    } else if (is_knn_subquery(sub_node)) {
      fusion_node->has_vector_subquery_ = true;
      path_top_k_limit = static_cast<ObVecSearchNode *>(top_score_node)->get_topk_limit_expr();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(path_top_k_limit)) {
      fusion_node->path_top_k_limit_.push_back(path_top_k_limit);
    }
  }
  return ret;
}

int ObHybridSearchGenerator::split_domain_contains_node(const ObDSLScalarQuery *scalar_query,
                                                        ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExpr *filter_expr = nullptr;
  if (OB_ISNULL(scalar_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null scalar query", K(ret), KP(scalar_query));
  } else if (OB_ISNULL(filter_expr = scalar_query->scalar_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter expr", K(ret), KPC(scalar_query));
  } else if (OB_ISNULL(filter_expr = ObRawExprUtils::skip_inner_added_expr(filter_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter expr", K(ret), KPC(filter_expr));
  } else if (filter_expr->get_expr_type() == T_FUN_SYS_JSON_CONTAINS ||
             filter_expr->get_expr_type() == T_FUNC_SYS_ARRAY_CONTAINS_ALL) {
    bool has_search_index = false;
    uint8_t cons_encode_type = 0;
    if (OB_FAIL(get_search_index_cons_encode_type(filter_expr, has_search_index, cons_encode_type))) {
      LOG_WARN("failed to get search index constraint encode type", K(ret), KPC(filter_expr));
    } else if (!has_search_index) {
      // no search index, no need to split
    } else if (filter_expr->get_expr_type() == T_FUN_SYS_JSON_CONTAINS) {
      if (OB_FAIL(split_json_contains(scalar_query, filter_expr, cons_encode_type, split_node))) {
        LOG_WARN("failed to split json contains node", K(ret));
      }
    } else if (filter_expr->get_expr_type() == T_FUNC_SYS_ARRAY_CONTAINS_ALL) {
      if (OB_FAIL(split_array_contains_all(scalar_query, filter_expr, split_node))) {
        LOG_WARN("failed to split array contains all node", K(ret));
      }
    }
  } else {
    // no need to split
  }
  return ret;
}

int ObHybridSearchGenerator::split_json_contains(const ObDSLScalarQuery *scalar_query,
                                                 ObRawExpr *filter_expr,
                                                 uint8_t cons_encode_type,
                                                 ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(scalar_query) || OB_ISNULL(plan_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(scalar_query), KP(plan_), KP(allocator_));
  } else if (OB_ISNULL(exec_ctx = plan_->get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr factory", K(ret));
  } else {
    const ObRawExpr *candidate_expr = nullptr;
    if (filter_expr->get_param_count() == 2) {
      candidate_expr = filter_expr->get_param_expr(1);
    }
    if (OB_ISNULL(candidate_expr) || !candidate_expr->is_const_expr() ||
       !ObJsonExprHelper::is_convertible_to_json(candidate_expr->get_result_type().get_type())) {
      // do nothing
    } else {
      bool got_value = false;
      ObObj result;
      ObArenaAllocator tmp_allocator(ObModIds::OB_SQL_EXPR_CALC);
      ObIJsonBase *j_base = nullptr;
      bool can_extract = false;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                           candidate_expr,
                                                           result,
                                                           got_value,
                                                           *allocator_))) {
        LOG_WARN("failed to calc const expr", K(ret), KPC(candidate_expr));
      } else if (!got_value || result.is_null()) {
        // no need to split
      } else if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(result,
                                                                         exec_ctx,
                                                                         false,
                                                                         allocator_,
                                                                         j_base))) {
        LOG_WARN("failed to refine range json value const", K(ret));
      } else if (OB_ISNULL(j_base)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get json base", K(ret));
      } else if (j_base->is_json_scalar(j_base->json_type())) {
        // scalar, no need to split, need to add scalar constraint during query range extraction
      } else if (j_base->json_type() != common::ObJsonNodeType::J_ARRAY) {
        // not an array, no need to split
      } else if (j_base->element_count() == 0) {
        // empty array: no scalar sub-predicates to split, leave split_node as nullptr.
        // aligning with the SQL-form json_contains(col, '[]') behavior.
      } else if (OB_FAIL(ObSearchIndexConstraint::is_json_scalar_or_array_match(j_base, cons_encode_type,
                                                                                can_extract))) {
        LOG_WARN("failed to check json array match", K(ret));
      } else if (!can_extract) {
        // not all member json scalar or json string length can safety to compare after search
        // index value encoding, no need to split.
      } else {
        // array, need to split
        // e.g. json_contains(doc, '[1,2,3]') -> 
        // json_contains(doc, json_extract('[1,2,3]', '$[0]')) AND
        // json_contains(doc, json_extract('[1,2,3]', '$[1]')) AND
        // json_contains(doc, json_extract('[1,2,3]', '$[2]'))
        const int64_t element_count = j_base->element_count();
        ObBooleanQueryNode *bool_node = nullptr;
        ObSqlArray<ObIndexMergeNode*> *split_bool_slot = nullptr;
        ObConstRawExpr *split_boost_expr = nullptr;
        if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate boolean node", K(ret));
        } else {
          bool_node->origin_expr_ = filter_expr;
          split_bool_slot = scalar_query->need_cal_score_ ? &bool_node->must_nodes_ : &bool_node->filter_nodes_;
          if (OB_FAIL(build_split_boost_expr(scalar_query, element_count, split_boost_expr))) {
            LOG_WARN("failed to build split boost expr", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
          ObRawExpr *json_extract_expr = nullptr;
          ObSysFunRawExpr *new_expr = nullptr;
          ObScalarQueryNode *child_node = nullptr;
          char *json_path = nullptr;
          const static int64_t MAX_JSON_PATH_SIZE = 32;
          if (OB_ISNULL(json_path = static_cast<char *>(allocator_->alloc(MAX_JSON_PATH_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate json path", K(ret));
          } else if (OB_FALSE_IT(snprintf(json_path, MAX_JSON_PATH_SIZE, "$[%ld]", i))) {
          } else if (OB_FAIL(ObRawExprUtils::build_json_extract_expr(*expr_factory,
                                                                      *plan_->get_optimizer_context().get_session_info(),
                                                                      const_cast<ObRawExpr*>(candidate_expr),
                                                                      ObString::make_string(json_path),
                                                                      json_extract_expr))) {
            LOG_WARN("failed to build json extract expr", K(ret));
          } else if (OB_ISNULL(json_extract_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json extract expr is null", K(ret));
          } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_CONTAINS, new_expr))) {
            LOG_WARN("failed to create json contains expr", K(ret));
          } else if (OB_FALSE_IT(new_expr->set_func_name(N_JSON_CONTAINS))) {
          } else if (OB_FAIL(new_expr->set_param_exprs(filter_expr->get_param_expr(0), json_extract_expr))) {
            LOG_WARN("failed to set json contains params", K(ret));
          } else if (OB_FAIL(new_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
            LOG_WARN("failed to formalize json contains expr", K(ret));
          } else if (OB_FALSE_IT(new_expr->set_is_inner_split_contains_expr(true))) {
          } else if (OB_FAIL(generate_scalar_node(new_expr, child_node))) {
            LOG_WARN("failed to generate scalar node", K(ret));
          } else if (OB_UNLIKELY(1 != child_node->pri_table_query_params_.pushdown_filters_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected pushdown filter count", K(ret),
              K(child_node->pri_table_query_params_.pushdown_filters_.count()));
          } else if (FALSE_IT(child_node->pri_table_query_params_.pushdown_filters_.at(0) = filter_expr)) {
            // replace the pushdown filter with the original filter
          } else if (OB_FAIL(split_bool_slot->push_back(child_node))) {
            LOG_WARN("failed to push back child node", K(ret));
          } else {
            ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(child_node);
            new_node->is_scoring_ = scalar_query->need_cal_score_;
            new_node->is_top_level_scoring_ = false;
            new_node->boost_ = split_boost_expr;
          }
        }
        if (OB_SUCC(ret)) {
          // add json array count constraint
          PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
          int64_t extra = ObSearchIndexConstraint::make_extra(
              ObSearchIndexConstraint::JSON_ARRAY_COUNT, cons_encode_type, element_count);
          ObConstraintExtra cons_extra;
          cons_extra.extra_ = extra;
          if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
              ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
            LOG_WARN("failed to push back expr constraint", K(ret));
          } else {
            split_node = bool_node;
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::split_array_contains_all(const ObDSLScalarQuery *scalar_query,
                                                      ObRawExpr *filter_expr,
                                                      ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(scalar_query) || OB_ISNULL(plan_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(scalar_query), KP(plan_), KP(allocator_));
  } else if (OB_ISNULL(exec_ctx = plan_->get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr factory", K(ret));
  } else {
    const ObRawExpr *array_expr = nullptr;
    const ObRawExpr *candidate_expr = nullptr;
    if (filter_expr->get_param_count() == 2) {
      array_expr = filter_expr->get_param_expr(0);
      candidate_expr = filter_expr->get_param_expr(1);
    }
    if (OB_ISNULL(array_expr) || !array_expr->has_flag(IS_COLUMN)) {
      // do nothing
    } else if (OB_ISNULL(candidate_expr) || !candidate_expr->is_const_expr()) {
      // do nothing
    } else {
      bool got_value = false;
      ObObj result;
      ObArenaAllocator tmp_allocator(ObModIds::OB_SQL_EXPR_CALC);
      const common::ObSqlCollectionInfo *coll_info = nullptr;
      const common::ObSqlCollectionInfo *col_coll_info = nullptr;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                           candidate_expr,
                                                           result,
                                                           got_value,
                                                           *allocator_))) {
        LOG_WARN("failed to calc const expr", K(ret), KPC(candidate_expr));
      } else if (!got_value || result.is_null() || !result.is_collection_sql_type()) {
        // no need to split
      } else if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(candidate_expr, exec_ctx, coll_info))) {
        LOG_WARN("failed to get collection info", K(ret));
      } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collection info is null", K(ret), KPC(coll_info));
      } else if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(array_expr, exec_ctx, col_coll_info))) {
        LOG_WARN("failed to get column collection info", K(ret));
      } else if (OB_ISNULL(col_coll_info) || OB_ISNULL(col_coll_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column collection info is null", K(ret));
      } else {
        ObString data_str = result.get_string();
        common::ObIArrayType *arr_obj = nullptr;
        bool can_extract = true;
        const bool need_array_string_constraint =
          ObSearchIndexConstraint::need_array_string_constraint(*col_coll_info);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, result, data_str))) {
          LOG_WARN("failed to read array data", K(ret));
        } else if (OB_FAIL(common::ObArrayTypeObjFactory::construct(tmp_allocator,
                                                                    *coll_info->collection_meta_,
                                                                    arr_obj,
                                                                    true))) {
          LOG_WARN("failed to construct array obj", K(ret));
        } else if (OB_FAIL(arr_obj->init(data_str))) {
          LOG_WARN("failed to init array obj", K(ret));
        } else if (need_array_string_constraint &&
            OB_FAIL(ObSearchIndexConstraint::is_array_string_length_match(arr_obj, can_extract))) {
          LOG_WARN("failed to check array string length", K(ret));
        } else if (!can_extract) {
          // do nothing
        } else {
          const int64_t element_count = arr_obj->size();
          ObBooleanQueryNode *bool_node = nullptr;
          ObSqlArray<ObIndexMergeNode*> *split_bool_slot = nullptr;
          ObConstRawExpr *split_boost_expr = nullptr;
          if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate boolean node", K(ret));
          } else {
            bool_node->origin_expr_ = filter_expr;
            split_bool_slot = scalar_query->need_cal_score_ ? &bool_node->must_nodes_ : &bool_node->filter_nodes_;
            if (OB_FAIL(build_split_boost_expr(scalar_query, element_count, split_boost_expr))) {
              LOG_WARN("failed to build split boost expr", K(ret));
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
            ObConstRawExpr *idx_expr = nullptr;
            ObSysFunRawExpr *element_at_expr = nullptr;
            ObSysFunRawExpr *new_expr = nullptr;
            ObScalarQueryNode *child_node = nullptr;
            // Build element_at(array, index) expression, note: element_at uses 1-based index
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, i + 1, idx_expr))) {
              LOG_WARN("failed to build const int expr", K(ret), K(i));
            } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ELEMENT_AT, element_at_expr))) {
              LOG_WARN("failed to create element_at expr", K(ret));
            } else if (OB_FALSE_IT(element_at_expr->set_func_name(N_ELEMENT_AT))) {
            } else if (OB_FAIL(element_at_expr->set_param_exprs(
                const_cast<ObRawExpr*>(candidate_expr), idx_expr))) {
              LOG_WARN("failed to set element_at params", K(ret));
            } else if (OB_FAIL(element_at_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
              LOG_WARN("failed to formalize element_at expr", K(ret));
            } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ARRAY_CONTAINS, new_expr))) {
              LOG_WARN("failed to create array contains expr", K(ret));
            } else if (OB_FALSE_IT(new_expr->set_func_name(N_ARRAY_CONTAINS))) {
            } else if (OB_FAIL(new_expr->set_param_exprs(filter_expr->get_param_expr(0), element_at_expr))) {
              LOG_WARN("failed to set array contains params", K(ret));
            } else if (OB_FAIL(new_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
              LOG_WARN("failed to formalize array contains expr", K(ret));
            } else if (OB_FALSE_IT(new_expr->set_is_inner_split_contains_expr(true))) {
            } else if (OB_FAIL(generate_scalar_node(new_expr, child_node))) {
              LOG_WARN("failed to generate scalar node", K(ret));
            } else if (OB_FAIL(split_bool_slot->push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            } else {
              ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(child_node);
              new_node->is_scoring_ = scalar_query->need_cal_score_;
              new_node->is_top_level_scoring_ = false;
              new_node->boost_ = split_boost_expr;
            }
          }
          if (OB_SUCC(ret)) {
            PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
            // add array string length constraint
            if (need_array_string_constraint) {
              int64_t extra = ObSearchIndexConstraint::make_extra(
                ObSearchIndexConstraint::ARRAY_STRING_LENGTH, 0, 0);
              ObConstraintExtra cons_extra;
              cons_extra.extra_ = extra;
              if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
                  ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
                LOG_WARN("failed to push back expr constraint", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              // add array count constraint
              int64_t extra = ObSearchIndexConstraint::make_extra(
                ObSearchIndexConstraint::ARRAY_ELEMENT_COUNT, 0, element_count);
              ObConstraintExtra cons_extra;
              cons_extra.extra_ = extra;
              if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
                  ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
                LOG_WARN("failed to push back expr constraint", K(ret));
              } else {
                split_node = bool_node;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::check_node_match_index_merge_column_hint(const ObIndexMergeNode *node,
                                                                       bool &is_match) const
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> column_ids;
  is_match = true;
  if (OB_ISNULL(index_merge_hint_column_ids_)) {
    // Automatic SQL-to-DSL without a structured hint does not restrict columns.
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(node->filter_, column_ids))) {
    LOG_WARN("failed to extract column ids from index merge node", K(ret), KPC(node));
  } else if (column_ids.empty()) {
    is_match = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < column_ids.count(); ++i) {
    is_match = ObOptimizerUtil::find_item(*index_merge_hint_column_ids_, column_ids.at(i));
  }
  return ret;
}
