/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_CG
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_cluster_version.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_define.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "share/logical_table/ob_logical_table_define.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_join_order.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
namespace sql
{

typedef int (*ObGenerateHybridSearchCtDefFunc)(ObHybridSearchCgService &,
                                               ObLogTableScan &,
                                               const ObHybridSearchNodeBase *,
                                               ObDASBaseCtDef *&);

template <typename NodeType, typename CtDefType>
int generate_hybrid_search_ctdef_impl(ObHybridSearchCgService &cg_service,
                                      ObLogTableScan &op,
                                      const ObHybridSearchNodeBase *node,
                                      ObDASBaseCtDef *&ctdef)
{
  int ret = OB_SUCCESS;
  CtDefType *new_ctdef = nullptr;
  if (OB_FAIL(cg_service.generate_ctdef(op, static_cast<const NodeType*>(node), new_ctdef))) {
    LOG_WARN("failed to generate ctdef", K(ret));
  } else if (OB_ISNULL(new_ctdef)) {
    if (op.is_logical_table()) {
      // [LT hybrid CG] nullptr 为逻辑表降级信号，向上层传播
      ctdef = nullptr;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new ctdef is null on non-logical-table", K(ret));
    }
  } else {
    ctdef = new_ctdef;
  }
  return ret;
}

static ObGenerateHybridSearchCtDefFunc GENERATE_FUNCS[1 << (sizeof(ObIndexMergeType) * 8)];

struct ObHybridSearchCgInit
{
  ObHybridSearchCgInit()
  {
    #define REGISTER_GEN_FUNC(NODE_TYPE, NODE_CLASS, CTDEF_CLASS) \
      GENERATE_FUNCS[NODE_TYPE] = &generate_hybrid_search_ctdef_impl<NODE_CLASS, CTDEF_CLASS>;

    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_SCALAR_QUERY, ObScalarQueryNode, ObDASScalarCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_BOOLEAN_QUERY, ObBooleanQueryNode, ObDASBooleanQueryCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_FUSION_SEARCH, ObFusionNode, ObDASFusionCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_FTS_QUERY, ObFullTextQueryNode, ObDASBaseCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_KNN, ObVecSearchNode, ObDASVecIndexDriverCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_NESTED_QUERY, ObNestedQueryNode, ObDASNestedQueryCtDef);
    #undef REGISTER_GEN_FUNC
  }
};

static ObHybridSearchCgInit HYBRID_SEARCH_CG_INIT;

#define GENERATE_HYBRID_SEARCH_CTDEF(cg_service, op, node, ctdef)                        \
  do {                                                                                   \
    if (OB_ISNULL(node)) {                                                               \
      ret = OB_ERR_UNEXPECTED;                                                           \
      LOG_WARN("node is null", K(ret));                                                  \
    } else if (node->node_type_ >= ARRAYSIZEOF(GENERATE_FUNCS) ||                        \
               nullptr == GENERATE_FUNCS[node->node_type_]) {                            \
      ret = OB_ERR_UNEXPECTED;                                                           \
      LOG_WARN("unexpected node type", K(node->node_type_), K(ret));                     \
    } else if (OB_FAIL(GENERATE_FUNCS[node->node_type_](cg_service, op, node, ctdef))) { \
      LOG_WARN("failed to generate ctdef", K(ret));                                      \
    }                                                                                    \
  } while(0)

namespace {

// [LT hybrid CG] Return true if a hybrid-search child is a logical-table implicit
// PK prefix filter (single-column namespace_id or ltable_id equality/range predicate
// that CG may safely skip).
//
// Rationale: logical-table KV keys are [ns_id][lt_id][...]; the scan already
// isolates ns_id/lt_id, so repeating the same column filter in the DSL is tautology.
//
// Returns true only when all of:
//   1. node type is INDEX_MERGE_HYBRID_SCALAR_QUERY
//   2. ObScalarQueryNode::filter_.count() == 1
//   3. the filter references exactly one column
//   4. that column name is "namespace_id" or "ltable_id" (case_compare)
// Otherwise returns false (conservative: do not skip).
static bool is_logical_table_implicit_pk_filter_node_(const ObHybridSearchNodeBase *node)
{
  bool bret = false;
  if (OB_NOT_NULL(node) && node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(node);
    if (scalar_node->filter_.count() == 1) {
      ObRawExpr *filter = scalar_node->filter_.at(0);
      common::ObSEArray<ObRawExpr *, 4> col_exprs;
      if (OB_NOT_NULL(filter)
          && OB_SUCCESS == ObRawExprUtils::extract_column_exprs(filter, col_exprs)
          && col_exprs.count() == 1
          && OB_NOT_NULL(col_exprs.at(0))
          && col_exprs.at(0)->is_column_ref_expr()) {
        const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(col_exprs.at(0));
        if (0 == col->get_column_name().case_compare("namespace_id")
            || 0 == col->get_column_name().case_compare("ltable_id")) {
          bret = true;
        }
      }
    }
  }
  return bret;
}

// [LT hybrid CG] Count non-implicit-PK nodes in a bool clause array, i.e. the
// effective clause count after skipping logical-table implicit PK filters.
static int64_t count_non_implicit_pk_nodes_(const common::ObIArray<ObIndexMergeNode *> &nodes)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < nodes.count(); ++i) {
    if (!is_logical_table_implicit_pk_filter_node_(
            static_cast<const ObHybridSearchNodeBase *>(nodes.at(i)))) {
      ++cnt;
    }
  }
  return cnt;
}

// [LT hybrid CG] must_not-only-on-LT universe-source predicate.
// When all of the following hold, the caller must keep one PK filter from
// filter_nodes_ as a universe filter clause; otherwise a pure must_not bool has
// no leading iterator at runtime:
//   - op.is_logical_table()
//   - must_nodes_ / should_nodes_ are empty
//   - filter_nodes_ is non-empty and every entry is a skippable PK filter
//   - must_not_nodes_ contains at least one non-PK filter (eff_must_not > 0)
// The preserved PK filter scalar acts as a clean universe under LT prefix scan
// (ns/lt=X is tautology-redundant there).
static bool need_preserve_pk_filter_for_must_not_only_(
    const ObLogTableScan &op,
    const ObBooleanQueryNode *bn)
{
  bool bret = false;
  if (op.is_logical_table() && OB_NOT_NULL(bn)
      && bn->must_nodes_.empty() && bn->should_nodes_.empty()
      && bn->filter_nodes_.count() > 0 && bn->must_not_nodes_.count() > 0) {
    const bool all_filter_is_pk = (0 == count_non_implicit_pk_nodes_(bn->filter_nodes_));
    bret = all_filter_is_pk && count_non_implicit_pk_nodes_(bn->must_not_nodes_) > 0;
  }
  return bret;
}

// Return true if the subtree rooted at `node` contains any nested_query
// (possibly `node` itself). Walks both the bool-clause arrays and generic
// children_, so shapes like must_not: [bool{must:[nested{...}]}] are also
// detected.
static bool subtree_contains_nested_query_(const ObHybridSearchNodeBase *node)
{
  bool bret = false;
  if (OB_ISNULL(node)) {
    // stop
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    bret = true;
  } else {
    if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      const ObBooleanQueryNode *bn = static_cast<const ObBooleanQueryNode *>(node);
      const common::ObIArray<ObIndexMergeNode *> *clause_arrays[] = {
        &bn->must_nodes_, &bn->should_nodes_, &bn->filter_nodes_, &bn->must_not_nodes_
      };
      for (int64_t a = 0; !bret && a < ARRAYSIZEOF(clause_arrays); ++a) {
        const common::ObIArray<ObIndexMergeNode *> &clause_nodes = *clause_arrays[a];
        for (int64_t i = 0; !bret && i < clause_nodes.count(); ++i) {
          bret = subtree_contains_nested_query_(
              static_cast<const ObHybridSearchNodeBase *>(clause_nodes.at(i)));
        }
      }
    }
    for (int64_t i = 0; !bret && i < node->children_.count(); ++i) {
      bret = subtree_contains_nested_query_(
          static_cast<const ObHybridSearchNodeBase *>(node->children_.at(i)));
    }
  }
  return bret;
}

// Parent-scope negation: any nested_query anywhere under must_not produces
// parent-scope doc_ids via the nested→parent join, so a bool of shape
// {must_not: [ ... nested{...} ... ]} at parent scope is semantically valid
// even when we are not currently generating a nested_query subtree
// (is_generating_nested_query_ == false).
static bool must_not_contains_nested_query_(const ObBooleanQueryNode *bn)
{
  bool bret = false;
  if (OB_NOT_NULL(bn)) {
    for (int64_t i = 0; !bret && i < bn->must_not_nodes_.count(); ++i) {
      bret = subtree_contains_nested_query_(
          static_cast<const ObHybridSearchNodeBase *>(bn->must_not_nodes_.at(i)));
    }
  }
  return bret;
}

static void try_update_best_implicit_pk_scalar_(ObScalarQueryNode *candidate,
                                                ObScalarQueryNode *&scalar_node)
{
  if (OB_ISNULL(candidate)) {
  } else if (OB_ISNULL(scalar_node)) {
    scalar_node = candidate;
  } else if (OB_ISNULL(scalar_node->pri_table_ap_)
             && OB_NOT_NULL(candidate->pri_table_ap_)) {
    scalar_node = candidate;
  }
}

// [LT hybrid CG] Effective bool-clause counts after skipping implicit PK filters.
struct BoolClauseEffCounts
{
  BoolClauseEffCounts() : must_(0), should_(0), filter_(0), must_not_(0) {}
  int64_t positive_count() const { return must_ + should_ + filter_; }
  int64_t must_;
  int64_t should_;
  int64_t filter_;
  int64_t must_not_;
  TO_STRING_KV(K_(must), K_(should), K_(filter), K_(must_not));
};

static void populate_boolean_query_ctdef_layout_(ObDASBooleanQueryCtDef *ctdef,
                                                 int64_t &min_should_match,
                                                 const BoolClauseEffCounts &eff,
                                                 const bool has_merged_probe)
{
  int64_t current_offset = 0;
  if (min_should_match > eff.should_) {
    min_should_match = eff.should_;
  }
  ctdef->set_min_should_match(min_should_match);
  if (eff.must_ > 0) {
    ctdef->set_must(current_offset, eff.must_);
    current_offset += eff.must_;
  }
  if (eff.should_ > 0) {
    ctdef->set_should(current_offset, eff.should_);
    current_offset += eff.should_;
  }
  if (eff.filter_ > 0) {
    ctdef->set_filter(current_offset, eff.filter_);
    current_offset += eff.filter_;
  }
  if (eff.must_not_ > 0) {
    ctdef->set_must_not(current_offset, eff.must_not_);
    current_offset += eff.must_not_;
  }
  if (has_merged_probe) {
    ctdef->set_merged_probe(current_offset);
  }
}

// [LT hybrid CG] Compute effective bool-clause counts for a logical table.
// Single-column namespace_id / ltable_id filters in the DSL are tautology-redundant
// under KV prefix scan and may be skipped at CG time.
static void calc_lt_bool_clause_eff_counts_(const ObLogTableScan &op,
                                            const ObBooleanQueryNode *bn,
                                            BoolClauseEffCounts &eff)
{
  eff.must_ = count_non_implicit_pk_nodes_(bn->must_nodes_);
  eff.should_ = count_non_implicit_pk_nodes_(bn->should_nodes_);
  eff.filter_ = count_non_implicit_pk_nodes_(bn->filter_nodes_);
  eff.must_not_ = count_non_implicit_pk_nodes_(bn->must_not_nodes_);
  // [LT hybrid CG] must_not-only-on-LT: keep one PK filter from filter_nodes_ as a
  // universe filter clause so pure must_not has a leading iterator at runtime.
  if (need_preserve_pk_filter_for_must_not_only_(op, bn)
      && eff.filter_ < bn->filter_nodes_.count()) {
    ++eff.filter_;
    LOG_INFO("[LT hybrid CG] preserve one PK filter as universe for must_not-only",
             K(eff),
             "raw_filter", bn->filter_nodes_.count(),
             "raw_must_not", bn->must_not_nodes_.count());
  }
}

// [LT hybrid CG] Decision when no effective positive clause remains after skipping
// implicit PK filters:
//   - If effective must_not remains: invalid semantics; error (aligned with
//     ob_hybrid_search_dsl_resolver.cpp on the raw DSL).
//   - Otherwise: set degrade_to_full_scan; caller returns nullptr for upstream
//     full-scan degrade. Two cases:
//       (a) raw DSL had positive clauses but all were ns/lt skips (common baseline)
//       (b) raw DSL was empty (formerly an error path) — keep degrade semantics
//     Both appear externally as "no DSL predicate, full scan".
static int decide_lt_bool_no_positive_clause_(const ObBooleanQueryNode *bn,
                                              const BoolClauseEffCounts &eff,
                                              bool &degrade_to_full_scan)
{
  int ret = OB_SUCCESS;
  degrade_to_full_scan = false;
  if (eff.must_not_ > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "bool query, it should have at least one positive clause");
    LOG_WARN("[LT hybrid CG] bool query must have at least one positive clause "
             "after skipping logical PK filters",
             KR(ret), K(eff),
             "raw_must", bn->must_nodes_.count(),
             "raw_should", bn->should_nodes_.count(),
             "raw_filter", bn->filter_nodes_.count(),
             "raw_must_not", bn->must_not_nodes_.count());
  } else {
    degrade_to_full_scan = true;
    LOG_TRACE("[LT hybrid CG] boolean query degrade to full scan after skipping logical PK filters",
              K(eff),
              "raw_must", bn->must_nodes_.count(),
              "raw_should", bn->should_nodes_.count(),
              "raw_filter", bn->filter_nodes_.count(),
              "raw_must_not", bn->must_not_nodes_.count());
  }
  return ret;
}

// [LT hybrid CG] Sole entry for logical-table boolean query ctdef layout:
// effective counts → degrade decision → layout population.
static int setup_lt_boolean_query_ctdef_layout_(const ObLogTableScan &op,
                                                const ObBooleanQueryNode *bn,
                                                ObDASBooleanQueryCtDef *ctdef,
                                                int64_t &min_should_match,
                                                const bool can_generate_must_not_only,
                                                bool &degrade_to_full_scan)
{
  int ret = OB_SUCCESS;
  degrade_to_full_scan = false;
  BoolClauseEffCounts eff;
  calc_lt_bool_clause_eff_counts_(op, bn, eff);
  if (0 == eff.positive_count() && !can_generate_must_not_only) {
    if (OB_FAIL(decide_lt_bool_no_positive_clause_(bn, eff, degrade_to_full_scan))) {
      LOG_WARN("bool query has no positive clause after LT PK skip", KR(ret), K(eff));
    }
  } else {
    populate_boolean_query_ctdef_layout_(
        ctdef, min_should_match, eff, OB_NOT_NULL(bn->merged_probe_scalar_node_));
  }
  return ret;
}

} // anonymous namespace

int ObHybridSearchCgService::find_logical_table_implicit_pk_scalar_node_(
    ObHybridSearchNodeBase *node,
    ObScalarQueryNode *&scalar_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else {
    if (is_logical_table_implicit_pk_filter_node_(node)) {
      try_update_best_implicit_pk_scalar_(static_cast<ObScalarQueryNode *>(node), scalar_node);
    }
    if (OB_SUCC(ret) && node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode *>(node);
      const common::ObIArray<ObIndexMergeNode *> *clause_arrays[] = {
        &bool_node->must_nodes_,
        &bool_node->should_nodes_,
        &bool_node->filter_nodes_,
        &bool_node->must_not_nodes_
      };
      for (int64_t a = 0; OB_SUCC(ret) && a < ARRAYSIZEOF(clause_arrays); ++a) {
        const common::ObIArray<ObIndexMergeNode *> &clause_nodes = *clause_arrays[a];
        for (int64_t i = 0; OB_SUCC(ret) && i < clause_nodes.count(); ++i) {
          ObHybridSearchNodeBase *child =
              static_cast<ObHybridSearchNodeBase *>(clause_nodes.at(i));
          if (OB_FAIL(find_logical_table_implicit_pk_scalar_node_(child, scalar_node))) {
            LOG_WARN("failed to find implicit pk scalar in boolean clause", K(ret), K(a), K(i));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase *>(node->children_.at(i));
      if (OB_FAIL(find_logical_table_implicit_pk_scalar_node_(child, scalar_node))) {
        LOG_WARN("failed to find implicit pk scalar in children", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_logical_table_degrade_search_ctdef_(
    ObLogTableScan &op,
    ObFusionNode *fusion_node,
    ObDASBaseCtDef *&search_ctdef)
{
  int ret = OB_SUCCESS;
  search_ctdef = nullptr;
  ObScalarQueryNode *scalar_node = nullptr;
  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  common::ObSEArray<ObDASScalarScanCtDef *, 4> scalar_scan_ctdefs;
  if (OB_ISNULL(fusion_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion node is null", K(ret));
  } else if (fusion_node->search_index_ != 0
             || !fusion_node->has_search_subquery_
             || fusion_node->has_vector_subquery_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fusion shape for logical table degrade",
             K(ret), K(fusion_node->search_index_),
             K(fusion_node->has_search_subquery_),
             K(fusion_node->has_vector_subquery_));
  } else if (OB_FAIL(find_logical_table_implicit_pk_scalar_node_(fusion_node, scalar_node))) {
    LOG_WARN("failed to find implicit pk scalar node for degrade", K(ret));
  } else if (OB_ISNULL(scalar_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no implicit pk scalar node found for degrade", K(ret));
  } else if (OB_FAIL(generate_ctdef(op, scalar_node, scalar_ctdef))) {
    LOG_WARN("failed to generate scalar ctdef for degrade", K(ret));
  } else if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null after generate_ctdef", K(ret));
  } else {
    collect_scan_ctdefs_(scalar_ctdef, scalar_scan_ctdefs);
    if (scalar_scan_ctdefs.count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("degrade scalar has no scalar scan ctdef", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < scalar_scan_ctdefs.count(); ++i) {
      ObDASScalarScanCtDef *scan_ctdef = scalar_scan_ctdefs.at(i);
      if (OB_ISNULL(scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scalar scan ctdef is null", K(ret), K(i));
      } else if (scan_ctdef->logical_table_aux_type_ != share::LT_AUX_PRIMARY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("degrade scalar scan is not logical primary aux",
                 K(ret), K(i), "aux_type", static_cast<int>(scan_ctdef->logical_table_aux_type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_ctdef(op, fusion_node, fusion_ctdef))) {
      LOG_WARN("failed to generate fusion ctdef for degrade", K(ret));
    } else if (OB_ISNULL(fusion_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion ctdef is null", K(ret));
    } else if (OB_ISNULL(cg_.phy_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan is null", K(ret));
    } else if (OB_ISNULL(fusion_ctdef->children_ = OB_NEW_ARRAY(
        ObDASBaseCtDef *, &cg_.phy_plan_->get_allocator(), 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate fusion children failed", K(ret));
    } else {
      fusion_ctdef->children_cnt_ = 1;
      fusion_ctdef->children_[0] = scalar_ctdef;
      search_ctdef = fusion_ctdef;
      LOG_TRACE("[LT hybrid CG] built degrade search ctdef: Fusion -> Scalar (implicit PK full scan)");
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_hybrid_search_ctdef(ObLogTableScan &op,
                                                          ObTableScanCtDef &tsc_ctdef,
                                                          ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASBaseCtDef *search_ctdef = nullptr;
  const IndexMergePath *path = nullptr;
  ObDASAsyncIndexExecMode async_exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  ObSEArray<ObRawExpr *, 16> logical_fts_filter_exprs;
  const ObIArray<ObRawExpr *> *saved_logical_fts_filter_exprs = logical_fts_filter_exprs_;
  // RAII restore: guarantees the member is reset on every exit path (incl. future early returns)
  // before the stack-local logical_fts_filter_exprs goes out of scope.
  DEFER(logical_fts_filter_exprs_ = saved_logical_fts_filter_exprs);
  if (OB_ISNULL(op.get_access_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_UNLIKELY(!op.use_index_merge())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index merge is not used", K(ret));
  } else {
    path = static_cast<const IndexMergePath*>(op.get_access_path());
    ObHybridSearchNodeBase *root = static_cast<ObHybridSearchNodeBase*>(path->root_);
    if (op.is_logical_table()
        && OB_FAIL(collect_logical_fts_filter_exprs_(root, logical_fts_filter_exprs))) {
      LOG_WARN("failed to collect logical fts filter exprs", K(ret));
    } else if (FALSE_IT(logical_fts_filter_exprs_ =
                   op.is_logical_table() ? &logical_fts_filter_exprs : nullptr)) {
    } else if (OB_FAIL(generate_hybrid_search_node_ctdef(op, root, search_ctdef))) {
      LOG_WARN("failed to generate hybrid search ctdef", K(ret));
    } else if (OB_ISNULL(search_ctdef)) {
      // [LT hybrid CG] 整棵 search 子树降级（仅逻辑表）：Lookup -> Fusion -> Scalar -> Primary 全扫。
      ObDASBaseCtDef *degrade_search_ctdef = nullptr;
      if (OB_UNLIKELY(!op.is_logical_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hybrid search degrade unexpectedly on non-logical-table path", K(ret));
      } else if (OB_ISNULL(root)
                 || root->node_type_ != INDEX_MERGE_HYBRID_FUSION_SEARCH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("degrade path expects fusion root", K(ret), "root_type",
                 OB_NOT_NULL(root) ? root->node_type_ : -1);
      } else if (OB_FAIL(mock_das_scan_ctdef(op, tsc_ctdef.scan_ctdef_))) {
        LOG_WARN("failed to mock das scan ctdef in degrade path", K(ret));
      } else if (OB_FAIL(generate_logical_table_degrade_search_ctdef_(
                     op, static_cast<ObFusionNode *>(root), degrade_search_ctdef))) {
        LOG_WARN("failed to generate degrade search ctdef", K(ret));
      } else if (OB_ISNULL(degrade_search_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("degrade search ctdef is null", K(ret));
      } else if (OB_FAIL(generate_final_lookup_ctdef(op, tsc_ctdef,
                                                     *degrade_search_ctdef,
                                                     root_ctdef))) {
        LOG_WARN("failed to generate final lookup ctdef in degrade path", K(ret));
      } else {
        LOG_TRACE("[LT hybrid CG] hybrid search degraded to full scan via Fusion->Scalar");
      }
    } else if (OB_FAIL(normalize_async_fts_boolean_domain(
                   search_ctdef, async_exec_mode))) {
      LOG_WARN("failed to normalize async fts boolean domain", K(ret));
    } else if (OB_FAIL(normalize_async_knn_filter_domains_(search_ctdef))) {
      LOG_WARN("failed to normalize async knn filter domain", K(ret));
    } else if (search_ctdef->op_type_ == DAS_OP_FUSION_QUERY
        && OB_FAIL(try_alloc_topk_collect_ctdef(static_cast<ObDASFusionCtDef*>(search_ctdef)))) {
      LOG_WARN("failed to try alloc topk collect ctdef", K(ret));
    } else if (!op.get_index_back()) {
      ObArray<ObRawExpr *> non_pushdown_filters;
      ObArray<ObRawExpr *> lookup_pushdown_filters;
      if (OB_FAIL(prepare_main_scan_access_ctdef(op,
                                                 tsc_ctdef.scan_ctdef_,
                                                 non_pushdown_filters,
                                                 lookup_pushdown_filters))) {
        LOG_WARN("failed to prepare main scan access ctdef", K(ret));
      } else {
        tsc_ctdef.flashback_item_.need_scn_ |= tsc_ctdef.scan_ctdef_.check_need_fill_scn();
        root_ctdef = search_ctdef;
      }
    } else if (OB_FAIL(generate_final_lookup_ctdef(op, tsc_ctdef, *search_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate final lookup ctdef", K(ret));
    } else if (OB_FAIL(synthesize_parent_domain_must_not_required(op, root_ctdef))) {
      LOG_WARN("failed to synthesize parent domain must_not required", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(mock_das_scan_ctdef(op, tsc_ctdef.scan_ctdef_))) {
      LOG_WARN("failed to mock das scan ctdef", K(ret));
    } else if (OB_SUCC(ret) && op.is_logical_table()
               && OB_FAIL(rewrite_for_logical_table_(op, root_ctdef, tsc_ctdef.scan_ctdef_))) {
      LOG_WARN("rewrite for logical table failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      tsc_ctdef.scan_ctdef_.use_async_index_group_alignment_ =
          ASYNC_INDEX_EXEC_GROUP == async_exec_mode;
    }
  }
  return ret;
}

int ObHybridSearchCgService::collect_logical_fts_filter_exprs_(
    ObHybridSearchNodeBase *node,
    ObIArray<ObRawExpr *> &filter_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // skip
  } else {
    if (node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY
        && !is_logical_table_implicit_pk_filter_node_(node)) {
      const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(node);
      for (int64_t i = 0; OB_SUCC(ret) && i < scalar_node->filter_.count(); ++i) {
        ObRawExpr *expr = scalar_node->filter_.at(i);
        // [LT BM25] T_FUN_MATCH_AGAINST must be evaluated against the inverted
        // index and cannot run on the primary stats scan. Exclude it from the
        // logical FTS stats filter to avoid a null-vector crash at runtime; BM25
        // tf/df are still sourced from postings independently.
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null scalar filter expr", K(ret), K(i));
        } else if (expr->has_flag(CNT_MATCH_EXPR)) {
          LOG_TRACE("[LT BM25] skip MATCH AGAINST in hybrid stats filter", KPC(expr));
        } else if (OB_FAIL(add_var_to_array_no_dup(filter_exprs, expr))) {
          LOG_WARN("failed to collect logical fts filter expr", K(ret), KPC(expr));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase *>(node->children_.at(i));
      if (OB_FAIL(SMART_CALL(collect_logical_fts_filter_exprs_(child, filter_exprs)))) {
        LOG_WARN("failed to collect child filter exprs", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_hybrid_search_node_ctdef(ObLogTableScan &op,
                                                               ObHybridSearchNodeBase *node,
                                                               ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t children_cnt = 0;
  ObDASBaseCtDef *new_root_ctdef = nullptr;
  GENERATE_HYBRID_SEARCH_CTDEF(*this, op, node, new_root_ctdef);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_root_ctdef)) {
    // [LT hybrid CG] Step 2 降级信号：boolean 节点全部 positive clause 被跳过
    // 直接向上传递 nullptr，由更高一层（fusion / 顶层 generate_hybrid_search_ctdef）兜底
    root_ctdef = nullptr;
    LOG_TRACE("[LT hybrid CG] sub ctdef is null, propagate degrade signal upwards",
             "node_type", OB_NOT_NULL(node) ? node->node_type_ : -1);
    return ret;
  } else if (OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan is null", K(ret));
  } else if (FALSE_IT(children_cnt = node->children_.count())) {
  } else if (node->is_leaf_node()) {
    // children cnt of scan node is not necessarily equal to children_ of the search node
    if (node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
      if (OB_UNLIKELY(children_cnt > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scalar query node should not have children", K(ret));
      }
    }
  } else if (children_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected children count for non-leaf node", KR(ret), K(node->node_type_), K(children_cnt));
  } else {
    common::ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    // [LT hybrid CG] must_not-only-on-LT 保留 1 个 PK filter 作为 universe。
    // children_ 顺序见 ob_hybrid_search_node.cpp 中 bool_node->children_ 拼装：
    //   must → should → filter → must_not。
    // 只在 filter 段 [filter_begin, filter_end) 内做"保留第一个 PK filter"。
    const ObBooleanQueryNode *lt_bn = nullptr;
    bool preserve_one_pk_in_filter = false;
    int64_t lt_filter_begin = 0;
    int64_t lt_filter_end = 0;
    if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      lt_bn = static_cast<const ObBooleanQueryNode *>(node);
      preserve_one_pk_in_filter = need_preserve_pk_filter_for_must_not_only_(op, lt_bn);
      if (preserve_one_pk_in_filter) {
        lt_filter_begin = lt_bn->must_nodes_.count() + lt_bn->should_nodes_.count();
        lt_filter_end = lt_filter_begin + lt_bn->filter_nodes_.count();
      }
    }
    bool pk_filter_preserved = false;
    if (OB_ISNULL(new_root_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, children_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate children failed", K(ret));
    } else {
      int64_t actual_idx = 0;
      bool saved_is_nested = is_generating_nested_query_;
      if (node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
        is_generating_nested_query_ = true;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
        ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(node->children_.at(i));
        ObDASBaseCtDef *child_ctdef = nullptr;
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null child", K(ret));
        } else if (op.is_logical_table()
                   && is_logical_table_implicit_pk_filter_node_(child)) {
          // [LT hybrid CG] must_not-only-on-LT 时保留 filter 段第一个 PK filter
          const bool in_filter_group =
              preserve_one_pk_in_filter && i >= lt_filter_begin && i < lt_filter_end;
          if (in_filter_group && !pk_filter_preserved) {
            pk_filter_preserved = true;
            LOG_INFO("[LT hybrid CG] keep logical PK filter as universe (must_not-only)",
                     "child_node_type", child->node_type_, K(i));
            if (OB_FAIL(SMART_CALL(generate_hybrid_search_node_ctdef(op, child, child_ctdef)))) {
              LOG_WARN("failed to generate hybrid search node ctdef", K(ret));
            } else if (OB_ISNULL(child_ctdef)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("preserved PK filter ctdef unexpectedly null", K(ret));
            } else {
              new_root_ctdef->children_[actual_idx++] = child_ctdef;
            }
            continue;
          }
          // [LT hybrid CG] 跳过逻辑表隐式 PK filter（与 Step 2 的 eff_* 计算保持完全一致）
          LOG_TRACE("[LT hybrid CG] skip logical PK filter scalar node in CG",
                   "child_node_type", child->node_type_);
          continue;
        } else if (OB_FAIL(SMART_CALL(generate_hybrid_search_node_ctdef(op, child, child_ctdef)))) {
          LOG_WARN("failed to generate hybrid search node ctdef", K(ret));
        } else if (OB_ISNULL(child_ctdef)) {
          // [LT hybrid CG] 子节点降级返回 nullptr → 同样跳过，不写入 children
          LOG_TRACE("[LT hybrid CG] child ctdef is null (subtree degraded), skip",
                   "child_node_type", child->node_type_);
          continue;
        } else {
          new_root_ctdef->children_[actual_idx++] = child_ctdef;
        }
      }
      if (node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
        is_generating_nested_query_ = saved_is_nested;
      }
      if (OB_SUCC(ret)) {
        new_root_ctdef->children_cnt_ = actual_idx;
        if (actual_idx == 0) {
          // [LT hybrid CG] 非叶节点的所有子节点都被跳过 → 本节点也降级，向上传 nullptr
          LOG_TRACE("[LT hybrid CG] all children skipped, degrade current node",
                   "node_type", node->node_type_);
          new_root_ctdef = nullptr;
        }
      }
    }
  }

  // Wrap an async index leaf with its shared delete index. Nested subtrees defer
  // the doc-id exclusion until their outermost boundary restores BASE scope.
  const ObAsyncIndexDeleteInfo *async_delete_info =
      OB_NOT_NULL(node) ? node->get_async_delete_info() : nullptr;
  if (OB_SUCC(ret) && OB_NOT_NULL(async_delete_info)
      && !is_generating_nested_query_) {
    ObDASBaseCtDef *wrapped_ctdef = nullptr;
    if (OB_UNLIKELY(!async_delete_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async delete info", K(ret), KPC(async_delete_info));
    } else if (OB_FAIL(wrap_async_index_with_delete(
        op,
        async_delete_info->get_delete_idx_tid(),
        async_delete_info->get_delete_doc_id_column(),
        async_delete_info->get_delete_rowscn_expr(),
        new_root_ctdef,
        wrapped_ctdef))) {
      LOG_WARN("failed to wrap async index ctdef with delete", K(ret));
    } else {
      new_root_ctdef = wrapped_ctdef;
    }
  }

  if (OB_SUCC(ret)) {
    // Populate fusion ctdef's nested_rowid_exprs_ AFTER children ctdefs are generated, so
    // the doc_id/nested_key column rt_expr_ pointers (created by leaf scalar-scan CG) are
    // available. See populate_nested_rowid_exprs() for details.
    // [LT hybrid CG] 逻辑表 fusion 子树降级为 nullptr 时跳过 populate，向上传递降级信号；
    // 物理表 fusion ctdef 为 null 仍视为异常。
    if (OB_NOT_NULL(node) && node->node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
      if (OB_ISNULL(new_root_ctdef)) {
        if (!op.is_logical_table()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fusion ctdef is null on non-logical-table path", K(ret));
        }
      } else {
        const ObFusionNode *fusion_node = static_cast<const ObFusionNode *>(node);
        ObDASFusionCtDef *fusion_ctdef = static_cast<ObDASFusionCtDef *>(new_root_ctdef);
        if (OB_FAIL(populate_nested_rowid_exprs(op, fusion_node, fusion_ctdef))) {
          LOG_WARN("failed to populate nested_rowid_exprs", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(node) && node->node_type_ == INDEX_MERGE_HYBRID_NESTED_QUERY) {
    // Mark every scalar-scan ctdef in this nested subtree as NESTED-scope. This drives
    // ObDASScalarScanRtDef::generate_op to route them through ObDASSortOp instead of
    // ObDASBitmapOp at runtime, since the rowkey is multi-column [doc_id, nested_key].
    if (OB_FAIL(mark_subtree_nested_scope(new_root_ctdef))) {
      LOG_WARN("failed to mark subtree as nested scope", K(ret));
    }
    // For nested(bool(only must_not)): synthesize a full-subtree scan as the required
    // source and inject it into the bool ctdef so must_not has something to exclude from.
    if (OB_SUCC(ret) && OB_FAIL(synthesize_child_domain_must_not_required(op, new_root_ctdef))) {
      LOG_WARN("failed to synthesize child-domain must_not required source", K(ret));
    }
    const bool is_outermost_nested = !is_generating_nested_query_;
    if (OB_SUCC(ret) && is_outermost_nested) {
      const ObAsyncIndexDeleteInfo *async_delete_info = nullptr;
      if (OB_FAIL(collect_async_delete_info(node, async_delete_info))) {
        LOG_WARN("failed to collect nested async delete info", K(ret));
      } else if (OB_NOT_NULL(async_delete_info)) {
        ObDASBaseCtDef *wrapped_ctdef = nullptr;
        if (OB_FAIL(wrap_async_index_with_delete(
            op,
            async_delete_info->get_delete_idx_tid(),
            async_delete_info->get_delete_doc_id_column(),
            async_delete_info->get_delete_rowscn_expr(),
            new_root_ctdef,
            wrapped_ctdef))) {
          LOG_WARN("failed to wrap outer nested boundary with async delete", K(ret),
              KPC(async_delete_info));
        } else {
          new_root_ctdef = wrapped_ctdef;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = new_root_ctdef;
  }

  return ret;
}

int ObHybridSearchCgService::wrap_async_index_with_delete(
    ObLogTableScan &op,
    uint64_t delete_idx_tid,
    ObColumnRefRawExpr *delete_doc_id_column,
    ObRawExpr *delete_rowscn_expr,
    ObDASBaseCtDef *index_ctdef,
    ObDASBaseCtDef *&boolean_ctdef)
{
  int ret = OB_SUCCESS;
  boolean_ctdef = nullptr;
  ObDASScalarScanCtDef *delete_scan_ctdef = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(index_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(cg_.phy_plan_), KP(index_ctdef));
  } else if (OB_UNLIKELY(OB_INVALID_ID == delete_idx_tid
      || OB_ISNULL(delete_doc_id_column)
      || OB_ISNULL(delete_rowscn_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async delete index info", KR(ret), K(delete_idx_tid),
        KP(delete_doc_id_column), KP(delete_rowscn_expr));
  } else if (OB_FAIL(generate_async_delete_scan_ctdef(
      op, delete_idx_tid, delete_doc_id_column, delete_rowscn_expr, delete_scan_ctdef))) {
    LOG_WARN("failed to generate async delete scan ctdef", KR(ret));
  } else if (OB_FAIL(build_async_fts_domain_wrapper(
                 index_ctdef, delete_scan_ctdef, boolean_ctdef))) {
    LOG_WARN("failed to build async index wrapper", KR(ret));
  }
  return ret;
}

int ObHybridSearchCgService::get_async_fts_wrapper_children_(
    ObDASBaseCtDef *wrapper_ctdef,
    ObDASBaseCtDef *&query_ctdef,
    ObDASScalarScanCtDef *&delete_scan_ctdef) const
{
  int ret = OB_SUCCESS;
  query_ctdef = nullptr;
  delete_scan_ctdef = nullptr;
  ObBooleanSubClause<ObIDASSearchCtDef> must;
  ObBooleanSubClause<ObIDASSearchCtDef> must_not;
  if (OB_ISNULL(wrapper_ctdef)
      || OB_UNLIKELY(DAS_OP_BOOLEAN_QUERY != wrapper_ctdef->op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async fts wrapper", K(ret), KP(wrapper_ctdef));
  } else {
    ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<ObDASBooleanQueryCtDef *>(wrapper_ctdef);
    ObIDASSearchCtDef *query = nullptr;
    ObIDASSearchCtDef *deletion = nullptr;
    if (OB_UNLIKELY(!boolean_ctdef->is_async())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("boolean ctdef is not an async fts wrapper", K(ret));
    } else if (OB_FAIL(boolean_ctdef->must(must))) {
      LOG_WARN("failed to get async wrapper must clause", K(ret));
    } else if (OB_FAIL(boolean_ctdef->must_not(must_not))) {
      LOG_WARN("failed to get async wrapper must-not clause", K(ret));
    } else if (OB_UNLIKELY(1 != must.count() || 1 != must_not.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected analyzed async fts wrapper shape", K(ret),
               K(must.count()), K(must_not.count()));
    } else if (OB_FAIL(must.get(0, query))) {
      LOG_WARN("failed to get async wrapper query", K(ret));
    } else if (OB_FAIL(must_not.get(0, deletion))) {
      LOG_WARN("failed to get async wrapper deletion", K(ret));
    } else if (OB_ISNULL(query) || OB_ISNULL(deletion)
               || OB_UNLIKELY(DAS_OP_SCALAR_SCAN_QUERY != deletion->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async fts wrapper child", K(ret), KP(query), KP(deletion));
    } else {
      // The analyzer validates the complete wrapper contract. Planning only needs mutable
      // pointers for the later atomic rewrite.
      query_ctdef = query;
      delete_scan_ctdef = static_cast<ObDASScalarScanCtDef *>(deletion);
    }
  }
  return ret;
}

int ObHybridSearchCgService::build_async_fts_domain_wrapper(
    ObDASBaseCtDef *query_ctdef,
    ObDASScalarScanCtDef *delete_scan_ctdef,
    ObDASBaseCtDef *&wrapper_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASBooleanQueryCtDef *boolean_ctdef = nullptr;
  wrapper_ctdef = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(query_ctdef) || OB_ISNULL(delete_scan_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async fts domain wrapper argument", K(ret), KP(cg_.phy_plan_),
             KP(query_ctdef), KP(delete_scan_ctdef));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
                 DAS_OP_BOOLEAN_QUERY, cg_.phy_plan_->get_allocator(), boolean_ctdef))) {
    LOG_WARN("failed to allocate async fts domain wrapper", K(ret));
  } else if (OB_ISNULL(boolean_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async fts domain wrapper is null", K(ret));
  } else if (OB_ISNULL(boolean_ctdef->children_ =
                 OB_NEW_ARRAY(ObDASBaseCtDef *, &cg_.phy_plan_->get_allocator(), 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate async fts domain children", K(ret));
  } else {
    // The outer wrapper only performs Group-level deletion exclusion; preserve the original
    // Boolean must/should/filter/MSM and scoring semantics.
    boolean_ctdef->children_cnt_ = 2;
    boolean_ctdef->children_[0] = query_ctdef;
    boolean_ctdef->children_[1] = delete_scan_ctdef;
    boolean_ctdef->set_must(0, 1);
    boolean_ctdef->set_must_not(1, 1);
    boolean_ctdef->set_min_should_match(0);
    boolean_ctdef->set_is_async(true);
    boolean_ctdef->set_is_scoring(
        static_cast<ObIDASSearchCtDef *>(query_ctdef)->is_scoring());
    boolean_ctdef->set_is_top_level_scoring(
        static_cast<ObIDASSearchCtDef *>(query_ctdef)->is_top_level_scoring());
    wrapper_ctdef = boolean_ctdef;
  }
  return ret;
}

int ObHybridSearchCgService::normalize_async_fts_boolean_domain(
    ObDASBaseCtDef *&root_ctdef,
    ObDASAsyncIndexExecMode &exec_mode)
{
  int ret = OB_SUCCESS;
  ObDASAsyncIndexExecMode planned_exec_mode = ASYNC_INDEX_EXEC_LEGACY;
  if (OB_ISNULL(root_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null hybrid search ctdef", K(ret));
  } else {
    ObDASAsyncIndexDomainAnalysis analysis;
    if (OB_FAIL(ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
            root_ctdef, analysis))) {
      LOG_WARN("failed to analyze async index normalization eligibility", K(ret));
    } else if (!analysis.has_async_fts_wrapper_) {
      // An unwrapped runtime tree has no code-generation rewrite target.
    } else if (ASYNC_INDEX_DOMAIN_MIXED_SYNC == analysis.type_) {
      // A selected synchronous scalar/full-text path makes the whole query ineligible.
      // The analyzer has already validated every async wrapper; preserve the original
      // CtDef tree so runtime uses the existing execution path.
      LOG_TRACE("keep legacy async index tree for mixed synchronous query", K(analysis));
    } else if (ASYNC_INDEX_DOMAIN_NON_ALIGNABLE == analysis.type_) {
      // Structurally valid but non-alignable query shapes stay on the existing path.
      LOG_TRACE("keep legacy async index tree for non-alignable query", K(analysis));
    } else if (ASYNC_INDEX_DOMAIN_NONE == analysis.type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async wrapper has no async index domain", K(ret), K(analysis));
    } else if (ASYNC_INDEX_EXEC_LEGACY == analysis.get_exec_mode()) {
      // A single logical async index with one semantic root retains its existing tree.
      LOG_TRACE("keep single async index tree unchanged", K(analysis));
    } else {
      bool has_domain_rewrite = false;
      if (OB_FAIL(normalize_async_fts_boolean_domain_impl(
              root_ctdef, has_domain_rewrite))) {
        LOG_WARN("failed to normalize eligible async index domain", K(ret), K(analysis));
      } else if (has_domain_rewrite) {
        planned_exec_mode = ASYNC_INDEX_EXEC_GROUP;
      } else {
        LOG_TRACE("keep legacy async index tree because no rewrite was planned", K(analysis));
      }
    }
  }
  if (OB_SUCC(ret)) {
    exec_mode = planned_exec_mode;
  }
  return ret;
}

int ObHybridSearchCgService::normalize_async_knn_filter_domains_(
    ObDASBaseCtDef *root_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ctdef)) {
    // Leave malformed/optional children to the ordinary CtDef validation path. This
    // pass only discovers independent KNN filter domains and must not reject unrelated trees.
  } else if (DAS_OP_VEC_INDEX_DRIVER == root_ctdef->op_type_) {
    // Vector driver child[0] is the vector scan and child[1], when it contains an
    // async query, is the KNN filter. Keep the vector/extracted-filter domains intact.
    ObDASVecIndexDriverCtDef *vec_driver_ctdef =
        static_cast<ObDASVecIndexDriverCtDef *>(root_ctdef);
    if (vec_driver_ctdef->need_pre_filter_iter()
        && root_ctdef->children_cnt_ > 1
        && OB_NOT_NULL(root_ctdef->children_)
        && OB_NOT_NULL(root_ctdef->children_[1])
        && ObDASAsyncIndexQueryCtx::contains_async_index_query(
               root_ctdef->children_[1])) {
      ObDASBaseCtDef *filter_ctdef = root_ctdef->children_[1];
      ObDASAsyncIndexExecMode filter_exec_mode = ASYNC_INDEX_EXEC_LEGACY;
      if (OB_FAIL(normalize_async_fts_boolean_domain(
              filter_ctdef, filter_exec_mode))) {
        LOG_WARN("failed to normalize independent async knn filter", K(ret));
      } else {
        // Save whether this KNN PRE filter can use GroupMerge. Runtime reads this flag
        // instead of inheriting the Query mode or analyzing the filter tree again.
        // When reading a plan generated by an older observer, the flag defaults to false
        // and the KNN PRE filter uses Legacy mode.
        vec_driver_ctdef->use_async_knn_pre_filter_group_alignment_ =
            ASYNC_INDEX_EXEC_GROUP == filter_exec_mode
            && DAS_OP_BOOLEAN_QUERY == filter_ctdef->op_type_
            && static_cast<const ObDASBooleanQueryCtDef *>(filter_ctdef)->is_async();
        root_ctdef->children_[1] = filter_ctdef;
        LOG_TRACE("processed independent async knn filter domain normalization",
                  K(filter_exec_mode),
                  K(vec_driver_ctdef->use_async_knn_pre_filter_group_alignment_),
                  K(root_ctdef->children_cnt_));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
      ObDASBaseCtDef *child = OB_NOT_NULL(root_ctdef->children_)
          ? root_ctdef->children_[i] : nullptr;
      if (OB_NOT_NULL(child)
          && OB_FAIL(SMART_CALL(normalize_async_knn_filter_domains_(child)))) {
        LOG_WARN("failed to normalize child async knn filter", K(ret), K(i),
                 K(root_ctdef->op_type_));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::normalize_async_fts_boolean_domain_impl(
    ObDASBaseCtDef *&root_ctdef,
    bool &has_domain_rewrite)
{
  int ret = OB_SUCCESS;
  ObSEArray<AsyncFtsDomainRewriteAction, 16> rewrite_actions;
  ObDASBaseCtDef *normalized_ctdef = nullptr;
  bool has_unowned_sync_filter = false;
  has_domain_rewrite = false;
  if (OB_FAIL(plan_async_fts_boolean_domain_(
                 root_ctdef, rewrite_actions, has_unowned_sync_filter,
                 normalized_ctdef))) {
    LOG_WARN("failed to plan async fts boolean domain rewrite", K(ret));
  } else if (has_unowned_sync_filter) {
    // A nested rewrite alone would still select GROUP for the whole runtime.
    // Discard every planned action before committing, including child wrappers,
    // when a global synchronous Filter has no positive async ownership proof.
    LOG_TRACE("discard async group rewrites for unowned synchronous filter");
  } else {
    has_domain_rewrite = normalized_ctdef != root_ctdef || !rewrite_actions.empty();
    // Analysis validates the CtDef domain first. Planning completes every allocation and
    // array append before these pointer assignments, so failure leaves the input tree intact.
    for (int64_t i = 0; i < rewrite_actions.count(); ++i) {
      AsyncFtsDomainRewriteAction &action = rewrite_actions.at(i);
      *action.target_ = action.replacement_;
    }
    root_ctdef = normalized_ctdef;
  }
  return ret;
}

static bool is_async_fts_rewrite_parent(const ObDASOpType op_type)
{
  return DAS_OP_FUSION_QUERY == op_type
      || DAS_OP_TOPK_COLLECT == op_type
      || DAS_OP_TABLE_LOOKUP == op_type
      || DAS_OP_IR_AUX_LOOKUP == op_type
      || DAS_OP_FUNC_LOOKUP == op_type
      || DAS_OP_INDEX_PROJ_LOOKUP == op_type
      || DAS_OP_INDEX_MERGE == op_type
      || DAS_OP_DOC_ID_MERGE == op_type
      || DAS_OP_DOMAIN_ID_MERGE == op_type;
}

static int get_boolean_required_child_role(
    const ObDASBooleanQueryCtDef &boolean_ctdef,
    const ObDASBaseCtDef *child,
    bool &is_filter_child,
    bool &is_required_child)
{
  int ret = OB_SUCCESS;
  enum RequiredClauseIndex
  {
    MUST_CLAUSE_IDX = 0,
    FILTER_CLAUSE_IDX,
    REQUIRED_CLAUSE_COUNT
  };
  ObBooleanSubClause<ObIDASSearchCtDef> required_clauses[REQUIRED_CLAUSE_COUNT];
  is_filter_child = false;
  is_required_child = false;
  if (OB_FAIL(boolean_ctdef.must(required_clauses[MUST_CLAUSE_IDX]))) {
    LOG_WARN("failed to get boolean must clauses", K(ret));
  } else if (OB_FAIL(boolean_ctdef.filter(required_clauses[FILTER_CLAUSE_IDX]))) {
    LOG_WARN("failed to get boolean filter clauses", K(ret));
  }
  for (int64_t clause_idx = 0; OB_SUCC(ret) && !is_required_child
       && clause_idx < ARRAYSIZEOF(required_clauses); ++clause_idx) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_required_child
         && i < required_clauses[clause_idx].count(); ++i) {
      ObIDASSearchCtDef *required_child = nullptr;
      if (OB_FAIL(required_clauses[clause_idx].get(i, required_child))) {
        LOG_WARN("failed to get boolean required child", K(ret), K(clause_idx), K(i));
      } else if (required_child == child) {
        is_required_child = true;
        is_filter_child = FILTER_CLAUSE_IDX == clause_idx;
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::plan_async_fts_boolean_domain_(
    ObDASBaseCtDef *root_ctdef,
    ObIArray<AsyncFtsDomainRewriteAction> &rewrite_actions,
    bool &has_unowned_sync_filter,
    ObDASBaseCtDef *&normalized_ctdef)
{
  int ret = OB_SUCCESS;
  normalized_ctdef = root_ctdef;
  if (OB_ISNULL(root_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null hybrid search ctdef", K(ret));
  } else if (DAS_OP_BOOLEAN_QUERY == root_ctdef->op_type_
             && static_cast<ObDASBooleanQueryCtDef *>(root_ctdef)->is_async()) {
    // An analyzed async wrapper is an atomic leaf until its Boolean parent coalesces it.
  } else if (DAS_OP_BOOLEAN_QUERY != root_ctdef->op_type_) {
    if (is_async_fts_rewrite_parent(root_ctdef->op_type_)) {
      // Keep the same structural traversal boundary as domain analysis. IR, scalar-query,
      // vector, nested, and unsupported nodes remain atomic for this rewrite.
      for (int64_t i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
        ObDASBaseCtDef *normalized_child = nullptr;
        if (OB_ISNULL(root_ctdef->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async fts parent child", K(ret), K(i), K(root_ctdef->op_type_));
        } else if (OB_FAIL(SMART_CALL(plan_async_fts_boolean_domain_(
                       root_ctdef->children_[i], rewrite_actions,
                       has_unowned_sync_filter, normalized_child)))) {
          LOG_WARN("failed to plan async fts parent child", K(ret), K(i),
                   K(root_ctdef->op_type_));
        } else if (normalized_child != root_ctdef->children_[i]
                   && OB_FAIL(rewrite_actions.push_back(
                          AsyncFtsDomainRewriteAction(
                              &root_ctdef->children_[i], normalized_child)))) {
          LOG_WARN("failed to append async fts parent rewrite", K(ret), K(i),
                   K(root_ctdef->op_type_));
        }
      }
    }
  } else {
    int64_t async_child_cnt = 0;
    ObSEArray<ObDASBaseCtDef *, 8> normalized_children;
    for (int64_t i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
      ObDASBaseCtDef *normalized_child = nullptr;
      if (OB_ISNULL(root_ctdef->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null hybrid search child ctdef", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(plan_async_fts_boolean_domain_(
                     root_ctdef->children_[i], rewrite_actions,
                     has_unowned_sync_filter, normalized_child)))) {
        LOG_WARN("failed to plan async fts child domain", K(ret), K(i));
      } else if (OB_FAIL(normalized_children.push_back(normalized_child))) {
        LOG_WARN("failed to append planned async fts child", K(ret), K(i));
      } else if (DAS_OP_BOOLEAN_QUERY == normalized_child->op_type_
                 && static_cast<ObDASBooleanQueryCtDef *>(
                     normalized_child)->is_async()) {
        ++async_child_cnt;
      }
    }
    bool mixed_children_are_sync_filters = true;
    bool has_required_async_child = false;
    const ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<const ObDASBooleanQueryCtDef *>(root_ctdef);
    for (int64_t i = 0; OB_SUCC(ret) && mixed_children_are_sync_filters
         && i < normalized_children.count(); ++i) {
      ObDASBaseCtDef *normalized_child = normalized_children.at(i);
      const bool is_async_wrapper = DAS_OP_BOOLEAN_QUERY == normalized_child->op_type_
          && static_cast<ObDASBooleanQueryCtDef *>(normalized_child)->is_async();
      bool is_filter_child = false;
      bool is_required_child = false;
      if (OB_FAIL(get_boolean_required_child_role(
              *boolean_ctdef, root_ctdef->children_[i],
              is_filter_child, is_required_child))) {
        LOG_WARN("failed to classify mixed boolean child", K(ret), K(i));
      } else if (is_async_wrapper) {
        has_required_async_child |= is_required_child;
      } else {
        bool is_group_compatible_filter = false;
        if (is_filter_child
            && OB_FAIL(ObDASAsyncIndexQueryCtx::is_group_compatible_filter_domain(
                   normalized_child, is_group_compatible_filter))) {
          LOG_WARN("failed to analyze mixed Boolean filter child", K(ret), K(i));
        } else {
          mixed_children_are_sync_filters = is_filter_child
              && is_group_compatible_filter;
        }
      }
    }
    if (OB_SUCCESS != ret || 0 == async_child_cnt) {
      // Preserve the original tree when no async full-text domain exists.
    } else if (async_child_cnt != root_ctdef->children_cnt_
               && (!mixed_children_are_sync_filters || !has_required_async_child)) {
      // Eligibility is checked before mutation. Keep this guard non-failing so an
      // unrecognized mixed shape always falls back to the original execution tree.
      // A global synchronous Filter needs a required async MUST/FILTER to restrict
      // its output to the current Group. MUST_NOT and optional SHOULD do not provide
      // that ownership. Conservatively retain the original tree for SHOULD-only
      // async domains: its effective minimum_should_match may be a runtime expression.
      LOG_TRACE("keep mixed async index boolean tree unchanged",
                K(root_ctdef->op_type_), K(async_child_cnt), K(root_ctdef->children_cnt_),
                K(has_required_async_child));
      if (mixed_children_are_sync_filters && !has_required_async_child) {
        has_unowned_sync_filter = true;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < normalized_children.count(); ++i) {
        if (normalized_children.at(i) != root_ctdef->children_[i]
            && OB_FAIL(rewrite_actions.push_back(
                   AsyncFtsDomainRewriteAction(
                       &root_ctdef->children_[i], normalized_children.at(i))))) {
          LOG_WARN("failed to append mixed async fts rewrite", K(ret), K(i));
        }
      }
    } else {
      ObDASScalarScanCtDef *shared_delete_ctdef = nullptr;
      ObSEArray<ObDASBaseCtDef *, 8> query_ctdefs;
      for (int64_t i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
        ObDASBaseCtDef *query_ctdef = nullptr;
        ObDASScalarScanCtDef *delete_scan_ctdef = nullptr;
        ObDASBaseCtDef *normalized_child = normalized_children.at(i);
        const bool is_async_wrapper = DAS_OP_BOOLEAN_QUERY == normalized_child->op_type_
            && static_cast<ObDASBooleanQueryCtDef *>(normalized_child)->is_async();
        if (is_async_wrapper) {
          if (OB_FAIL(get_async_fts_wrapper_children_(
                  normalized_child, query_ctdef, delete_scan_ctdef))) {
            LOG_WARN("failed to extract async fts child wrapper", K(ret), K(i));
          } else if (OB_ISNULL(shared_delete_ctdef)) {
            shared_delete_ctdef = delete_scan_ctdef;
          }
        } else {
          query_ctdef = normalized_child;
        }
        if (OB_SUCC(ret) && OB_FAIL(query_ctdefs.push_back(query_ctdef))) {
          LOG_WARN("failed to append unwrapped async fts query", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(shared_delete_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normalized async domain has no shared delete scan", K(ret));
      } else if (OB_SUCC(ret)) {
        ObDASBaseCtDef *wrapper_ctdef = nullptr;
        if (OB_FAIL(build_async_fts_domain_wrapper(
                root_ctdef, shared_delete_ctdef, wrapper_ctdef))) {
          LOG_WARN("failed to preallocate normalized async fts wrapper", K(ret));
        } else {
          normalized_ctdef = wrapper_ctdef;
          for (int64_t i = 0; OB_SUCC(ret) && i < query_ctdefs.count(); ++i) {
            if (query_ctdefs.at(i) != root_ctdef->children_[i]
                && OB_FAIL(rewrite_actions.push_back(
                       AsyncFtsDomainRewriteAction(
                           &root_ctdef->children_[i], query_ctdefs.at(i))))) {
              LOG_WARN("failed to append async fts unwrap rewrite", K(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_async_delete_scan_ctdef(
    ObLogTableScan &op,
    uint64_t delete_idx_tid,
    ObColumnRefRawExpr *delete_doc_id_column,
    ObRawExpr *delete_rowscn_expr,
    ObDASScalarScanCtDef *&delete_scan_ctdef)
{
  int ret = OB_SUCCESS;
  delete_scan_ctdef = nullptr;
  ObSEArray<ObRawExpr *, 2> access_exprs;
  ObSEArray<uint64_t, 2> output_cids;
  ObSEArray<ObRawExpr *, 1> rowkey_exprs;
  if (OB_UNLIKELY(OB_INVALID_ID == delete_idx_tid)
      || OB_ISNULL(delete_doc_id_column)
      || OB_ISNULL(delete_rowscn_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async delete scan metadata", KR(ret), K(delete_idx_tid),
        KP(delete_doc_id_column), KP(delete_rowscn_expr));
  } else if (OB_FAIL(access_exprs.push_back(delete_doc_id_column))) {
    LOG_WARN("failed to push back delete doc id column expr", KR(ret));
  } else if (OB_FAIL(access_exprs.push_back(delete_rowscn_expr))) {
    LOG_WARN("failed to push back delete rowscn expr", KR(ret));
  } else if (OB_FAIL(rowkey_exprs.push_back(delete_doc_id_column))) {
    LOG_WARN("failed to push back delete rowkey expr", KR(ret));
  } else if (OB_FAIL(output_cids.push_back(delete_doc_id_column->get_column_id()))) {
    LOG_WARN("failed to push back delete doc id column id", KR(ret));
  } else if (OB_FAIL(output_cids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
    LOG_WARN("failed to push back delete rowscn column id", KR(ret));
  } else {
    // Keep the rowkey list limited to __pk_increment(doc_id); ORA_ROWSCN is an
    // access-only column used to filter the bitmap by the async snapshot.
    DASScalarCGParams scalar_cg_params(
        op.get_ref_table_id(),
        delete_idx_tid,
        false, /* is new query range */
        true,  /* is rowkey order scan */
        false, /* is primary table scan */
        op.get_table_type(),
        &access_exprs,
        &output_cids,
        &rowkey_exprs,
        nullptr, /* pre range graph */
        nullptr, /* pre query range */
        false, /* has pre query range */
        false, /* is get */
        true,  /* need rowkey order */
        false, /* is use column store */
        nullptr, /* domain id expr */
        nullptr, /* domain id col ids */
        nullptr, /* pushdown filters */
        nullptr, /* pushdown aggr exprs */
        nullptr  /* filter monotonicity */
    );
    if (OB_FAIL(generate_scalar_scan_ctdef(scalar_cg_params, op, delete_scan_ctdef))) {
      LOG_WARN("failed to generate async delete scan ctdef", KR(ret),
          K(delete_idx_tid));
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObScalarQueryNode* scalar_query_node, ObDASScalarCtDef *&scalar_ctdef)
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  bool is_new_query_range = false;
  const ObQueryRange *pre_query_range = nullptr;
  const ObPreRangeGraph *pre_range_graph = nullptr;
  ObDASScalarCtDef *new_scalar_ctdef = nullptr;
  ObDASScalarScanCtDef * scalar_index_scan_ctdef = nullptr;
  ObDASScalarScanCtDef * scalar_main_scan_ctdef = nullptr;
  bool is_search_index_path = false;

  if (OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null", K(ret));
  } else if (OB_ISNULL(scalar_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar query node or access path is null", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_QUERY, cg_.phy_plan_->get_allocator(), new_scalar_ctdef))) {
    LOG_WARN("failed to allocate scalar ctdef", K(ret));
  } else if (OB_ISNULL(new_scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_FALSE_IT(new_scalar_ctdef->set_primary_get_ratio(
      scalar_query_node->is_scoring_ ? 0 : scalar_query_node->primary_get_ratio_))) {
  } else if (scalar_query_node->is_merged_probe_) {
    // Merged primary-probe node: a primary-table ROR point-get built from the AND filter of
    // all must/filter clauses. No index scan (the probe IS the primary scan); the primary access
    // path supplies a rowkey-shaped range template that runtime rewrites for each driver candidate.
    new_scalar_ctdef->set_has_index_scan(false);
    new_scalar_ctdef->set_has_main_scan(true);
  } else if (nullptr != scalar_query_node->ap_) {
    // has index scan access path
    is_search_index_path = scalar_query_node->ap_->is_search_index_path();
    if (scalar_query_node->ap_->is_ordered_by_pk_ || !scalar_query_node->need_rowkey_order_) {
      // index scan is ordered by pk or no need to preserve rowkey order
      // just scan index table
      new_scalar_ctdef->set_has_index_scan(true);
      // For pk-ordered equality scans that need pk order, also generate the primary scan
      // ctdef when primary_get_ratio_ favors it, so runtime can choose a primary probe.
      const bool enable_primary_probe = scalar_query_node->ap_->is_ordered_by_pk_
                                        && scalar_query_node->need_rowkey_order_
                                        && scalar_query_node->primary_get_ratio_ > 0
                                        && nullptr != scalar_query_node->pri_table_ap_;
      new_scalar_ctdef->set_has_main_scan(enable_primary_probe);
    } else if (!scalar_query_node->ap_->is_ordered_by_pk_ && scalar_query_node->need_rowkey_order_) {
      // index scan is not ordered by pk and need to preserve rowkey order
      // scan index table or primary table, the decision will be made at execution time
      new_scalar_ctdef->set_has_index_scan(true);
      new_scalar_ctdef->set_has_main_scan(nullptr != scalar_query_node->pri_table_ap_);
    } else {
      // never happen, defensive programming
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected case", K(ret));
    }
  } else if (nullptr != scalar_query_node->pri_table_ap_) {
    // has primary table scan access path, but no index scan access path
    // must scan primary table
    new_scalar_ctdef->set_has_index_scan(false);
    new_scalar_ctdef->set_has_main_scan(true);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }

  // index scan
  if (OB_SUCC(ret) && new_scalar_ctdef->has_index_scan()) {
    const AccessPath *ap = scalar_query_node->ap_;
    if (OB_ISNULL(ap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(ap->get_query_range_provider())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query range provider", K(ret), KPC(ap));
    } else if (OB_FAIL(ap->get_query_range_provider()->is_get(is_get))) {
      LOG_WARN("failed to get query range provider is get", K(ret));
    } else {
      is_new_query_range = nullptr != ap->pre_range_graph_;
      pre_range_graph = ap->pre_range_graph_;
      pre_query_range = ap->pre_query_range_;
      // Logical table SI: rowkey_exprs_ must match Fusion rowid (op rowid), not SI doc_id.
      const ObIArray<ObRawExpr *> &scalar_rowkey_raw_exprs =
          (op.is_logical_table() && is_search_index_path)
          ? op.get_rowkey_exprs()
          : scalar_query_node->index_table_rowkey_exprs_;
      ObHybridSearchCgService::DASScalarCGParams cg_params(op.get_ref_table_id(),
                                                           ap->get_real_index_table_id(),
                                                           is_new_query_range,
                                                           ap->is_ordered_by_pk_,
                                                           false,
                                                           op.get_table_type(),
                                                           &scalar_query_node->index_table_query_params_.access_exprs_,
                                                           &scalar_query_node->index_table_query_params_.output_col_ids_,
                                                           &scalar_rowkey_raw_exprs,
                                                           pre_range_graph,
                                                           pre_query_range,
                                                           true, /* has_pre_query_range */
                                                           is_get,
                                                           scalar_query_node->need_rowkey_order_,
                                                           op.use_column_store(),
                                                           nullptr,
                                                           nullptr,
                                                           &scalar_query_node->index_table_query_params_.pushdown_filters_,
                                                           nullptr,
                                                           &scalar_query_node->index_table_query_params_.filter_monotonicity_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, op, scalar_index_scan_ctdef))) {
        LOG_WARN("failed to generate index scan ctdef", K(ret));
      } else if (OB_ISNULL(scalar_index_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index scan ctdef is null", K(ret));
      } else {
        scalar_index_scan_ctdef->is_primary_table_scan_ = false;
        scalar_index_scan_ctdef->is_search_index_ = is_search_index_path;
        scalar_index_scan_ctdef->is_index_term_point_scan_ = ap->is_index_term_point_scan_;
        scalar_index_scan_ctdef->set_is_scoring(scalar_query_node->is_scoring_);
        scalar_index_scan_ctdef->set_is_top_level_scoring(scalar_query_node->is_top_level_scoring_);
      }
    }
  }

  // primary table scan
  if (OB_SUCC(ret) && new_scalar_ctdef->has_main_scan()) {
    const AccessPath *ap = scalar_query_node->pri_table_ap_;
    if (OB_ISNULL(ap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (scalar_query_node->is_merged_probe_) {
      // Retain the primary access-path range as the mutable rowkey-shaped probe template.
      is_new_query_range = nullptr != ap->pre_range_graph_;
      pre_range_graph = ap->pre_range_graph_;
      pre_query_range = ap->pre_query_range_;
      ObHybridSearchCgService::DASScalarCGParams cg_params(op.get_ref_table_id(),
                                                           ap->get_index_table_id(),
                                                           is_new_query_range,
                                                           true,  /* is_rowkey_order_scan */
                                                           true,  /* is_primary_table_scan */
                                                           op.get_table_type(),
                                                           &scalar_query_node->pri_table_query_params_.access_exprs_,
                                                           &scalar_query_node->pri_table_query_params_.output_col_ids_,
                                                           &op.get_rowkey_exprs(),
                                                           pre_range_graph,
                                                           pre_query_range,
                                                           true,  /* has_pre_query_range */
                                                           true,  /* is_get */
                                                           true,  /* need_rowkey_order */
                                                           op.use_column_store(),
                                                           nullptr,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.pushdown_filters_,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.filter_monotonicity_);
      if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, op, scalar_main_scan_ctdef))) {
        LOG_WARN("failed to generate merged probe main scan ctdef", K(ret));
      } else if (OB_ISNULL(scalar_main_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("main scan ctdef is null", K(ret));
      } else {
        scalar_main_scan_ctdef->is_primary_table_scan_ = true;
        scalar_main_scan_ctdef->set_is_scoring(scalar_query_node->is_scoring_);
        scalar_main_scan_ctdef->set_is_top_level_scoring(scalar_query_node->is_top_level_scoring_);
      }
    } else if (OB_ISNULL(ap->get_query_range_provider())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query range provider", K(ret), KPC(ap));
    } else if (OB_FAIL(ap->get_query_range_provider()->is_get(is_get))) {
      LOG_WARN("failed to get query range provider is get", K(ret));
    } else if (OB_UNLIKELY(!ap->is_ordered_by_pk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary table is not ordered by pk", K(ret), KPC(ap));
    } else {
      is_new_query_range = nullptr != ap->pre_range_graph_;
      pre_range_graph = ap->pre_range_graph_;
      pre_query_range = ap->pre_query_range_;
      ObHybridSearchCgService::DASScalarCGParams cg_params(op.get_ref_table_id(),
                                                           ap->get_index_table_id(),
                                                           is_new_query_range,
                                                           scalar_query_node->is_extracted_ ? false : ap->is_ordered_by_pk_,
                                                           true,
                                                           op.get_table_type(),
                                                           &scalar_query_node->pri_table_query_params_.access_exprs_,
                                                           &scalar_query_node->pri_table_query_params_.output_col_ids_,
                                                           &op.get_rowkey_exprs(),
                                                           pre_range_graph,
                                                           pre_query_range,
                                                           true, /* has_pre_query_range */
                                                           scalar_query_node->is_extracted_ ? true : is_get,
                                                           scalar_query_node->need_rowkey_order_,
                                                           op.use_column_store(),
                                                           nullptr,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.pushdown_filters_,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.filter_monotonicity_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, op, scalar_main_scan_ctdef))) {
        LOG_WARN("failed to generate main scan ctdef", K(ret));
      } else if (OB_ISNULL(scalar_main_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("main scan ctdef is null", K(ret));
      } else {
        scalar_main_scan_ctdef->set_is_scoring(scalar_query_node->is_scoring_);
        scalar_main_scan_ctdef->set_is_top_level_scoring(scalar_query_node->is_top_level_scoring_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (new_scalar_ctdef->has_index_scan() && new_scalar_ctdef->has_main_scan()) {
    if (OB_ISNULL(new_scalar_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate children failed", K(ret));
    } else if (OB_ISNULL(scalar_index_scan_ctdef) || OB_ISNULL(scalar_main_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan ctdef or main scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 2;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_index_scan_ctdef);
      new_scalar_ctdef->children_[1] = static_cast<ObDASBaseCtDef*>(scalar_main_scan_ctdef);
    }
  } else if (OB_ISNULL(new_scalar_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate children failed", K(ret));
  } else if (new_scalar_ctdef->has_index_scan()) {
    if (OB_ISNULL(scalar_index_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 1;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_index_scan_ctdef);
    }
  } else if (new_scalar_ctdef->has_main_scan()) {
    if (OB_ISNULL(scalar_main_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("main scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 1;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_main_scan_ctdef);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected case", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(scalar_query_node->boost_) &&
             OB_FAIL(cg_.generate_rt_expr(*scalar_query_node->boost_, new_scalar_ctdef->boost_))) {
    LOG_WARN("failed to generate rt expr for scalar boost", K(ret));
  } else {
    // Logical table v0: keep scoring flags consistent with CG-side BM25 degrade
    // (see ob_hybrid_search_cg_service_fulltext.cpp synchronization point).
    const bool degrade_for_logical = op.is_logical_table();
    new_scalar_ctdef->set_is_top_level_scoring(
        scalar_query_node->is_top_level_scoring_ && !degrade_for_logical);
    new_scalar_ctdef->set_is_scoring(
        scalar_query_node->is_scoring_);
    scalar_ctdef = new_scalar_ctdef;
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObBooleanQueryNode *boolean_query_node, ObDASBooleanQueryCtDef *&boolean_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASBooleanQueryCtDef *new_boolean_query_ctdef = nullptr;
  bool can_generate_must_not_only = false;
  bool degrade_to_full_scan = false;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(boolean_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(boolean_query_node));
  } else if (FALSE_IT(can_generate_must_not_only = is_generating_nested_query_ || must_not_contains_nested_query_(boolean_query_node))) {
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_BOOLEAN_QUERY, cg_.phy_plan_->get_allocator(), new_boolean_query_ctdef))) {
    LOG_WARN("failed to allocate boolean query ctdef", KR(ret));
  } else if (OB_ISNULL(new_boolean_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("boolean query ctdef is null", KR(ret));
  } else {
    // Basic parameter validation first (before accessing op — tests may pass
    // a dummy ObLogTableScan whose contents are uninitialized).
    int64_t min_should_match = static_cast<int64_t>(boolean_query_node->min_should_match_);
    if (min_should_match < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("min should match must be positive", KR(ret), K(min_should_match));
    } else if (boolean_query_node->should_nodes_.count() +
               boolean_query_node->must_nodes_.count() +
               boolean_query_node->filter_nodes_.count() == 0 &&
               (boolean_query_node->must_not_nodes_.count() == 0 || !can_generate_must_not_only)) {
      // must_not-only bool is allowed when either
      //   (a) we are inside a nested_query subtree (child-scope: universe is
      //       synthesized by synthesize_child_domain_must_not_required), or
      //   (b) must_not directly wraps a nested_query at parent scope
      //       (parent-scope negation: the nested_query itself produces the
      //       parent-doc universe via the nested→parent join).
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there should be at least one positive or must_not clause", KR(ret));
    } else if (op.is_logical_table()) {
      if (OB_FAIL(setup_lt_boolean_query_ctdef_layout_(
              op, boolean_query_node, new_boolean_query_ctdef,
              min_should_match, can_generate_must_not_only, degrade_to_full_scan))) {
        LOG_WARN("failed to setup LT boolean query ctdef layout", KR(ret));
      }
    } else {
      BoolClauseEffCounts eff;
      eff.must_ = boolean_query_node->must_nodes_.count();
      eff.should_ = boolean_query_node->should_nodes_.count();
      eff.filter_ = boolean_query_node->filter_nodes_.count();
      eff.must_not_ = boolean_query_node->must_not_nodes_.count();
      populate_boolean_query_ctdef_layout_(new_boolean_query_ctdef, min_should_match, eff,
                                          OB_NOT_NULL(boolean_query_node->merged_probe_scalar_node_));
    }
  }
  if (OB_FAIL(ret)) {
    // error already logged
  } else if (degrade_to_full_scan) {
    // [LT hybrid CG] No error; return nullptr so generate_hybrid_search_node_ctdef
    // propagates the degrade signal upward. new_boolean_query_ctdef is owned by
    // phy_plan allocator and needs no manual free.
    boolean_query_ctdef = nullptr;
  } else if (OB_NOT_NULL(new_boolean_query_ctdef)) {
    // Logical table v0: keep scoring flags consistent with CG-side BM25 degrade.
    // The op-pickers in ob_das_boolean_query.cpp (req()/opt()) read these flags to
    // decide whether to construct BMM/BMW. Without this sync, the outer boolean
    // ctdef would still report is_top_level_scoring=true even when its child
    // fulltext ctdefs have been degraded, causing BMM to wrap a non-scoring token
    // op and crash inside ObTextRetrievalTokenIter::estimate_token_doc_cnt.
    const bool degrade_for_logical = op.is_logical_table();
    new_boolean_query_ctdef->set_is_top_level_scoring(
        boolean_query_node->is_top_level_scoring_ && !degrade_for_logical);
    new_boolean_query_ctdef->set_is_scoring(
        boolean_query_node->is_scoring_);
    new_boolean_query_ctdef->set_primary_get_ratio(
        boolean_query_node->is_scoring_ ? 0 : boolean_query_node->primary_get_ratio_);
    if (OB_NOT_NULL(boolean_query_node->minimum_should_match_)) {
      ObExpr *msm_rt_expr = nullptr;
      if (OB_FAIL(cg_.generate_rt_expr(*boolean_query_node->minimum_should_match_, msm_rt_expr))) {
        LOG_WARN("failed to generate rt expr for bool minimum_should_match", KR(ret));
      } else {
        new_boolean_query_ctdef->set_min_should_match_expr(msm_rt_expr);
        new_boolean_query_ctdef->set_is_msm_unresolved_expr(ob_is_string_type(msm_rt_expr->obj_meta_.get_type()));
      }
    } else {
      new_boolean_query_ctdef->set_min_should_match_expr(nullptr);
      new_boolean_query_ctdef->set_is_msm_unresolved_expr(false);
    }
    if (OB_SUCC(ret)) {
      boolean_query_ctdef = new_boolean_query_ctdef;
    }
  }
  return ret;
}

int ObHybridSearchCgService::mark_subtree_nested_scope(ObDASBaseCtDef *root_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ctdef)) {
    // nothing to mark
  } else {
    if (root_ctdef->op_type_ == DAS_OP_SCALAR_SCAN_QUERY) {
      static_cast<ObDASScalarScanCtDef *>(root_ctdef)->is_nested_scope_ = 1;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(mark_subtree_nested_scope(root_ctdef->children_[i])))) {
        LOG_WARN("failed to mark child ctdef as nested scope", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::collect_async_delete_info(
    const ObHybridSearchNodeBase *node,
    const ObAsyncIndexDeleteInfo *&async_delete_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else {
    const ObAsyncIndexDeleteInfo *node_delete_info = node->get_async_delete_info();
    if (OB_NOT_NULL(node_delete_info) && OB_UNLIKELY(!node_delete_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async delete info in nested query",
          K(ret), K(node->node_type_), KPC(node_delete_info));
    } else if (OB_NOT_NULL(node_delete_info)) {
      if (OB_ISNULL(async_delete_info)) {
        async_delete_info = node_delete_info;
      } else if (OB_UNLIKELY(
          async_delete_info->get_delete_idx_tid() != node_delete_info->get_delete_idx_tid()
          || OB_ISNULL(async_delete_info->get_delete_doc_id_column())
          || async_delete_info->get_delete_doc_id_column()->get_column_id()
             != node_delete_info->get_delete_doc_id_column()->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inconsistent shared async delete metadata", K(ret),
            KPC(async_delete_info), KPC(node_delete_info));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      const ObHybridSearchNodeBase *child =
          static_cast<const ObHybridSearchNodeBase *>(node->children_.at(i));
      if (OB_FAIL(SMART_CALL(collect_async_delete_info(child, async_delete_info)))) {
        LOG_WARN("failed to collect child async delete info", K(ret), K(i));
      }
    }
  }
  return ret;
}

ObDASScalarScanCtDef *ObHybridSearchCgService::find_first_scalar_scan_ctdef(ObDASBaseCtDef *ctdef)
{
  ObDASScalarScanCtDef *result = nullptr;
  if (OB_ISNULL(ctdef)) {
  } else if (ctdef->op_type_ == DAS_OP_SCALAR_SCAN_QUERY) {
    result = static_cast<ObDASScalarScanCtDef *>(ctdef);
  } else {
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i])) {
        ObDASScalarScanCtDef *found = find_first_scalar_scan_ctdef(ctdef->children_[i]);
        if (OB_NOT_NULL(found)) {
          result = found;
          break;
        }
      }
    }
  }
  return result;
}

bool ObHybridSearchCgService::subtree_contains_must_not_only_bool(ObDASBaseCtDef *ctdef)
{
  bool found = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    // nothing to do
  } else if (ctdef->op_type_ == DAS_OP_BOOLEAN_QUERY) {
    ObDASBooleanQueryCtDef *bool_ctdef = static_cast<ObDASBooleanQueryCtDef *>(ctdef);
    ObBooleanSubClause<ObIDASSearchCtDef> must_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> filter_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> should_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> must_not_clause;
    if (OB_SUCC(bool_ctdef->must(must_clause)) &&
        OB_SUCC(bool_ctdef->filter(filter_clause)) &&
        OB_SUCC(bool_ctdef->should(should_clause)) &&
        OB_SUCC(bool_ctdef->must_not(must_not_clause))) {
      if (!must_not_clause.empty() && must_clause.empty() &&
          filter_clause.empty() && should_clause.empty()) {
        found = true;
        return found;
      }
    }
    // Not a must_not-only at this node; recurse into clause children.
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i]) &&
          subtree_contains_must_not_only_bool(ctdef->children_[i])) {
        found = true;
        break;
      }
    }
  } else {
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i]) &&
          subtree_contains_must_not_only_bool(ctdef->children_[i])) {
        found = true;
        break;
      }
    }
  }
  return found;
}

int ObHybridSearchCgService::synthesize_child_domain_must_not_required(ObLogTableScan &op, ObDASBaseCtDef *root_ctdef)
{
  int ret = OB_SUCCESS;
  // root_ctdef is the nested query ctdef; its children_[0] is the inner subtree.
  if (OB_ISNULL(root_ctdef) || root_ctdef->op_type_ != DAS_OP_NESTED_QUERY) {
    // not a nested query ctdef, nothing to do
  } else if (root_ctdef->children_cnt_ < 1 || OB_ISNULL(root_ctdef->children_[0])) {
    // no inner child, skip
  } else {
    ObDASBaseCtDef *inner = root_ctdef->children_[0];
    if (inner->op_type_ != DAS_OP_BOOLEAN_QUERY) {
      // inner is not a boolean query, nothing to do
    } else {
      ObDASBooleanQueryCtDef *bool_ctdef = static_cast<ObDASBooleanQueryCtDef *>(inner);
      // Check: must_not exists AND must/filter/should do NOT exist.
      // Use the public clause accessors; an empty clause means the sub-clause does not exist.
      ObBooleanSubClause<ObIDASSearchCtDef> must_not_clause;
      ObBooleanSubClause<ObIDASSearchCtDef> must_clause;
      ObBooleanSubClause<ObIDASSearchCtDef> filter_clause;
      ObBooleanSubClause<ObIDASSearchCtDef> should_clause;
      if (OB_FAIL(bool_ctdef->must_not(must_not_clause))) {
        LOG_WARN("failed to get must_not clause", K(ret));
      } else if (OB_FAIL(bool_ctdef->must(must_clause))) {
        LOG_WARN("failed to get must clause", K(ret));
      } else if (OB_FAIL(bool_ctdef->filter(filter_clause))) {
        LOG_WARN("failed to get filter clause", K(ret));
      } else if (OB_FAIL(bool_ctdef->should(should_clause))) {
        LOG_WARN("failed to get should clause", K(ret));
      } else if (must_not_clause.empty() || !must_clause.empty() ||
                 !filter_clause.empty() || !should_clause.empty()) {
        // Not a direct must_not-only bool — nothing to synthesize at this level.
        //
        // Guard: detect nested bool(only must_not) in any positive (must/filter)
        // clause subtree. This shape is not supported because child-domain
        // synthesis only handles a direct bool(only must_not) child of the nested
        // cctdef. Emit a clear user error here rather than letting it cascade into
        // a confusing runtime "Internal error" (-4016) from
        // ObDASBooleanQueryRtDef::compute_cost() / compute_required_cost().
        const int64_t pos_clause_kind_cnt = 2;
        const ObBooleanSubClause<ObIDASSearchCtDef> *pos_clauses[pos_clause_kind_cnt] = {
          &must_clause, &filter_clause
        };
        for (int64_t i = 0; OB_SUCC(ret) && i < pos_clause_kind_cnt; ++i) {
          const ObBooleanSubClause<ObIDASSearchCtDef> &cls = *pos_clauses[i];
          if (cls.empty()) { continue; }
          for (int64_t j = 0; OB_SUCC(ret) && j < cls.count(); ++j) {
            ObIDASSearchCtDef *child = nullptr;
            if (OB_FAIL(cls.get(j, child))) {
              LOG_WARN("failed to get positive clause child", KR(ret), K(i), K(j));
            } else if (OB_NOT_NULL(child) &&
                       subtree_contains_must_not_only_bool(child)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                "nested bool query: bool(only must_not) is not supported "
                "inside must/filter clauses under a nested path; "
                "please add an explicit positive clause (must/should/filter)");
              LOG_WARN("[nested-domain CG] reject must_not-only bool inside positive clause "
                      "under nested path",
                      KR(ret), K(i), K(j));
            }
          }
        }
      } else if (OB_ISNULL(cg_.phy_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan is null", K(ret));
      } else {
        common::ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
        // In a must_not-only bool, must_not offset is 0 and all children belong to must_not.
        const int64_t must_not_offset = 0;
        const int64_t must_not_count = must_not_clause.count();

        // 1. Find first scalar scan ctdef in the must_not subtree
        ObDASScalarScanCtDef *template_scan = nullptr;
        for (int64_t i = must_not_offset;
             OB_SUCC(ret) && OB_ISNULL(template_scan) && i < must_not_offset + must_not_count;
             ++i) {
          if (i < inner->children_cnt_ && OB_NOT_NULL(inner->children_[i])) {
            template_scan = find_first_scalar_scan_ctdef(inner->children_[i]);
          }
        }
        if (OB_SUCC(ret) && OB_ISNULL(template_scan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no scalar scan ctdef found in must_not subtree", K(ret));
        }

        // 2. Deep-clone the template scan ctdef
        ObDASScalarScanCtDef *synth_scan = nullptr;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_SCAN_QUERY, allocator, synth_scan))) {
          LOG_WARN("failed to allocate synthesized scan ctdef", K(ret));
        } else if (OB_ISNULL(synth_scan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("synth scan ctdef is null", K(ret));
        } else {
          // Copy key fields from the template.
          // table_param_ has DISALLOW_COPY_AND_ASSIGN; pd_expr_spec_ has no assign().
          // Copy the individual sub-fields that init_scan_param actually uses.
          synth_scan->ref_table_id_ = template_scan->ref_table_id_;
          synth_scan->schema_version_ = template_scan->schema_version_;
          if (OB_FAIL(synth_scan->access_column_ids_.assign(template_scan->access_column_ids_))) {
            LOG_WARN("failed to assign access_column_ids", K(ret));
          } else if (OB_FAIL(synth_scan->pd_expr_spec_.access_exprs_.assign(
                         template_scan->pd_expr_spec_.access_exprs_))) {
            LOG_WARN("failed to assign pd_expr_spec access_exprs", K(ret));
          } else if (OB_FAIL(synth_scan->pd_expr_spec_.calc_exprs_.assign(
                         template_scan->pd_expr_spec_.calc_exprs_))) {
            LOG_WARN("failed to assign pd_expr_spec calc_exprs", K(ret));
          } else if (OB_FAIL(synth_scan->result_output_.assign(template_scan->result_output_))) {
            LOG_WARN("failed to assign result_output", K(ret));
          } else if (OB_FAIL(synth_scan->rowkey_exprs_.assign(template_scan->rowkey_exprs_))) {
            LOG_WARN("failed to assign rowkey_exprs", K(ret));
          } else {
            synth_scan->pd_expr_spec_.max_batch_size_ = template_scan->pd_expr_spec_.max_batch_size_;
            synth_scan->pd_expr_spec_.pd_storage_flag_ = template_scan->pd_expr_spec_.pd_storage_flag_;
            // Initialize table_param_ via convert() so it survives DAS serialization.
            // ObTableParam has DISALLOW_COPY_AND_ASSIGN, so call convert() with the same
            // schema to properly populate main_read_info_, output_projector_, etc.
            ObSqlSchemaGuard *schema_guard = nullptr;
            ObBasicSessionInfo *session_info = nullptr;
            const ObTableSchema *table_schema = nullptr;
            uint64_t ref_table_id = template_scan->ref_table_id_;
            int64_t route_policy = 0;
            if (OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr schema guard", K(ret));
            } else if (OB_ISNULL(session_info = cg_.opt_ctx_->get_session_info())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr session info", K(ret));
            } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                               ref_table_id,
                                                               op.get_stmt(),
                                                               table_schema))) {
              LOG_WARN("failed to get table schema", K(ret), K(op.get_table_id()), K(ref_table_id));
            } else if (OB_ISNULL(table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr table schema", K(ret), K(ref_table_id));
            } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
              LOG_WARN("failed to get route policy", K(ret));
            } else {
              // Mirror the template scan's table_param_ runtime flags so the
              // synthesized scan reads the same search index with identical
              // semantics. These are serialized members consumed by the storage
              // layer; convert() does NOT derive them, so set them before convert().
              synth_scan->table_param_.get_enable_lob_locator_v2()
                  = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
              synth_scan->table_param_.set_is_fts_index(template_scan->table_param_.is_fts_index());
              synth_scan->table_param_.set_is_multivalue_index(template_scan->table_param_.is_multivalue_index());
              synth_scan->table_param_.set_is_vec_index(template_scan->table_param_.is_vec_index());
              synth_scan->table_param_.set_is_mlog_table(template_scan->table_param_.is_mlog_table());
              synth_scan->table_param_.set_is_partition_table(template_scan->table_param_.is_partition_table());
              synth_scan->table_param_.set_is_enable_semistruct_encoding(
                  template_scan->table_param_.is_enable_semistruct_encoding());
              synth_scan->table_param_.set_plan_enable_rich_format(
                  template_scan->table_param_.plan_enable_rich_format());
              if (OB_FAIL(synth_scan->table_param_.convert(*table_schema,
                                                            synth_scan->access_column_ids_,
                                                            synth_scan->pd_expr_spec_.pd_storage_flag_,
                                                            &template_scan->access_column_ids_,
                                                            false,
                                                            ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy))) {
                LOG_WARN("failed to convert table param for synth scan", K(ret), K(ref_table_id));
              }
            }
            if (OB_SUCC(ret)) {
              synth_scan->table_scan_opt_ = template_scan->table_scan_opt_;
              // Set flags for full-subtree scan
              synth_scan->is_full_subtree_scan_for_nested_ = 1;
              synth_scan->is_wrap_with_unique_sort_ = 1;
              synth_scan->is_nested_scope_ = 1;
              synth_scan->is_search_index_ = 1;
              synth_scan->is_async_index_ = template_scan->is_async_index_;
              synth_scan->is_primary_table_scan_ = template_scan->is_primary_table_scan_;
              synth_scan->is_new_query_range_ = template_scan->is_new_query_range_;
              synth_scan->need_rowkey_order_ = template_scan->need_rowkey_order_;

              ObDASNestedQueryCtDef *nested_ctdef = static_cast<ObDASNestedQueryCtDef *>(root_ctdef);
              // column_idx is resolved at DSL-resolve time (from the nested clause's JSON
              // column) and carried on the nested ctdef. It equals the base-table column
              // ordinal written as the first rowkey column of the search index.
              synth_scan->column_idx_for_full_subtree_ = nested_ctdef->column_idx_;
              // nesting_depth tells the sort op how many bytes of nested_key to keep
              // (depth * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL, i.e. depth * 4)
              // before dedup — deeper-level rows get truncated to the current nested
              // element identity.
              synth_scan->nesting_depth_for_full_subtree_ = nested_ctdef->nesting_depth_;
              // Deep-copy the path from parent ObDASNestedQueryCtDef
              if (!nested_ctdef->path_.empty()) {
                if (OB_FAIL(ob_write_string(allocator, nested_ctdef->path_,
                                            synth_scan->nested_path_for_full_subtree_))) {
                  LOG_WARN("failed to copy nested path", K(ret));
                }
              }
            }
          }
        }

        // 3. Wrap in ObDASScalarCtDef(has_index_scan_=true, children_[0]=synth_scan)
        ObDASScalarCtDef *wrapper_ctdef = nullptr;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_QUERY, allocator, wrapper_ctdef))) {
          LOG_WARN("failed to allocate wrapper scalar ctdef", K(ret));
        } else if (OB_ISNULL(wrapper_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrapper ctdef is null", K(ret));
        } else {
          wrapper_ctdef->set_has_index_scan(true);
          if (OB_ISNULL(wrapper_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate wrapper children", K(ret));
          } else {
            wrapper_ctdef->children_cnt_ = 1;
            wrapper_ctdef->children_[0] = static_cast<ObDASBaseCtDef *>(synth_scan);
          }
        }

        // 4. Inject as must-child of the bool ctdef:
        //    reallocate children_ array +1, shift existing right, set children_[0] = wrapper
        if (OB_FAIL(ret)) {
        } else {
          const int64_t old_cnt = inner->children_cnt_;
          const int64_t new_cnt = old_cnt + 1;
          ObDASBaseCtDef **new_children = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, new_cnt);
          if (OB_ISNULL(new_children)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate new children array", K(ret));
          } else {
            // Shift existing children right by 1
            new_children[0] = static_cast<ObDASBaseCtDef *>(wrapper_ctdef);
            for (int64_t i = 0; i < old_cnt; ++i) {
              new_children[i + 1] = inner->children_[i];
            }
            inner->children_ = new_children;
            inner->children_cnt_ = static_cast<uint32_t>(new_cnt);

            // Update sub-clause offsets:
            // must = {exist_=true, offset_=0, count_=1}  (the new synthesized child)
            bool_ctdef->set_must(0, 1);
            // must_not offset shifts by +1 (was at offset 0 before injection)
            bool_ctdef->set_must_not(must_not_offset + 1, must_not_count);
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::synthesize_parent_domain_must_not_required(ObLogTableScan &op, ObDASBaseCtDef *root_ctdef)
{
  int ret = OB_SUCCESS;
  // After generate_final_lookup_ctdef, root_ctdef is TableLookup/IndexProjLookup.
  // Walk: Lookup → children_[0] (Fusion) → children_[search_idx] → BooleanQuery.
  // Extract the data-table scan from Lookup for use as clone template.
  const ObDASScanCtDef *data_scan = nullptr;
  ObDASBaseCtDef *bool_root = nullptr;

  if (OB_ISNULL(root_ctdef) || OB_ISNULL(cg_.phy_plan_)
      || (root_ctdef->op_type_ != DAS_OP_TABLE_LOOKUP
          && root_ctdef->op_type_ != DAS_OP_INDEX_PROJ_LOOKUP)) {
    // not a lookup ctdef or phy_plan null — skip
  } else {
    const ObDASTableLookupCtDef *lookup = static_cast<const ObDASTableLookupCtDef *>(root_ctdef);
    data_scan = lookup->get_lookup_scan_ctdef();
    if (root_ctdef->children_cnt_ > 0 && OB_NOT_NULL(root_ctdef->children_[0])) {
      ObDASBaseCtDef *rowkey_scan = root_ctdef->children_[0];
      if (rowkey_scan->op_type_ == DAS_OP_FUSION_QUERY) {
        const ObDASFusionCtDef *fusion = static_cast<const ObDASFusionCtDef *>(rowkey_scan);
        int64_t sidx = fusion->get_search_idx();
        if (sidx >= 0 && sidx < fusion->children_cnt_) {
          bool_root = fusion->children_[sidx];
        }
      } else if (rowkey_scan->op_type_ == DAS_OP_BOOLEAN_QUERY) {
        bool_root = rowkey_scan;
      }
    }
  }

  if (OB_ISNULL(bool_root) || bool_root->op_type_ != DAS_OP_BOOLEAN_QUERY
      || OB_ISNULL(data_scan) || OB_ISNULL(cg_.phy_plan_)) {
    // nothing to do
  } else {
    ObDASBooleanQueryCtDef *bool_ctdef = static_cast<ObDASBooleanQueryCtDef *>(bool_root);
    ObBooleanSubClause<ObIDASSearchCtDef> must_not_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> must_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> filter_clause;
    ObBooleanSubClause<ObIDASSearchCtDef> should_clause;
    if (OB_FAIL(bool_ctdef->must_not(must_not_clause))) {
      LOG_WARN("failed to get must_not clause", KR(ret));
    } else if (OB_FAIL(bool_ctdef->must(must_clause))) {
      LOG_WARN("failed to get must clause", KR(ret));
    } else if (OB_FAIL(bool_ctdef->filter(filter_clause))) {
      LOG_WARN("failed to get filter clause", KR(ret));
    } else if (OB_FAIL(bool_ctdef->should(should_clause))) {
      LOG_WARN("failed to get should clause", KR(ret));
    } else if (must_not_clause.empty() || !must_clause.empty() ||
               !filter_clause.empty() || !should_clause.empty()) {
      // Not a must_not-only bool; nothing to synthesize.
    } else {
      const int64_t must_not_offset = 0;
      const int64_t must_not_count = must_not_clause.count();

      ObDASNestedQueryCtDef *any_nested = nullptr;
      for (int64_t i = must_not_offset;
           OB_ISNULL(any_nested) && i < must_not_offset + must_not_count; ++i) {
        if (i < bool_root->children_cnt_ && OB_NOT_NULL(bool_root->children_[i])) {
          any_nested = find_first_nested_query_ctdef(bool_root->children_[i]);
        }
      }
      if (OB_ISNULL(any_nested)) {
        // no nested_query under must_not — skip
      } else {
        common::ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
        ObDASScalarScanCtDef *univ_scan = nullptr;
        ObDASScalarCtDef *wrapper_ctdef = nullptr;
        if (OB_FAIL(build_parent_domain_data_universe_scan(op, *data_scan, univ_scan))) {
          LOG_WARN("failed to build parent-domain universe scan", KR(ret));
        } else if (OB_ISNULL(univ_scan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("universe scan ctdef is null", KR(ret));
        } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
                       DAS_OP_SCALAR_QUERY, allocator, wrapper_ctdef))) {
          LOG_WARN("failed to allocate wrapper scalar ctdef", KR(ret));
        } else if (OB_ISNULL(wrapper_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrapper ctdef is null", KR(ret));
        } else {
          wrapper_ctdef->set_has_main_scan(true);
          if (OB_ISNULL(wrapper_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, 1))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate wrapper children", KR(ret));
          } else {
            wrapper_ctdef->children_cnt_ = 1;
            wrapper_ctdef->children_[0] = static_cast<ObDASBaseCtDef *>(univ_scan);
          }
        }

        // Inject wrapper as must[0]: reallocate children_ array +1, shift right.
        if (OB_SUCC(ret)) {
          const int64_t old_cnt = bool_root->children_cnt_;
          const int64_t new_cnt = old_cnt + 1;
          ObDASBaseCtDef **new_children = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, new_cnt);
          if (OB_ISNULL(new_children)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate new children array", KR(ret));
          } else {
            new_children[0] = static_cast<ObDASBaseCtDef *>(wrapper_ctdef);
            for (int64_t i = 0; i < old_cnt; ++i) {
              new_children[i + 1] = bool_root->children_[i];
            }
            bool_root->children_ = new_children;
            bool_root->children_cnt_ = static_cast<uint32_t>(new_cnt);
            bool_ctdef->set_must(0, 1);
            bool_ctdef->set_must_not(must_not_offset + 1, must_not_count);
            LOG_TRACE("[parent-domain must_not(nested) universe] "
                     "injected data-table scan as must[0]",
                     "ref_table_id", univ_scan->ref_table_id_,
                     "old_children_cnt", old_cnt,
                     "new_children_cnt", new_cnt);
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::build_parent_domain_data_universe_scan(
    ObLogTableScan &op,
    const ObDASScanCtDef &src,
    ObDASScalarScanCtDef *&univ_scan)
{
  int ret = OB_SUCCESS;
  univ_scan = nullptr;
  if (OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan is null", KR(ret));
  } else {
    ObSEArray<ObRawExpr *, 4> rowkey_exprs;
    ObSEArray<uint64_t, 4> rowkey_col_ids;
    LOG_TRACE("[parent-domain universe] source ctdef info",
             "src_ref_table_id", src.ref_table_id_,
             "src_schema_version", src.schema_version_,
             "src_access_column_ids_cnt", src.access_column_ids_.count());
    if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
      LOG_WARN("failed to assign rowkey exprs", KR(ret));
    } else if (OB_UNLIKELY(rowkey_exprs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent-domain universe rowkey exprs is empty", KR(ret), K(op.get_ref_table_id()));
    } else if (OB_FAIL(extract_column_ids(rowkey_exprs, rowkey_col_ids))) {
      LOG_WARN("failed to extract rowkey column ids", KR(ret));
    } else if (OB_UNLIKELY(rowkey_col_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent-domain universe rowkey column ids is empty", KR(ret), K(rowkey_exprs));
    } else {
      DASScalarCGParams cg_params(op.get_ref_table_id(),
                                  src.ref_table_id_,
                                  false, /* is new query range */
                                  true,  /* is rowkey order scan */
                                  true,  /* is primary table scan */
                                  op.get_table_type(),
                                  &rowkey_exprs,
                                  &rowkey_col_ids,
                                  &rowkey_exprs,
                                  nullptr, /* pre range graph */
                                  nullptr, /* pre query range */
                                  false, /* has pre query range */
                                  false, /* is get */
                                  true,  /* need rowkey order */
                                  op.use_column_store(),
                                  nullptr, /* domain id expr */
                                  nullptr, /* domain id col ids */
                                  nullptr, /* pushdown filters */
                                  nullptr, /* pushdown aggr exprs */
                                  nullptr  /* filter monotonicity */);
      if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, op, univ_scan))) {
        LOG_WARN("failed to generate parent-domain universe scan ctdef",
                 KR(ret), K(src.ref_table_id_));
      }
      if (OB_SUCC(ret)) {
        // Primary-table full scan in rowkey order → ObDASScalarPrimaryROROp.
        // Not a search-index scan; not a nested-domain leaf.
        univ_scan->table_scan_opt_ = src.table_scan_opt_;
        // No query range is available for the hybrid_search table-scan node
        // (op has no attached pre_range_graph_/pre_query_range_; the ranges
        // live per-search-subquery). Signal the runtime via
        // is_data_table_universe_scan_ to build a whole-range at prepare time.
        univ_scan->is_new_query_range_ = false;
        univ_scan->is_primary_table_scan_ = 1;
        univ_scan->is_rowkey_order_scan_ = 1;
        univ_scan->need_rowkey_order_ = 1;
        univ_scan->is_search_index_ = 0;
        univ_scan->is_nested_scope_ = 0;
        univ_scan->is_full_subtree_scan_for_nested_ = 0;
        univ_scan->is_wrap_with_unique_sort_ = 0;
        univ_scan->is_data_table_universe_scan_ = 1;
      }
    }
  }
  return ret;
}

ObDASNestedQueryCtDef *ObHybridSearchCgService::find_first_nested_query_ctdef(ObDASBaseCtDef *ctdef)
{
  ObDASNestedQueryCtDef *result = nullptr;
  if (OB_ISNULL(ctdef)) {
  } else if (ctdef->op_type_ == DAS_OP_NESTED_QUERY) {
    result = static_cast<ObDASNestedQueryCtDef *>(ctdef);
  } else {
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i])) {
        ObDASNestedQueryCtDef *found = find_first_nested_query_ctdef(ctdef->children_[i]);
        if (OB_NOT_NULL(found)) {
          result = found;
          break;
        }
      }
    }
  }
  return result;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op,
                                            const ObNestedQueryNode *nested_query_node,
                                            ObDASNestedQueryCtDef *&nested_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASNestedQueryCtDef *new_nested_ctdef = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(nested_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(cg_.phy_plan_), KP(nested_query_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_NESTED_QUERY,
                                                        cg_.phy_plan_->get_allocator(),
                                                        new_nested_ctdef))) {
    LOG_WARN("failed to allocate nested query ctdef", KR(ret));
  } else if (OB_ISNULL(new_nested_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested query ctdef is null", KR(ret));
  } else {
    new_nested_ctdef->nesting_depth_ = nested_query_node->nesting_depth_;
    new_nested_ctdef->column_idx_ = nested_query_node->column_idx_;
    if (!nested_query_node->path_.empty()
        && OB_FAIL(ob_write_string(cg_.phy_plan_->get_allocator(),
                                   nested_query_node->path_,
                                   new_nested_ctdef->path_))) {
      LOG_WARN("failed to deep-copy nested path string into ctdef", KR(ret));
    } else {
      new_nested_ctdef->set_is_top_level_scoring(nested_query_node->is_top_level_scoring_);
      new_nested_ctdef->set_is_scoring(nested_query_node->is_scoring_);
      nested_query_ctdef = new_nested_ctdef;
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_scalar_scan_ctdef(const DASScalarCGParams &cg_params,
                                                        const ObLogTableScan &op,
                                                        ObDASScalarScanCtDef *&scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  ObDASScalarScanCtDef *new_scalar_ctdef = nullptr;
  if (!cg_params.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_SCAN_QUERY, cg_.phy_plan_->get_allocator(), new_scalar_ctdef))) {
    LOG_WARN("failed to allocate scalar ctdef", K(ret));
  } else if (OB_ISNULL(new_scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
             OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx or schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(cg_params.get_index_table_id(), new_scalar_ctdef->schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(cg_params.get_index_table_id()));
  } else {
    const ObTableSchema *scan_table_schema = nullptr;
    new_scalar_ctdef->ref_table_id_ = cg_params.get_index_table_id();
    new_scalar_ctdef->is_get_ = cg_params.is_get();
    new_scalar_ctdef->is_new_query_range_ = cg_params.is_new_query_range();
    new_scalar_ctdef->need_rowkey_order_ = cg_params.need_rowkey_order();
    new_scalar_ctdef->is_rowkey_order_scan_ = cg_params.is_rowkey_order_scan();
    new_scalar_ctdef->is_primary_table_scan_ = cg_params.is_primary_table_scan();
    if (OB_FAIL(schema_guard->get_table_schema(
            cg_params.get_index_table_id(), scan_table_schema))) {
      LOG_WARN("get scalar scan table schema failed", K(ret), K(cg_params.get_index_table_id()));
    } else if (OB_ISNULL(scan_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar scan table schema is null", K(ret), K(cg_params.get_index_table_id()));
    } else {
      new_scalar_ctdef->is_async_index_ =
          share::schema::is_async_search_data_index(scan_table_schema->get_index_type());
    }
    // determine logical_table_aux_type_ from table schema index_type for logical table queries
    if (OB_SUCC(ret) && op.is_logical_table()) {
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(cg_params.get_index_table_id(), table_schema))) {
        LOG_WARN("get table schema for aux_type failed", K(ret), K(cg_params.get_index_table_id()));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(cg_params.get_index_table_id()));
      } else {
        new_scalar_ctdef->logical_table_aux_type_ = share::classify_aux_type_from_schema(*table_schema);
        new_scalar_ctdef->logical_index_seq_ = cg_params.get_index_table_id();
      }
      LOG_TRACE("[LT DEBUG] generate_scalar_scan_ctdef set aux_type from op",                                                                                             
        K(cg_params.get_index_table_id()), K(cg_params.get_data_table_id()),                                                                                      
        "aux_type", static_cast<int>(new_scalar_ctdef->logical_table_aux_type_),                                                                                  
        K(new_scalar_ctdef->is_primary_table_scan_),
        K(op.is_logical_table()));     
    }
  }

  // 1. generate scalar access
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(nullptr == cg_params.get_access_exprs() || cg_params.get_access_exprs()->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access exprs is empty", K(ret));
  } else {
    ObArray<uint64_t> access_col_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_params.get_access_exprs()->count(); ++i) {
      ObRawExpr *expr = cg_params.get_access_exprs()->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        if (OB_FAIL(access_col_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
          LOG_WARN("push back rowscn column id failed", K(ret));
        }
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
  
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_access_exprs(), new_scalar_ctdef->pd_expr_spec_.access_exprs_))) {
        LOG_WARN("generate rt exprs failed", K(ret));
      } else if (OB_FAIL(new_scalar_ctdef->access_column_ids_.assign(access_col_ids))) {
        LOG_WARN("assign access column ids failed", K(ret));
      } else if (OB_FAIL(cg_.mark_expr_self_produced(*cg_params.get_access_exprs()))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      }
    }
  }

  ObSEArray<uint64_t, 4> aggregate_column_ids;
  ObSEArray<uint64_t, 4> group_by_column_ids;
  ObSEArray<ObAggrParamProperty, 4> aggregate_param_props;
  // generate pushdown aggr columns
  if (OB_FAIL(ret)) {
  } else if (nullptr == cg_params.get_pushdown_aggr_exprs() || cg_params.get_pushdown_aggr_exprs()->empty()) {
    // skip
  } else {
    const ObIArray<ObRawExpr*> &pushdown_aggr_exprs = *cg_params.get_pushdown_aggr_exprs();
    ExprFixedArray &aggregate_output = new_scalar_ctdef->pd_expr_spec_.pd_storage_aggregate_output_;
    new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
    if (OB_FAIL(aggregate_output.reserve(pushdown_aggr_exprs.count()))) {
      LOG_WARN("reserve aggregate output exprs failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_aggr_exprs.count(); ++i) {
      ObAggFunRawExpr *aggr_expr = static_cast<ObAggFunRawExpr *>(pushdown_aggr_exprs.at(i));
      ObRawExpr *param_expr = nullptr;
      ObColumnRefRawExpr* col_expr = nullptr;
      ObExpr *e = nullptr;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_expr(*aggr_expr, e))) {
        LOG_WARN("generate rt expr failed", K(ret));
      } else if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(aggregate_output.push_back(e))) {
        LOG_WARN("push back aggregate output expr failed", K(ret));
      } else if (OB_FAIL(cg_.mark_expr_self_produced(aggr_expr))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      } else if (aggr_expr->get_real_param_exprs().empty()) {
        OZ(aggregate_column_ids.push_back(OB_COUNT_AGG_PD_COLUMN_ID));
        OZ(aggregate_param_props.push_back(ObAggrParamProperty(Monotonicity::NONE_MONO, true)));
      } else if (OB_ISNULL(param_expr = aggr_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param expr", K(ret), KP(param_expr));
      } else {
        ObSEArray<ObRawExpr*, 1> column_exprs;
        // TODO: support aggr param property
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(param_expr, column_exprs))) {
          LOG_WARN("fail to extract column expr", K(ret));
        } else if (OB_UNLIKELY(column_exprs.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpcted column expr count", K(ret), K(column_exprs));
        } else {
          col_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
          OZ(aggregate_column_ids.push_back(col_expr->get_column_id()));
          OZ(aggregate_param_props.push_back(ObAggrParamProperty(Monotonicity::NONE_MONO, true)));
        }
      }
    }
  }

  // 2. generate scalar table param
  if (OB_FAIL(ret)) {
  } else {
    const ObTableID &index_table_id = cg_params.get_index_table_id();
    const ObTableID &data_table_id = cg_params.get_data_table_id();
    int64_t route_policy = 0;
    bool is_cs_replica_query = false;
    ObOptimizerContext *opt_ctx = nullptr;
    const ObBasicSessionInfo *session_info = nullptr;
    const ObTableSchema *table_schema = nullptr;
  
    if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
        OB_ISNULL(session_info = opt_ctx->get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid id", K(index_table_id), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(index_table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(index_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table schema", K(ret), K(index_table_id));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info->get_effective_tenant_id()));
      if (table_schema->is_fts_or_async_fts_index() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_fts_index(true))) {
      } else if (table_schema->is_multivalue_index_aux() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_multivalue_index(true))) {
      } else if (table_schema->get_semistruct_encoding_type().is_enable_semistruct_encoding() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_enable_semistruct_encoding(true))) {
      } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
        LOG_WARN("get route policy failed", K(ret));
      } else {
        is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
        new_scalar_ctdef->table_param_.set_is_partition_table(table_schema->is_partitioned_table());
        new_scalar_ctdef->table_param_.set_is_mlog_table(table_schema->is_mlog_table());
        new_scalar_ctdef->table_param_.set_plan_enable_rich_format(opt_ctx->get_enable_rich_vector_format());
        new_scalar_ctdef->table_param_.get_enable_lob_locator_v2() = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cg_params.get_tsc_out_cols())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsc out cols is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->table_param_.convert(*table_schema,
                                                              new_scalar_ctdef->access_column_ids_,
                                                              new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                              cg_params.get_tsc_out_cols(),
                                                              is_oracle_mapping_real_virtual_table(data_table_id),
                                                              is_cs_replica_query))) {
      LOG_WARN("convert table param failed", K(ret), K(data_table_id), K(index_table_id));
    } else if (new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown() &&
        OB_FAIL(new_scalar_ctdef->table_param_.convert_group_by(*table_schema,
                                                                new_scalar_ctdef->access_column_ids_,
                                                                aggregate_column_ids,
                                                                group_by_column_ids,
                                                                aggregate_param_props,
                                                                new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_))) {
      LOG_WARN("convert group by failed", K(ret), KPC(table_schema));
    }
  }

  // 3. generate scalar result output
  if (OB_FAIL(ret)) {
  } else {
    const ExprFixedArray &access_exprs = new_scalar_ctdef->pd_expr_spec_.access_exprs_;
    const ExprFixedArray &agg_exprs = new_scalar_ctdef->pd_expr_spec_.pd_storage_aggregate_output_;
    int64_t access_expr_cnt = access_exprs.count();
    int64_t agg_expr_cnt = agg_exprs.count();
    int64_t access_column_cnt = new_scalar_ctdef->access_column_ids_.count();
    int64_t output_column_cnt = OB_INVALID_ID;
    int64_t domain_id_expr_cnt = OB_INVALID_ID;
    int64_t domain_id_col_ids_cnt = OB_INVALID_ID;
    if (OB_ISNULL(cg_params.get_tsc_out_cols())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsc out cols is null", K(ret));
    } else if (FALSE_IT(output_column_cnt = cg_params.get_tsc_out_cols()->count())) {
    } else if (OB_NOT_NULL(cg_params.get_domain_id_expr()) && FALSE_IT(domain_id_expr_cnt = cg_params.get_domain_id_expr()->count())) {
    } else if (OB_NOT_NULL(cg_params.get_domain_id_col_ids()) && FALSE_IT(domain_id_col_ids_cnt = cg_params.get_domain_id_col_ids()->count())) {
    } else if (OB_UNLIKELY(access_column_cnt != access_expr_cnt ||
                           OB_INVALID_ID == output_column_cnt ||
                           domain_id_expr_cnt != domain_id_col_ids_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt), K(domain_id_expr_cnt), K(domain_id_col_ids_cnt));
    } else if (OB_FAIL(new_scalar_ctdef->result_output_.init(output_column_cnt + agg_expr_cnt))) {
      LOG_WARN("init result output failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_cnt; ++i) {
        int64_t idx = OB_INVALID_INDEX;
        if (OB_NOT_NULL(cg_params.get_domain_id_col_ids()) &&
            OB_NOT_NULL(cg_params.get_domain_id_expr()) &&
            has_exist_in_array(*cg_params.get_domain_id_col_ids(), cg_params.get_tsc_out_cols()->at(i), &idx)) {
          ObExpr *domain_id_expr = nullptr;
          if (OB_ISNULL(cg_params.get_domain_id_expr()->at(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("domain id expr is null", K(ret));
          } else if (OB_FAIL(cg_.generate_rt_expr(*cg_params.get_domain_id_expr()->at(idx), domain_id_expr))) {
            LOG_WARN("generate rt expr failed", K(ret));
          } else if (OB_ISNULL(domain_id_expr)) {
            ret = OB_ERR_UNEXPECTED;  
            LOG_WARN("domain id expr is null", K(ret));
          } else if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(domain_id_expr))) {
            LOG_WARN("push back domain id expr failed", K(ret), K(cg_params.get_domain_id_col_ids()->at(idx)));
          }
        } else if (!has_exist_in_array(new_scalar_ctdef->access_column_ids_, cg_params.get_tsc_out_cols()->at(i), &idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("output column does not exist in access column ids", K(ret),
                   K(new_scalar_ctdef->access_column_ids_), K(cg_params.get_tsc_out_cols()->at(i)), K(*cg_params.get_tsc_out_cols()),
                   K(cg_params.get_index_table_id()), K(cg_params.get_data_table_id()));
        } else if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(access_exprs.at(idx)))) {
          LOG_WARN("store access expr to result output exprs failed", K(ret));
        }
      }

      if (OB_SUCC(ret) && new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr_cnt; ++i) {
          if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(agg_exprs.at(i)))) {
            LOG_WARN("store agg expr to result output exprs failed", K(ret));
          }
        }
      }
    }
  }

  // 4. generate scalar rowkey exprs
  if (OB_FAIL(ret)) {
  } else {
    if (OB_ISNULL(cg_params.get_rowkey_exprs()) || cg_params.get_rowkey_exprs()->empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey exprs is null", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_rowkey_exprs(), new_scalar_ctdef->rowkey_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret));
    }
  }

  // 5. generate scalar pushdown filters
  if (OB_FAIL(ret)) {
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", K(ret));
    } else if (OB_ISNULL(cg_params.get_access_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access exprs is null", K(ret));
    } else {
      bool pd_blockscan = ObPushdownFilterUtils::is_blockscan_pushdown_enabled(tenant_config->_pushdown_storage_level);
      bool pd_filter = ObPushdownFilterUtils::is_filter_pushdown_enabled(tenant_config->_pushdown_storage_level);
      bool enable_base_skip_index = tenant_config->_enable_skip_index;
      bool enable_inc_skip_index = tenant_config->_enable_skip_index;
      int tmp_ret = OB_E(EventTable::EN_DISABLE_INC_SKIP_INDEX_SCAN) OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        enable_inc_skip_index = false;
      }
      bool enable_prefetch_limit = tenant_config->_enable_prefetch_limiting;
      bool enable_column_store = cg_params.is_use_column_store();
      bool enable_filter_reordering = tenant_config->_enable_filter_reordering;
  
      new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_flags(pd_blockscan, pd_filter, enable_base_skip_index, 
        enable_column_store, enable_prefetch_limit, enable_filter_reordering, enable_inc_skip_index);
      new_scalar_ctdef->table_scan_opt_.io_read_batch_size_ = tenant_config->_io_read_batch_size;
      new_scalar_ctdef->table_scan_opt_.io_read_gap_size_ = tenant_config->_io_read_batch_size * tenant_config->_io_read_redundant_limit_percentage / 100;
      new_scalar_ctdef->table_scan_opt_.storage_rowsets_size_ = tenant_config->storage_rowsets_size;
  
      if (!pd_blockscan) {
        pd_filter = false;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && (pd_blockscan || pd_filter) && i < cg_params.get_access_exprs()->count(); ++i) {
          ObRawExpr *expr = cg_params.get_access_exprs()->at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret));
          } else if ((enable_column_store && T_ORA_ROWSCN == expr->get_expr_type()) ||
                      T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type() ||
                      T_PSEUDO_OLD_NEW_COL == expr->get_expr_type()) {
            pd_blockscan = false;
            pd_filter = false;
          } else if (expr->is_column_ref_expr()) {
            const ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr *>(expr);
            if (col->is_lob_column() && cg_.get_cur_cluster_version() < CLUSTER_VERSION_4_1_0_0) {
              pd_filter = false;
            }
          }
        }
      }
  
      if (OB_SUCC(ret)) {
        enable_base_skip_index = enable_base_skip_index && pd_filter;
        enable_inc_skip_index = enable_inc_skip_index && pd_filter;
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(pd_filter);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(enable_base_skip_index);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(enable_inc_skip_index);
        LOG_TRACE("pushdown filters flag and table scan opt", K(cg_params.get_index_table_id()),
                                                              "pd storage flag", new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                              "table scan opt", new_scalar_ctdef->table_scan_opt_);
      }
    }
  
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(cg_params.get_pushdown_filters()) && !cg_params.get_pushdown_filters()->empty()) {
      // generate pushdown filters
      if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_pushdown_filters(), new_scalar_ctdef->pd_expr_spec_.pushdown_filters_))) {
        LOG_WARN("generate pushdown filters failed", K(ret));
      } else if (new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
        ObPushdownFilterConstructor filter_constructor(&cg_.phy_plan_->get_allocator(),
                                                       cg_,
                                                       cg_params.get_filter_monotonicity(),
                                                       new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_use_column_store(),
                                                       cg_params.get_table_type(),
                                                       new_scalar_ctdef->table_param_.is_enable_semistruct_encoding());
        if (OB_FAIL(filter_constructor.apply(*cg_params.get_pushdown_filters(), new_scalar_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to apply filter constructor", K(ret));
        } else if (OB_FAIL(new_scalar_ctdef->table_param_.check_is_safe_filter_with_di(*cg_params.get_pushdown_filters(),
                                                                                       *new_scalar_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to check lob column pushdown", K(ret));
        }
        LOG_TRACE("pushdown filters", K(cg_params.get_index_table_id()), K(*cg_params.get_pushdown_filters()));  
      }
    }
  }

  // 6. generate scalar query range
  if (OB_FAIL(ret) || !cg_params.has_pre_query_range()) {
  } else if (cg_params.is_new_query_range()) {
    if (OB_ISNULL(cg_params.get_pre_range_graph())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pre range graph is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->pre_range_graph_.deep_copy(*cg_params.get_pre_range_graph()))) {
      LOG_WARN("deep copy pre range graph failed", K(ret));
    } else {
      new_scalar_ctdef->enable_new_false_range_ = cg_params.get_pre_range_graph()->enable_new_false_range();
    }
  } else {
    if (OB_ISNULL(cg_params.get_pre_query_range())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pre query range is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->pre_query_range_.deep_copy(*cg_params.get_pre_query_range()))) {
      LOG_WARN("deep copy pre query range failed", K(ret));
    } else {
      new_scalar_ctdef->enable_new_false_range_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    scan_ctdef = new_scalar_ctdef;
    LOG_TRACE("generate scalar search ctdef success", KPC(scan_ctdef));
  }
  return ret;
}

// In the hybrid search scenario, tsc_ctdef.scan_ctdef_ is not used to perform scans, but it undertakes
// functions such as partition routing and represents the primary partition corresponding to the DAS SCAN OP.
// Therefore, it is still necessary to fill the required information.
int ObHybridSearchCgService::mock_das_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = nullptr;
  common::ObTableID main_table_id = op.get_ref_table_id();
  if (OB_ISNULL(cg_.opt_ctx_) ||
      OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(main_table_id,
                                                            scan_ctdef.schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(main_table_id));
  } else {
    scan_ctdef.ref_table_id_ = main_table_id;
    if (OB_NOT_NULL(op.get_plan()->get_stmt()) && OB_NOT_NULL(op.get_plan()->get_stmt()->get_query_ctx())) {
      scan_ctdef.hybrid_search_monitor_ = op.get_plan()->get_stmt()->get_query_ctx()->get_global_hint().monitor_;
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_final_lookup_ctdef(ObLogTableScan &op,
                                                         ObTableScanCtDef &tsc_ctdef,
                                                         ObDASBaseCtDef &search_ctdef,
                                                         ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDef *main_scan_ctdef = nullptr;
  ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  ObDASTableLocMeta *loc_meta = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || 
      OB_ISNULL(cg_.opt_ctx_) ||
      OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.phy_plan_), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(), op.get_ref_table_id(), op.get_stmt(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(op.get_table_id()), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr table schema", K(ret));
  } else {
    ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    if (OB_ISNULL(loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate lookup location meta buffer failed", K(ret));
    } else {
      loc_meta->reset();
      loc_meta->table_loc_id_ = op.get_table_id();
      loc_meta->ref_table_id_ = op.get_real_ref_table_id();
      // force select leader for now
      loc_meta->select_leader_ = 1;
      TableLocRelInfo *rel_info = cg_.opt_ctx_->get_loc_rel_info_by_id(op.get_table_id(), op.get_real_ref_table_id());
      if (OB_ISNULL(rel_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("relation info is nullptr", K(ret), K(op.get_table_id()), K(op.get_real_ref_table_id()));
      } else if (rel_info->related_ids_.count() > 1) {
        loc_meta->related_table_ids_.set_capacity(rel_info->related_ids_.count() - 1);
        for (int64_t i = 0; OB_SUCC(ret) && i < rel_info->related_ids_.count(); ++i) {
          if (rel_info->related_ids_.at(i) != loc_meta->ref_table_id_) {
            if (OB_FAIL(loc_meta->related_table_ids_.push_back(rel_info->related_ids_.at(i)))) {
              LOG_WARN("store related table id failed", K(ret), KPC(rel_info), K(i), K(loc_meta));
            }
          }
        }
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    const bool is_dsl_query = is_dsl_query_path(op.get_access_path());
    if (OB_FAIL(generate_main_scan_ctdef(op, main_scan_ctdef))) {
      LOG_WARN("failed to generate main scan ctdef", K(ret), K(op.get_ref_table_id()));
    } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
        is_dsl_query ? DAS_OP_INDEX_PROJ_LOOKUP : DAS_OP_TABLE_LOOKUP, allocator, lookup_ctdef))) {
      LOG_WARN("alloc lookup ctdef failed", K(ret), K(is_dsl_query));
    } else if (OB_ISNULL(lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      lookup_ctdef->children_cnt_ = 2;
      if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(loc_meta))) {
        LOG_WARN("store scan loc meta failed", K(ret));
      } else {
        lookup_ctdef->children_[0] = &search_ctdef;
        lookup_ctdef->children_[1] = main_scan_ctdef;
        tsc_ctdef.flashback_item_.need_scn_ |= main_scan_ctdef->check_need_fill_scn();
      }
    }
  }

  // generate lookup result output exprs
  if (OB_SUCC(ret)) {
    ObArray<ObExpr *> result_outputs;
    if (!ObDASTaskFactory::is_attached(search_ctdef.op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search ctdef is not attached", K(ret), K(search_ctdef.op_type_));
    } else if (OB_FAIL(result_outputs.assign(main_scan_ctdef->result_output_))) {
      LOG_WARN("assign result output failed", K(ret));
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == lookup_ctdef->op_type_) {
      // DSL query need to project the result output of the search ctdef
      ObDASIndexProjLookupCtDef *index_proj_lookup_ctdef = static_cast<ObDASIndexProjLookupCtDef *>(lookup_ctdef);
      const ObDASFusionCtDef *fusion_ctdef = static_cast<const ObDASFusionCtDef *>(&search_ctdef);
      if (fusion_ctdef->has_hybrid_fusion_op_) {
        // hybrid fusion op needs all path scores, always project full result_output
        if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.assign(
            static_cast<ObDASAttachCtDef &>(search_ctdef).result_output_))) {
          LOG_WARN("failed to assign full result output for hybrid fusion op", K(ret));
        }
      } else if (fusion_ctdef->fusion_iter_exec_mode_ == ObFusionIterExecMode::SKIP_FUSION_ITER) {
        const int64_t proj_expr_cnt = fusion_ctdef->rowid_exprs_.count() + (fusion_ctdef->track_score_ ? 1 : 0);
        index_proj_lookup_ctdef->index_scan_proj_exprs_.set_capacity(proj_expr_cnt);
        for (int64_t i = 0; OB_SUCC(ret) && i < fusion_ctdef->rowid_exprs_.count(); ++i) {
          if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.push_back(fusion_ctdef->rowid_exprs_.at(i)))) {
            LOG_WARN("failed to push back rowid expr for skip-fusion path", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && fusion_ctdef->track_score_) {
          ObExpr *fusion_score_expr = fusion_ctdef->get_fusion_score_expr();
          if (OB_ISNULL(fusion_score_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fusion score expr is null in skip-fusion path", K(ret), KPC(fusion_ctdef));
          } else if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.push_back(fusion_score_expr))) {
            LOG_WARN("failed to push back fusion score expr for skip-fusion path", K(ret));
          }
        }
      } else if (!fusion_ctdef->track_score_) {
        if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.assign(fusion_ctdef->rowid_exprs_))) {
          LOG_WARN("failed to assign rowid exprs for non-score path", K(ret));
        }
      } else if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.assign(
          static_cast<ObDASAttachCtDef &>(search_ctdef).result_output_))) {
        LOG_WARN("failed to assign index scan project column exprs", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(append_array_no_dup(result_outputs, index_proj_lookup_ctdef->index_scan_proj_exprs_))) {
        LOG_WARN("failed to append final result outputs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      } else {
        root_ctdef = lookup_ctdef;
        LOG_TRACE("generate final lookup ctdef", KPC(root_ctdef));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_main_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef *&main_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(cg_.opt_ctx_) || OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.phy_plan_), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN,
                                                       cg_.phy_plan_->get_allocator(),
                                                       scan_ctdef))) {
    LOG_WARN("failed to allocate scan ctdef", K(ret));
  } else if (OB_ISNULL(scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan ctdef is null", K(ret));
  } else {
    common::ObTableID ref_table_id = op.get_real_ref_table_id();
    ObArray<ObRawExpr *> lookup_pushdown_filters;
    ObArray<ObRawExpr *> non_pushdown_filters;
    if (OB_FAIL(prepare_main_scan_access_ctdef(op,
                                               *scan_ctdef,
                                               non_pushdown_filters,
                                               lookup_pushdown_filters))) {
      LOG_WARN("failed to prepare main scan access ctdef", K(ret));
    }

    // generate auto split exprs
    if (OB_SUCC(ret)) {
      ObRawExpr *auto_split_expr = const_cast<ObRawExpr*>(op.get_auto_split_filter());
      const uint64_t auto_split_filter_type = op.get_auto_split_filter_type();
      if (OB_NOT_NULL(auto_split_expr) && OB_INVALID_ID != auto_split_filter_type) {
        if (OB_FAIL(cg_.generate_rt_expr(*auto_split_expr,
                                         scan_ctdef->pd_expr_spec_.auto_split_expr_))) {
          LOG_WARN("generate auto split filter expr failed", K(ret));
        } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_auto_split_params(),
                                                 scan_ctdef->pd_expr_spec_.auto_split_params_))) {
          LOG_WARN("generate auto split params failed", K(ret));
        } else {
          scan_ctdef->pd_expr_spec_.auto_split_filter_type_ = auto_split_filter_type;
        }
      }
    }

    // generate table schema version
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard->get_table_schema_version(ref_table_id,
                                                         scan_ctdef->schema_version_))) {
        LOG_WARN("get table schema version failed", K(ret), K(ref_table_id));
      }
    }

    // generate output column ids and table param
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> output_exprs;
      ObArray<ObRawExpr *> output_column_exprs;
      ObArray<uint64_t> output_column_ids;
      if (OB_FAIL(output_exprs.assign(op.get_output_exprs()))) {
        LOG_WARN("failed to assign output exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(output_exprs, non_pushdown_filters))) {
        LOG_WARN("failed to append filter exprs to output exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(output_exprs, output_column_exprs, true))) {
        LOG_WARN("failed to extract output column exprs", K(ret));
      } else if (OB_FAIL(extract_column_ids(output_column_exprs, output_column_ids))) {
        LOG_WARN("failed to extract output column ids", K(ret));
      } else {
        ObBasicSessionInfo *session_info = cg_.opt_ctx_->get_session_info();
        const ObTableSchema *table_schema = nullptr;
        int64_t route_policy = 0;
        if (OB_ISNULL(session_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr session info", K(ret), K(session_info));
        } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                          ref_table_id,
                                                          op.get_stmt(),
                                                          table_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(op.get_table_id()), K(ref_table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr table schema", K(ret), K(op.get_table_id()), K(ref_table_id));
        } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
          LOG_WARN("failed to get route policy", K(ret));
        } else if (FALSE_IT(scan_ctdef->table_param_.get_enable_lob_locator_v2()
                            = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
        } else if (OB_FAIL(scan_ctdef->table_param_.convert(*table_schema,
                                                            scan_ctdef->access_column_ids_,
                                                            scan_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                            &output_column_ids,
                                                            false, /* not oracle mapping real virtual table */
                                                            ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy))) {
          LOG_WARN("convert schema failed", K(ret), K(*table_schema), K(scan_ctdef->access_column_ids_));
        } else if (OB_FAIL(scan_ctdef->result_output_.init(output_column_ids.count()))) {
          LOG_WARN("init result output failed", K(ret), K(ref_table_id), K(output_column_ids));
        } else if (OB_UNLIKELY(scan_ctdef->pd_expr_spec_.access_exprs_.count() != scan_ctdef->access_column_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("access exprs count is not equal to access column ids count", K(ret), 
                   K(ref_table_id), K(scan_ctdef->pd_expr_spec_.access_exprs_.count()), K(scan_ctdef->access_column_ids_.count()),
                   K(scan_ctdef->pd_expr_spec_.access_exprs_), K(scan_ctdef->access_column_ids_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); i++) {
            int64_t idx = OB_INVALID_INDEX;
            if (!has_exist_in_array(scan_ctdef->access_column_ids_, output_column_ids.at(i), &idx)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("output column does not exist in access column ids", K(ret),
                       K(ref_table_id), K(i), K(output_column_ids), K(scan_ctdef->access_column_ids_));
            } else if (OB_FAIL(scan_ctdef->result_output_.push_back(scan_ctdef->pd_expr_spec_.access_exprs_.at(idx)))) {
              LOG_WARN("store access expr to result output exprs failed", K(ret));
            }
          }
        }
      }
      LOG_TRACE("main scan output column ids", K(ret), K(ref_table_id), K(output_column_ids));
    }

    // generate pd storage flag and pushdown filters
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_.generate_tsc_flags(op, *scan_ctdef))) {
        LOG_WARN("failed to generate tsc flags", K(ret));
      } else {
        bool pd_blockscan = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_blockscan_pushdown();
        bool pd_filter = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown();
        bool enable_base_skip_index = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_apply_base_skip_index();
        bool enable_inc_skip_index = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_apply_inc_skip_index();
        if (!pd_blockscan) {
          pd_filter = false;
        } else {
          FOREACH_CNT_X(e, op.get_access_exprs(), pd_blockscan || pd_filter) {
            if ((op.use_column_store() && T_ORA_ROWSCN == (*e)->get_expr_type()) 
                || T_PSEUDO_EXTERNAL_FILE_URL == (*e)->get_expr_type()
                || T_PSEUDO_OLD_NEW_COL == (*e)->get_expr_type()) {
              pd_blockscan = false;
              pd_filter = false;
            }
          }
        }
        enable_base_skip_index = enable_base_skip_index && pd_filter;
        enable_inc_skip_index = enable_inc_skip_index && pd_filter;
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(pd_filter);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(enable_base_skip_index);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(enable_inc_skip_index);
        LOG_TRACE("main scan pd storage flag", K(ref_table_id), K(pd_blockscan), K(pd_filter), K(enable_base_skip_index), K(enable_inc_skip_index));
        if (!lookup_pushdown_filters.empty()) {
          if (OB_FAIL(cg_.generate_rt_exprs(lookup_pushdown_filters, scan_ctdef->pd_expr_spec_.pushdown_filters_))) {
            LOG_WARN("failed to generate lookup pushdown filters", K(ret));
          } else if (scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
            ObPushdownFilterConstructor filter_constructor(
              &cg_.phy_plan_->get_allocator(), cg_, 
              &op.get_filter_monotonicity(),
              scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_use_column_store(),
              op.get_table_type(),
              scan_ctdef->table_param_.is_enable_semistruct_encoding());
            if (OB_FAIL(filter_constructor.apply(
              lookup_pushdown_filters, scan_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
              LOG_WARN("failed to apply filter constructor", K(ret));
            } else if (OB_FAIL(scan_ctdef->table_param_.check_is_safe_filter_with_di(
                        lookup_pushdown_filters,
                        *scan_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
              LOG_WARN("failed to check lob column pushdown", K(ret));
            }
            LOG_TRACE("main scan pushdown filters", K(ref_table_id), K(lookup_pushdown_filters));
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(scan_ctdef)) {
      main_scan_ctdef = scan_ctdef;
      main_scan_ctdef->is_get_ = true;
    }
  }
  return ret;
}

int ObHybridSearchCgService::extract_column_ids(const ObIArray<ObRawExpr *> &column_exprs,
                                                ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    if (OB_ISNULL(column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_ORA_ROWSCN == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("store ora rowscan failed", K(ret));
      }
    } else if (T_PSEUDO_OLD_NEW_COL == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID))) {
        LOG_WARN("store mview old new column id failed", K(ret));
      }
    } else if (T_PSEUDO_GROUP_ID == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_GROUP_IDX_COLUMN_ID))) {
        LOG_WARN("store group column id failed", K(ret));
      }
    } else if (column_exprs.at(i)->is_column_ref_expr()) {
      const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr*>(column_exprs.at(i));
      if (col_expr->is_pseudo_column_ref()) {
        //part_id pseudo columnref exprs not produced in DAS, ignore it
      } else if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store column id failed", K(ret));
      }
    } else {
      //other column exprs not produced in DAS, ignore it
    }
  }
  return ret;
}

int ObHybridSearchCgService::check_column_ids_accessible(
  uint64_t ref_table_id, const ObSqlSchemaGuard &schema_guard, const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema", K(ret), K(ref_table_id));
  } else {
    ObArray<ObColDesc> column_descs;
    if (OB_FAIL(table_schema->get_store_column_ids(column_descs))) {
      LOG_WARN("failed to get store column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
        // ORA_ROWSCN is supplied by storage, not by a schema store column.
        bool is_found = (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_ids.at(i));
        for (int64_t j = 0; !is_found && j < column_descs.count(); j++) {
          if (column_ids.at(i) == column_descs.at(j).col_id_) {
            is_found = true;
          }
        }
        if (!is_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column id is not accessible", K(ret), K(i), K(ref_table_id), K(column_ids), K(column_descs));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::try_alloc_topk_collect_ctdef(ObDASFusionCtDef *fusion_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  if (OB_ISNULL(fusion_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fusion_ctdef));
  } else if (fusion_ctdef->need_topk()
      && fusion_ctdef->has_search_subquery()
      && fusion_ctdef->need_search_score()
      && fusion_ctdef->get_search_ctdef()->is_scoring()) {
    ObDASTopKCollectCtDef *topk_collect_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TOPK_COLLECT, allocator, topk_collect_ctdef))) {
      LOG_WARN("failed to allocate topk collect ctdef", K(ret));
    } else if (OB_ISNULL(topk_collect_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null top k collect ctdef", K(ret));
    } else if (OB_ISNULL(topk_collect_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate fusion ctdef children failed", K(ret));
    } else {
      topk_collect_ctdef->set_is_scoring(fusion_ctdef->get_search_ctdef()->is_scoring());
      topk_collect_ctdef->limit_ = fusion_ctdef->rank_window_size_expr_;
      const int64_t search_idx = fusion_ctdef->get_search_idx();
      topk_collect_ctdef->children_[0] = fusion_ctdef->children_[search_idx];
      topk_collect_ctdef->children_cnt_ = 1;
      fusion_ctdef->children_[search_idx] = topk_collect_ctdef;
    }
  }
  return ret;
}

// ─── M4: logical table rewrite pass for hybrid search ───

namespace
{
ObColumnRefRawExpr *hybrid_lt_find_col_ref_by_id(ObLogTableScan &op, const uint64_t col_id)
{
  ObColumnRefRawExpr *found = nullptr;
  const ObIArray<ObRawExpr *> &accs = op.get_access_exprs();
  for (int64_t i = 0; i < accs.count(); ++i) {
    ObRawExpr *e = accs.at(i);
    if (OB_NOT_NULL(e) && e->is_column_ref_expr()) {
      ObColumnRefRawExpr *c = static_cast<ObColumnRefRawExpr *>(e);
      if (c->get_column_id() == col_id) {
        found = c;
        break;
      }
    }
  }
  if (OB_ISNULL(found)) {
    const ObIArray<ObRawExpr *> &pse = op.get_pseudo_columnref_exprs();
    for (int64_t i = 0; i < pse.count(); ++i) {
      ObRawExpr *e = pse.at(i);
      if (OB_NOT_NULL(e) && e->is_column_ref_expr()) {
        ObColumnRefRawExpr *c = static_cast<ObColumnRefRawExpr *>(e);
        if (c->get_column_id() == col_id) {
          found = c;
          break;
        }
      }
    }
  }
  return found;
}

int hybrid_lt_resolve_ivf_aux_col_ref(ObLogTableScan &op,
                                      const share::schema::ObTableSchema &aux_schema,
                                      const uint64_t cid,
                                      ObRawExprFactory &expr_factory,
                                      const ObSQLSessionInfo *session,
                                      ObColumnRefRawExpr *&cref)
{
  int ret = OB_SUCCESS;
  cref = hybrid_lt_find_col_ref_by_id(op, cid);
  if (OB_ISNULL(cref)) {
    const share::schema::ObColumnSchemaV2 *col_schema = aux_schema.get_column_schema(cid);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG][IVF aux decode] column schema not found in aux_schema",
               K(ret),
               K(cid),
               "aux_table_id",
               aux_schema.get_table_id());
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, session, cref))) {
      LOG_WARN("[LT hybrid CG][IVF aux decode] build_column_expr from aux schema failed",
               K(ret),
               K(cid));
    }
  }
  return ret;
}

int hybrid_lt_build_json_path(const ObColumnRefRawExpr *col, ObIAllocator &alloc, ObString &path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString &cname = col->get_column_name();
    const int32_t path_len = 2 + cname.length();
    char *buf = static_cast<char *>(alloc.alloc(path_len + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      buf[0] = '$';
      buf[1] = '.';
      if (cname.length() > 0) {
        MEMCPY(buf + 2, cname.ptr(), cname.length());
      }
      buf[path_len] = '\0';
      path.assign_ptr(buf, path_len);
    }
  }
  return ret;
}

int hybrid_lt_push_mapping_entry(const ObColumnRefRawExpr *col,
                                 const ObLogicalColumnSource src,
                                 const bool is_rowid,
                                 const int64_t key_suffix_ordinal,
                                 common::ObIAllocator &alloc,
                                 ObLogicalColumnMappingEntry &entry,
                                 const bool is_query_range_ns = false)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    entry.logical_column_id_ = col->get_column_id();
    entry.source_ = src;
    entry.is_rowid_ = is_rowid;
    entry.is_query_range_ns_ = is_query_range_ns;
    entry.key_suffix_ordinal_ = key_suffix_ordinal;
    // Slice ObRawExprResType -> ObObjMeta: carries type/cs_type/scale/has_lob_header
    // so that decoder side (project_logical_columns) can dispatch by original column type.
    entry.obj_meta_ = col->get_result_type();
    if (FROM_VALUE_PATH == src) {
      ObString path_str;
      if (OB_FAIL(hybrid_lt_build_json_path(col, alloc, path_str))) {
      } else if (!path_str.empty()) {
        char *buf = static_cast<char *>(alloc.alloc(path_str.length()));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(buf, path_str.ptr(), path_str.length());
          entry.json_path_.assign_ptr(buf, path_str.length());
        }
      }
    }
  }
  return ret;
}
}  // namespace

int ObHybridSearchCgService::build_hybrid_logical_decode_meta_(
    ObLogTableScan &op,
    const uint64_t logical_schema_table_id,
    const ObIArray<uint64_t> &orig_access_col_ids,
    const ObIArray<ObExpr *> &orig_access_exprs,
    ObLogicalTableColumnMapping &logical_column_mapping,
    sql::ExprFixedArray &logical_output_exprs)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &alloc = cg_.phy_plan_->get_allocator();
  logical_column_mapping.entries_.reuse();
  logical_output_exprs.destroy();
  common::ObArray<ObExpr *> tmp_logical_exprs;
  const int64_t n = orig_access_col_ids.count();
  LOG_TRACE("[LT hybrid CG] build_hybrid_logical_decode_meta_ begin",
           K(logical_schema_table_id), K(n), K(orig_access_exprs.count()));
  if (OB_UNLIKELY(n != orig_access_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG] orig access col ids / exprs count mismatch", K(ret), K(n),
             K(orig_access_exprs.count()));
  } else if (OB_ISNULL(cg_.opt_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG] opt_ctx is null", K(ret));
  } else {
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    const share::schema::ObTableSchema *table_schema = nullptr;
    ObRawExprFactory &expr_factory = cg_.opt_ctx_->get_expr_factory();
    const ObSQLSessionInfo *session_info = nullptr;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG] schema_guard is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(logical_schema_table_id, table_schema))) {
      LOG_WARN("[LT hybrid CG] get table schema failed", K(ret), K(logical_schema_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG] table schema is null", K(ret), K(logical_schema_table_id));
    } else {
      if (OB_NOT_NULL(cg_.opt_ctx_->get_exec_ctx())) {
        session_info = cg_.opt_ctx_->get_exec_ctx()->get_my_session();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
        const uint64_t cid = orig_access_col_ids.at(i);
        ObExpr *rt_expr = orig_access_exprs.at(i);
        if (common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == cid) {
          LOG_TRACE("[LT hybrid CG] skip hidden group idx column for logical mapping", K(i), K(cid));
          continue;
        }
        ObColumnRefRawExpr *col = nullptr;
        if (OB_FAIL(hybrid_lt_resolve_ivf_aux_col_ref(
                op, *table_schema, cid, expr_factory, session_info, col))) {
          LOG_WARN("[LT hybrid CG] resolve logical column ref failed", K(ret), K(i), K(cid));
        } else if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[LT hybrid CG] column ref is null after resolve", K(ret), K(i), K(cid));
        } else {
          ObLogicalColumnMappingEntry entry;
          // namespace_id / ltable_id: filled from query range prefix in DAS (lt_ctx_),
          // aligned with ObTscCgService::build_logical_table_scan_meta_.
          if (0 == col->get_column_name().case_compare("namespace_id")) {
            if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_QUERY_RANGE, false,
                    /*ordinal*/ -1, alloc, entry, true))) {
              LOG_WARN("[LT hybrid CG] push namespace_id mapping entry failed", K(ret), K(i));
            }
          } else if (0 == col->get_column_name().case_compare("ltable_id")) {
            if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_QUERY_RANGE, false,
                    /*ordinal*/ -1, alloc, entry, false))) {
              LOG_WARN("[LT hybrid CG] push ltable_id mapping entry failed", K(ret), K(i));
            }
          } else if (0 == col->get_column_name().case_compare(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME)) {
            // primary table col_descs: [0]=rowid (uint64). Logical table uses __pk_increment.
            if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_ROWID, true, /*ordinal*/ 0, alloc, entry))) {
              LOG_WARN("[LT hybrid CG] push rowid mapping entry failed", K(ret), K(i));
            }
          } else {
            if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_VALUE_PATH, false, /*ordinal*/ -1, alloc, entry))) {
              LOG_WARN("[LT hybrid CG] push value-path mapping entry failed", K(ret), K(i));
            } else if (ob_is_collection_sql_type(col->get_data_type())) {
              entry.is_vector_col_ = true;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(logical_column_mapping.entries_.push_back(entry))) {
              LOG_WARN("[LT hybrid CG] push_back mapping entry failed", K(ret), K(i));
            } else if (OB_FAIL(tmp_logical_exprs.push_back(rt_expr))) {
              LOG_WARN("[LT hybrid CG] push_back logical output expr failed", K(ret), K(i));
            } else {
              LOG_TRACE("[LT hybrid CG] mapping entry added",
                       "idx", i, K(cid), "col_name", col->get_column_name(),
                       "source", static_cast<int>(entry.source_),
                       K(entry.is_query_range_ns_), KPC(rt_expr));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(logical_output_exprs.init(tmp_logical_exprs.count()))) {
      LOG_WARN("[LT hybrid CG] init logical_output_exprs failed", K(ret));
    } else if (OB_FAIL(logical_output_exprs.assign(tmp_logical_exprs))) {
      LOG_WARN("[LT hybrid CG] assign logical_output_exprs failed", K(ret));
    } else {
      LOG_TRACE("[LT hybrid CG] build_hybrid_logical_decode_meta_ done",
               "mapping_cnt", logical_column_mapping.entries_.count(),
               "logical_output_cnt", logical_output_exprs.count());
    }
  }
  return ret;
}

template <class CtDefT>
int ObHybridSearchCgService::build_scalar_primary_logical_decode_meta_(
    ObLogTableScan &op,
    const ObIArray<uint64_t> &orig_access_col_ids,
    const ObIArray<ObExpr *> &orig_access_exprs,
    CtDefT &mut_scan)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &alloc = cg_.phy_plan_->get_allocator();
  common::ObArray<ObExpr *> tmp_logical_exprs;
  const int64_t n = orig_access_col_ids.count();
  mut_scan.logical_column_mapping_.entries_.reuse();
  mut_scan.logical_output_exprs_.destroy();
  if (OB_UNLIKELY(n != orig_access_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG] scalar primary access col ids / exprs count mismatch",
             K(ret), K(n), K(orig_access_exprs.count()));
  } else if (OB_ISNULL(cg_.opt_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG] opt_ctx is null for scalar primary decode meta", K(ret));
  } else {
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    const share::schema::ObTableSchema *primary_schema = nullptr;
    ObRawExprFactory &expr_factory = cg_.opt_ctx_->get_expr_factory();
    const ObSQLSessionInfo *session_info = nullptr;
    if (OB_NOT_NULL(cg_.opt_ctx_->get_exec_ctx())) {
      session_info = cg_.opt_ctx_->get_exec_ctx()->get_my_session();
    }
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG] schema guard is null for scalar primary decode meta", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), primary_schema))) {
      LOG_WARN("[LT hybrid CG] get primary table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_ISNULL(primary_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG] primary table schema is null", K(ret), K(op.get_ref_table_id()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
      const uint64_t cid = orig_access_col_ids.at(i);
      ObExpr *rt_expr = orig_access_exprs.at(i);
      ObColumnRefRawExpr *col = nullptr;
      if (OB_FAIL(hybrid_lt_resolve_ivf_aux_col_ref(
              op, *primary_schema, cid, expr_factory, session_info, col))) {
        LOG_WARN("[LT hybrid CG] resolve scalar primary column ref failed", K(ret), K(i), K(cid));
      } else if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid CG] column expr not found for scalar primary stats scan",
                 K(ret), K(i), K(cid));
      } else {
        ObLogicalColumnMappingEntry entry;
        if (0 == col->get_column_name().case_compare("namespace_id")) {
          if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_QUERY_RANGE, false,
                   /*ordinal*/ -1, alloc, entry, true))) {
            LOG_WARN("push namespace mapping failed", K(ret));
          }
        } else if (0 == col->get_column_name().case_compare("ltable_id")) {
          if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_QUERY_RANGE, false,
                   /*ordinal*/ -1, alloc, entry, false))) {
            LOG_WARN("push ltable mapping failed", K(ret));
          }
        } else if (share::is_logical_rowid_column_name(col->get_column_name())) {
          if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_ROWID, true,
                   /*ordinal*/ 0, alloc, entry))) {
            LOG_WARN("push rowid mapping failed", K(ret));
          }
        } else if (OB_FAIL(hybrid_lt_push_mapping_entry(col, FROM_VALUE_PATH, false,
                    /*ordinal*/ -1, alloc, entry))) {
          LOG_WARN("push value-path mapping failed", K(ret), KPC(col));
        } else if (ob_is_collection_sql_type(col->get_data_type())) {
          entry.is_vector_col_ = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(mut_scan.logical_column_mapping_.entries_.push_back(entry))) {
            LOG_WARN("push scalar primary mapping failed", K(ret));
          } else if (OB_FAIL(tmp_logical_exprs.push_back(rt_expr))) {
            LOG_WARN("push scalar primary logical output failed", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mut_scan.logical_output_exprs_.init(tmp_logical_exprs.count()))) {
      LOG_WARN("init scalar primary logical outputs failed", K(ret));
    } else if (OB_FAIL(mut_scan.logical_output_exprs_.assign(tmp_logical_exprs))) {
      LOG_WARN("assign scalar primary logical outputs failed", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchCgService::build_hybrid_lookup_logical_decode_meta_(
    ObLogTableScan &op,
    const ObIArray<uint64_t> &orig_access_col_ids,
    const ObIArray<ObExpr *> &orig_access_exprs,
    ObDASScanCtDef &mut_lookup)
{
  return build_hybrid_logical_decode_meta_(op,
                                           mut_lookup.ref_table_id_,
                                           orig_access_col_ids,
                                           orig_access_exprs,
                                           mut_lookup.logical_column_mapping_,
                                           mut_lookup.logical_output_exprs_);
}

int ObHybridSearchCgService::build_ivf_aux_logical_decode_meta_(
    ObLogTableScan &op,
    const share::schema::ObTableSchema &aux_schema,
    const common::ObIArray<uint64_t> &orig_access_col_ids,
    const common::ObIArray<ObExpr *> &orig_access_exprs,
    ObDASScanCtDef &mut_scan)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &alloc = cg_.phy_plan_->get_allocator();
  const uint64_t logical_aux_tid = mut_scan.ref_table_id_;
  const int64_t n_ids = orig_access_col_ids.count();
  const int64_t n_exprs = orig_access_exprs.count();
  LOG_TRACE("[LT hybrid CG][IVF aux decode] build_ivf_aux_logical_decode_meta_ enter",
           K(ret),
           K(logical_aux_tid),
           KPC(&mut_scan),
           K(n_ids),
           K(n_exprs),
           "logical_mapping_entries_before",
           mut_scan.logical_column_mapping_.entries_.count(),
           "logical_output_exprs_before",
           mut_scan.logical_output_exprs_.count());
  if (OB_UNLIKELY(n_ids != n_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][IVF aux decode] orig access col ids / exprs count mismatch",
             K(ret),
             K(n_ids),
             K(n_exprs));
  } else if (OB_ISNULL(cg_.opt_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][IVF aux decode] opt_ctx is null", K(ret));
  } else {
    ObRawExprFactory &expr_factory = cg_.opt_ctx_->get_expr_factory();
    const ObSQLSessionInfo *session_info = nullptr;
    if (OB_NOT_NULL(cg_.opt_ctx_->get_exec_ctx())) {
      session_info = cg_.opt_ctx_->get_exec_ctx()->get_my_session();
    }
    mut_scan.logical_column_mapping_.entries_.reuse();
    mut_scan.logical_output_exprs_.destroy();
    // Resolve aux_type + col_descs once. Schema rowkey count and col_descs col_cnt
    // are NOT required to match: e.g., LT_AUX_IVF_ROWID_CID schema rowkey is
    // (rowid) only, but col_descs encodes (rowid, cid). We locate cid/rowid slot
    // ordinals in col_descs[] then route each access column by its column_flag.
    const share::ObLogicalTableAuxType aux_type = share::classify_aux_type_from_schema(aux_schema);
    const share::ObLogicalTableKeyColDesc *col_descs = nullptr;
    int64_t col_cnt = 0;
    int64_t cid_ordinal_in_col_descs = -1;
    int64_t rowid_ordinal_in_col_descs = -1;
    if (OB_FAIL(share::get_aux_type_col_descs(aux_type, col_descs, col_cnt))) {
      LOG_WARN("[LT hybrid CG][IVF aux decode] get col descs failed",
               K(ret), K(aux_type));
    } else {
      for (int64_t k = 0; k < col_cnt; ++k) {
        if (common::ObVarcharType == col_descs[k].type_
            && col_descs[k].is_order_preserving_encoded_) {
          cid_ordinal_in_col_descs = k;
        } else if (common::ObUInt64Type == col_descs[k].type_) {
          rowid_ordinal_in_col_descs = k;
        }
      }
    }
    common::ObArray<ObExpr *> tmp_logical_exprs;
    // Each col_descs slot can host at most one access column. If multiple
    // candidate access columns match the same slot type (e.g. multi-col main_pk
    // in IVF_CID_ROWID), only the first one is routed to KEY; subsequent ones
    // fall back to FROM_VALUE_PATH. Note: which uint64 rowkey is THE rowid is
    // determined by access-column traversal order; if this differs from KV
    // writer order, decoded rowid will be wrong. Current production schemas
    // have single-column main_pk so the ambiguity does not trigger.
    bool cid_slot_used = false;
    bool rowid_slot_used = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < n_ids; ++i) {
      const uint64_t cid = orig_access_col_ids.at(i);
      ObExpr *rt_expr = orig_access_exprs.at(i);
      if (common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == cid) {
        continue;
      }
      ObColumnRefRawExpr *cref = nullptr;
      if (OB_FAIL(hybrid_lt_resolve_ivf_aux_col_ref(
              op, aux_schema, cid, expr_factory, session_info, cref))) {
        LOG_WARN("[LT hybrid CG][IVF aux decode] resolve IVF aux column ref failed", K(ret), K(i), K(cid));
      } else if (OB_ISNULL(cref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid CG][IVF aux decode] column ref is null after resolve", K(ret), K(i), K(cid));
      } else {
        const common::ObString &cname = cref->get_column_name();
        if (0 == cname.case_compare("namespace_id") || 0 == cname.case_compare("ltable_id")) {
          // stripped from logical rowkey prefix; not decoded from KV JSON
          continue;
        }
        const share::schema::ObColumnSchemaV2 *col_schema = aux_schema.get_column_schema(cid);
        ObLogicalColumnSource src = FROM_VALUE_PATH;
        bool is_rowid = false;
        int64_t ordinal = -1;
        if (OB_NOT_NULL(col_schema)) {
          // Schema rowkey size and col_descs col_cnt are NOT 1:1.
          // For example:
          //   - centroid table with part_key: schema rowkey = (cid, part_key_uint64),
          //     col_descs = [cid]. The part_key column is rowkey+uint64 in schema
          //     but is NOT encoded in logical key — it lives in KV value.
          //   - cid_vector table when main_table_rowkey_size > 1: schema rowkey =
          //     (cid, main_pk_a, main_pk_b, ...), col_descs = [cid, rowid]. Only
          //     one main_pk column maps to col_descs rowid slot; extras go to value.
          // Strategy: only route to FROM_KEY_* when col_descs actually has a
          // matching slot; otherwise fall back to FROM_VALUE_PATH.
          const bool is_cid_col = col_schema->is_vec_ivf_center_id_column()
              || col_schema->is_vec_ivf_meta_id_column()
              || col_schema->is_vec_ivf_pq_center_id_column();
          const bool is_rowkey_uint64 = col_schema->is_rowkey_column()
              && common::ObUInt64Type == col_schema->get_data_type();
          if (is_cid_col && cid_ordinal_in_col_descs >= 0 && !cid_slot_used) {
            src = FROM_KEY_CID;
            ordinal = cid_ordinal_in_col_descs;
            cid_slot_used = true;
          } else if (is_rowkey_uint64 && rowid_ordinal_in_col_descs >= 0 && !rowid_slot_used) {
            src = FROM_ROWID;
            is_rowid = true;
            ordinal = rowid_ordinal_in_col_descs;
            rowid_slot_used = true;
          }
          // else: column not encoded in logical key -> FROM_VALUE_PATH (default).
          //   - is_cid_col but col_descs has no cid slot: shouldn't happen for
          //     known IVF aux types, defensive fallback.
          //   - is_rowkey_uint64 but col_descs has no uint64 slot: e.g. centroid
          //     table's part_key column.
          //   - is_rowkey_uint64 with extra main_pk columns beyond the single
          //     uint64 slot in col_descs: only the first such column is routed
          //     to FROM_ROWID; the rest fall through to FROM_VALUE_PATH.
        }
        if (OB_SUCC(ret)) {
          ObLogicalColumnMappingEntry entry;
          if (OB_FAIL(hybrid_lt_push_mapping_entry(cref, src, is_rowid,
                                                   ordinal, alloc, entry))) {
            LOG_WARN("[LT hybrid CG][IVF aux decode] push mapping entry failed",
                     K(ret), K(i), K(cid), K(cname), K(src), K(ordinal));
          } else if (OB_FAIL(mut_scan.logical_column_mapping_.entries_.push_back(entry))) {
            LOG_WARN("[LT hybrid CG][IVF aux decode] push_back mapping failed",
                     K(ret), K(i));
          } else if (OB_FAIL(tmp_logical_exprs.push_back(rt_expr))) {
            LOG_WARN("[LT hybrid CG][IVF aux decode] push_back logical expr failed",
                     K(ret), K(i));
          } else {
            LOG_TRACE("[LT hybrid CG][IVF aux decode] mapping entry built",
                     K(i), K(cid), K(cname), K(aux_type), K(src), K(ordinal),
                     K(is_rowid));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mut_scan.logical_output_exprs_.init(tmp_logical_exprs.count()))) {
        LOG_WARN("[LT hybrid CG][IVF aux decode] init logical_output_exprs failed", K(ret));
      } else if (OB_FAIL(mut_scan.logical_output_exprs_.assign(tmp_logical_exprs))) {
        LOG_WARN("[LT hybrid CG][IVF aux decode] assign logical_output_exprs failed", K(ret));
      } else {
        LOG_TRACE("[LT hybrid CG][IVF aux decode] build_ivf_aux_logical_decode_meta_ done",
                 "mapping_cnt", mut_scan.logical_column_mapping_.entries_.count(),
                 "logical_output_cnt", mut_scan.logical_output_exprs_.count());
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[LT hybrid CG][IVF aux decode] build_ivf_aux_logical_decode_meta_ exit with error",
             K(ret));
  }
  return ret;
}

void ObHybridSearchCgService::collect_scan_ctdefs_(ObDASBaseCtDef *ctdef,
                                                    common::ObIArray<ObDASScalarScanCtDef *> &scan_ctdefs)
{
  if (OB_ISNULL(ctdef)) {
    // skip
  } else if (ctdef->op_type_ == DAS_OP_SCALAR_SCAN_QUERY) {
    scan_ctdefs.push_back(static_cast<ObDASScalarScanCtDef *>(ctdef));
  } else {
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i])) {
        collect_scan_ctdefs_(ctdef->children_[i], scan_ctdefs);
      }
    }
  }
}

void ObHybridSearchCgService::collect_vec_ivf_table_scan_ctdefs_(
    ObDASBaseCtDef *ctdef,
    const bool under_ivf,
    common::ObIArray<ObDASScanCtDef *> &scan_ctdefs)
{
  if (OB_ISNULL(ctdef)) {
    // skip
  } else {
    bool child_under_ivf = under_ivf;
    if (ctdef->op_type_ == DAS_OP_VEC_INDEX_IVF_SCAN) {
      child_under_ivf = true;
    } else if (ctdef->op_type_ == DAS_OP_VEC_INDEX_HNSW_SCAN) {
      // Logical-table hybrid in this path is IVF-only; do not treat HNSW subtree as IVF aux scans.
      return;
    }
    if (child_under_ivf && ctdef->op_type_ == DAS_OP_TABLE_SCAN) {
      scan_ctdefs.push_back(static_cast<ObDASScanCtDef *>(ctdef));
    }
    for (int64_t i = 0; i < ctdef->children_cnt_; ++i) {
      if (OB_NOT_NULL(ctdef->children_[i])) {
        collect_vec_ivf_table_scan_ctdefs_(ctdef->children_[i], child_under_ivf, scan_ctdefs);
      }
    }
  }
}

int ObHybridSearchCgService::apply_logical_table_kv_access_and_table_param_(
    ObLogTableScan &op,
    ObExpr *phys_key_expr,
    ObExpr *phys_value_expr,
    UIntFixedArray &access_column_ids,
    ObPushdownExprSpec &pd_expr_spec,
    share::schema::ObTableParam &table_param,
    sql::ExprFixedArray *result_output_opt,
    const common::ObIArray<uint64_t> *extra_col_ids,
    const common::ObIArray<ObExpr *> *extra_access_exprs,
    sql::ExprFixedArray *logical_residual_filters_out,
    const bool skip_kv_value)
{
  int ret = OB_SUCCESS;
  const uint64_t phys_key_col_id = op.get_kv_key_col_id();
  const uint64_t phys_value_col_id = op.get_kv_value_col_id();
  const uint64_t physical_data_table_id = op.get_logical_physical_data_table_id();
  if (OB_UNLIKELY(OB_INVALID_ID == phys_key_col_id || OB_INVALID_ID == phys_value_col_id
                  || OB_INVALID_ID == physical_data_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid logical table metadata", K(ret), K(phys_key_col_id),
             K(phys_value_col_id), K(physical_data_table_id));
  } else if (OB_UNLIKELY((nullptr == extra_col_ids) != (nullptr == extra_access_exprs))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_col_ids and extra_access_exprs must be both null or both non-null", K(ret),
             KP(extra_col_ids), KP(extra_access_exprs));
  } else if (OB_UNLIKELY(nullptr != extra_col_ids
                         && extra_col_ids->count() != extra_access_exprs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra arrays size mismatch", K(ret),
             "ids_cnt", extra_col_ids->count(), "exprs_cnt", extra_access_exprs->count());
  } else {
    const int64_t extra_cnt = (nullptr != extra_col_ids) ? extra_col_ids->count() : 0;
    ObSEArray<uint64_t, 4> kv_col_ids;
    ObSEArray<ObExpr *, 4> kv_access_exprs;
    if (OB_FAIL(kv_col_ids.push_back(phys_key_col_id))) {
      LOG_WARN("push kv_key col id failed", K(ret));
    } else if (!skip_kv_value && OB_FAIL(kv_col_ids.push_back(phys_value_col_id))) {
      LOG_WARN("push kv_value col id failed", K(ret));
    } else if (OB_FAIL(kv_access_exprs.push_back(phys_key_expr))) {
      LOG_WARN("push kv_key expr failed", K(ret));
    } else if (!skip_kv_value && OB_FAIL(kv_access_exprs.push_back(phys_value_expr))) {
      LOG_WARN("push kv_value expr failed", K(ret));
    } else if (skip_kv_value) {
      LOG_TRACE("[LT hybrid CG] skip encoded_value binding for search index doc-id-only scan",
               K(phys_key_col_id), K(phys_value_col_id));
    }
    // Append extra (e.g. OB_HIDDEN_GROUP_IDX_COLUMN_ID) so the layout becomes
    // [kv_key, kv_value, extra...]. Both access_column_ids_ and access_exprs_ must
    // include them BEFORE the single table_param.convert() below.
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_cnt; ++i) {
      if (OB_FAIL(kv_col_ids.push_back(extra_col_ids->at(i)))) {
        LOG_WARN("push extra col id failed", K(ret), K(i));
      } else if (OB_FAIL(kv_access_exprs.push_back(extra_access_exprs->at(i)))) {
        LOG_WARN("push extra access expr failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      access_column_ids.destroy();
      pd_expr_spec.access_exprs_.destroy();
      if (OB_FAIL(access_column_ids.assign(kv_col_ids))) {
        LOG_WARN("assign kv col ids failed", K(ret));
      } else if (OB_FAIL(pd_expr_spec.access_exprs_.assign(kv_access_exprs))) {
        LOG_WARN("assign kv access exprs failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && nullptr != result_output_opt) {
      result_output_opt->destroy();
      if (OB_FAIL(result_output_opt->assign(pd_expr_spec.access_exprs_))) {
        LOG_WARN("assign result_output to kv access exprs failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
      const ObTableSchema *kv_schema = nullptr;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(physical_data_table_id, kv_schema))) {
        LOG_WARN("get kv table schema failed", K(ret), K(physical_data_table_id));
      } else if (OB_ISNULL(kv_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("kv table schema not found", K(ret), K(physical_data_table_id));
      } else if (FALSE_IT(table_param.reset())) {
      } else if (FALSE_IT(table_param.set_is_partition_table(kv_schema->is_partitioned_table()))) {
      } else if (FALSE_IT(table_param.set_is_mlog_table(kv_schema->is_mlog_table()))) {
      } else if (FALSE_IT(table_param.set_plan_enable_rich_format(
                     cg_.opt_ctx_->get_enable_rich_vector_format()))) {
      } else if (FALSE_IT(table_param.get_enable_lob_locator_v2() =
                     (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
      } else if (OB_FAIL(table_param.convert(
                     *kv_schema,
                     access_column_ids,
                     pd_expr_spec.pd_storage_flag_,
                     nullptr,
                     false,
                     false))) {
        LOG_WARN("convert table_param with kv schema failed", K(ret), K(physical_data_table_id));
      }
    }
    if (OB_SUCC(ret)) {
      // KV tables cannot evaluate logical-column filters at storage; save for DAS decode path.
      // Residual filters run after decode in DAS (ScalarScanOp / ObDASScalarROROp::fetch_and_process_storage_batch_).
      if (OB_NOT_NULL(logical_residual_filters_out)) {
        logical_residual_filters_out->destroy();
        if (!pd_expr_spec.pushdown_filters_.empty()
            && OB_FAIL(logical_residual_filters_out->assign(pd_expr_spec.pushdown_filters_))) {
          LOG_WARN("assign logical residual filters failed", K(ret));
        }
      }
      pd_expr_spec.pushdown_filters_.reset();
      pd_expr_spec.pd_storage_filters_.set_filter_tree(nullptr);
    }
  }
  return ret;
}

int ObHybridSearchCgService::prepare_hybrid_lt_rewrite_ctx_(ObLogTableScan &op,
                                                             HybridLtRewriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  ctx.kv_key_raw_ = op.get_kv_key_raw_expr();
  ctx.kv_value_raw_ = op.get_kv_value_raw_expr();
  ctx.physical_data_table_id_ = op.get_logical_physical_data_table_id();
  ctx.logical_ref_table_id_ = op.get_real_ref_table_id();
  if (OB_ISNULL(ctx.kv_key_raw_) || OB_ISNULL(ctx.kv_value_raw_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical table kv raw exprs are null", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*ctx.kv_key_raw_, ctx.phys_key_expr_))) {
    LOG_WARN("generate rt expr for kv_key failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*ctx.kv_value_raw_, ctx.phys_value_expr_))) {
    LOG_WARN("generate rt expr for kv_value failed", K(ret));
  } else if (OB_ISNULL(ctx.phys_key_expr_) || OB_ISNULL(ctx.phys_value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical kv exprs are null", K(ret));
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_storage_scalar_scans_(ObLogTableScan &op,
                                                            ObDASBaseCtDef *root_ctdef,
                                                            const HybridLtRewriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDASScalarScanCtDef *, 8> scan_ctdefs;
  collect_scan_ctdefs_(root_ctdef, scan_ctdefs);
  LOG_TRACE("[LT DEBUG] rewrite_storage_scalar_scans_ entry",
           K(scan_ctdefs.count()),
           K(ctx.physical_data_table_id_),
           K(op.get_kv_key_col_id()), K(op.get_kv_value_col_id()));
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ctdefs.count(); ++i) {
    ObDASScalarScanCtDef *scan_ctdef = scan_ctdefs.at(i);
    if (OB_ISNULL(scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan ctdef is null", K(ret), K(i));
    } else {
        const share::ObLogicalTableAuxType aux_type = scan_ctdefs.at(i)->logical_table_aux_type_;
        if (share::LT_AUX_FTS_INVERTED == aux_type || share::LT_AUX_FTS_FORWARD == aux_type) {
          // FTS aux path: build decode meta + KV rewrite with logical_column_mapping_
        if (OB_FAIL(rewrite_fts_aux_ctdef_for_logical_table_(
                       op, aux_type, *scan_ctdef))) {
          LOG_WARN("rewrite FTS aux scalar scan ctdef for logical table failed",
                   K(ret), K(i), K(aux_type));
        }
      } else {
        // PRIMARY / SEARCH_INDEX path: KV rewrite (result_output_ overwrite, no mapping).
        // aux_type / logical_index_seq_ are derived from index schema at the tail of
        // rewrite_scan_ctdef_for_logical_table_.
        if (OB_FAIL(rewrite_scan_ctdef_for_logical_table_(
                       op, *scan_ctdef, ctx.phys_key_expr_, ctx.phys_value_expr_))) {
          LOG_WARN("rewrite scan ctdef for logical table failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_storage_ivf_table_scans_(ObLogTableScan &op,
                                                               ObDASBaseCtDef *root_ctdef,
                                                               const HybridLtRewriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDASScanCtDef *, 16> ivf_aux_table_scans;
  collect_vec_ivf_table_scan_ctdefs_(root_ctdef, false, ivf_aux_table_scans);
  LOG_TRACE("[LT DEBUG] rewrite_storage_ivf_table_scans_ entry",
           K(ivf_aux_table_scans.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < ivf_aux_table_scans.count(); ++i) {
    ObDASScanCtDef *ivf_scan = ivf_aux_table_scans.at(i);
    if (OB_ISNULL(ivf_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf aux table scan ctdef is null", K(ret), K(i));
    } else if (OB_FAIL(rewrite_das_scan_ctdef_for_logical_table_ivf_aux_(
                   op, *ivf_scan, ctx.phys_key_expr_, ctx.phys_value_expr_))) {
      LOG_WARN("rewrite IVF aux DAS TABLE_SCAN ctdef for logical table failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_lookup_main_scan_(ObLogTableScan &op,
                                                        ObDASBaseCtDef *root_ctdef,
                                                        const HybridLtRewriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef is null", K(ret));
  } else if (root_ctdef->op_type_ != DAS_OP_TABLE_LOOKUP
             && root_ctdef->op_type_ != DAS_OP_INDEX_PROJ_LOOKUP) {
    // not a lookup-shaped tree, nothing to do.
  } else {
    const ObDASScanCtDef *lookup_scan =
        static_cast<const ObDASTableLookupCtDef *>(root_ctdef)->get_lookup_scan_ctdef();
    if (OB_ISNULL(lookup_scan)) {
      // no lookup scan, skip.
    } else {
      ObDASScanCtDef *mut_lookup = const_cast<ObDASScanCtDef *>(lookup_scan);
      // 幂等守卫：若该 lookup 已被逻辑表 KV 改写过（aux_type 已置 LT_AUX_PRIMARY 且
      // physical_data_table_id_ 已指向本 ctx 的 KV 表），说明本 root_ctdef 已完成 rewrite，
      // 直接跳过第二次改写。否则第二次会用逻辑表 schema 误解析已被首次改写成 KV col id
      // 的 access_column_ids_（KV 表与逻辑表 column_id 重叠：kv_key/kv_value 会被当作
      // ltable_id/document），导致 logical_output_exprs_ 与 result_output_ 错位、结果列写坏。
      if (share::LT_AUX_PRIMARY == mut_lookup->logical_table_aux_type_
          && common::OB_INVALID_ID != mut_lookup->physical_data_table_id_
          && mut_lookup->physical_data_table_id_ == ctx.physical_data_table_id_) {
        LOG_TRACE("[LT hybrid CG] lookup main scan already rewritten, skip duplicate rewrite",
                 KP(mut_lookup), K(ctx.physical_data_table_id_));
      } else {
        mut_lookup->logical_table_aux_type_ = share::LT_AUX_PRIMARY;
        mut_lookup->logical_index_seq_ = ctx.logical_ref_table_id_;
        mut_lookup->physical_data_table_id_ = ctx.physical_data_table_id_;
        ObSEArray<uint64_t, 8> saved_col_ids;
        ObSEArray<ObExpr *, 8> saved_access_exprs;
        if (OB_FAIL(saved_col_ids.assign(mut_lookup->access_column_ids_))) {
          LOG_WARN("[LT hybrid CG] save lookup access_column_ids_ failed", K(ret));
        } else if (OB_FAIL(saved_access_exprs.assign(mut_lookup->pd_expr_spec_.access_exprs_))) {
          LOG_WARN("[LT hybrid CG] save lookup access_exprs_ failed", K(ret));
        } else if (OB_FAIL(build_hybrid_lookup_logical_decode_meta_(
                       op, saved_col_ids, saved_access_exprs, *mut_lookup))) {
          LOG_WARN("[LT hybrid CG] build lookup logical decode meta failed", K(ret));
        } else {
          LOG_TRACE("[LT hybrid CG] lookup saved access snapshot before KV rewrite",
                   "saved_col_cnt", saved_col_ids.count(),
                   "saved_expr_cnt", saved_access_exprs.count(),
                   "logical_mapping_cnt", mut_lookup->logical_column_mapping_.entries_.count(),
                   "logical_output_cnt", mut_lookup->logical_output_exprs_.count());
        }
        // Extract pseudo-columns to preserve through KV rewrite (currently only
        // OB_HIDDEN_GROUP_IDX_COLUMN_ID). They are appended after [kv_key, kv_value]
        // in a single apply_* call, so table_param.convert() runs exactly once.
        ObSEArray<uint64_t, 1> extra_ids;
        ObSEArray<ObExpr *, 1> extra_exprs;
        for (int64_t gi = 0; OB_SUCC(ret) && gi < saved_col_ids.count(); ++gi) {
          if (common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == saved_col_ids.at(gi)) {
            if (OB_FAIL(extra_ids.push_back(common::OB_HIDDEN_GROUP_IDX_COLUMN_ID))) {
              LOG_WARN("[LT hybrid CG] collect group_idx col id failed", K(ret), K(gi));
            } else if (OB_FAIL(extra_exprs.push_back(saved_access_exprs.at(gi)))) {
              LOG_WARN("[LT hybrid CG] collect group_idx access expr failed", K(ret), K(gi));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(apply_logical_table_kv_access_and_table_param_(
                         op,
                         ctx.phys_key_expr_,
                         ctx.phys_value_expr_,
                         mut_lookup->access_column_ids_,
                         mut_lookup->pd_expr_spec_,
                         mut_lookup->table_param_,
                         /*result_output_opt=*/ nullptr,
                         &extra_ids,
                         &extra_exprs,
                         &mut_lookup->logical_residual_filters_))) {
            LOG_WARN("[LT hybrid CG] apply kv access for lookup main scan failed",
                     K(ret), K(ctx.physical_data_table_id_), K(extra_ids.count()));
          } else {
            LOG_TRACE("[LT DEBUG] rewrite lookup ctdef for logical table done",
                     K(mut_lookup->ref_table_id_), K(ctx.physical_data_table_id_),
                     "extra_pseudo_cnt", extra_ids.count(),
                     "table_param_table_id", mut_lookup->table_param_.get_table_id());
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::mark_kv_exprs_self_produced_(ObLogTableScan &op,
                                                           const HybridLtRewriteCtx &ctx)
{
  UNUSED(op);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.kv_key_raw_) || OB_ISNULL(ctx.kv_value_raw_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kv raw exprs are null", K(ret));
  } else if (OB_FAIL(cg_.mark_expr_self_produced(static_cast<ObRawExpr *>(ctx.kv_key_raw_)))) {
    LOG_WARN("mark kv_key expr self produced failed", K(ret));
  } else if (OB_FAIL(cg_.mark_expr_self_produced(static_cast<ObRawExpr *>(ctx.kv_value_raw_)))) {
    LOG_WARN("mark kv_value expr self produced failed", K(ret));
  }
  return ret;
}

void ObHybridSearchCgService::record_logical_scan_ns_to_plan_(const ObRawExpr *ns_const_expr)
{
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(ns_const_expr)) {
    LOG_TRACE("skip recording hybrid logical scan ns: plan or expr is null",
              KP(cg_.phy_plan_), KP(ns_const_expr));
  } else {
    int64_t param_idx = -1;
    uint64_t literal = 0;
    if (share::extract_logical_ns_const_value(ns_const_expr, param_idx, literal)) {
      int64_t recorded_param_idx = cg_.phy_plan_->get_logical_ns_param_idx();
      uint64_t recorded_literal = cg_.phy_plan_->get_logical_ns_literal();
      if (share::record_logical_ns_if_compatible(
              param_idx, literal, recorded_param_idx, recorded_literal)) {
        cg_.phy_plan_->set_logical_ns_param_idx(recorded_param_idx);
        cg_.phy_plan_->set_logical_ns_literal(recorded_literal);
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "hybrid logical scan namespace differs across table scans",
                     K(param_idx), K(literal),
                     K(recorded_param_idx), K(recorded_literal));
      }
    } else {
      LOG_TRACE("failed to extract hybrid logical scan ns constant", KPC(ns_const_expr));
    }
  }
}

int ObHybridSearchCgService::try_extract_lt_pk_rt_expr_from_scalar_filter_(
    const ObRawExpr *filter,
    ObExpr *&out_ns_expr,
    ObExpr *&out_lt_expr,
    const bool allow_record_plan_ns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter) || !filter->is_op_expr()) {
  } else {
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(filter);
    const ObItemType op_type = op_expr->get_expr_type();
    if (T_OP_EQ == op_type || T_OP_NSEQ == op_type) {
      const ObRawExpr *lhs = op_expr->get_param_expr(0);
      const ObRawExpr *rhs = op_expr->get_param_expr(1);
      if (OB_ISNULL(lhs) || OB_ISNULL(rhs) || !lhs->is_column_ref_expr()) {
      } else {
        const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(lhs);
        const bool is_ns = share::is_logical_namespace_column(col);
        const bool is_lt = share::is_logical_ltable_column(col);
        if ((is_ns || is_lt) && !share::is_logical_table_literal_or_param_expr(rhs)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "namespace_id/ltable_id predicate must be constant");
          LOG_WARN("hybrid search ns/lt term RHS must be const expr", K(ret), KPC(rhs));
        } else if (is_ns || is_lt) {
          ObExpr *rt_expr = nullptr;
          if (OB_FAIL(cg_.generate_rt_expr(*const_cast<ObRawExpr *>(rhs), rt_expr))) {
            LOG_WARN("generate rt expr for hybrid ns/lt term failed", K(ret));
          } else if (is_ns) {
            out_ns_expr = rt_expr;
            if (allow_record_plan_ns) {
              record_logical_scan_ns_to_plan_(rhs);
            }
          } else {
            out_lt_expr = rt_expr;
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::collect_logical_table_ns_lt_exprs_from_node_(
    ObHybridSearchNodeBase *node,
    ObExpr *&ns_expr,
    ObExpr *&lt_expr,
    const bool in_must_not)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY
             && is_logical_table_implicit_pk_filter_node_(node)) {
    const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(node);
    if (scalar_node->filter_.count() == 1) {
      ObExpr *tmp_ns = nullptr;
      ObExpr *tmp_lt = nullptr;
      if (OB_FAIL(try_extract_lt_pk_rt_expr_from_scalar_filter_(
              scalar_node->filter_.at(0), tmp_ns, tmp_lt, !in_must_not))) {
        LOG_WARN("extract ns/lt rt expr from hybrid scalar filter failed", K(ret));
      } else {
        if (OB_NOT_NULL(tmp_ns)) {
          ns_expr = tmp_ns;
        }
        if (OB_NOT_NULL(tmp_lt)) {
          lt_expr = tmp_lt;
        }
      }
    }
  }
  if (OB_SUCC(ret) && node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode *>(node);
    const common::ObIArray<ObIndexMergeNode *> *clause_arrays[] = {
      &bool_node->must_nodes_,
      &bool_node->should_nodes_,
      &bool_node->filter_nodes_,
      &bool_node->must_not_nodes_
    };
    static const bool is_must_not_clause[] = {false, false, false, true};
    STATIC_ASSERT(ARRAYSIZEOF(clause_arrays) == ARRAYSIZEOF(is_must_not_clause),
                  "boolean clause flags must align");
    for (int64_t a = 0; OB_SUCC(ret) && a < ARRAYSIZEOF(clause_arrays); ++a) {
      const common::ObIArray<ObIndexMergeNode *> &clause_nodes = *clause_arrays[a];
      for (int64_t i = 0; OB_SUCC(ret) && i < clause_nodes.count(); ++i) {
        ObHybridSearchNodeBase *child =
            static_cast<ObHybridSearchNodeBase *>(clause_nodes.at(i));
        if (OB_FAIL(collect_logical_table_ns_lt_exprs_from_node_(
                child, ns_expr, lt_expr, in_must_not || is_must_not_clause[a]))) {
          LOG_WARN("collect ns/lt from boolean clause failed", K(ret), K(a), K(i));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase *>(node->children_.at(i));
      if (OB_FAIL(collect_logical_table_ns_lt_exprs_from_node_(
              child, ns_expr, lt_expr, in_must_not))) {
        LOG_WARN("collect ns/lt from hybrid child failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::extract_ns_lt_from_table_filters_(
    const ObLogTableScan &op,
    ObExpr *&ns_expr,
    ObExpr *&lt_expr)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObRawExpr*> &filters = op.get_filter_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    const ObRawExpr *filter = filters.at(i);
    if (OB_ISNULL(filter)) {
    } else if (T_OP_AND == filter->get_expr_type()) {
      const ObOpRawExpr *and_expr = static_cast<const ObOpRawExpr*>(filter);
      for (int64_t j = 0; OB_SUCC(ret) && j < and_expr->get_param_count(); ++j) {
        ObExpr *tmp_ns = nullptr;
        ObExpr *tmp_lt = nullptr;
        if (OB_FAIL(try_extract_lt_pk_rt_expr_from_scalar_filter_(
                and_expr->get_param_expr(j), tmp_ns, tmp_lt, true))) {
          LOG_WARN("extract ns/lt from AND child failed", K(ret), K(j));
        } else {
          if (OB_NOT_NULL(tmp_ns)) { ns_expr = tmp_ns; }
          if (OB_NOT_NULL(tmp_lt)) { lt_expr = tmp_lt; }
        }
      }
    } else {
      ObExpr *tmp_ns = nullptr;
      ObExpr *tmp_lt = nullptr;
      if (OB_FAIL(try_extract_lt_pk_rt_expr_from_scalar_filter_(
              filter, tmp_ns, tmp_lt, true))) {
        LOG_WARN("extract ns/lt from table filter failed", K(ret), K(i));
      } else {
        if (OB_NOT_NULL(tmp_ns)) { ns_expr = tmp_ns; }
        if (OB_NOT_NULL(tmp_lt)) { lt_expr = tmp_lt; }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::inject_logical_table_ns_lt_into_main_scan_ctdef_(
    ObLogTableScan &op,
    ObDASScanCtDef &main_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObExpr *ns_expr = nullptr;
  ObExpr *lt_expr = nullptr;
  if (OB_ISNULL(op.get_access_path()) || !op.use_index_merge()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inject ns/lt expects index merge access path", K(ret));
  } else {
    const IndexMergePath *path = static_cast<const IndexMergePath *>(op.get_access_path());
    ObHybridSearchNodeBase *root = static_cast<ObHybridSearchNodeBase *>(path->root_);
    if (OB_FAIL(collect_logical_table_ns_lt_exprs_from_node_(root, ns_expr, lt_expr, false))) {
      LOG_WARN("collect ns/lt exprs from hybrid search root failed", K(ret));
    } else {
      // Fallback: for plain SQL index scans via idx_json (WHERE ns=? AND lt=? AND
      // JSON_EXTRACT(...)=?), the ns/lt predicates are table-level filters, not
      // part of the hybrid search boolean tree. Extract them from
      // op.get_filter_exprs().
      if (OB_ISNULL(ns_expr) || OB_ISNULL(lt_expr)) {
        if (OB_FAIL(extract_ns_lt_from_table_filters_(op, ns_expr, lt_expr))) {
          LOG_WARN("extract ns/lt from table filters failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(ns_expr) && OB_NOT_NULL(lt_expr)) {
        ObLogicalTableScanCtDef &lt_ctx = main_scan_ctdef.lt_ctx_;
        lt_ctx.ns_id_expr_ = ns_expr;
        lt_ctx.lt_id_start_expr_ = lt_expr;
        lt_ctx.lt_id_end_expr_ = lt_expr;
        lt_ctx.lt_start_inclusive_ = true;
        lt_ctx.lt_end_inclusive_ = true;
        lt_ctx.has_ns_pred_ = true;
        lt_ctx.has_lt_pred_ = true;
        lt_ctx.analyzed_ = true;
        LOG_TRACE("[LT hybrid CG] injected ns/lt exprs into main scan ctdef",
                 KP(ns_expr), KP(lt_expr));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::inject_logical_table_ns_lt_into_fts_inv_scan_ctdef(
    ObLogTableScan &op,
    ObDASScanCtDef &inv_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObExpr *ns_expr = nullptr;
  ObExpr *lt_expr = nullptr;
  if (OB_FAIL(extract_ns_lt_from_table_filters_(op, ns_expr, lt_expr))) {
    LOG_WARN("[LT plain MATCH] extract ns/lt from table filters failed", K(ret));
  } else if (OB_ISNULL(ns_expr) || OB_ISNULL(lt_expr)) {
    // Leave lt_ctx_ empty on purpose: ObDASScanOp::init_scan_param then raises the
    // user-facing "logical table query must carry namespace_id and ltable_id
    // predicates" error, which is the correct diagnosis for such a statement.
    LOG_TRACE("[LT plain MATCH] ns/lt predicates not found in table filters",
              KP(ns_expr), KP(lt_expr));
  } else {
    ObLogicalTableScanCtDef &lt_ctx = inv_scan_ctdef.lt_ctx_;
    lt_ctx.ns_id_expr_ = ns_expr;
    lt_ctx.lt_id_start_expr_ = lt_expr;
    lt_ctx.lt_id_end_expr_ = lt_expr;
    lt_ctx.lt_start_inclusive_ = true;
    lt_ctx.lt_end_inclusive_ = true;
    lt_ctx.has_ns_pred_ = true;
    lt_ctx.has_lt_pred_ = true;
    // analyzed_ is intentionally NOT set, see the declaration comment: the FTS aux scan
    // must keep its token ranges and go through the runtime KV range rewrite.
    LOG_TRACE("[LT plain MATCH] injected ns/lt exprs into fts inv scan ctdef",
              KP(ns_expr), KP(lt_expr));
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_for_logical_table_(ObLogTableScan &op,
                                                         ObDASBaseCtDef *root_ctdef,
                                                         ObDASScanCtDef &main_scan_ctdef)
{
  int ret = OB_SUCCESS;
  HybridLtRewriteCtx ctx;
  if (OB_ISNULL(root_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef is null", K(ret));
  } else if (OB_FAIL(inject_logical_table_ns_lt_into_main_scan_ctdef_(op, main_scan_ctdef))) {
    LOG_WARN("inject ns/lt into main scan ctdef failed", K(ret));
  } else if (OB_FAIL(prepare_hybrid_lt_rewrite_ctx_(op, ctx))) {
    LOG_WARN("prepare lt rewrite ctx failed", K(ret));
  } else if (OB_FAIL(rewrite_storage_scalar_scans_(op, root_ctdef, ctx))) {
    LOG_WARN("rewrite storage scalar scans failed", K(ret));
  } else if (OB_FAIL(rewrite_storage_ivf_table_scans_(op, root_ctdef, ctx))) {
    LOG_WARN("rewrite storage ivf table scans failed", K(ret));
  } else if (OB_FAIL(rewrite_lookup_main_scan_(op, root_ctdef, ctx))) {
    LOG_WARN("rewrite lookup main scan failed", K(ret));
  } else if (OB_FAIL(mark_kv_exprs_self_produced_(op, ctx))) {
    LOG_WARN("mark kv exprs self produced failed", K(ret));
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_scan_ctdef_for_logical_table_(
    ObLogTableScan &op,
    ObDASScalarScanCtDef &scan_ctdef,
    ObExpr *phys_key_expr,
    ObExpr *phys_value_expr)
{
  int ret = OB_SUCCESS;
  const uint64_t physical_data_table_id = op.get_logical_physical_data_table_id();
  const bool need_decode_meta = !scan_ctdef.pd_expr_spec_.pushdown_filters_.empty();
  // The encoded_value of SEARCH_INDEX is always NULL; when there is no pushdown/residual filtering (need_decode_meta
  // is false, equivalent to logical_output/residual being empty, only requiring rowid), only bind encoded_key, skip
  // encoded_value, and reduce the storage read volume per row. This gate must match the value requirement of the
  // primary-get probe (if residual is empty, the probe also takes the fill_logical_rowid_exprs fast path, only
  // requiring key).
  const bool skip_kv_value =
      (share::LT_AUX_SEARCH_INDEX == scan_ctdef.logical_table_aux_type_) && !need_decode_meta;
  ObSEArray<uint64_t, 8> saved_col_ids;
  ObSEArray<ObExpr *, 8> saved_access_exprs;
  if (need_decode_meta) {
    if (OB_FAIL(saved_col_ids.assign(scan_ctdef.access_column_ids_))) {
      LOG_WARN("[LT hybrid CG] save scalar access_column_ids_ failed", K(ret));
    } else if (OB_FAIL(saved_access_exprs.assign(scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("[LT hybrid CG] save scalar access_exprs_ failed", K(ret));
    } else if (OB_FAIL(build_hybrid_logical_decode_meta_(op,
                                                         scan_ctdef.ref_table_id_,
                                                         saved_col_ids,
                                                         saved_access_exprs,
                                                         scan_ctdef.logical_column_mapping_,
                                                         scan_ctdef.logical_output_exprs_))) {
      LOG_WARN("[LT hybrid CG] build scalar logical decode meta failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(apply_logical_table_kv_access_and_table_param_(op,
                                                               phys_key_expr,
                                                               phys_value_expr,
                                                               scan_ctdef.access_column_ids_,
                                                               scan_ctdef.pd_expr_spec_,
                                                               scan_ctdef.table_param_,
                                                               &scan_ctdef.result_output_,
                                                               nullptr,
                                                               nullptr,
                                                               &scan_ctdef.logical_residual_filters_,
                                                               skip_kv_value))) {
    LOG_WARN("apply logical table kv rewrite for scalar scan failed", K(ret), K(skip_kv_value));
  } else {
    scan_ctdef.physical_data_table_id_ = physical_data_table_id;
    // scan_ctdef.logical_index_seq_ and logical_table_aux_type_ has set in generate_scalar_scan_ctdef
    LOG_TRACE("[LT DEBUG] rewrite_scan_ctdef done",
             K(scan_ctdef.ref_table_id_), K(physical_data_table_id),
             "aux_type", static_cast<int>(scan_ctdef.logical_table_aux_type_),
             K(op.get_kv_key_col_id()), K(op.get_kv_value_col_id()),
             "table_param_table_id", scan_ctdef.table_param_.get_table_id());
  }
  return ret;
}

int ObHybridSearchCgService::rewrite_das_scan_ctdef_for_logical_table_ivf_aux_(
    ObLogTableScan &op,
    ObDASScanCtDef &scan_ctdef,
    ObExpr *phys_key_expr,
    ObExpr *phys_value_expr)
{
  int ret = OB_SUCCESS;
  const uint64_t physical_data_table_id = op.get_logical_physical_data_table_id();
  const uint64_t logical_aux_tid = scan_ctdef.ref_table_id_;
  share::ObLogicalTableAuxType aux_type = share::LT_AUX_MAX_TYPE;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  const ObTableSchema *aux_schema = nullptr;
  LOG_TRACE("[LT hybrid CG][IVF aux rewrite] rewrite_das_scan_ctdef_for_logical_table_ivf_aux_ enter",
           K(ret),
           K(logical_aux_tid),
           K(physical_data_table_id),
           KPC(&scan_ctdef),
           "ir_scan_type", static_cast<int>(scan_ctdef.ir_scan_type_),
           "access_col_cnt_before", scan_ctdef.access_column_ids_.count(),
           "access_expr_cnt_before", scan_ctdef.pd_expr_spec_.access_exprs_.count(),
           KPC(phys_key_expr),
           KPC(phys_value_expr));
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][IVF aux rewrite] schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(logical_aux_tid, aux_schema))) {
    LOG_WARN("[LT hybrid CG][IVF aux rewrite] get IVF aux table schema failed", K(ret), K(logical_aux_tid));
  } else if (OB_ISNULL(aux_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][IVF aux rewrite] IVF aux table schema is null", K(ret), K(logical_aux_tid));
  }

  ObSEArray<uint64_t, 16> saved_access_col_ids;
  ObSEArray<ObExpr *, 16> saved_access_exprs;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(saved_access_col_ids.assign(scan_ctdef.access_column_ids_))) {
      LOG_WARN("[LT hybrid CG][IVF aux rewrite] snapshot access_column_ids_ failed", K(ret));
    } else if (OB_FAIL(saved_access_exprs.assign(scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("[LT hybrid CG][IVF aux rewrite] snapshot access_exprs_ failed", K(ret));
    } else {
      LOG_TRACE("[LT hybrid CG][IVF aux rewrite] snapshot access before KV rewrite",
               K(saved_access_col_ids.count()),
               K(saved_access_exprs.count()),
               K(logical_aux_tid));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_ivf_aux_logical_decode_meta_(op,
                                                     *aux_schema,
                                                     saved_access_col_ids,
                                                     saved_access_exprs,
                                                     scan_ctdef))) {
      LOG_WARN("[LT hybrid CG][IVF aux rewrite] build_ivf_aux_logical_decode_meta_ failed", K(ret));
    } else {
      LOG_TRACE("[LT hybrid CG][IVF aux rewrite] build_ivf_aux_logical_decode_meta_ finished",
               K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(apply_logical_table_kv_access_and_table_param_(op,
                                                                 phys_key_expr,
                                                                 phys_value_expr,
                                                                 scan_ctdef.access_column_ids_,
                                                                 scan_ctdef.pd_expr_spec_,
                                                                 scan_ctdef.table_param_,
                                                                 nullptr,
                                                                 nullptr,
                                                                 nullptr,
                                                                 &scan_ctdef.logical_residual_filters_))) {
      LOG_WARN("[LT hybrid CG][IVF aux rewrite] apply logical table kv rewrite for IVF aux TABLE_SCAN failed",
               K(ret));
    } else {
      LOG_TRACE("[LT hybrid CG][IVF aux rewrite] apply_logical_table_kv_access_and_table_param_ ok",
               "access_col_cnt_after", scan_ctdef.access_column_ids_.count(),
               "access_expr_cnt_after", scan_ctdef.pd_expr_spec_.access_exprs_.count(),
               "table_param_table_id", scan_ctdef.table_param_.get_table_id(),
               "result_output_cnt_after", scan_ctdef.result_output_.count(),
               "is_cid_vec_scan", ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN == scan_ctdef.ir_scan_type_);
    }
  }
  if (OB_SUCC(ret)) {
    aux_type = share::classify_aux_type_from_schema(*aux_schema);
    scan_ctdef.logical_index_seq_ = logical_aux_tid;
    scan_ctdef.logical_table_aux_type_ = aux_type;
    scan_ctdef.physical_data_table_id_ = physical_data_table_id;
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[LT hybrid CG][IVF aux rewrite] rewrite_das_scan_ctdef_for_logical_table_ivf_aux_ done",
             "logical_aux_tid", logical_aux_tid,
             K(physical_data_table_id),
             "ref_table_id_after", scan_ctdef.ref_table_id_,
             "physical_data_table_id_after", scan_ctdef.physical_data_table_id_,
             "aux_type", static_cast<int>(aux_type),
             "ir_scan_type", static_cast<int>(scan_ctdef.ir_scan_type_),
             "table_param_table_id", scan_ctdef.table_param_.get_table_id(),
             "logical_mapping_entries", scan_ctdef.logical_column_mapping_.entries_.count(),
             "logical_output_exprs", scan_ctdef.logical_output_exprs_.count());
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[LT hybrid CG][IVF aux rewrite] rewrite_das_scan_ctdef_for_logical_table_ivf_aux_ exit with error",
             K(ret),
             "logical_aux_tid", logical_aux_tid);
  }
  return ret;
}

template <class CtDefT>
int ObHybridSearchCgService::build_fts_aux_logical_decode_meta_(
    ObLogTableScan &op,
    const share::schema::ObTableSchema &aux_schema,
    share::ObLogicalTableAuxType aux_type,
    const common::ObIArray<ObExpr *> &access_exprs,
    const common::ObIArray<uint64_t> &access_col_ids,
    CtDefT &mut_scan,
    common::ObIArray<ObExpr *> &out_logical_exprs)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &alloc = cg_.phy_plan_->get_allocator();
  mut_scan.logical_column_mapping_.entries_.reuse();
  mut_scan.logical_output_exprs_.destroy();

  // FTS forward col_descs: [0]=rowid (uint64), [1]=token (varchar)
  // FTS inverted col_descs: [0]=token (varchar), [1]=rowid (uint64)
  const int64_t doc_id_ordinal = (share::LT_AUX_FTS_FORWARD == aux_type) ? 0 : 1;
  const int64_t word_ordinal   = (share::LT_AUX_FTS_FORWARD == aux_type) ? 1 : 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < access_col_ids.count(); ++i) {
    const uint64_t cid = access_col_ids.at(i);
    const ObColumnSchemaV2 *col = aux_schema.get_column_schema(cid);
    if (OB_ISNULL(col)) {
      // Skip columns not found in schema (e.g. virtual columns)
      continue;
    }
    const ObString &cname = col->get_column_name_str();
    ObLogicalColumnMappingEntry entry;
    entry.source_idx_         = i;
    entry.logical_column_id_  = cid;
    entry.obj_meta_           = col->get_meta_type();
    if (OB_FAIL(ob_write_string(alloc, cname, entry.column_name_))) {
      LOG_WARN("deep copy column_name failed", K(ret), K(i));
    }

    const bool is_logical_rowid = share::is_logical_rowid_column_name(cname);
    const bool is_doc_id = col->is_doc_id_column();
    const bool is_word   = col->is_word_segment_column();

    if (is_doc_id || is_logical_rowid) {
      entry.source_              = FROM_ROWID;
      entry.is_rowid_            = true;
      entry.key_suffix_ordinal_  = doc_id_ordinal;
    } else if (is_word) {
      entry.source_              = FROM_KEY_SUFFIX;
      entry.is_rowid_            = false;
      entry.key_suffix_ordinal_  = word_ordinal;
    } else {
      entry.source_       = FROM_VALUE_PATH;
      entry.is_rowid_     = false;
      // Build json_path: "$.<schema_column_name>"
      const int64_t path_len = cname.length() + 2;  // "$." prefix
      char *buf = static_cast<char *>(alloc.alloc(path_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[LT hybrid CG][FTS aux decode] alloc json path buffer failed", K(ret), K(path_len));
        break;
      }
      buf[0] = '$'; buf[1] = '.';
      MEMCPY(buf + 2, cname.ptr(), cname.length());
      entry.json_path_.assign_ptr(buf, path_len);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mut_scan.logical_column_mapping_.entries_.push_back(entry))) {
      LOG_WARN("[LT hybrid CG][FTS aux decode] push mapping entry failed", K(ret), K(i));
    } else if (OB_FAIL(out_logical_exprs.push_back(access_exprs.at(i)))) {
      LOG_WARN("[LT hybrid CG][FTS aux decode] push logical expr failed", K(ret), K(i));
    }
  }

  // Assign collected exprs to ctdef.logical_output_exprs_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mut_scan.logical_output_exprs_.init(out_logical_exprs.count()))) {
      LOG_WARN("[LT hybrid CG][FTS aux decode] init logical_output_exprs failed", K(ret));
    } else if (OB_FAIL(mut_scan.logical_output_exprs_.assign(out_logical_exprs))) {
      LOG_WARN("[LT hybrid CG][FTS aux decode] assign logical_output_exprs failed", K(ret));
    } else {
      LOG_TRACE("[LT hybrid CG][FTS aux decode] build_fts_aux_logical_decode_meta_ done",
               "aux_type", static_cast<int>(aux_type),
               "mapping_cnt", mut_scan.logical_column_mapping_.entries_.count(),
               "logical_output_cnt", mut_scan.logical_output_exprs_.count());
    }
  }
  return ret;
}

template <class CtDefT>
int ObHybridSearchCgService::rewrite_fts_aux_ctdef_for_logical_table_(
    ObLogTableScan &op,
    share::ObLogicalTableAuxType aux_type,
    CtDefT &scan_ctdef)
{
  int ret = OB_SUCCESS;
  const uint64_t physical_data_table_id = op.get_logical_physical_data_table_id();
  const uint64_t logical_aux_tid = scan_ctdef.ref_table_id_;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  const ObTableSchema *aux_schema = nullptr;

  LOG_TRACE("[LT hybrid CG][FTS aux rewrite] rewrite_scalar_scan_ctdef_for_logical_table_fts_aux_ enter",
           K(logical_aux_tid), K(physical_data_table_id),
           "aux_type", static_cast<int>(aux_type),
           "access_col_cnt_before", scan_ctdef.access_column_ids_.count(),
           "access_expr_cnt_before", scan_ctdef.pd_expr_spec_.access_exprs_.count());

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][FTS aux rewrite] schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(logical_aux_tid, aux_schema))) {
    LOG_WARN("[LT hybrid CG][FTS aux rewrite] get FTS aux table schema failed", K(ret), K(logical_aux_tid));
  } else if (OB_ISNULL(aux_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT hybrid CG][FTS aux rewrite] FTS aux table schema is null", K(ret), K(logical_aux_tid));
  }

  // Step 1: Snapshot original access columns and expressions before KV rewrite
  ObSEArray<uint64_t, 16> saved_access_col_ids;
  ObSEArray<ObExpr *, 16> saved_access_exprs;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(saved_access_col_ids.assign(scan_ctdef.access_column_ids_))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] snapshot access_column_ids_ failed", K(ret));
    } else if (OB_FAIL(saved_access_exprs.assign(scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] snapshot access_exprs_ failed", K(ret));
    }
  }

  // Step 2: Build FTS aux logical decode metadata from original access columns
  ObSEArray<ObExpr *, 16> tmp_logical_exprs;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_fts_aux_logical_decode_meta_(op,
                                                     *aux_schema,
                                                     aux_type,
                                                     saved_access_exprs,
                                                     saved_access_col_ids,
                                                     scan_ctdef,
                                                     tmp_logical_exprs))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] build_fts_aux_logical_decode_meta_ failed", K(ret));
    }
  }

  // Step 3: Apply KV access rewrite (overwrite result_output_ with [kv_key, kv_value])
  // FTS path: result_output_ IS overwritten (no subscript-based direct reads in FTS)
  if (OB_SUCC(ret)) {
    ObExpr *phys_key_expr = nullptr;
    ObExpr *phys_value_expr = nullptr;
    ObColumnRefRawExpr *kv_key_raw = op.get_kv_key_raw_expr();
    ObColumnRefRawExpr *kv_value_raw = op.get_kv_value_raw_expr();
    if (OB_ISNULL(kv_key_raw) || OB_ISNULL(kv_value_raw)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] kv raw exprs are null", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*kv_key_raw, phys_key_expr))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] generate kv_key rt expr failed", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*kv_value_raw, phys_value_expr))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] generate kv_value rt expr failed", K(ret));
    } else if (OB_FAIL(apply_logical_table_kv_access_and_table_param_(op,
                                                                        phys_key_expr,
                                                                        phys_value_expr,
                                                                        scan_ctdef.access_column_ids_,
                                                                        scan_ctdef.pd_expr_spec_,
                                                                        scan_ctdef.table_param_,
                                                                        &scan_ctdef.result_output_,
                                                                        nullptr,
                                                                        nullptr,
                                                                        &scan_ctdef.logical_residual_filters_))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] apply KV rewrite for FTS aux scalar scan failed", K(ret));
    } else if (OB_FAIL(scan_ctdef.table_param_.fill_fts_meta_from_schema(*aux_schema))) {
      LOG_WARN("[LT hybrid CG][FTS aux rewrite] fill FTS meta into table_param failed",
               K(ret), K(logical_aux_tid));
    }
  }

  // Step 4: Set aux type and physical data table ID
  if (OB_SUCC(ret)) {
    scan_ctdef.logical_table_aux_type_ = aux_type;
    scan_ctdef.logical_index_seq_ = scan_ctdef.ref_table_id_;
    scan_ctdef.physical_data_table_id_ = physical_data_table_id;
    // NOTE: keep ref_table_id_ as the logical aux tid (aligned with IVF aux path).
    // Inverted-index token paths read ref_table_id_ to locate aux schema at runtime.
    LOG_INFO("[LT hybrid CG][FTS aux rewrite] rewrite_scalar_scan_ctdef_for_logical_table_fts_aux_ done",
             K(logical_aux_tid), K(physical_data_table_id),
             "ref_table_id_after", scan_ctdef.ref_table_id_,
             "aux_type", static_cast<int>(aux_type),
             "table_param_table_id", scan_ctdef.table_param_.get_table_id(),
             "mapping_entries", scan_ctdef.logical_column_mapping_.entries_.count(),
             "logical_output_cnt", scan_ctdef.logical_output_exprs_.count());
  }
  return ret;
}

// Explicit instantiations: both ObDASScalarScanCtDef (hybrid) and ObDASScanCtDef (plain MATCH IR sub-tree)
// share the same set of fields used by the helpers; the template body is identical for both.
template int ObHybridSearchCgService::build_fts_aux_logical_decode_meta_<ObDASScalarScanCtDef>(
    ObLogTableScan &, const share::schema::ObTableSchema &, share::ObLogicalTableAuxType,
    const common::ObIArray<ObExpr *> &, const common::ObIArray<uint64_t> &,
    ObDASScalarScanCtDef &, common::ObIArray<ObExpr *> &);
template int ObHybridSearchCgService::build_fts_aux_logical_decode_meta_<ObDASScanCtDef>(
    ObLogTableScan &, const share::schema::ObTableSchema &, share::ObLogicalTableAuxType,
    const common::ObIArray<ObExpr *> &, const common::ObIArray<uint64_t> &,
    ObDASScanCtDef &, common::ObIArray<ObExpr *> &);
template int ObHybridSearchCgService::rewrite_fts_aux_ctdef_for_logical_table_<ObDASScalarScanCtDef>(
    ObLogTableScan &, share::ObLogicalTableAuxType, ObDASScalarScanCtDef &);
template int ObHybridSearchCgService::rewrite_fts_aux_ctdef_for_logical_table_<ObDASScanCtDef>(
    ObLogTableScan &, share::ObLogicalTableAuxType, ObDASScanCtDef &);
template int ObHybridSearchCgService::build_scalar_primary_logical_decode_meta_<ObDASScalarScanCtDef>(
    ObLogTableScan &, const common::ObIArray<uint64_t> &,
    const common::ObIArray<ObExpr *> &, ObDASScalarScanCtDef &);
template int ObHybridSearchCgService::build_scalar_primary_logical_decode_meta_<ObDASScanCtDef>(
    ObLogTableScan &, const common::ObIArray<uint64_t> &,
    const common::ObIArray<ObExpr *> &, ObDASScanCtDef &);

int ObHybridSearchCgService::rewrite_text_ir_ctdef_for_logical_table(
    ObLogTableScan &op,
    ObDASIRScanCtDef &ir_ctdef,
    const HybridLtRewriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  // Rewrite each FTS aux ctdef directly via ObDASIRScanCtDef accessors. The IR sub-tree
  // holds ObDASScanCtDef (not ObDASScalarScanCtDef), so we bypass collect_scan_ctdefs_.
  // doc_id_idx_agg is intentionally NOT rewritten here: on logic table FTS, the
  // op.need_skip_rowkey_doc() flag is expected to be set by plan stage (no separate
  // physical doc_id_idx aux), and its execution should be skipped at IR runtime.
  ObDASScanCtDef *const inv_scan = const_cast<ObDASScanCtDef *>(ir_ctdef.get_inv_idx_scan_ctdef());
  ObDASScanCtDef *const inv_agg  = const_cast<ObDASScanCtDef *>(ir_ctdef.get_inv_idx_agg_ctdef());
  ObDASScanCtDef *const fwd_agg  = const_cast<ObDASScanCtDef *>(ir_ctdef.get_fwd_idx_agg_ctdef());

  if (OB_NOT_NULL(inv_scan)
      && OB_FAIL(rewrite_fts_aux_ctdef_for_logical_table_(op, share::LT_AUX_FTS_INVERTED, *inv_scan))) {
    LOG_WARN("[LT plain MATCH] rewrite inv_idx_scan ctdef for logical table failed", K(ret));
  } else if (OB_NOT_NULL(inv_agg)
             && OB_FAIL(rewrite_fts_aux_ctdef_for_logical_table_(op, share::LT_AUX_FTS_INVERTED, *inv_agg))) {
    LOG_WARN("[LT plain MATCH] rewrite inv_idx_agg ctdef for logical table failed", K(ret));
  } else if (OB_NOT_NULL(fwd_agg)
             && OB_FAIL(rewrite_fts_aux_ctdef_for_logical_table_(op, share::LT_AUX_FTS_FORWARD, *fwd_agg))) {
    LOG_WARN("[LT plain MATCH] rewrite fwd_idx_agg ctdef for logical table failed", K(ret));
  }
  if (OB_SUCC(ret) && ir_ctdef.has_stats_primary_scan_) {
    ObDASScanCtDef *stats_primary =
        const_cast<ObDASScanCtDef *>(ir_ctdef.get_stats_primary_scan_ctdef());
    if (OB_ISNULL(stats_primary)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT plain MATCH] stats primary ctdef is null", K(ret));
    } else if (OB_FAIL(apply_logical_table_kv_access_and_table_param_(
                   op,
                   ctx.phys_key_expr_,
                   ctx.phys_value_expr_,
                   stats_primary->access_column_ids_,
                   stats_primary->pd_expr_spec_,
                   stats_primary->table_param_,
                   &stats_primary->result_output_,
                   nullptr,
                   nullptr,
                   &stats_primary->logical_residual_filters_))) {
      LOG_WARN("[LT plain MATCH] apply KV rewrite for stats primary scan failed", K(ret));
    } else {
      stats_primary->physical_data_table_id_ = ctx.physical_data_table_id_;
    }
  }
  return ret;
}

int ObHybridSearchCgService::prepare_main_scan_access_ctdef(ObLogTableScan &op,
                                                            ObDASScanCtDef &scan_ctdef,
                                                            ObIArray<ObRawExpr *> &non_pushdown_filters,
                                                            ObIArray<ObRawExpr *> &lookup_pushdown_filters)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = nullptr;
  common::ObTableID ref_table_id = op.get_real_ref_table_id();
  ObArray<ObRawExpr *> filters;
  ObArray<ObRawExpr *> access_exprs;
  const bool is_dsl_query = is_dsl_query_path(op.get_access_path());
  scan_ctdef.ref_table_id_ = ref_table_id;
  if (OB_ISNULL(cg_.opt_ctx_) || OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.opt_ctx_), K(schema_guard));
  } else if (is_dsl_query) {
    if (!op.get_filter_exprs().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dsl query should not have additional filters", K(ret), K(ref_table_id));
    }
  } else if (OB_FAIL(filters.assign(op.get_filter_exprs()))) {
    LOG_WARN("failed to assign filters", K(ret));
  } else if (OB_FAIL(op.extract_nonpushdown_filters(
                 filters, non_pushdown_filters, lookup_pushdown_filters))) {
    LOG_WARN("failed to extract pushdown filters", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObArray<ObRawExpr *> filter_columns;
    if (OB_FAIL(access_exprs.assign(op.get_access_exprs()))) {
      LOG_WARN("failed to assign access exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filters, filter_columns))) {
      LOG_WARN("failed to extract filter columns", K(ret));
    } else if (OB_FAIL(append_array_no_dup(access_exprs, filter_columns))) {
      LOG_WARN("failed to append filter columns", K(ret));
    } else if (OB_NOT_NULL(op.get_auto_split_filter())) {
      ObArray<ObRawExpr *> auto_split_filter_columns;
      ObRawExpr *auto_split_expr = const_cast<ObRawExpr *>(op.get_auto_split_filter());
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(auto_split_expr,
                                                       auto_split_filter_columns))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, auto_split_filter_columns))) {
        LOG_WARN("append filter column to access exprs failed", K(ret));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(ObTscCgService::remove_virtual_generated_access_exprs(op, cg_, access_exprs))) {
      LOG_WARN("failed to remove virtual generated access exprs", K(ret));
    }
    LOG_TRACE("main scan access exprs", K(ret), K(ref_table_id), K(access_exprs));
  }

  if (OB_SUCC(ret)) {
    ObArray<uint64_t> access_column_ids;
    if (OB_FAIL(extract_column_ids(access_exprs, access_column_ids))) {
      LOG_WARN("failed to extract access column ids", K(ret));
    } else if (OB_FAIL(check_column_ids_accessible(
                   ref_table_id, *schema_guard, access_column_ids))) {
      LOG_WARN("column ids are not accessible", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(
                   access_exprs, scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("failed to generate access exprs", K(ret));
    } else if (OB_FAIL(cg_.mark_expr_self_produced(access_exprs))) {
      LOG_WARN("mark expr self produced failed", K(ret), K(access_exprs));
    } else if (OB_FAIL(scan_ctdef.access_column_ids_.assign(access_column_ids))) {
      LOG_WARN("assign output column ids failed", K(ret));
    } else {
      LOG_TRACE("main scan access column ids", K(ret), K(ref_table_id), K(access_column_ids));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
