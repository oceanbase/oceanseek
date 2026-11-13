/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "lib/container/ob_heap.h"
#include "ob_das_boolean_query.h"
#include "ob_das_search_define.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_disjunction_filter_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_bitmap_op.h"
#include "ob_das_bmw_op.h"
#include "ob_das_async_index_query_ctx.h"
#include "ob_das_async_index_deletion_op.h"
#include "ob_das_search_utils.h"
#include "ob_das_scalar_define.h"
#include "ob_das_match_query.h"
#include "ob_das_multi_match_query.h"
#include "ob_das_query_string_query.h"
#include "sql/das/ob_das_ir_define.h"
#include "lib/utility/ob_sort.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"

namespace oceanbase
{
namespace sql
{
bool ObDASBooleanQueryRtDef::prefer_streaming_ror_filter(
    int64_t match_rows, int64_t filter_rows, int64_t table_rows,
    int64_t consumers, int64_t topk)
{
  bool prefer = false;
  // Compare one bitmap scan with independent candidate-driven cursors. These
  // conservative work estimates are not output limits or pruning bounds.
  static constexpr double SEEK_TO_SCAN_RATIO = 64.0;
  if (match_rows > 0 && filter_rows > 0 && table_rows > 0 && filter_rows <= table_rows
      && consumers > 1 && topk > 0 && topk <= 1000) {
    const double selectivity = static_cast<double>(filter_rows) / table_rows;
    const double per_consumer = std::max(128.0, 32.0 * topk) / selectivity;
    const double candidates = std::min(static_cast<double>(match_rows), per_consumer * consumers);
    const double streaming_cost = (candidates + 16.0 * consumers) * SEEK_TO_SCAN_RATIO;
    prefer = streaming_cost < static_cast<double>(filter_rows);
  }
  return prefer;
}
ERRSIM_POINT_DEF(DISABLE_BMM_FILTER_PUSHDOWN, "disable bmm filter pushdown");

namespace
{
constexpr int64_t TOPK_PROBE_MAX_LIMIT = 1000;
constexpr int64_t TOPK_PROBE_MIN_CANDIDATES = 128;
constexpr int64_t TOPK_PROBE_CANDIDATES_PER_HIT = 32;
constexpr int64_t TOPK_PROBE_REFERENCE_GET_TO_SCAN_RATIO = 256;
constexpr int64_t TOPK_PROBE_REFERENCE_OPTION_RATIO = 1000;

class ObDASTopKProbeCostGuard
{
public:
  ObDASTopKProbeCostGuard(ObDASSearchCtx &search_ctx, const bool disable)
      : search_ctx_(search_ctx), previous_limit_(search_ctx.get_generating_topk_limit())
  {
    if (disable) {
      search_ctx_.set_generating_topk_limit(-1);
    }
  }
  ~ObDASTopKProbeCostGuard()
  {
    search_ctx_.set_generating_topk_limit(previous_limit_);
  }

private:
  ObDASSearchCtx &search_ctx_;
  int64_t previous_limit_;
  DISALLOW_COPY_AND_ASSIGN(ObDASTopKProbeCostGuard);
};

class ObDASGroupFilterDomainGuard
{
public:
  explicit ObDASGroupFilterDomainGuard(
      ObDASSearchCtx &search_ctx, const bool enable_filter_domain = true)
      : search_ctx_(search_ctx),
        previous_(search_ctx.is_generating_group_filter_domain())
  {
    if (enable_filter_domain) {
      search_ctx_.set_generating_group_filter_domain(true);
    }
  }
  ~ObDASGroupFilterDomainGuard()
  {
    search_ctx_.set_generating_group_filter_domain(previous_);
  }

private:
  ObDASSearchCtx &search_ctx_;
  bool previous_;
  DISALLOW_COPY_AND_ASSIGN(ObDASGroupFilterDomainGuard);
};

// Classify complete top-level Filter clauses without rewriting their Boolean semantics.
// Only success with is_splittable=true makes both output arrays a complete partition.
// Success with is_splittable=false is a supported fallback; the arrays may contain
// a classified prefix and must not be used as a valid partition of the Filter.
int split_group_filter_clauses(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObIArray<ObIDASSearchRtDef *> &sync_clauses,
    ObIArray<ObIDASSearchRtDef *> &async_clauses,
    bool &is_splittable)
{
  int ret = OB_SUCCESS;
  is_splittable = true;
  sync_clauses.reset();
  async_clauses.reset();
  for (int64_t i = 0; OB_SUCC(ret) && is_splittable && i < filter_clauses.count(); ++i) {
    ObIDASSearchRtDef *clause = nullptr;
    const ObDASBaseCtDef *clause_ctdef = nullptr;
    ObDASGroupFilterDomainAnalysis analysis;
    if (OB_FAIL(filter_clauses.get(i, clause))) {
      LOG_WARN("failed to get Group filter clause for scope classification", K(ret), K(i));
    } else if (OB_ISNULL(clause)
               || OB_ISNULL(clause_ctdef = static_cast<const ObDASBaseCtDef *>(clause->ctdef_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null Group filter clause definition", K(ret), K(i), KP(clause));
    } else if (OB_FAIL(ObDASAsyncIndexQueryCtx::analyze_group_filter_domain(
                   clause_ctdef, analysis))) {
      LOG_WARN("failed to classify Group filter clause", K(ret), K(i));
    } else if (!analysis.is_compatible_
               || analysis.has_sync_filter_ == analysis.has_async_filter_) {
      // A top-level clause containing both visibility domains cannot be split
      // without rewriting its internal Boolean semantics. Keep the safe
      // Group-local/direct fallback for that shape.
      is_splittable = false;
    } else if (analysis.has_sync_filter_) {
      if (OB_FAIL(sync_clauses.push_back(clause))) {
        LOG_WARN("failed to append query-scope sync filter clause", K(ret), K(i));
      }
    } else if (OB_FAIL(async_clauses.push_back(clause))) {
      LOG_WARN("failed to append Group-scope async filter clause", K(ret), K(i));
    }
  }
  return ret;
}
}

OB_SERIALIZE_MEMBER((ObDASBooleanQueryCtDef, ObIDASSearchCtDef),
                    must_,
                    filter_,
                    should_,
                    must_not_,
                    min_should_match_,
                    is_async_,
                    min_should_match_expr_,
                    is_msm_unresolved_expr_,
                    merged_probe_,
                    primary_get_ratio_);

OB_SERIALIZE_MEMBER(ObDASBooleanQueryCtDef::SubClauseInfo,
                    exist_,
                    offset_,
                    count_);

OB_SERIALIZE_MEMBER((ObDASBooleanQueryRtDef, ObIDASSearchRtDef));

ObDASScalarScanRtDef *ObDASBooleanQueryRtDef::get_merged_probe_rtdef() const
{
  ObDASScalarScanRtDef *merged_rtdef = nullptr;
  const ObDASBooleanQueryCtDef *bctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(bctdef) || !bctdef->has_merged_probe()) {
    // do nothing
  } else {
    const int64_t offset = bctdef->get_merged_probe_offset();
    if (offset < 0 || offset >= children_cnt_ || OB_ISNULL(children_) || OB_ISNULL(children_[offset])) {
      // do nothing
    } else if (DAS_OP_SCALAR_QUERY != children_[offset]->op_type_) {
      // The merged probe child is a DAS_OP_SCALAR_QUERY (ObDASScalarRtDef) whose only child
      // is the primary point-get scan.
    } else {
      ObDASScalarRtDef *scalar_rtdef = static_cast<ObDASScalarRtDef *>(children_[offset]);
      merged_rtdef = scalar_rtdef->get_main_scan_rtdef();
    }
  }
  return merged_rtdef;
}

int ObDASBooleanQueryRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  ObDASSearchCost min_required_cost;
  ObDASSearchCost min_should_cost;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(compute_required_cost(search_ctx, min_required_cost))) {
    LOG_WARN("failed to compute required cost", KR(ret));
  } else if (min_required_cost.is_valid() && ctdef->min_should_match_ == 0) {
    // when min_should_match = 0, should is handled in ReqOpt, no need to consider its cost
    cost = min_required_cost;
  } else if (OB_FAIL(compute_should_cost(search_ctx, min_should_cost))) {
    LOG_WARN("failed to compute should cost", KR(ret));
  } else if (!min_required_cost.is_valid() && !min_should_cost.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid cost", KR(ret));
  } else if (!min_required_cost.is_valid()) {
    cost = min_should_cost;
  } else if (!min_should_cost.is_valid()) {
    cost = min_required_cost;
  } else if (OB_FAIL(ObDASSearchCost::min(min_required_cost, min_should_cost, cost))) {
    LOG_WARN("failed to get min cost", KR(ret), K(min_required_cost), K(min_should_cost));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::compute_required_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get must clauses", KR(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get filter clauses", KR(ret));
  } else {
    #define ITERATE_CLAUSES(clauses)                                                                                     \
    do {                                                                                                                 \
      for (int i = 0; OB_SUCC(ret) && i < clauses.count(); i++) {                                                        \
        ObDASSearchCost clause_cost;                                                                                     \
        ObIDASSearchRtDef *clause = nullptr;                                                                             \
        if (OB_FAIL(clauses.get(i, clause))) {                                                                           \
          LOG_WARN("failed to get clause", KR(ret), K(i));                                                               \
        } else if (OB_ISNULL(clause)) {                                                                                  \
          ret = OB_ERR_UNEXPECTED;                                                                                       \
          LOG_WARN("unexpected nullptr clause", KR(ret), K(i));                                                          \
        } else if (OB_FAIL(clause->get_cost(search_ctx, clause_cost))) {                                                 \
          LOG_WARN("failed to get cost", KR(ret));                                                                       \
        } else if (!clause_cost.is_valid()) {                                                                            \
          ret = OB_ERR_UNEXPECTED;                                                                                       \
          LOG_WARN("invalid cost", KR(ret));                                                                             \
        } else if (!cost.is_valid()) {                                                                                   \
          cost = clause_cost;                                                                                            \
        } else if (OB_FAIL(ObDASSearchCost::min(cost, clause_cost, cost))) {                                             \
          LOG_WARN("failed to min cost", KR(ret), K(cost), K(clause_cost));                                              \
        }                                                                                                                \
      }                                                                                                                  \
    } while (0);
    ITERATE_CLAUSES(must_clauses);
    ITERATE_CLAUSES(filter_clauses);
    #undef ITERATE_CLAUSES
  }
  return ret;
}

int ObDASBooleanQueryRtDef::compute_should_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get should clauses", KR(ret));
  } else if (should_clauses.empty()) {
  } else {
    const int64_t should_cnt = should_clauses.count();
    const int64_t least_clause_cnt = should_cnt - ctdef->min_should_match_ + 1;
    ObDASInt64LessComp cmp;
    ObBinaryHeap<int64_t, ObDASInt64LessComp> max_heap(cmp);
    for (int64_t i = 0; OB_SUCC(ret) && i < should_cnt; ++i) {
      ObDASSearchCost clause_cost;
      ObIDASSearchRtDef *clause = nullptr;
      if (OB_FAIL(should_clauses.get(i, clause))) {
        LOG_WARN("failed to get clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else if (OB_FAIL(clause->get_cost(search_ctx, clause_cost))) {
        LOG_WARN("failed to get should clause cost", KR(ret), K(i));
      } else if (!clause_cost.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid should clause cost", KR(ret), K(i));
      } else {
        int64_t cur_cost = clause_cost.cost();
        if (OB_FAIL(max_heap.push(cur_cost))) {
          LOG_WARN("failed to push cost to heap", KR(ret), K(cur_cost));
        } else if (max_heap.count() > least_clause_cnt) {
          if (OB_FAIL(max_heap.pop())) {
            LOG_WARN("failed to pop from heap", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t sum_cost = 0;
      while (!max_heap.empty() && OB_SUCC(ret)) {
        sum_cost += max_heap.top();
        if (OB_FAIL(max_heap.pop())) {
          LOG_WARN("failed to pop from heap", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        cost = ObDASSearchCost(sum_cost);
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef =
      static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  // Additional scoring or predicates can make a descendant's competitive
  // candidate count much larger than K/selectivity. Keep legacy costing for
  // the whole subtree; the async deletion wrapper itself is not a user clause.
  const bool disable_topk_probe_cost = OB_NOT_NULL(ctdef) && !ctdef->is_async()
      && (ctdef->should_.count_ > 0 || ctdef->must_not_.count_ > 0
          || ctdef->must_.count_ > 1 || ctdef->filter_.count_ > 1);
  ObDASTopKProbeCostGuard topk_probe_cost_guard(search_ctx, disable_topk_probe_cost);
  const int64_t previous_costing_topk_hint = search_ctx.get_streaming_ror_costing_topk_hint();
  DEFER(search_ctx.set_streaming_ror_costing_topk_hint(previous_costing_topk_hint));
  if (OB_NOT_NULL(ctdef) && !ctdef->is_async()
      && (ctdef->should_.count_ > 0 || ctdef->must_not_.count_ > 0
          || ctdef->must_.count_ > 1 || ctdef->filter_.count_ > 1)) {
    search_ctx.set_streaming_ror_costing_topk_hint(-1);
  }
  if (is_async()) {
    if (OB_FAIL(generate_async_op(lead_cost, search_ctx, op))) {
      LOG_WARN("failed to generate multi-segment async query op", K(ret));
    }
  } else if (OB_NOT_NULL(ctdef)
             && search_ctx.get_async_group_slot() >= 0
             && common::OB_INVALID_ID == search_ctx.get_async_index_table_id()) {
    ObDASAsyncIndexDomainAnalysis analysis;
    if (OB_FAIL(ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
            ctdef, analysis))) {
      LOG_WARN("failed to analyze boolean async index domain", K(ret));
    } else if (ASYNC_INDEX_DOMAIN_SINGLE == analysis.type_
               && !analysis.has_group_compatible_sync_filter_) {
      if (analysis.is_pure_range_subtree_) {
        if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
          LOG_WARN("failed to generate pure async scalar range boolean query op",
                   K(ret), K(analysis));
        }
      } else if (OB_FAIL(generate_single_async_index_op(
                     lead_cost, analysis.index_table_id_, search_ctx, op))) {
        LOG_WARN("failed to claim single-index boolean subtree", K(ret), K(analysis));
      }
    } else if (ASYNC_INDEX_DOMAIN_MULTI == analysis.type_
               || (ASYNC_INDEX_DOMAIN_SINGLE == analysis.type_
                   && analysis.has_group_compatible_sync_filter_)) {
      if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate cross-index boolean query op", K(ret));
      }
    } else if (search_ctx.is_generating_group_filter_domain()) {
      bool is_group_compatible_filter = false;
      if (OB_FAIL(ObDASAsyncIndexQueryCtx::is_group_compatible_filter_domain(
              ctdef, is_group_compatible_filter))) {
        LOG_WARN("failed to validate nested Group filter domain", K(ret));
      } else if (!is_group_compatible_filter) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid nested Group filter domain", K(ret), K(analysis));
      } else if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate nested Group filter op", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected boolean async index domain in group mode", K(ret), K(analysis));
    }
  } else if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
    LOG_WARN("failed to generate boolean query op", K(ret));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_single_async_index_op(
    ObDASSearchCost lead_cost,
    const uint64_t index_table_id,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
  const ObDASBooleanQueryCtDef *ctdef =
      static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObSEArray<ObIDASSearchOp *, 4> segment_ops;
  ObDASAsyncBindingGuard binding_guard(search_ctx);
  const int64_t group_slot = search_ctx.get_async_group_slot();
  int64_t max_segment_cnt = 0;
  op = nullptr;
  if (OB_ISNULL(query_ctx) || OB_ISNULL(ctdef)
      || OB_UNLIKELY(OB_INVALID_ID == index_table_id || group_slot < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid single-index boolean claim", K(ret), KP(query_ctx), KP(ctdef),
             K(index_table_id), K(group_slot));
  } else {
    max_segment_cnt = query_ctx->get_max_segment_cnt(group_slot, index_table_id);
    binding_guard.set(index_table_id, group_slot, 1 >= max_segment_cnt);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX(1L, max_segment_cnt); ++i) {
    ObIDASSearchOp *segment_op = nullptr;
    if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, segment_op))) {
      LOG_WARN("failed to generate single-index boolean segment subtree",
               K(ret), K(index_table_id), K(group_slot), K(i));
    } else if (OB_ISNULL(segment_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null single-index boolean segment subtree",
               K(ret), K(index_table_id), K(group_slot), K(i));
    } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
      LOG_WARN("failed to append single-index boolean segment subtree", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
          segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, op))) {
    LOG_WARN("failed to create single-index boolean segment merge",
             K(ret), K(index_table_id), K(group_slot));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_op_impl(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> must_not_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObDASSearchCost self_cost;
  int64_t bool_should_msm = 0;
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (OB_FAIL(get_cost(search_ctx, self_cost))) {
    LOG_WARN("failed to get cost", KR(ret));
  } else if (!self_cost.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid cost", KR(ret));
  } else if (OB_FAIL(ObDASSearchCost::min(lead_cost, self_cost, lead_cost))) {
    LOG_WARN("failed to get min cost", KR(ret), K(lead_cost), K(self_cost));
  } else if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get must clauses", KR(ret));
  } else if (OB_FAIL(must_not(must_not_clauses))) {
    LOG_WARN("failed to get must not clauses", KR(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get filter clauses", KR(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get should clauses", KR(ret));
  } else {
    bool_should_msm = ctdef->min_should_match_;
    const int64_t should_n = should_clauses.count();
    ObDatum *bool_msm_snapshot = nullptr;
    if (should_n > 0 && OB_NOT_NULL(ctdef->min_should_match_expr_)) {
      ObDatum *msm_datum = nullptr;
      if (OB_ISNULL(eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null eval ctx for bool minimum_should_match", KR(ret));
      } else if (OB_FAIL(ctdef->min_should_match_expr_->eval(*eval_ctx_, msm_datum))) {
        LOG_WARN("failed to eval bool minimum_should_match expr", KR(ret));
      } else if (OB_UNLIKELY(msm_datum->is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null bool msm datum", KR(ret));
      } else {
        bool_msm_snapshot = msm_datum;
        if (OB_FAIL(ObDASMSMRuntimeUtil::calc_bool_should_msm(allocator_, should_n,
                                                              ctdef->is_msm_unresolved_expr_,
                                                              msm_datum, bool_should_msm))) {
          LOG_WARN("failed to calculate bool should minimum_should_match", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret) && should_n > 0 && OB_ISNULL(ctdef->min_should_match_expr_)) {
      bool_should_msm = MAX(static_cast<int64_t>(0), MIN(bool_should_msm, should_n));
    }

    ObDASScalarScanRtDef *merged_probe_rtdef = get_merged_probe_rtdef();
    bool do_merge_probe_opt = false;
    if (OB_SUCC(ret) && OB_NOT_NULL(merged_probe_rtdef)) {
      if (OB_FAIL(try_boolean_merge_probe_opt(
          must_clauses,
          filter_clauses,
          should_clauses,
          must_not_clauses,
          bool_should_msm,
          lead_cost,
          search_ctx,
          merged_probe_rtdef,
          op,
          do_merge_probe_opt))) {
        LOG_WARN("failed to try boolean merge probe opt", KR(ret));
      }
    }

    if (OB_FAIL(ret) || do_merge_probe_opt) {
    } else if (should_clauses.count() == 0) {
      // full conjunction query
      ObIDASSearchOp *req_op = nullptr;
      if (OB_FAIL(req(must_clauses, filter_clauses, lead_cost, ctdef->is_top_level_scoring(), search_ctx, req_op))) {
        LOG_WARN("failed to generate required op", KR(ret));
      } else if (OB_FAIL(excl(req_op, must_not_clauses, lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate excluded op", KR(ret));
      }
    } else if (must_clauses.count() == 0 && filter_clauses.count() == 0) {
      // full disjunction query
      // Without required clauses, a candidate must come from a matching child.
      ObIDASSearchOp *opt_op = nullptr;
      if (OB_FAIL(opt(should_clauses, MAX(1L, bool_should_msm), ctdef->is_scoring(), lead_cost,
                      ctdef->is_top_level_scoring(), search_ctx, opt_op))) {
        if (OB_NOT_NULL(bool_msm_snapshot)) {
          LOG_WARN("Boolean query failed to build optional should operator",
                   KR(ret), KPC(bool_msm_snapshot));
        } else {
          LOG_WARN("failed to generate optional op", KR(ret));
        }
      } else if (OB_FAIL(excl(opt_op, must_not_clauses, lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate excluded op", KR(ret));
      }
    } else {
      // mixed conjunction and disjunction query
      ObIDASSearchOp *req_op = nullptr;
      ObIDASSearchOp *excl_op = nullptr;
      ObIDASSearchOp *opt_op = nullptr;

      // Push filter ops directly into BMMOp when must is empty (only filter as required) and
      // BMMOp would be created for the should clauses, to enable early pivot pruning within BMMOp.
      const bool can_push_filter_to_should =
          must_clauses.empty() &&
          must_not_clauses.empty() &&
          !filter_clauses.empty() &&
          !DISABLE_BMM_FILTER_PUSHDOWN &&
          should_clauses.count() == 1;
      bool can_push_filter = false;
      int64_t group_filter_consumer_count = 1;
      int64_t query_filter_consumer_count = 1;
      ObIDASSearchRtDef *pushdown_should_rtdef = nullptr;
      ObIDASSearchOp *pushed_filter_op = nullptr;
      ObDASSearchCost pushed_filter_cost;
      ObDASBMMBuildOptions bmm_options(false);
      bool has_bmm_options = false;
      ObDASFilterBitmapProvider *pushed_filter_provider = nullptr;
      if (can_push_filter_to_should) {
        if (OB_FAIL(should_clauses.get(0, pushdown_should_rtdef))) {
          LOG_WARN("failed to get should clause", KR(ret));
        } else if (OB_ISNULL(pushdown_should_rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr to should clause", KR(ret));
        } else if (OB_FAIL(pushdown_should_rtdef->can_pushdown_filter_to_bmm(
                     search_ctx, can_push_filter))) {
          LOG_WARN("failed to check filter pushdown for should clause", KR(ret));
        }
      }

      if (OB_SUCC(ret) && can_push_filter
          && search_ctx.get_async_group_slot() >= 0) {
        if (OB_FAIL(pushdown_should_rtdef->get_bmm_filter_consumer_counts(
                search_ctx, group_filter_consumer_count,
                query_filter_consumer_count))) {
          LOG_WARN("failed to get optional BMM filter consumer counts", K(ret));
        } else if (group_filter_consumer_count <= 0
                   || (0 == bool_should_msm && group_filter_consumer_count > 1)) {
          // An empty optional MATCH still has the complete Filter result domain.
          // Multiple BMMs in one Group have no unique owner for filter-only rows.
          // Keep ReqOpt in both cases, even when the Filter can enumerate RowIDs.
          can_push_filter = false;
        }
      }
      if (OB_SUCC(ret) && can_push_filter
          && search_ctx.get_async_group_slot() >= 0
          && (group_filter_consumer_count > 1
              || query_filter_consumer_count > 1)) {
        ObSEArray<ObIDASSearchRtDef *, 4> sync_filter_clauses;
        ObSEArray<ObIDASSearchRtDef *, 4> async_filter_clauses;
        bool is_scope_splittable = false;
        if (OB_FAIL(split_group_filter_clauses(
                filter_clauses, sync_filter_clauses,
                async_filter_clauses, is_scope_splittable))) {
          LOG_WARN("failed to split optional filter clauses by scope", K(ret));
        } else if (0 == bool_should_msm
                   && query_filter_consumer_count > 1
                   && !(is_scope_splittable && sync_filter_clauses.empty()
                        && !async_filter_clauses.empty())) {
          // A pure async Filter is Group-local, so one BMM per Group is safe.
          // A synchronous or unproven mixed domain may repeat filter-only rows
          // across Groups; sharing its bitmap does not establish row ownership.
          can_push_filter = false;
        } else if (is_scope_splittable
                   && !sync_filter_clauses.empty()
                   && !async_filter_clauses.empty()
                   && query_filter_consumer_count > 1) {
          // Required mixed filters use a query-scope sync provider plus a
          // Group-local async component. Keep optional mixed semantics outside
          // BMM until filter-only row ownership has an explicit Group contract.
          can_push_filter = false;
        } else {
          const bool is_query_scope = is_scope_splittable
              && async_filter_clauses.empty() && !sync_filter_clauses.empty();
          const int64_t consumer_count = is_query_scope
              ? query_filter_consumer_count : group_filter_consumer_count;
          if (consumer_count > 1
              && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()) {
            const ObDASFilterBitmapProviderScope provider_scope = is_query_scope
                ? DAS_FILTER_BITMAP_QUERY_SCOPE : DAS_FILTER_BITMAP_GROUP_SCOPE;
            const int64_t provider_group_slot = is_query_scope
                ? DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT
                : search_ctx.get_async_group_slot();
            if (OB_FAIL(try_get_filter_bitmap_provider(
                    filter_clauses, lead_cost, search_ctx,
                    provider_scope, provider_group_slot,
                    pushed_filter_provider))) {
              LOG_WARN("failed to prepare optional filter bitmap provider", K(ret));
            } else if (OB_ISNULL(pushed_filter_provider)) {
              can_push_filter = false;
            } else if (OB_FAIL(estimate_filter_cost(
                           filter_clauses, search_ctx, pushed_filter_cost))) {
              LOG_WARN("failed to estimate optional provider filter cost", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret) && can_push_filter && OB_ISNULL(pushed_filter_provider)) {
        ObDASSearchCost pushdown_should_cost;
        if (OB_FAIL(pushdown_should_rtdef->get_cost(search_ctx, pushdown_should_cost))) {
          LOG_WARN("failed to prepare should cost for bmm filter pushdown",
                   KR(ret), K(pushdown_should_rtdef->op_type_));
        } else if (OB_FAIL(prepare_bmm_input(
            filter_clauses, lead_cost, search_ctx, pushed_filter_op,
            pushed_filter_cost, 0 == bool_should_msm, bmm_options, has_bmm_options))) {
          LOG_WARN("failed to generate filter op for bmm pushdown", KR(ret));
        } else if (OB_ISNULL(pushed_filter_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null generated filter op", KR(ret));
        } else if (0 == bool_should_msm
                   && !pushed_filter_op->can_enumerate_rowids()) {
          // Query-optional BMM is filter-driven. Probe-only filters cannot define
          // its complete result domain, so retain the ReqOpt fallback topology.
          can_push_filter = false;
          pushed_filter_op = nullptr;
          pushed_filter_cost.reset();
        }
      }

      if (OB_FAIL(ret)) {
      } else if (can_push_filter) {
        if (OB_FAIL(opt(
                       should_clauses, bool_should_msm, ctdef->is_scoring(),
                       lead_cost, ctdef->is_top_level_scoring(), search_ctx,
                       opt_op, bool_should_msm == 0, pushed_filter_op,
                       pushed_filter_cost, pushed_filter_provider,
                       has_bmm_options ? &bmm_options : nullptr))) {
          if (OB_NOT_NULL(bool_msm_snapshot)) {
            LOG_WARN("Boolean query failed to build optional operator with filter pushdown",
                     KR(ret), KPC(bool_msm_snapshot));
          } else {
            LOG_WARN("failed to generate opt op with pushed filter", KR(ret));
          }
        } else {
          op = opt_op;
        }
      } else if (OB_FAIL(
              req(must_clauses, filter_clauses, lead_cost, ctdef->is_top_level_scoring(), search_ctx, req_op))) {
        LOG_WARN("failed to generate required op", KR(ret));
      } else if (OB_FAIL(excl(req_op, must_not_clauses, lead_cost, search_ctx, excl_op))) {
        LOG_WARN("failed to generate excluded op", KR(ret));
      } else if (OB_FAIL(opt(should_clauses, bool_should_msm, ctdef->is_scoring(), lead_cost,
                             ctdef->is_top_level_scoring(), search_ctx, opt_op))) {
        if (OB_NOT_NULL(bool_msm_snapshot)) {
          LOG_WARN("Boolean query failed to build optional should operator",
                   KR(ret), KPC(bool_msm_snapshot));
        } else {
          LOG_WARN("failed to generate optional op", KR(ret));
        }
      } else if (bool_should_msm > 0) {
        ObArray<ObIDASSearchOp *> required;
        if (OB_FAIL(construct_op_array(required, excl_op, opt_op))) {
          LOG_WARN("failed to construct op array", KR(ret));
        } else if (OB_FAIL(generate_conjunction_op(required, search_ctx, op))) {
          LOG_WARN("failed to generate conjunction op", KR(ret));
        }
      } else if (bool_should_msm == 0) {
        if (OB_FAIL(generate_req_opt_op(excl_op, opt_op, search_ctx, op))) {
          LOG_WARN("failed to generate req opt op", KR(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected min should match", KR(ret), K(bool_should_msm));
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_async_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  op = nullptr;
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> must_not_clauses;
  ObIDASSearchRtDef *query_rtdef = nullptr;
  ObSEArray<ObIDASSearchOp *, 4> group_ops;
  ObDASSearchCost self_cost;
  ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
  const ObDASBooleanQueryCtDef *ctdef =
      static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  ObDASAsyncBindingGuard binding_guard(search_ctx);
  int64_t max_group_cnt = 0;
  const int64_t bound_group = search_ctx.get_async_group_slot();
  if (OB_ISNULL(query_ctx) || OB_UNLIKELY(!query_ctx->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index query catalog is not initialized", K(ret), KP(query_ctx));
  } else if (!search_ctx.use_async_group_alignment()) {
    return generate_legacy_async_op(lead_cost, search_ctx, op);
  } else if (OB_FAIL(get_cost(search_ctx, self_cost))) {
    LOG_WARN("failed to get async boolean query cost", K(ret));
  } else if (OB_FAIL(ObDASSearchCost::min(lead_cost, self_cost, lead_cost))) {
    LOG_WARN("failed to get async boolean lead cost", K(ret));
  } else if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get async must clauses", K(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get async filter clauses", K(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get async should clauses", K(ret));
  } else if (OB_FAIL(must_not(must_not_clauses))) {
    LOG_WARN("failed to get async must-not clauses", K(ret));
  } else if (OB_UNLIKELY(1 != must_clauses.count() || !filter_clauses.empty()
                         || !should_clauses.empty() || 1 != must_not_clauses.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async boolean query shape", K(ret), K(must_clauses.count()),
             K(filter_clauses.count()), K(should_clauses.count()), K(must_not_clauses.count()));
  } else if (OB_FAIL(must_clauses.get(0, query_rtdef))) {
    LOG_WARN("failed to get async query clause", K(ret));
  } else if (OB_ISNULL(query_rtdef) || OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async query definition", K(ret), KP(query_rtdef), KP(ctdef));
  } else if (OB_UNLIKELY((max_group_cnt = query_ctx->get_max_group_cnt()) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async query catalog has no segment group", K(ret), K(max_group_cnt));
  }

  if (OB_SUCC(ret) && (bound_group < -1 || bound_group >= max_group_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bound async group outside query catalog", K(ret), K(bound_group), K(max_group_cnt));
  }
  for (int64_t group_slot = bound_group < 0 ? 0 : bound_group;
       OB_SUCC(ret) && group_slot < (bound_group < 0 ? max_group_cnt : bound_group + 1); ++group_slot) {
    ObIDASSearchOp *query_op = nullptr;
    ObIDASSearchOp *deletion_op = nullptr;
    ObIDASSearchOp *group_op = nullptr;
    // Each Group clones the complete user query and adds its own deletion exclusion layer.
    // The binding guard restores the previous binding on scope exit.
    binding_guard.set(OB_INVALID_ID, group_slot, false);
    if (OB_FAIL(query_rtdef->generate_op(lead_cost, search_ctx, query_op))) {
      LOG_WARN("failed to generate async Group query tree", K(ret), K(group_slot));
    } else if (OB_ISNULL(query_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null async Group query op", K(ret), K(group_slot));
    } else if (OB_FAIL(generate_async_deletion_op(
                   must_not_clauses, search_ctx, deletion_op))) {
      LOG_WARN("failed to generate async Group deletion op", K(ret), K(group_slot));
    } else if (OB_FAIL(generate_req_excl_op(
                   query_op, deletion_op, search_ctx, group_op))) {
      LOG_WARN("failed to apply deletion inside async Group", K(ret), K(group_slot));
    } else if (OB_FAIL(group_ops.push_back(group_op))) {
      LOG_WARN("failed to append async Group query tree", K(ret), K(group_slot));
    }
  }
  if (OB_SUCC(ret) && bound_group >= 0) {
    if (1 != group_ops.count()) { ret = OB_ERR_UNEXPECTED; }
    else { op = group_ops.at(0); }
  } else if (OB_SUCC(ret) && OB_FAIL(ObDASSearchUtils::create_async_segment_group_merge_op(
          group_ops, ctdef->is_scoring(), search_ctx, op))) {
    LOG_WARN("failed to create async segment group merge", K(ret), K(max_group_cnt));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_legacy_async_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> should_clauses;
  ObBooleanSubClause<ObIDASSearchRtDef> must_not_clauses;
  ObIDASSearchRtDef *query_rtdef = nullptr;
  ObIDASSearchOp *query_op = nullptr;
  ObDASSearchCost self_cost;
  ObDASAsyncBindingGuard binding_guard(search_ctx);
  binding_guard.set(OB_INVALID_ID, -1, false);
  op = nullptr;
  if (OB_FAIL(get_cost(search_ctx, self_cost))) {
    LOG_WARN("failed to get legacy async boolean query cost", K(ret));
  } else if (OB_FAIL(ObDASSearchCost::min(lead_cost, self_cost, lead_cost))) {
    LOG_WARN("failed to get legacy async boolean lead cost", K(ret));
  } else if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get legacy async must clauses", K(ret));
  } else if (OB_FAIL(filter(filter_clauses))) {
    LOG_WARN("failed to get legacy async filter clauses", K(ret));
  } else if (OB_FAIL(should(should_clauses))) {
    LOG_WARN("failed to get legacy async should clauses", K(ret));
  } else if (OB_FAIL(must_not(must_not_clauses))) {
    LOG_WARN("failed to get legacy async must-not clauses", K(ret));
  } else if (OB_UNLIKELY(1 != must_clauses.count() || !filter_clauses.empty()
                         || !should_clauses.empty() || 1 != must_not_clauses.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected legacy async boolean query shape", K(ret),
             K(must_clauses.count()), K(filter_clauses.count()),
             K(should_clauses.count()), K(must_not_clauses.count()));
  } else if (OB_FAIL(must_clauses.get(0, query_rtdef))) {
    LOG_WARN("failed to get legacy async query clause", K(ret));
  } else if (OB_ISNULL(query_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null legacy async query clause", K(ret));
  } else if (OB_FAIL(query_rtdef->generate_op(lead_cost, search_ctx, query_op))) {
    LOG_WARN("failed to generate legacy async query tree", K(ret));
  } else if (OB_ISNULL(query_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null legacy async query op", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIDASSearchOp *deletion_op = nullptr;
    if (OB_FAIL(generate_async_deletion_op(
            must_not_clauses, search_ctx, deletion_op))) {
      LOG_WARN("failed to generate legacy async deletion op", K(ret));
    } else if (OB_FAIL(generate_req_excl_op(
                   query_op, deletion_op, search_ctx, op))) {
      LOG_WARN("failed to apply legacy async deletion", K(ret));
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::req(const ObBooleanSubClause<ObIDASSearchRtDef> &must,
                                const ObBooleanSubClause<ObIDASSearchRtDef> &filter,
                                const ObDASSearchCost& lead_cost,
                                const bool is_top_level,
                                ObDASSearchCtx &search_ctx,
                                ObIDASSearchOp *&op)
{
  // TODO: block max conjunction
  int ret = OB_SUCCESS;
  ObArray<ObIDASSearchRtDef *> required_clauses;
  ObArray<ObIDASSearchOp *> required_ops;
  bool filter_pushed_down = false;
  #define FILL_REQUIRED_CLAUSES(source)                                                                                  \
  do {                                                                                                                   \
    for (int i = 0; OB_SUCC(ret) && i < source.count(); i++) {                                                           \
      ObIDASSearchRtDef *clause = nullptr;                                                                               \
      if (OB_FAIL(source.get(i, clause))) {                                                                              \
        LOG_WARN("failed to get clause", KR(ret), K(i));                                                                 \
      } else if (OB_ISNULL(clause)) {                                                                                    \
        ret = OB_ERR_UNEXPECTED;                                                                                         \
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));                                                            \
      } else if (OB_FAIL(required_clauses.push_back(clause))) {                                                          \
        LOG_WARN("failed to push back clause", KR(ret), K(i));                                                           \
      }                                                                                                                  \
    }                                                                                                                    \
  } while (0);

  FILL_REQUIRED_CLAUSES(must);
  const bool can_try_pushdown_filter =
      1 == must.count() && !filter.empty() && !DISABLE_BMM_FILTER_PUSHDOWN;
  // Same physical choice as knn+filter primary get: only skip BMM when a filter scalar
  // will actually be a non-driver probe after the required clauses are cost-sorted.
  // Otherwise keep BMM pushdown. Once filters stay in conjunction, the path below reuses
  // the vector path: cost-sort, allow_probe on non-driver scalars, generate_op.
  ObIDASSearchRtDef *must_clause = nullptr;
  bool can_pushdown_to_bmm = false;
  bool skip_bmm_for_primary_get = false;
  ObIDASSearchRtDef *topk_probe_clause = nullptr;
  int64_t group_filter_consumer_count = 1;
  int64_t query_filter_consumer_count = 1;
  const bool can_use_filter_provider = search_ctx.get_async_group_slot() >= 0
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type();
  if (OB_SUCC(ret) && can_try_pushdown_filter) {
    if (OB_FAIL(must.get(0, must_clause))) {
      LOG_WARN("failed to get must clause", KR(ret));
    } else if (OB_ISNULL(must_clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr must clause", KR(ret));
    } else if (OB_FAIL(must_clause->can_pushdown_filter_to_bmm(
                   search_ctx, can_pushdown_to_bmm))) {
      LOG_WARN("failed to check filter pushdown for must clause", KR(ret));
    } else if (can_pushdown_to_bmm
               && search_ctx.get_async_group_slot() >= 0
               && OB_FAIL(must_clause->get_bmm_filter_consumer_counts(
                      search_ctx, group_filter_consumer_count,
                      query_filter_consumer_count))) {
      LOG_WARN("failed to get BMM filter consumer counts", K(ret));
    } else if (can_pushdown_to_bmm && group_filter_consumer_count > 0
               && OB_FAIL(should_skip_bmm_for_async_fts_primary_get(
                   *must_clause, filter, lead_cost, search_ctx, skip_bmm_for_primary_get))) {
      LOG_WARN("failed to decide skip bmm for async fts primary get", KR(ret));
    } else if (!skip_bmm_for_primary_get && can_pushdown_to_bmm && 1 == group_filter_consumer_count
               && 1 == query_filter_consumer_count
               && OB_FAIL(choose_topk_range_probe(
                   *must_clause, filter, search_ctx, topk_probe_clause))) {
      LOG_WARN("failed to choose topk range probe", KR(ret));
    } else if (OB_NOT_NULL(topk_probe_clause)) {
      skip_bmm_for_primary_get = true;
    } else if (skip_bmm_for_primary_get) {
      LOG_TRACE("skip bmm filter pushdown for async fts+filter primary get");
    }
  }
  if (OB_SUCC(ret)
      && (0 == group_filter_consumer_count
          || (can_pushdown_to_bmm && !skip_bmm_for_primary_get))) {
    const bool filter_branch_is_empty = 0 == group_filter_consumer_count;
    if (filter_branch_is_empty) {
      // MATCH is required and has no token/BMM consumer. The whole required
      // expression is empty, so do not generate or open the filter.
      filter_pushed_down = true;
    } else {
      ObSEArray<ObIDASSearchRtDef *, 4> sync_filter_clauses;
      ObSEArray<ObIDASSearchRtDef *, 4> async_filter_clauses;
      ObDASFilterBitmapProvider *filter_provider = nullptr;
      ObDASSearchCost filter_cost;
      bool is_scope_splittable = false;
      ObDASScalarScanRtDef *streaming_filter = nullptr;
      if (can_use_filter_provider && group_filter_consumer_count > 1
          && common::OB_INVALID_ID == search_ctx.get_async_index_table_id()
          && DAS_OP_MATCH_QUERY == must_clause->op_type_ && 1 == filter.count()) {
        ObIDASSearchRtDef *filter_clause = nullptr;
        if (OB_FAIL(filter.get(0, filter_clause))) {
          LOG_WARN("failed to get streaming ROR candidate", K(ret));
        } else if (OB_NOT_NULL(filter_clause) && DAS_OP_SCALAR_QUERY == filter_clause->op_type_) {
          ObDASScalarRtDef *scalar = static_cast<ObDASScalarRtDef *>(filter_clause);
          ObDASScalarScanRtDef *scan = scalar->get_index_scan_rtdef();
          if (OB_NOT_NULL(scalar->get_ctdef()) && scalar->get_ctdef()->has_index_scan()
              && !scalar->get_ctdef()->is_scoring() && OB_NOT_NULL(scan)
              && scan->can_build_streaming_group_filter(search_ctx)) {
            ObDASSearchCost match_cost;
            ObDASSearchCost scalar_cost;
            if (OB_FAIL(must_clause->get_cost(search_ctx, match_cost))) {
              LOG_WARN("failed to estimate streaming match cost", K(ret));
            } else if (OB_FAIL(scalar->get_cost(search_ctx, scalar_cost))) {
              LOG_WARN("failed to estimate streaming filter cost", K(ret));
            } else if (match_cost.is_valid() && scalar_cost.is_valid()
                && search_ctx.get_row_count().is_valid()
                && prefer_streaming_ror_filter(match_cost.cost(), scalar_cost.cost(),
                    search_ctx.get_row_count().cost(), group_filter_consumer_count,
                    search_ctx.get_streaming_ror_costing_topk_hint())) {
              streaming_filter = scan;
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(streaming_filter) && can_use_filter_provider
          && (group_filter_consumer_count > 1
              || query_filter_consumer_count > 1)
          && OB_FAIL(split_group_filter_clauses(
                 filter, sync_filter_clauses,
                 async_filter_clauses, is_scope_splittable))) {
        LOG_WARN("failed to split required filter clauses by scope", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(streaming_filter)) {
        if (OB_FAIL(estimate_filter_cost(filter, search_ctx, filter_cost))) {
          LOG_WARN("failed to estimate streaming ROR filter cost", K(ret));
        } else {
          static_cast<ObDASMatchRtDef *>(must_clause)->set_streaming_group_filter(
              streaming_filter, filter_cost);
          filter_pushed_down = true;
        }
      } else if (is_scope_splittable
                 && !sync_filter_clauses.empty()
                 && query_filter_consumer_count > 1) {
        ObBooleanSubClause<ObIDASSearchRtDef> sync_filter(sync_filter_clauses);
        ObDASFilterBitmapProvider *sync_provider = nullptr;
        if (OB_FAIL(try_get_filter_bitmap_provider(
                sync_filter, lead_cost, search_ctx,
                DAS_FILTER_BITMAP_QUERY_SCOPE,
                DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
                sync_provider))) {
          LOG_WARN("failed to prepare query-scope sync filter provider", K(ret));
        } else if (OB_ISNULL(sync_provider)) {
          // Keep the outer conjunction when the sync source cannot enumerate.
        } else if (async_filter_clauses.empty()) {
          filter_provider = sync_provider;
        } else if (group_filter_consumer_count > 1) {
          ObBooleanSubClause<ObIDASSearchRtDef> async_filter(async_filter_clauses);
          if (OB_FAIL(try_get_mixed_filter_bitmap_provider(
                  *sync_provider, async_filter, lead_cost,
                  search_ctx, filter_provider))) {
            LOG_WARN("failed to prepare mixed Group filter provider", K(ret));
          }
        } else {
          ObBooleanSubClause<ObIDASSearchRtDef> async_filter(async_filter_clauses);
          ObDASBitmapOp *sync_view = nullptr;
          ObIDASSearchOp *async_filter_op = nullptr;
          ObIDASSearchOp *mixed_filter_op = nullptr;
          ObArray<ObIDASSearchOp *> mixed_filter_ops;
          if (OB_FAIL(sync_provider->create_view(search_ctx, sync_view))) {
            LOG_WARN("failed to create query-scope sync filter view", K(ret));
          } else if (OB_FAIL(generate_materialized_filter_op(
                         async_filter, lead_cost, search_ctx,
                         false /* allow_direct_drain */, async_filter_op))) {
            LOG_WARN("failed to generate direct async filter for mixed pushdown", K(ret));
          } else if (OB_FAIL(mixed_filter_ops.push_back(sync_view))) {
            LOG_WARN("failed to append direct sync filter view", K(ret));
          } else if (OB_FAIL(mixed_filter_ops.push_back(async_filter_op))) {
            LOG_WARN("failed to append direct async filter op", K(ret));
          } else if (OB_FAIL(generate_conjunction_op(
                         mixed_filter_ops, search_ctx, mixed_filter_op))) {
            LOG_WARN("failed to generate direct mixed filter conjunction", K(ret));
          } else if (OB_FAIL(estimate_filter_cost(filter, search_ctx, filter_cost))) {
            LOG_WARN("failed to estimate direct mixed filter cost", K(ret));
          } else {
            must_clause->set_pushdown_filter(
                false, mixed_filter_op, filter_cost);
            filter_pushed_down = true;
          }
        }
      } else if ((is_scope_splittable && async_filter_clauses.empty()
                  ? query_filter_consumer_count : group_filter_consumer_count) > 1
                 && can_use_filter_provider) {
        const bool is_query_scope = is_scope_splittable
            && async_filter_clauses.empty() && !sync_filter_clauses.empty();
        const ObDASFilterBitmapProviderScope provider_scope = is_query_scope
            ? DAS_FILTER_BITMAP_QUERY_SCOPE : DAS_FILTER_BITMAP_GROUP_SCOPE;
        const int64_t provider_group_slot = is_query_scope
            ? DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT
            : search_ctx.get_async_group_slot();
        if (OB_FAIL(try_get_filter_bitmap_provider(
                filter, lead_cost, search_ctx, provider_scope,
                provider_group_slot, filter_provider))) {
          LOG_WARN("failed to prepare required filter bitmap provider", K(ret),
                   K(provider_scope), K(provider_group_slot));
        }
      } else {
        ObIDASSearchOp *filter_op = nullptr;
        ObDASBMMBuildOptions bmm_options(false);
        bool has_bmm_options = false;
        if (OB_FAIL(prepare_bmm_input(
                filter, lead_cost, search_ctx, filter_op, filter_cost,
                false /* query_optional */, bmm_options, has_bmm_options))) {
          LOG_WARN("failed to generate filter op for bmm pushdown", KR(ret));
        } else if (has_bmm_options) {
          if (OB_FAIL(must_clause->set_bmm_input(false, filter_op, filter_cost, bmm_options))) {
            LOG_WARN("failed to install required BMM input", K(ret));
          } else {
            filter_pushed_down = true;
          }
        } else {
          must_clause->set_pushdown_filter(false, filter_op, filter_cost);
          filter_pushed_down = true;
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(filter_provider)) {
        if (OB_FAIL(estimate_filter_cost(filter, search_ctx, filter_cost))) {
          LOG_WARN("failed to estimate required provider filter cost", K(ret));
        } else if (OB_FAIL(must_clause->set_pushdown_filter_provider(
                       false, filter_provider, filter_cost))) {
          LOG_WARN("failed to install required filter bitmap provider", K(ret));
        } else {
          filter_pushed_down = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !filter_pushed_down) {
    FILL_REQUIRED_CLAUSES(filter);
  }
  #undef FILL_REQUIRED_CLAUSES

  if (OB_SUCC(ret)) {
    // Same as knn+filter: sort by cost so the cheapest clause drives; non-drivers may probe.
    ObDASRtDefCostComparator comparator(search_ctx);
    lib::ob_sort(required_clauses.begin(), required_clauses.end(), comparator);
    // A selected probe cannot drive a conjunction. Move it behind every enumerable
    // clause while preserving the cost order of all other required clauses. Do not
    // assume a two-clause shape so future required-clause combinations remain valid.
    move_selected_probe_after_drivers(required_clauses, topk_probe_clause);
    // Set allow_probe on each scalar required clause before driving its generate_op:
    //   - driver (i == 0) is the lead, must iterate via advance_to/next_rowid, so allow_probe = false
    //   - non-driver scalar clause gets allow_probe = true, thus a probe op could be generated
    //     based on the actual cost and primary_get_ratio_ config. Once the probe op is generated,
    //     the conjunction will coordinate it via probe() against the driver.
    // NOTE: only scalar clauses with direct parent conjunction are eligible for probe path.
    //     knn+filter and should/must_not paths are not affected currently.
    // Async fulltext+filter: after skipping BMM, the query/filter clauses reuse the same gate
    //     (ObDASScalarRtDef::generate_op -> prefer_primary_probe).
    for (int64_t i = 0; OB_SUCC(ret) && i < required_clauses.count(); i++) {
      ObIDASSearchRtDef *clause = nullptr;
      const ObIDASSearchCtDef *ctdef = nullptr;
      if (OB_FAIL(required_clauses.at(i, clause))) {
        LOG_WARN("failed to get clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else if (OB_ISNULL((ctdef = static_cast<const ObIDASSearchCtDef *>(clause->ctdef_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr ctdef", KR(ret));
      } else if (clause->op_type_ == DAS_OP_SCALAR_QUERY) {
        clause->set_allow_probe(
          i != 0 &&
          !ctdef->is_scoring() &&
          DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type());
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < required_clauses.count(); i++) {
      ObIDASSearchRtDef *clause = nullptr;
      ObIDASSearchOp *required_op = nullptr;
      if (OB_FAIL(required_clauses.at(i, clause))) {
        LOG_WARN("failed to get clause", KR(ret), K(i));
      } else if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else {
        bool is_filter_clause = false;
        // Cost sorting must not erase a clause's original FILTER identity.
        // Only FILTER children inherit this domain; ordinary MUST children
        // still use the enclosing query domain.
        for (int64_t j = 0; OB_SUCC(ret) && !is_filter_clause && j < filter.count(); ++j) {
          ObIDASSearchRtDef *filter_clause = nullptr;
          if (OB_FAIL(filter.get(j, filter_clause))) {
            LOG_WARN("failed to identify required filter clause", K(ret), K(j));
          } else {
            is_filter_clause = filter_clause == clause;
          }
        }
        if (OB_SUCC(ret)) {
          ObDASGroupFilterDomainGuard filter_domain_guard(search_ctx, is_filter_clause);
          // The bounded Top-K model has already selected the scalar's primary-probe
          // physical path. Express that choice directly and retain the real lead cost.
          if (clause == topk_probe_clause) {
            if (OB_UNLIKELY(DAS_OP_SCALAR_QUERY != clause->op_type_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected non-scalar topk probe clause", KR(ret), K(clause->op_type_));
            } else if (OB_FAIL(static_cast<ObDASScalarRtDef *>(clause)->generate_selected_primary_probe_op(
                           lead_cost, search_ctx, required_op))) {
              LOG_WARN("failed to generate selected topk primary probe", KR(ret));
            }
          } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, required_op))) {
            LOG_WARN("failed to generate required op", KR(ret));
          }
          if (OB_SUCC(ret) && OB_FAIL(required_ops.push_back(required_op))) {
            LOG_WARN("failed to push back required op", KR(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (required_ops.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected required ops", KR(ret), K(required_ops.size()));
    } else if (1 == required_ops.size()) {
      op = required_ops.at(0);
    } else if (OB_FAIL(generate_conjunction_op(required_ops, search_ctx, op))) {
      LOG_WARN("failed to generate conjunction op", KR(ret));
    }
    // async fulltext: each query type (match / multi_match / query_string) wraps its own
    // SegmentMergeOp internally, so boolean layer no longer wraps the required subtree.
  }

  return ret;
}

int ObDASBooleanQueryRtDef::opt(const ObBooleanSubClause<ObIDASSearchRtDef> &should,
                                const int64_t min_should_match,
                                const bool need_score,
                                const ObDASSearchCost& lead_cost,
                                const bool is_top_level,
                                ObDASSearchCtx &search_ctx,
                                ObIDASSearchOp *&op,
                                const bool query_optional,
                                ObIDASSearchOp *filter_op,
                                ObDASSearchCost filter_cost,
                                ObDASFilterBitmapProvider *filter_provider,
                                const ObDASBMMBuildOptions *bmm_options)
{
  int ret = OB_SUCCESS;
  ObArray<ObIDASSearchOp *> optional_ops;
  ObArray<ObDASSearchCost> optional_costs;
  const bool need_optional_cost = OB_NOT_NULL(filter_op) || OB_NOT_NULL(filter_provider)
      || (should.count() > 1 && need_score && is_top_level && min_should_match <= 1);
  if (OB_UNLIKELY(OB_NOT_NULL(filter_op) && OB_NOT_NULL(filter_provider))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pushdown filter op and provider are mutually exclusive", K(ret));
  } else if (OB_NOT_NULL(bmm_options) && OB_ISNULL(filter_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("explicit BMM input requires its filter", K(ret));
  } else if (OB_UNLIKELY((OB_NOT_NULL(filter_op) || OB_NOT_NULL(filter_provider))
                         && should.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected should clauses with pushdown filter", KR(ret), K(should.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < should.count(); i++) {
    ObIDASSearchRtDef *clause = nullptr;
    ObIDASSearchOp *optional_op = nullptr;
    ObDASSearchCost optional_cost;
    if (OB_FAIL(should.get(i, clause))) {
      LOG_WARN("failed to get clause", KR(ret), K(i));
    } else if (OB_ISNULL(clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
    } else if (need_optional_cost
        && OB_FAIL(clause->get_cost(search_ctx, optional_cost))) {
      LOG_WARN("failed to prepare optional clause cost", KR(ret), K(i));
    } else if (OB_NOT_NULL(filter_provider)
               && OB_FAIL(clause->set_pushdown_filter_provider(
                   query_optional, filter_provider, filter_cost))) {
      LOG_WARN("failed to install optional filter bitmap provider", K(ret), K(i));
    } else if (OB_NOT_NULL(bmm_options)
               && OB_FAIL(clause->set_bmm_input(
                   query_optional, filter_op, filter_cost, *bmm_options))) {
      LOG_WARN("failed to install optional BMM input", K(ret));
    } else if (OB_NOT_NULL(filter_op)
               && OB_ISNULL(bmm_options)
               && FALSE_IT(clause->set_pushdown_filter(query_optional, filter_op, filter_cost))) {
    } else if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, optional_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (OB_FAIL(optional_ops.push_back(optional_op))) {
      LOG_WARN("failed to push back optional op", KR(ret));
    } else if (need_optional_cost && OB_FAIL(optional_costs.push_back(optional_cost))) {
      LOG_WARN("failed to push back optional cost", KR(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (optional_ops.size() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected optional clauses", KR(ret), K(optional_ops.size()));
  } else if (1 == optional_ops.size()) {
    op = optional_ops.at(0);
  } else if (need_score && is_top_level) {
    if (min_should_match > 1) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_op_param(optional_ops, min_should_match, allocator_);
      bmw_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
      if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
        LOG_WARN("failed to create bmw op", KR(ret));
      } else {
        op = bmw_op;
      }
    } else {
      ObDASBMMOp *bmm_op = nullptr;
      ObDASBMMBuildOptions build_options;
      build_options.lead_cost_ = lead_cost;
      ObDASBMMOpParam bmm_op_param(
          optional_ops, nullptr, allocator_, build_options, &optional_costs);
      bmm_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
      if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
        LOG_WARN("failed to create bmm op", KR(ret));
      } else {
        op = bmm_op;
      }
    }
  } else if (need_score) {
    ObDASDisjunctionOp *disjunction_op = nullptr;
    ObDASDisjunctionOpParam disjunction_op_param(optional_ops, min_should_match, false, lead_cost);
    disjunction_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
    if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
      LOG_WARN("failed to create disjunction op", K(ret));
    } else {
      op = disjunction_op;
    }
  } else {
    ObDASDisjunctionFilterOp *disjunction_filter_op = nullptr;
    ObDASDisjunctionFilterOpParam disjunction_filter_op_param(optional_ops, min_should_match, lead_cost);
    disjunction_filter_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
    if (OB_FAIL(search_ctx.create_op(disjunction_filter_op_param, disjunction_filter_op))) {
      LOG_WARN("failed to create disjunction filter op", KR(ret));
    } else {
      op = disjunction_filter_op;
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::excl(ObIDASSearchOp *main,
                                 const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited,
                                 ObDASSearchCost lead_cost,
                                 ObDASSearchCtx &search_ctx,
                                 ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(main)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr main op", KR(ret));
  } else if (prohibited.empty()) {
    op = main;
  } else if (is_async()) {
    // async fulltext: the must_not clause is the shared delete index; materialize it as the
    // dedicated deletion op and exclude via ReqExclOp (per-rowid on-demand probing).
    ObIDASSearchOp *deletion_op = nullptr;
    if (OB_FAIL(generate_async_deletion_op(prohibited, search_ctx, deletion_op))) {
      LOG_WARN("failed to generate async deletion op", KR(ret));
    } else if (OB_FAIL(generate_req_excl_op(main, deletion_op, search_ctx, op))) {
      LOG_WARN("failed to generate req excl op for async delete", KR(ret));
    }
  } else {
    ObIDASSearchOp *optional_op = nullptr;
    if (OB_FAIL(opt(prohibited, 1 /*min_should_match*/, false /*need_score*/, lead_cost, false /*is_top_level*/, search_ctx, optional_op))) {
      LOG_WARN("failed to generate optional op", KR(ret));
    } else if (OB_FAIL(generate_req_excl_op(main, optional_op, search_ctx, op))) {
      LOG_WARN("failed to generate req excl op", KR(ret));
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_conjunction_op(const ObArray<ObIDASSearchOp *> &required,
                                                    ObDASSearchCtx &search_ctx,
                                                    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASConjunctionOp *conjunction_op = nullptr;
  ObDASConjunctionOpParam conjunction_op_param(required);
  conjunction_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
  if (OB_FAIL(search_ctx.create_op(conjunction_op_param, conjunction_op))) {
    LOG_WARN("failed to create conjunction op", K(ret));
  } else {
    op = conjunction_op;
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_disjunction_op(const ObArray<ObIDASSearchOp *> &optional_ops,
                                                    const int64_t min_should_match,
                                                    ObDASSearchCost lead_cost,
                                                    ObDASSearchCtx &search_ctx,
                                                    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASDisjunctionOp *disjunction_op = nullptr;
  ObDASDisjunctionOpParam disjunction_op_param(optional_ops, min_should_match, false, lead_cost);
  disjunction_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
  if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
    LOG_WARN("failed to create disjunction op", K(ret));
  } else {
    op = disjunction_op;
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_req_excl_op(ObIDASSearchOp *required,
                                                 ObIDASSearchOp *excluded,
                                                 ObDASSearchCtx &search_ctx,
                                                 ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASReqExclOp *req_excl_op = nullptr;
  ObDASReqExclOpParam req_excl_op_param(required, excluded);
  req_excl_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
  if (OB_FAIL(search_ctx.create_op(req_excl_op_param, req_excl_op))) {
    LOG_WARN("failed to create req excl op", K(ret));
  } else {
    op = req_excl_op;
  }
  return ret;
}

bool ObDASBooleanQueryRtDef::is_async() const
{
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  return OB_NOT_NULL(ctdef) && ctdef->is_async();
}

int ObDASBooleanQueryRtDef::resolve_async_fts_rtdef(
    ObIDASSearchRtDef &query_rtdef,
    ObIDASSearchRtDef *&fts_rtdef) const
{
  int ret = OB_SUCCESS;
  fts_rtdef = nullptr;
  const ObDASBaseCtDef *base_ctdef = query_rtdef.ctdef_;
  if (DAS_OP_MATCH_QUERY == query_rtdef.op_type_) {
    if (OB_ISNULL(base_ctdef)
        || OB_UNLIKELY(DAS_OP_MATCH_QUERY != base_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid direct match ctdef", K(ret), KP(base_ctdef));
    } else {
      const ObDASMatchCtDef *match_ctdef =
          static_cast<const ObDASMatchCtDef *>(base_ctdef);
      const ObDASIRScanCtDef *ir_ctdef = match_ctdef->get_ir_ctdef();
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ir ctdef for direct match", K(ret));
      } else if (ir_ctdef->is_async_index_) {
        fts_rtdef = &query_rtdef;
      }
    }
  } else if (DAS_OP_MULTI_MATCH_QUERY == query_rtdef.op_type_) {
    if (OB_ISNULL(base_ctdef)
        || OB_UNLIKELY(DAS_OP_MULTI_MATCH_QUERY != base_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid direct multi match ctdef", K(ret), KP(base_ctdef));
    } else {
      const ObDASMultiMatchCtDef *multi_match_ctdef =
          static_cast<const ObDASMultiMatchCtDef *>(base_ctdef);
      const ObDASIRScanCtDef *ir_ctdef = multi_match_ctdef->get_ir_ctdef(0);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid direct multi match ir ctdef", K(ret));
      } else if (ir_ctdef->is_async_index_) {
        fts_rtdef = &query_rtdef;
      }
    }
  } else if (DAS_OP_QUERY_STRING_QUERY == query_rtdef.op_type_) {
    if (OB_ISNULL(base_ctdef)
        || OB_UNLIKELY(DAS_OP_QUERY_STRING_QUERY != base_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid direct query-string ctdef", K(ret), KP(base_ctdef));
    } else {
      const ObDASQueryStringCtDef *query_string_ctdef =
          static_cast<const ObDASQueryStringCtDef *>(base_ctdef);
      const ObDASIRScanCtDef *ir_ctdef = query_string_ctdef->get_ir_ctdef(0);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid direct query-string ir ctdef", K(ret));
      } else if (ir_ctdef->is_async_index_) {
        fts_rtdef = &query_rtdef;
      }
    }
  } else if (DAS_OP_BOOLEAN_QUERY == query_rtdef.op_type_) {
    if (OB_ISNULL(base_ctdef)
        || OB_UNLIKELY(DAS_OP_BOOLEAN_QUERY != base_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid boolean wrapper ctdef", K(ret), KP(base_ctdef));
    } else {
      // Async fulltext wraps its query inside BooleanQuery (must=[query], must_not=[delete]).
      ObDASBooleanQueryRtDef *bool_rtdef =
          static_cast<ObDASBooleanQueryRtDef *>(&query_rtdef);
      if (bool_rtdef->is_async()
          && OB_FAIL(bool_rtdef->get_async_fts_filter_pushdown_target(fts_rtdef))) {
        LOG_WARN("failed to get async fts query under boolean wrap", KR(ret));
      }
    }
  }
  return ret;
}

void ObDASBooleanQueryRtDef::move_selected_probe_after_drivers(
    ObIArray<ObIDASSearchRtDef *> &required_clauses,
    ObIDASSearchRtDef *probe_clause)
{
  if (OB_NOT_NULL(probe_clause)) {
    int64_t probe_idx = -1;
    for (int64_t i = 0; i < required_clauses.count() && probe_idx < 0; ++i) {
      if (required_clauses.at(i) == probe_clause) {
        probe_idx = i;
      }
    }
    if (probe_idx >= 0) {
      for (int64_t i = probe_idx; i + 1 < required_clauses.count(); ++i) {
        required_clauses.at(i) = required_clauses.at(i + 1);
      }
      required_clauses.at(required_clauses.count() - 1) = probe_clause;
    }
  }
}

bool ObDASBooleanQueryRtDef::is_topk_range_probe_cost_effective(
    int64_t match_rows, int64_t filter_rows, int64_t table_rows,
    int64_t topk, int64_t primary_get_ratio)
{
  bool prefer_probe = false;
  if (match_rows > 0 && filter_rows > 0 && table_rows > 0
      && filter_rows <= table_rows && topk > 0 && topk <= TOPK_PROBE_MAX_LIMIT
      && primary_get_ratio > 0) {
    const double candidate_budget = static_cast<double>(
        std::max(TOPK_PROBE_MIN_CANDIDATES, topk * TOPK_PROBE_CANDIDATES_PER_HIT));
    const double selectivity = static_cast<double>(filter_rows) / table_rows;
    // Ranking produces more competitive candidates than K. Account for rejected
    // candidates and for a point get costing more than one cached range row.
    const double candidate_rows = std::min(static_cast<double>(match_rows), candidate_budget / selectivity);
    // Keep the legacy gate unchanged and scale only this estimate.
    const double get_to_scan_ratio = std::max(
        static_cast<double>(TOPK_PROBE_REFERENCE_GET_TO_SCAN_RATIO),
        static_cast<double>(TOPK_PROBE_REFERENCE_GET_TO_SCAN_RATIO)
            * static_cast<double>(primary_get_ratio) / TOPK_PROBE_REFERENCE_OPTION_RATIO);
    prefer_probe = candidate_rows <= static_cast<double>(filter_rows) / get_to_scan_ratio;
  }
  return prefer_probe;
}

int ObDASBooleanQueryRtDef::choose_topk_range_probe(
    ObIDASSearchRtDef &must_clause,
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchRtDef *&probe_clause) const
{
  int ret = OB_SUCCESS;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  ObIDASSearchRtDef *filter_rtdef = nullptr;
  probe_clause = nullptr;
  // Docid slices bound the MATCH driver. The scalar only probes its candidates,
  // so worker trees can retain the same Top-K primary-probe choice.
  if (1 != filter_clauses.count() || !search_ctx.use_dynamic_pruning()
      || search_ctx.get_generating_topk_limit() <= 0
      || search_ctx.get_generating_topk_limit() > TOPK_PROBE_MAX_LIMIT
      || DAS_SCOPE_BASE != search_ctx.get_op_creating_scope()
      || search_ctx.has_nested_scope()
      || search_ctx.is_logical_table() || search_ctx.is_vec_index_search()
      || DAS_ROWID_TYPE_UINT64 != search_ctx.get_rowid_type()) {
  } else if (OB_FAIL(resolve_async_fts_rtdef(must_clause, fts_rtdef))) {
    LOG_WARN("failed to resolve topk match clause", KR(ret));
  } else if (OB_ISNULL(fts_rtdef) || DAS_OP_MATCH_QUERY != fts_rtdef->op_type_
      || OB_ISNULL(fts_rtdef->ctdef_)
      || !static_cast<const ObIDASSearchCtDef *>(fts_rtdef->ctdef_)->is_scoring()) {
  } else if (OB_FAIL(filter_clauses.get(0, filter_rtdef))) {
    LOG_WARN("failed to get topk filter clause", KR(ret));
  } else if (OB_NOT_NULL(filter_rtdef) && DAS_OP_SCALAR_QUERY == filter_rtdef->op_type_) {
    ObDASScalarRtDef *scalar = static_cast<ObDASScalarRtDef *>(filter_rtdef);
    const ObDASScalarCtDef *ctdef = scalar->get_ctdef();
    const ObDASScalarScanCtDef *index = OB_NOT_NULL(ctdef) ? ctdef->get_index_scan_ctdef() : nullptr;
    ObDASSearchCost match_cost;
    ObDASSearchCost filter_cost;
    if (OB_ISNULL(ctdef) || OB_ISNULL(index) || ctdef->is_scoring()
        || !ctdef->has_main_scan() || !index->is_async_index_
        || index->is_async_ror_scan() || ctdef->get_primary_get_ratio() <= 0) {
    } else if (OB_FAIL(must_clause.get_cost(search_ctx, match_cost))) {
      LOG_WARN("failed to get topk match cost", KR(ret));
    } else if (OB_FAIL(scalar->get_cost(search_ctx, filter_cost))) {
      LOG_WARN("failed to get topk range cost", KR(ret));
    } else {
      const int64_t match_rows = match_cost.cost();
      const int64_t filter_rows = filter_cost.cost();
      const int64_t table_rows = search_ctx.get_row_count().cost();
      const int64_t topk = search_ctx.get_generating_topk_limit();
      const int64_t primary_get_ratio = ctdef->get_primary_get_ratio();
      LOG_TRACE("try topk range probe optimization", K(topk), K(match_rows),
          K(filter_rows), K(table_rows), K(primary_get_ratio));
      if (is_topk_range_probe_cost_effective(
          match_rows, filter_rows, table_rows, topk, primary_get_ratio)) {
        probe_clause = filter_rtdef;
        LOG_TRACE("selected topk range probe optimization");
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::should_skip_bmm_for_async_fts_primary_get(
    ObIDASSearchRtDef &must_clause,
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    const ObDASSearchCost &lead_cost,
    ObDASSearchCtx &search_ctx,
    bool &skip_bmm) const
{
  int ret = OB_SUCCESS;
  skip_bmm = false;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  ObArray<ObIDASSearchRtDef *> required_clauses;
  if (DAS_ROWID_TYPE_UINT64 != search_ctx.get_rowid_type()) {
    // Primary-get probe currently requires UINT64 rowid (HEAP), same as knn+filter.
  } else if (OB_FAIL(resolve_async_fts_rtdef(must_clause, fts_rtdef))) {
    LOG_WARN("failed to resolve async fts rtdef", KR(ret));
  } else if (OB_ISNULL(fts_rtdef)) {
    // Non-async-fulltext must: keep default BMM pushdown.
  } else if (OB_FAIL(required_clauses.push_back(&must_clause))) {
    LOG_WARN("failed to push async fts required clause", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_clauses.count(); ++i) {
      ObIDASSearchRtDef *filter_clause = nullptr;
      if (OB_FAIL(filter_clauses.get(i, filter_clause))) {
        LOG_WARN("failed to get filter clause", KR(ret), K(i));
      } else if (OB_ISNULL(filter_clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null filter clause", KR(ret), K(i));
      } else if (OB_FAIL(required_clauses.push_back(filter_clause))) {
        LOG_WARN("failed to push filter required clause", KR(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(fts_rtdef)) {
    // Use the exact clause order and lead cost that req() will use after BMM is skipped.
    // Only direct non-driver scalar clauses receive allow_probe in req().
    ObDASRtDefCostComparator comparator(search_ctx);
    lib::ob_sort(required_clauses.begin(), required_clauses.end(), comparator);
    for (int64_t i = 1; OB_SUCC(ret) && !skip_bmm && i < required_clauses.count(); ++i) {
      ObIDASSearchRtDef *clause = required_clauses.at(i);
      if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sorted required clause", KR(ret), K(i));
      } else if (DAS_OP_SCALAR_QUERY == clause->op_type_) {
        ObDASScalarRtDef *scalar = static_cast<ObDASScalarRtDef *>(clause);
        const ObDASScalarCtDef *scalar_ctdef = scalar->get_ctdef();
        const ObDASScalarScanCtDef *index_scan_ctdef =
            OB_NOT_NULL(scalar_ctdef) ? scalar_ctdef->get_index_scan_ctdef() : nullptr;
        bool prefer_probe = false;
        if (OB_NOT_NULL(scalar_ctdef) && scalar_ctdef->has_main_scan()) {
          if (OB_FAIL(scalar->prefer_primary_probe(lead_cost, search_ctx, prefer_probe))) {
            LOG_WARN("failed to check filter primary probe preference", KR(ret), K(i));
          } else if (prefer_probe) {
            skip_bmm = true;
            LOG_TRACE("skip bmm because non-driver filter prefers primary get probe",
                K(lead_cost.cost()), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::can_pushdown_filter_to_bmm(
    ObDASSearchCtx &search_ctx,
    bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  if (OB_FAIL(get_async_fts_filter_pushdown_target(fts_rtdef))) {
    LOG_WARN("failed to get async fts filter pushdown target", K(ret));
  } else if (OB_NOT_NULL(fts_rtdef)
             && OB_FAIL(fts_rtdef->can_pushdown_filter_to_bmm(
                 search_ctx, can_pushdown))) {
    LOG_WARN("failed to check async fts filter pushdown", K(ret));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::get_bmm_filter_consumer_counts(
    ObDASSearchCtx &search_ctx,
    int64_t &group_consumer_count,
    int64_t &query_consumer_count)
{
  int ret = OB_SUCCESS;
  group_consumer_count = 0;
  query_consumer_count = 0;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  if (OB_FAIL(get_async_fts_filter_pushdown_target(fts_rtdef))) {
    LOG_WARN("failed to get async fts filter consumer target", K(ret));
  } else if (OB_ISNULL(fts_rtdef)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("boolean query has no BMM filter consumer target", K(ret));
  } else if (OB_FAIL(fts_rtdef->get_bmm_filter_consumer_counts(
                 search_ctx, group_consumer_count, query_consumer_count))) {
    LOG_WARN("failed to get async fts filter consumer counts", K(ret));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::set_bmm_input(
    const bool query_optional,
    ObIDASSearchOp *filter_op,
    ObDASSearchCost filter_cost,
    const ObDASBMMBuildOptions &options)
{
  int ret = OB_SUCCESS;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  if (OB_FAIL(get_async_fts_filter_pushdown_target(fts_rtdef))) {
    LOG_WARN("failed to resolve BMM input target", K(ret));
  } else if (OB_ISNULL(fts_rtdef)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(fts_rtdef->set_bmm_input(
      query_optional, filter_op, filter_cost, options))) {
    LOG_WARN("failed to install BMM input", K(ret));
  }
  return ret;
}

void ObDASBooleanQueryRtDef::set_pushdown_filter(
    const bool query_optional,
    ObIDASSearchOp *filter_op,
    ObDASSearchCost filter_cost)
{
  int ret = OB_SUCCESS;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  if (OB_FAIL(get_async_fts_filter_pushdown_target(fts_rtdef))) {
    LOG_WARN("failed to get async fts filter pushdown target", K(ret));
  } else if (OB_NOT_NULL(fts_rtdef)) {
    fts_rtdef->set_pushdown_filter(query_optional, filter_op, filter_cost);
  }
}

int ObDASBooleanQueryRtDef::set_pushdown_filter_provider(
    const bool query_optional,
    ObDASFilterBitmapProvider *provider,
    ObDASSearchCost filter_cost)
{
  int ret = OB_SUCCESS;
  ObIDASSearchRtDef *fts_rtdef = nullptr;
  if (OB_FAIL(get_async_fts_filter_pushdown_target(fts_rtdef))) {
    LOG_WARN("failed to get async fts filter provider target", K(ret));
  } else if (OB_ISNULL(fts_rtdef)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("boolean query has no filter bitmap provider target", K(ret));
  } else if (OB_FAIL(fts_rtdef->set_pushdown_filter_provider(
                 query_optional, provider, filter_cost))) {
    LOG_WARN("failed to install async fts filter provider", K(ret));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::get_async_fts_filter_pushdown_target(
    ObIDASSearchRtDef *&fts_rtdef) const
{
  int ret = OB_SUCCESS;
  fts_rtdef = nullptr;
  ObBooleanSubClause<ObIDASSearchRtDef> must_clauses;
  ObIDASSearchRtDef *must_clause = nullptr;
  if (OB_ISNULL(ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null boolean ctdef for async fts filter pushdown", K(ret));
  } else if (!is_async()) {
    // skip
  } else if (OB_FAIL(must(must_clauses))) {
    LOG_WARN("failed to get async boolean must clauses", K(ret));
  } else if (OB_UNLIKELY(1 != must_clauses.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async boolean must clause count", K(ret), K(must_clauses.count()));
  } else if (OB_FAIL(must_clauses.get(0, must_clause))) {
    LOG_WARN("failed to get async boolean must clause", K(ret));
  } else if (OB_ISNULL(must_clause)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async boolean must clause", K(ret));
  } else if (DAS_OP_MATCH_QUERY == must_clause->op_type_) {
    if (OB_ISNULL(must_clause->ctdef_)
        || OB_UNLIKELY(DAS_OP_MATCH_QUERY != must_clause->ctdef_->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async match ctdef", K(ret), KP(must_clause->ctdef_));
    } else {
      const ObDASMatchCtDef *match_ctdef =
          static_cast<const ObDASMatchCtDef *>(must_clause->ctdef_);
      const ObDASIRScanCtDef *ir_ctdef = match_ctdef->get_ir_ctdef();
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ir ctdef for async match", K(ret));
      } else if (OB_UNLIKELY(!ir_ctdef->is_async_index_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-async ir ctdef under async boolean", K(ret), K(ir_ctdef->is_async_index_));
      } else {
        fts_rtdef = must_clause;
      }
    }
  } else if (DAS_OP_MULTI_MATCH_QUERY == must_clause->op_type_) {
    if (OB_ISNULL(must_clause->ctdef_)
        || OB_UNLIKELY(DAS_OP_MULTI_MATCH_QUERY != must_clause->ctdef_->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async multi match ctdef", K(ret), KP(must_clause->ctdef_));
    } else {
      const ObDASMultiMatchCtDef *multi_match_ctdef =
          static_cast<const ObDASMultiMatchCtDef *>(must_clause->ctdef_);
      const ObDASIRScanCtDef *ir_ctdef = multi_match_ctdef->get_ir_ctdef(0);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid wrapped multi match ir ctdef", K(ret));
      } else if (OB_UNLIKELY(!ir_ctdef->is_async_index_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-async multi match under async boolean", K(ret));
      } else {
        fts_rtdef = must_clause;
      }
    }
  } else if (DAS_OP_QUERY_STRING_QUERY == must_clause->op_type_) {
    if (OB_ISNULL(must_clause->ctdef_)
        || OB_UNLIKELY(DAS_OP_QUERY_STRING_QUERY != must_clause->ctdef_->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async query-string ctdef", K(ret), KP(must_clause->ctdef_));
    } else {
      const ObDASQueryStringCtDef *query_string_ctdef =
          static_cast<const ObDASQueryStringCtDef *>(must_clause->ctdef_);
      const ObDASIRScanCtDef *ir_ctdef = query_string_ctdef->get_ir_ctdef(0);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid wrapped query-string ir ctdef", K(ret));
      } else if (OB_UNLIKELY(!ir_ctdef->is_async_index_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-async query-string under async boolean", K(ret));
      } else {
        fts_rtdef = must_clause;
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_async_deletion_op(
    const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  // The async must_not clause is the shared delete index scalar scan. Materialize it as the
  // dedicated ObDASAsyncIndexDeletionOp (deletion bitmap), not a plain scalar scan.
  ObIDASSearchRtDef *clause = nullptr;
  if (OB_UNLIKELY(prohibited.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async delete must_not must have exactly one clause", K(ret), K(prohibited.count()));
  } else if (OB_FAIL(prohibited.get(0, clause))) {
    LOG_WARN("failed to get prohibited clause", K(ret));
  } else if (OB_ISNULL(clause) || OB_UNLIKELY(DAS_OP_SCALAR_SCAN_QUERY != clause->op_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async delete clause", K(ret), KP(clause));
  } else {
    ObDASScalarScanRtDef *scan_rtdef = static_cast<ObDASScalarScanRtDef *>(clause);
    const ObDASScalarScanCtDef *scan_ctdef = scan_rtdef->get_ctdef();
    ObDASAsyncIndexDeletionOp *deletion_op = nullptr;
    ObDASAsyncIndexDeletionOpParam deletion_param(scan_ctdef, scan_rtdef);
    deletion_param.set_is_scoring(false);
    if (OB_ISNULL(scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null delete scan ctdef", K(ret));
    } else if (OB_FAIL(search_ctx.create_op(deletion_param, deletion_op))) {
      LOG_WARN("failed to create async index deletion op", K(ret));
    } else {
      op = deletion_op;
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_req_opt_op(ObIDASSearchOp *required,
                                                ObIDASSearchOp *optional,
                                                ObDASSearchCtx &search_ctx,
                                                ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASReqOptOp *req_opt_op = nullptr;
  ObDASReqOptOpParam req_opt_op_param(required, optional, true);
  req_opt_op_param.set_is_scoring(static_cast<const ObIDASSearchCtDef *>(ctdef_)->is_scoring());
  if (OB_FAIL(search_ctx.create_op(req_opt_op_param, req_opt_op))) {
    LOG_WARN("failed to create req opt op", K(ret));
  } else {
    op = req_opt_op;
  }
  return ret;
}

int ObDASBooleanQueryCtDef::must(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (must_.offset_ + must_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(must_.offset_), K(must_.count_), K(children_cnt_));
  } else if (must_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + must_.offset_), must_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::filter(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (filter_.offset_ + filter_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(filter_.offset_), K(filter_.count_), K(children_cnt_));
  } else if (filter_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + filter_.offset_), filter_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::should(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (should_.offset_ + should_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(should_.offset_), K(should_.count_), K(children_cnt_));
  } else if (should_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + should_.offset_), should_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryCtDef::must_not(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const
{
  int ret = OB_SUCCESS;
  if (must_not_.offset_ + must_not_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(must_not_.offset_), K(must_not_.count_), K(children_cnt_));
  } else if (must_not_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>((ObIDASSearchCtDef **)(children_ + must_not_.offset_), must_not_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchCtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::must(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->must_.offset_ + ctdef->must_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->must_.offset_), K(ctdef->must_.count_), K(children_cnt_));
  } else if (ctdef->must_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->must_.offset_), ctdef->must_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::filter(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->filter_.offset_ + ctdef->filter_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->filter_.offset_), K(ctdef->filter_.count_), K(children_cnt_));
  } else if (ctdef->filter_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->filter_.offset_), ctdef->filter_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::should(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->should_.offset_ + ctdef->should_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->should_.offset_), K(ctdef->should_.count_), K(children_cnt_));
  } else if (ctdef->should_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->should_.offset_), ctdef->should_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::must_not(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const
{
  int ret = OB_SUCCESS;
  const ObDASBooleanQueryCtDef *ctdef = static_cast<const ObDASBooleanQueryCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", KR(ret));
  } else if (ctdef->must_not_.offset_ + ctdef->must_not_.count_ > children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out of range", KR(ret), K(ctdef->must_not_.offset_), K(ctdef->must_not_.count_), K(children_cnt_));
  } else if (ctdef->must_not_.exist_) {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>((ObIDASSearchRtDef **)(children_ + ctdef->must_not_.offset_), ctdef->must_not_.count_);
  } else {
    clause = ObBooleanSubClause<ObIDASSearchRtDef>();
  }
  return ret;
}

int ObDASBooleanQueryRtDef::estimate_filter_cost(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &filter_cost)
{
  int ret = OB_SUCCESS;
  ObDASGroupFilterDomainGuard filter_domain_guard(search_ctx);
  filter_cost.reset();
  bool collect_cost = true;
  if (filter_clauses.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty filter clauses", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_clauses.count(); ++i) {
    ObIDASSearchRtDef *clause = nullptr;
    if (OB_FAIL(filter_clauses.get(i, clause))) {
      LOG_WARN("failed to get filter clause", K(ret), K(i));
    } else if (OB_ISNULL(clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter clause", K(ret), K(i));
    } else if (collect_cost) {
      int cost_ret = OB_SUCCESS;
      ObDASSearchCost clause_cost;
      if (OB_SUCCESS != (cost_ret = clause->get_cost(search_ctx, clause_cost))) {
        LOG_WARN("failed to get optional filter cost metadata", K(cost_ret), K(i));
      } else if (!clause_cost.is_valid()) {
        cost_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid optional filter cost metadata", K(cost_ret), K(i));
      } else if (!filter_cost.is_valid()) {
        filter_cost = clause_cost;
      } else if (OB_SUCCESS != (cost_ret = ObDASSearchCost::min(
                   filter_cost, clause_cost, filter_cost))) {
        LOG_WARN("failed to aggregate optional filter cost metadata", K(cost_ret), K(i));
      }
      if (OB_SUCCESS != cost_ret) {
        // Unknown metadata must not fail Filter generation or leave a misleading
        // partial estimate. BMM consumers must handle an invalid cost explicitly.
        filter_cost.reset();
        collect_cost = false;
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::prepare_bmm_input(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&filter_op,
    ObDASSearchCost &filter_cost,
    const bool query_optional,
    ObDASBMMBuildOptions &bmm_options,
    bool &has_bmm_options)
{
  int ret = OB_SUCCESS;
  ObDASGroupFilterDomainGuard filter_domain_guard(search_ctx);
  filter_op = nullptr;
  has_bmm_options = false;
  bmm_options = ObDASBMMBuildOptions(false);
  ObIDASSearchRtDef *single_clause = nullptr;
  if (OB_FAIL(estimate_filter_cost(filter_clauses, search_ctx, filter_cost))) {
    LOG_WARN("failed to estimate direct filter cost", K(ret));
  } else if (1 == filter_clauses.count()) {
    if (OB_FAIL(filter_clauses.get(0, single_clause))) {
      LOG_WARN("failed to get single filter clause", K(ret));
    } else if (OB_ISNULL(single_clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null single filter clause", K(ret));
    } else {
      // RtDefs are reused across Groups. A former conjunction follower may
      // allow primary probe, but a single BMM input must enumerate its matches.
      single_clause->set_allow_probe(false);
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(single_clause)
      && DAS_OP_SCALAR_QUERY == single_clause->op_type_) {
    const bool supports_direct_drain =
        static_cast<ObDASScalarRtDef *>(single_clause)->can_build_direct_rowid_drain(search_ctx);
    ObDASBMMOp::select_filter_build_options(search_ctx, lead_cost, filter_cost,
        query_optional, supports_direct_drain, bmm_options);
    has_bmm_options = true;
  }
  if (OB_FAIL(ret)) {
  } else if (has_bmm_options) {
    if (OB_FAIL(static_cast<ObDASScalarRtDef *>(single_clause)->generate_op(
        lead_cost, search_ctx,
        ObDASBMMBuildOptions::FilterMode::ORDERED != bmm_options.filter_mode_, filter_op))) {
      LOG_WARN("failed to generate BMM filter", K(ret));
    }
  } else if (1 == filter_clauses.count()) {
    if (OB_FAIL(single_clause->generate_op(lead_cost, search_ctx, filter_op))) {
      LOG_WARN("failed to generate filter op", KR(ret));
    }
  } else {
    // Reuse req() to enable probe mode for excessive filter clauses.
    ObBooleanSubClause<ObIDASSearchRtDef> empty_must_clauses;
    if (OB_FAIL(req(empty_must_clauses,
                    filter_clauses,
                    lead_cost,
                    false /* is_top_level */,
                    search_ctx,
                    filter_op))) {
      LOG_WARN("failed to generate conjunctive filter op", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    filter_op = nullptr;
    has_bmm_options = false;
    bmm_options = ObDASBMMBuildOptions(false);
  }
  return ret;
}

int ObDASBooleanQueryRtDef::generate_materialized_filter_op(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    const bool allow_direct_drain,
    ObIDASSearchOp *&filter_op)
{
  int ret = OB_SUCCESS;
  ObDASGroupFilterDomainGuard filter_domain_guard(search_ctx);
  ObArray<ObIDASSearchOp *> filter_ops;
  filter_op = nullptr;
  if (filter_clauses.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("materialized filter clauses are empty", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_clauses.count(); ++i) {
    ObIDASSearchRtDef *clause = nullptr;
    ObIDASSearchOp *clause_op = nullptr;
    if (OB_FAIL(filter_clauses.get(i, clause))) {
      LOG_WARN("failed to get materialized filter clause", K(ret), K(i));
    } else if (OB_ISNULL(clause)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("materialized filter clause is null", K(ret), K(i));
    }
    if (OB_SUCC(ret)) {
      // Provider sources must materialize their complete match set. Explicitly
      // disable candidate-relative primary probe generation.
      clause->set_allow_probe(false);
      const bool use_direct_drain = allow_direct_drain
          && 1 == filter_clauses.count()
          && DAS_OP_SCALAR_QUERY == clause->op_type_
          && static_cast<ObDASScalarRtDef *>(clause)->can_build_direct_rowid_drain(search_ctx);
      if (OB_FAIL(use_direct_drain
          ? static_cast<ObDASScalarRtDef *>(clause)->generate_op(
                lead_cost, search_ctx, true, clause_op)
          : clause->generate_op(lead_cost, search_ctx, clause_op))) {
        LOG_WARN("failed to generate materialized filter clause", K(ret), K(i),
            K(use_direct_drain));
      } else if (OB_ISNULL(clause_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("materialized filter clause op is null", K(ret), K(i));
      } else if (OB_FAIL(filter_ops.push_back(clause_op))) {
        LOG_WARN("failed to append materialized filter clause op", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (1 == filter_ops.count()) {
    filter_op = filter_ops.at(0);
  } else if (OB_FAIL(generate_conjunction_op(filter_ops, search_ctx, filter_op))) {
    LOG_WARN("failed to generate materialized conjunctive filter", K(ret));
  }
  return ret;
}

int ObDASBooleanQueryRtDef::try_get_filter_bitmap_provider(
    const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    const ObDASFilterBitmapProviderScope scope,
    const int64_t group_slot,
    ObDASFilterBitmapProvider *&provider)
{
  int ret = OB_SUCCESS;
  provider = search_ctx.find_filter_bitmap_provider(this, scope, group_slot);
  if (OB_NOT_NULL(provider)) {
    // Reuse the provider created by an earlier consumer in this scope.
  } else if (OB_UNLIKELY((DAS_FILTER_BITMAP_QUERY_SCOPE != scope
                          && DAS_FILTER_BITMAP_GROUP_SCOPE != scope)
                         || (DAS_FILTER_BITMAP_QUERY_SCOPE == scope
                             && group_slot != DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT)
                         || (DAS_FILTER_BITMAP_GROUP_SCOPE == scope
                             && (group_slot < 0
                                 || group_slot != search_ctx.get_async_group_slot())))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("filter bitmap provider scope does not match current binding",
             K(ret), K(scope), K(group_slot),
             "current_group_slot", search_ctx.get_async_group_slot());
  } else {
    ObIDASSearchOp *source_op = nullptr;
    ObDASAsyncBindingGuard source_binding_guard(search_ctx);
    if (DAS_FILTER_BITMAP_GROUP_SCOPE == scope) {
      // A detached source owns its SegmentMerge; it must not inherit the
      // consuming Boolean's per-Segment index claim.
      source_binding_guard.set(OB_INVALID_ID, group_slot, false);
    }
    if (OB_FAIL(generate_materialized_filter_op(
            filter_clauses, lead_cost, search_ctx,
            true /* allow_direct_drain */, source_op))) {
      LOG_WARN("failed to generate materialized filter provider source", K(ret));
    } else if (OB_ISNULL(source_op)
               || (!source_op->can_enumerate_rowids()
                   && !source_op->supports_direct_rowid_drain())) {
      // Keep the original conjunction path for probe-only or otherwise
      // candidate-relative filters.
      LOG_TRACE("filter source cannot be lifted into a bitmap provider",
                KP(source_op));
    } else {
      const int provider_ret = search_ctx.create_filter_bitmap_provider(
          this, scope, group_slot, source_op, provider);
      if (OB_NOT_SUPPORTED == provider_ret) {
        LOG_TRACE("filter bitmap provider is not supported for this source",
                  K(provider_ret), "source_type", source_op->get_op_type());
      } else if (OB_SUCCESS != provider_ret) {
        ret = provider_ret;
        LOG_WARN("failed to create filter bitmap provider", K(ret));
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::try_get_mixed_filter_bitmap_provider(
    ObDASFilterBitmapProvider &sync_provider,
    const ObBooleanSubClause<ObIDASSearchRtDef> &async_filter_clauses,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObDASFilterBitmapProvider *&provider)
{
  int ret = OB_SUCCESS;
  const int64_t group_slot = search_ctx.get_async_group_slot();
  provider = search_ctx.find_filter_bitmap_provider(
      this, DAS_FILTER_BITMAP_GROUP_SCOPE, group_slot);
  if (OB_NOT_NULL(provider)) {
    // Reuse the mixed provider created while generating an earlier BMM in this Group.
  } else if (OB_UNLIKELY(group_slot < 0 || async_filter_clauses.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mixed filter provider arguments", K(ret),
             K(group_slot), K(async_filter_clauses.count()));
  } else {
    ObDASBitmapOp *sync_view = nullptr;
    ObIDASSearchOp *async_filter_op = nullptr;
    ObIDASSearchOp *source_op = nullptr;
    ObArray<ObIDASSearchOp *> filter_ops;
    ObDASAsyncBindingGuard source_binding_guard(search_ctx);
    // The async component must independently cover this Group's Segments.
    source_binding_guard.set(OB_INVALID_ID, group_slot, false);
    if (OB_FAIL(sync_provider.create_view(search_ctx, sync_view))) {
      LOG_WARN("failed to create query-scope sync filter view for mixed provider", K(ret));
    } else if (OB_FAIL(generate_materialized_filter_op(
                   async_filter_clauses, lead_cost, search_ctx,
                   false /* allow_direct_drain */, async_filter_op))) {
      LOG_WARN("failed to generate Group-scope async filter source", K(ret));
    } else if (OB_ISNULL(async_filter_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async filter source is null for mixed provider", K(ret));
    } else if (OB_FAIL(filter_ops.push_back(sync_view))) {
      LOG_WARN("failed to append sync filter view to mixed provider", K(ret));
    } else if (OB_FAIL(filter_ops.push_back(async_filter_op))) {
      LOG_WARN("failed to append async filter source to mixed provider", K(ret));
    } else if (OB_FAIL(generate_conjunction_op(filter_ops, search_ctx, source_op))) {
      LOG_WARN("failed to generate mixed filter provider conjunction", K(ret));
    } else {
      if (OB_ISNULL(source_op)
          || (!source_op->can_enumerate_rowids()
              && !source_op->supports_direct_rowid_drain())) {
        LOG_TRACE("mixed filter source cannot be lifted into a bitmap provider",
                  KP(source_op));
      } else {
        const int provider_ret = search_ctx.create_filter_bitmap_provider(
            this, DAS_FILTER_BITMAP_GROUP_SCOPE, group_slot,
            source_op, provider);
        if (OB_NOT_SUPPORTED == provider_ret) {
          LOG_TRACE("mixed filter bitmap provider is not supported",
                    K(provider_ret), "source_type", source_op->get_op_type());
        } else if (OB_SUCCESS != provider_ret) {
          ret = provider_ret;
          LOG_WARN("failed to create mixed filter bitmap provider", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASBooleanQueryRtDef::try_boolean_merge_probe_opt(
  const ObBooleanSubClause<ObIDASSearchRtDef> &must_clauses,
  const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
  const ObBooleanSubClause<ObIDASSearchRtDef> &should_clauses,
  const ObBooleanSubClause<ObIDASSearchRtDef> &must_not_clauses,
  const int64_t bool_should_msm,
  const ObDASSearchCost &lead_cost,
  ObDASSearchCtx &search_ctx,
  ObDASScalarScanRtDef *merged_probe_rtdef,
  ObIDASSearchOp *&op,
  bool &do_merge_probe_opt)
{
  int ret = OB_SUCCESS;
  do_merge_probe_opt = false;
  op = nullptr;
  const ObIDASSearchCtDef *ctdef = static_cast<const ObIDASSearchCtDef *>(ctdef_);
  if (OB_ISNULL(merged_probe_rtdef)) {
    // do nothing
  } else if (!must_not_clauses.empty() ||
             (!should_clauses.empty() && 1 != bool_should_msm) ||
             DAS_ROWID_TYPE_UINT64 != search_ctx.get_rowid_type() ||
             (must_clauses.empty() && filter_clauses.empty())) {
    // do nothing
  } else if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctdef", KR(ret));
  } else if (!ctdef->is_scoring()) {
    #define FILL_REQUIRED_CLAUSES(source)                                                                                  \
    do {                                                                                                                   \
      for (int i = 0; OB_SUCC(ret) && i < source.count(); i++) {                                                           \
        ObIDASSearchRtDef *clause = nullptr;                                                                               \
        if (OB_FAIL(source.get(i, clause))) {                                                                              \
          LOG_WARN("failed to get clause", KR(ret), K(i));                                                                 \
        } else if (OB_ISNULL(clause)) {                                                                                    \
          ret = OB_ERR_UNEXPECTED;                                                                                         \
          LOG_WARN("unexpected nullptr clause", KR(ret), K(i));                                                            \
        } else if (OB_FAIL(required_clauses.push_back(clause))) {                                                          \
          LOG_WARN("failed to push back clause", KR(ret), K(i));                                                           \
        }                                                                                                                  \
      }                                                                                                                    \
    } while (0);

    ObArray<ObIDASSearchRtDef *> required_clauses;
    ObArray<bool> clauses_prefer_primary_probe;
    int64_t probe_cnt = 0;
    bool should_prefer_primary_probe = should_clauses.empty();
    FILL_REQUIRED_CLAUSES(must_clauses);
    FILL_REQUIRED_CLAUSES(filter_clauses);
    #undef FILL_REQUIRED_CLAUSES
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clauses_prefer_primary_probe.prepare_allocate(required_clauses.count()))) {
      LOG_WARN("failed to prepare allocate clauses prefer primary probe", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < required_clauses.count(); ++i) {
      bool prefer_primary_probe = false;
      ObIDASSearchRtDef *clause = required_clauses.at(i);
      if (OB_ISNULL(clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr clause", KR(ret), K(i));
      } else if (OB_FAIL(clause->prefer_primary_probe(lead_cost, search_ctx, prefer_primary_probe))) {
        LOG_WARN("failed to prefer primary probe", KR(ret), K(i));
      } else {
        clauses_prefer_primary_probe.at(i) = prefer_primary_probe;
        if (prefer_primary_probe) {
          probe_cnt++;
        }
      }
    }
    if (OB_SUCC(ret) && !should_clauses.empty()) {
      ObDASSearchCost should_cost;
      const int64_t ratio = ctdef->get_primary_get_ratio();
      if (OB_FAIL(compute_should_cost(search_ctx, should_cost))) {
        LOG_WARN("failed to compute should cost", KR(ret));
      } else if (ratio > 0 && should_cost.is_valid()) {
        should_prefer_primary_probe = lead_cost.cost() <= should_cost.cost() / ratio;
      }
    }

    if (OB_SUCC(ret)) {
      const bool has_folded_should = !should_clauses.empty() && should_prefer_primary_probe;
      LOG_TRACE("try_boolean_merge_probe_opt", K(required_clauses.count()), K(probe_cnt),
        K(should_clauses.count()), K(should_prefer_primary_probe), K(has_folded_should));
      if (0 == probe_cnt && !has_folded_should) {
        // Nothing would be folded into the merged probe.
        do_merge_probe_opt = false;
      } else if (probe_cnt == required_clauses.count() && should_prefer_primary_probe) {
        // no driver clause
        do_merge_probe_opt = false;
      } else {
        ObArray<ObIDASSearchOp *> required_ops;
        ObIDASSearchOp *should_op = nullptr;
        if (!should_clauses.empty() && !should_prefer_primary_probe) {
          if (OB_FAIL(opt(
            should_clauses,
            1,          // min should match
            false,      // is scoring
            lead_cost,  // lead cost
            false,      // is top level scoring
            search_ctx,
            should_op))) {
            LOG_WARN("failed to generate should op", KR(ret));
          } else if (OB_FAIL(required_ops.push_back(should_op))) {
            LOG_WARN("failed to push back should op", KR(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < required_clauses.count(); ++i) {
          ObIDASSearchRtDef *clause = required_clauses.at(i);
          ObIDASSearchOp *op = nullptr;
          if (!clauses_prefer_primary_probe.at(i)) {
            if (OB_FAIL(clause->generate_op(lead_cost, search_ctx, op))) {
              LOG_WARN("failed to generate op", KR(ret), K(i));
            } else if (OB_FAIL(required_ops.push_back(op))) {
              LOG_WARN("failed to push back op", KR(ret), K(i));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObIDASSearchOp *merged_probe_op = nullptr;
          if (OB_UNLIKELY(required_ops.empty())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected empty required ops", KR(ret));
          } else if (FALSE_IT(merged_probe_rtdef->set_allow_probe(true))) {
          } else if (OB_FAIL(merged_probe_rtdef->generate_op(lead_cost, search_ctx, merged_probe_op))) {
            LOG_WARN("failed to generate merged probe op", KR(ret));
          } else if (OB_FAIL(required_ops.push_back(merged_probe_op))) {
            LOG_WARN("failed to push back merged probe op", KR(ret));
          } else if (OB_FAIL(generate_conjunction_op(required_ops, search_ctx, op))) {
            LOG_WARN("failed to generate conjunction op", KR(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(op)) {
          do_merge_probe_opt = true;
        }
      }
    }
  }
  return ret;
}

template <typename... Args>
int ObDASBooleanQueryRtDef::construct_op_array(ObArray<ObIDASSearchOp *> &ops, ObIDASSearchOp *op, Args... rest_ops)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ops.push_back(op))) {
    LOG_WARN("failed to push back op", KR(ret));
  } else if (OB_FAIL(construct_op_array(ops, rest_ops...))) {
    LOG_WARN("failed to construct op array", KR(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
