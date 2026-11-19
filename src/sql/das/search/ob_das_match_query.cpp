/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_match_query.h"
#include "ob_das_bmw_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_bitmap_op.h"
#include "ob_das_dummy_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_async_index_query_ctx.h"
#include "ob_das_search_utils.h"
#include "ob_das_scalar_define.h"
#include "ob_das_fts_cost_utils.h"
#include "storage/fts/ob_fts_tokenizer.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"


namespace oceanbase
{

namespace sql
{

OB_SERIALIZE_MEMBER((ObDASMatchCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    match_operator_,
                    minimum_should_match_,
                    ir_ctdef_idx_,
                    pushdown_must_filter_op_ctdef_idx_,
                    is_msm_unresolved_expr_);

OB_SERIALIZE_MEMBER((ObDASMatchRtDef, ObIDASSearchRtDef));

void ObDASMatchRtDef::reset_cached_runtime_state()
{
  query_tokens_.reuse();
  token_counts_.reuse();
  token_costs_.reuse();
  total_token_cnt_ = 0;
  query_tokens_prepared_ = false;
}

int ObDASMatchRtDef::copy_cached_runtime_state_from(const ObIDASSearchRtDef &other)
{
  int ret = OB_SUCCESS;
  reset_cached_runtime_state();
  if (OB_UNLIKELY(DAS_OP_MATCH_QUERY != other.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search rtdef type for match runtime state copy", KR(ret), K(other.op_type_));
  } else {
    const ObDASMatchRtDef &src = static_cast<const ObDASMatchRtDef &>(other);
    if (OB_FAIL(ObIDASSearchRtDef::copy_cached_runtime_state_from(other))) {
      LOG_WARN("failed to copy base search runtime state", KR(ret));
    } else if (OB_FAIL(query_tokens_.reserve(src.query_tokens_.count()))) {
      LOG_WARN("failed to reserve copied match query tokens", KR(ret), K(src.query_tokens_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.query_tokens_.count(); ++i) {
      ObString copied_token;
      if (OB_FAIL(ob_write_string(allocator_, src.query_tokens_.at(i), copied_token))) {
        LOG_WARN("failed to deep copy match query token", KR(ret), K(i));
      } else if (OB_FAIL(query_tokens_.push_back(copied_token))) {
        LOG_WARN("failed to append copied match query token", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(token_counts_.assign(src.token_counts_))) {
      LOG_WARN("failed to copy match token counts", KR(ret), K(src.token_counts_.count()));
    } else if (OB_SUCC(ret) && OB_FAIL(token_costs_.assign(src.token_costs_))) {
      LOG_WARN("failed to copy match token costs", KR(ret), K(src.token_costs_.count()));
    } else if (OB_SUCC(ret)) {
      total_token_cnt_ = src.total_token_cnt_;
      query_tokens_prepared_ = src.query_tokens_prepared_;
    }
  }
  if (OB_FAIL(ret)) {
    reset_cached_runtime_state();
    reset_cost();
  }
  return ret;
}

int ObDASMatchRtDef::prepare_query_tokens()
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = get_ctdef();
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  if (query_tokens_prepared_) {
    // reuse tokens prepared while computing async match cost
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->query_text_)
      || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_ISNULL(inv_idx_scan_ctdef =
      ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inverted index scan ctdef", KR(ret));
  } else if (inv_idx_scan_ctdef->is_logical_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("match query on logical table is not supported", KR(ret));
  } else if (OB_NOT_NULL(ctdef->get_pushdown_must_filter_op_ctdef())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown must filter op ctdef is not supported", KR(ret));
  } else if (OB_FAIL(ObDASSearchUtils::eval_and_tokenize_query_text(
      allocator_, *eval_ctx_, *ctdef->query_text_, *inv_idx_scan_ctdef,
      query_tokens_, token_counts_, ctdef->get_ir_ctdef()))) {
    LOG_WARN("failed to prepare match query tokens", KR(ret));
  } else {
    total_token_cnt_ = 0;
    for (int64_t i = 0; i < token_counts_.count(); ++i) {
      total_token_cnt_ += token_counts_.at(i);
    }
    query_tokens_prepared_ = true;
  }
  return ret;
}

int ObDASMatchRtDef::align_token_costs()
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = get_ctdef();
  const ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef() : nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(ir_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match ctdef when aligning token costs", KR(ret), KP(ctdef), KP(ir_ctdef));
  } else if (ir_ctdef->is_async_index_) {
    if (token_costs_.count() != query_tokens_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mismatched async match token costs", KR(ret),
          K(query_tokens_.count()), K(token_costs_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_costs_.count(); ++i) {
      if (OB_UNLIKELY(!token_costs_.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid async match token cost", KR(ret), K(i),
            K(query_tokens_.count()), K(token_costs_.count()));
      }
    }
  } else if (token_costs_.count() != query_tokens_.count()) {
    token_costs_.reuse();
    if (OB_FAIL(token_costs_.reserve(query_tokens_.count()))) {
      LOG_WARN("failed to reserve aligned token costs", K(ret), K_(query_tokens));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      if (OB_FAIL(token_costs_.push_back(ObDASSearchCost()))) {
        LOG_WARN("failed to append invalid token cost", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
      token_costs_.reuse();
    }
  }
  return ret;
}

bool ObDASMatchRtDef::should_use_bmm_op(
    bool is_top_level_scoring,
    int64_t match_operator,
    int64_t minimum_should_match)
{
  return is_top_level_scoring
      && ObMatchOperator::MATCH_OPERATOR_AND != match_operator
      && minimum_should_match <= 1;
}

int ObDASMatchRtDef::compute_async_match_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = get_ctdef();
  ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef();
  ObDatum *match_operator_datum = nullptr;
  ObDatum *minimum_should_match_datum = nullptr;
  ObSEArray<int64_t, 16> token_costs;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())
      || OB_ISNULL(ir_rtdef) || OB_ISNULL(ctdef->match_operator_)
      || OB_ISNULL(ctdef->minimum_should_match_) || !search_ctx.get_row_count().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid match cost context", KR(ret), KP(ctdef), KP_(eval_ctx),
        KP(ir_rtdef), K(search_ctx.get_row_count()));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(
      *eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_ISNULL(match_operator_datum) || OB_ISNULL(minimum_should_match_datum)
      || OB_UNLIKELY(match_operator_datum->is_null()
          || minimum_should_match_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(match_operator_datum), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(token_costs.reserve(query_tokens_.count()))) {
    LOG_WARN("failed to reserve token costs", KR(ret), K(query_tokens_.count()));
  } else if (OB_FAIL(ObDASFtsCostUtils::estimate_async_token_doc_cnts(
      search_ctx, *ctdef->get_ir_ctdef(), *ir_rtdef, query_tokens_, token_costs))) {
    LOG_WARN("failed to estimate async token document counts", KR(ret), K_(query_tokens));
  }

  if (OB_SUCC(ret)) {
    token_costs_.reuse();
    if (OB_FAIL(token_costs_.reserve(token_costs.count()))) {
      LOG_WARN("failed to reserve match token cost metadata", K(ret), K(token_costs.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_costs.count(); ++i) {
      if (OB_FAIL(token_costs_.push_back(ObDASSearchCost(token_costs.at(i))))) {
        LOG_WARN("failed to save match token cost metadata", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t table_row_count = search_ctx.get_row_count().cost();
    int64_t minimum_should_match = 0;
    int64_t raw_cost = 0;
    if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator_datum->get_int()) {
      if (OB_FAIL(ObDASFtsCostUtils::aggregate_and_cost(token_costs, raw_cost))) {
        LOG_WARN("failed to aggregate and cost", KR(ret), K(token_costs));
      }
    } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_match_msm(
        allocator_, total_token_cnt_, ctdef->is_msm_unresolved_expr_,
        minimum_should_match_datum, minimum_should_match))) {
      LOG_WARN("failed to calculate match minimum should match for cost estimation",
          KR(ret), K_(query_tokens), KPC(minimum_should_match_datum));
    } else if (minimum_should_match >= total_token_cnt_) {
      if (OB_FAIL(ObDASFtsCostUtils::aggregate_and_cost(token_costs, raw_cost))) {
        LOG_WARN("failed to aggregate and cost", KR(ret), K(token_costs));
      }
    } else if (minimum_should_match <= 1) {
      // OR semantics: duplicate tokens match the same documents, so aggregating
      // deduplicated token costs avoids counting the same token repeatedly.
      if (OB_FAIL(ObDASFtsCostUtils::aggregate_or_cost(
          token_costs, table_row_count, raw_cost))) {
        LOG_WARN("failed to aggregate or cost", KR(ret), K(token_costs), K(table_row_count));
      }
    } else if (OB_UNLIKELY(token_costs.count() != token_counts_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mismatched match token costs and counts", KR(ret),
          K(token_costs.count()), K(token_counts_.count()));
    } else {
      // msm > 1 keeps per-occurrence clause semantics: expand the deduplicated
      // token costs by occurrence counts before aggregation.
      ObSEArray<int64_t, 16> expanded_costs;
      if (OB_FAIL(expanded_costs.reserve(total_token_cnt_))) {
        LOG_WARN("failed to reserve expanded token costs", KR(ret), K_(total_token_cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < token_costs.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < token_counts_.at(i); ++j) {
          if (OB_FAIL(expanded_costs.push_back(token_costs.at(i)))) {
            LOG_WARN("failed to expand match token cost", KR(ret), K(i));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDASFtsCostUtils::aggregate_msm_cost(
          expanded_costs, minimum_should_match, table_row_count, raw_cost))) {
        LOG_WARN("failed to aggregate minimum should match cost",
            KR(ret), K(expanded_costs), K(minimum_should_match), K(table_row_count));
      }
    }
    if (OB_SUCC(ret)) {
      cost = ObDASSearchCost(std::min(raw_cost, table_row_count));
    }
  }
  return ret;
}

int ObDASMatchRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASMatchCtDef *ctdef = get_ctdef();
  if (OB_ISNULL(ctdef) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match definition", KR(ret), KP(ctdef));
  } else if (!ctdef->get_ir_ctdef()->is_async_index_) {
    // Non-async match keeps an invalid cost and falls back to the table row count.
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare query tokens for cost estimation", KR(ret));
  } else if (query_tokens_.empty()) {
    cost = ObDASSearchCost(0);
  } else if (OB_FAIL(compute_async_match_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute async match cost", KR(ret));
  }
  return ret;
}

int ObDASMatchRtDef::can_pushdown_filter_to_bmm(
    ObDASSearchCtx &search_ctx,
    bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  const ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef() : nullptr;
  const ObDASScalarScanCtDef *inv_ctdef =
      OB_NOT_NULL(ir_ctdef) ? ir_ctdef->get_inv_idx_scan_scalar_ctdef() : nullptr;
  can_pushdown = false;
  pushdown_filter_op_ = nullptr;
  streaming_group_filter_ = nullptr;
  has_bmm_build_options_ = false;
  pushdown_filter_provider_ = nullptr;
  pushdown_filter_cost_.reset();
  query_optional_ = false;
  ObDatum *minimum_should_match_datum = nullptr;
  ObDatum *match_operator_datum = nullptr;
  if (OB_ISNULL(ctdef)
      || OB_ISNULL(ir_ctdef)
      || OB_ISNULL(eval_ctx_)
      || OB_ISNULL(ctdef->minimum_should_match_)
      || OB_ISNULL(ctdef->match_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match definition", K(ret), KP(ctdef), KP(ir_ctdef), KP_(eval_ctx));
  } else if (OB_NOT_NULL(ctdef->get_pushdown_must_filter_op_ctdef())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown must filter op ctdef is not supported", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_ISNULL(minimum_should_match_datum)
             || OB_ISNULL(match_operator_datum)
             || OB_UNLIKELY(minimum_should_match_datum->is_null()
                            || match_operator_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(minimum_should_match_datum), KPC(match_operator_datum));
  } else {
    const bool base_ok = ctdef->is_top_level_scoring()
        && ObMatchOperator::MATCH_OPERATOR_AND != match_operator_datum->get_int();
    int64_t effective_msm = 0;
    ObSEArray<ObString, 16> query_tokens;
    ObSEArray<int64_t, 16> token_counts;
    if (!base_ok) {
      // can_pushdown stays false
    } else if (!ctdef->is_msm_unresolved_expr_ && minimum_should_match_datum->get_int() >= 0) {
      // A non-negative integer MSM does not depend on the number of query tokens.
      // Keep this path independent of query_text_ so lightweight/runtime CTDefs can
      // still answer the filter-pushdown capability check.
      effective_msm = minimum_should_match_datum->get_int();
      can_pushdown = effective_msm <= 1;
    } else if (OB_ISNULL(ctdef->query_text_) || OB_ISNULL(inv_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query definition for bmm pushdown",
               KR(ret), KP(ctdef->query_text_), KP(inv_ctdef));
    } else if (OB_FAIL(ObDASSearchUtils::eval_and_tokenize_query_text(
                       allocator_, *eval_ctx_, *ctdef->query_text_, *inv_ctdef,
                       query_tokens, token_counts, ir_ctdef))) {
      LOG_WARN("failed to tokenize query text for bmm pushdown", KR(ret));
    } else if (0 == query_tokens.count()) {
      // can_pushdown stays false
    } else {
      int64_t total_token_cnt = 0;
      for (int64_t i = 0; i < token_counts.count(); ++i) {
        total_token_cnt += token_counts.at(i);
      }
      if (OB_FAIL(ObDASMSMRuntimeUtil::calc_match_msm(
                      allocator_,
                      total_token_cnt,
                      ctdef->is_msm_unresolved_expr_,
                      minimum_should_match_datum,
                      effective_msm))) {
        LOG_WARN("failed to calc match msm for bmm pushdown", KR(ret));
      } else {
        can_pushdown = effective_msm <= 1;
      }
    }
    if (OB_SUCC(ret) && can_pushdown && ir_ctdef->is_async_index_) {
      uint64_t index_table_id = OB_INVALID_ID;
      int64_t max_segment_cnt = 0;
      // A Group-scoped MATCH may use one bitmap provider across the Segment
      // BMMs in that Group. Legacy execution allows at most one Segment.
      can_pushdown = false;
      if (OB_ISNULL(inv_ctdef) || OB_INVALID_ID == inv_ctdef->ref_table_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async match index definition", K(ret), KP(inv_ctdef));
      } else if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
                     *ir_ctdef, search_ctx, index_table_id, max_segment_cnt))) {
        LOG_WARN("failed to resolve async match filter pushdown domain", K(ret));
      } else {
        can_pushdown = 1 >= max_segment_cnt
            || (search_ctx.get_async_group_slot() >= 0
                && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type());
      }
    }
  }
  return ret;
}

int ObDASMatchRtDef::get_bmm_filter_consumer_counts(
    ObDASSearchCtx &search_ctx,
    int64_t &group_consumer_count,
    int64_t &query_consumer_count)
{
  int ret = OB_SUCCESS;
  group_consumer_count = 0;
  query_consumer_count = 0;
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  const ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef() : nullptr;
  const ObDASScalarScanCtDef *inv_ctdef =
      OB_NOT_NULL(ir_ctdef) ? ir_ctdef->get_inv_idx_scan_scalar_ctdef() : nullptr;
  ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
  if (OB_ISNULL(ctdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(inv_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match definition for filter consumer counts",
             K(ret), KP(ctdef), KP(ir_ctdef), KP(inv_ctdef));
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare match tokens for filter consumer counts", K(ret));
  } else if (query_tokens_.empty()) {
    // A required MATCH with no token has no BMM consumer in any Group.
  } else if (!ir_ctdef->is_async_index_) {
    group_consumer_count = 1;
    query_consumer_count = 1;
  } else if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async query catalog is null for filter consumer counts", K(ret));
  } else {
    uint64_t index_table_id = common::OB_INVALID_ID;
    int64_t current_group_segment_count = 0;
    if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
            *ir_ctdef, search_ctx, index_table_id, current_group_segment_count))) {
      LOG_WARN("failed to resolve async match consumer domain", K(ret));
    } else if (OB_UNLIKELY(common::OB_INVALID_ID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async match index for filter consumer counts",
               K(ret), K(index_table_id));
    } else {
      // For one semantic MATCH branch, every effective Segment in the current
      // binding produces one Group consumer. The complete index Segment count
      // is the query-scope sharing threshold for synchronous Filters.
      group_consumer_count = current_group_segment_count;
      query_consumer_count = query_ctx->get_max_segment_cnt(index_table_id);
    }
  }
  return ret;
}

int ObDASMatchRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  DEFER(pushdown_filter_op_ = nullptr;
        streaming_group_filter_ = nullptr;
        has_bmm_build_options_ = false;
        pushdown_filter_provider_ = nullptr;
        query_optional_ = false);
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  const ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef() : nullptr;
  const ObDASScalarScanCtDef *inv_ctdef =
      OB_NOT_NULL(ir_ctdef) ? ir_ctdef->get_inv_idx_scan_scalar_ctdef() : nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(inv_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match definition", K(ret), KP(ctdef), KP(ir_ctdef), KP(inv_ctdef));
  } else if (!ir_ctdef->is_async_index_) {
    if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
      LOG_WARN("failed to generate synchronous match op", K(ret));
    }
  } else {
    ObSEArray<ObIDASSearchOp *, 4> segment_ops;
    ObDASAsyncBindingGuard binding_guard(search_ctx);
    const uint64_t claimed_index_table_id = search_ctx.get_async_index_table_id();
    uint64_t index_table_id = common::OB_INVALID_ID;
    int64_t max_segment_cnt = 0;
    if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
            *ir_ctdef, search_ctx, index_table_id, max_segment_cnt))) {
      LOG_WARN("failed to resolve async match segment domain", K(ret));
    } else if (common::OB_INVALID_ID != claimed_index_table_id) {
      if (OB_UNLIKELY(claimed_index_table_id != index_table_id)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async match does not belong to claimed index domain",
                 K(ret), K(claimed_index_table_id), K(index_table_id));
      } else if (OB_NOT_NULL(streaming_group_filter_)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("streaming Group filter requires per-Segment construction",
                 K(ret), K(index_table_id));
      } else if (OB_NOT_NULL(pushdown_filter_op_)
                 && !search_ctx.get_async_direct_bind()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async match filter pushdown requires a direct segment binding",
                 K(ret), K(index_table_id), K(max_segment_cnt));
      } else if (OB_NOT_NULL(pushdown_filter_provider_)) {
        ObDASBitmapOp *filter_view = nullptr;
        if (OB_FAIL(pushdown_filter_provider_->create_view(search_ctx, filter_view))) {
          LOG_WARN("failed to create bound async match filter view", K(ret), K(index_table_id));
        } else if (FALSE_IT(pushdown_filter_op_ = filter_view)) {
        } else if (FALSE_IT(has_bmm_build_options_ = false)) {
        } else if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
          LOG_WARN("failed to generate bound async match subtree with filter view",
                   K(ret), K(index_table_id));
        }
      } else if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate bound async match subtree", K(ret), K(index_table_id));
      }
    } else {
      const int64_t group_slot = search_ctx.get_async_group_slot();
      if (OB_NOT_NULL(pushdown_filter_op_) && 1 < max_segment_cnt) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async match filter pushdown crosses SegmentMerge",
                 K(ret), K(index_table_id), K(max_segment_cnt));
      } else {
        // An async Token captures its logical index, Group, and direct-binding attributes,
        // so later partition switches do not depend on shared construction context.
        binding_guard.set(index_table_id, group_slot, group_slot >= 0 && 1 >= max_segment_cnt);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX(1L, max_segment_cnt); ++i) {
        ObIDASSearchOp *segment_op = nullptr;
        if (OB_NOT_NULL(streaming_group_filter_)) {
          pushdown_filter_op_ = nullptr;
          has_bmm_build_options_ = false;
          if (OB_FAIL(streaming_group_filter_->generate_streaming_group_filter_op(
                  lead_cost, search_ctx, pushdown_filter_op_))) {
            LOG_WARN("failed to create per-BMM streaming filter", K(ret), K(i));
          }
        } else if (OB_NOT_NULL(pushdown_filter_provider_)) {
          ObDASBitmapOp *filter_view = nullptr;
          if (OB_FAIL(pushdown_filter_provider_->create_view(search_ctx, filter_view))) {
            LOG_WARN("failed to create async match Segment filter view",
                     K(ret), K(i), K(index_table_id));
          } else {
            pushdown_filter_op_ = filter_view;
            has_bmm_build_options_ = false;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_op_impl(lead_cost, search_ctx, segment_op))) {
          LOG_WARN("failed to generate async match segment subtree",
                   K(ret), K(i), K(index_table_id));
        } else if (OB_ISNULL(segment_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async match segment subtree", K(ret), K(i), K(index_table_id));
        } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
          LOG_WARN("failed to append async match segment subtree", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
              segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, op))) {
        LOG_WARN("failed to create per-index async match segment merge",
                 K(ret), K(index_table_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
    op = nullptr;
  }
  pushdown_filter_op_ = nullptr;
  has_bmm_build_options_ = false;
  pushdown_filter_provider_ = nullptr;
  pushdown_filter_cost_.reset();
  query_optional_ = false;
  return ret;
}

int ObDASMatchRtDef::generate_op_impl(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  ObDASIRScanRtDef *ir_rtdef = nullptr;
  ObDatum *minimum_should_match_datum = nullptr;
  bool is_async_index = false;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_NOT_NULL(ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef())
             && ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef()->is_logical_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "match query on logical table is");
    LOG_WARN("match query on logical table is not supported", KR(ret));
  } else if (OB_NOT_NULL(ctdef->get_pushdown_must_filter_op_ctdef())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown must filter op ctdef is not supported", KR(ret));
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare query tokens", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_UNLIKELY(minimum_should_match_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret), KPC(minimum_should_match_datum));
  }

  if (OB_FAIL(ret)) {
    // skip
  } else if (query_tokens_.empty()) {
    if (query_optional_ && OB_NOT_NULL(pushdown_filter_op_)) {
      // An empty optional scoring query still preserves the filter-owned domain.
      op = pushdown_filter_op_;
    } else {
      ObDASDummyOp *dummy_op = nullptr;
      ObDASDummyOpParam dummy_op_param;
      dummy_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
        LOG_WARN("failed to create dummy op", KR(ret));
      } else {
        op = dummy_op;
      }
    }
  } else {
    ir_rtdef = get_ir_rtdef();
    ObDatum *boost_datum = nullptr;
    ObDatum *match_operator_datum = nullptr;
    ObSEArray<ObIDASSearchOp *, 16> token_ops;
    if (OB_ISNULL(ir_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ir rtdef", K(ret));
    } else if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("expr evaluation failed", KR(ret));
    } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
      LOG_WARN("expr evaluation failed", KR(ret));
    } else if (OB_ISNULL(boost_datum)
               || OB_ISNULL(match_operator_datum)
               || OB_UNLIKELY(boost_datum->is_null() || match_operator_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param datum", KR(ret), KPC(boost_datum), KPC(match_operator_datum));
    } else if (OB_UNLIKELY(token_counts_.count() != query_tokens_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mismatched match tokens and counts", KR(ret),
          K(query_tokens_.count()), K(token_counts_.count()));
    }

    // With AND, or with OR and minimum_should_match <= 1, duplicate query tokens
    // match the same documents and token scoring is linear in boost, so duplicates
    // can be folded into one token op with a count-weighted boost. OR with msm > 1
    // keeps per-occurrence token ops to preserve clause-count semantics.
    int64_t match_operator = ObMatchOperator::MATCH_OPERATOR_OR;
    int64_t minimum_should_match = 0;
    bool fold_duplicate_tokens = false;
    if (OB_SUCC(ret)) {
      match_operator = match_operator_datum->get_int();
      if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator) {
        fold_duplicate_tokens = true;
      } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_match_msm(
                     allocator_, total_token_cnt_, get_ctdef()->is_msm_unresolved_expr_,
                     minimum_should_match_datum, minimum_should_match))) {
        LOG_WARN("failed to calculate match minimum_should_match",
            KR(ret), KPC(minimum_should_match_datum));
      } else {
        fold_duplicate_tokens = minimum_should_match <= 1;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(token_ops.reserve(
        fold_duplicate_tokens ? query_tokens_.count() : total_token_cnt_))) {
      LOG_WARN("failed to reserve token ops", KR(ret), K(query_tokens_.count()));
    }

    is_async_index = OB_NOT_NULL(ctdef->get_ir_ctdef())
        && ctdef->get_ir_ctdef()->is_async_index_;
    if (OB_FAIL(ret)) {
    } else if (is_async_index || block_max_param_initialized_) {
      // async fts does not generate block_max scan in current phase
    } else if (OB_FAIL(block_max_param_.init(*ctdef->get_ir_ctdef(), allocator_))) {
      LOG_WARN("failed to init block max param", KR(ret));
    } else {
      block_max_param_initialized_ = true;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      const ObString &query_token = query_tokens_.at(i);
      const int64_t occurrence_cnt = fold_duplicate_tokens ? 1 : token_counts_.at(i);
      const double token_boost = fold_duplicate_tokens
          ? boost_datum->get_double() * token_counts_.at(i)
          : boost_datum->get_double();
      for (int64_t j = 0; OB_SUCC(ret) && j < occurrence_cnt; ++j) {
        ObIDASSearchOp *token_op = nullptr;
        storage::ObBlockMaxScoreIterParam *block_max_param =
            is_async_index ? nullptr : &block_max_param_;
        if (OB_FAIL(ObDASSearchUtils::create_token_search_op(
                *ctdef->get_ir_ctdef(), *ir_rtdef, block_max_param, query_token,
                token_boost, ctdef->is_scoring(), search_ctx, token_op))) {
          LOG_WARN("failed to create match token op", K(ret), K(i));
        } else if (OB_FAIL(token_ops.push_back(token_op))) {
          LOG_WARN("failed to push back token op", KR(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator) {
      ObDASConjunctionOp *conjunction_op = nullptr;
      ObDASConjunctionOpParam conjunction_op_param(token_ops);
      conjunction_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(conjunction_op_param, conjunction_op))) {
        LOG_WARN("failed to create conjunction op", KR(ret));
      } else {
        op = conjunction_op;
      }
    } else if (should_use_bmm_op(
                   get_ctdef()->is_top_level_scoring(),
                   match_operator,
                   minimum_should_match)) {
      ObDASBMMOp *bmm_op = nullptr;
      ObDASSearchCost match_cost;
      if (OB_FAIL(get_cost(search_ctx, match_cost))) {
        LOG_WARN("failed to prepare match cost for BMM", KR(ret));
      } else if (OB_FAIL(align_token_costs())) {
        LOG_WARN("failed to align BMM token costs", K(ret), K_(query_tokens));
      } else {
        ObDASBMMBuildOptions bmm_options = has_bmm_build_options_
            ? bmm_build_options_ : ObDASBMMBuildOptions();
        bmm_options.query_optional_ = query_optional_;
        bmm_options.lead_cost_ = lead_cost;
        bmm_options.filter_cost_ = pushdown_filter_cost_;
        ObDASBMMOpParam bmm_op_param(
            token_ops, pushdown_filter_op_, allocator_, bmm_options, &token_costs_);
        bmm_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
          LOG_WARN("Match query failed to create BMM operator with minimum should match", KR(ret), KPC(minimum_should_match_datum));
        } else {
          op = bmm_op;
        }
      }
    } else if (get_ctdef()->is_top_level_scoring()) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_op_param(token_ops, minimum_should_match, allocator_);
      bmw_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
        LOG_WARN("Match query failed to create BMW operator with minimum should match", KR(ret), KPC(minimum_should_match_datum));
      } else {
        op = bmw_op;
      }
    } else {
      ObDASDisjunctionOp *disjunction_op = nullptr;
      ObDASDisjunctionOpParam disjunction_op_param(
          token_ops,
          minimum_should_match,
          false,
          lead_cost);
      disjunction_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
        LOG_WARN("Match query failed to create disjunction operator with minimum should match", KR(ret), KPC(minimum_should_match_datum));
      } else {
        op = disjunction_op;
      }
    }
  }
  if (OB_FAIL(ret)) {
    op = nullptr;
  }
  return ret;
}

}
}
