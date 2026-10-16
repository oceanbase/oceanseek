/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_multi_match_query.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_bmw_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_dummy_op.h"
#include "ob_das_async_index_query_ctx.h"
#include "ob_das_search_utils.h"
#include "ob_das_fts_cost_utils.h"
#include "lib/container/ob_array_helper.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/fts/ob_fts_tokenizer.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"


namespace oceanbase {

namespace sql {

OB_SERIALIZE_MEMBER((ObDASMultiMatchCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    match_operator_,
                    minimum_should_match_,
                    type_,
                    field_boosts_,
                    ir_ctdef_indices_,
                    is_msm_unresolved_expr_);

OB_SERIALIZE_MEMBER((ObDASMultiMatchRtDef, ObIDASSearchRtDef));

void ObDASMultiMatchRtDef::reset_cached_runtime_state()
{
  query_tokens_.reuse();
  field_token_costs_.reuse();
  field_costs_.reuse();
  query_tokens_prepared_ = false;
}

int ObDASMultiMatchRtDef::copy_cached_runtime_state_from(const ObIDASSearchRtDef &other)
{
  int ret = OB_SUCCESS;
  reset_cached_runtime_state();
  if (OB_UNLIKELY(DAS_OP_MULTI_MATCH_QUERY != other.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search rtdef type for multi-match runtime state copy",
        KR(ret), K(other.op_type_));
  } else {
    const ObDASMultiMatchRtDef &src = static_cast<const ObDASMultiMatchRtDef &>(other);
    if (OB_FAIL(ObIDASSearchRtDef::copy_cached_runtime_state_from(other))) {
      LOG_WARN("failed to copy base search runtime state", KR(ret));
    } else if (OB_FAIL(query_tokens_.reserve(src.query_tokens_.count()))) {
      LOG_WARN("failed to reserve copied multi-match query tokens",
          KR(ret), K(src.query_tokens_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.query_tokens_.count(); ++i) {
      ObString copied_token;
      if (OB_FAIL(ob_write_string(allocator_, src.query_tokens_.at(i), copied_token))) {
        LOG_WARN("failed to deep copy multi-match query token", KR(ret), K(i));
      } else if (OB_FAIL(query_tokens_.push_back(copied_token))) {
        LOG_WARN("failed to append copied multi-match query token", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(field_token_costs_.assign(src.field_token_costs_))) {
      LOG_WARN("failed to copy multi-match field token costs",
          KR(ret), K(src.field_token_costs_.count()));
    } else if (OB_SUCC(ret) && OB_FAIL(field_costs_.assign(src.field_costs_))) {
      LOG_WARN("failed to copy multi-match field costs", KR(ret), K(src.field_costs_.count()));
    } else if (OB_SUCC(ret)) {
      query_tokens_prepared_ = src.query_tokens_prepared_;
    }
  }
  if (OB_FAIL(ret)) {
    reset_cached_runtime_state();
    reset_cost();
  }
  return ret;
}

int ObDASMultiMatchRtDef::prepare_query_tokens()
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  if (query_tokens_prepared_) {
    // Reuse tokens prepared while computing the async cost.
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->query_text_)
      || ctdef->ir_ctdef_indices_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null multi-match definition", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_ISNULL(ctdef->get_ir_ctdef(0))
      || OB_ISNULL(inv_idx_scan_ctdef =
          ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inverted index scan ctdef", KR(ret));
  } else if (inv_idx_scan_ctdef->is_logical_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multi match query on logical table is not supported");
    LOG_WARN("multi match query on logical table is not supported", KR(ret));
  } else if (OB_FAIL(ObDASSearchUtils::eval_and_tokenize_query_text(
      allocator_, *eval_ctx_, *ctdef->query_text_, *inv_idx_scan_ctdef, query_tokens_))) {
    LOG_WARN("failed to prepare multi-match query tokens", KR(ret));
  } else {
    query_tokens_prepared_ = true;
  }
  return ret;
}

int ObDASMultiMatchRtDef::compute_async_multi_match_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  ObDatum *match_operator_datum = nullptr;
  ObDatum *minimum_should_match_datum = nullptr;
  int64_t effective_msm = 0;
  ObSEArray<int64_t, 8> raw_field_costs;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->match_operator_)
      || OB_ISNULL(ctdef->minimum_should_match_) || ctdef->ir_ctdef_indices_.empty()
      || !search_ctx.get_row_count().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid multi-match cost context", KR(ret), KP(ctdef),
        KP_(eval_ctx), K(search_ctx.get_row_count()));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("failed to eval match operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(
      *eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("failed to eval minimum should match", KR(ret));
  } else if (OB_ISNULL(match_operator_datum) || OB_ISNULL(minimum_should_match_datum)
      || OB_UNLIKELY(match_operator_datum->is_null()
          || minimum_should_match_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null multi-match cost datum", KR(ret),
        KPC(match_operator_datum), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
      allocator_, query_tokens_.count(), ctdef->is_msm_unresolved_expr_,
      minimum_should_match_datum, effective_msm))) {
    LOG_WARN("failed to calculate multi-match minimum_should_match for cost estimation",
        KR(ret), K(query_tokens_.count()), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(raw_field_costs.reserve(ctdef->ir_ctdef_indices_.count()))) {
    LOG_WARN("failed to reserve multi-match field costs", KR(ret));
  }

  // Only top-level scoring queries feed costs into BMM operators; skip caching
  // the field-level cost details otherwise.
  const bool cache_costs = OB_NOT_NULL(ctdef) && ctdef->is_top_level_scoring();
  if (OB_SUCC(ret)) {
    field_token_costs_.reuse();
    field_costs_.reuse();
    if (cache_costs && OB_FAIL(field_token_costs_.reserve(
            ctdef->ir_ctdef_indices_.count() * query_tokens_.count()))) {
      LOG_WARN("failed to reserve multi-match field token cost metadata", KR(ret));
    } else if (cache_costs
               && OB_FAIL(field_costs_.reserve(ctdef->ir_ctdef_indices_.count()))) {
      LOG_WARN("failed to reserve multi-match field cost metadata", KR(ret));
    }
  }

  const int64_t table_row_count = search_ctx.get_row_count().cost();
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->ir_ctdef_indices_.count(); ++i) {
    ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(i);
    ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(i);
    ObSEArray<int64_t, 16> raw_token_costs;
    int64_t field_cost = 0;
    if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null multi-match field definition", KR(ret), K(i),
          KP(ir_ctdef), KP(ir_rtdef));
    } else if (OB_FAIL(raw_token_costs.reserve(query_tokens_.count()))) {
      LOG_WARN("failed to reserve multi-match token costs", KR(ret), K(i));
    } else if (OB_FAIL(ObDASFtsCostUtils::estimate_async_token_doc_cnts(
        search_ctx, *ir_ctdef, *ir_rtdef, query_tokens_, raw_token_costs))) {
      LOG_WARN("failed to estimate multi-match token costs", KR(ret), K(i));
    } else if (OB_FAIL(ObDASFtsCostUtils::aggregate_match_cost(
        raw_token_costs, match_operator_datum->get_int(), effective_msm,
        table_row_count, field_cost))) {
      LOG_WARN("failed to aggregate multi-match field cost", KR(ret), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && cache_costs && j < raw_token_costs.count(); ++j) {
      if (OB_FAIL(field_token_costs_.push_back(ObDASSearchCost(raw_token_costs.at(j))))) {
        LOG_WARN("failed to save multi-match token cost", KR(ret), K(i), K(j));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cache_costs && OB_FAIL(field_costs_.push_back(ObDASSearchCost(field_cost)))) {
      LOG_WARN("failed to save multi-match field cost", KR(ret), K(i));
    } else if (OB_FAIL(raw_field_costs.push_back(field_cost))) {
      LOG_WARN("failed to append multi-match field cost", KR(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t query_cost = 0;
    if (OB_FAIL(ObDASFtsCostUtils::aggregate_or_cost(
        raw_field_costs, table_row_count, query_cost))) {
      LOG_WARN("failed to aggregate multi-match field union cost", KR(ret));
    } else {
      cost = ObDASSearchCost(query_cost);
    }
  }
  if (OB_FAIL(ret)) {
    field_token_costs_.reuse();
    field_costs_.reuse();
  }
  return ret;
}

int ObDASMultiMatchRtDef::compute_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null multi-match ctdef", KR(ret));
  } else if (!ctdef->is_async_index()) {
    // Non-async multi-match keeps an invalid cost and falls back to the table row count.
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare multi-match query tokens for cost estimation", KR(ret));
  } else if (query_tokens_.empty()) {
    cost = ObDASSearchCost(0);
  } else if (OB_FAIL(compute_async_multi_match_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute async multi-match cost", KR(ret));
  }
  return ret;
}

int ObDASMultiMatchRtDef::align_cost_array(
    ObDASSearchCtx &search_ctx,
    ObArray<ObDASSearchCost> &costs,
    const int64_t expected_cnt,
    const char *cost_name)
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  bool costs_aligned = costs.count() == expected_cnt;
  if (OB_ISNULL(ctdef) || OB_UNLIKELY(expected_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid multi-match BMM cost alignment argument",
        KR(ret), KP(ctdef), K(expected_cnt), K(cost_name));
  } else if (!costs_aligned) {
    // Cold path: trigger cost estimation, which fills the cached cost arrays.
    ObDASSearchCost boundary_cost;
    if (OB_FAIL(get_cost(search_ctx, boundary_cost))) {
      LOG_WARN("failed to obtain cold multi-match BMM boundary cost", KR(ret), K(cost_name));
    }
    costs_aligned = costs.count() == expected_cnt;
  }

  if (OB_FAIL(ret)) {
  } else if (ctdef->is_async_index()) {
    if (!costs_aligned) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mismatched async multi-match costs",
          KR(ret), K(expected_cnt), K(costs.count()), K(cost_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_cnt; ++i) {
      if (OB_UNLIKELY(!costs.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid async multi-match cost", KR(ret), K(i), K(cost_name));
      }
    }
  } else if (!costs_aligned) {
    costs.reuse();
    if (OB_FAIL(costs.reserve(expected_cnt))) {
      LOG_WARN("failed to reserve aligned multi-match costs", KR(ret), K(expected_cnt), K(cost_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_cnt; ++i) {
      if (OB_FAIL(costs.push_back(ObDASSearchCost()))) {
        LOG_WARN("failed to append invalid multi-match cost", KR(ret), K(i), K(cost_name));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctdef) && !ctdef->is_async_index()) {
    costs.reuse();
  }
  return ret;
}

int ObDASMultiMatchRtDef::init_block_max_params(
    const ObDASMultiMatchCtDef &ctdef,
    const int64_t field_cnt)
{
  int ret = OB_SUCCESS;
  if (block_max_params_initialized_) {
    // skip
  } else if (OB_FAIL(block_max_params_.init(field_cnt))) {
    LOG_WARN("failed to init array of block max params", K(ret), K(field_cnt));
  } else if (OB_FAIL(block_max_params_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate array of block max params", K(ret), K(field_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      ObDASIRScanCtDef *ir_ctdef = ctdef.get_ir_ctdef(i);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(i));
      } else if (!ir_ctdef->need_block_max_scan()) {
        // Non-scoring sync leaves need no param.
      } else if (OB_FAIL(block_max_params_[i].init(*ir_ctdef, allocator_))) {
        LOG_WARN("failed to init block max param", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    block_max_params_initialized_ = true;
  } else {
    block_max_params_.reset();
  }
  return ret;
}

int ObDASMultiMatchRtDef::can_pushdown_filter_to_bmm(
    ObDASSearchCtx &search_ctx,
    bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  pushdown_filter_op_ = nullptr;
  has_bmm_build_options_ = false;
  pushdown_filter_cost_.reset();
  query_optional_ = false;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  ObDatum *match_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (ctdef->ir_ctdef_indices_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no ir ctdef", KR(ret));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("failed to eval match_operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
    LOG_WARN("failed to eval minimum_should_match", KR(ret));
  } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
    LOG_WARN("failed to eval type", KR(ret));
  } else if (OB_UNLIKELY(match_operator_datum->is_null()
      || min_should_match_datum->is_null()
      || type_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(match_operator_datum), KPC(min_should_match_datum), KPC(type_datum));
  } else {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t type_val = type_datum->get_int();
    const bool is_supported_type =
        (ObMatchFieldsType::MATCH_BEST_FIELDS == type_val
        || ObMatchFieldsType::MATCH_MOST_FIELDS == type_val) && 1 == field_cnt;
    const bool is_supported_msm = !ctdef->is_msm_unresolved_expr_
        && min_should_match_datum->get_int() >= 0
        && min_should_match_datum->get_int() <= 1;
    const bool base_shape_supported =
        ObMatchOperator::MATCH_OPERATOR_OR == match_operator_datum->get_int()
        && is_supported_msm
        && ctdef->is_top_level_scoring()
        && is_supported_type;
    int64_t effective_msm = 0;
    ObDatum *query_text_datum = nullptr;
    ObSEArray<ObString, 16> query_tokens;
    if (!base_shape_supported) {
      // can_pushdown stays false
    } else if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
      LOG_WARN("failed to eval query text for bmm pushdown", KR(ret));
    } else if (OB_UNLIKELY(query_text_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query text for bmm pushdown", KR(ret));
    } else if (OB_ISNULL(ctdef->get_ir_ctdef(0))
        || OB_ISNULL(ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ir ctdef for bmm pushdown", KR(ret));
    } else if (OB_FAIL(ObFTSTokenizer::tokenize(
        allocator_,
        query_text_datum->get_string(),
        ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_name(),
        ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_property(),
        ctdef->query_text_->obj_meta_,
        query_tokens))) {
      LOG_WARN("failed to tokenize query text for bmm pushdown", KR(ret));
    } else if (query_tokens.empty()) {
      // can_pushdown stays false
    } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
        allocator_, query_tokens.count(), ctdef->is_msm_unresolved_expr_,
        min_should_match_datum, effective_msm))) {
      LOG_WARN("failed to calc multi_match msm for bmm pushdown", KR(ret));
    } else {
      can_pushdown = effective_msm <= 1;
    }
    if (OB_SUCC(ret) && can_pushdown && ctdef->is_async_index()) {
      for (int64_t i = 0; OB_SUCC(ret) && can_pushdown && i < field_cnt; ++i) {
        ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(i);
        uint64_t index_table_id = OB_INVALID_ID;
        int64_t max_segment_cnt = 0;
        if (OB_ISNULL(ir_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async multi-match field", K(ret), K(i));
        } else if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
                       *ir_ctdef, search_ctx, index_table_id, max_segment_cnt))) {
          LOG_WARN("failed to resolve async multi-match filter pushdown domain", K(ret), K(i));
        } else {
          can_pushdown = 1 == max_segment_cnt;
        }
      }
    }
  }
  return ret;
}

int ObDASMultiMatchRtDef::generate_field_op(
    const int64_t field_idx,
    const ObIArray<ObString> &query_tokens,
    const double field_token_boost,
    const int64_t match_operator,
    const int64_t min_should_match,
    const int64_t match_fields_type,
    const ObDASSearchCost &lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&field_op)
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef(field_idx) : nullptr;
  ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(field_idx);
  ObSEArray<ObIDASSearchOp *, 8> token_ops;
  const bool is_async_index = OB_NOT_NULL(ctdef) && ctdef->is_async_index();
  const bool has_pushdown_filter = nullptr != pushdown_filter_op_;
  field_op = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null multi-match field definition", K(ret), K(field_idx),
             KP(ctdef), KP(ir_ctdef), KP(ir_rtdef));
  } else if (OB_FAIL(token_ops.reserve(query_tokens.count()))) {
    LOG_WARN("failed to reserve multi-match field token ops", K(ret), K(field_idx));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < query_tokens.count(); ++j) {
    ObIDASSearchOp *token_op = nullptr;
    storage::ObBlockMaxScoreIterParam *block_max_param =
        !is_async_index && ir_ctdef->need_block_max_scan()
            ? &block_max_params_[field_idx] : nullptr;
    if (OB_FAIL(ObDASSearchUtils::create_token_search_op(
            *ir_ctdef, *ir_rtdef, block_max_param, query_tokens.at(j), field_token_boost,
            ctdef->is_scoring(), search_ctx, token_op))) {
      LOG_WARN("failed to create multi-match token op", K(ret), K(field_idx), K(j));
    } else if (OB_FAIL(token_ops.push_back(token_op))) {
      LOG_WARN("failed to append multi-match token op", K(ret), K(field_idx), K(j));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (1 == token_ops.count() && !has_pushdown_filter) {
    field_op = token_ops.at(0);
  } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator
             || (ObMatchOperator::MATCH_OPERATOR_OR == match_operator
                 && min_should_match >= query_tokens.count()
                 && !has_pushdown_filter)) {
    ObDASConjunctionOp *conj_op = nullptr;
    ObDASConjunctionOpParam conj_param(token_ops);
    conj_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(conj_param, conj_op))) {
      LOG_WARN("failed to create multi-match field conjunction", K(ret), K(field_idx));
    } else {
      field_op = conj_op;
    }
  } else if (ObMatchOperator::MATCH_OPERATOR_OR == match_operator) {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    if (!ctdef->is_top_level_scoring()
        || (field_cnt > 1 && ObMatchFieldsType::MATCH_MOST_FIELDS == match_fields_type)) {
      ObDASDisjunctionOp *disj_op = nullptr;
      ObDASDisjunctionOpParam disj_param(token_ops, min_should_match, false, lead_cost);
      disj_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(disj_param, disj_op))) {
        LOG_WARN("failed to create multi-match field disjunction", K(ret), K(field_idx));
      } else {
        field_op = disj_op;
      }
    } else if (min_should_match > 1) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_param(token_ops, min_should_match, allocator_);
      bmw_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(bmw_param, bmw_op))) {
        LOG_WARN("failed to create multi-match field bmw op", K(ret), K(field_idx));
      } else {
        field_op = bmw_op;
      }
    } else {
      ObDASBMMOp *bmm_op = nullptr;
      const int64_t token_cnt = query_tokens.count();
      if (OB_FAIL(align_cost_array(
              search_ctx, field_token_costs_,
              ctdef->ir_ctdef_indices_.count() * token_cnt, "token"))) {
        LOG_WARN("failed to align multi-match field BMM costs", K(ret), K(field_idx));
      } else {
        ObDASSearchCost *token_cost_base = token_cnt > 0
            ? &field_token_costs_.at(field_idx * token_cnt) : nullptr;
        ObArrayHelper<ObDASSearchCost> token_costs(token_cnt, token_cost_base, token_cnt);
        ObDASBMMBuildOptions bmm_options = has_bmm_build_options_
            ? bmm_build_options_ : ObDASBMMBuildOptions();
        bmm_options.query_optional_ = query_optional_;
        bmm_options.lead_cost_ = lead_cost;
        bmm_options.filter_cost_ = pushdown_filter_cost_;
        ObDASBMMOpParam bmm_param(
            token_ops, pushdown_filter_op_, allocator_, bmm_options, &token_costs);
        bmm_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(bmm_param, bmm_op))) {
          LOG_WARN("failed to create multi-match field bmm op", K(ret), K(field_idx));
        } else {
          field_op = bmm_op;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected multi-match operator", K(ret), K(match_operator));
  }
  return ret;
}

/*
                NOT top-level scoring      top-level scoring

  BEST_FIELDS:      disj max op               disj max op
                     /   |   \                 /   |   \  
                 disj op / conj op     bmm op / bmw op / conj op

  MOST_FIELDS:        disj op                   bmm op
                     /   |   \                 /   |   \
                 disj op / conj op         disj op / conj op
 */

int ObDASMultiMatchRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASBaseCtDef *base_ctdef = ctdef_;
  DEFER(pushdown_filter_op_ = nullptr; pushdown_filter_cost_.reset(); query_optional_ = false;
        has_bmm_build_options_ = false);
  op = nullptr;
  if (OB_ISNULL(base_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null multi-match definition", K(ret));
  } else if (OB_UNLIKELY(DAS_OP_MULTI_MATCH_QUERY != op_type_
                         || DAS_OP_MULTI_MATCH_QUERY != base_ctdef->op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid multi-match runtime definition",
             K(ret), K(op_type_), K(base_ctdef->op_type_));
  } else {
    const ObDASMultiMatchCtDef *ctdef =
        static_cast<const ObDASMultiMatchCtDef *>(base_ctdef);
    if (OB_FAIL(ObDASSearchUtils::generate_async_compound_query_op(
            *this, *ctdef, ctdef->is_async_index(), pushdown_filter_op_,
            lead_cost, search_ctx, generate_op_impl_adapter, op))) {
      LOG_WARN("failed to generate multi-match query op", K(ret));
    }
  }
  return ret;
}

int ObDASMultiMatchRtDef::generate_op_impl_adapter(
    ObIDASSearchRtDef &rtdef,
    const ObIDASSearchCtDef &ctdef,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DAS_OP_MULTI_MATCH_QUERY != rtdef.op_type_
                  || DAS_OP_MULTI_MATCH_QUERY != ctdef.op_type_
                  || rtdef.ctdef_ != &ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid multi-match generator binding",
             K(ret), K(rtdef.op_type_), K(ctdef.op_type_),
             KP(rtdef.ctdef_), KP(&ctdef));
  } else {
    ObDASMultiMatchRtDef &multi_match_rtdef =
        static_cast<ObDASMultiMatchRtDef &>(rtdef);
    ret = multi_match_rtdef.generate_op_impl(lead_cost, search_ctx, op);
  }
  return ret;
}

int ObDASMultiMatchRtDef::generate_op_impl(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare multi-match query tokens", KR(ret));
  }

  ObDatum *boost_datum = nullptr;
  ObDatum *match_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;

  if (OB_FAIL(ret)) {
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
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t token_cnt = query_tokens_.count();
    const bool is_async_index = ctdef->is_async_index();
    const bool parent_owns_async_segment_binding = is_async_index
        && common::OB_INVALID_ID != search_ctx.get_async_index_table_id();
    int64_t min_should_match = 0;
    if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("failed to eval boost", KR(ret));
    } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
      LOG_WARN("failed to eval match_operator", KR(ret));
    } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
      LOG_WARN("failed to eval minimum_should_match", KR(ret));
    } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
      LOG_WARN("failed to eval type", KR(ret));
    } else if (OB_UNLIKELY(match_operator_datum->is_null()
        || min_should_match_datum->is_null() || type_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", KR(ret), KPC(match_operator_datum),
              KPC(min_should_match_datum), KPC(type_datum));
    } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(allocator_, token_cnt,
                       ctdef->is_msm_unresolved_expr_, min_should_match_datum, min_should_match))) {
      LOG_WARN("failed to calculate multi_match minimum_should_match", KR(ret), K(token_cnt), KPC(min_should_match_datum));
    }

    ObSEArray<ObIDASSearchOp *, 8> field_ops;
    if (FAILEDx(field_ops.reserve(field_cnt))) {
      LOG_WARN("failed to reserve for field ops", K(ret));
    } else if (!is_async_index && OB_FAIL(init_block_max_params(*ctdef, field_cnt))) {
      LOG_WARN("failed to init block max params", KR(ret), KPC(ctdef), K(field_cnt), KPC(min_should_match_datum));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(i);
      ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(i);
      ObDASAsyncBindingGuard binding_guard(search_ctx);
      ObDatum *field_boost_datum = nullptr;
      ObSEArray<ObIDASSearchOp *, 4> segment_ops;
      uint64_t index_table_id = OB_INVALID_ID;
      int64_t segment_op_cnt = 1;
      if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef) || OB_ISNULL(ir_rtdef->eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(i));
      } else if (OB_FAIL(ctdef->field_boosts_.at(i)->eval(*eval_ctx_, field_boost_datum))) {
        LOG_WARN("failed to eval field boost", K(ret), K(i));
      } else if (field_boost_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(i));
      } else if (is_async_index) {
        if (OB_FAIL(ObDASSearchUtils::resolve_async_field_segment_domain(
                *ir_ctdef, search_ctx, binding_guard,
                index_table_id, segment_op_cnt))) {
          LOG_WARN("failed to resolve async multi-match segment domain", K(ret), K(i));
        }
      }
      for (int64_t s = 0; OB_SUCC(ret) && s < segment_op_cnt; ++s) {
        ObIDASSearchOp *segment_op = nullptr;
        if (OB_FAIL(generate_field_op(
                i, query_tokens_,
                field_boost_datum->get_double() * boost_datum->get_double(),
                match_operator_datum->get_int(), min_should_match, type_datum->get_int(),
                lead_cost, search_ctx, segment_op))) {
          LOG_WARN("failed to generate multi-match field subtree", K(ret), K(i), K(s));
        } else if (OB_ISNULL(segment_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null multi-match field subtree", K(ret), K(i), K(s));
        } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
          LOG_WARN("failed to append multi-match segment subtree", K(ret), K(i), K(s));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_async_index || parent_owns_async_segment_binding) {
        if (OB_FAIL(field_ops.push_back(segment_ops.at(0)))) {
          LOG_WARN("failed to append direct multi-match field", K(ret), K(i));
        }
      } else {
        ObIDASSearchOp *merge_op = nullptr;
        if (OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
                segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, merge_op))) {
          LOG_WARN("failed to create per-index multi-match segment merge",
                   K(ret), K(i), K(index_table_id));
        } else if (OB_FAIL(field_ops.push_back(merge_op))) {
          LOG_WARN("failed to append multi-match segment merge", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (1 == field_ops.count()) {
        op = field_ops.at(0);
      } else if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_datum->get_int()) {
        ObDASDisjunctionOp *disj_op = nullptr;
        ObDASDisjunctionOpParam disj_op_param(field_ops, 1, true, lead_cost);
        disj_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
          LOG_WARN("failed to create disjunction op", KR(ret));
        } else {
          op = disj_op;
        }
      } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_datum->get_int()) {
        if (!ctdef->is_top_level_scoring()) {
          ObDASDisjunctionOp *disj_op = nullptr;
          ObDASDisjunctionOpParam disj_op_param(field_ops, 1, false, lead_cost);
          disj_op_param.set_is_scoring(ctdef->is_scoring());
          if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
            LOG_WARN("failed to create disjunction op", KR(ret));
          } else {
            op = disj_op;
          }
        } else {
          ObDASBMMOp *bmm_op = nullptr;
          if (OB_FAIL(align_cost_array(search_ctx, field_costs_, field_cnt, "field"))) {
            LOG_WARN("failed to align multi-match MOST_FIELDS BMM costs", KR(ret));
          } else {
            ObDASBMMBuildOptions build_options;
            build_options.query_optional_ = query_optional_;
            build_options.lead_cost_ = lead_cost;
            build_options.filter_cost_ = pushdown_filter_cost_;
            ObDASBMMOpParam bmm_op_param(
                field_ops, pushdown_filter_op_, allocator_, build_options, &field_costs_);
            bmm_op_param.set_is_scoring(ctdef->is_scoring());
            if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
              LOG_WARN("failed to create bmm op", KR(ret));
            } else {
              op = bmm_op;
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match fields type", K(ret), K(type_datum->get_int()));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
