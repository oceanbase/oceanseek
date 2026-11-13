/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <cmath>

#include "ob_das_query_string_query.h"
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

OB_SERIALIZE_MEMBER((ObDASQueryStringCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    default_operator_,
                    minimum_should_match_,
                    type_,
                    field_boosts_,
                    ir_ctdef_indices_,
                    is_msm_unresolved_expr_);

OB_SERIALIZE_MEMBER((ObDASQueryStringRtDef, ObIDASSearchRtDef));

void ObDASQueryStringRtDef::reset_cached_runtime_state()
{
  token_groups_.reuse();
  token_boosts_.reuse();
  field_token_costs_.reuse();
  field_costs_.reuse();
  group_costs_.reuse();
  token_count_ = 0;
  query_groups_prepared_ = false;
}

int ObDASQueryStringRtDef::copy_cached_runtime_state_from(const ObIDASSearchRtDef &other)
{
  int ret = OB_SUCCESS;
  reset_cached_runtime_state();
  if (OB_UNLIKELY(DAS_OP_QUERY_STRING_QUERY != other.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search rtdef type for query-string runtime state copy",
        KR(ret), K(other.op_type_));
  } else {
    const ObDASQueryStringRtDef &src = static_cast<const ObDASQueryStringRtDef &>(other);
    if (OB_FAIL(ObIDASSearchRtDef::copy_cached_runtime_state_from(other))) {
      LOG_WARN("failed to copy base search runtime state", KR(ret));
    } else if (OB_FAIL(token_groups_.reserve(src.token_groups_.count()))) {
      LOG_WARN("failed to reserve copied query-string groups", KR(ret), K(src.token_groups_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.token_groups_.count(); ++i) {
      ObArray<ObString> tokens;
      if (OB_FAIL(tokens.reserve(src.token_groups_.at(i).count()))) {
        LOG_WARN("failed to reserve copied query-string tokens", KR(ret), K(i));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < src.token_groups_.at(i).count(); ++j) {
        ObString copied_token;
        if (OB_FAIL(ob_write_string(allocator_, src.token_groups_.at(i).at(j), copied_token))) {
          LOG_WARN("failed to deep copy query-string token", KR(ret), K(i), K(j));
        } else if (OB_FAIL(tokens.push_back(copied_token))) {
          LOG_WARN("failed to append copied query-string token", KR(ret), K(i), K(j));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(token_groups_.push_back(tokens))) {
        LOG_WARN("failed to append copied query-string group", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(token_boosts_.assign(src.token_boosts_))) {
      LOG_WARN("failed to copy query-string token boosts", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(field_token_costs_.assign(src.field_token_costs_))) {
      LOG_WARN("failed to copy query-string field token costs", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(field_costs_.assign(src.field_costs_))) {
      LOG_WARN("failed to copy query-string field costs", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(group_costs_.assign(src.group_costs_))) {
      LOG_WARN("failed to copy query-string group costs", KR(ret));
    } else if (OB_SUCC(ret)) {
      token_count_ = src.token_count_;
      query_groups_prepared_ = src.query_groups_prepared_;
    }
  }
  if (OB_FAIL(ret)) {
    reset_cached_runtime_state();
    reset_cost();
  }
  return ret;
}

int ObDASQueryStringRtDef::prepare_query_groups()
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  if (query_groups_prepared_) {
    // Reuse token groups prepared while computing the async cost.
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->query_text_)
      || ctdef->ir_ctdef_indices_.empty()
      || OB_ISNULL(ctdef->get_ir_ctdef(0))
      || OB_ISNULL(ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query-string definition", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else {
    token_groups_.reuse();
    token_boosts_.reuse();
    token_count_ = 0;
    if (OB_FAIL(parse_query_string())) {
      token_groups_.reuse();
      token_boosts_.reuse();
      LOG_WARN("failed to parse query string", KR(ret));
    } else {
      for (int64_t i = 0; i < token_groups_.count(); ++i) {
        token_count_ += token_groups_.at(i).count();
      }
      query_groups_prepared_ = true;
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::compute_async_query_string_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDatum *default_operator_datum = nullptr;
  ObDatum *minimum_should_match_datum = nullptr;
  int64_t effective_msm = 0;
  ObSEArray<ObString, 16> all_tokens;
  ObSEArray<int64_t, 8> group_end_offsets;
  ObSEArray<ObSEArray<int64_t, 4>, 8> raw_group_field_costs;
  ObSEArray<int64_t, 8> raw_group_costs;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->default_operator_)
      || OB_ISNULL(ctdef->minimum_should_match_) || ctdef->ir_ctdef_indices_.empty()
      || !search_ctx.get_row_count().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid query-string cost context", KR(ret), KP(ctdef),
        KP_(eval_ctx), K(search_ctx.get_row_count()));
  } else if (OB_FAIL(ctdef->default_operator_->eval(*eval_ctx_, default_operator_datum))) {
    LOG_WARN("failed to eval query-string default operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(
      *eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("failed to eval query-string minimum should match", KR(ret));
  } else if (OB_ISNULL(default_operator_datum) || OB_ISNULL(minimum_should_match_datum)
      || OB_UNLIKELY(default_operator_datum->is_null()
          || minimum_should_match_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query-string cost datum", KR(ret),
        KPC(default_operator_datum), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
      allocator_, token_groups_.count(), ctdef->is_msm_unresolved_expr_,
      minimum_should_match_datum, effective_msm))) {
    LOG_WARN("failed to calculate query-string minimum_should_match for cost estimation",
        KR(ret), K(token_groups_.count()), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(all_tokens.reserve(token_count_))) {
    LOG_WARN("failed to reserve query-string tokens", KR(ret), K_(token_count));
  } else if (OB_FAIL(group_end_offsets.reserve(token_groups_.count()))) {
    LOG_WARN("failed to reserve query-string group offsets", KR(ret));
  } else if (OB_FAIL(raw_group_field_costs.reserve(token_groups_.count()))) {
    LOG_WARN("failed to reserve query-string group field costs", KR(ret));
  } else if (OB_FAIL(raw_group_costs.reserve(token_groups_.count()))) {
    LOG_WARN("failed to reserve query-string group costs", KR(ret));
  }

  // Only top-level scoring queries feed costs into BMM operators: field-level
  // details are consumed only when there is a single token group, and group
  // costs are consumed by the outermost group BMM. Skip caching otherwise.
  const bool top_scoring = OB_NOT_NULL(ctdef) && ctdef->is_top_level_scoring();
  const bool cache_field_costs = top_scoring && 1 == token_groups_.count();
  if (OB_SUCC(ret)) {
    field_token_costs_.reuse();
    field_costs_.reuse();
    group_costs_.reuse();
    if (top_scoring && OB_FAIL(group_costs_.reserve(token_groups_.count()))) {
      LOG_WARN("failed to reserve query-string group cost metadata", KR(ret));
    } else if (cache_field_costs && OB_FAIL(field_token_costs_.reserve(
                   ctdef->ir_ctdef_indices_.count() * token_count_))) {
      LOG_WARN("failed to reserve query-string token cost metadata", KR(ret));
    } else if (cache_field_costs
               && OB_FAIL(field_costs_.reserve(ctdef->ir_ctdef_indices_.count()))) {
      LOG_WARN("failed to reserve query-string field cost metadata", KR(ret));
    }
  }

  const int64_t table_row_count = search_ctx.get_row_count().cost();
  for (int64_t i = 0; OB_SUCC(ret) && i < token_groups_.count(); ++i) {
    const ObIArray<ObString> &tokens = token_groups_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < tokens.count(); ++j) {
      if (OB_FAIL(all_tokens.push_back(tokens.at(j)))) {
        LOG_WARN("failed to append query-string token", KR(ret), K(i), K(j));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(group_end_offsets.push_back(all_tokens.count()))) {
      LOG_WARN("failed to append query-string group offset", KR(ret), K(i));
    } else if (OB_SUCC(ret)) {
      ObSEArray<int64_t, 4> field_costs;
      if (OB_FAIL(field_costs.reserve(ctdef->ir_ctdef_indices_.count()))) {
        LOG_WARN("failed to reserve query-string field costs", KR(ret), K(i));
      } else if (OB_FAIL(raw_group_field_costs.push_back(field_costs))) {
        LOG_WARN("failed to initialize query-string group field costs", KR(ret), K(i));
      }
    }
  }

  for (int64_t field_idx = 0;
       OB_SUCC(ret) && field_idx < ctdef->ir_ctdef_indices_.count();
       ++field_idx) {
    ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(field_idx);
    ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(field_idx);
    ObSEArray<int64_t, 16> token_costs;
    if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query-string field definition",
          KR(ret), K(field_idx), KP(ir_ctdef), KP(ir_rtdef));
    } else if (OB_FAIL(token_costs.reserve(all_tokens.count()))) {
      LOG_WARN("failed to reserve query-string token costs", KR(ret), K(field_idx));
    } else if (OB_FAIL(ObDASFtsCostUtils::estimate_async_token_doc_cnts(
        search_ctx, *ir_ctdef, *ir_rtdef, all_tokens, token_costs))) {
      LOG_WARN("failed to estimate query-string token document counts",
          KR(ret), K(field_idx));
    }

    int64_t group_begin = 0;
    for (int64_t group_idx = 0;
         OB_SUCC(ret) && group_idx < group_end_offsets.count();
         ++group_idx) {
      const int64_t group_end = group_end_offsets.at(group_idx);
      ObSEArray<int64_t, 16> group_token_costs;
      int64_t field_cost = 0;
      if (OB_FAIL(group_token_costs.reserve(group_end - group_begin))) {
        LOG_WARN("failed to reserve query-string group token costs",
            KR(ret), K(group_idx), K(field_idx));
      }
      for (int64_t token_idx = group_begin;
           OB_SUCC(ret) && token_idx < group_end;
           ++token_idx) {
        if (OB_FAIL(group_token_costs.push_back(token_costs.at(token_idx)))) {
          LOG_WARN("failed to append query-string group token cost",
              KR(ret), K(group_idx), K(field_idx), K(token_idx));
        } else if (cache_field_costs && OB_FAIL(field_token_costs_.push_back(
            ObDASSearchCost(token_costs.at(token_idx))))) {
          LOG_WARN("failed to save query-string group token cost",
              KR(ret), K(group_idx), K(field_idx), K(token_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDASFtsCostUtils::aggregate_match_cost(
          group_token_costs, default_operator_datum->get_int(),
          1 /* group-level minimum_should_match is aggregated below */,
          table_row_count, field_cost))) {
        LOG_WARN("failed to aggregate query-string group field cost",
            KR(ret), K(group_idx), K(field_idx));
      } else if (OB_FAIL(raw_group_field_costs.at(group_idx).push_back(field_cost))) {
        LOG_WARN("failed to append query-string group field cost",
            KR(ret), K(group_idx), K(field_idx));
      } else if (cache_field_costs
          && OB_FAIL(field_costs_.push_back(ObDASSearchCost(field_cost)))) {
        LOG_WARN("failed to save query-string group field cost",
            KR(ret), K(group_idx), K(field_idx));
      }
      group_begin = group_end;
    }
  }

  for (int64_t group_idx = 0;
       OB_SUCC(ret) && group_idx < raw_group_field_costs.count();
       ++group_idx) {
    int64_t group_cost = 0;
    if (OB_FAIL(ObDASFtsCostUtils::aggregate_or_cost(
        raw_group_field_costs.at(group_idx), table_row_count, group_cost))) {
      LOG_WARN("failed to aggregate query-string group field costs", KR(ret), K(group_idx));
    } else if (OB_FAIL(raw_group_costs.push_back(group_cost))) {
      LOG_WARN("failed to append query-string group cost", KR(ret), K(group_idx));
    } else if (top_scoring && OB_FAIL(group_costs_.push_back(ObDASSearchCost(group_cost)))) {
      LOG_WARN("failed to save query-string group cost", KR(ret), K(group_idx));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t query_cost = 0;
    if (OB_FAIL(ObDASFtsCostUtils::aggregate_match_cost(
        raw_group_costs, default_operator_datum->get_int(),
        effective_msm, table_row_count, query_cost))) {
      LOG_WARN("failed to aggregate query-string group cost", KR(ret));
    } else {
      cost = ObDASSearchCost(query_cost);
    }
  }
  if (OB_FAIL(ret)) {
    field_token_costs_.reuse();
    field_costs_.reuse();
    group_costs_.reuse();
  }
  return ret;
}

int ObDASQueryStringRtDef::compute_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query-string ctdef", KR(ret));
  } else if (!ctdef->is_async_index()) {
    // Non-async query-string keeps an invalid cost and falls back to the table row count.
  } else if (OB_FAIL(prepare_query_groups())) {
    LOG_WARN("failed to prepare query-string groups for cost estimation", KR(ret));
  } else if (token_groups_.empty()) {
    cost = ObDASSearchCost(0);
  } else if (OB_FAIL(compute_async_query_string_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute async query-string cost", KR(ret));
  }
  return ret;
}

int ObDASQueryStringRtDef::align_cost_array(
    ObDASSearchCtx &search_ctx,
    ObArray<ObDASSearchCost> &costs,
    const int64_t expected_cnt,
    const char *cost_name)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  bool costs_aligned = costs.count() == expected_cnt;
  if (OB_ISNULL(ctdef) || OB_UNLIKELY(expected_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query-string BMM cost alignment argument",
        KR(ret), KP(ctdef), K(expected_cnt), K(cost_name));
  } else if (!costs_aligned) {
    // Cold path: trigger cost estimation, which fills the cached cost arrays.
    ObDASSearchCost boundary_cost;
    if (OB_FAIL(get_cost(search_ctx, boundary_cost))) {
      LOG_WARN("failed to obtain cold query-string BMM boundary cost", KR(ret), K(cost_name));
    }
    costs_aligned = costs.count() == expected_cnt;
  }

  if (OB_FAIL(ret)) {
  } else if (ctdef->is_async_index()) {
    if (!costs_aligned) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mismatched async query-string costs",
          KR(ret), K(expected_cnt), K(costs.count()), K(cost_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_cnt; ++i) {
      if (OB_UNLIKELY(!costs.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid async query-string cost", KR(ret), K(i), K(cost_name));
      }
    }
  } else if (!costs_aligned) {
    costs.reuse();
    if (OB_FAIL(costs.reserve(expected_cnt))) {
      LOG_WARN("failed to reserve aligned query-string costs", KR(ret), K(expected_cnt), K(cost_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_cnt; ++i) {
      if (OB_FAIL(costs.push_back(ObDASSearchCost()))) {
        LOG_WARN("failed to append invalid query-string cost", KR(ret), K(i), K(cost_name));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctdef) && !ctdef->is_async_index()) {
    costs.reuse();
  }
  return ret;
}

int ObDASQueryStringRtDef::init_block_max_params(
    const ObDASQueryStringCtDef &ctdef,
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
        // No block-max fields are generated for need_score=false; skip init.
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

int ObDASQueryStringRtDef::parse_query_string()
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDatum *query_text_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->query_text_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query-string expression", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
    LOG_WARN("failed to eval query text", K(ret));
  } else if (OB_UNLIKELY(query_text_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KPC(query_text_datum));
  } else {
    const ObString &original_string = query_text_datum->get_string();
    const ObDASIRScanCtDef &ir = *ctdef->get_ir_ctdef(0);
    const ObDASSearchIndexTextInfo &info = ir.search_index_fulltext_info_;
    const ObString &parser_name = info.has_search_index_fulltext()
        ? info.parser_name_ : ir.get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_name();
    const ObString &parser_property = info.has_search_index_fulltext()
        ? info.parser_properties_ : ir.get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_property();
    const ObObjMeta &meta = ctdef->query_text_->obj_meta_;
    const ObCollationType src_coll = ctdef->query_text_->datum_meta_.cs_type_;
    const ObCollationType dst_coll = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    ObString query_string;
    if (src_coll != dst_coll) {
      ObString tmp_string;
      if (OB_FAIL(ObCharset::tolower(src_coll, original_string, tmp_string, allocator_))) {
        LOG_WARN("failed to casedown string", K(ret), K(src_coll), K(original_string));
      } else if (OB_FAIL(ObCharset::charset_convert(
          allocator_, tmp_string, src_coll, dst_coll, query_string))) {
        LOG_WARN("failed to convert string", K(ret), K(src_coll), K(original_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(src_coll, original_string, query_string, allocator_))) {
      LOG_WARN("failed to casedown string", K(ret), K(src_coll), K(original_string));
    }
    if (FAILEDx(check_reserved_operators(query_string))) {
      LOG_WARN("query has reserved operators", K(ret));
    }
    const char separator = ' ';
    const char boost_op = '^';
    ObArray<ObString> empty;
    empty.set_attr(ObMemAttr(MTL_ID(), "DASQStrTok"));
    while (OB_SUCC(ret) && !query_string.empty()) {
      ObString boosted_text = query_string.split_on(boost_op);
      if (boosted_text.empty()) {
        ObString tmp_string;
        if (!token_groups_.empty() && token_groups_.at(token_groups_.count() - 1).empty()) {
          // skip adding a group
          token_boosts_.at(token_boosts_.count() - 1) = 1.0;
        } else if (OB_FAIL(token_groups_.push_back(empty))) {
          LOG_WARN("failed to append token groups", K(ret));
        } else if (OB_FAIL(token_boosts_.push_back(1.0))) {
          LOG_WARN("failed to append token boosts", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (src_coll == dst_coll) {
          tmp_string = query_string;
        } else if (OB_FAIL(ObCharset::charset_convert(
            allocator_, query_string, dst_coll, src_coll, tmp_string))) {
          LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(query_string));
        }
        if (FAILEDx(ObFTSTokenizer::tokenize(
            allocator_, tmp_string, parser_name, parser_property, meta,
            token_groups_.at(token_groups_.count() - 1)))) {
          LOG_WARN("failed to tokenize query text", K(ret));
        } else {
          query_string.reset();
        }
      } else {
        ObString unboosted_text = boosted_text.split_on(boosted_text.reverse_find(separator));
        ObString boost_string = query_string.split_on(separator);
        if (boost_string.empty()) {
          boost_string.assign(query_string.ptr(), query_string.length());
          query_string.reset();
        }
        if (!unboosted_text.empty()) {
          ObString tmp_string;
          if (!token_groups_.empty() && token_groups_.at(token_groups_.count() - 1).empty()) {
            // skip adding a group
            token_boosts_.at(token_boosts_.count() - 1) = 1.0;
          } else if (OB_FAIL(token_groups_.push_back(empty))) {
            LOG_WARN("failed to append token groups", K(ret));
          } else if (OB_FAIL(token_boosts_.push_back(1.0))) {
            LOG_WARN("failed to append token boosts", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (src_coll == dst_coll) {
            tmp_string = unboosted_text;
          } else if (OB_FAIL(ObCharset::charset_convert(
              allocator_, unboosted_text, dst_coll, src_coll, tmp_string))) {
            LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(unboosted_text));
          }
          if (FAILEDx(ObFTSTokenizer::tokenize(
              allocator_, tmp_string, parser_name, parser_property, meta,
              token_groups_.at(token_groups_.count() - 1)))) {
            LOG_WARN("failed to tokenize unboosted text", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(boost_string.empty())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: malformed query");
          LOG_WARN("malformed query in query string query", K(ret));
        } else {
          char *boost_str = static_cast<char *>(allocator_.alloc(boost_string.length() + 1));
          if (OB_ISNULL(boost_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(boost_str, boost_string.ptr(), boost_string.length());
            boost_str[boost_string.length()] = '\0';
            char *end_ptr = nullptr;
            double boost = strtod(boost_str, &end_ptr);
            if (OB_UNLIKELY(end_ptr != boost_str + boost_string.length())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: malformed query");
              LOG_WARN("failed to parse boost", K(ret));
            } else if (OB_UNLIKELY(!std::isfinite(boost) || boost <= 0.0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                             "query_string query: token boost must be a finite positive number");
              LOG_WARN("unexpected non-finite or non-positive boost", K(ret), K(boost));
            }
            ObString tmp_string;
            if (OB_FAIL(ret)) {
            } else if (!token_groups_.empty()
                && token_groups_.at(token_groups_.count() - 1).empty()) {
              // skip adding a group
              token_boosts_.at(token_boosts_.count() - 1) = boost;
            } else if (OB_FAIL(token_groups_.push_back(empty))) {
              LOG_WARN("failed to append token groups", K(ret));
            } else if (OB_FAIL(token_boosts_.push_back(boost))) {
              LOG_WARN("failed to append token boosts", K(ret));
            }
            if (OB_FAIL(ret)) {
            } else if (src_coll == dst_coll) {
              tmp_string = boosted_text;
            } else if (OB_FAIL(ObCharset::charset_convert(
                allocator_, boosted_text, dst_coll, src_coll, tmp_string))) {
              LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(boosted_text));
            }
            if (FAILEDx(ObFTSTokenizer::tokenize(
                allocator_, tmp_string, parser_name, parser_property, meta,
                token_groups_.at(token_groups_.count() - 1)))) {
              LOG_WARN("failed to tokenize boosted text", K(ret));
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!token_groups_.empty() && token_groups_.at(token_groups_.count() - 1).empty()) {
      token_groups_.pop_back();
      token_boosts_.pop_back();
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::check_reserved_operators(const ObString &query)
{
  int ret = OB_SUCCESS;
  if (!query.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(RESERVED_CHARS); ++i) {
      if (NULL != query.find(RESERVED_CHARS[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: query contains reserved character");
        LOG_WARN("query has reserved operator", K(ret), K(query), K(RESERVED_CHARS[i]));
      }
    }

    ObString tmp = query;
    while (OB_SUCC(ret) && !tmp.empty()) {
      const int64_t old_len = tmp.length();
      ObString token = tmp.split_on(' ');
      if (token.empty() && tmp.length() == old_len) {
        token = tmp;
        tmp.reset();
      }
      if (token.empty()) {
        // skip empty token caused by consecutive spaces
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(RESERVED_KEYWORDS); ++i) {
          if (token.compare_equal(RESERVED_KEYWORDS[i])) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: query contains reserved keyword");
            LOG_WARN("query has reserved operator", K(ret), K(query), K(token));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::can_pushdown_filter_to_bmm(
    ObDASSearchCtx &search_ctx,
    bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  pushdown_filter_op_ = nullptr;
  has_bmm_build_options_ = false;
  pushdown_filter_cost_.reset();
  query_optional_ = false;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDatum *default_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(ctdef->default_operator_->eval(*eval_ctx_, default_operator_datum))) {
    LOG_WARN("failed to eval default_operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
    LOG_WARN("failed to eval minimum_should_match", KR(ret));
  } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
    LOG_WARN("failed to eval type", KR(ret));
  } else if (OB_UNLIKELY(default_operator_datum->is_null()
      || min_should_match_datum->is_null()
      || type_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(default_operator_datum), KPC(min_should_match_datum), KPC(type_datum));
  } else {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t type_val = type_datum->get_int();
    const bool is_supported_type =
        (ObMatchFieldsType::MATCH_BEST_FIELDS == type_val && 1 == field_cnt)
        || ObMatchFieldsType::MATCH_MOST_FIELDS == type_val;
    const bool is_supported_msm = !ctdef->is_msm_unresolved_expr_
        && min_should_match_datum->get_int() >= 0
        && min_should_match_datum->get_int() <= 1;
    const bool base_shape_supported = ctdef->is_top_level_scoring()
        && ObMatchOperator::MATCH_OPERATOR_OR == default_operator_datum->get_int()
        && is_supported_msm
        && is_supported_type;
    int64_t effective_msm = 0;
    if (!base_shape_supported) {
      // can_pushdown stays false
    } else if (OB_ISNULL(ctdef->get_ir_ctdef(0))
        || OB_ISNULL(ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ir ctdef for query_string pushdown", KR(ret));
    } else if (OB_FAIL(prepare_query_groups())) {
      LOG_WARN("failed to prepare query string for bmm pushdown", KR(ret));
    } else if (token_groups_.empty()) {
      // can_pushdown stays false
    } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
        allocator_, token_groups_.count(), ctdef->is_msm_unresolved_expr_,
        min_should_match_datum, effective_msm))) {
      LOG_WARN("failed to calc query_string msm for bmm pushdown", KR(ret));
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
          LOG_WARN("null async query-string field", K(ret), K(i));
        } else if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
                       *ir_ctdef, search_ctx, index_table_id, max_segment_cnt))) {
          LOG_WARN("failed to resolve async query-string filter pushdown domain", K(ret), K(i));
        } else {
          can_pushdown = 1 >= max_segment_cnt;
        }
      }
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::generate_group_field_op(
    const int64_t group_idx,
    const int64_t field_idx,
    const ObIArray<ObString> &tokens,
    const double token_boost,
    const int64_t default_operator,
    const int64_t group_cnt,
    const int64_t match_fields_type,
    const ObDASSearchCost &lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&field_op)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDASIRScanCtDef *ir_ctdef = OB_NOT_NULL(ctdef) ? ctdef->get_ir_ctdef(field_idx) : nullptr;
  ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(field_idx);
  ObSEArray<ObIDASSearchOp *, 8> token_ops;
  const bool is_async_index = OB_NOT_NULL(ctdef) && ctdef->is_async_index();
  const bool push_filter_to_field = nullptr != pushdown_filter_op_
      && (!is_async_index
          || (1 == ctdef->ir_ctdef_indices_.count() && 1 == group_cnt));
  field_op = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query-string field definition", K(ret), K(field_idx),
             KP(ctdef), KP(ir_ctdef), KP(ir_rtdef));
  } else if (OB_FAIL(token_ops.reserve(tokens.count()))) {
    LOG_WARN("failed to reserve query-string token ops", K(ret), K(field_idx));
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < tokens.count(); ++k) {
    ObIDASSearchOp *token_op = nullptr;
    storage::ObBlockMaxScoreIterParam *block_max_param =
        !is_async_index && ir_ctdef->need_block_max_scan()
            ? &block_max_params_[field_idx] : nullptr;
    if (OB_FAIL(ObDASSearchUtils::create_token_search_op(
            *ir_ctdef, *ir_rtdef, block_max_param, tokens.at(k), token_boost,
            ctdef->is_scoring(), search_ctx, token_op))) {
      LOG_WARN("failed to create query-string token op", K(ret), K(field_idx), K(k));
    } else if (OB_FAIL(token_ops.push_back(token_op))) {
      LOG_WARN("failed to append query-string token op", K(ret), K(field_idx), K(k));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (1 == token_ops.count() && !push_filter_to_field) {
    field_op = token_ops.at(0);
  } else if (ObMatchOperator::MATCH_OPERATOR_OR == default_operator) {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    if (ctdef->is_top_level_scoring() && 1 == group_cnt
        && (1 == field_cnt || ObMatchFieldsType::MATCH_BEST_FIELDS == match_fields_type)) {
      ObDASBMMOp *bmm_op = nullptr;
      const int64_t token_cnt = tokens.count();
      if (OB_FAIL(align_cost_array(
              search_ctx, field_token_costs_, field_cnt * token_cnt, "token"))) {
        LOG_WARN("failed to align query-string field BMM costs",
            K(ret), K(group_idx), K(field_idx));
      } else {
        ObIDASSearchOp *filter_op = push_filter_to_field ? pushdown_filter_op_ : nullptr;
        const ObDASSearchCost filter_cost = OB_NOT_NULL(filter_op)
            ? pushdown_filter_cost_ : ObDASSearchCost();
        ObDASSearchCost *token_cost_base = token_cnt > 0
            ? &field_token_costs_.at(field_idx * token_cnt) : nullptr;
        ObArrayHelper<ObDASSearchCost> token_costs(token_cnt, token_cost_base, token_cnt);
        ObDASBMMBuildOptions bmm_options = push_filter_to_field && has_bmm_build_options_
            ? bmm_build_options_ : ObDASBMMBuildOptions();
        bmm_options.query_optional_ = query_optional_;
        bmm_options.lead_cost_ = lead_cost;
        bmm_options.filter_cost_ = filter_cost;
        ObDASBMMOpParam bmm_param(
            token_ops, filter_op, allocator_, bmm_options, &token_costs);
        bmm_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(bmm_param, bmm_op))) {
          LOG_WARN("failed to create query-string field bmm op", K(ret), K(field_idx));
        } else {
          field_op = bmm_op;
        }
      }
    } else {
      ObDASDisjunctionOp *disj_op = nullptr;
      ObDASDisjunctionOpParam disj_param(token_ops, 1, false, lead_cost);
      disj_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(disj_param, disj_op))) {
        LOG_WARN("failed to create query-string field disjunction", K(ret), K(field_idx));
      } else {
        field_op = disj_op;
      }
    }
  } else if (ObMatchOperator::MATCH_OPERATOR_AND == default_operator) {
    ObDASConjunctionOp *conj_op = nullptr;
    ObDASConjunctionOpParam conj_param(token_ops);
    conj_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(conj_param, conj_op))) {
      LOG_WARN("failed to create query-string field conjunction", K(ret), K(field_idx));
    } else {
      field_op = conj_op;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected query-string default operator", K(ret), K(default_operator));
  }
  return ret;
}

int ObDASQueryStringRtDef::generate_op(
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
    LOG_WARN("null query-string definition", K(ret));
  } else if (OB_UNLIKELY(DAS_OP_QUERY_STRING_QUERY != op_type_
                         || DAS_OP_QUERY_STRING_QUERY != base_ctdef->op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query-string runtime definition",
             K(ret), K(op_type_), K(base_ctdef->op_type_));
  } else {
    const ObDASQueryStringCtDef *ctdef =
        static_cast<const ObDASQueryStringCtDef *>(base_ctdef);
    if (OB_FAIL(ObDASSearchUtils::generate_async_compound_query_op(
            *this, *ctdef, ctdef->is_async_index(), pushdown_filter_op_,
            lead_cost, search_ctx, generate_op_impl_adapter, op))) {
      LOG_WARN("failed to generate query-string query op", K(ret));
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::generate_op_impl_adapter(
    ObIDASSearchRtDef &rtdef,
    const ObIDASSearchCtDef &ctdef,
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DAS_OP_QUERY_STRING_QUERY != rtdef.op_type_
                  || DAS_OP_QUERY_STRING_QUERY != ctdef.op_type_
                  || rtdef.ctdef_ != &ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query-string generator binding",
             K(ret), K(rtdef.op_type_), K(ctdef.op_type_),
             KP(rtdef.ctdef_), KP(&ctdef));
  } else {
    ObDASQueryStringRtDef &query_string_rtdef =
        static_cast<ObDASQueryStringRtDef &>(rtdef);
    ret = query_string_rtdef.generate_op_impl(lead_cost, search_ctx, op);
  }
  return ret;
}

int ObDASQueryStringRtDef::generate_op_impl(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  const bool has_pushdown_filter = nullptr != pushdown_filter_op_;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(prepare_query_groups())) {
    LOG_WARN("failed to prepare query-string groups", KR(ret));
  }

  ObDatum *boost_datum = nullptr;
  ObDatum *default_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;

  if (OB_FAIL(ret)) {
  } else if (token_groups_.empty()) {
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
    const int64_t group_cnt = token_groups_.count();
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const bool is_async_index = ctdef->is_async_index();
    const bool parent_owns_async_segment_binding = is_async_index
        && common::OB_INVALID_ID != search_ctx.get_async_index_table_id();
    int64_t min_should_match = 0;
    if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("failed to eval boost", KR(ret));
    } else if (OB_FAIL(ctdef->default_operator_->eval(*eval_ctx_, default_operator_datum))) {
      LOG_WARN("failed to eval default_operator", KR(ret));
    } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
      LOG_WARN("failed to eval minimum_should_match", KR(ret));
    } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
      LOG_WARN("failed to eval type", KR(ret));
    } else if (OB_UNLIKELY(default_operator_datum->is_null()
        || min_should_match_datum->is_null() || type_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", KR(ret), KPC(default_operator_datum),
              KPC(min_should_match_datum), KPC(type_datum));
    } else if (OB_FAIL(ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(allocator_, group_cnt,
                       ctdef->is_msm_unresolved_expr_, min_should_match_datum, min_should_match))) {
      LOG_WARN("failed to calculate query_string minimum_should_match", KR(ret), K(group_cnt), KPC(min_should_match_datum));
    }

    ObSEArray<double, 8> field_boosts;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(field_boosts.reserve(field_cnt))) {
      LOG_WARN("Query string failed to reserve field boost array capacity", KR(ret), KPC(min_should_match_datum));
    } else if (!is_async_index && OB_FAIL(init_block_max_params(*ctdef, field_cnt))) {
      LOG_WARN("failed to init block max params", KR(ret), KPC(ctdef), K(field_cnt), KPC(min_should_match_datum));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(i);
      ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(i);
      ObDatum *field_boost_datum = nullptr;
      if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef) || OB_ISNULL(ir_rtdef->eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(i));
      } else if (OB_FAIL(ctdef->field_boosts_.at(i)->eval(*eval_ctx_, field_boost_datum))) {
        LOG_WARN("failed to eval field boost", K(ret), K(i));
      } else if (field_boost_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(i));
      } else if (OB_FAIL(field_boosts.push_back(field_boost_datum->get_double()))) {
        LOG_WARN("failed to append field boost", K(ret));
      }
    }

    ObSEArray<ObIDASSearchOp *, 8> group_ops;
    ObSEArray<ObIDASSearchOp *, 8> field_ops;
    if (FAILEDx(group_ops.reserve(group_cnt))) {
      LOG_WARN("failed to reserve array of group ops", K(ret));
    } else if (OB_FAIL(field_ops.reserve(field_cnt))) {
      LOG_WARN("failed to reserve array of group ops", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group_cnt; ++i) {
      field_ops.reuse();
      const ObIArray<ObString> &tokens = token_groups_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_cnt; ++j) {
        ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(j);
        ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(j);
        ObDASAsyncBindingGuard binding_guard(search_ctx);
        ObSEArray<ObIDASSearchOp *, 4> segment_ops;
        uint64_t index_table_id = OB_INVALID_ID;
        int64_t segment_op_cnt = 1;
        if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null query-string field", K(ret), K(i), K(j));
        } else if (is_async_index) {
          if (OB_FAIL(ObDASSearchUtils::resolve_async_field_segment_domain(
                  *ir_ctdef, search_ctx, binding_guard,
                  index_table_id, segment_op_cnt))) {
            LOG_WARN("failed to resolve async query-string segment domain", K(ret), K(i), K(j));
          }
        }
        for (int64_t s = 0; OB_SUCC(ret) && s < MAX(1L, segment_op_cnt); ++s) {
          ObIDASSearchOp *segment_op = nullptr;
          if (OB_FAIL(generate_group_field_op(
                  i, j, tokens,
                  boost_datum->get_double() * field_boosts.at(j) * token_boosts_.at(i),
                  default_operator_datum->get_int(), group_cnt, type_datum->get_int(),
                  lead_cost, search_ctx, segment_op))) {
            LOG_WARN("failed to generate query-string group field subtree",
                     K(ret), K(i), K(j), K(s));
          } else if (OB_ISNULL(segment_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null query-string group field subtree", K(ret), K(i), K(j), K(s));
          } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
            LOG_WARN("failed to append query-string segment subtree", K(ret), K(i), K(j), K(s));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!is_async_index || parent_owns_async_segment_binding) {
          if (OB_FAIL(field_ops.push_back(segment_ops.at(0)))) {
            LOG_WARN("failed to append direct query-string field", K(ret), K(i), K(j));
          }
        } else {
          ObIDASSearchOp *merge_op = nullptr;
          if (OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
                  segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, merge_op))) {
            LOG_WARN("failed to create per-index query-string segment merge",
                     K(ret), K(i), K(j), K(index_table_id));
          } else if (OB_FAIL(field_ops.push_back(merge_op))) {
            LOG_WARN("failed to append query-string segment merge", K(ret), K(i), K(j));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (1 == field_ops.count()) {
        if (OB_FAIL(group_ops.push_back(field_ops.at(0)))) {
          LOG_WARN("failed to append the only field op to group ops", K(ret));
        }
      } else if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_datum->get_int()) {
        ObDASDisjunctionOp *disj_op = nullptr;
        ObDASDisjunctionOpParam disj_op_param(field_ops, 1, true, lead_cost);
        disj_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
          LOG_WARN("failed to create disjunction op", KR(ret));
        } else if (OB_FAIL(group_ops.push_back(disj_op))) {
          LOG_WARN("failed to append disjunction op to group ops", K(ret));
        }
      } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_datum->get_int()) {
        if (ctdef->is_top_level_scoring() && 1 == group_cnt) {
          ObDASBMMOp *bmm_op = nullptr;
          if (OB_FAIL(align_cost_array(search_ctx, field_costs_, field_cnt, "field"))) {
            LOG_WARN("failed to align query-string group field BMM costs", KR(ret), K(i));
          } else {
            ObIDASSearchOp *filter_op = pushdown_filter_op_;
            const ObDASSearchCost filter_cost = OB_NOT_NULL(filter_op)
                ? pushdown_filter_cost_ : ObDASSearchCost();
            ObDASBMMBuildOptions bmm_options = has_bmm_build_options_
                ? bmm_build_options_ : ObDASBMMBuildOptions();
            bmm_options.query_optional_ = query_optional_;
            bmm_options.lead_cost_ = lead_cost;
            bmm_options.filter_cost_ = has_bmm_build_options_ ? pushdown_filter_cost_ : filter_cost;
            ObDASBMMOpParam bmm_op_param(
                field_ops, filter_op, allocator_, bmm_options, &field_costs_);
            bmm_op_param.set_is_scoring(ctdef->is_scoring());
            if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
              LOG_WARN("failed to create bmm op", KR(ret));
            } else if (OB_FAIL(group_ops.push_back(bmm_op))) {
              LOG_WARN("failed to append bmm op to group ops", K(ret));
            }
          }
        } else {
          ObDASDisjunctionOp *disj_op = nullptr;
          ObDASDisjunctionOpParam disj_op_param(field_ops, 1, false, lead_cost);
          disj_op_param.set_is_scoring(ctdef->is_scoring());
          if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
            LOG_WARN("failed to create disjunction op", KR(ret));
          } else if (OB_FAIL(group_ops.push_back(disj_op))) {
            LOG_WARN("failed to append disjunction op to group ops", K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match fields type", K(ret), K(type_datum->get_int()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (1 == group_ops.count()) {
      op = group_ops.at(0);
    } else if ((MATCH_OPERATOR_AND == default_operator_datum->get_int()
        || min_should_match >= group_cnt) && !has_pushdown_filter) {
      ObDASConjunctionOp *conj_op = nullptr;
      ObDASConjunctionOpParam conj_op_param(group_ops);
      conj_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(conj_op_param, conj_op))) {
        LOG_WARN("failed to create conjunction op", KR(ret));
      } else {
        op = conj_op;
      }
    } else if (!ctdef->is_top_level_scoring()) {
      ObDASDisjunctionOp *disj_op = nullptr;
      ObDASDisjunctionOpParam disj_op_param(group_ops, min_should_match, false, lead_cost);
      disj_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
        LOG_WARN("failed to create disjunction op", KR(ret));
      } else {
        op = disj_op;
      }
    } else if (min_should_match > 1) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_op_param(group_ops, min_should_match, allocator_);
      bmw_op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
        LOG_WARN("failed to create bmw op", KR(ret));
      } else {
        op = bmw_op;
      }
    } else {
      ObDASBMMOp *bmm_op = nullptr;
      if (OB_FAIL(align_cost_array(search_ctx, group_costs_, group_cnt, "group"))) {
        LOG_WARN("failed to align query-string group BMM costs", KR(ret));
      } else {
        ObIDASSearchOp *filter_op = pushdown_filter_op_;
        const ObDASSearchCost filter_cost = OB_NOT_NULL(filter_op)
            ? pushdown_filter_cost_ : ObDASSearchCost();
        ObDASBMMBuildOptions bmm_options = has_bmm_build_options_
            ? bmm_build_options_ : ObDASBMMBuildOptions();
        bmm_options.query_optional_ = query_optional_;
        bmm_options.lead_cost_ = lead_cost;
        bmm_options.filter_cost_ = has_bmm_build_options_ ? pushdown_filter_cost_ : filter_cost;
        ObDASBMMOpParam bmm_op_param(
            group_ops, filter_op, allocator_, bmm_options, &group_costs_);
        bmm_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
          LOG_WARN("failed to create bmm op", KR(ret));
        } else {
          op = bmm_op;
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
