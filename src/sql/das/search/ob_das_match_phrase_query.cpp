/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "plugin/legacy/sys/ob_plugin_helper.h" 
#include "ob_das_match_phrase_query.h"
#include "ob_das_match_phrase_op.h"
#include "ob_das_async_segment_merge_op.h"
#include "ob_das_dummy_op.h"
#include "ob_das_token_op.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_fts_cost_utils.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "share/ob_fts_index_builder_util.h"
#include "storage/fts/ob_fts_tokenizer.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASMatchPhraseCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    slop_,
                    ir_ctdef_idx_);

OB_SERIALIZE_MEMBER((ObDASMatchPhraseRtDef, ObIDASSearchRtDef));

void ObDASMatchPhraseRtDef::reset_cached_runtime_state()
{
  query_tokens_.reuse();
  token_ids_.reuse();
  token_offsets_.reuse();
  has_duplicate_tokens_ = false;
  query_tokens_prepared_ = false;
}

int ObDASMatchPhraseRtDef::copy_cached_runtime_state_from(const ObIDASSearchRtDef &other)
{
  int ret = OB_SUCCESS;
  reset_cached_runtime_state();
  if (OB_UNLIKELY(DAS_OP_MATCH_PHRASE_QUERY != other.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search rtdef type for match phrase runtime state copy", KR(ret), K(other.op_type_));
  } else {
    const ObDASMatchPhraseRtDef &src = static_cast<const ObDASMatchPhraseRtDef &>(other);
    if (OB_FAIL(ObIDASSearchRtDef::copy_cached_runtime_state_from(other))) {
      LOG_WARN("failed to copy base search runtime state", KR(ret));
    } else if (OB_FAIL(query_tokens_.reserve(src.query_tokens_.count()))) {
      LOG_WARN("failed to reserve copied match phrase query tokens", KR(ret), K(src.query_tokens_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.query_tokens_.count(); ++i) {
      ObString copied_token;
      if (OB_FAIL(ob_write_string(allocator_, src.query_tokens_.at(i), copied_token))) {
        LOG_WARN("failed to deep copy match phrase query token", KR(ret), K(i));
      } else if (OB_FAIL(query_tokens_.push_back(copied_token))) {
        LOG_WARN("failed to append copied match phrase query token", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(token_ids_.assign(src.token_ids_))) {
      LOG_WARN("failed to copy match phrase token ids", KR(ret), K(src.token_ids_.count()));
    } else if (OB_SUCC(ret) && OB_FAIL(token_offsets_.assign(src.token_offsets_))) {
      LOG_WARN("failed to copy match phrase token offsets", KR(ret), K(src.token_offsets_.count()));
    } else if (OB_SUCC(ret)) {
      has_duplicate_tokens_ = src.has_duplicate_tokens_;
      query_tokens_prepared_ = src.query_tokens_prepared_;
    }
  }
  if (OB_FAIL(ret)) {
    reset_cached_runtime_state();
    reset_cost();
  }
  return ret;
}

int ObDASMatchPhraseRtDef::prepare_query_tokens()
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseCtDef *ctdef = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_);
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  if (query_tokens_prepared_) {
    // reuse tokens prepared while computing async match phrase cost
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
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "match phrase query on logical table is");
    LOG_WARN("match phrase query on logical table is not supported", KR(ret));
  } else {
    ObDatum *query_text_datum = nullptr;
    reset_cached_runtime_state();
    if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
      LOG_WARN("failed to eval query text", KR(ret));
    } else if (OB_ISNULL(query_text_datum) || OB_UNLIKELY(query_text_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", KR(ret), KPC(query_text_datum));
    } else if (OB_FAIL(ObFTSTokenizer::tokenize(
        allocator_, query_text_datum->get_string(),
        inv_idx_scan_ctdef->table_param_.get_parser_name(),
        inv_idx_scan_ctdef->table_param_.get_parser_property(),
        ctdef->query_text_->obj_meta_, query_tokens_, token_ids_, token_offsets_,
        has_duplicate_tokens_))) {
      LOG_WARN("failed to tokenize query text", KR(ret));
    } else {
      query_tokens_prepared_ = true;
    }
    if (OB_FAIL(ret)) {
      reset_cached_runtime_state();
    }
  }
  return ret;
}

int ObDASMatchPhraseRtDef::compute_async_phrase_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseCtDef *ctdef = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_);
  ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef();
  ObSEArray<int64_t, 16> token_doc_cnts;
  int64_t raw_cost = 0;
  if (OB_ISNULL(ctdef) || OB_ISNULL(ctdef->get_ir_ctdef()) || OB_ISNULL(ir_rtdef)
      || !search_ctx.get_row_count().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid match phrase cost context", KR(ret), KP(ctdef),
        KP(ir_rtdef), K(search_ctx.get_row_count()));
  } else if (OB_FAIL(token_doc_cnts.reserve(query_tokens_.count()))) {
    LOG_WARN("failed to reserve token document counts", KR(ret), K(query_tokens_.count()));
  } else if (OB_FAIL(ObDASFtsCostUtils::estimate_async_token_doc_cnts(
      search_ctx, *ctdef->get_ir_ctdef(), *ir_rtdef, query_tokens_, token_doc_cnts))) {
    LOG_WARN("failed to estimate async token document counts", KR(ret), K_(query_tokens));
  } else if (OB_FAIL(ObDASFtsCostUtils::aggregate_and_cost(token_doc_cnts, raw_cost))) {
    LOG_WARN("failed to aggregate match phrase cost", KR(ret), K(token_doc_cnts));
  } else {
    cost = ObDASSearchCost(std::min(raw_cost, search_ctx.get_row_count().cost()));
  }
  return ret;
}

int ObDASMatchPhraseRtDef::compute_cost(
    ObDASSearchCtx &search_ctx,
    ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  cost.reset();
  const ObDASMatchPhraseCtDef *ctdef = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_);
  if (OB_ISNULL(ctdef) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match phrase definition", KR(ret), KP(ctdef));
  } else if (!ctdef->get_ir_ctdef()->is_async_index_) {
    // Non-async match phrase keeps an invalid cost and falls back to the table row count.
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare match phrase query tokens for cost estimation", KR(ret));
  } else if (query_tokens_.empty()) {
    cost = ObDASSearchCost(0);
  } else if (OB_FAIL(compute_async_phrase_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute async match phrase cost", KR(ret));
  }
  return ret;
}

int ObDASMatchPhraseRtDef::generate_single_segment_op(
    const ObDASMatchPhraseCtDef &ctdef,
    ObDASMatchPhraseOpParam &phrase_param,
    const bool use_single_token,
    const double token_boost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASIRScanCtDef *ir_ctdef = ctdef.get_ir_ctdef();
  ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef();
  op = nullptr;
  if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)
      || OB_UNLIKELY(token_boost <= 0.0
                     || (use_single_token && phrase_param.query_tokens_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid match phrase segment argument", K(ret), KP(ir_ctdef), KP(ir_rtdef),
             K(token_boost), K(use_single_token), K(phrase_param.query_tokens_.count()));
  } else if (use_single_token) {
    storage::ObBlockMaxScoreIterParam *block_max_param =
        ir_ctdef->is_async_index_ ? nullptr : &block_max_param_;
    if (OB_FAIL(ObDASSearchUtils::create_token_search_op(
            *ir_ctdef, *ir_rtdef, block_max_param,
            phrase_param.query_tokens_.at(0), token_boost,
            ctdef.is_scoring(), search_ctx, op))) {
      LOG_WARN("failed to create match phrase single token op", K(ret));
    }
  } else {
    if (ir_ctdef->is_async_index_) {
      phrase_param.index_table_id_ = search_ctx.get_async_index_table_id();
      phrase_param.group_slot_ = search_ctx.get_async_group_slot();
      phrase_param.direct_bind_ = search_ctx.get_async_direct_bind();
    }
    ObDASMatchPhraseOp *match_phrase_op = nullptr;
    if (OB_FAIL(search_ctx.create_op(phrase_param, match_phrase_op))) {
      LOG_WARN("failed to create match phrase op", K(ret));
    } else {
      op = match_phrase_op;
    }
  }
  return ret;
}

int ObDASMatchPhraseRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseCtDef *ctdef =
      static_cast<const ObDASMatchPhraseCtDef *>(ctdef_);
  ObDASMatchPhraseOpParam param;
  ObDatum *boost_datum = nullptr;
  ObDatum *slop_datum = nullptr;
  bool is_async_index = false;
  bool use_single_token = false;
  UNUSED(lead_cost);
  op = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())
      || OB_ISNULL(get_ir_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctdef), KP_(eval_ctx),
             "ir_ctdef", OB_ISNULL(ctdef) ? nullptr : ctdef->get_ir_ctdef(),
             "ir_rtdef", OB_ISNULL(ctdef) ? nullptr : get_ir_rtdef());
  } else if (OB_FAIL(prepare_query_tokens())) {
    LOG_WARN("failed to prepare match phrase query tokens", KR(ret));
  } else if (OB_FAIL(param.query_tokens_.assign(query_tokens_))) {
    LOG_WARN("failed to copy match phrase query tokens", KR(ret));
  } else if (OB_FAIL(param.token_ids_.assign(token_ids_))) {
    LOG_WARN("failed to copy match phrase token ids", KR(ret));
  } else if (param.query_tokens_.empty()) {
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    dummy_op_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", K(ret));
    } else {
      op = dummy_op;
    }
  } else if (OB_NOT_NULL(ctdef->get_ir_ctdef())
             && !ctdef->get_ir_ctdef()->is_async_index_
             && (OB_ISNULL(ctdef->get_ir_ctdef()->inv_scan_doc_length_col_)
                 || OB_ISNULL(ctdef->get_ir_ctdef()->inv_scan_pos_list_col_))) {
    ret = OB_ERR_UNEXPECTED;
    // tolerate null inv_scan_token_cnt_col_ for compatibility with pre-4.6.0 plan caches
    LOG_WARN("missing required column in sync fulltext index", K(ret), K(ctdef->get_ir_ctdef()));
  } else if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
    LOG_WARN("failed to eval boost", K(ret));
  } else if (OB_FAIL(ctdef->slop_->eval(*eval_ctx_, slop_datum))) {
    LOG_WARN("failed to eval slop", K(ret));
  } else if (OB_UNLIKELY(boost_datum->is_null() || slop_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(boost_datum), KPC(slop_datum));
  } else {
    is_async_index = ctdef->get_ir_ctdef()->is_async_index_;
    if (is_async_index || block_max_param_initialized_) {
      // Async full-text does not currently generate block-max scans.
    } else if (OB_FAIL(block_max_param_.init(*ctdef->get_ir_ctdef(), allocator_))) {
      LOG_WARN("failed to init block max param", KR(ret));
    } else {
      block_max_param_initialized_ = true;
    }
  }

  if (OB_SUCC(ret) && !param.query_tokens_.empty()) {
    use_single_token = 1 == param.query_tokens_.count() && !has_duplicate_tokens_;
    param.boost_ = boost_datum->get_double();
    param.slop_ = slop_datum->get_int();
    param.set_is_scoring(ctdef->is_scoring());
    param.ir_ctdef_ = ctdef->get_ir_ctdef();
    param.ir_rtdef_ = get_ir_rtdef();
    param.block_max_param_ = is_async_index ? nullptr : &block_max_param_;
    // CG generates rich-format expressions for both async and synchronous multi-token phrases.
    param.use_rich_format_ = true;
    if (OB_UNLIKELY(param.slop_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected negative slop", K(ret), K_(param.slop));
    } else if (OB_UNLIKELY(param.boost_ <= 0.0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-positive boost", K(ret), K_(param.boost));
    } else if (!use_single_token && OB_FAIL(ObPhraseMatchCounter::create(
            allocator_, param.token_ids_, token_offsets_,
            slop_datum->get_int(), has_duplicate_tokens_, param.counter_))) {
      LOG_WARN("failed to create phrase match counter", K(ret));
    }
  }

  if (OB_FAIL(ret) || param.query_tokens_.empty()) {
  } else if (!is_async_index) {
    if (OB_FAIL(generate_single_segment_op(
            *ctdef, param, use_single_token, param.boost_, search_ctx, op))) {
      LOG_WARN("failed to generate synchronous match phrase op", K(ret));
    }
  } else {
    ObSEArray<ObIDASSearchOp *, 4> segment_ops;
    ObDASAsyncBindingGuard binding_guard(search_ctx);
    const uint64_t claimed_index_table_id = search_ctx.get_async_index_table_id();
    uint64_t index_table_id = common::OB_INVALID_ID;
    int64_t max_segment_cnt = 0;
    if (OB_FAIL(ObDASSearchUtils::resolve_async_segment_domain(
            *ctdef->get_ir_ctdef(), search_ctx, index_table_id, max_segment_cnt))) {
      LOG_WARN("failed to resolve async match phrase segment domain", K(ret));
    } else if (common::OB_INVALID_ID != claimed_index_table_id) {
      if (OB_UNLIKELY(claimed_index_table_id != index_table_id)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async match phrase crosses claimed index domain", K(ret),
                 K(claimed_index_table_id), K(index_table_id));
      } else if (OB_FAIL(generate_single_segment_op(
                     *ctdef, param, use_single_token, param.boost_, search_ctx, op))) {
        LOG_WARN("failed to generate bound async match phrase op", K(ret), K(index_table_id));
      }
    } else {
      binding_guard.set(index_table_id, search_ctx.get_async_group_slot(),
                        search_ctx.get_async_group_slot() >= 0
                        && 1 == max_segment_cnt);
      for (int64_t s = 0; OB_SUCC(ret) && s < max_segment_cnt; ++s) {
        ObIDASSearchOp *segment_op = nullptr;
        if (OB_FAIL(generate_single_segment_op(
                *ctdef, param, use_single_token, param.boost_, search_ctx, segment_op))) {
          LOG_WARN("failed to generate async match phrase segment op", K(ret), K(s));
        } else if (OB_ISNULL(segment_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null async match phrase segment op", K(ret), K(s));
        } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
          LOG_WARN("failed to append async match phrase segment op", K(ret), K(s));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
              segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, op))) {
        LOG_WARN("failed to create async match phrase segment merge", K(ret),
                 K(index_table_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
    op = nullptr;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
