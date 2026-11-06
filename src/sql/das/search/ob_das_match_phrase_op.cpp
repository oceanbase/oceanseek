/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "plugin/legacy/sys/ob_plugin_helper.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "ob_das_match_phrase_op.h"
#include "ob_das_token_op.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_fts_pos_list_codec.h"

namespace oceanbase
{
namespace sql
{

ObDASMatchPhraseOp::ObDASMatchPhraseOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    allocator_(common::ObMemAttr(MTL_ID(), "DASMatchPhrase")),
    abs_pos_lists_(),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    token_ids_(),
    token_helpers_(),
    token_iters_(),
    async_helpers_(),
    estimator_(),
    async_estimator_(),
    use_async_index_(false),
    block_max_inited_(false),
    total_token_weight_(-1.0),
    avg_doc_token_cnt_(0.0),
    decoder_(nullptr),
    counter_(nullptr),
    slop_(0),
    boost_(0.0),
    use_rich_format_(false),
    lead_idx_(0),
    curr_id_(),
    dynamic_pruning_enabled_(false),
    is_inited_(false)
{}

int ObDASMatchPhraseOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseOpParam &param = static_cast<const ObDASMatchPhraseOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    counter_ = param.counter_;
    slop_ = param.slop_;
    boost_ = param.boost_;
    use_rich_format_ = param.use_rich_format_;
    use_async_index_ = ir_ctdef_->is_async_index_;
    token_ids_.set_allocator(&allocator_);
    token_helpers_.set_allocator(&allocator_);
    token_iters_.set_allocator(&allocator_);
    async_helpers_.set_allocator(&allocator_);
    if (OB_FAIL(init_abs_pos_lists(param.query_tokens_.count()))) {
      LOG_WARN("failed to init abs pos lists", K(ret));
    } else if (OB_ISNULL(decoder_ = OB_NEWx(ObFTSPositionListStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create pos list store", K(ret));
    } else if (OB_FAIL(token_ids_.assign(param.token_ids_))) {
      LOG_WARN("failed to assign token id array", K(ret));
    } else if (use_async_index_) {
      if (OB_FAIL(async_helpers_.init(param.query_tokens_.count()))) {
        LOG_WARN("failed to init async helper array", K(ret));
      } else if (OB_FAIL(async_helpers_.prepare_allocate(param.query_tokens_.count()))) {
        LOG_WARN("failed to prepare allocate async helper array", K(ret));
      } else {
        ObDASAsyncTokenHelperParam helper_param;
        helper_param.ir_ctdef_ = ir_ctdef_;
        helper_param.ir_rtdef_ = ir_rtdef_;
        helper_param.index_table_id_ = param.index_table_id_;
        helper_param.group_slot_ = param.group_slot_;
        helper_param.direct_bind_ = param.direct_bind_;
        // Per-token dim weight only; phrase-level boost_ is applied in evaluate().
        helper_param.token_boost_ = 1.0;
        for (int64_t i = 0; OB_SUCC(ret) && i < param.query_tokens_.count(); ++i) {
          helper_param.query_token_ = param.query_tokens_.at(i);
          if (OB_ISNULL(async_helpers_[i] = OB_NEWx(ObDASAsyncTokenHelper, &allocator_, search_ctx_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to create async token helper", K(ret));
          } else if (OB_FAIL(async_helpers_[i]->init(helper_param))) {
            LOG_WARN("failed to init async token helper", K(ret), K(i));
          }
        }
      }
    } else if (OB_FAIL(token_helpers_.init(param.query_tokens_.count()))) {
      LOG_WARN("failed to init token helper array", K(ret));
    } else if (OB_FAIL(token_helpers_.prepare_allocate(param.query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate token helper array", K(ret));
    } else if (OB_FAIL(token_iters_.init(param.query_tokens_.count()))) {
      LOG_WARN("failed to init token iter array", K(ret));
    } else if (OB_FAIL(token_iters_.prepare_allocate(param.query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate token iter array", K(ret));
    } else {
      ObDASTokenOpParam token_param;
      token_param.ir_ctdef_ = ir_ctdef_;
      token_param.ir_rtdef_ = ir_rtdef_;
      token_param.block_max_param_ = param.block_max_param_;
      token_param.token_boost_ = 1.0;
      token_param.use_rich_format_ = use_rich_format_;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.query_tokens_.count(); ++i) {
        token_param.query_token_ = param.query_tokens_.at(i);
        if (OB_ISNULL(token_helpers_[i] = OB_NEWx(ObDASTokenOpHelper, &allocator_, search_ctx_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create token helper", K(ret));
        } else if (OB_FAIL(token_helpers_[i]->init(token_param))) {
          LOG_WARN("failed to init token helper", K(ret));
        }
      }
      if (OB_SUCC(ret) && !token_helpers_.empty()) {
        if (OB_FAIL(token_helpers_[0]->init_bm25_param_estimator(estimator_))) {
          LOG_WARN("failed to init bm25 param estimator", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    int64_t dynamic_pruning_level = HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_BASIC;
    if (tenant_config.is_valid()) {
      dynamic_pruning_level = tenant_config->_hybrid_search_topk_dynamic_pruning_level;
    }
    dynamic_pruning_enabled_ = use_async_index_
        ? dynamic_pruning_level >= HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_BASIC
        : dynamic_pruning_level >= HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_WITH_MATCH_PHRASE;
    is_inited_ = true;
  }
  return ret;
}

int ObDASMatchPhraseOp::init_abs_pos_lists(const int64_t token_cnt)
{
  int ret = OB_SUCCESS;
  abs_pos_lists_.set_allocator(&allocator_);
  if (OB_FAIL(abs_pos_lists_.init(token_cnt))) {
    LOG_WARN("failed to init abs pos lists", K(ret), K(token_cnt));
  } else if (OB_FAIL(abs_pos_lists_.prepare_allocate(token_cnt))) {
    LOG_WARN("failed to prepare allocate abs pos lists", K(ret), K(token_cnt));
  }
  if (OB_SUCCESS != ret) {
    abs_pos_lists_.reset();
  }
  return ret;
}

int ObDASMatchPhraseOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (use_async_index_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < async_helpers_.count(); ++i) {
      if (OB_FAIL(async_helpers_[i]->open())) {
        LOG_WARN("failed to open async token helper", K(ret), K(i));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
      if (OB_ISNULL(token_iters_[i] = OB_NEWx(ObTextRetrievalBlockMaxIter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create token iter", K(ret));
      } else if (OB_FAIL(token_helpers_[i]->init_text_retrieval_iter(*token_iters_[i]))) {
        LOG_WARN("failed to open token iter", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(prepare_token_stats())) {
    LOG_WARN("failed to prepare phrase token stats", K(ret));
  }
  return ret;
}

int ObDASMatchPhraseOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (use_async_index_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < async_helpers_.count(); ++i) {
      if (OB_FAIL(async_helpers_[i]->rescan())) {
        LOG_WARN("failed to rescan async token helper", K(ret), K(i));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
      if (OB_FAIL(token_helpers_[i]->reuse())) {
        LOG_WARN("failed to reuse token iter", K(ret));
      } else if (FALSE_IT(token_iters_[i]->reuse())) {
      } else if (OB_FAIL(token_helpers_[i]->rescan())) {
        LOG_WARN("failed to rescan token iter", K(ret));
      }
    }
    if (OB_SUCC(ret) && !token_helpers_.empty()) {
      estimator_.switch_tablet_id(token_helpers_[0]->get_inv_idx_tablet_id());
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(prepare_token_stats())) {
    LOG_WARN("failed to prepare phrase token stats on rescan", K(ret));
  }
  return ret;
}

int ObDASMatchPhraseOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (!use_async_index_) {
    ret = OB_SUCCESS;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < async_helpers_.count(); ++i) {
      if (OB_FAIL(async_helpers_[i]->rescan_segment(range))) {
        LOG_WARN("failed to rescan_segment async token helper", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(prepare_token_stats())) {
      LOG_WARN("failed to prepare phrase token stats on segment switch", K(ret));
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::prepare_token_stats()
{
  int ret = OB_SUCCESS;
  bool is_empty_segment = false;
  block_max_inited_ = false;
  total_token_weight_ = -1.0;
  avg_doc_token_cnt_ = 0.0;
  lead_idx_ = 0;
  if (use_async_index_) {
    async_estimator_.reset();
    if (OB_UNLIKELY(async_helpers_.empty()) || OB_ISNULL(async_helpers_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing async phrase token helper", K(ret));
    } else if ((is_empty_segment = async_helpers_[0]->is_empty_segment())) {
      // All phrase tokens share this Segment; empty inputs need no statistics.
      total_token_weight_ = 0.0;
    } else if (OB_FAIL(async_helpers_[0]->init_bm25_param_estimator(async_estimator_))) {
      LOG_WARN("failed to init async bm25 param estimator", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_empty_segment) {
    if (OB_FAIL(estimate_bm25_param())) {
      LOG_WARN("failed to estimate bm25 param", K(ret));
    } else if (OB_FAIL(select_lead_idx())) {
      LOG_WARN("failed to select lead idx", K(ret));
    }
  }
  return ret;
}

void ObDASMatchPhraseOp::close_posting_iters()
{
  if (use_async_index_) {
    for (int64_t i = 0; i < async_helpers_.count(); ++i) {
      if (OB_NOT_NULL(async_helpers_[i])) {
        async_helpers_[i]->reuse();
      }
    }
  }
}

int ObDASMatchPhraseOp::do_close()
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < async_helpers_.count(); ++i) {
      if (OB_NOT_NULL(async_helpers_[i]) && OB_FAIL(async_helpers_[i]->close())) {
        LOG_WARN("failed to close async token helper", K(ret), K(i));
      }
      async_helpers_[i] = nullptr;
    }
    async_helpers_.reset();
    async_estimator_.reset();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
      if (OB_NOT_NULL(token_helpers_[i]) && OB_FAIL(token_helpers_[i]->close())) {
        LOG_WARN("failed to close token iter", K(ret), K(i));
      }
      token_helpers_[i] = nullptr;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
      if (OB_NOT_NULL(token_iters_[i])) {
        token_iters_[i]->reset();
      }
      token_iters_[i] = nullptr;
    }
    token_helpers_.reset();
    token_iters_.reset();
    estimator_.reset();
  }
  token_ids_.reset();
  if (OB_NOT_NULL(counter_)) {
    counter_->reset();
    counter_ = nullptr;
  }
  abs_pos_lists_.reset();
  allocator_.reset();
  is_inited_ = false;
  return ret;
}

// token_* adapters below: the only place that branches on use_async_index_.
// find_intersection / pre_evaluate / evaluate call token_* only; they do not distinguish async vs sync themselves.

int ObDASMatchPhraseOp::token_get_curr_id(const int64_t idx, const ObDatum *&curr_id) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_curr_id(curr_id))) {
      LOG_WARN("failed to get curr id from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_id(curr_id))) {
    LOG_WARN("failed to get curr id from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_advance_to(const int64_t idx, const ObDatum &target_id)
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->advance_to(target_id))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->advance_to(target_id))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_next_row(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_next_row())) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_next_row())) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_curr_pos_list(const int64_t idx, ObString &pos_list) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_curr_pos_list(pos_list))) {
      LOG_WARN("failed to get curr pos list from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_pos_list(pos_list))) {
    LOG_WARN("failed to get curr pos list from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_curr_doc_length(const int64_t idx, int64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    int64_t token_freq = 0;
    if (OB_FAIL(async_helpers_.at(idx)->get_curr_bm25_param(token_freq, doc_length))) {
      LOG_WARN("failed to get curr bm25 param from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_doc_length(doc_length))) {
    LOG_WARN("failed to get curr doc length from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_curr_bm25_param(
    const int64_t idx,
    int64_t &token_freq,
    int64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_curr_bm25_param(token_freq, doc_length))) {
      LOG_WARN("failed to get curr bm25 param from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_doc_length(doc_length))) {
    LOG_WARN("failed to get curr doc length from sync iter", K(ret), K(idx));
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_token_freq(token_freq))) {
    LOG_WARN("failed to get curr token freq from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_max_token_relevance(const int64_t idx, double &weight) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_max_token_relevance(weight))) {
      LOG_WARN("failed to get max token relevance from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_max_token_relevance(weight))) {
    LOG_WARN("failed to get max token relevance from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_advance_shallow(const int64_t idx, const ObDatum &id, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->advance_shallow(id, inclusive))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->advance_shallow(id, inclusive))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_curr_block_max_info(
    const int64_t idx, const storage::ObMaxScoreTuple *&tuple) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    if (OB_FAIL(async_helpers_.at(idx)->get_curr_phrase_block_max_info(tuple))) {
      LOG_WARN("failed to get phrase block max info from async helper", K(ret), K(idx));
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_curr_block_max_info(tuple))) {
    LOG_WARN("failed to get curr block max info from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::token_get_token_doc_cnt(const int64_t idx, int64_t &token_doc_cnt) const
{
  int ret = OB_SUCCESS;
  if (use_async_index_) {
    uint64_t tc = 0;
    if (OB_FAIL(async_helpers_.at(idx)->get_param_estimator().get_token_doc_cnt(tc))) {
      LOG_WARN("failed to get async token doc cnt", K(ret), K(idx));
    } else {
      token_doc_cnt = static_cast<int64_t>(tc);
    }
  } else if (OB_FAIL(token_iters_.at(idx)->get_token_doc_cnt(token_doc_cnt))) {
    LOG_WARN("failed to get token doc cnt from sync iter", K(ret), K(idx));
  }
  return ret;
}

int ObDASMatchPhraseOp::find_intersection(ObDASRowID &rowid, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *curr_id = nullptr;
  bool got_result = false;
  const int64_t token_cnt = use_async_index_ ? async_helpers_.count() : token_iters_.count();
  const int64_t lead = lead_idx_;
  const bool use_dynamic_pruning = dynamic_pruning_enabled_ && 0.0 < min_competitive_score_;
  if (use_dynamic_pruning && !use_async_index_) {
    if (OB_FAIL(init_block_max_iters_on_demand())) {
      LOG_WARN("failed to init block max iters on demand", K(ret));
    }
  }
  while (OB_SUCC(ret) && !got_result) {
    // Phrase intersection loop (sync/async share the same control flow).
    // Async vs sync dispatch lives inside token_* / pre_evaluate / evaluate, not here.
    if (OB_FAIL(token_get_curr_id(lead, curr_id))) {
      LOG_WARN("failed to get lead curr id", K(ret));
    }
    if (!use_dynamic_pruning) {
      got_result = true;
    } else {
      // Shallow non-lead tokens to the lead doc block before calculating the phrase score upper bound.
      for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
        if (i == lead) {
          // skip
        } else if (OB_FAIL(token_advance_shallow(i, *curr_id, true))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow token iter", K(ret), KPC(curr_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pre_evaluate(got_result))) {
        LOG_WARN("failed to pre evaluate", K(ret));
      } else if (!got_result && OB_FAIL(token_get_next_row(lead))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row after pre-evaluation", K(ret));
      }
    }
    // Doc-id intersection: advance_to all tokens to the same doc (token_advance_to).
    for (int64_t i = 0; OB_SUCC(ret) && got_result && i < token_cnt; ++i) {
      if (i == lead) {
        // skip
      } else {
        const ObDatum *temp_id = nullptr;
        if (OB_FAIL(token_advance_to(i, *curr_id))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance token iter", K(ret));
        } else if (OB_FAIL(token_get_curr_id(i, temp_id))) {
          LOG_WARN("failed to get curr id", K(ret));
        } else if (OB_UNLIKELY(!ObDatum::binary_equal(*curr_id, *temp_id))) {
          got_result = false;
          if (OB_FAIL(token_advance_to(lead, *temp_id))) {
            LOG_WARN_IGNORE_ITER_END(ret, "failed to advance lead token iter", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret) || !got_result) {
    } else if (OB_FAIL(evaluate(score))) {
      LOG_WARN("failed to evaluate", K(ret));
    } else if (0 == score) {
      got_result = false;
      if (OB_FAIL(token_get_next_row(lead))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row after evaluation", K(ret));
      }
    }
  }
  // Position-list materialization may invalidate the previous datum buffer.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(token_get_curr_id(lead, curr_id))) {
    LOG_WARN("failed to re-get curr id after evaluate", K(ret));
  } else if (OB_FAIL(write_datum_to_rowid(*curr_id, curr_id_, allocator_))) {
    LOG_WARN("failed to write curr id to rowid", K(ret));
  } else {
    rowid = curr_id_;
  }
  return ret;
}

int ObDASMatchPhraseOp::pre_evaluate(bool &is_candidate)
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  int64_t doc_length = 0;
  int64_t lead_tf = 0;
  const int64_t token_cnt = use_async_index_ ? async_helpers_.count() : token_iters_.count();
  if (OB_UNLIKELY(!dynamic_pruning_enabled_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pre-evaluation when dynamic pruning disabled", K(ret));
  } else if (use_async_index_) {
    avg_doc_token_cnt = avg_doc_token_cnt_;
  } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(token_get_curr_bm25_param(lead_idx_, lead_tf, doc_length))) {
    LOG_WARN("failed to get curr bm25 param", K(ret));
  } else if (OB_FAIL(calculate_total_token_weight_on_demand())) {
    LOG_WARN("failed to calculate total token weight on demand", K(ret));
  } else {
    const double length_normalizer = ObExprBM25::length_normalizer(doc_length, avg_doc_token_cnt);
    int64_t phrase_freq_sum = 1 - token_cnt;
    int64_t phrase_freq_min = lead_tf;
    bool has_all_block_max = true;
    const storage::ObMaxScoreTuple *max_score_tuple = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && has_all_block_max && i < token_ids_.count(); ++i) {
      if (lead_idx_ == token_ids_[i]) {
        phrase_freq_sum += lead_tf;
        phrase_freq_min = MIN(phrase_freq_min, lead_tf);
      } else if (OB_FAIL(token_get_curr_block_max_info(token_ids_[i], max_score_tuple))) {
        if (OB_NOT_SUPPORTED == ret) {
          ret = OB_SUCCESS;
          has_all_block_max = false;
        } else {
          LOG_WARN("failed to get curr block max info", K(ret));
        }
      } else if (OB_ISNULL(max_score_tuple)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null block max info", K(ret), K(i), K(token_ids_[i]));
      } else {
        const int64_t tf_max = static_cast<int64_t>(
            max_score_tuple->max_score_ * length_normalizer + 1E-5);
        phrase_freq_sum += tf_max;
        phrase_freq_min = MIN(phrase_freq_min, tf_max);
      }
    }
    if (OB_SUCC(ret)) {
      if (!has_all_block_max) {
        is_candidate = true;
      } else {
        const double phrase_freq_ub = (0 == slop_)
            ? phrase_freq_min
            : (phrase_freq_min + phrase_freq_sum) / 2.0;
        const double norm_len = doc_length / avg_doc_token_cnt;
        const double doc_weight_ub = ObExprBM25::doc_phrase_weight(phrase_freq_ub, norm_len);
        const double score_ub = total_token_weight_ * doc_weight_ub * boost_;
        is_candidate = score_ub > min_competitive_score_;
      }
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::evaluate(double &score)
{
  int ret = OB_SUCCESS;
  double total_match_count = 0.0;
  const int64_t token_cnt = use_async_index_ ? async_helpers_.count() : token_iters_.count();
  for (int64_t i = 0; i < abs_pos_lists_.count(); ++i) {
    abs_pos_lists_.at(i).reuse();
  }
  ObString pos_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
    if (OB_FAIL(token_get_curr_pos_list(i, pos_list))) {
      LOG_WARN("failed to get curr pos list", K(ret), K(i), K(token_cnt));
    } else if (OB_FAIL(decoder_->deserialize_and_decode(
               pos_list.ptr(), pos_list.length(), abs_pos_lists_.at(i)))) {
      LOG_WARN("failed to decode pos list string", K(ret), K(i), K(pos_list));
    }
  }

  if (FAILEDx(counter_->count_matches(abs_pos_lists_, total_match_count))) {
    LOG_WARN("failed to count matches", K(ret));
  } else if (OB_UNLIKELY(total_match_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative total match count", K(ret), K(total_match_count));
  } else if (0 == total_match_count) {
    score = 0.0;
  } else {
    int64_t doc_length = 0;
    uint64_t total_doc_cnt = 0;
    double avg_doc_length = 0.0;
    if (OB_FAIL(calculate_total_token_weight_on_demand())) {
      LOG_WARN("failed to calculate total token weight", K(ret));
    } else if (OB_FAIL(token_get_curr_doc_length(lead_idx_, doc_length))) {
      LOG_WARN("failed to get curr doc length", K(ret));
    } else if (use_async_index_) {
      avg_doc_length = avg_doc_token_cnt_;
    } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_length))) {
      LOG_WARN("failed to get bm25 param", K(ret));
    }
    if (OB_SUCC(ret)) {
      const double norm_len = doc_length / avg_doc_length;
      const double doc_weight = ObExprBM25::doc_phrase_weight(total_match_count, norm_len);
      score = total_token_weight_ * doc_weight * boost_;
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::calculate_total_token_weight_on_demand()
{
  int ret = OB_SUCCESS;
  if (total_token_weight_ < 0.0) {
    total_token_weight_ = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < token_ids_.count(); ++i) {
      double token_weight = 0.0;
      if (OB_FAIL(token_get_max_token_relevance(token_ids_[i], token_weight))) {
        LOG_WARN("failed to get token weight", K(ret));
      } else {
        total_token_weight_ += token_weight;
      }
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(token_get_next_row(lead_idx_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row", K(ret));
  } else if (OB_FAIL(find_intersection(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to intersect", K(ret));
  }
  return ret;
}

int ObDASMatchPhraseOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  ObDatum target_datum;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from target", K(ret));
  } else if (OB_FAIL(token_advance_to(lead_idx_, target_datum))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row", K(ret));
  } else if (OB_FAIL(find_intersection(curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to intersect", K(ret));
  }
  return ret;
}

int ObDASMatchPhraseOp::select_lead_idx()
{
  int ret = OB_SUCCESS;
  int64_t min_token_doc_cnt = INT64_MAX;
  lead_idx_ = 0;
  const int64_t n = use_async_index_ ? async_helpers_.count() : token_iters_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
    int64_t token_doc_cnt = 0;
    if (OB_FAIL(token_get_token_doc_cnt(i, token_doc_cnt))) {
      LOG_WARN("failed to get token doc cnt", K(ret), K(i));
    } else if (token_doc_cnt < min_token_doc_cnt) {
      min_token_doc_cnt = token_doc_cnt;
      lead_idx_ = i;
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  max_score_tuple = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (use_async_index_) {
    if (OB_FAIL(do_advance_shallow_async(target, inclusive, max_score_tuple))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance shallow async", K(ret), K(target), K(inclusive));
    }
  } else {
    // Sync: degrade to advance_to.
    // TODO: fix inclusive min id
    ObDASRowID tmp_id;
    double tmp_score = 0.0;
    int cmp_ret = 0;
    if (OB_FAIL(calculate_total_token_weight_on_demand())) {
      LOG_WARN("failed to calculate total token weight", K(ret));
    } else if (OB_FAIL(do_advance_to(target, tmp_id, tmp_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", K(ret));
    } else if (!inclusive && OB_FAIL(compare_rowid(tmp_id, target, cmp_ret))) {
      LOG_WARN("failed to compare rowids", K(ret));
    } else if (!inclusive && 0 == cmp_ret && OB_FAIL(do_next_rowid(tmp_id, tmp_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObDASRowID max_id;
      max_id.set_max();
      max_score_tuple_.set(tmp_id, max_id, total_token_weight_);
      max_score_tuple = &max_score_tuple_;
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_advance_shallow_async(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  // Intersect per-token block ranges via helper advance_shallow (no materialize).
  // Score upper bound stays total_token_weight_; per-token block_max unused for score.
  int ret = OB_SUCCESS;
  ObDatum target_datum;
  ObDASRowID maximum_min_id;
  ObDASRowID minimal_max_id;
  ObDASRowID candidate_target = target;
  // After empty-intersect retarget, probe must be inclusive or pivot doc is skipped.
  bool cur_inclusive = inclusive;
  const int64_t token_cnt = async_helpers_.count();
  int cmp = 0;
  max_score_tuple = nullptr;
  if (OB_UNLIKELY(token_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty async helpers", K(ret), K(token_cnt));
  } else if (OB_FAIL(calculate_total_token_weight_on_demand())) {
    LOG_WARN("failed to calculate total token weight", K(ret));
  }
  while (OB_SUCC(ret)) {
    maximum_min_id.set_min();
    minimal_max_id.set_max();
    if (OB_FAIL(get_datum_from_rowid(candidate_target, target_datum))) {
      LOG_WARN("failed to get datum from rowid", K(ret), K(candidate_target));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
      ObDASRowID token_min_id;
      ObDASRowID token_max_id;
      uint64_t min_uid = 0;
      uint64_t max_uid = 0;
      if (OB_FAIL(async_helpers_.at(i)->advance_shallow(target_datum, cur_inclusive))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance_shallow async helper",
                                 K(ret), K(i), K(candidate_target));
      } else if (OB_FAIL(async_helpers_.at(i)->get_block_id_range(min_uid, max_uid))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to get block id range", K(ret), K(i));
      } else {
        token_min_id.set_uint64(min_uid);
        token_max_id.set_uint64(max_uid);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(compare_rowid(maximum_min_id, token_min_id, cmp))) {
        LOG_WARN("failed to compare min rowids", K(ret), K(i));
      } else if (cmp < 0) {
        maximum_min_id = token_min_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(compare_rowid(minimal_max_id, token_max_id, cmp))) {
        LOG_WARN("failed to compare max rowids", K(ret), K(i));
      } else if (cmp > 0) {
        minimal_max_id = token_max_id;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(compare_rowid(maximum_min_id, minimal_max_id, cmp))) {
      LOG_WARN("failed to compare intersect bounds", K(ret), K(maximum_min_id), K(minimal_max_id));
    } else if (cmp > 0) {
      int progress_cmp = 0;
      if (OB_FAIL(compare_rowid(candidate_target, maximum_min_id, progress_cmp))) {
        LOG_WARN("failed to check shallow retarget progress", K(ret),
                 K(candidate_target), K(maximum_min_id));
      } else if (progress_cmp > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phrase shallow retarget moved backward", K(ret),
                 K(candidate_target), K(maximum_min_id), K(minimal_max_id));
      } else if (0 == progress_cmp) {
        if (!cur_inclusive) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phrase shallow stuck after exclusive retarget", K(ret),
                   K(candidate_target), K(maximum_min_id), K(minimal_max_id));
        } else {
          cur_inclusive = false;
        }
      } else {
        candidate_target = maximum_min_id;
        cur_inclusive = true;
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    max_score_tuple_.set(maximum_min_id, minimal_max_id, total_token_weight_);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASMatchPhraseOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(calculate_total_token_weight_on_demand())) {
    LOG_WARN("failed to calculate total token weight", K(ret));
  } else {
    threshold = total_token_weight_;
  }
  return ret;
}

int ObDASMatchPhraseOp::estimate_bm25_param()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  ObExpr *total_doc_cnt_expr = nullptr;
  ObExpr *avg_doc_token_cnt_expr = nullptr;
  if (use_async_index_) {
    uint64_t token_doc_cnt = 0;
    if (OB_FAIL(async_estimator_.get_bm25_params(total_doc_cnt, avg_doc_token_cnt, token_doc_cnt))) {
      LOG_WARN("failed to get async bm25 params", K(ret));
    } else if (OB_FAIL(resolve_bm25_write_exprs(total_doc_cnt_expr, avg_doc_token_cnt_expr))) {
      LOG_WARN("failed to resolve bm25 write exprs for async phrase", K(ret));
    } else if (OB_FAIL(write_bm25_params_to_exprs(
               total_doc_cnt, avg_doc_token_cnt, total_doc_cnt_expr, avg_doc_token_cnt_expr))) {
      LOG_WARN("failed to write async bm25 params to exprs", K(ret));
    }
  } else if (OB_FAIL(estimator_.do_estimation(search_ctx_))) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else if (OB_FAIL(resolve_bm25_write_exprs(total_doc_cnt_expr, avg_doc_token_cnt_expr))) {
    LOG_WARN("failed to resolve bm25 write exprs for sync phrase", K(ret));
  } else if (OB_FAIL(write_bm25_params_to_exprs(
             total_doc_cnt, avg_doc_token_cnt, total_doc_cnt_expr, avg_doc_token_cnt_expr))) {
    LOG_WARN("failed to write sync bm25 params to exprs", K(ret));
  }
  if (OB_SUCC(ret) && use_async_index_) {
    avg_doc_token_cnt_ = avg_doc_token_cnt;
  }
  return ret;
}

int ObDASMatchPhraseOp::init_block_max_iters_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (block_max_inited_) {
    // skip
  } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
      if (OB_FAIL(token_iters_[i]->init_block_max_iter(total_doc_cnt, avg_doc_token_cnt))) {
        LOG_WARN("failed to init block max iter", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      block_max_inited_ = true;
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::resolve_bm25_write_exprs(
    ObExpr *&total_doc_cnt_expr,
    ObExpr *&avg_doc_token_cnt_expr)
{
  int ret = OB_SUCCESS;
  total_doc_cnt_expr = nullptr;
  avg_doc_token_cnt_expr = nullptr;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)
      || OB_ISNULL(ir_ctdef_->relevance_expr_)
      || OB_ISNULL(total_doc_cnt_expr = ir_ctdef_->relevance_expr_
         ->args_[ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bm25 expr or eval ctx", K(ret),
             KP_(ir_ctdef), KP_(ir_rtdef), KP(total_doc_cnt_expr));
  } else if (OB_NOT_NULL(ir_ctdef_->avg_doc_token_cnt_expr_)) {
    avg_doc_token_cnt_expr = ir_ctdef_->avg_doc_token_cnt_expr_;
  } else if (ir_ctdef_->relevance_expr_->arg_cnt_ > ObExprBM25::AVG_DOC_CNT_PARAM_IDX
             && OB_NOT_NULL(ir_ctdef_->relevance_expr_->args_[ObExprBM25::AVG_DOC_CNT_PARAM_IDX])) {
    avg_doc_token_cnt_expr = ir_ctdef_->relevance_expr_->args_[ObExprBM25::AVG_DOC_CNT_PARAM_IDX];
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null avg doc token cnt write slot", K(ret),
             KP_(ir_ctdef), K(ir_ctdef_->relevance_expr_->arg_cnt_));
  }
  return ret;
}

int ObDASMatchPhraseOp::write_bm25_params_to_exprs(
    const uint64_t total_doc_cnt,
    const double avg_doc_token_cnt,
    ObExpr *total_doc_cnt_expr,
    ObExpr *avg_doc_token_cnt_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)
      || OB_ISNULL(total_doc_cnt_expr) || OB_ISNULL(avg_doc_token_cnt_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bm25 write ctx", K(ret), KP_(ir_rtdef),
             KP(total_doc_cnt_expr), KP(avg_doc_token_cnt_expr));
  } else {
    ObEvalCtx &eval_ctx = *ir_rtdef_->eval_ctx_;
    if (use_rich_format_) {
      if (OB_FAIL(total_doc_cnt_expr->init_vector_for_write(
          eval_ctx, total_doc_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else if (OB_FAIL(avg_doc_token_cnt_expr->init_vector_for_write(
                 eval_ctx, avg_doc_token_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else {
        total_doc_cnt_expr->get_vector(eval_ctx)->set_int(0, total_doc_cnt);
        avg_doc_token_cnt_expr->get_vector(eval_ctx)->set_double(0, avg_doc_token_cnt);
      }
    } else {
      total_doc_cnt_expr->locate_datum_for_write(eval_ctx).set_int(total_doc_cnt);
      avg_doc_token_cnt_expr->locate_datum_for_write(eval_ctx).set_double(avg_doc_token_cnt);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
