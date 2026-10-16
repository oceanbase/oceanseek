/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_
#define OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_

#include "sql/das/ob_das_ir_define.h"
#include "ob_i_das_search_op.h"
#include "ob_das_async_token_helper.h"
#include "ob_das_token_utils.h"
#include "storage/retrieval/ob_text_retrieval_token_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace storage
{
class ObPhraseMatchCounter;
}

namespace share
{
class ObFTSPositionListStore;
}

namespace sql
{

struct ObDASMatchPhraseOpParam : public ObIDASSearchOpParam
{
  ObDASMatchPhraseOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_MATCH_PHRASE),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      block_max_param_(nullptr),
      token_ids_(),
      query_tokens_(),
      counter_(nullptr),
      index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1),
      direct_bind_(false),
      slop_(0),
      boost_(0.0),
      use_rich_format_(false)
  {}
  virtual ~ObDASMatchPhraseOpParam() {}

  bool is_valid() const
  {
    return nullptr != ir_ctdef_
        && nullptr != ir_rtdef_
        && (nullptr != block_max_param_ || ir_ctdef_->is_async_index_)
        && !query_tokens_.empty()
        && nullptr != counter_
        && slop_ >= 0
        && (!ir_ctdef_->is_async_index_
            || (common::OB_INVALID_ID != index_table_id_
                && (!direct_bind_ || group_slot_ >= 0)))
        && slop_ >= 0
        && boost_ > 0.0;
  }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    // leaf node, no children
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("ObDASMatchPhraseOpParam", ObIDASSearchOpParam,
      KPC_(ir_ctdef),
      KPC_(ir_rtdef),
      K_(query_tokens),
      K_(index_table_id),
      K_(group_slot),
      K_(direct_bind),
      K_(slop),
      K_(boost),
      K_(use_rich_format));

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  storage::ObBlockMaxScoreIterParam *block_max_param_;
  ObSEArray<int64_t, 16> token_ids_;
  ObSEArray<ObString, 16> query_tokens_;
  ObPhraseMatchCounter *counter_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  int64_t slop_;
  double boost_;
  bool use_rich_format_;
};

class ObDASMatchPhraseOp : public ObIDASSearchOp
{
public:
  ObDASMatchPhraseOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASMatchPhraseOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

private:
  int find_intersection(ObDASRowID &rowid, double &score);
  int pre_evaluate(bool &is_candidate);
  int evaluate(double &score);
  int select_lead_idx();
  int calculate_total_token_weight_on_demand();
  int estimate_bm25_param();
  int init_block_max_iters_on_demand();
  int resolve_bm25_write_exprs(ObExpr *&total_doc_cnt_expr, ObExpr *&avg_doc_token_cnt_expr);
  int write_bm25_params_to_exprs(
      const uint64_t total_doc_cnt,
      const double avg_doc_token_cnt,
      ObExpr *total_doc_cnt_expr,
      ObExpr *avg_doc_token_cnt_expr);
  int token_get_curr_id(const int64_t idx, const ObDatum *&curr_id) const;
  int token_advance_to(const int64_t idx, const ObDatum &target_id);
  int token_get_next_row(const int64_t idx);
  int token_get_curr_pos_list(const int64_t idx, ObString &pos_list) const;
  int token_get_curr_doc_length(const int64_t idx, int64_t &doc_length) const;
  int token_get_curr_bm25_param(
      const int64_t idx, int64_t &token_freq, int64_t &doc_length) const;
  int token_get_max_token_relevance(const int64_t idx, double &weight) const;
  int token_advance_shallow(const int64_t idx, const ObDatum &id, const bool inclusive);
  int token_get_curr_block_max_info(const int64_t idx, const storage::ObMaxScoreTuple *&tuple) const;
  int token_get_token_doc_cnt(const int64_t idx, int64_t &token_doc_cnt) const;
  int init_abs_pos_lists(const int64_t token_cnt);
  // Async-only: conjunction-style block-range intersect via helpers (no materialize).
  int do_advance_shallow_async(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple);
private:
  ObArenaAllocator allocator_;
  // Outer slots use the operator arena; inner arrays keep their default ModulePageAllocator.
  ObFixedArray<ObArray<int64_t>, ObIAllocator> abs_pos_lists_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObFixedArray<int64_t, ObIAllocator> token_ids_;
  ObFixedArray<ObDASTokenOpHelper *, ObIAllocator> token_helpers_;
  ObFixedArray<ObTextRetrievalBlockMaxIter *, ObIAllocator> token_iters_;
  ObFixedArray<ObDASAsyncTokenHelper *, ObIAllocator> async_helpers_;
  ObBM25IndexParamEstimator estimator_;
  storage::ObAsyncBM25ParamEstimator async_estimator_;
  bool use_async_index_;
  bool block_max_inited_;
  double total_token_weight_;
  double avg_doc_token_cnt_;
  ObFTSPositionListStore *decoder_;
  ObPhraseMatchCounter *counter_;
  int64_t slop_;
  double boost_;
  bool use_rich_format_;
  int64_t lead_idx_;
  ObDASRowID curr_id_;
  bool dynamic_pruning_enabled_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_
