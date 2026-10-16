/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_
#define OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

struct ObDASMatchPhraseOpParam;

struct ObDASMatchPhraseCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchPhraseCtDef(ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_MATCH_PHRASE_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      slop_(nullptr),
      ir_ctdef_idx_(-1)
  {
    set_is_scoring(true);
  }
  ObDASIRScanCtDef *get_ir_ctdef() const
  {
    return ir_ctdef_idx_ >= 0 ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_idx_]) : nullptr;
  }
  virtual ~ObDASMatchPhraseCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(slop),
      K_(ir_ctdef_idx));
public:
  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *slop_;
  int64_t ir_ctdef_idx_;
};

struct ObDASMatchPhraseRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchPhraseRtDef()
    : ObIDASSearchRtDef(DAS_OP_MATCH_PHRASE_QUERY),
      query_tokens_(),
      token_ids_(),
      token_offsets_(),
      has_duplicate_tokens_(false),
      query_tokens_prepared_(false),
      allocator_(ObMemAttr(MTL_ID(), "DASMatchPQ")),
      block_max_param_(),
      block_max_param_initialized_(false)
  {
    query_tokens_.set_attr(ObMemAttr(MTL_ID(), "DASPhraseTok"));
    token_ids_.set_attr(ObMemAttr(MTL_ID(), "DASPhraseId"));
    token_offsets_.set_attr(ObMemAttr(MTL_ID(), "DASPhraseOff"));
  }
  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override;
  int copy_cached_runtime_state_from(const ObIDASSearchRtDef &other) override;
  ObDASIRScanRtDef *get_ir_rtdef() const
  {
    const int64_t ir_rtdef_idx = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_)->ir_ctdef_idx_;
    return ir_rtdef_idx >= 0 ? static_cast<ObDASIRScanRtDef *>(children_[ir_rtdef_idx]) : nullptr;
  }
private:
  int prepare_query_tokens();
  int compute_async_phrase_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  void reset_cached_runtime_state();
  int generate_single_segment_op(
      const ObDASMatchPhraseCtDef &ctdef,
      ObDASMatchPhraseOpParam &phrase_param,
      const bool use_single_token,
      const double token_boost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op);

  ObSEArray<ObString, 16> query_tokens_;
  ObSEArray<int64_t, 16> token_ids_;
  ObSEArray<int64_t, 16> token_offsets_;
  bool has_duplicate_tokens_;
  bool query_tokens_prepared_;
public:
  ObArenaAllocator allocator_; // op-level lifetime
  storage::ObBlockMaxScoreIterParam block_max_param_;
  bool block_max_param_initialized_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_
