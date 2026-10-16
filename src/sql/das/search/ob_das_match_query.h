/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_
#define OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

// Execution meta data for Match Query:
//   Run disjunctive / conjunctive retrieval on tokens tokenized from query text
struct ObDASMatchCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_MATCH_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      match_operator_(nullptr),
      minimum_should_match_(nullptr),
      ir_ctdef_idx_(-1),
      pushdown_must_filter_op_ctdef_idx_(-1),
      is_msm_unresolved_expr_(false)
  {
    set_is_scoring(true);
  }
  ObDASIRScanCtDef *get_ir_ctdef() const
  {
    return ir_ctdef_idx_ >= 0 ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_idx_]) : nullptr;
  }
  ObIDASSearchCtDef *get_pushdown_must_filter_op_ctdef() const
  {
    return pushdown_must_filter_op_ctdef_idx_ >= 0
        ? static_cast<ObIDASSearchCtDef *>(children_[pushdown_must_filter_op_ctdef_idx_]) : nullptr;
  }
  virtual ~ObDASMatchCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(match_operator),
      KPC_(minimum_should_match),
      K_(ir_ctdef_idx),
      K_(pushdown_must_filter_op_ctdef_idx),
      K_(is_msm_unresolved_expr));

  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *match_operator_;
  ObExpr *minimum_should_match_;
  int64_t ir_ctdef_idx_;
  int64_t pushdown_must_filter_op_ctdef_idx_;
  /// 代码生成阶段记录 MSM 表达式是否为字符串类型；表达式元信息擦除后，执行阶段使用该标记。
  bool is_msm_unresolved_expr_;
};

struct ObDASMatchRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchRtDef()
    : ObIDASSearchRtDef(DAS_OP_MATCH_QUERY),
      query_tokens_(),
      token_counts_(),
      token_costs_(),
      total_token_cnt_(0),
      query_tokens_prepared_(false),
      allocator_(ObMemAttr(MTL_ID(), "DASMatchQ")),
      block_max_param_(),
      pushdown_filter_op_(nullptr),
      pushdown_filter_provider_(nullptr),
      streaming_group_filter_(nullptr),
      pushdown_filter_cost_(),
      block_max_param_initialized_(false),
      query_optional_(false),
      bmm_build_options_(false),
      has_bmm_build_options_(false)
  {
    query_tokens_.set_attr(ObMemAttr(MTL_ID(), "DASMatchTok"));
    token_counts_.set_attr(ObMemAttr(MTL_ID(), "DASMatchTokCnt"));
    token_costs_.set_attr(ObMemAttr(MTL_ID(), "DASMatchCost"));
  }
  virtual ~ObDASMatchRtDef() {}
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  virtual int copy_cached_runtime_state_from(const ObIDASSearchRtDef &other) override;
  virtual int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override;
  virtual int get_bmm_filter_consumer_counts(
      ObDASSearchCtx &search_ctx,
      int64_t &group_consumer_count,
      int64_t &query_consumer_count) override;
  int set_bmm_input(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost,
      const ObDASBMMBuildOptions &options) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(filter_op)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      set_pushdown_filter(query_optional, filter_op, filter_cost);
      bmm_build_options_ = options;
      has_bmm_build_options_ = true;
    }
    return ret;
  }
  using ObIDASSearchRtDef::set_pushdown_filter;
  void set_streaming_group_filter(ObDASScalarScanRtDef *filter, ObDASSearchCost cost)
  {
    set_pushdown_filter(false, nullptr, cost);
    streaming_group_filter_ = filter;
  }
  virtual void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override
  {
    pushdown_filter_op_ = filter_op;
    has_bmm_build_options_ = false;
    pushdown_filter_provider_ = nullptr;
    streaming_group_filter_ = nullptr;
    pushdown_filter_cost_ = filter_cost;
    query_optional_ = query_optional;
  }
  virtual int set_pushdown_filter_provider(
      const bool query_optional,
      ObDASFilterBitmapProvider *provider,
      ObDASSearchCost filter_cost) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(provider)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cannot install a null filter bitmap provider", K(ret));
    } else {
      pushdown_filter_op_ = nullptr;
      has_bmm_build_options_ = false;
      pushdown_filter_provider_ = provider;
      streaming_group_filter_ = nullptr;
      pushdown_filter_cost_ = filter_cost;
      query_optional_ = query_optional;
    }
    return ret;
  }
  ObDASIRScanRtDef *get_ir_rtdef() const
  {
    const int64_t ir_rtdef_idx = static_cast<const ObDASMatchCtDef *>(ctdef_)->ir_ctdef_idx_;
    return ir_rtdef_idx >= 0 ? static_cast<ObDASIRScanRtDef *>(children_[ir_rtdef_idx]) : nullptr;
  }
  ObIDASSearchRtDef *get_pushdown_must_filter_op_rtdef() const
  {
    const int64_t pushdown_must_filter_op_rtdef_idx
        = static_cast<const ObDASMatchCtDef *>(ctdef_)->pushdown_must_filter_op_ctdef_idx_;
    return pushdown_must_filter_op_rtdef_idx >= 0
        ? static_cast<ObIDASSearchRtDef *>(children_[pushdown_must_filter_op_rtdef_idx]) : nullptr;
  }
private:
  int generate_op_impl(ObDASSearchCost lead_cost,
                       ObDASSearchCtx &search_ctx,
                       ObIDASSearchOp *&op);
  const ObDASMatchCtDef * get_ctdef() const { return static_cast<const ObDASMatchCtDef *>(ctdef_); }
  int prepare_query_tokens();
  int compute_async_match_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  int align_token_costs();
  static bool should_use_bmm_op(
      bool is_top_level_scoring,
      int64_t match_operator,
      int64_t minimum_should_match);
  void reset_cached_runtime_state();

  // Deduplicated tokens tokenized from the query text; token_counts_[i] records
  // the occurrence count of query_tokens_[i], and total_token_cnt_ is the sum of
  // token_counts_ (i.e. the clause count seen by minimum_should_match semantics).
  ObSEArray<ObString, 16> query_tokens_;
  ObSEArray<int64_t, 16> token_counts_;
  ObSEArray<ObDASSearchCost, 16> token_costs_;
  int64_t total_token_cnt_;
  bool query_tokens_prepared_;
public:
  ObArenaAllocator allocator_; // op-level lifetime
  storage::ObBlockMaxScoreIterParam block_max_param_;
  ObIDASSearchOp *pushdown_filter_op_;
  ObDASFilterBitmapProvider *pushdown_filter_provider_;
  // Construction-only recipe, instantiated separately for each FTS BMM.
  ObDASScalarScanRtDef *streaming_group_filter_;
  ObDASSearchCost pushdown_filter_cost_;
  bool block_max_param_initialized_;
  bool query_optional_;
  ObDASBMMBuildOptions bmm_build_options_;
  bool has_bmm_build_options_;
};


}
}
#endif // OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_
