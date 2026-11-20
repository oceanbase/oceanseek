/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_search_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/optimizer/ob_storage_estimator.h"

namespace oceanbase
{
namespace sql
{
int ObIDASSearchRtDef::set_bmm_input(
    const bool query_optional,
    ObIDASSearchOp *filter_op,
    ObDASSearchCost filter_cost,
    const ObDASBMMBuildOptions &options)
{
  UNUSEDx(query_optional, filter_op, filter_cost, options);
  return OB_NOT_SUPPORTED;
}


OB_SERIALIZE_MEMBER((ObIDASSearchCtDef, ObDASAttachCtDef), is_scoring_, is_top_level_scoring_);

OB_SERIALIZE_MEMBER((ObIDASSearchRtDef, ObDASAttachRtDef));

int ObIDASSearchRtDef::copy_cached_runtime_state_from(const ObIDASSearchRtDef &other)
{
  cost_ = other.cost_;
  return OB_SUCCESS;
}

int ObIDASSearchRtDef::get_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  if (cost_.is_valid()) {
    cost = cost_;
  } else if (OB_FAIL(compute_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute cost", K(ret));
  } else if (!cost.is_valid()) {
    cost = search_ctx.get_row_count();
    cost_ = cost;
  } else {
    cost_ = cost;
  }
  return ret;
}

void ObIDASSearchRtDef::reset_cost_recursive()
{
  cost_.reset();
  for (uint32_t i = 0; i < children_cnt_; ++i) {
    ObDASBaseRtDef *child = children_[i];
    if (nullptr != child && nullptr != child->ctdef_ && child->ctdef_->is_search_ctdef()) {
      static_cast<ObIDASSearchRtDef *>(child)->reset_cost_recursive();
    }
  }
}

int ObIDASSearchRtDef::can_pushdown_filter_to_bmm(
    ObDASSearchCtx &search_ctx,
    bool &can_pushdown)
{
  UNUSED(search_ctx);
  can_pushdown = false;
  return OB_SUCCESS;
}

int ObIDASSearchRtDef::get_bmm_filter_consumer_counts(
    ObDASSearchCtx &search_ctx,
    int64_t &group_consumer_count,
    int64_t &query_consumer_count)
{
  UNUSED(search_ctx);
  group_consumer_count = 1;
  query_consumer_count = 1;
  return OB_SUCCESS;
}

void ObIDASSearchRtDef::set_pushdown_filter(
    const bool query_optional,
    ObIDASSearchOp *filter_op)
{
  set_pushdown_filter(query_optional, filter_op, ObDASSearchCost());
}

void ObIDASSearchRtDef::set_pushdown_filter(
    const bool query_optional,
    ObIDASSearchOp *filter_op,
    ObDASSearchCost filter_cost)
{
  UNUSED(filter_op);
  UNUSED(query_optional);
  UNUSED(filter_cost);
}

int ObIDASSearchRtDef::set_pushdown_filter_provider(
    const bool query_optional,
    ObDASFilterBitmapProvider *provider,
    ObDASSearchCost filter_cost)
{
  UNUSED(query_optional);
  UNUSED(provider);
  UNUSED(filter_cost);
  return OB_NOT_SUPPORTED;
}

int ObIDASSearchRtDef::prefer_primary_probe(
  ObDASSearchCost lead_cost,
  ObDASSearchCtx &search_ctx,
  bool &prefer_primary_probe)
{
  int ret = OB_SUCCESS;
  prefer_primary_probe = false;
  if (OB_ISNULL(ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef is null", K(ret));
  } else {
    const ObIDASSearchCtDef *search_ctdef = static_cast<const ObIDASSearchCtDef *>(ctdef_);
    if (OB_ISNULL(search_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search_ctdef is null", K(ret));
    } else {
      const int64_t ratio = search_ctdef->get_primary_get_ratio();
      ObDASSearchCost self_cost;
      if (ratio > 0) {
        if (OB_FAIL(get_cost(search_ctx, self_cost))) {
          LOG_WARN("failed to get cost", K(ret));
        } else {
          prefer_primary_probe = lead_cost.cost() <= self_cost.cost() / ratio;
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASFusionCtDef, ObDASAttachCtDef), search_index_, rowid_exprs_, score_exprs_, rank_exprs_, weight_exprs_, path_top_k_limit_exprs_, size_expr_, offset_expr_, rank_window_size_expr_, rank_constant_expr_, min_score_expr_, has_search_subquery_, has_vector_subquery_, is_top_k_query_, fusion_method_, has_hybrid_fusion_op_, enable_parallel_, query_dop_, track_score_, fusion_iter_exec_mode_, is_single_partition_, rerank_window_size_expr_, has_nested_paths_, nested_rowid_exprs_, shard_doc_expr_, has_search_after_);

OB_SERIALIZE_MEMBER((ObDASFusionRtDef, ObDASAttachRtDef));

int ObDASFusionCtDef::init(
    int64_t search_index,
    bool has_search_subquery,
    bool has_vector_subquery,
    bool is_top_k_query,
    ObFusionMethod fusion_method,
    bool has_hybrid_fusion_op,
    bool is_single_partition,
    bool enable_parallel,
    int64_t query_dop,
    ObExpr *size_expr,
    ObExpr *offset_expr,
    ObExpr *rank_window_size_expr,
    ObExpr *rerank_window_size_expr,
    ObExpr *rank_constant_expr,
    ObExpr *min_score_expr,
    const common::ObIArray<ObExpr *> &rowid_exprs,
    const common::ObIArray<ObExpr *> &score_exprs,
    const common::ObIArray<ObExpr *> &result_output_exprs,
    const common::ObIArray<ObExpr *> &weight_exprs,
    const common::ObIArray<ObExpr *> &path_top_k_limit_exprs,
    bool track_score,
    ObFusionIterExecMode fusion_iter_exec_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowid_exprs_.assign(rowid_exprs))) {
    LOG_WARN("failed to assign rowid_exprs", K(ret));
  } else if (OB_FAIL(score_exprs_.assign(score_exprs))) {
    LOG_WARN("failed to assign score_exprs", K(ret));
  } else if (OB_FAIL(result_output_.assign(result_output_exprs))) {
    LOG_WARN("failed to assign result_output", K(ret));
  } else if (OB_FAIL(weight_exprs_.assign(weight_exprs))) {
    LOG_WARN("failed to assign weight_exprs", K(ret));
  } else if (OB_FAIL(path_top_k_limit_exprs_.assign(path_top_k_limit_exprs))) {
    LOG_WARN("failed to assign path_top_k_limit_exprs", K(ret));
  } else {
    search_index_ = search_index;
    has_search_subquery_ = has_search_subquery;
    has_vector_subquery_ = has_vector_subquery;
    is_top_k_query_ = is_top_k_query;
    fusion_method_ = fusion_method;
    has_hybrid_fusion_op_ = has_hybrid_fusion_op;
    is_single_partition_ = is_single_partition;
    enable_parallel_ = enable_parallel;
    query_dop_ = query_dop;
    size_expr_ = size_expr;
    offset_expr_ = offset_expr;
    rank_window_size_expr_ = rank_window_size_expr;
    rerank_window_size_expr_ = rerank_window_size_expr;
    rank_constant_expr_ = rank_constant_expr;
    min_score_expr_ = min_score_expr;
    track_score_ = track_score;
    fusion_iter_exec_mode_ = fusion_iter_exec_mode;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASHybridSearchSortCtDef, ObDASSortCtDef),
    after_exprs_, score_sort_flags_, unique_shard_doc_flags_, single_query_weight_expr_,
    final_score_expr_, main_ref_table_id_, sort_value_scan_ctdef_, has_sort_value_lookup_, das_sort_topn_enable_);

OB_SERIALIZE_MEMBER((ObDASHybridSearchSortRtDef, ObDASSortRtDef), sort_value_scan_rtdef_);

int64_t ObDASHybridSearchSortCtDef::unique_shard_doc_index() const
{
  int64_t result = -1;
  for (int64_t i = 0; result < 0 && i < unique_shard_doc_flags_.count(); ++i) {
    if (unique_shard_doc_flags_.at(i)) {
      result = i;
    }
  }
  return result;
}

int ObDASHybridSearchSortCtDef::validate() const
{
  int ret = OB_SUCCESS;
  const int64_t count = sort_exprs_.count();
  int64_t shard_doc_count = 0;
  if (op_type_ != DAS_OP_HYBRID_SEARCH_SORT || get_query_ctdef() == nullptr || count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid sort child definition", K(ret), K_(op_type), K_(children_cnt), K(count));
  } else if (count != sort_collations_.count() || count != sort_cmp_funcs_.count()
      || count != score_sort_flags_.count() || count != unique_shard_doc_flags_.count()
      || (has_after() && count != after_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hybrid sort metadata array lengths do not match", K(ret), K(count),
             K(sort_collations_.count()), K(sort_cmp_funcs_.count()), K(score_sort_flags_.count()),
             K(unique_shard_doc_flags_.count()), K(after_exprs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObExpr *expr = sort_exprs_.at(i);
    if (expr == nullptr || sort_cmp_funcs_.at(i).cmp_func_ == nullptr
        || (has_after() && after_exprs_.at(i) == nullptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid sort expression or comparison function is missing", K(ret), K(i), KP(expr),
               "has_cmp_func", sort_cmp_funcs_.at(i).cmp_func_ != nullptr,
               "has_after", has_after(),
               "has_after_expr", has_after() && after_exprs_.at(i) != nullptr);
    } else if (unique_shard_doc_flags_.at(i)) {
      ++shard_doc_count;
      if (score_sort_flags_.at(i) || expr->type_ != T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC
          || expr->arg_cnt_ != 2 || expr->args_ == nullptr
          || expr->args_[0] == nullptr || expr->args_[1] == nullptr
          || expr->datum_meta_.cs_type_ != CS_TYPE_BINARY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hybrid __shard_doc sort expression", K(ret), K(i), K(score_sort_flags_.at(i)),
                 K(expr->type_), K(expr->arg_cnt_), K(expr->datum_meta_.cs_type_),
                 "has_tablet_expr", expr->args_ != nullptr && expr->arg_cnt_ > 0 && expr->args_[0] != nullptr,
                 "has_increment_expr", expr->args_ != nullptr && expr->arg_cnt_ > 1 && expr->args_[1] != nullptr);
      }
    } else if (das_sort_topn_enable_ && !has_sort_value_lookup_
               && !score_sort_flags_.at(i) && !expr->is_const_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid sort field requires a sort value lookup", K(ret), K(i), K(expr->type_));
    }
  }
  if (OB_SUCC(ret) && (shard_doc_count > 1 || ((has_after() || das_sort_topn_enable_) && shard_doc_count != 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid __shard_doc sort item count", K(ret), K(shard_doc_count),
             "has_after", has_after(), K_(das_sort_topn_enable));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
