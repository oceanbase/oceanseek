/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_QUERY_STRING_QUERY_H_
#define OCEANBASE_SQL_OB_DAS_QUERY_STRING_QUERY_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/container/ob_fixed_array.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

struct ObDASQueryStringCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASQueryStringCtDef(ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_QUERY_STRING_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      default_operator_(nullptr),
      minimum_should_match_(nullptr),
      type_(nullptr),
      field_boosts_(alloc),
      ir_ctdef_indices_(alloc),
      is_msm_unresolved_expr_(false)
  {
    set_is_scoring(true);
  }
  virtual ~ObDASQueryStringCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(default_operator),
      KPC_(minimum_should_match),
      KPC_(type),
      K_(ir_ctdef_indices),
      K_(field_boosts),
      K_(is_msm_unresolved_expr));

  ObDASIRScanCtDef *get_ir_ctdef(const int64_t idx) const
  {
    return (idx >= 0 && idx < ir_ctdef_indices_.count())
        ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_indices_.at(idx)])
        : nullptr;
  }
  bool is_async_index() const
  {
    const ObDASIRScanCtDef *ir_ctdef = get_ir_ctdef(0);
    return OB_NOT_NULL(ir_ctdef) && ir_ctdef->is_async_index_;
  }

public:
  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *default_operator_;
  ObExpr *minimum_should_match_;
  ObExpr *type_;
  ExprFixedArray field_boosts_;
  ObFixedArray<int64_t, ObIAllocator> ir_ctdef_indices_;
  /// 代码生成阶段根据 minimum_should_match_ 表达式元信息设置，用于区分字符串和整数 MSM 路径。
  bool is_msm_unresolved_expr_;
};

struct ObDASQueryStringRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASQueryStringRtDef()
    : ObIDASSearchRtDef(DAS_OP_QUERY_STRING_QUERY),
      token_groups_(),
      token_boosts_(),
      field_token_costs_(),
      field_costs_(),
      group_costs_(),
      token_count_(0),
      query_groups_prepared_(false),
      allocator_(ObMemAttr(MTL_ID(), "DASQStrQ")),
      block_max_params_(&allocator_),
      pushdown_filter_op_(nullptr),
      pushdown_filter_cost_(),
      block_max_params_initialized_(false),
      query_optional_(false),
      bmm_build_options_(false),
      has_bmm_build_options_(false)
  {
    token_groups_.set_attr(ObMemAttr(MTL_ID(), "DASQStrGrp"));
    token_boosts_.set_attr(ObMemAttr(MTL_ID(), "DASQStrBst"));
    field_token_costs_.set_attr(ObMemAttr(MTL_ID(), "DASQStrTCost"));
    field_costs_.set_attr(ObMemAttr(MTL_ID(), "DASQStrFCost"));
    group_costs_.set_attr(ObMemAttr(MTL_ID(), "DASQStrGCost"));
  }
  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override;
  int copy_cached_runtime_state_from(const ObIDASSearchRtDef &other) override;
  int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override;
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
  void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override
  {
    pushdown_filter_op_ = filter_op;
    has_bmm_build_options_ = false;
    pushdown_filter_cost_ = filter_cost;
    query_optional_ = query_optional;
  }
private:
  static int generate_op_impl_adapter(
      ObIDASSearchRtDef &rtdef,
      const ObIDASSearchCtDef &ctdef,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op);
  int generate_op_impl(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op);
  inline static constexpr char RESERVED_CHARS[] = {
      '+', '-', '&', '|', '!', '=', '<', '>', '(', ')', '[', ']', '{', '}',
      '"', '~', '*', '?', ':', '\\', '/'
  };
  inline static constexpr const char *RESERVED_KEYWORDS[] = {
      "or", "and", "not", "to"
  };
  ObDASIRScanRtDef *get_ir_rtdef(const int64_t idx) const
  {
    ObDASIRScanRtDef *pret = nullptr;
    const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
    if (OB_NOT_NULL(ctdef) && OB_LIKELY(idx >= 0 && idx < ctdef->ir_ctdef_indices_.count())) {
      const int64_t child_idx = ctdef->ir_ctdef_indices_.at(idx);
      if (child_idx >= 0 && child_idx < children_cnt_) {
        pret = static_cast<ObDASIRScanRtDef *>(children_[child_idx]);
      }
    }
    return pret;
  }
  int prepare_query_groups();
  int compute_async_query_string_cost(
      ObDASSearchCtx &search_ctx,
      ObDASSearchCost &cost);
  int init_block_max_params(const ObDASQueryStringCtDef &ctdef, const int64_t field_cnt);
  // Align one cached BMM cost array to the shape the current operator consumes:
  // trigger cold cost estimation when missing, validate async costs, or fill
  // invalid placeholder costs for the non-async path.
  int align_cost_array(
      ObDASSearchCtx &search_ctx,
      ObArray<ObDASSearchCost> &costs,
      const int64_t expected_cnt,
      const char *cost_name);
  void reset_cached_runtime_state();
  int generate_group_field_op(const int64_t group_idx,
                              const int64_t field_idx,
                              const common::ObIArray<common::ObString> &tokens,
                              const double token_boost,
                              const int64_t default_operator,
                              const int64_t group_cnt,
                              const int64_t match_fields_type,
                              const ObDASSearchCost &lead_cost,
                              ObDASSearchCtx &search_ctx,
                              ObIDASSearchOp *&field_op);
  int parse_query_string();
  static int check_reserved_operators(const ObString &query);
  ObArray<ObArray<ObString>> token_groups_;
  ObArray<double> token_boosts_;
  // Token-level costs consumed by the field-level token BMM. Cached only for a
  // top-level scoring query with a single token group, in flat field-major
  // layout: index = field_idx * token_cnt + token_idx.
  ObArray<ObDASSearchCost> field_token_costs_;
  // Per-field costs consumed by the MOST_FIELDS outer BMM, gated the same as
  // field_token_costs_.
  ObArray<ObDASSearchCost> field_costs_;
  // Per-group costs consumed by the outermost group BMM. Cached only for a
  // top-level scoring query.
  ObArray<ObDASSearchCost> group_costs_;
  int64_t token_count_;
  bool query_groups_prepared_;
public:
  ObArenaAllocator allocator_; // op-level lifetime
  ObFixedArray<ObBlockMaxScoreIterParam, ObIAllocator> block_max_params_;
  ObIDASSearchOp *pushdown_filter_op_;
  ObDASSearchCost pushdown_filter_cost_;
  bool block_max_params_initialized_;
  bool query_optional_;
  ObDASBMMBuildOptions bmm_build_options_;
  bool has_bmm_build_options_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_QUERY_STRING_QUERY_H_
