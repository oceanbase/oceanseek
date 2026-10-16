/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_
#define DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_

#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_req_excl_op.h"
#include "sql/das/search/ob_das_req_opt_op.h"

namespace oceanbase {
namespace sql {

struct ObDASScalarScanRtDef;

template<typename T>
class ObBooleanSubClause 
{
public:
  ObBooleanSubClause() : clauses_(nullptr), clause_array_(nullptr), count_(0) {}
  ObBooleanSubClause(T **clauses, int64_t count)
      : clauses_(clauses), clause_array_(nullptr), count_(count) {}
  explicit ObBooleanSubClause(const common::ObIArray<T *> &clauses)
      : clauses_(nullptr), clause_array_(&clauses), count_(clauses.count()) {}
  ~ObBooleanSubClause() {}
  int get(int64_t idx, T *&clause) const {
    int ret = OB_SUCCESS;
    if (idx < 0 || idx >= count_
        || (OB_ISNULL(clauses_) && OB_ISNULL(clause_array_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(idx), K(clauses_), KP(clause_array_), K(count_));
    } else if (OB_NOT_NULL(clause_array_)) {
      clause = clause_array_->at(idx);
    } else {
      clause = clauses_[idx];
    }
    return ret;
  }
  int64_t count() const { return count_; }
  bool empty() const { return count_ == 0; }
private:
  T **clauses_;
  const common::ObIArray<T *> *clause_array_;
  int64_t count_;
};

struct ObDASBooleanQueryCtDef : ObIDASSearchCtDef {
  friend struct ObDASBooleanQueryRtDef;
  OB_UNIS_VERSION(1);

public:
  ObDASBooleanQueryCtDef(common::ObIAllocator &alloc)
      : ObIDASSearchCtDef(alloc, DAS_OP_BOOLEAN_QUERY), must_(), filter_(), should_(), must_not_(),
        min_should_match_(0), is_async_(false), min_should_match_expr_(nullptr),
        is_msm_unresolved_expr_(false), merged_probe_(), primary_get_ratio_(0) {}
  virtual ~ObDASBooleanQueryCtDef() {}
  int must(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int filter(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int should(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;
  int must_not(ObBooleanSubClause<ObIDASSearchCtDef> &clause) const;

  void set_must(int64_t offset, int64_t count) { must_ = SubClauseInfo(true, offset, count); }
  void set_filter(int64_t offset, int64_t count) { filter_ = SubClauseInfo(true, offset, count); }
  void set_should(int64_t offset, int64_t count) { should_ = SubClauseInfo(true, offset, count); }
  void set_must_not(int64_t offset, int64_t count) { must_not_ = SubClauseInfo(true, offset, count); }
  void set_merged_probe(int64_t offset) { merged_probe_ = SubClauseInfo(true, offset, 1); }
  bool has_merged_probe() const { return merged_probe_.exist_; }
  bool has_positive_clause() const
  { return must_.count_ > 0 || filter_.count_ > 0 || should_.count_ > 0; }
  int64_t get_merged_probe_offset() const { return merged_probe_.offset_; }
  void set_min_should_match(int64_t val) { min_should_match_ = val; }
  /// 仅代码生成阶段使用；运行时通过友元 ObDASBooleanQueryRtDef 按需直接访问。
  void set_min_should_match_expr(ObExpr *expr) { min_should_match_expr_ = expr; }
  void set_is_msm_unresolved_expr(bool is_unresolved_expr) { is_msm_unresolved_expr_ = is_unresolved_expr; }
  OB_INLINE int64_t get_min_should_match() const { return min_should_match_; }
  OB_INLINE bool is_msm_unresolved_expr() const { return is_msm_unresolved_expr_; }
  void set_is_async(bool is_async) { is_async_ = is_async; }
  bool is_async() const { return is_async_; }
  OB_INLINE int64_t get_primary_get_ratio() const override { return primary_get_ratio_; }
  OB_INLINE void set_primary_get_ratio(int64_t ratio) { primary_get_ratio_ = ratio; }

private:
  struct SubClauseInfo {
    OB_UNIS_VERSION(1);
  public:
    SubClauseInfo() : exist_(false), offset_(0), count_(0) {}
    SubClauseInfo(bool exist, int64_t offset, int64_t count) : exist_(exist), offset_(offset), count_(count) {}
    ~SubClauseInfo() {}
    TO_STRING_KV(K(exist_), K(offset_), K(count_));
    bool exist_;
    int64_t offset_;
    int64_t count_;
  };

  SubClauseInfo must_;
  SubClauseInfo filter_;
  SubClauseInfo should_;
  SubClauseInfo must_not_;
  int64_t min_should_match_;
  bool is_async_;  // async fulltext: must=query, must_not=delete scan
  ObExpr *min_should_match_expr_;
  /// 在代码生成阶段记录 minimum_should_match 表达式元信息，运行时据此区分字符串和整数路径。
  bool is_msm_unresolved_expr_;
  // Merged primary-probe ctdef slot info.
  // When exist_=true, children_[merged_probe_.offset_] is an ObDASScalarCtDef (DAS_OP_SCALAR_QUERY)
  // whose single child is an ObDASScalarScanCtDef primary point-get evaluating the complete
  // extracted bool filter. get_merged_probe_rtdef digs into the shell to get the scan rtdef.
  SubClauseInfo merged_probe_;
  // Primary-get optimization ratio for this boolean query; 0 means disabled. Sourced from the
  // DSL search option (same as scalar). Used by prefer_primary_probe to decide whether this
  // clause's filter (extracted into the merged probe) can replace its own scan.
  int64_t primary_get_ratio_;
};

struct ObDASBooleanQueryRtDef : ObIDASSearchRtDef {
  OB_UNIS_VERSION(1);

public:
  ObDASBooleanQueryRtDef(const ObDASOpType &op_type = DAS_OP_BOOLEAN_QUERY)
      : ObIDASSearchRtDef(op_type),
        allocator_(ObMemAttr(MTL_ID(), "DASBoolQuery")) {}
  virtual ~ObDASBooleanQueryRtDef() {}
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override;
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  virtual int can_pushdown_filter_to_bmm(
      ObDASSearchCtx &search_ctx,
      bool &can_pushdown) override;
  virtual int get_bmm_filter_consumer_counts(
      ObDASSearchCtx &search_ctx,
      int64_t &group_consumer_count,
      int64_t &query_consumer_count) override;
  using ObIDASSearchRtDef::set_pushdown_filter;
  int set_bmm_input(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost,
      const ObDASBMMBuildOptions &options) override;
  virtual void set_pushdown_filter(
      const bool query_optional,
      ObIDASSearchOp *filter_op,
      ObDASSearchCost filter_cost) override;
  virtual int set_pushdown_filter_provider(
      const bool query_optional,
      ObDASFilterBitmapProvider *provider,
      ObDASSearchCost filter_cost) override;
  ObDASScalarScanRtDef *get_merged_probe_rtdef() const;

private:
  static bool prefer_streaming_ror_filter(int64_t match_rows, int64_t filter_rows,
      int64_t table_rows, int64_t consumers, int64_t topk);
  int generate_op_impl(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                       ObIDASSearchOp *&op);
  int generate_async_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                        ObIDASSearchOp *&op);
  int generate_legacy_async_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                               ObIDASSearchOp *&op);
  int generate_single_async_index_op(
      ObDASSearchCost lead_cost,
      const uint64_t index_table_id,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op);
  int compute_required_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  int compute_should_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  int excl(ObIDASSearchOp *main, const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited, ObDASSearchCost lead_cost,
           ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int req(const ObBooleanSubClause<ObIDASSearchRtDef> &must, const ObBooleanSubClause<ObIDASSearchRtDef> &filter,
          const ObDASSearchCost& lead_cost, const bool is_top_level, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int opt(const ObBooleanSubClause<ObIDASSearchRtDef> &should, const int64_t min_should_match, const bool need_score,
          const ObDASSearchCost& lead_cost, const bool is_top_level, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op,
          const bool query_optional = false, ObIDASSearchOp *filter_op = nullptr,
          ObDASSearchCost filter_cost = ObDASSearchCost(),
          ObDASFilterBitmapProvider *filter_provider = nullptr,
          const ObDASBMMBuildOptions *bmm_options = nullptr);
  int prepare_bmm_input(const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
                         ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
                         ObIDASSearchOp *&filter_op, ObDASSearchCost &filter_cost,
                         const bool query_optional,
                         ObDASBMMBuildOptions &bmm_options, bool &has_bmm_options);
  // Reuse the direct Filter estimate for Provider consumers without opening a source.
  // Unknown metadata invalidates the whole aggregate, but does not fail the query.
  int estimate_filter_cost(const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
                           ObDASSearchCtx &search_ctx, ObDASSearchCost &filter_cost);
  int generate_materialized_filter_op(
      const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      const bool allow_direct_drain,
      ObIDASSearchOp *&filter_op);
  int try_get_filter_bitmap_provider(
      const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      const ObDASFilterBitmapProviderScope scope,
      const int64_t group_slot,
      ObDASFilterBitmapProvider *&provider);
  int try_get_mixed_filter_bitmap_provider(
      ObDASFilterBitmapProvider &sync_provider,
      const ObBooleanSubClause<ObIDASSearchRtDef> &async_filter_clauses,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObDASFilterBitmapProvider *&provider);
  int try_boolean_merge_probe_opt(const ObBooleanSubClause<ObIDASSearchRtDef> &must_clauses,
                                  const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
                                  const ObBooleanSubClause<ObIDASSearchRtDef> &should_clauses,
                                  const ObBooleanSubClause<ObIDASSearchRtDef> &must_not_clauses,
                                  const int64_t bool_should_msm,
                                  const ObDASSearchCost &lead_cost,
                                  ObDASSearchCtx &search_ctx,
                                  ObDASScalarScanRtDef *merged_probe_rtdef,
                                  ObIDASSearchOp *&op,
                                  bool &do_merge_probe_opt);
  int generate_conjunction_op(const ObArray<ObIDASSearchOp *> &required,
                              ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_disjunction_op(const ObArray<ObIDASSearchOp *> &optional_ops, const int64_t min_should_match, ObDASSearchCost lead_cost,
                              ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_req_excl_op(ObIDASSearchOp *required, ObIDASSearchOp *excluded,
                           ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  // async fulltext index helpers
  int get_async_fts_filter_pushdown_target(ObIDASSearchRtDef *&fts_rtdef) const;
  bool is_async() const;
  // Resolve a supported async fulltext rtdef directly or under an async Boolean wrapper.
  // The current implementation supports Match, MultiMatch and QueryString.
  int resolve_async_fts_rtdef(
      ObIDASSearchRtDef &query_rtdef,
      ObIDASSearchRtDef *&fts_rtdef) const;
  // Whether the cost-sorted conjunction would generate a primary-get probe for a direct,
  // non-driver filter scalar. If true, req() must skip BMM pushdown.
  int should_skip_bmm_for_async_fts_primary_get(
      ObIDASSearchRtDef &must_clause,
      const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
      const ObDASSearchCost &lead_cost,
      ObDASSearchCtx &search_ctx,
      bool &skip_bmm) const;
  static bool is_topk_range_probe_cost_effective(
      int64_t match_rows, int64_t filter_rows, int64_t table_rows,
      int64_t topk, int64_t primary_get_ratio);
  static void move_selected_probe_after_drivers(
      common::ObIArray<ObIDASSearchRtDef *> &required_clauses,
      ObIDASSearchRtDef *probe_clause);
  int choose_topk_range_probe(
      ObIDASSearchRtDef &must_clause,
      const ObBooleanSubClause<ObIDASSearchRtDef> &filter_clauses,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchRtDef *&probe_clause) const;
  // build the delete-index excluded op as ObDASAsyncIndexDeletionOp from a prohibited scalar clause
  int generate_async_deletion_op(const ObBooleanSubClause<ObIDASSearchRtDef> &prohibited,
                                 ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int generate_req_opt_op(ObIDASSearchOp *required, ObIDASSearchOp *optional,
                          ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op);
  int must(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int filter(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int should(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  int must_not(ObBooleanSubClause<ObIDASSearchRtDef> &clause) const;
  template<typename... Args>
  int construct_op_array(ObArray<ObIDASSearchOp*> &ops, ObIDASSearchOp *op, Args... rest_ops);
  inline int construct_op_array(ObArray<ObIDASSearchOp*> &ops) { return OB_SUCCESS; };
private:
  ObArenaAllocator allocator_; // op-level lifetime
};

struct ObDASRtDefCostComparator
{
public:
  ObDASRtDefCostComparator(ObDASSearchCtx &search_ctx) : search_ctx_(search_ctx) {}
  ~ObDASRtDefCostComparator() {}

  bool operator()(ObIDASSearchRtDef *lhs, ObIDASSearchRtDef *rhs)
  {
    bool less = false;
    if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
      // nullptr should be sorted to the end
      less = (lhs != nullptr);
    } else {
      ObDASSearchCost lhs_cost;
      ObDASSearchCost rhs_cost;
      int ret_lhs = lhs->get_cost(search_ctx_, lhs_cost);
      int ret_rhs = rhs->get_cost(search_ctx_, rhs_cost);

      if (OB_SUCCESS != ret_lhs || OB_SUCCESS != ret_rhs) {
        // If get_cost fails, treat as invalid cost
        less = (OB_SUCCESS == ret_lhs && lhs_cost.is_valid());
      } else if (!lhs_cost.is_valid() && !rhs_cost.is_valid()) {
        // Both invalid, maintain order
        less = false;
      } else if (!lhs_cost.is_valid()) {
        // lhs invalid, rhs valid - invalid should go after valid
        less = false;
      } else if (!rhs_cost.is_valid()) {
        // lhs valid, rhs invalid - valid should go before invalid
        less = true;
      } else {
        // Both valid, compare cost values (ascending order: smaller cost first)
        less = lhs_cost.cost() < rhs_cost.cost();
      }
    }
    return less;
  }

private:
  ObDASSearchCtx &search_ctx_;
};

} // namespace sql
} // namespace oceanbase

#endif // DEV_SRC_SQL_DAS_SEARCH_OB_DAS_BOOLEAN_QUERY_H_
