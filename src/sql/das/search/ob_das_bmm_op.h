/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_BMM_OP_H_
#define OCEANBASE_SQL_OB_DAS_BMM_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_daat_merge_iter.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/das/search/ob_das_sparse_candidate_buffer.h"
#include "sql/engine/ob_bit_vector.h"


namespace oceanbase
{
namespace sql
{
class ObIDisjunctiveDAATMerger;

class ObDASBMMOpParam : public ObIDASSearchOpParam
{
public:
  ObDASBMMOpParam(
      const ObIArray<ObIDASSearchOp *> &score_ops,
      ObIDASSearchOp *filter_op,
      ObIAllocator &iter_allocator,
      const ObDASBMMBuildOptions &options,
      const ObIArray<ObDASSearchCost> *score_op_costs = nullptr)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_BMM),
      score_ops_(&score_ops),
      score_op_costs_(score_op_costs),
      filter_op_(filter_op),
      iter_allocator_(&iter_allocator),
      build_options_(options)
  {}
  virtual ~ObDASBMMOpParam() {}
  INHERIT_TO_STRING_KV("ObDASBMMOpParam", ObIDASSearchOpParam,
      KPC_(score_ops),
      KPC_(score_op_costs),
      KP_(filter_op),
      KP_(iter_allocator),
      K_(build_options));

  bool is_valid() const
  {
    return nullptr != score_ops_
        && nullptr != iter_allocator_
        && (ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN == build_options_.filter_mode_
            ? (build_options_.has_explicit_vectorized_merge_
                && build_options_.enable_vectorized_merge_ && !query_optional()
                && nullptr != filter_op_ && filter_op_->supports_direct_rowid_drain())
            : (nullptr == filter_op_ || !filter_op_->supports_direct_rowid_drain()
                || filter_op_->can_enumerate_rowids()))
        && (nullptr == score_op_costs_ || score_ops_->count() == score_op_costs_->count());
  }
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_score_ops() const { return score_ops_; }
  OB_INLINE const ObIArray<ObDASSearchCost> *get_score_op_costs() const { return score_op_costs_; }
  OB_INLINE ObIDASSearchOp *get_filter_op() const { return filter_op_; }
  OB_INLINE bool has_filter_op() const { return nullptr != filter_op_; }
  OB_INLINE const ObDASBMMBuildOptions *get_build_options() const
  { return build_options_.has_explicit_vectorized_merge_ ? &build_options_ : nullptr; }
  OB_INLINE ObIAllocator *get_iter_allocator() const { return iter_allocator_; }
  OB_INLINE bool query_optional() const { return build_options_.query_optional_; }
  OB_INLINE ObDASSearchCost get_lead_cost() const { return build_options_.lead_cost_; }
  OB_INLINE ObDASSearchCost get_filter_cost() const { return build_options_.filter_cost_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObIArray<ObIDASSearchOp *> *score_ops_;
  const ObIArray<ObDASSearchCost> *score_op_costs_;
  ObIDASSearchOp *filter_op_;
  ObIAllocator *iter_allocator_;
  ObDASBMMBuildOptions build_options_;
};

// Block Max MaxScore top-k dynamic pruning operator
class ObDASBMMOp : public ObIDASSearchOp
{
public:
  ObDASBMMOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASBMMOp() {}
  OB_INLINE ObIDASSearchOp *get_filter_op() const { return filter_op_; }
  static void select_filter_build_options(
      const ObDASSearchCtx &search_ctx,
      const ObDASSearchCost &lead_cost,
      const ObDASSearchCost &filter_cost,
      const bool query_optional,
      const bool supports_direct_drain,
      ObDASBMMBuildOptions &options);

private:
  enum class FilterMergeMode {
    DENSE_WINDOW,
    SPARSE_SEEK,
  };
  static void select_filter_strategy(
      const ObDASSearchCost &lead_cost,
      const ObDASSearchCost &filter_cost,
      const ObDASSearchCost &table_row_count,
      const bool has_filter,
      const bool query_optional,
      FilterMergeMode &merge_mode,
      bool &filter_after_score);
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_set_min_competitive_score(const double &threshold) override;

private:
  static int compare_uint64_rowid(const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret);
  bool has_filter() const { return nullptr != filter_op_; }
  bool can_seek_filter_for_vec_handoff() const
  {
    return has_filter() && ObDASBMMBuildOptions::FilterMode::ORDERED == filter_mode_
        && filter_op_->can_enumerate_rowids();
  }
  bool is_filter_driven_optional() const
  {
    return query_optional_ && 0.0 == min_competitive_score_;
  }
  double get_essential_dim_threshold() const { return min_competitive_score_ - non_essential_dim_max_score_; }
  int init_merge_iters(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator);
  int init_vec_merge_on_demand(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator);
  int inner_get_next_row_plain(ObDASRowID &next_id, double &score);
  int inner_get_next_row_with_pruning(ObDASRowID &next_id, double &score);
  int inner_get_next_row_vectorized(ObDASRowID &next_id, double &score);
  // Vectorized advance_to: try consuming from the already-materialized
  // doc_score_iter_ window first (fast path); on ITER_END fall through to a
  // full boundary recompute at target.
  int inner_advance_to_vectorized(const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  int inner_advance_to_vectorized_scoring_driven(
      const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  int inner_advance_to_vectorized_filter_driven(
      const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  int rebuild_vectorized_window_at(const ObDASRowID &target);
  int inner_get_next_batch();
  int inner_get_next_batch_scoring_driven();
  // Query-optional BMM is filter-driven while the competitive threshold is zero:
  // filter hits create result presence and scoring dimensions enrich their scores.
  int inner_get_next_batch_filter_driven();
  int sort_iters_by_max_score();
  int try_update_essential_dims();
  int evaluate_pivot_range(const ObDASRowID &pivot_id, double &non_essential_block_max_score, bool &is_candidate);
  int evaluate_essential_pivot(
      const ObDASRowID &pivot_id,
      const double non_essential_block_max_score,
      ObDASRowID &collected_id,
      double &essential_score,
      bool &is_candidate,
      bool &is_valid_pivot);
  int evaluate_pivot(const ObDASRowID &pivot_id, const double &essential_score, double &score);
  int next_filter_rowid(ObDASRowID &next_id);
  int advance_filter_to(const ObDASRowID &target, ObDASRowID &curr_id);
  int get_next_row_plain_with_filter(ObDASRowID &next_id, double &score);
  int find_next_pivot_with_filter(ObDASRowID &pivot_id);
  int evaluate_bmm_pivot(
      const ObDASRowID &pivot_id,
      const double &non_essential_bm_score,
      ObDASRowID &next_id,
      double &score,
      bool &found_row);
  int compute_vec_boundary();
  int find_next_vec_window_start(ObDASRowID &min_id);
  int find_vec_handoff(const ObDASRowID &lower, ObDASRowID &next);
  // Compute boundary starting at a known target.
  int compute_vec_boundary_at(const ObDASRowID &target);
  // Expand a candidate window to the vectorized minimum boundary size.
  int expand_vec_boundary(const ObDASRowID &candidate_start, ObDASRowID &boundary) const;
  // Refresh token block metadata and repartition dimensions at the specified start rowid.
  int repartition_vec_dims_at(
      const ObDASRowID &min_id,
      const bool min_id_inclusive,
      bool &use_all_dims);
  // Calculate the block upper bound and boundary from essential dimensions only.
  int compute_vec_essential_boundary(
      const int64_t non_essential_dim_count,
      ObDASRowID &mini_max_id,
      double &essential_block_max_score);
  // Refine the execution partition with conservative bounds over the frozen window.
  int refine_vec_dims_with_range_max(
      const ObDASRowID &min_id,
      const bool min_id_inclusive,
      const ObDASRowID &boundary,
      bool &skip_range);
  // Build reverse range-local max-score sums for non-essential dimensions.
  int build_vec_non_essential_suffix_sums();
  // Advance essential dimensions to the candidate start rowid and rebuild the merger.
  int rebuild_vec_essential_merger(const ObDASRowID &min_id);
  int evaluate_essential_batch(const ObDASRowID &batch_end_id, bool &is_batch_valid);
  // Prepare filter for a vectorized batch. Filter-empty or exhausted batches
  // are reported via need_evaluate_batch=false and handled by the state machine.
  int prepare_filter_for_vectorized_batch(
      const ObDASRowID &batch_start_id,
      ObDASRowID &batch_end_id,
      bool &need_evaluate_batch);
  int prepare_direct_filter_for_vectorized_batch(
      const ObDASRowID &batch_start_id,
      ObDASRowID &batch_end_id,
      bool &need_evaluate_batch);
  int apply_filter_to_scored_batch();
  // Probe a sparse active candidate set against one non-essential dimension.
  int score_active_candidates(ObDAATMergeIter &iter);
  int evaluate_non_essential_batch(
      const ObDASRowID &batch_end_id,
      bool &is_batch_valid);
  // Build filter bitmask for [batch_start_id, batch_end_id] into filter_bitset_.
  // Advances curr_filter_id_ past batch_end_id. In filter-driven optional mode
  // the same drain also seeds accumulator presence with score 0. Called BEFORE
  // evaluation so the accumulator mask skips non-matching scoring docs during
  // add_dim_batch.
  int build_filter_bitset(const ObDASRowID &batch_start_id, const ObDASRowID &batch_end_id);
  // Materialize direct-drain RowIDs into the window bitmask.
  // has_hit reports whether the window contains a hit. Cache the lookahead past batch_end_id,
  // or set filter_end_ and curr_filter_id_ to EOF state when the source is exhausted.
  int build_direct_filter_bitset(
      const ObDASRowID &batch_start_id,
      const ObDASRowID &batch_end_id,
      bool &has_hit);
  // Advance only the essential iters lagging behind target, track the minimum surviving
  // curr_id (min_id) and sum the scores of iters whose curr_id == target (SUM semantics,
  // same as merge_one_row). has_alive is false when every essential iter is EOF.
  int align_essential_iters_to_target(
      const ObDASRowID &target,
      ObDASRowID &min_id,
      double &essential_score,
      bool &has_alive);
  // Align filter and essential iters within the current window. On OB_SUCCESS:
  // !window_end: matched_id == curr_filter_id_, essential_score sums essential scores;
  // window_end: finish the window; candidate outputs are invalid.
  // OB_ITER_END: filter EOF, or all essential iters EOF before a window is established.
  // Leaves the matched rows unconsumed and row_merger_ untouched.
  int find_leapfrog_candidate(ObDASRowID &matched_id, double &essential_score, bool &window_end);
  // Fill the candidate buffer with filter-hit essential intersections inside the current
  // boundary window, admitting only candidates whose essential score plus the non-essential
  // dim-max upper bound beats min_competitive_score_. Boundary crossing / buffer full stops
  // the fill. Returns OB_ITER_END only when the scan is exhausted with an empty buffer.
  int collect_leapfrog_candidates();
  // Score alive candidates dim by dim (highest dim-max first) and prune candidates that
  // cannot beat min_competitive_score_ with the remaining dim-max budget.
  int score_leapfrog_candidates();
private:
  enum SearchStatus {
    FIND_NEXT_PIVOT,
    EVALUATE_PIVOT_RANGE,
    EVALUATE_PIVOT,
    FIND_NEXT_PIVOT_RANGE,
    FOUND_NEXT_ROW,
    FINISHED,
    COMPUTE_VEC_BOUNDARY,
    EVALUATE_VEC_ESSENTIAL,
    EVALUATE_VEC_NON_ESSENTIAL,
    EVALUATE_VEC_LEAP_FROG,
    MAX_STATUS,
  };
private:
  ObIAllocator *iter_allocator_;
  ObFixedArray<ObDAATMergeIter *, ObIAllocator> merge_iters_;
  ObFixedArray<ObDASSearchCost, ObIAllocator> score_op_costs_;
  // Cost gates indexed by merge iterator index, refreshed on first use after open/rescan.
  ObFixedArray<bool, ObIAllocator> score_op_probe_allowed_;
  ObFixedArray<int64_t, ObIAllocator> sorted_iter_idxes_;
  // 按照上述分数上界从小到大保存迭代器下标，用于划分 non-essential 和 essential 维度。
  ObFixedArray<int64_t, ObIAllocator> vec_sorted_iter_idxes_;
  // 记录每个迭代器在当前向量化区间内的分数上界，Range Max 不可用时记录全局上界。
  ObFixedArray<double, ObIAllocator> vec_range_max_scores_;
  // Max scores indexed by merge iterator index, refreshed before each sort.
  ObFixedArray<double, ObIAllocator> dim_max_scores_;
  int64_t non_essential_dim_count_;
  double non_essential_dim_max_score_;
  double non_essential_dim_threshold_;
  // 使用 Range Max 进一步细化后的 non-essential 维度数和分数上界。
  int64_t vec_non_essential_dim_count_;
  double vec_non_essential_dim_max_score_;
  // Sum of global dim max scores for the current non-essential dimensions, used for window handoff.
  double vec_non_essential_dim_max_score_sum_;
  // 记录上次全局上界分组时已经结束的 token 数量。
  int64_t vec_partition_iter_end_count_;
  ObIDisjunctiveDAATMerger *row_merger_;
  ObIDASSearchOp *filter_op_;
  ObDASRowID curr_filter_id_;
  SearchStatus status_;
  ObFixedArray<double, ObIAllocator> non_ess_block_max_score_suffix_sums_;
  // Pre-allocated bitmask for every path that may execute a dense window, including
  // query-optional whose preselected scoring mode is SPARSE_SEEK.
  ObBitVector *filter_bitset_;
  // Dense score window and iterator. Query-optional always starts here so filter-only
  // zero-score documents remain visible before a positive competitive threshold arrives.
  DocScoreAccumulator doc_score_accumulator_;
  DocScoreIter doc_score_iter_;
  ObDASRowID vec_boundary_id_;
  bool enable_vec_merge_;
  ObDASBMMBuildOptions::FilterMode filter_mode_;
  bool query_optional_;
  // Estimated cost of the driver (lead) clause captured at init time. Reserved for
  // future cost-aware pruning decisions; currently stored but not yet consumed.
  ObDASSearchCost lead_cost_;
  ObDASSearchCost filter_cost_;
  bool iters_sorted_;
  bool filter_end_;
  // Scoring exhaustion is not root EOF in query-optional mode; remaining filter
  // rows are still emitted with score 0 while the competitive threshold is 0.
  bool optional_scoring_end_;
  // Set after skipping an ordered-filter DENSE_WINDOW batch with no hits. The next
  // vectorized batch is capped at curr_filter_id_ to avoid scoring past the first hit.
  bool cap_next_batch_to_filter_id_;
  // Static strategy for ordered required dense filters, selected from filter cost at init.
  bool filter_after_score_;
  // Within candidate-filter mode, medium-density filters may seek sparse active
  // candidates; very dense filters keep the sequential posting drain.
  bool use_active_candidate_probe_;
  // Preselected scoring-driven filter merge mode. Filter-driven query-optional execution
  // always uses a dense window regardless of this value.
  FilterMergeMode filter_merge_mode_;
  SparseCandidateBuffer sparse_candidate_buffer_;
  bool is_inited_;
};



} // namespace sql
} // namespace oceanbase
#endif
