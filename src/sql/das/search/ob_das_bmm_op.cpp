/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include <cfloat>
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_disjunctive_daat_merger.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace sql
{

ERRSIM_POINT_DEF(DISABLE_BMM_VEC_MERGE, "disable bmm vectorized merge");

namespace
{
static constexpr int64_t BMM_VEC_MIN_BOUNDARY_SIZE = 4096 * 4;
// 启用 Range Max 剪枝的最小维度数阈值。
static constexpr int64_t BMM_RANGE_MAX_MIN_DIM_COUNT = 3;
// Prefer monotonic candidate seeks once per-dimension pruning leaves at most
// this many live docs in a 16K accumulator window.
static constexpr int64_t BMM_ACTIVE_PROBE_MAX_COUNT = 64;
// Without a scalar filter, keep low-density postings on the sequential path.
static constexpr int64_t BMM_ACTIVE_PROBE_POSTING_COST_DENOMINATOR = 8;
// Sparse active-candidate seeks pay off for the medium-density candidate-filter
// path, but not for very dense filters where a sequential posting drain is cheaper.
static constexpr int64_t BMM_ACTIVE_PROBE_FILTER_COST_DENOMINATOR = 4;

class ObBMMFilterBitsetDrainSink : public ObDASRowIDBatchConsumer
{
public:
  ObBMMFilterBitsetDrainSink(
      const uint64_t base_id,
      const int64_t capacity,
      uint64_t *filter_words,
      DocScoreAccumulator *seed_accumulator)
    : base_id_(base_id),
      capacity_(capacity),
      filter_words_(filter_words),
      seed_accumulator_(seed_accumulator),
      has_hit_(false)
  {}
  ~ObBMMFilterBitsetDrainSink() override {}

  int consume(const uint64_t *doc_ids, const int64_t count) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count < 0
                   || (count > 0 && (OB_ISNULL(doc_ids) || OB_ISNULL(filter_words_))))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid filter bitset drain batch", K(ret), KP(doc_ids), KP_(filter_words), K(count));
    } else if (0 == count) {
      // nothing to do
    } else {
      // Drain output is sorted, so validating both endpoints validates the whole batch.
      const int64_t first_offset = get_offset(doc_ids[0]);
      const int64_t last_offset = get_offset(doc_ids[count - 1]);
      if (OB_UNLIKELY(first_offset < 0 || last_offset < first_offset || last_offset >= capacity_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected doc_id batch range", K(ret), K(first_offset), K(last_offset),
            K(count), K_(base_id), K_(capacity));
      } else if (OB_NOT_NULL(seed_accumulator_)
                 && OB_FAIL(seed_accumulator_->seed_docs(doc_ids, count))) {
        LOG_WARN("failed to seed optional filter docs", K(ret), K(count));
      } else {
        // Aggregate masks in one pass so every doc offset is calculated only once.
        int64_t word_idx = das_bitset_helper::word_index(first_offset);
        uint64_t agg_mask = das_bitset_helper::offset_mask(first_offset);
        for (int64_t i = 1; i < count; ++i) {
          const int64_t offset = static_cast<int64_t>(doc_ids[i] - base_id_);
          const int64_t curr_word_idx = das_bitset_helper::word_index(offset);
          if (curr_word_idx != word_idx) {
            filter_words_[word_idx] |= agg_mask;
            word_idx = curr_word_idx;
            agg_mask = 0;
          }
          agg_mask |= das_bitset_helper::offset_mask(offset);
        }
        filter_words_[word_idx] |= agg_mask;
        has_hit_ = true;
      }
    }
    return ret;
  }

  bool has_hit() const { return has_hit_; }

private:
  int64_t get_offset(const uint64_t doc_id) const
  {
    return doc_id >= base_id_ ? static_cast<int64_t>(doc_id - base_id_) : -1;
  }

private:
  uint64_t base_id_;
  int64_t capacity_;
  uint64_t *filter_words_;
  DocScoreAccumulator *seed_accumulator_;
  bool has_hit_;
  DISALLOW_COPY_AND_ASSIGN(ObBMMFilterBitsetDrainSink);
};

} // namespace

void ObDASBMMOp::select_filter_strategy(
    const ObDASSearchCost &lead_cost,
    const ObDASSearchCost &filter_cost,
    const ObDASSearchCost &table_row_count,
    const bool has_filter,
    const bool query_optional,
    FilterMergeMode &merge_mode,
    bool &filter_after_score)
{
  static constexpr int64_t SPARSE_COST_RATIO = 100;
  static constexpr int64_t FILTER_AFTER_SCORE_COST_RATIO = 16;
  merge_mode = FilterMergeMode::DENSE_WINDOW;
  filter_after_score = false;
  // Drain capability must not change the cost-based filtering strategy.
  if (has_filter && lead_cost.is_valid() && table_row_count.is_valid()
      && table_row_count.cost() >= SPARSE_COST_RATIO
      && lead_cost.cost() <= table_row_count.cost() / SPARSE_COST_RATIO) {
    merge_mode = FilterMergeMode::SPARSE_SEEK;
  } else if (has_filter && !query_optional && filter_cost.is_valid()
      && table_row_count.is_valid() && filter_cost.cost() > 0
      && filter_cost.cost() >= table_row_count.cost() / FILTER_AFTER_SCORE_COST_RATIO) {
    filter_after_score = true;
  }
}

void ObDASBMMOp::select_filter_build_options(
    const ObDASSearchCtx &search_ctx,
    const ObDASSearchCost &lead_cost,
    const ObDASSearchCost &filter_cost,
    const bool query_optional,
    const bool supports_direct_drain,
    ObDASBMMBuildOptions &options)
{
  FilterMergeMode merge_mode = FilterMergeMode::DENSE_WINDOW;
  bool filter_after_score = false;
  select_filter_strategy(lead_cost, filter_cost, search_ctx.get_row_count(),
      true /* has_filter */, query_optional, merge_mode, filter_after_score);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  options = ObDASBMMBuildOptions(tenant_config.is_valid()
      && tenant_config->_enable_bmm_vectorized_merge);
  if (FilterMergeMode::DENSE_WINDOW == merge_mode && !filter_after_score
      && !query_optional && options.enable_vectorized_merge_
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()
      && !search_ctx.is_logical_table() && supports_direct_drain) {
    options.filter_mode_ = ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN;
  }
}

int ObDASBMMOp::compare_uint64_rowid(const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  const bool l_min = l.is_min();
  const bool l_max = l.is_max();
  const bool r_min = r.is_min();
  const bool r_max = r.is_max();
  if (l.is_normal() && r.is_normal()) {
    const uint64_t l_val = l.get_uint64();
    const uint64_t r_val = r.get_uint64();
    cmp_ret = (l_val > r_val) ? 1 : ((l_val < r_val) ? -1 : 0);
  } else if (l_max || r_max) {
    cmp_ret = l_max - r_max;
  } else if (l_min || r_min) {
    cmp_ret = -(l_min - r_min);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected min max status", K(ret), K(l), K(r));
  }
  return ret;
}

int ObDASBMMOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(score_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*score_ops_))) {
    LOG_WARN("failed to assign score ops", KR(ret));
  } else if (OB_NOT_NULL(filter_op_) && OB_FAIL(children.push_back(filter_op_))) {
    LOG_WARN("failed to push back filter op", KR(ret));
  }
  return ret;
}

ObDASBMMOp::ObDASBMMOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    iter_allocator_(nullptr),
    merge_iters_(),
    score_op_costs_(),
    score_op_probe_allowed_(),
    sorted_iter_idxes_(),
    vec_sorted_iter_idxes_(),
    vec_range_max_scores_(),
    dim_max_scores_(),
    non_essential_dim_count_(0),
    non_essential_dim_max_score_(0.0),
    non_essential_dim_threshold_(0.0),
    vec_non_essential_dim_count_(0),
    vec_non_essential_dim_max_score_(0.0),
    vec_non_essential_dim_max_score_sum_(0.0),
    vec_partition_iter_end_count_(-1),
    row_merger_(nullptr),
    filter_op_(nullptr),
    curr_filter_id_(),
    status_(SearchStatus::MAX_STATUS),
    filter_bitset_(nullptr),
    doc_score_accumulator_(),
    doc_score_iter_(),
    vec_boundary_id_(),
    enable_vec_merge_(false),
    filter_mode_(ObDASBMMBuildOptions::FilterMode::ORDERED),
    query_optional_(false),
    lead_cost_(),
    filter_cost_(),
    iters_sorted_(false),
    filter_end_(false),
    optional_scoring_end_(false),
    cap_next_batch_to_filter_id_(false),
    filter_after_score_(false),
    use_active_candidate_probe_(false),
    filter_merge_mode_(FilterMergeMode::DENSE_WINDOW),
    sparse_candidate_buffer_(),
    is_inited_(false) {}


int ObDASBMMOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASBMMOpParam &param = static_cast<const ObDASBMMOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(init_merge_iters(param, *param.get_iter_allocator()))) {
    LOG_WARN("failed to init merge iters", K(ret), K(param));
  } else if (OB_NOT_NULL(param.get_score_op_costs())
             && OB_FAIL(score_op_costs_.assign(*param.get_score_op_costs()))) {
    LOG_WARN("failed to copy score op costs", K(ret), K(param));
  } else if (OB_FAIL(ObIDisjunctiveDAATMerger::alloc_merger(
      merge_iters_,
      get_rowid_exprs(),
      get_rowid_meta(),
      get_rowid_type(),
      false,
      *param.get_iter_allocator(),
      row_merger_))) {
    LOG_WARN("failed to alloc row merger", K(ret), K(param));
  } else if (OB_FAIL(init_vec_merge_on_demand(param, *param.get_iter_allocator()))) {
    LOG_WARN("failed to init vec merge on demand", K(ret), K(param));
  } else {
    status_ = enable_vec_merge_ ? SearchStatus::COMPUTE_VEC_BOUNDARY : SearchStatus::FIND_NEXT_PIVOT;
    iter_allocator_ = param.get_iter_allocator();
    filter_op_ = param.get_filter_op();
    curr_filter_id_.set_min();
    query_optional_ = param.query_optional();
    lead_cost_ = param.get_lead_cost();
    filter_cost_ = param.get_filter_cost();
    iters_sorted_ = false;
    filter_end_ = false;
    optional_scoring_end_ = false;
    min_competitive_score_ = 0.0;
    non_essential_dim_count_ = 0;
    non_essential_dim_max_score_ = 0.0;
    non_essential_dim_threshold_ = 0.0;
    vec_non_essential_dim_count_ = 0;
    vec_non_essential_dim_max_score_ = 0.0;
    vec_non_essential_dim_max_score_sum_ = 0.0;
    vec_partition_iter_end_count_ = -1;
    is_inited_ = true;
    LOG_TRACE("initialized bmm cost metadata", K_(lead_cost), K_(filter_cost),
        K(score_op_costs_.count()), K(param.has_filter_op()));
  }
  return ret;
}

int ObDASBMMOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (query_optional_) {
    SET_METRIC_VAL(common::ObMetricId::HS_BMM_OPTIONAL_EXEC_MODE,
        enable_vec_merge_ ? 1 : 0);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child", K(ret));
    }
  }
  return ret;
}

int ObDASBMMOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(row_merger_->reuse())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
    if (OB_ISNULL(children_[i]) || OB_ISNULL(merge_iters_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan_segment(range))) {
      LOG_WARN("failed to rescan_segment child", K(ret), K(i));
    } else {
      merge_iters_[i]->reuse();
      sorted_iter_idxes_[i] = i;
    }
  }
  if (OB_SUCC(ret) && has_filter()) {
    if (OB_FAIL(filter_op_->rescan_segment(range))) {
      LOG_WARN("failed to rescan_segment filter op", K(ret));
    } else {
      curr_filter_id_.set_min();
    }
  }
  if (OB_SUCC(ret)) {
    status_ = enable_vec_merge_ ? SearchStatus::COMPUTE_VEC_BOUNDARY : SearchStatus::FIND_NEXT_PIVOT;
    min_competitive_score_ = 0.0;
    iters_sorted_ = false;
    filter_end_ = false;
    optional_scoring_end_ = false;
    curr_filter_id_.set_min();
    doc_score_iter_.reset();
    doc_score_accumulator_.clear_all();
    vec_boundary_id_.set_min();
    cap_next_batch_to_filter_id_ = false;
    non_essential_dim_count_ = 0;
    non_essential_dim_max_score_ = 0.0;
    non_essential_dim_threshold_ = 0.0;
    vec_non_essential_dim_count_ = 0;
    vec_non_essential_dim_max_score_ = 0.0;
    vec_non_essential_dim_max_score_sum_ = 0.0;
    vec_partition_iter_end_count_ = -1;
    sparse_candidate_buffer_.reuse();
  }
  return ret;
}

int ObDASBMMOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(row_merger_->reuse())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  } else if (query_optional_) {
    SET_METRIC_VAL(common::ObMetricId::HS_BMM_OPTIONAL_EXEC_MODE,
        enable_vec_merge_ ? 1 : 0);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan child", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
    if (OB_ISNULL(merge_iters_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null merge iter", K(ret), K(i));
    } else {
      merge_iters_[i]->reuse();
      sorted_iter_idxes_[i] = i;
    }
  }

  if (OB_SUCC(ret) && has_filter()) {
    curr_filter_id_.set_min();
  }

  if (OB_SUCC(ret)) {
    status_ = enable_vec_merge_ ? SearchStatus::COMPUTE_VEC_BOUNDARY : SearchStatus::FIND_NEXT_PIVOT;
    min_competitive_score_ = 0.0;
    iters_sorted_ = false;
    filter_end_ = false;
    optional_scoring_end_ = false;
    curr_filter_id_.set_min();
    doc_score_iter_.reset();
    doc_score_accumulator_.clear_all();
    vec_boundary_id_.set_min();
    cap_next_batch_to_filter_id_ = false;
    non_essential_dim_count_ = 0;
    non_essential_dim_max_score_ = 0.0;
    non_essential_dim_threshold_ = 0.0;
    vec_non_essential_dim_count_ = 0;
    vec_non_essential_dim_max_score_ = 0.0;
    vec_non_essential_dim_max_score_sum_ = 0.0;
    vec_partition_iter_end_count_ = -1;
    sparse_candidate_buffer_.reuse();
  }
  return ret;
}

int ObDASBMMOp::do_close()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  for (int64_t i = 0; i < merge_iters_.count(); ++i) {
    if (nullptr != merge_iters_[i]) {
      merge_iters_[i]->~ObDAATMergeIter();
      if (nullptr != iter_allocator_) {
        iter_allocator_->free(merge_iters_[i]);
      }
      merge_iters_[i] = nullptr;
    }
  }
  merge_iters_.reset();
  score_op_probe_allowed_.reset();
  sorted_iter_idxes_.reset();
  vec_sorted_iter_idxes_.reset();
  vec_range_max_scores_.reset();
  non_ess_block_max_score_suffix_sums_.reset();
  dim_max_scores_.reset();
  if (nullptr != row_merger_) {
    row_merger_->~ObIDisjunctiveDAATMerger();
    if (nullptr != iter_allocator_) {
      iter_allocator_->free(row_merger_);
    }
    row_merger_ = nullptr;
  }
  filter_op_ = nullptr;
  doc_score_iter_.reset();
  doc_score_accumulator_.reset();
  if (nullptr != filter_bitset_ && nullptr != iter_allocator_) {
    iter_allocator_->free(filter_bitset_);
    filter_bitset_ = nullptr;
  }
  sparse_candidate_buffer_.reset();
  filter_merge_mode_ = FilterMergeMode::DENSE_WINDOW;
  filter_mode_ = ObDASBMMBuildOptions::FilterMode::ORDERED;
  query_optional_ = false;
  filter_end_ = false;
  optional_scoring_end_ = false;
  filter_after_score_ = false;
  use_active_candidate_probe_ = false;
  iter_allocator_ = nullptr;
  return ret;
}

int ObDASBMMOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (enable_vec_merge_) {
    if (OB_FAIL(inner_get_next_row_vectorized(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row vectorized");
    }
  } else if (0 == min_competitive_score_) {
    if (OB_FAIL(inner_get_next_row_plain(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row plain");
    }
  } else if (OB_FAIL(inner_get_next_row_with_pruning(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row with pruning");
  }
  return ret;
}

int ObDASBMMOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  return OB_NOT_SUPPORTED;
}

int ObDASBMMOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (enable_vec_merge_) {
    if (OB_FAIL(inner_advance_to_vectorized(target, curr_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to vectorized", K(ret), K(target));
    }
  } else {
    if (0 == min_competitive_score_ && query_optional_) {
      // otherwise, merge iters must be involved, which are advanced below
      ObDASRowID temp_id;
      if (OB_FAIL(advance_filter_to(target, curr_filter_id_))) {
        LOG_WARN("failed to advance filter op", K(ret), K(target));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
      if (OB_ISNULL(merge_iters_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null merge iter", K(ret), K(i));
      } else if (merge_iters_[i]->iter_end()) {
        // skip
      } else if (OB_FAIL(merge_iters_[i]->advance_to(target))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance to", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(row_merger_->reuse_with_selected_iters(
        sorted_iter_idxes_, false, non_essential_dim_count_, sorted_iter_idxes_.count() - 1))) {
      LOG_WARN("failed to rebuild with selected iters", K(ret));
    } else if (OB_FAIL(do_next_rowid(curr_id, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rowid", K(ret));
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    status_ = SearchStatus::FINISHED;
  }
  return ret;
}

int ObDASBMMOp::inner_advance_to_vectorized(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (is_filter_driven_optional()) {
    if (OB_FAIL(inner_advance_to_vectorized_filter_driven(target, curr_id, score))) {
      LOG_WARN_IGNORE_ITER_END(
          ret, "failed to advance filter-driven vectorized bmm", K(ret), K(target));
    }
  } else if (OB_FAIL(inner_advance_to_vectorized_scoring_driven(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(
        ret, "failed to advance scoring-driven vectorized bmm", K(ret), K(target));
  }
  return ret;
}

int ObDASBMMOp::inner_advance_to_vectorized_scoring_driven(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  bool got_result = false;

  // Fast path: the current window still has unconsumed docs.
  if (FilterMergeMode::SPARSE_SEEK == filter_merge_mode_ 
      && sparse_candidate_buffer_.has_unconsumed()) {
    // Skip candidates before target, then emit the first surviving candidate.
    const uint64_t target_u64 = target.get_uint64();
    uint64_t tmp_id = 0;
    if (OB_FAIL(sparse_candidate_buffer_.advance_to(
        target_u64, min_competitive_score_, tmp_id, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance sparse candidate", K(ret), K(target));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      curr_id.set_uint64(tmp_id);
      got_result = true;
    }
  } else if (!doc_score_iter_.iter_end()) {
    uint64_t tmp_id = 0;
    double tmp_score = 0.0;
    if (OB_FAIL(doc_score_iter_.advance(target.get_uint64(), tmp_id, tmp_score, min_competitive_score_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance doc score iter", K(ret), K(target));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      curr_id.set_uint64(tmp_id);
      score = tmp_score;
      got_result = true;
    }
  }

  if (OB_SUCC(ret) && !got_result) {
    if (OB_FAIL(rebuild_vectorized_window_at(target))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild vectorized window", K(ret), K(target));
    } else if (OB_FAIL(do_next_rowid(curr_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid after window rebuild", K(ret));
    }
  }
  return ret;
}

int ObDASBMMOp::rebuild_vectorized_window_at(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  doc_score_iter_.reset();
  doc_score_accumulator_.clear_all();
  vec_boundary_id_.set_min();
  cap_next_batch_to_filter_id_ = false;
  sparse_candidate_buffer_.reuse();

  if (is_filter_driven_optional()) {
    if (filter_end_) {
      ret = OB_ITER_END;
    } else if (curr_filter_id_.is_min()) {
      if (OB_FAIL(advance_filter_to(target, curr_filter_id_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to initialize filter at target", K(ret), K(target));
      }
    } else if (OB_FAIL(compare_rowid(curr_filter_id_, target, cmp_ret))) {
      LOG_WARN("failed to compare filter id with target", K(ret), K_(curr_filter_id), K(target));
    } else if (cmp_ret < 0 && OB_FAIL(advance_filter_to(target, curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to target", K(ret), K(target));
    }
    if (OB_SUCC(ret)) {
      status_ = SearchStatus::COMPUTE_VEC_BOUNDARY;
    }
  } else {
    // Sort lazily after async token children have opened their segment posting iterators.
    if (!iters_sorted_ && OB_FAIL(sort_iters_by_max_score())) {
      LOG_WARN("failed to sort iters for vectorized merge", K(ret));
    } else if (OB_FAIL(compute_vec_boundary_at(target))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to compute vec boundary at target", K(ret), K(target));
    } else {
      status_ = (FilterMergeMode::SPARSE_SEEK == filter_merge_mode_)
          ? SearchStatus::EVALUATE_VEC_LEAP_FROG
          : SearchStatus::EVALUATE_VEC_ESSENTIAL;
    }
  }
  return ret;
}

int ObDASBMMOp::inner_advance_to_vectorized_filter_driven(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  bool got_result = false;
  if (OB_UNLIKELY(!is_filter_driven_optional() || !has_filter())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid query-optional vectorized bmm", K(ret), K_(query_optional),
        K_(min_competitive_score), KP_(filter_op));
  } else if (target.is_max()) {
    doc_score_iter_.reset();
    doc_score_accumulator_.clear_all();
    filter_end_ = true;
    ret = OB_ITER_END;
  } else if (!doc_score_iter_.iter_end()) {
    uint64_t tmp_id = 0;
    double tmp_score = 0.0;
    if (OB_FAIL(doc_score_iter_.advance(
            target.get_uint64(), tmp_id, tmp_score, min_competitive_score_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance optional doc score iter", K(ret), K(target));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      curr_id.set_uint64(tmp_id);
      score = tmp_score;
      got_result = true;
    }
  }

  if (OB_SUCC(ret) && !got_result) {
    if (OB_FAIL(rebuild_vectorized_window_at(target))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild vectorized window", K(ret), K(target));
    } else if (OB_FAIL(inner_get_next_row_vectorized(curr_id, score))) {
      LOG_WARN_IGNORE_ITER_END(
          ret, "failed to get filter-driven row after window rebuild", K(ret), K(target));
    }
  }
  return ret;
}

int ObDASBMMOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to set min competitive score", K(ret));
  } else if (OB_UNLIKELY(SearchStatus::FINISHED == status_)) {
    // skip
  } else if (enable_vec_merge_) {
    // Retain the materialized window. DocScoreIter applies the latest threshold
    // to its remaining rows, and the next batch selects the new driver mode.
  } else if (!iters_sorted_ && OB_FAIL(sort_iters_by_max_score())) {
    LOG_WARN("failed to sort iters by max score", K(ret));
  } else if (OB_FAIL(try_update_essential_dims())) {
    LOG_WARN("failed to try update essential dims", K(ret));
  }
  return ret;
}

int ObDASBMMOp::init_merge_iters(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  merge_iters_.set_allocator(&iter_allocator);
  score_op_costs_.set_allocator(&iter_allocator);
  sorted_iter_idxes_.set_allocator(&iter_allocator);
  dim_max_scores_.set_allocator(&iter_allocator);
  const int64_t score_op_cnt = op_param.get_score_ops()->count();
  if (OB_FAIL(merge_iters_.init(score_op_cnt))) {
    LOG_WARN("failed to init merge iters", K(ret));
  } else if (OB_FAIL(merge_iters_.prepare_allocate(score_op_cnt))) {
    LOG_WARN("failed to prepare allocate merge iters", K(ret));
  } else if (OB_FAIL(sorted_iter_idxes_.init(score_op_cnt))) {
    LOG_WARN("failed to init sorted iter idxes", K(ret));
  } else if (OB_FAIL(sorted_iter_idxes_.prepare_allocate(score_op_cnt))) {
    LOG_WARN("failed to prepare allocate sorted iter idxes", K(ret));
  } else if (OB_FAIL(dim_max_scores_.init(score_op_cnt))) {
    LOG_WARN("failed to init dim max scores", K(ret));
  } else if (OB_FAIL(dim_max_scores_.prepare_allocate(score_op_cnt))) {
    LOG_WARN("failed to prepare allocate dim max scores", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < score_op_cnt; ++i) {
      ObDAATMergeIter *merge_iter = OB_NEWx(ObDAATMergeIter, &iter_allocator);
      const int64_t iter_idx = i;
      if (OB_ISNULL(merge_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(iter_idx, *op_param.get_score_ops()->at(i)))) {
        LOG_WARN("failed to init merge iter", K(ret));
      } else {
        merge_iters_[i] = merge_iter;
        sorted_iter_idxes_[i] = iter_idx;
      }
    }
  }
  return ret;
}

int ObDASBMMOp::init_vec_merge_on_demand(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  const ObDASBMMBuildOptions *options = op_param.get_build_options();
  bool enable_vec_merge = false;
  if (OB_NOT_NULL(options)) {
    enable_vec_merge = options->enable_vectorized_merge_;
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    enable_vec_merge = tenant_config.is_valid() && tenant_config->_enable_bmm_vectorized_merge;
  }
  ObIDASSearchOp *filter_op = op_param.get_filter_op();
  filter_mode_ = OB_NOT_NULL(options) ? options->filter_mode_
      : ObDASBMMBuildOptions::FilterMode::ORDERED;
  const bool has_direct_filter = ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN == filter_mode_;
  // Multiple filters are represented by a conjunction. Until conjunction provides native
  // batch draining, its row-by-row fallback can regress a filter-driven optional query.
  const bool can_vectorize_optional_filter =
      op_param.has_filter_op()
      && OB_NOT_NULL(filter_op)
      && DAS_SEARCH_OP_CONJUNCTION != filter_op->get_op_type()
      && filter_op->can_enumerate_rowids();
  // Only uint64_t rowids are supported. Query-optional vectorization additionally
  // requires an enumerable non-conjunction filter because it owns the complete result domain.
  enable_vec_merge_ = enable_vec_merge
    && get_rowid_type() == DAS_ROWID_TYPE_UINT64
    && (!op_param.query_optional() || can_vectorize_optional_filter)
    && !search_ctx_.is_logical_table();

  // The dense accumulator capacity follows the SQL execution max batch size.
  // Filter-before-score keeps the original 4K window. Filter-after-score uses a
  // 16K window so one candidate window aligns with the minimum BMM boundary.
  static constexpr int64_t VEC_WINDOW_BATCH_MULTIPLE = 16;
  static constexpr int64_t FILTER_AFTER_SCORE_WINDOW_BATCH_MULTIPLE = 64;

  if (has_direct_filter) {
    // The BMM build decision fixes the consumption protocol before creating the
    // filter. Later cost estimates cannot turn a drain-only input into a seeker.
    filter_merge_mode_ = FilterMergeMode::DENSE_WINDOW;
    filter_after_score_ = false;
  } else if (enable_vec_merge_) {
    select_filter_strategy(op_param.get_lead_cost(), op_param.get_filter_cost(),
        search_ctx_.get_row_count(), op_param.has_filter_op(), op_param.query_optional(),
        filter_merge_mode_, filter_after_score_);
  }
  if (has_direct_filter && (!enable_vec_merge_ || op_param.query_optional()
      || OB_ISNULL(filter_op) || !filter_op->supports_direct_rowid_drain())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct BMM filter requires filter-before-score dense windows", K(ret),
        K(op_param.query_optional()), K(get_rowid_type()),
        "is_logical_table", search_ctx_.is_logical_table(),
        K_(filter_merge_mode), K_(filter_after_score));
  } else if (!enable_vec_merge_) {
    // skip
  } else {
    const int64_t iter_cnt = merge_iters_.count();
    const ObDASSearchCost &table_row_count = search_ctx_.get_row_count();
    const ObDASSearchCost filter_cost = op_param.get_filter_cost();
    // Sparse non-essential candidates also occur without a scalar filter. Keep
    // dense batches on the drain path; the active-count limit is checked per batch.
    const bool can_probe_without_filter = !op_param.has_filter_op() && !op_param.query_optional();
    use_active_candidate_probe_ = can_probe_without_filter || (filter_after_score_
        && table_row_count.cost() >= BMM_ACTIVE_PROBE_FILTER_COST_DENOMINATOR
        && filter_cost.cost() <= table_row_count.cost() / BMM_ACTIVE_PROBE_FILTER_COST_DENOMINATOR);

    non_ess_block_max_score_suffix_sums_.set_allocator(&iter_allocator);
    vec_sorted_iter_idxes_.set_allocator(&iter_allocator);
    vec_range_max_scores_.set_allocator(&iter_allocator);
    score_op_probe_allowed_.set_allocator(&iter_allocator);
    if (OB_FAIL(non_ess_block_max_score_suffix_sums_.prepare_allocate(iter_cnt))) {
      LOG_WARN("failed to prepare non essential block max score suffix sums", K(ret));
    } else if (OB_FAIL(vec_sorted_iter_idxes_.prepare_allocate(iter_cnt))) {
      LOG_WARN("failed to prepare vectorized iterator order", K(ret));
    } else if (OB_FAIL(vec_range_max_scores_.prepare_allocate(iter_cnt))) {
      LOG_WARN("failed to prepare vectorized range max scores", K(ret));
    } else if (OB_FAIL(score_op_probe_allowed_.prepare_allocate(iter_cnt))) {
      LOG_WARN("failed to prepare score op probe gates", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < iter_cnt; ++i) {
        non_ess_block_max_score_suffix_sums_.at(i) = 0.0;
        vec_sorted_iter_idxes_.at(i) = i;
        vec_range_max_scores_.at(i) = 0.0;
        score_op_probe_allowed_.at(i) = false;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (op_param.query_optional()
        || FilterMergeMode::DENSE_WINDOW == filter_merge_mode_) {
      const int64_t window_batch_multiple = filter_after_score_
          ? FILTER_AFTER_SCORE_WINDOW_BATCH_MULTIPLE
          : VEC_WINDOW_BATCH_MULTIPLE;
      if (OB_FAIL(doc_score_accumulator_.init(
          search_ctx_.max_batch_size() * window_batch_multiple, iter_allocator))) {
        LOG_WARN("failed to init doc score accumulator", K(ret));
      }

      // Pre-allocate the filter bitmask for every path that may execute a dense window.
      if (OB_SUCC(ret) && op_param.has_filter_op()) {
        const int64_t capacity = doc_score_accumulator_.get_capacity();
        const int64_t mem_size = ObBitVector::memory_size(capacity);
        void *buf = nullptr;
        if (OB_ISNULL(buf = iter_allocator.alloc(mem_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate filter bitset", K(ret), K(capacity), K(mem_size));
        } else {
          filter_bitset_ = to_bit_vector(buf);
          filter_bitset_->init(capacity);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FilterMergeMode::SPARSE_SEEK == filter_merge_mode_) {
      const int64_t cand_cap = ObDAATMergeIter::DRAIN_BATCH_SIZE;
      if (OB_FAIL(sparse_candidate_buffer_.init(cand_cap, iter_allocator))) {
        LOG_WARN("failed to init sparse candidate buffer", K(ret), K(cand_cap));
      }
    }

    if (OB_SUCC(ret)) {
      vec_boundary_id_.set_min();
    }
  }
  LOG_TRACE("[BMM VEC MERGE] init on demand", K(enable_vec_merge_), K_(filter_merge_mode),
      K_(filter_after_score), K_(use_active_candidate_probe),
      "table_row_count", search_ctx_.get_row_count(), K_(lead_cost), K_(filter_cost), K(op_param));

  return ret;
}

int ObDASBMMOp::inner_get_next_row_plain(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t dim_cnt = 0;
  if (has_filter()) {
    if (OB_FAIL(get_next_row_plain_with_filter(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row plain with filter", K(ret));
    }
  } else if (OB_FAIL(row_merger_->rebuild_with_next())) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with next", K(ret));
  } else if (OB_FAIL(row_merger_->merge_one_row(next_id, score, dim_cnt))) {
    LOG_WARN("failed to merge one row", K(ret));
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_row_with_pruning(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(SearchStatus::FINISHED == status_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(SearchStatus::FIND_NEXT_PIVOT != status_
      || !iters_sorted_
      || 0 == min_competitive_score_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status for pruning", K(ret), K_(status), K(iters_sorted_), K(min_competitive_score_));
  }

  bool found_row = false;
  ObDASRowID pivot_id;
  double non_essential_block_max_score = 0.0;
  while (OB_SUCC(ret) && !found_row && SearchStatus::FINISHED != status_) {
    switch (status_) {
    case SearchStatus::FIND_NEXT_PIVOT: {
      double max_score = 0.0;
      if (has_filter()) {
        if (OB_FAIL(find_next_pivot_with_filter(pivot_id))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to find next pivot with filter", K(ret));
        }
      } else if (OB_FAIL(row_merger_->rebuild_with_next())) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with next", K(ret));
      } else if (OB_FAIL(row_merger_->pop_until_found_pivot(0, 1, pivot_id, max_score))) {
        LOG_WARN("failed to pop until found pivot", K(ret));
      }
      if (OB_SUCC(ret)) {
        status_ = SearchStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate = false;
      non_essential_block_max_score = 0.0;
      if (OB_FAIL(evaluate_pivot_range(pivot_id, non_essential_block_max_score, is_candidate))) {
        LOG_WARN("failed to evaluate pivot range", K(ret));
      } else if (is_candidate) {
        status_ = SearchStatus::EVALUATE_PIVOT;
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT: {
      if (OB_FAIL(evaluate_bmm_pivot(pivot_id, non_essential_block_max_score, next_id, score, found_row))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to evaluate bmm pivot", K(ret));
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case SearchStatus::FIND_NEXT_PIVOT_RANGE: {
      if (OB_FAIL(row_merger_->to_next_range_with_shallow_iters(get_essential_dim_threshold()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to to next range with shallow iters", K(ret));
        }
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(ret), K_(status));
    }
    }
  }

  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    status_ = SearchStatus::FINISHED;
  }
  return ret;
}


int ObDASBMMOp::sort_iters_by_max_score()
{
  int ret = OB_SUCCESS;

  class DimMaxScoreCmp
  {
  public:
    explicit DimMaxScoreCmp(const ObIArray<double> &dim_max_scores)
      : dim_max_scores_(dim_max_scores)
    {}
    bool operator()(const int64_t lhs, const int64_t rhs) const
    {
      return dim_max_scores_.at(lhs) < dim_max_scores_.at(rhs);
    }
  private:
    const ObIArray<double> &dim_max_scores_;
  };

  // Collect max scores before sorting so the comparator cannot encounter scan errors.
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
    if (OB_FAIL(merge_iters_.at(i)->get_max_score(dim_max_scores_.at(i)))) {
      LOG_WARN("failed to get dim max score", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    // Child score estimation is complete. Cache cost gates once per scan,
    // using the current tablet row count, before processing candidate windows.
    if (enable_vec_merge_) {
      const ObDASSearchCost &table_row_count = search_ctx_.get_row_count();
      for (int64_t i = 0; i < merge_iters_.count(); ++i) {
        score_op_probe_allowed_.at(i) = use_active_candidate_probe_ && (filter_after_score_
            || (i < score_op_costs_.count()
                && score_op_costs_.at(i).is_valid()
                && table_row_count.is_valid()
                && table_row_count.cost() >= BMM_ACTIVE_PROBE_POSTING_COST_DENOMINATOR
                && score_op_costs_.at(i).cost()
                    >= table_row_count.cost() / BMM_ACTIVE_PROBE_POSTING_COST_DENOMINATOR));
      }
    }
    DimMaxScoreCmp cmp(dim_max_scores_);
    lib::ob_sort(sorted_iter_idxes_.begin(), sorted_iter_idxes_.end(), cmp);
    iters_sorted_ = true;
  }
  return ret;
}

int ObDASBMMOp::try_update_essential_dims()
{
  int ret = OB_SUCCESS;

  // Notice that non_essential_dim_max_score could decrease with non-essential dims iter end
  double curr_non_ess_max_score = 0;
  for (int64_t i = 0;
      OB_SUCC(ret) && i < sorted_iter_idxes_.count() && non_essential_dim_threshold_ <= min_competitive_score_;
      ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    double score = 0.0;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->get_max_score(score))) {
      LOG_WARN("failed to get curr score", K(ret));
    } else {
      const double next_non_ess_max_score = curr_non_ess_max_score + score;
      if (next_non_ess_max_score > min_competitive_score_) {
        non_essential_dim_count_ = (i - 1) >= 0 ? i : 0;
        non_essential_dim_max_score_ = curr_non_ess_max_score;
        non_essential_dim_threshold_ = next_non_ess_max_score;
      } else {
        curr_non_ess_max_score = next_non_ess_max_score;
      }
    }
  }

  if (FAILEDx(row_merger_->reuse_with_selected_iters(
      sorted_iter_idxes_, true, non_essential_dim_count_, sorted_iter_idxes_.count() - 1))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to reuse with selected iters", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  LOG_DEBUG("[Sparse Retrieval] try update essential dims", K(ret), K(non_essential_dim_count_), K(non_essential_dim_max_score_),
      K(non_essential_dim_threshold_), K(min_competitive_score_), K(sorted_iter_idxes_));
  return ret;
}

int ObDASBMMOp::evaluate_pivot_range(
    const ObDASRowID &pivot_id,
    double &non_essential_block_max_score,
    bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  non_essential_block_max_score = 0.0;
  double essential_block_max_score = 0.0;
  if (OB_FAIL(row_merger_->advance_for_block_max_score(pivot_id, essential_block_max_score))) {
    LOG_WARN("failed to advance for block max score", K(ret));
  } else if (essential_block_max_score <= get_essential_dim_threshold()) {
    is_candidate = false;
  } else {
    const MaxScoreTuple *max_score_tuple = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
      const int64_t iter_idx = sorted_iter_idxes_[i];
      ObDAATMergeIter *iter = merge_iters_[iter_idx];
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
      } else if (iter->iter_end()) {
        // skip
      } else if (OB_FAIL(iter->advance_shallow(pivot_id, true))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
        LOG_WARN("failed to get curr block max info", K(ret), K(iter_idx));
      } else {
        non_essential_block_max_score += max_score_tuple->get_max_score();
      }
    }
    if (OB_SUCC(ret)) {
      // Essential range skipping does not track non-essential block boundaries.
      is_candidate = true;
    }
  }
  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot range", K(ret), K(pivot_id), K(is_candidate),
      K(non_essential_block_max_score), K(essential_block_max_score), K(get_essential_dim_threshold()),
      K_(non_essential_dim_count), K_(min_competitive_score));
  return ret;
}

int ObDASBMMOp::evaluate_essential_pivot(
    const ObDASRowID &pivot_id,
    const double non_essential_block_max_score,
    ObDASRowID &collected_id,
    double &essential_score,
    bool &is_candidate,
    bool &is_valid_pivot)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  is_valid_pivot = false;
  int cmp_ret = 0;
  int64_t dim_cnt = 0;
  if (OB_FAIL(row_merger_->rebuild_with_advance(pivot_id))) {
    LOG_WARN("failed to rebuild with advance", K(ret));
  } else if (OB_FAIL(row_merger_->get_top_id(collected_id))) {
    LOG_WARN("failed to get top id", K(ret));
  } else if (OB_FAIL(compare_rowid(collected_id, pivot_id, cmp_ret))) {
    LOG_WARN("failed to compare rowid", K(ret));
  } else {
    is_valid_pivot = 0 == cmp_ret;
    if (!is_valid_pivot) {
      // A pivot selected from a shallow iterator may only be an inexact block lower bound.
    } else if (OB_FAIL(row_merger_->merge_one_row(collected_id, essential_score, dim_cnt))) {
      LOG_WARN("failed to merge one row", K(ret));
    } else {
      is_candidate = non_essential_block_max_score + essential_score > min_competitive_score_;
    }
  }
  LOG_DEBUG("[Sparse Retrieval] eval essential pivot", K(ret), K(pivot_id), K(collected_id),
        K(non_essential_block_max_score), K(essential_score), K_(min_competitive_score),
        K(is_candidate), K(is_valid_pivot));
  return ret;
}

int ObDASBMMOp::evaluate_pivot(const ObDASRowID &pivot_id, const double &essential_score, double &score)
{
  int ret = OB_SUCCESS;
  double non_essential_score = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    ObDASRowID curr_id;
    int cmp_ret = 0;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->advance_to_id(pivot_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to pivot id", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_id(curr_id))) {
      LOG_WARN("failed to get curr id", K(ret), K(iter_idx));
    } else if (OB_FAIL(compare_rowid(curr_id, pivot_id, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret));
    } else if (0 != cmp_ret) {
      // skip
    } else if (OB_FAIL(iter->load_curr_score(score))) {
      LOG_WARN("failed to load curr score", K(ret), K(iter_idx));
    } else {
      non_essential_score += score;
    }
  }

  if (OB_SUCC(ret)) {
    score = non_essential_score + essential_score;
  }

  LOG_DEBUG("[Sparse Retrieval] eval pivot", K(ret), K(pivot_id), K(essential_score),
      K(non_essential_score), K(score));
  return ret;
}

int ObDASBMMOp::next_filter_rowid(ObDASRowID &next_id)
{
  int ret = OB_SUCCESS;
  double filter_score = 0.0;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(filter_op_->next_rowid(next_id, filter_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rowid", K(ret));
    } else {
      filter_end_ = true;
    }
  }
  return ret;
}

int ObDASBMMOp::advance_filter_to(const ObDASRowID &target, ObDASRowID &curr_id)
{
  int ret = OB_SUCCESS;
  double filter_score = 0.0;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(filter_op_->advance_to(target, curr_id, filter_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to target", K(ret));
    } else {
      filter_end_ = true;
    }
  }
  return ret;
}

int ObDASBMMOp::get_next_row_plain_with_filter(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else {
    bool found_row = false;
    int64_t dim_cnt = 0;
    bool project_filter_id = false;
    if (curr_filter_id_.is_min() && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid from filter", K(ret));
    } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
      LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
    } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to rebuild with advance", K(ret), K_(curr_filter_id));
      } else if (query_optional_) {
        ret = OB_SUCCESS;
        project_filter_id = true;
      }
    }

    ObDASRowID top_id;
    while(OB_SUCC(ret) && !found_row && !project_filter_id) {
      int cmp_ret = 0;
      if (OB_FAIL(row_merger_->get_top_id(top_id))) {
        LOG_WARN("failed to get top id", K(ret));
      } else if (OB_FAIL(compare_rowid(top_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret), K(top_id), K_(curr_filter_id));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected top id smaller than curr_filter_id", K(ret), K(top_id), K(curr_filter_id_));
      } else if (cmp_ret == 0) {
        // filter aligned with next row
      } else if (query_optional_) {
        project_filter_id = true;
      } else if (OB_FAIL(advance_filter_to(top_id, curr_filter_id_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to top id", K(ret), K(top_id));
      } else if (OB_FAIL(compare_rowid(top_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret), K(top_id), K_(curr_filter_id));
      }

      if (OB_FAIL(ret)) {
      } else if (project_filter_id) {
        // skip
      } else if (cmp_ret != 0) {
        if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
          LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
        } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
        }
      } else if (OB_FAIL(row_merger_->merge_one_row(next_id, score, dim_cnt))) {
        LOG_WARN("failed to merge one row", K(ret));
      } else {
        found_row = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (project_filter_id) {
        next_id = curr_filter_id_;
        score = 0.0;
      }

      if (!found_row && !project_filter_id) {
        // only move filter op forward when filter id is evaluated
      } else if (OB_FAIL(next_filter_rowid(curr_filter_id_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next rowid", K(ret));
        } else {
          // return last row
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObDASBMMOp::find_next_pivot_with_filter(ObDASRowID &pivot_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(curr_filter_id_.is_min()) && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid from filter", K(ret));
  } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
    LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
  } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
  }
  bool found_matched_pivot = false;
  double max_score = 0.0;
  while (OB_SUCC(ret) && !found_matched_pivot) {
    int cmp_ret = 0;
    if (OB_FAIL(row_merger_->pop_until_found_pivot(0, 1, pivot_id, max_score))) {
      LOG_WARN("failed to pop until found pivot", K(ret), K(max_score));
    } else if (OB_FAIL(compare_rowid(pivot_id, curr_filter_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pivot id smaller than curr_filter_id", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (0 == cmp_ret) {
      found_matched_pivot = true;
    } else if (OB_FAIL(advance_filter_to(pivot_id, curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to pivot id", K(ret), K(pivot_id));
    } else if (OB_FAIL(compare_rowid(pivot_id, curr_filter_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (0 == cmp_ret) {
      found_matched_pivot = true;
    } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
      LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
    } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
    }
  }
  return ret;
}

int ObDASBMMOp::evaluate_bmm_pivot(
    const ObDASRowID &pivot_id,
    const double &non_essential_bm_score,
    ObDASRowID &next_id,
    double &score,
    bool &found_row)
{
  int ret = OB_SUCCESS;
  ObDASRowID collected_id;
  double essential_score = 0.0;
  bool is_candidate = false;
  bool is_valid_pivot = false;
  bool need_forward_filter_after_evaluate = false;
  if (OB_FAIL(evaluate_essential_pivot(
      pivot_id, non_essential_bm_score, collected_id, essential_score, is_candidate, is_valid_pivot))) {
    LOG_WARN("failed to evaluate essential pivot", K(ret));
  } else if (!is_valid_pivot) {
    // Retry with the next pivot without advancing the filter for an inexact block pivot.
  } else if (is_candidate) {
    bool filter_match = true;
    if (has_filter()) {
      int cmp_ret = 0;
      if (OB_FAIL(compare_rowid(collected_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collected id smaller than curr_filter_id", K(ret),
            K(pivot_id), K(collected_id), K_(curr_filter_id));
      } else if (0 == cmp_ret) {
        filter_match = true;
      } else {
        // collected filter id not match, advance filter op to collected id
        filter_match = false;
        if (OB_FAIL(advance_filter_to(collected_id, curr_filter_id_))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to collected id", K(ret), K(collected_id));
        } else if (OB_FAIL(compare_rowid(collected_id, curr_filter_id_, cmp_ret))) {
          LOG_WARN("failed to compare rowid", K(ret), K(collected_id), K_(curr_filter_id));
        } else if (0 == cmp_ret) {
          filter_match = true;
        }
      }
    }

    if (OB_FAIL(ret) || !filter_match) {
    } else if (OB_FAIL(evaluate_pivot(collected_id, essential_score, score))) {
      LOG_WARN("failed to evaluate pivot", K(ret));
    } else if (FALSE_IT(need_forward_filter_after_evaluate = has_filter())) {
      LOG_WARN("failed to get next rowid from filter", K(ret));
    } else if (score > min_competitive_score_) {
      found_row = true;
      next_id = collected_id;
    }
  } else {
    need_forward_filter_after_evaluate = has_filter();
  }

  if (OB_FAIL(ret) || !need_forward_filter_after_evaluate) {
  } else if (OB_FAIL(next_filter_rowid(curr_filter_id_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rowid from filter", K(ret));
    } else if (found_row) {
      // return last matched pivot
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_row_vectorized(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_id = 0;
  bool got_row = false;
  // SPARSE_SEEK output is served from the compact candidate buffer; DENSE_WINDOW output from the
  // dense doc_score_iter_ window. A query-optional dense window remains active until its
  // iterator is exhausted, even if a positive threshold has selected sparse scoring.
  bool need_convert_to_scoring_sparse = FilterMergeMode::SPARSE_SEEK == filter_merge_mode_
                                       && !is_filter_driven_optional(); 
  bool need_next_batch = !sparse_candidate_buffer_.has_unconsumed() && doc_score_iter_.iter_end();
  if (OB_UNLIKELY(!enable_vec_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non vectorized merge", K(ret));
  }

  while (OB_SUCC(ret) && !got_row) {
    if (need_next_batch) {
      if (OB_FAIL(inner_get_next_batch())) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to get next batch", K(ret));
      } else if (need_convert_to_scoring_sparse) {
        // Candidate buffer was filled and scored by inner_get_next_batch().
        need_next_batch = false;
      } else if (OB_FAIL(doc_score_iter_.open(
                     doc_score_accumulator_,
                     is_filter_driven_optional()
                         ? DocScoreIter::ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD
                         : DocScoreIter::ProjectionPolicy::STRICTLY_ABOVE_THRESHOLD))) {
        LOG_WARN("failed to open doc score iter", K(ret));
      } else {
        need_next_batch = false;
      }
    } else if (sparse_candidate_buffer_.has_unconsumed()) {
      if (OB_FAIL(sparse_candidate_buffer_.next(
          min_competitive_score_, tmp_id, score))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next sparse candidate", K(ret));
        } else {
          ret = OB_SUCCESS;
          need_next_batch = true;
        }
      } else {
        got_row = true;
        next_id.set_uint64(tmp_id);
      }
    } else if (OB_FAIL(doc_score_iter_.next(tmp_id, score, min_competitive_score_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rowid from doc score iter", K(ret));
      } else {
        ret = OB_SUCCESS;
        doc_score_iter_.reset();
        need_next_batch = true;
      }
    } else {
      got_row = true;
    }
  }

  if (OB_SUCC(ret) && got_row) {
    next_id.set_uint64(tmp_id);
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_batch()
{
  int ret = OB_SUCCESS;
  if (is_filter_driven_optional()) {
    if (OB_FAIL(inner_get_next_batch_filter_driven())) {
      LOG_WARN_IGNORE_ITER_END(
          ret, "failed to get filter-driven next batch", K(ret));
    }
  } else if (OB_FAIL(inner_get_next_batch_scoring_driven())) {
    LOG_WARN_IGNORE_ITER_END(
        ret, "failed to get scoring-driven next batch", K(ret));
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_batch_scoring_driven()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(SearchStatus::FINISHED == status_)) {
    ret = OB_ITER_END;
  }

  bool found_valid_batch = false;
  ObDASRowID batch_start_id;
  ObDASRowID batch_end_id;
  while (OB_SUCC(ret) && !found_valid_batch) {
    switch (status_) {
      case SearchStatus::COMPUTE_VEC_BOUNDARY: {
        // Vectorized path partitions essential dims by block max score per boundary.
        // Keep a stable global max-score order only as the base iteration order.
        // Sort lazily on first use: posting iters are guaranteed open here, while
        // at open/rescan time async token children have not opened them yet.
        if (!iters_sorted_ && OB_FAIL(sort_iters_by_max_score())) {
          LOG_WARN("failed to sort iters for vectorized merge", K(ret));
        } else if (filter_end_) {
          // Filter was exhausted while filtering the previous batch.
          ret = OB_ITER_END;
        } else if (vec_boundary_id_.is_min() && OB_FAIL(row_merger_->rebuild_with_next())) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild merger with next", K(ret));
        } else if (OB_FAIL(compute_vec_boundary())) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to compute vec boundary", K(ret));
        } else if (FilterMergeMode::SPARSE_SEEK == filter_merge_mode_) {
          status_ = SearchStatus::EVALUATE_VEC_LEAP_FROG;
        } else {
          status_ = SearchStatus::EVALUATE_VEC_ESSENTIAL;
        }
        break;
      }
      case SearchStatus::EVALUATE_VEC_ESSENTIAL: {
        ObDASRowID max_batch_id;
        uint64_t max_batch_id_u64 = 0;
        int cmp_ret = 0;
        // Generate a candidate batch window [batch_start_id, batch_end_id].
        if (filter_after_score_ && filter_end_) {
          ret = OB_ITER_END;
        } else if (OB_UNLIKELY(row_merger_->is_empty())) {
          status_ = SearchStatus::COMPUTE_VEC_BOUNDARY;
          break;
        } else if (OB_FAIL(row_merger_->get_top_id(batch_start_id))) {
          LOG_WARN("failed to get batch start id", K(ret));
        } else if (!vec_boundary_id_.is_max()
            && batch_start_id.get_uint64() > vec_boundary_id_.get_uint64()) {
          status_ = SearchStatus::COMPUTE_VEC_BOUNDARY;
          break;
        } else if (OB_FAIL(doc_score_accumulator_.reuse_with_start_id(batch_start_id.get_uint64(), max_batch_id_u64))) {
          LOG_WARN("failed to reuse doc score accumulator", K(ret), K(batch_start_id));
        } else if (FALSE_IT(max_batch_id.set_uint64(max_batch_id_u64))) {
        } else if (OB_FAIL(compare_uint64_rowid(max_batch_id, vec_boundary_id_, cmp_ret))) {
          LOG_WARN("failed to compare rowid", K(ret));
        } else {
          batch_end_id = cmp_ret > 0 ? vec_boundary_id_ : max_batch_id;
        }

        bool need_evaluate_essential_batch = true;
        bool is_essential_batch_valid = true;
        // Filter-after-score skips the input mask and probes only competitive candidates.
        if (OB_SUCC(ret) && !filter_after_score_) {
          if (OB_FAIL(prepare_filter_for_vectorized_batch(
              batch_start_id, batch_end_id, need_evaluate_essential_batch))) {
            LOG_WARN("failed to prepare filter for vectorized batch",
                K(ret), K(batch_start_id), K(batch_end_id));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (!need_evaluate_essential_batch) {
          if (filter_end_) {
            // Current batch is invalid and filter is exhausted, stop iteration.
            ret = OB_ITER_END;
          } else {
            // Current batch has no filter hit. Move score iterators to the next filter id.
            ObDASRowID skip_target = curr_filter_id_;
            cmp_ret = 0;
            if (OB_FAIL(compare_uint64_rowid(skip_target, vec_boundary_id_, cmp_ret))) {
              LOG_WARN("failed to compare skip target with vec boundary", K(ret),
                  K(skip_target), K_(vec_boundary_id));
            } else if (cmp_ret > 0) {
              // Skip target crosses the vector boundary, recompute the boundary.
              if (OB_FAIL(compute_vec_boundary_at(skip_target))) {
                LOG_WARN_IGNORE_ITER_END(ret, "failed to compute vec boundary at target", K(ret), K(skip_target));
              }
            } else {
              // Skip target does not cross the vector boundary, rebuild the merger with advance.
              if (OB_FAIL(row_merger_->pop_until_target(skip_target))) {
                LOG_WARN("failed to drain merger heap before skip", K(ret), K(skip_target));
              } else if (OB_FAIL(row_merger_->rebuild_with_advance(skip_target))) {
                LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K(skip_target));
              }
            }
          }
        } else if (OB_FAIL(evaluate_essential_batch(batch_end_id, is_essential_batch_valid))) {
          LOG_WARN("failed to evaluate essential batch", K(ret));
        } else if (is_essential_batch_valid) {
          // Current batch has surviving essential docs, evaluate the non-essential docs.
          status_ = SearchStatus::EVALUATE_VEC_NON_ESSENTIAL;
        } else {
          // Current batch has no surviving essential docs. Re-compare the batch_end_id 
          // and vec_boundary_id due to cap_next_batch_to_filter_id_ optimization.
          cmp_ret = 0;
          if (OB_FAIL(compare_uint64_rowid(batch_end_id, vec_boundary_id_, cmp_ret))) {
            LOG_WARN("failed to compare batch end with vec boundary", K(ret),
                K(batch_end_id), K_(vec_boundary_id));
          } else if (cmp_ret > 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected batch end crosses vec boundary", K(cmp_ret), K(batch_end_id), K_(vec_boundary_id));
          } else if (cmp_ret == 0) {
            status_ = SearchStatus::COMPUTE_VEC_BOUNDARY;
          } else {
            // Stay in EVALUATE_VEC_ESSENTIAL and compute the next batch.
          }
        }
        break;
      }
      case SearchStatus::EVALUATE_VEC_NON_ESSENTIAL: {
        found_valid_batch = true;
        if (OB_FAIL(evaluate_non_essential_batch(batch_end_id, found_valid_batch))) {
          LOG_WARN("failed to evaluate non essential batch", K(ret));
        } else if (found_valid_batch && filter_after_score_
            && OB_FAIL(apply_filter_to_scored_batch())) {
          LOG_WARN("failed to apply filter to scored batch", K(ret), K(batch_end_id));
        } else {
          found_valid_batch = found_valid_batch && !doc_score_accumulator_.is_empty();
          doc_score_accumulator_.clear_filter_mask();
          status_ = SearchStatus::EVALUATE_VEC_ESSENTIAL;
        }
        break;
      }
      case SearchStatus::EVALUATE_VEC_LEAP_FROG: {
        if (OB_FAIL(collect_leapfrog_candidates())) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to collect leapfrog candidates", K(ret));
        } else if (OB_FAIL(score_leapfrog_candidates())) {
          LOG_WARN("failed to score leapfrog candidates", K(ret));
        } else {
          found_valid_batch = true;
          // Stay in EVALUATE_VEC_LEAP_FROG so the next refill continues this boundary window.
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status for vectorized merge", K(ret), K_(status));
      }
    }
  }

  if (OB_UNLIKELY(OB_ITER_END == ret)) {
    status_ = SearchStatus::FINISHED;
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_batch_filter_driven()
{
  int ret = OB_SUCCESS;
  int boundary_ret = OB_SUCCESS;
  int cmp_ret = 0;
  bool found_valid_batch = false;
  bool evaluate_scoring = false;
  bool filter_mask_attached = false;
  bool has_scoring_candidate = false;
  uint64_t max_batch_id_u64 = 0;
  ObDASRowID batch_start_id;
  ObDASRowID batch_end_id;
  if (OB_UNLIKELY(!enable_vec_merge_
                  || !is_filter_driven_optional()
                  || !has_filter()
                  || !filter_op_->can_enumerate_rowids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter-driven optional bmm", K(ret), K_(enable_vec_merge),
        K_(query_optional), K_(min_competitive_score), KP_(filter_op));
  } else if (SearchStatus::FINISHED == status_ || filter_end_) {
    ret = OB_ITER_END;
  }

  while (OB_SUCC(ret) && !found_valid_batch) {
    boundary_ret = OB_SUCCESS;
    cmp_ret = 0;
    evaluate_scoring = false;
    filter_mask_attached = false;
    has_scoring_candidate = false;
    max_batch_id_u64 = 0;
    batch_start_id.reset();
    batch_end_id.reset();
    if (curr_filter_id_.is_min() && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to initialize optional filter", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!curr_filter_id_.is_normal())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected optional filter rowid", K(ret), K_(curr_filter_id));
    } else if (FALSE_IT(batch_start_id = curr_filter_id_)) {
    } else if (OB_FAIL(doc_score_accumulator_.reuse_with_start_id(
                   batch_start_id.get_uint64(), max_batch_id_u64))) {
      LOG_WARN("failed to reuse optional doc score accumulator", K(ret), K(batch_start_id));
    } else {
      batch_end_id.set_uint64(max_batch_id_u64);
    }

    if (OB_SUCC(ret) && !optional_scoring_end_) {
      if (!iters_sorted_ && OB_FAIL(sort_iters_by_max_score())) {
        LOG_WARN("failed to sort optional scoring iters", K(ret));
      } else {
        boundary_ret = compute_vec_boundary_at(batch_start_id);
        if (OB_ITER_END == boundary_ret) {
          // Scoring exhaustion/non-competitiveness only changes subsequent scores;
          // filter remains the root driver and may still produce zero-score rows.
          optional_scoring_end_ = true;
        } else if (OB_SUCCESS != boundary_ret) {
          ret = boundary_ret;
          LOG_WARN("failed to compute optional scoring boundary", K(ret), K(batch_start_id));
        } else {
          if (OB_FAIL(compare_rowid(batch_end_id, vec_boundary_id_, cmp_ret))) {
            LOG_WARN("failed to compare optional batch boundary", K(ret),
                K(batch_end_id), K_(vec_boundary_id));
          } else {
            if (cmp_ret > 0) {
              batch_end_id = vec_boundary_id_;
            }
            evaluate_scoring = true;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_filter_bitset(batch_start_id, batch_end_id))) {
      LOG_WARN("failed to seed optional filter batch", K(ret), K(batch_start_id), K(batch_end_id));
    } else {
      doc_score_accumulator_.set_filter_mask(filter_bitset_);
      filter_mask_attached = true;
    }

    if (OB_SUCC(ret) && evaluate_scoring) {
      if (OB_FAIL(evaluate_essential_batch(batch_end_id, has_scoring_candidate))) {
        LOG_WARN("failed to evaluate optional essential batch", K(ret), K(batch_end_id));
      } else if (OB_FAIL(evaluate_non_essential_batch(
                     batch_end_id, has_scoring_candidate))) {
        LOG_WARN("failed to evaluate optional non-essential batch", K(ret),
            K(batch_start_id), K(batch_end_id));
      }
    }

    if (filter_mask_attached) {
      doc_score_accumulator_.clear_filter_mask();
    }
    if (OB_SUCC(ret) && min_competitive_score_ > 0.0
        && OB_FAIL(doc_score_accumulator_.filter_below_score(min_competitive_score_))) {
      LOG_WARN("failed to filter optional batch by competitive score",
          K(ret), K_(min_competitive_score));
    }
    if (OB_SUCC(ret)) {
      found_valid_batch = !doc_score_accumulator_.is_empty();
      if (!found_valid_batch && filter_end_) {
        ret = OB_ITER_END;
      }
    }
  }

  if (OB_ITER_END == ret) {
    status_ = SearchStatus::FINISHED;
  } else if (OB_SUCC(ret)) {
    // A positive threshold may switch drivers after this batch. Resume the
    // current window: a filter-driven batch need not consume its entire boundary.
    status_ = FilterMergeMode::SPARSE_SEEK == filter_merge_mode_
        ? SearchStatus::EVALUATE_VEC_LEAP_FROG : SearchStatus::EVALUATE_VEC_ESSENTIAL;
  }
  return ret;
}

// Called only after all candidates from the old window have been scored.
int ObDASBMMOp::find_vec_handoff(const ObDASRowID &lower, ObDASRowID &next)
{
  int ret = OB_SUCCESS;
  // No earlier candidate is possible once the successor reaches the lower bound.
  if (next.is_normal() && next.get_uint64() == lower.get_uint64()) {
    // The successor is already at the earliest possible window start.
  } else {
    const int64_t dim_cnt = sorted_iter_idxes_.count();
    // Fill every scratch slot once; only old non-essential dimensions need coverage.
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
      const int64_t idx = vec_sorted_iter_idxes_.at(i);
      double bound = 0.0;
      if (i < vec_non_essential_dim_count_ && !merge_iters_.at(idx)->iter_end()
          && OB_FAIL(merge_iters_.at(idx)->get_max_score(bound))) {
        LOG_WARN("failed to get handoff dimension bound", K(ret), K(idx));
      } else {
        vec_range_max_scores_.at(idx) = bound;
      }
    }
    double remaining = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
      remaining += vec_range_max_scores_.at(sorted_iter_idxes_.at(i));
      non_ess_block_max_score_suffix_sums_.at(i) = remaining;
    }
    // Remove the largest bounds first. Prefix sums avoid repeated subtraction.
    for (int64_t i = dim_cnt - 1; OB_SUCC(ret) && i >= 0
        && non_ess_block_max_score_suffix_sums_.at(i) > min_competitive_score_
        && (next.is_max() || next.get_uint64() > lower.get_uint64()); --i) {
      const int64_t idx = sorted_iter_idxes_.at(i);
      ObDAATMergeIter *iter = merge_iters_.at(idx);
      ObDASRowID curr;
      if (vec_range_max_scores_.at(idx) == 0.0) {
        // The old essential set already covers this dimension, or it is exhausted.
      } else if (OB_FAIL(iter->advance_to_id(lower))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to locate handoff dimension", K(ret), K(idx), K(lower));
        }
      } else if (OB_FAIL(iter->get_curr_id(curr))) {
        LOG_WARN("failed to get handoff rowid", K(ret), K(idx));
      } else if (next.is_max() || curr.get_uint64() < next.get_uint64()) {
        next = curr;
      }
    }
  }
  return ret;
}

// Find a safe window start before rebuilding the partition and merger.
int ObDASBMMOp::find_next_vec_window_start(ObDASRowID &min_id)
{
  int ret = OB_SUCCESS;
  min_id.set_max();
  if (vec_boundary_id_.is_min()) {
    if (OB_UNLIKELY(row_merger_->is_empty())) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(row_merger_->get_top_id(min_id))) {
      LOG_WARN("failed to get initial min id", K(ret));
    }
  } else if (vec_boundary_id_.is_max() || UINT64_MAX == vec_boundary_id_.get_uint64()) {
    ret = OB_ITER_END;
  } else {
    ObDASRowID lower;
    lower.set_uint64(vec_boundary_id_.get_uint64() + 1);
    if (can_seek_filter_for_vec_handoff()) {
      // Only enumerable filters can seek to prove the gap empty. Direct drains
      // use scoring bounds below because they do not expose an ordered cursor.
      if ((curr_filter_id_.is_min() || curr_filter_id_.get_uint64() < lower.get_uint64())
          && OB_FAIL(advance_filter_to(lower, curr_filter_id_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to locate filter after window", K(ret), K(lower));
      } else {
        min_id = curr_filter_id_;
      }
    } else {
      if (!row_merger_->is_empty() && OB_FAIL(row_merger_->get_top_id(min_id))) {
        LOG_WARN("failed to get essential successor", K(ret));
      } else if (vec_non_essential_dim_max_score_sum_ > min_competitive_score_
          && OB_FAIL(find_vec_handoff(lower, min_id))) {
        LOG_WARN("failed to restore candidate coverage", K(ret), K(lower));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (min_id.is_max()
      || (search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value()
          && min_id.get_uint64() > search_ctx_.get_docid_range_hi().get_uint64())) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASBMMOp::compute_vec_boundary()
{
  int ret = OB_SUCCESS;
  ObDASRowID min_id;
  if (OB_FAIL(find_next_vec_window_start(min_id))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to find next vectorized window start", K(ret));
  } else if (OB_FAIL(row_merger_->pop_all())) {
    LOG_WARN("failed to clear row merger", K(ret));
  } else if (OB_FAIL(compute_vec_boundary_at(min_id))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to compute vec boundary at", K(ret), K(min_id));
  }
  return ret;
}

// 将候选窗口扩展到最小向量化范围，并受查询 doc_id 上界约束。
int ObDASBMMOp::expand_vec_boundary(
    const ObDASRowID &candidate_start,
    ObDASRowID &boundary) const
{
  int ret = OB_SUCCESS;
  // MAX 哨兵保持不变，仅对普通右边界计算最小窗口。
  if (!boundary.is_max()) {
    // 按闭区间计算最小窗口右边界，并在 uint64 溢出前饱和到最大值。
    const uint64_t start_id = candidate_start.get_uint64();
    const uint64_t boundary_delta = static_cast<uint64_t>(BMM_VEC_MIN_BOUNDARY_SIZE - 1);
    uint64_t floor_boundary = start_id > UINT64_MAX - boundary_delta
        ? UINT64_MAX : start_id + boundary_delta;
    // 查询指定有限 doc_id 范围时，将扩展结果截断到范围上界。
    if (search_ctx_.has_docid_range() && !search_ctx_.get_docid_range_hi().is_max_value()
        && floor_boundary > search_ctx_.get_docid_range_hi().get_uint64()) {
      floor_boundary = search_ctx_.get_docid_range_hi().get_uint64();
    }
    // 仅扩大原始 boundary，保留已经覆盖更大范围的自然边界。
    if (boundary.get_uint64() < floor_boundary) {
      boundary.set_uint64(floor_boundary);
    }
  }
  return ret;
}

int ObDASBMMOp::repartition_vec_dims_at(
    const ObDASRowID &min_id,
    const bool min_id_inclusive,
    bool &use_all_dims)
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = sorted_iter_idxes_.count();
  use_all_dims = false;
  // 累加所有 token 的 block_max_score，用于保留原有的整段跳过判断。
  double curr_block_max_score_sum = 0.0;
  int64_t iter_end_count = 0;

  // 每个 token 在当前 min_id 只执行一次 shallow advance并累加当前 block 上界。
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    const MaxScoreTuple *max_score_tuple = nullptr;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // 统计已经结束的 token，用于判断全局上界分组是否需要刷新。
      ++iter_end_count;
    } else if (OB_FAIL(iter->advance_shallow(min_id, min_id_inclusive))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance shallow to min id", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
        // 统计本次 shallow advance 后结束的 token。
        ++iter_end_count;
      }
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      LOG_WARN("failed to get curr block max info", K(ret), K(iter_idx));
    } else {
      // 将当前 block_max_score 加入总和。
      curr_block_max_score_sum += max_score_tuple->get_max_score();
    }
  }

  if (OB_SUCC(ret)) {
    // 当前 block 上界总和不超过阈值时，本轮 boundary 临时由全部 token 计算。
    use_all_dims = curr_block_max_score_sum <= min_competitive_score_;
  }

  // token 结束数量或竞争阈值跨过下一分组点时，重新计算稳定的全局上界分组。
  if (OB_SUCC(ret)
      && (iter_end_count != vec_partition_iter_end_count_
          || non_essential_dim_threshold_ <= min_competitive_score_)) {
    double curr_non_ess_max_score = 0.0;
    int64_t new_non_essential_dim_count = 0;
    double new_non_essential_dim_max_score = 0.0;
    double new_non_essential_dim_threshold = 0.0;
    bool partition_found = false;
    // 按全局 dim_max_score 重新划分 non-essential 前缀和 essential 后缀。
    for (int64_t i = 0; OB_SUCC(ret) && !partition_found && i < dim_cnt; ++i) {
      const int64_t iter_idx = sorted_iter_idxes_[i];
      ObDAATMergeIter *iter = merge_iters_[iter_idx];
      double dim_max_score = 0.0;
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
      } else if (iter->iter_end()) {
        // 已结束的 token 不再贡献全局分数上界。
      } else if (OB_FAIL(iter->get_max_score(dim_max_score))) {
        LOG_WARN("failed to get dim max score", K(ret), K(iter_idx));
      } else {
        // 将当前 token 的 dim_max_score 加入 non-essential 前缀和。
        const double next_non_ess_max_score = curr_non_ess_max_score + dim_max_score;
        if (next_non_ess_max_score > min_competitive_score_) {
          // 记录第一个使前缀和超过竞争阈值的 token，该 token 及其后缀归入 essential。
          new_non_essential_dim_count = i > 0 ? i : 0;
          new_non_essential_dim_max_score = curr_non_ess_max_score;
          new_non_essential_dim_threshold = next_non_ess_max_score;
          partition_found = true;
        } else {
          // 当前 token 仍属于 non-essential 前缀，更新前缀和。
          curr_non_ess_max_score = next_non_ess_max_score;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!partition_found) {
        // 所有存活 token 的全局上界之和均未越过阈值，后续阈值增长不再触发相同分组计算。
        new_non_essential_dim_threshold = DBL_MAX;
      }
      // 保存稳定的全局上界分组，供后续 boundary 直接复用。
      non_essential_dim_count_ = new_non_essential_dim_count;
      non_essential_dim_max_score_ = new_non_essential_dim_max_score;
      non_essential_dim_threshold_ = new_non_essential_dim_threshold;
      vec_partition_iter_end_count_ = iter_end_count;
    }
  }
  LOG_TRACE("[BMM VEC MERGE] repartition ess/non-ess dimensions", K(min_id),
    K(use_all_dims), K(non_essential_dim_count_), K(non_essential_dim_max_score_),
    K(non_essential_dim_threshold_), K(vec_partition_iter_end_count_));
  return ret;
}

int ObDASBMMOp::compute_vec_essential_boundary(
    const int64_t non_essential_dim_count,
    ObDASRowID &mini_max_id,
    double &essential_block_max_score)
{
  int ret = OB_SUCCESS;
  bool has_mini_max_id = false;
  const int64_t dim_cnt = sorted_iter_idxes_.count();
  // 使用本轮传入的有效分组数量，避免 use_all_dims 生效时误用缓存的 non_essential_dim_count_。
  const int64_t essential_dim_count = dim_cnt - non_essential_dim_count;
  int64_t iter_end_cnt = 0;
  essential_block_max_score = 0.0;
  // 仅遍历 essential 后缀，累加其当前 block 上界并计算最小 block 结束位置。
  for (int64_t i = non_essential_dim_count; OB_SUCC(ret) && i < dim_cnt; ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    const MaxScoreTuple *max_score_tuple = nullptr;
    int cmp_ret = 0;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      ++iter_end_cnt;
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      LOG_WARN("failed to get curr block max info", K(ret), K(iter_idx));
    } else if (FALSE_IT(essential_block_max_score += max_score_tuple->get_max_score())) {
    } else if (!has_mini_max_id) {
      mini_max_id = max_score_tuple->get_max_id();
      has_mini_max_id = true;
    } else if (OB_FAIL(compare_uint64_rowid(max_score_tuple->get_max_id(), mini_max_id, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(max_score_tuple->get_max_id()), K(mini_max_id));
    } else if (cmp_ret < 0) {
      mini_max_id = max_score_tuple->get_max_id();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (iter_end_cnt == essential_dim_count) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!has_mini_max_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected missing minimum max rowid across all dims", K(ret), K(essential_dim_count),
        K(iter_end_cnt), K(non_essential_dim_count), K(sorted_iter_idxes_));
  }
  return ret;
}

// 使用当前 RowID 区间的分数上界细化向量化维度分组，并输出该区间是否可整体跳过。
int ObDASBMMOp::refine_vec_dims_with_range_max(
    const ObDASRowID &min_id,
    const bool min_id_inclusive,
    const ObDASRowID &boundary,
    bool &skip_range)
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = sorted_iter_idxes_.count();
  // 维度数和竞争阈值满足条件时，为各维度查询当前区间的分数上界。
  const bool try_range_max = dim_cnt >= BMM_RANGE_MAX_MIN_DIM_COUNT
      && min_competitive_score_ > 0.0;
  bool has_range_max = false;
  // 默认保留当前区间并继承全局上界分组，Range Max 生效后再更新为区间分组。
  skip_range = false;
  vec_non_essential_dim_count_ = non_essential_dim_count_;
  vec_non_essential_dim_max_score_ = non_essential_dim_max_score_;

  // 按全局排序遍历所有维度，填充区间上界数组和待排序的迭代器下标。
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
    const int64_t iter_idx = sorted_iter_idxes_.at(i);
    ObDAATMergeIter *iter = merge_iters_.at(iter_idx);
    double max_score = 0.0;
    bool can_calc_range_max = false;
    vec_sorted_iter_idxes_.at(i) = iter_idx;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // 已结束的维度在当前及后续窗口中的分数贡献均为零。
    } else if (try_range_max) {
      // 查询当前维度在指定 RowID 区间内的分数上界。
      if (OB_FAIL(iter->get_range_max_score(
          min_id, min_id_inclusive, boundary, max_score, can_calc_range_max))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get range max score", K(ret), K(iter_idx),
              K(min_id), K(min_id_inclusive), K(boundary));
        } else {
          // 将 end 状态回退到缓存的全局上界。
          ret = OB_SUCCESS;
          if (OB_FAIL(iter->get_max_score(max_score))) {
            LOG_WARN("failed to get fallback dimension max score", K(ret), K(iter_idx));
          }
        }
      } else if (can_calc_range_max) {
        // 标记至少一个维度成功取得 Range Max，后续使用区间上界重新分组。
        has_range_max = true;
      } else {
        // 将活跃维度的不可快速计算状态回退到缓存的全局上界。
        if (OB_FAIL(iter->get_max_score(max_score))) {
          LOG_WARN("failed to get fallback dimension max score", K(ret), K(iter_idx));
        }
      }
    } else if (OB_FAIL(iter->get_max_score(max_score))) {
      LOG_WARN("failed to get dimension max score", K(ret), K(iter_idx));
    }
    if (OB_SUCC(ret)) {
      // 以迭代器下标为槽位，保存本区间实际使用的维度分数上界。
      vec_range_max_scores_.at(iter_idx) = max_score;
    }
  }

  // 至少一个维度取得 Range Max 时，按本区间上界重新排序并细化维度分组。
  if (OB_SUCC(ret) && has_range_max) {
    class RangeMaxScoreCmp
    {
    public:
      explicit RangeMaxScoreCmp(const ObIArray<double> &range_max_scores)
        : range_max_scores_(range_max_scores)
      {}
      bool operator()(const int64_t lhs, const int64_t rhs) const
      {
        const double lhs_score = range_max_scores_.at(lhs);
        const double rhs_score = range_max_scores_.at(rhs);
        return lhs_score < rhs_score || (lhs_score == rhs_score && lhs < rhs);
      }
    private:
      const ObIArray<double> &range_max_scores_;
    } cmp(vec_range_max_scores_);

    // 按分数上界升序排列，上界相同时按迭代器下标保持确定顺序。
    lib::ob_sort(vec_sorted_iter_idxes_.begin(), vec_sorted_iter_idxes_.end(), cmp);
    double non_essential_max_score = 0.0;
    int64_t non_essential_dim_count = 0;
    // 从低上界维度开始累加，构造总上界不超过竞争阈值的 non-essential 前缀。
    while (non_essential_dim_count < dim_cnt) {
      const int64_t iter_idx = vec_sorted_iter_idxes_.at(non_essential_dim_count);
      const double next_max_score =
          non_essential_max_score + vec_range_max_scores_.at(iter_idx);
      if (next_max_score > min_competitive_score_) {
        break;
      }
      non_essential_max_score = next_max_score;
      ++non_essential_dim_count;
    }
    if (non_essential_dim_count == dim_cnt) {
      // 所有维度的区间上界之和均未超过竞争阈值，标记整个区间可跳过。
      skip_range = true;
    } else {
      // 保存当前区间的 non-essential 维度数量及其分数上界总和。
      vec_non_essential_dim_count_ = non_essential_dim_count;
      vec_non_essential_dim_max_score_ = non_essential_max_score;
    }
  }

  if (OB_SUCC(ret) && !skip_range && !can_seek_filter_for_vec_handoff()) {
    vec_non_essential_dim_max_score_sum_ = non_essential_dim_max_score_;
    if (has_range_max) {
      vec_non_essential_dim_max_score_sum_ = 0.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < vec_non_essential_dim_count_; ++i) {
        ObDAATMergeIter *iter = merge_iters_.at(vec_sorted_iter_idxes_.at(i));
        double bound = 0.0;
        if (!iter->iter_end() && OB_FAIL(iter->get_max_score(bound))) {
          LOG_WARN("failed to get global coverage bound", K(ret), K(i));
        } else {
          vec_non_essential_dim_max_score_sum_ += bound;
        }
      }
    }
  }

  LOG_TRACE("[BMM VEC MERGE] refine dimensions with range max", K(ret),
      K(min_id), K(min_id_inclusive), K(boundary), K(try_range_max),
      K(has_range_max), K(skip_range), K_(vec_non_essential_dim_count),
      K_(vec_non_essential_dim_max_score));
  return ret;
}

// 为当前区间的 non-essential 维度构造分数上界后缀和。
int ObDASBMMOp::build_vec_non_essential_suffix_sums()
{
  int ret = OB_SUCCESS;
  // 按区间分组使用的相同分数上界逆序累加，使槽位 i 保存从 i 到前缀末尾的上界总和。
  double non_essential_block_max_score = 0.0;
  for (int64_t i = vec_non_essential_dim_count_ - 1; i >= 0; --i) {
    const int64_t iter_idx = vec_sorted_iter_idxes_.at(i);
    non_essential_block_max_score += vec_range_max_scores_.at(iter_idx);
    non_ess_block_max_score_suffix_sums_.at(i) = non_essential_block_max_score;
  }
  return ret;
}

int ObDASBMMOp::rebuild_vec_essential_merger(const ObDASRowID &min_id)
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = sorted_iter_idxes_.count();
  // 将 Range Max 细化后的 essential 维度重新构建 merger。
  for (int64_t i = vec_non_essential_dim_count_; OB_SUCC(ret) && i < dim_cnt; ++i) {
    const int64_t iter_idx = vec_sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx), K(i), K_(vec_non_essential_dim_count));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->advance_to(min_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance iter to candidate min id", K(ret), K(iter_idx), K(min_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_merger_->reuse_with_selected_iters(
      vec_sorted_iter_idxes_, false, vec_non_essential_dim_count_, dim_cnt - 1))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to rebuild with selected iters", K(ret), K_(vec_non_essential_dim_count),
          K(dim_cnt), K(vec_sorted_iter_idxes_));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// 从 target 开始定位可产生候选文档的向量化窗口，并准备该窗口的分数上界和 essential merger。
int ObDASBMMOp::compute_vec_boundary_at(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  ObDASRowID min_id = target;
  ObDASRowID mini_max_id;
  ObDASRowID natural_boundary;
  bool range_ready = false;
  // 首轮包含 target；跳过当前区间后改为从 block 结束位置之后继续定位。
  bool min_id_inclusive = true;

  // 持续跳过总分上界不超过竞争阈值的区间，直到找到候选窗口或迭代结束。
  while (OB_SUCC(ret) && !range_ready) {
    bool is_candidate_range = false;
    while (OB_SUCC(ret) && !is_candidate_range) {
      is_candidate_range = true;
      // 当 block 上界之和不超过最小竞争阈值时，使用全部 token 来计算 essential boundary
      bool use_all_dims = false;
      int64_t effective_non_essential_dim_count = 0;
      double effective_non_essential_dim_max_score = 0.0;
      double essential_block_max_score = 0.0;
      // 依次刷新分组并计算 essential boundary；essential 上界未超过剩余阈值时可整体跳过当前区间。
      if (OB_FAIL(repartition_vec_dims_at(min_id, min_id_inclusive, use_all_dims))) {
        LOG_WARN("failed to repartition vec dimensions", K(ret), K(min_id), K(min_id_inclusive));
      } else {
        // 当前 block 可整体跳过时，本轮使用全部 token；否则复用稳定的全局上界分组。
        effective_non_essential_dim_count = use_all_dims ? 0 : non_essential_dim_count_;
        effective_non_essential_dim_max_score = use_all_dims ? 0.0 : non_essential_dim_max_score_;
        if (OB_FAIL(compute_vec_essential_boundary(
            effective_non_essential_dim_count, mini_max_id, essential_block_max_score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to compute vec essential boundary", K(ret), K(min_id));
        } else if (essential_block_max_score
            <= min_competitive_score_ - effective_non_essential_dim_max_score) {
          if (mini_max_id.is_max()) {
            // essential boundary 已到最大 RowID，后续不存在可继续定位的区间。
            ret = OB_ITER_END;
          } else {
            is_candidate_range = false;
            // 下一轮从当前 essential 最早 block 的结束位置之后继续，避免重复处理同一边界。
            min_id_inclusive = false;
            min_id = mini_max_id;
          }
        }
      }
      LOG_TRACE("[BMM VEC MERGE] calculate mini max id", K(ret), K(is_candidate_range),
        K(min_id), K(mini_max_id), K(essential_block_max_score),
        K(effective_non_essential_dim_max_score));
    }

    // 保存自然边界并扩展候选窗口，再使用 Range Max 判断扩展后的冻结窗口是否可整体跳过。
    bool skip_range = false;
    if (OB_FAIL(ret)) {
    } else if (!is_candidate_range) {
      ret = OB_ITER_END;
    } else if (FALSE_IT(natural_boundary = mini_max_id)) {
    } else if (OB_FAIL(expand_vec_boundary(min_id, mini_max_id))) {
      LOG_WARN("failed to expand vec boundary", K(ret), K(min_id), K(mini_max_id));
    } else if (OB_FAIL(refine_vec_dims_with_range_max(
        min_id, min_id_inclusive, mini_max_id, skip_range))) {
      LOG_WARN("failed to refine vec dimensions", K(ret), K(min_id), K(mini_max_id));
    } else if (skip_range) {
      const bool reach_rowid_end = mini_max_id.is_max()
          || (mini_max_id.is_normal() && UINT64_MAX == mini_max_id.get_uint64());
      if (reach_rowid_end) {
        // 饱和后的 uint64 边界不存在可表示的排他后继 RowID。
        ret = OB_ITER_END;
      } else {
        // 将下一轮起点移动到当前区间边界之后，继续定位后续候选区间。
        min_id = mini_max_id;
        min_id_inclusive = false;
      }
    } else {
      // 当前区间未被 Range Max 剪枝，结束区间定位并进入窗口状态准备。
      range_ready = true;
    }
  }

  // 使用最终区间分组构造 non-essential 上界后缀和，再以 essential 后缀重建 merger。
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_vec_non_essential_suffix_sums())) {
    LOG_WARN("failed to build vec non-essential suffix sums", K(ret), K(min_id),
        K_(vec_non_essential_dim_count));
  } else if (OB_FAIL(rebuild_vec_essential_merger(min_id))) {
    LOG_WARN("failed to rebuild vec essential merger", K(ret), K(min_id),
        K_(vec_non_essential_dim_count));
  } else {
    // 所有区间局部状态准备完成后，发布本轮向量化执行使用的最终边界。
    vec_boundary_id_ = mini_max_id;
  }
  const bool boundary_expanded = OB_SUCC(ret) && natural_boundary.is_normal()
      && vec_boundary_id_.is_normal()
      && natural_boundary.get_uint64() < vec_boundary_id_.get_uint64();
  const int64_t essential_dim_count =
      sorted_iter_idxes_.count() - vec_non_essential_dim_count_;
  LOG_TRACE("[BMM VEC MERGE] compute vec boundary finished", K(ret), K(min_id),
      K(natural_boundary), K(vec_boundary_id_), K(BMM_VEC_MIN_BOUNDARY_SIZE),
      K(boundary_expanded), K_(vec_non_essential_dim_count), K(essential_dim_count));
  return ret;
}

int ObDASBMMOp::prepare_direct_filter_for_vectorized_batch(
    const ObDASRowID &batch_start_id,
    ObDASRowID &batch_end_id,
    bool &need_evaluate_batch)
{
  int ret = OB_SUCCESS;
  bool has_hit = false;
  need_evaluate_batch = true;
  if (OB_ISNULL(filter_op_) || !filter_op_->supports_direct_rowid_drain()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid direct filter op", K(ret), KP_(filter_op));
  } else if (filter_end_) {
    need_evaluate_batch = false;
  } else if (OB_FAIL(build_direct_filter_bitset(
        batch_start_id, batch_end_id, has_hit))) {
    LOG_WARN("failed to build direct filter bitset", K(ret),
        K(batch_start_id), K(batch_end_id));
  } else if (!has_hit) {
    need_evaluate_batch = false;
  } else {
    doc_score_accumulator_.set_filter_mask(filter_bitset_);
  }
  return ret;
}

int ObDASBMMOp::prepare_filter_for_vectorized_batch(
    const ObDASRowID &batch_start_id,
    ObDASRowID &batch_end_id,
    bool &need_evaluate_batch)
{
  int ret = OB_SUCCESS;
  need_evaluate_batch = true;
  if (!has_filter()) {
    // no filter to prepare
  } else if (filter_end_) {
    need_evaluate_batch = false;
  } else if (ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN == filter_mode_) {
    if (OB_FAIL(prepare_direct_filter_for_vectorized_batch(
        batch_start_id, batch_end_id, need_evaluate_batch))) {
      LOG_WARN("failed to prepare direct filter for vectorized batch", K(ret),
          K(batch_start_id), K(batch_end_id));
    }
  } else {
    // Advance filter op to batch start id if necessary.
    int filter_cmp = 0;
    if (OB_FAIL(compare_uint64_rowid(curr_filter_id_, batch_start_id, filter_cmp))) {
      LOG_WARN("failed to compare filter id with batch start", K(ret),
          K_(curr_filter_id), K(batch_start_id));
    } else if (filter_cmp < 0 && OB_FAIL(advance_filter_to(batch_start_id, curr_filter_id_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance filter to batch start", K(ret), K(batch_start_id));
      } else {
        // Filter exhausted while catching up to batch start.
        need_evaluate_batch = false;
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret) && !filter_end_) {
      int end_cmp = 0;
      if (OB_FAIL(compare_uint64_rowid(curr_filter_id_, batch_end_id, end_cmp))) {
        LOG_WARN("failed to compare filter id with batch end", K(ret),
            K_(curr_filter_id), K(batch_end_id));
      } else if (end_cmp > 0) {
        // No filter hit in [batch_start_id, batch_end_id].
        need_evaluate_batch = false;
        cap_next_batch_to_filter_id_ = true;
      }
    }

    // After skipping a filter-empty batch, the next computed window may still
    // span many score docs. Cap it at curr_filter_id_ once, so scoring covers
    // the first known filter hit instead of a full accumulator window.
    if (OB_SUCC(ret) && need_evaluate_batch && cap_next_batch_to_filter_id_) {
      cap_next_batch_to_filter_id_ = false;
      int cap_cmp = 0;
      if (OB_FAIL(compare_uint64_rowid(curr_filter_id_, batch_end_id, cap_cmp))) {
        LOG_WARN("failed to compare filter id with batch end for cap", K(ret),
            K_(curr_filter_id), K(batch_end_id));
      } else if (cap_cmp < 0) {
        batch_end_id = curr_filter_id_;
      }
    }

    // Filter-before-score: build filter bitset and set it as mask BEFORE
    // evaluation so add_dim_batch skips non-matching docs during accumulation.
    if (OB_SUCC(ret) && need_evaluate_batch) {
      if (OB_FAIL(build_filter_bitset(batch_start_id, batch_end_id))) {
        LOG_WARN("failed to build filter bitset", K(ret), K(batch_start_id), K(batch_end_id));
      } else {
        doc_score_accumulator_.set_filter_mask(filter_bitset_);
      }
    }
  }
  return ret;
}

int ObDASBMMOp::apply_filter_to_scored_batch()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_bitset_) || !doc_score_accumulator_.is_valid() || !has_filter()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid candidate filter state", K(ret), KP_(filter_bitset), KP_(filter_op));
  } else if (doc_score_accumulator_.get_max_score() <= min_competitive_score_) {
    doc_score_accumulator_.clear_all();
  } else {
    const int64_t capacity = doc_score_accumulator_.get_capacity();
    const int64_t word_cnt = das_bitset_helper::word_count(capacity);
    const uint64_t start_id = doc_score_accumulator_.get_start_id();
    const uint64_t *candidate_words = doc_score_accumulator_.get_bitset_words();
    uint64_t *matched_words = filter_bitset_->reinterpret_data<uint64_t>();
    MEMSET(matched_words, 0, sizeof(uint64_t) * word_cnt);

    bool stop = false;
    for (int64_t word_idx = 0; OB_SUCC(ret) && !stop && word_idx < word_cnt; ++word_idx) {
      const int64_t word_base = word_idx * das_bitset_helper::WORD_BITS;
      uint64_t candidates = das_bitset_helper::mask_window_tail(
          candidate_words[word_idx], word_base, capacity);
      while (OB_SUCC(ret) && !stop && 0 != candidates) {
        const int64_t bit_idx = das_bitset_helper::lowest_set_bit(candidates);
        candidates = das_bitset_helper::clear_lowest_set_bit(candidates);
        const int64_t offset = word_base + bit_idx;
        const uint64_t candidate_doc_id = start_id + static_cast<uint64_t>(offset);
        ObDASRowID candidate_id;
        candidate_id.set_uint64(candidate_doc_id);
        double candidate_score = 0.0;
        int cmp_ret = 0;
        if (OB_FAIL(doc_score_accumulator_.get_score(candidate_doc_id, candidate_score))) {
          LOG_WARN("failed to get candidate score", K(ret), K(candidate_doc_id));
        } else if (candidate_score <= min_competitive_score_) {
          // skip uncompetitive candidates
        } else if (OB_FAIL(compare_uint64_rowid(curr_filter_id_, candidate_id, cmp_ret))) {
          LOG_WARN("failed to compare candidate with filter",
              K(ret), K_(curr_filter_id), K(candidate_id));
        } else if (cmp_ret < 0 && OB_FAIL(advance_filter_to(candidate_id, curr_filter_id_))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            stop = true;
          } else {
            LOG_WARN("failed to advance filter to candidate", K(ret), K(candidate_id));
          }
        } else if (OB_FAIL(compare_uint64_rowid(curr_filter_id_, candidate_id, cmp_ret))) {
          LOG_WARN("failed to compare advanced filter with candidate",
              K(ret), K_(curr_filter_id), K(candidate_id));
        } else if (0 == cmp_ret) {
          matched_words[word_idx] |= das_bitset_helper::bit_mask(bit_idx);
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(doc_score_accumulator_.retain_by_mask(*filter_bitset_))) {
      LOG_WARN("failed to retain filter-matched candidates", K(ret));
    }
  }
  return ret;
}

int ObDASBMMOp::evaluate_essential_batch(const ObDASRowID &batch_end_id, bool &is_batch_valid)
{
  int ret = OB_SUCCESS;
  is_batch_valid = false;
  if (OB_ISNULL(row_merger_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null row merger", K(ret));
  } else if (OB_FAIL(row_merger_->merge_one_batch(batch_end_id, doc_score_accumulator_))) {
    LOG_WARN("failed to merge one batch", K(ret), K(batch_end_id));
  } else {
    is_batch_valid = !doc_score_accumulator_.is_empty();
  }
  return ret;
}

int ObDASBMMOp::score_active_candidates(ObDAATMergeIter &iter)
{
  int ret = OB_SUCCESS;
  const uint64_t *active_words = doc_score_accumulator_.get_bitset_words();
  const int64_t capacity = doc_score_accumulator_.get_capacity();
  const int64_t word_cnt = das_bitset_helper::word_count(capacity);
  const uint64_t start_id = doc_score_accumulator_.get_start_id();
  bool iter_end = false;
  for (int64_t word_idx = 0; OB_SUCC(ret) && !iter_end && word_idx < word_cnt; ++word_idx) {
    const int64_t word_base = word_idx * das_bitset_helper::WORD_BITS;
    uint64_t active = das_bitset_helper::mask_window_tail(
        active_words[word_idx], word_base, capacity);
    while (OB_SUCC(ret) && !iter_end && 0 != active) {
      const int64_t bit_idx = das_bitset_helper::lowest_set_bit(active);
      active = das_bitset_helper::clear_lowest_set_bit(active);
      const uint64_t candidate_doc_id = start_id
          + static_cast<uint64_t>(word_base + bit_idx);
      ObDASRowID candidate_id;
      ObDASRowID curr_id;
      double score = 0.0;
      candidate_id.set_uint64(candidate_doc_id);
      if (OB_FAIL(iter.advance_to_id(candidate_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          iter_end = true;
        } else {
          LOG_WARN("failed to seek non-essential dim to active candidate",
              K(ret), K(candidate_id));
        }
      } else if (OB_FAIL(iter.get_curr_id(curr_id))) {
        LOG_WARN("failed to get non-essential dim rowid", K(ret), K(candidate_id));
      } else if (curr_id.get_uint64() != candidate_doc_id) {
        // This dimension does not contain the active candidate.
      } else if (OB_FAIL(iter.load_curr_score(score))) {
        LOG_WARN("failed to load non-essential candidate score", K(ret), K(candidate_id));
      } else if (OB_FAIL(doc_score_accumulator_.add_existing_dim(candidate_doc_id, score))) {
        LOG_WARN("failed to add non-essential candidate score", K(ret), K(candidate_id));
      }
    }
  }
  return ret;
}

int ObDASBMMOp::evaluate_non_essential_batch(
    const ObDASRowID &batch_end_id,
    bool &is_batch_valid)
{
  int ret = OB_SUCCESS;
  is_batch_valid = true;
  // Reverse iteration: process from highest block-max non-essential dim to lowest.
  // High-score dims are evaluated first, so the remaining block-max sum (from
  // unevaluated low-score dims) shrinks fast, giving a tight per-doc filter
  // threshold that prunes more docs early.
  //
  // non_ess_block_max_score_suffix_sums_[i] = sum(block_max[i..end]) (reverse cumulative).
  // remaining_block_max_score tracks the running sum of unevaluated dims to avoid recomputation.
  if (OB_UNLIKELY(doc_score_accumulator_.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty doc score accumulator", K(ret));
  }
  double remaining_block_max_score =
      (vec_non_essential_dim_count_ > 0 ? non_ess_block_max_score_suffix_sums_.at(0) : 0.0);
  for (int64_t i = vec_non_essential_dim_count_ - 1;
      OB_SUCC(ret) && is_batch_valid && i >= 0;
      --i) {
    const int64_t iter_idx = vec_sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    // Per-dim block-max = cum_sum[i] - cum_sum[i+1]
    const double dim_block_max_score = (i + 1 < vec_non_essential_dim_count_)
        ? non_ess_block_max_score_suffix_sums_.at(i) - non_ess_block_max_score_suffix_sums_.at(i + 1)
        : non_ess_block_max_score_suffix_sums_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      remaining_block_max_score -= dim_block_max_score;
    } else if (doc_score_accumulator_.get_max_score() + remaining_block_max_score <= min_competitive_score_) {
      is_batch_valid = false;
    } else {
      const bool probe_score_op = score_op_probe_allowed_.at(iter_idx)
          && doc_score_accumulator_.get_active_count(BMM_ACTIVE_PROBE_MAX_COUNT) <= BMM_ACTIVE_PROBE_MAX_COUNT;
      if (probe_score_op) {
        if (OB_FAIL(score_active_candidates(*iter))) {
          LOG_WARN("failed to score sparse active candidates", K(ret), K(iter_idx));
        }
      } else if (OB_FAIL(iter->drain_until_active(batch_end_id, doc_score_accumulator_))) {
        LOG_WARN("failed to drain active iter until boundary", K(ret), K(iter_idx));
      }
      // Remove the dim we just evaluated from the remaining block-max budget.
      if (OB_SUCC(ret)) {
        remaining_block_max_score -= dim_block_max_score;
      }
      // Per-doc competitive filtering: remove docs that cannot reach
      // min_competitive_score_ even with all remaining dims at their block-max.
      if (OB_SUCC(ret) && remaining_block_max_score >= 0) {
        const double min_required = min_competitive_score_ - remaining_block_max_score;
        if (min_required > 0) {
          if (OB_FAIL(doc_score_accumulator_.filter_below_score(min_required))) {
            LOG_WARN("failed to filter below score", K(ret), K(min_required));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_batch_valid) {
    is_batch_valid = !doc_score_accumulator_.is_empty();
  }
  return ret;
}

int ObDASBMMOp::build_filter_bitset(
    const ObDASRowID &batch_start_id,
    const ObDASRowID &batch_end_id)
{
  int ret = OB_SUCCESS;
  // Precondition: curr_filter_id_ >= batch_start_id (already advanced in EVALUATE_VEC_ESSENTIAL).
  if (OB_ISNULL(filter_bitset_) || !doc_score_accumulator_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter bitset or doc score accumulator", K(ret));
  } else if (filter_end_) {
    // nothing to do
  } else {
    const int64_t capacity = doc_score_accumulator_.get_capacity();
    uint64_t *filter_words = filter_bitset_->reinterpret_data<uint64_t>();
    ObDASBitmapOp *bitmap_filter = DAS_SEARCH_OP_BITMAP == filter_op_->get_op_type()
        ? static_cast<ObDASBitmapOp *>(filter_op_) : nullptr;
    if (!is_filter_driven_optional()
        && OB_NOT_NULL(bitmap_filter)
        && bitmap_filter->is_filter_bitmap_view()) {
      if (OB_FAIL(bitmap_filter->drain_into_filter_bitset(
              batch_start_id, batch_end_id, capacity,
              filter_words, curr_filter_id_, filter_end_))) {
        LOG_WARN("failed to copy provider bitmap into BMM filter bitset",
                 K(ret), K(batch_start_id), K(batch_end_id));
      }
    } else {
      const int64_t word_cnt = das_bitset_helper::word_count(capacity);
      MEMSET(filter_words, 0, sizeof(uint64_t) * word_cnt);
      const uint64_t start_id = batch_start_id.get_uint64();
      ObBMMFilterBitsetDrainSink sink(
          start_id,
          capacity,
          filter_words,
          is_filter_driven_optional() ? &doc_score_accumulator_ : nullptr);
      if (OB_FAIL(drain_search_op_rowids_until(
              *filter_op_, curr_filter_id_, filter_end_, batch_end_id, sink))) {
        LOG_WARN("failed to drain filter into bitset", K(ret), K(batch_start_id), K(batch_end_id));
      }
    }
  }
  return ret;
}

int ObDASBMMOp::build_direct_filter_bitset(
    const ObDASRowID &batch_start_id,
    const ObDASRowID &batch_end_id,
    bool &has_hit)
{
  int ret = OB_SUCCESS;
  has_hit = false;
  if (OB_ISNULL(filter_op_) || !filter_op_->supports_direct_rowid_drain()
      || OB_ISNULL(filter_bitset_) || !doc_score_accumulator_.is_valid()
      || OB_UNLIKELY(!batch_start_id.is_normal() || !batch_end_id.is_normal()
                     || batch_start_id.get_uint64() > batch_end_id.get_uint64())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct filter bitset input", K(ret), KP_(filter_op),
        KP_(filter_bitset), K(batch_start_id), K(batch_end_id));
  } else {
    const int64_t capacity = doc_score_accumulator_.get_capacity();
    const int64_t word_cnt = das_bitset_helper::word_count(capacity);
    uint64_t *filter_words = filter_bitset_->reinterpret_data<uint64_t>();
    MEMSET(filter_words, 0, sizeof(uint64_t) * word_cnt);

    ObDASRowID next_id_after;
    ObBMMFilterBitsetDrainSink sink(
        batch_start_id.get_uint64(), capacity, filter_words, nullptr);
    if (OB_FAIL(filter_op_->direct_drain_rowids(
        batch_start_id.get_uint64(), batch_end_id.get_uint64(),
        sink, next_id_after))) {
      LOG_WARN("failed to drain direct filter", K(ret),
          K(batch_start_id), K(batch_end_id));
    } else if (!next_id_after.is_max()
               && OB_UNLIKELY(!next_id_after.is_normal()
                              || next_id_after.get_uint64()
                                 <= batch_end_id.get_uint64())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid direct filter lookahead", K(ret),
          K(batch_end_id), K(next_id_after));
    } else {
      has_hit = sink.has_hit();
      filter_end_ = next_id_after.is_max();
      curr_filter_id_ = next_id_after;
    }
  }
  return ret;
}

int ObDASBMMOp::align_essential_iters_to_target(
    const ObDASRowID &target,
    ObDASRowID &min_id,
    double &essential_score,
    bool &has_alive)
{
  int ret = OB_SUCCESS;
  const uint64_t target_uint64 = target.get_uint64();
  const int64_t dim_cnt = vec_sorted_iter_idxes_.count();
  bool has_min = false;
  has_alive = false;
  essential_score = 0.0;
  // Only the current range-local essential suffix participates; non-essential dims are
  // probed separately and repositioned by the next compute_vec_boundary_at.
  for (int64_t i = vec_non_essential_dim_count_; OB_SUCC(ret) && i < dim_cnt; ++i) {
    ObDAATMergeIter *iter = merge_iters_[vec_sorted_iter_idxes_[i]];
    ObDASRowID curr_id;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null essential iter", K(ret), K(i), K_(vec_non_essential_dim_count));
    } else if (iter->iter_end()) {
      // skip: a single exhausted essential dim does not end the query.
    } else if (OB_FAIL(iter->get_curr_id(curr_id))) {
      LOG_WARN("failed to get curr id", K(ret), K(i));
    } else if (curr_id.get_uint64() < target_uint64) {
      // Lagging behind the filter target: advance once.
      if (OB_FAIL(iter->advance_to_id(target))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to advance essential iter to target", K(ret), K(i), K(target));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iter->get_curr_id(curr_id))) {
        LOG_WARN("failed to re-get curr id", K(ret), K(i));
      } else if (OB_UNLIKELY(curr_id.get_uint64() < target_uint64)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected essential curr id still behind target", K(ret),
            K(curr_id), K(target), K(i));
      }
    }

    if (OB_SUCC(ret) && !iter->iter_end()) {
      has_alive = true;
      if (!has_min) {
        min_id = curr_id;
        has_min = true;
      } else if (curr_id.get_uint64() < min_id.get_uint64()) {
        min_id = curr_id;
      }

      if (curr_id.get_uint64() == target_uint64) {
        // Exact intersection at target: accumulate this clause's score (SUM semantics,
        // matching merge_one_row's update_score aggregation).
        double curr_score = 0.0;
        if (OB_FAIL(iter->load_curr_score(curr_score))) {
          LOG_WARN("failed to load curr score", K(ret), K(i));
        } else {
          essential_score += curr_score;
        }
      }
    }
  }
  return ret;
}

int ObDASBMMOp::find_leapfrog_candidate(
    ObDASRowID &matched_id, double &essential_score, bool &window_end)
{
  int ret = OB_SUCCESS;
  essential_score = 0.0;
  window_end = false;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(curr_filter_id_.is_min()) && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to init filter cursor for leapfrog", K(ret));
    }
  }

  bool matched = false;
  while (OB_SUCC(ret) && !matched && !window_end) {
    ObDASRowID min_id;
    bool has_alive = false;
    double essential_score_round = 0.0;
    if (!vec_boundary_id_.is_min() && !vec_boundary_id_.is_max()
        && curr_filter_id_.get_uint64() > vec_boundary_id_.get_uint64()) {
      window_end = true;
    } else if (OB_FAIL(align_essential_iters_to_target(
            curr_filter_id_, min_id, essential_score_round, has_alive))) {
      LOG_WARN("failed to align essential iters to filter target", K(ret), K_(curr_filter_id));
    } else if (!has_alive) {
      // Local essential EOF only exhausts this window.
      window_end = !vec_boundary_id_.is_min();
      if (!window_end) {
        ret = OB_ITER_END;
      }
    } else if (OB_UNLIKELY(min_id.get_uint64() < curr_filter_id_.get_uint64())) {
      // align contract: every surviving essential curr_id >= target, so min_id >= filter.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected min id smaller than curr filter id", K(ret),
          K(min_id), K_(curr_filter_id));
    } else if (!vec_boundary_id_.is_min() && !vec_boundary_id_.is_max()
        && min_id.get_uint64() > vec_boundary_id_.get_uint64()) {
      window_end = true;
    } else if (min_id.get_uint64() > curr_filter_id_.get_uint64()) {
      // Essential set is past the filter: pull the filter forward to min_id and re-align.
      // advance_filter_to may land on min_id (exact) or beyond it; both are re-aligned.
      if (OB_FAIL(advance_filter_to(min_id, curr_filter_id_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to min id", K(ret), K(min_id));
      }
    } else {
      // Exact intersection: min_id == curr_filter_id_.
      matched = true;
      matched_id = curr_filter_id_;
      essential_score = essential_score_round;
    }
  }
  return ret;
}

int ObDASBMMOp::collect_leapfrog_candidates()
{
  int ret = OB_SUCCESS;
  sparse_candidate_buffer_.reuse();
  const int64_t cand_cap = sparse_candidate_buffer_.get_capacity();
  if (OB_UNLIKELY(cand_cap <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sparse candidate buffer state", K(ret), K(cand_cap));
  }
  // Range-local non-essential upper bound of the current boundary window; refreshed after
  // an in-loop boundary recompute rebuilds the vectorized partition.
  double non_ess_total_max = vec_non_essential_dim_max_score_;
  bool stop = false;
  while (OB_SUCC(ret) && !stop && !sparse_candidate_buffer_.is_full()) {
    ObDASRowID matched_id;
    double essential_score = 0.0;
    bool window_end = false;
    if (OB_FAIL(find_leapfrog_candidate(matched_id, essential_score, window_end))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to find filter essential match", K(ret));
      } else if (!sparse_candidate_buffer_.is_empty()) {
        // Filter or essential exhausted; emit the candidates collected so far.
        ret = OB_SUCCESS;
        stop = true;
      }
      // else: propagate OB_ITER_END with an empty buffer.
    } else if (window_end && !sparse_candidate_buffer_.is_empty()) {
      // Score and emit the old buffer before overwriting its partition and bounds.
      stop = true;
    } else if (window_end) {
      if (OB_FAIL(compute_vec_boundary())) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to restore sparse window coverage", K(ret));
      } else {
        non_ess_total_max = vec_non_essential_dim_max_score_;
      }
    } else {
      // Intersection inside the boundary window: admit on the dim-max upper bound.
      const bool is_candidate = essential_score + non_ess_total_max > min_competitive_score_;
      if (is_candidate) {
        if (OB_FAIL(sparse_candidate_buffer_.append(matched_id.get_uint64(), essential_score))) {
          LOG_WARN("failed to append sparse candidate", K(ret), K(matched_id), K(essential_score));
        }
      }
      // Advance the filter past the matched doc exactly once to guarantee forward progress;
      // essential iters stay positioned and are re-advanced by the next alignment round.
      if (FAILEDx(next_filter_rowid(curr_filter_id_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance filter after match", K(ret), K(matched_id));
        } else if (!sparse_candidate_buffer_.is_empty()) {
          // Filter exhausted; emit the candidates collected so far.
          ret = OB_SUCCESS;
          stop = true;
        }
        // else: propagate OB_ITER_END with an empty buffer.
      }
    }
  }
  return ret;
}

int ObDASBMMOp::score_leapfrog_candidates()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sparse_candidate_buffer_.is_empty())) {
    // nothing to probe
  }
  // Reverse iteration over range-local non-essential dims (highest upper bound first) so
  // the remaining score budget shrinks fast and prunes more candidates early.
  double remaining_max_score =
      (vec_non_essential_dim_count_ > 0 ? non_ess_block_max_score_suffix_sums_.at(0) : 0.0);
  for (int64_t i = vec_non_essential_dim_count_ - 1;
      OB_SUCC(ret) && sparse_candidate_buffer_.has_alive() && i >= 0;
      --i) {
    const int64_t iter_idx = vec_sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    // Per-dim upper bound = cum_sum[i] - cum_sum[i+1].
    const double dim_max_score = (i + 1 < vec_non_essential_dim_count_)
        ? non_ess_block_max_score_suffix_sums_.at(i) - non_ess_block_max_score_suffix_sums_.at(i + 1)
        : non_ess_block_max_score_suffix_sums_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (!iter->iter_end()) {
      // Score alive candidates in ascending doc id order (monotonic advance_to).
      bool dim_exhausted = false;
      const int64_t candidate_count = sparse_candidate_buffer_.get_count();
      for (int64_t c = 0; OB_SUCC(ret) && !dim_exhausted && c < candidate_count; ++c) {
        uint64_t candidate_doc_id = 0;
        if (!sparse_candidate_buffer_.get_doc_id(c, candidate_doc_id)) {
          // pruned already
        } else {
          ObDASRowID cand_id;
          cand_id.set_uint64(candidate_doc_id);
          ObDASRowID curr_id;
          double curr_score = 0.0;
          if (OB_FAIL(iter->advance_to_id(cand_id))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to advance non essential iter", K(ret), K(iter_idx), K(cand_id));
            } else {
              // Dim exhausted; remaining candidates get no score from this dim.
              ret = OB_SUCCESS;
              dim_exhausted = true;
            }
          } else if (OB_FAIL(iter->get_curr_id(curr_id))) {
            LOG_WARN("failed to get curr id", K(ret), K(iter_idx));
          } else if (curr_id.get_uint64() != cand_id.get_uint64()) {
            // this dim does not contain cand_id
          } else if (OB_FAIL(iter->load_curr_score(curr_score))) {
            LOG_WARN("failed to load curr score", K(ret), K(iter_idx));
          } else if (OB_FAIL(sparse_candidate_buffer_.add_score(c, curr_score))) {
            LOG_WARN("failed to add sparse candidate score", K(ret), K(c), K(curr_score));
          }
        }
      }
    }

    // Remove the dim just evaluated (or skipped at EOF) from the remaining budget and
    // prune candidates that cannot beat min_competitive_score_ even with every remaining
    // dim at its dim max.
    if (OB_SUCC(ret)) {
      remaining_max_score -= dim_max_score;
      if (remaining_max_score < 0) {
        remaining_max_score = 0.0;
      }
      const double min_required_score = min_competitive_score_ - remaining_max_score;
      if (OB_FAIL(sparse_candidate_buffer_.prune_below(min_required_score))) {
        LOG_WARN("failed to prune sparse candidates", K(ret), K(min_required_score));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
