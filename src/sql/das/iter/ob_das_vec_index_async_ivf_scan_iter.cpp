/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_vec_index_async_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "lib/alloc/alloc_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include <cmath>

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace blocksstable;

// The plain SQL vector path owns a different iterator and profile family, so an
// ambient profile of any other id is ignored.
ObOpProfile<ObMetric> *ObDASVecIndexAsyncIVFScanIter::get_current_hybrid_vector_iter_profile()
{
  ObOpProfile<ObMetric> *profile = common::get_current_profile();
  return OB_NOT_NULL(profile) && profile->get_id() == ObProfileId::HYBRID_SEARCH_VEC_ITER
             ? profile
             : nullptr;
}

void ObDASVecIndexAsyncIVFScanIter::bind_partition_profile()
{
  profile_ = ObIVFProfileUtil::register_child(
      get_current_hybrid_vector_iter_profile(), ObProfileId::HYBRID_SEARCH_VEC_ASYNC_IVF_SCAN);
}

void ObDASVecIndexAsyncIVFScanIter::record_profile_finish(const int scan_ret)
{
  if (OB_SUCCESS != scan_ret && OB_ITER_END != scan_ret) {
    const int64_t tag = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == scan_ret
        ? PROFILE_VECTOR_RETRY : PROFILE_QUERY_ERROR;
    ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_FINISH_TAG, tag);
  } else if (OB_ITER_END == scan_ret || round_ready_) {
    ObIVFProfileUtil::set_once(profile_, ObMetricId::VEC_IVF_FINISH_TAG, PROFILE_RETURN_RESULT);
  }
}

void ObDASVecIndexAsyncIVFScanIter::clear_partition_profile()
{
  // Metrics are recorded as work completes; closing early must not invent a finish tag.
  profile_ = nullptr;
}

int ObDASVecIndexAsyncIVFScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  bind_partition_profile();
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::rescan()
{
  clear_partition_profile();
  bind_partition_profile();
  reset_epoch();
  return OB_SUCCESS;
}

int ObDASVecIndexAsyncIVFScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (DAS_ITER_VEC_INDEX_ASYNC_IVF_SCAN != param.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async IVF iterator type", K(ret), K(param.type_));
  } else {
    auto &p = static_cast<ObDASVecIndexAsyncIVFScanIterParam &>(param);
    ctdef_ = p.ctdef_; rtdef_ = p.rtdef_; search_ctx_ = p.search_ctx_;
    tx_desc_ = p.tx_desc_; snapshot_ = p.snapshot_; filter_rtdef_ = p.filter_rtdef_;
    if (!p.is_valid() || nullptr == ctdef_->get_index_scan_ctdef()
        || nullptr == ctdef_->get_delete_scan_ctdef()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async IVF iterator parameters", K(ret), KP_(ctdef), KP_(rtdef), KP_(search_ctx));
    }
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::estimate_partition_filter(
    ObDASSearchCtx &ctx, ObIDASSearchRtDef &filter,
    ObDASSearchCost &cost, double &selectivity)
{
  int ret = OB_SUCCESS;
  // Both estimates must describe the current partition, not a previous Group or
  // parameter binding. Unknown/zero estimates remain budget hints, never pruning.
  ObDASAsyncBindingGuard binding_guard(ctx);
  binding_guard.set(OB_INVALID_ID, -1, false);
  filter.reset_cost_recursive();
  cost.reset();
  selectivity = 0.1;
  if (OB_FAIL(ctx.refresh_table_row_count_if_needed())) {
    LOG_WARN("failed to refresh async IVF partition row count", K(ret));
  } else if (OB_FAIL(filter.get_cost(ctx, cost))) {
    LOG_WARN("failed to estimate async IVF partition filter", K(ret));
  } else if (ctx.get_row_count().is_valid() && ctx.get_row_count().cost() > 0
             && cost.is_valid() && cost.cost() > 0
             && cost.cost() <= ctx.get_row_count().cost()) {
    selectivity = static_cast<double>(cost.cost()) / ctx.get_row_count().cost();
  }
  // The caller may restore a different Group binding after this estimate.
  filter.reset_cost_recursive();
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::build_query_condition(ObIAllocator &alloc,
    ObEvalCtx &ctx, ObExpr *search_vec, bool need_post_filter, uint64_t limit)
{
  int ret = OB_SUCCESS;
  need_post_filter_ = need_post_filter;
  limit_ = limit;
  if (nullptr == ctdef_ || nullptr == search_vec || nullptr == snapshot_ || 0 == limit || limit > INT64_MAX / 10) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async IVF query state", K(ret), KP_(ctdef), KP(search_vec), KP_(snapshot), K(limit));
  } else if (OB_FAIL(apply_async_vector_read_snapshot(snapshot_, effective_snapshot_))) {
    LOG_WARN("failed to bind IVF effective snapshot", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(alloc, &ctx, search_vec, query_vector_))) {
    LOG_WARN("failed to materialize IVF query vector", K(ret));
  } else if (query_vector_.length() != ctdef_->dim_ * sizeof(float)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async IVF query dimension mismatch", K(ret), K(query_vector_.length()), K(ctdef_->dim_));
  } else {
    nprobe_ = 8;
    if (ctdef_->query_param_.is_set_ivf_nprobes_) {
      nprobe_ = ctdef_->query_param_.ivf_nprobes_;
    } else if (nullptr != ctx.exec_ctx_.get_my_session()) {
      uint64_t probe = 0;
      if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_ob_ivf_nprobes(probe))) {
        LOG_WARN("failed to read IVF nprobes", K(ret));
      } else { nprobe_ = probe; }
    }
    if (nullptr == ctdef_->sort_expr_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing async IVF distance expression", K(ret));
    }
    else if (T_FUN_SYS_L2_DISTANCE == ctdef_->sort_expr_->type_) { query_.expected_dist_type_ = share::VIDA_L2; }
    else if (T_FUN_SYS_COSINE_DISTANCE == ctdef_->sort_expr_->type_) { query_.expected_dist_type_ = share::VIDA_COS; }
    else if (T_FUN_SYS_NEGATIVE_INNER_PRODUCT == ctdef_->sort_expr_->type_) { query_.expected_dist_type_ = share::VIDA_IP; }
    else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported async IVF distance expression", K(ret), K(ctdef_->sort_expr_->type_));
    }
    const bool auto_search = ctdef_->query_param_.is_set_ivf_nprobes_ && 0 == ctdef_->query_param_.ivf_nprobes_;
    query_.query_ = reinterpret_cast<const float *>(query_vector_.ptr());
    query_.dim_ = ctdef_->dim_;
    query_.nprobe_ = nprobe_;
    query_.cluster_size_ = ctdef_->vector_index_param_.cluster_size_;
    if (OB_SUCC(ret) && ctdef_->query_param_.is_set_similarity_threshold_
        && ctdef_->query_param_.similarity_threshold_ > 0
        && OB_FAIL(ObDasVecScanUtils::check_ivf_support_similarity_threshold(*ctdef_->sort_expr_))) {
      LOG_WARN("unsupported IVF similarity metric", K(ret));
    }
    query_.k_ = limit;
    query_.original_k_ = 0;
    query_.num_candidates_ = 0;
    query_.is_set_refine_k_ = ctdef_->query_param_.is_set_refine_k_;
    const float refine_k = ctdef_->query_param_.is_set_refine_k_
        ? ctdef_->query_param_.refine_k_ : ctdef_->vector_index_param_.refine_k_;
    query_.refine_k_ = refine_k;
    if (OB_SUCC(ret) && auto_search) {
      const double candidates = std::ceil(static_cast<double>(limit) * refine_k);
      if (!std::isfinite(candidates) || candidates < limit || candidates >= static_cast<double>(INT64_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DiskBBQ automatic candidate count", K(ret), K(limit), K(refine_k));
      } else {
        query_.original_k_ = limit;
        query_.num_candidates_ = static_cast<int64_t>(candidates);
      }
    }
    if (OB_SUCC(ret) && need_post_filter_) {
      const double filter_selectivity = std::isfinite(selectivity_)
          && selectivity_ >= 0.0 && selectivity_ <= 1.0 ? std::max(0.1, selectivity_) : 1.0;
      const double candidate_limit = std::ceil(static_cast<double>(limit) * refine_k / filter_selectivity);
      if (!std::isfinite(candidate_limit) || candidate_limit < limit
          || candidate_limit >= static_cast<double>(INT64_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DiskBBQ quantized candidate limit", K(ret), K(limit), K(candidate_limit));
      } else {
        query_.k_ = static_cast<int64_t>(candidate_limit);
      }
    }
    next_nprobe_ = nprobe_;
    query_.selectivity_ = selectivity_;
    query_.read_stats_ = &stats_;
    omt::ObTenantConfigGuard conf(TENANT_CONF(MTL_ID()));
    if (ctdef_->query_param_.is_set_strategy_) {
      strategy_ = ctdef_->query_param_.strategy_;
    } else {
      const ObString tenant_strategy(conf->ob_vector_search_strategy.get_value());
      strategy_ = tenant_strategy == share::ObVectorTenantSearchStrategy::LATENCY_FIRST_STR
          ? share::ObVecIdxQueryStrategy::LATENCY_FIRST : share::ObVecIdxQueryStrategy::RECALL_FIRST;
    }
    const int64_t max_scan_vectors = conf->_disk_bbq_max_scan_vectors;
    scan_budget_ = ObIvfScanBudget(need_post_filter_ ? max_scan_vectors : 0);
    query_.scan_budget_ = need_post_filter_ ? &scan_budget_ : nullptr;
  }
  return ret;
}

bool ObDASVecIndexAsyncIVFScanIter::can_group_pre_filter() const
{
  ObDASAsyncIndexDomainAnalysis analysis;
  const auto *partition = nullptr == search_ctx_ ? nullptr : search_ctx_->get_current_async_partition();
  return VEC_INDEX_PRE == vec_index_type_ && nullptr != filter_rtdef_ && nullptr != partition
      && nullptr != search_ctx_->get_async_query_ctx()
      && search_ctx_->get_async_query_ctx()->is_group_alignment_enabled()
      && partition->get_storage_group_ctx().get_group_count() > 1
      && OB_SUCCESS == ObDASAsyncIndexQueryCtx::analyze_async_index_domain(filter_rtdef_->ctdef_, analysis)
      && (ASYNC_INDEX_DOMAIN_SINGLE == analysis.type_ || ASYNC_INDEX_DOMAIN_MULTI == analysis.type_);
}

int ObDASVecIndexAsyncIVFScanIter::prepare_group_pre_filter(int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (!can_group_pre_filter()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF group PRE context", K(ret));
  } else if (OB_FAIL(group_filter_.prepare(*filter_rtdef_, *search_ctx_))) {
    LOG_WARN("failed to prepare IVF group PRE", K(ret));
  } else if (group_filter_.count() > INT64_MAX) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF group PRE count exceeds signed range", K(ret), "count", group_filter_.count());
  } else {
    // Sum of per-Group cardinalities, not partition-wide distinct VIDs.
    count = group_filter_.count();
    group_ready_ = true;
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::abort_partition_pre_filter_build()
{
  int ret = OB_SUCCESS;
  if (opened_ || group_ready_ || nullptr != query_.pre_filter_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot abort IVF PRE bitmap after scanner preparation", K(ret), K_(opened), K_(group_ready));
  } else {
    // query_.pre_filter_ also blocks rollback after a partial open failure.
    // The member keeps its stable address; invalidate the binding before clearing
    // its representation. No Segment scanner may borrow it during this rollback.
    pre_filter_.reset();
    pre_filter_bitmap_.reset();
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::prepare_partition_pre_filter(ObIvfPreFilterBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  bitmap = nullptr;
  if (VEC_INDEX_PRE != vec_index_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot prepare async IVF PRE bitmap in this mode", K(ret), K_(vec_index_type));
  } else if (opened_ || group_ready_ || pre_filter_.is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("async IVF PRE filter is already prepared", K(ret), K_(opened), K_(group_ready));
  } else if (OB_FAIL(pre_filter_.init(pre_filter_bitmap_))) {
    LOG_WARN("failed to bind IVF PRE bitmap", K(ret));
  } else {
    bitmap = &pre_filter_bitmap_;
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::open_epoch()
{
  int ret = OB_SUCCESS;
  const auto *partition = search_ctx_->get_current_async_partition();
  int64_t index_slot = -1;
  ObTabletID delete_tablet;
  if (nullptr == partition || nullptr == ctdef_->get_delete_scan_ctdef()
      || nullptr == rtdef_->get_index_scan_rtdef() || nullptr == rtdef_->get_delete_scan_rtdef()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing async IVF partition or scan definitions", K(ret), KP(partition), KP_(ctdef), KP_(rtdef));
  } else if (OB_FAIL(partition->find_index_tablet_idx(ctdef_->get_index_scan_ctdef()->ref_table_id_, index_slot))) {
    LOG_WARN("failed to find IVF catalog slot", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(partition->ls_id_,
      partition->index_tablet_ids_.at(index_slot), ctdef_->get_index_scan_ctdef(),
      rtdef_->get_index_scan_rtdef(), tx_desc_, snapshot_, scan_param_, false, &alloc_))) {
    LOG_WARN("failed to initialize IVF scan parameters", K(ret));
  } else if (OB_FAIL(search_ctx_->get_related_tablet_id(ctdef_->get_delete_scan_ctdef(), delete_tablet))) {
    LOG_WARN("failed to resolve IVF shared delete Tablet", K(ret));
  } else if (OB_FAIL(search_ctx_->init_scan_param(delete_tablet, ctdef_->get_delete_scan_ctdef(),
      rtdef_->get_delete_scan_rtdef(), delete_param_, false))) {
    LOG_WARN("failed to initialize IVF deletion scan", K(ret));
  } else if (OB_FAIL(storage::ObDeletionBitmapProvider::get_or_build_bitmap(delete_tablet,
      partition->get_read_snapshot().get_val_for_tx(), alloc_, delete_param_,
      *ctdef_->get_delete_scan_ctdef(), *rtdef_->get_delete_scan_rtdef(), deletion_))) {
    LOG_WARN("failed to pin IVF shared deletion bitmap", K(ret));
  } else {
    query_.deletion_bitmap_handle_ = &deletion_;
    // Both POST paths can refill candidates, including POST without a filter.
    // PRE searches each Segment once, as in the SQL path.
    query_.iterative_centroids_ = vec_index_type_ == VEC_INDEX_POST_WITHOUT_FILTER
        || vec_index_type_ == VEC_INDEX_POST_ITERATIVE_FILTER;
    if (VEC_INDEX_PRE == vec_index_type_ && !group_ready_) {
      if (!pre_filter_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async IVF PRE bitmap was not prepared", K(ret));
      }
      else { query_.pre_filter_ = &pre_filter_; }
    }
    if (OB_SUCC(ret)) {
      root_.set_profile(profile_, need_post_filter_);
      ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_SCN,
                            partition->get_read_snapshot().get_val_for_tx());
    }
    if (OB_SUCC(ret) && OB_FAIL(root_.open(*partition, ctdef_->get_index_scan_ctdef()->ref_table_id_,
        scan_param_, query_, group_ready_ ? &group_filter_.filters() : nullptr))) {
      LOG_WARN("failed to open IVF query core", K(ret));
    }
    if (OB_SUCC(ret)) { opened_ = true; }
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::publish(const ObIArray<ObDASVectorCandidate> &candidates,
    bool quantized_only)
{
  int ret = OB_SUCCESS;
  // The driver merges filtered POST batches after refinement.
  const bool merge_topk = !need_post_filter_;
  ObIvfKnnSearchResult best(alloc_);
  reset_result();
  if (merge_topk && OB_FAIL(ObDASAsyncIVFTopKMergeOp::collect_topk(candidates, limit_, alloc_, best))) {
    LOG_WARN("failed to merge IVF topk", K(ret));
  }
  const int64_t count = merge_topk ? best.count() : candidates.count();
  if (OB_SUCC(ret)) {
    ObIAllocator &result_alloc = need_post_filter_ ? static_cast<ObIAllocator &>(post_batch_alloc_) : alloc_;
    void *buf = result_alloc.alloc(sizeof(share::ObVectorQueryVidIterator));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async IVF result iterator", K(ret), K(count));
    }
    else {
      vid_iter_ = new (buf) share::ObVectorQueryVidIterator(0, 0, 0, nullptr);
      if (OB_FAIL(vid_iter_->init(std::max<int64_t>(1, count), &result_alloc))) { LOG_WARN("failed to allocate IVF result", K(ret)); }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const auto &entry = merge_topk ? best.at(i) : candidates.at(i).entry_;
        // The filter consumes only VIDs. Approximate distances must not be
        // converted, scored or compared with the similarity threshold.
        double distance = 0;
        bool accepted = true;
        if (!quantized_only) {
          distance = entry.distance_;
          if (share::VIDA_L2 == query_.expected_dist_type_) {
            if (distance < -1e-6) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("negative async IVF squared L2 distance", K(ret), K(entry));
            } else {
              distance = std::sqrt(std::max(0.0, distance));
            }
          } else if (share::VIDA_IP == query_.expected_dist_type_) {
            distance -= 1;
          }
          if (OB_SUCC(ret) && ctdef_->query_param_.is_set_similarity_threshold_
              && ctdef_->query_param_.similarity_threshold_ > 0) {
            double score = 0;
            if (OB_FAIL(distance_to_score(ctdef_->sort_expr_, distance, score))) {
              LOG_WARN("failed to calculate IVF similarity", K(ret));
            } else {
              accepted = score >= ctdef_->query_param_.similarity_threshold_;
            }
          }
        }
        if (OB_SUCC(ret) && accepted && OB_FAIL(vid_iter_->add_result(entry.vid_, distance, nullptr))) {
          LOG_WARN("failed to publish IVF candidate", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) { reset_result(); }
  return ret;
}

int64_t ObDASVecIndexAsyncIVFScanIter::calc_next_nprobe(int64_t matched_count) const
{
  const int64_t remaining_count = limit_ - matched_count;
  const int64_t refill_nprobe = static_cast<int64_t>(
      nprobe_ * static_cast<double>(remaining_count) / limit_ + 1);
  return std::min(nprobe_, std::max<int64_t>(1, refill_nprobe));
}

int ObDASVecIndexAsyncIVFScanIter::search_post_without_filter(
    ObIArray<ObDASVectorCandidate> &candidates)
{
  int ret = OB_SUCCESS;
  int64_t next_nprobe = nprobe_;
  ObSEArray<ObDASVectorCandidate, 16> round(OB_MALLOC_NORMAL_BLOCK_SIZE,
      ModulePageAllocator(ObMemAttr(MTL_ID(), "DASIVFTmp")));
  // Each round visits every active Segment. The root deduplicates VIDs across
  // Segments and rounds, so only new visible candidates count towards LIMIT.
  while (OB_SUCC(ret) && candidates.count() < limit_ && root_.has_more()) {
    if (OB_FAIL(root_.search_round(next_nprobe, round, query_.k_))) {
      LOG_WARN("failed to search unfiltered IVF round", K(ret), K(next_nprobe));
    } else if (OB_FAIL(append(candidates, round))) {
      LOG_WARN("failed to retain unfiltered IVF candidates", K(ret));
    } else if (candidates.count() < limit_) {
      next_nprobe = calc_next_nprobe(candidates.count());
    }
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  UNUSED(capacity);
  int ret = OB_SUCCESS;
  count = 0;
  if (round_ready_) { ret = OB_ITER_END; }
  else {
    ScopedTimer total_timer(ObMetricId::VEC_TOTAL_SEARCH_TIME, profile_);
    if (need_post_filter_) {
      reset_result();
      post_batch_alloc_.reuse();
    }
    ObIAllocator &candidate_alloc = need_post_filter_ ? static_cast<ObIAllocator &>(post_batch_alloc_) : alloc_;
    ObSEArray<ObDASVectorCandidate, 16> candidates(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(candidate_alloc));
    if (!opened_ && OB_FAIL(open_epoch())) { LOG_WARN("failed to open IVF scan epoch", K(ret)); }
    if (OB_SUCC(ret) && go_brute_force_) {
      ret = root_.search_brute_force(candidates);
    } else if (OB_SUCC(ret) && need_post_filter_) {
      ret = root_.next_post_filter_batch(next_nprobe_, candidates, post_round_end_);
    } else if (OB_SUCC(ret) && vec_index_type_ == VEC_INDEX_POST_WITHOUT_FILTER) {
      ret = search_post_without_filter(candidates);
    } else if (OB_SUCC(ret)) {
      ret = root_.search_round(nprobe_, candidates, query_.k_);
    }
    if (OB_SUCC(ret) && OB_FAIL(publish(candidates, need_post_filter_ /* quantized_only */))) {
      LOG_WARN("failed to publish IVF round", K(ret));
    }
    if (OB_SUCC(ret)) {
      round_ready_ = true;
      count = nullptr == vid_iter_ ? 0 : vid_iter_->get_total();
      // Both are M_FIRST_VAL: record once, after the driver has fixed the
      // partition path. set_selectivity() may have run earlier with a default.
      ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_IS_BRUTE_FORCE, go_brute_force_);
      if (selectivity_ > 0) {
        const int64_t selectivity_scale = 100; // integer percent
        const double bounded = selectivity_ > 1.0 ? 1.0 : selectivity_;
        ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_EST_SELECTIVITY,
                              static_cast<int64_t>(bounded * selectivity_scale));
      }
    }
    else {
      reset_result();
      root_.close();
      group_filter_.reset();
      pre_filter_.reset();
      pre_filter_bitmap_.reset();
      opened_ = false; group_ready_ = false;
    }
  }
  record_profile_finish(ret);
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::refine_post_filter_candidates(const ObIvfKnnSearchResult &selected)
{
  int ret = OB_SUCCESS;
  ScopedTimer total_timer(ObMetricId::VEC_TOTAL_SEARCH_TIME, profile_);
  reset_result();
  post_batch_alloc_.reuse();
  ObSEArray<ObDASVectorCandidate, 16> refined(
      OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(post_batch_alloc_));
  if (OB_FAIL(root_.refine_candidates(selected, refined))) {
    LOG_WARN("failed to refine filtered async IVF candidates", K(ret));
  } else if (OB_FAIL(publish(refined, false /* quantized_only */))) {
    LOG_WARN("failed to publish refined async IVF candidates", K(ret));
  }
  return ret;
}

int ObDASVecIndexAsyncIVFScanIter::adjust_search_for_next_round(bool first_search,
    share::ObVectorQueryVidIterator *iter, int64_t unfiltered, int64_t added,
    int64_t scanned, bool &stop)
{
  // Here added is the cumulative accepted count, not this round's delta.
  UNUSED(iter);
  UNUSED(unfiltered);
  UNUSED(scanned);
  if (first_search) {
    stop = false;
    return OB_SUCCESS;
  }
  if (!need_post_filter_) {
    // Ordinary POST completes its refill before publishing any result.
    stop = true;
    return OB_SUCCESS;
  }
  const bool round_end = post_round_end_ || scan_budget_.should_stop();
  stop = round_end && (added >= limit_ || !root_.has_more() || scan_budget_.is_exhausted());
  if (!stop) {
    if (round_end) {
      next_nprobe_ = calc_next_nprobe(added);
      scan_budget_.allow_batch_stop_ = share::ObVecIdxQueryStrategy::LATENCY_FIRST == strategy_;
    }
    round_ready_ = false;
  }
  return OB_SUCCESS;
}
void ObDASVecIndexAsyncIVFScanIter::reset_result()
{
  if (nullptr != vid_iter_) {
    vid_iter_->reset();
    vid_iter_->~ObVectorQueryVidIterator();
    vid_iter_ = nullptr;
  }
}
void ObDASVecIndexAsyncIVFScanIter::reset_epoch()
{
  root_.close();
  group_filter_.reset();
  reset_result();
  pre_filter_.reset(); deletion_.reset();
  pre_filter_bitmap_.reset();
  ObDasVecScanUtils::release_scan_param(scan_param_);
  ObDasVecScanUtils::release_scan_param(delete_param_);
  effective_snapshot_.reset(); query_.reset(); query_vector_.reset(); stats_.reset();
  opened_ = false; round_ready_ = false; group_ready_ = false;
  post_round_end_ = false;
  scan_budget_ = ObIvfScanBudget(0);
  post_batch_alloc_.reuse();
  alloc_.reuse();
}
} // namespace sql
} // namespace oceanbase
