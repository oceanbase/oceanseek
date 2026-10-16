/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/iter/ob_das_vec_index_brute_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_hnsw_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_sort.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "share/logical_table/ob_logical_table_range_builder.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASVecIndexDriverCtDef, ObDASAttachCtDef),
                    sort_expr_,
                    limit_expr_,
                    offset_expr_,
                    vec_index_param_,
                    query_param_,
                    vector_index_param_,
                    algorithm_type_,
                    vec_type_,
                    row_count_,
                    dim_,
                    filter_mode_,
                    flags_);
OB_SERIALIZE_MEMBER((ObDASVecIndexDriverRtDef, ObDASAttachRtDef));

void ObDASVecIndexDriverIter::fill_related_tablet_ids(
    ObDASRelatedTabletID &related_tablet_ids) const
{
  related_tablet_ids.delta_buf_tablet_id_ = delta_buf_tablet_id_;
  related_tablet_ids.index_id_tablet_id_ = index_id_tablet_id_;
  related_tablet_ids.snapshot_tablet_id_ = snapshot_tablet_id_;
  related_tablet_ids.lookup_tablet_id_ = com_aux_vec_tablet_id_;
  if (is_ivf_index()) {
    // IVF auxiliary tablets belong to the scan, and change on partition rescan.
    static_cast<const ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_)
        ->fill_related_tablet_ids(related_tablet_ids);
  }
}

int ObDASVecIndexDriverIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *my_profile = nullptr;  
  if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_VEC_ITER, my_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec index scan iter or filter iter is null", K(ret));
  } else {
    profile_ = my_profile;
    common::ObProfileSwitcher switcher(my_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_INDEX_PATH, static_cast<uint64_t>(get_vec_index_path_type()));
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_PARTITION_ROW_COUNT, static_cast<uint64_t>(last_partition_row_count_));
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_FILTER_EST_ROW_COUNT, static_cast<uint64_t>(last_filter_est_row_count_));
    if (OB_FAIL(vec_index_scan_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    } else if (OB_NOT_NULL(pre_filter_iter_) && OB_FAIL(pre_filter_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan on pre filter iter", K(ret));
    } else if (OB_NOT_NULL(post_filter_iter_) && OB_FAIL(post_filter_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan on post filter iter", K(ret));
    }
  }
  
  return ret;
}

int ObDASVecIndexDriverIter::rescan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *my_profile = nullptr;
  
  if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_VEC_ITER, my_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec index scan iter or filter iter is null", K(ret));
  } else {
    profile_ = my_profile;
    common::ObProfileSwitcher switcher(my_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_INDEX_PATH, static_cast<uint64_t>(get_vec_index_path_type()));
    if (OB_FAIL(vec_index_scan_iter_->rescan())) {
      LOG_WARN("failed to rescan vec index scan iter", K(ret));
    } else if (!is_brute_force_scan() && OB_FAIL(evaluate_partition_path())) {
      LOG_WARN("failed to evaluate partition path for next partition", K(ret));
    } else if (OB_NOT_NULL(filter_iter_) && OB_FAIL(filter_iter_->rescan())) {
      LOG_WARN("failed to rescan filter iter", K(ret));
    } else {
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_INDEX_PATH, static_cast<uint64_t>(get_vec_index_path_type()));
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_PARTITION_ROW_COUNT, static_cast<uint64_t>(last_partition_row_count_));
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_FILTER_EST_ROW_COUNT, static_cast<uint64_t>(last_filter_est_row_count_));
    }
  }

  return ret;
}

void ObDASVecIndexDriverIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(vec_index_scan_iter_)) {
    vec_index_scan_iter_->clear_evaluated_flag();
  }
  if (OB_NOT_NULL(filter_iter_)) {
    filter_iter_->clear_evaluated_flag();
  }
}

int ObDASVecIndexDriverIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for vec index driver iter", K(ret), K(param));
  } else {
    ObDASVecIndexDriverIterParam &vec_index_driver_param = static_cast<ObDASVecIndexDriverIterParam &>(param);
    ls_id_ = vec_index_driver_param.ls_id_;
    tx_desc_ = vec_index_driver_param.tx_desc_;
    snapshot_ = vec_index_driver_param.snapshot_;
    vec_index_scan_iter_ = vec_index_driver_param.vec_index_scan_iter_;
    filter_iter_ = vec_index_driver_param.filter_iter_;
    pre_filter_iter_ = vec_index_driver_param.pre_filter_iter_;
    post_filter_iter_ = vec_index_driver_param.post_filter_iter_;
    vec_index_driver_ctdef_ = vec_index_driver_param.vec_index_driver_ctdef_;
    vec_index_driver_rtdef_ = vec_index_driver_param.vec_index_driver_rtdef_;
    vec_index_type_ = vec_index_driver_param.vec_index_type_;
    vec_idx_try_path_ = vec_index_driver_param.vec_idx_try_path_;
    sort_expr_ = vec_index_driver_param.sort_expr_;
    limit_expr_ = vec_index_driver_param.limit_expr_;
    offset_expr_ = vec_index_driver_param.offset_expr_;
    dim_ = vec_index_driver_param.dim_;
    search_ctx_ = vec_index_driver_param.search_ctx_;
    score_expr_ = vec_index_driver_param.score_expr_;
    filter_mode_ = vec_index_driver_param.filter_mode_;
    scalar_scan_ctdef_ = vec_index_driver_param.scalar_scan_ctdef_;
    scalar_scan_rtdef_ = vec_index_driver_param.scalar_scan_rtdef_;
    filter_rtdef_for_reeval_ = vec_index_driver_param.filter_rtdef_for_reeval_;
    go_brute_force_ = vec_index_driver_param.go_brute_force_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "VECSEARCH", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector search memory context", K(ret));
      } else {
        bitmap_.set_allocator(&mem_context_->get_arena_allocator());
      }
    }

    if (FAILEDx(init_limit_param())) {
      LOG_WARN("failed to init limit param", K(ret));
    } else if (OB_FAIL(init_sort())) {
      LOG_WARN("failed to init sort", K(ret));
    }
    if (OB_SUCC(ret) && !is_brute_force_scan() && OB_FAIL(evaluate_partition_path())) {
      LOG_WARN("failed to evaluate partition path for first partition", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::init_sort()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret));
  } else if (sort_expr_->is_vector_sort_expr()) {
    if (sort_expr_->arg_cnt_ != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg num", K(ret), K(sort_expr_->arg_cnt_));
    } else if (sort_expr_->args_[0]->is_const_expr()) {
      search_vec_ = sort_expr_->args_[0];
    } else if (sort_expr_->args_[1]->is_const_expr()) {
      search_vec_ = sort_expr_->args_[1];
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::init_limit_param()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(limit_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit expr or offset expr is null", K(ret));
  } else {
    bool is_null = false;
    ObDatum *limit_datum = nullptr;
    ObDatum *offset_datum = nullptr;
    if (OB_FAIL(limit_expr_->eval(*vec_index_driver_rtdef_->eval_ctx_, limit_datum))) {
      LOG_WARN("failed to eval limit expr", K(ret));
    } else if (limit_datum->is_null()) {
      is_null = true;
      limit_param_.limit_ = 0;
    } else {
      limit_param_.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(offset_expr_)) {
      if (!is_null && OB_FAIL(offset_expr_->eval(*vec_index_driver_rtdef_->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param_.offset_ = 0;
      } else {
        limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }

    if (OB_SUCC(ret)) {
      int64_t max_query_result_size = vec_index_scan_iter_->get_max_query_result_size();
      if (max_query_result_size != INT64_MAX &&
          limit_param_.offset_ + limit_param_.limit_ > max_query_result_size) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "query size (limit + offset) is more than max query result size", K(limit_param_));
      } else {
        vec_index_scan_iter_->set_limit(limit_param_.limit_+limit_param_.offset_);
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  profile_ = nullptr;

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(vec_index_scan_iter_) && OB_FAIL(vec_index_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse vec index scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(pre_filter_iter_) && OB_FAIL(pre_filter_iter_->reuse())) {
    LOG_WARN("failed to reuse pre filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(post_filter_iter_) && OB_FAIL(post_filter_iter_->reuse())) {
    LOG_WARN("failed to reuse post filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  // memory reset
  if (vid_to_distance_.created()) {
    vid_to_distance_.destroy();
  } 

  query_cond_built_ = false;
  bitmap_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  vec_op_alloc_.reset();

  // reset iterative filter state
  iter_unfiltered_vid_cnt_ = 0;
  iter_added_cnt_ = 0;
  iter_scan_total_num_ = 0;
  ready_to_output_ = false;

  last_partition_row_count_ = 0;
  last_filter_est_row_count_ = 0;

  // must be after memory reset
  if (OB_SUCC(ret) && OB_FAIL(set_vec_index_param(vec_index_driver_ctdef_->vec_index_param_))) {
    LOG_WARN("failed to set vec index param", K(ret));
  }
  
  return ret;
}

int ObDASVecIndexDriverIter::inner_release()
{
  int ret = OB_SUCCESS;
  profile_ = nullptr;

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(vec_index_scan_iter_) && OB_FAIL(release_input_iter(vec_index_scan_iter_))) {
    LOG_WARN("failed to release vec index scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(pre_filter_iter_) && OB_FAIL(release_input_iter(pre_filter_iter_))) {
    LOG_WARN("failed to release pre filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(post_filter_iter_) && OB_FAIL(release_input_iter(post_filter_iter_))) {
    LOG_WARN("failed to release post filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  vec_index_scan_iter_ = nullptr;
  filter_iter_ = nullptr;
  pre_filter_iter_ = nullptr;
  post_filter_iter_ = nullptr;

  // memory reset
  if (vid_to_distance_.created()) {
    vid_to_distance_.destroy();
  }

  query_cond_built_ = false;
  bitmap_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  vec_op_alloc_.reset();

  return ret;
}

int ObDASVecIndexDriverIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

void ObDASVecIndexDriverIter::switch_to_pre_filter()
{
  vec_index_type_ = ObVecIndexType::VEC_INDEX_PRE;
  filter_iter_ = pre_filter_iter_;
  filter_mode_ = ObVecFilterMode::VEC_FILTER_MODE_PRE_FILTER;
}

void ObDASVecIndexDriverIter::switch_to_post_filter()
{
  vec_index_type_ = ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER;
  filter_iter_ = post_filter_iter_;
  filter_mode_ = OB_NOT_NULL(scalar_scan_rtdef_)
                     ? ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER
                     : ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER;
}

int ObDASVecIndexDriverIter::refresh_bruteforce_fallback_threshold(int64_t partition_row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_index_driver_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null vec index driver ctdef", K(ret));
  } else {
    bruteforce_fallback_threshold_ = ObVecIdxExtraInfo::get_hnsw_bruteforce_fallback_threshold(
        vec_index_driver_ctdef_->query_param_, partition_row_count);
    LOG_TRACE("refresh hybrid hnsw bruteforce fallback threshold",
              K(partition_row_count), K_(bruteforce_fallback_threshold));
  }
  return ret;
}

void ObDASVecIndexDriverIter::update_go_brute_force_after_pre_filter()
{
  const ObKnnFilterMode knn_filter_mode = OB_NOT_NULL(vec_index_driver_ctdef_)
      ? vec_index_driver_ctdef_->filter_mode_
      : ObKnnFilterMode::INVALID_KNN_FILTER_MODE;
  const int64_t actual_filter_cnt = bitmap_.get_valid_cnt();
  const bool can_brute_force = actual_filter_cnt <= bruteforce_fallback_threshold_;

  go_brute_force_ = (ObKnnFilterMode::PRE_KNN != knn_filter_mode) && can_brute_force;
  last_filter_est_row_count_ = actual_filter_cnt;
  if (OB_NOT_NULL(vec_index_scan_iter_)) {
    vec_index_scan_iter_->set_vec_index_type(vec_index_type_, vec_idx_try_path_, go_brute_force_);
  }
  LOG_TRACE("update go_brute_force by actual pre filter row count",
            K(knn_filter_mode), K(go_brute_force_), K_(bruteforce_fallback_threshold),
            K(actual_filter_cnt), K(bitmap_.type_));
}

int ObDASVecIndexDriverIter::decide_ivf_partition_path_(
    int64_t filter_est_row_count,
    int64_t partition_row_count,
    ObIvfPathDecision &decision) const
{
  int ret = OB_SUCCESS;
  const int64_t safe_filter_est_row_count =
      filter_est_row_count < 0 ? 0 : filter_est_row_count;
  decision = ObIvfPathDecision();
  if (partition_row_count <= 0) {
    LOG_WARN("invalid IVF partition row count, fall back to iterative post filter",
             K(partition_row_count), K(filter_est_row_count));
  } else {
    const double selectivity = static_cast<double>(safe_filter_est_row_count)
                               / static_cast<double>(partition_row_count);
    decision.selectivity_ = selectivity > 1.0 ? 1.0 : selectivity;
    if (safe_filter_est_row_count
        <= static_cast<int64_t>(ObDASVecIndexIVFBaseScanIter::MAX_IVF_BRUTE_FORCE_SIZE)) {
      decision.vec_index_type_ = ObVecIndexType::VEC_INDEX_PRE;
      decision.go_brute_force_ = true;
    } else if (decision.selectivity_ > ObVecIdxExtraInfo::DEFAULT_SELECTIVITY_RATE) {
      // Filtering a bounded unfiltered TopK heap can discard all but a few
      // candidates. Iterative filtering keeps searching until TopK is filled.
      decision.vec_index_type_ = ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER;
    } else {
      decision.vec_index_type_ = ObVecIndexType::VEC_INDEX_PRE;
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::apply_ivf_partition_path_(const ObIvfPathDecision &decision)
{
  int ret = OB_SUCCESS;
  const bool choose_pre = ObVecIndexType::VEC_INDEX_PRE == decision.vec_index_type_;
  if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF path apply has null vec index scan iter", K(ret),
             "decision_vec_index_type", decision.vec_index_type_,
             K_(vec_index_type), K_(vec_idx_try_path), K_(filter_mode));
  } else if (choose_pre && OB_ISNULL(pre_filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF PRE path selected but pre filter iter is null", K(ret),
             "decision_vec_index_type", decision.vec_index_type_,
             K_(vec_index_type), K_(vec_idx_try_path), K_(filter_mode),
             "ctdef_filter_mode", vec_index_driver_ctdef_->filter_mode_);
  } else if (!choose_pre && OB_ISNULL(post_filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF POST path selected but post filter iter is null", K(ret),
             "decision_vec_index_type", decision.vec_index_type_,
             K_(vec_index_type), K_(vec_idx_try_path), K_(filter_mode),
             "ctdef_filter_mode", vec_index_driver_ctdef_->filter_mode_);
  } else {
    vec_index_type_ = decision.vec_index_type_;
    go_brute_force_ = decision.go_brute_force_;
    if (choose_pre) {
      filter_iter_ = pre_filter_iter_;
      filter_mode_ = ObVecFilterMode::VEC_FILTER_MODE_PRE_FILTER;
    } else {
      filter_iter_ = post_filter_iter_;
      filter_mode_ = OB_NOT_NULL(scalar_scan_rtdef_)
                         ? ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER
                         : ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER;
    }
    vec_index_scan_iter_->set_vec_index_type(vec_index_type_, vec_idx_try_path_, go_brute_force_);
    ObDASVecIndexIVFBaseScanIter *ivf_iter =
        static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_);
    const ObIvfFilterMode ivf_filter_mode =
        ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER == vec_index_type_
            ? ObIvfFilterMode::IVF_FILTER_MODE_ITER_FILTER
            : (ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER == vec_index_type_
                   ? ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER
                   : ObIvfFilterMode::IVF_FILTER_MODE_PRE_FILTER);
    // Reused IVF iterators retain center/expansion state. Bind the selected mode and
    // clear that state as one operation before the new partition starts producing rows.
    ivf_iter->prepare_partition_path(ivf_filter_mode, decision.selectivity_);
    query_cond_built_ = false;
  }
  return ret;
}

int ObDASVecIndexDriverIter::evaluate_partition_path()
{
  int ret = OB_SUCCESS;
  ObDASSearchCost p_cost;
  const bool user_forced_specific_pre =
      OB_NOT_NULL(vec_index_driver_ctdef_)
      && ObVecIndexType::VEC_INDEX_INVALID != vec_index_driver_ctdef_->vec_type_
      && ObKnnFilterMode::PRE_ADAPTIVE != vec_index_driver_ctdef_->filter_mode_
      && ObKnnFilterMode::POST_FILTER != vec_index_driver_ctdef_->filter_mode_
      && ObKnnFilterMode::POST_INDEX_MERGE != vec_index_driver_ctdef_->filter_mode_;
  if (is_ivf_index()) {
    const bool is_auto_ivf_path = OB_NOT_NULL(vec_index_driver_ctdef_)
                                  && ObVecIndexType::VEC_INDEX_INVALID
                                         == vec_index_driver_ctdef_->vec_type_
                                  && ObKnnFilterMode::PRE_ADAPTIVE
                                         != vec_index_driver_ctdef_->filter_mode_
                                  && ObKnnFilterMode::POST_FILTER
                                         != vec_index_driver_ctdef_->filter_mode_
                                  && ObKnnFilterMode::POST_INDEX_MERGE
                                         != vec_index_driver_ctdef_->filter_mode_;
    if (OB_ISNULL(search_ctx_) || OB_ISNULL(vec_index_driver_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null in IVF partition path evaluation", K(ret),
               KP(search_ctx_), KP(vec_index_driver_ctdef_));
    } else if (!is_auto_ivf_path || OB_ISNULL(filter_rtdef_for_reeval_)) {
      // Only the truly automatic IVF path is re-evaluated per partition.
      if (OB_ISNULL(filter_rtdef_for_reeval_) && search_ctx_->is_logical_table()) {
        if (OB_FAIL(search_ctx_->refresh_table_row_count())) {
          LOG_WARN("failed to refresh logical table partition row count for IVF", K(ret));
        } else if (search_ctx_->get_row_count().is_valid()) {
          last_partition_row_count_ = search_ctx_->get_row_count().cost();
        }
      }
    } else if (OB_FALSE_IT(filter_rtdef_for_reeval_->reset_cost_recursive())) {
    } else if (OB_FAIL(search_ctx_->refresh_table_row_count())) {
      LOG_WARN("failed to refresh IVF partition row count", K(ret));
    } else if (OB_FAIL(filter_rtdef_for_reeval_->get_cost(*search_ctx_, p_cost))) {
      LOG_WARN("failed to get IVF filter cost for current partition", K(ret));
    } else if (!p_cost.is_valid() || !search_ctx_->get_row_count().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF filter cost or partition row count", K(ret),
               K(p_cost), K(search_ctx_->get_row_count()));
    } else {
      const int64_t partition_row_count = search_ctx_->get_row_count().cost();
      ObIvfPathDecision decision;
      if (OB_FAIL(decide_ivf_partition_path_(
              p_cost.cost(), partition_row_count, decision))) {
        LOG_WARN("failed to decide IVF partition path", K(ret),
                 K(p_cost), K(partition_row_count));
      } else if (OB_FAIL(apply_ivf_partition_path_(decision))) {
        LOG_WARN("failed to apply IVF partition path", K(ret),
                 "decision_vec_index_type", decision.vec_index_type_,
                 K(decision.go_brute_force_), K(decision.selectivity_));
      } else {
        last_partition_row_count_ = partition_row_count;
        last_filter_est_row_count_ = p_cost.cost();
        LOG_TRACE("[IVF partition path]", K(partition_row_count),
                  "filter_est_row_count", p_cost.cost(),
                  K(decision.selectivity_),
                  "vec_index_type", decision.vec_index_type_,
                  K(decision.go_brute_force_),
                  "ctdef_filter_mode", vec_index_driver_ctdef_->filter_mode_);
      }
    }
  } else if (OB_ISNULL(search_ctx_) || OB_ISNULL(vec_index_driver_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null in evaluate_partition_path", K(ret),
             KP(search_ctx_), KP(vec_index_driver_ctdef_));
  } else if (user_forced_specific_pre) {
    if (OB_FAIL(search_ctx_->refresh_table_row_count())) {
      LOG_WARN("failed to refresh partition row count", K(ret));
    } else if (!search_ctx_->get_row_count().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition row count", K(ret), K(search_ctx_->get_row_count()));
    } else {
      const int64_t partition_row_count = search_ctx_->get_row_count().cost();
      if (OB_FAIL(refresh_bruteforce_fallback_threshold(partition_row_count))) {
        LOG_WARN("failed to refresh bruteforce fallback threshold", K(ret));
      } else {
        last_partition_row_count_ = partition_row_count;
        last_filter_est_row_count_ = 0;
      }
    }
  } else if (OB_ISNULL(filter_rtdef_for_reeval_)) {
    // No filter or a fixed POST path: there is no partition path to reevaluate.
    if (search_ctx_->is_logical_table()) {
      if (OB_FAIL(search_ctx_->refresh_table_row_count())) {
        LOG_WARN("failed to refresh logical table partition row count", K(ret));
      } else if (search_ctx_->get_row_count().is_valid()) {
        last_partition_row_count_ = search_ctx_->get_row_count().cost();
      }
    }
  } else if (OB_FALSE_IT(filter_rtdef_for_reeval_->reset_cost_recursive())) {
  } else if (OB_FAIL(search_ctx_->refresh_table_row_count())) {
    LOG_WARN("failed to refresh partition row count", K(ret));
  } else if (OB_FAIL(filter_rtdef_for_reeval_->get_cost(*search_ctx_, p_cost))) {
    LOG_WARN("failed to get filter cost for current partition", K(ret));
  } else if (!p_cost.is_valid() || !search_ctx_->get_row_count().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter cost or partition row count", K(ret), K(p_cost), K(search_ctx_->get_row_count()));
  } else {
    const int64_t partition_row_count = search_ctx_->get_row_count().cost();
    const bool user_forced_post = ObKnnFilterMode::POST_FILTER == vec_index_driver_ctdef_->filter_mode_
                                  || ObKnnFilterMode::POST_INDEX_MERGE == vec_index_driver_ctdef_->filter_mode_;
    if (user_forced_post) {
      last_partition_row_count_ = partition_row_count;
      last_filter_est_row_count_ = p_cost.cost();
    } else {
      const bool is_pre_adaptive = ObKnnFilterMode::PRE_ADAPTIVE == vec_index_driver_ctdef_->filter_mode_;
      ObVecIndexType new_type = is_pre_adaptive
          ? ObVecIndexType::VEC_INDEX_PRE
          : ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER;
      bool new_bf = false;
      if (OB_FAIL(refresh_bruteforce_fallback_threshold(partition_row_count))) {
        LOG_WARN("failed to refresh bruteforce fallback threshold", K(ret));
      } else if (p_cost.cost() <= bruteforce_fallback_threshold_) {
        new_type = ObVecIndexType::VEC_INDEX_PRE;
        new_bf = true;
      } else if (partition_row_count > 0
                 && static_cast<double>(p_cost.cost()) / static_cast<double>(partition_row_count)
                        <= ObVecIdxExtraInfo::get_pre_filter_threshold(vec_index_driver_ctdef_->query_param_)) {
        new_type = ObVecIndexType::VEC_INDEX_PRE;
      }

      const bool choose_pre = ObVecIndexType::VEC_INDEX_PRE == new_type;
      if (choose_pre && OB_ISNULL(pre_filter_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("PRE path chosen but pre_filter_iter_ is null", K(ret));
      } else if (!choose_pre && OB_ISNULL(post_filter_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("POST path chosen but post_filter_iter_ is null", K(ret));
      } else {
        const ObVecIndexType old_type = vec_index_type_;
        const bool old_bf = go_brute_force_;
        if (choose_pre && ObVecIndexType::VEC_INDEX_PRE != vec_index_type_) {
          switch_to_pre_filter();
        } else if (!choose_pre && ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER != vec_index_type_) {
          switch_to_post_filter();
        }
        go_brute_force_ = new_bf;
        last_partition_row_count_ = partition_row_count;
        last_filter_est_row_count_ = p_cost.cost();
        vec_index_scan_iter_->set_vec_index_type(vec_index_type_, vec_idx_try_path_, go_brute_force_);
        LOG_TRACE("partition path result", K(old_type), K(vec_index_type_),
                  K(old_bf), K(new_bf), K(p_cost.cost()), K(partition_row_count));
      }
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);

  if (!ready_to_output_) {
    if (limit_param_.limit_ + limit_param_.offset_ == 0) {
      ret = OB_ITER_END;
    } else if (!query_cond_built_ &&
               OB_FAIL(vec_index_scan_iter_->build_query_condition(
                   mem_context_->get_arena_allocator(), *vec_index_driver_rtdef_->eval_ctx_,
                   search_vec_, is_iter_filter(),
                   limit_param_.limit_ + limit_param_.offset_))) {
      LOG_WARN("failed to build query condition", K(ret));
    } else if (OB_FALSE_IT(query_cond_built_ = true)) {
    } else {
      common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_TOTAL_SEARCH_TIME);
      if (is_brute_force_scan()) {
        if (OB_FAIL(process_brute_force_mode(count, capacity))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to process brute force mode", K(ret));
        }
      } else if (is_pre_filter()) {
        if (OB_FAIL(process_pre_filter_mode(count, capacity))) {
          if (OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to process pre filter mode", K(ret));
          }
        }
      } else if (is_iter_filter() && is_ivf_index()) {
        if (OB_FAIL(process_iterative_filter_mode_ivf(count, capacity))) {
          LOG_WARN("failed to process deferred calc mode", K(ret));
        }
      } else if (is_iter_filter()) {
        if (OB_FAIL(process_iterative_filter_mode(count, capacity))) {
          if (OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to process iterative filter mode", K(ret));
          }
        }
      } else if (is_post_filter() && is_ivf_index()) {
        // For IVF post-filter (VEC_INDEX_POST_WITHOUT_FILTER), scan iter must walk
        // do_post_filter_round rather than do_post_filter_collect_round.
        // build_query_condition only knows PRE/ITER modes; explicitly set POST here.
        static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_)
            ->set_ivf_filter_mode(ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER);
        if (OB_FAIL(process_post_filter_mode_ivf(count, capacity))) {
          LOG_WARN("failed to process post filter iter mode for ivf", K(ret));
        }
      } else if (is_post_filter()) {
        if (OB_FAIL(process_post_filter_mode(count, capacity))) {
          if (OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to process post filter mode", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && limit_param_.limit_ + limit_param_.offset_ > 0) {
      if (OB_FAIL(truncate_result_adaptor_vid_iter_by_limit())) {
        LOG_WARN("failed to truncate result adaptor vid iter by limit", K(ret));
      }
    }
    ready_to_output_ = true;
  }

  if (OB_SUCC(ret)) {
    if (is_brute_force_scan() || is_pre_filter()) {
      share::ObVectorQueryVidIterator *adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
      if (OB_FAIL(fill_results_to_eval_ctx(adaptor_vid_iter, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    } else if (is_iter_filter()) {
      share::ObVectorQueryVidIterator *result_iter =
          OB_NOT_NULL(adaptor_vid_iter_) ? adaptor_vid_iter_
                                         : vec_index_scan_iter_->get_adaptor_vid_iter();
      if (OB_FAIL(fill_results_to_eval_ctx(result_iter, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    } else if (is_post_filter()) {
      ObVectorQueryVidIterator *adaptor_vid_iter = nullptr;
      if (is_ivf_index()) {
        if (OB_NOT_NULL(filter_iter_)) {
          adaptor_vid_iter = adaptor_vid_iter_;
          if (OB_ISNULL(adaptor_vid_iter)) {
            ret = OB_ITER_END;
          }
        } else {
          adaptor_vid_iter = OB_NOT_NULL(adaptor_vid_iter_)
              ? adaptor_vid_iter_
              : vec_index_scan_iter_->get_adaptor_vid_iter();
        }
      } else {
        adaptor_vid_iter = filter_iter_ != nullptr ? adaptor_vid_iter_ : vec_index_scan_iter_->get_adaptor_vid_iter();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fill_results_to_eval_ctx(adaptor_vid_iter, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    }
    INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  }

  return ret;
}

uint64_t ObDASVecIndexDriverIter::get_bitmap_init_size() const
{
  return is_ivf_index() ? MAX_BITMAP_INIT_SIZE
                        : static_cast<uint64_t>(bruteforce_fallback_threshold_);
}

int ObDASVecIndexDriverIter::build_bitmap_from_filter_iter()
{
  int ret = OB_SUCCESS;
  int64_t min_vid = 0;
  int64_t max_vid = INT64_MAX;

  // Pre-filter runs before scan iter get_next_rows. HNSW adaptor may not be acquired yet;
  // ObVecIndexBitmap::init requires min_vid/max_vid >= 0 (INT64_MIN is invalid).
  if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_index_scan_iter is null", K(ret));
  } else if (is_brute_force_scan()) {
    // Heap-table hidden PKs do not have an HNSW adaptor bound.  Keep the full
    // non-negative VID domain and let the bitmap choose a sparse representation.
  } else if (!is_ivf_index()) {
    ObDASVecIndexHNSWScanIter *hnsw_iter =
        static_cast<ObDASVecIndexHNSWScanIter *>(vec_index_scan_iter_);
    ObVidBound bound;
    share::ObPluginVectorIndexAdaptor *adaptor = nullptr;
    if (OB_FAIL(hnsw_iter->acquire_adaptor_if_needed())) {
      LOG_WARN("failed to acquire adaptor for vid bound", K(ret));
    } else if (OB_ISNULL(adaptor = hnsw_iter->get_adaptor())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("adaptor is null after acquire", K(ret));
    } else if (OB_FAIL(adaptor->get_vid_bound(bound))) {
      LOG_WARN("failed to get vid bound", K(ret));
    } else {
      min_vid = bound.min_vid_;
      max_vid = bound.max_vid_;
    }
  }

  if (OB_SUCC(ret)) {
    // A bitmap is shared with scan/search iterators after it is materialized.  Their
    // reuse/release hooks run before the driver resets the bitmap, so never free an
    // existing representation here.  Reject an unexpected second build instead.
    if (OB_UNLIKELY(0 != bitmap_.capacity_
                    || 0 != bitmap_.get_valid_cnt()
                    || OB_NOT_NULL(bitmap_.vids_))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("bitmap was not reset before materialization", K(ret), K(bitmap_.type_),
               K(bitmap_.capacity_), "valid_cnt", bitmap_.get_valid_cnt());
    } else if (OB_FAIL(bitmap_.init(min_vid, max_vid, get_bitmap_init_size()))) {
      LOG_WARN("failed to init bitmap", K(ret), K(min_vid), K(max_vid));
    }
  }

  ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
  int64_t batch_row_count = eval_ctx->max_batch_size_ > 0 ? 
                            min(eval_ctx->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) : 
                            ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  bool index_end = false;
  common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_FILTER_TIME);
  while (OB_SUCC(ret) && !index_end) {
    int64_t scan_row_cnt = 0;
    filter_iter_->clear_evaluated_flag();
    if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row.", K(ret));
      }
      index_end = true;
    }

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
    } else if (scan_row_cnt > 0) {
      ret = OB_SUCCESS;
      INC_METRIC_VAL(common::ObMetricId::HS_VEC_DRIVER_PRE_FILTER_ITER_SCANNED, static_cast<uint64_t>(scan_row_cnt));
    } 

    ObExpr *vid_expr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (scan_row_cnt == 0) {
    } else if (OB_ISNULL(search_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search ctx is null", K(ret));
    } else if (OB_UNLIKELY(search_ctx_->get_rowid_exprs().count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid exprs is empty", K(ret));
    } else if (OB_ISNULL(vid_expr = search_ctx_->get_rowid_exprs().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid expr is null", K(ret));
    } else if (vid_expr->enable_rich_format() && is_valid_format(vid_expr->get_format(*eval_ctx))) {
      ObIVector *vec = vid_expr->get_vector(*eval_ctx);
      if (OB_ISNULL(vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          int64_t vid = vec->get_uint64(i);
          if (OB_FAIL(bitmap_.add_vid(vid))) {
            LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
          }
        }
      }
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
      guard.set_batch_size(scan_row_cnt);
      ObDatum *vid_datums = vid_expr->locate_batch_datums(*eval_ctx);
      if (OB_ISNULL(vid_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vid datums is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          int64_t vid = vid_datums[i].get_uint64();
          if (OB_FAIL(bitmap_.add_vid(vid))) {
            LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  LOG_TRACE("[LT hybrid DAS] build_bitmap_from_filter_iter done",
           K(ret), "bitmap_valid_cnt", bitmap_.get_valid_cnt());
  return ret;
}

int ObDASVecIndexDriverIter::process_pre_filter_mode(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_bitmap_from_filter_iter())) {
    LOG_WARN("failed to build bitmap from filter iter", K(ret));
  } else if (bitmap_.get_valid_cnt() == 0) {
    ret = OB_ITER_END;
  } else {
    if (!is_ivf_index()) {
      update_go_brute_force_after_pre_filter();
    }
    // bitmap upgrade (VIDS → BYTE_ARRAY) is handled inside scan iter
    // (IVF: do_ivf_flat/pq_pre_filter; HNSW: init_pre_filter)
    common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_VEC_INDEX_TIME);
    vec_index_scan_iter_->set_bitmap(&bitmap_);
    vec_index_scan_iter_->clear_evaluated_flag();
    int64_t dummy_count = 0;
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        if (OB_SCHEMA_EAGAIN != ret) {
          LOG_WARN("failed to query with prefilter", K(ret));
        }
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  
  return ret;
}

// The brute force scan iter materializes the exact top-K in one pass.  For an indexed
// filter, build the matching VID set first and let the exact scan test membership.
int ObDASVecIndexDriverIter::process_brute_force_mode(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  UNUSED(count);
  ObVecBruteAccessMode access_mode = ObVecBruteAccessMode::WHOLE_RANGE_SCAN;
  ObVecBruteFallbackReason fallback_reason = ObVecBruteFallbackReason::NO_OR_EMPTY_FILTER;
  const ObDASVecIndexBruteScanCtDef *brute_scan_ctdef = nullptr;
  ObDASVecIndexBruteScanIter *brute_scan_iter = nullptr;
  bool filter_pushdown = false;
  int64_t actual_filter_cnt = 0;
  int64_t vid_span = 0;
  int64_t selectivity_base = 0;
  bool below_absolute_limit = false;
  bool below_selectivity_limit = false;
  bool use_partition_row_count = false;
  bool empty_filter = false;
  if (OB_ISNULL(vec_index_scan_iter_)
      || OB_ISNULL(vec_index_driver_ctdef_)
      || OB_UNLIKELY(vec_index_driver_ctdef_->children_cnt_ < 1)
      || OB_ISNULL(vec_index_driver_ctdef_->children_)
      || OB_ISNULL(vec_index_driver_ctdef_->children_[0])
      || OB_UNLIKELY(DAS_OP_VEC_INDEX_BRUTE_SCAN
                     != vec_index_driver_ctdef_->children_[0]->op_type_)
      || OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_INDEX_BRUTE_SCAN
                     != vec_index_scan_iter_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid brute force scan state", K(ret), KP(vec_index_scan_iter_),
             KP(vec_index_driver_ctdef_));
  } else {
    brute_scan_ctdef = static_cast<const ObDASVecIndexBruteScanCtDef *>(
        vec_index_driver_ctdef_->children_[0]);
    brute_scan_iter = static_cast<ObDASVecIndexBruteScanIter *>(vec_index_scan_iter_);
    filter_pushdown = brute_scan_ctdef->filter_pushdown_;
    brute_scan_iter->set_use_filtered_get(false);
    vec_index_scan_iter_->set_bitmap(nullptr);
    if (filter_pushdown && OB_ISNULL(filter_iter_)) {
      fallback_reason = ObVecBruteFallbackReason::SCALAR_FILTER_PUSHDOWN;
    }
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FILTER_ROWS, 0);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_ACCESS_MODE,
                   static_cast<uint64_t>(access_mode));
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FALLBACK_REASON,
                   static_cast<uint64_t>(fallback_reason));
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_MAIN_TABLE_ROWS, 0);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_DISTANCE_ROWS, 0);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_MAIN_TABLE_TIME, 0);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_DISTANCE_TIME, 0);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_OUTPUT_ROWS, 0);
    if (last_partition_row_count_ <= 0 && OB_NOT_NULL(search_ctx_)) {
      const int refresh_ret = search_ctx_->refresh_table_row_count();
      if (OB_SUCCESS == refresh_ret && search_ctx_->get_row_count().is_valid()) {
        last_partition_row_count_ = search_ctx_->get_row_count().cost();
      } else {
        LOG_TRACE("partition row count unavailable for brute force point get decision",
                  K(refresh_ret), K(search_ctx_->get_row_count()));
      }
    }
    if (OB_FAIL(refresh_bruteforce_fallback_threshold(last_partition_row_count_))) {
      LOG_WARN("failed to refresh brute force fallback threshold", K(ret),
               K_(last_partition_row_count));
    } else {
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_PARTITION_ROW_COUNT,
                     static_cast<uint64_t>(last_partition_row_count_));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(filter_iter_)) {
      if (OB_FAIL(build_bitmap_from_filter_iter())) {
        LOG_WARN("failed to build bitmap for brute force scan", K(ret));
      } else if (0 == bitmap_.get_valid_cnt()) {
        access_mode = ObVecBruteAccessMode::UNKNOWN;
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FILTER_ROWS, 0);
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_ACCESS_MODE,
                       static_cast<uint64_t>(access_mode));
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FALLBACK_REASON,
                       static_cast<uint64_t>(fallback_reason));
        empty_filter = true;
      } else if (ObVecIndexBitmap::ROARING_BITMAP != bitmap_.type_ &&
                 OB_FAIL(bitmap_.upgrade_to_roaring_bitmap())) {
        LOG_WARN("failed to upgrade brute force filter bitmap", K(ret));
      } else if (OB_FAIL(bitmap_.get_roaring_cardinality(actual_filter_cnt))) {
        LOG_WARN("failed to get brute force filter cardinality", K(ret));
      } else {
        below_absolute_limit = actual_filter_cnt <= bruteforce_fallback_threshold_;
        vid_span = bitmap_.max_vid_ >= bitmap_.min_vid_
            ? bitmap_.max_vid_ - bitmap_.min_vid_ + 1 : 0;
        use_partition_row_count = last_partition_row_count_ > 0;
        selectivity_base = use_partition_row_count ? last_partition_row_count_ : vid_span;
        below_selectivity_limit = selectivity_base > 0
            && static_cast<double>(actual_filter_cnt) / static_cast<double>(selectivity_base)
                   <= BRUTE_FORCE_POINT_GET_SELECTIVITY_THRESHOLD;
        if (!below_absolute_limit) {
          fallback_reason = ObVecBruteFallbackReason::OVER_ABSOLUTE_THRESHOLD;
        } else if (selectivity_base <= 0) {
          fallback_reason = ObVecBruteFallbackReason::STAT_UNAVAILABLE;
        } else if (!below_selectivity_limit) {
          fallback_reason = ObVecBruteFallbackReason::OVER_SELECTIVITY_THRESHOLD;
        } else {
          access_mode = ObVecBruteAccessMode::FILTERED_POINT_GET;
          fallback_reason = ObVecBruteFallbackReason::NONE;
        }
        if (ObVecBruteAccessMode::FILTERED_POINT_GET == access_mode) {
          vec_index_scan_iter_->set_bitmap(&bitmap_);
          brute_scan_iter->set_use_filtered_get(true);
        } else if (!filter_pushdown) {
          // Bitmap-only whole-range mode still needs exact membership checks. If the
          // complete predicate is already in the main-table scan, avoid the redundant
          // bitmap lookup while preserving the indexed point-get decision above.
          vec_index_scan_iter_->set_bitmap(&bitmap_);
        }
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FILTER_ROWS,
                       static_cast<uint64_t>(actual_filter_cnt));
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_ACCESS_MODE,
                       static_cast<uint64_t>(access_mode));
        SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FALLBACK_REASON,
                       static_cast<uint64_t>(fallback_reason));
      }
    } else if (OB_SUCC(ret)) {
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FILTER_ROWS, 0);
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_ACCESS_MODE,
                     static_cast<uint64_t>(access_mode));
      SET_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_FALLBACK_REASON,
                     static_cast<uint64_t>(fallback_reason));
    }
    if (OB_SUCC(ret)) {
      const int64_t access_mode_value = static_cast<int64_t>(access_mode);
      const int64_t fallback_reason_value = static_cast<int64_t>(fallback_reason);
      LOG_TRACE("choose brute force main table access path",
                K_(com_aux_vec_tablet_id), K_(dim), "k", limit_param_.limit_ + limit_param_.offset_,
                K(actual_filter_cnt), K_(last_partition_row_count), K(vid_span),
                K(selectivity_base), K(use_partition_row_count),
                "absolute_threshold", bruteforce_fallback_threshold_,
                "selectivity_threshold", BRUTE_FORCE_POINT_GET_SELECTIVITY_THRESHOLD,
                K(below_absolute_limit), K(below_selectivity_limit),
                K(filter_pushdown), K(access_mode_value), K(fallback_reason_value));
    }
    if (OB_SUCC(ret) && empty_filter) {
      ret = OB_ITER_END;
    }
    if (OB_SUCC(ret)) {
      common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_VEC_INDEX_TIME);
      int64_t dummy_count = 0;
      vec_index_scan_iter_->clear_evaluated_flag();
      if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to do brute force scan", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::set_bitmap_to_filter_iter()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(filter_iter_)) {
    ObDASSearchDriverIter *search_driver_iter = static_cast<ObDASSearchDriverIter*>(filter_iter_);
    search_driver_iter->set_bitmap(&bitmap_);
  }
  return ret;
}

int ObDASVecIndexDriverIter::process_iterative_filter_mode(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  bool end_search = false;
  bool first_search = true;
  bool stop_search = false;

  if (OB_FAIL(vec_index_scan_iter_->adjust_search_for_next_round(
          first_search, adaptor_vid_iter_,
          iter_unfiltered_vid_cnt_, iter_added_cnt_,
          iter_scan_total_num_, stop_search))) {
    LOG_WARN("failed to adjust search for first round", K(ret));
  } else if (stop_search) {
    end_search = true;
  }

  search_ctx_->set_is_vec_index_search(true);
  while (OB_SUCC(ret) && !end_search) {
    INC_METRIC_VAL(common::ObMetricId::HS_VEC_DRIVER_ITER_FILTER_ROUND, 1);
    if (OB_FAIL(fetch_vids_from_vec_index(count, capacity))) {
      if (OB_SCHEMA_EAGAIN != ret) {
        LOG_WARN("failed to fetch vids from vec index", K(ret));
      }
    } else if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
      if (OB_FAIL(post_query_vid_with_expr_filter())) {
        LOG_WARN("failed to post query vid with expr filter", K(ret));
      } else {
        INC_METRIC_VAL(common::ObMetricId::HS_VEC_DRIVER_ITER_TOTAL_ROW_COUNT, iter_added_cnt_);
      }
    } else {
      if (!first_search && OB_FAIL(filter_iter_->reuse())) {
        LOG_WARN("failed to reuse filter iter", K(ret));
      } else if (!first_search && OB_FAIL(filter_iter_->rescan())) {
        LOG_WARN("failed to rescan filter iter", K(ret));
      } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
        LOG_WARN("failed to set bitmap to filter iter", K(ret));
      } else if (OB_FAIL(post_query_vid_with_filter())) {
        LOG_WARN("failed to post query vid with filter", K(ret));
      } else if (iter_unfiltered_vid_cnt_ > 0) {
        INC_METRIC_VAL(common::ObMetricId::HS_VEC_DRIVER_ITER_TOTAL_ROW_COUNT, iter_added_cnt_);
      }
    }

    first_search = false;
    stop_search = false;
    if (FAILEDx(vec_index_scan_iter_->adjust_search_for_next_round(
            first_search, adaptor_vid_iter_,
            iter_unfiltered_vid_cnt_, iter_added_cnt_,
            iter_scan_total_num_, stop_search))) {
      LOG_WARN("failed to adjust search for next round", K(ret));
    } else if (stop_search) {
      end_search = true;
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  search_ctx_->set_is_vec_index_search(false);

  return ret;
}

int ObDASVecIndexDriverIter::fill_results_to_eval_ctx(share::ObVectorQueryVidIterator *adaptor_vid_iter, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(adaptor_vid_iter)) {
    ret = OB_ITER_END;
  } else if (!adaptor_vid_iter->is_init()) {
    ret = OB_ITER_END;
  } else {
    ObNewRow *row = nullptr;
    if (OB_FALSE_IT(adaptor_vid_iter->set_batch_size(capacity))) {
    } else if (OB_FAIL(adaptor_vid_iter->get_next_rows(row, count, vec_index_driver_ctdef_->result_output_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rows from adaptor vid iter", K(ret));
    } else if (count > 0) {
      ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
      const ExprFixedArray& res_exprs = vec_index_driver_ctdef_->result_output_;
      // For LT_AUX_IVF_CID_ROWID, IVF scan decodes logical rowids from KV keys before filling adaptor vids.

      // res_exprs size should be 1, which is vid_column
      if (res_exprs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_exprs count is not equal to 1", K(ret), K(res_exprs));
      } else {
        int64_t a_batch_obj_cnt = 2;
        ObExpr *vid_expr = res_exprs.at(0);
        if (OB_ISNULL(vid_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (vid_expr->enable_rich_format()) {
          ObIVector *vec = nullptr;
          if (OB_FAIL(vid_expr->init_vector_for_write(*eval_ctx, vid_expr->get_default_res_format(), count))) {
            LOG_WARN("failed to init vector for write", K(ret));
          } else if (OB_ISNULL(vec = vid_expr->get_vector(*eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vector is null", K(ret));
          } else {
            for (int64_t num = 0; OB_SUCC(ret) && num < count; ++num) {
              int64_t vid_val = row->get_cell(num * a_batch_obj_cnt + 1).get_int();
              vec->set_uint(num, vid_val);
            }
          }
        } else {
          ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
          guard.set_batch_size(count);
          ObDatum *vid_datum = nullptr;
          if (OB_ISNULL(vid_datum = vid_expr->locate_datums_for_update(*eval_ctx, count))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(vid_expr));
          } else {
            for (int64_t num = 0; OB_SUCC(ret) && num < count; ++num) {
              guard.set_batch_idx(num);
              if (OB_FAIL(vid_datum[num].from_obj(row->get_cell(num * a_batch_obj_cnt + 1)))) {
                LOG_WARN("fail to from obj", K(ret));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          vid_expr->set_evaluated_projected(*eval_ctx);
        }
      }

      if (FAILEDx(save_vec_socre_expr_result(row, count))) {
        LOG_WARN("failed to save vec score expr result", K(ret));
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::fetch_vids_from_vec_index(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_index_scan_iter is null", K(ret));
  } else {
    share::ObVectorQueryVidIterator *adaptor_vid_iter = nullptr;
    int64_t dummy_count = 0;
    vec_index_scan_iter_->clear_evaluated_flag();
    {
      common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_VEC_INDEX_TIME);
      if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
        if (OB_ITER_END != ret) {
          if (OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("failed to get next rows from vec index scan iter to trigger init", K(ret));
          }
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();

    if (OB_SUCC(ret) && OB_NOT_NULL(adaptor_vid_iter)) {
      int64_t total_count = adaptor_vid_iter->get_total();
      iter_unfiltered_vid_cnt_ = total_count;
      iter_scan_total_num_ += total_count;
      INC_METRIC_VAL(common::ObMetricId::HS_VEC_DRIVER_VID_SCANNED, static_cast<uint64_t>(total_count));

      bool need_bitmap = filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER 
                         && nullptr != filter_iter_;
      if (need_bitmap && total_count > 0) {
        const int64_t *vids = adaptor_vid_iter->get_vids();
        const float *distances = adaptor_vid_iter->get_distance();
        int64_t min_vid = INT64_MAX;
        int64_t max_vid = INT64_MIN;
        if (OB_ISNULL(vids) || OB_ISNULL(distances)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vids or distances is null", K(ret), KP(vids), KP(distances), K(total_count));
        } else if (is_ivf_index()) {
          for (int64_t i = 0; i < total_count; ++i) {
            min_vid = min(min_vid, vids[i]);
            max_vid = max(max_vid, vids[i]);
          }
        } else {
          ObVidBound bound;
          share::ObPluginVectorIndexAdaptor *adaptor = vec_index_scan_iter_->get_adaptor();
          if (OB_ISNULL(adaptor)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("adaptor is null", K(ret));
          } else if (OB_FAIL(adaptor->get_vid_bound(bound))) {
            LOG_WARN("failed to get vid bound", K(ret));
          } else {
            min_vid = bound.min_vid_;
            max_vid = bound.max_vid_;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(bitmap_.init(min_vid, max_vid, get_bitmap_init_size()))) {
          LOG_WARN("failed to init bitmap", K(ret));
        } else if (OB_FAIL(vid_to_distance_.create(total_count, "VidToDistMap"))) {
          LOG_WARN("failed to create vid to distance hash map", K(ret), K(total_count));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
            int64_t vid = vids[i];
            float distance = distances[i];

            if (OB_FAIL(bitmap_.add_vid(vid))) {
              LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
            } else if (OB_FAIL(vid_to_distance_.set_refactored(vid, distance))) {
              LOG_WARN("failed to set vid to distance mapping", K(ret), K(vid), K(distance));
            }
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::truncate_result_adaptor_vid_iter_by_limit()
{
  int ret = OB_SUCCESS;
  ObVectorQueryVidIterator *adaptor_vid_iter = nullptr;
  if (is_brute_force_scan() || is_pre_filter()) {
    adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
  } else if (is_iter_filter()) {
    adaptor_vid_iter = OB_NOT_NULL(adaptor_vid_iter_)
        ? adaptor_vid_iter_
        : vec_index_scan_iter_->get_adaptor_vid_iter();
  } else if (is_post_filter()) {
    if (is_ivf_index()) {
      adaptor_vid_iter = OB_NOT_NULL(adaptor_vid_iter_)
          ? adaptor_vid_iter_
          : vec_index_scan_iter_->get_adaptor_vid_iter();
    } else {
      adaptor_vid_iter = OB_NOT_NULL(filter_iter_)
          ? adaptor_vid_iter_
          : vec_index_scan_iter_->get_adaptor_vid_iter();
    }
  }

  if (OB_ISNULL(adaptor_vid_iter)) {
    // nothing to do
  } else {
    const uint64_t final_limit = limit_param_.limit_ + limit_param_.offset_;
    if (adaptor_vid_iter->get_total() > static_cast<int64_t>(final_limit)) {
      const int64_t total = adaptor_vid_iter->get_total();
      adaptor_vid_iter->set_total(final_limit);
      LOG_TRACE("truncate adaptor vid iter by limit", K(total), K(final_limit), K(vec_index_type_), K(vec_idx_try_path_));
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::init_adaptor_vid_iter_if_null()
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(adaptor_vid_iter_)) {
    void *iter_buff = nullptr;
    uint64_t base_cnt = limit_param_.limit_ + limit_param_.offset_;
    uint64_t final_res_cnt = vec_index_scan_iter_->get_expected_result_buffer_size(base_cnt);
    int64_t extra_info_actual_size = 0;
    int64_t extra_column_count = 0;

    if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate adaptor vid iter", K(ret));
    } else if (OB_FALSE_IT(adaptor_vid_iter_ = new(iter_buff)
                            ObVectorQueryVidIterator(extra_column_count, extra_info_actual_size,
                                                     0, nullptr))) {
    } else if (OB_FAIL(adaptor_vid_iter_->init(final_res_cnt, &vec_op_alloc_))) {
      LOG_WARN("adaptor vid iter init failed", K(ret));
    }
  }
  
  return ret;
}

int ObDASVecIndexDriverIter::post_query_vid_with_filter(ObIArray<VidDistancePair> *out_pairs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter_iter or adaptor is null", K(ret));
  } else if (OB_ISNULL(out_pairs) && OB_FAIL(init_adaptor_vid_iter_if_null())) {
    LOG_WARN("failed to init adaptor vid iter", K(ret));
  } else if (iter_unfiltered_vid_cnt_ > 0) {
    iter_added_cnt_ = 0;

    ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
    int64_t batch_row_count = eval_ctx->max_batch_size_ > 0 ?
                              min(eval_ctx->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) :
                              ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

    // When out_pairs is null, use a local buffer for sort+fill into adaptor_vid_iter_ (original behavior)
    VidDistancePair *local_pairs = nullptr;
    int64_t valid_total_cnt = 0;
    int64_t allocated_capacity = iter_unfiltered_vid_cnt_;

    if (OB_ISNULL(out_pairs)) {
      if (OB_ISNULL(local_pairs = static_cast<VidDistancePair *>(
                    mem_context_->get_arena_allocator().alloc(sizeof(VidDistancePair) * allocated_capacity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate local pairs", K(ret), K(allocated_capacity));
      }
    }

    if (OB_SUCC(ret)) {
      bool filter_end = false;
      {
        common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_FILTER_TIME);
        while (OB_SUCC(ret) && !filter_end) {
          int64_t scan_row_cnt = 0;
          filter_iter_->clear_evaluated_flag();

          if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from filter iter", K(ret));
            } else {
              filter_end = true;
              ret = OB_SUCCESS;
            }
          }

          if (OB_SUCC(ret) && scan_row_cnt > 0) {
            ObExpr *vid_expr = nullptr;
            if (OB_ISNULL(search_ctx_) || OB_UNLIKELY(search_ctx_->get_rowid_exprs().count() <= 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("search ctx or rowid exprs is invalid", K(ret));
            } else if (OB_ISNULL(vid_expr = search_ctx_->get_rowid_exprs().at(0))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("vid expr is null", K(ret));
            } else {
              ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
              guard.set_batch_size(scan_row_cnt);

              int64_t filtered_vid = 0;
              bool use_rich_format = vid_expr->enable_rich_format() && is_valid_format(vid_expr->get_format(*eval_ctx));
              ObIVector *vec = nullptr;
              ObDatum *vid_datums = nullptr;

              if (use_rich_format) {
                if (OB_ISNULL(vec = vid_expr->get_vector(*eval_ctx))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("vector is null", K(ret));
                }
              } else {
                if (OB_ISNULL(vid_datums = vid_expr->locate_batch_datums(*eval_ctx))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("vid datums is null", K(ret));
                }
              }

              if (OB_SUCC(ret)) {
                for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                  if (use_rich_format) {
                    filtered_vid = vec->get_uint64(i);
                  } else {
                    guard.set_batch_idx(i);
                    filtered_vid = vid_datums[i].get_uint64();
                  }

                  float distance = 0.0f;
                  if (OB_FAIL(vid_to_distance_.get_refactored(filtered_vid, distance))) {
                    LOG_WARN("failed to get distance from hash map", K(ret), K(filtered_vid));
                  } else {
                    if (OB_NOT_NULL(out_pairs)) {
                      VidDistancePair pair;
                      pair.vid = filtered_vid;
                      pair.distance = distance;
                      if (OB_FAIL(out_pairs->push_back(pair))) {
                        LOG_WARN("failed to push back pair", K(ret), K(filtered_vid), K(distance));
                      } else {
                        iter_added_cnt_++;
                      }
                    } else if (OB_UNLIKELY(valid_total_cnt >= allocated_capacity)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("valid total cnt exceeds capacity", K(ret), K(valid_total_cnt), K(allocated_capacity));
                    } else {
                      local_pairs[valid_total_cnt].vid = filtered_vid;
                      local_pairs[valid_total_cnt].distance = distance;
                      valid_total_cnt++;
                    }
                  }
                }
              }
            }
          }
        }
      }

      // When using local buffer (original path): sort and fill adaptor_vid_iter_
      if (OB_SUCC(ret) && OB_ISNULL(out_pairs) && valid_total_cnt > 0) {
        lib::ob_sort(local_pairs, local_pairs + valid_total_cnt, VidDistancePair::compare);

        for (int64_t i = 0; OB_SUCC(ret) && i < valid_total_cnt && !adaptor_vid_iter_->get_enough(); ++i) {
          if (OB_FAIL(adaptor_vid_iter_->add_result(local_pairs[i].vid, local_pairs[i].distance, nullptr))) {
            LOG_WARN("failed to add result to adaptor vid iter", K(ret), K(local_pairs[i].vid), K(local_pairs[i].distance));
          } else {
            iter_added_cnt_++;
          }
        }
      }

      bitmap_.reset();
      if (vid_to_distance_.created()) {
        vid_to_distance_.destroy();
      }
    }
  }

  return ret;
}


int ObDASVecIndexDriverIter::process_post_filter_mode_ivf(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  bool first_search = true;
  bool stop_search = false;
  bool end_search = false;
  bool fell_back_to_brute_force = false;
  int64_t cumulative_added = 0;
  ObDASVecIndexIVFBaseScanIter *ivf_iter =
      static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_);

  ObSEArray<VidDistancePair, 128> all_pairs;
  all_pairs.set_attr(ObMemAttr(MTL_ID(), "IvfPostPairs"));
  const int64_t top_k = limit_param_.limit_ + limit_param_.offset_;

  if (OB_FAIL(vec_index_scan_iter_->adjust_search_for_next_round(
          first_search, adaptor_vid_iter_,
          iter_unfiltered_vid_cnt_, iter_added_cnt_,
          iter_scan_total_num_, stop_search))) {
    LOG_WARN("failed to adjust search for first round", K(ret));
  } else if (stop_search) {
    end_search = true;
  }

  while (OB_SUCC(ret) && !end_search) {
    // Step 1: IVF scan iter scans current center batch → outputs vids+distances via adaptor_vid_iter
    int64_t dummy_count = 0;
    vec_index_scan_iter_->clear_evaluated_flag();
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rows from vec index scan iter", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    // Step 1.5: check if scan iter needs fallback to brute force (centroid empty)
    if (OB_SUCC(ret) && ivf_need_fallback_brute_force()) {
      fell_back_to_brute_force = true;
      if (OB_FAIL(fallback_to_brute_force(count, capacity))) {
        LOG_WARN("failed to fallback to brute force", K(ret));
      }
      break;
    }

    // Step 2: get scan iter's adaptor_vid_iter with candidate vids+distances
    share::ObVectorQueryVidIterator *scan_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
    if (OB_SUCC(ret) && OB_NOT_NULL(scan_vid_iter)) {
      int64_t total_count = scan_vid_iter->get_total();
      iter_unfiltered_vid_cnt_ = total_count;
      iter_scan_total_num_ += total_count;
    }

    // Step 3: apply filter to candidates, collect (vid, distance) into all_pairs
    if (OB_SUCC(ret) && OB_NOT_NULL(scan_vid_iter) && scan_vid_iter->get_total() > 0) {
      int64_t total = scan_vid_iter->get_total();
      int64_t *vids = scan_vid_iter->get_vids();
      float *distances = scan_vid_iter->get_distance();

      if (OB_NOT_NULL(filter_iter_)) {
        if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
          // Expr filter mode: per-vid index-back filter
          iter_added_cnt_ = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < total; ++i) {
            bool filter_res = false;
            if (OB_FAIL(filter_by_index_back(vids[i], distances[i], filter_res))) {
              LOG_WARN("failed to filter by index back", K(ret), K(vids[i]));
            } else if (filter_res) {
              VidDistancePair pair;
              pair.vid = vids[i];
              pair.distance = distances[i];
              if (OB_FAIL(all_pairs.push_back(pair))) {
                LOG_WARN("failed to push back pair", K(ret), K(pair.vid), K(pair.distance));
              } else {
                iter_added_cnt_++;
              }
            }
          }
        } else {
          // Search driver filter mode: build bitmap, filter via filter_iter, collect into all_pairs
          if (total > 0) {
            // reset stale state from previous round (while loop may iterate multiple times
            // when expanding to more centers); re-init/create directly would leak or hit
            // OB_INIT_TWICE on the second iteration.
            bitmap_.reset();
            if (vid_to_distance_.created()) {
              vid_to_distance_.destroy();
            }
            int64_t min_vid = INT64_MAX;
            int64_t max_vid = INT64_MIN;
            for (int64_t i = 0; i < total; ++i) {
              min_vid = min(min_vid, vids[i]);
              max_vid = max(max_vid, vids[i]);
            }
            if (OB_FAIL(bitmap_.init(min_vid, max_vid, get_bitmap_init_size()))) {
              LOG_WARN("failed to init bitmap", K(ret));
            } else if (OB_FAIL(vid_to_distance_.create(total, "VidToDistMap"))) {
              LOG_WARN("failed to create vid to distance map", K(ret), K(total));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < total; ++i) {
                if (OB_FAIL(bitmap_.add_vid(vids[i]))) {
                  LOG_WARN("failed to add vid to bitmap", K(ret), K(vids[i]));
                } else if (OB_FAIL(vid_to_distance_.set_refactored(vids[i], distances[i]))) {
                  LOG_WARN("failed to set vid to distance", K(ret), K(vids[i]), K(distances[i]));
                }
              }
              iter_unfiltered_vid_cnt_ = total;
            }

            if (OB_SUCC(ret)) {
              if (!first_search && OB_FAIL(filter_iter_->reuse())) {
                LOG_WARN("failed to reuse filter iter", K(ret));
              } else if (!first_search && OB_FAIL(filter_iter_->rescan())) {
                LOG_WARN("failed to rescan filter iter", K(ret));
              } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
                LOG_WARN("failed to set bitmap to filter iter", K(ret));
              } else if (OB_FAIL(post_query_vid_with_filter(&all_pairs))) {
                LOG_WARN("failed to post query vid with filter", K(ret));
              }
            }
          }
        }
      } else {
        // No filter: collect scan results directly
        iter_added_cnt_ = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < total; ++i) {
          VidDistancePair pair;
          pair.vid = vids[i];
          pair.distance = distances[i];
          if (OB_FAIL(all_pairs.push_back(pair))) {
            LOG_WARN("failed to push back pair", K(ret), K(pair.vid), K(pair.distance));
          } else {
            iter_added_cnt_++;
          }
        }
      }
    }

    // Step 4: check if we need to continue
    cumulative_added += iter_added_cnt_;
    ivf_iter->set_cumulative_added(cumulative_added);

    if (top_k > 0 && cumulative_added >= top_k) {
      end_search = true;
    } else {
      first_search = false;
      stop_search = false;
      if (FAILEDx(vec_index_scan_iter_->adjust_search_for_next_round(
              first_search, adaptor_vid_iter_,
              iter_unfiltered_vid_cnt_, iter_added_cnt_,
              iter_scan_total_num_, stop_search))) {
        LOG_WARN("failed to adjust search for next round", K(ret));
      } else if (stop_search) {
        end_search = true;
      }
    }
  }

  // Always initialize the result iterator, including the zero-match case.
  // Otherwise inner_get_next_rows() falls back to the IVF scan iterator and
  // exposes the original unfiltered candidates when all candidates are rejected.
  //
  // The brute-force fallback of Step 1.5 must be excluded: it breaks out of the loop
  // without ever filling all_pairs, so an unconditional init here would publish an empty
  // driver adaptor and mask the brute-force results. Both fallback shapes are already
  // correct on their own: Path A1/A2 populate the driver adaptor themselves (they call
  // init_adaptor_vid_iter_if_null() even when everything is filtered out, so an empty
  // result stays empty), while Path B leaves the results in the scan iter adaptor and
  // relies on the output stage falling back to scan_iter->get_adaptor_vid_iter().
  if (OB_SUCC(ret) && !fell_back_to_brute_force) {
    if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
      LOG_WARN("failed to init adaptor vid iter", K(ret));
    } else if (all_pairs.count() > 0) {
      lib::ob_sort(all_pairs.get_data(), all_pairs.get_data() + all_pairs.count(),
                   VidDistancePairCompare(ivf_iter->get_dis_type()));
      for (int64_t i = 0; OB_SUCC(ret) && i < all_pairs.count()
                          && !adaptor_vid_iter_->get_enough(); ++i) {
        if (OB_FAIL(adaptor_vid_iter_->add_result(
                all_pairs.at(i).vid, all_pairs.at(i).distance, nullptr))) {
          LOG_WARN("failed to add result", K(ret),
                   K(all_pairs.at(i).vid), K(all_pairs.at(i).distance));
        }
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASVecIndexDriverIter::process_iterative_filter_mode_ivf(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;


  bool first_search = true;
  bool stop_search = false;
  bool end_search = false;
  int64_t cumulative_added = 0;
  ObDASVecIndexIVFBaseScanIter *ivf_iter =
      static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_);

  ObSEArray<VidDistancePair, 128> all_pairs;
  all_pairs.set_attr(ObMemAttr(MTL_ID(), "IvfIterPairs"));
  const int64_t top_k = limit_param_.limit_ + limit_param_.offset_;

  // ivf_filter_mode_ is already set to ITER_FILTER by build_query_condition()
  // (is_iter_filter param was passed as true for IVF iter_filter path)

  if (OB_FAIL(vec_index_scan_iter_->adjust_search_for_next_round(
          first_search, adaptor_vid_iter_,
          iter_unfiltered_vid_cnt_, iter_added_cnt_,
          iter_scan_total_num_, stop_search))) {
    LOG_WARN("failed to adjust search for first round", K(ret));
  } else if (stop_search) {
    end_search = true;
  }

  while (OB_SUCC(ret) && !end_search) {
    // Phase 1: IVF scan iter collects vid+vec/pq_ids (no distance computation)
    int64_t dummy_count = 0;
    vec_index_scan_iter_->clear_evaluated_flag();
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rows from vec index scan iter", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    // Phase 1.5: check if scan iter needs fallback to brute force (centroid empty)
    if (OB_SUCC(ret) && ivf_need_fallback_brute_force()) {
      if (OB_FAIL(fallback_to_brute_force(count, capacity))) {
        LOG_WARN("failed to fallback to brute force", K(ret));
      }
      break;
    }

    // Phase 2+3: filter vids → push to all_pairs → calc distance in-place
    share::ObVectorQueryVidIterator *scan_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();

    int64_t prev_count = all_pairs.count();

    if (OB_SUCC(ret) && OB_NOT_NULL(scan_vid_iter) && scan_vid_iter->get_total() > 0) {
      int64_t total = scan_vid_iter->get_total();
      int64_t *vids = scan_vid_iter->get_vids();
      iter_unfiltered_vid_cnt_ = total;
      iter_scan_total_num_ += total;

      if (OB_NOT_NULL(filter_iter_)
          && filter_mode_ != ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
        // Search driver filter: bitmap + post_query_vid_with_filter (same as post_filter_mode_ivf)
        // reset stale state from previous round (while loop may iterate multiple times
        // when expanding to more centers); re-init/create directly would leak or hit
        // OB_INIT_TWICE on the second iteration.
        bitmap_.reset();
        if (vid_to_distance_.created()) {
          vid_to_distance_.destroy();
        }
        int64_t min_vid = INT64_MAX;
        int64_t max_vid = INT64_MIN;
        for (int64_t i = 0; i < total; ++i) {
          min_vid = min(min_vid, vids[i]);
          max_vid = max(max_vid, vids[i]);
        }
        if (OB_FAIL(bitmap_.init(min_vid, max_vid, get_bitmap_init_size()))) {
          LOG_WARN("failed to init bitmap", K(ret));
        } else if (OB_FAIL(vid_to_distance_.create(total, "VidToDistMap"))) {
          LOG_WARN("failed to create vid to distance map", K(ret), K(total));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < total; ++i) {
            if (OB_FAIL(bitmap_.add_vid(vids[i]))) {
              LOG_WARN("failed to add vid to bitmap", K(ret), K(vids[i]));
            } else if (OB_FAIL(vid_to_distance_.set_refactored(vids[i], 0.0f))) {
              // distance computed later via calc_distance_for_vids in iterative path
              LOG_WARN("failed to set vid to distance", K(ret), K(vids[i]));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (!first_search && OB_FAIL(filter_iter_->reuse())) {
            LOG_WARN("failed to reuse filter iter", K(ret));
          } else if (!first_search && OB_FAIL(filter_iter_->rescan())) {
            LOG_WARN("failed to rescan filter iter", K(ret));
          } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
            LOG_WARN("failed to set bitmap to filter iter", K(ret));
          } else if (OB_FAIL(post_query_vid_with_filter(&all_pairs))) {
            LOG_WARN("failed to post query vid with filter", K(ret));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < total; ++i) {
          bool pass = true;
          if (OB_NOT_NULL(filter_iter_)) {
            if (OB_FAIL(filter_by_index_back(vids[i], 0.0f /*placeholder*/, pass))) {
              LOG_WARN("failed to filter by index back", K(ret), K(vids[i]));
            }
          }
          if (OB_SUCC(ret) && pass) {
            VidDistancePair pair;
            pair.vid = vids[i];
            pair.distance = 0.0f;
            if (OB_FAIL(all_pairs.push_back(pair))) {
              LOG_WARN("failed to push back vid distance pair", K(ret));
            }
          }
        }
      }

      // calc distance in-place for newly added pairs
      // (reuses outer-scope ivf_iter declared at the start of this function)
      if (OB_SUCC(ret) && all_pairs.count() > prev_count) {
        if (OB_FAIL(ivf_iter->calc_distance_for_vids(all_pairs, prev_count))) {
          LOG_WARN("failed to calc distance for vids", K(ret));
        }
      }
      iter_added_cnt_ = all_pairs.count() - prev_count;
    }

    // Check if we need to continue
    cumulative_added += iter_added_cnt_;
    ivf_iter->set_cumulative_added(cumulative_added);

    if (top_k > 0 && cumulative_added >= top_k) {
      end_search = true;
    } else {
      first_search = false;
      stop_search = false;
      if (FAILEDx(vec_index_scan_iter_->adjust_search_for_next_round(
              first_search, adaptor_vid_iter_,
              iter_unfiltered_vid_cnt_, iter_added_cnt_,
              iter_scan_total_num_, stop_search))) {
        LOG_WARN("failed to adjust search for next round", K(ret));
      } else if (stop_search) {
        end_search = true;
      }
    }
  }

  // Sort all collected pairs by distance and fill top-K into adaptor_vid_iter_
  if (OB_SUCC(ret) && all_pairs.count() > 0) {
    lib::ob_sort(all_pairs.get_data(), all_pairs.get_data() + all_pairs.count(),
                 VidDistancePairCompare(ivf_iter->get_dis_type()));
    if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
      LOG_WARN("failed to init adaptor vid iter", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_pairs.count()
                          && !adaptor_vid_iter_->get_enough(); ++i) {
        if (OB_FAIL(adaptor_vid_iter_->add_result(
                all_pairs.at(i).vid, all_pairs.at(i).distance, nullptr))) {
          LOG_WARN("failed to add result", K(ret),
                   K(all_pairs.at(i).vid), K(all_pairs.at(i).distance));
        }
      }
    }
  } else if (OB_SUCC(ret) && OB_NOT_NULL(filter_iter_) && OB_ISNULL(adaptor_vid_iter_)) {
    // All candidates from scan iter (incl. brute-force results) were rejected by
    // filter_iter_. Must materialize an empty driver adaptor; otherwise the output
    // stage falls back to scan_iter's adaptor which holds unfiltered brute-force
    // results, leading to incorrect rows being returned for queries that should be
    // empty after filtering.
    if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
      LOG_WARN("failed to init empty driver adaptor for fully-filtered result", K(ret));
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

bool ObDASVecIndexDriverIter::ivf_need_fallback_brute_force() const
{
  bool need_fallback = false;
  if (is_ivf_index()) {
    auto *ivf_iter = static_cast<ObDASVecIndexIVFBaseScanIter*>(vec_index_scan_iter_);
    need_fallback = ivf_iter->need_fallback_brute_force();
  }
  return need_fallback;
}

int ObDASVecIndexDriverIter::fallback_to_brute_force(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(filter_iter_) &&
      filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER) {
    // Path A1: SD filter → brute force full scan → fill bitmap → SD post-filter.
    //
    // Bug fix: the previous implementation called process_pre_filter_mode(), which called
    // build_bitmap_from_filter_iter(filter_iter_=search_driver_iter).  That in turn called
    // filter_iter_->get_next_rows() → process_post_filter(), which requires
    // search_driver_iter::bitmap_ to be pre-populated with candidate VIDs.  But no
    // set_bitmap_to_filter_iter() call was made, so bitmap_ was always null → OB_ERR_UNEXPECTED.
    //
    // Root cause: process_pre_filter_mode expects filter_iter_ to *produce* VIDs (forward scan),
    // but ObDASSearchDriverIter in DAS_SEARCH_TYPE_POST_FILTER mode *consumes* a bitmap.
    // The correct flow mirrors process_post_filter_mode():
    //   1. Full brute-force vector scan via fetch_vids_from_vec_index()
    //      (fills bitmap_ with all scanned VIDs when filter_mode == SEARCH_DRIVER_FILTER)
    //   2. set_bitmap_to_filter_iter() → hands bitmap_ to search_driver_iter::bitmap_
    //   3. post_query_vid_with_filter() → SD iter post-filters using the now-valid bitmap
    vec_index_scan_iter_->set_go_brute_force(true);
    if (is_ivf_index()) {
      ObDASVecIndexIVFBaseScanIter *ivf_iter =
          static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_);
      ivf_iter->set_ivf_filter_mode(ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER);
      ivf_iter->set_brute_force_query_limit(
          ObDASVecIndexIVFBaseScanIter::MAX_IVF_BRUTE_FORCE_SIZE);
    }
    if (OB_FAIL(fetch_vids_from_vec_index(count, capacity))) {
      LOG_WARN("failed to fetch vids in brute force for SD filter", K(ret));
    } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
      LOG_WARN("failed to set bitmap to SD filter iter after brute force", K(ret));
    } else if (OB_FAIL(post_query_vid_with_filter())) {
      LOG_WARN("failed to post query vid with SD filter after brute force", K(ret));
    }
  } else if (OB_NOT_NULL(filter_iter_) &&
             filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
    // Path A2: has filter (expr/index-back) → brute force full COM_AUX scan + post expr filter
    // filter_iter_ is a per-row index-back scanner; cannot batch scan to build bitmap.
    // Do full COM_AUX brute force (like Path B), then apply expr filter on results.
    // Set query_limit to MAX so brute force heap holds all rows (post-filter truncates to K).
    vec_index_scan_iter_->set_go_brute_force(true);
    if (is_ivf_index()) {
      ObDASVecIndexIVFBaseScanIter *ivf_iter =
          static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_);
      ivf_iter->set_ivf_filter_mode(ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER);
      // Temporarily set query_limit to MAX so brute force returns all candidates;
      // post_query_vid_with_expr_filter will then filter and the adaptor_vid_iter_ limit
      // (set during init_adaptor_vid_iter_if_null) enforces the final top-K.
      ivf_iter->set_brute_force_query_limit(
          ObDASVecIndexIVFBaseScanIter::MAX_IVF_BRUTE_FORCE_SIZE);
    }
    vec_index_scan_iter_->clear_evaluated_flag();
    int64_t dummy_count = 0;
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to do brute force scan for expr filter", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    // Apply expr filter per VID on brute force results, populate driver's adaptor_vid_iter_.
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(vec_index_scan_iter_->get_adaptor_vid_iter())) {
        if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
          LOG_WARN("failed to init empty adaptor after brute force", K(ret));
        }
      } else if (OB_FAIL(post_query_vid_with_expr_filter())) {
        LOG_WARN("failed to apply expr filter after brute force", K(ret));
      }
    }
  } else {
    // Path B: no filter → let scan iter brute force directly (full COM_AUX scan + heap top-K)
    //
    // POST_FILTER (not PRE_FILTER) is required here, same as Path A1/A2: with POST_FILTER the
    // second get_next_rows() lands on do_post_filter_round(), whose go_brute_force_ shortcut
    // enters do_brute_force_scan() immediately. The PRE_FILTER entry do_ivf_flat_pre_filter()
    // only takes its brute-force shortcut when a VIDS bitmap exists, which never holds without
    // a filter, so it would probe the (already known empty) centroid table a second time before
    // falling back - a pure waste that grows with centroid table size.
    vec_index_scan_iter_->set_go_brute_force(true);
    if (is_ivf_index()) {
      static_cast<ObDASVecIndexIVFBaseScanIter *>(vec_index_scan_iter_)
          ->set_ivf_filter_mode(ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER);
    }
    vec_index_scan_iter_->clear_evaluated_flag();
    int64_t dummy_count = 0;
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to do brute force scan", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    // Brute force results land in scan_iter's adaptor_vid_iter_.
    // Output phase will fall back to scan_iter->get_adaptor_vid_iter() when
    // driver's adaptor_vid_iter_ is null, so no redirect needed here.
  }
  return ret;
}

int ObDASVecIndexDriverIter::process_post_filter_mode(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  bool first_search = true;
  bool stop_search = false;
  if (OB_FAIL(vec_index_scan_iter_->adjust_search_for_next_round(
          first_search, adaptor_vid_iter_,
          iter_unfiltered_vid_cnt_, iter_added_cnt_,
          iter_scan_total_num_, stop_search))) {
    LOG_WARN("failed to adjust search for post filter", K(ret));
  } else if (OB_FAIL(fetch_vids_from_vec_index(count, capacity))) {
    if (OB_SCHEMA_EAGAIN != ret) {
      LOG_WARN("failed to fetch vids from vec index", K(ret));
    }
  } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
    LOG_WARN("failed to set bitmap to filter iter", K(ret));
  } else if (OB_NOT_NULL(filter_iter_)) {
    if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
      if (OB_FAIL(post_query_vid_with_expr_filter())) {
        LOG_WARN("failed to post query vid with expr filter", K(ret));
      }
    } else {
      if (OB_FAIL(post_query_vid_with_filter())) {
        LOG_WARN("failed to post query vid with filter", K(ret));
      }
    }
  } else if (OB_ISNULL(filter_iter_)) {
    ObVectorQueryVidIterator *adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
    if (OB_ISNULL(adaptor_vid_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("adaptor vid iter is null", K(ret));
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASVecIndexDriverIter::save_vec_socre_expr_result(ObNewRow *row, int64_t size)
{
  int ret = OB_SUCCESS;

  ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
  int64_t a_batch_obj_cnt = 2;
  if (OB_ISNULL(score_expr_) || OB_ISNULL(row) || OB_ISNULL(eval_ctx) || OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score expr or row or eval ctx or sort expr is null", K(ret), K(score_expr_), K(row), K(eval_ctx), K(sort_expr_));
  } else if (score_expr_->enable_rich_format()) {
    if (OB_FAIL(score_expr_->init_vector_for_write(*eval_ctx, score_expr_->get_default_res_format(), size))) {
      LOG_WARN("failed to init vector for write", K(ret));
    } else {
      ObIVector *vec = score_expr_->get_vector(*eval_ctx);
      for (int64_t num = 0; OB_SUCC(ret) && num < size; ++num) {
        double distance = static_cast<double>(row->get_cell(num * a_batch_obj_cnt).get_float());
        double score = 0;
        if (OB_FAIL(distance_to_score(distance, score))) {
          LOG_WARN("failed to convert distance to score", K(ret));
        } else {
          vec->set_double(num, score);
        }
      }
    }
  } else {
    ObDatum *score_datum = nullptr;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    guard.set_batch_size(size);
    if (OB_ISNULL(score_datum = score_expr_->locate_datums_for_update(*eval_ctx, size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(score_expr_));
    } else {
      for (int64_t num = 0; OB_SUCC(ret) && num < size; ++num) {
        guard.set_batch_idx(num);
        double distance = static_cast<double>(row->get_cell(num * a_batch_obj_cnt).get_float());
        double score = 0;
        if (OB_FAIL(distance_to_score(distance, score))) {
          LOG_WARN("failed to convert distance to score", K(ret));
        } else {
          score_datum[num].set_double(score);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    score_expr_->set_evaluated_projected(*eval_ctx);
  }

  return ret;
}

int ObDASVecIndexDriverIter::distance_to_score(double distance, double &score)
{
  return vec_index_scan_iter_->distance_to_score(sort_expr_, distance, score);
}

int ObDASVecIndexDriverIter::post_query_vid_with_expr_filter()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter iter or adaptor is null", K(ret));
  } else if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
    LOG_WARN("failed to init adaptor vid iter", K(ret));
  } else {
    iter_added_cnt_ = 0;
    if (OB_FAIL(do_expr_filter_batch())) {
      LOG_WARN("do_expr_filter_batch failed", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::do_expr_filter_batch()
{
  int ret = OB_SUCCESS;
  int64_t processed = 0;

  share::ObVectorQueryVidIterator *src_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
  if (OB_ISNULL(src_iter) || OB_ISNULL(scalar_scan_ctdef_) || OB_ISNULL(scalar_scan_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(src_iter), KP(scalar_scan_ctdef_), KP(scalar_scan_rtdef_));
  } else {
    const int64_t total_count = src_iter->get_total();
    const int64_t *vids = src_iter->get_vids();
    const float *distances = src_iter->get_distance();
    ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
    const bool is_vectorized = eval_ctx->is_vectorized();
    const int64_t batch_size = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
    ObIAllocator &arena = mem_context_->get_arena_allocator();
    hash::ObHashMap<int64_t, int64_t> vid_to_idx_map;

    if (OB_FAIL(vid_to_idx_map.create(batch_size * 2, lib::ObMemAttr(MTL_ID(), "VidToIdxMap")))) {
      LOG_WARN("create vid_to_idx_map failed", K(ret));
    }

    ObObj *vid_objs = static_cast<ObObj *>(arena.alloc(sizeof(ObObj) * batch_size));
    bool *matched_flags = static_cast<bool *>(arena.alloc(sizeof(bool) * batch_size));
    if (OB_ISNULL(vid_objs) || OB_ISNULL(matched_flags)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc vid_objs or matched_flags failed", K(ret), K(batch_size));
    }

    while (OB_SUCC(ret) && processed < total_count && !adaptor_vid_iter_->get_enough()) {
      vid_to_idx_map.reuse();
      MEMSET(matched_flags, 0, sizeof(bool) * batch_size);
      scalar_scan_rtdef_->key_ranges_.reset();
      int64_t batch_cnt = 0;
      const int64_t batch_start = processed;

      const int64_t needed = adaptor_vid_iter_->get_alloc_size() - adaptor_vid_iter_->get_total();
      int64_t effective_batch = batch_size;
      if (needed > 0) {
        double selectivity = 0;
        if (last_partition_row_count_ > 0 && last_filter_est_row_count_ > 0
            && last_filter_est_row_count_ < last_partition_row_count_) {
          selectivity = static_cast<double>(last_filter_est_row_count_)
                        / static_cast<double>(last_partition_row_count_);
        }
        effective_batch = ObDasVecScanUtils::calc_effective_batch(needed, selectivity, batch_size);
      }

      for (int64_t i = processed; OB_SUCC(ret) && i < total_count && batch_cnt < effective_batch; ++i) {
        int64_t vid = vids[i];
        ObNewRange look_range;
        new (&vid_objs[batch_cnt]) ObObj();
        vid_objs[batch_cnt].set_uint64(static_cast<uint64_t>(vid));
        ObRowkey vid_rowkey(&vid_objs[batch_cnt], 1);
        if (OB_FAIL(look_range.build_range(scalar_scan_ctdef_->ref_table_id_, vid_rowkey))) {
          LOG_WARN("build_range failed", K(ret), K(vid));
        } else if (OB_FAIL(scalar_scan_rtdef_->key_ranges_.push_back(look_range))) {
          LOG_WARN("push_back key_ranges_ failed", K(ret));
        } else if (OB_FAIL(vid_to_idx_map.set_refactored(vid, i))) {
          LOG_WARN("set vid_to_idx_map failed", K(ret), K(vid), K(i));
        } else {
          ++batch_cnt;
        }
      }

      processed += batch_cnt;

      if (OB_FAIL(ret) || batch_cnt == 0) {
      } else if (OB_FAIL(filter_iter_->reuse())) {
        LOG_WARN("filter_iter_ reuse failed", K(ret));
      } else if (OB_FAIL(filter_iter_->rescan())) {
        LOG_WARN("filter_iter_ rescan failed", K(ret));
      } else {
        ObExpr *vid_expr = nullptr;
        if (OB_UNLIKELY(scalar_scan_ctdef_->rowkey_exprs_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowkey_exprs_ is empty", K(ret));
        } else if (OB_ISNULL(vid_expr = scalar_scan_ctdef_->rowkey_exprs_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vid_expr is null", K(ret));
        } else if (is_vectorized) {
          bool scan_end = false;
          filter_iter_->clear_evaluated_flag();
          while (!scan_end && OB_SUCC(ret)) {
            int64_t scan_row_cnt = 0;
            if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, batch_size))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("get_next_rows from filter_iter_ failed", K(ret));
              } else {
                scan_end = true;
                ret = OB_SUCCESS;
              }
            }
            if (OB_SUCC(ret) && scan_row_cnt > 0) {
              ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
              guard.set_batch_size(scan_row_cnt);
              bool use_rich_format = vid_expr->enable_rich_format() && is_valid_format(vid_expr->get_format(*eval_ctx));
              ObIVector *vec = nullptr;
              ObDatum *datums = nullptr;
              if (use_rich_format) {
                if (OB_ISNULL(vec = vid_expr->get_vector(*eval_ctx))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get_vector failed", K(ret));
                }
              } else {
                if (OB_ISNULL(datums = vid_expr->locate_batch_datums(*eval_ctx))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("locate_batch_datums failed", K(ret));
                }
              }

              for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
                int64_t filtered_vid = 0;
                if (use_rich_format) {
                  filtered_vid = static_cast<int64_t>(vec->get_uint64(j));
                } else {
                  guard.set_batch_idx(j);
                  filtered_vid = static_cast<int64_t>(datums[j].get_uint64());
                }
                int64_t idx = 0;
                if (OB_FAIL(vid_to_idx_map.get_refactored(filtered_vid, idx))) {
                  LOG_WARN("vid not found in batch map", K(ret), K(filtered_vid));
                } else if (idx < batch_start || idx >= batch_start + batch_cnt) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("matched vid idx is out of current batch", K(ret), K(filtered_vid), K(idx), K(batch_start), K(batch_cnt));
                } else {
                  matched_flags[idx - batch_start] = true;
                }
              }
            }
          }
        } else {
          filter_iter_->clear_evaluated_flag();
          while (OB_SUCC(ret)) {
            if (OB_FAIL(filter_iter_->get_next_row())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("get_next_row from filter_iter_ failed", K(ret));
              }
            } else {
              ObDatum *datum = nullptr;
              if (OB_FAIL(vid_expr->eval(*eval_ctx, datum))) {
                LOG_WARN("eval vid_expr failed", K(ret));
              } else {
                int64_t filtered_vid = static_cast<int64_t>(datum->get_uint64());
                int64_t idx = 0;
                if (OB_FAIL(vid_to_idx_map.get_refactored(filtered_vid, idx))) {
                  LOG_WARN("vid not found in batch map", K(ret), K(filtered_vid));
                } else if (idx < batch_start || idx >= batch_start + batch_cnt) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("matched vid idx is out of current batch", K(ret), K(filtered_vid), K(idx), K(batch_start), K(batch_cnt));
                } else {
                  matched_flags[idx - batch_start] = true;
                }
              }
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_cnt && !adaptor_vid_iter_->get_enough(); ++i) {
        if (matched_flags[i]) {
          const int64_t idx = batch_start + i;
          if (OB_FAIL(adaptor_vid_iter_->add_result(vids[idx], distances[idx], nullptr))) {
            LOG_WARN("add_result failed", K(ret), K(vids[idx]), K(idx));
          } else {
            iter_added_cnt_++;
          }
        }
      }
    } // end outer while

    if (vid_to_idx_map.created()) {
      vid_to_idx_map.destroy();
    }
  }
  return ret;
}

int ObDASVecIndexDriverIter::filter_by_index_back(int64_t vid, float distance, bool &filter_res)
{
  int ret = OB_SUCCESS;
  filter_res = false;
  UNUSED(distance);

  if (filter_mode_ != ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter_by_index_back should only be called in expr filter mode",
             K(ret), K(filter_mode_));
  } else if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter iter is null", K(ret));
  } else if (OB_FAIL(filter_iter_->reuse())) {
    LOG_WARN("failed to reuse filter iter", K(ret));
  } else if (OB_FAIL(set_rowkey_by_vid(vid))) {
    LOG_WARN("failed to set rowkey by vid", K(ret), K(vid));
  } else if (OB_FAIL(filter_iter_->rescan())) {
    LOG_WARN("failed to rescan filter iter", K(ret));
  } else {
    bool is_vectorized = vec_index_driver_rtdef_->eval_ctx_->is_vectorized();
    int64_t scan_row_cnt = 0;
    if (is_vectorized) {
      filter_iter_->clear_evaluated_flag();
      ret = filter_iter_->get_next_rows(scan_row_cnt, 1);
      if (OB_SUCC(ret) && scan_row_cnt <= 0) {
        ret = OB_ITER_END;
      }
    } else if (OB_FAIL(get_single_row_from_filter_iter(is_vectorized))) {
      // pass through
    }
    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get single row from data filter iter", K(ret), K(vid));
      } else {
        ret = OB_SUCCESS;
        filter_res = false;
      }
    } else {
      filter_res = true;
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::set_rowkey_by_vid(int64_t vid)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(scalar_scan_ctdef_) || OB_ISNULL(scalar_scan_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar_scan_ctdef or scalar_scan_rtdef is null", K(ret), K(scalar_scan_ctdef_), K(scalar_scan_rtdef_));
  } else {
    scalar_scan_rtdef_->key_ranges_.reset();
    // Always build a simple uint64 vid range here.
    // For logical table, rewrite_table_scan_param_key_ranges in rescan will handle
    // the physical KV key encoding with the correct prefix.
    vid_obj_for_lookup_.set_uint64(vid);
    ObRowkey vid_rowkey(&vid_obj_for_lookup_, 1);
    ObNewRange look_range;
    if (OB_FAIL(look_range.build_range(scalar_scan_ctdef_->ref_table_id_, vid_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret));
    } else if (OB_FAIL(scalar_scan_rtdef_->key_ranges_.push_back(look_range))) {
      LOG_WARN("failed to push back lookup range", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::get_single_row_from_filter_iter(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  filter_iter_->clear_evaluated_flag();
  common::ScopedTimer timer(common::ObMetricId::HS_VEC_DRIVER_FILTER_TIME);
  if (is_vectorized) {
    int64_t scan_row_cnt = 0;
    if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, 1))) {
      // pass through
    } else if (scan_row_cnt <= 0) {
      ret = OB_ITER_END;
    }
  } else {
    ret = filter_iter_->get_next_row();
  }

  return ret;
}

ObVecPathType ObDASVecIndexDriverIter::get_vec_index_path_type()
{
  ObVecPathType vec_path_type = ObVecPathType::VEC_PATH_INVALID;
  if (is_brute_force_scan()) {
    vec_path_type = ObVecPathType::VEC_PATH_BRUTE_FORCE;
  } else if (is_pre_filter()) {
    if (vec_index_scan_iter_->is_brute_force()) {
      vec_path_type = ObVecPathType::VEC_PATH_PRE_BRUTE;
    } else {
      vec_path_type = ObVecPathType::VEC_PATH_PRE_KNN;
    }
  } else if (is_iter_filter()) {
    if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER) {
      vec_path_type = ObVecPathType::VEC_PATH_POST_ITERATIVE_FILTER_SEARCH_DRIVER_MODE;
    } else if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
      vec_path_type = ObVecPathType::VEC_PATH_POST_ITERATIVE_FILTER_EXPR_FILTER_MODE;
    }
  } else if (is_post_filter()) {
    if (is_post_without_filter()) {
      vec_path_type = ObVecPathType::VEC_PATH_POST_WITHOUT_FILTER;
    } else if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER) {
      vec_path_type = ObVecPathType::VEC_PATH_POST_FILTER_SEARCH_DRIVER_MODE;
    } else if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
      vec_path_type = ObVecPathType::VEC_PATH_POST_FILTER_EXPR_FILTER_MODE;
    }
  }
  return vec_path_type;
}


} // namespace sql
} // namespace oceanbase
