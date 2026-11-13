/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include <cmath>
#include "lib/worker.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/async_index/ob_deletion_row_iter.h"
#include "src/storage/access/ob_table_scan_iterator.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "sql/engine/expr/ob_expr_vec_ivf_sq8_data_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"
#include "deps/oblib/src/lib/vector/ob_vector_util.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_scan_iter.h"
#include "storage/access/ob_table_scan_range.h"
#include "share/ob_debug_sync.h"
#include "sql/das/iter/ob_das_profile_iter.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;
using namespace sql;

namespace sql
{

ERRSIM_POINT_DEF(ERRSIM_ENABLE_ASYNC_IVF_FLASHBACK_RESET,
                "Reset the async IVF flashback snapshot instead of rejecting flashback queries");

void ObIVFProfileUtil::set(ObOpProfile<ObMetric> *profile, ObMetricId id, uint64_t value)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(profile);
  SET_METRIC_VAL(id, value);
}

void ObIVFProfileUtil::inc(ObOpProfile<ObMetric> *profile, ObMetricId id, uint64_t value)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(profile);
  INC_METRIC_VAL(id, value);
}

void ObIVFProfileUtil::set_once(ObOpProfile<ObMetric> *profile, ObMetricId id, uint64_t value)
{
  if (nullptr != profile && nullptr == profile->get_metric(id)) {
    set(profile, id, value);
  }
}

ObOpProfile<ObMetric> *ObIVFProfileUtil::register_child(ObOpProfile<ObMetric> *profile, ObProfileId id)
{
  int ret = OB_SUCCESS;
  ObOpProfile<ObMetric> *child = nullptr;
  if (nullptr != profile && OB_FAIL(profile->register_child(id, child))) {
    LOG_WARN("failed to register async ivf profile child", KR(ret), K(id));
    child = nullptr;
  }
  return child;
}
static common::ObOpProfile<common::ObMetric> *get_current_vector_iter_profile()
{
  common::ObOpProfile<common::ObMetric> *profile = common::get_current_profile();
  return OB_NOT_NULL(profile) && profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER
             ? profile
             : nullptr;
}

int ObDASIvfBaseScanIter::record_profile_path_switch(
    ObVecIdxAdaTryPath old_path,
    ObVecIdxPathSwitchReason switch_reason)
{
  int ret = OB_SUCCESS;
  if (old_path != vec_idx_try_path_) {
    profile_current_path_ = vec_idx_try_path_;
    profile_switch_reason_ = switch_reason;
    common::ObOpProfile<common::ObMetric> *current_profile = common::get_current_profile();
    if (OB_NOT_NULL(current_profile)
        && current_profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER) {
      SET_METRIC_VAL(common::ObMetricId::VEC_CURRENT_PATH,
                     static_cast<int64_t>(profile_current_path_));
      SET_METRIC_VAL(common::ObMetricId::VEC_SWITCH_REASON,
                     static_cast<int64_t>(profile_switch_reason_));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::prepare_runtime_profile()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current_profile = common::get_current_profile();
  common::ObOpProfile<common::ObMetric> *profile = nullptr;
  bool new_profile_instance = false;
  if (OB_NOT_NULL(current_profile)
      && current_profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER) {
    if (OB_NOT_NULL(profile_)
        && profile_->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_IVF_SCAN
        && profile_->get_parent() == current_profile) {
      profile = profile_;
    } else if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
                   common::ObProfileId::VECTOR_SEARCH_VEC_IVF_SCAN, profile))) {
      LOG_WARN("failed to initialize ivf runtime profile", KR(ret));
    } else {
      new_profile_instance = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (new_profile_instance) {
      profile_current_path_ = is_adaptive_filter() ? vec_idx_try_path_ : profile_try_path_;
      profile_switch_reason_ = ObVecIdxPathSwitchReason::VEC_PATH_NO_SWITCH;
    }
    profile_ = profile;
  } else {
    profile_ = nullptr;
  }
  return ret;
}
int ObDASIvfBaseScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  int profile_ret = prepare_runtime_profile();
  if (OB_SUCCESS != profile_ret) {
    LOG_WARN("failed to prepare ivf runtime profile", KR(profile_ret));
  }
  if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_iter_ is null", K(ret));
  } else if (OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
    LOG_WARN("fail to do inv idx table scan.", K(ret));
  }

  return ret;
}

int ObDASIvfBaseScanIter::rescan()
{
  int ret = OB_SUCCESS;
  int profile_ret = prepare_runtime_profile();
  if (OB_SUCCESS != profile_ret) {
    LOG_WARN("failed to prepare ivf runtime profile for rescan", KR(profile_ret));
  }
  // Synchronous FLAT/SQ8/PQ also reuse this iterator across correlated bindings.
  if (OB_FAIL(refresh_query_vector())) {
    LOG_WARN("failed to refresh ivf query vector on rescan", K(ret));
  } else if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->rescan())) {
    LOG_WARN("failed to rescan inv_idx_scan_iter_", K(ret));
  }

  return ret;
}

void ObDASIvfBaseScanIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(inv_idx_scan_iter_)) {
    inv_idx_scan_iter_->clear_evaluated_flag();
  }
}

int ObDASIvfBaseScanIter::gen_rowkeys_itr()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(saved_rowkeys_itr_) && saved_rowkeys_itr_->is_init()) {
  } else {
    uint64_t rowkey_count = saved_rowkeys_.count();
    void *iter_buff = nullptr;
    if (rowkey_count == 0) {
      ret = OB_ITER_END;
      LOG_WARN("no rowkeys found", K(ret));
    } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryRowkeyIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator rowkey iter.", K(ret));
    } else if (OB_FALSE_IT(saved_rowkeys_itr_ = new (iter_buff) ObVectorQueryRowkeyIterator())) {
    } else if (OB_FAIL(saved_rowkeys_itr_->init(rowkey_count, &saved_rowkeys_))) {
      LOG_WARN("iter init failed.", K(ret));
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_table_full_scan(bool is_vectorized,
                                             const ObDASScanCtDef *ctdef,
                                             ObDASScanRtDef *rtdef,
                                             ObDASScanIter *iter,
                                             ObTabletID &tablet_id,
                                             bool &first_scan,
                                             ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    ObNewRange scan_range;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param,
                                                   false /*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to generate init vec aux scan param", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
    scan_param.need_switch_param_ =
        scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id_;

    ObNewRange scan_range;
    if (OB_FAIL(iter->reuse())) {
      LOG_WARN("failed to reuse scan iterator.", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_aux_table_scan(bool &first_scan,
                                            ObTableScanParam &scan_param,
                                            const ObDASScanCtDef *ctdef,
                                            ObDASScanRtDef *rtdef,
                                            ObDASScanIter *iter,
                                            ObTabletID &tablet_id,
                                            bool is_get)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    scan_param.need_switch_param_ = false;
    // Storage owns and frees each lookup batch's datum rowkeys on reuse().
    // Keep the allocator itself alive across rescans, without allocating these
    // short-lived datums in the arena that holds cross-round IVF state.
    const bool freeable_lookup = is_get && &scan_param == &data_filter_scan_param_
        && OB_NOT_NULL(vec_aux_ctdef_)
        && ObVectorIndexAlgorithmType::VIAT_DISK_BBQ == vec_aux_ctdef_->algorithm_type_;
    if (freeable_lookup) {
      disk_bbq_lookup_alloc_.bind(mem_context_->get_malloc_allocator());
    }
    ObIAllocator *scan_allocator = freeable_lookup
        ? static_cast<ObIAllocator *>(&disk_bbq_lookup_alloc_)
        : &mem_context_->get_arena_allocator();
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(
            ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param, is_get, scan_allocator))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
    scan_param.need_switch_param_ =
        scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id_;
    if (OB_FAIL(iter->rescan())) {
      LOG_WARN("fail to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

// Reset memory context and clean up associated resources
void ObDASIvfBaseScanIter::reset_memory_context()
{
  // Clean up HGraph iterative search context and reset memory context
  if (OB_NOT_NULL(hgraph_iter_ctx_)) {
    obvectorutil::delete_iter_ctx(hgraph_iter_ctx_);
    hgraph_iter_ctx_ = nullptr;
  }
  // Clean up HGraph iterative search context and reset memory context before mem_context_ reset
  if (OB_NOT_NULL(hgraph_vsag_alloc_)) {
    hgraph_vsag_alloc_->~ObVsagSearchAlloc();
    hgraph_vsag_alloc_ = nullptr;
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  // Reset HGraph state for next iteration
  hgraph_has_next_center_ = true;
  has_used_hgraph_ = false;
}

// Unified interface for checking if there are more centers available
bool ObDASIvfBaseScanIter::has_next_center()
{
  bool has_next = false;
  if (has_used_hgraph_) {
    has_next = hgraph_has_next_center_;
  } else {
    has_next = iterative_filter_ctx_.has_next_center();
  }
  return has_next;
}

int ObDASIvfBaseScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  prefilter_aborted_ = false;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
  } else if (!centroid_iter_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                               ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
  } else if (!cid_vec_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_))) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
  } else if (!rowkey_cid_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(
                 ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
    LOG_WARN("failed to reuse vid rowkey iter", K(ret));
  } else if (!brute_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, brute_iter_, brute_scan_param_, brute_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } else if (!data_filter_iter_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, data_filter_iter_, data_filter_scan_param_, data_filter_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } 

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
  }
  // Reset memory context and clean up associated resources
  reset_memory_context();

  vec_op_alloc_.reset();
  saved_rowkeys_.reset();
  pre_fileter_rowkeys_.reset();
  // Release any centroid ref_cnt_ held from the previous round before resetting
  // the cache mgr guard.
  {
    int tmp_ret = batch_guard_.release_all();
    if (common::OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to release batch_guard_ in inner_reuse", K(tmp_ret));
    }
  }
  center_cache_guard_.reset();
  iterative_filter_ctx_.reuse();
  // adaptive_ctx_.reset();
  profile_ = nullptr;
  return ret;
}

int ObDASIvfBaseScanIter::init_hgraph_search_alloc()
{
  int ret = OB_SUCCESS;
  // Create VSAG allocator if not exists (same lifecycle as hgraph_iter_ctx_)
  if (OB_ISNULL(hgraph_vsag_alloc_)) {
    void *alloc_buf = mem_context_->get_arena_allocator().alloc(sizeof(ObVsagSearchAlloc));
    if (OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for hgraph vsag allocator", K(ret));
    } else {
      hgraph_vsag_alloc_ = new(alloc_buf) ObVsagSearchAlloc(MTL_ID());
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  const bool is_async_ivf =
      ObDASIterType::DAS_ITER_ASYNC_IVF_SCAN == param.type_;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_IVF_SCAN != param.type_
                  && !is_async_ivf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for ivf scan iter", K(ret), K(param));
  } else {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    ls_id_ = ivf_scan_param.ls_id_;
    tx_desc_ = ivf_scan_param.tx_desc_;
    snapshot_ = ivf_scan_param.snapshot_;

    inv_idx_scan_iter_ = ivf_scan_param.inv_idx_scan_iter_;
    centroid_iter_ = ivf_scan_param.centroid_iter_;
    cid_vec_iter_ = ivf_scan_param.cid_vec_iter_;
    rowkey_cid_iter_ = ivf_scan_param.rowkey_cid_iter_;
    brute_iter_ = ivf_scan_param.brute_iter_;
    data_filter_iter_ = ivf_scan_param.data_filter_iter_;

    vec_aux_ctdef_ = ivf_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = ivf_scan_param.vec_aux_rtdef_;
    sort_ctdef_ = ivf_scan_param.sort_ctdef_;
    sort_rtdef_ = ivf_scan_param.sort_rtdef_;
    data_filter_ctdef_ = ivf_scan_param.data_filter_ctdef_;
    data_filter_rtdef_ = ivf_scan_param.data_filter_rtdef_;

    vec_index_type_ = ivf_scan_param.vec_index_type_;
    vec_idx_try_path_ = ivf_scan_param.vec_idx_try_path_;
    profile_try_path_ = ObDasVecScanUtils::get_initial_vec_idx_path(
        vec_index_type_, vec_idx_try_path_);
    profile_current_path_ = profile_try_path_;
    profile_switch_reason_ = ObVecIdxPathSwitchReason::VEC_PATH_NO_SWITCH;
    strategy_ = ivf_scan_param.strategy_;

    adaptive_ctx_.reset();
    adaptive_ctx_.selectivity_ = vec_aux_ctdef_->selectivity_;
    adaptive_ctx_.row_count_ = vec_aux_ctdef_->row_count_;
    adaptive_ctx_.is_primary_index_ = ivf_scan_param.is_primary_index_;
    adaptive_ctx_.can_use_vec_pri_opt_ = vec_aux_ctdef_->can_use_vec_pri_opt();

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "IVF", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector ivf memory context", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_NOT_NULL(sort_ctdef_) && OB_NOT_NULL(sort_rtdef_)) {
        ObExpr *distance_calc = nullptr;
        if (OB_FAIL(
                ObDasVecScanUtils::init_limit(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_))) {
          LOG_WARN("failed to init limit", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
        } else if (OB_FAIL(ObDasVecScanUtils::init_sort(
                      vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_, search_vec_, distance_calc))) {
          LOG_WARN("failed to init sort", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
        } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                       vec_aux_ctdef_->vec_index_param_, ObVectorIndexType::VIT_IVF_INDEX, vec_index_param_))) {
          LOG_WARN("fail to parse params from string", K(ret), K(vec_aux_ctdef_->vec_index_param_));
        } else if (OB_FALSE_IT(vec_index_param_.dim_ = vec_aux_ctdef_->dim_)) {
        } else if (OB_FAIL(refresh_query_vector())) {
          LOG_WARN("failed to initialize query vector", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObVectorIndexParam::build_search_param(vec_aux_ctdef_->vector_index_param_, vec_aux_ctdef_->vec_query_param_, search_param_))) {
            LOG_WARN("build search param fail", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_));
          } else {
            LOG_TRACE("search param", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_), K(search_param_));
            if (search_param_.similarity_threshold_ > 0) {
              if (OB_FAIL(ObDasVecScanUtils::check_ivf_support_similarity_threshold(*sort_ctdef_->sort_exprs_[0]))) {
                LOG_WARN("check support similarity threshold fail", K(ret));
              } else {
                similarity_threshold_ = search_param_.similarity_threshold_;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          ObSQLSessionInfo *session = nullptr;
          uint64_t ob_ivf_nprobes = 0;
          
          if (OB_NOT_NULL(vec_aux_ctdef_) && vec_aux_ctdef_->vec_query_param_.is_set_ivf_nprobes_) {
            nprobes_ = vec_aux_ctdef_->vec_query_param_.ivf_nprobes_;
            LOG_TRACE("use stmt ivf_nprobes", K(nprobes_));
          } else if (OB_ISNULL(session = sort_rtdef_->eval_ctx_->exec_ctx_.get_my_session())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get session", K(ret), KPC(session));
          } else if (OB_FAIL(session->get_ob_ivf_nprobes(ob_ivf_nprobes))) {
            LOG_WARN("failed to get ob ob_ivf_nprobes", K(ret));
          } else {
            nprobes_ = ob_ivf_nprobes;
          }
        }
      }
    }

    // Async IVF selects its path per partition without updating the shared plan.
    if (OB_SUCC(ret) && !is_async_ivf && is_adaptive_filter()) {
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_);
      ObVecIdxAdaTryPath cur_path = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
      if (OB_ISNULL(plan_ctx->get_phy_plan())) {
        // remote scan, phy plan is null, do nothing, just use try path in ctdef
        LOG_WARN("plan ctx is null", K(ret), KP(plan_ctx));
      } else if (OB_FALSE_IT(cur_path = static_cast<ObVecIdxAdaTryPath>(plan_ctx->get_phy_plan()->stat_.vec_index_exec_ctx_.cur_path_))) {
      } else if (cur_path != vec_idx_try_path_ && 
                  cur_path > ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN &&
                  cur_path < ObVecIdxAdaTryPath::VEC_PATH_MAX) {
        LOG_INFO("adaptive filter change path", K(cur_path), K(vec_idx_try_path_));
        vec_idx_try_path_ = cur_path;
      }
    }
    if (OB_SUCC(ret) && is_adaptive_filter()) {
      profile_current_path_ = vec_idx_try_path_;
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(data_filter_ctdef_)) {
      int64_t main_rowkey_cnt = data_filter_ctdef_->table_param_.get_read_info().get_schema_rowkey_count();
      void *ptr = nullptr;
      if (OB_ISNULL(ptr = persist_alloc_.alloc(sizeof(ObObj) * main_rowkey_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(main_rowkey_cnt));
      } else {
        ObObj *obj_ptr = new (ptr) ObObj[main_rowkey_cnt];
        tmp_main_rowkey_.assign(obj_ptr, main_rowkey_cnt);
      }
    }

    if (OB_SUCC(ret)) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID())); 
      if (tenant_config.is_valid()) {
        max_scan_vectors_ = is_async_ivf
            ? tenant_config->_disk_bbq_max_scan_vectors
            : tenant_config->_ivf_max_scan_vectors;
      }
    }
    if (OB_SUCC(ret) && !is_async_ivf) {
      const ObDASBaseRtDef *index_rtdef = vec_aux_rtdef_->get_inv_idx_scan_rtdef();
      if (OB_NOT_NULL(index_rtdef->table_loc_)) {
        scan_tablet_size_ = index_rtdef->table_loc_->get_tablet_locs().size();
      }
    }
  }

  if (OB_SUCC(ret) && !is_async_ivf) {
    if (OB_FAIL(get_ns_id())) {
      LOG_WARN("failed to get ns_id", K(ret));
    }
  }

    dim_ = vec_aux_ctdef_->dim_;
    selectivity_ = vec_aux_ctdef_->selectivity_;
    // mock ns_id

  return ret;
}

int ObDASIvfBaseScanIter::refresh_query_vector()
{
  int ret = OB_SUCCESS;
  ObString query_vector;
  real_search_vec_.reset();
  query_vector_alloc_.reuse();
  need_norm_ = false;
  if (OB_ISNULL(vec_aux_ctdef_) || OB_ISNULL(sort_ctdef_) || OB_ISNULL(sort_rtdef_)
      || OB_ISNULL(sort_rtdef_->eval_ctx_) || OB_ISNULL(search_vec_)
      || OB_UNLIKELY(vec_aux_ctdef_->dim_ <= 0 || sort_ctdef_->sort_exprs_.empty())
      || OB_ISNULL(sort_ctdef_->sort_exprs_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF query vector state", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(
                 query_vector_alloc_, sort_rtdef_->eval_ctx_, search_vec_, query_vector))) {
    LOG_WARN("failed to evaluate current query vector", K(ret));
  } else if (OB_UNLIKELY(query_vector.length() != vec_aux_ctdef_->dim_ * sizeof(float))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF query vector dimension mismatch", K(ret), K(query_vector.length()), K(vec_aux_ctdef_->dim_));
  } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(
                 *sort_ctdef_->sort_exprs_[0], *sort_rtdef_->eval_ctx_, dis_type_))) {
    LOG_WARN("failed to get distance type", K(ret));
  } else if (OB_FAIL(ob_write_string(query_vector_alloc_, query_vector, real_search_vec_))) {
    LOG_WARN("failed to copy query vector", K(ret));
  } else if (ObExprVectorDistance::ObVecDisType::COSINE == dis_type_) {
    // Own the bytes: normalization must not overwrite an expression's cached datum.
    if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                   vec_aux_ctdef_->dim_, reinterpret_cast<float *>(real_search_vec_.ptr()),
                   reinterpret_cast<float *>(real_search_vec_.ptr())))) {
      LOG_WARN("failed to normalize query vector", K(ret));
    } else {
      need_norm_ = true;
      dis_type_ = ObExprVectorDistance::ObVecDisType::DOT;
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_ns_id()
{
  int ret = OB_SUCCESS;
  ns_id_ = share::ObIvfCacheMgrKey::INVALID_NS_ID;
  ltable_id_ = share::OB_INVALID_LOGICAL_TABLE_ID;

  if (OB_NOT_NULL(vec_aux_rtdef_)) {
    ObDASBaseRtDef *inv_rtdef = vec_aux_rtdef_->get_inv_idx_scan_rtdef();
    if (OB_NOT_NULL(inv_rtdef) && DAS_OP_TABLE_SCAN == inv_rtdef->op_type_) {
      const ObLogicalTableRtDef &lt_rt = static_cast<ObDASScanRtDef *>(inv_rtdef)->lt_rt_;
      if (lt_rt.has_resolved_ns_lt()) {
        ns_id_ = static_cast<int64_t>(lt_rt.namespace_id_);
        ltable_id_ = lt_rt.logical_table_id_;
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(release_input_iter(inv_idx_scan_iter_))) {
    LOG_WARN("failed to release inv_idx_scan_iter_", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(centroid_iter_) && OB_FAIL(release_input_iter(centroid_iter_))) {
    LOG_WARN("failed to release centroid_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(cid_vec_iter_) && OB_FAIL(release_input_iter(cid_vec_iter_))) {
    LOG_WARN("failed to release cid_vec_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_cid_iter_) && OB_FAIL(release_input_iter(rowkey_cid_iter_))) {
    LOG_WARN("failed to release rowkey_cid_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(brute_iter_) && OB_FAIL(release_input_iter(brute_iter_))) {
    LOG_WARN("failed to release brute_iter_", K(ret));
  }
  if (OB_NOT_NULL(data_filter_iter_) && OB_FAIL(release_input_iter(data_filter_iter_))) {
    LOG_WARN("failed to release data_filter_iter_", K(ret));
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  centroid_iter_ = nullptr;
  cid_vec_iter_ = nullptr;
  rowkey_cid_iter_ = nullptr;
  brute_iter_ = nullptr;
  data_filter_iter_ = nullptr;

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
  }

  saved_rowkeys_.reset();
  pre_fileter_rowkeys_.reset();
  // Reset memory context and clean up associated resources
  disk_bbq_lookup_alloc_.reset();
  reset_memory_context();
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  tmp_main_rowkey_.reset();
  vec_op_alloc_.reset();
  persist_alloc_.reset();
  query_vector_alloc_.reset();
  // Release any centroid ref_cnt_ before tearing down the cache mgr guard.
  {
    int tmp_ret = batch_guard_.release_all();
    if (common::OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to release batch_guard_ in inner_release", K(tmp_ret));
    }
  }
  center_cache_guard_.reset();
  iterative_filter_ctx_.reset();
  adaptive_ctx_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  ObDasVecScanUtils::release_scan_param(centroid_scan_param_);
  ObDasVecScanUtils::release_scan_param(cid_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_cid_scan_param_);
  ObDasVecScanUtils::release_scan_param(brute_scan_param_);
  ObDasVecScanUtils::release_scan_param(data_filter_scan_param_);

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  real_search_vec_ = nullptr;
  data_filter_ctdef_ = nullptr;
  data_filter_rtdef_ = nullptr;
  profile_ = nullptr;

  return ret;
}

int ObDASIvfBaseScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current_profile = common::get_current_profile();
  common::ObOpProfile<common::ObMetric> *metric_profile =
      OB_NOT_NULL(current_profile)
          && current_profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER
          ? current_profile
          : nullptr;
  common::ScopedTimer total_search_timer(
      common::ObMetricId::VEC_TOTAL_SEARCH_TIME,
      limit_param_.limit_ + limit_param_.offset_ > 0 ? metric_profile : nullptr);
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(false/*is_vectorized*/))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process ivf scan state", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey iter", K(ret));
  } else if (OB_FAIL(get_next_saved_rowkey())) {
    LOG_WARN("failed to get saved rowkey", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(metric_profile)) {
    INC_METRIC_VAL(common::ObMetricId::VEC_OUTPUT_ROW_COUNT, 1);
  }

  return ret;
}

int ObDASIvfBaseScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current_profile = common::get_current_profile();
  common::ObOpProfile<common::ObMetric> *metric_profile =
      OB_NOT_NULL(current_profile)
          && current_profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER
          ? current_profile
          : nullptr;
  common::ScopedTimer total_search_timer(
      common::ObMetricId::VEC_TOTAL_SEARCH_TIME,
      limit_param_.limit_ + limit_param_.offset_ > 0 ? metric_profile : nullptr);
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(true/*is_vectorized*/))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process ivf scan state", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey iter", K(ret));
  } else if (OB_FALSE_IT(saved_rowkeys_itr_->set_batch_size(capacity))) {
  } else if (OB_FAIL(get_next_saved_rowkeys(count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get saved rowkeys", K(ret));
    }
  }
  if (OB_SUCC(ret) && count > 0 && OB_NOT_NULL(metric_profile)) {
    INC_METRIC_VAL(common::ObMetricId::VEC_OUTPUT_ROW_COUNT, count);
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  adaptive_ctx_.reuse();
  if (is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post(is_vectorized))) {
      LOG_WARN("failed to process ivf_scan post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(mem_context_->get_arena_allocator(), is_vectorized))) {
    if (OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == ret) {
      prefilter_aborted_ = true;
    }
    LOG_WARN("failed to process ivf_scan pre", K(ret));
  }
  return ret;
}

int ObDASIvfBaseScanIter::process_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current_profile = common::get_current_profile();
  if (OB_NOT_NULL(current_profile)
      && current_profile->get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ITER
      && OB_NOT_NULL(vec_aux_ctdef_)) {
    SET_METRIC_VAL(common::ObMetricId::VEC_TRY_PATH,
                   static_cast<int64_t>(profile_try_path_));
    SET_METRIC_VAL(common::ObMetricId::VEC_CURRENT_PATH,
                   static_cast<int64_t>(profile_current_path_));
    SET_METRIC_VAL(common::ObMetricId::VEC_SWITCH_REASON,
                   static_cast<int64_t>(profile_switch_reason_));
  }
  can_retry_ = ObDASIterType::DAS_ITER_ASYNC_IVF_SCAN != get_type() && check_if_can_retry();
  if (OB_FAIL(do_ivf_scan(is_vectorized))) {
    LOG_WARN("failed to process ivf_scan pre", K(ret));
  }

  if (OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == ret && can_retry_) {
    LOG_INFO("index adaptive scan need retry", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
    const ObVecIdxAdaTryPath old_path = vec_idx_try_path_;
    ObVecIdxPathSwitchReason switch_reason = ObVecIdxPathSwitchReason::VEC_PATH_NO_SWITCH;
    if (old_path == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
      switch_reason = is_pre_filter_cost_too_high()
                          ? ObVecIdxPathSwitchReason::VEC_PATH_IVF_PRE_FILTER_COST_RATIO_TOO_HIGH
                          : ObVecIdxPathSwitchReason::VEC_PATH_UNKNOWN_SWITCH_REASON;
    } else if (old_path == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
      switch_reason = adaptive_ctx_.can_use_vec_pri_opt_
                          ? ObVecIdxPathSwitchReason::VEC_PATH_ITER_FILTER_SELECTIVITY_TOO_LOW
                          : ObVecIdxPathSwitchReason::VEC_PATH_IVF_ITER_DISTANCE_CALC_OVER_LIMIT;
    }
    if (OB_FAIL(reset_filter_path())) {
      LOG_WARN("failed to reset filter path", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
    } else if (old_path == vec_idx_try_path_) {
      ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
      LOG_WARN("adaptive retry requires a different filter path", K(ret), K(old_path));
    } else if (OB_FAIL(record_profile_path_switch(old_path, switch_reason))) {
      LOG_WARN("failed to record ivf profile path switch", K(ret), K(old_path), K(vec_idx_try_path_));
    } else if (OB_FAIL(do_ivf_scan(is_vectorized))) {
      LOG_WARN("failed to process ivf_scan pre", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("ivf scan stat info", K_(vec_index_type), K_(vec_idx_try_path), K(adaptive_ctx_), K(scan_tablet_size_), K(centroid_tablet_id_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_rowkeys_itr())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to gen rowkeys itr", K(saved_rowkeys_));
    }
  }

  reset_memory_context();

  prefilter_aborted_ = false;
  return ret;
}

int ObDASIvfBaseScanIter::check_iter_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double iter_selectivity = double(adaptive_ctx_.iter_res_row_cnt_) /  double(adaptive_ctx_.iter_filter_row_cnt_);
  double output_row_cnt = iter_selectivity * adaptive_ctx_.row_count_;
  if (adaptive_ctx_.iter_times_ > 2) {
    if (adaptive_ctx_.can_use_vec_pri_opt_) {
      ret = (iter_selectivity <= ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_ROWKEY) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    } else {
      ret = (adaptive_ctx_.vec_dist_calc_cnt_ > ObVecIdxExtraInfo::MAX_IVF_POST_DIST_CALC_CNT
            && output_row_cnt < ObVecIdxExtraInfo::MAX_IVF_PRE_ROW_CNT_WITH_IDX
            && iter_selectivity <= ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_IDX) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("switch path check iter filter need retry", K(ret), K(adaptive_ctx_), K(iter_selectivity), K(output_row_cnt), K(scan_tablet_size_), K(centroid_tablet_id_), K(strategy_));
  }
  return ret;
}

bool ObDASIvfBaseScanIter::is_pre_filter_cost_too_high()
{
  double pre_selectivity =
      double(adaptive_ctx_.pre_scan_row_cnt_) / double(adaptive_ctx_.row_count_);
  if (adaptive_ctx_.can_use_vec_pri_opt_) {
    pre_selectivity =
        double(adaptive_ctx_.vec_dist_calc_cnt_) / double(adaptive_ctx_.cid_vec_scan_rows_);
  }
  return adaptive_ctx_.pre_scan_row_cnt_ >= IVF_MAX_BRUTE_FORCE_SIZE
         && (adaptive_ctx_.can_use_vec_pri_opt_
                 ? pre_selectivity > ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_ROWKEY
                 : pre_selectivity > ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_IDX);
}

int ObDASIvfBaseScanIter::check_pre_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double pre_selectivity = double(adaptive_ctx_.pre_scan_row_cnt_) / double(adaptive_ctx_.row_count_);
  if (adaptive_ctx_.can_use_vec_pri_opt_) pre_selectivity =  double(adaptive_ctx_.vec_dist_calc_cnt_) / double(adaptive_ctx_.cid_vec_scan_rows_);
  if (is_pre_filter_cost_too_high()) {
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("switch path check pre filter need retry", K(ret), K(adaptive_ctx_), K(pre_selectivity), K(scan_tablet_size_), K(centroid_tablet_id_), K(strategy_));
  }
  return ret;
}

int ObDASIvfBaseScanIter::reset_filter_path()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_);
  ObPlanStat* plan_stat = nullptr;
  if (strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST && vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
    // RT first mode, do nothing
  } else if (OB_ISNULL(plan_ctx->get_phy_plan())) {
    // remote scan, phy plan is null, do nothing, just use try path in ctdef
    LOG_WARN("plan ctx is null", K(ret), KP(plan_ctx));
  } else if (OB_FALSE_IT(plan_stat = const_cast<ObPlanStat*>(&(plan_ctx->get_phy_plan()->stat_)))) {
  } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
    vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
    double iter_selectivity = double(adaptive_ctx_.iter_res_row_cnt_) / double(adaptive_ctx_.iter_filter_row_cnt_);
    adaptive_ctx_.selectivity_ = iter_selectivity;
    vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
  }
  if (OB_FAIL(ret) || OB_ISNULL(plan_stat)) {
  } else if (OB_FAIL(updata_vec_exec_ctx(plan_stat))) {
    LOG_WARN("failed to updata vec exec ctx", K(ret), K(vec_idx_try_path_));
  } else {
    can_retry_ = false;
    reuse_cid_ctx();
  }
  return ret;
}

int ObDASIvfBaseScanIter::updata_vec_exec_ctx(ObPlanStat* plan_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan stat is null", K(ret), KP(plan_stat));
  } else {
    int32_t record_count = plan_stat->vec_index_exec_ctx_.record_count_ + 1;
    if (record_count < CHANGE_PATH_WINDOW_SIZE) {
      if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
        ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_));
      } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
        ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_));
      }
      ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.record_count_));
    } else {
      double iter_time = plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_;
      double pre_time = plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_;
      iter_time = std::log(iter_time) * DECAY_FACTOR;
      pre_time = std::log(pre_time) * DECAY_FACTOR;
      FLOG_INFO("begin to reset plan stat filter path", K(plan_stat->vec_index_exec_ctx_.record_count_), 
      K(plan_stat->vec_index_exec_ctx_.cur_path_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.record_count_), 0);
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_), static_cast<int64_t>(iter_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_), static_cast<int64_t>(pre_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.cur_path_), static_cast<uint8_t>(vec_idx_try_path_));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::build_cid_vec_query_rowkey(const ObString &cid,
                                                     bool is_min,
                                                     int64_t rowkey_cnt,
                                                     common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // rowkey: [cid, rowkey]
    obj_ptr[0].set_varbinary(cid);
    for (int64_t i = 1; i < rowkey_cnt; ++i) {
      if (is_min) {
        obj_ptr[i].set_min_value();
      } else {
        obj_ptr[i].set_max_value();
      }
    }

    rowkey.assign(obj_ptr, rowkey_cnt);
  }
  return ret;
}

int ObDASIvfBaseScanIter::build_cid_vec_query_range(const ObString &cid,
                                                    int64_t rowkey_cnt,
                                                    ObNewRange &cid_pri_key_range)
{
  int ret = OB_SUCCESS;
  ObRowkey cid_rowkey_min;
  ObRowkey cid_rowkey_max;
  if (cid.empty()) {
    cid_pri_key_range.set_whole_range();
  } else {
    if (OB_FAIL(build_cid_vec_query_rowkey(cid, true, rowkey_cnt, cid_rowkey_min))) {
      LOG_WARN("failed to build cid vec query rowkey", K(ret));
    } else if (OB_FAIL(build_cid_vec_query_rowkey(cid, false, rowkey_cnt, cid_rowkey_max))) {
      LOG_WARN("failed to build cid vec query rowkey", K(ret));
    } else {
      cid_pri_key_range.start_key_ = cid_rowkey_min;
      cid_pri_key_range.end_key_ = cid_rowkey_max;
    }
  }

  return ret;
}

void ObDASIvfBaseScanIter::set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
{
  centroid_tablet_id_ = related_tablet_ids.centroid_tablet_id_;
  cid_vec_tablet_id_ = related_tablet_ids.cid_vec_tablet_id_;
  rowkey_cid_tablet_id_ = related_tablet_ids.rowkey_cid_tablet_id_;
  sq_meta_tablet_id_ = related_tablet_ids.special_aux_tablet_id_;
  pq_centroid_tablet_id_ = related_tablet_ids.special_aux_tablet_id_;
  brute_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  data_filter_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
}

int ObDASIvfBaseScanIter::get_next_saved_rowkey()
{
  int ret = OB_SUCCESS;
  if (saved_rowkeys_itr_->is_get_from_scan_iter()) {
    if (OB_FAIL(saved_rowkeys_itr_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from scan iter", K(ret));
      }
    }
  } else {
    ObRowkey rowkey;
    if (OB_FAIL(saved_rowkeys_itr_->get_next_row(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
      }
    } else {
      const ExprFixedArray& ivf_res_exprs = vec_aux_ctdef_->result_output_;
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < ivf_res_exprs.count(); ++i) {
        ObExpr *expr = ivf_res_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else {
          ObDatum &datum = expr->locate_datum_for_write(*vec_aux_rtdef_->eval_ctx_);
          if (OB_FAIL(datum.from_obj(rowkey.get_obj_ptr()[i]))) {
            LOG_WARN("failed to from obj", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_next_saved_rowkeys(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (saved_rowkeys_itr_->is_get_from_scan_iter()) {
    if (OB_FAIL(saved_rowkeys_itr_->get_next_rows(count))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from scan iter", K(ret));
      }
    }
  } else {
    ObSEArray<ObRowkey, 8> rowkeys;
    if (OB_FAIL(saved_rowkeys_itr_->get_next_rows(rowkeys, count))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next next row from adaptor iter", K(ret));
      }
    } else if (count > 0) {
      const ExprFixedArray& ivf_res_exprs = vec_aux_ctdef_->result_output_;
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(count);
      for (int64_t idx_exp = 0; OB_SUCC(ret) && idx_exp < ivf_res_exprs.count(); ++idx_exp) {
        ObExpr *expr = ivf_res_exprs.at(idx_exp);
        ObDatum *datum = nullptr;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_ISNULL(datum = expr->locate_datums_for_update(*vec_aux_rtdef_->eval_ctx_, count))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(expr));
        } else {
          for (int64_t idx_key = 0; OB_SUCC(ret) && idx_key < count; ++idx_key) {
            guard.set_batch_idx(idx_key);
            if (OB_FAIL(datum[idx_key].from_obj(rowkeys[idx_key].get_obj_ptr()[idx_exp]))) {
              LOG_WARN("fail to from obj", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
          }
        }
      }
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::gen_rowkeys_itr_brute(ObDASIter *scan_iter)
{
  int ret = OB_SUCCESS;

  void *iter_buff = nullptr;
  if (OB_ISNULL(scan_iter) || OB_ISNULL(scan_iter->get_output())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_iter is null.", K(ret), KP(scan_iter));
  } else if (!scan_iter->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_iter is not inited.", K(ret));
  } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryRowkeyIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator rowkey iter.", K(ret));
  } else if (OB_FALSE_IT(saved_rowkeys_itr_ = new (iter_buff) ObVectorQueryRowkeyIterator())) {
  } else {
    if (OB_FAIL(saved_rowkeys_itr_->init(scan_iter))) {
      LOG_WARN("iter init failed.", K(ret));
    }
  }

  return ret;
}

int64_t ObDASIvfBaseScanIter::get_nprobe(const common::ObLimitParam &limit_param, int64_t enlargement_factor /*= 1*/)
{
  int64_t nprobe = INT64_MAX;
  int64_t sum = limit_param.limit_ + limit_param.offset_;

  if (sum > 0 && sum <= (INT64_MAX / enlargement_factor)) {
    nprobe = sum * enlargement_factor;
  }
  return nprobe;
}

int64_t ObDASIvfBaseScanIter::get_heap_size(const int64_t limit_k, const double select_ratio)
{
  int64_t heap_size = limit_k;
  /**
   * The minimum magnification is 2x and the maximum is 10x.
   * so if select_ratio >= 0.5, use 2x
   * if select_ratio < 0.1, use 10x
   * otherwise use limit_k / select_ratio
  */
  if (select_ratio > 0.0 && select_ratio < 1.0) {
    if (select_ratio >= 0.5) heap_size = 2 * limit_k;
    else if (select_ratio <= 0.1) heap_size = 10 * limit_k;
    else heap_size = std::ceil(limit_k / select_ratio);
  }
  return heap_size;
}

int64_t ObDASIvfBaseScanIter::calc_next_nprobe(
    const int64_t limit_k,
    const int64_t matched_count) const
{
  int64_t next_nprobe = nprobes_;
  if (limit_k > 0) {
    const int64_t left_search = OB_MAX(
        limit_k - matched_count, static_cast<int64_t>(0));
    next_nprobe = OB_MIN(
        nprobes_,
        OB_MAX(static_cast<int64_t>(1),
               static_cast<int64_t>(nprobes_ * static_cast<double>(left_search)
                                    / static_cast<double>(limit_k) + 1)));
  }
  return next_nprobe;
}

int ObDASIvfBaseScanIter::gen_near_cid_heap_from_cache(ObIvfICache &cent_cache,
                                                   share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
                                                   bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  ObCenterId center_id;
  center_id.tablet_id_ = centroid_tablet_id_.id();
  CenterSaveMode center_save_mode = CenterSaveMode::SHALLOW_COPY_CENTER_VEC;
  if (is_ns_id_valid()) {
    if (cent_cache.get_cache_type() != IvfCacheType::IVF_MAP_CENTROID_CACHE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cache type for map centroid path", K(ret),
               "actual_type", cent_cache.get_cache_type());
    } else {
      // ns_id path: ref_cnt_ via batch_guard_ protects cid_vec_ across the
      // bucket-RLock boundary; no outer RLockGuard.
      ObIvfMapCentCache *map_cache = static_cast<ObIvfMapCentCache *>(&cent_cache);
      ObSEArray<share::ObCentroidInfo, 64> centroids;
      centroids.set_attr(ObMemAttr(MTL_ID(), "IvfAcqCent"));
      if (OB_FAIL(map_cache->acquire_visible_centroids(snapshot_->version(),
                                                       batch_guard_, centroids))) {
        LOG_WARN("fail to acquire visible centroids", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < centroids.count(); ++i) {
        center_id.center_id_ = centroids.at(i).cid_;
        if (OB_FAIL(nearest_cid_heap.push_center(center_id, centroids.at(i).cid_vec_, dim_, center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret), K(center_id));
        }
      }
      // batch_guard_ holds ref_cnt_ across this boundary; release at the next
      // entry or in inner_reuse / inner_release.
    }
  } else {
    // regular mode: sequential traversal by index (valid center id start from 1)
    // Array-based ObIvfCentCache shares rwlock_ with writer paths.
    RWLock::RLockGuard guard(cent_cache.get_lock());
    uint64_t capacity = cent_cache.get_count();
    float *cid_vec = nullptr;
    for (uint64_t i = 1; i <= capacity && OB_SUCC(ret); ++i) {
      if (OB_FAIL(cent_cache.read_centroid(i, cid_vec))) {
        LOG_WARN("fail to read centroid", K(ret), K(i));
      } else if (FALSE_IT(center_id.center_id_ = i)) {
      } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, cid_vec, dim_, center_save_mode))) {
        LOG_WARN("failed to push center.", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::write_centroids_from_scan(
    ObIvfICache &cent_cache,
    bool is_vectorized,
    bool use_add_centroid)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObCenterId cent_id;
  SCN min_scn = SCN::min_scn();
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);

  if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObString cid = cid_datum[i].get_string();
        ObString cid_vec = cid_vec_datum[i].get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &tmp_allocator, ObLongTextType, CS_TYPE_BINARY, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                       cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
          LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
        } else if (use_add_centroid) {
          if (OB_FAIL(cent_cache.add_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()),
                         cid_vec.length() / sizeof(float), &min_scn))) {
            LOG_WARN("fail to add centroid", K(ret), K(cent_id));
          }
        } else {
          if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(cent_id));
          }
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
  } else {
    centroid_iter_->clear_evaluated_flag();
    ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
    ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    ObDatum &cid_vec_datum = cid_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);

    for (int i = 0; OB_SUCC(ret); ++i) {
      if (OB_FAIL(centroid_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan centroid iter", K(ret));
        }
      } else {
        ObString cid = cid_datum.get_string();
        ObString cid_vec = cid_vec_datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &tmp_allocator, ObLongTextType, CS_TYPE_BINARY, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                       cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
          LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
        } else if (use_add_centroid) {
          if (OB_FAIL(cent_cache.add_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()),
                         cid_vec.length() / sizeof(float), &min_scn))) {
            LOG_WARN("fail to add centroid", K(ret), K(cent_id));
          }
        } else {
          if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(cent_id));
          }
        }
      }
    }
    int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
    if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
      LOG_WARN("failed to reuse centroid iter.", K(ret));
    } else {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::try_write_centroid_cache(
    ObIvfICache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already writed centroids cache, skip", K(ret));
  } else {
    const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
    ObDASScanRtDef *centroid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_centroid_tbl_idx());
    if (OB_FAIL(do_table_full_scan(is_vectorized,
                                   centroid_ctdef,
                                   centroid_rtdef,
                                   centroid_iter_,
                                   centroid_tablet_id_,
                                   centroid_iter_first_scan_,
                                   centroid_scan_param_))) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else {
      // Note: always false, because if ns_id_ is valid, will warm up the cache
      bool use_add_centroid = is_ns_id_valid();
      if (OB_FAIL(write_centroids_from_scan(cent_cache, is_vectorized, use_add_centroid))) {
        LOG_WARN("failed to write centroids from scan", K(ret), K(use_add_centroid));
      }
    }

    if (OB_SUCC(ret)) {
    if (cent_cache.get_count() > 0) {
      // if ns_id_ is valid, not build hgraph
      if (cent_cache.can_build_hgraph()) {
        ObVectorIndexParam hgraph_param = vec_index_param_;
        if (hgraph_param.dim_ <= 0) {
          hgraph_param.dim_ = dim_;
        }
        if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT && need_norm_) {
          hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_COS;
        } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
          hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_IP;
        } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ||
                   dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED) {
          hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_L2;
        }
        if (OB_FAIL(cent_cache.build_hgraph_and_release_centers(hgraph_param))) {
          LOG_WARN("failed to build hgraph when writing centroid cache", K(ret), K(centroid_tablet_id_));
          cent_cache.reuse();
        } else {
          cent_cache.set_completed();
          LOG_INFO("success to write centroid table cache with hgraph", K(centroid_tablet_id_));
        }
      } else {
        cent_cache.set_completed();
        LOG_DEBUG("success to write centroid table cache", K(centroid_tablet_id_));
      }
    } else {
      cent_cache.reuse();
      LOG_DEBUG("Empty centroid table, no need to set cache", K(centroid_tablet_id_));
    }
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_centers_cache(bool is_vectorized,
                                        bool is_pq_centers, 
                                        ObIvfCacheMgrGuard &cache_guard,
                                        ObIvfICache *&cent_cache, 
                                        bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  is_cache_usable = false;
  cent_cache = nullptr;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);

  // pq/flat both use centroid_tablet_id_
  ObIvfCacheMgrKey cache_key(centroid_tablet_id_,
      is_pq_centers ? ObIvfCacheMgrKey::INVALID_NS_ID : ns_id_,
      is_pq_centers ? ObIvfCacheMgrKey::INVALID_LTABLE_ID : static_cast<uint64_t>(ltable_id_));
  bool cache_ready = false;
  int64_t role_epoch = 0;
  int state_ret = OB_SUCCESS;
  if (OB_ISNULL(vec_index_service) || OB_ISNULL(centroid_ctdef)) {
    LOG_DEBUG("vec index service or centroid ctdef is null, skip cache",
              KP(vec_index_service), KP(centroid_ctdef));
  } else if (OB_SUCCESS != (state_ret = vec_index_service->get_ls_ivf_cache_state(
                 ls_id_, cache_ready, role_epoch))) {
    cache_guard.reset();
    LOG_WARN_RET(state_ret, "fail to get ivf cache state, fallback to persistent scan",
                 K(state_ret), K_(ls_id));
  } else if (!cache_ready) {
    cache_guard.reset();
    LOG_DEBUG("ivf cache is unavailable before guard acquisition, fallback to persistent scan",
              K(state_ret), K_(ls_id), K(role_epoch));
  } else {
    if (cache_guard.is_valid() && cache_guard.get_role_epoch() != role_epoch) {
      cache_guard.reset();
    }
    if (!cache_guard.is_valid() && OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
                                     ls_id_, cache_key, vec_index_param_, dim_,
                                     centroid_ctdef->ref_table_id_, cache_guard))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        if (is_ns_id_valid() && vec_index_service->is_ns_id_hot(ls_id_, centroid_tablet_id_, ns_id_)) {
          LOG_WARN("ns_id is hot, but not found in cache", K(ls_id_), K(centroid_tablet_id_), K(ns_id_));
        } else {
          LOG_INFO("cache mgr not exist, skip", K(ret), K(ls_id_), K(centroid_tablet_id_), K(vec_index_param_));
        }
        is_cache_usable = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get ObPluginVectorIndexAdapter", K(ret), K(ls_id_), K(centroid_tablet_id_),
                 K(vec_index_param_));
      }
    } else if (FALSE_IT(cache_guard.set_role_epoch(role_epoch))) {
    } else if (OB_SUCCESS != (state_ret = vec_index_service->get_ls_ivf_cache_state(
                   ls_id_, cache_ready, role_epoch))) {
      cache_guard.reset();
      LOG_WARN_RET(state_ret, "fail to recheck ivf cache state, fallback to persistent scan",
                   K(state_ret), K_(ls_id));
    } else if (!cache_ready || cache_guard.get_role_epoch() != role_epoch) {
      cache_guard.reset();
      LOG_DEBUG("ivf cache role generation changed during guard acquisition, fallback",
                K(state_ret), K_(ls_id), K(cache_ready), K(role_epoch));
    } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null cache mgr", K(ret));
    } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<ObIvfICache>(
                 is_pq_centers
                     ? IvfCacheType::IVF_PQ_CENTROID_CACHE
                     : (is_ns_id_valid() ? IvfCacheType::IVF_MAP_CENTROID_CACHE : IvfCacheType::IVF_CENTROID_CACHE),
                 cent_cache))) {
      LOG_WARN("fail to get or create cache node", K(ret), K(is_pq_centers), K(ns_id_));
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        is_cache_usable = false;
        ret = OB_SUCCESS;
      }
    } else if (!cent_cache->is_completed()) {
      if (is_ns_id_valid()) {
        LOG_WARN("ns_id is valid, but cache is not completed, skip!", K(ret), K(is_vectorized),
                 KPC(cent_cache), K(is_pq_centers));
        is_cache_usable = false;
        ret = OB_SUCCESS;
      } else if (cent_cache->set_writing_if_idle()) {
        // write cache
        if (is_pq_centers) {
          DEBUG_SYNC(IVF_PQ_CENTROID_CACHE_BEFORE_BUILD);
        } else {
          DEBUG_SYNC(IVF_CENTROID_CACHE_BEFORE_BUILD);
        }
        const common::ObMetricId build_metric_id =
            is_pq_centers
                ? common::ObMetricId::VEC_IVF_PQ_CENTROID_CACHE_BUILD_TIME
                : common::ObMetricId::VEC_IVF_CENTROID_CACHE_BUILD_TIME;
        common::ScopedTimer build_timer(build_metric_id, profile_);
        int tmp_ret = is_pq_centers ?
            try_write_pq_centroid_cache(*cent_cache, is_vectorized) :
            try_write_centroid_cache(*cent_cache, is_vectorized);
        if (OB_TMP_FAIL(tmp_ret)) {
          LOG_WARN("fail to try write centroid cache", K(ret), K(is_vectorized), KPC(cent_cache), K(is_pq_centers));
        } else {
          is_cache_usable = cent_cache->is_completed();
        }
      } else {
        LOG_INFO("other threads already writed centroids cache, skip", K(ret));
      }
    } else {
      // read cache
      is_cache_usable = true;
    }
  }
  return ret;
}

template <typename T>
int ObDASIvfBaseScanIter::generate_nearest_cid_heap(
    bool is_vectorized,
    T &nearest_cid_heap,
    bool save_center_vec /*= false*/,
    ObIvfICache *cent_cache /*= nullptr*/,
    bool is_cache_usable /*= false*/)
{
  int ret = OB_SUCCESS;

  // SPFresh mode visibility filtering is performed inside
  // ObIvfMapCentCache::acquire_visible_centroids (called via
  // gen_near_cid_heap_from_cache below). All visible centroids participate
  // in nprobe selection; ref_cnt_ on the matching batch_guard_ keeps cid_vec_
  // alive while the heap holds shallow pointers to it.

  if (is_cache_usable && OB_NOT_NULL(cent_cache)) {
    if (OB_FAIL(gen_near_cid_heap_from_cache(*cent_cache, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from cache", K(ret), K(center_cache_guard_));
    }
  } else {
    const common::ObMetricId fallback_metric_id =
        is_ns_id_valid()
            ? common::ObMetricId::VEC_IVF_MAP_CENTROID_CACHE_FALLBACK_SCAN_TIME
            : common::ObMetricId::VEC_IVF_CENTROID_CACHE_FALLBACK_SCAN_TIME;
    common::ScopedTimer fallback_timer(fallback_metric_id, profile_);
    if (OB_FAIL(gen_near_cid_heap_from_table(is_vectorized, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from table", K(ret), K(center_cache_guard_));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::gen_near_cid_heap_from_table(
  bool is_vectorized,
  share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
  bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  ObDASScanRtDef *centroid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_centroid_tbl_idx());
  
  // if no have center table cache, need deep copy center vec, else shallol copy
  // todo(wmj): need fit ivf adaptor cache
  CenterSaveMode center_save_mode = save_center_vec ? CenterSaveMode::DEEP_COPY_CENTER_VEC : CenterSaveMode::NOT_SAVE_CENTER_VEC;
  if (OB_FAIL(do_table_full_scan(is_vectorized,
                                  centroid_ctdef,
                                  centroid_rtdef,
                                  centroid_iter_,
                                  centroid_tablet_id_,
                                  centroid_iter_first_scan_,
                                  centroid_scan_param_))) {
    LOG_WARN("failed to do centroid table scan", K(ret));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      ObCenterId center_id;

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObString cid = cid_datum[i].get_string();
        ObString cid_vec = cid_vec_datum[i].get_string();
        
        if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
          LOG_WARN("failed to get center id from string", K(ret));
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &mem_context_->get_arena_allocator(),
                        ObLongTextType,
                        CS_TYPE_BINARY,
                        cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, reinterpret_cast<float *>(cid_vec.ptr()), dim_, center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
  } else {
    centroid_iter_->clear_evaluated_flag();
    ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
    ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    ObDatum &cid_vec_datum = cid_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);

    ObCenterId center_id;
    for (int i = 0; OB_SUCC(ret); ++i) {
      if (OB_FAIL(centroid_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan vid rowkey iter", K(ret));
        }
      } else {
        ObString cid = cid_datum.get_string();
        ObString cid_vec = cid_vec_datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&mem_context_->get_arena_allocator(), ObLongTextType,
                                                              CS_TYPE_BINARY, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (cid_vec.empty()) {
          // ignoring null vector.
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
          LOG_WARN("failed to get center id from string", K(ret));
        } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, reinterpret_cast<float *>(cid_vec.ptr()), dim_,
                                                        center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }

    if (ret == OB_ITER_END) {
      if (OB_FAIL(centroid_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }
  
  return ret;
}

int ObDASIvfBaseScanIter::prepare_cid_range(
  const ObDASScanCtDef *cid_vec_ctdef, 
  int64_t &cid_vec_column_count,
  int64_t &cid_vec_pri_key_cnt,
  int64_t &rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cid_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef is null", K(ret), KP(cid_vec_ctdef));
  } else if (OB_FALSE_IT(cid_vec_column_count = cid_vec_ctdef->access_column_ids_.count())) {
  } else if (OB_UNLIKELY(cid_vec_column_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else {
    cid_vec_pri_key_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT; // access_column_ids_ have cid and vector column + rk
    rowkey_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT;
  }
  return ret;
}

int ObDASIvfBaseScanIter::scan_cid_range(
  const ObString &cid, 
  int64_t cid_vec_pri_key_cnt, 
  const ObDASScanCtDef *cid_vec_ctdef, 
  ObDASScanRtDef *cid_vec_rtdef,
  storage::ObTableScanIterator *&cid_vec_scan_iter)
{
  int ret = OB_SUCCESS;
  ObNewRange cid_pri_key_range;
  if (OB_ISNULL(cid_vec_ctdef) || OB_ISNULL(cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(cid_vec_ctdef), KP(cid_vec_rtdef));
  } else if (OB_FAIL(build_cid_vec_query_range(cid, cid_vec_pri_key_cnt, cid_pri_key_range))) {
    LOG_WARN("failed to build cid vec query rowkey", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(cid_pri_key_range, cid_vec_scan_param_, cid_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to append scan range", K(ret));
  } else if (OB_FAIL(do_aux_table_scan(cid_vec_iter_first_scan_,
                                        cid_vec_scan_param_,
                                        cid_vec_ctdef,
                                        cid_vec_rtdef,
                                        cid_vec_iter_,
                                        cid_vec_tablet_id_))) {
    LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
  } else {
    cid_vec_scan_iter = static_cast<storage::ObTableScanIterator *>(cid_vec_iter_->get_output_result_iter());
    if (OB_ISNULL(cid_vec_scan_iter)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null scan iter", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_rowkey_pre_filter(ObIAllocator& allocator, bool is_vectorized, int64_t max_rowkey_count)
{
  int ret = OB_SUCCESS;
  uint64_t rowkey_count = 0;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer pre_filter_timer(common::ObMetricId::VEC_PRE_FILTER_TIME,
                                       vector_iter_profile);
  while (OB_SUCC(ret) && rowkey_count < max_rowkey_count) {
    inv_idx_scan_iter_->clear_evaluated_flag();
    if (!is_vectorized) {
      ObRowkey *rowkey = nullptr;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        if (OB_ITER_END != ret && OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY != ret) {
          LOG_WARN("failed to get next rowkey", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        rowkey_count++;
        if (OB_FAIL(get_rowkey(allocator, rowkey))) {
          // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
          LOG_WARN("failed to get rowkey", K(ret));
        } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
          LOG_WARN("failed to push rowkey", K(ret));
        }
      }
    } else {
      int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

      int64_t scan_row_cnt = 0;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next rowkey", K(ret), K(scan_row_cnt));
        }
      }

      rowkey_count += scan_row_cnt;
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from inv_idx_scan_iter_", K(ret));
      } else if (scan_row_cnt > 0) {
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey = nullptr;
          // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
          if (OB_FAIL(get_rowkey(allocator, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
            LOG_WARN("store push rowkey", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    // A failed prefilter is not a complete candidate set, including adaptive retry.
    pre_fileter_rowkeys_.reset();
  } else if (OB_ITER_END == ret && rowkey_count > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && rowkey_count > 0 && OB_NOT_NULL(vector_iter_profile)) {
    common::ObProfileSwitcher vector_iter_switcher(vector_iter_profile);
    INC_METRIC_VAL(common::ObMetricId::VEC_PRE_FILTER_ROWS_SCANNED, rowkey_count);
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_main_rowkey_from_cid_vec_datum(ObIAllocator& allocator,
                                                             const ObDASScanCtDef *cid_vec_ctdef,
                                                             const int64_t rowkey_cnt,
                                                             ObRowkey &main_rowkey,
                                                             bool need_alloc /* true */)
{
  int ret = OB_SUCCESS;

  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(cid_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(cid_vec_ctdef));
  } else {
    // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
    // Note: when _enable_defensive_check = 2, cid_vec_out_exprs is [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY
    // DEFENSE_CHECK_COL]
    const ExprFixedArray& cid_vec_out_exprs = cid_vec_ctdef->result_output_;
    if (rowkey_cnt > cid_vec_out_exprs.count() - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_cnt is illegal", K(ret), K(rowkey_cnt), K(cid_vec_out_exprs.count()));
    } else if (need_alloc) {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
      } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
      }
    } else {
      obj_ptr = main_rowkey.get_obj_ptr();
    }
    if (OB_FAIL(ret)) {
    } else {
      int rowkey_idx = 0;
      for (int64_t i = 2; OB_SUCC(ret) && i < cid_vec_out_exprs.count() && rowkey_idx < rowkey_cnt; ++i) {
        ObObj tmp_obj;
        ObExpr *expr = cid_vec_out_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(obj_ptr[rowkey_idx++], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && need_alloc) {
      main_rowkey.assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_pre_filter_rowkey_batch(ObIAllocator &allocator,
                                                  bool is_vectorized,
                                                  int64_t batch_row_count,
                                                  bool &index_end)
{
  int ret = OB_SUCCESS;
  const int64_t row_count_before = pre_fileter_rowkeys_.count();
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      DAS_ITER_ASYNC_IVF_SCAN == get_type() ? nullptr : get_current_vector_iter_profile();
  common::ScopedTimer pre_filter_timer(common::ObMetricId::VEC_PRE_FILTER_TIME,
                                       vector_iter_profile);
  index_end = false;
  if (prefilter_aborted_) {
    // Empty-centroid fallback must not treat the unconsumed suffix as a full scan.
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
    LOG_WARN("cannot resume brute-force fallback after interrupted prefilter", K(ret));
  } else if (!is_vectorized) {
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      inv_idx_scan_iter_->clear_evaluated_flag();
      ObRowkey *rowkey = nullptr;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
        // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
        LOG_WARN("failed to save rowkey", K(ret));
      }
    }
  } else {
    int64_t scan_row_cnt = 0;
    if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row.", K(ret));
      }
      index_end = true;
    }

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
    } else if (scan_row_cnt > 0) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
        guard.set_batch_idx(i);
        ObRowkey *rowkey = nullptr;
        // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
        if (OB_FAIL(get_rowkey(allocator, rowkey))) {
          LOG_WARN("failed to add rowkey", K(ret), K(i));
        } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
          LOG_WARN("failed to save rowkey", K(ret));
        }
      }
    }
  }
  const int64_t scanned_rows = pre_fileter_rowkeys_.count() - row_count_before;
  if (OB_SUCC(ret) && scanned_rows > 0 && OB_NOT_NULL(vector_iter_profile)) {
    common::ObProfileSwitcher vector_iter_switcher(vector_iter_profile);
    INC_METRIC_VAL(common::ObMetricId::VEC_PRE_FILTER_ROWS_SCANNED, scanned_rows);
  }

  return ret;
}

template <typename T>
int ObDASIvfBaseScanIter::calc_vec_dis(T *a, T *b, int dim, float &dis, ObExprVectorDistance::ObVecDisType dis_type)
{
  int ret = OB_SUCCESS;
  if (dis_type != oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    double distance = 0;
    if (OB_FAIL(oceanbase::sql::ObExprVectorDistance::DisFunc<T>::distance_funcs[dis_type](a, b, dim, distance))) {
      LOG_WARN("failed to calculate distance", K(ret), K(dis_type), KP(a), KP(b), K(dim));
    } else {
      dis = distance;
    }
  } else {
    dis = ObVectorL2Distance<T>::l2_square_flt_func(a, b, dim);
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_post_filter(bool is_vectorized, ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      is_iter_filter() ? get_current_vector_iter_profile() : nullptr;
  const int64_t processed_candidate_count = rowkey_dist_map.size();
  const int64_t matched_candidate_count_before = matched_rowkeys.count();
  common::ScopedTimer iter_filter_timer(common::ObMetricId::VEC_ITER_FILTER_TIME,
                                        vector_iter_profile);
  if (vec_aux_ctdef_->can_use_vec_pri_opt()) {
    if (OB_FAIL(do_simple_post_filter(rowkey_dist_map, matched_rowkeys))) {
      LOG_WARN("do_simple_post_filter fail", K(ret));
    }
  } else if (OB_FAIL(do_table_post_filter(is_vectorized, rowkey_dist_map, matched_rowkeys))) {
    LOG_WARN("do_table_post_filter fail", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(vector_iter_profile)) {
    const int64_t matched_candidate_count =
        matched_rowkeys.count() - matched_candidate_count_before;
    common::ObProfileSwitcher vector_iter_switcher(vector_iter_profile);
    INC_METRIC_VAL(common::ObMetricId::VEC_ITER_FILTER_LOOP_CNT, 1);
    INC_METRIC_VAL(common::ObMetricId::VEC_ITER_FILTER_ROW, processed_candidate_count);
    INC_METRIC_VAL(common::ObMetricId::VEC_ITER_FILTER_MATCHED_ROW_COUNT,
                   matched_candidate_count);
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_simple_post_filter(ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys)
{
  int ret = OB_SUCCESS;
  ObIVFRowkeyDistMapIterator rowkey_iter = rowkey_dist_map.begin();
  ObIVFRowkeyDistMapIterator rowkey_end = rowkey_dist_map.end();
  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  ObArray<const ObNewRange *> rk_range;
  const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
  for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
    const ObNewRange *range = &key_range.at(i);
    if (OB_FAIL(rk_range.push_back(range))) {
      LOG_WARN("fail to push back range", K(ret), K(i));
    }
  }
  while (OB_SUCC(ret) && rowkey_iter != rowkey_end) {
    ObRowkey &rowkey = rowkey_iter->first;
    const ObIvfRowkeyDistEntry &entry = rowkey_iter->second;
    bool is_match = false;
    ObNewRange tmp_range;
    if (OB_FAIL(tmp_range.build_range(rk_range.at(0)->table_id_, rowkey))) {
      LOG_WARN("fail to build tmp range", K(ret));
    }
    // do compare
    for (int64_t i = 0; i < rk_range.count() && !is_match && OB_SUCC(ret); i++) {
      if (rk_range.at(i)->compare_with_startkey2(tmp_range) <= 0 && rk_range.at(i)->compare_with_endkey2(tmp_range) >= 0) {
        is_match = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_match && OB_FAIL(matched_rowkeys.push_back(entry))) {
      LOG_WARN("failed to push back.", K(ret), K(rowkey), K(entry));
    } else {
      ++rowkey_iter;
    }
  }

  if (OB_SUCC(ret)) {
    adaptive_ctx_.iter_filter_row_cnt_ += rowkey_dist_map.size();
    adaptive_ctx_.iter_res_row_cnt_ += matched_rowkeys.count();
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_table_post_filter(bool is_vectorized,
    ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys,
    ObOpProfile<ObMetric> *filter_profile)
{
  int ret = OB_SUCCESS;
  // Use point gets only for DiskBBQ post-filter lookups; other IVF scans keep their existing path.
  const bool is_get = OB_NOT_NULL(vec_aux_ctdef_)
      && ObVectorIndexAlgorithmType::VIAT_DISK_BBQ == vec_aux_ctdef_->algorithm_type_;
  ScopedTimer filter_timer(ObMetricId::VEC_ITER_FILTER_TIME,
                          rowkey_dist_map.size() > 0 ? filter_profile : nullptr);
  if (rowkey_dist_map.size() > 0) {
    ObIVFProfileUtil::inc(filter_profile, ObMetricId::VEC_ITER_FILTER_LOOP_CNT, 1);
  }
  int64_t scan_row_cnt = 0;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  int64_t count = 0;
  int64_t matched_count = 0;
  ObIVFRowkeyDistMapIterator rowkey_iter = rowkey_dist_map.begin();
  ObIVFRowkeyDistMapIterator rowkey_end = rowkey_dist_map.end();
  while (OB_SUCC(ret) && rowkey_iter != rowkey_end) {
    const int64_t count_before_batch = count;
    bool scan_attempted = false;
    bool scan_opened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && rowkey_iter != rowkey_end; ++i) {
      ObRowkey &rowkey = rowkey_iter->first;
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              rowkey, data_filter_scan_param_, data_filter_ctdef_->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret), K(i), K(count));
      } else {
        ++count;
        ++rowkey_iter;
        LOG_DEBUG("add filter rowkey", K(i), K(rowkey), K(count));
      }
    }
    if (nullptr != filter_profile && count > count_before_batch) {
      // Preserve prior rounds and count only the keys added by this batch, including partial work.
      ObIVFProfileUtil::inc(filter_profile, ObMetricId::VEC_ITER_FILTER_ROW, count - count_before_batch);
    }
    if (OB_SUCC(ret)) {
      scan_attempted = true;
      LOG_DEBUG("begin IVF filter batch scan", K(is_get), K_(ls_id),
                K_(data_filter_tablet_id), K_(data_filter_iter_first_scan),
                "key_count", data_filter_scan_param_.key_ranges_.count());
      if (OB_FAIL(do_aux_table_scan(data_filter_iter_first_scan_,
                                   data_filter_scan_param_,
                                   data_filter_ctdef_,
                                   data_filter_rtdef_,
                                   data_filter_iter_,
                                   data_filter_tablet_id_,
                                   is_get))) {
        LOG_WARN("failed to do data filter table scan.", K(ret), K(count), K(data_filter_iter_first_scan_));
      } else {
        scan_opened = true;
        LOG_DEBUG("IVF filter batch scan opened", K(is_get), K_(ls_id),
                  K_(data_filter_tablet_id), K_(data_filter_iter_first_scan),
                  "key_count", data_filter_scan_param_.key_ranges_.count());
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("skip IVF filter batch read after preparation failure", K(ret),
               K(scan_attempted), K(scan_opened), K(is_get), K_(ls_id),
               K_(data_filter_tablet_id), K_(data_filter_iter_first_scan),
               "key_count", data_filter_scan_param_.key_ranges_.count());
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(data_filter_iter_)
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObRowkey &main_rowkey = tmp_main_rowkey_;
          ObIvfRowkeyDistEntry* entry = nullptr;
          if (OB_FAIL(ObDasVecScanUtils::get_main_rowkey(data_filter_ctdef_, vec_aux_rtdef_->eval_ctx_, main_rowkey))) {
            LOG_WARN("fail to get main rowkey", K(ret));
          } else if (OB_ISNULL(entry = rowkey_dist_map.get(main_rowkey))) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("rowkey not exist", K(ret), K(main_rowkey));
          } else if (OB_FAIL(matched_rowkeys.push_back(*entry))) {
            LOG_WARN("failed to push back.", K(ret), KPC(entry));
          } else {
            ++matched_count;
            LOG_DEBUG("match filter rowkey", K(i), K(main_rowkey), K(matched_count));
          }
        }
      } // IVF_GET_NEXT_ROWS_BEGIN loop; cleanup is shared with the scalar/error path below.
    } else {
      data_filter_iter_->clear_evaluated_flag();
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        ObRowkey &main_rowkey = tmp_main_rowkey_;
        ObIvfRowkeyDistEntry* entry = nullptr;
        if (OB_FAIL(data_filter_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else if (OB_FAIL(ObDasVecScanUtils::get_main_rowkey(data_filter_ctdef_, vec_aux_rtdef_->eval_ctx_, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (OB_ISNULL(entry = rowkey_dist_map.get(main_rowkey))) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("rowkey not exist", K(ret), K(main_rowkey));
        } else if (OB_FAIL(matched_rowkeys.push_back(*entry))) {
          LOG_WARN("failed to push back.", K(ret), KPC(entry));
        } else {
          ++matched_count;
          LOG_DEBUG("match filter rowkey", K(i), K(main_rowkey), K(matched_count));
        }
      }
    }
    if (scan_opened || (is_get && scan_attempted)) {
      const int scan_ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      const int reuse_ret = ObDasVecScanUtils::reuse_iter(
          ls_id_, data_filter_iter_, data_filter_scan_param_, data_filter_tablet_id_);
      if (OB_SUCCESS != reuse_ret) {
        LOG_WARN("failed to leave IVF filter batch", K(reuse_ret), K(scan_ret));
      } else if (is_get) {
        // All storage readers have left this batch. Free any allocations that
        // the common code abandoned, without touching cross-round IVF state.
        disk_bbq_lookup_alloc_.reuse();
      }
      // On cleanup failure keep the batch owner alive until inner_release()
      // drains the iterator; never free memory that may still be borrowed.
      ret = is_get && OB_SUCCESS != scan_ret ? scan_ret
          : (OB_SUCCESS != reuse_ret ? reuse_ret : scan_ret);
    } else if (is_get && OB_FAIL(ret)) {
      data_filter_scan_param_.key_ranges_.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    adaptive_ctx_.iter_filter_row_cnt_ += count;
    adaptive_ctx_.iter_res_row_cnt_ += matched_count;
  }
  // Keep completed work even when filtering failed.
  ObIVFProfileUtil::inc(filter_profile, ObMetricId::VEC_ITER_FILTER_MATCHED_ROW_COUNT, matched_count);
  return ret;
}

void ObDASIvfBaseScanIter::reuse_cid_ctx()
{
  iterative_filter_ctx_.reuse();
}

ObDASIVFAsyncIter::ObDASIVFAsyncIter()
  : ObDASIvfBaseScanIter(),
    async_ivf_index_tablet_id_(),
    async_delete_tablet_id_(),
    async_ivf_scan_param_(),
    async_delete_scan_param_(),
    segment_group_ctx_(),
    sql_snapshot_(nullptr),
    async_read_snapshot_(nullptr),
    deletion_bitmap_handle_(),
    filter_rowkey_objs_(),
    partition_scan_finished_(false),
    is_adaptive_specify_by_hint_(false),
    profile_est_rows_(0),
    profile_est_total_rows_(0),
    partition_profile_(nullptr)
{
  filter_rowkey_objs_.set_attr(ObMemAttr(MTL_ID(), "AsyncIVFRkBuf"));
}

void ObDASIVFAsyncIter::bind_partition_profile()
{
  const int64_t selectivity_scale = 100; // integer percent
  partition_profile_ = get_current_vector_iter_profile();
  profile_ = ObIVFProfileUtil::register_child(
      partition_profile_, ObProfileId::VECTOR_SEARCH_VEC_ASYNC_IVF_SCAN);
  if (is_adaptive_filter() && !is_adaptive_specify_by_hint_) {
    ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_EST_ROWS, profile_est_rows_);
    ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_EST_SELECTIVITY,
        profile_est_total_rows_ > 0
            ? static_cast<int64_t>(std::min(1.0, static_cast<double>(profile_est_rows_)
                / profile_est_total_rows_) * selectivity_scale)
            : 0);
  }
}

void ObDASIVFAsyncIter::record_profile_finish(const int scan_ret)
{
  if (OB_SUCCESS != scan_ret && OB_ITER_END != scan_ret) {
    const int64_t tag = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == scan_ret
        ? PROFILE_VECTOR_RETRY : PROFILE_QUERY_ERROR;
    ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_FINISH_TAG, tag);
  } else if (OB_ITER_END == scan_ret || partition_scan_finished_) {
    ObIVFProfileUtil::set_once(partition_profile_, ObMetricId::VEC_IVF_FINISH_TAG, PROFILE_RETURN_RESULT);
  }
}

void ObDASIVFAsyncIter::clear_partition_profile()
{
  // Closing early must not invent a finish tag or retain another partition's estimates.
  partition_profile_ = nullptr;
  profile_ = nullptr;
  profile_est_rows_ = 0;
  profile_est_total_rows_ = 0;
}

int ObDASIVFAsyncIter::inner_get_next_row()
{
  int ret = ObDASIvfBaseScanIter::inner_get_next_row();
  record_profile_finish(ret);
  return ret;
}

int ObDASIVFAsyncIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = ObDASIvfBaseScanIter::inner_get_next_rows(count, capacity);
  record_profile_finish(ret);
  return ret;
}

int ObDASIVFAsyncIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfBaseScanIter::inner_init(param);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init async ivf base iter", K(ret));
  } else if (OB_ISNULL(async_read_snapshot_ =
                 OB_NEWx(transaction::ObTxReadSnapshot, &persist_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate async ivf read snapshot", K(ret));
  } else {
    ObDASIVFAsyncIterParam &async_param = static_cast<ObDASIVFAsyncIterParam &>(param);
    async_ivf_index_tablet_id_ = async_param.async_ivf_index_tablet_id_;
    async_delete_tablet_id_ = async_param.async_delete_tablet_id_;
    is_adaptive_specify_by_hint_ = async_param.is_adaptive_specify_by_hint_;
    sql_snapshot_ = snapshot_;
    partition_scan_finished_ = false;
    if (OB_NOT_NULL(data_filter_iter_)) {
      data_filter_iter_->set_scan_param(data_filter_scan_param_);
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  bind_partition_profile();
  ScopedTimer timer(ObMetricId::VEC_TOTAL_SEARCH_TIME, partition_profile_);
  // The partition path was selected after binding the filter input at tree creation.
  if (OB_ISNULL(vec_aux_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async ivf scan definition", K(ret));
  } else if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf filter input binding", K(ret), KP_(inv_idx_scan_iter));
  } else if (OB_FAIL(prepare_async_ivf_read())) {
    LOG_WARN("failed to prepare async ivf read state", K(ret));
  } else if (OB_FAIL(start_filter_input(false))) {
    LOG_WARN("failed to start async ivf filter input", K(ret));
  }
  if (OB_SUCCESS != ret) {
    record_profile_finish(ret);
  }
  return ret;
}

int ObDASIVFAsyncIter::rescan()
{
  int ret = OB_SUCCESS;
  clear_partition_profile();
  bind_partition_profile();
  ScopedTimer timer(ObMetricId::VEC_TOTAL_SEARCH_TIME, partition_profile_);
  // DAS already reused the tree before filling the new input ranges. The SQL
  // parameter setter invalidated dependent expressions; materialize this binding
  // again before opening its Segment scans.
  if (OB_ISNULL(vec_aux_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async ivf scan definition", K(ret));
  } else if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf filter input binding", K(ret), KP_(inv_idx_scan_iter));
  } else if (OB_FAIL(refresh_partition_filter_path())) {
    LOG_WARN("failed to select async ivf partition filter path on rescan", K(ret));
  } else if (OB_FAIL(refresh_query_vector())) {
    LOG_WARN("failed to refresh async ivf query vector on rescan", K(ret));
  } else if (OB_FAIL(prepare_async_ivf_read())) {
    LOG_WARN("failed to prepare async ivf rescan state", K(ret));
  } else if (OB_FAIL(start_filter_input(true))) {
    LOG_WARN("failed to rescan async ivf filter input", K(ret));
  }
  if (OB_SUCCESS != ret) {
    record_profile_finish(ret);
  }
  return ret;
}

int ObDASIVFAsyncIter::get_filter_input_scan(ObDASScanIter *&scan_iter) const
{
  int ret = OB_SUCCESS;
  ObDASIter *input = inv_idx_scan_iter_;
  scan_iter = nullptr;
  if (OB_NOT_NULL(input) && DAS_ITER_LOCAL_LOOKUP == input->get_type()) {
    input = static_cast<ObDASLocalLookupIter *>(input)->get_index_table_iter();
  }
  if (OB_ISNULL(input) || DAS_ITER_SCAN != input->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf filter scan", K(ret), KP(input));
  } else {
    scan_iter = static_cast<ObDASScanIter *>(input);
  }
  return ret;
}

int ObDASIVFAsyncIter::start_filter_input(const bool is_rescan)
{
  int ret = OB_SUCCESS;
  ScopedTimer timer(ObMetricId::VEC_PRE_FILTER_TIME, is_pre_filter() ? partition_profile_ : nullptr);
  // Filtered paths share the input lifecycle; only PRE consumes its rows.
  if (ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER == vec_index_type_) {
    // The input is the async IVF index itself; unfiltered queries use Segment KNN scans.
  } else if (OB_FAIL(prepare_filter_input_scan_params())) {
    LOG_WARN("failed to prepare async ivf filter input", K(ret));
  } else if (is_rescan) {
    if (OB_FAIL(inv_idx_scan_iter_->rescan())) {
      LOG_WARN("failed to rescan async ivf filter input", K(ret));
    }
  } else if (OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
    LOG_WARN("failed to open async ivf filter input", K(ret));
  }
  return ret;
}

int ObDASIVFAsyncIter::estimate_partition_rows(
    const ObDASScanCtDef &ctdef,
    const ObTabletID &tablet_id,
    const ObQueryFlag &scan_flag,
    const ObIArray<ObNewRange> &ranges,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  const ObAccessService *access_service = MTL(ObAccessService *);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFRowEstimate"));
  ObTableScanParam est_param;
  ObTableScanRange scan_range;
  ObSimpleBatch batch;
  ObNewRange whole_range;
  SQLScanRangeArray scan_ranges;
  ObSEArray<ObEstRowCountRecord, 1> records;
  int64_t physical_rows = -1;
  row_count = -1;
  if (!tablet_id.is_valid() || !ls_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf estimate tablet or ls", K(ret), K(tablet_id), K_(ls_id));
  } else if (OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async ivf estimate access service", K(ret));
  } else if (ranges.empty() && ctdef.enable_new_false_range_) {
    // Preserve the scan's false-range convention.
    row_count = 0;
  } else {
    est_param.index_id_ = ctdef.ref_table_id_;
    est_param.tablet_id_ = tablet_id;
    est_param.ls_id_ = ls_id_;
    est_param.schema_version_ = ctdef.schema_version_;
    est_param.scan_flag_ = scan_flag;
    est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
    if (ranges.empty()) {
      whole_range.table_id_ = ctdef.ref_table_id_;
      whole_range.set_whole_range();
      batch.type_ = ObSimpleBatch::T_SCAN;
      batch.range_ = &whole_range;
    } else if (1 == ranges.count()) {
      batch.type_ = ObSimpleBatch::T_SCAN;
      batch.range_ = &ranges.at(0);
    } else if (OB_FAIL(scan_ranges.assign(ranges))) {
      LOG_WARN("failed to copy async ivf estimate ranges", K(ret));
    } else {
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = &scan_ranges;
    }
    if (FAILEDx(scan_range.init(est_param, batch, allocator))) {
      LOG_WARN("failed to initialize async ivf estimate range", K(ret));
    } else if (OB_FAIL(access_service->estimate_row_count(est_param, scan_range,
                   THIS_WORKER.get_timeout_remain(), records, row_count, physical_rows))) {
      LOG_WARN("failed to estimate async ivf partition rows", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::reset_partition_filter_path()
{
  int ret = OB_SUCCESS;
  profile_est_rows_ = 0;
  profile_est_total_rows_ = 0;
  if (OB_ISNULL(vec_aux_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async ivf scan definition", K(ret));
  } else if (is_adaptive_filter()) {
    // Restore plan defaults even on failure; never retain another partition's estimates.
    vec_idx_try_path_ = vec_aux_ctdef_->adaptive_try_path_;
    profile_current_path_ = vec_idx_try_path_;
    selectivity_ = vec_aux_ctdef_->selectivity_;
    adaptive_ctx_.selectivity_ = vec_aux_ctdef_->selectivity_;
    adaptive_ctx_.row_count_ = vec_aux_ctdef_->row_count_;
  }
  return ret;
}

int ObDASIVFAsyncIter::apply_partition_filter_estimate(
    const int estimate_ret, const int64_t partition_rows, const int64_t filter_rows)
{
  int ret = OB_SUCCESS;
  const double pre_filter_threshold = 0.10;
  if (OB_FAIL(reset_partition_filter_path())) {
    LOG_WARN("failed to restore async ivf plan defaults", K(ret));
  } else if (is_adaptive_filter()) {
    if (OB_SUCCESS == estimate_ret && partition_rows >= 0 && filter_rows >= 0) {
      profile_est_rows_ = filter_rows;
      profile_est_total_rows_ = partition_rows;
      ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_EST_ROWS, profile_est_rows_);
    }
    if (OB_INVALID_ARGUMENT == estimate_ret || OB_ERR_UNEXPECTED == estimate_ret
        || OB_NOT_INIT == estimate_ret
        || OB_TIMEOUT == estimate_ret || OB_ALLOCATE_MEMORY_FAILED == estimate_ret
        || OB_TENANT_OUT_OF_MEM == estimate_ret || OB_CANCELED == estimate_ret
        || OB_ERR_QUERY_INTERRUPTED == estimate_ret || OB_ERR_SESSION_INTERRUPTED == estimate_ret
        || OB_SESSION_KILLED == estimate_ret || OB_ERR_INTERRUPTED == estimate_ret) {
      ret = estimate_ret;
    } else if (OB_SUCCESS == estimate_ret && partition_rows > 0 && filter_rows >= 0) {
      const int64_t selectivity_scale = 100; // integer percent
      selectivity_ = std::min(1.0, static_cast<double>(filter_rows) / partition_rows);
      ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_EST_SELECTIVITY,
          static_cast<int64_t>(std::min(1.0, static_cast<double>(profile_est_rows_)
              / profile_est_total_rows_) * selectivity_scale));
      adaptive_ctx_.selectivity_ = selectivity_;
      adaptive_ctx_.row_count_ = partition_rows;
      vec_idx_try_path_ = filter_rows <= ObVecIdxExtraInfo::MAX_IVF_BRUTE_FORCE_SIZE
          || selectivity_ <= pre_filter_threshold
          ? ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER
          : ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
      profile_current_path_ = vec_idx_try_path_;
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::refresh_partition_filter_path()
{
  int ret = OB_SUCCESS;
  int estimate_ret = OB_SUCCESS;
  int64_t partition_rows = -1;
  int64_t filter_rows = -1;
  const int64_t start_time = ObTimeUtility::current_time();
  const ObVecIdxAdaTryPath old_path = vec_idx_try_path_;
  ObDASScanIter *input_scan = nullptr;
  ObTabletID input_tablet_id;
  ObNewRange whole_range;
  ObSEArray<ObNewRange, 1> whole_ranges;
  const ObDASBaseCtDef *input_def = nullptr;
  const char *fallback_reason = "none";
  if (OB_ISNULL(vec_aux_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null async ivf scan definition", K(ret));
  } else if (!is_adaptive_filter() || is_adaptive_specify_by_hint_) {
    // Hinted adaptive paths, fixed paths and unfiltered scans keep the plan choice.
  } else if (OB_FAIL(reset_partition_filter_path())) {
    LOG_WARN("failed to restore async ivf plan defaults", K(ret));
  } else if (OB_ISNULL(data_filter_ctdef_) || OB_ISNULL(data_filter_rtdef_)
             || OB_ISNULL(input_def = vec_aux_ctdef_->get_inv_idx_scan_ctdef())
             || DAS_OP_TABLE_SCAN != input_def->op_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf estimate definitions", K(ret), KP(input_def));
  } else if (OB_FAIL(get_filter_input_scan(input_scan))) {
    LOG_WARN("failed to get async ivf estimate input", K(ret));
  } else {
    const ObDASScanCtDef &input_ctdef = *static_cast<const ObDASScanCtDef *>(input_def);
    const ObTableScanParam &input_param = input_scan->get_scan_param();
    input_tablet_id = input_param.tablet_id_;
    whole_range.table_id_ = data_filter_ctdef_->ref_table_id_;
    whole_range.set_whole_range();
    if (!input_tablet_id.is_valid() || !data_filter_tablet_id_.is_valid() || !ls_id_.is_valid()) {
      estimate_ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async ivf partition estimate ids", K(estimate_ret),
               K(input_tablet_id), K_(data_filter_tablet_id), K_(ls_id));
    } else if (OB_SUCCESS != (estimate_ret = whole_ranges.push_back(whole_range))) {
    } else if (OB_SUCCESS != (estimate_ret = estimate_partition_rows(*data_filter_ctdef_,
                   data_filter_tablet_id_, data_filter_rtdef_->scan_flag_, whole_ranges, partition_rows))) {
    } else if (input_ctdef.ref_table_id_ == data_filter_ctdef_->ref_table_id_
               && input_param.key_ranges_.count() == 1 && input_param.key_ranges_.at(0).is_whole_range()) {
      filter_rows = partition_rows;
    } else {
      estimate_ret = estimate_partition_rows(input_ctdef, input_tablet_id,
          input_param.scan_flag_, input_param.key_ranges_, filter_rows);
    }
    if (OB_SUCCESS != estimate_ret) {
      fallback_reason = "estimate_failed";
    } else if (partition_rows <= 0 || filter_rows < 0) {
      fallback_reason = "invalid_estimate";
    }
    if (OB_FAIL(apply_partition_filter_estimate(estimate_ret, partition_rows, filter_rows))) {
      LOG_WARN("async ivf partition estimate failed", K(ret));
    } else if (OB_NOT_NULL(exec_ctx_) && OB_FAIL(exec_ctx_->check_status())) {
      LOG_WARN("async ivf query interrupted during partition estimation", K(ret));
    }
    LOG_INFO("async ivf partition filter path", K(ret), K_(ls_id), K_(data_filter_tablet_id),
             K(input_tablet_id), K(partition_rows), K(filter_rows), K_(selectivity),
             K(old_path), K_(vec_idx_try_path), K(estimate_ret), K(fallback_reason),
             "estimate_time_us", ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObDASIVFAsyncIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  clear_partition_profile();
  ObDASIter *input_iter = inv_idx_scan_iter_;
  reset_async_read_state();
  // Reuse both filter branches regardless of the chosen path. M is selected
  // again before rescan; prepare_filter_input_scan_params() handles lazy lookups.
  if (OB_NOT_NULL(input_iter) && DAS_ITER_LOCAL_LOOKUP == input_iter->get_type()) {
    ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(input_iter);
    lookup_iter->get_lookup_param().need_switch_param_ = true;
    input_iter = lookup_iter->get_index_table_iter();
  }
  if (OB_NOT_NULL(inv_idx_scan_iter_)) {
    if (OB_ISNULL(input_iter) || DAS_ITER_SCAN != input_iter->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async ivf pre-filter input", K(ret), KP(input_iter));
    } else {
      static_cast<ObDASScanIter *>(input_iter)->get_scan_param().need_switch_param_ = true;
    }
  }
  data_filter_scan_param_.need_switch_param_ = true;
  if (OB_SUCC(ret) && OB_FAIL(ObDASIvfBaseScanIter::inner_reuse())) {
    LOG_WARN("failed to reuse ivf base scan iter", K(ret));
  }
  if (OB_SUCC(ret)) {
    filter_rowkey_objs_.reset();
  }
  return ret;
}

int ObDASIVFAsyncIter::inner_release()
{
  deletion_bitmap_handle_.trace_cache_handle("legacy_async_ivf_release_before");
  clear_partition_profile();
  // The base releases both filter branches, including scans never opened.
  reset_async_read_state();
  deletion_bitmap_handle_.trace_cache_handle("legacy_async_ivf_release_after");
  if (OB_NOT_NULL(async_read_snapshot_)) {
    async_read_snapshot_->~ObTxReadSnapshot();
    async_read_snapshot_ = nullptr;
  }
  int ret = ObDASIvfBaseScanIter::inner_release();
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to release ivf base scan iter", K(ret));
  }
  filter_rowkey_objs_.reset();
  ls_id_.reset();
  async_ivf_index_tablet_id_.reset();
  async_delete_tablet_id_.reset();
  sql_snapshot_ = nullptr;
  is_adaptive_specify_by_hint_ = false;
  return ret;
}

void ObDASIVFAsyncIter::reset_async_read_state()
{
  // Each search releases its Segment iterators before the read state is reset.
  adaptive_ctx_.pre_scan_row_cnt_ = 0;
  deletion_bitmap_handle_.reset();
  ObDasVecScanUtils::release_scan_param(async_ivf_scan_param_);
  ObDasVecScanUtils::release_scan_param(async_delete_scan_param_);
  segment_group_ctx_.reset();
  snapshot_ = sql_snapshot_;
  if (OB_NOT_NULL(async_read_snapshot_)) {
    async_read_snapshot_->reset();
  }
  partition_scan_finished_ = false;
}

int ObDASIVFAsyncIter::prepare_async_read_snapshot(const share::SCN &read_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_snapshot_) || OB_ISNULL(async_read_snapshot_) || OB_UNLIKELY(!read_snapshot.is_valid()
      || read_snapshot <= share::SCN::min_scn() || read_snapshot > sql_snapshot_->version())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf read snapshot", K(ret), K(read_snapshot), KP_(sql_snapshot));
  } else if (OB_FAIL(async_read_snapshot_->assign(*sql_snapshot_))) {
    LOG_WARN("failed to copy sql read snapshot", K(ret));
  } else {
    async_read_snapshot_->specify_snapshot_scn(read_snapshot);
    ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_SCN, read_snapshot.get_val_for_tx());
    // The base iterator retains this scan param across partition reuse/rescan.
    if (!data_filter_iter_first_scan_
        && OB_FAIL(data_filter_scan_param_.snapshot_.assign(*async_read_snapshot_))) {
      LOG_WARN("failed to refresh async ivf data-filter snapshot", K(ret));
    } else {
      snapshot_ = async_read_snapshot_;
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::prepare_async_scan_params()
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *scan_ctdef = vec_aux_ctdef_->get_async_ivf_index_scan_ctdef();
  ObDASScanRtDef *scan_rtdef = vec_aux_rtdef_->get_async_ivf_index_scan_rtdef();
  const ObDASScanCtDef *delete_ctdef = vec_aux_ctdef_->get_async_delete_scan_ctdef();
  ObDASScanRtDef *delete_rtdef = vec_aux_rtdef_->get_async_delete_scan_rtdef();
  if (OB_UNLIKELY(!async_delete_tablet_id_.is_valid())
      || OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_rtdef)
      || OB_ISNULL(delete_ctdef) || OB_ISNULL(delete_rtdef)
      || OB_ISNULL(sql_snapshot_) || OB_ISNULL(async_read_snapshot_)
      || OB_ISNULL(scan_rtdef->p_pd_expr_op_) || OB_ISNULL(delete_rtdef->p_pd_expr_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf scan definitions", K(ret),
             K_(async_delete_tablet_id), KP(scan_ctdef), KP(scan_rtdef), KP(delete_ctdef),
             KP(delete_rtdef), KP_(sql_snapshot), KP_(async_read_snapshot));
  } else if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(
                 ls_id_, async_ivf_index_tablet_id_, scan_ctdef, scan_rtdef,
                 tx_desc_, sql_snapshot_, async_ivf_scan_param_, false, &vec_op_alloc_))) {
    LOG_WARN("failed to init async ivf scan param", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::init_scan_param(
                 ls_id_, async_delete_tablet_id_, delete_ctdef, delete_rtdef,
                 tx_desc_, sql_snapshot_, async_delete_scan_param_, false, &vec_op_alloc_))) {
    LOG_WARN("failed to init async ivf deletion scan param", K(ret));
  } else if (OB_FALSE_IT(async_delete_scan_param_.need_scn_ =
                 delete_ctdef->check_need_fill_scn())) {
  } else if (OB_FAIL(async_delete_scan_param_.key_ranges_.assign(delete_rtdef->key_ranges_))) {
    LOG_WARN("failed to assign async ivf deletion scan ranges", K(ret));
  } else if (OB_FAIL(GDS.execute(BEFORE_ASYNC_IVF_DELETION_BITMAP,
                 MTL_ID(), ls_id_.id(), async_delete_tablet_id_.id(),
                 delete_ctdef->ref_table_id_))) {
    // The segment handles and logical snapshot are fixed; no bitmap is held yet.
    LOG_WARN("failed to wait before async ivf deletion bitmap", K(ret),
             K_(async_delete_tablet_id), KPC(async_read_snapshot_));
  } else if (0 != ERRSIM_ENABLE_ASYNC_IVF_FLASHBACK_RESET) {
    // Segment selection has fixed the logical read version for this scan.
    async_ivf_scan_param_.fb_snapshot_.reset();
    async_ivf_scan_param_.fb_read_tx_uncommitted_ = false;
    // Do not let flashback override the physical deletion snapshot prepared below.
    async_delete_scan_param_.fb_snapshot_.reset();
    async_delete_scan_param_.fb_read_tx_uncommitted_ = false;
  } else if (async_ivf_scan_param_.fb_snapshot_.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("flashback query with async IVF is not supported", K(ret),
             K_(async_ivf_index_tablet_id), "fb_snapshot", async_ivf_scan_param_.fb_snapshot_);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "flashback query with async IVF");
  }
  if (OB_SUCC(ret)) {
    const int64_t sql_snapshot = async_delete_scan_param_.snapshot_.is_valid()
        && async_delete_scan_param_.snapshot_.core_.version_.is_valid()
        ? async_delete_scan_param_.snapshot_.core_.version_.get_val_for_tx() : 0;
    if (OB_UNLIKELY(sql_snapshot <= 0 || INT64_MAX == sql_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async ivf SQL deletion snapshot", K(ret), K(sql_snapshot));
    } else if (OB_FAIL(ObDeletionRowIter::prepare_read_snapshot(
                   ls_id_, async_delete_tablet_id_, async_delete_scan_param_.timeout_,
                   async_delete_scan_param_.snapshot_, sql_snapshot))) {
      LOG_WARN("failed to prepare async ivf deletion physical snapshot", K(ret),
               K_(async_delete_tablet_id), K(sql_snapshot));
    } else if (OB_FAIL(ObDeletionBitmapProvider::get_or_build_bitmap(
                   async_delete_tablet_id_, async_read_snapshot_->version().get_val_for_tx(),
                   vec_op_alloc_, async_delete_scan_param_, *delete_ctdef, *delete_rtdef,
                   deletion_bitmap_handle_))) {
      LOG_WARN("failed to prepare async ivf deletion bitmap", K(ret), K_(async_delete_tablet_id));
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  bool need_pre_filter = false;
  ObIvfKnnSearchResult retained_post;
  if (ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER != vec_index_type_
      && !is_iter_filter()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async ivf query path is not implemented", K(ret), K_(vec_index_type),
             K_(vec_idx_try_path));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "async IVF query path");
  } else if (partition_scan_finished_) {
    // An empty scan does not create saved_rowkeys_itr_; keep it as a terminal result.
  } else if (OB_ISNULL(vec_aux_ctdef_)
             || OB_ISNULL(vec_aux_rtdef_)
             || OB_ISNULL(snapshot_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf partition state", K(ret), KP_(vec_aux_ctdef),
             KP_(vec_aux_rtdef), KP_(snapshot));
  } else if (OB_UNLIKELY(1 != vec_aux_ctdef_->result_output_.count())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async ivf only supports one hidden primary rowkey", K(ret),
             "output_count", vec_aux_ctdef_->result_output_.count());
  } else if (OB_FAIL(is_iter_filter()
                 ? scan_async_disk_bbq_post_filter(is_vectorized, need_pre_filter, retained_post)
                 : scan_async_ivf_segments_iteratively(is_vectorized))) {
    LOG_WARN("failed to execute iterative async ivf segment scan", K(ret), K_(ls_id),
             K_(async_ivf_index_tablet_id), K_(vec_index_type));
  }
  if (OB_SUCC(ret) && need_pre_filter) {
    // POST's Segment borrowers have been released. Keep the query, snapshot,
    // filter input and allocator alive while consuming the budgeted PRE input.
    if (OB_FAIL(run_async_pre_filter(mem_context_->get_arena_allocator(),
                                    is_vectorized, &retained_post))) {
      LOG_WARN("failed to run async IVF POST fallback pre-filter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    partition_scan_finished_ = true;
  }
  return ret;
}

int ObDASIVFAsyncIter::add_async_ivf_filter_candidates(
    const ObIvfKnnSearchResult &segment_result,
    ObIVFRowkeyDistMap &rowkey_dist_map)
{
  int ret = OB_SUCCESS;
  // Allocate before publishing any rowkey pointers. Capacity is reused across
  // refill rounds, so memory follows the largest round, not the sum of rounds.
  filter_rowkey_objs_.reuse();
  if (OB_FAIL(filter_rowkey_objs_.prepare_allocate(segment_result.count()))) {
    LOG_WARN("failed to prepare async ivf filter rowkeys", K(ret),
             "candidate_count", segment_result.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < segment_result.count(); ++i) {
    const ObIvfKnnEntry &entry = segment_result.at(i);
    if (OB_UNLIKELY(entry.vid_ < 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid async ivf candidate vid", K(ret), K(i), K(entry));
    } else {
      ObObj *rowkey_obj = &filter_rowkey_objs_.at(i);
      rowkey_obj->set_uint64(static_cast<uint64_t>(entry.vid_));
      ObRowkey rowkey(rowkey_obj, 1);
      ObIvfRowkeyDistEntry rowkey_entry(rowkey, entry.distance_);
      if (OB_FAIL(rowkey_dist_map.set_refactored(rowkey, rowkey_entry))) {
        LOG_WARN("failed to add async ivf filter candidate", K(ret), K(i), K(entry));
      }
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::get_async_ivf_segment_group_ctx()
{
  int ret = OB_SUCCESS;
  // Hold the query before the target partition resolves R. The test can run
  // the delete refresh while this point is blocked, then let the scan pick up
  // the post-refresh published versions.
  DEBUG_SYNC(BEFORE_ASYNC_IVF_SEGMENT_GROUP_CTX);
  ObAccessService *access_service = MTL(ObAccessService *);
  ObSEArray<ObTabletID, 1> index_tablet_ids;
  // Scan/rescan selects the groups at Q before binding the effective snapshot M.
  if (OB_ISNULL(access_service)
      || OB_ISNULL(snapshot_)
      || OB_UNLIKELY(!ls_id_.is_valid()
                     || !async_ivf_index_tablet_id_.is_valid()
                     || dim_ <= 0
                     || real_search_vec_.length() != dim_ * sizeof(float)
                     || nprobes_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf scan preparation state", K(ret), KP(access_service),
             KP_(snapshot), K_(ls_id), K_(async_ivf_index_tablet_id), K_(dim),
             "query_size", real_search_vec_.length(), K_(nprobes));
  } else if (OB_FAIL(index_tablet_ids.push_back(async_ivf_index_tablet_id_))) {
    LOG_WARN("failed to append async ivf tablet id", K(ret),
             K_(async_ivf_index_tablet_id));
  } else if (OB_FAIL(access_service->get_async_index_segment_group_ctx(
                 ls_id_, index_tablet_ids, async_delete_tablet_id_, snapshot_->version(), segment_group_ctx_))) {
    LOG_WARN("failed to get async ivf segment groups", K(ret), K_(ls_id),
             K_(async_ivf_index_tablet_id));
  }
  return ret;
}

int ObDASIVFAsyncIter::prepare_async_ivf_read()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_async_ivf_segment_group_ctx())) {
    LOG_WARN("failed to get async ivf segment groups", K(ret));
  } else {
    if (nullptr != profile_) {
      int64_t segment_count = 0;
      for (int64_t i = 0; i < segment_group_ctx_.get_group_count(); ++i) {
        const ObAsyncIndexSegmentGroup *group = segment_group_ctx_.get_group(i);
        if (nullptr != group && group->get_tablet_cnt() > 0) {
          segment_count += group->get_tablet_version_ranges(0).count();
        }
      }
      ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_SEGMENT_COUNT, segment_count);
    }
    if (segment_group_ctx_.get_group_count() > 0) {
      if (OB_FAIL(prepare_async_read_snapshot(segment_group_ctx_.get_async_index_read_snapshot()))) {
        LOG_WARN("failed to prepare async ivf read snapshot", K(ret));
      } else if (OB_FAIL(prepare_async_scan_params())) {
        LOG_WARN("failed to prepare async ivf scan params", K(ret));
      }
    } else {
      // An empty partition remains terminal until the next reuse.
      partition_scan_finished_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    DEBUG_SYNC(AFTER_ASYNC_IVF_QUERY_READ_PREPARED);
  }
  return ret;
}

int ObDASIVFAsyncIter::prepare_filter_input_scan_params()
{
  int ret = OB_SUCCESS;
  ObDASScanIter *scan_iter = nullptr;
  if (OB_ISNULL(vec_aux_ctdef_) || OB_ISNULL(snapshot_)
      || OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf filter input tree", K(ret), KP_(inv_idx_scan_iter), KP_(snapshot));
  } else if (OB_FAIL(get_filter_input_scan(scan_iter))) {
    LOG_WARN("failed to get async ivf filter input scan", K(ret));
  } else if (OB_FAIL(scan_iter->get_scan_param().snapshot_.assign(*snapshot_))) {
    LOG_WARN("failed to assign async ivf filter input snapshot", K(ret));
  } else {
    // Both covering scans and lookup inputs must use M, including after reuse.
    scan_iter->get_scan_param().need_switch_param_ = OB_NOT_NULL(scan_iter->get_output_result_iter());
    if (DAS_ITER_LOCAL_LOOKUP == inv_idx_scan_iter_->get_type()) {
      ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(inv_idx_scan_iter_);
      ObDASIter *data_iter = lookup_iter->get_data_table_iter();
      if (OB_ISNULL(data_iter) || DAS_ITER_SCAN != data_iter->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async ivf filter data scan", K(ret), KP(data_iter));
      } else if (OB_FAIL(lookup_iter->get_lookup_param().snapshot_.assign(*snapshot_))) {
        LOG_WARN("failed to assign async ivf filter input lookup snapshot", K(ret));
      } else {
        // Lookup remains lazy: its first scan must initialize after partition reuse.
        lookup_iter->get_lookup_param().need_switch_param_ =
            OB_NOT_NULL(static_cast<ObDASScanIter *>(data_iter)->get_output_result_iter());
      }
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::prepare_async_ivf_scan(
    const int64_t search_k,
    const blocksstable::ObIvfPreFilter *pre_filter,
    ObIArray<ObIvfSSTableScanIter *> &scan_iters,
    ObIvfScanBudget *scan_budget)
{
  int ret = OB_SUCCESS;
  const int64_t async_ivf_tablet_idx = 0;
  ObIvfKnnSearchParam knn_param;
  ObIvfSSTableScanIter *scan_iter = nullptr;
  if (OB_UNLIKELY(search_k <= 0 || scan_iters.count() != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf segment scan state", K(ret), K(search_k), K(scan_iters.count()));
  } else if (segment_group_ctx_.get_group_count() > 0) {
    knn_param.query_ = reinterpret_cast<const float *>(real_search_vec_.ptr());
    knn_param.dim_ = dim_;
    knn_param.nprobe_ = nprobes_;
    // Unfiltered POST also refills when deleted or duplicate VIDs leave fewer than LIMIT rows.
    knn_param.iterative_centroids_ = nullptr == pre_filter && is_post_filter();
    knn_param.k_ = search_k;
    knn_param.cluster_size_ = vec_index_param_.cluster_size_;
    knn_param.refine_k_ = search_param_.refine_k_;
    knn_param.deletion_bitmap_handle_ = &deletion_bitmap_handle_;
    knn_param.pre_filter_ = pre_filter;
    knn_param.selectivity_ = selectivity_;
    knn_param.scan_budget_ = scan_budget;
    knn_param.is_set_refine_k_ = vec_aux_ctdef_->vec_query_param_.is_set_refine_k_;
    const int64_t limit_k = limit_param_.limit_ + limit_param_.offset_; // limit+offset
    if (vec_aux_ctdef_->vec_query_param_.is_set_ivf_nprobes_ && 0 == vec_aux_ctdef_->vec_query_param_.ivf_nprobes_) {
      const double candidates = std::ceil(static_cast<double>(limit_k) * search_param_.refine_k_);
      if (!std::isfinite(candidates) || candidates < limit_k || candidates >= static_cast<double>(INT64_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DiskBBQ automatic candidate count", K(ret), K(limit_k), K(search_param_));
      } else {
        knn_param.original_k_ = limit_k;
        knn_param.num_candidates_ = static_cast<int64_t>(candidates);
      }
    }
    for (int64_t group_idx = 0;
         OB_SUCC(ret) && group_idx < segment_group_ctx_.get_group_count();
         ++group_idx) {
      const ObAsyncIndexSegmentGroup *group = segment_group_ctx_.get_group(group_idx);
      if (OB_ISNULL(group)
          || OB_UNLIKELY(group->get_tablet_cnt() <= async_ivf_tablet_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async ivf segment group", K(ret), K(group_idx), KP(group));
      } else {
        const ObIArray<ObNewVersionRange> &version_ranges =
            group->get_tablet_version_ranges(async_ivf_tablet_idx);
        for (int64_t range_idx = 0;
             OB_SUCC(ret) && range_idx < version_ranges.count();
             ++range_idx) {
          if (OB_FAIL(create_async_ivf_scan_iter(
                  knn_param, version_ranges.at(range_idx), scan_iter))) {
            LOG_WARN("failed to create async ivf segment scan iter", K(ret),
                     K(group_idx), K(range_idx), K(version_ranges.at(range_idx)));
          } else if (OB_FAIL(scan_iters.push_back(scan_iter))) {
            LOG_WARN("failed to append async ivf segment scan iter", K(ret),
                     K(group_idx), K(range_idx), KP(scan_iter));
          } else {
            scan_iter = nullptr;
          }
        }
      }
    }
    if (OB_NOT_NULL(scan_iter)) {
      scan_iter->~ObIvfSSTableScanIter();
      async_ivf_scan_param_.allocator_->free(scan_iter);
      scan_iter = nullptr;
    }
    if (OB_FAIL(ret)) {
      release_async_ivf_segment_scan_iters(scan_iters);
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::create_async_ivf_scan_iter(
    const ObIvfKnnSearchParam &knn_param,
    const ObNewVersionRange &version_range,
    ObIvfSSTableScanIter *&scan_iter)
{
  int ret = OB_SUCCESS;
  ObOpProfile<ObMetric> *segment_profile =
      ObIVFProfileUtil::register_child(profile_, ObProfileId::VECTOR_SEARCH_IVF_SEGMENT);
  ScopedTimer timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME, segment_profile);
  if (version_range.is_valid()) {
    ObIVFProfileUtil::set(segment_profile, ObMetricId::VEC_IVF_VERSION_RANGE_BASE,
                           version_range.base_version_);
    ObIVFProfileUtil::set(segment_profile, ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT,
                           version_range.snapshot_version_);
  }
  const int64_t async_ivf_tablet_idx = 0;
  ObAccessService *access_service = MTL(ObAccessService *);
  // Keep the SSTable lookup on the tablet that supplied the segment version ranges.
  scan_iter = nullptr;
  if (OB_ISNULL(access_service)
      || OB_ISNULL(sql_snapshot_)
      || OB_ISNULL(async_ivf_scan_param_.allocator_)
      || OB_UNLIKELY(!ls_id_.is_valid()
                     || !async_ivf_index_tablet_id_.is_valid()
                     || !knn_param.is_valid()
                     || !version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf scan iter state", K(ret), KP(access_service),
             K_(ls_id), K_(async_ivf_index_tablet_id), K(knn_param), K(version_range));
  } else if (OB_FAIL(access_service->async_index_ivf_scan(
                 ls_id_, segment_group_ctx_.get_index_tablet_handle(async_ivf_tablet_idx),
                 async_ivf_scan_param_, scan_iter))) {
    LOG_WARN("failed to create async ivf scan iter", K(ret), K_(ls_id),
             K_(async_ivf_index_tablet_id));
  } else if (OB_ISNULL(scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async ivf scan iter is null", K(ret), K_(ls_id),
             K_(async_ivf_index_tablet_id));
  } else if (OB_FAIL(scan_iter->set_query(knn_param))) {
    LOG_WARN("failed to set async ivf query", K(ret), K(knn_param));
  } else if (OB_FAIL(scan_iter->switch_sstable(
                 version_range, sql_snapshot_->version()))) {
    LOG_WARN("failed to bind async ivf segment", K(ret), K(version_range));
  } else {
    scan_iter->set_profile(segment_profile, is_iter_filter());
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scan_iter)) {
    scan_iter->~ObIvfSSTableScanIter();
    async_ivf_scan_param_.allocator_->free(scan_iter);
    scan_iter = nullptr;
  }
  return ret;
}

int ObDASIVFAsyncIter::scan_async_ivf_segments(
    bool first_round,
    int64_t next_nprobe,
    const ObIArray<ObIvfSSTableScanIter *> &scan_iters,
    ObIvfKnnSearchResult &round_result,
    bool &has_active_segment,
    const ObIvfScanBudget *scan_budget)
{
  int ret = OB_SUCCESS;
  ObIvfKnnSearchResult segment_result;
  has_active_segment = false;
  round_result.reset();
  if (OB_UNLIKELY(next_nprobe < 0 || scan_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf segment scan round", K(ret), K(next_nprobe),
             "scan_iter_count", scan_iters.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_iters.count()
       && (nullptr == scan_budget || !scan_budget->should_stop()); ++i) {
    ObIvfSSTableScanIter *current_iter = scan_iters.at(i);
    if (OB_ISNULL(current_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iterative async ivf scan iter is null", K(ret), K(i));
    } else if (first_round || current_iter->has_remaining_centroids()) {
      segment_result.reset();
      if (OB_FAIL(current_iter->knn_search(next_nprobe, segment_result))) {
        LOG_WARN("failed to search iterative async ivf segment", K(ret), K(i),
                 K(next_nprobe));
      }
      ScopedTimer delivery_timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME,
                                 current_iter->get_profile());
      for (int64_t j = 0; OB_SUCC(ret) && j < segment_result.count(); ++j) {
        if (OB_FAIL(round_result.add(segment_result.at(j)))) {
          LOG_WARN("failed to append async ivf round result", K(ret), K(i), K(j),
                   K(segment_result.at(j)));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(current_iter) && current_iter->has_remaining_centroids()) {
      has_active_segment = true;
    }
  }
  return ret;
}

void ObDASIVFAsyncIter::release_async_ivf_segment_scan_iters(
    ObIArray<ObIvfSSTableScanIter *> &scan_iters)
{
  for (int64_t i = 0; i < scan_iters.count(); ++i) {
    ObIvfSSTableScanIter *scan_iter = scan_iters.at(i);
    if (OB_NOT_NULL(scan_iter)) {
      scan_iter->~ObIvfSSTableScanIter();
      async_ivf_scan_param_.allocator_->free(scan_iter);
      scan_iters.at(i) = nullptr;
    }
  }
  scan_iters.reset();
}

int ObDASIVFAsyncIter::collect_async_ivf_candidates(
    const ObIvfKnnSearchResult &candidates,
    ObIvfTopKCollector &collector,
    int64_t &collected_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collected_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collected async ivf candidate count", K(ret), K(collected_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObIvfKnnEntry &entry = candidates.at(i);
    if (OB_FAIL(collector.push(entry.cid_, entry.vid_, entry.distance_))) {
      LOG_WARN("failed to collect async ivf candidate", K(ret), K(i), K(entry));
    }
  }
  if (OB_SUCC(ret)) {
    collected_count = collector.size();
  }
  return ret;
}

int ObDASIVFAsyncIter::collect_filtered_async_ivf_candidates(
    const ObIArray<ObIvfRowkeyDistEntry> &candidates,
    ObIvfTopKCollector &collector,
    int64_t &collected_count)
{
  int ret = OB_SUCCESS;
  // Post-filter results retain the VID rowkey but not the source CID. CID does
  // not participate in partition Top-K ordering or the final DAS output.
  const int64_t unused_cid = 0;
  if (OB_UNLIKELY(collected_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collected filtered async ivf candidate count", K(ret),
             K(collected_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObIvfRowkeyDistEntry &entry = candidates.at(i);
    const ObObj *vid_obj = nullptr;
    uint64_t vid = 0;
    if (OB_UNLIKELY(1 != entry.rowkey_.get_obj_cnt()
                    || OB_ISNULL(vid_obj = entry.rowkey_.get_obj_ptr())
                    || !vid_obj->is_uint64())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid filtered async ivf rowkey", K(ret), K(i), K(entry));
    } else if (OB_FALSE_IT(vid = vid_obj->get_uint64())) {
    } else if (OB_UNLIKELY(vid > static_cast<uint64_t>(INT64_MAX))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("filtered async ivf vid is out of range", K(ret), K(i), K(vid));
    } else if (OB_FAIL(collector.push(
                   unused_cid, static_cast<int64_t>(vid), entry.distance_))) {
      LOG_WARN("failed to collect filtered async ivf candidate", K(ret), K(i), K(entry));
    }
  }
  if (OB_SUCC(ret)) {
    collected_count = collector.size();
  }
  return ret;
}

int ObDASIVFAsyncIter::append_async_ivf_topk(ObIvfTopKCollector &collector)
{
  int ret = OB_SUCCESS;
  ObIvfKnnSearchResult topk_result;
  if (OB_FAIL(collector.finish(topk_result))) {
    LOG_WARN("failed to finish async ivf partition top-k", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < topk_result.count(); ++i) {
    void *buf = vec_op_alloc_.alloc(sizeof(ObObj));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async ivf rowkey", K(ret), K(i));
    } else {
      ObObj *rowkey_obj = new (buf) ObObj();
      rowkey_obj->set_uint64(static_cast<uint64_t>(topk_result.at(i).vid_));
      ObRowkey rowkey(rowkey_obj, 1);
      if (OB_FAIL(saved_rowkeys_.push_back(rowkey))) {
        LOG_WARN("failed to append async ivf rowkey", K(ret), K(i), K(topk_result.at(i)));
      }
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::scan_async_disk_bbq_post_filter(
    bool is_vectorized, bool &need_pre_filter, ObIvfKnnSearchResult &retained_post)
{
  int ret = OB_SUCCESS;
  need_pre_filter = false;
  retained_post.reset();
  PostFilterProgress progress;
  const int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
  // Aim to retain K * refine_k candidates after filtering. Match the existing
  // post-filter 10x expansion cap when the estimated pass rate is very small.
  const double filter_selectivity = std::isfinite(selectivity_)
      && selectivity_ >= 0.0 && selectivity_ <= 1.0
      ? std::max(0.1, selectivity_) : 1.0;
  const double candidate_limit = std::ceil(
      static_cast<double>(limit_k) * search_param_.refine_k_ / filter_selectivity);
  int64_t search_k = 0;
  ObSEArray<ObIvfSSTableScanIter *, 8> scan_iters;
  ObIvfKnnSearchResult candidates;
  ObIvfKnnSearchResult selected;
  ObIvfKnnSearchResult refined;
  ObIVFRowkeyDistMap rowkey_dist_map;
  ObSEArray<ObIvfRowkeyDistEntry, 16> matched;
  ObIvfTopKCollector collector;
  ObIvfScanBudget scan_budget(max_scan_vectors_);
  int64_t collected_count = 0;
  int64_t next_nprobe = nprobes_;
  bool first_round = true;
  bool has_active_segment = false;
  adaptive_ctx_.iter_times_ = 0;
  adaptive_ctx_.iter_filter_row_cnt_ = 0;
  adaptive_ctx_.iter_res_row_cnt_ = 0;
  if (OB_UNLIKELY(limit_k <= 0 || !std::isfinite(candidate_limit)
      || candidate_limit < limit_k || candidate_limit >= static_cast<double>(INT64_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ quantized candidate limit", K(ret), K(limit_k), K(candidate_limit));
  } else if (OB_ISNULL(data_filter_iter_) || OB_ISNULL(data_filter_ctdef_)
      || OB_ISNULL(data_filter_rtdef_) || !data_filter_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ filter state", K(ret));
  } else if (OB_FALSE_IT(search_k = static_cast<int64_t>(candidate_limit))) {
  } else if (OB_FAIL(prepare_async_ivf_scan(search_k, nullptr, scan_iters, &scan_budget))) {
    LOG_WARN("failed to prepare DiskBBQ quantized scan", K(ret));
  } else if (OB_FAIL(collector.init(limit_k, vec_op_alloc_))) {
    LOG_WARN("failed to initialize filtered DiskBBQ Top-K", K(ret));
  } else if (OB_FAIL(rowkey_dist_map.create(32, lib::ObMemAttr(MTL_ID(), "AsyncIVFFilter")))) {
    LOG_WARN("failed to initialize DiskBBQ filter lookup", K(ret));
  }
  has_active_segment = scan_iters.count() > 0;
  while (OB_SUCC(ret) && collected_count < limit_k
      && has_active_segment && !scan_budget.is_exhausted()) {
    ++adaptive_ctx_.iter_times_;
    scan_budget.allow_batch_stop_ = !first_round && strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST;
    has_active_segment = false;
    // Keep each segment pinned through approximate selection, SQL filtering
    // and RAW reads. Each segment/round retains at most search_k VIDs.
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_iters.count()
         && !scan_budget.should_stop(); ++i) {
      ObIvfSSTableScanIter *iter = scan_iters.at(i);
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (first_round || iter->has_remaining_centroids()) {
        rowkey_dist_map.reuse();
        matched.reuse();
        selected.reset();
        refined.reset();
        if (OB_FAIL(iter->knn_search(next_nprobe, candidates, true /* quantized_only */))) {
          LOG_WARN("failed to select DiskBBQ quantized candidates", K(ret), K(i), K(next_nprobe));
        } else if (OB_FAIL(add_async_ivf_filter_candidates(candidates, rowkey_dist_map))) {
          LOG_WARN("failed to prepare DiskBBQ candidates for filtering", K(ret));
        } else if (rowkey_dist_map.size() > 0 && OB_FAIL(do_table_post_filter(
            is_vectorized, rowkey_dist_map, matched, partition_profile_))) {
          LOG_WARN("failed to filter DiskBBQ quantized candidates", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < matched.count(); ++j) {
          const ObRowkey &rowkey = matched.at(j).rowkey_;
          const ObObj *vid = rowkey.get_obj_ptr();
          if (OB_UNLIKELY(rowkey.get_obj_cnt() != 1 || OB_ISNULL(vid)
              || !vid->is_uint64() || vid->get_uint64() > static_cast<uint64_t>(INT64_MAX))) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid filtered DiskBBQ VID", K(ret), K(rowkey));
          } else {
            ObIvfKnnEntry entry;
            entry.vid_ = static_cast<int64_t>(vid->get_uint64());
            if (OB_FAIL(selected.add(entry))) {
              LOG_WARN("failed to retain filtered DiskBBQ VID", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && selected.count() > 0
            && OB_FAIL(iter->refine_candidates(selected, refined))) {
          LOG_WARN("failed to refine filtered DiskBBQ candidates", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < refined.count(); ++j) {
          const ObIvfKnnEntry &entry = refined.at(j);
          const bool retained = collector.contains(entry.vid_);
          if (OB_FAIL(collector.push(entry.cid_, entry.vid_, entry.distance_))) {
            LOG_WARN("failed to merge refined DiskBBQ candidate", K(ret), K(entry));
          } else if (!retained && collected_count < limit_k) {
            ++collected_count;
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(iter) && iter->has_remaining_centroids()) {
        has_active_segment = true;
      }
    }
    // A completed round covers one fixed/automatic window on every active
    // Segment. Use the accumulated distinct Top-K count, not a batch's count.
    // Do not turn an interrupted/partially processed window into a zero proof.
    if (OB_SUCC(ret) && !scan_budget.should_stop()) {
      const bool stalled = progress.finish_window(collected_count);
      if (collected_count == 0 || stalled) {
        need_pre_filter = true;
        break;
      }
    }
    first_round = false;
    if (OB_SUCC(ret) && collected_count < limit_k
        && has_active_segment && !scan_budget.is_exhausted()) {
      next_nprobe = calc_next_nprobe(limit_k, collected_count);
    }
  }
  if (OB_SUCC(ret)) {
    if (need_pre_filter && OB_FAIL(collector.finish(retained_post))) {
      LOG_WARN("failed to retain DiskBBQ POST candidates on fallback", K(ret));
    } else if (!need_pre_filter && OB_FAIL(append_async_ivf_topk(collector))) {
      LOG_WARN("failed to append filtered DiskBBQ Top-K", K(ret));
    }
  }
  LOG_TRACE("DiskBBQ quantized post-filter finished", K(ret), K(search_k), K(collected_count),
      K(limit_k), K(need_pre_filter), K_(selectivity), K(filter_selectivity), K(scan_budget), K_(adaptive_ctx));
  release_async_ivf_segment_scan_iters(scan_iters);
  return ret;
}

int ObDASIVFAsyncIter::scan_async_ivf_segments_iteratively(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const bool need_post_filter = is_iter_filter();
  const int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
  const int64_t search_k = need_post_filter
      ? get_heap_size(limit_k, selectivity_) : limit_k;
  ObSEArray<ObIvfSSTableScanIter *, 8> scan_iters;
  ObIvfKnnSearchResult round_result;
  ObIVFRowkeyDistMap rowkey_dist_map;
  ObSEArray<ObIvfRowkeyDistEntry, 16> filtered_candidates;
  ObIvfTopKCollector collector;
  int64_t collected_count = 0;
  ObIvfScanBudget scan_budget(need_post_filter ? max_scan_vectors_ : 0);
  int64_t next_nprobe = nprobes_;
  bool first_round = true;
  bool has_active_segment = false;
  bool budget_exhausted = false;
  adaptive_ctx_.iter_times_ = 0;
  adaptive_ctx_.iter_filter_row_cnt_ = 0;
  adaptive_ctx_.iter_res_row_cnt_ = 0;
  if (OB_UNLIKELY(limit_k <= 0 || search_k <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iterative async ivf scan limit", K(ret), K(limit_k), K(search_k));
  } else if (need_post_filter
             && (OB_ISNULL(data_filter_iter_)
                 || OB_ISNULL(data_filter_ctdef_)
                 || OB_ISNULL(data_filter_rtdef_)
                 || OB_UNLIKELY(!data_filter_tablet_id_.is_valid()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf post-filter state", K(ret), KP_(data_filter_iter),
             KP_(data_filter_ctdef), KP_(data_filter_rtdef), K_(data_filter_tablet_id));
  } else if (OB_FAIL(prepare_async_ivf_scan(
                 search_k, nullptr, scan_iters, &scan_budget))) {
    LOG_WARN("failed to prepare async ivf scan", K(ret));
  } else if (OB_FAIL(collector.init(limit_k, vec_op_alloc_))) {
    LOG_WARN("failed to init async ivf partition top-k", K(ret), K(limit_k));
  } else if (need_post_filter
             && OB_FAIL(rowkey_dist_map.create(
                    32, lib::ObMemAttr(MTL_ID(), "AsyncIVFFilter")))) {
    LOG_WARN("failed to create async ivf rowkey distance map", K(ret));
  }

  has_active_segment = scan_iters.count() > 0;
  while (OB_SUCC(ret) && collected_count < limit_k && has_active_segment && !budget_exhausted) {
    ++adaptive_ctx_.iter_times_;
    scan_budget.allow_batch_stop_ = !first_round && strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST;
    if (OB_FAIL(scan_async_ivf_segments(
            first_round, next_nprobe, scan_iters, round_result, has_active_segment, &scan_budget))) {
      LOG_WARN("failed to scan iterative async ivf round", K(ret), K_(adaptive_ctx));
    } else if (need_post_filter) {
      rowkey_dist_map.reuse();
      filtered_candidates.reuse();
      if (OB_FAIL(add_async_ivf_filter_candidates(round_result, rowkey_dist_map))) {
        LOG_WARN("failed to build async ivf post-filter candidates", K(ret),
                 K(round_result.count()));
      } else if (rowkey_dist_map.size() > 0
                 && OB_FAIL(do_table_post_filter(
                        is_vectorized, rowkey_dist_map, filtered_candidates, partition_profile_))) {
        LOG_WARN("failed to filter iterative async ivf candidates", K(ret),
                 "candidate_count", rowkey_dist_map.size());
      } else if (OB_FAIL(collect_filtered_async_ivf_candidates(
                     filtered_candidates, collector, collected_count))) {
        LOG_WARN("failed to collect post-filtered async ivf candidates", K(ret),
                 K(filtered_candidates.count()));
      }
    } else if (OB_FAIL(collect_async_ivf_candidates(
                   round_result, collector, collected_count))) {
      LOG_WARN("failed to collect async ivf round candidates", K(ret),
               K(round_result.count()));
    }
    first_round = false;
    budget_exhausted = scan_budget.is_exhausted();
    if (OB_SUCC(ret) && collected_count < limit_k && has_active_segment && !budget_exhausted) {
      const int64_t left_search = limit_k - collected_count;
      const double select_ratio = adaptive_ctx_.iter_filter_row_cnt_ > 0
          ? static_cast<double>(adaptive_ctx_.iter_res_row_cnt_)
              / static_cast<double>(adaptive_ctx_.iter_filter_row_cnt_)
          : 0.0;
      next_nprobe = calc_next_nprobe(limit_k, collected_count);
      LOG_INFO("async ivf scan continues", K_(vec_index_type), K(limit_k),
               K(collected_count), K(left_search), K(next_nprobe), K(select_ratio),
               K_(adaptive_ctx));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append_async_ivf_topk(collector))) {
    LOG_WARN("failed to append async ivf top-k result", K(ret), K(collected_count));
  }
  LOG_INFO("async ivf iterative search finished", K(ret), K(collected_count), K(limit_k),
           K(scan_budget), K(budget_exhausted), K(has_active_segment), K_(strategy), K_(adaptive_ctx));
  release_async_ivf_segment_scan_iters(scan_iters);
  return ret;
}

int64_t ObDASIVFAsyncIter::get_pre_filter_estimated_rows() const
{
  int64_t estimated_rows = -1;
  if (nullptr != vec_aux_ctdef_) {
    // Keep the full-predicate plan selectivity: adaptive_ctx_ may have been
    // overwritten with a range-only estimate. The plan count is a weak hint
    // when no partition estimate exists, including fixed PRE paths.
    const double rows = profile_est_total_rows_ > 0
        ? static_cast<double>(profile_est_total_rows_) : vec_aux_ctdef_->row_count_;
    const double selectivity = vec_aux_ctdef_->selectivity_;
    double estimate = -1;
    if (std::isfinite(rows) && rows > 0 && std::isfinite(selectivity)
        && selectivity >= 0 && selectivity <= 1) {
      estimate = rows * selectivity;
    }
    const ObDASBaseCtDef *input_def = vec_aux_ctdef_->get_inv_idx_scan_ctdef();
    if (profile_est_total_rows_ > 0 && profile_est_rows_ >= 0
        && profile_est_rows_ <= profile_est_total_rows_
        && nullptr != inv_idx_scan_iter_ && DAS_ITER_SCAN == inv_idx_scan_iter_->get_type()
        && nullptr != input_def && DAS_OP_TABLE_SCAN == input_def->op_type_) {
      const ObDASScanCtDef &input_ctdef = *static_cast<const ObDASScanCtDef *>(input_def);
      // Range rows represent output only when the covering scan has no
      // residual predicates. Otherwise use the full-predicate plan hint.
      if (input_ctdef.pd_expr_spec_.pushdown_filters_.empty()
          && input_ctdef.logical_residual_filters_.empty()) {
        estimate = static_cast<double>(profile_est_rows_);
      }
    }
    if (std::isfinite(estimate) && estimate >= 0 && estimate < static_cast<double>(INT64_MAX)) {
      estimated_rows = static_cast<int64_t>(std::ceil(estimate));
    }
  }
  return estimated_rows;
}

int ObDASIVFAsyncIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (partition_scan_finished_) {
  } else if (!is_pre_filter() || OB_ISNULL(inv_idx_scan_iter_)
             || OB_ISNULL(vec_aux_ctdef_) || OB_ISNULL(vec_aux_rtdef_)
             || OB_UNLIKELY(vec_aux_ctdef_->result_output_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf pre-filter state", K(ret), KP_(inv_idx_scan_iter),
             KP_(vec_aux_ctdef), KP_(vec_aux_rtdef));
  } else if (segment_group_ctx_.get_group_count() > 0) {
    if (OB_FAIL(run_async_pre_filter(allocator, is_vectorized))) {
      LOG_WARN("failed to execute async IVF pre-filter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    partition_scan_finished_ = true;
  }
  // All Segment borrowers have finished before the query-owned bitmap is released.
  return ret;
}

namespace
{
// Expose the inherited check only inside IVF. The member pointer still calls
// ObDASScanIter on the original input; no derived scan object or downcast is used.
class ObIVFPreFilterBudgetChecker : public ObDASScanIter
{
public:
  static int check(ObDASScanIter &input_scan)
  {
    const auto check_budget = &ObIVFPreFilterBudgetChecker::check_vec_pre_filter_status;
    return (input_scan.*check_budget)();
  }
};
}

int ObDASIVFAsyncIter::run_async_pre_filter(
    ObIAllocator &allocator, bool is_vectorized, const ObIvfKnnSearchResult *retained_post)
{
  int ret = OB_SUCCESS;
  if (nullptr != retained_post && strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST) {
    ObDASScanIter *input_scan = nullptr;
    // Early success on a PRE budget stop must not hide a query failure.
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("query stopped before async IVF POST fallback", K(ret));
      return ret;
    } else if (OB_FAIL(get_filter_input_scan(input_scan))) {
      LOG_WARN("failed to get async IVF POST fallback input", K(ret));
      return ret;
    } else if (input_scan->is_vec_pre_filtering_timeout_set()) {
      const int budget_ret = ObIVFPreFilterBudgetChecker::check(*input_scan);
      if (OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == budget_ret) {
        // Keep the original clock. The PRE budget is spent; return the POST
        // candidates without reading input or constructing another bitmap.
        if (retained_post->count() > 0) {
          ObSEArray<ObIvfKnnEntry, 1> no_pre_candidates;
          if (OB_FAIL(append_pre_filter_topk(no_pre_candidates, retained_post))) {
            LOG_WARN("failed to preserve POST candidates after async IVF budget stop", K(ret));
          }
        }
        return ret;
      } else if (OB_SUCCESS != budget_ret) {
        ret = budget_ret;
        LOG_WARN("failed to check async IVF POST fallback budget", K(ret));
        return ret;
      }
    }
  }
  blocksstable::ObIvfPreFilterBitmap pre_filter_bitmap(allocator, get_pre_filter_estimated_rows());
  // Keep the original PRE budget and return policy, including after POST fallback.
  if (OB_FAIL(materialize_pre_filter(is_vectorized, pre_filter_bitmap))) {
    LOG_WARN("failed to materialize async IVF pre-filter", K(ret));
  } else if (pre_filter_bitmap.get_cardinality() > 0) {
    if (OB_FAIL(scan_pre_filtered_segments(pre_filter_bitmap, retained_post))) {
      LOG_WARN("failed to search pre-filtered async IVF segments", K(ret));
    }
  } else if (nullptr != retained_post && retained_post->count() > 0) {
    // An empty budgeted prefix does not invalidate candidates already filtered by POST.
    ObSEArray<ObIvfKnnEntry, 1> no_pre_candidates;
    if (OB_FAIL(append_pre_filter_topk(no_pre_candidates, retained_post))) {
      LOG_WARN("failed to preserve POST candidates after empty async IVF pre-filter", K(ret));
    }
  }
  return ret;
}

int ObDASIVFAsyncIter::init_filter_input(ObDASIter *filter_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_iter) || OB_NOT_NULL(inv_idx_scan_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async ivf filter input binding", K(ret), KP(filter_iter));
  } else {
    inv_idx_scan_iter_ = filter_iter;
  }
  return ret;
}

int ObDASIVFAsyncIter::get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  rowkey = nullptr;
  if (OB_ISNULL(vec_aux_ctdef_) || OB_ISNULL(vec_aux_rtdef_)
      || OB_ISNULL(ctdef = vec_aux_ctdef_->get_async_ivf_index_scan_ctdef())
      || OB_ISNULL(rtdef = vec_aux_rtdef_->get_async_ivf_index_scan_rtdef())
      || OB_ISNULL(rtdef->eval_ctx_)
      || OB_UNLIKELY(ctdef->rowkey_exprs_.count() != 1)
      || OB_ISNULL(ctdef->rowkey_exprs_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf rowkey scan definitions", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
    LOG_WARN("failed to get async ivf rowkey", K(ret));
  }
  return ret;
}

int ObDASIVFAsyncIter::materialize_pre_filter(
    bool is_vectorized, blocksstable::ObIvfPreFilterBitmap &pre_filter_bitmap)
{
  int ret = OB_SUCCESS;
  ScopedTimer timer(ObMetricId::VEC_PRE_FILTER_TIME, partition_profile_);
  bool scan_end = false;
  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  ObExpr *vid_expr = nullptr;
  ObSEArray<uint64_t, 256> vids;
  if (OB_ISNULL(inv_idx_scan_iter_) || OB_ISNULL(vec_aux_ctdef_)
      || OB_ISNULL(vec_aux_rtdef_) || OB_ISNULL(vec_aux_rtdef_->eval_ctx_)
      || OB_UNLIKELY(inv_idx_scan_iter_->get_max_size() <= 0
                     || vec_aux_ctdef_->result_output_.count() != 1)
      || OB_ISNULL(vec_aux_ctdef_->result_output_.at(0))
      || OB_ISNULL(ctdef = vec_aux_ctdef_->get_async_ivf_index_scan_ctdef())
      || OB_ISNULL(rtdef = vec_aux_rtdef_->get_async_ivf_index_scan_rtdef())
      || OB_ISNULL(rtdef->eval_ctx_)
      || OB_UNLIKELY(ctdef->rowkey_exprs_.count() != 1)
      || OB_ISNULL(vid_expr = ctdef->rowkey_exprs_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf pre-filter input", K(ret), KP_(inv_idx_scan_iter),
             KP_(vec_aux_ctdef), KP_(vec_aux_rtdef));
  } else if (OB_UNLIKELY(ObIntType != vid_expr->datum_meta_.type_
                        && ObUInt64Type != vid_expr->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf pre-filter vid type", K(ret), K(vid_expr->datum_meta_));
  } else if (OB_FAIL(vids.reserve(inv_idx_scan_iter_->get_max_size()))) {
    LOG_WARN("failed to reserve async ivf pre-filter vids", K(ret));
  }
  while (OB_SUCC(ret) && !scan_end) {
    vids.reuse();
    do {
      int64_t count = 0;
      inv_idx_scan_iter_->clear_evaluated_flag();
      if (is_vectorized) {
        ret = inv_idx_scan_iter_->get_next_rows(count, inv_idx_scan_iter_->get_max_size());
      } else if (OB_SUCC(ret = inv_idx_scan_iter_->get_next_row())) {
        count = 1;
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        scan_end = true; // A final vectorized batch may still contain rows.
      } else if (OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == ret
                 && strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST) {
        ret = OB_SUCCESS;
        scan_end = true;
        count = 0;
        LOG_INFO("async ivf pre-filter budget finished", K_(strategy),
                 "collected_vids", pre_filter_bitmap.get_cardinality(),
                 "pending_valid_vids", vids.count());
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to read async ivf pre-filter vids", K(ret));
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(count < 0 || count > inv_idx_scan_iter_->get_max_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async ivf pre-filter batch size", K(ret), K(count));
      }
      // Scalar predicates and MVCC are handled by the input tree. Copy only the
      // VID while its datum is valid, without materializing generic rowkeys.
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const ObDatum &datum = is_vectorized ? vid_expr->locate_expr_datum(*rtdef->eval_ctx_, i)
                                            : vid_expr->locate_expr_datum(*rtdef->eval_ctx_);
        if (OB_UNLIKELY(datum.is_null() || nullptr == datum.ptr_
                        || datum.len_ != sizeof(uint64_t) || datum.get_uint() > INT64_MAX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async ivf pre-filter vid", K(ret), K(i));
        } else if (OB_FAIL(vids.push_back(datum.get_uint()))) {
          LOG_WARN("failed to buffer async ivf pre-filter vid", K(ret));
        }
      }
    } while (OB_SUCC(ret) && !scan_end && !is_vectorized
             && vids.count() < inv_idx_scan_iter_->get_max_size());
    if (OB_SUCC(ret) && vids.count() > 0
        && OB_FAIL(pre_filter_bitmap.value_add_many(vids.get_data(), vids.count()))) {
      LOG_WARN("failed to append async ivf pre-filter bitmap batch", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(pre_filter_bitmap.finish())) {
    LOG_WARN("failed to finish async ivf pre-filter bitmap", K(ret));
  }
  ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_PRE_FILTER_ROWS_SCANNED,
                         pre_filter_bitmap.get_cardinality());
  if (OB_SUCC(ret)) {
    adaptive_ctx_.pre_scan_row_cnt_ = pre_filter_bitmap.get_cardinality();
  } else {
    pre_filter_bitmap.reset();
    adaptive_ctx_.pre_scan_row_cnt_ = 0;
  }
  return ret;
}

int ObDASIVFAsyncIter::scan_pre_filtered_segments(
    blocksstable::ObIvfPreFilterBitmap &pre_filter_bitmap, const ObIvfKnnSearchResult *retained_post)
{
  int ret = OB_SUCCESS;
  const int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
  ObSEArray<ObIvfSSTableScanIter *, 8> scan_iters;
  ObIvfKnnSearchResult segment_result;
  ObSEArray<ObIvfKnnEntry, 16> candidates;
  blocksstable::ObIvfPreFilter pre_filter;
  const uint64_t pre_filter_row_count = pre_filter_bitmap.get_cardinality();
  adaptive_ctx_.is_brute_force_ =
      pre_filter_row_count <= ObVecIdxExtraInfo::MAX_IVF_BRUTE_FORCE_SIZE;
  if (OB_FAIL(pre_filter.init(pre_filter_bitmap))) {
    LOG_WARN("failed to bind async ivf pre-filter bitmap", K(ret));
  } else if (OB_FAIL(prepare_async_ivf_scan(limit_k, &pre_filter, scan_iters))) {
    LOG_WARN("failed to prepare pre-filtered async ivf segments", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_iters.count(); ++i) {
    if (OB_ISNULL(scan_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pre-filtered async ivf segment iterator", K(ret), K(i));
    } else if (adaptive_ctx_.is_brute_force_) {
      ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_IS_BRUTE_FORCE, true);
      ret = scan_iters.at(i)->brute_force_search(segment_result);
    } else {
      ObIVFProfileUtil::set(partition_profile_, ObMetricId::VEC_IVF_IS_BRUTE_FORCE, false);
      ret = scan_iters.at(i)->knn_search(nprobes_, segment_result);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to search pre-filtered async ivf segment", K(ret), K(i));
    }
    ScopedTimer delivery_timer(ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME,
        nullptr != scan_iters.at(i) ? scan_iters.at(i)->get_profile() : nullptr);
    for (int64_t j = 0; OB_SUCC(ret) && j < segment_result.count(); ++j) {
      if (OB_FAIL(candidates.push_back(segment_result.at(j)))) {
        LOG_WARN("failed to collect pre-filtered async ivf candidates", K(ret), K(i), K(j));
      }
    }
  }
  // POST candidates already passed the SQL filter at this snapshot. A budgeted
  // PRE prefix may not contain them; merge them without re-filtering by bitmap.
  if (OB_SUCC(ret) && OB_FAIL(append_pre_filter_topk(candidates, retained_post))) {
    LOG_WARN("failed to append pre-filtered async ivf top-k", K(ret));
  }
  LOG_INFO("async ivf pre-filter search finished", K(ret), K(pre_filter_row_count),
           K_(adaptive_ctx), K_(nprobes), "segment_count", scan_iters.count());
  release_async_ivf_segment_scan_iters(scan_iters);
  return ret;
}

int ObDASIVFAsyncIter::append_pre_filter_topk(
    const ObIArray<ObIvfKnnEntry> &candidates, const ObIvfKnnSearchResult *retained_post)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  if (OB_FAIL(collector.init(limit_param_.limit_ + limit_param_.offset_, vec_op_alloc_))) {
    LOG_WARN("failed to initialize pre-filtered async ivf top-k", K(ret));
  }
  // Retained entries own only CID/VID/distance values, so no POST Segment
  // pointer crosses the switch. One collector deduplicates both paths.
  for (int64_t i = 0; OB_SUCC(ret) && nullptr != retained_post && i < retained_post->count(); ++i) {
    const ObIvfKnnEntry &entry = retained_post->at(i);
    if (OB_FAIL(collector.push(entry.cid_, entry.vid_, entry.distance_))) {
      LOG_WARN("failed to merge a retained POST candidate", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObIvfKnnEntry &entry = candidates.at(i);
    if (OB_FAIL(collector.push(entry.cid_, entry.vid_, entry.distance_))) {
      LOG_WARN("failed to collect pre-filtered async ivf candidate", K(ret), K(i), K(entry));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append_async_ivf_topk(collector))) {
    LOG_WARN("failed to append pre-filtered async ivf top-k", K(ret));
  }
  return ret;
}

/*************************** implement ObDASIvfScanIter ****************************/
int ObDASIvfScanIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfBaseScanIter::inner_init(param);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init", K(ret));
  } else if (is_post_filter()) {
    if (OB_FAIL(near_cid_.reserve(nprobes_))) {
      LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
    }
  } else if (OB_FAIL(near_cid_dist_.allocate_array(persist_alloc_, vec_index_param_.nlist_ + 1))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else {
    memset(near_cid_dist_.get_data(), 0, near_cid_dist_.count() * sizeof(bool));
  }
  return ret;
};

int ObDASIvfScanIter::inner_reuse()
{
  near_cid_.reuse();
  memset(near_cid_dist_.get_data(), 0, near_cid_dist_.count() * sizeof(bool));
  return ObDASIvfBaseScanIter::inner_reuse();
}

int ObDASIvfScanIter::inner_release()
{
  near_cid_.reset();
  near_cid_dist_.reset();
  return ObDASIvfBaseScanIter::inner_release();
}

// HGraph-based iterative filtering implementations
int ObDASIvfScanIter::get_next_probe_centers_ids_by_hgraph(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, false /*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret));
  } else if (is_cache_usable && OB_NOT_NULL(cent_cache) && cent_cache->has_hgraph_index()) {
    if (OB_FAIL(get_nearest_probe_center_ids_with_hgraph(
        is_vectorized, cent_cache, false, !vec_aux_ctdef_->is_post_filter(), next_nprobe))) {
      LOG_WARN("HGraph iterative search failed", K(ret), K(next_nprobe));
    }
  } else {
    // If HGraph was used before but now unavailable, this is an error
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("HGraph was used in initial scan but now unavailable for iterative filtering", K(ret),
        K(is_cache_usable), K(OB_NOT_NULL(cent_cache)), K(cent_cache ? cent_cache->has_hgraph_index() : false));
  }

  return ret;
}

int ObDASIvfScanIter::get_next_center_ids(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  if (has_used_hgraph_) {
    if (OB_FAIL(get_next_probe_centers_ids_by_hgraph(is_vectorized, next_nprobe))) {
      LOG_WARN("failed to get next probe centers ids by hgraph", K(ret), K(is_vectorized), K(next_nprobe));
    }
  } else {
    if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(next_nprobe, near_cid_))) {
      LOG_WARN("failed to get next nearest probe center ids", K(ret), K(next_nprobe));
    }
  }
  return ret;
}

int ObDASIvfScanIter::get_nearest_probe_center_ids_with_hgraph(bool is_vectorized, ObIvfICache *hgraph_cache,
                                                               bool need_vectors, bool need_distances, int64_t incremental_nprobes)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(hgraph_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hgraph cache is null", K(ret));
  } else {
    common::obvsag::VectorIndexPtr hgraph_index = hgraph_cache->get_hgraph_index();
    if (OB_ISNULL(hgraph_index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hgraph index is null", K(ret));
    } else {
      const float* distances = nullptr;
      const int64_t* result_ids = nullptr;
      int64_t result_size = 0;
      float* search_vec = reinterpret_cast<float*>(const_cast<char*>(real_search_vec_.ptr()));
      const char* extra_info = nullptr;
      int64_t centers_total = hgraph_cache->get_count();
      int64_t ef_search = 64;
      // Check if need to initialize HGraph state (incremental_nprobes == -1 means initial search)
      bool need_init = (incremental_nprobes == -1);
      int64_t search_count = need_init ? nprobes_ : incremental_nprobes;
      // Create VSAG allocator if not exists (same lifecycle as hgraph_iter_ctx_)
      if (OB_FAIL(init_hgraph_search_alloc())) {
        LOG_WARN("failed to init hgraph search alloc", K(ret));
      } else if (OB_FAIL(obvectorutil::knn_search(hgraph_index,
                                           search_vec,
                                           dim_,
                                           search_count,  // Request incremental count
                                           distances,
                                           result_ids,
                                           extra_info,
                                           result_size,
                                           ef_search,
                                           nullptr,
                                           false,
                                           false,
                                           1.0f,
                                           hgraph_vsag_alloc_,
                                           false,
                                           hgraph_iter_ctx_,        // Always pass iter context, let HGraph manage it
                                           false))) {               // is_last_search, keep searching
        LOG_WARN("failed to search with hgraph index", K(ret), K(search_count), K(ef_search));
      }
      
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(result_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hgraph search result is null", K(ret));
      } else if (result_size == 0) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("hgraph search result is empty", K(ret));
      } else {
        near_cid_.reset();
        int64_t new_center_cnt = 0;
        for (int64_t i = 0; i < result_size && OB_SUCC(ret); ++i) {
          int64_t center_idx = result_ids[i];
          if (center_idx >= 1 && center_idx <= centers_total) {
            ObCenterId center_id;
            center_id.center_id_ = center_idx;
            center_id.tablet_id_ = centroid_tablet_id_.id();

            if (OB_FAIL(near_cid_.push_back(center_id))) {
              LOG_WARN("failed to push center id", K(ret), K(center_id));
            } else {
              new_center_cnt++;
              // Mark in near_cid_dist_ for check_cid_exist (if needed)
              if (need_distances && center_idx < near_cid_dist_.count()) {
                near_cid_dist_.get_data()[center_idx] = true;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid center index from hgraph", K(ret), K(center_idx), K(centers_total));
          }
        }
        
        LOG_DEBUG("HGraph IVF Flat search completed", K(result_size), K(new_center_cnt),
                 K(incremental_nprobes), K(search_count));

        // Mark that HGraph has reached the end if returned fewer centers than requested
        if (!need_init && new_center_cnt < search_count) {
          hgraph_has_next_center_ = false;
          LOG_INFO("HGraph reached end of centers", K(new_center_cnt), K(search_count));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfScanIter::get_nearest_probe_center_ids(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  
  // decide which search strategy to use based on cache type
  bool need_vectors = false;  
  bool need_distances = !vec_aux_ctdef_->is_post_filter();  
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, false /*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret));
  } else if (is_cache_usable && OB_NOT_NULL(cent_cache) && cent_cache->has_hgraph_index()) {
    has_used_hgraph_ = true;  // Record that HGraph was used in initial scan
    if (OB_FAIL(get_nearest_probe_center_ids_with_hgraph(is_vectorized, cent_cache, need_vectors, need_distances))) {
      LOG_WARN("HGraph search failed for IVF scan", K(ret));
    }
  } else {
    share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()),
      dis_type_, dim_, nprobes_, 0.0, (is_pre_filter() || is_iter_filter()), &iterative_filter_ctx_.allocator_);
    if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap, false, cent_cache, is_cache_usable))) {
      LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
    }
    if (OB_SUCC(ret)) {
      if (nearest_cid_heap.get_center_count() == 0) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (nearest_cid_heap.is_save_all_center()) {
        if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
          LOG_WARN("init_centroids fail", K(ret));
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(nprobes_, near_cid_))) {
          LOG_WARN("failed to get top n", K(ret), K(iterative_filter_ctx_));
        }
      } else if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids(near_cid_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  }
  return ret;
}

// for flat/sq, con_key is vector(float/uint8)
int ObDASIvfScanIter::parse_cid_vec_datum(
  ObIAllocator& allocator,
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey,
  ObString &com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *vec_expr = cid_vec_ctdef->result_output_[1];
  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(allocator, cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (OB_FALSE_IT(com_key = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &allocator,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  com_key))) {
    LOG_WARN("failed to get real data.", K(ret));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_rowkeys_to_heap(const ObString &cid_str, int64_t cid_vec_pri_key_cnt,
                                          int64_t cid_vec_column_count, int64_t rowkey_cnt, bool is_vectorized,
                                          ObVectorCenterClusterHelper<T, ObRowkey> &nearest_rowkey_heap,
                                          bool &is_first_vec, bool &cid_vec_need_norm, ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = adaptive_ctx_.cid_vec_scan_rows_;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
  if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
    LOG_WARN("fail to scan cid range", K(ret), K(cid_str), K(cid_vec_pri_key_cnt));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObExpr *cid_expr = cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      adaptive_ctx_.cid_vec_scan_rows_ += scan_row_cnt;
      if (strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST && max_scan_vectors_ > 0 && adaptive_ctx_.iter_times_ > 1) {
        if (adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
          LOG_INFO("latency mode reach max iter limit", K(ret), K(max_scan_vectors_), K(adaptive_ctx_));
          index_end = true;
        }
      }
      common::ScopedTimer distance_timer(
          common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME, profile_);
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObRowkey main_rowkey;
        ObString vec = cid_datum[i].get_string();
        bool skip = false;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        mem_context_->get_arena_allocator(),
                        cid_datum[i],
                        cid_expr->datum_meta_,
                        vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_ISNULL(vec.ptr())) {
          // ignoring null vector.
        } else if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
          // has been filter, do nothing
          skip = true;
        } else if (std::is_same<T, float>::value && need_norm_) {
          // If the first vec needs do_norm, it means that the vec in the cid_vector table is not normalized.
          if (is_first_vec) {
            if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                    vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()), reinterpret_cast<float *>(vec.ptr()),
                    &cid_vec_need_norm))) {
              LOG_WARN("failed to normalize vector.", K(ret));
            } else {
              is_first_vec = false;
            }
          } else if (cid_vec_need_norm && OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                                              vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()),
                                              reinterpret_cast<float *>(vec.ptr())))) {
            LOG_WARN("failed to normalize vector.", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get rowkey", K(ret));
        } else if (skip) {
        } else if (OB_NOT_NULL(vec.ptr()) && OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
          LOG_WARN("failed to push center.", K(ret));
        } else {
          adaptive_ctx_.vec_dist_calc_cnt_ ++;
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
  } else {
    common::ScopedTimer distance_timer(
        common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME, profile_);
    while (OB_SUCC(ret)) {
      ObRowkey main_rowkey;
      ObString vec;
      bool skip = false;
      adaptive_ctx_.cid_vec_scan_rows_ ++;
      // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
      if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan vid rowkey iter", K(ret));
        }
      } else if (OB_FAIL(parse_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_column_count, cid_vec_ctdef, rowkey_cnt, main_rowkey, vec))) {
        LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
      } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
        // has been filter, do nothing
        skip = true;
      } else if (OB_ISNULL(vec.ptr())) {
        // ignoring null vector.
      } else if (std::is_same<T, float>::value && need_norm_) {
        // If the first vec needs do_norm, it means that the vec in the cid_vector table is not normalized.
        if (is_first_vec) {
          if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                  vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()), reinterpret_cast<float *>(vec.ptr()),
                  &cid_vec_need_norm))) {
            LOG_WARN("failed to normalize vector.", K(ret));
          } else {
            is_first_vec = false;
          }
        } else if (cid_vec_need_norm && OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                                            vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()),
                                            reinterpret_cast<float *>(vec.ptr())))) {
          LOG_WARN("failed to normalize vector.", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (skip) {
      } else if (OB_NOT_NULL(vec.ptr()) &&
                 OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
        LOG_WARN("failed to push center.", K(ret));
      } else {
        adaptive_ctx_.vec_dist_calc_cnt_ ++;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (OB_FAIL(cid_vec_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }

  const int64_t candidate_rows = adaptive_ctx_.cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    if (OB_SUCC(ret) && candidate_rows > 0) {
      INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
    }
  }

  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    T *search_vec,
    ObSEArray<ObRowkey, 16> &saved_rowkeys,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  // only scan without filter can reach here
  ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
  share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
      vec_op_alloc_, search_vec, cur_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
  if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized, search_vec, nearest_rowkey_heap, prefilter))) {
    LOG_WARN("calc_nearest_limit_rowkeys_in_cids fail", K(ret));
  } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys))) {
    LOG_WARN("failed to get top n", K(ret));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    T *serch_vec,
    share::ObVectorCenterClusterHelper<T, ObRowkey> &nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid", K(ret));
  } else {
    cid_str.assign_buffer(buf, buf_len);
  } 
  // 3. Obtain nprobes * k rowkeys
  if (near_cid_.count() == 0) {
    // The situation of index creation with table creation
    ObString empty_cid;
    bool is_first_vec = true;
    bool cid_vec_need_norm = true;
    if (OB_FAIL(get_rowkeys_to_heap(empty_cid, cid_vec_pri_key_cnt, cid_vec_column_count, rowkey_cnt, is_vectorized,
                                    nearest_rowkey_heap, is_first_vec, cid_vec_need_norm, prefilter))) {
      LOG_WARN("failed to get rowkeys to heap, when near_cid is empty", K(ret));
    }
  } else {
    // for adaptor old version(< 4353), which cid_vec is not normlized in cosine dis
    bool is_first_vec = true;
    bool cid_vec_need_norm = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < near_cid_.count(); ++i) {
      const ObCenterId &cur_cid = near_cid_.at(i);
      if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
      } else if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(cur_cid), K(cid_str));
      } else if (OB_FAIL(get_rowkeys_to_heap(cid_str, cid_vec_pri_key_cnt, cid_vec_column_count, rowkey_cnt,
                                             is_vectorized, nearest_rowkey_heap, is_first_vec, cid_vec_need_norm, prefilter))) {
        LOG_WARN("failed to get rowkeys to heap", K(ret), K(cur_cid));
      }
    }
  }
  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_ivf_scan_post<float>(is_vectorized, reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    LOG_WARN("failed to do post filter", K(ret), K(is_vectorized));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::do_ivf_scan_post(bool is_vectorized, T *search_vec)
{
  int ret = OB_SUCCESS;
  const int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
    // 1. Scan the centroid table, range: [min] ~ [max]; sort by l2_distance(c_vec, search_vec_); limit nprobes;
    // return the cid column.
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else if (is_adaptive_filter()) {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  } else if (is_iter_filter()) {
    adaptive_ctx_.iter_times_ = 0;
    ObSEArray<ObIvfRowkeyDistEntry, 16> near_rowkeys;
    int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
    int64_t iter_cnt = 0;
    int64_t next_nprobe = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    int64_t heap_size = get_heap_size(limit_k, selectivity_);
    share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
        vec_op_alloc_, search_vec, dis_type_, dim_, heap_size, similarity_threshold_);
    ObIVFRowkeyDistMap rowkey_dist_map;
    ObIvfRowkeyDistItemCompare head_cmp(dis_type_);
    ObIvfRowkeyDistHeap rowkey_dist_heap(head_cmp);
    if (OB_FAIL(rowkey_dist_map.create(32, lib::ObMemAttr(MTL_ID(), "IVFMap") ))) {
      LOG_WARN("create rowkey dist map fail", K(ret));
    }
    while (OB_SUCC(ret) && ! iter_end && near_rowkeys.count() < limit_k) {
      rowkey_dist_map.reuse();
      ++adaptive_ctx_.iter_times_;
      int32_t start_idx = -1;
      if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized, search_vec, nearest_rowkey_heap, nullptr))) {
        LOG_WARN("failed to get nearest limit rowkeys in cids", K(near_cid_));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_centers_dist_map(rowkey_dist_map))) {
        LOG_WARN("get dist map fail", K(ret));
      } else if (OB_FALSE_IT(start_idx = near_rowkeys.count())) {
      } else if (OB_FAIL(do_post_filter(is_vectorized, rowkey_dist_map, near_rowkeys))) {
        LOG_WARN("do post filter fail", K(ret), K(near_rowkeys.count()));
      } else if (OB_FALSE_IT(no_new_near_rowkeys = (near_rowkeys.count() == start_idx))) {
        // there are no new rowkeys in near_rowkeys after post filter
      } else {
        for (int64_t i = start_idx; OB_SUCC(ret) && i < near_rowkeys.count(); ++i) {
          const ObIvfRowkeyDistEntry &entry = near_rowkeys.at(i);
          if (OB_FAIL(rowkey_dist_heap.push(ObIvfRowkeyDistItem(i, entry.distance_)))) {
            LOG_WARN("push rowkey dist fail", K(ret), K(i), K(entry));
          } else {
            LOG_TRACE("push", K(entry), K(i), K(limit_k));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (near_rowkeys.count() < limit_k) {
        ++iter_cnt;
        const int64_t left_search = limit_k - near_rowkeys.count();
        next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
        const double select_ratio = double(adaptive_ctx_.iter_res_row_cnt_) /  double(adaptive_ctx_.iter_filter_row_cnt_);
        const int64_t new_heap_size = OB_MAX(get_heap_size(left_search, select_ratio), heap_size);
        LOG_INFO("postfilter does not get enough result", K(limit_k), "near_rowkeys_count", near_rowkeys.count(),
            K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(heap_size),
            K(new_heap_size), K(select_ratio), K(can_retry_), K(strategy_), K(adaptive_ctx_));
        near_cid_.reuse();
        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
          LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(near_rowkeys.count()), K(start_idx));
        } else if (max_scan_vectors_ > 0 && adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
          iter_end = true;
          LOG_INFO("reach max scan vectors, stop iterative filter", K(no_new_near_rowkeys), K(near_rowkeys.count()), 
              K(start_idx), K(limit_k), K(left_search), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
        } else if (can_retry_ && OB_FAIL(check_iter_filter_need_retry())) {
          LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
        } else if (! has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(get_next_center_ids(is_vectorized, next_nprobe))) {
          LOG_WARN("get next centers failed", K(ret), K(has_used_hgraph_), K(next_nprobe));
        } else if (near_cid_.count() == 0) {
          // Common check: if no centers found after getting next batch, end iteration
          iter_end = true;
          LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
        } else if (OB_FAIL(nearest_rowkey_heap.set_nprobe(new_heap_size))) {
          LOG_WARN("set new heap size fail", K(ret), K(new_heap_size), K(heap_size));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for(int64_t cnt = 0; OB_SUCC(ret) && cnt < limit_k && ! rowkey_dist_heap.empty(); ++cnt) {
        const ObIvfRowkeyDistItem &item = rowkey_dist_heap.top();
        if (item.rowkey_idx_ > near_rowkeys.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out of bound of near_rowkeys", K(ret), K(item), K(near_rowkeys.count()));
        } else if (OB_FAIL(saved_rowkeys_.push_back(near_rowkeys.at(item.rowkey_idx_).rowkey_))) {
          LOG_WARN("push back fail", K(ret), K(item));
        } else if (OB_FAIL(rowkey_dist_heap.pop())) {
          LOG_WARN("rowkey_dist_heap pop fail", K(ret), K(saved_rowkeys_.count()), K(item));
        } else {
          LOG_TRACE("result", K(item), "i", cnt, K(limit_k));
        }
      }
    }
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized,
                                                              search_vec,
                                                              saved_rowkeys_, nullptr))) {
    // 2. get cidx in (top-nprobes 个 cid)
    //    scan cid_vector table, range: [cidx, min] ~ [cidx, max]; sort by l2_distance(vec, search_vec_); limit
    //    (limit+offset); return rowkey column
    LOG_WARN("failed to get nearest limit rowkeys in cids", K(near_cid_));
  }
  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_ivf_scan_pre<float>(allocator, is_vectorized, reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::do_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized, T *search_vec)
{
  int ret = OB_SUCCESS;
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  bool is_range_prefilter = vec_aux_ctdef_->can_use_vec_pri_opt();
  const int64_t est_output_row_cnt = adaptive_ctx_.selectivity_ * adaptive_ctx_.row_count_;
  const bool need_check_brute = est_output_row_cnt <= IVF_MAX_BRUTE_FORCE_SIZE * 10 || ! is_range_prefilter;
  ObIvfPreFilter prefilter(MTL_ID());
  if (need_check_brute && OB_FAIL(get_rowkey_pre_filter(mem_context_->get_arena_allocator(), is_vectorized, IVF_MAX_BRUTE_FORCE_SIZE))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  } else if (need_check_brute && pre_fileter_rowkeys_.count() < IVF_MAX_BRUTE_FORCE_SIZE) {
    if (OB_FAIL(do_brute_force_prefilter_rowkeys(is_vectorized, reinterpret_cast<float *>(real_search_vec_.ptr()), raw_dis_type))) {
      LOG_WARN("failed to do brute force prefilter rowkeys", K(ret));
    }
  } else {
    if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        // cid_center table is empty, just do brute search
        ret = OB_SUCCESS;
        adaptive_ctx_.is_brute_force_= true;
        IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, reinterpret_cast<float *>(real_search_vec_.ptr()) /*unused*/, raw_dis_type, dim_,
                                          get_nprobe(limit_param_, 1), similarity_threshold_);
        bool index_end = false;
        while (OB_SUCC(ret) && !index_end) {
          if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                  index_end))) {
            LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
          } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
            LOG_WARN("failed to get limit rowkey brute", K(ret));
          } else {
            pre_fileter_rowkeys_.reset();
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("failed to get top n", K(ret));
        }
      } else {
        LOG_WARN("failed to get nearest probe center ids", K(ret));
      }
    } else {
      if (is_range_prefilter) { // rowkey range prefilter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        adaptive_ctx_.is_range_prefilter_ = true;
        ObArray<const ObNewRange *> rk_range;
        const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
        for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
          const ObNewRange *range = &key_range.at(i);
          if (OB_FAIL(rk_range.push_back(range))) {
            LOG_WARN("fail to push back range", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prefilter.init(rk_range))) {
            LOG_WARN("fail to init prefilter as range filter", K(ret));
          }
        }
      } else if (OB_FAIL(prefilter.init())) { // rowkey hash bitmap prefilter
        LOG_WARN("fail to init prefilter as roaring bitmap", K(ret));
      } else { // add bitmap for bitmap filter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        for (int i = 0; i < pre_fileter_rowkeys_.count() && OB_SUCC(ret); i++) {
          uint64_t hash_val = hash_val_for_rk(pre_fileter_rowkeys_.at(i));
          if (OB_FAIL(prefilter.add(hash_val))) {
            LOG_WARN("fail to add hash val to prefilter", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(build_rowkey_hash_set(prefilter, is_vectorized, batch_row_count))) {
          LOG_WARN("fail to build rk hash set", K(ret));
        }
      }

      // do for loop until saved_rowkeys_.count() >= limitK
      if (OB_SUCC(ret)) {
        int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
        int64_t iter_cnt = 0;
        int64_t next_nprobe = 0;
        bool iter_end = false;
        bool is_first_scan = true;
        int32_t start_idx = -1;
        bool no_new_near_rowkeys = false;
        share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
            vec_op_alloc_, search_vec, dis_type_, dim_, limit_k, similarity_threshold_);
        while (OB_SUCC(ret) && ! iter_end && nearest_rowkey_heap.count() < limit_k) {
          if (OB_FALSE_IT(start_idx = nearest_rowkey_heap.count())) {
          } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(
              is_vectorized,
              search_vec,
              nearest_rowkey_heap,
              &prefilter))) {
            LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
          } else if (OB_FALSE_IT(no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx))) {
            // there are no new rowkeys in near_rowkeys after post filter
          }
          if (OB_FAIL(ret)) {
          } else if (nearest_rowkey_heap.count() < limit_k) {
            ++iter_cnt;
            const int64_t left_search = limit_k - nearest_rowkey_heap.count();
            next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
            LOG_INFO("prefilter does not get enough result", K(limit_k), "nearest_rowkey_heap_count", nearest_rowkey_heap.count(),
                K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(can_retry_), K(strategy_), K(adaptive_ctx_));
            near_cid_.reuse();
            is_first_scan = false;
            if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
              iter_end = true;
              LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), K(start_idx), K(limit_k));
            } else if (max_scan_vectors_ > 0 && adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
              iter_end = true;
              LOG_INFO("reach max scan vectors, stop iterative filter", K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), 
                K(start_idx), K(limit_k), K(left_search), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
            } else if (! has_next_center()) {
              iter_end = true;
            } else if (OB_FAIL(get_next_center_ids(is_vectorized, next_nprobe))) {
              LOG_WARN("get next centers failed", K(ret), K(has_used_hgraph_), K(next_nprobe));
            } else if (near_cid_.count() == 0) {
              // Common check: if no centers found after getting next batch, end iteration
              iter_end = true;
              LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("get rowkeys from heap fail", K(ret), K(limit_k));
        } else {
          LOG_TRACE("get rowkeys from heap success", K(limit_k), K(saved_rowkeys_.count()), K(iter_cnt), K(next_nprobe), K(nprobes_));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::check_cid_exist(const ObString &src_cid, bool &src_cid_exist)
{
  int ret = OB_SUCCESS;
  src_cid_exist = false;
  ObCenterId src_centor_id;
  if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else if (OB_UNLIKELY(src_centor_id.center_id_ >= near_cid_dist_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_cid is not exist", K(ret), K(src_centor_id), K(near_cid_dist_.count()));
  } else if (near_cid_dist_.at(src_centor_id.center_id_)) {
    src_cid_exist = true;
  }

  return ret;
}

void ObDASIvfScanIter::reuse_cid_ctx()
{
  ObDASIvfBaseScanIter::reuse_cid_ctx();
  near_cid_.reuse();
}
/************************************************** ObDASIvfPQScanIter ******************************************************/

int ObDASIvfPQScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam index_param;
  uint64_t tenant_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_FAIL(ObDASIvfBaseScanIter::inner_init(param))) {
    LOG_WARN("fail to do inner init ", K(ret), K(param));
  } else if (!((tenant_cluster_version >= MOCK_CLUSTER_VERSION_4_3_5_3 &&
                tenant_cluster_version < CLUSTER_VERSION_4_4_0_0) ||
               tenant_cluster_version >= CLUSTER_VERSION_4_4_1_0)) {
    if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vec_aux_ctdef_->vec_index_param_,
                                                             ObVectorIndexType::VIT_IVF_INDEX, index_param))) {
      LOG_WARN("fail to parse params from string", K(ret), K(vec_aux_ctdef_->vec_index_param_));
    }
  } else if (OB_FAIL(index_param.assign(vec_aux_ctdef_->get_vec_index_param()))) {
    LOG_WARN("fail to assign params from vec_aux_ctdef_", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    pq_centroid_iter_ = ivf_scan_param.pq_centroid_iter_;
    m_ = vec_index_param_.m_;
    nbits_ = vec_index_param_.nbits_;
    if (dim_ % m_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dim should be able to divide m exactly", K(ret), K(dim_), K(m_));
    } else if (nbits_ < 1 || nbits_ > 24) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nbits should be in range [1,24]", K(ret), K(nbits_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(near_cid_vec_.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else if (OB_FAIL(near_cid_vec_dis_.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest cid vec dis", K(ret), K(vec_index_param_.nlist_));
  } else if (OB_FAIL(near_cid_vec_ptrs_.allocate_array(persist_alloc_, vec_index_param_.nlist_ + 1))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else {
    memset(near_cid_vec_ptrs_.get_data(), 0, near_cid_vec_ptrs_.count() * sizeof(float *));
  }
  return ret;
}

int ObDASIvfPQScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  near_cid_vec_.reuse();
  near_cid_vec_dis_.reuse();
  memset(near_cid_vec_ptrs_.get_data(), 0, near_cid_vec_ptrs_.count() * sizeof(float *));

  if (!pq_centroid_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } else if (OB_FAIL(ObDASIvfBaseScanIter::inner_reuse())) {
    LOG_WARN("fail to do inner reuse", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pq_centroid_iter_) && OB_FAIL(release_input_iter(pq_centroid_iter_))) {
    LOG_WARN("failed to release pq_centroid_iter_", K(ret));
  }
  pq_centroid_iter_ = nullptr;
  ObDasVecScanUtils::release_scan_param(pq_centroid_scan_param_);
  near_cid_vec_.reset();
  near_cid_vec_dis_.reset();
  near_cid_vec_ptrs_.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDASIvfBaseScanIter::inner_release())) {
    LOG_WARN("fail to do inner release", K(ret));
  }

  return ret;
}

// for pq, con_key is array(vector(float))
int ObDASIvfPQScanIter::parse_pq_ids_vec_datum(
  ObIAllocator &allocator,
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey,
  ObString &com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
  
  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(allocator, cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).is_null()) {
    // do nothing
    com_key.reset();
  } else {
    com_key = pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string();
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids_by_cache(ObIvfICache &cent_cache, 
                                                              const ObString &pq_center_ids,
                                                              const ObIArray<float *> &splited_residual,
                                                              float &distance) {
  int ret = OB_SUCCESS;

  RWLock::RLockGuard guard(cent_cache.get_lock());
  float square = 0.0f;
  const uint8_t* pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);
  for (int j = 0; OB_SUCC(ret) && j < m_; ++j) {
    // 3.2.1 pq_center_ids[j] is put into ivf_pq_centroid table to find pq_center_vecs[j]
    float *pq_cid_vec = nullptr;
    if (OB_FAIL(cent_cache.read_pq_centroid(j + 1 /*m*/, decoder.decode() + 1, pq_cid_vec))) {
      LOG_WARN("fail to get pq cid vec from cache", K(ret), K(j + 1));
    } else if (OB_ISNULL(pq_cid_vec)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null pq_cid_vec", 
        K(ret), K(j), KPHEX(pq_center_ids.ptr(), pq_center_ids.length()));
    } else {
      // 3.3.2 Calculate the distance between pq_center_vecs[j] and r(x)[j].
      //       The sum of j = 0 ~ m is the distance from x to rowkey
      float dis = 0.0f;
      if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(j), pq_cid_vec, dim_ / m_, dis, dis_type_))) {
        SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
      } else {
        square += dis;
      }
    }
  } // end for
  
  if (OB_SUCC(ret)) {
    distance = square;
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids_by_table(
  bool is_vectorized,
  const ObString &pq_center_ids,
  const ObIArray<float *> &splited_residual,
  int64_t batch_row_count,
  float &distance)
{
  // todo(wmj): need fit ivf adaptor cache，pq_center_ids find pq_center_vec
  int ret = OB_SUCCESS;
  float dis_square = 0.0f;
  const ObDASScanCtDef *pq_cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_pq_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *pq_cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_pq_id_tbl_idx());
  common::ScopedTimer fallback_scan_timer(
      common::ObMetricId::VEC_IVF_PQ_CENTROID_CACHE_FALLBACK_SCAN_TIME,
      OB_NOT_NULL(pq_cid_vec_ctdef) && OB_NOT_NULL(pq_cid_vec_rtdef) ? profile_ : nullptr);
  if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
  } 

  int count = 0;
  int pq_cid_idx = 0;
  int64_t pq_cid_count = m_;
  uint64_t tablet_id = ObVecIVFPQCenterIDS::get_tablet_id(pq_center_ids.ptr());
  const uint8_t* pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);
  while (OB_SUCC(ret) && count < pq_cid_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < pq_cid_count; ++i, ++count) {
      ObRowkey pq_cid_rowkey;
      ObString pq_center_id_str;
      ObPqCenterId pq_center_id(tablet_id, count + 1, decoder.decode() + 1);
      if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_center_id_str,
                                                                    &mem_context_->get_arena_allocator()))) {
        LOG_WARN("fail to set pq center id to string", K(ret), K(pq_center_id));
      } else if (OB_FAIL(build_cid_vec_query_rowkey(pq_center_id_str, true /*is_min*/, CENTROID_PRI_KEY_CNT, // first rowkey is pq_cid
                                             pq_cid_rowkey))) {
        LOG_WARN("failed to build cid vec query rowkey", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(pq_cid_rowkey, pq_centroid_scan_param_,
                                                           pq_cid_vec_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_table_scan(pq_centroid_first_scan_, pq_centroid_scan_param_, pq_cid_vec_ctdef,
                                         pq_cid_vec_rtdef, pq_centroid_iter_, pq_centroid_tablet_id_))) {
      LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(pq_centroid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
        ObDatum *vec_datum = vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          float cur_dis = 0.0f;
          guard.set_batch_idx(i);
          ObString vec = vec_datum[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_ISNULL(vec.ptr())) {
            // ignoring null vector.
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(pq_cid_idx), reinterpret_cast<float *>(vec.ptr()),
                                                 dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_)
    } else {
      pq_centroid_iter_->clear_evaluated_flag();
      ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        float cur_dis = 0.0f;
        if (OB_FAIL(pq_centroid_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else {
          ObString vec = vec_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_ISNULL(vec.ptr())) {
            // ignoring null vector.
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(pq_cid_idx), reinterpret_cast<float *>(vec.ptr()),
                                                 dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }
  if (pq_cid_idx != pq_cid_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pq cid idx", K(ret), K(pq_cid_idx), K(pq_cid_count));
  } else {
    distance = dis_square;
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids(
    bool is_vectorized,
    const ObString &pq_center_ids, 
    const ObIArray<float *> &splited_residual, 
    float &distance)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(cent_cache));
  } else if (is_cache_usable) {
    if (OB_FAIL(calc_distance_between_pq_ids_by_cache(*cent_cache, pq_center_ids, splited_residual, distance))) {
      LOG_WARN("fail to calc distance between pq ids by cache", K(ret), K(is_vectorized), KPC(cent_cache));
    }
  } else {
    if (OB_FAIL(calc_distance_between_pq_ids_by_table(is_vectorized, 
                                                      pq_center_ids, 
                                                      splited_residual, 
                                                      ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE, 
                                                      distance))) {
      LOG_WARN("fail to calc distance between pq ids by table", K(ret), K(is_vectorized));
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::calc_distance_with_precompute(
    ObEvalCtx::BatchInfoScopeGuard &guard,
    int64_t scan_row_cnt,
    int64_t rowkey_cnt,
    ObRowkey& filter_main_rowkey,
    float *sim_table,
    float dis0,
    IvfRowkeyHeap& nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObExpr *cid_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
  ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
  int counter = 0;
  size_t saved_j[4] = {0, 0, 0, 0};
  for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
    bool is_skip = false;
    if (cid_datum[j].is_null()) {
      is_skip = true;
    } else if (prefilter != nullptr) {
      // get rowkey and filter first
      guard.set_batch_idx(j);
      if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, filter_main_rowkey, false))) {
        LOG_WARN("fail to get main rowkey", K(ret));
      } else if (!prefilter->test(filter_main_rowkey)) {
        is_skip = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_skip) {
    } else {
      adaptive_ctx_.vec_dist_calc_cnt_++;
      saved_j[0] = (counter == 0) ? j : saved_j[0];
      saved_j[1] = (counter == 1) ? j : saved_j[1];
      saved_j[2] = (counter == 2) ? j : saved_j[2];
      saved_j[3] = (counter == 3) ? j : saved_j[3];
      counter += 1;
      if (counter == 4) {
        float distance_0 = 0;
        float distance_1 = 0;
        float distance_2 = 0;
        float distance_3 = 0;
        {
          const uint8_t* pq_id_ptr_0 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[0]].get_string().ptr());
          const uint8_t* pq_id_ptr_1 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[1]].get_string().ptr());
          const uint8_t* pq_id_ptr_2 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[2]].get_string().ptr());
          const uint8_t* pq_id_ptr_3 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[3]].get_string().ptr());
          ObVectorL2Distance<float>::distance_four_codes(
            m_, nbits_, sim_table, pq_id_ptr_0, pq_id_ptr_1, pq_id_ptr_2, pq_id_ptr_3,
            distance_0, distance_1, distance_2, distance_3);
          distance_0 = distance_0 + dis0;
          distance_1 = distance_1 + dis0;
          distance_2 = distance_2 + dis0;
          distance_3 = distance_3 + dis0;
          // 先检查是否需要分配内存，避免不必要的内存分配
          if (nearest_rowkey_heap.should_push_center(distance_0)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[0]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_0))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_1)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[1]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_1))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_2)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[2]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_2))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_3)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[3]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_3))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
        }
        counter = 0;
      }
    }
  }
  // process counter left
  for (size_t kk = 0; kk < counter && OB_SUCC(ret); kk++) {
    const uint8_t* pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[kk]].get_string().ptr());
    float dis = dis0 + ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
    if (nearest_rowkey_heap.should_push_center(dis)) {
      guard.set_batch_idx(saved_j[kk]);
      ObRowkey main_rowkey;
      {
        if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, dis))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::check_can_pre_compute(
    bool is_vectorized,
    ObIvfCacheMgrGuard &pre_cache_guard,
    ObIvfICache *&pre_cent_cache,
    bool &pre_compute_table)
{
  int ret = OB_SUCCESS;
  int64_t ksub = 1L << nbits_;
  pre_compute_table = false;
  if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    if (OB_FAIL(get_pq_precomputetable_cache(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
      LOG_WARN("fail to get pq precompute table cache", K(ret), K(is_vectorized), KPC(pre_cent_cache));
    }
  } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
    // check pq center cache is ok
    ObIvfCacheMgrGuard pq_center_guard;
    ObIvfICache *pq_cent_cache = nullptr;
    bool is_pq_cent_cache_usable = false;
    if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
      LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
    } else if (is_pq_cent_cache_usable) {
      pre_compute_table = true;
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    ObSEArray<ObRowkey, 16> &saved_rowkeys,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  int64_t sub_dim = dim_ / m_;
  // only scan without filter can reach here
  ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
  IvfRowkeyHeap nearest_rowkey_heap(
      vec_op_alloc_, search_vec, cur_dis_type, sub_dim, get_nprobe(limit_param_, 1), similarity_threshold_);
  if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(is_vectorized, search_vec, nearest_rowkey_heap, prefilter))) {
    LOG_WARN("calc_nearest_limit_rowkeys_in_cids fail", K(ret));
  } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys))) {
    LOG_WARN("failed to get top n", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    IvfRowkeyHeap &nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = adaptive_ctx_.cid_vec_scan_rows_;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);

  int64_t sub_dim = dim_ / m_;
  int64_t ksub = 1L << nbits_;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  ObArray<float *> splited_residual;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;
  float *residual = nullptr;
  bool pre_compute_table = false;
  ObIvfCacheMgrGuard pre_cache_guard;
  ObIvfICache *pre_cent_cache = nullptr;
  float *sim_table = nullptr;
  float *sim_table_2 = nullptr;
  const float* sim_table_ptrs = nullptr;
  ObRowkey filter_main_rowkey;
  bool is_l2 = (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN);
  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid", K(ret));
  } else if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
  } else if (OB_FAIL(check_can_pre_compute(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
    LOG_WARN("fail to check can use precomputetable", K(ret));
  } else if (!pre_compute_table) {
    char *residual_buf = nullptr;
    if (OB_FAIL(splited_residual.reserve(m_))) {
      LOG_WARN("fail to init splited residual array", K(ret), K(m_));
    } else if (is_l2) { // only L2 need residual
      if (OB_ISNULL(residual_buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(dim_ * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc residual buf", K(ret));
      } else if (OB_FALSE_IT(residual = new(residual_buf) float[dim_])) {
      }
    }
  } else { // scan by precompute table cache
    ObObj *obj_ptr = nullptr;
    sim_table = (float*)mem_context_->get_arena_allocator().alloc(sizeof(float) * (ksub * m_) * 2);
    obj_ptr = (ObObj*)mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt);
    if (OB_ISNULL(sim_table) || OB_ISNULL(obj_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for precompute table", K(ret), K(ksub), K(m_), K(rowkey_cnt),
               "sim_table_size", sizeof(float) * (ksub * m_) * 2,
               "obj_ptr_size", sizeof(ObObj) * rowkey_cnt,
               KP(sim_table), KP(obj_ptr));
    } else {
      filter_main_rowkey.assign(obj_ptr, rowkey_cnt);
      sim_table_2 = sim_table + (ksub * m_);
      if (is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table_2, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      } else if (!is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      }
    }
  }
  // 1. for every (cid, cid_vec),
  for (int i = 0; OB_SUCC(ret) && i < near_cid_vec_.count(); ++i) {
    const ObCenterId &cur_cid = near_cid_vec_.at(i).first;
    float *cur_cid_vec = near_cid_vec_.at(i).second;
    float dis0 = 0.0f;
    if (pre_compute_table) {
      if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
        sim_table_ptrs = pre_cent_cache->get_centroids() + (cur_cid.center_id_ - 1) * ksub * m_;
        ObVectorL2Distance<float>::fvec_madd(m_ * ksub,
                                             sim_table_ptrs,
                                             -2.0,
                                             sim_table_2,
                                             sim_table);
      }
      dis0 = near_cid_vec_dis_.at(i);
    } else {
      // 1.1 Calculate the residual r(x) = x - cid_vec
      //     split r(x) into m parts, the jth part is called r(x)[j]
      splited_residual.reuse();
      if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
        if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(dim_, search_vec, cur_cid_vec, residual))) {
          LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
        } 
      } else {
        dis0 = near_cid_vec_dis_.at(i);
        residual = search_vec; // ip dis = dis0 + search_vec · pq_vec
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
      } else if (OB_FAIL(ObVectorIndexUtil::split_vector(m_, dim_, residual, splited_residual))) {
        LOG_WARN("fail to split vector", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // 1.2 cid put the query in the ivf_pq_code table to find (rowkey, pq_center_ids)
      storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
      if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
      } else if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(cur_cid), K(cid_str));
      } else if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
        LOG_WARN("fail to scan cid range", K(ret), K(cur_cid), K(cid_vec_pri_key_cnt));
      } else if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          ObExpr *cid_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
          ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
          adaptive_ctx_.cid_vec_scan_rows_ += scan_row_cnt;
          if (strategy_ == ObVecIdxQueryStrategy::LATENCY_FIRST && max_scan_vectors_ > 0 && adaptive_ctx_.iter_times_ > 1) {
            if (adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
              LOG_INFO("latency mode reach max iter limit", K(ret), K(max_scan_vectors_), K(adaptive_ctx_));
              index_end = true;
            }
          }

          common::ScopedTimer distance_timer(
              common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME, profile_);
          common::ScopedTimer fallback_distance_timer(
              common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_FALLBACK_CALC_TIME,
              pre_compute_table ? nullptr : profile_);
          if (pre_compute_table) {
            if (OB_FAIL(calc_distance_with_precompute(guard, scan_row_cnt, rowkey_cnt, filter_main_rowkey,
                                                      sim_table, dis0, nearest_rowkey_heap, prefilter))) {
              LOG_WARN("fail to calc distance with pre compute table", K(ret));
            }
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
              bool is_skip = false;
              guard.set_batch_idx(j);
              if (cid_datum[j].is_null() || cid_datum[j].get_string().empty()) {
                // do nothing
              } else {
                ObRowkey main_rowkey;
                ObString pq_center_ids = cid_datum[j].get_string();
                float distance = 0;
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
                  // has been filter, do nothing
                } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids, splited_residual, distance))) {
                  LOG_WARN("fail to calc distance between pq ids", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
                  LOG_WARN("failed to push center.", K(ret));
                } else {
                  adaptive_ctx_.vec_dist_calc_cnt_++;
                }
              }
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
      } else {
        common::ScopedTimer distance_timer(
            common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME, profile_);
        common::ScopedTimer fallback_distance_timer(
            common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_FALLBACK_CALC_TIME,
            pre_compute_table ? nullptr : profile_);
        while (OB_SUCC(ret)) {
          ObRowkey main_rowkey;
          ObString vec_arr_str;
          ObString pq_center_ids;
          float distance = 0.0f;
          adaptive_ctx_.cid_vec_scan_rows_++;
          // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
          if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to scan vid rowkey iter", K(ret));
            }
          } else if (OB_FAIL(parse_pq_ids_vec_datum(
              mem_context_->get_arena_allocator(), 
              cid_vec_column_count, 
              cid_vec_ctdef, 
              rowkey_cnt,
              main_rowkey, 
              pq_center_ids))) {
            LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
          } else if (pq_center_ids.empty()) {
            // ignore null arr
          } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
            // has been filter, do nothing
          } else {
            if (pre_compute_table) {
              const uint8_t* pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
              distance = ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
            } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids, splited_residual, distance))) {
              LOG_WARN("fail to calc distance between pq ids", K(ret));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
              LOG_WARN("failed to push center.", K(ret));
            } else {
              adaptive_ctx_.vec_dist_calc_cnt_++;
            }
          }
        } // end while

        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          if (OB_FAIL(cid_vec_iter_->reuse())) {
            LOG_WARN("fail to reuse scan iterator.", K(ret));
          }
        }
      }
    }
  } // end for i
  const int64_t candidate_rows = adaptive_ctx_.cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    if (OB_SUCC(ret) && candidate_rows > 0) {
      INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
    }
  }
  return ret;
}

// HGraph-based iterative filtering implementations
int ObDASIvfPQScanIter::get_next_probe_centers_by_hgraph(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, false /*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret));
  } else if (is_cache_usable && OB_NOT_NULL(cent_cache) && cent_cache->has_hgraph_index()) {
    if (OB_FAIL(get_nearest_probe_centers_with_hgraph(
        is_vectorized, cent_cache, true, true, next_nprobe))) {
      LOG_WARN("HGraph PQ iterative search failed", K(ret), K(next_nprobe));
    }
  } else {
    // If HGraph was used before but now unavailable, this is an error
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("HGraph was used in initial scan but now unavailable for iterative filtering", K(ret),
        K(is_cache_usable), K(OB_NOT_NULL(cent_cache)), K(cent_cache ? cent_cache->has_hgraph_index() : false));
  }

  return ret;
}

int ObDASIvfPQScanIter::get_next_centers(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  if (has_used_hgraph_) {
    if (OB_FAIL(get_next_probe_centers_by_hgraph(is_vectorized, next_nprobe))) {
      LOG_WARN("failed to get next probe centers by hgraph", K(ret), K(is_vectorized), K(next_nprobe));
    }
  } else {
    if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(next_nprobe, near_cid_vec_, near_cid_vec_dis_))) {
      LOG_WARN("failed to get next nearest probe centers vec dist", K(ret), K(next_nprobe));
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::get_nearest_probe_centers_with_hgraph(
    bool is_vectorized, 
    ObIvfICache *hgraph_cache,
    bool need_vectors, 
    bool need_distances,
    int64_t incremental_nprobes)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hgraph_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hgraph cache is null", K(ret));
  } else {
    common::obvsag::VectorIndexPtr hgraph_index = hgraph_cache->get_hgraph_index();
    if (OB_ISNULL(hgraph_index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hgraph index is null", K(ret));
    } else {
      // use OceanBase's VSAG interface to search
      const float* distances = nullptr;
      const int64_t* result_ids = nullptr;
      int64_t result_size = 0;
      float* search_vec = reinterpret_cast<float*>(const_cast<char*>(real_search_vec_.ptr()));
      const char* extra_info = nullptr;
      int64_t centers_total = hgraph_cache->get_count();
      int64_t ef_search = 64;
      // Check if need to initialize HGraph state (incremental_nprobes == -1 means initial search)
      bool need_init = (incremental_nprobes == -1);
      int64_t search_count = need_init ? nprobes_ : incremental_nprobes;
      // Create VSAG allocator if not exists (same lifecycle as hgraph_iter_ctx_)
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_hgraph_search_alloc())) {
        LOG_WARN("failed to init hgraph search alloc", K(ret));
      } else if (OB_FAIL(obvectorutil::knn_search(hgraph_index,
                                            search_vec,
                                            dim_, 
                                            search_count,
                                            distances,
                                            result_ids,
                                            extra_info,
                                            result_size,
                                            ef_search,
                                            nullptr,
                                            false,
                                            false,
                                            1.0f,
                                            hgraph_vsag_alloc_,      // Use persistent allocator with same lifecycle as iter_ctx
                                            false,
                                            hgraph_iter_ctx_,        // Always pass iter context, let HGraph manage it
                                            false))) {               // is_last_search, keep searching
        LOG_WARN("failed to search with hgraph index", K(ret), K(search_count), K(ef_search));
      }
      
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(result_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hgraph search result is null", K(ret));
      } else if (result_size == 0) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("hgraph search result is empty", K(ret));
      } else {
        near_cid_vec_.reset();
        near_cid_vec_dis_.reset();
        int64_t new_center_cnt = 0;
        for (int64_t i = 0; i < result_size && OB_SUCC(ret); ++i) {
          int64_t center_idx = result_ids[i];
          if (center_idx >= 1 && center_idx <= centers_total) {
            ObCenterId center_id;
            center_id.center_id_ = center_idx;
            center_id.tablet_id_ = centroid_tablet_id_.id();
            if (need_vectors) {
              float* center_vec = nullptr;
              if (OB_FAIL(hgraph_cache->read_centroid_from_hgraph(
                      center_idx, center_vec, *hgraph_vsag_alloc_))) {
                LOG_WARN("failed to read centroid for hgraph center vector", K(ret), K(center_idx));
              } else if (OB_ISNULL(center_vec)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("read centroid returned null pointer", K(ret), K(center_idx));
              } else {
                IvfCidVecPair cid_vec_pair(center_id, center_vec);
                if (OB_FAIL(near_cid_vec_.push_back(cid_vec_pair))) {
                  LOG_WARN("failed to push cid vec pair", K(ret));
                }
              }
            }
            // save distance
            if (OB_SUCC(ret) && need_distances && OB_NOT_NULL(distances)) {
              float final_distance = distances[i];
              if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT && need_norm_) {
                final_distance = 1.0f - distances[i];
              } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
                final_distance = 1.0f - distances[i];
              }
              if (OB_FAIL(near_cid_vec_dis_.push_back(final_distance))) {
                LOG_WARN("failed to push distance", K(ret));
              }
            }
            
            if (OB_SUCC(ret)) {
              new_center_cnt++;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid center index from hgraph", K(ret), K(center_idx), K(centers_total));
          }
        }
        
        LOG_DEBUG("HGraph IVF PQ search completed", K(result_size), K(new_center_cnt), 
                 K(incremental_nprobes), K(search_count));

        // Mark that HGraph has reached the end if returned fewer centers than requested
        if (!need_init && new_center_cnt < search_count) {
          hgraph_has_next_center_ = false;
          LOG_INFO("HGraph reached end of centers", K(new_center_cnt), K(search_count));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::get_nearest_probe_centers(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  //precompute table use euclidean_squared, so we need to convert to euclidean
  //L2_squared = dis0^2 + precomcute.result
  ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_;
  if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    cur_dis_type = oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED;
  }
  // decide which search strategy to use based on cache type
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, false /*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("failed to get centers cache", K(ret));
  } else if (is_cache_usable && OB_NOT_NULL(cent_cache) && cent_cache->has_hgraph_index()) {
    has_used_hgraph_ = true;  // Record that HGraph was used in initial scan
    if (OB_FAIL(get_nearest_probe_centers_with_hgraph(is_vectorized, cent_cache, true/*need_vectors*/, true/*need_distances*/))) {
      LOG_WARN("HGraph search failed for PQ scan", K(ret));
    }
  } else {
    share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()),
      cur_dis_type, dim_, nprobes_, 0.0, (is_pre_filter() || is_iter_filter()), &iterative_filter_ctx_.allocator_);
    if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap, true/*save_center_vec*/, cent_cache, is_cache_usable))) {
      LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
    } else {
      if (nearest_cid_heap.get_center_count() == 0) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (nearest_cid_heap.is_save_all_center()) {
        if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
          LOG_WARN("init_centroids fail", K(ret));
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(nprobes_, near_cid_vec_, near_cid_vec_dis_))) {
          LOG_WARN("failed to get top n", K(ret), K(iterative_filter_ctx_));
        }
      } else { // whatever pre or post, go here
        if (OB_FAIL(nearest_cid_heap.get_nearest_probe_centers_vec_dist(near_cid_vec_, near_cid_vec_dis_))) {
          LOG_WARN("failed to get top n", K(ret));
        } 
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_rowkey_brute_post(bool is_vectorized, IvfRowkeyHeap& nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer search_timer(common::ObMetricId::VEC_INDEX_SEARCH_TIME,
                                   vector_iter_profile);

  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObString raw_search_vec;
  const ObDASScanCtDef *brute_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_brute_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  ObDASScanRtDef *brute_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_brute_tbl_idx());
  int64_t main_rowkey_cnt = 0;
  // prepare norm info
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
  ObVectorNormalizeInfo norm_info;
  if (OB_ISNULL(brute_ctdef) || OB_ISNULL(brute_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(brute_ctdef), KP(brute_rtdef));
  } else if (OB_FALSE_IT(main_rowkey_cnt = brute_ctdef->table_param_.get_read_info().get_schema_rowkey_count())) {
  } else if (OB_UNLIKELY(main_rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else if (need_norm_) {
    if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(), sort_rtdef_->eval_ctx_, search_vec_,
                                                       raw_search_vec))) {
      LOG_WARN("failed to get real search vec", K(ret));
    } else if (OB_FALSE_IT(search_vec = reinterpret_cast<float *>(raw_search_vec.ptr()))) {
    }
  }
  int count = 0;
  int batch_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  int search_count = (pre_fileter_rowkeys_.count() > 0) ? pre_fileter_rowkeys_.count() : 1;
  while (OB_SUCC(ret) && count < search_count) {
    if (pre_fileter_rowkeys_.count() > 0) {
      // setup lookup key range
      brute_scan_param_.key_ranges_.reset(); // clean key range array
      for (int i = 0; OB_SUCC(ret) && i < batch_count && count < search_count; i++, count++) {
        ObNewRange rk_range;
        if (OB_FAIL(rk_range.build_range(brute_ctdef->ref_table_id_, pre_fileter_rowkeys_.at(count)))) {
          LOG_WARN("fail to build key range", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(rk_range, brute_scan_param_, brute_ctdef->ref_table_id_))) {
          LOG_WARN("failed to append scan range", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(do_aux_table_scan(brute_first_scan_,
                                          brute_scan_param_,
                                          brute_ctdef,
                                          brute_rtdef,
                                          brute_iter_,
                                          brute_tablet_id_))) {
        LOG_WARN("fail to rescan brute table scan iterator.", K(ret));
      }
    } else {
      count = search_count;
      if (OB_FAIL(do_table_full_scan(is_vectorized,
                                     brute_ctdef,
                                     brute_rtdef,
                                     brute_iter_,
                                     brute_tablet_id_,
                                     brute_first_scan_,
                                     brute_scan_param_))) {
        LOG_WARN("failed to do centroid table scan", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(brute_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *vec_expr = brute_ctdef->result_output_[DATA_VECTOR_IDX];
        ObDatum *vec_datum = vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString c_vec = vec_datum[i].get_string();
          if (vec_datum[i].is_null()) {
            // do nothing for null vector
          } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                             &mem_context_->get_arena_allocator(),
                             ObLongTextType,
                             CS_TYPE_BINARY,
                             c_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else { // dis for l2
            float distance = 0.0f;
            ObRowkey main_rowkey;
            if (OB_FAIL(calc_vec_dis<float>(reinterpret_cast<float *>(c_vec.ptr()), search_vec, dim_, distance, raw_dis_type))) {
              if (OB_ERR_NULL_VALUE == ret) {
                ret = OB_SUCCESS;
                LOG_TRACE("skip zero-norm vector, cosine distance undefined");
              } else {
                LOG_WARN("fail to calc vec dis", K(ret), K(dim_));
              }
            } else if (OB_FAIL(get_main_rowkey_brute(mem_context_->get_arena_allocator(), brute_ctdef, main_rowkey_cnt, main_rowkey))) {
              LOG_WARN("fail to get main rowkey", K(ret));
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
              LOG_WARN("failed to push center.", K(ret));
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(brute_iter_, brute_scan_param_, brute_tablet_id_)
    } else {
      brute_iter_->clear_evaluated_flag();
      ObExpr *vec_expr = brute_ctdef->result_output_[DATA_VECTOR_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(brute_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (vec_datum.is_null()) {
          // do nothing
        } else {
          ObString c_vec = vec_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                      &mem_context_->get_arena_allocator(),
                      ObLongTextType,
                      CS_TYPE_BINARY,
                      c_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else {
            float distance = 0.0f;
            ObRowkey main_rowkey;
            if (OB_FAIL(calc_vec_dis<float>(reinterpret_cast<float *>(c_vec.ptr()), search_vec, dim_, distance, raw_dis_type))) {
              if (OB_ERR_NULL_VALUE == ret) {
                ret = OB_SUCCESS;
                LOG_TRACE("skip zero-norm vector, cosine distance undefined");
              } else {
                LOG_WARN("fail to calc vec dis", K(ret), K(dim_));
              }
            } else if (OB_FAIL(get_main_rowkey_brute(mem_context_->get_arena_allocator(), brute_ctdef, main_rowkey_cnt, main_rowkey))) {
              LOG_WARN("fail to get main rowkey", K(ret));
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
              LOG_WARN("failed to push center.", K(ret));
            }
          }
        }
      } // end while
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, brute_iter_, brute_scan_param_, brute_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_brute_force_prefilter_rowkeys(
    bool is_vectorized,
    float *search_vec,
    ObExprVectorDistance::ObVecDisType raw_dis_type)
{
  int ret = OB_SUCCESS;
  adaptive_ctx_.is_brute_force_ = true;
  IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_,
                                    search_vec /*unused*/,
                                    raw_dis_type,
                                    dim_,
                                    get_nprobe(limit_param_, 1),
                                    similarity_threshold_);
  if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
    LOG_WARN("failed to get limit rowkey brute", K(ret));
  } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
    LOG_WARN("failed to get top n", K(ret));
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_main_rowkey_brute(
  ObIAllocator &allocator,
  const ObDASScanCtDef *brute_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(brute_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(brute_ctdef));
  } else {
    // brute_iter output: [DATA_VECTOR ROWKEY]
    // Note: when _enable_defensive_check = 2, cid_vec_out_exprs is [DATA_VECTOR ROWKEY DEFENSE_CHECK_COL]
    const ExprFixedArray& brute_out_exprs = brute_ctdef->result_output_;
    if (rowkey_cnt > brute_out_exprs.count() - 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_cnt is illegal", K(ret), K(rowkey_cnt), K(brute_out_exprs.count()));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
    } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
    } else {
      int rowkey_idx = 0;
      for (int64_t i = 1; OB_SUCC(ret) && i < brute_out_exprs.count() && rowkey_idx < rowkey_cnt; ++i) {
        ObObj tmp_obj;
        ObExpr *expr = brute_out_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else if (OB_FALSE_IT(obj_ptr[rowkey_idx++] = tmp_obj)) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      main_rowkey.assign(obj_ptr, rowkey_cnt);
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec, 
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else if (is_adaptive_filter()) {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  } else if (is_iter_filter()) {
    adaptive_ctx_.iter_times_ = 0;
    ObSEArray<ObIvfRowkeyDistEntry, 16> near_rowkeys;
    int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
    int64_t iter_cnt = 0;
    int64_t next_nprobe = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    float* search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
    const int64_t sub_dim = dim_ / m_;
    int64_t enlargement_factor = (selectivity_ != 0 && selectivity_ != 1 && is_iter_filter()) ? POST_ENLARGEMENT_FACTOR : 1;
    int64_t heap_size = get_heap_size(limit_k, selectivity_);
    ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
    IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, cur_dis_type, sub_dim, heap_size, similarity_threshold_);
    ObIVFRowkeyDistMap rowkey_dist_map;
    ObIvfRowkeyDistItemCompare head_cmp(cur_dis_type);
    ObIvfRowkeyDistHeap rowkey_dist_heap(head_cmp);
    if (OB_FAIL(rowkey_dist_map.create(32, lib::ObMemAttr(MTL_ID(), "IVFMap") ))) {
      LOG_WARN("create rowkey dist map fail", K(ret));
    }
    while (OB_SUCC(ret) && ! iter_end && near_rowkeys.count() < limit_k) {
      rowkey_dist_map.reuse();
      ++adaptive_ctx_.iter_times_;
      int32_t start_idx = -1;
      if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
          is_vectorized,
          search_vec,
          nearest_rowkey_heap,
          nullptr))) {
        LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_centers_dist_map(rowkey_dist_map))) {
        LOG_WARN("get dist map fail", K(ret));
      } else if (OB_FALSE_IT(start_idx = near_rowkeys.count())) {
      } else if (OB_FAIL(do_post_filter(is_vectorized, rowkey_dist_map, near_rowkeys))) {
        LOG_WARN("do post filter fail", K(ret), K(near_rowkeys.count()));
      } else if (OB_FALSE_IT(no_new_near_rowkeys = (near_rowkeys.count() == start_idx))) {
        // there are no new rowkeys in near_rowkeys after post filter
      } else {
        for (int64_t i = start_idx; OB_SUCC(ret) && i < near_rowkeys.count(); ++i) {
          const ObIvfRowkeyDistEntry &entry = near_rowkeys.at(i);
          if (OB_FAIL(rowkey_dist_heap.push(ObIvfRowkeyDistItem(i, entry.distance_)))) {
            LOG_WARN("push rowkey dist fail", K(ret), K(i), K(entry));
          } else {
            LOG_TRACE("push", K(entry), K(i), K(limit_k));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (near_rowkeys.count() < limit_k) {
        ++iter_cnt;
        const int64_t left_search = limit_k - near_rowkeys.count();
        next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
        const double select_ratio = double(adaptive_ctx_.iter_res_row_cnt_) /  double(adaptive_ctx_.iter_filter_row_cnt_);
        const int64_t new_heap_size = OB_MAX(get_heap_size(left_search, select_ratio), heap_size);
        LOG_INFO("postfilter does not get enough result", K(limit_k), "near_rowkeys_count", near_rowkeys.count(),
            K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(heap_size),
            K(new_heap_size), K(select_ratio), K(can_retry_), K(strategy_), K(adaptive_ctx_));
        near_cid_vec_.reuse();
        near_cid_vec_dis_.reuse();
        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
          LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(near_rowkeys.count()), K(start_idx));
        } else if (max_scan_vectors_ > 0 && adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
          iter_end = true;
          LOG_INFO("reach max scan vectors, stop iterative filter", K(no_new_near_rowkeys), K(near_rowkeys.count()), 
              K(start_idx), K(limit_k), K(left_search), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
        } else if (can_retry_ && OB_FAIL(check_iter_filter_need_retry())) {
          LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
        } else if (! has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(get_next_centers(is_vectorized, next_nprobe))) {
          LOG_WARN("get next centers failed", K(ret), K(has_used_hgraph_), K(next_nprobe));
        } else if (near_cid_vec_.count() != near_cid_vec_dis_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("near_cid_vec count is not equal to near_cid_vec_dis count", K(ret),
              "near_cid_vec_count", near_cid_vec_.count(), "near_cid_vec_dis_count", near_cid_vec_dis_.count());
        } else if (near_cid_vec_dis_.count() == 0) {
          // Common check: if no centers found after getting next batch, end iteration
          iter_end = true;
          LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
        } else if (OB_FAIL(nearest_rowkey_heap.set_nprobe(new_heap_size))) {
          LOG_WARN("set new heap size fail", K(ret), K(new_heap_size), K(heap_size));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for(int64_t cnt = 0; OB_SUCC(ret) && cnt < limit_k && ! rowkey_dist_heap.empty(); ++cnt) {
        const ObIvfRowkeyDistItem &item = rowkey_dist_heap.top();
        if (item.rowkey_idx_ > near_rowkeys.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out of bound of near_rowkeys", K(ret), K(item), K(near_rowkeys.count()));
        } else if (OB_FAIL(saved_rowkeys_.push_back(near_rowkeys.at(item.rowkey_idx_).rowkey_))) {
          LOG_WARN("push back fail", K(ret), K(item));
        } else if (OB_FAIL(rowkey_dist_heap.pop())) {
          LOG_WARN("rowkey_dist_heap pop fail", K(ret), K(saved_rowkeys_.count()), K(item));
        } else {
          LOG_TRACE("result", K(item), "i", cnt, K(limit_k));
        }
      }
    }
  } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
      is_vectorized,
      reinterpret_cast<float *>(real_search_vec_.ptr()),
      saved_rowkeys_,
      nullptr))) {
    // 2. search nearest rowkeys
    LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
  }

  return ret;
}

// if cid not exist, center_vec is nullptr
int ObDASIvfPQScanIter::check_cid_exist(
    const ObString &src_cid,
    float *&center_vec,
    bool &src_cid_exist)
{
  int ret = OB_SUCCESS;
  src_cid_exist = false;
  ObCenterId src_centor_id;
  center_vec = nullptr;
  if (OB_UNLIKELY(near_cid_vec_ptrs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("near_cid_vec_ptrs_ is empty", K(ret));
  } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else if (OB_UNLIKELY(src_centor_id.center_id_ >= near_cid_vec_ptrs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_cid is not exist", K(ret), K(src_centor_id), K(near_cid_vec_ptrs_.count()));
  } else if (OB_NOT_NULL(near_cid_vec_ptrs_.at(src_centor_id.center_id_))) {
    src_cid_exist = true;
    center_vec = near_cid_vec_ptrs_.at(src_centor_id.center_id_);
  }

  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;

  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  bool is_range_prefilter = vec_aux_ctdef_->can_use_vec_pri_opt();
  ObIvfPreFilter prefilter(MTL_ID());
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec, 
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      // cid_center table is empty, just do brute search
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    }
  } else {
    const int64_t est_output_row_cnt = adaptive_ctx_.selectivity_ * adaptive_ctx_.row_count_;
    const bool need_check_brute = est_output_row_cnt <= IVF_MAX_BRUTE_FORCE_SIZE * 10 || ! is_range_prefilter;
    if (need_check_brute && OB_FAIL(get_rowkey_pre_filter(mem_context_->get_arena_allocator(), is_vectorized, IVF_MAX_BRUTE_FORCE_SIZE))) {
      LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
    } else if (need_check_brute && pre_fileter_rowkeys_.count() < IVF_MAX_BRUTE_FORCE_SIZE) {
      if (OB_FAIL(do_brute_force_prefilter_rowkeys(is_vectorized, search_vec, raw_dis_type))) {
        LOG_WARN("failed to do brute force prefilter rowkeys", K(ret));
      }
    } else { // search with prefilter
      if (is_range_prefilter) { // rowkey range prefilter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        adaptive_ctx_.is_range_prefilter_ = true;
        ObArray<const ObNewRange *> rk_range;
        const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
        for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
          const ObNewRange *range = &key_range.at(i);
          if (OB_FAIL(rk_range.push_back(range))) {
            LOG_WARN("fail to push back range", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prefilter.init(rk_range))) {
            LOG_WARN("fail to init prefilter as range filter", K(ret));
          }
        }
      } else if (OB_FAIL(prefilter.init())) { // rowkey hash bitmap prefilter
        LOG_WARN("fail to init prefilter as roaring bitmap", K(ret));
      } else { // add bitmap for bitmap filter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        for (int i = 0; i < pre_fileter_rowkeys_.count() && OB_SUCC(ret); i++) {
          uint64_t hash_val = hash_val_for_rk(pre_fileter_rowkeys_.at(i));
          if (OB_FAIL(prefilter.add(hash_val))) {
            LOG_WARN("fail to add hash val to prefilter", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(build_rowkey_hash_set(prefilter, is_vectorized, batch_row_count))) {
          LOG_WARN("fail to build rk hash set", K(ret));
        }
      }
      // do for loop until saved_rowkeys_.count() >= limitK
      if (OB_SUCC(ret)) {
        int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
        int64_t iter_cnt = 0;
        int64_t next_nprobe = 0;
        bool iter_end = false;
        bool is_first_scan = true;
        const int64_t sub_dim = dim_ / m_;
        int32_t start_idx = -1;
        bool no_new_near_rowkeys = false;
        ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
        IvfRowkeyHeap nearest_rowkey_heap(
            vec_op_alloc_, reinterpret_cast<float *>(real_search_vec_.ptr()), cur_dis_type, sub_dim, limit_k, similarity_threshold_);
        while (OB_SUCC(ret) && ! iter_end && nearest_rowkey_heap.count() < limit_k) {
          if (OB_FALSE_IT(start_idx = nearest_rowkey_heap.count())) {
          } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
              is_vectorized,
              reinterpret_cast<float *>(real_search_vec_.ptr()),
              nearest_rowkey_heap,
              &prefilter))) {
            LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
          } else if (OB_FALSE_IT(no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx))) {
            // there are no new rowkeys in near_rowkeys after post filter
          }
          if (OB_FAIL(ret)) {
          } else if (nearest_rowkey_heap.count() < limit_k) {
            ++iter_cnt;
            const int64_t left_search = limit_k - nearest_rowkey_heap.count();
            next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
            LOG_INFO("prefilter does not get enough result", K(limit_k), "nearest_rowkey_heap_count", nearest_rowkey_heap.count(),
                K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(can_retry_), K(strategy_), K(adaptive_ctx_));
            near_cid_vec_.reuse();
            near_cid_vec_dis_.reuse();
            is_first_scan = false;
            if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
                iter_end = true;
                LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), K(start_idx), K(limit_k));
            } else if (max_scan_vectors_ > 0 && adaptive_ctx_.cid_vec_scan_rows_ > max_scan_vectors_) {
              iter_end = true;
              LOG_INFO("reach max scan vectors, stop iterative filter", K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), 
                K(start_idx), K(limit_k), K(left_search), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
            } else if (! has_next_center()) {
              iter_end = true;
            } else if (OB_FAIL(get_next_centers(is_vectorized, next_nprobe))) {
              LOG_WARN("get next centers failed", K(ret), K(has_used_hgraph_), K(next_nprobe));
            } else if (near_cid_vec_.count() != near_cid_vec_dis_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("near_cid_vec count is not equal to near_cid_vec_dis count", K(ret),
                "near_cid_vec_count", near_cid_vec_.count(), "near_cid_vec_dis_count", near_cid_vec_dis_.count());
            } else if (near_cid_vec_dis_.count() == 0) {
              // Common check: if no centers found after getting next batch, end iteration
              iter_end = true;
              LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("get rowkeys from heap fail", K(ret), K(limit_k));
        } else {
          LOG_TRACE("get rowkeys from heap success", K(limit_k), K(saved_rowkeys_.count()), K(iter_cnt), K(next_nprobe), K(nprobes_));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::try_write_pq_centroid_cache(
    ObIvfICache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already writed centroids cache, skip", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    const ObDASScanCtDef *pq_cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_pq_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
    ObDASScanRtDef *pq_cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_pq_id_tbl_idx());
    ObPqCenterId pq_cent_id;
    if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
    } else if (OB_FAIL(do_table_full_scan(is_vectorized,
                                    pq_cid_vec_ctdef,
                                    pq_cid_vec_rtdef,
                                    pq_centroid_iter_,
                                    pq_centroid_tablet_id_,
                                    pq_centroid_first_scan_,
                                    pq_centroid_scan_param_))) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(pq_centroid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *cid_expr = pq_cid_vec_ctdef->result_output_[CID_IDX];
        ObExpr *cid_vec_expr = pq_cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
        ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        uint64_t center_idx = 0;

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString cid = cid_datum[i].get_string();
          ObString cid_vec = cid_vec_datum[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &tmp_allocator,
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
            ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_pq_centroid(
                pq_cent_id.m_id_, pq_cent_id.center_id_, reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(pq_cent_id), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_)
    } else {
      pq_centroid_iter_->clear_evaluated_flag();
      ObExpr *cid_expr = pq_cid_vec_ctdef->result_output_[CID_IDX];
      ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      for (int i = 0; OB_SUCC(ret); ++i) {
        if (OB_FAIL(pq_centroid_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else {
          ObString cid_vec = vec_datum.get_string();
          ObString cid = cid_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
            ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_pq_centroid(
                pq_cent_id.m_id_, pq_cent_id.center_id_, reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(pq_cent_id), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (cent_cache.get_count() > 0) {
        cent_cache.set_completed();
        LOG_DEBUG("success to write centroid table cache", K(pq_centroid_tablet_id_), K(cent_cache.get_count()));
      } else {
        cent_cache.reuse();
        LOG_DEBUG("Empty centroid table, no need to set cache", K(pq_centroid_tablet_id_));
      }
      
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }
  
  return ret;
}

int ObDASIvfPQScanIter::get_pq_precomputetable_cache(
    bool is_vectorized, 
    ObIvfCacheMgrGuard &cache_guard,
    ObIvfICache *&cent_cache, 
    bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  // pq/flat both use centroid_tablet_id_
  if (OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
        ls_id_, ObIvfCacheMgrKey(centroid_tablet_id_), vec_index_param_, dim_, centroid_ctdef->ref_table_id_, cache_guard))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      LOG_INFO("cache mgr not exist, skip", K(ret), K(ls_id_), K(centroid_tablet_id_));
      is_cache_usable = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ObPluginVectorIndexAdapter",
        K(ret), K(ls_id_), K(centroid_tablet_id_), K(vec_index_param_));
    }
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node(IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret));
    if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      is_cache_usable = false;
      ret = OB_SUCCESS;
    }
  } else if (!cent_cache->is_completed()) {
    if (cent_cache->set_writing_if_idle()) {
      DEBUG_SYNC(IVF_PQ_PRECOMPUTE_CACHE_BEFORE_BUILD);
      common::ScopedTimer build_timer(
          common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_BUILD_TIME,
          profile_);
      int tmp_ret = try_write_pq_precompute_table_cache(*cent_cache, is_vectorized);
      if (OB_TMP_FAIL(tmp_ret)) {
        if (tmp_ret != OB_EAGAIN) {
          LOG_WARN("fail to try write centroid cache", K(tmp_ret), K(is_vectorized), KPC(cent_cache));
        } else {
          is_cache_usable = false;
        }
      } else {
        is_cache_usable = cent_cache->is_completed();
      }
    } else {
      LOG_INFO("other threads already writed pq precompute table cache, skip", K(ret));
    }
  } else {
    // read cache
    is_cache_usable = true;
  }
  return ret;
}

int ObDASIvfPQScanIter::try_write_pq_precompute_table_cache(
    ObIvfICache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  // write cache
  ObIvfCacheMgrGuard pq_center_guard;
  ObIvfICache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;
  ObIvfCacheMgrGuard center_guard;
  ObIvfICache *ivf_cent_cache = nullptr;
  bool is_ivf_cent_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
  } else if (OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq_centers*/, center_guard, ivf_cent_cache, is_ivf_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(ivf_cent_cache));
  } else {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    if (!cent_cache.is_writing()) {
      LOG_INFO("other threads already writed centroids cache, skip", K(ret));
    } else {
      if (is_pq_cent_cache_usable && is_ivf_cent_cache_usable) {
        
          ObArenaAllocator tmp_allocator;
          ObVsagSearchAlloc hgraph_allocator(MTL_ID());
          int64_t ksub = 1L << nbits_;
          int64_t sub_dim = dim_ / m_;
          float *ivf_centers = nullptr;
          float *pq_center = pq_cent_cache->get_centroids(); // m * ksub * sub_dim
          float *precompute_table = cent_cache.get_centroids();
          float *r_norms = (float*)tmp_allocator.alloc(sizeof(float) * m_ * ksub);
          if (ivf_cent_cache->has_hgraph_index()) {
            if (OB_FAIL(ivf_cent_cache->get_centroids_from_hgraph(&hgraph_allocator, &ivf_centers))) {
              LOG_WARN("fail to get ivf centers", K(ret));
            }
          } else {
            ivf_centers = ivf_cent_cache->get_centroids();
          }

          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(r_norms)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc norms", K(ret), K(m_), K(ksub));
          } else {
            // compute norms
            for (int i = 0; i < m_; i++) {
              for (int j = 0; j < ksub; j++) {
                float* pq_cid_vec = pq_center + (i * ksub + j) * sub_dim;
                r_norms[i * ksub + j] = ObVectorL2Distance<float>::l2_norm_square(pq_cid_vec, sub_dim);
              }
            }
            // nlist
            for (int i = 0; i < ivf_cent_cache->get_capacity(); i++) {
              float* ivf_center = ivf_centers + i * dim_;
              float* tab = &precompute_table[i * m_ * ksub];
              for (int j = 0; j < m_; j++) {
                ObVectorIpDistance<float>::fvec_inner_products_ny(tab + j * ksub,
                                                                  ivf_center + j * sub_dim,
                                                                  pq_center + j * ksub * sub_dim,
                                                                  sub_dim,
                                                                  ksub);
              }
              ObVectorL2Distance<float>::fvec_madd(m_ * ksub, r_norms, 2.0, tab, tab);
            }
          }
        if (OB_SUCC(ret)) {
          cent_cache.set_completed();
        }
        if (OB_FAIL(ret)) {
          cent_cache.reuse();
        }
      } else {
        ret = OB_EAGAIN;
        cent_cache.reuse();
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::pre_compute_inner_prod_table(
    const float* search_vec,
    float* dis_table,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t ksub = 1L << nbits_;
  int64_t sub_dim = dim_ / m_;
  // write cache
  ObIvfCacheMgrGuard pq_center_guard;
  ObIvfICache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
  } else if (is_pq_cent_cache_usable) {
    float *pq_center = pq_cent_cache->get_centroids();
    for (int i = 0; i < m_; i++) {
      ObVectorIpDistance<float>::fvec_inner_products_ny(dis_table + i * ksub,
                                                        search_vec + i * sub_dim,
                                                        pq_center + i * ksub * sub_dim,
                                                        sub_dim,
                                                        ksub);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get pq center cache", K(ret));
  }
  return ret;
}

void ObDASIvfPQScanIter::reuse_cid_ctx()
{
  ObDASIvfBaseScanIter::reuse_cid_ctx();
  near_cid_vec_.reuse();
  near_cid_vec_dis_.reuse();
}

uint64_t ObDASIvfBaseScanIter::hash_val_for_rk(const common::ObRowkey& rk)
{
  uint64_t hash_val = 0;
  if (rk.get_obj_cnt() == 1 && ob_is_int_uint_tc(rk.get_obj_ptr()[0].get_type())) {
    hash_val = rk.get_obj_ptr()[0].get_uint64();
  } else {
    for (int i = 0; i < rk.get_obj_cnt(); i++) {
      (void)rk.get_obj_ptr()[i].hash(hash_val, hash_val);
    }
  }
  return hash_val;
}

int ObDASIvfBaseScanIter::build_rowkey_hash_set(
  ObIvfPreFilter &prefilter,
  bool is_vectorized,
  int64_t batch_row_count)
{
  int ret = OB_SUCCESS;
  const int64_t row_count_before = adaptive_ctx_.pre_scan_row_cnt_;
  common::ObOpProfile<common::ObMetric> *vector_iter_profile =
      get_current_vector_iter_profile();
  common::ScopedTimer pre_filter_timer(common::ObMetricId::VEC_PRE_FILTER_TIME,
                                       vector_iter_profile);
  bool index_end = false;
  // alloc one RowKey
  ObArenaAllocator& allocator = mem_context_->get_arena_allocator();
  int64_t rowkey_cnt = 0;
  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(),
                                                                        ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx());
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FALSE_IT(rowkey_cnt = ctdef->rowkey_exprs_.count())) {
  } else if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "IVFRBTMP"));
    const bool is_one_int_rowkey = rowkey_cnt == 1 && ob_is_int_uint_tc(ctdef->rowkey_exprs_.at(0)->obj_meta_.get_type());
    if (!is_vectorized) {
      while (OB_SUCC(ret) && !index_end) {
        inv_idx_scan_iter_->clear_evaluated_flag();
        if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
          index_end = true;
        } else {
          uint64_t hash_val = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
            ObObj tmp_obj;
            ObExpr *expr = ctdef->rowkey_exprs_.at(i);
            ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
            if (OB_ISNULL(datum.ptr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get col datum null", K(ret));
            } else if (is_one_int_rowkey) {
              hash_val = datum.get_uint64();
            } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
              LOG_WARN("convert datum to obj failed", K(ret));
            } else if (OB_FAIL(tmp_obj.hash(hash_val, hash_val))) {
              LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(prefilter.add(hash_val))) {
              LOG_WARN("fail to add hash val to prefilter", K(ret));
            } else {
              adaptive_ctx_.pre_scan_row_cnt_ ++;
            }
          }
        }
        if (OB_SUCC(ret) && can_retry_ && OB_FAIL(check_pre_filter_need_retry())) {
          LOG_WARN("ret of check pre filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_), K(strategy_));
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    } else {
      while (OB_SUCC(ret) && !index_end) {
        int64_t scan_row_cnt = 0;
        if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row.", K(ret));
          }
          index_end = true;
        }
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
        } else if (scan_row_cnt > 0) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          adaptive_ctx_.pre_scan_row_cnt_ += scan_row_cnt;
          for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
            guard.set_batch_idx(i);
            // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
            uint64_t hash_val = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_cnt; ++j) {
              ObObj tmp_obj;
              ObExpr *expr = ctdef->rowkey_exprs_.at(j);
              ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
              if (OB_ISNULL(datum.ptr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get col datum null", K(ret));
              } else if (is_one_int_rowkey) {
                hash_val = datum.get_uint64();
              } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
                LOG_WARN("convert datum to obj failed", K(ret));
              } else if (OB_FAIL(tmp_obj.hash(hash_val, hash_val))) {
                LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(prefilter.add(hash_val))) {
                LOG_WARN("fail to add hash val to prefilter", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && can_retry_ && OB_FAIL(check_pre_filter_need_retry())) {
          LOG_WARN("ret of check pre filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }
  const int64_t scanned_rows = adaptive_ctx_.pre_scan_row_cnt_ - row_count_before;
  if (OB_SUCC(ret) && scanned_rows > 0 && OB_NOT_NULL(vector_iter_profile)) {
    common::ObProfileSwitcher vector_iter_switcher(vector_iter_profile);
    INC_METRIC_VAL(common::ObMetricId::VEC_PRE_FILTER_ROWS_SCANNED, scanned_rows);
  }
  return ret;
}

/********************************************************************************************************/

int ObDASIvfSQ8ScanIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfScanIter::inner_init(param);
  if (OB_SUCC(ret)) {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    sq_meta_iter_ = ivf_scan_param.sq_meta_iter_;
  }

  return ret;
}

int ObDASIvfSQ8ScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sq_meta_iter_) && OB_FAIL(release_input_iter(sq_meta_iter_))) {
    LOG_WARN("failed to release inv_idx_scan_iter_", K(ret));
  }
  sq_meta_iter_ = nullptr;
  ObDasVecScanUtils::release_scan_param(sq_meta_scan_param_);
  if (OB_SUCC(ret) && OB_FAIL(ObDASIvfScanIter::inner_release())) {
    LOG_WARN("fail to do ObDASIvfScanIter::inner_release", K(ret));
  }

  return ret;
}

int ObDASIvfSQ8ScanIter::get_real_search_vec_u8(bool is_vectorized, ObString &real_search_vec_u8)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *sq_meta_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *sq_meta_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx());
  ObString min_vec;
  ObString step_vec;
  if (OB_FAIL(do_table_full_scan(is_vectorized,
                                 sq_meta_ctdef,
                                 sq_meta_rtdef,
                                 sq_meta_iter_,
                                 sq_meta_tablet_id_,
                                 sq_meta_iter_first_scan_,
                                 sq_meta_scan_param_))) {
    LOG_WARN("failed to do table scan sq_meta", K(ret));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(sq_meta_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *meta_vec_expr = sq_meta_ctdef->result_output_[META_VECTOR_IDX];
        ObDatum *meta_vec_datum = meta_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          if (i == ObIvfConstant::SQ8_META_MIN_IDX || i == ObIvfConstant::SQ8_META_STEP_IDX) {
            ObString c_vec = meta_vec_datum[i].get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(
                    mem_context_->get_arena_allocator(),
                    meta_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_),
                    sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->datum_meta_,
                    c_vec))) {
              LOG_WARN("failed to get real data.", K(ret));
            } else if (i == ObIvfConstant::SQ8_META_MIN_IDX) {
              // if not has lob, need deepcopy, because datum_row whill reuse
              min_vec = c_vec;
            } else if (i == ObIvfConstant::SQ8_META_STEP_IDX) {
              step_vec = c_vec;
            }
          }
        }
      }
    IVF_GET_NEXT_ROWS_END(sq_meta_iter_, sq_meta_scan_param_, sq_meta_tablet_id_)
  } else {
    // get min_vec max_vec step_vec in sq_meta table
    sq_meta_iter_->clear_evaluated_flag();
    ObExpr *meta_vec_expr = sq_meta_ctdef->result_output_[META_VECTOR_IDX];
    ObDatum &meta_vec_datum = meta_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    int row_index = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sq_meta_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else {
        ObString c_vec = meta_vec_datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(
                mem_context_->get_arena_allocator(),
                meta_vec_datum,
                sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->datum_meta_,
                c_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (row_index == ObIvfConstant::SQ8_META_MIN_IDX) {
          // if not has lob, need deepcopy, because datum_row whill reuse
          min_vec = c_vec;
        } else if (row_index == ObIvfConstant::SQ8_META_STEP_IDX) {
          step_vec = c_vec;
        }
      }
      row_index++;
    }
    if (ret == OB_ITER_END) {
      if (OB_FAIL(sq_meta_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    uint8_t *res_vec = nullptr;
    if (OB_ISNULL(min_vec.ptr()) || OB_ISNULL(step_vec.ptr())) {
      if (OB_ISNULL(res_vec = reinterpret_cast<uint8_t *>(mem_context_->get_arena_allocator().alloc(sizeof(uint8_t) * dim_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(sizeof(uint8_t) * dim_));
      } else {
        MEMSET(res_vec, 0, sizeof(uint8_t) * dim_);
      }
    } else if (OB_FAIL(
                    ObExprVecIVFSQ8DataVector::cal_u8_data_vector(mem_context_->get_arena_allocator(),
                                                                  dim_,
                                                                  reinterpret_cast<float *>(min_vec.ptr()),
                                                                  reinterpret_cast<float *>(step_vec.ptr()),
                                                                  reinterpret_cast<float *>(real_search_vec_.ptr()),
                                                                  res_vec))) {
      LOG_WARN("fail to cal u8 data vector", K(ret), K(dim_));
    }
    if (OB_SUCC(ret)) {
      real_search_vec_u8.assign_ptr(reinterpret_cast<char *>(res_vec), dim_ * sizeof(uint8_t));
    }
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObString real_search_vec_u8;
  if (OB_FAIL(get_real_search_vec_u8(is_vectorized, real_search_vec_u8))) {
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(do_ivf_scan_post<uint8_t>(is_vectorized, reinterpret_cast<uint8_t *>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to do post filter", K(ret), K(is_vectorized));
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObString real_search_vec_u8;
  if (OB_FAIL(get_real_search_vec_u8(is_vectorized, real_search_vec_u8))) {
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(do_ivf_scan_pre<uint8_t>(allocator, is_vectorized, reinterpret_cast<uint8_t*>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  }
  return ret;
}

// ObIvfPreFilter
void ObIvfPreFilter::reset()
{
  // release memory
  if (OB_NOT_NULL(roaring_bitmap_)) {
    if (type_ == FilterType::ROARING_BITMAP) {
      roaring::api::roaring64_bitmap_free(roaring_bitmap_);
    }
  }
  // reset members
  type_ = FilterType::ROARING_BITMAP;
  roaring_bitmap_ = nullptr;
  rk_range_.reset();
}

int ObIvfPreFilter::init()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(roaring_bitmap_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ROARING_TRY_CATCH(roaring_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_SUCC(ret)) {
      type_ = FilterType::ROARING_BITMAP;
    }
  }
  return ret;
}

int ObIvfPreFilter::init(const ObIArray<const ObNewRange*> &range)
{
  int ret = OB_SUCCESS;
  if (rk_range_.count() != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(rk_range_));
  } else if (range.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rk range", K(ret), K(range));
  } else if (OB_FAIL(rk_range_.assign(range))) {
    LOG_WARN("fail to assign rt st", K(ret));
  } else {
    type_ = FilterType::SIMPLE_RANGE;
  }
  return ret;
}

int ObIvfPreFilter::add(int64_t id)
{
  int ret = OB_SUCCESS;
  if (type_ == FilterType::ROARING_BITMAP) {
    ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(roaring_bitmap_, id));
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("simple range not support add", K(ret));
  }
  return ret;
}

bool ObIvfPreFilter::test(const ObRowkey& main_rowkey)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (type_ == FilterType::ROARING_BITMAP) {
    uint64_t hash_val = 0;
    if (main_rowkey.get_obj_cnt() == 1 && ob_is_int_uint_tc(main_rowkey.get_obj_ptr()[0].get_type())) {
      hash_val = main_rowkey.get_obj_ptr()[0].get_uint64();
    } else {
      for (int i = 0; i < main_rowkey.get_obj_cnt(); i++) {
        (void)main_rowkey.get_obj_ptr()[i].hash(hash_val, hash_val);
      }
    }
    bret = roaring::api::roaring64_bitmap_contains(roaring_bitmap_, hash_val);
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ObNewRange tmp_range;
    if (OB_FAIL(tmp_range.build_range(rk_range_.at(0)->table_id_, main_rowkey))) {
      LOG_WARN("fail to build tmp range", K(ret));
    }
    // do compare
    for (int64_t i = 0; i < rk_range_.count() && !bret && OB_SUCC(ret); i++) {
      if (rk_range_.at(i)->compare_with_startkey2(tmp_range) <= 0 && rk_range_.at(i)->compare_with_endkey2(tmp_range) >= 0) {
        bret = true;
      }
    }
  }
  return bret;
}

void ObIvfIterativeFilterContext::reset()
{
  visited_center_cnt_ = 0;
  centroids_.reset();
  allocator_.reset();
}

void ObIvfIterativeFilterContext::reuse()
{
  visited_center_cnt_ = 0;
  centroids_.reuse();
  allocator_.reuse();
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_center_ids(const int64_t nprobe, ObIArray<ObCenterId> &center_ids)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      if (OB_FAIL(center_ids.push_back(cur.id_))) {
        LOG_WARN("failed to push center id", K(ret), K(i), K(cur));
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_center_ids_dist(const int64_t nprobe, ObArrayWrap<bool> &nearest_cid_dist)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      const ObCenterId &center_id = cur.id_;
      if (OB_UNLIKELY(center_id.center_id_ >= nearest_cid_dist.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("center_id is not less than nearest_cid_dist", K(ret), K(center_id), K(nearest_cid_dist.count()));
      } else {
        nearest_cid_dist.at(center_id.center_id_) = true;
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_centers_vec_dist(const int64_t nprobe, ObIArray<std::pair<ObCenterId, float *>> &center_ids, ObIArray<float> &distances)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      if (OB_FAIL(center_ids.push_back(std::make_pair(cur.id_, (float *)cur.vec_)))) {
        LOG_WARN("failed to push center id", K(ret), K(i), K(cur));
      } else if (OB_FAIL(distances.push_back(cur.distance_))) {
        LOG_WARN("failed to push distance", K(ret), K(i), K(cur));
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

bool ObIvfIterativeFilterContext::has_next_center() const
{
  LOG_TRACE("next info", K_(visited_center_cnt), "center_count", centroids_.count());
  return visited_center_cnt_ < centroids_.count();
}

}  // namespace sql
}  // namespace oceanbase
