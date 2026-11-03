/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "ob_vector_index_freeze_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "storage/ls/ob_ls.h"
#include "storage/vector_index/ob_vector_index_refresh.h"
#include "storage/async_index/ob_async_index_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
namespace share
{
ERRSIM_POINT_DEF(ERRSIM_VEC_DISABLE_TUNE_INDEX_IN_FREEZE, "When set, vector index tune index is disabled in freeze task.");
ERRSIM_POINT_DEF(ERRSIM_VEC_FREEZE_AFTER_ACTIVE_ROTATE,
    "Inject an error after vector index active segment rotation.");

int ObVecIdxFreezeTaskExecutor::do_check_task_result(
    ObVecIndexAsyncTaskCtx *task_ctx,
    ObVecTaskResultCheckAction &action)
{
  return do_check_normal_running_task_result(task_ctx, action);
}

bool ObVecIdxFreezeTaskExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  const bool is_not_support = ! ObPluginVectorIndexHelper::enable_persist_vector_index_incremental(tenant_id_);
  if (is_not_support) {
    bret = false;
    LOG_DEBUG("skip this round, not support async task.");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_5_1_0) {
    bret = false;
    LOG_DEBUG("vector index async task can not work with data version less than 4_5_0_0", K(tenant_data_version));
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_active_segment_max_size(tenant_id_, max_active_segment_size_))) {
    LOG_WARN("get active segment max size fail", K(ret), K(tenant_id_));
  }
  return bret;
}

int ObVecIdxFreezeTaskExecutor::load_triggered_task(const ObVecIndexTaskStatus &task_row)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index_ls_mgr", K(ret));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    ObVecIndexTaskStatus task_row_copy = task_row;
    task_row_copy.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::try_add_task_ctx_from_inner_row(
            tenant_id_, task_row_copy, ls, ls_handle_, *index_ls_mgr, true /*verify_tablet_on_ls*/))) {
      LOG_WARN("fail to add triggered freeze task ctx", K(ret), K(task_row));
    }
  }
  return ret;
}

int ObVecIdxFreezeTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObVecIndexSchemaChecker schema_checker(tenant_id_, schema_guard);
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_NOT_NULL(index_ls_mgr) && index_ls_mgr->get_async_task_opt().is_stop()) {
    // LS is being destroyed; skip creating new tasks.  Cancellation of existing
    // tasks is handled by check_and_schedule_* later in the same tick.
    LOG_INFO("[VEC_ASYNC_TASK] ls is stopping, skip load task", K(tenant_id_),
             K(ls_handle_.get_ls()->get_ls_id()));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    common::hash::ObHashSet<uint64_t> dropping_index_ids;
    if (OB_FAIL(dropping_index_ids.create(16))) {
      LOG_WARN("fail to create dropping index set", KR(ret), K_(tenant_id), K_(ls_handle));
    } else if (OB_FAIL(get_dropping_index_ids(tenant_id_, dropping_index_ids))) {
    }
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    ObTenantVectorAllocator& vector_allocator = shared_mem_mgr->vector_allocator();
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);
    ObSEArray<ObTabletID, 64> adapter_tablet_ids;
    if (OB_SUCC(ret) && OB_FAIL(index_ls_mgr->collect_adaptor_tablet_ids(adapter_tablet_ids))) {
      LOG_WARN("fail to collect task adapter tablets", KR(ret), K_(tenant_id), K_(ls_handle));
    }

    // 冻结持久化不会引入额外内存消耗, 这里主要是根据租户剩余内存, 判断是否需要提前冻结，避免合并时内存不足
    // 对与HNSW主要是避免Segment过大, 导致写入变慢
    int64_t avail_merge_mem = 1 * 1024 * 1024 * 1024L;  // default 1GB
    int64_t freeze_segment_size = 128 * 1024 * 1024; // default 128MB
    int64_t total_pre_alloc_size = 0;
    int64_t freeze_threshold = 0;
    for (int64_t adapter_idx = 0; OB_SUCC(ret) && adapter_idx < adapter_tablet_ids.count()
         && task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT; ++adapter_idx) {
      const ObTabletID adapter_tablet_id = adapter_tablet_ids.at(adapter_idx);
      ObPluginVectorIndexAdapterGuard adapter_guard;
      if (OB_FAIL(index_ls_mgr->get_adapter_inst_guard(adapter_tablet_id, adapter_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get task adapter", KR(ret), K_(tenant_id), K(adapter_tablet_id));
        }
        continue;
      }
      ObPluginVectorIndexAdaptor *adapter = adapter_guard.get_adatper();
      ObTabletID tablet_id = adapter_tablet_id;
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        int64_t new_task_id = OB_INVALID_ID;
        int64_t index_table_id = OB_INVALID_ID;
        bool inc_new_task = false;
        bool task_ctx_in_map = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = nullptr;
        ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
        bool need_freeze = false;
        bool schema_active = false;
        int64_t inc_mem_size = 0;
        if (! adapter->is_complete()) {
          LOG_TRACE("adapter not complete, no need freeze", K(ret), KPC(adapter));
        } else if (!adapter->is_ready_complete()) {
          LOG_INFO("adapter is not complete, no need freeze", K(ret), KPC(adapter));
        } else if (adapter->is_sparse_vector_index_type()) {
          LOG_TRACE("adapter is sparse vector index, no need merge", K(ret), KPC(adapter));
        } else if (OB_HASH_EXIST == dropping_index_ids.exist_refactored(adapter->get_inc_table_id())) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("[VEC_DDL_SKIP] skip freeze generation for dropping index",
                     K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          } else {
            LOG_DEBUG("[VEC_DDL_SKIP] skip freeze generation for dropping index",
                      K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          }
        } else if (OB_FALSE_IT(inc_mem_size = adapter->get_incr_vsag_mem_used())) {
        } else if (inc_mem_size <= 0) {
          LOG_TRACE("incr vsag mem used is 0, no need to freeze", K(ret), K(inc_mem_size));
        } else if (OB_FAIL(calc_freeze_threshold(vector_allocator, inc_mem_size, total_pre_alloc_size, freeze_threshold))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            LOG_WARN("fail to calc freeze threshold", K(ret));
          } else {
            // TODO zhichen
            ret = OB_SUCCESS;
            LOG_INFO("left memory may be not enough, let it go...", K(vector_allocator.get_left_pre_alloc_size()));
          }
        } else if (OB_FAIL(adapter->check_need_freeze(freeze_threshold, need_freeze))) {
          LOG_WARN("failed to check need freeze", K(ret), KPC(adapter));
        } else if (! need_freeze && ! adapter->has_frozen()) {
          LOG_TRACE("active segment is not need to freeze", K(ret), KPC(adapter));
        } else if (adapter->is_frozen_finish()) {
          LOG_INFO("frozen data is already finished, no need to freeze", K(ret), KPC(adapter));
        } else if (OB_FAIL(schema_checker.check_table(adapter->get_inc_table_id(), schema_active))) {
          LOG_WARN("fail to check freeze task schema", KR(ret), K(tablet_id));
        } else if (!schema_active) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("[VEC_SCHEMA_SKIP] skip freeze task for inactive schema",
                     K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          } else {
            LOG_DEBUG("[VEC_SCHEMA_SKIP] skip freeze task for inactive schema",
                      K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          }
        } else if (OB_NOT_NULL(scheduler_) && scheduler_->is_task_disabled(
                       ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_FREEZE, tablet_id.id())) {
          LOG_TRACE("[VEC_IDX_TASK_DISABLED] skip generating disabled task",
                    K_(tenant_id), K(tablet_id), "task_type", "FREEZE");
        } else if (OB_ISNULL(task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(adapter, tablet_id, index_table_id))) { // only get table 3 table_id to generate new task
          LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
        } else if (OB_INVALID_ID == index_table_id) {
          LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_INFO("[VEC_ASYNC_TASK] task loaded with PREPARE status",
                   K(ret), K(tablet_id), K(tenant_id_), K(new_trace_id), K(new_task_id),
                   K(task_trace_base_num), K(ls->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_handle_ = ls_handle_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_FREEZE;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          if (OB_FAIL(try_add_task_ctx_for_adapter(
                       *index_ls_mgr, adapter_tablet_id, adapter, task_ctx, inc_new_task))) {
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          }
        }
        if (OB_NOT_NULL(task_ctx) && (!OB_SUCC(ret) || !inc_new_task)) { // release memory when fail
          if (task_ctx_in_map) {
            int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(index_ls_mgr->get_async_task_opt(), task_ctx);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("fail to clear task ctx after load task failed", K(tmp_ret), K(ret), K(task_ctx));
            }
          } else {
            task_ctx->~ObVecIndexAsyncTaskCtx();
            allocator->free(task_ctx); // arena need free
          }
          task_ctx = nullptr;
        }
      }
    }
    if (task_ctx_array.count() > 0) {
      LOG_INFO("finish load async task", K(ret), K(ls->get_ls_id()), K(task_ctx_array.count()), K(current_task_cnt));
    }
    vector_allocator.pre_free(total_pre_alloc_size);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_handle_));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObVecIdxFreezeTaskExecutor::calc_freeze_threshold(
    ObTenantVectorAllocator& vector_allocator, 
    const int64_t inc_mem_size,
    int64_t &total_pre_alloc_size, 
    int64_t &freeze_threshold)
{
  int ret = OB_SUCCESS;
  const int64_t left_size = vector_allocator.get_left_pre_alloc_size();
  if (inc_mem_size >= left_size) {
    freeze_threshold = inc_mem_size;
  } else {
    freeze_threshold = max_active_segment_size_;
  }
  if (OB_FAIL(vector_allocator.pre_alloc(inc_mem_size))) {
    LOG_TRACE("fail to pre alloc", K(ret), K(inc_mem_size));
  } else {
    total_pre_alloc_size += inc_mem_size;
    LOG_TRACE("calc freeze threshold success", K(ret), K(inc_mem_size), K(freeze_threshold), K(left_size), K(total_pre_alloc_size));
  }
  return ret;
}

int ObVecIdxFreezeTask::do_work()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_VECTOR_MID);
  ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
  LOG_INFO("[VEC_ASYNC_TASK] start freeze do_work", K(ret), K(ctx_->task_status_), K(ls_id_));
  DEBUG_SYNC(HANDLE_VECTOR_INDEX_FREEZE_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_FREEZE) {
    if (OB_FAIL(process_freeze())) {
      LOG_WARN("fail to process freeze", K(ret), KPC(ctx_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", K(ret), KPC(ctx_));
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }
  LOG_INFO("[VEC_ASYNC_TASK] end freeze do_work", K(ret), K(ctx_->task_status_));
  return ret;
}

bool ObVecIdxFreezeTask::is_alter_param_child() const
{
  return OB_NOT_NULL(ctx_)
      && ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_FREEZE
      && ctx_->task_status_.trigger_type_ == OB_VEC_TRIGGER_ALTER_INDEX_PARAM
      && ctx_->task_status_.task_info_.alter_param_info_.is_child_valid();
}

int ObVecIdxFreezeTask::prepare_alter_param_freeze_(
    ObPluginVectorIndexAdaptor &adaptor,
    bool &need_rotate,
    const ObVectorIndexParam &target_param)
{
  int ret = OB_SUCCESS;
  need_rotate = false;
  const ObVecIdxActiveDataHandle &active = adaptor.get_incr_data();
  bool active_equal = false;
  if (!active.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("active segment is invalid", KR(ret), KPC(&adaptor));
  } else {
    TCRLockGuard lock_guard(active->mem_data_rwlock_);
    if (!active->is_inited()) {
      need_rotate = true;
    } else if (!active->segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("active segment handle is invalid", KR(ret), KPC(&adaptor));
    } else if (OB_FAIL(ObVectorIndexUtil::check_build_params_equal(
            active->segment_handle_->get_index_param(), target_param, active_equal))) {
      LOG_WARN("fail to compare active segment params", KR(ret), KPC(&adaptor));
    } else {
      need_rotate = !active_equal;
    }
  }
  return ret;
}

int ObVecIdxFreezeTask::try_handle_empty_frozen_segment_(
    ObPluginVectorIndexAdaptor &adaptor,
    ObVecIdxFrozenDataHandle &frozen_data,
    bool &handled)
{
  int ret = OB_SUCCESS;
  int64_t frozen_vec_cnt = 0;
  handled = false;
  if (OB_FAIL(frozen_data->segment_handle_->get_index_number(frozen_vec_cnt))) {
    LOG_WARN("failed to get frozen index number", KR(ret), K(frozen_data));
  } else if (0 == frozen_vec_cnt) {
    const ObVecIdxSnapshotDataHandle &snap_data = adaptor.get_snap_data();
    bool has_persistent_segment = true;
    if (!snap_data.is_valid() || !snap_data->has_complete_) {
      ret = OB_EAGAIN;
      LOG_INFO("snapshot data is not ready when checking empty tablet",
               KR(ret), KPC(&adaptor));
    } else {
      TCRLockGuard lock_guard(snap_data->mem_data_rwlock_);
      has_persistent_segment = snap_data->meta_.segment_count() > 0;
    }
    if (OB_SUCC(ret) && !has_persistent_segment) {
      const ObVecIdxActiveDataHandle &active = adaptor.get_incr_data();
      ObVectorIndexSegmentHandle active_segment;
      ObVectorIndexParam active_param;
      if (!active.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("active data is invalid", KR(ret), KPC(&adaptor));
      } else {
        {
          TCRLockGuard lock_guard(active->mem_data_rwlock_);
          active_segment = active->segment_handle_;
        }
        if (!active_segment.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("active segment is invalid", KR(ret), KPC(&adaptor));
        } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                       active_segment->get_index_param(),
                       ObVectorIndexType::VIT_HNSW_INDEX, active_param))) {
          LOG_WARN("failed to parse active segment params", KR(ret), KPC(&adaptor));
        } else if (OB_FAIL(adaptor.update_index_param(
                       active_param, active_segment->get_index_param()))) {
          LOG_WARN("failed to publish active segment params for empty tablet", KR(ret), KPC(&adaptor));
        }
      }
    }
    if (OB_SUCC(ret)) {
      frozen_data->reuse();
      handled = true;
      LOG_INFO("skip empty frozen segment",
               K(has_persistent_segment), KPC(&adaptor));
    }
  }
  return ret;
}

int ObVecIdxFreezeTask::process_freeze()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  LOG_TRACE("[VECTOR INDEX FREEZE] start process_freeze", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, adpt_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("can not get adapter, need wait", K(ret), KPC(ctx_));
    } else {
      LOG_WARN("fail to get adapter instance", KR(ret), KPC(ctx_));
    }
  } else if (OB_ISNULL(adpt_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
  } else if (!adpt_guard.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(adpt_guard.get_adatper()));
  } else if (!adpt_guard.get_adatper()->is_ready_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter is not complete, no need freeze", K(ret), KPC(adpt_guard.get_adatper()));
  } else if (!check_snapshot_table_available(*adpt_guard.get_adatper())) {
    ret = OB_EAGAIN; // will retry
    LOG_INFO("skip to do async task due to snapshot table not available", KR(ret), KPC(ctx_));
  } else if (FALSE_IT(set_old_adapter(adpt_guard.get_adatper()))) {
  } else if (OB_FAIL(check_and_freeze(adpt_guard, ls_id_))) {
    LOG_WARN("[VECTOR INDEX FREEZE] fail to check and freeze", K(ret), KP(adpt_guard.get_adatper()));
  } else {
    LOG_TRACE("[VECTOR INDEX FREEZE] check freeze success", K(ret), KP(adpt_guard.get_adatper()));
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }
  LOG_TRACE("[VECTOR INDEX FREEZE] end process_freeze", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecIdxFreezeTask::check_and_freeze(ObPluginVectorIndexAdapterGuard &adpt_guard, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor* adaptor = adpt_guard.get_adatper();
  ObVecIdxFrozenDataHandle &frozen_data = adaptor->get_frozen_data();
  if (frozen_data->try_set_in_progress()) {
    if (OB_FAIL(do_check_and_freeze(adpt_guard, ls_id))) {
      LOG_WARN("failed to check and freeze", K(ret), K(ls_id));
    }
    frozen_data->set_not_in_progress();
  } else {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ret = OB_CANCELED;
    ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
    LOG_INFO("another dump task is in progress, skip", KPC(adaptor));
  }
  return ret;
}

int ObVecIdxFreezeTask::do_check_and_freeze(ObPluginVectorIndexAdapterGuard &adpt_guard, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor* adaptor = adpt_guard.get_adatper();
  bool can_freeze = false;
  const bool force_rotate_active = is_alter_param_child();
  bool need_rotate = true;
  ObVectorIndexParam target_param;
  const ObString target_param_str = force_rotate_active
      ? ctx_->task_status_.task_info_.alter_param_info_.index_params_.str()
      : ObString();
  bool has_async_index = false;
  share::SCN async_safe_scn;
  ObVecIdxFrozenDataHandle &frozen_data = adaptor->get_frozen_data();
  if (force_rotate_active && OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
          target_param_str, ObVectorIndexType::VIT_HNSW_INDEX, target_param))) {
    LOG_WARN("fail to parse alter param freeze target", KR(ret), K(target_param_str));
  } else if (ObVecIdxFrozenData::State::NO_FROZEN != frozen_data->state_) {
    // A failed Freeze leaves its state in frozen_data. Resume that state
    // before checking the already-rotated active segment.
    can_freeze = true;
  } else if (force_rotate_active
      && OB_FAIL(prepare_alter_param_freeze_(*adaptor, need_rotate, target_param))) {
    LOG_WARN("fail to prepare alter param freeze", KR(ret), KPC(adaptor));
  } else if (force_rotate_active && !need_rotate) {
    LOG_INFO("active segment already uses target params, skip rotate",
             "parent_task_id", ctx_->task_status_.task_info_.alter_param_info_.parent_task_id_,
             KPC(adaptor));
  } else if (OB_FAIL(adaptor->check_can_freeze(
      ls_id, 0 /* allways in execution */, can_freeze, force_rotate_active))) {
    LOG_WARN("failed to check need freeze", K(ret));
  } else if (! can_freeze) {
    ret = OB_EAGAIN;
    LOG_INFO("active segment actually can not freeze in task execution", K(ret), KPC(adaptor));
  } else if (force_rotate_active
             && OB_FAIL(adaptor->freeze_active_segment(
                 ls_id, &target_param, &target_param_str))) {
    LOG_WARN("failed to freeze active segment with target params", K(ret));
  } else if (!force_rotate_active && OB_FAIL(adaptor->freeze_active_segment(ls_id))) {
    LOG_WARN("failed to freeze active segment", K(ret));
  }

  if (OB_SUCC(ret) && can_freeze) {
    ObArenaAllocator tmp_allocator("VecIdxFrez", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    const share::SCN &frozen_scn = frozen_data->frozen_scn_;
    share::SCN refresh_scn = frozen_scn;
    if (OB_FAIL(get_fast_refresh_target_scn(
            adaptor, has_async_index, async_safe_scn))) {
      LOG_WARN("failed to get vector refresh target scn", K(ret), K(ls_id), K(frozen_scn));
    } else if (has_async_index) {
      refresh_scn = SCN::min(frozen_scn, async_safe_scn);
    }
    while (OB_SUCC(ret) && 
        (ObVecIdxFrozenData::State::NO_FROZEN != frozen_data->state_ && ObVecIdxFrozenData::State::FINISH != frozen_data->state_)) {
      switch (frozen_data->state_) {
      case ObVecIdxFrozenData::State::FROZEN: {
        if (OB_FAIL(check_and_wait_write(frozen_data))) {
          LOG_WARN("check and wait write finished fail", K(ret));
        } else {
          bool handled = false;
          if (OB_FAIL(try_handle_empty_frozen_segment_(
                  *adaptor, frozen_data, handled))) {
            LOG_WARN("failed to handle empty frozen segment", KR(ret), KPC(adaptor));
          } else if (!handled) {
            if (force_rotate_active && OB_UNLIKELY(OB_SUCCESS != (ret = EVENT_CALL(
                ERRSIM_VEC_FREEZE_AFTER_ACTIVE_ROTATE, adaptor->get_data_table_id())))) {
              LOG_WARN("injected error after vector index active segment rotation",
                       KR(ret), K(adaptor->get_data_table_id()));
            } else {
              frozen_data->state_ = ObVecIdxFrozenData::State::TUNE_INDEX;
            }
          }
        }
        break;
      }
      case ObVecIdxFrozenData::State::TUNE_INDEX: {
        if (OB_FAIL(do_tune_index_if_needed(frozen_data, adaptor))) {
          LOG_WARN("tune index fail", K(ret));
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::WAIT_WRITE_FINISHED;
        }
        break;
      }
      case ObVecIdxFrozenData::State::WAIT_WRITE_FINISHED: {
        if (! adaptor->is_hybrid_index()
            && OB_FAIL(ObVectorIndexRefresher::tablet_fast_refresh(
                adaptor, refresh_scn, has_async_index))) {
          if (!has_async_index
              && (OB_INVALID_QUERY_TIMESTAMP == ret
                  || OB_TABLE_DEFINITION_CHANGED == ret)) {
            // Keep the legacy fallback only for a normal vector refresh. An
            // async-index refresh must never move table-3 rows beyond S_safe.
            LOG_INFO("snapshot version is too old, refresh to latest", K(ret), KP(adaptor), K(frozen_scn));
            SCN new_refresh_scn;
            if (OB_FAIL(get_current_scn(new_refresh_scn))) {
              LOG_WARN("fail to get scn", KR(ret));
            } else if (OB_FAIL(ObVectorIndexRefresher::tablet_fast_refresh(adaptor, new_refresh_scn))) {
              LOG_WARN("tablet_fast_refresh fail again", K(ret), K(new_refresh_scn), K(frozen_scn));
            } else {
              frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_VBITMAP;
              LOG_INFO("tablet fast refresh success after use current scn", KP(adaptor), K(new_refresh_scn));
            }
          } else {
            LOG_WARN("tablet_fast_refresh fail", K(ret), KP(adaptor), K(frozen_scn));
          }
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_VBITMAP;
        }
        break;
      }
      case ObVecIdxFrozenData::State::REFRESH_VBITMAP: {
        if (OB_FAIL(adaptor->update_vbitmap_memdata(ls_id, refresh_scn, tmp_allocator))) {
          LOG_WARN("update_vbitmap_memdata fail", K(ret));
        } else if (has_async_index
                   && OB_FAIL(adaptor->advance_async_query_scn(refresh_scn))) {
          // The refreshed vBitmap may expose newer ACTIVE/FROZEN VIDs to iFilter.
          LOG_WARN("failed to advance async query barrier after vbitmap refresh",
                   K(ret), K(refresh_scn), K(frozen_scn), K(async_safe_scn));
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::PERSIST;
        }
        break;
      }
      case ObVecIdxFrozenData::State::PERSIST: {
        bool handled = false;
        if (OB_FAIL(try_handle_empty_frozen_segment_(
                *adaptor, frozen_data, handled))) {
          LOG_WARN("failed to handle empty frozen segment", KR(ret), KPC(adaptor));
        } else if (!handled && OB_FAIL(adaptor->persist_incr_segment(ls_id, ctx_))) {
          LOG_WARN("persist frozen segment fail", K(ret), K(ls_id));
        } else if (!handled) {
          frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_SNAP;
        }
        break;
      }
      case ObVecIdxFrozenData::State::REFRESH_SNAP: {
        if (OB_FAIL(refresh_adaptor(
                adpt_guard, tmp_allocator, ls_id, has_async_index))) {
          LOG_WARN("replace new meta fail", K(ret));
        } else {
          frozen_data->set_frozen_finish();
          LOG_INFO("active segment freeze and persist success");
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid state", K(ret), K(frozen_data->state_));
        break;
      }
      }
      CHECK_TASK_CANCELLED(ret, ctx_);
    }
    if (OB_FAIL(ret)) {
      frozen_data->ret_code_ = ret;
      frozen_data->retry_cnt_++;
      if (frozen_data->retry_too_many()) {
        // ignore ret
        LOG_ERROR("NOTICE!!!: frozen and persist vector index fail too many times", KPC(adaptor));
      }
    }
  }
  return ret;
}

/**
 * Select the logical cutoff used only by tablet-level 3->4 fast refresh. A
 * table with async indexes uses the minimum published SCN of this data
 * partition's available async targets. A normal vector index keeps the legacy
 * frozen_scn selected by the caller.
 *
 * @param adaptor Vector adaptor that identifies the data table and partition.
 * @param has_async_index Output indicating which cutoff rule was selected.
 * @param target_scn Output async-index safety cutoff; invalid for a normal index.
 */
int ObVecIdxFreezeTask::get_fast_refresh_target_scn(
    ObPluginVectorIndexAdaptor *adaptor,
    bool &has_async_index,
    share::SCN &target_scn)
{
  int ret = OB_SUCCESS;
  storage::ObAsyncIndexService *async_index_service = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *data_table_schema = nullptr;
  has_async_index = false;
  target_scn.reset();
  if (OB_ISNULL(adaptor) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state for vector freeze target", K(ret), KP(adaptor), KP(ctx_));
  } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
                         .get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, adaptor->get_data_table_id(), data_table_schema))) {
    LOG_WARN("failed to get vector data table schema", K(ret),
             "data_table_id", adaptor->get_data_table_id());
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("vector data table schema is null", K(ret),
             "data_table_id", adaptor->get_data_table_id());
  } else if (!data_table_schema->has_async_index()) {
    // Keep target_scn invalid. The normal path obtains frozen_scn at the
    // original pointer-rotation point in freeze_active_segment().
  } else if (OB_ISNULL(async_index_service = MTL(storage::ObAsyncIndexService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index service is null", K(ret), K(tenant_id_));
  } else {
    storage::ObAsyncIndexDepProvider &dep_provider =
        async_index_service->get_refresher().get_dep_provider();
    if (OB_FAIL(dep_provider.get_partition_min_async_published_scn(
            tenant_id_, ctx_->get_ls(), adaptor->get_data_table_id(),
            adaptor->get_data_tablet_id(), has_async_index, target_scn))) {
      LOG_WARN("failed to get async index safety point", K(ret),
               "data_table_id", adaptor->get_data_table_id(),
               "data_tablet_id", adaptor->get_data_tablet_id());
    } else if (!has_async_index) {
      ret = OB_EAGAIN;
      LOG_WARN("data table has async index but no refresh target is available",
               K(ret), "data_table_id", adaptor->get_data_table_id(),
               "data_tablet_id", adaptor->get_data_tablet_id());
    } else {
      LOG_DEBUG("get vector fast refresh target scn", K(has_async_index),
                K(target_scn), "data_table_id", adaptor->get_data_table_id(),
                "data_tablet_id", adaptor->get_data_tablet_id());
    }
  }
  return ret;
}

int ObVecIdxFreezeTask::check_and_wait_write(const ObVecIdxFrozenDataHandle &frozen_data)
{
  int ret = OB_SUCCESS;
  const static int64_t WAIT_US = 100 * 1000; // 100ms
  const static int64_t MAX_WAIT_CNT = 100;
  int64_t check_cnt = 0;
  int64_t loop_cnt = 0;
  while(OB_SUCC(ret) && check_cnt < MAX_WAIT_CNT && frozen_data->segment_handle_->get_write_ref() > 0) {
    LOG_INFO("there are some write thread still run, so need wait", K(frozen_data), K(check_cnt));
    ob_usleep(WAIT_US);
    ++check_cnt;
    CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
  }
  if (check_cnt >= MAX_WAIT_CNT || frozen_data->segment_handle_->get_write_ref() > 0) {
    ret = OB_EAGAIN;
    LOG_WARN("reach max wait cnt, still has write thread", K(ret), K(check_cnt), K(frozen_data));
  }
  return ret;
}

int ObVecIdxFreezeTask::do_tune_index_if_needed(const ObVecIdxFrozenDataHandle &frozen_data,
                                                ObPluginVectorIndexAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam frozen_param;
  if (OB_ISNULL(adaptor) || ! frozen_data->segment_handle_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("segment or adaptor is null", K(ret), KPC(adaptor));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
      frozen_data->segment_handle_->get_index_param(),
      ObVectorIndexType::VIT_HNSW_INDEX, frozen_param))) {
    LOG_WARN("failed to parse frozen segment index param", K(ret), K(frozen_data));
  } else {
    const ObVectorIndexAlgorithmType index_type = frozen_param.type_;
    const ObVectorIndexAlgorithmType current_type = frozen_data->segment_handle_->get_index_type();
    if (ERRSIM_VEC_DISABLE_TUNE_INDEX_IN_FREEZE == OB_SUCCESS
        && current_type == VIAT_HGRAPH  && (index_type == VIAT_HNSW_SQ || index_type == VIAT_HNSW_BQ)) {
      const int64_t start_time_us = ObTimeUtility::fast_current_time();
      const ObVectorIndexAlgorithmType tune_type = index_type;
      // tune_index builds params via construct_vsag_create_param and updates handler index_type on success.
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIndexVsagADP"));
      lib::ObLightBacktraceGuard light_backtrace_guard(false);
      if (OB_FAIL(frozen_data->segment_handle_->tune_index(tune_type))) {
        LOG_WARN("tune index failed", K(ret), KPC(adaptor), K(index_type), K(current_type), K(tune_type));
      } else {
        const int64_t cost_us = ObTimeUtility::fast_current_time() - start_time_us;
        LOG_INFO("tune index success", KP(adaptor), K(index_type), K(current_type), K(tune_type), K(cost_us));
      }
    } else {
      LOG_INFO("index type is not hnsw_sq or hnsw_bq, no need tune", K(index_type), K(current_type), KP(adaptor));
    }
  }
  return ret;
}

int ObVecIdxFreezeTask::refresh_adaptor(
    ObPluginVectorIndexAdapterGuard &adpt_guard,
    ObIAllocator &allocator,
    const ObLSID &ls_id,
    const bool has_async_index)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  ObPluginVectorIndexAdaptor* new_adapter = nullptr;
  if (OB_FAIL(create_new_adapter(vector_index_service, adpt_guard, new_adapter))) {
    LOG_WARN("fail to get or create new adapter", K(ret), K(ctx_));
  } else if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(const_cast<ObLSID&>(ls_id),
                                new_adapter,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator,
                                allocator,
                                snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("read_local_tablet fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (! datum_row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first row of vector index is not meta row", K(ret), KPC(datum_row));
  } else {
    ObString meta_data = datum_row->storage_datums_[1].get_string();
    int64_t meta_scn = 0;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, meta_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
      LOG_WARN("get meta scn fail", K(ret));
    } else {
      CHECK_TASK_CANCELLED(ret, ctx_);
      DEBUG_SYNC(BEFORE_VECTOR_INDEX_FREEZE_DESERIALIZE);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_adapter->deserialize_snap_meta(meta_data))) {
      LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(meta_data.length()));
    } else if (OB_FAIL(try_reuse_segments_from_old_adapter())) {
      LOG_WARN("try reuse segments from old adapter fail", K(ret));
    } else if (OB_FAIL(new_adapter->get_snap_data()->load_persist_segments(new_adapter, allocator_, ls_id_,
        current_scn, ctx_))) {
      LOG_WARN("load persist segments fail", K(ret));
    } else if (OB_FALSE_IT(new_adapter->get_snap_data()->set_inited())) {
    } else if (OB_FALSE_IT(new_adapter->set_snap_data_has_complete())) {
    } else if (has_async_index
               && !new_adapter->get_async_query_scn().is_valid()
               && OB_FAIL(ObPluginVectorIndexUtils::refresh_async_query_scn(
                      ls_id, new_adapter))) {
      LOG_WARN("failed to initialize async query scn for freeze adaptor",
               K(ret), K(ls_id), KPC(new_adapter));
    } else {
      LOG_INFO("build new adaptor success", KPC(new_adapter), KPC(adpt_guard.get_adatper()));
    }
  }

  if (OB_SUCC(ret)) {
    ObPluginVectorIndexAdapterGuard latest_adpt_guard;
    RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
    // TODO handle snapshot scn is old
    if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard_in_lock(ctx_->task_status_.tablet_id_, latest_adpt_guard))) {
      LOG_WARN("fail to get adapter instance", K(ret), KPC(ctx_));
    } else if (OB_FAIL(new_adapter->merge_incr_data(latest_adpt_guard.get_adatper(), false))){
      LOG_WARN("partial vector index adapter not valid", K(ret), KP(latest_adpt_guard.get_adatper()), KPC(this));
    } else if (has_async_index
               && latest_adpt_guard.get_adatper()->get_async_query_scn().is_valid()
               && OB_FAIL(new_adapter->advance_async_query_scn(
                      latest_adpt_guard.get_adatper()->get_async_query_scn()))) {
      // Keep the query barrier consistent with the latest vBitmap inherited above.
      LOG_WARN("failed to inherit latest async query scn", K(ret),
               KPC(latest_adpt_guard.get_adatper()), KPC(new_adapter));
    } else if (OB_FAIL(new_adapter->set_replace_scn(current_scn))) {
      LOG_WARN("failed to set replace scn", K(ret), K(current_scn));
    } else if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter))) {
      LOG_WARN("failed to replace old adapter", K(ret));
    } else {
      LOG_INFO("relpace old adaptor success", KP(new_adapter), KP(adpt_guard.get_adatper()), KP(latest_adpt_guard.get_adatper()));
    }
  }

  // clean tmp info
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(vector_index_service->release_vector_index_tmp_info(ctx_->task_status_.task_id_))) {
      LOG_WARN("fail to release vector index tmp info", K(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_adapter)) {
    LOG_INFO("release new adapter memory in failure", K(ret));
    new_adapter->~ObPluginVectorIndexAdaptor();
    vector_index_service->get_adaptor_allocator().free(new_adapter);
    new_adapter = nullptr;
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  if (OB_SUCC(ret)) {
    // just trigger, so don't afftect return
    int tmp_ret = OB_SUCCESS;
    ObSEArray<uint64_t, 2> tablet_ids;
    if (OB_TMP_FAIL(tablet_ids.push_back(new_adapter->get_inc_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(tablet_ids.push_back(new_adapter->get_vbitmap_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVectorIndexRefresher::trigger_major_freeze(tenant_id_, tablet_ids))) {
      LOG_WARN("failed to trigger major freeze", K(tmp_ret));
    }
  }

  return ret;
}

}
}
