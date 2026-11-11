/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/disk_bbq_cache/ob_disk_bbq_cache_task_executor.h"
#include "share/vector_index/disk_bbq_cache/ob_disk_bbq_cache_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

int ObDiskBBQCacheTaskExecutor::do_check_task_result(
    ObVecIndexAsyncTaskCtx *task_ctx,
    ObVecTaskResultCheckAction &action)
{
  return do_check_normal_running_task_result(task_ctx, action);
}

int ObDiskBBQCacheTaskExecutor::load_triggered_task(
    const ObVecIndexTaskStatus &task_row)
{
  UNUSED(task_row);
  return OB_NOT_SUPPORTED;
}

int ObDiskBBQCacheTaskExecutor::check_and_set_thread_pool()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ cache task executor is not initialized", KR(ret));
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", KR(ret), K_(tenant_id));
  } else {
    ObVecIndexAsyncTaskHandler &handler =
        vector_index_service_->get_vec_async_task_handle();
    common::ObSpinLockGuard init_guard(handler.lock_);
    if (handler.get_tg_id() != INVALID_TG_ID) {
    } else if (OB_FAIL(handler.init())) {
      LOG_WARN("failed to init vector async task handler", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(handler.start())) {
      LOG_WARN("failed to start vector async task handler", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObDiskBBQCacheTaskExecutor::add_task_ctx_(
    ObPluginVectorIndexMgr &index_ls_mgr,
    const ObTabletID &tablet_id,
    const uint64_t table_id,
    const ObVecIndexAsyncTaskType task_type,
    uint64_t &task_trace_base_num,
    ObVecIndexTaskCtxArray &task_status_array)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool inc_new_task = false;
  bool task_ctx_in_map = false;
  ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr.get_async_task_opt();
  ObIAllocator *allocator = task_opt.get_allocator();
  ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
  int64_t new_task_id = OB_INVALID_ID;
  common::ObCurTraceId::TraceId new_trace_id;
  if (!tablet_id.is_valid() || OB_INVALID_ID == table_id
      || (OB_VECTOR_ASYNC_DISK_BBQ_CACHE_CLEAN != task_type
          && OB_VECTOR_ASYNC_DISK_BBQ_HGRAPH_BUILD != task_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ cache task argument",
             KR(ret), K(tablet_id), K(table_id), K(task_type));
  } else if (OB_FAIL(task_opt.is_task_ctx_exist(tablet_id, task_type, is_exist))) {
    LOG_WARN("failed to check DiskBBQ cache task existence",
             KR(ret), K(tablet_id), K(task_type));
  } else if (is_exist) {
  } else if (OB_NOT_NULL(scheduler_)
             && scheduler_->is_task_disabled(task_type, tablet_id.id())) {
    LOG_TRACE("skip generating disabled DiskBBQ cache task",
              K_(tenant_id), K(tablet_id), K(task_type));
  } else if (OB_ISNULL(task_ctx = OB_NEWx(ObVecIndexAsyncTaskCtx, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate DiskBBQ cache task ctx", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(
                 tenant_id_, new_task_id))) {
    LOG_WARN("failed to fetch DiskBBQ cache task id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(
                 ++task_trace_base_num, allocator, new_trace_id))) {
    LOG_WARN("failed to fetch DiskBBQ cache task trace id", KR(ret), K(tablet_id));
  } else {
    task_ctx->tenant_id_ = tenant_id_;
    task_ctx->ls_handle_ = ls_handle_;
    task_ctx->task_status_.tablet_id_ = tablet_id;
    task_ctx->task_status_.tenant_id_ = tenant_id_;
    task_ctx->task_status_.table_id_ = table_id;
    task_ctx->task_status_.task_id_ = new_task_id;
    task_ctx->task_status_.task_type_ = task_type;
    task_ctx->task_status_.trigger_type_ = OB_VEC_TRIGGER_AUTO;
    task_ctx->task_status_.status_ = OB_VECTOR_ASYNC_TASK_PREPARE;
    task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
    task_ctx->task_status_.trace_id_ = new_trace_id;
    task_ctx->task_status_.target_scn_.convert_from_ts(
        ObTimeUtility::current_time());
    if (OB_FAIL(task_opt.add_task_ctx(
            task_ctx->task_status_.tablet_id_, task_ctx, inc_new_task))) {
      LOG_WARN("failed to add DiskBBQ cache task ctx",
               KR(ret), K(tablet_id), K(task_type));
    } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
    } else if (inc_new_task && OB_FAIL(task_status_array.push_back(task_ctx))) {
      LOG_WARN("failed to append DiskBBQ cache task ctx",
               KR(ret), K(tablet_id), K(task_type));
    }
  }
  if (OB_NOT_NULL(task_ctx) && (OB_FAIL(ret) || !inc_new_task)) {
    if (task_ctx_in_map) {
      const int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(task_opt, task_ctx);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to clear DiskBBQ cache task ctx",
                 K(tmp_ret), K(tablet_id), K(task_type));
      }
    } else {
      task_ctx->~ObVecIndexAsyncTaskCtx();
      allocator->free(task_ctx);
    }
  }
  return ret;
}

int ObDiskBBQCacheTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  storage::ObLS *ls = ls_handle_.get_ls();
  ObVecIndexTaskCtxArray task_status_array;
  ObSEArray<ObLSCacheMgrKeyPair, 64> cache_keys;
  ObLSID ls_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ cache task executor is not initialized", KR(ret));
  } else if (!check_operation_allow()) {
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LS is null for DiskBBQ cache task loading", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("failed to get LS vector manager for DiskBBQ cache tasks", KR(ret));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("LS vector manager is null for DiskBBQ cache tasks", KR(ret));
  } else if (index_ls_mgr->get_async_task_opt().is_stop()) {
    LOG_INFO("LS is stopping, skip DiskBBQ cache task loading", K(ls->get_ls_id()));
  } else if (FALSE_IT(ls_id = ls->get_ls_id())) {
  } else if (OB_FAIL(index_ls_mgr->get_cache_keys(ls_id, cache_keys))) {
    LOG_WARN("failed to snapshot DiskBBQ cache keys", KR(ret), K(ls_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cache_keys.count(); ++i) {
    const ObIvfCacheMgrKey &cache_key = cache_keys.at(i).key_;
    if (!cache_key.is_sstable_key()) {
      continue;
    }
    ObIvfCacheMgrGuard cache_mgr_guard;
    ObIvfCacheMgr *cache_mgr = nullptr;
    const int tmp_ret = index_ls_mgr->get_ivf_cache_mgr_guard(
        cache_key, cache_mgr_guard);
    if (OB_HASH_NOT_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
    } else if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to acquire DiskBBQ cache manager",
               KR(ret), K(cache_key));
    } else if (OB_ISNULL(cache_mgr = cache_mgr_guard.get_ivf_cache_mgr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null DiskBBQ cache manager", KR(ret), K(cache_key));
    } else {
      bool exists = false;
      if (OB_FAIL(ObDiskBBQCacheTaskUtil::check_sstable_exists(
              *ls, cache_key, exists))) {
        LOG_WARN("failed to validate DiskBBQ cache", KR(ret), K(cache_key));
      } else if (!exists) {
        if (OB_FAIL(add_task_ctx_(
                *index_ls_mgr, cache_key.get_tablet_id(), cache_mgr->get_table_id(),
                OB_VECTOR_ASYNC_DISK_BBQ_CACHE_CLEAN,
                task_trace_base_num, task_status_array))) {
          LOG_WARN("failed to generate DiskBBQ cache clean task",
                   KR(ret), K(cache_key));
        }
      } else if (cache_key.get_group_id() == 0
                 && !vector_index_service_->get_ivf_sstable_cache_controller().is_admission_closed()
                 && index_ls_mgr->has_ivf_sstable_hgraph_pending(cache_key)
                 && OB_FAIL(add_task_ctx_(
                        *index_ls_mgr, cache_key.get_tablet_id(), cache_mgr->get_table_id(),
                        OB_VECTOR_ASYNC_DISK_BBQ_HGRAPH_BUILD,
                        task_trace_base_num, task_status_array))) {
        LOG_WARN("failed to generate DiskBBQ HGraph task", KR(ret), K(cache_key));
      }
    }
  }

  if (OB_SUCC(ret) && !task_status_array.empty()) {
    if (OB_FAIL(check_and_set_thread_pool())) {
      LOG_WARN("failed to start thread pool for DiskBBQ cache tasks",
               KR(ret), K(ls_id));
    } else if (OB_FAIL(insert_new_task(task_status_array))) {
      LOG_WARN("failed to persist DiskBBQ cache tasks", KR(ret), K(ls_id));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(index_ls_mgr) && !task_status_array.empty()) {
    const int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctxs(
        index_ls_mgr->get_async_task_opt(), task_status_array);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to clear DiskBBQ cache task ctxs", K(tmp_ret), K(ls_id));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
