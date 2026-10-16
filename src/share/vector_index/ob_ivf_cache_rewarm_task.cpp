/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_ivf_cache_rewarm_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_ivf_cache_warmup_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_debug_sync.h"
#include "observer/ob_server_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "share/location_cache/ob_location_service.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace share
{

int ObIvfCacheRewarmExecutor::init(uint64_t tenant_id, ObLSHandle &ls_handle)
{
  return ObVecITaskExecutor::init(tenant_id, ls_handle);
}

int ObIvfCacheRewarmExecutor::request_rewarm()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf cache executor not initialized for rewarm", KR(ret));
  } else {
    common::ObSpinLockGuard guard(request_lock_);
    cleanup_only_ = false;
    ATOMIC_INC(&requested_epoch_);
  }
  return ret;
}

int ObIvfCacheRewarmExecutor::request_cleanup()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf cache executor not initialized for cleanup", KR(ret));
  } else {
    common::ObSpinLockGuard guard(request_lock_);
    cleanup_only_ = true;
    ATOMIC_INC(&requested_epoch_);
  }
  return ret;
}

bool ObIvfCacheRewarmExecutor::check_operation_allow()
{
  return true;
}

int ObIvfCacheRewarmExecutor::load_triggered_task(const ObVecIndexTaskStatus &task_row)
{
  UNUSED(task_row);
  return OB_NOT_SUPPORTED;
}

ObIvfCacheLifecycleLoadAction ObIvfCacheRewarmExecutor::decide_load_action(
    const bool has_pending_request,
    const bool request_role_is_current,
    const bool need_run,
    const bool task_ctx_exists,
    const int64_t task_status,
    const bool role_generation_applied)
{
  UNUSED(task_status);
  ObIvfCacheLifecycleLoadAction action = IVF_CACHE_LIFECYCLE_IDLE;
  if (!has_pending_request) {
    action = IVF_CACHE_LIFECYCLE_IDLE;
  } else if (task_ctx_exists) {
    // The common result path still owns active and terminal task contexts.
    // A published generation can still be rolled back by a late cancellation.
    action = IVF_CACHE_LIFECYCLE_WAIT_TASK;
  } else if (!request_role_is_current || !need_run || role_generation_applied) {
    action = IVF_CACHE_LIFECYCLE_CONSUME;
  } else {
    action = IVF_CACHE_LIFECYCLE_CREATE_TASK;
  }
  return action;
}

bool ObIvfCacheRewarmExecutor::should_start_thread_pool(
    const bool has_pending_request,
    const bool need_run,
    const bool pool_stopped,
    const bool pool_started)
{
  return has_pending_request && need_run && !pool_stopped && !pool_started;
}

bool ObIvfCacheRewarmExecutor::can_publish_generation(
    const int work_ret,
    const bool task_stopped,
    const bool task_cancelled,
    const bool role_epoch_current)
{
  return OB_SUCCESS == work_ret && !task_stopped && !task_cancelled && role_epoch_current;
}

bool ObIvfCacheRewarmExecutor::should_invalidate_published_generation(
    const bool generation_published,
    const bool task_cancelled)
{
  return generation_published && task_cancelled;
}

bool ObIvfCacheRewarmExecutor::uses_synthetic_ls_tablet(const int64_t task_type)
{
  return ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_IVF_CACHE_REWARM == task_type;
}

int ObIvfCacheRewarmExecutor::check_need_run_lifecycle_task_(
    ObPluginVectorIndexMgr *mgr,
    const bool cleanup_only,
    bool &need_run)
{
  int ret = OB_SUCCESS;
  need_run = false;
  if (OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index LS manager for IVF cache lifecycle", KR(ret));
  } else {
    bool has_cache_mgr = false;
    {
      // ObHashMap::size() is not atomic. PREWARM and a lifecycle task can
      // insert/erase managers concurrently, so inspect it under the same map
      // lock used by snapshot/erase.
      RWLock::RLockGuard lock_guard(mgr->get_ivf_cache_mgr_map_rwlock());
      has_cache_mgr = mgr->get_ivf_cache_mgr_map().size() > 0;
    }
    if (has_cache_mgr) {
      // Cleanup must still run after the IVF schema/catalog is removed.
      need_run = true;
    } else if (!cleanup_only) {
      bool has_ivf_index = false;
      ObSEArray<uint64_t, 16> vec_table_id_array;
      if (OB_FAIL(ObPluginVectorIndexUtils::get_tenant_vector_index_ids(
              tenant_id_, has_ivf_index, vec_table_id_array))) {
        LOG_WARN("fail to check tenant IVF index for cache rewarm", KR(ret), K_(tenant_id));
      } else {
        // PREWARM is leader-only, so an empty local map is expected on the new
        // leader. Tenant schema, not follower-local state, decides whether the
        // leader rewarm request is actionable.
        need_run = has_ivf_index;
      }
    }
  }
  return ret;
}

int ObIvfCacheRewarmExecutor::check_and_set_thread_pool()
{
  int ret = OB_SUCCESS;
  int64_t requested_epoch = 0;
  ObPluginVectorIndexMgr *mgr = nullptr;
  bool cleanup_only = false;
  bool need_run = false;
  {
    common::ObSpinLockGuard guard(request_lock_);
    requested_epoch = ATOMIC_LOAD(&requested_epoch_);
    cleanup_only = cleanup_only_;
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf cache rewarm executor not initialized", KR(ret));
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", KR(ret), K_(tenant_id));
  } else if (requested_epoch == ATOMIC_LOAD(&applied_epoch_)) {
    // No request, avoid creating the shared task pool solely for this executor.
  } else if (OB_FAIL(get_index_ls_mgr(mgr))) {
    LOG_WARN("fail to get index LS manager for IVF cache lifecycle", KR(ret), K_(ls_handle));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index LS manager is null for IVF cache lifecycle", KR(ret), K_(ls_handle));
  } else if (OB_FAIL(check_need_run_lifecycle_task_(mgr, cleanup_only, need_run))) {
    LOG_WARN("fail to check whether IVF cache lifecycle task is needed",
             KR(ret), K_(tenant_id), K(cleanup_only));
  } else if (!need_run) {
    // load_task() owns request consumption after checking the standard task
    // ctx, so a late cancellation cannot lose an in-flight request here.
  } else {
    ObVecIndexAsyncTaskHandler &handle = vector_index_service_->get_vec_async_task_handle();
    common::ObSpinLockGuard init_guard(handle.lock_);
    if (!should_start_thread_pool(true, need_run, handle.is_stopped(), handle.is_started())) {
      // Tenant shutdown owns the lifecycle now; never resurrect the pool.
    } else {
      if (OB_FAIL(handle.init())) {
        if (OB_INIT_TWICE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to init vector async task pool", KR(ret), K_(tenant_id));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(handle.start())) {
        LOG_WARN("fail to start vector async task pool", KR(ret), K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObIvfCacheRewarmExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  int64_t requested_epoch = 0;
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
  bool inc_new_task = false;
  bool task_ctx_in_map = false;
  bool cleanup_only = false;
  bool need_run = false;
  bool is_leader = false;
  bool cache_ready = false;
  int64_t role_epoch = 0;
  ObIvfCacheLifecycleLoadAction load_action = IVF_CACHE_LIFECYCLE_IDLE;
  ObArenaAllocator request_allocator(ObMemAttr(tenant_id_, "IvfRewarmReq"));
  ObSEArray<ObString, 4> sdk_db_names;
  ObString copied_db_name;
  ObTabletID representative_tablet;
  {
    common::ObSpinLockGuard guard(request_lock_);
    requested_epoch = ATOMIC_LOAD(&requested_epoch_);
    cleanup_only = cleanup_only_;
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (requested_epoch == ATOMIC_LOAD(&applied_epoch_)) {
  } else if (OB_FAIL(get_index_ls_mgr(mgr))) {
    LOG_WARN("fail to get index mgr for ivf cache rewarm", KR(ret), K_(ls_handle));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (mgr->get_async_task_opt().is_stop()) {
  } else {
    // The persistent task uniqueness key is (tablet_id, task_type). Use the
    // target LS id as this LS-scoped task's synthetic representative tablet;
    // a tenant-wide constant would make concurrent rewarm/cleanup requests
    // from different LSs suppress each other as duplicates.
    representative_tablet = ObTabletID(mgr->get_ls_id().id());
    mgr->get_ivf_cache_role_state(is_leader, cache_ready, role_epoch);
    ObVecIndexAsyncTaskOption &task_opt = mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    bool exists = false;
    int64_t status = OB_VECTOR_ASYNC_TASK_INVALID;
    if (OB_FAIL(task_opt.get_task_ctx_status(representative_tablet,
            OB_VECTOR_ASYNC_IVF_CACHE_REWARM, exists, status))) {
      LOG_WARN("fail to check ivf cache rewarm ctx", KR(ret), K_(ls_handle));
    } else if (OB_FAIL(check_need_run_lifecycle_task_(mgr, cleanup_only, need_run))) {
      LOG_WARN("fail to check whether IVF cache lifecycle task should load",
               KR(ret), K_(tenant_id), K(cleanup_only));
    } else {
      load_action = decide_load_action(
          true,
          cleanup_only ? !is_leader : is_leader,
          need_run,
          exists,
          status,
          mgr->get_ivf_cache_applied_role_epoch() == role_epoch);
      if (IVF_CACHE_LIFECYCLE_CONSUME == load_action) {
        bool can_consume_request = true;
        const bool request_role_is_current = cleanup_only ? !is_leader : is_leader;
        if (request_role_is_current && !need_run
            && mgr->get_ivf_cache_applied_role_epoch() != role_epoch) {
          int64_t active_writer_count = 0;
          if (OB_FAIL(mgr->check_ivf_cache_prewarm_writers_drained(
                  role_epoch, is_leader, active_writer_count))) {
            // A PREWARM admitted by the previous role may still be scanning
            // before it creates its first cache manager. Do not publish and
            // consume an apparently empty generation until that writer exits;
            // otherwise a follower can retain the manager it creates later.
            can_consume_request = false;
            LOG_INFO("wait for manual prewarm writers before consuming empty generation",
                     KR(ret), K_(tenant_id), K_(ls_handle), K(cleanup_only),
                     K(role_epoch), K(active_writer_count));
          }
          // Even an empty leader generation must be published. Otherwise a
          // tenant that creates its first IVF index after this role callback
          // would keep ready=false forever and every manual PREWARM would be
          // rejected despite there being no rebuild task to open the gate.
          if (OB_SUCC(ret)) {
            const bool ready = !cleanup_only;
            can_consume_request = mgr->set_ivf_cache_ready_if_role_epoch(
                role_epoch, is_leader, ready);
            if (!can_consume_request) {
              LOG_INFO("ivf cache role changed while publishing empty generation",
                       K_(tenant_id), K_(ls_handle), K(cleanup_only), K(role_epoch));
            }
          }
        }
        if (OB_SUCC(ret) && can_consume_request) {
          // Consume only the captured request. A concurrent newer request remains pending.
          ATOMIC_STORE(&applied_epoch_, requested_epoch);
        }
      } else if (IVF_CACHE_LIFECYCLE_CREATE_TASK == load_action && !cleanup_only) {
        // An empty list still needs a task to erase the previous generation.
        if (OB_FAIL(vector_index_service_->resolve_sdk_databases_for_ivf_rewarm(
                request_allocator, sdk_db_names))) {
          LOG_WARN("fail to resolve SDK databases in IVF cache executor",
                   KR(ret), K_(tenant_id), K_(ls_handle));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (IVF_CACHE_LIFECYCLE_CREATE_TASK != load_action) {
      // Active/terminal ctx stays under the common result path. No-op and
      // obsolete generations have already been consumed above.
    } else if (OB_ISNULL(task_ctx = OB_NEWx(ObVecIndexAsyncTaskCtx, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t new_task_id = OB_INVALID_ID;
      ObCurTraceId::TraceId new_trace_id;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
        LOG_WARN("fail to fetch ivf cache rewarm task id", KR(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(
                     ++task_trace_base_num, allocator, new_trace_id))) {
        LOG_WARN("fail to fetch ivf cache rewarm trace id", KR(ret));
      } else {
        task_ctx->tenant_id_ = tenant_id_;
        task_ctx->ls_handle_ = ls_handle_;
        task_ctx->task_status_.tenant_id_ = tenant_id_;
        task_ctx->task_status_.table_id_ = ObTabletID::LS_TX_CTX_TABLET_ID;
        task_ctx->task_status_.tablet_id_ = representative_tablet;
        task_ctx->task_status_.task_id_ = new_task_id;
        task_ctx->task_status_.task_type_ = OB_VECTOR_ASYNC_IVF_CACHE_REWARM;
        task_ctx->task_status_.trigger_type_ = OB_VEC_TRIGGER_AUTO;
        task_ctx->task_status_.status_ = OB_VECTOR_ASYNC_TASK_PREPARE;
        task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
        task_ctx->task_status_.trace_id_ = new_trace_id;
        if (OB_FAIL(task_ctx->task_status_.target_scn_.convert_from_ts(
                ObTimeUtility::current_time()))) {
          LOG_WARN("fail to set ivf cache rewarm target scn", KR(ret), K(new_task_id));
        } else {
          task_ctx->ivf_cache_cleanup_only_ = cleanup_only;
          task_ctx->ivf_cache_role_epoch_ = role_epoch;
        }
        if (OB_SUCC(ret) && !cleanup_only) {
          for (int64_t i = 0; OB_SUCC(ret) && i < sdk_db_names.count(); ++i) {
            if (OB_FAIL(ob_write_string(task_ctx->allocator_, sdk_db_names.at(i), copied_db_name))) {
              LOG_WARN("fail to copy sdk database name into rewarm task ctx", KR(ret), K(i));
            } else if (OB_FAIL(task_ctx->ivf_rewarm_sdk_db_names_.push_back(copied_db_name))) {
              LOG_WARN("fail to collect sdk database name in rewarm task ctx", KR(ret), K(i));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(task_opt.add_task_ctx(representative_tablet, task_ctx, inc_new_task))) {
          LOG_WARN("fail to add ivf cache rewarm ctx", KR(ret), K_(ls_handle));
        } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
        } else if (inc_new_task) {
          ObVecIndexTaskCtxArray tasks;
          if (OB_FAIL(tasks.push_back(task_ctx))) {
            LOG_WARN("fail to collect ivf cache rewarm ctx", KR(ret));
          } else if (OB_FAIL(insert_new_task(tasks))) {
            LOG_WARN("fail to persist ivf cache rewarm task", KR(ret), K_(ls_handle));
          } else {
            // A successful insert_new_task() transfers ownership. The ctx is
            // either retained by task_opt or cleared there as a persistent
            // duplicate, so this executor must never roll it back afterwards.
            task_ctx = nullptr;
            task_ctx_in_map = false;
            // insert_new_task() returns success even when a non-terminal row
            // with the same key exists; in that case it also removes this ctx
            // from task_opt. Do not consume the request epoch: the stale row
            // will be swept by the common result path and the next timer tick
            // must retry loading this request.
            bool task_still_exists = false;
            int64_t loaded_status = OB_VECTOR_ASYNC_TASK_INVALID;
            if (OB_FAIL(task_opt.get_task_ctx_status(representative_tablet,
                    OB_VECTOR_ASYNC_IVF_CACHE_REWARM, task_still_exists, loaded_status))) {
              LOG_WARN("fail to verify persisted ivf cache task ctx", KR(ret), K_(ls_handle));
            } else if (!task_still_exists) {
              LOG_INFO("ivf cache task was suppressed by persistent duplicate, retry later",
                       K_(tenant_id), K_(ls_handle), K(requested_epoch), K(new_task_id));
            } else {
              LOG_INFO("ivf cache rewarm task loaded by executor",
                       K_(tenant_id), K_(ls_handle), K(requested_epoch), K(new_task_id),
                       K(representative_tablet));
            }
          }
        }
      }
      if (OB_NOT_NULL(task_ctx) && (OB_FAIL(ret) || !inc_new_task)) {
        if (task_ctx_in_map) {
          common::ObSpinLockGuard task_ctx_map_guard(mgr->task_ctx_lock_);
          int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(task_opt, task_ctx);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to rollback ivf cache rewarm ctx", K(tmp_ret), K_(ls_handle));
          }
        } else {
          task_ctx->~ObVecIndexAsyncTaskCtx();
          allocator->free(task_ctx);
        }
      }
    }
  }
  return ret;
}

int ObIvfCacheRewarmTask::init_ls_rewarm(
    const uint64_t tenant_id,
    ObMySQLProxy *sql_proxy,
    const ObLSID &target_ls_id,
    ObVecIndexAsyncTaskCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(sql_proxy) || !target_ls_id.is_valid() || OB_ISNULL(ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[IVF_CACHE_REWARM] invalid argument", KR(ret), KP(sql_proxy),
             K(target_ls_id), KP(ctx));
  } else if (OB_FAIL(ObVecIndexIAsyncTask::init(tenant_id, target_ls_id,
                     OB_VECTOR_ASYNC_IVF_CACHE_REWARM, ctx))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to init scheduled task", KR(ret), K(target_ls_id));
  } else {
    sql_proxy_ = sql_proxy;
    target_ls_id_ = target_ls_id;
    cleanup_only_ = ctx->ivf_cache_cleanup_only_;
    role_epoch_ = ctx->ivf_cache_role_epoch_;
    if (cleanup_only_) {
      // Follower cleanup never queries metadata and needs no SDK database.
    } else {
      ObString copied_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx->ivf_rewarm_sdk_db_names_.count(); ++i) {
        if (OB_FAIL(ob_write_string(allocator_, ctx->ivf_rewarm_sdk_db_names_.at(i), copied_name))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to copy sdk database name from task ctx", KR(ret), K(i));
        } else if (OB_FAIL(sdk_db_names_.push_back(copied_name))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to collect sdk database name", KR(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObIvfCacheRewarmTask::do_work()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *published_mgr = nullptr;
  bool generation_published = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[IVF_CACHE_REWARM] task not inited", KR(ret));
  } else {
    if (cleanup_only_ && OB_FAIL(cleanup_ls_())) {
      LOG_WARN("[IVF_CACHE_CLEANUP] fail to clear follower LS caches",
               KR(ret), K_(target_ls_id));
    } else if (!cleanup_only_ && OB_FAIL(reset_leader_cache_())) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to clear stale leader LS caches",
               KR(ret), K_(target_ls_id));
    } else if (!cleanup_only_ && OB_FAIL(rewarm_ls_())) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to rebuild caches", KR(ret), K_(target_ls_id));
    } else if (!cleanup_only_) {
      ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
      ObPluginVectorIndexMgr *mgr = nullptr;
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[IVF_CACHE_REWARM] vector index service is null when publishing generation", KR(ret));
      } else if (OB_FAIL(service->get_ls_index_mgr_map().get_refactored(target_ls_id_, mgr))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to get LS index mgr when publishing generation",
                 KR(ret), K_(target_ls_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[IVF_CACHE_REWARM] LS index mgr is null when publishing generation",
                 KR(ret), K_(target_ls_id));
      } else if (is_task_stopped_()) {
        ret = OB_CANCELED;
        LOG_INFO("[IVF_CACHE_REWARM] task stopped before publishing generation",
                 K_(target_ls_id), K_(role_epoch));
      } else if (!is_role_epoch_current_(true)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("[IVF_CACHE_REWARM] leadership changed before publishing generation",
                 KR(ret), K_(target_ls_id));
      } else {
        common::ObSpinLockGuard ctx_guard(ctx_->lock_);
        const bool task_cancelled = ctx_->task_status_.status_ == OB_VECTOR_ASYNC_TASK_CANCEL
            || ctx_->task_status_.ret_code_ == OB_CANCELED;
        if (!ObIvfCacheRewarmExecutor::can_publish_generation(
                OB_SUCCESS, false, task_cancelled, true)) {
          ret = OB_CANCELED;
        } else if (!mgr->set_ivf_cache_ready_if_role_epoch(role_epoch_, true, true)) {
          ret = OB_NOT_MASTER;
          LOG_INFO("[IVF_CACHE_REWARM] role changed while publishing generation",
                   K_(target_ls_id), K_(role_epoch));
        } else {
          published_mgr = mgr;
          generation_published = true;
          LOG_INFO("[IVF_CACHE_REWARM] new cache generation is queryable",
                   K_(target_ls_id), K_(role_epoch));
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard guard(ctx_->lock_);
    if (ctx_->task_status_.status_ == OB_VECTOR_ASYNC_TASK_CANCEL
        || ctx_->task_status_.ret_code_ == OB_CANCELED) {
      if (OB_NOT_NULL(published_mgr)
          && ObIvfCacheRewarmExecutor::should_invalidate_published_generation(
                 generation_published, true)) {
        const bool invalidated = published_mgr->invalidate_ivf_cache_generation_if_role_epoch(
            role_epoch_, true);
        LOG_INFO("[IVF_CACHE_REWARM] rollback cache generation after late cancellation",
                 K(invalidated), K_(target_ls_id), K_(role_epoch));
      }
      ret = OB_CANCELED;
      ctx_->task_status_.ret_code_ = OB_CANCELED;
    } else {
      ctx_->task_status_.ret_code_ = ret;
    }
    ctx_->task_status_.all_finished_ = true;
  }
  return ret;
}

int ObIvfCacheRewarmTask::reset_leader_cache_()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObSEArray<ObIvfCacheMgrKey, 16> keys;
  keys.set_attr(ObMemAttr(tenant_id_, "IvfResetKeys"));
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] vector index service is null before reset", KR(ret));
  } else if (OB_FAIL(service->get_ls_index_mgr_map().get_refactored(target_ls_id_, mgr))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to get LS index mgr before reset",
             KR(ret), K_(target_ls_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] LS index mgr is null before reset",
             KR(ret), K_(target_ls_id));
  } else if (!is_role_epoch_current_(true)) {
    ret = OB_NOT_MASTER;
    LOG_INFO("[IVF_CACHE_REWARM] LS is no longer leader before reset",
             K_(target_ls_id));
  } else {
    int64_t active_writer_count = 0;
    if (OB_FAIL(mgr->check_ivf_cache_prewarm_writers_drained(
            role_epoch_, true, active_writer_count))) {
      LOG_INFO("[IVF_CACHE_REWARM] wait for manual prewarm writers before reset",
               KR(ret), K_(target_ls_id), K_(role_epoch), K(active_writer_count));
    }
    // Snapshot only the target LS manager's keys while holding its read lock.
    // Erase outside the lock so a large hot namespace set never blocks the map.
    if (OB_SUCC(ret)) {
      RWLock::RLockGuard lock_guard(mgr->get_ivf_cache_mgr_map_rwlock());
      FOREACH_X(iter, mgr->get_ivf_cache_mgr_map(), OB_SUCC(ret)) {
        if (OB_FAIL(keys.push_back(iter->first))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to snapshot stale cache key", KR(ret));
        }
      }
    }
    int64_t erased_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (is_task_stopped_()) {
        ret = OB_CANCELED;
      } else if (!is_role_epoch_current_(true)) {
        ret = OB_NOT_MASTER;
      } else if (OB_FAIL(mgr->erase_ivf_cache_mgr(keys.at(i)))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[IVF_CACHE_REWARM] fail to erase stale cache",
                   KR(ret), K(keys.at(i)));
        }
      } else {
        ++erased_count;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[IVF_CACHE_REWARM] stale leader LS caches cleared asynchronously",
               K_(tenant_id), K_(target_ls_id), K(erased_count),
               "snapshot_count", keys.count());
    } else {
      LOG_INFO("[IVF_CACHE_REWARM] stop resetting stale cache after role/task change",
               KR(ret), K_(target_ls_id), K(erased_count),
               "snapshot_count", keys.count());
    }
  }
  return ret;
}

int ObIvfCacheRewarmTask::cleanup_ls_()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObSEArray<ObIvfCacheMgrKey, 16> keys;
  keys.set_attr(ObMemAttr(tenant_id_, "IvfCleanKeys"));
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_CLEANUP] vector index service is null", KR(ret));
  } else if (OB_FAIL(service->get_ls_index_mgr_map().get_refactored(target_ls_id_, mgr))) {
    LOG_WARN("[IVF_CACHE_CLEANUP] fail to get LS index mgr", KR(ret), K_(target_ls_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_CLEANUP] LS index mgr is null", KR(ret), K_(target_ls_id));
  } else if (!is_role_epoch_current_(false)) {
    ret = OB_CANCELED;
    LOG_INFO("[IVF_CACHE_CLEANUP] LS became leader before cleanup, cancel",
             K_(target_ls_id));
  } else {
    int64_t active_writer_count = 0;
    if (OB_FAIL(mgr->check_ivf_cache_prewarm_writers_drained(
            role_epoch_, false, active_writer_count))) {
      if (OB_NOT_MASTER == ret) {
        ret = OB_CANCELED;
      }
      LOG_INFO("[IVF_CACHE_CLEANUP] wait for manual prewarm writers before cleanup",
               KR(ret), K_(target_ls_id), K_(role_epoch), K(active_writer_count));
    }
    if (OB_SUCC(ret)) {
      RWLock::RLockGuard lock_guard(mgr->get_ivf_cache_mgr_map_rwlock());
      FOREACH_X(iter, mgr->get_ivf_cache_mgr_map(), OB_SUCC(ret)) {
        if (OB_FAIL(keys.push_back(iter->first))) {
          LOG_WARN("[IVF_CACHE_CLEANUP] fail to snapshot cache key", KR(ret));
        }
      }
    }
    int64_t erased_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      if (is_task_stopped_() || !is_role_epoch_current_(false)) {
        ret = OB_CANCELED;
        LOG_INFO("[IVF_CACHE_CLEANUP] role changed or task stopped, cancel cleanup",
                 K_(target_ls_id), K(i), "total_count", keys.count());
      } else if (OB_FAIL(mgr->erase_ivf_cache_mgr(keys.at(i)))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[IVF_CACHE_CLEANUP] fail to erase cache", KR(ret), K(keys.at(i)));
        }
      } else {
        ++erased_count;
      }
    }
    if (OB_SUCC(ret) && is_role_epoch_current_(false) && !is_task_stopped_()) {
      common::ObSpinLockGuard ctx_guard(ctx_->lock_);
      if (ctx_->task_status_.status_ == OB_VECTOR_ASYNC_TASK_CANCEL
          || !mgr->set_ivf_cache_ready_if_role_epoch(role_epoch_, false, false)) {
        ret = OB_CANCELED;
      } else {
        LOG_INFO("[IVF_CACHE_CLEANUP] follower LS caches cleared",
                 K_(tenant_id), K_(target_ls_id), K(erased_count),
                 "snapshot_count", keys.count());
      }
    }
  }
  return ret;
}

bool ObIvfCacheRewarmTask::is_task_stopped_() const
{
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  bool stopped = OB_ISNULL(service) || service->get_vec_async_task_handle().is_stopped();
  if (!stopped && OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard guard(ctx_->lock_);
    stopped = ctx_->task_status_.status_ == OB_VECTOR_ASYNC_TASK_CANCEL
        || ctx_->task_status_.status_ == OB_VECTOR_ASYNC_TASK_FINISH;
  }
  return stopped;
}

bool ObIvfCacheRewarmTask::is_role_epoch_current_(const bool expect_leader) const
{
  bool current = false;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *mgr = nullptr;
  int tmp_ret = OB_SUCCESS;
  if (!target_ls_id_.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[IVF_CACHE_REWARM] invalid target ls id");
  } else if (OB_ISNULL(service)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "[IVF_CACHE_REWARM] vector index service is null");
  } else if (OB_TMP_FAIL(service->get_ls_index_mgr_map().get_refactored(target_ls_id_, mgr))) {
    LOG_WARN_RET(tmp_ret, "[IVF_CACHE_REWARM] fail to get ls manager for role validation",
                 K(tmp_ret), K_(target_ls_id));
  } else if (OB_ISNULL(mgr)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "[IVF_CACHE_REWARM] null ls manager for role validation",
                 K_(target_ls_id));
  } else {
    bool is_leader = false;
    bool cache_ready = false;
    int64_t role_epoch = 0;
    mgr->get_ivf_cache_role_state(is_leader, cache_ready, role_epoch);
    current = is_leader == expect_leader && role_epoch == role_epoch_;
  }
  return current;
}

int ObIvfCacheRewarmTask::rewarm_ls_()
{
  int ret = OB_SUCCESS;
  int64_t collections_total = 0;
  int64_t collections_skipped = 0;
  int64_t collections_rewarmed = 0;
  int64_t databases_scanned = 0;
  const int64_t start_us = ObTimeUtility::current_time();
  DEBUG_SYNC(HANDLE_IVF_CACHE_REWARM_TASK);
  for (int64_t i = 0; OB_SUCC(ret) && i < sdk_db_names_.count(); ++i) {
    if (is_task_stopped_()) {
      ret = OB_CANCELED;
    } else if (!is_role_epoch_current_(true)) {
      ret = OB_NOT_MASTER;
    } else {
      sdk_db_name_ = sdk_db_names_.at(i);
      if (OB_FAIL(rewarm_database_(collections_total, collections_skipped, collections_rewarmed))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to rewarm database", KR(ret), K_(sdk_db_name), K_(target_ls_id));
      } else {
        ++databases_scanned;
      }
    }
  }
  LOG_INFO("[IVF_CACHE_REWARM] ls rewarm done", KR(ret), K_(tenant_id), K_(target_ls_id),
           "databases_total", sdk_db_names_.count(), K(databases_scanned),
           K(collections_total), K(collections_skipped), K(collections_rewarmed),
           "cost_us", ObTimeUtility::current_time() - start_us);
  return ret;
}

int ObIvfCacheRewarmTask::rewarm_database_(
    int64_t &ls_collections_total,
    int64_t &ls_collections_skipped,
    int64_t &ls_collections_rewarmed)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] vec service is null", KR(ret));
  } else if (OB_UNLIKELY(sdk_db_name_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] sdk database name is empty", KR(ret));
  } else {
    int64_t collections_total = 0;
    int64_t collections_skipped = 0;
    int64_t collections_rewarmed = 0;
    int first_collection_error = OB_SUCCESS;
    const int64_t start_us = ObTimeUtility::current_time();
    LOG_INFO("[IVF_CACHE_REWARM] start database rewarm", K_(tenant_id), K_(target_ls_id), K_(sdk_db_name));

    // One arena per collection loop body; partition names are pushed into the task
    // member (deep copied) before each warmup_collection_() call, so the arena only
    // needs to outlive a single iteration.
    ObArenaAllocator arena(ObMemAttr(tenant_id_, "IvfLsRewarm"));
    ObSqlString collections_sql;
    ObString escaped_sdk_db_name;
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            arena, sdk_db_name_, escaped_sdk_db_name, false/*is_oracle_mode*/))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to escape sdk database name", KR(ret));
    } else if (OB_FAIL(collections_sql.assign_fmt(
            "SELECT collection_id FROM `%.*s`.sdk_collections",
            escaped_sdk_db_name.length(), escaped_sdk_db_name.ptr()))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to build collections sql", KR(ret));
    }

    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id_, collections_sql.ptr()))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to query sdk_collections", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[IVF_CACHE_REWARM] null result", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(ret = result->next())) {
          if (!is_role_epoch_current_(true)) {
            LOG_INFO("[IVF_CACHE_REWARM] ls is no longer leader, abort rewarm",
                     K_(tenant_id), K_(target_ls_id));
            ret = OB_NOT_MASTER;
            break;
          }
          if (is_task_stopped_()) {
            ret = OB_CANCELED;
            break;
          }
          collections_total++;
          arena.reuse();
          ObString collection_id;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "collection_id", collection_id);
          if (OB_FAIL(ret)) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to extract collection_id", KR(ret));
            break;
          }

          // Fetch the hot_table schema of this collection; skip collections without one.
          ObSchemaGetterGuard schema_guard;
          const ObTableSchema *hot_table_schema = nullptr;
          ObSqlString hot_table_name;
          if (OB_FAIL(hot_table_name.assign_fmt("%.*s_hot_table",
                  collection_id.length(), collection_id.ptr()))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to build hot table name",
                     KR(ret), K(collection_id));
            break;
          } else if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[IVF_CACHE_REWARM] schema_service is null", KR(ret));
            break;
          } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                         tenant_id_, schema_guard))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to get schema guard", KR(ret));
            break;
          } else if (OB_FAIL(schema_guard.get_table_schema(
                         tenant_id_, sdk_db_name_,
                         ObString(hot_table_name.length(), hot_table_name.ptr()),
                         false/*is_index*/, hot_table_schema))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to get hot_table schema",
                     KR(ret), K(collection_id));
            break;
          } else if (OB_ISNULL(hot_table_schema)) {
            collections_skipped++;
            continue;
          }

          // Per-collection partition scoping on the target LS (pure in-memory).
          ObSEArray<ObString, 16> ls_partition_names;
          ls_partition_names.set_attr(ObMemAttr(tenant_id_, "IvfLsRewPart"));
          if (OB_FAIL(service->calc_my_partition_list_(
                  *hot_table_schema, target_ls_id_, ls_partition_names, arena))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to calc partition list",
                     KR(ret), K(collection_id), K_(target_ls_id));
            break;
          } else if (ls_partition_names.empty()) {
            // This collection has no partition on the target LS, nothing to rebuild.
            collections_skipped++;
            continue;
          }

          // Reuse the collection warmup flow with this task's members swapped in.
          rewarm_allocator_.reuse();
          collection_id_.reset();
          partition_names_.reset();
          if (OB_FAIL(ob_write_string(rewarm_allocator_, collection_id, collection_id_))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to deep copy collection_id", KR(ret));
            break;
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_names.count(); i++) {
            ObString copied;
            if (OB_FAIL(ob_write_string(rewarm_allocator_, ls_partition_names.at(i), copied))) {
              LOG_WARN("[IVF_CACHE_REWARM] fail to deep copy partition name", KR(ret), K(i));
            } else if (OB_FAIL(partition_names_.push_back(copied))) {
              LOG_WARN("[IVF_CACHE_REWARM] fail to push partition name", KR(ret), K(i));
            }
          }
          if (OB_FAIL(ret)) {
            break;
          }
          if (OB_FAIL(warmup_collection_())) {
            LOG_WARN("[IVF_CACHE_REWARM] rewarm collection failed, continue with next",
                     KR(ret), K_(collection_id), K_(target_ls_id));
            if (OB_SUCCESS == first_collection_error) {
              first_collection_error = ret;
            }
            ret = OB_SUCCESS;
          } else {
            collections_rewarmed++;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret) && OB_SUCCESS != first_collection_error) {
          ret = first_collection_error;
        }
      }
    }

    const int64_t cost_us = ObTimeUtility::current_time() - start_us;
    ls_collections_total += collections_total;
    ls_collections_skipped += collections_skipped;
    ls_collections_rewarmed += collections_rewarmed;
    LOG_INFO("[IVF_CACHE_REWARM] database rewarm done", KR(ret), K_(tenant_id), K_(target_ls_id), K_(sdk_db_name),
             K(collections_total), K(collections_skipped), K(collections_rewarmed),
             K(cost_us), "role_epoch_current", is_role_epoch_current_(true));
  }
  return ret;
}

int ObIvfCacheRewarmTask::warmup_collection_()
{
  int ret = OB_SUCCESS;
  int64_t ns_warmed = 0;
  int64_t ns_skipped = 0;
  int64_t ns_failed = 0;
  int first_warmup_error = OB_SUCCESS;
  int64_t total_sql_rows = 0;
  int64_t page_count = 0;
  ObString escaped_sdk_db_name;
  ObString escaped_hot_table_name;
  ObSEArray<ObString, 16> escaped_partition_names;
  escaped_partition_names.set_attr(ObMemAttr(tenant_id_, "IvfEscParts"));
  ObSqlString collection_id_literal;

  // NOTE: the scan SQL is built per page inside the pagination loop below using a
  // single assign_fmt pass. collection_id_/sdk_db_name_/partition names are passed
  // as %.*s data arguments, never baked into a template that is then re-formatted,
  // so user-controlled strings can no longer be interpreted as a printf format
  // string (avoids format-string crash / SQL injection from a 2nd format pass).

  // Resolve centroid info for the current collection.
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *logic_data_table_schema = nullptr;
  const ObTableSchema *centroid_table_schema = nullptr;
  uint64_t centroid_table_id = OB_INVALID_ID;
  ObVectorIndexParam vec_param;
  bool centroid_skip = false;
  ObString logic_data_table_name;
  {
    char buf[256];
    int64_t len = snprintf(buf, sizeof(buf), "%.*s_logic_data_table",
                           collection_id_.length(), collection_id_.ptr());
    if (len < 0 || len >= static_cast<int64_t>(sizeof(buf))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[IVF_CACHE_REWARM] collection_id too long for buffer", KR(ret),
               K(collection_id_), K(len));
    } else if (OB_FAIL(ob_write_string(rewarm_allocator_,
                                       ObString(len, buf), logic_data_table_name))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to build logic_data_table name", KR(ret));
    }
  }

  if (OB_SUCCESS != ret) {
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, sdk_db_name_, logic_data_table_name,
                 false/*is_index*/, logic_data_table_schema))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to get logic_data_table schema", KR(ret),
             K(logic_data_table_name));
  } else if (OB_ISNULL(logic_data_table_schema)) {
    LOG_INFO("[IVF_CACHE_REWARM] logic_data_table not found, skip",
             K_(collection_id), K(logic_data_table_name));
    centroid_skip = true;
  } else if (OB_FAIL(ObIvfCacheWarmupUtil::resolve_centroid_for_collection(
                 tenant_id_, schema_guard, *logic_data_table_schema,
                 centroid_table_id, centroid_table_schema, vec_param, centroid_skip))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to resolve centroid", KR(ret), K_(collection_id));
  }

  if (OB_FAIL(ret)) {
    return ret;
  } else if (centroid_skip) {
    LOG_INFO("[IVF_CACHE_REWARM] collection skipped (no flat IVF centroid or param not ready)",
             K_(collection_id));
    return ret;
  }

  ObSqlString hot_table_name;
  if (OB_FAIL(hot_table_name.assign_fmt("%.*s_hot_table",
          collection_id_.length(), collection_id_.ptr()))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to build hot table name", KR(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 rewarm_allocator_, sdk_db_name_, escaped_sdk_db_name,
                 false/*is_oracle_mode*/))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to escape sdk database name", KR(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 rewarm_allocator_, hot_table_name.string(), escaped_hot_table_name,
                 false/*is_oracle_mode*/))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to escape hot table name", KR(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(collection_id_, collection_id_literal))) {
    LOG_WARN("[IVF_CACHE_REWARM] fail to escape collection id literal", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_names_.count(); ++i) {
    ObString escaped_partition_name;
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            rewarm_allocator_, partition_names_.at(i), escaped_partition_name,
            false/*is_oracle_mode*/))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to escape partition name", KR(ret), K(i));
    } else if (OB_FAIL(escaped_partition_names.push_back(escaped_partition_name))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to save escaped partition name", KR(ret), K(i));
    }
  }

  // Paginated scan: streaming aggregation of ltable_ids per namespace.
  // Use a composite (namespace_id, ltable_id) keyset cursor so a namespace whose
  // ltables straddle a page boundary is not partially skipped; carry the pending
  // namespace's accumulated ltable_ids across pages so each namespace is flushed
  // exactly once when it is complete.
  uint64_t ns_cursor = 0;
  uint64_t lt_cursor = 0;
  bool has_more = true;
  uint64_t cur_ns_id = 0;
  bool has_pending = false;
  ObSEArray<uint64_t, 8> ltable_ids;  // <=100 ltables per ns (<1KB), no set_attr needed
  ObPluginVectorIndexService *vec_service = MTL(ObPluginVectorIndexService *);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(vec_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[IVF_CACHE_REWARM] vec service is null", KR(ret));
  }

  while (OB_SUCC(ret) && has_more && !is_task_stopped_() && is_role_epoch_current_(true)) {
    page_count++;
    // Build the page SQL in a single assign_fmt pass: ns_cursor / lt_cursor are
    // %lu / %lu arguments alongside the %.*s identifier args, so user-controlled
    // strings are never treated as a printf format string.
    ObSqlString page_sql;
    // The per-collection hot table is the durable PREWARM intent.  Only namespaces
    // explicitly marked hot must be rebuilt after leader switch; sdk_ltables is
    // joined only to recover their logical-table ids.  The centroid tablet
    // location check below remains the authoritative target-LS filter.
    if (OB_UNLIKELY(partition_names_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[IVF_CACHE_REWARM] target ls partition list is empty",
               KR(ret), K_(collection_id), K_(target_ls_id));
    } else if (OB_FAIL(page_sql.assign_fmt(
            "SELECT h.namespace_id, l.ltable_id "
            "FROM `%.*s`.`%.*s` PARTITION (",
            escaped_sdk_db_name.length(), escaped_sdk_db_name.ptr(),
            escaped_hot_table_name.length(), escaped_hot_table_name.ptr()))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to build partition sql prefix", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < escaped_partition_names.count(); ++i) {
      if (i > 0 && OB_FAIL(page_sql.append(","))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to append partition delimiter", KR(ret));
      } else if (OB_SUCC(ret)) {
        const ObString &escaped_partition_name = escaped_partition_names.at(i);
        if (OB_FAIL(page_sql.append_fmt(
                "`%.*s`", escaped_partition_name.length(), escaped_partition_name.ptr()))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to append partition name", KR(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(page_sql.append_fmt(
            ") h "
            "INNER JOIN `%.*s`.sdk_ltables l "
            "ON l.collection_id = %.*s AND l.namespace_id = h.namespace_id "
            "INNER JOIN `%.*s`.sdk_namespaces n "
            "ON n.collection_id = %.*s AND n.namespace_id = h.namespace_id "
            "AND LEFT(n.namespace_name, 13) <> '__recyclebin_' "
            "WHERE h.namespace_id > %lu "
            "OR (h.namespace_id = %lu AND l.ltable_id > %lu) "
            "ORDER BY h.namespace_id, l.ltable_id LIMIT %ld",
            escaped_sdk_db_name.length(), escaped_sdk_db_name.ptr(),
            collection_id_literal.length(), collection_id_literal.ptr(),
            escaped_sdk_db_name.length(), escaped_sdk_db_name.ptr(),
            collection_id_literal.length(), collection_id_literal.ptr(),
            ns_cursor, ns_cursor, lt_cursor, SQL_BATCH_SIZE))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to build sql", KR(ret));
    }
    if (OB_FAIL(ret)) {
      break;
    }

    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id_, page_sql.ptr()))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to execute page sql", KR(ret), K(page_sql));
        break;
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[IVF_CACHE_REWARM] null result", KR(ret));
        break;
      }

      int64_t row_count = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        row_count++;
        uint64_t ns_id = 0;
        uint64_t ltable_id = 0;
        if (OB_FAIL(read_compatible_integer_id(*result, "namespace_id", ns_id))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to extract namespace_id", KR(ret));
        } else if (OB_FAIL(read_compatible_integer_id(*result, "ltable_id", ltable_id))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to extract ltable_id", KR(ret));
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to extract fields", KR(ret));
          break;
        }

        // Advance the composite keyset cursor to the last seen row.
        ns_cursor = ns_id;
        lt_cursor = ltable_id;
        if (!has_pending) {
          cur_ns_id = ns_id;
          has_pending = true;
        }

        if (ns_id != cur_ns_id) {
          // namespace changed => the previous namespace is complete, flush it
          if (is_task_stopped_() || !is_role_epoch_current_(true)) {
            break;
          }
          int tmp_ret = OB_SUCCESS;
          ObTabletID centroid_tablet_id;
          ObLSID warmup_ls_id;
          bool ns_skip = false;
          if (OB_TMP_FAIL(ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
                  *centroid_table_schema, cur_ns_id, centroid_tablet_id))) {
            LOG_WARN("[IVF_CACHE_REWARM] fail to calc centroid tablet, skip ns",
                     K(tmp_ret), K(cur_ns_id));
            ns_failed++;
            if (OB_SUCCESS == first_warmup_error) {
              first_warmup_error = tmp_ret;
            }
            ns_skip = true;
          }
          if (OB_SUCCESS == tmp_ret && !ns_skip) {
            bool is_cache_hit = false;
            if (OB_TMP_FAIL(GCTX.location_service_->get(
                    tenant_id_, centroid_tablet_id, 0, is_cache_hit, warmup_ls_id))) {
              LOG_WARN("[IVF_CACHE_REWARM] fail to get ls for centroid tablet, skip ns",
                       K(tmp_ret), K(centroid_tablet_id), K(cur_ns_id));
              ns_failed++;
              if (OB_SUCCESS == first_warmup_error) {
                first_warmup_error = tmp_ret;
              }
              ns_skip = true;
            }
          }
          if (OB_SUCCESS == tmp_ret && !ns_skip && target_ls_id_.is_valid()
              && warmup_ls_id != target_ls_id_) {
            // Leader switch mode: only rebuild namespaces that really live on the
            // target LS; do not trust the earlier partition scoping blindly.
            ns_skipped++;
            ns_skip = true;
          }
          if (OB_SUCCESS == tmp_ret && !ns_skip) {
            ObIvfPrewarmParam param;
            param.namespace_id_ = static_cast<int64_t>(cur_ns_id);
            param.center_table_id_ = centroid_table_id;
            param.center_tablet_id_ = centroid_tablet_id;
            if (OB_TMP_FAIL(param.ltable_ids_.assign(ltable_ids))) {
              LOG_WARN("[IVF_CACHE_REWARM] fail to assign ltable_ids", K(tmp_ret));
              ns_failed++;
              if (OB_SUCCESS == first_warmup_error) {
                first_warmup_error = tmp_ret;
              }
            } else if (OB_TMP_FAIL(vec_service->warmup_ivf_cache_by_ns_ids(
                           warmup_ls_id, param, vec_param))) {
              LOG_WARN("[IVF_CACHE_REWARM] warmup failed for ns, continue",
                       K(tmp_ret), K(cur_ns_id), K(warmup_ls_id));
              ns_failed++;
              if (OB_SUCCESS == first_warmup_error) {
                first_warmup_error = tmp_ret;
              }
            } else {
              ns_warmed++;
            }
          }

          // Reset accumulation for the new namespace
          ltable_ids.reset();
          cur_ns_id = ns_id;
        }

        if (OB_SUCC(ret) && OB_FAIL(ltable_ids.push_back(ltable_id))) {
          LOG_WARN("[IVF_CACHE_REWARM] fail to push ltable_id", KR(ret));
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }

      // Do NOT flush the pending namespace here: if there are more pages it may
      // continue on the next page, so it is carried over and flushed only when a
      // namespace boundary is crossed or after the final page.
      total_sql_rows += row_count;
      has_more = (row_count >= SQL_BATCH_SIZE);
    }
  }

  // Flush the final pending namespace; with no more pages it is now complete.
  if (OB_SUCC(ret) && has_pending && !is_task_stopped_() && is_role_epoch_current_(true)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletID centroid_tablet_id;
    ObLSID warmup_ls_id;
    bool ns_skip = false;
    if (OB_TMP_FAIL(ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
            *centroid_table_schema, cur_ns_id, centroid_tablet_id))) {
      LOG_WARN("[IVF_CACHE_REWARM] fail to calc centroid tablet, skip ns",
               K(tmp_ret), K(cur_ns_id));
      ns_failed++;
      if (OB_SUCCESS == first_warmup_error) {
        first_warmup_error = tmp_ret;
      }
      ns_skip = true;
    }
    if (OB_SUCCESS == tmp_ret && !ns_skip) {
      bool is_cache_hit = false;
      if (OB_TMP_FAIL(GCTX.location_service_->get(
              tenant_id_, centroid_tablet_id, 0, is_cache_hit, warmup_ls_id))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to get ls, skip ns",
                 K(tmp_ret), K(centroid_tablet_id), K(cur_ns_id));
        ns_failed++;
        if (OB_SUCCESS == first_warmup_error) {
          first_warmup_error = tmp_ret;
        }
        ns_skip = true;
      }
    }
    if (OB_SUCCESS == tmp_ret && !ns_skip && target_ls_id_.is_valid()
        && warmup_ls_id != target_ls_id_) {
      ns_skipped++;
      ns_skip = true;
    }
    if (OB_SUCCESS == tmp_ret && !ns_skip) {
      ObIvfPrewarmParam param;
      param.namespace_id_ = static_cast<int64_t>(cur_ns_id);
      param.center_table_id_ = centroid_table_id;
      param.center_tablet_id_ = centroid_tablet_id;
      if (OB_TMP_FAIL(param.ltable_ids_.assign(ltable_ids))) {
        LOG_WARN("[IVF_CACHE_REWARM] fail to assign ltable_ids", K(tmp_ret));
        ns_failed++;
        if (OB_SUCCESS == first_warmup_error) {
          first_warmup_error = tmp_ret;
        }
      } else if (OB_TMP_FAIL(vec_service->warmup_ivf_cache_by_ns_ids(
                     warmup_ls_id, param, vec_param))) {
        LOG_WARN("[IVF_CACHE_REWARM] warmup failed for ns, continue",
                 K(tmp_ret), K(cur_ns_id));
        ns_failed++;
        if (OB_SUCCESS == first_warmup_error) {
          first_warmup_error = tmp_ret;
        }
      } else {
        ns_warmed++;
      }
    }
  }

  LOG_INFO("[IVF_CACHE_REWARM] collection done",
           K_(collection_id), K(ns_warmed), K(ns_skipped), K(ns_failed),
           K(page_count), K(total_sql_rows),
           "stopped", is_task_stopped_());
  if (OB_SUCC(ret) && is_task_stopped_()) {
    ret = OB_CANCELED;
  } else if (OB_SUCC(ret) && !is_role_epoch_current_(true)) {
    ret = OB_NOT_MASTER;
  } else if (OB_SUCC(ret) && OB_SUCCESS != first_warmup_error) {
    // Do not publish a partially rebuilt generation. Queries keep using the
    // persistent fallback until the common task retry succeeds completely.
    ret = first_warmup_error;
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
