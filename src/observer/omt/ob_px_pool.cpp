/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_px_pool.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/ob_define.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/worker.h"
#include "share/config/ob_server_config.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/engine/px/ob_px_worker.h"

#if defined(__linux__)
#include <sys/resource.h>
#endif

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::share;

int ObPxPools::init(uint64_t tenant_id)
{
  static int PX_POOL_COUNT = 128; // 128 groups, generally enough
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  ObMemAttr attr(tenant_id, "PxPoolBkt");
  if (OB_FAIL(pool_map_.create(PX_POOL_COUNT, attr, attr))) {
    LOG_WARN("fail init pool map", K(ret));
  }
  return ret;
}

int ObPxPools::get_or_create(int64_t group_id, ObPxPool *&pool)
{
  int ret = OB_SUCCESS;
  if (!pool_map_.created()) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_pool(group_id, pool))) {
        LOG_WARN("fail create pool", K(ret), K(group_id));
      }
    } else {
      LOG_WARN("fail get group id from hashmap", K(ret), K(group_id));
    }
  }
  return ret;
}

int ObPxPools::create_pool(int64_t group_id, ObPxPool *&pool)
{
  static constexpr uint64_t MAX_TASKS_PER_CPU = 1;
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  if (OB_FAIL(pool_map_.get_refactored(group_id, pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      pool = OB_NEW(ObPxPool, ObMemAttr(tenant_id_, "PxPool"));
      if (OB_ISNULL(pool)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        pool->set_tenant_id(tenant_id_);
        pool->set_group_id(group_id);
        if (OBCG_DEFAULT == group_id) {
          pool->set_thread_group_id(OB_THREAD_GROUP_PX);
        }
        pool->set_run_wrapper(MTL_CTX());
        if (OB_FAIL(pool->start())) {
          LOG_WARN("fail startup px pool", K(group_id), K(tenant_id_), K(ret));
        } else if (OB_FAIL(pool_map_.set_refactored(group_id, pool))) {
          LOG_WARN("fail set pool to hashmap", K(group_id), K(ret));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(pool)) {
        pool->destroy();
        common::ob_delete(pool);
        pool = nullptr;
      }
    } else {
      LOG_WARN("fail get group id from hashmap", K(ret), K(group_id));
    }
  }
  return ret;
}

int ObPxPools::thread_recycle()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  ThreadRecyclePoolFunc recycle_pool_func;
  if (OB_FAIL(pool_map_.foreach_refactored(recycle_pool_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  }
  return ret;
}

int ObPxPools::ThreadRecyclePoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    IGNORE_RETURN pool->thread_recycle();
  }
  return ret;
}

int ObPxPools::StopPoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    pool->stop();
    LOG_INFO("DEL_POOL_STEP_1: mark px pool stop succ!", K(group_id));
  }
  return ret;
}

int ObPxPools::DeletePoolFunc::operator() (common::hash::HashMapPair<int64_t, ObPxPool*> &kv)
{
  int ret = OB_SUCCESS;
  int64_t &group_id = kv.first;
  ObPxPool *pool = kv.second;
  if (NULL == pool) {
    LOG_WARN("pool is null", K(group_id));
  } else {
    pool->wait();
    LOG_INFO("DEL_POOL_STEP_2: wait pool empty succ!", K(group_id));
    pool->destroy();
    LOG_INFO("DEL_POOL_STEP_3: pool destroy succ!", K(group_id), K(pool->get_queue_size()));
    common::ob_delete(pool);
  }
  return ret;
}

void ObPxPools::mtl_stop(ObPxPools *&pools)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pools)) {
    // ignore ret
    // pools will be null if it's creating tenant and failed.
    LOG_WARN("pools is null");
  } else {
    common::SpinWLockGuard g(pools->lock_);
    StopPoolFunc stop_pool_func;
    if (OB_FAIL(pools->pool_map_.foreach_refactored(stop_pool_func))) {
      LOG_WARN("failed to do foreach", K(ret));
    }
  }
}

void ObPxPools::destroy()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  DeletePoolFunc free_pool_func;
  if (OB_FAIL(pool_map_.foreach_refactored(free_pool_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    pool_map_.destroy();
    tenant_id_ = OB_INVALID_ID;
  }
}

int ObPxPool::submit(const RunFuncT &func)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
    is_inited_ = true;
  }
  disable_recycle();
  ATOMIC_INC(&concurrency_);
  if (ATOMIC_LOAD(&active_threads_) < ATOMIC_LOAD(&concurrency_)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    Task *t = OB_NEW(Task, ObMemAttr(tenant_id_, "PxTask"), func);
    if (OB_ISNULL(t)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(queue_.push(static_cast<ObLink*>(t), 0))) {
      LOG_ERROR("px push queue failed", K(ret));
    }
  }
  if (ret != OB_SUCCESS) {
    ATOMIC_DEC(&concurrency_);
  }
  enable_recycle();
  return ret;
}

int ObPxPool::expand_thread_count(int64_t task_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task count for expanding px pool", K(ret), K(task_count));
  } else if (!ATOMIC_BCAS(&is_expanding_, false, true)) {
    // Another thread is expanding the pool. Let the caller retry after the existing 5 ms backoff.
    LOG_TRACE("px pool expansion is already in progress", K(task_count),
              K_(tenant_id), K_(group_id));
  } else {
    int64_t concurrency = 0;
    int64_t active_thread_count = 0;
    int64_t pool_thread_count = 0;
    disable_recycle();
    concurrency = ATOMIC_LOAD(&concurrency_);
    active_thread_count = ATOMIC_LOAD(&active_threads_);
    pool_thread_count = get_pool_size();
    enable_recycle();
    if (OB_UNLIKELY(concurrency > INT64_MAX - task_count)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("target px thread count overflow", K(ret), K(concurrency), K(task_count));
    } else {
      const int64_t target_thread_count = concurrency + task_count;
      const int64_t expand_count = std::max(target_thread_count - active_thread_count, 0L);
      int64_t expanded_count = 0;
      while (OB_SUCC(ret) && expanded_count < expand_count) {
        if (OB_FAIL(inc_thread_count(1))) {
          LOG_WARN("fail to expand px pool", K(ret), K(expand_count), K(expanded_count),
                   K(concurrency), K(active_thread_count), K(pool_thread_count), K(task_count),
                   K_(tenant_id), K_(group_id));
        } else {
          ++expanded_count;
        }
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("expand px pool", K(expand_count), K(concurrency), K(active_thread_count),
                  K(pool_thread_count), K(task_count), K(target_thread_count),
                  K_(tenant_id), K_(group_id));
      }
    }
    ATOMIC_STORE(&is_expanding_, false);
  }
  return ret;
}

void ObPxPool::handle(ObLink *task)
{
  Task *t  = static_cast<Task*>(task);
  if (t == nullptr) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "px task is invalid");
  } else {
    bool need_exec = true;
    t->func_(need_exec);
    OB_DELETE(Task, "PxTask", t);
  }
  ATOMIC_DEC(&concurrency_);
}

void ObPxPool::set_px_thread_name()
{
  char buf[32];
  snprintf(buf, 32, "PX_G%ld", group_id_);
  ob_get_tenant_id() = tenant_id_;
  lib::set_thread_name(buf);
}

void ObPxPool::apply_px_worker_thread_sched_priority()
{
#if defined(__linux__)
  // Linux per-thread nice: setpriority(PRIO_PROCESS, tid, prio) adjusts the thread identified by
  // tid (see gettid(2)); naming is historical. who==0 also means the calling thread.
  static constexpr int PX_WORKER_THREAD_NICE = 10;
  const id_t tid = static_cast<id_t>(ob_gettid());
  if (0 != setpriority(PRIO_PROCESS, tid, PX_WORKER_THREAD_NICE)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "setpriority failed for px worker thread", K(errno), K(tid), K(PX_WORKER_THREAD_NICE));
  }
#endif
}

void ObPxPool::run(int64_t idx)
{
  ATOMIC_INC(&active_threads_);
  set_thread_idx(idx);
  // Create worker for current thread.
  ObPxWorker worker;
  Worker::set_worker_to_thread_local(&worker);
  run1();
}


void ObPxPool::run1()
{
  int ret = OB_SUCCESS;
  ObTLTaGuard ta_guard(tenant_id_);
  common::ObBackGroundSessionGuard backgroud_session_guard(tenant_id_, group_id_);
  ObDIActionGuard action_guard("PxPool", "PxWorker", "");
  set_px_thread_name();
  //ObTaTLCacheGuard ta_guard(tenant_id_);
  CLEAR_INTERRUPTABLE();
  ObCgroupCtrl *cgroup_ctrl = GCTX.cgroup_ctrl_;
  LOG_INFO("run px pool", K(group_id_), K(tenant_id_), K_(active_threads));
  SET_GROUP_ID(group_id_);
  apply_px_worker_thread_sched_priority();

	if (!is_inited_) {
    queue_.set_limit(common::ObServerConfig::get_instance().tenant_task_queue_size);
    is_inited_ = true;
  }

  ObLink *task = nullptr;
  int64_t idle_time = 0;
  while (!Thread::current().has_set_stop()) {
	  if (!is_inited_) {
      ob_usleep(10 * 1000L);
    } else {
      if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
        handle(task);
        idle_time = 0; // reset recycle timer
      } else {
        idle_time += QUEUE_WAIT_TIME;
        // if idle for more than 10 min, exit thread
        try_recycle(idle_time);
      }
    }
  }
}

void ObPxPool::try_recycle(int64_t idle_time)
{
  // recycle thread policy:
  // 1. first N threads reserved for first 10 min idle period
  // 2. no thread reserved after 1 hour idle period
  //
  // impl. note: must ensure active_threads_ > concurrency_, otherwise may hang task
  const int N = 8;
  if ((idle_time > 10LL * 60 * 1000 * 1000 && get_thread_count() >= N)
      || idle_time > 60LL * 60 * 1000 * 1000) {
    if (OB_SUCCESS == recycle_lock_.trylock()) {
      if (ATOMIC_LOAD(&active_threads_) > ATOMIC_LOAD(&concurrency_)) {
        ATOMIC_DEC(&active_threads_);
        // when thread marked as stopped,
        // it will exit the event loop and recycled by background deamon
        Thread::current().stop();
      }
      recycle_lock_.unlock();
    }
  }
}

void ObPxPool::stop()
{
  int ret = OB_SUCCESS;
  Threads::stop();
  ObLink *task = nullptr;
  bool need_exec = false;
  while (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
    Task *t  = static_cast<Task*>(task);
    if (OB_NOT_NULL(t)) {
      t->func_(need_exec);
      OB_DELETE(Task, "PxTask", t);
    }
    ATOMIC_DEC(&concurrency_);
  }
}
