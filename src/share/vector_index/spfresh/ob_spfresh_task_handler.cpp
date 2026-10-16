/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_task_handler.h"
#include "share/vector_index/spfresh/ob_spfresh_task_executor.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{

ObSpfreshTaskHandler::InFlightGuard::InFlightGuard(volatile int64_t *counter)
  : counter_(counter)
{
}

ObSpfreshTaskHandler::InFlightGuard::~InFlightGuard()
{
  if (OB_NOT_NULL(counter_)) {
    ATOMIC_DEC(counter_);
  }
}

ObSpfreshTaskHandler::ObSpfreshTaskHandler()
  : pending_retry_lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    pending_lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    is_inited_(false),
    tg_id_(INVALID_TG_ID),
    tenant_id_(OB_INVALID_TENANT_ID),
    executor_(nullptr),
    in_flight_count_(0),
    stopped_(false),
    started_(false)
{
}

ObSpfreshTaskHandler::~ObSpfreshTaskHandler()
{
  if (IS_INIT) {
    destroy();
  }
}

int ObSpfreshTaskHandler::init(const uint64_t tenant_id, ObSpfreshTaskExecutor *executor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("spfresh task handler init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(executor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(executor));
  } else {
    tenant_id_ = tenant_id;
    executor_ = executor;
    is_inited_ = true;
    LOG_INFO("spfresh task handler init done", K(tenant_id));
  }
  return ret;
}

int ObSpfreshTaskHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("spfresh task handler not init", KR(ret));
  } else if (started_) {
    // Idempotent.
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorTaskPool, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed for spfresh task pool", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT, MAX_THREAD_COUNT))) {
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed for spfresh task pool", KR(ret), K_(tg_id));
    TG_DESTROY(tg_id_);
    tg_id_ = INVALID_TG_ID;
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed for spfresh task pool", KR(ret), K_(tg_id));
    TG_DESTROY(tg_id_);
    tg_id_ = INVALID_TG_ID;
  } else {
    ATOMIC_STORE(&stopped_, false);
    started_ = true;
    LOG_INFO("spfresh task handler started", K_(tg_id), K(MIN_THREAD_COUNT), K(MAX_THREAD_COUNT));
  }
  return ret;
}

void ObSpfreshTaskHandler::stop()
{
  if (!started_) {
    return;
  }
  LOG_INFO("spfresh task handler start to stop", K_(tg_id));
  ATOMIC_STORE(&stopped_, true);
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("spfresh task handler finish to stop", K_(tg_id));
}

void ObSpfreshTaskHandler::wait()
{
  if (!started_) {
    return;
  }
  LOG_INFO("spfresh task handler start to wait", K_(tg_id), K(ATOMIC_LOAD(&in_flight_count_)));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
  LOG_INFO("spfresh task handler finish to wait", K_(tg_id), K(ATOMIC_LOAD(&in_flight_count_)));
}

void ObSpfreshTaskHandler::destroy()
{
  lib::ObMutexGuard retry_guard(pending_retry_lock_);
  LOG_INFO("spfresh task handler start to destroy", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  // The executor drains dispatches before calling destroy. The retry mutex
  // also protects direct retry calls and executor_ until cleanup finishes.
  PendingTask *tasks = detach_pending_tasks_();
  while (nullptr != tasks) {
    PendingTask *task = tasks;
    tasks = task->next_;
    if (nullptr != executor_) {
      int ret = executor_->unmark_in_flight_(task->task_key_);
      if (OB_SUCCESS != ret) {
        LOG_ERROR("failed to release retained task on destroy", K(ret), K(task->task_key_));
      }
    }
    task->~PendingTask();
    ob_free(task);
  }
  tg_id_ = INVALID_TG_ID;
  executor_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ATOMIC_STORE(&in_flight_count_, 0);
  ATOMIC_STORE(&stopped_, false);
  started_ = false;
  is_inited_ = false;
  LOG_INFO("spfresh task handler finish to destroy");
}

int ObSpfreshTaskHandler::reserve_in_flight_slot_(int64_t max_in_flight)
{
  int ret = OB_SUCCESS;
  int64_t current_count = ATOMIC_LOAD(&in_flight_count_);
  bool reserved = false;

  if (0 >= max_in_flight) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max in-flight count", K(ret), K(max_in_flight));
  } else {
    while (OB_SUCC(ret) && !reserved && current_count < max_in_flight) {
      reserved = ATOMIC_BCAS(&in_flight_count_, current_count, current_count + 1);
      if (!reserved) {
        current_count = ATOMIC_LOAD(&in_flight_count_);
        PAUSE();
      }
    }
    if (!reserved) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObSpfreshTaskHandler::push_task(
    const ObSpfreshTask &task,
    int64_t max_in_flight)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("spfresh task handler not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid spfresh task", KR(ret), K(task));
  } else if (ATOMIC_LOAD(&stopped_)) {
    // Pool is stopping; reject new task. Caller (process_queue) will push task back to queue.
    ret = OB_IN_STOP_STATE;
    LOG_WARN("spfresh task handler is stopped, reject push", KR(ret), K(task));
  } else if (!started_) {
    ret = OB_NOT_INIT;
    LOG_WARN("spfresh task handler not started", KR(ret), K(task));
  } else if (OB_FAIL(reserve_in_flight_slot_(max_in_flight))) {
    if (OB_SIZE_OVERFLOW == ret) {
      LOG_TRACE("[SPFRESH_TRACE] spfresh in-flight limit reached",
                K(max_in_flight), K(ATOMIC_LOAD(&in_flight_count_)));
    } else {
      LOG_WARN("failed to reserve spfresh in-flight slot", K(ret), K(max_in_flight));
    }
  } else {
    void *buf = ob_malloc(sizeof(PendingTask), lib::ObMemAttr(tenant_id_, "SpfreshTaskHdl"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ATOMIC_DEC(&in_flight_count_);
      LOG_WARN("failed to allocate spfresh task copy", KR(ret));
    } else {
      PendingTask *task_copy = new (buf) PendingTask(task);
      if (OB_FAIL(TG_PUSH_TASK(tg_id_, task_copy))) {
        LOG_WARN("fail to TG_PUSH_TASK for spfresh task", KR(ret), K(task));
        ATOMIC_DEC(&in_flight_count_);
        task_copy->~PendingTask();
        ob_free(buf);
      } else {
        LOG_TRACE("[SPFRESH_TRACE] spfresh task pushed to handler",
                  K(task), K(ATOMIC_LOAD(&in_flight_count_)));
      }
    }
  }
  return ret;
}

void ObSpfreshTaskHandler::retry_pending_tasks()
{
  lib::ObMutexGuard retry_guard(pending_retry_lock_);
  PendingTask *tasks = detach_pending_tasks_();
  PendingTask *failed_head = nullptr;
  PendingTask *failed_tail = nullptr;
  while (nullptr != tasks) {
    PendingTask *task = tasks;
    tasks = task->next_;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(executor_)) {
      ret = OB_NOT_INIT;
    } else {
      ret = executor_->requeue_in_flight_task_(*task);
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("retained spfresh task still awaiting requeue", K(ret), K(task->task_key_));
      task->next_ = nullptr;
      if (nullptr == failed_head) {
        failed_head = task;
      } else {
        failed_tail->next_ = task;
      }
      failed_tail = task;
    } else {
      task->~PendingTask();
      ob_free(task);
    }
  }
  if (nullptr != failed_head) {
    common::ObSpinLockGuard guard(pending_lock_);
    failed_tail->next_ = pending_tasks_;
    pending_tasks_ = failed_head;
  }
}

ObSpfreshTaskHandler::PendingTask *ObSpfreshTaskHandler::detach_pending_tasks_()
{
  common::ObSpinLockGuard guard(pending_lock_);
  PendingTask *tasks = pending_tasks_;
  pending_tasks_ = nullptr;
  return tasks;
}

void ObSpfreshTaskHandler::handle(void *task)
{
  common::ObCurTraceId::TraceId trace_id;
  trace_id.init(GCONF.self_addr_);
  common::ObTraceIdGuard trace_guard(trace_id);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null task in handle", KR(ret));
  } else {
    PendingTask *spfresh_task = static_cast<PendingTask *>(task);
    const ObSpfreshTaskKey task_key = spfresh_task->task_key_;
    int exec_ret = OB_SUCCESS;
    bool owns_in_flight = true;
    bool retained = false;
    // Decrement in_flight_count_ on every error/exit path.
    InFlightGuard guard(&in_flight_count_);
    if (OB_ISNULL(executor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spfresh task executor is null", KR(ret));
    } else {
      exec_ret = executor_->process_one_task_(*spfresh_task);
      if (OB_EAGAIN == exec_ret) {
        // End this handler turn immediately. REASSIGN is restored to its
        // dedicated delayed queue, so waiting for old transactions never
        // sleeps or spins in a worker thread.
        int retry_ret = executor_->requeue_in_flight_task_(*spfresh_task);
        if (OB_SUCCESS != retry_ret) {
          LOG_ERROR_RET(retry_ret, "failed to requeue pending spfresh task", K(retry_ret),
                        K(*spfresh_task));
          // Keep the existing allocation and in-flight key. The timer retries
          // the same atomic requeue operation, including its REASSIGN delay.
          common::ObSpinLockGuard pending_guard(pending_lock_);
          spfresh_task->next_ = pending_tasks_;
          pending_tasks_ = spfresh_task;
          retained = true;
        } else {
          owns_in_flight = false;
          LOG_TRACE("[SPFRESH_TRACE] pending spfresh task requeued",
                    K(*spfresh_task));
        }
      } else if (OB_SUCCESS != exec_ret) {
        LOG_WARN("spfresh task execute failed", K(exec_ret), K(*spfresh_task));
      } else {
        LOG_TRACE("[SPFRESH_TRACE] spfresh task handled by handler", K(*spfresh_task));
      }
    }
    // Retained tasks remain owned by the pending list. The guard still
    // releases the worker slot when this handler turn ends.
    if (!retained) {
      spfresh_task->~PendingTask();
      ob_free(spfresh_task);
    }
    // Release the in-flight key AFTER execution so process_queue never
    // dispatches a same-key task concurrently with this one.
    if (!retained && owns_in_flight && OB_NOT_NULL(executor_)) {
      int unmark_ret = executor_->unmark_in_flight_(task_key);
      if (OB_SUCCESS != unmark_ret) {
        LOG_ERROR_RET(unmark_ret, "failed to release spfresh in-flight key", K(unmark_ret), K(task_key));
      }
    }
  }
}

void ObSpfreshTaskHandler::handle_drop(void *task)
{
  if (OB_NOT_NULL(task)) {
    PendingTask *spfresh_task = static_cast<PendingTask *>(task);
    const ObSpfreshTaskKey task_key = spfresh_task->task_key_;
    spfresh_task->~PendingTask();
    ob_free(spfresh_task);
    ATOMIC_DEC(&in_flight_count_);
    if (OB_NOT_NULL(executor_)) {
      int unmark_ret = executor_->unmark_in_flight_(task_key);
      if (OB_SUCCESS != unmark_ret) {
        LOG_ERROR_RET(unmark_ret, "failed to release spfresh in-flight key", K(unmark_ret), K(task_key));
      }
    }
    LOG_INFO("spfresh task dropped without execution on pool stop", K(task_key));
  }
}

}  // namespace share
}  // namespace oceanbase
