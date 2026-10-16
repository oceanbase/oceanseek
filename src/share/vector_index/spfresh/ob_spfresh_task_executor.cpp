/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_task_executor.h"
#include "share/vector_index/spfresh/ob_spfresh_policy.h"
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_ivf_cache_warmup_util.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/location_cache/ob_location_service.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_sort.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_sql_mem_leak_checker.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/container/ob_se_array.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{

ObSpfreshTaskExecutor::ObSpfreshDistributionSnapshot::ObSpfreshDistributionSnapshot()
  : total_count_(0),
    normal_center_count_(0),
    cids_(),
    counts_(),
    split_retry_after_us_()
{
}

void ObSpfreshTaskExecutor::ObSpfreshDistributionSnapshot::reset()
{
  total_count_ = 0;
  normal_center_count_ = 0;
  cids_.reset();
  counts_.reset();
  split_retry_after_us_.reset();
}

int ObSpfreshTaskExecutor::ObSpfreshDistributionSnapshot::add_normal_center(
    int64_t cid,
    int64_t count,
    int64_t split_retry_after_us)
{
  int ret = OB_SUCCESS;
  total_count_ += count;
  ++normal_center_count_;
  if (OB_FAIL(cids_.push_back(cid))) {
    LOG_WARN("failed to push cid", K(ret), K(cid));
  } else if (OB_FAIL(counts_.push_back(count))) {
    LOG_WARN("failed to push count", K(ret), K(cid), K(count));
  } else if (OB_FAIL(split_retry_after_us_.push_back(split_retry_after_us))) {
    LOG_WARN("failed to push retry time", K(ret), K(cid), K(split_retry_after_us));
  }
  return ret;
}

ObSpfreshTaskExecutor::ObSpfreshSplitScheduleResult::ObSpfreshSplitScheduleResult()
  : submitted_count_(0),
    scheduled_count_(0)
{
}

ObSpfreshTaskExecutor::ObSpfreshTaskContext::ObSpfreshTaskContext()
  : queue_(nullptr),
    sql_proxy_(nullptr),
    kv_info_(),
    queue_tablet_id_(OB_INVALID_ID),
    physical_tablet_id_(),
    physical_ls_id_(),
    map_cache_(nullptr),
    dist_algo_(ObVectorIndexDistAlgorithm::VIDA_L2)
{
}

bool ObSpfreshTaskExecutor::ObSpfreshSplitCandidateCmp::operator()(
    const ObSpfreshSplitCandidate &left,
    const ObSpfreshSplitCandidate &right) const
{
  return left.count_ > right.count_
      || (left.count_ == right.count_ && left.center_id_ < right.center_id_);
}

// ObSpfreshTaskQueueMap implementation
ObSpfreshTaskQueueMap::ObSpfreshTaskQueueMap()
  : is_inited_(false),
    lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_MAP_LOCK),
    queue_map_()
{
}

ObSpfreshTaskQueueMap::~ObSpfreshTaskQueueMap()
{
  destroy();
}

int ObSpfreshTaskQueueMap::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshTaskQueueMap init twice", K(ret));
  } else if (OB_FAIL(queue_map_.create(1024, lib::ObMemAttr(OB_SERVER_TENANT_ID, "SpfreshQMap")))) {
    LOG_WARN("failed to create queue map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSpfreshTaskQueueMap::destroy()
{
  if (is_inited_) {
    common::ObSpinLockGuard guard(lock_);
    // Destroy all queues
    for (hash::ObHashMap<uint64_t, ObSpfreshTaskQueue *>::iterator it = queue_map_.begin();
          it != queue_map_.end(); ++it) {
      if (OB_NOT_NULL(it->second)) {
        it->second->destroy();
        ob_free(it->second);
      }
    }
    queue_map_.destroy();
    is_inited_ = false;
    LOG_INFO("Task queue map destroyed");
  }
}

int ObSpfreshTaskQueueMap::get_queue(uint64_t tablet_id, ObSpfreshTaskQueue *&queue)
{
  int ret = OB_SUCCESS;
  queue = nullptr;
  // Disable SQL memory leak detection for long-lived queue allocation
  DISABLE_SQL_MEMLEAK_GUARD;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueueMap not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObSpfreshTaskQueue *tmp_queue = nullptr;
    int hash_ret = queue_map_.get_refactored(tablet_id, tmp_queue);
    if (OB_SUCCESS == hash_ret) {
      queue = tmp_queue;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      // Create new queue
      void *buf = ob_malloc(sizeof(ObSpfreshTaskQueue), "SpfreshQueue");
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for queue", K(ret));
      } else {
        tmp_queue = new (buf) ObSpfreshTaskQueue();
        if (OB_FAIL(tmp_queue->init())) {
          LOG_WARN("failed to init queue", K(ret));
          tmp_queue->~ObSpfreshTaskQueue();
          ob_free(buf);
        } else if (OB_FAIL(queue_map_.set_refactored(tablet_id, tmp_queue))) {
          LOG_WARN("failed to add queue to map", K(ret));
          tmp_queue->destroy();
          tmp_queue->~ObSpfreshTaskQueue();
          ob_free(buf);
        } else {
          queue = tmp_queue;
        }
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get queue from map", K(ret), K(tablet_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] get_queue done", K(tablet_id), KR(ret));
  return ret;
}

int ObSpfreshTaskQueueMap::snapshot_queues(
    common::ObIArray<uint64_t> &queue_tablet_ids, int64_t &queue_count)
{
  int ret = OB_SUCCESS;
  queue_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueueMap not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    hash::ObHashMap<uint64_t, ObSpfreshTaskQueue *>::iterator it = queue_map_.begin();
    for (; OB_SUCC(ret) && it != queue_map_.end(); ++it) {
      queue_count++;
      if (OB_ISNULL(it->second)) {
        LOG_TRACE("[SPFRESH_TRACE] queue is null", K(it->first));
      } else if (OB_FAIL(queue_tablet_ids.push_back(it->first))) {
        LOG_WARN("failed to snapshot queue tablet id", K(ret), K(it->first));
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] snapshot_queues done", K(queue_tablet_ids), K(queue_count), KR(ret));
  return ret;
}

int ObSpfreshTaskQueueMap::remove_queue(uint64_t tablet_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueueMap not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObSpfreshTaskQueue *queue = nullptr;
    int hash_ret = queue_map_.get_refactored(tablet_id, queue);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // Queue not found, this is OK
      ret = OB_HASH_NOT_EXIST;
      LOG_TRACE("[SPFRESH_TRACE] Queue not found for tablet", K(tablet_id));
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to get queue from map", K(ret), K(tablet_id));
    } else if (OB_ISNULL(queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("queue is null", K(ret), K(tablet_id));
    } else {
      // Destroy and free the queue
      queue->destroy();
      ob_free(queue);
      // Remove from map
      if (OB_FAIL(queue_map_.erase_refactored(tablet_id))) {
        LOG_WARN("failed to erase queue from map", K(ret), K(tablet_id));
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] remove_queue done", K(tablet_id), KR(ret));
  return ret;
}

// ObSpfreshTaskExecutor implementation
ObSpfreshTaskExecutor::ObSpfreshTaskExecutor()
  : ObVecITaskExecutor(),
    dispatch_lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    dispatch_cond_(common::ObWaitEventIds::COND_WAIT_FOR_BASE_CLASS),
    queue_map_(),
    handler_(),
    handler_start_lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    thread_cnt_(0),
    task_trace_base_num_(0)
{
}

ObSpfreshTaskExecutor::~ObSpfreshTaskExecutor()
{
}

int ObSpfreshTaskExecutor::init(const uint64_t tenant_id)
{
  lib::ObMutexGuard guard(dispatch_lock_);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshTaskExecutor init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    // Note: Do NOT call base class init() because it requires ls to be non-null.
    // SpfreshTaskExecutor does not need ls, so we set member variables directly.
    tenant_id_ = tenant_id;
    vector_index_service_ = MTL(ObPluginVectorIndexService *);
    if (OB_ISNULL(vector_index_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector_index_service is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(queue_map_.init())) {
      LOG_WARN("failed to init queue map", K(ret));
    } else if (OB_FAIL(handler_.init(tenant_id, this))) {
      LOG_WARN("failed to init spfresh task handler", K(ret), K(tenant_id));
    } else {
      is_dispatch_stopped_ = false;
      is_inited_ = true;
    }
  }
  LOG_TRACE("[SPFRESH_TRACE] ObSpfreshTaskExecutor init done", K(tenant_id), KR(ret));
  return ret;
}

int ObSpfreshTaskExecutor::init(
    const uint64_t tenant_id, storage::ObLSHandle &ls_handle)
{
  UNUSED(ls_handle);
  return init(tenant_id);
}

int ObSpfreshTaskExecutor::load_triggered_task(
    const ObVecIndexTaskStatus &task_row)
{
  UNUSED(task_row);
  return OB_NOT_SUPPORTED;
}

int ObSpfreshTaskExecutor::ensure_handler_started_()
{
  // Serialize lazy start with stop. An admitted round may reach here after
  // stop has closed dispatch admission and must not restart the worker pool.
  lib::ObMutexGuard guard(handler_start_lock_);
  int ret = OB_SUCCESS;
  bool is_dispatch_stopped = false;
  {
    lib::ObMutexGuard dispatch_guard(dispatch_lock_);
    is_dispatch_stopped = is_dispatch_stopped_;
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskExecutor not init", K(ret));
  } else if (is_dispatch_stopped) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("SPFresh dispatch stopped before handler start", K(ret));
  } else if (handler_.is_started()) {
    // Handler already running.
  } else {
    // Recover before starting workers so recovered tasks can be consumed.
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = recover_stuck_tasks_())) {
      LOG_WARN("failed to recover stuck tasks before handler start", K(tmp_ret));
    }
    if (OB_FAIL(handler_.start())) {
      LOG_WARN("failed to start spfresh task handler", K(ret));
    } else {
      LOG_INFO("ObSpfreshTaskExecutor handler lazy-started", K(tenant_id_), K(MAX_CONCURRENT));
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::start()
{
  DispatchGuard guard(*this);
  int ret = OB_SUCCESS;
  if (!guard.is_entered()) {
    ret = OB_NOT_RUNNING;
  } else {
    ret = ensure_handler_started_();
  }
  return ret;
}

void ObSpfreshTaskExecutor::stop()
{
  {
    lib::ObMutexGuard guard(dispatch_lock_);
    is_dispatch_stopped_ = true;
  }
  // Wait for a competing lazy start, then stop the pool during the stop
  // phase. Already admitted dispatches will fail publication and requeue.
  lib::ObMutexGuard start_guard(handler_start_lock_);
  handler_.stop();
}

void ObSpfreshTaskExecutor::wait()
{
  stop();
  wait_dispatches_();
  if (IS_INIT) {
    handler_.wait();
    LOG_INFO("ObSpfreshTaskExecutor wait done", K(tenant_id_));
  }
}

void ObSpfreshTaskExecutor::destroy()
{
  // LS timers can outlive this service's resources. Close admission and drain
  // the complete scan/dispatch round before releasing queues or service maps.
  wait();
  if (IS_INIT) {
    handler_.destroy();
    queue_map_.destroy();
    is_inited_ = false;
    LOG_INFO("ObSpfreshTaskExecutor destroyed", K(tenant_id_));
  }
}

int ObSpfreshTaskExecutor::pop_and_mark_in_flight_(
    ObSpfreshTaskQueue &queue,
    ObSpfreshTask &task,
    bool &marked)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue.pop_and_mark_in_flight(task, marked))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to pop and mark task", K(ret));
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::unmark_in_flight_(const ObSpfreshTaskKey &key)
{
  int ret = OB_SUCCESS;
  ObSpfreshTaskQueue *queue = nullptr;
  if (OB_FAIL(queue_map_.get_queue(key.centroid_tablet_id_, queue))) {
    LOG_WARN("failed to get queue for unmark", K(ret), K(key));
  } else if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queue is null for unmark", K(ret), K(key));
  } else if (OB_FAIL(queue->unmark_in_flight(key))) {
    LOG_WARN("failed to unmark in-flight task", K(ret), K(key));
  }
  return ret;
}

int ObSpfreshTaskExecutor::requeue_in_flight_task_(const ObSpfreshTask &task)
{
  int ret = OB_SUCCESS;
  ObSpfreshTaskQueue *queue = nullptr;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task for in-flight requeue", K(ret), K(task));
  } else if (OB_FAIL(queue_map_.get_queue(
      task.task_key_.centroid_tablet_id_, queue))) {
    LOG_WARN("failed to get queue for in-flight requeue", K(ret), K(task));
  } else if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queue is null for in-flight requeue", K(ret), K(task));
  } else if (OB_FAIL(queue->requeue_in_flight_task(
      task, ObSpfreshTaskType::REASSIGN == task.task_type_
                ? REASSIGN_RETRY_DELAY_US : 0))) {
    LOG_WARN("failed to requeue in-flight task", K(ret), K(task));
  }
  return ret;
}

int ObSpfreshTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  task_trace_base_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskExecutor not init", K(ret));
  } else if (OB_FAIL(recover_stuck_tasks_())) {
    LOG_WARN("failed to recover stuck tasks", K(ret));
  }

  LOG_TRACE("[SPFRESH_TRACE] load_task done", KR(ret));

  return ret;
}

int ObSpfreshTaskExecutor::check_and_set_thread_pool()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskExecutor not init", K(ret));
  }
  // Thread pool is managed by the scheduler
  return ret;
}

bool ObSpfreshTaskExecutor::check_operation_allow()
{
  bool bret = true;

  if (IS_NOT_INIT) {
    bret = false;
    LOG_WARN_RET(OB_NOT_INIT, "ObSpfreshTaskExecutor not init");
  }
  // TODO: Add more checks if needed

  return bret;
}

int ObSpfreshTaskExecutor::process_queue()
{
  DispatchGuard guard(*this);
  int ret = OB_SUCCESS;
  if (guard.is_entered()) {
    int tmp_ret = check_all_spfresh_distributions_();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("check_all_spfresh_distributions failed", K(tmp_ret));
    }
    ret = process_queue_();
  }
  return ret;
}

bool ObSpfreshTaskExecutor::try_enter_dispatch_()
{
  lib::ObMutexGuard guard(dispatch_lock_);
  bool is_entered = !is_dispatch_stopped_;
  if (is_entered) {
    ++active_dispatch_count_;
  }
  return is_entered;
}

void ObSpfreshTaskExecutor::leave_dispatch_()
{
  lib::ObMutexGuard guard(dispatch_lock_);
  --active_dispatch_count_;
  if (0 == active_dispatch_count_) {
    dispatch_cond_.signal(UINT32_MAX);
  }
}

void ObSpfreshTaskExecutor::wait_dispatches_()
{
  bool is_drained = false;
  while (!is_drained) {
    uint32_t key = 0;
    {
      lib::ObMutexGuard guard(dispatch_lock_);
      key = dispatch_cond_.get_key();
      is_drained = (0 == active_dispatch_count_);
    }
    if (!is_drained) {
      // The generation key prevents a missed wakeup after releasing the lock.
      // Timed futex wait sleeps; no dispatch/queue lock is held here.
      const int wait_ret = dispatch_cond_.wait(key, 1000000);
      if (OB_SUCCESS != wait_ret && OB_TIMEOUT != wait_ret && OB_EAGAIN != wait_ret) {
        LOG_WARN_RET(wait_ret, "failed to wait for spfresh dispatches", K(wait_ret));
      }
    }
  }
}

int ObSpfreshTaskExecutor::process_queue_()
{
  int ret = OB_SUCCESS;
  int64_t queue_count = 0;
  int64_t total_dispatched = 0;
  // Snapshot tablet ids under the map lock, then dispatch without the map lock.
  ObSEArray<uint64_t, 128> queue_tablet_ids;
  ObSpfreshTask task;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskExecutor not init", K(ret));
  } else if (FALSE_IT(handler_.retry_pending_tasks())) {
  } else if (OB_FAIL(queue_map_.snapshot_queues(queue_tablet_ids, queue_count))) {
    LOG_WARN("failed to snapshot spfresh queues", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_tablet_ids.count(); ++i) {
      const uint64_t tablet_id = queue_tablet_ids.at(i);
      ObSpfreshTaskQueue *queue = nullptr;
      if (OB_FAIL(queue_map_.get_queue(tablet_id, queue))) {
        LOG_WARN("failed to get queue after snapshot", K(ret), K(tablet_id));
      } else if (OB_ISNULL(queue)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("queue is null after snapshot", K(ret), K(tablet_id));
      } else {
        bool marked = false;
        int pop_ret = OB_SUCCESS;
        int back_ret = OB_SUCCESS;
        int dispatch_ret = OB_SUCCESS;
        // Dispatch as much of this queue as possible within the global limit.
        while (OB_SUCC(ret) && handler_.get_in_flight_count() < MAX_CONCURRENT) {
          marked = false;
          pop_ret = pop_and_mark_in_flight_(*queue, task, marked);
          if (OB_ENTRY_NOT_EXIST == pop_ret) {
            // Distribution scheduling scans existing caches before queue dispatch.
            break;
          } else if (OB_SUCCESS != pop_ret) {
            ret = pop_ret;
            LOG_WARN("failed to pop task", K(ret), K(tablet_id), "queue_idx", i);
          } else if (!marked) {
            // Keep a retry at the front while the same key is still in flight.
            LOG_TRACE("[SPFRESH_TRACE] wait duplicate in-flight task", K(task));
            break;
          } else if (OB_FAIL(ensure_handler_started_())) {
            LOG_WARN("failed to lazy-start spfresh handler, pushing back to queue",
                     K(ret), K(tablet_id), K(task));
            back_ret = queue->requeue_in_flight_task(task, 0);
            if (OB_SUCCESS != back_ret) {
              LOG_ERROR("push back to queue failed, task lost", K(back_ret), K(task));
            }
            break;
          } else {
            dispatch_ret = handler_.push_task(task, MAX_CONCURRENT);
            if (OB_SUCCESS != dispatch_ret) {
              if (OB_SIZE_OVERFLOW == dispatch_ret) {
                LOG_TRACE("[SPFRESH_TRACE] handler capacity reached", K(tablet_id));
              } else {
                ret = dispatch_ret;
                LOG_WARN("failed to dispatch task to handler",
                         K(ret), K(tablet_id), K(task));
              }
              back_ret = queue->requeue_in_flight_task(task, 0);
              if (OB_SUCCESS != back_ret) {
                ret = back_ret;
                LOG_ERROR("push back to queue failed, task lost", K(back_ret), K(task));
              }
              break;  // Stop this queue for the current round and retry later.
            } else {
              ++total_dispatched;
            }
          }
        }
      }
    }
    if (total_dispatched > 0) {
      LOG_TRACE("[SPFRESH_TRACE] process_queue dispatched tasks",
                K(total_dispatched), K(queue_count));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] process_queue done", K(queue_tablet_ids), K(queue_count), KR(ret));
  return ret;
}

int ObSpfreshTaskExecutor::submit_task(ObSpfreshTaskType task_type,
                                       uint64_t index_table_id,
                                       uint64_t tablet_id,
                                       uint64_t center_id,
                                       int64_t ns_id,
                                       uint64_t ltable_id,
                                       int64_t cutover_modify_ts)
{
  int ret = OB_SUCCESS;
  bool task_submitted = false;

  if (OB_FAIL(submit_task_if_absent_(task_type, index_table_id, tablet_id,
      center_id, ns_id, ltable_id, task_submitted, cutover_modify_ts))) {
    LOG_WARN("failed to submit task", K(ret), K(task_type), K(index_table_id),
             K(tablet_id), K(center_id), K(ns_id), K(ltable_id));
  }
  return ret;
}

int ObSpfreshTaskExecutor::submit_task_if_absent_(
    ObSpfreshTaskType task_type,
    uint64_t index_table_id,
    uint64_t tablet_id,
    uint64_t center_id,
    int64_t ns_id,
    uint64_t ltable_id,
    bool &task_submitted,
    int64_t cutover_modify_ts)
{
  int ret = OB_SUCCESS;
  task_submitted = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskExecutor not init", K(ret));
  } else {
    ObSpfreshTaskQueue *queue = nullptr;
    if (OB_FAIL(queue_map_.get_queue(tablet_id, queue))) {
      LOG_WARN("failed to get queue", K(ret), K(tablet_id));
    } else if (OB_ISNULL(queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("queue is null", K(ret), K(tablet_id));
    } else {
      if (ObSpfreshTaskType::REASSIGN == task_type && cutover_modify_ts <= 0) {
        // External/lazy recovery does not carry the original publication
        // timestamp. A later timestamp is conservative: it may wait more
        // transactions but cannot miss an old writer.
        cutover_modify_ts = ObTimeUtility::current_time();
      }
      ObSpfreshTaskKey task_key(index_table_id, tablet_id, center_id, ns_id, ltable_id, task_type);
      ObSpfreshTask task(task_type, task_key, cutover_modify_ts);
      if (ObSpfreshTaskType::MERGE == task_type
          && OB_FAIL(queue->push_merge_if_no_distribution_task(task, task_submitted))) {
        LOG_WARN("failed to push merge task", K(ret), K(task));
      } else if (ObSpfreshTaskType::SPLIT == task_type
                 && OB_FAIL(queue->push_split_if_below_limit(
                     task, ObSpfreshPolicy::get_max_pending_split_count(),
                     task_submitted))) {
        LOG_WARN("failed to push split task", K(ret), K(task));
      } else if (ObSpfreshTaskType::MERGE != task_type
                 && ObSpfreshTaskType::SPLIT != task_type
                 && OB_FAIL(queue->push_if_absent(task, task_submitted))) {
        LOG_WARN("failed to push absent task", K(ret), K(task));
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] submit_task done",
            K(task_type), K(index_table_id), K(tablet_id), K(center_id),
            K(ns_id), K(ltable_id), K(task_submitted), KR(ret));
  return ret;
}

int ObSpfreshTaskExecutor::get_pending_split_count_(
    uint64_t index_table_id,
    uint64_t tablet_id,
    int64_t ns_id,
    uint64_t ltable_id,
    int64_t &split_count)
{
  int ret = OB_SUCCESS;
  ObSpfreshTaskQueue *queue = nullptr;
  split_count = 0;

  if (OB_FAIL(queue_map_.get_queue(tablet_id, queue))) {
    LOG_WARN("failed to get queue", K(ret), K(tablet_id));
  } else if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queue is null", K(ret), K(tablet_id));
  } else if (OB_FAIL(queue->get_pending_task_count(index_table_id, ns_id,
      ltable_id, ObSpfreshTaskType::SPLIT, split_count))) {
    LOG_WARN("failed to count pending split tasks", K(ret), K(tablet_id));
  }
  return ret;
}

int ObSpfreshTaskExecutor::check_index_dropped_(const uint64_t index_table_id, bool &dropped)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *index_schema = nullptr;
  dropped = false;
  if (OB_INVALID_ID == index_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid SPFresh index table id", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
      .get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for SPFresh index", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id, index_schema))) {
    LOG_WARN("failed to get SPFresh index schema", K(ret), K(index_table_id));
  } else {
    dropped = OB_ISNULL(index_schema) || index_schema->is_in_recyclebin();
  }
  return ret;
}

int ObSpfreshTaskExecutor::process_one_task_(ObSpfreshTask &task)
{
  int ret = OB_SUCCESS;
  ObSpfreshTaskContext context;
  bool dropped = false;

  if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else if (OB_FAIL(check_index_dropped_(task.task_key_.index_table_id_, dropped))) {
    LOG_WARN("failed to check SPFresh task index", K(ret), K(task));
  } else if (dropped) {
    // DROP may finish after dispatch; release this task and its in-flight key normally.
    LOG_DEBUG("skip SPFresh task for dropped index", K(task));
  } else if (OB_FAIL(prepare_task_context_(task, context))) {
    LOG_WARN("failed to prepare SPFresh task context", K(ret), K(task));
  } else if (OB_ISNULL(context.map_cache_)) {
    LOG_DEBUG("skip SPFresh task without available IVF cache", K(task));
  } else {
    switch (task.task_type_) {
    case ObSpfreshTaskType::SPLIT: {
      if (OB_FAIL(process_split_task_(task, context))) {
        LOG_WARN("failed to process split task", K(ret), K(task));
      }
      break;
    }
    case ObSpfreshTaskType::MERGE: {
      if (OB_FAIL(process_merge_task_(task, context))) {
        LOG_WARN("failed to process merge task", K(ret), K(task));
      }
      break;
    }
    case ObSpfreshTaskType::REASSIGN: {
      if (OB_FAIL(process_reassign_task_(task, context))) {
        LOG_WARN("failed to process reassign task", K(ret), K(task));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid task type", K(ret), K(task.task_type_));
      break;
    }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] process_one_task_ done",
            K(task.task_type_), K(task.task_key_.center_id_), K(ret));
  return ret;
}

int ObSpfreshTaskExecutor::prepare_task_context_(
    ObSpfreshTask &task,
    ObSpfreshTaskContext &context)
{
  int ret = OB_SUCCESS;
  bool location_cache_hit = false;
  ObSpfreshKvTableResolver resolver;
  ObIvfCacheMgr *cache_mgr = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  share::ObLSID ls_id;
  share::ObLSID physical_ls_id;
  common::ObTabletID tablet_id(task.task_key_.centroid_tablet_id_);
  common::ObTabletID physical_tablet_id;
  const schema::ObTableSchema *kv_table_schema = nullptr;
  ObVectorIndexParam vec_param;

  context.queue_tablet_id_ = task.task_key_.centroid_tablet_id_;
  if (OB_FAIL(queue_map_.get_queue(context.queue_tablet_id_, context.queue_))) {
    LOG_WARN("failed to get queue", K(ret), K(context.queue_tablet_id_));
  } else if (OB_ISNULL(context.queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queue is null", K(ret), K(context.queue_tablet_id_));
  } else if (OB_ISNULL(context.sql_proxy_ = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(resolver.resolve(
      tenant_id_, task.task_key_.index_table_id_, context.kv_info_))) {
    LOG_WARN("failed to resolve kv table info", K(ret), K(task));
  } else if (!context.kv_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kv table info invalid", K(ret), K(context.kv_info_));
  } else {
    context.kv_info_.namespace_id_ = task.task_key_.namespace_id_;
    context.kv_info_.logical_table_id_ = task.task_key_.logical_table_id_;
    context.kv_info_.centroid_tablet_id_ = task.task_key_.centroid_tablet_id_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCTX.location_service_->get(
      tenant_id_, tablet_id, 0, location_cache_hit, ls_id))) {
    LOG_WARN("failed to get ls_id from tablet", K(ret), K(context.queue_tablet_id_));
  } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
      .get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id_, context.kv_info_.kv_data_table_id_, kv_table_schema))) {
    LOG_WARN("failed to get physical KV table schema", K(ret), K(context.kv_info_));
  } else if (OB_ISNULL(kv_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("physical KV table schema is null", K(ret), K(context.kv_info_));
  } else if (OB_FAIL(ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
      *kv_table_schema, task.task_key_.namespace_id_, physical_tablet_id))) {
    LOG_WARN("failed to resolve physical KV tablet for SPFresh task", K(ret), K(task));
  } else if (OB_FAIL(GCTX.location_service_->get(
      tenant_id_, physical_tablet_id, 0, location_cache_hit, physical_ls_id))) {
    LOG_WARN("failed to get physical KV LS", K(ret), K(physical_tablet_id), K(task));
  } else if (FALSE_IT(context.physical_tablet_id_ = physical_tablet_id)) {
  } else if (FALSE_IT(context.physical_ls_id_ = physical_ls_id)) {
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_param_with_dim(
      schema_guard, tenant_id_, task.task_key_.index_table_id_,
      task.task_key_.index_table_id_, ObVectorIndexType::VIT_IVF_INDEX, vec_param))) {
    LOG_WARN("failed to get vector index param", K(ret),
             K(task.task_key_.index_table_id_));
  } else if (!task.cache_guard_.is_valid() && OB_FAIL(
      vector_index_service_->acquire_ivf_cache_mgr_guard(
      ls_id, ObIvfCacheMgrKey(tablet_id, task.task_key_.namespace_id_,
                             static_cast<int64_t>(task.task_key_.logical_table_id_)),
      vec_param, vec_param.dim_, task.task_key_.index_table_id_, task.cache_guard_))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("skip SPFresh task after IVF cache deletion", K(task));
    } else {
      LOG_WARN("failed to acquire ivf cache mgr guard", K(ret), K(ls_id),
               K(context.queue_tablet_id_));
    }
  } else if (!task.cache_guard_.is_valid()) {
  } else if (OB_ISNULL(cache_mgr = task.cache_guard_.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<ObIvfMapCentCache>(
      IvfCacheType::IVF_MAP_CENTROID_CACHE, context.map_cache_))) {
    LOG_WARN("failed to get or create map cache", K(ret));
  } else if (OB_ISNULL(context.map_cache_) || !context.map_cache_->is_completed()) {
    context.map_cache_ = nullptr;
    task.cache_guard_.reset();
  } else {
    context.dist_algo_ = vec_param.dist_algorithm_;
  }
  return ret;
}

int ObSpfreshTaskExecutor::process_split_task_(
    const ObSpfreshTask &task,
    ObSpfreshTaskContext &context)
{
  int ret = OB_SUCCESS;
  int exec_ret = OB_SUCCESS;
  int history_ret = OB_SUCCESS;
  int64_t task_start_time = 0;
  int64_t task_end_time = 0;
  ObSpfreshSplitTask split_task;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  ObString vec_index_param_str;

  if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance()
      .get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id_, task.task_key_.index_table_id_, index_table_schema))) {
    LOG_WARN("failed to get index table schema", K(ret),
             K(task.task_key_.index_table_id_));
  } else if (OB_ISNULL(index_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("index table schema is null", K(ret), K(task.task_key_.index_table_id_));
  } else {
    vec_index_param_str = index_table_schema->get_index_params();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(split_task.init(
      tenant_id_, context.sql_proxy_, task.task_key_.index_table_id_, context.kv_info_,
      context.queue_tablet_id_, task.task_key_.center_id_, task.task_key_.namespace_id_,
      task.task_key_.logical_table_id_, context.queue_, context.map_cache_,
      vec_index_param_str, context.dist_algo_))) {
    LOG_WARN("failed to init split task", K(ret), K(task));
  } else {
    split_task.set_cache_guard(task.cache_guard_);
    task_start_time = ObTimeUtility::current_time();
    exec_ret = split_task.execute(context.physical_tablet_id_);
    task_end_time = ObTimeUtility::current_time();
    if (OB_SUCCESS != exec_ret) {
      ret = exec_ret;
      LOG_WARN("split task failed", K(ret), K(task));
    } else {
      LOG_TRACE("[SPFRESH_TRACE] split task executed successfully", K(task));
    }
    history_ret = record_split_task_history_(
        task, split_task, exec_ret, task_start_time, task_end_time);
    if (OB_SUCCESS != history_ret) {
      LOG_WARN("failed to record split task history", K(history_ret), K(task));
    }
  }
  split_task.destroy();
  return ret;
}

int ObSpfreshTaskExecutor::record_split_task_history_(
    const ObSpfreshTask &task,
    const ObSpfreshSplitTask &split_task,
    int task_ret,
    int64_t start_time,
    int64_t end_time)
{
  int ret = OB_SUCCESS;
  common::ObSqlString task_info_json;
  const common::ObIArray<int64_t> &child_ids = split_task.get_child_center_ids();
  const common::ObIArray<int64_t> &child_counts = split_task.get_child_vector_counts();

  if (OB_FAIL(task_info_json.append_fmt(
      "{\"center_id\":%ld,\"source_vector_count\":%ld,\"child_center_ids\":[",
      split_task.get_center_id(), split_task.get_source_vector_count()))) {
    LOG_WARN("failed to format split task info", K(ret), K(task));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_ids.count(); ++i) {
    if (OB_FAIL(task_info_json.append_fmt(
        "%s%ld", (i > 0 ? "," : ""), child_ids.at(i)))) {
      LOG_WARN("failed to append child center id", K(ret), K(task));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(task_info_json.append_fmt(
      "],\"child_vector_counts\":["))) {
    LOG_WARN("failed to append child vector counts", K(ret), K(task));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_counts.count(); ++i) {
    if (OB_FAIL(task_info_json.append_fmt(
        "%s%ld", (i > 0 ? "," : ""), child_counts.at(i)))) {
      LOG_WARN("failed to append child vector count", K(ret), K(task));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(task_info_json.append_fmt("]}"))) {
    LOG_WARN("failed to finish split task info", K(ret), K(task));
  } else if (OB_SUCC(ret) && OB_FAIL(record_task_history_(
      task, ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_SPFRESH_SPLIT,
      task_ret, task_info_json, start_time, end_time))) {
    LOG_WARN("failed to record split task history", K(ret), K(task));
  }
  return ret;
}

int ObSpfreshTaskExecutor::process_merge_task_(
    const ObSpfreshTask &task,
    ObSpfreshTaskContext &context)
{
  int ret = OB_SUCCESS;
  int exec_ret = OB_SUCCESS;
  int history_ret = OB_SUCCESS;
  int64_t task_start_time = 0;
  int64_t task_end_time = 0;
  ObSpfreshMergeTask merge_task;
  common::ObSqlString task_info_json;

  if (OB_FAIL(merge_task.init(
      tenant_id_, context.sql_proxy_, task.task_key_.index_table_id_, context.kv_info_,
      context.queue_tablet_id_, task.task_key_.center_id_, task.task_key_.namespace_id_,
      task.task_key_.logical_table_id_, context.queue_, context.map_cache_,
      context.dist_algo_))) {
    LOG_WARN("failed to init merge task", K(ret), K(task));
  } else {
    merge_task.set_cache_guard(task.cache_guard_);
    task_start_time = ObTimeUtility::current_time();
    exec_ret = merge_task.execute(context.physical_tablet_id_);
    task_end_time = ObTimeUtility::current_time();
    if (OB_SUCCESS != exec_ret) {
      ret = exec_ret;
      LOG_WARN("merge task failed", K(ret), K(task));
    } else {
      LOG_TRACE("[SPFRESH_TRACE] merge task executed successfully", K(task));
    }
    history_ret = task_info_json.append_fmt(
        "{\"center_id\":%ld,\"target_center_id\":%ld,\"migrated_vector_count\":%ld}",
        merge_task.get_source_center_id(), merge_task.get_target_center_id(),
        merge_task.get_migrated_vector_count());
    if (OB_SUCCESS != history_ret) {
      LOG_WARN("failed to format merge task info", K(history_ret), K(task));
    } else if (OB_SUCCESS != (history_ret = record_task_history_(
        task, ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_SPFRESH_MERGE,
        exec_ret, task_info_json, task_start_time, task_end_time))) {
      LOG_WARN("failed to record merge task history", K(history_ret), K(task));
    }
  }
  merge_task.destroy();
  return ret;
}

int ObSpfreshTaskExecutor::process_reassign_task_(
    const ObSpfreshTask &task,
    ObSpfreshTaskContext &context)
{
  int ret = OB_SUCCESS;
  int exec_ret = OB_SUCCESS;
  int history_ret = OB_SUCCESS;
  int64_t task_start_time = 0;
  int64_t task_end_time = 0;
  ObSpfreshReassignTask reassign_task;
  common::ObSqlString task_info_json;

  if (OB_FAIL(reassign_task.init(
      tenant_id_, context.sql_proxy_, task.task_key_.index_table_id_, context.kv_info_,
      context.queue_tablet_id_, task.task_key_.center_id_, task.task_key_.namespace_id_,
      task.task_key_.logical_table_id_, context.queue_, context.map_cache_,
      context.dist_algo_, context.physical_ls_id_, context.physical_tablet_id_,
      task.cutover_modify_ts_))) {
    LOG_WARN("failed to init reassign task", K(ret), K(task));
  } else {
    task_start_time = ObTimeUtility::current_time();
    exec_ret = reassign_task.execute();
    task_end_time = ObTimeUtility::current_time();
    if (OB_EAGAIN == exec_ret) {
      ret = OB_EAGAIN;
      // The task preserved REASSIGNING. The handler restores this in-flight
      // task to its queue after execute() returns.
      LOG_INFO("reassign task is waiting for pre-cutover writers", K(task));
    } else if (OB_SUCCESS != exec_ret) {
      ret = exec_ret;
      LOG_WARN("reassign task failed", K(ret), K(task));
    } else {
      LOG_TRACE("[SPFRESH_TRACE] reassign task executed successfully", K(task));
    }
    if (OB_EAGAIN != exec_ret) {
      history_ret = task_info_json.append_fmt(
          "{\"center_id\":%ld,\"reassigned_vector_count\":%ld}",
          reassign_task.get_center_id(), reassign_task.get_reassigned_vector_count());
      if (OB_SUCCESS != history_ret) {
        LOG_WARN("failed to format reassign task info", K(history_ret), K(task));
      } else if (OB_SUCCESS != (history_ret = record_task_history_(
          task, ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_SPFRESH_REASSIGN,
          exec_ret, task_info_json, task_start_time, task_end_time))) {
        LOG_WARN("failed to record reassign task history", K(history_ret), K(task));
      }
    }
  }
  reassign_task.destroy();
  return ret;
}

int ObSpfreshTaskExecutor::recover_stuck_tasks_()
{
  int ret = OB_SUCCESS;

  // Recovery iterates ls_mgr_map which only contains LSs active on this node.
  // Non-leader nodes skip recovery implicitly since their LS mgrs don't have
  // the centroids for SPFresh indexes on other nodes' LSs.
  if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector_index_service_ is null", K(ret));
  } else {
    LSIndexMgrMap &ls_mgr_map = vector_index_service_->get_ls_index_mgr_map();
    FOREACH_X(ls_iter, ls_mgr_map, OB_SUCC(ret)) {
      ObPluginVectorIndexMgr *ls_mgr = ls_iter->second;
      if (OB_ISNULL(ls_mgr)) {
        continue;
      }

      common::RWLock::RLockGuard lock_guard(ls_mgr->get_ivf_cache_mgr_map_rwlock());
      IvfCacheMgrMap &cache_map = ls_mgr->get_ivf_cache_mgr_map();

      FOREACH_X(cache_iter, cache_map, OB_SUCC(ret)) {
        const ObIvfCacheMgrKey &key = cache_iter->first;

        // Only process SPFresh indexes (non-zero namespace_id)
        if (ObIvfCacheMgrKey::INVALID_NS_ID == key.namespace_id_) {
          continue;
        }

        uint64_t ltable_id = key.ltable_id_;
        if (OB_INVALID_ID == ltable_id) {
          continue;
        }

        ObIvfCacheMgr *cache_mgr = cache_iter->second;
        if (OB_ISNULL(cache_mgr)) {
          continue;
        }

        ObIvfMapCentCache *map_cache = nullptr;
        int tmp_ret = cache_mgr->get_or_create_cache_node<ObIvfMapCentCache>(
            IvfCacheType::IVF_MAP_CENTROID_CACHE, map_cache);
        if (OB_SUCCESS != tmp_ret) {
          continue;
        } else if (OB_ISNULL(map_cache) || 0 == map_cache->get_count()) {
          continue;
        }

        uint64_t index_table_id = static_cast<uint64_t>(cache_mgr->get_table_id());
        uint64_t tablet_id = key.tablet_id_.id();
        int64_t ns_id = key.namespace_id_;
        int64_t stuck_count = 0;
        int64_t reassign_count = 0;

        // Resolve kv_info for state reset (SPLITTING/MERGING -> NORMAL)
        ObSpfreshKvTableResolver resolver;
        ObSpfreshKvTableInfo kv_info;
        ObSpfreshTableHelper *table_helper = nullptr;
        bool has_table_helper = false;
        int resolve_ret = resolver.resolve(tenant_id_, index_table_id, kv_info);
        if (OB_SUCCESS == resolve_ret && kv_info.is_valid()) {
          table_helper = static_cast<ObSpfreshTableHelper *>(
              ob_malloc(sizeof(ObSpfreshTableHelper), "SpfreshRecHlp"));
          if (OB_NOT_NULL(table_helper)) {
            new (table_helper) ObSpfreshTableHelper();
            int init_ret = table_helper->init(
                tenant_id_, GCTX.sql_proxy_, kv_info);
            if (OB_SUCCESS == init_ret) {
              has_table_helper = true;
            }
          }
        }

        // Iterate all centroids including non-NORMAL states
        struct RecoveryCb
        {
          int operator()(int64_t cid, const ObCentroidInfo &info)
          {
            int ret = OB_SUCCESS;
            if (ObIvfCentroidState::SPLITTING == info.state_
                || ObIvfCentroidState::MERGING == info.state_) {
              (*stuck_count_)++;
              if (*has_table_helper_ && OB_NOT_NULL(*table_helper_)) {
                SCN commit_scn;
                int reset_ret = (*table_helper_)->update_centroid_state(
                    static_cast<uint64_t>(cid),
                    ObIvfCentroidState::NORMAL,
                    info.state_,
                    commit_scn);
                if (OB_SUCCESS != reset_ret) {
                  LOG_WARN("recovery reset state failed",
                           K(reset_ret), K(cid));
                } else {
                  LOG_TRACE("[SPFRESH_TRACE] recovery reset centroid to NORMAL",
                           K(cid), "old_state", static_cast<int>(info.state_));
                }
              }
            } else if (ObIvfCentroidState::SPLIT_DONE == info.state_
                       || ObIvfCentroidState::MERGED == info.state_
                       || ObIvfCentroidState::REASSIGNING == info.state_) {
              (*reassign_count_)++;
              int submit_ret = executor_->submit_task(ObSpfreshTaskType::REASSIGN,
                  index_table_id_, tablet_id_,
                  static_cast<uint64_t>(cid), ns_id_, ltable_id_);
              if (OB_SUCCESS != submit_ret) {
                LOG_WARN("recovery submit reassign failed",
                         K(submit_ret), K(cid));
              } else {
                LOG_TRACE("[SPFRESH_TRACE] recovery submitted REASSIGN",
                         K(cid), K_(tablet_id), K_(ns_id));
              }
            }
            return ret;
          }
          ObSpfreshTaskExecutor *executor_;
          uint64_t index_table_id_;
          uint64_t tablet_id_;
          int64_t ns_id_;
          uint64_t ltable_id_;
          int64_t *stuck_count_;
          int64_t *reassign_count_;
          ObSpfreshTableHelper **table_helper_;
          bool *has_table_helper_;
        };

        RecoveryCb recovery_cb;
        recovery_cb.executor_ = this;
        recovery_cb.index_table_id_ = index_table_id;
        recovery_cb.tablet_id_ = tablet_id;
        recovery_cb.ns_id_ = ns_id;
        recovery_cb.ltable_id_ = ltable_id;
        recovery_cb.stuck_count_ = &stuck_count;
        recovery_cb.reassign_count_ = &reassign_count;
        recovery_cb.table_helper_ = &table_helper;
        recovery_cb.has_table_helper_ = &has_table_helper;
        map_cache->for_each_centroid_info_all(SCN::max_scn(), recovery_cb);

        if (has_table_helper && OB_NOT_NULL(table_helper)) {
          table_helper->destroy();
          ob_free(table_helper);
          table_helper = nullptr;
        }

        if (stuck_count > 0 || reassign_count > 0) {
          LOG_INFO("recovery scan complete",
                   K(index_table_id), K(tablet_id),
                   K(stuck_count), K(reassign_count));
        }
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] recover_stuck_tasks_ exit", K(ret), K(MTL_ID()), "this", this);
  return ret;
}

int ObSpfreshTaskExecutor::check_and_submit_distribution_tasks_(
    ObIvfMapCentCache *map_cache,
    uint64_t tablet_id, uint64_t index_table_id,
    int64_t ns_id, uint64_t ltable_id)
{
  int ret = OB_SUCCESS;
  ObSpfreshTaskQueue *queue = nullptr;
  bool dropped = false;

  if (OB_ISNULL(map_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("map cache is null", K(ret));
  } else if (OB_FAIL(check_index_dropped_(index_table_id, dropped))) {
    LOG_WARN("failed to check SPFresh distribution index", K(ret), K(index_table_id));
  } else if (dropped) {
    // Cache reclamation is asynchronous; do not schedule work from a dropped index's cache.
    LOG_DEBUG("skip SPFresh distribution for dropped index", K(index_table_id));
  } else if (OB_FAIL(queue_map_.get_queue(tablet_id, queue))) {
    LOG_WARN("failed to get queue for distribution check", K(ret), K(tablet_id));
  } else if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("queue is null for distribution check", K(ret), K(tablet_id));
  } else {
    lib::ObMutexGuard guard(queue->get_distribution_mutex());
    if (OB_FAIL(do_check_and_submit_distribution_tasks_(
        map_cache, tablet_id, index_table_id, ns_id, ltable_id))) {
      LOG_WARN("failed to check and submit distribution tasks", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::do_check_and_submit_distribution_tasks_(
    ObIvfMapCentCache *map_cache,
    uint64_t tablet_id, uint64_t index_table_id,
    int64_t ns_id, uint64_t ltable_id)
{
  int ret = OB_SUCCESS;
  ObSpfreshDistributionSnapshot snapshot;
  ObSpfreshSplitScheduleResult split_schedule_result;
  int64_t merge_submitted = 0;
  int64_t desired_center_count = 1;
  int64_t target_nlist = 1;
  bool bootstrap_submitted = false;

  if (OB_ISNULL(map_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("map cache is null", K(ret));
  } else if (FALSE_IT(target_nlist = map_cache->get_nlist_param())) {
  } else if (OB_FAIL(collect_distribution_snapshot_(map_cache, snapshot))) {
    LOG_WARN("failed to collect distribution snapshot", K(ret));
  } else if (OB_FAIL(get_pending_split_count_(index_table_id, tablet_id,
      ns_id, ltable_id, split_schedule_result.scheduled_count_))) {
    LOG_WARN("failed to get pending split count", K(ret), K(tablet_id));
  } else if (0 == snapshot.normal_center_count_) {
    // No scheduling is needed when no NORMAL centroid exists.
  } else if (1 == snapshot.normal_center_count_) {
    if (0 == split_schedule_result.scheduled_count_
        && snapshot.total_count_ >= ObSpfreshPolicy::get_bootstrap_trigger_rows()
        && target_nlist >= 2
        && snapshot.split_retry_after_us_.at(0) <= ObTimeUtility::current_time()) {
      LOG_INFO("first split triggered",
               K(tablet_id), K_(snapshot.total_count), K(target_nlist));
      if (OB_FAIL(submit_task_if_absent_(ObSpfreshTaskType::SPLIT,
          index_table_id, tablet_id, snapshot.cids_.at(0), ns_id, ltable_id,
          bootstrap_submitted))) {
        LOG_WARN("failed to submit first split task", K(ret));
      } else if (bootstrap_submitted) {
        split_schedule_result.submitted_count_ = 1;
        split_schedule_result.scheduled_count_ = 1;
      } else if (OB_FAIL(get_pending_split_count_(index_table_id, tablet_id,
          ns_id, ltable_id, split_schedule_result.scheduled_count_))) {
        LOG_WARN("failed to refresh bootstrap split count", K(ret), K(tablet_id));
      }
    }
  } else {
    desired_center_count = ObSpfreshPolicy::get_desired_center_count(
        snapshot.total_count_, target_nlist);
    if (OB_FAIL(submit_split_tasks_(snapshot, desired_center_count,
        tablet_id, index_table_id, ns_id, ltable_id, split_schedule_result))) {
      LOG_WARN("failed to submit split tasks", K(ret), K(tablet_id));
    } else if (0 == split_schedule_result.submitted_count_
               && 0 == split_schedule_result.scheduled_count_
               && OB_FAIL(submit_merge_task_(snapshot, desired_center_count,
                   tablet_id, index_table_id, ns_id, ltable_id,
                   merge_submitted))) {
      LOG_WARN("failed to submit merge task", K(ret), K(tablet_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] distribution decision", K(tablet_id),
            K_(snapshot.normal_center_count), K_(snapshot.total_count),
            K(desired_center_count),
            "split_submitted", split_schedule_result.submitted_count_,
            "split_scheduled", split_schedule_result.scheduled_count_,
            K(merge_submitted));
  snapshot.reset();

  return ret;
}

ObSpfreshTaskExecutor::CollectDistributionFn::CollectDistributionFn(
    ObSpfreshDistributionSnapshot &snapshot)
    : snapshot_(snapshot)
{}

int ObSpfreshTaskExecutor::CollectDistributionFn::operator()(
    int64_t cid,
    const ObCentroidInfo &info)
{
  int ret = OB_SUCCESS;
  if (ObIvfCentroidState::NORMAL == info.state_) {
    if (OB_FAIL(snapshot_.add_normal_center(
        cid, info.posting_list_count_, info.split_retry_after_us_))) {
      LOG_WARN("failed to add normal center to snapshot", K(ret), K(cid));
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::collect_distribution_snapshot_(
    ObIvfMapCentCache *map_cache,
    ObSpfreshDistributionSnapshot &snapshot)
{
  int ret = OB_SUCCESS;

  snapshot.reset();
  if (OB_ISNULL(map_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("map cache is null", K(ret));
  } else {
    CollectDistributionFn collect_fn(snapshot);
    if (OB_FAIL(map_cache->for_each_centroid_info(SCN::max_scn(), collect_fn))) {
      LOG_WARN("failed to iterate centroids", K(ret));
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::submit_split_tasks_(
    const ObSpfreshDistributionSnapshot &snapshot,
    int64_t desired_center_count,
    uint64_t tablet_id,
    uint64_t index_table_id,
    int64_t ns_id,
    uint64_t ltable_id,
    ObSpfreshSplitScheduleResult &schedule_result)
{
  int ret = OB_SUCCESS;
  int64_t now_us = ObTimeUtility::current_time();
  int64_t split_high = ObSpfreshPolicy::get_split_high_rows();
  int64_t max_pending_split_count = ObSpfreshPolicy::get_max_pending_split_count();
  int64_t pending_split_count = 0;
  int64_t high_water_end = 0;
  int64_t snapshot_idx = -1;
  bool task_submitted = false;
  ObSpfreshSplitCandidate candidate = ObSpfreshSplitCandidate();
  ObSpfreshSplitCandidateCmp candidate_cmp;
  common::ObSEArray<ObSpfreshSplitCandidate, 128> candidates;

  for (int64_t i = 0; OB_SUCC(ret) && i < snapshot.cids_.count(); ++i) {
    if (snapshot.counts_.at(i) > 1
        && snapshot.split_retry_after_us_.at(i) <= now_us) {
      candidate.snapshot_idx_ = i;
      candidate.count_ = snapshot.counts_.at(i);
      candidate.center_id_ = snapshot.cids_.at(i);
      if (OB_FAIL(candidates.push_back(candidate))) {
        LOG_WARN("failed to add split candidate", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(candidates.begin(), candidates.end(), candidate_cmp);
    while (high_water_end < candidates.count()
           && candidates.at(high_water_end).count_ > split_high) {
      ++high_water_end;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < high_water_end
       && schedule_result.scheduled_count_ < max_pending_split_count; ++i) {
    snapshot_idx = candidates.at(i).snapshot_idx_;
    task_submitted = false;
    if (OB_FAIL(submit_task_if_absent_(ObSpfreshTaskType::SPLIT,
        index_table_id, tablet_id, snapshot.cids_.at(snapshot_idx), ns_id,
        ltable_id, task_submitted))) {
      LOG_WARN("failed to submit high water split", K(ret),
               "cid", snapshot.cids_.at(snapshot_idx),
               "count", snapshot.counts_.at(snapshot_idx));
    } else if (task_submitted) {
      ++schedule_result.submitted_count_;
      ++schedule_result.scheduled_count_;
    }
  }
  for (int64_t i = high_water_end; OB_SUCC(ret) && i < candidates.count()
       && snapshot.normal_center_count_ + schedule_result.scheduled_count_
           < desired_center_count
       && schedule_result.scheduled_count_ < max_pending_split_count; ++i) {
    snapshot_idx = candidates.at(i).snapshot_idx_;
    task_submitted = false;
    if (OB_FAIL(submit_task_if_absent_(ObSpfreshTaskType::SPLIT,
        index_table_id, tablet_id, snapshot.cids_.at(snapshot_idx), ns_id,
        ltable_id, task_submitted))) {
      LOG_WARN("failed to submit center debt split", K(ret),
               "cid", snapshot.cids_.at(snapshot_idx),
               "count", snapshot.counts_.at(snapshot_idx));
    } else if (task_submitted) {
      ++schedule_result.submitted_count_;
      ++schedule_result.scheduled_count_;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(get_pending_split_count_(index_table_id,
      tablet_id, ns_id, ltable_id, pending_split_count))) {
    LOG_WARN("failed to refresh pending split count", K(ret), K(tablet_id));
  } else if (OB_SUCC(ret)) {
    schedule_result.scheduled_count_ = pending_split_count;
  }
  return ret;
}

int ObSpfreshTaskExecutor::submit_merge_task_(
    const ObSpfreshDistributionSnapshot &snapshot,
    int64_t desired_center_count,
    uint64_t tablet_id,
    uint64_t index_table_id,
    int64_t ns_id,
    uint64_t ltable_id,
    int64_t &merge_submitted)
{
  int ret = OB_SUCCESS;
  int64_t merge_low = ObSpfreshPolicy::get_merge_low_rows();
  int64_t center_floor = ObSpfreshPolicy::get_merge_center_floor(desired_center_count);
  int64_t min_idx = -1;
  int64_t min_count = INT64_MAX;
  bool task_submitted = false;
  merge_submitted = 0;

  if (snapshot.normal_center_count_ > center_floor) {
    for (int64_t i = 0; i < snapshot.cids_.count(); ++i) {
      if (snapshot.counts_.at(i) <= merge_low
          && snapshot.counts_.at(i) < min_count) {
        min_idx = i;
        min_count = snapshot.counts_.at(i);
      }
    }
    if (min_idx >= 0 && ObSpfreshPolicy::get_max_merge_per_round() > 0) {
      ObSpfreshTaskQueue *queue = nullptr;
      if (OB_FAIL(queue_map_.get_queue(tablet_id, queue))) {
        LOG_WARN("failed to get queue for merge", K(ret), K(tablet_id));
      } else if (OB_ISNULL(queue)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("queue is null for merge", K(ret), K(tablet_id));
      } else {
        ObSpfreshTaskKey task_key(index_table_id, tablet_id,
                                  snapshot.cids_.at(min_idx), ns_id,
                                  ltable_id, ObSpfreshTaskType::MERGE);
        ObSpfreshTask task(ObSpfreshTaskType::MERGE, task_key);
        if (OB_FAIL(queue->push_merge_if_no_distribution_task(
            task, task_submitted))) {
          LOG_WARN("failed to submit merge task", K(ret),
                   "cid", snapshot.cids_.at(min_idx), K(min_count));
        } else if (task_submitted) {
          merge_submitted = 1;
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTaskExecutor::check_all_spfresh_distributions_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector_index_service_ is null", K(ret));
  } else {
    LSIndexMgrMap &ls_mgr_map = vector_index_service_->get_ls_index_mgr_map();
    int64_t ls_mgr_count = 0;
    int64_t cache_entry_count = 0;
    int64_t valid_ns_count = 0;
    int64_t valid_ltable_count = 0;
    int64_t checked_count = 0;
    FOREACH_X(ls_iter, ls_mgr_map, OB_SUCC(ret)) {
      ++ls_mgr_count;
      ObPluginVectorIndexMgr *ls_mgr = ls_iter->second;
      if (OB_ISNULL(ls_mgr)) {
        LOG_WARN("ls mgr is null, skip", K(ls_iter->first));
        continue;
      }
      common::RWLock::RLockGuard lock_guard(ls_mgr->get_ivf_cache_mgr_map_rwlock());
      IvfCacheMgrMap &cache_map = ls_mgr->get_ivf_cache_mgr_map();
      FOREACH_X(cache_iter, cache_map, OB_SUCC(ret)) {
        ++cache_entry_count;
        const ObIvfCacheMgrKey &key = cache_iter->first;
        if (ObIvfCacheMgrKey::INVALID_NS_ID != key.namespace_id_) {
          ++valid_ns_count;
          uint64_t ltable_id = key.ltable_id_;
          if (OB_INVALID_ID != ltable_id) {
            ++valid_ltable_count;
            if (OB_NOT_NULL(cache_iter->second)) {
              ObIvfCacheMgr *cache_mgr = cache_iter->second;
              ObIvfCacheMgrGuard cache_guard;
              int set_ret = cache_guard.set_cache_mgr(cache_mgr);
              if (OB_SUCCESS != set_ret) {
                LOG_WARN("failed to set cache mgr guard", K(set_ret));
              } else {
                ObIvfMapCentCache *map_cache = nullptr;
                int tmp_ret = cache_mgr->get_or_create_cache_node<ObIvfMapCentCache>(
                    IvfCacheType::IVF_MAP_CENTROID_CACHE, map_cache);
                if (OB_SUCCESS != tmp_ret) {
                  LOG_WARN("failed to get map cache", K(tmp_ret));
                } else if (OB_NOT_NULL(map_cache)) {
                  ++checked_count;
                  uint64_t index_table_id = static_cast<uint64_t>(cache_mgr->get_table_id());
                  int check_ret = check_and_submit_distribution_tasks_(map_cache,
                      key.tablet_id_.id(), index_table_id, key.namespace_id_, ltable_id);
                  if (OB_SUCCESS != check_ret) {
                    LOG_WARN("check_and_submit_distribution_tasks failed",
                             K(check_ret), K(key.tablet_id_), K(index_table_id));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTaskExecutor::record_task_history_(const ObSpfreshTask &task,
                                                int64_t task_type,
                                                int task_ret,
                                                const common::ObSqlString &task_info_json,
                                                int64_t start_time,
                                                int64_t end_time)
{
  int ret = OB_SUCCESS;
  int64_t new_task_id = OB_INVALID_ID;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null, skip recording task history", K(ret));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
    LOG_WARN("failed to fetch new task id for spfresh history", K(ret), K(tenant_id_));
  } else {
    ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
    char *buf = static_cast<char *>(ob_malloc(sizeof(ObVecIndexAsyncTaskCtx), "SpfreshHistory"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate task ctx", K(ret));
    } else {
      task_ctx = new (buf) ObVecIndexAsyncTaskCtx();
      task_ctx->tenant_id_ = tenant_id_;
      task_ctx->task_status_.tenant_id_ = tenant_id_;
      task_ctx->task_status_.table_id_ = task.task_key_.index_table_id_;
      task_ctx->task_status_.tablet_id_ = task.task_key_.centroid_tablet_id_;
      task_ctx->task_status_.task_id_ = new_task_id;
      task_ctx->task_status_.task_type_ = task_type;
      task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
      task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
      task_ctx->task_status_.ret_code_ = task_ret;
      task_ctx->task_status_.target_scn_ = SCN::min_scn();
      {
        uint64_t uval[4];
        ObCurTraceId::get_trace_id()->get_uval(uval);
        // ATOMIC_AAF is required: record_task_history_ runs concurrently on the
        // spfresh handler thread pool (up to MAX_THREAD_COUNT=12), so a plain
        // ++ would race and produce duplicate trace_id uval[3] across tasks.
        uval[3] = ATOMIC_AAF(&task_trace_base_num_, 1);
        task_ctx->task_status_.trace_id_.set(uval);
      }

      ObVecIndexTaskCtxArray task_ctx_array;
      if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
        LOG_WARN("failed to push task ctx", K(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::batch_insert_vec_task(
                     tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, *sql_proxy, task_ctx_array))) {
        LOG_WARN("failed to insert spfresh task history", K(ret), K(task_type),
                 K(task.task_key_.center_id_));
      }

      // Update observable columns separately (not included in batch_insert_vec_task).
      if (OB_SUCC(ret)) {
        uint64_t data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
          LOG_WARN("failed to get data version", K(ret), K(tenant_id_));
        } else if (start_time <= 0 || end_time < start_time) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid spfresh task execution time", K(ret), K(start_time), K(end_time));
        } else if (data_version >= DATA_VERSION_4_5_1_0) {
          int64_t affect_rows = 0;
          ObSqlString update_sql;
          const char *task_info = task_info_json.empty() ? "" : task_info_json.ptr();
          if (data_version >= DATA_VERSION_4_6_0_0
              && OB_FAIL(update_sql.assign_fmt(
                  "UPDATE %s SET task_info = '%.*s', start_time = usec_to_time(%ld),"
                  " end_time = usec_to_time(%ld)"
                  " WHERE tenant_id = %ld AND table_id = %ld"
                  " AND tablet_id = %ld AND task_id = %ld",
                  OB_ALL_VECTOR_INDEX_TASK_TNAME,
                  static_cast<int>(task_info_json.length()), task_info,
                  start_time,
                  end_time,
                  ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_),
                  task.task_key_.index_table_id_,
                  task.task_key_.centroid_tablet_id_,
                  new_task_id))) {
            LOG_WARN("failed to format task_info and execution time update sql", K(ret));
          } else if (data_version < DATA_VERSION_4_6_0_0
                     && OB_FAIL(update_sql.assign_fmt(
                         "UPDATE %s SET task_info = '%.*s'"
                         " WHERE tenant_id = %ld AND table_id = %ld"
                         " AND tablet_id = %ld AND task_id = %ld",
                         OB_ALL_VECTOR_INDEX_TASK_TNAME,
                         static_cast<int>(task_info_json.length()), task_info,
                         ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_),
                         task.task_key_.index_table_id_,
                         task.task_key_.centroid_tablet_id_,
                         new_task_id))) {
            LOG_WARN("failed to format task_info update sql", K(ret));
          } else if (OB_FAIL(sql_proxy->write(tenant_id_, update_sql.ptr(), affect_rows))) {
            LOG_WARN("failed to update task observable columns", K(ret), K(update_sql));
          } else if (1 != affect_rows) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected rows when updating task observable columns",
                     K(ret), K(affect_rows), K(update_sql));
          }
        }
      }

      task_ctx->~ObVecIndexAsyncTaskCtx();
      ob_free(buf);
      buf = nullptr;
    }
  }
  LOG_TRACE("[SPFRESH_TRACE] record_task_history done", K(task), K(task_type), K(task_ret),
            K(task_info_json), K(start_time), K(end_time), KR(ret));
  return ret;
}

}  // namespace share
}  // namespace oceanbase
