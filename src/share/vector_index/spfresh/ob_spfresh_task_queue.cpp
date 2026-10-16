/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

#include <functional>

namespace oceanbase
{
namespace share
{

ObSpfreshTaskQueue::ObSpfreshTaskQueue()
  : is_inited_(false),
    index_table_id_(OB_INVALID_ID),
    namespace_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
    logical_table_id_(OB_INVALID_LOGICAL_TABLE_ID),
    lock_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    task_queue_(),
    reassign_task_queue_(),
    task_keys_(),
    in_flight_keys_(),
    distribution_mutex_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK),
    migrate_mutex_(common::ObLatchIds::OB_SPFRESH_TASK_QUEUE_LOCK)
{
}

ObSpfreshTaskQueue::~ObSpfreshTaskQueue()
{
  destroy();
}

int ObSpfreshTaskQueue::init(const char *label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshTaskQueue init twice", K(ret));
  } else if (OB_FAIL(task_keys_.create(
      1024, lib::ObMemAttr(OB_SERVER_TENANT_ID, "SpfreshTaskKey")))) {
    LOG_WARN("failed to create task keys set", K(ret));
  } else if (OB_FAIL(in_flight_keys_.create(
      64, lib::ObMemAttr(OB_SERVER_TENANT_ID, "SpfreshInFly")))) {
    task_keys_.destroy();
    LOG_WARN("failed to create in-flight keys set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSpfreshTaskQueue::destroy()
{
  if (is_inited_) {
    common::ObSpinLockGuard guard(lock_);
    task_queue_.reset();
    reassign_task_queue_.reset();
    task_keys_.destroy();
    in_flight_keys_.destroy();
    is_inited_ = false;
  }
}

int ObSpfreshTaskQueue::push(const ObSpfreshTask &task)
{
  int ret = OB_SUCCESS;
  bool task_pushed = false;

  if (OB_FAIL(push(task, task_pushed))) {
    LOG_WARN("failed to push task", K(ret), K(task));
  }
  return ret;
}

int ObSpfreshTaskQueue::push(const ObSpfreshTask &task, bool &task_pushed)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  task_pushed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (ObSpfreshTaskType::SPLIT == task.task_type_
        || ObSpfreshTaskType::MERGE == task.task_type_) {
      hash_ret = in_flight_keys_.exist_refactored(task.task_key_);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("topology retry is not in flight", K(ret), K(task));
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to check topology retry", K(ret), K(task));
      } else if (OB_FAIL(push_task_(task, task_pushed))) {
        LOG_WARN("failed to push topology retry", K(ret), K(task));
      }
    } else if (OB_FAIL(push_task_(task, task_pushed))) {
      LOG_WARN("failed to push task under queue lock", K(ret), K(task));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] push done", K(task), K(task_pushed), KR(ret));
  return ret;
}

int ObSpfreshTaskQueue::push_if_absent(
    const ObSpfreshTask &task,
    bool &task_pushed)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  task_pushed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!task.is_valid()
             || ObSpfreshTaskType::SPLIT == task.task_type_
             || ObSpfreshTaskType::MERGE == task.task_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid non-topology task", K(ret), K(task));
  } else {
    common::ObSpinLockGuard guard(lock_);
    hash_ret = in_flight_keys_.exist_refactored(task.task_key_);
    if (OB_HASH_EXIST == hash_ret) {
      LOG_TRACE("[SPFRESH_TRACE] task already in flight, ignore", K(task));
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to check in-flight task", K(ret), K(task));
    } else if (OB_FAIL(push_task_(task, task_pushed))) {
      LOG_WARN("failed to push absent task", K(ret), K(task));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] push_if_absent done", K(task), K(task_pushed), KR(ret));
  return ret;
}

int ObSpfreshTaskQueue::push_split_if_below_limit(
    const ObSpfreshTask &task,
    int64_t max_pending_split_count,
    bool &task_pushed)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  int64_t pending_merge_count = 0;
  int64_t pending_split_count = 0;
  task_pushed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!task.is_valid()
             || ObSpfreshTaskType::SPLIT != task.task_type_
             || ObSpfreshTaskType::SPLIT != task.task_key_.task_type_
             || 0 >= max_pending_split_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split task or limit",
             K(ret), K(task), K(max_pending_split_count));
  } else {
    common::ObSpinLockGuard guard(lock_);
    hash_ret = in_flight_keys_.exist_refactored(task.task_key_);
    if (OB_HASH_EXIST == hash_ret) {
      LOG_TRACE("[SPFRESH_TRACE] split already in flight, ignore", K(task));
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to check in-flight split", K(ret), K(task));
    } else if (OB_HASH_EXIST
               == (hash_ret = task_keys_.exist_refactored(task.task_key_))) {
      LOG_TRACE("[SPFRESH_TRACE] split already queued, ignore", K(task));
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to check queued split", K(ret), K(task));
    } else if (OB_FAIL(get_pending_task_count_(
        task.task_key_.index_table_id_, task.task_key_.namespace_id_,
        task.task_key_.logical_table_id_, ObSpfreshTaskType::MERGE,
        pending_merge_count))) {
      LOG_WARN("failed to count pending merge tasks", K(ret), K(task));
    } else if (0 < pending_merge_count) {
      LOG_TRACE("[SPFRESH_TRACE] pending merge blocks split",
                K(task), K(pending_merge_count));
    } else if (OB_FAIL(get_pending_task_count_(
        task.task_key_.index_table_id_, task.task_key_.namespace_id_,
        task.task_key_.logical_table_id_, ObSpfreshTaskType::SPLIT,
        pending_split_count))) {
      LOG_WARN("failed to count pending split tasks", K(ret), K(task));
    } else if (pending_split_count >= max_pending_split_count) {
      LOG_TRACE("[SPFRESH_TRACE] pending split limit reached",
                K(task), K(pending_split_count), K(max_pending_split_count));
    } else if (OB_FAIL(push_task_(task, task_pushed))) {
      LOG_WARN("failed to push split task", K(ret), K(task));
    }
  }

  return ret;
}

int ObSpfreshTaskQueue::push_merge_if_no_distribution_task(
    const ObSpfreshTask &task,
    bool &task_pushed)
{
  int ret = OB_SUCCESS;
  int64_t pending_split_count = 0;
  int64_t pending_merge_count = 0;
  task_pushed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!task.is_valid()
             || ObSpfreshTaskType::MERGE != task.task_type_
             || ObSpfreshTaskType::MERGE != task.task_key_.task_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge task", K(ret), K(task));
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (OB_FAIL(get_pending_task_count_(
        task.task_key_.index_table_id_, task.task_key_.namespace_id_,
        task.task_key_.logical_table_id_, ObSpfreshTaskType::SPLIT,
        pending_split_count))) {
      LOG_WARN("failed to count pending split tasks", K(ret), K(task));
    } else if (OB_FAIL(get_pending_task_count_(
        task.task_key_.index_table_id_, task.task_key_.namespace_id_,
        task.task_key_.logical_table_id_, ObSpfreshTaskType::MERGE,
        pending_merge_count))) {
      LOG_WARN("failed to count pending merge tasks", K(ret), K(task));
    } else if (0 < pending_split_count || 0 < pending_merge_count) {
      LOG_TRACE("[SPFRESH_TRACE] distribution task exists, skip merge",
                K(task), K(pending_split_count), K(pending_merge_count));
    } else if (OB_FAIL(push_task_(task, task_pushed))) {
      LOG_WARN("failed to push merge task", K(ret), K(task));
    }
  }

  return ret;
}

int ObSpfreshTaskQueue::push_task_(
    const ObSpfreshTask &task,
    bool &task_pushed)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObArray<ObSpfreshTask> &target_queue =
      ObSpfreshTaskType::REASSIGN == task.task_type_
          ? reassign_task_queue_ : task_queue_;
  task_pushed = false;
  hash_ret = task_keys_.exist_refactored(task.task_key_);
  if (OB_HASH_EXIST == hash_ret) {
    // Treat an existing queued key as idempotent success.
    LOG_TRACE("[SPFRESH_TRACE] task already exists, ignore", K(task));
  } else if (OB_HASH_NOT_EXIST == hash_ret) {
    // Insert the task before registering its queued key.
    if (OB_FAIL(target_queue.push_back(task))) {
      LOG_WARN("failed to push task to queue", K(ret), K(task));
    } else if (OB_FAIL(task_keys_.set_refactored(task.task_key_))) {
      // Roll back the queue insertion if key registration fails.
      target_queue.at(target_queue.count() - 1).reset();
      target_queue.pop_back();
      LOG_WARN("failed to add task key to set", K(ret), K(task));
    } else {
      task_pushed = true;
      // The first task records queue metadata for later distribution checks.
      if (OB_INVALID_ID == index_table_id_) {
        index_table_id_ = task.task_key_.index_table_id_;
        namespace_id_ = task.task_key_.namespace_id_;
        logical_table_id_ = task.task_key_.logical_table_id_;
      }
    }
  } else {
    ret = hash_ret;
    LOG_WARN("failed to check task existence", K(ret), K(task));
  }
  return ret;
}

int ObSpfreshTaskQueue::remove_task_(ObArray<ObSpfreshTask> &tasks, int64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= tasks.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    for (int64_t i = idx; i + 1 < tasks.count(); ++i) {
      tasks.at(i) = tasks.at(i + 1);
    }
    tasks.at(tasks.count() - 1).reset();
    tasks.pop_back();
  }
  return ret;
}

int ObSpfreshTaskQueue::pop_and_mark_in_flight(
    ObSpfreshTask &task,
    bool &marked)
{
  int ret = OB_SUCCESS;
  task.reset();
  marked = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObArray<ObSpfreshTask> *target_queue = nullptr;
    int64_t task_idx = -1;
    if (!task_queue_.empty()) {
      target_queue = &task_queue_;
      task_idx = 0;
    } else {
      const int64_t now_us = ObTimeUtility::current_time();
      for (int64_t i = 0; task_idx < 0 && i < reassign_task_queue_.count(); ++i) {
        if (reassign_task_queue_.at(i).retry_after_us_ <= now_us) {
          target_queue = &reassign_task_queue_;
          task_idx = i;
        }
      }
    }
    if (OB_ISNULL(target_queue)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int hash_ret = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      task = target_queue->at(task_idx);
      hash_ret = in_flight_keys_.exist_refactored(task.task_key_);
      if (OB_HASH_EXIST == hash_ret) {
        // Keep a same-key retry at the front until the current task completes.
      } else if (OB_HASH_NOT_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to check in-flight key", K(ret), K(task));
      } else if (OB_FAIL(in_flight_keys_.set_refactored(task.task_key_))) {
        LOG_WARN("failed to add in-flight key", K(ret), K(task));
      } else if (OB_SUCCESS != (hash_ret = task_keys_.erase_refactored(task.task_key_))) {
        ret = (OB_HASH_NOT_EXIST == hash_ret) ? OB_ERR_UNEXPECTED : hash_ret;
        tmp_ret = in_flight_keys_.erase_refactored(task.task_key_);
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("failed to rollback in-flight key", K(tmp_ret), K(task));
        }
        LOG_WARN("failed to erase queued task key", K(ret), K(hash_ret), K(task));
      } else if (OB_FAIL(remove_task_(*target_queue, task_idx))) {
        tmp_ret = task_keys_.set_refactored(task.task_key_);
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("failed to restore queued task key", K(tmp_ret), K(task));
        }
        tmp_ret = in_flight_keys_.erase_refactored(task.task_key_);
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("failed to rollback in-flight key", K(tmp_ret), K(task));
        }
        LOG_WARN("failed to remove task from queue", K(ret), K(task));
      } else {
        marked = true;
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] pop_and_mark_in_flight done", K(task), K(marked), KR(ret));
  return ret;
}

int ObSpfreshTaskQueue::unmark_in_flight(const ObSpfreshTaskKey &key)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task key", K(ret), K(key));
  } else {
    common::ObSpinLockGuard guard(lock_);
    hash_ret = in_flight_keys_.erase_refactored(key);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // Treat duplicate cleanup as idempotent success.
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to erase in-flight key", K(ret), K(key));
    }
  }
  return ret;
}

int ObSpfreshTaskQueue::requeue_in_flight_task(
    const ObSpfreshTask &task,
    int64_t retry_delay_us)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t queued_idx = -1;
  int64_t inserted_idx = -1;
  bool task_inserted = false;
  ObSpfreshTask queued_task = task;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (!task.is_valid() || retry_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task or retry delay", K(ret), K(task), K(retry_delay_us));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObArray<ObSpfreshTask> &target_queue =
        ObSpfreshTaskType::REASSIGN == task.task_type_
            ? reassign_task_queue_ : task_queue_;
    queued_task.retry_after_us_ = retry_delay_us > 0
        ? ObTimeUtility::current_time() + retry_delay_us : 0;
    hash_ret = in_flight_keys_.exist_refactored(task.task_key_);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("task is not in flight", K(ret), K(task));
    } else if (OB_HASH_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to check in-flight task", K(ret), K(task));
    } else {
      hash_ret = task_keys_.exist_refactored(task.task_key_);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(task_keys_.set_refactored(task.task_key_))) {
          LOG_WARN("failed to restore queued task key", K(ret), K(task));
        } else if (OB_FAIL(target_queue.push_back(queued_task))) {
          tmp_ret = task_keys_.erase_refactored(task.task_key_);
          if (OB_SUCCESS != tmp_ret) {
            LOG_ERROR("failed to rollback queued task key", K(tmp_ret), K(task));
          }
          LOG_WARN("failed to restore task to queue", K(ret), K(task));
        } else {
          if (ObSpfreshTaskType::REASSIGN == task.task_type_) {
            inserted_idx = target_queue.count() - 1;
          } else {
            for (int64_t i = target_queue.count() - 1; i > 0; --i) {
              target_queue.at(i) = target_queue.at(i - 1);
            }
            target_queue.at(0) = queued_task;
            inserted_idx = 0;
          }
          task_inserted = true;
        }
      } else if (OB_HASH_EXIST == hash_ret) {
        for (int64_t i = 0; queued_idx < 0 && i < target_queue.count(); ++i) {
          if (task.task_key_ == target_queue.at(i).task_key_) {
            queued_idx = i;
          }
        }
        if (queued_idx < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("queued task key has no task", K(ret), K(task));
        } else if (ObSpfreshTaskType::REASSIGN == task.task_type_) {
          for (int64_t i = queued_idx; i + 1 < target_queue.count(); ++i) {
            target_queue.at(i) = target_queue.at(i + 1);
          }
          target_queue.at(target_queue.count() - 1) = queued_task;
        } else {
          for (int64_t i = queued_idx; i > 0; --i) {
            target_queue.at(i) = target_queue.at(i - 1);
          }
          target_queue.at(0) = queued_task;
        }
      } else {
        ret = hash_ret;
        LOG_WARN("failed to check queued task", K(ret), K(task));
      }
      if (OB_SUCC(ret)) {
        hash_ret = in_flight_keys_.erase_refactored(task.task_key_);
        if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          if (task_inserted) {
            tmp_ret = remove_task_(target_queue, inserted_idx);
            if (OB_SUCCESS != tmp_ret) {
              LOG_ERROR("failed to rollback restored task", K(tmp_ret), K(task));
            }
            tmp_ret = task_keys_.erase_refactored(task.task_key_);
            if (OB_SUCCESS != tmp_ret) {
              LOG_ERROR("failed to rollback restored key", K(tmp_ret), K(task));
            }
          }
          LOG_WARN("failed to clear in-flight task", K(ret), K(task));
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTaskQueue::get_pending_task_count(
    uint64_t index_table_id,
    int64_t namespace_id,
    uint64_t logical_table_id,
    ObSpfreshTaskType task_type,
    int64_t &task_count) const
{
  int ret = OB_SUCCESS;
  task_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else if (ObSpfreshTaskType::MAX_TYPE == task_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", K(ret), K(task_type));
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (OB_FAIL(get_pending_task_count_(index_table_id, namespace_id,
        logical_table_id, task_type, task_count))) {
      LOG_WARN("failed to get pending task count", K(ret), K(index_table_id),
               K(namespace_id), K(logical_table_id), K(task_type));
    }
  }
  return ret;
}

int ObSpfreshTaskQueue::get_pending_task_count_(
    uint64_t index_table_id,
    int64_t namespace_id,
    uint64_t logical_table_id,
    ObSpfreshTaskType task_type,
    int64_t &task_count) const
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  task_count = 0;

  for (int64_t i = 0; i < task_queue_.count(); ++i) {
    const ObSpfreshTaskKey &key = task_queue_.at(i).task_key_;
    if (index_table_id == key.index_table_id_
        && namespace_id == key.namespace_id_
        && logical_table_id == key.logical_table_id_
        && task_type == key.task_type_) {
      ++task_count;
    }
  }
  for (int64_t i = 0; i < reassign_task_queue_.count(); ++i) {
    const ObSpfreshTaskKey &key = reassign_task_queue_.at(i).task_key_;
    if (index_table_id == key.index_table_id_
        && namespace_id == key.namespace_id_
        && logical_table_id == key.logical_table_id_
        && task_type == key.task_type_) {
      ++task_count;
    }
  }
  for (hash::ObHashSet<ObSpfreshTaskKey>::const_iterator iter = in_flight_keys_.begin();
       OB_SUCC(ret) && iter != in_flight_keys_.end(); ++iter) {
    const ObSpfreshTaskKey &key = iter->first;
    if (index_table_id == key.index_table_id_
        && namespace_id == key.namespace_id_
        && logical_table_id == key.logical_table_id_
        && task_type == key.task_type_) {
      hash_ret = task_keys_.exist_refactored(key);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ++task_count;
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to check queued task key", K(ret));
      }
    }
  }
  return ret;
}

int ObSpfreshTaskQueue::get_metadata(uint64_t &index_table_id,
                                     int64_t &namespace_id,
                                     uint64_t &logical_table_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    index_table_id = index_table_id_;
    namespace_id = namespace_id_;
    logical_table_id = logical_table_id_;
  }
  return ret;
}

int64_t ObSpfreshTaskQueue::size() const
{
  int64_t size = 0;
  if (is_inited_) {
    common::ObSpinLockGuard guard(lock_);
    size = task_queue_.size() + reassign_task_queue_.size();
  }
  return size;
}

bool ObSpfreshTaskQueue::empty() const
{
  bool is_empty = true;
  if (is_inited_) {
    common::ObSpinLockGuard guard(lock_);
    is_empty = task_queue_.empty() && reassign_task_queue_.empty();
  }
  return is_empty;
}

bool ObSpfreshTaskQueue::contains(const ObSpfreshTaskKey &key) const
{
  bool contains_key = false;
  if (is_inited_) {
    common::ObSpinLockGuard guard(lock_);
    contains_key = (OB_HASH_EXIST == task_keys_.exist_refactored(key));
  }
  return contains_key;
}

int ObSpfreshTaskQueue::foreach_task(
    const std::function<int(int64_t pos, const ObSpfreshTask &task)> &fn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTaskQueue not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_queue_.size(); ++i) {
      if (OB_FAIL(fn(i, task_queue_.at(i)))) {
        LOG_WARN("foreach_task callback failed", K(ret), K(i));
      }
    }
    const int64_t distribution_count = task_queue_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < reassign_task_queue_.size(); ++i) {
      if (OB_FAIL(fn(distribution_count + i, reassign_task_queue_.at(i)))) {
        LOG_WARN("foreach reassign task callback failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
