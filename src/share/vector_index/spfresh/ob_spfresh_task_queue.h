/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_QUEUE_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_QUEUE_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_object.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace share
{

// SPFresh task type enum
enum class ObSpfreshTaskType : int8_t
{
  SPLIT = 0,
  MERGE = 1,
  REASSIGN = 2,
  MAX_TYPE
};

// Task key for deduplication
struct ObSpfreshTaskKey
{
public:
  ObSpfreshTaskKey()
    : index_table_id_(OB_INVALID_ID),
      centroid_tablet_id_(OB_INVALID_ID),
      center_id_(OB_INVALID_ID),
      namespace_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
      logical_table_id_(OB_INVALID_LOGICAL_TABLE_ID),
      task_type_(ObSpfreshTaskType::MAX_TYPE)
  {}

  ObSpfreshTaskKey(uint64_t index_table_id, uint64_t centroid_tablet_id, int64_t center_id)
    : index_table_id_(index_table_id),
      centroid_tablet_id_(centroid_tablet_id),
      center_id_(center_id),
      namespace_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
      logical_table_id_(OB_INVALID_LOGICAL_TABLE_ID),
      task_type_(ObSpfreshTaskType::MAX_TYPE)
  {}

  ObSpfreshTaskKey(uint64_t index_table_id, uint64_t centroid_tablet_id, int64_t center_id,
                   int64_t ns_id, uint64_t ltable_id, ObSpfreshTaskType task_type)
    : index_table_id_(index_table_id),
      centroid_tablet_id_(centroid_tablet_id),
      center_id_(center_id),
      namespace_id_(ns_id),
      logical_table_id_(ltable_id),
      task_type_(task_type)
  {}

  void reset()
  {
    index_table_id_ = OB_INVALID_ID;
    centroid_tablet_id_ = OB_INVALID_ID;
    center_id_ = OB_INVALID_ID;
    namespace_id_ = ObIvfCacheMgrKey::INVALID_NS_ID;
    logical_table_id_ = OB_INVALID_LOGICAL_TABLE_ID;
    task_type_ = ObSpfreshTaskType::MAX_TYPE;
  }

  bool is_valid() const
  {
    return OB_INVALID_ID != index_table_id_ &&
           OB_INVALID_ID != centroid_tablet_id_ &&
           OB_INVALID_ID != center_id_ &&
           ObSpfreshTaskType::SPLIT <= task_type_ &&
           task_type_ < ObSpfreshTaskType::MAX_TYPE;
  }

  int hash(uint64_t &res) const
  {
    res = 0;
    res = murmurhash(&index_table_id_, sizeof(index_table_id_), res);
    res = murmurhash(&centroid_tablet_id_, sizeof(centroid_tablet_id_), res);
    res = murmurhash(&center_id_, sizeof(center_id_), res);
    res = murmurhash(&namespace_id_, sizeof(namespace_id_), res);
    res = murmurhash(&logical_table_id_, sizeof(logical_table_id_), res);
    res = murmurhash(&task_type_, sizeof(task_type_), res);
    return OB_SUCCESS;
  }

  bool operator==(const ObSpfreshTaskKey &other) const
  {
    return index_table_id_ == other.index_table_id_ &&
           centroid_tablet_id_ == other.centroid_tablet_id_ &&
           center_id_ == other.center_id_ &&
           namespace_id_ == other.namespace_id_ &&
           logical_table_id_ == other.logical_table_id_ &&
           task_type_ == other.task_type_;
  }

  TO_STRING_KV(K_(index_table_id), K_(centroid_tablet_id), K_(center_id), K_(namespace_id),
               K_(logical_table_id), K_(task_type));

public:
  uint64_t index_table_id_;      // IVF index table id
  uint64_t centroid_tablet_id_;  // centroid table tablet id
  int64_t center_id_;            // centroid id
  int64_t namespace_id_;         // spfresh namespace id for cache key
  uint64_t logical_table_id_;    // logical table id (@ltable_id) for kv key prefix
  ObSpfreshTaskType task_type_;  // task type for deduplication
};

// SPFresh task structure
struct ObSpfreshTask
{
public:
  ObSpfreshTask()
    : task_type_(ObSpfreshTaskType::MAX_TYPE),
      task_key_(),
      create_time_(0),
      cutover_modify_ts_(0),
      retry_after_us_(0)
  {}

  ObSpfreshTask(ObSpfreshTaskType type, const ObSpfreshTaskKey &key,
                int64_t cutover_modify_ts = 0)
    : task_type_(type),
      task_key_(key),
      create_time_(ObTimeUtility::current_time()),
      cutover_modify_ts_(cutover_modify_ts),
      retry_after_us_(0)
  {
    // Set task_type in task_key for proper deduplication
    task_key_.task_type_ = type;
  }

  void reset()
  {
    task_type_ = ObSpfreshTaskType::MAX_TYPE;
    task_key_.reset();
    create_time_ = 0;
    cutover_modify_ts_ = 0;
    retry_after_us_ = 0;
    cache_guard_.reset();
  }

  bool is_valid() const
  {
    return ObSpfreshTaskType::SPLIT <= task_type_
        && task_type_ < ObSpfreshTaskType::MAX_TYPE
        && task_key_.is_valid()
        && task_type_ == task_key_.task_type_;
  }

  TO_STRING_KV(K_(task_type), K_(task_key), K_(create_time),
               K_(cutover_modify_ts), K_(retry_after_us));

public:
  ObSpfreshTaskType task_type_;
  ObSpfreshTaskKey task_key_;
  int64_t create_time_;
  int64_t cutover_modify_ts_;
  // In-memory dispatch deadline. REASSIGN retries use this instead of
  // sleeping in a handler thread while waiting for the snapshot barrier.
  int64_t retry_after_us_;
  ObIvfCacheMgrGuard cache_guard_;
};

// SPFresh task queue with deduplication
class ObSpfreshTaskQueue
{
public:
  ObSpfreshTaskQueue();
  ~ObSpfreshTaskQueue();

  int init(const char *label = "SpfreshTaskQueue");
  void destroy();

  // Get the metadata associated with this queue under the queue lock.
  int get_metadata(uint64_t &index_table_id,
                   int64_t &namespace_id,
                   uint64_t &logical_table_id) const;

  // Push a task to the queue
  // If a task with the same key already exists, the new task is ignored
  // @param [in] task The task to push
  // @return OB_SUCCESS on success
  int push(const ObSpfreshTask &task);
  // Enqueue a task and report whether it was added; duplicate keys are no-ops.
  int push(const ObSpfreshTask &task, bool &task_pushed);
  // Deduplicate non-topology tasks against queued and in-flight keys.
  int push_if_absent(const ObSpfreshTask &task, bool &task_pushed);
  // Atomically check split deduplication, the merge gate, and the pending limit.
  int push_split_if_below_limit(const ObSpfreshTask &task,
                                int64_t max_pending_split_count,
                                bool &task_pushed);
  // Enqueue a merge only when the tablet has no pending split or merge.
  int push_merge_if_no_distribution_task(const ObSpfreshTask &task,
                                          bool &task_pushed);

  // Pop and mark in-flight while holding the same per-tablet lock.
  int pop_and_mark_in_flight(ObSpfreshTask &task, bool &marked);
  // Atomically clear in-flight and restore a task. REASSIGN retries are put
  // into their dedicated queue and become dispatchable after retry_delay_us.
  // SPLIT/MERGE dispatch failures retain the original front-of-queue behavior.
  int requeue_in_flight_task(const ObSpfreshTask &task, int64_t retry_delay_us);
  // Clear the in-flight mark after task execution finishes.
  int unmark_in_flight(const ObSpfreshTaskKey &key);
  // Count distinct queued or in-flight tasks for the current index.
  int get_pending_task_count(uint64_t index_table_id,
                             int64_t namespace_id,
                             uint64_t logical_table_id,
                             ObSpfreshTaskType task_type,
                             int64_t &task_count) const;

  // Get the number of tasks in the queue
  int64_t size() const;

  // Check if the queue is empty
  bool empty() const;

  // Check if a task with the given key exists
  bool contains(const ObSpfreshTaskKey &key) const;

  // Mutex serializing child center_id allocation across concurrent splits on this
  // tablet. Public so ObSpfreshSplitTask can lock it around migrate_vectors_.
  lib::ObMutex &get_migrate_mutex() { return migrate_mutex_; }
  // Serialize one tablet's distribution scheduling round without executing tasks.
  lib::ObMutex &get_distribution_mutex() { return distribution_mutex_; }

  // Read-only iteration over all tasks in the queue (for virtual table)
  // Callback is called under lock, must not call queue methods to avoid deadlock
  // @param [in] fn Callback function called for each task with (position, task)
  // @return OB_SUCCESS on success
  int foreach_task(
      const std::function<int(int64_t pos, const ObSpfreshTask &task)> &fn) const;

private:
  // The caller holds lock_. Deduplicate queued tasks but permit retries for an
  // in-flight key.
  int push_task_(const ObSpfreshTask &task, bool &task_pushed);
  // The caller holds lock_. Count distinct queued and in-flight tasks.
  int get_pending_task_count_(uint64_t index_table_id,
                              int64_t namespace_id,
                              uint64_t logical_table_id,
                              ObSpfreshTaskType task_type,
                              int64_t &task_count) const;
  static int remove_task_(ObArray<ObSpfreshTask> &tasks, int64_t idx);

  bool is_inited_;
  uint64_t index_table_id_;  // remembered from first pushed task
  int64_t namespace_id_;     // remembered from first pushed task
  uint64_t logical_table_id_; // remembered from first pushed task
  mutable ObSpinLock lock_;
  // Distribution work and barrier-delayed REASSIGN work are deliberately
  // separated so a pending old transaction cannot monopolize the shared
  // handler pool or block SPLIT/MERGE dispatch.
  ObArray<ObSpfreshTask> task_queue_;
  ObArray<ObSpfreshTask> reassign_task_queue_;
  // Hash set for deduplication
  hash::ObHashSet<ObSpfreshTaskKey> task_keys_;
  // Keys popped but not completed share the per-tablet lock with queued state.
  hash::ObHashSet<ObSpfreshTaskKey> in_flight_keys_;
  // Serialize snapshot collection, candidate selection, and submission per tablet.
  lib::ObMutex distribution_mutex_;

  // Per-tablet mutex serializing child center_id allocation in migrate_vectors_.
  // center_id is unique per centroid tablet, so contention is naturally per-tablet;
  // a process-wide lock would needlessly serialize unrelated tablets/tenants.
  // ObMutex (not spinlock) because the critical section spans SQL + batch DML.
  lib::ObMutex migrate_mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshTaskQueue);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_QUEUE_H_
