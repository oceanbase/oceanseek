/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_EXECUTOR_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_EXECUTOR_H_

#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "share/vector_index/spfresh/ob_spfresh_task_handler.h"
#include "share/vector_index/spfresh/ob_spfresh_split_task.h"
#include "share/vector_index/spfresh/ob_spfresh_merge_task.h"
#include "share/vector_index/spfresh/ob_spfresh_reassign_task.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_scond.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}

namespace share
{

// Per-LS task queue map
class ObSpfreshTaskQueueMap
{
public:
  ObSpfreshTaskQueueMap();
  ~ObSpfreshTaskQueueMap();

  int init();
  void destroy();

  // Get or create task queue for a tablet
  int get_queue(uint64_t tablet_id, ObSpfreshTaskQueue *&queue);

  // Get snapshot of all queue tablet IDs (with lock protection)
  int snapshot_queues(common::ObIArray<uint64_t> &queue_tablet_ids, int64_t &queue_count);

  // Remove task queue for a tablet (called when table/index is dropped)
  // @param [in] tablet_id The tablet ID to remove
  // @return OB_SUCCESS on success, OB_HASH_NOT_EXIST if queue not found
  int remove_queue(uint64_t tablet_id);

private:
  bool is_inited_;
  ObSpinLock lock_;
  hash::ObHashMap<uint64_t, ObSpfreshTaskQueue *> queue_map_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshTaskQueueMap);
};

// SPFresh task executor
// Manages task queues and executes Split/Merge/Reassign tasks
class ObSpfreshTaskExecutor : public ObVecITaskExecutor
{
public:
  ObSpfreshTaskExecutor();
  virtual ~ObSpfreshTaskExecutor();

  // SPFresh stays in the legacy scheduler and does not bind to a specific LS.
  int init(const uint64_t tenant_id);
  int init(const uint64_t tenant_id, storage::ObLSHandle &ls_handle) override;

  // Start handler thread pool on first real dispatch (lazy). Idempotent.
  int start();

  // Stop accepting new task dispatches. Idempotent.
  void stop();

  // Drain scan/dispatch rounds, then stop and join handler threads.
  void wait();

  // Drain admitted rounds/workers, then release handler and queue map.
  void destroy();

  // Load tasks from table on startup (crash recovery). Base class contract.
  // recover_stuck_tasks_ is also invoked from ensure_handler_started_() on
  // the first dispatch so recovered tasks are consumable by handler threads.
  int load_triggered_task(const ObVecIndexTaskStatus &task_row) override;
  int load_task(uint64_t &task_trace_base_num) override;

  // Check and set thread pool
  int check_and_set_thread_pool() override;

  // Check if operation is allowed
  bool check_operation_allow() override;

  // One admitted timer round covers both distribution scanning and dispatch.
  int process_queue();

  // Submit a task to the queue
  // @param [in] task_type The task type (SPLIT/MERGE/REASSIGN)
  // @param [in] index_table_id The IVF index table ID
  // @param [in] tablet_id The centroid tablet ID
  // @param [in] ns_id The SPFresh namespace id for cache key
  int submit_task(ObSpfreshTaskType task_type, uint64_t index_table_id, uint64_t tablet_id,
                   uint64_t center_id, int64_t ns_id, uint64_t ltable_id,
                   int64_t cutover_modify_ts = 0);

  // Get queue map for cleanup (called by ObPluginVectorIndexService)
  ObSpfreshTaskQueueMap &get_queue_map() { return queue_map_; }

private:
  // Only called within an admitted dispatch; never expose an unguarded scan.
  int check_all_spfresh_distributions_();
  int process_queue_();
  bool try_enter_dispatch_();
  void leave_dispatch_();
  void wait_dispatches_();

  class DispatchGuard
  {
  public:
    explicit DispatchGuard(ObSpfreshTaskExecutor &executor)
      : executor_(executor), is_entered_(executor.try_enter_dispatch_()) {}
    ~DispatchGuard()
    {
      if (is_entered_) {
        executor_.leave_dispatch_();
      }
    }
    bool is_entered() const { return is_entered_; }
  private:
    ObSpfreshTaskExecutor &executor_;
    bool is_entered_;
    DISALLOW_COPY_AND_ASSIGN(DispatchGuard);
  };

  class ObSpfreshDistributionSnapshot
  {
  public:
    ObSpfreshDistributionSnapshot();
    void reset();
    int add_normal_center(int64_t cid, int64_t count, int64_t split_retry_after_us);

  private:
    int64_t total_count_;
    int64_t normal_center_count_;
    common::ObSEArray<int64_t, 128> cids_;
    common::ObSEArray<int64_t, 128> counts_;
    common::ObSEArray<int64_t, 128> split_retry_after_us_;
    friend class ObSpfreshTaskExecutor;
    DISALLOW_COPY_AND_ASSIGN(ObSpfreshDistributionSnapshot);
  };

  class ObSpfreshSplitScheduleResult
  {
  public:
    ObSpfreshSplitScheduleResult();

  private:
    // Newly queued splits in this round prevent a merge from using the stale snapshot.
    int64_t submitted_count_;
    // Distinct queued or in-flight splits determine the center deficit and merge gate.
    int64_t scheduled_count_;
    friend class ObSpfreshTaskExecutor;
    DISALLOW_COPY_AND_ASSIGN(ObSpfreshSplitScheduleResult);
  };

  // A split candidate stores only the snapshot index and stable sorting keys.
  struct ObSpfreshSplitCandidate
  {
    int64_t snapshot_idx_;
    int64_t count_;
    int64_t center_id_;

    TO_STRING_KV(K_(snapshot_idx), K_(count), K_(center_id));
  };

  class ObSpfreshSplitCandidateCmp
  {
  public:
    bool operator()(const ObSpfreshSplitCandidate &left,
                    const ObSpfreshSplitCandidate &right) const;
  };

  class CollectDistributionFn
  {
  public:
    explicit CollectDistributionFn(ObSpfreshDistributionSnapshot &snapshot);
    int operator()(int64_t cid, const ObCentroidInfo &info);

  private:
    ObSpfreshDistributionSnapshot &snapshot_;
    DISALLOW_COPY_AND_ASSIGN(CollectDistributionFn);
  };

  class ObSpfreshTaskContext
  {
  public:
    ObSpfreshTaskContext();

  private:
    ObSpfreshTaskQueue *queue_;
    common::ObMySQLProxy *sql_proxy_;
    ObSpfreshKvTableInfo kv_info_;
    uint64_t queue_tablet_id_;
    common::ObTabletID physical_tablet_id_;
    share::ObLSID physical_ls_id_;
    ObIvfMapCentCache *map_cache_;
    ObVectorIndexDistAlgorithm dist_algo_;
    friend class ObSpfreshTaskExecutor;
    DISALLOW_COPY_AND_ASSIGN(ObSpfreshTaskContext);
  };

  // The handler reserves slots with CAS to enforce this hard in-flight limit.
  static constexpr int64_t MAX_CONCURRENT = 64;
  // A barrier-pending REASSIGN yields its handler thread and cannot be
  // redispatched until a later scheduler round.
  static constexpr int64_t REASSIGN_RETRY_DELAY_US = 1000L * 1000L;

  // Process a single task
  int check_index_dropped_(const uint64_t index_table_id, bool &dropped);
  int process_one_task_(ObSpfreshTask &task);
  int prepare_task_context_(ObSpfreshTask &task, ObSpfreshTaskContext &context);
  int process_split_task_(const ObSpfreshTask &task, ObSpfreshTaskContext &context);
  int process_merge_task_(const ObSpfreshTask &task, ObSpfreshTaskContext &context);
  int process_reassign_task_(const ObSpfreshTask &task, ObSpfreshTaskContext &context);
  int record_split_task_history_(const ObSpfreshTask &task,
                                 const ObSpfreshSplitTask &split_task,
                                 int task_ret,
                                 int64_t start_time,
                                 int64_t end_time);

  // Iterate centroids in map_cache and submit SPLIT/MERGE tasks
  int check_and_submit_distribution_tasks_(ObIvfMapCentCache *map_cache,
                                           uint64_t tablet_id, uint64_t index_table_id,
                                           int64_t ns_id, uint64_t ltable_id);
  // The caller holds the per-tablet distribution mutex.
  int do_check_and_submit_distribution_tasks_(ObIvfMapCentCache *map_cache,
                                              uint64_t tablet_id,
                                              uint64_t index_table_id,
                                              int64_t ns_id,
                                              uint64_t ltable_id);
  // Collect posting counts and split retry deadlines for NORMAL centroids.
  int collect_distribution_snapshot_(ObIvfMapCentCache *map_cache,
                                     ObSpfreshDistributionSnapshot &snapshot);
  // Return success with task_submitted=false when the key is already pending.
  int submit_task_if_absent_(ObSpfreshTaskType task_type,
                             uint64_t index_table_id,
                             uint64_t tablet_id,
                             uint64_t center_id,
                             int64_t ns_id,
                             uint64_t ltable_id,
                             bool &task_submitted,
                             int64_t cutover_modify_ts = 0);
  // Count distinct queued or in-flight splits for one index and tablet.
  int get_pending_split_count_(uint64_t index_table_id,
                               uint64_t tablet_id,
                               int64_t ns_id,
                               uint64_t ltable_id,
                               int64_t &split_count);
  // Sort by posting count descending and center id ascending, while keeping the
  // total number of pending splits within the policy limit.
  int submit_split_tasks_(const ObSpfreshDistributionSnapshot &snapshot,
                          int64_t desired_center_count,
                          uint64_t tablet_id,
                          uint64_t index_table_id,
                          int64_t ns_id,
                          uint64_t ltable_id,
                          ObSpfreshSplitScheduleResult &schedule_result);
  int submit_merge_task_(const ObSpfreshDistributionSnapshot &snapshot,
                         int64_t desired_center_count,
                         uint64_t tablet_id,
                         uint64_t index_table_id,
                         int64_t ns_id,
                         uint64_t ltable_id,
                         int64_t &merge_submitted);
  // Recover stuck tasks on startup
  int recover_stuck_tasks_();

  // Lazily start handler threads on first real task dispatch. Idempotent.
  int ensure_handler_started_();

  // Pop and mark in-flight in one critical section to hide the state transition.
  int pop_and_mark_in_flight_(ObSpfreshTaskQueue &queue,
                              ObSpfreshTask &task,
                              bool &marked);

  // Clear the per-tablet in-flight mark after completion, discard, or dispatch failure.
  int unmark_in_flight_(const ObSpfreshTaskKey &key);
  int requeue_in_flight_task_(const ObSpfreshTask &task);

  // Record task execution result to __all_vector_index_task for history archiving
  // Write failure only logs WARN and does not affect task_ret
  // @param [in] task The SPFresh task that was executed
  // @param [in] task_type The ObVecIndexAsyncTaskType enum value for this task
  // @param [in] task_ret The return value of execute()
  // @param [in] task_info_json The JSON string to store in task_info column
  // @param [in] start_time Task execution start timestamp in microseconds
  // @param [in] end_time Task execution end timestamp in microseconds
  int record_task_history_(const ObSpfreshTask &task,
                           int64_t task_type,
                           int task_ret,
                           const common::ObSqlString &task_info_json,
                           int64_t start_time,
                           int64_t end_time);

private:
  // Protect only admission/count changes, never scanning, SQL or worker joins.
  lib::ObMutex dispatch_lock_;
  common::SimpleCond dispatch_cond_;
  bool is_dispatch_stopped_ = true;
  int64_t active_dispatch_count_ = 0;
  ObSpfreshTaskQueueMap queue_map_;
  ObSpfreshTaskHandler handler_;
  // Serializes the first handler start (recover + TG start).
  mutable lib::ObMutex handler_start_lock_;
  int64_t thread_cnt_;
  uint64_t task_trace_base_num_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshTaskExecutor);

  friend class ObSpfreshTaskHandler;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_EXECUTOR_H_
