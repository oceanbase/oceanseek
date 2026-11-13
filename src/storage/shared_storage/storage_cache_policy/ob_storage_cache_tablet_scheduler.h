/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_CACHE_TABLET_SCHEDULER_H_
#define OCEANBASE_STORAGE_CACHE_TABLET_SCHEDULER_H_
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_ptr_handle.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
namespace oceanbase
{
namespace storage
{
class ObStorageCacheTabletTask;
typedef ObPtrHandle<ObStorageCacheTabletTask> ObStorageCacheTabletTaskHandle;
typedef hash::ObHashMap<int64_t, ObStorageCacheTabletTaskHandle> SCPTabletTaskMap;

static const int64_t DEFAULT_LS_SIZE = 8; // Used to obtain log stream service

int get_macro_blocks(const ObLSID &ls_id, 
                     const ObTabletID &tablet_id, 
                     ObIArray<MacroBlockId> &data_block_ids, 
                     ObIArray<MacroBlockId> &meta_block_ids);
int get_tablet_ls_id(const int64_t tenant_id, const uint64_t tablet_id, int64_t &ls_id);

enum ObStorageCacheTaskType : uint8_t
{ 
  OB_STORAGE_CACHE_PARTITION_TASK = 0,
  OB_STORAGE_CACHE_TASK_TYPE_MAX = 2
};

class ObStorageCacheTabletScheduler : public lib::TGTaskHandler
{
public:
  ObStorageCacheTabletScheduler();
  virtual ~ObStorageCacheTabletScheduler() { destroy(); };
  static int get_tablet_handle(const ObLSID &ls_id,
                               const ObTabletID &tablet_id,
                               ObTabletHandle &tablet_handle);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait(); 
  void destroy();
  int check_is_active_tablet(const int64_t tablet_id, bool &is_active) const;
  int generate_partition_task(const uint64_t tenant_id, const int64_t tablet_id, const PolicyStatus &policy_status);
  int push_task(ObStorageCacheTabletTaskHandle &task_handle);
  int cancel_task(ObStorageCacheTabletTaskHandle &task_handle);
  // schema 不可服务时强制取消任意状态任务，阻止 resume/skipped 路径重新生成。
  int invalidate_task_by_tablet_id(const int64_t tablet_id);
  virtual void handle(void *task_handle) override;
  virtual void handle_drop(void *task_handle) override;
  int convert_hot_to_auto(const int64_t tablet_id);
  int convert_macro_blks_to_auto(const ObIArray<MacroBlockId> &block_ids);

  int get_skipped_macro_cache_tasks(ObIArray<uint64_t> &unfinished_tablet_ids);
  int trigger_skipped_macro_cache_tablets();
  // Collect tablet IDs whose tasks failed to enqueue and are waiting for a periodic retry.
  int get_retry_waiting_tablet_ids(ObIArray<uint64_t> &retry_tablet_ids) const;
  // Regenerate tasks whose last enqueue attempt returned OB_EAGAIN.
  int trigger_retry_waiting_tasks();
  int regenerate_task(const ObStorageCacheTabletTaskHandle &task_handle);
  bool get_exist_skipped_tablet() const { return exist_skipped_tablet_; }
  void set_exist_skipped_tablet(const bool exist_skipped_tablet) { exist_skipped_tablet_ = exist_skipped_tablet; }
  bool has_retry_waiting_task() const { return ATOMIC_LOAD(&has_retry_waiting_task_); }
  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(tg_id),
      "has_retry_waiting_task", has_retry_waiting_task());
public:
  static const int64_t MAX_TASK_NUM = 10000; // Maximum number of tasks in a queue thread
  static const int64_t DEFAULT_TABLET_TASK_MAP_SIZE = 10000; // The maximum number of tasks in the task map
public:
  SCPTabletTaskMap tablet_task_map_;
  hash::ObHashSet<uint64_t> active_tablet_ids_;
  bool exist_skipped_tablet_; // When this value is true, it means that there exists tablet in the current task that skips the macro cache.

private:
  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
  bool has_retry_waiting_task_; // Indicates that tablet_task_map_ may contain tasks waiting to be re-enqueued.
};

struct ObStorageCacheTaskStatus
{
  enum TaskStatus : uint8_t
  {
    OB_STORAGE_CACHE_TASK_INIT = 0,
    OB_STORAGE_CACHE_TASK_DOING = 1,
    OB_STORAGE_CACHE_TASK_FINISHED = 2,
    OB_STORAGE_CACHE_TASK_FAILED = 3,
    OB_STORAGE_CACHE_TASK_SUSPENDED = 4,
    OB_STORAGE_CACHE_TASK_CANCELED = 5,
    OB_STORAGE_CACHE_TASK_STATUS_MAX
  };
  static const char *get_str(const TaskStatus &type);
  static OB_INLINE bool is_valid(const TaskStatus &type)
  {
    return type >= 0 && type < TaskStatus::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  }
  static bool is_completed_status(const TaskStatus &type)
  {
    return type == TaskStatus::OB_STORAGE_CACHE_TASK_FINISHED
        || type == TaskStatus::OB_STORAGE_CACHE_TASK_FAILED
        || type == TaskStatus::OB_STORAGE_CACHE_TASK_CANCELED;
  }
  static bool is_suspended_status(const TaskStatus &type)
  {
    return type == TaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED;
  }
  static bool is_finished_or_failed(const TaskStatus &type)
  {
    return (TaskStatus::OB_STORAGE_CACHE_TASK_FINISHED == type ||
            TaskStatus::OB_STORAGE_CACHE_TASK_FAILED == type);
  }
};
typedef ObStorageCacheTaskStatus::TaskStatus ObStorageCacheTaskStatusType;
class ObStorageCacheTabletTask
{
public:
  ObStorageCacheTabletTask();
  ~ObStorageCacheTabletTask() {
    destroy();
  };
  int init_partition_task(const uint64_t tenant_id, const int64_t tablet_id, const PolicyStatus &policy_status);
  void destroy();
  void inc_ref_count();
  void dec_ref_count();
  int process();
  ObStorageCacheTaskStatusType get_status() const;
  PolicyStatus get_policy_status() const;
  bool is_hot() const;
  bool is_auto() const;
  bool is_cold() const;
  bool is_canceled() const;
  bool is_completed() const;
  bool is_suspended() const;
  bool is_uncompleted() const;

  bool is_valid() const;
  bool is_skipped_macro_cache() const;
  // 仅 HOT skipped task 需要重试；AUTO 空间不足时保留结束态，不参与自动重试，避免长期阻塞 schema refresh。
  bool need_retry_skipped_task() const;
  bool need_retry_push_task() const;
  bool is_partition_task() const { return task_type_ == ObStorageCacheTaskType::OB_STORAGE_CACHE_PARTITION_TASK; }
  
  int64_t get_tablet_id() const;
  int64_t get_ls_id() const;
  uint64_t get_tenant_id() const;
  int64_t get_ref_count() const;
  ObSSPrewarmStat get_prewarm_stat() const;
  double get_speed() const;
  int64_t get_start_time() const;
  int64_t get_end_time() const;
  int get_result() const;
  int get_comment(common::ObIAllocator &allocator, ObString &comment) const;
  ObStorageCacheTaskType get_task_type() const;

  // 将任意有效状态强制置为 CANCELED，阻止 resume/skipped 路径重新生成任务。
  int set_canceled();
  /**
   * @brief Attempts to transition the task to the target status with concurrency control
   * 
   * @param target_status Target state to transition to
   * @param origin_status [out] Returns the original state before transition attempt
   * @param succ          [out] Indicates whether the state transition succeeded
   * 
   * @return int - Operation status codes:
   *   OB_FAIL: Indicates invalid task state or illegal transition path
   *   OB_SUCCESS with succ=false: Valid state but transition failed due to concurrent modifications
   *   OB_SUCCESS with succ=true: Successful state transition to target_status
   */
  int set_status(
      const ObStorageCacheTaskStatusType &target_status,
      ObStorageCacheTaskStatusType &origin_status,
      bool &succ);
  // == set_status(target_status, origin_status, succ) without [out] param
  int set_status(const ObStorageCacheTaskStatusType &target_status);
  // Record a scheduling failure. OB_EAGAIN waits for a periodic retry; other errors are terminal.
  int mark_schedule_failure(const int schedule_ret);
  void set_tablet_id(const int64_t tablet_id);
  void set_start_time(const int64_t start_time);
  // This function is only used for test!!!!
  void set_end_time(const int64_t end_time);
  void set_ls_id(const int64_t ls_id);
  void set_tenant_id(const uint64_t tenant_id);
  void set_prewarm_stat(const ObSSPrewarmStat &stat);
  void set_policy_status(const PolicyStatus &policy_status);
  void set_task_type(const ObStorageCacheTaskType task_type);

  int do_prewarm(); // Provided for hot task
  int generate_comment();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(ref_count),
      K_(is_inited), K_(is_stop), K(prewarm_stat_),
      K(start_time_us_), K(end_time_us_), K(result_),
      K_(status), K(ObStorageCacheTaskStatus::get_str(status_)), K(policy_status_));
      
public:
  static const int64_t MAX_RETRY_TIMES = 3;
  static const int64_t RETRY_INTERVAL = 100 * 1000; // 100ms

private:
  // This function is only used for test!!!!
  int init_for_test(const uint64_t tenant_id, const int64_t ls_id, const int64_t tablet_id, const PolicyStatus &policy_status);

private:
  bool is_inited_;
  bool is_stop_;
  volatile bool is_skipped_macro_cache_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t ref_count_;
  ObStorageCacheTaskType task_type_;
  common::SpinRWLock task_lock_;
  ObStorageCacheTaskStatusType status_;
  PolicyStatus policy_status_;
  int64_t start_time_us_;
  int64_t end_time_us_;
  ObString comment_;
  ObArenaAllocator comment_allocator_; // Owns the backing memory of comment_ only.
  int result_;
  ObSSPrewarmStat prewarm_stat_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageCacheTabletTask);
};
}
}
#endif
