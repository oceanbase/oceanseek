/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_LS_ASYNC_INDEX_SCHEDULER_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_LS_ASYNC_INDEX_SCHEDULER_H_

#include "logservice/ob_log_base_type.h"  // ObIRoleChangeSubHandler, etc.
#include "lib/lock/ob_spin_lock.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_ls_id.h"
#include "storage/async_index/ob_async_index_define.h"

namespace oceanbase
{
namespace storage
{

class ObLS;
struct ObRetryTaskNode;
class ObTenantAsyncIndexRefresher;
class ObAsyncIndexService;

// LS-level async index incremental refresh scheduler.
//
// Each LS owns one instance of this class, registered to logservice as an
// ObIRoleChangeSubHandler so it receives leader/follower switch notifications
// directly. This replaces the previous design where a single tenant-level
// refresher polled all LS leader sets each round and fenced ALL tasks globally
// on any LS change.
//
// Responsibilities:
//   - Maintain per-LS leader state (is_leader_, need_do_for_switch_).
//   - Maintain per-LS leader epoch for fencing in-flight tasks: only tasks
//     stamped with the current LS epoch are executed; residual tasks from a
//     previous leader term are dropped in handle().
//   - Maintain per-LS retry task list: only the retry tasks belonging to this
//     LS are cancelled on a leader->follower switch, leaving other LSs' retry
//     tasks intact.
//
// The actual timer and worker QUEUE_THREAD are shared at the tenant level
// (owned by ObTenantAsyncIndexRefresher). Each timer round the refresher
// iterates all LS schedulers, skips non-leader ones, and drives refresh for
// the leader LSs using the shared worker.
//
// Lifecycle: init() is called from ObLS::register_to_service_() with the
// tenant-level refresher; it registers this scheduler with the refresher so
// the timer can iterate all LS schedulers. destroy() unregisters from the
// refresher and cleans up retry tasks. The logservice registration/
// unregistration is handled by the REGISTER_TO_LOGSERVICE /
// UNREGISTER_FROM_LOGSERVICE macros in ob_ls.cpp.
class ObLSAsyncIndexScheduler : public logservice::ObIRoleChangeSubHandler,
                                public logservice::ObICheckpointSubHandler,
                                public logservice::ObIReplaySubHandler
{
public:
  ObLSAsyncIndexScheduler();
  virtual ~ObLSAsyncIndexScheduler();

  int init(storage::ObLS *ls, const uint64_t tenant_id,
           ObAsyncIndexService *async_index_service);
  void destroy();

  // ======================== ObIRoleChangeSubHandler ========================
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual void switch_to_follower_forcedly() override;
  virtual int resume_leader() override;

  // ======================== ObICheckpointSubHandler ========================
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &) override { return OB_SUCCESS; }

  // ======================== ObIReplaySubHandler ========================
  virtual int replay(const void *buffer,
                     const int64_t buf_size,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override
  {
    UNUSEDx(buffer, buf_size, lsn, scn);
    return OB_SUCCESS;
  }

  // ======================== Accessors ========================
  share::ObLSID get_ls_id() const { return ls_id_; }
  storage::ObLS *get_ls() const { return ls_; }
  bool is_leader() const { return ATOMIC_LOAD(&is_leader_); }

  // Consume the need_do_for_switch_ flag (CAS: true -> false).
  // Returns true if a switch happened since the last consumption.
  bool consume_switch_flag()
  {
    return ATOMIC_BCAS(&need_do_for_switch_, true, false);
  }

  // ======================== Leader epoch ========================
  // The leader epoch is bumped on every leader->follower or follower->leader
  // transition. Tasks are stamped with the epoch at enqueue time; handle()
  // compares the stamp against the current epoch and drops stale tasks.
  int64_t get_leader_epoch() const { return ATOMIC_LOAD(&leader_epoch_); }

  // ======================== Retry task management ========================
  // Access the per-LS retry list. Caller must hold retry_lock_ while iterating.
  common::ObDList<ObRetryTaskNode> &get_retry_tasks() { return retry_tasks_; }
  common::ObSpinLock &get_retry_lock() { return retry_lock_; }

  // Current number of parked retry tasks on this LS. Takes retry_lock_ internally.
  int64_t retry_task_count() const
  {
    common::ObSpinLockGuard guard(retry_lock_);
    return retry_tasks_.get_size();
  }

  // Cancel all retry tasks parked on this LS (called on leader->follower switch).
  void cancel_retry_tasks();

  // ======================== Liveness reference count ========================
  // This scheduler is an inline member of ObLS (ob_ls.h), so its memory is
  // released together with ObLS. The tenant-level timer and the shared worker,
  // however, reference it across lock boundaries. To make single-LS offline /
  // GC / transfer-out safe while the tenant is still alive, every user
  // (timer round, in-flight worker task) holds a reference for the exact window
  // it dereferences the scheduler; destroy() blocks until the count drains to 0.
  //
  // inc_ref()/dec_ref() are plain atomics. A reference must only ever be taken
  // through ObTenantAsyncIndexRefresher::acquire_scheduler(), which does the
  // "still registered? -> inc_ref" step under the registry lock so it cannot
  // race with mark_stopped_()/unregister (both under the registry write lock).
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  void dec_ref() { ATOMIC_DEC(&ref_cnt_); }
  int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  // Whether new references are refused (set under the registry write lock in
  // unregister_ls_scheduler, right before the scheduler is removed).
  bool is_stopped() const { return ATOMIC_LOAD(&stopped_); }
  void mark_stopped() { ATOMIC_STORE(&stopped_, true); }
  // Read-only snapshot of retry task stats on this LS, used by the refresh
  // progress virtual table. Appends to stats; caller controls when to reset.
  int get_retry_task_stats(common::ObSEArray<ObRefreshTaskStat, 64> &stats) const;

  TO_STRING_KV(K_(ls_id), K_(tenant_id), K_(is_leader), K_(need_do_for_switch),
               K_(leader_epoch));

private:
  // Common logic for switching to follower (used by both graceful and forced).
  void inner_switch_to_follower_();

private:
  bool is_inited_;
  storage::ObLS *ls_;
  share::ObLSID ls_id_;
  uint64_t tenant_id_;
  bool is_leader_;
  bool need_do_for_switch_;
  int64_t leader_epoch_;
  ObTenantAsyncIndexRefresher *refresher_;  // tenant-level refresher (register/unregister)
  // Liveness reference count: number of in-flight users (timer round + executing
  // worker tasks) currently dereferencing this scheduler. destroy() waits for
  // this to reach 0 before letting ObLS free the scheduler's memory. stopped_
  // refuses new references once destroy() begins.
  volatile int64_t ref_cnt_;
  volatile bool stopped_;

  // Per-LS retry list for failed tasks that need deferred retry.
  common::ObDList<ObRetryTaskNode> retry_tasks_;
  mutable common::ObSpinLock retry_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObLSAsyncIndexScheduler);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_LS_ASYNC_INDEX_SCHEDULER_H_
