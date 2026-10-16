/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_TENANT_ASYNC_INDEX_REFRESHER_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_TENANT_ASYNC_INDEX_REFRESHER_H_

#include "lib/task/ob_timer.h"  // ObTimerTask
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_async_index_trigger.h"
#include "share/ob_ls_id.h"
#include "share/ob_common_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
class ObSchemaGetterGuard;
}
}

namespace storage
{

class ObTenantAsyncIndexRefresher;
class ObLSAsyncIndexScheduler;

// Periodic timer task that drives async index incremental refresh scheduling.
// Runs unconditionally on a TIMER thread group; each round the task body
// iterates the registered LS schedulers, processes only the leader ones, and
// drives refresh for the tablets on those LSs using the shared worker.
class ObAsyncIndexRefreshTimerTask : public common::ObTimerTask
{
public:
  ObAsyncIndexRefreshTimerTask() : host_(nullptr) {}
  virtual ~ObAsyncIndexRefreshTimerTask() {}
  int init(ObTenantAsyncIndexRefresher *host);
  virtual void runTimerTask() override;
private:
  ObTenantAsyncIndexRefresher *host_;
};

// Tenant-level async index incremental refresh manager.
//
// Lifecycle managed by ObAsyncIndexService, which calls init()/start()/stop()/
// wait()/destroy(). It is NOT bound to any single LS role: the manager runs on
// every node and owns the shared timer and worker thread.
//
// Architecture (TIMER + per-LS scheduler model):
//   - Holds a TIMER thread group (tg_id_) running ObAsyncIndexRefreshTimerTask
//     on a fixed interval. The timer fires unconditionally.
//   - Each LS registers its ObLSAsyncIndexScheduler via register_ls_scheduler()
//     during LS creation, and unregisters via unregister_ls_scheduler() during
//     LS teardown. The schedulers receive leader/follower switch notifications
//     directly from logservice (ObIRoleChangeSubHandler).
//   - Each timer round iterates the registered LS schedulers. For each leader
//     LS, it processes retry tasks and enumerates async-index data tablets on
//     that LS, evaluates trigger conditions, and enqueues refresh tasks into
//     the shared worker. Non-leader LSs are skipped.
//   - A node that leads no LS does nothing (low base noise).
//   - When an LS switches from leader to follower, only that LS's retry tasks
//     are cancelled and only that LS's in-flight tasks are fenced (via the
//     LS-level leader epoch), leaving other LSs' tasks intact.
class ObTenantAsyncIndexRefresher
{
public:
  ObTenantAsyncIndexRefresher();
  ~ObTenantAsyncIndexRefresher();

  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();

  // Register/unregister an LS scheduler. Called from ObLS lifecycle.
  int register_ls_scheduler(ObLSAsyncIndexScheduler *scheduler);
  // Mark this exact scheduler stopped and remove it from the registry under the
  // write lock. Multiple ObLS instances with the same LS id may overlap during
  // create failure cleanup, so matching by LS id can remove the wrong instance.
  // After this returns, acquire_scheduler() can no longer hand out a fresh
  // reference to this scheduler; already-held references are drained by the caller.
  void unregister_ls_scheduler(ObLSAsyncIndexScheduler *scheduler);

  // Look up a still-registered scheduler by LS id and take a liveness reference
  // on it, all under the registry lock so it cannot race with unregister. Returns
  // nullptr if the LS is not (or no longer) registered -- the caller must then
  // abandon the work targeting that LS. Every non-null return MUST be balanced by
  // a release_scheduler() once the caller stops dereferencing the scheduler.
  // This is the ONLY safe way for the shared worker to reach a per-LS scheduler,
  // since the raw pointer stamped on a queued task may already have been freed.
  ObLSAsyncIndexScheduler *acquire_scheduler(const share::ObLSID &ls_id);
  void release_scheduler(ObLSAsyncIndexScheduler *scheduler);

  // Timer task body (invoked by ObAsyncIndexRefreshTimerTask::runTimerTask).
  void run_refresh_task();

  // Accessor for the shared worker (used by tests).
  ObAsyncIndexRefreshWorker &get_refresh_worker() { return refresh_worker_; }

  // Accessor for the production dep provider (used by the direct-trigger path
  // in ObAsyncIndexService). Returns the concrete ObAsyncIndexDepProvider so the
  // caller can invoke get_single_table_refresh_info without a full deps scan.
  ObAsyncIndexDepProvider &get_dep_provider() { return dep_provider_; }
  // Collects retry task stats from leader schedulers under ls_schedulers_lock_ (read), then
  // collects in-flight worker task stats outside that lock. Allowed to drift
  // ±1 against the worker; LS side is atomic relative to scheduler create/
  // destroy and role-change callbacks.
  int build_view_snapshot(ObAsyncIndexRefreshViewSnapshot &snapshot) const;

private:
  // Map from LS id to the scheduler that LEADS it this round. Built once per
  // timer round and used to route data tablets to their owning leader in O(1).
  typedef common::hash::ObHashMap<share::ObLSID, ObLSAsyncIndexScheduler *> ObLeaderSchedulerMap;
  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> ObTableLastRefreshTsMap;

  // Pre-process all registered schedulers for this round: consume each LS's
  // role-switch flag (skip one round on a switch), drop followers, process the
  // retry tasks of active leaders, and collect the active leaders into
  // leader_map. Does NOT touch refresh deps, so a node that leads nothing can
  // skip the (tenant-wide) deps build entirely.
  int prepare_leader_map_(
      const common::ObIArray<ObLSAsyncIndexScheduler *> &schedulers,
      ObLeaderSchedulerMap &leader_map);
  // Single pass over ALL async-index data tablets in the tenant: route each
  // tablet to its owning leader scheduler (O(1) via data_ls_id_ when valid,
  // otherwise via probe_owning_leader_) and drive refresh for it. Replaces the
  // previous per-LS full-tablet scan, removing the O(leader_ls * tablets) cost.
  int dispatch_refresh_by_ls_(
      const ObAsyncIndexRefreshDeps &refresh_deps,
      ObLeaderSchedulerMap &leader_map,
      const int64_t now_ts);
  // Clean pairs in last_refresh_ts_map_ for tables that no longer carry async indexes, then
  // select the tables whose configured refresh interval has elapsed.
  int filter_due_data_tables_(
      const common::ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals,
      const int64_t now_ts,
      common::ObIArray<uint64_t> &due_data_table_ids);
  // Fallback tablet->leader routing for tablets whose data_ls_id_ is invalid:
  // probe each active leader's ObLS via get_tablet(). A scheduler with a null
  // ObLS (unit-test setup) is treated as owning every tablet. Returns the
  // owning leader scheduler, or nullptr if no active leader owns the tablet.
  ObLSAsyncIndexScheduler *probe_owning_leader_(
      const common::ObTabletID &data_tablet_id,
      ObLeaderSchedulerMap &leader_map);
  // Evaluate one data table tablet and enqueue refresh tasks for its qualifying indexes.
  // A single data table failure is logged and swallowed so the round continues.
  int refresh_one_data_table_(
      const ObAsyncIndexDataTableInfo &data_table_info,
      int64_t data_tablet_pos,
      ObLSAsyncIndexScheduler *ls_scheduler);
  // Process retry tasks whose delay has expired on one LS scheduler.
  int process_retry_tasks_(ObLSAsyncIndexScheduler *scheduler, int64_t &admission_budget);
  // The kind of refresh SQL a task carries. insert-new and delete-old are
  // separate tasks targeting different tablets (index tablet vs delete table).
  // INSERT_NEW / DELETE_OLD are the two incremental (mview delta) ops. COMPLETE
  // is the first/no-baseline full-snapshot refresh of a (non-delete) index.
  enum ObRefreshSqlOp { REFRESH_SQL_INSERT_NEW = 0, REFRESH_SQL_DELETE_OLD = 1, REFRESH_SQL_COMPLETE = 2 };
  // Generate the refresh SQL for one decision and the given op into `sql`.
  // refresh_task_id is the unique task id generated by the caller for the inner SQL hint.
  int build_refresh_sql_(
      const ObAsyncIndexDataTableInfo &data_table_info,
      int64_t data_tablet_pos,
      const ObAsyncIndexInfo &index_info,
      const ObRefreshDecision &decision,
      const ObRefreshSqlOp op,
      const int64_t refresh_task_id,
      common::ObSqlString &sql);
  // Schedule refresh for a single target tablet with its pre-generated SQL.
  // All scheduling inputs are bundled in param.
  int schedule_tablet_refresh_(const ObAsyncIndexRefreshTaskParam &param);

  // Check if a tablet has any pending task (in-flight + retry on its LS).
  bool has_pending_task_(const common::ObTabletID &tablet_id,
                         ObLSAsyncIndexScheduler *ls_scheduler) const;

  // Generate a unique refresh_task_id for the refresh inner SQL hint, using
  // ObCommonIDUtils::gen_unique_id (same approach as mview refresh_id).
  int generate_refresh_task_id_(int64_t &refresh_task_id);

  // Resolve the dep provider to use. Returns the injected test override when set,
  // otherwise the production member. Lets unit tests drive check_and_refresh_
  // with a fake provider without touching the production path.
  ObIAsyncIndexDepProvider &get_dep_provider_()
  {
    return nullptr != dep_provider_override_ ? *dep_provider_override_ : dep_provider_;
  }

private:
  static const int64_t SCHEDULE_PERIOD_US = OB_ASYNC_INDEX_TIMER_INTERVAL_US;
  static const int64_t MAX_LS_SCHEDULER_COUNT = 128;
  static const int64_t LAST_REFRESH_TS_MAP_BUCKET_COUNT = 1024;

  bool is_inited_;
  uint64_t tenant_id_;
  bool is_oracle_checked_;   // whether compat mode has been lazily checked
  bool is_oracle_mode_;      // cached result: true = oracle tenant, skip refresh
  int tg_id_;
  ObAsyncIndexRefreshTimerTask timer_task_;
  ObAsyncIndexRefreshWorker refresh_worker_;
  ObAsyncIndexDepProvider dep_provider_;  // production storage/schema/MDS access
  ObIAsyncIndexDepProvider *dep_provider_override_;  // test-only injection; null in production
  ObAsyncIndexTrigger trigger_;           // trigger-condition evaluation
  int64_t refresh_id_seq_;  // monotonically increasing refresh batch id generator
  // Monotonically increasing timer scheduling round counter, bumped once at the
  // start of each run_refresh_task tick. Touched only by the single TIMER thread
  // (no atomic needed) and printed in the key debug logs so all activity of one
  // tick can be grouped together.
  int64_t round_seq_;
  // Timer-thread-only state: only a completed dispatch advances the timestamp.
  ObTableLastRefreshTsMap last_refresh_ts_map_;
  // Resume from the first task rejected by backpressure instead of restarting
  // from the same table/partition/index prefix on every timer tick.
  uint64_t pending_data_table_id_;
  common::ObTabletID pending_data_tablet_id_;
  common::ObTabletID pending_index_tablet_id_;

  // Registered LS schedulers. Protected by ls_schedulers_lock_ (read-heavy:
  // timer reads every round, writes only on LS create/destroy).
  common::ObSEArray<ObLSAsyncIndexScheduler *, 16> ls_schedulers_;
  mutable common::SpinRWLock ls_schedulers_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantAsyncIndexRefresher);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_TENANT_ASYNC_INDEX_REFRESHER_H_
