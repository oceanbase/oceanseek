/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/async_index/ob_async_index_sql_gen.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/ob_common_id_utils.h"
#include "storage/ls/ob_ls.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_async_index_params.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObAsyncIndexRefreshTimerTask ========================
int ObAsyncIndexRefreshTimerTask::init(ObTenantAsyncIndexRefresher *host)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    host_ = host;
  }
  return ret;
}

void ObAsyncIndexRefreshTimerTask::runTimerTask()
{
  if (OB_NOT_NULL(host_)) {
    host_->run_refresh_task();
  }
}

// ======================== ObTenantAsyncIndexRefresher ========================
ObTenantAsyncIndexRefresher::ObTenantAsyncIndexRefresher()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_oracle_checked_(false),
    is_oracle_mode_(false),
    tg_id_(-1),
    timer_task_(),
    refresh_worker_(),
    dep_provider_(),
    dep_provider_override_(nullptr),
    trigger_(),
    refresh_id_seq_(0),
    round_seq_(0),
    last_refresh_ts_map_(),
    pending_data_table_id_(OB_INVALID_ID),
    pending_data_tablet_id_(),
    pending_index_tablet_id_(),
    ls_schedulers_(),
    ls_schedulers_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK)
{
}

ObTenantAsyncIndexRefresher::~ObTenantAsyncIndexRefresher()
{
  destroy();
}

int ObTenantAsyncIndexRefresher::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantAsyncIndexTimer, tg_id_))) {
    LOG_WARN("failed to create timer thread group", K(ret), K(tenant_id));
  } else if (OB_FAIL(timer_task_.init(this))) {
    LOG_WARN("failed to init timer task", K(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_worker_.init(tenant_id, lib::TGDefIDs::AsyncIndexRefreshWorker,
                                          &dep_provider_, this))) {
    LOG_WARN("failed to init refresh worker", K(ret), K(tenant_id));
  } else if (OB_FAIL(trigger_.init(tenant_id, &dep_provider_))) {
    LOG_WARN("failed to init trigger", K(ret), K(tenant_id));
  } else if (OB_FAIL(last_refresh_ts_map_.create(LAST_REFRESH_TS_MAP_BUCKET_COUNT,
                                                 "AsyncIdxTsMap", "AsyncIdxTsMap", tenant_id))) {
    LOG_WARN("failed to create last refresh timestamp map", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("ObTenantAsyncIndexRefresher inited", K(tenant_id));
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    // NOTE: refresh_worker_ is NOT started here. Its thread group is created
    // lazily on the first add_task() (see ObAsyncIndexRefreshWorker::add_task),
    // so a tenant that never refreshes an async index carries no idle worker
    // thread. Only the timer below is started unconditionally.
    LOG_WARN("failed to start timer thread group", K(ret), K_(tenant_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, timer_task_, SCHEDULE_PERIOD_US, true /*repeat*/))) {
    LOG_WARN("failed to schedule timer task", K(ret), K_(tenant_id));
  } else {
    LOG_INFO("ObTenantAsyncIndexRefresher started", K_(tenant_id));
  }
  return ret;
}

void ObTenantAsyncIndexRefresher::stop()
{
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
  refresh_worker_.stop();
}

void ObTenantAsyncIndexRefresher::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
  }
  refresh_worker_.wait();
}

void ObTenantAsyncIndexRefresher::destroy()
{
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  refresh_worker_.destroy();
  pending_data_table_id_ = OB_INVALID_ID;
  pending_data_tablet_id_.reset();
  pending_index_tablet_id_.reset();
  if (last_refresh_ts_map_.created()) {
    last_refresh_ts_map_.destroy();
  }
  {
    SpinWLockGuard guard(ls_schedulers_lock_);
    ls_schedulers_.reset();
  }
  is_inited_ = false;
}

int ObTenantAsyncIndexRefresher::register_ls_scheduler(ObLSAsyncIndexScheduler *scheduler)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null scheduler", K(ret));
  } else {
    SpinWLockGuard guard(ls_schedulers_lock_);
    if (OB_FAIL(ls_schedulers_.push_back(scheduler))) {
      LOG_WARN("failed to register ls scheduler", K(ret), "ls_id", scheduler->get_ls_id());
    } else {
      LOG_INFO("registered ls async index scheduler", "ls_id", scheduler->get_ls_id(),
               K_(tenant_id), "count", ls_schedulers_.count());
    }
  }
  return ret;
}

void ObTenantAsyncIndexRefresher::unregister_ls_scheduler(ObLSAsyncIndexScheduler *scheduler)
{
  if (OB_ISNULL(scheduler)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "null ls async index scheduler");
  } else {
    SpinWLockGuard guard(ls_schedulers_lock_);
    // Refuse new references under the same lock used by acquire_scheduler().
    // Compare pointer identity without dereferencing registry entries: failed LS
    // creation can temporarily leave multiple scheduler instances with one LS id.
    scheduler->mark_stopped();
    for (int64_t i = 0; i < ls_schedulers_.count(); ++i) {
      if (ls_schedulers_.at(i) == scheduler) {
        ls_schedulers_.remove(i);
        LOG_INFO("unregistered ls async index scheduler",
                 "ls_id", scheduler->get_ls_id(), KP(scheduler), K_(tenant_id),
                 "count", ls_schedulers_.count());
        break;
      }
    }
  }
}

ObLSAsyncIndexScheduler *ObTenantAsyncIndexRefresher::acquire_scheduler(const share::ObLSID &ls_id)
{
  ObLSAsyncIndexScheduler *acquired = nullptr;
  SpinRLockGuard guard(ls_schedulers_lock_);
  for (int64_t i = 0; nullptr == acquired && i < ls_schedulers_.count(); ++i) {
    ObLSAsyncIndexScheduler *scheduler = ls_schedulers_.at(i);
    if (OB_NOT_NULL(scheduler) && scheduler->get_ls_id() == ls_id && !scheduler->is_stopped()) {
      // Still registered and not stopping: take a reference under the lock so it
      // cannot race with unregister_ls_scheduler (which marks stopped + removes
      // under the write lock). The scheduler is guaranteed alive until the
      // matching release_scheduler().
      scheduler->inc_ref();
      acquired = scheduler;
    }
  }
  return acquired;
}

void ObTenantAsyncIndexRefresher::release_scheduler(ObLSAsyncIndexScheduler *scheduler)
{
  if (OB_NOT_NULL(scheduler)) {
    scheduler->dec_ref();
  }
}

int ObTenantAsyncIndexRefresher::build_view_snapshot(
  ObAsyncIndexRefreshViewSnapshot &snapshot) const
{
  int ret = OB_SUCCESS;
  snapshot.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant async index refresher not inited", K(ret), K_(tenant_id));
  } else {
    // Hold ls_schedulers_lock_ (read) for the WHOLE LS-side snapshot. This pins
    // every scheduler pointer (destroy must take the write lock to unregister)
    // and freezes is_leader_ / retry_tasks_ relative to the snapshot.
    SpinRLockGuard guard(ls_schedulers_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_schedulers_.count(); ++i) {
      const ObLSAsyncIndexScheduler *scheduler = ls_schedulers_.at(i);
      if (OB_ISNULL(scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ls scheduler in registry", K(ret), K(i));
      } else {
        const bool is_leader = scheduler->is_leader();
        if (is_leader && OB_FAIL(scheduler->get_retry_task_stats(snapshot.retry_stats_))) {
          // append-semantics: each leader scheduler appends to the shared array
          LOG_WARN("failed to get retry task stats", K(ret),
                  "ls_id", scheduler->get_ls_id());
        }
      }
    }
  }
  // Worker in-flight snapshot only touches task_pool_.lock_ (independent from
  // ls_schedulers_lock_), so collect it outside the read guard. LS-vs-worker
  // drift (±1) is tolerated by the view.
  if (OB_SUCC(ret)
      && OB_FAIL(refresh_worker_.get_in_flight_stats(snapshot.in_flight_stats_))) {
    LOG_WARN("failed to get in-flight stats", K(ret), K_(tenant_id));
  }
  return ret;
}

void ObTenantAsyncIndexRefresher::run_refresh_task()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_ASYNC_INDEX_REFRESH);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!last_refresh_ts_map_.created())) {
    ret = OB_NOT_INIT;
    LOG_WARN("last refresh timestamp map is not initialized", K(ret), K_(tenant_id));
  } else {
    bool enable_refresh = share::ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_);
    if (!enable_refresh) {
      LOG_DEBUG("[ASYNC_INDEX] refresh timer retry task dropped because _enable_async_index_refresh is off", K(tenant_id_));
      // Refresh disabled: drain retry lists, do not bump round_seq_, skip dispatch.
      SpinRLockGuard guard(ls_schedulers_lock_);
      for (int64_t i = 0; i < ls_schedulers_.count(); ++i) {
        ObLSAsyncIndexScheduler *scheduler = ls_schedulers_.at(i);
        if (OB_NOT_NULL(scheduler)) {
          scheduler->cancel_retry_tasks();
        }
      }
    } else if (FALSE_IT(++round_seq_)) {
      // Bump the timer scheduling round counter once per tick. Only the single
      // TIMER thread reaches here, so a plain increment is safe (no atomic).
    } else if (!is_oracle_checked_) {
      // Lazy check: during start() the tenant schema may not be ready yet, so we
      // defer the compat mode query to the first timer callback when the tenant is
      // fully up.
      bool is_oracle = false;
      if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
              tenant_id_, is_oracle))) {
        // Schema still not ready (e.g. tenant bootstrap in progress). Retry next
        // round; do not cache the result.
        LOG_WARN("failed to check compat mode, will retry next round", K(ret), K_(tenant_id));
      } else {
        is_oracle_mode_ = is_oracle;
        is_oracle_checked_ = true;
        if (is_oracle) {
          LOG_INFO("async index refresher detected oracle mode tenant, skip refresh",
                   K_(tenant_id));
        }
      }
    }
    if (OB_FAIL(ret) || is_oracle_mode_ || !enable_refresh) {
      // Either compat mode check failed (will retry) or oracle tenant or refresh disabled (skip).
    } else {
      // Take a snapshot of the registered LS schedulers under read lock, taking a
      // liveness reference on each so they cannot be freed out from under this
      // round after the lock is dropped (a single-LS offline can call
      // unregister + ObLS free concurrently). Every acquired reference is released
      // at the end of the round below.
      ObSEArray<ObLSAsyncIndexScheduler *, 16> schedulers;
      schedulers.set_attr(ObMemAttr(tenant_id_, "AsyncIdxDeps"));
      {
        SpinRLockGuard guard(ls_schedulers_lock_);
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_schedulers_.count(); ++i) {
          ObLSAsyncIndexScheduler *scheduler = ls_schedulers_.at(i);
          if (OB_ISNULL(scheduler) || scheduler->is_stopped()) {
            // null slot or a scheduler already being torn down: skip
          } else {
            scheduler->inc_ref();
            if (OB_FAIL(schedulers.push_back(scheduler))) {
              // roll back this one ref; refs already in `schedulers` are released below
              scheduler->dec_ref();
              LOG_WARN("failed to snapshot ls scheduler", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // Phase 1: pre-process schedulers (consume switch flags, process retries)
        // and collect the LSs this node actively leads this round. This does NOT
        // build the tenant-wide refresh deps, so a node that leads nothing (or
        // whose only LSs are skipping a round after a role switch) avoids the
        // deps build entirely.
        ObLeaderSchedulerMap leader_map;
        if (OB_FAIL(leader_map.create(MAX_LS_SCHEDULER_COUNT, "AsyncIdxLdr", "AsyncIdxLdr",
                                      tenant_id_))) {
          LOG_WARN("failed to create leader scheduler map", K(ret), K_(tenant_id));
        } else if (OB_FAIL(prepare_leader_map_(schedulers, leader_map))) {
          LOG_WARN("failed to prepare leader scheduler map", K(ret), K_(tenant_id));
        } else if (leader_map.empty()) {
          // this node leads no LS this round: nothing to do (low base noise)
          // no ls leader on this server, clear the map
          if (!last_refresh_ts_map_.empty() && OB_FAIL(last_refresh_ts_map_.clear())) {
            LOG_WARN("failed to clear last refresh timestamp map", K(ret), K_(tenant_id));
          }
        } else {
          // Phase 2: collect only table ids and refresh intervals, clean stale
          // timestamps, and filter out disabled/not-yet-due tables before doing
          // any expensive schema deep copy.
          const int64_t now_ts = common::ObTimeUtility::current_monotonic_time();
          ObSEArray<ObAsyncTableRefreshInterval, 8> table_refresh_intervals;
          ObSEArray<uint64_t, 8> due_data_table_ids;
          table_refresh_intervals.set_attr(ObMemAttr(tenant_id_, "AsyncIdxDeps"));
          due_data_table_ids.set_attr(ObMemAttr(tenant_id_, "AsyncIdxDeps"));
          ObAsyncIndexRefreshDeps refresh_deps(tenant_id_);
          if (OB_FAIL(get_dep_provider_().get_async_table_refresh_intervals(tenant_id_, table_refresh_intervals))) {
            LOG_WARN("failed to get async table refresh intervals", K(ret), K_(tenant_id));
          } else if (OB_FAIL(filter_due_data_tables_(table_refresh_intervals, now_ts, due_data_table_ids))) {
            LOG_WARN("failed to filter due async index data tables", K(ret), K_(tenant_id));
          } else if (due_data_table_ids.empty()) {
            // No table is due this round. Avoid schema deep copy and snapshot SCN.
          } else if (OB_FAIL(get_dep_provider_().get_async_index_refresh_deps(tenant_id_, due_data_table_ids, refresh_deps))) {
            LOG_WARN("failed to build async index refresh deps", K(ret), K_(tenant_id));
          } else if (refresh_deps.empty()) {
            // Selected schemas disappeared or became invalid in this schema round.
          } else if (OB_FAIL(get_dep_provider_().refresh_snapshot_scn())) {
            LOG_WARN("failed to refresh snapshot scn", K(ret), K_(tenant_id));
          } else if (OB_FAIL(dispatch_refresh_by_ls_(refresh_deps, leader_map, now_ts))) {
            LOG_WARN("failed to dispatch refresh by ls", K(ret), K_(tenant_id));
          }
        }
      }
      // Release the round's liveness references on every path (including a failed
      // snapshot above, where only a prefix of `schedulers` was populated). A
      // scheduler pending destroy() unblocks once its last reference is dropped.
      for (int64_t i = 0; i < schedulers.count(); ++i) {
        if (OB_NOT_NULL(schedulers.at(i))) {
          schedulers.at(i)->dec_ref();
        }
      }
    }
  }
}

int ObTenantAsyncIndexRefresher::filter_due_data_tables_(
    const common::ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals,
    const int64_t now_ts,
    common::ObIArray<uint64_t> &due_data_table_ids)
{
  int ret = OB_SUCCESS;
  due_data_table_ids.reset();
  if (OB_UNLIKELY(now_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current timestamp", K(ret), K(now_ts));
  } else if (OB_UNLIKELY(!last_refresh_ts_map_.created())) {
    ret = OB_NOT_INIT;
    LOG_WARN("last refresh timestamp map is not initialized", K(ret), K_(tenant_id));
  } else if (table_refresh_intervals.empty()) {
    // collect no async index tables in this tenant, clear the refresh map
    if (!last_refresh_ts_map_.empty() && OB_FAIL(last_refresh_ts_map_.clear())) {
      LOG_WARN("failed to clear last refresh timestamp map", K(ret), K_(tenant_id));
    }
  } else {
    common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> current_table_ids;
    ObSEArray<uint64_t, 8> stale_table_ids;
    stale_table_ids.set_attr(ObMemAttr(tenant_id_, "AsyncIdxDeps"));
    if (OB_FAIL(current_table_ids.create(table_refresh_intervals.count(), "AsyncIdxTblSet", "AsyncIdxTblSet", tenant_id_))) {
      LOG_WARN("failed to create current async table id set", K(ret), K_(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_refresh_intervals.count(); ++i) {
      const ObAsyncTableRefreshInterval &interval = table_refresh_intervals.at(i);
      if (OB_UNLIKELY(!interval.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid async table refresh interval", K(ret), K(interval));
      } else if (OB_FAIL(current_table_ids.set_refactored(interval.data_table_id_))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add current async table id", K(ret), K(interval));
        }
      }
    }
    for (ObTableLastRefreshTsMap::const_iterator iter = last_refresh_ts_map_.begin();
         OB_SUCC(ret) && iter != last_refresh_ts_map_.end(); ++iter) {
      const int hash_ret = current_table_ids.exist_refactored(iter->first);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(stale_table_ids.push_back(iter->first))) {
          LOG_WARN("failed to collect stale async table id", K(ret), K(iter->first));
        }
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to find current async table id", K(ret), K(iter->first));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stale_table_ids.count(); ++i) {
      if (OB_FAIL(last_refresh_ts_map_.erase_refactored(stale_table_ids.at(i)))) {
        LOG_WARN("failed to erase stale refresh timestamp", K(ret), "data_table_id", stale_table_ids.at(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_refresh_intervals.count(); ++i) {
      const ObAsyncTableRefreshInterval &interval = table_refresh_intervals.at(i);
      bool due = false;
      if (ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL == interval.refresh_interval_us_) {
        // Timer refresh is disabled for this table.
      } else {
        int64_t last_refresh_ts = 0;
        const int hash_ret = last_refresh_ts_map_.get_refactored(interval.data_table_id_, last_refresh_ts);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          due = true;
        } else if (OB_SUCCESS == hash_ret) {
          if (now_ts - last_refresh_ts >= interval.refresh_interval_us_) {
            due = true;
          }
        } else {
          ret = hash_ret;
          LOG_WARN("failed to get last refresh timestamp", K(ret), K(interval));
        }
      }
      if (due) {
        if (OB_FAIL(due_data_table_ids.push_back(interval.data_table_id_))) {
          LOG_WARN("failed to push due async table id", K(ret), K(interval));
        }
      }
    }
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::prepare_leader_map_(
    const common::ObIArray<ObLSAsyncIndexScheduler *> &schedulers,
    ObLeaderSchedulerMap &leader_map)
{
  int ret = OB_SUCCESS;
  int64_t retry_budget = calc_async_index_retry_admission_budget(
      OB_ASYNC_INDEX_TASK_QUEUE_SIZE - refresh_worker_.get_in_flight_count(), round_seq_);
  for (int64_t i = 0; OB_SUCC(ret) && i < schedulers.count(); ++i) {
    ObLSAsyncIndexScheduler *scheduler = schedulers.at((i + round_seq_) % schedulers.count());
    if (OB_ISNULL(scheduler)) {
      // skip null entry
    } else if (scheduler->consume_switch_flag()) {
      // A leader/follower switch happened on this LS since the last round.
      // The LS scheduler already bumped its epoch and cancelled retry tasks
      // in switch_to_follower / switch_to_leader. Skip one round for this LS
      // to let in-flight tasks drain and get fenced by the epoch check.
      LOG_INFO("async index refresher skip one round for ls after role switch",
               "ls_id", scheduler->get_ls_id(), K_(tenant_id),
               "is_leader", scheduler->is_leader());
    } else if (!scheduler->is_leader()) {
      // This LS is a follower: nothing to do.
    } else {
      // Steady state: this LS is leader and no switch happened. Process its
      // retry tasks and register it as an active leader for this round.
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(process_retry_tasks_(scheduler, retry_budget))) {
        LOG_WARN("failed to process retry tasks for ls", K(tmp_ret),
                 "ls_id", scheduler->get_ls_id());
      }
      if (OB_FAIL(leader_map.set_refactored(scheduler->get_ls_id(), scheduler))) {
        LOG_WARN("failed to register leader scheduler", K(ret),
                 "ls_id", scheduler->get_ls_id());
      }
    }
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::dispatch_refresh_by_ls_(
    const ObAsyncIndexRefreshDeps &refresh_deps,
    ObLeaderSchedulerMap &leader_map,
    const int64_t now_ts)
{
  int ret = OB_SUCCESS;
  int64_t total_tablets = 0;
  int64_t routed_tablets = 0;
  int64_t first_table = 0;
  for (int64_t i = 0; i < refresh_deps.data_table_infos_.count(); ++i) {
    if (refresh_deps.data_table_infos_.at(i).data_table_id_ == pending_data_table_id_) {
      first_table = i;
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < refresh_deps.data_table_infos_.count(); ++i) {
    const ObAsyncIndexDataTableInfo &data_table_info = refresh_deps.data_table_infos_.at(
        (first_table + i) % refresh_deps.data_table_infos_.count());
    int64_t first_tablet = 0;
    if (data_table_info.data_table_id_ == pending_data_table_id_) {
      for (int64_t t = 0; t < data_table_info.data_tablet_infos_.count(); ++t) {
        if (data_table_info.data_tablet_infos_.at(t).data_tablet_id_ == pending_data_tablet_id_) {
          first_tablet = t;
          break;
        }
      }
    }
    // The prefix was dispatched on preceding ticks. Do not wrap within a table:
    // under sustained DML, revisiting it could starve every later data table.
    for (int64_t t = first_tablet; OB_SUCC(ret) && t < data_table_info.data_tablet_infos_.count(); ++t) {
      const ObAsyncIndexDataTabletInfo &data_tablet = data_table_info.data_tablet_infos_.at(t);
      ++total_tablets;
      ObLSAsyncIndexScheduler *scheduler = nullptr;
      if (data_tablet.data_ls_id_.is_valid()) {
        // O(1) routing: the dep provider already resolved the owning LS.
        const int hash_ret = leader_map.get_refactored(data_tablet.data_ls_id_, scheduler);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          scheduler = nullptr;  // this LS is not led by this node, skip
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to lookup leader scheduler map", K(ret),
                   "data_ls_id", data_tablet.data_ls_id_);
        }
      } else {
        // Fallback: owning LS unknown, probe the active leaders.
        scheduler = probe_owning_leader_(data_tablet.data_tablet_id_, leader_map);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(scheduler)) {
        ObAsyncIndexService *svc = MTL(ObAsyncIndexService*);
        if (OB_NOT_NULL(svc) && svc->is_tablet_rpc_refreshing(data_tablet.data_tablet_id_)) {
          LOG_INFO("[ASYNC_INDEX_REFRESH] skip data tablet handled by RPC refresh",
                    K_(tenant_id), "data_tablet_id", data_tablet.data_tablet_id_);
        } else {
          ++routed_tablets;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(refresh_one_data_table_(data_table_info, t, scheduler))) {
            if (OB_SIZE_OVERFLOW == tmp_ret) {
              ret = tmp_ret;
            } else {
              LOG_WARN("failed to refresh one data table, continue", K(tmp_ret),
                       K_(tenant_id), "data_table_id", data_table_info.data_table_id_,
                       "data_tablet_id", data_tablet.data_tablet_id_);
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(last_refresh_ts_map_.set_refactored(data_table_info.data_table_id_, now_ts, 1))) {
      LOG_WARN("failed to update completed refresh dispatch timestamp", K(ret), K(now_ts));
    }
  }
  if (OB_SIZE_OVERFLOW == ret) {
    // Keep this table due, and retry the rejected tablet before scanning the prefix.
    ret = OB_SUCCESS;
  } else if (OB_SUCC(ret)) {
    pending_data_table_id_ = OB_INVALID_ID;
    pending_data_tablet_id_.reset();
    pending_index_tablet_id_.reset();
  }
  LOG_TRACE("[ASYNC_INDEX_REFRESH] dispatch_refresh_by_ls done", K_(tenant_id), K_(round_seq),
            K(total_tablets), K(routed_tablets), "leader_ls_cnt", leader_map.size());
  return ret;
}

ObLSAsyncIndexScheduler *ObTenantAsyncIndexRefresher::probe_owning_leader_(
    const common::ObTabletID &data_tablet_id,
    ObLeaderSchedulerMap &leader_map)
{
  ObLSAsyncIndexScheduler *owner = nullptr;
  for (ObLeaderSchedulerMap::iterator it = leader_map.begin();
       nullptr == owner && it != leader_map.end(); ++it) {
    ObLSAsyncIndexScheduler *scheduler = it->second;
    if (OB_ISNULL(scheduler)) {
      // skip
    } else if (OB_ISNULL(scheduler->get_ls())) {
      // Null ObLS (unit-test setup): treat this leader as owning every tablet.
      owner = scheduler;
    } else {
      ObTabletHandle tablet_handle;
      const int tmp_ret = get_async_index_refresh_tablet(
          *scheduler->get_ls(), data_tablet_id, tablet_handle,
          -1 /* transfer_seq */, true /* is_probe */);
      if (OB_SUCCESS == tmp_ret) {
        owner = scheduler;  // tablet resides on this leader LS
      } else if (OB_TABLET_NOT_EXIST == tmp_ret || OB_EAGAIN == tmp_ret) {
        // tablet not on this LS, keep probing
      } else {
        LOG_WARN_RET(tmp_ret, "failed to probe tablet existence on ls, skip",
                     "ls_id", scheduler->get_ls_id(), K(data_tablet_id));
      }
    }
  }
  return owner;
}

int ObTenantAsyncIndexRefresher::refresh_one_data_table_(
    const ObAsyncIndexDataTableInfo &data_table_info,
    int64_t data_tablet_pos,
    ObLSAsyncIndexScheduler *ls_scheduler)
{
  int ret = OB_SUCCESS;
  ObRefreshDecisionArray decisions;
  const uint64_t data_table_id = data_table_info.data_table_id_;
  const common::ObTabletID &data_tablet_id =
      data_table_info.data_tablet_infos_.at(data_tablet_pos).data_tablet_id_;
  const share::ObLSID ls_id = ls_scheduler->get_ls_id();
  ObLS *ls = ls_scheduler->get_ls();
  ObTabletHandle data_tablet_handle;
  // Validate even the cached LS mapping; it may predate a transfer.
  if (OB_NOT_NULL(ls)
      && OB_FAIL(get_async_index_refresh_tablet(
          *ls, data_tablet_id, data_tablet_handle))) {
    LOG_INFO("skip async index scheduling on stale data tablet", K(ret), K(ls_id), K(data_tablet_id));
  } else if (OB_FAIL(trigger_.check_and_generate_tasks(
          data_table_info, data_tablet_pos, ls, decisions, round_seq_))) {
    LOG_WARN("failed to check and generate tasks", K(ret), K(data_table_id), K(data_tablet_id));
  } else {
    // Each decision drives exactly ONE task targeting its OWN index tablet,
    // using its own SCN window / parallelism. Two refresh kinds:
    //   - COMPLETE (first refresh / no baseline): a non-delete index runs a full
    //     snapshot scan; the delete index produces no task (its delete buffer
    //     baseline is empty -- there is no history to capture).
    //   - INCREMENTAL: a non-delete index runs insert-new; the delete index --
    //     an independent index table with its own publish scn / sstable check --
    //     runs delete-old.
    // The SQL is generated once here, plus the data table schema version captured
    // now (re-validated before execution). A single tablet failure is logged and
    // skipped; it never aborts the data table.
    int64_t first_decision = 0;
    if (data_table_id == pending_data_table_id_ && data_tablet_id == pending_data_tablet_id_) {
      for (int64_t i = 0; i < decisions.count(); ++i) {
        if (decisions.at(i).index_tablet_id_ == pending_index_tablet_id_) {
          first_decision = i;
          break;
        }
      }
    }
    for (int64_t i = first_decision; OB_SUCC(ret) && i < decisions.count(); ++i) {
      ObRefreshDecision decision = decisions.at(i);
      if (has_pending_task_(decision.index_tablet_id_, ls_scheduler)) {
        continue;
      } else if (refresh_worker_.get_in_flight_count() >= OB_ASYNC_INDEX_TASK_QUEUE_SIZE) {
        pending_data_table_id_ = data_table_id;
        pending_data_tablet_id_ = data_tablet_id;
        pending_index_tablet_id_ = decision.index_tablet_id_;
        ret = OB_SIZE_OVERFLOW;
        break;
      }
      refresh_worker_.get_adaptive_parallelism(
          decision.index_tablet_id_, decision.checkpoint_backlog_,
          data_table_info.refresh_interval_us_, decision.is_complete_refresh_,
          decision.parallelism_);

      // Find the index info whose tablet at data_tablet_pos matches the decision
      int64_t idx_pos = -1;
      for (int64_t j = 0; j < data_table_info.index_infos_.count(); ++j) {
        const ObAsyncIndexInfo &idx = data_table_info.index_infos_.at(j);
        if (data_tablet_pos < idx.tablet_ids_.count()
            && idx.tablet_ids_.at(data_tablet_pos) == decision.index_tablet_id_) {
          idx_pos = j;
          break;
        }
      }
      if (idx_pos < 0) {
        LOG_WARN("cannot find index info for decision, skip", K(decision),
                 K(data_table_id), K(data_tablet_id));
        continue;
      }
      const ObAsyncIndexInfo &index_info = data_table_info.index_infos_.at(idx_pos);
      ObTabletHandle index_tablet_handle;
      if (OB_NOT_NULL(ls)
          && OB_FAIL(get_async_index_refresh_tablet(
              *ls, decision.index_tablet_id_, index_tablet_handle))) {
        LOG_INFO("skip async index scheduling on stale index tablet", K(ret), K(ls_id), K(decision));
        break;
      }

      // Fields shared with the single task this decision produces. Per-call
      // fields (index_tablet_id_, refresh_id_, sql_) are filled in below.
      ObAsyncIndexRefreshTaskParam base_param;
      base_param.tenant_id_ = tenant_id_;
      base_param.data_table_id_ = data_table_id;
      base_param.ls_id_ = ls_id;
      base_param.data_tablet_id_ = data_tablet_id;
      if (OB_NOT_NULL(ls)) {
        base_param.data_transfer_seq_ = data_tablet_handle.get_obj()->get_transfer_seq();
        base_param.index_transfer_seq_ = index_tablet_handle.get_obj()->get_transfer_seq();
      }
      base_param.start_refresh_scn_ = decision.start_refresh_scn_;
      base_param.target_refresh_scn_ = decision.target_refresh_scn_;
      base_param.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
      base_param.parallelism_ = decision.parallelism_;
      base_param.schema_version_ = data_table_info.schema_version_;
      base_param.leader_proposal_id_ = decision.leader_proposal_id_;
      // Delete-old carries every partition routing column, so keep the normal
      // PARTITION_RANGE/RANGE/SORT DDL path enabled.
      base_param.is_partition_local_ = false;
      base_param.is_complete_refresh_ = decision.is_complete_refresh_;
      base_param.is_refresh_on_error_skip_ = index_info.is_refresh_on_error_skip_;
      base_param.is_search_index_ =
          share::schema::is_async_search_data_index(index_info.index_type_);
      base_param.ls_scheduler_ = ls_scheduler;

      // Select the op: COMPLETE for a no-baseline non-delete index; otherwise the
      // incremental op (insert-new for a physical inverted index, delete-old for the delete index).
      // Either way the task targets this decision's own index tablet.
      ObRefreshSqlOp op = REFRESH_SQL_INSERT_NEW;
      if (decision.is_complete_refresh_) {
        op = REFRESH_SQL_COMPLETE;
      } else if (index_info.is_delete_index_) {
        op = REFRESH_SQL_DELETE_OLD;
      }
      int64_t refresh_task_id = 0;
      ObSqlString refresh_sql;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(generate_refresh_task_id_(refresh_task_id))) {
        LOG_WARN("failed to generate refresh_task_id, skip", K(tmp_ret),
                 K_(tenant_id), K(data_tablet_id), K(decision));
      } else if (OB_TMP_FAIL(build_refresh_sql_(data_table_info, data_tablet_pos, index_info,
                                         decision, op, refresh_task_id, refresh_sql))) {
        LOG_WARN("failed to build refresh sql, skip", K(tmp_ret), K(op),
                 K(data_tablet_id), K(decision));
      } else {
        ObAsyncIndexRefreshTaskParam param = base_param;
        param.index_tablet_id_ = decision.index_tablet_id_;
        param.refresh_id_ = ATOMIC_AAF(&refresh_id_seq_, 1);
        param.refresh_task_id_ = refresh_task_id;
        param.sql_ = &refresh_sql;
        if (OB_TMP_FAIL(schedule_tablet_refresh_(param))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            pending_data_table_id_ = data_table_id;
            pending_data_tablet_id_ = data_tablet_id;
            pending_index_tablet_id_ = decision.index_tablet_id_;
            ret = tmp_ret;
          } else {
            LOG_WARN("failed to schedule refresh, continue", K(tmp_ret), K(op),
                     K(data_tablet_id), K(decision));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::build_refresh_sql_(
    const ObAsyncIndexDataTableInfo &data_table_info,
    int64_t data_tablet_pos,
    const ObAsyncIndexInfo &index_info,
    const ObRefreshDecision &decision,
    const ObRefreshSqlOp op,
    const int64_t refresh_task_id,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  const ObAsyncIndexDataTabletInfo &data_tablet =
      data_table_info.data_tablet_infos_.at(data_tablet_pos);
  ObAsyncIndexSqlParam param;
  param.snapshot_scn_ = decision.target_refresh_scn_;
  param.last_refresh_scn_ = decision.start_refresh_scn_;
  param.parallelism_ = decision.parallelism_;
  param.schema_version_ = data_table_info.schema_version_;
  param.database_name_ = data_table_info.database_name_;
  param.data_table_name_ = data_table_info.data_table_name_;
  param.data_partition_name_ = data_tablet.partition_name_;
  param.index_table_name_ = index_info.index_table_name_;
  // For delete-old path, the target table is the delete index itself, so
  // index_info here is the delete index's own info (its decision drives this).
  if (REFRESH_SQL_DELETE_OLD == op) {
    param.delete_table_name_ = index_info.index_table_name_;
  }
  param.select_columns_ = &index_info.select_columns_;
  param.source_columns_ = &index_info.source_columns_;
  param.partition_key_columns_ = &index_info.partition_key_columns_;
  param.rowkey_columns_ = &index_info.rowkey_columns_;
  param.order_columns_ = &index_info.order_columns_;
  param.index_table_id_ = index_info.index_table_id_;
  param.is_search_data_ = share::schema::is_async_search_data_index(index_info.index_type_);
  param.enable_newsort_ = index_info.enable_newsort_;
  param.refresh_task_id_ = refresh_task_id;
  param.execution_id_ = 1;  // fixed value for now
  // Selects the identifier quote char used when escaping schema names. Async
  // index refresh only runs for mysql tenants (oracle tenants are skipped in
  // run_refresh_task), so this is false in practice, but pass the cached compat
  // mode explicitly for correctness / defense-in-depth.
  param.is_oracle_mode_ = is_oracle_mode_;

  // Index partition name at the same position as the data tablet
  if (data_tablet_pos < index_info.partition_names_.count()) {
    param.index_partition_name_ = index_info.partition_names_.at(data_tablet_pos);
  }

  if (REFRESH_SQL_COMPLETE == op) {
    if (param.is_valid_for_complete()) {
      ret = ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(param, sql);
    } else {
      LOG_DEBUG("complete-refresh sql param incomplete, defer generation", K(decision), K(param));
    }
  } else if (REFRESH_SQL_INSERT_NEW == op) {
    if (param.is_valid_for_insert()) {
      ret = ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql);
    } else {
      LOG_DEBUG("insert-new sql param incomplete, defer generation", K(decision), K(param));
    }
  } else {  // REFRESH_SQL_DELETE_OLD
    if (param.is_valid_for_delete()) {
      ret = ObAsyncIndexSqlGenerator::generate_delete_old_rows_sql(param, sql);
    } else {
      LOG_DEBUG("delete-old sql param incomplete, defer generation", K(decision), K(param));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to generate refresh sql", K(ret), K(op), K(decision));
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::process_retry_tasks_(
    ObLSAsyncIndexScheduler *scheduler, int64_t &admission_budget)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, scheduler is null", K(ret));
  } else {
    const int64_t now = common::ObTimeUtility::current_monotonic_time();
    common::ObSpinLockGuard guard(scheduler->get_retry_lock());
    common::ObDList<ObRetryTaskNode> &retry_list = scheduler->get_retry_tasks();
    ObRetryTaskNode *node = retry_list.get_first();
    while (OB_SUCC(ret) && node != retry_list.get_header()) {
      ObRetryTaskNode *next = node->get_next();
      if (scheduler->is_stopped() || !scheduler->is_leader()
          || node->stat_.leader_epoch_ != scheduler->get_leader_epoch()
          || (OB_SUCCESS != node->stat_.result_
              && !is_async_index_retryable_error(node->stat_.result_))) {
        // Drop obsolete work even when its delay has not elapsed or the pool is
        // full. OB_SUCCESS is reserved for tasks deferred before SQL admission.
        refresh_worker_.record_finished_stat(node->stat_);
        retry_list.remove(node);
        node->~ObRetryTaskNode();
        ob_free(node);
      } else if (now >= node->retry_after_ts_ && admission_budget > 0) {
        ObAsyncIndexRefreshTaskParam param;
        node->build_task_param(scheduler, param);
        if (OB_FAIL(refresh_worker_.add_task(param))) {
          if (OB_SIZE_OVERFLOW == ret) {
            // queue full, try again next cycle
            ret = OB_SUCCESS;
            admission_budget = 0;
          } else {
            LOG_WARN("failed to resubmit retry task", K(ret), K(node->stat_));
          }
        } else {
          --admission_budget;
          retry_list.remove(node);
          node->~ObRetryTaskNode();
          ob_free(node);
        }
      }
      node = next;
    }
  }
  return ret;
}

int ObTenantAsyncIndexRefresher::schedule_tablet_refresh_(const ObAsyncIndexRefreshTaskParam &param)
{
  int ret = OB_SUCCESS;
  // dedup at TABLET granularity, covering both in-flight and retry-parked tasks:
  // a tablet with ANY unfinished refresh batch must not get a second one, since
  // two batches over the same data tablet would carry overlapping SCN ranges and
  // double-write.
  if (has_pending_task_(param.index_tablet_id_, param.ls_scheduler_)) {
    LOG_DEBUG("tablet has a pending refresh batch, skip",
              K(param.index_tablet_id_), K(param.target_refresh_scn_));
  } else if (refresh_worker_.get_in_flight_count() >= OB_ASYNC_INDEX_TASK_QUEUE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(refresh_worker_.add_task(param))) {
    if (OB_SIZE_OVERFLOW == ret) {
      LOG_WARN("[ASYNC_INDEX_REFRESH] task queue full, will retry next cycle", K(ret),
               K(param.index_tablet_id_));
      // Propagate backpressure so the caller retains its cursor and due time.
    } else {
      LOG_WARN("[ASYNC_INDEX_REFRESH] failed to add refresh task", K(ret),
               K(param.index_tablet_id_));
    }
  } else {
    // A refresh task has been enqueued for execution; the SQL itself is logged
    // when the worker picks it up (see ObAsyncIndexRefreshTask::handle).
    LOG_TRACE("[ASYNC_INDEX_REFRESH] enqueue refresh task", K_(tenant_id), K_(round_seq), K(param));
  }
  return ret;
}

bool ObTenantAsyncIndexRefresher::has_pending_task_(
    const common::ObTabletID &tablet_id,
    ObLSAsyncIndexScheduler *ls_scheduler) const
{
  bool found = false;
  // Check in-flight tasks in the shared worker pool.
  if (refresh_worker_.has_in_flight_task(tablet_id)) {
    found = true;
  }
  // Check the LS scheduler's retry list.
  if (!found && OB_NOT_NULL(ls_scheduler)) {
    common::ObSpinLockGuard guard(ls_scheduler->get_retry_lock());
    const ObRetryTaskNode *node = ls_scheduler->get_retry_tasks().get_first();
    while (!found && node != ls_scheduler->get_retry_tasks().get_header()) {
      if (node->stat_.tablet_id_ == tablet_id) {
        found = true;
      }
      node = node->get_next();
    }
  }
  return found;
}

int ObTenantAsyncIndexRefresher::generate_refresh_task_id_(int64_t &refresh_task_id)
{
  int ret = OB_SUCCESS;
  refresh_task_id = 0;
  share::ObCommonID unique_id;
  MTL_SWITCH(tenant_id_)
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, unique_id))) {
      LOG_WARN("failed to gen unique id for refresh_task_id", KR(ret), K_(tenant_id));
    }
  }
  else
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id_, unique_id))) {
      LOG_WARN("failed to gen unique id by rpc for refresh_task_id", KR(ret), K_(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    refresh_task_id = unique_id.id();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
