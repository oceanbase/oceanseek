/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_refresh_worker.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_ddl_common.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
namespace storage
{

ObAsyncIndexRefreshWorker::ObAsyncIndexRefreshWorker()
  : is_inited_(false),
    is_stopped_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    tg_def_id_(-1),
    tg_id_(-1),
    dep_provider_(nullptr),
    refresher_(nullptr),
    task_pool_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK),
    finished_head_(0),
    finished_count_(0),
    finished_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK),
    adaptive_dop_states_(nullptr),
    adaptive_dop_state_count_(0),
    adaptive_dop_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK),
    running_refresh_dop_(0),
    refresh_dop_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK),
    give_up_task_cnt_(0),
    dropped_retry_cnt_(0)
{
}

ObAsyncIndexRefreshWorker::~ObAsyncIndexRefreshWorker()
{
  destroy();
}

int ObAsyncIndexRefreshWorker::init(const int64_t tenant_id, const int tg_def_id,
                                    ObIAsyncIndexDepProvider *dep_provider,
                                    ObTenantAsyncIndexRefresher *refresher)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("worker already inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    tg_def_id_ = tg_def_id;
    dep_provider_ = dep_provider;
    refresher_ = refresher;
    struct InitTaskFn {
      ObAsyncIndexRefreshWorker *host_;
      explicit InitTaskFn(ObAsyncIndexRefreshWorker *host) : host_(host) {}
      void operator()(ObAsyncIndexRefreshTask &task) { task.host_ = host_; }
    };
    task_pool_.init(InitTaskFn(this));
    finished_head_ = 0;
    finished_count_ = 0;
    adaptive_dop_state_count_ = 0;
    running_refresh_dop_ = 0;
    give_up_task_cnt_ = 0;
    dropped_retry_cnt_ = 0;
    is_stopped_ = false;
    is_inited_ = true;
    LOG_INFO("async index refresh worker inited", K(tenant_id), K(tg_def_id));
  }
  return ret;
}

int ObAsyncIndexRefreshWorker::start()
{
  int ret = OB_SUCCESS;
  const float THREAD_FACTOR = 0.5;
  const int64_t MIN_THREAD_COUNT = 1;
  int64_t max_thread_cnt = MTL_CPU_COUNT() * THREAD_FACTOR;
  max_thread_cnt = OB_MAX(max_thread_cnt, MIN_THREAD_COUNT);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("worker not inited", K(ret));
  } else if (-1 != tg_id_) {
    // Thread group already created (lazily started by a prior add_task()).
    // start() is idempotent so the lazy-start path can call it unconditionally.
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_def_id_, tg_id_))) {
    LOG_WARN("failed to create worker thread group", K(ret), K_(tg_def_id));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT, max_thread_cnt))) {
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("failed to set handler and start worker", K(ret), K_(tg_id));
  } else {
    LOG_INFO("async index refresh worker started", K_(tenant_id), K_(tg_id));
  }
  return ret;
}

void ObAsyncIndexRefreshWorker::stop()
{
  is_stopped_ = true;
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObAsyncIndexRefreshWorker::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

void ObAsyncIndexRefreshWorker::destroy()
{
  if (IS_INIT) {
    stop();
    if (-1 != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
    is_inited_ = false;
  }
  free_adaptive_dop_states_(adaptive_dop_states_);
  adaptive_dop_states_ = nullptr;
  adaptive_dop_state_count_ = 0;
}

int ObAsyncIndexRefreshWorker::add_task(const ObAsyncIndexRefreshTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("worker not inited", K(ret));
  } else if (is_stopped_) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("worker is stopped", K(ret));
  } else if (-1 == tg_id_ && OB_FAIL(start())) {
    // Lazy start: the worker thread group is created on the first task instead
    // of at tenant boot, so tenants that never run an async index refresh do not
    // carry an idle worker thread. add_task() is only ever called from the single
    // refresher TIMER thread, so this lazy start needs no extra synchronization.
    LOG_WARN("failed to lazily start refresh worker", K(ret), K_(tenant_id));
  } else {
    ObAsyncIndexRefreshTask *task = task_pool_.acquire();
    if (OB_ISNULL(task)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("task pool exhausted", K(ret), K(param));
    } else if (OB_FAIL(task->set_task(param))) {
      LOG_WARN("failed to set task", K(ret), K(param));
      task_pool_.release(task);
    } else {
      if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
        LOG_WARN("failed to push task to thread group", K(ret), K(param));
        task_pool_.release(task);
      }
    }
  }
  return ret;
}

bool ObAsyncIndexRefreshWorker::has_task(
    const common::ObTabletID &tablet_id,
    const share::SCN &target_scn) const
{
  struct MatchTaskFn {
    const common::ObTabletID &tablet_id_;
    const share::SCN &target_scn_;
    MatchTaskFn(const common::ObTabletID &tid, const share::SCN &scn)
      : tablet_id_(tid), target_scn_(scn) {}
    bool operator()(const ObAsyncIndexRefreshTask &task) const
    { return task.match(tablet_id_, target_scn_); }
  };
  return task_pool_.find_in_use(MatchTaskFn(tablet_id, target_scn));
}

bool ObAsyncIndexRefreshWorker::has_in_flight_task(const common::ObTabletID &tablet_id) const
{
  struct MatchTabletFn {
    const common::ObTabletID &tablet_id_;
    explicit MatchTabletFn(const common::ObTabletID &tid) : tablet_id_(tid) {}
    bool operator()(const ObAsyncIndexRefreshTask &task) const
    { return task.stat_.tablet_id_ == tablet_id_; }
  };
  return task_pool_.find_in_use(MatchTabletFn(tablet_id));
}

int64_t ObAsyncIndexRefreshWorker::get_in_flight_count() const
{
  return task_pool_.in_use_count();
}

int ObAsyncIndexRefreshWorker::try_acquire_refresh_dop(const int64_t requested_dop)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(requested_dop <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index refresh DOP", K(ret), K(requested_dop));
  } else {
    common::ObSpinLockGuard guard(refresh_dop_lock_);
    // Read the dynamic tenant limit while holding the same lock as the
    // reservation check, so each admission uses one consistent limit.
    const int64_t dop_limit = share::ObDDLUtil::get_real_parallelism(
        share::ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(tenant_id_),
        false /* not a materialized-view refresh */);
    if (requested_dop > dop_limit) {
      // The generated SQL carries this stale DOP. Let the next scheduling round
      // recompute the DOP and regenerate SQL instead of retrying it forever.
      ret = OB_STATE_NOT_MATCH;
    } else if (running_refresh_dop_ > dop_limit - requested_dop) {
      ret = OB_EAGAIN;
    } else {
      running_refresh_dop_ += requested_dop;
    }
  }
  return ret;
}

void ObAsyncIndexRefreshWorker::release_refresh_dop(const int64_t requested_dop)
{
  if (requested_dop > 0) {
    common::ObSpinLockGuard guard(refresh_dop_lock_);
    running_refresh_dop_ -= requested_dop;
  }
}

int64_t ObAsyncIndexRefreshWorker::get_running_refresh_dop() const
{
  common::ObSpinLockGuard guard(refresh_dop_lock_);
  return running_refresh_dop_;
}

void ObAsyncIndexRefreshWorker::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null task in handle", K(ret));
  } else if (!share::ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_)) {
    // Refresh disabled: drop queued task without recording.
    ObAsyncIndexRefreshTask *refresh_task = static_cast<ObAsyncIndexRefreshTask *>(task);
    LOG_TRACE("[ASYNC_INDEX] refresh worker dropped task because _enable_async_index_refresh is off", K(tenant_id_), KPC(refresh_task));
    task_pool_.release(refresh_task);
  } else {
    ObAsyncIndexRefreshTask *refresh_task = static_cast<ObAsyncIndexRefreshTask *>(task);
    // Resolve the per-LS scheduler SAFELY. The raw ls_scheduler_ stamped on the
    // task at enqueue time may already have been freed if the LS went offline
    // while the task sat in the queue. When the worker is owned by a refresher,
    // re-resolve the scheduler by ls_id under the registry lock and hold a
    // liveness reference for the whole execution; if the LS is gone, abandon the
    // task (the next timer round regenerates it from the persisted publish scn).
    // When there is no refresher (unit tests / direct-trigger path), fall back to
    // the pre-stamped pointer -- those callers own the scheduler's lifetime.
    ObLSAsyncIndexScheduler *acquired = nullptr;
    bool abandon = false;
    if (OB_NOT_NULL(refresher_)) {
      acquired = refresher_->acquire_scheduler(refresh_task->stat_.ls_id_);
      if (OB_ISNULL(acquired)) {
        // LS no longer registered (offline / GC / transfer-out): drop without
        // executing so we never dereference freed scheduler memory.
        abandon = true;
        LOG_INFO("async index ls scheduler gone, abandon in-flight refresh task",
                 "ls_id", refresh_task->stat_.ls_id_, K(refresh_task->stat_));
      } else {
        // Pin the freshly-acquired (ref-held) scheduler onto the task so
        // handle()/need_retry()/add_retry_task_ all dereference the live object.
        refresh_task->ls_scheduler_ = acquired;
      }
    }

    bool retried = false;
    if (!abandon) {
      if (OB_FAIL(refresh_task->handle())) {
        LOG_WARN("refresh task handle failed", K(ret));
      }
      if (OB_FAIL(ret) && refresh_task->need_retry()) {
        if (OB_FAIL(add_retry_task_(*refresh_task))) {
          // add_retry_task_ may reject the node when the per-LS retry list is
          // full (OB_SIZE_OVERFLOW); in that case the task is NOT retried and
          // falls through to the finished ring below (regenerated next round).
          LOG_WARN("failed to add retry task", K(ret), K(refresh_task->stat_));
        } else {
          retried = true;
        }
      } else if (OB_FAIL(ret)
                 && refresh_task->stat_.retry_cnt_ >= OB_ASYNC_INDEX_MAX_RETRY_CNT) {
        // Terminal give-up after exhausting retries: count it and warn (throttled).
        ATOMIC_INC(&give_up_task_cnt_);
        if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
          LOG_WARN("async index refresh task gave up after max retries",
                   "result", refresh_task->stat_.result_,
                   "retry_cnt", refresh_task->stat_.retry_cnt_,
                   "give_up_task_cnt", ATOMIC_LOAD(&give_up_task_cnt_),
                   K(refresh_task->stat_));
        }
      }
      // Only record tasks that have truly finished (success, or failure that
      // will not be retried) into the finished ring for observability.
      if (!retried) {
        record_finished_task_(refresh_task->stat_);
      }
    }
    // Release the liveness reference BEFORE returning the task to the pool. Any
    // retry node cloned in add_retry_task_ carries only stat_ + sql_ (no scheduler
    // pointer), so it does not extend the scheduler's lifetime.
    if (OB_NOT_NULL(acquired)) {
      refresher_->release_scheduler(acquired);
    }
    // return task to pool regardless of retry handling result
    task_pool_.release(refresh_task);
  }
}

int ObAsyncIndexRefreshWorker::add_retry_task_(const ObAsyncIndexRefreshTask &task)
{
  int ret = OB_SUCCESS;
  ObLSAsyncIndexScheduler *ls_scheduler = task.ls_scheduler_;
  if (OB_ISNULL(ls_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls scheduler is null, cannot add retry task", K(ret), K(task.stat_));
  } else if (ls_scheduler->retry_task_count() >= OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN) {
    // Per-LS retry list is full. Reject the new node (reject-new policy): keep
    // the older nodes that are closer to their retry time; the rejected task is
    // regenerated by the next timer round from the persisted publish scn, so no
    // data is lost. Count and warn (throttled).
    ret = OB_SIZE_OVERFLOW;
    ATOMIC_INC(&dropped_retry_cnt_);
    if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
      LOG_WARN("async index retry list full, drop retry task",
               "ls_id", ls_scheduler->get_ls_id(),
               "max_len", OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN,
               "dropped_retry_cnt", ATOMIC_LOAD(&dropped_retry_cnt_),
               K(task.stat_));
    }
  } else {
    void *buf = ob_malloc(sizeof(ObRetryTaskNode), "AsyncIdxRetry");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc retry node", K(ret));
    } else {
      ObRetryTaskNode *node = new (buf) ObRetryTaskNode();
      node->stat_ = task.stat_;
      // Waiting for tenant DOP is not an execution failure and must not consume
      // the bounded error-retry budget.
      if (!task.is_dop_quota_deferred()) {
        node->stat_.retry_cnt_++;
      }
      const int64_t delay_us = calc_async_index_retry_delay_us(node->stat_.retry_cnt_);
      node->retry_after_ts_ = common::ObTimeUtility::current_monotonic_time() + delay_us;
      // carry over the already-generated SQL so the retry executes without
      // regenerating it (same batch => identical SQL)
      if (OB_FAIL(node->sql_.assign(task.sql_))) {
        LOG_WARN("failed to copy sql into retry node", K(ret), K(task.stat_));
        node->~ObRetryTaskNode();
        ob_free(node);
      } else {
        // Add to the LS scheduler's retry list (per-LS isolation).
        common::ObSpinLockGuard guard(ls_scheduler->get_retry_lock());
        if (ls_scheduler->is_stopped() || !ls_scheduler->is_leader()
            || task.stat_.leader_epoch_ != ls_scheduler->get_leader_epoch()) {
          // Scheduler already stopped or switched to follower; discard the retry
          // node to avoid leaking it into a list that nobody drains.
          node->~ObRetryTaskNode();
          ob_free(node);
          ATOMIC_INC(&dropped_retry_cnt_);
        } else if (ls_scheduler->get_retry_tasks().get_size() >= OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN) {
          // The preliminary size check is only an optimization: enforce the cap
          // atomically with insertion when several workers finish concurrently.
          ret = OB_SIZE_OVERFLOW;
          node->~ObRetryTaskNode();
          ob_free(node);
          ATOMIC_INC(&dropped_retry_cnt_);
        } else if (!ls_scheduler->get_retry_tasks().add_last(node)) {
          ret = OB_ERR_UNEXPECTED;
          node->~ObRetryTaskNode();
          ob_free(node);
          LOG_WARN("failed to add retry node to list", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexRefreshWorker::record_finished_task_(const ObRefreshTaskStat &stat)
{
  {
    common::ObSpinLockGuard guard(finished_lock_);
    const int64_t pos = finished_head_ % OB_ASYNC_INDEX_FINISHED_RING_SIZE;
    finished_ring_[pos] = stat;
    finished_head_ = (finished_head_ + 1) % OB_ASYNC_INDEX_FINISHED_RING_SIZE;
    if (finished_count_ < OB_ASYNC_INDEX_FINISHED_RING_SIZE) {
      ++finished_count_;
    }
  }
  record_adaptive_sample_(stat, common::ObTimeUtility::current_time());
}

int ObAsyncIndexRefreshWorker::get_in_flight_stats(
  common::ObIArray<ObRefreshTaskStat> &stats) const
{
  int ret = OB_SUCCESS;
  // Reserve to the pool's hard capacity so push_back inside the spin lock never
  // triggers an ObSEArray expand (which would call ob_malloc under the lock).
  // task_pool_ is instantiated with OB_ASYNC_INDEX_TASK_QUEUE_SIZE.
  if (OB_FAIL(stats.reserve(stats.count() + OB_ASYNC_INDEX_TASK_QUEUE_SIZE))) {
    LOG_WARN("failed to reserve stats array", K(ret));
  } else {
    int inner_ret = OB_SUCCESS;
    // predicate always returns false so the walk continues to the end; we
    // discard find_in_use's match-bool return value.
    struct CollectStatsFn {
      common::ObIArray<ObRefreshTaskStat> &stats_;
      int &inner_ret_;
      CollectStatsFn(common::ObIArray<ObRefreshTaskStat> &stats, int &inner_ret)
        : stats_(stats), inner_ret_(inner_ret) {}
      bool operator()(const ObAsyncIndexRefreshTask &task) const
      {
        ObRefreshTaskStat stat;
        task.copy_stat(stat);
        int tmp_ret = stats_.push_back(stat);
        if (OB_SUCCESS != tmp_ret) {
          inner_ret_ = tmp_ret;
          return true;
        } else {
          return false;
        }
      }
    };
    task_pool_.find_in_use(CollectStatsFn(stats, inner_ret));
    ret = inner_ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to push stat into stats", K(ret));
    }
  }
  return ret;
}

int ObAsyncIndexRefreshWorker::get_recent_finished_stats(
    common::ObIArray<ObRefreshTaskStat> &stats) const
{
  int ret = OB_SUCCESS;
  stats.reset();
  // Reserve outside the spin lock: push_back must not allocate while holding a
  // lock shared with the task-completion hot path.
  if (OB_FAIL(stats.reserve(OB_ASYNC_INDEX_FINISHED_RING_SIZE))) {
    LOG_WARN("failed to reserve finished stats array", K(ret));
  } else {
    common::ObSpinLockGuard guard(finished_lock_);
    // When not yet wrapped, valid entries are [0, finished_count_) with the oldest
    // at index 0. Once full, finished_head_ points at the oldest (next overwrite).
    const bool wrapped = finished_count_ >= OB_ASYNC_INDEX_FINISHED_RING_SIZE;
    for (int64_t i = 0; OB_SUCC(ret) && i < finished_count_; ++i) {
      const int64_t idx = wrapped
          ? (finished_head_ + i) % OB_ASYNC_INDEX_FINISHED_RING_SIZE
          : i;
      if (OB_FAIL(stats.push_back(finished_ring_[idx]))) {
        LOG_WARN("failed to push finished stat", K(ret), K(i));
      }
    }
  }
  return ret;
}

void ObAsyncIndexRefreshWorker::get_adaptive_parallelism(
    const common::ObTabletID &index_tablet_id,
    const int64_t checkpoint_backlog,
    const int64_t refresh_interval_us,
    const bool is_complete_refresh,
    int64_t &parallelism)
{
  parallelism = OB_ASYNC_INDEX_MIN_PARALLELISM;
  if (share::ObAsyncIndexRefreshUtil::is_async_index_adaptive_parallelism_enabled(tenant_id_)) {
    const int64_t parallelism_cap =
      share::ObDDLUtil::get_real_parallelism(
          share::ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(tenant_id_),
          false /* is_mv_refresh */);
    get_adaptive_parallelism_(
        index_tablet_id, checkpoint_backlog, refresh_interval_us,
        is_complete_refresh, parallelism_cap,
        common::ObTimeUtility::current_time(), parallelism);
  }
}

// Choose the DOP for one index tablet from its recent periodic-refresh samples.
void ObAsyncIndexRefreshWorker::get_adaptive_parallelism_(
    const common::ObTabletID &index_tablet_id,
    const int64_t checkpoint_backlog,
    const int64_t refresh_interval_us,
    const bool is_complete_refresh,
    const int64_t parallelism_cap,
    const int64_t now_us,
    int64_t &parallelism)
{
  // Start from the minimum DOP; periodic refreshes may raise it from history.
  parallelism = OB_ASYNC_INDEX_MIN_PARALLELISM;
  // A disabled periodic interval has no latency target, so keep the minimum DOP.
  if (!is_complete_refresh && refresh_interval_us > 0) {
    const int64_t capped_parallelism =
        MAX(OB_ASYNC_INDEX_MIN_PARALLELISM, parallelism_cap);
    // Sampling and scheduling can run concurrently. Hold the controller lock
    // while locating, expiring, evaluating, and updating this tablet's state.
    common::ObSpinLockGuard guard(adaptive_dop_lock_);
    ObAdaptiveDopState *state = nullptr;
    int64_t state_idx = -1;
    // Controller states are stored in a bounded array keyed by index tablet.
    for (int64_t i = 0; i < adaptive_dop_state_count_; ++i) {
      if (adaptive_dop_states_[i].tablet_id_ == index_tablet_id) {
        state = &adaptive_dop_states_[i];
        state_idx = i;
        break;
      }
    }
    // Discard stale feedback so an inactive tablet restarts from the cold-start.
    if (OB_NOT_NULL(state)
        && now_us > state->last_update_ts_
        && now_us - state->last_update_ts_ > OB_ASYNC_INDEX_DOP_STATE_TTL_US) {
      // Fill the removed slot with the last active entry.
      adaptive_dop_states_[state_idx] =
          adaptive_dop_states_[adaptive_dop_state_count_ - 1];
      adaptive_dop_states_[adaptive_dop_state_count_ - 1].reset();
      --adaptive_dop_state_count_;
      state = nullptr;
    }
    if (OB_NOT_NULL(state)) {
      // Reuse the DOP that produced the samples, but clamp it if the current
      // configured DOP cap is lower than when those samples were collected.
      parallelism = MIN(
          capped_parallelism,
          MAX(OB_ASYNC_INDEX_MIN_PARALLELISM, state->sample_dop_));
      // Calculate the slow and fast thresholds, to decide whether to increase or decrease the DOP.
      if (parallelism == state->sample_dop_) {
        const int64_t interval_quotient = refresh_interval_us / 100;
        const int64_t interval_remainder = refresh_interval_us % 100;
        const int64_t slow_threshold_us =
            interval_quotient * OB_ASYNC_INDEX_SLOW_THRESHOLD_PERCENT
            + interval_remainder * OB_ASYNC_INDEX_SLOW_THRESHOLD_PERCENT / 100;
        const int64_t fast_threshold_us =
            interval_quotient * OB_ASYNC_INDEX_FAST_THRESHOLD_PERCENT
            + interval_remainder * OB_ASYNC_INDEX_FAST_THRESHOLD_PERCENT / 100;
        // Estimate the largest current-DOP duration that remains within the slow
        // threshold after dropping one worker.
        const int64_t lower_dop_safe_threshold_us =
            slow_threshold_us - slow_threshold_us / parallelism
            - (0 == slow_threshold_us % parallelism ? 0 : 1);
        // Require two comparable samples before changing DOP, and move only one
        // level per decision. Two slow samples request more workers. Two fast
        // samples may release one worker only when there is no real backlog and
        // the projected lower-DOP duration still fits within the slow threshold.
        if (2 == state->sample_count_
            && state->latest_elapsed_us_ > slow_threshold_us
            && state->previous_elapsed_us_ > slow_threshold_us) {
          parallelism = MIN(capped_parallelism, parallelism + 1);
        } else if (2 == state->sample_count_
                   && checkpoint_backlog <= 1
                   && state->latest_elapsed_us_ < fast_threshold_us
                   && state->previous_elapsed_us_ < fast_threshold_us
                   && state->latest_elapsed_us_ <= lower_dop_safe_threshold_us
                   && state->previous_elapsed_us_ <= lower_dop_safe_threshold_us) {
          parallelism = MAX(OB_ASYNC_INDEX_MIN_PARALLELISM, parallelism - 1);
        }
      }
      if (parallelism != state->sample_dop_) {
        // Samples from the old DOP are not comparable with the new setting.
        // Keep the selected DOP, clear both durations, and wait for two fresh
        // samples before making another adjustment.
        state->sample_dop_ = parallelism;
        state->previous_elapsed_us_ = 0;
        state->latest_elapsed_us_ = 0;
        state->sample_count_ = 0;
        state->last_update_ts_ = now_us;
      }
    }
  }
}

int ObAsyncIndexRefreshWorker::ensure_adaptive_dop_states_()
{
  int ret = OB_SUCCESS;
  bool need_alloc = false;
  {
    common::ObSpinLockGuard guard(adaptive_dop_lock_);
    need_alloc = OB_ISNULL(adaptive_dop_states_);
  }
  if (need_alloc) {
    void *buf = ob_malloc(
        sizeof(ObAdaptiveDopState) * OB_ASYNC_INDEX_TASK_QUEUE_SIZE,
        lib::ObMemAttr(tenant_id_, "AsyncDopState"));
    ObAdaptiveDopState *new_states = static_cast<ObAdaptiveDopState *>(buf);
    if (OB_NOT_NULL(new_states)) {
      for (int64_t i = 0; i < OB_ASYNC_INDEX_TASK_QUEUE_SIZE; ++i) {
        new (&new_states[i]) ObAdaptiveDopState();
      }
    }
    {
      common::ObSpinLockGuard guard(adaptive_dop_lock_);
      if (OB_ISNULL(adaptive_dop_states_) && OB_NOT_NULL(new_states)) {
        adaptive_dop_states_ = new_states;
        new_states = nullptr;
      } else if (OB_ISNULL(adaptive_dop_states_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (OB_NOT_NULL(new_states)) {
      free_adaptive_dop_states_(new_states);
    }
  }
  return ret;
}

void ObAsyncIndexRefreshWorker::free_adaptive_dop_states_(ObAdaptiveDopState *states)
{
  if (OB_NOT_NULL(states)) {
    for (int64_t i = 0; i < OB_ASYNC_INDEX_TASK_QUEUE_SIZE; ++i) {
      states[i].~ObAdaptiveDopState();
    }
    ob_free(states);
  }
}

void ObAsyncIndexRefreshWorker::record_adaptive_sample_(
    const ObRefreshTaskStat &stat,
    const int64_t now_us)
{
  // Keep only clean periodic incremental samples so elapsed times remain comparable.
  if (OB_SUCCESS == stat.result_
      && stat.tablet_id_.is_valid()
      && ASYNC_REFRESH_TYPE_PERIOD == stat.refresh_type_
      && !stat.is_complete_refresh_
      && 0 == stat.retry_cnt_
      && stat.refresh_parallelism_ >= OB_ASYNC_INDEX_MIN_PARALLELISM
      && stat.start_time_ >= 0
      && stat.end_time_ > stat.start_time_
      && now_us > 0) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ensure_adaptive_dop_states_())) {
      LOG_WARN("failed to alloc adaptive dop state cache", K(ret), K_(tenant_id));
      return;
    }
    const int64_t elapsed_us = stat.end_time_ - stat.start_time_;
    // Scheduling and task completion may access the controller state concurrently.
    common::ObSpinLockGuard guard(adaptive_dop_lock_);
    ObAdaptiveDopState *state = nullptr;
    int64_t oldest_idx = 0;
    // Find this tablet's state while tracking the eviction candidate on a miss.
    for (int64_t i = 0; i < adaptive_dop_state_count_; ++i) {
      if (adaptive_dop_states_[i].tablet_id_ == stat.tablet_id_) {
        state = &adaptive_dop_states_[i];
        break;
      } else if (adaptive_dop_states_[i].last_update_ts_
                 < adaptive_dop_states_[oldest_idx].last_update_ts_) {
        // Record the oldest state index.
        oldest_idx = i;
      }
    }
    // Reset the state if it is stale.
    if (OB_NOT_NULL(state)
        && now_us > state->last_update_ts_
        && now_us - state->last_update_ts_ > OB_ASYNC_INDEX_DOP_STATE_TTL_US) {
      state->reset();
    }
    // Create a new state if it is not found.
    if (OB_ISNULL(state)) {
      if (adaptive_dop_state_count_ < OB_ASYNC_INDEX_TASK_QUEUE_SIZE) {
        // Add a new state if the state count is less than the queue size.
        state = &adaptive_dop_states_[adaptive_dop_state_count_++];
      } else {
        // Reuse the oldest state if the state count is equal to the queue size.
        state = &adaptive_dop_states_[oldest_idx];
      }
      state->reset();
    }
    state->tablet_id_ = stat.tablet_id_;
    // DOP changes invalidate old samples; otherwise retain only the latest two.
    if (state->sample_dop_ != stat.refresh_parallelism_) {
      // Drop the old samples, and record the new DOP and elapsed time,
      // if the new DOP is different from the current DOP.
      state->sample_dop_ = stat.refresh_parallelism_;
      state->previous_elapsed_us_ = 0;
      state->latest_elapsed_us_ = elapsed_us;
      state->sample_count_ = 1;
    } else if (0 == state->sample_count_) {
      // Record the new elapsed time if the sample count is 0.
      state->latest_elapsed_us_ = elapsed_us;
      state->sample_count_ = 1;
    } else {
      // Record the new elapsed time if the sample count is 2,
      // and the new DOP is the same as the current DOP.
      state->previous_elapsed_us_ = state->latest_elapsed_us_;
      state->latest_elapsed_us_ = elapsed_us;
      state->sample_count_ = 2;
    }
    state->last_update_ts_ = now_us;
  }
}

} // namespace storage
} // namespace oceanbase
