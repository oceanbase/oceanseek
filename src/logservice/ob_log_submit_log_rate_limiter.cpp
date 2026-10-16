/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "logservice/ipalf/ipalf_handle.h"
#include "logservice/ipalf/ipalf_env.h"
#include "ob_log_submit_log_rate_limiter.h"
#include "lib/random/ob_random.h"
#include "share/aggregate/util.h"

namespace oceanbase {
namespace logservice {

ObLogLSSubmitLogRateLimiter::ObLogLSSubmitLogRateLimiter()
  : is_inited_(false), need_ignore_rate_limit_(false), is_force_allows_(false),
    replay_lag_threshold_us_(0), committed_end_scn_val_us_(0),
    ls_id_(), ipalf_handle_(nullptr), ipalf_env_(nullptr),
    pass_one_log_to_keep_alive_lock_(common::ObLatchIds::OB_LOG_SUBMIT_LOG_RATE_LIMITER_PASS_ONE_LOG_TO_KEEP_ALIVE_LOCK),
    force_allows_update_lock_(common::ObLatchIds::OB_LOG_SUBMIT_LOG_RATE_LIMITER_FORCE_ALLOWS_UPDATE_LOCK),
    last_leader_point_update_lock_(common::ObLatchIds::OB_LOG_SUBMIT_LOG_RATE_LIMITER_LAST_LEADER_POINT_UPDATE_LOCK),
    last_pass_one_log_to_keep_alive_us_(0),
    last_force_allows_update_time_us_(0), last_leader_point_update_time_us_(0),
    last_print_log_time_us_(0),
    rpc_proxy_(nullptr), ls_replay_process_cache_() {}

ObLogLSSubmitLogRateLimiter::~ObLogLSSubmitLogRateLimiter()
{
  destroy();
}

int ObLogLSSubmitLogRateLimiter::init(const share::ObLSID &ls_id, obrpc::ObLogServiceRpcProxy *rpc_proxy, ipalf::IPalfEnv *palf_env)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogLSSubmitLogRateLimiter has inited", KPC(this));
  } else if (!ls_id.is_valid() || OB_ISNULL(rpc_proxy) || OB_ISNULL(palf_env)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id), KP(rpc_proxy), KP(palf_env));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (FALSE_IT(ipalf_env_ = palf_env)) {
  } else if (OB_FAIL(palf_env->open(ls_id.id(), ipalf_handle_))) {
    CLOG_LOG(WARN, "failed to open palf handle", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_replay_process_cache_.init(ls_id, rpc_proxy))) {
    CLOG_LOG(WARN, "failed to init ls_replay_process_cache_", K(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ATOMIC_STORE(&need_ignore_rate_limit_, (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)));
    // Seed the threshold from tenant config so the first try_acquire (before the periodic
    // refresh kicks in) already reflects the configured value.
    int64_t init_threshold_us = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      init_threshold_us = tenant_config->_log_submit_replay_lag_threshold;
    }
    ATOMIC_STORE(&replay_lag_threshold_us_, init_threshold_us);
    ATOMIC_STORE(&is_force_allows_, (0 == init_threshold_us));
    ATOMIC_STORE(&committed_end_scn_val_us_, 0);
    ls_id_ = ls_id;
    ATOMIC_STORE(&last_pass_one_log_to_keep_alive_us_, 0);
    ATOMIC_STORE(&last_force_allows_update_time_us_, 0);
    ATOMIC_STORE(&last_leader_point_update_time_us_, 0);
    ATOMIC_STORE(&last_print_log_time_us_, 0);
    rpc_proxy_ = rpc_proxy;
    ATOMIC_STORE(&is_inited_, true);
    CLOG_LOG(INFO, "init ObLogLSSubmitLogRateLimiter success", KPC(this));
  }
  // cleanup partial init to avoid leaking palf handle / nullptr deref in destroy()
  if (OB_FAIL(ret) && OB_NOT_NULL(ipalf_env_) && OB_NOT_NULL(ipalf_handle_) && ipalf_handle_->is_valid()) {
    ipalf_env_->close(ipalf_handle_);
    ipalf_env_ = nullptr;
    ipalf_handle_ = nullptr;
  }
  return ret;
}

void ObLogLSSubmitLogRateLimiter::destroy()
{
  int ret = OB_SUCCESS;
  // Mark as not inited first so concurrent callers fail fast.
  ATOMIC_STORE(&is_inited_, false);
  ATOMIC_STORE(&need_ignore_rate_limit_, false);
  ATOMIC_STORE(&is_force_allows_, false);
  ATOMIC_STORE(&replay_lag_threshold_us_, 0);
  ATOMIC_STORE(&committed_end_scn_val_us_, 0);
  ls_id_ = share::ObLSID();
  if (OB_NOT_NULL(ipalf_env_) && OB_NOT_NULL(ipalf_handle_)) {
    ObLockGuard<common::ObSpinLock> guard(last_leader_point_update_lock_);
    if (ipalf_handle_->is_valid()) {
      ipalf_env_->close(ipalf_handle_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "invalid palf handle", KR(ret), KP(ipalf_handle_));
    }
    ipalf_handle_ = nullptr;
  }
  ipalf_env_ = nullptr;
  ATOMIC_STORE(&last_pass_one_log_to_keep_alive_us_, 0);
  ATOMIC_STORE(&last_force_allows_update_time_us_, 0);
  ATOMIC_STORE(&last_leader_point_update_time_us_, 0);
  ATOMIC_STORE(&last_print_log_time_us_, 0);
  rpc_proxy_ = nullptr;
  ls_replay_process_cache_.destroy();
}

int ObLogLSSubmitLogRateLimiter::update_force_allows_(const int64_t now_us)
{
  int ret = OB_SUCCESS;
  int64_t read_last_force_allows_update_time_us = ATOMIC_LOAD(&last_force_allows_update_time_us_);
  if (now_us > read_last_force_allows_update_time_us + FORCE_ALLOWS_UPDATE_INTERVAL_US) {
    ObTryLockGuard<common::ObSpinLock> guard(force_allows_update_lock_);
    if (guard.locked()) {
      read_last_force_allows_update_time_us = ATOMIC_LOAD(&last_force_allows_update_time_us_);
      if (now_us > read_last_force_allows_update_time_us + FORCE_ALLOWS_UPDATE_INTERVAL_US) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (OB_UNLIKELY(!tenant_config.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "invalid tenant config", KR(ret));
          ATOMIC_STORE(&last_force_allows_update_time_us_,
            now_us - FORCE_ALLOWS_UPDATE_INTERVAL_US + FORCE_ALLOWS_UPDATE_FAIL_WAIT_US);
        } else {
          const int64_t threshold_us = tenant_config->_log_submit_replay_lag_threshold;
          ATOMIC_STORE(&replay_lag_threshold_us_, threshold_us);
          ATOMIC_STORE(&is_force_allows_, (0 == threshold_us));
          ATOMIC_STORE(&last_force_allows_update_time_us_, now_us);
        }
      }
    }
  }
  return ret;
}

int ObLogLSSubmitLogRateLimiter::update_leader_point_(const int64_t now_us)
{
  int ret = OB_SUCCESS;
  share::SCN committed_end_scn;
  int64_t read_last_leader_point_update_time_us = ATOMIC_LOAD(&last_leader_point_update_time_us_);
  if (now_us > read_last_leader_point_update_time_us + UPDATE_LEADER_POINT_INTERVAL_US) {
    ObTryLockGuard<common::ObSpinLock> guard(last_leader_point_update_lock_);
    if (guard.locked()) {
      read_last_leader_point_update_time_us = ATOMIC_LOAD(&last_leader_point_update_time_us_);
      if (now_us > read_last_leader_point_update_time_us + UPDATE_LEADER_POINT_INTERVAL_US) {
        if (OB_ISNULL(ipalf_handle_) || false == ipalf_handle_->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "invalid palf handle", KR(ret), K(ls_id_), KP(ipalf_handle_));
        } else if (OB_FAIL(ipalf_handle_->get_end_scn(committed_end_scn))) {
          CLOG_LOG(WARN, "failed to get max submitted scn", KR(ret));
        } else {
          ATOMIC_STORE(&committed_end_scn_val_us_, committed_end_scn.get_val_for_logservice() / 1000);
          CLOG_LOG(INFO, "update leader point", KR(ret), K(ls_id_), K(committed_end_scn));
        }
        ATOMIC_STORE(&last_leader_point_update_time_us_, (OB_SUCCESS == ret)?
          now_us: (now_us - UPDATE_LEADER_POINT_INTERVAL_US + UPDATE_LEADER_POINT_FAIL_WAIT_US));
      }
    }
  }
  return ret;
}

int ObLogLSSubmitLogRateLimiter::update_replica_replay_process(const common::ObAddr &replica,
  const LSReplicaReplayReachingMachine &replay_reaching_machine, const bool leader_view_follower_is_in_sync)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = common::ObTimeUtil::current_monotonic_raw_time();
  CLOG_LOG(INFO, "[RTO.REPLAY] Receive replica replay reaching machine", K(ls_id_), K(replica),
    K(replay_reaching_machine), K(leader_view_follower_is_in_sync), K(now_us));
  if (!ATOMIC_LOAD(&is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLSSubmitLogRateLimiter is not init", KR(ret));
  } else if (!GCONF.enable_logservice) { // do nothing
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (OB_FAIL(ls_replay_process_cache_.update_replica_replay_process(replica, replay_reaching_machine, leader_view_follower_is_in_sync, now_us))) {
    CLOG_LOG(WARN, "failed to update replica replay process", KR(ret), K(ls_id_), K(replica), K(replay_reaching_machine));
  } else if (OB_SUCCESS != update_force_allows_(now_us)) {
    CLOG_LOG(WARN, "failed to update force allows", KR(ret), K(ls_id_));
  } else { } // do nothing
  return ret;
}

bool ObLogLSSubmitLogRateLimiter::is_force_allows() const
{
  return ATOMIC_LOAD(&is_force_allows_);
}

void ObLogLSSubmitLogRateLimiter::reset_force_allows()
{
  // This interface is not used now, so do nothing
}

void ObLogLSSubmitLogRateLimiter::set_force_allows()
{
  // This interface is not used now, so do nothing
}

bool ObLogLSSubmitLogRateLimiter::pass_one_log_to_keep_alive_(const int64_t now_us)
{
  bool bool_ret = false;
  int64_t read_last_pass_one_log_to_keep_alive_us = ATOMIC_LOAD(&last_pass_one_log_to_keep_alive_us_);
  if (now_us > read_last_pass_one_log_to_keep_alive_us + PASS_ONE_LOG_TO_KEEP_ALIVE_INTERVAL_US) {
    ObTryLockGuard<common::ObSpinLock> guard(pass_one_log_to_keep_alive_lock_);
    if (guard.locked()) {
      read_last_pass_one_log_to_keep_alive_us = ATOMIC_LOAD(&last_pass_one_log_to_keep_alive_us_);
      if (now_us > read_last_pass_one_log_to_keep_alive_us + PASS_ONE_LOG_TO_KEEP_ALIVE_INTERVAL_US) {
        ATOMIC_STORE(&last_pass_one_log_to_keep_alive_us_, now_us);
        bool_ret = true;
      }
    }
  }
  if (bool_ret) {
    CLOG_LOG(INFO, "[RTO.REPLAY] pass one log to keep alive", K(ls_id_), K(now_us), K(read_last_pass_one_log_to_keep_alive_us));
  }
  return bool_ret;
}

// Rate limit by the fastest follower's replay lag (in microseconds):
//   lag = committed_end_scn - max_replayed_scn
//   lag < threshold                : pass
//   lag in [threshold, threshold+zone) : linear-probability reject
//   lag >= threshold + zone         : reject
// threshold (replay_lag_threshold_us_) comes from config; 0 means no rate limit
// (already short-circuited by is_force_allows_, handled defensively here too).
int ObLogLSSubmitLogRateLimiter::scn_need_rate_limit_(const share::SCN &max_replayed_scn) const
{
  int ret = OB_SUCCESS;
  int64_t max_replayed_scn_val_us = max_replayed_scn.get_val_for_logservice() / 1000;
  int64_t committed_end_scn_val_us = ATOMIC_LOAD(&committed_end_scn_val_us_);
  const int64_t begin_diff_us = ATOMIC_LOAD(&replay_lag_threshold_us_);
  int64_t end_diff_us = 0;
  int64_t max_replayed_plus_end_us = 0;
  if (begin_diff_us <= 0) {
    ret = OB_SUCCESS;
  } else if (add_overflow(begin_diff_us, RATE_LIMIT_RANDOM_ZONE_US, end_diff_us)
             || add_overflow(max_replayed_scn_val_us, end_diff_us, max_replayed_plus_end_us)) {
    ret = OB_OPERATE_OVERFLOW;
    CLOG_LOG(WARN, "scn overflow", KR(ret), K(max_replayed_scn_val_us), K(begin_diff_us),
      K(RATE_LIMIT_RANDOM_ZONE_US), K(end_diff_us), K(max_replayed_plus_end_us));
  } else if (committed_end_scn_val_us < max_replayed_scn_val_us + begin_diff_us) {
    ret = OB_SUCCESS;
  } else if (committed_end_scn_val_us < max_replayed_plus_end_us) {
    // Random zone: linear success probability from 100% (diff=0) to ~0% (diff=zone_size).
    // diff ranges in [0, RATE_LIMIT_RANDOM_ZONE_US).
    const int64_t diff = committed_end_scn_val_us - max_replayed_scn_val_us - begin_diff_us;
    const int64_t zone_size = RATE_LIMIT_RANDOM_ZONE_US - 1;
    // random_val in [0, zone_size] inclusive, probability = (zone_size - diff) / zone_size.
    // Use >= so that diff=0 always passes.
    const int64_t random_val = common::ObRandom::rand(0, zone_size);
    ret = ((random_val >= diff) ? OB_SUCCESS : OB_EAGAIN);
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

// arg0 is scn val in microseconds
// notice: SCN::get_val_for_logservice() return val in nanoseconds
int ObLogLSSubmitLogRateLimiter::try_acquire(const int64_t permits, const int64_t arg0, const int64_t arg1)
{
  UNUSED(permits);
  UNUSED(arg0);
  UNUSED(arg1);
  int ret = OB_SUCCESS;
  share::SCN max_replayed_scn;
  palf::LSN min_unreplayed_lsn;
  int64_t ro_publish_time_us = 0;
  const int64_t now_us = common::ObTimeUtil::current_monotonic_raw_time();
  if (!ATOMIC_LOAD(&is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLSSubmitLogRateLimiter is not init", KR(ret));
  } else if (!GCONF.enable_logservice) { // do nothing
  } else if (ATOMIC_LOAD(&need_ignore_rate_limit_)) { // do nothing
  } else if (FALSE_IT(update_force_allows_(now_us))) { // do nothing
  } else if (ATOMIC_LOAD(&is_force_allows_)) { // do nothing
  } else if (OB_FAIL(ls_replay_process_cache_.check_and_update_rto_group_validity(now_us))) {
    CLOG_LOG(WARN, "failed to check and update rto group validity", KR(ret));
  } else if (OB_FAIL(ls_replay_process_cache_.get_faster_replica_max_replayed_point(
      max_replayed_scn, min_unreplayed_lsn, ro_publish_time_us))) {
    CLOG_LOG(WARN, "failed to get faster replica max replayed point", KR(ret));
  } else if (OB_FAIL(update_leader_point_(now_us))) {
    CLOG_LOG(WARN, "failed to update leader point", KR(ret));
  } else {
    UNUSED(ro_publish_time_us);
    const bool follower_valid = !max_replayed_scn.is_max();
    if (follower_valid) {
      ret = scn_need_rate_limit_(max_replayed_scn);
    }
  }
  // pass one log to keep alive if acquire failed, make sure the log stream can accept new log after the whole ob cluster restarted.
  if (OB_FAIL(ret) && OB_NOT_INIT != ret && pass_one_log_to_keep_alive_(now_us)) {
    ret = OB_SUCCESS;
  // Fail-open on internal errors: only OB_EAGAIN should throttle writers.
  // This ensures append/restore paths don't surface unexpected errors due to cache/metric refresh issues,
  // especially under bad network or transient internal failures.
  } else if (OB_FAIL(ret) && OB_EAGAIN != ret && OB_NOT_INIT != ret) {
    const int64_t last_print = ATOMIC_LOAD(&last_print_log_time_us_);
    if (now_us >= last_print + 1_s && ATOMIC_BCAS(&last_print_log_time_us_, last_print, now_us)) {
      CLOG_LOG(ERROR, "[RTO.REPLAY] try_acquire internal error, fail-open", KR(ret), K(ls_id_),
          K(max_replayed_scn), K(min_unreplayed_lsn), K(now_us));
    }
    ret = OB_SUCCESS;
  } else if (OB_EAGAIN == ret) {
    const int64_t last_print = ATOMIC_LOAD(&last_print_log_time_us_);
    if (now_us >= last_print + 1_s && ATOMIC_BCAS(&last_print_log_time_us_, last_print, now_us)) {
      const int64_t committed_end_scn_val_us = ATOMIC_LOAD(&committed_end_scn_val_us_);
      const int64_t replay_lag_threshold_us = ATOMIC_LOAD(&replay_lag_threshold_us_);
      const int64_t max_replayed_scn_val_us = max_replayed_scn.get_val_for_logservice() / 1000;
      CLOG_LOG(WARN, "[RTO.REPLAY] acquire failed, log rate limit reached", KR(ret), K(ls_id_),
          K(max_replayed_scn), K(min_unreplayed_lsn),
          K(committed_end_scn_val_us),         // where the leader has committed to
          K(replay_lag_threshold_us),          // current throttle threshold
          "lag_us", committed_end_scn_val_us - max_replayed_scn_val_us,  // fastest follower's actual replay lag
          K(now_us));
    }
  } else { } // do nothing
  return ret;
}

int ObLogLSSubmitLogRateLimiter::acquire(const int64_t permits, const int64_t arg0, const int64_t arg1)
{
  UNUSED(permits);
  UNUSED(arg0);
  UNUSED(arg1);
  int ret = OB_NOT_SUPPORTED;
  CLOG_LOG(ERROR, "ObLogLSSubmitLogRateLimiter::acquire is not supported", KR(ret), KPC(this));
  return ret;
}

ObLogSSRTOKeeper::ObLogSSRTOKeeper()
  : lock_(common::ObLatchIds::OB_LOG_SUBMIT_LOG_RATE_LIMITER_RTO_KEEPER_LOCK),
    is_inited_(false),
    rpc_proxy_(nullptr),
    ls_submit_log_rate_limiter_map_() {}

ObLogSSRTOKeeper::~ObLogSSRTOKeeper()
{
  destroy();
}

void ObLogSSRTOKeeper::destroy()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSSRTOKeeper is not init", KR(ret));
  } else {
    is_inited_ = false;
    rpc_proxy_ = nullptr;
    struct DestroyRateLimiterFunctor
    {
      bool operator()(const share::ObLSID &ls_id, ObLogLSSubmitLogRateLimiter *ls_submit_log_rate_limiter)
      {
        UNUSED(ls_id);
        if (OB_NOT_NULL(ls_submit_log_rate_limiter)) {
          MTL_DELETE(ObLogLSSubmitLogRateLimiter, "LgSSRateLimiter", ls_submit_log_rate_limiter);
        }
        return true; // remove
      }
    } destroy_functor;
    int ret = OB_SUCCESS;
    do {
      ret = ls_submit_log_rate_limiter_map_.remove_if(destroy_functor);
      if (OB_EAGAIN == ret) {
        CLOG_LOG(WARN, "failed to remove ls_submit_log_rate_limiter_map_", KR(ret));
        ob_usleep(10_ms);
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "failed to remove ls_submit_log_rate_limiter_map_", KR(ret));
      }
    } while (OB_EAGAIN == ret);
    ls_submit_log_rate_limiter_map_.destroy();
  }
}

int ObLogSSRTOKeeper::init(obrpc::ObLogServiceRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogSSRTOKeeper has inited", KPC(this));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "rpc_proxy is nullptr", K(ret));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.init("RTOKEEPER_MAP", MTL_ID()))) {
    CLOG_LOG(WARN, "failed to init ls_submit_log_rate_limiter_map_", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObLogSSRTOKeeper::add_ls(const share::ObLSID &ls_id, ipalf::IPalfEnv *palf_env)
{
  int ret = OB_SUCCESS;
  ObLogLSSubmitLogRateLimiter *check_exist_var = nullptr;
  ObLogLSSubmitLogRateLimiter *ls_submit_log_rate_limiter = nullptr;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSSRTOKeeper is not init", KR(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(palf_env)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(ls_id), KP(palf_env));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.get(ls_id, check_exist_var)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "failed to get ls_submit_log_rate_limiter", KR(ret), K(ls_id));
  } else if (OB_NOT_NULL(check_exist_var)) {
    check_exist_var = nullptr;
    ret = OB_ENTRY_EXIST;
    CLOG_LOG(WARN, "ls_submit_log_rate_limiter already exists", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_submit_log_rate_limiter = MTL_NEW(ObLogLSSubmitLogRateLimiter, "LgSSRateLimiter"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc ls_submit_log_rate_limiter", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_submit_log_rate_limiter->init(ls_id, rpc_proxy_, palf_env))) {
    CLOG_LOG(WARN, "failed to init ls_submit_log_rate_limiter", KR(ret), K(ls_id), KPC(ls_submit_log_rate_limiter));
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.insert(ls_id, ls_submit_log_rate_limiter))) {
    CLOG_LOG(WARN, "failed to insert ls_submit_log_rate_limiter", KR(ret), K(ls_id), KPC(ls_submit_log_rate_limiter));
  } else {
    CLOG_LOG(INFO, "add_ls success", KR(ret), K(ls_id));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(ls_submit_log_rate_limiter)) {
      MTL_DELETE(ObLogLSSubmitLogRateLimiter, "LgSSRateLimiter", ls_submit_log_rate_limiter);
      ls_submit_log_rate_limiter = nullptr;
    }
  }
  return ret;
}

int ObLogSSRTOKeeper::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLogLSSubmitLogRateLimiter *ls_submit_log_rate_limiter = nullptr;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSSRTOKeeper is not init", KR(ret));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.get(ls_id, ls_submit_log_rate_limiter))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(WARN, "ls not exist in ls_submit_log_rate_limiter_map_", K(ret), K(ls_id));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(ERROR, "get ls_submit_log_rate_limiter failed", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.erase(ls_id))) {
    CLOG_LOG(ERROR, "failed to erase ls_submit_log_rate_limiter", KR(ret), K(ls_id));
  } else {
    MTL_DELETE(ObLogLSSubmitLogRateLimiter, "LgSSRateLimiter", ls_submit_log_rate_limiter);
    ls_submit_log_rate_limiter = nullptr;
  }
  return ret;
}

int ObLogSSRTOKeeper::get_ls_submit_log_rate_limiter(const share::ObLSID &ls_id, ObLogLSSubmitLogRateLimiter *&ls_submit_log_rate_limiter) const
{
  int ret = OB_SUCCESS;
  ObLogLSSubmitLogRateLimiter *local_ls_submit_log_rate_limiter = nullptr;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSSRTOKeeper is not init", KR(ret));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "shared log service is not enabled", K(ret));
  } else if (OB_FAIL(ls_submit_log_rate_limiter_map_.get(ls_id, local_ls_submit_log_rate_limiter))) {
    CLOG_LOG(WARN, "failed to get ls_submit_log_rate_limiter", KR(ret), K(ls_id));
  } else {
    ls_submit_log_rate_limiter = local_ls_submit_log_rate_limiter;
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
