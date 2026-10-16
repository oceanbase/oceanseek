/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_follower_replay_process_cache.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
namespace logservice {

ReplicaReplayProcessCache::ReplicaReplayProcessCache(const ObLSID &ls_id)
  : ls_id_(ls_id),
    machine_(ls_id),
    last_update_time_us_(common::ObTimeUtil::current_monotonic_raw_time())
{
}

ReplicaReplayProcessCache::~ReplicaReplayProcessCache() {
  ls_id_.reset();
  machine_.reset();
  last_update_time_us_ = 0;
}

// the status of machine only can be change in the direction: reaching -> syncing, or syncing -> reaching
// the first direction e.g. reaching -> syncing, is pushed by the followr, and incoming machine`s max_replayed_scn must be greater than the current machine`s max_replayed_scn
// follower think that he is ready to be in the group of RTO
// the second direction e.g. syncing -> reaching, is pushed by the leader, and incoming machine`s max_replayed_scn must be same as current machine`s max_replayed_scn
// leader think that the follower need to be outside the group of RTO, cause long time missing reported message
int ReplicaReplayProcessCache::update_replica_replay_process(const LSReplicaReplayReachingMachine &replay_reaching_machine,
    const bool leader_view_follower_is_in_sync, const int64_t now_us, bool &need_notify, share::SCN &notify_scn)
{
  int ret = OB_SUCCESS;
  need_notify = false;
  notify_scn.reset();
  if (!replay_reaching_machine.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(replay_reaching_machine));
  } else if (replay_reaching_machine.max_replayed_scn_ < machine_.max_replayed_scn_) {
    ret = OB_DISCARD_PACKET;
    CLOG_LOG(WARN, "RPC packages arrived disorder, incoming max_replayed_scn is stale", KR(ret), 
        K(machine_), K(replay_reaching_machine));
  } else {
    if (!replay_reaching_machine.is_active_follower_) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "not active follower", KR(ret), K(replay_reaching_machine));
    } else if (LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING == machine_.status_) {
      if (LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING == replay_reaching_machine.status_) { // do nothing
      } else if (LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING == replay_reaching_machine.status_) {
        // the follower is migrated out and in the machine, this branch maybe happen, and the leader does not aware of it
        // The old and now follower actually is two different replicas, so it need try to join the group of RTO again
        machine_.status_ = LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING;
        CLOG_LOG(INFO, "[RTO.REPLAY] replica is lost, mark status REPLAY_STATUS_REACHING", KR(ret), K(machine_), K(replay_reaching_machine), K(now_us));
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    } else if (LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING == machine_.status_) {
      if (LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING == replay_reaching_machine.status_) {
        if (leader_view_follower_is_in_sync && replay_reaching_machine.last_reach_to_sync_scn_ > machine_.last_reach_to_sync_scn_) {
          machine_ = replay_reaching_machine;
          CLOG_LOG(INFO, "[RTO.REPLAY] replica join the group of RTO successfully", KR(ret), K(machine_), K(replay_reaching_machine), K(now_us));
        } else if (replay_reaching_machine.last_reach_to_sync_scn_ >= machine_.last_reach_to_sync_scn_) {
          // push msg to notify the follower to be outside the group of RTO, and need to try re-sync again
          need_notify = true;
          notify_scn = replay_reaching_machine.last_reach_to_sync_scn_;
          CLOG_LOG(INFO, "[RTO.REPLAY] push msg to notify the follower to be outside the group of RTO, and need to try re-sync again", KR(ret),
            K(machine_), K(replay_reaching_machine), K(notify_scn), K(now_us));
        } else { } // do nothing
      } else if (LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING == replay_reaching_machine.status_) { // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      CLOG_LOG(INFO, "[RTO.REPLAY] update replica replay process cache successfully", KR(ret), K(machine_), K(replay_reaching_machine), K(now_us));
      machine_.max_replayed_scn_ = replay_reaching_machine.max_replayed_scn_;
      machine_.min_unreplayed_lsn_ = replay_reaching_machine.min_unreplayed_lsn_;
      machine_.last_reach_to_sync_scn_ = share::SCN::max(machine_.last_reach_to_sync_scn_, replay_reaching_machine.last_reach_to_sync_scn_);
      last_update_time_us_ = now_us;
    } else if (OB_ERR_UNEXPECTED != ret) {
      CLOG_LOG(WARN, "unexpected status", KR(ret), K(machine_), K(replay_reaching_machine), K(now_us));
    } else {
      CLOG_LOG(ERROR, "unexpected status", KR(ret), K(machine_), K(replay_reaching_machine), K(now_us));
    }
  }
  return ret;
}

int ReplicaReplayProcessCache::check_and_update_in_rto_group(const int64_t now_us, const int64_t cache_valid_interval_us,
    bool &in_group, share::SCN &max_replayed_scn, palf::LSN &min_unreplayed_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t now = now_us > 0 ? now_us : common::ObTimeUtil::current_monotonic_raw_time();
  in_group = false;
  if (LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING == machine_.status_) {
    const bool has_recent_report = (cache_valid_interval_us > 0 && now > 0 && last_update_time_us_ > 0 && now >= last_update_time_us_
        && (now - last_update_time_us_ <= cache_valid_interval_us));
    if (has_recent_report) {
      in_group = true;
      max_replayed_scn = machine_.max_replayed_scn_;
      min_unreplayed_lsn = machine_.min_unreplayed_lsn_;
    } else {
      // follower has not reported for a long time, move it out of the RTO group
      machine_.status_ = LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING;
      CLOG_LOG(INFO, "[RTO.REPLAY] follower timeout, mark status REPLAY_STATUS_REACHING", K(machine_), K(last_update_time_us_), K(now));
    }
  }
  return ret;
}

bool ReplicaReplayProcessCache::is_stale(const int64_t now_us, const int64_t stale_interval_us) const
{
  bool is_stale_cache = false;
  if (stale_interval_us <= 0) {
    is_stale_cache = false;
  } else {
    const int64_t now = now_us > 0 ? now_us : common::ObTimeUtil::current_monotonic_raw_time();
    if (last_update_time_us_ <= 0) {
      is_stale_cache = (now >= stale_interval_us);
    } else if (now >= last_update_time_us_) {
      is_stale_cache = ((now - last_update_time_us_) >= stale_interval_us);
    } else {
      // clock moved backwards, treat as not stale to avoid premature eviction
      is_stale_cache = false;
    }
  }
  return is_stale_cache;
}

LSReplayProcessCache::LSReplayProcessCache()
  : lock_(common::ObLatchIds::OB_LOG_FOLLOWER_REPLAY_PROCESS_CACHE_RWLOCK),
    validity_check_lock_(common::ObLatchIds::OB_LOG_FOLLOWER_REPLAY_PROCESS_CACHE_VALIDITY_CHECK_LOCK),
    is_inited_(false), ls_id_(), rpc_proxy_(nullptr), last_update_time_us_(0), is_rto_group_valid_(false),
    ls_max_replayed_scn_val_(0), ls_max_min_unreplayed_lsn_val_(0), replica_cache_() { }

LSReplayProcessCache::~LSReplayProcessCache() {
    destroy();
}

int LSReplayProcessCache::init(const share::ObLSID &ls_id, obrpc::ObLogServiceRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t MAP_TENANT_ID = MTL_ID();
  WLockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "LSReplayProcessCache has inited", KPC(this));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "rpc_proxy is nullptr", K(ret));
  } else if (OB_FAIL(replica_cache_.init("RTO.REPLAY.LRPC", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "failed to init replica_cache_", K(ret));
  } else {
    ls_id_ = ls_id;
    rpc_proxy_ = rpc_proxy;
    last_update_time_us_ = 0;
    is_rto_group_valid_ = false;
    ls_max_replayed_scn_val_ = 0;
    ls_max_min_unreplayed_lsn_val_ = 0;
    is_inited_ = true;
    CLOG_LOG(INFO, "LSReplayProcessCache has inited", KPC(this));
  }
  return ret;
}

void LSReplayProcessCache::destroy()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LSReplayProcessCache is not init", KR(ret));
  } else {
    is_inited_ = false;
    ls_id_ = share::ObLSID();
    rpc_proxy_ = nullptr;
    last_update_time_us_ = 0;
    is_rto_group_valid_ = false;
    ls_max_replayed_scn_val_ = 0;
    ls_max_min_unreplayed_lsn_val_ = 0;
    DestroyReplicaReplayProcessCacheFunctor destroy_functor;
    do {
      ret = replica_cache_.remove_if(destroy_functor);
      if (OB_EAGAIN == ret) {
        CLOG_LOG(WARN, "failed to destroy replica_cache_", KR(ret));
        ob_usleep(10_ms);
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "failed to destroy replica_cache_", KR(ret));
      }
    } while (OB_EAGAIN == ret);
    replica_cache_.destroy();
  }
}

int LSReplayProcessCache::get_or_create_replica_replay_process_cache_(const common::ObAddr &replica,
  ReplicaReplayProcessCache *&replica_replay_process_cache)
{
  int ret = OB_SUCCESS;
  ReplicaReplayProcessCache* tmp_replica_replay_process_cache = nullptr;
  // expect caller to hold write lock
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "LSReplayProcessCache is not init", KR(ret));
  } else if (OB_FAIL(replica_cache_.get(replica, tmp_replica_replay_process_cache)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "failed to get replica_replay_process_cache", KR(ret), K(replica));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (OB_ISNULL(tmp_replica_replay_process_cache = MTL_NEW(ReplicaReplayProcessCache, "RTO.REPLAY.RRPC", ls_id_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "failed to allocate memory for replica_replay_process_cache", KR(ret));
    } else if (OB_FAIL(replica_cache_.insert(replica, tmp_replica_replay_process_cache))) {
      MTL_DELETE(ReplicaReplayProcessCache, "RTO.REPLAY.RRPC", tmp_replica_replay_process_cache);
      tmp_replica_replay_process_cache = nullptr;
      CLOG_LOG(WARN, "failed to set replica_replay_process_cache", KR(ret), K(replica));
    } else { } // do nothing
  } else { } // do nothing
  if (OB_SUCC(ret)) {
    replica_replay_process_cache = tmp_replica_replay_process_cache;
  }
  return ret;
}

// protected by write lock
void LSReplayProcessCache::update_ls_max_replayed_scn_val_(const share::SCN &replica_max_replayed_scn, const palf::LSN &replica_min_unreplayed_lsn)
{
  int ret = OB_SUCCESS;
  uint64_t replica_max_replayed_scn_val = replica_max_replayed_scn.get_val_for_logservice();
  uint64_t replica_min_unreplayed_lsn_val = replica_min_unreplayed_lsn.val_;
  ATOMIC_STORE(&ls_max_replayed_scn_val_, replica_max_replayed_scn_val);
  ATOMIC_STORE(&ls_max_min_unreplayed_lsn_val_, replica_min_unreplayed_lsn_val);
  CLOG_LOG(INFO, "[RTO.REPLAY] update ls max replayed scn val successfully", KR(ret),
    K(replica_max_replayed_scn), K(replica_min_unreplayed_lsn));
  return;
}

LSReplayProcessCache::CheckAndEvictReplicaReplayProcessCacheFunctor::CheckAndEvictReplicaReplayProcessCacheFunctor(
    const int64_t now_us, const int64_t cache_valid_interval_us, const int64_t stale_interval_us)
  : now_us_(now_us),
    cache_valid_interval_us_(cache_valid_interval_us),
    stale_interval_us_(stale_interval_us),
    is_rto_group_valid_(false),
    ls_max_replayed_scn_(share::SCN::min_scn()),
    ls_max_min_unreplayed_lsn_(0),
    evicted_cnt_(0)
{
}

LSReplayProcessCache::CheckAndEvictReplicaReplayProcessCacheFunctor::~CheckAndEvictReplicaReplayProcessCacheFunctor()
{
  is_rto_group_valid_ = false;
  ls_max_replayed_scn_.set_min();
  ls_max_min_unreplayed_lsn_.val_ = 0;
  evicted_cnt_ = 0;
}

bool LSReplayProcessCache::CheckAndEvictReplicaReplayProcessCacheFunctor::operator()(const common::ObAddr &replica,
    ReplicaReplayProcessCache *replica_replay_process_cache)
{
  int ret = OB_SUCCESS;
  bool need_remove = false;
  bool in_group = false;
  share::SCN replica_max_replayed_scn;
  palf::LSN replica_min_unreplayed_lsn;
  if (OB_ISNULL(replica_replay_process_cache)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "replica_replay_process_cache is nullptr", KR(ret), K(replica));
    need_remove = true;
  } else {
    if (OB_FAIL(replica_replay_process_cache->check_and_update_in_rto_group(now_us_, cache_valid_interval_us_,
      in_group, replica_max_replayed_scn, replica_min_unreplayed_lsn))) {
      CLOG_LOG(WARN, "failed to check and update in rto group", KR(ret), K(replica));
    } else if (in_group && (!replica_max_replayed_scn.is_valid() || !replica_min_unreplayed_lsn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected replica point", KR(ret), K(replica), K(replica_max_replayed_scn), K(replica_min_unreplayed_lsn));
    } else if (in_group) {
      is_rto_group_valid_ = true;
      ls_max_replayed_scn_ = share::SCN::max(ls_max_replayed_scn_, replica_max_replayed_scn);
      ls_max_min_unreplayed_lsn_ = palf::LSN(max(ls_max_min_unreplayed_lsn_.val_, replica_min_unreplayed_lsn.val_));
    } else { } // do nothing
    CLOG_LOG(INFO, "[RTO.REPLAY] check and update in rto group", KR(ret), K(replica), K(in_group),
      KPC(replica_replay_process_cache));
    if (replica_replay_process_cache->is_stale(now_us_, stale_interval_us_)) {
      need_remove = true;
      ++evicted_cnt_;
      CLOG_LOG(INFO, "[RTO.REPLAY] evict stale replica cache", K(replica), K(now_us_), K(stale_interval_us_),
        KPC(replica_replay_process_cache));
      MTL_DELETE(ReplicaReplayProcessCache, "RTO.REPLAY.RRPC", replica_replay_process_cache);
      replica_replay_process_cache = nullptr;
    } else {
      CLOG_LOG(INFO, "[RTO.REPLAY] check replica replay process cache", K(replica), K(in_group),
        KPC(replica_replay_process_cache));
    }
  }
  return need_remove;
}

bool LSReplayProcessCache::CheckAndEvictReplicaReplayProcessCacheFunctor::is_rto_group_valid() const
{
  return is_rto_group_valid_;
}

int LSReplayProcessCache::CheckAndEvictReplicaReplayProcessCacheFunctor::get_ls_max_point(
    share::SCN &ls_max_max_replayed_scn, palf::LSN &ls_max_min_unreplayed_lsn) const
{
  int ret = OB_SUCCESS;
  if (!ls_max_replayed_scn_.is_valid() || !ls_max_min_unreplayed_lsn_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error", KR(ret), K(ls_max_replayed_scn_), K(ls_max_min_unreplayed_lsn_));
  } else {
    ls_max_max_replayed_scn = ls_max_replayed_scn_;
    ls_max_min_unreplayed_lsn = ls_max_min_unreplayed_lsn_;
  }
  return ret;
}

LSReplayProcessCache::DestroyReplicaReplayProcessCacheFunctor::DestroyReplicaReplayProcessCacheFunctor() { }

LSReplayProcessCache::DestroyReplicaReplayProcessCacheFunctor::~DestroyReplicaReplayProcessCacheFunctor() { }

bool LSReplayProcessCache::DestroyReplicaReplayProcessCacheFunctor::operator()(const common::ObAddr &replica,
  ReplicaReplayProcessCache *replica_replay_process_cache)
{
  UNUSED(replica);
  if (OB_NOT_NULL(replica_replay_process_cache)) {
    MTL_DELETE(ReplicaReplayProcessCache, "RTO.REPLAY.RRPC", replica_replay_process_cache);
  }
  return true;
}

// check if the RTO group is valid, if there is no replica in the group, set the validity to false
// use lease to avoid redundant concurrent checks
int LSReplayProcessCache::check_and_update_rto_group_validity(const int64_t now_us_input)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = now_us_input > 0 ? now_us_input : common::ObTimeUtil::current_monotonic_raw_time();
  const int64_t atomic_last_update_time_us = ATOMIC_LOAD(&last_update_time_us_);
  const bool need_check = (now_us >= atomic_last_update_time_us &&
      now_us - atomic_last_update_time_us > RTO_GROUP_VALIDITY_CHECK_INTERVAL_US);
  if (need_check) {
    ObTryLockGuard<common::ObSpinLock> guard(validity_check_lock_);
    if (guard.locked()) {
      const int64_t refreshed_last_update_time_us = ATOMIC_LOAD(&last_update_time_us_);
      const bool refreshed_need_check = (now_us >= refreshed_last_update_time_us &&
          now_us - refreshed_last_update_time_us > RTO_GROUP_VALIDITY_CHECK_INTERVAL_US);
      if (refreshed_need_check) {
        ret = inner_check_and_update_rto_group_validity_(now_us);
        if (false == common::IS_SYSTEM_SUPPORT_MONOTONIC_RAW) {
          CLOG_LOG(WARN, "current system not support CLOCK_MONOTONIC_RAW, use current_time instead");
        } else { } // do nothing
      }
    }
  }
  return ret;
}

int LSReplayProcessCache::inner_check_and_update_rto_group_validity_(const int64_t now_us_input)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = now_us_input > 0 ? now_us_input : common::ObTimeUtil::current_monotonic_raw_time();
  CheckAndEvictReplicaReplayProcessCacheFunctor functor(now_us, CACHE_VALID_TIME_INTERVAL_US,
    REPLICA_CACHE_STALE_TIME_INTERVAL_US);
  WLockGuard guard(lock_);
  if (OB_FAIL(replica_cache_.remove_if(functor))) {
    CLOG_LOG(WARN, "failed to refresh replica cache", KR(ret), K(ls_id_));
  } else {
    const bool functor_valid = functor.is_rto_group_valid();
    const bool cached_valid = ATOMIC_LOAD(&is_rto_group_valid_);
    if (functor_valid != cached_valid) {
      ATOMIC_STORE(&is_rto_group_valid_, functor_valid);
      CLOG_LOG(INFO, "[RTO.REPLAY] update rto group validity", KR(ret), K(ls_id_), K(functor_valid));
    } else {
      CLOG_LOG(INFO, "[RTO.REPLAY] rto group validity unchanged", KR(ret), K(ls_id_), K(functor_valid));
    }
    if (functor_valid) {
      share::SCN ls_max_max_replayed_scn;
      palf::LSN ls_max_min_unreplayed_lsn;
      if (OB_FAIL(functor.get_ls_max_point(ls_max_max_replayed_scn, ls_max_min_unreplayed_lsn))) {
        CLOG_LOG(WARN, "failed to get ls max point", KR(ret), K(ls_id_));
      } else {
        update_ls_max_replayed_scn_val_(ls_max_max_replayed_scn, ls_max_min_unreplayed_lsn);
      }
    } else {
      (void) update_ls_max_replayed_scn_val_(share::SCN::max_scn(), palf::LSN(palf::LOG_MAX_LSN_VAL));
    }
    if (functor.evicted_cnt() > 0) {
      CLOG_LOG(INFO, "[RTO.REPLAY] evict stale replica cache finished", KR(ret), K(ls_id_), K(functor.evicted_cnt()));
    }
    ATOMIC_STORE(&last_update_time_us_, now_us);
    CLOG_LOG(INFO, "[RTO.REPLAY] publish rto group result", KR(ret), K(ls_id_), K(now_us));
  }
  return ret;
}

int LSReplayProcessCache::get_faster_replica_max_replayed_point(share::SCN &max_replayed_scn,
  palf::LSN &min_unreplayed_lsn, int64_t &publish_time_us) const
{
  int ret = OB_SUCCESS;
  bool atomic_is_rto_group_valid = ATOMIC_LOAD(&is_rto_group_valid_);
  uint64_t atomic_ls_max_replayed_scn_val = 0;
  uint64_t atomic_ls_max_min_unreplayed_lsn_val = 0;
  if (atomic_is_rto_group_valid) {
    atomic_ls_max_replayed_scn_val = ATOMIC_LOAD(&ls_max_replayed_scn_val_);
    atomic_ls_max_min_unreplayed_lsn_val = ATOMIC_LOAD(&ls_max_min_unreplayed_lsn_val_);
    ret = max_replayed_scn.convert_for_logservice(atomic_ls_max_replayed_scn_val);
    min_unreplayed_lsn = palf::LSN(atomic_ls_max_min_unreplayed_lsn_val);
    publish_time_us = ATOMIC_LOAD(&last_update_time_us_);
  } else {
    max_replayed_scn.set_max();
    min_unreplayed_lsn = palf::LSN(palf::LOG_MAX_LSN_VAL);
    publish_time_us = 0;
  }
  return ret;
}

int LSReplayProcessCache::async_send_notify_follower_move_out_from_rto_group_(
    obrpc::ObLogServiceRpcProxy *rpc_proxy,
    const common::ObAddr &replica,
    const share::SCN &notify_scn,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const common::ObAddr self_addr = GCTX.self_addr();
  const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
  if (!self_addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "self_addr is invalid", KR(ret), K(replica));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "rpc_proxy_ is NULL", KR(ret), K(replica));
  } else {
    LogNotifyFollowerMoveOutFromRTOGroupResp req(self_addr, ls_id, notify_scn);
    int rpc_ret = rpc_proxy->to(replica)
        .timeout(CONN_TIMEOUT_US)
        .trace_time(true)
        .max_process_handler_time(static_cast<int32_t>(CONN_TIMEOUT_US))
        .by(MTL_ID())
        .group_id(share::OBCG_CLOG)
        .notify_follower_move_out_from_rto_group(req, NULL);
    if (OB_FAIL(rpc_ret)) {
      CLOG_LOG(WARN, "[RTO.REPLAY] notify_follower_move_out_from_rto_group RPC failed", KR(rpc_ret), K(replica), K(ls_id), K(notify_scn), K(req));
    } else {
      CLOG_LOG(INFO, "[RTO.REPLAY] notify_follower_move_out_from_rto_group RPC success", KR(rpc_ret), K(replica), K(ls_id), K(notify_scn), K(req));
    }
  }
  return ret;
}

int LSReplayProcessCache::update_replica_replay_process(const common::ObAddr &replica, const LSReplicaReplayReachingMachine &replay_reaching_machine,
  const bool leader_view_follower_is_in_sync, const int64_t now_us)
{
  int ret = OB_SUCCESS;
  ReplicaReplayProcessCache *replica_replay_process_cache = nullptr;
  bool need_notify = false;
  share::SCN notify_scn;
  obrpc::ObLogServiceRpcProxy *local_rpc_proxy = nullptr;
  share::ObLSID local_ls_id;
  {
    WLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "LSReplayProcessCache is not init", KR(ret), K(ls_id_), K(replica));
    } else if (OB_FAIL(get_or_create_replica_replay_process_cache_(replica, replica_replay_process_cache))) {
      CLOG_LOG(WARN, "failed to get replica_replay_process_cache", KR(ret), K(ls_id_), K(replica));
    } else if (OB_FAIL(replica_replay_process_cache->update_replica_replay_process(replay_reaching_machine, leader_view_follower_is_in_sync,
      now_us, need_notify, notify_scn))) {
      CLOG_LOG(WARN, "failed to update replica replay process", KR(ret), K(ls_id_), K(replica), K(replay_reaching_machine));
    } else {
      // Snapshot fields needed by async RPC under lock to avoid races with destroy().
      local_rpc_proxy = rpc_proxy_;
      local_ls_id = ls_id_;
    }
  }
  // send RPC to notify the follower to be outside the group of RTO
  if (OB_SUCC(ret) && need_notify && notify_scn.is_valid()) {
    (void) async_send_notify_follower_move_out_from_rto_group_(local_rpc_proxy, replica, notify_scn, local_ls_id);
  }
  if (OB_FAIL(ret)) { // do nothing
  } else if (OB_FAIL(inner_check_and_update_rto_group_validity_(now_us))) {
    CLOG_LOG(WARN, "[RTO.REPLAY] failed to check and update rto group validity", KR(ret), K(ls_id_), K(replica));
  } else {
    CLOG_LOG(INFO, "[RTO.REPLAY] update replica replay process successfully", KR(ret), K(ls_id_), K(replica), K(replay_reaching_machine),
      K(leader_view_follower_is_in_sync), K(need_notify), K(notify_scn));
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase