/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_REPLAYSERVICE_OB_LOG_FOLLOWER_REPLAY_PROCESS_CACHE_H
#define OCEANBASE_LOGSERVICE_REPLAYSERVICE_OB_LOG_FOLLOWER_REPLAY_PROCESS_CACHE_H

#include "share/scn.h"
#include "logservice/replayservice/ob_log_replay_reporter.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_rwlock.h"
#include "lib/random/ob_random.h"

namespace oceanbase {
namespace logservice {

template <typename LockT>
class ObTryLockGuard
{
public:
  [[nodiscard]] explicit ObTryLockGuard(LockT &lock);
  ~ObTryLockGuard();
  inline int get_ret() const { return ret_; }
  inline bool locked() const { return locked_; }
private:
  // disallow copy
  ObTryLockGuard(const ObTryLockGuard &other);
  ObTryLockGuard &operator=(const ObTryLockGuard &other);
  // disallow new
  void *operator new(std::size_t size);
  void *operator new(std::size_t size, const std::nothrow_t &nothrow_constant) throw();
  void *operator new(std::size_t size, void *ptr) throw();
private:
  // data members
  LockT &lock_;
  bool locked_;
  int ret_;
};

template <typename LockT>
inline ObTryLockGuard<LockT>::ObTryLockGuard(LockT &lock)
  : lock_(lock),
      locked_(false),
      ret_(common::OB_SUCCESS)
{
  if (OB_UNLIKELY(common::OB_SUCCESS != (ret_ = lock_.trylock()))) {
    if (common::OB_EAGAIN != ret_) {
      COMMON_LOG_RET(ERROR, ret_, "Fail to trylock, ", K_(ret));
    } else { } // do nothing
  } else {
    locked_ = true;
  }
}

template <typename LockT>
inline ObTryLockGuard<LockT>::~ObTryLockGuard()
{
  constexpr static int64_t SLEEP_US = 10_ms;
  int64_t random_sleep_us = 0;
  if (locked_) {
    while (OB_UNLIKELY(common::OB_SUCCESS != (ret_ = lock_.unlock()))) {
      random_sleep_us = common::ObRandom::rand(SLEEP_US, 2 * SLEEP_US);
      COMMON_LOG_RET(ERROR, ret_, "Fail to unlock, retry...", K_(ret), K(random_sleep_us));
      ob_usleep(static_cast<uint32_t>(random_sleep_us));
    }
  }
}

// no need to use lock here, because replica cache is protected by the caller, e.g. LSReplayProcessCache.
class ReplicaReplayProcessCache {
public:
  ReplicaReplayProcessCache(const ObLSID &ls_id);
  ~ReplicaReplayProcessCache();
  int update_replica_replay_process(const LSReplicaReplayReachingMachine &replay_reaching_machine,
    const bool leader_view_follower_is_in_sync, const int64_t now_us, bool &need_notify, share::SCN &notify_scn);
  int check_and_update_in_rto_group(const int64_t now_us, const int64_t cache_valid_interval_us,
    bool &in_group, share::SCN &max_replayed_scn, palf::LSN &min_unreplayed_lsn);
  bool is_stale(const int64_t now_us, const int64_t stale_interval_us) const;
  TO_STRING_KV(K_(ls_id), K_(machine), K_(last_update_time_us));
private:
  share::ObLSID ls_id_;
  LSReplicaReplayReachingMachine machine_;
  int64_t last_update_time_us_;
};

class LSReplayProcessCache {
public:
  LSReplayProcessCache();
  ~LSReplayProcessCache();
  int init(const share::ObLSID &ls_id, obrpc::ObLogServiceRpcProxy *rpc_proxy);
  void destroy();
  int update_replica_replay_process(const common::ObAddr &replica, const LSReplicaReplayReachingMachine &replay_reaching_machine, const bool leader_view_follower_is_in_sync, const int64_t now_us);
  int check_and_update_rto_group_validity(const int64_t now_us);
  int get_faster_replica_max_replayed_point(share::SCN &max_replayed_scn,
    palf::LSN &min_unreplayed_lsn, int64_t &publish_time_us) const;
  TO_STRING_KV(K_(is_inited), K_(ls_id), KP(rpc_proxy_), K_(last_update_time_us), K_(is_rto_group_valid), K_(ls_max_replayed_scn_val), K_(ls_max_min_unreplayed_lsn_val));
private:
  constexpr static int64_t CACHE_VALID_TIME_INTERVAL_US = 2_s;
  constexpr static int64_t RTO_GROUP_VALIDITY_CHECK_INTERVAL_US = 500_ms;
  constexpr static int64_t REPLICA_CACHE_STALE_TIME_INTERVAL_US = CACHE_VALID_TIME_INTERVAL_US;
  static_assert(CACHE_VALID_TIME_INTERVAL_US <= REPLICA_CACHE_STALE_TIME_INTERVAL_US, "cache valid time interval must be less than replica cache stale time interval");
private:
  class CheckAndEvictReplicaReplayProcessCacheFunctor {
  public:
    CheckAndEvictReplicaReplayProcessCacheFunctor(const int64_t now_us, const int64_t cache_valid_interval_us,
      const int64_t stale_interval_us);
    ~CheckAndEvictReplicaReplayProcessCacheFunctor();
    bool operator()(const common::ObAddr &replica, ReplicaReplayProcessCache *replica_replay_process_cache);
    bool is_rto_group_valid() const;
    int get_ls_max_point(share::SCN &ls_max_max_replayed_scn, palf::LSN &ls_max_min_unreplayed_lsn) const;
    int64_t evicted_cnt() const { return evicted_cnt_; }
  private:
    const int64_t now_us_;
    const int64_t cache_valid_interval_us_;
    const int64_t stale_interval_us_;
    bool is_rto_group_valid_;
    share::SCN ls_max_replayed_scn_;
    palf::LSN ls_max_min_unreplayed_lsn_;
    int64_t evicted_cnt_;
  };
  class DestroyReplicaReplayProcessCacheFunctor {
  public:
    DestroyReplicaReplayProcessCacheFunctor();
    ~DestroyReplicaReplayProcessCacheFunctor();
    bool operator()(const common::ObAddr &replica, ReplicaReplayProcessCache *replica_replay_process_cache);
  };
private:
  int get_or_create_replica_replay_process_cache_(const common::ObAddr &replica, ReplicaReplayProcessCache *&replica_replay_process_cache);
  void update_ls_max_replayed_scn_val_(const share::SCN &replica_max_replayed_scn, const palf::LSN &replica_min_unreplayed_lsn);
  int async_send_notify_follower_move_out_from_rto_group_(obrpc::ObLogServiceRpcProxy *rpc_proxy,
                                                          const common::ObAddr &replica,
                                                          const share::SCN &notify_scn,
                                                          const share::ObLSID &ls_id);
  int inner_check_and_update_rto_group_validity_(const int64_t now_us);
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  mutable RWLock lock_;
  mutable common::ObSpinLock validity_check_lock_;
  bool is_inited_;
  share::ObLSID ls_id_;
  mutable obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  // atomic to show the last update time of the RTO group
  int64_t last_update_time_us_;
  // atomic to show the validity of the RTO group
  bool is_rto_group_valid_;
  // atomic to show the max_replay_scn ever received from all replicas
  uint64_t ls_max_replayed_scn_val_;
  // atomic to show the min_unreplayed_lsn ever received from all replicas
  uint64_t ls_max_min_unreplayed_lsn_val_;
  common::ObLinearHashMap<common::ObAddr, ReplicaReplayProcessCache*> replica_cache_;
};

} // end namespace logservice
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_REPLAYSERVICE_OB_LOG_FOLLOWER_REPLAY_PROCESS_CACHE_H