/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_SUBMIT_LOG_RATE_LIMITER_H
#define OCEANBASE_LOGSERVICE_OB_LOG_SUBMIT_LOG_RATE_LIMITER_H

#include "lib/utility/ob_rate_limiter.h"
#include "replayservice/ob_log_follower_replay_process_cache.h"
#include "replayservice/ob_log_replay_reporter.h"
#include "lib/lock/ob_rwlock.h"
#include "lib/lock/ob_spin_lock.h"

#include <type_traits>
#include <cmath>

namespace oceanbase {
namespace ipalf {
class IPalfHandle;
class IPalfEnv;
} // end namespace ipalf
namespace logservice {

template<typename T, class = typename std::enable_if<sizeof(T) <= sizeof(int64_t)>::type>
bool add_overflow(const T &lhs, const T &rhs, T &res)
{
  return __builtin_add_overflow(lhs, rhs, &res);
}

// no need to use lock here, because the modification of the ls rate limiter is done in the function stack and protected by the caller, e.g. ObLogSSRTOKeeper.
class ObLogLSSubmitLogRateLimiter : public lib::ObRateLimiter {
public:
  ObLogLSSubmitLogRateLimiter();
  virtual ~ObLogLSSubmitLogRateLimiter();
  int init(const share::ObLSID &ls_id, obrpc::ObLogServiceRpcProxy *rpc_proxy, ipalf::IPalfEnv *palf_env);
  void destroy();
  int update_replica_replay_process(const common::ObAddr &replica, const LSReplicaReplayReachingMachine &replay_reaching_machine, const bool leader_view_follower_is_in_sync);
  virtual bool is_force_allows() const override;
  virtual void reset_force_allows() override;
  void set_force_allows();
  // return OB_SUCCESS for acquire success and OB_EAGAIN for not acquired
  virtual int try_acquire(const int64_t permits = 1,
                          const int64_t arg0 = 0,
                          const int64_t arg1 = 0) override;
  virtual int acquire(const int64_t permits = 1,
                      const int64_t arg0 = 0,
                      const int64_t arg1 = 0) override;
  TO_STRING_KV(K_(is_inited), K_(need_ignore_rate_limit), K_(is_force_allows), K_(replay_lag_threshold_us), K_(committed_end_scn_val_us),
    K_(ls_id), KP(ipalf_handle_), KP(ipalf_env_), K_(last_pass_one_log_to_keep_alive_us), K_(last_force_allows_update_time_us),
    K_(last_leader_point_update_time_us), K_(last_print_log_time_us), KP(rpc_proxy_), K_(ls_replay_process_cache));
private:
  // The smooth random-reject zone width above the configured replay-lag threshold.
  // lag < threshold: pass; [threshold, threshold + zone): linear probability reject; >= threshold + zone: reject.
  constexpr static int64_t RATE_LIMIT_RANDOM_ZONE_US = 2_s;
  static_assert(RATE_LIMIT_RANDOM_ZONE_US - 1 > 0,
                "RATE_LIMIT_RANDOM_ZONE_US must be greater than 1us to ensure zone_size > 0");
  constexpr static int64_t PASS_ONE_LOG_TO_KEEP_ALIVE_INTERVAL_US = 50_ms;
  constexpr static int64_t UPDATE_LEADER_POINT_INTERVAL_US = 25_ms;
  constexpr static int64_t UPDATE_LEADER_POINT_FAIL_WAIT_US = 5_ms;
  constexpr static int64_t FORCE_ALLOWS_UPDATE_INTERVAL_US = 500_ms;
  constexpr static int64_t FORCE_ALLOWS_UPDATE_FAIL_WAIT_US = 50_ms;
private:
  bool pass_one_log_to_keep_alive_(const int64_t now_us);
  int update_force_allows_(const int64_t now_us);
  int update_leader_point_(const int64_t now_us);
  int scn_need_rate_limit_(const share::SCN &max_replayed_scn) const;
private:
  bool is_inited_;
  bool need_ignore_rate_limit_;
  bool is_force_allows_;
  int64_t replay_lag_threshold_us_;
  int64_t committed_end_scn_val_us_;
  share::ObLSID ls_id_;
  ipalf::IPalfHandle *ipalf_handle_;
  ipalf::IPalfEnv *ipalf_env_;
  common::ObSpinLock pass_one_log_to_keep_alive_lock_;
  common::ObSpinLock force_allows_update_lock_;
  common::ObSpinLock last_leader_point_update_lock_;
  int64_t last_pass_one_log_to_keep_alive_us_;
  int64_t last_force_allows_update_time_us_;
  int64_t last_leader_point_update_time_us_;
  int64_t last_print_log_time_us_;
  mutable obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  LSReplayProcessCache ls_replay_process_cache_;
};

class ObLogSSRTOKeeper {
public:
  ObLogSSRTOKeeper();
  ~ObLogSSRTOKeeper();
  int init(obrpc::ObLogServiceRpcProxy *rpc_proxy);
  void destroy();
  int add_ls(const share::ObLSID &ls_id, ipalf::IPalfEnv *palf_env);
  int remove_ls(const share::ObLSID &ls_id);
  int get_ls_submit_log_rate_limiter(const share::ObLSID &ls_id, ObLogLSSubmitLogRateLimiter *&ls_submit_log_rate_limiter) const;
  TO_STRING_KV(K_(is_inited), KP(rpc_proxy_));
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  mutable RWLock lock_;
  bool is_inited_;
  mutable obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  common::ObLinearHashMap<share::ObLSID, ObLogLSSubmitLogRateLimiter*> ls_submit_log_rate_limiter_map_;
};

} // end namespace logservice
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_SUBMIT_LOG_RATE_LIMITER_H