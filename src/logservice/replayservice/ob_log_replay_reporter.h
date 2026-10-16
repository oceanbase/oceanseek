/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_REPORTER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_REPORTER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "lib/lock/ob_rwlock.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace obrpc
{
class ObLogServiceRpcProxy;
class LSReplicaReplayReachingMachineWrapper;
};
namespace logservice
{

enum class LSReplicaReplayReachingStatus {
  REPLAY_STATUS_REACHING = 0,
  REPLAY_STATUS_SYNCING = 1,
};

struct LSReplicaReplayReachingMachine {
  share::ObLSID ls_id_;
  LSReplicaReplayReachingStatus status_;
  share::SCN last_reach_to_sync_scn_;
  bool is_active_follower_;
  share::SCN max_replayed_scn_;
  palf::LSN min_unreplayed_lsn_;
  ObAddr election_leader_;
  int64_t proposal_id_;
  TO_STRING_KV(K_(ls_id), K_(status), K_(last_reach_to_sync_scn), K_(is_active_follower),
    K_(max_replayed_scn), K_(min_unreplayed_lsn), K_(election_leader), K_(proposal_id));
  LSReplicaReplayReachingMachine(const share::ObLSID &ls_id);
  LSReplicaReplayReachingMachine(const share::ObLSID &ls_id, LSReplicaReplayReachingStatus status,
    const share::SCN &last_reach_to_sync_scn, const bool is_active_follower,
    const share::SCN &max_replayed_scn, const palf::LSN &min_unreplayed_lsn,
    const ObAddr &election_leader, const int64_t proposal_id);
  bool is_valid() const;
  void reset();
};

// Wrapper to protect LSReplicaReplayReachingMachine with a spin lock
struct LSReplicaReplayReachingMachineWithLock {
  common::ObSpinLock lock_;
  LSReplicaReplayReachingMachine machine_;
  explicit LSReplicaReplayReachingMachineWithLock(const share::ObLSID &ls_id)
    : lock_(common::ObLatchIds::OB_LOG_REPLAY_REPORTER_LOCK), machine_(ls_id) {}
};

class ObLogReplayProcessWatcher {
public:
  ObLogReplayProcessWatcher();
  virtual ~ObLogReplayProcessWatcher();
  int init(obrpc::ObLogServiceRpcProxy *rpc_proxy);
  void destroy();
  int add_ls(const share::ObLSID &ls_id);
  int remove_ls(const share::ObLSID &ls_id);
  int report_replay_reaching_machine_status();
  int update_ls_replica_replay_reaching_machine(const share::ObLSID &ls_id,
                                                const share::SCN &max_replayed_scn,
                                                const palf::LSN &min_unreplayed_lsn,
                                                const ObAddr &election_leader,
                                                const int64_t proposal_id,
                                                const bool is_in_sync,
                                                const bool is_active_follower);
  int mark_follower_active_flag(const share::ObLSID &ls_id, const bool is_active_follower);
  int notify_follower_move_out_from_rto_group(const share::ObLSID &ls_id, const share::SCN &replica_last_reach_to_sync_scn);
  TO_STRING_KV(K_(is_inited), KP(rpc_proxy_), K_(last_report_log_time_us));
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  class ReportReplayReachingMachineFunctor
  {
  public:
    ReportReplayReachingMachineFunctor();
    ~ReportReplayReachingMachineFunctor();
    bool operator()(const share::ObLSID &ls_id, LSReplicaReplayReachingMachineWithLock *machine_wrapper);
    struct SnapshotItem {
      share::ObLSID ls_id_;
      LSReplicaReplayReachingMachine snapshot_;
      TO_STRING_KV(K_(ls_id), K_(snapshot));
      SnapshotItem() : ls_id_(), snapshot_(ls_id_) {}
      SnapshotItem(const share::ObLSID &ls_id, const LSReplicaReplayReachingMachine &snapshot)
        : ls_id_(ls_id), snapshot_(snapshot) {}
    };
    // Most (8 + 88) * 4 = 384 bytes, can be put in stack
    common::ObSEArray<SnapshotItem, 4> snapshots_;
  };
private:
  constexpr static int64_t REPORT_LOG_INTERVAL_US = 10_s;
  static int switch_status(LSReplicaReplayReachingMachine &machine, const share::SCN &max_replayed_scn, const bool is_in_sync);
private:
  mutable RWLock lock_;
  bool is_inited_;
  mutable obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  int64_t last_report_log_time_us_;
  common::ObLinearHashMap<share::ObLSID, LSReplicaReplayReachingMachineWithLock*> ls_replica_replay_reaching_machine_map_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_REPORTER_H_
