/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_replay_reporter.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace logservice {

LSReplicaReplayReachingMachine::LSReplicaReplayReachingMachine(const share::ObLSID &ls_id)
  : ls_id_(ls_id),
    status_(LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING),
    last_reach_to_sync_scn_(share::SCN::min_scn()),
    is_active_follower_(false),
    max_replayed_scn_(share::SCN::min_scn()),
    min_unreplayed_lsn_(palf::LSN(0)),
    election_leader_(),
    proposal_id_(palf::INVALID_PROPOSAL_ID) {}

LSReplicaReplayReachingMachine::LSReplicaReplayReachingMachine(const share::ObLSID &ls_id, LSReplicaReplayReachingStatus status,
  const share::SCN &last_reach_to_sync_scn, const bool is_active_follower,
  const share::SCN &max_replayed_scn, const palf::LSN &min_unreplayed_lsn, const ObAddr &election_leader, const int64_t proposal_id)
  : ls_id_(ls_id),
    status_(status),
    last_reach_to_sync_scn_(last_reach_to_sync_scn),
    is_active_follower_(is_active_follower),
    max_replayed_scn_(max_replayed_scn),
    min_unreplayed_lsn_(min_unreplayed_lsn),
    election_leader_(election_leader),
    proposal_id_(proposal_id) {}

bool LSReplicaReplayReachingMachine::is_valid() const
{
  return ls_id_.is_valid() && max_replayed_scn_.is_valid() && min_unreplayed_lsn_.is_valid()
    && election_leader_.is_valid() && proposal_id_ >= 0 && proposal_id_ != palf::INVALID_PROPOSAL_ID;
}

void LSReplicaReplayReachingMachine::reset()
{
  ls_id_.reset();
  status_ = LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING;
  last_reach_to_sync_scn_.reset();
  is_active_follower_ = false;
  max_replayed_scn_.reset();
  min_unreplayed_lsn_.reset();
  election_leader_.reset();
  proposal_id_ = palf::INVALID_PROPOSAL_ID;
}

ObLogReplayProcessWatcher::ObLogReplayProcessWatcher()
  : lock_(common::ObLatchIds::OB_LOG_REPLAY_REPORTER_RWLOCK),
    is_inited_(false),
    rpc_proxy_(NULL),
    last_report_log_time_us_(common::OB_INVALID_TIMESTAMP),
    ls_replica_replay_reaching_machine_map_() {}

ObLogReplayProcessWatcher::~ObLogReplayProcessWatcher() {
  destroy();
}

int ObLogReplayProcessWatcher::init(obrpc::ObLogServiceRpcProxy *rpc_proxy) {
  int ret = OB_SUCCESS;
  const uint64_t MAP_TENANT_ID = MTL_ID();
  WLockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher init twice", K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "rpc_proxy is NULL", K(ret));
  } else if (OB_FAIL(
    ls_replica_replay_reaching_machine_map_.init("RTO.REPLAY.RWRM", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "ls_replica_replay_reaching_machine_map_ init error", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObLogReplayProcessWatcher::destroy() {
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
  } else {
    is_inited_ = false;
    // Free value pointers stored in map to avoid leaks.
    struct DestroyMachineFunctor
    {
      bool operator()(const share::ObLSID &ls_id, LSReplicaReplayReachingMachineWithLock *machine_wrapper)
      {
        UNUSED(ls_id);
        if (OB_NOT_NULL(machine_wrapper)) {
          MTL_DELETE(LSReplicaReplayReachingMachineWithLock, "ReplayRM", machine_wrapper);
        }
        return true; // remove
      }
    } destroy_functor;
    do {
      ret = ls_replica_replay_reaching_machine_map_.remove_if(destroy_functor);
      if (OB_EAGAIN == ret) {
        CLOG_LOG(WARN, "failed to remove ls_replica_replay_reaching_machine_map_", KR(ret));
        ob_usleep(10_ms);
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "failed to remove ls_replica_replay_reaching_machine_map_", KR(ret));
      }
    } while (OB_EAGAIN == ret);
    rpc_proxy_ = NULL;
    ATOMIC_STORE(&last_report_log_time_us_, common::OB_INVALID_TIMESTAMP);
    ls_replica_replay_reaching_machine_map_.destroy();
  }
}

int ObLogReplayProcessWatcher::add_ls(const share::ObLSID &ls_id) {
  int ret = OB_SUCCESS;
  LSReplicaReplayReachingMachineWithLock *check_exist_var = NULL;
  LSReplicaReplayReachingMachineWithLock *ls_replica_replay_reaching_machine = NULL;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.get(ls_id, check_exist_var)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(ERROR, "get ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
  } else if (OB_NOT_NULL(check_exist_var)) {
    check_exist_var = NULL;
    ret = OB_ENTRY_EXIST;
    CLOG_LOG(WARN, "ls_id already exists", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_replica_replay_reaching_machine = MTL_NEW(LSReplicaReplayReachingMachineWithLock, "ReplayRM", ls_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc ls_replica_replay_reaching_machine", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.insert(ls_id, ls_replica_replay_reaching_machine))) {
    CLOG_LOG(ERROR, "insert ls_replica_replay_reaching_machine failed", K(ret));
  } else {
    CLOG_LOG(INFO, "add_ls success", K(ret), K(ls_id));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(ls_replica_replay_reaching_machine)) {
      MTL_DELETE(LSReplicaReplayReachingMachineWithLock, "ReplayRM", ls_replica_replay_reaching_machine);
      ls_replica_replay_reaching_machine = NULL;
    }
  }
  return ret;
}

int ObLogReplayProcessWatcher::remove_ls(const share::ObLSID &ls_id) {
  int ret = OB_SUCCESS;
  LSReplicaReplayReachingMachineWithLock *ls_replica_replay_reaching_machine = NULL;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.get(ls_id, ls_replica_replay_reaching_machine))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(TRACE, "ls not exist in ls_replica_replay_reaching_machine_map_", K(ret), K(ls_id));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(ERROR, "get ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.erase(ls_id))) {
    CLOG_LOG(ERROR, "erase ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
  } else {
    MTL_DELETE(LSReplicaReplayReachingMachineWithLock, "ReplayRM", ls_replica_replay_reaching_machine);
    ls_replica_replay_reaching_machine = NULL;
    CLOG_LOG(INFO, "remove_ls success", K(ret), K(ls_id));
  }
  return ret;
}

ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor::ReportReplayReachingMachineFunctor()
  : snapshots_() {}

ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor::~ReportReplayReachingMachineFunctor() {
  snapshots_.reset();
}

bool ObLogReplayProcessWatcher::ReportReplayReachingMachineFunctor::operator()(const share::ObLSID &ls_id, LSReplicaReplayReachingMachineWithLock *machine_wrapper) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(machine_wrapper)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "[RTO.REPLAY] machine_wrapper is NULL", K(ret), K(ls_id));
  } else {
    // snapshot under lock to avoid holding lock during RPC
    LSReplicaReplayReachingMachine snapshot(ls_id);
    const ObAddr &self_addr = GCONF.self_addr_;
    {
      ObSpinLockGuard guard(machine_wrapper->lock_);
      snapshot = machine_wrapper->machine_;
    }
    if (false == snapshot.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(TRACE, "[RTO.REPLAY] machine snapshot is invalid", K(ret), K(ls_id), K(snapshot));
    } else if (!snapshot.is_active_follower_) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(TRACE, "[RTO.REPLAY] not active follower, do nothing", K(ret), K(ls_id), K(snapshot));
    } else if (self_addr == snapshot.election_leader_) {
      CLOG_LOG(TRACE, "[RTO.REPLAY] self is leader, skip report", K(ret), K(ls_id), K(snapshot));
    } else {
      SnapshotItem item(ls_id, snapshot);
      if (OB_FAIL(snapshots_.push_back(item))) {
        CLOG_LOG(WARN, "[RTO.REPLAY] push_back snapshot failed", K(ret), K(ls_id), K(snapshot));
      }
    }
  }
  return true;
}

int ObLogReplayProcessWatcher::report_replay_reaching_machine_status() {
  int ret = OB_SUCCESS;
  ReportReplayReachingMachineFunctor functor;
  obrpc::ObLogServiceRpcProxy *local_rpc_proxy = NULL;
  {
    RLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
    } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.for_each(functor))) {
      CLOG_LOG(ERROR, "for_each ls_replica_replay_reaching_machine_map_ failed", K(ret));
    } else {
      // Copy pointer under lock to avoid data race with destroy().
      local_rpc_proxy = rpc_proxy_;
    }
  }
  // Lock is released here, now send RPCs for collected snapshots
  if (OB_SUCC(ret)) {
    const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
    const ObAddr &self_addr = GCONF.self_addr_;
    if (OB_ISNULL(local_rpc_proxy)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "rpc_proxy_ is NULL", K(ret));
    } else {
      for (int64_t i = 0; i < functor.snapshots_.count(); ++i) {
        const ReportReplayReachingMachineFunctor::SnapshotItem &item = functor.snapshots_.at(i);
        const share::ObLSID &ls_id = item.ls_id_;
        const LSReplicaReplayReachingMachine &snapshot = item.snapshot_;
        LogFollowerReportReplayReachingMachineReq req(self_addr, ls_id, snapshot);
        int rpc_ret = local_rpc_proxy->to(snapshot.election_leader_)
          .timeout(CONN_TIMEOUT_US)
          .trace_time(true)
          .max_process_handler_time(static_cast<int32_t>(CONN_TIMEOUT_US))
          .by(MTL_ID())
          .group_id(share::OBCG_CLOG)
          .report_replay_reaching_machine(req, NULL);
        if (OB_FAIL(rpc_ret)) {
          CLOG_LOG(WARN, "[RTO.REPLAY] report_replay_reaching_machine failed", K(rpc_ret), K(ls_id), K(snapshot));
        } else {
          CLOG_LOG(TRACE, "[RTO.REPLAY] report_replay_reaching_machine success", K(rpc_ret), K(ls_id), K(snapshot));
        }
      }
      const int64_t now_us = ObClockGenerator::getClock();
      const int64_t last_report_log_time_us = ATOMIC_LOAD(&last_report_log_time_us_);
      if ((common::OB_INVALID_TIMESTAMP == last_report_log_time_us
           || now_us >= last_report_log_time_us + REPORT_LOG_INTERVAL_US)
          && ATOMIC_BCAS(&last_report_log_time_us_, last_report_log_time_us, now_us)) {
        CLOG_LOG(INFO, "[RTO.REPLAY] report replay reaching machine summary", K(ret),
            "snapshot_count", functor.snapshots_.count());
      } else {
        CLOG_LOG(TRACE, "report_replay_reaching_machine_status finished", K(ret),
            "snapshot_count", functor.snapshots_.count());
      }
    }
  }
  return ret;
}

int ObLogReplayProcessWatcher::switch_status(
    LSReplicaReplayReachingMachine &machine, const share::SCN &max_replayed_scn, const bool is_in_sync) {
  int ret = OB_SUCCESS;
  switch (machine.status_) {
    case LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING: {
      if (is_in_sync
          && max_replayed_scn > machine.last_reach_to_sync_scn_) {
        machine.status_ = LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING;
        machine.last_reach_to_sync_scn_ = max_replayed_scn;
        CLOG_LOG(INFO, "switch to syncing status", K(ret), K(machine), K(is_in_sync), K(max_replayed_scn));
      } else {
        CLOG_LOG(TRACE, "not switch to syncing status, do nothing", K(ret), K(machine), K(is_in_sync), K(max_replayed_scn));
      }
      break;
    }
    case LSReplicaReplayReachingStatus::REPLAY_STATUS_SYNCING: {
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid status", K(ret), K(machine), K(is_in_sync), K(max_replayed_scn));
      break;
    }
  }
  return ret;
}

int ObLogReplayProcessWatcher::mark_follower_active_flag(const share::ObLSID &ls_id,
                                                         const bool is_active_follower)
{
  int ret = OB_SUCCESS;
  LSReplicaReplayReachingMachineWithLock *ls_replica_replay_reaching_machine = NULL;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "ls_id is invalid", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.get(ls_id, ls_replica_replay_reaching_machine))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "ls is not tracked, skip marking follower active flag", K(ls_id), K(is_active_follower));
    } else {
      CLOG_LOG(ERROR, "get ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls_replica_replay_reaching_machine)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_replica_replay_reaching_machine is NULL", K(ret), K(ls_id));
  } else {
    ObSpinLockGuard guard(ls_replica_replay_reaching_machine->lock_);
    const bool old_is_active_follower =
        ls_replica_replay_reaching_machine->machine_.is_active_follower_;
    ls_replica_replay_reaching_machine->machine_.is_active_follower_ = is_active_follower;
    if (old_is_active_follower != is_active_follower) {
      CLOG_LOG(INFO, "follower active flag changed", K(ret), K(ls_id),
          K(old_is_active_follower), K(is_active_follower));
    } else {
      CLOG_LOG(TRACE, "follower active flag unchanged", K(ret), K(ls_id), K(is_active_follower));
    }
  }
  return ret;
}

int ObLogReplayProcessWatcher::update_ls_replica_replay_reaching_machine(
    const share::ObLSID &ls_id,
    const share::SCN &max_replayed_scn,
    const palf::LSN &min_unreplayed_lsn,
    const ObAddr &election_leader,
    const int64_t proposal_id,
    const bool is_in_sync,
    const bool is_active_follower) {
  int ret = OB_SUCCESS;
  LSReplicaReplayReachingMachineWithLock *ls_replica_replay_reaching_machine = NULL;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
  } else if (!ls_id.is_valid()
             || !max_replayed_scn.is_valid()
             || !min_unreplayed_lsn.is_valid()
             || !election_leader.is_valid()
             || proposal_id < 0
             || proposal_id == palf::INVALID_PROPOSAL_ID) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "update val for machine is invalid", K(ret), K(ls_id), K(max_replayed_scn), K(min_unreplayed_lsn),
        K(election_leader), K(proposal_id));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.get(ls_id, ls_replica_replay_reaching_machine))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "ls is not tracked, skip updating replay reaching machine", K(ls_id));
    } else {
      CLOG_LOG(ERROR, "get ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls_replica_replay_reaching_machine)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_replica_replay_reaching_machine is NULL", K(ret), K(ls_id));
  } else {
    ObSpinLockGuard guard(ls_replica_replay_reaching_machine->lock_);
    const bool old_is_active_follower =
        ls_replica_replay_reaching_machine->machine_.is_active_follower_;
    ls_replica_replay_reaching_machine->machine_.max_replayed_scn_ = max_replayed_scn;
    ls_replica_replay_reaching_machine->machine_.min_unreplayed_lsn_ = min_unreplayed_lsn;
    ls_replica_replay_reaching_machine->machine_.election_leader_ = election_leader;
    ls_replica_replay_reaching_machine->machine_.proposal_id_ = proposal_id;
    ls_replica_replay_reaching_machine->machine_.is_active_follower_ = is_active_follower;
    if (old_is_active_follower != is_active_follower) {
      CLOG_LOG(INFO, "follower active flag changed", K(ret), K(ls_id),
          K(old_is_active_follower), K(is_active_follower));
    }
    if (is_active_follower && OB_FAIL(ObLogReplayProcessWatcher::switch_status(
        ls_replica_replay_reaching_machine->machine_, max_replayed_scn, is_in_sync))) {
      CLOG_LOG(ERROR, "[RTO.REPLAY] switch_status failed", K(ret), K(ls_id), K(max_replayed_scn),
          K(ls_replica_replay_reaching_machine->machine_), K(is_in_sync));
    } else {
      CLOG_LOG(TRACE, "[RTO.REPLAY] update_ls_replica_replay_reaching_machine success", K(ret), K(ls_id),
          K(max_replayed_scn), K(ls_replica_replay_reaching_machine->machine_), K(is_in_sync));
    }
  }
  return ret;
}

int ObLogReplayProcessWatcher::notify_follower_move_out_from_rto_group(
    const share::ObLSID &ls_id, const share::SCN &replica_last_reach_to_sync_scn) {
  int ret = OB_SUCCESS;
  LSReplicaReplayReachingMachineWithLock *ls_replica_replay_reaching_machine = NULL;
  CLOG_LOG(INFO, "[RTO.REPLAY] Receive notify_follower_move_out_from_rto_group", K(ls_id), K(replica_last_reach_to_sync_scn));
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogReplayProcessWatcher not init", K(ret));
  } else if (!replica_last_reach_to_sync_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "replica_last_reach_to_sync_scn is invalid", K(ret), K(replica_last_reach_to_sync_scn));
  } else if (OB_FAIL(ls_replica_replay_reaching_machine_map_.get(ls_id, ls_replica_replay_reaching_machine))) {
    CLOG_LOG(WARN, "get ls_replica_replay_reaching_machine failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_replica_replay_reaching_machine)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_replica_replay_reaching_machine is NULL", K(ret), K(ls_id));
  } else {
    ObSpinLockGuard guard(ls_replica_replay_reaching_machine->lock_);
    if (replica_last_reach_to_sync_scn >= ls_replica_replay_reaching_machine->machine_.last_reach_to_sync_scn_) {
      // notify the follower to be outside the group of RTO, and need to try re-sync again
      ls_replica_replay_reaching_machine->machine_.status_ = LSReplicaReplayReachingStatus::REPLAY_STATUS_REACHING;
      CLOG_LOG(INFO, "[RTO.REPLAY] notify follower move out from RTO group, change status to REPLAY_STATUS_REACHING",
          K(ls_id), K(replica_last_reach_to_sync_scn), K(ls_replica_replay_reaching_machine->machine_));
    } else {
      // replica_last_reach_to_sync_scn < machine.last_reach_to_sync_scn_, do nothing
      CLOG_LOG(INFO, "[RTO.REPLAY] replica_last_reach_to_sync_scn < machine.last_reach_to_sync_scn_, do nothing",
          K(ls_id), K(replica_last_reach_to_sync_scn), K(ls_replica_replay_reaching_machine->machine_));
    }
  }
  return ret;
}


} // namespace logservice
} // namespace oceanbase
