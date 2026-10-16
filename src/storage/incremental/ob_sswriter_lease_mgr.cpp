/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_lease_mgr.h"
#include "storage/incremental/ob_sswriter_rpc.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_locality_manager.h"
#include "storage/tx/ob_ts_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::transaction;

ERRSIM_POINT_DEF(EN_SSWRITER_LEASE_UNAVAILABLE,
    "Pause SSWriter lease grants and renewals for the specified tenant and LS");

namespace oceanbase
{
namespace storage
{
int64_t ObSSWriterEpochGenerator::get_next_value()
{
  const int64_t epoch_value = ATOMIC_LOAD(&epoch_value_);
  return -1 == epoch_value ? -1 : ATOMIC_AAF(&epoch_value_, 1);
}

int64_t ObSSWriterEpochGenerator::get_current_value() const
{
  const int64_t epoch_value = ATOMIC_LOAD(&epoch_value_);
  return epoch_value;
}

bool ObSSWriterGroupKey::is_valid() const
{
  return id_ > 0
         && ls_id_.is_valid()
         && ObSSWriterType::INVALID != sswriter_type_;
}

void ObSSWriterGroupKey::reset()
{
  id_ = -1;
  ls_id_.reset();
  sswriter_type_ = ObSSWriterType::INVALID;
}

int ObSSWriterGroupLease::init(
    const int64_t id,
    const share::ObLSID ls_id,
    const ObSSWriterType type,
    ObSSWriterRegionLease *sswriter_region_lease,
    ObSSWriterEpochGenerator *epoch_generator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterGroupLease inited twice", KR(ret));
  } else if (id <= 0 || !ls_id.is_valid() || ObSSWriterType::INVALID == type
      || NULL == sswriter_region_lease || NULL == epoch_generator || !epoch_generator->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(id), K(type),
        KP(sswriter_region_lease), KP(epoch_generator));
  } else {
    epoch_generator_ = epoch_generator;
    sswriter_region_lease_ = sswriter_region_lease;
    state_ = ObSSWriterGroupState::UNKNOWN;
    group_key_.id_ = id;
    group_key_.ls_id_ = ls_id;
    group_key_.sswriter_type_ = type;
    is_inited_ = true;
  }
  LOG_INFO("ObSSWriterGroupLease init", K(ret), K(*this));
  return ret;
}

void ObSSWriterGroupLease::destroy()
{
  reset();
}

void ObSSWriterGroupLease::reset()
{
  group_key_.reset();
  sswriter_region_lease_ = NULL;
  state_ = ObSSWriterGroupState::INVALID;
  last_update_state_ts_ = -1;
  lease_expire_ts_ = -1;
  epoch_ = -1;
  server_.reset();
  epoch_generator_ = NULL;
  is_inited_ = false;
}

void ObSSWriterGroupLease::reset_lease()
{
  ObSpinLockGuard guard(lock_);
  reset();
}

int ObSSWriterGroupLease::set_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterGroupLease not inited", KR(ret), K(*this));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server));
  } else {
    server_ = server;
  }
  return ret;
}

int ObSSWriterGroupLease::check_status()
{
  int ret = OB_SUCCESS;
  ObSSWriterGrantArg arg;
  bool need_grant = false;
  {
    ObSpinLockGuard guard(lock_);
    const ObSSWriterGroupState state = state_;
    LOG_DEBUG("ObSSWriterGroupLease check status", K(*this));
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObSSWriterGroupLease not inited", KR(ret), K(*this));
    } else {
      switch (state) {
      case ObSSWriterGroupState::UNKNOWN: {
        // TODO, select target sswriter
        set_state_(ObSSWriterGroupState::PREPAREING);
        server_ = GCTX.self_addr();
        set_state_(ObSSWriterGroupState::EXPECTED);
        epoch_ = epoch_generator_->get_next_value();
        arg.group_id_ = group_key_.id_;
        arg.ls_id_ = sswriter_region_lease_->get_ls_id();
        arg.type_ = group_key_.sswriter_type_;
        arg.target_addr_ = server_;
        arg.epoch_ = epoch_;
        need_grant = true;
        break;
      }
      case ObSSWriterGroupState::PREPAREING: {
        // in this state, collection info to select suitable sswriter
        int64_t now = ObTimeUtility::current_time();
        if (SSWRITER_STATE_DURATION_US > now - last_update_state_ts_) {
          // do nothing
        } else {
          set_state_(ObSSWriterGroupState::UNKNOWN);
          epoch_ = -1;
        }
        break;
      }
      case ObSSWriterGroupState::EXPECTED: {
        const int64_t now = ObTimeUtility::current_time();
        if (SSWRITER_STATE_DURATION_US > now - last_update_state_ts_) {
          // do nothing
        } else if (lease_expire_ts_ < now) {
          set_state_(ObSSWriterGroupState::UNKNOWN);
          epoch_ = -1;
        }
        break;
      }
      case ObSSWriterGroupState::EXISTING: {
        const int64_t now = ObTimeUtility::current_time();
        if (SSWRITER_STATE_DURATION_US > now - last_update_state_ts_) {
          // do nothing
        } else if (lease_expire_ts_ < now) {
          set_state_(ObSSWriterGroupState::UNKNOWN);
          epoch_ = -1;
        } else {
          if (TC_REACH_TIME_INTERVAL(4000000)) {
            LOG_INFO("sswriter exist", K(*this));
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    LOG_DEBUG("ObSSWriterGroupLease check status end", K(*this));
  }
  if (need_grant) {
    if (OB_FAIL(MTL(ObSSWriterService*)->grant_sswriter(arg))) {
      LOG_WARN("grant sswriter failed", KR(ret), K(arg));
    } else {
      LOG_DEBUG("grant sswriter success", KR(ret));
    }
  }
  return ret;
}

void ObSSWriterGroupLease::set_state_(const ObSSWriterGroupState state)
{
  LOG_INFO("switch state", K(state), K(*this));
  state_ = state;
  last_update_state_ts_ = ObTimeUtility::current_time();
}

// if lease is not updated, return eagain
int ObSSWriterGroupLease::update_lease(
    const int64_t epoch,
    const int64_t lease_us)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterGroupLease not inited", KR(ret), K(*this));
  } else if (epoch_ != epoch) {
    ret = OB_EAGAIN;
  } else if (ObSSWriterGroupState::EXPECTED != state_
      && ObSSWriterGroupState::EXISTING != state_) {
    ret = OB_EAGAIN;
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t lease_expire_ts = now + lease_us;
    if (lease_expire_ts <= lease_expire_ts_) {
      ret = OB_EAGAIN;
    } else {
      // update lease to a higher value
      lease_expire_ts_ = lease_expire_ts;
      if (ObSSWriterGroupState::EXPECTED == state_) {
        set_state_(ObSSWriterGroupState::EXISTING);
      }
    }
  }
  LOG_INFO("update lease", K(*this));
  return ret;
}

int ObSSWriterRegionLease::init(
    const common::ObRegion &region,
    const share::ObLSID ls_id,
    ObSSWriterEpochGenerator *epoch_generator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterRegionLease inited twice", KR(ret));
  } else if (region.is_empty() || !ls_id.is_valid() || NULL == epoch_generator
      || !epoch_generator->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(region), K(ls_id), KP(epoch_generator));
  } else if (OB_FAIL(default_group_lease_.init(SSWRITER_DEFAULT_GROUP_ID, ls_id,
          ObSSWriterType::FREEZE, this, epoch_generator))) {
    LOG_WARN("default group lease init failed", KR(ret), K(region));
  } else {
    region_ = region;
    ls_id_ = ls_id;
    epoch_generator_ = epoch_generator;
    is_inited_ = true;
  }
  return ret;
}

void ObSSWriterRegionLease::reset_lease()
{
  ls_id_.reset();
  region_.reset();
  default_group_lease_.reset_lease();
  epoch_generator_ = NULL;
  is_inited_ = false;
}

void ObSSWriterRegionLease::destroy()
{
  ls_id_.reset();
  region_.reset();
  default_group_lease_.reset();
  epoch_generator_ = NULL;
  is_inited_ = false;
}

int ObSSWriterRegionLease::check_status()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter region lease mgr not inited", KR(ret));
  } else {
    default_group_lease_.check_status();
  }
  return ret;
}

int ObSSWriterRegionLease::handle_lease_request(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter region lease mgr not inited", KR(ret));
  } else {
    const int64_t group_id = req.get_group_id();
    const ObSSWriterType sswriter_type = req.get_sswriter_type();
    const int64_t lease_us = req.get_lease_us();
    const int64_t epoch = req.get_epoch();
    // TODO, choose target group lease according to group_id and type
    if (SSWRITER_DEFAULT_GROUP_ID == group_id
        && ObSSWriterType::FREEZE == sswriter_type) {
      if (OB_FAIL(default_group_lease_.update_lease(epoch, lease_us))) {
        LOG_WARN("update lease failed", KR(ret), K(epoch), K(lease_us));
      }
    }
  }
  return ret;
}

int ObSSWriterRegionLease::get_sswriter_addr(
    common::ObRegion &target_region,
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    int64_t &remaining_lease_us)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter region lease mgr not inited", KR(ret));
  } else {
    // TODO, get group id according to key
    int64_t group_id = SSWRITER_DEFAULT_GROUP_ID;
    const ObSSWriterType sswriter_type = key.get_type();
    // TODO, choose target group lease according to group_id and type
    if (SSWRITER_DEFAULT_GROUP_ID == group_id) {
      // default group
      switch (sswriter_type) {
      case ObSSWriterType::INVALID: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      case ObSSWriterType::FREEZE:
      case ObSSWriterType::COMPACTION:
      case ObSSWriterType::META:
      case ObSSWriterType::GC: {
        const int64_t lease_expire_ts = default_group_lease_.get_lease_expire_ts();
        const int64_t current_ts = ObTimeUtility::current_time();
        if (lease_expire_ts > current_ts) {
          sswriter_addr.epoch_ = default_group_lease_.get_epoch();
          sswriter_addr.addr_ = default_group_lease_.get_target_addr();
          remaining_lease_us = lease_expire_ts - current_ts;
        } else {
          ret = OB_LEASE_NOT_ENOUGH;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObSSWriterRegionLease::iterate_sswriter_mgr_stat(
    ObSSWriterMgrStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    // ignore
    LOG_INFO("sswriter region lease mgr not inited", KR(ret));
  } else {
    const int64_t current_ts = ObTimeUtility::current_time();
    ObSSWriterMgrStat sswriter_mgr_stat;
    if (OB_FAIL(sswriter_mgr_stat.init(
            default_group_lease_.group_key_.ls_id_,
            default_group_lease_.group_key_.sswriter_type_,
            default_group_lease_.group_key_.id_,
            region_,
            default_group_lease_.state_,
            default_group_lease_.server_,
            current_ts,
            default_group_lease_.last_update_state_ts_,
            default_group_lease_.lease_expire_ts_,
            default_group_lease_.epoch_))) {
      LOG_WARN("sswriter group lease init failed", KR(ret));
    } else if (OB_FAIL(stat_iter.push(sswriter_mgr_stat))) {
      LOG_WARN("push sswriter group stat failed", K(sswriter_mgr_stat));
    } else {
      // success
    }
  }
  return ret;
}

int ObSSWriterLSHandler::init(
    ObLS *ls,
    ObSSWriterServiceTenantType service_tenant_type)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterLSHandler inited twice", KR(ret));
  } else if (NULL == ls || ObSSWriterServiceTenantType::INVALID == service_tenant_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls), K(service_tenant_type));
  } else {
    ls_id_ = ls->get_ls_id();
    ls_ = ls;
    service_tenant_type_ = service_tenant_type;
    is_inited_ = true;
  }
  LOG_INFO("ObSSWriterLSHandler init", KR(ret), K(*this));
  return ret;
}

int ObSSWriterLSHandler::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else {
    // do nothing
  }
  LOG_INFO("ObSSWriterLSHandler start", KR(ret));
  return ret;
}

void ObSSWriterLSHandler::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else {
    // do nothing
  }
  LOG_INFO("ObSSWriterLSHandler stop");
}

void ObSSWriterLSHandler::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else {
    // do nothing
  }
  LOG_INFO("ObSSWriterLSHandler wait");
}

void ObSSWriterLSHandler::destroy()
{
  const int is_inited = is_inited_;
  if (is_inited_) {
    ls_id_.reset();
    ls_ = NULL;
    leader_start_ts_ = -1;
    service_tenant_type_ = ObSSWriterServiceTenantType::INVALID;
    is_inited_ = false;
  }
  LOG_INFO("ObSSWriterLSHandler destroy", K(is_inited));
}

void ObSSWriterLSHandler::switch_to_follower_forcedly()
{
  ObSpinLockGuard guard(lock_);
  lease_service_.stop();
  lease_service_.wait();
  lease_service_.destroy();
  ATOMIC_STORE(&leader_start_ts_, -1);
  LOG_INFO("ObSSWriterLSHandler switch to follower forcely", K(*this));
}

int ObSSWriterLSHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else if (0 < ATOMIC_LOAD(&leader_start_ts_) || NULL == ls_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(*this), K_(leader_start_ts), KP_(ls));
  } else if (OB_FAIL(lease_service_.init(this))) {
    LOG_WARN("lease service init failed", KR(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ATOMIC_STORE(&leader_start_ts_, start_ts);
    if (OB_FAIL(lease_service_.start())) {
      LOG_WARN("lease service start failed", KR(ret));
      ATOMIC_STORE(&leader_start_ts_, -1);
    }
  }
  LOG_INFO("ObSSWriterLSHandler switch to leader", K(ret), K(*this));
  return ret;
}

int ObSSWriterLSHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    lease_service_.stop();
    lease_service_.wait();
    lease_service_.destroy();
    ATOMIC_STORE(&leader_start_ts_, -1);
  }
  LOG_INFO("ObSSWriterLSHandler switch to follower gracefully", K(ret), K(*this));
  return ret;
}

int ObSSWriterLSHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter ls handler not inited", KR(ret));
  } else if (0 < ATOMIC_LOAD(&leader_start_ts_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ATOMIC_STORE(&leader_start_ts_, start_ts);
    if (OB_FAIL(lease_service_.start())) {
      LOG_WARN("lease service start failed", KR(ret));
      ATOMIC_STORE(&leader_start_ts_, -1);
    }
  }
  LOG_INFO("ObSSWriterLSHandler resume leader", K(ret), K(*this));
  return ret;
}

int ObSSWriterLSHandler::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  return ret;
}

share::SCN ObSSWriterLSHandler::get_rec_scn()
{
  return share::SCN::max_scn();
}

int ObSSWriterLSHandler::flush(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSSWriterLSHandler::offline()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  const int64_t leader_start_ts = ATOMIC_LOAD(&leader_start_ts_);
  if (0 < leader_start_ts) {
    // if leader, stop lease service
    lease_service_.stop();
    lease_service_.wait();
    lease_service_.destroy();
    ATOMIC_STORE(&leader_start_ts_, -1);
  }
  LOG_INFO("ObSSWriterLSHandler offline", K(leader_start_ts), K(*this));
  return ret;
}

int ObSSWriterLSHandler::online()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  LOG_INFO("ObSSWriterLSHandler online");
  return ret;
}

int ObSSWriterLSHandler::handle_lease_request(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  const int64_t leader_start_ts = ATOMIC_LOAD(&leader_start_ts_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLSHandler not inited", KR(ret));
  } else if (0 >= leader_start_ts) {
    // not ls leader, return not master
    ret = OB_NOT_MASTER;
    LOG_INFO("not ls leader", KR(ret), K(*this), K(req));
  } else if (OB_FAIL(lease_service_.handle_lease_request(req))) {
    LOG_WARN("handle leaase request", KR(ret), K(req));
  }
  return ret;
}

int ObSSWriterLSHandler::get_sswriter_addr(
    common::ObRegion &target_region,
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    int64_t &remaining_lease_us)
{
  int ret = OB_SUCCESS;
  const int64_t leader_start_ts = ATOMIC_LOAD(&leader_start_ts_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLSHandler not inited", KR(ret));
  } else if (0 >= leader_start_ts) {
    // not ls leader, return not master
    ret = OB_NOT_MASTER;
    LOG_INFO("not ls leader", KR(ret), K(*this), K(key));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_FAIL(lease_service_.get_sswriter_addr(target_region, key,
          sswriter_addr, remaining_lease_us))) {
    LOG_WARN("handle leaase request", KR(ret), K(key));
  } else {
    // do nothing
  }
  LOG_INFO("get sswriter addr", K(ret), K(key), K(sswriter_addr),
      K(remaining_lease_us), K(*this));
  return ret;
}

int ObSSWriterLSHandler::iterate_sswriter_mgr_stat(
    ObSSWriterMgrStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  const int64_t leader_start_ts = ATOMIC_LOAD(&leader_start_ts_);
  if (!is_inited_ || 0 >= leader_start_ts) {
    // ignore this one
    LOG_INFO("no need to iterate this sswriter mgr", K(*this));
  } else if (OB_FAIL(lease_service_.iterate_sswriter_mgr_stat(stat_iter))) {
    LOG_WARN("iterate sswriter mgr stat failed", KR(ret), K(*this));
  }
  return ret;
}

void ObSSWriterLeaseService::reset_lease()
{
  // do nothing
}

int ObSSWriterLeaseService::init(ObSSWriterLSHandler *ls_handler)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == ls_handler) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ls_handler_ = ls_handler;
    ls_id_ = ls_handler->get_ls_id();
    is_inited_ = true;
  }
  return ret;
}

int ObSSWriterLeaseService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter lease service not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter lease service already running", KR(ret));
  } else if (OB_FAIL(MTL(ObSSWriterService*)->register_ls_lease_service(this))) {
    LOG_WARN("register ls lease service failed", KR(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ATOMIC_STORE(&start_ts_, start_ts);
    ATOMIC_STORE(&epoch_base_, -1);
    is_running_ = true;
  }
  LOG_INFO("ObSSWriterLeaseService start", KR(ret), K(*this));
  return ret;
}

void ObSSWriterLeaseService::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter lease service not inited", KR(ret));
  } else if (!is_running_) {
    // do nothing
  } else if (OB_FAIL(MTL(ObSSWriterService*)->unregister_ls_lease_service(this))) {
    LOG_ERROR("unregister lease service error", KR(ret));
  } else {
    start_ts_ = -1;
    epoch_base_ = -1;
    sswriter_region_lease_.reset_lease();
    epoch_generator_.reset();
    is_running_ = false;
  }
  LOG_INFO("ObSSWriterLeaseService stop", K(ret), K(*this));
}

void ObSSWriterLeaseService::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter lease service not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sswriter lease service", KR(ret));
  } else {
    // do nothing
  }
  LOG_INFO("ObSSWriterLeaseService wait", K(ret), K_(ls_id));
}

void ObSSWriterLeaseService::destroy()
{
  const int is_inited = is_inited_;
  if (is_inited_) {
    stop();
    wait();
    epoch_base_ = -1;
    start_ts_ = -1;
    ls_handler_ = NULL;
    sswriter_region_lease_.reset_lease();
    epoch_generator_.reset();
    ls_id_.reset();
    is_inited_ = false;
  }
  LOG_INFO("ObSSWriterLeaseService destroy", K(is_inited), K_(ls_id));
}

int ObSSWriterLeaseService::do_work()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  const ObSSWriterServiceTenantType expected_tenant_type = MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()
      ? ObSSWriterServiceTenantType::PRIMARY : ObSSWriterServiceTenantType::RESTORE;
  if (-1 == start_ts_) {
    LOG_WARN("invalid start ts", K_(start_ts));
  } else if (start_ts_ + SSWRITER_DEFAULT_LEASE_US >= now) {
    // do nothing
  } else if (expected_tenant_type != ls_handler_->get_service_tenant_type()) {
    // only print syslog
    LOG_WARN("unexpected sswriter lease service", K(expected_tenant_type), K(*this));
  } else {
    if (!sswriter_region_lease_.is_inited()) {
      // refresh epoch base first
      if (!epoch_generator_.is_valid() && OB_FAIL(prepare_epoch_generator_())) {
        LOG_WARN("refresh epoch base failed", KR(ret), K(*this));
      } else {
        ObLocalityManager *locality_manager = GCTX.locality_manager_;
        const ObAddr &self_addr = GCTX.self_addr();
        common::ObRegion region;
        // TODO, get all regions
        if (OB_FAIL(locality_manager->get_server_region(self_addr, region))) {
          LOG_WARN("get server region failed", KR(ret), K(region));
        } else if (region.is_empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected region", KR(ret));
        } else if (OB_FAIL(sswriter_region_lease_.init(region,
                ls_id_, &epoch_generator_))) {
          LOG_WARN("sswriter region init failed", KR(ret));
        } else {
          LOG_INFO("sswriter region init success", K(region));
        }
      }
    }
    (void)sswriter_region_lease_.check_status();
  }
  return ret;
}

int ObSSWriterLeaseService::handle_lease_request(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLeaseService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObSSWriterLeaseService not runnig", KR(ret));
#ifdef ERRSIM
  } else if (MTL_ID() == GCONF.errsim_tenant_id
             && OB_FAIL(EVENT_CALL(EN_SSWRITER_LEASE_UNAVAILABLE, ls_id_.id()))) {
    LOG_INFO("pause sswriter lease request", KR(ret), K_(ls_id));
#endif
  } else {
    const ObAddr &sender = req.get_sender();
    ObLocalityManager *locality_manager = GCTX.locality_manager_;
    common::ObRegion region;
    if (OB_FAIL(locality_manager->get_server_region(sender, region))) {
      LOG_WARN("get server region failed", KR(ret), K(region));
    } else if (region.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected region", KR(ret));
    } else {
      //TODO, get sswriter region accorging to sender region
      if (region == sswriter_region_lease_.get_region()) {
        if (OB_FAIL(sswriter_region_lease_.handle_lease_request(req))) {
          LOG_WARN("handle lease request failed", KR(ret), K(req));
        }
      } else {
        // if no region in local, return not master
        ret = OB_NOT_MASTER;
      }
    }
    LOG_INFO("handle lease request", K(req), K(region));
  }
  return ret;
}

int ObSSWriterLeaseService::get_sswriter_addr(
    common::ObRegion &target_region,
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    int64_t &remaining_lease_us)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLeaseService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObSSWriterLeaseService not runnig", KR(ret));
  } else {
    //TODO, get sswriter region according to sender region
    if (target_region == sswriter_region_lease_.get_region()) {
      if (OB_FAIL(sswriter_region_lease_.get_sswriter_addr(target_region, key,
              sswriter_addr, remaining_lease_us))) {
        LOG_WARN("get sswriter addr failed", KR(ret), K(target_region), K(key));
      }
    } else {
      // if no region in local, return not master
      ret = OB_NOT_MASTER;
      LOG_WARN("there's no region in local", KR(ret), K(target_region), K(sswriter_region_lease_), K(key));
    }
  }
  return ret;
}

// make sure that the start_ts is valid
int ObSSWriterLeaseService::prepare_epoch_generator_()
{
  int ret = OB_SUCCESS;
  const MonotonicTs stc(start_ts_);
  MonotonicTs tmp_receive_gts_ts;
  SCN gts;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(MTL_ID());
  const uint64_t gts_tenant_id = meta_tenant_id;
  if (OB_FAIL(OB_TS_MGR.get_gts(gts_tenant_id, stc, NULL, gts, tmp_receive_gts_ts))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("get gts failed", KR(ret), K(stc), K(gts));
    }
  } else {
    epoch_base_ = gts.get_val_for_tx();
    epoch_generator_.set(epoch_base_);
  }
  // const int64_t now = ObTimeUtility::current_time();
  // epoch_generator_.set(now);
  LOG_INFO("try refresh epoch base", K(meta_tenant_id), K_(ls_id),
      K_(epoch_base), K_(epoch_generator));
  return ret;
}

int ObSSWriterLeaseService::iterate_sswriter_mgr_stat(
    ObSSWriterMgrStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_running_) {
    // ignore
    LOG_INFO("ObSSWriterLeaseService not runnig", KR(ret));
  } else if (OB_FAIL(sswriter_region_lease_.iterate_sswriter_mgr_stat(stat_iter))) {
    LOG_WARN("iterate sswriter mgr stat failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPrimarySSWriterLSHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (NULL == ls) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls));
  } else if (OB_FAIL(ObSSWriterLSHandler::init(ls,
          ObSSWriterServiceTenantType::PRIMARY))) {
    LOG_WARN("init failed", KR(ret));
  } else {
  }
  return ret;
}

int ObRestoreSSWriterLSHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (NULL == ls) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls));
  } else if (OB_FAIL(ObSSWriterLSHandler::init(ls,
          ObSSWriterServiceTenantType::RESTORE))) {
    LOG_WARN("init failed", KR(ret));
  } else {
  }
  return ret;
}

} // storage
} // oceanbase

