/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_service.h"
#include "lib/time/ob_time_utility.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_task.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/ob_locality_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
void ObSSWriterStatistics::reset()
{
  last_print_stat_ts_ = 0;
  active_sswriter_ctx_count_ = 0;
  check_lease_count_ = 0;
  check_lease_with_groupid_count_ = 0;
  grant_sswriter_request_count_ = 0;
  lease_request_count_ = 0;
  sswriter_loop_count_ = 0;
  sswriter_loop_used_time_us_ = 0;
  get_ctx_not_exist_count_ = 0;
  lease_async_request_count_ = 0;
  active_location_ctx_count_ = 0;
}

void ObSSWriterStatistics::inc_active_sswriter_ctx_count()
{
  ATOMIC_INC(&active_sswriter_ctx_count_);
}

void ObSSWriterStatistics::inc_check_lease_count()
{
  ATOMIC_INC(&check_lease_count_);
}

void ObSSWriterStatistics::inc_check_lease_with_groupid_count()
{
  ATOMIC_INC(&check_lease_with_groupid_count_);
}

void ObSSWriterStatistics::inc_grant_sswriter_request_count()
{
  ATOMIC_INC(&grant_sswriter_request_count_);
}

void ObSSWriterStatistics::inc_lease_request_count()
{
  ATOMIC_INC(&lease_request_count_);
}

void ObSSWriterStatistics::inc_sswriter_loop_count()
{
  ATOMIC_INC(&sswriter_loop_count_);
}

void ObSSWriterStatistics::add_sswriter_loop_used_time(const int64_t used_time_us)
{
  ATOMIC_FAA(&sswriter_loop_used_time_us_, used_time_us);
}

void ObSSWriterStatistics::inc_get_ctx_not_exist_count()
{
  ATOMIC_INC(&get_ctx_not_exist_count_);
}

void ObSSWriterStatistics::inc_lease_async_request_count()
{
  ATOMIC_INC(&lease_async_request_count_);
}

void ObSSWriterStatistics::inc_active_location_ctx_count()
{
  ATOMIC_INC(&active_location_ctx_count_);
}

void ObSSWriterStatistics::try_print()
{
  static const int64_t STAT_INTERVAL = 10 * 1000 * 1000;  // 10 seconds
  const int64_t now = ObTimeUtility::current_time();
  if (now - last_print_stat_ts_ > STAT_INTERVAL) {
    const int64_t check_lease_count = ATOMIC_LOAD(&check_lease_count_);
    const int64_t check_lease_with_groupid_count = ATOMIC_LOAD(&check_lease_with_groupid_count_);
    const int64_t grant_sswriter_request_count = ATOMIC_LOAD(&grant_sswriter_request_count_);
    const int64_t lease_request_count = ATOMIC_LOAD(&lease_request_count_);
    const int64_t sswriter_loop_count = ATOMIC_LOAD(&sswriter_loop_count_);
    const int64_t sswriter_loop_used_time_us = ATOMIC_LOAD(&sswriter_loop_used_time_us_);
    const int64_t get_ctx_not_exist_count = ATOMIC_LOAD(&get_ctx_not_exist_count_);
    const int64_t lease_async_request_count = ATOMIC_LOAD(&lease_async_request_count_);
    const int64_t active_location_ctx_count = ATOMIC_LOAD(&active_location_ctx_count_);

    FLOG_INFO("sswriter statistics",
        K(check_lease_count),
        K(check_lease_with_groupid_count),
        K(grant_sswriter_request_count),
        K(lease_request_count),
        K(sswriter_loop_count),
        K(sswriter_loop_used_time_us),
        K(get_ctx_not_exist_count),
        K(lease_async_request_count),
        K(active_location_ctx_count));

    ATOMIC_STORE(&check_lease_count_, 0);
    ATOMIC_STORE(&check_lease_with_groupid_count_, 0);
    ATOMIC_STORE(&grant_sswriter_request_count_, 0);
    ATOMIC_STORE(&lease_request_count_, 0);
    ATOMIC_STORE(&sswriter_loop_count_, 0);
    ATOMIC_STORE(&sswriter_loop_used_time_us_, 0);
    ATOMIC_STORE(&last_print_stat_ts_, now);
    ATOMIC_STORE(&get_ctx_not_exist_count_, 0);
    ATOMIC_STORE(&lease_async_request_count_, 0);
  }
}

int ObSSWriterService::mtl_init(ObSSWriterService *&sswriter_service)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObSSWriterService mtl init");
  return sswriter_service->init();
}

int ObSSWriterService::init()
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterService inited twice", KR(ret));
  } else if (OB_FAIL(sswriter_rpc_.init(net_frame->get_req_transport(), self))) {
    LOG_WARN("sswriter rpc init failed", KR(ret));
  } else if (OB_FAIL(ctx_mgr_.init())) {
    LOG_WARN("sswriter ctx mgr init failed", KR(ret));
  } else if (OB_FAIL(location_mgr_.init())) {
    LOG_WARN("sswriter location mgr init failed", KR(ret));
  } else if (OB_FAIL(worker_.init(&ctx_mgr_, &location_mgr_))) {
    LOG_WARN("sswriter worker init failed", KR(ret));
  } else if (OB_FAIL(ls_ckpt_executor_.init())) {
    LOG_WARN("ls ckpt executor init failed", KR(ret));
  } else if (FALSE_IT(worker_.set_ls_ckpt_executor(&ls_ckpt_executor_))) {
  } else {
    // prepare for meta sswriter location
    int tmp_ret = OB_SUCCESS;
    const ObSSWriterKey meta_sswriter_key(ObSSWriterType::META, SYS_LS);
    ObSSWriterLocationCtx *location_ctx = NULL;
    if (OB_SUCCESS != (tmp_ret = location_mgr_.acquire_sswriter_location_ctx(
            meta_sswriter_key, location_ctx))) {
      LOG_WARN("acquire sswriter location ctx failed", K(tmp_ret), K(meta_sswriter_key));
    } else {
      LOG_INFO("prepare meta sswriter location success", K(meta_sswriter_key));
    }
    if (NULL != location_ctx) {
      if (OB_SUCCESS != (tmp_ret = location_mgr_.revert_sswriter_location_ctx(location_ctx))) {
        LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret), K(meta_sswriter_key));
      }
    }
    is_inited_ = true;
  }
  return ret;
}

int ObSSWriterService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter service not inited", KR(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service is already running", KR(ret));
  } else if (OB_FAIL(sswriter_rpc_.start())) {
    LOG_WARN("sswriter rpc start failed", KR(ret));
  } else if (OB_FAIL(worker_.start())) {
    LOG_WARN("sswriter worker start failed", KR(ret));
  } else {
    is_running_ = true;
  }
  LOG_INFO("ObSSWriterService start", KR(ret));
  return ret;
}

void ObSSWriterService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter service not inited", KR(ret));
  } else {
    worker_.stop();
    stop_ls_ckpt_dag_();
    sswriter_rpc_.stop();
    is_running_ = false;
  }
  LOG_INFO("ObSSWriterService stop", KR(ret));
}

void ObSSWriterService::stop_ls_ckpt_dag_()
{
  int tmp_ret = OB_SUCCESS;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  if (OB_NOT_NULL(scheduler)) {
    ObSSLsCkptDag dag;
    // cancel_dag() looks up the DAG by identity, so no pointer to the running DAG is needed.
    // The identity is DAG_TYPE_SS_LS_CKPT alone, without mode or epoch;
    // valid NORMAL parameters suffice.
    ObSSLsCkptDagParam param(ObSSLsCkptMode::NORMAL, 0 /*sswriter_epoch*/, SYS_LS);
    if (OB_TMP_FAIL(dag.init_by_param(&param))) {
      LOG_WARN_RET(OB_SUCCESS, "init ls ckpt dag failed", KR(tmp_ret));
    } else if (OB_TMP_FAIL(scheduler->cancel_dag(&dag, true/*force_cancel*/))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN_RET(OB_SUCCESS, "cancel ls ckpt dag failed", KR(tmp_ret));
      }
    }
  }
}

void ObSSWriterService::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter service not inited", KR(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service is already running", KR(ret));
  } else {
    worker_.wait();
    sswriter_rpc_.wait();
  }
  LOG_INFO("ObSSWriterService wait", KR(ret));
}

void ObSSWriterService::destroy()
{
  const int is_inited = is_inited_;
  if (is_inited_) {
    stop();
    wait();
    ls_ckpt_executor_.destroy();
    location_mgr_.destroy();
    ctx_mgr_.destroy();
    worker_.destroy();
    sswriter_rpc_.destroy();
    DLIST_FOREACH_REMOVESAFE_NORET(cur, cbs_) {
      if (NULL != cur) {
        (void)cbs_.remove(cur);
      }
    }
    is_inited_ = false;
  }
  LOG_INFO("ObSSWriterService destroy", K(is_inited));
}

int ObSSWriterService::do_ls_checkpoint(const ObSSLsCkptMode mode)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter service not inited", KR(ret));
  } else if (OB_FAIL(ls_ckpt_executor_.do_checkpoint(mode))) {
    LOG_WARN("do ls checkpoint failed", KR(ret), K(mode));
  }
  return ret;
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  is_sswriter = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // const ObSSWriterKey sswriter_ctx_key(key.get_type(), key.get_ls_id());
    const ObLsSSWriterKey ls_sswriter_ctx_key(ObSSWriterType::FREEZE, key.get_mgr_ls_id());
    ObLsSSWriterCtx *ls_sswriter_ctx = NULL;
    if (OB_FAIL(ctx_mgr_.get_ls_sswriter_ctx(ls_sswriter_ctx_key, ls_sswriter_ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // rewrite ret to success, and set is_sswriter to false
        ret = OB_SUCCESS;
        is_sswriter = false;
      } else {
        LOG_WARN("get sswriter ctx failed", KR(ret), K(key), K(ls_sswriter_ctx_key));
      }
    } else if (NULL == ls_sswriter_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected sswriter ctx", KR(ret), K(key), K(ls_sswriter_ctx_key));
    } else {
      bool need_plain_check_lease = false;
      if (-1 != epoch) {
        // for performance
        if (ls_sswriter_ctx->is_lease_valid_with_epoch(epoch)) {
          is_sswriter = true;
          statistics_.inc_check_lease_count();
        } else {
          need_plain_check_lease = true;
        }
      } else {
        need_plain_check_lease = true;
      }
      if (need_plain_check_lease) {
        if (OB_FAIL(ls_sswriter_ctx->check_lease(key.get_tablet_id(), is_sswriter, epoch))) {
          LOG_WARN("check lease failed", KR(ret), K(key));
        } else {
          // do nothing
        }
      }
      if (OB_SUCCESS != (tmp_ret = ctx_mgr_.revert_ls_sswriter_ctx(ls_sswriter_ctx))) {
        LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret));
      }
    }
  }
  // for statistics
  statistics_.inc_check_lease_count();
  return ret;
}

int ObSSWriterService::check_lease_for_atomic_protocol(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  int ret = OB_SUCCESS;
  if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
    ret = check_lease_by_palf_role(key, is_sswriter, epoch);
  } else {
    ret = check_lease(key, is_sswriter, epoch);
  }
  return ret;
}

int ObSSWriterService::check_lease_by_palf_role(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  int ret = OB_SUCCESS;
  is_sswriter = false;
  epoch = 0;
  ObRole role = FOLLOWER;
  int64_t proposal_id = 0;
  ObRole unused_curr_role = FOLLOWER;
  int64_t unused_curr_proposal_id = 0;
  bool is_pending_state = false;
  ObLSHandle ls_handle;
  // Takeover waits for replay, so replay must not wait for an SSWriter lease granted after takeover.
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_map()->get_ls(
      key.get_mgr_ls_id(), ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      // If the LS is not hosted locally, report a non-writer so the atomic protocol rejects the operation.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get ls failed", KR(ret), K(key));
    }
  } else if (OB_FAIL(ls_handle.get_ls()->get_log_handler()->prepare_switch_role(
      unused_curr_role, unused_curr_proposal_id, role, proposal_id, is_pending_state))) {
    if (OB_NOT_INIT == ret || OB_NOT_RUNNING == ret) {
      // The LS may be in the map before its logs are loaded, or it may already be stopped.
      // Neither state qualifies as a writer.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get palf role failed", KR(ret), K(key));
    }
  } else if (LEADER == role && !is_pending_state) {
    is_sswriter = true;
    epoch = proposal_id;
  }
  // for statistics
  statistics_.inc_check_lease_count();
  return ret;
}

void ObSSWriterService::try_print_statistics()
{
  ctx_mgr_.try_print_statistics();
  location_mgr_.try_print_statistics();
  statistics_.try_print();
}

int ObSSWriterService::grant_sswriter(const ObSSWriterGrantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    ObSSWriterGrantRequest request;
    if (OB_FAIL(request.init(arg.group_id_, arg.ls_id_, arg.type_, arg.epoch_,
            GCTX.self_addr()))) {
      LOG_WARN("request init failed", KR(ret), K(arg));
    } else if (OB_FAIL(sswriter_rpc_.grant_sswriter(arg.target_addr_, request))) {
      LOG_WARN("grant sswriter failed", KR(ret), K(arg));
    } else {
      LOG_INFO("grant sswriter success", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObSSWriterService::request_sswriter_lease(const ObSSWriterLeaseRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else if (OB_FAIL(request_sswriter_lease_(arg))) {
    LOG_WARN("request sswriter lease failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterService::request_sswriter_lease_async(
    const ObSSWriterLeaseRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    ObSSWriterLeaseRequest request;
    if (OB_FAIL(request.init(arg))) {
      LOG_WARN("request init failed", KR(ret), K(arg));
    } else if (OB_FAIL(sswriter_rpc_.request_lease_async(arg.target_addr_, request))) {
      LOG_WARN("request lease failed", KR(ret), K(arg), K(request));
    }
  }
  return ret;
}

int ObSSWriterService::respond_sswriter_lease_async(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid_for_async()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(req));
  } else {
    ObSSWriterLeaseResponse response;
    if (OB_FAIL(response.init(req))) {
      LOG_WARN("response init failed", KR(ret), K(req));
    } else if (OB_FAIL(sswriter_rpc_.respond_lease_async(req.get_sender(), response))) {
      LOG_WARN("respond lease failed", KR(ret), K(req), K(response));
    }
  }
  return ret;
}

int ObSSWriterService::request_sswriter_lease_(const ObSSWriterLeaseRequestArg &arg)
{
  int ret = OB_SUCCESS;
  ObSSWriterLeaseRequest request;
  if (OB_FAIL(request.init(arg))) {
    LOG_WARN("request init failed", KR(ret), K(arg));
  } else if (OB_FAIL(sswriter_rpc_.request_lease_sync(arg.target_addr_, request))) {
    LOG_WARN("request lease failed", KR(ret), K(arg), K(request));
  }
  return ret;
}

int ObSSWriterService::handle_sswriter_grant_req(
    const ObSSWriterGrantRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(req));
  } else {
    ObLsSSWriterCtx *ls_sswriter_ctx = NULL;
    const int64_t group_id = req.get_group_id();
    const int64_t epoch = req.get_epoch();
    const ObSSWriterType type = req.get_sswriter_type();
    const ObLSID ls_id = req.get_ls_id();
    const ObAddr &sender = req.get_sender();
    // NOTE that the key uses type and ls_id only
    const ObLsSSWriterKey key(type, ls_id);
    ObLsSSWriterCtxCreateArg arg;
    arg.grant_server_ = sender;
    if (OB_FAIL(ctx_mgr_.acquire_ls_sswriter_ctx(arg, key, ls_sswriter_ctx))) {
      LOG_WARN("get sswriter ctx failed", KR(ret), K(key));
    } else if (NULL == ls_sswriter_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected sswriter ctx", KR(ret), K(key));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ls_sswriter_ctx->add_group(group_id, epoch, sender))) {
        // NOTE that if init_with_group is called, add_group should not be called
        if (OB_EAGAIN == ret) {
          LOG_INFO("need wait", KR(ret), K(key), K(group_id));
        } else if (OB_ENTRY_EXIST == ret) {
          // group exist
          LOG_INFO("group exist", KR(ret), K(key), K(group_id));
        } else {
          LOG_WARN("add sswriter group failed", KR(ret), K(group_id), K(epoch));
        }
      } else {
        int64_t now = ObTimeUtility::current_time();
        ObSSWriterLeaseRequestArg arg;
        arg.group_id_ = group_id;
        arg.ls_id_ = ls_id;
        arg.type_ = type;
        arg.epoch_ = epoch;
        arg.lease_us_ = SSWRITER_DEFAULT_LEASE_US;
        arg.target_addr_ = sender;
        if (OB_FAIL(request_sswriter_lease_(arg))) {
          LOG_WARN("request sswriter lease failed", KR(ret), K(arg));
        } else if (OB_FAIL(ls_sswriter_ctx->update_lease(group_id,
                now + SSWRITER_DEFAULT_LEASE_US, epoch))) {
          LOG_WARN("update sswriter lease failed", KR(ret), K(arg));
        } else {
          // do nothing
        }
        if (OB_SUCC(ret) && ObSSWriterType::FREEZE == type) {
          // do callback tasks if freeze type
          ObSpinLockGuard guard(cbs_lock_);
          DLIST_FOREACH_X(cur, cbs_, OB_SUCC(ret)) {
            if (cur->get_ls_id() == ls_id) {
              if (OB_FAIL(cur->on_sswriter_up(ls_id, epoch, true))) {
                LOG_WARN("on sswriter up failed", KR(ret), K(req));
              }
            }
          }
        }
        // if request lease fail, remove group
        if (OB_FAIL(ret)) {
          if (OB_SUCCESS != (tmp_ret = ls_sswriter_ctx->remove_group(group_id))) {
            LOG_WARN("remove group failed", KR(ret), K(group_id));
          }
        }
      }
      LOG_INFO("handle sswriter grant request", K(key));
      if (OB_SUCCESS != (tmp_ret = ctx_mgr_.revert_ls_sswriter_ctx(ls_sswriter_ctx))) {
        LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret));
      }
    }
  }
  // for statistics
  statistics_.inc_grant_sswriter_request_count();
  return ret;
}

int ObSSWriterService::register_callback(const share::ObLSID &ls_id, ObISSWriterCb *cb)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cbs_lock_);
  cb->set_ls_id(ls_id);
  if (!cbs_.add_last(cb)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObTabletID tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID); // fake
    ObSSWriterKey key(ObSSWriterType::FREEZE, ls_id, tablet_id);
    bool is_sswriter;
    int64_t epoch;
    if (OB_FAIL(check_lease(key, is_sswriter, epoch))) {
      LOG_WARN("check lease fail", K(ret));
    } else if (is_sswriter) {
      if (OB_FAIL(cb->on_sswriter_up(ls_id, epoch, true))) {
        LOG_WARN("callback fail", K(ret), K(ls_id));
      }
    }
  }

  return ret;
}

int ObSSWriterService::unregister_callback(const share::ObLSID &ls_id, ObISSWriterCb *cb)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cbs_lock_);
  DLIST_FOREACH(cur, cbs_) {
    if (cur == cb) {
      cbs_.remove(cur);
    }
  }
  return ret;
}

int ObSSWriterService::handle_sswriter_lease_req(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(req));
  } else {
    ObLSService *ls_svr =  MTL(ObLSService *);
    ObLSHandle handle;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log stream service", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls(req.get_ls_id(),
                                      handle,
                                      ObLSGetMod::TRANS_MOD))) {
      LOG_WARN("get log stream failed", KR(ret), K(req));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log stream", KR(ret), K(req));
    } else {
      if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
        // for primary
        if (OB_FAIL(ls->get_primary_sswriter_ls_handler().handle_lease_request(req))) {
          LOG_WARN("handle lease request failed", KR(ret), K(req));
        }
      } else {
        // for standby
        if (OB_FAIL(ls->get_restore_sswriter_ls_handler().handle_lease_request(req))) {
          LOG_WARN("handle lease request failed", KR(ret), K(req));
        }
      }
    }
    LOG_INFO("handle sswriter lease request", K(ret), K(req));
  }
  // for statistics
  statistics_.inc_lease_request_count();
  return ret;
}

int ObSSWriterService::handle_sswriter_lease_async_req(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid_for_async()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(req));
  } else {
    ObLSService *ls_svr =  MTL(ObLSService *);
    ObLSHandle handle;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log stream service", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls(req.get_ls_id(),
                                      handle,
                                      ObLSGetMod::TRANS_MOD))) {
      LOG_WARN("get log stream failed", KR(ret), K(req));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log stream", KR(ret), K(req));
    } else {
      if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
        if (OB_FAIL(ls->get_primary_sswriter_ls_handler().handle_lease_request(req))) {
          LOG_WARN("handle lease request failed", KR(ret), K(req));
        }
      } else {
        if (OB_FAIL(ls->get_restore_sswriter_ls_handler().handle_lease_request(req))) {
          LOG_WARN("handle lease request failed", KR(ret), K(req));
        }
      }
      if (OB_SUCC(ret)) {
        // if success, respond
        if (OB_FAIL(respond_sswriter_lease_async(req))) {
          LOG_WARN("respond async failed", KR(ret), K(req));
        }
      }
    }
    LOG_INFO("handle sswriter lease async request", K(ret), K(req));
  }
  // for statistics
  statistics_.inc_lease_async_request_count();
  return ret;
}

int ObSSWriterService::handle_sswriter_lease_async_resp(
    const ObSSWriterLeaseResponse &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(resp));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObLsSSWriterCtx *ls_sswriter_ctx = NULL;
    const int64_t group_id = resp.get_group_id();
    const int64_t epoch = resp.get_epoch();
    const ObSSWriterType type = resp.get_sswriter_type();
    const ObLSID ls_id = resp.get_ls_id();
    const ObAddr &sender = resp.get_sender();
    const int64_t base_ts = resp.get_sswriter_base_ts();
    // NOTE that the key uses type and ls_id only
    const ObLsSSWriterKey key(type, ls_id);
    if (OB_FAIL(ctx_mgr_.get_ls_sswriter_ctx(key, ls_sswriter_ctx))) {
      LOG_WARN("get sswriter ctx failed", KR(ret), K(key));
    } else if (NULL == ls_sswriter_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected sswriter ctx", KR(ret), K(key));
    } else if (OB_FAIL(ls_sswriter_ctx->update_lease(group_id,
            base_ts + SSWRITER_DEFAULT_LEASE_US, epoch))) {
      LOG_WARN("update sswriter lease failed", KR(ret), K(resp));
    } else {
      // do nothing
    }
    if (NULL != ls_sswriter_ctx) {
      if (OB_SUCCESS != (tmp_ret = ctx_mgr_.revert_ls_sswriter_ctx(ls_sswriter_ctx))) {
        LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret));
      }
    }
    LOG_INFO("handle sswriter lease async response", KR(ret), K(resp));
  }
  return ret;
}

int ObSSWriterService::handle_sswriter_location_req(
    const ObSSWriterLocationRequest &req,
    const bool is_local_request)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(req));
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
      ObSSWriterKey key = req.get_sswriter_key();
      ObLSID mgr_ls_id = key.get_mgr_ls_id();
      ObLSService *ls_svr = MTL(ObLSService *);
      ObLSHandle handle;
      ObLS *ls = nullptr;
      // results
      ObSSWriterAddr sswriter_addr;
      int64_t remaining_lease_us = -1;
      if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected log stream service", KR(ret));
      } else if (OB_FAIL(ls_svr->get_ls(mgr_ls_id,
                                        handle,
                                        ObLSGetMod::TRANS_MOD))) {
        LOG_WARN("get log stream failed", KR(ret), K(req));
      } else if (OB_ISNULL(ls = handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected log stream", KR(ret), K(req));
      } else {
        if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
          if (OB_FAIL(ls->get_primary_sswriter_ls_handler().get_sswriter_addr(region, key,
                  sswriter_addr, remaining_lease_us))) {
            LOG_WARN("get sswriter addr failed", KR(ret), K(req));
          }
        } else {
          if (OB_FAIL(ls->get_restore_sswriter_ls_handler().get_sswriter_addr(region, key,
                  sswriter_addr, remaining_lease_us))) {
            LOG_WARN("get sswriter addr failed", KR(ret), K(req));
          }
        }
        if (OB_SUCC(ret)) {
          if (is_local_request) {
            // local request, update location ctx directly
            if (OB_FAIL(update_sswriter_location_(key, sswriter_addr.addr_,
                    sswriter_addr.epoch_, req.get_request_start_ts() + remaining_lease_us))) {
              LOG_WARN("update sswriter location failed", KR(ret), K(sswriter_addr),
                  K(remaining_lease_us));
            }
          } else {
            // if success, respond
            int tmp_ret = OB_SUCCESS;
            ObSSWriterLocationResponseArg arg;
            arg.sswriter_addr_ = sswriter_addr.addr_;
            arg.epoch_ = sswriter_addr.epoch_;
            arg.remaining_lease_us_ = remaining_lease_us;
            if (OB_SUCCESS != (tmp_ret = respond_sswriter_location(req, arg))) {
              LOG_WARN("respond sswriter location failed", KR(ret), K(req), K(arg));
            }
          }
        } else {
          // if fail, do not respond
        }
      }
    }
    LOG_INFO("handle sswriter location request", K(ret), K(req));
  }
  return ret;
}

int ObSSWriterService::handle_sswriter_location_resp(
    const ObSSWriterLocationResponse &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(resp));
  } else if (OB_FAIL(update_sswriter_location_(resp.get_sswriter_key(),
          resp.get_sswriter_addr(), resp.get_epoch(), resp.get_lease_expire_ts()))) {
    LOG_WARN("update sswriter location failed", KR(ret), K(resp));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterService::register_ls_lease_service(
    ObSSWriterLeaseService *lease_service)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (OB_FAIL(worker_.register_ls_lease_service(lease_service))) {
    LOG_WARN("register ls lease service failed", KR(ret));
  } else {
    // LOG_INFO("register ls lease service success");
  }
  return ret;
}

int ObSSWriterService::unregister_ls_lease_service(
    ObSSWriterLeaseService *lease_service)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (OB_FAIL(worker_.unregister_ls_lease_service(lease_service))) {
    LOG_WARN("unregister ls lease service failed", KR(ret));
  } else {
    LOG_INFO("unregister ls lease service success");
  }
  return ret;
}

// Currently, if timeout, return invalid sswriter addr
// TODO, return error code if timeout regardless of input parameter timeout_us
int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const static int64_t MAX_TIMEOUT_US = 120000000;  // 120 senconds
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!key.is_valid() || 0 > timeout_us || MAX_TIMEOUT_US < timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key), K(timeout_us));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObSSWriterLocationCtx *location_ctx = NULL;
    if (OB_FAIL(location_mgr_.get_sswriter_location_ctx(key, location_ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(location_mgr_.acquire_sswriter_location_ctx(key, location_ctx))) {
          LOG_WARN("acquire sswriter location ctx failed", KR(ret), K(key));
        } else {
          // do nothing
        }
      } else {
        LOG_WARN("get sswriter location ctx failed", KR(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (NULL == location_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ctx", KR(ret), K(key));
    } else if (OB_FAIL(location_ctx->try_renew_location())) {
      LOG_WARN("try refresh sswriter location failed", KR(ret), K(key));
    } else {
      const int64_t expire_ts = ObClockGenerator::getClock() + timeout_us;
      bool need_retry = false;
      do {
        need_retry = false;
        if (OB_FAIL(location_ctx->get_addr(sswriter_addr))) {
          LOG_WARN("get sswriter addr failed", KR(ret), K(key));
        } else {
          if (-1 == sswriter_addr.epoch_
              && ObClockGenerator::getClock() < expire_ts) {
            need_retry = true;
            if (TC_REACH_TIME_INTERVAL(1000000)) {
              LOG_INFO("get sswriter addr need retry", K(key), K(timeout_us),
                  K(expire_ts), K(ObClockGenerator::getClock()));
            }
            ob_usleep(1000);  // sleep 1000 us
          }
        }
      } while (true == need_retry);
      LOG_INFO("get sswriter addr", K(ret), K(key), K(sswriter_addr), K(timeout_us));
    }
    if (NULL != location_ctx) {
      if (OB_SUCCESS != (tmp_ret = location_mgr_.revert_sswriter_location_ctx(location_ctx))) {
        LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret), K(key));
      }
    }
  }
  return ret;
}

int ObSSWriterService::update_sswriter_location_(
    const ObSSWriterKey &key,
    const common::ObAddr &addr,
    const int64_t epoch,
    const int64_t lease_expire_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSWriterLocationCtx *location_ctx = NULL;
  if (OB_FAIL(location_mgr_.get_sswriter_location_ctx(key, location_ctx))) {
    LOG_WARN("get sswriter location ctx failed", KR(ret), K(key));
  } else if (NULL == location_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ctx", KR(ret), K(key));
  } else if (OB_FAIL(location_ctx->update_addr(addr, epoch, lease_expire_ts))) {
    LOG_WARN("update sswriter addr failed", KR(ret), K(key), K(addr),
        K(epoch), K(lease_expire_ts));
  } else {
    LOG_INFO("update sswriter addr success", K(key), K(addr), K(epoch), K(lease_expire_ts));
  }
  if (NULL != location_ctx) {
    if (OB_SUCCESS != (tmp_ret = location_mgr_.revert_sswriter_location_ctx(location_ctx))) {
      LOG_ERROR("revert ls sswriter ctx error", KR(tmp_ret), K(key));
    }
  }
  return ret;
}

int ObSSWriterService::request_sswriter_location(
    const ObSSWriterLocationRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    int tmp_ret = OB_SUCCESS;
    common::ObAddr mgr_addr;
    ObLocationService *location_service = GCTX.location_service_;
    const ObLSID mgr_ls_id = arg.sswriter_key_.get_mgr_ls_id();
    const int64_t cluster_id = GCONF.cluster_id;
    if (NULL == location_service) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected location service", KR(ret));
    } else if (OB_SUCCESS != (tmp_ret = location_service->nonblock_get_leader(cluster_id,
            MTL_ID(), mgr_ls_id, mgr_addr))) {
      LOG_DEBUG("nonblock get leader from locatition cache failed", KR(tmp_ret), K(arg));
      if (OB_SUCCESS != (tmp_ret = location_service->nonblock_renew(cluster_id,
              MTL_ID(), mgr_ls_id))) {
        LOG_WARN("nonblock renew from location cache failed", KR(tmp_ret), K(arg));
      }
    } else {
      ObSSWriterLocationRequest request;
      if (OB_FAIL(request.init(arg))) {
        LOG_WARN("init request failed", KR(ret), K(arg));
      } else if (GCTX.self_addr() == mgr_addr) {
        // local logic
        const bool is_local_request = true;
        if (OB_FAIL(handle_sswriter_location_req(request, is_local_request))) {
          LOG_WARN("handle local sswriter location request failed", KR(ret),
              K(mgr_ls_id), K(request));
        }
      } else {
        // remote logic
        if (OB_FAIL(sswriter_rpc_.request_location(mgr_addr, request))) {
          LOG_WARN("request sswriter location failed", KR(ret), K(request));
        } else {
          LOG_INFO("request sswriter location success", K(mgr_addr), K(mgr_ls_id), K(request));
        }
      }
    }
  }
  return ret;
}

int ObSSWriterService::respond_sswriter_location(
    const ObSSWriterLocationRequest &req,
    const ObSSWriterLocationResponseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (!req.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(req), K(arg));
  } else {
    ObSSWriterLocationResponse response;
    if (OB_FAIL(response.init(req, arg))) {
      LOG_WARN("init response failed", KR(ret), K(req), K(arg));
    } else if (OB_FAIL(sswriter_rpc_.respond_location(req.get_sender(), response))) {
      LOG_WARN("respond location failed", KR(ret), K(response));
    } else {
      // LOG_INFO("respond location success");
    }
  }
  return ret;
}

int ObSSWriterService::iterate_sswriter_group_stat(
    ObSSWriterGroupStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else if (OB_FAIL(ctx_mgr_.iterate_sswriter_group_stat(stat_iter))) {
    LOG_WARN("iterate sswrriter group stat failed", KR(ret));
  } else {
    LOG_INFO("iterate sswrriter group stat success");
  }
  return ret;
}

int ObSSWriterService::iterate_sswriter_mgr_stat(
    ObSSWriterMgrStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterService not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter service is not running", KR(ret));
  } else {
    ObLSIterator *ls_iter = NULL;
    common::ObSharedGuard<ObLSIterator> ls_iterator_guard;
    ObLSService *ls_svr = MTL(ObLSService*);
    if (OB_ISNULL(ls_svr)) {
      // ignore ret
      SERVER_LOG(WARN, "ls service is null");
    } else if (OB_FAIL(ls_svr->get_ls_iter(ls_iterator_guard, ObLSGetMod::TRANS_MOD))) {
      SERVER_LOG(WARN, "get ls iterator failed", K(ret));
    } else if (OB_ISNULL(ls_iter = ls_iterator_guard.get_ptr())) {
      // ignore ret
      SERVER_LOG(WARN, "ls iterator is null", K(ret), K(ls_iter));
    } else {
      while (OB_SUCC(ret)) {
        ObLS *ls = NULL;
        if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "iterate ls get next failed", K(ret));
          } else {
            SERVER_LOG(INFO, "iterate to end", K(ret));
          }
        } else if (OB_ISNULL(ls)) {
          // ignore ret
          SERVER_LOG(WARN, "ls is NULL", K(ret), K(ls));
        } else {
          if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
            if (OB_FAIL(ls->get_primary_sswriter_ls_handler().iterate_sswriter_mgr_stat(stat_iter))) {
              LOG_WARN("handle lease request failed", KR(ret));
            }
          } else {
            if (OB_FAIL(ls->get_restore_sswriter_ls_handler().iterate_sswriter_mgr_stat(stat_iter))) {
              LOG_WARN("handle lease request failed", KR(ret));
            }
          }
        }
      } // end while
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    LOG_INFO("iterate sswrriter lease mgr stat", K(ret));
  }
  return ret;
}

void ObSSWriterWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    ctx_mgr_ = NULL;
    TG_DESTROY(tg_id_);
    if (0 != ATOMIC_LOAD(&lease_service_count_)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected lease service count", K_(lease_service_count));
    }
    lease_service_count_ = 0;
    location_mgr_ = NULL;
    is_inited_ = false;
  }
}

int ObSSWriterWorker::init(
    ObLsSSWriterCtxMgr *ctx_mgr,
    ObSSWriterLocationMgr *location_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterWorker inited twice", KR(ret));
  } else if (NULL == ctx_mgr || NULL == location_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ctx_mgr), KP(location_mgr));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSWriterWorker, tg_id_))) {
    LOG_ERROR("create tg failed", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_ERROR("set runnable failed", K(ret));
  } else {
    ctx_mgr_ = ctx_mgr;
    location_mgr_ = location_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObSSWriterWorker::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter worker not inited", KR(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter worker is already running", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("tg start failed", K(ret));
  } else {
    is_running_ = true;
  }
  return ret;
}

void ObSSWriterWorker::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter worker not inited", KR(ret));
  } else {
    TG_STOP(tg_id_);
    is_running_ = false;
  }
}

void ObSSWriterWorker::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter worker not inited", KR(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter worker is already running", KR(ret));
  } else {
    TG_WAIT(tg_id_);
  }
}

void ObSSWriterWorker::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("SSWWorker");
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    // PHASE ONE:
    // manage sswriter in ls leader
    const int64_t lease_service_count = ATOMIC_LOAD(&lease_service_count_);
    if (0 < lease_service_count) {
      ObSpinLockGuard guard(lease_service_lock_);
      DLIST_FOREACH(cur, lease_service_list_) {
        if (NULL != cur) {
          if (OB_FAIL(cur->do_work())) {
            LOG_WARN("lease service do work failed", KR(ret));
          }
        }
      }
    }
    // PHASE TWO:
    // check and renew lease in sswriter
    // MTL(ObSSWriterService*)->try_print();
    const int64_t start = ObTimeUtility::current_time();
    if (NULL != ctx_mgr_) {
      if (OB_FAIL(ctx_mgr_->try_renew_lease())) {
        LOG_WARN("try renew lease failed", KR(ret));
      }
      // ctx_mgr_->try_print_statistics();
    }
    // PHASE THREE:
    // check and renew sswriter location
    if (NULL != location_mgr_) {
      if (OB_FAIL(location_mgr_->try_renew_location())) {
        LOG_WARN("try renew location failed", KR(ret));
      }
    }
    // PHASE FOUR:
    // print statistics
    MTL(ObSSWriterService*)->try_print_statistics();
    // PHASE FIVE:
    // do ls checkpoint for sys tenant
    try_do_ls_checkpoint_();
    // for test
    if (TC_REACH_TIME_INTERVAL(1000000)) {
      test_();
      LOG_INFO("ObSSWriterWorker run", K_(tg_id));
    }
    const int64_t end = ObTimeUtility::current_time();
    MTL(ObSSWriterService*)->get_statistics().inc_sswriter_loop_count();
    MTL(ObSSWriterService*)->get_statistics().add_sswriter_loop_used_time(end - start);
    ob_usleep(200000);
  }
}

void ObSSWriterWorker::try_do_ls_checkpoint_()
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  if (OB_ISNULL(ls_ckpt_executor_)) {
  } else if (!GCTX.is_shared_storage_mode()) {
  } else if (!is_sys_tenant(MTL_ID())) {
  } else if (GCTX.in_bootstrap_) {
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t retry_interval_us = ObSSLsCkptExecutor::LS_CKPT_SCHEDULE_RETRY_INTERVAL_US;
    if (now < next_ls_ckpt_time_us_) {
    } else if (OB_FAIL(check_ss_ls_ckpt_enabled(MTL_ID(), is_enabled))) {
      next_ls_ckpt_time_us_ = now + retry_interval_us;
      LOG_WARN("failed to check ls ckpt enabled", KR(ret), K(MTL_ID()), K_(next_ls_ckpt_time_us));
    } else if (!is_enabled) {
      // The gate is dynamic during upgrade. Throttle rechecks while disabled as well.
      next_ls_ckpt_time_us_ = now + retry_interval_us;
    } else {
      int64_t ls_ckpt_timeout_us = ObSSLsCkptExecutor::LS_CKPT_INTERVAL_US;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSMetaInfoUtil::get_ls_ckpt_timeout_us(MTL_ID(), ls_ckpt_timeout_us))) {
        LOG_WARN("failed to get ls checkpoint timeout", KR(tmp_ret), K(MTL_ID()));
      }
      // schedule_checkpoint_dag内部会有sswriter检查机制，如果lease过期，则快速失败
      bool is_scheduled = false;
      if (OB_FAIL(ls_ckpt_executor_->schedule_checkpoint_dag(
              ObSSLsCkptMode::NORMAL, is_scheduled))) {
        next_ls_ckpt_time_us_ = now + retry_interval_us;
        LOG_WARN("schedule ls ckpt dag failed", KR(ret));
      } else if (is_scheduled) {
        next_ls_ckpt_time_us_ = now + ls_ckpt_timeout_us;
      } else {
        // lease 切换窗口内没有实际调度时短间隔重试，避免首次 checkpoint 延迟一个完整周期。
        next_ls_ckpt_time_us_ = now + retry_interval_us;
      }
    }
  }
}

int ObSSWriterWorker::register_ls_lease_service(
    ObSSWriterLeaseService *lease_service)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterWorker not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter worker is not running", KR(ret));
  } else if (NULL == lease_service) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(lease_service));
  } else {
    ObSpinLockGuard guard(lease_service_lock_);
    if (!lease_service_list_.add_last(lease_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add last failed", KR(ret));
    } else {
      ATOMIC_INC(&lease_service_count_);
      LOG_INFO("register ls lease service success", K_(lease_service_count));
    }
  }
  return ret;
}

int ObSSWriterWorker::unregister_ls_lease_service(
    ObSSWriterLeaseService *lease_service)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterWorker not inited", KR(ret));
  } else if (NULL == lease_service) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(lease_service));
  } else {
    ObSpinLockGuard guard(lease_service_lock_);
    DLIST_FOREACH_REMOVESAFE_NORET(cur, lease_service_list_) {
      if (cur == lease_service) {
        (void)lease_service_list_.remove(cur);
        ATOMIC_DEC(&lease_service_count_);
        LOG_INFO("unregister ls lease service success", K_(lease_service_count));
        break;
      }
    }
  }
  return ret;
}

void ObSSWriterWorker::test_()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id(1);
  common::ObTabletID tablet_id(500001);
  ObSSWriterType freeze_type = ObSSWriterType::FREEZE;
  ObSSWriterType compaction_type = ObSSWriterType::COMPACTION;
  ObSSWriterType meta_type = ObSSWriterType::META;
  ObSSWriterKey sswriter_key_1(freeze_type, ls_id, tablet_id);
  ObSSWriterKey sswriter_key_2(compaction_type, ls_id);
  ObSSWriterKey sswriter_key_3(meta_type, ls_id);
  bool is_sswriter = false;
  int64_t epoch = -1;
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(sswriter_key_1, is_sswriter, epoch))) {
    LOG_WARN("check lease failed", KR(ret), K(sswriter_key_1), K(is_sswriter), K(epoch));
  } else {
    LOG_INFO("check lease", KR(ret), K(sswriter_key_1), K(is_sswriter), K(epoch));
  }
  is_sswriter = false;
  epoch = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(sswriter_key_2, is_sswriter, epoch))) {
    LOG_WARN("check lease failed", KR(ret), K(sswriter_key_2), K(is_sswriter), K(epoch));
  } else {
    LOG_INFO("check lease", KR(ret), K(sswriter_key_2), K(is_sswriter), K(epoch));
  }
  is_sswriter = false;
  epoch = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(sswriter_key_3, is_sswriter, epoch))) {
    LOG_WARN("check lease failed", KR(ret), K(sswriter_key_3), K(is_sswriter), K(epoch));
  } else {
    LOG_INFO("check lease", KR(ret), K(sswriter_key_3), K(is_sswriter), K(epoch));
  }
  ObSSWriterAddr sswriter_addr;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(MTL(ObSSWriterService*)->get_sswriter_addr(sswriter_key_3, sswriter_addr, 0))) {
    LOG_WARN("get sswriter addr failed", KR(ret), K(sswriter_key_3));
  } else {
    LOG_INFO("get sswriter addr", KR(ret), K(sswriter_key_3), K(sswriter_addr));
  }
}

} // storage
} // oceanbase
