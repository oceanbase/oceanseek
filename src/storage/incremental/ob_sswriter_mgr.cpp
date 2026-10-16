/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_mgr.h"
#include "storage/incremental/ob_sswriter_rpc.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_locality_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class ObIterateSSWriterGroupStatFunctor
{
public:
  explicit ObIterateSSWriterGroupStatFunctor(ObSSWriterGroupStatIterator &iter)
    : sswriter_group_stat_iter_(iter),
      ret_(OB_SUCCESS) {}
  bool operator()(const ObLsSSWriterKey &key, ObLsSSWriterCtx *ctx_base)
  {
    bool ret_bool = false;
    int ret = OB_SUCCESS;
    if (!key.is_valid() || OB_ISNULL(ctx_base)) {
      LOG_WARN("invalid argument", K(key), KP(ctx_base));
    } else if (!ctx_base->is_inited()) {
      // do nothing
    } else {
      int64_t now = ObTimeUtility::current_time();
      ObSSWriterGroupStat sswriter_group_stat;
      if (OB_FAIL(sswriter_group_stat.init(
              key,
              ctx_base->default_group_.group_id_,
              ctx_base->grant_server_,
              now,
              ctx_base->default_group_.lease_expire_ts_ - now,
              ctx_base->default_group_.lease_expire_ts_,
              ctx_base->default_group_.create_ts_,
              ctx_base->default_group_.epoch_,
              ctx_base->default_group_.last_request_lease_ts_))) {
        LOG_WARN("sswriter group stat init failed", K(key), K(sswriter_group_stat));
      } else if (OB_FAIL(sswriter_group_stat_iter_.push(sswriter_group_stat))) {
        LOG_WARN("push sswriter group stat failed", K(key), K(sswriter_group_stat));
      } else {
        // success
      }
    }
    if (OB_SUCC(ret)) {
      ret_bool = true;
    } else {
      ret_ = ret;
    }
    return ret_bool;
  }
  int get_ret() const { return ret_; }
private:
  ObSSWriterGroupStatIterator &sswriter_group_stat_iter_;
  int ret_;
};

class ObRenewAllSSWriterLeaseFunctor
{
public:
  ObRenewAllSSWriterLeaseFunctor() : renew_lease_count_(0) {}
  int64_t get_renew_lease_count() const { return renew_lease_count_; }
  bool operator()(const ObLsSSWriterKey &key, ObLsSSWriterCtx *ctx_base)
  {
    bool ret_bool = false;
    int ret = OB_SUCCESS;
    if (!key.is_valid() || OB_ISNULL(ctx_base)) {
      LOG_WARN("invalid argument", K(key), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->try_renew_lease_for_default_group())) {
        LOG_WARN("try renew lease failed", KR(ret));
      } else if (OB_FAIL(ctx_base->check_status_and_try_exit())) {
        LOG_WARN("check status failed", KR(ret));
      } else {
        ++renew_lease_count_;
        ret_bool = true;
      }
    }
    return ret_bool;
  }
private:
  int64_t renew_lease_count_;
};

class ObReleaseAllSSWriterCtxFunctor
{
public:
  ObReleaseAllSSWriterCtxFunctor() : released_count_(0) {}
  int64_t get_released_count() const { return released_count_; }
  bool operator()(const ObLsSSWriterKey &key, ObLsSSWriterCtx *ctx_base)
  {
    bool ret_bool = false;
    int ret = common::OB_SUCCESS;

    if (!key.is_valid() || OB_ISNULL(ctx_base)) {
      LOG_WARN("invalid argument", K(key), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->kill())) {
        LOG_WARN("kill sswriter ctx error", KR(ret));
      } else {
        // do nothing
      }
      ++released_count_;
      ret_bool = true;
    }

    return ret_bool;
  }
private:
  int64_t released_count_;
};

void ObLsSSWriterCtxCreateArg::reset()
{
  grant_server_.reset();
}

bool ObLsSSWriterCtxCreateArg::is_valid() const
{
  return grant_server_.is_valid();
}

void ObSSWriterGroup::reset()
{
  ATOMIC_STORE(&group_id_, -1);
  ATOMIC_STORE(&lease_expire_ts_, -1);
  ATOMIC_STORE(&epoch_, -1);
  ATOMIC_STORE(&create_ts_, -1);
}

// if in grant phase, do not refresh
bool ObSSWriterGroup::need_refresh(
    const int64_t current_ts) const
{
  bool ret_bool = false;
  if (0 < group_id_ && 0 < lease_expire_ts_) {
    if ((current_ts < lease_expire_ts_)
        && (lease_expire_ts_ - current_ts < SSWRITER_LEASE_RENEW_THRESHOLD_US)) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

// NOTE that if in grant phase (-1 == lease_expire_ts), return false
bool ObSSWriterGroup::is_lease_expired(
    const int64_t current_ts) const
{
  return 0 < lease_expire_ts_ && lease_expire_ts_ < current_ts;
}

void ObLsSSWriterCtx::reset()
{
  hash_node_ = NULL;  // for hash value
  is_inited_ = false;
  is_exiting_ = false;
  is_waiting_for_group_ = true;
  grant_server_.reset();
  key_.reset();
  ctx_mgr_ = NULL;
  default_group_.reset();
}

int ObLsSSWriterCtx::set_exiting_()
{
  int ret = OB_SUCCESS;
  if (is_exiting_) {
    // do nothing
  } else if (NULL == ctx_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sswriter ctx mgr", KR(ret));
  } else {
    if (OB_FAIL(ctx_mgr_->erase_ls_sswriter_ctx(key_))) {
      LOG_WARN("erase sswriter ctx failed", KR(ret), K(*this));
    } else {
      is_exiting_ = true;
    }
    LOG_INFO("set exiting", K(ret), K(*this));
  }
  return ret;
}

void ObLsSSWriterCtx::destroy()
{
  reset();
}

int ObLsSSWriterCtx::kill()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterCtx not inited", KR(ret));
  } else if (OB_FAIL(set_exiting_())) {
    LOG_WARN("set exiting failed", KR(ret), K(*this));
  } else {
    // do nothing
  }
  LOG_INFO("kill ls sswriter ctx", K(ret), K(*this));
  return ret;
}

int ObLsSSWriterCtx::init(
    const ObLsSSWriterKey &key,
    ObLsSSWriterCtxMgr *ctx_mgr,
    const common::ObAddr &grant_server)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterCtx inited twice", KR(ret), K(key));
  } else if (!key.is_valid() || !grant_server.is_valid() || NULL == ctx_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key), K(grant_server), KP(ctx_mgr));
  } else {
    key_ = key;
    ctx_mgr_ = ctx_mgr;
    grant_server_ = grant_server;
    is_inited_ = true;
  }
  return ret;
}

int ObLsSSWriterCtx::add_group(
    const int64_t group_id,
    const int64_t epoch,
    const common::ObAddr &grant_server)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterCtx not inited", KR(ret));
  } else if (0 >= group_id || 0 >= epoch || !grant_server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(group_id), K(epoch));
  } else if (grant_server_ != grant_server) {
    ret = OB_EAGAIN;
    LOG_WARN("different grant server", KR(ret), K(grant_server), K(epoch), K(*this));
  } else if (OB_FAIL(add_group_(group_id, epoch, grant_server))) {
    LOG_WARN("add group failed", KR(ret));
  } else {
    // for other group
  }
  // set false regardless of the result
  is_waiting_for_group_ = false;
  return ret;
}

int ObLsSSWriterCtx::add_group_(
    const int64_t group_id,
    const int64_t epoch,
    const common::ObAddr &grant_server)
{
  int ret = OB_SUCCESS;
  if (SSWRITER_DEFAULT_GROUP_ID == group_id) {
    // for default group
    if (default_group_.is_valid()) {
      ret = OB_ENTRY_EXIST;
    } else {
      default_group_.group_id_ = SSWRITER_DEFAULT_GROUP_ID;
      default_group_.epoch_ = epoch;
      default_group_.create_ts_ = ObTimeUtility::current_time();
    }
  } else {
    // for other group
  }
  LOG_INFO("add group", K(ret), K(*this));
  return ret;
}

int ObLsSSWriterCtx::remove_group(
    const int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterCtx not inited", KR(ret));
  } else if (0 >= group_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(group_id));
  } else {
    remove_group_(group_id);
  }
  return ret;
}

int ObLsSSWriterCtx::update_lease(
    const int64_t group_id,
    const int64_t lease_expire_ts,
    const int64_t epoch)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterCtx not inited", KR(ret));
  } else if (0 >= group_id || 0 >= lease_expire_ts || 0 >= epoch) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(group_id), K(lease_expire_ts),
        K(epoch));
  } else if (OB_FAIL(update_lease_(group_id, lease_expire_ts, epoch))) {
    LOG_WARN("update lease failed", KR(ret), K(group_id), K(lease_expire_ts), K(epoch));
  } else {
    // do nothing
  }
  return ret;
}

int ObLsSSWriterCtx::update_lease_(
    const int64_t group_id,
    const int64_t lease_expire_ts,
    const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (SSWRITER_DEFAULT_GROUP_ID == group_id) {
    if (default_group_.is_valid()) {
      if (default_group_.epoch_ == epoch && lease_expire_ts > default_group_.lease_expire_ts_) {
        default_group_.lease_expire_ts_ = lease_expire_ts;
        LOG_INFO("update sswriter lease", K_(key), K(group_id), K(lease_expire_ts),
            K(epoch));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

// if lease needs to be refreshed, request lease from grant server
int ObLsSSWriterCtx::try_renew_lease_for_default_group()
{
  int ret = OB_SUCCESS;
  bool need_refresh = false;
  int64_t start_ts = -1;
  int64_t end_ts = -1;
  int64_t epoch = -1;
  int64_t used_time_us = 0;
  ObTimeGuard timeguard("try renew lease", 100 * 1000);
  {
    // step 1, check whether the lease needs to be refreshed
    // under protection of ctx lock
    ObSpinLockGuard guard(lock_);
    start_ts = ObTimeUtility::current_time();
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObSSWriterCtx not inited", KR(ret));
    } else if (!default_group_.is_valid() || default_group_.is_in_grant()) {
      // need_refresh = false
    } else if (default_group_.need_refresh(start_ts)) {
      need_refresh = true;
      epoch = default_group_.epoch_;
    }
  }
  timeguard.click();
  if (OB_SUCC(ret) && need_refresh) {
    // step 2, request lease from grant server
    // without protection of ctx lock
    ObSSWriterLeaseRequestArg arg;
    arg.group_id_ = SSWRITER_DEFAULT_GROUP_ID;
    arg.ls_id_ = key_.get_ls_id();
    arg.type_ = key_.get_type();
    arg.epoch_ = epoch;
    arg.lease_us_ = SSWRITER_DEFAULT_LEASE_US;
    arg.target_addr_ = grant_server_;
    arg.sswriter_base_ts_ = start_ts;
    if (GCTX.self_addr() == grant_server_) {
      // for local
      if (OB_FAIL(MTL(ObSSWriterService*)->request_sswriter_lease(arg))) {
        LOG_WARN("request sswriter lease local failed", KR(ret), K(arg));
      } else {
        ObSpinLockGuard guard(lock_);
        const int64_t lease_expire_ts = start_ts + SSWRITER_DEFAULT_LEASE_US;
        default_group_.last_request_lease_ts_ = start_ts;
        if (OB_FAIL(update_lease_(SSWRITER_DEFAULT_GROUP_ID, lease_expire_ts, epoch))) {
          LOG_WARN("update default group lease failed", KR(ret), K(lease_expire_ts), K(epoch));
        }
      }
    } else {
      // for remote
      if (OB_FAIL(MTL(ObSSWriterService*)->request_sswriter_lease_async(arg))) {
        LOG_WARN("request sswriter lease failed", KR(ret), K(arg));
      } else {
        // in this case, update lease after async response
        default_group_.last_request_lease_ts_ = start_ts;
      }
    }
  }
  timeguard.click();
  end_ts = ObTimeUtility::current_time();
  used_time_us = end_ts - start_ts;
  LOG_INFO("try renew sswriter lease", KR(ret), K(need_refresh),
      K(epoch), K(used_time_us));
  return ret;
}

// if lease of each group is expired, try exit
int ObLsSSWriterCtx::check_status_and_try_exit()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtx not inited", KR(ret));
  } else if (is_waiting_for_group_) {
    // in this case, there are no groups added after init
    LOG_INFO("waiting for group", K(*this));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    // step 1, remove group if lease is expired
    if (default_group_.is_valid() && default_group_.is_lease_expired(now)) {
      remove_group_(SSWRITER_DEFAULT_GROUP_ID);
    }
    // step 2, if all groups are removed, exit
    if (!default_group_.is_valid()) {
      ret = set_exiting_();
    }
    LOG_INFO("check status", K(ret), K(*this));
  }
  return ret;
}

bool ObLsSSWriterCtx::is_lease_valid(
    const common::ObTabletID &tablet_id)
{
  bool ret_bool = false;
  ObSpinLockGuard guard(lock_);
  // for default group
  ret_bool = is_lease_valid_(SSWRITER_DEFAULT_GROUP_ID);
  return ret_bool;
}

bool ObLsSSWriterCtx::is_lease_valid(
    const int64_t group_id)
{
  ObSpinLockGuard guard(lock_);
  return is_lease_valid_(group_id);
}

bool ObLsSSWriterCtx::is_lease_valid_(
    const int64_t group_id) const
{
  bool ret_bool = false;
  if (SSWRITER_DEFAULT_GROUP_ID == group_id && default_group_.is_valid()) {
    if (ObTimeUtility::current_time() <= default_group_.lease_expire_ts_) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

void ObLsSSWriterCtx::remove_group_(
    const int64_t group_id)
{
  LOG_INFO("remove group", K(*this));
  if (SSWRITER_DEFAULT_GROUP_ID == group_id) {
    default_group_.reset();
  }
}

// NOTE that performance is poor when called by multiple threads simultaneously
// TODO, optimimize the performance
int ObLsSSWriterCtx::check_lease(
    const common::ObTabletID &tablet_id,
    bool &is_sswriter,
    int64_t &epoch)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterCtx not inited", KR(ret));
  } else {
    // check whether the tablet is in default group
    int64_t now = ObTimeUtility::current_time();
    if (default_group_.is_valid() && !default_group_.is_lease_expired(now)) {
      is_sswriter = true;
      epoch = default_group_.epoch_;
    } else {
      is_sswriter = false;
    }
  }
  return ret;
}

// for performance
// without ctx lock
bool ObLsSSWriterCtx::is_lease_valid_with_epoch(
    const int64_t epoch) const
{
  int ret_bool = false;
  if (-1 != epoch) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t lease_expire_ts = ATOMIC_LOAD(&default_group_.lease_expire_ts_);
    const int64_t current_epoch = ATOMIC_LOAD(&default_group_.epoch_);
    if (current_epoch == epoch && lease_expire_ts > now) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

int ObLsSSWriterCtxMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLsSSWriterCtxMgr inited twice", KR(ret));
  } else if (OB_FAIL(ctx_map_.init("SSWCtxMap", MTL_ID()))) {
    LOG_WARN("ctx map init failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLsSSWriterCtxMgr::destroy()
{
  if (is_inited_) {
    release_all_ctx_();
    ctx_map_.destroy();
    is_inited_ = false;
    LOG_INFO("ObLsSSWriterCtxMgr destroy", K(*this));
  }
}

// get ls sswriter ctx
// if not exit, create ctx and insert it into map first
int ObLsSSWriterCtxMgr::acquire_ls_sswriter_ctx(
    const ObLsSSWriterCtxCreateArg &create_arg,
    const ObLsSSWriterKey &key,
    ObLsSSWriterCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObLsSSWriterCtx *tmp_ctx = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else if (!create_arg.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key), K(create_arg));
  } else if (OB_SUCC(get_ls_sswriter_ctx_(key, tmp_ctx))) {
    // if exist
    ctx = tmp_ctx;
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_ERROR("get ls sswriter ctx error", KR(ret), K(key));
  } else {
    tmp_ctx = NULL;
    // if not exist
    if (OB_FAIL(ctx_map_.alloc_value(tmp_ctx))) {
      LOG_WARN("alloc sswriter ctx failed", KR(ret));
    } else if (NULL == tmp_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sswriter ctx", KR(ret));
    } else if (OB_FAIL(tmp_ctx->init(key, this, create_arg.grant_server_))) {
      LOG_WARN("ls sswriter ctx init failed", KR(ret), K(key), K(create_arg));
    } else if (OB_FAIL(ctx_map_.insert_and_get(key, tmp_ctx))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        tmp_ctx->reset();
        ctx_map_.free_value(tmp_ctx);
        tmp_ctx = NULL;
        if (OB_FAIL(get_ls_sswriter_ctx_(key, tmp_ctx))) {
          LOG_WARN("get ls sswriter ctx failed", KR(ret), K(key));
        } else {
          // if exist
          ctx = tmp_ctx;
        }
      } else {
        LOG_WARN("insert ls sswriter ctx into map failed", KR(ret), K(key));
      }
    } else if (NULL == tmp_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected ctx", KR(ret), K(key));
    } else {
      ctx = tmp_ctx;
    }
  }
  return ret;
}

// return sswriter ctx
// if not exist, return OB_ENTRY_NOT_EXIST
int ObLsSSWriterCtxMgr::get_ls_sswriter_ctx(
    const ObLsSSWriterKey &key,
    ObLsSSWriterCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObLsSSWriterCtx *tmp_ctx = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_FAIL(get_ls_sswriter_ctx_(key, tmp_ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get ls sswriter ctx error", KR(ret), K(key));
    } else {
      MTL(ObSSWriterService*)->get_statistics().inc_get_ctx_not_exist_count();
    }
  } else {
    ctx = tmp_ctx;
  }
  return ret;
}

int ObLsSSWriterCtxMgr::get_ls_sswriter_ctx_(
    const ObLsSSWriterKey &key,
    ObLsSSWriterCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObLsSSWriterCtx *tmp_ctx = NULL;
  if (OB_SUCC(ctx_map_.get(key, tmp_ctx))) {
    if (OB_ISNULL(tmp_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is NULL", KR(ret), KP(tmp_ctx));
    } else {
      ctx = tmp_ctx;
    }
  }
  return ret;
}

int ObLsSSWriterCtxMgr::revert_ls_sswriter_ctx(ObLsSSWriterCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ctx));
  } else {
    ctx_map_.revert(ctx);
  }
  return ret;
}

int ObLsSSWriterCtxMgr::erase_ls_sswriter_ctx(const ObLsSSWriterKey &key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_FAIL(ctx_map_.del(key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("erase sswriter ctx from hashmap error",
          KR(ret), K(key));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLsSSWriterCtxMgr::release_all_ctx_()
{
  int ret = OB_SUCCESS;
  ObReleaseAllSSWriterCtxFunctor fn;
  if (OB_FAIL(ctx_map_.remove_if(fn))) {
    LOG_WARN("remove sswriter ctx failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObLsSSWriterCtxMgr::try_print_statistics()
{
  static const int64_t PRINT_INTERVAL = 10 * 1000 * 1000;
  int64_t now = ObTimeUtility::current_time();
  if (is_inited_ && (now - last_print_ts_ > PRINT_INTERVAL)) {
    int64_t active_value_count = ctx_map_.get_alloc_handle().get_active_value_count();
    LOG_INFO("sswriter ctx mgr statistics", K(active_value_count));
    last_print_ts_ = now;
  }
}

int ObLsSSWriterCtxMgr::try_renew_lease()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else {
    ObRenewAllSSWriterLeaseFunctor fn;
    if (OB_FAIL(ctx_map_.for_each(fn))) {
      LOG_WARN("try renew sswriter lease failed", KR(ret));
    } else {
      // do noting
    }
  }
  return ret;
}

int ObLsSSWriterCtxMgr::iterate_sswriter_group_stat(
    ObSSWriterGroupStatIterator &stat_iter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLsSSWriterCtxMgr not inited", KR(ret));
  } else {
    ObIterateSSWriterGroupStatFunctor fn(stat_iter);
    if (OB_FAIL(ctx_map_.for_each(fn))) {
      ret = fn.get_ret();
      LOG_WARN("for each sswriter group failed", KR(ret));
    }
  }
  return ret;
}

} // storage
} // oceanbase

