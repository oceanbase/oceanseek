/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_location.h"
#include "storage/incremental/ob_sswriter_msg.h"
#include "storage/incremental/ob_sswriter_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class ObRenewSSWriterLocationFunctor
{
public:
  ObRenewSSWriterLocationFunctor() {}
  bool operator()(const ObSSWriterKey &key, ObSSWriterLocationCtx *ctx_base)
  {
    bool ret_bool = false;
    int ret = OB_SUCCESS;
    if (!key.is_valid() || OB_ISNULL(ctx_base)) {
      LOG_WARN("invalid argument", K(key), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->try_renew_location())) {
        LOG_WARN("try renew lease failed", KR(ret));
      } else if (OB_FAIL(ctx_base->check_status_and_try_exit())) {
        LOG_WARN("check status failed", KR(ret));
      } else {
        ret_bool = true;
      }
    }
    return ret_bool;
  }
};

class ObReleaseSSWriterLocationCtxFunctor
{
public:
  ObReleaseSSWriterLocationCtxFunctor() : released_count_(0) {}
  int64_t get_released_count() const { return released_count_; }
  bool operator()(const ObSSWriterKey &key, ObSSWriterLocationCtx *ctx_base)
  {
    bool ret_bool = false;
    int ret = common::OB_SUCCESS;

    if (!key.is_valid() || OB_ISNULL(ctx_base)) {
      LOG_WARN("invalid argument", K(key), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->kill())) {
        LOG_WARN("kill sswriter location ctx error", KR(ret));
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

int ObSSWriterLocationCtx::init(
    const ObSSWriterKey &sswriter_key,
    ObSSWriterLocationMgr *location_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterLocationCtx inited twice", KR(ret), K(sswriter_key));
  } else if (!sswriter_key.is_valid() || NULL == location_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sswriter_key), KP(location_mgr));
  } else {
    sswriter_key_ = sswriter_key;
    location_mgr_ = location_mgr;
    last_get_addr_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

void ObSSWriterLocationCtx::reset()
{
  hash_node_ = NULL;  // for hash value
  sswriter_key_.reset();
  addr_.reset();
  epoch_ = -1;
  lease_expire_ts_ = -1;
  last_get_addr_ts_ = -1;
  last_update_ts_ = -1;
  last_request_ts_ = -1;
  last_send_request_ts_ = -1;
  location_mgr_ = NULL;
  is_exiting_ = false;
  is_inited_ = false;
}

int ObSSWriterLocationCtx::kill()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationCtx not inited", KR(ret));
  } else if (OB_FAIL(set_exiting_())) {
    LOG_WARN("set exiting failed", KR(ret), K(*this));
  } else {
    // do nothing
  }
  LOG_INFO("kill sswriter location ctx", K(ret), K(*this));
  return ret;
}

int ObSSWriterLocationCtx::set_exiting_()
{
  int ret = OB_SUCCESS;
  if (is_exiting_) {
    // do nothing
  } else if (NULL == location_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sswriter ctx mgr", KR(ret));
  } else {
    if (OB_FAIL(location_mgr_->erase_sswriter_location_ctx(sswriter_key_))) {
      LOG_WARN("erase sswriter ctx failed", KR(ret), K(*this));
    } else {
      is_exiting_ = true;
    }
    LOG_INFO("set exiting", K(ret), K(*this));
  }
  return ret;
}

// only for check loop
int ObSSWriterLocationCtx::try_renew_location()
{
  int ret = OB_SUCCESS;
  bool need_refresh = false;
  int64_t current_ts = -1;
  {
    ObSpinLockGuard guard(lock_);
    current_ts = ObTimeUtility::current_time();
    if (!is_inited_ || is_exiting_) {
      need_refresh = false;
    } else if (-1 == epoch_) {
      need_refresh = true;
    } else if (current_ts < lease_expire_ts_) {
      need_refresh = false;
    } else if (1000 > current_ts - last_send_request_ts_) {
      need_refresh = false;
    } else if (current_ts - last_get_addr_ts_ > GC_TIME_INTERVAL) {
      if (ObSSWriterType::META == sswriter_key_.get_type()) {
        need_refresh = true;
      } else {
        need_refresh = false;
      }
    } else {
      need_refresh = true;
    }
    if (need_refresh) {
      last_send_request_ts_ = current_ts;
    }
  }
  if (need_refresh) {
    int tmp_ret = OB_SUCCESS;
    ObSSWriterLocationRequestArg arg;
    arg.sswriter_key_ = sswriter_key_;
    arg.request_start_ts_ = current_ts;
    if (OB_SUCCESS != (tmp_ret = MTL(ObSSWriterService*)->request_sswriter_location(arg))) {
      LOG_WARN("request sswriter location failed", KR(tmp_ret), K(arg));
    } else {
      // LOG_INFO("request sswriter location success", K(arg));
    }
  }
  return ret;
}

int ObSSWriterLocationCtx::check_status_and_try_exit()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationCtx not inited", KR(ret));
  } else if (ObSSWriterType::META == sswriter_key_.get_type()
      && sswriter_key_.get_ls_id().is_sys_ls()) {
    // do not exit for meta sswriter location
  } else {
    const int64_t current_ts = ObTimeUtility::current_time();
    if (GC_TIME_INTERVAL < current_ts - last_get_addr_ts_) {
      ret = set_exiting_();
    }
    LOG_INFO("check status", K(ret), K(*this));
  }
  return ret;
}

int ObSSWriterLocationCtx::get_addr(ObSSWriterAddr &sswriter_addr)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationCtx not inited", KR(ret));
  } else {
    // NOTE that
    // return sswriter addr regardless of its lease
    sswriter_addr.addr_ = addr_;
    sswriter_addr.epoch_ = epoch_;
  }
  last_get_addr_ts_ = ObTimeUtility::current_time();
  return ret;
}

int ObSSWriterLocationCtx::update_addr(
    const common::ObAddr &addr,
    const int64_t epoch,
    const int64_t lease_expire_ts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationCtx not inited", KR(ret));
  } else if (!addr.is_valid() || 0 >= epoch || 0 >= lease_expire_ts) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr), K(epoch), K(lease_expire_ts));
  } else {
    if (epoch > epoch_) {
      epoch_ = epoch;
      lease_expire_ts_ = lease_expire_ts;
      addr_ = addr;
      last_update_ts_ = ObTimeUtility::current_time();
    } else if (epoch == epoch_ && lease_expire_ts > lease_expire_ts_) {
      lease_expire_ts_ = lease_expire_ts;
      last_update_ts_ = ObTimeUtility::current_time();
      if (addr_ != addr) {
        // only print error log
        LOG_ERROR("unexpected sswriter addr", K(*this), K(addr), K(lease_expire_ts));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObSSWriterLocationMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterLocationMgr inited twice", KR(ret));
  } else if (OB_FAIL(ctx_map_.init("SSWLCtxMap", MTL_ID()))) {
    LOG_WARN("ctx map init failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSWriterLocationMgr::destroy()
{
  if (is_inited_) {
    release_all_ctx_();
    ctx_map_.destroy();
    last_print_ts_ = -1;
    is_inited_ = false;
    LOG_INFO("ObSSWriterLocationMgr destroy", K(*this));
  }
}

int ObSSWriterLocationMgr::release_all_ctx_()
{
  int ret = OB_SUCCESS;
  ObReleaseSSWriterLocationCtxFunctor fn;
  if (OB_FAIL(ctx_map_.remove_if(fn))) {
    LOG_WARN("remove sswriter location ctx failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

// get sswriter location ctx
// if not exit, create ctx and insert it into map first
int ObSSWriterLocationMgr::acquire_sswriter_location_ctx(
    const ObSSWriterKey &key,
    ObSSWriterLocationCtx *&ctx)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  ObSSWriterLocationCtx *tmp_ctx = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationMgr not inited", KR(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_SUCC(get_sswriter_location_ctx_(key, tmp_ctx))) {
    // if exist
    ctx = tmp_ctx;
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_ERROR("get sswriter location ctx error", KR(ret), K(key));
  } else {
    // if not exist
    tmp_ctx = NULL;
    int64_t retry_count = 0;
    do {
      retry_count++;
      if (OB_FAIL(ctx_map_.alloc_value(tmp_ctx))) {
        LOG_WARN("alloc sswriter location ctx failed", KR(ret));
      } else if (NULL == tmp_ctx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sswriter location ctx", KR(ret));
      } else if (OB_FAIL(tmp_ctx->init(key, this))) {
        LOG_WARN("sswriter location ctx init failed", KR(ret), K(key));
      } else if (OB_FAIL(ctx_map_.insert_and_get(key, tmp_ctx))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          tmp_ctx->reset();
          ctx_map_.free_value(tmp_ctx);
          tmp_ctx = NULL;
          if (OB_FAIL(get_sswriter_location_ctx_(key, tmp_ctx))) {
            // if return OB_ENTRY_NOT_EXIST, we need retry
            LOG_WARN("get sswriter location ctx failed", KR(ret), K(key));
          } else {
            // if exist
            ctx = tmp_ctx;
          }
        } else {
          LOG_WARN("insert sswriter location ctx into map failed", KR(ret), K(key));
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret && MAX_RETRY_COUNT >= retry_count);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (NULL == tmp_ctx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected ctx", KR(ret), K(key));
    } else {
      ctx = tmp_ctx;
    }
  }
  return ret;
}

int ObSSWriterLocationMgr::get_sswriter_location_ctx(
    const ObSSWriterKey &sswriter_key,
    ObSSWriterLocationCtx *&sswriter_location_ctx)
{
  int ret = OB_SUCCESS;
  ObSSWriterLocationCtx *tmp_ctx = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!sswriter_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sswriter_key));
  } else if (OB_FAIL(get_sswriter_location_ctx_(sswriter_key, tmp_ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get sswriter location ctx error", KR(ret), K(sswriter_key));
    } else {
      // TODO, statistics
      // MTL(ObSSWriterService*)->get_statistics().inc_get_ctx_not_exist_count();
    }
  } else {
    sswriter_location_ctx = tmp_ctx;
  }
  return ret;
}

int ObSSWriterLocationMgr::get_sswriter_location_ctx_(
    const ObSSWriterKey &key,
    ObSSWriterLocationCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObSSWriterLocationCtx *tmp_ctx = NULL;
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

int ObSSWriterLocationMgr::revert_sswriter_location_ctx(
    ObSSWriterLocationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationMgr not inited", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ctx));
  } else {
    ctx_map_.revert(ctx);
  }
  return ret;
}

int ObSSWriterLocationMgr::erase_sswriter_location_ctx(
    const ObSSWriterKey &key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_FAIL(ctx_map_.del(key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("erase sswriter location ctx from hashmap error",
          KR(ret), K(key));
    }
  } else {
    // do nothing
  }
  return ret;
}

void ObSSWriterLocationMgr::try_print_statistics()
{
  static const int64_t PRINT_INTERVAL = 10 * 1000 * 1000;
  int64_t now = ObTimeUtility::current_time();
  if (is_inited_ && (now - last_print_ts_ > PRINT_INTERVAL)) {
    int64_t active_value_count = ctx_map_.get_alloc_handle().get_active_value_count();
    LOG_INFO("sswriter location mgr statistics", K(active_value_count));
    last_print_ts_ = now;
  }
}

int ObSSWriterLocationMgr::try_renew_location()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSWriterLocationMgr not inited", KR(ret));
  } else {
    ObRenewSSWriterLocationFunctor fn;
    if (OB_FAIL(ctx_map_.for_each(fn))) {
      LOG_WARN("try renew sswriter location failed", KR(ret));
    } else {
      // do noting
    }
  }
  return ret;
}

} // storage
} // oceanbase

