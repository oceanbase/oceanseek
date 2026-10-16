/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "logservice/ob_garbage_collector.h"
#include "logservice/palf/lsn.h"


using namespace oceanbase::logservice;
using namespace oceanbase::palf;
namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObIMetaFunction,
                    type_);
OB_SERIALIZE_MEMBER_INHERIT(ObSSLSMeta, ObLSMeta,
                            palf_meta_);


int ObTestMetaFunction::operator()(ObSSLSMeta &ls_meta)
{
  LOG_INFO("test update ls meta", K(ls_meta));
  return OB_SUCCESS;
}

int ObSSLSMeta::init_for_ss(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls meta get invalid argument", K(ret), K(ls_meta));
  } else {
    tenant_id_ = ls_meta.tenant_id_;
    ls_id_ = ls_meta.ls_id_;
    ls_persistent_state_ = ObLSPersistentState::State::LS_NORMAL;
    // the create clog checkpoint scn
    ss_checkpoint_scn_ = ls_meta.clog_checkpoint_scn_;
    ss_checkpoint_lsn_.val_ = ls_meta.clog_base_lsn_.val_;

    rebuild_seq_ = 0;
    migration_status_ = OB_MIGRATION_STATUS_NONE;
    gc_state_ = LSGCState::NORMAL;
    restore_status_ = ObLSRestoreStatus::NONE;
    transfer_scn_.set_min();
    // do nothing
    // major_mv_merge_info_ = major_mv_merge_info;
    store_format_ = common::ObLSStoreType::OB_LS_STORE_NORMAL;

    palf_meta_.generate_by_default();
  }
  return ret;
}

int ObSSLSMeta::update_for_ss_gc_state(const logservice::LSGCState &gc_state,
                                       const SCN &scn,
                                       bool &updated)
{
  int ret = OB_SUCCESS;
  if (!ObGCHandler::is_valid_ls_gc_state(gc_state)
      || (ObGCHandler::is_ls_offline_gc_state(gc_state) && !scn.is_valid())
      || (!ObGCHandler::is_ls_offline_gc_state(gc_state) && scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_state invalid", K(ret), K(gc_state));
  } else {
    ObReentrantWLockGuard guard(rw_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("lock failed", K(ret), KPC(this));
    } else {
      ObSSLSMeta tmp(*this);
      tmp.gc_state_ = gc_state;
      tmp.offline_scn_ = scn;
      if (OB_FAIL(update_for_ss_(tmp, updated))) {
        LOG_WARN("update ss ls meta failed", K(ret), K(tmp));
      }
    }
  }
  return ret;
}

int ObSSLSMeta::update_for_ss_id_meta(const int64_t service_type,
                                      const int64_t limited_id,
                                      const SCN &latest_scn,
                                      bool &updated)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!latest_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(latest_scn));
  } else {
    ObReentrantWLockGuard guard(rw_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("lock failed", K(ret), KPC(this));
    } else {
      ObSSLSMeta tmp(*this);
      if (OB_FAIL(tmp.all_id_meta_.update_id_meta(service_type,
                                                  limited_id,
                                                  latest_scn,
                                                  updated))) {
        LOG_WARN("update id meta failed", K(ret), K(service_type), K(limited_id),
                 K(latest_scn));
      } else if (!updated) {
        // no updated, do nothing
      } else if (OB_FAIL(update_for_ss_(tmp, updated))) {
        LOG_WARN("update ss ls meta failed", K(ret), K(tmp));
      }
    }
  }
  return ret;
}

int ObSSLSMeta::update_for_ss_ls_meta(ObIMetaFunction &fn,
                                      bool &updated)
{
  int ret = OB_SUCCESS;
  ObReentrantWLockGuard guard(rw_lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("lock failed", K(ret), KPC(this));
  } else {
    ObSSLSMeta tmp(*this);
    if (OB_FAIL(fn(tmp))) {
      LOG_WARN("update ls meta failed", K(ret), K(tmp));
    } else if (OB_FAIL(update_for_ss_(tmp, updated))) {
      LOG_WARN("update ss ls meta failed", K(ret), K(tmp));
    }
  }
  return ret;
}

int ObSSLSMeta::update_for_ss(const ObSSLSMeta &ls_meta, bool &updated)
{
  int ret = OB_SUCCESS;
  if (!ls_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_meta));
  } else {
    ObReentrantWLockGuard guard(rw_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("lock failed", K(ret), K(ls_meta));
    } else if (OB_FAIL(update_for_ss_(ls_meta, updated))) {
      LOG_WARN("update for ss failed", K(ret), K(ls_meta));
    }
  }
  return ret;
}

int ObSSLSMeta::set_palf_meta(const palf::PalfBaseInfo &palf_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!palf_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("palf meta invalid", K(ret), K(palf_meta));
  } else {
    palf_meta_ = palf_meta;
  }
  return ret;
}

int ObSSLSMeta::get_palf_meta(palf::PalfBaseInfo &palf_meta) const
{
  int ret = OB_SUCCESS;
  palf_meta = palf_meta_;
  return ret;
}

int ObSSLSMeta::update_for_ss_(const ObSSLSMeta &ls_meta, bool &updated)
{
  int ret = OB_SUCCESS;
  bool tmp_updated = false;
  updated = false;
  if (ls_meta.ss_checkpoint_scn_ > ss_checkpoint_scn_) {
    ss_checkpoint_scn_ = ls_meta.ss_checkpoint_scn_;

    if (ls_meta.ss_checkpoint_lsn_ > ss_checkpoint_lsn_) {
      ss_checkpoint_lsn_ = ls_meta.ss_checkpoint_lsn_;
    }

    if (ls_meta.palf_meta_.is_valid()
        && ls_meta.palf_meta_.curr_lsn_ > palf_meta_.curr_lsn_) {
      palf_meta_ = ls_meta.palf_meta_;
    }
    
    if (ls_meta.transfer_scn_ > transfer_scn_) {
      transfer_scn_ = ls_meta.transfer_scn_;
      transfer_meta_info_ = ls_meta.transfer_meta_info_;
    }
    
    updated = true;
  }

  if (ls_meta.offline_scn_ > offline_scn_) {
    offline_scn_ = ls_meta.offline_scn_;
    updated = true;
  }

  if (ls_meta.gc_state_ > gc_state_) {
    gc_state_ = ls_meta.gc_state_;
    updated = true;
  }
  
  all_id_meta_.update_all_id_meta(ls_meta.all_id_meta_, tmp_updated);
  if (tmp_updated) {
    updated = tmp_updated;
  }

  return ret;
}

int ObSSLSMeta::set_ss_checkpoint(const SCN &ss_checkpoint_scn, const LSN &ss_checkpoint_lsn)
{
  int ret = OB_SUCCESS;
  if (!ss_checkpoint_scn.is_valid() || !ss_checkpoint_lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid checkpoint", K(ss_checkpoint_scn), K(ss_checkpoint_lsn));
  } else {
    ObReentrantWLockGuard guard(rw_lock_);
    ss_checkpoint_scn_ = ss_checkpoint_scn;
    ss_checkpoint_lsn_ = ss_checkpoint_lsn;
  }
  return ret;
}

int ObSSLSMeta::get_ss_checkpoint(SCN &ss_checkpoint_scn, LSN &ss_checkpoint_lsn)
{
  int ret = OB_SUCCESS;
  ObReentrantRLockGuard guard(rw_lock_);
  ss_checkpoint_scn = ss_checkpoint_scn_;
  ss_checkpoint_lsn = ss_checkpoint_lsn_;
  return ret;
}

int ObSSLSMeta::set_transfer_meta(const share::SCN &transfer_scn, const ObLSTransferMetaInfo &transfer_meta_info)
{
  int ret = OB_SUCCESS;
  if (!transfer_scn.is_valid() || !transfer_meta_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(transfer_scn), K(transfer_meta_info));
  } else {
    ObReentrantWLockGuard guard(rw_lock_);
    transfer_scn_ = transfer_scn;
    transfer_meta_info_ = transfer_meta_info;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
