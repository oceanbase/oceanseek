// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX STORAGE

#include "ob_ss_checkpoint_executor.h"

#include "storage/ls/ob_ls.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#endif

namespace oceanbase {

using namespace share;

namespace storage {

void ObSSCheckpointExecutor::reset() {
  is_updating_ckpt_ = false;
  ckpt_change_record_.reset();
}

int ObSSCheckpointExecutor::init() {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("trying to init ss checkpoint executor again", KR(ret), K(ls_.get_ls_id()));
  } else {
    (void)reset();
    for (int64_t idx = 0; idx < SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE; idx++) {
      ss_inc_ckpt_[idx] = nullptr;
    }
    is_inited_ = true;
  }
  return ret;
}

int ObSSCheckpointExecutor::online()
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(byte_lock_);
  is_online_ = true;
  return ret;
}

int ObSSCheckpointExecutor::offline()
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(byte_lock_);
  is_online_ = false;
  reset();
  return ret;
}

void ObSSCheckpointExecutor::destroy() { reset(); }

int ObSSCheckpointExecutor::register_inc_ckpt(const SSIncCheckpointType inc_ckpt_type, ObSSIncCheckpoint *inc_ckpt)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ss_inc_ckpt_[inc_ckpt_type])) {
    ret = OB_INIT_TWICE;
    LOG_ERROR(
        "this inc checkpoint has been registered", K(inc_ckpt_type), KP(ss_inc_ckpt_[inc_ckpt_type]), KP(inc_ckpt));
  } else {
    ss_inc_ckpt_[inc_ckpt_type] = inc_ckpt;
    FLOG_INFO("finish register inc checkpoint", K(ls_.get_ls_id()), K(inc_ckpt_type), KP(inc_ckpt));
  }
  return ret;
}

void ObSSCheckpointExecutor::unregister_inc_ckpt(const SSIncCheckpointType inc_ckpt_type, ObSSIncCheckpoint *inc_ckpt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ss_inc_ckpt_[inc_ckpt_type]) || ss_inc_ckpt_[inc_ckpt_type] != inc_ckpt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected ss_inc_ckpt", K(inc_ckpt_type), KP(ss_inc_ckpt_[inc_ckpt_type]), KP(inc_ckpt));
  } else {
    ss_inc_ckpt_[inc_ckpt_type] = nullptr;
    FLOG_INFO("finish unregister inc checkpoint", K(ls_.get_ls_id()), K(inc_ckpt_type), KP(inc_ckpt));
  }
}

int ObSSCheckpointExecutor::get_ss_rec_scn(SCN &ss_rec_scn)
{
  int ret = OB_SUCCESS;
  ss_rec_scn.set_invalid();
  if (ATOMIC_BCAS(&is_updating_ckpt_, false, true)) {
    ObByteLockGuard guard(byte_lock_);
    if (is_online_) {
      SCN sn_clog_checkpoint_scn = ls_.get_clog_checkpoint_scn();
      ss_rec_scn = sn_clog_checkpoint_scn;

      // used for print debug info
      ObSEArray<SCN, int64_t(SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE)> ss_ckpt_scn_arr;
      for (int64_t ckpt_idx = 0; OB_SUCC(ret) && ckpt_idx < SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE;
           ckpt_idx++) {
        if (OB_ISNULL(ss_inc_ckpt_[ckpt_idx])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid ss inc checkpoint", KR(ret), K(ls_.get_ls_id()), K(ckpt_idx), K(ss_inc_ckpt_));
        } else {
          SCN cur_rec_scn = ss_inc_ckpt_[ckpt_idx]->get_ss_rec_scn();
          ss_ckpt_scn_arr.push_back(cur_rec_scn);
          ss_rec_scn = MIN(ss_rec_scn, cur_rec_scn);
        }
      }

      if (OB_SUCC(ret)) {
        (void)ObCkptUtil::check_ckpt_changed(true /*is_ss_ckpt*/, ls_.get_ls_id(), ss_rec_scn, ckpt_change_record_);
      }
      LOG_INFO("finish get ss_rec_scn",
               K(ret),
               K(ls_.get_ls_id()),
               K(ss_rec_scn),
               K(sn_clog_checkpoint_scn),
               ObSSIncCheckpoint::get_inc_ckpt_string(0),
               ss_ckpt_scn_arr[0],
               ObSSIncCheckpoint::get_inc_ckpt_string(1),
               ss_ckpt_scn_arr[1]);

          const char *INFO = "Aggregate ss_rec_scn";
          ADD_SS_DIAGNOSE_INFO(ObSSDiagnoseInfo::DIA_TYPE_SS_CKPT,
                               ls_.get_ls_id(),
                               ObTabletID(0),
                               ObClockGenerator::getClock(),
                               K(ret),
                               K(INFO),
                               K(ss_rec_scn),
                               K(sn_clog_checkpoint_scn),
                               ObSSIncCheckpoint::get_inc_ckpt_string(0),
                               ss_ckpt_scn_arr[0],
                               ObSSIncCheckpoint::get_inc_ckpt_string(1),
                               ss_ckpt_scn_arr[1]);

    } else {
      ret = OB_LS_OFFLINE;
      LOG_WARN("ls has been offlined", KR(ret));
    }

    // reset updating flag
    ATOMIC_STORE(&is_updating_ckpt_, false);
  } else {
    ret = OB_EAGAIN;
    LOG_INFO("another updating task is running. skip this one", KR(ret));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
