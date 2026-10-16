// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX STORAGE

#include "ob_ss_inc_meta_checkpoint.h"

#include "storage/ls/ob_ls.h"

namespace oceanbase {
using namespace share;
using namespace logservice;

namespace storage {

void ObSSIncMetaCheckpoint::reset()
{
  for (int64_t idx = 0; idx < SSIncMetaType::MAX_INC_META_TYPE; idx++) {
    inc_meta_ckpts_[idx] = nullptr;
  }
}

int ObSSIncMetaCheckpoint::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("trying to init inc meta checkpoint again", KR(ret));
  } else {
    reset();
    const ObLSID ls_id = ls_.get_ls_id();
    is_inited_ = true;
    FLOG_INFO("finish init inc meta checkpoint", K(ls_id), KP(inc_meta_ckpts_));
  }
  return ret;
}

SCN ObSSIncMetaCheckpoint::get_ss_rec_scn()
{
  int ret = OB_SUCCESS;
  SCN ss_rec_scn(SCN::max_scn());
  int64_t min_meta_ckpt_type = SSIncMetaType::MAX_INC_META_TYPE;

  for (int64_t idx = 0; idx < SSIncMetaType::MAX_INC_META_TYPE; idx++) {
    if (OB_ISNULL(inc_meta_ckpts_[idx])) {
      LOG_TRACE("this inc_meta_ckpt is nullptr", K(idx), "IncMetaCkptType", get_meta_ckpt_string(min_meta_ckpt_type));
    } else if (inc_meta_ckpts_[idx]->get_rec_scn() < ss_rec_scn) {
      ss_rec_scn = inc_meta_ckpts_[idx]->get_rec_scn();
      min_meta_ckpt_type = idx;
    }
  }

  LOG_INFO("finish get ss_rec_scn from inc_meta_checkpoint",
           K(ret),
           K(ls_.get_ls_id()),
           K(ss_rec_scn),
           "IncMetaCkptType", get_meta_ckpt_string(min_meta_ckpt_type));

  return ss_rec_scn;
}

int ObSSIncMetaCheckpoint::register_ss_inc_meta_ckpt(SSIncMetaType inc_meta_type, ObICheckpointSubHandler *inc_meta)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_.get_ls_id();

  if (inc_meta_type >= SSIncMetaType::MAX_INC_META_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trying to register an unexpected meta type", K(ls_id), K(inc_meta_type));
  } else if (OB_ISNULL(inc_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid nullptr of inc meta", K(ls_id), K(inc_meta_type));
  } else if (OB_NOT_NULL(inc_meta_ckpts_[inc_meta_type])) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("inc meta ckpt is not nullptr",
              K(ret),
              K(inc_meta_type),
              KP(inc_meta),
              KP(inc_meta_ckpts_[inc_meta_type]),
              K(ls_));
  } else {
    inc_meta_ckpts_[inc_meta_type] = inc_meta;
    FLOG_INFO(
        "register one inc meta ckpt", K(inc_meta_type), "CKPT TYPE", get_meta_ckpt_string(inc_meta_type), KP(inc_meta));
  }
  return ret;
}

void ObSSIncMetaCheckpoint::unregister_ss_inc_meta_ckpt(SSIncMetaType inc_meta_type, ObICheckpointSubHandler *inc_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inc_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nullptr of inc meta");
  } else if (OB_ISNULL(inc_meta_ckpts_[inc_meta_type]) || (inc_meta_ckpts_[inc_meta_type] != inc_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inc meta ckpt is invalid",
              K(ret),
              "Inc Meta TYPe", get_meta_ckpt_string(inc_meta_type),
              KP(inc_meta),
              KP(inc_meta_ckpts_[inc_meta_type]),
              K(ls_));
  }
  inc_meta_ckpts_[inc_meta_type] = nullptr;
}

const char *ObSSIncMetaCheckpoint::get_meta_ckpt_string(int64_t type_idx)
{
  STATIC_ASSERT(int64_t(SSIncMetaType::GC_HANDLER) == 0, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::MAX_COMMON_META_TYPE) == 1, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::GTS) == 2, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::TX_IDS) == 3, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::MAX_ALL_SYS_LS_META_TYPE) == 4, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::DAS_IDS) == 5, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::MAX_NON_SSLOG_LS_META_TYPE) == 6, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::SSLOG_GTS) == 7, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::SSLOG_UID) == 8, "invalid ss inc meta type");
  STATIC_ASSERT(int64_t(SSIncMetaType::MAX_INC_META_TYPE) == 9, "invalid ss inc meta type");
  const static char META_TYPE_TO_CHAR[SSIncMetaType::MAX_INC_META_TYPE + 1][32] = {"GC_HANDLER",
                                                                                   "MAX_COMMON_META_TYPE",
                                                                                   "GTS",
                                                                                   "TX_IDS",
                                                                                   "MAX_ALL_SYS_LS_META_TYPE",
                                                                                   "DAS_IDS",
                                                                                   "MAX_NON_SSLOG_LS_META_TYPE",
                                                                                   "SSLOG_GTS",
                                                                                   "SSLOG_UID",
                                                                                   "MAX_INC_META_TYPE"};
  return META_TYPE_TO_CHAR[type_idx];
}

}  // namespace storage
}  // namespace oceanbase
