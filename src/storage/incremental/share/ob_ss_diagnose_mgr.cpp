/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_diagnose_mgr.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::share;
namespace oceanbase 
{
namespace storage
{
/*
 * ObSSDiagnoseInfoMgr implement
 * */
int ObSSDiagnoseInfoMgr::mtl_init(ObSSDiagnoseInfoMgr *&diagnose_tablet_mgr)
{
  return diagnose_tablet_mgr->init();
}

ObSSDiagnoseInfoMgr::ObSSDiagnoseInfoMgr()
  : is_inited_(false),
    diagnose_map_lock_(common::ObLatchIds::OB_SS_DIAGNOSE_MGR_LOCK),
    diagnose_tablet_map_()
{
  memset(idx_, 0, sizeof(idx_));
  memset(diagnose_infos_, 0, sizeof(ObSSDiagnoseInfo *) * ObSSDiagnoseInfo::DIA_TYPE_MAX);
}

int ObSSDiagnoseInfoMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSDiagnoseInfoMgr has already been initiated", K(ret));
  } else if (OB_FAIL(diagnose_tablet_map_.create(MAX_DIAGNOSE_TABLET_BUCKET_NUM, "DiaTabletMap", "DiaTabletNode", MTL_ID()))) {
    LOG_WARN("Fail to create diagnose tablet map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObSSDiagnoseInfo::DIA_TYPE_MAX; i++) {
      void *ptr = NULL;
      if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSDiagnoseInfo) * MAX_DIAGNOSE_CNT[i], "SSDiagnoseInfo"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(i));
      } else {
        memset(ptr, 0, sizeof(ObSSDiagnoseInfo) * MAX_DIAGNOSE_CNT[i]);
        diagnose_infos_[i] = (ObSSDiagnoseInfo *)ptr;
      }
    }
  } 
  
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

void ObSSDiagnoseInfoMgr::destroy()
{
  if (diagnose_tablet_map_.created()) {
    (void)diagnose_tablet_map_.destroy();
  }
  for (int64_t i = 0; i < ObSSDiagnoseInfo::DIA_TYPE_MAX; i++) {
    if (OB_NOT_NULL(diagnose_infos_[i])) {
      ob_free(diagnose_infos_[i]);
    }
  }
  memset(diagnose_infos_, 0, sizeof(ObSSDiagnoseInfo *) * ObSSDiagnoseInfo::DIA_TYPE_MAX);
  is_inited_ = false;
}

// for diagnose
int ObSSDiagnoseInfoMgr::add_diagnose_tablet(
  const share::ObLSID &ls_id, 
  const ObTabletID &tablet_id, 
  const ObSSDiagnoseInfo::ObDiagnoseType type)
{
  LOG_TRACE("add_diagnose_tablet", K(ls_id), K(tablet_id), K(type));
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSDiagnoseInfoMgr is not init", K(ret));
  } else {
    if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() ||
        type <= ObSSDiagnoseInfo::DIA_TYPE_INVALID || type >= ObSSDiagnoseInfo::DIA_TYPE_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(type));
    } else {
      lib::ObMutexGuard guard(diagnose_map_lock_);
      ObSSDiagnoseTablet diagnose_tablet(ls_id, tablet_id);
      int64_t flag = 0;
      if (OB_FAIL(diagnose_tablet_map_.get_refactored(diagnose_tablet, flag))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get diagnose tablet from map", K(ret), K(diagnose_tablet));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (ObSSDiagnoseTablet::is_flagged(flag, type)) {
        ret = OB_HASH_EXIST;
      }
      if (OB_SUCC(ret)) {
        ObSSDiagnoseTablet::set_flag(flag, type);
        if (OB_FAIL(diagnose_tablet_map_.set_refactored(diagnose_tablet, flag, 1))) {
          LOG_WARN("fail to add diagnose tablet into map", K(ret), K(diagnose_tablet), K(flag));
        }
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSDiagnoseInfoMgr::get_diagnose_tablets(ObIArray<ObSSDiagnoseTablet> &diagnose_tablets)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSDiagnoseInfoMgr is not init", K(ret));
  } else {
    lib::ObMutexGuard guard(diagnose_map_lock_);
    for (DiagnoseTabletMap::iterator iter = diagnose_tablet_map_.begin(); 
        OB_SUCC(ret) && iter != diagnose_tablet_map_.end(); ++iter) {
      if (OB_FAIL(diagnose_tablets.push_back(iter->first))) {
        LOG_WARN("fail to get diagnose tablet", K(ret));
      }
    }
  }
  return ret;
}

int ObSSDiagnoseInfoMgr::delete_diagnose_tablet(
  const share::ObLSID &ls_id, 
  const ObTabletID &tablet_id,
  const ObSSDiagnoseInfo::ObDiagnoseType type)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("delete_diagnose_tablet", K(ls_id), K(tablet_id), K(type));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSDiagnoseInfoMgr is not init", K(ret));
  } else {
    if (!ls_id.is_valid() || !tablet_id.is_valid() 
        || type <= ObSSDiagnoseInfo::DIA_TYPE_INVALID
        || type >= ObSSDiagnoseInfo::DIA_TYPE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(type));
    } else {
      lib::ObMutexGuard guard(diagnose_map_lock_);
      ObSSDiagnoseTablet diagnose_tablet(ls_id, tablet_id);
      int64_t flag = 0;
      if (OB_FAIL(diagnose_tablet_map_.get_refactored(diagnose_tablet, flag))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get diagnose tablet from map", K(ret), K(diagnose_tablet));
        }
      } else {
        ObSSDiagnoseTablet::del_flag(flag, type);
        if (0 == flag) {
          if (OB_FAIL(diagnose_tablet_map_.erase_refactored(diagnose_tablet))) {
            LOG_WARN("fail to delete diagnose tablet", K(ret), K(diagnose_tablet));
          }
        } else if (OB_FAIL(diagnose_tablet_map_.set_refactored(diagnose_tablet, flag, 1/*overwrite*/))) {
          LOG_WARN("fail to add diagnose tablet into map", K(ret), K(diagnose_tablet), K(flag));
        }
      }
    }
  }
  return ret;
}

void ObSSDiagnoseInfoMgr::remove_diagnose_tablets(ObIArray<ObSSDiagnoseTablet> &tablets)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN_RET(tmp_ret, "ObSSDiagnoseInfoMgr is not init", K(tmp_ret));
  } else {
    lib::ObMutexGuard guard(diagnose_map_lock_);
    for (int64_t i = 0; i < tablets.count(); ++i) {
      const ObSSDiagnoseTablet &tablet = tablets.at(i);
      if (OB_TMP_FAIL(diagnose_tablet_map_.erase_refactored(tablet))) {
        LOG_WARN_RET(tmp_ret, "fail to delete diagnose tablet", K(tmp_ret), K(tablet));
      }
    }
  }
}

void ObSSDiagnoseInfoMgr::get_diagnose_info(ObSSDiagnoseInfo::ObDiagnoseType type,
                                            ObSSDiagnoseInfo *info_array,
                                            int64_t &pos,
                                            const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSDiagnoseInfoMgr is not init", K(ret));
  } else if (type <= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_INVALID || type >= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_MAX
      || OB_ISNULL(info_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(type), KP(info_array));
  } else {
    ByteLockGuard guard(diagnose_lock_[type].lock_);
    const int64_t type_max_cnt = MAX_DIAGNOSE_CNT[type];
    ObSSDiagnoseInfo *diagnose_infos = diagnose_infos_[type];
    for (int64_t i = 0; (i < type_max_cnt) && diagnose_infos && diagnose_infos[i].is_valid(); i++) {
      ObSSDiagnoseInfo &info = diagnose_infos[i];
      if (pos < max_cnt && need_diagnose_info_(type, &info)) {
        ObSSDiagnoseInfo &dest = info_array[pos++];
        dest.copy(info);
      }
    }
  }
}

bool ObSSDiagnoseInfoMgr::need_diagnose_info_(const int64_t type, ObSSDiagnoseInfo *info)
{
  int ret = OB_SUCCESS;
  bool need_diag = true;
  if (type <= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_INVALID || type >= ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_MAX
      || OB_ISNULL(info)) {
    need_diag = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid diagnose type", K(ret), K(type), KP(info));
  } else if (info->timestamp_ == 0) {
    // diagnosis info has not been set yet
    need_diag = false;
  } else {
    switch(type) {
      case ObSSDiagnoseInfo::ObDiagnoseType::DIA_TYPE_META_READ_WRITE: {
        need_diag = false;
        const int64_t THRESHOLD = 600 * 1000 * 1000; // only the newest 10min diagnose info usefull.
        int64_t curr_time = ObTimeUtility::current_time();
        if (info->timestamp_ != 0
            && curr_time - info->timestamp_ <= THRESHOLD) {
          need_diag = true;
        }
        break;
      }
      default: {
        need_diag = true;
      }
    }
  }
  return need_diag;
}

}
}
