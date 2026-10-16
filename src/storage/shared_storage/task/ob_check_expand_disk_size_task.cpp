/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_check_expand_disk_size_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "observer/omt/ob_tenant.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCheckExpandDiskTask::ObCheckExpandDiskTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(INVALID_TG_ID)
{
}

int ObCheckExpandDiskTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckExpandDiskTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init check expand disk size task", K(tenant_id));
  }
  return ret;
}

int ObCheckExpandDiskTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 1000L * 1000L; // 1s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("check expand disk size task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObCheckExpandDiskTask", KR(ret), K_(tenant_id), K(tg_id));
  } else {
    LOG_INFO("succ to start check expand disk size task", K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObCheckExpandDiskTask::destroy()
{
  tg_id_ = INVALID_TG_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObCheckExpandDiskTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    if (OB_TMP_FAIL(check_expand_disk_size_())) {
      LOG_WARN("fail to check expand disk size", KR(ret), KR(tmp_ret), K(tenant_id_), K(tg_id_));
    }
    
    if (OB_TMP_FAIL(check_shrink_disk_size_())) {
      LOG_WARN("fail to check shrink disk size", KR(ret), KR(tmp_ret), K(tenant_id_), K(tg_id_));
    }

    if (OB_TMP_FAIL(sync_tenant_data_disk_size_())) {
      LOG_WARN("fail to sync tenant unit actual data disk size",
          KR(ret), KR(tmp_ret), K(tenant_id_), K(tg_id_));
    }

    if (OB_TMP_FAIL(sync_sys_tenant_data_disk_size_())) {
      LOG_WARN("fail to sync hidden sys data disk size",
          KR(ret), KR(tmp_ret), K(tenant_id_), K(tg_id_));
    }
  }
}

int ObCheckExpandDiskTask::check_expand_disk_size_() const
{
  int ret = OB_SUCCESS;
  bool need_expand = false;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K(tenant_id_));
  } else if (OB_FAIL(tnt_disk_space_mgr->need_expand_disk_size(need_expand))) {
    LOG_WARN("fail to check expand data disk size",
        KR(ret), K(tenant_id_), KPC(tnt_disk_space_mgr));
  } else if (!need_expand) {
    // do nothing, cannot expand disk size
  } else if (OB_FAIL(tnt_disk_space_mgr->expand_data_disk_size(
      ObDiskSpaceManager::DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH))) {
    LOG_WARN("fail to auto expand data disk size",
        KR(ret), K(tenant_id_), KPC(tnt_disk_space_mgr));
  }
  return ret;
}

int ObCheckExpandDiskTask::check_shrink_disk_size_() const
{
  int ret = OB_SUCCESS;
  bool need_shrink = false;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K(tenant_id_));
  } else if (OB_FAIL(tnt_disk_space_mgr->need_shrink_disk_size(need_shrink))) {
    LOG_WARN("fail to check shrink data disk size",
        KR(ret), K(tenant_id_), KPC(tnt_disk_space_mgr));
  } else if (!need_shrink) {
    // do nothing
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), KP(macro_cache_mgr), K(tenant_id_));
  } else {
    macro_cache_mgr->set_fg_evict_trigger();
    LOG_INFO("need shrink, set fg evict trigger", K(tenant_id_), KPC(macro_cache_mgr));
    int64_t macro_cache_cur_min_shrinkable_size = 0;
    int64_t cur_available_shrink_size = 0;
    if (OB_FAIL(macro_cache_mgr->cal_cur_min_shrinkable_size(macro_cache_cur_min_shrinkable_size))) {
      LOG_WARN("fail to cal macro cache min shrinkable size",
          KR(ret), K(tenant_id_), KPC(macro_cache_mgr));
    } else if (OB_FAIL(tnt_disk_space_mgr->cal_cur_available_shrink_size(
        macro_cache_cur_min_shrinkable_size, cur_available_shrink_size))) {
      LOG_WARN("fail to cal cur available shrink size",
          KR(ret), K(tenant_id_), K(macro_cache_cur_min_shrinkable_size), KPC(tnt_disk_space_mgr));
    } else if (cur_available_shrink_size <= 0) {
      // do nothing, cannot shrink disk size
    } else if (OB_FAIL(tnt_disk_space_mgr->shrink_data_disk_size(cur_available_shrink_size))) {
      LOG_WARN("fail to shrink data disk size",
          KR(ret), K(tenant_id_), K(cur_available_shrink_size), KPC(tnt_disk_space_mgr));
    }
  }
  return ret;
}

int ObCheckExpandDiskTask::sync_tenant_data_disk_size_() const
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  share::ObUnitInfoGetter::ObTenantConfig unit;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_unit(tenant_id_, unit))) {
    LOG_WARN("fail to get tenant unit", KR(ret), K(tenant_id_));
  } else {
    const int64_t actual_data_disk_size = tnt_disk_space_mgr->get_total_disk_size();
    if (OB_UNLIKELY(actual_data_disk_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("actual data disk size is unexpected",
          KR(ret), K(tenant_id_), K(actual_data_disk_size));
    } else if (actual_data_disk_size != unit.actual_data_disk_size_) {
      if (OB_FAIL(GCTX.omt_->update_tenant_actual_disk_size(tenant_id_, actual_data_disk_size))) {
        LOG_WARN("fail to update tenant actual data disk size",
            KR(ret), K(tenant_id_), K(actual_data_disk_size), K(unit));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tnt_disk_space_mgr->set_config_data_disk_size(unit.config_.data_disk_size()))) {
      LOG_WARN("fail to set config data disk size",
          KR(ret), KR(tmp_ret), K(unit), KPC(tnt_disk_space_mgr));
    }

    if (OB_TMP_FAIL(tnt_disk_space_mgr->sync_tenant_pair_unallocated_disk_size())) {
      LOG_WARN("fail to sync tenant pair unallocated disk size",
          KR(ret), KR(tmp_ret), K(tenant_id_), KPC(tnt_disk_space_mgr));
    }
  }
  return ret;
}

// sys tenant is never dropped; it only switches between hidden and real.
// During that switch, tenant config regenerates actual_data_disk_size with a
// default value—persist the real size in OB_SERVER_DISK_SPACE_MGR and sync it
// here to keep the value accurate.
int ObCheckExpandDiskTask::sync_sys_tenant_data_disk_size_() const
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = nullptr;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K(is_inited_));
  } else if (!is_sys_tenant(tenant_id_)) {
    // do nothing, only sys tenant need sync data disk size
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id_, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is null", KR(ret), KP(tenant));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.update_sys_tenant_data_disk_size(
        tnt_disk_space_mgr->get_total_disk_size(), tenant->is_hidden()))) {
      LOG_WARN("fail to update sys tenant data disk size",
          KR(ret), K(tenant_id_), KPC(tnt_disk_space_mgr));
    }
  }
  return ret;
}

} // storage
} // oceanbase
