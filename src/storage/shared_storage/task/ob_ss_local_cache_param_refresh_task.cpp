/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/task/ob_ss_local_cache_param_refresh_task.h"
#include "lib/thread/thread_mgr.h"
#include "share/config/ob_config_helper.h"
#include "share/shared_storage/ob_local_cache_control_mode.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

ObSSLocalCacheParamRefreshTask::ObSSLocalCacheParamRefreshTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObSSLocalCacheParamRefreshTask::~ObSSLocalCacheParamRefreshTask()
{
  destroy();
}

int ObSSLocalCacheParamRefreshTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("local cache param refresh task has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
  }
  LOG_INFO("finish to init local cache param refresh task", KR(ret));
  return ret;
}

int ObSSLocalCacheParamRefreshTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache param refresh task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tg id", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule local cache param refresh task", KR(ret), K_(tenant_id), K(tg_id));
  }
  LOG_INFO("finish to start local cache param refresh task", KR(ret), K_(tenant_id), K(tg_id));
  return ret;
}

void ObSSLocalCacheParamRefreshTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObSSLocalCacheParamRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  ObSSLocalCacheControlMode mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache param refresh task is not init", KR(ret));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service is null", KR(ret));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant config", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(tenant_config->_local_cache_control.init_mode(mode))) {
      LOG_WARN("init mode failed", KR(ret));
    } else {
      local_cache_service->set_local_cache_control_mode(mode);

      const int64_t configured_macro_cache_min_admit_block_size =
          tenant_config->_macro_cache_min_admit_block_size;
      const int64_t macro_cache_min_admit_block_size = (configured_macro_cache_min_admit_block_size > 0
          ? configured_macro_cache_min_admit_block_size
          : ObSSLocalCacheParams::DEF_MACRO_CACHE_MIN_ADMIT_BLOCK_SIZE);
      local_cache_service->set_macro_cache_min_admit_block_size(macro_cache_min_admit_block_size);

      const int64_t threshold = tenant_config->_macro_cache_miss_threshold_for_prefetch;
      local_cache_service->set_miss_threshold_for_prefetch(threshold);

      const int64_t disk_space_cal_interval_us = tenant_config->_disk_space_calibration_time_interval;
      local_cache_service->set_disk_space_cal_interval_us(disk_space_cal_interval_us);

      ObSSLocalCacheEvictOldSSTablePolicy policy = ObSSLocalCacheEvictOldSSTablePolicy::MAX_POLICY;
      const ObString policy_str = tenant_config->_local_cache_evict_old_sstable_policy.get_value_string();
      if (OB_FAIL(get_evict_old_sstable_policy(policy_str, policy))) {
        LOG_WARN("fail to get evict old sstable policy", KR(ret), K(policy_str));
      } else {
        local_cache_service->set_evict_old_sstable_policy(policy);
      }
    }
  }
}

} // storage
} // oceanbase
