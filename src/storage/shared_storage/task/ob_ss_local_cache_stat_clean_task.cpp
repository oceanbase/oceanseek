/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/task/ob_ss_local_cache_stat_clean_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

ObSSLocalCacheStatCleanTask::ObSSLocalCacheStatCleanTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObSSLocalCacheStatCleanTask::~ObSSLocalCacheStatCleanTask()
{
  destroy();
}

int ObSSLocalCacheStatCleanTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("local cache stat clean task has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  LOG_INFO("finish to init local cache tablet stat clean task", KR(ret));
  return ret;
}

int ObSSLocalCacheStatCleanTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache tablet stat clean task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule local cache tablet stat clean task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule local cache tablet stat clean task", K(tg_id));
  }
  return ret;
}

void ObSSLocalCacheStatCleanTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObSSLocalCacheStatCleanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache stat clean task is not inited", KR(ret));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service should not be null", KR(ret));
  } else if (OB_FAIL(local_cache_service->do_local_cache_tablet_stat_map_clean_work())) {
    LOG_WARN("fail to do local cache tablet stat map clean work", KR(ret));
  }
}

} // storage
} // oceanbase
