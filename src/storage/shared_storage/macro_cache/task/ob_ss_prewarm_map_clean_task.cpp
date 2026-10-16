/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_prewarm_map_clean_task.h"
#include "lib/thread/thread_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{

ObSSPrewarmMapCleanTask::ObSSPrewarmMapCleanTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), local_cache_service_(nullptr)
{
}

ObSSPrewarmMapCleanTask::~ObSSPrewarmMapCleanTask()
{
  destroy();
}

int ObSSPrewarmMapCleanTask::init(ObSSLocalCacheService *local_cache_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init prewarm map clean task twice", KR(ret));
  } else if (OB_ISNULL(local_cache_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(local_cache_service));
  } else {
    local_cache_service_ = local_cache_service;
    is_inited_ = true;
  }
  LOG_INFO("finish to init prewarm map clean task", KR(ret));
  return ret;
}

int ObSSPrewarmMapCleanTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("prewarm map clean task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule prewarm map clean task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule prewarm map clean task", K(tg_id));
  }
  return ret;
}

void ObSSPrewarmMapCleanTask::destroy()
{
  local_cache_service_ = nullptr;
  is_stopped_ = true;
  is_inited_ = false;
}

void ObSSPrewarmMapCleanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("prewarm map clean task is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    LOG_INFO("prewarm map clean task is stopped");
  } else if (OB_ISNULL(local_cache_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (OB_FAIL(local_cache_service_->do_prewarm_map_clean_work())) {
    LOG_WARN("fail to do prewarm map clean work", KR(ret));
  }
}

} // namespace storage
} // namespace oceanbase
