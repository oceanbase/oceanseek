/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_preread_map_clear_task.h"
#include "lib/thread/thread_mgr.h"
#include "storage/shared_storage/ob_ss_preread_cache_manager.h"

namespace oceanbase
{
namespace storage
{

ObSSPrereadMapClearTask::ObSSPrereadMapClearTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), preread_cache_mgr_(nullptr)
{
}

ObSSPrereadMapClearTask::~ObSSPrereadMapClearTask()
{
  destroy();
}

int ObSSPrereadMapClearTask::init(ObPrereadCacheManager *preread_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init preread map clear task twice", KR(ret));
  } else if (OB_ISNULL(preread_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(preread_cache_mgr));
  } else {
    preread_cache_mgr_ = preread_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init preread map clear task", KR(ret));
  return ret;
}

int ObSSPrereadMapClearTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread map clear task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule preread map clear task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule preread map clear task", K(tg_id));
  }
  return ret;
}

void ObSSPrereadMapClearTask::destroy()
{
  preread_cache_mgr_ = nullptr;
  is_stopped_ = true;
  is_inited_ = false;
}

void ObSSPrereadMapClearTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread map clear task is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    LOG_INFO("preread map clear task is stopped");
  } else if (OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("preread cache mgr should not be null", KR(ret));
  } else if (OB_FAIL(preread_cache_mgr_->do_preread_map_clear_work())) {
    LOG_WARN("fail to do preread map clear work", KR(ret));
  } 
}

} // namespace storage
} // namespace oceanbase
