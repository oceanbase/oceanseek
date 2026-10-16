/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_evict_task.h"
#include "lib/thread/thread_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/shared_storage/ob_disk_space_manager.h"

namespace oceanbase
{
namespace storage
{

ObSSMacroCacheEvictTask::ObSSMacroCacheEvictTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(nullptr)
{
}

ObSSMacroCacheEvictTask::~ObSSMacroCacheEvictTask()
{
  destroy();
}

int ObSSMacroCacheEvictTask::init(ObSSMacroCacheMgr *macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init macro cache evict task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr));
  } else {
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init macro cache evict task", KR(ret));
  return ret;
}

int ObSSMacroCacheEvictTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache evict task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule macro cache evict task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule macro cache evict task", K(tg_id));
  }
  return ret;
}

void ObSSMacroCacheEvictTask::destroy()
{
}

void ObSSMacroCacheEvictTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (!OB_SERVER_DISK_SPACE_MGR.is_share_tenant_disk_space_quota_initialized(MTL_ID())) {
    LOG_INFO("tenant pair is not ready, skip macro cache evict task");
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (!macro_cache_mgr_->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache evict task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(macro_cache_mgr_->do_evict_work())) {
    LOG_WARN("fail to do evict work", KR(ret));
  }
}

} // namespace storage
} // namespace oceanbase
