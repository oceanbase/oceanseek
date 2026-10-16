/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_expire_task.h"
#include "lib/thread/thread_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{

ObSSMacroCacheExpireTask::ObSSMacroCacheExpireTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(nullptr), last_exec_timestamp_us_(0)
{
}

ObSSMacroCacheExpireTask::~ObSSMacroCacheExpireTask()
{
  destroy();
}

int ObSSMacroCacheExpireTask::init(ObSSMacroCacheMgr *macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init macro cache expire task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr));
  } else {
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init macro cache expire task", KR(ret));
  return ret;
}

int ObSSMacroCacheExpireTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache expire task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule macro cache expire task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule macro cache expire task", K(tg_id));
  }
  return ret;
}

void ObSSMacroCacheExpireTask::destroy()
{
}

void ObSSMacroCacheExpireTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_need = false;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (!macro_cache_mgr_->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache expire task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(check_if_need_do_expire_work(is_need))) {
    LOG_WARN("fail to check if need do expire work", KR(ret));
  } else if (is_need && OB_FAIL(macro_cache_mgr_->do_expire_work())) {
    LOG_WARN("fail to do expire work", KR(ret));
  }
}

int ObSSMacroCacheExpireTask::check_if_need_do_expire_work(bool &is_need)
{
  // no need to check is_inited_ and is_stopped_ again here
  int ret = OB_SUCCESS;
  is_need = false;
  if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else {
    int64_t expiration_time_us = macro_cache_mgr_->get_expiration_time_us();
    int64_t exec_interval_us = MIN(expiration_time_us, MAX_EXEC_INTERVAL_US);
    if ((ObTimeUtility::current_time_us() - last_exec_timestamp_us_) > exec_interval_us) {
      is_need = true;
      last_exec_timestamp_us_ = ObTimeUtility::current_time_us();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
