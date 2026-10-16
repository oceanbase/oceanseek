/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"
#include "lib/thread/thread_mgr.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

ObSSMacroCacheTaskBase::ObSSMacroCacheTaskBase(volatile bool &is_stopped)
    : is_inited_(false), is_stopped_(is_stopped), run_cnt_(0)
{
}

ObSSMacroCacheTaskBase::~ObSSMacroCacheTaskBase()
{
}

int ObSSMacroCacheTaskBase::check_init_and_stop_() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObSSMacroCacheTaskBase not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    OB_LOG(WARN, "ObSSMacroCacheTaskBase is stopped", KR(ret), K(is_stopped_));
  }
  return ret;
}

int ObSSMacroCacheTaskBase::schedule_task_(
    ObTimerTask &task, const int tg_id, const int64_t interval_us, const bool repeat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(INVALID_TG_ID == tg_id || interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id), K(interval_us));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, task, interval_us, repeat))) {
    LOG_WARN("fail to schedule macro cache task", KR(ret), K(task), K(tg_id), K(interval_us), K(repeat));
  } else {
    LOG_INFO("succ to schedule macro cache task", K(task), K(tg_id), K(interval_us), K(repeat));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase