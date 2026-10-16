/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_release_cache_task.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSReleaseCacheTask------------------------------------------*/
ObSSReleaseCacheTask::ObSSReleaseCacheTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheBaseTask(), runner_(runner), evict_op_(), reorganize_op_()
{}

void ObSSReleaseCacheTask::destroy()
{
  evict_op_.destroy();
  reorganize_op_.destroy();
  ObSSMicroCacheBaseTask::destroy();
}

int ObSSReleaseCacheTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseTask::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(evict_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init evict_op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(reorganize_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init reorganize_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSReleaseCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(evict_op_.execute_eviction())) {
      LOG_WARN("fail to execute eviction", KR(ret));
    } else if (OB_FAIL(reorganize_op_.execute_reorganization())) {
      LOG_WARN("fail to execute reorganization", KR(ret));
    }
    
    clear_for_next_round();
  }

  try_adjust_task_interval();
  if (OB_TMP_FAIL(runner_.schedule_release_cache_task(cur_interval_us_))) {
    LOG_WARN("fail to reschedule release_cache task", KR(ret), K(tmp_ret), K_(ori_interval_us), K_(cur_interval_us));
  }
}

void ObSSReleaseCacheTask::clear_for_next_round()
{
  evict_op_.clear_for_next_round();
  reorganize_op_.clear_for_next_round();
}


void ObSSReleaseCacheTask::try_adjust_task_interval()
{
  cur_interval_us_ = ori_interval_us_;
  if (IS_INIT) {
    const bool need_adjust = task_ctx_->micro_meta_mgr_->get_arc_info().need_eviction();
    if (need_adjust) {
      cur_interval_us_ /= 10;
    }
  }
}

} // storage
} // oceanbase
