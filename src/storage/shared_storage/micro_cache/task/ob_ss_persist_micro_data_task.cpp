/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_persist_micro_data_task.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSPersistMicroDataTask-----------------------------------------*/
ObSSPersistMicroDataTask::ObSSPersistMicroDataTask(ObSSMicroCacheTaskRunner &runner)
    : ObSSMicroCacheBaseTask(), runner_(runner), persist_data_op_()
{}

void ObSSPersistMicroDataTask::destroy()
{
  persist_data_op_.destroy();
  ObSSMicroCacheBaseTask::destroy();
}

int ObSSPersistMicroDataTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseTask::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(persist_data_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init persist_data_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObSSPersistMicroDataTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(persist_data_op_.execute())) {
      LOG_WARN("fail to execute persist_data_op", KR(ret));
    }
    const int64_t alloc_blk_fail_cnt = persist_data_op_.get_alloc_blk_fail_cnt();
    try_adjust_task_interval(alloc_blk_fail_cnt);
  }
  
  if (OB_TMP_FAIL(runner_.schedule_persist_data_task(cur_interval_us_))) {
    LOG_WARN("fail to reschedule persist micro_data task", KR(ret), K(tmp_ret), K_(cur_interval_us));
  }
}

void ObSSPersistMicroDataTask::try_adjust_task_interval(const int64_t alloc_blk_fail_cnt)
{
  cur_interval_us_ = ori_interval_us_;
  if ((0 == alloc_blk_fail_cnt) && (task_ctx_->mem_blk_mgr_->get_sealed_mem_block_cnt() > 0)) {
    cur_interval_us_ = MIN(SS_PERSIST_DATA_TASK_FAST_INTERVAL_US, cur_interval_us_);
  }
}
} // storage
} // oceanbase
