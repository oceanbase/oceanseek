/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_persist_micro_meta_task.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObSSPersistMicroMetaTask::ObSSPersistMicroMetaTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheBaseTask(), runner_(runner), persist_meta_op_()
{}

void ObSSPersistMicroMetaTask::destroy()
{
  persist_meta_op_.destroy();
  ObSSMicroCacheBaseTask::destroy();
}

int ObSSPersistMicroMetaTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseTask::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(persist_meta_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init persist_meta_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSPersistMicroMetaTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(persist_meta_op_.execute_checkpoint())) {
    LOG_WARN("fail to execute persist_meta_op", KR(ret));
  }

  if (OB_TMP_FAIL(runner_.schedule_persist_meta_task(cur_interval_us_))) {
    LOG_WARN("fail to reschedule persist micro_meta task", KR(ret), K(tmp_ret), K_(cur_interval_us), K_(ori_interval_us));
  }
}

} // storage
} // oceanbase
