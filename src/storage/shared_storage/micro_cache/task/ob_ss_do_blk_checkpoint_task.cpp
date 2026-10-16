/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_do_blk_checkpoint_task.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSDoBlkCheckpointTask------------------------------------------*/
ObSSDoBlkCheckpointTask::ObSSDoBlkCheckpointTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheBaseTask(), runner_(runner), ckpt_op_()
{}

void ObSSDoBlkCheckpointTask::destroy()
{
  ckpt_op_.destroy();
  ObSSMicroCacheBaseTask::destroy();
}

int ObSSDoBlkCheckpointTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseTask::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(ckpt_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init ckpt_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSDoBlkCheckpointTask::runTimerTask()
{

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ckpt_op_.execute_checkpoint())) {
    LOG_WARN("fail to execute blk_info checkpoint", KR(ret));
  }

  if (OB_TMP_FAIL(runner_.schedule_do_blk_ckpt_task(cur_interval_us_))) {
    LOG_WARN("fail to reschedule do_blk_ckpt task", KR(ret), K(tmp_ret), K_(cur_interval_us), K_(ori_interval_us));
  }
}

} // storage
} // oceanbase
