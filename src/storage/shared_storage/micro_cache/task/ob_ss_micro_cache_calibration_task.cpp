/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_calibration_task.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

ObSSMicroCacheCalibrationTask::ObSSMicroCacheCalibrationTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheBaseTask(), runner_(runner), calibration_op_()
{
}

void ObSSMicroCacheCalibrationTask::destroy()
{
  calibration_op_.destroy();
  ObSSMicroCacheBaseTask::destroy();
}

int ObSSMicroCacheCalibrationTask::init(
    const uint64_t tenant_id,
    const int64_t interval_us,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseTask::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("failed to init base task", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(calibration_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("failed to init calibration op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObSSMicroCacheCalibrationTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(calibration_op_.execute())) {
    LOG_WARN("failed to execute calibration op", KR(ret));
  }

  if (OB_TMP_FAIL(runner_.schedule_calibration_task(cur_interval_us_))) {
    LOG_WARN("failed to schedule calibration task", KR(ret), K(tmp_ret), K_(cur_interval_us));
    tmp_ret = OB_SUCCESS;
  }
}

} // namespace storage
} // namespace oceanbase
