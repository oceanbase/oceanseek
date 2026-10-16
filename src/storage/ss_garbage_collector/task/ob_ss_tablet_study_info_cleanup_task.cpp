/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_tablet_study_info_cleanup_task.h"
#include "lib/time/ob_time_utility.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"

namespace oceanbase
{
namespace storage
{

ObSSTabletStudyInfoCleanupTask::ObSSTabletStudyInfoCleanupTask(
    volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSTabletStudyInfoCleanupTask::~ObSSTabletStudyInfoCleanupTask()
{
  destroy();
}

int ObSSTabletStudyInfoCleanupTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet study info cleanup task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletStudyInfoCleanupTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule tablet study info cleanup task", KR(ret),
        K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSTabletStudyInfoCleanupTask::destroy()
{
  reset_task_state_();
}

void ObSSTabletStudyInfoCleanupTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  ObSSGCService *gc_service = nullptr;
  LOG_INFO("start to run tablet study info cleanup task", K_(tenant_id));
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check tablet study info cleanup task state", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(gc_service = MTL(ObSSGCService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc service is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(gc_service->cleanup_stale_local_tablet_study_info())) {
    LOG_WARN("fail to cleanup stale local tablet study info", KR(ret),
        K_(tenant_id));
  }
  LOG_INFO("finish to run tablet study info cleanup task", KR(ret),
      K_(tenant_id), "cost_us", time_piece.fly_us());
}

} // namespace storage
} // namespace oceanbase
