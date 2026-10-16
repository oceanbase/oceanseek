/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSharedGCBaseTask::ObSharedGCBaseTask(const ObITask::ObITaskType task_type)
  : ObITask(task_type)
{
}

int ObSharedGCBaseTask::check_if_stopped_or_failed() const
{
  int ret = OB_SUCCESS;
  if (TC_REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { // check every 10s
    ObTenantDagScheduler *dag_scheduler = nullptr;
    ObIDag *dag = get_dag();
    if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get ObTenantDagScheduler from MTL", KR(ret));
    } else if (dag_scheduler->has_set_stop()) {
      ret = OB_SERVICE_STOPPED;
      LOG_INFO("tenant dag scheduler has been stopped, stop dag task", KR(ret));
    } else if (OB_NOT_NULL(dag)
        && OB_FAIL(ObSSGCDagBase::check_if_stopped_or_failed(*dag))) {
      LOG_INFO("stop shared storage gc dag task", KR(ret),
          "dag_type", dag->get_type(), KP(dag));
    }
  }
  return ret;
}

// TODO(binifei.bnf): Accumulate experience on non-retriable errno
bool ObSharedGCBaseTask::can_retry(const int ret) const
{
  bool bret = false;
  switch (ret) {
    case OB_NOT_INIT:
    case OB_INIT_TWICE:
    case OB_ERR_UNEXPECTED:
    case OB_INVALID_ARGUMENT:
    case OB_CANCELED:
    case OB_SERVICE_STOPPED:
    case OB_OBJECT_NOT_EXIST:
      bret = false;
      break;
    default:
      bret = true;
      break;
  }
  return bret;
}

} // namespace storage
} // namespace oceanbase
