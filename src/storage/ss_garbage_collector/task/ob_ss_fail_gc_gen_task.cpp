/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_fail_gc_gen_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"

namespace oceanbase
{
namespace storage
{

using namespace common;

ObSSFailGCGenTask::ObSSFailGCGenTask(volatile bool &is_stopped)
  : ObSSGCDagTaskBase(
      is_stopped,
      static_cast<int64_t>(ObSSFailGCDag::DAG_TYPE),
      "SUBMIT_FAIL_GC_DAG")
{
}

ObSSFailGCGenTask::~ObSSFailGCGenTask()
{
  destroy();
}

int ObSSFailGCGenTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSGCDagTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init fail gc gen task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSFailGCGenTask::start(const int tg_id)
{
  return ObSSGCDagTaskBase::start(tg_id);
}

void ObSSFailGCGenTask::stop()
{
  ObSSGCDagTaskBase::stop();
}

void ObSSFailGCGenTask::destroy()
{
  ObSSGCDagTaskBase::destroy();
}

void ObSSFailGCGenTask::runTimerTask()
{
  ObSSGCDagTaskBase::runTimerTask();
}

int ObSSFailGCGenTask::try_schedule_fail_gc_dag_()
{
  return try_schedule_gc_dag_();
}

int ObSSFailGCGenTask::check_can_schedule_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_fail_gc_dag_(can_schedule);
}

int ObSSFailGCGenTask::check_can_schedule_fail_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_gc_dag_common_(can_schedule);
}

int ObSSFailGCGenTask::submit_gc_dag_(const int64_t check_ts)
{
  return prepare_and_submit_typed_gc_dag_<ObSSFailGCGenTask, ObSSFailGCDagParam>(
      check_ts, this, &ObSSFailGCGenTask::submit_fail_gc_dag_);
}

int ObSSFailGCGenTask::submit_fail_gc_dag_(const ObSSFailGCDagParam &param)
{
  return submit_typed_gc_dag_<ObSSFailGCDag>(param);
}

int ObSSFailGCGenTask::check_gc_dag_exist_(bool &exist)
{
  return check_fail_gc_dag_exist_(exist);
}

int ObSSFailGCGenTask::check_fail_gc_dag_exist_(bool &exist)
{
  return check_typed_gc_dag_exist_<ObSSFailGCDag>(exist);
}

void ObSSFailGCGenTask::reset_dag_schedule_controller_()
{
  ObSSGCDagTaskBase::reset_dag_schedule_controller_();
}

void ObSSFailGCGenTask::commit_dag_schedule_start_time_(const int64_t start_ts)
{
  ObSSGCDagTaskBase::commit_dag_schedule_start_time_(start_ts);
}

} // namespace storage
} // namespace oceanbase
