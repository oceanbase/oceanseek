/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_lock_row_gc_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.h"

namespace oceanbase
{
namespace storage
{

using namespace common;

ObSSLockRowGCTask::ObSSLockRowGCTask(volatile bool &is_stopped)
  : ObSSGCDagTaskBase(
      is_stopped,
      static_cast<int64_t>(ObSSLockRowGCDag::DAG_TYPE),
      "SUBMIT_LOCK_ROW_GC_DAG")
{
}

ObSSLockRowGCTask::~ObSSLockRowGCTask()
{
  destroy();
}

int ObSSLockRowGCTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSGCDagTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init lock row gc task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSLockRowGCTask::start(const int tg_id)
{
  return ObSSGCDagTaskBase::start(tg_id);
}

void ObSSLockRowGCTask::stop()
{
  ObSSGCDagTaskBase::stop();
}

void ObSSLockRowGCTask::destroy()
{
  ObSSGCDagTaskBase::destroy();
}

void ObSSLockRowGCTask::runTimerTask()
{
  ObSSGCDagTaskBase::runTimerTask();
}

int ObSSLockRowGCTask::try_schedule_lock_row_gc_dag_()
{
  return try_schedule_gc_dag_();
}

int ObSSLockRowGCTask::check_can_schedule_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_lock_row_gc_dag_(can_schedule);
}

int ObSSLockRowGCTask::check_can_schedule_lock_row_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_gc_dag_common_(can_schedule);
}

int ObSSLockRowGCTask::submit_gc_dag_(const int64_t check_ts)
{
  return prepare_and_submit_typed_gc_dag_<ObSSLockRowGCTask, ObSSLockRowGCDagParam>(
      check_ts, this, &ObSSLockRowGCTask::submit_lock_row_gc_dag_);
}

int ObSSLockRowGCTask::submit_lock_row_gc_dag_(const ObSSLockRowGCDagParam &param)
{
  return submit_typed_gc_dag_<ObSSLockRowGCDag>(param);
}

int ObSSLockRowGCTask::check_gc_dag_exist_(bool &exist)
{
  return check_lock_row_gc_dag_exist_(exist);
}

int ObSSLockRowGCTask::check_lock_row_gc_dag_exist_(bool &exist)
{
  return check_typed_gc_dag_exist_<ObSSLockRowGCDag>(exist);
}

void ObSSLockRowGCTask::reset_dag_schedule_controller_()
{
  ObSSGCDagTaskBase::reset_dag_schedule_controller_();
}

void ObSSLockRowGCTask::commit_dag_schedule_start_time_(const int64_t start_ts)
{
  ObSSGCDagTaskBase::commit_dag_schedule_start_time_(start_ts);
}

} // namespace storage
} // namespace oceanbase
