/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_ls_meta_gc_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_ls_meta_gc_dag.h"

namespace oceanbase
{
namespace storage
{

using namespace common;

ObSSLSMetaGCTask::ObSSLSMetaGCTask(volatile bool &is_stopped)
  : ObSSGCDagTaskBase(
      is_stopped,
      static_cast<int64_t>(ObSSLSMetaGCDag::DAG_TYPE),
      "SUBMIT_LS_META_GC_DAG")
{
}

ObSSLSMetaGCTask::~ObSSLSMetaGCTask()
{
  destroy();
}

int ObSSLSMetaGCTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSGCDagTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init ls meta gc task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSLSMetaGCTask::start(const int tg_id)
{
  return ObSSGCDagTaskBase::start(tg_id);
}

void ObSSLSMetaGCTask::stop()
{
  ObSSGCDagTaskBase::stop();
}

void ObSSLSMetaGCTask::destroy()
{
  ObSSGCDagTaskBase::destroy();
}

void ObSSLSMetaGCTask::runTimerTask()
{
  ObSSGCDagTaskBase::runTimerTask();
}

int ObSSLSMetaGCTask::try_schedule_ls_meta_gc_dag_()
{
  return try_schedule_gc_dag_();
}

int ObSSLSMetaGCTask::check_can_schedule_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_ls_meta_gc_dag_(can_schedule);
}

int ObSSLSMetaGCTask::check_can_schedule_ls_meta_gc_dag_(bool &can_schedule) const
{
  return check_can_schedule_gc_dag_common_(can_schedule);
}

int ObSSLSMetaGCTask::submit_gc_dag_(const int64_t check_ts)
{
  return prepare_and_submit_typed_gc_dag_<ObSSLSMetaGCTask, ObSSLSMetaGCDagParam>(
      check_ts, this, &ObSSLSMetaGCTask::submit_ls_meta_gc_dag_);
}

int ObSSLSMetaGCTask::submit_ls_meta_gc_dag_(const ObSSLSMetaGCDagParam &param)
{
  return submit_typed_gc_dag_<ObSSLSMetaGCDag>(param);
}

int ObSSLSMetaGCTask::check_gc_dag_exist_(bool &exist)
{
  return check_ls_meta_gc_dag_exist_(exist);
}

int ObSSLSMetaGCTask::check_ls_meta_gc_dag_exist_(bool &exist)
{
  return check_typed_gc_dag_exist_<ObSSLSMetaGCDag>(exist);
}

void ObSSLSMetaGCTask::reset_dag_schedule_controller_()
{
  ObSSGCDagTaskBase::reset_dag_schedule_controller_();
}

void ObSSLSMetaGCTask::commit_dag_schedule_start_time_(const int64_t start_ts)
{
  ObSSGCDagTaskBase::commit_dag_schedule_start_time_(start_ts);
}

} // namespace storage
} // namespace oceanbase
