/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/worker.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_exec_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

namespace oceanbase
{
namespace storage
{

ObSSMarkSweepGCExecTask::ObSSMarkSweepGCExecTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSMarkSweepGCExecTask::~ObSSMarkSweepGCExecTask()
{
  destroy();
}

int ObSSMarkSweepGCExecTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mark sweep gc exec task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepGCExecTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule mark sweep gc exec task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSMarkSweepGCExecTask::destroy()
{
  reset_task_state_();
}

void ObSSMarkSweepGCExecTask::get_gc_exec_interval_us_(int64_t &exec_interval_us) const
{
  exec_interval_us = DEFAULT_GC_EXEC_INTERVAL_US;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    LOG_WARN_RET(OB_INVALID_CONFIG, "tenant config is invalid, fallback to default",
        K_(tenant_id), K(exec_interval_us));
  } else {
    exec_interval_us = tenant_config->_shared_gc_task_scan_interval;
  }
}

void ObSSMarkSweepGCExecTask::runTimerTask()
{
  lib::ErrsimModuleGuard errsim_guard(common::ObErrsimModuleType::ERRSIM_MODULE_SS_GC);
  int ret = OB_SUCCESS;
  bool is_need = false;
  if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do mark sweep gc exec work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_submit_dag_task_())) {
    LOG_WARN("fail to submit mark sweep gc dag task", KR(ret), K_(tenant_id));
  }
}

int ObSSMarkSweepGCExecTask::do_submit_dag_task_()
{
  int ret = OB_SUCCESS;
  ObSharedGCControlInfo control_info;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCControlOperator::read_row(
        *GCTX.sql_proxy_, tenant_id_, control_info))) {
    LOG_WARN("fail to read control info", KR(ret), K_(tenant_id));
  } else {
    ObSSGCExecDagInitParam param;
    param.tenant_id_ = tenant_id_;
    param.gc_type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
    param.allow_gc_round_id_ = control_info.allow_gc_round_id_;
    if (OB_FAIL(ObSSGCUtil::create_and_add_dag(param))) {
      LOG_WARN("fail to create and add mark sweep dag", KR(ret), K_(tenant_id), K(param));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
