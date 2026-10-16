/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_fail_gc_exec_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"

namespace oceanbase
{
namespace storage
{

ObSSFailGCExecTask::ObSSFailGCExecTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSFailGCExecTask::~ObSSFailGCExecTask()
{
  destroy();
}

int ObSSFailGCExecTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc exec task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSFailGCExecTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule fail gc exec task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSFailGCExecTask::destroy()
{
  reset_task_state_();
}

void ObSSFailGCExecTask::get_gc_exec_interval_us_(int64_t &exec_interval_us) const
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

void ObSSFailGCExecTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_need = false;
  if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do fail gc exec work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_submit_dag_task_())) {
    LOG_WARN("fail to submit fail gc dag task", KR(ret), K_(tenant_id));
  }
}

int ObSSFailGCExecTask::do_submit_dag_task_()
{
  int ret = OB_SUCCESS;
  // The sswriter gate is enforced upstream in ObSSGCTaskBase::check_if_need_do_gc_work,
  // so this method runs only on the GC executor leader.
  // FailGC ignores allow_gc_round_id (iterator-internal), but we pass 0 here
  // rather than INT64_MAX to avoid a sentinel value.
  ObSSGCExecDagInitParam param;
  param.tenant_id_ = tenant_id_;
  param.gc_type_ = ObSharedGCTaskType::FAIL_GC;
  param.allow_gc_round_id_ = 0;
  if (OB_FAIL(ObSSGCUtil::create_and_add_dag(param))) {
    LOG_WARN("fail to create and add fail gc dag", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("fail_gc dag scheduled", K_(tenant_id), K_(run_cnt));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
