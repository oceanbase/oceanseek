/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_TASK_BASE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_TASK_BASE_H_

#include "lib/oblog/ob_log.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_schedule_controller.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCDagTaskBase : public ObSSGCTaskBase
{
public:
  ObSSGCDagTaskBase(
      volatile bool &is_stopped,
      const int64_t dag_type,
      const char *submit_event_name);
  virtual ~ObSSGCDagTaskBase();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  virtual void stop() override;
  void destroy();
  virtual void runTimerTask() override;

protected:
  virtual int check_can_schedule_gc_dag_(bool &can_schedule) const = 0;
  virtual int submit_gc_dag_(const int64_t check_ts) = 0;
  virtual int check_gc_dag_exist_(bool &exist) = 0;

  virtual int64_t get_gc_dag_schedule_check_time_us_() const;
  virtual int check_gc_dag_retry_state_(
      const int64_t check_ts,
      bool &has_pending_retry,
      bool &need_retry) const;
  virtual int check_need_schedule_dag_(bool &need_schedule, int64_t &check_ts);
  void commit_gc_dag_schedule_time_(const int64_t schedule_ts);
  int check_can_schedule_gc_dag_common_(bool &can_schedule) const;
  int try_schedule_gc_dag_();
  void reset_dag_schedule_controller_();
  void commit_dag_schedule_start_time_(const int64_t start_ts);

  template <typename TaskType, typename DagParamType>
  int prepare_and_submit_typed_gc_dag_(
      const int64_t check_ts,
      TaskType *task,
      int (TaskType::*submit_func)(const DagParamType &))
  {
    int ret = common::OB_SUCCESS;
    DagParamType param;
    if (OB_ISNULL(task) || OB_ISNULL(submit_func)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid gc dag submit callback", KR(ret),
          KP(task), K_(tenant_id), K_(dag_type));
    } else if (OB_FAIL(param.init(tenant_id_, check_ts))) {
      STORAGE_LOG(WARN, "fail to init gc dag param", KR(ret),
          K_(tenant_id), K_(dag_type), K(check_ts));
    } else if (OB_FAIL(check_init_and_stop_())) {
      STORAGE_LOG(INFO, "skip submitting gc dag after task runner is stopped",
          KR(ret), K_(tenant_id), K_(dag_type));
    } else {
      ++run_cnt_;
      ret = (task->*submit_func)(param);
    }
    return ret;
  }

  template <typename DagType, typename DagParamType>
  int submit_typed_gc_dag_(const DagParamType &param)
  {
    int ret = common::OB_SUCCESS;
    share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
    if (OB_FAIL(check_init_and_stop_())) {
      STORAGE_LOG(INFO, "skip adding gc dag after task runner is stopped",
          KR(ret), K_(tenant_id), K_(dag_type));
    } else if (OB_ISNULL(dag_scheduler)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tenant dag scheduler is null", KR(ret),
          K_(tenant_id), K_(dag_type));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag<DagType>(&param))) {
      // The caller classifies the result and decides whether to commit the anchor.
    }
    return ret;
  }

  template <typename DagType>
  int check_typed_gc_dag_exist_(bool &exist)
  {
    int ret = common::OB_SUCCESS;
    exist = true;
    share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
    if (OB_ISNULL(dag_scheduler)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tenant dag scheduler is null", KR(ret),
          K_(tenant_id), K_(dag_type));
    } else {
      DagType probe;
      bool unused_is_emergency = false;
      probe.set_tenant_id(tenant_id_);
      if (OB_FAIL(dag_scheduler->check_dag_exist(&probe, exist, unused_is_emergency))) {
        STORAGE_LOG(WARN, "fail to check gc dag exist", KR(ret),
            K_(tenant_id), K_(dag_type));
      }
    }
    return ret;
  }

private:
  static constexpr int64_t SCHEDULE_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  const int64_t dag_type_;
  const char *submit_event_name_;
  ObSSGCDagScheduleController dag_schedule_controller_;
  int64_t last_period_commit_time_us_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_TASK_BASE_H_
