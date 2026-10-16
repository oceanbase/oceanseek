/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SHARED_GC_BASE_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SHARED_GC_BASE_TASK_H_

#include "lib/oblog/ob_log.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{

// ====================================================================================
// BaseTask - shared helpers for shared storage GC DAG task types.
// ====================================================================================
class ObSharedGCBaseTask : public share::ObITask
{
public:
  static constexpr int64_t CTRL_SCAN_PAGE_SIZE = 500;
  static constexpr int64_t EXECUTOR_BATCH_SIZE = 128;

  explicit ObSharedGCBaseTask(const share::ObITask::ObITaskType task_type);
  virtual ~ObSharedGCBaseTask() = default;

private:
  struct NoneOpFn {
    int operator()() { return common::OB_SUCCESS; }
  };

protected:
  bool can_retry(const int ret) const;
  // Returns OB_SERVICE_STOPPED if ObTenantDagScheduler has stopped,
  // or OB_CANCELED if the dag has already failed (e.g. a sibling task failed).
  int check_if_stopped_or_failed() const;
  template <typename WorkFn, typename OnRetryFn>
  int execute_with_retry(WorkFn &work_fn, OnRetryFn &on_retry_fn, int64_t &retry_cnt)
  {
    int ret = common::OB_SUCCESS;
    const int64_t MAX_TASK_RETRY_TIMES = 3;
    retry_cnt = 0;
    bool need_retry = false;
    do {
      need_retry = false;
      ret = common::OB_SUCCESS;
      if (OB_FAIL(work_fn())) {
        STORAGE_LOG(WARN, "fail to execute work fn", KR(ret));
      }
      if (OB_FAIL(ret) && can_retry(ret) && (retry_cnt < MAX_TASK_RETRY_TIMES)) {
        ret = common::OB_SUCCESS;
        if (OB_FAIL(on_retry_fn())) {
          STORAGE_LOG(WARN, "fail to execute on retry fn", KR(ret));
        } else if (OB_FAIL(share::dag_yield())) {
          STORAGE_LOG(WARN, "fail to yield before retry task", KR(ret), K(retry_cnt));
        } else {
          need_retry = true;
          ++retry_cnt;
          STORAGE_LOG(INFO, "retry task execution", K(retry_cnt));
        }
      }
    } while (need_retry);
    return ret;
  }

  template <typename WorkFn>
  int execute_with_retry(WorkFn &work_fn, int64_t &retry_cnt)
  {
    NoneOpFn none_op_fn;
    return execute_with_retry(work_fn, none_op_fn/*on_retry_fn*/, retry_cnt);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCBaseTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SHARED_GC_BASE_TASK_H_
