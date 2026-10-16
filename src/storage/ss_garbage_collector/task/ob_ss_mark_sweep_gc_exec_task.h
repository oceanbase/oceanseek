/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_EXEC_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_EXEC_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObSSMarkSweepGCExecTask final : public ObSSGCTaskBase
{
public:
  explicit ObSSMarkSweepGCExecTask(volatile bool &is_stopped);
  virtual ~ObSSMarkSweepGCExecTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

protected:
  int do_submit_dag_task_();
  virtual void get_gc_exec_interval_us_(int64_t &exec_interval_us) const override;

private:
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000 * 1000L; // 10s
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_EXEC_TASK_H_
