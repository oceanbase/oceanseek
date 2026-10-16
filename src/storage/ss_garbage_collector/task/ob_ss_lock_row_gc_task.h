/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_LOCK_ROW_GC_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_LOCK_ROW_GC_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_task_base.h"

namespace oceanbase
{
namespace storage
{

struct ObSSLockRowGCDagParam;

class ObSSLockRowGCTask : public ObSSGCDagTaskBase
{
public:
  explicit ObSSLockRowGCTask(volatile bool &is_stopped);
  virtual ~ObSSLockRowGCTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  virtual void stop() override;
  void destroy();
  virtual void runTimerTask() override;

protected:
  virtual int check_can_schedule_lock_row_gc_dag_(bool &can_schedule) const;
  virtual int submit_lock_row_gc_dag_(const ObSSLockRowGCDagParam &param);
  virtual int check_lock_row_gc_dag_exist_(bool &exist);

private:
  friend class ObSSGCTaskRunner;
  virtual int check_can_schedule_gc_dag_(bool &can_schedule) const override;
  virtual int submit_gc_dag_(const int64_t check_ts) override;
  virtual int check_gc_dag_exist_(bool &exist) override;
  int try_schedule_lock_row_gc_dag_();
  void reset_dag_schedule_controller_();
  void commit_dag_schedule_start_time_(const int64_t start_ts);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_LOCK_ROW_GC_TASK_H_
