/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_HISTORY_CLEANUP_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_HISTORY_CLEANUP_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCHistoryCleanupTask final : public ObSSGCTaskBase
{
public:
  explicit ObSSGCHistoryCleanupTask(volatile bool &is_stopped);
  virtual ~ObSSGCHistoryCleanupTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int do_cleanup_();

  static const int64_t SCHEDULE_INTERVAL_US  = 1LL * 24LL * 60LL * 60LL * 1000LL * 1000LL;   // 1d

  static const int64_t DEFAULT_RETENTION_US  = 30LL * 24LL * 60LL * 60LL * 1000LL * 1000LL;   // 30d

  DISALLOW_COPY_AND_ASSIGN(ObSSGCHistoryCleanupTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_HISTORY_CLEANUP_TASK_H_
