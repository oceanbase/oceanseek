/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_TABLET_STUDY_INFO_CLEANUP_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_TABLET_STUDY_INFO_CLEANUP_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObSSTabletStudyInfoCleanupTask final : public ObSSGCTaskBase
{
public:
  explicit ObSSTabletStudyInfoCleanupTask(volatile bool &is_stopped);
  virtual ~ObSSTabletStudyInfoCleanupTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  // 本地 tablet study 位点固定每小时核对一次生命周期。
  static const int64_t SCHEDULE_INTERVAL_US =
      60LL * 60LL * 1000LL * 1000LL;
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletStudyInfoCleanupTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_TABLET_STUDY_INFO_CLEANUP_TASK_H_
