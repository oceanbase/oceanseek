/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_RECORD_CLEANUP_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_RECORD_CLEANUP_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

struct ObSharedGCTask;

// A record is the MARK_SWEEP_GC_DIAG round marker in the shared GC task table.
// Delete its round directory first, then delete the marker. Retain the marker on failure
// so that the next run can retry the cleanup.
class ObSSMarkSweepDiagRecordCleanupTask final : public ObSSGCTaskBase
{
public:
  explicit ObSSMarkSweepDiagRecordCleanupTask(
      volatile bool &is_stopped);
  virtual ~ObSSMarkSweepDiagRecordCleanupTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int do_cleanup_();
  int get_min_round_id_(bool &has_min_round_id, int64_t &min_round_id);
  int cleanup_stale_rounds_by_task_(
      const int64_t min_round_id);
  int cleanup_task_(
      const ObSharedGCTask &task);
  int delete_diag_dir_(const int64_t round_id);
  virtual void get_gc_exec_interval_us_(
      int64_t &exec_interval_us) const override;

  static const int64_t SCHEDULE_INTERVAL_US = 1LL * 60LL * 60LL * 1000LL * 1000LL; // 1h

#ifdef ERRSIM
  static const int64_t ERRSIM_INTERVAL_US = 10LL * 1000LL * 1000LL; // 10s
#endif

  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDiagRecordCleanupTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_RECORD_CLEANUP_TASK_H_
