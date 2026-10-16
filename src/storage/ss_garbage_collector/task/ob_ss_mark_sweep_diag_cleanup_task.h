/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_CLEANUP_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_CLEANUP_TASK_H_

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace storage
{

// Periodically list diagnose round directories as a fallback for objects without round markers.
// The cleanup bound is the minimum MARK_SWEEP_GC round_id in active/history; with no bound,
// delete nothing. Per-directory cleanup is best-effort and returns the first failure.
class ObSSMarkSweepDiagCleanupTask final : public ObSSGCTaskBase
{
public:
  explicit ObSSMarkSweepDiagCleanupTask(volatile bool &is_stopped);
  virtual ~ObSSMarkSweepDiagCleanupTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int do_cleanup_();
  int get_min_round_id_(bool &has_min_round_id, int64_t &min_round_id);
  int cleanup_stale_rounds_by_dir_(const int64_t min_round_id);
  virtual void get_gc_exec_interval_us_(int64_t &exec_interval_us) const override;

  static const int64_t SCHEDULE_INTERVAL_US =
      7LL * 24LL * 60LL * 60LL * 1000LL * 1000LL; // 7d

#ifdef ERRSIM
  static const int64_t ERRSIM_INTERVAL_US = 10LL * 1000LL * 1000LL; // 10s
#endif

  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDiagCleanupTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_DIAG_CLEANUP_TASK_H_
