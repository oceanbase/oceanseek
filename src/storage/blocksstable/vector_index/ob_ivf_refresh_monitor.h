/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_REFRESH_MONITOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_REFRESH_MONITOR_H_
#include "lib/lock/ob_spin_lock.h"
#include "lib/profile/ob_trace_id.h"
#include "share/vector_index/ob_ivf_refresh_progress.h"
namespace oceanbase
{
namespace storage
{
class ObDDLDagMonitorNode;
class ObDDLDagMonitorEntry;
class ObIVFRefreshMonitorInfo;
// Shared by the refresh registration, tablet runtime and cached monitor entries.
// SQL reads copied statistics only; no query dereferences a pipeline or trainer.
class ObIVFRefreshMonitor final : public share::ObIVFRefreshProgress
{
public:
  struct StageStat {
    StageStat() : start_us_(0), end_us_(0), completed_(0), total_(-1), ret_code_(0) {}
    int64_t start_us_;
    int64_t end_us_;
    int64_t completed_;
    int64_t total_; // -1 means unknown, including during capture
    int ret_code_;
  };
  ObIVFRefreshMonitor();
  ~ObIVFRefreshMonitor();
  void init(ObDDLDagMonitorNode *node, int64_t task_id, int64_t execution_id,
            uint64_t tablet_id, uint64_t start_snapshot, uint64_t target_snapshot);
  void inc_ref();
  void dec_ref();
  virtual void begin_stage(Stage stage) override;
  virtual void add_completed(Stage stage, int64_t count) override;
  virtual void set_total(Stage stage, int64_t count) override;
  virtual void iteration_finished(int64_t iteration, int64_t elapsed_us,
                                  double diff, double imbalance) override;
  void set_center_count(int64_t count);
  // Idempotent. Called after trans.end(), also on failure/cancellation.
  void finish(int ret_code, int64_t task_start_us = 0, int64_t retry_count = 0);
  int to_entry(ObDDLDagMonitorEntry &entry, int64_t stage) const;
  static const char *stage_name(int64_t stage);
private:
  const char *stage_state_(int64_t stage) const;
  mutable common::SpinRWLock lock_;
  volatile int64_t refs_;
  int64_t task_id_;
  int64_t execution_id_;
  uint64_t tablet_id_;
  uint64_t start_snapshot_;
  uint64_t target_snapshot_;
  int64_t start_us_;
  int64_t end_us_;
  int64_t task_start_us_;
  int64_t retry_count_;
  int result_;
  int64_t current_stage_;
  StageStat stages_[STAGE_COUNT];
  int64_t iterations_;
  int64_t last_iteration_us_;
  int64_t iteration_sum_us_;
  double diff_;
  double imbalance_;
  int64_t centers_;
  common::ObCurTraceId::TraceId trace_id_;
  ObDDLDagMonitorNode *node_;
  ObIVFRefreshMonitorInfo *entries_[STAGE_COUNT + 1];
  DISALLOW_COPY_AND_ASSIGN(ObIVFRefreshMonitor);
};
} // namespace storage
} // namespace oceanbase
#endif
