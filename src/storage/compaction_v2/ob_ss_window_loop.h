// Copyright (c) 2026 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_STORAGE_COMPACTION_SS_WINDOW_LOOP_H_
#define OB_STORAGE_COMPACTION_SS_WINDOW_LOOP_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_compaction_schedule_iterator.h"

namespace oceanbase
{
namespace compaction
{

struct ObSSScheduleTabletFunc;
class ObTenantTabletScheduler;

class ObSSWindowLoop final
{
public:
  ObSSWindowLoop();
  ~ObSSWindowLoop() = default;
  void reset();
  int get_adaptive_compaction_thread_cnt(
      const int64_t compaction_low_thread_score, int64_t &thread_cnt) const;
  int try_schedule_window_compaction(const ObMergeInfo &merge_info, const int64_t local_frozen_scn);
private:
  void adapt_window_thread_cnt_(const bool can_schedule);
  void reset_adaptive_compaction_thread_cnt_();
  void update_adaptive_compaction_thread_cnt_();
  void update_window_schedule_state_(const ObMergeInfo &merge_info);
  int check_can_schedule_(bool &can_schedule, int64_t &window_end_time_us) const;
  int calc_writer_schedule_quota_(int64_t &writer_schedule_quota) const;
  bool can_continue_schedule_(const int64_t writer_schedule_quota, const int64_t window_end_time_us) const;
  int loop_in_ls_(storage::ObLSHandle &ls_handle, ObSSScheduleTabletFunc &schedule_func,
      int64_t &writer_schedule_quota, const int64_t window_end_time_us);
  int loop_(const int64_t window_end_time_us);
private:
  static const int64_t WRITER_DAG_LIMIT_FACTOR = 4;
  int64_t window_start_time_us_;
  int64_t target_scn_;
  int64_t loop_cnt_;
  ObCompactionScheduleIterator ls_tablet_iter_;
  // WindowLoop publishes the cache; DagScheduler only reads it, without SQL under its priority lock.
  mutable common::SpinRWLock thread_cnt_lock_;
  int64_t adaptive_thread_cnt_;
  bool thread_cnt_assigned_;

  DISALLOW_COPY_AND_ASSIGN(ObSSWindowLoop);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SS_WINDOW_LOOP_H_
