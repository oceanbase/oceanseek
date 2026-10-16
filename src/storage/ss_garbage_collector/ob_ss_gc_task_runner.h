/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_TASK_RUNNER_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_TASK_RUNNER_H_

#include "lib/ob_define.h"
#include "storage/ss_data_check/ob_ss_data_check_gen_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_fail_gc_gen_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_ls_meta_gc_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_ckpt_gc_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_lock_row_gc_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_exec_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_fail_gc_exec_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_history_cleanup_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_tablet_study_info_cleanup_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_cleanup_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_record_cleanup_task.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCTaskRunner
{
public:
  ObSSGCTaskRunner();
  virtual ~ObSSGCTaskRunner();
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  bool need_retry_mark_sweep_dag() const;
  void set_need_retry_mark_sweep_dag(const bool need_retry);
  void report_data_check_dag_result(
      const bool need_retry,
      const int64_t finish_ts);

private:
  static const int INVALID_TG_ID = -1;
  int start_ckpt_gc_task_();
  void stop_timer_groups_();
  void wait_timer_groups_();
  bool is_inited_;
  volatile bool is_stopped_;
  uint64_t tenant_id_;
  int low_tg_id_;
  int mid_tg_id_;
  ObSSMarkSweepGCGenTask mark_sweep_gc_gen_task_;
  ObSSFailGCGenTask fail_gc_gen_task_;
  ObSSDataCheckGenTask data_check_gen_task_;
  ObSSLSMetaGCTask ls_meta_gc_task_;
  ObSSCkptGCTask ckpt_gc_task_;
  ObSSLockRowGCTask lock_row_gc_task_;
  ObSSMarkSweepGCExecTask mark_sweep_gc_exec_task_;
  ObSSFailGCExecTask fail_gc_exec_task_;
  ObSSGCHistoryCleanupTask history_cleanup_task_;
  ObSSTabletStudyInfoCleanupTask tablet_study_info_cleanup_task_;
  ObSSMarkSweepDiagCleanupTask mark_sweep_gc_diagnose_cleanup_task_;
  ObSSMarkSweepDiagRecordCleanupTask mark_sweep_gc_diagnose_record_cleanup_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSGCTaskRunner);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_TASK_RUNNER_H_
