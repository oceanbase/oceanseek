/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_GEN_TASK_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_GEN_TASK_H_

#include "lib/lock/ob_spin_lock.h"
#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_task_base.h"

namespace oceanbase
{
namespace storage
{

struct ObSSDataCheckGenTaskOptions final
{
  ObSSDataCheckGenTaskOptions();
  ~ObSSDataCheckGenTaskOptions() = default;

  int init(
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_timeout_us);
  bool is_valid() const;
  void reset();

  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_timeout_us_;

  TO_STRING_KV(K_(memory_limit), K_(file_buf_size), K_(io_timeout_us));
};

// 本任务只复用 GC DAG 调度基类的周期控制和 DAG 存在性探测，
// 不读取 shared GC 开关、sswriter lease 或 _shared_gc_interval 配置。
// 巡检模式和调度周期分别读取独立的租户配置。
class ObSSDataCheckGenTask : public ObSSGCDagTaskBase
{
public:
  explicit ObSSDataCheckGenTask(volatile bool &is_stopped);
  virtual ~ObSSDataCheckGenTask();

  int init(const uint64_t tenant_id);
  int init(
      const uint64_t tenant_id,
      const ObSSDataCheckGenTaskOptions &options);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  void report_data_check_dag_result(
      const bool need_retry,
      const int64_t finish_ts);

protected:
  static constexpr int64_t DEFAULT_EXEC_INTERVAL_US =
      7LL * 24LL * 60LL * 60LL * 1000LL * 1000LL;
  static constexpr int64_t INITIAL_RETRY_INTERVAL_US =
      60LL * 60LL * 1000LL * 1000LL;
  static constexpr int64_t MAX_ACCELERATED_RETRY_COUNT = 3;
  virtual int check_sys_ls_strong_leader_(bool &is_leader) const;
  virtual int get_shared_meta_snapshot_(share::SCN &snapshot_meta_scn) const;
  virtual int get_data_check_mode_(ObSSDataCheckMode &check_mode) const;
  virtual int submit_data_check_dag_(const ObSSDataCheckDagParam &param);
  virtual int check_data_check_dag_exist_(bool &exist);
  virtual int check_gc_dag_retry_state_(
      const int64_t check_ts,
      bool &has_pending_retry,
      bool &need_retry) const override;
  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override;
  virtual void get_gc_exec_interval_us_(
      int64_t &exec_interval_us) const override;

private:
  friend class ObSSGCTaskRunner;
  virtual int check_can_schedule_gc_dag_(bool &can_schedule) const override;
  virtual int submit_gc_dag_(const int64_t check_ts) override;
  virtual int check_gc_dag_exist_(bool &exist) override;
  void reset_dag_schedule_controller_();
  void commit_dag_schedule_start_time_(const int64_t start_ts);

  int build_dag_param_(
      const int64_t check_ts,
      const share::SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode,
      ObSSDataCheckDagParam &param) const;
  void reset_data_check_dag_retry_();
  int64_t get_retry_interval_locked_(const int64_t exec_interval_us) const;
  static int64_t add_interval_safely_(
      const int64_t base_ts,
      const int64_t interval_us);

private:
  ObSSDataCheckGenTaskOptions options_;
  mutable common::ObSpinLock retry_lock_;
  int64_t retry_not_before_us_;
  int64_t retry_count_;
  ObSSDataCheckMode pending_check_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckGenTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_GEN_TASK_H_
