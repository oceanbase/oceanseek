/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_MONITOR_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_MONITOR_OP_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"

namespace oceanbase
{
namespace storage
{

struct SSMicroCacheMonitorCtx
{
public:
  // Max allowed |delta_add - delta_fail| when judging whether all add requests are failed. 
  // Because add_cnt and fail_add_cnt are read separately, add requests may concurrently update these counters.
  static constexpr int64_t INCREMENT_ADD_FAIL_DELTA_TOLERANCE = 1;
  int64_t pre_add_cnt_;
  int64_t cur_add_cnt_;
  int64_t pre_fail_add_cnt_;
  int64_t cur_fail_add_cnt_;
  int64_t write_stuck_cnt_;
  int64_t last_check_time_us_;
  int64_t prev_check_config_time_us_;
  int64_t prev_update_load_time_us_;
  int64_t prev_print_stat_time_us_;
  int64_t print_stat_interval_us_;  // configurable, default SS_PRINT_CACHE_STAT_INTERVAL_US
  int64_t prev_abnormal_log_time_us_;

  SSMicroCacheMonitorCtx() { reset(); }
  ~SSMicroCacheMonitorCtx() { reset(); }
  void reset();
  void update_newest_add_info(const int64_t cur_add_cnt, const int64_t cur_fail_add_cnt);
  bool exist_increament_io(const int64_t min_increament_cnt = 1) const;
  bool is_all_increament_io_failed() const;
  bool need_check_config() const;
  bool need_update_cache_load() const;
  bool need_print_cache_stat() const;
  bool need_print_abnormal_log() const;
  void finish_check_config();
  void finish_update_cache_load();
  void finish_print_cache_stat();
  void finish_print_abnormal_log();

  TO_STRING_KV(K_(pre_add_cnt), K_(cur_add_cnt), K_(pre_fail_add_cnt),
      K_(cur_fail_add_cnt), K_(write_stuck_cnt), K_(last_check_time_us),
      K_(prev_check_config_time_us), K_(prev_update_load_time_us), K_(prev_print_stat_time_us),
      K_(print_stat_interval_us), K_(prev_abnormal_log_time_us));
};

class ObSSMicroCacheMonitorOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSMicroCacheMonitorOp();
  virtual ~ObSSMicroCacheMonitorOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  int execute();

  SSMicroCacheMonitorCtx &get_monitor_ctx() { return monitor_ctx_; }

private:
  int execute_monitor_op();
  int check_write_abnormal(bool &is_abnormal);
  int check_arguments_abnormal(bool &is_abnormal);
  int inner_check_arc_info(bool &is_abnormal);
  int inner_check_cache_stat(bool &is_abnormal);
  int check_task_abnormal(bool &is_abnormal);
  void report_micro_cache_status(const bool is_abnormal, const uint64_t initial_flag);
  void handle_periodic_extra_tasks();

private:
  static constexpr int64_t WRITE_STUCK_CHECK_INTERVAL_US = 60L * 1000 * 1000; // 1min
  static constexpr int64_t MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT = 10; // 10 consecutive checks (10 minutes)
  static constexpr int64_t TASK_STUCK_TIMEOUT_US = 10L * 60 * 1000 * 1000; // 10 minutes

private:
  SSMicroCacheMonitorCtx monitor_ctx_;
  bool enable_update_arc_limit_;  // when false, skips get_and_update_cache_limit_size (for test) in check_config interval
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_MONITOR_OP_H_