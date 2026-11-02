/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_file_op.h"

namespace oceanbase
{
namespace storage
{

struct ObCalibrateDiskSpaceResult;

enum class ObSSMacroCacheType : uint8_t;

struct ObCalibrateDiskSpaceTimeLog
{
public:
  ObCalibrateDiskSpaceTimeLog()
    : start_calc_us_(0), finish_tmp_file_calc_us_(0), finish_meta_file_calc_us_(0),
      finish_macro_block_calc_us_(0)
  {}
  virtual ~ObCalibrateDiskSpaceTimeLog() {}
  TO_STRING_KV("total_cost_us", finish_macro_block_calc_us_ - start_calc_us_,
               "tmp_file_cost_us", finish_tmp_file_calc_us_ - start_calc_us_,
               "meta_file_cost_us", finish_meta_file_calc_us_ - finish_tmp_file_calc_us_,
               "macro_block_cost_us", finish_macro_block_calc_us_ - finish_meta_file_calc_us_);

public:
  int64_t start_calc_us_;
  int64_t finish_tmp_file_calc_us_;
  int64_t finish_meta_file_calc_us_;
  int64_t finish_macro_block_calc_us_;
};

/*
 * ObCalibrateDiskSpaceTask is used to calibrate disk space intervally(1h),
 * because of write failure or observer restart, the tenant disk space usage statistics is inaccurate
 */
class ObCalibrateDiskSpaceTask : public common::ObTimerTask
{
public:
  ObCalibrateDiskSpaceTask();
  virtual ~ObCalibrateDiskSpaceTask() { destroy(); }

  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();

  virtual void runTimerTask() override;
  int schedule_calibrate_disk_space();
  void set_mandatory_calibrate();

private:
  int check_if_need_calibration(const bool is_mandatory_calibrate, bool &is_need_exec);
  int calibrate_disk_space();
  // Refresh the time boundary and initial accounting on every unstable macro-block scan.
  int calc_macro_block_disk_space_with_retry_(ObFileStatInfo &initial_stat_info,
      int64_t &initial_hot_tablet_used_size, ObCalibrateDiskSpaceResult &calibrate_res);
  // Prepare the macro/HOT accounting snapshot before its mtime cutoff, with bounded time retries.
  int prepare_macro_calibration_snapshot_(int64_t &start_calc_time_s,
      ObFileStatInfo &macro_stat, ObFileStatInfo &hot_stat);
  int calibrate_macro_cache_used_size(const ObSSMacroCacheType cache_type,
                                      const int64_t start_calc_size_time_s,
                                      const ObFileStatInfo &initial_stat_info,
                                      const int64_t initial_hot_tablet_used_size = 0);
  int calibrate_macro_cache_used_size_forcely(const ObSSMacroCacheType cache_type,
                                              const ObFileStatInfo &initial_stat_info,
                                              const int64_t initial_hot_tablet_used_size,
                                              ObCalibrateDiskSpaceResult &calibrate_res);
  // Atomically apply the calibration via ObTenantDiskSpaceManager::calibrate_alloc_size_with_initial
  // so allocs/frees racing between snapshot and apply are folded into the final used_,
  // instead of being silently dropped (and later turning into free_size_exceeded_sum_).
  int calibrate_macro_cache_used_size_normally(const ObSSMacroCacheType cache_type,
                                               const ObFileStatInfo &initial_stat_info,
                                               const int64_t initial_hot_tablet_used_size,
                                               ObCalibrateDiskSpaceResult &calibrate_res);
  int choose_start_calc_size_time_s(int64_t &start_calc_size_time_s);
  int delete_tmp_seq_files(ObDirCalcSizeOp &del_tmp_seq_file_op); // delete tenant_id dir's .tmp.seq files, for example SERVER_META and TENANT_DISK_SPACE_META
  int rm_logical_deleted_file(); // delete tmp_data dir's .deleted files and empty scatter & tmp_file directories
  int get_macro_cache_used_size(ObFileStatInfo &meta_file_stat_info, ObFileStatInfo &tmp_file_stat_info,
                                ObFileStatInfo &macro_block_stat_info, ObFileStatInfo &hot_tablet_macro_block_stat_info);
  int get_macro_cache_used_size(const ObSSMacroCacheType cache_type,
                                int64_t &used_size);
  int get_macro_cache_used_size(const ObSSMacroCacheType cache_type,
                                ObFileStatInfo &stat_info);
  int get_macro_cache_used_dir_info(const ObSSMacroCacheType cache_type,
                                    int64_t &dir_size,
                                    int64_t &dir_num);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  volatile bool is_stop_;
  uint64_t run_cnt_; // for test, in order to observe whether task is running
  int64_t last_exec_timestamp_us_;
  volatile bool is_mandatory_calibrate_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_ */