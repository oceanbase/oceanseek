/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_calibration_op.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;
struct ObSSMicroCacheTaskCtx;

/**
 * @brief Micro block cache calibration task.
 *
 * Rebuilds meta, sub_range, arc_info, phy_blk valid_len, etc. when add requests are stuck,
 * so that internal state matches the actual data.
 *
 * Trigger: Only when FLAG_WRITE_STUCK is set (add requests keep failing). Does NOT run for
 * FLAG_INVALID_STAT or FLAG_TASK_STUCK, because in those cases the write path is still
 * functioning and calibration is unnecessary.
 *
 * abnormal_flag handling: The monitor task only sets flags (add_abnormal_flag), never clears.
 * Calibration is the only task that clears abnormal_flag (on successful completion).
 *
 * Impact on micro cache:
 * - Before: enable_read, disable_write, wait flying add requests, disable common bg tasks + meta_ckpt,
 *   wait until persist_data / blk_ckpt / monitor ops close (not meta_ckpt/release_cache — same TG as calib).
 * - During: clear sub_ranges, traverse meta, rebuild valid_len / arc / micro_stat / sub_range_cnt, etc.
 * - After: enable background tasks, enable_write, clear abnormal_flag.
 *
 * Unlike other tasks: calibration holds is_operating_ (mutex with clear_micro_cache and resize)
 * and pauses other tasks. It is a "maintenance shutdown" operation.
 */
class ObSSMicroCacheCalibrationTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSMicroCacheCalibrationTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSMicroCacheCalibrationTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask();
  void destroy();

  bool is_calibration_op_closed() const { return calibration_op_.is_closed(); }
  void enable_calibration_op() { calibration_op_.enable_op(); }
  void disable_calibration_op() { calibration_op_.disable_op(); }

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSMicroCacheCalibrationOp calibration_op_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_TASK_H_
