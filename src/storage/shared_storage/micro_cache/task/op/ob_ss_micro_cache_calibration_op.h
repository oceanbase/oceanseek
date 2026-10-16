/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_OP_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace storage
{

class ObSSMicroCacheCalibrationOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSMicroCacheCalibrationOp();
  virtual ~ObSSMicroCacheCalibrationOp() override { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  int execute();

private:
  int execute_calibration_op();
  // Returns true only when write is stuck (FLAG_WRITE_STUCK), which means the cache cannot
  // serve writes and must be calibrated. Other anomaly flags indicate degraded state but the
  // cache is still functional, so they don't warrant the cost of a full calibration shutdown.
  bool need_calibration(const uint64_t abnormal_flag) const;
  int prepare_calibration();
  void finish_calibration(const bool calibration_succeeded);
  int do_calibration();
  void reset_for_calibration();
  int traverse_meta_and_rebuild();
  int rebuild_cache_status();

private:
  common::ObArray<int64_t> expected_valid_len_arr_;  // new valid_len for each phy_blk
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_CALIBRATION_OP_H_
