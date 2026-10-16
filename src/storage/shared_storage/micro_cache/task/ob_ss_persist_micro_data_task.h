/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_data_op.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;
struct ObSSMicroCacheTaskCtx;

class ObSSPersistMicroDataTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSPersistMicroDataTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSPersistMicroDataTask() { destroy(); }
  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  // For ob_admin or test
  bool is_persist_data_op_closed() const { return persist_data_op_.is_closed(); }
  void enable_persist_data_op() { persist_data_op_.enable_op(); }
  void disable_persist_data_op() { persist_data_op_.disable_op(); }

private:
  void try_adjust_task_interval(const int64_t alloc_blk_fail_cnt);

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSPersistMicroDataOp persist_data_op_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_ */