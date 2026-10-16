/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_do_blk_checkpoint_op.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;

class ObSSDoBlkCheckpointTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSDoBlkCheckpointTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSDoBlkCheckpointTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  // For ob_admin or test
  bool is_blk_ckpt_op_closed() const { return ckpt_op_.is_closed(); }
  void enable_blk_ckpt_op() { ckpt_op_.enable_op(); }
  void disable_blk_ckpt_op() { ckpt_op_.disable_op(); }

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSDoBlkCheckpointOp ckpt_op_; // TODO @donglou.zl rename as 'blk_ckpt_op_'
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_TASK_H_ */