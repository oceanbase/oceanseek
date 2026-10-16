/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_
#define OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_evict_micro_op.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_reorganize_micro_op.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;

class ObSSReleaseCacheTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSReleaseCacheTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSReleaseCacheTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  // For ob_admin or test
  bool is_evict_op_closed() const { return evict_op_.is_closed(); }
  void enable_evict_op() { evict_op_.enable_op(); }
  void disable_evict_op() { evict_op_.disable_op(); }
  bool is_reorganize_op_closed() const { return reorganize_op_.is_closed(); }
  void enable_reorganize_op() { reorganize_op_.enable_op(); }
  void disable_reorganize_op() { reorganize_op_.disable_op(); }

private:
  void clear_for_next_round();
  void try_adjust_task_interval();

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSEvictMicroOp evict_op_;
  ObSSReorganizeMicroOp reorganize_op_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_ */