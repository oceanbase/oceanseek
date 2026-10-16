/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_meta_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;

class ObSSPersistMicroMetaTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSPersistMicroMetaTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSPersistMicroMetaTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  void enable_replay_ckpt() { persist_meta_op_.enable_replay_ckpt(); }
  // For ob_admin or test
  bool is_meta_ckpt_op_closed() const { return persist_meta_op_.is_closed(); }
  void enable_meta_ckpt_op() { persist_meta_op_.enable_op(); }
  void disable_meta_ckpt_op() { persist_meta_op_.disable_op(); }

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSPersistMicroMetaOp persist_meta_op_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_TASK_H_ */