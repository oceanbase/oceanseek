/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MICRO_CACHE_MONITOR_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MICRO_CACHE_MONITOR_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_monitor_op.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;
struct ObSSMicroCacheTaskCtx;

class ObSSMicroCacheMonitorTask : public common::ObTimerTask, public ObSSMicroCacheBaseTask
{
public:
  ObSSMicroCacheMonitorTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSMicroCacheMonitorTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask();
  void destroy();

  bool is_monitor_op_closed() const { return monitor_op_.is_closed(); }
  void enable_monitor_op() { monitor_op_.enable_op(); }
  void disable_monitor_op() { monitor_op_.disable_op(); }

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSMicroCacheMonitorOp monitor_op_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MICRO_CACHE_MONITOR_TASK_H_