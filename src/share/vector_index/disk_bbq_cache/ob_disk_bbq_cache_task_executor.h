/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_EXECUTOR_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_EXECUTOR_H_

#include "share/vector_index/ob_vector_index_i_task_executor.h"

namespace oceanbase
{
namespace share
{

// Discovers Tablet-scoped maintenance work for DiskBBQ SSTable caches and
// publishes it through the common vector async-task framework.
class ObDiskBBQCacheTaskExecutor final : public ObVecITaskExecutor
{
public:
  ObDiskBBQCacheTaskExecutor() : ObVecITaskExecutor() {}
  virtual ~ObDiskBBQCacheTaskExecutor() = default;

  int load_triggered_task(const ObVecIndexTaskStatus &task_row) override;
  int load_task(uint64_t &task_trace_base_num) override;
  int check_and_set_thread_pool() override;
  static int do_check_task_result(
      ObVecIndexAsyncTaskCtx *task_ctx,
      ObVecTaskResultCheckAction &action);

private:
  bool check_operation_allow() override { return true; }
  int add_task_ctx_(ObPluginVectorIndexMgr &index_ls_mgr,
                    const ObTabletID &tablet_id,
                    uint64_t table_id,
                    ObVecIndexAsyncTaskType task_type,
                    uint64_t &task_trace_base_num,
                    ObVecIndexTaskCtxArray &task_status_array);

  DISALLOW_COPY_AND_ASSIGN(ObDiskBBQCacheTaskExecutor);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_EXECUTOR_H_
