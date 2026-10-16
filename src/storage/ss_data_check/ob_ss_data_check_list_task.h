/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_TASK_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_data_check/ob_ss_data_check_list_checker.h"

namespace oceanbase
{
namespace storage
{

// 在最终 Expected 文件发布后，从租户运行环境获取对象存储和 shared meta
// 依赖并执行单向包含校验。
class ObSSDataCheckListCheckTask final
    : public ObSharedGCBaseTask,
      public share::ObCheckStopProvider
{
public:
  ObSSDataCheckListCheckTask();
  virtual ~ObSSDataCheckListCheckTask() = default;

  int init(ObSSDataCheckCtx *ctx);
  virtual int process() override;
  virtual int check_stop() const override;
  const ObSSDataCheckListCompareStat &get_stat() const { return stat_; }

  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckListCheckTask",
      KP(this), K_(check_attempted), K_(check_complete), K_(stat));

private:
  int check_expected_(
      const ObSSDataCheckSortedFileInfo &expected_file,
      const int64_t expected_item_count);
  int check_non_empty_expected_(
      const ObSSDataCheckSortedFileInfo &expected_file,
      const int64_t expected_item_count);

private:
  bool is_inited_;
  bool check_attempted_;
  bool check_complete_;
  ObSSDataCheckCtx *ctx_;
  ObSSDataCheckListCompareStat stat_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckListCheckTask);
};

// LIST 校验任务完成且 active task 计数归零后，发布本轮 HEALTH 或
// NOT_HEALTH。任务失败时不发布任何业务状态。
class ObSSDataCheckFinalizeTask final : public ObSharedGCBaseTask
{
public:
  ObSSDataCheckFinalizeTask();
  virtual ~ObSSDataCheckFinalizeTask() = default;

  int init(ObSSDataCheckCtx *ctx);
  virtual int process() override;

  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckFinalizeTask",
      KP(this), K_(publish_attempted), K_(publish_complete));

private:
  void record_server_event_(const ObSSDataCheckStatus status) const;

private:
  bool is_inited_;
  bool publish_attempted_;
  bool publish_complete_;
  ObSSDataCheckCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckFinalizeTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_TASK_H_
