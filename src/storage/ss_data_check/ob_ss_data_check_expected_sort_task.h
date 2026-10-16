/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORT_TASK_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORT_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObSSDataCheckExpectedMergeCompleteTask;

// 为预期数据排序子图提供非持有式 Context 绑定。Context 由 DAG 持有，必须等
// 所有任务退出后才能析构。
class ObSSDataCheckExpectedSortBaseTask : public ObSharedGCBaseTask
{
public:
  explicit ObSSDataCheckExpectedSortBaseTask(
      const share::ObITask::ObITaskType task_type);
  virtual ~ObSSDataCheckExpectedSortBaseTask() = default;

protected:
  int init_ctx_(ObSSDataCheckCtx *ctx);
  int inc_active_task_count_(ObSSDataCheckCtx *&ctx);
  void leave_task_(ObSSDataCheckCtx *ctx, const int task_ret);
  ObSSDataCheckCtx *get_ctx_() const { return ctx_; }
  int check_if_stopped_or_failed() const;
  int check_complete_relay_(
      const ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task) const;

private:
  ObSSDataCheckCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedSortBaseTask);
};

// 静态的生产者屏障 -> MergeStart 和 MergeStart -> MergeComplete 依赖边由所属
// DAG 构建，MergeStart 负责扩展第一轮控制任务的转接依赖。
class ObSSDataCheckExpectedMergeStartTask final
    : public ObSSDataCheckExpectedSortBaseTask
{
public:
  ObSSDataCheckExpectedMergeStartTask();
  virtual ~ObSSDataCheckExpectedMergeStartTask() = default;
  int init(
      ObSSDataCheckCtx *ctx,
      ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckExpectedMergeStartTask",
      KP(this), KP_(merge_complete_task));

private:
  int create_first_pass_ctrl_task_(ObSSDataCheckCtx &ctx);

private:
  bool is_inited_;
  ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedMergeStartTask);
};

// 每个控制任务持有一个不可变的轮次编号。第 r 轮（r > 0）先提升第 r-1 轮的
// 输出，随后结束合并或扩展 Batch[r,*] -> Ctrl[r+1]。
class ObSSDataCheckExpectedMergePassCtrlTask final
    : public ObSSDataCheckExpectedSortBaseTask
{
public:
  ObSSDataCheckExpectedMergePassCtrlTask();
  virtual ~ObSSDataCheckExpectedMergePassCtrlTask() = default;
  int init(
      ObSSDataCheckCtx *ctx,
      const int64_t pass_idx,
      ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckExpectedMergePassCtrlTask",
      KP(this), K_(pass_idx), KP_(merge_complete_task));

private:
  int create_next_pass_tasks_(
      ObSSDataCheckCtx &ctx,
      const int64_t batch_count);

private:
  bool is_inited_;
  int64_t pass_idx_;
  ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedMergePassCtrlTask);
};

// 同一轮的批次任务彼此独立。generate_next_task() 将下一轮控制子任务复制到
// 每个新生成的同级任务，以形成屏障。
class ObSSDataCheckExpectedMergeBatchTask final
    : public ObSSDataCheckExpectedSortBaseTask
{
public:
  ObSSDataCheckExpectedMergeBatchTask();
  virtual ~ObSSDataCheckExpectedMergeBatchTask() = default;
  int init(
      ObSSDataCheckCtx *ctx,
      const int64_t pass_idx,
      const int64_t batch_idx,
      const int64_t total_batch_count);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckExpectedMergeBatchTask",
      KP(this), K_(pass_idx), K_(batch_idx), K_(total_batch_count),
      K_(next_task_generated));

private:
  struct MergeBatchFn final
  {
    MergeBatchFn(
        ObSSDataCheckExpectedSortPipeline &pipeline,
        const int64_t pass_idx,
        const int64_t batch_idx)
      : pipeline_(pipeline), pass_idx_(pass_idx), batch_idx_(batch_idx)
    {}
    int operator()();
    ObSSDataCheckExpectedSortPipeline &pipeline_;
    int64_t pass_idx_;
    int64_t batch_idx_;
  };

  struct MergeBatchOnRetryFn final
  {
    explicit MergeBatchOnRetryFn(ObSSDataCheckExpectedMergeBatchTask &task)
      : task_(task)
    {}
    int operator()();
    ObSSDataCheckExpectedMergeBatchTask &task_;
  };

private:
  bool is_inited_;
  bool next_task_generated_;
  int64_t pass_idx_;
  int64_t batch_idx_;
  int64_t total_batch_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedMergeBatchTask);
};

// 转接依赖保证本任务仅在末轮控制任务完成后才可运行。本任务发布最终排序文件，
// 但不清理 Context，因为后续校验阶段仍需使用该文件。
class ObSSDataCheckExpectedMergeCompleteTask final
    : public ObSSDataCheckExpectedSortBaseTask
{
public:
  ObSSDataCheckExpectedMergeCompleteTask();
  virtual ~ObSSDataCheckExpectedMergeCompleteTask() = default;
  int init(ObSSDataCheckCtx *ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckExpectedMergeCompleteTask",
      KP(this), K_(final_pass_idx), K_(is_complete));

private:
  struct CompleteMergeFn final
  {
    CompleteMergeFn(
        ObSSDataCheckExpectedSortPipeline &pipeline,
        const int64_t final_pass_idx)
      : pipeline_(pipeline), final_pass_idx_(final_pass_idx)
    {}
    int operator()();
    ObSSDataCheckExpectedSortPipeline &pipeline_;
    int64_t final_pass_idx_;
  };

private:
  bool is_inited_;
  bool is_complete_;
  int64_t final_pass_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedMergeCompleteTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORT_TASK_H_
