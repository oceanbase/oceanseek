/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_expected_sort_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

// ==================== ObSSDataCheckExpectedSortBaseTask ====================

ObSSDataCheckExpectedSortBaseTask::ObSSDataCheckExpectedSortBaseTask(
    const ObITask::ObITaskType task_type)
  : ObSharedGCBaseTask(task_type), ctx_(nullptr)
{
}

int ObSSDataCheckExpectedSortBaseTask::init_ctx_(ObSSDataCheckCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected sort task context initialized twice", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected sort task context", KR(ret), KP(ctx));
  } else {
    ctx_ = ctx;
  }
  return ret;
}

int ObSSDataCheckExpectedSortBaseTask::inc_active_task_count_(
    ObSSDataCheckCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ctx = nullptr;
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected sort task context is null", KR(ret));
  } else if (OB_FAIL(ctx_->inc_active_task_count())) {
    LOG_WARN("failed to increment expected sort active task count", KR(ret), KP_(ctx));
  } else {
    ctx = ctx_;
  }
  return ret;
}

void ObSSDataCheckExpectedSortBaseTask::leave_task_(
    ObSSDataCheckCtx *ctx,
    const int task_ret)
{
  if (OB_NOT_NULL(ctx)) {
    ctx->dec_active_task_count(task_ret);
  }
}

int ObSSDataCheckExpectedSortBaseTask::check_if_stopped_or_failed() const
{
  int ret = OB_SUCCESS;
  ObIDag *dag = get_dag();
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  if (OB_NOT_NULL(dag_scheduler) && dag_scheduler->has_set_stop()) {
    ret = OB_SERVICE_STOPPED;
    LOG_INFO("tenant dag scheduler has stopped expected sort task", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected sort task dag is null", KR(ret));
  } else if (OB_FAIL(ObSSGCDagBase::check_if_stopped_or_failed(*dag))) {
    LOG_INFO("expected sort task dag has stopped",
        KR(ret), "dag_type", dag->get_type());
  }
  return ret;
}

int ObSSDataCheckExpectedSortBaseTask::check_complete_relay_(
    const ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task) const
{
  int ret = OB_SUCCESS;
  const ObIDag *dag = get_dag();
  if (OB_ISNULL(dag) || OB_ISNULL(merge_complete_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge relay has null task or dag", KR(ret), KP(dag), KP(merge_complete_task));
  } else if (OB_UNLIKELY(merge_complete_task->get_dag() != dag
      || ObITask::TASK_STATUS_WAITING != merge_complete_task->get_status()
      || merge_complete_task->get_indegree() <= 0)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected merge complete relay anchor is invalid", KR(ret), "expected_dag", dag,
        "complete_dag", merge_complete_task->get_dag(),
        "complete_status", merge_complete_task->get_status(),
        "complete_indegree", merge_complete_task->get_indegree());
  }
  return ret;
}

// ==================== ObSSDataCheckExpectedMergeStartTask ====================

ObSSDataCheckExpectedMergeStartTask::ObSSDataCheckExpectedMergeStartTask()
  : ObSSDataCheckExpectedSortBaseTask(TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_START),
    is_inited_(false),
    merge_complete_task_(nullptr)
{
}

int ObSSDataCheckExpectedMergeStartTask::init(
    ObSSDataCheckCtx *ctx,
    ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected merge start task initialized twice", KR(ret));
  } else if (OB_ISNULL(merge_complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected merge complete task is null", KR(ret));
  } else if (OB_FAIL(init_ctx_(ctx))) {
    LOG_WARN("failed to initialize expected merge start context", KR(ret));
  } else {
    merge_complete_task_ = merge_complete_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedMergeStartTask::create_first_pass_ctrl_task_(
    ObSSDataCheckCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObSSDataCheckExpectedMergePassCtrlTask *pass_ctrl_task = nullptr;
  int64_t pass_idx = -1;
  if (OB_ISNULL(dag = get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge start dag is null", KR(ret));
  } else if (OB_ISNULL(merge_complete_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge complete task is null", KR(ret));
  } else if (OB_FAIL(ctx.get_expected_sort_pipeline().get_current_pass_idx(pass_idx))) {
    LOG_WARN("failed to get first expected merge pass", KR(ret));
  } else if (OB_UNLIKELY(0 != pass_idx)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected first expected merge pass", KR(ret), K(pass_idx));
  } else if (OB_FAIL(dag->alloc_task(pass_ctrl_task))) {
    LOG_WARN("failed to allocate first expected pass control task", KR(ret));
  } else if (OB_FAIL(pass_ctrl_task->init(&ctx, pass_idx, merge_complete_task_))) {
    LOG_WARN("failed to initialize first expected pass control task", KR(ret));
  // 先建立下游转接依赖，再通过当前任务或 DAG 暴露新任务。这样可在 MergeStart
  // 释放静态锚点时，保证 Complete 的入度仍大于零。
  } else if (OB_FAIL(check_complete_relay_(merge_complete_task_))) {
    LOG_WARN("invalid expected merge complete relay anchor", KR(ret));
  } else if (OB_FAIL(pass_ctrl_task->add_child(
      *merge_complete_task_, false /*check_child_task_status*/))) {
    LOG_WARN("failed to wire first pass control to expected merge complete", KR(ret));
  } else if (OB_FAIL(add_child(*pass_ctrl_task))) {
    LOG_WARN("failed to wire expected merge start to first pass control", KR(ret));
  } else if (OB_FAIL(dag->add_task(*pass_ctrl_task))) {
    LOG_WARN("failed to add first expected pass control task", KR(ret));
  }
  return ret;
}

int ObSSDataCheckExpectedMergeStartTask::process()
{
  int ret = OB_SUCCESS;
  ObSSDataCheckCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merge start task is not initialized", KR(ret));
  } else if (OB_FAIL(inc_active_task_count_(ctx))) {
    LOG_WARN("failed to enter expected merge start task", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("expected merge start task has been stopped", KR(ret));
  } else if (OB_FAIL(ctx->finish_expected_collect())) {
    LOG_WARN("failed to finish expected collect at worker barrier", KR(ret));
  } else if (OB_FAIL(ctx->get_expected_sort_pipeline().prepare_merge())) {
    LOG_WARN("failed to prepare expected merge", KR(ret));
  } else if (OB_FAIL(create_first_pass_ctrl_task_(*ctx))) {
    LOG_WARN("failed to create first expected pass control task", KR(ret));
  }
  leave_task_(ctx, ret);
  return ret;
}

// ==================== ObSSDataCheckExpectedMergePassCtrlTask ====================

ObSSDataCheckExpectedMergePassCtrlTask::ObSSDataCheckExpectedMergePassCtrlTask()
  : ObSSDataCheckExpectedSortBaseTask(TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_PASS_CTRL),
    is_inited_(false),
    pass_idx_(-1),
    merge_complete_task_(nullptr)
{
}

int ObSSDataCheckExpectedMergePassCtrlTask::init(
    ObSSDataCheckCtx *ctx,
    const int64_t pass_idx,
    ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected pass control task initialized twice", KR(ret));
  } else if (OB_UNLIKELY(pass_idx < 0) || OB_ISNULL(merge_complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected pass control argument", KR(ret), K(pass_idx), KP(merge_complete_task));
  } else if (OB_FAIL(init_ctx_(ctx))) {
    LOG_WARN("failed to initialize expected pass control context", KR(ret));
  } else {
    pass_idx_ = pass_idx;
    merge_complete_task_ = merge_complete_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedMergePassCtrlTask::create_next_pass_tasks_(
    ObSSDataCheckCtx &ctx,
    const int64_t batch_count)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObSSDataCheckExpectedMergePassCtrlTask *next_pass_ctrl_task = nullptr;
  ObSSDataCheckExpectedMergeBatchTask *first_batch_task = nullptr;
  if (OB_UNLIKELY(batch_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected merge batch count", KR(ret), K(batch_count), K_(pass_idx));
  } else if (OB_ISNULL(dag = get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected pass control dag is null", KR(ret), K_(pass_idx));
  } else if (OB_ISNULL(merge_complete_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge complete task is null", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(dag->alloc_task(next_pass_ctrl_task))) {
    LOG_WARN("failed to allocate next expected pass control task", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(next_pass_ctrl_task->init(&ctx, pass_idx_ + 1, merge_complete_task_))) {
    LOG_WARN("failed to initialize next expected pass control task", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(dag->alloc_task(first_batch_task))) {
    LOG_WARN("failed to allocate first expected merge batch task", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(first_batch_task->init(&ctx, pass_idx_, 0 /*batch_idx*/, batch_count))) {
    LOG_WARN("failed to initialize first expected merge batch task",
        KR(ret), K_(pass_idx), K(batch_count));
  // 先转接 Complete，再形成批次屏障，随后通过当前控制任务暴露新任务，最后将
  // 新任务发布到 DAG。
  } else if (OB_FAIL(check_complete_relay_(merge_complete_task_))) {
    LOG_WARN("invalid expected merge complete relay anchor", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(next_pass_ctrl_task->add_child(
      *merge_complete_task_, false /*check_child_task_status*/))) {
    LOG_WARN("failed to relay expected merge complete dependency", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(first_batch_task->add_child(*next_pass_ctrl_task))) {
    LOG_WARN("failed to wire first expected batch to next pass control", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(add_child(*first_batch_task))) {
    LOG_WARN("failed to wire pass control to first expected batch", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(add_child(*next_pass_ctrl_task))) {
    LOG_WARN("failed to wire pass control to next pass control", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(dag->add_task(*first_batch_task))) {
    LOG_WARN("failed to add first expected merge batch task", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(dag->add_task(*next_pass_ctrl_task))) {
    LOG_WARN("failed to add next expected pass control task", KR(ret), K_(pass_idx));
  }
  return ret;
}

int ObSSDataCheckExpectedMergePassCtrlTask::process()
{
  int ret = OB_SUCCESS;
  ObSSDataCheckCtx *ctx = nullptr;
  int64_t current_pass_idx = -1;
  int64_t input_count = 0;
  int64_t batch_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected pass control task is not initialized", KR(ret));
  } else if (OB_FAIL(inc_active_task_count_(ctx))) {
    LOG_WARN("failed to enter expected pass control task", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("expected pass control task has been stopped", KR(ret), K_(pass_idx));
  } else if (pass_idx_ > 0
      && OB_FAIL(ctx->get_expected_sort_pipeline().prepare_next_pass(pass_idx_ - 1))) {
    LOG_WARN("failed to promote expected merge pass", KR(ret), K_(pass_idx));
  } else if (OB_FAIL(ctx->get_expected_sort_pipeline().get_current_pass_idx(current_pass_idx))) {
    LOG_WARN("failed to get current expected merge pass", KR(ret), K_(pass_idx));
  } else if (OB_UNLIKELY(current_pass_idx != pass_idx_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected merge pass does not match control task",
        KR(ret), K_(pass_idx), K(current_pass_idx));
  } else if (OB_FAIL(ctx->get_expected_sort_pipeline().get_input_count(pass_idx_, input_count))) {
    LOG_WARN("failed to get expected merge input count", KR(ret), K_(pass_idx));
  } else if (input_count > 1 && OB_FAIL(ctx->get_expected_sort_pipeline().get_batch_count(
          pass_idx_, batch_count))) {
    LOG_WARN("failed to get expected merge batch count", KR(ret), K_(pass_idx));
  } else if (input_count > 1 && OB_FAIL(create_next_pass_tasks_(*ctx, batch_count))) {
    LOG_WARN("failed to create next expected merge pass tasks", KR(ret), K_(pass_idx), K(batch_count));
  }
  leave_task_(ctx, ret);
  return ret;
}

// ==================== ObSSDataCheckExpectedMergeBatchTask ====================

ObSSDataCheckExpectedMergeBatchTask::ObSSDataCheckExpectedMergeBatchTask()
  : ObSSDataCheckExpectedSortBaseTask(TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_BATCH),
    is_inited_(false),
    next_task_generated_(false),
    pass_idx_(-1),
    batch_idx_(-1),
    total_batch_count_(0)
{
}

int ObSSDataCheckExpectedMergeBatchTask::init(
    ObSSDataCheckCtx *ctx,
    const int64_t pass_idx,
    const int64_t batch_idx,
    const int64_t total_batch_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected merge batch task initialized twice", KR(ret));
  } else if (OB_UNLIKELY(pass_idx < 0 || batch_idx < 0 || total_batch_count <= 0
      || batch_idx >= total_batch_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected merge batch argument",
        KR(ret), K(pass_idx), K(batch_idx), K(total_batch_count));
  } else if (OB_FAIL(init_ctx_(ctx))) {
    LOG_WARN("failed to initialize expected merge batch context", KR(ret));
  } else {
    pass_idx_ = pass_idx;
    batch_idx_ = batch_idx;
    total_batch_count_ = total_batch_count;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedMergeBatchTask::generate_next_task(
    ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObSSDataCheckExpectedMergeBatchTask *next_batch_task = nullptr;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merge batch task is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (batch_idx_ + 1 >= total_batch_count_) {
    next_task_generated_ = true;
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected merge batch dag is null", KR(ret), K_(pass_idx), K_(batch_idx));
  } else if (OB_FAIL(dag->alloc_task(next_batch_task))) {
    LOG_WARN("failed to allocate next expected merge batch task",
        KR(ret), K_(pass_idx), K_(batch_idx));
  } else if (OB_FAIL(next_batch_task->init(
      get_ctx_(), pass_idx_, batch_idx_ + 1, total_batch_count_))) {
    LOG_WARN("failed to initialize next expected merge batch task",
        KR(ret), K_(pass_idx), K_(batch_idx));
  } else {
    next_task_generated_ = true;
    next_task = next_batch_task;
  }
  return ret;
}

int ObSSDataCheckExpectedMergeBatchTask::MergeBatchFn::operator()()
{
  return pipeline_.batch_merge(pass_idx_, batch_idx_);
}

int ObSSDataCheckExpectedMergeBatchTask::MergeBatchOnRetryFn::operator()()
{
  return task_.check_if_stopped_or_failed();
}

int ObSSDataCheckExpectedMergeBatchTask::process()
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObSSDataCheckCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merge batch task is not initialized", KR(ret));
  } else if (OB_FAIL(inc_active_task_count_(ctx))) {
    LOG_WARN("failed to enter expected merge batch task", KR(ret), K_(pass_idx), K_(batch_idx));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("expected merge batch task has been stopped", KR(ret), K_(pass_idx), K_(batch_idx));
  } else {
    MergeBatchFn work_fn(ctx->get_expected_sort_pipeline(), pass_idx_, batch_idx_);
    MergeBatchOnRetryFn on_retry_fn(*this);
    if (OB_FAIL(execute_with_retry(work_fn, on_retry_fn, retry_count))) {
      LOG_WARN("failed to execute expected merge batch task",
          KR(ret), K_(pass_idx), K_(batch_idx), K(retry_count));
    }
  }
  leave_task_(ctx, ret);
  return ret;
}

// ==================== ObSSDataCheckExpectedMergeCompleteTask ====================

ObSSDataCheckExpectedMergeCompleteTask::ObSSDataCheckExpectedMergeCompleteTask()
  : ObSSDataCheckExpectedSortBaseTask(TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_COMPLETE),
    is_inited_(false),
    is_complete_(false),
    final_pass_idx_(-1)
{
}

int ObSSDataCheckExpectedMergeCompleteTask::init(ObSSDataCheckCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected merge complete task initialized twice", KR(ret));
  } else if (OB_FAIL(init_ctx_(ctx))) {
    LOG_WARN("failed to initialize expected merge complete context", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedMergeCompleteTask::CompleteMergeFn::operator()()
{
  return pipeline_.complete_merge(final_pass_idx_);
}

int ObSSDataCheckExpectedMergeCompleteTask::process()
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObSSDataCheckCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected merge complete task is not initialized", KR(ret));
  } else if (OB_FAIL(inc_active_task_count_(ctx))) {
    LOG_WARN("failed to enter expected merge complete task", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("expected merge complete task has been stopped", KR(ret));
  } else if (is_complete_) {
    // Idempotent process retry after the final file has been published.
  } else if (final_pass_idx_ < 0 && OB_FAIL(ctx->get_expected_sort_pipeline().get_current_pass_idx(
          final_pass_idx_))) {
    LOG_WARN("failed to capture final expected merge pass", KR(ret));
  } else {
    CompleteMergeFn work_fn(ctx->get_expected_sort_pipeline(), final_pass_idx_);
    if (OB_FAIL(execute_with_retry(work_fn, retry_count))) {
      LOG_WARN("failed to complete expected merge", KR(ret), K_(final_pass_idx), K(retry_count));
    } else {
      is_complete_ = true;
    }
  }
  leave_task_(ctx, ret);
  return ret;
}

} // namespace storage
} // namespace oceanbase
