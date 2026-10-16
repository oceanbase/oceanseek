/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/ob_async_index_merge_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
namespace compaction
{

ObAsyncIndexMergePrepareTask::ObAsyncIndexMergePrepareTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
    is_inited_(false),
    merge_dag_(nullptr)
{
}

int ObAsyncIndexMergePrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(dag_) || !is_compaction_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
  } else {
    merge_dag_ = static_cast<ObTabletMergeDag *>(dag_);
    is_inited_ = true;
  }
  return ret;
}

int ObAsyncIndexMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObAsyncIndexMergeCtx *ctx = nullptr;
  bool finish_flag = false;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(merge_dag_->prepare_merge_ctx(finish_flag))) {
    LOG_WARN("failed to prepare async index merge ctx", K(ret), KPC(merge_dag_));
  } else if (OB_ISNULL(ctx = static_cast<ObAsyncIndexMergeCtx *>(merge_dag_->get_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (finish_flag) {
  } else if (OB_FAIL(ctx->init_inc_major_uncommit_tx_info())) {
    LOG_WARN("failed to init uncommit tx info", K(ret));
  } else if (OB_FAIL(merge_dag_->generate_merge_task(*ctx, this))) {
    LOG_WARN("failed to generate async index merge task", K(ret), KPC(ctx));
  }
  return ret;
}

ObAsyncIndexMergeFinishTask::ObAsyncIndexMergeFinishTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_FINISH),
    is_inited_(false),
    merge_dag_(nullptr)
{
}

int ObAsyncIndexMergeFinishTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(dag_) || !is_compaction_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
  } else {
    merge_dag_ = static_cast<ObTabletMergeDag *>(dag_);
    is_inited_ = true;
  }
  return ret;
}

int ObAsyncIndexMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(merge_dag_) || OB_ISNULL(merge_dag_->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObAsyncIndexMergeCtx *ctx = static_cast<ObAsyncIndexMergeCtx *>(merge_dag_->get_ctx());
    SET_MEM_CTX(ctx->mem_ctx_);
    if (OB_FAIL(ctx->update_tablet_after_merge())) {
      LOG_WARN("failed to update tablet after async index merge", K(ret), KPC(ctx));
    } else {
      LOG_INFO("success to update tablet after async index merge", K(ret), KPC(ctx));
    }
  }
  return ret;
}

ObAsyncIndexMergeTask::ObAsyncIndexMergeTask()
  : ObITask(ObITask::TASK_TYPE_MACROMERGE),
    allocator_("InvMergerArena"),
    idx_(0),
    ctx_(nullptr),
    merger_(nullptr),
    is_inited_(false)
{
}

ObAsyncIndexMergeTask::~ObAsyncIndexMergeTask()
{
  if (OB_NOT_NULL(merger_)) {
    merger_->~ObPartitionInvSSTableMerger();
    merger_ = nullptr;
  }
}

int ObAsyncIndexMergeTask::init(const int64_t idx, ObAsyncIndexMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (idx < 0 || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(allocator_.bind_mem_ctx(ctx.mem_ctx_))) {
  } else if (OB_ISNULL(merger_ = OB_NEWx(ObPartitionInvSSTableMerger, &allocator_, allocator_, ctx.static_param_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    idx_ = idx;
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObAsyncIndexMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(merger_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(merger_->merge_partition(*ctx_, idx_))) {
    LOG_WARN("failed to merge async index partition", K(ret), K(idx_), KPC(ctx_));
  }
  return ret;
}

int ObAsyncIndexMergeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (idx_ + 1 == ctx_->get_concurrent_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObAsyncIndexMergeTask *merge_task = nullptr;
    ObTabletMergeDag *merge_dag = static_cast<ObTabletMergeDag *>(dag_);
    if (OB_FAIL(merge_dag->alloc_task(merge_task))) {
      LOG_WARN("failed to alloc next async index merge task", K(ret));
    } else if (OB_FAIL(merge_task->init(idx_ + 1, *ctx_))) {
      LOG_WARN("failed to init next async index merge task", K(ret), K(idx_));
    } else {
      next_task = merge_task;
    }
  }
  return ret;
}

ObAsyncIndexMergeDag::ObAsyncIndexMergeDag(const share::ObDagType::ObDagTypeEnum type)
  : ObTabletMergeDag(type)
{
}

int ObAsyncIndexMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = ObTabletMergeDag::init_by_param(param);
  if (OB_FAIL(ret)) {
  } else if (!get_param().is_inv_index_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected param for async index merge dag", K(ret), K(get_param()));
  }
  return ret;
}

int ObAsyncIndexMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAsyncIndexMergePrepareTask *task = nullptr;
  if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    LOG_WARN("failed to create async index prepare task", K(ret), K(get_param()));
  } else {
    LOG_INFO("success to create async index prepare task", K(ret), K(get_param()));
  }
  return ret;
}

int ObAsyncIndexMergeDag::generate_merge_task(ObBasicTabletMergeCtx &ctx, share::ObITask *prepare_task)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexMergeTask *merge_task = nullptr;
  ObAsyncIndexMergeFinishTask *finish_task = nullptr;
  SET_MEM_CTX(ctx.mem_ctx_);
  const int64_t start_task_idx = ctx.get_start_task_idx();
  if (start_task_idx < 0 || start_task_idx > ctx.get_concurrent_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected start task idx", K(ret), K(start_task_idx), K(ctx.get_concurrent_cnt()));
  } else if (start_task_idx == ctx.get_concurrent_cnt()) {
    LOG_INFO("all merge task finish, just create finish task", K(ret), K(start_task_idx), K(ctx.get_concurrent_cnt()));
    if (OB_FAIL(create_task(prepare_task, finish_task))) {
      LOG_WARN("failed to create async index finish task", K(ret), K(ctx));
    }
  } else if (OB_FAIL(create_task(
      prepare_task, merge_task, start_task_idx, static_cast<ObAsyncIndexMergeCtx &>(ctx)))) {
    LOG_WARN("failed to create async index merge task", K(ret), K(start_task_idx), K(ctx));
  } else if (OB_FAIL(create_task(merge_task, finish_task))) {
    LOG_WARN("failed to create async index finish task", K(ret), K(ctx));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
