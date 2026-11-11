/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_dag_planner.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObIVFDagProcessor::ObIVFDagProcessor()
  : is_inited_(false)
{
}

int ObIVFDagProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DAG processor init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObIVFDagProcessor::cancel(const int ret_code)
{
  UNUSED(ret_code);
}

ObIVFDagPlanner::ObIVFDagPlanner()
  : is_inited_(false),
    allocator_(nullptr),
    process_type_(ObIVFProcessType::INVALID),
    processor_(nullptr),
    artifact_(nullptr),
    next_plan_id_(-1),
    current_task_plan_(nullptr),
    task_plans_(),
    task_result_allocator_(ObMemAttr(MTL_ID(), "IVFTaskResults", ObCtxIds::VECTOR_CTX_ID)),
    task_results_()
{
}

int ObIVFDagPlanner::init(
    common::ObIAllocator &allocator,
    const ObIVFProcessType process_type,
    ObIVFDagProcessor &processor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DAG planner init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_ivf_process_type(process_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF process type",
        K(ret), K(static_cast<int64_t>(process_type)));
  } else if (OB_UNLIKELY(!processor.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DAG processor is not initialized", K(ret));
  } else {
    allocator_ = &allocator;
    process_type_ = process_type;
    processor_ = &processor;
    next_plan_id_ = 0;
    task_plans_.set_block_allocator(common::ObWrapperAllocator(allocator));
    task_results_.set_allocator(&task_result_allocator_);
    is_inited_ = true;
  }
  return ret;
}

int ObIVFDagPlanner::start(
    const ObIVFTaskPlan *&first_task_plan)
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  first_task_plan = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DAG planner is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_NOT_NULL(current_task_plan_)
      || !task_plans_.empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF DAG planner already started",
        K(ret), KPC_(current_task_plan));
  } else if (OB_FAIL(prepare_first_task_plan(next_plan_id_, task_count))) {
    LOG_WARN("fail to prepare first IVF task plan", K(ret), K_(next_plan_id));
  } else if (OB_FAIL(create_and_activate_task_plan(
      task_count, first_task_plan))) {
    LOG_WARN("fail to create first IVF task plan", K(ret), K(task_count));
  }
  if (OB_FAIL(ret) && is_inited_) {
    cancel(ret);
  }
  return ret;
}

int ObIVFDagPlanner::process_task(
    const ObIVFTaskPlan &task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFTaskResult task_result;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DAG planner is not initialized", K(ret));
  } else if (OB_UNLIKELY(current_task_plan_ != &task_plan
      || task_plan.get_process_type() != process_type_
      || !task_plan.is_valid_task_idx(task_idx))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF task does not belong to current task plan",
        K(ret), K(task_plan), K(task_idx), KPC_(current_task_plan));
  } else if (OB_FAIL(task_result.init(task_plan, task_idx))) {
    LOG_WARN("fail to init IVF task result",
        K(ret), K(task_plan), K(task_idx));
  } else if (OB_FAIL(execute_process_task(
      task_plan, task_idx, task_result))) {
    LOG_WARN("fail to execute IVF process task",
        K(ret), K(task_plan), K(task_idx));
  } else if (OB_FAIL(task_result.seal())) {
    LOG_WARN("fail to seal IVF task result", K(ret), K(task_result));
  } else if (OB_FAIL(publish_task_result(task_result))) {
    LOG_WARN("fail to publish IVF task result", K(ret), K(task_result));
  }
  if (OB_FAIL(ret) && is_inited_ && OB_EAGAIN != ret) {
    cancel(ret);
  }
  return ret;
}

int ObIVFDagPlanner::finish_task_plan(
    const ObIVFTaskPlan &finished_task_plan,
    const ObIVFTaskPlan *&next_task_plan)
{
  int ret = OB_SUCCESS;
  bool process_finished = false;
  int64_t next_task_count = 0;
  next_task_plan = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DAG planner is not initialized", K(ret));
  } else if (OB_FAIL(check_task_plan_finished(finished_task_plan))) {
    LOG_WARN("IVF task plan result is incomplete",
        K(ret), K(finished_task_plan));
  } else if (OB_FAIL(finish_process_task_plan(
      finished_task_plan,
      task_results_,
      next_plan_id_,
      process_finished,
      next_task_count))) {
    LOG_WARN("fail to finish IVF process task plan",
        K(ret), K(finished_task_plan));
  } else if (process_finished) {
    if (OB_UNLIKELY(OB_ISNULL(artifact_) || !artifact_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF process finished without valid artifact", K(ret));
    } else {
      current_task_plan_ = nullptr;
      task_results_.reset();
      task_result_allocator_.reset();
    }
  } else if (OB_UNLIKELY(next_task_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF process returned empty next task plan",
        K(ret), K(finished_task_plan), K(next_task_count));
  } else if (OB_FAIL(create_and_activate_task_plan(
      next_task_count, next_task_plan))) {
    LOG_WARN("fail to create next IVF task plan",
        K(ret), K(finished_task_plan), K(next_task_count));
  }
  if (OB_FAIL(ret) && is_inited_) {
    cancel(ret);
  }
  return ret;
}

int ObIVFDagPlanner::create_and_activate_task_plan(
    const int64_t task_count,
    const ObIVFTaskPlan *&task_plan)
{
  int ret = OB_SUCCESS;
  ObIVFTaskPlan *new_task_plan = nullptr;
  bool task_plan_retained = false;
  task_plan = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    void *buf = allocator_->alloc(sizeof(ObIVFTaskPlan));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate IVF task plan", K(ret));
    } else {
      new_task_plan = new (buf) ObIVFTaskPlan();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_task_plan->init(
      process_type_, next_plan_id_, task_count))) {
    LOG_WARN("fail to init IVF task plan",
        K(ret), K_(next_plan_id), K(task_count));
  } else if (OB_FAIL(task_plans_.push_back(new_task_plan))) {
    LOG_WARN("fail to retain IVF task plan", K(ret), K_(next_plan_id));
  } else if (FALSE_IT(task_plan_retained = true)) {
  } else if (OB_FAIL(activate_task_plan(*new_task_plan))) {
    LOG_WARN("fail to activate IVF task plan", K(ret), KPC(new_task_plan));
  } else {
    task_plan = new_task_plan;
    new_task_plan = nullptr;
    ++next_plan_id_;
  }
  if (OB_NOT_NULL(new_task_plan) && !task_plan_retained) {
    new_task_plan->~ObIVFTaskPlan();
    allocator_->free(new_task_plan);
  }
  return ret;
}

int ObIVFDagPlanner::activate_task_plan(
    const ObIVFTaskPlan &task_plan)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF task plan to activate", K(ret), K(task_plan));
  } else {
    task_results_.reset();
    task_result_allocator_.reset();
    if (OB_FAIL(task_results_.prepare_allocate(task_plan.get_task_count()))) {
      LOG_WARN("fail to allocate IVF task-result slots", K(ret), K(task_plan));
    } else {
      current_task_plan_ = &task_plan;
    }
  }
  return ret;
}

int ObIVFDagPlanner::publish_task_result(
    const ObIVFTaskResult &task_result)
{
  int ret = OB_SUCCESS;
  const int64_t task_idx = task_result.get_task_idx();
  if (OB_UNLIKELY(OB_ISNULL(current_task_plan_)
      || !task_result.is_valid()
      || !task_result.is_sealed()
      || task_result.get_process_type() != process_type_
      || task_result.get_plan_id() != current_task_plan_->get_plan_id()
      || task_idx < 0
      || task_idx >= task_results_.count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF task result for current task plan",
        K(ret), K(task_result), KPC_(current_task_plan));
  } else if (task_results_.at(task_idx).is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF task result already published", K(ret), K(task_result));
  } else {
    task_results_.at(task_idx) = task_result;
  }
  return ret;
}

int ObIVFDagPlanner::check_task_plan_finished(
    const ObIVFTaskPlan &task_plan) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(current_task_plan_ != &task_plan
      || task_plan.get_process_type() != process_type_
      || task_results_.count() != task_plan.get_task_count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF task plan is not active",
        K(ret), K(task_plan), KPC_(current_task_plan));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_results_.count(); ++i) {
      const ObIVFTaskResult &result = task_results_.at(i);
      if (OB_UNLIKELY(!result.is_valid()
          || !result.is_sealed()
          || result.get_task_idx() != i
          || result.get_process_type() != process_type_
          || result.get_plan_id() != task_plan.get_plan_id())) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("missing or invalid IVF task result",
            K(ret), K(i), K(result), K(task_plan));
      }
    }
  }
  return ret;
}

void ObIVFDagPlanner::cancel(const int ret_code)
{
  const int cancel_ret = OB_SUCCESS == ret_code ? OB_CANCELED : ret_code;
  if (OB_NOT_NULL(processor_)) {
    processor_->cancel(cancel_ret);
  }
}

int ObIVFDagPlanner::set_artifact(
    const ObIVFDagArtifact &artifact)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_NOT_NULL(artifact_))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DAG artifact already set", K(ret));
  } else if (OB_UNLIKELY(!artifact.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DAG artifact", K(ret));
  } else {
    artifact_ = &artifact;
  }
  return ret;
}

int ObIVFDagPlanner::get_artifact(
    const ObIVFDagArtifact *&artifact) const
{
  int ret = OB_SUCCESS;
  artifact = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(processor_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_ISNULL(artifact_) || !artifact_->is_valid())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF DAG artifact is not ready", K(ret));
  } else {
    artifact = artifact_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
