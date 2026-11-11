/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_task_plan.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObIVFTaskPlan::ObIVFTaskPlan()
  : is_inited_(false),
    process_type_(ObIVFProcessType::INVALID),
    plan_id_(-1),
    task_count_(0)
{
}

int ObIVFTaskPlan::init(
    const ObIVFProcessType process_type,
    const int64_t plan_id,
    const int64_t task_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF task plan init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_ivf_process_type(process_type)
      || plan_id < 0 || task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF task plan header",
        K(ret), K(static_cast<int64_t>(process_type)), K(plan_id), K(task_count));
  } else {
    process_type_ = process_type;
    plan_id_ = plan_id;
    task_count_ = task_count;
    is_inited_ = true;
  }
  return ret;
}

bool ObIVFTaskPlan::is_valid() const
{
  return is_inited_ && is_valid_ivf_process_type(process_type_)
      && plan_id_ >= 0 && task_count_ > 0;
}

ObIVFTaskResult::ObIVFTaskResult()
  : is_inited_(false),
    process_type_(ObIVFProcessType::INVALID),
    plan_id_(-1),
    task_idx_(-1),
    is_sealed_(false)
{
}

int ObIVFTaskResult::init(
    const ObIVFTaskPlan &task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF task result init twice", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || !task_plan.is_valid_task_idx(task_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF task plan or task index",
        K(ret), K(task_plan), K(task_idx));
  } else {
    process_type_ = task_plan.get_process_type();
    plan_id_ = task_plan.get_plan_id();
    task_idx_ = task_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFTaskResult::seal()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF task result is not initialized", K(ret));
  } else if (OB_UNLIKELY(is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF task result is already sealed", K(ret), KPC(this));
  } else {
    is_sealed_ = true;
  }
  return ret;
}

bool ObIVFTaskResult::is_valid() const
{
  return is_inited_ && is_valid_ivf_process_type(process_type_)
      && plan_id_ >= 0 && task_idx_ >= 0;
}

} // namespace storage
} // namespace oceanbase
