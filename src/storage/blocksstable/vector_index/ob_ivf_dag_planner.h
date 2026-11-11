/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DAG_PLANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DAG_PLANNER_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/vector_index/ob_ivf_task_plan.h"

namespace oceanbase
{
namespace storage
{

class ObIVFDagArtifact
{
public:
  virtual ~ObIVFDagArtifact() = default;
  virtual bool is_valid() const = 0;
};

class ObIVFDagProcessor
{
public:
  ObIVFDagProcessor();
  virtual ~ObIVFDagProcessor() = default;
  int init();
  bool is_inited() const { return is_inited_; }
  // Cleanup may be forwarded by different DAG adapters and must be idempotent.
  virtual void cancel(const int ret_code);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDagProcessor);
};

// Common logical planner consumed by DDL and Merge DAG adapters. It owns the
// immutable TaskPlans and task-result slots for one IVF process. Concrete
// planners only implement process-specific step preparation and execution.
class ObIVFDagPlanner
{
public:
  ObIVFDagPlanner();
  virtual ~ObIVFDagPlanner() = default;

  int start(const ObIVFTaskPlan *&first_task_plan);
  int process_task(const ObIVFTaskPlan &task_plan, const int64_t task_idx);
  // On success, a null next_task_plan means that the logical process finished.
  int finish_task_plan(
      const ObIVFTaskPlan &finished_task_plan,
      const ObIVFTaskPlan *&next_task_plan);
  void cancel(const int ret_code);

  bool is_inited() const { return is_inited_; }
  ObIVFProcessType get_process_type() const { return process_type_; }
  int get_artifact(const ObIVFDagArtifact *&artifact) const;

protected:
  int init(
      common::ObIAllocator &allocator,
      const ObIVFProcessType process_type,
      ObIVFDagProcessor &processor);
  int set_artifact(const ObIVFDagArtifact &artifact);

  virtual int prepare_first_task_plan(
      const int64_t plan_id,
      int64_t &task_count) = 0;
  virtual int execute_process_task(
      const ObIVFTaskPlan &task_plan,
      const int64_t task_idx,
      ObIVFTaskResult &task_result) = 0;
  virtual int finish_process_task_plan(
      const ObIVFTaskPlan &finished_task_plan,
      const common::ObIArray<ObIVFTaskResult> &task_results,
      const int64_t next_plan_id,
      bool &process_finished,
      int64_t &next_task_count) = 0;

private:
  int create_and_activate_task_plan(
      const int64_t task_count,
      const ObIVFTaskPlan *&task_plan);
  int activate_task_plan(const ObIVFTaskPlan &task_plan);
  int publish_task_result(const ObIVFTaskResult &task_result);
  int check_task_plan_finished(const ObIVFTaskPlan &task_plan) const;

  bool is_inited_;
  common::ObIAllocator *allocator_;
  ObIVFProcessType process_type_;
  ObIVFDagProcessor *processor_;
  const ObIVFDagArtifact *artifact_;
  int64_t next_plan_id_;
  const ObIVFTaskPlan *current_task_plan_;
  common::ObArray<ObIVFTaskPlan *, common::ObWrapperAllocator> task_plans_;
  // Results are consumed at a phase barrier. They must not accumulate in the
  // caller's process-lifetime arena when ObFixedArray::reset() calls free().
  common::ObArenaAllocator task_result_allocator_;
  common::ObFixedArray<ObIVFTaskResult, common::ObIAllocator> task_results_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFDagPlanner);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DAG_PLANNER_H_
