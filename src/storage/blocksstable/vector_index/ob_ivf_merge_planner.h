/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_PLANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_PLANNER_H_

#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"

namespace oceanbase
{
namespace storage
{

// Execution vocabulary only. Strategy state and transitions belong to each
// concrete planner; this class owns the common task/barrier/artifact protocol.
class ObIVFMergePlanner : public ObIVFDagPlanner
{
public:
  ObIVFMergePlanner();
  virtual ~ObIVFMergePlanner() = default;
  ObIVFMergeStage get_stage() const { return stage_; }
  ObIVFMergeStep get_step() const { return step_; }
  int64_t get_depth() const { return depth_; }
  void set_perf_observer(ObIVFMergePerfObserver *observer) { perf_observer_ = observer; }
  const ObIVFMergeStepPerf &get_step_perf() const { return step_perf_; }

protected:
  int init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor,
      const ObIVFProcessType process_type);
  virtual void reset_state() = 0;
  virtual int advance_state(const ObIVFMergeStepResult &result) = 0;
  virtual void get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const = 0;

private:
  int prepare_first_task_plan(const int64_t plan_id, int64_t &task_count) final;
  int execute_process_task(const ObIVFTaskPlan &task_plan, const int64_t task_idx,
      ObIVFTaskResult &task_result) final;
  int finish_process_task_plan(const ObIVFTaskPlan &finished_task_plan,
      const common::ObIArray<ObIVFTaskResult> &task_results, const int64_t next_plan_id,
      bool &process_finished, int64_t &next_task_count) final;
  int prepare_operation(const int64_t plan_id, int64_t &task_count);
  ObIVFMergeProcessor *processor_;
  // Immutable execution view for the active plan, never used to choose a transition.
  ObIVFMergeStage stage_;
  ObIVFMergeStep step_;
  int64_t depth_;
  ObIVFMergeArtifact final_artifact_;
  ObIVFMergePerfObserver *perf_observer_;
  ObIVFMergeStepPerf step_perf_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFMergePlanner);
};

// Owns only FULL_REBUILD transitions. Reuse planners install this flow at a
// completed fallback barrier, retaining their original DAG and plan IDs.
class ObIVFFullRebuildState
{
public:
  ObIVFFullRebuildState() { reset(); }
  void reset() { state_ = State::CLUSTER; depth_ = 0; }
  int advance(const ObIVFMergeStepResult &result);
  void get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const;
private:
  enum class State : uint8_t
  {
    SAMPLE_PREPARE, CLUSTER, ASSIGN, UPDATE, NEIGHBORHOOD_ADD, NEIGHBORHOOD_BUILD,
    REFINE_SAMPLE_PREPARE, REFINE_TRAIN, REFINE_ASSIGN, REFINE_UPDATE, COMPACT, SOAR,
    CENTROID_INDEX, QUANT, RAW_WRITE, FINISH
  };
  State state_;
  int64_t depth_;
};

// Initial DDL builds and full rebuilds share the hierarchy state machine.
// The process type remains attached to every task and artifact.
class ObIVFFullBuildPlanner : public ObIVFMergePlanner
{
public:
  int init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor,
      const ObIVFProcessType process_type = ObIVFProcessType::FULL_REBUILD)
  {
    return !is_ivf_full_build_process_type(process_type) ? OB_INVALID_ARGUMENT
        : ObIVFMergePlanner::init(allocator, processor, process_type);
  }
protected:
  void reset_state() override { state_.reset(); }
  int advance_state(const ObIVFMergeStepResult &result) override { return state_.advance(result); }
  void get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const override
  { state_.get_operation(stage, step, depth); }
private:
  ObIVFFullRebuildState state_;
};

class ObIVFFullRebuildPlanner final : public ObIVFFullBuildPlanner
{
public:
  int init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor)
  { return ObIVFFullBuildPlanner::init(allocator, processor, ObIVFProcessType::FULL_REBUILD); }
};

class ObIVFConcatPlanner final : public ObIVFMergePlanner
{
public:
  ObIVFConcatPlanner() : state_(State::PRIOR) {}
  int init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor);
protected:
  void reset_state() override { state_ = State::PRIOR; }
  int advance_state(const ObIVFMergeStepResult &result) override;
  void get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const override;
private:
  enum class State : uint8_t
  {
    PRIOR, CENTROID_INDEX, QUANT, RAW_WRITE, FINISH
  };
  State state_;
};

class ObIVFInsertionPlanner final : public ObIVFMergePlanner
{
public:
  // One immutable-center insertion execution.
  ObIVFInsertionPlanner() : state_(State::PRIOR), depth_(0), fallback_() {}
  int init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor);
protected:
  void reset_state() override { state_ = State::PRIOR; depth_ = 0; fallback_.reset(); }
  int advance_state(const ObIVFMergeStepResult &result) override;
  void get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const override;
private:
  enum class State : uint8_t
  {
    PRIOR, ROOT_ASSIGN, UPDATE, NEIGHBORHOOD_ADD,
    NEIGHBORHOOD_BUILD, COMPACT, SOAR, CENTROID_INDEX, QUANT, RAW_WRITE, FALLBACK, FINISH
  };
  State state_;
  int64_t depth_;
  ObIVFFullRebuildState fallback_;
};

} // namespace storage
} // namespace oceanbase
#endif
