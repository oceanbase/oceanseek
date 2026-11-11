/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObIVFMergePlanner::ObIVFMergePlanner()
  : processor_(nullptr), stage_(ObIVFMergeStage::INVALID), step_(ObIVFMergeStep::INVALID),
    depth_(IVF_MERGE_INVALID_DEPTH), final_artifact_(), perf_observer_(nullptr), step_perf_()
{
}

int ObIVFMergePlanner::init(common::ObIAllocator &allocator,
    ObIVFMergeProcessor &processor, const ObIVFProcessType process_type)
{
  int ret = processor.process_type_ != ObIVFProcessType::INVALID ? OB_INIT_TWICE
      : processor.is_runtime_enabled() && process_type != processor.get_process_type() ? OB_STATE_NOT_MATCH
      : ObIVFDagPlanner::init(allocator, process_type, processor);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to initialize IVF merge planner", K(ret), K(process_type),
        "processor_type", processor.get_process_type());
  }
  if (OB_SUCC(ret)) {
    processor_ = &processor;
    processor.process_type_ = process_type;
  }
  return ret;
}

int ObIVFMergePlanner::prepare_first_task_plan(const int64_t plan_id, int64_t &task_count)
{
  reset_state();
  return prepare_operation(plan_id, task_count);
}

int ObIVFMergePlanner::prepare_operation(const int64_t plan_id, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObIVFMergeStage stage;
  ObIVFMergeStep step;
  int64_t depth;
  const int64_t started_at = ObTimeUtility::current_time();
  get_operation(stage, step, depth);
  if (processor_ != nullptr && stage == ObIVFMergeStage::CENTROID_INDEX) {
    bool required = false;
    if (OB_FAIL(processor_->resolve_centroid_index_operation(step, depth, required))) {
      LOG_WARN("[DISK_BBQ] failed to resolve centroid index operation", K(ret), K(plan_id));
    } else if (!required) {
      // No empty task/barrier is needed below the hierarchy threshold.
      ObIVFMergeStepResult skipped;
      skipped.centroid_index_finished_ = true;
      if (OB_FAIL(advance_state(skipped))) {
        LOG_WARN("[DISK_BBQ] failed to skip flat centroid index stage", K(ret), K(plan_id));
      } else {
        get_operation(stage, step, depth);
      }
    }
  }
  if (OB_NOT_NULL(perf_observer_)) {
    step_perf_ = ObIVFMergeStepPerf();
    step_perf_.stage_ = stage;
    step_perf_.step_ = step;
    if (processor_ != nullptr && step == ObIVFMergeStep::REFINE_TRAIN) {
      step_perf_.refine_phase_ = stage == ObIVFMergeStage::CENTROID_INDEX
          ? processor_->centroid_index_refine_phase() : processor_->refine_train_phase();
    }
    step_perf_.quantize_in_soar_ = processor_ != nullptr
        && stage != ObIVFMergeStage::CENTROID_INDEX
        && (step == ObIVFMergeStep::SOAR_ASSIGN || step == ObIVFMergeStep::QUANT)
        && processor_->quantizes_during_soar();
    step_perf_.depth_ = depth;
    step_perf_.plan_id_ = plan_id;
    step_perf_.start_us_ = started_at;
    perf_observer_->on_step_perf(step_perf_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(processor_)) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation", "ObIVFMergePlanner::prepare_operation",
        KP(processor_), K(plan_id));
    ret = OB_NOT_INIT;
  } else if (!is_valid_ivf_merge_operation(stage, step, depth)) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFMergePlanner::prepare_operation",
        K(stage), K(step), K(depth), K(plan_id));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(processor_->prepare_step(stage, step, depth, plan_id, task_count))) {
    LOG_WARN("failed to prepare IVF operation", K(ret), K(stage), K(step), K(depth), K(plan_id));
  } else if (task_count <= 0) {
    LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
        "ObIVFMergePlanner::prepare_operation", K(task_count), K(plan_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    stage_ = stage;
    step_ = step;
    depth_ = depth;
  }
  if (OB_NOT_NULL(perf_observer_)) {
    step_perf_.prepared_us_ = ObTimeUtility::current_time();
    step_perf_.task_count_ = task_count;
    step_perf_.ret_code_ = ret;
    if (OB_FAIL(ret)) { step_perf_.end_us_ = step_perf_.prepared_us_; }
    perf_observer_->on_step_perf(step_perf_);
  }
  return ret;
}

int ObIVFMergePlanner::execute_process_task(const ObIVFTaskPlan &task_plan,
    const int64_t task_idx, ObIVFTaskResult &task_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(processor_) || !task_plan.is_valid()
      || !is_valid_ivf_merge_operation(stage_, step_, depth_)) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergePlanner::execute_process_task", KP(processor_), K(stage_), K(step_), K(depth_), K(task_idx));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ret = processor_->execute_task(stage_, step_, depth_, task_idx, task_result);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "processor_->execute_task", K(task_idx));
    }
  }
  return ret;
}

int ObIVFMergePlanner::finish_process_task_plan(const ObIVFTaskPlan &finished_task_plan,
    const common::ObIArray<ObIVFTaskResult> &task_results, const int64_t next_plan_id,
    bool &process_finished, int64_t &next_task_count)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(perf_observer_)) {
    step_perf_.finish_begin_us_ = ObTimeUtility::current_time();
    perf_observer_->on_step_perf(step_perf_);
  }
  UNUSED(task_results);
  process_finished = false;
  next_task_count = 0;
  ObIVFMergeStepResult result;
  if (OB_ISNULL(processor_) || next_plan_id != finished_task_plan.get_plan_id() + 1) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergePlanner::finish_process_task_plan", KP(processor_), K(next_plan_id),
        "finished_task_plan_get_plan_id", finished_task_plan.get_plan_id());
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(processor_->finish_step(stage_, step_, depth_,
      finished_task_plan.get_plan_id(), result))) {
    LOG_WARN("failed to finish IVF operation", K(ret), K_(stage), K_(step), K_(depth));
  } else if (!result.is_valid() || result.plan_id_ != finished_task_plan.get_plan_id()
      || result.stage_ != stage_ || result.step_ != step_ || result.depth_ != depth_) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergePlanner::finish_process_task_plan", K(result.plan_id_), "finished_task_plan_get_plan_id",
        finished_task_plan.get_plan_id(), K(result.stage_), K(stage_), K(result.step_), K(step_), K(result.depth_),
        K(depth_), K(next_plan_id));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(advance_state(result))) {
    LOG_WARN("failed to advance IVF strategy", K(ret), K(result));
  } else {
    ObIVFMergeStage stage;
    ObIVFMergeStep step;
    int64_t depth;
    get_operation(stage, step, depth);
    if (stage == ObIVFMergeStage::FINISH) {
      if (!result.merge_finished_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergePlanner::finish_process_task_plan", K(result.merge_finished_), K(next_plan_id));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(processor_->build_final_artifact(final_artifact_))) {
        LOG_WARN("failed to build IVF artifact", K(ret));
      } else if (OB_FAIL(set_artifact(final_artifact_))) {
        LOG_WARN("failed to publish IVF artifact", K(ret));
      } else {
        stage_ = stage;
        step_ = step;
        depth_ = depth;
        process_finished = true;
      }
    } else if (result.merge_finished_) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergePlanner::finish_process_task_plan", K(result.merge_finished_), K(next_plan_id));
      ret = OB_STATE_NOT_MATCH;
    }
  }
  if (OB_NOT_NULL(perf_observer_)) {
    step_perf_.end_us_ = ObTimeUtility::current_time();
    step_perf_.ret_code_ = ret;
    perf_observer_->on_step_perf(step_perf_);
  }
  if (OB_SUCC(ret) && !process_finished) {
    ret = prepare_operation(next_plan_id, next_task_count);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to prepare next IVF operation", K(ret), K(next_plan_id));
    }
  }
  return ret;
}

int ObIVFFullRebuildState::advance(const ObIVFMergeStepResult &result)
{
  int ret = OB_SUCCESS;
  if (result.quality_fallback_) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFFullRebuildState::advance",
        K(result.quality_fallback_));
    return OB_STATE_NOT_MATCH;
  }
  switch (state_) {
    case State::SAMPLE_PREPARE: state_ = State::CLUSTER; break;
    case State::CLUSTER: state_ = State::ASSIGN; break;
    case State::ASSIGN: state_ = State::UPDATE; break;
    case State::UPDATE:
      if (!result.hierarchy_finished_) {
        ++depth_;
        state_ = result.next_cluster_needs_samples_ ? State::SAMPLE_PREPARE : State::CLUSTER;
      } else {
        depth_ = IVF_MERGE_INVALID_DEPTH;
        switch (result.neighborhood_mode_) {
          case ObIVFNeighborhoodMode::HGRAPH: state_ = State::NEIGHBORHOOD_ADD; break;
          case ObIVFNeighborhoodMode::BRUTE_FORCE: state_ = State::NEIGHBORHOOD_BUILD; break;
          case ObIVFNeighborhoodMode::SKIP: state_ = result.refine_skipped_ ? State::COMPACT
              : (result.next_refine_needs_samples_ ? State::REFINE_SAMPLE_PREPARE : State::REFINE_TRAIN); break;
          default:
            LOG_WARN("unexpected IVF planner state", "ret", OB_STATE_NOT_MATCH, K(state_),
                K(result.stage_), K(result.step_), K(result.neighborhood_mode_));
            ret = OB_STATE_NOT_MATCH;
            break;
        }
      }
      break;
    case State::NEIGHBORHOOD_ADD: state_ = State::NEIGHBORHOOD_BUILD; break;
    case State::NEIGHBORHOOD_BUILD:
      state_ = result.refine_skipped_ ? State::COMPACT
              : (result.next_refine_needs_samples_ ? State::REFINE_SAMPLE_PREPARE : State::REFINE_TRAIN);
      break;
    case State::REFINE_SAMPLE_PREPARE: state_ = State::REFINE_TRAIN; break;
    case State::REFINE_TRAIN:
      if (result.refine_train_finished_) { state_ = State::REFINE_ASSIGN; }
      break;
    case State::REFINE_ASSIGN:
      state_ = result.refine_has_changes_ ? State::REFINE_UPDATE : State::COMPACT;
      break;
    case State::REFINE_UPDATE: state_ = State::COMPACT; break;
    case State::COMPACT: state_ = State::SOAR; break;
    case State::SOAR: state_ = State::CENTROID_INDEX; break;
    case State::CENTROID_INDEX:
      if (result.centroid_index_finished_) { state_ = State::QUANT; }
      break;
    case State::QUANT: state_ = State::RAW_WRITE; break;
    case State::RAW_WRITE:
      if (!result.merge_finished_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFFullRebuildState::advance",
            K(result.merge_finished_));
        ret = OB_STATE_NOT_MATCH;
      }
      else { state_ = State::FINISH; }
      break;
    default:
      LOG_WARN("unexpected IVF planner state", "ret", OB_STATE_NOT_MATCH, K(state_),
          K(result.stage_), K(result.step_), K(result.neighborhood_mode_));
      ret = OB_STATE_NOT_MATCH;
      break;
  }
  return ret;
}

void ObIVFFullRebuildState::get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const
{
  stage = ObIVFMergeStage::INVALID;
  step = ObIVFMergeStep::INVALID;
  depth = depth_;
  switch (state_) {
    case State::SAMPLE_PREPARE: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE; break;
    case State::CLUSTER: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_CLUSTER; break;
    case State::ASSIGN: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_ASSIGN; break;
    case State::UPDATE: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_UPDATE; break;
    case State::NEIGHBORHOOD_ADD: stage = ObIVFMergeStage::NEIGHBORHOOD; step = ObIVFMergeStep::NEIGHBORHOOD_ADD; break;
    case State::NEIGHBORHOOD_BUILD: stage = ObIVFMergeStage::NEIGHBORHOOD; step = ObIVFMergeStep::NEIGHBORHOOD_BUILD; break;
    case State::REFINE_SAMPLE_PREPARE: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_SAMPLE_PREPARE; break;
    case State::REFINE_TRAIN: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_TRAIN; break;
    case State::REFINE_ASSIGN: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_ASSIGN; break;
    case State::REFINE_UPDATE: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_UPDATE; break;
    case State::COMPACT: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_COMPACT; break;
    case State::SOAR: stage = ObIVFMergeStage::SOAR; step = ObIVFMergeStep::SOAR_ASSIGN; break;
    case State::CENTROID_INDEX: stage = ObIVFMergeStage::CENTROID_INDEX; step = ObIVFMergeStep::INVALID; break;
    case State::QUANT: stage = ObIVFMergeStage::QUANT; step = ObIVFMergeStep::QUANT; break;
    case State::RAW_WRITE: stage = ObIVFMergeStage::RAW_WRITE; step = ObIVFMergeStep::RAW_WRITE; break;
    case State::FINISH: stage = ObIVFMergeStage::FINISH; step = ObIVFMergeStep::INVALID; break;
  }
}

int ObIVFConcatPlanner::init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor)
{
  return ObIVFMergePlanner::init(allocator, processor, ObIVFProcessType::CONCATENATION);
}

int ObIVFConcatPlanner::advance_state(const ObIVFMergeStepResult &result)
{
  int ret = OB_SUCCESS;
  switch (state_) {
    case State::PRIOR:
      if (!result.prior_finished_ || result.quality_fallback_) { ret = OB_STATE_NOT_MATCH; }
      else { state_ = State::CENTROID_INDEX; }
      break;
    case State::CENTROID_INDEX:
      if (result.centroid_index_finished_) { state_ = State::QUANT; }
      break;
    case State::QUANT: state_ = State::RAW_WRITE; break;
    case State::RAW_WRITE:
      if (!result.merge_finished_) { ret = OB_STATE_NOT_MATCH; }
      else { state_ = State::FINISH; }
      break;
    default: ret = OB_STATE_NOT_MATCH; break;
  }
  if (OB_FAIL(ret)) { LOG_WARN("invalid concat transition", K(ret), K(state_), K(result)); }
  return ret;
}

void ObIVFConcatPlanner::get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const
{
  stage = ObIVFMergeStage::INVALID;
  step = ObIVFMergeStep::INVALID;
  depth = IVF_MERGE_INVALID_DEPTH;
  switch (state_) {
    case State::PRIOR: stage = ObIVFMergeStage::PRIOR; step = ObIVFMergeStep::PRIOR_WORK; depth = 0; break;
    case State::CENTROID_INDEX: stage = ObIVFMergeStage::CENTROID_INDEX; break;
    case State::QUANT: stage = ObIVFMergeStage::QUANT; step = ObIVFMergeStep::QUANT; break;
    case State::RAW_WRITE: stage = ObIVFMergeStage::RAW_WRITE; step = ObIVFMergeStep::RAW_WRITE; break;
    case State::FINISH: stage = ObIVFMergeStage::FINISH; break;
  }
}

int ObIVFInsertionPlanner::init(common::ObIAllocator &allocator, ObIVFMergeProcessor &processor)
{
  return ObIVFMergePlanner::init(allocator, processor, ObIVFProcessType::INSERTION);
}

int ObIVFInsertionPlanner::advance_state(const ObIVFMergeStepResult &result)
{
  int ret = OB_SUCCESS;
  if (state_ == State::FALLBACK) {
    return fallback_.advance(result);
  } else if (result.quality_fallback_) {
    if (state_ != State::PRIOR && state_ != State::ROOT_ASSIGN) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFInsertionPlanner::advance_state", K(state_));
      return OB_STATE_NOT_MATCH;
    }
    fallback_.reset();
    state_ = State::FALLBACK;
    return OB_SUCCESS;
  }
  switch (state_) {
    case State::PRIOR:
      // Import retained centers; old vectors keep their encoded relations.
      if (result.prior_finished_) { state_ = State::ROOT_ASSIGN; }
      break;
    case State::ROOT_ASSIGN: state_ = State::UPDATE; break;
    case State::UPDATE:
      if (!result.hierarchy_finished_) {
        return OB_STATE_NOT_MATCH; // Insertion retains every old leaf without splitting.
      } else {
        depth_ = IVF_MERGE_INVALID_DEPTH;
        switch (result.neighborhood_mode_) {
          case ObIVFNeighborhoodMode::HGRAPH: state_ = State::NEIGHBORHOOD_ADD; break;
          case ObIVFNeighborhoodMode::BRUTE_FORCE: state_ = State::NEIGHBORHOOD_BUILD; break;
          case ObIVFNeighborhoodMode::SKIP: state_ = State::COMPACT; break;
          default:
            LOG_WARN("unexpected IVF planner state", "ret", OB_STATE_NOT_MATCH, K(state_),
                K(result.stage_), K(result.step_), K(result.neighborhood_mode_));
            ret = OB_STATE_NOT_MATCH;
            break;
        }
      }
      break;
    case State::NEIGHBORHOOD_ADD: state_ = State::NEIGHBORHOOD_BUILD; break;
    case State::NEIGHBORHOOD_BUILD:
      state_ = State::COMPACT;
      break;
    case State::COMPACT: state_ = State::SOAR; break;
    case State::SOAR: state_ = State::CENTROID_INDEX; break;
    case State::CENTROID_INDEX:
      if (result.centroid_index_finished_) { state_ = State::QUANT; }
      break;
    case State::QUANT: state_ = State::RAW_WRITE; break;
    case State::RAW_WRITE:
      if (!result.merge_finished_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFInsertionPlanner::advance_state", K(result.merge_finished_));
        ret = OB_STATE_NOT_MATCH;
      }
      else { state_ = State::FINISH; }
      break;
    default:
      LOG_WARN("unexpected IVF planner state", "ret", OB_STATE_NOT_MATCH, K(state_),
          K(result.stage_), K(result.step_), K(result.neighborhood_mode_));
      ret = OB_STATE_NOT_MATCH;
      break;
  }
  return ret;
}

void ObIVFInsertionPlanner::get_operation(ObIVFMergeStage &stage, ObIVFMergeStep &step, int64_t &depth) const
{
  stage = ObIVFMergeStage::INVALID;
  step = ObIVFMergeStep::INVALID;
  depth = depth_;
  switch (state_) {
    case State::PRIOR: stage = ObIVFMergeStage::PRIOR; step = ObIVFMergeStep::PRIOR_WORK; break;
    case State::ROOT_ASSIGN: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_ASSIGN; break;
    case State::UPDATE: stage = ObIVFMergeStage::HIERARCHY; step = ObIVFMergeStep::HIERARCHY_UPDATE; break;
    case State::NEIGHBORHOOD_ADD: stage = ObIVFMergeStage::NEIGHBORHOOD; step = ObIVFMergeStep::NEIGHBORHOOD_ADD; break;
    case State::NEIGHBORHOOD_BUILD: stage = ObIVFMergeStage::NEIGHBORHOOD; step = ObIVFMergeStep::NEIGHBORHOOD_BUILD; break;
    case State::COMPACT: stage = ObIVFMergeStage::REFINE; step = ObIVFMergeStep::REFINE_COMPACT; break;
    case State::SOAR: stage = ObIVFMergeStage::SOAR; step = ObIVFMergeStep::SOAR_ASSIGN; break;
    case State::CENTROID_INDEX: stage = ObIVFMergeStage::CENTROID_INDEX; step = ObIVFMergeStep::INVALID; break;
    case State::QUANT: stage = ObIVFMergeStage::QUANT; step = ObIVFMergeStep::QUANT; break;
    case State::RAW_WRITE: stage = ObIVFMergeStage::RAW_WRITE; step = ObIVFMergeStep::RAW_WRITE; break;
    case State::FINISH: stage = ObIVFMergeStage::FINISH; step = ObIVFMergeStep::INVALID; break;
    case State::FALLBACK: fallback_.get_operation(stage, step, depth); break;
  }
}

} // namespace storage
} // namespace oceanbase
