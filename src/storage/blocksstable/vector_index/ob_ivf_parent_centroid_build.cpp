/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_parent_centroid_build.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
namespace
{
// Leaf ordinals are stable synthetic VIDs. Only requested batches and bounded
// training samples are copied by the existing reader interface; the complete
// leaf vector buffer remains owned by the outer build.
class ObIVFCentroidMemoryInput final : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet
{
public:
  ObIVFCentroidMemoryInput(const float *values, int64_t count, int64_t dimension)
    : values_(values), count_(count), dimension_(dimension) {}
  bool is_valid() const override
  { return values_ != nullptr && count_ > 0 && dimension_ > 0 && count_ <= INT64_MAX / dimension_; }
  bool is_fully_memory_resident() const override { return true; }
  int64_t get_vector_count() const override { return count_; }
  int64_t get_sample_count() const override { return count_; }
  int64_t get_dimension() const override { return dimension_; }
  int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
  {
    vids.reuse();
    int ret = vids.prepare_allocate(count_);
    if (OB_FAIL(ret)) {
      LOG_WARN("[DISK_BBQ] failed to allocate parent clustering input VIDs", K(ret), K_(count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) { vids.at(i) = i; }
    return ret;
  }
  int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin, int64_t end,
      ObIArray<float> &vectors) const override
  {
    int ret = OB_SUCCESS;
    vectors.reuse();
    if (!is_valid() || begin < 0 || end <= begin || end > vids.count()
        || end - begin > INT64_MAX / dimension_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] invalid parent clustering vector batch", K(ret), K(begin), K(end), K_(count), K_(dimension));
    } else if (OB_FAIL(vectors.prepare_allocate((end - begin) * dimension_))) {
      LOG_WARN("[DISK_BBQ] failed to allocate parent clustering vector batch", K(ret),
          K(begin), K(end), K_(dimension), "value_count", (end - begin) * dimension_);
    } else {
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        if (vids.at(i) >= static_cast<uint64_t>(count_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[DISK_BBQ] parent clustering input VID is out of range", K(ret), K(i), "vid", vids.at(i), K_(count));
        }
        else {
          MEMCPY(vectors.get_data() + (i - begin) * dimension_,
              values_ + vids.at(i) * dimension_, dimension_ * sizeof(float));
        }
      }
    }
    return ret;
  }
  int batch_get_samples(int64_t begin, int64_t end, ObIArray<ObIVFVectorVID> &vids,
      ObIArray<float> &vectors) const override
  {
    int ret = OB_SUCCESS;
    vids.reuse();
    vectors.reuse();
    if (!is_valid() || begin < 0 || end <= begin || end > count_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] invalid parent clustering sample range", K(ret), K(begin), K(end), K_(count), K_(dimension));
    } else if (OB_FAIL(vids.prepare_allocate(end - begin))) {
      LOG_WARN("[DISK_BBQ] failed to allocate parent sample VIDs", K(ret), K(begin), K(end));
    } else if (OB_FAIL(vectors.prepare_allocate((end - begin) * dimension_))) {
      LOG_WARN("[DISK_BBQ] failed to allocate parent sample vectors", K(ret), K(begin), K(end), K_(dimension));
    } else {
      for (int64_t i = begin; i < end; ++i) { vids.at(i - begin) = i; }
      MEMCPY(vectors.get_data(), values_ + begin * dimension_,
          (end - begin) * dimension_ * sizeof(float));
    }
    return ret;
  }
private:
  const float *values_;
  int64_t count_;
  int64_t dimension_;
};

// Parent clustering must never encode its input centers as data postings or
// RAW vectors. The outer build owns those later physical output stages.
class ObIVFNoPhysicalOutput final : public ObIVFMergeQuantizer
{
public:
  int quantize_cluster(ObIVFVectorReader &, const ObIArray<float> &, int64_t, int64_t,
      const ObIArray<ObIVFVectorVID> &, const ObIArray<ObIVFVectorVID> *) override
  {
    int ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] unexpected physical output stage during parent centroid clustering", K(ret));
    return ret;
  }
  int finish_quantization(int64_t, int64_t, int64_t) override
  {
    int ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] unexpected physical output stage during parent centroid clustering", K(ret));
    return ret;
  }
  int write_raw_vectors(ObIVFVectorReader &, const ObIArray<ObIVFVectorVID> &,
      int64_t, int64_t, int64_t) override
  {
    int ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] unexpected physical output stage during parent centroid clustering", K(ret));
    return ret;
  }
  int finish_raw_write(int64_t) override
  {
    int ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] unexpected physical output stage during parent centroid clustering", K(ret));
    return ret;
  }
};
} // namespace

struct ObIVFParentCentroidBuilder::State
{
  State(const float *values, int64_t count, int64_t dim)
    : source_(values, count, dim), input_(), graph_allocator_(nullptr), algorithm_(),
      output_(), processor_(), flow_(), stage_(ObIVFMergeStage::INVALID),
      step_(ObIVFMergeStep::INVALID), depth_(-1), plan_id_(-1), task_count_(0), prepared_(false), finished_(false) {}
  int init_graph_allocator()
  {
    int ret = OB_SUCCESS;
    share::ObPluginVectorIndexService *service = MTL(share::ObPluginVectorIndexService *);
    if (graph_allocator_.is_inited()) {
    } else if (service == nullptr || service->get_all_vsag_use_mem() == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[DISK_BBQ] vector index service is unavailable for parent HGraph", K(ret));
    } else if (OB_FAIL(graph_allocator_.init(service->get_memory_context(),
        service->get_all_vsag_use_mem(), MTL_ID(), "IVFParentHGraph"))) {
      LOG_WARN("[DISK_BBQ] failed to initialize parent HGraph allocator", K(ret));
    }
    return ret;
  }
  ObIVFCentroidMemoryInput source_;
  ObIVFMergeInput input_;
  // Use the same concurrent, tenant-accounted allocator as leaf DDL/Merge.
  // Initialize it only for HGraph, and destroy it after algorithm_'s graph.
  share::ObVsagMemContext graph_allocator_;
  ObIVFHcaAlgorithmAdapter algorithm_;
  ObIVFNoPhysicalOutput output_;
  ObIVFMergeProcessor processor_;
  ObIVFFullRebuildState flow_;
  ObIVFMergeStage stage_;
  ObIVFMergeStep step_;
  int64_t depth_;
  int64_t plan_id_;
  int64_t task_count_;
  bool prepared_;
  bool finished_;
};

ObIVFParentCentroidBuilder::ObIVFParentCentroidBuilder(ObIAllocator &allocator)
  : allocator_(allocator), state_(nullptr) {}

ObIVFParentCentroidBuilder::~ObIVFParentCentroidBuilder()
{
  if (state_ != nullptr) { state_->~State(); allocator_.free(state_); }
}

int ObIVFParentCentroidBuilder::init(const ObIArrayWrap<float> &leaves, int64_t dimension,
    const ObIVFHcaAlgorithmParam &param, int64_t cluster_size)
{
  int ret = OB_SUCCESS;
  void *mem = nullptr;
  if (state_ != nullptr) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DISK_BBQ] parent centroid builder is already initialized", K(ret));
  } else if (dimension <= 0 || leaves.empty() || leaves.get_data() == nullptr
      || leaves.count() % dimension != 0 || leaves.count() / dimension > INT32_MAX
      || cluster_size <= 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid parent centroid build input", K(ret), K(dimension), K(cluster_size),
        "leaf_values", leaves.count(), "has_leaf_data", leaves.get_data() != nullptr,
        "cluster_params_valid", param.is_valid());
  } else if ((mem = allocator_.alloc(sizeof(State))) == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[DISK_BBQ] failed to allocate parent centroid builder state", K(ret),
        K(dimension), "leaf_values", leaves.count(), "alloc_size", sizeof(State));
  } else {
    state_ = new (mem) State(leaves.get_data(), leaves.count() / dimension, dimension);
    // The shared clustering kernels use L2 in the existing FHT coordinates.
    // New parent-center normalization is supplied by the algorithm parameters.
    if (OB_FAIL(state_->input_.init(state_->source_, state_->source_, cluster_size,
        param.center_norm_info_ != nullptr, true, true, share::VIDA_L2))) {
      LOG_WARN("[DISK_BBQ] failed to initialize parent centroid memory input", K(ret),
          K(dimension), K(cluster_size), "leaf_values", leaves.count());
    } else if (OB_FAIL(state_->algorithm_.init(param, &state_->graph_allocator_))) {
      LOG_WARN("[DISK_BBQ] failed to initialize parent centroid clustering algorithm", K(ret), K(dimension));
    } else if (OB_FAIL(state_->processor_.init(
        allocator_, state_->input_, state_->algorithm_, state_->output_))) {
      LOG_WARN("[DISK_BBQ] failed to initialize parent centroid build processor", K(ret),
          K(dimension), K(cluster_size));
    }
  }
  return ret;
}

int ObIVFParentCentroidBuilder::get_next_operation(ObIVFMergeStep &step, int64_t &depth) const
{
  int ret = OB_SUCCESS;
  ObIVFMergeStage stage = ObIVFMergeStage::INVALID;
  step = ObIVFMergeStep::INVALID;
  depth = IVF_MERGE_INVALID_DEPTH;
  if (state_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent centroid builder is not initialized for next operation", K(ret));
  } else if (state_->prepared_ || state_->finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] centroid index has no pending operation", K(ret), KP_(state),
        "prepared", state_->prepared_, "finished", state_->finished_);
  } else {
    state_->flow_.get_operation(stage, step, depth);
    if (stage < ObIVFMergeStage::HIERARCHY || stage > ObIVFMergeStage::SOAR) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("[DISK_BBQ] invalid centroid index operation", K(ret), K(stage), K(step), K(depth));
    }
  }
  return ret;
}

ObIVFRefinePhase ObIVFParentCentroidBuilder::get_refine_phase() const
{
  return state_ == nullptr ? ObIVFRefinePhase::DISTANCE : state_->processor_.refine_train_phase();
}

int ObIVFParentCentroidBuilder::prepare(int64_t plan_id, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  if (state_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent centroid builder is not initialized for preparation", K(ret), K(plan_id));
  } else if (plan_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid parent centroid build plan ID", K(ret), K(plan_id));
  } else if (state_->prepared_ || state_->finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] parent centroid builder cannot prepare another plan", K(ret), K(plan_id),
        "active_plan_id", state_->plan_id_, "prepared", state_->prepared_, "finished", state_->finished_);
  } else {
    state_->flow_.get_operation(state_->stage_, state_->step_, state_->depth_);
    if (state_->stage_ < ObIVFMergeStage::HIERARCHY || state_->stage_ > ObIVFMergeStage::SOAR) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("[DISK_BBQ] invalid parent centroid build stage", K(ret), K(plan_id),
          "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
    } else if (state_->step_ == ObIVFMergeStep::NEIGHBORHOOD_ADD
        && OB_FAIL(state_->init_graph_allocator())) {
      LOG_WARN("[DISK_BBQ] failed to prepare parent HGraph memory", K(ret), K(plan_id));
    } else if (OB_FAIL(state_->processor_.prepare_step(
        state_->stage_, state_->step_, state_->depth_, plan_id, task_count))) {
      LOG_WARN("[DISK_BBQ] failed to prepare parent centroid build step", K(ret), K(plan_id), K(task_count),
          "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
    } else if (task_count <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[DISK_BBQ] unexpected parent centroid build task count", K(ret), K(plan_id), K(task_count),
          "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
    } else {
      state_->plan_id_ = plan_id;
      state_->task_count_ = task_count;
      state_->prepared_ = true;
    }
  }
  return ret;
}

int ObIVFParentCentroidBuilder::execute(int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFTaskPlan plan;
  ObIVFTaskResult result;
  if (state_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent centroid builder is not initialized for execution", K(ret), K(task_idx));
  } else if (!state_->prepared_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] parent centroid build plan is not prepared", K(ret), K(task_idx),
        "plan_id", state_->plan_id_);
  } else if (task_idx < 0 || task_idx >= state_->task_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] parent centroid task index is out of range", K(ret), K(task_idx),
        "plan_id", state_->plan_id_, "task_count", state_->task_count_);
  } else if (OB_FAIL(plan.init(ObIVFProcessType::FULL_REBUILD, state_->plan_id_, state_->task_count_))) {
    LOG_WARN("[DISK_BBQ] failed to initialize parent centroid task plan", K(ret), K(task_idx),
        "plan_id", state_->plan_id_, "task_count", state_->task_count_);
  } else if (OB_FAIL(result.init(plan, task_idx))) {
    LOG_WARN("[DISK_BBQ] failed to initialize parent centroid task result", K(ret), K(task_idx),
        "plan_id", state_->plan_id_);
  } else if (OB_FAIL(state_->processor_.execute_task(
      state_->stage_, state_->step_, state_->depth_, task_idx, result))) {
    state_->processor_.cancel(ret);
    LOG_WARN("[DISK_BBQ] failed to execute parent centroid build task", K(ret), K(task_idx),
        "plan_id", state_->plan_id_, "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
  }
  return ret;
}

int ObIVFParentCentroidBuilder::finish(int64_t plan_id,
    blocksstable::ObIvfCentroidLayout &layout, bool &finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  ObIVFMergeStepResult result;
  if (state_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent centroid builder is not initialized for completion", K(ret), K(plan_id));
  } else if (plan_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid parent centroid completion plan ID", K(ret), K(plan_id));
  } else if (!state_->prepared_ || state_->plan_id_ != plan_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] parent centroid completion does not match its plan", K(ret), K(plan_id),
        "active_plan_id", state_->plan_id_, "prepared", state_->prepared_);
  } else if (OB_FAIL(state_->processor_.finish_step(
      state_->stage_, state_->step_, state_->depth_, plan_id, result))) {
    LOG_WARN("[DISK_BBQ] failed to finish parent centroid build step", K(ret), K(plan_id),
        "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
  } else if (state_->step_ == ObIVFMergeStep::SOAR_ASSIGN) {
    if (OB_FAIL(state_->processor_.export_centroid_layout(layout))) {
      LOG_WARN("[DISK_BBQ] failed to export completed parent centroid layout", K(ret), K(plan_id));
    } else {
      state_->finished_ = true;
      finished = true;
    }
  } else if (OB_FAIL(state_->flow_.advance(result))) {
    LOG_WARN("[DISK_BBQ] failed to advance parent centroid build state", K(ret), K(plan_id),
        "stage", state_->stage_, "step", state_->step_, "depth", state_->depth_);
  }
  if (OB_SUCC(ret)) {
    state_->prepared_ = false;
  }
  return ret;
}

int ObIVFParentCentroidBuilder::get_work_perf(int64_t task_idx, ObIVFMergeWorkPerf &perf) const
{
  int ret = OB_SUCCESS;
  if (state_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent centroid builder is not initialized for work statistics", K(ret), K(task_idx));
  } else if (OB_FAIL(state_->processor_.get_work_perf(task_idx, perf))) {
    LOG_WARN("[DISK_BBQ] failed to read parent centroid work statistics", K(ret), K(task_idx),
        "plan_id", state_->plan_id_);
  }
  return ret;
}
ObIVFMergeStage ObIVFParentCentroidBuilder::get_stage() const
{ return state_ == nullptr ? ObIVFMergeStage::INVALID : state_->stage_; }
ObIVFMergeStep ObIVFParentCentroidBuilder::get_step() const
{ return state_ == nullptr ? ObIVFMergeStep::INVALID : state_->step_; }
int64_t ObIVFParentCentroidBuilder::get_depth() const
{ return state_ == nullptr ? -1 : state_->depth_; }

} // namespace storage
} // namespace oceanbase
