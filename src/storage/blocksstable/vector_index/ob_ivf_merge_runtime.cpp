/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_block_reuse_source.h"
#include "storage/blocksstable/vector_index/ob_ivf_parent_centroid_build.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"
#include "storage/ob_parallel_external_sort.h"

#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_2d_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_sort.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

#include <cmath>
#include <utility>

namespace oceanbase
{
using namespace common;
namespace storage
{

namespace
{
static const int64_t IVF_VECTOR_VIDS_PER_TASK = 4096;       // assign 任务粒度
static const int64_t IVF_CENTERS_PER_TASK = 128;            // neighborhood 任务粒度
static const int64_t IVF_RAW_VIDS_PER_TASK = 64 * 1024;      // RAW 写入目标粒度
static const int64_t IVF_MAX_NEIGHBOR_COUNT = 128;          // neighborhood 质心邻居数
static const int64_t IVF_HGRAPH_CENTER_THRESHOLD = 10000;   // neighborhood 质心邻接结构构件阈值

int64_t get_chunk_task_count(
    const int64_t item_count,
    const int64_t items_per_task)
{
  return item_count <= 0 || items_per_task <= 0
      ? 0 : (item_count - 1) / items_per_task + 1;
}

int append_vector_ref(
    ObIVFHierarchyVIDList &vid_list,
    const ObIVFVectorVID vid,
    const int64_t vector_idx)
{
  int ret = OB_SUCCESS;
  if (vid_list.identity_ || vid_list.sealed_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot append to sealed or identity IVF vector list", K(ret),
        K(vid), K(vector_idx), K(vid_list.identity_), K(vid_list.sealed_));
  } else if (vector_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF vector reference", K(ret), K(vid), K(vector_idx));
  } else if (vid_list.vids_.aliases_positions() && vid == static_cast<uint64_t>(vector_idx)) {
    ret = vid_list.vector_idxs_.push_back(vector_idx);
  } else if (OB_FAIL(vid_list.vids_.push_back(vid))) {
    LOG_WARN("failed to append IVF vector-reference VID", K(ret), K(vid),
        K(vector_idx));
  } else if (OB_FAIL(vid_list.vector_idxs_.push_back(vector_idx))) {
    LOG_WARN("failed to append IVF vector-reference index", K(ret), K(vid),
        K(vector_idx));
    vid_list.vids_.pop_back();
  }
  return ret;
}

int reserve_vector_refs(
    ObIVFHierarchyVIDList &vid_list,
    const int64_t vector_count)
{
  int ret = OB_SUCCESS;
  if (vid_list.identity_ || vid_list.sealed_ || vector_count < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(vid_list.vids_.reserve(vector_count))) {
    LOG_WARN("failed to reserve IVF member VIDs", K(ret), K(vector_count));
  } else if (OB_FAIL(vid_list.vector_idxs_.reserve(vector_count))) {
    LOG_WARN("failed to reserve IVF member vector indexes", K(ret),
        K(vector_count));
  }
  return ret;
}

void release_active_plan(
    ObIAllocator &allocator,
    ObIVFMergeActivePlan *&active_plan)
{
  if (OB_NOT_NULL(active_plan)) {
    for (int64_t i = 0; i < active_plan->result_slots_.count(); ++i) {
      ObIVFMergeResultSlot &slot = active_plan->result_slots_.at(i);
      free_runtime_object(allocator, slot.center_values_);
      free_runtime_object(allocator, slot.center_idxs_);
      free_runtime_object(allocator, slot.secondary_center_idxs_);
      free_runtime_object(allocator, slot.neighbor_offsets_);
      free_runtime_object(allocator, slot.neighbor_cids_);
      free_runtime_object(allocator, slot.neighbor_max_distances_);
    }
    if (nullptr != active_plan->source_update_) {
      for (int64_t i = 0; i < active_plan->source_update_->results_.count(); ++i) {
        free_runtime_object(allocator, active_plan->source_update_->results_.at(i).center_values_);
      }
      free_runtime_object(allocator, active_plan->source_update_);
    }
    free_runtime_object(allocator, active_plan->ddl_);
    free_runtime_object(allocator, active_plan);
  }
}

void release_vid_list(
    ObIAllocator &allocator,
    ObIVFHierarchyVIDList *&vid_list)
{
  free_runtime_object(allocator, vid_list);
}

void release_member_lists(
    ObIAllocator &allocator,
    ObIVFHierarchyMemberLists *&member_lists,
    const bool preserve_non_empty_lists)
{
  if (OB_NOT_NULL(member_lists)) {
    for (int64_t i = 0; i < member_lists->lists_.count(); ++i) {
      ObIVFHierarchyVIDList *&vid_list = member_lists->lists_.at(i);
      if (vid_list != nullptr && (!preserve_non_empty_lists || vid_list->vids_.empty())) {
        release_vid_list(allocator, vid_list);
      }
    }
    free_runtime_object(allocator, member_lists);
  }
}

void release_node_state(
    ObIAllocator &allocator,
    ObIVFHierarchyNodeState *&node,
    const bool preserve_non_empty_member_lists)
{
  if (OB_NOT_NULL(node)) {
    release_vid_list(allocator, node->vid_list_);
    release_member_lists(
        allocator, node->member_lists_, preserve_non_empty_member_lists);
    free_runtime_object(allocator, node);
  }
}

void release_pending_next_nodes(
    ObIAllocator &allocator,
    ObIArray<ObIVFHierarchyNodeState *> &pending_next_nodes)
{
  for (int64_t i = 0; i < pending_next_nodes.count(); ++i) {
    ObIVFHierarchyNodeState *&node = pending_next_nodes.at(i);
    if (OB_NOT_NULL(node)) {
      // The child VIDList is still owned by the current node until the whole
      // depth commits. Only discard the uncommitted next-node shell here.
      node->vid_list_ = nullptr;
      release_node_state(allocator, node, false);
    }
  }
}




static ObIVFNeighborhoodMode choose_neighborhood_mode(const int64_t k)
{
  ObIVFNeighborhoodMode mode = ObIVFNeighborhoodMode::INVALID;
  if (k > 0 && k <= IVF_MAX_NEIGHBOR_COUNT) {
    mode = ObIVFNeighborhoodMode::SKIP;
  } else if (k > IVF_MAX_NEIGHBOR_COUNT
      && k < IVF_HGRAPH_CENTER_THRESHOLD) {
    mode = ObIVFNeighborhoodMode::BRUTE_FORCE;
  } else if (k >= IVF_HGRAPH_CENTER_THRESHOLD) {
    mode = ObIVFNeighborhoodMode::HGRAPH;
  }
  return mode;
}

template <typename Array>
static int append_filled_int_array(
    const int64_t count,
    const int64_t value,
    Array &array)
{
  int ret = OB_SUCCESS;
  if (count < 0 || count > INT64_MAX - array.count()) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(array.reserve(array.count() + count))) {
    LOG_WARN("failed to reserve filled IVF integer array", K(ret), K(count),
        "existing_count", array.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ret = array.push_back(value);
  }
  return ret;
}


template <typename Array>
static bool contains_cid(
    const Array &cids,
    const int64_t begin,
    const int64_t end,
    const int64_t cid)
{
  bool found = false;
  for (int64_t i = begin; !found && i < end; ++i) {
    found = cids.at(i) == cid;
  }
  return found;
}

} // namespace

bool is_valid_ivf_merge_operation(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth)
{
  bool is_valid = false;
  switch (stage) {
    case ObIVFMergeStage::PRIOR:
      is_valid = depth == 0 && step == ObIVFMergeStep::PRIOR_WORK;
      break;
    case ObIVFMergeStage::HIERARCHY:
      is_valid = depth >= 0
          && ((ObIVFMergeStep::HIERARCHY_CLUSTER == step || ObIVFMergeStep::HIERARCHY_WARM_START == step)
              || ObIVFMergeStep::HIERARCHY_ASSIGN == step
              || ObIVFMergeStep::HIERARCHY_UPDATE == step
              || (depth > 0 && ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE == step));
      break;
    case ObIVFMergeStage::NEIGHBORHOOD:
      is_valid = IVF_MERGE_INVALID_DEPTH == depth
          && (ObIVFMergeStep::NEIGHBORHOOD_ADD == step
              || ObIVFMergeStep::NEIGHBORHOOD_BUILD == step);
      break;
    case ObIVFMergeStage::REFINE:
      is_valid = IVF_MERGE_INVALID_DEPTH == depth
          && (ObIVFMergeStep::REFINE_TRAIN == step
              || ObIVFMergeStep::REFINE_ASSIGN == step
              || ObIVFMergeStep::REFINE_UPDATE == step
              || ObIVFMergeStep::REFINE_COMPACT == step
              || ObIVFMergeStep::REFINE_SAMPLE_PREPARE == step);
      break;
    case ObIVFMergeStage::SOAR:
      is_valid = IVF_MERGE_INVALID_DEPTH == depth
          && ObIVFMergeStep::SOAR_ASSIGN == step;
      break;
    case ObIVFMergeStage::CENTROID_INDEX:
      is_valid = step != ObIVFMergeStep::HIERARCHY_WARM_START
          && (is_valid_ivf_merge_operation(ObIVFMergeStage::HIERARCHY, step, depth)
              || is_valid_ivf_merge_operation(ObIVFMergeStage::NEIGHBORHOOD, step, depth)
              || is_valid_ivf_merge_operation(ObIVFMergeStage::REFINE, step, depth)
              || is_valid_ivf_merge_operation(ObIVFMergeStage::SOAR, step, depth));
      break;
    case ObIVFMergeStage::QUANT:
      is_valid = IVF_MERGE_INVALID_DEPTH == depth
          && ObIVFMergeStep::QUANT == step;
      break;
    case ObIVFMergeStage::RAW_WRITE:
      is_valid = IVF_MERGE_INVALID_DEPTH == depth
          && ObIVFMergeStep::RAW_WRITE == step;
      break;
    default:
      break;
  }
  return is_valid;
}






static int copy_insertion_center(const share::ObVectorMatrix &centers, const int64_t cid,
    const int64_t dimension, share::ObVectorMatrix &out)
{
  int ret = OB_SUCCESS;
  if (cid < 0 || dimension <= 0 || cid >= centers.count() / dimension) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insertion center to copy", K(ret), K(cid), K(dimension), K(centers.count()));
  } else if (OB_FAIL(out.set_dimension(dimension))) {
    LOG_WARN("failed to set insertion center dimension", K(ret), K(cid), K(dimension));
  }
  for (int64_t d = 0; OB_SUCC(ret) && d < dimension; ++d) {
    ret = out.push_back(centers.at(cid * dimension + d));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to copy insertion center coordinate", K(ret), K(cid), K(d), K(dimension));
    }
  }
  return ret;
}

// Old memberships only need a deterministic working owner while their codes and
// complete primary/secondary relations are copied later. Do not fetch old RAW to
// choose between two inherited owners; only delta vectors need distance search.
static int assign_frozen_insertion(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFVectorReader &reader, const share::ObVectorMatrix &centers, int64_t begin, int64_t end,
    ObIArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIVFVectorVID, 64> delta;
  ObSEArray<int64_t, 64> positions;
  common::ObArray<int64_t> offsets;
  common::ObArray<int64_t> candidates;
  common::ObArray<int64_t> delta_assignments;
  const ObMemAttr attr(MTL_ID(), "IVFInsertRoute", ObCtxIds::VECTOR_CTX_ID);
  delta.set_attr(attr);
  positions.set_attr(attr);
  offsets.set_attr(attr);
  candidates.set_attr(attr);
  delta_assignments.set_attr(attr);
  if (begin < 0 || end <= begin || end > runtime.all_vids_.count()
      || runtime.insertion_offsets_.count() != runtime.all_vids_.count() + 1) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(assignments.prepare_allocate(end - begin))) {
    LOG_WARN("failed to allocate frozen insertion assignments", K(ret));
  }
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    const int64_t first = runtime.insertion_offsets_.at(i);
    const int64_t last = runtime.insertion_offsets_.at(i + 1);
    if (first < 0 || last < first || last > runtime.insertion_candidates_.count()) {
      ret = OB_INVALID_DATA;
    } else if (first < last) {
      assignments.at(i - begin) = runtime.insertion_candidates_.at(first);
    } else if (OB_FAIL(delta.push_back(runtime.all_vids_.at(i)))
        || OB_FAIL(positions.push_back(i - begin)) || OB_FAIL(offsets.push_back(0))) {
      LOG_WARN("failed to retain frozen insertion delta", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && !delta.empty()) {
    if (OB_FAIL(offsets.push_back(0))) {
    } else if (OB_FAIL(algorithm.insertion_assign_vectors(reader, delta, centers, offsets, candidates,
        0, delta.count(), delta_assignments))) {
      LOG_WARN("failed to route frozen insertion delta", K(ret));
    } else if (delta_assignments.count() != delta.count()) {
      ret = OB_INVALID_DATA;
    } else {
      for (int64_t i = 0; i < delta.count(); ++i) {
        assignments.at(positions.at(i)) = delta_assignments.at(i);
      }
    }
  }
  return ret;
}

static bool uses_loaded_insertion(const ObIVFMergeRuntime &runtime, const ObIVFMergeQuantizer &output)
{
  return runtime.insertion_prior_ != nullptr && runtime.ddl_runtime_ != nullptr
      && output.supports_loaded_quantization() && output.supports_loaded_posting_reuse();
}

// A task's already gathered vectors feed the unchanged insertion routing
// kernel. Synthetic VIDs are row offsets in this immutable, bounded batch.
class ObIVFLoadedInsertionBatch final : public ObIVFVectorStore
{
public:
  ObIVFLoadedInsertionBatch(const share::ObVectorMatrix &vectors, int64_t dim)
      : vectors_(vectors), dim_(dim) {}
  bool is_valid() const override
  { return dim_ > 0 && !vectors_.empty() && vectors_.count() % dim_ == 0; }
  int64_t get_dimension() const override { return dim_; }
  int64_t get_vector_count() const override { return dim_ > 0 ? vectors_.count() / dim_ : 0; }
  int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS;
    vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < get_vector_count(); ++i) { ret = vids.push_back(i); }
    return ret;
  }
  int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
      share::ObVectorMatrix &values) const override
  {
    int ret = OB_SUCCESS;
    values.reuse();
    if (!is_valid() || begin < 0 || end <= begin || end > vids.count()
        || end - begin > INT64_MAX / dim_) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(values.set_dimension(dim_))) {
    } else if (OB_FAIL(values.resize((end - begin) * dim_))) {
      LOG_WARN("failed to allocate resident insertion batch", K(ret));
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      if (vids.at(i) >= static_cast<uint64_t>(get_vector_count())) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        MEMCPY(&values.at((i - begin) * dim_), &vectors_.at(vids.at(i) * dim_), dim_ * sizeof(float));
      }
    }
    return ret;
  }
private:
  const share::ObVectorMatrix &vectors_;
  const int64_t dim_;
};

static int64_t reusable_insertion_child(const ObIVFMergeRuntime &runtime, int64_t parent)
{
  const int64_t begin = runtime.insertion_child_offsets_.at(parent);
  const int64_t end = runtime.insertion_child_offsets_.at(parent + 1);
  const int64_t cid = end - begin == 1 ? runtime.insertion_child_cids_.at(begin) : -1;
  return cid >= 0 && runtime.insertion_reuse_cids_.at(cid) >= 0 ? cid : -1;
}

static bool insertion_needs_payload(const ObIVFMergeRuntime &runtime, int64_t idx)
{
  const int64_t begin = runtime.insertion_offsets_.at(idx);
  const int64_t end = runtime.insertion_offsets_.at(idx + 1);
  bool needed = begin == end; // Delta always needs primary encoding, even without SOAR.
  for (int64_t i = begin; !needed && i < end; ++i) {
    needed = reusable_insertion_child(runtime, runtime.insertion_candidates_.at(i)) < 0;
  }
  return needed;
}

static int append_loaded_insertion_tasks(ObIVFMergeRuntime &r, ObIVFMergeActivePlan &plan,
    int64_t &source_tasks)
{
  int ret = OB_SUCCESS;
  source_tasks = 0;
  const int64_t n = r.all_vids_.count(), k = r.final_cluster_count_;
  if (OB_ISNULL(r.ddl_runtime_) || r.dimension_ <= 0 || k <= 0 || n <= 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "append_loaded_insertion_tasks",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  const int64_t batch = OB_SUCC(ret) ? ObIVFDDLRuntime::batch_rows(r.dimension_) : 0;
  r.insertion_source_input_count_ = 0;
  for (int64_t begin = 0; OB_SUCC(ret) && begin < n; begin += batch) {
    ObIVFMergeWorkItem item;
    item.vid_begin_ = begin; item.vid_end_ = MIN(n, begin + batch);
    for (int64_t row = begin; row < item.vid_end_; ++row) {
      item.source_input_count_ += insertion_needs_payload(r, r.ddl_runtime_->vector_index(row)) ? 1 : 0;
    }
    if (is_runtime_canceled(r)) {
      ret = OB_CANCELED;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "append_loaded_insertion_tasks",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("insertion task preparation interrupted", K(ret), K(begin));
    } else if (item.source_input_count_ > 0 && OB_FAIL(plan.work_items_.push_back(item))) {
      LOG_WARN("failed to append global insertion source window", K(ret), K(begin));
    } else { r.insertion_source_input_count_ += item.source_input_count_; }
  }
  source_tasks = plan.work_items_.count();
  for (int64_t begin = 0; OB_SUCC(ret) && begin < k; begin += IVF_CENTERS_PER_TASK) {
    ObIVFMergeWorkItem item;
    item.cid_begin_ = begin; item.cid_end_ = MIN(k, begin + IVF_CENTERS_PER_TASK);
    bool needed = false;
    for (int64_t cid = begin; cid < item.cid_end_; ++cid) { needed |= r.insertion_reuse_cids_.at(cid) >= 0; }
    if (needed && OB_FAIL(plan.work_items_.push_back(item))) {
      LOG_WARN("failed to append insertion encoded copy task", K(ret), K(begin));
    }
  }
  return ret;
}

static int prepare_loaded_insertion_children(ObIVFMergeRuntime &r)
{
  int ret = OB_SUCCESS;
  const auto &prior = *r.insertion_prior_;
  const auto &source = prior.segments_.at(prior.dominant_segment_);
  const int64_t roots = source.centroid_count_, k = r.final_cluster_count_, n = r.all_vids_.count();

  common::ObArray<int64_t> cursors(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFInsertPlan", ObCtxIds::VECTOR_CTX_ID)));
  if (!r.refine_finished_ || r.a0_ == nullptr || r.old_to_new_ == nullptr || r.c1_ == nullptr || roots <= 0 || k <= 0
      || r.dimension_ <= 0 || r.old_to_new_->count() != k || r.c1_->count() != k * r.dimension_
      || r.insertion_origins_.count() != k || r.insertion_offsets_.count() != n + 1 || r.insertion_offsets_.at(0) != 0
      || r.insertion_offsets_.at(n) != r.insertion_candidates_.count()
      || r.insertion_delta_vids_.count() != n - r.insertion_old_count_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_children",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  } else if (OB_FAIL(append_filled_int_array(roots + 1, 0, r.insertion_child_offsets_))
      || OB_FAIL(append_filled_int_array(k, -1, r.insertion_reuse_cids_))
      || OB_FAIL(append_filled_int_array(k, 0, r.insertion_posting_counts_))
      || OB_FAIL(append_filled_int_array(roots + 1, 0, r.insertion_source_offsets_))) {
    LOG_WARN("failed to allocate source insertion lineage", K(ret), K(roots), K(k));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    const int64_t parent = r.insertion_origins_.at(cid);
    if (parent < 0 || parent >= roots || r.old_to_new_->at(cid) != cid) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_children",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else { ++r.insertion_child_offsets_.at(parent + 1); }
  }
  for (int64_t parent = 0; OB_SUCC(ret) && parent < roots; ++parent) {
    if (r.insertion_child_offsets_.at(parent + 1) <= 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_children",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else { r.insertion_child_offsets_.at(parent + 1) += r.insertion_child_offsets_.at(parent); }
  }
  if (OB_SUCC(ret) && (OB_FAIL(r.insertion_child_cids_.prepare_allocate(k))
      || OB_FAIL(cursors.assign(r.insertion_child_offsets_)))) {
    LOG_WARN("failed to allocate source insertion children", K(ret));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    r.insertion_child_cids_.at(cursors.at(r.insertion_origins_.at(cid))++) = cid;
  }
  return ret;
}

static int prepare_loaded_insertion_soar(ObIVFMergeRuntime &r, ObIVFMergeActivePlan &plan,
    ObIVFMergeQuantizer &output)
{
  int ret = OB_SUCCESS;
  const auto &prior = *r.insertion_prior_;
  const auto &source = prior.segments_.at(prior.dominant_segment_);
  const int64_t roots = source.centroid_count_, k = r.final_cluster_count_, n = r.all_vids_.count();

  common::ObArray<int64_t> cursors(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFInsertPlan", ObCtxIds::VECTOR_CTX_ID)));
  if (OB_FAIL(prepare_loaded_insertion_children(r))) {
    LOG_WARN("failed to prepare insertion child lineage", K(ret), K(roots), K(k));
  }
  int64_t reusable_centers = 0, old_postings = 0;
  for (int64_t parent = 0; OB_SUCC(ret) && parent < roots; ++parent) {
    const int64_t first = r.insertion_child_offsets_.at(parent);
    const int64_t source_cid = source.centroid_begin_ + parent;
    const int64_t mass = prior.posting_mass_.at(source_cid);
    if (mass < 0 || mass > INT64_MAX - old_postings) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else { old_postings += mass; }
    if (OB_SUCC(ret) && r.insertion_child_offsets_.at(parent + 1) - first == 1) {
      const int64_t cid = r.insertion_child_cids_.at(first);
      if (MEMCMP(&r.c1_->at(cid * r.dimension_), &prior.centroids_.at(source_cid * r.dimension_),
          r.dimension_ * sizeof(float)) == 0) {
        r.insertion_reuse_cids_.at(cid) = source_cid;
        ++reusable_centers;
      }
    }
    if (OB_SUCC(ret)) {
      // Reverse indexes exist only for encoded-copy streams, never for rows
      // which the source-order tasks will re-encode.
      r.insertion_source_offsets_.at(parent + 1) = r.insertion_source_offsets_.at(parent)
          + (reusable_insertion_child(r, parent) >= 0 ? mass : 0);
    }
  }
  r.insertion_reused_posting_count_ = OB_SUCC(ret) ? r.insertion_source_offsets_.at(roots) : 0;
  if (OB_SUCC(ret) && old_postings != r.insertion_candidates_.count()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  } else if (OB_SUCC(ret) && (OB_FAIL(share::vector_array_resize(r.insertion_source_idxs_, r.insertion_reused_posting_count_))
      || OB_FAIL(cursors.assign(r.insertion_source_offsets_)))) {
    LOG_WARN("failed to allocate reusable insertion source indexes", K(ret));
  }
  int64_t delta_count = 0;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < n; ++idx) {
    if (idx % IVF_VECTOR_VIDS_PER_TASK == 0) {
      ret = is_runtime_canceled(r) ? OB_CANCELED : share::dag_yield();
      if (OB_FAIL(ret)) { LOG_WARN("insertion source preparation interrupted", K(ret), K(idx)); }
    }
    const int64_t begin = r.insertion_offsets_.at(idx), end = r.insertion_offsets_.at(idx + 1);
    if (OB_SUCC(ret) && (begin < 0 || end < begin || end > old_postings)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
    if (OB_SUCC(ret) && begin == end) { ++delta_count; }
    for (int64_t j = begin; OB_SUCC(ret) && j < end; ++j) {
      const int64_t parent = r.insertion_candidates_.at(j);
      if (parent < 0 || parent >= roots) {
        ret = OB_INVALID_DATA;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      } else if (reusable_insertion_child(r, parent) >= 0) {
        if (cursors.at(parent) >= r.insertion_source_offsets_.at(parent + 1)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
              "dimension", r.dimension_, "centers", r.final_cluster_count_);
        } else { r.insertion_source_idxs_.at(cursors.at(parent)++) = idx; }
      }
    }
  }
  for (int64_t parent = 0; OB_SUCC(ret) && parent < roots; ++parent) {
    if (cursors.at(parent) != r.insertion_source_offsets_.at(parent + 1)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
  }
  if (OB_SUCC(ret) && delta_count != n - r.insertion_old_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "prepare_loaded_insertion_soar",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  if (OB_SUCC(ret) && OB_FAIL(r.ddl_runtime_->prepare_source_routes(r))) {
    LOG_WARN("failed to prepare global insertion source routes", K(ret));
  }
  int64_t source_tasks = 0;
  if (OB_SUCC(ret) && OB_FAIL(append_loaded_insertion_tasks(r, plan, source_tasks))) {
    LOG_WARN("failed to prepare source insertion tasks", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(output.prepare_loaded_output(k, r.ddl_runtime_->parallelism()))) {
    LOG_WARN("failed to prepare source insertion output", K(ret), K(k));
  }
  if (OB_SUCC(ret)) {
    // Adjacency remains alive through SOAR; delta membership is derivable from
    // empty adjacency, so its separate O(delta) lists are no longer needed.
    r.insertion_delta_idxs_.reset(); r.insertion_delta_vids_.reset();
    LOG_INFO("IVF source insertion posting plan", "old_vectors", r.insertion_old_count_,
        K(delta_count), K(old_postings), K(roots), K(k), K(reusable_centers),
        "reused_postings", r.insertion_reused_posting_count_,
        "source_vectors", r.insertion_source_input_count_, K(source_tasks),
        "copy_tasks", plan.work_items_.count() - source_tasks);
  }
  return ret;
}

static int prepare_incremental_soar(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
    ObIVFMergeQuantizer &output)
{
  if (uses_loaded_insertion(runtime, output)) { return prepare_loaded_insertion_soar(runtime, plan, output); }
  int ret = OB_SUCCESS;
  const auto &prior = *runtime.insertion_prior_;
  const auto &source = prior.segments_.at(prior.dominant_segment_);
  const int64_t roots = source.centroid_count_;
  const int64_t k = runtime.final_cluster_count_;
  const int64_t n = runtime.all_vids_.count();
  ObSEArray<int64_t, 64> cursors;
  cursors.set_attr(ObMemAttr(MTL_ID(), "IVFInsertPlan", ObCtxIds::VECTOR_CTX_ID));
  if (!runtime.refine_finished_ || runtime.a0_ == nullptr || runtime.old_to_new_ == nullptr
      || runtime.c1_ == nullptr || runtime.insertion_origins_.count() != k
      || runtime.insertion_offsets_.count() != n + 1
      || runtime.insertion_delta_vids_.count() != n - runtime.insertion_old_count_
      || runtime.insertion_delta_idxs_.count() != runtime.insertion_delta_vids_.count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid incremental SOAR preparation state", K(ret), K(roots), K(k), K(n),
        K(runtime.refine_finished_));
  } else if (OB_FAIL(append_filled_int_array(roots + 1, 0, runtime.insertion_source_offsets_))) {
    LOG_WARN("failed to allocate incremental source offsets", K(ret), K(roots));
  } else if (OB_FAIL(append_filled_int_array(roots + 1, 0, runtime.insertion_child_offsets_))) {
    LOG_WARN("failed to allocate incremental child offsets", K(ret), K(roots));
  } else if (OB_FAIL(append_filled_int_array(k, -1, runtime.insertion_reuse_cids_))) {
    LOG_WARN("failed to allocate incremental reuse centers", K(ret), K(k));
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) {
    const int64_t mass = prior.posting_mass_.at(source.centroid_begin_ + c);
    const int64_t prefix = runtime.insertion_source_offsets_.at(c);
    if (mass < 0 || prefix > INT64_MAX - mass) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid incremental posting mass or prefix overflow", K(ret), K(c), K(mass), K(prefix));
    }
    else { runtime.insertion_source_offsets_.at(c + 1) = prefix + mass; }
  }
  const int64_t posting_count = OB_SUCC(ret) ? runtime.insertion_source_offsets_.at(roots) : 0;
  if (OB_SUCC(ret) && (posting_count != runtime.insertion_candidates_.count()
      || posting_count > INT64_MAX / static_cast<int64_t>(sizeof(int64_t)))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("incremental posting count mismatch or overflow", K(ret), K(posting_count),
        K(runtime.insertion_candidates_.count()));
  }
  if (OB_SUCC(ret) && OB_FAIL(share::vector_array_resize(runtime.insertion_source_idxs_, posting_count))) {
    LOG_WARN("failed to allocate incremental source indexes", K(ret), K(posting_count));
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) {
    ret = cursors.push_back(runtime.insertion_source_offsets_.at(c));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to retain incremental posting cursor", K(ret), K(c), K(roots));
    }
  }
  // Transpose the already validated VID -> old posting references. Only dense
  // vector indexes are retained; neither float vectors nor old codes are copied.
  for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
    for (int64_t j = runtime.insertion_offsets_.at(i);
         OB_SUCC(ret) && j < runtime.insertion_offsets_.at(i + 1); ++j) {
      const int64_t c = runtime.insertion_candidates_.at(j);
      if (c < 0 || c >= roots || cursors.at(c) >= runtime.insertion_source_offsets_.at(c + 1)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid incremental source posting position", K(ret), K(i), K(j), K(c), K(roots));
      } else { runtime.insertion_source_idxs_.at(cursors.at(c)++) = i; }
    }
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) {
    if (cursors.at(c) != runtime.insertion_source_offsets_.at(c + 1)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("incremental source cursor mismatch", K(ret), K(c), K(cursors.at(c)),
          K(runtime.insertion_source_offsets_.at(c + 1)));
    }
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    const int64_t c = runtime.insertion_origins_.at(cid);
    if (c < 0 || c >= roots || runtime.old_to_new_->at(cid) != cid) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid insertion center lineage", K(ret), K(cid), K(c), K(roots));
    } else {
      ++runtime.insertion_child_offsets_.at(c + 1);
    }
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) {
    if (runtime.insertion_child_offsets_.at(c + 1) <= 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("insertion source center has no children", K(ret), K(c));
    } else {
      runtime.insertion_child_offsets_.at(c + 1) += runtime.insertion_child_offsets_.at(c);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(runtime.insertion_child_cids_.prepare_allocate(k))) {
    LOG_WARN("failed to allocate incremental child centers", K(ret), K(k));
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) { cursors.at(c) = runtime.insertion_child_offsets_.at(c); }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    runtime.insertion_child_cids_.at(cursors.at(runtime.insertion_origins_.at(cid))++) = cid;
  }
  int64_t reusable_centers = 0;
  for (int64_t c = 0; OB_SUCC(ret) && c < roots; ++c) {
    const int64_t begin = runtime.insertion_child_offsets_.at(c);
    if (runtime.insertion_child_offsets_.at(c + 1) - begin == 1) {
      const int64_t cid = runtime.insertion_child_cids_.at(begin);
      const int64_t source_cid = source.centroid_begin_ + c;
      if (0 == MEMCMP(&runtime.c1_->at(cid * runtime.dimension_),
          &prior.centroids_.at(source_cid * runtime.dimension_), runtime.dimension_ * sizeof(float))) {
        runtime.insertion_reuse_cids_.at(cid) = source_cid;
        ++reusable_centers;
      }
    }
  }
  const int64_t delta_count = runtime.insertion_delta_vids_.count();
  const int64_t batch_rows = IVF_VECTOR_VIDS_PER_TASK;
  const int64_t tasks = get_chunk_task_count(posting_count, batch_rows)
      + get_chunk_task_count(delta_count, batch_rows);
  if (OB_SUCC(ret) && OB_FAIL(plan.work_items_.reserve(tasks))) {
    LOG_WARN("failed to reserve incremental posting tasks", K(ret), K(tasks));
  }
  // These items only inherit old relations or route within their source parent's
  // descendants. cid_begin_ >= 0 distinguishes them from actual delta SOAR.
  for (int64_t i = 0; OB_SUCC(ret) && i < posting_count; i += batch_rows) {
    ObIVFMergeWorkItem item;
    item.vid_begin_ = i; item.vid_end_ = MIN(posting_count, i + batch_rows);
    item.cid_begin_ = 0;
    ret = plan.work_items_.push_back(item);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append incremental posting task", K(ret), K(i), K(item));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < delta_count; i += batch_rows) {
    ObIVFMergeWorkItem item;
    item.vid_begin_ = i; item.vid_end_ = MIN(delta_count, i + batch_rows);
    ret = plan.work_items_.push_back(item);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append incremental posting task", K(ret), K(i), K(item));
    }
  }
  if (OB_SUCC(ret)) {
    runtime.insertion_offsets_.reset();
    runtime.insertion_candidates_.reset();
    LOG_INFO("IVF incremental posting plan", "old_vectors", runtime.insertion_old_count_,
        "new_vectors", delta_count, K(posting_count), K(roots), K(k), K(reusable_centers), K(tasks));
  }
  return ret;
}

static int execute_inherited_postings(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFVectorReader &reader, const ObIVFMergeWorkItem &item,
    ObIVFMergeResultSlot &slot, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t, ObWrapperAllocator> *targets = nullptr;
  const int64_t count = item.vid_end_ - item.vid_begin_;
  const int64_t roots = runtime.insertion_child_offsets_.count() - 1;
  if (item.vid_begin_ < 0 || count <= 0 || item.vid_end_ > runtime.insertion_source_idxs_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inherited posting range", K(ret), K(item.vid_begin_), K(item.vid_end_), K(count));
  } else if (OB_FAIL(alloc_runtime_array(allocator, targets))) {
    LOG_WARN("failed to allocate inherited posting targets", K(ret), K(count));
  } else if (OB_FAIL(append_filled_int_array(count, -1, *targets))) {
    LOG_WARN("failed to initialize inherited posting targets", K(ret), K(count));
  }
  ObArenaAllocator scratch(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID));
  share::ObVectorMatrix centers(share::vector_array_allocator(scratch));

  ObSEArray<ObIVFVectorVID, 64> vids;
  vids.set_attr(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID));
  ObSEArray<int64_t, 64> positions;
  positions.set_attr(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID));

  common::ObArray<int64_t> offsets(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID)));

  common::ObArray<int64_t> candidates(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID)));

  common::ObArray<int64_t> local_targets(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFOldRoute", ObCtxIds::VECTOR_CTX_ID)));
  int64_t left = 0, right = roots;
  while (OB_SUCC(ret) && left < right) {
    const int64_t mid = left + (right - left) / 2;
    if (runtime.insertion_source_offsets_.at(mid + 1) <= item.vid_begin_) { left = mid + 1; }
    else { right = mid; }
  }
  int64_t origin = left;
  for (int64_t pos = item.vid_begin_; OB_SUCC(ret) && pos < item.vid_end_; ++origin) {
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
      break;
    } else if (origin >= roots) {
      ret = OB_INVALID_DATA;
      LOG_WARN("inherited posting origin out of range", K(ret), K(origin), K(roots), K(pos));
      break;
    }
    const int64_t end = MIN(item.vid_end_, runtime.insertion_source_offsets_.at(origin + 1));
    const int64_t child_begin = runtime.insertion_child_offsets_.at(origin);
    const int64_t child_count = runtime.insertion_child_offsets_.at(origin + 1) - child_begin;
    if (end <= pos || child_count <= 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid inherited posting child range", K(ret), K(origin), K(pos), K(end), K(child_count));
      break;
    }
    vids.reuse(); positions.reuse(); offsets.reuse(); local_targets.reuse(); centers.reuse();
    for (; OB_SUCC(ret) && pos < end; ++pos) {
      const int64_t idx = runtime.insertion_source_idxs_.at(pos);
      if (idx < 0 || idx >= runtime.all_vids_.count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("inherited posting vector index out of range", K(ret), K(pos), K(idx),
            K(runtime.all_vids_.count()));
        break;
      }
      const int64_t owner = runtime.a0_->at(idx);
      if (owner < 0 || owner >= runtime.final_cluster_count_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("inherited posting owner out of range", K(ret), K(idx), K(owner),
            K(runtime.final_cluster_count_));
        break;
      }
      if (child_count == 1) {
        targets->at(pos - item.vid_begin_) = runtime.insertion_child_cids_.at(child_begin);
      } else if (runtime.insertion_origins_.at(owner) == origin) {
        // Reuse the existing hierarchy route for this working-owner relation.
        targets->at(pos - item.vid_begin_) = owner;
      } else if (OB_FAIL(vids.push_back(runtime.all_vids_.at(idx)))) {
        LOG_WARN("failed to retain inherited posting VID", K(ret), K(idx), K(pos));
      } else if (OB_FAIL(positions.push_back(pos - item.vid_begin_))) {
        LOG_WARN("failed to retain inherited posting position", K(ret), K(pos), K(item.vid_begin_));
      }
    }
    if (OB_SUCC(ret) && !vids.empty()) {
      for (int64_t c = 0; OB_SUCC(ret) && c < child_count; ++c) {
        ret = copy_insertion_center(*runtime.c1_, runtime.insertion_child_cids_.at(child_begin + c),
            runtime.dimension_, centers);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to copy inherited posting child center", K(ret), K(origin), K(c), K(child_begin));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(append_filled_int_array(vids.count() + 1, 0, offsets))) {
        LOG_WARN("failed to initialize inherited posting candidate offsets", K(ret), K(origin),
            K(vids.count()));
      } else if (OB_SUCC(ret) && OB_FAIL(algorithm.insertion_assign_vectors(reader, vids,
          centers, offsets, candidates, 0, vids.count(), local_targets))) {
        LOG_WARN("failed to assign inherited postings to child centers", K(ret), K(origin), K(child_count),
            K(vids.count()));
      } else if (OB_SUCC(ret) && local_targets.count() != vids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inherited posting assignment count mismatch", K(ret), K(local_targets.count()),
            K(vids.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < local_targets.count(); ++i) {
        const int64_t c = local_targets.at(i);
        if (c < 0 || c >= child_count) {
          ret = OB_INVALID_DATA;
          LOG_WARN("inherited posting target out of range", K(ret), K(i), K(c), K(child_count));
        }
        else { targets->at(positions.at(i)) = runtime.insertion_child_cids_.at(child_begin + c); }
      }
      if (OB_SUCC(ret)) { (void)ATOMIC_AAF(&runtime.insertion_routed_vectors_, vids.count()); }
    }
    if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
      LOG_WARN("failed to yield IVF merge task", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_runtime_canceled(runtime)) {
    ret = OB_CANCELED;
    LOG_WARN("inherited posting task canceled", K(ret), K(item.vid_begin_), K(item.vid_end_));
  }
  if (OB_SUCC(ret) && is_runtime_canceled(runtime)) { ret = OB_CANCELED; }
  if (OB_SUCC(ret)) {
    slot.center_idxs_ = targets;
    slot.installed_ = true;
    targets = nullptr;
  }
  free_runtime_object(allocator, targets);
  return ret;
}

struct ObIVFInsertionRelation
{
  int64_t parent_, row_, cid_;
  TO_STRING_KV(K_(parent), K_(row), K_(cid));
  bool operator<(const ObIVFInsertionRelation &other) const
  { return parent_ == other.parent_ ? row_ < other.row_ : parent_ < other.parent_; }
};

static int route_loaded_insertion_relations(ObIVFMergeRuntime &r, ObIVFMergeAlgorithm &algorithm,
    const share::ObVectorMatrix &vectors, ObArray<ObIVFInsertionRelation> &relations, int64_t &routed)
{
  int ret = OB_SUCCESS;
  routed = 0;
  const int64_t dim = r.dimension_;
  const ObMemAttr attr(MTL_ID(), "IVFInsertSource", ObCtxIds::VECTOR_CTX_ID);

  common::ObArray<int64_t> offsets(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr));
  common::ObArray<int64_t> candidates(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr));
  common::ObArray<int64_t> local_targets(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr));
  ObArray<int64_t> positions;
  ObArray<ObIVFVectorVID> routing_rows;
  share::ObVectorMatrix centers(attr);
  positions.set_attr(attr); routing_rows.set_attr(attr);
  if (dim <= 0 || vectors.count() % dim != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "route_loaded_insertion_relations",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  ObIVFLoadedInsertionBatch resident(vectors, dim);
  ObIVFVectorReader reader;
  if (OB_SUCC(ret) && OB_FAIL(reader.init(resident))) {
    LOG_WARN("failed to open insertion resident batch", K(ret));
  }
  if (OB_SUCC(ret)) { lib::ob_sort(relations.begin(), relations.end()); }
  int64_t route_groups = 0;
  for (int64_t begin = 0; OB_SUCC(ret) && begin < relations.count();) {
    const int64_t parent = relations.at(begin).parent_;
    int64_t end = begin;
    routing_rows.reuse(); positions.reuse(); centers.reuse(); offsets.reuse(); local_targets.reuse();
    while (OB_SUCC(ret) && end < relations.count() && relations.at(end).parent_ == parent) {
      if (relations.at(end).cid_ < 0 && (OB_FAIL(routing_rows.push_back(relations.at(end).row_))
          || OB_FAIL(positions.push_back(end)))) {
        LOG_WARN("failed to retain insertion child route", K(ret), K(parent));
      }
      ++end;
    }
    const int64_t child_begin = r.insertion_child_offsets_.at(parent);
    const int64_t child_count = r.insertion_child_offsets_.at(parent + 1) - child_begin;
    if (OB_SUCC(ret) && !routing_rows.empty()) {
      for (int64_t c = 0; OB_SUCC(ret) && c < child_count; ++c) {
        if (OB_FAIL(copy_insertion_center(*r.c1_, r.insertion_child_cids_.at(child_begin + c), dim, centers))) {
          LOG_WARN("failed to copy insertion routing center", K(ret), K(parent), K(c));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(append_filled_int_array(routing_rows.count() + 1, 0, offsets))) {
        LOG_WARN("failed to allocate insertion routing offsets", K(ret), K(parent));
      } else if (OB_SUCC(ret) && OB_FAIL(algorithm.insertion_assign_vectors(reader, routing_rows,
          centers, offsets, candidates, 0, routing_rows.count(), local_targets))) {
        LOG_WARN("failed to route source-ordered inherited postings", K(ret), K(parent));
      } else if (OB_SUCC(ret) && local_targets.count() != positions.count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "route_loaded_insertion_relations",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < local_targets.count(); ++i) {
        const int64_t target = local_targets.at(i);
        if (target < 0 || target >= child_count) {
          ret = OB_INVALID_DATA;
          LOG_WARN("source insertion validation failed", K(ret), "operation", "route_loaded_insertion_relations",
              "dimension", r.dimension_, "centers", r.final_cluster_count_);
        } else { relations.at(positions.at(i)).cid_ = r.insertion_child_cids_.at(child_begin + target); }
      }
      routed += routing_rows.count();
    }
    if (OB_SUCC(ret) && is_runtime_canceled(r)) {
      ret = OB_CANCELED;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "route_loaded_insertion_relations",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
    if (OB_SUCC(ret) && ++route_groups % 64 == 0 && OB_FAIL(share::dag_yield())) {
      LOG_WARN("insertion relation routing interrupted", K(ret), K(route_groups));
    }
    begin = end;
  }
  return ret;
}

static int assign_loaded_insertion_secondary(ObIVFMergeRuntime &r,
    const share::ObVectorMatrix &vectors, const common::ObIArray<int64_t> &primary_cids,
    ObIArray<int64_t> &secondary, int64_t &secondary_count)
{
  int ret = OB_SUCCESS;
  secondary_count = 0;
  const int64_t dim = r.dimension_, k = r.final_cluster_count_;
  if (OB_ISNULL(r.ddl_runtime_) || OB_ISNULL(r.c1_) || dim <= 0 || k <= 0
      || primary_cids.empty() || vectors.count() / dim != primary_cids.count()
      || vectors.count() % dim != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "assign_loaded_insertion_secondary",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  ObIVFNeighborGraphView graph;
  if (OB_SUCC(ret) && r.enable_soar_ && k > 1) {
    if (r.g1_offsets_ != nullptr) {
      if (r.g1_cids_ == nullptr || r.g1_max_distances_ == nullptr) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "assign_loaded_insertion_secondary",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      } else { graph = ObIVFNeighborGraphView(*r.g1_offsets_, *r.g1_cids_, *r.g1_max_distances_, k); }
      if (!graph.is_valid()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "assign_loaded_insertion_secondary",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(r.ddl_runtime_->assign_loaded_secondary(vectors, dim,
        *r.c1_, primary_cids, graph.is_valid() ? &graph : nullptr, secondary))) {
      LOG_WARN("failed to assign source insertion delta SOAR", K(ret));
    } else if (OB_SUCC(ret) && secondary.count() != primary_cids.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "assign_loaded_insertion_secondary",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < secondary.count(); ++i) {
      const int64_t cid = secondary.at(i), primary = primary_cids.at(i);
      if (cid < -1 || cid >= k || cid == primary || (cid >= 0 && graph.is_valid()
          && !contains_cid(*graph.cids_, graph.offsets_->at(primary), graph.offsets_->at(primary + 1), cid))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "assign_loaded_insertion_secondary",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      } else { secondary_count += cid >= 0 ? 1 : 0; }
    }
  }
  return ret;
}

static int gather_loaded_insertion_relations(ObIVFMergeRuntime &r, const ObIVFMergeWorkItem &item,
    share::ObVectorArray<int64_t> &indices, share::ObVectorMatrix &vectors, ObIArray<int64_t> &delta_rows,
    ObIArray<ObIVFInsertionRelation> &relations)
{
  int ret = OB_SUCCESS;
  const int64_t dim = r.dimension_, k = r.final_cluster_count_;
  const int64_t roots = r.insertion_child_offsets_.count() - 1;
  const int64_t batch_rows = ObIVFDDLRuntime::batch_rows(dim);
  if (item.vid_begin_ < 0 || item.vid_end_ <= item.vid_begin_ || item.vid_end_ > r.all_vids_.count()
      || item.vid_end_ - item.vid_begin_ > batch_rows || item.source_input_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  for (int64_t source = item.vid_begin_; OB_SUCC(ret) && source < item.vid_end_; ++source) {
    const int64_t idx = r.ddl_runtime_->vector_index(source);
    if (idx < 0 || idx >= r.all_vids_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else if (insertion_needs_payload(r, idx) && OB_FAIL(indices.push_back(idx))) {
      LOG_WARN("failed to retain insertion source vector", K(ret), K(source));
    }
  }
  if (OB_SUCC(ret) && indices.count() != item.source_input_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  if (OB_SUCC(ret) && OB_FAIL(r.vector_store_->batch_get_by_source(indices, vectors))) {
    LOG_WARN("failed to read global insertion source window", K(ret), K(item));
  } else if (OB_SUCC(ret) && (vectors.count() != indices.count() * dim || !are_finite_center_values(vectors))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  for (int64_t row = 0; OB_SUCC(ret) && row < indices.count(); ++row) {
    const int64_t idx = indices.at(row), owner = r.a0_->at(idx);
    const int64_t begin = r.insertion_offsets_.at(idx), end = r.insertion_offsets_.at(idx + 1);
    if (owner < 0 || owner >= k || r.old_to_new_->at(owner) != owner) {
      ret = OB_INVALID_DATA;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    } else if (begin == end && OB_FAIL(delta_rows.push_back(row))) {
      LOG_WARN("failed to retain insertion delta row", K(ret), K(row));
    }
    for (int64_t j = begin; OB_SUCC(ret) && j < end; ++j) {
      const int64_t parent = r.insertion_candidates_.at(j);
      if (parent < 0 || parent >= roots) {
        ret = OB_INVALID_DATA;
        LOG_WARN("source insertion validation failed", K(ret), "operation", "gather_loaded_insertion_relations",
            "dimension", r.dimension_, "centers", r.final_cluster_count_);
      } else if (reusable_insertion_child(r, parent) < 0) {
        const int64_t first = r.insertion_child_offsets_.at(parent);
        const int64_t child_count = r.insertion_child_offsets_.at(parent + 1) - first;
        ObIVFInsertionRelation relation = {parent, row,
            child_count == 1 ? r.insertion_child_cids_.at(first)
                : r.insertion_origins_.at(owner) == parent ? owner : -1};
        if (OB_FAIL(relations.push_back(relation))) {
          LOG_WARN("failed to retain insertion relation", K(ret), K(relation));
        }
      }
    }
  }
  return ret;
}

static int execute_loaded_insertion_source(ObIVFMergeRuntime &r, ObIVFMergeAlgorithm &algorithm,
    const ObIVFMergeWorkItem &item, ObIVFMergeResultSlot &slot, ObIVFMergeQuantizer &output)
{
  int ret = OB_SUCCESS;
  const int64_t dim = r.dimension_, k = r.final_cluster_count_;
  const int64_t batch_rows = ObIVFDDLRuntime::batch_rows(dim);
  const ObMemAttr attr(MTL_ID(), "IVFInsertSource", ObCtxIds::VECTOR_CTX_ID);

  share::ObVectorArray<int64_t> indices(attr);
  ObArray<int64_t> delta_rows;
  ObArray<ObIVFVectorVID> encoded_vids;
  share::ObVectorMatrix vectors(attr);
  share::ObVectorMatrix encoded_vectors(attr);
  common::ObArray<int64_t> encoded_cids(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr));
  common::ObArray<int64_t> secondary(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr));
  ObArray<ObIVFInsertionRelation> relations;
  delta_rows.set_attr(attr); encoded_vids.set_attr(attr); relations.set_attr(attr);
  if (OB_FAIL(gather_loaded_insertion_relations(r, item, indices, vectors, delta_rows, relations))) {
    LOG_WARN("failed to gather source insertion relations", K(ret), K(item));
  }
  int64_t routed = 0;
  if (OB_SUCC(ret) && OB_FAIL(route_loaded_insertion_relations(r, algorithm, vectors, relations, routed))) {
    LOG_WARN("failed to route source insertion relations", K(ret));
  }
  // Expand only compact relation metadata globally within this task. Payload
  // expansion is bounded to the same 4096 / 16 MiB limit as the source read.
  for (int64_t begin = 0; OB_SUCC(ret) && begin < relations.count(); begin += batch_rows) {
    const int64_t end = MIN(relations.count(), begin + batch_rows);
    encoded_vids.reuse(); encoded_cids.reuse(); encoded_vectors.reuse();
    if (OB_FAIL(encoded_vectors.set_dimension(dim))) {
    } else if (OB_FAIL(encoded_vectors.resize((end - begin) * dim))) {
      LOG_WARN("failed to allocate inherited encoding batch", K(ret));
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const auto &relation = relations.at(i);
      if (relation.cid_ < 0 || relation.cid_ >= k || r.insertion_origins_.at(relation.cid_) != relation.parent_
          || r.insertion_reuse_cids_.at(relation.cid_) >= 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid source insertion encoding relation", K(ret), K(relation), K(k));
      } else if (OB_FAIL(encoded_vids.push_back(r.all_vids_.at(indices.at(relation.row_))))
          || OB_FAIL(encoded_cids.push_back(relation.cid_))) {
        LOG_WARN("failed to retain inherited encoded relation", K(ret), K(i));
      } else { MEMCPY(&encoded_vectors.at((i - begin) * dim), &vectors.at(relation.row_ * dim), dim * sizeof(float)); }
    }
    if (OB_SUCC(ret) && is_runtime_canceled(r)) {
      ret = OB_CANCELED;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "execute_loaded_insertion_source",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
    if (OB_SUCC(ret) && OB_FAIL(r.ddl_runtime_->posting_error())) {
      LOG_WARN("insertion posting output already failed", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(output.quantize_loaded_vectors(*r.c1_, dim,
        encoded_vids, encoded_vectors, encoded_cids, nullptr))) {
      LOG_WARN("failed to encode source-ordered inherited relations", K(ret));
    }
  }
  int64_t secondary_count = 0;
  if (OB_SUCC(ret) && !delta_rows.empty()) {
    encoded_vids.reuse(); encoded_cids.reuse(); encoded_vectors.reuse();
    if (OB_FAIL(encoded_vectors.set_dimension(dim))) {
    } else if (OB_FAIL(encoded_vectors.resize(delta_rows.count() * dim))) {
      LOG_WARN("failed to allocate insertion delta encoding batch", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < delta_rows.count(); ++i) {
      const int64_t row = delta_rows.at(i), idx = indices.at(row);
      if (OB_FAIL(encoded_vids.push_back(r.all_vids_.at(idx))) || OB_FAIL(encoded_cids.push_back(r.a0_->at(idx)))) {
        LOG_WARN("failed to retain source insertion delta", K(ret), K(i));
      } else { MEMCPY(&encoded_vectors.at(i * dim), &vectors.at(row * dim), dim * sizeof(float)); }
    }
    if (OB_SUCC(ret) && OB_FAIL(assign_loaded_insertion_secondary(
        r, encoded_vectors, encoded_cids, secondary, secondary_count))) {
      LOG_WARN("failed to prepare insertion delta secondary assignments", K(ret));
    }
    if (OB_SUCC(ret) && is_runtime_canceled(r)) {
      ret = OB_CANCELED;
      LOG_WARN("source insertion validation failed", K(ret), "operation", "execute_loaded_insertion_source",
          "dimension", r.dimension_, "centers", r.final_cluster_count_);
    }
    if (OB_SUCC(ret) && OB_FAIL(r.ddl_runtime_->posting_error())) {
      LOG_WARN("insertion posting output already failed", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(output.quantize_loaded_vectors(*r.c1_, dim, encoded_vids,
        encoded_vectors, encoded_cids, secondary.empty() ? nullptr : &secondary))) {
      LOG_WARN("failed to encode source insertion delta", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_runtime_canceled(r)) {
    ret = OB_CANCELED;
    LOG_WARN("source insertion validation failed", K(ret), "operation", "execute_loaded_insertion_source",
        "dimension", r.dimension_, "centers", r.final_cluster_count_);
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < relations.count(); ++i) { ATOMIC_INC(&r.insertion_posting_counts_.at(relations.at(i).cid_)); }
    for (int64_t i = 0; i < delta_rows.count(); ++i) {
      ATOMIC_INC(&r.insertion_posting_counts_.at(r.a0_->at(indices.at(delta_rows.at(i)))));
    }
    for (int64_t i = 0; i < secondary.count(); ++i) {
      if (secondary.at(i) >= 0) { ATOMIC_INC(&r.insertion_posting_counts_.at(secondary.at(i))); }
    }
    ATOMIC_AAF(&r.insertion_routed_vectors_, routed);
    slot.primary_posting_count_ = relations.count() + delta_rows.count();
    slot.secondary_posting_count_ = secondary_count;
    slot.installed_ = true;
  }
  return ret;
}

static int execute_loaded_insertion_copy(ObIVFMergeRuntime &r, const ObIVFMergeWorkItem &item,
    ObIVFMergeResultSlot &slot, ObIVFMergeQuantizer &output)
{
  int ret = OB_SUCCESS;

  share::ObVectorArray<ObIVFVectorVID> expected(ObMemAttr(MTL_ID(), "IVFReuseCodes", ObCtxIds::VECTOR_CTX_ID));
  int64_t copied = 0;
  if (item.cid_begin_ < 0 || item.cid_end_ <= item.cid_begin_ || item.cid_end_ > r.final_cluster_count_
      || item.vid_begin_ >= 0 || item.source_input_count_ != 0) { ret = OB_INVALID_ARGUMENT; }
  for (int64_t cid = item.cid_begin_; OB_SUCC(ret) && cid < item.cid_end_; ++cid) {
    const int64_t source_cid = r.insertion_reuse_cids_.at(cid);
    if (source_cid < 0) { continue; }
    expected.reuse();
    const int64_t parent = r.insertion_origins_.at(cid);
    const int64_t begin = r.insertion_source_offsets_.at(parent), end = r.insertion_source_offsets_.at(parent + 1);
    if (OB_FAIL(expected.reserve(end - begin))) {
      LOG_WARN("failed to reserve reusable source VIDs", K(ret), K(cid));
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      if (i % IVF_VECTOR_VIDS_PER_TASK == 0) { ret = is_runtime_canceled(r) ? OB_CANCELED : share::dag_yield(); }
      if (OB_SUCC(ret)) { ret = expected.push_back(r.all_vids_.at(r.insertion_source_idxs_.at(i))); }
    }
    if (OB_SUCC(ret) && expected.count() != r.insertion_prior_->posting_mass_.at(source_cid)) { ret = OB_INVALID_DATA; }
    if (OB_SUCC(ret)) { ret = is_runtime_canceled(r) ? OB_CANCELED : r.ddl_runtime_->posting_error(); }
    if (OB_SUCC(ret) && OB_FAIL(output.copy_loaded_postings(*r.insertion_prior_->posting_source_,
        source_cid, cid, expected))) {
      LOG_WARN("failed to copy source insertion encoded postings", K(ret), K(cid), K(source_cid));
    }
    if (OB_SUCC(ret)) { copied += expected.count(); }
  }
  if (OB_SUCC(ret) && is_runtime_canceled(r)) { ret = OB_CANCELED; }
  if (OB_SUCC(ret)) {
    for (int64_t cid = item.cid_begin_; cid < item.cid_end_; ++cid) {
      const int64_t source_cid = r.insertion_reuse_cids_.at(cid);
      if (source_cid >= 0) { ATOMIC_AAF(&r.insertion_posting_counts_.at(cid), r.insertion_prior_->posting_mass_.at(source_cid)); }
    }
    slot.primary_posting_count_ = copied;
    slot.installed_ = true;
  }
  return ret;
}

ObIVFVectorReader::ObIVFVectorReader()
  : vector_store_(nullptr)
{
}

int ObIVFVectorReader::init(const ObIVFVectorStore &vector_store)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(vector_store_)) {
    ret = OB_INIT_TWICE;
  } else if (!vector_store.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    vector_store_ = &vector_store;
  }
  return ret;
}

int ObIVFVectorReader::batch_get_by_vid(
    const ObIVFVectorIDView &vids,
    const int64_t vid_begin,
    const int64_t vid_end,
    share::ObVectorMatrix &vectors) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector_store_)) {
    ret = OB_NOT_INIT;
  } else if (vid_begin < 0 || vid_end <= vid_begin || vid_end > vids.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(vectors.set_dimension(vector_store_->get_dimension()))) {
  } else if (OB_FAIL(vector_store_->batch_get_by_vid(vids, vid_begin, vid_end, vectors))) {
    LOG_WARN("failed to read IVF vector batch", K(ret), K(vid_begin),
        K(vid_end), "vid_count", vids.count());
  } else if (vectors.count()
      != (vid_end - vid_begin) * vector_store_->get_dimension()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF vector batch size", K(ret), K(vid_begin),
        K(vid_end), "value_count", vectors.count(),
        "dimension", vector_store_->get_dimension());
  }
  return ret;
}

int64_t ObIVFVectorReader::get_dimension() const
{
  return OB_ISNULL(vector_store_) ? 0 : vector_store_->get_dimension();
}

ObIVFMergeInput::ObIVFMergeInput()
  : is_inited_(false),
    vector_store_(nullptr),
    first_level_samples_(nullptr),
    cluster_size_(0),
    enable_soar_(true),
    vectors_normalized_(false),
    vectors_rotated_(false),
    dist_type_(share::VIDA_COS)
{
}

int ObIVFMergeInput::init(
    const ObIVFVectorStore &vector_store,
    const ObIVFFirstLevelSampleSet &first_level_samples,
    const int64_t cluster_size,
    const bool vectors_normalized,
    const bool vectors_rotated,
    const bool enable_soar,
    const share::ObVectorIndexDistAlgorithm dist_type)
{
  int ret = OB_SUCCESS;
  if (is_inited_ || !vector_store.is_valid()
      || !first_level_samples.is_valid()
      || vector_store.get_vector_count() <= 0
      || vector_store.get_dimension() <= 0
      || first_level_samples.get_sample_count() <= 0
      || first_level_samples.get_sample_count() > vector_store.get_vector_count()
      || first_level_samples.get_dimension() != vector_store.get_dimension()
      || !vectors_rotated || (dist_type == share::VIDA_COS && !vectors_normalized)
      || cluster_size <= 0
      || dist_type < share::VIDA_L2 || dist_type >= share::VIDA_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge input", K(ret),
        "vector_store_valid", vector_store.is_valid(),
        "sample_set_valid", first_level_samples.is_valid(), K(cluster_size),
        K(enable_soar), K(dist_type), K(vectors_normalized), K(vectors_rotated));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    vector_store_ = &vector_store;
    first_level_samples_ = &first_level_samples;
    cluster_size_ = cluster_size;
    enable_soar_ = enable_soar;
    vectors_normalized_ = vectors_normalized;
    vectors_rotated_ = vectors_rotated;
    dist_type_ = dist_type;
  }
  return ret;
}

bool ObIVFMergeInput::is_valid() const
{
  return is_inited_ && OB_NOT_NULL(vector_store_) && vector_store_->is_valid()
      && vector_store_->get_vector_count() > 0
      && vector_store_->get_dimension() > 0
      && OB_NOT_NULL(first_level_samples_) && first_level_samples_->is_valid()
      && first_level_samples_->get_sample_count() > 0
      && first_level_samples_->get_sample_count()
          <= vector_store_->get_vector_count()
      && first_level_samples_->get_dimension() == vector_store_->get_dimension()
      && vectors_rotated_ && (dist_type_ != share::VIDA_COS || vectors_normalized_)
      && cluster_size_ > 0
      && dist_type_ >= share::VIDA_L2 && dist_type_ < share::VIDA_MAX;
}

ObIVFNeighborGraphView::ObIVFNeighborGraphView()
  : offsets_(), cids_(), max_intra_distances_(),
    center_count_(0)
{
}

ObIVFNeighborGraphView::ObIVFNeighborGraphView(
    const common::ObIArray<int64_t> &offsets,
    const share::ObVectorArray<int64_t> &cids,
    const common::ObIArray<float> &max_intra_distances,
    const int64_t center_count)
  : offsets_(&offsets), cids_(&cids),
    max_intra_distances_(&max_intra_distances), center_count_(center_count)
{
}

bool ObIVFNeighborGraphView::is_valid() const
{
  bool is_valid = OB_NOT_NULL(offsets_) && OB_NOT_NULL(cids_)
      && OB_NOT_NULL(max_intra_distances_)
      && center_count_ > 0 && offsets_->count() == center_count_ + 1
      && max_intra_distances_->count() == center_count_
      && 0 == offsets_->at(0)
      && offsets_->at(center_count_) == cids_->count();
  for (int64_t i = 0; is_valid && i < center_count_; ++i) {
    is_valid = offsets_->at(i) <= offsets_->at(i + 1)
        && offsets_->at(i) >= 0
        && std::isfinite(max_intra_distances_->at(i))
        && max_intra_distances_->at(i) >= 0.0F;
  }
  return is_valid;
}

ObIVFMergeStepResult::ObIVFMergeStepResult()
  : plan_id_(-1),
    stage_(ObIVFMergeStage::INVALID),
    step_(ObIVFMergeStep::INVALID),
    depth_(IVF_MERGE_INVALID_DEPTH),
    committed_(false),
    prior_finished_(false),
    quality_fallback_(false),
    hierarchy_finished_(false),
    next_cluster_needs_samples_(false),
    next_refine_needs_samples_(false),
    k_(0),
    neighborhood_mode_(ObIVFNeighborhoodMode::INVALID),
    refine_skipped_(false),
    refine_has_changes_(false),
    refine_train_finished_(true),
    centroid_index_finished_(true),
    merge_finished_(false)
{
}

bool ObIVFMergeStepResult::is_valid() const
{
  return plan_id_ >= 0 && is_valid_ivf_merge_operation(stage_, step_, depth_)
      && committed_ && (!next_cluster_needs_samples_
          || (ObIVFMergeStep::HIERARCHY_UPDATE == step_ && !hierarchy_finished_))
      && (!hierarchy_finished_
          || (k_ > 0 && is_valid_ivf_neighborhood_mode(neighborhood_mode_)))
      && (!merge_finished_
          || ObIVFMergeStep::RAW_WRITE == step_);
}

ObIVFMergeArtifact::ObIVFMergeArtifact()
  : finalized_(false),
    selected_type_(ObIVFProcessType::FULL_REBUILD),
    effective_type_(ObIVFProcessType::FULL_REBUILD),
    fallback_reason_(ObIVFQualityFallback::NONE),
    stage_elapsed_us_{},
    vector_count_(0),
    cluster_count_(0),
    secondary_assignment_count_(0)
{
}

ObIVFMergeRuntimeAllocator::ObIVFMergeRuntimeAllocator()
  : allocator_("IVFMergeRun"), used_(0), peak_(0), allocation_count_(0)
{}

void ObIVFMergeRuntimeAllocator::set_attr(const ObMemAttr &attr)
{
  OB_ASSERT(0 == used());
  allocator_.set_attr(attr);
}

void *ObIVFMergeRuntimeAllocator::alloc(const int64_t size)
{
  void *ptr = nullptr;
  const int64_t overhead = sizeof(Header) + alignof(Header) + sizeof(ObAllocAlign::Header);
  if (size > 0 && size <= INT64_MAX - overhead) {
    Header *header = static_cast<Header *>(allocator_.alloc_align(size + sizeof(Header), alignof(Header)));
    if (nullptr != header) {
      header->size_ = size;
      header->owner_ = this;
      const int64_t live = used_.fetch_add(size, std::memory_order_relaxed) + size;
      int64_t peak = peak_.load(std::memory_order_relaxed);
      while (peak < live && !peak_.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {}
      allocation_count_.fetch_add(1, std::memory_order_relaxed);
      ptr = header + 1;
    }
  }
  return ptr;
}

void ObIVFMergeRuntimeAllocator::free(void *ptr)
{
  if (nullptr != ptr) {
    Header *header = static_cast<Header *>(ptr) - 1;
    abort_unless(header->owner_ == this);
    used_.fetch_sub(header->size_, std::memory_order_relaxed);
    allocator_.free_align(header);
  }
}

ObIVFMergeProcessor::ObIVFMergeProcessor()
  : ObIVFDagProcessor(),
    runtime_allocator_(),
    allocator_(nullptr),
    algorithm_(nullptr),
    physical_output_(nullptr),
    runtime_(nullptr),
    process_type_(ObIVFProcessType::INVALID)
{
}

ObIVFMergeProcessor::~ObIVFMergeProcessor()
{
  if (OB_NOT_NULL(runtime_)) {
    if (OB_NOT_NULL(allocator_)) {
      release_active_plan(*allocator_, runtime_->active_plan_);
      release_active_plan(*allocator_, runtime_->cluster_sample_plan_);
      free_runtime_object(*allocator_, runtime_->ddl_runtime_);
      for (int64_t i = 0; i < runtime_->current_nodes_.count(); ++i) {
        release_node_state(
            *allocator_, runtime_->current_nodes_.at(i), false);
      }
      for (int64_t i = 0;
           i < runtime_->initial_member_lists_.count();
           ++i) {
        release_vid_list(
            *allocator_, runtime_->initial_member_lists_.at(i));
      }
      free_runtime_object(*allocator_, runtime_->a0_);
      free_runtime_object(*allocator_, runtime_->g0_offsets_);
      free_runtime_object(*allocator_, runtime_->g0_cids_);
      free_runtime_object(*allocator_, runtime_->g0_max_distances_);
      free_runtime_object(*allocator_, runtime_->working_centers_);
      free_runtime_object(*allocator_, runtime_->refined_assignments_);
      release_member_lists(
          *allocator_, runtime_->refined_member_lists_, false);
      free_runtime_object(*allocator_, runtime_->changed_cids_);
      free_runtime_object(*allocator_, runtime_->old_to_new_);
      free_runtime_object(*allocator_, runtime_->parent_builder_);
      free_runtime_object(*allocator_, runtime_->c1_);
      release_member_lists(
          *allocator_, runtime_->final_member_lists_, false);
      free_runtime_object(*allocator_, runtime_->g1_offsets_);
      free_runtime_object(*allocator_, runtime_->g1_cids_);
      free_runtime_object(*allocator_, runtime_->g1_max_distances_);
      free_runtime_object(*allocator_, runtime_->a1_);
      free_runtime_object(*allocator_, runtime_->a2_);
      release_member_lists(
          *allocator_, runtime_->soar_member_lists_, false);
    }
    runtime_->~ObIVFMergeRuntime();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(runtime_);
    }
    runtime_ = nullptr;
  }
  allocator_ = nullptr;
  algorithm_ = nullptr;
  physical_output_ = nullptr;
  process_type_ = ObIVFProcessType::INVALID;
}

int ObIVFMergeProcessor::configure_ddl_runtime(ObIVFDDLHcaAdapter &algorithm, int64_t parallelism)
{
  int ret = OB_SUCCESS;
  if (runtime_ == nullptr || allocator_ == nullptr || algorithm_ != &algorithm
      || runtime_->ddl_runtime_ != nullptr || runtime_->active_plan_ != nullptr
      || !runtime_->vector_store_->supports_source_order() || parallelism <= 0) {
    ret = OB_STATE_NOT_MATCH;
  } else if (physical_output_ == nullptr || !physical_output_->supports_loaded_quantization()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF DDL runtime requires loaded quantization output", K(ret), KP_(physical_output));
  } else { ret = alloc_runtime_object(*allocator_, runtime_->ddl_runtime_, *allocator_, algorithm, parallelism); }
  return ret;
}

int ObIVFMergeProcessor::init(
    common::ObIAllocator &allocator,
    const ObIVFMergeInput &merge_input,
    ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeQuantizer &physical_output,
    const ObIVFProcessType process_type, const ObIVFMergePrior *prior)
{
  int ret = OB_SUCCESS;
  bool allocator_initialized = false;
  if (OB_FAIL(ObIVFDagProcessor::init())) {
    LOG_WARN("fail to initialize IVF DAG processor", K(ret));
  } else if (ObIVFProcessType::INITIAL_BUILD != process_type) {
    // Merge DagCtx supplies its reclaiming, accounted allocator. Preserve
    // that ownership boundary (including injected allocators in tests).
    ret = init_merge_runtime(allocator, merge_input, algorithm, physical_output, process_type, prior);
    if (OB_FAIL(ret)) {
        LOG_WARN("failed to initialize IVF merge processor runtime", K(ret));
      }
  } else if (OB_FAIL(runtime_allocator_.init(
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      ObMemAttr(MTL_ID(), "IVFFullBuild", ObCtxIds::VECTOR_CTX_ID),
      INT64_MAX))) {
    LOG_WARN("fail to initialize IVF FullBuild runtime allocator", K(ret));
  } else if (FALSE_IT(allocator_initialized = true)) {
  } else if (OB_FAIL(init_merge_runtime(
      runtime_allocator_, merge_input, algorithm, physical_output, process_type, prior))) {
    LOG_WARN("fail to initialize IVF full rebuild runtime", K(ret));
  }
  if (OB_FAIL(ret) && allocator_initialized) {
    runtime_allocator_.destroy();
  }
  return ret;
}

int ObIVFMergeProcessor::init_merge_runtime(
    common::ObIAllocator &allocator,
    const ObIVFMergeInput &merge_input,
    ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeQuantizer &physical_output,
    const ObIVFProcessType process_type, const ObIVFMergePrior *prior)
{
  int ret = OB_SUCCESS;
  ObIVFHierarchyVIDList *root_vid_list = nullptr;
  ObIVFHierarchyNodeState *root_node = nullptr;
  if (!is_inited()) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation",
        "ObIVFMergeProcessor::init_merge_runtime");
    ret = OB_NOT_INIT;
  } else if (!merge_input.is_valid() || OB_NOT_NULL(runtime_)
      || (!is_ivf_full_build_process_type(process_type)
          && process_type != ObIVFProcessType::CONCATENATION
          && (process_type != ObIVFProcessType::INSERTION))
      || (!is_ivf_full_build_process_type(process_type) && (prior == nullptr
          || prior->vector_count_ != merge_input.vector_store_->get_vector_count()
          || prior->dimension_ != merge_input.vector_store_->get_dimension()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge runtime input or prior", K(ret), K(process_type), K(merge_input), KPC(prior));
  } else if (OB_FAIL(alloc_runtime_object(allocator, runtime_, allocator))) {
    LOG_WARN("failed to allocate IVF merge runtime", K(ret));
  } else {
    allocator_ = &allocator;
    algorithm_ = &algorithm;
    physical_output_ = &physical_output;
    runtime_->selected_type_ = process_type;
    runtime_->effective_type_ = process_type;
    runtime_->prior_ = is_ivf_full_build_process_type(process_type) ? nullptr : prior;
    if ((process_type == ObIVFProcessType::INSERTION) && prior != nullptr
        && prior->posting_source_ != nullptr && physical_output.supports_encoded_posting_reuse()) {
      runtime_->insertion_prior_ = prior;
    }
    runtime_->vector_store_ = merge_input.vector_store_;
    runtime_->cluster_size_ = merge_input.cluster_size_;
    runtime_->dimension_ = merge_input.vector_store_->get_dimension();
    ret = runtime_->c0_.set_dimension(runtime_->dimension_);
    runtime_->enable_soar_ = merge_input.enable_soar_;
    runtime_->first_level_samples_ = merge_input.first_level_samples_;
    const int64_t vector_count = runtime_->vector_store_->get_vector_count();
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to set IVF runtime centroid dimension", K(ret), "dimension", runtime_->dimension_);
    } else if (process_type == ObIVFProcessType::CONCATENATION
        && (!prior->can_concatenate() || !physical_output.supports_loaded_posting_reuse()
            || !physical_output.supports_loaded_quantization())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("concat requires complete encoded postings and loaded output", K(ret));
    } else if ((process_type == ObIVFProcessType::INSERTION) && runtime_->insertion_prior_ == nullptr) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("insertion requires a complete encoded posting source", K(ret));
    } else if (prior != nullptr && OB_FAIL(prior->validate())) {
      LOG_WARN("invalid IVF runtime prior", K(ret));
    } else if (runtime_->vector_store_->uses_position_ids()
        && FALSE_IT(runtime_->all_vids_.set_identity(vector_count))) {
    } else if (!runtime_->vector_store_->uses_position_ids() && OB_FAIL(
        runtime_->vector_store_->get_all_vids(runtime_->all_vids_.mutable_array()))) {
      LOG_WARN("fail to enumerate IVF full rebuild VIDs", K(ret));
    } else if (runtime_->all_vids_.count()
        != vector_count) {
      ret = OB_ERR_UNEXPECTED;
    } else if ((process_type == ObIVFProcessType::INSERTION)
        && OB_FAIL(init_insertion_memberships())) {
      LOG_WARN("failed to initialize insertion membership", K(ret));
    } else if (process_type == ObIVFProcessType::CONCATENATION) {
      // Concat imports final leaves directly; it has no vector hierarchy nodes.
    } else if (OB_FAIL(alloc_runtime_object(
        *allocator_, root_vid_list, *allocator_))) {
      LOG_WARN("failed to allocate IVF root VID list", K(ret));
    } else {
      root_vid_list->identity_ = true;
      root_vid_list->sealed_ = true;
    }
    if (OB_SUCC(ret) && process_type != ObIVFProcessType::CONCATENATION) {
      if (OB_FAIL(alloc_runtime_object(
          *allocator_, root_node, *allocator_, runtime_->dimension_))) {
        LOG_WARN("failed to allocate IVF root hierarchy node", K(ret));
      } else if (OB_FAIL(runtime_->current_nodes_.push_back(root_node))) {
        LOG_WARN("failed to append IVF root hierarchy node", K(ret));
      } else {
        root_node->vid_list_ = root_vid_list;
        root_node = nullptr;
        root_vid_list = nullptr;
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    if (OB_NOT_NULL(allocator_)) {
      free_runtime_object(*allocator_, root_node);
      release_vid_list(*allocator_, root_vid_list);
    }
    if (OB_NOT_NULL(runtime_)) {
      runtime_->~ObIVFMergeRuntime();
      allocator.free(runtime_);
      runtime_ = nullptr;
    }
    allocator_ = nullptr;
    algorithm_ = nullptr;
    physical_output_ = nullptr;
  }
  return ret;
}

bool ObIVFMergeProcessor::is_runtime_enabled() const
{
  return OB_NOT_NULL(runtime_) && OB_NOT_NULL(algorithm_) && OB_NOT_NULL(physical_output_);
}

ObIVFProcessType ObIVFMergeProcessor::get_process_type() const
{
  return runtime_ != nullptr ? runtime_->selected_type_
      : process_type_ != ObIVFProcessType::INVALID ? process_type_ : ObIVFProcessType::FULL_REBUILD;
}

bool ObIVFMergeProcessor::uses_prior_prefix() const
{
  return runtime_ != nullptr && !is_ivf_full_build_process_type(runtime_->effective_type_);
}

bool ObIVFMergeProcessor::centroid_route_reused() const
{
  return runtime_ != nullptr && runtime_->centroid_route_reused_;
}

int ObIVFMergeProcessor::resolve_centroid_index_operation(
    ObIVFMergeStep &step, int64_t &depth, bool &required)
{
  int ret = OB_SUCCESS;
  required = false;
  step = ObIVFMergeStep::INVALID;
  depth = IVF_MERGE_INVALID_DEPTH;
  if (!is_runtime_enabled()) {
    // Framework-only processors have no center data to index.
  } else if (is_runtime_canceled(*runtime_)) {
    ret = OB_CANCELED;
    LOG_INFO("[DISK_BBQ] centroid index resolution canceled", K(ret));
  } else if (!runtime_->soar_finished_
      || runtime_->c1_ == nullptr || runtime_->active_plan_ != nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] leaf clustering is not ready for centroid index", K(ret));
  } else {
    if (runtime_->parent_builder_ == nullptr && !runtime_->centroid_layout_.is_ready()) {
      if (runtime_->parent_threshold_ == 0) {
        runtime_->parent_threshold_ = blocksstable::ObIvfCentroidLayout::get_hierarchy_threshold();
      }
      const int64_t leaf_count = runtime_->c1_->count() / runtime_->dimension_;
      const auto *prior = runtime_->insertion_prior_;
      if (runtime_->effective_type_ == ObIVFProcessType::INSERTION
          && prior != nullptr && prior->routing_source_ != nullptr
          // A configuration change across the FLAT/grouped boundary must keep
          // the original creation decision, even when every leaf is unchanged.
          && (leaf_count >= runtime_->parent_threshold_)
              == !prior->routing_source_->parent_centers_.empty()) {
        const int reuse_ret = prior->routing_source_->restore_centroid_layout(
            *runtime_->c1_, runtime_->centroid_layout_, runtime_->centroid_route_reused_);
        if (OB_SUCCESS != reuse_ret) {
          // This is optional acceleration. Incomplete source geometry retains
          // the existing parent builder, including its normal failure handling.
          LOG_WARN("[DISK_BBQ] cannot retain insertion centroid route", K(reuse_ret));
        }
      }
      runtime_->parent_started_us_ = ObTimeUtility::current_time();
      if (runtime_->centroid_route_reused_) {
        return install_centroid_index_layout();
      }
      if (leaf_count < runtime_->parent_threshold_) {
        if (OB_FAIL(runtime_->centroid_layout_.init_flat(leaf_count, runtime_->dimension_))) {
          LOG_WARN("[DISK_BBQ] failed to initialize flat centroid layout", K(ret), K(leaf_count));
        } else {
          ret = install_centroid_index_layout();
        }
      } else {
        ObIVFHcaAlgorithmParam param;
        if (OB_FAIL(algorithm_->get_parent_cluster_params(param))) {
          LOG_WARN("[DISK_BBQ] failed to obtain centroid index clustering parameters", K(ret));
        } else if (OB_FAIL(alloc_runtime_object(*allocator_, runtime_->parent_builder_, *allocator_))) {
          LOG_WARN("[DISK_BBQ] failed to allocate centroid index builder", K(ret), K(leaf_count));
        } else if (OB_FAIL(runtime_->parent_builder_->init(*runtime_->c1_, runtime_->dimension_,
            param, blocksstable::IVF_PARENT_CLUSTER_SIZE))) {
          LOG_WARN("[DISK_BBQ] failed to initialize centroid index builder", K(ret), K(leaf_count));
        }
      }
    }
    if (OB_SUCC(ret) && runtime_->parent_builder_ != nullptr) {
      required = true;
      ret = runtime_->parent_builder_->get_next_operation(step, depth);
    }
  }
  return ret;
}

ObIVFRefinePhase ObIVFMergeProcessor::centroid_index_refine_phase() const
{
  return runtime_ == nullptr || runtime_->parent_builder_ == nullptr
      ? ObIVFRefinePhase::DISTANCE : runtime_->parent_builder_->get_refine_phase();
}

int ObIVFMergeProcessor::install_centroid_index_layout()
{
  int ret = OB_SUCCESS;
  if (runtime_ == nullptr || physical_output_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] centroid index output is not initialized", K(ret), KP_(runtime), KP_(physical_output));
  } else if (!runtime_->centroid_layout_.is_ready()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] centroid index layout is not ready", K(ret));
  } else if (OB_FAIL(physical_output_->set_centroid_layout(runtime_->centroid_layout_))) {
    LOG_WARN("[DISK_BBQ] failed to install centroid index layout", K(ret));
  } else {
    const int64_t layout_elapsed_us = ObTimeUtility::current_time() - runtime_->parent_started_us_;
    LOG_INFO("built IVF centroid layout", "threshold", runtime_->parent_threshold_,
        "cluster_size", blocksstable::IVF_PARENT_CLUSTER_SIZE,
        "leaf_count", runtime_->centroid_layout_.leaf_count(),
        "parent_count", runtime_->centroid_layout_.parent_count(),
        "center_row_count", runtime_->centroid_layout_.center_row_count(), K(layout_elapsed_us));
  }
  return ret;
}

int ObIVFMergeProcessor::prepare_centroid_index_step(ObIVFMergeStep step, int64_t depth,
    int64_t plan_id, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = nullptr;
  ObIVFMergeStep expected_step = ObIVFMergeStep::INVALID;
  int64_t expected_depth = IVF_MERGE_INVALID_DEPTH;
  task_count = 0;
  if (runtime_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] centroid index runtime is not initialized", K(ret), K(step), K(depth), K(plan_id));
  } else if (is_runtime_canceled(*runtime_)) {
    ret = OB_CANCELED;
    LOG_INFO("[DISK_BBQ] centroid index plan preparation canceled", K(ret), K(step), K(depth), K(plan_id));
  } else if (plan_id < 0 || !is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX, step, depth)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid index plan arguments", K(ret), K(step), K(depth), K(plan_id));
  } else if (runtime_->parent_builder_ == nullptr
      || runtime_->active_plan_ != nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] centroid index cannot prepare a task plan", K(ret), K(step), K(depth), K(plan_id));
  } else if (OB_FAIL(runtime_->parent_builder_->get_next_operation(expected_step, expected_depth))) {
    LOG_WARN("[DISK_BBQ] failed to read next centroid index operation", K(ret), K(plan_id));
  } else if (step != expected_step || depth != expected_depth) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] centroid index task does not match its state", K(ret),
        K(step), K(depth), K(expected_step), K(expected_depth), K(plan_id));
  } else if (OB_FAIL(alloc_runtime_object(*allocator_, plan, *allocator_))) {
    LOG_WARN("[DISK_BBQ] failed to allocate centroid index task plan", K(ret), K(plan_id));
  } else if (OB_FAIL(runtime_->parent_builder_->prepare(plan_id, task_count))) {
    LOG_WARN("[DISK_BBQ] failed to prepare centroid index work", K(ret), K(step), K(depth), K(plan_id));
  } else if (OB_FAIL(share::vector_array_resize(plan->work_items_, task_count))) {
    LOG_WARN("[DISK_BBQ] failed to allocate centroid index work items", K(ret), K(plan_id), K(task_count));
  } else if (OB_FAIL(share::vector_array_resize(plan->result_slots_, task_count))) {
    LOG_WARN("[DISK_BBQ] failed to allocate centroid index result slots", K(ret), K(plan_id), K(task_count));
  } else {
    plan->plan_id_ = plan_id;
    plan->stage_ = ObIVFMergeStage::CENTROID_INDEX;
    plan->step_ = step;
    plan->depth_ = depth;
    runtime_->active_plan_ = plan;
    plan = nullptr;
  }
  if (plan != nullptr) { release_active_plan(*allocator_, plan); }
  return ret;
}

int ObIVFMergeProcessor::execute_centroid_index_task(ObIVFMergeStep step, int64_t depth,
    int64_t plan_id, int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_ == nullptr ? nullptr : runtime_->active_plan_;
  if (runtime_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] centroid index runtime is not initialized", K(ret), K(plan_id), K(task_idx));
  } else if (is_runtime_canceled(*runtime_)) {
    ret = OB_CANCELED;
    LOG_INFO("[DISK_BBQ] centroid index task canceled before execution", K(ret),
        K(plan_id), K(task_idx), K(step), K(depth));
  } else if (plan_id < 0 || task_idx < 0
      || !is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX, step, depth)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid index task arguments", K(ret), K(plan_id), K(task_idx), K(step), K(depth));
  } else if (runtime_->parent_builder_ == nullptr || plan == nullptr
      || plan->stage_ != ObIVFMergeStage::CENTROID_INDEX || plan->step_ != step
      || plan->depth_ != depth || plan->plan_id_ != plan_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid centroid index task execution", K(ret), K(plan_id), K(task_idx), K(step), K(depth));
  } else if (task_idx >= plan->result_slots_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] centroid index task index is out of range", K(ret), K(plan_id), K(task_idx),
        "task_count", plan->result_slots_.count());
  } else if (OB_FAIL(runtime_->parent_builder_->execute(task_idx))) {
    LOG_WARN("[DISK_BBQ] centroid index task failed", K(ret), K(plan_id), K(task_idx), K(step), K(depth));
  } else if (is_runtime_canceled(*runtime_)) {
    ret = OB_CANCELED;
    LOG_INFO("[DISK_BBQ] centroid index task canceled before installing result", K(ret),
        "stage", ObIVFMergeStage::CENTROID_INDEX, K(step), K(depth), K(plan_id), K(task_idx));
  } else {
    plan->result_slots_.at(task_idx).installed_ = true;
  }
  return ret;
}

int ObIVFMergeProcessor::finish_centroid_index_step(ObIVFMergeStep step, int64_t depth,
    int64_t plan_id, ObIVFMergeStepResult &step_result)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_ == nullptr ? nullptr : runtime_->active_plan_;
  bool finished = false;
  if (runtime_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] centroid index runtime is not initialized", K(ret), K(plan_id), K(step), K(depth));
  } else if (is_runtime_canceled(*runtime_)) {
    ret = OB_CANCELED;
    LOG_INFO("[DISK_BBQ] centroid index completion canceled", K(ret), K(plan_id), K(step), K(depth));
  } else if (plan_id < 0 || !is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX, step, depth)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid index completion arguments", K(ret), K(plan_id), K(step), K(depth));
  } else if (runtime_->parent_builder_ == nullptr || plan == nullptr
      || plan->stage_ != ObIVFMergeStage::CENTROID_INDEX || plan->step_ != step
      || plan->depth_ != depth || plan->plan_id_ != plan_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid centroid index completion barrier", K(ret), K(plan_id), K(step), K(depth));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan->result_slots_.count(); ++i) {
    if (!plan->result_slots_.at(i).installed_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("[DISK_BBQ] centroid index task has not completed", K(ret), K(plan_id), K(i), K(step), K(depth));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(runtime_->parent_builder_->finish(
      plan_id, runtime_->centroid_layout_, finished))) {
    LOG_WARN("[DISK_BBQ] failed to finish centroid index operation", K(ret), K(plan_id), K(step), K(depth));
  } else if (OB_SUCC(ret) && finished && OB_FAIL(install_centroid_index_layout())) {
    LOG_WARN("[DISK_BBQ] failed to publish completed centroid index layout", K(ret), K(plan_id));
  }
  if (OB_SUCC(ret)) {
    // The inner state machine stops at SOAR. All tasks have joined before its
    // borrowed leaf view is released and the outer build resumes quantization.
    if (finished) { free_runtime_object(*allocator_, runtime_->parent_builder_); }
    release_active_plan(*allocator_, runtime_->active_plan_);
    step_result.plan_id_ = plan_id;
    step_result.stage_ = ObIVFMergeStage::CENTROID_INDEX;
    step_result.step_ = step;
    step_result.depth_ = depth;
    step_result.committed_ = true;
    step_result.centroid_index_finished_ = finished;
  }
  return ret;
}

int ObIVFMergeProcessor::prepare_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t plan_id,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  // Resolution initializes the borrowed center input before this first plan.
  // Include that setup in the centroid-index aggregate as well as step perf.
  const int64_t started_at = stage == ObIVFMergeStage::CENTROID_INDEX
      && runtime_ != nullptr && runtime_->parent_builder_ != nullptr
      && runtime_->parent_builder_->get_step() == ObIVFMergeStep::INVALID
      ? runtime_->parent_started_us_ : ObTimeUtility::current_time();
  task_count = 0;
  if (!is_inited()) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation", "ObIVFMergeProcessor::prepare_step",
        K(plan_id), K(stage), K(step), K(depth));
    ret = OB_NOT_INIT;
  } else if (plan_id < 0 || !is_valid_ivf_merge_operation(stage, step, depth)) {
    LOG_WARN("invalid IVF merge operation", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeProcessor::prepare_step", K(plan_id), K(stage), K(step), K(depth));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_runtime_enabled()
      && step == ObIVFMergeStep::HIERARCHY_WARM_START) {
    // Retain the old operation implementation, but insertion now uses v2.
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFMergeProcessor::prepare_step",
        K(plan_id), K(stage), K(step), K(depth));
    ret = OB_STATE_NOT_MATCH;
  } else if (ObIVFMergeStage::CENTROID_INDEX == stage && is_runtime_enabled()) {
    ret = prepare_centroid_index_step(step, depth, plan_id, task_count);
  } else if (ObIVFMergeStage::PRIOR == stage && is_runtime_enabled()) {
    ret = prepare_prior_step(plan_id, task_count);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "prepare_prior_step", K(plan_id), K(stage), K(step),
          K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY == stage && is_runtime_enabled()) {
    ret = prepare_hierarchy_step(stage, step, depth, plan_id, task_count);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "prepare_hierarchy_step", K(plan_id), K(stage),
          K(step), K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY != stage
      && is_runtime_enabled()) {
    ret = prepare_post_hierarchy_step(
        stage, step, plan_id, task_count);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "prepare_post_hierarchy_step", K(plan_id), K(stage),
          K(step), K(depth));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF full rebuild step has no production implementation",
        K(ret), K(static_cast<int64_t>(stage)),
        K(static_cast<int64_t>(step)), K(depth), K(plan_id));
  }
  if (OB_SUCC(ret) && runtime_ != nullptr && runtime_->active_plan_ != nullptr) {
    runtime_->active_plan_->started_at_us_ = started_at;
  }
  return ret;
}

int ObIVFMergeProcessor::init_insertion_memberships()
{
  int ret = OB_SUCCESS;
  const ObIVFMergePrior &prior = *runtime_->prior_;
  const ObIVFSegmentPriorInfo &segment = prior.segments_.at(prior.dominant_segment_);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFInsertRefs", ObCtxIds::VECTOR_CTX_ID));
  share::ObVectorArray<ObIVFPriorPosting> postings(share::vector_array_allocator(allocator));
  common::ObArray<int64_t, common::ObWrapperAllocator> masses(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(allocator));

  if (!prior.postings_complete_ || !segment.provides_prior_ || segment.centroid_count_ < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insertion requires complete dominant postings", K(ret), K(segment));
  } else if (OB_FAIL(append_filled_int_array(segment.centroid_count_, 0, masses))) {
    LOG_WARN("failed to allocate insertion posting counts", K(ret));
  }
  int64_t posting_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < segment.centroid_count_; ++i) {
    const int64_t mass = prior.posting_mass_.at(segment.centroid_begin_ + i);
    if (mass < 0 || mass > INT64_MAX - posting_count) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("insertion posting count overflows", K(ret), K(i), K(mass), K(posting_count));
    } else { posting_count += mass; }
  }
  if (OB_SUCC(ret)) {
    if (posting_count > prior.postings_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("dominant posting counts exceed captured membership", K(ret), K(posting_count),
          "captured_count", prior.postings_.count());
    } else if (posting_count > (INT64_MAX - (postings.get_block_size() - 1))
        / static_cast<int64_t>(sizeof(ObIVFPriorPosting))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("insertion posting reservation overflows", K(ret), K(posting_count));
    } else if (OB_FAIL(postings.reserve(posting_count))) {
      LOG_WARN("failed to reserve dominant insertion postings", K(ret), K(posting_count));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < prior.postings_.count(); ++i) {
    const ObIVFPriorPosting &posting = prior.postings_.at(i);
    if (posting.centroid_idx_ < 0 || posting.centroid_idx_ >= prior.posting_mass_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid prior posting reference", K(ret), K(i), K(posting.vid_), K(posting.centroid_idx_));
    } else if (posting.centroid_idx_ >= segment.centroid_begin_
        && posting.centroid_idx_ < segment.centroid_begin_ + segment.centroid_count_) {
      if (postings.count() >= posting_count) {
        ret = OB_INVALID_DATA;
        LOG_WARN("dominant postings exceed declared counts", K(ret), K(posting_count), K(i));
      } else if (OB_FAIL(postings.push_back(ObIVFPriorPosting(posting.vid_, posting.centroid_idx_ - segment.centroid_begin_)))) {
        LOG_WARN("failed to retain dominant posting", K(ret), K(i));
      } else {
        ++masses.at(posting.centroid_idx_ - segment.centroid_begin_);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < masses.count(); ++i) {
    if (masses.at(i) != prior.posting_mass_.at(segment.centroid_begin_ + i)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("insertion posting count mismatch", K(ret), K(i), K(masses.at(i)));
    }
  }
  if (OB_SUCC(ret) && !postings.empty()) {
    lib::ob_sort(share::vector_array_begin(postings), share::vector_array_end(postings), [](const ObIVFPriorPosting &a, const ObIVFPriorPosting &b) {
      return a.vid_ < b.vid_ || (a.vid_ == b.vid_ && a.centroid_idx_ < b.centroid_idx_);
    });
  }
  if (OB_SUCC(ret) && (runtime_->all_vids_.count() >= INT64_MAX / int64_t(sizeof(int64_t)) - 1
      || postings.count() >= INT64_MAX / int64_t(sizeof(int64_t)))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_SUCC(ret) && (OB_FAIL(runtime_->insertion_offsets_.reserve(runtime_->all_vids_.count() + 1))
      || OB_FAIL(runtime_->insertion_candidates_.reserve(postings.count())))) {
    LOG_WARN("failed to reserve insertion memberships", K(ret));
  }
  int64_t pos = 0;
  int64_t old_vector_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < runtime_->all_vids_.count(); ++i) {
    const uint64_t vid = runtime_->all_vids_.at(i);
    if ((i > 0 && vid <= runtime_->all_vids_.at(i - 1))
        || (pos < postings.count() && postings.at(pos).vid_ < vid)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("insertion posting has no unique RAW vector", K(ret), K(i), K(vid), K(pos));
    } else if (OB_FAIL(runtime_->insertion_offsets_.push_back(pos))) {
      LOG_WARN("failed to append insertion posting offset", K(ret), K(i));
    } else {
      const int64_t start = pos;
      while (OB_SUCC(ret) && pos < postings.count() && postings.at(pos).vid_ == vid) {
        if (pos > start && postings.at(pos).centroid_idx_ == postings.at(pos - 1).centroid_idx_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("duplicate insertion posting", K(ret), K(vid), K(pos));
        } else if (OB_FAIL(runtime_->insertion_candidates_.push_back(postings.at(pos).centroid_idx_))) {
          LOG_WARN("failed to append insertion candidate", K(ret), K(vid), K(pos));
        } else {
          ++pos;
        }
      }
      old_vector_count += pos > start;
      if (OB_SUCC(ret) && runtime_->insertion_prior_ != nullptr && pos == start) {
        if (OB_FAIL(runtime_->insertion_delta_vids_.push_back(vid))) {
          LOG_WARN("failed to retain incremental delta VID", K(ret), K(vid), K(i));
        } else if (OB_FAIL(runtime_->insertion_delta_idxs_.push_back(i))) {
          LOG_WARN("failed to retain incremental delta index", K(ret), K(vid), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (pos != postings.count() || old_vector_count != segment.live_vector_count_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("incomplete dominant insertion membership", K(ret), K(pos), K(old_vector_count), K(segment));
    } else if (OB_FAIL(runtime_->insertion_offsets_.push_back(pos))) {
      LOG_WARN("failed to seal insertion posting offsets", K(ret));
    }
  }
  if (OB_SUCC(ret) && runtime_->insertion_prior_ != nullptr) {
    bool supported = runtime_->enable_soar_ || postings.count() == old_vector_count;
    for (int64_t cid = 0; cid < segment.centroid_count_; ++cid) {
      supported = supported && (masses.at(cid) > 0 || (runtime_->insertion_prior_ != nullptr));
    }
    for (int64_t i = 0; i < runtime_->all_vids_.count(); ++i) {
      supported = supported && runtime_->insertion_offsets_.at(i + 1)
          - runtime_->insertion_offsets_.at(i) <= 2;
    }
    if (supported) {
      runtime_->insertion_old_count_ = old_vector_count;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("insertion requires at most two inherited relations per vector", K(ret));
    }
  }
  return ret;
}

// Keep exact center bytes: copied residual codes remain relative to these centers.
int ObIVFMergeProcessor::init_concat_memberships()
{
  int ret = OB_SUCCESS;
  auto &r = *runtime_;
  const auto &prior = *r.prior_;
  const int64_t sources = prior.posting_mass_.count();
  const int64_t postings = prior.postings_.count();
  common::ObArray<int64_t, common::ObWrapperAllocator> centers(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));

  if (OB_FAIL(alloc_runtime_array(*allocator_, r.old_to_new_)) || OB_FAIL(alloc_runtime_array(*allocator_, r.c1_))
      || OB_FAIL(r.c1_->set_dimension(r.dimension_))
      || OB_FAIL(append_filled_int_array(sources, -1, *r.old_to_new_))
      || OB_FAIL(append_filled_int_array(sources + 1, 0, r.concat_offsets_))
      || OB_FAIL(centers.reserve(sources))
      || OB_FAIL(r.c1_->reserve(prior.centroids_.count()))
      || OB_FAIL(share::vector_array_resize(r.concat_postings_, postings))
      || OB_FAIL(share::vector_array_resize(r.concat_keep_, postings))) {
    LOG_WARN("failed to allocate concat mapping", K(ret), K(sources), K(postings));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sources; ++i) { ret = centers.push_back(i); }
  const int64_t bytes = r.dimension_ * sizeof(float);
  const auto compare_centers = [&](int64_t a, int64_t b) {
    return MEMCMP(&prior.centroids_.at(a * r.dimension_), &prior.centroids_.at(b * r.dimension_), bytes);
  };
  if (OB_SUCC(ret)) {
    lib::ob_sort(centers.begin(), centers.end(), [&](int64_t a, int64_t b) {
      const int cmp = compare_centers(a, b);
      return cmp < 0 || (cmp == 0 && a < b);
    });
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sources; ++i) {
    if (i == 0 || compare_centers(centers.at(i - 1), centers.at(i)) != 0) {
      for (int64_t d = 0; OB_SUCC(ret) && d < r.dimension_; ++d) {
        ret = r.c1_->push_back(prior.centroids_.at(centers.at(i) * r.dimension_ + d));
      }
      if (OB_SUCC(ret)) { ret = r.concat_counts_.push_back(0); }
    }
    r.old_to_new_->at(centers.at(i)) = r.concat_counts_.count() - 1;
    if (OB_SUCC(ret) && i % IVF_CENTERS_PER_TASK == 0) {
      ret = is_runtime_canceled(r) ? OB_CANCELED : share::dag_yield();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < postings; ++i) {
    const int64_t cid = prior.postings_.at(i).centroid_idx_;
    if (cid < 0 || cid >= sources) { ret = OB_INVALID_DATA; }
    else { r.concat_postings_.at(i) = i; r.concat_keep_.at(i) = 0; }
  }
  if (OB_SUCC(ret)) {
    // VID-first order checks complete RAW coverage and deduplicates (CID, VID).
    lib::ob_sort(share::vector_array_begin(r.concat_postings_), share::vector_array_end(r.concat_postings_), [&](int64_t a, int64_t b) {
      const auto &x = prior.postings_.at(a), &y = prior.postings_.at(b);
      const int64_t cx = r.old_to_new_->at(x.centroid_idx_), cy = r.old_to_new_->at(y.centroid_idx_);
      return x.vid_ != y.vid_ ? x.vid_ < y.vid_ : cx != cy ? cx < cy : x.centroid_idx_ < y.centroid_idx_;
    });
  }
  int64_t vector_pos = 0, emitted = 0, last_cid = -1, relations = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < postings; ++i) {
    const int64_t idx = r.concat_postings_.at(i);
    const auto &posting = prior.postings_.at(idx);
    const int64_t cid = r.old_to_new_->at(posting.centroid_idx_);
    if (i == 0 || posting.vid_ != prior.postings_.at(r.concat_postings_.at(i - 1)).vid_) {
      if (vector_pos >= r.all_vids_.count() || posting.vid_ != r.all_vids_.at(vector_pos)
          || (vector_pos > 0 && posting.vid_ <= r.all_vids_.at(vector_pos - 1))) {
        ret = OB_INVALID_DATA;
      } else { ++vector_pos; last_cid = -1; relations = 0; }
    }
    if (OB_SUCC(ret) && cid != last_cid) {
      if (++relations > 2 || r.concat_counts_.at(cid) == UINT32_MAX) { ret = OB_INVALID_DATA; }
      else { r.concat_keep_.at(idx) = 1; ++r.concat_counts_.at(cid); ++emitted; last_cid = cid; }
    }
    if (OB_SUCC(ret) && i % IVF_VECTOR_VIDS_PER_TASK == 0) {
      ret = is_runtime_canceled(r) ? OB_CANCELED : share::dag_yield();
    }
  }
  if (OB_SUCC(ret) && vector_pos != r.all_vids_.count()) { ret = OB_INVALID_DATA; }
  if (OB_SUCC(ret)) {
    lib::ob_sort(share::vector_array_begin(r.concat_postings_), share::vector_array_end(r.concat_postings_), [&](int64_t a, int64_t b) {
      const auto &x = prior.postings_.at(a), &y = prior.postings_.at(b);
      return x.centroid_idx_ != y.centroid_idx_ ? x.centroid_idx_ < y.centroid_idx_ : x.vid_ < y.vid_;
    });
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < postings; ++i) {
    const auto &posting = prior.postings_.at(r.concat_postings_.at(i));
    if (i > 0) {
      const auto &prev = prior.postings_.at(r.concat_postings_.at(i - 1));
      if (prev.centroid_idx_ == posting.centroid_idx_ && prev.vid_ == posting.vid_) { ret = OB_INVALID_DATA; }
    }
    ++r.concat_offsets_.at(posting.centroid_idx_ + 1);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sources; ++i) {
    if (r.concat_offsets_.at(i + 1) != prior.posting_mass_.at(i)) { ret = OB_INVALID_DATA; }
    else { r.concat_offsets_.at(i + 1) += r.concat_offsets_.at(i); }
  }
  if (OB_SUCC(ret)) {
    r.final_cluster_count_ = r.concat_counts_.count();
    r.secondary_assignment_count_ = emitted - r.all_vids_.count();
    ret = physical_output_->prepare_loaded_output(r.final_cluster_count_,
        r.ddl_runtime_ == nullptr ? 1 : r.ddl_runtime_->parallelism());
  }
  if (OB_FAIL(ret)) { LOG_WARN("failed to prepare concatenated postings", K(ret), K(sources), K(postings)); }
  return ret;
}

// The existing sink accepts one ordered source at a time and sorts output rows.
// Filter only relations proven redundant by center deduplication; validate all
// scanned rows against the captured source membership, including filtered rows.
class ObIVFConcatPostingSource final : public ObIVFEncodedPostingSource
{
public:
  explicit ObIVFConcatPostingSource(const ObIVFMergeRuntime &runtime) : r_(runtime) {}
  int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &output) const override
  {
    class Consumer final : public ObIVFEncodedPostingConsumer
    {
    public:
      Consumer(const ObIVFMergeRuntime &r, int64_t begin, int64_t end, ObIVFEncodedPostingConsumer &out)
        : r_(r), pos_(begin), end_(end), out_(out) {}
      int append(ObIVFVectorVID vid, const ObString &payload) override
      {
        int ret = OB_SUCCESS;
        if (is_runtime_canceled(r_)) { ret = OB_CANCELED; }
        else if (pos_ >= end_ || r_.prior_->postings_.at(r_.concat_postings_.at(pos_)).vid_ != vid) {
          ret = OB_INVALID_DATA;
        } else {
          const int64_t idx = r_.concat_postings_.at(pos_++);
          if (r_.concat_keep_.at(idx)) { ret = out_.append(vid, payload); }
        }
        return ret;
      }
      const ObIVFMergeRuntime &r_;
      int64_t pos_, end_;
      ObIVFEncodedPostingConsumer &out_;
    } consumer(r_, r_.concat_offsets_.at(cid), r_.concat_offsets_.at(cid + 1), output);
    int ret = r_.prior_->posting_source_->scan_postings(cid, consumer);
    if (OB_SUCC(ret) && consumer.pos_ != consumer.end_) { ret = OB_INVALID_DATA; }
    return ret;
  }
private:
  const ObIVFMergeRuntime &r_;
};

int ObIVFMergeProcessor::prepare_prior_step(const int64_t plan_id, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  ObIVFMergeActivePlan *plan = nullptr;
  if (!uses_prior_prefix() || runtime_->prior_ == nullptr || runtime_->active_plan_ != nullptr
      || is_runtime_canceled(*runtime_)) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::prepare_prior_step", K(plan_id), "runtime_prior",
        static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->prior_), "runtime_active_plan",
        static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->active_plan_));
    ret = OB_STATE_NOT_MATCH;
  }
  else if (runtime_->effective_type_ == ObIVFProcessType::CONCATENATION
      && OB_FAIL(init_concat_memberships())) {
    LOG_WARN("failed to import concat membership", K(ret));
  } else {
    runtime_->prior_started_ = true;
    task_count = runtime_->effective_type_ == ObIVFProcessType::CONCATENATION
        ? get_chunk_task_count(runtime_->prior_->posting_mass_.count(), IVF_CENTERS_PER_TASK) : 1;
    if (task_count <= 0) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::prepare_prior_step", K(plan_id), K(task_count));
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_FAIL(alloc_runtime_object(*allocator_, plan, *allocator_))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "alloc_runtime_object");
    }
    else {
      plan->plan_id_ = plan_id; plan->stage_ = ObIVFMergeStage::PRIOR;
      plan->step_ = ObIVFMergeStep::PRIOR_WORK; plan->depth_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < task_count; ++i) {
        ObIVFMergeWorkItem item;
        if (runtime_->effective_type_ == ObIVFProcessType::CONCATENATION) {
          item.cid_begin_ = i * IVF_CENTERS_PER_TASK;
          item.cid_end_ = MIN(item.cid_begin_ + IVF_CENTERS_PER_TASK, runtime_->prior_->posting_mass_.count());
        }
        ret = plan->work_items_.push_back(item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "plan->work_items_.push_back", K(plan_id), K(i),
              K(task_count));
        }
      }
      if (OB_SUCC(ret)) {
        ret = share::vector_array_resize(plan->result_slots_, task_count);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "plan->result_slots_.prepare_allocate",
              K(plan_id));
        }
      }
      if (OB_SUCC(ret)) { runtime_->active_plan_ = plan; plan = nullptr; }
    }
  }
  release_active_plan(*allocator_, plan);
  return ret;
}

int ObIVFMergeProcessor::execute_prior_task(const int64_t plan_id, const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_->active_plan_;
  if (is_runtime_canceled(*runtime_) || plan == nullptr || plan->plan_id_ != plan_id
      || plan->stage_ != ObIVFMergeStage::PRIOR || task_idx < 0
      || task_idx >= plan->result_slots_.count() || plan->result_slots_.at(task_idx).installed_) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::execute_prior_task", K(plan_id), K(task_idx), KP(runtime_), KP(plan), "plan_plan_id",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->plan_id_), "plan_stage",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->stage_), "plan_result_slots_count",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->result_slots_.count()));
    ret = OB_STATE_NOT_MATCH;
  } else if (runtime_->effective_type_ == ObIVFProcessType::CONCATENATION) {
    ObIVFConcatPostingSource source(*runtime_);

    share::ObVectorArray<ObIVFVectorVID> expected(ObMemAttr(MTL_ID(), "IVFConcatCopy", ObCtxIds::VECTOR_CTX_ID));
    const auto &item = plan->work_items_.at(task_idx);
    auto &slot = plan->result_slots_.at(task_idx);
    for (int64_t cid = item.cid_begin_; OB_SUCC(ret) && cid < item.cid_end_; ++cid) {
      expected.reuse();
      for (int64_t i = runtime_->concat_offsets_.at(cid);
           OB_SUCC(ret) && i < runtime_->concat_offsets_.at(cid + 1); ++i) {
        const int64_t idx = runtime_->concat_postings_.at(i);
        if (runtime_->concat_keep_.at(idx)) { ret = expected.push_back(runtime_->prior_->postings_.at(idx).vid_); }
      }
      if (OB_SUCC(ret)) {
        ret = is_runtime_canceled(*runtime_) ? OB_CANCELED : physical_output_->copy_loaded_postings(
            source, cid, runtime_->old_to_new_->at(cid), expected);
      }
      if (OB_SUCC(ret)) { slot.primary_posting_count_ += expected.count(); }
    }
    if (OB_SUCC(ret) && is_runtime_canceled(*runtime_)) { ret = OB_CANCELED; }
    if (OB_SUCC(ret)) { slot.installed_ = true; }
  } else { plan->result_slots_.at(task_idx).installed_ = true; }

  return ret;
}

int ObIVFMergeProcessor::finish_prior_step(const int64_t plan_id, ObIVFMergeStepResult &result)
{
  int ret = OB_SUCCESS;
  auto &r = *runtime_;
  ObIVFMergeActivePlan *plan = r.active_plan_;
  if (is_runtime_canceled(r) || plan == nullptr || plan->plan_id_ != plan_id
      || plan->stage_ != ObIVFMergeStage::PRIOR || r.prior_ == nullptr) {
    ret = OB_STATE_NOT_MATCH;
  }
  int64_t copied = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < plan->result_slots_.count(); ++i) {
    if (!plan->result_slots_.at(i).installed_) { ret = OB_STATE_NOT_MATCH; }
    else { copied += plan->result_slots_.at(i).primary_posting_count_; }
  }
  if (OB_SUCC(ret) && r.effective_type_ == ObIVFProcessType::CONCATENATION) {
    if (copied != r.all_vids_.count() + r.secondary_assignment_count_) { ret = OB_INVALID_DATA; }
    else if (OB_FAIL(physical_output_->set_leaf_posting_counts(r.concat_counts_))) {
      LOG_WARN("concat posting counts disagree with output", K(ret));
    } else {
      r.hierarchy_k_ = r.final_cluster_count_;
      r.hierarchy_finished_ = r.refine_finished_ = r.soar_finished_ = true;
      r.refine_skipped_ = true;
      r.neighborhood_mode_ = ObIVFNeighborhoodMode::SKIP;
      LOG_INFO("IVF concat copied existing clusters", "source_centers", r.prior_->posting_mass_.count(),
          "output_centers", r.final_cluster_count_, "source_postings", r.prior_->postings_.count(), K(copied));
      r.concat_postings_.reset(); r.concat_keep_.reset(); r.concat_offsets_.reset(); r.concat_counts_.reset();
      free_runtime_object(*allocator_, r.old_to_new_);
    }
  } else if (OB_SUCC(ret)) {
    const auto &segment = r.prior_->segments_.at(r.prior_->dominant_segment_);
    const int64_t k = segment.centroid_count_;
    if (r.current_nodes_.count() != 1 || k < 2) { ret = OB_INVALID_DATA; }
    else {
      auto &root = *r.current_nodes_.at(0);
      for (int64_t i = segment.centroid_begin_ * r.dimension_;
           OB_SUCC(ret) && i < (segment.centroid_begin_ + k) * r.dimension_; ++i) {
        ret = root.local_centers_.push_back(r.prior_->centroids_.at(i));
      }
      if (OB_SUCC(ret)) { root.local_k_ = k; }
    }
  }
  if (OB_SUCC(ret)) {
    release_active_plan(*allocator_, r.active_plan_);
    result.plan_id_ = plan_id; result.stage_ = ObIVFMergeStage::PRIOR;
    result.step_ = ObIVFMergeStep::PRIOR_WORK; result.depth_ = 0;
    result.committed_ = result.prior_finished_ = true;
    r.prior_ = nullptr;
  } else { LOG_WARN("failed to finish IVF prior import", K(ret), K(plan_id)); }
  return ret;
}

int ObIVFMergeProcessor::prepare_hierarchy_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t plan_id,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *active_plan = nullptr;
  task_count = 0;
  if (is_runtime_canceled(*runtime_) || runtime_->hierarchy_finished_
      || OB_NOT_NULL(runtime_->active_plan_) || runtime_->current_nodes_.empty()) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::prepare_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), KP(runtime_),
        "runtime_hierarchy_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->hierarchy_finished_),
        "runtime_active_plan", static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->active_plan_),
        "runtime_current_nodes_count",
        (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->current_nodes_.count()));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(alloc_runtime_object(*allocator_, active_plan, *allocator_))) {
    LOG_WARN("failed to allocate IVF hierarchy task plan", K(ret),
        K(stage), K(step), K(depth), K(plan_id));
  } else {
    active_plan->plan_id_ = plan_id;
    active_plan->stage_ = stage;
    active_plan->step_ = step;
    active_plan->depth_ = depth;
    if (ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE == step
        || ObIVFMergeStep::HIERARCHY_CLUSTER == step
        || ObIVFMergeStep::HIERARCHY_WARM_START == step) {
      if (OB_FAIL(active_plan->work_items_.reserve(
          runtime_->current_nodes_.count()))) {
        LOG_WARN("failed to reserve IVF hierarchy cluster work items", K(ret),
            K(depth), K(plan_id),
            "node_count", runtime_->current_nodes_.count());
      }
      for (int64_t node_idx = 0;
           OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count();
           ++node_idx) {
        ObIVFMergeWorkItem work_item;
        work_item.node_idx_ = node_idx;
        ret = active_plan->work_items_.push_back(work_item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step), K(depth), K(node_idx), "runtime_current_nodes_count",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->current_nodes_.count()));
        }
      }
      if (OB_SUCC(ret) && step == ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE) {
        if (!needs_cluster_samples(*runtime_, *algorithm_, depth)
            || nullptr != runtime_->cluster_sample_plan_) {
          ret = OB_STATE_NOT_MATCH;
        } else {
          ret = prepare_cluster_samples(*runtime_, *algorithm_, *active_plan, *allocator_);
        }
      } else if (OB_SUCC(ret) && step == ObIVFMergeStep::HIERARCHY_CLUSTER
          && needs_cluster_samples(*runtime_, *algorithm_, depth)) {
        if (nullptr == runtime_->cluster_sample_plan_ || runtime_->cluster_sample_plan_->depth_ != depth) {
          ret = OB_STATE_NOT_MATCH;
        }
      }
    } else if (ObIVFMergeStep::HIERARCHY_ASSIGN == step
        && depth > 0
        && runtime_->vector_store_->supports_source_order() && algorithm_->supports_loaded_assign()
        && runtime_->current_nodes_.count() <= INT32_MAX) {
      ret = prepare_source_assign(*runtime_, *active_plan, *allocator_);
    } else if (ObIVFMergeStep::HIERARCHY_ASSIGN == step) {
      int64_t expected_work_item_count = 0;
      for (int64_t i = 0; i < runtime_->current_nodes_.count(); ++i) {
        ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(i);
        if (OB_ISNULL(node) || node->local_k_ <= 0
            || node->local_centers_.count()
                != node->local_k_ * runtime_->dimension_
            || OB_ISNULL(node->vid_list_)
            || !is_valid_vector_ref_list(
                *node->vid_list_, runtime_->all_vids_)) {
          ret = OB_STATE_NOT_MATCH;
          break;
        } else {
          const int64_t node_task_count = get_chunk_task_count(
              get_vector_ref_count(*node->vid_list_, runtime_->all_vids_),
              IVF_VECTOR_VIDS_PER_TASK);
          if (node_task_count <= 0
              || INT64_MAX - expected_work_item_count < node_task_count) {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          expected_work_item_count += node_task_count;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(active_plan->work_items_.reserve(
          expected_work_item_count))) {
        LOG_WARN("failed to reserve IVF hierarchy assignment work items",
            K(ret), K(depth), K(plan_id), K(expected_work_item_count));
      }
      for (int64_t node_idx = 0; OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count(); ++node_idx) {
        const int64_t vid_count = get_vector_ref_count(
            *runtime_->current_nodes_.at(node_idx)->vid_list_,
            runtime_->all_vids_);
        for (int64_t vid_begin = 0; OB_SUCC(ret) && vid_begin < vid_count;
             vid_begin += IVF_VECTOR_VIDS_PER_TASK) {
          ObIVFMergeWorkItem work_item;
          work_item.node_idx_ = node_idx;
          work_item.vid_begin_ = vid_begin;
          work_item.vid_end_ = MIN(
              vid_count, vid_begin + IVF_VECTOR_VIDS_PER_TASK);
          ret = active_plan->work_items_.push_back(work_item);
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back",
                K(plan_id), K(stage), K(step), K(depth), K(vid_begin), K(vid_count));
          }
        }
      }
    } else if (ObIVFMergeStep::HIERARCHY_UPDATE == step) {
      int64_t expected_work_item_count = 0;
      for (int64_t node_idx = 0; OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count(); ++node_idx) {
        ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(node_idx);
        if (OB_ISNULL(node) || OB_ISNULL(node->member_lists_)
            || node->member_lists_->lists_.count() != node->local_k_) {
          ret = OB_STATE_NOT_MATCH;
        }
        for (int64_t local_center_idx = 0; OB_SUCC(ret) && local_center_idx < node->local_k_; ++local_center_idx) {
          ObIVFHierarchyVIDList *member_list = node->member_lists_->lists_.at(local_center_idx);
          if (OB_ISNULL(member_list)) {
            ret = OB_STATE_NOT_MATCH;
          } else if (!member_list->vids_.empty() || (runtime_->insertion_prior_ != nullptr && depth == 0)) {
            ++expected_work_item_count;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(active_plan->work_items_.reserve(
          expected_work_item_count))) {
        LOG_WARN("failed to reserve IVF hierarchy update work items", K(ret),
            K(depth), K(plan_id), K(expected_work_item_count));
      }
      for (int64_t node_idx = 0;
           OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count();
           ++node_idx) {
        ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(node_idx);
        for (int64_t local_center_idx = 0;
             OB_SUCC(ret) && local_center_idx < node->local_k_;
             ++local_center_idx) {
          ObIVFHierarchyVIDList *member_list =
              node->member_lists_->lists_.at(local_center_idx);
          if (OB_NOT_NULL(member_list) && (!member_list->vids_.empty()
              || (runtime_->insertion_prior_ != nullptr && depth == 0))) {
            ObIVFMergeWorkItem work_item;
            work_item.node_idx_ = node_idx;
            work_item.local_center_idx_ = local_center_idx;
            ret = active_plan->work_items_.push_back(work_item);
            if (OB_FAIL(ret)) {
              LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back",
                  K(plan_id), K(stage), K(step), K(depth), KP(member_list), "member_list_vids_count",
                  (nullptr == member_list) ? -1 : static_cast<int64_t>(member_list->vids_.count()));
            }
          }
        }
      }
    } else {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
          "ObIVFMergeProcessor::prepare_hierarchy_step", K(plan_id), K(stage), K(step), K(depth));
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (step == ObIVFMergeStep::HIERARCHY_UPDATE
      && runtime_->vector_store_->supports_source_order() && algorithm_->supports_source_update()
      && active_plan->work_items_.count() <= INT32_MAX) {
    ret = prepare_source_update(*runtime_, *active_plan, *allocator_);
  }
  if (OB_FAIL(ret)) {
  } else if (active_plan->work_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF hierarchy task plan has no work items", K(ret),
        K(stage), K(step), K(depth), K(plan_id));
  } else if (OB_FAIL(share::vector_array_resize(active_plan->result_slots_,
      active_plan->work_items_.count()))) {
    LOG_WARN("failed to allocate IVF hierarchy result slots", K(ret),
        K(stage), K(step), K(plan_id),
        "task_count", active_plan->work_items_.count());
  } else {
    runtime_->active_plan_ = active_plan;
    task_count = active_plan->work_items_.count();
    active_plan = nullptr;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    release_active_plan(*allocator_, active_plan);
  }
  return ret;
}

int ObIVFMergeProcessor::prepare_post_hierarchy_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t plan_id,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *active_plan = nullptr;
  task_count = 0;
  if (is_runtime_canceled(*runtime_) || !runtime_->hierarchy_finished_
      || runtime_->merge_finished_
      || OB_NOT_NULL(runtime_->active_plan_)) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), KP(runtime_),
        "runtime_hierarchy_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->hierarchy_finished_),
        "runtime_merge_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->merge_finished_),
        "runtime_active_plan", static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->active_plan_));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(alloc_runtime_object(
      *allocator_, active_plan, *allocator_))) {
    LOG_WARN("failed to allocate IVF post-hierarchy task plan", K(ret),
        K(stage), K(step), K(plan_id));
  } else {
    active_plan->plan_id_ = plan_id;
    active_plan->stage_ = stage;
    active_plan->step_ = step;
    active_plan->depth_ = IVF_MERGE_INVALID_DEPTH;
    const int64_t hierarchy_k = runtime_->hierarchy_k_;
    const bool neighborhood_ready =
        ObIVFNeighborhoodMode::SKIP == runtime_->neighborhood_mode_
        || runtime_->neighborhood_finished_;
    if (runtime_->ddl_runtime_ != nullptr && runtime_->ddl_runtime_->handles(*runtime_, step, *physical_output_)) {
      ret = runtime_->ddl_runtime_->prepare(*runtime_, *active_plan, *physical_output_);
    } else if (ObIVFMergeStep::NEIGHBORHOOD_ADD == step) {
      if (ObIVFNeighborhoodMode::HGRAPH != runtime_->neighborhood_mode_
          || runtime_->neighborhood_started_ || hierarchy_k <= 0) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "runtime_neighborhood_mode",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_),
            "runtime_neighborhood_started",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_started_), K(hierarchy_k));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(active_plan->work_items_.reserve(
          get_chunk_task_count(hierarchy_k, IVF_CENTERS_PER_TASK)))) {
        LOG_WARN("failed to reserve IVF neighborhood-add work items", K(ret),
            K(plan_id), K(hierarchy_k));
      }
      for (int64_t cid_begin = 0;
           OB_SUCC(ret) && cid_begin < hierarchy_k;
           cid_begin += IVF_CENTERS_PER_TASK) {
        ObIVFMergeWorkItem work_item;
        work_item.cid_begin_ = cid_begin;
        work_item.cid_end_ = MIN(
            hierarchy_k, cid_begin + IVF_CENTERS_PER_TASK);
        ret = active_plan->work_items_.push_back(work_item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step), K(cid_begin), K(hierarchy_k));
        }
      }
      if (OB_SUCC(ret)) {
        ret = algorithm_->neighborhood_begin(
            runtime_->neighborhood_mode_,
            runtime_->c0_,
            hierarchy_k,
            runtime_->dimension_);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "algorithm_->neighborhood_begin", K(plan_id),
              K(stage), K(step));
        }
        if (OB_SUCC(ret)) {
          runtime_->neighborhood_started_ = true;
        }
      }
    } else if (ObIVFMergeStep::NEIGHBORHOOD_BUILD == step) {
      if ((ObIVFNeighborhoodMode::BRUTE_FORCE
              != runtime_->neighborhood_mode_
           && ObIVFNeighborhoodMode::HGRAPH
              != runtime_->neighborhood_mode_)
          || runtime_->neighborhood_finished_ || hierarchy_k <= 0) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "runtime_neighborhood_mode",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_),
            "runtime_neighborhood_finished",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_finished_), K(hierarchy_k));
        ret = OB_STATE_NOT_MATCH;
      } else if (ObIVFNeighborhoodMode::HGRAPH == runtime_->neighborhood_mode_
          && (!runtime_->neighborhood_started_
              || !runtime_->neighborhood_add_finished_)) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "runtime_neighborhood_mode",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_),
            "runtime_neighborhood_started",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_started_),
            "runtime_neighborhood_add_finished",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_add_finished_));
        ret = OB_STATE_NOT_MATCH;
      } else if (ObIVFNeighborhoodMode::BRUTE_FORCE == runtime_->neighborhood_mode_
          && runtime_->neighborhood_started_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "runtime_neighborhood_mode",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_),
            "runtime_neighborhood_started",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_started_));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(active_plan->work_items_.reserve(
          get_chunk_task_count(hierarchy_k, IVF_CENTERS_PER_TASK)))) {
        LOG_WARN("failed to reserve IVF neighborhood-build work items", K(ret),
            K(plan_id), K(hierarchy_k));
      }
      for (int64_t cid_begin = 0;
           OB_SUCC(ret) && cid_begin < hierarchy_k;
           cid_begin += IVF_CENTERS_PER_TASK) {
        ObIVFMergeWorkItem work_item;
        work_item.cid_begin_ = cid_begin;
        work_item.cid_end_ = MIN(
            hierarchy_k, cid_begin + IVF_CENTERS_PER_TASK);
        ret = active_plan->work_items_.push_back(work_item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step), K(cid_begin), K(hierarchy_k));
        }
      }
      if (OB_SUCC(ret)
          && ObIVFNeighborhoodMode::BRUTE_FORCE == runtime_->neighborhood_mode_) {
        ret = algorithm_->neighborhood_begin(
            runtime_->neighborhood_mode_,
            runtime_->c0_,
            hierarchy_k,
            runtime_->dimension_);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "algorithm_->neighborhood_begin", K(plan_id),
              K(stage), K(step), "runtime_neighborhood_mode",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_));
        }
        if (OB_SUCC(ret)) {
          runtime_->neighborhood_started_ = true;
        }
      }
    } else if (ObIVFMergeStep::REFINE_TRAIN == step) {
      const int64_t count = runtime_->working_centers_ == nullptr ? 1
          : algorithm_->refine_train_task_count();
      if (!neighborhood_ready || runtime_->refine_skipped_
          || runtime_->a0_ == nullptr || count <= 0) {
        ret = OB_STATE_NOT_MATCH;
      } else if (((runtime_->ddl_runtime_ != nullptr || runtime_->selected_type_ == ObIVFProcessType::INITIAL_BUILD)
          && algorithm_->supports_batched_refine_train())) {
        ObIVFVectorReader reader;
        ObIVFNeighborGraphView graph_view;
        const ObIVFNeighborGraphView *graph = nullptr;
        int64_t batch_size = 0;
        if (runtime_->g0_offsets_ != nullptr && runtime_->g0_cids_ != nullptr
            && runtime_->g0_max_distances_ != nullptr) {
          graph_view = ObIVFNeighborGraphView(*runtime_->g0_offsets_, *runtime_->g0_cids_,
              *runtime_->g0_max_distances_, hierarchy_k);
          graph = &graph_view;
        }
        if (runtime_->ddl_runtime_ != nullptr) {
          ret = runtime_->ddl_runtime_->prepare_refine_phase(batch_size);
        } else if (OB_FAIL(reader.init(*runtime_->vector_store_))) {
        } else {
          ret = algorithm_->refine_train_prepare(reader, runtime_->all_vids_,
              runtime_->c0_, *runtime_->a0_, graph, hierarchy_k, batch_size);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to prepare IVF refinement distance batch", K(ret), K(plan_id));
        } else if (batch_size <= 0) { ret = OB_ERR_UNEXPECTED; }
        else if (OB_FAIL(active_plan->work_items_.reserve(
            get_chunk_task_count(batch_size, IVF_VECTOR_VIDS_PER_TASK)))) {
        }
        for (int64_t begin = 0; OB_SUCC(ret) && begin < batch_size; begin += IVF_VECTOR_VIDS_PER_TASK) {
          ObIVFMergeWorkItem item;
          item.vid_begin_ = begin;
          item.vid_end_ = MIN(batch_size, begin + IVF_VECTOR_VIDS_PER_TASK);
          if (OB_FAIL(active_plan->work_items_.push_back(item))) {
            LOG_WARN("failed to append IVF refinement distance task", K(ret), K(begin));
          }
        }
      } else if (OB_FAIL(active_plan->work_items_.reserve(count))) {
        LOG_WARN("failed to reserve IVF refine distance tasks", K(ret), K(count));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          ret = active_plan->work_items_.push_back(ObIVFMergeWorkItem());
          if (OB_FAIL(ret)) {
        LOG_WARN("failed to append IVF refine training task", K(ret));
      }
        }
      }
    } else if (ObIVFMergeStep::REFINE_ASSIGN == step) {
      if (!neighborhood_ready || runtime_->refine_skipped_
          || OB_ISNULL(runtime_->a0_)
          || OB_ISNULL(runtime_->working_centers_)
          || OB_NOT_NULL(runtime_->refined_assignments_)
          || runtime_->working_centers_->count()
              != hierarchy_k * runtime_->dimension_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), K(neighborhood_ready),
            "runtime_refine_skipped", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->refine_skipped_),
            "runtime_a0", static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->a0_),
            "runtime_working_centers",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->working_centers_),
            "runtime_refined_assignments",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->refined_assignments_),
            "runtime_working_centers_count",
            (nullptr == runtime_ || nullptr == runtime_->working_centers_) ? -1 : static_cast<int64_t>(runtime_->working_centers_->count()));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(active_plan->work_items_.reserve(
          get_chunk_task_count(
              runtime_->all_vids_.count(), IVF_VECTOR_VIDS_PER_TASK)))) {
        LOG_WARN("failed to reserve IVF refine-assignment work items", K(ret),
            K(plan_id), "vector_count", runtime_->all_vids_.count());
      }
      for (int64_t vid_begin = 0;
           OB_SUCC(ret) && vid_begin < runtime_->all_vids_.count();
           vid_begin += IVF_VECTOR_VIDS_PER_TASK) {
        ObIVFMergeWorkItem work_item;
        work_item.vid_begin_ = vid_begin;
        work_item.vid_end_ = MIN(
            runtime_->all_vids_.count(),
            vid_begin + IVF_VECTOR_VIDS_PER_TASK);
        ret = active_plan->work_items_.push_back(work_item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step), K(vid_begin), "runtime_all_vids_count",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()));
        }
      }
    } else if (ObIVFMergeStep::REFINE_UPDATE == step) {
      if (runtime_->refine_skipped_ || !runtime_->refine_has_changes_
          || OB_ISNULL(runtime_->working_centers_)
          || OB_ISNULL(runtime_->refined_member_lists_)
          || OB_ISNULL(runtime_->changed_cids_)
          || runtime_->changed_cids_->count() != hierarchy_k) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), "runtime_refine_skipped",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->refine_skipped_), "runtime_refine_has_changes",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->refine_has_changes_), "runtime_working_centers",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->working_centers_),
            "runtime_refined_member_lists",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->refined_member_lists_),
            "runtime_changed_cids",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->changed_cids_),
            "runtime_changed_cids_count",
            (nullptr == runtime_ || nullptr == runtime_->changed_cids_) ? -1 : static_cast<int64_t>(runtime_->changed_cids_->count()));
        ret = OB_STATE_NOT_MATCH;
      }
      for (int64_t cid = 0; OB_SUCC(ret) && cid < hierarchy_k; ++cid) {
        if (0 != runtime_->changed_cids_->at(cid)
            && !runtime_->refined_member_lists_->lists_.at(cid)
                    ->vids_.empty()) {
          ObIVFMergeWorkItem work_item;
          work_item.local_center_idx_ = cid;
          ret = active_plan->work_items_.push_back(work_item);
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back",
                K(plan_id), K(stage), K(step), "runtime_changed_cids_count",
                (nullptr == runtime_ || nullptr == runtime_->changed_cids_) ? -1 : static_cast<int64_t>(runtime_->changed_cids_->count()),
                K(cid), "runtime_refined_member_lists_lists_count",
                (nullptr == runtime_ || nullptr == runtime_->refined_member_lists_) ? -1 : static_cast<int64_t>(runtime_->refined_member_lists_->lists_.count()));
          }
        }
      }
    } else if (ObIVFMergeStep::REFINE_COMPACT == step) {
      const bool skipped_state_valid = runtime_->refine_skipped_
          && OB_NOT_NULL(runtime_->a0_)
          && !runtime_->initial_member_lists_.empty();
      const bool refined_state_valid = !runtime_->refine_skipped_
          && OB_NOT_NULL(runtime_->working_centers_)
          && OB_NOT_NULL(runtime_->refined_assignments_)
          && OB_NOT_NULL(runtime_->refined_member_lists_);
      if (!neighborhood_ready || runtime_->refine_finished_
          || (!skipped_state_valid && !refined_state_valid)) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), K(neighborhood_ready),
            "runtime_refine_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->refine_finished_),
            K(skipped_state_valid), K(refined_state_valid));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(active_plan->work_items_.reserve(1))) {
        LOG_WARN("failed to reserve IVF refine-compact work item", K(ret),
            K(plan_id));
      } else {
        ret = active_plan->work_items_.push_back(
            ObIVFMergeWorkItem());
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step));
        }
      }
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step && runtime_->insertion_prior_ != nullptr) {
      ret = prepare_incremental_soar(*runtime_, *active_plan, *physical_output_);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to prepare incremental IVF SOAR", K(ret));
      }
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step) {
      if (!runtime_->refine_finished_ || OB_ISNULL(runtime_->old_to_new_)
          || OB_ISNULL(runtime_->c1_)
          || OB_ISNULL(runtime_->final_member_lists_)
          || OB_NOT_NULL(runtime_->a1_)) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "runtime_refine_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->refine_finished_),
            "runtime_old_to_new", static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->old_to_new_),
            "runtime_c1", static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->c1_),
            "runtime_final_member_lists",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->final_member_lists_), "runtime_a1",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->a1_));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(active_plan->work_items_.reserve(
          get_chunk_task_count(
              runtime_->all_vids_.count(), IVF_VECTOR_VIDS_PER_TASK)))) {
        LOG_WARN("failed to reserve IVF SOAR-assignment work items", K(ret),
            K(plan_id), "vector_count", runtime_->all_vids_.count());
      }
      for (int64_t vid_begin = 0;
           OB_SUCC(ret) && vid_begin < runtime_->all_vids_.count();
           vid_begin += IVF_VECTOR_VIDS_PER_TASK) {
        ObIVFMergeWorkItem work_item;
        work_item.vid_begin_ = vid_begin;
        work_item.vid_end_ = MIN(
            runtime_->all_vids_.count(),
            vid_begin + IVF_VECTOR_VIDS_PER_TASK);
        ret = active_plan->work_items_.push_back(work_item);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back", K(plan_id),
              K(stage), K(step), K(vid_begin), "runtime_all_vids_count",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()));
        }
      }
    } else if (ObIVFMergeStep::QUANT == step && (runtime_->effective_type_ == ObIVFProcessType::CONCATENATION
        || uses_loaded_insertion(*runtime_, *physical_output_))) {
      if (!runtime_->soar_finished_ || runtime_->quant_finished_ || runtime_->c1_ == nullptr
          || (uses_loaded_insertion(*runtime_, *physical_output_)
              && OB_SUCCESS != (ret = runtime_->ddl_runtime_->posting_error()))) {
        if (OB_SUCC(ret)) { ret = OB_STATE_NOT_MATCH; }
      } else {
        ObIVFMergeWorkItem centers;
        centers.cid_begin_ = 0;
        centers.cid_end_ = runtime_->final_cluster_count_;
        ret = active_plan->work_items_.push_back(centers);
        for (int64_t i = 0; OB_SUCC(ret) && i < physical_output_->posting_write_task_count(); ++i) {
          ObIVFMergeWorkItem postings;
          postings.local_center_idx_ = i;
          ret = active_plan->work_items_.push_back(postings);
        }
      }
    } else if (ObIVFMergeStep::QUANT == step) {
      if (!runtime_->soar_finished_ || OB_ISNULL(runtime_->c1_)
          || OB_ISNULL(runtime_->final_member_lists_)
          || runtime_->quant_finished_) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), "runtime_soar_finished",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->soar_finished_), "runtime_c1",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->c1_), "runtime_a1",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->a1_), "runtime_final_member_lists",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->final_member_lists_),
            "runtime_quant_finished", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->quant_finished_));
        ret = OB_STATE_NOT_MATCH;
      }
      const int64_t final_k = OB_ISNULL(runtime_->final_member_lists_)
          ? 0 : runtime_->final_member_lists_->lists_.count();
      int64_t task_count = 0;
      if (OB_SUCC(ret) && OB_FAIL(physical_output_->prepare_quantization(
          *runtime_->c1_, final_k, task_count))) {
        LOG_WARN("failed to prepare IVF quantization ranges", K(ret), K(final_k));
      } else if (OB_SUCC(ret) && (task_count <= 0 || task_count > final_k)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid IVF quantization task count", K(ret), K(task_count), K(final_k));
      } else if (OB_SUCC(ret) && OB_FAIL(active_plan->work_items_.reserve(task_count))) {
        LOG_WARN("failed to reserve IVF quantization ranges", K(ret), K(task_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < task_count; ++i) {
        ObIVFMergeWorkItem work_item;
        const int64_t width = final_k / task_count;
        const int64_t extra = final_k % task_count;
        work_item.cid_begin_ = i * width + MIN(i, extra);
        work_item.cid_end_ = work_item.cid_begin_ + width + (i < extra ? 1 : 0);
        work_item.local_center_idx_ = work_item.cid_begin_;
        ret = active_plan->work_items_.push_back(work_item);
      }
    } else if (ObIVFMergeStep::RAW_WRITE == step) {
      if (!runtime_->quant_finished_ || runtime_->merge_finished_
          || runtime_->all_vids_.empty()) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step), "runtime_quant_finished",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->quant_finished_), "runtime_merge_finished",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->merge_finished_), "runtime_all_vids_count",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(runtime_->vector_store_->prepare_raw_output())) {
        LOG_WARN("failed to prepare IVF RAW output order", K(ret));
      } else {
        const int64_t vector_count = runtime_->all_vids_.count();
        const int64_t raw_task_count = MIN(
            ObIVFMergeProcessor::MAX_RAW_TASK_COUNT,
            (vector_count - 1) / IVF_RAW_VIDS_PER_TASK + 1);
        const int64_t vids_per_task =
            (vector_count - 1) / raw_task_count + 1;
        if (OB_FAIL(active_plan->work_items_.reserve(raw_task_count))) {
          LOG_WARN("failed to reserve IVF RAW-write work items", K(ret),
              K(plan_id), K(vector_count), K(raw_task_count));
        }
        for (int64_t vid_begin = 0;
             OB_SUCC(ret) && vid_begin < vector_count;
             vid_begin += vids_per_task) {
          ObIVFMergeWorkItem work_item;
          work_item.vid_begin_ = vid_begin;
          work_item.vid_end_ = MIN(vector_count, vid_begin + vids_per_task);
          ret = active_plan->work_items_.push_back(work_item);
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "active_plan->work_items_.push_back",
                K(plan_id), K(stage), K(step), K(vid_begin), K(vector_count), K(vids_per_task));
          }
        }
      }
    } else {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
          "ObIVFMergeProcessor::prepare_post_hierarchy_step", K(plan_id), K(stage), K(step));
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (active_plan->work_items_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF post-hierarchy task plan has no work items", K(ret),
        K(stage), K(step), K(plan_id));
  } else if (OB_FAIL(share::vector_array_resize(active_plan->result_slots_,
      active_plan->work_items_.count()))) {
    LOG_WARN("failed to allocate IVF post-hierarchy result slots", K(ret),
        K(stage), K(step), K(plan_id),
        "task_count", active_plan->work_items_.count());
  } else {
    runtime_->active_plan_ = active_plan;
    task_count = active_plan->work_items_.count();
    active_plan = nullptr;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    release_active_plan(*allocator_, active_plan);
  }
  return ret;
}

int ObIVFMergeProcessor::execute_task(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t task_idx,
    ObIVFTaskResult &task_result)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation", "ObIVFMergeProcessor::execute_task",
        K(task_idx), K(stage), K(step), K(depth));
    ret = OB_NOT_INIT;
  } else if (!task_result.is_valid() || task_result.is_sealed() || !is_valid_ivf_merge_operation(stage, step, depth)) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFMergeProcessor::execute_task",
        K(task_idx), K(stage), K(step), K(depth));
    ret = OB_STATE_NOT_MATCH;
  } else if (is_runtime_enabled()
      && step == ObIVFMergeStep::HIERARCHY_WARM_START) {
    // No supported strategy executes the legacy insertion warm-start step.
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "ObIVFMergeProcessor::execute_task",
        K(task_idx), K(stage), K(step), K(depth));
    ret = OB_STATE_NOT_MATCH;
  } else if (ObIVFMergeStage::CENTROID_INDEX == stage && is_runtime_enabled()) {
    ret = execute_centroid_index_task(step, depth, task_result.get_plan_id(), task_idx);
  } else if (ObIVFMergeStage::PRIOR == stage && is_runtime_enabled()) {
    ret = execute_prior_task(task_result.get_plan_id(), task_idx);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "execute_prior_task", K(task_idx), K(stage), K(step),
          K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY == stage && is_runtime_enabled()) {
    ret = execute_hierarchy_task(stage, step, depth, task_result.get_plan_id(), task_idx);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "execute_hierarchy_task", K(task_idx), K(stage),
          K(step), K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY != stage && is_runtime_enabled()) {
    ret = execute_post_hierarchy_task(stage, step, task_result.get_plan_id(), task_idx);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "execute_post_hierarchy_task", K(task_idx), K(stage),
          K(step), K(depth));
    }
  } else {
    LOG_WARN("unsupported IVF merge operation", "ret", OB_NOT_SUPPORTED, "operation",
        "ObIVFMergeProcessor::execute_task", K(task_idx), K(stage), K(step), K(depth));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObIVFMergeProcessor::execute_hierarchy_task(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t plan_id,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_->active_plan_;
  if (is_runtime_canceled(*runtime_) || OB_ISNULL(plan)
      || plan->plan_id_ != plan_id || plan->stage_ != stage
      || plan->step_ != step || plan->depth_ != depth
      || task_idx < 0 || task_idx >= plan->work_items_.count()) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
        KP(runtime_), KP(plan), "plan_plan_id", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->plan_id_),
        "plan_stage", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->stage_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObIVFMergeResultSlot &slot = plan->result_slots_.at(task_idx);
    const ObIVFMergeWorkItem &work_item = plan->work_items_.at(task_idx);
    ObIVFHierarchyNodeState *node = (plan->source_assign_ || nullptr != plan->source_update_
        || step == ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE)
        ? nullptr : runtime_->current_nodes_.at(work_item.node_idx_);
    ObIVFVectorReader reader;
    if (slot.installed_) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
          K(slot.installed_));
      ret = OB_STATE_NOT_MATCH;
    } else if (ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE == step) {
      ret = gather_cluster_samples(*runtime_, *plan, task_idx);
    } else if (nullptr != plan->source_update_) {
      ret = execute_source_update(*runtime_, *algorithm_, *plan, task_idx);
    } else if (plan->source_assign_) {
      ret = execute_source_assign(*runtime_, *algorithm_, *plan, task_idx);
    } else if (OB_FAIL(reader.init(*runtime_->vector_store_))) {
      LOG_WARN("failed to initialize IVF hierarchy vector reader", K(ret),
          K(stage), K(step), K(depth), K(task_idx), K(work_item));
    } else if (ObIVFMergeStep::HIERARCHY_CLUSTER == step || ObIVFMergeStep::HIERARCHY_WARM_START == step) {
      share::ObVectorMatrix *centers = nullptr;
      int64_t local_k = 0;
      if (OB_FAIL(alloc_runtime_array(*allocator_, centers))) {
        LOG_WARN("failed to allocate IVF hierarchy centers", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (step == ObIVFMergeStep::HIERARCHY_WARM_START
          && OB_FAIL(algorithm_->insertion_train_centers(reader, get_vector_vids(*node->vid_list_, runtime_->all_vids_),
              *runtime_->first_level_samples_, node->local_centers_, node->local_k_, *centers))) {
        LOG_WARN("failed to warm-start insertion root", K(ret));
      } else if (step == ObIVFMergeStep::HIERARCHY_WARM_START
          && FALSE_IT(local_k = node->local_k_)) {
      } else if (nullptr != runtime_->cluster_sample_plan_
          && OB_FAIL(train_cluster_samples(*runtime_, *algorithm_, *runtime_->cluster_sample_plan_,
              work_item, local_k, *centers))) {
        LOG_WARN("failed to train compact IVF samples", K(ret), K(task_idx));
      } else if (nullptr == runtime_->cluster_sample_plan_ && step != ObIVFMergeStep::HIERARCHY_WARM_START
          && OB_FAIL(algorithm_->hca_train_local_centers(
          depth,
          work_item.node_idx_,
          reader,
          get_vector_vids(*node->vid_list_, runtime_->all_vids_),
          0 == depth
              ? runtime_->first_level_samples_
              : nullptr,
          runtime_->cluster_size_,
          local_k,
          *centers))) {
        LOG_WARN("failed to train IVF hierarchy local centers", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (local_k < 1 || local_k > 128
          || centers->count() != local_k * runtime_->dimension_
          || !are_finite_center_values(*centers)) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
            K(local_k), "centers_count", (nullptr == centers) ? -1 : static_cast<int64_t>(centers->count()),
            "runtime_dimension", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_ERR_UNEXPECTED;
      } else if (is_runtime_canceled(*runtime_)) {
        LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
            "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
            KP(runtime_));
        ret = OB_CANCELED;
      } else {
        slot.local_k_ = local_k;
        slot.center_values_ = centers;
        slot.installed_ = true;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
        free_runtime_object(*allocator_, centers);
      }
    } else if (ObIVFMergeStep::HIERARCHY_ASSIGN == step) {
      ObArray<int64_t, ObWrapperAllocator> *local_center_idxs = nullptr;
      const int64_t work_item_count =
          work_item.vid_end_ - work_item.vid_begin_;
      if (OB_FAIL(alloc_runtime_array(*allocator_, local_center_idxs))) {
        LOG_WARN("failed to allocate IVF hierarchy local assignments", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (OB_FAIL(local_center_idxs->reserve(work_item_count))) {
        LOG_WARN("failed to reserve IVF hierarchy local assignments", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if ((runtime_->effective_type_ == ObIVFProcessType::INSERTION) && depth == 0
          && OB_FAIL(assign_frozen_insertion(*runtime_, *algorithm_, reader, node->local_centers_,
                  work_item.vid_begin_, work_item.vid_end_, *local_center_idxs))) {
        LOG_WARN("failed to assign insertion vectors", K(ret), K(task_idx));
      } else if (!((runtime_->effective_type_ == ObIVFProcessType::INSERTION) && depth == 0)
          && OB_FAIL(algorithm_->hca_assign_vectors(
          reader,
          get_vector_vids(*node->vid_list_, runtime_->all_vids_),
          node->local_centers_,
          node->local_k_,
          work_item.vid_begin_,
          work_item.vid_end_,
          *local_center_idxs))) {
        LOG_WARN("failed to assign IVF hierarchy vectors", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (local_center_idxs->count() != work_item_count) {
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < local_center_idxs->count(); ++i) {
        const int64_t local_center_idx = local_center_idxs->at(i);
        if (local_center_idx < 0 || local_center_idx >= node->local_k_) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
              K(local_center_idx), "node_local_k", (nullptr == node) ? -1 : static_cast<int64_t>(node->local_k_));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_runtime_canceled(*runtime_)) {
          LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
              "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
              KP(runtime_));
          ret = OB_CANCELED;
        } else {
          slot.center_idxs_ = local_center_idxs;
          local_center_idxs = nullptr;
          slot.installed_ = true;
        }
      }
      free_runtime_object(*allocator_, local_center_idxs);
    } else if (ObIVFMergeStep::HIERARCHY_UPDATE == step) {
      share::ObVectorMatrix *updated_center = nullptr;
      ObIVFHierarchyVIDList *member_list = node->member_lists_->lists_.at(work_item.local_center_idx_);
      const bool update_center = runtime_->insertion_prior_ == nullptr || depth != 0;
      const int64_t member_count = member_list->vids_.count();
      const int64_t parent_count = get_vector_ref_count(*node->vid_list_, runtime_->all_vids_);
      const bool skip_stats = runtime_->insertion_prior_ == nullptr && can_skip_hierarchy_center_stats(
          member_count, parent_count, runtime_->cluster_size_);
      // Incremental root plans also retain empty inherited centers.
      const bool inherited_root = runtime_->insertion_prior_ != nullptr && depth == 0;
      if (member_count < 0 || (member_count == 0 && !inherited_root)
          || member_count > parent_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid IVF hierarchy center member count", K(ret), K(depth),
            K(task_idx), K(member_count), K(parent_count));
      } else if (skip_stats) {
        // Keep the existing task/slot order without reading unused vectors.
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, updated_center))) {
        LOG_WARN("failed to allocate IVF hierarchy updated center", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (OB_FAIL(updated_center->reserve(runtime_->dimension_))) {
        LOG_WARN("failed to reserve IVF hierarchy updated center", K(ret),
            K(depth), K(task_idx), K(work_item),
            "dimension", runtime_->dimension_);
      } else if (OB_FAIL(
          update_center ? algorithm_->hca_compute_local_center_stats_range(
              reader, member_list->vids_, node->local_centers_,
              work_item.local_center_idx_, *updated_center)
          : copy_insertion_center(node->local_centers_, work_item.local_center_idx_,
              runtime_->dimension_, *updated_center))) {
        LOG_WARN("failed to compute IVF hierarchy center stats", K(ret),
            K(depth), K(task_idx), K(work_item));
      } else if (updated_center->count() != runtime_->dimension_
          || !are_finite_center_values(*updated_center)) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
            "updated_center_count", (nullptr == updated_center) ? -1 : static_cast<int64_t>(updated_center->count()),
            "runtime_dimension", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCC(ret)) {
        if (is_runtime_canceled(*runtime_)) {
          LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
              "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth),
              KP(runtime_));
          ret = OB_CANCELED;
        } else {
          slot.center_values_ = updated_center;
          slot.installed_ = true;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
        free_runtime_object(*allocator_, updated_center);
      }
    } else {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::execute_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(depth));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObIVFMergeProcessor::execute_post_hierarchy_task(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t plan_id,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_->active_plan_;
  if (is_runtime_canceled(*runtime_) || OB_ISNULL(plan)
      || plan->plan_id_ != plan_id || plan->stage_ != stage
      || plan->step_ != step
      || IVF_MERGE_INVALID_DEPTH != plan->depth_
      || task_idx < 0 || task_idx >= plan->work_items_.count()) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), KP(runtime_),
        KP(plan), "plan_plan_id", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->plan_id_), "plan_stage",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->stage_), "plan_step",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->step_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObIVFMergeResultSlot &slot = plan->result_slots_.at(task_idx);
    const ObIVFMergeWorkItem &work_item = plan->work_items_.at(task_idx);
    ObIVFVectorReader reader;
    const int64_t hierarchy_k = runtime_->hierarchy_k_;
    if (slot.installed_) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
          K(slot.installed_));
      ret = OB_STATE_NOT_MATCH;
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step && uses_loaded_insertion(*runtime_, *physical_output_)
        && OB_SUCCESS != (ret = runtime_->ddl_runtime_->posting_error())) {
      // A sink may have accepted a prefix before a task failed. Never retry an
      // incremental posting task into the same prepared output.
    } else if (runtime_->ddl_runtime_ != nullptr && runtime_->ddl_runtime_->handles(*runtime_, step, *physical_output_)) {
      ret = runtime_->ddl_runtime_->execute(*runtime_, *plan, task_idx, *physical_output_);
    } else if (OB_FAIL(reader.init(*runtime_->vector_store_))) {
      LOG_WARN("failed to initialize IVF post-hierarchy vector reader", K(ret),
          K(stage), K(step), K(task_idx), K(work_item));
    } else if (ObIVFMergeStep::NEIGHBORHOOD_ADD == step) {
      if (OB_FAIL(algorithm_->neighborhood_add_centers(
          runtime_->c0_,
          hierarchy_k,
          runtime_->dimension_,
          work_item.cid_begin_,
          work_item.cid_end_))) {
        LOG_WARN("failed to add IVF neighborhood centers", K(ret),
            K(task_idx), K(work_item));
      } else if (is_runtime_canceled(*runtime_)) {
        LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            KP(runtime_));
        ret = OB_CANCELED;
      } else {
        slot.installed_ = true;
      }
    } else if (ObIVFMergeStep::NEIGHBORHOOD_BUILD == step) {
      ObArray<int64_t, ObWrapperAllocator> *neighbor_offsets = nullptr;
      ObArray<int64_t, ObWrapperAllocator> *neighbor_cids = nullptr;
      ObArray<float, ObWrapperAllocator> *neighbor_max_distances = nullptr;
      const int64_t row_count = work_item.cid_end_ - work_item.cid_begin_;
      if (OB_FAIL(alloc_runtime_object(*allocator_, neighbor_offsets,
          OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(*allocator_)))) {
        LOG_WARN("failed to allocate IVF neighbor offsets", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, neighbor_cids))) {
        LOG_WARN("failed to allocate IVF neighbor CIDs", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(alloc_runtime_object(*allocator_, neighbor_max_distances,
          OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(*allocator_)))) {
        LOG_WARN("failed to allocate IVF neighbor distances", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(algorithm_->neighborhood_find_neighbors(
          runtime_->neighborhood_mode_,
          runtime_->c0_,
          hierarchy_k,
          runtime_->dimension_,
          work_item.cid_begin_,
          work_item.cid_end_,
          *neighbor_offsets,
          *neighbor_cids,
          *neighbor_max_distances))) {
        LOG_WARN("failed to build IVF neighborhood rows", K(ret),
            K(task_idx), K(work_item));
      } else if (neighbor_offsets->count() != row_count + 1
          || neighbor_max_distances->count() != row_count
          || 0 != neighbor_offsets->at(0)
          || neighbor_offsets->at(row_count) != neighbor_cids->count()) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            "neighbor_offsets_count",
            (nullptr == neighbor_offsets) ? -1 : static_cast<int64_t>(neighbor_offsets->count()), K(row_count),
            "neighbor_max_distances_count",
            (nullptr == neighbor_max_distances) ? -1 : static_cast<int64_t>(neighbor_max_distances->count()),
            "neighbor_cids_count", (nullptr == neighbor_cids) ? -1 : static_cast<int64_t>(neighbor_cids->count()));
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t local_row = 0; OB_SUCC(ret) && local_row < row_count; ++local_row) {
        const int64_t begin = neighbor_offsets->at(local_row);
        const int64_t end = neighbor_offsets->at(local_row + 1);
        const int64_t self_cid = work_item.cid_begin_ + local_row;
        if (begin < 0 || end <= begin || end > neighbor_cids->count()
            || end - begin > IVF_MAX_NEIGHBOR_COUNT
            || !std::isfinite(neighbor_max_distances->at(local_row))
            || neighbor_max_distances->at(local_row) < 0.0F) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(begin),
              K(end), "neighbor_cids_count",
              (nullptr == neighbor_cids) ? -1 : static_cast<int64_t>(neighbor_cids->count()),
              "neighbor_max_distances_count",
              (nullptr == neighbor_max_distances) ? -1 : static_cast<int64_t>(neighbor_max_distances->count()),
              K(local_row));
          ret = OB_ERR_UNEXPECTED;
        }
        for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
          const int64_t neighbor_cid = neighbor_cids->at(i);
          if (neighbor_cid < 0 || neighbor_cid >= hierarchy_k
              || neighbor_cid == self_cid
              || contains_cid(*neighbor_cids, begin, i, neighbor_cid)) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
                K(neighbor_cid), K(hierarchy_k), K(self_cid), K(neighbor_cids), K(begin));
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_runtime_canceled(*runtime_)) {
          LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              KP(runtime_));
          ret = OB_CANCELED;
        } else {
          slot.neighbor_offsets_ = neighbor_offsets;
          slot.neighbor_cids_ = neighbor_cids;
          slot.neighbor_max_distances_ = neighbor_max_distances;
          slot.installed_ = true;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
        free_runtime_object(*allocator_, neighbor_offsets);
        free_runtime_object(*allocator_, neighbor_cids);
        free_runtime_object(*allocator_, neighbor_max_distances);
      }
    } else if (ObIVFMergeStep::REFINE_TRAIN == step
        && ((runtime_->ddl_runtime_ != nullptr || runtime_->selected_type_ == ObIVFProcessType::INITIAL_BUILD)
          && algorithm_->supports_batched_refine_train())) {
      ret = runtime_->ddl_runtime_ != nullptr
          ? runtime_->ddl_runtime_->execute_refine_phase(work_item.vid_begin_, work_item.vid_end_)
          : algorithm_->refine_train_distances(work_item.vid_begin_, work_item.vid_end_);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to compute IVF refinement distances", K(ret), K(task_idx), K(work_item));
      } else if (is_runtime_canceled(*runtime_)) {
        ret = OB_CANCELED;
        LOG_INFO("[DISK_BBQ] refinement distance task canceled before installing result", K(ret),
            K(stage), K(step), K(plan_id), K(task_idx), K(work_item));
      } else {
        slot.installed_ = true;
      }
    } else if (ObIVFMergeStep::REFINE_TRAIN == step
        && runtime_->working_centers_ != nullptr) {
      if (OB_FAIL(algorithm_->refine_train_distance_task(task_idx))) {
        LOG_WARN("failed to compute IVF refine distances", K(ret), K(plan_id), K(task_idx));
      } else {
        slot.installed_ = true;
      }
    } else if (ObIVFMergeStep::REFINE_TRAIN == step) {
      share::ObVectorMatrix *working_centers = nullptr;
      ObIVFNeighborGraphView graph_view;
      const ObIVFNeighborGraphView *graph = nullptr;
      if (OB_NOT_NULL(runtime_->g0_offsets_)
          && OB_NOT_NULL(runtime_->g0_cids_)
          && OB_NOT_NULL(runtime_->g0_max_distances_)) {
        graph_view = ObIVFNeighborGraphView(
            *runtime_->g0_offsets_, *runtime_->g0_cids_,
            *runtime_->g0_max_distances_, hierarchy_k);
        if (!graph_view.is_valid()) {
          LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              K(graph_view));
          ret = OB_STATE_NOT_MATCH;
        } else {
          graph = &graph_view;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, working_centers))) {
        LOG_WARN("failed to allocate IVF refined working centers", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(algorithm_->refine_train_begin(
          reader,
          runtime_->all_vids_,
          runtime_->c0_,
          *runtime_->a0_,
          graph,
          hierarchy_k,
          *working_centers))) {
        LOG_WARN("failed to train IVF refined centers", K(ret),
            K(task_idx), K(work_item));
      } else if (working_centers->count() != hierarchy_k * runtime_->dimension_
          || !are_finite_center_values(*working_centers)) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            "working_centers_count", (nullptr == working_centers) ? -1 : static_cast<int64_t>(working_centers->count()),
            K(hierarchy_k), "runtime_dimension",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_ERR_UNEXPECTED;
      } else if (is_runtime_canceled(*runtime_)) {
        LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            KP(runtime_));
        ret = OB_CANCELED;
      } else {
        slot.center_values_ = working_centers;
        slot.installed_ = true;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
        free_runtime_object(*allocator_, working_centers);
      }
    } else if (ObIVFMergeStep::REFINE_ASSIGN == step) {
      ObArray<int64_t, ObWrapperAllocator> *new_cids = nullptr;
      const int64_t work_item_count =
          work_item.vid_end_ - work_item.vid_begin_;
      ObIVFNeighborGraphView graph_view;
      const ObIVFNeighborGraphView *graph = nullptr;
      if (OB_NOT_NULL(runtime_->g0_offsets_)
          && OB_NOT_NULL(runtime_->g0_cids_)
          && OB_NOT_NULL(runtime_->g0_max_distances_)) {
        graph_view = ObIVFNeighborGraphView(
            *runtime_->g0_offsets_, *runtime_->g0_cids_,
            *runtime_->g0_max_distances_, hierarchy_k);
        if (!graph_view.is_valid()) {
          LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              K(graph_view));
          ret = OB_STATE_NOT_MATCH;
        } else {
          graph = &graph_view;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, new_cids))) {
        LOG_WARN("failed to allocate IVF refined CIDs", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(new_cids->reserve(work_item_count))) {
        LOG_WARN("failed to reserve IVF refined CIDs", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(algorithm_->refine_assign_vectors(
          reader,
          runtime_->all_vids_,
          *runtime_->working_centers_,
          *runtime_->a0_,
          graph,
          hierarchy_k,
          work_item.vid_begin_,
          work_item.vid_end_,
          *new_cids))) {
        LOG_WARN("failed to refine IVF vector assignments", K(ret),
            K(task_idx), K(work_item));
      } else if (new_cids->count() != work_item_count) {
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < new_cids->count(); ++i) {
        const int64_t cid = new_cids->at(i);
        if (cid < 0 || cid >= hierarchy_k) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step), K(cid),
              K(hierarchy_k));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_runtime_canceled(*runtime_)) {
          LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              KP(runtime_));
          ret = OB_CANCELED;
        } else {
          slot.center_idxs_ = new_cids;
          new_cids = nullptr;
          slot.installed_ = true;
        }
      }
      free_runtime_object(*allocator_, new_cids);
    } else if (ObIVFMergeStep::REFINE_UPDATE == step) {
      share::ObVectorMatrix *updated_center = nullptr;
      const int64_t cid = work_item.local_center_idx_;
      ObIVFHierarchyVIDList *member_list =
          runtime_->refined_member_lists_->lists_.at(cid);
      if (OB_FAIL(alloc_runtime_array(*allocator_, updated_center))) {
        LOG_WARN("failed to allocate refined IVF center", K(ret),
            K(task_idx), K(cid));
      } else if (OB_FAIL(updated_center->reserve(runtime_->dimension_))) {
        LOG_WARN("failed to reserve refined IVF center", K(ret),
            K(task_idx), K(cid), "dimension", runtime_->dimension_);
      } else if (OB_FAIL(algorithm_->refine_update_center(
          reader,
          member_list->vids_,
          *runtime_->working_centers_,
          hierarchy_k,
          cid,
          *updated_center))) {
        LOG_WARN("failed to update refined IVF center", K(ret),
            K(task_idx), K(cid));
      } else if (updated_center->count() != runtime_->dimension_
          || !are_finite_center_values(*updated_center)) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            "updated_center_count", (nullptr == updated_center) ? -1 : static_cast<int64_t>(updated_center->count()),
            "runtime_dimension", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_ERR_UNEXPECTED;
      } else if (is_runtime_canceled(*runtime_)) {
        LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            KP(runtime_));
        ret = OB_CANCELED;
      } else {
        slot.center_values_ = updated_center;
        slot.installed_ = true;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
        free_runtime_object(*allocator_, updated_center);
      }
    } else if (ObIVFMergeStep::REFINE_COMPACT == step) {
      slot.installed_ = true;
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step && uses_loaded_insertion(*runtime_, *physical_output_)) {
      ret = work_item.vid_begin_ >= 0
          ? execute_loaded_insertion_source(*runtime_, *algorithm_, work_item, slot, *physical_output_)
          : execute_loaded_insertion_copy(*runtime_, work_item, slot, *physical_output_);
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step
        && runtime_->insertion_prior_ != nullptr && work_item.cid_begin_ >= 0) {
      ret = execute_inherited_postings(*runtime_, *algorithm_, reader, work_item, slot, *allocator_);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to execute inherited IVF postings", K(ret), K(task_idx), K(work_item));
      }
    } else if (ObIVFMergeStep::SOAR_ASSIGN == step) {
      const share::ObVectorArray<int64_t> *old_assignments
          = runtime_->refine_skipped_ ? runtime_->a0_ : runtime_->refined_assignments_;
      const int64_t final_k = runtime_->final_member_lists_->lists_.count();
      const int64_t work_item_count =
          work_item.vid_end_ - work_item.vid_begin_;
      ObArray<int64_t, ObWrapperAllocator> *primary_cids = nullptr;
      ObArray<int64_t, ObWrapperAllocator> *secondary_cids = nullptr;
      ObIVFNeighborGraphView graph_view;
      const ObIVFNeighborGraphView *graph = nullptr;
      if (OB_NOT_NULL(runtime_->g1_offsets_)
          && OB_NOT_NULL(runtime_->g1_cids_)
          && OB_NOT_NULL(runtime_->g1_max_distances_)) {
        graph_view = ObIVFNeighborGraphView(
            *runtime_->g1_offsets_, *runtime_->g1_cids_,
            *runtime_->g1_max_distances_, final_k);
        if (!graph_view.is_valid()) {
          LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              K(graph_view));
          ret = OB_STATE_NOT_MATCH;
        } else {
          graph = &graph_view;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
      } else if (OB_ISNULL(old_assignments)
          || old_assignments->count() != runtime_->all_vids_.count()) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            KP(old_assignments), "old_assignments_count",
            (nullptr == old_assignments) ? -1 : static_cast<int64_t>(old_assignments->count()),
            "runtime_all_vids_count", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, primary_cids))) {
        LOG_WARN("failed to allocate primary IVF SOAR CIDs", K(ret),
            K(task_idx), K(work_item));
      } else if (OB_FAIL(primary_cids->reserve(work_item_count))) {
        LOG_WARN("failed to reserve primary IVF SOAR CIDs", K(ret),
            K(task_idx), K(work_item));
      }
      for (int64_t idx = work_item.vid_begin_;
           OB_SUCC(ret) && idx < work_item.vid_end_;
           ++idx) {
        const int64_t vector_idx = runtime_->insertion_prior_ != nullptr
            ? runtime_->insertion_delta_idxs_.at(idx) : idx;
        const int64_t old_cid = old_assignments->at(vector_idx);
        if (old_cid < 0 || old_cid >= runtime_->old_to_new_->count()
            || runtime_->old_to_new_->at(old_cid) < 0) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              K(old_cid), "runtime_old_to_new_count",
              (nullptr == runtime_ || nullptr == runtime_->old_to_new_) ? -1 : static_cast<int64_t>(runtime_->old_to_new_->count()));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(primary_cids->push_back(
            runtime_->old_to_new_->at(old_cid)))) {
          LOG_WARN("failed to append primary IVF SOAR CID", K(ret),
              K(task_idx), K(idx), K(old_cid));
        }
      }
      if (OB_SUCC(ret) && runtime_->enable_soar_ && final_k > 1) {
        if (OB_FAIL(alloc_runtime_array(*allocator_, secondary_cids))) {
          LOG_WARN("failed to allocate secondary IVF SOAR CIDs", K(ret),
              K(task_idx), K(work_item));
        } else if (OB_FAIL(secondary_cids->reserve(work_item_count))) {
          LOG_WARN("failed to reserve secondary IVF SOAR CIDs", K(ret),
              K(task_idx), K(work_item));
        } else if (OB_FAIL(algorithm_->soar_assign_secondary(
            reader,
            runtime_->insertion_prior_ != nullptr
                ? static_cast<const ObIVFVectorIDView &>(runtime_->insertion_delta_vids_)
                : static_cast<const ObIVFVectorIDView &>(runtime_->all_vids_),
            *runtime_->c1_,
            *primary_cids,
            graph,
            final_k,
            work_item.vid_begin_,
            work_item.vid_end_,
            *secondary_cids))) {
          LOG_WARN("failed to assign secondary IVF SOAR CIDs", K(ret),
              K(task_idx), K(work_item));
        } else if (secondary_cids->count() != primary_cids->count()) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              "secondary_cids_count", (nullptr == secondary_cids) ? -1 : static_cast<int64_t>(secondary_cids->count()),
              "primary_cids_count", (nullptr == primary_cids) ? -1 : static_cast<int64_t>(primary_cids->count()));
          ret = OB_ERR_UNEXPECTED;
        }
        for (int64_t i = 0;
             OB_SUCC(ret) && i < secondary_cids->count();
             ++i) {
          const int64_t secondary_cid = secondary_cids->at(i);
          const int64_t primary_cid = primary_cids->at(i);
          if (secondary_cid < -1 || secondary_cid >= final_k
              || secondary_cid == primary_cid) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
                K(secondary_cid), K(final_k), K(primary_cid));
            ret = OB_ERR_UNEXPECTED;
          } else if (secondary_cid >= 0 && OB_NOT_NULL(graph)
              && !contains_cid(
                  *graph->cids_,
                  graph->offsets_->at(primary_cid),
                  graph->offsets_->at(primary_cid + 1),
                  secondary_cid)) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
                K(secondary_cid), KP(graph), "graph_cids",
                static_cast<const void *>((nullptr == graph) ? nullptr : graph->cids_), "graph_offsets_count",
                (nullptr == graph || nullptr == graph->offsets_) ? -1 : static_cast<int64_t>(graph->offsets_->count()),
                K(primary_cid));
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_runtime_canceled(*runtime_)) {
          LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
              "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
              KP(runtime_));
          ret = OB_CANCELED;
        } else {
          slot.center_idxs_ = primary_cids;
          slot.secondary_center_idxs_ = secondary_cids;
          primary_cids = nullptr;
          secondary_cids = nullptr;
          slot.installed_ = true;
        }
      }
      free_runtime_object(*allocator_, primary_cids);
      free_runtime_object(*allocator_, secondary_cids);
    } else if (ObIVFMergeStep::QUANT == step && (runtime_->effective_type_ == ObIVFProcessType::CONCATENATION
        || uses_loaded_insertion(*runtime_, *physical_output_))) {
      if (uses_loaded_insertion(*runtime_, *physical_output_)
          && OB_SUCCESS != (ret = runtime_->ddl_runtime_->posting_error())) {
      } else if (work_item.local_center_idx_ >= 0) {
        ret = physical_output_->write_posting_range(work_item.local_center_idx_);
      } else {
        ret = physical_output_->write_loaded_centers(*runtime_->c1_, runtime_->dimension_,
            work_item.cid_begin_, work_item.cid_end_);
      }
      if (OB_SUCC(ret) && is_runtime_canceled(*runtime_)) { ret = OB_CANCELED; }
      if (OB_SUCC(ret)) { slot.installed_ = true; }
    } else if (ObIVFMergeStep::QUANT == step) {
      const int64_t final_k = runtime_->final_member_lists_->lists_.count();
      ObIVFMergeQuantizer *range_output = nullptr;
      if (work_item.cid_begin_ < 0 || work_item.cid_end_ <= work_item.cid_begin_
          || work_item.cid_end_ > final_k) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid IVF quantization task range", K(ret), K(task_idx), K(work_item), K(final_k));
      } else if (OB_FAIL(physical_output_->open_quantization_range(
          work_item.cid_begin_, work_item.cid_end_, task_idx, *allocator_, range_output))) {
        LOG_WARN("failed to open IVF quantization range", K(ret), K(task_idx));
      } else if (range_output == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null IVF quantization range writer", K(ret), K(task_idx));
      }
      for (int64_t cid = work_item.cid_begin_; OB_SUCC(ret) && cid < work_item.cid_end_; ++cid) {
        const auto *primary = runtime_->final_member_lists_->lists_.at(cid);
        auto *secondary = runtime_->soar_member_lists_ == nullptr ? nullptr
            : runtime_->soar_member_lists_->lists_.at(cid);
        const ObIVFVectorIDView secondary_vids = secondary == nullptr ? ObIVFVectorIDView() : secondary->vids_;
        if (is_runtime_canceled(*runtime_)) {
          ret = OB_CANCELED;
          LOG_WARN("IVF quantization task canceled", K(ret), K(cid));
        } else if (primary == nullptr || !primary->sealed_
            || (secondary != nullptr && !secondary->sealed_)) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("IVF quantization member lists are missing or unsealed", K(ret), K(cid), K(task_idx));
        } else if (runtime_->insertion_prior_ != nullptr
            && runtime_->insertion_reuse_cids_.at(cid) >= 0) {
          const int64_t source_cid = runtime_->insertion_reuse_cids_.at(cid);
          ret = range_output->quantize_cluster_with_reuse(reader, *runtime_->c1_, final_k, cid,
              primary->vids_, secondary == nullptr ? nullptr : &secondary_vids,
              *runtime_->insertion_prior_->posting_source_, source_cid,
              runtime_->insertion_prior_->posting_mass_.at(source_cid));
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to quantize IVF cluster with reused postings", K(ret), K(cid), K(source_cid),
                K(task_idx));
          }
        } else {
          ret = range_output->quantize_cluster(reader, *runtime_->c1_, final_k, cid,
              primary->vids_, secondary == nullptr ? nullptr : &secondary_vids);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to quantize IVF cluster", K(ret), K(cid), K(task_idx));
          }
        }
        // Copy-only clusters may not enter an encoder batch. Keep those long
        // ranges cooperative as well, without releasing the active writer.
        if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
          LOG_WARN("IVF quantization task yield failed", K(ret), K(cid), K(task_idx));
        }
      }
      if (OB_SUCC(ret) && is_runtime_canceled(*runtime_)) {
        ret = OB_CANCELED;
        LOG_WARN("IVF quantization range canceled", K(ret));
      }
      if (range_output != nullptr) {
        const int close_ret = physical_output_->close_quantization_range(range_output, OB_SUCC(ret));
        if (OB_SUCCESS != close_ret) {
          LOG_WARN("failed to close IVF quantization task range", K(close_ret), K(ret), K(task_idx));
        }
        if (OB_SUCC(ret)) { ret = close_ret; }
      }
      if (OB_SUCC(ret)) { slot.installed_ = true; }
    } else if (ObIVFMergeStep::RAW_WRITE == step) {
      if (!runtime_->quant_finished_ || runtime_->merge_finished_
          || OB_FAIL(physical_output_->write_raw_vectors(
              reader,
              runtime_->all_vids_,
              work_item.vid_begin_,
              work_item.vid_end_,
              task_idx))) {
        if (OB_SUCCESS == ret) {
          ret = OB_STATE_NOT_MATCH;
        }
        LOG_WARN("fail to write IVF merge RAW vector range", K(ret),
            K(work_item), K(task_idx),
            "quant_finished", runtime_->quant_finished_,
            "merge_finished", runtime_->merge_finished_);
      } else if (is_runtime_canceled(*runtime_)) {
        LOG_INFO("IVF merge task canceled", "ret", OB_CANCELED, "operation",
            "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step),
            KP(runtime_));
        ret = OB_CANCELED;
      } else {
        slot.installed_ = true;
      }
    } else {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
          "ObIVFMergeProcessor::execute_post_hierarchy_task", K(plan_id), K(task_idx), K(stage), K(step));
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_FAIL(ret) && (step == ObIVFMergeStep::SOAR_ASSIGN || step == ObIVFMergeStep::QUANT)
      && uses_loaded_insertion(*runtime_, *physical_output_)) {
    runtime_->ddl_runtime_->fail_postings(ret);
  }
  return ret;
}

int ObIVFMergeProcessor::finish_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    bool &hierarchy_finished)
{
  int ret = OB_SUCCESS;
  hierarchy_finished = false;
  if (!is_inited()) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation", "ObIVFMergeProcessor::finish_step",
        K(stage), K(step), K(depth));
    ret = OB_NOT_INIT;
  } else if (!is_valid_ivf_merge_operation(stage, step, depth)) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "ObIVFMergeProcessor::finish_step",
        K(stage), K(step), K(depth));
    ret = OB_INVALID_ARGUMENT;
  } else {
    LOG_WARN("unsupported IVF merge operation", "ret", OB_NOT_SUPPORTED, "operation",
        "ObIVFMergeProcessor::finish_step", K(stage), K(step), K(depth));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObIVFMergeProcessor::finish_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t plan_id,
    ObIVFMergeStepResult &step_result)
{
  int ret = OB_SUCCESS;
  const int64_t started_at = runtime_ != nullptr && runtime_->active_plan_ != nullptr
      ? runtime_->active_plan_->started_at_us_ : 0;
  const int64_t task_count = runtime_ != nullptr && runtime_->active_plan_ != nullptr
      ? runtime_->active_plan_->work_items_.count() : 0;
  if (ObIVFMergeStage::CENTROID_INDEX == stage && is_runtime_enabled()) {
    ret = finish_centroid_index_step(step, depth, plan_id, step_result);
  } else if (ObIVFMergeStage::PRIOR == stage && is_runtime_enabled()) {
    ret = finish_prior_step(plan_id, step_result);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "finish_prior_step", K(plan_id), K(stage), K(step),
          K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY == stage
      && is_runtime_enabled()) {
    ret = finish_hierarchy_step(stage, step, depth, plan_id, step_result);
    if (OB_SUCC(ret) && depth == 0 && step == ObIVFMergeStep::HIERARCHY_ASSIGN
        && (runtime_->effective_type_ == ObIVFProcessType::INSERTION)) {
      algorithm_->finish_insertion_assign();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "finish_hierarchy_step", K(plan_id), K(stage),
          K(step), K(depth));
    }
  } else if (ObIVFMergeStage::HIERARCHY != stage
      && is_runtime_enabled()) {
    ret = finish_post_hierarchy_step(
        stage, step, plan_id, step_result);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "finish_post_hierarchy_step", K(plan_id), K(stage),
          K(step), K(depth));
    }
  } else {
    bool hierarchy_finished = false;
    if (OB_FAIL(finish_step(stage, step, depth, hierarchy_finished))) {
      LOG_WARN("failed to finish delegated IVF merge step", K(ret),
          K(stage), K(step), K(depth), K(plan_id));
    } else {
      step_result.plan_id_ = plan_id;
      step_result.stage_ = stage;
      step_result.step_ = step;
      step_result.depth_ = depth;
      step_result.committed_ = true;
      step_result.hierarchy_finished_ = hierarchy_finished
          || ObIVFMergeStage::HIERARCHY != stage;
      step_result.k_ = step_result.hierarchy_finished_ ? 129 : 0;
      step_result.neighborhood_mode_ = step_result.hierarchy_finished_
          ? ObIVFNeighborhoodMode::HGRAPH
          : ObIVFNeighborhoodMode::INVALID;
      step_result.refine_skipped_ = false;
      step_result.refine_has_changes_ =
          ObIVFMergeStep::REFINE_ASSIGN == step;
      step_result.merge_finished_ =
          ObIVFMergeStep::RAW_WRITE == step;
    }
  }
  if (OB_SUCC(ret) && runtime_ != nullptr && started_at > 0
      && stage > ObIVFMergeStage::INVALID && stage < ObIVFMergeStage::MAX) {
    const int64_t elapsed_us = ObTimeUtility::current_time() - started_at;
    runtime_->stage_elapsed_us_[static_cast<int>(stage)] += elapsed_us;
    LOG_INFO("IVF merge step completed", K(plan_id), K(stage), K(step), K(depth), K(task_count), K(elapsed_us),
        "runtime_allocator_used", allocator_->used());
  }
  return ret;
}

int ObIVFMergeProcessor::finish_hierarchy_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t depth,
    const int64_t plan_id,
    ObIVFMergeStepResult &step_result)
{
  int ret = OB_SUCCESS;
  ObIVFMergeActivePlan *plan = runtime_->active_plan_;
  if (is_runtime_canceled(*runtime_) || OB_ISNULL(plan)
      || plan->plan_id_ != plan_id || plan->stage_ != stage
      || plan->step_ != step || plan->depth_ != depth) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), KP(runtime_), KP(plan),
        "plan_plan_id", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->plan_id_), "plan_stage",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->stage_), "plan_step",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->step_));
    ret = OB_STATE_NOT_MATCH;
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < plan->result_slots_.count();
       ++i) {
    if (!plan->result_slots_.at(i).installed_) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
          "plan_result_slots_count", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->result_slots_.count()), K(i));
      ret = OB_STATE_NOT_MATCH;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE == step) {
    if (nullptr != runtime_->cluster_sample_plan_) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      // The completed gather plan owns the immutable shards until training
      // drains. Publish only after all worker slots have succeeded.
      plan->ddl_->cluster_requests_.reset();
      runtime_->cluster_sample_plan_ = plan;
      runtime_->active_plan_ = nullptr;
    }
  } else if (ObIVFMergeStep::HIERARCHY_CLUSTER == step || ObIVFMergeStep::HIERARCHY_WARM_START == step) {
    for (int64_t i = 0; OB_SUCC(ret) && i < plan->work_items_.count(); ++i) {
      ObIVFHierarchyNodeState *node =
          runtime_->current_nodes_.at(plan->work_items_.at(i).node_idx_);
      ObIVFMergeResultSlot &slot = plan->result_slots_.at(i);
      if (OB_FAIL(node->local_centers_.reserve(slot.center_values_->count()))) {
        LOG_WARN("failed to reserve IVF hierarchy node centers", K(ret),
            K(depth), K(plan_id), K(i),
            "center_value_count", slot.center_values_->count());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < plan->work_items_.count(); ++i) {
      ObIVFHierarchyNodeState *node =
          runtime_->current_nodes_.at(plan->work_items_.at(i).node_idx_);
      ObIVFMergeResultSlot &slot = plan->result_slots_.at(i);
      node->local_k_ = slot.local_k_;
      node->local_centers_.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < slot.center_values_->count(); ++j) {
        ret = node->local_centers_.push_back(slot.center_values_->at(j));
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "node->local_centers_.push_back", K(plan_id),
              K(stage), K(step), K(depth), K(j), "slot_center_values_count",
              (nullptr == slot.center_values_) ? -1 : static_cast<int64_t>(slot.center_values_->count()));
        }
      }
    }
  } else if (ObIVFMergeStep::HIERARCHY_ASSIGN == step) {
    ObArenaAllocator count_allocator(
        "IVFHierCounts", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObArray<ObIVFHierarchyMemberLists *, common::ObWrapperAllocator> staging(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));
    common::ObArray<int64_t, common::ObWrapperAllocator> node_cid_offsets(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(count_allocator));
    common::ObArray<int64_t, common::ObWrapperAllocator> member_counts(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(count_allocator));

    int64_t total_local_k = 0;
    if (OB_FAIL(staging.reserve(runtime_->current_nodes_.count()))) {
      LOG_WARN("failed to reserve staged IVF hierarchy member lists", K(ret),
          K(depth), K(plan_id),
          "node_count", runtime_->current_nodes_.count());
    } else if (OB_FAIL(node_cid_offsets.reserve(
        runtime_->current_nodes_.count() + 1))) {
      LOG_WARN("failed to reserve IVF hierarchy member offsets", K(ret),
          K(depth), K(plan_id),
          "node_count", runtime_->current_nodes_.count());
    } else if (OB_FAIL(node_cid_offsets.push_back(0))) {
      LOG_WARN("failed to initialize IVF hierarchy member offsets", K(ret),
          K(depth), K(plan_id));
    }
    for (int64_t node_idx = 0;
         OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count();
         ++node_idx) {
      ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(node_idx);
      ObIVFHierarchyMemberLists *member_lists = nullptr;
      if (OB_ISNULL(node) || OB_ISNULL(node->vid_list_)
          || node->local_k_ <= 0
          || INT64_MAX - total_local_k < node->local_k_) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(alloc_runtime_object(
          *allocator_, member_lists, *allocator_))) {
        LOG_WARN("failed to allocate IVF hierarchy member lists", K(ret),
            K(depth), K(plan_id), K(node_idx));
      } else if (OB_FAIL(member_lists->lists_.reserve(node->local_k_))) {
        LOG_WARN("failed to reserve IVF hierarchy member-list slots", K(ret),
            K(depth), K(plan_id), K(node_idx), K(node->local_k_));
      }
      for (int64_t local_idx = 0;
           OB_SUCC(ret) && local_idx < node->local_k_;
           ++local_idx) {
        ObIVFHierarchyVIDList *vid_list = nullptr;
        if (OB_FAIL(alloc_runtime_object(*allocator_, vid_list, *allocator_,
            runtime_->vector_store_->uses_position_ids()))) {
          LOG_WARN("failed to allocate IVF hierarchy member VID list", K(ret),
              K(depth), K(plan_id), K(node_idx), K(local_idx));
        } else if (OB_FAIL(member_lists->lists_.push_back(vid_list))) {
          LOG_WARN("failed to append IVF hierarchy member VID list", K(ret),
              K(depth), K(plan_id), K(node_idx), K(local_idx));
          release_vid_list(*allocator_, vid_list);
        }
      }
      if (OB_SUCC(ret)) {
        total_local_k += node->local_k_;
        if (OB_FAIL(node_cid_offsets.push_back(total_local_k))) {
          LOG_WARN("failed to append IVF hierarchy member offset", K(ret),
              K(depth), K(plan_id), K(node_idx), K(total_local_k));
        } else if (OB_FAIL(staging.push_back(member_lists))) {
          LOG_WARN("failed to stage IVF hierarchy member lists", K(ret),
              K(depth), K(plan_id), K(node_idx));
        } else {
          member_lists = nullptr;
        }
      }
      if (OB_NOT_NULL(member_lists)) {
        release_member_lists(*allocator_, member_lists, false);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(member_counts.prepare_allocate(total_local_k))) {
      LOG_WARN("failed to allocate IVF hierarchy member counts", K(ret),
          K(depth), K(plan_id), K(total_local_k));
    } else {
      for (int64_t i = 0; i < member_counts.count(); ++i) {
        member_counts.at(i) = 0;
      }
    }
    if (OB_SUCC(ret) && plan->source_assign_) {
      ret = visit_source_assignments(*runtime_, *plan,
          [&](int64_t n, int64_t cid, ObIVFVectorVID vid, int64_t idx) {
            UNUSEDx(vid, idx);
            ++member_counts.at(node_cid_offsets.at(n) + cid);
            return OB_SUCCESS;
          });
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && !plan->source_assign_ && task_idx < plan->work_items_.count();
         ++task_idx) {
      const ObIVFMergeWorkItem &work_item = plan->work_items_.at(task_idx);
      const ObArray<int64_t, ObWrapperAllocator> *center_idxs =
          plan->result_slots_.at(task_idx).center_idxs_;
      if (work_item.node_idx_ < 0
          || work_item.node_idx_ >= staging.count()
          || OB_ISNULL(center_idxs)
          || work_item.vid_begin_ < 0
          || work_item.vid_end_ > get_vector_ref_count(
              *runtime_->current_nodes_.at(work_item.node_idx_)->vid_list_,
              runtime_->all_vids_)
          || center_idxs->count()
              != work_item.vid_end_ - work_item.vid_begin_) {
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && i < center_idxs->count();
           ++i) {
        const int64_t cid = center_idxs->at(i);
        const ObIVFHierarchyNodeState *node =
            runtime_->current_nodes_.at(work_item.node_idx_);
        if (cid < 0 || cid >= node->local_k_) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int64_t count_idx =
              node_cid_offsets.at(work_item.node_idx_) + cid;
          if (count_idx < 0 || count_idx >= member_counts.count()
              || INT64_MAX == member_counts.at(count_idx)) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            ++member_counts.at(count_idx);
          }
        }
      }
    }
    for (int64_t node_idx = 0;
         OB_SUCC(ret) && node_idx < staging.count();
         ++node_idx) {
      const ObIVFHierarchyNodeState *node =
          runtime_->current_nodes_.at(node_idx);
      for (int64_t cid = 0; OB_SUCC(ret) && cid < node->local_k_; ++cid) {
        if (OB_FAIL(reserve_vector_refs(
            *staging.at(node_idx)->lists_.at(cid),
            member_counts.at(node_cid_offsets.at(node_idx) + cid)))) {
          LOG_WARN("failed to reserve IVF hierarchy member list", K(ret),
              K(depth), K(plan_id), K(node_idx), K(cid));
        }
      }
    }
    if (OB_SUCC(ret) && plan->source_assign_) {
      ret = visit_source_assignments(*runtime_, *plan,
          [&](int64_t n, int64_t cid, ObIVFVectorVID vid, int64_t idx) {
            return append_vector_ref(*staging.at(n)->lists_.at(cid), vid, idx);
          });
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && !plan->source_assign_ && task_idx < plan->work_items_.count();
         ++task_idx) {
      const ObIVFMergeWorkItem &work_item = plan->work_items_.at(task_idx);
      const common::ObIArray<int64_t> &center_idxs =
          *plan->result_slots_.at(task_idx).center_idxs_;
      const ObIVFHierarchyNodeState *node =
          runtime_->current_nodes_.at(work_item.node_idx_);
      for (int64_t i = 0;
           OB_SUCC(ret) && i < center_idxs.count();
           ++i) {
        ObIVFVectorVID vid = 0;
        int64_t vector_idx = -1;
        const int64_t source_offset = work_item.vid_begin_ + i;
        const int64_t cid = center_idxs.at(i);
        if (OB_FAIL(get_vector_ref(
            *node->vid_list_, runtime_->all_vids_, source_offset,
            vid, vector_idx))) {
          LOG_WARN("failed to resolve IVF hierarchy vector reference", K(ret),
              K(depth), K(plan_id), K(task_idx), K(source_offset));
        } else if (OB_FAIL(append_vector_ref(
            *staging.at(work_item.node_idx_)->lists_.at(cid),
            vid, vector_idx))) {
          LOG_WARN("failed to append IVF hierarchy member", K(ret),
              K(depth), K(plan_id), K(task_idx), K(cid), K(source_offset));
        }
      }
    }
    for (int64_t node_idx = 0;
         OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count();
         ++node_idx) {
      int64_t total_member_count = 0;
      ObIVFHierarchyMemberLists *member_lists = staging.at(node_idx);
      for (int64_t i = 0; i < member_lists->lists_.count(); ++i) {
        if (member_lists->lists_.at(i)->vector_idxs_.count()
            != member_lists->lists_.at(i)->vids_.count()) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
              "member_lists_lists_count",
              (nullptr == member_lists) ? -1 : static_cast<int64_t>(member_lists->lists_.count()), K(i));
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        member_lists->lists_.at(i)->sealed_ = true;
        total_member_count += member_lists->lists_.at(i)->vids_.count();
      }
      if (total_member_count != get_vector_ref_count(
          *runtime_->current_nodes_.at(node_idx)->vid_list_,
          runtime_->all_vids_)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t node_idx = 0; node_idx < runtime_->current_nodes_.count(); ++node_idx) {
        runtime_->current_nodes_.at(node_idx)->member_lists_ = staging.at(node_idx);
      }
    } else {
      for (int64_t node_idx = 0; node_idx < staging.count(); ++node_idx) {
        release_member_lists(*allocator_, staging.at(node_idx), false);
      }
    }
  } else if (ObIVFMergeStep::HIERARCHY_UPDATE == step) {
    if (nullptr != plan->source_update_) {
      ret = finish_source_update(*runtime_, *algorithm_, *plan, *allocator_);
    }
    const auto &update_items = nullptr == plan->source_update_
        ? plan->work_items_ : plan->source_update_->items_;
    auto &update_results = nullptr == plan->source_update_
        ? plan->result_slots_ : plan->source_update_->results_;
    common::ObArray<ObIVFHierarchyPendingLeaf, common::ObWrapperAllocator> pending_leaves(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));
    common::ObArray<ObIVFHierarchyNodeState *, common::ObWrapperAllocator> pending_next_nodes(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));
    common::ObArray<ObIVFHierarchyNodeState *, common::ObWrapperAllocator> old_nodes(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));

    const int64_t old_center_value_count = runtime_->c0_.count();
    const int64_t old_member_list_count = runtime_->initial_member_lists_.count();
    int64_t no_progress_leaf_delta = 0;
    int64_t no_progress_member_delta = 0;
    int64_t update_task_idx = 0;
    bool next_nodes_committed = false;
    share::ObVectorArray<int64_t> *pending_a0 = nullptr;
    for (int64_t node_idx = 0;
         OB_SUCC(ret) && node_idx < runtime_->current_nodes_.count();
         ++node_idx) {
      ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(node_idx);
      int64_t non_empty_count = 0;
      int64_t only_non_empty_idx = -1;
      int64_t total_member_count = 0;
      for (int64_t local_idx = 0; local_idx < node->local_k_; ++local_idx) {
        const int64_t count =
            node->member_lists_->lists_.at(local_idx)->vids_.count();
        total_member_count += count;
        if (count > 0) {
          ++non_empty_count;
          only_non_empty_idx = local_idx;
        }
      }
      const int64_t parent_member_count = get_vector_ref_count(
          *node->vid_list_, runtime_->all_vids_);
      if (!is_valid_vector_ref_list(*node->vid_list_, runtime_->all_vids_)
          || total_member_count != parent_member_count
          || non_empty_count <= 0) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
            "node_vid_list_vector_idxs_count",
            (nullptr == node || nullptr == node->vid_list_) ? -1 : static_cast<int64_t>(node->vid_list_->vector_idxs_.count()),
            "node_vid_list_vids_count",
            (nullptr == node || nullptr == node->vid_list_) ? -1 : static_cast<int64_t>(node->vid_list_->vids_.count()),
            K(total_member_count), K(non_empty_count));
        ret = OB_ERR_UNEXPECTED;
      }
      const bool no_progress = OB_SUCC(ret)
          && parent_member_count > runtime_->cluster_size_
          && 1 == non_empty_count
          && node->member_lists_->lists_.at(only_non_empty_idx)->vids_.count()
              == parent_member_count;
      if (no_progress) {
        LOG_TRACE("IVF hierarchy split made no progress",
            K(plan_id), K(depth), K(node_idx),
            K(parent_member_count),
            "only_child_member_count",
                node->member_lists_->lists_.at(only_non_empty_idx)->vids_.count(),
            "cluster_size", runtime_->cluster_size_,
            "local_k", node->local_k_,
            "empty_cid_count", node->local_k_ - non_empty_count);
      }
      for (int64_t local_idx = 0;
           OB_SUCC(ret) && local_idx < node->local_k_;
           ++local_idx) {
        ObIVFHierarchyVIDList *member_list =
            node->member_lists_->lists_.at(local_idx);
        if (!member_list->vids_.empty() || (runtime_->insertion_prior_ != nullptr && depth == 0)) {
          if (update_task_idx >= update_items.count()
              || update_items.at(update_task_idx).node_idx_ != node_idx
              || update_items.at(update_task_idx).local_center_idx_ != local_idx) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
                K(update_task_idx), "plan_work_items_count",
                (nullptr == plan) ? -1 : static_cast<int64_t>(update_items.count()), K(node_idx), K(local_idx));
            ret = OB_ERR_UNEXPECTED;
          } else {
            share::ObVectorMatrix *updated_center =
                update_results.at(update_task_idx).center_values_;
            if (runtime_->insertion_prior_ != nullptr || member_list->vids_.count() <= runtime_->cluster_size_
                || no_progress) {
              if (OB_ISNULL(updated_center)
                  || updated_center->count() != runtime_->dimension_
                  || !are_finite_center_values(*updated_center)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid IVF hierarchy leaf center", K(ret), K(plan_id),
                    K(depth), K(node_idx), K(local_idx), KP(updated_center));
              } else {
                ObIVFHierarchyPendingLeaf leaf;
                leaf.center_values_ = updated_center;
                leaf.vid_list_ = member_list;
                leaf.origin_cid_ = depth == 0 ? local_idx : node->origin_cid_;
                if (OB_FAIL(pending_leaves.push_back(leaf))) {
                  LOG_WARN("failed to append IVF hierarchy leaf", K(ret), K(plan_id),
                      K(depth), K(node_idx), K(local_idx), K(no_progress));
                }
              }
              if (OB_SUCC(ret) && no_progress) {
                ++no_progress_leaf_delta;
                no_progress_member_delta += member_list->vids_.count();
              }
            } else {
              ObIVFHierarchyNodeState *next_node = nullptr;
              if (OB_FAIL(alloc_runtime_object(
                  *allocator_, next_node, *allocator_, runtime_->dimension_))) {
                LOG_WARN("failed to allocate next IVF hierarchy node", K(ret),
                    K(depth), K(plan_id), K(node_idx), K(local_idx));
              } else {
                next_node->vid_list_ = member_list;
                next_node->origin_cid_ = depth == 0 ? local_idx : node->origin_cid_;
                if (OB_FAIL(pending_next_nodes.push_back(next_node))) {
                  LOG_WARN("failed to append next IVF hierarchy node", K(ret),
                      K(depth), K(plan_id), K(node_idx), K(local_idx));
                  next_node->vid_list_ = nullptr;
                  release_node_state(*allocator_, next_node, false);
                }
              }
            }
            ++update_task_idx;
          }
        }
      }
    }
    if (OB_SUCC(ret) && update_task_idx != update_items.count()) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), K(update_task_idx),
          "plan_work_items_count", (nullptr == plan) ? -1 : static_cast<int64_t>(update_items.count()));
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      if (runtime_->dimension_ <= 0
          || old_member_list_count > INT64_MAX / runtime_->dimension_
          || old_center_value_count
              != old_member_list_count * runtime_->dimension_
          || pending_leaves.count() > INT64_MAX - old_member_list_count) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), "runtime_dimension",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_), K(old_member_list_count),
            K(old_center_value_count), "pending_leaves_count", pending_leaves.count());
        ret = OB_ERR_UNEXPECTED;
      }
    }
    const int64_t required_center_count = OB_SUCC(ret)
        ? old_member_list_count + pending_leaves.count() : 0;
    if (OB_SUCC(ret)) {
      if (required_center_count > INT64_MAX / runtime_->dimension_) {
        LOG_WARN("IVF merge size overflow", "ret", OB_SIZE_OVERFLOW, "operation",
            "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
            K(required_center_count), "runtime_dimension",
            (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(old_nodes.reserve(runtime_->current_nodes_.count()))) {
        LOG_WARN("failed to reserve old IVF hierarchy nodes", K(ret),
            K(depth), K(plan_id),
            "node_count", runtime_->current_nodes_.count());
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && i < runtime_->current_nodes_.count();
           ++i) {
        ret = old_nodes.push_back(runtime_->current_nodes_.at(i));
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "old_nodes.push_back", K(plan_id), K(stage),
              K(step), K(depth), K(i), "runtime_current_nodes_count",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->current_nodes_.count()));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
      } else if (OB_FAIL(runtime_->c0_.reserve(
          required_center_count * runtime_->dimension_))) {
        LOG_WARN("failed to reserve IVF hierarchy centers", K(ret),
            K(depth), K(plan_id), K(required_center_count),
            "dimension", runtime_->dimension_);
      } else if (OB_FAIL(runtime_->initial_member_lists_.reserve(
          required_center_count))) {
        LOG_WARN("failed to reserve IVF initial member lists", K(ret),
            K(depth), K(plan_id), K(required_center_count));
      }
    }
    if (OB_SUCC(ret) && pending_next_nodes.empty()) {
      if (OB_NOT_NULL(runtime_->a0_)) {
        LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
            "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), "runtime_a0",
            static_cast<const void *>((nullptr == runtime_) ? nullptr : runtime_->a0_));
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, pending_a0))) {
        LOG_WARN("failed to allocate initial IVF assignments", K(ret),
            K(depth), K(plan_id));
      } else if (OB_FAIL(append_filled_int_array(
          runtime_->all_vids_.count(), -1, *pending_a0))) {
        LOG_WARN("failed to initialize IVF assignments", K(ret),
            K(depth), K(plan_id),
            "vector_count", runtime_->all_vids_.count());
      }
      for (int64_t cid = 0;
           OB_SUCC(ret) && cid < runtime_->initial_member_lists_.count();
           ++cid) {
        ObIVFHierarchyVIDList *member_list =
            runtime_->initial_member_lists_.at(cid);
        const ObIVFVectorIDView &member_vids = member_list->vids_;
        const share::ObVectorArray<int64_t> &member_vector_idxs = member_list->vector_idxs_;
        if (member_vector_idxs.count() != member_vids.count()) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
              "member_vector_idxs_count", member_vector_idxs.count(), "member_vids_count", member_vids.count());
          ret = OB_ERR_UNEXPECTED;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < member_vids.count(); ++i) {
          const ObIVFVectorVID vid = member_vids.at(i);
          const int64_t vector_idx = member_vector_idxs.at(i);
          if (vector_idx < 0 || vector_idx >= pending_a0->count()
              || runtime_->all_vids_.at(vector_idx) != vid
              || pending_a0->at(vector_idx) >= 0) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), K(vector_idx),
                "pending_a0_count", (nullptr == pending_a0) ? -1 : static_cast<int64_t>(pending_a0->count()),
                "runtime_all_vids_count",
                (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()), K(vid));
            ret = OB_ERR_UNEXPECTED;
          } else {
            pending_a0->at(vector_idx) = cid;
          }
        }
      }
      for (int64_t pending_idx = 0;
           OB_SUCC(ret) && pending_idx < pending_leaves.count();
           ++pending_idx) {
        const int64_t cid = old_member_list_count + pending_idx;
        ObIVFHierarchyVIDList *member_list =
            pending_leaves.at(pending_idx).vid_list_;
        const ObIVFVectorIDView &member_vids = member_list->vids_;
        const share::ObVectorArray<int64_t> &member_vector_idxs = member_list->vector_idxs_;
        if (member_vector_idxs.count() != member_vids.count()) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
              "member_vector_idxs_count", member_vector_idxs.count(), "member_vids_count", member_vids.count());
          ret = OB_ERR_UNEXPECTED;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < member_vids.count(); ++i) {
          const ObIVFVectorVID vid = member_vids.at(i);
          const int64_t vector_idx = member_vector_idxs.at(i);
          if (vector_idx < 0 || vector_idx >= pending_a0->count()
              || runtime_->all_vids_.at(vector_idx) != vid
              || pending_a0->at(vector_idx) >= 0) {
            LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
                "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), K(vector_idx),
                "pending_a0_count", (nullptr == pending_a0) ? -1 : static_cast<int64_t>(pending_a0->count()),
                "runtime_all_vids_count",
                (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()), K(vid));
            ret = OB_ERR_UNEXPECTED;
          } else {
            pending_a0->at(vector_idx) = cid;
          }
        }
      }
      for (int64_t vector_idx = 0;
           OB_SUCC(ret) && vector_idx < pending_a0->count();
           ++vector_idx) {
        if (pending_a0->at(vector_idx) < 0
            || pending_a0->at(vector_idx) >= required_center_count) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth), "pending_a0_count",
              (nullptr == pending_a0) ? -1 : static_cast<int64_t>(pending_a0->count()), K(vector_idx),
              K(required_center_count));
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    if (OB_SUCC(ret) && runtime_->insertion_prior_ != nullptr
        && OB_FAIL(runtime_->insertion_origins_.reserve(required_center_count))) {
      LOG_WARN("failed to reserve insertion leaf lineage", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pending_leaves.count(); ++i) {
      if (runtime_->insertion_prior_ != nullptr) {
        ret = runtime_->insertion_origins_.push_back(pending_leaves.at(i).origin_cid_);
        if (OB_FAIL(ret)) {
        LOG_WARN("failed to retain IVF insertion leaf origin", K(ret), K(i));
      }
      }
      for (int64_t d = 0; OB_SUCC(ret) && d < runtime_->dimension_; ++d) {
        ret = runtime_->c0_.push_back(pending_leaves.at(i).center_values_->at(d));
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "runtime_->c0_.push_back", K(plan_id), K(stage),
              K(step), K(depth), K(d), "runtime_dimension",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        }
      }
      if (OB_SUCC(ret)) {
        ret = runtime_->initial_member_lists_.push_back(
            pending_leaves.at(i).vid_list_);
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "runtime_->initial_member_lists_.push_back",
              K(plan_id), K(stage), K(step), K(depth));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // assign allocates/copies before replacing the old pointer array. On OOM
      // the current owners remain intact; rollback must not allocate memory.
      if (OB_FAIL(runtime_->current_nodes_.assign(pending_next_nodes))) {
        LOG_WARN("failed to commit next IVF hierarchy nodes", K(ret), K(plan_id), K(depth));
      } else {
        next_nodes_committed = true;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < old_nodes.count(); ++i) {
        if (runtime_->insertion_prior_ != nullptr && depth == 0) {
          // Empty working-owner lists with old postings are retained leaves.
          // Detach them too; the generic release helper preserves nonempty lists.
          for (int64_t j = 0; j < old_nodes.at(i)->member_lists_->lists_.count(); ++j) {
            auto *&list = old_nodes.at(i)->member_lists_->lists_.at(j);
            if (list != nullptr && list->vids_.empty()) { list = nullptr; }
          }
        }
        release_node_state(*allocator_, old_nodes.at(i), true);
      }
      runtime_->no_progress_leaf_count_ += no_progress_leaf_delta;
      runtime_->no_progress_member_count_ += no_progress_member_delta;
      runtime_->actual_max_depth_ = MAX(runtime_->actual_max_depth_, depth + 1);
      runtime_->hierarchy_finished_ = runtime_->current_nodes_.empty();
      if (runtime_->hierarchy_finished_) {
        runtime_->a0_ = pending_a0;
        pending_a0 = nullptr;
        runtime_->neighborhood_mode_ =
            choose_neighborhood_mode(required_center_count);
        runtime_->hierarchy_k_ = required_center_count;
        runtime_->refine_skipped_ = runtime_->effective_type_ == ObIVFProcessType::INSERTION
            || 1 == required_center_count || required_center_count == runtime_->all_vids_.count();
        if (!is_valid_ivf_neighborhood_mode(
            runtime_->neighborhood_mode_)) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
              "runtime_neighborhood_mode",
              (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->neighborhood_mode_));
          ret = OB_ERR_UNEXPECTED;
        }
      }
    } else {
      while (runtime_->insertion_origins_.count() > old_member_list_count) {
        runtime_->insertion_origins_.pop_back();
      }
      while (runtime_->c0_.count() > old_center_value_count) {
        runtime_->c0_.pop_back();
      }
      while (runtime_->initial_member_lists_.count() > old_member_list_count) {
        runtime_->initial_member_lists_.pop_back();
      }
      if (!next_nodes_committed) {
        release_pending_next_nodes(*allocator_, pending_next_nodes);
      }
    }
    if (OB_NOT_NULL(pending_a0)) {
      free_runtime_object(*allocator_, pending_a0);
    }
  } else {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth));
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret) && step == ObIVFMergeStep::HIERARCHY_ASSIGN && depth == 0
      && !is_ivf_full_build_process_type(runtime_->effective_type_)
      && (runtime_->effective_type_ != ObIVFProcessType::INSERTION)) {
    if (runtime_->current_nodes_.count() != 1) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_hierarchy_step", K(plan_id), K(stage), K(step), K(depth),
          "runtime_current_nodes_count",
          (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->current_nodes_.count()));
      ret = OB_ERR_UNEXPECTED;
    }
    else {
      ObIVFHierarchyNodeState *root = runtime_->current_nodes_.at(0);

      ObArray<int64_t> counts;
      counts.set_attr(ObMemAttr(MTL_ID(), "IVFHcaQuality", ObCtxIds::VECTOR_CTX_ID));
      if (OB_FAIL(counts.reserve(root->local_k_))) {
        LOG_WARN("failed to reserve IVF cluster quality counts", K(ret), "center_count", root->local_k_);
      }
      for (int64_t cid = 0; OB_SUCC(ret) && cid < root->local_k_; ++cid) {
        ret = counts.push_back(root->member_lists_->lists_.at(cid)->vids_.count());
        if (OB_FAIL(ret)) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "counts.push_back", K(plan_id), K(stage), K(step),
              K(depth), K(cid), "root_local_k", (nullptr == root) ? -1 : static_cast<int64_t>(root->local_k_));
        }
      }
      bool degenerate = false;
      if (OB_SUCC(ret) && OB_FAIL(algorithm_->check_cluster_quality(counts, degenerate))) {
        LOG_WARN("failed to check IVF root assignment quality", K(ret));
      } else if (OB_SUCC(ret) && degenerate) {
        release_member_lists(*allocator_, root->member_lists_, false);
        root->local_centers_.reuse();
        root->local_k_ = 0;
        runtime_->effective_type_ = ObIVFProcessType::FULL_REBUILD;
        runtime_->fallback_reason_ = ObIVFQualityFallback::ASSIGNMENT_DEGENERATE;
        step_result.quality_fallback_ = true;
        LOG_INFO("IVF reuse assignment falls back to full rebuild", K(runtime_->selected_type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObIVFMergeStep::HIERARCHY_ASSIGN == step && depth == 0
        && runtime_->insertion_prior_ == nullptr) {
      // Without encoded reuse, root assignment is the last consumer. Failed
      // assignment attempts retain these arrays until a successful barrier.
      runtime_->insertion_offsets_.reset();
      runtime_->insertion_candidates_.reset();
    }
    if (ObIVFMergeStep::HIERARCHY_CLUSTER == step) {
      release_active_plan(*allocator_, runtime_->cluster_sample_plan_);
    }
    release_active_plan(*allocator_, runtime_->active_plan_);
    step_result.plan_id_ = plan_id;
    step_result.stage_ = stage;
    step_result.step_ = step;
    step_result.depth_ = depth;
    step_result.committed_ = true;
    step_result.hierarchy_finished_ = runtime_->hierarchy_finished_;
    step_result.next_refine_needs_samples_ = runtime_->ddl_runtime_ != nullptr && !runtime_->refine_skipped_;
    step_result.next_cluster_needs_samples_ = step == ObIVFMergeStep::HIERARCHY_UPDATE
        && !runtime_->hierarchy_finished_ && needs_cluster_samples(*runtime_, *algorithm_, depth + 1);
    step_result.k_ = runtime_->hierarchy_finished_
        ? runtime_->initial_member_lists_.count() : 0;
    step_result.neighborhood_mode_ = runtime_->neighborhood_mode_;
    step_result.refine_skipped_ = runtime_->refine_skipped_;
  }
  return ret;
}

static int alloc_empty_member_lists(
    ObIAllocator &allocator,
    const int64_t count,
    ObIVFHierarchyMemberLists *&member_lists,
    const bool use_position_ids = false)
{
  int ret = OB_SUCCESS;
  member_lists = nullptr;
  if (count <= 0) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "alloc_empty_member_lists",
        K(count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(alloc_runtime_object(
      allocator, member_lists, allocator))) {
    LOG_WARN("failed to allocate empty IVF member lists", K(ret), K(count));
  } else if (OB_FAIL(member_lists->lists_.reserve(count))) {
    LOG_WARN("failed to reserve empty IVF member-list slots", K(ret),
        K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObIVFHierarchyVIDList *vid_list = nullptr;
    if (OB_FAIL(alloc_runtime_object(allocator, vid_list, allocator, use_position_ids))) {
      LOG_WARN("failed to allocate empty IVF member VID list", K(ret),
          K(count), K(i));
    } else if (OB_FAIL(member_lists->lists_.push_back(vid_list))) {
      LOG_WARN("failed to append empty IVF member VID list", K(ret),
          K(count), K(i));
      release_vid_list(allocator, vid_list);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    release_member_lists(allocator, member_lists, false);
  }
  return ret;
}

static void release_initial_member_lists(
    ObIAllocator &allocator,
    ObIVFMergeRuntime &runtime)
{
  for (int64_t i = 0; i < runtime.initial_member_lists_.count(); ++i) {
    release_vid_list(allocator, runtime.initial_member_lists_.at(i));
  }
  runtime.initial_member_lists_.reset();
}

static int reserve_member_lists_from_plan(
    const ObIVFMergeActivePlan &plan,
    ObIVFHierarchyMemberLists &member_lists,
    const bool use_secondary_center_idxs)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator count_allocator(
      "IVFMemberCount", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  common::ObArray<int64_t, common::ObWrapperAllocator> member_counts(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(count_allocator));

  const int64_t center_count = member_lists.lists_.count();
  if (center_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(member_counts.prepare_allocate(center_count))) {
    LOG_WARN("failed to allocate IVF member counts", K(ret),
        K(center_count), K(use_secondary_center_idxs));
  } else {
    for (int64_t i = 0; i < member_counts.count(); ++i) {
      member_counts.at(i) = 0;
    }
  }
  for (int64_t task_idx = 0;
       OB_SUCC(ret) && task_idx < plan.work_items_.count();
       ++task_idx) {
    const ObIVFMergeWorkItem &work_item = plan.work_items_.at(task_idx);
    const ObIVFMergeResultSlot &slot = plan.result_slots_.at(task_idx);
    const ObArray<int64_t, ObWrapperAllocator> *center_idxs =
        use_secondary_center_idxs
            ? slot.secondary_center_idxs_ : slot.center_idxs_;
    if (OB_ISNULL(center_idxs)
        || center_idxs->count() != work_item.vid_end_ - work_item.vid_begin_) {
      ret = OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && i < center_idxs->count();
         ++i) {
      const int64_t cid = center_idxs->at(i);
      if (use_secondary_center_idxs && -1 == cid) {
        // -1 means this vector has no secondary assignment.
      } else if (cid < 0 || cid >= center_count
          || INT64_MAX == member_counts.at(cid)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ++member_counts.at(cid);
      }
    }
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count; ++cid) {
    ObIVFHierarchyVIDList *member_list = member_lists.lists_.at(cid);
    if (OB_ISNULL(member_list)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
        reserve_vector_refs(*member_list, member_counts.at(cid)))) {
      LOG_WARN("failed to reserve IVF member list", K(ret), K(cid),
          K(member_counts.at(cid)), K(use_secondary_center_idxs));
    }
  }
  return ret;
}

static int finish_loaded_insertion_soar(ObIVFMergeRuntime &r, ObIAllocator &allocator,
    ObIVFMergeQuantizer &output)
{
  int ret = r.ddl_runtime_->posting_error();
  const auto &plan = *r.active_plan_;
  const int64_t n = r.all_vids_.count(), k = r.final_cluster_count_;
  const int64_t old_postings = r.insertion_candidates_.count(), delta_count = n - r.insertion_old_count_;
  ObIVFHierarchyMemberLists *postings = nullptr;
  int64_t source_end = 0, copy_end = 0, inputs = 0, primary = 0, secondary = 0, copied = 0;
  bool copying = false;
  if (OB_SUCC(ret) && (r.final_member_lists_ == nullptr || r.final_member_lists_->lists_.count() != k
      || r.insertion_posting_counts_.count() != k || r.insertion_reuse_cids_.count() != k
      || old_postings < r.insertion_old_count_ || delta_count < 0 || old_postings > INT64_MAX - delta_count)) {
    ret = OB_STATE_NOT_MATCH;
  }
  for (int64_t t = 0; OB_SUCC(ret) && t < plan.work_items_.count(); ++t) {
    const auto &item = plan.work_items_.at(t);
    const auto &slot = plan.result_slots_.at(t);
    if (!slot.installed_ || slot.center_idxs_ != nullptr || slot.secondary_center_idxs_ != nullptr
        || slot.primary_posting_count_ < 0
        || (slot.primary_posting_count_ == 0 && item.vid_begin_ >= 0)
        || slot.secondary_posting_count_ < 0
        || slot.primary_posting_count_ > old_postings + delta_count - primary
        || slot.secondary_posting_count_ > delta_count - secondary) {
      ret = OB_INVALID_DATA;
    } else if (item.vid_begin_ >= 0) {
      if (copying || item.cid_begin_ >= 0 || item.vid_begin_ < source_end || item.vid_end_ <= item.vid_begin_
          || item.vid_end_ > n || item.vid_end_ - item.vid_begin_ > ObIVFDDLRuntime::batch_rows(r.dimension_)
          || item.source_input_count_ <= 0 || item.source_input_count_ > item.vid_end_ - item.vid_begin_
          || slot.primary_posting_count_ < item.source_input_count_
          || slot.secondary_posting_count_ > item.source_input_count_
          || ((!r.enable_soar_ || k == 1) && slot.secondary_posting_count_ != 0)) {
        ret = OB_INVALID_DATA;
      } else { source_end = item.vid_end_; inputs += item.source_input_count_; }
    } else {
      copying = true;
      int64_t expected = 0;
      if (item.cid_begin_ < copy_end || item.cid_end_ <= item.cid_begin_ || item.cid_end_ > k
          || item.source_input_count_ != 0 || slot.secondary_posting_count_ != 0) { ret = OB_INVALID_DATA; }
      for (int64_t cid = item.cid_begin_; OB_SUCC(ret) && cid < item.cid_end_; ++cid) {
        const int64_t source_cid = r.insertion_reuse_cids_.at(cid);
        if (source_cid >= 0) { expected += r.insertion_prior_->posting_mass_.at(source_cid); }
      }
      if (OB_SUCC(ret) && slot.primary_posting_count_ != expected) { ret = OB_INVALID_DATA; }
      if (OB_SUCC(ret)) { copy_end = item.cid_end_; copied += expected; }
    }
    if (OB_SUCC(ret)) {
      primary += slot.primary_posting_count_; secondary += slot.secondary_posting_count_;
      ret = is_runtime_canceled(r) ? OB_CANCELED : share::dag_yield();
    }
  }
  if (OB_SUCC(ret) && (inputs != r.insertion_source_input_count_ || primary != old_postings + delta_count
      || copied != r.insertion_reused_posting_count_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("source insertion posting coverage mismatch", K(ret), K(inputs), K(primary), K(copied),
        K(old_postings), K(delta_count), K(r.insertion_source_input_count_), K(r.insertion_reused_posting_count_));
  }
  int64_t total = 0;
  common::ObArray<uint32_t, common::ObWrapperAllocator> counts(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(allocator));

  if (OB_SUCC(ret) && primary > INT64_MAX - secondary) { ret = OB_SIZE_OVERFLOW; }
  if (OB_SUCC(ret) && OB_FAIL(counts.prepare_allocate(k))) {
    LOG_WARN("failed to allocate insertion final posting counts", K(ret), K(k));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    const int64_t count = r.insertion_posting_counts_.at(cid), source_cid = r.insertion_reuse_cids_.at(cid);
    counts.at(cid) = 0;
    if (count < 0 || count > primary + secondary - total
        || (source_cid >= 0 && count < r.insertion_prior_->posting_mass_.at(source_cid))) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(blocksstable::ObIvfCentroidPayload::add_child_count(counts.at(cid), count))) {
      LOG_WARN("insertion leaf posting count overflow", K(ret), K(cid), K(count));
    } else { total += count; }
  }
  if (OB_SUCC(ret) && total != primary + secondary) { ret = OB_INVALID_DATA; }
  if (OB_SUCC(ret) && OB_FAIL(alloc_empty_member_lists(allocator, k, postings))) {
    LOG_WARN("failed to allocate source insertion CID shells", K(ret), K(k));
  }
  if (OB_SUCC(ret) && OB_FAIL(output.set_leaf_posting_counts(counts))) {
    LOG_WARN("loaded insertion posting counts disagree with output", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t cid = 0; cid < k; ++cid) { postings->lists_.at(cid)->sealed_ = true; }
    release_member_lists(allocator, r.final_member_lists_, false);
    r.final_member_lists_ = postings; postings = nullptr;
    r.secondary_assignment_count_ = old_postings - r.insertion_old_count_ + secondary;
    r.soar_finished_ = true;
    free_runtime_object(allocator, r.a0_); free_runtime_object(allocator, r.old_to_new_);
    free_runtime_object(allocator, r.g1_offsets_); free_runtime_object(allocator, r.g1_cids_);
    free_runtime_object(allocator, r.g1_max_distances_);
    r.insertion_offsets_.reset(); r.insertion_candidates_.reset();
    r.insertion_source_offsets_.reset(); r.insertion_source_idxs_.reset();
    r.insertion_child_offsets_.reset(); r.insertion_child_cids_.reset(); r.insertion_origins_.reset();
    r.insertion_posting_counts_.reset(); r.ddl_runtime_->release_routes();
    LOG_INFO("IVF source insertion SOAR completed", "old_vectors", r.insertion_old_count_,
        "new_vectors", delta_count, K(inputs), K(old_postings), K(primary), K(secondary), K(copied),
        "old_routed_vectors", ATOMIC_LOAD(&r.insertion_routed_vectors_));
  }
  release_member_lists(allocator, postings, false);
  return ret;
}

static int finish_incremental_soar(ObIVFMergeRuntime &runtime, ObIAllocator &allocator,
    ObIVFMergeQuantizer &output)
{
  if (uses_loaded_insertion(runtime, output)) { return finish_loaded_insertion_soar(runtime, allocator, output); }
  int ret = OB_SUCCESS;
  const auto &plan = *runtime.active_plan_;
  const int64_t k = runtime.final_cluster_count_;
  const int64_t delta_count = runtime.insertion_delta_vids_.count();
  const int64_t old_postings = runtime.insertion_source_idxs_.count();
  const bool secondary_enabled = runtime.enable_soar_ && k > 1;
  ObIVFHierarchyMemberLists *postings = nullptr;
  ObIVFHierarchyMemberLists *secondary = nullptr;
  int64_t old_seen = 0, delta_seen = 0, secondary_count = 0, origin = 0;
  if (runtime.final_member_lists_ == nullptr || runtime.a0_ == nullptr
      || runtime.final_member_lists_->lists_.count() != k
      || runtime.insertion_reuse_cids_.count() != k) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid incremental SOAR finish state", K(ret), K(k), K(delta_count), K(old_postings));
  } else if (OB_FAIL(alloc_empty_member_lists(allocator, k, postings))) {
    LOG_WARN("failed to allocate incremental posting member lists", K(ret), K(k));
  } else if (secondary_enabled && delta_count > 0
      && OB_FAIL(alloc_empty_member_lists(allocator, k, secondary))) {
    LOG_WARN("failed to allocate incremental secondary member lists", K(ret), K(k));
  }
  for (int64_t t = 0; OB_SUCC(ret) && t < plan.work_items_.count(); ++t) {
    const auto &item = plan.work_items_.at(t);
    const auto &slot = plan.result_slots_.at(t);
    const bool inherited = item.cid_begin_ >= 0;
    const int64_t count = item.vid_end_ - item.vid_begin_;
    if (!slot.installed_ || slot.center_idxs_ == nullptr || count <= 0
        || slot.center_idxs_->count() != count || item.vid_begin_ < 0
        || (inherited && (delta_seen != 0 || item.vid_begin_ != old_seen || item.vid_end_ > old_postings))
        || (!inherited && (item.vid_begin_ != delta_seen || item.vid_end_ > delta_count))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid incremental SOAR task result", K(ret), K(t), K(count), K(inherited), K(old_seen),
          K(delta_seen));
      break;
    }
    if ((inherited || !secondary_enabled) && slot.secondary_center_idxs_ != nullptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unexpected secondary result for inherited postings", K(ret), K(t), K(inherited),
          K(secondary_enabled));
    } else if (!inherited && secondary_enabled
        && (slot.secondary_center_idxs_ == nullptr || slot.secondary_center_idxs_->count() != count)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("missing or mismatched secondary posting result", K(ret), K(t), K(count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const int64_t pos = item.vid_begin_ + i;
      const int64_t idx = inherited ? runtime.insertion_source_idxs_.at(pos)
          : runtime.insertion_delta_idxs_.at(pos);
      const int64_t cid = slot.center_idxs_->at(i);
      if (idx < 0 || idx >= runtime.all_vids_.count() || cid < 0 || cid >= k) {
        ret = OB_INVALID_DATA;
        LOG_WARN("incremental posting vector or center out of range", K(ret), K(t), K(i), K(idx), K(cid),
            K(k));
        break;
      }
      const ObIVFVectorVID vid = runtime.all_vids_.at(idx);
      if (inherited) {
        while (origin + 1 < runtime.insertion_source_offsets_.count()
            && runtime.insertion_source_offsets_.at(origin + 1) <= pos) { ++origin; }
        // Parent lineage partitions old relations. Different source parents
        // cannot coalesce into one final CID, irrespective of old SOAR roles.
        if (origin >= runtime.insertion_child_offsets_.count() - 1
            || runtime.insertion_origins_.at(cid) != origin) {
          ret = OB_INVALID_DATA;
          LOG_WARN("inherited posting center lineage mismatch", K(ret), K(pos), K(cid), K(origin));
        }
      } else if (vid != runtime.insertion_delta_vids_.at(pos)
          || runtime.a0_->at(idx) != cid) {
        ret = OB_INVALID_DATA;
        LOG_WARN("delta posting VID or owner mismatch", K(ret), K(pos), K(idx), K(vid), K(cid));
      }
      if (OB_SUCC(ret) && OB_FAIL(postings->lists_.at(cid)->vids_.push_back(vid))) {
        LOG_WARN("failed to append incremental primary posting", K(ret), K(cid), K(vid));
      }
      if (OB_SUCC(ret) && !inherited && secondary_enabled) {
        const int64_t second_cid = slot.secondary_center_idxs_->at(i);
        if (second_cid < -1 || second_cid >= k || second_cid == cid) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid incremental secondary center", K(ret), K(pos), K(cid), K(second_cid), K(k));
        } else if (second_cid >= 0) {
          if (OB_FAIL(secondary->lists_.at(second_cid)->vids_.push_back(vid))) {
            LOG_WARN("failed to retain incremental secondary posting", K(ret), K(second_cid), K(vid));
          }
          ++secondary_count;
        }
      }
    }
    if (inherited) { old_seen = item.vid_end_; }
    else { delta_seen = item.vid_end_; }
  }
  if (OB_SUCC(ret) && (old_seen != old_postings || delta_seen != delta_count
      || old_postings < runtime.insertion_old_count_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("incremental posting coverage mismatch", K(ret), K(old_seen), K(old_postings), K(delta_seen),
        K(delta_count));
  }
  int64_t total = 0;
  for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
    auto *list = postings->lists_.at(cid);
    const int64_t count = list->vids_.count();
    if (count < 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("incremental center has no postings", K(ret), K(cid));
    }
    else {
      total += count;
      list->sealed_ = true;
      if (secondary != nullptr) { secondary->lists_.at(cid)->sealed_ = true; }
    }
    const int64_t source_cid = runtime.insertion_reuse_cids_.at(cid);
    if (OB_SUCC(ret) && source_cid >= 0
        && count < runtime.insertion_prior_->posting_mass_.at(source_cid)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("reusable center has fewer postings than its source", K(ret), K(cid), K(source_cid),
          K(count));
    }
  }
  if (OB_SUCC(ret) && total != old_postings + delta_count) {
    ret = OB_INVALID_DATA;
    LOG_WARN("incremental total posting count mismatch", K(ret), K(total), K(old_postings), K(delta_count));
  }
  if (OB_SUCC(ret)) {
    release_member_lists(allocator, runtime.final_member_lists_, false);
    runtime.final_member_lists_ = postings;
    runtime.soar_member_lists_ = secondary;
    postings = nullptr; secondary = nullptr;
    runtime.secondary_assignment_count_ = old_postings - runtime.insertion_old_count_ + secondary_count;
    runtime.soar_finished_ = true;
    free_runtime_object(allocator, runtime.a0_);
    free_runtime_object(allocator, runtime.old_to_new_);
    free_runtime_object(allocator, runtime.g1_offsets_);
    free_runtime_object(allocator, runtime.g1_cids_);
    free_runtime_object(allocator, runtime.g1_max_distances_);
    runtime.insertion_source_offsets_.reset();
    runtime.insertion_source_idxs_.reset();
    runtime.insertion_child_offsets_.reset();
    runtime.insertion_child_cids_.reset();
    runtime.insertion_origins_.reset();
    runtime.insertion_delta_vids_.reset();
    runtime.insertion_delta_idxs_.reset();
    LOG_INFO("IVF incremental SOAR completed", "old_vectors", runtime.insertion_old_count_,
        "new_vectors", delta_count, K(old_postings), "soar_vectors", secondary_enabled ? delta_count : 0,
        "old_routed_vectors", ATOMIC_LOAD(&runtime.insertion_routed_vectors_),
        "new_secondary_postings", secondary_count);
  }
  release_member_lists(allocator, postings, false);
  release_member_lists(allocator, secondary, false);
  return ret;
}

int ObIVFMergeProcessor::finish_post_hierarchy_step(
    const ObIVFMergeStage stage,
    const ObIVFMergeStep step,
    const int64_t plan_id,
    ObIVFMergeStepResult &step_result)
{
  int ret = OB_SUCCESS;
  bool refine_train_finished = true;
  ObIVFMergeActivePlan *plan = runtime_->active_plan_;
  if (is_runtime_canceled(*runtime_) || OB_ISNULL(plan)
      || plan->plan_id_ != plan_id || plan->stage_ != stage
      || plan->step_ != step
      || IVF_MERGE_INVALID_DEPTH != plan->depth_) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), KP(runtime_), KP(plan),
        "plan_plan_id", (nullptr == plan) ? -1 : static_cast<int64_t>(plan->plan_id_), "plan_stage",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->stage_), "plan_step",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->step_), "plan_depth",
        (nullptr == plan) ? -1 : static_cast<int64_t>(plan->depth_));
    ret = OB_STATE_NOT_MATCH;
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < plan->result_slots_.count();
       ++i) {
    if (!plan->result_slots_.at(i).installed_) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), "plan_result_slots_count",
          (nullptr == plan) ? -1 : static_cast<int64_t>(plan->result_slots_.count()), K(i));
      ret = OB_STATE_NOT_MATCH;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (ObIVFMergeStep::NEIGHBORHOOD_ADD == step) {
    if (OB_FAIL(algorithm_->neighborhood_finish_add())) {
      LOG_WARN("failed to finish adding IVF neighborhood centers", K(ret),
          K(plan_id));
    } else {
      runtime_->neighborhood_add_finished_ = true;
    }
  } else if (ObIVFMergeStep::NEIGHBORHOOD_BUILD == step) {
    ObArray<int64_t, ObWrapperAllocator> *g0_offsets = nullptr;
    share::ObVectorArray<int64_t> *g0_cids = nullptr;
    ObArray<float, ObWrapperAllocator> *g0_max_distances = nullptr;
    const int64_t k = runtime_->hierarchy_k_;
    int64_t expected_cid_begin = 0;
    if (OB_FAIL(alloc_runtime_array(*allocator_, g0_offsets))) {
      LOG_WARN("failed to allocate IVF neighborhood offsets", K(ret),
          K(plan_id));
    } else if (OB_FAIL(alloc_runtime_array(*allocator_, g0_cids))) {
      LOG_WARN("failed to allocate IVF neighborhood CIDs", K(ret), K(plan_id));
    } else if (OB_FAIL(alloc_runtime_array(*allocator_, g0_max_distances))) {
      LOG_WARN("failed to allocate IVF neighborhood distances", K(ret),
          K(plan_id));
    } else if (OB_FAIL(g0_offsets->push_back(0))) {
      LOG_WARN("failed to initialize IVF neighborhood offsets", K(ret),
          K(plan_id));
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && task_idx < plan->work_items_.count();
         ++task_idx) {
      const ObIVFMergeWorkItem &work_item =
          plan->work_items_.at(task_idx);
      const ObIVFMergeResultSlot &slot =
          plan->result_slots_.at(task_idx);
      const int64_t row_count = work_item.cid_end_ - work_item.cid_begin_;
      if (work_item.cid_begin_ != expected_cid_begin
          || OB_ISNULL(slot.neighbor_offsets_)
          || OB_ISNULL(slot.neighbor_cids_)
          || OB_ISNULL(slot.neighbor_max_distances_)
          || slot.neighbor_max_distances_->count() != row_count) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(work_item.cid_begin_),
            K(expected_cid_begin), KP(slot.neighbor_offsets_), KP(slot.neighbor_cids_),
            KP(slot.neighbor_max_distances_), "slot_neighbor_max_distances_count",
            (nullptr == slot.neighbor_max_distances_) ? -1 : static_cast<int64_t>(slot.neighbor_max_distances_->count()));
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t local_row = 0;
           OB_SUCC(ret) && local_row < row_count;
           ++local_row) {
        const int64_t begin = slot.neighbor_offsets_->at(local_row);
        const int64_t end = slot.neighbor_offsets_->at(local_row + 1);
        for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
          ret = g0_cids->push_back(slot.neighbor_cids_->at(i));
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "g0_cids->push_back", K(plan_id), K(stage),
                K(step), K(i), K(begin), K(end));
          }
        }
        if (OB_SUCC(ret)) {
          ret = g0_offsets->push_back(g0_cids->count());
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "g0_offsets->push_back", K(plan_id), K(stage),
                K(step));
          }
        }
        if (OB_SUCC(ret)) {
          ret = g0_max_distances->push_back(
              slot.neighbor_max_distances_->at(local_row));
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "g0_max_distances->push_back", K(plan_id),
                K(stage), K(step));
          }
        }
      }
      expected_cid_begin = work_item.cid_end_;
    }
    if (OB_SUCC(ret)
        && (expected_cid_begin != k || g0_offsets->count() != k + 1
            || g0_max_distances->count() != k)) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(expected_cid_begin), K(k),
          "g0_offsets_count", (nullptr == g0_offsets) ? -1 : static_cast<int64_t>(g0_offsets->count()),
          "g0_max_distances_count",
          (nullptr == g0_max_distances) ? -1 : static_cast<int64_t>(g0_max_distances->count()));
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      ObIVFNeighborGraphView graph_view(
          *g0_offsets, *g0_cids, *g0_max_distances, k);
      if (!graph_view.is_valid()) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(graph_view));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(algorithm_->neighborhood_finish())) {
        LOG_WARN("failed to finish IVF neighborhood build", K(ret), K(plan_id));
      }
    }
    if (OB_SUCC(ret)) {
      runtime_->g0_offsets_ = g0_offsets;
      runtime_->g0_cids_ = g0_cids;
      runtime_->g0_max_distances_ = g0_max_distances;
      runtime_->neighborhood_finished_ = true;
      g0_offsets = nullptr;
      g0_cids = nullptr;
      g0_max_distances = nullptr;
    }
    free_runtime_object(*allocator_, g0_offsets);
    free_runtime_object(*allocator_, g0_cids);
    free_runtime_object(*allocator_, g0_max_distances);
  } else if (ObIVFMergeStep::REFINE_SAMPLE_PREPARE == step) {
    ret = runtime_->ddl_runtime_ == nullptr ? OB_STATE_NOT_MATCH : runtime_->ddl_runtime_->finish_samples();
  } else if (ObIVFMergeStep::REFINE_TRAIN == step
      && ((runtime_->ddl_runtime_ != nullptr || runtime_->selected_type_ == ObIVFProcessType::INITIAL_BUILD)
          && algorithm_->supports_batched_refine_train())) {
    share::ObVectorMatrix *centers = nullptr;
    if (OB_FAIL(alloc_runtime_array(*allocator_, centers))) {
    } else if (OB_FAIL(runtime_->ddl_runtime_ != nullptr
        ? runtime_->ddl_runtime_->finish_refine_phase(*centers, refine_train_finished)
        : algorithm_->refine_train_finish_batch(*centers, refine_train_finished))) {
      LOG_WARN("failed to finish IVF refinement distance batch", K(ret), K(plan_id));
    } else if (refine_train_finished) {
      if (centers->count() != runtime_->hierarchy_k_ * runtime_->dimension_
          || !are_finite_center_values(*centers)) { ret = OB_ERR_UNEXPECTED; }
      else { runtime_->working_centers_ = centers; centers = nullptr; }
    }
    free_runtime_object(*allocator_, centers);
  } else if (ObIVFMergeStep::REFINE_TRAIN == step) {
    if (runtime_->working_centers_ != nullptr) {
      if (OB_FAIL(algorithm_->refine_train_finish_batch(
          *runtime_->working_centers_, refine_train_finished))) {
        LOG_WARN("failed to finish IVF refine batch", K(ret), K(plan_id));
      } else if (refine_train_finished
          && (runtime_->working_centers_->count() != runtime_->hierarchy_k_ * runtime_->dimension_
              || !are_finite_center_values(*runtime_->working_centers_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid refined IVF center values", K(ret), K(plan_id),
            K(runtime_->working_centers_->count()), K(runtime_->hierarchy_k_), K(runtime_->dimension_));
      }
    } else {
      ObIVFMergeResultSlot &slot = plan->result_slots_.at(0);
      if (plan->result_slots_.count() != 1 || slot.center_values_ == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid IVF refine result slot", K(ret), K(plan_id), K(plan->result_slots_.count()));
      } else {
        runtime_->working_centers_ = slot.center_values_;
        slot.center_values_ = nullptr;
        refine_train_finished = !algorithm_->supports_parallel_refine();
      }
    }
  } else if (ObIVFMergeStep::REFINE_ASSIGN == step) {
    const int64_t k = runtime_->hierarchy_k_;
    const int64_t n = runtime_->all_vids_.count();
    share::ObVectorArray<int64_t> *refined_assignments = nullptr;
    ObArray<uint8_t, ObWrapperAllocator> *changed_cids = nullptr;
    ObIVFHierarchyMemberLists *refined_member_lists = nullptr;
    int64_t changed_center_count = 0;
    int64_t expected_vid_begin = 0;
    if (OB_FAIL(alloc_runtime_array(
        *allocator_, refined_assignments))) {
      LOG_WARN("failed to allocate reduced IVF refined assignments", K(ret),
          K(plan_id), K(n));
    } else if (OB_FAIL(append_filled_int_array(
        n, -1, *refined_assignments))) {
      LOG_WARN("failed to initialize reduced IVF refined assignments", K(ret),
          K(plan_id), K(n));
    } else if (OB_FAIL(alloc_runtime_object(*allocator_, changed_cids,
        OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(*allocator_)))) {
      LOG_WARN("failed to allocate reduced IVF changed-center set", K(ret),
          K(plan_id), K(k));
    } else if (OB_FAIL(append_filled_byte_array(
        k, 0, *changed_cids))) {
      LOG_WARN("failed to initialize reduced IVF changed-center set", K(ret),
          K(plan_id), K(k));
    } else if (OB_FAIL(alloc_empty_member_lists(
        *allocator_, k, refined_member_lists, runtime_->vector_store_->uses_position_ids()))) {
      LOG_WARN("failed to allocate reduced IVF refined member lists", K(ret),
          K(plan_id), K(k));
    } else if (OB_FAIL(reserve_member_lists_from_plan(
        *plan, *refined_member_lists, false /* use_secondary_center_idxs */))) {
      LOG_WARN("failed to reserve reduced IVF refined member lists", K(ret),
          K(plan_id), K(k), K(n));
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && task_idx < plan->work_items_.count();
         ++task_idx) {
      const ObIVFMergeWorkItem &work_item =
          plan->work_items_.at(task_idx);
      const ObArray<int64_t, ObWrapperAllocator> *center_idxs =
          plan->result_slots_.at(task_idx).center_idxs_;
      if (work_item.vid_begin_ != expected_vid_begin
          || work_item.vid_begin_ < 0 || work_item.vid_end_ > n
          || OB_ISNULL(center_idxs)
          || center_idxs->count()
              != work_item.vid_end_ - work_item.vid_begin_) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(work_item.vid_begin_),
            K(expected_vid_begin), KP(center_idxs), "assignments_count",
            (nullptr == center_idxs) ? -1 : static_cast<int64_t>(center_idxs->count()), K(work_item.vid_end_));
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && i < center_idxs->count();
           ++i) {
        const int64_t vector_idx = runtime_->ddl_runtime_ != nullptr && plan->ddl_ != nullptr
            ? runtime_->ddl_runtime_->vector_index(work_item.vid_begin_ + i) : work_item.vid_begin_ + i;
        const int64_t cid = center_idxs->at(i);
        const int64_t old_cid = runtime_->a0_->at(vector_idx);
        if (cid < 0 || cid >= k || old_cid < 0 || old_cid >= k
            || refined_assignments->at(vector_idx) >= 0) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          refined_assignments->at(vector_idx) = cid;
          if (old_cid != cid) {
            changed_cids->at(old_cid) = 1;
            changed_cids->at(cid) = 1;
          }
          ret = append_vector_ref(
              *refined_member_lists->lists_.at(cid),
              runtime_->all_vids_.at(vector_idx), vector_idx);
        }
      }
      expected_vid_begin = work_item.vid_end_;
    }
    if (OB_SUCC(ret) && expected_vid_begin != n) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(expected_vid_begin),
          K(n));
      ret = OB_ERR_UNEXPECTED;
    }
    for (int64_t vector_idx = 0;
         OB_SUCC(ret) && vector_idx < n;
         ++vector_idx) {
      if (refined_assignments->at(vector_idx) < 0) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "refined_assignments_count",
            (nullptr == refined_assignments) ? -1 : static_cast<int64_t>(refined_assignments->count()), K(vector_idx));
        ret = OB_ERR_UNEXPECTED;
      }
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
      refined_member_lists->lists_.at(cid)->sealed_ = true;
      if (0 != changed_cids->at(cid)) {
        ++changed_center_count;
      }
    }
    if (OB_SUCC(ret)) {
      runtime_->refined_assignments_ = refined_assignments;
      runtime_->changed_cids_ = changed_cids;
      runtime_->refined_member_lists_ = refined_member_lists;
      runtime_->refine_has_changes_ = changed_center_count > 0;
      refined_assignments = nullptr;
      changed_cids = nullptr;
      refined_member_lists = nullptr;
      free_runtime_object(*allocator_, runtime_->a0_);
      release_initial_member_lists(*allocator_, *runtime_);
    }
    free_runtime_object(*allocator_, refined_assignments);
    free_runtime_object(*allocator_, changed_cids);
    release_member_lists(*allocator_, refined_member_lists, false);
  } else if (ObIVFMergeStep::REFINE_UPDATE == step) {
    share::ObVectorMatrix *updated_working_centers = nullptr;
    if (OB_FAIL(alloc_runtime_array(
        *allocator_, updated_working_centers))) {
      LOG_WARN("failed to allocate reduced IVF working centers", K(ret),
          K(plan_id));
    } else if (OB_FAIL(updated_working_centers->set_dimension(runtime_->dimension_))) {
    } else if (OB_FAIL(updated_working_centers->reserve(runtime_->working_centers_->count()))) {
      LOG_WARN("failed to reserve reduced IVF working centers", K(ret),
          K(plan_id),
          "center_value_count", runtime_->working_centers_->count());
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && i < runtime_->working_centers_->count();
         ++i) {
      ret = updated_working_centers->push_back(
          runtime_->working_centers_->at(i));
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge operation failed", K(ret), "operation", "updated_working_centers->push_back", K(plan_id),
            K(stage), K(step), K(i), "runtime_working_centers_count",
            (nullptr == runtime_ || nullptr == runtime_->working_centers_) ? -1 : static_cast<int64_t>(runtime_->working_centers_->count()));
      }
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && task_idx < plan->work_items_.count();
         ++task_idx) {
      const int64_t cid =
          plan->work_items_.at(task_idx).local_center_idx_;
      share::ObVectorMatrix *updated_center =
          plan->result_slots_.at(task_idx).center_values_;
      if (OB_ISNULL(updated_center)
          || updated_center->count() != runtime_->dimension_) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), KP(updated_center),
            "updated_center_count", (nullptr == updated_center) ? -1 : static_cast<int64_t>(updated_center->count()),
            "runtime_dimension", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t d = 0; OB_SUCC(ret) && d < runtime_->dimension_; ++d) {
        updated_working_centers->at(cid * runtime_->dimension_ + d) =
            updated_center->at(d);
      }
    }
    if (OB_SUCC(ret)) {
      free_runtime_object(*allocator_, runtime_->working_centers_);
      runtime_->working_centers_ = updated_working_centers;
      updated_working_centers = nullptr;
    }
    free_runtime_object(*allocator_, updated_working_centers);
  } else if (ObIVFMergeStep::REFINE_COMPACT == step) {
    const share::ObVectorMatrix &source_centers
        = runtime_->refine_skipped_ ? (runtime_->c0_) : (*runtime_->working_centers_);
    ObIVFHierarchyMemberLists *source_member_lists =
        runtime_->refine_skipped_ ? nullptr : runtime_->refined_member_lists_;
    const int64_t old_k = runtime_->refine_skipped_
        ? runtime_->initial_member_lists_.count()
        : source_member_lists->lists_.count();
    ObArray<int64_t, ObWrapperAllocator> *old_to_new = nullptr;
    share::ObVectorMatrix *c1 = nullptr;
    ObIVFHierarchyMemberLists *final_member_lists = nullptr;
    ObArray<int64_t, ObWrapperAllocator> *g1_offsets = nullptr;
    share::ObVectorArray<int64_t> *g1_cids = nullptr;
    ObArray<float, ObWrapperAllocator> *g1_max_distances = nullptr;
    if (OB_FAIL(alloc_runtime_array(*allocator_, old_to_new))) {
      LOG_WARN("failed to allocate compacted IVF CID mapping", K(ret),
          K(plan_id), K(old_k));
    } else if (OB_FAIL(append_filled_int_array(old_k, -1, *old_to_new))) {
      LOG_WARN("failed to initialize compacted IVF CID mapping", K(ret),
          K(plan_id), K(old_k));
    } else if (OB_FAIL(alloc_runtime_array(*allocator_, c1))) {
      LOG_WARN("failed to allocate compacted IVF centers", K(ret), K(plan_id));
    } else if (OB_FAIL(c1->set_dimension(runtime_->dimension_))) {
    } else if (OB_FAIL(alloc_runtime_object(*allocator_, final_member_lists, *allocator_))) {
      LOG_WARN("failed to allocate compacted IVF member lists", K(ret),
          K(plan_id));
    }
    for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_k; ++old_cid) {
      ObIVFHierarchyVIDList *member_list = runtime_->refine_skipped_
          ? runtime_->initial_member_lists_.at(old_cid)
          : source_member_lists->lists_.at(old_cid);
      if (!member_list->vids_.empty() || runtime_->insertion_prior_ != nullptr) {
        old_to_new->at(old_cid) = final_member_lists->lists_.count();
        for (int64_t d = 0; OB_SUCC(ret) && d < runtime_->dimension_; ++d) {
          ret = c1->push_back(
              source_centers.at(old_cid * runtime_->dimension_ + d));
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "c1->push_back", K(plan_id), K(stage), K(step),
                K(d), "runtime_dimension", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
          }
        }
        if (OB_SUCC(ret)) {
          ret = final_member_lists->lists_.push_back(member_list);
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF merge operation failed", K(ret), "operation", "final_member_lists->lists_.push_back",
                K(plan_id), K(stage), K(step));
          }
        }
      }
    }
    const int64_t final_k = OB_SUCC(ret)
        ? final_member_lists->lists_.count() : 0;
    if (OB_SUCC(ret)
        && (final_k <= 0
            || (runtime_->insertion_prior_ != nullptr
                && (final_k != old_k || final_k != runtime_->insertion_origins_.count()))
            || c1->count() != final_k * runtime_->dimension_)) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(final_k), "c1_count",
          (nullptr == c1) ? -1 : static_cast<int64_t>(c1->count()), "runtime_dimension",
          (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->dimension_));
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret) && final_k > IVF_MAX_NEIGHBOR_COUNT
        && OB_NOT_NULL(runtime_->g0_offsets_)
        && OB_NOT_NULL(runtime_->g0_cids_)
        && OB_NOT_NULL(runtime_->g0_max_distances_)) {
      if (OB_FAIL(alloc_runtime_array(*allocator_, g1_offsets))) {
        LOG_WARN("failed to allocate compacted IVF neighbor offsets", K(ret),
            K(plan_id), K(final_k));
      } else if (OB_FAIL(alloc_runtime_array(*allocator_, g1_cids))) {
        LOG_WARN("failed to allocate compacted IVF neighbor CIDs", K(ret),
            K(plan_id), K(final_k));
      } else if (OB_FAIL(alloc_runtime_array(
          *allocator_, g1_max_distances))) {
        LOG_WARN("failed to allocate compacted IVF neighbor distances", K(ret),
            K(plan_id), K(final_k));
      } else if (OB_FAIL(g1_offsets->push_back(0))) {
        LOG_WARN("failed to initialize compacted IVF neighbor offsets", K(ret),
            K(plan_id), K(final_k));
      }
      for (int64_t old_cid = 0;
           OB_SUCC(ret) && old_cid < old_k;
           ++old_cid) {
        const int64_t new_cid = old_to_new->at(old_cid);
        if (new_cid >= 0) {
          const int64_t begin = runtime_->g0_offsets_->at(old_cid);
          const int64_t end = runtime_->g0_offsets_->at(old_cid + 1);
          for (int64_t i = begin;
               OB_SUCC(ret) && i < end
                   && g1_cids->count() - g1_offsets->at(new_cid)
                       < IVF_MAX_NEIGHBOR_COUNT;
               ++i) {
            const int64_t old_neighbor = runtime_->g0_cids_->at(i);
            const int64_t new_neighbor = old_to_new->at(old_neighbor);
            if (new_neighbor >= 0 && new_neighbor != new_cid
                && !contains_cid(
                    *g1_cids,
                    g1_offsets->at(new_cid),
                    g1_cids->count(),
                    new_neighbor)) {
              ret = g1_cids->push_back(new_neighbor);
              if (OB_FAIL(ret)) {
                LOG_WARN("IVF merge operation failed", K(ret), "operation", "g1_cids->push_back", K(plan_id), K(stage),
                    K(step), K(new_neighbor), K(new_cid), K(g1_cids), "g1_offsets_count",
                    (nullptr == g1_offsets) ? -1 : static_cast<int64_t>(g1_offsets->count()), "g1_cids_count",
                    (nullptr == g1_cids) ? -1 : static_cast<int64_t>(g1_cids->count()));
              }
            }
          }
          if (OB_SUCC(ret)) {
            ret = g1_offsets->push_back(g1_cids->count());
            if (OB_FAIL(ret)) {
              LOG_WARN("IVF merge operation failed", K(ret), "operation", "g1_offsets->push_back", K(plan_id), K(stage),
                  K(step));
            }
          }
          if (OB_SUCC(ret)) {
            ret = g1_max_distances->push_back(
                runtime_->g0_max_distances_->at(old_cid));
            if (OB_FAIL(ret)) {
              LOG_WARN("IVF merge operation failed", K(ret), "operation", "g1_max_distances->push_back", K(plan_id),
                  K(stage), K(step));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObIVFNeighborGraphView graph_view(
            *g1_offsets, *g1_cids, *g1_max_distances, final_k);
        if (!graph_view.is_valid()) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(graph_view));
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t old_cid = 0; old_cid < old_k; ++old_cid) {
        if (old_to_new->at(old_cid) >= 0) {
          if (runtime_->refine_skipped_) {
            runtime_->initial_member_lists_.at(old_cid) = nullptr;
          } else {
            source_member_lists->lists_.at(old_cid) = nullptr;
          }
        }
      }
      runtime_->old_to_new_ = old_to_new;
      runtime_->c1_ = c1;
      runtime_->final_member_lists_ = final_member_lists;
      runtime_->final_cluster_count_ = final_k;
      runtime_->g1_offsets_ = g1_offsets;
      runtime_->g1_cids_ = g1_cids;
      runtime_->g1_max_distances_ = g1_max_distances;
      runtime_->refine_finished_ = true;
      old_to_new = nullptr;
      c1 = nullptr;
      final_member_lists = nullptr;
      g1_offsets = nullptr;
      g1_cids = nullptr;
      g1_max_distances = nullptr;
      if (runtime_->refine_skipped_) {
        runtime_->initial_member_lists_.reset();
      } else {
        release_member_lists(
            *allocator_, runtime_->refined_member_lists_, false);
      }
      runtime_->c0_.reset();
      free_runtime_object(*allocator_, runtime_->g0_offsets_);
      free_runtime_object(*allocator_, runtime_->g0_cids_);
      free_runtime_object(*allocator_, runtime_->g0_max_distances_);
      free_runtime_object(*allocator_, runtime_->working_centers_);
      free_runtime_object(*allocator_, runtime_->changed_cids_);
    }
    free_runtime_object(*allocator_, old_to_new);
    free_runtime_object(*allocator_, c1);
    release_member_lists(*allocator_, final_member_lists, true);
    free_runtime_object(*allocator_, g1_offsets);
    free_runtime_object(*allocator_, g1_cids);
    free_runtime_object(*allocator_, g1_max_distances);
  } else if (ObIVFMergeStep::SOAR_ASSIGN == step && runtime_->insertion_prior_ != nullptr) {
    ret = finish_incremental_soar(*runtime_, *allocator_, *physical_output_);
    if (OB_FAIL(ret)) {
        LOG_WARN("failed to finalize incremental IVF SOAR", K(ret));
      }
  } else if (ObIVFMergeStep::SOAR_ASSIGN == step) {
    // Loaded DDL QUANT consumes dense assignments, never secondary VID lists.
    // Avoid constructing O(N) lists solely to discard them at this barrier.
    const bool loaded_quant = runtime_->ddl_runtime_ != nullptr
        && physical_output_->supports_loaded_quantization();
    const int64_t n = runtime_->all_vids_.count();
    const int64_t k = runtime_->final_member_lists_->lists_.count();
    common::ObArray<uint32_t, common::ObWrapperAllocator> leaf_posting_counts(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ObWrapperAllocator(*allocator_));

    share::ObVectorArray<int64_t> *a1 = nullptr;
    share::ObVectorArray<int64_t> *a2 = nullptr;
    ObIVFHierarchyMemberLists *soar_member_lists = nullptr;
    int64_t expected_vid_begin = 0;
    int64_t secondary_count = 0;
    if (loaded_quant && OB_FAIL(leaf_posting_counts.prepare_allocate(k))) {
      LOG_WARN("failed to allocate DDL leaf posting counts", K(ret), K(k));
    } else if (loaded_quant) {
      std::fill_n(leaf_posting_counts.get_data(), leaf_posting_counts.count(), 0);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_runtime_array(*allocator_, a1))) {
      LOG_WARN("failed to allocate final IVF primary assignments", K(ret),
          K(plan_id), K(n));
    } else if (OB_FAIL(append_filled_int_array(n, -1, *a1))) {
      LOG_WARN("failed to initialize final IVF primary assignments", K(ret),
          K(plan_id), K(n));
    }
    if (OB_SUCC(ret) && runtime_->enable_soar_ && k > 1) {
      if (OB_FAIL(alloc_runtime_array(*allocator_, a2))) {
        LOG_WARN("failed to allocate final IVF secondary assignments", K(ret),
            K(plan_id), K(n));
      } else if (OB_FAIL(append_filled_int_array(n, -1, *a2))) {
        LOG_WARN("failed to initialize final IVF secondary assignments", K(ret),
            K(plan_id), K(n));
      } else if (!loaded_quant && OB_FAIL(alloc_empty_member_lists(
          *allocator_, k, soar_member_lists, runtime_->vector_store_->uses_position_ids()))) {
        LOG_WARN("failed to allocate final IVF SOAR member lists", K(ret),
            K(plan_id), K(k));
      } else if (!loaded_quant && OB_FAIL(reserve_member_lists_from_plan(
          *plan, *soar_member_lists, true /* use_secondary_center_idxs */))) {
        LOG_WARN("failed to reserve final IVF SOAR member lists", K(ret),
            K(plan_id), K(k), K(n));
      }
    }
    for (int64_t task_idx = 0;
         OB_SUCC(ret) && task_idx < plan->work_items_.count();
         ++task_idx) {
      const ObIVFMergeWorkItem &work_item =
          plan->work_items_.at(task_idx);
      const ObArray<int64_t, ObWrapperAllocator> *center_idxs =
          plan->result_slots_.at(task_idx).center_idxs_;
      if (work_item.vid_begin_ != expected_vid_begin
          || work_item.vid_begin_ < 0 || work_item.vid_end_ > n
          || OB_ISNULL(center_idxs)
          || center_idxs->count()
              != work_item.vid_end_ - work_item.vid_begin_) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(work_item.vid_begin_),
            K(expected_vid_begin), KP(center_idxs), "assignments_count",
            (nullptr == center_idxs) ? -1 : static_cast<int64_t>(center_idxs->count()), K(work_item.vid_end_));
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && i < center_idxs->count();
           ++i) {
        const int64_t vector_idx = runtime_->ddl_runtime_ != nullptr && plan->ddl_ != nullptr
            ? runtime_->ddl_runtime_->vector_index(work_item.vid_begin_ + i) : work_item.vid_begin_ + i;
        const int64_t cid = center_idxs->at(i);
        if (cid < 0 || cid >= k || a1->at(vector_idx) >= 0) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          a1->at(vector_idx) = cid;
        }
      }
      const ObArray<int64_t, ObWrapperAllocator> *secondary_center_idxs =
          plan->result_slots_.at(task_idx).secondary_center_idxs_;
      const bool secondary_enabled = runtime_->enable_soar_ && k > 1;
      if (OB_SUCC(ret)) {
        if (secondary_enabled
            && (OB_ISNULL(a2) || (!loaded_quant && OB_ISNULL(soar_member_lists))
                || OB_ISNULL(secondary_center_idxs)
                || secondary_center_idxs->count()
                    != work_item.vid_end_ - work_item.vid_begin_)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!secondary_enabled
            && OB_NOT_NULL(secondary_center_idxs)) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      for (int64_t i = 0;
           OB_SUCC(ret) && secondary_enabled
               && i < secondary_center_idxs->count();
           ++i) {
        const int64_t vector_idx = runtime_->ddl_runtime_ != nullptr && plan->ddl_ != nullptr
            ? runtime_->ddl_runtime_->vector_index(work_item.vid_begin_ + i) : work_item.vid_begin_ + i;
        const int64_t cid = secondary_center_idxs->at(i);
        if (cid < -1 || cid >= k
            || (cid >= 0 && (a2->at(vector_idx) >= 0
                || a1->at(vector_idx) == cid))) {
          ret = OB_ERR_UNEXPECTED;
        } else if (cid >= 0) {
          a2->at(vector_idx) = cid;
          if (loaded_quant && OB_FAIL(blocksstable::ObIvfCentroidPayload::add_child_count(
              leaf_posting_counts.at(cid), 1))) {
            LOG_WARN("DDL secondary child count overflow", K(ret), K(cid));
          }
          if (!loaded_quant) {
            ret = append_vector_ref(*soar_member_lists->lists_.at(cid),
                runtime_->all_vids_.at(vector_idx), vector_idx);
          }
          ++secondary_count;
        }
      }
      expected_vid_begin = work_item.vid_end_;
    }
    int64_t primary_member_count = 0;
    for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
      const ObIVFVectorIDView &member_vids =
          runtime_->final_member_lists_->lists_.at(cid)->vids_;
      const share::ObVectorArray<int64_t> &member_vector_idxs =
          runtime_->final_member_lists_->lists_.at(cid)->vector_idxs_;
      primary_member_count += member_vids.count();
      if (loaded_quant && OB_FAIL(blocksstable::ObIvfCentroidPayload::add_child_count(
          leaf_posting_counts.at(cid), member_vids.count()))) {
        LOG_WARN("DDL primary child count overflow", K(ret), K(cid));
      }
      if (member_vector_idxs.count() != member_vids.count()) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
            "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step),
            "member_vector_idxs_count", member_vector_idxs.count(), "member_vids_count", member_vids.count());
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < member_vids.count(); ++i) {
        const int64_t vector_idx = member_vector_idxs.at(i);
        if (vector_idx < 0 || vector_idx >= n
            || runtime_->all_vids_.at(vector_idx) != member_vids.at(i)
            || a1->at(vector_idx) != cid) {
          LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
              "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(vector_idx), K(n),
              "runtime_all_vids_count", (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->all_vids_.count()),
              "member_vids_count", member_vids.count(), K(i), "a1_count",
              (nullptr == a1) ? -1 : static_cast<int64_t>(a1->count()));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_NOT_NULL(soar_member_lists)) {
        soar_member_lists->lists_.at(cid)->sealed_ = true;
      }
    }
    if (OB_SUCC(ret)
        && (expected_vid_begin != n || primary_member_count != n)) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), K(expected_vid_begin), K(n),
          K(primary_member_count));
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      if (loaded_quant && OB_FAIL(physical_output_->set_leaf_posting_counts(leaf_posting_counts))) {
        LOG_WARN("failed to install DDL leaf posting counts", K(ret), K(k));
      }
    }
    if (OB_SUCC(ret)) {
      // SOAR has validated the dense assignment arrays and vector indexes.
      // QUANT consumes only the per-CID VID posting lists, so discard the
      // redundant dense/index views before the potentially long QUANT stage.
      for (int64_t cid = 0; cid < k; ++cid) {
        runtime_->final_member_lists_->lists_.at(cid)->vector_idxs_.reset();
        if (OB_NOT_NULL(soar_member_lists)) {
          soar_member_lists->lists_.at(cid)->vector_idxs_.reset();
        }
        if (runtime_->ddl_runtime_ != nullptr && physical_output_->supports_loaded_quantization()) {
          // Loaded QUANT uses dense ownership only; retain just the small list
          // shells needed by the shared completion contract, not N posting VIDs.
          runtime_->final_member_lists_->lists_.at(cid)->vids_.reset();
          if (soar_member_lists != nullptr) { soar_member_lists->lists_.at(cid)->vids_.reset(); }
        }
      }
      runtime_->a1_ = a1;
      runtime_->a2_ = a2;
      runtime_->soar_member_lists_ = soar_member_lists;
      runtime_->secondary_assignment_count_ = secondary_count;
      runtime_->soar_finished_ = true;
      a1 = nullptr;
      a2 = nullptr;
      soar_member_lists = nullptr;
      const bool postings_encoded = runtime_->ddl_runtime_ != nullptr
          && physical_output_->supports_loaded_quantization()
          && ObIVFDDLRuntime::quantize_during_soar(*runtime_);
      if (runtime_->ddl_runtime_ == nullptr || !physical_output_->supports_loaded_quantization()
          || postings_encoded) {
        free_runtime_object(*allocator_, runtime_->a1_);
        free_runtime_object(*allocator_, runtime_->a2_);
      }
      if (postings_encoded) { runtime_->ddl_runtime_->release_routes(); }
      if (runtime_->refine_skipped_) {
        free_runtime_object(*allocator_, runtime_->a0_);
      } else {
        free_runtime_object(
            *allocator_, runtime_->refined_assignments_);
      }
      free_runtime_object(*allocator_, runtime_->old_to_new_);
      free_runtime_object(*allocator_, runtime_->g1_offsets_);
      free_runtime_object(*allocator_, runtime_->g1_cids_);
      free_runtime_object(*allocator_, runtime_->g1_max_distances_);
    }
    free_runtime_object(*allocator_, a1);
    free_runtime_object(*allocator_, a2);
    release_member_lists(*allocator_, soar_member_lists, false);
  } else if (ObIVFMergeStep::QUANT == step) {
    const int64_t vector_count = runtime_->all_vids_.count();
    const int64_t center_count = runtime_->final_cluster_count_;
    int64_t posting_count = 0;
    if (!runtime_->soar_finished_ || runtime_->quant_finished_
        || center_count <= 0 || OB_ISNULL(runtime_->c1_)
        || (runtime_->effective_type_ != ObIVFProcessType::CONCATENATION
            && (OB_ISNULL(runtime_->final_member_lists_)
                || runtime_->final_member_lists_->lists_.count() != center_count))
        || runtime_->secondary_assignment_count_ < 0) {
      ret = OB_STATE_NOT_MATCH;
    } else if (INT64_MAX - vector_count
        < runtime_->secondary_assignment_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF MERGE posting count overflow", K(ret),
          K(vector_count),
          "secondary_count", runtime_->secondary_assignment_count_);
    } else if (FALSE_IT(posting_count =
        vector_count + runtime_->secondary_assignment_count_)) {
    } else if (OB_FAIL(physical_output_->finish_quantization(
        vector_count, center_count, posting_count))) {
      LOG_WARN("fail to finish IVF merge quantization", K(ret),
          K(vector_count), K(center_count), K(posting_count));
    } else {
      runtime_->quant_finished_ = true;
      runtime_->centroid_layout_.reset();
      runtime_->insertion_reuse_cids_.reset();
      // RAW_WRITE needs only the immutable VID sequence. Release all
      // assignment/member/center state as soon as the QUANT barrier has
      // durably finished consuming it instead of retaining O(N) metadata
      // throughout the raw-vector write.
      free_runtime_object(*allocator_, runtime_->c1_);
      release_member_lists(
          *allocator_, runtime_->final_member_lists_, false);
      free_runtime_object(*allocator_, runtime_->a1_);
      free_runtime_object(*allocator_, runtime_->a2_);
      release_member_lists(
          *allocator_, runtime_->soar_member_lists_, false);
    }
  } else if (ObIVFMergeStep::RAW_WRITE == step) {
    if (!runtime_->quant_finished_ || runtime_->merge_finished_ || plan->work_items_.empty()) {
      LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
          "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step), "runtime_quant_finished",
          (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->quant_finished_), "runtime_merge_finished",
          (nullptr == runtime_) ? -1 : static_cast<int64_t>(runtime_->merge_finished_), "plan_work_items_count",
          (nullptr == plan) ? -1 : static_cast<int64_t>(plan->work_items_.count()));
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(physical_output_->finish_raw_write(runtime_->all_vids_.count()))) {
      LOG_WARN("fail to finish IVF merge RAW vector write", K(ret),
          "vector_count", runtime_->all_vids_.count());
    } else {
      runtime_->merge_finished_ = true;
    }
  } else {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeProcessor::finish_post_hierarchy_step", K(plan_id), K(stage), K(step));
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret)) {
    if (runtime_->ddl_runtime_ != nullptr) {
      if (step == ObIVFMergeStep::REFINE_UPDATE || step == ObIVFMergeStep::REFINE_COMPACT) {
        runtime_->ddl_runtime_->release_stats();
      } else if (step == ObIVFMergeStep::QUANT) { runtime_->ddl_runtime_->release_routes(); }
    }
    release_active_plan(*allocator_, runtime_->active_plan_);
    step_result.plan_id_ = plan_id;
    step_result.stage_ = stage;
    step_result.step_ = step;
    step_result.depth_ = IVF_MERGE_INVALID_DEPTH;
    step_result.committed_ = true;
    step_result.hierarchy_finished_ = true;
    step_result.k_ = runtime_->final_cluster_count_ > 0
        ? runtime_->final_cluster_count_ : runtime_->hierarchy_k_;
    step_result.neighborhood_mode_ = runtime_->neighborhood_mode_;
    step_result.refine_skipped_ = runtime_->refine_skipped_;
    step_result.refine_has_changes_ = runtime_->refine_has_changes_;
    step_result.refine_train_finished_ = refine_train_finished;
    step_result.next_refine_needs_samples_ = runtime_->ddl_runtime_ != nullptr && !runtime_->refine_skipped_;
    step_result.merge_finished_ =
        runtime_->merge_finished_;
  }
  return ret;
}

int ObIVFMergeProcessor::build_final_artifact(
    ObIVFMergeArtifact &final_artifact)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    LOG_WARN("IVF merge is not initialized", "ret", OB_NOT_INIT, "operation",
        "ObIVFMergeProcessor::build_final_artifact");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(runtime_) || !runtime_->merge_finished_
      || !runtime_->quant_finished_
      || runtime_->final_cluster_count_ <= 0) {
    ret = OB_STATE_NOT_MATCH;
  } else if (final_artifact.finalized_) {
    LOG_WARN("IVF merge is already initialized", "ret", OB_INIT_TWICE, "operation",
        "ObIVFMergeProcessor::build_final_artifact", K(final_artifact.finalized_));
    ret = OB_INIT_TWICE;
  } else {
    final_artifact.selected_type_ = runtime_->selected_type_;
    final_artifact.effective_type_ = runtime_->effective_type_;
    final_artifact.fallback_reason_ = runtime_->fallback_reason_;
    MEMCPY(final_artifact.stage_elapsed_us_, runtime_->stage_elapsed_us_, sizeof(final_artifact.stage_elapsed_us_));
    const int64_t vector_count = runtime_->all_vids_.count();
    final_artifact.vector_count_ = vector_count;
    final_artifact.cluster_count_ = runtime_->final_cluster_count_;
    final_artifact.secondary_assignment_count_ =
        runtime_->secondary_assignment_count_;
    final_artifact.finalized_ = true;
    if (!final_artifact.is_valid()) {
      LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation",
          "ObIVFMergeProcessor::build_final_artifact");
      ret = OB_ERR_UNEXPECTED;
      final_artifact = ObIVFMergeArtifact();
    } else {
      // The artifact keeps the scalar count; no later FullBuild stage reads
      // the VID sequence after RAW_WRITE has crossed its final barrier.
      runtime_->all_vids_.reset();
    }
  }
  return ret;
}

int ObIVFMergeProcessor::export_centroid_layout(blocksstable::ObIvfCentroidLayout &layout) const
{
  int ret = OB_SUCCESS;
  if (runtime_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] parent clustering runtime is not initialized for layout export", K(ret));
  } else if (!runtime_->soar_finished_ || runtime_->c1_ == nullptr
      || runtime_->final_member_lists_ == nullptr || runtime_->active_plan_ != nullptr
      || runtime_->all_vids_.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] parent clustering is not ready to export layout", K(ret), KP_(runtime));
  } else if (OB_FAIL(layout.begin_grouped(*runtime_->c1_, runtime_->all_vids_.count(), runtime_->dimension_))) {
    LOG_WARN("[DISK_BBQ] failed to initialize exported parent centroid layout", K(ret),
        "leaf_count", runtime_->all_vids_.count(), "dimension", runtime_->dimension_);
  } else {
    const int64_t parents = layout.parent_count();
    if (runtime_->final_member_lists_->lists_.count() != parents
        || (runtime_->soar_member_lists_ != nullptr && runtime_->soar_member_lists_->lists_.count() != parents)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] parent membership list count mismatch", K(ret), K(parents),
          "primary_lists", runtime_->final_member_lists_->lists_.count(),
          "secondary_lists", runtime_->soar_member_lists_ == nullptr ? 0 : runtime_->soar_member_lists_->lists_.count());
    }
    for (int64_t kind = 0; OB_SUCC(ret) && kind < 2; ++kind) {
      const ObIVFHierarchyMemberLists *lists = kind == 0
          ? runtime_->final_member_lists_ : runtime_->soar_member_lists_;
      if (lists == nullptr) { continue; }
      for (int64_t p = 0; OB_SUCC(ret) && p < parents; ++p) {
        const ObIVFHierarchyVIDList *list = lists->lists_.at(p);
        if (list == nullptr) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] missing parent membership list", K(ret), K(kind), K(p), K(parents));
        }
        else {
          const auto &vids = get_vector_vids(*list, runtime_->all_vids_);
          for (int64_t i = 0; OB_SUCC(ret) && i < vids.count(); ++i) {
            if (vids.at(i) >= static_cast<uint64_t>(layout.leaf_count())) {
              ret = OB_INVALID_DATA;
              LOG_WARN("[DISK_BBQ] parent membership references invalid leaf", K(ret), K(kind), K(p),
                  K(i), "leaf", vids.at(i), "leaf_count", layout.leaf_count());
            }
            else if (OB_FAIL(layout.set_parent(static_cast<int64_t>(vids.at(i)), p, kind == 1))) {
              LOG_WARN("[DISK_BBQ] failed to export parent centroid membership", K(ret), K(kind), K(p), K(i),
                  "leaf", vids.at(i));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(layout.finish_grouped())) {
      LOG_WARN("[DISK_BBQ] failed to finish exported parent centroid layout", K(ret), K(parents));
    }
  }
  if (OB_FAIL(ret)) { layout.reset(); }
  return ret;
}

void ObIVFMergeProcessor::cancel(const int ret_code)
{
  UNUSED(ret_code);
  // One failed task can cancel while sibling tasks still read active_plan_ or
  // use algorithm-owned HGraph state. Publish only the atomic stop flag;
  // DagCtx destruction runs after task drain and owns actual teardown.
  if (OB_NOT_NULL(runtime_)) {
    (void)ATOMIC_BCAS(&runtime_->canceled_, 0, 1);
  }
}

int ObIVFMergeTestPeer::set_parent_build_threshold(ObIVFMergeProcessor &processor, int64_t threshold)
{
  int ret = OB_SUCCESS;
  if (processor.runtime_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] merge test runtime is not initialized for parent build threshold", K(ret), K(threshold));
  } else if (threshold <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid test parent build threshold", K(ret), K(threshold));
  } else if (processor.runtime_->parent_started_us_ != 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] test parent build threshold changed after preparation", K(ret), K(threshold),
        "parent_started_us", processor.runtime_->parent_started_us_);
  } else {
    processor.runtime_->parent_threshold_ = threshold;
  }
  return ret;
}

int ObIVFMergeTestPeer::finish_hierarchy_step(
    ObIVFMergeProcessor &processor,
    const ObIVFTaskPlan &task_plan,
    ObIVFMergeStepResult &step_result)
{
  int ret = OB_SUCCESS;
  ObIVFMergeRuntime *runtime = processor.runtime_;
  ObIVFMergeActivePlan *active_plan = OB_ISNULL(runtime)
      ? nullptr : runtime->active_plan_;
  if (!task_plan.is_valid() || OB_ISNULL(active_plan)
      || !is_valid_ivf_process_type(task_plan.get_process_type())
      || task_plan.get_plan_id() != active_plan->plan_id_
      || ObIVFMergeStage::HIERARCHY != active_plan->stage_
      || ObIVFMergeStep::HIERARCHY_UPDATE != active_plan->step_) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeTestPeer::finish_hierarchy_step", KP(active_plan), "task_plan_get_plan_id", task_plan.get_plan_id(),
        "active_plan_plan_id", (nullptr == active_plan) ? -1 : static_cast<int64_t>(active_plan->plan_id_),
        "active_plan_stage", (nullptr == active_plan) ? -1 : static_cast<int64_t>(active_plan->stage_),
        "active_plan_step", (nullptr == active_plan) ? -1 : static_cast<int64_t>(active_plan->step_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ret = processor.finish_step(
        active_plan->stage_,
        active_plan->step_,
        active_plan->depth_,
        active_plan->plan_id_,
        step_result);
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "processor.finish_step");
    }
  }
  return ret;
}

bool ObIVFMergeTestPeer::is_hierarchy_finished(
    const ObIVFMergeProcessor &processor)
{
  return OB_NOT_NULL(processor.runtime_)
      && processor.runtime_->hierarchy_finished_;
}

int64_t ObIVFMergeTestPeer::get_k(
    const ObIVFMergeProcessor &processor)
{
  return OB_ISNULL(processor.runtime_)
      ? 0 : processor.runtime_->hierarchy_k_;
}

int64_t ObIVFMergeTestPeer::get_actual_max_depth(
    const ObIVFMergeProcessor &processor)
{
  return OB_ISNULL(processor.runtime_)
      ? IVF_MERGE_INVALID_DEPTH : processor.runtime_->actual_max_depth_;
}

int64_t ObIVFMergeTestPeer::get_no_progress_leaf_count(
    const ObIVFMergeProcessor &processor)
{
  return OB_ISNULL(processor.runtime_)
      ? 0 : processor.runtime_->no_progress_leaf_count_;
}

int64_t ObIVFMergeTestPeer::get_no_progress_member_count(
    const ObIVFMergeProcessor &processor)
{
  return OB_ISNULL(processor.runtime_)
      ? 0 : processor.runtime_->no_progress_member_count_;
}

bool ObIVFMergeTestPeer::is_canceled(
    const ObIVFMergeProcessor &processor)
{
  return OB_NOT_NULL(processor.runtime_)
      && is_runtime_canceled(*processor.runtime_);
}

bool ObIVFMergeTestPeer::has_active_plan(
    const ObIVFMergeProcessor &processor)
{
  return OB_NOT_NULL(processor.runtime_)
      && OB_NOT_NULL(processor.runtime_->active_plan_);
}

bool ObIVFMergeTestPeer::root_uses_identity_view(
    const ObIVFMergeProcessor &processor)
{
  const ObIVFMergeRuntime *runtime = processor.runtime_;
  return OB_NOT_NULL(runtime) && 1 == runtime->current_nodes_.count()
      && OB_NOT_NULL(runtime->current_nodes_.at(0))
      && OB_NOT_NULL(runtime->current_nodes_.at(0)->vid_list_)
      && runtime->current_nodes_.at(0)->vid_list_->identity_;
}

int64_t ObIVFMergeTestPeer::get_root_owned_vid_count(
    const ObIVFMergeProcessor &processor)
{
  const ObIVFMergeRuntime *runtime = processor.runtime_;
  return OB_ISNULL(runtime) || 1 != runtime->current_nodes_.count()
      || OB_ISNULL(runtime->current_nodes_.at(0))
      || OB_ISNULL(runtime->current_nodes_.at(0)->vid_list_)
      ? -1 : runtime->current_nodes_.at(0)->vid_list_->vids_.count();
}

int64_t ObIVFMergeTestPeer::get_root_owned_vector_idx_count(
    const ObIVFMergeProcessor &processor)
{
  const ObIVFMergeRuntime *runtime = processor.runtime_;
  return OB_ISNULL(runtime) || 1 != runtime->current_nodes_.count()
      || OB_ISNULL(runtime->current_nodes_.at(0))
      || OB_ISNULL(runtime->current_nodes_.at(0)->vid_list_)
      ? -1 : runtime->current_nodes_.at(0)->vid_list_->vector_idxs_.count();
}

int ObIVFMergeTestPeer::get_root_vector_ref(
    const ObIVFMergeProcessor &processor,
    const int64_t offset,
    ObIVFVectorVID &vid,
    int64_t &vector_idx)
{
  int ret = OB_SUCCESS;
  const ObIVFMergeRuntime *runtime = processor.runtime_;
  if (OB_ISNULL(runtime) || 1 != runtime->current_nodes_.count()
      || OB_ISNULL(runtime->current_nodes_.at(0))
      || OB_ISNULL(runtime->current_nodes_.at(0)->vid_list_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_vector_ref(*runtime->current_nodes_.at(0)->vid_list_,
        runtime->all_vids_, offset, vid, vector_idx);
  }
  return ret;
}

int64_t ObIVFMergeTestPeer::get_member_count(
    const ObIVFMergeProcessor &processor,
    const int64_t global_cid)
{
  return OB_ISNULL(processor.runtime_) || global_cid < 0
      || global_cid >= processor.runtime_->initial_member_lists_.count()
      ? 0
      : processor.runtime_->initial_member_lists_.at(global_cid)->vids_.count();
}

int ObIVFMergeTestPeer::get_member_vid(
    const ObIVFMergeProcessor &processor,
    const int64_t global_cid,
    const int64_t member_idx,
    ObIVFVectorVID &vid)
{
  int ret = OB_SUCCESS;
  vid = 0;
  if (OB_ISNULL(processor.runtime_) || global_cid < 0
      || global_cid >= processor.runtime_->initial_member_lists_.count()) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeTestPeer::get_member_vid", KP(processor.runtime_), K(global_cid),
        "processor_runtime_initial_member_lists_count",
        (nullptr == processor.runtime_) ? -1 : static_cast<int64_t>(processor.runtime_->initial_member_lists_.count()),
        K(member_idx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObIVFHierarchyVIDList *vid_list =
        processor.runtime_->initial_member_lists_.at(global_cid);
    if (member_idx < 0 || member_idx >= vid_list->vids_.count()) {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
          "ObIVFMergeTestPeer::get_member_vid", K(member_idx), "vid_list_vids_count",
          (nullptr == vid_list) ? -1 : static_cast<int64_t>(vid_list->vids_.count()), K(global_cid));
      ret = OB_INVALID_ARGUMENT;
    } else {
      vid = vid_list->vids_.at(member_idx);
    }
  }
  return ret;
}

int ObIVFMergeTestPeer::get_center_value(
    const ObIVFMergeProcessor &processor,
    const int64_t global_cid,
    const int64_t dimension_idx,
    float &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  if (OB_ISNULL(processor.runtime_) || global_cid < 0
      || global_cid >= processor.runtime_->initial_member_lists_.count()
      || dimension_idx < 0 || dimension_idx >= processor.runtime_->dimension_) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeTestPeer::get_center_value", KP(processor.runtime_), K(global_cid),
        "processor_runtime_initial_member_lists_count",
        (nullptr == processor.runtime_) ? -1 : static_cast<int64_t>(processor.runtime_->initial_member_lists_.count()),
        K(dimension_idx), "processor_runtime_dimension",
        (nullptr == processor.runtime_) ? -1 : static_cast<int64_t>(processor.runtime_->dimension_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    value = processor.runtime_->c0_.at(
        global_cid * processor.runtime_->dimension_ + dimension_idx);
  }
  return ret;
}

ObIVFNeighborhoodMode ObIVFMergeTestPeer::choose_neighborhood_mode(
    const int64_t k)
{
  return oceanbase::storage::choose_neighborhood_mode(k);
}

bool ObIVFMergeTestPeer::is_merge_finished(
    const ObIVFMergeProcessor &processor)
{
  return OB_NOT_NULL(processor.runtime_)
      && processor.runtime_->merge_finished_;
}

int64_t ObIVFMergeTestPeer::get_final_k(
    const ObIVFMergeProcessor &processor)
{
  return OB_ISNULL(processor.runtime_)
      ? 0 : processor.runtime_->final_cluster_count_;
}

bool ObIVFMergeTestPeer::are_quant_intermediates_released(
    const ObIVFMergeProcessor &processor)
{
  const ObIVFMergeRuntime *runtime = processor.runtime_;
  return OB_NOT_NULL(runtime) && runtime->quant_finished_
      && OB_ISNULL(runtime->c1_)
      && OB_ISNULL(runtime->final_member_lists_)
      && OB_ISNULL(runtime->a1_)
      && OB_ISNULL(runtime->a2_)
      && OB_ISNULL(runtime->soar_member_lists_);
}

static int find_vector_idx_for_test(
    const ObIVFMergeRuntime &runtime,
    const ObIVFVectorVID vid,
    int64_t &vector_idx)
{
  int ret = OB_ENTRY_NOT_EXIST;
  vector_idx = -1;
  for (int64_t i = 0; i < runtime.all_vids_.count(); ++i) {
    if (runtime.all_vids_.at(i) == vid) {
      if (vector_idx >= 0) {
        LOG_WARN("unexpected IVF merge state", "ret", OB_ERR_UNEXPECTED, "operation", "find_vector_idx_for_test",
            K(vector_idx));
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      vector_idx = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObIVFMergeTestPeer::get_primary_assignment(
    const ObIVFMergeProcessor &processor,
    const ObIVFVectorVID vid,
    int64_t &cid)
{
  int ret = OB_SUCCESS;
  int64_t vector_idx = -1;
  cid = -1;
  if (OB_ISNULL(processor.runtime_)
      || OB_ISNULL(processor.runtime_->a1_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge argument", K(ret), "operation",
        "ObIVFMergeTestPeer::get_primary_assignment", KP(processor.runtime_), "processor_runtime_a1",
        static_cast<const void *>((nullptr == processor.runtime_) ? nullptr : processor.runtime_->a1_));
  } else if (OB_FAIL(find_vector_idx_for_test(
      *processor.runtime_, vid, vector_idx))) {
    LOG_WARN("failed to find IVF primary assignment for test", K(ret), K(vid));
  } else {
    cid = processor.runtime_->a1_->at(vector_idx);
  }
  return ret;
}

int ObIVFMergeTestPeer::get_secondary_assignment(
    const ObIVFMergeProcessor &processor,
    const ObIVFVectorVID vid,
    int64_t &cid)
{
  int ret = OB_SUCCESS;
  int64_t vector_idx = -1;
  cid = -1;
  if (OB_ISNULL(processor.runtime_)
      || OB_ISNULL(processor.runtime_->a2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge argument", K(ret), "operation",
        "ObIVFMergeTestPeer::get_secondary_assignment", KP(processor.runtime_), "processor_runtime_a2",
        static_cast<const void *>((nullptr == processor.runtime_) ? nullptr : processor.runtime_->a2_));
  } else if (OB_FAIL(find_vector_idx_for_test(
      *processor.runtime_, vid, vector_idx))) {
    LOG_WARN("failed to find IVF secondary assignment for test", K(ret), K(vid));
  } else {
    cid = processor.runtime_->a2_->at(vector_idx);
  }
  return ret;
}

int ObIVFMergeTestPeer::get_primary_assignment_by_vector_idx(
    const ObIVFMergeProcessor &processor,
    const int64_t vector_idx,
    int64_t &cid)
{
  int ret = OB_SUCCESS;
  cid = -1;
  if (OB_ISNULL(processor.runtime_) || OB_ISNULL(processor.runtime_->a1_)
      || vector_idx < 0 || vector_idx >= processor.runtime_->a1_->count()) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeTestPeer::get_primary_assignment_by_vector_idx", KP(processor.runtime_), "processor_runtime_a1",
        static_cast<const void *>((nullptr == processor.runtime_) ? nullptr : processor.runtime_->a1_), K(vector_idx),
        "processor_runtime_a1_count",
        (nullptr == processor.runtime_ || nullptr == processor.runtime_->a1_) ? -1 : static_cast<int64_t>(processor.runtime_->a1_->count()));
    ret = OB_INVALID_ARGUMENT;
  } else {
    cid = processor.runtime_->a1_->at(vector_idx);
  }
  return ret;
}

int ObIVFMergeTestPeer::get_secondary_assignment_by_vector_idx(
    const ObIVFMergeProcessor &processor,
    const int64_t vector_idx,
    int64_t &cid)
{
  int ret = OB_SUCCESS;
  cid = -1;
  if (OB_ISNULL(processor.runtime_) || vector_idx < 0
      || vector_idx >= processor.runtime_->all_vids_.count()) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeTestPeer::get_secondary_assignment_by_vector_idx", KP(processor.runtime_), K(vector_idx),
        "processor_runtime_all_vids_count",
        (nullptr == processor.runtime_) ? -1 : static_cast<int64_t>(processor.runtime_->all_vids_.count()));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(processor.runtime_->a2_)) {
    cid = -1;
  } else {
    cid = processor.runtime_->a2_->at(vector_idx);
  }
  return ret;
}

int64_t ObIVFMergeTestPeer::get_final_member_count(
    const ObIVFMergeProcessor &processor,
    const int64_t cid)
{
  return OB_ISNULL(processor.runtime_)
      || OB_ISNULL(processor.runtime_->final_member_lists_)
      || cid < 0
      || cid >= processor.runtime_->final_member_lists_->lists_.count()
      ? 0
      : processor.runtime_->final_member_lists_->lists_.at(cid)
          ->vids_.count();
}

int ObIVFMergeTestPeer::get_final_center_value(
    const ObIVFMergeProcessor &processor,
    const int64_t cid,
    const int64_t dimension_idx,
    float &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  if (OB_ISNULL(processor.runtime_)
      || OB_ISNULL(processor.runtime_->c1_)
      || OB_ISNULL(processor.runtime_->final_member_lists_)
      || cid < 0
      || cid >= processor.runtime_->final_member_lists_->lists_.count()
      || dimension_idx < 0
      || dimension_idx >= processor.runtime_->dimension_) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFMergeTestPeer::get_final_center_value", KP(processor.runtime_), "processor_runtime_c1",
        static_cast<const void *>((nullptr == processor.runtime_) ? nullptr : processor.runtime_->c1_),
        "processor_runtime_final_member_lists",
        static_cast<const void *>((nullptr == processor.runtime_) ? nullptr : processor.runtime_->final_member_lists_),
        K(cid), "processor_runtime_final_member_lists_lists_count",
        (nullptr == processor.runtime_ || nullptr == processor.runtime_->final_member_lists_) ? -1 : static_cast<int64_t>(processor.runtime_->final_member_lists_->lists_.count()),
        K(dimension_idx), "processor_runtime_dimension",
        (nullptr == processor.runtime_) ? -1 : static_cast<int64_t>(processor.runtime_->dimension_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    value = processor.runtime_->c1_->at(
        cid * processor.runtime_->dimension_ + dimension_idx);
  }
  return ret;
}


const char *ivf_merge_stage_name(ObIVFMergeStage stage)
{
  switch (stage) {
    case ObIVFMergeStage::PRIOR: return "PRIOR";
    case ObIVFMergeStage::HIERARCHY: return "HIERARCHY";
    case ObIVFMergeStage::NEIGHBORHOOD: return "NEIGHBORHOOD";
    case ObIVFMergeStage::REFINE: return "REFINE";
    case ObIVFMergeStage::SOAR: return "SOAR";
    case ObIVFMergeStage::CENTROID_INDEX: return "CENTROID_INDEX";
    case ObIVFMergeStage::QUANT: return "QUANT";
    case ObIVFMergeStage::RAW_WRITE: return "RAW_WRITE";
    case ObIVFMergeStage::FINISH: return "FINISH";
    default: return "INVALID";
  }
}

bool ObIVFMergeProcessor::quantizes_during_soar() const
{
  return runtime_ != nullptr && runtime_->ddl_runtime_ != nullptr
      && (ObIVFDDLRuntime::quantize_during_soar(*runtime_)
          || (physical_output_ != nullptr && uses_loaded_insertion(*runtime_, *physical_output_)));
}

ObIVFRefinePhase ObIVFMergeProcessor::refine_train_phase() const
{
  return runtime_ != nullptr && runtime_->ddl_runtime_ != nullptr
      ? runtime_->ddl_runtime_->refine_train_phase() : ObIVFRefinePhase::DISTANCE;
}

const char *ivf_merge_step_name(const ObIVFMergeStepPerf &perf)
{
  return perf.step_ == ObIVFMergeStep::REFINE_TRAIN && perf.refine_phase_ == ObIVFRefinePhase::ASSIGN
      ? "REFINE_TRAIN_ASSIGN"
      : perf.step_ == ObIVFMergeStep::REFINE_TRAIN && perf.refine_phase_ == ObIVFRefinePhase::UPDATE
      ? "REFINE_TRAIN_UPDATE"
      : perf.quantize_in_soar_ && perf.step_ == ObIVFMergeStep::SOAR_ASSIGN ? "SOAR_QUANT"
      : perf.quantize_in_soar_ && perf.step_ == ObIVFMergeStep::QUANT ? "QUANT_WRITE"
      : ivf_merge_step_name(perf.step_);
}

const char *ivf_merge_step_name(ObIVFMergeStep step)
{
  switch (step) {
    case ObIVFMergeStep::PRIOR_WORK: return "PRIOR_WORK";
    case ObIVFMergeStep::HIERARCHY_WARM_START: return "HIERARCHY_WARM_START";
    case ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE: return "HIERARCHY_SAMPLE_PREPARE";
    case ObIVFMergeStep::HIERARCHY_CLUSTER: return "HIERARCHY_CLUSTER";
    case ObIVFMergeStep::HIERARCHY_ASSIGN: return "HIERARCHY_ASSIGN";
    case ObIVFMergeStep::HIERARCHY_UPDATE: return "HIERARCHY_UPDATE";
    case ObIVFMergeStep::NEIGHBORHOOD_ADD: return "NEIGHBORHOOD_ADD";
    case ObIVFMergeStep::NEIGHBORHOOD_BUILD: return "NEIGHBORHOOD_BUILD";
    case ObIVFMergeStep::REFINE_SAMPLE_PREPARE: return "REFINE_SAMPLE_PREPARE";
    case ObIVFMergeStep::REFINE_TRAIN: return "REFINE_TRAIN";
    case ObIVFMergeStep::REFINE_ASSIGN: return "REFINE_ASSIGN";
    case ObIVFMergeStep::REFINE_UPDATE: return "REFINE_UPDATE";
    case ObIVFMergeStep::REFINE_COMPACT: return "REFINE_COMPACT";
    case ObIVFMergeStep::SOAR_ASSIGN: return "SOAR_ASSIGN";
    case ObIVFMergeStep::QUANT: return "QUANT";
    case ObIVFMergeStep::RAW_WRITE: return "RAW_WRITE";
    default: return "INVALID";
  }
}

int ObIVFMergeProcessor::get_work_perf(
    const int64_t task_idx, ObIVFMergeWorkPerf &perf) const
{
  int ret = OB_SUCCESS;
  perf = ObIVFMergeWorkPerf();
  const ObIVFMergeActivePlan *plan = nullptr == runtime_ ? nullptr : runtime_->active_plan_;
  if (nullptr == plan || task_idx < 0 || task_idx >= plan->work_items_.count()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (plan->stage_ == ObIVFMergeStage::CENTROID_INDEX && runtime_->parent_builder_ != nullptr) {
    ret = runtime_->parent_builder_->get_work_perf(task_idx, perf);
  } else {
    const ObIVFMergeWorkItem &item = plan->work_items_.at(task_idx);
    perf.node_idx_ = item.node_idx_;
    perf.center_idx_ = item.local_center_idx_;
    if (item.vid_begin_ >= 0) {
      perf.input_vectors_ = item.vid_end_ - item.vid_begin_;
    }
    if (plan->source_assign_ || nullptr != plan->source_update_) {
      perf.input_vectors_ = item.source_input_count_;
      // A physical window can span several parents; no single node/center.
      perf.node_idx_ = -1;
      perf.centers_ = -1;
    }
    if (item.cid_begin_ >= 0) {
      perf.centers_ = item.cid_end_ - item.cid_begin_;
    }
    if (item.node_idx_ >= 0 && item.node_idx_ < runtime_->current_nodes_.count()) {
      const ObIVFHierarchyNodeState *node = runtime_->current_nodes_.at(item.node_idx_);
      if (nullptr != node && nullptr != node->vid_list_) {
        if (ObIVFMergeStep::HIERARCHY_CLUSTER == plan->step_) {
          perf.input_vectors_ = get_vector_ref_count(*node->vid_list_, runtime_->all_vids_);
        }
        perf.centers_ = ObIVFMergeStep::HIERARCHY_CLUSTER == plan->step_ ? -1 : node->local_k_;
        if (ObIVFMergeStep::HIERARCHY_CLUSTER == plan->step_ && 0 == plan->depth_
            && nullptr != runtime_->first_level_samples_) {
          perf.first_level_samples_ = runtime_->first_level_samples_->get_sample_count();
        }
        if (ObIVFMergeStep::HIERARCHY_UPDATE == plan->step_
            && nullptr != node->member_lists_ && item.local_center_idx_ >= 0
            && item.local_center_idx_ < node->member_lists_->lists_.count()
            && nullptr != node->member_lists_->lists_.at(item.local_center_idx_)) {
          const int64_t member_count = node->member_lists_->lists_.at(item.local_center_idx_)->vids_.count();
          const bool skip_stats = can_skip_hierarchy_center_stats(member_count,
              get_vector_ref_count(*node->vid_list_, runtime_->all_vids_), runtime_->cluster_size_);
          perf.input_vectors_ = skip_stats ? 0 : member_count;
          perf.centers_ = skip_stats ? 0 : 1;
        }
      }
    }
    if (ObIVFMergeStep::REFINE_TRAIN == plan->step_
        || ObIVFMergeStep::REFINE_COMPACT == plan->step_) {
      perf.input_vectors_ = ObIVFMergeStep::REFINE_TRAIN == plan->step_
          && ((runtime_->ddl_runtime_ != nullptr || runtime_->selected_type_ == ObIVFProcessType::INITIAL_BUILD)
          && algorithm_->supports_batched_refine_train())
          ? item.vid_end_ - item.vid_begin_ : runtime_->all_vids_.count();
      perf.centers_ = runtime_->hierarchy_k_;
      if (plan->step_ == ObIVFMergeStep::REFINE_TRAIN
          && refine_train_phase() == ObIVFRefinePhase::UPDATE) {
        perf.input_vectors_ = -1;
        perf.centers_ = item.vid_end_ - item.vid_begin_;
      }
    } else if (ObIVFMergeStep::REFINE_UPDATE == plan->step_
        && nullptr != runtime_->refined_member_lists_ && item.local_center_idx_ >= 0
        && item.local_center_idx_ < runtime_->refined_member_lists_->lists_.count()
        && nullptr != runtime_->refined_member_lists_->lists_.at(item.local_center_idx_)) {
      perf.input_vectors_ = runtime_->ddl_runtime_ != nullptr ? 0
          : runtime_->refined_member_lists_->lists_.at(item.local_center_idx_)->vids_.count();
      perf.centers_ = 1;
    } else if (ObIVFMergeStep::QUANT == plan->step_
        && (plan->ddl_ != nullptr || uses_loaded_insertion(*runtime_, *physical_output_))) {
      if (item.cid_begin_ >= 0 || item.local_center_idx_ >= 0) { perf.input_vectors_ = 0; }
      else if (runtime_->a2_ != nullptr) {
        for (int64_t i = item.vid_begin_; i < item.vid_end_; ++i) {
          perf.secondary_vectors_ += runtime_->a2_->at(runtime_->ddl_runtime_->vector_index(i)) >= 0 ? 1 : 0;
        }
      }
    } else if (ObIVFMergeStep::QUANT == plan->step_
        && runtime_->final_member_lists_ != nullptr && item.cid_begin_ >= 0
        && item.cid_end_ <= runtime_->final_member_lists_->lists_.count()) {
      perf.centers_ = item.cid_end_ - item.cid_begin_;
      for (int64_t cid = item.cid_begin_; cid < item.cid_end_; ++cid) {
        if (runtime_->final_member_lists_->lists_.at(cid) != nullptr) {
          perf.input_vectors_ += runtime_->final_member_lists_->lists_.at(cid)->vids_.count();
        }
        if (runtime_->soar_member_lists_ != nullptr
            && cid < runtime_->soar_member_lists_->lists_.count()
            && runtime_->soar_member_lists_->lists_.at(cid) != nullptr) {
          perf.secondary_vectors_ += runtime_->soar_member_lists_->lists_.at(cid)->vids_.count();
        }
      }
    }
    const auto &slot = plan->result_slots_.at(task_idx);
    if (plan->step_ == ObIVFMergeStep::PRIOR_WORK && runtime_->effective_type_ == ObIVFProcessType::CONCATENATION) {
      perf.input_vectors_ = 0;
      perf.centers_ = item.cid_end_ - item.cid_begin_;
      if (slot.installed_) { perf.posting_rows_ = perf.reused_posting_rows_ = slot.primary_posting_count_; }
    } else if (plan->step_ == ObIVFMergeStep::SOAR_ASSIGN && uses_loaded_insertion(*runtime_, *physical_output_)) {
      perf.input_vectors_ = item.source_input_count_;
      if (slot.installed_) {
        perf.secondary_vectors_ = slot.secondary_posting_count_;
        perf.posting_rows_ = slot.primary_posting_count_ + slot.secondary_posting_count_;
        perf.reused_posting_rows_ = item.vid_begin_ < 0 ? slot.primary_posting_count_ : 0;
      }
    } else if (plan->step_ == ObIVFMergeStep::SOAR_ASSIGN && quantizes_during_soar() && slot.installed_) {
      perf.posting_rows_ = slot.center_idxs_ == nullptr ? 0 : slot.center_idxs_->count();
      if (slot.secondary_center_idxs_ != nullptr) {
        for (int64_t i = 0; i < slot.secondary_center_idxs_->count(); ++i) {
          perf.secondary_vectors_ += slot.secondary_center_idxs_->at(i) >= 0 ? 1 : 0;
        }
        perf.posting_rows_ += perf.secondary_vectors_;
      }
    } else if (plan->step_ == ObIVFMergeStep::QUANT && !quantizes_during_soar()
        && slot.installed_ && runtime_->final_member_lists_ != nullptr && item.cid_begin_ >= 0) {
      for (int64_t cid = item.cid_begin_; cid < item.cid_end_; ++cid) {
        perf.posting_rows_ += runtime_->final_member_lists_->lists_.at(cid)->vids_.count();
        if (runtime_->soar_member_lists_ != nullptr) {
          perf.posting_rows_ += runtime_->soar_member_lists_->lists_.at(cid)->vids_.count();
        }
        if (runtime_->insertion_prior_ != nullptr && runtime_->insertion_reuse_cids_.at(cid) >= 0) {
          perf.reused_posting_rows_ += runtime_->insertion_prior_->posting_mass_.at(runtime_->insertion_reuse_cids_.at(cid));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
