/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STATE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STATE_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_2d_array.h"
#include "lib/allocator/ob_allocator.h"
#include <cmath>
#include <utility>

namespace oceanbase
{
namespace storage
{
using common::ObIAllocator;
using common::ObIArray;
using common::ObArray;
using common::ObWrapperAllocator;
using common::ObFixedArray;

struct ObIVFHierarchyVIDList
{
  explicit ObIVFHierarchyVIDList(ObIAllocator &allocator)
    : vids_(), vector_idxs_(), identity_(false), sealed_(false)
  {
    vids_.set_block_allocator(ObWrapperAllocator(allocator));
    vector_idxs_.set_block_allocator(ObWrapperAllocator(allocator));
  }
  ObArray<ObIVFVectorVID, ObWrapperAllocator> vids_;
  ObArray<int64_t, ObWrapperAllocator> vector_idxs_;
  // The root covers all_vids_[0, N) in identity order. Keeping that relation
  // implicit avoids materializing another VID array and another dense-index
  // array for every vector. Child/member lists remain normally owned arrays.
  bool identity_;
  bool sealed_;
  TO_STRING_KV(K_(vids), K_(vector_idxs), K_(identity), K_(sealed));
};

struct ObIVFHierarchyMemberLists
{
  explicit ObIVFHierarchyMemberLists(ObIAllocator &allocator)
    : lists_()
  {
    lists_.set_block_allocator(ObWrapperAllocator(allocator));
  }
  ObArray<ObIVFHierarchyVIDList *, ObWrapperAllocator> lists_;
  TO_STRING_KV(K_(lists));
};

struct ObIVFHierarchyNodeState
{
  explicit ObIVFHierarchyNodeState(ObIAllocator &allocator)
    : vid_list_(nullptr),
      local_k_(0),
      local_centers_(),
      member_lists_(nullptr), origin_cid_(-1)
  {
    local_centers_.set_block_allocator(ObWrapperAllocator(allocator));
  }
  ObIVFHierarchyVIDList *vid_list_;
  int64_t local_k_;
  ObArray<float, ObWrapperAllocator> local_centers_;
  ObIVFHierarchyMemberLists *member_lists_;
  int64_t origin_cid_; // Dominant prior-local parent; inherited by split children.
  TO_STRING_KV(KP_(vid_list), K_(local_k), K_(local_centers),
      KP_(member_lists));
};

struct ObIVFMergeWorkItem
{
  ObIVFMergeWorkItem()
    : node_idx_(-1),
      vid_begin_(-1),
      vid_end_(-1),
      local_center_idx_(-1),
      cid_begin_(-1),
      cid_end_(-1),
      source_input_count_(0)
  {}
  int64_t node_idx_;
  int64_t vid_begin_;
  int64_t vid_end_;
  int64_t local_center_idx_;
  int64_t cid_begin_;
  int64_t cid_end_;
  int64_t source_input_count_;
  TO_STRING_KV(K_(node_idx), K_(vid_begin), K_(vid_end),
      K_(local_center_idx), K_(cid_begin), K_(cid_end));
};

struct ObIVFMergeResultSlot
{
  ObIVFMergeResultSlot()
    : installed_(false),
      primary_posting_count_(0), secondary_posting_count_(0),
      local_k_(0),
      center_values_(nullptr),
      center_idxs_(nullptr),
      secondary_center_idxs_(nullptr),
      neighbor_offsets_(nullptr),
      neighbor_cids_(nullptr),
      neighbor_max_distances_(nullptr)
  {}
  bool installed_;
  // Loaded INSERTION tasks validate relations without retaining posting VID lists.
  int64_t primary_posting_count_;
  int64_t secondary_posting_count_;
  int64_t local_k_;
  ObArray<float, ObWrapperAllocator> *center_values_;
  // One CID per vector in the corresponding work-item range. VID and the
  // dense vector index are derivable from that range, so retaining a 24-byte
  // {cid, vid, vector_idx} tuple per row is unnecessary.
  ObArray<int64_t, ObWrapperAllocator> *center_idxs_;
  ObArray<int64_t, ObWrapperAllocator> *secondary_center_idxs_;
  ObArray<int64_t, ObWrapperAllocator> *neighbor_offsets_;
  ObArray<int64_t, ObWrapperAllocator> *neighbor_cids_;
  ObArray<float, ObWrapperAllocator> *neighbor_max_distances_;
  TO_STRING_KV(K_(installed), K_(local_k), KP_(center_values),
      KP_(center_idxs), KP_(secondary_center_idxs),
      KP_(neighbor_offsets), KP_(neighbor_cids),
      KP_(neighbor_max_distances));
};

struct ObIVFSourceUpdate;
struct ObIVFDDLPlan;
struct ObIVFMergeActivePlan
{
  explicit ObIVFMergeActivePlan(ObIAllocator &allocator)
    : plan_id_(-1), started_at_us_(0), stage_(ObIVFMergeStage::INVALID),
      step_(ObIVFMergeStep::INVALID), depth_(IVF_MERGE_INVALID_DEPTH),
      source_assign_(false), source_update_(nullptr), ddl_(nullptr)
  {
    work_items_.set_block_allocator(ObWrapperAllocator(allocator));
    result_slots_.set_allocator(&allocator);
  }
  int64_t plan_id_;
  int64_t started_at_us_;
  ObIVFMergeStage stage_;
  ObIVFMergeStep step_;
  int64_t depth_;
  ObArray<ObIVFMergeWorkItem, ObWrapperAllocator> work_items_;
  ObFixedArray<ObIVFMergeResultSlot, ObIAllocator> result_slots_;
  bool source_assign_;
  ObIVFSourceUpdate *source_update_;
  ObIVFDDLPlan *ddl_; // Present only for INITIAL_BUILD source-order operations.
};

struct ObIVFHierarchyPendingLeaf
{
  ObIVFHierarchyPendingLeaf()
    : center_values_(nullptr), vid_list_(nullptr), origin_cid_(-1)
  {}
  ObArray<float, ObWrapperAllocator> *center_values_;
  ObIVFHierarchyVIDList *vid_list_;
  int64_t origin_cid_;
  TO_STRING_KV(KP_(center_values), KP_(vid_list), K_(origin_cid));
};

template <typename T, typename... Args>
int alloc_runtime_object(ObIAllocator &allocator, T *&object, Args&&... args)
{
  int ret = OB_SUCCESS;
  object = nullptr;
  void *buf = allocator.alloc(sizeof(T));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    object = new (buf) T(std::forward<Args>(args)...);
  }
  return ret;
}

template <typename T>
int alloc_runtime_array(
    ObIAllocator &allocator,
    ObArray<T, ObWrapperAllocator> *&array)
{
  int ret = alloc_runtime_object(allocator, array);
  if (OB_SUCC(ret)) {
    array->set_block_allocator(ObWrapperAllocator(allocator));
  }
  return ret;
}

template <typename T>
void free_runtime_object(ObIAllocator &allocator, T *&object)
{
  if (OB_NOT_NULL(object)) {
    object->~T();
    allocator.free(object);
    object = nullptr;
  }
}

inline const ObIArray<ObIVFVectorVID> &get_vector_vids(
    const ObIVFHierarchyVIDList &vid_list,
    const ObIArray<ObIVFVectorVID> &all_vids)
{
  return vid_list.identity_ ? all_vids : vid_list.vids_;
}

inline int64_t get_vector_ref_count(
    const ObIVFHierarchyVIDList &vid_list,
    const ObIArray<ObIVFVectorVID> &all_vids)
{
  return get_vector_vids(vid_list, all_vids).count();
}

inline bool can_skip_hierarchy_center_stats(
    const int64_t member_count,
    const int64_t parent_count,
    const int64_t cluster_size)
{
  // Non-leaf children retrain their centers at the next depth. A child that
  // contains the whole parent is a no-progress leaf and still needs its mean.
  return member_count > cluster_size && member_count < parent_count;
}

inline bool is_valid_vector_ref_list(
    const ObIVFHierarchyVIDList &vid_list,
    const ObIArray<ObIVFVectorVID> &all_vids)
{
  return vid_list.identity_
      ? vid_list.vids_.empty() && vid_list.vector_idxs_.empty()
          && !all_vids.empty()
      : vid_list.vids_.count() == vid_list.vector_idxs_.count();
}

inline int get_vector_ref(
    const ObIVFHierarchyVIDList &vid_list,
    const ObIArray<ObIVFVectorVID> &all_vids,
    const int64_t offset,
    ObIVFVectorVID &vid,
    int64_t &vector_idx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObIVFVectorVID> &vids = get_vector_vids(vid_list, all_vids);
  if (!is_valid_vector_ref_list(vid_list, all_vids)
      || offset < 0 || offset >= vids.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    vid = vids.at(offset);
    vector_idx = vid_list.identity_ ? offset : vid_list.vector_idxs_.at(offset);
    if (vector_idx < 0 || vector_idx >= all_vids.count()
        || all_vids.at(vector_idx) != vid) {
      ret = OB_INVALID_DATA;
      vid = 0;
      vector_idx = -1;
    }
  }
  return ret;
}

inline bool are_finite_center_values(const common::ObIArray<float> &values)
{
  bool are_finite = true;
  for (int64_t i = 0; are_finite && i < values.count(); ++i) {
    are_finite = std::isfinite(values.at(i));
  }
  return are_finite;
}
class ObIVFDDLRuntime;

class ObIVFParentCentroidBuilder;

struct ObIVFMergeRuntime
{
  explicit ObIVFMergeRuntime(ObIAllocator &allocator)
    : ddl_runtime_(nullptr), selected_type_(ObIVFProcessType::FULL_REBUILD),
      effective_type_(ObIVFProcessType::FULL_REBUILD),
      fallback_reason_(ObIVFQualityFallback::NONE),
      stage_elapsed_us_{},
      prior_(nullptr), prior_started_(false),
      vector_store_(nullptr),
      cluster_size_(0),
      dimension_(0),
      enable_soar_(true),
      all_vids_(), insertion_offsets_(), insertion_candidates_(),
      insertion_prior_(nullptr), insertion_old_count_(0), insertion_routed_vectors_(0),
      insertion_source_input_count_(0), insertion_reused_posting_count_(0),
      insertion_delta_vids_(), insertion_delta_idxs_(), insertion_origins_(),
      insertion_source_offsets_(), insertion_source_idxs_(),
      insertion_child_offsets_(), insertion_child_cids_(), insertion_reuse_cids_(), insertion_posting_counts_(),
      first_level_samples_(nullptr),
      current_nodes_(),
      active_plan_(nullptr),
      cluster_sample_plan_(nullptr),
      c0_(),
      initial_member_lists_(),
      a0_(nullptr),
      neighborhood_mode_(ObIVFNeighborhoodMode::INVALID),
      neighborhood_started_(false),
      neighborhood_add_finished_(false),
      neighborhood_finished_(false),
      g0_offsets_(nullptr),
      g0_cids_(nullptr),
      g0_max_distances_(nullptr),
      working_centers_(nullptr),
      refined_assignments_(nullptr),
      refined_member_lists_(nullptr),
      changed_cids_(nullptr),
      refine_skipped_(false),
      refine_has_changes_(false),
      refine_finished_(false),
      old_to_new_(nullptr),
      c1_(nullptr),
      centroid_layout_(allocator), centroid_route_reused_(false),
      parent_builder_(nullptr),
      parent_started_us_(0),
      parent_threshold_(0),
      final_member_lists_(nullptr),
      g1_offsets_(nullptr),
      g1_cids_(nullptr),
      g1_max_distances_(nullptr),
      a1_(nullptr),
      a2_(nullptr),
      soar_member_lists_(nullptr),
      final_cluster_count_(0),
      secondary_assignment_count_(0),
      soar_finished_(false),
      quant_finished_(false),
      merge_finished_(false),
      hierarchy_finished_(false),
      hierarchy_k_(0),
      actual_max_depth_(IVF_MERGE_INVALID_DEPTH),
      no_progress_leaf_count_(0),
      no_progress_member_count_(0),
      canceled_(0)
  {
    all_vids_.set_block_allocator(ObWrapperAllocator(allocator));
    concat_postings_.set_block_allocator(ObWrapperAllocator(allocator));
    concat_keep_.set_block_allocator(ObWrapperAllocator(allocator));
    concat_offsets_.set_block_allocator(ObWrapperAllocator(allocator));
    concat_counts_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_offsets_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_candidates_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_delta_vids_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_delta_idxs_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_origins_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_source_offsets_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_source_idxs_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_child_offsets_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_child_cids_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_reuse_cids_.set_block_allocator(ObWrapperAllocator(allocator));
    insertion_posting_counts_.set_block_allocator(ObWrapperAllocator(allocator));
    current_nodes_.set_block_allocator(ObWrapperAllocator(allocator));
    c0_.set_block_allocator(ObWrapperAllocator(allocator));
    initial_member_lists_.set_block_allocator(ObWrapperAllocator(allocator));
  }

  ObIVFDDLRuntime *ddl_runtime_;
  ObIVFProcessType selected_type_;
  ObIVFProcessType effective_type_;
  ObIVFQualityFallback fallback_reason_;
  int64_t stage_elapsed_us_[static_cast<int>(ObIVFMergeStage::MAX)];
  const ObIVFMergePrior *prior_;
  bool prior_started_;
  const ObIVFVectorStore *vector_store_;
  int64_t cluster_size_;
  int64_t dimension_;
  bool enable_soar_;
  ObArray<ObIVFVectorVID, ObWrapperAllocator> all_vids_;
  // Indices into prior_->postings_, sorted by source CID/VID after output-key
  // deduplication. Copy tasks validate every input relation, including losers.
  ObArray<int64_t, ObWrapperAllocator> concat_postings_;
  ObArray<uint8_t, ObWrapperAllocator> concat_keep_;
  ObArray<int64_t, ObWrapperAllocator> concat_offsets_;
  ObArray<uint32_t, ObWrapperAllocator> concat_counts_;
  ObArray<int64_t, ObWrapperAllocator> insertion_offsets_;
  ObArray<int64_t, ObWrapperAllocator> insertion_candidates_;
  const ObIVFMergePrior *insertion_prior_; // Separate from the retired PRIOR step state.
  int64_t insertion_old_count_;
  int64_t insertion_routed_vectors_;
  int64_t insertion_source_input_count_;
  int64_t insertion_reused_posting_count_;
  ObArray<ObIVFVectorVID, ObWrapperAllocator> insertion_delta_vids_;
  ObArray<int64_t, ObWrapperAllocator> insertion_delta_idxs_;
  ObArray<int64_t, ObWrapperAllocator> insertion_origins_;
  ObArray<int64_t, ObWrapperAllocator> insertion_source_offsets_;
  ObArray<int64_t, ObWrapperAllocator> insertion_source_idxs_;
  ObArray<int64_t, ObWrapperAllocator> insertion_child_offsets_;
  ObArray<int64_t, ObWrapperAllocator> insertion_child_cids_;
  ObArray<int64_t, ObWrapperAllocator> insertion_reuse_cids_;
  ObArray<int64_t, ObWrapperAllocator> insertion_posting_counts_;
  const ObIVFFirstLevelSampleSet *first_level_samples_;
  ObArray<ObIVFHierarchyNodeState *, ObWrapperAllocator> current_nodes_;
  ObIVFMergeActivePlan *active_plan_;
  ObIVFMergeActivePlan *cluster_sample_plan_;
  ObArray<float, ObWrapperAllocator> c0_;
  ObArray<ObIVFHierarchyVIDList *, ObWrapperAllocator> initial_member_lists_;
  ObArray<int64_t, ObWrapperAllocator> *a0_;
  ObIVFNeighborhoodMode neighborhood_mode_;
  bool neighborhood_started_;
  bool neighborhood_add_finished_;
  bool neighborhood_finished_;
  ObArray<int64_t, ObWrapperAllocator> *g0_offsets_;
  ObArray<int64_t, ObWrapperAllocator> *g0_cids_;
  ObArray<float, ObWrapperAllocator> *g0_max_distances_;
  ObArray<float, ObWrapperAllocator> *working_centers_;
  ObArray<int64_t, ObWrapperAllocator> *refined_assignments_;
  ObIVFHierarchyMemberLists *refined_member_lists_;
  ObArray<uint8_t, ObWrapperAllocator> *changed_cids_;
  bool refine_skipped_;
  bool refine_has_changes_;
  bool refine_finished_;
  ObArray<int64_t, ObWrapperAllocator> *old_to_new_;
  ObArray<float, ObWrapperAllocator> *c1_;
  blocksstable::ObIvfCentroidLayout centroid_layout_;
  bool centroid_route_reused_;
  ObIVFParentCentroidBuilder *parent_builder_;
  int64_t parent_started_us_;
  int64_t parent_threshold_;
  ObIVFHierarchyMemberLists *final_member_lists_;
  ObArray<int64_t, ObWrapperAllocator> *g1_offsets_;
  ObArray<int64_t, ObWrapperAllocator> *g1_cids_;
  ObArray<float, ObWrapperAllocator> *g1_max_distances_;
  ObArray<int64_t, ObWrapperAllocator> *a1_;
  ObArray<int64_t, ObWrapperAllocator> *a2_;
  ObIVFHierarchyMemberLists *soar_member_lists_;
  int64_t final_cluster_count_;
  int64_t secondary_assignment_count_;
  bool soar_finished_;
  bool quant_finished_;
  bool merge_finished_;
  bool hierarchy_finished_;
  int64_t hierarchy_k_;
  int64_t actual_max_depth_;
  int64_t no_progress_leaf_count_;
  int64_t no_progress_member_count_;
  int64_t canceled_;
};
inline bool is_runtime_canceled(const ObIVFMergeRuntime &runtime)
{
  return 0 != ATOMIC_LOAD(&runtime.canceled_);
}
inline int append_filled_byte_array(
    const int64_t count,
    const uint8_t value,
    common::ObIArray<uint8_t> &array)
{
  int ret = OB_SUCCESS;
  if (count < 0 || count > INT64_MAX - array.count()) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(array.reserve(array.count() + count))) {
    LOG_WARN("failed to reserve filled IVF byte array", K(ret), K(count),
        "existing_count", array.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ret = array.push_back(value);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STATE_H_
