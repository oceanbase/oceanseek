/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_MERGE_POLICY_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_MERGE_POLICY_H_

#include "lib/container/ob_se_array.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase
{
namespace share
{
struct ObVecMergeConfig
{
  static constexpr int64_t MERGE_CAPACITY_PERCENT = 120;
  static constexpr int64_t TARGET_MIN_GROWTH_PERCENT = 20;
  int64_t max_vectors_ = 10000000; // Target size; ordinary merges may grow to merge_capacity().
  int64_t merge_factor_ = 2; // Tier logarithm base and minimum candidate positions, not a window limit.
  int64_t delete_percentage_ = 20;
  int64_t idle_us_ = 24L * 60 * 60 * 1000000;
  int64_t merge_capacity() const
  {
    // vector_index_segment_max_vectors is limited to 50,000,000.
    return max_vectors_ > 0 ? max_vectors_ * MERGE_CAPACITY_PERCENT / 100 : 0;
  }
};

struct ObVecSegmentMergeInfo
{
  int64_t vector_count_ = 0;
  int64_t deleted_vector_count_ = 0;
  int64_t scn_ = 0;
  bool readable_ = false;
  ObVectorIndexSegmentType seg_type_ = ObVectorIndexSegmentType::INVALID;
  int64_t live_vector_count() const { return MAX(vector_count_ - deleted_vector_count_, 0); }
  TO_STRING_KV(K_(vector_count), K_(deleted_vector_count), K_(scn), K_(readable), K_(seg_type));
};

struct ObVecMergePolicyInput
{
  common::ObSEArray<ObVecSegmentMergeInfo, 8> segments_;
  int64_t base_count_ = 0;
  // True even when async targets are unavailable or have no usable publication point.
  bool has_async_index_ = false;
  // Minimum safe publication SCN. Zero forbids reclamation when has_async_index_ is true;
  // ignored when has_async_index_ is false, since no async visibility constraint applies.
  int64_t async_published_scn_ = 0;
  int64_t deleted_vid_count_ = 0;
};

struct ObVecMergePlan
{
  enum MergeStrategy {
    TIERED_MERGE,
    DELETE_MERGE,
    RESIDUAL_MERGE,
    MANUAL_MERGE,
    FULL_MERGE,
    TARGET_MERGE,
    FREEZE_REBUILD
  };
  int64_t begin_ = -1;
  int64_t end_ = -1;
  bool output_base_ = false;
  // INVALID until a merge plan is selected; also determines bitmap reclamation.
  ObVectorIndexSegmentType output_type_ = ObVectorIndexSegmentType::INVALID;
  MergeStrategy strategy_ = TIERED_MERGE;
  // Sum live vector counts from the same input snapshot that produced this plan.
  int get_vector_count(const ObVecMergePolicyInput &input, int64_t &vector_count) const;
  TO_STRING_KV(K_(begin), K_(end), K_(output_base), K_(output_type), K_(strategy));
};

// Select positions in the immutable Base + Incr snapshot supplied by the caller.
// This class does not access indexes, SQL, bitmaps or tenant memory reservations.
class ObVectorIndexMergePolicy
{
public:
  explicit ObVectorIndexMergePolicy(const ObVecMergeConfig &config) : config_(config) {}
  // AUTO scheduling uses cached per-segment statistics.
  // A successful check with need_task=true returns a full plan.
  int check_need_merge_task(const ObVecMergePolicyInput &input,
      bool &need_task, ObVecMergePlan &plan) const;
  // Select windows from the supplied statistics; no global-delete fallback.
  int find_merge_segments(const ObVecMergePolicyInput &input, const ObVecIndexAsyncTaskTriggerType trigger,
      ObVecMergePlan &plan) const;
private:
  int validate_(const ObVecMergePolicyInput &input) const;
  int find_target_merge_(const ObVecMergePolicyInput &input, ObVecMergePlan &plan,
      const bool for_freeze_rebuild = false) const;
  int find_tiered_merge_(const ObVecMergePolicyInput &input,
      ObVecMergePlan &plan, const bool for_freeze_rebuild = false) const;
  int find_delete_merge_(const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const;
  int find_residual_merge_(const ObVecMergePolicyInput &input,
      const int64_t now_us, ObVecMergePlan &plan, const bool for_freeze_rebuild = false) const;
  int find_freeze_rebuild_(const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const;
  int find_manual_merge_(const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const;
  bool pick_window_(const ObVecMergePolicyInput &input,
      int64_t start, const int64_t stop, const int64_t required,
      ObVecMergePlan &plan, const bool for_freeze_rebuild = false) const;
  bool can_reclaim_insert_(const ObVecMergePolicyInput &input,
      const int64_t begin, const int64_t end, const bool for_freeze_rebuild = false) const;
  bool can_merge_window_(const ObVecMergePolicyInput &input,
      const int64_t begin, const int64_t end) const;
  void determine_output_role_(const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const;
  static bool reaches_threshold_(const uint64_t deleted, const uint64_t count,
      const uint64_t percentage);
  ObVecMergeConfig config_;
};
} // namespace share
} // namespace oceanbase
#endif
