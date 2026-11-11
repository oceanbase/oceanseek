/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_STRATEGY_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_STRATEGY_H_

#include "lib/container/ob_array.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "storage/blocksstable/vector_index/ob_ivf_task_plan.h"

namespace oceanbase
{
namespace storage
{
class ObIVFEncodedPostingSource;

enum class ObIVFQualityFallback : uint8_t { NONE, PRIOR_DEGENERATE, ASSIGNMENT_DEGENERATE };

struct ObIVFSegmentPriorInfo
{
  ObIVFSegmentPriorInfo()
    : live_vector_count_(0), centroid_begin_(0), centroid_count_(0), provides_prior_(false) {}
  int64_t live_vector_count_;
  int64_t centroid_begin_;
  int64_t centroid_count_;
  bool provides_prior_;
  TO_STRING_KV(K_(live_vector_count), K_(centroid_begin), K_(centroid_count), K_(provides_prior));
};

// Immutable after input capture. Mass counts live postings (including SOAR),
// whereas vector_count and coverage count unique live RAW vectors.
struct ObIVFPriorPosting
{
  ObIVFPriorPosting() : vid_(0), centroid_idx_(-1) {}
  ObIVFPriorPosting(uint64_t vid, int64_t centroid_idx) : vid_(vid), centroid_idx_(centroid_idx) {}
  uint64_t vid_;
  int64_t centroid_idx_; // Index in the concatenated prior array, not a table-local CID.
  TO_STRING_KV(K_(vid), K_(centroid_idx));
};

struct ObIVFMergePrior
{
  explicit ObIVFMergePrior(common::ObIAllocator &allocator)
    : dimension_(0), vector_count_(0), covered_vector_count_(0), dominant_segment_(-1),
      segments_(), centroids_(), old_cids_(), posting_mass_(), postings_(), postings_complete_(false),
      posting_source_(nullptr)
  {
    segments_.set_block_allocator(common::ObWrapperAllocator(allocator));
    centroids_.set_block_allocator(common::ObWrapperAllocator(allocator));
    old_cids_.set_block_allocator(common::ObWrapperAllocator(allocator));
    posting_mass_.set_block_allocator(common::ObWrapperAllocator(allocator));
    postings_.set_block_allocator(common::ObWrapperAllocator(allocator));
  }
  int validate() const;
  bool can_concatenate() const
  {
    return postings_complete_ && posting_source_ != nullptr && !posting_mass_.empty()
        && covered_vector_count_ == vector_count_;
  }
  int64_t dimension_;
  int64_t vector_count_;
  int64_t covered_vector_count_;
  int64_t dominant_segment_;
  common::ObArray<ObIVFSegmentPriorInfo, common::ObWrapperAllocator> segments_;
  common::ObArray<float, common::ObWrapperAllocator> centroids_;
  common::ObArray<int64_t, common::ObWrapperAllocator> old_cids_;
  common::ObArray<int64_t, common::ObWrapperAllocator> posting_mass_;
  common::ObArray<ObIVFPriorPosting, common::ObWrapperAllocator> postings_;
  bool postings_complete_;
  // Borrowed for the DAG lifetime. Null preserves the original merge path.
  const ObIVFEncodedPostingSource *posting_source_;
  TO_STRING_KV(K_(dimension), K_(vector_count), K_(covered_vector_count), K_(dominant_segment),
      K_(segments), "prior_count", posting_mass_.count());
};

struct ObIVFMergeStrategy
{
  static constexpr int64_t MIN_PRIOR_COUNT = 32;
  static constexpr int64_t SMALL_INPUT_CLUSTER_COUNT = 4;
  static constexpr double INSERTION_DOMINANT_RATIO = 0.8;
  static bool should_capture_prior(int64_t vector_count, int64_t cluster_size);
  static int select(const ObIVFMergePrior &prior, const int64_t cluster_size,
      ObIVFProcessType &process_type);
};

} // namespace storage
} // namespace oceanbase
#endif
