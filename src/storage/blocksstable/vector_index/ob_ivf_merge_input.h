/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_INPUT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_INPUT_H_

#include "storage/ob_i_table.h"
#include "lib/container/ob_array.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/ob_ivf_replay_memory_mgr.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include "storage/compaction/ob_compaction_memory_context.h"

namespace oceanbase
{
namespace storage { class ObDeletionBitmapHandle; }
namespace compaction
{

struct ObAsyncIndexMergeCtx;

// Retain source-table ownership for prior/posting validation and a sealed
// replay location for vector reads. Capture copies each live RAW payload once.
struct ObAsyncIVFSourceLocator
{
  ObAsyncIVFSourceLocator() : vid_(0), table_idx_(-1), slice_idx_(-1), row_id_(-1) {}
  ObAsyncIVFSourceLocator(const storage::ObIVFVectorVID vid, const int64_t table_idx,
      const int64_t slice_idx, const int64_t row_id)
    : vid_(vid), table_idx_(table_idx), slice_idx_(slice_idx), row_id_(row_id)
  {}
  // VID 0 is a valid heap-table row identity.
  bool is_valid() const { return table_idx_ >= 0 && slice_idx_ >= 0 && row_id_ >= 0; }
  TO_STRING_KV(K_(vid), K_(table_idx), K_(slice_idx), K_(row_id));

  storage::ObIVFVectorVID vid_;
  int64_t table_idx_;
  int64_t slice_idx_;
  int64_t row_id_;
};

// Production input shared by all strategies for the IVF SSTables selected by the merge
// range. Refresh RAW vectors are already in the persisted FHT coordinate
// space, so this adapter exposes them without rotating them again. Sampling
// runs alongside RAW capture using the shared bottom-K sampler.
class ObIVFMergeInputAdapter final
    : public storage::ObIVFVectorStore,
      public storage::ObIVFFirstLevelSampleSet,
      public storage::ObIVFEncodedPostingSource,
      public storage::ObIVFCentroidSource
{
public:
  // The compaction context, when supplied, must outlive the input adapter.
  explicit ObIVFMergeInputAdapter(common::ObIAllocator &allocator,
      ObCompactionMemoryContext *memory_context = nullptr);
  virtual ~ObIVFMergeInputAdapter();

  int init(ObAsyncIndexMergeCtx &merge_ctx);
  int search_centroids(int64_t source_segment_idx, const float *query,
      int64_t dimension, int32_t *candidates, int64_t capacity, int64_t &count) const override;
  virtual int scan_postings(const int64_t source_center_idx,
      storage::ObIVFEncodedPostingConsumer &consumer) const override;
  const storage::ObIVFMergePrior &get_prior() const { return prior_; }
  // A successfully scanned, empty live set must still replace the input range.
  bool is_empty() const { return is_inited_ && locators_.empty(); }
  const storage::ObIVFMergeInput &get_merge_input() const
  {
    return merge_input_;
  }

  virtual bool is_valid() const override;
  virtual int64_t get_vector_count() const override
  {
    return locators_.count();
  }
  virtual int64_t get_dimension() const override { return vector_dim_; }
  virtual bool supports_source_order() const override { return is_valid(); }
  virtual bool is_fully_memory_resident() const override;
  virtual int64_t get_source_read_parallelism() const override { return replay_inputs_.count(); }
  virtual int64_t get_available_replay_memory() const override;
  virtual int get_source_row_idx(const int64_t vector_idx, int64_t &source_idx) const override;
  virtual int batch_get_by_source(const common::ObIArray<int64_t> &vector_idxs,
      common::ObIArray<float> &vectors) const override;
  virtual int get_all_vids(
      common::ObIArray<storage::ObIVFVectorVID> &vids) const override;
  virtual int batch_get_by_vid(
      const common::ObIArray<storage::ObIVFVectorVID> &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<float> &vectors) const override;

  virtual int64_t get_sample_count() const override
  {
    return sample_locators_.count();
  }
  virtual int batch_get_samples(
      const int64_t sample_begin,
      const int64_t sample_end,
      common::ObIArray<storage::ObIVFVectorVID> &sample_vids,
      common::ObIArray<float> &sample_vectors) const override;

  TO_STRING_KV(K_(is_inited), K_(index_table_id), K_(index_tablet_id),
      K_(vector_dim), K_(dist_type),
      "vector_count", locators_.count(),
      "sample_count", sample_locators_.count());

private:
  friend class ObIVFMergeCaptureCtx;
  friend class ObIVFMergeDagCtx;
  int prepare_source_(ObAsyncIndexMergeCtx &merge_ctx);
  int finish_source_();
  int prepare_schema_and_meta_(ObAsyncIndexMergeCtx &merge_ctx);
  int init_source_(ObAsyncIndexMergeCtx &merge_ctx);
  int capture_prior_(ObAsyncIndexMergeCtx &merge_ctx);
  int reserve_prior_arrays_(int64_t &centroid_count, int64_t &posting_limit);
  int finish_sample_locators_();
  void release_first_level_samples();
  // Drop only membership detail; centers and counts remain available for reuse.
  void release_prior_postings();
  // Only after the terminal build barrier. The adapter object stays alive for
  // borrowed runtime views; its caller can then reset the dedicated input arena.
  void release_input_buffers();
  int init_replay_inputs_(const int64_t slice_count);
  int find_locator_(
      const storage::ObIVFVectorVID vid,
      const ObAsyncIVFSourceLocator *&locator) const;
  int find_locator_(
      const storage::ObIVFVectorVID vid,
      const ObAsyncIVFSourceLocator *&locator,
      int64_t &cursor) const;
  int read_replay_vectors_(
      const common::ObIArray<ObAsyncIVFSourceLocator> &locators,
      common::ObIArray<float> &vectors) const;
private:
  // Shared by RAW capture and the adapter's prior/posting scans.
  static constexpr int64_t IVF_CAPTURE_YIELD_INTERVAL = 4096;

  common::ObIAllocator &allocator_;
  // Stores must be destroyed before releasing the tenant-wide replay budget.
  share::ObIVFReplayMemoryReservation replay_memory_reservation_;
  common::ObArray<storage::ObIVFReplayInput *, common::ObWrapperAllocator> replay_inputs_;
  common::ObArray<int64_t, common::ObWrapperAllocator> replay_row_offsets_;
  ObLocalArena prior_posting_allocator_;
  storage::ObIVFMergePrior prior_;
  common::ObArray<ObAsyncIVFSourceLocator, common::ObWrapperAllocator>
      locators_;
  ObLocalArena sample_heap_allocator_;
  ObLocalArena sample_locator_allocator_;
  common::ObArray<ObAsyncIVFSourceLocator, common::ObWrapperAllocator>
      sample_locators_;
  storage::ObIVFSampleCandidateMaxCmp sample_cmp_;
  storage::ObIVFSampleHeap sample_heap_;
  uint64_t sample_seed_;
  storage::ObIVFMergeInput merge_input_;
  bool is_inited_;
  bool samples_released_;
  uint64_t index_table_id_;
  common::ObTabletID index_tablet_id_;
  int64_t vector_dim_;
  int64_t cluster_size_;
  const storage::ObDeletionBitmapHandle *deletions_;
  int64_t max_sample_count_;
  share::ObVectorIndexDistAlgorithm dist_type_;
  blocksstable::ObIvfSSTableRowDesc row_desc_;
  storage::ObTablesHandleArray source_tables_;
  share::ObLSID ls_id_;
  const storage::ObITableReadInfo *read_info_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFMergeInputAdapter);
};

} // namespace compaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_INPUT_H_
