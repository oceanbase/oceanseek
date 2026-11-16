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
#include "storage/blocksstable/vector_index/ob_ivf_block_reuse_source.h"

namespace oceanbase
{
namespace storage { class ObDeletionBitmapHandle; }
namespace compaction
{

struct ObAsyncIndexMergeCtx;

// Retain source-table ownership and a stable source-order location. Payloads
// come from sealed replay stores or, for certified metadata-first input, from
// the pinned source SSTable at the same merge-reader visibility window.
struct ObAsyncIVFSourceLocator
{
  ObAsyncIVFSourceLocator() : vid_(0), table_idx_(-1), slice_idx_(-1), row_id_(-1) {}
  ObAsyncIVFSourceLocator(const storage::ObIVFVectorVID vid, const int64_t table_idx,
      const int64_t slice_idx, const int64_t row_id)
    : vid_(vid),
      table_idx_(table_idx >= 0 && table_idx <= INT32_MAX ? static_cast<int32_t>(table_idx) : -1),
      slice_idx_(slice_idx >= 0 && slice_idx <= INT32_MAX ? static_cast<int32_t>(slice_idx) : -1),
      row_id_(row_id)
  {}
  // VID 0 is a valid heap-table row identity.
  bool is_valid() const { return table_idx_ >= 0 && slice_idx_ >= 0 && row_id_ >= 0; }
  TO_STRING_KV(K_(vid), K_(table_idx), K_(slice_idx), K_(row_id));

  storage::ObIVFVectorVID vid_;
  int32_t table_idx_;
  int32_t slice_idx_;
  int64_t row_id_;
};

// A contiguous, reclaiming buffer whose ownership can pass from a capture
// slice to the input without copying every locator. All buffers in one capture
// borrow the input's allocator, which outlives its slices.
class ObIVFSourceLocators final
{
public:
  explicit ObIVFSourceLocators(common::ObIAllocator &allocator, ObCompactionMemoryContext *memory_context = nullptr)
    : allocator_(allocator), memory_context_(memory_context), data_(nullptr), count_(0), capacity_(0) {}
  ~ObIVFSourceLocators() { reset(); }
  int64_t count() const { return count_; }
  bool empty() const { return count_ == 0; }
  ObCompactionMemoryContext *get_memory_context() const { return memory_context_; }
  int64_t get_capacity() const { return capacity_; }
  int64_t get_data_size() const { return capacity_ * sizeof(ObAsyncIVFSourceLocator); }
  ObAsyncIVFSourceLocator *begin() { return data_; }
  ObAsyncIVFSourceLocator *end() { return count_ == 0 ? data_ : data_ + count_; }
  const ObAsyncIVFSourceLocator *begin() const { return data_; }
  const ObAsyncIVFSourceLocator *end() const { return count_ == 0 ? data_ : data_ + count_; }
  ObAsyncIVFSourceLocator &at(int64_t idx) { OB_ASSERT(idx >= 0 && idx < count_); return data_[idx]; }
  const ObAsyncIVFSourceLocator &at(int64_t idx) const
  { OB_ASSERT(idx >= 0 && idx < count_); return data_[idx]; }
  int reserve(int64_t capacity);
  int prepare_allocate(int64_t count);
  int push_back(const ObAsyncIVFSourceLocator &locator);
  int take_over(ObIVFSourceLocators &source);
  void reset();
private:
  common::ObIAllocator &allocator_;
  ObCompactionMemoryContext *memory_context_;
  ObAsyncIVFSourceLocator *data_;
  int64_t count_;
  int64_t capacity_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFSourceLocators);
};

// Production input shared by all strategies for the IVF SSTables selected by the merge
// range. Refresh RAW vectors are already in the persisted FHT coordinate
// space, so this adapter exposes them without rotating them again. Sampling
// retains bottom-K key locators; metadata-first input loads their payloads only
// when requested and does not reserve a replay cache or create spill files.
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
  bool uses_metadata_counts() const { return metadata_counts_valid_; }
  bool uses_lazy_raw() const { return lazy_raw_; }
  int64_t lazy_raw_read_rows() const { return ATOMIC_LOAD(&lazy_raw_read_rows_); }
  storage::ObIVFProcessType metadata_strategy() const { return metadata_strategy_; }
  int prepare_block_reuse();
  // Requires processor.centroid_route_reused() and successful publication.
  // Best effort: missing caches keep the existing lazy-loading path.
  void reuse_centroid_cache(const blocksstable::ObIvfSSTable &output,
      const common::ObIArray<uint32_t> &posting_counts) const;
  const common::ObIArray<blocksstable::ObIVFBlockReuseSource *> &get_reuse_sources() const
  { return reuse_sources_; }
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
  virtual int64_t get_source_read_parallelism() const override
  { return lazy_raw_ ? MAX(0L, replay_row_offsets_.count() - 1) : replay_inputs_.count(); }
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
  int prepare_metadata_();
  int read_source_vectors_(const common::ObIArray<ObAsyncIVFSourceLocator> &locators,
      common::ObIArray<float> &vectors) const;
  int finish_source_();
  int prepare_schema_and_meta_(ObAsyncIndexMergeCtx &merge_ctx);
  int init_source_(ObAsyncIndexMergeCtx &merge_ctx);
  int capture_prior_(ObAsyncIndexMergeCtx &merge_ctx);
  int reserve_prior_arrays_(int64_t &centroid_count, int64_t &posting_limit);
  int retain_reuse_centroid_(blocksstable::ObIVFBlockReuseSource &source,
      const blocksstable::ObIvfSSTableMeta &meta,
      const blocksstable::ObIVFReuseKey &key, uint32_t count, const float *vector, int64_t payload_size);
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
  // Dense offsets for capture slices, retained also in lazy mode where there
  // are no replay stores. Source-order reads map through these key locations.
  common::ObArray<int64_t, common::ObWrapperAllocator> replay_row_offsets_;
  ObLocalArena prior_posting_allocator_;
  storage::ObIVFMergePrior prior_;
  common::ObArray<blocksstable::ObIVFBlockReuseSource *, common::ObWrapperAllocator> reuse_sources_;
  storage::ObIVFMergeRuntimeAllocator locator_allocator_;
  ObIVFSourceLocators locators_;
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
  bool enable_metadata_;
  bool metadata_counts_valid_;
  bool lazy_raw_;
  mutable int64_t lazy_raw_read_rows_;
  storage::ObIVFProcessType metadata_strategy_;
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
