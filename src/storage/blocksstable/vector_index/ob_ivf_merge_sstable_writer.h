/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_SSTABLE_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_SSTABLE_WRITER_H_

#include "storage/blocksstable/vector_index/ob_ivf_vector_ids.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"
#include "storage/blocksstable/vector_index/ob_ivf_block_reuse_source.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIVFBlockReuseWriter;
class ObDataStoreDesc;
class ObSSTableIndexBuilder;

// Physical output adapter shared by the FULL_REBUILD, CONCATENATION and
// INSERTION merge flows. It deliberately knows nothing about how the input
// was produced. Ordered CID ranges write postings through independent writers;
// a separate writer emits the centroid rows. Disjoint VID-ordered RAW ranges stream
// through independent macro writers. The current format writes only
// centroids, one-bit postings and raw vectors; exbits are outside this
// module's interface until their on-disk contract is supported.
class ObIVFMergeSSTableWriter final
    : public storage::ObIVFMergeQuantizer
{
public:
  static constexpr int64_t MAX_QUANT_TASK_COUNT = 16;
  static constexpr int64_t OUTPUT_WRITER_COUNT =
      1 + MAX_QUANT_TASK_COUNT + storage::ObIVFMergeProcessor::MAX_RAW_TASK_COUNT;
  explicit ObIVFMergeSSTableWriter(common::ObIAllocator &allocator);
  virtual ~ObIVFMergeSSTableWriter();
  bool supports_loaded_quantization() const override { return true; }
  bool supports_loaded_posting_reuse() const override { return true; }
  int copy_loaded_postings(const storage::ObIVFEncodedPostingSource &source,
      int64_t source_cid, int64_t target_cid,
      const storage::ObIVFVectorIDView &expected_vids) override;
  int prepare_loaded_output(int64_t center_count, int64_t parallelism) override;
  int64_t posting_write_task_count() const override;
  int write_posting_range(int64_t task_idx) override;
  int write_loaded_centers(const share::ObVectorMatrix &centers,
      int64_t dimension, int64_t begin, int64_t end) override;
  int quantize_loaded_vectors(const share::ObVectorMatrix &centers, int64_t dimension,
      const storage::ObIVFVectorIDView &vids, const share::ObVectorMatrix &vectors,
      const common::ObIArray<int64_t> &primary, const common::ObIArray<int64_t> *secondary) override;
  int set_centroid_layout(const ObIvfCentroidLayout &layout) override;
  int set_leaf_posting_counts(const common::ObIArray<uint32_t> &counts) override;
  int set_reuse_sources(const common::ObIArray<ObIVFBlockReuseSource *> &sources,
      bool is_full_merge = false);
  virtual bool supports_encoded_posting_reuse() const override { return true; }
  virtual int prepare_quantization(const share::ObVectorMatrix &centers,
      const int64_t center_count, int64_t &task_count) override;
  virtual int open_quantization_range(const int64_t cid_begin, const int64_t cid_end,
      const int64_t task_idx, common::ObIAllocator &allocator,
      storage::ObIVFMergeQuantizer *&output) override;
  virtual int close_quantization_range(storage::ObIVFMergeQuantizer *&output,
      const bool success) override;
  virtual int quantize_cluster_with_reuse(
      storage::ObIVFVectorReader &reader, const share::ObVectorMatrix &final_centers,
      const int64_t center_count, const int64_t cid,
      const storage::ObIVFVectorIDView &member_vids,
      const storage::ObIVFVectorIDView *secondary_member_vids,
      const storage::ObIVFEncodedPostingSource &source, const int64_t source_center_idx,
      const int64_t reused_prefix_count) override;

  // data_macro_seq_window is supplied by the merge ctx and excludes its final
  // index tree range. This adapter must not allocate another sequence namespace.
  // Input vectors and centroids must already be in the current FHT coordinate
  // space. Merge output never rotates its inputs.
  int init(
      const ObDataStoreDesc &data_store_desc,
      ObSSTableIndexBuilder &index_builder,
      const ObMacroSeqWindow &data_macro_seq_window,
      const int64_t snapshot_version,
      const int64_t vector_dim,
      const share::ObVectorIndexDistAlgorithm dist_type);

  virtual int quantize_cluster(
      storage::ObIVFVectorReader &reader,
      const share::ObVectorMatrix &final_centers,
      const int64_t center_count,
      const int64_t cid,
      const storage::ObIVFVectorIDView &primary_member_vids,
      const storage::ObIVFVectorIDView *secondary_member_vids) override;
  virtual int finish_quantization(
      const int64_t vector_count,
      const int64_t center_count,
      const int64_t posting_count) override;
  virtual int write_raw_vectors(
      storage::ObIVFVectorReader &reader,
      const storage::ObIVFVectorIDView &all_vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      const int64_t task_idx) override;
  virtual int finish_raw_write(const int64_t vector_count) override;

  bool is_finished() const
  {
    return quantization_finished_ && raw_write_finished_;
  }
  const common::ObIArray<uint32_t> &get_leaf_posting_counts() const { return leaf_posting_counts_; }
  int64_t get_row_count() const
  {
    return ATOMIC_LOAD(&output_row_count_);
  }
  int64_t get_one_bit_code_size() const { return one_bit_code_size_; }
  const ObIvfSSTableMeta &get_ivf_meta() const { return ivf_meta_; }

private:
  class LoadedOutput;
  class QuantizationRange;
  class ReusePostingConsumer;
  int quantize_cluster_(storage::ObIVFVectorReader &reader,
      const share::ObVectorMatrix &final_centers, const int64_t center_count,
      const int64_t cid, const storage::ObIVFVectorIDView &member_vids,
      const storage::ObIVFVectorIDView *secondary_member_vids,
      const storage::ObIVFEncodedPostingSource *source,
      const int64_t source_center_idx, const int64_t reused_prefix_count,
      QuantizationRange *range = nullptr);
  int finish_quantization_ranges_(int64_t vector_count, int64_t center_count, int64_t posting_count);
  int append_posting_(QuantizationRange *range, int64_t cid,
      storage::ObIVFVectorVID vid, const common::ObString &payload);
  static int build_macro_seq_window_(
      const ObMacroSeqWindow &data_window,
      const int64_t writer_idx,
      ObMacroSeqWindow &window);
  // Called under sorter_lock_ after validating a nonempty global output row.
  int ensure_sorter_inited_();
  int append_sort_row_(
      const int64_t row_type,
      const int64_t cid,
      const storage::ObIVFVectorVID vid,
      const common::ObString &payload);
  int open_writer_(ObIvfMacroBlockWriter &writer, const int64_t writer_idx);
  int write_sorted_rows_(ObIvfMacroBlockWriter &writer);
  int append_macro_row_(
      ObIvfMacroBlockWriter &writer,
      const storage::ObIVFSortItem &item,
      ObDatumRow &row, ObIVFBlockReuseWriter *reuse = nullptr);
  int prepare_posting_reuse_();
  int prepare_centroid_reuse_(const share::ObVectorMatrix &centers, int64_t dimension);
  const ObIVFBlockReuseSource *find_reuse_source_(int64_t prior_centroid_index) const;
  int normalize_reuse_ranges_(common::ObArray<ObIVFReuseRange, common::ObWrapperAllocator> &ranges);

private:
  const ObIvfCentroidLayout *centroid_layout_; // borrowed through QUANT
  common::ObIAllocator &allocator_;
  const ObDataStoreDesc *data_store_desc_;
  ObSSTableIndexBuilder *index_builder_;
  ObMacroSeqWindow data_macro_seq_window_;
  int64_t snapshot_version_;
  int64_t vector_dim_;
  int64_t one_bit_code_size_;
  ObIvfSSTableRowDesc row_desc_;
  ObIvfSSTableMeta ivf_meta_;
  storage::ObIVFSorter sorter_;
  bool sorter_inited_;
  common::ObSpinLock sorter_lock_;
  int64_t centroid_row_count_;
  int64_t one_bit_row_count_;
  int64_t raw_vector_row_count_;
  int64_t output_row_count_;
  int64_t reused_posting_count_;
  // Borrowed immutable centers stay alive through the QUANT barrier.
  const share::ObVectorMatrix *quant_centers_;
  common::ObArray<uint32_t, common::ObWrapperAllocator> leaf_posting_counts_;
  int64_t quant_range_count_;
  int64_t completed_quant_ranges_;
  int64_t quantized_center_count_;
  uint64_t started_quant_ranges_;
  // Reuses DDL encoding and posting sharding with merge-owned macro writers.
  // Encoded reuse ranges and loaded output are mutually exclusive.
  LoadedOutput *loaded_output_;
  // Slots retain input table identity; incompatible sources stay null. Objects
  // are owned by the input adapter until all output tasks have drained.
  common::ObArray<ObIVFBlockReuseSource *, common::ObWrapperAllocator> reuse_sources_;
  // Global prior ordinals: -1 means no source, -2 means not one complete source.
  common::ObArray<int64_t, common::ObWrapperAllocator> reuse_posting_origins_;
  common::ObArray<ObIVFReuseRange, common::ObWrapperAllocator> reuse_raw_ranges_;
  common::ObArray<ObIVFReuseRange, common::ObWrapperAllocator> reuse_posting_ranges_;
  common::ObArray<ObIVFReuseRange, common::ObWrapperAllocator> reuse_centroid_ranges_;
  bool is_inited_;
  bool quantization_finished_;
  bool raw_write_finished_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFMergeSSTableWriter);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_SSTABLE_WRITER_H_
