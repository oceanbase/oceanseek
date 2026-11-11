/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_SSTABLE_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_SSTABLE_WRITER_H_

#include "lib/lock/ob_mutex.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"

namespace oceanbase
{
namespace storage
{
class ObITabletSliceWriter;
}

namespace blocksstable
{
struct ObIVFPostingShard;

// Each output call returns its writer to the provider after append/close and
// row-count validation. DDL destroys the writer then; borrowed sinks may keep it.
class ObIVFDDLRawWriterProvider
{
public:
  virtual ~ObIVFDDLRawWriterProvider() = default;
  virtual int create_center_writer(storage::ObITabletSliceWriter *&writer)
  { writer = nullptr; return OB_NOT_SUPPORTED; }
  virtual int create_raw_writer(
      const int64_t task_idx, storage::ObITabletSliceWriter *&writer) = 0;
  virtual int create_posting_writer(int64_t task_idx, storage::ObITabletSliceWriter *&writer)
  { UNUSED(task_idx); writer = nullptr; return OB_NOT_SUPPORTED; }
  // The default is for externally owned sinks, such as compaction output.
  virtual void destroy_writer(storage::ObITabletSliceWriter *writer) { UNUSED(writer); }
};

// INITIAL_BUILD streams centers through slice 0 and sorted postings through
// disjoint CID slices. RAW slices follow the reserved posting slots and stream
// disjoint VID ranges. Publication follows the final RAW barrier.
class ObIVFDDLSSTableWriter final
    : public storage::ObIVFMergeQuantizer
{
public:
  explicit ObIVFDDLSSTableWriter(common::ObIAllocator &allocator);
  virtual ~ObIVFDDLSSTableWriter();
  int set_centroid_layout(const ObIvfCentroidLayout &layout) override;

  // Borrow an already initialized sink (compaction and recording callers).
  // It is not returned to the provider. DDL uses the factory-only overload below.
  int init(
      storage::ObITabletSliceWriter &ddl_writer,
      ObIVFDDLRawWriterProvider &raw_writer_provider,
      const int64_t snapshot_version,
      const int64_t vector_dim,
      const share::ObVectorIndexDistAlgorithm dist_type,
      const bool input_vectors_are_rotated = true);

  // Physical writers are created only when their output task starts and are
  // destroyed before it returns. The provider must outlive this object.
  int init(
      ObIVFDDLRawWriterProvider &writer_provider,
      const int64_t snapshot_version,
      const int64_t vector_dim,
      const share::ObVectorIndexDistAlgorithm dist_type,
      const bool input_vectors_are_rotated = true);

  bool supports_loaded_quantization() const override { return true; }
  int set_leaf_posting_counts(const common::ObIArray<uint32_t> &counts) override;
  int prepare_loaded_output(int64_t center_count, int64_t parallelism) override;
  // Encoded merge postings enter the same validated CID shards as freshly
  // encoded batches. Payloads are copied before this call returns.
  int append_loaded_postings(const common::ObIArray<storage::ObIVFBuildRow> &rows);
  int64_t posting_write_task_count() const override { return posting_shard_count_; }
  int write_posting_range(int64_t task_idx) override;
  int write_loaded_centers(const common::ObIArray<float> &centers,
      int64_t dimension, int64_t begin, int64_t end) override;
  int quantize_loaded_vectors(const common::ObIArray<float> &centers, int64_t dimension,
      const common::ObIArray<storage::ObIVFVectorVID> &vids, const common::ObIArray<float> &vectors,
      const common::ObIArray<int64_t> &primary, const common::ObIArray<int64_t> *secondary) override;
  virtual int quantize_cluster(
      storage::ObIVFVectorReader &reader,
      const common::ObIArray<float> &final_centers,
      const int64_t center_count,
      const int64_t cid,
      const common::ObIArray<storage::ObIVFVectorVID> &primary_member_vids,
      const common::ObIArray<storage::ObIVFVectorVID> *secondary_member_vids) override;
  virtual int finish_quantization(
      const int64_t vector_count,
      const int64_t center_count,
      const int64_t posting_count) override;
  virtual int write_raw_vectors(
      storage::ObIVFVectorReader &reader,
      const common::ObIArray<storage::ObIVFVectorVID> &all_vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      const int64_t task_idx) override;
  virtual int finish_raw_write(const int64_t vector_count) override;

  bool is_finished() const
  {
    return quantization_finished_ && raw_write_finished_;
  }
  int64_t get_row_count() const
  {
    return ATOMIC_LOAD(&output_row_count_);
  }
  int64_t get_one_bit_code_size() const { return one_bit_code_size_; }
  const ObIvfSSTableMeta &get_ivf_meta() const { return ivf_meta_; }

private:
  int append_partitioned_rows_(const common::ObIArray<storage::ObIVFBuildRow> &rows);
  int write_center_range_(const common::ObIArray<float> &centers, int64_t dim, int64_t begin, int64_t end);
  int append_ddl_row_(storage::ObITabletSliceWriter &writer, int64_t type, int64_t cid,
      storage::ObIVFVectorVID vid, const common::ObString &payload, ObDatumRow &row);
  int finish_partitioned_output_(int64_t vector_count, int64_t center_count, int64_t posting_count);
  int append_loaded_rows_(const common::ObIArray<storage::ObIVFBuildRow> &rows);
  // Called with sorter_lock_ held, only for nonempty global-sort output.
  int ensure_sorter_inited_();
  int append_sort_row_(
      const int64_t row_type,
      const int64_t cid,
      const storage::ObIVFVectorVID vid,
      const common::ObString &payload);
  int create_center_writer_(storage::ObITabletSliceWriter *&writer);
  int write_sorted_rows_to_ddl_(storage::ObITabletSliceWriter &writer);

private:
  const ObIvfCentroidLayout *centroid_layout_; // borrowed through QUANT
  common::ObIAllocator &allocator_;
  storage::ObITabletSliceWriter *ddl_writer_; // optional, externally owned sink
  ObIVFDDLRawWriterProvider *raw_writer_provider_;
  ObIVFPostingShard *posting_shards_[storage::ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT];
  int64_t posting_shard_count_;
  int64_t posting_center_count_;
  common::ObArray<uint32_t, common::ObWrapperAllocator> leaf_posting_counts_;
  bool centers_started_;
  bool centers_finished_;
  struct RawRange
  {
    RawRange() : begin_(-1), end_(-1), finished_(false) {}
    int64_t begin_;
    int64_t end_;
    bool finished_;
  };
  RawRange raw_ranges_[storage::ObIVFMergeProcessor::MAX_RAW_TASK_COUNT];
  int64_t snapshot_version_;
  int64_t vector_dim_;
  int64_t one_bit_code_size_;
  ObIvfSSTableRowDesc row_desc_;
  quantization::ObFhtKacRotator rotator_;
  ObIvfSSTableMeta ivf_meta_;
  storage::ObIVFSorter sorter_;
  uint64_t sorter_tenant_id_;
  bool sorter_inited_;
  // append_row() may synchronously spill/sort under memory pressure. DDL
  // tasks use a blocking mutex so waiters do not spin during that work.
  lib::ObMutex sorter_lock_;
  int64_t centroid_row_count_;
  int64_t closed_center_row_count_;
  int64_t one_bit_row_count_;
  int64_t raw_vector_row_count_;
  int64_t output_row_count_;
  int loaded_write_error_;
  int64_t sorter_wait_us_;
  int64_t sorter_append_us_;
  int64_t quant_encode_us_;
  bool is_inited_;
  bool input_vectors_are_rotated_;
  bool quantization_finished_;
  bool raw_write_finished_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLSSTableWriter);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_SSTABLE_WRITER_H_
