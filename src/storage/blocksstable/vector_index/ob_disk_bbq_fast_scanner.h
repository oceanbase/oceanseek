/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_FAST_SCANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_FAST_SCANNER_H_

#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_fast_scan.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"
#include "storage/blocksstable/vector_index/ob_ivf_fastscan_micro_block_transformer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIvfSSTable;

// One Extended-RaBitQ-style block of approximate results. Keeping the storage
// inline avoids allocating while repeatedly scanning posting-list blocks.
class ObDistBBQFastScanBatch final
{
public:
  static constexpr int64_t CAPACITY =
      quantization::ObRaBitQFastScanConstants::BATCH_SIZE;

  ObDistBBQFastScanBatch() : count_(0), scan_count_(0), items_() {}
  void reset() { count_ = 0; scan_count_ = 0; }
  int add(const ObIvfKnnEntry &item)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count_ >= CAPACITY)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      items_[count_++] = item;
      ++scan_count_;
    }
    return ret;
  }
  int add_batch(const int64_t cid,
                const int64_t *vids,
                const float *distances,
                const int64_t item_count)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(item_count < 0 || item_count > CAPACITY - count_)
        || OB_UNLIKELY(item_count > 0
            && (OB_ISNULL(vids) || OB_ISNULL(distances)))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObIvfKnnEntry *output = items_ + count_;
      for (int64_t i = 0; i < item_count; ++i) {
        output[i].cid_ = cid;
        output[i].vid_ = vids[i];
        output[i].distance_ = distances[i];
      }
      count_ += item_count;
      scan_count_ += item_count;
    }
    return ret;
  }
  int64_t count() const { return count_; }
  // pre-filter emits only matching rows, but charges the original nonempty 32-row
  // posting batch so selective decoding does not expand the search window.
  int64_t scan_count() const { return scan_count_; }
  void add_filtered_count(int64_t count) { scan_count_ += count; }
  const ObIvfKnnEntry &at(const int64_t index) const { return items_[index]; }

private:
  int64_t count_;
  int64_t scan_count_;
  ObIvfKnnEntry items_[CAPACITY];
};

// DiskBBQ posting scanner and batched distance computer. The
// SSTable, read_info, and access_ctx are borrowed and must outlive this object.
// access_ctx must not be concurrently mutated while operations are in flight.
// Per-query statistics are passed to each operation.
class ObDiskBBQFastScanner final
{
public:
  ObDiskBBQFastScanner();
  ~ObDiskBBQFastScanner();

  int init(
      const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc,
      ObIvfSSTable &sstable,
      storage::ObTableAccessContext &access_ctx);
  void reset();
  // Explicit query scope for PRE: the immutable rotated query is borrowed until
  // end_query(). Call begin_query again for new contents, even at the same address.
  int begin_query(const float *rotated_query);
  void end_query();
  // Both vectors are borrowed, immutable, and must remain valid until close_scan().
  int open_scan(
      int64_t cid,
      ObIvfSSTableReadStats *read_stats,
      const float *rotated_query,
      const float *rotated_centroid,
      const ObIvfPreFilter *pre_filter = nullptr,
      bool use_lower_bound = true);
  int get_next_batch(
      ObDistBBQFastScanBatch &batch,
      bool &is_iter_end);
  void close_scan();

private:
  static constexpr int64_t DATA_PREFETCH_DEPTH = 5;
  // Keep one spare slot beyond the active prefetch window.
  static constexpr int64_t DATA_RING_CAP = DATA_PREFETCH_DEPTH + 1;

  struct DataPrefetchItem
  {
    DataPrefetchItem()
      : data_handle_(), is_left_border_(false), is_right_border_(false) {}
    void reset()
    {
      data_handle_.reset();
      is_left_border_ = false;
      is_right_border_ = false;
    }
    storage::ObMicroBlockDataHandle data_handle_;
    bool is_left_border_;
    bool is_right_border_;
  };

  int inner_open_scan(int64_t cid, ObIvfSSTableReadStats *read_stats);
  int prepare_row_computer();
  int init_posting_range();
  void set_posting_cid(int64_t cid);
  int try_prefetch_data_blocks();
  int prefetch_one_data_block(
      const ObMicroIndexInfo &index_info,
      storage::ObMicroBlockDataHandle &handle);
  int open_next_micro_block(bool &is_iter_end);
  int decode_current_rows(
      int64_t row_cap,
      ObDistBBQFastScanBatch &batch);
  int decode_filtered_rows(int64_t row_cap, int64_t &selected_count);
  int decode_cached_pre_rows(int64_t row_cap, ObDistBBQFastScanBatch &batch);
  void finish_row_batch(int64_t row_cap, int64_t evaluated_rows, ObDistBBQFastScanBatch &batch);
  int decode_column(int32_t column, int64_t row_count, common::ObDatum *datums);
  int decode_current_fastscan_batch(
      ObDistBBQFastScanBatch &batch,
      bool &found);
  bool is_current_micro_block_exhausted() const;
  bool seek_current_fastscan_batch(int64_t cid);
  void reset_scan_state();
  bool has_prefetched_item() const { return data_read_idx_ < data_prefetch_idx_; }
  bool is_prefetch_window_full() const
  {
    return data_prefetch_idx_ - data_read_idx_ >= DATA_PREFETCH_DEPTH;
  }
  DataPrefetchItem &current_data_item()
  {
    return data_ring_[data_read_idx_ % DATA_RING_CAP];
  }
  DataPrefetchItem &next_prefetch_item()
  {
    return data_ring_[data_prefetch_idx_ % DATA_RING_CAP];
  }

private:
  ObIvfSSTable *sstable_;
  const storage::ObITableReadInfo *read_info_;
  storage::ObTableAccessContext *access_ctx_;
  ObIvfSSTableReadStats *read_stats_;
  ObIvfSSTableRowDesc row_desc_;
  quantization::ObQuantizationParams quantization_params_;
  common::ObArenaAllocator query_allocator_;
  quantization::ObQuantizationComputer computer_;
  quantization::ObRaBitQFastScanComputer fastscan_computer_;
  int64_t code_size_;
  common::ObArenaAllocator decoder_allocator_;
  common::ObArenaAllocator scan_allocator_;
  common::ObFIFOAllocator data_io_allocator_;
  ObSSTableIndexBaseScanner index_scanner_;
  ObSSTableIndexScanParam index_scan_param_;
  ObMicroBlockReaderHelper reader_helper_;
  ObIMicroBlockReader *micro_reader_;
  const ObIvfFastScanBlockHeader *fastscan_header_;
  const ObIvfQuantizedRowBlockHeader *quantized_row_header_;
  int64_t current_fastscan_batch_idx_;
  int64_t current_fastscan_vid_idx_;
  DataPrefetchItem data_ring_[DATA_RING_CAP];
  int64_t data_read_idx_;
  int64_t data_prefetch_idx_;
  int64_t current_cid_;
  const ObIvfPreFilter *pre_filter_;
  bool empty_pre_filter_;
  bool use_lower_bound_;
  const float *prepared_query_;
  float query_raw_norm_;
  const float *scan_query_;
  const float *scan_centroid_;
  bool row_computer_ready_;
  int64_t current_row_idx_;
  int64_t current_last_row_idx_;
  int32_t row_ids_[ObDistBBQFastScanBatch::CAPACITY];
  int64_t vid_values_[ObDistBBQFastScanBatch::CAPACITY];
  common::ObDatum vid_datums_[ObDistBBQFastScanBatch::CAPACITY];
  common::ObDatum payload_datums_[ObDistBBQFastScanBatch::CAPACITY];
  const char *cell_datas_[ObDistBBQFastScanBatch::CAPACITY];
  common::ObSEArray<int32_t, 2> decode_columns_;
  common::ObSEArray<const share::schema::ObColumnParam *, 2> decode_column_params_;
  common::ObSEArray<ObSqlDatumInfo, 2> decode_datum_infos_;
  ObStorageDatum posting_start_datums_[IVF_ROWKEY_COL_COUNT];
  ObStorageDatum posting_end_datums_[IVF_ROWKEY_COL_COUNT];
  ObDatumRowkey posting_start_key_;
  ObDatumRowkey posting_end_key_;
  ObDatumRange posting_range_;
  bool index_iter_end_;
  bool micro_block_opened_;
  bool scan_opened_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDiskBBQFastScanner);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_FAST_SCANNER_H_
