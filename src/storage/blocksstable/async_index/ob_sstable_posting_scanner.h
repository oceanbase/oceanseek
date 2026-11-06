/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_SSTABLE_POSTING_SCANNER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_SSTABLE_POSTING_SCANNER_H_

#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_posting_meta_row_scanner.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"

namespace oceanbase
{
namespace common
{
template <typename T>
class ObFunction;
}
namespace blocksstable
{

// Scans the index tree of an inverted index SSTable for one query term and iterates
// posting block meta in order.
// Prefetch (adaptive): sstable_index_scanner_ records data micro block addresses in
// data_ring_ and keeps each item's index_info plus borrowed agg buffer alive through its
// leaf pin generation. Data block IO is issued only after the caller demonstrates read
// demand by calling get_cur_micro_block(); blocks skipped by block-max cost no data IO.
// When need_posting_value_ is true, each ring slot may also carry a paired value micro
// block (position list) at the agg-recorded offset; value IO is similarly on-demand via
// get_cur_value_micro_block() and uses a physical cache key distinct from the data block.
class ObSSTablePostingScanner final
{
public:
  static constexpr int64_t DEFAULT_DATA_PREFETCH_DEPTH = 5;
  static constexpr int64_t DATA_RING_CAP = DEFAULT_DATA_PREFETCH_DEPTH + 1;
  static constexpr int64_t LEAF_PIN_SLOT_CAP = DATA_RING_CAP;

  ObSSTablePostingScanner();
  ~ObSSTablePostingScanner() { reset(); }

  int open(
      const ObITableReadInfo &read_info,
      const ObDatumRowkey &query_key,
      const ObTabletID &tablet_id,
      const ObQueryFlag &query_flag,
      ObInvSSTable &sstable,
      ObIAllocator &scan_allocator,
      const bool need_posting_value = false);

  // Advance the iterator to the next posting block. block_changed reports whether the
  // read cursor switched to a new prefetched index row
  int next_block(bool &block_changed);
  // Meta of the posting block at the current cursor. Valid until the next next_block()/advance_to().
  int get_cur_block_meta(ObPostingBlockMeta &posting_meta);
  // Read-only O(1) identity check. OB_SUCCESS with reusable=false requires normal
  // range parsing; errors reject the range without scoring or moving cursors.
  int check_cached_range_meta(const ObPostingBlockMeta &cached,
      const uint64_t first_id, const uint64_t end_id, bool &reusable) const;
  // Batch the range frontier into one callback per pinned metadata row.
  // Does not advance the cursor; each row's intersecting slices must be contiguous.
  int visit_prefetched_frontier_spans(
      const uint64_t first_id, const uint64_t end_id,
      const common::ObFunction<int(const ObPostingFrontierSpan &)> &visitor,
      bool &range_covered, bool &score_available);
  // Get current posting micro block for read. Calling
  // this marks read demand and turns on adaptive prefetch of subsequent data blocks.
  int get_cur_micro_block(ObMicroBlockData &data_block);
  // Get current posting value micro block paired with the read cursor's data block.
  // Only issues IO when need_posting_value_ is true. Returns reset value_block when
  // the segment has no posting value (no position list column).
  // @retval OB_NOT_SUPPORTED  need_posting_value_ is false
  // @retval OB_ITER_END       scan exhausted
  // @retval OB_SUCCESS        value_block valid, or segment has no value column (reset block)
  int get_cur_value_micro_block(ObMicroBlockData &value_block);
  // Skip to the first posting block whose max_doc_id >= target_id (inclusive) or
  // > target_id (exclusive)
  // block_changed reports whether the read cursor switched to a new prefetched index row.
  int advance_to(const uint64_t target_id, const bool inclusive, bool &block_changed);
  void reset();
  OB_INLINE ObInvIdxType get_index_type() const
  {
    return index_type_;
  }

  TO_STRING_KV(K_(is_inited), K_(need_posting_value), K_(posting_index_block_opened),
      K_(scan_iter_end), K_(data_io_enabled), K_(data_read_idx), K_(data_prefetch_idx),
      K_(idx_iter_end), K_(last_leaf_pin_idx), K_(index_type));

private:
  // A pinned leaf index micro block shared by all ring items whose rows live in that block.
  // Multiple leaf rows inside one leaf block share a single slot (ref_cnt_ bumps per item);
  // the slot is released (and generation_ bumped) only when the last referencing item is
  // consumed. generation_ guards against an item mistaking a recycled slot for its own.
  struct LeafPinSlot
  {
    LeafPinSlot()
      : handle_(), ref_cnt_(0), generation_(0) {}
    void reset()
    {
      handle_.reset();
      ref_cnt_ = 0;
      ++generation_;
    }
    storage::ObMicroBlockDataHandle handle_;  // pins the leaf index block (refcounted, no buffer copy)
    int64_t ref_cnt_;
    uint64_t generation_;
  };

  // One prefetched posting data micro block (one leaf index row). index_info_ is also
  // borrowed by metadata range scans while its leaf pin generation remains valid. Its raw pointers
  // (agg_row_buf_/row_header_/endkey_) point into the leaf block pinned by the referenced
  // LeafPinSlot, and table_read_info_ is stable for the scan lifetime. data_handle_ carries
  // the data block address even before IO is issued.
  struct DataPrefetchItem
  {
    DataPrefetchItem()
      : leaf_pin_idx_(-1), leaf_pin_generation_(0), index_info_(),
        value_row_header_(), value_row_header_valid_(false),
        data_handle_(), value_handle_(), value_micro_offset_(0), value_micro_size_(0),
        max_doc_id_(0), io_issued_(false), requested_(false),
        value_io_issued_(false), value_requested_(false) {}
    ~DataPrefetchItem() {}
    void reset();
    int64_t leaf_pin_idx_;          // references LeafPinSlot; does not own the leaf handle directly
    uint64_t leaf_pin_generation_;  // guards against slot recycling
    ObMicroIndexInfo index_info_;   // shallow copy; agg/row_header/endkey kept alive by the leaf pin slot
    ObIndexBlockRowHeader value_row_header_; // copy of data row header with value offset/size for IO
    bool value_row_header_valid_;
    storage::ObMicroBlockDataHandle data_handle_;
    storage::ObMicroBlockDataHandle value_handle_;
    uint32_t value_micro_offset_;   // from agg header; macro-block absolute offset of value micro block
    uint32_t value_micro_size_;
    uint64_t max_doc_id_;
    bool io_issued_;
    bool requested_;
    bool value_io_issued_;
    bool value_requested_;
  };

  // Acquire a LeafPinSlot for current leaf: reuse the slot of the last prefetched leaf when
  // scanner is still in the same block, otherwise allocate a free slot and hold into it.
  int acquire_leaf_pin_slot(
      int64_t &slot_idx,
      uint64_t &slot_generation);
  // Drop the item's reference to its LeafPinSlot; release the slot when ref_cnt_ hits 0.
  void release_leaf_pin_slot(DataPrefetchItem &item);
  bool validate_leaf_pin_ref(const DataPrefetchItem &item) const;
  // Reopen the single meta_row_scanner_ on the current read item's agg buffer.
  int open_cur_read_item_meta();
  // Borrow one pinned metadata row without moving scanner cursors or issuing data IO.
  int get_prefetched_meta_row(
      const int64_t row_offset,
      const char *&agg_buf,
      int64_t &agg_size,
      uint64_t &max_doc_id) const;
  // Extend metadata lookahead toward end_id without moving the logical read cursor.
  int ensure_meta_lookahead(const uint64_t end_id);
  int64_t get_prefetched_meta_row_count() const
  { return data_prefetch_idx_ - data_read_idx_; }
  bool is_prefetched_to_end() const { return idx_iter_end_; }
  // Single point of term-boundary check: decide whether a trailing probe row beyond the
  // current term should terminate iteration. Sets iter_end=true to drop the row.
  int check_row_in_term(const ObMicroIndexInfo &index_info, bool &iter_end);
  // Build the full physical scan range and skip key from a logical term key.
  // Padding the physical range with doc_id MIN/MAX keeps all index rows for a
  // term visible when its posting list spans multiple index rows.
  int init_scan_keys(
      const ObDatumRowkey &query_key,
      const int64_t rowkey_column_cnt,
      ObIAllocator &scan_allocator);
  // Update the doc_id datum in skip_key_ before calling sstable_index_scanner_.advance_to().
  void set_skip_key_doc_id(const uint64_t doc_id);
  int try_adaptive_prefetch();
  // Try scan one row from sstable_index_scanner_ within prefetch depth
  int try_prefetch_one_index_row(bool &row_scanned);
  // Drop pin + reset item before it is committed into the data ring.
  void abandon_prefetch_item(DataPrefetchItem &item);
  // Undo ++data_prefetch_idx_ commitment after data/value prefetch failure.
  void rollback_prefetched_index_row(DataPrefetchItem &item, bool &row_scanned);
  int prefetch_one_data_block(
      const ObMicroIndexInfo &index_info,
      storage::ObMicroBlockDataHandle &handle);
  // Issue data/value IO for a metadata row already committed to the ring.
  int prefetch_item_blocks(DataPrefetchItem &item);
  int prefetch_one_value_block(
      DataPrefetchItem &item,
      storage::ObMicroBlockDataHandle &handle);
  int fill_value_locator_from_agg(DataPrefetchItem &item);
  // Move forward data block read cursor. Turn off adaptive prefetch if the block was never read.
  void advance_data_read_cursor();
  int get_doc_id_from_endkey(const ObMicroIndexInfo &index_info, uint64_t &doc_id);
  bool is_data_window_full() const { return data_prefetch_idx_ - data_read_idx_ >= DATA_RING_CAP; }
  bool has_read_item() const { return data_read_idx_ < data_prefetch_idx_; }
  DataPrefetchItem &cur_data_item() { return data_ring_[data_read_idx_ % DATA_RING_CAP]; }
  DataPrefetchItem &cur_prefetch_item() { return data_ring_[data_prefetch_idx_ % DATA_RING_CAP]; }
  void reset_scan_status();

private:
  ObFIFOAllocator data_io_allocator_;
  ObSSTableIndexBaseScanner sstable_index_scanner_;
  // The single posting-meta row scanner, reopened on each read item's agg buffer.
  ObPostingMetaRowScanner meta_row_scanner_;
  DataPrefetchItem data_ring_[DATA_RING_CAP];
  // Pool of leaf index block pins, one per live leaf block in the prefetch window.
  LeafPinSlot leaf_pin_slots_[LEAF_PIN_SLOT_CAP];
  int64_t last_leaf_pin_idx_;  // slot used by the most recently prefetched row (-1 if none)
  ObDatumRange scan_range_;
  ObSSTableIndexScanParam scan_param_;
  // Contiguous storage for the full physical start, end, and skip rowkeys.
  ObStorageDatum *scan_key_datums_;
  // Term-only key used to filter a trailing probe row returned by the index scanner.
  ObDatumRowkey term_key_;
  // Pre-built skip key for advance_to(): (term_datum(s)..., doc_id_slot).
  // skip_datums_ points into scan_key_datums_; the last datum is updated per advance_to().
  ObStorageDatum *skip_datums_;
  ObDatumRowkey skip_key_;
  ObIAllocator *scan_allocator_;  // borrowed; used only to free scan_key_datums_ in reset()
  int64_t data_read_idx_;      // read index for ring buffer
  int64_t data_prefetch_idx_;  // prefetch index for ring buffer
  int64_t data_prefetch_depth_;
  int64_t doc_id_col_idx_;
  bool data_io_enabled_;       // adaptive: issue data block IO ahead of the read cursor
  bool idx_iter_end_;         // sstable_index_scanner_ has no more leaf rows
  bool posting_index_block_opened_;
  bool scan_iter_end_;
  bool need_posting_value_;
  ObInvIdxType index_type_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSSTablePostingScanner);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_SSTABLE_POSTING_SCANNER_H_
