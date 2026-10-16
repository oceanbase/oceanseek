/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_ITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_ITER_H_

#include "common/ob_tablet_id.h"
#include "common/ob_common_types.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_posting_meta_row_scanner.h"
#include "storage/blocksstable/async_index/ob_posting_block_meta.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_doc_id_prefetcher.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace storage
{
class ObITableReadInfo;
class ObTabletHandle;
class ObTableHandleV2;
}
namespace blocksstable
{

struct ObInvRangeScanParam
{
  ObInvRangeScanParam() { reset(); }
  void reset()
  {
    scan_range_ = nullptr;
    tablet_id_.reset();
    query_flag_ = common::ObQueryFlag();
    allocator_ = nullptr;
    need_posting_value_ = false;
    enable_doc_id_drain_ = false;
  }
  bool is_valid() const
  {
    return nullptr != scan_range_
        && scan_range_->is_valid()
        && tablet_id_.is_valid()
        && nullptr != allocator_;
  }
  TO_STRING_KV(KP_(scan_range), K_(tablet_id), K_(query_flag), KP_(allocator),
      K_(need_posting_value), K_(enable_doc_id_drain));

  // Term key range or full physical rowkey range padded with doc_id MIN/MAX;
  // either side may be min/max.
  // Borrowed from caller, MUST outlive the iter.
  const ObDatumRange *scan_range_;
  common::ObTabletID tablet_id_;
  common::ObQueryFlag query_flag_;
  // Borrowed from caller, MUST outlive the iter.
  common::ObIAllocator *allocator_;
  // Whether the caller needs posting value access (pos_list / nested_key_list).
  bool need_posting_value_;
  // Only the Async Search BASE route may opt into raw doc-id drain.
  bool enable_doc_id_drain_;
};

// =============================================================================
// Range iterator — iterate (term, doc_id) pairs across an inverted index range
//
// Access mode:
//   - Datum cursor: get_cur_id() for the doc_id, get_cur_term() for the leading
//     term columns, as well as get_cur_pos_list() and get_cur_nested_key_list().
//
// A forward-only full scan of the term range. Iterates in dictionary order:
// terms ascending, doc_ids ascending within each term.
//
// Lifetime: ObDatum references returned by get_cur_id() / get_cur_term() are
// owned by the iterator and invalidated on next(). Callers MUST copy if
// retaining across calls.
//
// Usage:
//   ObInvSSTableRangeIter iter;
//   iter.open(param, read_info, sstable);
//   while (OB_SUCC(iter.next())) {
//     const ObDatum *id_datum = nullptr;
//     iter.get_cur_id(id_datum);
//     // process (term, id_datum)
//   }
// =============================================================================
class ObInvSSTableRangeIter
{
public:
  ObInvSSTableRangeIter();
  ~ObInvSSTableRangeIter();

  // ---------------- Initialization ----------------
  /// Open the iterator for a term key or physical rowkey range. Initializes the
  /// index-tree range scanner; row mode positions on the first (term, doc_id),
  /// while raw doc-id drain requests its first physical group lazily. No posting
  /// data micro block IO is issued here.
  /// @param param     scan parameters (see ObInvRangeScanParam)
  /// @param read_info read info for decoding index / posting rows (borrowed,
  ///                  must outlive the iterator). Sourced from the tablet.
  /// @param sstable   the inverted index SSTable to read from (borrowed, not owned)
  int open(
      const ObInvRangeScanParam &param,
      const storage::ObITableReadInfo &read_info,
      ObInvSSTable &sstable);
  // Keep the tablet/table handles alive for iterators opened through LS tablet service.
  int set_table_handles(
      const storage::ObTabletHandle &tablet_handle,
      const storage::ObTableHandleV2 &table_handle);

  // ---------------- Iteration ----------------
  /// Reset the iterator to initial state (before open). Does not free this.
  void reset();
  /// Advance to the next (term, doc_id).
  /// @retval OB_ITER_END  range exhausted
  int next();
  /// Whether the iterator has been exhausted.
  bool is_end() const { return is_end_; }

  /// Return the next contiguous raw doc-id slice from the current physical
  /// posting cache value. No ordering or boundary contract is provided across
  /// ranges. The returned view is invalidated by the next iterator call/reset.
  /// This API is opt-in and mutually exclusive with row materialization.
  int get_next_doc_ids(
      const int64_t capacity,
      const uint64_t *&doc_ids,
      int64_t &count);

  // ---------------- Current row access ----------------
  /// Get the current doc_id as a datum reference.
  /// Caller must copy if retaining across next().
  int get_cur_id(const common::ObDatum *&id_datum) const;
  /// Get the current term as a rowkey (the leading term columns of the current
  /// index row). Non-const: may lazily materialize term on first call.
  /// Caller must copy if retaining across next().
  int get_cur_term(const ObDatumRowkey *&term_rowkey);
  /// Get the current pos list as a datum reference.
  /// Caller must copy if retaining across next().
  int get_cur_pos_list(const common::ObDatum *&datum);
  /// Get the current nested key list as a datum reference.
  /// Caller must copy if retaining across next().
  int get_cur_nested_key_list(const common::ObDatum *&datum);

  TO_STRING_KV(K_(is_opened), K_(is_end), K_(curr_posting_block_loaded),
               K_(curr_row_idx), K_(curr_meta), K_(term_col_cnt), K_(doc_id_col_idx));

private:
  static bool is_valid_range_key(
      const ObDatumRowkey &key,
      const int64_t term_col_cnt,
      const int64_t schema_rowkey_cnt,
      const bool is_start_key);
  // Accept only logical term keys or complete physical keys whose doc_id uses
  // the side-specific sentinel (MIN for start, MAX for end).
  static int validate_scan_range(
      const ObDatumRange &scan_range,
      const int64_t term_col_cnt,
      const int64_t schema_rowkey_cnt);
  // Convert logical term bounds to physical (term, doc_id) bounds owned by
  // the posting iterator. Shared index-row range semantics stay unchanged.
  int prepare_index_scan_range(bool &is_empty_range);
  // Only the posting iterator interprets a right-boundary index row as an
  // in-range row or the trailing routing probe returned by the shared scanner.
  int check_index_row_in_range(
      const ObMicroIndexInfo &index_info,
      const bool is_right_boundary,
      bool &iter_end) const;
  // Advance the outermost cursor to the next term's index row and reopen the
  // posting-meta scanner on it; OB_ITER_END past the range's last index row.
  int move_to_next_index_row();
  // Advance to the next posting block, crossing into the next index row when the
  // current one is exhausted; OB_ITER_END at the end of the range.
  int move_to_next_posting_block();
  // Load curr_meta_ from the current posting block of the meta scanner.
  int load_current_block_meta();
  // Fetch and decode the posting micro block addressed by curr_index_info_.
  int load_current_micro_block();
  // Fetch and decode the current posting micro block for curr_meta_.
  int load_current_block();
  // Fetch and decode the current posting value micro block for curr_meta_.
  // Lazy-loaded only when need_posting_value_ is set.
  int load_current_value_block();
  // Shared row-state checks for get_cur_pos_list / get_cur_nested_key_list (no value IO).
  int validate_posting_value_row_state() const;
  // Decode row_idx: doc_id from the posting block.
  int materialize_row(const int64_t row_idx);
  // Materialize the term columns of the current index row endkey into curr_term_.
  int materialize_term();
  // Drop the current posting block view, keeping curr_meta_ untouched.
  void reset_loaded_block();
  // Clear the materialized row cursor.
  void reset_row_cursor();
  // Prepare the FlatCache view for one physical index-row group. Complete transformed
  // groups bypass posting-meta parsing; boundary and exact-agg groups parse endpoints.
  int prepare_physical_doc_id_group();
  int get_doc_id_block_idx_range(
      const ObIndexRowGroup &group,
      int64_t &first_block_idx,
      int64_t &last_block_idx);
  int ensure_prefetched_group();
  void reset_prefetched_group();
  // Clear non-owning batch views before releasing their backing cache handle.
  void reset_prepared_doc_id_batch();
  int fill_doc_id_prefetch_ring();
  int open_posting_micro_reader(
      const MacroBlockId &macro_id,
      const int64_t block_offset,
      const int64_t block_size);
  // Whether curr_index_info_ points at a different posting micro block than the loaded one.
  OB_INLINE bool need_load_micro_block() const
  {
    return curr_posting_micro_locator_.block_offset_ < 0
        || curr_posting_micro_locator_.block_offset_ != static_cast<int64_t>(curr_index_info_.get_block_offset())
        || curr_posting_micro_locator_.block_size_ != static_cast<int64_t>(curr_index_info_.get_block_size())
        || curr_posting_micro_locator_.macro_id_ != curr_index_info_.get_macro_id();
  }
  // Whether curr_meta_ points at a different posting value micro block than the loaded one.
  OB_INLINE bool need_load_value_micro_block() const
  {
    return curr_value_micro_locator_.block_offset_ < 0
        || curr_value_micro_locator_.block_offset_ != static_cast<int64_t>(curr_meta_.locator_.value_micro_offset_)
        || curr_value_micro_locator_.block_size_ != static_cast<int64_t>(curr_meta_.locator_.value_micro_size_)
        || curr_value_micro_locator_.macro_id_ != curr_index_info_.get_macro_id();
  }

private:
  struct MicroBlockLocator
  {
    MicroBlockLocator() { reset(); }
    void reset()
    {
      macro_id_.reset();
      block_offset_ = -1;
      block_size_ = -1;
    }
    void set(const MacroBlockId &id, const int64_t offset, const int64_t size)
    {
      macro_id_ = id;
      block_offset_ = offset;
      block_size_ = size;
    }
    MacroBlockId macro_id_;
    int64_t block_offset_;
    int64_t block_size_;
  };
  ObInvSSTable *sstable_;                       // bound SSTable (borrowed, not owned)
  const storage::ObITableReadInfo *read_info_;  // borrowed; drives decoding / projection
  common::ObIAllocator *allocator_;             // borrowed; internal buffers
  storage::ObTabletHandle tablet_handle_;
  storage::ObTableHandleV2 table_handle_;
  ObDatumRange scan_range_;
  // Outermost cursor: iterates index rows across terms within scan_range_.
  ObSSTableIndexScanParam index_scan_param_;
  ObSSTableIndexBaseScanner index_scanner_;
  // One group parked on prefetch OB_EAGAIN. The scanner does not advance while it is valid.
  ObIndexRowGroup prefetched_group_;
  int64_t prefetched_first_block_idx_;
  int64_t prefetched_last_block_idx_;
  ObMicroIndexInfo curr_index_info_;            // current index row (term endkey + posting locator)
  // Middle cursor: posting blocks of the current index row.
  ObPostingMetaRowScanner meta_scanner_;
  ObPostingBlockMeta curr_meta_;
  // Inner cursor: rows within the current posting micro block.
  ObFIFOAllocator data_io_allocator_;           // backs posting_data_handle_ IO buffers
  ObInvSSTableDocIdPrefetcher doc_id_prefetcher_;
  storage::ObMicroBlockDataHandle posting_data_handle_;
  ObInvIdxMicroBlockReader posting_micro_reader_;
  ObMicroBlockData curr_posting_micro_block_;
  MicroBlockLocator curr_posting_micro_locator_;
  bool curr_posting_block_loaded_;
  ObPostingBlockData posting_block_data_;
  // Posting value related.
  storage::ObMicroBlockDataHandle posting_value_data_handle_;
  ObInvIdxMicroBlockReader posting_value_micro_reader_;
  ObMicroBlockData curr_value_micro_block_;
  MicroBlockLocator curr_value_micro_locator_;
  bool curr_value_block_loaded_;
  ObPostingValueBlockReader posting_value_block_reader_;
  ObIndexBlockRowHeader value_row_header_;
  bool need_posting_value_;
  bool enable_doc_id_drain_;
  int64_t curr_row_idx_;
  // Current doc_id datum cursor.
  common::ObDatum curr_id_datum_;
  common::ObDatum curr_value_datum_;           // pos_list / nested_key_list output
  uint64_t curr_id_storage_;                   // backing storage for curr_id_datum_
  const common::ObDatum *curr_id_;
  // Current term rowkey, materialized from the index row endkey prefix.
  ObDatumRow term_buf_;
  ObDatumRowkey curr_term_;
  // Buffer to materialize a discrete index row endkey before term projection.
  ObDatumRow endkey_buf_;
  ObDatumRowkey materialized_endkey_;
  int64_t term_col_cnt_;                        // number of leading term columns
  int64_t doc_id_col_idx_;                      // doc_id column index in storage order
  bool term_materialized_;                      // curr_term_ valid for current index row
  const uint64_t *prepared_doc_ids_;            // borrowed from posting_data_handle_
  int64_t prepared_doc_count_;
  int64_t next_doc_offset_;
  bool is_opened_;
  bool is_end_;
  DISALLOW_COPY_AND_ASSIGN(ObInvSSTableRangeIter);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_RANGE_ITER_H_
