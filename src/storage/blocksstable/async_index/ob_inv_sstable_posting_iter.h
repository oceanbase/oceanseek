/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_POSTING_ITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_POSTING_ITER_H_

#include "common/ob_tablet_id.h"
#include "common/ob_common_types.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_sstable_posting_scanner.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/async_index/ob_posting_block_meta.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"
#include "sql/engine/expr/ob_expr_bm25.h"

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

struct ObInvPostingScanParam
{
  ObInvPostingScanParam() { reset(); }
  void reset()
  {
    search_key_ = nullptr;
    tablet_id_.reset();
    query_flag_ = common::ObQueryFlag();
    allocator_ = nullptr;
    has_relevance_params_ = false;
    doc_freq_ = 0;
    total_doc_cnt_ = 0;
    avg_doc_length_ = 0.0;
    need_posting_value_ = false;
    min_doc_id_ = 0;
    max_doc_id_ = UINT64_MAX;
  }
  // BM25 corpus statistics (avg_doc_length_ / doc_freq_ / total_doc_cnt_) are only
  // required when has_relevance_params_ is true. Indexes without relevance params
  // (search index, or FTS without BM25) may leave them unset.
  bool is_valid() const
  {
    return nullptr != search_key_
        && tablet_id_.is_valid()
        && nullptr != allocator_
        && doc_freq_ >= 0
        && total_doc_cnt_ >= 0
        && min_doc_id_ <= max_doc_id_
        && (!has_relevance_params_ || avg_doc_length_ > 0.0);
  }
  TO_STRING_KV(KP_(search_key), K_(tablet_id), K_(query_flag),
      KP_(allocator), K_(has_relevance_params), K_(doc_freq), K_(total_doc_cnt),
      K_(avg_doc_length),
      K_(need_posting_value), K_(min_doc_id), K_(max_doc_id));

  const ObDatumRowkey *search_key_;
  common::ObTabletID tablet_id_;
  common::ObQueryFlag query_flag_;
  common::ObIAllocator *allocator_;
  bool has_relevance_params_;
  uint64_t doc_freq_;
  uint64_t total_doc_cnt_;
  double avg_doc_length_;
  bool need_posting_value_;
  // Inclusive physical DocID bounds; corpus statistics remain query-wide.
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
};

// =============================================================================
// Posting list iterator — iterate doc_ids within a single inverted index key
//
// Supports the full pruning surface:
//   - next() / advance_to() / advance_to_id() / advance_shallow() for skip-scan and BlockMax
//   - get_block_max_tuple() / in_shallow_status() for WAND / MaxScore
//   - get_curr_bm25_param() / load_curr_bm25_param() for relevance scoring
//
// Access mode:
//   - Datum cursor: get_cur_id() / get_cur_pos_list(datum) /
//                   get_cur_nested_key_list(datum)
//                   — for posting traversal & on-demand columns. Both value
//                     accessors share validate / ensure-load helpers, then call
//                     reader semantic APIs (physical col idx lives in value header).
//
// The iterator is bound to a single inverted-index key after open(); the
// key space dimension is doc_id only.
//
// Lifetime: ObDatum references returned by get_cur_id() / value accessors are
// owned by the iterator and invalidated on next() / advance_to() / advance_to_id() /
// advance_shallow(). get_cur_pos_list and get_cur_nested_key_list share one
// curr_value_datum_; holding both pointers at once is undefined. Callers MUST
// copy if retaining across calls.
//
// Usage:
//   ObInvSSTablePostingIter iter;
//   iter.open(sstable, param);
//   while (OB_SUCC(iter.next())) {
//     const ObDatum *id_datum = nullptr;
//     iter.get_cur_id(id_datum);
//     // process id_datum
//   }
// =============================================================================
class ObInvSSTablePostingIter
{
public:
  ObInvSSTablePostingIter();
  ~ObInvSSTablePostingIter();

  // ---------------- Initialization ----------------
  /// Open the posting list for a specific inverted index key.
  /// Opens the underlying posting scanner and positions on the first posting
  /// block meta. No posting data micro block IO is issued here; data is fetched
  /// lazily on the first row materialization. If the term is absent the iterator
  /// is left valid with is_end() == true and OB_SUCCESS is returned.
  /// @param param            scan parameters (see ObInvPostingScanParam)
  /// @param rowkey_read_info rowkey read info for decoding posting rows (borrowed,
  ///                         must outlive the iterator). Sourced from the tablet.
  /// @param sstable          the inverted index SSTable to read from (borrowed, not owned)
  int open(
      const ObInvPostingScanParam &param,
      const storage::ObITableReadInfo &rowkey_read_info,
      ObInvSSTable &sstable);
  // Keep the tablet/table handles alive for iterators opened through LS tablet service.
  int set_table_handles(
      const storage::ObTabletHandle &tablet_handle,
      const storage::ObTableHandleV2 &table_handle);

  // ---------------- Iteration ----------------
  /// Reset the iterator to initial state (before open). Does not free this.
  void reset();
  /// Advance to the next doc_id in the posting list.
  /// Invalid after advance_shallow() is called.
  /// @retval OB_ITER_END  posting list exhausted
  int next();
  /// Whether the iterator has been exhausted.
  bool is_end() const { return is_end_; }

  /// Batch drain (zero-copy projection): expose direct pointers to the next contiguous
  /// run of posting rows whose doc_id <= boundary, starting after the current cursor and
  /// confined to a single decoded block. No data is copied; the caller (async token op)
  /// reads token_freq/doc_length in place for BM25 and copies only doc_ids it needs downstream.
  ///
  /// The returned pointers reference the cached block buffer and are valid only until the
  /// next drain_until()/drain_until_from_current()/next()/advance_*() call.
  /// Never returns OB_ITER_END for normal EOF; the terminal condition is carried by count:
  ///   count > 0               a batch is projected; call again for more rows <= boundary
  ///   count == 0 && !is_end() reached the first row > boundary: the cursor is positioned
  ///                           on it (read via get_cur_id() / load_curr_bm25_param())
  ///   count == 0 && is_end()  posting exhausted
  /// A single call returns at most one block's worth of rows (also capped by max_count).
  /// It positions the next candidate row index directly (no first-row materialization).
  /// When count > 0 the cursor is positioned on the last projected row; when count == 0
  /// && !is_end() it is positioned on the boundary lookahead row. Both states guarantee
  /// only doc-id/index availability; load_curr_bm25_param() materializes relevance lazily.
  /// @param boundary   inclusive upper bound doc_id
  /// @param max_count  > 0; upper bound on the projected run length (caller's remaining room)
  /// @param [out] doc_ids       pointer into the block; [0, count)
  /// @param [out] token_freqs   pointer into the block, or nullptr in the absence of relevance params
  /// @param [out] doc_lengths   pointer into the block, or nullptr in the absence of relevance params
  /// @param [out] count         number of projected rows, all <= boundary
  int drain_until(
      const uint64_t boundary,
      const int64_t max_count,
      const uint64_t *&doc_ids,
      const uint64_t *&token_freqs,
      const uint64_t *&doc_lengths,
      int64_t &count);

  /// Same zero-copy projection as drain_until(), but includes the current row. Position
  /// the iterator with advance_to() or advance_to_id(); only doc-id/index availability
  /// is required, and relevance is projected through token_freqs/doc_lengths.
  int drain_until_from_current(
      const uint64_t boundary,
      const int64_t max_count,
      const uint64_t *&doc_ids,
      const uint64_t *&token_freqs,
      const uint64_t *&doc_lengths,
      int64_t &count);

  /// Get the current doc_id as a datum reference.
  /// Invalid in shallow status; caller must copy if retaining across next().
  int get_cur_id(const common::ObDatum *&id_datum) const;

  /// Skip forward to the first doc_id >= target.
  int advance_to(const common::ObDatum &target);
  /// Skip forward to the first doc_id >= target and materialize the doc id only.
  /// Relevance params are not decoded until load_curr_bm25_param() is called.
  int advance_to_id(const common::ObDatum &target);
  /// Shallow advance: skip to the posting block that may contain target.
  /// Sets in_shallow_status_ = true; callers must use advance_to() or
  /// advance_to_id() to materialize a row before row-level iteration. Shallow
  /// positioning does not materialize a row and does not issue new posting-data
  /// IO. If the target remains in the already decoded current block, the decoded
  /// view may be retained for a following exact advance; otherwise it is
  /// invalidated.
  int advance_shallow(const common::ObDatum &target, const bool inclusive);
  /// Whether the cursor is in shallow status (block positioned but not decoded).
  bool in_shallow_status() const { return in_shallow_status_; }
  /// Return BM25 scoring parameters (tf, doc_length) for the current row.
  /// Invalid in shallow status.
  int get_curr_bm25_param(int64_t &token_freq, int64_t &doc_length) const;
  /// Materialize BM25 scoring parameters for the current row on demand.
  int load_curr_bm25_param(int64_t &token_freq, int64_t &doc_length);
  // BM25 score for a (token_freq, doc_length) pair using the scan params.
  double calc_bm25_score(const uint64_t token_freq, const uint64_t doc_length) const;
  int get_block_max_tuple(uint64_t &min_id, uint64_t &max_id, double &max_score) const;
  // Phrase block-max ranking: max_i f_m[i] / length_normalizer(L_m[i], avg_doc_length).
  // Valid after advance_shallow(); matches sync ObBlockMaxScoreCalc match_phrase_mode.
  int get_curr_phrase_block_max_ranking(double &max_ranking) const;
  // Compute the BM25 upper bound over a forward range using only metadata pinned by
  // the main scanner. can_calc is false unless the complete range is covered.
  int get_range_max_score(
      const uint64_t start_id,
      const bool start_inclusive,
      const uint64_t end_id,
      double &max_score,
      bool &can_calc);

  // ---------------- On-demand column access ----------------
  /// Lazy-load paired value micro; datum lifetime same as get_cur_id().
  int get_cur_pos_list(const common::ObDatum *&datum);
  // ---------------- Nested key list access ----------------
  /// Return the nested key list for the current doc_id as an ObString datum.
  int get_cur_nested_key_list(const common::ObDatum *&datum);

  TO_STRING_KV(K_(is_opened), K_(is_end), K_(in_shallow_status), K_(curr_block_loaded),
               K_(has_relevance_params), KP_(curr_id), K_(curr_token_freq),
               K_(curr_doc_length), K_(curr_row_idx), K_(curr_relevance_materialized),
               K_(need_load_micro_block), K_(need_load_value_micro_block), K_(curr_meta));

private:
  class ObRangeMaxScoreVisitor;
  // Validate target datum is a uint64 doc_id and extract it.
  int decode_target_doc_id(const common::ObDatum &target, uint64_t &target_doc_id) const;
  // Load curr_meta_ from the scanner's current posting block.
  int load_current_block_meta();
  // Fetch current posting data micro block, strip the physical header, locate the
  // logical posting block by curr_meta_.locator_.block_idx_, and init the decoder.
  int load_current_block();
  // Fetch current posting value micro block, strip the physical header, locate the
  // logical value block by curr_meta_.locator_.block_idx_, and init the value reader.
  int load_current_value_block();
  // Shared row-state checks for get_cur_pos_list / get_cur_nested_key_list (no value IO).
  int validate_posting_value_row_state();
  // Locate the first row whose doc_id is not less than target. Does not materialize the row.
  int locate_target(const uint64_t target_doc_id, int64_t &target_idx);
  // Decode doc_id for row_idx and update the row cursor without relevance params.
  int materialize_doc_id(const int64_t row_idx);
  // Decode relevance params for curr_row_idx_ if present and not materialized.
  int materialize_curr_relevance();
  // Decode doc_id (+ relevance params if present) for row_idx and update row state.
  int materialize_row(const int64_t row_idx);
  // Position the next drain candidate index (the row after the current cursor) WITHOUT
  // materializing it, loading/advancing blocks as needed. Normal EOF sets is_end_ and
  // returns OB_SUCCESS (caller checks is_end_). Used by drain_until to avoid a first-row
  // materialization.
  int position_drain_start(int64_t &start_idx);
  // Shared projection implementation for the next-row and current-row entry points.
  int drain_until_impl(
      const uint64_t boundary,
      const int64_t max_count,
      const uint64_t *&doc_ids,
      const uint64_t *&token_freqs,
      const uint64_t *&doc_lengths,
      int64_t &count,
      const bool include_current);
  // Advance the scanner to the next posting block; OB_ITER_END when exhausted.
  int move_to_next_block();
  // Drop the current posting / logical-value block view.
  // Data & value leaf micros are retained when need_load_*_micro_block_ stays false
  // (same leaf hosts multiple logical blocks), matching each other.
  void reset_loaded_block();
  // Clear the materialized row cursor (curr_id_, freq/length, row_idx_).
  void reset_row_cursor();
private:
  ObInvSSTable *sstable_;           // bound SSTable (borrowed, not owned)
  common::ObIAllocator *allocator_; // allocator for internal buffers (borrowed, not owned)

  // Copied corpus-dependent BM25 statistics from the scan param (see ObInvPostingScanParam).
  int64_t doc_freq_;
  int64_t total_doc_cnt_;
  double avg_doc_length_;
  // Corpus-dependent BM25 scoring constants (score_multiplier fixed to 1.0), precomputed
  // once when opening the iterator so block-max scoring uses the same single-division
  // kernel as the SQL-layer document scoring.
  sql::ObBM25PrecomputedParams bm25_calc_params_;
  // Whether the bound SSTable carries BM25 relevance params (token_freq / doc_length).
  // Copied from sstable.get_inv_meta().get_stats().has_relevance_params_ at open time. When false,
  // BM25 scoring and block-max frontier computation are skipped (search index path).
  bool has_relevance_params_;
  storage::ObTabletHandle tablet_handle_;
  storage::ObTableHandleV2 table_handle_;
  ObSSTablePostingScanner scanner_;
  ObPostingBlockMeta curr_meta_;
  ObMicroBlockData curr_data_micro_block_;
  ObMicroBlockData curr_value_micro_block_;
  // When false, load_current_block() reuses curr_data_micro_block_/posting_micro_reader_
  // (same leaf micro hosts multiple posting blocks).
  bool need_load_micro_block_;
  // Parallel to need_load_micro_block_ for the paired posting value leaf micro.
  bool need_load_value_micro_block_;
  ObInvIdxMicroBlockReader posting_micro_reader_;
  ObPostingBlockData posting_block_data_;
  ObInvIdxMicroBlockReader posting_value_micro_reader_;
  ObPostingValueBlockReader posting_value_block_reader_;
  common::ObDatum curr_id_datum_;
  common::ObDatum curr_value_datum_;
  uint64_t curr_id_storage_;        // backing storage for curr_id_datum_
  const common::ObDatum *curr_id_;
  int64_t curr_token_freq_;
  int64_t curr_doc_length_;
  int64_t curr_row_idx_;
  bool curr_relevance_materialized_;
  // BM25 upper bound computed once when positioning on a new posting block.
  double curr_block_max_score_;
  // Monotonically non-decreasing scan frontier: the lower bound of doc ids this iterator can still emit
  uint64_t frontier_min_id_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  // True when curr_meta_ is positioned AND posting_decoder_ is loaded for the
  // current block so rows can be materialized.
  bool curr_block_loaded_;
  bool curr_value_block_loaded_;
  bool need_posting_value_;
  bool is_end_;
  bool is_opened_;
  bool in_shallow_status_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInvSSTablePostingIter);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_POSTING_ITER_H_
