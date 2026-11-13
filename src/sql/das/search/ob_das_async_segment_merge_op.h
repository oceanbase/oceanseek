/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_MERGE_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_MERGE_OP_H_

#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_nested_key.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{

enum ObDASAsyncSegmentDomainType
{
  ASYNC_SEGMENT_DOMAIN_INVALID = 0,
  ASYNC_SEGMENT_DOMAIN_INDEX
};

struct ObDASAsyncSegmentDomain
{
  ObDASAsyncSegmentDomain()
    : type_(ASYNC_SEGMENT_DOMAIN_INVALID), index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1)
  {}
  explicit ObDASAsyncSegmentDomain(
      const uint64_t index_table_id,
      const int64_t group_slot = -1)
    : type_(ASYNC_SEGMENT_DOMAIN_INDEX), index_table_id_(index_table_id),
      group_slot_(group_slot)
  {}
  bool is_valid() const
  {
    return ASYNC_SEGMENT_DOMAIN_INDEX == type_
        && common::OB_INVALID_ID != index_table_id_
        && group_slot_ >= -1;
  }
  TO_STRING_KV(K_(type), K_(index_table_id), K_(group_slot));

  ObDASAsyncSegmentDomainType type_;
  uint64_t index_table_id_;
  int64_t group_slot_;
};

struct ObDASAsyncSegmentMergeOpParam : public ObIDASSearchOpParam
{
public:
  explicit ObDASAsyncSegmentMergeOpParam(
      const common::ObIArray<ObIDASSearchOp *> &segment_ops,
      const ObDASAsyncSegmentDomain &segment_domain,
      const bool force_merge = false,
      const ObDASAsyncSegmentMergeMode mode = ObDASAsyncSegmentMergeMode::SORT_MERGE)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE),
      segment_ops_(&segment_ops),
      segment_domain_(segment_domain),
      force_merge_(force_merge),
      mode_(mode)
  {}
  virtual ~ObDASAsyncSegmentMergeOpParam() {}

  int get_children_ops(common::ObIArray<ObIDASSearchOp *> &children) const override;
  int64_t get_segment_op_cnt() const
  { return OB_NOT_NULL(segment_ops_) ? segment_ops_->count() : 0; }

  const common::ObIArray<ObIDASSearchOp *> *segment_ops_;
  ObDASAsyncSegmentDomain segment_domain_;
  bool force_merge_;
  ObDASAsyncSegmentMergeMode mode_;
};

// Each child is a complete retrieval subtree bound to one Segment. SORT_MERGE keeps one head row
// from every active child and performs a k-way ordered RowID merge. Equal RowIDs across Segments
// are emitted once with their maximum score. DIRECT_MERGE forwards children without global
// ordering or cross-Segment deduplication: raw child batches go to a Bitmap/Sort materializer,
// while bounded direct drains go to an outer BMM filter.
// The tree contains the maximum Segment count among all SQC candidates; active_segment_cnt_ is
// reset to the current partition's real count on every partition rescan.
class ObDASAsyncSegmentMergeOp : public ObIDASSearchOp
{
private:
  static constexpr int64_t CHILD_BATCH_SIZE = 256;
  static constexpr int64_t BATCH_SCORE_BLOCK_SIZE = 16;
  static constexpr int64_t BATCH_SCORE_BLOCK_COUNT =
      (CHILD_BATCH_SIZE + BATCH_SCORE_BLOCK_SIZE - 1) / BATCH_SCORE_BLOCK_SIZE;
  // Bound the speculative RowID/score cache retained by one segment merge op.
  static constexpr int64_t BATCH_BUFFER_BYTE_BUDGET = 1 * 1024 * 1024;
  static_assert(CHILD_BATCH_SIZE <= INT16_MAX,
                "async segment batch cursor fields are too narrow");

  struct BatchCursor
  {
    BatchCursor()
      : position_(0),
        count_(0),
        source_exhausted_(false),
        source_in_shallow_status_(false)
    {}

    void reset()
    {
      position_ = 0;
      count_ = 0;
      source_exhausted_ = false;
      source_in_shallow_status_ = false;
    }

    TO_STRING_KV(K_(position), K_(count), K_(source_exhausted), K_(source_in_shallow_status));

    // Offset of the next unread RowID in this child's batch buffer.
    int16_t position_;
    // Number of valid RowIDs currently buffered for this child.
    int16_t count_;
    // No more rows remain after the current buffered batch.
    bool source_exhausted_;
    // The child source was shallow-advanced beyond the buffered RowIDs.
    bool source_in_shallow_status_;
  };

  // Each phase owns its heap and the contributors removed for its last output.
  template <typename RowType>
  struct MergeCursor
  {
    MergeCursor(common::ObIArray<RowType> &heads,
                common::ObIArray<int64_t> &heap,
                common::ObIArray<int64_t> &pending,
                const bool use_heap = true)
      : heads_(heads), heap_(heap), pending_(pending), use_heap_(use_heap)
    {}

    common::ObIArray<RowType> &heads_;
    common::ObIArray<int64_t> &heap_;
    common::ObIArray<int64_t> &pending_;
    bool use_heap_;
  };

public:
  ObDASAsyncSegmentMergeOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      segment_domain_(),
      segment_ranges_(),
      head_rowids_(),
      head_scores_(),
      child_ended_(),
      child_opened_(),
      heap_child_indices_(),
      pending_child_indices_(),
      nested_key_heads_(),
      nested_heap_child_indices_(),
      nested_pending_child_indices_(),
      curr_nk_(),
      is_first_nk_(true),
      ordered_child_indices_(),
      ordered_child_pos_(0),
      use_ordered_concat_(false),
      concat_head_consumed_(false),
      active_segment_cnt_(0),
      batch_allocator_(common::ObMemAttr(MTL_ID(), "AsyncSegBatch")),
      batch_ids_(nullptr),
      batch_scores_(nullptr),
      batch_score_suffix_maxes_(nullptr),
      batch_child_capacity_(0),
      batch_cursors_(),
      force_merge_(false),
      mode_(ObDASAsyncSegmentMergeMode::SORT_MERGE),
      direct_drain_enabled_(false),
      direct_child_next_rowids_(),
      direct_active_child_indices_(),
      rowid_output_exprs_(),
      current_batch_child_idx_(0),
      target_allocator_(common::ObMemAttr(MTL_ID(), "AsyncSegTgt")),
      threshold_cached_(false),
      cached_threshold_(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE),
      shallow_target_(0),
      use_batch_merge_(false),
      shallow_inclusive_(true),
      in_shallow_status_(false),
      iter_end_(false)
  {
    op_type_ = DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE;
  }
  virtual ~ObDASAsyncSegmentMergeOp() {}
  int get_bitmap_input_range(bool &available, uint64_t &lo, uint64_t &hi,
                             int64_t &estimated_count) override
  {
    return collect_bitmap_input_range(active_segment_cnt_, available, lo, hi, estimated_count);
  }

  const common::ObIArray<ObExpr *> *get_rowid_output_exprs() const override
  {
    return ObDASAsyncSegmentMergeMode::DIRECT_MERGE == mode_
        && !rowid_output_exprs_.empty() ? &rowid_output_exprs_ : nullptr;
  }
  // Forward the next raw doc-id view from a DIRECT_MERGE range child.
  int get_next_doc_ids(
      const int64_t capacity,
      const uint64_t *&doc_ids,
      int64_t &count);
  bool can_forward_raw_doc_ids() const;
  bool supports_direct_rowid_drain() const override
  { return direct_drain_enabled_; }
  bool can_enumerate_rowids() const override
  {
    // A DIRECT_MERGE range source remains enumerable through its ordered
    // materializer path; only a direct-drain source lacks that contract.
    return !direct_drain_enabled_ && ObIDASSearchOp::can_enumerate_rowids();
  }

private:
  int init_nested_key_cursor();
  MergeCursor<ObDASRowID> rowid_merge_cursor()
  { return {head_rowids_, heap_child_indices_, pending_child_indices_, !use_single_child_fast_path()}; }
  MergeCursor<ObDASNestedKey> nested_merge_cursor()
  { return {nested_key_heads_, nested_heap_child_indices_, nested_pending_child_indices_}; }
  template <typename RowType>
  int merge_next(MergeCursor<RowType> &cursor, const int64_t capacity,
                 RowType &result, double &score);
  template <typename RowType>
  int merge_advance(MergeCursor<RowType> &cursor, const RowType &target,
                    RowType &result, double &score);
  // Ensure all child heads reach target by calling advance_to only for lagging children.
  template <typename RowType>
  int advance_merge_heads(MergeCursor<RowType> &cursor, const RowType &target);
  template <typename RowType>
  int emit_min_head(MergeCursor<RowType> &cursor, RowType &result, double &score);
  template <typename RowType>
  int heap_less(const MergeCursor<RowType> &cursor,
                const int64_t left_child_idx, const int64_t right_child_idx, bool &less) const;
  template <typename RowType>
  int heap_push(MergeCursor<RowType> &cursor, const int64_t child_idx);
  template <typename RowType>
  int heap_sift_down(MergeCursor<RowType> &cursor, const int64_t start_pos);
  template <typename RowType>
  int heap_pop(MergeCursor<RowType> &cursor, int64_t &child_idx);
  int fetch_next_head(const int64_t child_idx, const int64_t capacity, ObDASRowID &head);
  int fetch_next_head(const int64_t child_idx, const int64_t capacity, ObDASNestedKey &head);
  int seek_merge_head(const int64_t child_idx, const ObDASRowID &target, ObDASRowID &head);
  int seek_merge_head(const int64_t child_idx, const ObDASNestedKey &target, ObDASNestedKey &head);
  bool merge_head_in_range(const ObDASRowID &head) const;
  bool merge_head_in_range(const ObDASNestedKey &) const { return true; }
  double merge_head_score(const int64_t child_idx, const ObDASRowID &) const
  { return head_scores_.at(child_idx); }
  double merge_head_score(const int64_t, const ObDASNestedKey &) const { return 0.0; }
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score) override;
  int do_next_rowid(ObDASNestedKey &next_nk, double &score) override;
  int do_next_rowids(int64_t capacity, int64_t &count) override;
  int do_drain_until(const ObDASRowID &boundary,
                     int64_t capacity,
                     uint64_t *out_ids,
                     double *out_scores,
                     int64_t &count,
                     ObDASRowID &next_id_after,
                     double &next_score_after) override;
  int do_set_min_competitive_score(const double &threshold) override;
  int do_calc_max_score(double &threshold) override;
  int do_direct_drain_rowids(
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &next_id_after) override;

  int load_current_partition_segments();
  // Outputs the caller's concat-path decision and child order. Unavailable/overlapping
  // ranges leave disjoint false (heap fallback); lookup errors are propagated.
  // Only physically empty Segments are omitted, using the range API's [UINT64_MAX, 0] sentinel.
  int check_segment_rowid_ranges(bool &disjoint,
                                 common::ObIArray<int64_t> &ordered_child_indices) const;
  int reset_children_for_partition(const bool open_children);
  int bind_and_prime_active_children();
  // Skips ended children, advancing ordered_child_pos_ and resetting concat_head_consumed_.
  int64_t current_ordered_child_idx();
  int emit_ordered_concat_head(ObDASRowID &rowid, double &score);
  int next_ordered_concat(const int64_t capacity, ObDASRowID &rowid, double &score);
  int advance_ordered_concat(const ObDASRowID &target);
  int drain_ordered_concat(const ObDASRowID &boundary, const int64_t capacity,
      uint64_t *out_ids, double *out_scores, int64_t &count,
      ObDASRowID &next_id_after, double &next_score_after);
  int next_rowid_with_capacity(
      const int64_t capacity, ObDASRowID &next_id, double &score);
  int drain_bounded_single_child_batch(
      const ObDASRowID &boundary,
      const int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after);
  int fetch_next_head(const int64_t child_idx, const int64_t capacity);
  // Call next_rowid for every pending child; batch DocIDs use the buffered next record.
  template <typename RowType>
  int advance_pending_children(MergeCursor<RowType> &cursor, const int64_t capacity);
  int advance_pending_children(const int64_t capacity);
  int advance_heads_to(const ObDASRowID &target);
  int try_advance_single_live(const ObDASRowID &target,
                              ObDASRowID &rowid,
                              double &score,
                              bool &handled);
  int materialize_shallow_children(const ObDASRowID &target);
  int emit_min_head(ObDASRowID &rowid, double &score);
  int build_heap();
  int heap_push(const int64_t child_idx);
  int heap_pop(int64_t &child_idx);
  int ensure_batch_buffer();
  int refill_batch_child(const int64_t child_idx, const int64_t capacity);
  int fetch_next_batch_head(const int64_t child_idx, const int64_t capacity);
  int advance_batch_head_to(const int64_t child_idx,
                            const ObDASRowID &target,
                            const bool inclusive = true);
  int get_batch_child_shallow_tuple(const int64_t child_idx,
                                    const ObDASRowID &target,
                                    const bool inclusive,
                                    bool &has_tuple,
                                    ObDASRowID &min_id,
                                    ObDASRowID &max_id,
                                    double &max_score);
  bool use_single_child_fast_path() const
  {
    return !force_merge_
        && (segment_domain_.group_slot_ >= 0 || is_async_nested_scope(scope_))
        && 1 == active_segment_cnt_;
  }
  bool current_doc_has_single_nk_source() const
  {
    return use_ordered_concat_
        || (!force_merge_ && 1 == pending_child_indices_.count());
  }
  void reset_merge_state();
  void reset_direct_state();
  int init_direct_rowid_output_exprs(
      const common::ObIArray<ObIDASSearchOp *> &segment_ops);
  int bind_direct_active_children();
  int drain_direct_child(
      const int64_t child_idx,
      const uint64_t range_start,
      const uint64_t range_end,
      ObDASRowIDBatchConsumer &consumer,
      ObDASRowID &child_next_id);
  int record_mode_metric();

private:
  ObDASAsyncSegmentDomain segment_domain_;
  common::ObSEArray<common::ObNewVersionRange, 4> segment_ranges_;
  common::ObSEArray<ObDASRowID, 4> head_rowids_;
  common::ObSEArray<double, 4> head_scores_;
  common::ObSEArray<bool, 4> child_ended_;
  common::ObSEArray<bool, 4> child_opened_;
  common::ObSEArray<int64_t, 4> heap_child_indices_;
  // All children that contributed to the last deduplicated output are advanced together.
  common::ObSEArray<int64_t, 4> pending_child_indices_;
  common::ObSEArray<ObDASNestedKey, 4> nested_key_heads_;
  common::ObSEArray<int64_t, 4> nested_heap_child_indices_;
  // NK contributors are distinct from the complete current-document contributors above.
  common::ObSEArray<int64_t, 4> nested_pending_child_indices_;
  ObDASNestedKey curr_nk_;
  bool is_first_nk_;
  // Proven disjoint children use a separate cursor, never the heap/pending scheduler.
  common::ObSEArray<int64_t, 4> ordered_child_indices_;
  int64_t ordered_child_pos_;
  bool use_ordered_concat_;
  bool concat_head_consumed_;
  int64_t active_segment_cnt_;
  common::ObArenaAllocator batch_allocator_;
  uint64_t *batch_ids_;
  double *batch_scores_;
  // One suffix maximum per score block avoids rescanning the buffered tail.
  double *batch_score_suffix_maxes_;
  int64_t batch_child_capacity_;
  common::ObSEArray<BatchCursor, 4> batch_cursors_;
  bool force_merge_;
  ObDASAsyncSegmentMergeMode mode_;
  bool direct_drain_enabled_;
  // Per-child lookahead for DIRECT_MERGE. MIN means not drained in the current
  // lifecycle, normal means the first row after the previous window, and MAX
  // means EOF (the child is removed from subsequent scheduling).
  common::ObSEArray<ObDASRowID, 4> direct_child_next_rowids_;
  // EOF children leave this set until the next partition open/rescan.
  common::ObSEArray<int64_t, 4> direct_active_child_indices_;
  // DIRECT_MERGE forwards child batches without copying their vectors. All
  // children must therefore populate the same stable expression objects.
  common::ObSEArray<ObExpr *, 2> rowid_output_exprs_;
  int64_t current_batch_child_idx_;
  common::ObArenaAllocator target_allocator_;
  bool threshold_cached_;
  double cached_threshold_;
  uint64_t shallow_target_;
  // Selected once at open from RowID type and worst-case child count.
  bool use_batch_merge_;
  bool shallow_inclusive_;
  bool in_shallow_status_;
  bool iter_end_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncSegmentMergeOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_SEGMENT_MERGE_OP_H_
