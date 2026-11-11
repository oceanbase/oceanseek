/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


/*
 * Overview
 * - Disjunction filter search operator, only used for filter Disjunction search.
 * - It may expose one caller-provided constant score when its children are alternate
 *   ranges of the same logical predicate; it does not combine arbitrary child scores.
 * - In the future, we will perform reasonable abstractions and compositions to unify
 *   Disjunction both for filter and score.
 * - And perhaps the same applies to Conjunction.
 * Key Responsibilities
 * - 
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASDisjunctionFilterOpParam : public ObIDASSearchOpParam
{
public:
  ObDASDisjunctionFilterOpParam(
      const ObIArray<ObIDASSearchOp *> &optional_ops,
      int64_t minimum_should_match,
      ObDASSearchCost lead_cost,
      const double match_score = 0.0)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DISJUNCTION_FILTER),
      optional_ops_(&optional_ops),
      minimum_should_match_(minimum_should_match),
      lead_cost_(lead_cost),
      match_score_(match_score) {}
  ~ObDASDisjunctionFilterOpParam() {}

  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_optional_ops() const { return optional_ops_; }
  OB_INLINE int64_t get_minimum_should_match() const { return minimum_should_match_; }
  OB_INLINE ObDASSearchCost get_lead_cost() const { return lead_cost_; }
  OB_INLINE double get_match_score() const { return match_score_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASDisjunctionFilterOpParam", ObIDASSearchOpParam,
      KPC_(optional_ops),
      K_(minimum_should_match),
      K_(lead_cost),
      K_(match_score));

private:
  const ObIArray<ObIDASSearchOp *> *optional_ops_;
  int64_t minimum_should_match_;
  ObDASSearchCost lead_cost_;
  // Optional constant score for alternate ranges of one logical predicate.
  double match_score_;
};

class ObDASDisjunctionFilterOp : public ObIDASSearchOp
{
public:
  ObDASDisjunctionFilterOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      iter_end_(false),
      min_should_match_(0),
      lead_cost_(),
      cur_row_id_(),
      rowid_allocators_{
        common::ObMemAttr(MTL_ID(), "DASDisjRowID"),
        common::ObMemAttr(MTL_ID(), "DASDisjRowID")},
      rowid_allocator_idx_(0),
      child_row_ids_(),
      batch_allocator_(common::ObMemAttr(MTL_ID(), "DASDisjBatch")),
      batch_ids_(nullptr),
      batch_row_capacity_(0),
      child_batch_size_(0),
      batch_cursors_(),
      heap_child_indices_(),
      use_batch_merge_(false),
      batch_cursor_primed_(false),
      match_score_(0.0)
  {}

  ScoreCapability get_score_capability() const override
  { return is_scoring_ ? ScoreCapability::CONSTANT : ScoreCapability::NONE; }
  double get_constant_score() const override { return is_scoring_ ? match_score_ : 0.0; }

  bool can_enumerate_rowids() const override
  {
    // minimum_should_match=0 means "match the caller's candidate". There is
    // no finite rowid stream to enumerate without that candidate universe.
    bool can_enumerate = min_should_match_ > 0;
    for (int64_t i = 0; can_enumerate && i < children_cnt_; ++i) {
      can_enumerate = OB_NOT_NULL(children_[i])
          && children_[i]->can_enumerate_rowids();
    }
    return can_enumerate;
  }

protected:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_drain_until(const ObDASRowID &boundary,
                     int64_t capacity,
                     uint64_t *out_ids,
                     double *out_scores,
                     int64_t &count,
                     ObDASRowID &next_id_after,
                     double &next_score_after) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

  int open_children(bool &can_batch_merge);
  int rescan_children(const int64_t opened_children_cnt);
  int reset_merge_cursor();
  double get_match_score() const { return match_score_; }

private:
  static constexpr int64_t CHILD_BATCH_SIZE = 256;
  // Bound the speculative RowID cache retained by one disjunction merge op.
  static constexpr int64_t BATCH_BUFFER_BYTE_BUDGET = 1 * 1024 * 1024;
  static_assert(CHILD_BATCH_SIZE <= INT16_MAX,
                "disjunction batch cursor fields are too narrow");

  struct BatchCursor
  {
    BatchCursor()
      : position_(0),
        count_(0),
        source_exhausted_(false)
    {}

    void reset()
    {
      position_ = 0;
      count_ = 0;
      source_exhausted_ = false;
    }

    TO_STRING_KV(K_(position), K_(count), K_(source_exhausted));

    // Offset of the next unread RowID in this child's batch buffer.
    int16_t position_;
    // Number of valid RowIDs currently buffered for this child.
    int16_t count_;
    // No more rows remain after the current buffered batch.
    bool source_exhausted_;
  };

  static int64_t calc_child_batch_size(const int64_t child_count);
  void reset_batch_cursor();
  int ensure_batch_buffer();
  int init_batch_cursor();
  int prime_batch_cursor();
  int refill_batch_child(const int64_t child_idx, const int64_t capacity);
  int advance_batch_child_to(const int64_t child_idx, const uint64_t target);
  int requeue_batch_child(const int64_t child_idx, const int64_t capacity);
  int build_heap();
  int heap_push(const int64_t child_idx);
  int heap_pop(int64_t &child_idx);
  int heap_sift_down(const int64_t start_pos);
  bool heap_less(const int64_t left_child_idx,
                 const int64_t right_child_idx) const;
  uint64_t batch_child_head(const int64_t child_idx) const;
  int next_batch_rowid(
      ObDASRowID &next_id, double &score, const int64_t capacity);
  int advance_batch_rowid(const ObDASRowID &target,
                          ObDASRowID &curr_id,
                          double &score);
protected:
  bool iter_end_;

private:
  int64_t min_should_match_;
  ObDASSearchCost lead_cost_;
  ObDASRowID cur_row_id_;
  // Keep the emitted compact rowid stable while children advance and reuse
  // their output buffers. The current and next output use alternate arenas.
  common::ObArenaAllocator rowid_allocators_[2];
  int64_t rowid_allocator_idx_;
  ObArray<ObDASRowID> child_row_ids_;
  common::ObArenaAllocator batch_allocator_;
  uint64_t *batch_ids_;
  // Number of uint64_t slots allocated in batch_ids_.
  int64_t batch_row_capacity_;
  // Per-child RowID capacity selected from the total byte budget.
  int64_t child_batch_size_;
  common::ObSEArray<BatchCursor, 4> batch_cursors_;
  common::ObSEArray<int64_t, 4> heap_child_indices_;
  // Selected once at open from minimum_should_match and RowID type.
  bool use_batch_merge_;
  // Child reads are delayed until the first seek/drain so an enclosing
  // operator can finish binding its child inputs before iteration starts.
  bool batch_cursor_primed_;
  double match_score_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_
