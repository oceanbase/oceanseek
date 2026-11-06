/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SQL_DAS_SEARCH_OB_DAAT_MERGE_ITER_H_
#define SQL_DAS_SEARCH_OB_DAAT_MERGE_ITER_H_

#include <type_traits>
#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase

{
namespace sql
{

class DocScoreAccumulator;
class ObDASAsyncTokenOp;
class SparseCandidateBuffer;

// Operator wrapper iter for document-at-a-time query processing
class ObDAATMergeIter
{
public:
  ObDAATMergeIter();
  virtual ~ObDAATMergeIter() {}
  int init(const int64_t iter_idx, ObIDASSearchOp &op);
  void reuse();
  int next();
  int advance_to(const ObDASRowID &target);
  int advance_to_id(const ObDASRowID &target);
  // EOF is reported as success with iter_end set; curr_id is invalid in that case.
  // Otherwise curr_id is at least target. The fallback may materialize the score.
  int advance_to_uint64(const uint64_t target, uint64_t &curr_id, bool &iter_end);
  // Probe 1..MAX_SORTED_PROBE_COUNT strictly increasing physical IDs. Add scores and
  // mark hits, including zero scores; leave unmatched slots unchanged. Restore row
  // positioning as needed; EOF succeeds. Errors may leave partial results and cursor progress.
  int probe_sorted_candidates(const uint64_t *ids, const int64_t count,
      double *scores, bool *matched);
  int advance_shallow(const ObDASRowID &target, const bool inclusive);
  int to_shallow();
  int get_curr_id(ObDASRowID &curr_id) const;
  int get_curr_score(double &score) const;
  int load_curr_score(double &score);
  int get_curr_block_max_info(const MaxScoreTuple *&max_score_tuple) const;
  int get_max_score(double &max_score);
  int get_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc);
  int64_t get_iter_idx() const { return iter_idx_; }
  // Batch drain: consume this iterator's current doc and following docs <= boundary_id
  // into the sink. The underlying search op drain starts from its next row, so this
  // wrapper owns the current cached row semantics.
  // After return: curr_id_/curr_score_ reflect the first doc beyond boundary (or iter_end_).
  template <typename Sink>
  int drain_until(const ObDASRowID &boundary_id, Sink &sink);
  // Add scores to active docs within inclusive bounds and invalidate the cached scalar row.
  int drain_until_active(const uint64_t range_start, const uint64_t range_end,
                         DocScoreAccumulator &doc_score_acc);
  // Supplement compact, unconsumed candidates and refresh the lookahead row.
  int score_candidates(SparseCandidateBuffer &candidates);

  // Internal buffer size for batch drain.
  static constexpr int64_t DRAIN_BATCH_SIZE = DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE;

  bool in_shallow_status() const { return in_shallow_status_; }
  bool iter_end() const { return iter_end_; }
  TO_STRING_KV(
      K_(curr_id),
      K_(curr_score),
      K_(curr_score_materialized),
      KP_(curr_block_max_info),
      K_(max_score),
      K_(iter_idx),
      K_(in_shallow_status),
      K_(iter_end));
private:
  int drain_into_accumulator(const ObDASRowID &boundary_id, DocScoreAccumulator &doc_score_acc);

  ObIDASSearchOp *op_;
  ObDASRowID curr_id_;
  double curr_score_;
  bool curr_score_materialized_;
  const MaxScoreTuple *curr_block_max_info_;
  double max_score_;
  int64_t iter_idx_;
  bool max_score_calculated_;
  bool in_shallow_status_;
  bool iter_end_;
  ObDASAsyncTokenOp *async_token_op_;
  bool is_inited_;
};

// Consumes the caller-owned current row before draining subsequent rows without
// requesting scores. A short batch must satisfy drain_until's lookahead contract.
template <typename DrainSinkT>
int drain_search_op_rowids_until(
    ObIDASSearchOp &op,
    ObDASRowID &curr_id,
    bool &iter_end,
    const ObDASRowID &boundary_id,
    DrainSinkT &sink)
{
  int ret = OB_SUCCESS;
  if (iter_end) {
    // nothing to drain
  } else if (OB_UNLIKELY(!curr_id.is_normal() || !boundary_id.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowid-only drain argument", K(ret), K(curr_id), K(boundary_id));
  } else if (curr_id.get_uint64() <= boundary_id.get_uint64()) {
    uint64_t batch_ids[ObDAATMergeIter::DRAIN_BATCH_SIZE];
    ObDASRowID next_id;
    double ignored_next_score = 0.0;
    bool drained = false;
    const uint64_t current_id = curr_id.get_uint64();
    if (OB_FAIL(sink.consume(&current_id, 1))) {
      LOG_WARN("failed to consume current rowid", K(ret), K(current_id));
      drained = true;
    }

    while (OB_SUCC(ret) && !drained) {
      int64_t count = 0;
      if (OB_FAIL(op.drain_until(
              boundary_id, ObDAATMergeIter::DRAIN_BATCH_SIZE,
              batch_ids, nullptr, count, next_id, ignored_next_score))) {
        LOG_WARN("failed to drain rowid-only batch", K(ret));
      } else if (count > 0 && OB_FAIL(sink.consume(batch_ids, count))) {
        LOG_WARN("failed to consume rowid-only batch", K(ret), K(count));
      }

      if (OB_SUCC(ret) && count < ObDAATMergeIter::DRAIN_BATCH_SIZE) {
        if (next_id.is_max()) {
          iter_end = true;
          drained = true;
        } else if (OB_UNLIKELY(!next_id.is_normal()
                              || next_id.get_uint64() <= boundary_id.get_uint64())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowid-only drain lookahead", K(ret), K(next_id), K(boundary_id));
        } else {
          curr_id = next_id;
          drained = true;
        }
      }
    }
  }
  return ret;
}

template <typename DrainSinkT>
int drain_search_op_until(
    ObIDASSearchOp &op,
    ObDASRowID &curr_id,
    double &curr_score,
    bool &iter_end,
    const ObDASRowID &boundary_id,
    DrainSinkT &sink)
{
  int ret = OB_SUCCESS;
  if (iter_end) {
    // nothing to drain
  } else if (OB_UNLIKELY(!curr_id.is_normal() || !boundary_id.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(curr_id), K(boundary_id));
  } else {
    const uint64_t curr_u64 = curr_id.get_uint64();
    const uint64_t boundary_u64 = boundary_id.get_uint64();
    if (curr_u64 <= boundary_u64) {
      uint64_t batch_ids[ObDAATMergeIter::DRAIN_BATCH_SIZE];
      double batch_scores[ObDAATMergeIter::DRAIN_BATCH_SIZE];
      ObDASRowID next_id;
      double next_score = 0.0;
      bool drained = false;
      if (OB_FAIL(sink.add_dim(curr_u64, curr_score))) {
        LOG_WARN("failed to add current dim in drain_until", K(ret));
        drained = true;
      }

      while (OB_SUCC(ret) && !drained) {
        int64_t count = 0;
        if (OB_FAIL(op.drain_until(
            boundary_id, ObDAATMergeIter::DRAIN_BATCH_SIZE,
            batch_ids, batch_scores, count, next_id, next_score))) {
          LOG_WARN("failed to drain batch", K(ret));
        } else if (count > 0
            && OB_FAIL(sink.add_dim_batch(batch_ids, batch_scores, count))) {
          LOG_WARN("failed to add dim batch", K(ret));
        }

        if (OB_SUCC(ret) && count < ObDAATMergeIter::DRAIN_BATCH_SIZE) {
          if (next_id.is_max()) {
            iter_end = true;
          } else {
            curr_id = next_id;
            curr_score = next_score;
          }
          drained = true;
        }
      }
    }
  }
  return ret;
}

template <typename Sink>
int ObDAATMergeIter::drain_until(
    const ObDASRowID &boundary_id, Sink &sink)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge iterator not initialized", K(ret));
  } else if (iter_end_) {
  } else if (in_shallow_status_ || !curr_score_materialized_) {
    // Shallow positioning or deferred scoring leaves no usable cached score to drain.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scored drain requires a materialized current row", K(ret));
  } else {
    if constexpr (std::is_same<Sink, DocScoreAccumulator>::value) {
      if (OB_FAIL(drain_into_accumulator(boundary_id, sink))) {
        LOG_WARN("failed to drain rows into accumulator", K(ret), K(boundary_id));
      }
    } else if (OB_FAIL(drain_search_op_until(
        *op_, curr_id_, curr_score_, iter_end_, boundary_id, sink))) {
      LOG_WARN("failed to drain scored rows until boundary", K(ret), K(boundary_id));
    }
    if (OB_SUCC(ret)) {
      // A successful drain retains a materialized lookahead score unless the iterator reached EOF.
      curr_score_materialized_ = !iter_end_;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
#endif
