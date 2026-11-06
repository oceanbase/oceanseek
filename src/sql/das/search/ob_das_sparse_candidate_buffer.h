/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef SQL_DAS_SEARCH_OB_DAS_SPARSE_CANDIDATE_BUFFER_H_
#define SQL_DAS_SEARCH_OB_DAS_SPARSE_CANDIDATE_BUFFER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql
{

// Fixed-capacity buffer for sparse scored candidates. The buffer is indexed by candidate
// ordinal rather than by a contiguous doc_id offset: candidate doc_ids are stored in an
// explicit ascending array, scores are stored in a parallel compact array, and the bitmap
// records whether a candidate is still alive after dynamic pruning.
//
// A buffer batch has three phases:
// 1. Collection: append candidates in strictly ascending doc_id order.
// 2. Scoring: read an alive candidate by slot, then add scores to that slot.
// 3. Output: consume alive candidates through the internal cursor.
//
// This class is intentionally not a generic sparse score accumulator. In particular, it
// does not support inserting a new doc_id after collection has started a scoring round.
class SparseCandidateBuffer final
{
public:
  SparseCandidateBuffer();
  ~SparseCandidateBuffer() { reset(); }

  int init(const int64_t capacity, ObIAllocator &allocator);
  void reset();
  void reuse();

  OB_INLINE int64_t get_capacity() const { return capacity_; }
  OB_INLINE int64_t get_count() const { return count_; }
  OB_INLINE bool is_empty() const { return 0 == count_; }
  OB_INLINE bool is_full() const { return count_ >= capacity_; }
  OB_INLINE bool has_alive() const { return alive_count_ > 0; }
  // The current candidate remains seekable until next() advances past it.
  OB_INLINE bool has_unconsumed() const { return count_ > 0 && cursor_ < count_; }

  // Append one candidate and mark it alive. doc_id must be greater than the last candidate.
  int append(const uint64_t doc_id, const double initial_score);
  // Input IDs are strictly increasing; reserve room for the entire input batch.
  // Keep rows whose partial scores exceed min_required_score.
  int append_competitive_batch(const uint64_t *doc_ids, const double *scores,
      const int64_t count, const double min_required_score);

  // Return the doc_id at a candidate slot. The return value is false when the buffer is
  // not initialized, the slot is out of range, or the candidate has been pruned. Callers
  // normally iterate slots from zero to get_count() - 1 and skip false results.
  bool get_doc_id(const int64_t idx, uint64_t &doc_id) const;

  // Raw IDs; prune_below leaves dead slots, while prune_and_compact removes them.
  OB_INLINE const uint64_t *get_doc_ids() const { return doc_ids_.get_data(); }

  // Add a dimension score to an existing alive candidate slot.
  int add_score(const int64_t idx, const double score);

  // Match sorted uint32/uint64 postings without widening; slot persists across batches.
  // Evaluate the scorer only for matches and append their count to scored_count.
  template <typename DocIdT, typename Scorer>
  OB_INLINE int add_scored_dim_batch(const DocIdT *doc_ids, const int64_t count,
      const Scorer &scorer, int64_t &slot, int64_t &scored_count)
  {
    int ret = OB_SUCCESS;
    const uint64_t *candidate_ids = get_doc_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < count && slot < count_; ++i) {
      while (slot < count_ && candidate_ids[slot] < doc_ids[i]) {
        ++slot;
      }
      if (slot >= count_ || candidate_ids[slot] != doc_ids[i]) {
      } else if (OB_FAIL(add_score(slot, scorer(i)))) {
        LOG_WARN("failed to supplement compact candidate", K(ret), K(slot), K(doc_ids[i]));
      } else {
        ++slot;
        ++scored_count;
      }
    }
    return ret;
  }

  // Mark alive candidates whose accumulated score is not greater than min_required_score
  // as pruned. Comparison semantics match an output predicate of score > min_score.
  int prune_below(const double min_required_score);
  // Before output starts, remove pruned slots and preserve the surviving ID order.
  int prune_and_compact(const double min_required_score);

  // Advance to and return the first candidate whose doc_id is not less than target and
  // whose score is greater than min_score. The cursor never moves backward and remains
  // on the returned candidate, so advancing to the current candidate is idempotent.
  int advance_to(
      const uint64_t target,
      const double min_score,
      uint64_t &doc_id,
      double &score);

  // Move past the current candidate and return the next alive candidate whose score is
  // greater than min_score. Before the first output, iteration starts at slot zero.
  int next(const double min_score, uint64_t &doc_id, double &score);

  TO_STRING_KV(K_(capacity), K_(count), K_(alive_count), K_(cursor), KP_(alive_bits),
      K_(is_inited));

private:
  bool is_valid_index(const int64_t idx) const { return idx >= 0 && idx < count_; }

private:
  ObIAllocator *allocator_;
  ObFixedArray<uint64_t, ObIAllocator> doc_ids_;
  ObFixedArray<double, ObIAllocator> scores_;
  ObBitVector *alive_bits_;
  int64_t capacity_;
  int64_t count_;
  int64_t alive_count_;
  int64_t cursor_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(SparseCandidateBuffer);
};

} // namespace sql
} // namespace oceanbase
#endif
