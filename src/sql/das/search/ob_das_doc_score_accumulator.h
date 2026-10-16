/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef SQL_DAS_SEARCH_OB_DAS_DOC_SCORE_ACCUMULATOR_H_
#define SQL_DAS_SEARCH_OB_DAS_DOC_SCORE_ACCUMULATOR_H_

#include "sql/das/search/ob_das_search_define.h"
#include "sql/engine/ob_bit_vector.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{

// uint64_t-word bit utilities shared by the dense doc-score bitset scans
// (DocScoreAccumulator / DocScoreIter) and the BMM filter drain sink.
namespace das_bitset_helper
{

static constexpr int64_t WORD_BITS = ObBitVector::WORD_BITS;      // 64
static constexpr int64_t WORD_SHIFT = 6;                          // log2(WORD_BITS)
static constexpr int64_t WORD_MASK = WORD_BITS - 1;               // low bit-index mask
static_assert(WORD_BITS == (1LL << WORD_SHIFT), "WORD_SHIFT must equal log2(WORD_BITS)");

// offset -> containing word index.
OB_INLINE constexpr int64_t word_index(const int64_t offset) { return offset >> WORD_SHIFT; }
// offset -> bit position within its word.
OB_INLINE constexpr int64_t bit_index(const int64_t offset) { return offset & WORD_MASK; }
// Single-bit mask.
OB_INLINE constexpr uint64_t bit_mask(const int64_t bit) { return 1ULL << bit; }
// Mask of the low n bits. Safe for n in [0, WORD_BITS]; returns ~0ULL when n >= WORD_BITS
// (where (1ULL << n) - 1 would be UB).
OB_INLINE constexpr uint64_t low_bits_mask(const int64_t n)
{ return n >= WORD_BITS ? ~0ULL : (bit_mask(n) - 1ULL); }
// offset -> single-bit mask.
OB_INLINE constexpr uint64_t offset_mask(const int64_t offset) { return bit_mask(offset & WORD_MASK); }
// capacity -> number of words needed.
OB_INLINE constexpr int64_t word_count(const int64_t capacity)
{ return (capacity + WORD_BITS - 1) >> WORD_SHIFT; }

// Set the bit at `offset` in `word`, returning the new word. `offset` may be a global
// bit offset or a local bit position in [0, WORD_BITS); offset_mask normalizes either.
OB_INLINE constexpr uint64_t set_bit(uint64_t word, const int64_t offset)
{ return word | offset_mask(offset); }
// Clear the bit at `offset` in `word`, returning the new word.
OB_INLINE constexpr uint64_t clear_bit(uint64_t word, const int64_t offset)
{ return word & ~offset_mask(offset); }
// Index of the lowest set bit.
OB_INLINE constexpr int64_t lowest_set_bit(const uint64_t word) { return __builtin_ctzll(word); }
// Clear the lowest set bit of word.
OB_INLINE constexpr uint64_t clear_lowest_set_bit(const uint64_t word) { return word & (word - 1); }
// Clear all bits below `bit`, keeping bits [bit, WORD_BITS). Safe for bit in [0, WORD_BITS].
OB_INLINE constexpr uint64_t clear_bits_below(const uint64_t word, const int64_t bit)
{ return word & ~low_bits_mask(bit); }
// Mask off bits that fall beyond the window tail. word_base is the offset of this
// word's bit 0; capacity is the window size. Returns 0 when the word is fully past
// the window, the low-valid-bits slice when it straddles the tail, else word as-is.
OB_INLINE constexpr uint64_t mask_window_tail(
    const uint64_t word, const int64_t word_base, const int64_t capacity)
{
  const int64_t valid_bits = capacity - word_base;
  return valid_bits <= 0        ? 0ULL
       : valid_bits < WORD_BITS ? (word & low_bits_mask(valid_bits))
                                : word;
}

} // namespace das_bitset_helper

// Accumulates per-doc scores over a contiguous doc_id window [start_id_, start_id_ + capacity_).
// Unify and narrow the input parameters directly to uint64_t type for consistency.
// Backed by a dense scores array and a presence bitset for future SIMD optimization.
// Scores at unset offsets are unspecified; setting a presence bit must initialize the
// corresponding score before it becomes observable.
class DocScoreAccumulator final
{
public:
  friend class DocScoreIter;
  static constexpr int64_t SCORE_BATCH_SIZE = 4;
  DocScoreAccumulator();
  ~DocScoreAccumulator() { reset(); }

  int init(const int64_t capacity, ObIAllocator &allocator);
  int reuse_with_start_id(const uint64_t start_id, uint64_t &max_id);
  void reset();
  // Add a zero-score document to the result domain without overwriting an already
  // accumulated score. Used by query-optional BMM where filter hits own presence.
  int seed_doc(const uint64_t doc_id);
  // Batch Invariant: doc_ids must be in ascending order.
  int seed_docs(const uint64_t *doc_ids, const int64_t count);
  int add_dim(const uint64_t doc_id, const double score);
  // Batch Invariant: doc_ids must be in ascending order.
  int add_dim_batch(const uint64_t *doc_ids, const double *scores, const int64_t count);
  // Ascending IDs, no filter mask. scorer.eval_batch() evaluates each full SIMD
  // batch, while scorer() handles the scalar tail. The scorer may not mutate or
  // observe the accumulator or input IDs; accumulation preserves add_dim_batch's order.
  template <typename Scorer>
  OB_INLINE int add_scored_dim_batch(
      const uint64_t *doc_ids,
      const int64_t count,
      const Scorer &scorer)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count <= 0)) {
      // Match the empty-batch contract of add_dim_batch.
    } else if (OB_UNLIKELY(nullptr == doc_ids || !is_valid() || has_filter_mask())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid directly scored batch", K(ret), KP(doc_ids), K(count), K_(is_inited));
    } else if (OB_UNLIKELY(!is_doc_in_window(doc_ids[0])
                          || !is_doc_in_window(doc_ids[count - 1]))) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("directly scored batch endpoints out of range", K(ret), K(count),
          K(doc_ids[0]), K(doc_ids[count - 1]), K_(start_id), K_(capacity));
    } else {
      double *acc_scores = scores_.get_data();
      uint64_t *words = bitset_->reinterpret_data<uint64_t>();
      int64_t i = 0;
      for (; i + SCORE_BATCH_SIZE - 1 < count; i += SCORE_BATCH_SIZE) {
        uint64_t offsets[SCORE_BATCH_SIZE];
        for (int64_t lane = 0; lane < SCORE_BATCH_SIZE; ++lane) {
          offsets[lane] = doc_ids[i + lane] - start_id_;
        }
        double values[SCORE_BATCH_SIZE];
        scorer.eval_batch(i, SCORE_BATCH_SIZE, values);
        for (int64_t lane = 0; lane < SCORE_BATCH_SIZE; ++lane) {
          const uint64_t offset = offsets[lane];
          const uint64_t mask = 1ULL << (offset & 63);
          if (0 == (words[offset >> 6] & mask)) {
            acc_scores[offset] = values[lane];
            words[offset >> 6] |= mask;
            max_score_ = MAX(max_score_, values[lane]);
            is_empty_ = false;
          } else {
            acc_scores[offset] += values[lane];
            max_score_ = MAX(max_score_, acc_scores[offset]);
          }
        }
      }
      for (; i < count; ++i) {
        const uint64_t offset = doc_ids[i] - start_id_;
        const double score = scorer(i);
        const uint64_t mask = 1ULL << (offset & 63);
        if (0 == (words[offset >> 6] & mask)) {
          acc_scores[offset] = score;
          words[offset >> 6] |= mask;
          max_score_ = MAX(max_score_, score);
          is_empty_ = false;
        } else {
          acc_scores[offset] += score;
          max_score_ = MAX(max_score_, acc_scores[offset]);
        }
      }
    }
    return ret;
  }
  // Accumulate into an existing doc. The caller must ensure the doc is active in bitset_.
  OB_INLINE int add_existing_dim(const uint64_t doc_id, const double score)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid doc score accumulator", K(ret), K_(is_inited), KP_(bitset));
    } else if (OB_UNLIKELY(!is_doc_in_window(doc_id))) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("doc_id is out of range", K(ret), K(doc_id), K(start_id_), K_(capacity));
    } else {
      const int64_t offset = static_cast<int64_t>(doc_id - start_id_);
      scores_.at(offset) += score;
      max_score_ = MAX(max_score_, scores_.at(offset));
    }
    return ret;
  }

  OB_INLINE bool is_valid() const { return is_inited_ && nullptr != bitset_; }
  OB_INLINE double get_max_score() const { return max_score_; }
  OB_INLINE bool is_empty() const { return is_empty_; }
  OB_INLINE int64_t get_capacity() const { return capacity_; }
  OB_INLINE uint64_t get_start_id() const { return start_id_; }
  OB_INLINE const uint64_t *get_bitset_words() const
  { return bitset_->reinterpret_data<uint64_t>(); }

  // Stop once the count exceeds stop_after; only counts <= stop_after are exact.
  OB_INLINE int64_t get_active_count(const int64_t stop_after = INT64_MAX) const
  {
    int64_t count = 0;
    if (OB_LIKELY(is_valid())) {
      const uint64_t *words = bitset_->reinterpret_data<uint64_t>();
      const int64_t word_cnt = das_bitset_helper::word_count(capacity_);
      for (int64_t word_idx = 0; word_idx < word_cnt && count <= stop_after; ++word_idx) {
        const int64_t word_base = word_idx * das_bitset_helper::WORD_BITS;
        count += __builtin_popcountll(
            das_bitset_helper::mask_window_tail(words[word_idx], word_base, capacity_));
      }
    }
    return count;
  }

  int get_score(const uint64_t doc_id, double &score) const;
  // Remove docs whose accumulated score <= min_required_score from the bitset and
  // update max_score_ and is_empty_ accordingly.
  // Used for per-doc competitive filtering between non-essential dimensions.
  int filter_below_score(const double min_required_score);
  // Keep only documents whose corresponding bit is present in `mask`.
  // The caller must provide a mask with the same capacity as this accumulator.
  int retain_by_mask(const ObBitVector &mask);
  void clear_all();

  // Filter mask: when set, add_dim/add_dim_batch skip docs whose bit is NOT set in the mask.
  // The mask is NOT owned by this class; caller must ensure the lifetime of the mask.
  OB_INLINE void set_filter_mask(const ObBitVector *filter_mask) { filter_mask_ = filter_mask; }
  OB_INLINE void clear_filter_mask() { filter_mask_ = nullptr; }
  OB_INLINE bool has_filter_mask() const { return filter_mask_ != nullptr; }

  TO_STRING_KV(K_(start_id), K_(capacity), K_(max_score), K_(is_empty), K_(is_inited));

private:
  OB_INLINE bool is_doc_in_window(const uint64_t doc_id) const
  {
    return capacity_ > 0
        && doc_id >= start_id_
        && doc_id - start_id_ < static_cast<uint64_t>(capacity_);
  }

  uint64_t start_id_;
  ObFixedArray<double, ObIAllocator> scores_;
  ObBitVector *bitset_;
  int64_t capacity_;
  double max_score_;
  ObIAllocator *allocator_;
  const ObBitVector *filter_mask_;
  bool is_empty_;
  bool is_inited_;
};

// Projects (doc_id, score) pairs from a DocScoreAccumulator's set bits.
// next() and advance() share one consuming scan cursor (curr_word_/curr_word_idx_):
// each call returns the next un-consumed set bit and clears it from the local
// word cache. The backing bitset_words_ array is never modified by this iter;
// only the local curr_word_ cache is consumed.
//
// High-water mark: curr_id_ (when curr_valid_ is true) tracks the highest doc_id
// ever returned by next()/advance(). advance() clamps its target to curr_id_ when
// target < curr_id_, so backward seeks never re-scan consumed bits. The bit at 
// curr_id_ itself may be re-found by the re-scan; if its score still meets min_score
// it is returned (idempotent), otherwise it is skipped and the scan continues.
// Mutating the accumulator (clear_all / reuse_with_start_id) will invalidate
// the cached iter state; callers must re-open() or reset() the iter after such mutations.
class DocScoreIter final
{
public:
  enum class ProjectionPolicy
  {
    STRICTLY_ABOVE_THRESHOLD,
    // Query-optional semantics: while the competitive threshold is zero, every
    // present filter document is projectable, including its zero-score seed.
    INCLUDE_PRESENCE_AT_ZERO_THRESHOLD,
  };

  DocScoreIter();
  virtual ~DocScoreIter() = default;
  int open(
      const DocScoreAccumulator &acc,
      const ProjectionPolicy projection_policy = ProjectionPolicy::STRICTLY_ABOVE_THRESHOLD);
  // Consume the next set bit. In strict mode docs with score <= min_score are skipped.
  int next(uint64_t &next_id, double &score, double min_score = 0.0);
  // Advance to the first un-consumed set bit >= target and consume it.
  // Projection uses the same threshold predicate as next().
  int advance(uint64_t target, uint64_t &out_id, double &out_score, double min_score = 0.0);
  OB_INLINE bool is_valid() const { return nullptr != accumulator_ && nullptr != bitset_words_; }
  // Whether the current window has reached sticky EOF. reset() leaves no active
  // window, so iter_end() remains true until the next successful open().
  OB_INLINE bool iter_end() const { return iter_end_; }
  void reset();
private:
  // Return the lowest un-consumed set bit from curr_word_ (consuming it), or
  // load successive words until a set bit is found or the bitset is exhausted.
  // Docs rejected by the selected projection policy are consumed but not returned.
  int consume_next_set_bit(uint64_t &out_id, double &out_score, double min_score);
  OB_INLINE bool should_project(const double score, const double min_score) const
  {
    return (ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD == projection_policy_
            && min_score <= 0.0)
        || score > min_score;
  }

  const DocScoreAccumulator *accumulator_;
  const uint64_t *bitset_words_;
  int64_t curr_word_idx_;
  int64_t curr_word_base_idx_;
  int64_t word_cnt_;
  uint64_t curr_word_;
  // High-water mark: the highest doc_id ever returned. Used by advance() to
  // clamp target so backward seeks never re-scan consumed bits.
  uint64_t curr_id_;
  ProjectionPolicy projection_policy_;
  bool curr_valid_;
  bool iter_end_;
};

} // namespace sql
} // namespace oceanbase
#endif
