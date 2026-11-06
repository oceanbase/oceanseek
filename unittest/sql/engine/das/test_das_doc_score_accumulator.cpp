/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

class TestDASDocScoreAccumulator : public ::testing::Test
{
public:
  TestDASDocScoreAccumulator() : allocator_("DocScoreAccTest") {}

protected:
  ObArenaAllocator allocator_;
};

TEST_F(TestDASDocScoreAccumulator, duplicate_ids_in_batch_preserve_scalar_accumulation_order)
{
  for (const int64_t capacity : {1, 64, 257, 16384}) {
    for (const uint64_t start : {uint64_t(0), UINT64_MAX - capacity + 1}) {
      DocScoreAccumulator expected;
      DocScoreAccumulator actual;
      ASSERT_EQ(OB_SUCCESS, expected.init(capacity, allocator_));
      ASSERT_EQ(OB_SUCCESS, actual.init(capacity, allocator_));
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, expected.reuse_with_start_id(start, max_id));
      ASSERT_EQ(OB_SUCCESS, actual.reuse_with_start_id(start, max_id));
      uint64_t ids[513];
      double scores[513];
      for (int64_t i = 0; i < 513; ++i) {
        ids[i] = start + MIN(i / 2, capacity - 1);
        scores[i] = i % 5 == 0 ? 1e16 : 0.125 * (1 + i % 7);
      }
      for (int64_t pass = 0; pass < 3; ++pass) {
        for (int64_t i = 0; i < 513; ++i) {
          ASSERT_EQ(OB_SUCCESS, expected.add_dim(ids[i], scores[i]));
        }
        ASSERT_EQ(OB_SUCCESS, actual.add_dim_batch(ids, scores, 513));
        EXPECT_EQ(expected.is_empty(), actual.is_empty());
        EXPECT_EQ(expected.get_active_count(), actual.get_active_count());
        const double expected_max = expected.get_max_score();
        const double actual_max = actual.get_max_score();
        EXPECT_EQ(0, MEMCMP(&expected_max, &actual_max, sizeof(double)));
        EXPECT_EQ(0, MEMCMP(expected.get_bitset_words(), actual.get_bitset_words(),
            ((capacity + 63) / 64) * sizeof(uint64_t)));
        for (int64_t i = 0; i < capacity; ++i) {
          if (0 != (expected.get_bitset_words()[i >> 6] & (1ULL << (i & 63)))) {
            double expected_score = 0;
            double actual_score = 0;
            ASSERT_EQ(OB_SUCCESS, expected.get_score(start + i, expected_score));
            ASSERT_EQ(OB_SUCCESS, actual.get_score(start + i, actual_score));
            EXPECT_EQ(0, MEMCMP(&expected_score, &actual_score, sizeof(double)));
          }
        }
        ASSERT_EQ(OB_SUCCESS, expected.filter_below_score(1.0));
        ASSERT_EQ(OB_SUCCESS, actual.filter_below_score(1.0));
      }
    }
  }
}

// Accumulate overlapping score batches under a filter mask spanning offsets 0, 63, 64, and
// 129, ensuring only masked documents remain, duplicate scores sum, and max/empty state
// stays consistent.
TEST_F(TestDASDocScoreAccumulator, batch_accumulation_respects_filter_mask_across_words)
{
  static constexpr int64_t CAPACITY = 130;
  static constexpr uint64_t START_ID = 1000;
  DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(CAPACITY, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(START_ID, max_id));
  ASSERT_EQ(START_ID + CAPACITY - 1, max_id);

  void *mask_buf = allocator_.alloc(ObBitVector::memory_size(CAPACITY));
  ASSERT_NE(nullptr, mask_buf);
  ObBitVector *filter_mask = to_bit_vector(mask_buf);
  filter_mask->init(CAPACITY);
  filter_mask->set(0);
  filter_mask->set(63);
  filter_mask->set(64);
  filter_mask->set(CAPACITY - 1);
  accumulator.set_filter_mask(filter_mask);

  const uint64_t first_ids[] = {1000, 1001, 1063, 1064, 1129};
  const double first_scores[] = {1.0, 50.0, 2.0, 3.0, 4.0};
  const uint64_t second_ids[] = {1000, 1064, 1128, 1129};
  const double second_scores[] = {1.0, 5.0, 50.0, 6.0};
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(
      first_ids, first_scores, ARRAYSIZEOF(first_ids)));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(
      second_ids, second_scores, ARRAYSIZEOF(second_ids)));

  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1000, score));
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1001, score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1063, score));
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1064, score));
  EXPECT_DOUBLE_EQ(8.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1128, score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1129, score));
  EXPECT_DOUBLE_EQ(10.0, score);
  EXPECT_DOUBLE_EQ(10.0, accumulator.get_max_score());
  EXPECT_FALSE(accumulator.is_empty());
}

// Check every inclusive boundary, including an empty prefix, word boundaries,
// zero-score presence, dense edges, window reuse and uint64 overflow boundaries.
TEST_F(TestDASDocScoreAccumulator, active_range_matches_presence_within_boundary)
{
  constexpr int64_t CAPACITY = 130;
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(CAPACITY, allocator_));
  for (const uint64_t start : {1000ULL, UINT64_MAX - 129ULL}) {
    for (int pattern = 0; pattern < 5; ++pattern) {
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(start, max_id));
      bool active[CAPACITY] = {};
      for (int64_t i = 0; i < CAPACITY; ++i) {
        active[i] = (pattern == 1 && (i == 0 || i == 129))
            || (pattern == 2 && (i == 63 || i == 64))
            || (pattern == 3 && i == 65) || pattern == 4;
        if (active[i]) {
          ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(start + i));
        }
      }
      uint64_t first = 0;
      uint64_t last = 0;
      EXPECT_FALSE(accumulator.get_active_range(accumulator.get_start_id(), start - 1, first, last));
      for (int64_t limit = 0; limit < CAPACITY; ++limit) {
        int64_t expected_first = -1;
        int64_t expected_last = -1;
        for (int64_t i = 0; i <= limit; ++i) {
          if (active[i]) {
            if (expected_first < 0) { expected_first = i; }
            expected_last = i;
          }
        }
        const bool found = accumulator.get_active_range(accumulator.get_start_id(), start + limit, first, last);
        ASSERT_EQ(expected_first >= 0, found) << pattern << ":" << limit;
        if (found) {
          EXPECT_EQ(start + expected_first, first);
          EXPECT_EQ(start + expected_last, last);
        }
      }
      const bool found = accumulator.get_active_range(accumulator.get_start_id(), UINT64_MAX, first, last);
      EXPECT_EQ(pattern != 0, found);
      if (found) {
        EXPECT_LE(last, max_id);
      }
    }
  }
}

// Each NE pruning round changes the bounds without changing the bitmap's base.
TEST_F(TestDASDocScoreAccumulator, active_range_shrinks_after_pruning)
{
  DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(1000, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1000, max_id));
  const uint64_t ids[] = {1020, 1200, 1250, 1800};
  const double scores[] = {1.0, 3.0, 5.0, 1.0};
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(ids, scores, ARRAYSIZEOF(ids)));
  uint64_t first = 0;
  uint64_t last = 0;
  ASSERT_TRUE(accumulator.get_active_range(accumulator.get_start_id(), max_id, first, last));
  EXPECT_EQ(1020, first);
  EXPECT_EQ(1800, last);
  // Unchanged candidates reuse the previous bounds, including in/out aliasing.
  ASSERT_TRUE(accumulator.get_active_range(first, last, first, last));
  EXPECT_EQ(1020, first);
  EXPECT_EQ(1800, last);
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(1.0));
  ASSERT_TRUE(accumulator.get_active_range(first, last, first, last));
  EXPECT_EQ(1200, first);
  EXPECT_EQ(1250, last);
  EXPECT_EQ(1000, accumulator.get_start_id());
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(3.0));
  ASSERT_TRUE(accumulator.get_active_range(first, last, first, last));
  EXPECT_EQ(1250, first);
  EXPECT_EQ(first, last);
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(5.0));
  EXPECT_FALSE(accumulator.get_active_range(first, last, first, last));
  // A new batch initializes bounds again instead of inheriting the old singleton.
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(2000, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(2001));
  ASSERT_TRUE(accumulator.get_active_range(2000, max_id, first, last));
  EXPECT_EQ(2001, first);
  EXPECT_EQ(first, last);
}

// Restrict both ends inside one word; out-of-range active bits must not leak in.
TEST_F(TestDASDocScoreAccumulator, active_range_masks_both_bounds)
{
  DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(130, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1000, max_id));
  for (const uint64_t id : {1000, 1005, 1010, 1063, 1064, 1129}) {
    ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(id));
  }
  uint64_t first = 1001;
  uint64_t last = 1009;
  ASSERT_TRUE(accumulator.get_active_range(first, last, first, last));
  EXPECT_EQ(1005, first);
  EXPECT_EQ(1005, last);
  EXPECT_FALSE(accumulator.get_active_range(1006, 1009, first, last));
  EXPECT_FALSE(accumulator.get_active_range(1130, UINT64_MAX, first, last));
  EXPECT_FALSE(accumulator.get_active_range(1010, 1000, first, last));
  ASSERT_TRUE(accumulator.get_active_range(1063, 1064, first, last));
  EXPECT_EQ(1063, first);
  EXPECT_EQ(1064, last);
}

// Prune documents across bitset words at a score threshold, ensuring scores equal to or below
// it are removed, the surviving maximum is recomputed, and the accumulator becomes empty
// when all candidates are removed.
TEST_F(TestDASDocScoreAccumulator, filter_below_score_uses_strict_boundary_across_words)
{
  static constexpr int64_t CAPACITY = 130;
  static constexpr uint64_t START_ID = 100;
  DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(CAPACITY, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(START_ID, max_id));

  const uint64_t doc_ids[] = {100, 163, 164, 229};
  const double scores[] = {1.0, 2.0, 3.0, 4.0};
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(doc_ids, scores, ARRAYSIZEOF(doc_ids)));
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(2.0));

  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(100, score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(163, score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(164, score));
  EXPECT_DOUBLE_EQ(3.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(229, score));
  EXPECT_DOUBLE_EQ(4.0, score);
  EXPECT_DOUBLE_EQ(4.0, accumulator.get_max_score());
  EXPECT_FALSE(accumulator.is_empty());

  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(4.0));
  EXPECT_DOUBLE_EQ(0.0, accumulator.get_max_score());
  EXPECT_TRUE(accumulator.is_empty());
}

// Mix next() with forward, backward, and thresholded advance() calls across bitset words,
// ensuring one monotonic cursor prevents rewinds or duplicates and reopening after reuse
// exposes no stale bits.
TEST_F(TestDASDocScoreAccumulator, iterator_mixes_next_and_advance_without_rewinding)
{
  static constexpr int64_t CAPACITY = 130;
  DocScoreAccumulator accumulator;
  DocScoreIter iter;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(CAPACITY, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));

  const uint64_t doc_ids[] = {100, 163, 164, 170, 228};
  const double scores[] = {1.0, 5.0, 3.0, 6.0, 4.0};
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(doc_ids, scores, ARRAYSIZEOF(doc_ids)));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));

  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.next(doc_id, score));
  EXPECT_EQ(100, doc_id);
  EXPECT_DOUBLE_EQ(1.0, score);
  // The score equal to the output threshold is consumed but not returned.
  ASSERT_EQ(OB_SUCCESS, iter.advance(164, doc_id, score, 3.0));
  EXPECT_EQ(170, doc_id);
  EXPECT_DOUBLE_EQ(6.0, score);
  // A backward target stays at the iterator high-water mark.
  ASSERT_EQ(OB_SUCCESS, iter.advance(120, doc_id, score, 3.0));
  EXPECT_EQ(170, doc_id);
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(doc_id, score, 3.0));
  EXPECT_EQ(228, doc_id);
  EXPECT_DOUBLE_EQ(4.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(doc_id, score, 3.0));

  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1000, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1001, 7.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1065, 8.0));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(doc_id, score));
  EXPECT_EQ(1001, doc_id);
  EXPECT_DOUBLE_EQ(7.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(doc_id, score));
  EXPECT_EQ(1065, doc_id);
  EXPECT_DOUBLE_EQ(8.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(doc_id, score));
}

// Check seed idempotence, zero-score projection, iterator end/reset, and strict-threshold behavior,
// then reuse a window ending at UINT64_MAX to ensure its bound saturates and out-of-range row IDs
// are rejected without wraparound.
TEST_F(TestDASDocScoreAccumulator, seed_projection_and_uint64_tail_contract)
{
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(4, allocator_));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(10, max_id));
  EXPECT_EQ(13, max_id);
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(10));
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(11));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(11, 2.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(11));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(12, 3.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(12));
  double score = -1.0;
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(10, score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(11, score));
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(12, score));
  EXPECT_DOUBLE_EQ(3.0, score);

  DocScoreIter iter;
  uint64_t rowid = 0;
  EXPECT_TRUE(iter.iter_end());
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  EXPECT_FALSE(iter.iter_end());
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score, 0.0));
  EXPECT_EQ(11, rowid);
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score, 0.0));
  EXPECT_EQ(12, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score, 0.0));
  EXPECT_TRUE(iter.iter_end());
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score, 0.0));

  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(10, rowid);
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.advance(10, rowid, score));
  EXPECT_EQ(10, rowid);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(11, rowid);

  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  EXPECT_EQ(OB_ITER_END, iter.advance(UINT64_MAX, rowid, score));
  EXPECT_TRUE(iter.iter_end());
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));

  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score, 2.0));
  EXPECT_EQ(12, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score, 2.0));
  iter.reset();
  EXPECT_TRUE(iter.iter_end());
  EXPECT_FALSE(iter.is_valid());

  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(UINT64_MAX - 1, max_id));
  EXPECT_EQ(UINT64_MAX, max_id);
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, accumulator.seed_doc(UINT64_MAX - 2));
  EXPECT_EQ(OB_SUCCESS, accumulator.seed_doc(UINT64_MAX));
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, accumulator.seed_doc(0));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(UINT64_MAX, rowid);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
}

TEST_F(TestDASDocScoreAccumulator, compact_dense_ne_updates_accumulator_in_place)
{
  DocScoreAccumulator accumulator;
  uint64_t doc_ids[8];
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(130, allocator_));
  uint64_t end = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1000, end));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1000, 1.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1063, 2.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1064, 3.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1129, 4.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_scored_candidates(doc_ids, count, 2.0));
  ASSERT_EQ(2, count);
  EXPECT_EQ(1064u, doc_ids[0]);
  EXPECT_EQ(1129u, doc_ids[1]);
  ASSERT_EQ(OB_SUCCESS, accumulator.add_existing_dim(1064, 2.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_scored_candidates(doc_ids, count, 4.0));
  ASSERT_EQ(1, count);
  EXPECT_EQ(1, accumulator.get_active_count());
  EXPECT_DOUBLE_EQ(5.0, accumulator.get_max_score());
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, accumulator.get_score(1064, score));
  EXPECT_DOUBLE_EQ(5.0, score);
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_scored_candidates(doc_ids, count, 5.0));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(accumulator.is_empty());
  EXPECT_EQ(0, accumulator.get_active_count());
  EXPECT_DOUBLE_EQ(0.0, accumulator.get_max_score());
}

TEST_F(TestDASDocScoreAccumulator, bulk_collect_reuse_word_boundaries_and_capacity)
{
  DocScoreAccumulator accumulator;
  uint64_t doc_ids[129];
  int64_t count = 1;
  uint64_t end = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  EXPECT_EQ(0, count);
  ASSERT_EQ(OB_SUCCESS, accumulator.init(130, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1000, end));
  count = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      accumulator.collect_active_doc_ids(nullptr, ARRAYSIZEOF(doc_ids), count));
  EXPECT_EQ(0, count);
  count = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, accumulator.collect_active_doc_ids(doc_ids, 0, count));
  EXPECT_EQ(0, count);
  for (int64_t i = 0; i < 129; ++i) {
    ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1000 + i, i + 1.0));
  }
  ASSERT_EQ(OB_SUCCESS, accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  ASSERT_EQ(129, count);
  for (int64_t i = 0; i < 129; ++i) {
    EXPECT_EQ(1000u + i, doc_ids[i]);
  }
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_scored_candidates(doc_ids, count, 65.0));
  ASSERT_EQ(64, count);
  for (int64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(1065u + i, doc_ids[i]);
  }
  for (int64_t i = 0; i < 65; ++i) {
    ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1000 + i, i + 1.0));
  }
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1129, 130.0));
  EXPECT_EQ(OB_SIZE_OVERFLOW,
      accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  accumulator.clear_all();
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(1129, 7.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  ASSERT_EQ(1, count);
  EXPECT_EQ(1129u, doc_ids[0]);
  EXPECT_DOUBLE_EQ(7.0, accumulator.get_max_score());
  accumulator.clear_all();
  ASSERT_EQ(OB_SUCCESS, accumulator.collect_active_doc_ids(doc_ids, ARRAYSIZEOF(doc_ids), count));
  EXPECT_EQ(0, count);
}

TEST_F(TestDASDocScoreAccumulator, initial_and_zero_thresholds_have_distinct_projection)
{
  DocScoreAccumulator accumulator;
  DocScoreIter iter;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(8, allocator_));
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1, max_id));
  const uint64_t ids[] = {1, 2, 3, 4, 5};
  const double scores[] = {0.0, 0.0, 1.0, 0.0, 2.0};
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim_batch(ids, scores, ARRAYSIZEOF(ids)));
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  uint64_t id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter.next(id, score));
  EXPECT_EQ(1, id);
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(id, score, 0.0));
  EXPECT_EQ(3, id);
  ASSERT_EQ(OB_SUCCESS, iter.advance(4, id, score, 1.0));
  EXPECT_EQ(5, id);
  EXPECT_DOUBLE_EQ(2.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(id, score, 1.0));

  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(0.0));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.next(id, score));
  EXPECT_EQ(3, id);
  ASSERT_EQ(OB_SUCCESS, iter.next(id, score));
  EXPECT_EQ(5, id);
  EXPECT_EQ(OB_ITER_END, iter.next(id, score));

  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(1, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.seed_doc(1));
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  ASSERT_EQ(OB_SUCCESS, iter.advance(1, id, score));
  EXPECT_EQ(1, id);
  ASSERT_EQ(OB_SUCCESS, iter.open(accumulator));
  EXPECT_EQ(OB_ITER_END, iter.advance(1, id, score, 0.0));
  ASSERT_EQ(OB_SUCCESS, accumulator.filter_below_score(0.0));
  EXPECT_TRUE(accumulator.is_empty());
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
