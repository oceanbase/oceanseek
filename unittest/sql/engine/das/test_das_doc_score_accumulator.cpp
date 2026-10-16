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
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(11, rowid);
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(12, rowid);
  EXPECT_DOUBLE_EQ(3.0, score);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
  EXPECT_TRUE(iter.iter_end());
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));

  ASSERT_EQ(OB_SUCCESS, iter.open(
      accumulator, DocScoreIter::ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(10, rowid);
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, iter.advance(10, rowid, score));
  EXPECT_EQ(10, rowid);
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(11, rowid);

  ASSERT_EQ(OB_SUCCESS, iter.open(
      accumulator, DocScoreIter::ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD));
  EXPECT_EQ(OB_ITER_END, iter.advance(UINT64_MAX, rowid, score));
  EXPECT_TRUE(iter.iter_end());
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));

  ASSERT_EQ(OB_SUCCESS, iter.open(
      accumulator, DocScoreIter::ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD));
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
  ASSERT_EQ(OB_SUCCESS, iter.open(
      accumulator, DocScoreIter::ProjectionPolicy::INCLUDE_PRESENCE_AT_ZERO_THRESHOLD));
  ASSERT_EQ(OB_SUCCESS, iter.next(rowid, score));
  EXPECT_EQ(UINT64_MAX, rowid);
  EXPECT_EQ(OB_ITER_END, iter.next(rowid, score));
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
