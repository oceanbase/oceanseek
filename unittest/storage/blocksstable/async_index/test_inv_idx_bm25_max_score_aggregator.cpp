/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_bm25_max_score_aggregator.h"

#include <gtest/gtest.h>
#include <vector>

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

class InvIdxBM25MaxScoreAggregatorTest : public ::testing::Test
{
protected:
  InvIdxBM25MaxScoreAggregatorTest() {}
  virtual ~InvIdxBM25MaxScoreAggregatorTest() {}

  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp() {}
  virtual void TearDown() {}

  static int fill_array(const std::vector<uint64_t> &values, common::ObArray<uint64_t> &array)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < values.size(); ++i) {
      if (OB_FAIL(array.push_back(values[i]))) {
        LOG_WARN("failed to push value", K(ret), K(i), K(values[i]));
      }
    }
    return ret;
  }

  static bool has_pair(
      const common::ObIArray<ObInvIdxBM25ParamPair> &frontier_pairs,
      const uint64_t token_freq,
      const uint64_t doc_length)
  {
    bool found = false;
    for (int64_t i = 0; !found && i < frontier_pairs.count(); ++i) {
      const ObInvIdxBM25ParamPair &pair = frontier_pairs.at(i);
      if (pair.token_freq_ == token_freq && pair.doc_length_ == doc_length) {
        found = true;
      }
    }
    return found;
  }

  static void expect_pairs(
      const common::ObIArray<ObInvIdxBM25ParamPair> *frontier_pairs,
      const std::vector<std::pair<uint64_t, uint64_t>> &expected_pairs)
  {
    ASSERT_NE(nullptr, frontier_pairs);
    ASSERT_EQ(expected_pairs.size(), frontier_pairs->count());
    for (int64_t i = 0; i < expected_pairs.size(); ++i) {
      EXPECT_TRUE(has_pair(*frontier_pairs, expected_pairs[i].first, expected_pairs[i].second))
          << "missing pair tf=" << expected_pairs[i].first << ", dl=" << expected_pairs[i].second;
    }
  }

  static void aggregate_and_expect(
      const int64_t max_pair_count,
      const std::vector<uint64_t> &token_freq_values,
      const std::vector<uint64_t> &doc_length_values,
      const std::vector<std::pair<uint64_t, uint64_t>> &expected_pairs)
  {
    int ret = OB_SUCCESS;
    common::ObArenaAllocator allocator(common::ObModIds::TEST);
    common::ObArray<uint64_t> token_freqs;
    common::ObArray<uint64_t> doc_lengths;
    ObInvIdxBM25MaxScoreAggregator aggregator;
    const common::ObIArray<ObInvIdxBM25ParamPair> *frontier_pairs = nullptr;

    ASSERT_EQ(OB_SUCCESS, fill_array(token_freq_values, token_freqs));
    ASSERT_EQ(OB_SUCCESS, fill_array(doc_length_values, doc_lengths));
    ASSERT_EQ(OB_SUCCESS, aggregator.init(allocator, max_pair_count));
    ret = aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs);
    ASSERT_EQ(OB_SUCCESS, ret);
    expect_pairs(frontier_pairs, expected_pairs);
  }
};

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_error_handling)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<uint64_t> token_freqs;
  common::ObArray<uint64_t> doc_lengths;
  const common::ObIArray<ObInvIdxBM25ParamPair> *frontier_pairs = nullptr;

  ASSERT_EQ(OB_SUCCESS, token_freqs.push_back(1));
  ASSERT_EQ(OB_SUCCESS, doc_lengths.push_back(10));
  {
    ObInvIdxBM25MaxScoreAggregator aggregator;
    ASSERT_EQ(OB_NOT_INIT, aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs));
    ASSERT_EQ(nullptr, frontier_pairs);
  }
  {
    ObInvIdxBM25MaxScoreAggregator aggregator;
    ASSERT_EQ(OB_INVALID_ARGUMENT, aggregator.init(allocator, 0));
  }
  {
    ObInvIdxBM25MaxScoreAggregator aggregator;
    common::ObArray<uint64_t> empty_token_freqs;
    common::ObArray<uint64_t> empty_doc_lengths;
    ASSERT_EQ(OB_SUCCESS, aggregator.init(allocator, 2));
    ASSERT_EQ(OB_INVALID_ARGUMENT, aggregator.aggregate(
        empty_token_freqs, empty_doc_lengths, frontier_pairs));
    ASSERT_EQ(OB_SUCCESS, token_freqs.push_back(2));
    ASSERT_EQ(OB_INVALID_ARGUMENT, aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs));
  }
}

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_basic_frontier_cases)
{
  aggregate_and_expect(2, {10}, {100}, {{10, 100}});
  aggregate_and_expect(4, {10, 5, 8}, {100, 200, 150}, {{10, 100}});
  aggregate_and_expect(4, {5, 8, 10}, {200, 150, 100}, {{10, 100}});
  aggregate_and_expect(4, {10, 8, 5}, {300, 200, 100}, {{10, 300}, {8, 200}, {5, 100}});
  aggregate_and_expect(4, {10, 10, 10}, {100, 100, 120}, {{10, 100}});
  aggregate_and_expect(2, {10, 5}, {300, 100}, {{10, 300}, {5, 100}});
  aggregate_and_expect(4, {10, 20, 30, 40, 30, 20, 35}, {300, 400, 500, 250, 220, 210, 200}, {{40, 250}, {35, 200}});
}

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_multiple_pairs_at_writer_limit)
{
  aggregate_and_expect(12,
      {10, 20, 30, 40, 50},
      {100, 200, 300, 400, 500},
      {{10, 100}, {20, 200}, {30, 300}, {40, 400}, {50, 500}});
}

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_fallback_at_writer_limit)
{
  aggregate_and_expect(12, {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150},
      {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500}, {{150, 100}});
}

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_fallback_cases)
{
  aggregate_and_expect(2, {10, 8, 5}, {300, 200, 100}, {{10, 100}});
  aggregate_and_expect(2, {10, 8, 5, 20, 1}, {300, 200, 100, 400, 50}, {{20, 50}});
  aggregate_and_expect(2, {10, 20, 30, 40, 30, 20, 35}, {300, 400, 500, 250, 220, 210, 200}, {{40, 200}});
}

TEST_F(InvIdxBM25MaxScoreAggregatorTest, test_reuse_and_reset)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  common::ObArray<uint64_t> token_freqs;
  common::ObArray<uint64_t> doc_lengths;
  const common::ObIArray<ObInvIdxBM25ParamPair> *frontier_pairs = nullptr;
  ObInvIdxBM25MaxScoreAggregator aggregator;

  ASSERT_EQ(OB_SUCCESS, aggregator.init(allocator, 4));
  ASSERT_EQ(OB_SUCCESS, fill_array({10, 8, 5}, token_freqs));
  ASSERT_EQ(OB_SUCCESS, fill_array({300, 200, 100}, doc_lengths));
  ASSERT_EQ(OB_SUCCESS, aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs));
  expect_pairs(frontier_pairs, {{10, 300}, {8, 200}, {5, 100}});

  aggregator.reuse();
  token_freqs.reset();
  doc_lengths.reset();
  ASSERT_EQ(OB_SUCCESS, fill_array({3}, token_freqs));
  ASSERT_EQ(OB_SUCCESS, fill_array({30}, doc_lengths));
  ASSERT_EQ(OB_SUCCESS, aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs));
  expect_pairs(frontier_pairs, {{3, 30}});

  aggregator.reset();
  ASSERT_EQ(OB_NOT_INIT, aggregator.aggregate(token_freqs, doc_lengths, frontier_pairs));
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
