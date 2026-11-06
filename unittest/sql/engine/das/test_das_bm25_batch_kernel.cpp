/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <cfenv>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>
#include <type_traits>
#include <utility>
#include "common/ob_target_specific.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_param.h"
#define private public
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#undef private
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "storage/blocksstable/async_index/ob_inv_idx_bm25_max_score_aggregator.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::blocksstable;

namespace
{

template <bool ONLY_EXISTING, typename Kernels>
struct CanCallFused
{
  template <typename K>
  static auto check(int) -> decltype(
      std::declval<DocScoreAccumulator &>().template add_scored_dim_batch_fused<ONLY_EXISTING>(
          std::declval<const ObPostingDocIdView &>(), std::declval<const ObPostingRelevanceView &>(),
          int64_t(1), std::declval<const ObBM25PrecomputedParams &>(), std::declval<const K &>(),
          std::declval<int64_t &>()), std::true_type());
  template <typename>
  static std::false_type check(...);
  static constexpr bool value = decltype(check<Kernels>(0))::value;
};

struct ArbitraryFusedKernel
{
  int64_t operator()(const uint64_t *, int64_t, uint64_t, int64_t,
      double *, uint64_t *, double &) const;
};

static_assert(!std::is_convertible<BM25AccumulationKernels<false>, BM25AccumulationKernels<true>>::value,
    "Expansion kernels must not convert to existing-only kernels");
static_assert(!std::is_convertible<BM25AccumulationKernels<true>, BM25AccumulationKernels<false>>::value,
    "Existing-only kernels must not convert to expansion kernels");
static_assert(CanCallFused<false, BM25AccumulationKernels<false>>::value,
    "Expansion kernels must be accepted by the expansion entry point");
static_assert(CanCallFused<true, BM25AccumulationKernels<true>>::value,
    "Existing-only kernels must be accepted by the existing-only entry point");
static_assert(!CanCallFused<true, BM25AccumulationKernels<false>>::value,
    "The existing-only entry point must reject expansion kernels");
static_assert(!CanCallFused<false, BM25AccumulationKernels<true>>::value,
    "The expansion entry point must reject existing-only kernels");
static_assert(!CanCallFused<false, ArbitraryFusedKernel>::value
    && !CanCallFused<true, ArbitraryFusedKernel>::value,
    "External callbacks must not receive writable accumulator state");

template <typename DocId>
ObPostingDocIdView make_doc_id_view(const DocId *ids)
{
  ObPostingDocIdView view;
  view.values_ = ids;
  view.width_ = sizeof(DocId);
  return view;
}

template <typename Relevance>
ObPostingRelevanceView make_relevance_view(const Relevance *tfs, const Relevance *dls)
{
  ObPostingRelevanceView view;
  view.token_freqs_ = tfs;
  view.doc_lengths_ = dls;
  view.width_ = sizeof(Relevance);
  return view;
}

const uint64_t UINT64_ROUNDING_EDGES[] = {
  0, 1, 3, UINT32_MAX - 1ULL, UINT32_MAX, UINT32_MAX + 1ULL, UINT32_MAX + 2ULL,
  (1ULL << 53) - 1, 1ULL << 53, (1ULL << 53) + 1, (1ULL << 53) + 2, (1ULL << 53) + 3,
  (1ULL << 63) - 513, (1ULL << 63) - 512, (1ULL << 63) - 511, (1ULL << 63) - 1,
  1ULL << 63, (1ULL << 63) + 1, (1ULL << 63) + 1023, (1ULL << 63) + 1024,
  (1ULL << 63) + 1025, (1ULL << 63) + 3071, (1ULL << 63) + 3072, (1ULL << 63) + 3073,
  UINT64_MAX - 1024, UINT64_MAX - 1023, UINT64_MAX - 1022, UINT64_MAX - 1, UINT64_MAX
};

double reference_score(const uint64_t tf, const uint64_t dl, const ObBM25PrecomputedParams &params)
{
  // Volatile intermediates make the rounding oracle independent of FP contraction.
  volatile double biased_tf = static_cast<double>(tf) + params.denom_bias_;
  volatile double scaled_dl = params.doc_len_scale_ * static_cast<double>(dl);
  volatile double denom = biased_tf + scaled_dl;
  volatile double weighted = params.query_weight_ * (static_cast<double>(tf) / denom);
  volatile double result = weighted * params.score_multiplier_;
  return result;
}

void expect_same_accumulator(const DocScoreAccumulator &expected, const DocScoreAccumulator &actual)
{
  ASSERT_EQ(expected.get_capacity(), actual.get_capacity());
  ASSERT_EQ(expected.get_start_id(), actual.get_start_id());
  EXPECT_EQ(expected.is_empty(), actual.is_empty());
  EXPECT_EQ(expected.get_active_count(), actual.get_active_count());
  const double expected_max = expected.get_max_score();
  const double actual_max = actual.get_max_score();
  EXPECT_EQ(0, std::memcmp(&expected_max, &actual_max, sizeof(double)));
  const int64_t word_count = (expected.get_capacity() + 63) / 64;
  EXPECT_EQ(0, std::memcmp(expected.get_bitset_words(), actual.get_bitset_words(),
      word_count * sizeof(uint64_t)));
  for (int64_t i = 0; i < expected.get_capacity(); ++i) {
    if (0 != (expected.get_bitset_words()[i >> 6] & (1ULL << (i & 63)))) {
      double expected_score = 0;
      double actual_score = 0;
      ASSERT_EQ(OB_SUCCESS, expected.get_score(expected.get_start_id() + i, expected_score));
      ASSERT_EQ(OB_SUCCESS, actual.get_score(actual.get_start_id() + i, actual_score));
      EXPECT_EQ(0, std::memcmp(&expected_score, &actual_score, sizeof(double))) << "offset=" << i;
    }
  }
}

class TestDASBM25BatchKernel : public ::testing::TestWithParam<ObTargetArch>
{
protected:
  void SetUp() override
  {
    BM25AccumulationKernels<false> kernels;
    const int ret = get_bm25_accumulation_kernels_for_arch<false>(GetParam(), kernels);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(kernels.is_valid());
    BM25AccumulationKernels<true> existing_kernels;
    ASSERT_EQ(OB_SUCCESS, get_bm25_accumulation_kernels_for_arch<true>(GetParam(), existing_kernels));
    ASSERT_TRUE(existing_kernels.is_valid());
    ASSERT_EQ(OB_SUCCESS, get_bm25_score_kernels_for_arch(GetParam(), score_kernels_));
    ASSERT_TRUE(score_kernels_.is_valid());
    ASSERT_EQ(GetParam(), score_kernels_.get_arch());
    ASSERT_EQ(OB_SUCCESS, get_bm25_frontier_max_kernels_for_arch(GetParam(), frontier_kernels_));
    ASSERT_TRUE(frontier_kernels_.is_valid());
    ASSERT_EQ(GetParam(), frontier_kernels_.get_arch());
  }

  template <bool ONLY_EXISTING>
  BM25AccumulationKernels<ONLY_EXISTING> selected_kernels()
  {
    BM25AccumulationKernels<ONLY_EXISTING> kernels;
    EXPECT_EQ(OB_SUCCESS, get_bm25_accumulation_kernels_for_arch<ONLY_EXISTING>(GetParam(), kernels));
    EXPECT_EQ(GetParam(), kernels.get_arch());
    return kernels;
  }

  BM25ScoreKernels selected_score_kernels() { return score_kernels_; }
  BM25FrontierMaxKernels selected_frontier_kernels() { return frontier_kernels_; }

  template <bool ONLY_EXISTING>
  BM25AccumulationKernels<ONLY_EXISTING> scalar_kernels()
  {
    BM25AccumulationKernels<ONLY_EXISTING> kernels;
    EXPECT_EQ(OB_SUCCESS,
        get_bm25_accumulation_kernels_for_arch<ONLY_EXISTING>(ObTargetArch::Default, kernels));
    return kernels;
  }

  BM25ScoreKernels scalar_score_kernels()
  {
    BM25ScoreKernels kernels;
    EXPECT_EQ(OB_SUCCESS, get_bm25_score_kernels_for_arch(ObTargetArch::Default, kernels));
    return kernels;
  }

  template <bool ONLY_EXISTING, typename DocId, typename Relevance>
  void check_accumulation(const uint64_t start_id)
  {
    constexpr int64_t CAPACITY = 1025;
    // Full batches cover one bitmap word, mixed groups, distinct words,
    // and reuse of the same word across batches.
    const int64_t offsets[] = {
      0, 1, 2, 7, 31, 32, 62, 63,
      64, 65, 66, 127, 128, 129, 191, 192,
      256, 320, 384, 448, 512, 576, 640, 704,
      705, 706, 707, 708, 709, 710, 711, 712,
      1024
    };
    const uint64_t parameters[] = {
      0, 1, 3, UINT32_MAX, UINT32_MAX + 1ULL,
      (1ULL << 53) - 1, (1ULL << 53) + 1, UINT64_MAX
    };
    std::vector<DocId> all_ids;
    if (ONLY_EXISTING) {
      all_ids.push_back(0);
      all_ids.push_back(static_cast<DocId>(start_id - 1));
    }
    for (const int64_t offset : offsets) {
      all_ids.push_back(static_cast<DocId>(start_id + offset));
    }
    if (ONLY_EXISTING) {
      all_ids.push_back(static_cast<DocId>(start_id + CAPACITY));
      all_ids.push_back(std::numeric_limits<DocId>::max());
    }
    ObBM25PrecomputedParams params;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.7, 173.5, 1.3, params));
    const BM25AccumulationKernels<ONLY_EXISTING> scalar = scalar_kernels<ONLY_EXISTING>();
    const BM25AccumulationKernels<ONLY_EXISTING> selected = selected_kernels<ONLY_EXISTING>();
    for (const int64_t count : {int64_t(0), int64_t(1), int64_t(2), int64_t(3),
        int64_t(7), int64_t(8), int64_t(9), int64_t(16), int64_t(17),
        int64_t(24), int64_t(32), static_cast<int64_t>(all_ids.size())}) {
      std::vector<DocId> ids(all_ids.begin(), all_ids.begin() + count);
      std::vector<Relevance> tfs(count);
      std::vector<Relevance> dls(count);
      for (int64_t i = 0; i < count; ++i) {
        tfs[i] = static_cast<Relevance>(parameters[i % 8]);
        dls[i] = static_cast<Relevance>(parameters[(i + 3) % 8]);
      }
      for (const int membership : {0, 1, 2}) {
        SCOPED_TRACE(::testing::Message() << "existing=" << ONLY_EXISTING
            << " doc_width=" << sizeof(DocId) << " relevance_width=" << sizeof(Relevance)
            << " start=" << start_id << " count=" << count << " membership=" << membership);
        ObArenaAllocator allocator("BM25Widths");
        DocScoreAccumulator expected;
        DocScoreAccumulator actual;
        DocScoreAccumulator scalar_result;
        for (DocScoreAccumulator *acc : {&expected, &actual, &scalar_result}) {
          ASSERT_EQ(OB_SUCCESS, acc->init(CAPACITY, allocator));
          uint64_t max_id = 0;
          ASSERT_EQ(OB_SUCCESS, acc->reuse_with_start_id(start_id, max_id));
          // Leave NaNs in absent slots to detect accidental unmasked score reads.
          for (int64_t offset = 0; offset < CAPACITY; ++offset) {
            ASSERT_EQ(OB_SUCCESS, acc->add_dim(start_id + offset, std::numeric_limits<double>::quiet_NaN()));
          }
          acc->clear_all();
          for (int64_t offset = 0; offset < CAPACITY; ++offset) {
            if (membership == 2 || (membership == 1 && offset % 3 == 0)) {
              ASSERT_EQ(OB_SUCCESS, acc->add_dim(start_id + offset, (offset + 1) * 0.125));
            }
          }
        }
        int64_t expected_count = 0;
        for (int64_t i = 0; i < count; ++i) {
          const uint64_t offset = static_cast<uint64_t>(ids[i]) - start_id;
          if (ids[i] >= start_id && offset < CAPACITY) {
            const bool present = 0 != (expected.get_bitset_words()[offset >> 6] & (1ULL << (offset & 63)));
            if (!ONLY_EXISTING || present) {
              ASSERT_EQ(OB_SUCCESS, expected.add_dim(ids[i], reference_score(tfs[i], dls[i], params)));
              ++expected_count;
            }
          }
        }
        const ObPostingRelevanceView relevance = make_relevance_view(tfs.data(), dls.data());
        int64_t matched = -1;
        ASSERT_EQ(OB_SUCCESS, scalar_result.add_scored_dim_batch_fused(
            make_doc_id_view(ids.data()), relevance, count, params, scalar, matched));
        EXPECT_EQ(expected_count, matched);
        ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
            make_doc_id_view(ids.data()), relevance, count, params, selected, matched));
        EXPECT_EQ(expected_count, matched);
        expect_same_accumulator(expected, scalar_result);
        expect_same_accumulator(expected, actual);
      }
    }
  }

  template <bool ONLY_EXISTING>
  void check_all_widths()
  {
    for (const uint64_t start : {uint64_t(1000), uint64_t(UINT32_MAX) - 2048}) {
      check_accumulation<ONLY_EXISTING, uint32_t, uint32_t>(start);
      check_accumulation<ONLY_EXISTING, uint32_t, uint64_t>(start);
    }
    for (const uint64_t start : {UINT32_MAX + 17ULL, UINT64_MAX - 2048ULL}) {
      check_accumulation<ONLY_EXISTING, uint64_t, uint32_t>(start);
      check_accumulation<ONLY_EXISTING, uint64_t, uint64_t>(start);
    }
  }

  template <bool ONLY_EXISTING, typename DocId>
  void check_uint64_rounding_edges()
  {
    const BM25AccumulationKernels<ONLY_EXISTING> kernels = selected_kernels<ONLY_EXISTING>();
    constexpr int64_t CAPACITY = 129;
    const uint64_t offsets[] = {0, 1, 7, 31, 62, 63, 64, 127, 128};
    ObBM25PrecomputedParams params;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
    ObArenaAllocator allocator("BM25Edges");
    DocScoreAccumulator expected;
    DocScoreAccumulator actual;
    ASSERT_EQ(OB_SUCCESS, expected.init(CAPACITY, allocator));
    ASSERT_EQ(OB_SUCCESS, actual.init(CAPACITY, allocator));
    for (const uint64_t start : {uint64_t(0), uint64_t(std::numeric_limits<DocId>::max()) - 128}) {
      DocId ids[9];
      for (int64_t i = 0; i < 9; ++i) {
        ids[i] = static_cast<DocId>(start + offsets[i]);
      }
      for (const uint64_t tf : UINT64_ROUNDING_EDGES) {
        for (const uint64_t dl : UINT64_ROUNDING_EDGES) {
          SCOPED_TRACE(::testing::Message() << "existing=" << ONLY_EXISTING
              << " doc_width=" << sizeof(DocId) << " start=" << start << " tf=" << tf << " dl=" << dl);
          uint64_t max_id = 0;
          ASSERT_EQ(OB_SUCCESS, expected.reuse_with_start_id(start, max_id));
          ASSERT_EQ(OB_SUCCESS, actual.reuse_with_start_id(start, max_id));
          uint64_t tfs[9];
          uint64_t dls[9];
          // Every pair reaches all eight AVX512 lanes, both NEON lanes and the scalar tail.
          for (int64_t i = 0; i < 9; ++i) {
            tfs[i] = tf;
            dls[i] = dl;
            if (ONLY_EXISTING) {
              ASSERT_EQ(OB_SUCCESS, expected.add_dim(ids[i], 0.25));
              ASSERT_EQ(OB_SUCCESS, actual.add_dim(ids[i], 0.25));
            }
          }
          const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
          for (int64_t pass = 0; pass < 3; ++pass) {
            int64_t expected_matched = 0;
            for (int64_t i = 0; i < 9; ++i) {
              const uint64_t offset = offsets[i];
              if (!ONLY_EXISTING || 0 != (expected.get_bitset_words()[offset >> 6]
                  & (1ULL << (offset & 63)))) {
                ASSERT_EQ(OB_SUCCESS, expected.add_dim(ids[i], reference_score(tf, dl, params)));
                ++expected_matched;
              }
            }
            int64_t matched = -1;
            ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
                make_doc_id_view(ids), relevance, 9, params, kernels, matched));
            EXPECT_EQ(expected_matched, matched);
            expect_same_accumulator(expected, actual);
            ASSERT_EQ(OB_SUCCESS, expected.filter_below_score(1.0));
            ASSERT_EQ(OB_SUCCESS, actual.filter_below_score(1.0));
            expect_same_accumulator(expected, actual);
          }
        }
      }
    }
  }

  template <bool ONLY_EXISTING>
  void check_invalid_views()
  {
    ObArenaAllocator allocator("BM25BadViews");
    ObBM25PrecomputedParams params;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
    const uint32_t ids32[] = {10, 11}, tfs32[] = {3, 5}, dls32[] = {7, 9};
    const uint64_t ids64[] = {10, 11}, tfs64[] = {3, 5}, dls64[] = {7, 9};
    const BM25AccumulationKernels<ONLY_EXISTING> kernels = selected_kernels<ONLY_EXISTING>();
    const BM25AccumulationKernels<ONLY_EXISTING> invalid_kernels;
    DocScoreAccumulator expected;
    DocScoreAccumulator actual;
    for (DocScoreAccumulator *acc : {&expected, &actual}) {
      ASSERT_EQ(OB_SUCCESS, acc->init(8, allocator));
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, acc->reuse_with_start_id(10, max_id));
      ASSERT_EQ(OB_SUCCESS, acc->add_dim(10, 0.5));
    }
    for (const int64_t count : {0, -1}) {
      int64_t matched = -1;
      ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
          ObPostingDocIdView(), ObPostingRelevanceView(), count, params, invalid_kernels, matched));
      EXPECT_EQ(0, matched);
      expect_same_accumulator(expected, actual);
    }
    for (const bool narrow : {true, false}) {
      SCOPED_TRACE(::testing::Message() << "existing=" << ONLY_EXISTING << " narrow=" << narrow);
      const ObPostingDocIdView ids = narrow ? make_doc_id_view(ids32) : make_doc_id_view(ids64);
      const ObPostingRelevanceView relevance = narrow
          ? make_relevance_view(tfs32, dls32) : make_relevance_view(tfs64, dls64);
      int64_t matched = -1;
      EXPECT_EQ(OB_INVALID_ARGUMENT, actual.add_scored_dim_batch_fused(
          ids, relevance, 2, params, invalid_kernels, matched));
      EXPECT_EQ(0, matched);
      expect_same_accumulator(expected, actual);
      DocScoreAccumulator uninitialized;
      matched = -1;
      EXPECT_EQ(OB_INVALID_ARGUMENT, uninitialized.add_scored_dim_batch_fused(
          ids, relevance, 2, params, kernels, matched));
      EXPECT_EQ(0, matched);
      ObPostingDocIdView missing_ids = ids;
      missing_ids.values_ = nullptr;
      matched = -1;
      EXPECT_EQ(OB_INVALID_ARGUMENT, actual.add_scored_dim_batch_fused(
          missing_ids, relevance, 2, params, kernels, matched));
      EXPECT_EQ(0, matched);
      expect_same_accumulator(expected, actual);
      for (const bool missing_tf : {true, false}) {
        ObPostingRelevanceView missing = relevance;
        if (missing_tf) { missing.token_freqs_ = nullptr; } else { missing.doc_lengths_ = nullptr; }
        matched = -1;
        EXPECT_EQ(OB_INVALID_ARGUMENT, actual.add_scored_dim_batch_fused(
            ids, missing, 2, params, kernels, matched));
        EXPECT_EQ(0, matched);
        expect_same_accumulator(expected, actual);
      }
      for (const uint32_t width : {0, 1, 2, 3, 16}) {
        SCOPED_TRACE(::testing::Message() << "invalid_width=" << width);
        ObPostingDocIdView bad_ids = ids;
        ObPostingRelevanceView bad_relevance = relevance;
        bad_ids.width_ = width;
        bad_relevance.width_ = width;
        matched = -1;
        EXPECT_EQ(OB_INVALID_ARGUMENT, actual.add_scored_dim_batch_fused(
            bad_ids, relevance, 2, params, kernels, matched));
        EXPECT_EQ(0, matched);
        expect_same_accumulator(expected, actual);
        matched = -1;
        EXPECT_EQ(OB_INVALID_ARGUMENT, actual.add_scored_dim_batch_fused(
            ids, bad_relevance, 2, params, kernels, matched));
        EXPECT_EQ(0, matched);
        expect_same_accumulator(expected, actual);
      }
    }
  }

  template <typename Relevance>
  void check_scoring()
  {
    const BM25ScoreKernels scalar = scalar_score_kernels();
    const BM25ScoreKernels selected = selected_score_kernels();
    EXPECT_EQ(ObTargetArch::Default, scalar.get_arch());
    EXPECT_EQ(GetParam(), selected.get_arch());
    ObBM25PrecomputedParams params;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.7, 173.5, 1.3, params));
    for (const int64_t count : {0, 1, 2, 3, 7, 8, 9, 17, 257}) {
      SCOPED_TRACE(::testing::Message() << "width=" << sizeof(Relevance) << " count=" << count);
      std::vector<Relevance> tfs(count), dls(count);
      std::vector<double> expected(count + 2, -1234.5);
      std::vector<double> actual(expected), scalar_scores(expected);
      for (int64_t i = 0; i < count; ++i) {
        tfs[i] = i % 3 == 0 ? std::numeric_limits<Relevance>::max() : i;
        dls[i] = i % 5 == 0 ? std::numeric_limits<Relevance>::max() : 5 + i;
        expected[i + 1] = reference_score(tfs[i], dls[i], params);
      }
      scalar.score(tfs.data(), dls.data(), count, params, scalar_scores.data() + 1);
      selected.score(tfs.data(), dls.data(), count, params, actual.data() + 1);
      EXPECT_EQ(0, std::memcmp(expected.data(), scalar_scores.data(), expected.size() * sizeof(double)));
      EXPECT_EQ(0, std::memcmp(expected.data(), actual.data(), expected.size() * sizeof(double)));
    }
  }

  BM25ScoreKernels score_kernels_;
  BM25FrontierMaxKernels frontier_kernels_;
};

TEST_P(TestDASBM25BatchKernel, accumulation_matches_scalar_and_rounding_oracle)
{
  check_all_widths<false>();
}

TEST_P(TestDASBM25BatchKernel, existing_only_preserves_membership_and_ignores_outside_window)
{
  check_all_widths<true>();
}

TEST_P(TestDASBM25BatchKernel, uint64_rounding_edges_preserve_fused_scores_and_state)
{
  check_uint64_rounding_edges<false, uint32_t>();
  check_uint64_rounding_edges<false, uint64_t>();
  check_uint64_rounding_edges<true, uint32_t>();
  check_uint64_rounding_edges<true, uint64_t>();
}

TEST_P(TestDASBM25BatchKernel, fused_batches_match_materialized_scores_after_reuse_and_filtering)
{
  const BM25AccumulationKernels<false> kernels = selected_kernels<false>();
  ObArenaAllocator allocator("BM25Batch");
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  for (const int64_t capacity : {1, 64, 257, 16384}) {
    DocScoreAccumulator expected;
    DocScoreAccumulator actual;
    ASSERT_EQ(OB_SUCCESS, expected.init(capacity, allocator));
    ASSERT_EQ(OB_SUCCESS, actual.init(capacity, allocator));
    for (const uint64_t start : {uint64_t(0), UINT64_MAX - capacity + 1}) {
      for (const int64_t requested : {0, 1, 2, 3, 7, 8, 9, 31, 128, 255, 256, 513}) {
        const int64_t count = MIN(capacity, requested);
        SCOPED_TRACE(::testing::Message() << "capacity=" << capacity << " start=" << start << " count=" << count);
        uint64_t max_id = 0;
        ASSERT_EQ(OB_SUCCESS, expected.reuse_with_start_id(start, max_id));
        ASSERT_EQ(OB_SUCCESS, actual.reuse_with_start_id(start, max_id));
        std::vector<uint64_t> ids(count), tfs(count), dls(count);
        std::vector<double> scores(count);
        for (int64_t i = 0; i < count; ++i) {
          ids[i] = start + i;
          tfs[i] = 1 + (i * 5) % 13;
          dls[i] = i % 17 == 0 ? UINT64_MAX : uint64_t(20 + (i * 31) % 400);
          scores[i] = reference_score(tfs[i], dls[i], params);
        }
        const ObPostingRelevanceView relevance = make_relevance_view(tfs.data(), dls.data());
        for (int64_t pass = 0; pass < 3; ++pass) {
          ASSERT_EQ(OB_SUCCESS, expected.add_dim_batch(ids.data(), scores.data(), count));
          int64_t matched = -1;
          ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
              make_doc_id_view(ids.data()), relevance, count, params, kernels, matched));
          EXPECT_EQ(count, matched);
          expect_same_accumulator(expected, actual);
          ASSERT_EQ(OB_SUCCESS, expected.filter_below_score(1.0));
          ASSERT_EQ(OB_SUCCESS, actual.filter_below_score(1.0));
          expect_same_accumulator(expected, actual);
        }
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, fused_views_validate_both_modes_without_mutating_state)
{
  check_invalid_views<false>();
  check_invalid_views<true>();
}

TEST_P(TestDASBM25BatchKernel, fused_batch_rejects_invalid_inputs_without_mutating_state)
{
  ObArenaAllocator allocator("BM25Validate");
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  const uint64_t ids[] = {10, 11};
  const uint64_t below[] = {9, 11};
  const uint64_t above[] = {10, 18};
  const uint64_t tfs[] = {3, 5};
  const uint64_t dls[] = {7, 9};
  const BM25AccumulationKernels<false> kernels = selected_kernels<false>();
  const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
  DocScoreAccumulator accumulator;
  int64_t matched = -1;
  const auto add_batch = [&](const uint64_t *doc_ids, const int64_t count) {
    matched = -1;
    return accumulator.add_scored_dim_batch_fused(
        make_doc_id_view(doc_ids), relevance, count, params, kernels, matched);
  };
  EXPECT_EQ(OB_SUCCESS, add_batch(nullptr, 0));
  EXPECT_EQ(0, matched);
  EXPECT_EQ(OB_SUCCESS, add_batch(nullptr, -1));
  EXPECT_EQ(0, matched);
  EXPECT_EQ(OB_INVALID_ARGUMENT, add_batch(ids, 1));
  EXPECT_EQ(0, matched);
  ASSERT_EQ(OB_SUCCESS, accumulator.init(8, allocator));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(10, 0.5));
  DocScoreAccumulator expected;
  ASSERT_EQ(OB_SUCCESS, expected.init(8, allocator));
  ASSERT_EQ(OB_SUCCESS, expected.reuse_with_start_id(10, max_id));
  ASSERT_EQ(OB_SUCCESS, expected.add_dim(10, 0.5));
  EXPECT_EQ(OB_INVALID_ARGUMENT, add_batch(nullptr, 1));
  EXPECT_EQ(0, matched);
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, add_batch(below, 2));
  EXPECT_EQ(0, matched);
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, add_batch(above, 2));
  EXPECT_EQ(0, matched);
  ObBitVector *filter = to_bit_vector(allocator.alloc(ObBitVector::memory_size(8)));
  ASSERT_NE(nullptr, filter);
  filter->init(8);
  accumulator.set_filter_mask(filter);
  EXPECT_EQ(OB_INVALID_ARGUMENT, add_batch(ids, 2));
  EXPECT_EQ(0, matched);
  expect_same_accumulator(expected, accumulator);
  accumulator.clear_filter_mask();
  ASSERT_EQ(OB_SUCCESS, add_batch(ids, 2));
  EXPECT_EQ(2, matched);
  EXPECT_EQ(2, accumulator.get_active_count());
}

TEST_P(TestDASBM25BatchKernel, existing_only_keeps_filtered_candidates_removed_and_preserves_empty_state)
{
  const BM25AccumulationKernels<true> existing_kernels = selected_kernels<true>();
  const BM25AccumulationKernels<false> expansion_kernels = selected_kernels<false>();
  ObArenaAllocator allocator("BM25Presence");
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  const uint64_t ids[] = {99, 100, 101, 102, 103, 104, 105, 106, 107};
  const uint64_t tfs[] = {3, 3, 3, 3, 3, 3, 3, 3, 3};
  const uint64_t dls[] = {5, 5, 5, 5, 5, 5, 5, 5, 5};
  const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
  DocScoreAccumulator expected;
  DocScoreAccumulator actual;
  for (DocScoreAccumulator *acc : {&expected, &actual}) {
    ASSERT_EQ(OB_SUCCESS, acc->init(17, allocator));
    uint64_t max_id = 0;
    ASSERT_EQ(OB_SUCCESS, acc->reuse_with_start_id(100, max_id));
    ASSERT_EQ(OB_SUCCESS, acc->add_dim(100, 0.25));
    ASSERT_EQ(OB_SUCCESS, acc->add_dim(101, 1.0));
    ASSERT_EQ(OB_SUCCESS, acc->add_dim(106, 2.0));
    ASSERT_EQ(OB_SUCCESS, acc->add_dim(116, 4.0));
    ASSERT_EQ(OB_SUCCESS, acc->filter_below_score(0.5));
  }
  ASSERT_EQ(OB_SUCCESS, expected.add_dim(101, reference_score(3, 5, params)));
  ASSERT_EQ(OB_SUCCESS, expected.add_dim(106, reference_score(3, 5, params)));
  int64_t matched = -1;
  ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 9, params, existing_kernels, matched));
  EXPECT_EQ(2, matched);
  EXPECT_EQ(3, actual.get_active_count());
  expect_same_accumulator(expected, actual);

  ObBitVector *mask = to_bit_vector(allocator.alloc(ObBitVector::memory_size(17)));
  ASSERT_NE(nullptr, mask);
  mask->init(17);
  mask->set(16);
  ASSERT_EQ(OB_SUCCESS, expected.retain_by_mask(*mask));
  ASSERT_EQ(OB_SUCCESS, actual.retain_by_mask(*mask));
  actual.set_filter_mask(mask);
  matched = -1;
  ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 9, params, existing_kernels, matched));
  EXPECT_EQ(0, matched);
  EXPECT_FALSE(actual.is_empty());
  expect_same_accumulator(expected, actual);

  expected.clear_all();
  actual.clear_all();
  matched = -1;
  ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 9, params, existing_kernels, matched));
  EXPECT_EQ(0, matched);
  EXPECT_TRUE(actual.is_empty());
  expect_same_accumulator(expected, actual);
  actual.clear_filter_mask();
  matched = -1;
  ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 0, params, expansion_kernels, matched));
  EXPECT_EQ(0, matched);
  expect_same_accumulator(expected, actual);
  ASSERT_EQ(OB_SUCCESS, actual.add_scored_dim_batch_fused(
      make_doc_id_view(ids + 1), relevance, 8, params, expansion_kernels, matched));
  EXPECT_EQ(8, matched);
  EXPECT_FALSE(actual.is_empty());
  EXPECT_EQ(8, actual.get_active_count());
}

TEST_P(TestDASBM25BatchKernel, scoring_matches_scalar_including_tails)
{
  check_scoring<uint32_t>();
  check_scoring<uint64_t>();
}

TEST_P(TestDASBM25BatchKernel, uint64_score_only_matches_rounding_oracle_and_fused)
{
  constexpr int64_t COUNT = 17;
  const BM25ScoreKernels scalar = scalar_score_kernels();
  const BM25ScoreKernels selected = selected_score_kernels();
  const BM25AccumulationKernels<false> fused = selected_kernels<false>();
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.13, 67.0, 1.7, params));
  ObArenaAllocator allocator("BM25Score64");
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(COUNT, allocator));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
  uint64_t ids[COUNT];
  for (int64_t i = 0; i < COUNT; ++i) { ids[i] = 100 + i; }
  for (const uint64_t tf : UINT64_ROUNDING_EDGES) {
    for (const uint64_t dl : UINT64_ROUNDING_EDGES) {
      SCOPED_TRACE(::testing::Message() << "tf=" << tf << " dl=" << dl);
      std::vector<uint64_t> tfs(COUNT, tf), dls(COUNT, dl);
      std::vector<double> expected(COUNT + 2, -1234.5);
      std::vector<double> actual(expected), scalar_scores(expected);
      for (int64_t i = 0; i < COUNT; ++i) { expected[i + 1] = reference_score(tf, dl, params); }
      scalar.score(tfs.data(), dls.data(), COUNT, params, scalar_scores.data() + 1);
      selected.score(tfs.data(), dls.data(), COUNT, params, actual.data() + 1);
      EXPECT_EQ(0, std::memcmp(expected.data(), scalar_scores.data(), expected.size() * sizeof(double)));
      EXPECT_EQ(0, std::memcmp(expected.data(), actual.data(), expected.size() * sizeof(double)));
      accumulator.clear_all();
      const ObPostingRelevanceView relevance = make_relevance_view(tfs.data(), dls.data());
      int64_t matched = -1;
      ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
          make_doc_id_view(ids), relevance, COUNT, params, fused, matched));
      EXPECT_EQ(COUNT, matched);
      for (int64_t i = 0; i < COUNT; ++i) {
        double score = 0;
        ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids[i], score));
        EXPECT_EQ(0, std::memcmp(&score, &actual[i + 1], sizeof(double)));
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, scalar_formula_matches_batch_and_rounding_oracle)
{
  const BM25ScoreKernels selected = selected_score_kernels();
  for (const double average : {3.0, 173.5, std::numeric_limits<double>::denorm_min()}) {
    for (const double boost : {0.0, 1.3}) {
      ObBM25PrecomputedParams params;
      ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.7, average, boost, params));
      for (int64_t i = 0; i < ARRAYSIZEOF(UINT64_ROUNDING_EDGES); ++i) {
        const uint64_t tf = UINT64_ROUNDING_EDGES[i];
        const uint64_t dl = UINT64_ROUNDING_EDGES[(i * 3) % ARRAYSIZEOF(UINT64_ROUNDING_EDGES)];
        SCOPED_TRACE(::testing::Message() << "tf=" << tf << " dl=" << dl
            << " average=" << average << " boost=" << boost);
        const double expected = reference_score(tf, dl, params);
        const double actual = ObExprBM25::eval_with_precomputed_params(tf, dl, params);
        double batch = 0;
        selected.score(&tf, &dl, 1, params, &batch);
        if (std::isnan(expected)) {
          EXPECT_TRUE(std::isnan(actual));
          EXPECT_TRUE(std::isnan(batch));
        } else {
          EXPECT_EQ(0, std::memcmp(&expected, &actual, sizeof(expected)));
          EXPECT_EQ(0, std::memcmp(&expected, &batch, sizeof(expected)));
        }
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, frontier_max_matches_scalar_and_rounding_oracle)
{
  BM25FrontierMaxKernels scalar;
  ASSERT_EQ(OB_SUCCESS, get_bm25_frontier_max_kernels_for_arch(ObTargetArch::Default, scalar));
  const BM25FrontierMaxKernels selected = selected_frontier_kernels();
  const uint32_t values[] = {0, 1, 2, 7, 1024, UINT32_MAX - 1, UINT32_MAX};
  for (const double avg_dl : {0.125, 3.0, 173.5,
      std::numeric_limits<double>::min(), std::numeric_limits<double>::denorm_min()}) {
    for (const double boost : {0.0, 1.3}) {
      ObBM25PrecomputedParams params;
      ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.7, avg_dl, boost, params));
      for (const int64_t count : {0, 1, 2, 3, 7, 8, 9, 15, 16, 17}) {
        std::vector<ObInvIdxBM25ParamPair> pairs(count);
        for (int64_t i = 0; i < count; ++i) {
          pairs[i] = {values[i % ARRAYSIZEOF(values)], values[(i * 3) % ARRAYSIZEOF(values)]};
        }
        const ObInvIdxBM25ParamPair *data = count > 0 ? pairs.data() : nullptr;
        for (const double initial : {-1.0, 0.0, 100.0}) {
          SCOPED_TRACE(::testing::Message() << "avg_dl=" << avg_dl << " boost=" << boost
              << " count=" << count << " initial=" << initial);
          double expected = initial;
          for (const ObInvIdxBM25ParamPair &pair : pairs) {
            expected = std::max(expected, reference_score(pair.token_freq_, pair.doc_length_, params));
          }
          const double scalar_max = scalar.max_score(data, count, params, initial);
          const double actual = selected.max_score(data, count, params, initial);
          EXPECT_EQ(0, std::memcmp(&expected, &scalar_max, sizeof(expected)));
          EXPECT_EQ(0, std::memcmp(&expected, &actual, sizeof(expected)));
          const int64_t split = count / 2;
          double span_max = selected.max_score(data, split, params, initial);
          span_max = selected.max_score(count > 0 ? data + split : nullptr, count - split, params, span_max);
          EXPECT_EQ(0, std::memcmp(&expected, &span_max, sizeof(expected)));
        }
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, frontier_max_preserves_scalar_rounding_in_each_lane_and_tail)
{
  const BM25FrontierMaxKernels kernels = selected_frontier_kernels();
  ObBM25PrecomputedParams params[2];
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.0, 3.0, 1.0, params[0]));
  params[1].denom_bias_ = 0;
  params[1].doc_len_scale_ = std::nextafter(1.0, 2.0);
  params[1].query_weight_ = 1;
  params[1].score_multiplier_ = 1;
  params[1].is_inited_ = true;
  const ObInvIdxBM25ParamPair winners[] = {{1, 6}, {3, 5}};
  const double expected[] = {0.32258064516129037, 0.375};
  for (int64_t p = 0; p < ARRAYSIZEOF(params); ++p) {
    ASSERT_EQ(expected[p], reference_score(winners[p].token_freq_, winners[p].doc_length_, params[p]));
    for (const int64_t count : {1, 2, 3, 7, 8, 9, 16, 17}) {
      for (int64_t winner_idx = 0; winner_idx < count; ++winner_idx) {
        SCOPED_TRACE(::testing::Message() << "params=" << p << " count=" << count
            << " winner=" << winner_idx);
        std::vector<ObInvIdxBM25ParamPair> pairs(count, ObInvIdxBM25ParamPair{0, 1});
        pairs[winner_idx] = winners[p];
        const double actual = kernels.max_score(pairs.data(), count, params[p], 0.0);
        EXPECT_EQ(0, std::memcmp(expected + p, &actual, sizeof(actual)));
        EXPECT_GE(actual, reference_score(winners[p].token_freq_, winners[p].doc_length_, params[p]));
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, frontier_max_preserves_ordered_nan_comparisons)
{
  const BM25FrontierMaxKernels kernels = selected_frontier_kernels();
  ObBM25PrecomputedParams params;
  params.denom_bias_ = 0;
  params.doc_len_scale_ = 0;
  params.query_weight_ = 1;
  params.score_multiplier_ = 1;
  params.is_inited_ = true;
  for (const bool all_nan : {false, true}) {
    for (const int64_t count : {0, 1, 2, 3, 7, 8, 9, 16, 17}) {
      std::vector<ObInvIdxBM25ParamPair> pairs(count, ObInvIdxBM25ParamPair{0, 0});
      if (!all_nan) {
        for (int64_t i = 1; i < count; i += 2) {
          pairs[i] = {3, 5};
        }
      }
      const ObInvIdxBM25ParamPair *data = count > 0 ? pairs.data() : nullptr;
      for (const double initial : {0.5, std::numeric_limits<double>::quiet_NaN()}) {
        SCOPED_TRACE(::testing::Message() << "all_nan=" << all_nan << " count=" << count
            << " initial=" << initial);
        double expected = initial;
        for (const ObInvIdxBM25ParamPair &pair : pairs) {
          expected = std::max(expected, reference_score(pair.token_freq_, pair.doc_length_, params));
        }
        const double actual = kernels.max_score(data, count, params, initial);
        EXPECT_EQ(0, std::memcmp(&expected, &actual, sizeof(expected)));
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, shared_formula_vector_body_and_tail_preserve_scalar_rounding)
{
  const BM25AccumulationKernels<false> kernels = selected_kernels<false>();
  ObBM25PrecomputedParams params;
  params.denom_bias_ = 0;
  params.doc_len_scale_ = std::nextafter(1.0, 2.0);
  params.query_weight_ = 1;
  params.score_multiplier_ = 1;
  params.is_inited_ = true;
  volatile uint64_t tf = 3;
  volatile uint64_t dl = 5;
  const double expected = 0.375;
  EXPECT_EQ(expected, reference_score(tf, dl, params));
  double scalar_score = 0;
  const uint64_t scalar_tf = tf;
  const uint64_t scalar_dl = dl;
  scalar_score_kernels().score(&scalar_tf, &scalar_dl, 1, params, &scalar_score);
  EXPECT_EQ(expected, scalar_score);
  EXPECT_EQ(expected, ObExprBM25::eval_with_precomputed_params(scalar_tf, scalar_dl, params));
  const uint32_t tfs[] = {3, 3, 3, 3, 3, 3, 3, 3, 3};
  const uint32_t dls[] = {5, 5, 5, 5, 5, 5, 5, 5, 5};
  const uint64_t tfs64[] = {3, 3, 3, 3, 3, 3, 3, 3, 3};
  const uint64_t dls64[] = {5, 5, 5, 5, 5, 5, 5, 5, 5};
  for (const BM25ScoreKernels score_kernels : {scalar_score_kernels(), selected_score_kernels()}) {
    double scores[9] = {};
    score_kernels.score(tfs, dls, 9, params, scores);
    for (const double score : scores) {
      EXPECT_EQ(expected, score);
    }
    score_kernels.score(tfs64, dls64, 9, params, scores);
    for (const double score : scores) {
      EXPECT_EQ(expected, score);
    }
  }
  const uint32_t ids[] = {100, 101, 102, 103, 104, 105, 106, 107, 108};
  ObArenaAllocator allocator("BM25Contract");
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(9, allocator));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
  const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
  int64_t matched = -1;
  ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 9, params, kernels, matched));
  EXPECT_EQ(9, matched);
  for (const uint32_t id : ids) {
    double score = 0;
    ASSERT_EQ(OB_SUCCESS, accumulator.get_score(id, score));
    EXPECT_EQ(expected, score);
  }
  EXPECT_EQ(expected, accumulator.get_max_score());
}

TEST_P(TestDASBM25BatchKernel, legal_params_preserve_scalar_rounding_for_all_widths_and_tails)
{
  constexpr int64_t COUNT = 9;
  volatile double avg_doc_length = 3.0;
  ObBM25PrecomputedParams params;
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.0, avg_doc_length, 1.0, params));
  const double expected = reference_score(1, 6, params);
  const double accumulated = expected + expected;
  ASSERT_EQ(0.32258064516129037, expected);
  const double single_score = ObExprBM25::eval_with_precomputed_params(1, 6, params);
  EXPECT_EQ(0, std::memcmp(&expected, &single_score, sizeof(expected)));
  uint32_t ids32[COUNT];
  uint64_t ids64[COUNT];
  uint32_t tfs32[COUNT];
  uint32_t dls32[COUNT];
  uint64_t tfs64[COUNT];
  uint64_t dls64[COUNT];
  for (int64_t i = 0; i < COUNT; ++i) {
    ids32[i] = ids64[i] = 100 + i;
    tfs32[i] = tfs64[i] = 1;
    dls32[i] = dls64[i] = 6;
  }
  const BM25AccumulationKernels<false> expansion = selected_kernels<false>();
  const BM25AccumulationKernels<true> existing = selected_kernels<true>();
  for (const bool compact_ids : {false, true}) {
    const ObPostingDocIdView ids = compact_ids ? make_doc_id_view(ids32) : make_doc_id_view(ids64);
    for (const bool compact_relevance : {false, true}) {
      const ObPostingRelevanceView relevance = compact_relevance
          ? make_relevance_view(tfs32, dls32) : make_relevance_view(tfs64, dls64);
      for (const int64_t count : {int64_t(1), int64_t(7), int64_t(8), COUNT}) {
        SCOPED_TRACE(::testing::Message() << "compact_ids=" << compact_ids
            << " compact_relevance=" << compact_relevance << " count=" << count);
        double scores[COUNT] = {};
        selected_score_kernels().score(relevance, count, params, scores);
        for (int64_t i = 0; i < count; ++i) {
          EXPECT_EQ(0, std::memcmp(&expected, scores + i, sizeof(expected)));
        }
        ObArenaAllocator allocator("BM25Contract");
        DocScoreAccumulator accumulator;
        ASSERT_EQ(OB_SUCCESS, accumulator.init(COUNT, allocator));
        uint64_t max_id = 0;
        ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
        int64_t matched = 0;
        ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
            ids, relevance, count, params, expansion, matched));
        EXPECT_EQ(count, matched);
        for (int64_t i = 0; i < count; ++i) {
          double score = 0;
          ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids.at(i), score));
          EXPECT_EQ(0, std::memcmp(&expected, &score, sizeof(score)));
        }
        ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
            ids, relevance, count, params, existing, matched));
        EXPECT_EQ(count, matched);
        for (int64_t i = 0; i < count; ++i) {
          double score = 0;
          ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids.at(i), score));
          EXPECT_EQ(0, std::memcmp(&accumulated, &score, sizeof(score)));
        }
        const double max_score = accumulator.get_max_score();
        EXPECT_EQ(0, std::memcmp(&accumulated, &max_score, sizeof(max_score)));
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, accumulation_rounds_boost_before_adding_existing_score)
{
  constexpr int64_t CAPACITY = 17;
  uint32_t ids[CAPACITY];
  uint32_t tfs[CAPACITY];
  uint32_t dls[CAPACITY];
  for (int64_t i = 0; i < CAPACITY; ++i) {
    ids[i] = 100 + i;
    tfs[i] = dls[i] = 1;
  }
  ObBM25PrecomputedParams params;
  params.denom_bias_ = 0;
  params.doc_len_scale_ = 0;
  params.query_weight_ = 5;
  params.score_multiplier_ = std::nextafter(1.0, 2.0);
  params.is_inited_ = true;
  const double initial = 3.0;
  volatile double rounded_score = reference_score(1, 1, params);
  const double expected = initial + rounded_score;
  ASSERT_EQ(8.0, expected);
  ASSERT_GT(std::fma(params.query_weight_, params.score_multiplier_, initial), expected);
  const BM25AccumulationKernels<false> expansion = selected_kernels<false>();
  const BM25AccumulationKernels<true> existing = selected_kernels<true>();
  const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
  for (const bool only_existing : {false, true}) {
    for (const int64_t count : {1, 2, 3, 7, 8, 9, 16, 17}) {
      SCOPED_TRACE(::testing::Message() << "existing=" << only_existing << " count=" << count);
      ObArenaAllocator allocator("BM25ScoreRound");
      DocScoreAccumulator accumulator;
      ASSERT_EQ(OB_SUCCESS, accumulator.init(CAPACITY, allocator));
      uint64_t max_id = 0;
      ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
      for (int64_t i = 0; i < count; ++i) {
        ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(ids[i], initial));
      }
      int64_t matched = 0;
      if (only_existing) {
        ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
            make_doc_id_view(ids), relevance, count, params, existing, matched));
      } else {
        ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
            make_doc_id_view(ids), relevance, count, params, expansion, matched));
      }
      EXPECT_EQ(count, matched);
      for (int64_t i = 0; i < count; ++i) {
        double actual = 0;
        ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids[i], actual));
        EXPECT_EQ(0, std::memcmp(&expected, &actual, sizeof(actual)));
      }
      const double maximum = accumulator.get_max_score();
      EXPECT_EQ(0, std::memcmp(&expected, &maximum, sizeof(maximum)));
    }
  }
}

TEST_P(TestDASBM25BatchKernel, fused_and_score_only_respect_block_max)
{
  constexpr int64_t COUNT = 17;
  const uint32_t tfs[COUNT] = {3, 5, 7, 9, 10, 11, 19, 21, 33, 2, 4, 6, 8, 9, 10, 18, 20};
  const uint32_t dls[COUNT] = {5, 9, 13, 17, 19, 23, 39, 43, 67, 5, 9, 13, 17, 19, 23, 39, 43};
  uint64_t tfs64[COUNT];
  uint64_t dls64[COUNT];
  uint64_t wide_dls[COUNT];
  uint32_t ids[COUNT];
  ObArray<uint64_t> token_freqs;
  ObArray<uint64_t> doc_lengths;
  for (int64_t i = 0; i < COUNT; ++i) {
    ids[i] = 100 + i;
    tfs64[i] = tfs[i];
    dls64[i] = dls[i];
    // Larger lengths are dominated by the same frontier pairs.
    wide_dls[i] = i % 3 == 0 ? (1ULL << 53) + 1
        : (i % 3 == 1 ? (1ULL << 63) + 1025 : UINT64_MAX);
    ASSERT_EQ(OB_SUCCESS, token_freqs.push_back(tfs[i]));
    ASSERT_EQ(OB_SUCCESS, doc_lengths.push_back(dls[i]));
  }
  const BM25ScoreKernels score_kernels[] = {
    scalar_score_kernels(), selected_score_kernels()
  };
  const BM25AccumulationKernels<false> accumulation_kernels[] = {
    scalar_kernels<false>(), selected_kernels<false>()
  };
  for (const int64_t max_pair_count : {1, 12}) {
    ObArenaAllocator allocator(ObModIds::TEST);
    ObInvIdxBM25MaxScoreAggregator aggregator;
    const ObIArray<ObInvIdxBM25ParamPair> *frontier = nullptr;
    ASSERT_EQ(OB_SUCCESS, aggregator.init(allocator, max_pair_count));
    ASSERT_EQ(OB_SUCCESS, aggregator.aggregate(token_freqs, doc_lengths, frontier));
    ASSERT_NE(nullptr, frontier);
    ASSERT_EQ(max_pair_count == 1 ? 1 : 9, frontier->count());
    for (const double avg_dl : {0.5, 3.0, 9.0, 173.5, 1e9}) {
      for (const double boost : {0.0, 1.3}) {
        ObBM25PrecomputedParams params;
        ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(1.7, avg_dl, boost, params));
        double block_max = 0;
        for (int64_t i = 0; i < frontier->count(); ++i) {
          const ObInvIdxBM25ParamPair &pair = frontier->at(i);
          block_max = MAX(block_max, reference_score(
              pair.token_freq_, pair.doc_length_, params));
        }
        for (int64_t k = 0; k < 2; ++k) {
          SCOPED_TRACE(::testing::Message() << "max_pair_count=" << max_pair_count
              << " avg_dl=" << avg_dl << " boost=" << boost << " kernel=" << k);
          double scores32[COUNT] = {};
          double scores64[COUNT] = {};
          double wide_scores[COUNT] = {};
          double fused_scores[COUNT] = {};
          ObArenaAllocator fused_allocator("BM25Bound");
          DocScoreAccumulator accumulator;
          ASSERT_EQ(OB_SUCCESS, accumulator.init(COUNT, fused_allocator));
          uint64_t max_id = 0;
          ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
          const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
          score_kernels[k].score(tfs, dls, COUNT, params, scores32);
          score_kernels[k].score(tfs64, dls64, COUNT, params, scores64);
          score_kernels[k].score(tfs64, wide_dls, COUNT, params, wide_scores);
          int64_t matched = -1;
          ASSERT_EQ(OB_SUCCESS, accumulator.add_scored_dim_batch_fused(
              make_doc_id_view(ids), relevance, COUNT, params, accumulation_kernels[k], matched));
          EXPECT_EQ(COUNT, matched);
          EXPECT_EQ((1ULL << COUNT) - 1, accumulator.get_bitset_words()[0]);
          const double max_score = accumulator.get_max_score();
          for (int64_t i = 0; i < COUNT; ++i) {
            ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids[i], fused_scores[i]));
          }
          EXPECT_EQ(0, std::memcmp(scores32, fused_scores, sizeof(scores32)));
          EXPECT_EQ(0, std::memcmp(scores32, scores64, sizeof(scores32)));
          for (int64_t i = 0; i < COUNT; ++i) {
            const double expected = reference_score(tfs[i], dls[i], params);
            const double wide_expected = reference_score(tfs64[i], wide_dls[i], params);
            EXPECT_EQ(0, std::memcmp(&expected, &scores32[i], sizeof(double)));
            EXPECT_EQ(0, std::memcmp(&wide_expected, &wide_scores[i], sizeof(double)));
            EXPECT_LE(scores32[i], block_max);
            EXPECT_LE(scores64[i], block_max);
            EXPECT_LE(wide_scores[i], block_max);
            EXPECT_LE(fused_scores[i], block_max);
          }
          EXPECT_LE(max_score, block_max);
          if (max_pair_count == 12) {
            EXPECT_EQ(0, std::memcmp(&max_score, &block_max, sizeof(double)));
          }
        }
      }
    }
  }
}

TEST_P(TestDASBM25BatchKernel, inactive_lane_does_not_divide_zero_by_zero)
{
  const BM25AccumulationKernels<true> kernels = selected_kernels<true>();
  ObBM25PrecomputedParams params;
  params.denom_bias_ = 0;
  params.doc_len_scale_ = 1;
  params.query_weight_ = 1;
  params.score_multiplier_ = 1;
  params.is_inited_ = true;
  const uint32_t ids[] = {100, 101, 102, 103, 104, 105, 106, 107, 108};
  const uint32_t tfs[] = {3, 0, 3, 0, 3, 0, 3, 0, 0};
  const uint32_t dls[] = {5, 0, 5, 0, 5, 0, 5, 0, 0};
  ObArenaAllocator allocator("BM25Inactive");
  DocScoreAccumulator accumulator;
  ASSERT_EQ(OB_SUCCESS, accumulator.init(9, allocator));
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, accumulator.reuse_with_start_id(100, max_id));
  for (const uint32_t id : ids) {
    ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(id, std::numeric_limits<double>::quiet_NaN()));
  }
  accumulator.clear_all();
  for (int64_t i = 0; i < 8; i += 2) {
    ASSERT_EQ(OB_SUCCESS, accumulator.add_dim(ids[i], 0.5));
  }
  const ObPostingRelevanceView relevance = make_relevance_view(tfs, dls);
  int64_t matched = -1;
  ASSERT_EQ(0, std::feclearexcept(FE_ALL_EXCEPT));
  const int ret = accumulator.add_scored_dim_batch_fused(
      make_doc_id_view(ids), relevance, 9, params, kernels, matched);
  const int exceptions = std::fetestexcept(FE_INVALID | FE_DIVBYZERO);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, exceptions);
  EXPECT_EQ(4, matched);
  EXPECT_EQ(0x55, accumulator.get_bitset_words()[0]);
  for (int64_t i = 0; i < 9; ++i) {
    double score = 0;
    ASSERT_EQ(OB_SUCCESS, accumulator.get_score(ids[i], score));
    EXPECT_EQ(i < 8 && i % 2 == 0 ? 0.875 : 0.0, score);
  }
  EXPECT_FALSE(accumulator.is_empty());
  EXPECT_EQ(0.875, accumulator.get_max_score());
}

std::vector<ObTargetArch> get_supported_bm25_arches()
{
  std::vector<ObTargetArch> test_arches{ObTargetArch::Default};
#if OB_USE_MULTITARGET_CODE
  if (is_arch_supported(ObTargetArch::AVX512)) {
    test_arches.push_back(ObTargetArch::AVX512);
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (is_arch_supported(ObTargetArch::NEON)) {
    test_arches.push_back(ObTargetArch::NEON);
  }
#endif
  return test_arches;
}

TEST(BM25KernelDispatch, explicit_targets_clear_outputs_on_error_and_match_automatic_dispatch)
{
  const uint32_t saved_arches = arches;
  BM25FrontierMaxKernels unbound_frontier;
  EXPECT_FALSE(unbound_frontier.is_valid());
  EXPECT_EQ(ObTargetArch::Default, unbound_frontier.get_arch());
  BM25ScoreKernels unbound_scores;
  EXPECT_FALSE(unbound_scores.is_valid());
  const std::vector<ObTargetArch> supported_arches = get_supported_bm25_arches();
  const ObTargetArch automatic_arch = supported_arches.back();
  for (const ObTargetArch arch : {ObTargetArch::Default, ObTargetArch::AVX512,
      ObTargetArch::NEON, ObTargetArch::AVX2, static_cast<ObTargetArch>(UINT32_MAX)}) {
    const bool supported = std::find(supported_arches.begin(), supported_arches.end(), arch)
        != supported_arches.end();
    BM25AccumulationKernels<false> kernels = get_bm25_accumulation_kernels<false>();
    BM25AccumulationKernels<true> existing_kernels = get_bm25_accumulation_kernels<true>();
    EXPECT_EQ(automatic_arch, kernels.get_arch());
    EXPECT_EQ(automatic_arch, existing_kernels.get_arch());
    ASSERT_TRUE(kernels.is_valid());
    ASSERT_TRUE(existing_kernels.is_valid());
    EXPECT_EQ(supported ? OB_SUCCESS : OB_NOT_SUPPORTED,
        get_bm25_accumulation_kernels_for_arch<false>(arch, kernels));
    EXPECT_EQ(supported ? OB_SUCCESS : OB_NOT_SUPPORTED,
        get_bm25_accumulation_kernels_for_arch<true>(arch, existing_kernels));
    EXPECT_EQ(supported, kernels.is_valid());
    EXPECT_EQ(supported, existing_kernels.is_valid());
    EXPECT_EQ(supported ? arch : ObTargetArch::Default, kernels.get_arch());
    EXPECT_EQ(supported ? arch : ObTargetArch::Default, existing_kernels.get_arch());
    BM25ScoreKernels scores = get_bm25_score_kernels();
    EXPECT_EQ(automatic_arch, scores.get_arch());
    ASSERT_TRUE(scores.is_valid());
    EXPECT_EQ(supported ? OB_SUCCESS : OB_NOT_SUPPORTED, get_bm25_score_kernels_for_arch(arch, scores));
    EXPECT_EQ(supported, scores.is_valid());
    EXPECT_EQ(supported ? arch : ObTargetArch::Default, scores.get_arch());
    BM25FrontierMaxKernels frontier = get_bm25_frontier_max_kernels();
    EXPECT_EQ(automatic_arch, frontier.get_arch());
    ASSERT_TRUE(frontier.is_valid());
    EXPECT_EQ(supported ? OB_SUCCESS : OB_NOT_SUPPORTED,
        get_bm25_frontier_max_kernels_for_arch(arch, frontier));
    EXPECT_EQ(supported, frontier.is_valid());
    EXPECT_EQ(supported ? arch : ObTargetArch::Default, frontier.get_arch());
  }
  EXPECT_EQ(saved_arches, arches);
}

#if OB_USE_MULTITARGET_CODE
class BM25ArchMaskGuard
{
public:
  BM25ArchMaskGuard() : saved_(arches) {}
  ~BM25ArchMaskGuard() { arches = saved_; }
private:
  uint32_t saved_;
};

TEST(BM25KernelDispatch, avx512_dispatch_uses_existing_arch_capability)
{
  BM25ArchMaskGuard guard;
  const uint32_t avx2 = static_cast<uint32_t>(ObTargetArch::AVX2);
  const uint32_t avx512 = static_cast<uint32_t>(ObTargetArch::AVX512);
  // Inspect bindings only: synthetic capability masks must never execute kernels.
  for (const uint32_t mask : {0u, avx2, avx512, avx2 | avx512}) {
    SCOPED_TRACE(mask);
    arches = mask;
    const bool has_avx512 = 0 != (mask & avx512);
    const ObTargetArch expected = has_avx512 ? ObTargetArch::AVX512
        : ObTargetArch::Default;
    BM25ScoreKernels scores = get_bm25_score_kernels();
    BM25FrontierMaxKernels frontier = get_bm25_frontier_max_kernels();
    BM25AccumulationKernels<false> expansion = get_bm25_accumulation_kernels<false>();
    BM25AccumulationKernels<true> existing = get_bm25_accumulation_kernels<true>();
    EXPECT_EQ(expected, scores.get_arch());
    EXPECT_EQ(expected, frontier.get_arch());
    EXPECT_EQ(expected, expansion.get_arch());
    EXPECT_EQ(expected, existing.get_arch());
    for (const ObTargetArch arch : {ObTargetArch::Default, ObTargetArch::AVX2, ObTargetArch::AVX512}) {
      const bool supported = ObTargetArch::Default == arch
          || (ObTargetArch::AVX512 == arch && has_avx512);
      scores = get_bm25_score_kernels();
      frontier = get_bm25_frontier_max_kernels();
      expansion = get_bm25_accumulation_kernels<false>();
      existing = get_bm25_accumulation_kernels<true>();
      const int expected_ret = supported ? OB_SUCCESS : OB_NOT_SUPPORTED;
      EXPECT_EQ(expected_ret, get_bm25_score_kernels_for_arch(arch, scores));
      EXPECT_EQ(expected_ret, get_bm25_frontier_max_kernels_for_arch(arch, frontier));
      EXPECT_EQ(expected_ret, get_bm25_accumulation_kernels_for_arch<false>(arch, expansion));
      EXPECT_EQ(expected_ret, get_bm25_accumulation_kernels_for_arch<true>(arch, existing));
      EXPECT_EQ(supported, scores.is_valid());
      EXPECT_EQ(supported, frontier.is_valid());
      EXPECT_EQ(supported, expansion.is_valid());
      EXPECT_EQ(supported, existing.is_valid());
      const ObTargetArch bound_arch = supported ? arch : ObTargetArch::Default;
      EXPECT_EQ(bound_arch, scores.get_arch());
      EXPECT_EQ(bound_arch, frontier.get_arch());
      EXPECT_EQ(bound_arch, expansion.get_arch());
      EXPECT_EQ(bound_arch, existing.get_arch());
    }
  }
}
#endif

INSTANTIATE_TEST_CASE_P(Architectures, TestDASBM25BatchKernel,
    ::testing::ValuesIn(get_supported_bm25_arches()),
    [](const ::testing::TestParamInfo<ObTargetArch> &info) {
      return info.param == ObTargetArch::Default ? "Scalar"
          : (info.param == ObTargetArch::AVX512 ? "AVX512" : "NEON");
    });

} // namespace

int main(int argc, char **argv)
{
  init_arches();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
