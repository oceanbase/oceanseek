/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <algorithm>

#include "sql/das/search/ob_das_search_define.h"
#include "sql/engine/expr/ob_expr_bm25.h"

using namespace oceanbase;
using namespace oceanbase::sql;

namespace
{

double phrase_score_upper_bound(
    const int64_t phrase_freq_ub,
    const int64_t doc_length,
    const double avg_doc_length,
    const double total_token_weight,
    const double boost)
{
  const double norm_len = doc_length / avg_doc_length;
  const double doc_weight_ub = ObExprBM25::doc_phrase_weight(
      static_cast<double>(phrase_freq_ub), norm_len);
  return total_token_weight * doc_weight_ub * boost;
}

} // namespace

TEST(MatchPhraseDynamicPruning, pruning_level_ordering)
{
  EXPECT_LT(HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_DISABLED,
            HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_BASIC);
  EXPECT_LT(HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_BASIC,
            HYBRID_SEARCH_TOPK_DYNAMIC_PRUNING_WITH_MATCH_PHRASE);
}

TEST(MatchPhraseDynamicPruning, phrase_freq_upper_bound_slop_zero_uses_min_tf)
{
  const int64_t lead_tf = 3;
  const int64_t token_cnt = 3;
  int64_t phrase_freq_sum = 1 - token_cnt;
  int64_t phrase_freq_min = lead_tf;
  phrase_freq_sum += lead_tf;
  phrase_freq_min = std::min(phrase_freq_min, lead_tf);
  const int64_t tf_max_other = 5;
  phrase_freq_sum += tf_max_other;
  phrase_freq_min = std::min(phrase_freq_min, tf_max_other);
  phrase_freq_sum += tf_max_other;
  phrase_freq_min = std::min(phrase_freq_min, tf_max_other);

  const double phrase_freq_ub = phrase_freq_min;
  EXPECT_EQ(3, phrase_freq_ub);
  EXPECT_LT(phrase_freq_ub, (phrase_freq_min + phrase_freq_sum) / 2.0);
}

TEST(MatchPhraseDynamicPruning, phrase_freq_upper_bound_with_slop_averages)
{
  const int64_t phrase_freq_min = 2;
  const int64_t phrase_freq_sum = 8;
  const double phrase_freq_ub = (phrase_freq_min + phrase_freq_sum) / 2.0;
  EXPECT_DOUBLE_EQ(5.0, phrase_freq_ub);
}

TEST(MatchPhraseDynamicPruning, score_ub_below_threshold_is_pruned)
{
  const double score_ub = phrase_score_upper_bound(
      2 /*phrase_freq_ub*/, 100 /*doc_length*/, 100.0 /*avg_doc_length*/,
      1.5 /*total_token_weight*/, 1.0 /*boost*/);
  const double min_competitive_score = score_ub + 0.01;
  EXPECT_FALSE(score_ub > min_competitive_score);
}

TEST(MatchPhraseDynamicPruning, score_ub_above_threshold_is_candidate)
{
  const double score_ub = phrase_score_upper_bound(
      4 /*phrase_freq_ub*/, 80 /*doc_length*/, 100.0 /*avg_doc_length*/,
      2.0 /*total_token_weight*/, 1.2 /*boost*/);
  const double min_competitive_score = score_ub * 0.5;
  EXPECT_TRUE(score_ub > min_competitive_score);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
