/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#undef private
#include "simple_server/env/ob_simple_cluster_test_base.h"

using namespace oceanbase;
using namespace oceanbase::sql;

namespace
{

void check_decision(const ObDASVecIndexDriverIter &iter,
                    const int64_t filter_rows,
                    const int64_t partition_rows,
                    const ObVecIndexType expected_type,
                    const bool expected_brute_force,
                    const double expected_selectivity)
{
  ObDASVecIndexDriverIter::ObIvfPathDecision decision;
  ASSERT_EQ(OB_SUCCESS,
            iter.decide_ivf_partition_path_(filter_rows, partition_rows, decision));
  EXPECT_EQ(expected_type, decision.vec_index_type_);
  EXPECT_EQ(expected_brute_force, decision.go_brute_force_);
  EXPECT_DOUBLE_EQ(expected_selectivity, decision.selectivity_);
}

TEST(TestIvfPartitionPathDecision, exact_boundaries)
{
  ObDASVecIndexDriverIter iter;

  // The absolute 10K threshold is inclusive.
  check_decision(iter, 10000, 40000, ObVecIndexType::VEC_INDEX_PRE, true, 0.25);
  check_decision(iter, 10001, 40000, ObVecIndexType::VEC_INDEX_PRE, false, 0.250025);

  // Selectivity 0.3 stays PRE; only values strictly greater than 0.3 use ITER.
  check_decision(iter, 12000, 40000, ObVecIndexType::VEC_INDEX_PRE, false, 0.3);
  check_decision(iter, 12001, 40000,
                 ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, 0.300025);

  // Filtered scans above 0.3 keep iterating to fill TopK, including above 0.9.
  check_decision(iter, 36000, 40000,
                 ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, 0.9);
  check_decision(iter, 36001, 40000,
                 ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, 0.900025);
}

TEST(TestIvfPartitionPathDecision, invalid_and_clamped_input)
{
  ObDASVecIndexDriverIter iter;

  check_decision(iter, -1, 40000, ObVecIndexType::VEC_INDEX_PRE, true, 0.0);
  check_decision(iter, 50000, 40000,
                 ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, 1.0);
  check_decision(iter, 1, 0,
                 ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER, false, 1.0);
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}
