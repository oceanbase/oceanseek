/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <iostream>
using namespace std;

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>
#include "share/rc/ob_tenant_base.h"

#define private public
#define protected public
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#undef private
#undef protected
#include "sql/ob_sql_init.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObRandomAffiTaskSplitTest : public ::testing::Test
{
public:

  const static int64_t TEST_PARTITION_COUNT = 5;
  const static int64_t TEST_SPLIT_TASK_COUNT = 8;

  ObRandomAffiTaskSplitTest() = default;
  virtual ~ObRandomAffiTaskSplitTest() = default;
  virtual void SetUp() {};
  virtual void TearDown() {};

private:
  // disallow copy
  ObRandomAffiTaskSplitTest(const ObRandomAffiTaskSplitTest &other);
  ObRandomAffiTaskSplitTest& operator=(const ObRandomAffiTaskSplitTest &other);
};

TEST_F(ObRandomAffiTaskSplitTest, split_task_test) {
  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    // 1000, 900, 800, 700, 600 assign to 3 workers
    // worker1 worker2 worker3
    // 1000    900     800
    //         600     700
    for (int i = 0; i < 5; ++i) {
      px_part_info.estimated_row_count_ = (10 - i) * 100;
      affinitize_rule.add_partition(i,i,parallel,tenant_id,px_part_info);
    }
    affinitize_rule.do_random(true, tenant_id);
    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < result.count(); ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }
    ASSERT_EQ(0, result.at(0).worker_id_);
    ASSERT_EQ(1, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(2, result.at(3).worker_id_);
    ASSERT_EQ(1, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 16;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    px_part_info.estimated_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 1000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 3500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }
    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(4, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(0, result.at(3).worker_id_);
    ASSERT_EQ(3, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    // 3000, 4000, 2500, 1500, 2000 assign to 3 workers
    // worker1 worker2 worker3
    // 4000    3000    2500
    //         1500    2000


    px_part_info.estimated_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 4000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 1500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }

    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(0, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(1, result.at(3).worker_id_);
    ASSERT_EQ(2, result.at(4).worker_id_);
 }

}

TEST_F(ObRandomAffiTaskSplitTest, px_transmit_fallback_offset_exceeds_uint16)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObPxTransmitOp::VectorSendParams params(allocator);
  const int64_t row_count = 104;
  const int64_t fallback_slice_count = 640;
  void *buf = allocator.alloc(row_count * sizeof(*params.fallback_array_));
  ASSERT_NE(nullptr, buf);
  params.fallback_array_ = static_cast<decltype(params.fallback_array_)>(buf);

  // The first row takes the direct-send path. Each of the remaining 103 rows
  // falls back to 640 channels, so the final cumulative offset is 65920.
  params.fallback_cnt_ = 0;
  params.fallback_array_[0] = params.fallback_cnt_;
  for (int64_t i = 1; i < row_count; ++i) {
    params.fallback_cnt_ += fallback_slice_count;
    params.fallback_array_[i] = params.fallback_cnt_;
  }

  ASSERT_EQ(65920, params.fallback_cnt_);
  EXPECT_EQ(params.fallback_cnt_, static_cast<int64_t>(params.fallback_array_[row_count - 1]));
  EXPECT_EQ(fallback_slice_count,
            static_cast<int64_t>(params.fallback_array_[row_count - 1])
                - static_cast<int64_t>(params.fallback_array_[row_count - 2]));
}

class ObPartitionRangeTaskSplitTest : public ::testing::Test
{
public:
  ObPartitionRangeTaskSplitTest() : tenant_(OB_SERVER_TENANT_ID), previous_tenant_(nullptr) {}
  void SetUp() override
  {
    previous_tenant_ = ObTenantEnv::get_tenant();
    ObTenantEnv::set_tenant(&tenant_);
  }
  void TearDown() override
  {
    ObTenantEnv::set_tenant(previous_tenant_);
  }

protected:
  template <size_t PART_COUNT>
  void check_distribution(const int64_t (&row_counts)[PART_COUNT],
                          const int64_t thread_count,
                          const int64_t (&expected_thread_counts)[PART_COUNT])
  {
    const int64_t prefix_task_count = 5;
    const int64_t part_count = PART_COUNT;
    ObDASTabletLoc locations[PART_COUNT];
    ObSlaveMapUtil::AffinitizedPartRowMetaArray row_metas;
    ObPxPartChMapArray map;
    for (int64_t i = 0; i < part_count; ++i) {
      locations[i].tablet_id_ = ObTabletID(100 + i);
      ObSlaveMapUtil::AffinitizedPartitionRowMeta meta;
      meta.loc_ = &locations[i];
      meta.row_count_ = row_counts[i];
      ASSERT_EQ(OB_SUCCESS, row_metas.push_back(meta));
    }
    ASSERT_EQ(OB_SUCCESS, ObSlaveMapUtil::dispatch_affinitized_heavy_then_zigzag(
        thread_count, prefix_task_count, row_metas, map));

    // Exercise the production conversion from SQC task IDs to global channel IDs.
    ObThreadCond msg_ready_cond;
    ObPxTransmitChProvider provider(msg_ready_cond);
    ASSERT_EQ(OB_SUCCESS, provider.msg_.get_part_affinity_map().assign(map));
    ObPxPartChInfo ch_info;
    ASSERT_EQ(OB_SUCCESS, provider.inner_get_part_ch_map(ch_info));
    ASSERT_EQ(map.count(), ch_info.part_ch_array_.count());

    ObArenaAllocator allocator(ObModIds::TEST);
    ObExecContext ctx(allocator);
    ObPxSqcHandler sqc_handler;
    ctx.set_sqc_handler(&sqc_handler);
    ObTableSchema table_schema;
    ObArray<ObExpr *> sort_exprs;
    ObSlaveMapPkeyRangeIdxCalc range_calc(
        ctx, table_schema, nullptr, ObPQDistributeMethod::PARTITION_RANGE,
        ObNullDistributeMethod::NONE, ch_info, sort_exprs, nullptr, nullptr,
        OB_REPARTITION_NO_REPARTITION, nullptr);
    // The consumer rejects a tablet that occurs in more than one contiguous group.
    ASSERT_EQ(OB_SUCCESS, range_calc.build_partition_range_channel_map(range_calc.part_range_map_));
    ASSERT_EQ(part_count, range_calc.part_range_map_.size());

    ObSEArray<int64_t, 16> thread_use_counts;
    for (int64_t i = 0; i < thread_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, thread_use_counts.push_back(0));
    }
    int64_t expected_map_count = 0;
    for (int64_t i = 0; i < part_count; ++i) {
      ObSlaveMapPkeyRangeIdxCalc::PartitionRangeChannelInfo *part_info = nullptr;
      ASSERT_EQ(OB_SUCCESS, range_calc.part_range_map_.get_refactored(
          locations[i].tablet_id_.id(), part_info));
      ASSERT_NE(nullptr, part_info);
      ASSERT_EQ(expected_thread_counts[i], part_info->channels_.count());
      expected_map_count += expected_thread_counts[i];
      for (int64_t j = 0; j < part_info->channels_.count(); ++j) {
        const int64_t task_idx = part_info->channels_.at(j) - prefix_task_count;
        ASSERT_GE(task_idx, 0);
        ASSERT_LT(task_idx, thread_count);
        ++thread_use_counts.at(task_idx);
        if (j > 0) {
          EXPECT_EQ(part_info->channels_.at(j - 1) + 1, part_info->channels_.at(j));
        }
      }
    }
    EXPECT_EQ(expected_map_count, map.count());
    for (int64_t i = 0; i < thread_count; ++i) {
      EXPECT_GT(thread_use_counts.at(i), 0);
    }
  }

private:
  ObTenantBase tenant_;
  ObTenantBase *previous_tenant_;
};

TEST_F(ObPartitionRangeTaskSplitTest, all_heavy_multiple_remaining_threads)
{
  const int64_t three_parts[] = {100, 100, 100};
  const int64_t eight_threads[] = {3, 3, 2};
  check_distribution(three_parts, 8, eight_threads);
  const int64_t six_parts[] = {100, 100, 100, 100, 100, 100};
  const int64_t sixteen_threads[] = {3, 3, 3, 3, 2, 2};
  check_distribution(six_parts, 16, sixteen_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, all_heavy_contiguous_channels)
{
  const int64_t equal_parts[] = {100, 100};
  const int64_t seven_threads[] = {4, 3};
  check_distribution(equal_parts, 7, seven_threads);
  const int64_t eight_threads[] = {4, 4};
  check_distribution(equal_parts, 8, eight_threads);
  const int64_t unequal_parts[] = {300, 200};
  check_distribution(unequal_parts, 7, seven_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, mixed_heavy_and_light_partitions)
{
  const int64_t abundant_rows[] = {550, 150, 150, 150};
  const int64_t abundant_threads[] = {5, 2, 2, 1};
  check_distribution(abundant_rows, 10, abundant_threads);
  const int64_t scarce_rows[] = {800, 100, 100};
  const int64_t scarce_threads[] = {4, 1, 1};
  check_distribution(scarce_rows, 5, scarce_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, skip_heavy_detection)
{
  const int64_t abundant_threads[] = {3, 3, 2};
  const int64_t overflow_rows[] = {INT64_MAX, INT64_MAX, 1};
  check_distribution(overflow_rows, 8, abundant_threads);
  const int64_t positive_rows[] = {300, 200, 100};
  const int64_t scarce_threads[] = {1, 1, 1};
  check_distribution(positive_rows, 2, scarce_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, large_row_counts_exact_quotas)
{
  // Keep the skewed allocation on both sides of the double precision boundary.
  const int64_t max_exact_double_int = INT64_C(1) << 53;
  const int64_t boundary_rows[] = {max_exact_double_int - 1, 1};
  const int64_t heavy_threads[] = {7, 1};
  check_distribution(boundary_rows, 8, heavy_threads);
  // At 2^53 + 1, floating-point quotas previously left no thread for the light partition.
  const int64_t large_rows[] = {max_exact_double_int, 1};
  check_distribution(large_rows, 8, heavy_threads);
  // The total fits int64_t, but multiplying the heavy partition's rows by 8 does not.
  const int64_t max_rows[] = {INT64_MAX - 1, 1};
  check_distribution(max_rows, 8, heavy_threads);
  const int64_t single_partition[] = {INT64_MAX};
  const int64_t all_threads[] = {8};
  check_distribution(single_partition, 8, all_threads);
  // Light partitions share the remaining thread when partitions outnumber threads.
  const int64_t scarce_rows[] = {max_exact_double_int, 3, 2, 1};
  const int64_t shared_threads[] = {2, 1, 1, 1};
  check_distribution(scarce_rows, 3, shared_threads);
  // Choose the truly smaller heavy partition even when double would round their rows equal.
  const int64_t close_heavy_rows[] = {max_exact_double_int + 1, max_exact_double_int};
  const int64_t close_heavy_threads[] = {3, 4};
  check_distribution(close_heavy_rows, 7, close_heavy_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, zero_and_one_row_partitions)
{
  const int64_t zero_rows[] = {0, 0, 0};
  const int64_t abundant_threads[] = {3, 3, 2};
  check_distribution(zero_rows, 8, abundant_threads);
  const int64_t mixed_rows[] = {1000, 0, 1};
  const int64_t shared_threads[] = {7, 1, 1};
  check_distribution(mixed_rows, 8, shared_threads);
  const int64_t large_rows[] = {INT64_MAX - 2, 0, 1};
  check_distribution(large_rows, 8, shared_threads);
  const int64_t empty_partition_rows[] = {INT64_MAX - 1, 0};
  const int64_t heavy_threads[] = {7, 1};
  check_distribution(empty_partition_rows, 8, heavy_threads);
  // Adding the empty partition's minimum weight must also participate in overflow detection.
  const int64_t overflow_rows[] = {INT64_MAX, 0};
  const int64_t even_threads[] = {4, 4};
  check_distribution(overflow_rows, 8, even_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, exact_heavy_threshold)
{
  const int64_t boundary_rows[] = {200, 60, 60, 80};
  const int64_t light_threads[] = {1, 1, 1, 1};
  check_distribution(boundary_rows, 4, light_threads);
  // A quota strictly between 2 and 3 is heavy even though integer division yields 2.
  const int64_t above_boundary_rows[] = {201, 60, 60, 79};
  const int64_t heavy_threads[] = {2, 1, 1, 1};
  check_distribution(above_boundary_rows, 4, heavy_threads);
}

TEST_F(ObPartitionRangeTaskSplitTest, reject_empty_light_partitions)
{
  ObSlaveMapUtil::AffinitizedPartRowMetaArray row_metas;
  ObPxPartChMapArray map;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ObSlaveMapUtil::dispatch_light_partitions_abundant(1, 0, 0, row_metas, map));
  EXPECT_EQ(0, map.count());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  //oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
