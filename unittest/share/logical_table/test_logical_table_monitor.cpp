/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>

#define private public
#define protected public
#include "share/logical_table/logical_table_monitor.h"
#include "share/logical_table/ob_logical_table_define.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace share;
using namespace common;
namespace unittest
{

class TestLogicalTableMonitor : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, monitor_.init(&dummy_proxy_, 1001/*tenant_id*/));
  }
  ObMySQLProxy dummy_proxy_;
  ObLogicalTableMonitor monitor_;
};

TEST(LogicalTableDefine, record_logical_scan_namespace)
{
  int64_t recorded_param_idx = -1;
  uint64_t recorded_literal = 0;
  ASSERT_TRUE(record_logical_ns_if_compatible(
      1, 0, recorded_param_idx, recorded_literal));
  ASSERT_EQ(1, recorded_param_idx);
  ASSERT_EQ(0, recorded_literal);
  ASSERT_TRUE(record_logical_ns_if_compatible(
      1, 0, recorded_param_idx, recorded_literal));
  ASSERT_FALSE(record_logical_ns_if_compatible(
      2, 0, recorded_param_idx, recorded_literal));
  ASSERT_EQ(1, recorded_param_idx);
  ASSERT_EQ(0, recorded_literal);

  recorded_param_idx = -1;
  recorded_literal = 0;
  ASSERT_TRUE(record_logical_ns_if_compatible(
      -1, 1001, recorded_param_idx, recorded_literal));
  ASSERT_TRUE(record_logical_ns_if_compatible(
      -1, 1001, recorded_param_idx, recorded_literal));
  ASSERT_FALSE(record_logical_ns_if_compatible(
      -1, 1002, recorded_param_idx, recorded_literal));
  ASSERT_FALSE(record_logical_ns_if_compatible(
      1, 0, recorded_param_idx, recorded_literal));
  ASSERT_EQ(-1, recorded_param_idx);
  ASSERT_EQ(1001, recorded_literal);
}

TEST(TestNamespaceStatRow, validity_and_reset)
{
  ObNamespaceStatRow row;
  ASSERT_FALSE(row.is_valid());
  row.namespace_id_ = 1;
  ASSERT_TRUE(row.is_valid());
  row.reset();
  ASSERT_FALSE(row.is_valid());
  ASSERT_EQ(0, row.row_count_);
  ASSERT_EQ(0, row.total_size_);
}

TEST_F(TestLogicalTableMonitor, ns_stat_defaults_mean_unlimited)
{
  ObLogicalTableMonitor::NsStat stat;
  ASSERT_FALSE(stat.exists_);
  ASSERT_EQ(-1, stat.row_limit_);
  ASSERT_EQ(-1, stat.size_limit_);
  ASSERT_EQ(0, stat.row_count_);
  ASSERT_EQ(0, stat.total_size_);
}

TEST_F(TestLogicalTableMonitor, eval_no_stat_row_passes)
{
  ObLogicalTableMonitor::NsStat stat;
  bool exceeded = true;
  ASSERT_EQ(OB_SUCCESS, monitor_.eval_namespace_limit_(
      ObString("test"), ObString("c"), 1, stat, exceeded));
  ASSERT_FALSE(exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_unlimited_passes)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 100000000;
  stat.total_size_ = 1024L * 1024L * 1024L;
  stat.row_limit_ = -1;
  stat.size_limit_ = -1;
  bool exceeded = true;
  ASSERT_EQ(OB_SUCCESS, monitor_.eval_namespace_limit_(
      ObString("test"), ObString("c"), 1, stat, exceeded));
  ASSERT_FALSE(exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_zero_limit_blocks_writes)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 0;
  stat.total_size_ = 0;
  stat.row_limit_ = 0;
  stat.size_limit_ = -1;
  bool exceeded = false;
  ASSERT_EQ(OB_SUCCESS, monitor_.eval_namespace_limit_(
      ObString("test"), ObString("c"), 1, stat, exceeded));
  ASSERT_TRUE(exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_under_limit_passes)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 99;
  stat.total_size_ = 9999;
  stat.row_limit_ = 100;
  stat.size_limit_ = 10000;
  bool exceeded = true;
  ASSERT_EQ(OB_SUCCESS, monitor_.eval_namespace_limit_(
      ObString("test"), ObString("c"), 1, stat, exceeded));
  ASSERT_FALSE(exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_row_limit_exceeded)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 100;
  stat.row_limit_ = 100;
  stat.size_limit_ = -1;
  bool row_exceeded = false;
  bool size_exceeded = false;
  ASSERT_TRUE(ObLogicalTableMonitor::calc_estimate_exceeded_(stat, row_exceeded, size_exceeded));
  ASSERT_TRUE(row_exceeded);
  ASSERT_FALSE(size_exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_size_limit_exceeded)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 1000;
  stat.total_size_ = 1000000;
  stat.row_limit_ = -1;
  stat.size_limit_ = 1000000;
  bool row_exceeded = false;
  bool size_exceeded = false;
  ASSERT_TRUE(ObLogicalTableMonitor::calc_estimate_exceeded_(stat, row_exceeded, size_exceeded));
  ASSERT_FALSE(row_exceeded);
  ASSERT_TRUE(size_exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_at_exact_row_limit_is_exceeded)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 100;
  stat.row_limit_ = 100;
  stat.size_limit_ = -1;
  bool row_exceeded = false;
  bool size_exceeded = false;
  ASSERT_TRUE(ObLogicalTableMonitor::calc_estimate_exceeded_(stat, row_exceeded, size_exceeded));
  ASSERT_TRUE(row_exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_at_exact_size_limit_is_exceeded)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 10;
  stat.total_size_ = 1000;
  stat.row_limit_ = -1;
  stat.size_limit_ = 1000;
  bool row_exceeded = false;
  bool size_exceeded = false;
  ASSERT_TRUE(ObLogicalTableMonitor::calc_estimate_exceeded_(stat, row_exceeded, size_exceeded));
  ASSERT_TRUE(size_exceeded);
}

TEST_F(TestLogicalTableMonitor, eval_exceeded_estimate_needs_db_recheck)
{
  ObLogicalTableMonitor::NsStat stat;
  stat.exists_ = true;
  stat.row_count_ = 100;
  stat.row_limit_ = 100;
  stat.size_limit_ = -1;
  bool exceeded = false;
  ASSERT_NE(OB_SUCCESS, monitor_.eval_namespace_limit_(
      ObString("test"), ObString("c"), 1, stat, exceeded));
}

TEST_F(TestLogicalTableMonitor, monitor_ttl_constants_sanity)
{
  ASSERT_EQ(7L * 24L * 3600L, ObLogicalTableMonitor::STATS_STALE_TTL_SECONDS);
  ASSERT_LT(10L * 1000L * 1000L, 1L * 60L * 1000L * 1000L);
}

TEST(TestLogicalTableMonitorNotInit, local_round_requires_init)
{
  ObLogicalTableMonitor monitor;
  ASSERT_EQ(OB_NOT_INIT, monitor.run_monitor_round_on_local_leaders());
}

TEST_F(TestLogicalTableMonitor, stopped_local_round_is_rejected)
{
  monitor_.stopped_ = true;
  ASSERT_EQ(OB_NOT_INIT, monitor_.run_monitor_round_on_local_leaders());
  ASSERT_EQ(0, static_cast<int64_t>(monitor_.running_rounds_));
}

TEST(TestLogicalTableMonitorNotInit, check_requires_init)
{
  ObLogicalTableMonitor monitor;
  bool exceeded = false;
  ASSERT_EQ(OB_NOT_INIT, monitor.check_logical_table_row_limit(
      1, 100, ObString("test"), ObString("c0ffee"), exceeded));
}

TEST_F(TestLogicalTableMonitor, check_rejects_invalid_namespace_id)
{
  bool exceeded = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, monitor_.check_logical_table_row_limit(
      OB_INVALID_ID, 100, ObString("test"), ObString("c0ffee"), exceeded));
}

TEST_F(TestLogicalTableMonitor, check_rejects_unsafe_sql_ident)
{
  bool exceeded = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, monitor_.check_logical_table_row_limit(
      1, 100, ObString("te`st"), ObString("c0ffee"), exceeded));
  ASSERT_EQ(OB_INVALID_ARGUMENT, monitor_.check_logical_table_row_limit(
      1, 100, ObString("test"), ObString("c0ffee'; DROP TABLE x;--"), exceeded));
  ASSERT_EQ(OB_INVALID_ARGUMENT, monitor_.check_logical_table_row_limit(
      1, 100, ObString("test"), ObString(""), exceeded));
  ASSERT_EQ(OB_INVALID_ARGUMENT, monitor_.check_logical_table_row_limit(
      1, 100, ObString(""), ObString("c0ffee"), exceeded));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
