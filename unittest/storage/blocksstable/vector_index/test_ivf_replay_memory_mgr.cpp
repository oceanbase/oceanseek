/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "share/vector_index/ob_ivf_replay_memory_mgr.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;

TEST(TestIVFReplayMemoryMgr, zero_budget_retains_no_data)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation first;
  ObIVFReplayMemoryReservation second;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(0, 16, first));
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(0, 4, second));
  EXPECT_EQ(0, first.get_slice_memory_limit());
  EXPECT_EQ(0, second.get_slice_memory_limit());
  EXPECT_TRUE(first.is_reserved());
  EXPECT_TRUE(second.is_reserved());
  EXPECT_EQ(16, first.get_slice_count());
  EXPECT_EQ(0, first.get_configured_limit());
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  EXPECT_EQ(2, mgr.get_active_build_count());
  first.reset();
  first.reset();
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  EXPECT_EQ(1, mgr.get_active_build_count());
  second.reset();
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  EXPECT_EQ(0, mgr.get_active_build_count());
}

TEST(TestIVFReplayMemoryMgr, exhausted_build_spills_without_waiting)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation first;
  ObIVFReplayMemoryReservation second;
  ObIVFReplayMemoryReservation third;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(16L << 30, 16, first));
  EXPECT_EQ(1L << 30, first.get_slice_memory_limit());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(16L << 30, 16, second));
  EXPECT_TRUE(second.is_reserved());
  EXPECT_EQ(0, second.get_slice_memory_limit());
  EXPECT_EQ(16L << 30, mgr.get_reserved_bytes());
  first.reset();
  // A running build keeps its original quota, including a zero-byte quota.
  EXPECT_EQ(0, second.get_slice_memory_limit());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(16L << 30, 8, third));
  EXPECT_EQ(2L << 30, third.get_slice_memory_limit());
  EXPECT_EQ(2, mgr.get_active_build_count());
}

TEST(TestIVFReplayMemoryMgr, dynamic_limit_keeps_existing_reservations)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation first;
  ObIVFReplayMemoryReservation grown;
  ObIVFReplayMemoryReservation shrunk;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(1L << 30, 16, first));
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(3L << 30, 2, grown));
  EXPECT_EQ(1L << 30, first.get_reserved_bytes());
  EXPECT_EQ(1L << 30, grown.get_slice_memory_limit());
  EXPECT_EQ(3L << 30, mgr.get_reserved_bytes());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(1L << 30, 16, shrunk));
  EXPECT_EQ(0, shrunk.get_reserved_bytes());
  EXPECT_EQ(3L << 30, mgr.get_reserved_bytes());
  ObIVFReplayMemoryReservation disabled;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(0, 16, disabled));
  EXPECT_EQ(0, disabled.get_reserved_bytes());
  EXPECT_EQ(3L << 30, mgr.get_reserved_bytes());
  first.reset();
  grown.reset();
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  ObIVFReplayMemoryReservation resumed;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(2L << 30, 8, resumed));
  EXPECT_EQ(256L << 20, resumed.get_slice_memory_limit());
  EXPECT_EQ(0, disabled.get_reserved_bytes());
  EXPECT_EQ(3, mgr.get_active_build_count());
}

TEST(TestIVFReplayMemoryMgr, tiny_and_non_divisible_limits_do_not_overbook)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation too_small;
  ObIVFReplayMemoryReservation divided;
  ObIVFReplayMemoryReservation remainder;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(4, 16, too_small));
  EXPECT_EQ(0, too_small.get_slice_memory_limit());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(33, 16, divided));
  EXPECT_EQ(2, divided.get_slice_memory_limit());
  EXPECT_EQ(32, mgr.get_reserved_bytes());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(33, 1, remainder));
  EXPECT_EQ(1, remainder.get_slice_memory_limit());
  EXPECT_EQ(33, mgr.get_reserved_bytes());
}

TEST(TestIVFReplayMemoryMgr, invalid_or_repeated_reservation_preserves_state)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation reservation;
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.reserve(-1, 1, reservation));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.reserve(1, 0, reservation));
  EXPECT_FALSE(reservation.is_reserved());
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(100, 4, reservation));
  EXPECT_EQ(OB_INIT_TWICE, mgr.reserve(200, 4, reservation));
  EXPECT_EQ(100, mgr.get_reserved_bytes());
  EXPECT_EQ(1, mgr.get_active_build_count());
}

TEST(TestIVFReplayMemoryMgr, zero_budget_does_not_consume_later_positive_quota)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation disabled;
  ObIVFReplayMemoryReservation enabled;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(0, 16, disabled));
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(64L << 20, 4, enabled));
  EXPECT_EQ(16L << 20, enabled.get_slice_memory_limit());
  EXPECT_EQ(0, disabled.get_slice_memory_limit());
  EXPECT_EQ(64L << 20, mgr.get_reserved_bytes());
}

TEST(TestIVFReplayMemoryMgr, extreme_limits_do_not_overflow)
{
  ObIVFReplayMemoryMgr mgr;
  ObIVFReplayMemoryReservation full;
  ObIVFReplayMemoryReservation exhausted;
  ObIVFReplayMemoryReservation disabled;
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(INT64_MAX, 1, full));
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(INT64_MAX, 1, exhausted));
  EXPECT_EQ(INT64_MAX, mgr.get_reserved_bytes());
  EXPECT_EQ(0, exhausted.get_reserved_bytes());
  full.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.reserve(0, INT64_MAX, disabled));
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  EXPECT_EQ(0, disabled.get_slice_memory_limit());
  EXPECT_EQ(INT64_MAX, disabled.get_slice_count());
}

TEST(TestIVFReplayMemoryMgr, tenant_instances_are_isolated)
{
  ObIVFReplayMemoryMgr first_tenant;
  ObIVFReplayMemoryMgr second_tenant;
  ObIVFReplayMemoryReservation first;
  ObIVFReplayMemoryReservation second;
  ASSERT_EQ(OB_SUCCESS, first_tenant.reserve(16L << 30, 16, first));
  ASSERT_EQ(OB_SUCCESS, second_tenant.reserve(16L << 30, 16, second));
  EXPECT_EQ(1L << 30, first.get_slice_memory_limit());
  EXPECT_EQ(1L << 30, second.get_slice_memory_limit());
  first.reset();
  EXPECT_EQ(0, first_tenant.get_reserved_bytes());
  EXPECT_EQ(16L << 30, second_tenant.get_reserved_bytes());
}

TEST(TestIVFReplayMemoryMgr, scope_exit_returns_failed_build_reservation)
{
  ObIVFReplayMemoryMgr mgr;
  {
    ObIVFReplayMemoryReservation failed_build;
    ASSERT_EQ(OB_SUCCESS, mgr.reserve(16L << 30, 16, failed_build));
    EXPECT_EQ(16L << 30, mgr.get_reserved_bytes());
    // The build fails before any slice is initialized; guard cleanup alone
    // must return the quota, just as after successful replay destruction.
  }
  EXPECT_EQ(0, mgr.get_reserved_bytes());
  EXPECT_EQ(0, mgr.get_active_build_count());
}

TEST(TestIVFReplayMemoryMgr, simultaneous_builds_never_overbook)
{
  static const int64_t THREAD_COUNT = 16;
  static const int64_t LIMIT = (64L << 20) + 7;
  for (int64_t limit : {int64_t(0), LIMIT}) {
    SCOPED_TRACE(limit);
    ObIVFReplayMemoryMgr mgr;
    std::atomic<bool> start(false);
    std::atomic<bool> release(false);
    std::atomic<int64_t> ready(0);
    int ret_codes[THREAD_COUNT] = {};
    int64_t reserved[THREAD_COUNT] = {};
    std::vector<std::thread> threads;
    for (int64_t i = 0; i < THREAD_COUNT; ++i) {
      threads.emplace_back([&, i]() {
        ObIVFReplayMemoryReservation reservation;
        while (!start.load()) {
          std::this_thread::yield();
        }
        ret_codes[i] = mgr.reserve(limit, i + 1, reservation);
        reserved[i] = reservation.get_reserved_bytes();
        ready.fetch_add(1);
        while (!release.load()) {
          std::this_thread::yield();
        }
      });
    }
    start.store(true);
    while (ready.load() != THREAD_COUNT) {
      std::this_thread::yield();
    }
    int64_t sum = 0;
    for (int64_t i = 0; i < THREAD_COUNT; ++i) {
      EXPECT_EQ(OB_SUCCESS, ret_codes[i]);
      sum += reserved[i];
    }
    EXPECT_LE(sum, limit);
    EXPECT_EQ(sum, mgr.get_reserved_bytes());
    EXPECT_EQ(THREAD_COUNT, mgr.get_active_build_count());
    release.store(true);
    for (std::thread &thread : threads) {
      thread.join();
    }
    EXPECT_EQ(0, mgr.get_reserved_bytes());
    EXPECT_EQ(0, mgr.get_active_build_count());
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
