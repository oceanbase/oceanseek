/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/tablet/ob_tablet_mds_data_cache.h"

namespace oceanbase
{
using namespace storage;
namespace unittest
{

using State = ObMdsInfoCachePopulateState;

class TestMdsInfoCache : public ::testing::Test
{};

TEST_F(TestMdsInfoCache, initial_state_is_invalid)
{
  ObMdsInfoCache cache;
  ASSERT_EQ(static_cast<uint8_t>(State::INVALID), cache.populate_state_);
  ASSERT_EQ(0, cache.newest_commit_version_);
  ASSERT_EQ(0, cache.updating_cnt_);
  ASSERT_FALSE(cache.is_valid());
  ASSERT_FALSE(cache.is_empty());
  ASSERT_FALSE(cache.has_popluated());
}

// on_user_commit primes a fresh tablet's cache without any reader ever running.
TEST_F(TestMdsInfoCache, on_user_commit_primes_invalid_cache)
{
  ObMdsInfoCache cache;
  cache.on_set();          // tx writes a row: increments updating_cnt
  ASSERT_FALSE(cache.is_valid()); // still INVALID + updating_cnt>0
  cache.on_user_commit(100);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_TRUE(cache.has_popluated());
  ASSERT_FALSE(cache.is_empty());
  ASSERT_EQ(100, cache.newest_commit_version());
  ASSERT_EQ(0, cache.updating_cnt_);
}

// on_user_abort does NOT promote state — only releases the in-flight slot.
TEST_F(TestMdsInfoCache, on_user_abort_does_not_promote)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_user_abort();
  ASSERT_FALSE(cache.is_valid());
  ASSERT_FALSE(cache.has_popluated());
  ASSERT_EQ(0, cache.newest_commit_version());
  ASSERT_EQ(0, cache.updating_cnt_);

  // A subsequent committed write must still promote correctly.
  cache.on_set();
  cache.on_user_commit(50);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_EQ(50, cache.newest_commit_version());
}

// set_empty publishes EMPTY only when the cache is still INVALID; once promoted
// to NON_EMPTY by a commit it never regresses to EMPTY.
TEST_F(TestMdsInfoCache, set_empty_does_not_downgrade_non_empty)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_user_commit(80);
  ASSERT_FALSE(cache.is_empty());
  cache.set_empty();
  ASSERT_FALSE(cache.is_empty());
  ASSERT_TRUE(cache.is_valid());
  ASSERT_EQ(80, cache.newest_commit_version());
}

// EMPTY -> NON_EMPTY: a reader's empty seed is correctly flipped non-empty by
// a later commit.
TEST_F(TestMdsInfoCache, empty_promoted_to_non_empty_by_commit)
{
  ObMdsInfoCache cache;
  cache.set_empty();
  ASSERT_TRUE(cache.is_valid());
  ASSERT_TRUE(cache.is_empty());
  cache.on_set();
  cache.on_user_commit(10);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_FALSE(cache.is_empty());
  ASSERT_EQ(10, cache.newest_commit_version());
}

// updating_cnt > 0 gates is_valid even after NON_EMPTY: a fresh write whose
// commit has not yet fired must keep readers out of the cache-hit path.
TEST_F(TestMdsInfoCache, in_flight_set_makes_cache_invalid)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_user_commit(200);
  ASSERT_TRUE(cache.is_valid());

  cache.on_set(); // second in-flight tx
  ASSERT_FALSE(cache.is_valid()) << "updating_cnt > 0 should block is_valid";
  ASSERT_TRUE(cache.has_popluated()) << "populate_state stays NON_EMPTY";

  cache.on_user_commit(300);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_EQ(300, cache.newest_commit_version());
}

// newest_commit_version_ is max-monotone: out-of-order commits do not regress.
TEST_F(TestMdsInfoCache, newest_commit_version_is_max_monotonic)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_set();
  cache.on_user_commit(500);
  cache.on_user_commit(300); // earlier scn arriving later
  ASSERT_EQ(500, cache.newest_commit_version());
  ASSERT_TRUE(cache.is_valid());
}

// has_update_after_snapshot covers both newer commits and in-flight writes.
TEST_F(TestMdsInfoCache, has_update_after_snapshot)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_user_commit(100);
  ASSERT_FALSE(cache.has_update_after_snapshot(100));
  ASSERT_FALSE(cache.has_update_after_snapshot(200));
  ASSERT_TRUE(cache.has_update_after_snapshot(50));
  cache.on_set();
  ASSERT_TRUE(cache.has_update_after_snapshot(1000)) << "updating_cnt > 0 forces true";
}

// reset returns the cache to the absorbing INVALID state.
TEST_F(TestMdsInfoCache, reset_returns_to_invalid)
{
  ObMdsInfoCache cache;
  cache.on_set();
  cache.on_user_commit(999);
  cache.on_set();
  cache.reset();
  ASSERT_FALSE(cache.is_valid());
  ASSERT_FALSE(cache.has_popluated());
  ASSERT_EQ(0, cache.newest_commit_version());
  ASSERT_EQ(0, cache.updating_cnt_);
}

// set_value with cnt==0 seeds EMPTY only when still INVALID; with cnt>0 stores
// NON_EMPTY unconditionally; newest_commit_version_ is max-monotone.
TEST_F(TestMdsInfoCache, set_value_paths)
{
  {
    ObMdsInfoCache cache;
    cache.set_value(123, 0u);
    ASSERT_TRUE(cache.is_empty());
    ASSERT_EQ(123, cache.newest_commit_version());
  }
  {
    ObMdsInfoCache cache;
    cache.set_value(123, 1u);
    ASSERT_TRUE(cache.is_valid());
    ASSERT_FALSE(cache.is_empty());
    ASSERT_EQ(123, cache.newest_commit_version());
  }
  {
    ObMdsInfoCache cache;
    cache.on_set();
    cache.on_user_commit(500);
    cache.set_value(100, 0u); // would-be EMPTY seed must NOT downgrade NON_EMPTY
    ASSERT_FALSE(cache.is_empty());
    ASSERT_TRUE(cache.is_valid());
    ASSERT_EQ(500, cache.newest_commit_version()) << "max-CAS keeps higher scn";
  }
}

// populate 门禁：snapshot >= newest_commit_version；不完整读保持 INVALID。
TEST_F(TestMdsInfoCache, populate_gate_incomplete_snapshot_stays_invalid)
{
  ObMdsInfoCache cache;
  const int64_t newest_commit_version = 200;
  const int64_t snapshot = 100;
  ASSERT_FALSE(cache.has_popluated());
  if (snapshot < newest_commit_version) {
    // 与 inner_read_mds_info_array 一致：不调用 set_value/set_empty
  } else {
    cache.set_value(newest_commit_version, 1u);
  }
  ASSERT_FALSE(cache.has_popluated());
  ASSERT_FALSE(cache.is_valid());

  cache.set_value(newest_commit_version, 1u);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_EQ(newest_commit_version, cache.newest_commit_version());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_mds_info_cache.log*");
  OB_LOGGER.set_file_name("test_mds_info_cache.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
