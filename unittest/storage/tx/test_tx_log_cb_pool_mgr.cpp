/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstdlib>
#include <gtest/gtest.h>
#define private public
#include "storage/tx/ob_tx_log_cb_mgr.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class PoolTestAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    ++attempts_;
    if (0 == remaining_allocations_) {
      return nullptr;
    }
    if (remaining_allocations_ > 0) {
      --remaining_allocations_;
    }
    void *ptr = std::malloc(size);
    if (nullptr != ptr) {
      ++live_allocations_;
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      --live_allocations_;
      std::free(ptr);
    }
  }

  int64_t remaining_allocations_ = -1;
  int64_t attempts_ = 0;
  int64_t live_allocations_ = 0;
};

struct PoolCase
{
  int64_t initial_pools_;
  int64_t active_tx_cnt_;
  int64_t expected_pools_;
};

class TestTxLogCbPoolMgr : public ::testing::TestWithParam<PoolCase>
{
public:
  void SetUp() override
  {
    mgr_.allocator_.set_allocator(&allocator_);
    ASSERT_EQ(OB_SUCCESS, mgr_.init(OB_SERVER_TENANT_ID, share::ObLSID(1001)));
  }
  void TearDown() override
  {
    mgr_.destroy();
    EXPECT_EQ(0, allocator_.live_allocations_);
  }

  PoolTestAllocator allocator_;
  ObTxLogCbPoolMgr mgr_;
};

TEST_P(TestTxLogCbPoolMgr, fills_only_missing_pools)
{
  const PoolCase param = GetParam();
  for (int64_t i = 0; i < param.initial_pools_; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.append_new_log_cb_pool_());
  }
  const int64_t attempts_before = allocator_.attempts_;
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(param.active_tx_cnt_));
  EXPECT_EQ(param.expected_pools_, mgr_.pool_list_.get_size());
  EXPECT_EQ(param.expected_pools_ - param.initial_pools_,
            allocator_.attempts_ - attempts_before);
  EXPECT_TRUE(mgr_.allow_expand_);
  EXPECT_FALSE(mgr_.is_all_busy());

  // Repeated takeover must not allocate or shrink an already sufficient pool list.
  const int64_t attempts_after = allocator_.attempts_;
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(param.active_tx_cnt_));
  EXPECT_EQ(param.expected_pools_, mgr_.pool_list_.get_size());
  EXPECT_EQ(attempts_after, allocator_.attempts_);
}

INSTANTIATE_TEST_CASE_P(pool_counts, TestTxLogCbPoolMgr, ::testing::Values(
    PoolCase{0, 0, 1}, PoolCase{0, 1, 1}, PoolCase{0, 1000, 3},
    PoolCase{1, 1000, 3}, PoolCase{2, 1000, 3}, PoolCase{3, 1000, 3},
    PoolCase{4, 1000, 4}, PoolCase{3, 0, 3},
    PoolCase{0, 341, 1}, PoolCase{0, 342, 2},
    PoolCase{0, 1024, 3}, PoolCase{0, 1025, 4}));

TEST_F(TestTxLogCbPoolMgr, allocation_failure_leaves_empty_pool_and_can_retry)
{
  allocator_.remaining_allocations_ = 0;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, mgr_.switch_to_leader(1000));
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, allocator_.attempts_);
  EXPECT_EQ(0, allocator_.live_allocations_);

  allocator_.remaining_allocations_ = -1;
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
  EXPECT_EQ(3, mgr_.pool_list_.get_size());
  EXPECT_EQ(4, allocator_.attempts_);
}

TEST_F(TestTxLogCbPoolMgr, partial_allocation_failure_preserves_pools_for_retry)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.append_new_log_cb_pool_());
  allocator_.remaining_allocations_ = 1;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, mgr_.switch_to_leader(1000));
  EXPECT_EQ(2, mgr_.pool_list_.get_size());
  EXPECT_EQ(3, allocator_.attempts_);
  EXPECT_EQ(2, allocator_.live_allocations_);

  allocator_.remaining_allocations_ = -1;
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
  EXPECT_EQ(3, mgr_.pool_list_.get_size());
  EXPECT_EQ(4, allocator_.attempts_);
}

TEST_F(TestTxLogCbPoolMgr, follower_cleanup_allows_next_takeover)
{
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
    ASSERT_EQ(3, mgr_.pool_list_.get_size());
    ASSERT_EQ(OB_SUCCESS, mgr_.clear_log_cb_pool(false /* for_replay */));
    EXPECT_EQ(0, mgr_.pool_list_.get_size());
    EXPECT_EQ(0, mgr_.gc_pool_list_.get_size());
    EXPECT_EQ(0, allocator_.live_allocations_);
    EXPECT_FALSE(mgr_.allow_expand_);
  }
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  EXPECT_EQ(1, mgr_.pool_list_.get_size());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_tx_log_cb_pool_mgr.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
