/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstdlib>
#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
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
  // A tenant of its own, so that lowering its memory limit affects nothing else.
  static constexpr uint64_t TENANT_ID = 1002;

  static void SetUpTestCase()
  {
    const int ret =
        lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
  }

  void SetUp() override
  {
    // 10% of 2 GB is room for about 60 pools, enough for every case here.
    lib::set_tenant_memory_limit(TENANT_ID, 2L << 30);
    mgr_.allocator_.set_allocator(&allocator_);
    ASSERT_EQ(OB_SUCCESS, budget_.init(TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, mgr_.init(TENANT_ID, share::ObLSID(1001), budget_));
  }
  void TearDown() override
  {
    mgr_.destroy();
    EXPECT_EQ(0, allocator_.live_allocations_);
    EXPECT_EQ(0, budget_.get_used());
  }

  void set_pool_limit(const int64_t pool_cnt)
  {
    lib::set_tenant_memory_limit(TENANT_ID, pool_cnt * 10 * sizeof(ObTxLogCbPool));
    ASSERT_EQ(pool_cnt, budget_.get_limit());
  }

  // Expansion is prohibited through the real trigger: pools were expanded but the log
  // throughput did not grow.
  void prohibit_expansion()
  {
    for (int64_t i = 0; i < MAX_SYNC_SIZE_HISTORY_RECORD_SIZE; ++i) {
      mgr_.sync_size_history_[i * 2] = 0;
      mgr_.sync_size_history_[i * 2 + 1] = ObTxLogCbPoolMgr::EXPAND;
    }
  }

  void set_observation_interval(const int64_t interval_us)
  {
    DLIST_FOREACH_NORET(pool, mgr_.pool_list_)
    {
      pool->stat_.barrier_ts_ = ObTimeUtility::fast_current_time() - interval_us;
    }
  }

  // Retire a referenced pool through the existing cleanup path, without waiting.
  void retire_pool(ObTxLogCbPool *pool)
  {
    SpinWLockGuard guard(mgr_.pool_list_rw_lock_);
    (void)ATOMIC_BCAS(&mgr_.idle_pool_ptr_, pool, nullptr);
    ASSERT_EQ(OB_EAGAIN, mgr_.free_log_cb_pool_(pool, 0));
  }

  PoolTestAllocator allocator_;
  ObTxLogCbPoolBudget budget_;
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
  EXPECT_EQ(0, budget_.get_used());

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

TEST_F(TestTxLogCbPoolMgr, prohibited_expansion_still_shrinks_the_idle_pools)
{
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.append_new_log_cb_pool_());
  }
  prohibit_expansion();
  set_observation_interval(1000L * 1000);

  // The idle pools must be released even though expanding is prohibited.
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_FALSE(mgr_.allow_expand_);
  EXPECT_EQ(5, mgr_.pool_list_.get_size());
  EXPECT_EQ(5, allocator_.live_allocations_);

  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(0, allocator_.live_allocations_);
}

TEST_F(TestTxLogCbPoolMgr, no_observation_interval_keeps_the_pools)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
  ASSERT_EQ(3, mgr_.pool_list_.get_size());
  // A non positive observation interval must not be divided by.
  set_observation_interval(-1000L * 1000);
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(1000));
  EXPECT_EQ(3, mgr_.pool_list_.get_size());
}

TEST_F(TestTxLogCbPoolMgr, shrink_reaches_idle_pools_after_a_busy_prefix)
{
  for (int i = 0; i < 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.append_new_log_cb_pool_());
  }
  ObTxLogCbPoolRefGuard guards[30];
  ObTxLogCbPool *pool = mgr_.pool_list_.get_first();
  for (int i = 0; i < 30; ++i, pool = pool->get_next()) {
    guards[i].set_pool_ptr(pool);
  }
  prohibit_expansion();
  set_observation_interval(1000L * 1000);

  // The first round scans only the 30 pinned pools, leaving the five idle pools.
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_EQ(35, mgr_.pool_list_.get_size());
  // The next round must reach and reclaim the idle tail, still at most five pools.
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_EQ(30, mgr_.pool_list_.get_size());
  EXPECT_EQ(30, allocator_.live_allocations_);
}

TEST_F(TestTxLogCbPoolMgr, prewarming_is_bounded_by_the_tenant_budget)
{
  set_pool_limit(2);
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(INT64_MAX / 4));
  EXPECT_EQ(2, mgr_.pool_list_.get_size());
  EXPECT_EQ(2, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, expansion_is_bounded_by_the_tenant_budget)
{
  set_pool_limit(2);
  // Without pools the round wants three, the budget stops it at two.
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(1000));
  EXPECT_EQ(2, mgr_.pool_list_.get_size());
  EXPECT_EQ(2, budget_.get_used());
  // Idle pools are still reclaimed while the budget is used up.
  set_observation_interval(1000L * 1000);
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(0, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, budget_is_shared_between_ls_and_held_until_gc)
{
  set_pool_limit(1);
  PoolTestAllocator other_allocator;
  ObTxLogCbPoolMgr other_mgr;
  other_mgr.allocator_.set_allocator(&other_allocator);
  ASSERT_EQ(OB_SUCCESS, other_mgr.init(TENANT_ID, share::ObLSID(1002), budget_));

  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  ASSERT_EQ(1, mgr_.pool_list_.get_size());
  ASSERT_EQ(OB_SUCCESS, other_mgr.switch_to_leader(0));
  EXPECT_EQ(0, other_mgr.pool_list_.get_size());

  // A pool awaiting GC still holds its slot, the slot is returned when it is freed.
  ObTxLogCbPool *pool = mgr_.pool_list_.get_first();
  ObTxLogCbPoolRefGuard guard;
  guard.set_pool_ptr(pool);
  retire_pool(pool);
  ASSERT_EQ(1, mgr_.reclaim_gc_pool_list_());
  EXPECT_EQ(1, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(1, budget_.get_used());
  ASSERT_EQ(OB_SUCCESS, other_mgr.switch_to_leader(0));
  EXPECT_EQ(0, other_mgr.pool_list_.get_size());

  guard.reset();
  ASSERT_EQ(0, mgr_.reclaim_gc_pool_list_());
  EXPECT_EQ(0, budget_.get_used());
  ASSERT_EQ(OB_SUCCESS, other_mgr.switch_to_leader(0));
  EXPECT_EQ(1, other_mgr.pool_list_.get_size());

  other_mgr.destroy();
  EXPECT_EQ(0, other_allocator.live_allocations_);
}

TEST_F(TestTxLogCbPoolMgr, gc_pool_is_reclaimed_by_leader_takeover)
{
  set_pool_limit(1);
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  ObTxLogCbPool *pool = mgr_.pool_list_.get_first();
  ObTxLogCbPoolRefGuard guard;
  guard.set_pool_ptr(pool);
  // Clean up times out while the pool is referenced: it is left in GC, the budget is kept.
  retire_pool(pool);
  ASSERT_EQ(1, mgr_.reclaim_gc_pool_list_());
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(1, budget_.get_used());

  // The reference is released, and the next takeover, without any explicit free, gives
  // the budget back and prewarms a fresh pool.
  guard.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  EXPECT_EQ(0, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(1, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, gc_pool_is_reclaimed_by_leader_adjust_without_active_pools)
{
  set_pool_limit(1);
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  ObTxLogCbPool *pool = mgr_.pool_list_.get_first();
  ObTxLogCbPoolRefGuard guard;
  guard.set_pool_ptr(pool);
  retire_pool(pool);
  ASSERT_EQ(1, mgr_.reclaim_gc_pool_list_());
  guard.reset();

  // No active pool and no expansion: the periodic adjust still returns the budget.
  ASSERT_EQ(OB_SUCCESS, mgr_.adjust_log_cb_pool(0));
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(0, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(0, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, huge_tenant_memory_limit_does_not_overflow_the_budget)
{
  lib::set_tenant_memory_limit(TENANT_ID, INT64_MAX);
  EXPECT_GT(budget_.get_limit(), 0);
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
  EXPECT_EQ(3, mgr_.pool_list_.get_size());
  EXPECT_EQ(3, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, clear_keeps_budget_until_the_reference_is_released)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  ObTxLogCbPool *pool = mgr_.pool_list_.get_first();
  ObTxLogCbPoolRefGuard guard;
  guard.set_pool_ptr(pool);
  // The existing cleanup timeout leaves the referenced pool in GC with its budget.
  EXPECT_EQ(OB_EAGAIN, mgr_.clear_log_cb_pool(false /* for_replay */));
  EXPECT_EQ(0, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(1, budget_.get_used());

  guard.reset();
  EXPECT_EQ(OB_SUCCESS, mgr_.clear_log_cb_pool(false /* for_replay */));
  EXPECT_EQ(0, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(0, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, leader_takeover_reclaims_all_freeable_gc_pools)
{
  ObTxLogCbPoolRefGuard guards[7];
  for (int i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.append_new_log_cb_pool_());
    guards[i].set_pool_ptr(mgr_.pool_list_.get_last());
  }
  while (mgr_.pool_list_.get_size() > 0) {
    retire_pool(mgr_.pool_list_.get_first());
  }
  ASSERT_EQ(7, mgr_.gc_pool_list_.get_size());
  ASSERT_EQ(7, budget_.get_used());
  for (int i = 0; i < 7; ++i) {
    guards[i].reset();
  }

  // All 7 pools are freeable, the takeover must not leave any of them to the next round.
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  EXPECT_EQ(0, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(1, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, budget_.get_used());
}

TEST_F(TestTxLogCbPoolMgr, leader_reclaims_freeable_gc_pool_before_a_busy_tail)
{
  set_pool_limit(2);
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(1000));
  ObTxLogCbPool *first = mgr_.pool_list_.get_first();
  ObTxLogCbPool *last = mgr_.pool_list_.get_last();
  ObTxLogCbPoolRefGuard first_guard;
  ObTxLogCbPoolRefGuard last_guard;
  first_guard.set_pool_ptr(first);
  last_guard.set_pool_ptr(last);
  retire_pool(first);
  retire_pool(last);
  ASSERT_EQ(2, budget_.get_used());

  // The older GC pool can release one slot even while the tail is still referenced.
  first_guard.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.switch_to_leader(0));
  EXPECT_EQ(1, mgr_.pool_list_.get_size());
  EXPECT_EQ(1, mgr_.gc_pool_list_.get_size());
  EXPECT_EQ(last, mgr_.gc_pool_list_.get_first());
  EXPECT_EQ(2, budget_.get_used());
}

TEST(TestTxLogCbPoolMgrUninit, adjust_before_init_does_not_push_sync_size_history)
{
  ObTxLogCbPoolMgr mgr;
  const int64_t last_flag_idx = (MAX_SYNC_SIZE_HISTORY_RECORD_SIZE - 1) * 2 + 1;
  ASSERT_EQ(0, mgr.sync_size_history_[last_flag_idx]);
  EXPECT_EQ(OB_NOT_INIT, mgr.adjust_log_cb_pool(1000));
  // A pushed round would have recorded NO_CHANGE (-1) here.
  EXPECT_EQ(0, mgr.sync_size_history_[last_flag_idx]);
  EXPECT_EQ(0, mgr.pool_list_.get_size());
  EXPECT_FALSE(mgr.allow_expand_);
  EXPECT_EQ(OB_NOT_INIT, mgr.switch_to_leader(1000));
  EXPECT_EQ(0, mgr.pool_list_.get_size());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_tx_log_cb_pool_mgr.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
