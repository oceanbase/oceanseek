// owner: yunxing.cyx
// owner group: transaction

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SHARE

#define protected public
#define private public
#include "mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/throttle/ob_share_throttle_define.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
namespace share
{

// Regression tests for the MDS memstore hard memory limit, i.e. the
// ObMemstoreResourceKind::MDS branch of
// ObMemstoreAllocator::check_memory_limit_() (MR for Dima 2026092100119304850).
//
// All cases drive the real ObMemstoreAllocator::alloc() of the MTL MDS arena.
// The two throttle buckets are controlled directly:
//   - the MdsMemstore module bucket via
//     ObShareResourceThrottleTool::set_resource_limit<ObMdsMemstoreAllocator>();
//   - the TxShare share bucket by storing share_unit.resource_limit_ directly,
//     because the share unit has the adaptive limit enabled and the public
//     setter only records the config-specified limit, while the last-resort
//     hard check reads resource_limit_ itself.
//
// The expected semantics follow the MR description as updated after the
// review finding MDS_LIMIT_SHARE_COUPLING_01: the MDS branch checks the
// MdsMemstore module bucket only (hold + aligned apply > limit, strict
// greater-than) via exceeded_module_resource_limit<ObMdsMemstoreAllocator>().
// The aggregated TxShare share bucket must NOT take part in the hard
// rejection: when the share quota is exhausted by the other modules
// (DATA memstore + TxData + Vector), an MDS allocation below its own module
// limit still succeeds, so MDS availability is never coupled to the other
// modules' usage.
//
// The module-limit cases (module_limit_boundary_semantics,
// one_byte_request_rejected_when_only_7_bytes_left and
// mds_module_bucket_independently_rejects) are the regression guard for the
// new MDS branch: they fail if the branch is removed, because alloc() then
// keeps succeeding with room left in the arena page.
class TestMdsMemstoreMemoryLimit : public ::testing::Test
{
public:
  TestMdsMemstoreMemoryLimit() : dummy_mt_(), handle_(dummy_mt_) {}
  virtual ~TestMdsMemstoreMemoryLimit() = default;

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, storage::MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    storage::MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp() override
  {
    ASSERT_TRUE(storage::MockTenantModuleEnv::get_instance().is_inited());
    alloc_mgr_ = MTL(ObSharedMemAllocMgr *);
    ASSERT_NE(nullptr, alloc_mgr_);
    mds_alloc_ = &(alloc_mgr_->mds_memstore_allocator());
    tool_ = &(alloc_mgr_->share_resource_throttle_tool());
    saved_mds_limit_ = tool_->get_resource_limit<ObMdsMemstoreAllocator>();
    saved_share_limit_ = share_unit().resource_limit_;
    ASSERT_EQ(OB_SUCCESS, handle_.bind(ObMemstoreResourceKind::MDS));
    // Start every case from permissive buckets; each case shrinks the bucket
    // it exercises. TearDown restores the environment values.
    relax_both_limits();
  }

  virtual void TearDown() override
  {
    // Unbind the handle and restore both bucket limits so that later cases
    // and the environment teardown observe the original configuration.
    set_mds_limit(saved_mds_limit_);
    ATOMIC_STORE(&share_unit().resource_limit_, saved_share_limit_);
    handle_.destroy();
  }

private:
  ObThrottleUnit<FakeAllocatorForTxShare> &share_unit()
  {
    return tool_->module_throttle_tuple_
        .element<ModuleThrottleTool<FakeAllocatorForTxShare>>()
        .module_throttle_unit_;
  }

  void set_mds_limit(const int64_t limit)
  {
    tool_->set_resource_limit<ObMdsMemstoreAllocator>(limit);
  }

  void set_share_limit(const int64_t limit)
  {
    ATOMIC_STORE(&share_unit().resource_limit_, limit);
  }

  // Both buckets far above anything the arena can hold in this test, so that
  // only the bucket a case shrinks can reject. 64GB keeps the throttle decay
  // arithmetic free of overflow.
  void relax_both_limits()
  {
    set_mds_limit(RELAXED_LIMIT);
    set_share_limit(RELAXED_LIMIT);
  }

  int64_t mds_hold() const { return mds_alloc_->hold(); }

  // The aggregated holding size the TxShare share bucket compares against.
  int64_t share_hold_sum() const
  {
    return alloc_mgr_->mds_memstore_allocator().hold()
         + alloc_mgr_->memstore_allocator().hold()
         + alloc_mgr_->tx_data_allocator().hold()
         + alloc_mgr_->vector_allocator().hold();
  }

  // Direct access to the private limit check for boundary-semantics cases.
  int check_mds_memory_limit(const int64_t alloc_size, bool &is_out_of_mem)
  {
    return mds_alloc_->check_memory_limit_(MTL_ID(), alloc_size, is_out_of_mem);
  }

  // One successful allocation so that the arena has an active page and later
  // small allocations do not create a new page and move hold().
  void *warm_up_alloc()
  {
    void *ptr = mds_alloc_->alloc(handle_, 1);
    EXPECT_NE(nullptr, ptr);
    return ptr;
  }

private:
  static const int64_t RELAXED_LIMIT = 64LL << 30;  // 64GB
  memtable::ObMemtable dummy_mt_;
  ObMemstoreAllocator::AllocHandle handle_;
  ObSharedMemAllocMgr *alloc_mgr_;
  ObMemstoreAllocator *mds_alloc_;
  TxShareThrottleTool *tool_;
  int64_t saved_mds_limit_;
  int64_t saved_share_limit_;
};

// Runs first on purpose: the MTL MDS arena is still empty in this binary
// because the fixture creates no LS / LS_MDS_TABLET, so hold() == 0.
//
// The hard check compares the requested aligned size only, while the arena
// charges the whole new page on the first allocation: with a module limit
// below one page the check passes, and the actual hold overshoots the limit.
// This is the accepted page-granularity overshoot of the last-resort check:
// the overshoot is bounded by one arena page, and once hold is above the
// limit every further request is hard-rejected.
TEST_F(TestMdsMemstoreMemoryLimit, page_overshoot_bounded_and_then_rejected)
{
  ASSERT_EQ(0, mds_hold());  // precondition: empty arena
  const int64_t small_limit = 4 * 1024;  // below one arena page
  set_mds_limit(small_limit);
  void *ptr = mds_alloc_->alloc(handle_, 1);
  ASSERT_NE(nullptr, ptr);  // check passed: 0 + aligned(1) = 8 <= 4K
  const int64_t hold_after_page = mds_hold();
  ASSERT_GT(hold_after_page, small_limit);  // the new page pushed hold over the limit
  // The arena page is charged at the malloc big-chunk granularity (2MB), so
  // the overshoot of an accepted first allocation is bounded by one chunk.
  ASSERT_LE(hold_after_page - small_limit, 2LL * 1024 * 1024);
  // While hold stays above the limit, further requests are rejected and hold
  // keeps stable.
  const int64_t hold_before = mds_hold();
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 64));
  EXPECT_EQ(hold_before, mds_hold());
}

// Boundary semantics of the MDS module bucket: hold + aligned_size is
// compared with the strict greater-than, so the equal case is admitted.
// Covers the direct check_memory_limit_() result and the real alloc() path.
TEST_F(TestMdsMemstoreMemoryLimit, module_limit_boundary_semantics)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  bool is_out_of_mem = false;

  // below: hold + 8 < limit
  set_mds_limit(hold + 100);
  EXPECT_EQ(OB_SUCCESS, check_mds_memory_limit(8, is_out_of_mem));
  EXPECT_FALSE(is_out_of_mem);
  EXPECT_NE(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());

  // equal: hold + 8 == limit is admitted (strict >)
  set_mds_limit(hold + 8);
  EXPECT_EQ(OB_SUCCESS, check_mds_memory_limit(8, is_out_of_mem));
  EXPECT_FALSE(is_out_of_mem);
  EXPECT_NE(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());

  // above: hold + 8 > limit by one byte
  set_mds_limit(hold + 7);
  EXPECT_EQ(OB_SUCCESS, check_mds_memory_limit(8, is_out_of_mem));
  EXPECT_TRUE(is_out_of_mem);
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());
}

// A 1-byte request is aligned up to 8 bytes: with only 7 bytes left below the
// limit the aligned request no longer fits and the allocation fails without
// growing hold().
TEST_F(TestMdsMemstoreMemoryLimit, one_byte_request_rejected_when_only_7_bytes_left)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  set_mds_limit(hold + 7);  // only 7 bytes below the limit
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());
}

// The share bucket must not reject on its own (review finding
// MDS_LIMIT_SHARE_COUPLING_01): the MDS module bucket is permissive and the
// TxShare aggregate is at its limit, so the MDS allocation still succeeds —
// MDS availability is not coupled to the share quota usage of the other
// modules. The share quota still guards MDS through the rate-model throttle
// path, not through this hard-rejection path.
TEST_F(TestMdsMemstoreMemoryLimit, share_bucket_exhaustion_does_not_reject_mds)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  const int64_t share_sum = share_hold_sum();
  // module bucket permissive, share bucket exactly exhausted
  set_mds_limit(RELAXED_LIMIT);
  set_share_limit(share_sum);
  bool is_out_of_mem = true;
  EXPECT_EQ(OB_SUCCESS, check_mds_memory_limit(8, is_out_of_mem));
  EXPECT_FALSE(is_out_of_mem);
  EXPECT_NE(nullptr, mds_alloc_->alloc(handle_, 1));
}

// The module bucket rejects on its own: the share bucket is permissive, the
// MDS hold is at its own limit, so hold + 8 > module limit.
TEST_F(TestMdsMemstoreMemoryLimit, mds_module_bucket_independently_rejects)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  set_mds_limit(hold);  // any aligned request exceeds the module limit
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());
}

// Repeated rejections do not grow hold() and do not damage the handle: after
// the limits are restored the same handle allocates successfully again, so
// there is no permanent failure state.
TEST_F(TestMdsMemstoreMemoryLimit, repeated_failures_then_recovery_on_same_handle)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  const int64_t share_sum = share_hold_sum();
  set_mds_limit(hold);
  set_share_limit(share_sum);
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
    EXPECT_EQ(hold, mds_hold());
  }
  // restore both buckets and allocate through the same handle again
  relax_both_limits();
  EXPECT_NE(nullptr, mds_alloc_->alloc(handle_, 1));
}

// The DATA kind is still decided by the tenant freezer, not by the MDS
// branch: on a fresh tenant the DATA memstore is not full, so both the direct
// check and a real DATA allocation succeed.
TEST_F(TestMdsMemstoreMemoryLimit, data_kind_still_decided_by_freezer)
{
  bool is_out_of_mem = true;
  EXPECT_EQ(OB_SUCCESS, alloc_mgr_->memstore_allocator().check_memory_limit_(MTL_ID(), 8, is_out_of_mem));
  EXPECT_FALSE(is_out_of_mem);

  memtable::ObMemtable data_mt;
  ObMemstoreAllocator::AllocHandle data_handle(data_mt);
  ASSERT_EQ(OB_SUCCESS, data_handle.bind(ObMemstoreResourceKind::DATA));
  EXPECT_NE(nullptr, alloc_mgr_->memstore_allocator().alloc(data_handle, 1));
  data_handle.destroy();
}

// A null throttle tool pointer must fail closed: alloc() returns nullptr,
// nothing is allocated and the process does not crash. Restoring the pointer
// makes the same handle work again.
TEST_F(TestMdsMemstoreMemoryLimit, null_throttle_tool_fails_closed)
{
  warm_up_alloc();
  const int64_t hold = mds_hold();
  TxShareThrottleTool *saved_tool = mds_alloc_->throttle_tool_;
  mds_alloc_->throttle_tool_ = nullptr;
  EXPECT_EQ(nullptr, mds_alloc_->alloc(handle_, 1));
  EXPECT_EQ(hold, mds_hold());
  mds_alloc_->throttle_tool_ = saved_tool;
  EXPECT_NE(nullptr, mds_alloc_->alloc(handle_, 1));
}

}  // namespace share
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_memstore_memory_limit.log*");
  OB_LOGGER.set_file_name("test_mds_memstore_memory_limit.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
