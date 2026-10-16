/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define protected public
#include "share/rc/ob_tenant_base.h"
#undef protected
// Oracle client headers define CONST, which conflicts with Monotonicity::CONST.
#ifdef CONST
#undef CONST
#endif
#include "lib/allocator/ob_allocator.h"
#include "lib/json_type/ob_json_path.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObLogicalScanRtDefAllocatorTest : public ::testing::Test
{
public:
  ObLogicalScanRtDefAllocatorTest()
    : exec_allocator_("LtScanExecUT"),
      das_allocator_("LtScanDasUT"),
      exec_ctx_(exec_allocator_),
      eval_ctx_(exec_ctx_),
      ctdef_(exec_allocator_),
      snapshot_(),
      rtdef_(),
      saved_tenant_id_(OB_INVALID_TENANT_ID)
  {}
  ~ObLogicalScanRtDefAllocatorTest() override = default;

protected:
  int init_scan()
  {
    // Use the production allocation entry, not ensure_lt_scan_rt_allocated()
    // with an allocator selected by the test. Each invocation is a new DAS task.
    ObDASScanOp scan_op(das_allocator_);
    scan_op.set_scan_ctdef(&ctdef_);
    scan_op.set_scan_rtdef(&rtdef_);
    scan_op.set_snapshot(&snapshot_);
    return scan_op.init_scan_param();
  }

  ObArenaAllocator exec_allocator_;
  ObArenaAllocator das_allocator_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASScanCtDef ctdef_;
  transaction::ObTxReadSnapshot snapshot_;
  // Destroy the rtdef before the execution allocator, but after DAS arena reuse.
  ObDASScanRtDef rtdef_;

private:
  void SetUp() override
  {
    share::SCN read_scn;
    saved_tenant_id_ = MTL_ID();
    share::ObTenantEnv::get_tenant_local()->id_ = OB_SYS_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, read_scn.convert_for_tx(100));
    snapshot_.init_special_read(read_scn);
    ctdef_.logical_table_aux_type_ = share::LT_AUX_PRIMARY;
    ctdef_.lt_ctx_.analyzed_ = true;
    rtdef_.eval_ctx_ = &eval_ctx_;
    // Identity has already been resolved, as on the reopen path in the core.
    rtdef_.lt_rt_.namespace_id_ = 1011;
    rtdef_.lt_rt_.logical_table_id_ = 1011;
    rtdef_.lt_rt_.enabled_ = true;
    rtdef_.stmt_allocator_.set_alloc(&exec_allocator_);
    rtdef_.scan_allocator_.set_alloc(&das_allocator_);
    ASSERT_EQ(OB_SUCCESS, rtdef_.init_pd_op(exec_ctx_, ctdef_));
    // ObTableScanOp::close_and_reopen() rebinds this wrapper to the DAS arena.
    rtdef_.stmt_allocator_.set_alloc(&das_allocator_);
  }

  void TearDown() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = saved_tenant_id_;
  }

  uint64_t saved_tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLogicalScanRtDefAllocatorTest);
};

TEST_F(ObLogicalScanRtDefAllocatorTest, allocation_uses_exec_context_after_stmt_rebind)
{
  const int64_t exec_used_before = exec_allocator_.used();
  ASSERT_EQ(OB_SUCCESS, init_scan());
  ASSERT_NE(nullptr, rtdef_.lt_scan_rt_);
  // This assertion also detects the old allocation choice without Sanity.
  EXPECT_EQ(&exec_ctx_.get_allocator(), rtdef_.lt_scan_rt_->path_cache_.get_allocator());
  EXPECT_GE(exec_allocator_.used() - exec_used_before,
            static_cast<int64_t>(sizeof(ObLogicalTableScanRtDef)));
}

TEST_F(ObLogicalScanRtDefAllocatorTest, empty_path_cache_destruction_after_das_reuse)
{
  ASSERT_EQ(OB_SUCCESS, init_scan());
  ASSERT_NE(nullptr, rtdef_.lt_scan_rt_);
  EXPECT_EQ(0, rtdef_.lt_scan_rt_->path_cache_.size());
  rtdef_.lt_scan_rt_->physical_kv_tablet_id_ = ObTabletID(10001);
  // Match ObDASRef::reuse(): the rtdef remains alive while its former statement
  // arena is reset. The fixture then invokes the real ObDASScanRtDef destructor.
  // With the old code, Sanity detects the poisoned lt_scan_rt_ in that destructor.
  das_allocator_.reset_remain_one_page();
  EXPECT_EQ(0, das_allocator_.used());
}

TEST_F(ObLogicalScanRtDefAllocatorTest, populated_path_cache_survives_repeated_das_reuse)
{
  const int64_t PATH_COUNT = 12;
  const int64_t RESCAN_COUNT = 8;
  ObString path = ObString::make_string("$.nested.value");
  ObJsonPath *parsed_paths[PATH_COUNT] = {};
  ObLogicalTableScanRtDef *lt_scan_rt = nullptr;
  int64_t exec_used_after_init = 0;
  ASSERT_EQ(OB_SUCCESS, init_scan());
  ASSERT_NE(nullptr, rtdef_.lt_scan_rt_);
  lt_scan_rt = rtdef_.lt_scan_rt_;
  for (int64_t i = 0; i < PATH_COUNT; ++i) {
    // Use enough argument slots to allocate the path cache's internal vectors.
    ASSERT_EQ(OB_SUCCESS, lt_scan_rt->path_cache_.find_and_add_cache(
        exec_allocator_, parsed_paths[i], path, static_cast<int>(i)));
    ASSERT_NE(nullptr, parsed_paths[i]);
  }
  exec_used_after_init = exec_allocator_.used();
  for (int64_t round = 0; round < RESCAN_COUNT; ++round) {
    lt_scan_rt->direct_phys_ranges_built_ = true;
    lt_scan_rt->physical_kv_tablet_id_ = ObTabletID(10001 + round);
    das_allocator_.reset_remain_one_page();
    EXPECT_EQ(0, das_allocator_.used());
    // Access state after reuse, then create another task sharing the same rtdef.
    EXPECT_EQ(10001 + round, lt_scan_rt->physical_kv_tablet_id_.id());
    ASSERT_EQ(OB_SUCCESS, init_scan());
    EXPECT_EQ(lt_scan_rt, rtdef_.lt_scan_rt_);
    EXPECT_FALSE(lt_scan_rt->direct_phys_ranges_built_);
    for (int64_t i = 0; i < PATH_COUNT; ++i) {
      EXPECT_EQ(parsed_paths[i], lt_scan_rt->path_cache_.find_path(path, i));
      EXPECT_EQ(2, parsed_paths[i]->path_node_cnt());
    }
    // Reopening must not allocate another logical scan state or lose cached paths.
    EXPECT_EQ(exec_used_after_init, exec_allocator_.used());
  }
  das_allocator_.reset_remain_one_page();
}

TEST_F(ObLogicalScanRtDefAllocatorTest, ordinary_scan_does_not_allocate_logical_state)
{
  ctdef_.logical_table_aux_type_ = share::LT_AUX_MAX_TYPE;
  rtdef_.lt_rt_.reset_runtime_identity();
  ASSERT_EQ(OB_SUCCESS, init_scan());
  EXPECT_EQ(nullptr, rtdef_.lt_scan_rt_);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
