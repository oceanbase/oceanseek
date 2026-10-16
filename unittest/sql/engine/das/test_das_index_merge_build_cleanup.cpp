/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "lib/utility/ob_tracepoint.h"
#include <algorithm>
#include <cstdlib>
#include <vector>
#define private public
#define protected public
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_index_merge_and_iter.h"
#include "share/rc/ob_tenant_base.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
class MergeFailingAllocator : public ObIAllocator
{
public:
  struct Allocation { void *ptr_; int64_t size_; };
  MergeFailingAllocator() : attempts_(0), fail_on_(0) {}
  ~MergeFailingAllocator() override
  {
    for (const auto &a : allocations_) { std::free(a.ptr_); }
  }
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (++attempts_ != fail_on_) {
      ptr = std::malloc(size);
      if (nullptr != ptr) { allocations_.push_back({ptr, size}); }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      auto pos = std::find_if(allocations_.begin(), allocations_.end(),
          [ptr](const Allocation &a) { return a.ptr_ == ptr; });
      ASSERT_NE(allocations_.end(), pos);
      std::free(ptr);
      allocations_.erase(pos);
    }
  }
  int64_t attempts_;
  int64_t fail_on_;
  std::vector<Allocation> allocations_;
};

class TestDASIndexMergeBuildCleanup : public ::testing::Test
{
protected:
  TestDASIndexMergeBuildCleanup()
      : arena_("MergeCleanupUT"), exec_ctx_(arena_), eval_ctx_(exec_ctx_),
        scan_ctdef_(arena_), sort_ctdef_(arena_), merge_ctdef_(arena_),
        related_ids_(arena_), pd_op_(eval_ctx_, scan_ctdef_.pd_expr_spec_)
  {}
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    scan_ctdef_.ref_table_id_ = 100001;
    scan_ctdef_.index_merge_idx_ = 0;
    scan_rtdef_.ctdef_ = &scan_ctdef_;
    scan_rtdef_.eval_ctx_ = &eval_ctx_;
    scan_rtdef_.p_pd_expr_op_ = &pd_op_;
    merge_ctdef_.merge_type_ = INDEX_MERGE_INTERSECT;
    merge_ctdef_.children_ = ct_children_;
    merge_ctdef_.children_cnt_ = 3;
    merge_ctdef_.main_scan_ctdef_ = &scan_ctdef_;
    merge_rtdef_.ctdef_ = &merge_ctdef_;
    merge_rtdef_.eval_ctx_ = &eval_ctx_;
    merge_rtdef_.children_ = rt_children_;
    merge_rtdef_.children_cnt_ = 3;
    merge_rtdef_.main_scan_rtdef_ = &scan_rtdef_;
    sort_rtdef_.ctdef_ = &sort_ctdef_;
    sort_rtdef_.eval_ctx_ = &eval_ctx_;
    sort_ct_child_ = &scan_ctdef_;
    sort_rt_child_ = &scan_rtdef_;
    sort_ctdef_.children_ = &sort_ct_child_;
    sort_ctdef_.children_cnt_ = 1;
    sort_rtdef_.children_ = &sort_rt_child_;
    sort_rtdef_.children_cnt_ = 1;
    for (int64_t i = 0; i < 3; ++i) {
      ct_children_[i] = &scan_ctdef_;
      rt_children_[i] = &scan_rtdef_;
    }
    ASSERT_EQ(OB_SUCCESS, merge_ctdef_.merge_node_types_.init(3));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, merge_ctdef_.merge_node_types_.push_back(INDEX_MERGE_SCAN));
    }
    ASSERT_EQ(OB_SUCCESS, related_ids_.index_merge_tablet_ids_.init(1));
    ASSERT_EQ(OB_SUCCESS, related_ids_.index_merge_tablet_ids_.push_back(ObTabletID(1001)));
    related_ids_.lookup_tablet_id_ = ObTabletID(1002);
  }
  void TearDown() override
  {
    // pd_op_ is a fixture member, not the rtdef placement-new instance.
    scan_rtdef_.p_pd_expr_op_ = nullptr;
    EventItem reset_event;
    EXPECT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_DAS_INDEX_MERGE_BEFORE_ATTACH_FAILED", reset_event));
  }
  void check_validation_failure(int64_t child_position)
  {
    skip_child(1);
    EventItem failure;
    failure.error_code_ = OB_ALLOCATE_MEMORY_FAILED;
    failure.occur_ = 1;
    failure.cond_ = child_position;
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_DAS_INDEX_MERGE_BEFORE_ATTACH_FAILED", failure));
    ObDASIter *root = nullptr;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, build(root));
    EXPECT_EQ(nullptr, root);
    ASSERT_EQ(4, alloc_.allocations_.size()); // No children array allocated before validation.
    expect_all_nodes_released();
    auto *merge = static_cast<ObDASIndexMergeAndIter *>(
        static_cast<ObDASIter *>(alloc_.allocations_[3].ptr_));
    EXPECT_EQ(nullptr, merge->main_scan_iter_);
    EXPECT_EQ(nullptr, merge->main_scan_param_);
    EXPECT_EQ(nullptr, merge->mem_ctx_.ref_context());
  }
  void skip_child(int64_t idx)
  {
    ct_children_[idx] = &sort_ctdef_;
    rt_children_[idx] = &sort_rtdef_;
  }
  int build(ObDASIter *&result)
  {
    return ObDASIterUtils::create_index_merge_tree_common(scan_param_, alloc_, &merge_ctdef_,
        &merge_rtdef_, related_ids_, nullptr, &snapshot_, result);
  }
  void expect_all_nodes_released()
  {
    for (const auto &a : alloc_.allocations_) {
      if (a.size_ == sizeof(ObDASScanIter) || a.size_ == sizeof(ObDASIndexMergeAndIter)
          || a.size_ == sizeof(ObDASSortIter)) {
        EXPECT_FALSE(static_cast<ObDASIter *>(a.ptr_)->is_inited());
      }
    }
  }
  ObArenaAllocator arena_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASScanCtDef scan_ctdef_;
  ObDASScanRtDef scan_rtdef_;
  ObDASSortCtDef sort_ctdef_;
  ObDASSortRtDef sort_rtdef_;
  ObDASBaseCtDef *sort_ct_child_;
  ObDASBaseRtDef *sort_rt_child_;
  ObDASIndexMergeCtDef merge_ctdef_;
  ObDASIndexMergeRtDef merge_rtdef_;
  ObDASBaseCtDef *ct_children_[3];
  ObDASBaseRtDef *rt_children_[3];
  ObDASRelatedTabletID related_ids_;
  storage::ObTableScanParam scan_param_;
  transaction::ObTxReadSnapshot snapshot_;
  ObPushdownOperator pd_op_;
  MergeFailingAllocator alloc_;
};

TEST_F(TestDASIndexMergeBuildCleanup, skips_last_child)
{
  skip_child(2);
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build(root));
  auto *merge = static_cast<ObDASIndexMergeAndIter *>(root);
  ASSERT_EQ(2, merge->get_children_cnt());
  EXPECT_EQ(OB_SUCCESS, root->release());
  expect_all_nodes_released();
}

TEST_F(TestDASIndexMergeBuildCleanup, skips_all_but_first_child)
{
  skip_child(1);
  skip_child(2);
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build(root));
  auto *merge = static_cast<ObDASIndexMergeAndIter *>(root);
  ASSERT_EQ(1, merge->get_children_cnt());
  EXPECT_EQ(OB_SUCCESS, root->release());
  expect_all_nodes_released();
}

TEST_F(TestDASIndexMergeBuildCleanup, no_skipped_child)
{
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build(root));
  EXPECT_EQ(3, root->get_children_cnt());
  EXPECT_EQ(OB_SUCCESS, root->release());
  expect_all_nodes_released();
}

TEST_F(TestDASIndexMergeBuildCleanup, allocation_failures_release_constructed_nodes)
{
  skip_child(1);
  // Two retained scans, main scan, merge, and the final children array.
  for (int64_t failure = 1; failure <= 5; ++failure) {
    alloc_.fail_on_ = alloc_.attempts_ + failure;
    ObDASIter *root = nullptr;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, build(root));
    EXPECT_EQ(nullptr, root);
    expect_all_nodes_released();
  }
}

TEST_F(TestDASIndexMergeBuildCleanup, failure_validating_first_child_releases_unattached_trees)
{
  check_validation_failure(1);
}

TEST_F(TestDASIndexMergeBuildCleanup, failure_validating_last_child_releases_unattached_trees)
{
  check_validation_failure(2);
}

TEST_F(TestDASIndexMergeBuildCleanup, failed_initialization_preserves_main_scan_and_children)
{
  skip_child(1);
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build(root));
  auto *owner = static_cast<ObDASIndexMergeAndIter *>(root);
  ObDASIndexMergeIterParam param;
  param.max_size_ = 1;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.output_ = &merge_ctdef_.result_output_;
  param.merge_type_ = INDEX_MERGE_INTERSECT;
  param.ctdef_ = &merge_ctdef_;
  param.rtdef_ = &merge_rtdef_;
  param.rowkey_exprs_ = &merge_ctdef_.rowkey_exprs_;
  param.child_iters_ = &owner->child_iters_;
  param.child_scan_rtdefs_ = &owner->child_scan_rtdefs_;
  param.main_scan_iter_ = owner->main_scan_iter_;
  param.snapshot_ = nullptr; // Fail after allocating the candidate's main scan parameter.
  ObDASIndexMergeAndIter candidate;
  EXPECT_EQ(OB_ERR_UNEXPECTED, candidate.init(param));
  EXPECT_EQ(nullptr, candidate.main_scan_param_); // Published only after successful preparation.
  EXPECT_FALSE(candidate.is_init_complete());
  EXPECT_EQ(OB_SUCCESS, candidate.release());
  EXPECT_EQ(nullptr, candidate.main_scan_param_);
  EXPECT_EQ(nullptr, candidate.mem_ctx_.ref_context());
  EXPECT_TRUE(owner->main_scan_iter_->is_inited());
  for (int64_t i = 0; i < owner->child_iters_.count(); ++i) {
    EXPECT_TRUE(owner->child_iters_.at(i)->is_inited());
  }
  EXPECT_EQ(OB_SUCCESS, root->release());
  expect_all_nodes_released();
}

} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
