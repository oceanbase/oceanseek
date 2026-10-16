/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdlib>
#include <vector>
#define private public
#define protected public
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/das/ob_das_scan_op.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
class FailingAllocator : public ObIAllocator
{
public:
  FailingAllocator() : attempts_(0), fail_on_(0) {}
  ~FailingAllocator() override
  {
    for (void *ptr : allocations_) {
      std::free(ptr);
    }
  }
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (++attempts_ != fail_on_) {
      ptr = std::malloc(size);
      if (nullptr != ptr) {
        allocations_.push_back(ptr);
      }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      const auto pos = std::find(allocations_.begin(), allocations_.end(), ptr);
      EXPECT_NE(allocations_.end(), pos);
      if (allocations_.end() != pos) {
        std::free(ptr);
        allocations_.erase(pos);
      }
    }
  }
  int64_t attempts_;
  int64_t fail_on_;
  std::vector<void *> allocations_;
};

// Count every release, including repeated calls, without hiding them behind reset.
class CountingIter : public ObDASIter
{
public:
  CountingIter() : ObDASIter(DAS_ITER_SCAN), releases_(0) {}
  int do_table_scan() override { return OB_SUCCESS; }
  int rescan() override { return OB_SUCCESS; }
  void clear_evaluated_flag() override {}
  int releases_;
protected:
  int inner_init(ObDASIterParam &) override { return OB_SUCCESS; }
  int inner_release() override { ++releases_; return OB_SUCCESS; }
  int inner_reuse() override { return OB_SUCCESS; }
  int inner_get_next_row() override { return OB_ITER_END; }
  int inner_get_next_rows(int64_t &, int64_t) override { return OB_ITER_END; }
};

class TestDASIterBuildCleanup : public ::testing::Test
{
protected:
  TestDASIterBuildCleanup()
      : context_alloc_("IterCleanupUT"), exec_ctx_(context_alloc_), eval_ctx_(exec_ctx_),
        output_(context_alloc_), scan_ctdef_(context_alloc_), lookup_ctdef_(context_alloc_),
        related_ids_(context_alloc_)
  {}
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    scan_ctdef_.ref_table_id_ = 100001;
    scan_rtdef_.eval_ctx_ = &eval_ctx_;
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.rowkey_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef_.rowkey_exprs_.push_back(nullptr));
    ASSERT_EQ(OB_SUCCESS, lookup_ctdef_.index_scan_proj_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, lookup_ctdef_.index_scan_proj_exprs_.push_back(nullptr));
    ct_children_[0] = &scan_ctdef_;
    ct_children_[1] = &scan_ctdef_;
    rt_children_[0] = &scan_rtdef_;
    rt_children_[1] = &scan_rtdef_;
    lookup_ctdef_.children_ = ct_children_;
    lookup_ctdef_.children_cnt_ = 2;
    lookup_rtdef_.children_ = rt_children_;
    lookup_rtdef_.children_cnt_ = 2;
    related_ids_.lookup_tablet_id_ = ObTabletID(1001);
    ObDASIterParam param(DAS_ITER_SCAN);
    param.eval_ctx_ = &eval_ctx_;
    param.exec_ctx_ = &exec_ctx_;
    param.output_ = &output_;
    ASSERT_EQ(OB_SUCCESS, input_.init(param));
  }
  void TearDown() override
  {
    if (input_.is_inited()) {
      EXPECT_EQ(OB_SUCCESS, input_.release());
    }
    EXPECT_EQ(1, input_.releases_);
  }
  int build_lookup(bool cache, ObDASIter *&result)
  {
    int ret = OB_SUCCESS;
    if (cache) {
      ObDASCacheLookupIter *lookup = nullptr;
      ret = ObDASIterUtils::create_cache_lookup_sub_tree(scan_param_, alloc_,
          &lookup_ctdef_, &lookup_rtdef_, nullptr, nullptr, &input_, related_ids_, lookup, true, 4);
      result = lookup;
    } else {
      ObDASLocalLookupIter *lookup = nullptr;
      ret = ObDASIterUtils::create_local_lookup_sub_tree(scan_param_, alloc_,
          &scan_ctdef_, &scan_rtdef_, &scan_ctdef_, &scan_rtdef_, nullptr, nullptr,
          &input_, ObTabletID(1001), lookup, true, 4);
      result = lookup;
    }
    return ret;
  }
  void check_failed_lookup_allocations(bool cache)
  {
    // The real builder allocates its scan, lookup and children array in order.
    for (int64_t failure = 1; failure <= 3; ++failure) {
      alloc_.fail_on_ = alloc_.attempts_ + failure;
      const size_t old_count = alloc_.allocations_.size();
      ObDASIter *result = nullptr;
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, build_lookup(cache, result));
      EXPECT_EQ(nullptr, result);
      EXPECT_EQ(0, input_.releases_);
      EXPECT_TRUE(input_.is_inited());
      // Successful object allocations live in the query allocator, but their
      // release() must already have run before the builder returns failure.
      ASSERT_EQ(old_count + failure - 1, alloc_.allocations_.size());
      for (size_t i = old_count; i < alloc_.allocations_.size(); ++i) {
        EXPECT_FALSE(static_cast<ObDASIter *>(alloc_.allocations_[i])->is_inited());
      }
      if (failure == 3) {
        auto *lookup = static_cast<ObDASLookupIter *>(
            static_cast<ObDASIter *>(alloc_.allocations_.back()));
        EXPECT_EQ(nullptr, lookup->lookup_memctx_);
        if (cache) {
          EXPECT_EQ(nullptr, static_cast<ObDASCacheLookupIter *>(lookup)->index_proj_rows_.store_rows_);
        }
      }
    }
  }
  ObArenaAllocator context_alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray output_;
  ObDASScanCtDef scan_ctdef_;
  ObDASScanRtDef scan_rtdef_;
  ObDASIndexProjLookupCtDef lookup_ctdef_;
  ObDASIndexProjLookupRtDef lookup_rtdef_;
  ObDASBaseCtDef *ct_children_[2];
  ObDASBaseRtDef *rt_children_[2];
  ObDASRelatedTabletID related_ids_;
  storage::ObTableScanParam scan_param_;
  CountingIter input_;
  FailingAllocator alloc_;
};

TEST_F(TestDASIterBuildCleanup, local_lookup_allocation_failures_preserve_input)
{
  check_failed_lookup_allocations(false);
}

TEST_F(TestDASIterBuildCleanup, cache_lookup_allocation_failures_preserve_input)
{
  check_failed_lookup_allocations(true);
}

TEST_F(TestDASIterBuildCleanup, failed_lookup_initialization_preserves_both_inputs)
{
  CountingIter data;
  ObDASCacheLookupIterParam param;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.output_ = &output_;
  param.index_ctdef_ = &scan_ctdef_;
  param.lookup_ctdef_ = &scan_ctdef_;
  param.index_rtdef_ = &scan_rtdef_;
  param.lookup_rtdef_ = &scan_rtdef_;
  param.rowkey_exprs_ = &scan_ctdef_.rowkey_exprs_;
  param.index_table_iter_ = &input_;
  param.data_table_iter_ = &data;
  // Lookup's base init saves inputs and allocates its context, then local init
  // rejects this type. Exercise the existing factory's real failure cleanup.
  param.type_ = DAS_ITER_GLOBAL_LOOKUP;
  ObDASCacheLookupIter *result = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASIterUtils::create_das_iter(alloc_, param, result));
  EXPECT_EQ(nullptr, result);
  EXPECT_EQ(0, input_.releases_);
  EXPECT_EQ(0, data.releases_);
  EXPECT_TRUE(alloc_.allocations_.empty());
}

TEST_F(TestDASIterBuildCleanup, successful_lookup_releases_input_only_when_owner_closes)
{
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_lookup(false, root));
  EXPECT_EQ(0, input_.releases_);
  EXPECT_EQ(OB_SUCCESS, root->release());
  EXPECT_EQ(1, input_.releases_);
}

TEST_F(TestDASIterBuildCleanup, successful_cache_lookup_releases_input_only_when_owner_closes)
{
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_lookup(true, root));
  EXPECT_EQ(0, input_.releases_);
  EXPECT_EQ(OB_SUCCESS, root->release());
  EXPECT_EQ(1, input_.releases_);
}

TEST_F(TestDASIterBuildCleanup, profile_allocation_failures_preserve_input)
{
  for (int64_t failure = 1; failure <= 2; ++failure) {
    alloc_.fail_on_ = alloc_.attempts_ + failure;
    ObDASIter *result = nullptr;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_profile_iter(alloc_,
        ObTabletID(1001), &input_, false, ObProfileId::HYBRID_SEARCH, result));
    EXPECT_EQ(nullptr, result);
    EXPECT_EQ(0, input_.releases_);
    if (failure == 2) {
      EXPECT_FALSE(static_cast<ObDASIter *>(alloc_.allocations_.back())->is_inited());
    }
  }
}

TEST_F(TestDASIterBuildCleanup, failed_profile_keeps_aliased_input_output)
{
  ObDASIter *root = &input_;
  alloc_.fail_on_ = 2;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_profile_iter(alloc_,
      ObTabletID(1001), root, false, ObProfileId::HYBRID_SEARCH, root));
  EXPECT_EQ(&input_, root);
  EXPECT_EQ(0, input_.releases_);
}

TEST_F(TestDASIterBuildCleanup, failed_profile_after_lookup_keeps_complete_root)
{
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_lookup(true, root));
  alloc_.fail_on_ = alloc_.attempts_ + 2;
  ObDASIter *profile = nullptr;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_profile_iter(alloc_,
      ObTabletID(1001), root, false, ObProfileId::HYBRID_SEARCH, profile));
  EXPECT_EQ(nullptr, profile);
  EXPECT_EQ(0, input_.releases_);
  EXPECT_EQ(OB_SUCCESS, root->release());
  EXPECT_EQ(1, input_.releases_);
}

TEST_F(TestDASIterBuildCleanup, completed_lookup_profile_chain_releases_input_once)
{
  ObDASIter *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_lookup(true, root));
  ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_profile_iter(alloc_,
      ObTabletID(1001), root, false, ObProfileId::HYBRID_SEARCH, root));
  EXPECT_EQ(0, input_.releases_);
  EXPECT_EQ(OB_SUCCESS, root->release());
  EXPECT_EQ(1, input_.releases_);
}


} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
