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
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_index_merge_fts_and_iter.h"
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

struct ResourceStats
{
  int live_ = 0;
  int releases_ = 0;
};

struct TestIterParam : ObDASIterParam
{
  TestIterParam() : ObDASIterParam(DAS_ITER_SCAN) {}
  bool is_valid() const override { return valid_; }
  ResourceStats *stats_ = nullptr;
  ObDASIter *input_ = nullptr;
  int init_error_ = OB_SUCCESS;
  int release_error_ = OB_SUCCESS;
  bool valid_ = true;
};

// An independent allocation makes a missed inner_release observable even when
// the allocator reclaims the iterator object's raw storage.
class ResourceIter : public ObDASIter
{
public:
  ResourceIter() : ObDASIter(DAS_ITER_SCAN) {}
  int do_table_scan() override { return OB_SUCCESS; }
  int rescan() override { return OB_SUCCESS; }
  void clear_evaluated_flag() override {}
protected:
  int inner_init(ObDASIterParam &param) override
  {
    auto &test_param = static_cast<TestIterParam &>(param);
    stats_ = test_param.stats_;
    input_ = test_param.input_;
    release_error_ = test_param.release_error_;
    resource_ = std::malloc(32);
    ++stats_->live_;
    return nullptr == resource_ ? OB_ALLOCATE_MEMORY_FAILED : test_param.init_error_;
  }
  int inner_release() override
  {
    int ret = release_input_iter(input_);
    input_ = nullptr;
    if (nullptr != resource_) {
      std::free(resource_);
      resource_ = nullptr;
      --stats_->live_;
    }
    if (nullptr != stats_) {
      ++stats_->releases_;
    }
    if (OB_SUCCESS == ret) {
      ret = release_error_;
    }
    return ret;
  }
  int inner_reuse() override { return OB_SUCCESS; }
  int inner_get_next_row() override { return OB_ITER_END; }
  int inner_get_next_rows(int64_t &, int64_t) override { return OB_ITER_END; }
private:
  ResourceStats *stats_ = nullptr;
  ObDASIter *input_ = nullptr;
  void *resource_ = nullptr;
  int release_error_ = OB_SUCCESS;
};

class DerivedFailIter : public ResourceIter
{
protected:
  int inner_init(ObDASIterParam &param) override
  {
    const int ret = ResourceIter::inner_init(param);
    return OB_SUCCESS == ret ? OB_ALLOCATE_MEMORY_FAILED : ret;
  }
};

class TestDASIterOwnership : public ::testing::Test
{
protected:
  TestDASIterOwnership()
      : context_alloc_("IterCleanupUT"), exec_ctx_(context_alloc_),
        eval_ctx_(exec_ctx_), output_(context_alloc_) {}
  TestIterParam param(ResourceStats &stats, ObDASIter *input = nullptr)
  {
    TestIterParam p;
    p.stats_ = &stats;
    p.input_ = input;
    p.eval_ctx_ = &eval_ctx_;
    p.exec_ctx_ = &exec_ctx_;
    p.output_ = &output_;
    p.max_size_ = 1;
    return p;
  }
  ObArenaAllocator context_alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ExprFixedArray output_;
  FailingAllocator alloc_;
};

TEST_F(TestDASIterOwnership, allocation_and_validation_failures_preserve_input)
{
  ResourceStats input_stats;
  ResourceIter input;
  auto input_param = param(input_stats);
  ASSERT_EQ(OB_SUCCESS, input.init(input_param));
  for (const bool fail_allocation : {true, false}) {
    ResourceStats parent_stats;
    auto p = param(parent_stats, &input);
    p.valid_ = fail_allocation;
    alloc_.fail_on_ = fail_allocation ? alloc_.attempts_ + 1 : 0;
    int ret = OB_SUCCESS;
    {
      ResourceIter *result = &input;
      ret = ObDASIterUtils::create_das_iter(alloc_, p, result);
      EXPECT_EQ(nullptr, result);
      EXPECT_EQ(fail_allocation ? OB_ALLOCATE_MEMORY_FAILED : OB_ERR_UNEXPECTED, ret);
    }
    EXPECT_EQ(1, input_stats.live_);
    EXPECT_EQ(0, input_stats.releases_);
    EXPECT_EQ(0, parent_stats.live_);
  }
  EXPECT_EQ(OB_SUCCESS, input.release());
}

TEST_F(TestDASIterOwnership, derived_init_failure_does_not_consume_input)
{
  ResourceStats input_stats;
  ResourceStats parent_stats;
  ResourceIter input;
  auto input_param = param(input_stats);
  ASSERT_EQ(OB_SUCCESS, input.init(input_param));
  int ret = OB_SUCCESS;
  {
    auto p = param(parent_stats, &input);
    DerivedFailIter *result = nullptr;
    ret = ObDASIterUtils::create_das_iter(alloc_, p, result);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
    EXPECT_EQ(nullptr, result);
  }
  EXPECT_EQ(0, parent_stats.live_);
  EXPECT_EQ(1, parent_stats.releases_);
  EXPECT_EQ(1, input_stats.live_);
  EXPECT_EQ(0, input_stats.releases_);
  EXPECT_EQ(OB_SUCCESS, input.release());
}

TEST_F(TestDASIterOwnership, partial_children_and_unattached_nodes_are_released_once)
{
  ResourceStats parent_stats, child_stats, orphan_stats;
  int ret = OB_SUCCESS;
  {
    ResourceIter *parent = nullptr;
    ObDASIterGuard parent_guard(parent, ret);
    ResourceIter *child = nullptr;
    ObDASIterGuard child_guard(child, ret);
    ResourceIter *orphan = nullptr;
    ObDASIterGuard orphan_guard(orphan, ret);
    auto pp = param(parent_stats);
    auto cp = param(child_stats);
    auto op = param(orphan_stats);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, pp, parent));
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_iter_children_array(3, alloc_, parent));
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, cp, child));
    parent->get_children()[0] = child;
    child_guard.dismiss();
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, op, orphan));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  for (const ResourceStats *stats : {&parent_stats, &child_stats, &orphan_stats}) {
    EXPECT_EQ(0, stats->live_);
    EXPECT_EQ(1, stats->releases_);
  }
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
}

TEST_F(TestDASIterOwnership, pointer_array_growth_failure_cleans_unregistered_child)
{
  ResourceStats first_stats, second_stats;
  FailingAllocator array_alloc;
  ObArray<ObDASIter *, ObWrapperAllocator> children(sizeof(void *), ObWrapperAllocator(array_alloc));
  int ret = OB_SUCCESS;
  {
    ObDASIterArrayGuard children_guard(children, ret);
    ResourceIter *first = nullptr;
    ObDASIterGuard first_guard(first, ret);
    ResourceIter *second = nullptr;
    ObDASIterGuard second_guard(second, ret);
    auto fp = param(first_stats);
    auto sp = param(second_stats);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, fp, first));
    ASSERT_EQ(OB_SUCCESS, children.push_back(first));
    first_guard.dismiss();
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, sp, second));
    array_alloc.fail_on_ = array_alloc.attempts_ + 1;
    ret = children.reserve(children.get_capacity() + 1);
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
    // second has been created but has not been recorded in the pointer array.
    EXPECT_EQ(1, children.count());
  }
  EXPECT_EQ(1, first_stats.releases_);
  EXPECT_EQ(1, second_stats.releases_);
  EXPECT_EQ(0, first_stats.live_ + second_stats.live_);
}

TEST_F(TestDASIterOwnership, profile_builder_failure_preserves_borrowed_tree)
{
  ResourceStats child_stats;
  ResourceIter child;
  auto cp = param(child_stats);
  ASSERT_EQ(OB_SUCCESS, child.init(cp));
  for (const int64_t allocation : {1, 2}) {
    FailingAllocator failing_alloc;
    failing_alloc.fail_on_ = allocation;
    int ret = OB_SUCCESS;
    {
      ObDASIter *profile = nullptr;
      ret = ObDASIterUtils::create_profile_iter(failing_alloc, ObTabletID(1001),
          &child, false, ObProfileId::HYBRID_SEARCH, profile);
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
      EXPECT_EQ(nullptr, profile);
    }
    EXPECT_TRUE(child.is_inited());
    EXPECT_EQ(0, child_stats.releases_);
  }
  EXPECT_EQ(OB_SUCCESS, child.release());
}

TEST_F(TestDASIterOwnership, successful_wrapper_transfers_input_before_later_failure)
{
  ResourceStats child_stats;
  ResourceIter child;
  auto cp = param(child_stats);
  ASSERT_EQ(OB_SUCCESS, child.init(cp));
  int ret = OB_SUCCESS;
  {
    ObDASIter *profile = nullptr;
    ObDASIterGuard profile_guard(profile, ret);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_profile_iter(alloc_, ObTabletID(1001),
        &child, false, ObProfileId::HYBRID_SEARCH, profile));
    ret = OB_ERR_UNEXPECTED;
  }
  EXPECT_FALSE(child.is_inited());
  EXPECT_EQ(1, child_stats.releases_);
  EXPECT_EQ(0, child_stats.live_);
}

TEST_F(TestDASIterOwnership, successful_publication_keeps_resources_until_owner_releases)
{
  ResourceStats child_stats, parent_stats;
  ResourceIter *parent = nullptr;
  int ret = OB_SUCCESS;
  {
    ObDASIterGuard parent_guard(parent, ret);
    ResourceIter *child = nullptr;
    ObDASIterGuard child_guard(child, ret);
    auto cp = param(child_stats);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, cp, child));
    auto pp = param(parent_stats, child);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, pp, parent));
    child_guard.dismiss();
  }
  EXPECT_EQ(1, child_stats.live_);
  EXPECT_EQ(1, parent_stats.live_);
  EXPECT_EQ(OB_SUCCESS, parent->release());
  EXPECT_EQ(1, child_stats.releases_);
  EXPECT_EQ(1, parent_stats.releases_);
  EXPECT_EQ(0, child_stats.live_ + parent_stats.live_);
}

TEST_F(TestDASIterOwnership, replacing_local_root_releases_the_entire_chain_once)
{
  ResourceStats child_stats;
  ResourceIter *child = nullptr;
  auto cp = param(child_stats);
  ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, cp, child));
  int ret = OB_SUCCESS;
  {
    ObDASIter *root = child;
    ObDASIterGuard root_guard(root, ret);
    for (int i = 0; i < 3; ++i) {
      ObDASIter *wrapper = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_profile_iter(alloc_, ObTabletID(1001),
          root, false, ObProfileId::HYBRID_SEARCH, wrapper));
      root = wrapper;
    }
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  EXPECT_EQ(1, child_stats.releases_);
  EXPECT_EQ(0, child_stats.live_);
}

TEST_F(TestDASIterOwnership, failed_wrapper_preserves_an_aliased_input_output)
{
  ResourceStats child_stats;
  ResourceIter child;
  auto cp = param(child_stats);
  ASSERT_EQ(OB_SUCCESS, child.init(cp));
  FailingAllocator failing_alloc;
  failing_alloc.fail_on_ = 2;
  ObDASIter *root = &child;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObDASIterUtils::create_profile_iter(failing_alloc,
      ObTabletID(1001), root, false, ObProfileId::HYBRID_SEARCH, root));
  EXPECT_EQ(&child, root);
  EXPECT_TRUE(child.is_inited());
  EXPECT_EQ(0, child_stats.releases_);
  EXPECT_EQ(OB_SUCCESS, root->release());
}

TEST_F(TestDASIterOwnership, partial_array_handoff_has_one_owner_per_child)
{
  ResourceStats parent_stats, first_stats, second_stats;
  int ret = OB_SUCCESS;
  {
    ResourceIter *parent = nullptr;
    ObDASIterGuard parent_guard(parent, ret);
    ObSEArray<ObDASIter *, 2> pending;
    ObDASIterArrayGuard pending_guard(pending, ret);
    auto pp = param(parent_stats);
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, pp, parent));
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_iter_children_array(2, alloc_, parent));
    for (ResourceStats *stats : {&first_stats, &second_stats}) {
      ResourceIter *child = nullptr;
      auto cp = param(*stats);
      ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_das_iter(alloc_, cp, child));
      ASSERT_EQ(OB_SUCCESS, pending.push_back(child));
    }
    parent->get_children()[0] = pending[0];
    pending[0] = nullptr;
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  for (ResourceStats *stats : {&parent_stats, &first_stats, &second_stats}) {
    EXPECT_EQ(1, stats->releases_);
    EXPECT_EQ(0, stats->live_);
  }
}

TEST_F(TestDASIterOwnership, failed_func_data_cleanup_preserves_the_callers_array)
{
  ResourceStats child_stats;
  ResourceIter child;
  auto cp = param(child_stats);
  ASSERT_EQ(OB_SUCCESS, child.init(cp));
  ObDASIter *inputs[] = {&child};
  ObDASFuncDataIter parent;
  parent.tr_merge_iters_ = inputs;
  parent.iter_count_ = 1;
  // Reproduce cleanup after inner_init saved the input array and then failed.
  EXPECT_EQ(OB_SUCCESS, parent.release());
  EXPECT_EQ(&child, inputs[0]);
  EXPECT_EQ(0, child_stats.releases_);
  EXPECT_EQ(OB_SUCCESS, child.release());
  EXPECT_EQ(1, child_stats.releases_);
}

TEST_F(TestDASIterOwnership, published_tree_still_reports_invalid_null_child)
{
  ResourceStats stats;
  ResourceIter iter;
  auto p = param(stats);
  ASSERT_EQ(OB_SUCCESS, iter.init(p));
  ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_iter_children_array(1, alloc_, &iter));
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.release());
  EXPECT_EQ(1, stats.releases_);
  EXPECT_EQ(0, stats.live_);
}


} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
