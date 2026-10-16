/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <cstdlib>
#include <gtest/gtest.h>
#include "sql/engine/sort/ob_sort_vec_op_store_row_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

// Track ownership without actually freeing until teardown, so an invalid free
// is reported as a test failure instead of hiding subsequent ownership checks.
class RowTestAllocator : public ObIAllocator
{
public:
  RowTestAllocator() : count_(0), fail_after_(-1), invalid_frees_(0) {}
  ~RowTestAllocator() override
  {
    for (int64_t i = 0; i < count_; ++i) {
      std::free(ptrs_[i]);
    }
  }
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (fail_after_ != 0 && count_ < 16) {
      if (fail_after_ > 0) {
        --fail_after_;
      }
      if (nullptr != (ptr = std::malloc(size))) {
        ptrs_[count_] = ptr;
        live_[count_++] = true;
      }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      int64_t i = 0;
      for (; i < count_ && ptrs_[i] != ptr; ++i) {}
      if (i == count_ || !live_[i]) {
        ++invalid_frees_;
      } else {
        live_[i] = false;
      }
    }
  }
  bool is_live(const void *ptr) const
  {
    bool found = false;
    for (int64_t i = 0; i < count_; ++i) {
      found |= ptrs_[i] == ptr && live_[i];
    }
    return found;
  }
  int64_t live_count() const
  {
    int64_t result = 0;
    for (int64_t i = 0; i < count_; ++i) {
      result += live_[i];
    }
    return result;
  }
  int64_t count_;
  int64_t fail_after_;
  int64_t invalid_frees_;
private:
  void *ptrs_[16];
  bool live_[16];
};

class SortVecRowFactoryTest : public ::testing::Test
{
protected:
  using Row = ObTopNSortKey<true>;
  SortVecRowFactoryTest()
      : profile_(ObSqlWorkAreaType::SORT_WORK_AREA), processor_(profile_), sk_meta_ptr_(&sk_meta_), addon_meta_ptr_(&addon_meta_),
        used_(0), topn_(16),
        factory_(allocator_, processor_, sk_meta_ptr_, addon_meta_ptr_, used_, topn_)
  {}
  Row *make_row(int64_t row_size, int64_t capacity, const RowMeta &meta)
  {
    void *buf = allocator_.alloc(capacity);
    Row *row = nullptr;
    if (nullptr != buf) {
      MEMSET(buf, 0, capacity);
      row = new (buf) Row();
      // Only row headers and sort-extra payload are needed by the copy path.
      reinterpret_cast<RowHeader *>(row)->row_size_ = row_size;
      row->set_max_size(capacity, meta);
      used_ += capacity;
      processor_.alloc(capacity);
    }
    return row;
  }
  void check_addon_failure(bool reuse_sort_buffer)
  {
    Row *src = make_row(64, 64, sk_meta_);
    Row *src_addon = make_row(128, 128, addon_meta_);
    Row *dst = make_row(32, reuse_sort_buffer ? 128 : 32, sk_meta_);
    Row *old_addon = make_row(32, 32, addon_meta_);
    ASSERT_NE(nullptr, src);
    ASSERT_NE(nullptr, src_addon);
    ASSERT_NE(nullptr, dst);
    ASSERT_NE(nullptr, old_addon);
    src->set_addon_ptr(src_addon, sk_meta_);
    dst->set_addon_ptr(old_addon, sk_meta_);
    Row *old_dst = dst;
    const int64_t before = used_;
    allocator_.fail_after_ = reuse_sort_buffer ? 0 : 1;

    ASSERT_NE(OB_SUCCESS, factory_.copy_sort_row(src, dst));
    EXPECT_EQ(old_dst, dst);
    EXPECT_EQ(old_addon, dst->get_addon_ptr(sk_meta_));
    EXPECT_EQ(src_addon, src->get_addon_ptr(sk_meta_));
    EXPECT_EQ(before, used_);
    EXPECT_EQ(4, allocator_.live_count());

    factory_.free_row_store(dst);
    EXPECT_TRUE(allocator_.is_live(src_addon));
    EXPECT_FALSE(allocator_.is_live(old_addon));
    factory_.free_row_store(src);
    EXPECT_EQ(0, allocator_.invalid_frees_);
    EXPECT_EQ(0, allocator_.live_count());
    EXPECT_EQ(0, used_);
  }
  RowTestAllocator allocator_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor processor_;
  RowMeta sk_meta_;
  RowMeta addon_meta_;
  const RowMeta *sk_meta_ptr_;
  const RowMeta *addon_meta_ptr_;
  int64_t used_;
  const int64_t topn_;
  ObSortVecOpStoreRowFactory<Row, true> factory_;
};

TEST_F(SortVecRowFactoryTest, ReusedSortBufferAddonAllocationFailure)
{
  check_addon_failure(true);
}

TEST_F(SortVecRowFactoryTest, NewSortBufferAddonAllocationFailure)
{
  check_addon_failure(false);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
