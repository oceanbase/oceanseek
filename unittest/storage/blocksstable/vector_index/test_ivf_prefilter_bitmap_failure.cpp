/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <gtest/gtest.h>
#include <new>
#include "lib/allocator/ob_malloc.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace blocksstable;

// CRoaring hooks are process-global. Keep fault injection in a dedicated,
// single-threaded executable, without a tenant environment or background tasks.
// Like ObRbMemMgr, the hook throws bad_alloc; returning NULL is not its contract.
class ObRoaringFailureMemory
{
public:
  ObRoaringFailureMemory() : allocations_(0), fail_on_(0), live_(0) {}
  static ObRoaringFailureMemory &instance()
  {
    static ObRoaringFailureMemory memory;
    return memory;
  }
  static void *alloc(size_t size)
  {
    ObRoaringFailureMemory &memory = instance();
    if (++memory.allocations_ == memory.fail_on_) {
      throw std::bad_alloc();
    }
    size_t *ptr = static_cast<size_t *>(ob_malloc(size + sizeof(size_t), "IVFRbFailTest"));
    if (nullptr == ptr) {
      throw std::bad_alloc();
    }
    *ptr = size;
    ++memory.live_;
    return ptr + 1;
  }
  static void release(void *ptr)
  {
    if (nullptr != ptr) {
      --instance().live_;
      ob_free(static_cast<size_t *>(ptr) - 1);
    }
  }
  static void *reallocate(void *ptr, size_t size)
  {
    void *next = nullptr;
    if (0 == size) {
      release(ptr);
    } else {
      next = alloc(size);
      if (nullptr != ptr) {
        const size_t old_size = *(static_cast<size_t *>(ptr) - 1);
        MEMCPY(next, ptr, MIN(old_size, size));
        release(ptr);
      }
    }
    return next;
  }
  static void *zero_alloc(size_t count, size_t size)
  {
    void *ptr = alloc(count * size);
    MEMSET(ptr, 0, count * size);
    return ptr;
  }
  static void *aligned_alloc(size_t alignment, size_t size)
  {
    void *raw = alloc(size + alignment - 1 + sizeof(void *));
    const uintptr_t address = (reinterpret_cast<uintptr_t>(raw) + sizeof(void *)
        + alignment - 1) & ~(alignment - 1);
    void **ptr = reinterpret_cast<void **>(address);
    ptr[-1] = raw;
    return ptr;
  }
  static void aligned_release(void *ptr)
  {
    if (nullptr != ptr) {
      release(static_cast<void **>(ptr)[-1]);
    }
  }
  void arm(int64_t fail_on) { allocations_ = 0; fail_on_ = fail_on; }
  int64_t allocations_;
  int64_t fail_on_;
  int64_t live_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRoaringFailureMemory);
};

class TestIvfPreFilterFailure : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObRoaringFailureMemory::instance().arm(0);
    ASSERT_EQ(0, ObRoaringFailureMemory::instance().live_);
  }
  void TearDown() override
  {
    ObRoaringFailureMemory::instance().arm(0);
    EXPECT_EQ(0, ObRoaringFailureMemory::instance().live_);
  }
  void verify_conversion_failure(bool compact, int64_t fail_on)
  {
    ObArenaAllocator allocator("IVFRbFailTest");
    ObIvfPreFilterBitmap bitmap(allocator);
    const int64_t count = compact ? 64 : MAX_BITMAP_SET_VALUES;
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i * 65536));
    }
    ASSERT_EQ(compact, bitmap.is_compact());
    ObRoaringFailureMemory::instance().arm(fail_on);
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, bitmap.value_add(ObIvfPreFilterBitmap::MAX_COMPACT_BITS));
    EXPECT_EQ(fail_on, ObRoaringFailureMemory::instance().allocations_);
    EXPECT_EQ(0, ObRoaringFailureMemory::instance().live_);
    EXPECT_FALSE(bitmap.is_valid());
    EXPECT_EQ(compact, bitmap.is_compact());
    EXPECT_FALSE(bitmap.is_roaring());
    EXPECT_EQ(count, bitmap.get_cardinality());
    // Failure closes the public view, but must preserve all old storage until reset.
    for (int64_t i = 0; i < count; ++i) {
      const uint64_t vid = i * 65536;
      if (compact) {
        const uint64_t offset = vid - bitmap.get_base_vid();
        EXPECT_NE(0, bitmap.get_words()[offset / 64] & (uint64_t(1) << (offset % 64)));
      } else {
        EXPECT_TRUE(bitmap.get_roaring()->is_contains(vid));
      }
    }
    ObIvfPreFilter filter;
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init(bitmap));
    EXPECT_FALSE(bitmap.is_contains(0));
    EXPECT_EQ(OB_STATE_NOT_MATCH, bitmap.value_add(1));
    bitmap.reset();
    ObRoaringFailureMemory::instance().arm(0);
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(7));
    ASSERT_TRUE(bitmap.is_contains(7));
    ASSERT_EQ(1, bitmap.get_cardinality());
  }
};

TEST_F(TestIvfPreFilterFailure, roaring_initialization_failure_preserves_small_and_compact_sets)
{
  // The query allocator succeeds; this fails inside convert_to_bitmap().
  verify_conversion_failure(false, 1);
  verify_conversion_failure(true, 1);
}

TEST_F(TestIvfPreFilterFailure, roaring_copy_failure_after_existing_members_is_transactional)
{
  ObArenaAllocator allocator("IVFRbFailTest");
  ObRoaringBitmap probe(&allocator);
  ASSERT_EQ(OB_SUCCESS, probe.convert_to_bitmap());
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(OB_SUCCESS, probe.value_add(i * 65536));
  }
  // Reproduce the same ascending copy, then fail the next allocation. This
  // proves the failure occurs after 16 old members, rather than at wrapper creation.
  const int64_t fail_on = ObRoaringFailureMemory::instance().allocations_ + 1;
  probe.set_empty();
  ASSERT_GT(fail_on, 1);
  verify_conversion_failure(false, fail_on);
  verify_conversion_failure(true, fail_on);
}

TEST_F(TestIvfPreFilterFailure, roaring_iterator_failure_during_promotion_preserves_members)
{
  ObArenaAllocator allocator("IVFRbFailTest");
  ObIvfPreFilterBitmap bitmap(allocator, 0);
  uint64_t vids[64];
  for (uint64_t i = 0; i < 64; ++i) {
    vids[i] = i * 128;
  }
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(vids, 64));
  ASSERT_TRUE(bitmap.is_roaring());
  const int64_t live_before = ObRoaringFailureMemory::instance().live_;
  ObRoaringFailureMemory::instance().arm(1);
  // The compact buffer uses the query allocator; this fails the CRoaring
  // iterator allocation while copying members into that buffer.
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, bitmap.finish());
  EXPECT_EQ(live_before, ObRoaringFailureMemory::instance().live_);
  EXPECT_FALSE(bitmap.is_valid());
  EXPECT_TRUE(bitmap.is_roaring());
  EXPECT_EQ(64, bitmap.get_cardinality());
  for (uint64_t i = 0; i < 64; ++i) {
    EXPECT_TRUE(bitmap.get_roaring()->is_contains(vids[i]));
  }
  ObRoaringFailureMemory::instance().arm(0);
  bitmap.reset();
  EXPECT_EQ(0, ObRoaringFailureMemory::instance().live_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using oceanbase::unittest::ObRoaringFailureMemory;
  testing::InitGoogleTest(&argc, argv);
  roaring_memory_t hooks;
  hooks.malloc = ObRoaringFailureMemory::alloc;
  hooks.realloc = ObRoaringFailureMemory::reallocate;
  hooks.calloc = ObRoaringFailureMemory::zero_alloc;
  hooks.free = ObRoaringFailureMemory::release;
  hooks.aligned_malloc = ObRoaringFailureMemory::aligned_alloc;
  hooks.aligned_free = ObRoaringFailureMemory::aligned_release;
  roaring_init_memory_hook(hooks);
  return RUN_ALL_TESTS();
}
