/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <vector>

#define private public
#define protected public

#define USING_LOG_PREFIX STORAGE

#include "lib/allocator/page_arena.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/ob_errno.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "share/config/ob_server_config.h"
#include "storage/async_index/ob_deletion_bitmap_cache.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{

class TestDeletionBitmapCache : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
  }
  virtual void TearDown() override
  {
    allocator_.reset();
  }
protected:
  ObArenaAllocator allocator_;
};

// ==================== CacheKey Tests ====================

TEST_F(TestDeletionBitmapCache, key_basic)
{
  ObDeletionBitmapCacheKey key1(1001, ObTabletID(100), 12345);
  EXPECT_TRUE(key1.is_valid());
  EXPECT_EQ(1001, key1.get_tenant_id());
  EXPECT_EQ(static_cast<int64_t>(sizeof(ObDeletionBitmapCacheKey)), key1.size());
}

TEST_F(TestDeletionBitmapCache, key_invalid)
{
  ObDeletionBitmapCacheKey key_default;
  EXPECT_FALSE(key_default.is_valid());

  ObDeletionBitmapCacheKey key_bad_tenant(OB_INVALID_TENANT_ID, ObTabletID(100), 12345);
  EXPECT_FALSE(key_bad_tenant.is_valid());

  ObDeletionBitmapCacheKey key_bad_snapshot(1001, ObTabletID(100), 0);
  EXPECT_FALSE(key_bad_snapshot.is_valid());
}

TEST_F(TestDeletionBitmapCache, key_equal)
{
  ObDeletionBitmapCacheKey key1(1001, ObTabletID(100), 12345);
  ObDeletionBitmapCacheKey key2(1001, ObTabletID(100), 12345);
  ObDeletionBitmapCacheKey key3(1001, ObTabletID(100), 99999);
  ObDeletionBitmapCacheKey key4(1001, ObTabletID(200), 12345);

  bool is_equal = false;
  EXPECT_EQ(OB_SUCCESS, key1.equal(key2, is_equal));
  EXPECT_TRUE(is_equal);

  EXPECT_EQ(OB_SUCCESS, key1.equal(key3, is_equal));
  EXPECT_FALSE(is_equal);

  EXPECT_EQ(OB_SUCCESS, key1.equal(key4, is_equal));
  EXPECT_FALSE(is_equal);
}

TEST_F(TestDeletionBitmapCache, key_hash)
{
  ObDeletionBitmapCacheKey key1(1001, ObTabletID(100), 12345);
  ObDeletionBitmapCacheKey key2(1001, ObTabletID(100), 12345);
  ObDeletionBitmapCacheKey key3(1001, ObTabletID(200), 12345);

  uint64_t h1 = 0, h2 = 0, h3 = 0;
  EXPECT_EQ(OB_SUCCESS, key1.hash(h1));
  EXPECT_EQ(OB_SUCCESS, key2.hash(h2));
  EXPECT_EQ(OB_SUCCESS, key3.hash(h3));

  EXPECT_EQ(h1, h2);
  EXPECT_NE(h1, h3);
}

TEST_F(TestDeletionBitmapCache, key_deep_copy)
{
  ObDeletionBitmapCacheKey key1(1001, ObTabletID(100), 12345);
  char buf[sizeof(ObDeletionBitmapCacheKey)];
  common::ObIKVCacheKey *copied_key = nullptr;

  EXPECT_EQ(OB_SUCCESS, key1.deep_copy(buf, sizeof(buf), copied_key));
  EXPECT_NE(nullptr, copied_key);

  bool is_equal = false;
  EXPECT_EQ(OB_SUCCESS, key1.equal(*copied_key, is_equal));
  EXPECT_TRUE(is_equal);

  // buf too small
  char small_buf[1];
  common::ObIKVCacheKey *bad_key = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.deep_copy(small_buf, 1, bad_key));
}

// ==================== CacheValue Tests ====================

TEST_F(TestDeletionBitmapCache, value_empty)
{
  ObDeletionBitmapCacheValue value;
  ObString empty_bin;
  EXPECT_EQ(OB_SUCCESS, value.init(empty_bin, 0));
  EXPECT_TRUE(value.is_valid());
  EXPECT_TRUE(value.is_empty());
  EXPECT_EQ(0, value.get_deletion_count());
}

TEST_F(TestDeletionBitmapCache, value_invalid_argument)
{
  ObDeletionBitmapCacheValue value;
  ObString empty_bin;
  EXPECT_EQ(OB_INVALID_ARGUMENT, value.init(empty_bin, -1));
}

TEST_F(TestDeletionBitmapCache, value_with_bitmap)
{
  // Build a roaring bitmap and serialize it
  common::ObRoaringBitmap bm(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm.value_add(10));
  EXPECT_EQ(OB_SUCCESS, bm.value_add(20));
  EXPECT_EQ(OB_SUCCESS, bm.value_add(30));

  common::ObStringBuffer buf(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm.serialize(buf));
  ObString bitmap_bin(buf.length(), buf.ptr());

  ObDeletionBitmapCacheValue value;
  EXPECT_EQ(OB_SUCCESS, value.init(bitmap_bin, 3));
  EXPECT_TRUE(value.is_valid());
  EXPECT_FALSE(value.is_empty());
  EXPECT_EQ(3, value.get_deletion_count());
  EXPECT_EQ(bitmap_bin.length(), value.get_bitmap_bin().length());
}

TEST_F(TestDeletionBitmapCache, value_deep_copy)
{
  common::ObRoaringBitmap bm(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm.value_add(5));
  EXPECT_EQ(OB_SUCCESS, bm.value_add(15));

  common::ObStringBuffer buf(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm.serialize(buf));
  ObString bitmap_bin(buf.length(), buf.ptr());

  ObDeletionBitmapCacheValue value;
  EXPECT_EQ(OB_SUCCESS, value.init(bitmap_bin, 2));

  const int64_t need_size = value.size();
  char *copy_buf = static_cast<char*>(allocator_.alloc(need_size));
  ASSERT_NE(nullptr, copy_buf);

  common::ObIKVCacheValue *copied_value = nullptr;
  EXPECT_EQ(OB_SUCCESS, value.deep_copy(copy_buf, need_size, copied_value));
  ASSERT_NE(nullptr, copied_value);

  const ObDeletionBitmapCacheValue *typed = static_cast<const ObDeletionBitmapCacheValue*>(copied_value);
  EXPECT_EQ(2, typed->get_deletion_count());
  EXPECT_EQ(bitmap_bin.length(), typed->get_bitmap_bin().length());
  EXPECT_EQ(0, MEMCMP(bitmap_bin.ptr(), typed->get_bitmap_bin().ptr(), bitmap_bin.length()));
}

// ==================== BitmapHandle Tests ====================

TEST_F(TestDeletionBitmapCache, handle_init_from_bitmap)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);
  EXPECT_EQ(OB_SUCCESS, bm->value_add(1));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(100));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(1000));

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));
  EXPECT_TRUE(handle.is_valid());
  EXPECT_FALSE(handle.is_empty());
  EXPECT_EQ(3, handle.get_deletion_count());
}

TEST_F(TestDeletionBitmapCache, handle_contains)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);
  EXPECT_EQ(OB_SUCCESS, bm->value_add(10));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(20));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(30));

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));

  EXPECT_TRUE(handle.contains(10));
  EXPECT_TRUE(handle.contains(20));
  EXPECT_TRUE(handle.contains(30));
  EXPECT_FALSE(handle.contains(5));
  EXPECT_FALSE(handle.contains(15));
  EXPECT_FALSE(handle.contains(50));
}

TEST_F(TestDeletionBitmapCache, handle_contains_any_in_range)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);
  EXPECT_EQ(OB_SUCCESS, bm->value_add(10));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(20));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(30));

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));

  EXPECT_TRUE(handle.contains_any_in_range(5, 15));
  EXPECT_TRUE(handle.contains_any_in_range(10, 10));
  EXPECT_TRUE(handle.contains_any_in_range(25, 35));
  EXPECT_FALSE(handle.contains_any_in_range(11, 19));
  EXPECT_FALSE(handle.contains_any_in_range(31, 100));
}

TEST_F(TestDeletionBitmapCache, handle_iter)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);
  EXPECT_EQ(OB_SUCCESS, bm->value_add(5));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(10));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(15));
  EXPECT_EQ(OB_SUCCESS, bm->value_add(20));

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));
  EXPECT_EQ(OB_SUCCESS, handle.open_iter());

  // Iterate and verify order
  std::vector<int64_t> results;
  int64_t doc_id = 0;
  int ret = handle.advance_to(0, doc_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  results.push_back(doc_id);
  while (OB_SUCC(ret = handle.get_next(doc_id))) {
    results.push_back(doc_id);
  }
  EXPECT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(4u, results.size());
  EXPECT_EQ(5, results[0]);
  EXPECT_EQ(10, results[1]);
  EXPECT_EQ(15, results[2]);
  EXPECT_EQ(20, results[3]);
}

TEST_F(TestDeletionBitmapCache, handle_empty_bitmap)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));
  EXPECT_TRUE(handle.is_valid());
  EXPECT_TRUE(handle.is_empty());
  EXPECT_EQ(0, handle.get_deletion_count());
  EXPECT_FALSE(handle.contains(1));
  EXPECT_FALSE(handle.contains_any_in_range(0, 100));
}

TEST_F(TestDeletionBitmapCache, handle_reset)
{
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bm);
  EXPECT_EQ(OB_SUCCESS, bm->value_add(42));

  ObDeletionBitmapHandle handle;
  EXPECT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bm));
  EXPECT_TRUE(handle.is_valid());

  handle.reset();
  EXPECT_FALSE(handle.is_valid());
  EXPECT_TRUE(handle.is_empty());
}

TEST_F(TestDeletionBitmapCache, handle_reset_returns_hazard_pointer_without_destructor)
{
  common::HazptrTLCache &cache = common::HazptrTLCache::get_instance();
  void *buf = allocator_.alloc(sizeof(ObDeletionBitmapHandle));
  ObDeletionBitmapHandle *handle = nullptr;
  common::HazardPointer *hazptr = nullptr;
  common::HazardPointer *reused_hazptr = nullptr;
  ASSERT_TRUE(GCONF._enable_kvcache_hazard_pointer);
  ASSERT_NE(nullptr, buf);
  cache.flush();
  for (int64_t i = 0; i < 1024; ++i) {
    // Simulate arena reuse after DAS release(), which does not call the destructor.
    handle = new (buf) ObDeletionBitmapHandle();
    ASSERT_EQ(OB_SUCCESS, cache.acquire_hazptr(hazptr));
    handle->cache_handle_.handle_.hazptr_holder_.hazptr_ = hazptr;
    cache.flush();
    // A cleared protection is not proof that the handle owns no pool resource.
    EXPECT_FALSE(handle->cache_handle_.handle_.is_valid());
    handle->reset();
    EXPECT_EQ(nullptr, handle->cache_handle_.handle_.hazptr_holder_.hazptr_);
    handle->reset();
    ASSERT_EQ(OB_SUCCESS, cache.acquire_hazptr(reused_hazptr));
    EXPECT_EQ(hazptr, reused_hazptr);
    cache.release_hazptr(reused_hazptr);
  }
  handle->~ObDeletionBitmapHandle();
  cache.flush();
}

TEST_F(TestDeletionBitmapCache, handle_reset_releases_bitmap_iterator_and_hazard_pointer)
{
  common::HazptrTLCache &cache = common::HazptrTLCache::get_instance();
  ObDeletionBitmapHandle handle;
  common::ObRoaringBitmap *bitmap = nullptr;
  common::HazardPointer *hazptr = nullptr;
  common::HazardPointer *reused_hazptr = nullptr;
  ASSERT_TRUE(GCONF._enable_kvcache_hazard_pointer);
  bitmap = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bitmap);
  ASSERT_EQ(OB_SUCCESS, bitmap->value_add(42));
  ASSERT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bitmap));
  ASSERT_EQ(OB_SUCCESS, handle.open_iter());
  ASSERT_NE(nullptr, handle.iter_);
  cache.flush();
  ASSERT_EQ(OB_SUCCESS, cache.acquire_hazptr(hazptr));
  handle.cache_handle_.handle_.hazptr_holder_.hazptr_ = hazptr;
  cache.flush();
  handle.reset();
  EXPECT_FALSE(handle.is_valid());
  EXPECT_EQ(nullptr, handle.bitmap_);
  EXPECT_EQ(nullptr, handle.iter_);
  EXPECT_EQ(nullptr, handle.cache_handle_.handle_.hazptr_holder_.hazptr_);
  ASSERT_EQ(OB_SUCCESS, cache.acquire_hazptr(reused_hazptr));
  EXPECT_EQ(hazptr, reused_hazptr);
  cache.release_hazptr(reused_hazptr);
  // A reset handle can be initialized again and safely destroyed later.
  bitmap = OB_NEWx(common::ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bitmap);
  ASSERT_EQ(OB_SUCCESS, bitmap->value_add(84));
  ASSERT_EQ(OB_SUCCESS, handle.init_from_bitmap(allocator_, bitmap));
  EXPECT_TRUE(handle.contains(84));
  EXPECT_FALSE(handle.contains(42));
  handle.reset();
  cache.flush();
}

// ==================== CacheValue Serialize/Deserialize Roundtrip ====================

TEST_F(TestDeletionBitmapCache, value_serialize_roundtrip)
{
  // Build bitmap with various values
  common::ObRoaringBitmap bm(&allocator_);
  for (uint64_t i = 0; i < 100; ++i) {
    EXPECT_EQ(OB_SUCCESS, bm.value_add(i * 7));
  }

  common::ObStringBuffer buf(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm.serialize(buf));
  ObString bitmap_bin(buf.length(), buf.ptr());

  // Put into cache value
  ObDeletionBitmapCacheValue value;
  EXPECT_EQ(OB_SUCCESS, value.init(bitmap_bin, 100));

  // Deep copy (simulates what KVCache does internally)
  const int64_t need_size = value.size();
  char *copy_buf = static_cast<char*>(allocator_.alloc(need_size));
  ASSERT_NE(nullptr, copy_buf);
  common::ObIKVCacheValue *copied_value = nullptr;
  EXPECT_EQ(OB_SUCCESS, value.deep_copy(copy_buf, need_size, copied_value));

  // Deserialize from copied value's bitmap_bin
  const ObDeletionBitmapCacheValue *typed = static_cast<const ObDeletionBitmapCacheValue*>(copied_value);
  common::ObRoaringBitmap bm2(&allocator_);
  EXPECT_EQ(OB_SUCCESS, bm2.deserialize(typed->get_bitmap_bin()));

  // Verify all values present
  for (uint64_t i = 0; i < 100; ++i) {
    EXPECT_TRUE(bm2.is_contains(i * 7));
  }
  EXPECT_FALSE(bm2.is_contains(1));
  EXPECT_FALSE(bm2.is_contains(8));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
