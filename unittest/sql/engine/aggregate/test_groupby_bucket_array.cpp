/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cstdlib>
#include <gtest/gtest.h>
#include <unordered_map>
#include "lib/allocator/page_arena.h"
#include "sql/engine/aggregate/ob_hash_agg_variant.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class BucketTestAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    ++alloc_calls_;
    void *ptr = fail_ ? nullptr : malloc(size);
    if (ptr != nullptr) {
      allocations_[ptr] = size;
      bytes_ += size;
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (ptr != nullptr) {
      auto it = allocations_.find(ptr);
      ASSERT_NE(allocations_.end(), it);
      bytes_ -= it->second;
      allocations_.erase(it);
      ::free(ptr);
    }
  }
  ~BucketTestAllocator() override
  {
    EXPECT_EQ(0, bytes_);
    for (auto &entry : allocations_) {
      ::free(entry.first);
    }
  }
  int64_t alloc_calls_ = 0;
  int64_t bytes_ = 0;
  bool fail_ = false;
  std::unordered_map<void *, int64_t> allocations_;
};

using BucketArray = ObGroupByBucketArray<ObGroupRowBucket>;

TEST(TestGroupByBucketArray, repeated_reuse_preserves_buffers_and_clears_buckets)
{
  BucketTestAllocator allocator;
  ModulePageAllocator page_allocator(allocator);
  BucketArray buckets(page_allocator);
  const int64_t capacities[] = {8192, buckets.BUCKET_ARRAY_CONTIGUOUS_THRESHOLD + 1};
  for (int64_t capacity : capacities) {
    ASSERT_EQ(OB_SUCCESS, buckets.init(capacity));
    const int64_t calls = allocator.alloc_calls_;
    const int64_t bytes = allocator.bytes_;
    for (int round = 0; round < 8; ++round) {
      auto *segmented = buckets.get_segmented_array();
      auto *contiguous = buckets.get_contiguous_array();
      for (int64_t i = 0; i < capacity; ++i) {
        auto &bucket = contiguous == nullptr ? segmented->at(i) : contiguous->at(i);
        bucket.set_valid();
        bucket.set_hash(123);
        bucket.set_bkt_seq(17);
      }
      allocator.fail_ = true;
      buckets.reuse();
      ASSERT_EQ(OB_SUCCESS, buckets.init(capacity));
      allocator.fail_ = false;
      ASSERT_EQ(capacity, buckets.count());
      EXPECT_EQ(calls, allocator.alloc_calls_);
      EXPECT_EQ(bytes, allocator.bytes_);
      EXPECT_EQ(segmented, buckets.get_segmented_array());
      for (int64_t i = 0; i < capacity; ++i) {
        const auto &bucket = contiguous == nullptr ? segmented->at(i) : contiguous->at(i);
        ASSERT_TRUE(bucket.is_empty());
        ASSERT_EQ(0, bucket.get_hash());
        ASSERT_EQ(0, bucket.get_bkt_seq());
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, buckets.init(64)); // segmented -> contiguous
  ASSERT_EQ(64, buckets.count());
  buckets.destroy();
  EXPECT_EQ(0, buckets.count());
  EXPECT_EQ(0, allocator.bytes_);
  buckets.destroy();
}

TEST(TestGroupByBucketArray, allocation_failure_can_be_destroyed_or_retried)
{
  BucketTestAllocator allocator;
  ModulePageAllocator page_allocator(allocator);
  BucketArray buckets(page_allocator);
  allocator.fail_ = true;
  const int64_t capacity = buckets.BUCKET_ARRAY_CONTIGUOUS_THRESHOLD + 1;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, buckets.init(capacity));
  EXPECT_EQ(0, buckets.count());
  allocator.fail_ = false;
  ASSERT_EQ(OB_SUCCESS, buckets.init(capacity));
  auto *segments = buckets.get_segmented_array();
  const int64_t bytes = allocator.bytes_;
  allocator.fail_ = true;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, buckets.init(capacity * 2));
  EXPECT_EQ(segments, buckets.get_segmented_array());
  EXPECT_EQ(bytes, allocator.bytes_);
  EXPECT_EQ(0, buckets.count());
  allocator.fail_ = false;
  ASSERT_EQ(OB_SUCCESS, buckets.init(capacity));
  buckets.destroy();
  EXPECT_EQ(0, allocator.bytes_);
}

template <typename Bucket>
class TestHashTable : public ObExtendHashTableVec<Bucket>
{
public:
  TestHashTable() : ObExtendHashTableVec<Bucket>(OB_SERVER_TENANT_ID) {}
  int init_empty_buckets(BucketTestAllocator &allocator)
  {
    using Array = typename ObExtendHashTableVec<Bucket>::BucketArray;
    this->allocator_.set_allocator(&allocator);
    void *buf = this->allocator_.alloc(sizeof(Array));
    if (nullptr == buf) {
      return OB_ALLOCATE_MEMORY_FAILED;
    }
    this->buckets_ = new(buf) Array(this->allocator_);
    return OB_SUCCESS;
  }
};

TEST(TestGroupByBucketArray, failed_reuse_propagates_through_resize_and_variant)
{
  BucketTestAllocator allocator;
  TestHashTable<ObGroupRowBucket> outline;
  TestHashTable<ObGroupRowBucketInline> inlined;
  ASSERT_EQ(OB_SUCCESS, outline.init_empty_buckets(allocator));
  ASSERT_EQ(OB_SUCCESS, inlined.init_empty_buckets(allocator));
  // A failed initialization leaves an empty array. Reusing it must report the
  // initialization error instead of continuing execution with zero buckets.
  EXPECT_EQ(OB_INVALID_ARGUMENT, outline.reuse());
  EXPECT_EQ(OB_INVALID_ARGUMENT, outline.resize(&allocator, 64));
  HashAggSetPtr variant = static_cast<ObExtendHashTableVec<ObGroupRowBucket> *>(&outline);
  ReuseVisitor visitor;
  EXPECT_EQ(OB_INVALID_ARGUMENT, boost::apply_visitor(visitor, variant));
  variant = static_cast<ObExtendHashTableVec<ObGroupRowBucketInline> *>(&inlined);
  EXPECT_EQ(OB_INVALID_ARGUMENT, boost::apply_visitor(visitor, variant));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_groupby_bucket_array.log", true);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
