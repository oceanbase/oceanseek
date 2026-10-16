/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include "lib/allocator/ob_allocator.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_simple_op.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

class TrackingAllocator : public ObIAllocator
{
public:
  struct Allocation
  {
    void *ptr_;
    int64_t size_;
  };

  TrackingAllocator() : alloc_count_(0), free_count_(0), invalid_free_(false) {}
  ~TrackingAllocator() override { reset(); }

  void *alloc(const int64_t size) override { return alloc_impl(size); }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc_impl(size);
  }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      const auto iter = std::find_if(allocations_.begin(), allocations_.end(),
          [ptr](const Allocation &allocation) { return allocation.ptr_ == ptr; });
      if (iter == allocations_.end()) {
        invalid_free_ = true;
      } else {
        std::free(iter->ptr_);
        allocations_.erase(iter);
        ++free_count_;
      }
    }
  }
  void reset()
  {
    for (const Allocation &allocation : allocations_) {
      std::free(allocation.ptr_);
    }
    allocations_.clear();
  }
  bool owns(const void *ptr) const
  {
    return std::any_of(allocations_.begin(), allocations_.end(),
        [ptr](const Allocation &allocation) {
          const uintptr_t begin = reinterpret_cast<uintptr_t>(allocation.ptr_);
          const uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
          return address >= begin && address < begin + allocation.size_;
        });
  }
  int64_t alloc_count() const { return alloc_count_; }
  int64_t free_count() const { return free_count_; }
  int64_t outstanding_count() const { return allocations_.size(); }
  int64_t outstanding_bytes() const
  {
    int64_t bytes = 0;
    for (const Allocation &allocation : allocations_) {
      bytes += allocation.size_;
    }
    return bytes;
  }
  bool has_invalid_free() const { return invalid_free_; }

private:
  void *alloc_impl(const int64_t size)
  {
    void *ptr = std::malloc(size);
    if (nullptr != ptr) {
      allocations_.push_back({ptr, size});
      ++alloc_count_;
    }
    return ptr;
  }

  std::vector<Allocation> allocations_;
  int64_t alloc_count_;
  int64_t free_count_;
  bool invalid_free_;
};

void fill_mapping(ObLogicalTableColumnMapping &mapping, const int64_t count)
{
  for (int64_t i = 0; i < count; ++i) {
    ObLogicalColumnMappingEntry entry;
    entry.source_ = FROM_VALUE_PATH;
    entry.source_idx_ = i;
    entry.column_name_ = ObString::make_string("column_name");
    entry.json_path_ = ObString::make_string("$.nested.value");
    entry.logical_column_id_ = static_cast<uint64_t>(i + 1);
    ASSERT_EQ(OB_SUCCESS, mapping.entries_.push_back(entry));
  }
}

TEST(TestLogicalColumnMappingAllocator, inline_and_expansion_storage)
{
  TrackingAllocator allocator;
  for (int64_t round = 0; round < 3; ++round) {
    for (const int64_t count : {0, 8, 9, 256}) {
      const int64_t alloc_count_before = allocator.alloc_count();
      void *buf = allocator.alloc(sizeof(ObDASEmptyCtDef));
      ASSERT_NE(nullptr, buf);
      ObDASEmptyCtDef *ctdef = new (buf) ObDASEmptyCtDef(allocator);
      EXPECT_EQ(&allocator, ctdef->logical_column_mapping_.allocator_);
      fill_mapping(ctdef->logical_column_mapping_, count);
      ASSERT_EQ(count, ctdef->logical_column_mapping_.count());
      for (int64_t i = 0; i < count; ++i) {
        EXPECT_EQ(i, ctdef->logical_column_mapping_.entries_.at(i).source_idx_);
      }
      if (count <= 8) {
        EXPECT_EQ(1, allocator.alloc_count() - alloc_count_before);
      } else {
        EXPECT_GT(allocator.alloc_count() - alloc_count_before, 1);
        EXPECT_TRUE(allocator.owns(&ctdef->logical_column_mapping_.entries_.at(8)));
      }
      if (count == 256) {
        EXPECT_GT(allocator.alloc_count() - alloc_count_before, 2);
      }
      // Plan arenas reclaim CtDefs and their array blocks without invoking destructors.
      allocator.reset();
      EXPECT_EQ(0, allocator.outstanding_count());
      EXPECT_EQ(0, allocator.outstanding_bytes());
      EXPECT_FALSE(allocator.has_invalid_free());
    }
  }
}

TEST(TestLogicalColumnMappingAllocator, deserialize_deep_copies_strings)
{
  ObArenaAllocator source_arena("LogColMapSrc");
  ObLogicalTableColumnMapping source(source_arena);
  source.index_seq_ = 17;
  fill_mapping(source, 9);

  const int64_t size = source.get_serialize_size();
  char *serialized = static_cast<char *>(source_arena.alloc(size));
  ASSERT_NE(nullptr, serialized);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(serialized, size, pos));

  ObArenaAllocator target_arena("LogColMapDst");
  ObLogicalTableColumnMapping target(target_arena);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(serialized, size, pos));
  ASSERT_EQ(9, target.count());
  ASSERT_EQ(17, target.index_seq_);

  memset(serialized, 0, size);
  source_arena.reset();
  for (int64_t i = 0; i < target.count(); ++i) {
    EXPECT_EQ(ObString::make_string("column_name"), target.entries_.at(i).column_name_);
    EXPECT_EQ(ObString::make_string("$.nested.value"), target.entries_.at(i).json_path_);
    EXPECT_EQ(i, target.entries_.at(i).source_idx_);
  }
  target.entries_.reset();
  target_arena.reset();
  EXPECT_EQ(0, target_arena.used());
}

template <typename CtDef>
void check_ctdef_normal_destruction()
{
  TrackingAllocator allocator;
  for (const int64_t count : {0, 64}) {
    const int64_t free_count_before = allocator.free_count();
    {
      CtDef ctdef(allocator);
      EXPECT_EQ(&allocator, ctdef.logical_column_mapping_.allocator_);
      fill_mapping(ctdef.logical_column_mapping_, count);
      if (count > 8) {
        EXPECT_TRUE(allocator.owns(&ctdef.logical_column_mapping_.entries_.at(8)));
      }
    }
    EXPECT_EQ(0, allocator.outstanding_count());
    EXPECT_EQ(0, allocator.outstanding_bytes());
    EXPECT_FALSE(allocator.has_invalid_free());
    if (count > 8) {
      EXPECT_GT(allocator.free_count(), free_count_before);
    }
  }
}

TEST(TestLogicalColumnMappingAllocator, ctdef_normal_destruction)
{
  check_ctdef_normal_destruction<ObDASScanCtDef>();
  check_ctdef_normal_destruction<ObDASInsCtDef>();
  check_ctdef_normal_destruction<ObDASTableLookupCtDef>();
  check_ctdef_normal_destruction<ObDASEmptyCtDef>();
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
