/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <cstdlib>
#include "lib/allocator/page_arena.h"
#include "sql/das/search/ob_das_sparse_candidate_buffer.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

class CountingFailAllocator : public ObIAllocator
{
public:
  CountingFailAllocator()
    : fail_at_(-1), alloc_attempts_(0), outstanding_allocations_(0)
  {}
  ~CountingFailAllocator() override {}

  void set_fail_at(const int64_t fail_at) { fail_at_ = fail_at; }
  int64_t get_alloc_attempts() const { return alloc_attempts_; }
  int64_t get_outstanding_allocations() const { return outstanding_allocations_; }

  void *alloc(const int64_t size) override
  {
    return alloc_impl(size);
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc_impl(size);
  }

  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      std::free(ptr);
      --outstanding_allocations_;
    }
  }

private:
  void *alloc_impl(const int64_t size)
  {
    void *ptr = nullptr;
    ++alloc_attempts_;
    if (alloc_attempts_ != fail_at_) {
      ptr = std::malloc(size);
      if (nullptr != ptr) {
        ++outstanding_allocations_;
      }
    }
    return ptr;
  }

private:
  int64_t fail_at_;
  int64_t alloc_attempts_;
  int64_t outstanding_allocations_;
};

class TestSparseCandidateBuffer : public ::testing::Test
{
public:
  TestSparseCandidateBuffer() : allocator_("SparseCandTest") {}

protected:
  ObArenaAllocator allocator_;
};

TEST_F(TestSparseCandidateBuffer, init_and_not_initialized_errors)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 99;
  double score = 9.0;

  buffer.reuse();
  EXPECT_EQ(0, buffer.get_capacity());
  EXPECT_EQ(0, buffer.get_count());
  EXPECT_TRUE(buffer.is_empty());
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.has_unconsumed());
  EXPECT_FALSE(buffer.get_doc_id(0, doc_id));
  EXPECT_EQ(0, doc_id);
  EXPECT_EQ(OB_NOT_INIT, buffer.append(1, 1.0));
  EXPECT_EQ(OB_NOT_INIT, buffer.add_score(0, 1.0));
  EXPECT_EQ(OB_NOT_INIT, buffer.prune_below(1.0));
  EXPECT_EQ(OB_NOT_INIT, buffer.next(0.0, doc_id, score));

  doc_id = 99;
  score = 9.0;
  EXPECT_EQ(OB_NOT_INIT, buffer.advance_to(1, 0.0, doc_id, score));
  EXPECT_EQ(0, doc_id);
  EXPECT_DOUBLE_EQ(0.0, score);

  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.init(0, allocator_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.init(-1, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.init(3, allocator_));
  EXPECT_EQ(3, buffer.get_capacity());
  EXPECT_EQ(0, buffer.get_count());
  EXPECT_TRUE(buffer.is_empty());
  EXPECT_FALSE(buffer.is_full());
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.has_unconsumed());
  EXPECT_EQ(OB_INIT_TWICE, buffer.init(3, allocator_));

  buffer.reset();
  EXPECT_EQ(0, buffer.get_capacity());
  EXPECT_EQ(0, buffer.get_count());
  EXPECT_TRUE(buffer.is_empty());
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.has_unconsumed());
  EXPECT_EQ(OB_NOT_INIT, buffer.append(1, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.init(2, allocator_));
  EXPECT_EQ(2, buffer.get_capacity());
}

TEST_F(TestSparseCandidateBuffer, append_and_lookup)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 99;
  ASSERT_EQ(OB_SUCCESS, buffer.init(3, allocator_));

  ASSERT_EQ(OB_SUCCESS, buffer.append(0, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(10, 2.0));
  EXPECT_EQ(2, buffer.get_count());
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.append(10, 3.0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.append(9, 3.0));
  EXPECT_EQ(2, buffer.get_count());

  ASSERT_EQ(OB_SUCCESS, buffer.append(UINT64_MAX, 3.0));
  EXPECT_EQ(3, buffer.get_count());
  EXPECT_FALSE(buffer.is_empty());
  EXPECT_TRUE(buffer.is_full());
  EXPECT_TRUE(buffer.has_alive());
  EXPECT_TRUE(buffer.has_unconsumed());
  EXPECT_EQ(OB_SIZE_OVERFLOW, buffer.append(UINT64_MAX, 4.0));
  EXPECT_EQ(3, buffer.get_count());

  EXPECT_TRUE(buffer.get_doc_id(0, doc_id));
  EXPECT_EQ(0, doc_id);
  EXPECT_TRUE(buffer.get_doc_id(1, doc_id));
  EXPECT_EQ(10, doc_id);
  EXPECT_TRUE(buffer.get_doc_id(2, doc_id));
  EXPECT_EQ(UINT64_MAX, doc_id);

  doc_id = 99;
  EXPECT_FALSE(buffer.get_doc_id(-1, doc_id));
  EXPECT_EQ(0, doc_id);
  doc_id = 99;
  EXPECT_FALSE(buffer.get_doc_id(3, doc_id));
  EXPECT_EQ(0, doc_id);
}

TEST_F(TestSparseCandidateBuffer, score_and_prune)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, buffer.init(4, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.append(10, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(20, 2.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(30, 3.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(40, 4.0));

  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, buffer.add_score(-1, 1.0));
  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, buffer.add_score(4, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.add_score(0, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.add_score(1, -0.5));
  ASSERT_EQ(OB_SUCCESS, buffer.add_score(2, 1.0));

  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(2.0));
  EXPECT_FALSE(buffer.get_doc_id(0, doc_id));
  EXPECT_FALSE(buffer.get_doc_id(1, doc_id));
  EXPECT_TRUE(buffer.get_doc_id(2, doc_id));
  EXPECT_EQ(30, doc_id);
  EXPECT_TRUE(buffer.get_doc_id(3, doc_id));
  EXPECT_EQ(40, doc_id);
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.add_score(0, 1.0));

  ASSERT_EQ(OB_SUCCESS, buffer.add_score(2, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(4.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(4.0));
  EXPECT_TRUE(buffer.has_alive());
  EXPECT_EQ(4, buffer.get_count());
  EXPECT_TRUE(buffer.get_doc_id(2, doc_id));
  EXPECT_FALSE(buffer.get_doc_id(3, doc_id));

  ASSERT_EQ(OB_SUCCESS, buffer.next(4.0, doc_id, score));
  EXPECT_EQ(30, doc_id);
  EXPECT_DOUBLE_EQ(5.0, score);
  EXPECT_EQ(OB_ITER_END, buffer.next(4.0, doc_id, score));

  buffer.reuse();
  ASSERT_EQ(OB_SUCCESS, buffer.append(1, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(1.0));
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.is_empty());
  EXPECT_TRUE(buffer.has_unconsumed());
  EXPECT_EQ(OB_ITER_END, buffer.next(0.0, doc_id, score));
  EXPECT_FALSE(buffer.has_unconsumed());
}

TEST_F(TestSparseCandidateBuffer, next_consumes_in_order)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, buffer.init(7, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.append(10, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(20, 5.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(30, 2.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(40, 4.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(50, 3.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(60, 6.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(2.0));

  ASSERT_EQ(OB_SUCCESS, buffer.next(3.0, doc_id, score));
  EXPECT_EQ(20, doc_id);
  EXPECT_DOUBLE_EQ(5.0, score);
  EXPECT_TRUE(buffer.has_unconsumed());
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.append(70, 7.0));
  EXPECT_EQ(6, buffer.get_count());

  ASSERT_EQ(OB_SUCCESS, buffer.next(3.0, doc_id, score));
  EXPECT_EQ(40, doc_id);
  EXPECT_DOUBLE_EQ(4.0, score);
  ASSERT_EQ(OB_SUCCESS, buffer.next(3.0, doc_id, score));
  EXPECT_EQ(60, doc_id);
  EXPECT_DOUBLE_EQ(6.0, score);
  EXPECT_EQ(OB_ITER_END, buffer.next(3.0, doc_id, score));
  EXPECT_FALSE(buffer.has_unconsumed());
  EXPECT_EQ(OB_ITER_END, buffer.next(3.0, doc_id, score));

  buffer.reuse();
  ASSERT_EQ(OB_SUCCESS, buffer.append(1, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(2, 2.0));
  EXPECT_EQ(OB_ITER_END, buffer.next(100.0, doc_id, score));
  EXPECT_FALSE(buffer.has_unconsumed());
  EXPECT_EQ(OB_ITER_END, buffer.next(-1.0, doc_id, score));
}

TEST_F(TestSparseCandidateBuffer, advance_to_is_monotonic_and_idempotent)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, buffer.init(6, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.append(10, 5.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(20, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(30, 5.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(40, 4.5));
  ASSERT_EQ(OB_SUCCESS, buffer.append(50, 2.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(60, 6.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(2.0));

  ASSERT_EQ(OB_SUCCESS, buffer.advance_to(0, 4.0, doc_id, score));
  EXPECT_EQ(10, doc_id);
  EXPECT_DOUBLE_EQ(5.0, score);

  ASSERT_EQ(OB_SUCCESS, buffer.advance_to(25, 4.0, doc_id, score));
  EXPECT_EQ(30, doc_id);
  EXPECT_DOUBLE_EQ(5.0, score);

  ASSERT_EQ(OB_SUCCESS, buffer.next(4.0, doc_id, score));
  EXPECT_EQ(40, doc_id);
  EXPECT_DOUBLE_EQ(4.5, score);

  ASSERT_EQ(OB_SUCCESS, buffer.advance_to(15, 4.0, doc_id, score));
  EXPECT_EQ(40, doc_id);
  EXPECT_DOUBLE_EQ(4.5, score);

  ASSERT_EQ(OB_SUCCESS, buffer.advance_to(40, 4.0, doc_id, score));
  EXPECT_EQ(40, doc_id);
  EXPECT_DOUBLE_EQ(4.5, score);

  ASSERT_EQ(OB_SUCCESS, buffer.next(4.0, doc_id, score));
  EXPECT_EQ(60, doc_id);
  EXPECT_DOUBLE_EQ(6.0, score);

  doc_id = 99;
  score = 9.0;
  EXPECT_EQ(OB_ITER_END, buffer.advance_to(100, 4.0, doc_id, score));
  EXPECT_EQ(0, doc_id);
  EXPECT_DOUBLE_EQ(0.0, score);
  EXPECT_FALSE(buffer.has_unconsumed());

  doc_id = 99;
  score = 9.0;
  EXPECT_EQ(OB_ITER_END, buffer.advance_to(0, 0.0, doc_id, score));
  EXPECT_EQ(0, doc_id);
  EXPECT_DOUBLE_EQ(0.0, score);
}

TEST_F(TestSparseCandidateBuffer, reuse_and_reset)
{
  SparseCandidateBuffer buffer;
  uint64_t doc_id = 0;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, buffer.init(3, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.append(100, 1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.append(200, 2.0));
  ASSERT_EQ(OB_SUCCESS, buffer.prune_below(1.0));
  ASSERT_EQ(OB_SUCCESS, buffer.next(0.0, doc_id, score));
  EXPECT_EQ(200, doc_id);
  EXPECT_TRUE(buffer.has_unconsumed());
  EXPECT_EQ(OB_ITER_END, buffer.next(0.0, doc_id, score));
  EXPECT_FALSE(buffer.has_unconsumed());

  buffer.reuse();
  EXPECT_EQ(3, buffer.get_capacity());
  EXPECT_EQ(0, buffer.get_count());
  EXPECT_TRUE(buffer.is_empty());
  EXPECT_FALSE(buffer.is_full());
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.has_unconsumed());
  ASSERT_EQ(OB_SUCCESS, buffer.append(1, 3.0));
  EXPECT_TRUE(buffer.get_doc_id(0, doc_id));
  EXPECT_EQ(1, doc_id);
  ASSERT_EQ(OB_SUCCESS, buffer.next(2.0, doc_id, score));
  EXPECT_EQ(1, doc_id);
  EXPECT_DOUBLE_EQ(3.0, score);

  buffer.reset();
  EXPECT_EQ(0, buffer.get_capacity());
  EXPECT_EQ(0, buffer.get_count());
  EXPECT_TRUE(buffer.is_empty());
  EXPECT_FALSE(buffer.has_alive());
  EXPECT_FALSE(buffer.has_unconsumed());
  buffer.reuse();
  EXPECT_EQ(OB_NOT_INIT, buffer.append(1, 1.0));

  ASSERT_EQ(OB_SUCCESS, buffer.init(1, allocator_));
  ASSERT_EQ(OB_SUCCESS, buffer.append(UINT64_MAX, 1.0));
  EXPECT_TRUE(buffer.is_full());
}

TEST_F(TestSparseCandidateBuffer, allocation_failures_are_cleaned_up)
{
  for (int64_t fail_at = 1; fail_at <= 3; ++fail_at) {
    CountingFailAllocator allocator;
    SparseCandidateBuffer buffer;
    allocator.set_fail_at(fail_at);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, buffer.init(4, allocator));
    EXPECT_EQ(fail_at, allocator.get_alloc_attempts());
    buffer.reset();
    EXPECT_EQ(0, allocator.get_outstanding_allocations());
  }

  CountingFailAllocator allocator;
  SparseCandidateBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.init(4, allocator));
  EXPECT_EQ(3, allocator.get_alloc_attempts());
  EXPECT_GT(allocator.get_outstanding_allocations(), 0);
  buffer.reset();
  EXPECT_EQ(0, allocator.get_outstanding_allocations());
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
