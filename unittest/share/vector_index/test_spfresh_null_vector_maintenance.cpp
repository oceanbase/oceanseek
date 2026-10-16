/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <algorithm>
#include <utility>
#include <vector>
#include "lib/allocator/ob_malloc.h"
#define private public
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/spfresh/ob_spfresh_reassign_task.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#undef private

namespace oceanbase
{
namespace share
{

class CountingPoisonAllocator : public common::ObIAllocator
{
public:
  struct Block
  {
    Block(void *ptr, int64_t size) : ptr_(ptr), size_(size) {}
    void *ptr_;
    int64_t size_;
  };

  CountingPoisonAllocator()
      : current_bytes_(0), peak_bytes_(0), reuse_count_(0), fail_alloc_(false)
  {}

  ~CountingPoisonAllocator() override
  {
    reset();
  }

  void *alloc(const int64_t size) override
  {
    return alloc(size, common::ObMemAttr(1, "SpfreshMemT"));
  }

  void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  {
    void *ptr = nullptr;
    if (!fail_alloc_ && size > 0 && OB_NOT_NULL(ptr = ob_malloc(size, attr))) {
      blocks_.push_back(Block(ptr, size));
      current_bytes_ += size;
      peak_bytes_ = std::max(peak_bytes_, current_bytes_);
    }
    return ptr;
  }

  void free(void *ptr) override
  {
    for (std::vector<Block>::iterator iter = blocks_.begin();
         iter != blocks_.end(); ++iter) {
      if (iter->ptr_ == ptr) {
        MEMSET(iter->ptr_, 0xA5, iter->size_);
        ob_free(iter->ptr_);
        current_bytes_ -= iter->size_;
        blocks_.erase(iter);
        break;
      }
    }
  }

  void reuse() override
  {
    for (int64_t i = 0; i < static_cast<int64_t>(blocks_.size()); ++i) {
      MEMSET(blocks_.at(i).ptr_, 0xA5, blocks_.at(i).size_);
      ob_free(blocks_.at(i).ptr_);
    }
    blocks_.clear();
    current_bytes_ = 0;
    ++reuse_count_;
  }

  void reset() override
  {
    reuse();
  }

  void set_fail_alloc(bool fail_alloc) { fail_alloc_ = fail_alloc; }
  int64_t current_bytes() const { return current_bytes_; }
  int64_t peak_bytes() const { return peak_bytes_; }
  int64_t reuse_count() const { return reuse_count_; }

private:
  std::vector<Block> blocks_;
  int64_t current_bytes_;
  int64_t peak_bytes_;
  int64_t reuse_count_;
  bool fail_alloc_;
};

TEST(TestSpfreshNullVectorMaintenance, safe_raw_vector_expected_bytes)
{
  int64_t expected_bytes = -1;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            ObSpfreshTableHelper::get_raw_vector_expected_bytes(0, expected_bytes));
  EXPECT_EQ(0, expected_bytes);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            ObSpfreshTableHelper::get_raw_vector_expected_bytes(-1, expected_bytes));
  EXPECT_EQ(0, expected_bytes);

  const int64_t max_dimension = INT64_MAX / static_cast<int64_t>(sizeof(float));
  EXPECT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::get_raw_vector_expected_bytes(
                max_dimension, expected_bytes));
  EXPECT_EQ(max_dimension * static_cast<int64_t>(sizeof(float)), expected_bytes);
  EXPECT_EQ(common::OB_SIZE_OVERFLOW,
            ObSpfreshTableHelper::get_raw_vector_expected_bytes(
                max_dimension + 1, expected_bytes));
  EXPECT_EQ(0, expected_bytes);
}

TEST(TestSpfreshNullVectorMaintenance, grouped_count_publication_is_failure_atomic)
{
  typedef std::pair<uint64_t, int64_t> CenterCount;
  common::ObSEArray<CenterCount, 2> local_counts;
  ASSERT_EQ(common::OB_SUCCESS, local_counts.push_back(std::make_pair(1, 11)));
  ASSERT_EQ(common::OB_SUCCESS, local_counts.push_back(std::make_pair(2, 22)));

  CountingPoisonAllocator backing_allocator;
  common::ModulePageAllocator page_allocator(backing_allocator, "SpfreshPubT");
  common::ObArray<CenterCount> center_counts(
      OB_MALLOC_NORMAL_BLOCK_SIZE, page_allocator);
  ASSERT_EQ(common::OB_SUCCESS,
            center_counts.push_back(std::make_pair(9, 99)));

  // ObArray::assign must allocate replacement storage. Inject failure there
  // and verify that the pre-existing result remains byte-for-byte observable.
  backing_allocator.set_fail_alloc(true);
  EXPECT_EQ(common::OB_ALLOCATE_MEMORY_FAILED,
            ObSpfreshTableHelper::publish_grouped_counts_(
                local_counts, center_counts));
  ASSERT_EQ(1, center_counts.count());
  EXPECT_EQ(9, center_counts.at(0).first);
  EXPECT_EQ(99, center_counts.at(0).second);

  backing_allocator.set_fail_alloc(false);
  ASSERT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::publish_grouped_counts_(
                local_counts, center_counts));
  ASSERT_EQ(2, center_counts.count());
  EXPECT_EQ(1, center_counts.at(0).first);
  EXPECT_EQ(11, center_counts.at(0).second);
  EXPECT_EQ(2, center_counts.at(1).first);
  EXPECT_EQ(22, center_counts.at(1).second);
}

TEST(TestSpfreshNullVectorMaintenance, raw_payload_must_match_dimension_exactly)
{
  const int64_t dimension = 3;
  int64_t expected_bytes = 0;
  ASSERT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::get_raw_vector_expected_bytes(
                dimension, expected_bytes));
  ASSERT_EQ(12, expected_bytes);

  common::ObArenaAllocator allocator("SpfreshRawT");
  common::ObArray<ObSpfreshVectorRow> vectors;
  ObSpfreshVectorRow key_row;
  key_row.center_id_ = 7;
  key_row.set_rowkey_from_int(42);
  const float payload[4] = {1.0F, 2.0F, 3.0F, 4.0F};

  ASSERT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::append_raw_cid_vector_(
                reinterpret_cast<const char *>(payload), 12, expected_bytes,
                allocator, key_row, vectors));
  ASSERT_EQ(1, vectors.count());
  EXPECT_EQ(12, vectors.at(0).vector_len_);
  EXPECT_EQ(0, MEMCMP(vectors.at(0).vector_, payload, 12));

  const int64_t count_before_null = vectors.count();
  EXPECT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::append_raw_cid_vector_(
                nullptr, 0, expected_bytes, allocator, key_row, vectors));
  EXPECT_EQ(count_before_null, vectors.count());

  // The 4/8/16-byte cases were accepted by the old alignment-only decoder;
  // they are the production-delta red cases for dimension-aware validation.
  const int64_t invalid_lengths[] = {4, 8, 16, 5};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_lengths); ++i) {
    const int64_t count_before_error = vectors.count();
    EXPECT_EQ(common::OB_INVALID_DATA,
              ObSpfreshTableHelper::append_raw_cid_vector_(
                  reinterpret_cast<const char *>(payload), invalid_lengths[i],
                  expected_bytes, allocator, key_row, vectors));
    EXPECT_EQ(count_before_error, vectors.count());
  }

  const int64_t count_before_null_error = vectors.count();
  EXPECT_EQ(common::OB_INVALID_DATA,
            ObSpfreshTableHelper::append_raw_cid_vector_(
                nullptr, expected_bytes, expected_bytes,
                allocator, key_row, vectors));
  EXPECT_EQ(count_before_null_error, vectors.count());
}

TEST(TestSpfreshNullVectorMaintenance, malformed_nonempty_count_is_rejected)
{
  EXPECT_EQ(common::OB_SUCCESS,
            ObSpfreshTableHelper::validate_raw_vector_count_result_(8191, 0));
  EXPECT_EQ(common::OB_INVALID_DATA,
            ObSpfreshTableHelper::validate_raw_vector_count_result_(8191, 1));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObSpfreshTableHelper::validate_raw_vector_count_result_(-1, 0));
}

TEST(TestSpfreshNullVectorMaintenance, scan_result_publication_is_failure_atomic)
{
  common::ObArray<ObSpfreshVectorRow> vectors;
  for (int64_t i = 0; i < 32; ++i) {
    ObSpfreshVectorRow row;
    row.center_id_ = 7;
    row.set_rowkey_from_int(i + 1);
    ASSERT_EQ(common::OB_SUCCESS, vectors.push_back(row));
    // Match the decoder's per-push repair. A later ObArray expansion can still
    // invalidate pointers in previously inserted rows.
    ObSpfreshVectorRow &last = vectors.at(vectors.count() - 1);
    last.rowkey_ = last.rowkey_buf_;
  }
  ASSERT_EQ(32, vectors.count());

  // A later malformed row or SQL read error must not expose any of the partial
  // rows, including rows whose self-referential pointer was invalidated.
  ObSpfreshTableHelper::finalize_vector_scan_result_(
      common::OB_INVALID_DATA, vectors);
  EXPECT_TRUE(vectors.empty());

  ObSpfreshVectorRow row;
  row.set_rowkey_from_int(42);
  ASSERT_EQ(common::OB_SUCCESS, vectors.push_back(row));
  vectors.at(0).rowkey_ = nullptr;
  ObSpfreshTableHelper::finalize_vector_scan_result_(
      common::OB_SUCCESS, vectors);
  ASSERT_EQ(1, vectors.count());
  EXPECT_EQ(vectors.at(0).rowkey_buf_, vectors.at(0).rowkey_);
}

TEST(TestSpfreshNullVectorMaintenance, batch_allocator_is_reused_after_every_batch)
{
  // Before per-batch reuse was added, these allocations accumulated and the
  // asserted peak grew linearly with the number of batches.
  CountingPoisonAllocator allocator;
  const int64_t bytes_per_batch =
      ObSpfreshReassignTask::BATCH_SIZE * 3 * static_cast<int64_t>(sizeof(float));
  char last_rowkey_buf[8] = {0};
  const char *last_rowkey = nullptr;
  int64_t last_rowkey_len = 0;
  int64_t total_processed = 0;

  for (int64_t batch = 0; batch < 3; ++batch) {
    void *batch_vectors = allocator.alloc(bytes_per_batch);
    ASSERT_NE(nullptr, batch_vectors);
    MEMSET(batch_vectors, static_cast<int>(batch), bytes_per_batch);
    char next_rowkey[8] = {0};
    next_rowkey[7] = static_cast<char>(batch + 1);
    ASSERT_EQ(common::OB_SUCCESS,
              ObSpfreshReassignTask::finish_batch_iteration_(
                  common::OB_SUCCESS, next_rowkey, sizeof(next_rowkey),
                  false, ObSpfreshReassignTask::BATCH_SIZE,
                  last_rowkey_buf, sizeof(last_rowkey_buf),
                  last_rowkey, last_rowkey_len, total_processed, allocator));
    EXPECT_EQ(0, allocator.current_bytes());
    ASSERT_EQ(last_rowkey_buf, last_rowkey);
    ASSERT_EQ(8, last_rowkey_len);
    EXPECT_EQ(batch + 1, static_cast<unsigned char>(last_rowkey[7]));
  }

  EXPECT_EQ(3 * ObSpfreshReassignTask::BATCH_SIZE, total_processed);
  EXPECT_EQ(3, allocator.reuse_count());
  EXPECT_LE(allocator.peak_bytes(), bytes_per_batch);

  ASSERT_NE(nullptr, allocator.alloc(bytes_per_batch));
  const int64_t total_before_error = total_processed;
  EXPECT_EQ(common::OB_TIMEOUT,
            ObSpfreshReassignTask::finish_batch_iteration_(
                common::OB_TIMEOUT, nullptr, 0, false,
                ObSpfreshReassignTask::BATCH_SIZE,
                last_rowkey_buf, sizeof(last_rowkey_buf),
                last_rowkey, last_rowkey_len, total_processed, allocator));
  EXPECT_EQ(total_before_error, total_processed);
  EXPECT_EQ(0, allocator.current_bytes());
  EXPECT_EQ(4, allocator.reuse_count());
  EXPECT_LE(allocator.peak_bytes(), bytes_per_batch);
}

TEST(TestSpfreshNullVectorMaintenance, empty_cid0_count_does_not_request_restore)
{
  common::ObSEArray<ObSpfreshCentroidRow, 2> centroids;
  ObSpfreshCentroidRow child;
  child.center_id_ = 7;
  ASSERT_EQ(common::OB_SUCCESS, centroids.push_back(child));

  common::hash::ObHashMap<uint64_t, int64_t> count_map;
  ASSERT_EQ(common::OB_SUCCESS,
            count_map.create(16, common::ObMemAttr(1, "SpfreshCid0T")));
  bool need_restore = true;
  int64_t cid0_posting = -1;
  EXPECT_EQ(common::OB_SUCCESS,
            ObIvfCacheUtil::check_bootstrap_cid0_restore_needed_(
                centroids, count_map, need_restore, cid0_posting));
  EXPECT_FALSE(need_restore);
  EXPECT_EQ(0, cid0_posting);

  ASSERT_EQ(common::OB_SUCCESS, count_map.set_refactored(0, 0));
  need_restore = true;
  cid0_posting = -1;
  EXPECT_EQ(common::OB_SUCCESS,
            ObIvfCacheUtil::check_bootstrap_cid0_restore_needed_(
                centroids, count_map, need_restore, cid0_posting));
  EXPECT_FALSE(need_restore);
  EXPECT_EQ(0, cid0_posting);

  ASSERT_EQ(common::OB_SUCCESS, count_map.set_refactored(0, 9, 1));
  EXPECT_EQ(common::OB_SUCCESS,
            ObIvfCacheUtil::check_bootstrap_cid0_restore_needed_(
                centroids, count_map, need_restore, cid0_posting));
  EXPECT_TRUE(need_restore);
  EXPECT_EQ(9, cid0_posting);
  count_map.destroy();
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
