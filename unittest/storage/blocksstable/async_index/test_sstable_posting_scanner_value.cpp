/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "mtlenv/mock_tenant_module_env.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/function/ob_function.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#define private public
#include "storage/blocksstable/async_index/ob_sstable_posting_scanner.h"
#undef private

#include <gtest/gtest.h>

namespace oceanbase
{
namespace blocksstable
{

class ObTestPostingBlockMetaVisitor final
{
public:
  ObTestPostingBlockMetaVisitor(
      common::ObSEArray<uint64_t, 2> &visited_max_ids,
      const uint64_t &stop_after_max_id)
    : visited_max_ids_(visited_max_ids),
      stop_after_max_id_(stop_after_max_id)
  {}

  int operator()(const ObPostingBlockMeta &block_meta, bool &stop)
  {
    int ret = OB_SUCCESS;
    stop = false;
    if (OB_FAIL(visited_max_ids_.push_back(block_meta.max_id_))) {
      LOG_WARN("failed to record visited posting block meta", K(ret), K(block_meta));
    } else {
      stop = block_meta.max_id_ >= stop_after_max_id_;
    }
    return ret;
  }

private:
  common::ObSEArray<uint64_t, 2> &visited_max_ids_;
  const uint64_t &stop_after_max_id_;
};

class TestSSTablePostingScannerValue : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  static int build_agg_buf(
      ObArenaAllocator &allocator,
      const int64_t posting_value_offset,
      const int64_t posting_value_size,
      char *&buf,
      int64_t &size)
  {
    return build_agg_buf_with_range(
        allocator, posting_value_offset, posting_value_size, 1, 1, buf, size);
  }

  static int build_agg_buf_with_range(
      ObArenaAllocator &allocator,
      const int64_t posting_value_offset,
      const int64_t posting_value_size,
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      char *&buf,
      int64_t &size)
  {
    ObSEArray<ObInvIdxLogicalBlockMeta, 2> metas;
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = 0;
    meta.doc_count_ = 1;
    meta.min_doc_id_ = min_doc_id;
    meta.max_doc_id_ = max_doc_id;
    int ret = OB_SUCCESS;
    if (OB_FAIL(metas.push_back(meta))) {
    } else if (OB_FAIL(ObInvIdxAggDataEncoder::build(
        allocator, posting_value_offset, posting_value_size, metas, buf, size))) {
    }
    return ret;
  }

  static void setup_data_row_header(
      ObIndexBlockRowHeader &row_header,
      const int32_t data_block_size)
  {
    row_header.reset();
    row_header.set_data_block();
    row_header.block_size_ = data_block_size;
  }
};

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_from_agg)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ObSEArray<ObInvIdxLogicalBlockMeta, 2> metas;
  ObInvIdxLogicalBlockMeta meta;
  meta.block_idx_ = 0;
  meta.doc_count_ = 3;
  meta.min_doc_id_ = 1;
  meta.max_doc_id_ = 3;
  ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
      allocator, 4096, 8192, metas, buf, size));

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObIndexBlockRowHeader row_header;
  setup_data_row_header(row_header, 4096 + 8192);
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.row_header_ = &row_header;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;
  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(4096U, item.value_micro_offset_);
  EXPECT_EQ(8192U, item.value_micro_size_);
  EXPECT_TRUE(item.value_row_header_valid_);
  EXPECT_EQ(4096U, item.value_row_header_.block_offset_);
  EXPECT_EQ(8192U, item.value_row_header_.block_size_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_disabled)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ObSEArray<ObInvIdxLogicalBlockMeta, 2> metas;
  ObInvIdxLogicalBlockMeta meta;
  meta.block_idx_ = 0;
  meta.doc_count_ = 1;
  meta.min_doc_id_ = 1;
  meta.max_doc_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
      allocator, 100, 200, metas, buf, size));

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = false;
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;
  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_invalid_agg_buffer)
{
  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObSSTablePostingScanner::DataPrefetchItem item;

  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);

  char tiny_buf[1] = {0};
  item.index_info_.agg_row_buf_ = tiny_buf;
  item.index_info_.agg_buf_size_ = sizeof(tiny_buf);
  ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_FALSE(item.value_row_header_valid_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_bad_agg_version)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 4096, 8192, buf, size));

  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);
  header->version_ = ObInvIdxAggDataHeader::VERSION + 1;

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObIndexBlockRowHeader row_header;
  row_header.reset();
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.row_header_ = &row_header;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;

  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_malformed_header)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 4096, 8192, buf, size));

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObIndexBlockRowHeader row_header;
  row_header.reset();
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.row_header_ = &row_header;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);

  header->total_size_ = sizeof(ObInvIdxAggDataHeader);
  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);

  header->total_size_ = size;
  header->logical_block_count_ = static_cast<uint16_t>(
      (size - sizeof(ObInvIdxAggDataHeader)) / sizeof(ObInvIdxAggDataEntry) + 1);
  ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_malformed_locator)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 4096, 8192, buf, size));

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObIndexBlockRowHeader row_header;
  row_header.reset();
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.row_header_ = &row_header;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);

  header->posting_value_size_ = 0;
  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);

  header->posting_value_offset_ = 0;
  header->posting_value_size_ = 8192;
  ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0U, item.value_micro_offset_);
  EXPECT_EQ(0U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);
}

TEST_F(TestSSTablePostingScannerValue, fill_value_locator_missing_row_header)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 100, 200, buf, size));

  ObSSTablePostingScanner scanner;
  scanner.need_posting_value_ = true;
  ObSSTablePostingScanner::DataPrefetchItem item;
  item.index_info_.row_header_ = nullptr;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;

  int ret = scanner.fill_value_locator_from_agg(item);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(100U, item.value_micro_offset_);
  EXPECT_EQ(200U, item.value_micro_size_);
  EXPECT_FALSE(item.value_row_header_valid_);
}

TEST_F(TestSSTablePostingScannerValue, borrow_prefetched_metadata_rows_without_cursor_change)
{
  ObArenaAllocator allocator;
  char *buf0 = nullptr;
  char *buf1 = nullptr;
  int64_t size0 = 0;
  int64_t size1 = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 0, 0, buf0, size0));
  ASSERT_EQ(OB_SUCCESS, build_agg_buf(allocator, 0, 0, buf1, size1));

  ObSSTablePostingScanner scanner;
  scanner.is_inited_ = true;
  scanner.data_read_idx_ = 7;
  scanner.data_prefetch_idx_ = 9;
  for (int64_t offset = 0; offset < 2; ++offset) {
    // 为连续 ring item 构造匹配 generation/ref-count 的 leaf pin 引用。
    const int64_t ring_idx =
        (scanner.data_read_idx_ + offset) % ObSSTablePostingScanner::DATA_RING_CAP;
    const int64_t pin_idx = offset;
    ObSSTablePostingScanner::DataPrefetchItem &item = scanner.data_ring_[ring_idx];
    item.leaf_pin_idx_ = pin_idx;
    item.leaf_pin_generation_ = scanner.leaf_pin_slots_[pin_idx].generation_;
    scanner.leaf_pin_slots_[pin_idx].ref_cnt_ = 1;
    item.index_info_.agg_row_buf_ = 0 == offset ? buf0 : buf1;
    item.index_info_.agg_buf_size_ = 0 == offset ? size0 : size1;
    item.max_doc_id_ = 0 == offset ? 100 : 200;
  }

  const char *borrowed = nullptr;
  int64_t borrowed_size = 0;
  uint64_t max_doc_id = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.get_prefetched_meta_row(
      0, borrowed, borrowed_size, max_doc_id));
  EXPECT_EQ(buf0, borrowed);
  EXPECT_EQ(size0, borrowed_size);
  EXPECT_EQ(100, max_doc_id);
  ASSERT_EQ(OB_SUCCESS, scanner.get_prefetched_meta_row(
      1, borrowed, borrowed_size, max_doc_id));
  EXPECT_EQ(buf1, borrowed);
  EXPECT_EQ(size1, borrowed_size);
  EXPECT_EQ(200, max_doc_id);
  // 两次借用都不改变 read/prefetch cursor。
  EXPECT_EQ(7, scanner.data_read_idx_);
  EXPECT_EQ(9, scanner.data_prefetch_idx_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.get_prefetched_meta_row(
      2, borrowed, borrowed_size, max_doc_id));
  EXPECT_EQ(nullptr, borrowed);
  EXPECT_EQ(0, borrowed_size);
  EXPECT_EQ(0, max_doc_id);
  borrowed = buf0;
  borrowed_size = size0;
  max_doc_id = 100;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.get_prefetched_meta_row(
      INT64_MAX, borrowed, borrowed_size, max_doc_id));
  EXPECT_EQ(nullptr, borrowed);
  EXPECT_EQ(0, borrowed_size);
  EXPECT_EQ(0, max_doc_id);

  // generation 变化后同一 slot 被识别为 stale pin。
  scanner.leaf_pin_slots_[1].generation_++;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_prefetched_meta_row(
      1, borrowed, borrowed_size, max_doc_id));
  scanner.data_read_idx_ = scanner.data_prefetch_idx_ = 0;
  scanner.is_inited_ = false;
}

TEST_F(TestSSTablePostingScannerValue, visit_prefetched_block_meta)
{
  ObArenaAllocator allocator;
  char *buf0 = nullptr;
  char *buf1 = nullptr;
  int64_t size0 = 0;
  int64_t size1 = 0;
  ASSERT_EQ(OB_SUCCESS, build_agg_buf_with_range(
      allocator, 0, 0, 1, 10, buf0, size0));
  ASSERT_EQ(OB_SUCCESS, build_agg_buf_with_range(
      allocator, 0, 0, 11, 20, buf1, size1));

  ObSSTablePostingScanner scanner;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.data_read_idx_ = 7;
  scanner.data_prefetch_idx_ = 9;
  scanner.data_prefetch_depth_ = 2;
  for (int64_t offset = 0; offset < 2; ++offset) {
    const int64_t ring_idx =
        (scanner.data_read_idx_ + offset) % ObSSTablePostingScanner::DATA_RING_CAP;
    ObSSTablePostingScanner::DataPrefetchItem &item = scanner.data_ring_[ring_idx];
    item.leaf_pin_idx_ = offset;
    item.leaf_pin_generation_ = scanner.leaf_pin_slots_[offset].generation_;
    scanner.leaf_pin_slots_[offset].ref_cnt_ = 1;
    item.index_info_.agg_row_buf_ = 0 == offset ? buf0 : buf1;
    item.index_info_.agg_buf_size_ = 0 == offset ? size0 : size1;
    item.max_doc_id_ = 0 == offset ? 10 : 20;
  }
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true /* 包含最大分数 */));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf0, size0));

  ObSEArray<uint64_t, 2> visited_max_ids;
  uint64_t stop_after_max_id = UINT64_MAX;
  ObTestPostingBlockMetaVisitor block_meta_visitor(visited_max_ids, stop_after_max_id);
  common::ObFunction<int(const ObPostingBlockMeta &, bool &)> visitor(
      block_meta_visitor);
  bool range_covered = false;

  // A single pinned row covers the requested boundary.
  scanner.data_prefetch_idx_ = 8;
  scanner.data_prefetch_depth_ = 1;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      10, visitor, range_covered));
  EXPECT_TRUE(range_covered);
  ASSERT_EQ(1, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));

  // The visitor crosses pinned rows when the requested boundary reaches the second row.
  visited_max_ids.reuse();
  scanner.data_prefetch_idx_ = 9;
  scanner.data_prefetch_depth_ = 2;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      20, visitor, range_covered));
  EXPECT_TRUE(range_covered);
  ASSERT_EQ(2, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));
  EXPECT_EQ(20, visited_max_ids.at(1));

  // 预取窗口无法完整覆盖目标边界时，在调用 block visitor 前直接返回。
  visited_max_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      21, visitor, range_covered));
  EXPECT_FALSE(range_covered);
  EXPECT_EQ(0, visited_max_ids.count());

  // Reaching the term end completes a range even when its boundary is beyond all postings.
  visited_max_ids.reuse();
  scanner.idx_iter_end_ = true;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      21, visitor, range_covered));
  EXPECT_TRUE(range_covered);
  EXPECT_EQ(2, visited_max_ids.count());

  // The callback may stop after any visited block without exposing the ring position.
  visited_max_ids.reuse();
  scanner.idx_iter_end_ = false;
  stop_after_max_id = 10;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      20, visitor, range_covered));
  EXPECT_FALSE(range_covered);
  ASSERT_EQ(1, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));
  EXPECT_EQ(7, scanner.data_read_idx_);
  EXPECT_EQ(9, scanner.data_prefetch_idx_);
  scanner.data_read_idx_ = scanner.data_prefetch_idx_ = 0;
  scanner.is_inited_ = false;
}

TEST_F(TestSSTablePostingScannerValue, visit_prefetched_block_meta_owns_cursor_and_order)
{
  ObArenaAllocator allocator;
  ObSEArray<ObInvIdxLogicalBlockMeta, 2> first_row_metas;
  ObInvIdxLogicalBlockMeta meta;
  meta.block_idx_ = 0;
  meta.doc_count_ = 1;
  meta.min_doc_id_ = 1;
  meta.max_doc_id_ = 5;
  ASSERT_EQ(OB_SUCCESS, first_row_metas.push_back(meta));
  meta.block_idx_ = 1;
  meta.min_doc_id_ = 6;
  meta.max_doc_id_ = 10;
  ASSERT_EQ(OB_SUCCESS, first_row_metas.push_back(meta));

  char *buf0 = nullptr;
  char *buf1 = nullptr;
  char *overlap_buf = nullptr;
  int64_t size0 = 0;
  int64_t size1 = 0;
  int64_t overlap_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
      allocator, 0, 0, first_row_metas, buf0, size0));
  ASSERT_EQ(OB_SUCCESS, build_agg_buf_with_range(
      allocator, 0, 0, 11, 20, buf1, size1));
  ASSERT_EQ(OB_SUCCESS, build_agg_buf_with_range(
      allocator, 0, 0, 10, 20, overlap_buf, overlap_size));

  ObSSTablePostingScanner scanner;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.data_read_idx_ = 0;
  scanner.data_prefetch_idx_ = 2;
  scanner.data_prefetch_depth_ = 2;
  for (int64_t offset = 0; offset < 2; ++offset) {
    ObSSTablePostingScanner::DataPrefetchItem &item = scanner.data_ring_[offset];
    item.leaf_pin_idx_ = offset;
    item.leaf_pin_generation_ = scanner.leaf_pin_slots_[offset].generation_;
    scanner.leaf_pin_slots_[offset].ref_cnt_ = 1;
    item.index_info_.agg_row_buf_ = 0 == offset ? buf0 : buf1;
    item.index_info_.agg_buf_size_ = 0 == offset ? size0 : size1;
    item.max_doc_id_ = 0 == offset ? 10 : 20;
  }
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true /* 包含最大分数 */));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf0, size0));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.next_block());

  ObSEArray<uint64_t, 2> visited_max_ids;
  const uint64_t stop_after_max_id = UINT64_MAX;
  ObTestPostingBlockMetaVisitor block_meta_visitor(visited_max_ids, stop_after_max_id);
  common::ObFunction<int(const ObPostingBlockMeta &, bool &)> visitor(
      block_meta_visitor);
  bool range_covered = false;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_block_meta(
      20, visitor, range_covered));
  EXPECT_TRUE(range_covered);
  ASSERT_EQ(2, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));
  EXPECT_EQ(20, visited_max_ids.at(1));

  // scanner 在向 visitor 暴露后续 block 前拒绝跨 metadata row 的重叠并返回错误。
  ObSSTablePostingScanner::DataPrefetchItem &second_item = scanner.data_ring_[1];
  second_item.index_info_.agg_row_buf_ = overlap_buf;
  second_item.index_info_.agg_buf_size_ = overlap_size;
  visited_max_ids.reuse();
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.visit_prefetched_block_meta(
      20, visitor, range_covered));
  EXPECT_FALSE(range_covered);
  ASSERT_EQ(1, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));

  // agg metadata 末端与 index row endkey 不一致时返回错误。
  second_item.index_info_.agg_row_buf_ = buf1;
  second_item.index_info_.agg_buf_size_ = size1;
  second_item.max_doc_id_ = 21;
  visited_max_ids.reuse();
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.visit_prefetched_block_meta(
      21, visitor, range_covered));
  EXPECT_FALSE(range_covered);
  ASSERT_EQ(2, visited_max_ids.count());
  EXPECT_EQ(10, visited_max_ids.at(0));
  EXPECT_EQ(20, visited_max_ids.at(1));

  scanner.data_read_idx_ = scanner.data_prefetch_idx_ = 0;
  scanner.is_inited_ = false;
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
