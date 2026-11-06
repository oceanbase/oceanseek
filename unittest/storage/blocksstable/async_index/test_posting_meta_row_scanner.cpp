/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#define private public
#include "storage/blocksstable/async_index/ob_posting_meta_row_scanner.h"
#undef private

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

class PostingMetaRowScannerTest : public ::testing::Test
{
protected:
  PostingMetaRowScannerTest() {}
  virtual ~PostingMetaRowScannerTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  // Build a 3-block leaf row agg buffer. block i has:
  //   doc_count = 5 + i, min = 10 + 100*i, max = 50 + 100*i,
  //   frontier  = i+1 pairs {{tf=i*10+1, dl=i*100+1}, ...}
  void build_agg(
      ObIAllocator &allocator,
      const int64_t value_offset,
      const int64_t value_size,
      char *&buf,
      int64_t &size)
  {
    ObSEArray<ObInvIdxBM25ParamPair, 4> pairs0;
    ObSEArray<ObInvIdxBM25ParamPair, 4> pairs1;
    ObSEArray<ObInvIdxBM25ParamPair, 4> pairs2;
    ASSERT_EQ(OB_SUCCESS, pairs0.push_back(ObInvIdxBM25ParamPair{1, 101}));
    ASSERT_EQ(OB_SUCCESS, pairs1.push_back(ObInvIdxBM25ParamPair{11, 201}));
    ASSERT_EQ(OB_SUCCESS, pairs1.push_back(ObInvIdxBM25ParamPair{12, 202}));
    ASSERT_EQ(OB_SUCCESS, pairs2.push_back(ObInvIdxBM25ParamPair{21, 301}));
    ASSERT_EQ(OB_SUCCESS, pairs2.push_back(ObInvIdxBM25ParamPair{22, 302}));
    ASSERT_EQ(OB_SUCCESS, pairs2.push_back(ObInvIdxBM25ParamPair{23, 303}));

    ObSEArray<ObInvIdxLogicalBlockMeta, 4> metas;
    for (int64_t i = 0; i < 3; ++i) {
      ObInvIdxLogicalBlockMeta meta;
      meta.block_idx_ = i;
      meta.doc_count_ = 5 + i;
      meta.min_doc_id_ = 10 + 100 * i;
      meta.max_doc_id_ = 50 + 100 * i;
      const ObSEArray<ObInvIdxBM25ParamPair, 4> &pairs =
          (0 == i) ? pairs0 : ((1 == i) ? pairs1 : pairs2);
      ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.assign(pairs));
      ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    }
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
        allocator, value_offset, value_size, metas, buf, size));
    ASSERT_NE(nullptr, buf);
    ASSERT_GT(size, 0);
  }

  void build_monotonic_agg(
      ObIAllocator &allocator,
      const int64_t block_count,
      char *&buf,
      int64_t &size)
  {
    ObSEArray<ObInvIdxLogicalBlockMeta, 16> metas;
    ObSEArray<ObInvIdxBM25ParamPair, 1> pairs;
    ASSERT_EQ(OB_SUCCESS, pairs.push_back(ObInvIdxBM25ParamPair{1, 1}));
    for (int64_t i = 0; i < block_count; ++i) {
      ObInvIdxLogicalBlockMeta meta;
      meta.block_idx_ = i;
      meta.doc_count_ = 1;
      meta.min_doc_id_ = 100 * i;
      meta.max_doc_id_ = 100 * i + 50;
      ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.assign(pairs));
      ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    }
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
        allocator, 1024, 512, metas, buf, size));
    ASSERT_NE(nullptr, buf);
    ASSERT_GT(size, 0);
  }
};

TEST_F(PostingMetaRowScannerTest, test_not_inited)
{
  ObPostingMetaRowScanner scanner;
  ObPostingBlockMeta meta;
  bool beyond = false;
  ASSERT_EQ(OB_NOT_INIT, scanner.open(reinterpret_cast<const char *>(0x1), 16));
  ASSERT_EQ(OB_NOT_INIT, scanner.get_cur_block_meta(meta));
  ASSERT_EQ(OB_NOT_INIT, scanner.next_block());
  ASSERT_EQ(OB_NOT_INIT, scanner.advance_to(1, beyond));
}

TEST_F(PostingMetaRowScannerTest, cached_meta_matches_row_local_cursor_and_fields)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);
  ObInvIdxAggDataEntry *entries = reinterpret_cast<ObInvIdxAggDataEntry *>(
      buf + sizeof(ObInvIdxAggDataHeader));
  for (int64_t i = 0; i < 3; ++i) {
    entries[i].block_idx_ = static_cast<uint16_t>(10 + i);
  }
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingBlockMeta cached;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  EXPECT_EQ(0, cached.idx_);
  EXPECT_EQ(10, cached.locator_.block_idx_);
  bool matched = false;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_TRUE(matched);
  for (int field = 0; field < 10; ++field) {
    ObPostingBlockMeta other = cached;
    switch (field) {
      case 0: ++other.idx_; break;
      case 1: ++other.min_id_; break;
      case 2: ++other.max_id_; break;
      case 3: ++other.doc_count_; break;
      case 4: ++other.locator_.block_idx_; break;
      case 5: ++other.locator_.value_micro_offset_; break;
      case 6: ++other.locator_.value_micro_size_; break;
      case 7: ++other.block_max_.pair_count_; break;
      case 8: ++other.block_max_.pairs_; break;
      case 9: other.reset(); break;
    }
    matched = true;
    ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, other, 10, 50, matched));
    EXPECT_FALSE(matched) << field;
    EXPECT_EQ(0, scanner.get_cur_block_idx());
  }
  ASSERT_EQ(OB_SUCCESS, scanner.next_block());
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 110, 150, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 110, 150, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(1, scanner.get_cur_block_idx());
  EXPECT_EQ(11, cached.locator_.block_idx_);
}

TEST_F(PostingMetaRowScannerTest, cached_meta_clears_match_after_eof_reuse_and_rebind)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);
  ObPostingMetaRowScanner scanner;
  ObPostingBlockMeta cached;
  bool matched = true;
  EXPECT_EQ(OB_NOT_INIT, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  matched = true;
  EXPECT_EQ(OB_NOT_INIT, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  char *other_buf = nullptr;
  int64_t other_size = 0;
  build_agg(allocator, 4096, 2048, other_buf, other_size);
  matched = true;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  EXPECT_EQ(0, scanner.get_cur_block_idx());
  ObInvIdxAggDataHeader *other_header = reinterpret_cast<ObInvIdxAggDataHeader *>(other_buf);
  ++other_header->version_;
  matched = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  --other_header->version_;
  ASSERT_EQ(OB_SUCCESS, scanner.open(other_buf, other_size));
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_TRUE(matched);
  bool beyond = false;
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(UINT64_MAX, beyond));
  EXPECT_TRUE(beyond);
  matched = true;
  EXPECT_EQ(OB_ITER_END, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  scanner.reuse();
  matched = true;
  EXPECT_EQ(OB_NOT_INIT, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  scanner.reset();
  matched = true;
  EXPECT_EQ(OB_NOT_INIT, scanner.match_cached_block_meta(other_buf, other_size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
}

TEST_F(PostingMetaRowScannerTest, cached_meta_revalidates_layout_before_matching)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingBlockMeta cached;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  bool matched = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.match_cached_block_meta(nullptr, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  matched = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.match_cached_block_meta(buf, 0, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  matched = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.match_cached_block_meta(buf, size, cached, 51, 50, matched));
  EXPECT_FALSE(matched);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);
  const ObInvIdxAggDataHeader original = *header;
  for (int field = 0; field < 5; ++field) {
    switch (field) {
      case 0: ++header->version_; break;
      case 1: header->logical_block_count_ = 0; break;
      case 2: header->total_size_ = sizeof(ObInvIdxAggDataHeader); break;
      case 3: header->total_size_ = size + 1; break;
      case 4: --header->total_size_; break;
    }
    matched = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched)) << field;
    EXPECT_FALSE(matched);
    ObPostingMetaRowScanner fresh;
    ASSERT_EQ(OB_SUCCESS, fresh.init(true));
    EXPECT_EQ(OB_ERR_UNEXPECTED, fresh.open(buf, size)) << field;
    *header = original;
  }
  --header->logical_block_count_;
  header->total_size_ -= sizeof(ObInvIdxAggDataEntry);
  matched = true;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  *header = original;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(0, scanner.get_cur_block_idx());
}

TEST_F(PostingMetaRowScannerTest, cached_meta_only_checks_intersecting_frontier)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingBlockMeta cached;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  ObInvIdxAggDataEntry *entries = reinterpret_cast<ObInvIdxAggDataEntry *>(
      buf + sizeof(ObInvIdxAggDataHeader));
  const ObInvIdxAggDataEntry original = entries[0];
  entries[0].frontier_offset_ = UINT16_MAX;
  entries[2].frontier_offset_ = UINT16_MAX;
  bool matched = false;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 0, 9, matched));
  EXPECT_TRUE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 110, 150, matched));
  EXPECT_TRUE(matched);
  ObPostingFrontierSpan span;
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 110, 150, span));
  EXPECT_TRUE(span.score_available_);
  EXPECT_EQ(2, span.pair_count_);
  matched = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  entries[0] = original;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_TRUE(matched);
  entries[0].frontier_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(cached));
  EXPECT_FALSE(cached.block_max_.has_max_score());
  ASSERT_EQ(OB_SUCCESS, scanner.match_cached_block_meta(buf, size, cached, 10, 50, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(0, scanner.get_cur_block_idx());
}

TEST_F(PostingMetaRowScannerTest, test_iterate_with_max_score)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true /*has_max_score*/));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ASSERT_EQ(3, scanner.header_->logical_block_count_);

  // indexed peek 读取第三个 block 后，普通 cursor 仍指向第一个 block。
  ObPostingBlockMeta peek_meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_block_meta_at(2, peek_meta));
  EXPECT_EQ(250u, peek_meta.max_id_);
  EXPECT_EQ(2, peek_meta.idx_);
  ObPostingBlockMeta still_first;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(still_first));
  EXPECT_EQ(0, still_first.idx_);
  EXPECT_EQ(OB_ITER_END, scanner.get_block_meta_at(3, peek_meta));

  for (int64_t i = 0; i < 3; ++i) {
    ObPostingBlockMeta meta;
    ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(meta));
    EXPECT_EQ(static_cast<uint64_t>(10 + 100 * i), meta.min_id_);
    EXPECT_EQ(static_cast<uint64_t>(50 + 100 * i), meta.max_id_);
    EXPECT_EQ(5 + i, meta.doc_count_);
    EXPECT_EQ(i, meta.idx_);
    EXPECT_EQ(i, meta.locator_.block_idx_);
    EXPECT_EQ(4096u, meta.locator_.value_micro_offset_);
    EXPECT_EQ(2048u, meta.locator_.value_micro_size_);
    EXPECT_EQ(i + 1, meta.block_max_.pair_count_);
    ASSERT_NE(nullptr, meta.block_max_.pairs_);
    EXPECT_EQ(static_cast<uint32_t>(i * 10 + 1), meta.block_max_.pairs_[0].token_freq_);
    EXPECT_EQ(static_cast<uint32_t>(i * 100 + 101), meta.block_max_.pairs_[0].doc_length_);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, scanner.next_block());
    } else {
      ASSERT_EQ(OB_ITER_END, scanner.next_block());
    }
  }
}

TEST_F(PostingMetaRowScannerTest, test_iterate_without_max_score)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 1, 1, buf, size);

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(false /*has_max_score*/));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(meta));
  EXPECT_FALSE(meta.block_max_.has_max_score());
  EXPECT_EQ(nullptr, meta.block_max_.pairs_);
  EXPECT_EQ(0, meta.block_max_.pair_count_);
}

TEST_F(PostingMetaRowScannerTest, test_advance_to_basic)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size); // max_doc_id = [50, 150, 250]

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));

  bool beyond = false;
  // target within first block range
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(5, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);

  // target lands on second block (first max_doc_id >= 120 is 150 at idx 1)
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(120, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(1, scanner.cur_idx_);

  // exactly equal to a block max_doc_id (250) -> idx 2
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(250, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(2, scanner.cur_idx_);

  // beyond the last block
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(1000, beyond));
  EXPECT_TRUE(beyond);
}

TEST_F(PostingMetaRowScannerTest, test_advance_to)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size); // max_doc_id = [50, 150, 250]

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));

  bool beyond = false;
  // hit in current block, cursor stays.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(20, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);

  // move forward to the next block.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(120, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(1, scanner.cur_idx_);

  // forward-only semantics: smaller target does not move backward.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(30, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(1, scanner.cur_idx_);

  // exact hit on third block.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(250, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(2, scanner.cur_idx_);

  // beyond end.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(1000, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner.header_->logical_block_count_, scanner.cur_idx_);

  // iter-end like state should keep beyond=true.
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(1001, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner.header_->logical_block_count_, scanner.cur_idx_);
}

TEST_F(PostingMetaRowScannerTest, test_advance_to_galloping_window)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_monotonic_agg(allocator, 8, buf, size); // max_doc_id = [50, 150, ..., 750]

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));

  bool beyond = false;
  // current block fast path
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(40, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);

  // next block fast path
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(120, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(1, scanner.cur_idx_);

  // jump across multiple blocks, should land at idx 6 (max_doc_id = 650)
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(620, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(6, scanner.cur_idx_);

  // beyond end and stable on repeated calls
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(2000, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner.header_->logical_block_count_, scanner.cur_idx_);
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(2001, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner.header_->logical_block_count_, scanner.cur_idx_);
}

TEST_F(PostingMetaRowScannerTest, test_locate_block)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObInvIdxMicroBlockWriter writer;
  const char b0[] = "AAA";
  const char b1[] = "BBBBB";
  const char b2[] = "CC";
  int64_t block_idx = -1;
  const char *payload = nullptr;
  int64_t payload_size = 0;
  char *micro_block = nullptr;
  int64_t micro_block_size = 0;

  ASSERT_EQ(OB_SUCCESS, writer.init(allocator));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(b0, 3, block_idx));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(b1, 5, block_idx));
  ASSERT_EQ(OB_SUCCESS, writer.append_logical_block(b2, 2, block_idx));
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block(payload, payload_size));
  ASSERT_GT(payload_size, 0);
  ObMicroBlockHeader micro_header;
  int64_t header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_header.init(
      1 /*data_version*/, 1 /*column_count*/, 0 /*rowkey_column_count*/,
      common::INV_IDX_ROW_STORE, false /*has_column_checksum*/));
  micro_header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
  micro_header.data_length_ = static_cast<int32_t>(payload_size);
  micro_header.data_zlength_ = static_cast<int32_t>(payload_size);
  micro_header.original_length_ = static_cast<int32_t>(payload_size);
  micro_header.data_checksum_ = ob_crc64_sse42(0, payload, payload_size);
  micro_header.set_header_checksum();
  micro_block_size = static_cast<int64_t>(micro_header.header_size_) + payload_size;
  micro_block = static_cast<char *>(allocator.alloc(micro_block_size));
  ASSERT_NE(nullptr, micro_block);
  ASSERT_EQ(OB_SUCCESS, micro_header.serialize(micro_block, micro_block_size, header_pos));
  ASSERT_EQ(micro_header.header_size_, header_pos);
  MEMCPY(micro_block + header_pos, payload, payload_size);
  ObInvIdxMicroBlockReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(micro_block, micro_block_size, false /*is_posting_value*/));

  const char *block_buf = nullptr;
  int64_t block_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.get_payload(0, block_buf, block_size));
  ASSERT_EQ(3, block_size);
  EXPECT_EQ(0, MEMCMP(b0, block_buf, 3));

  ASSERT_EQ(OB_SUCCESS, reader.get_payload(1, block_buf, block_size));
  ASSERT_EQ(5, block_size);
  EXPECT_EQ(0, MEMCMP(b1, block_buf, 5));

  ASSERT_EQ(OB_SUCCESS, reader.get_payload(2, block_buf, block_size));
  ASSERT_EQ(2, block_size);
  EXPECT_EQ(0, MEMCMP(b2, block_buf, 2));

  // out of range index
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader.get_payload(3, block_buf, block_size));
}

TEST_F(PostingMetaRowScannerTest, test_init_twice)
{
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_INIT_TWICE, scanner.init(true));
}

TEST_F(PostingMetaRowScannerTest, test_accessors_before_open)
{
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  EXPECT_FALSE(scanner.is_opened_);
  EXPECT_EQ(nullptr, scanner.header_);
  EXPECT_EQ(0, scanner.cur_idx_);
}

TEST_F(PostingMetaRowScannerTest, test_open_invalid_buffer)
{
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  // null buffer
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.open(nullptr, 16));
  // buffer smaller than the agg data header
  char small[8] = {0};
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.open(small, 8));
}

TEST_F(PostingMetaRowScannerTest, test_open_bad_version)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);
  // corrupt the version byte
  buf[0] = static_cast<char>(0xFF);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.open(buf, size));
}

TEST_F(PostingMetaRowScannerTest, test_open_bad_layout)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size); // 3 entries, frontier pool = 6 pairs = 48B

  // total_size_ larger than the provided agg size -> OB_INVALID_DATA
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf);
  const uint32_t saved_total = header->total_size_;
  header->total_size_ = static_cast<uint32_t>(size + 10);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.open(buf, size));

  // frontier_pool_size not a multiple of PAIR_SIZE -> OB_INVALID_DATA.
  // Bump total_size_ by 1 and pass an agg_size that covers it.
  header->total_size_ = static_cast<uint32_t>(size + 1);
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.open(buf, size + 1));
  header->total_size_ = saved_total;
}

TEST_F(PostingMetaRowScannerTest, test_single_block_next_end)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_monotonic_agg(allocator, 1, buf, size); // 1 block, max_doc_id = 50

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  EXPECT_EQ(1, scanner.header_->logical_block_count_);
  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(meta));
  // only one block: next_block immediately exhausts
  ASSERT_EQ(OB_ITER_END, scanner.next_block());
  // cursor past end: get_cur_block_meta reports iter end
  ASSERT_EQ(OB_ITER_END, scanner.get_cur_block_meta(meta));
}

TEST_F(PostingMetaRowScannerTest, test_advance_to_single_block)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_monotonic_agg(allocator, 1, buf, size); // max_doc_id = [50]

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));

  bool beyond = false;
  // within the only block: cursor stays at 0
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(10, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);
  // exact max_doc_id: still the first block
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(50, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);
  // beyond the only block
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(51, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner.header_->logical_block_count_, scanner.cur_idx_);
}

TEST_F(PostingMetaRowScannerTest, test_advance_to_target_zero_and_last_block)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size); // max_doc_id = [50, 150, 250]

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  bool beyond = false;
  // target 0 is <= first block max_doc_id, fast path stays at idx 0
  ASSERT_EQ(OB_SUCCESS, scanner.advance_to(0, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(0, scanner.cur_idx_);

  // galloping path landing exactly on the last block
  common::ObArenaAllocator allocator2(common::ObModIds::TEST);
  char *buf2 = nullptr;
  int64_t size2 = 0;
  build_monotonic_agg(allocator2, 8, buf2, size2); // max_doc_id = [50, 150, ..., 750]
  ObPostingMetaRowScanner scanner2;
  ASSERT_EQ(OB_SUCCESS, scanner2.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner2.open(buf2, size2));
  ASSERT_EQ(OB_SUCCESS, scanner2.advance_to(750, beyond));
  EXPECT_FALSE(beyond);
  EXPECT_EQ(7, scanner2.cur_idx_);
  // one past the last block max_doc_id -> beyond
  ASSERT_EQ(OB_SUCCESS, scanner2.advance_to(751, beyond));
  EXPECT_TRUE(beyond);
  EXPECT_EQ(scanner2.header_->logical_block_count_, scanner2.cur_idx_);
}

TEST_F(PostingMetaRowScannerTest, test_frontier_slice_out_of_pool)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size); // frontier pool = 48B, entry0 has 1 pair

  // Corrupt entry0.frontier_offset_ (uint16 at entry_start + 4) so its frontier slice
  // runs past the frontier pool. Entry layout: block_idx_(u16), frontier_count_(u16),
  // frontier_offset_(u16), ... so frontier_offset_ sits at byte 4 of the entry.
  const int64_t header_size = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));
  const int64_t entry0_frontier_offset_pos = header_size + 2 * sizeof(uint16_t);
  uint16_t bad_offset = 0xFFFF;
  MEMCPY(buf + entry0_frontier_offset_pos, &bad_offset, sizeof(uint16_t));

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true /*has_max_score*/));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.get_cur_block_meta(meta));
}

TEST_F(PostingMetaRowScannerTest, test_reuse_and_reopen)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf1 = nullptr;
  int64_t size1 = 0;
  build_agg(allocator, 4096, 2048, buf1, size1); // 3 blocks
  char *buf2 = nullptr;
  int64_t size2 = 0;
  build_monotonic_agg(allocator, 2, buf2, size2); // 2 blocks

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf1, size1));
  EXPECT_EQ(3, scanner.header_->logical_block_count_);
  ASSERT_EQ(OB_SUCCESS, scanner.next_block());
  ASSERT_EQ(OB_SUCCESS, scanner.next_block());

  // reuse() drops the borrowed buffer; reopen on a different agg buffer resets the cursor.
  scanner.reuse();
  EXPECT_FALSE(scanner.is_opened_);
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf2, size2));
  EXPECT_EQ(2, scanner.header_->logical_block_count_);
  EXPECT_EQ(0, scanner.cur_idx_);
  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(meta));
  EXPECT_EQ(0, meta.idx_);
}

TEST_F(PostingMetaRowScannerTest, test_reset_then_reinit)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 4096, 2048, buf, size);

  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ASSERT_EQ(3, scanner.header_->logical_block_count_);
  scanner.reset();
  EXPECT_FALSE(scanner.is_opened_);
  // re-init and re-open after reset()
  ASSERT_EQ(OB_SUCCESS, scanner.init(false));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  EXPECT_EQ(3, scanner.header_->logical_block_count_);
  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(meta));
  EXPECT_FALSE(meta.block_max_.has_max_score());
}

TEST_F(PostingMetaRowScannerTest, frontier_span_preserves_closed_range_and_cursor)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 0, 0, buf, size);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObPostingFrontierSpan span;
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 10, 149, span));
  EXPECT_TRUE(span.range_covered_);
  EXPECT_TRUE(span.score_available_);
  EXPECT_EQ(3, span.pair_count_);
  EXPECT_EQ(2, span.visited_block_count_);
  EXPECT_EQ(1, span.pairs_[0].token_freq_);
  EXPECT_EQ(12, span.pairs_[2].token_freq_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 51, 209, span));
  EXPECT_TRUE(span.range_covered_);
  EXPECT_EQ(2, span.pair_count_);
  EXPECT_EQ(11, span.pairs_[0].token_freq_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 151, 209, span));
  EXPECT_TRUE(span.range_covered_);
  EXPECT_EQ(0, span.pair_count_);
  EXPECT_EQ(nullptr, span.pairs_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(1, 110, 250, span));
  EXPECT_EQ(5, span.pair_count_);
  EXPECT_TRUE(span.range_covered_);
  EXPECT_EQ(0, scanner.get_cur_block_idx());
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 10, UINT64_MAX, span));
  EXPECT_FALSE(span.range_covered_); // only the outer scanner can certify term EOF
  EXPECT_EQ(6, span.pair_count_);
  EXPECT_EQ(250, span.last_max_id_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.get_frontier_span(0, 251, 250, span));
  EXPECT_EQ(nullptr, span.pairs_);
  ObInvIdxAggDataEntry *entries = const_cast<ObInvIdxAggDataEntry *>(scanner.entries_);
  entries[2].min_doc_id_ = UINT64_MAX - 1;
  entries[2].max_doc_id_ = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, UINT64_MAX, UINT64_MAX, span));
  EXPECT_TRUE(span.range_covered_);
  EXPECT_EQ(3, span.pair_count_);
  EXPECT_EQ(UINT64_MAX, span.last_max_id_);
}

TEST_F(PostingMetaRowScannerTest, frontier_span_rejects_gaps_overlaps_and_reverse_offsets)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 0, 0, buf, size);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObInvIdxAggDataEntry *entries = const_cast<ObInvIdxAggDataEntry *>(scanner.entries_);
  const int64_t indices[] = {1, 2, 2};
  const uint16_t offsets[] = {2 * sizeof(ObInvIdxBM25ParamPair), 2 * sizeof(ObInvIdxBM25ParamPair), 0};
  const uint64_t first_ids[] = {10, 10, 110};
  for (int64_t i = 0; i < ARRAYSIZEOF(indices); ++i) {
    SCOPED_TRACE(i);
    const uint16_t original_offset = entries[indices[i]].frontier_offset_;
    entries[indices[i]].frontier_offset_ = offsets[i];
    ObPostingFrontierSpan span;
    EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_frontier_span(0, first_ids[i], 250, span));
    EXPECT_EQ(nullptr, span.pairs_);
    EXPECT_EQ(0, scanner.get_cur_block_idx());
    entries[indices[i]].frontier_offset_ = original_offset;
    ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, first_ids[i], 250, span));
    EXPECT_TRUE(span.range_covered_);
    EXPECT_TRUE(span.score_available_);
  }
}

TEST_F(PostingMetaRowScannerTest, frontier_span_missing_discontiguous_and_corrupt_entries)
{
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t size = 0;
  build_agg(allocator, 0, 0, buf, size);
  ObPostingMetaRowScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.open(buf, size));
  ObInvIdxAggDataEntry *entries = const_cast<ObInvIdxAggDataEntry *>(scanner.entries_);
  const ObInvIdxAggDataEntry original = entries[1];
  ObPostingFrontierSpan span;
  entries[1].frontier_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 10, 250, span));
  EXPECT_FALSE(span.score_available_);
  entries[1] = original;
  entries[1].frontier_offset_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_frontier_span(0, 10, 250, span));
  entries[1] = original;
  entries[1].frontier_offset_ = UINT16_MAX;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_frontier_span(0, 10, 250, span));
  entries[1] = original;
  entries[1].min_doc_id_ = entries[0].max_doc_id_;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_frontier_span(0, 10, 250, span));
  entries[1] = original;
  entries[1].doc_count_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.get_frontier_span(0, 10, 250, span));
  entries[1] = original;
  scanner.has_max_score_ = false;
  ASSERT_EQ(OB_SUCCESS, scanner.get_frontier_span(0, 10, 250, span));
  EXPECT_FALSE(span.score_available_);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
