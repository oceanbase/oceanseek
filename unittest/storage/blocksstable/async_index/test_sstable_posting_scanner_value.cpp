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
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "storage/blocksstable/async_index/ob_sstable_posting_scanner.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#undef private

#include <gtest/gtest.h>
#include <cmath>
#include <algorithm>
#include <limits>

namespace oceanbase
{
namespace blocksstable
{

class ObTestFrontierMaxRecorder final
{
public:
  explicit ObTestFrontierMaxRecorder(const sql::BM25FrontierMaxKernels &kernels)
    : kernels_(kernels), calls_(0), pairs_(nullptr), pair_count_(0)
  {
    current_ = this;
  }
  ~ObTestFrontierMaxRecorder() { current_ = nullptr; }

  void reset()
  {
    calls_ = 0;
    pairs_ = nullptr;
    pair_count_ = 0;
  }

  static double record(const ObInvIdxBM25ParamPair *pairs, const int64_t count,
      const sql::ObBM25PrecomputedParams &params, const double maximum)
  {
    double result = maximum;
    EXPECT_NE(nullptr, current_);
    if (nullptr != current_) {
      ++current_->calls_;
      current_->pairs_ = pairs;
      current_->pair_count_ = count;
      result = current_->kernels_.max_score(pairs, count, params, maximum);
    }
    return result;
  }

  sql::BM25FrontierMaxKernels kernels_;
  int64_t calls_;
  const ObInvIdxBM25ParamPair *pairs_;
  int64_t pair_count_;
  static ObTestFrontierMaxRecorder *current_;
};

ObTestFrontierMaxRecorder *ObTestFrontierMaxRecorder::current_ = nullptr;

class TestSSTablePostingScannerValue : public ::testing::Test
{
protected:
  static double reference_bm25_score(
      const uint64_t token_freq, const uint64_t doc_length,
      const sql::ObBM25PrecomputedParams &params)
  {
    const double tf = static_cast<double>(token_freq);
    volatile double biased_tf = tf + params.denom_bias_;
    volatile double scaled_dl = params.doc_len_scale_ * static_cast<double>(doc_length);
    volatile double denom = biased_tf + scaled_dl;
    volatile double weighted = params.query_weight_ * (tf / denom);
    volatile double result = weighted * params.score_multiplier_;
    return result;
  }

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

TEST_F(TestSSTablePostingScannerValue, cached_range_check_preserves_errors_and_frontier_scope)
{
  ObArenaAllocator allocator;
  ObSEArray<ObInvIdxLogicalBlockMeta, 3> metas;
  for (int64_t i = 0; i < 3; ++i) {
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = 10 + i;
    meta.doc_count_ = 10;
    meta.min_doc_id_ = 10 + 10 * i;
    meta.max_doc_id_ = 19 + 10 * i;
    ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(ObInvIdxBM25ParamPair{1, 100}));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  }
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 0, 0, metas, buf, size));
  ObInvSSTablePostingIter posting;
  ObSSTablePostingScanner &scanner = posting.scanner_;
  bool reusable = true;
  EXPECT_EQ(OB_NOT_INIT, scanner.check_cached_range_meta(posting.curr_meta_, 10, 19, reusable));
  EXPECT_FALSE(reusable);
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.idx_iter_end_ = true;
  scanner.data_read_idx_ = 0;
  scanner.data_prefetch_idx_ = 1;
  auto &item = scanner.data_ring_[0];
  item.max_doc_id_ = 39;
  item.index_info_.agg_row_buf_ = buf;
  item.index_info_.agg_buf_size_ = size;
  item.leaf_pin_idx_ = 0;
  item.leaf_pin_generation_ = scanner.leaf_pin_slots_[0].generation_;
  scanner.leaf_pin_slots_[0].ref_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf, size));
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(posting.curr_meta_));
  posting.is_opened_ = true;
  posting.need_relevance_params_ = true;
  posting.bm25_frontier_kernels_ = sql::get_bm25_frontier_max_kernels();
  ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
      1.0, 100.0, 1.0, posting.bm25_calc_params_));
  ASSERT_EQ(OB_SUCCESS, scanner.check_cached_range_meta(posting.curr_meta_, 10, 19, reusable));
  EXPECT_TRUE(reusable);
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  ObPostingBlockMeta other = posting.curr_meta_;
  ++other.doc_count_;
  ASSERT_EQ(OB_SUCCESS, scanner.check_cached_range_meta(other, 10, 19, reusable));
  EXPECT_FALSE(reusable);
  item.max_doc_id_ = 9;
  reusable = true;
  ASSERT_EQ(OB_SUCCESS, scanner.check_cached_range_meta(posting.curr_meta_, 10, 19, reusable));
  EXPECT_FALSE(reusable);
  item.max_doc_id_ = 39;

  ObInvIdxAggDataEntry *entries = reinterpret_cast<ObInvIdxAggDataEntry *>(buf + sizeof(ObInvIdxAggDataHeader));
  entries[0].frontier_offset_ = UINT16_MAX;
  entries[2].frontier_offset_ = UINT16_MAX;
  double actual = -99.0;
  bool can_calc = false;
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(0, true, 9, actual, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(0.0, actual);
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  const double expected = reference_bm25_score(1, 100, posting.bm25_calc_params_);
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(19, false, 29, actual, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  posting.curr_block_max_score_ = expected;
  posting.curr_block_max_score_valid_ = true;
  actual = -99.0;
  can_calc = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(10, true, 19, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-99.0, actual);
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  entries[0].frontier_offset_ = 0;
  entries[2].frontier_offset_ = 2 * sizeof(ObInvIdxBM25ParamPair);

  char *replacement = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, replacement);
  MEMCPY(replacement, buf, size);
  ObInvIdxBM25ParamPair *pairs = reinterpret_cast<ObInvIdxBM25ParamPair *>(replacement
      + sizeof(ObInvIdxAggDataHeader) + 3 * sizeof(ObInvIdxAggDataEntry));
  pairs[0].token_freq_ = 8;
  item.index_info_.agg_row_buf_ = replacement;
  posting.curr_block_max_score_ = expected;
  posting.curr_block_max_score_valid_ = true;
  ASSERT_EQ(OB_SUCCESS, scanner.check_cached_range_meta(posting.curr_meta_, 10, 19, reusable));
  EXPECT_FALSE(reusable);
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 19, actual, can_calc));
  EXPECT_TRUE(can_calc);
  const double replacement_expected = reference_bm25_score(8, 100, posting.bm25_calc_params_);
  EXPECT_EQ(0, MEMCMP(&replacement_expected, &actual, sizeof(double)));
  item.index_info_.agg_row_buf_ = buf;

  ++item.leaf_pin_generation_;
  reusable = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.check_cached_range_meta(posting.curr_meta_, 10, 19, reusable));
  EXPECT_FALSE(reusable);
  actual = -99.0;
  can_calc = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(0, true, 9, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-99.0, actual);
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  --item.leaf_pin_generation_;
  EXPECT_EQ(0, scanner.data_read_idx_);
  EXPECT_EQ(0, scanner.meta_row_scanner_.get_cur_block_idx());
  EXPECT_FALSE(posting.curr_block_loaded_);
  posting.reset();
}

TEST_F(TestSSTablePostingScannerValue, range_score_batches_cold_cache_and_reuses_valid_zero)
{
  ObArenaAllocator allocator;
  for (const int64_t pair_count : {2, 8}) {
    SCOPED_TRACE(pair_count);
    ObSEArray<ObInvIdxLogicalBlockMeta, 2> metas;
    for (int64_t block = 0; block < 2; ++block) {
      ObInvIdxLogicalBlockMeta meta;
      meta.block_idx_ = block;
      meta.doc_count_ = 10;
      meta.min_doc_id_ = 10 + block * 10;
      meta.max_doc_id_ = 19 + block * 10;
      const int64_t count = 0 == block ? 1 : pair_count - 1;
      for (int64_t i = 0; i < count; ++i) {
        ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(
            ObInvIdxBM25ParamPair{static_cast<uint32_t>(0 == block ? 0 : i + 1), 100}));
      }
      ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    }
    char *buf = nullptr;
    int64_t size = 0;
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 0, 0, metas, buf, size));
    for (const common::ObTargetArch arch : {common::ObTargetArch::Default,
        common::ObTargetArch::AVX512, common::ObTargetArch::NEON}) {
      SCOPED_TRACE(arch);
      sql::BM25FrontierMaxKernels kernels;
      const int ret = sql::get_bm25_frontier_max_kernels_for_arch(arch, kernels);
      if (OB_NOT_SUPPORTED == ret) {
        continue;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ObTestFrontierMaxRecorder recorder(kernels);
      ObInvSSTablePostingIter posting;
      ObSSTablePostingScanner &scanner = posting.scanner_;
      scanner.is_inited_ = true;
      scanner.posting_index_block_opened_ = true;
      scanner.idx_iter_end_ = true;
      scanner.data_read_idx_ = 0;
      scanner.data_prefetch_idx_ = 1;
      auto &item = scanner.data_ring_[0];
      item.max_doc_id_ = 29;
      item.index_info_.agg_row_buf_ = buf;
      item.index_info_.agg_buf_size_ = size;
      item.leaf_pin_idx_ = 0;
      item.leaf_pin_generation_ = scanner.leaf_pin_slots_[0].generation_;
      scanner.leaf_pin_slots_[0].ref_cnt_ = 1;
      ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
      ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf, size));
      ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(posting.curr_meta_));
      posting.is_opened_ = true;
      posting.need_relevance_params_ = true;
      posting.bm25_frontier_kernels_ = kernels;
      posting.bm25_frontier_kernels_.kernel_ = ObTestFrontierMaxRecorder::record;
      posting.curr_block_max_score_ = 777.0;
      ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
          1.0, 100.0, 1.0, posting.bm25_calc_params_));
      const ObInvIdxBM25ParamPair *pairs = posting.curr_meta_.block_max_.pairs_;
      const double expected = reference_bm25_score(pair_count - 1, 100, posting.bm25_calc_params_);
      double actual = -99.0;
      bool can_calc = false;
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 29, actual, can_calc));
      EXPECT_TRUE(can_calc);
      EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
      EXPECT_EQ(1, recorder.calls_);
      EXPECT_EQ(pairs, recorder.pairs_);
      EXPECT_EQ(pair_count, recorder.pair_count_);
      EXPECT_FALSE(posting.curr_block_max_score_valid_);
      EXPECT_DOUBLE_EQ(777.0, posting.curr_block_max_score_);

      recorder.reset();
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 19, actual, can_calc));
      EXPECT_TRUE(can_calc);
      EXPECT_DOUBLE_EQ(0.0, actual);
      EXPECT_EQ(1, recorder.calls_);
      EXPECT_EQ(1, recorder.pair_count_);
      EXPECT_FALSE(posting.curr_block_max_score_valid_);
      uint64_t min_id = 0;
      uint64_t max_id = 0;
      recorder.reset();
      ASSERT_EQ(OB_SUCCESS, posting.get_block_max_tuple(min_id, max_id, actual));
      EXPECT_DOUBLE_EQ(0.0, actual);
      EXPECT_TRUE(posting.curr_block_max_score_valid_);
      EXPECT_EQ(1, recorder.calls_);
      EXPECT_EQ(1, recorder.pair_count_);

      recorder.reset();
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 29, actual, can_calc));
      EXPECT_TRUE(can_calc);
      EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
      EXPECT_EQ(1, recorder.calls_);
      EXPECT_EQ(pairs + 1, recorder.pairs_);
      EXPECT_EQ(pair_count - 1, recorder.pair_count_);
      recorder.reset();
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 19, actual, can_calc));
      EXPECT_TRUE(can_calc);
      EXPECT_DOUBLE_EQ(0.0, actual);
      EXPECT_EQ(0, recorder.calls_);
      recorder.reset();
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(19, false, 29, actual, can_calc));
      EXPECT_TRUE(can_calc);
      EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
      EXPECT_EQ(1, recorder.calls_);
      EXPECT_EQ(pairs + 1, recorder.pairs_);
      EXPECT_EQ(pair_count - 1, recorder.pair_count_);
      EXPECT_EQ(0, scanner.data_read_idx_);
      EXPECT_EQ(0, scanner.meta_row_scanner_.get_cur_block_idx());
      EXPECT_FALSE(posting.curr_block_loaded_);
      posting.reset();
    }
  }
}

TEST_F(TestSSTablePostingScannerValue, frontier_range_span_matches_scalar_and_error_protocol)
{
  ObArenaAllocator allocator;
  ObSEArray<ObInvIdxLogicalBlockMeta, 4> metas;
  const uint32_t values[] = {0, 1, 2, 7, 1024, UINT32_MAX - 1, UINT32_MAX};
  for (int64_t block = 0; block < 4; ++block) {
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = block;
    meta.doc_count_ = 9;
    meta.min_doc_id_ = 10 + block * 10;
    meta.max_doc_id_ = 19 + block * 10;
    for (int64_t lane = 0; lane < 9; ++lane) {
      ObInvIdxBM25ParamPair pair;
      pair.token_freq_ = values[(block + lane) % ARRAYSIZEOF(values)];
      pair.doc_length_ = values[(block * 3 + lane * 2) % ARRAYSIZEOF(values)];
      ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(pair));
    }
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  }
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 0, 0, metas, buf, size));
  ObInvSSTablePostingIter posting;
  ObSSTablePostingScanner &scanner = posting.scanner_;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.idx_iter_end_ = true;
  scanner.data_read_idx_ = 0;
  scanner.data_prefetch_idx_ = 1;
  scanner.data_ring_[0].max_doc_id_ = 49;
  scanner.data_ring_[0].index_info_.agg_row_buf_ = buf;
  scanner.data_ring_[0].index_info_.agg_buf_size_ = size;
  scanner.data_ring_[0].leaf_pin_idx_ = 0;
  scanner.data_ring_[0].leaf_pin_generation_ = scanner.leaf_pin_slots_[0].generation_;
  scanner.leaf_pin_slots_[0].ref_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf, size));
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(posting.curr_meta_));
  posting.is_opened_ = true;
  posting.need_relevance_params_ = true;
  posting.bm25_frontier_kernels_ = sql::get_bm25_frontier_max_kernels();
  const double averages[] = {0.125, 3.0, 10.0, 987.12345,
      std::numeric_limits<double>::min(), std::numeric_limits<double>::denorm_min()};
  for (const double average : averages) {
    ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
        1.23456789123, average, 0.987654321, posting.bm25_calc_params_));
    posting.curr_block_max_score_ = 0.0;
    for (const auto &pair : metas.at(0).frontier_pairs_) {
      posting.curr_block_max_score_ = std::max(posting.curr_block_max_score_,
          reference_bm25_score(pair.token_freq_, pair.doc_length_, posting.bm25_calc_params_));
    }
    posting.curr_block_max_score_valid_ = true; // fixture installs its scalar oracle cache
    const uint64_t starts[] = {1, 10, 20, 30, 40};
    const uint64_t ends[] = {9, 19, 29, 39, 49, UINT64_MAX};
    for (const uint64_t first : starts) {
      for (const uint64_t last : ends) {
        if (first > last) {
          continue;
        }
        double expected = 0.0;
        for (const auto &meta : metas) {
          if (meta.max_doc_id_ >= first && meta.min_doc_id_ <= last) {
            for (int64_t pair_idx = 0; pair_idx < meta.frontier_pairs_.count(); ++pair_idx) {
              const auto &pair = meta.frontier_pairs_.at(pair_idx);
              expected = std::max(expected, reference_bm25_score(
                  pair.token_freq_, pair.doc_length_, posting.bm25_calc_params_));
            }
          }
        }
        double actual = -1.0;
        bool can_calc = false;
        ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(first, true, last, actual, can_calc));
        ASSERT_TRUE(can_calc);
        EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
      }
    }
    double expected = 0.0;
    for (const auto &pair : metas.at(1).frontier_pairs_) {
      expected = std::max(expected, reference_bm25_score(
          pair.token_freq_, pair.doc_length_, posting.bm25_calc_params_));
    }
    double actual = -1.0;
    bool can_calc = false;
    ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(19, false, 29, actual, can_calc));
    ASSERT_TRUE(can_calc);
    EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
    posting.min_doc_id_ = 12;
    posting.max_doc_id_ = 15;
    ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(12, true, 15, actual, can_calc));
    ASSERT_TRUE(can_calc);
    EXPECT_EQ(0, MEMCMP(&posting.curr_block_max_score_, &actual, sizeof(double)));
    posting.min_doc_id_ = 0;
    posting.max_doc_id_ = UINT64_MAX;
  }
  EXPECT_EQ(0, scanner.data_read_idx_);
  EXPECT_EQ(0, scanner.meta_row_scanner_.get_cur_block_idx());
  EXPECT_FALSE(posting.curr_block_loaded_);

  ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
      1.23456789123, 10.0, 0.987654321, posting.bm25_calc_params_));
  posting.curr_block_max_score_ = 0.0;
  for (const auto &pair : metas.at(0).frontier_pairs_) {
    posting.curr_block_max_score_ = std::max(posting.curr_block_max_score_,
        reference_bm25_score(pair.token_freq_, pair.doc_length_, posting.bm25_calc_params_));
  }
  posting.curr_block_max_score_valid_ = true; // parameters changed above
  double actual = -99.0;
  bool can_calc = false;
  EXPECT_EQ(OB_INVALID_ARGUMENT, posting.get_range_max_score(10, false, 10, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_EQ(-99.0, actual);
  EXPECT_TRUE(posting.curr_block_max_score_valid_);
  ObInvIdxAggDataEntry *entries = const_cast<ObInvIdxAggDataEntry *>(scanner.meta_row_scanner_.entries_);
  const ObInvIdxAggDataEntry original = entries[1];
  entries[1].frontier_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 49, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_EQ(-99.0, actual); // never publish a partial upper bound
  entries[1] = original;
  entries[1].frontier_offset_ = 0;
  actual = -99.0;
  can_calc = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(10, true, 49, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_EQ(-99.0, actual);
  entries[1] = original;
  entries[1].frontier_offset_ = UINT16_MAX;
  EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(10, true, 49, actual, can_calc));
  EXPECT_FALSE(can_calc);
  entries[1] = original;
  entries[1].min_doc_id_ = entries[0].max_doc_id_;
  EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(10, true, 49, actual, can_calc));
  EXPECT_FALSE(can_calc);
  entries[1] = original;

  // A short span takes the flat scalar fallback without changing the oracle.
  entries[3].frontier_count_ = 7;
  ObPostingBlockMeta tail;
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.get_block_meta_at(3, tail));
  double expected = 0.0;
  for (int64_t j = 0; j < tail.block_max_.pair_count_; ++j) {
    expected = std::max(expected, reference_bm25_score(
        tail.block_max_.pairs_[j].token_freq_, tail.block_max_.pairs_[j].doc_length_,
        posting.bm25_calc_params_));
  }
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(40, true, 49, actual, can_calc));
  ASSERT_TRUE(can_calc);
  EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
  posting.reset();
}

TEST_F(TestSSTablePostingScannerValue, block_and_range_bounds_preserve_scalar_rounding_for_short_full_and_tail_spans)
{
  ObArenaAllocator allocator;
  ObSEArray<ObInvIdxLogicalBlockMeta, 3> metas;
  for (int64_t block = 0; block < 3; ++block) {
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = block;
    meta.doc_count_ = 9;
    meta.min_doc_id_ = 10 + block * 10;
    meta.max_doc_id_ = 19 + block * 10;
    for (int64_t pair = 0; pair < 9; ++pair) {
      ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(ObInvIdxBM25ParamPair{1, 6}));
    }
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  }
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 0, 0, metas, buf, size));
  ObInvSSTablePostingIter posting;
  ObSSTablePostingScanner &scanner = posting.scanner_;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.idx_iter_end_ = true;
  scanner.data_read_idx_ = 0;
  scanner.data_prefetch_idx_ = 1;
  scanner.data_ring_[0].max_doc_id_ = 39;
  scanner.data_ring_[0].index_info_.agg_row_buf_ = buf;
  scanner.data_ring_[0].index_info_.agg_buf_size_ = size;
  scanner.data_ring_[0].leaf_pin_idx_ = 0;
  scanner.data_ring_[0].leaf_pin_generation_ = scanner.leaf_pin_slots_[0].generation_;
  scanner.leaf_pin_slots_[0].ref_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf, size));
  ObPostingBlockMeta current_meta;
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(current_meta));
  posting.assign_current_block_meta(current_meta);
  posting.is_opened_ = true;
  posting.need_relevance_params_ = true;
  posting.bm25_frontier_kernels_ = sql::get_bm25_frontier_max_kernels();
  ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
      1.0, 3.0, 1.0, posting.bm25_calc_params_));
  const double expected = reference_bm25_score(1, 6, posting.bm25_calc_params_);
  ASSERT_EQ(0.32258064516129037, expected);
  const double score = sql::ObExprBM25::eval_with_precomputed_params(1, 6, posting.bm25_calc_params_);
  EXPECT_EQ(0, MEMCMP(&expected, &score, sizeof(score)));
  EXPECT_FALSE(posting.curr_block_max_score_valid_);
  for (const common::ObTargetArch arch : {common::ObTargetArch::Default,
      common::ObTargetArch::AVX512, common::ObTargetArch::NEON}) {
    SCOPED_TRACE(arch);
    const int ret = sql::get_bm25_frontier_max_kernels_for_arch(arch, posting.bm25_frontier_kernels_);
    if (OB_NOT_SUPPORTED == ret) {
      continue;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    for (const int64_t pair_count : {7, 8, 9}) {
      SCOPED_TRACE(pair_count);
      metas.at(1).frontier_pairs_.reuse();
      for (int64_t pair = 0; pair < pair_count; ++pair) {
        ASSERT_EQ(OB_SUCCESS, metas.at(1).frontier_pairs_.push_back(ObInvIdxBM25ParamPair{1, 6}));
      }
      ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(allocator, 0, 0, metas, buf, size));
      scanner.data_ring_[0].index_info_.agg_row_buf_ = buf;
      scanner.data_ring_[0].index_info_.agg_buf_size_ = size;
      scanner.meta_row_scanner_.reuse();
      ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buf, size));
      ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(current_meta));
      posting.assign_current_block_meta(current_meta);
      EXPECT_FALSE(posting.curr_block_max_score_valid_);
      uint64_t min_id = 0;
      uint64_t max_id = 0;
      double bound = 0;
      ASSERT_EQ(OB_SUCCESS, posting.get_block_max_tuple(min_id, max_id, bound));
      EXPECT_EQ(0, MEMCMP(&expected, &bound, sizeof(bound)));
      EXPECT_GE(bound, score);
      EXPECT_TRUE(posting.curr_block_max_score_valid_);
      for (const uint64_t first : {uint64_t(10), uint64_t(20)}) {
        bool can_calc = false;
        bound = -1;
        ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(first, true, 29, bound, can_calc));
        ASSERT_TRUE(can_calc);
        EXPECT_EQ(0, MEMCMP(&expected, &bound, sizeof(bound)));
        EXPECT_GE(bound, score);
      }
      bool can_calc = false;
      bound = -1;
      ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(10, true, 39, bound, can_calc));
      ASSERT_TRUE(can_calc);
      EXPECT_EQ(0, MEMCMP(&expected, &bound, sizeof(bound)));
      EXPECT_GE(bound, score);
    }
  }
  EXPECT_EQ(0, scanner.data_read_idx_);
  EXPECT_EQ(0, scanner.meta_row_scanner_.get_cur_block_idx());
  EXPECT_FALSE(posting.curr_block_loaded_);
  posting.reset();
  EXPECT_FALSE(posting.bm25_frontier_kernels_.is_valid());
}

TEST_F(TestSSTablePostingScannerValue, frontier_layout_error_discards_previous_row_score)
{
  ObArenaAllocator allocator;
  char *buffers[2] = {nullptr, nullptr};
  int64_t sizes[2] = {0, 0};
  ObSEArray<ObInvIdxLogicalBlockMeta, 3> row_metas[2];
  for (int64_t row = 0; row < 2; ++row) {
    const int64_t block_count = 0 == row ? 2 : 3;
    for (int64_t block = 0; block < block_count; ++block) {
      ObInvIdxLogicalBlockMeta meta;
      meta.block_idx_ = block;
      meta.doc_count_ = 10;
      meta.min_doc_id_ = (0 == row ? 10 : 30) + block * 10;
      meta.max_doc_id_ = meta.min_doc_id_ + 9;
      const uint32_t tf = 0 == row ? (0 == block ? 100 : 1) : 2 + block;
      for (int64_t pair = 0; pair < row + 1; ++pair) {
        ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(ObInvIdxBM25ParamPair{tf, 100}));
      }
      ASSERT_EQ(OB_SUCCESS, row_metas[row].push_back(meta));
    }
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
        allocator, 0, 0, row_metas[row], buffers[row], sizes[row]));
  }
  ObInvSSTablePostingIter posting;
  ObSSTablePostingScanner &scanner = posting.scanner_;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.idx_iter_end_ = true;
  scanner.data_read_idx_ = 7;
  scanner.data_prefetch_idx_ = 9;
  scanner.data_prefetch_depth_ = 2;
  for (int64_t row = 0; row < 2; ++row) {
    auto &item = scanner.data_ring_[(scanner.data_read_idx_ + row) % ObSSTablePostingScanner::DATA_RING_CAP];
    item.max_doc_id_ = 0 == row ? 29 : 59;
    item.index_info_.agg_row_buf_ = buffers[row];
    item.index_info_.agg_buf_size_ = sizes[row];
    item.leaf_pin_idx_ = row;
    item.leaf_pin_generation_ = scanner.leaf_pin_slots_[row].generation_;
    scanner.leaf_pin_slots_[row].ref_cnt_ = 1;
  }
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buffers[0], sizes[0]));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.next_block());
  ASSERT_EQ(OB_SUCCESS, scanner.get_cur_block_meta(posting.curr_meta_));
  posting.is_opened_ = true;
  posting.need_relevance_params_ = true;
  posting.bm25_frontier_kernels_ = sql::get_bm25_frontier_max_kernels();
  ASSERT_EQ(OB_SUCCESS, sql::ObExprBM25::init_precomputed_params(
      1.0, 100.0, 1.0, posting.bm25_calc_params_));
  ObInvIdxAggDataEntry *first = reinterpret_cast<ObInvIdxAggDataEntry *>(
      buffers[0] + sizeof(ObInvIdxAggDataHeader));
  first[0].frontier_offset_ = UINT16_MAX;
  ObInvIdxAggDataEntry *second = reinterpret_cast<ObInvIdxAggDataEntry *>(
      buffers[1] + sizeof(ObInvIdxAggDataHeader));
  int64_t calls = 0;
  double candidate = 0.0;
  const auto consume = [&](const ObPostingFrontierSpan &span) {
    ++calls;
    candidate = posting.bm25_frontier_kernels_.max_score(
        span.pairs_, span.pair_count_, posting.bm25_calc_params_, candidate);
    return OB_SUCCESS;
  };
  common::ObFunction<int(const ObPostingFrontierSpan &)> visitor(consume);
  bool covered = false;
  bool available = false;
  const double first_row_expected = reference_bm25_score(1, 100, posting.bm25_calc_params_);
  uint64_t block_min = 0;
  uint64_t block_max = 0;
  double block_bound = -1.0;
  ASSERT_EQ(OB_SUCCESS, posting.get_block_max_tuple(block_min, block_max, block_bound));
  ASSERT_DOUBLE_EQ(first_row_expected, block_bound);
  ASSERT_TRUE(posting.curr_block_max_score_valid_);
  const int64_t indices[] = {1, 1, 2};
  const uint16_t offsets[] = {3 * sizeof(ObInvIdxBM25ParamPair), sizeof(ObInvIdxBM25ParamPair), 0};
  for (int64_t i = 0; i < ARRAYSIZEOF(indices); ++i) {
    SCOPED_TRACE(i);
    const uint16_t original_offset = second[indices[i]].frontier_offset_;
    second[indices[i]].frontier_offset_ = offsets[i];
    calls = 0;
    candidate = 0.0;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        scanner.visit_prefetched_frontier_spans(20, 59, visitor, covered, available));
    EXPECT_FALSE(covered);
    EXPECT_EQ(1, calls);
    EXPECT_EQ(0, MEMCMP(&first_row_expected, &candidate, sizeof(double)));
    double actual = -99.0;
    bool can_calc = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED, posting.get_range_max_score(20, true, 59, actual, can_calc));
    EXPECT_FALSE(can_calc);
    EXPECT_DOUBLE_EQ(-99.0, actual);
    EXPECT_TRUE(posting.curr_block_max_score_valid_);
    EXPECT_DOUBLE_EQ(first_row_expected, posting.curr_block_max_score_);
    EXPECT_EQ(7, scanner.data_read_idx_);
    EXPECT_EQ(9, scanner.data_prefetch_idx_);
    EXPECT_EQ(1, scanner.meta_row_scanner_.get_cur_block_idx());
    EXPECT_FALSE(posting.curr_block_loaded_);
    second[indices[i]].frontier_offset_ = original_offset;
  }
  calls = 0;
  candidate = 0.0;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_frontier_spans(20, 59, visitor, covered, available));
  EXPECT_TRUE(covered);
  EXPECT_TRUE(available);
  EXPECT_EQ(2, calls);
  const double expected = reference_bm25_score(4, 100, posting.bm25_calc_params_);
  EXPECT_EQ(0, MEMCMP(&expected, &candidate, sizeof(double)));
  double actual = -99.0;
  bool can_calc = false;
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(20, true, 59, actual, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
  uint64_t min_id = 0;
  uint64_t max_id = 0;
  ASSERT_EQ(OB_SUCCESS, posting.get_block_max_tuple(min_id, max_id, actual));
  ASSERT_TRUE(posting.curr_block_max_score_valid_);
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(20, true, 59, actual, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_EQ(0, MEMCMP(&expected, &actual, sizeof(double)));
  row_metas[1].at(1).frontier_pairs_.reset();
  char *missing_buf = nullptr;
  int64_t missing_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
      allocator, 0, 0, row_metas[1], missing_buf, missing_size));
  auto &last_item = scanner.data_ring_[(scanner.data_read_idx_ + 1) % ObSSTablePostingScanner::DATA_RING_CAP];
  last_item.index_info_.agg_row_buf_ = missing_buf;
  last_item.index_info_.agg_buf_size_ = missing_size;
  actual = -99.0;
  can_calc = true;
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(20, true, 59, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-99.0, actual);
  posting.need_relevance_params_ = false;
  can_calc = true;
  ASSERT_EQ(OB_SUCCESS, posting.get_range_max_score(20, true, 59, actual, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-99.0, actual);
  posting.reset();
}

TEST_F(TestSSTablePostingScannerValue, frontier_spans_validate_cross_row_order_and_complete_coverage)
{
  ObArenaAllocator allocator;
  char *buffers[2] = {nullptr, nullptr};
  int64_t sizes[2] = {0, 0};
  for (int64_t row = 0; row < 2; ++row) {
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = 0;
    meta.doc_count_ = 1;
    meta.min_doc_id_ = 10 + row * 20;
    meta.max_doc_id_ = 20 + row * 20;
    ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.push_back(
        ObInvIdxBM25ParamPair{static_cast<uint32_t>(row + 1), 10}));
    ObSEArray<ObInvIdxLogicalBlockMeta, 1> metas;
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
    ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
        allocator, 0, 0, metas, buffers[row], sizes[row]));
  }
  ObSSTablePostingScanner scanner;
  scanner.is_inited_ = true;
  scanner.posting_index_block_opened_ = true;
  scanner.idx_iter_end_ = true;
  scanner.data_read_idx_ = 0;
  scanner.data_prefetch_idx_ = 2;
  scanner.data_prefetch_depth_ = 2;
  for (int64_t row = 0; row < 2; ++row) {
    auto &item = scanner.data_ring_[row];
    item.max_doc_id_ = 20 + row * 20;
    item.index_info_.agg_row_buf_ = buffers[row];
    item.index_info_.agg_buf_size_ = sizes[row];
    item.leaf_pin_idx_ = row;
    item.leaf_pin_generation_ = scanner.leaf_pin_slots_[row].generation_;
    scanner.leaf_pin_slots_[row].ref_cnt_ = 1;
  }
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.init(true));
  ASSERT_EQ(OB_SUCCESS, scanner.meta_row_scanner_.open(buffers[0], sizes[0]));
  int64_t calls = 0;
  uint64_t tf_sum = 0;
  const auto consume = [&](const ObPostingFrontierSpan &span) {
    ++calls;
    for (int64_t i = 0; i < span.pair_count_; ++i) {
      tf_sum += span.pairs_[i].token_freq_;
    }
    return OB_SUCCESS;
  };
  common::ObFunction<int(const ObPostingFrontierSpan &)> visitor(consume);
  bool covered = false;
  bool available = false;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_frontier_spans(10, 40, visitor, covered, available));
  EXPECT_TRUE(covered);
  EXPECT_TRUE(available);
  EXPECT_EQ(2, calls);
  EXPECT_EQ(3, tf_sum);
  EXPECT_EQ(0, scanner.data_read_idx_);
  EXPECT_EQ(0, scanner.meta_row_scanner_.get_cur_block_idx());

  ObInvIdxAggDataEntry *second = reinterpret_cast<ObInvIdxAggDataEntry *>(
      buffers[1] + sizeof(ObInvIdxAggDataHeader));
  second->min_doc_id_ = 20; // overlaps the preceding row at its inclusive end
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.visit_prefetched_frontier_spans(10, 40, visitor, covered, available));
  second->min_doc_id_ = 30;
  scanner.data_ring_[1].max_doc_id_ = 41;
  EXPECT_EQ(OB_ERR_UNEXPECTED, scanner.visit_prefetched_frontier_spans(10, 100, visitor, covered, available));
  scanner.data_ring_[1].max_doc_id_ = 40;

  // A full lookahead ring that does not cover the requested range cannot
  // publish the partial maxima of its pinned rows as a complete range bound.
  scanner.idx_iter_end_ = false;
  calls = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_frontier_spans(10, 100, visitor, covered, available));
  EXPECT_FALSE(covered);
  EXPECT_EQ(0, calls);
  scanner.idx_iter_end_ = true;
  ASSERT_EQ(OB_SUCCESS, scanner.visit_prefetched_frontier_spans(10, UINT64_MAX, visitor, covered, available));
  EXPECT_TRUE(covered);
  EXPECT_TRUE(available);
  EXPECT_EQ(2, calls);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::init_arches();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
