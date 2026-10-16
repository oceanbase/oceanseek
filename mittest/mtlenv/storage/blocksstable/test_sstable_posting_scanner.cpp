/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_sstable_posting_scanner.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"

namespace oceanbase
{
namespace unittest
{

namespace
{
// Format a feeder term string: "term" + 10-digit zero-padded seq (matches ObInvIdxRowFeeder).
void format_term(const int64_t seq, char *buf, const int64_t buf_len)
{
  snprintf(buf, buf_len, "term%010ld", static_cast<long>(seq));
}

// Build the rowkey read info from the FTS schema built by the prepare harness.
// Same recipe as test_inv_idx_sstable_basic.cpp::scan_inv_sstable_with_sec_meta_iterator.
void build_rowkey_read_info(
    const ObTableSchema &table_schema,
    common::ObIAllocator &allocator,
    ObTableReadInfo &read_info)
{
  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema.get_column_ids(schema_cols));
  OK(read_info.init(
      allocator,
      table_schema.get_column_count(),
      table_schema.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols,
      nullptr /*storage_cols_index*/));
  ASSERT_TRUE(read_info.is_valid());
}

// build_term_rowkey() lives in test_inv_idx_sstable_prepare.h.

// POD snapshot of one posting block's meta used for cross-block invariant checks.
struct BlockMetaSnap
{
  uint64_t min_id_;
  uint64_t max_id_;
  int64_t doc_count_;
  TO_STRING_KV(K_(min_id), K_(max_id), K_(doc_count));
};
} // namespace

// Build an inverted-index SSTable and scan one term end to end, verifying the posting
// block meta the scanner reports is internally consistent and doc-id-ordered/contiguous.
TEST_F(TestInvIdxSSTablePrepare, scan_single_term_meta)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  // open() positions the cursor on the first posting block, so the loop is driven by
  // get_cur_block_meta(); next_block() advances. Both report OB_ITER_END at exhaustion.
  ObPostingBlockMeta meta;
  uint64_t prev_max_id = 0;
  int64_t block_cnt = 0;
  int64_t sum_doc_count = 0;
  bool changed = false;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
    ASSERT_TRUE(meta.is_valid()) << "posting block meta must be valid";
    ASSERT_LE(meta.min_id_, meta.max_id_);
    ASSERT_GT(meta.doc_count_, 0);
    // within-term posting blocks are strictly ordered, non-overlapping and contiguous:
    // each block starts exactly at prev_max + 1 (prev_max_id == 0 for the first block).
    ASSERT_EQ(prev_max_id + 1, meta.min_id_)
        << "posting blocks within one term must be contiguous";
    prev_max_id = meta.max_id_;
    sum_doc_count += meta.doc_count_;
    ++block_cnt;
    ret = scanner.next_block(changed);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(block_cnt, 0);
  // The scanned term is non-empty, so its doc-id run must be non-empty.
  ASSERT_GT(sum_doc_count, 0);
  STORAGE_LOG(INFO, "scanned single term posting blocks",
      K(block_cnt), K(sum_doc_count), K(prev_max_id));
}

// For the same single term, fetch each posting data micro block via get_cur_micro_block(),
// decode its doc_ids with ObPostingBlockData, and cross-check the decoded content against
// the meta the scanner reported (count, min/max, strictly-increasing & contiguous).
TEST_F(TestInvIdxSSTablePrepare, scan_single_term_data_block)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  ObPostingBlockMeta meta;
  uint64_t prev_max_id = 0;
  int64_t block_cnt = 0;
  bool changed = false;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
    ASSERT_TRUE(meta.is_valid());
    // Fetch the posting data micro block backing the current leaf row. This issues the
    // data-block IO on demand and turns on adaptive prefetch.
    ObMicroBlockData data_block;
    OK(scanner.get_cur_micro_block(data_block));
    ASSERT_TRUE(data_block.is_valid());

    // Locate the posting block addressed by the meta's locator in FlatCache.
    ObInvIdxMicroBlockReader payload_reader;
    OK(payload_reader.init_flat_cache(data_block.get_buf(), data_block.get_buf_size()));
    ObPostingBlockData block_data;
    OK(payload_reader.get_posting_block_data(meta.locator_.block_idx_, block_data));
    ASSERT_TRUE(block_data.is_valid());

    uint64_t prev_doc_id = 0;
    uint64_t first_doc_id = 0;
    uint64_t last_doc_id = 0;
    for (int64_t i = 0; i < meta.doc_count_; ++i) {
      uint64_t doc_id = 0;
      OK(block_data.get_doc_id(i, doc_id));
      if (0 == i) {
        first_doc_id = doc_id;
        ASSERT_EQ(prev_max_id + 1, doc_id) << "first doc of a block continues the term run";
      } else {
        ASSERT_EQ(prev_doc_id + 1, doc_id) << "doc_ids within a posting block must be contiguous";
      }
      prev_doc_id = doc_id;
      last_doc_id = doc_id;
    }
    // Decoded content must match the meta the scanner reported for this block.
    ASSERT_EQ(meta.min_id_, first_doc_id);
    ASSERT_EQ(meta.max_id_, last_doc_id);
    prev_max_id = last_doc_id;
    ++block_cnt;
    ret = scanner.next_block(changed);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(block_cnt, 0);
  STORAGE_LOG(INFO, "decoded single term posting data blocks", K(block_cnt), K(prev_max_id));
}

// With need_posting_value_=true, fetch each paired value micro block via
// get_cur_value_micro_block() and decode position_list rows by meta.locator_.block_idx_.
TEST_F(TestInvIdxSSTablePrepare, scan_single_term_value_block)
{
  const int64_t doc_cnt = 1000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(true /*has_pos_list*/, doc_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_,
      true /*need_posting_value*/));

  ObPostingBlockMeta meta;
  int64_t block_cnt = 0;
  bool changed = false;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
    ASSERT_TRUE(meta.is_valid());
    ObMicroBlockData data_block;
    OK(scanner.get_cur_micro_block(data_block));
    ASSERT_TRUE(data_block.is_valid());

    ObMicroBlockData value_block;
    OK(scanner.get_cur_value_micro_block(value_block));
    ASSERT_TRUE(value_block.is_valid());

    const char *raw = value_block.get_buf();
    const int64_t raw_size = value_block.get_buf_size();
    blocksstable::ObMicroBlockHeader micro_header;
    ASSERT_GE(raw_size, blocksstable::ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET);
    OK(micro_header.deserialize_and_check_header(raw, raw_size));
    ASSERT_EQ(common::INV_IDX_ROW_STORE, micro_header.get_row_store_type());
    ASSERT_EQ(static_cast<uint8_t>(blocksstable::MICRO_BLOCK_INV_IDX_POSTING_VALUE),
        micro_header.inverted_index_type_);

    const char *block_buf = nullptr;
    int64_t block_size = 0;
    ObInvIdxMicroBlockReader payload_reader;
    OK(payload_reader.init(raw, raw_size, true /*is_posting_value*/));
    OK(payload_reader.get_payload(meta.locator_.block_idx_, block_buf, block_size));
    ASSERT_GT(block_size, 0);

    ObPostingValueBlockReader value_reader;
    OK(value_reader.init(block_buf, block_size));
    for (int64_t i = 0; i < meta.doc_count_; ++i) {
      ObDatum pos_list;
      OK(value_reader.get_position_list(i, pos_list));
      ASSERT_EQ(TEST_POS_LIST_LEN, pos_list.len_);
      for (int64_t j = 0; j < pos_list.len_; ++j) {
        ASSERT_EQ('p', pos_list.ptr_[j]);
      }
    }
    ++block_cnt;
    ret = scanner.next_block(changed);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(block_cnt, 0);
}

// get_cur_value_micro_block() returns OB_NOT_SUPPORTED when need_posting_value_ is false.
TEST_F(TestInvIdxSSTablePrepare, get_cur_value_micro_block_disabled)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  ObMicroBlockData value_block;
  ASSERT_EQ(OB_NOT_SUPPORTED, scanner.get_cur_value_micro_block(value_block));
}

// Cache disabled path: current posting data block must be served by raw IO callback path
// and still decode as a valid inverted posting micro block.
TEST_F(TestInvIdxSSTablePrepare, scan_single_term_data_block_raw_io_only)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag; // keep block cache disabled to force IO path
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  ObPostingBlockMeta meta;
  OK(scanner.get_cur_block_meta(meta));
  ASSERT_TRUE(meta.is_valid());

  ObMicroBlockData data_block;
  OK(scanner.get_cur_micro_block(data_block));
  ASSERT_TRUE(data_block.is_valid());
  const auto &item = scanner.cur_data_item();
  ASSERT_EQ(storage::ObSSTableMicroBlockState::IN_BLOCK_IO, item.data_handle_.block_state_);

  ObInvIdxMicroBlockReader payload_reader;
  OK(payload_reader.init_flat_cache(data_block.get_buf(), data_block.get_buf_size()));
  ObPostingBlockData block_data;
  OK(payload_reader.get_posting_block_data(meta.locator_.block_idx_, block_data));
  ASSERT_TRUE(block_data.is_valid());
}

// Cache enabled path: first scanner reads by IO and populates cache; second scanner
// for the same term should hit cache for the current posting data block.
TEST_F(TestInvIdxSSTablePrepare, scan_single_term_data_block_cache_hit)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();

  ObPostingBlockMeta meta;
  ObMicroBlockData data_block;

  {
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    OK(scanner.get_cur_block_meta(meta));
    OK(scanner.get_cur_micro_block(data_block));
    ASSERT_TRUE(data_block.is_valid());
  }

  {
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    OK(scanner.get_cur_block_meta(meta));
    OK(scanner.get_cur_micro_block(data_block));
    ASSERT_TRUE(data_block.is_valid());
    const auto &item = scanner.cur_data_item();
    ASSERT_EQ(storage::ObSSTableMicroBlockState::IN_BLOCK_CACHE, item.data_handle_.block_state_);

    ObInvIdxMicroBlockReader payload_reader;
    OK(payload_reader.init_flat_cache(data_block.get_buf(), data_block.get_buf_size()));
    ObPostingBlockData block_data;
    OK(payload_reader.get_posting_block_data(meta.locator_.block_idx_, block_data));
    ASSERT_TRUE(block_data.is_valid());
  }
}

// Enumerate every term in the sstable, scan all its posting blocks, and verify global
// accounting: the sum of every posting block's doc_count equals the input row count, and
// the union of all per-term doc-id ranges equals the sstable's inv-meta doc-id span.
// This proves the scanner walks every posting block exactly once, in order, across leaf
// index micro blocks and macro blocks.
TEST_F(TestInvIdxSSTablePrepare, scan_all_terms_accounting)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();

  int64_t total_doc_count = 0;
  int64_t term_cnt = 0;
  uint64_t global_min_id = UINT64_MAX;
  uint64_t global_max_id = 0;
  uint64_t prev_term_max_id = 0;

  // The feeder emits contiguous term seqs starting at 1; stop at the first absent term.
  for (int64_t seq = 1;; ++seq) {
    char term_buf[32] = {0};
    format_term(seq, term_buf, sizeof(term_buf));
    ObStorageDatum term_datum;
    ObDatumRowkey query_key;
    build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

    ObPostingBlockMeta meta;
    bool changed = false;
    uint64_t term_min_id = 0;
    uint64_t term_max_id = 0;
    int64_t term_doc_count = 0;
    int64_t term_block_cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
      ASSERT_TRUE(meta.is_valid());
      if (0 == term_block_cnt) {
        term_min_id = meta.min_id_;
        // The first term starts at the sstable's min doc id; every later term's first
        // block starts exactly where the previous term ended (feeder doc ids are globally
        // increasing and each term owns a contiguous run).
        ASSERT_EQ(prev_term_max_id + 1, meta.min_id_)
            << "per-term doc-id runs must be contiguous and ordered across terms";
      } else {
        // Within one term, consecutive posting blocks are non-overlapping and contiguous.
        ASSERT_EQ(term_max_id + 1, meta.min_id_)
            << "posting blocks within one term must be contiguous";
      }
      term_max_id = meta.max_id_;
      term_doc_count += meta.doc_count_;
      ++term_block_cnt;
      ret = scanner.next_block(changed);
      bool is_ret_valid = OB_ITER_END == ret || OB_SUCCESS == ret;
      ASSERT_TRUE(is_ret_valid);
    }
    ASSERT_EQ(OB_ITER_END, ret);

    STORAGE_LOG(INFO, "term scan", K(seq), K(term_block_cnt),
        K(term_min_id), K(term_max_id), K(term_doc_count),
        "prev_term_max", prev_term_max_id);

    if (0 == term_block_cnt) {
      // No posting block for this term -> term is absent; enumeration complete.
      break;
    }

    ++term_cnt;
    total_doc_count += term_doc_count;
    if (1 == term_cnt) {
      global_min_id = term_min_id;
    }
    global_max_id = term_max_id;
    prev_term_max_id = term_max_id;
  }

  ASSERT_GT(term_cnt, 0);
  // Every input (term, doc_id) row lands in exactly one posting block.
  ASSERT_EQ(row_cnt, total_doc_count);
  // The leaf-layer doc-id extremes match the sstable's inv extended meta.
  ASSERT_EQ(inv_sstable.get_inv_meta().min_doc_id_, global_min_id);
  ASSERT_EQ(inv_sstable.get_inv_meta().max_doc_id_, global_max_id);
  STORAGE_LOG(INFO, "scanned all terms", K(term_cnt), K(total_doc_count),
      K(global_min_id), K(global_max_id));
}

// Opening a term that is not present in the sstable must succeed but leave the cursor at
// end: the first next_block()/get_cur_block_meta() reports OB_ITER_END.
TEST_F(TestInvIdxSSTablePrepare, open_nonexistent_term)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 5000;  // small sstable is sufficient for this case
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // beyond the feeder's term range
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  ObPostingBlockMeta meta;
  ASSERT_EQ(OB_ITER_END, scanner.get_cur_block_meta(meta));
  bool changed = false;
  ASSERT_EQ(OB_ITER_END, scanner.next_block(changed));
}

// advance_to() must land the cursor on the first posting block whose max_doc_id >= target,
// be forward-only (a smaller later target does not move backward), and reach iter-end when
// the target exceeds the term's doc-id range.
TEST_F(TestInvIdxSSTablePrepare, advance_to_within_term)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  auto open_term1 = [&](ObSSTablePostingScanner &scanner) {
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
  };

  // Pass 1: collect term1's posting block meta snapshots (min/max/doc_count) in order.
  common::ObSEArray<BlockMetaSnap, 16> blocks;
  {
    ObSSTablePostingScanner scanner;
    open_term1(scanner);
    ObPostingBlockMeta meta;
    bool changed = false;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
      ASSERT_TRUE(meta.is_valid());
      OK(blocks.push_back(BlockMetaSnap{meta.min_id_, meta.max_id_, meta.doc_count_}));
      ret = scanner.next_block(changed);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_GE(blocks.count(), 2) << "term1 must span at least two posting blocks";

  const BlockMetaSnap &b0 = blocks.at(0);
  const BlockMetaSnap &b1 = blocks.at(1);

  // Pass 2: advance into the second block.
  {
    ObSSTablePostingScanner scanner;
    open_term1(scanner);
    bool changed = false;
    OK(scanner.advance_to(b1.min_id_, true /*inclusive*/, changed));
    ObPostingBlockMeta meta;
    OK(scanner.get_cur_block_meta(meta));
    // advance_to lands on the first block whose max_doc_id >= target; b1.min_id_ is inside
    // block 1 (and > block 0's max), so the cursor must be on block 1.
    ASSERT_EQ(b1.min_id_, meta.min_id_);
    ASSERT_LE(meta.min_id_, b1.min_id_);
    ASSERT_GE(meta.max_id_, b1.min_id_);

    // Forward-only: a target inside block 0 must not move the cursor back to block 0.
    OK(scanner.advance_to(b0.min_id_, true, changed));
    OK(scanner.get_cur_block_meta(meta));
    ASSERT_EQ(b1.min_id_, meta.min_id_)
        << "advance_to must be forward-only; a smaller target must not move backward";
  }
}

// advance_to(target, inclusive=false) with target == UINT64_MAX has no valid successor
// (target + 1 would wrap to 0). It must reach iter-end directly instead of wrapping.
TEST_F(TestInvIdxSSTablePrepare, advance_to_exclusive_uint64_max)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 5000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  bool changed = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.advance_to(UINT64_MAX, false /*exclusive*/, changed))
      << "exclusive advance_to(UINT64_MAX) must not wrap target+1 to 0";
}

// advance_to() to a doc id beyond the term's whole range must reach iter-end, whether the
// ring still holds items or the index scanner has already exhausted the term.
TEST_F(TestInvIdxSSTablePrepare, advance_to_beyond_term_returns_iter_end)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  int64_t term_count = 0;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable,
      false /*vary_posting_payload*/, 0 /*initial_term_seq*/, 0 /*initial_doc_id*/,
      false /*micro_index_clustered*/, 1 /*doc_id_step*/, &term_count);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(term_count, 0);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  // Use the final term so its closest out-of-range key is also beyond the last
  // prefetched leaf key and exercises the non-prefetched advance_to() cleanup.
  format_term(term_count, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();

  // Resolve the real end of this term first, then verify the closest out-of-range target
  // in addition to the far-out target below.
  uint64_t term_max_doc_id = 0;
  {
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    ObPostingBlockMeta meta;
    bool changed = false;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
      term_max_doc_id = meta.max_id_;
      ret = scanner.next_block(changed);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_GT(term_max_doc_id, 0);
  ASSERT_LT(term_max_doc_id, UINT64_MAX);

  {
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    scanner.set_skip_key_doc_id(term_max_doc_id + 1);
    ObSSTableIndexBlockLevelScanner *leaf =
        scanner.sstable_index_scanner_.level_scanners_.at(
            scanner.sstable_index_scanner_.level_scanners_.count() - 1);
    ASSERT_TRUE(nullptr != leaf);
    int cmp_ret = 0;
    ASSERT_EQ(OB_SUCCESS,
        leaf->last_prefetch_key_.compare(
            scanner.skip_key_, read_info.get_datum_utils(), cmp_ret, false));
    ASSERT_LT(cmp_ret, 0);
    bool changed = false;
    ASSERT_EQ(OB_ITER_END,
        scanner.advance_to(term_max_doc_id + 1, true /*inclusive*/, changed));
    ObPostingBlockMeta meta;
    ASSERT_EQ(OB_ITER_END, scanner.get_cur_block_meta(meta));
  }

  // 1 << 62 is beyond every doc id in the sstable (feeder max_doc_id == row_cnt).
  {
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    bool changed = false;
    ASSERT_EQ(OB_ITER_END, scanner.advance_to(1ULL << 62, true /*inclusive*/, changed));
    ObPostingBlockMeta meta;
    ASSERT_EQ(OB_ITER_END, scanner.get_cur_block_meta(meta));
  }
}

// Cold-read path: right after open() no data IO has been issued. The first get_cur_micro_block()
// must fetch the current item's data block on demand and decode consistently with its meta.
TEST_F(TestInvIdxSSTablePrepare, get_cur_micro_block_cold_read_after_open)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
  // Cold start: adaptive prefetch is off and no data IO has been issued yet.
  ASSERT_FALSE(scanner.data_io_enabled_);
  ASSERT_EQ(1, scanner.get_prefetched_meta_row_count());
  OK(scanner.ensure_meta_lookahead(UINT64_MAX));
  ASSERT_GT(scanner.get_prefetched_meta_row_count(), 1);
  for (int64_t item_idx = scanner.data_read_idx_;
      item_idx < scanner.data_prefetch_idx_;
      ++item_idx) {
    ASSERT_FALSE(scanner.data_ring_[item_idx % ObSSTablePostingScanner::DATA_RING_CAP].io_issued_);
  }

  ObPostingBlockMeta meta;
  OK(scanner.get_cur_block_meta(meta));
  ASSERT_TRUE(meta.is_valid());

  ObMicroBlockData data_block;
  OK(scanner.get_cur_micro_block(data_block));  // issues IO on demand for the current item
  ASSERT_TRUE(data_block.is_valid());
  ASSERT_TRUE(scanner.data_io_enabled_);  // first read demand turned adaptive prefetch on
  // Metadata rows pinned by range-max join the same asynchronous data-IO pipeline.
  for (int64_t item_idx = scanner.data_read_idx_ + 1;
      item_idx < scanner.data_prefetch_idx_;
      ++item_idx) {
    ASSERT_TRUE(scanner.data_ring_[item_idx % ObSSTablePostingScanner::DATA_RING_CAP].io_issued_);
  }

  // Decode the first posting block and cross-check against the reported meta.
  ObInvIdxMicroBlockReader payload_reader;
  OK(payload_reader.init_flat_cache(data_block.get_buf(), data_block.get_buf_size()));
  ObPostingBlockData block_data;
  OK(payload_reader.get_posting_block_data(meta.locator_.block_idx_, block_data));
  ASSERT_TRUE(block_data.is_valid());
  uint64_t first_doc_id = 0;
  uint64_t last_doc_id = 0;
  uint64_t prev_doc_id = 0;
  for (int64_t i = 0; i < meta.doc_count_; ++i) {
    uint64_t doc_id = 0;
    OK(block_data.get_doc_id(i, doc_id));
    if (0 == i) {
      first_doc_id = doc_id;
    } else {
      ASSERT_EQ(prev_doc_id + 1, doc_id);
    }
    prev_doc_id = doc_id;
    last_doc_id = doc_id;
  }
  ASSERT_EQ(meta.min_id_, first_doc_id);
  ASSERT_EQ(meta.max_id_, last_doc_id);
}

// block_changed must be true exactly when scanner movement switches to a new
// prefetched index row (data item), and false inside the same item.
TEST_F(TestInvIdxSSTablePrepare, block_changed_matches_data_item_switch)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  ObMicroBlockData data_block;
  OK(scanner.get_cur_micro_block(data_block));
  // One leaf index row may cover multiple posting blocks (same ring item); prefetch depth
  // counts leaf rows, not posting blocks inside meta_row_scanner_.
  const int64_t prefetch_window = scanner.data_prefetch_idx_ - scanner.data_read_idx_;
  ASSERT_GE(prefetch_window, 1);

  bool changed = false;
  if (prefetch_window > 1) {
    const int64_t prev_data_read_idx = scanner.data_read_idx_;
    const uint64_t next_item_max_doc_id =
        scanner.data_ring_[(prev_data_read_idx + 1) % ObSSTablePostingScanner::DATA_RING_CAP].max_doc_id_;
    OK(scanner.advance_to(next_item_max_doc_id, true /*inclusive*/, changed));
    ASSERT_GT(scanner.data_read_idx_, prev_data_read_idx);
    ASSERT_TRUE(changed);
  }

  ObPostingBlockMeta meta;
  OK(scanner.get_cur_block_meta(meta));
  OK(scanner.advance_to(meta.max_id_, true /*inclusive*/, changed));
  ASSERT_FALSE(changed);

  uint64_t prev_item_max_doc_id = scanner.cur_data_item().max_doc_id_;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
    ret = scanner.next_block(changed);
    if (OB_SUCC(ret)) {
      const uint64_t cur_item_max_doc_id = scanner.cur_data_item().max_doc_id_;
      const bool item_switched = (cur_item_max_doc_id != prev_item_max_doc_id);
      ASSERT_EQ(item_switched, changed)
          << "block_changed must mirror whether next_block switched the data item";
      prev_item_max_doc_id = cur_item_max_doc_id;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

// Leaf pin slot lifecycle: the sum of slot ref_cnt_ must always equal the live ring item
// count (no pin leak / over-release), and a recycled slot (generation mismatch) must fail
// validation against a stale item reference.
TEST_F(TestInvIdxSSTablePrepare, leaf_pin_slot_refcount_invariant_and_generation_guard)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTablePostingScanner scanner;
  OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));

  // Walk enough blocks to cross several items (each item = one posting data micro block),
  // exercising slot acquire / release / reuse.
  ObPostingBlockMeta meta;
  bool changed = false;
  int ret = OB_SUCCESS;
  for (int64_t step = 0; OB_SUCC(ret) && step < 20; ++step) {
    if (OB_FAIL(scanner.get_cur_block_meta(meta))) {
      break;
    }
    ret = scanner.next_block(changed);
  }
  ASSERT_TRUE(OB_ITER_END == ret || OB_SUCCESS == ret);

  // Invariant: total referenced pin count == live ring item count.
  int64_t total_ref = 0;
  for (int64_t i = 0; i < ObSSTablePostingScanner::LEAF_PIN_SLOT_CAP; ++i) {
    ASSERT_GE(scanner.leaf_pin_slots_[i].ref_cnt_, 0);
    total_ref += scanner.leaf_pin_slots_[i].ref_cnt_;
  }
  ASSERT_EQ(scanner.data_prefetch_idx_ - scanner.data_read_idx_, total_ref)
      << "leaf pin refcount must equal the live ring item count";

  // Generation guard: bumping the referenced slot's generation (simulating recycle) must
  // invalidate the current item's reference, preventing stale-slot reuse.
  if (scanner.has_read_item()) {
    ObSSTablePostingScanner::DataPrefetchItem &item = scanner.cur_data_item();
    ASSERT_TRUE(scanner.validate_leaf_pin_ref(item));
    scanner.leaf_pin_slots_[item.leaf_pin_idx_].generation_ = item.leaf_pin_generation_ + 1;
    ASSERT_FALSE(scanner.validate_leaf_pin_ref(item))
        << "a recycled slot must not validate against a stale item reference";
  }
  // reset() must drop every pin regardless of the corruption above.
  scanner.reset();
  for (int64_t i = 0; i < ObSSTablePostingScanner::LEAF_PIN_SLOT_CAP; ++i) {
    ASSERT_EQ(0, scanner.leaf_pin_slots_[i].ref_cnt_);
  }
}

// Cross-leaf prefetch window: with a large sstable, term1 spans many posting data micro
// blocks (and multiple leaf index micro blocks). advance_to() into a late block must land
// on the correct posting block, served from the cross-leaf prefetch ring or via re-seek.
TEST_F(TestInvIdxSSTablePrepare, cross_leaf_prefetch_window)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 3000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  // Pass 1: collect term1's posting block snapshots in order.
  common::ObSEArray<BlockMetaSnap, 64> blocks;
  {
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    ObPostingBlockMeta meta;
    bool changed = false;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
      ASSERT_TRUE(meta.is_valid());
      OK(blocks.push_back(BlockMetaSnap{meta.min_id_, meta.max_id_, meta.doc_count_}));
      ret = scanner.next_block(changed);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_GE(blocks.count(), 2) << "term1 must span at least two posting blocks";

  // Pass 2: advance into a block ~2/3 of the way through the term and verify the landing.
  {
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObSSTablePostingScanner scanner;
    OK(scanner.open(read_info, query_key, ObTabletID(TABLET_ID), query_flag, inv_sstable, allocator_));
    const BlockMetaSnap &late = blocks.at(blocks.count() * 2 / 3);
    bool changed = false;
    OK(scanner.advance_to(late.min_id_, true /*inclusive*/, changed));
    ObPostingBlockMeta meta;
    OK(scanner.get_cur_block_meta(meta));
    ASSERT_LE(meta.min_id_, late.min_id_);
    ASSERT_GE(meta.max_id_, late.min_id_)
        << "advance_to must land on a block covering the target doc id";
    // Continue scanning from there: the remainder must match the Pass-1 tail exactly.
    int64_t idx = blocks.count() * 2 / 3;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = scanner.get_cur_block_meta(meta))) {
      ASSERT_LT(idx, blocks.count());
      ASSERT_EQ(blocks.at(idx).min_id_, meta.min_id_);
      ASSERT_EQ(blocks.at(idx).max_id_, meta.max_id_);
      ASSERT_EQ(blocks.at(idx).doc_count_, meta.doc_count_);
      ++idx;
      ret = scanner.next_block(changed);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(blocks.count(), idx) << "advance_to must not skip or duplicate any block";
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_posting_scanner.log*");
  OB_LOGGER.set_file_name("test_sstable_posting_scanner.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
