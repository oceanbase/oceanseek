/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "mtlenv/mock_tenant_module_env.h"

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <signal.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_table_schema.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_row_logical_block_iter.h"
#include "storage/blocksstable/async_index/ob_inv_idx_logical_block_iter.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;
using namespace share::schema;

static const int64_t TEST_BM25_MAX_PAIR_COUNT = 4;

// 测试用自包含 read_info。迭代器只从 read_info 取输出宽度与列类型，列角色由传给
// open() 的 ObInvIdxRowDesc 决定（不再依赖 cols_index_）。子类化 ObReadInfoStruct
// 而非 ObTableReadInfo：该基类已实现所有 ObITableReadInfo 虚函数并持有 cols_desc_ /
// cols_index_，避免生产 Mock 对私有成员的依赖。cols_index_ 这里填恒等映射，仅为满足
// 基类不变式，迭代器并不消费它。
class TestReadInfo : public storage::ObReadInfoStruct
{
public:
  TestReadInfo() : storage::ObReadInfoStruct(false /*rowkey_mode*/) {}
  int init(common::ObIAllocator &allocator,
           const int64_t schema_column_count,
           const int64_t schema_rowkey_cnt,
           const bool is_oracle_mode,
           const common::ObIArray<ObColDesc> &cols_desc,
           const common::ObIArray<int32_t> *storage_cols_index = nullptr)
  {
    int ret = OB_SUCCESS;
    const int64_t out_cols_cnt = cols_desc.count();
    init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode,
        false /*is_cg_sstable*/, false /*is_cs_replica_compat*/,
        false /*is_delete_insert_table*/, false /*is_global_index_table*/,
        1 /*micro_block_format_version*/, false /*is_mv_major_refresh_tablet*/,
        false /*has_ttl_definition*/);
    if (OB_SUCC(ret = prepare_arrays(allocator, cols_desc, out_cols_cnt))) {
      for (int64_t i = 0; i < out_cols_cnt; ++i) {
        const int32_t store_idx = (nullptr == storage_cols_index)
            ? static_cast<int32_t>(i) : storage_cols_index->at(i);
        cols_index_.array_[i] = store_idx;
        memtable_cols_index_.array_[i] = store_idx;
      }
      is_inited_ = true;
    }
    return ret;
  }
};

// =============================================================================
// ObInvRowLogicalBlockIterator — logical block（posting block）-> 行迭代器
//
// 覆盖分层：
//   L1  正常路径          — doc_id-only / 带 relevance 的逐行输出
//   L2  分支              — read_info 投影（截断 / 扩展），value block
//   L3  条件              — row_count 不匹配、未打开、OB_ITER_END
//   L5  路径 / 生命周期   — peek doc_id、推进后失效、reuse / reset
// =============================================================================

class TestInvIdxLogicalBlockIter : public ::testing::Test
{
public:
  TestInvIdxLogicalBlockIter() : allocator_(ObModIds::TEST) {}
  virtual ~TestInvIdxLogicalBlockIter() {}

protected:
  // 构造一个 posting（logical）block，buffer 位于 allocator_ 中。
  int build_posting_block(
      const bool with_relevance,
      const int64_t row_cnt,
      const uint64_t *doc_ids,
      const uint64_t *token_freqs,
      const uint64_t *doc_lengths,
      ObPostingBlockDesc &desc)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColDesc> col_descs;
    const int64_t col_cnt = with_relevance ? 3 : 1;
    for (int64_t i = 0; i < col_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_type_.set_uint64();
      if (OB_FAIL(col_descs.push_back(col_desc))) {
        return ret;
      }
    }
    ObPostingBlockEncodingCtx ctx;
    ctx.reset();
    ctx.col_descs_ = &col_descs;
    ctx.block_capacity_ = 256;
    ctx.doc_id_col_idx_ = 0;
    ctx.token_freq_col_idx_ = with_relevance ? 1 : -1;
    ctx.doc_length_col_idx_ = with_relevance ? 2 : -1;
    const int64_t max_pair_count = with_relevance ? TEST_BM25_MAX_PAIR_COUNT : 0;
    ObPostingBlockEncoder encoder;
    if (OB_FAIL(encoder.init(ctx, allocator_, max_pair_count))) {
    } else {
      ObDatumRow row;
      if (OB_FAIL(row.init(allocator_, col_cnt))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        row.count_ = static_cast<uint16_t>(col_cnt);
        row.storage_datums_[0].set_uint(doc_ids[i]);
        if (with_relevance) {
          row.storage_datums_[1].set_uint(token_freqs[i]);
          row.storage_datums_[2].set_uint(doc_lengths[i]);
        }
        ret = encoder.append_row(row);
      }
      if (OB_SUCC(ret)) {
        ObPostingBlockDesc tmp;
        if (OB_FAIL(encoder.build_block(tmp))) {
        } else {
          // 从 encoder 拥有的 buffer 拷贝出来，使其在 encoder 销毁后仍然有效
          char *buf = static_cast<char *>(allocator_.alloc(tmp.size_));
          if (nullptr == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(buf, tmp.buf_, tmp.size_);
            desc = tmp;
            desc.buf_ = buf;
          }
        }
      }
    }
    return ret;
  }

  // 构造一个按行序与 posting block 对齐的 posting value block。
  int build_value_block(
      const bool with_nested_key,
      const int64_t row_cnt,
      const char **pos_bufs,
      const int64_t *pos_lens,
      const char **nested_bufs,
      const int64_t *nested_lens,
      ObPostingValueBlockDesc &desc)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColDesc> col_descs;
    const int64_t col_cnt = with_nested_key ? 2 : 1;
    for (int64_t i = 0; i < col_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_type_.set_varchar();
      if (OB_FAIL(col_descs.push_back(col_desc))) {
        return ret;
      }
    }
    ObPostingValueBlockWriteParam param;
    param.col_descs_ = &col_descs;
    param.block_capacity_ = 256;
    param.position_list_col_idx_ = 0;
    param.nested_key_col_idx_ = with_nested_key ? 1 : -1;
    ObPostingValueBlockWriter writer;
    if (OB_FAIL(writer.init(param, allocator_))) {
    } else {
      ObDatumRow row;
      if (OB_FAIL(row.init(allocator_, col_cnt))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        row.count_ = static_cast<uint16_t>(col_cnt);
        row.storage_datums_[0].ptr_ = pos_bufs[i];
        row.storage_datums_[0].pack_ = static_cast<uint32_t>(pos_lens[i]);
        if (with_nested_key) {
          row.storage_datums_[1].ptr_ = nested_bufs[i];
          row.storage_datums_[1].pack_ = static_cast<uint32_t>(nested_lens[i]);
        }
        ret = writer.append_row(row);
      }
      if (OB_SUCC(ret)) {
        ObPostingValueBlockDesc tmp;
        if (OB_FAIL(writer.build_block(tmp))) {
        } else {
          char *buf = static_cast<char *>(allocator_.alloc(tmp.size_));
          if (nullptr == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(buf, tmp.buf_, tmp.size_);
            desc = tmp;
            desc.buf_ = buf;
          }
        }
      }
    }
    return ret;
  }

  // 构造 read_info：scalar_cnt 个 uint64 列 + value_cnt 个 varchar 列。
  int build_read_info(
      TestReadInfo &read_info,
      const int64_t scalar_cnt,
      const int64_t value_cnt)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColDesc> col_descs;
    for (int64_t i = 0; OB_SUCC(ret) && i < scalar_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_type_.set_uint64();
      ret = col_descs.push_back(col_desc);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < value_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_type_.set_varchar();
      ret = col_descs.push_back(col_desc);
    }
    if (OB_SUCC(ret)) {
      const int64_t total = scalar_cnt + value_cnt;
      ret = read_info.init(allocator_, total, 1 /*schema_rowkey_cnt*/, false /*oracle*/, col_descs);
    }
    return ret;
  }

  // 构造 read_info，每个输出列类型由 col_types[i] 指定（doc_id/tf/doc_length
  // 用 uint64，pos/nested 用 varchar）。列角色不再由 read_info 决定，而是由传给
  // open() 的 ObInvIdxRowDesc 决定，因此这里只负责输出宽度与列类型。
  int build_read_info_types(
      TestReadInfo &read_info,
      const int64_t out_cnt,
      const ObObjType *col_types)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColDesc> col_descs;
    for (int64_t i = 0; OB_SUCC(ret) && i < out_cnt; ++i) {
      ObColDesc col_desc;
      col_desc.col_type_.set_type(col_types[i]);
      ret = col_descs.push_back(col_desc);
    }
    if (OB_SUCC(ret)) {
      ret = read_info.init(allocator_, out_cnt, 1 /*schema_rowkey_cnt*/, false /*oracle*/,
          col_descs);
    }
    return ret;
  }

  // 构造 ObInvIdxRowDesc：指定各 FTS 列在输出行中的位置；-1 表示该列不存在。
  // is_valid() 要求 doc_id_col_idx_ >= 0 且 token_freq/doc_length 同时存在或同时缺失。
  static ObInvIdxRowDesc make_row_desc(
      const int16_t doc_id_idx,
      const int16_t token_freq_idx = -1,
      const int16_t doc_length_idx = -1,
      const int16_t pos_list_idx = -1,
      const int16_t nested_key_idx = -1)
  {
    ObInvIdxRowDesc rd;
    rd.doc_id_col_idx_ = doc_id_idx;
    rd.token_freq_col_idx_ = token_freq_idx;
    rd.doc_length_col_idx_ = doc_length_idx;
    rd.pos_list_col_idx_ = pos_list_idx;
    rd.nested_key_col_idx_ = nested_key_idx;
    return rd;
  }

protected:
  ObArenaAllocator allocator_;
};

// =============================================================================
// L1. doc_id-only 读写一致性 — 逐行输出，doc_id 递增，OB_ITER_END
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, doc_id_only_round_trip)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));
  ASSERT_EQ(row_cnt, iter.get_row_count());

  uint64_t prev = 0;
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_NE(nullptr, row);
    ASSERT_EQ(1, row->get_column_count());
    const uint64_t doc_id = row->storage_datums_[0].get_uint();
    ASSERT_EQ(doc_ids[i], doc_id);
    if (i > 0) {
      ASSERT_LT(prev, doc_id);
    }
    prev = doc_id;
  }
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

// =============================================================================
// L1/L2. 带 relevance — 三列与解码器一致
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, with_relevance_round_trip)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, row_cnt, doc_ids, token_freqs, doc_lengths, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 3 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/, 1 /*tf*/, 2 /*dl*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(3, row->get_column_count());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[0].get_uint());
    ASSERT_EQ(token_freqs[i], row->storage_datums_[1].get_uint());
    ASSERT_EQ(doc_lengths[i], row->storage_datums_[2].get_uint());
  }
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

// =============================================================================
// L2. read_info 投影 — 请求的列数少于 block 实际拥有的列数
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, projection_doc_id_only_from_relevance_block)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, row_cnt, doc_ids, token_freqs, doc_lengths, desc));

  // row_desc 只声明 doc_id（tf / doc_length 缺失）-> 只物化 doc_id
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(1, row->get_column_count());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[0].get_uint());
  }
}

// =============================================================================
// L2. read_info 超出 block 列范围 — 不存在的列为 nop
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, doc_id_only_block_with_wide_read_info_nop_columns)
{
  const uint64_t doc_ids[] = {1, 2, 3};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  // row_desc 声明 tf / doc_length 位置，但 block 只有 doc_id（无 relevance）-> 二者为 nop
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 3 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/, 1 /*tf*/, 2 /*dl*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(3, row->get_column_count());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[0].get_uint());
    ASSERT_TRUE(row->storage_datums_[1].is_nop());
    ASSERT_TRUE(row->storage_datums_[2].is_nop());
  }
}

// =============================================================================
// L2. row_desc 指定非默认（非前缀）列位置 — 各 FTS 列落到 row_desc 指定的位置
//     out[0] <- token_freq, out[1] <- doc_length, out[2] <- doc_id
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, row_desc_arbitrary_positions)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, row_cnt, doc_ids, token_freqs, doc_lengths, desc));

  // 3 个输出列，FTS 列错位放置：tf@0, dl@1, doc_id@2
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 3 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(2 /*doc_id*/, 0 /*tf*/, 1 /*dl*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(3, row->get_column_count());
    ASSERT_EQ(token_freqs[i], row->storage_datums_[0].get_uint());
    ASSERT_EQ(doc_lengths[i], row->storage_datums_[1].get_uint());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[2].get_uint());
  }
}

// =============================================================================
// L2. 非 FTS 列（未被任何 row_desc 位置命中）保持 nop
//     out[0] <- 无角色 (nop), out[1] <- doc_id
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, non_fts_column_is_nop)
{
  const uint64_t doc_ids[] = {7, 8};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  // 2 个输出列，doc_id 在位置 1；位置 0 无 FTS 角色（模拟 term / MV 列）-> nop
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 2 /*scalar*/, 0 /*value*/));
  const ObInvIdxRowDesc rd = make_row_desc(1 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(2, row->get_column_count());
    ASSERT_TRUE(row->storage_datums_[0].is_nop());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[1].get_uint());
  }
}

// =============================================================================
// L2. row_desc 完整 5 列置换 + value block
//     输出顺序: [nested_key, doc_id, position_list, doc_length, token_freq]
//     row_desc  : nested@0, doc_id@1, pos@2, dl@3, tf@4
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, row_desc_full_permutation_with_value_block)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc posting_desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, row_cnt, doc_ids, token_freqs, doc_lengths, posting_desc));

  const char pos0[] = {1, 2, 3};
  const char pos1[] = {9, 8};
  const char pos2[] = {4};
  const char nested0[] = {7, 7};
  const char nested1[] = {6};
  const char nested2[] = {5, 5, 5};
  const char *pos_bufs[] = {pos0, pos1, pos2};
  const int64_t pos_lens[] = {sizeof(pos0), sizeof(pos1), sizeof(pos2)};
  const char *nested_bufs[] = {nested0, nested1, nested2};
  const int64_t nested_lens[] = {sizeof(nested0), sizeof(nested1), sizeof(nested2)};
  ObPostingValueBlockDesc value_desc;
  ASSERT_EQ(OB_SUCCESS, build_value_block(true, row_cnt, pos_bufs, pos_lens, nested_bufs, nested_lens, value_desc));

  // 请求所有五个语义列，以打乱的顺序
  const ObObjType col_types[] = {
      ObVarcharType /*nested_key*/, ObUInt64Type /*doc_id*/, ObVarcharType /*position_list*/,
      ObUInt64Type /*doc_length*/, ObUInt64Type /*token_freq*/};
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info_types(read_info, 5, col_types));
  // nested@0, doc_id@1, pos@2, dl@3, tf@4
  const ObInvIdxRowDesc rd = make_row_desc(1 /*doc_id*/, 4 /*tf*/, 3 /*dl*/, 2 /*pos*/, 0 /*nested*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(posting_desc, read_info, rd, allocator_, &value_desc));
  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(5, row->get_column_count());
    // out0 = nested_key, out1 = doc_id, out2 = position_list, out3 = doc_length, out4 = token_freq
    const ObStorageDatum &nested = row->storage_datums_[0];
    ASSERT_EQ(nested_lens[i], nested.len_);
    ASSERT_EQ(0, MEMCMP(nested_bufs[i], nested.ptr_, nested_lens[i]));
    ASSERT_EQ(doc_ids[i], row->storage_datums_[1].get_uint());
    const ObStorageDatum &pos = row->storage_datums_[2];
    ASSERT_EQ(pos_lens[i], pos.len_);
    ASSERT_EQ(0, MEMCMP(pos_bufs[i], pos.ptr_, pos_lens[i]));
    ASSERT_EQ(doc_lengths[i], row->storage_datums_[3].get_uint());
    ASSERT_EQ(token_freqs[i], row->storage_datums_[4].get_uint());
  }
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

// =============================================================================
// L2/L5. value block — position_list / nested_key 按行顺序物化
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, value_block_position_and_nested_key)
{
  const uint64_t doc_ids[] = {10, 20};
  const uint64_t token_freqs[] = {3, 5};
  const uint64_t doc_lengths[] = {100, 120};
  const int64_t row_cnt = 2;
  ObPostingBlockDesc posting_desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, row_cnt, doc_ids, token_freqs, doc_lengths, posting_desc));

  const char pos0[] = {1, 2, 3};
  const char pos1[] = {9, 8};
  const char nested0[] = {7, 7};
  const char nested1[] = {6};
  const char *pos_bufs[] = {pos0, pos1};
  const int64_t pos_lens[] = {sizeof(pos0), sizeof(pos1)};
  const char *nested_bufs[] = {nested0, nested1};
  const int64_t nested_lens[] = {sizeof(nested0), sizeof(nested1)};
  ObPostingValueBlockDesc value_desc;
  ASSERT_EQ(OB_SUCCESS, build_value_block(true, row_cnt, pos_bufs, pos_lens, nested_bufs, nested_lens, value_desc));

  // 请求 doc_id, tf, doc_length, position_list, nested_key（默认布局 0..4）
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 3 /*scalar*/, 2 /*value*/));
  const ObInvIdxRowDesc rd =
      make_row_desc(0 /*doc_id*/, 1 /*tf*/, 2 /*dl*/, 3 /*pos*/, 4 /*nested*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(posting_desc, read_info, rd, allocator_, &value_desc));

  for (int64_t i = 0; i < row_cnt; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(5, row->get_column_count());
    ASSERT_EQ(doc_ids[i], row->storage_datums_[0].get_uint());
    const ObStorageDatum &pos = row->storage_datums_[3];
    const ObStorageDatum &nested = row->storage_datums_[4];
    ASSERT_EQ(pos_lens[i], pos.len_);
    ASSERT_EQ(0, MEMCMP(pos_bufs[i], pos.ptr_, pos_lens[i]));
    ASSERT_EQ(nested_lens[i], nested.len_);
    ASSERT_EQ(0, MEMCMP(nested_bufs[i], nested.ptr_, nested_lens[i]));
  }
}

// =============================================================================
// L3. value block 行数不匹配 -> open 失败
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, value_block_row_count_mismatch)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  ObPostingBlockDesc posting_desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(true, 3, doc_ids, token_freqs, doc_lengths, posting_desc));

  // value block 只有 2 行，posting block 有 3 行 -> 不匹配
  const char pos0[] = {1};
  const char pos1[] = {2};
  const char *pos_bufs[] = {pos0, pos1};
  const int64_t pos_lens[] = {sizeof(pos0), sizeof(pos1)};
  ObPostingValueBlockDesc value_desc;
  ASSERT_EQ(OB_SUCCESS, build_value_block(false, 2, pos_bufs, pos_lens, nullptr, nullptr, value_desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 3, 2));
  const ObInvIdxRowDesc rd =
      make_row_desc(0 /*doc_id*/, 1 /*tf*/, 2 /*dl*/, 3 /*pos*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(posting_desc, read_info, rd, allocator_, &value_desc));
}

// =============================================================================
// L5. 生命周期 — 查看 doc_id（不推进）、推进使前一行失效、reuse、reset
// =============================================================================

// next() + get_curr_row() 原语：在第一次 next() 之前没有当前行（但迭代器未结束）；
// 每次 next() 之后 get_curr_row() 可重复调用；越过最后一行后 next() 返回 OB_ITER_END。
TEST_F(TestInvIdxLogicalBlockIter, next_get_curr_row_primitives)
{
  const uint64_t doc_ids[] = {7, 8, 9};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // 第一次 next() 之前：未结束，但没有当前行
  ASSERT_FALSE(iter.is_iter_end());
  ASSERT_EQ(nullptr, iter.get_curr_row());

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.next());
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    // get_curr_row 可重复调用：返回相同对象，不推进
    ASSERT_EQ(row, iter.get_curr_row());
    ASSERT_EQ(doc_ids[i],
        row->storage_datums_[0].get_uint());
  }
  ASSERT_EQ(OB_ITER_END, iter.next());
  ASSERT_TRUE(iter.is_iter_end());
  ASSERT_EQ(nullptr, iter.get_curr_row());
}

// get_curr_doc_id 反映当前行（即 get_curr_row 返回的行），是幂等的且不推进游标；
// 首次 next() 之前 / 结束后返回 OB_ITER_END
TEST_F(TestInvIdxLogicalBlockIter, get_curr_doc_id_matches_curr_row)
{
  const uint64_t doc_ids[] = {100, 200, 300};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // 第一次 next() 之前：没有当前行
  uint64_t peek = 0;
  ASSERT_EQ(OB_ITER_END, iter.get_curr_doc_id(peek));

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.next());
    // 当前 doc_id 是幂等的，不推进游标
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_doc_id(peek));
    ASSERT_EQ(doc_ids[i], peek);
    uint64_t peek_again = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_doc_id(peek_again));
    ASSERT_EQ(doc_ids[i], peek_again);
    // 与物化后的当前行 doc_id 列一致
    ASSERT_EQ(doc_ids[i],
        iter.get_curr_row()->storage_datums_[0].get_uint());
  }
  ASSERT_EQ(OB_ITER_END, iter.next());
  ASSERT_EQ(OB_ITER_END, iter.get_curr_doc_id(peek));
}

TEST_F(TestInvIdxLogicalBlockIter, advance_invalidates_previous_row)
{
  const uint64_t doc_ids[] = {100, 200};
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, 2, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  const ObDatumRow *row0 = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row0));
  ASSERT_EQ(100U, row0->storage_datums_[0].get_uint());

  const ObDatumRow *row1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row1));
  // 内部 buffer 被复用：同一行对象，内容被覆盖
  ASSERT_EQ(row0, row1);
  ASSERT_EQ(200U, row1->storage_datums_[0].get_uint());
}

TEST_F(TestInvIdxLogicalBlockIter, reuse_rewinds_cursor)
{
  const uint64_t doc_ids[] = {1, 2, 3};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(1U, row->storage_datums_[0].get_uint());

  iter.reuse();
  // reuse 之后，迭代从第一行重新开始
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(1U, row->storage_datums_[0].get_uint());
}

TEST_F(TestInvIdxLogicalBlockIter, reset_then_reopen)
{
  const uint64_t doc_ids_a[] = {1, 2};
  const uint64_t doc_ids_b[] = {10, 20, 30};
  ObPostingBlockDesc desc_a;
  ObPostingBlockDesc desc_b;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, 2, doc_ids_a, nullptr, nullptr, desc_a));
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, 3, doc_ids_b, nullptr, nullptr, desc_b));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc_a, read_info, rd, allocator_));
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));

  iter.reset();
  ASSERT_FALSE(iter.is_opened());
  uint64_t doc_id = 0;
  ASSERT_EQ(OB_NOT_INIT, iter.get_curr_doc_id(doc_id));

  // 在不同 block 上重新 open
  ASSERT_EQ(OB_SUCCESS, iter.open(desc_b, read_info, rd, allocator_));
  ASSERT_EQ(3, iter.get_row_count());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(doc_ids_b[i], row->storage_datums_[0].get_uint());
  }
}

// =============================================================================
// L5. advance_to — block 内向前跳过，定位到第一个 doc_id >= target 的行
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, advance_to_lands_on_first_ge_target)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40, 50};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // 精确命中：target == 某个已存在的 doc_id -> 直接定位到该行作为当前行
  ASSERT_EQ(OB_SUCCESS, iter.advance_to(30));
  uint64_t peek = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_doc_id(peek));
  ASSERT_EQ(30U, peek);
  ASSERT_EQ(30U, iter.get_curr_row()->storage_datums_[0].get_uint());
  // next() 从定位行前进到 doc_id 40；后续按顺序输出
  ASSERT_EQ(OB_SUCCESS, iter.next());
  ASSERT_EQ(40U, iter.get_curr_row()->storage_datums_[0].get_uint());
  ASSERT_EQ(OB_SUCCESS, iter.next());
  ASSERT_EQ(50U, iter.get_curr_row()->storage_datums_[0].get_uint());
}

TEST_F(TestInvIdxLogicalBlockIter, advance_to_gap_lands_on_next)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // target 落入间隙 (25) -> 定位到第一个 doc_id >= 25 的行，即 30
  ASSERT_EQ(OB_SUCCESS, iter.advance_to(25));
  ASSERT_EQ(30U, iter.get_curr_row()->storage_datums_[0].get_uint());
}

TEST_F(TestInvIdxLogicalBlockIter, advance_to_is_monotonic_no_rewind)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // 将当前行推进到第二行 (20)
  ASSERT_EQ(OB_SUCCESS, iter.next()); // current = 10
  ASSERT_EQ(OB_SUCCESS, iter.next()); // current = 20
  // 在当前行之后的 target (15) 是空操作：游标停留在 20
  ASSERT_EQ(OB_SUCCESS, iter.advance_to(15));
  uint64_t peek = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_doc_id(peek));
  ASSERT_EQ(20U, peek);
  ASSERT_EQ(20U, iter.get_curr_row()->storage_datums_[0].get_uint());
}

TEST_F(TestInvIdxLogicalBlockIter, advance_to_beyond_block_iter_end)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(false, row_cnt, doc_ids, nullptr, nullptr, desc));

  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(desc, read_info, rd, allocator_));

  // target 大于所有 doc_id -> OB_ITER_END，游标停留在末尾
  ASSERT_EQ(OB_ITER_END, iter.advance_to(100));
  ASSERT_TRUE(iter.is_iter_end());
  ASSERT_EQ(nullptr, iter.get_curr_row());
  uint64_t peek = 0;
  ASSERT_EQ(OB_ITER_END, iter.get_curr_doc_id(peek));
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

TEST_F(TestInvIdxLogicalBlockIter, advance_to_not_opened)
{
  ObInvRowLogicalBlockIterator iter;
  ASSERT_EQ(OB_NOT_INIT, iter.advance_to(10));
}

// =============================================================================
// L3. 异常路径 — 未 open、无效参数
// =============================================================================

TEST_F(TestInvIdxLogicalBlockIter, not_opened)
{
  ObInvRowLogicalBlockIterator iter;
  ASSERT_TRUE(iter.is_iter_end());
  ASSERT_EQ(nullptr, iter.get_curr_row());
  ASSERT_EQ(OB_NOT_INIT, iter.next());
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
  uint64_t doc_id = 0;
  ASSERT_EQ(OB_NOT_INIT, iter.get_curr_doc_id(doc_id));
}

TEST_F(TestInvIdxLogicalBlockIter, open_invalid_args)
{
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, build_read_info(read_info, 1, 0));
  const ObInvIdxRowDesc rd = make_row_desc(0 /*doc_id*/);

  ObInvRowLogicalBlockIterator iter;
  ObPostingBlockDesc invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(invalid, read_info, rd, allocator_));
}

// =============================================================================
// ObInvIdxLogicalBlockIterator — micro block -> logical block 迭代器
//
// 覆盖层级：
//   L1  正常路径   — 顺序输出，内容/大小一致性，
//                    最后一个 block 之后返回 OB_ITER_END
//   L3  条件分支   — 拒绝无效 payload header、拒绝 meta/count 不匹配、
//                    未 open、get_curr_block 边界情况
//   L5  路径/生命周期 — 通过注入 meta 和按需解码获取 range meta，
//                        range 查询不推进游标，reuse()
// =============================================================================

class TestInvIdxMicroBlockIter : public ::testing::Test
{
public:
  TestInvIdxMicroBlockIter() : allocator_(ObModIds::TEST) {}
  virtual ~TestInvIdxMicroBlockIter() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

protected:
  // 构造一个 micro block payload，将给定的原始 logical block 拼接在一起。
  // payload_buf/payload_size 输出参数指向 allocator_ 拥有的内存。
  // block_bufs/block_sizes 记录下来用于后续内容校验。
  int build_micro_block(
      const char *const *block_bufs,
      const int64_t *block_sizes,
      const int64_t block_cnt,
      const char *&payload_buf,
      int64_t &payload_size)
  {
    int ret = OB_SUCCESS;
    ObInvIdxMicroBlockWriter writer;
    if (OB_FAIL(writer.init(allocator_))) {
    } else {
      int64_t block_idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < block_cnt; ++i) {
        ret = writer.append_logical_block(block_bufs[i], block_sizes[i], block_idx);
      }
      if (OB_SUCC(ret)) {
        const char *buf = nullptr;
        int64_t size = 0;
        if (OB_FAIL(writer.build_micro_block(buf, size))) {
        } else {
          // 拷贝出来，使其在 writer 销毁后仍然有效
          char *owned = static_cast<char *>(allocator_.alloc(size));
          if (nullptr == owned) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(owned, buf, size);
            payload_buf = owned;
            payload_size = size;
          }
        }
      }
    }
    return ret;
  }

  // 构造一个 posting（logical）block，包含给定的 doc_ids（仅 doc_id，无相关性参数）。
  // buffer 位于 allocator_ 中。
  int build_posting_block(
      const int64_t row_cnt,
      const uint64_t *doc_ids,
      ObPostingBlockDesc &desc)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColDesc> col_descs;
    ObColDesc col_desc;
    col_desc.col_type_.set_uint64();
    if (OB_FAIL(col_descs.push_back(col_desc))) {
    } else {
      ObPostingBlockEncodingCtx ctx;
      ctx.reset();
      ctx.col_descs_ = &col_descs;
      ctx.block_capacity_ = 256;
      ctx.doc_id_col_idx_ = 0;
      ctx.token_freq_col_idx_ = -1;
      ctx.doc_length_col_idx_ = -1;
      ObPostingBlockEncoder encoder;
      if (OB_FAIL(encoder.init(ctx, allocator_, 0 /*max_pair_count*/))) {
      } else {
        ObDatumRow row;
        if (OB_FAIL(row.init(allocator_, 1))) {
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
          row.count_ = 1;
          row.storage_datums_[0].set_uint(doc_ids[i]);
          ret = encoder.append_row(row);
        }
        if (OB_SUCC(ret)) {
          ObPostingBlockDesc tmp;
          if (OB_FAIL(encoder.build_block(tmp))) {
          } else {
            char *buf = static_cast<char *>(allocator_.alloc(tmp.size_));
            if (nullptr == buf) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(buf, tmp.buf_, tmp.size_);
              desc = tmp;
              desc.buf_ = buf;
            }
          }
        }
      }
    }
    return ret;
  }

  // 构造一个按 block_idx 1:1 对齐的 ObIArray<ObInvIdxLogicalBlockMeta>。
  // 仅填充 range 字段（frontier_pairs_ 留空 — micro block 迭代器不需要它）。
  int build_metas(
      const int64_t block_cnt,
      const uint64_t *min_doc_ids,
      const uint64_t *max_doc_ids,
      const int64_t *doc_counts,
      ObArray<ObInvIdxLogicalBlockMeta> &metas)
  {
    int ret = OB_SUCCESS;
    metas.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < block_cnt; ++i) {
      ObInvIdxLogicalBlockMeta meta;
      meta.block_idx_ = i;
      meta.doc_count_ = doc_counts[i];
      meta.min_doc_id_ = min_doc_ids[i];
      meta.max_doc_id_ = max_doc_ids[i];
      meta.frontier_pairs_.reset();
      ret = metas.push_back(meta);
    }
    return ret;
  }

  // 将一个借用的 posting-data micro block payload 包装为 ObMicroBlockData，
  // 供 ObInvIdxLogicalBlockIterator::open() 使用。仅 buf_/size_ 被消费。
  void make_micro_block_data(
      const char *payload_buf,
      const int64_t payload_size,
      ObMicroBlockData &data) const
  {
    data.reset();
    data.get_buf() = payload_buf;
    data.get_buf_size() = payload_size;
    data.type_ = ObMicroBlockData::INV_IDX_POSTING_BLOCK;
  }

protected:
  ObArenaAllocator allocator_;
};

// =============================================================================
// L1. 顺序输出 — 内容/大小一致性，OB_ITER_END
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, sequential_yield_and_iter_end)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char block2[] = "ijklmno";
  const char *block_bufs[] = {block0, block1, block2};
  const int64_t block_sizes[] = {3, 5, 7};
  const int64_t block_cnt = 3;

  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, block_cnt, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));
  ASSERT_TRUE(iter.is_opened());
  EXPECT_EQ(block_cnt, iter.get_block_count());
  EXPECT_FALSE(iter.has_meta());

  for (int64_t i = 0; i < block_cnt; ++i) {
    ObPostingBlockDesc posting_block;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block)) << "i=" << i;
    EXPECT_EQ(block_sizes[i], posting_block.size_);
    ASSERT_NE(nullptr, posting_block.buf_);
    EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[i], block_sizes[i]));
    // 视图指向 payload 内部
    EXPECT_GE(posting_block.buf_, payload_buf);
    EXPECT_LE(posting_block.buf_ + posting_block.size_, payload_buf + payload_size);
  }
  // 第 (block_cnt + 1) 次调用 -> OB_ITER_END
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
  EXPECT_TRUE(iter.is_iter_end());
}

// =============================================================================
// L1. 单个 logical block
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, single_logical_block)
{
  const char block[] = "hello";
  const char *block_bufs[] = {block};
  const int64_t block_sizes[] = {5};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 1, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));
  EXPECT_EQ(1, iter.get_block_count());

  ObPostingBlockDesc posting_block;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(5, posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block, 5));
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
}

// =============================================================================
// L5. get_curr_block — 可重复调用，不推进；首次 next 之前 / 结束后返回 OB_ITER_END
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, get_curr_block_repeatable_no_advance)
{
  const char block0[] = "ab";
  const char block1[] = "cde";
  const char *block_bufs[] = {block0, block1};
  const int64_t block_sizes[] = {2, 3};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));

  // 第一次 next() 之前：没有当前 block（但未结束）
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_ITER_END, iter.get_curr_block(posting_block));
  EXPECT_FALSE(iter.is_iter_end());

  ASSERT_EQ(OB_SUCCESS, iter.next());
  // get_curr_block 返回同一 block，可重复调用，不推进
  ObPostingBlockDesc curr_block;
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_block(curr_block));
  EXPECT_EQ(2, curr_block.size_);
  EXPECT_EQ(0, MEMCMP(curr_block.buf_, block0, 2));
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_block(curr_block));
  EXPECT_EQ(2, curr_block.size_);
  EXPECT_EQ(0, MEMCMP(curr_block.buf_, block0, 2));

  // 推进到 block 1，没有跳过
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(3, posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block1, 3));

  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
  EXPECT_EQ(OB_ITER_END, iter.get_curr_block(posting_block));
  EXPECT_TRUE(iter.is_iter_end());
}

// =============================================================================
// L5. reuse — 回退到第一个 block，保持 payload 绑定
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, reuse_rewinds_to_first_block)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char *block_bufs[] = {block0, block1};
  const int64_t block_sizes[] = {3, 5};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));

  ObPostingBlockDesc posting_block;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block0, 3));
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block1, 5));
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));

  iter.reuse();
  // reuse 之后，迭代从同一 payload 的第一个 block 重新开始
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(3, posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block0, 3));
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block1, 5));
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
}

// =============================================================================
// L5. reset — 解除 payload 绑定，可重新 open
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, reset_then_reopen)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char *block_bufs[] = {block0, block1};
  const int64_t block_sizes[] = {3, 5};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));
  ObPostingBlockDesc posting_block;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));

  iter.reset();
  EXPECT_FALSE(iter.is_opened());
  // reset 之后处于未 open 状态
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_block(posting_block));
  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  EXPECT_EQ(OB_NOT_INIT, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));

  // 在同一 payload 上重新 open
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));
  EXPECT_EQ(2, iter.get_block_count());
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block0, 3));
}

// =============================================================================
// L3. 无效 payload header / 参数被拒绝
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, open_invalid_args)
{
  const char block[] = "abc";
  const char *block_bufs[] = {block};
  const int64_t block_sizes[] = {3};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 1, payload_buf, payload_size));

  ObInvIdxLogicalBlockIterator iter;
  ObMicroBlockData micro_block_data;
  make_micro_block_data(nullptr, payload_size, micro_block_data);
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data));
  make_micro_block_data(payload_buf, 0, micro_block_data);
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data));
  // buffer 小于 payload header 大小
  make_micro_block_data(payload_buf,
      static_cast<int64_t>(sizeof(ObInvIdxMicroBlockPayloadHeader)) - 1,
      micro_block_data);
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data));
}

// 损坏 payload header 使 is_valid() 失败（block_count_ == 0），
// 并验证 open() 拒绝该数据且迭代器保持未 open 状态。
TEST_F(TestInvIdxMicroBlockIter, open_invalid_payload_header)
{
  const char block[] = "abc";
  const char *block_bufs[] = {block};
  const int64_t block_sizes[] = {3};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 1, payload_buf, payload_size));

  // 损坏一份拷贝：将 block_count_ 置零 -> is_valid() 失败
  char *corrupted = static_cast<char *>(allocator_.alloc(payload_size));
  ASSERT_NE(nullptr, corrupted);
  MEMCPY(corrupted, payload_buf, payload_size);
  ObInvIdxMicroBlockPayloadHeader *header =
      reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(corrupted);
  header->block_count_ = 0;

  ObInvIdxLogicalBlockIterator iter;
  ObMicroBlockData micro_block_data;
  make_micro_block_data(corrupted, payload_size, micro_block_data);
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.open(micro_block_data));
  EXPECT_FALSE(iter.is_opened());
  // 后续 get_next_block() 返回 NOT_INIT 而非输出脏数据
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_block(posting_block));
}

// 声明的 total_size_ 超过提供的 buffer 大小的损坏 payload 被拒绝。
TEST_F(TestInvIdxMicroBlockIter, open_payload_size_smaller_than_total_size)
{
  const char block[] = "abc";
  const char *block_bufs[] = {block};
  const int64_t block_sizes[] = {3};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 1, payload_buf, payload_size));

  ObInvIdxLogicalBlockIterator iter;
  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size - 1, micro_block_data);
  // 截断尾部一个字节 -> payload_size < header->total_size_
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.open(micro_block_data));
}

// =============================================================================
// L3. 注入的 meta 不匹配 — 数量不匹配和 block_idx 不对齐
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, open_meta_count_exceeds_block_count)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char *block_bufs[] = {block0, block1};
  const int64_t block_sizes[] = {3, 5};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  // micro block 有 2 个 block 但提供了 3 个 meta -> meta 数量超过物理 block 数量
  // 不可能是有效的 term 子集
  const uint64_t min_doc_ids[] = {1, 10, 20};
  const uint64_t max_doc_ids[] = {3, 14, 24};
  const int64_t doc_counts[] = {3, 5, 5};
  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ASSERT_EQ(OB_SUCCESS, build_metas(3, min_doc_ids, max_doc_ids, doc_counts, metas));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data, &metas));
  EXPECT_FALSE(iter.is_opened());
  EXPECT_FALSE(iter.has_meta());
}

TEST_F(TestInvIdxMicroBlockIter, open_meta_block_idx_out_of_range)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char *block_bufs[] = {block0, block1};
  const int64_t block_sizes[] = {3, 5};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  // 数量在范围内 (2) 但第二个 meta 的 block_idx (2) >= block_count (2)
  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0;
  m0.block_idx_ = 0;
  m0.doc_count_ = 3;
  m0.min_doc_id_ = 1;
  m0.max_doc_id_ = 3;
  m0.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));
  ObInvIdxLogicalBlockMeta m1;
  m1.block_idx_ = 2; // 超出范围 [0, block_count)
  m1.doc_count_ = 2;
  m1.min_doc_id_ = 10;
  m1.max_doc_id_ = 11;
  m1.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m1));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data, &metas));
}

// block_idx 在范围内但不是严格递增的被拒绝（游标必须按升序遍历 term 的物理 block）。
TEST_F(TestInvIdxMicroBlockIter, open_meta_block_idx_not_increasing)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char block2[] = "ijklmno";
  const char *block_bufs[] = {block0, block1, block2};
  const int64_t block_sizes[] = {3, 5, 7};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 3, payload_buf, payload_size));

  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0;
  m0.block_idx_ = 1;
  m0.doc_count_ = 3;
  m0.min_doc_id_ = 1;
  m0.max_doc_id_ = 3;
  m0.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));
  ObInvIdxLogicalBlockMeta m1;
  m1.block_idx_ = 1; // 重复，非严格递增
  m1.doc_count_ = 2;
  m1.min_doc_id_ = 10;
  m1.max_doc_id_ = 11;
  m1.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m1));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data, &metas));
}

// 一个 term 仅占用 micro block 物理 block 的严格子集：meta 定义了迭代域，
// 因此游标仅遍历这些 block；内容验证是否定位到了对应的底层物理 block。
TEST_F(TestInvIdxMicroBlockIter, open_meta_term_subset_domain)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char block2[] = "ijklmno";
  const char *block_bufs[] = {block0, block1, block2};
  const int64_t block_sizes[] = {3, 5, 7};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 3, payload_buf, payload_size));

  // term 占用物理 block 1 和 2（block 0 属于另一个 term）
  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0;
  m0.block_idx_ = 1;
  m0.doc_count_ = 5;
  m0.min_doc_id_ = 10;
  m0.max_doc_id_ = 14;
  m0.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));
  ObInvIdxLogicalBlockMeta m1;
  m1.block_idx_ = 2;
  m1.doc_count_ = 7;
  m1.min_doc_id_ = 100;
  m1.max_doc_id_ = 106;
  m1.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m1));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data, &metas));
  EXPECT_TRUE(iter.has_meta());
  EXPECT_EQ(3, iter.get_block_count()); // 物理 block 数量，不变

  ObPostingBlockDesc posting_block;
  // 第一个域 block -> 物理 block 1
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(5, posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block1, 5));
  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
  EXPECT_EQ(10U, min_id);
  EXPECT_EQ(14U, max_id);
  EXPECT_EQ(5, doc_cnt);
  // 第二个域 block -> 物理 block 2
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(7, posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block2, 7));
  // 该 term 的 2 个 block 遍历完后域耗尽（block 0 从未输出）
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
  EXPECT_TRUE(iter.is_iter_end());
}

// 即使 count/idx 对齐，range 无效 (min > max) 的 meta 也被拒绝。
TEST_F(TestInvIdxMicroBlockIter, open_meta_invalid_range)
{
  const char block0[] = "abc";
  const char *block_bufs[] = {block0};
  const int64_t block_sizes[] = {3};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 1, payload_buf, payload_size));

  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ObInvIdxLogicalBlockMeta m0;
  m0.block_idx_ = 0;
  m0.doc_count_ = 3;
  m0.min_doc_id_ = 10;
  m0.max_doc_id_ = 5; // min > max
  m0.frontier_pairs_.reset();
  ASSERT_EQ(OB_SUCCESS, metas.push_back(m0));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(micro_block_data, &metas));
}

// =============================================================================
// L5. 通过注入 meta 获取 range meta — 零解码，不推进游标
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, range_meta_injected_path)
{
  const char block0[] = "abc";
  const char block1[] = "defgh";
  const char block2[] = "ijklmno";
  const char *block_bufs[] = {block0, block1, block2};
  const int64_t block_sizes[] = {3, 5, 7};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 3, payload_buf, payload_size));

  const uint64_t min_doc_ids[] = {1, 10, 100};
  const uint64_t max_doc_ids[] = {3, 14, 107};
  const int64_t doc_counts[] = {3, 5, 8};
  ObArray<ObInvIdxLogicalBlockMeta> metas;
  ASSERT_EQ(OB_SUCCESS, build_metas(3, min_doc_ids, max_doc_ids, doc_counts, metas));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data, &metas));
  EXPECT_TRUE(iter.has_meta());

  // 第一次 next() 之前：range 查询返回 OB_ITER_END
  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  EXPECT_EQ(OB_ITER_END, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));

  for (int64_t i = 0; i < 3; ++i) {
    ObPostingBlockDesc posting_block;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
    EXPECT_EQ(block_sizes[i], posting_block.size_);
    EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[i], block_sizes[i]));

    // range 查询返回注入的 meta，不推进游标
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
    EXPECT_EQ(min_doc_ids[i], min_id);
    EXPECT_EQ(max_doc_ids[i], max_id);
    EXPECT_EQ(doc_counts[i], doc_cnt);
    // 可重复
    uint64_t min2 = 0, max2 = 0;
    int64_t cnt2 = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min2, max2, cnt2));
    EXPECT_EQ(min_doc_ids[i], min2);
    EXPECT_EQ(max_doc_ids[i], max2);
    EXPECT_EQ(doc_counts[i], cnt2);
    // 游标未推进：get_curr_block 仍在 block i 上
    ObPostingBlockDesc curr_block;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_block(curr_block));
    EXPECT_EQ(block_sizes[i], curr_block.size_);
    EXPECT_EQ(0, MEMCMP(curr_block.buf_, block_bufs[i], block_sizes[i]));
  }
  // 最后一个 block 之后：range 查询返回 OB_ITER_END
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
  EXPECT_EQ(OB_ITER_END, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
}

// =============================================================================
// L5. 通过按需解码获取 range meta — 不注入 meta，偷看首/尾 doc_id
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, range_meta_decode_on_demand_path)
{
  // 构造三个真正的 posting block，已知 doc_id 范围
  const uint64_t doc_ids0[] = {1, 2, 3};
  const uint64_t doc_ids1[] = {10, 11, 12, 13, 14};
  const uint64_t doc_ids2[] = {100, 101, 102, 103, 104, 105, 106, 107};
  ObPostingBlockDesc desc0, desc1, desc2;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(3, doc_ids0, desc0));
  ASSERT_EQ(OB_SUCCESS, build_posting_block(5, doc_ids1, desc1));
  ASSERT_EQ(OB_SUCCESS, build_posting_block(8, doc_ids2, desc2));
  const char *block_bufs[] = {desc0.buf_, desc1.buf_, desc2.buf_};
  const int64_t block_sizes[] = {desc0.size_, desc1.size_, desc2.size_};
  const int64_t row_counts[] = {3, 5, 8};
  const uint64_t first_doc_ids[] = {1, 10, 100};
  const uint64_t last_doc_ids[] = {3, 14, 107};

  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 3, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data)); // 无 meta
  EXPECT_FALSE(iter.has_meta());
  EXPECT_EQ(3, iter.get_block_count());

  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  EXPECT_EQ(OB_ITER_END, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));

  for (int64_t i = 0; i < 3; ++i) {
    ObPostingBlockDesc posting_block;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
    EXPECT_EQ(block_sizes[i], posting_block.size_);
    EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[i], block_sizes[i]));

    // 按需解码：偷看当前 block 的首/尾行 doc_id
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
    EXPECT_EQ(first_doc_ids[i], min_id);
    EXPECT_EQ(last_doc_ids[i], max_id);
    EXPECT_EQ(row_counts[i], doc_cnt);

    // 可重复，不推进游标
    uint64_t min2 = 0, max2 = 0;
    int64_t cnt2 = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min2, max2, cnt2));
    EXPECT_EQ(first_doc_ids[i], min2);
    EXPECT_EQ(last_doc_ids[i], max2);
    EXPECT_EQ(row_counts[i], cnt2);
    ObPostingBlockDesc curr_block;
    ASSERT_EQ(OB_SUCCESS, iter.get_curr_block(curr_block));
    EXPECT_EQ(block_sizes[i], curr_block.size_);
    EXPECT_EQ(0, MEMCMP(curr_block.buf_, block_bufs[i], block_sizes[i]));
  }
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_ITER_END, iter.get_next_block(posting_block));
  EXPECT_EQ(OB_ITER_END, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
}

// 游标移动时按需解码重新绑定；reuse() 使缓存的解码失效，
// 下一次 range 查询会重新解码正确的 block。
TEST_F(TestInvIdxMicroBlockIter, range_meta_decode_on_demand_after_reuse)
{
  const uint64_t doc_ids0[] = {5, 6};
  const uint64_t doc_ids1[] = {20, 21, 22};
  ObPostingBlockDesc desc0, desc1;
  ASSERT_EQ(OB_SUCCESS, build_posting_block(2, doc_ids0, desc0));
  ASSERT_EQ(OB_SUCCESS, build_posting_block(3, doc_ids1, desc1));
  const char *block_bufs[] = {desc0.buf_, desc1.buf_};
  const int64_t block_sizes[] = {desc0.size_, desc1.size_};
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, 2, payload_buf, payload_size));

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(micro_block_data));

  ObPostingBlockDesc posting_block;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
  EXPECT_EQ(5U, min_id);
  EXPECT_EQ(6U, max_id);
  EXPECT_EQ(2, doc_cnt);

  iter.reuse();
  // reuse 之后，重新定位到 block 0 并再次解码 — 必须反映 block 0，而非 block 1
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(block_sizes[0], posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[0], block_sizes[0]));
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
  EXPECT_EQ(5U, min_id);
  EXPECT_EQ(6U, max_id);
  EXPECT_EQ(2, doc_cnt);
  // 推进到 block 1 — 解码器重新绑定
  ASSERT_EQ(OB_SUCCESS, iter.get_next_block(posting_block));
  EXPECT_EQ(block_sizes[1], posting_block.size_);
  EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[1], block_sizes[1]));
  ASSERT_EQ(OB_SUCCESS, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
  EXPECT_EQ(20U, min_id);
  EXPECT_EQ(22U, max_id);
  EXPECT_EQ(3, doc_cnt);
}

// =============================================================================
// L3. 未 open
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, not_opened)
{
  ObInvIdxLogicalBlockIterator iter;
  EXPECT_FALSE(iter.is_opened());
  EXPECT_TRUE(iter.is_iter_end());
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_NOT_INIT, iter.next());
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_block(posting_block));
  EXPECT_EQ(OB_NOT_INIT, iter.get_curr_block(posting_block));
  uint64_t min_id = 0, max_id = 0;
  int64_t doc_cnt = 0;
  EXPECT_EQ(OB_NOT_INIT, iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
}

// =============================================================================
// L1. 端到端集成 — micro block 迭代器 -> logical block 迭代器 -> 行
//     驱动完整读取栈：micro block 迭代器输出每个 logical block 的原始字节，
//     直接送入 ObInvRowLogicalBlockIterator 读取行。拼接后的 doc_id 序列
//     必须单调且完整，micro block 迭代器每个 block 的 range meta（按需解码）
//     必须与每个 block 的首/尾行 doc_id 一致。
// =============================================================================

TEST_F(TestInvIdxMicroBlockIter, integrated_micro_to_logical_block_to_rows)
{
  // 三个 posting block，其 doc_id 范围互不相交且递增
  const uint64_t doc_ids0[] = {1, 2, 3};
  const uint64_t doc_ids1[] = {10, 11, 12, 13, 14};
  const uint64_t doc_ids2[] = {100, 101, 102, 103, 104, 105, 106, 107};
  const int64_t row_cnts[] = {3, 5, 8};
  const uint64_t *doc_ids[] = {doc_ids0, doc_ids1, doc_ids2};
  const int64_t block_cnt = 3;
  const int64_t total_rows = 3 + 5 + 8;

  ObPostingBlockDesc descs[3];
  const char *block_bufs[3] = {nullptr, nullptr, nullptr};
  int64_t block_sizes[3] = {0, 0, 0};
  for (int64_t i = 0; i < block_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, build_posting_block(row_cnts[i], doc_ids[i], descs[i]));
    block_bufs[i] = descs[i].buf_;
    block_sizes[i] = descs[i].size_;
  }
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_micro_block(block_bufs, block_sizes, block_cnt, payload_buf, payload_size));

  // 1 列 read_info，仅请求 doc_id（存储列 0）
  ObArray<ObColDesc> col_descs;
  ObColDesc col_desc;
  col_desc.col_type_.set_uint64();
  ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));
  TestReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, 1, 1, false, col_descs));
  ObInvIdxRowDesc rd;
  rd.doc_id_col_idx_ = 0; // 仅 doc_id，tf/dl/pos/nested 缺失

  ObMicroBlockData micro_block_data;
  make_micro_block_data(payload_buf, payload_size, micro_block_data);

  ObInvIdxLogicalBlockIterator micro_iter;
  ASSERT_EQ(OB_SUCCESS, micro_iter.open(micro_block_data));
  EXPECT_EQ(block_cnt, micro_iter.get_block_count());

  ObInvRowLogicalBlockIterator row_iter;
  int64_t global_idx = 0;
  uint64_t prev_doc_id = 0;
  for (int64_t b = 0; b < block_cnt; ++b) {
    ObPostingBlockDesc posting_block;
    ASSERT_EQ(OB_SUCCESS, micro_iter.get_next_block(posting_block));
    // micro block 迭代器输出的字节正好是 logical block 迭代器
    // 期望解码的字节
    ASSERT_EQ(block_sizes[b], posting_block.size_);
    EXPECT_EQ(0, MEMCMP(posting_block.buf_, block_bufs[b], block_sizes[b]));

    // 每个 block 的 range meta（按需解码）与首/尾行 doc_id 一致
    uint64_t min_id = 0, max_id = 0;
    int64_t doc_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, micro_iter.get_curr_logical_block_range(min_id, max_id, doc_cnt));
    EXPECT_EQ(doc_ids[b][0], min_id);
    EXPECT_EQ(doc_ids[b][row_cnts[b] - 1], max_id);
    EXPECT_EQ(row_cnts[b], doc_cnt);

    // 将 logical block 送入行迭代器并读取每一行
    ASSERT_EQ(OB_SUCCESS, row_iter.open(posting_block, read_info, rd, allocator_));
    EXPECT_EQ(row_cnts[b], row_iter.get_row_count());
    for (int64_t r = 0; r < row_cnts[b]; ++r) {
      const ObDatumRow *row = nullptr;
      ASSERT_EQ(OB_SUCCESS, row_iter.get_next_row(row)) << "b=" << b << " r=" << r;
      ASSERT_NE(nullptr, row);
      const uint64_t doc_id =
          row->storage_datums_[0].get_uint();
      EXPECT_EQ(doc_ids[b][r], doc_id) << "b=" << b << " r=" << r;
      // 整个 micro block 的全局单调性
      if (global_idx > 0) {
        EXPECT_LT(prev_doc_id, doc_id);
      }
      prev_doc_id = doc_id;
      ++global_idx;
    }
    const ObDatumRow *row = nullptr;
    EXPECT_EQ(OB_ITER_END, row_iter.get_next_row(row));
  }
  // micro block 迭代器已耗尽
  ObPostingBlockDesc posting_block;
  EXPECT_EQ(OB_ITER_END, micro_iter.get_next_block(posting_block));
  // 每个 logical block 的每一行都已读取
  EXPECT_EQ(total_rows, global_idx);
}

} // namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_logical_block_iter.log*");
  OB_LOGGER.set_file_name("test_inv_idx_logical_block_iter.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
