/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "storage/lob/store/ob_lob_store_common.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#define private public
#define protected public

#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "lib/number/ob_number_v2.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{

static int fill_seq_id(char *buf, const int64_t buf_len, const int32_t *vals, const int64_t val_cnt)
{
  int ret = OB_SUCCESS;
  if (buf_len < ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + val_cnt * static_cast<int64_t>(sizeof(uint32_t))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    buf[0] = static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION);
    char *pos = buf + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN;
    for (int64_t i = 0; i < val_cnt; ++i) {
      pos = ObLobSeqId::store32be(pos, static_cast<uint32_t>(vals[i]));
    }
  }
  return ret;
}

class TestLobMetaIterator : public ::testing::Test
{
public:
  TestLobMetaIterator() = default;
  virtual ~TestLobMetaIterator() = default;
};

// verify ObLobMetaUtil transform roundtrip: info -> row -> info
TEST_F(TestLobMetaIterator, transform_roundtrip_basic)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = false;
  const int expect_col_cnt = ObLobMetaUtil::LOB_META_COLUMN_CNT;

  // build source ObLobMetaInfo with all non-null fields
  ObLobMetaInfo src;
  src.lob_col_idx_ = 3;
  src.lob_id_.assign(src.lob_col_idx_, 100, 200);
  src.byte_len_ = 1024;
  src.char_len_ = 512;

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + 2 * sizeof(uint32_t)] = {0};
  int32_t seq_vals[2] = {1, 5};
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), seq_vals, sizeof(seq_vals) / sizeof(seq_vals[0])));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  ObLobMetaExtraInfo lob_extra_info;
  lob_extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  lob_extra_info.lob_chunk_type_.set_in_table();
  ASSERT_EQ(OB_SUCCESS, lob_extra_info.serialize(allocator, src.lob_extra_info_));

  char block_id_buf[8] = "blk_001";
  src.lob_block_id_.assign_ptr(block_id_buf, static_cast<int32_t>(STRLEN(block_id_buf)));

  // info -> row
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  // row -> info
  ObLobMetaInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  // verify
  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(true, src.lob_id_ == dst.lob_id_);
  ASSERT_EQ(src.seq_id_, dst.seq_id_);
  ASSERT_EQ(src.byte_len_, dst.byte_len_);
  ASSERT_EQ(src.char_len_, dst.char_len_);
  ASSERT_EQ(src.lob_extra_info_, dst.lob_extra_info_);
  ASSERT_EQ(src.lob_block_id_, dst.lob_block_id_);
}

// verify roundtrip with extra rowkey (MVCC trans_version + sql_no between rowkey and normal)
TEST_F(TestLobMetaIterator, transform_roundtrip_with_extra_rowkey)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = true;
  const int expect_col_cnt = ObLobMetaUtil::LOB_META_COLUMN_CNT + ObLobMetaUtil::SKIP_INVALID_COLUMN;

  ObLobMetaInfo src;
  src.lob_col_idx_ = 1;
  src.lob_id_.assign(1/*column_idx*/, 42/*tablet_id*/, 99/*inc_id*/);
  src.byte_len_ = 65536;
  src.char_len_ = 32768;

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + 3 * sizeof(uint32_t)] = {0};
  int32_t seq_vals[3] = {2, 3, 1};
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), seq_vals, sizeof(seq_vals) / sizeof(seq_vals[0])));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  ObLobMetaExtraInfo lob_extra_info;
  lob_extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  lob_extra_info.lob_chunk_type_.set_in_table();
  ASSERT_EQ(OB_SUCCESS, lob_extra_info.serialize(allocator, src.lob_extra_info_));

  // lob_block_id null (phase1 default)
  src.lob_block_id_.reset();

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  // set MVCC dummy values between rowkey and normal columns
  row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 1].set_int(-100);
  row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-1);

  ObLobMetaInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(src.lob_id_, dst.lob_id_);
  ASSERT_EQ(src.byte_len_, dst.byte_len_);
  ASSERT_EQ(src.char_len_, dst.char_len_);
  ASSERT_EQ(src.lob_extra_info_, dst.lob_extra_info_);
  ASSERT_TRUE(dst.lob_block_id_.empty());
}

TEST_F(TestLobMetaIterator, transform_roundtrip_large_lengths)
{
  const uint64_t byte_len = (4ULL << 40) - 1;
  const uint64_t char_len = (1ULL << 40) + 17;
  for (int64_t with_mvcc = 0; with_mvcc < 2; ++with_mvcc) {
    const bool with_extra_rowkey = 0 != with_mvcc;
    const int64_t column_count = ObLobMetaUtil::LOB_META_COLUMN_CNT
        + (with_extra_rowkey ? ObLobMetaUtil::SKIP_INVALID_COLUMN : 0);
    ObLobMetaInfo src;
    src.lob_col_idx_ = 2;
    src.lob_id_.assign(2, 100, 300);
    src.byte_len_ = byte_len;
    src.char_len_ = char_len;
    char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
    ObString seq_id(sizeof(seq_buf), seq_buf);
    ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(1, seq_id));
    src.seq_id_ = seq_id;

    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(column_count));
    ASSERT_EQ(OB_SUCCESS,
              ObLobMetaUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));
    if (with_extra_rowkey) {
      row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 1].set_int(-100);
      row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-1);
    }

    ObLobMetaInfo dst;
    ASSERT_EQ(OB_SUCCESS,
              ObLobMetaUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));
    ASSERT_EQ(byte_len, dst.byte_len_);
    ASSERT_EQ(char_len, dst.char_len_);
    ASSERT_EQ(src.lob_id_, dst.lob_id_);
    ASSERT_EQ(src.seq_id_, dst.seq_id_);
  }
}

// verify build_rowkey_from_info produces correct 3-column rowkey
TEST_F(TestLobMetaIterator, build_rowkey)
{
  const int expect_col_cnt = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;

  ObLobMetaInfo info;
  info.lob_col_idx_ = 5;
  info.lob_id_.assign(5, 77, 88);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 42;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  info.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::build_rowkey_from_info(info, &row));

  // col 0: lob_col_idx
  ASSERT_EQ(info.lob_col_idx_, row.storage_datums_[ObLobMetaUtil::LOB_COL_IDX_COL_ID].get_int());

  // col 1: lob_id
  ObString lob_id_str = row.storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID].get_string();
  ObLobId parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_string(lob_id_str));
  ASSERT_EQ(info.lob_id_, parsed);

  // col 2: seq_id
  ObString seq_str = row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID].get_string();
  ASSERT_EQ(info.seq_id_, seq_str);
}

// verify roundtrip with all optional fields null
TEST_F(TestLobMetaIterator, null_optional_fields)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = false;
  const int expect_col_cnt = ObLobMetaUtil::LOB_META_COLUMN_CNT;

  ObLobMetaInfo src;
  src.lob_col_idx_ = 0;
  src.lob_id_.assign(0, 1, 1);
  src.byte_len_ = 0;
  src.char_len_ = 0;

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 1;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  // both optional fields null
  src.lob_extra_info_.reset();
  src.lob_block_id_.reset();

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  ObLobMetaInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(src.lob_id_, dst.lob_id_);
  ASSERT_EQ(src.byte_len_, dst.byte_len_);
  ASSERT_EQ(src.char_len_, dst.char_len_);
  ASSERT_TRUE(dst.lob_extra_info_.empty());
  ASSERT_TRUE(dst.lob_block_id_.empty());
}

// verify column count mismatch produces error
TEST_F(TestLobMetaIterator, invalid_column_count)
{
  ObArenaAllocator allocator;
  ObLobMetaInfo info;
  info.lob_col_idx_ = 0;
  info.lob_id_.assign(0, 1, 1);
  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 0;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  info.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  // wrong column count: too few
  ObDatumRow short_row;
  ASSERT_EQ(OB_SUCCESS, short_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT - 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObLobMetaUtil::transform_from_info_to_row(info, &short_row, false));

  // wrong column count: too many
  ObDatumRow long_row;
  ASSERT_EQ(OB_SUCCESS, long_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT + 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObLobMetaUtil::transform_from_info_to_row(info, &long_row, false));
}

} // storage
} // oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
