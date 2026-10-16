/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#define private public
#define protected public

#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/store/ob_lob_store_common.h"

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

class TestLobPiece : public ::testing::Test
{
public:
  TestLobPiece() = default;
  virtual ~TestLobPiece() = default;
};

// verify ObLobPieceUtil transform roundtrip: info -> row -> info
TEST_F(TestLobPiece, transform_roundtrip_basic)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = false;
  const int expect_col_cnt = ObLobPieceUtil::LOB_PIECE_COLUMN_CNT;

  ObLobPieceInfo src;
  src.lob_col_idx_ = 2;
  src.lob_id_.assign(src.lob_col_idx_, 10, 20);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + 2 * sizeof(uint32_t)] = {0};
  int32_t seq_vals[2] = {3, 7};
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), seq_vals, sizeof(seq_vals) / sizeof(seq_vals[0])));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  // 256K lob data
  const int data_len = 256 * 1024;
  char *data_buf = reinterpret_cast<char*>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, data_buf);
  for (int i = 0; i < data_len; i++) {
    data_buf[i] = static_cast<char>(i % 256);
  }
  src.lob_data_.assign_ptr(data_buf, data_len);

  // info -> row
  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  // row -> info
  ObLobPieceInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  // verify
  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(src.lob_id_, dst.lob_id_);
  ASSERT_EQ(src.seq_id_.length(), dst.seq_id_.length());
  ASSERT_EQ(0, MEMCMP(src.seq_id_.ptr(), dst.seq_id_.ptr(), src.seq_id_.length()));
  ASSERT_EQ(data_len, dst.lob_data_.length());
  ASSERT_EQ(0, MEMCMP(data_buf, dst.lob_data_.ptr(), data_len));
}

// verify roundtrip with extra rowkey (MVCC trans_version + sql_no)
TEST_F(TestLobPiece, transform_roundtrip_with_extra_rowkey)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = true;
  const int expect_col_cnt = ObLobPieceUtil::LOB_PIECE_COLUMN_CNT + ObLobPieceUtil::SKIP_INVALID_COLUMN;

  ObLobPieceInfo src;
  src.lob_col_idx_ = 0;
  src.lob_id_.assign(src.lob_col_idx_, 5, 15);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 99;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  const int data_len = 4096;
  char *data_buf = reinterpret_cast<char*>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'X', data_len);
  src.lob_data_.assign_ptr(data_buf, data_len);

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  // MVCC dummy columns between rowkey and normal (trans_version + sql_no)
  row.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID + 1].set_int(-50);
  row.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID + 2].set_int(-2);

  ObLobPieceInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(src.lob_id_, dst.lob_id_);
  ASSERT_EQ(data_len, dst.lob_data_.length());
}

// verify build_rowkey_from_info produces correct 3-column rowkey
TEST_F(TestLobPiece, build_rowkey)
{
  const int expect_col_cnt = ObLobPieceUtil::LOB_PIECE_SCHEMA_ROWKEY_COL_CNT;

  ObLobPieceInfo info;
  info.lob_col_idx_ = 3;
  info.lob_id_.assign(info.lob_col_idx_, 33, 44);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + 2 * sizeof(uint32_t)] = {0};
  int32_t seq_vals[2] = {1, 2};
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), seq_vals, sizeof(seq_vals) / sizeof(seq_vals[0])));
  info.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::build_rowkey_from_info(info, &row));

  // col 0: lob_col_idx
  ASSERT_EQ(info.lob_col_idx_, row.storage_datums_[ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID].get_int());

  // col 1: lob_id
  ObString lob_id_str = row.storage_datums_[ObLobPieceUtil::PIECE_LOB_ID_COL_ID].get_string();
  ObLobId parsed;
  ASSERT_EQ(OB_SUCCESS, parsed.from_string(lob_id_str));
  ASSERT_EQ(info.lob_id_, parsed);

  // col 2: seq_id
  ObString seq_str = row.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID].get_string();
  ASSERT_EQ(info.seq_id_.length(), seq_str.length());
  ASSERT_EQ(0, MEMCMP(info.seq_id_.ptr(), seq_str.ptr(), seq_str.length()));
}

// verify empty lob_data roundtrip
TEST_F(TestLobPiece, null_lob_data)
{
  ObArenaAllocator allocator;
  const bool with_extra_rowkey = false;
  const int expect_col_cnt = ObLobPieceUtil::LOB_PIECE_COLUMN_CNT;

  ObLobPieceInfo src;
  src.lob_col_idx_ = 1;
  src.lob_id_.assign(src.lob_col_idx_, 7, 8);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 0;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  src.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));
  src.lob_data_.reset();  // null data

  blocksstable::ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(expect_col_cnt));
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_info_to_row(src, &row, with_extra_rowkey));

  ObLobPieceInfo dst;
  ASSERT_EQ(OB_SUCCESS, ObLobPieceUtil::transform_from_row_to_info(&row, dst, with_extra_rowkey));

  ASSERT_EQ(src.lob_col_idx_, dst.lob_col_idx_);
  ASSERT_EQ(src.lob_id_, dst.lob_id_);
  ASSERT_TRUE(dst.lob_data_.empty());
}

// verify invalid column count returns error
TEST_F(TestLobPiece, invalid_column_count)
{
  ObArenaAllocator allocator;
  ObLobPieceInfo info;
  info.lob_col_idx_ = 0;
  info.lob_id_.assign(info.lob_col_idx_, 1, 1);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 0;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  info.seq_id_.assign_ptr(seq_buf, sizeof(seq_buf));

  // too few columns
  blocksstable::ObDatumRow short_row;
  ASSERT_EQ(OB_SUCCESS, short_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT - 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObLobPieceUtil::transform_from_info_to_row(info, &short_row, false));

  // too many columns
  blocksstable::ObDatumRow long_row;
  ASSERT_EQ(OB_SUCCESS, long_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT + 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObLobPieceUtil::transform_from_info_to_row(info, &long_row, false));
}

// verify construct helper
TEST_F(TestLobPiece, construct_helper)
{
  ObLobPieceInfo info;
  const int64_t lob_col_idx = 4;
  ObLobId lob_id;
  lob_id.assign(lob_col_idx, 55, 66);

  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  int32_t seq_val = 7;
  ASSERT_EQ(OB_SUCCESS, fill_seq_id(seq_buf, sizeof(seq_buf), &seq_val, 1));
  ObString seq_id(sizeof(seq_buf), seq_buf);

  char data_buf[128];
  MEMSET(data_buf, 'Y', sizeof(data_buf));
  ObString lob_data(sizeof(data_buf), data_buf);

  ASSERT_EQ(OB_SUCCESS,
            ObLobPieceUtil::construct(lob_col_idx, lob_id, seq_id, lob_data, info));

  ASSERT_EQ(lob_col_idx, info.lob_col_idx_);
  ASSERT_EQ(lob_id, info.lob_id_);
  ASSERT_EQ(sizeof(seq_buf), info.seq_id_.length());
  ASSERT_EQ(sizeof(data_buf), info.lob_data_.length());
  ASSERT_EQ(0, MEMCMP(data_buf, info.lob_data_.ptr(), sizeof(data_buf)));
}

} // storage
} // oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
