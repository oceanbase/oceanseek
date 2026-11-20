/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace share::schema;

static const int64_t OK = OB_SUCCESS;
static const int64_t TEST_BM25_MAX_PAIR_COUNT = 4;

// =============================================================================
// Coverage layering (weakest -> strongest among reachable black-box paths):
//   L1  statement/line + function  — every public API exercised on happy path
//   L2  branch                     — layout variant, empty blob, NOT_SUPPORTED
//   L3  condition / decision-cond  — each sub-predicate in compound conditions
//   L4  MC/DC                      — each condition independently flips outcome
//   L5  path                        — multi-step flows (reuse, posting alignment)
//   L6  data-flow                    — row_idx / col_idx width, zero-copy pointer
//   L7  extreme values               — large blobs, max-capacity stress
// =============================================================================

static char *copy_block(ObIAllocator &allocator, const char *src, const int64_t size)
{
  char *buf = static_cast<char *>(allocator.alloc(size));
  if (nullptr != buf) {
    MEMCPY(buf, src, size);
  }
  return buf;
}

class TestPostingValueBlockCodec : public ::testing::Test
{
public:
  TestPostingValueBlockCodec() : allocator_(ObModIds::TEST), col_descs_(), param_() {}
  virtual ~TestPostingValueBlockCodec() {}

protected:
  void init_param(const bool with_nested_key)
  {
    col_descs_.reset();
    ObColDesc col_desc;
    col_desc.col_type_.set_varchar();
    ASSERT_EQ(OK, col_descs_.push_back(col_desc));
    if (with_nested_key) {
      ASSERT_EQ(OK, col_descs_.push_back(col_desc));
    }
    param_.col_descs_ = &col_descs_;
    param_.block_capacity_ = 256;
    param_.position_list_col_idx_ = 0;
    param_.nested_key_col_idx_ = with_nested_key ? 1 : -1;
  }

  void init_param_nested_key_only()
  {
    col_descs_.reset();
    ObColDesc col_desc;
    col_desc.col_type_.set_varchar();
    ASSERT_EQ(OK, col_descs_.push_back(col_desc));
    param_.col_descs_ = &col_descs_;
    param_.block_capacity_ = 256;
    param_.position_list_col_idx_ = -1;
    param_.nested_key_col_idx_ = 0;
  }

  int make_value_row(
      ObDatumRow &row,
      const int64_t col_cnt,
      const char *pos_data,
      const int64_t pos_len,
      const char *nested_data,
      const int64_t nested_len)
  {
    int ret = OB_SUCCESS;
    if (!row.is_valid()) {
      if (OB_FAIL(row.init(col_cnt))) {
      }
    } else if (row.get_column_count() != col_cnt) {
      row.reset();
      if (OB_FAIL(row.init(col_cnt))) {
      }
    } else {
      row.count_ = static_cast<uint16_t>(col_cnt);
    }
    if (OB_SUCC(ret)) {
      if (param_.has_position_list()) {
        row.storage_datums_[param_.position_list_col_idx_].ptr_ = pos_data;
        row.storage_datums_[param_.position_list_col_idx_].len_ = static_cast<uint32_t>(pos_len);
        row.storage_datums_[param_.position_list_col_idx_].pack_
            = row.storage_datums_[param_.position_list_col_idx_].len_;
      }
      if (param_.has_nested_key()) {
        row.storage_datums_[param_.nested_key_col_idx_].ptr_ = nested_data;
        row.storage_datums_[param_.nested_key_col_idx_].len_ = static_cast<uint32_t>(nested_len);
        row.storage_datums_[param_.nested_key_col_idx_].pack_
            = row.storage_datums_[param_.nested_key_col_idx_].len_;
      }
    }
    return ret;
  }

  void assert_blob_equal(const ObDatum &value, const char *expected, const int64_t expected_len)
  {
    ASSERT_EQ(expected_len, value.len_);
    if (expected_len > 0) {
      ASSERT_EQ(0, MEMCMP(expected, value.ptr_, expected_len));
    }
  }

  void verify_value_block_header(
      const ObPostingValueBlockDesc &desc,
      const bool with_position_list,
      const bool with_nested_key,
      const int64_t row_cnt)
  {
    const ObPostingValueBlockHeader *header =
        reinterpret_cast<const ObPostingValueBlockHeader *>(desc.buf_);
    ASSERT_NE(nullptr, header);
    ASSERT_TRUE(header->is_valid());
    ASSERT_EQ(ObPostingValueBlockHeader::VALUE_BLOCK_HEADER_VERSION, header->version_);
    ASSERT_EQ(with_position_list ? 1 : 0, header->has_position_list_);
    ASSERT_EQ(with_nested_key ? 1 : 0, header->has_nested_key_);
    ASSERT_EQ((with_position_list ? 1 : 0) + (with_nested_key ? 1 : 0), header->column_cnt_);
    ASSERT_EQ(row_cnt, header->row_count_);
    ASSERT_EQ(row_cnt, desc.row_count_);
    ASSERT_GE(header->row_idx_offset_, sizeof(ObPostingValueBlockHeader));
    ASSERT_LE(header->row_idx_offset_ + row_cnt * header->row_idx_byte_width_, desc.size_);
    ASSERT_EQ(desc.size_, header->row_idx_offset_ + row_cnt * header->row_idx_byte_width_);
  }

protected:
  ObArenaAllocator allocator_;
  ObArray<ObColDesc> col_descs_;
  ObPostingValueBlockWriteParam param_;
  ObPostingValueBlockWriter writer_;
  ObPostingValueBlockReader reader_;
};

// =============================================================================
// L1. Statement / line / function coverage — write-read happy paths
// =============================================================================

TEST_F(TestPostingValueBlockCodec, position_list_round_trip)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  const char pos1[] = {9, 8};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, pos1, sizeof(pos1), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_TRUE(desc.is_valid());
  ASSERT_EQ(2, desc.row_count_);
  verify_value_block_header(desc, true, false, 2);

  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));
  for (int64_t i = 0; i < 2; ++i) {
    ObDatum value;
    ASSERT_EQ(OK, reader_.get_position_list(i, value));
    const char *expected = (0 == i) ? pos0 : pos1;
    const int64_t expected_len = (0 == i) ? sizeof(pos0) : sizeof(pos1);
    assert_blob_equal(value, expected, expected_len);
  }
  ObDatum nested_key;
  ASSERT_EQ(OB_NOT_SUPPORTED, reader_.get_nested_key(0, nested_key));
}

TEST_F(TestPostingValueBlockCodec, with_nested_key_round_trip)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2};
  const char nested0[] = {7, 7, 7};
  const char pos1[] = {3};
  const char nested1[] = {8};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 2, pos1, sizeof(pos1), nested1, sizeof(nested1)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, true, true, 2);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum pos;
  ObDatum nested;
  ASSERT_EQ(OK, reader_.get_position_list(0, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  assert_blob_equal(pos, pos0, sizeof(pos0));
  assert_blob_equal(nested, nested0, sizeof(nested0));

  ASSERT_EQ(OK, reader_.get_position_list(1, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(1, nested));
  assert_blob_equal(pos, pos1, sizeof(pos1));
  assert_blob_equal(nested, nested1, sizeof(nested1));
}

// Verify encode/decode round trip preserves binary payload byte-for-byte.
// Source row buffers are mutated after append_row to prove append_row already
// deep-copied data into block buffer via MEMCPY.
TEST_F(TestPostingValueBlockCodec, encode_decode_binary_memcmp)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  char pos0[] = {0x01, 0x02, 0x03};
  char nested0[] = {static_cast<char>(0xA0), static_cast<char>(0xB1)};
  char pos1[] = {static_cast<char>(0xFF)};
  char nested1[] = {0x10, 0x20, 0x30, 0x40};
  const char pos0_expected[] = {0x01, 0x02, 0x03};
  const char nested0_expected[] = {static_cast<char>(0xA0), static_cast<char>(0xB1)};
  const char pos1_expected[] = {static_cast<char>(0xFF)};
  const char nested1_expected[] = {0x10, 0x20, 0x30, 0x40};

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 2, pos1, sizeof(pos1), nested1, sizeof(nested1)));
  ASSERT_EQ(OK, writer_.append_row(row));

  // Mutate source buffers after append; decoded data must still match pre-mutation bytes.
  pos0[0] = 0xEE;
  nested0[0] = 0xEE;
  pos1[0] = 0xEE;
  nested1[0] = 0xEE;

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum pos;
  ObDatum nested;
  ASSERT_EQ(OK, reader_.get_position_list(0, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  ASSERT_EQ(0, MEMCMP(pos0_expected, pos.ptr_, sizeof(pos0_expected)));
  ASSERT_EQ(0, MEMCMP(nested0_expected, nested.ptr_, sizeof(nested0_expected)));

  ASSERT_EQ(OK, reader_.get_position_list(1, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(1, nested));
  ASSERT_EQ(0, MEMCMP(pos1_expected, pos.ptr_, sizeof(pos1_expected)));
  ASSERT_EQ(0, MEMCMP(nested1_expected, nested.ptr_, sizeof(nested1_expected)));
}

TEST_F(TestPostingValueBlockCodec, nested_key_only_round_trip)
{
  init_param_nested_key_only();
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char nested0[] = {1, 2, 3, 4};
  const char nested1[] = {5};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nested1, sizeof(nested1)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, false, true, 2);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum nested;
  ASSERT_EQ(OB_NOT_SUPPORTED, reader_.get_position_list(0, nested));
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  assert_blob_equal(nested, nested0, sizeof(nested0));
  ASSERT_EQ(OK, reader_.get_nested_key(1, nested));
  assert_blob_equal(nested, nested1, sizeof(nested1));
}

TEST_F(TestPostingValueBlockCodec, single_row_block)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {7, 8, 9};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, true, false, 1);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos0, sizeof(pos0));
}

TEST_F(TestPostingValueBlockCodec, empty_blob_round_trip)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  ASSERT_EQ(0, value.len_);
}

TEST_F(TestPostingValueBlockCodec, empty_position_list_and_null_nested_key)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, nullptr, 0, nullptr, 0));
  row.storage_datums_[param_.nested_key_col_idx_].set_null();
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum pos;
  ObDatum nested;
  ASSERT_EQ(OK, reader_.get_position_list(0, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  ASSERT_EQ(0, pos.len_);
  ASSERT_FALSE(pos.is_null());
  ASSERT_TRUE(nested.is_null());
}

TEST_F(TestPostingValueBlockCodec, multi_row_random_access)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {10};
  const char pos1[] = {20, 21};
  const char pos2[] = {30, 31, 32};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, pos1, sizeof(pos1), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, pos2, sizeof(pos2), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(2, value));
  assert_blob_equal(value, pos2, sizeof(pos2));
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos0, sizeof(pos0));
  ASSERT_EQ(OK, reader_.get_position_list(1, value));
  assert_blob_equal(value, pos1, sizeof(pos1));
}

// =============================================================================
// L2. Branch coverage — layout variant & NOT_SUPPORTED dispatch
// =============================================================================

TEST_F(TestPostingValueBlockCodec, get_position_list_not_supported)
{
  init_param_nested_key_only();
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char nested0[] = {1, 2};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OB_NOT_SUPPORTED, reader_.get_position_list(0, value));
}

TEST_F(TestPostingValueBlockCodec, get_nested_key_not_supported)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum nested;
  ASSERT_EQ(OB_NOT_SUPPORTED, reader_.get_nested_key(0, nested));
}

// =============================================================================
// L3/L4. Condition & MC/DC coverage — capacity-full predicates
// =============================================================================

TEST_F(TestPostingValueBlockCodec, writer_is_full)
{
  init_param(false);
  param_.block_capacity_ = 1;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, block_capacity_full)
{
  init_param(false);
  param_.block_capacity_ = 1;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, writer_.append_row(row));
}

// =============================================================================
// L5. Path coverage — multi-step flows & posting/value alignment
// =============================================================================

TEST_F(TestPostingValueBlockCodec, writer_reuse)
{
  init_param(false);

  const char pos_a[] = {1, 2};
  ObDatumRow row;
  ObPostingValueBlockDesc desc1;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  ASSERT_EQ(OK, make_value_row(row, 1, pos_a, sizeof(pos_a), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, writer_.build_block(desc1));

  writer_.reuse();

  const char pos_b[] = {9};
  const char pos_c[] = {8, 7, 6};
  ObPostingValueBlockDesc desc2;
  ASSERT_EQ(OK, make_value_row(row, 1, pos_b, sizeof(pos_b), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, pos_c, sizeof(pos_c), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, writer_.build_block(desc2));
  verify_value_block_header(desc2, true, false, 2);

  ASSERT_EQ(OK, reader_.init(desc2.buf_, desc2.size_));
  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos_b, sizeof(pos_b));
  ASSERT_EQ(OK, reader_.get_position_list(1, value));
  assert_blob_equal(value, pos_c, sizeof(pos_c));
}

TEST_F(TestPostingValueBlockCodec, reader_reuse)
{
  init_param(false);

  const char pos_a[] = {1, 2, 3};
  const char pos_b[] = {9};
  ObDatumRow row;
  ObPostingValueBlockDesc desc1;
  ObPostingValueBlockDesc desc2;

  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  ASSERT_EQ(OK, make_value_row(row, 1, pos_a, sizeof(pos_a), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, writer_.build_block(desc1));
  char *buf1 = copy_block(allocator_, desc1.buf_, desc1.size_);
  const int64_t size1 = desc1.size_;
  ASSERT_NE(nullptr, buf1);

  writer_.reuse();
  ASSERT_EQ(OK, make_value_row(row, 1, pos_b, sizeof(pos_b), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, writer_.build_block(desc2));

  ASSERT_EQ(OK, reader_.init(buf1, size1));
  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos_a, sizeof(pos_a));

  reader_.reuse();
  ASSERT_EQ(OK, reader_.init(desc2.buf_, desc2.size_));
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos_b, sizeof(pos_b));
}

TEST_F(TestPostingValueBlockCodec, writer_reset_lifecycle)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  writer_.reset();

  const char pos1[] = {9};
  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  ASSERT_EQ(OK, make_value_row(row, 1, pos1, sizeof(pos1), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, true, false, 1);

  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));
  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, pos1, sizeof(pos1));
}

TEST_F(TestPostingValueBlockCodec, posting_value_row_alignment)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {1, 2, 3};
  const uint64_t doc_lengths[] = {100, 200, 300};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);

  ObArray<ObColDesc> posting_col_descs;
  ObPostingBlockEncodingCtx posting_ctx;
  posting_ctx.reset();
  for (int64_t i = 0; i < 3; ++i) {
    ObColDesc col_desc;
    col_desc.col_type_.set_uint64();
    ASSERT_EQ(OK, posting_col_descs.push_back(col_desc));
  }
  posting_ctx.col_descs_ = &posting_col_descs;
  posting_ctx.block_capacity_ = 256;
  posting_ctx.doc_id_col_idx_ = 0;
  posting_ctx.token_freq_col_idx_ = 1;
  posting_ctx.doc_length_col_idx_ = 2;

  ObPostingBlockEncoder posting_encoder;
  ObPostingBlockDesc posting_desc;
  ObDatumRow posting_row;
  ASSERT_EQ(OK, posting_encoder.init(posting_ctx, allocator_, TEST_BM25_MAX_PAIR_COUNT));
  for (int64_t i = 0; i < row_cnt; ++i) {
    int ret = OB_SUCCESS;
    if (!posting_row.is_valid()) {
      ret = posting_row.init(3);
    } else {
      posting_row.count_ = 3;
    }
    if (OB_SUCC(ret)) {
      posting_row.storage_datums_[0].set_uint(doc_ids[i]);
      posting_row.storage_datums_[1].set_uint(token_freqs[i]);
      posting_row.storage_datums_[2].set_uint(doc_lengths[i]);
      ret = posting_encoder.append_row(posting_row);
    }
    ASSERT_EQ(OK, ret);
  }
  ASSERT_EQ(OK, posting_encoder.build_block(posting_desc));

  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  const char pos0[] = {1, 2};
  const char nested0[] = {7};
  const char pos1[] = {3};
  const char nested1[] = {8, 9};
  const char pos2[] = {4, 5, 6};
  const char nested2[] = {10};
  ObDatumRow value_row;
  ASSERT_EQ(OK, make_value_row(value_row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(value_row));
  ASSERT_EQ(OK, make_value_row(value_row, 2, pos1, sizeof(pos1), nested1, sizeof(nested1)));
  ASSERT_EQ(OK, writer_.append_row(value_row));
  ASSERT_EQ(OK, make_value_row(value_row, 2, pos2, sizeof(pos2), nested2, sizeof(nested2)));
  ASSERT_EQ(OK, writer_.append_row(value_row));

  ObPostingValueBlockDesc value_desc;
  ASSERT_EQ(OK, writer_.build_block(value_desc));
  ASSERT_EQ(posting_desc.doc_count_, value_desc.row_count_);

  ObPostingBlockDecoder posting_decoder;
  ASSERT_EQ(OK, posting_decoder.init(posting_desc.buf_, posting_desc.size_, allocator_));
  ASSERT_EQ(OK, reader_.init(value_desc.buf_, value_desc.size_));

  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ObDatum pos;
    ObDatum nested;
    const char *expected_pos = (0 == i) ? pos0 : ((1 == i) ? pos1 : pos2);
    const int64_t expected_pos_len = (0 == i) ? sizeof(pos0) : ((1 == i) ? sizeof(pos1) : sizeof(pos2));
    const char *expected_nested = (0 == i) ? nested0 : ((1 == i) ? nested1 : nested2);
    const int64_t expected_nested_len = (0 == i) ? sizeof(nested0) : ((1 == i) ? sizeof(nested1) : sizeof(nested2));
    ASSERT_EQ(OK, posting_decoder.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, posting_decoder.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(OK, reader_.get_position_list(i, pos));
    ASSERT_EQ(OK, reader_.get_nested_key(i, nested));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
    assert_blob_equal(pos, expected_pos, expected_pos_len);
    assert_blob_equal(nested, expected_nested, expected_nested_len);
  }
}

// =============================================================================
// L6. Data-flow coverage — row_idx / col_idx width & zero-copy pointer
// =============================================================================

TEST_F(TestPostingValueBlockCodec, row_index_byte_width)
{
  init_param(false);
  param_.block_capacity_ = 512;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  char row_bufs[300];
  const int64_t large_row_cnt = 300;
  for (int64_t i = 0; i < large_row_cnt; ++i) {
    row_bufs[i] = static_cast<char>(i & 0xFF);
    ASSERT_EQ(OK, make_value_row(row, 1, &row_bufs[i], 1, nullptr, 0));
    ASSERT_EQ(OK, writer_.append_row(row));
  }

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  const ObPostingValueBlockHeader *header =
      reinterpret_cast<const ObPostingValueBlockHeader *>(desc.buf_);
  ASSERT_GE(header->row_idx_byte_width_, 2);
  verify_value_block_header(desc, true, false, large_row_cnt);
}

TEST_F(TestPostingValueBlockCodec, estimated_size_matches_built_block)
{
  init_param_nested_key_only();
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  const char small_value[] = {1};
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, small_value, sizeof(small_value)));
  int64_t row_size = 0;
  int64_t estimated_size = 0;
  ASSERT_EQ(OK, writer_.estimate_row_size(row, row_size));
  ASSERT_EQ(OK, writer_.estimate_size_after_append(row_size, estimated_size));
  ASSERT_EQ(OK, writer_.append_row(row));
  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  EXPECT_EQ(estimated_size, desc.size_);

  writer_.reuse();
  char large_value[12000];
  MEMSET(large_value, 1, sizeof(large_value));
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, large_value, sizeof(large_value)));
    ASSERT_EQ(OK, writer_.estimate_row_size(row, row_size));
    ASSERT_EQ(OK, writer_.estimate_size_after_append(row_size, estimated_size));
    ASSERT_EQ(OK, writer_.append_row(row));
  }
  ASSERT_EQ(OK, writer_.build_block(desc));
  EXPECT_EQ(estimated_size, desc.size_);
  const ObPostingValueBlockHeader *header =
      reinterpret_cast<const ObPostingValueBlockHeader *>(desc.buf_);
  EXPECT_EQ(4, header->row_idx_byte_width_);
}

TEST_F(TestPostingValueBlockCodec, col_idx_byte_width_large_row)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  char large_blob[70000];
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(large_blob)); ++i) {
    large_blob[i] = static_cast<char>(i & 0xFF);
  }

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, large_blob, sizeof(large_blob), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  const ObPostingValueRowHeader *row_header = reinterpret_cast<const ObPostingValueRowHeader *>(
      desc.buf_ + sizeof(ObPostingValueBlockHeader));
  ASSERT_GE(row_header->col_idx_byte_width_, 2);

  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));
  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, large_blob, sizeof(large_blob));
}

TEST_F(TestPostingValueBlockCodec, zero_copy_blob_pointer)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2};
  const char nested0[] = {7, 8, 9};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum pos;
  ObDatum nested;
  ASSERT_EQ(OK, reader_.get_position_list(0, pos));
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  ASSERT_GE(pos.ptr_, desc.buf_);
  ASSERT_LE(pos.ptr_, desc.buf_ + desc.size_);
  ASSERT_GE(nested.ptr_, desc.buf_);
  ASSERT_LE(nested.ptr_, desc.buf_ + desc.size_);
  assert_blob_equal(pos, pos0, sizeof(pos0));
  assert_blob_equal(nested, nested0, sizeof(nested0));
}

TEST_F(TestPostingValueBlockCodec, varying_blob_sizes_multi_row)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  char medium_blob[100];
  for (int64_t i = 0; i < 100; ++i) {
    medium_blob[i] = static_cast<char>(i & 0xFF);
  }

  const char small_blob[] = {42};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, small_blob, sizeof(small_blob), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, medium_blob, sizeof(medium_blob), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, small_blob, sizeof(small_blob));
  ASSERT_EQ(OK, reader_.get_position_list(1, value));
  assert_blob_equal(value, medium_blob, sizeof(medium_blob));
  ASSERT_EQ(OK, reader_.get_position_list(2, value));
  ASSERT_EQ(0, value.len_);
}

// =============================================================================
// L7. Extreme value coverage — large blobs & max-capacity stress
// =============================================================================

TEST_F(TestPostingValueBlockCodec, large_blob_round_trip)
{
  init_param(false);
  param_.block_capacity_ = 4;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  char large_blob[200];
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(large_blob)); ++i) {
    large_blob[i] = static_cast<char>(i & 0xFF);
  }

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, large_blob, sizeof(large_blob), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OK, reader_.get_position_list(0, value));
  assert_blob_equal(value, large_blob, sizeof(large_blob));
}

TEST_F(TestPostingValueBlockCodec, max_capacity_block)
{
  init_param(false);
  param_.block_capacity_ = 256;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  char row_bufs[256];
  for (int64_t i = 0; i < 256; ++i) {
    row_bufs[i] = static_cast<char>(i & 0xFF);
    ASSERT_EQ(OK, make_value_row(row, 1, &row_bufs[i], 1, nullptr, 0));
    ASSERT_EQ(OK, writer_.append_row(row));
  }

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, true, false, 256);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  for (int64_t i = 0; i < 256; ++i) {
    ObDatum value;
    ASSERT_EQ(OK, reader_.get_position_list(i, value));
    ASSERT_EQ(1, value.len_);
    ASSERT_EQ(row_bufs[i], value.ptr_[0]);
  }
}

TEST_F(TestPostingValueBlockCodec, max_capacity_block_with_nested_key)
{
  init_param(true);
  param_.block_capacity_ = 256;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  char pos_bufs[256];
  char nested_bufs[256];
  for (int64_t i = 0; i < 256; ++i) {
    pos_bufs[i] = static_cast<char>(i & 0xFF);
    nested_bufs[i] = static_cast<char>((i + 17) & 0xFF);
    ASSERT_EQ(OK, make_value_row(row, 2, &pos_bufs[i], 1, &nested_bufs[i], 1));
    ASSERT_EQ(OK, writer_.append_row(row));
  }

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, true, true, 256);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  for (int64_t i = 0; i < 256; ++i) {
    ObDatum pos;
    ObDatum nested;
    ASSERT_EQ(OK, reader_.get_position_list(i, pos));
    ASSERT_EQ(OK, reader_.get_nested_key(i, nested));
    ASSERT_EQ(1, pos.len_);
    ASSERT_EQ(1, nested.len_);
    ASSERT_EQ(pos_bufs[i], pos.ptr_[0]);
    ASSERT_EQ(nested_bufs[i], nested.ptr_[0]);
  }
}

TEST_F(TestPostingValueBlockCodec, nested_key_only_max_capacity)
{
  init_param_nested_key_only();
  param_.block_capacity_ = 256;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  char nested_bufs[256];
  for (int64_t i = 0; i < 256; ++i) {
    nested_bufs[i] = static_cast<char>(i & 0xFF);
    ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, &nested_bufs[i], 1));
    ASSERT_EQ(OK, writer_.append_row(row));
  }

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  verify_value_block_header(desc, false, true, 256);
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  for (int64_t i = 0; i < 256; ++i) {
    ObDatum nested;
    ASSERT_EQ(OK, reader_.get_nested_key(i, nested));
    ASSERT_EQ(1, nested.len_);
    ASSERT_EQ(nested_bufs[i], nested.ptr_[0]);
  }
}

// =============================================================================
// Negative paths — writer error branches (L2/L3)
// =============================================================================

TEST_F(TestPostingValueBlockCodec, writer_not_init)
{
  ObDatumRow row;
  const char pos0[] = {1};
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OB_NOT_INIT, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OB_NOT_INIT, writer_.build_block(desc));
}

TEST_F(TestPostingValueBlockCodec, init_invalid_param)
{
  ObPostingValueBlockWriteParam invalid_param;
  invalid_param.col_descs_ = nullptr;
  invalid_param.block_capacity_ = 256;
  invalid_param.position_list_col_idx_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.init(invalid_param, allocator_));

  init_param(false);
  param_.block_capacity_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.init(param_, allocator_));

  init_param(false);
  param_.position_list_col_idx_ = -1;
  param_.nested_key_col_idx_ = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.init(param_, allocator_));
}

TEST_F(TestPostingValueBlockCodec, writer_init_twice)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  ASSERT_EQ(OB_INIT_TWICE, writer_.init(param_, allocator_));
}

TEST_F(TestPostingValueBlockCodec, empty_build_block)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.build_block(desc));
}

TEST_F(TestPostingValueBlockCodec, append_invalid_column)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, row.init(1));
  row.count_ = 1;
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_nop_column)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, row.init(1));
  row.count_ = 1;
  row.storage_datums_[0].set_nop();
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_nested_key_null)
{
  init_param_nested_key_only();
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, row.init(1));
  row.count_ = 1;
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));
  ObDatum nested;
  ASSERT_EQ(OK, reader_.get_nested_key(0, nested));
  ASSERT_TRUE(nested.is_null());
}

TEST_F(TestPostingValueBlockCodec, append_dual_column_nested_key_null)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, row.init(2));
  row.count_ = 2;
  row.storage_datums_[0].ptr_ = pos0;
  row.storage_datums_[0].len_ = sizeof(pos0);
  row.storage_datums_[0].pack_ = sizeof(pos0);
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OK, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_nested_key_empty_blob)
{
  init_param_nested_key_only();
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, nullptr, 0, nullptr, 0));
  ASSERT_FALSE(row.storage_datums_[0].is_null());
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_dual_column_position_list_null)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char nested0[] = {7, 8};
  ObDatumRow row;
  ASSERT_EQ(OK, row.init(2));
  row.count_ = 2;
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].ptr_ = nested0;
  row.storage_datums_[1].len_ = sizeof(nested0);
  row.storage_datums_[1].pack_ = sizeof(nested0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_dual_column_nested_key_nop)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {4, 5};
  ObDatumRow row;
  ASSERT_EQ(OK, row.init(2));
  row.count_ = 2;
  row.storage_datums_[0].ptr_ = pos0;
  row.storage_datums_[0].len_ = sizeof(pos0);
  row.storage_datums_[0].pack_ = sizeof(pos0);
  row.storage_datums_[1].set_nop();
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

TEST_F(TestPostingValueBlockCodec, append_dual_column_invalid_col_index)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer_.append_row(row));
}

// =============================================================================
// Negative paths — reader error branches (L2/L3)
// =============================================================================

TEST_F(TestPostingValueBlockCodec, reader_not_init)
{
  ObDatum value;
  ASSERT_EQ(OB_NOT_INIT, reader_.get_position_list(0, value));
  ASSERT_EQ(OB_NOT_INIT, reader_.get_nested_key(0, value));
}

TEST_F(TestPostingValueBlockCodec, decoder_invalid_buffer)
{
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(nullptr, 100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init("", 0));

  char buf[4] = {0};
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, sizeof(buf)));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_buffer)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);

  ObPostingValueBlockHeader *header = reinterpret_cast<ObPostingValueBlockHeader *>(buf);
  header->version_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, desc.size_));
  reader_.reuse();

  header->version_ = ObPostingValueBlockHeader::VALUE_BLOCK_HEADER_VERSION;
  header->row_count_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, desc.size_));
  reader_.reuse();

  header->row_count_ = 1;
  header->row_idx_offset_ = static_cast<uint32_t>(desc.size_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, desc.size_));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_invalid_row_idx_byte_width)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingValueBlockHeader *header = reinterpret_cast<ObPostingValueBlockHeader *>(buf);
  header->row_idx_byte_width_ = 3;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, desc.size_));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_column_cnt_mismatch)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  const char nested0[] = {2};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingValueBlockHeader *header = reinterpret_cast<ObPostingValueBlockHeader *>(buf);
  header->column_cnt_ = 3;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.init(buf, desc.size_));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_col_offset_out_of_row)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3, 4, 5};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingValueRowHeader *row_header = reinterpret_cast<ObPostingValueRowHeader *>(
      buf + sizeof(ObPostingValueBlockHeader));
  char *col_idx_buf = reinterpret_cast<char *>(row_header) + sizeof(ObPostingValueRowHeader);
  ObIntegerArrayGenerator col_idx_gen;
  ASSERT_EQ(OK, col_idx_gen.init(col_idx_buf, row_header->col_idx_byte_width_));
  col_idx_gen.get_array().set(1, 100);
  ASSERT_EQ(OK, reader_.init(buf, desc.size_));

  ObDatum value;
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader_.get_position_list(0, value));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_row_header)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1, 2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingValueRowHeader *row_header = reinterpret_cast<ObPostingValueRowHeader *>(
      buf + sizeof(ObPostingValueBlockHeader));
  row_header->col_idx_byte_width_ = 3;
  ASSERT_EQ(OK, reader_.init(buf, desc.size_));

  ObDatum value;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.get_position_list(0, value));
}

TEST_F(TestPostingValueBlockCodec, reader_corrupted_row_index_offsets)
{
  init_param(false);

  const char pos0[] = {1};
  const char pos1[] = {2, 3};
  ObDatumRow row;
  ASSERT_EQ(OK, writer_.init(param_, allocator_));
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));
  ASSERT_EQ(OK, make_value_row(row, 1, pos1, sizeof(pos1), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingValueBlockHeader *header =
      reinterpret_cast<ObPostingValueBlockHeader *>(buf);
  char *row_index = buf + header->row_idx_offset_;
  row_index[0] = 10;
  row_index[header->row_idx_byte_width_] = 5;
  ASSERT_EQ(OK, reader_.init(buf, desc.size_));

  ObDatum value;
  ASSERT_EQ(OB_ERR_UNEXPECTED, reader_.get_position_list(0, value));
}

TEST_F(TestPostingValueBlockCodec, invalid_row_index)
{
  init_param(false);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 1, pos0, sizeof(pos0), nullptr, 0));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum value;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.get_position_list(-1, value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.get_position_list(1, value));
}

TEST_F(TestPostingValueBlockCodec, get_nested_key_invalid_row_index)
{
  init_param(true);
  ASSERT_EQ(OK, writer_.init(param_, allocator_));

  const char pos0[] = {1};
  const char nested0[] = {2};
  ObDatumRow row;
  ASSERT_EQ(OK, make_value_row(row, 2, pos0, sizeof(pos0), nested0, sizeof(nested0)));
  ASSERT_EQ(OK, writer_.append_row(row));

  ObPostingValueBlockDesc desc;
  ASSERT_EQ(OK, writer_.build_block(desc));
  ASSERT_EQ(OK, reader_.init(desc.buf_, desc.size_));

  ObDatum nested;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.get_nested_key(-1, nested));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader_.get_nested_key(1, nested));
}

} // namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_posting_value_block_codec.log*");
  OB_LOGGER.set_file_name("test_posting_value_block_codec.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
