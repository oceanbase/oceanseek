/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define protected public
#define private public


#include "storage/blocksstable/ob_data_file_prepare.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestLobSeqId  : public ::testing::Test
{
public:
  TestLobSeqId() {}
  virtual ~TestLobSeqId() {};
  
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

static void make_seq_id(ObIAllocator* alloc, uint32_t dig_size, char*& seq_buf, uint32_t*& digits ,uint32_t& seq_len)
{
  uint32_t seq_size = dig_size * 16;
  
  uint32_t ser_len = 0;
  uint32_t offset = 0;

  seq_size += ObLobSeqId::LOB_SEQ_ID_VERSION_LEN;
  seq_buf = static_cast<char*>(alloc->alloc(seq_size));
  ASSERT_NE(seq_buf, nullptr);
  digits = static_cast<uint32_t*>(alloc->alloc(dig_size * sizeof(uint32_t)));
  ASSERT_NE(digits, nullptr);

  for (int i = 0; i < dig_size; i++) {
    if (i == 0) {
      seq_buf[offset++] = static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION);
    }
    digits[i] = random();
    (void)ObLobSeqId::store32be(seq_buf + offset, digits[i]);
    offset += sizeof(uint32_t);
  }

  seq_len = offset;
}

TEST_F(TestLobSeqId, parse)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = 32;

  ObArenaAllocator allocator;
  make_seq_id(&allocator, dig_size, seq_buf, digits, seq_len);

  ObString seq_str;
  seq_str.assign_ptr(seq_buf, seq_len);
  ObLobSeqIdGenerator seq_id(seq_str, &allocator);

  ASSERT_EQ(OB_SUCCESS, seq_id.parse());
  for (uint32_t i = 0; i < seq_id.dig_len_; ++i) {
    ASSERT_EQ(seq_id.digits_[i], digits[i]);
  }
}

TEST_F(TestLobSeqId, extend_internal)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = ObLobSeqIdGenerator::LOB_SEQ_DIGIT_DEFAULT_LEN;

  ObArenaAllocator allocator;
  make_seq_id(&allocator, dig_size, seq_buf, digits, seq_len);

  ObString seq_str;
  seq_str.assign_ptr(seq_buf, seq_len);
  ObLobSeqIdGenerator seq_id(seq_str, &allocator);

  ASSERT_EQ(OB_SUCCESS, seq_id.parse());
  for (uint32_t i = 0; i < seq_id.dig_len_; ++i) {
    ASSERT_EQ(seq_id.digits_[i], digits[i]);
  }

  ASSERT_EQ(OB_SUCCESS, seq_id.add_digits(256));
  ASSERT_EQ(OB_SUCCESS, seq_id.append_seq_buf(256));

  ASSERT_EQ(dig_size * 2, seq_id.dig_cap_);
  ASSERT_EQ(dig_size + 1, seq_id.dig_len_);     
}

TEST_F(TestLobSeqId, next_seq_id)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = ObLobSeqIdGenerator::LOB_SEQ_DIGIT_DEFAULT_LEN;

  ObArenaAllocator allocator;
  ObString seq_str;
  ObLobSeqIdGenerator seq_id(seq_str, &allocator);

  ASSERT_EQ(seq_id.dig_cap_, 0);
  ASSERT_EQ(seq_id.dig_len_, 0);
  ASSERT_EQ(seq_id.digits_, nullptr);

  ObString next_seq_id;
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 1);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[0], 256);

  ASSERT_EQ(seq_id.cap_, 1024 * 8);
  ASSERT_EQ(seq_id.len_, ObLobSeqId::LOB_SEQ_ID_MIN_LEN);
  ASSERT_NE(seq_id.buf_, nullptr);
  ASSERT_EQ(static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION), next_seq_id.ptr()[0]);
  
  seq_id.reset();
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 1);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[0], 512);

  ASSERT_EQ(seq_id.len_, ObLobSeqId::LOB_SEQ_ID_MIN_LEN);
  ASSERT_NE(seq_id.buf_, nullptr);

  seq_id.reset();
  char buf[32] = {0};
  buf[0] = static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION);
  ObLobSeqId::store32be(buf + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN, UINT32_MAX);
  ObString overflow_str;
  overflow_str.assign_ptr(buf, ObLobSeqId::LOB_SEQ_ID_MIN_LEN);
  seq_id.set_seq_id(overflow_str);
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN + ObLobSeqId::LOB_SEQ_ID_UNIT_LEN,
            next_seq_id.length());
  ASSERT_EQ(UINT32_MAX, ObLobSeqId::load32be(
                            next_seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN, ObLobSeqId::load32be(
      next_seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_MIN_LEN));
  ASSERT_EQ(-1, ObLobSeqId(overflow_str).compare(ObLobSeqId(next_seq_id)));
  
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 2);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[1], 256);

  ASSERT_NE(seq_id.buf_, nullptr);
  ASSERT_EQ(static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION), seq_id.buf_[0]);
}

TEST_F(TestLobSeqId, fixed_seq_id_boundaries)
{
  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {0};
  ObString seq_id(sizeof(seq_buf), seq_buf);
  const int64_t max_idx = static_cast<int64_t>(
      ObLobSeqId::LOB_SEQ_STEP_MAX / ObLobSeqId::LOB_SEQ_STEP_LEN);

  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(max_idx, seq_id));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN, seq_id.length());
  ASSERT_EQ(static_cast<uint32_t>(max_idx) * ObLobSeqId::LOB_SEQ_STEP_LEN,
            ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObLobSeqId::get_seq_id(max_idx + 1, seq_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLobSeqId::get_seq_id(-1, seq_id));

  ObString null_seq_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLobSeqId::get_seq_id(0, null_seq_id));
}

TEST_F(TestLobSeqId, allocated_seq_id_boundaries)
{
  ObArenaAllocator allocator;
  ObString seq_id;
  const int64_t max_idx = static_cast<int64_t>(ObLobSeqId::LOB_SEQ_UNIT_MAX);
  const uint32_t max_seq_num = static_cast<uint32_t>(max_idx * ObLobSeqId::LOB_SEQ_STEP_LEN);

  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, 0, seq_id));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN, seq_id.length());
  ASSERT_EQ(0, ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));

  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, max_idx, seq_id));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN, seq_id.length());
  ASSERT_EQ(max_seq_num, ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));

  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, max_idx + 1, seq_id));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN + ObLobSeqId::LOB_SEQ_ID_UNIT_LEN, seq_id.length());
  ASSERT_EQ(max_seq_num, ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_STEP_LEN,
            ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_MIN_LEN));

  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(allocator, 2 * max_idx + 1, seq_id));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN + 2 * ObLobSeqId::LOB_SEQ_ID_UNIT_LEN, seq_id.length());
  ASSERT_EQ(max_seq_num, ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN));
  ASSERT_EQ(max_seq_num, ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_MIN_LEN));
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_STEP_LEN,
            ObLobSeqId::load32be(seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_MIN_LEN
                                 + ObLobSeqId::LOB_SEQ_ID_UNIT_LEN));

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLobSeqId::get_seq_id(allocator, -1, seq_id));
  ASSERT_TRUE(seq_id.empty());
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObLobSeqId::get_seq_id(allocator, INT64_MAX, seq_id));
  ASSERT_TRUE(seq_id.empty());
}

TEST_F(TestLobSeqId, get_hex_str)
{
  char seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN] = {
      static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION),
      '\0',
      static_cast<char>(0x0F),
      static_cast<char>(0xAB),
      static_cast<char>(0xFF)};
  ObString seq_id(sizeof(seq_buf), seq_buf);

  ASSERT_EQ("01000fabff", ObLobSeqId(seq_id).get_hex_str());
}

TEST_F(TestLobSeqId, to_string_buffer_size)
{
  ObLobId lob_id;
  lob_id.assign(UINT64_MAX, UINT64_MAX, UINT64_MAX);
  char lob_id_str_buf[ObLobId::MAX_TO_STRING_BUF_SIZE];
  const int64_t lob_id_str_len = lob_id.to_string(lob_id_str_buf, sizeof(lob_id_str_buf));
  ASSERT_GT(lob_id_str_len, 0);
  ASSERT_LT(lob_id_str_len, static_cast<int64_t>(sizeof(lob_id_str_buf)));

  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + 2 * ObLobSeqId::LOB_SEQ_ID_UNIT_LEN];
  seq_id_buf[0] = static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION);
  ObLobSeqId::store32be(seq_id_buf + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN, UINT32_MAX);
  ObLobSeqId::store32be(seq_id_buf + ObLobSeqId::LOB_SEQ_ID_MIN_LEN, UINT32_MAX);
  const ObLobSeqId seq_id(ObString(sizeof(seq_id_buf), seq_id_buf));
  ObArenaAllocator allocator;
  const int64_t seq_id_str_buf_len = seq_id.needed_to_string_buffer_size();
  char *seq_id_str_buf = static_cast<char *>(allocator.alloc(seq_id_str_buf_len));
  ASSERT_NE(seq_id_str_buf, nullptr);
  const int64_t seq_id_str_len = seq_id.to_string(seq_id_str_buf, seq_id_str_buf_len);
  ASSERT_GT(seq_id_str_len, 0);
  ASSERT_LT(seq_id_str_len, seq_id_str_buf_len);
}

struct ObLobDataOutRowCtxV0
{
  ObLobDataOutRowCtxV0()
    : is_full_(0), op_(0), offset_(0), check_sum_(0), seq_no_st_(0), seq_no_cnt_(0),
      del_seq_no_cnt_(0), modified_len_(0), first_meta_offset_(0)
  {}

  uint64_t is_full_ : 1;
  uint64_t op_ : 8;
  uint64_t offset_ : 55;
  uint64_t check_sum_;
  int64_t seq_no_st_;
  uint32_t seq_no_cnt_;
  uint32_t del_seq_no_cnt_; // for sql update
  uint64_t modified_len_;
  uint32_t first_meta_offset_;
};

TEST_F(TestLobSeqId, test_chunk_size_v0_to_v1)
{
  ObLobDataOutRowCtxV0 ctx_v0;
  ctx_v0.op_ = ObLobDataOutRowCtx::DIFF;
  ctx_v0.offset_ = 10000;
  ctx_v0.check_sum_ = 12345;
  ctx_v0.seq_no_st_ = 56778899;
  ctx_v0.seq_no_cnt_ = 5;
  ctx_v0.del_seq_no_cnt_ = 0;
  ctx_v0.modified_len_ = 111111;
  ctx_v0.first_meta_offset_ = 1234567;

  ObLobDataOutRowCtx ctx_v1;
  ASSERT_EQ(sizeof(ObLobDataOutRowCtx), sizeof(ObLobDataOutRowCtxV0));
  MEMCPY(&ctx_v1, &ctx_v0, sizeof(ObLobDataOutRowCtx));
  ASSERT_EQ(ctx_v0.is_full_, ctx_v1.is_full_);
  ASSERT_EQ(ctx_v0.offset_, ctx_v1.offset_);
  ASSERT_EQ(ctx_v0.check_sum_, ctx_v1.check_sum_);
  ASSERT_EQ(ctx_v0.seq_no_st_, ctx_v1.seq_no_st_);
  ASSERT_EQ(ctx_v0.seq_no_cnt_, ctx_v1.seq_no_cnt_);
  ASSERT_EQ(ctx_v0.del_seq_no_cnt_, ctx_v1.del_seq_no_cnt_);
  ASSERT_EQ(ctx_v0.modified_len_, ctx_v1.modified_len_);
  ASSERT_EQ(0, ctx_v1.chunk_size_);
  ASSERT_EQ(ctx_v0.first_meta_offset_, ctx_v1.first_meta_offset_);
}

TEST_F(TestLobSeqId, test_chunk_size_v1_to_v0)
{
  ObLobDataOutRowCtx ctx_v1;
  ctx_v1.op_ = ObLobDataOutRowCtx::DIFF;
  ctx_v1.offset_ = 10000;
  ctx_v1.check_sum_ = 12345;
  ctx_v1.seq_no_st_ = 56778899;
  ctx_v1.seq_no_cnt_ = 5;
  ctx_v1.del_seq_no_cnt_ = 0;
  ctx_v1.modified_len_ = 111111;
  ctx_v1.chunk_size_ = 4;
  ctx_v1.first_meta_offset_ = 1234567;
  
  ObLobDataOutRowCtxV0 ctx_v0;
  ASSERT_EQ(sizeof(ObLobDataOutRowCtx), sizeof(ObLobDataOutRowCtxV0));
  MEMCPY(&ctx_v0, &ctx_v1, sizeof(ObLobDataOutRowCtx));
  ASSERT_EQ(ctx_v0.is_full_, ctx_v1.is_full_);
  ASSERT_EQ(ctx_v0.offset_, ctx_v1.offset_);
  ASSERT_EQ(ctx_v0.check_sum_, ctx_v1.check_sum_);
  ASSERT_EQ(ctx_v0.seq_no_st_, ctx_v1.seq_no_st_);
  ASSERT_EQ(ctx_v0.seq_no_cnt_, ctx_v1.seq_no_cnt_);
  ASSERT_EQ(ctx_v0.del_seq_no_cnt_, ctx_v1.del_seq_no_cnt_);
  ASSERT_EQ(ctx_v0.modified_len_, ctx_v1.modified_len_);
  ASSERT_EQ(4, ctx_v1.chunk_size_);
  ASSERT_EQ(ctx_v0.first_meta_offset_ & 0xffffff, ctx_v1.first_meta_offset_ );
}

TEST_F(TestLobSeqId, test_lob_outrow_locator_size)
{
  ASSERT_EQ(25, sizeof(ObLobCommon) + sizeof(ObLobId));
}


}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_seq_id.log*");
  OB_LOGGER.set_file_name("test_lob_seq_id.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
