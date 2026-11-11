/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/tx/ob_tx_seq.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/serialization.h"

#include "lib/allocator/page_arena.h"
#include "storage/lob/ob_ext_info_callback.h"
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobExtInfo : public ::testing::Test
{
public:
  TestLobExtInfo() {}
  virtual ~TestLobExtInfo() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestLobExtInfo, test_lob_id_serialize)
{
  const uint64_t test_values[][3] = {
    {0, 0, 0},
    {7, 1001, 42},
    {0x0123456789abcdefULL, 0x8000000000000000ULL, UINT64_MAX},
    {UINT64_MAX, UINT64_MAX, UINT64_MAX}
  };
  ObArenaAllocator allocator;
  for (const auto &values : test_values) {
    ObLobId source;
    source.assign(values[0], values[1], values[2]);
    const int64_t buf_len = source.get_serialize_size();
    char *buf = static_cast<char *>(allocator.alloc(buf_len));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
    ASSERT_EQ(buf_len, pos);

    // 验证版本头及直接编码的内部成员值，成员值由 htonll() 转换后保存。
    int64_t version = 0;
    int64_t body_len = 0;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, buf_len, pos, version));
    ASSERT_EQ(1, version);
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, buf_len, pos, body_len));
    ASSERT_EQ(buf_len - pos, body_len);
    for (const uint64_t value : values) {
      uint64_t decoded_value = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, buf_len, pos, decoded_value));
      ASSERT_EQ(htonll(value), decoded_value);
    }
    ASSERT_EQ(buf_len, pos);

    ObLobId decoded;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, buf_len, pos));
    ASSERT_EQ(buf_len, pos);
    ASSERT_EQ(source, decoded);
  }
}


TEST_F(TestLobExtInfo, test_lob_summary_ext_info_log)
{
  ObArenaAllocator allocator;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_summary_seq_id(seq_id));

  ObLobSummaryExtInfoLog log;
  log.lob_aux_table_id_ = 1001;
  log.lob_operation_seq_no_ = transaction::ObTxSEQ(1234, 0);
  log.seq_id_ = seq_id;
  log.byte_len_ = 1024;
  log.char_len_ = 512;
  log.lob_info_ = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\",\"chunk_count\":1,\"last_write_type\":0}");
  ASSERT_TRUE(log.is_valid());

  const int64_t buf_len = log.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  int64_t pos = 0;
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, log.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObLobSummaryExtInfoLog decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, buf_len, pos));
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(log.lob_aux_table_id_, decoded.lob_aux_table_id_);
  ASSERT_EQ(log.lob_operation_seq_no_, decoded.lob_operation_seq_no_);
  ASSERT_EQ(log.seq_id_, decoded.seq_id_);
  ASSERT_EQ(log.byte_len_, decoded.byte_len_);
  ASSERT_EQ(log.char_len_, decoded.char_len_);
  ASSERT_EQ(log.lob_info_, decoded.lob_info_);
}

TEST_F(TestLobExtInfo, test_lob_data_ext_info_log)
{
  ObArenaAllocator allocator;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(1, seq_id));

  ObLobDataExtInfoLog log;
  log.lob_aux_table_id_ = 1002;
  log.lob_operation_seq_no_ = transaction::ObTxSEQ(1234, 0);
  log.seq_id_ = seq_id;
  log.lob_data_ = ObString::make_string("lob piece data");
  ASSERT_TRUE(log.is_valid());

  const int64_t buf_len = log.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  int64_t pos = 0;
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, log.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObLobDataExtInfoLog decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, buf_len, pos));
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(log.lob_aux_table_id_, decoded.lob_aux_table_id_);
  ASSERT_EQ(log.lob_operation_seq_no_, decoded.lob_operation_seq_no_);
  ASSERT_EQ(log.seq_id_, decoded.seq_id_);
  ASSERT_EQ(log.lob_data_, decoded.lob_data_);
  ASSERT_TRUE(decoded.is_logging_);

  log.is_logging_ = false;
  log.lob_data_.reset();
  ASSERT_TRUE(log.is_valid());
  const int64_t no_logging_buf_len = log.get_serialize_size();
  char *no_logging_buf = static_cast<char *>(allocator.alloc(no_logging_buf_len));
  ASSERT_NE(nullptr, no_logging_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, log.serialize(no_logging_buf, no_logging_buf_len, pos));
  ObLobDataExtInfoLog no_logging_decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, no_logging_decoded.deserialize(no_logging_buf, no_logging_buf_len, pos));
  ASSERT_TRUE(no_logging_decoded.is_valid());
  ASSERT_FALSE(no_logging_decoded.is_logging_);
  ASSERT_TRUE(no_logging_decoded.lob_data_.empty());
}

TEST_F(TestLobExtInfo, test_lob_meta_info_serialize)
{
  ObArenaAllocator allocator;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_summary_seq_id(seq_id));

  ObLobMetaInfo source;
  source.lob_col_idx_ = 7;
  source.lob_id_.assign(7, 1001, 42);
  source.seq_id_ = seq_id;
  source.byte_len_ = 1024;
  source.char_len_ = 512;
  source.lob_extra_info_ = ObString::make_string("lob summary info");
  source.lob_block_id_ = ObString::make_string("lob block id");

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObLobMetaInfo decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_EQ(source.lob_col_idx_, decoded.lob_col_idx_);
  ASSERT_EQ(source.lob_id_, decoded.lob_id_);
  ASSERT_EQ(source.seq_id_, decoded.seq_id_);
  ASSERT_EQ(source.byte_len_, decoded.byte_len_);
  ASSERT_EQ(source.char_len_, decoded.char_len_);
  ASSERT_EQ(source.lob_extra_info_, decoded.lob_extra_info_);
  ASSERT_EQ(source.lob_block_id_, decoded.lob_block_id_);

  ObLobMetaInfo restored;
  ASSERT_EQ(OB_SUCCESS, restored.deep_copy(allocator, decoded));
  ASSERT_EQ(source.lob_col_idx_, restored.lob_col_idx_);
  ASSERT_EQ(source.lob_id_, restored.lob_id_);
  ASSERT_EQ(source.seq_id_, restored.seq_id_);
  ASSERT_EQ(source.byte_len_, restored.byte_len_);
  ASSERT_EQ(source.char_len_, restored.char_len_);
  ASSERT_EQ(source.lob_extra_info_, restored.lob_extra_info_);
  ASSERT_EQ(source.lob_block_id_, restored.lob_block_id_);
  ASSERT_NE(decoded.seq_id_.ptr(), restored.seq_id_.ptr());
  ASSERT_NE(decoded.lob_extra_info_.ptr(), restored.lob_extra_info_.ptr());
  ASSERT_NE(decoded.lob_block_id_.ptr(), restored.lob_block_id_.ptr());

  // 深拷贝结果不依赖 RPC 接收缓冲区的生命周期。
  MEMSET(buf, 0, buf_len);
  ASSERT_EQ(source.seq_id_, restored.seq_id_);
  ASSERT_EQ(source.lob_extra_info_, restored.lob_extra_info_);
  ASSERT_EQ(source.lob_block_id_, restored.lob_block_id_);

  decoded.reset();
  ASSERT_EQ(0, decoded.lob_col_idx_);
  ASSERT_EQ(ObLobId(), decoded.lob_id_);
  ASSERT_TRUE(decoded.seq_id_.empty());
  ASSERT_EQ(0, decoded.byte_len_);
  ASSERT_EQ(0, decoded.char_len_);
  ASSERT_TRUE(decoded.lob_extra_info_.empty());
  ASSERT_TRUE(decoded.lob_block_id_.empty());
}

TEST_F(TestLobExtInfo, test_summary_rpc_serialization)
{
  ObLobMetaInfo summary;
  summary.lob_col_idx_ = 5;
  summary.lob_id_.assign(5, 1001, 123456789);
  summary.seq_id_ = ObString::make_string("summary");
  summary.byte_len_ = 1ULL << 32;
  summary.char_len_ = 1024;
  summary.lob_extra_info_ = ObString::make_string("{\"chunk_count\":0}");
  summary.lob_block_id_ = ObString::make_string("external-identity");
  char buf[1024];
  char body_buf[1024];
  int64_t pos = 0;
  int64_t body_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, summary));
  ASSERT_EQ(summary.get_serialize_size(), pos);

  // Keep the upstream UNIS version header; encode LobId as a nested object with a version header.
  int64_t header_pos = 0;
  int64_t version = 0;
  int64_t body_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, header_pos, version));
  ASSERT_EQ(1, version);
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, header_pos, body_len));
  ASSERT_EQ(pos - header_pos, body_len);
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.lob_col_idx_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.lob_id_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.seq_id_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.byte_len_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.char_len_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.lob_extra_info_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(body_buf, sizeof(body_buf), body_pos,
                                             summary.lob_block_id_));
  ASSERT_EQ(body_len, body_pos);
  ASSERT_EQ(0, MEMCMP(body_buf, buf + header_pos, body_len));

  ObLobMetaInfo decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, read_pos, decoded));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(summary.lob_col_idx_, decoded.lob_col_idx_);
  ASSERT_TRUE(summary.lob_id_ == decoded.lob_id_);
  ASSERT_EQ(summary.byte_len_, decoded.byte_len_);
  ASSERT_EQ(summary.char_len_, decoded.char_len_);
  ASSERT_EQ(summary.seq_id_, decoded.seq_id_);
  ASSERT_EQ(summary.lob_extra_info_, decoded.lob_extra_info_);
  ASSERT_EQ(summary.lob_block_id_, decoded.lob_block_id_);

  // Summary handler deep-copies to read context allocator before RPC buffer release/reuse.
  ObArenaAllocator allocator;
  ObLobMetaInfo stable_summary;
  ASSERT_EQ(OB_SUCCESS, stable_summary.deep_copy(allocator, decoded));
  MEMSET(buf, 0, sizeof(buf));
  ASSERT_EQ(summary.seq_id_, stable_summary.seq_id_);
  ASSERT_EQ(summary.lob_extra_info_, stable_summary.lob_extra_info_);
  ASSERT_EQ(summary.lob_block_id_, stable_summary.lob_block_id_);
}

TEST_F(TestLobExtInfo, test_summary_rpc_reject_truncated_response)
{
  ObLobMetaInfo summary;
  summary.lob_col_idx_ = 1;
  summary.lob_id_.assign(1, 1001, 9999);
  summary.seq_id_ = ObString::make_string("summary");
  summary.lob_extra_info_ = ObString::make_string("{\"chunk_count\":0}");
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, summary.serialize(buf, sizeof(buf), pos));
  // Keep the full UNIS length header to verify that responses truncated in transit fail.
  int64_t header_pos = 0;
  int64_t version = 0;
  int64_t body_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, header_pos, version));
  ASSERT_EQ(1, version);
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, header_pos, body_len));
  ASSERT_EQ(pos - header_pos, body_len);
  ASSERT_GT(body_len, 0);
  for (int64_t len = header_pos; len < pos; ++len) {
    ObLobMetaInfo decoded;
    int64_t read_pos = 0;
    ASSERT_NE(OB_SUCCESS, decoded.deserialize(buf, len, read_pos)) << "length=" << len;
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_ext_info.log*");
  OB_LOGGER.set_file_name("test_lob_ext_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
