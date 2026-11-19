/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include "lib/utility/serialization.h"
#include "share/rc/ob_tenant_base.h"
#define private public
#include "sql/engine/cmd/ob_load_data_direct_impl.h"
#undef private
#include "sql/engine/cmd/ob_load_data_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
using DataBuffer = ObLoadDataDirectImpl::DataBuffer;
using DataReader = ObLoadDataDirectImpl::DataReader;
using LoadExecuteContext = ObLoadDataDirectImpl::LoadExecuteContext;
const int64_t HEADER_SIZE = sizeof(ObLoadFileBuffer);
const int64_t BASE_CAPACITY = ObLoadFileBuffer::MAX_BUFFER_SIZE - HEADER_SIZE;

void append_data(DataBuffer &buffer, const std::string &data)
{
  ASSERT_LE(static_cast<int64_t>(data.size()), buffer.get_remain_length());
  MEMCPY(buffer.file_buffer_->current_ptr(), data.data(), data.size());
  buffer.update_data_length(data.size());
}

TEST(TestLoadDataDirectBuffer, grow_preserves_tail_and_respects_capacity_limit)
{
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.init());
  append_data(buffer, "head-tail");
  buffer.advance(5);
  ASSERT_EQ(OB_SUCCESS, buffer.reserve(BASE_CAPACITY + 1));
  EXPECT_EQ(0, buffer.pos_);
  EXPECT_EQ(2 * BASE_CAPACITY, buffer.file_buffer_->get_buffer_size());
  EXPECT_EQ(OB_SIZE_OVERFLOW, buffer.reserve(DataBuffer::MAX_CAPACITY + 1));
  EXPECT_EQ(2 * BASE_CAPACITY, buffer.file_buffer_->get_buffer_size());
  ASSERT_EQ(4, buffer.get_data_length());
  EXPECT_EQ(0, MEMCMP(buffer.data(), "tail", 4));
}

TEST(TestLoadDataDirectBuffer, reuse_keeps_grown_buffer)
{
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.init());
  ASSERT_EQ(OB_SUCCESS, buffer.reserve(BASE_CAPACITY + 1));
  ObLoadFileBuffer *const grown_buffer = buffer.file_buffer_;
  append_data(buffer, "previous data");

  buffer.reuse();
  EXPECT_TRUE(buffer.empty());
  ASSERT_EQ(OB_SUCCESS, buffer.reserve(BASE_CAPACITY + 1));
  ASSERT_EQ(grown_buffer, buffer.file_buffer_);
  EXPECT_EQ(2 * BASE_CAPACITY, buffer.file_buffer_->get_buffer_size());
}

class TestStreamFileReader : public ObStreamFileReader
{
public:
  TestStreamFileReader(ObIAllocator &allocator, const ObString &data, int read_ret)
    : ObStreamFileReader(allocator), data_(data), offset_(0), read_ret_(read_ret), eof_(false)
  {}

  int read(char *buf, int64_t count, int64_t &read_size) override
  {
    read_size = 0;
    if (OB_SUCCESS != read_ret_ || offset_ == data_.length()) {
      eof_ = true;
    } else {
      read_size = MIN(count, data_.length() - offset_);
      MEMCPY(buf, data_.ptr() + offset_, read_size);
      offset_ += read_size;
    }
    return read_ret_;
  }

  int64_t get_offset() const override { return offset_; }
  bool eof() const override { return eof_; }

private:
  ObString data_;
  int64_t offset_;
  int read_ret_;
  bool eof_;
};

TEST(TestLoadDataDecompressReader, drains_final_snappy_block_before_eof)
{
  ObArenaAllocator allocator;
  const char data[] = "last-tail\nnext\n";
  const int32_t data_size = sizeof(data) - 1;
  const int64_t header_size = 2 * sizeof(int32_t);
  char frame[64];
  int64_t compressed_size = 0;
  snappy_1_2_2::ObSnappyCompressor1_2_2 compressor(allocator);
  ASSERT_EQ(OB_SUCCESS, compressor.compress(data, data_size, frame + header_size,
                                            sizeof(frame) - header_size, compressed_size));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(frame, sizeof(frame), pos, data_size));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(frame, sizeof(frame), pos, compressed_size));

  ObFileReadParam param;
  param.compression_format_ = ObCSVGeneralFormat::ObCSVCompression::SNAPPY_BLOCK;
  ObDecompressFileReader reader(allocator);
  ObFileReader *source_reader = OB_NEW(TestStreamFileReader,
                                      ObMemAttr(OB_SERVER_TENANT_ID, "TestLoadReader"),
                                      allocator, ObString(header_size + compressed_size, frame),
                                      OB_SUCCESS);
  ASSERT_NE(nullptr, source_reader);
  const int ret = reader.open(param, source_reader);
  if (OB_SUCCESS != ret) {
    ObFileReader::destroy(source_reader);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  char output[sizeof(data)];
  const int64_t first_read_size = 8;
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.readn(output, first_read_size, read_size));
  ASSERT_EQ(first_read_size, read_size);
  EXPECT_FALSE(reader.eof());
  ASSERT_EQ(OB_SUCCESS, reader.readn(output + first_read_size,
                                    sizeof(output) - first_read_size, read_size));
  ASSERT_EQ(data_size - first_read_size, read_size);
  EXPECT_TRUE(reader.eof());
  EXPECT_EQ(0, MEMCMP(output, data, data_size));
}

class TestLoadDataDirectReader : public ::testing::Test
{
protected:
  void init_reader(const char *data, bool read_raw, int read_ret = OB_SUCCESS)
  {
    execute_ctx_.job_stat_ = &job_stat_;
    reader_.execute_ctx_ = &execute_ctx_;
    reader_.read_raw_ = read_raw;
    if (!read_raw) {
      ObDataInFileStruct format;
      ASSERT_EQ(OB_SUCCESS, reader_.csv_parser_.init(format, 1, CS_TYPE_UTF8MB4_BIN));
      ASSERT_EQ(OB_SUCCESS, reader_.data_trimer_.alloc_buf(reader_.allocator_, 2));
    }
    reader_.file_reader_ = OB_NEW(TestStreamFileReader,
                                 ObMemAttr(OB_SERVER_TENANT_ID, "TestLoadReader"),
                                 reader_.allocator_, data, read_ret);
    ASSERT_NE(nullptr, reader_.file_reader_);
    reader_.is_inited_ = true;
  }

  ObLoadDataStat job_stat_;
  LoadExecuteContext execute_ctx_;
  DataReader reader_;
};

TEST_F(TestLoadDataDirectReader, grows_and_restores_csv_tail)
{
  ASSERT_NO_FATAL_FAILURE(init_reader("head\npartial\n", false));
  DataBuffer first;
  DataBuffer next;
  ASSERT_EQ(OB_SUCCESS, first.resize(8));
  ASSERT_EQ(OB_SUCCESS, next.resize(2));

  int64_t line_count = 0;
  ASSERT_EQ(OB_SUCCESS, reader_.get_next_buffer(first, line_count));
  EXPECT_EQ(1, line_count);
  ASSERT_EQ(5, first.get_data_length());
  EXPECT_EQ(0, MEMCMP(first.data(), "head\n", 5));
  EXPECT_EQ(3, reader_.data_trimer_.get_incomplate_data_len());
  EXPECT_EQ(4, reader_.data_trimer_.get_buffer_size());

  ASSERT_EQ(OB_SUCCESS, reader_.get_next_buffer(next, line_count));
  EXPECT_EQ(1, line_count);
  ASSERT_EQ(8, next.get_data_length());
  EXPECT_EQ(0, MEMCMP(next.data(), "partial\n", 8));
  EXPECT_EQ(0, reader_.data_trimer_.get_incomplate_data_len());
  EXPECT_EQ(OB_ITER_END, reader_.get_next_buffer(next, line_count));
}

TEST_F(TestLoadDataDirectReader, full_final_record_does_not_grow_buffer)
{
  ASSERT_NO_FATAL_FAILURE(init_reader("last", false));
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.resize(4));

  int64_t line_count = 0;
  ASSERT_EQ(OB_SUCCESS, reader_.get_next_buffer(buffer, line_count));
  EXPECT_TRUE(reader_.is_end_file());
  EXPECT_EQ(1, line_count);
  EXPECT_EQ(4, buffer.file_buffer_->get_buffer_size());
  ASSERT_EQ(4, buffer.get_data_length());
  EXPECT_EQ(0, MEMCMP(buffer.data(), "last", 4));
}

TEST_F(TestLoadDataDirectReader, raw_eof_preserves_pending_bytes)
{
  ASSERT_NO_FATAL_FAILURE(init_reader("last", true));
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.resize(4));
  ASSERT_EQ(OB_SUCCESS, reader_.get_next_raw_buffer(buffer));
  EXPECT_FALSE(reader_.is_end_file());
  EXPECT_EQ(0, buffer.get_remain_length());

  ASSERT_EQ(OB_SUCCESS, reader_.get_next_raw_buffer(buffer));
  EXPECT_TRUE(reader_.is_end_file());
  EXPECT_EQ(4, buffer.file_buffer_->get_buffer_size());
  EXPECT_EQ(OB_SUCCESS, reader_.get_next_raw_buffer(buffer));
  ASSERT_EQ(4, buffer.get_data_length());
  EXPECT_EQ(0, MEMCMP(buffer.data(), "last", 4));
  buffer.advance(4);
  EXPECT_EQ(OB_ITER_END, reader_.get_next_raw_buffer(buffer));
}

TEST_F(TestLoadDataDirectReader, raw_growth_preserves_probe_byte)
{
  ASSERT_NO_FATAL_FAILURE(init_reader("last!", true));
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.resize(4));
  ASSERT_EQ(OB_SUCCESS, reader_.get_next_raw_buffer(buffer));
  EXPECT_FALSE(reader_.is_end_file());

  ASSERT_EQ(OB_SUCCESS, reader_.get_next_raw_buffer(buffer));
  EXPECT_TRUE(reader_.is_end_file());
  EXPECT_EQ(8, buffer.file_buffer_->get_buffer_size());
  ASSERT_EQ(5, buffer.get_data_length());
  EXPECT_EQ(0, MEMCMP(buffer.data(), "last!", 5));
  EXPECT_EQ(5, ATOMIC_LOAD(&job_stat_.read_bytes_));
}

TEST_F(TestLoadDataDirectReader, read_error_is_not_end_of_iteration)
{
  ASSERT_NO_FATAL_FAILURE(init_reader("", false, OB_IO_ERROR));
  DataBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, buffer.resize(4));

  int64_t line_count = 0;
  EXPECT_EQ(OB_IO_ERROR, reader_.get_next_buffer(buffer, line_count));
  EXPECT_EQ(0, line_count);
  EXPECT_FALSE(reader_.is_iter_end_);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::share::ObTenantBase tenant(OB_SERVER_TENANT_ID);
  oceanbase::share::ObTenantEnv::set_tenant(&tenant);
  const int ret = RUN_ALL_TESTS();
  oceanbase::share::ObTenantEnv::set_tenant(nullptr);
  return ret;
}
