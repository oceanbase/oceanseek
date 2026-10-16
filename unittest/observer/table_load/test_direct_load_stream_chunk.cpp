/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstring>
#include <string>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace observer
{
namespace
{
const int64_t TEST_BATCH_SIZE = 8;
const int64_t TEST_COL_COUNT = 3;
const int64_t TEST_RES_BUF_LEN = 4096;
const uint64_t TEST_TENANT_ID = common::OB_SYS_TENANT_ID;

class TestExprFrame
{
public:
  TestExprFrame()
    : allocator_("DLSChunkUT"),
      exec_ctx_(allocator_),
      eval_ctx_(exec_ctx_),
      exprs_(),
      frame_size_(0)
  {
  }

  int init(const int64_t col_count, const int64_t batch_size, const bool batch_result)
  {
    int ret = OB_SUCCESS;
    char **frames = nullptr;
    char *frame = nullptr;
    int64_t pos = 0;
    frame_size_ = col_count * (sizeof(common::ObDatum) * batch_size
                  + sizeof(sql::ObEvalInfo) + TEST_RES_BUF_LEN * batch_size);
    if (OB_FAIL(exprs_.reserve(col_count))) {
      OB_LOG(WARN, "fail to reserve expr array", KR(ret));
    } else if (OB_ISNULL(frames = static_cast<char **>(allocator_.alloc(sizeof(char *))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(frame = static_cast<char *>(allocator_.alloc(frame_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(frame, 0, frame_size_);
      frames[0] = frame;
      eval_ctx_.frames_ = frames;
      eval_ctx_.set_max_batch_size(batch_size);
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        sql::ObExpr *expr = nullptr;
        if (OB_ISNULL(expr = new (allocator_.alloc(sizeof(sql::ObExpr))) sql::ObExpr())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          expr->frame_idx_ = 0;
          expr->datum_off_ = pos;
          pos += sizeof(common::ObDatum) * batch_size;
          expr->eval_info_off_ = pos;
          pos += sizeof(sql::ObEvalInfo);
          expr->res_buf_off_ = pos;
          expr->res_buf_len_ = TEST_RES_BUF_LEN;
          pos += TEST_RES_BUF_LEN * batch_size;
          expr->batch_result_ = batch_result;
          expr->batch_idx_mask_ = batch_result ? UINT64_MAX : 0;
          for (int64_t j = 0; j < batch_size; ++j) {
            common::ObDatum &datum = expr->locate_expr_datum(eval_ctx_, j);
            datum.set_null();
            datum.ptr_ = eval_ctx_.frames_[0] + expr->res_buf_off_ + TEST_RES_BUF_LEN * j;
          }
          expr->get_eval_info(eval_ctx_).set_evaluated(true);
          expr->get_eval_info(eval_ctx_).set_projected(true);
          if (OB_FAIL(exprs_.push_back(expr))) {
            OB_LOG(WARN, "fail to push expr", KR(ret));
          }
        }
      }
    }
    return ret;
  }

  sql::ObEvalCtx &eval_ctx() { return eval_ctx_; }
  common::ObIArray<sql::ObExpr *> &exprs() { return exprs_; }

  void set_int(const int64_t row_idx, const int64_t col_idx, const int64_t value)
  {
    common::ObDatum &datum = exprs_.at(col_idx)->locate_expr_datum(eval_ctx_, row_idx);
    datum.set_int(value);
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_evaluated(true);
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_projected(true);
  }

  void set_null(const int64_t row_idx, const int64_t col_idx)
  {
    common::ObDatum &datum = exprs_.at(col_idx)->locate_expr_datum(eval_ctx_, row_idx);
    datum.set_null();
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_evaluated(true);
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_projected(true);
  }

  void set_string(const int64_t row_idx, const int64_t col_idx, const char *value)
  {
    common::ObDatum &datum = exprs_.at(col_idx)->locate_expr_datum(eval_ctx_, row_idx);
    datum.set_string(value, static_cast<uint32_t>(strlen(value)));
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_evaluated(true);
    exprs_.at(col_idx)->get_eval_info(eval_ctx_).set_projected(true);
  }

  const common::ObDatum &datum(const int64_t row_idx, const int64_t col_idx) const
  {
    return exprs_.at(col_idx)->locate_expr_datum(const_cast<sql::ObEvalCtx &>(eval_ctx_), row_idx);
  }

private:
  common::ObArenaAllocator allocator_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  common::ObSEArray<sql::ObExpr *, TEST_COL_COUNT> exprs_;
  int64_t frame_size_;
};

void fill_row(TestExprFrame &frame, const int64_t row_idx, const int64_t id, const char *value)
{
  frame.set_int(row_idx, 0, id);
  frame.set_null(row_idx, 1);
  frame.set_string(row_idx, 2, value);
}

void expect_row(const TestExprFrame &frame, const int64_t row_idx, const int64_t id, const char *value)
{
  const common::ObDatum &id_datum = frame.datum(row_idx, 0);
  const common::ObDatum &null_datum = frame.datum(row_idx, 1);
  const common::ObDatum &str_datum = frame.datum(row_idx, 2);
  ASSERT_FALSE(id_datum.is_null());
  EXPECT_EQ(id, id_datum.get_int());
  EXPECT_TRUE(null_datum.is_null());
  ASSERT_FALSE(str_datum.is_null());
  ASSERT_EQ(static_cast<int64_t>(strlen(value)), str_datum.len_);
  EXPECT_EQ(0, MEMCMP(value, str_datum.ptr_, str_datum.len_));
}

void copy_chunk_to_shared_buf(common::ObIAllocator &allocator,
                              const common::ObString &chunk,
                              char *&buf,
                              int64_t &buf_size)
{
  if (buf_size < chunk.length()) {
    buf = static_cast<char *>(allocator.alloc(chunk.length()));
    ASSERT_NE(nullptr, buf);
    buf_size = chunk.length();
  }
  MEMCPY(buf, chunk.ptr(), chunk.length());
}
}

TEST(TestDirectLoadStreamChunk, writer_reader_row_roundtrip_and_meta)
{
  TestExprFrame input;
  TestExprFrame output;
  ObDirectLoadStreamChunkWriter writer;
  common::ObString chunk;
  common::ObString repeated_chunk;
  ObDirectLoadStreamChunkHeader header;
  ObDirectLoadStreamChunkReader reader;
  std::string encoded_chunk;

  ASSERT_EQ(OB_SUCCESS, input.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output.init(TEST_COL_COUNT, TEST_BATCH_SIZE, false));
  fill_row(input, 0, 7, "alpha-varlen");

  ASSERT_EQ(OB_SUCCESS, writer.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(input.exprs(), input.eval_ctx()));
  ASSERT_EQ(1, writer.get_row_count());
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(chunk));
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(repeated_chunk));
  EXPECT_EQ(chunk.ptr(), repeated_chunk.ptr());
  EXPECT_EQ(chunk.length(), repeated_chunk.length());
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamChunkHeader::decode(chunk.ptr(), chunk.length(), header));
  EXPECT_EQ(1, header.row_count_);
  EXPECT_GT(header.payload_size_, 0);
  encoded_chunk.assign(chunk.ptr(), chunk.length());

  EXPECT_EQ(OB_NOT_INIT, reader.open(chunk.ptr(), chunk.length(), TEST_COL_COUNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.init(common::OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), TEST_COL_COUNT));
  ASSERT_EQ(1, reader.get_row_count());
  ASSERT_EQ(header.payload_size_, reader.get_payload_size());
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(output.exprs(), output.eval_ctx()));
  expect_row(output, 0, 7, "alpha-varlen");
  EXPECT_EQ(encoded_chunk, std::string(chunk.ptr(), chunk.length()));
  MEMSET(const_cast<char *>(chunk.ptr()), 0x5a, chunk.length());
  expect_row(output, 0, 7, "alpha-varlen");
  EXPECT_EQ(OB_ITER_END, reader.get_next_row(output.exprs(), output.eval_ctx()));
}

TEST(TestDirectLoadStreamChunk, batch_partial_and_reader_reuse_with_same_buffer)
{
  common::ObArenaAllocator allocator("DLSChunkUT");
  TestExprFrame input;
  TestExprFrame output;
  ObDirectLoadStreamChunkWriter writer;
  ObDirectLoadStreamChunkReader reader;
  common::ObString first_chunk;
  common::ObString second_chunk;
  char *shared_buf = nullptr;
  int64_t shared_buf_size = 0;
  int64_t read_rows = 0;

  ASSERT_EQ(OB_SUCCESS, input.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  fill_row(input, 0, 1, "first-0");
  fill_row(input, 1, 2, "first-1-wide-string-value");
  fill_row(input, 2, 3, "first-2");

  ASSERT_EQ(OB_SUCCESS, writer.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(input.exprs(), input.eval_ctx(), 0, 3));
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(first_chunk));

  copy_chunk_to_shared_buf(allocator, first_chunk, shared_buf, shared_buf_size);
  ASSERT_EQ(OB_SUCCESS, reader.open(shared_buf, first_chunk.length(), TEST_COL_COUNT));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_batch(output.exprs(), output.eval_ctx(), 2, read_rows));
  ASSERT_EQ(2, read_rows);
  expect_row(output, 0, 1, "first-0");
  expect_row(output, 1, 2, "first-1-wide-string-value");
  const char *saved_ptr = output.datum(1, 2).ptr_;

  ASSERT_EQ(OB_SUCCESS, reader.get_next_batch(output.exprs(), output.eval_ctx(), 2, read_rows));
  ASSERT_EQ(1, read_rows);
  expect_row(output, 0, 3, "first-2");
  EXPECT_EQ(OB_ITER_END, reader.get_next_batch(output.exprs(), output.eval_ctx(), 2, read_rows));

  writer.reuse();
  fill_row(input, 0, 9, "second-0");
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(input.exprs(), input.eval_ctx(), 0, 1));
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(second_chunk));
  EXPECT_EQ(first_chunk.ptr(), second_chunk.ptr());
  copy_chunk_to_shared_buf(allocator, second_chunk, shared_buf, shared_buf_size);
  reader.reuse();
  ASSERT_EQ(OB_SUCCESS, reader.open(shared_buf, second_chunk.length(), TEST_COL_COUNT));
  ASSERT_EQ(OB_SUCCESS, reader.get_next_batch(output.exprs(), output.eval_ctx(), 1, read_rows));
  ASSERT_EQ(1, read_rows);
  expect_row(output, 0, 9, "second-0");
  ASSERT_NE(shared_buf, saved_ptr);
  expect_row(output, 1, 2, "first-1-wide-string-value");
}

TEST(TestDirectLoadStreamChunk, batch_frame_bounds_and_null_encoding)
{
  TestExprFrame input;
  TestExprFrame output;
  ObDirectLoadStreamChunkWriter writer;
  ObDirectLoadStreamChunkReader reader;
  common::ObString chunk;
  int64_t read_rows = 0;

  ASSERT_EQ(OB_SUCCESS, input.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output.init(TEST_COL_COUNT, 2, true));
  fill_row(input, 0, 1, "frame-0");
  fill_row(input, 1, 2, "frame-1");
  fill_row(input, 2, 3, "frame-2");
  const char *input_null_ptr = input.datum(0, 1).ptr_;
  ASSERT_NE(nullptr, input_null_ptr);

  ASSERT_EQ(OB_SUCCESS, writer.init(TEST_TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            writer.append_batch(input.exprs(), input.eval_ctx(), TEST_BATCH_SIZE - 1, 2));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            writer.append_batch(input.exprs(), input.eval_ctx(), INT64_MAX, 2));
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(input.exprs(), input.eval_ctx(), 0, 3));
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(chunk));

  const sql::ObChunkDatumStore::StoredRow *stored_row =
      reinterpret_cast<const sql::ObChunkDatumStore::StoredRow *>(
          chunk.ptr() + sizeof(ObDirectLoadStreamChunkHeader));
  ASSERT_TRUE(stored_row->cells()[1].is_null());
  EXPECT_EQ(0, reinterpret_cast<intptr_t>(stored_row->cells()[1].ptr_));
  EXPECT_NE(input_null_ptr, stored_row->cells()[1].ptr_);

  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.open(chunk.ptr(), chunk.length(), TEST_COL_COUNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.get_next_batch(output.exprs(), output.eval_ctx(), 3, read_rows));
  EXPECT_EQ(0, read_rows);
  ASSERT_EQ(OB_SUCCESS, reader.get_next_batch(output.exprs(), output.eval_ctx(), 2, read_rows));
  EXPECT_EQ(2, read_rows);
  expect_row(output, 0, 1, "frame-0");
  expect_row(output, 1, 2, "frame-1");

  std::string bytes(chunk.ptr(), chunk.length());
  sql::ObChunkDatumStore::StoredRow *bad_row =
      reinterpret_cast<sql::ObChunkDatumStore::StoredRow *>(
          &bytes[sizeof(ObDirectLoadStreamChunkHeader)]);
  bad_row->cells()[1].ptr_ = reinterpret_cast<const char *>(1);
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  bytes.assign(chunk.ptr(), chunk.length());
  bad_row = reinterpret_cast<sql::ObChunkDatumStore::StoredRow *>(
      &bytes[sizeof(ObDirectLoadStreamChunkHeader)]);
  bad_row->cells()[1].len_ = 1;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));
}

TEST(TestDirectLoadStreamChunk, reject_bad_chunk_boundaries)
{
  TestExprFrame input;
  ObDirectLoadStreamChunkWriter writer;
  common::ObString chunk;
  ObDirectLoadStreamChunkReader reader;

  ASSERT_EQ(OB_SUCCESS, input.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  fill_row(input, 0, 1, "bad-case");
  ASSERT_EQ(OB_SUCCESS, writer.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, reader.init(TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, writer.append_row(input.exprs(), input.eval_ctx()));
  ASSERT_EQ(OB_SUCCESS, writer.build_chunk(chunk));

  std::string bytes(chunk.ptr(), chunk.length());
  reinterpret_cast<ObDirectLoadStreamChunkHeader *>(&bytes[0])->magic_ = 0;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  bytes.assign(chunk.ptr(), chunk.length());
  reinterpret_cast<ObDirectLoadStreamChunkHeader *>(&bytes[0])->payload_size_ += 1;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  bytes.assign(chunk.ptr(), chunk.length());
  reinterpret_cast<ObDirectLoadStreamChunkHeader *>(&bytes[0])->row_count_ = 2;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  bytes.assign(chunk.ptr(), chunk.length());
  sql::ObChunkDatumStore::StoredRow *row = reinterpret_cast<sql::ObChunkDatumStore::StoredRow *>(
      &bytes[sizeof(ObDirectLoadStreamChunkHeader)]);
  row->row_size_ = sizeof(sql::ObChunkDatumStore::StoredRow) - 1;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  bytes.assign(chunk.ptr(), chunk.length());
  row = reinterpret_cast<sql::ObChunkDatumStore::StoredRow *>(
      &bytes[sizeof(ObDirectLoadStreamChunkHeader)]);
  row->cells()[0].ptr_ = reinterpret_cast<const char *>(row->row_size_ - 1);
  row->cells()[0].pack_ = 8;
  EXPECT_EQ(OB_INVALID_DATA, reader.open(bytes.data(), bytes.size(), TEST_COL_COUNT));

  EXPECT_EQ(OB_INVALID_DATA, reader.open(chunk.ptr(), chunk.length(), TEST_COL_COUNT - 1));
}

TEST(TestDirectLoadStreamChunk, writer_rejects_empty_and_payload_limit)
{
  TestExprFrame input;
  ObDirectLoadStreamChunkWriter writer;
  ObDirectLoadStreamChunkWriter oversized_writer;
  ObDirectLoadStreamChunkHeader oversized_header;
  common::ObString chunk;

  ASSERT_EQ(OB_SUCCESS, input.init(TEST_COL_COUNT, TEST_BATCH_SIZE, true));
  fill_row(input, 0, 1, "limit-case");
  EXPECT_EQ(OB_INVALID_ARGUMENT, oversized_writer.init(TEST_TENANT_ID, INT32_MAX));
  oversized_header.row_count_ = 1;
  oversized_header.payload_size_ = INT32_MAX;
  EXPECT_FALSE(oversized_header.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.init(common::OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, writer.init(TEST_TENANT_ID, 32));
  EXPECT_EQ(OB_SIZE_OVERFLOW, writer.append_row(input.exprs(), input.eval_ctx()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.build_chunk(chunk));
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
