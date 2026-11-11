/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/result_catalog/ob_async_result_parquet_writer.h"

#include <arrow/util/byte_size.h>
#include <exception>

namespace oceanbase
{
namespace sql
{

int ObAsyncResultFileDesc::set(
    const common::ObString &path,
    const int64_t row_count,
    const int64_t byte_count,
    const int64_t writer_sqc_id,
    const int64_t writer_task_id,
    const int64_t file_sequence)
{
  int ret = OB_SUCCESS;
  if (path.empty() || path.length() >= sizeof(file_path_)
      || row_count <= 0 || byte_count <= 0
      || writer_sqc_id < 0 || writer_task_id < 0 || file_sequence < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result file descriptor",
             K(ret), K(path), K(row_count), K(byte_count),
             K(writer_sqc_id), K(writer_task_id), K(file_sequence));
  } else {
    MEMSET(file_path_, 0, sizeof(file_path_));
    MEMCPY(file_path_, path.ptr(), path.length());
    row_count_ = row_count;
    byte_count_ = byte_count;
    writer_sqc_id_ = writer_sqc_id;
    writer_task_id_ = writer_task_id;
    file_sequence_ = file_sequence;
  }
  return ret;
}

ObAsyncResultParquetWriter::~ObAsyncResultParquetWriter()
{
  arrow_file_writer_.reset();
  output_stream_.reset();
  arrow_schema_.reset();
}

int ObAsyncResultParquetWriter::open_arrow_file_writer(
    ObArrowMemPool &arrow_alloc,
    const int64_t row_group_rows,
    const int64_t compress_type_index)
{
  int ret = OB_SUCCESS;
  try {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "ResultWriter"));
    if (arrow_schema_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async result arrow schema is null", K(ret));
    } else if (!is_file_opened_ && OB_FAIL(open_file())) {
      LOG_WARN("failed to open async result parquet file", K(ret), K(url_));
    } else {
      parquet::WriterProperties::Builder parquet_props_builder;
      parquet_props_builder.max_row_group_length(row_group_rows);
      parquet_props_builder.compression(
          static_cast<parquet::Compression::type>(compress_type_index));
      parquet_props_builder.memory_pool(&arrow_alloc);
      parquet_props_builder.enable_write_page_index();
      parquet::ArrowWriterProperties::Builder arrow_props_builder;
      arrow_props_builder.store_schema()->set_use_threads(false);
      output_stream_ = std::make_shared<ObParquetOutputStream>(
          &file_appender_, &storage_appender_, file_location_, url_);
      arrow::Result<std::unique_ptr<parquet::arrow::FileWriter>> writer_result =
          parquet::arrow::FileWriter::Open(
              *arrow_schema_,
              &arrow_alloc,
              output_stream_,
              parquet_props_builder.build(),
              arrow_props_builder.build());
      if (!writer_result.ok()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to open async result arrow parquet writer",
                 K(ret), K(url_),
                 "arrow_status", writer_result.status().ToString().c_str());
      } else {
        arrow_file_writer_ = std::move(writer_result).ValueOrDie();
        estimated_input_bytes_ = 0;
      }
    }
  } catch (const std::bad_alloc &ex) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("out of memory opening async result parquet writer",
             K(ret), K(url_), "info", ex.what());
  } catch (const std::exception &ex) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception opening async result parquet writer",
             K(ret), K(url_), "info", ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught unknown exception opening async result parquet writer",
             K(ret), K(url_));
  }
  return ret;
}

int ObAsyncResultParquetWriter::write_record_batch(
    const arrow::RecordBatch &record_batch)
{
  int ret = OB_SUCCESS;
  if (arrow_file_writer_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result arrow parquet writer is null", K(ret));
  } else {
    ObMallocHookAttrGuard guard(
        ObMemAttr(MTL_ID(), "ResultWriter"), false /* use_500 */);
    const arrow::Status status = arrow_file_writer_->WriteRecordBatch(record_batch);
    if (!status.ok()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to write async result arrow record batch",
               K(ret), K(url_), "arrow_status", status.ToString().c_str());
    } else {
      record_count_ += record_batch.num_rows();
      const int64_t batch_bytes = arrow::util::TotalBufferSize(record_batch);
      estimated_input_bytes_ = batch_bytes < 0
                                  || estimated_input_bytes_ > INT64_MAX - batch_bytes
                              ? INT64_MAX : estimated_input_bytes_ + batch_bytes;
    }
  }
  return ret;
}

int ObAsyncResultParquetWriter::write_file()
{
  // The Parquet footer is flushed by close_file().
  return OB_SUCCESS;
}

int64_t ObAsyncResultParquetWriter::get_file_size()
{
  const int64_t written_size = IntoFileLocation::SERVER_DISK == file_location_
                               ? file_appender_.get_buffered_file_pos()
                               : storage_appender_.offset_;
  // The public Arrow writer does not expose buffered Parquet bytes. Estimate
  // file rotation from input buffers while a row group remains unflushed.
  // Both counters cover this file, so take their maximum to avoid counting
  // flushed input twice. close_file() records the exact finalized file size.
  return written_size < 0 ? INT64_MAX : MAX(written_size, estimated_input_bytes_);
}

int ObAsyncResultParquetWriter::close_file()
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  int64_t final_file_size = 0;
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "ResultWriter"));
  if (arrow_file_writer_ != nullptr) {
    const arrow::Status status = arrow_file_writer_->Close();
    if (!status.ok()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to close async result arrow parquet writer",
               K(ret), K(url_), "arrow_status", status.ToString().c_str());
    } else if (output_stream_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async result parquet output stream is null after close",
               K(ret), K(url_));
    } else {
      const arrow::Result<int64_t> size_result = output_stream_->Tell();
      if (!size_result.ok() || size_result.ValueOrDie() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get finalized async result parquet file size",
                 K(ret), K(url_),
                 "arrow_status", size_result.ok()
                     ? "invalid file size"
                     : size_result.status().ToString().c_str());
      } else {
        final_file_size = size_result.ValueOrDie();
      }
    }
    arrow_file_writer_.reset();
  }
  if (is_file_opened_) {
    if (OB_SUCC(ret) && record_count_ > 0) {
      ObAsyncResultFileDesc file;
      if (OB_FAIL(file.set(url_, record_count_, final_file_size,
                           writer_sqc_id_, writer_task_id_, files_.count()))) {
        LOG_WARN("failed to describe closed async result file", K(ret), K(url_));
      } else if (OB_FAIL(files_.push_back(file))) {
        LOG_WARN("failed to retain closed async result file", K(ret), K(url_));
      }
    }
    record_count_ = 0;
    // Always finish or abort the multipart writer. The coordinator removes
    // the job prefix if any task or publication step fails.
    close_ret = ObExternalFileWriter::close_file();
    if (OB_SUCCESS != close_ret) {
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
      LOG_WARN("failed to close async result storage writer",
               K(ret), K(close_ret), K(url_));
    }
  }
  output_stream_.reset();
  estimated_input_bytes_ = 0;
  return ret;
}

} // namespace sql
} // namespace oceanbase
