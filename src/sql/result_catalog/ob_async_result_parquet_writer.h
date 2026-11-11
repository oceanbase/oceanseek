/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_WRITER_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_WRITER_H_

#include <memory>
#include <parquet/arrow/writer.h>

#include "lib/container/ob_se_array.h"
#include "sql/engine/basic/ob_external_file_writer.h"
#include "sql/result_catalog/ob_async_result_file.h"

namespace oceanbase
{
namespace sql
{

// Private Result Store writer. It shares the object-storage output stream
// with SELECT INTO, but never uses the generic Parquet column writer or the
// lake-table file descriptor path.
class ObAsyncResultParquetWriter final : public ObExternalFileWriter
{
public:
  ObAsyncResultParquetWriter(
      const common::ObObjectStorageInfo *access_info,
      const IntoFileLocation &file_location,
      const std::shared_ptr<arrow::Schema> &arrow_schema,
      const int64_t writer_sqc_id,
      const int64_t writer_task_id)
      : ObExternalFileWriter(access_info, file_location),
        arrow_file_writer_(nullptr),
        output_stream_(nullptr),
        arrow_schema_(arrow_schema),
        record_count_(0),
        estimated_input_bytes_(0),
        writer_sqc_id_(writer_sqc_id),
        writer_task_id_(writer_task_id),
        files_()
  {
    files_.set_attr(common::ObMemAttr(MTL_ID(), "ResultFiles"));
  }
  ~ObAsyncResultParquetWriter() override;

  bool is_file_writer_null() const { return arrow_file_writer_ == nullptr; }
  int open_arrow_file_writer(ObArrowMemPool &arrow_alloc,
                             int64_t row_group_rows,
                             int64_t compress_type_index);
  int write_record_batch(const arrow::RecordBatch &record_batch);
  int write_file() override;
  int64_t get_file_size() override;
  int close_file() override;
  const common::ObIArray<ObAsyncResultFileDesc> &get_files() const
  {
    return files_;
  }

private:
  std::unique_ptr<parquet::arrow::FileWriter> arrow_file_writer_;
  std::shared_ptr<arrow::io::OutputStream> output_stream_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
  int64_t record_count_;
  int64_t estimated_input_bytes_;
  int64_t writer_sqc_id_;
  int64_t writer_task_id_;
  common::ObSEArray<ObAsyncResultFileDesc, 1> files_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_WRITER_H_
