/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_TABLE_ROW_ITER_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_TABLE_ROW_ITER_H_

#include <memory>
#include <parquet/api/reader.h>

#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/result_catalog/ob_async_result_arrow_codec.h"

namespace oceanbase
{
namespace sql
{

// Reads only Result Catalog files and only through the private lossless
// codec. File and row-group traversal are owned here, independently of the
// external-table Parquet iterator.
class ObAsyncResultParquetTableRowIterator final : public ObExternalTableRowIterator
{
public:
  ObAsyncResultParquetTableRowIterator();
  ~ObAsyncResultParquetTableRowIterator() override;

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_rows(int64_t &count, int64_t capacity) override;
  int get_next_row() override { return OB_NOT_SUPPORTED; }
  int get_next_row(common::ObNewRow *&row) override
  {
    UNUSED(row);
    return OB_ERR_UNEXPECTED;
  }
  void reset() override;

private:
  int next_file();
  int next_row_group();
  int prepare_file_schema();
  int prepare_selected_columns();
  int read_batch(int64_t &count, int64_t capacity);
  int read_column(int64_t column_idx, int64_t row_count, ObEvalCtx &eval_ctx);
  int fill_file_meta_columns(int64_t row_count, ObEvalCtx &eval_ctx);
  int convert_columns(int64_t row_count, ObEvalCtx &eval_ctx);
  int assign_converted_column(
      ObEvalCtx &eval_ctx, ObExpr &from, ObExpr &to, int64_t row_count);

private:
  ObExternalIteratorState state_;
  common::ObArenaAllocator allocator_;
  ObArrowMemPool arrow_alloc_;
  parquet::ReaderProperties read_props_;
  ObExternalFileAccess file_access_;
  std::shared_ptr<ObArrowFile> arrow_file_;
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::shared_ptr<parquet::FileMetaData> file_metadata_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::shared_ptr<parquet::RowGroupReader> row_group_reader_;
  common::ObArrayWrap<int> column_indices_;
  common::ObArrayWrap<std::shared_ptr<parquet::ColumnReader>> column_readers_;
  common::ObArrayWrap<ObAsyncResultArrowReadPlan> read_plans_;
  common::ObArrayWrap<int16_t> definition_levels_;
  common::ObArrayWrap<int16_t> repetition_levels_;
  common::ObArrayWrap<char *> file_url_ptrs_;
  common::ObArrayWrap<ObLength> file_url_lens_;
  ObBitVector *skip_vector_;
  void *value_buffer_;
  common::ObSqlString url_;
  int64_t next_row_group_idx_;
  int64_t end_row_group_idx_;
  int64_t row_group_row_count_;
  int64_t read_rows_in_group_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_PARQUET_TABLE_ROW_ITER_H_
