/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_ARROW_CODEC_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_ARROW_CODEC_H_

#include <arrow/api.h>
#include <parquet/schema.h>
#include <parquet/types.h>
#include <memory>

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace common
{
class ObIVector;
class ObArenaAllocator;
}

namespace sql
{

class ObEvalCtx;
class ObExecContext;
class ObExpr;
struct ObResultColumnMetadata;

// Physical values returned by one primitive Parquet ColumnReader::ReadBatch().
// Result logical conversion never consumes this object directly: the compiled
// read plan first reconstructs the registered Arrow Array, then invokes its
// Arrow-to-OB callback.
struct ObAsyncResultParquetValueBatch
{
  enum class PhysicalKind : int8_t
  {
    INVALID = 0,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY
  };

  ObAsyncResultParquetValueBatch()
      : kind_(PhysicalKind::INVALID), values_(nullptr), row_count_(0), value_count_(0),
        definition_levels_(nullptr), max_definition_level_(0), fixed_byte_width_(0)
  {}

  PhysicalKind kind_;
  const void *values_;
  int64_t row_count_;
  int64_t value_count_;
  const int16_t *definition_levels_;
  int16_t max_definition_level_;
  int32_t fixed_byte_width_;
};

class ObAsyncResultArrowReadPlan
{
public:
  ObAsyncResultArrowReadPlan();

  bool is_valid() const;
  ObAsyncResultParquetValueBatch::PhysicalKind get_physical_kind() const
  {
    return physical_kind_;
  }
  int decode_parquet_batch(const ObAsyncResultParquetValueBatch &batch,
                           arrow::MemoryPool *memory_pool,
                           ObEvalCtx &eval_ctx,
                           ObExpr &expr,
                           const int64_t row_offset) const;

private:
  using ParquetToArrowFunc = int (*)(const ObAsyncResultParquetValueBatch &,
                                     const std::shared_ptr<arrow::DataType> &,
                                     arrow::MemoryPool *,
                                     std::shared_ptr<arrow::Array> &);
  using ArrowToObFunc = int (*)(const arrow::Array &,
                                ObEvalCtx &,
                                ObExpr &,
                                int64_t);

  friend class ObAsyncResultArrowCodec;
  std::shared_ptr<arrow::DataType> arrow_type_;
  ObAsyncResultParquetValueBatch::PhysicalKind physical_kind_;
  ParquetToArrowFunc parquet_to_arrow_func_;
  ArrowToObFunc arrow_to_ob_func_;
};

class ObAsyncResultArrowWritePlan
{
public:
  ObAsyncResultArrowWritePlan();

  void reset();
  const std::shared_ptr<arrow::Schema> &get_schema() const { return schema_; }
  int build_batch(ObEvalCtx &eval_ctx,
                  const ObBatchRows &batch_rows,
                  arrow::MemoryPool *memory_pool,
                  std::shared_ptr<arrow::RecordBatch> &record_batch) const;

private:
  using ObToArrowFunc = int (*)(const ObExpr &,
                                const common::ObIVector &,
                                const ObBatchRows &,
                                arrow::ArrayBuilder &,
                                common::ObArenaAllocator &);
  using ObToArrowEvalFunc = int (*)(const ObExpr &,
                                    ObEvalCtx &,
                                    const common::ObIVector &,
                                    const ObBatchRows &,
                                    arrow::ArrayBuilder &,
                                    common::ObArenaAllocator &);
  using ObToArrowZeroCopyFunc = int (*)(const common::ObIVector &,
                                        const ObBatchRows &,
                                        const std::shared_ptr<arrow::DataType> &,
                                        std::shared_ptr<arrow::Array> &,
                                        bool &);

  struct ColumnPlan
  {
    ColumnPlan()
        : expr_(nullptr), arrow_type_(), ob_to_arrow_func_(nullptr),
          ob_to_arrow_eval_func_(nullptr), zero_copy_func_(nullptr)
    {}
    TO_STRING_KV(KP_(expr));

    ObExpr *expr_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    ObToArrowFunc ob_to_arrow_func_;
    ObToArrowEvalFunc ob_to_arrow_eval_func_;
    ObToArrowZeroCopyFunc zero_copy_func_;
  };

  friend class ObAsyncResultArrowCodec;
  std::shared_ptr<arrow::Schema> schema_;
  common::ObSEArray<ColumnPlan, 8> columns_;
};

// Private, versioned Result Store type codec.  This is the sole owner of the
// OB logical type <-> Arrow carrier registry and of value conversion in both
// directions.  Engine operators only build and invoke the compiled plans.
class ObAsyncResultArrowCodec final
{
public:
  static constexpr int64_t CURRENT_CODEC_VERSION = 2;

  static int build_write_plan(ObExecContext &exec_ctx,
                              const common::ObIArray<ObExpr *> &select_exprs,
                              const common::ObIArray<common::ObString> &alias_names,
                              const common::ObIArray<uint64_t> &field_ids,
                              ObAsyncResultArrowWritePlan &plan);

  static int build_read_plan(const ObExecContext &exec_ctx,
                             const ObExpr &expr,
                             const arrow::Field &field,
                             const parquet::ColumnDescriptor &column_descriptor,
                             ObAsyncResultArrowReadPlan &plan);

  static int validate_schema(const arrow::Schema &schema,
                             const int64_t parquet_column_count);

  static int fill_result_column_metadata(common::ObIAllocator &allocator,
                                         const ObExpr &expr,
                                         ObResultColumnMetadata &column);

  static bool is_supported_result_column_metadata(
      const ObResultColumnMetadata &column);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_ARROW_CODEC_H_
