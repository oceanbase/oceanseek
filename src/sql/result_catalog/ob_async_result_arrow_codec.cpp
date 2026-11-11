/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/result_catalog/ob_async_result_arrow_codec.h"

#include <arrow/array/data.h>
#include <arrow/util/key_value_metadata.h>
#include <charconv>
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/enumset/ob_enum_set_meta.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_i_vector.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/result_catalog/ob_async_result_metadata.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{

static const char *const CODEC_KEY = "ob.async_result.codec";
static const char *const CODEC_V1 = "v1";
static const char *const CODEC_V2 = "v2";
static const char *const FIELD_ID_KEY = "PARQUET:field_id";
static const char *const MAPPING_VERSION_KEY = "ob.async_result.mapping_version";
static const char *const ENCODING_KEY = "ob.async_result.encoding";
static const char *const OB_TYPE_ID_KEY = "ob.async_result.ob_type_id";
static const char *const PRECISION_KEY = "ob.async_result.precision";
static const char *const SCALE_KEY = "ob.async_result.scale";
static const char *const COLLATION_ID_KEY = "ob.async_result.collation_id";
static const char *const COLLATION_LEVEL_KEY = "ob.async_result.collation_level";
static const char *const LENGTH_KEY = "ob.async_result.length";
static const char *const LENGTH_SEMANTICS_KEY = "ob.async_result.length_semantics";
static const char *const ENCODING_NATIVE = "native";
static const char *const ENCODING_DIRECT_BYTES = "direct_bytes";
static const char *const ENCODING_OPAQUE = "opaque_v1";
static const char *const ENCODING_COLLECTION_BINARY = "collection_binary_v1";
static const char OPAQUE_MAGIC[] = {'O', 'B', 'A', 'R'};
static const uint8_t OPAQUE_VALUE_VERSION = 1;
static const int64_t OPAQUE_HEADER_SIZE = sizeof(OPAQUE_MAGIC) + 2;

using ObToArrowFunc = int (*)(const ObExpr &,
                              const ObIVector &,
                              const ObBatchRows &,
                              arrow::ArrayBuilder &,
                              ObArenaAllocator &);
using ObToArrowEvalFunc = int (*)(const ObExpr &,
                                  ObEvalCtx &,
                                  const ObIVector &,
                                  const ObBatchRows &,
                                  arrow::ArrayBuilder &,
                                  ObArenaAllocator &);
using ParquetToArrowFunc = int (*)(const ObAsyncResultParquetValueBatch &,
                                   const std::shared_ptr<arrow::DataType> &,
                                   arrow::MemoryPool *,
                                   std::shared_ptr<arrow::Array> &);
using ArrowToObFunc = int (*)(const arrow::Array &, ObEvalCtx &, ObExpr &, int64_t);
using ObToArrowZeroCopyFunc = int (*)(const ObIVector &,
                                      const ObBatchRows &,
                                      const std::shared_ptr<arrow::DataType> &,
                                      std::shared_ptr<arrow::Array> &,
                                      bool &);

enum class CarrierKind : int8_t
{
  INVALID = 0,
  NULLS,
  INT8,
  INT16,
  INT32,
  INT64,
  UINT8,
  UINT16,
  UINT32,
  UINT64,
  FLOAT32,
  FLOAT64,
  DECIMAL128,
  DECIMAL256,
  DATE32,
  TIMESTAMP_NTZ_US,
  TIMESTAMP_UTC_US,
  UTF8,
  BINARY,
  LARGE_UTF8,
  LARGE_BINARY
};

struct MappingSpec
{
  MappingSpec()
      : carrier_(CarrierKind::INVALID), encoding_(nullptr), parquet_physical_(parquet::Type::UNDEFINED),
        arrow_type_(), arrow_type_name_(), parquet_logical_(),
        ob_to_arrow_func_(nullptr), ob_to_arrow_eval_func_(nullptr), zero_copy_func_(nullptr),
        parquet_to_arrow_func_(nullptr), arrow_to_ob_func_(nullptr)
  {}

  CarrierKind carrier_;
  const char *encoding_;
  parquet::Type::type parquet_physical_;
  std::shared_ptr<arrow::DataType> arrow_type_;
  std::string arrow_type_name_;
  std::string parquet_logical_;
  ObToArrowFunc ob_to_arrow_func_;
  ObToArrowEvalFunc ob_to_arrow_eval_func_;
  ObToArrowZeroCopyFunc zero_copy_func_;
  ParquetToArrowFunc parquet_to_arrow_func_;
  ArrowToObFunc arrow_to_ob_func_;
};

bool metadata_string_case_equal(const ObString &left, const ObString &right)
{
  bool equal = left.empty() && right.empty();
  if (!equal) {
    equal = 0 == left.case_compare(right);
  }
  return equal;
}

int resolve_semantic_obj_meta(const ObExpr &expr,
                              const ObExecContext &exec_ctx,
                              ObObjMeta &semantic_meta)
{
  int ret = OB_SUCCESS;
  const ObObjType type = expr.datum_meta_.type_;
  semantic_meta = expr.obj_meta_;
  if (ObCollectionSQLType == type) {
    // Collection expressions store a statement-local subschema id in the
    // collation bytes. It is not a durable collation descriptor.
    semantic_meta.set_collation_type(CS_TYPE_BINARY);
    semantic_meta.set_collation_level(CS_LEVEL_IMPLICIT);
  } else if (ObSetType == type) {
    const ObEnumSetMeta *enum_set_meta = nullptr;
    const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
    if (OB_FAIL(exec_ctx.get_enumset_meta_by_subschema_id(
            subschema_id, false /* is_in_pl */, enum_set_meta))) {
      LOG_WARN("failed to resolve async result enum/set semantic metadata",
               K(ret), K(type), K(subschema_id));
    } else if (OB_ISNULL(enum_set_meta)
               || !enum_set_meta->is_valid()
               || enum_set_meta->get_type() != type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async result enum/set semantic metadata",
               K(ret), K(type), K(subschema_id), KP(enum_set_meta));
    } else {
      semantic_meta = enum_set_meta->get_obj_meta();
    }
  }
  return ret;
}

template <int32_t ELEMENT_WIDTH>
int build_zero_copy_fixed_array(const ObIVector &vector,
                                const ObBatchRows &batch_rows,
                                const std::shared_ptr<arrow::DataType> &arrow_type,
                                std::shared_ptr<arrow::Array> &array,
                                bool &used)
{
  int ret = OB_SUCCESS;
  used = false;
  array.reset();
  if (batch_rows.size_ < 0 || OB_ISNULL(batch_rows.skip_) || OB_ISNULL(arrow_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result zero-copy input", K(ret), K(batch_rows.size_),
             KP(batch_rows.skip_), KP(arrow_type.get()));
  } else if (VEC_FIXED != vector.get_format()
             || !batch_rows.all_rows_active_
             || vector.has_null()) {
    // TODO: Nullable VEC_FIXED batches without skipped rows could still borrow
    // values after converting OB null bits to an Arrow validity bitmap.
    // Skipped rows or a different physical width still require conversion.
  } else {
    const ObFixedLengthBase &fixed_vector =
        static_cast<const ObFixedLengthBase &>(vector);
    if (ELEMENT_WIDTH != fixed_vector.get_length()) {
      // The OB vector and Arrow carrier have different physical widths.
    } else if (batch_rows.size_ > INT64_MAX / ELEMENT_WIDTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("async result zero-copy buffer is too large", K(ret),
               K(batch_rows.size_), K(ELEMENT_WIDTH));
    } else {
      std::vector<std::shared_ptr<arrow::Buffer>> buffers(2);
      // The caller synchronously passes this RecordBatch to the Parquet writer.
      // The OB vector therefore remains alive for the complete Arrow-buffer use.
      buffers[1] = std::make_shared<arrow::Buffer>(
          reinterpret_cast<const uint8_t *>(fixed_vector.get_data()),
          batch_rows.size_ * ELEMENT_WIDTH);
      const std::shared_ptr<arrow::ArrayData> array_data = arrow::ArrayData::Make(
          arrow_type, batch_rows.size_, buffers, 0 /* null_count */);
      array = arrow::MakeArray(array_data);
      if (OB_ISNULL(array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to wrap OB vector in async result Arrow array", K(ret));
      } else {
        used = true;
      }
    }
  }
  return ret;
}

int arrow_status_to_ret(const arrow::Status &status)
{
  int ret = OB_SUCCESS;
  if (!status.ok()) {
    ret = status.IsOutOfMemory() ? OB_ALLOCATE_MEMORY_FAILED : OB_ERR_UNEXPECTED;
  }
  return ret;
}

int get_metadata_value(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata,
                       const char *key,
                       const std::string *&value)
{
  int ret = OB_SUCCESS;
  int key_idx = -1;
  value = nullptr;
  if (OB_ISNULL(metadata) || OB_ISNULL(key)) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("async result Arrow metadata is missing", K(ret), KP(metadata.get()), KP(key));
  } else if ((key_idx = metadata->FindKey(key)) < 0) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("required async result Arrow metadata key is missing", K(ret), K(key));
  } else {
    value = &metadata->value(key_idx);
  }
  return ret;
}

int parse_metadata_int64(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata,
                         const char *key,
                         int64_t &value)
{
  int ret = OB_SUCCESS;
  const std::string *text = nullptr;
  value = 0;
  if (OB_FAIL(get_metadata_value(metadata, key, text))) {
    LOG_WARN("failed to get integer Arrow metadata", K(ret), K(key));
  } else {
    const char *begin = text->data();
    const char *end = begin + text->size();
    const std::from_chars_result parse_result = std::from_chars(begin, end, value);
    if (text->empty() || parse_result.ec != std::errc() || parse_result.ptr != end) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("invalid integer in async result Arrow metadata",
               K(ret), K(key), "metadata_value", text->c_str());
    }
  }
  return ret;
}

int deep_copy_string(ObIAllocator &allocator, const std::string &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  if (src.empty()) {
    dst.reset();
  } else if (OB_FAIL(ob_write_string(
                 allocator, ObString(src.length(), src.data()), dst, true))) {
    LOG_WARN("failed to copy async result codec string", K(ret));
  }
  return ret;
}

bool is_null_row(const ObAsyncResultParquetValueBatch &batch, const int64_t row_idx)
{
  return batch.max_definition_level_ > 0
      && (OB_ISNULL(batch.definition_levels_)
          || batch.definition_levels_[row_idx] < batch.max_definition_level_);
}

template <typename Builder, typename Src, typename Dst>
int build_numeric_arrow_array(const ObAsyncResultParquetValueBatch &batch,
                              const std::shared_ptr<arrow::DataType> &arrow_type,
                              arrow::MemoryPool *memory_pool,
                              std::shared_ptr<arrow::Array> &array)
{
  int ret = OB_SUCCESS;
  std::unique_ptr<arrow::ArrayBuilder> base_builder;
  if (batch.row_count_ < 0 || batch.value_count_ < 0
      || batch.value_count_ > batch.row_count_ || OB_ISNULL(batch.values_)
      || OB_ISNULL(memory_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result numeric Parquet batch", K(ret), K(batch.row_count_),
             K(batch.value_count_), KP(batch.values_), KP(memory_pool));
  } else if (OB_FAIL(arrow_status_to_ret(
                 arrow::MakeBuilder(memory_pool, arrow_type, &base_builder)))) {
    LOG_WARN("failed to create async result Arrow builder", K(ret));
  } else {
    Builder *builder = static_cast<Builder *>(base_builder.get());
    const Src *values = static_cast<const Src *>(batch.values_);
    int64_t value_idx = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch.row_count_; ++row_idx) {
      if (is_null_row(batch, row_idx)) {
        if (OB_FAIL(arrow_status_to_ret(builder->AppendNull()))) {
          LOG_WARN("failed to append null to async result Arrow array", K(ret), K(row_idx));
        }
      } else if (value_idx >= batch.value_count_) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("async result Parquet value count is too small",
                 K(ret), K(row_idx), K(value_idx), K(batch.value_count_));
      } else if (OB_FAIL(arrow_status_to_ret(
                     builder->Append(static_cast<Dst>(values[value_idx++]))))) {
        LOG_WARN("failed to append value to async result Arrow array", K(ret), K(row_idx));
      }
    }
    if (OB_SUCC(ret) && value_idx != batch.value_count_) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Parquet value count is inconsistent",
               K(ret), K(value_idx), K(batch.value_count_));
    } else if (OB_SUCC(ret)
               && OB_FAIL(arrow_status_to_ret(base_builder->Finish(&array)))) {
      LOG_WARN("failed to finish async result Arrow array", K(ret));
    }
  }
  return ret;
}

#define DEFINE_PARQUET_NUMERIC_TO_ARROW(func_name, builder_type, src_type, dst_type)          \
  int func_name(const ObAsyncResultParquetValueBatch &batch,                                  \
                const std::shared_ptr<arrow::DataType> &arrow_type,                           \
                arrow::MemoryPool *memory_pool,                                               \
                std::shared_ptr<arrow::Array> &array)                                         \
  {                                                                                            \
    return build_numeric_arrow_array<builder_type, src_type, dst_type>(                        \
        batch, arrow_type, memory_pool, array);                                                \
  }

DEFINE_PARQUET_NUMERIC_TO_ARROW(build_int8_array, arrow::Int8Builder, int32_t, int8_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_int16_array, arrow::Int16Builder, int32_t, int16_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_int32_array, arrow::Int32Builder, int32_t, int32_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_int64_array, arrow::Int64Builder, int64_t, int64_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_uint8_array, arrow::UInt8Builder, int32_t, uint8_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_uint16_array, arrow::UInt16Builder, int32_t, uint16_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_uint32_array, arrow::UInt32Builder, int32_t, uint32_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_uint64_array, arrow::UInt64Builder, int64_t, uint64_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_float_array, arrow::FloatBuilder, float, float)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_double_array, arrow::DoubleBuilder, double, double)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_date32_array, arrow::Date32Builder, int32_t, int32_t)
DEFINE_PARQUET_NUMERIC_TO_ARROW(build_timestamp_array, arrow::TimestampBuilder, int64_t, int64_t)

#undef DEFINE_PARQUET_NUMERIC_TO_ARROW

int append_arrow_bytes(arrow::StringBuilder &builder,
                       const uint8_t *value,
                       const int64_t length)
{
  const arrow::Status status = builder.Append(
      reinterpret_cast<const char *>(value), static_cast<int32_t>(length));
  return arrow_status_to_ret(status);
}

int append_arrow_bytes(arrow::BinaryBuilder &builder,
                       const uint8_t *value,
                       const int64_t length)
{
  const arrow::Status status = builder.Append(value, static_cast<int32_t>(length));
  return arrow_status_to_ret(status);
}

int append_arrow_bytes(arrow::LargeStringBuilder &builder,
                       const uint8_t *value,
                       const int64_t length)
{
  const arrow::Status status = builder.Append(
      reinterpret_cast<const char *>(value), length);
  return arrow_status_to_ret(status);
}

int append_arrow_bytes(arrow::LargeBinaryBuilder &builder,
                       const uint8_t *value,
                       const int64_t length)
{
  const arrow::Status status = builder.Append(value, length);
  return arrow_status_to_ret(status);
}

template <typename Builder>
int build_binary_arrow_array(const ObAsyncResultParquetValueBatch &batch,
                             const std::shared_ptr<arrow::DataType> &arrow_type,
                             arrow::MemoryPool *memory_pool,
                             std::shared_ptr<arrow::Array> &array)
{
  int ret = OB_SUCCESS;
  std::unique_ptr<arrow::ArrayBuilder> base_builder;
  if (batch.row_count_ < 0 || batch.value_count_ < 0
      || batch.value_count_ > batch.row_count_ || OB_ISNULL(batch.values_)
      || OB_ISNULL(memory_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result binary Parquet batch", K(ret), K(batch.row_count_),
             K(batch.value_count_), KP(batch.values_), KP(memory_pool));
  } else if (OB_FAIL(arrow_status_to_ret(
                 arrow::MakeBuilder(memory_pool, arrow_type, &base_builder)))) {
    LOG_WARN("failed to create async result binary Arrow builder", K(ret));
  } else {
    Builder *builder = static_cast<Builder *>(base_builder.get());
    const parquet::ByteArray *values =
        static_cast<const parquet::ByteArray *>(batch.values_);
    int64_t value_idx = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch.row_count_; ++row_idx) {
      if (is_null_row(batch, row_idx)) {
        if (OB_FAIL(arrow_status_to_ret(builder->AppendNull()))) {
          LOG_WARN("failed to append null to async result binary array", K(ret), K(row_idx));
        }
      } else if (value_idx >= batch.value_count_) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("async result binary value count is too small",
                 K(ret), K(row_idx), K(value_idx), K(batch.value_count_));
      } else {
        const parquet::ByteArray &value = values[value_idx++];
        if (value.len > 0 && OB_ISNULL(value.ptr)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("async result binary value has null data", K(ret), K(row_idx), K(value.len));
        } else if (OB_FAIL(append_arrow_bytes(*builder, value.ptr, value.len))) {
          LOG_WARN("failed to append async result binary value", K(ret), K(row_idx));
        }
      }
    }
    if (OB_SUCC(ret) && value_idx != batch.value_count_) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result binary value count is inconsistent",
               K(ret), K(value_idx), K(batch.value_count_));
    } else if (OB_SUCC(ret)
               && OB_FAIL(arrow_status_to_ret(base_builder->Finish(&array)))) {
      LOG_WARN("failed to finish async result binary Arrow array", K(ret));
    }
  }
  return ret;
}

int build_utf8_array(const ObAsyncResultParquetValueBatch &batch,
                     const std::shared_ptr<arrow::DataType> &arrow_type,
                     arrow::MemoryPool *memory_pool,
                     std::shared_ptr<arrow::Array> &array)
{
  return build_binary_arrow_array<arrow::StringBuilder>(
      batch, arrow_type, memory_pool, array);
}

int build_binary_array(const ObAsyncResultParquetValueBatch &batch,
                       const std::shared_ptr<arrow::DataType> &arrow_type,
                       arrow::MemoryPool *memory_pool,
                       std::shared_ptr<arrow::Array> &array)
{
  return build_binary_arrow_array<arrow::BinaryBuilder>(
      batch, arrow_type, memory_pool, array);
}

int build_large_utf8_array(const ObAsyncResultParquetValueBatch &batch,
                           const std::shared_ptr<arrow::DataType> &arrow_type,
                           arrow::MemoryPool *memory_pool,
                           std::shared_ptr<arrow::Array> &array)
{
  return build_binary_arrow_array<arrow::LargeStringBuilder>(
      batch, arrow_type, memory_pool, array);
}

int build_large_binary_array(const ObAsyncResultParquetValueBatch &batch,
                             const std::shared_ptr<arrow::DataType> &arrow_type,
                             arrow::MemoryPool *memory_pool,
                             std::shared_ptr<arrow::Array> &array)
{
  return build_binary_arrow_array<arrow::LargeBinaryBuilder>(
      batch, arrow_type, memory_pool, array);
}

template <typename Decimal, typename Builder>
int build_decimal_arrow_array(const ObAsyncResultParquetValueBatch &batch,
                              const std::shared_ptr<arrow::DataType> &arrow_type,
                              arrow::MemoryPool *memory_pool,
                              std::shared_ptr<arrow::Array> &array)
{
  int ret = OB_SUCCESS;
  std::unique_ptr<arrow::ArrayBuilder> base_builder;
  if (ObAsyncResultParquetValueBatch::PhysicalKind::FIXED_LEN_BYTE_ARRAY != batch.kind_
      || batch.row_count_ < 0 || batch.value_count_ < 0
      || batch.value_count_ > batch.row_count_ || batch.fixed_byte_width_ <= 0
      || OB_ISNULL(batch.values_) || OB_ISNULL(memory_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result decimal Parquet batch", K(ret), K(batch.kind_),
             K(batch.row_count_), K(batch.value_count_), K(batch.fixed_byte_width_));
  } else if (OB_FAIL(arrow_status_to_ret(
                 arrow::MakeBuilder(memory_pool, arrow_type, &base_builder)))) {
    LOG_WARN("failed to create async result decimal Arrow builder", K(ret));
  } else {
    Builder *builder = static_cast<Builder *>(base_builder.get());
    const parquet::FixedLenByteArray *values =
        static_cast<const parquet::FixedLenByteArray *>(batch.values_);
    int64_t value_idx = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch.row_count_; ++row_idx) {
      if (is_null_row(batch, row_idx)) {
        if (OB_FAIL(arrow_status_to_ret(builder->AppendNull()))) {
          LOG_WARN("failed to append null to async result decimal array", K(ret), K(row_idx));
        }
      } else if (value_idx >= batch.value_count_) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("async result decimal value count is too small",
                 K(ret), K(row_idx), K(value_idx), K(batch.value_count_));
      } else {
        const parquet::FixedLenByteArray &value = values[value_idx++];
        if (OB_ISNULL(value.ptr)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("async result decimal value has null data", K(ret), K(row_idx));
        } else {
          const arrow::Result<Decimal> decimal =
              Decimal::FromBigEndian(value.ptr, batch.fixed_byte_width_);
          if (!decimal.ok()) {
            ret = arrow_status_to_ret(decimal.status());
            LOG_WARN("failed to decode async result Parquet decimal",
                     K(ret), K(row_idx), "arrow_status", decimal.status().ToString().c_str());
          } else if (OB_FAIL(arrow_status_to_ret(builder->Append(decimal.ValueOrDie())))) {
            LOG_WARN("failed to append async result decimal", K(ret), K(row_idx));
          }
        }
      }
    }
    if (OB_SUCC(ret) && value_idx != batch.value_count_) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result decimal value count is inconsistent",
               K(ret), K(value_idx), K(batch.value_count_));
    } else if (OB_SUCC(ret)
               && OB_FAIL(arrow_status_to_ret(base_builder->Finish(&array)))) {
      LOG_WARN("failed to finish async result decimal Arrow array", K(ret));
    }
  }
  return ret;
}

int build_decimal128_array(const ObAsyncResultParquetValueBatch &batch,
                           const std::shared_ptr<arrow::DataType> &arrow_type,
                           arrow::MemoryPool *memory_pool,
                           std::shared_ptr<arrow::Array> &array)
{
  return build_decimal_arrow_array<arrow::Decimal128, arrow::Decimal128Builder>(
      batch, arrow_type, memory_pool, array);
}

int build_decimal256_array(const ObAsyncResultParquetValueBatch &batch,
                           const std::shared_ptr<arrow::DataType> &arrow_type,
                           arrow::MemoryPool *memory_pool,
                           std::shared_ptr<arrow::Array> &array)
{
  return build_decimal_arrow_array<arrow::Decimal256, arrow::Decimal256Builder>(
      batch, arrow_type, memory_pool, array);
}

int append_nulls(const ObExpr &expr,
                 const ObIVector &vector,
                 const ObBatchRows &batch_rows,
                 arrow::ArrayBuilder &builder,
                 ObArenaAllocator &allocator)
{
  UNUSED(expr);
  UNUSED(vector);
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (!batch_rows.skip_->contain(row_idx)
        && OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
      LOG_WARN("failed to append async result null", K(ret), K(row_idx));
    }
  }
  return ret;
}

#define DEFINE_OB_FIXED_TO_ARROW(func_name, builder_type, getter)                              \
  int func_name(const ObExpr &expr,                                                            \
                const ObIVector &vector,                                                       \
                const ObBatchRows &batch_rows,                                                 \
                arrow::ArrayBuilder &base_builder,                                             \
                ObArenaAllocator &allocator)                                                   \
  {                                                                                            \
    UNUSED(expr);                                                                              \
    UNUSED(allocator);                                                                         \
    int ret = OB_SUCCESS;                                                                      \
    builder_type &builder = static_cast<builder_type &>(base_builder);                          \
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {         \
      if (batch_rows.skip_->contain(row_idx)) {                                                 \
      } else if (vector.is_null(row_idx)) {                                                    \
        if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {                              \
          LOG_WARN("failed to append async result null", K(ret), K(row_idx));                  \
        }                                                                                      \
      } else if (OB_FAIL(arrow_status_to_ret(builder.Append(vector.getter(row_idx))))) {        \
        LOG_WARN("failed to append async result value", K(ret), K(row_idx));                   \
      }                                                                                        \
    }                                                                                          \
    return ret;                                                                                \
  }

DEFINE_OB_FIXED_TO_ARROW(append_tinyint, arrow::Int8Builder, get_tinyint)
DEFINE_OB_FIXED_TO_ARROW(append_smallint, arrow::Int16Builder, get_smallint)
DEFINE_OB_FIXED_TO_ARROW(append_int32, arrow::Int32Builder, get_int32)
DEFINE_OB_FIXED_TO_ARROW(append_int64, arrow::Int64Builder, get_int)
DEFINE_OB_FIXED_TO_ARROW(append_utinyint, arrow::UInt8Builder, get_utinyint)
DEFINE_OB_FIXED_TO_ARROW(append_usmallint, arrow::UInt16Builder, get_usmallint)
DEFINE_OB_FIXED_TO_ARROW(append_uint32, arrow::UInt32Builder, get_uint32)
DEFINE_OB_FIXED_TO_ARROW(append_uint64, arrow::UInt64Builder, get_uint64)
DEFINE_OB_FIXED_TO_ARROW(append_float, arrow::FloatBuilder, get_float)
DEFINE_OB_FIXED_TO_ARROW(append_double, arrow::DoubleBuilder, get_double)
DEFINE_OB_FIXED_TO_ARROW(append_date, arrow::Date32Builder, get_date)
DEFINE_OB_FIXED_TO_ARROW(append_datetime, arrow::TimestampBuilder, get_datetime)
DEFINE_OB_FIXED_TO_ARROW(append_timestamp, arrow::TimestampBuilder, get_timestamp)
DEFINE_OB_FIXED_TO_ARROW(append_time, arrow::Int64Builder, get_time)
DEFINE_OB_FIXED_TO_ARROW(append_bit, arrow::UInt64Builder, get_bit)
DEFINE_OB_FIXED_TO_ARROW(append_set, arrow::UInt64Builder, get_set)

#undef DEFINE_OB_FIXED_TO_ARROW

int append_mediumint(const ObExpr &expr,
                     const ObIVector &vector,
                     const ObBatchRows &batch_rows,
                     arrow::ArrayBuilder &base_builder,
                     ObArenaAllocator &allocator)
{
  UNUSED(expr);
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  arrow::Int32Builder &builder = static_cast<arrow::Int32Builder &>(base_builder);
  static const int32_t MIN_MEDIUMINT = -8388608;
  static const int32_t MAX_MEDIUMINT = 8388607;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result null", K(ret), K(row_idx));
      }
    } else {
      const int32_t value = vector.get_mediumint(row_idx);
      if (value < MIN_MEDIUMINT || value > MAX_MEDIUMINT) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid MEDIUMINT value in async result vector", K(ret), K(row_idx), K(value));
      } else if (OB_FAIL(arrow_status_to_ret(builder.Append(value)))) {
        LOG_WARN("failed to append async result MEDIUMINT", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

int append_umediumint(const ObExpr &expr,
                      const ObIVector &vector,
                      const ObBatchRows &batch_rows,
                      arrow::ArrayBuilder &base_builder,
                      ObArenaAllocator &allocator)
{
  UNUSED(expr);
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  arrow::UInt32Builder &builder = static_cast<arrow::UInt32Builder &>(base_builder);
  static const uint32_t MAX_UMEDIUMINT = 16777215U;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result null", K(ret), K(row_idx));
      }
    } else {
      const uint32_t value = vector.get_umediumint(row_idx);
      if (value > MAX_UMEDIUMINT) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid unsigned MEDIUMINT value in async result vector",
                 K(ret), K(row_idx), K(value));
      } else if (OB_FAIL(arrow_status_to_ret(builder.Append(value)))) {
        LOG_WARN("failed to append async result unsigned MEDIUMINT", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

int append_year(const ObExpr &expr,
                const ObIVector &vector,
                const ObBatchRows &batch_rows,
                arrow::ArrayBuilder &base_builder,
                ObArenaAllocator &allocator)
{
  UNUSED(expr);
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  arrow::UInt8Builder &builder = static_cast<arrow::UInt8Builder &>(base_builder);
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result null", K(ret), K(row_idx));
      }
    } else {
      const uint8_t value = vector.get_year(row_idx);
      if (OB_FAIL(arrow_status_to_ret(builder.Append(value)))) {
        LOG_WARN("failed to append async result YEAR", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

template <typename Builder>
int append_materialized_bytes(const ObExpr &expr,
                              const ObIVector &vector,
                              const ObBatchRows &batch_rows,
                              arrow::ArrayBuilder &base_builder,
                              ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  Builder &builder = static_cast<Builder &>(base_builder);
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result null", K(ret), K(row_idx));
      }
    } else {
      allocator.reuse();
      const ObDatum datum(vector.get_payload(row_idx), vector.get_length(row_idx), false);
      ObString value;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              allocator, datum, expr.datum_meta_, value))) {
        LOG_WARN("failed to materialize async result value", K(ret), K(row_idx),
                 K(expr.datum_meta_.type_));
      } else if (OB_FAIL(append_arrow_bytes(
                     builder,
                     reinterpret_cast<const uint8_t *>(value.ptr()),
                     value.length()))) {
        LOG_WARN("failed to append async result byte value", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

int append_utf8(const ObExpr &expr,
                const ObIVector &vector,
                const ObBatchRows &batch_rows,
                arrow::ArrayBuilder &base_builder,
                ObArenaAllocator &allocator)
{
  return append_materialized_bytes<arrow::StringBuilder>(
      expr, vector, batch_rows, base_builder, allocator);
}

int append_binary(const ObExpr &expr,
                  const ObIVector &vector,
                  const ObBatchRows &batch_rows,
                  arrow::ArrayBuilder &base_builder,
                  ObArenaAllocator &allocator)
{
  return append_materialized_bytes<arrow::BinaryBuilder>(
      expr, vector, batch_rows, base_builder, allocator);
}

int append_large_utf8(const ObExpr &expr,
                      const ObIVector &vector,
                      const ObBatchRows &batch_rows,
                      arrow::ArrayBuilder &base_builder,
                      ObArenaAllocator &allocator)
{
  return append_materialized_bytes<arrow::LargeStringBuilder>(
      expr, vector, batch_rows, base_builder, allocator);
}

int append_large_binary(const ObExpr &expr,
                        const ObIVector &vector,
                        const ObBatchRows &batch_rows,
                        arrow::ArrayBuilder &base_builder,
                        ObArenaAllocator &allocator)
{
  return append_materialized_bytes<arrow::LargeBinaryBuilder>(
      expr, vector, batch_rows, base_builder, allocator);
}

int append_collection_binary(const ObExpr &expr,
                             ObEvalCtx &eval_ctx,
                             const ObIVector &vector,
                             const ObBatchRows &batch_rows,
                             arrow::ArrayBuilder &base_builder,
                             ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  arrow::LargeBinaryBuilder &builder =
      static_cast<arrow::LargeBinaryBuilder &>(base_builder);
  static const uint8_t EMPTY_COLLECTION = 0;
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result collection null", K(ret), K(row_idx));
      }
    } else {
      const char *payload = nullptr;
      int32_t payload_len = 0;
      allocator.reuse();
      if (OB_FAIL(ObArrayExprUtils::get_collection_payload(
              allocator, eval_ctx, expr, row_idx, payload, payload_len,
              false /* with_lob_head */))) {
        LOG_WARN("failed to materialize async result collection",
                 K(ret), K(row_idx), K(expr.datum_meta_),
                 "subschema_id", expr.obj_meta_.get_subschema_id());
      } else if (payload_len < 0 || (payload_len > 0 && OB_ISNULL(payload))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid materialized async result collection",
                 K(ret), K(row_idx), K(payload_len), KP(payload));
      } else if (OB_FAIL(arrow_status_to_ret(builder.Append(
                     // Append copies the payload into Arrow-owned storage before
                     // the row allocator is reused on the next iteration.
                     payload_len > 0
                         ? reinterpret_cast<const uint8_t *>(payload)
                         : &EMPTY_COLLECTION,
                     payload_len)))) {
        LOG_WARN("failed to append async result collection bytes",
                 K(ret), K(row_idx), K(payload_len));
      }
    }
  }
  return ret;
}

template <typename ArrowDecimal, typename ArrowBuilder>
int append_decimal_value(ArrowBuilder &builder,
                         const ObDecimalInt *decimal_int,
                         const int32_t decimal_bytes)
{
  int ret = OB_SUCCESS;
  const int32_t arrow_bytes = static_cast<int32_t>(ArrowDecimal::kByteWidth);
  ObDecimalIntBuilder extended;
  extended.from(decimal_int, decimal_bytes);
  if (extended.get_int_bytes() < arrow_bytes) {
    extended.extend(arrow_bytes);
  }
  if (extended.get_int_bytes() != arrow_bytes) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
    LOG_WARN("async result decimal does not fit Arrow decimal carrier",
             K(ret), "decimal_bytes", extended.get_int_bytes(), K(arrow_bytes));
  } else {
    const uint8_t *little_endian =
        reinterpret_cast<const uint8_t *>(extended.get_decimal_int());
    uint8_t big_endian[ArrowDecimal::kByteWidth] = {0};
    for (int32_t i = 0; i < arrow_bytes; ++i) {
      big_endian[arrow_bytes - 1 - i] = little_endian[i];
    }
    const arrow::Result<ArrowDecimal> decimal =
        ArrowDecimal::FromBigEndian(big_endian, arrow_bytes);
    if (!decimal.ok()) {
      ret = arrow_status_to_ret(decimal.status());
      LOG_WARN("failed to build async result Arrow decimal", K(ret),
               "arrow_status", decimal.status().ToString().c_str());
    } else if (OB_FAIL(arrow_status_to_ret(builder.Append(decimal.ValueOrDie())))) {
      LOG_WARN("failed to append async result Arrow decimal", K(ret));
    }
  }
  return ret;
}

int append_decimal(const ObExpr &expr,
                   const ObIVector &vector,
                   const ObBatchRows &batch_rows,
                   arrow::ArrayBuilder &base_builder,
                   ObArenaAllocator &allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  const int32_t target_precision = expr.datum_meta_.precision_;
  const int32_t target_scale = expr.datum_meta_.scale_;
  const int32_t target_bytes =
      wide::ObDecimalIntConstValue::get_int_bytes_by_precision(target_precision);
  const arrow::Type::type arrow_type = base_builder.type()->id();
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(base_builder.AppendNull()))) {
        LOG_WARN("failed to append async result decimal null", K(ret), K(row_idx));
      }
    } else {
      const ObDecimalInt *source = nullptr;
      int32_t source_bytes = 0;
      ObScale source_scale = 0;
      ObDecimalIntBuilder number_value;
      ObDecimalIntBuilder scaled_value;
      if (ObDecimalIntType == expr.datum_meta_.type_) {
        source = vector.get_decimal_int(row_idx);
        source_bytes = vector.get_length(row_idx);
        source_scale = expr.datum_meta_.scale_;
      } else {
        const ObDatum datum(vector.get_payload(row_idx), vector.get_length(row_idx), false);
        const number::ObNumber &number = datum.get_number();
        ObDecimalInt *converted = nullptr;
        source_scale = number.get_scale();
        if (OB_FAIL(wide::from_number(
                number, number_value, source_scale, converted, source_bytes))) {
          LOG_WARN("failed to convert async result NUMBER to decimal integer",
                   K(ret), K(row_idx));
        } else {
          source = converted;
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(source)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async result decimal source is null", K(ret), K(row_idx));
      } else if (OB_SUCC(ret)
                 && ObDatumCast::need_scale_decimalint(
                        source_scale, source_bytes, target_scale, target_bytes)) {
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                source, source_bytes, source_scale, target_scale,
                target_precision, CM_NONE, scaled_value))) {
          LOG_WARN("failed to scale async result decimal", K(ret), K(row_idx));
        } else {
          source = scaled_value.get_decimal_int();
          source_bytes = scaled_value.get_int_bytes();
        }
      }
      if (OB_SUCC(ret) && arrow::Type::DECIMAL128 == arrow_type) {
        ret = append_decimal_value<arrow::Decimal128, arrow::Decimal128Builder>(
            static_cast<arrow::Decimal128Builder &>(base_builder), source, source_bytes);
      } else if (OB_SUCC(ret) && arrow::Type::DECIMAL256 == arrow_type) {
        ret = append_decimal_value<arrow::Decimal256, arrow::Decimal256Builder>(
            static_cast<arrow::Decimal256Builder &>(base_builder), source, source_bytes);
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid Arrow decimal builder in async result codec", K(ret), K(arrow_type));
      }
    }
  }
  return ret;
}

int append_serialized_obj(const ObExpr &expr,
                          const ObObj &obj,
                          arrow::LargeBinaryBuilder &builder,
                          ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t value_size = 0;
  int64_t pos = OPAQUE_HEADER_SIZE;
  char *buffer = nullptr;
  if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_get_serialize_size(obj, value_size))) {
    LOG_WARN("failed to get async result opaque value size", K(ret), K(obj.get_type()));
  } else if (value_size < 0 || value_size > INT32_MAX - OPAQUE_HEADER_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("async result opaque value is too large", K(ret), K(value_size));
  } else if (OB_ISNULL(buffer = static_cast<char *>(
                           allocator.alloc(value_size + OPAQUE_HEADER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate async result opaque value", K(ret), K(value_size));
  } else {
    MEMCPY(buffer, OPAQUE_MAGIC, sizeof(OPAQUE_MAGIC));
    buffer[sizeof(OPAQUE_MAGIC)] = static_cast<char>(OPAQUE_VALUE_VERSION);
    buffer[sizeof(OPAQUE_MAGIC) + 1] = static_cast<char>(expr.datum_meta_.type_);
    if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_serialize(
            obj, buffer, value_size + OPAQUE_HEADER_SIZE, pos))) {
      LOG_WARN("failed to serialize async result opaque value", K(ret), K(obj.get_type()));
    } else if (pos != value_size + OPAQUE_HEADER_SIZE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async result opaque value size changed during serialization",
               K(ret), K(pos), K(value_size));
    } else if (OB_FAIL(arrow_status_to_ret(builder.Append(
                   reinterpret_cast<const uint8_t *>(buffer), pos)))) {
      LOG_WARN("failed to append async result opaque value", K(ret), K(pos));
    }
  }
  return ret;
}

int append_opaque_scalar(const ObExpr &expr,
                         const ObIVector &vector,
                         const ObBatchRows &batch_rows,
                         arrow::ArrayBuilder &base_builder,
                         ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  arrow::LargeBinaryBuilder &builder =
      static_cast<arrow::LargeBinaryBuilder &>(base_builder);
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result opaque null", K(ret), K(row_idx));
      }
    } else {
      allocator.reuse();
      const ObDatum datum(vector.get_payload(row_idx), vector.get_length(row_idx), false);
      ObObj obj;
      if (OB_FAIL(datum.to_obj(obj, expr.obj_meta_))) {
        LOG_WARN("failed to construct async result opaque object",
                 K(ret), K(row_idx), K(expr.datum_meta_.type_));
      } else if (OB_FAIL(append_serialized_obj(expr, obj, builder, allocator))) {
        LOG_WARN("failed to append serialized async result object",
                 K(ret), K(row_idx), K(expr.datum_meta_.type_));
      }
    }
  }
  return ret;
}

int append_opaque_varlen(const ObExpr &expr,
                         const ObIVector &vector,
                         const ObBatchRows &batch_rows,
                         arrow::ArrayBuilder &base_builder,
                         ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  arrow::LargeBinaryBuilder &builder =
      static_cast<arrow::LargeBinaryBuilder &>(base_builder);
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_rows.size_; ++row_idx) {
    if (batch_rows.skip_->contain(row_idx)) {
    } else if (vector.is_null(row_idx)) {
      if (OB_FAIL(arrow_status_to_ret(builder.AppendNull()))) {
        LOG_WARN("failed to append async result opaque null", K(ret), K(row_idx));
      }
    } else {
      allocator.reuse();
      const ObDatum datum(vector.get_payload(row_idx), vector.get_length(row_idx), false);
      ObString value;
      ObObj obj;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              allocator, datum, expr.datum_meta_, value))) {
        LOG_WARN("failed to materialize async result opaque value",
                 K(ret), K(row_idx), K(expr.datum_meta_.type_));
      } else {
        obj.set_string(expr.datum_meta_.type_, value);
        obj.set_meta_type(expr.obj_meta_);
        if (OB_FAIL(append_serialized_obj(expr, obj, builder, allocator))) {
          LOG_WARN("failed to append serialized async result varlen object",
                   K(ret), K(row_idx), K(expr.datum_meta_.type_));
        }
      }
    }
  }
  return ret;
}

int decode_nulls(const arrow::Array &array,
                 ObEvalCtx &eval_ctx,
                 ObExpr &expr,
                 const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; row_idx < array.length(); ++row_idx) {
      vector->set_null(row_offset + row_idx);
    }
  }
  return ret;
}

#define DEFINE_ARROW_NUMERIC_TO_OB(func_name, array_type, setter)                            \
  int func_name(const arrow::Array &base_array,                                               \
                ObEvalCtx &eval_ctx,                                                          \
                ObExpr &expr,                                                                 \
                const int64_t row_offset)                                                     \
  {                                                                                            \
    int ret = OB_SUCCESS;                                                                      \
    ObIVector *vector = expr.get_vector(eval_ctx);                                             \
    if (OB_ISNULL(vector)) {                                                                    \
      ret = OB_ERR_UNEXPECTED;                                                                 \
      LOG_WARN("async result output vector is null", K(ret));                                \
    } else {                                                                                   \
      const array_type &array = static_cast<const array_type &>(base_array);                    \
      for (int64_t row_idx = 0; row_idx < array.length(); ++row_idx) {                         \
        if (array.IsNull(row_idx)) {                                                           \
          vector->set_null(row_offset + row_idx);                                              \
        } else {                                                                               \
          vector->setter(row_offset + row_idx, array.Value(row_idx));                          \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
    return ret;                                                                                \
  }

DEFINE_ARROW_NUMERIC_TO_OB(decode_int8, arrow::Int8Array, set_int)
DEFINE_ARROW_NUMERIC_TO_OB(decode_int16, arrow::Int16Array, set_int)
DEFINE_ARROW_NUMERIC_TO_OB(decode_int32, arrow::Int32Array, set_int)
DEFINE_ARROW_NUMERIC_TO_OB(decode_int64, arrow::Int64Array, set_int)
DEFINE_ARROW_NUMERIC_TO_OB(decode_uint8, arrow::UInt8Array, set_uint)
DEFINE_ARROW_NUMERIC_TO_OB(decode_uint16, arrow::UInt16Array, set_uint)
DEFINE_ARROW_NUMERIC_TO_OB(decode_uint32, arrow::UInt32Array, set_uint)
DEFINE_ARROW_NUMERIC_TO_OB(decode_uint64, arrow::UInt64Array, set_uint)
DEFINE_ARROW_NUMERIC_TO_OB(decode_float, arrow::FloatArray, set_float)
DEFINE_ARROW_NUMERIC_TO_OB(decode_double, arrow::DoubleArray, set_double)
DEFINE_ARROW_NUMERIC_TO_OB(decode_date, arrow::Date32Array, set_date)
DEFINE_ARROW_NUMERIC_TO_OB(decode_datetime, arrow::TimestampArray, set_datetime)
DEFINE_ARROW_NUMERIC_TO_OB(decode_timestamp, arrow::TimestampArray, set_timestamp)
DEFINE_ARROW_NUMERIC_TO_OB(decode_time, arrow::Int64Array, set_time)
DEFINE_ARROW_NUMERIC_TO_OB(decode_bit, arrow::UInt64Array, set_bit)
DEFINE_ARROW_NUMERIC_TO_OB(decode_set, arrow::UInt64Array, set_set)

#undef DEFINE_ARROW_NUMERIC_TO_OB

int decode_mediumint(const arrow::Array &base_array,
                     ObEvalCtx &eval_ctx,
                     ObExpr &expr,
                     const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::Int32Array &array =
      static_cast<const arrow::Int32Array &>(base_array);
  static const int32_t MIN_MEDIUMINT = -8388608;
  static const int32_t MAX_MEDIUMINT = 8388607;
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        const int32_t value = array.Value(row_idx);
        if (value < MIN_MEDIUMINT || value > MAX_MEDIUMINT) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("stored async result MEDIUMINT is out of range",
                   K(ret), K(row_idx), K(value));
        } else {
          vector->set_int(row_offset + row_idx, value);
        }
      }
    }
  }
  return ret;
}

int decode_umediumint(const arrow::Array &base_array,
                      ObEvalCtx &eval_ctx,
                      ObExpr &expr,
                      const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::UInt32Array &array =
      static_cast<const arrow::UInt32Array &>(base_array);
  static const uint32_t MAX_UMEDIUMINT = 16777215U;
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        const uint32_t value = array.Value(row_idx);
        if (value > MAX_UMEDIUMINT) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("stored async result unsigned MEDIUMINT is out of range",
                   K(ret), K(row_idx), K(value));
        } else {
          vector->set_uint(row_offset + row_idx, value);
        }
      }
    }
  }
  return ret;
}

int decode_year(const arrow::Array &base_array,
                ObEvalCtx &eval_ctx,
                ObExpr &expr,
                const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::UInt8Array &array =
      static_cast<const arrow::UInt8Array &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        const uint8_t value = array.Value(row_idx);
        vector->set_payload(row_offset + row_idx, &value, sizeof(value));
      }
    }
  }
  return ret;
}

int decode_legacy_sql_year(const arrow::Array &base_array,
                           ObEvalCtx &eval_ctx,
                           ObExpr &expr,
                           const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::Int16Array &array =
      static_cast<const arrow::Int16Array &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        const int16_t sql_year = array.Value(row_idx);
        if (0 != sql_year && (sql_year < 1901 || sql_year > 2155)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("stored async result YEAR is out of range",
                   K(ret), K(row_idx), K(sql_year));
        } else {
          const int8_t value = 0 == sql_year ? 0 : static_cast<int8_t>(sql_year - 1900);
          vector->set_year(row_offset + row_idx, value);
        }
      }
    }
  }
  return ret;
}

int set_owned_varlen_value(const uint8_t *value,
                           const int64_t length,
                           ObEvalCtx &eval_ctx,
                           ObExpr &expr,
                           ObIVector &vector,
                           const int64_t output_idx)
{
  int ret = OB_SUCCESS;
  char *owned_value = nullptr;
  static const uint8_t EMPTY_VALUE = 0;
  if (length < 0 || length > INT32_MAX
      || (length > 0 && OB_ISNULL(value))) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid async result variable-length value",
             K(ret), K(length), KP(value), K(output_idx));
  } else if (OB_UNLIKELY(!ObDynReserveBuf::supported(expr.datum_meta_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result type has no owned variable-length buffer",
             K(ret), K(expr.datum_meta_.type_), K(output_idx));
  } else if (VEC_CONTINUOUS == vector.get_format()) {
    // Continuous vectors own their data and copy synchronously.
    vector.set_payload(
        output_idx,
        length > 0 ? value : &EMPTY_VALUE,
        static_cast<ObLength>(length));
  } else if (OB_ISNULL(owned_value = expr.get_str_res_mem(
                 eval_ctx, length, output_idx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate async result variable-length output",
             K(ret), K(length), K(output_idx));
  } else {
    if (length > 0) {
      MEMCPY(owned_value, value, length);
    }
    // The vector points only to expression-result memory. The Arrow array may
    // be released immediately after this decoder returns.
    vector.set_payload_shallow(
        output_idx, owned_value, static_cast<ObLength>(length));
  }
  return ret;
}

template <typename ArrowType>
int decode_bytes(const arrow::Array &base_array,
                 ObEvalCtx &eval_ctx,
                 ObExpr &expr,
                 const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::BaseBinaryArray<ArrowType> &array =
      static_cast<const arrow::BaseBinaryArray<ArrowType> &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        typename ArrowType::offset_type length = 0;
        const uint8_t *value = array.GetValue(row_idx, &length);
        if (OB_FAIL(set_owned_varlen_value(
                value, length, eval_ctx, expr, *vector, row_offset + row_idx))) {
          LOG_WARN("failed to decode async result byte value",
                   K(ret), K(row_idx), K(length));
        }
      }
    }
  }
  return ret;
}

int decode_utf8(const arrow::Array &array,
                ObEvalCtx &eval_ctx,
                ObExpr &expr,
                const int64_t row_offset)
{
  return decode_bytes<arrow::StringType>(array, eval_ctx, expr, row_offset);
}

int decode_binary(const arrow::Array &array,
                  ObEvalCtx &eval_ctx,
                  ObExpr &expr,
                  const int64_t row_offset)
{
  return decode_bytes<arrow::BinaryType>(array, eval_ctx, expr, row_offset);
}

template <typename ArrowType>
int decode_text_bytes(const arrow::Array &base_array,
                      ObEvalCtx &eval_ctx,
                      ObExpr &expr,
                      const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::BaseBinaryArray<ArrowType> &array =
      static_cast<const arrow::BaseBinaryArray<ArrowType> &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        typename ArrowType::offset_type length = 0;
        const uint8_t *value = array.GetValue(row_idx, &length);
        if (length < 0 || length > INT32_MAX
            || (length > 0 && OB_ISNULL(value))) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid async result text value",
                   K(ret), K(row_idx), K(length), KP(value));
        } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(
                expr,
                eval_ctx,
                ObString(static_cast<int32_t>(length),
                         reinterpret_cast<const char *>(value)),
                row_offset + row_idx))) {
          LOG_WARN("failed to reconstruct async result text value",
                   K(ret), K(row_idx), K(expr.datum_meta_.type_));
        }
      }
    }
  }
  return ret;
}

int decode_large_utf8_text(const arrow::Array &array,
                           ObEvalCtx &eval_ctx,
                           ObExpr &expr,
                           const int64_t row_offset)
{
  return decode_text_bytes<arrow::LargeStringType>(array, eval_ctx, expr, row_offset);
}

int decode_large_binary_text(const arrow::Array &array,
                             ObEvalCtx &eval_ctx,
                             ObExpr &expr,
                             const int64_t row_offset)
{
  return decode_text_bytes<arrow::LargeBinaryType>(array, eval_ctx, expr, row_offset);
}

int decode_collection_binary(const arrow::Array &base_array,
                             ObEvalCtx &eval_ctx,
                             ObExpr &expr,
                             const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::LargeBinaryArray &array =
      static_cast<const arrow::LargeBinaryArray &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result collection output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        int64_t payload_len = 0;
        const uint8_t *payload = array.GetValue(row_idx, &payload_len);
        const int64_t output_idx = row_offset + row_idx;
        char *result_buf = nullptr;
        int64_t result_buf_len = 0;
        ObTextStringDatumResult collection_result(
            expr.datum_meta_.type_, &expr, &eval_ctx, vector, output_idx);
        if (payload_len < 0 || payload_len > INT32_MAX
            || (payload_len > 0 && OB_ISNULL(payload))) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid async result collection payload",
                   K(ret), K(row_idx), K(payload_len), KP(payload));
        } else if (OB_FAIL(collection_result.init_with_batch_idx(
                       payload_len, output_idx))) {
          LOG_WARN("failed to allocate async result collection output",
                   K(ret), K(row_idx), K(payload_len));
        } else if (OB_FAIL(collection_result.get_reserved_buffer(
                       result_buf, result_buf_len))) {
          LOG_WARN("failed to get async result collection output buffer",
                   K(ret), K(row_idx), K(payload_len));
        } else if (result_buf_len < payload_len
                   || (payload_len > 0 && OB_ISNULL(result_buf))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async result collection output buffer",
                   K(ret), K(row_idx), K(payload_len), K(result_buf_len));
        } else {
          if (payload_len > 0) {
            MEMCPY(result_buf, payload, payload_len);
          }
          if (OB_FAIL(collection_result.lseek(payload_len, 0))) {
            LOG_WARN("failed to finish async result collection output",
                     K(ret), K(row_idx), K(payload_len));
          } else {
            // set_result installs the eval-context-owned row buffer into the
            // vector. No pointer into the local Arrow array is retained.
            collection_result.set_result();
          }
        }
      }
    }
  }
  return ret;
}

int decode_decimal(const arrow::Array &base_array,
                   ObEvalCtx &eval_ctx,
                   ObExpr &expr,
                   const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::FixedSizeBinaryArray &array =
      static_cast<const arrow::FixedSizeBinaryArray &>(base_array);
  const arrow::DecimalType &decimal_type =
      static_cast<const arrow::DecimalType &>(*base_array.type());
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  if (OB_ISNULL(vector)
      || decimal_type.precision() != expr.datum_meta_.precision_
      || decimal_type.scale() != expr.datum_meta_.scale_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result decimal output", K(ret), KP(vector),
             "arrow_precision", decimal_type.precision(),
             "arrow_scale", decimal_type.scale(), K(expr.datum_meta_));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        ObDecimalIntBuilder decimal_value;
        if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                reinterpret_cast<ObDecimalInt *>(
                    const_cast<uint8_t *>(array.Value(row_idx))),
                decimal_type.byte_width(),
                decimal_type.scale(),
                expr.datum_meta_.scale_,
                expr.datum_meta_.precision_,
                CM_NONE,
                decimal_value))) {
          LOG_WARN("failed to decode async result Arrow decimal", K(ret), K(row_idx));
        } else if (ObDecimalIntType == expr.datum_meta_.type_) {
          vector->set_decimal_int(row_offset + row_idx,
                                  decimal_value.get_decimal_int(),
                                  decimal_value.get_int_bytes());
        } else {
          number::ObNumber number;
          if (OB_FAIL(wide::to_number(decimal_value.get_decimal_int(),
                                      decimal_value.get_int_bytes(),
                                      expr.datum_meta_.scale_,
                                      tmp_alloc_guard.get_allocator(),
                                      number))) {
            LOG_WARN("failed to reconstruct async result NUMBER", K(ret), K(row_idx));
          } else {
            vector->set_number(row_offset + row_idx, number);
          }
        }
      }
    }
  }
  return ret;
}

int decode_opaque(const arrow::Array &base_array,
                  ObEvalCtx &eval_ctx,
                  ObExpr &expr,
                  const int64_t row_offset)
{
  int ret = OB_SUCCESS;
  ObIVector *vector = expr.get_vector(eval_ctx);
  const arrow::LargeBinaryArray &array =
      static_cast<const arrow::LargeBinaryArray &>(base_array);
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result output vector is null", K(ret));
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < array.length(); ++row_idx) {
      if (array.IsNull(row_idx)) {
        vector->set_null(row_offset + row_idx);
      } else {
        int64_t length = 0;
        const uint8_t *value = array.GetValue(row_idx, &length);
        if (length < OPAQUE_HEADER_SIZE || OB_ISNULL(value)
            || 0 != MEMCMP(value, OPAQUE_MAGIC, sizeof(OPAQUE_MAGIC))
            || OPAQUE_VALUE_VERSION != value[sizeof(OPAQUE_MAGIC)]
            || static_cast<uint8_t>(expr.datum_meta_.type_)
                   != value[sizeof(OPAQUE_MAGIC) + 1]) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid async result opaque value header",
                   K(ret), K(row_idx), K(length), K(expr.datum_meta_.type_));
        } else {
          ObObj obj;
          int64_t pos = OPAQUE_HEADER_SIZE;
          obj.set_meta_type(expr.obj_meta_);
          if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(
                  obj,
                  reinterpret_cast<const char *>(value),
                  length,
                  pos))) {
            LOG_WARN("failed to deserialize async result opaque value",
                     K(ret), K(row_idx), K(expr.datum_meta_.type_));
          } else if (pos != length) {
            ret = OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("async result opaque value has trailing bytes",
                     K(ret), K(row_idx), K(pos), K(length));
          } else if (is_lob_storage(expr.datum_meta_.type_)) {
            const ObString payload = obj.get_string();
            if (payload.length() > 0 && OB_ISNULL(payload.ptr())) {
              ret = OB_INVALID_EXTERNAL_FILE;
              LOG_WARN("invalid decoded async result opaque LOB payload",
                       K(ret), K(row_idx), "payload_length", payload.length(),
                       K(expr.datum_meta_.type_));
            } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(
                           expr, eval_ctx, payload, row_offset + row_idx))) {
              LOG_WARN("failed to reconstruct async result opaque LOB value",
                       K(ret), K(row_idx), "payload_length", payload.length(),
                       K(expr.datum_meta_.type_));
            }
          } else {
            alignas(16) char datum_buffer[OBJ_DATUM_MAX_RES_SIZE] = {0};
            ObDatum datum;
            datum.ptr_ = datum_buffer;
            if (OB_FAIL(datum.from_obj(
                    obj, ObDatum::get_obj_datum_map_type(expr.datum_meta_.type_)))) {
              LOG_WARN("failed to convert async result opaque object to datum",
                       K(ret), K(row_idx), K(expr.datum_meta_.type_));
            } else if (datum.len_ > 0 && OB_ISNULL(datum.ptr_)) {
              ret = OB_INVALID_EXTERNAL_FILE;
              LOG_WARN("invalid decoded async result opaque datum",
                       K(ret), K(row_idx), K(datum.len_));
            } else if (ObDynReserveBuf::supported(expr.datum_meta_.type_)) {
              if (OB_FAIL(set_owned_varlen_value(
                      reinterpret_cast<const uint8_t *>(datum.ptr_),
                      datum.len_, eval_ctx, expr, *vector,
                      row_offset + row_idx))) {
                LOG_WARN("failed to retain decoded async result opaque datum",
                         K(ret), K(row_idx), K(datum.len_));
              }
            } else {
              vector->set_payload(row_offset + row_idx, datum.ptr_, datum.len_);
            }
          }
        }
      }
    }
  }
  return ret;
}

void configure_mapping(MappingSpec &mapping,
                       const CarrierKind carrier,
                       const char *encoding,
                       const parquet::Type::type parquet_physical,
                       const std::shared_ptr<arrow::DataType> &arrow_type,
                       const char *parquet_logical,
                       const ObToArrowFunc ob_to_arrow_func,
                       const ObToArrowZeroCopyFunc zero_copy_func,
                       const ParquetToArrowFunc parquet_to_arrow_func,
                       const ArrowToObFunc arrow_to_ob_func)
{
  mapping.carrier_ = carrier;
  mapping.encoding_ = encoding;
  mapping.parquet_physical_ = parquet_physical;
  mapping.arrow_type_ = arrow_type;
  mapping.arrow_type_name_ = OB_NOT_NULL(arrow_type) ? arrow_type->ToString() : "";
  mapping.parquet_logical_ = OB_NOT_NULL(parquet_logical) ? parquet_logical : "";
  mapping.ob_to_arrow_func_ = ob_to_arrow_func;
  mapping.ob_to_arrow_eval_func_ = nullptr;
  mapping.zero_copy_func_ = zero_copy_func;
  mapping.parquet_to_arrow_func_ = parquet_to_arrow_func;
  mapping.arrow_to_ob_func_ = arrow_to_ob_func;
}

bool is_arrow_utf8_charset(const ObCollationType collation)
{
  const ObCharsetType charset = ObCharset::charset_type_by_coll(collation);
  return CHARSET_UTF8MB4 == charset || CHARSET_ASCII == charset;
}

bool has_native_decimal_accuracy(const ObObjType type,
                                 const int32_t precision,
                                 const int32_t scale)
{
  const bool is_decimal_type = ObNumberType == type
                            || ObUNumberType == type
                            || ObDecimalIntType == type;
  return is_decimal_type
      && precision > 0
      && precision <= arrow::Decimal256Type::kMaxPrecision
      && scale != NUMBER_SCALE_UNKNOWN_YET
      && scale >= -precision
      && scale <= precision;
}

int select_v2_mapping(const ObObjType type,
                      const int32_t precision,
                      const int32_t scale,
                      const ObCollationType collation,
                      MappingSpec &mapping)
{
  int ret = OB_SUCCESS;
  mapping = MappingSpec();
  switch (type) {
    case ObNullType:
      configure_mapping(mapping, CarrierKind::NULLS, ENCODING_OPAQUE,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        append_nulls, nullptr, build_large_binary_array, decode_nulls);
      break;
    case ObTinyIntType:
      configure_mapping(mapping, CarrierKind::INT8, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int8(), "INT(8,true)",
                        append_tinyint, build_zero_copy_fixed_array<sizeof(int8_t)>,
                        build_int8_array, decode_int8);
      break;
    case ObSmallIntType:
      configure_mapping(mapping, CarrierKind::INT16, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int16(), "INT(16,true)",
                        append_smallint, build_zero_copy_fixed_array<sizeof(int16_t)>,
                        build_int16_array, decode_int16);
      break;
    case ObMediumIntType:
      configure_mapping(mapping, CarrierKind::INT32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int32(), "INT(32,true)",
                        append_mediumint, build_zero_copy_fixed_array<sizeof(int32_t)>,
                        build_int32_array, decode_mediumint);
      break;
    case ObInt32Type:
      configure_mapping(mapping, CarrierKind::INT32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int32(), "INT(32,true)",
                        append_int32, build_zero_copy_fixed_array<sizeof(int32_t)>,
                        build_int32_array, decode_int32);
      break;
    case ObIntType:
      configure_mapping(mapping, CarrierKind::INT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::int64(), "INT(64,true)",
                        append_int64, build_zero_copy_fixed_array<sizeof(int64_t)>,
                        build_int64_array, decode_int64);
      break;
    case ObUTinyIntType:
      configure_mapping(mapping, CarrierKind::UINT8, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::uint8(), "INT(8,false)",
                        append_utinyint, build_zero_copy_fixed_array<sizeof(uint8_t)>,
                        build_uint8_array, decode_uint8);
      break;
    case ObUSmallIntType:
      configure_mapping(mapping, CarrierKind::UINT16, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::uint16(), "INT(16,false)",
                        append_usmallint, build_zero_copy_fixed_array<sizeof(uint16_t)>,
                        build_uint16_array, decode_uint16);
      break;
    case ObUMediumIntType:
      configure_mapping(mapping, CarrierKind::UINT32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::uint32(), "INT(32,false)",
                        append_umediumint, build_zero_copy_fixed_array<sizeof(uint32_t)>,
                        build_uint32_array, decode_umediumint);
      break;
    case ObUInt32Type:
      configure_mapping(mapping, CarrierKind::UINT32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::uint32(), "INT(32,false)",
                        append_uint32, build_zero_copy_fixed_array<sizeof(uint32_t)>,
                        build_uint32_array, decode_uint32);
      break;
    case ObUInt64Type:
      configure_mapping(mapping, CarrierKind::UINT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::uint64(), "INT(64,false)",
                        append_uint64, build_zero_copy_fixed_array<sizeof(uint64_t)>,
                        build_uint64_array, decode_uint64);
      break;
    case ObFloatType:
    case ObUFloatType:
      configure_mapping(mapping, CarrierKind::FLOAT32, ENCODING_NATIVE,
                        parquet::Type::FLOAT, arrow::float32(), nullptr,
                        append_float, build_zero_copy_fixed_array<sizeof(float)>,
                        build_float_array, decode_float);
      break;
    case ObDoubleType:
    case ObUDoubleType:
      configure_mapping(mapping, CarrierKind::FLOAT64, ENCODING_NATIVE,
                        parquet::Type::DOUBLE, arrow::float64(), nullptr,
                        append_double, build_zero_copy_fixed_array<sizeof(double)>,
                        build_double_array, decode_double);
      break;
    case ObNumberType:
    case ObUNumberType:
    case ObDecimalIntType:
      if (has_native_decimal_accuracy(type, precision, scale)) {
        char logical_type[64] = {0};
        const int print_len = snprintf(logical_type, sizeof(logical_type),
                                       "DECIMAL(%d,%d)", precision, scale);
        if (print_len <= 0 || print_len >= static_cast<int>(sizeof(logical_type))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to format async result decimal mapping", K(ret),
                   K(precision), K(scale));
        } else if (precision <= arrow::Decimal128Type::kMaxPrecision) {
          configure_mapping(mapping, CarrierKind::DECIMAL128, ENCODING_NATIVE,
                            parquet::Type::FIXED_LEN_BYTE_ARRAY,
                            arrow::decimal128(precision, scale), logical_type,
                            append_decimal, nullptr, build_decimal128_array, decode_decimal);
        } else {
          configure_mapping(mapping, CarrierKind::DECIMAL256, ENCODING_NATIVE,
                            parquet::Type::FIXED_LEN_BYTE_ARRAY,
                            arrow::decimal256(precision, scale), logical_type,
                            append_decimal, nullptr, build_decimal256_array, decode_decimal);
        }
      } else {
        configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_OPAQUE,
                          parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                          append_opaque_scalar, nullptr,
                          build_large_binary_array, decode_opaque);
      }
      break;
    case ObNumberFloatType:
    case ObMySQLDateType:
    case ObMySQLDateTimeType:
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObIntervalYMType:
    case ObIntervalDSType:
      configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_OPAQUE,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        append_opaque_scalar, nullptr,
                        build_large_binary_array, decode_opaque);
      break;
    case ObEnumType:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ENUM is not supported by async result Arrow codec", K(ret));
      break;
    case ObDateTimeType:
      configure_mapping(mapping, CarrierKind::TIMESTAMP_NTZ_US, ENCODING_NATIVE,
                        parquet::Type::INT64,
                        arrow::timestamp(arrow::TimeUnit::MICRO),
                        "TIMESTAMP(MICROS,false)", append_datetime,
                        build_zero_copy_fixed_array<sizeof(int64_t)>,
                        build_timestamp_array, decode_datetime);
      break;
    case ObTimestampType:
      configure_mapping(mapping, CarrierKind::TIMESTAMP_UTC_US, ENCODING_NATIVE,
                        parquet::Type::INT64,
                        arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                        "TIMESTAMP(MICROS,true)", append_timestamp,
                        build_zero_copy_fixed_array<sizeof(int64_t)>,
                        build_timestamp_array, decode_timestamp);
      break;
    case ObDateType:
      configure_mapping(mapping, CarrierKind::DATE32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::date32(), "DATE",
                        append_date, build_zero_copy_fixed_array<sizeof(int32_t)>,
                        build_date32_array, decode_date);
      break;
    case ObTimeType:
      configure_mapping(mapping, CarrierKind::INT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::int64(), nullptr,
                        append_time, build_zero_copy_fixed_array<sizeof(int64_t)>,
                        build_int64_array, decode_time);
      break;
    case ObYearType:
      configure_mapping(mapping, CarrierKind::UINT8, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::uint8(), "INT(8,false)",
                        append_year, build_zero_copy_fixed_array<sizeof(uint8_t)>,
                        build_uint8_array, decode_year);
      break;
    case ObBitType:
      configure_mapping(mapping, CarrierKind::UINT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::uint64(), "INT(64,false)",
                        append_bit, build_zero_copy_fixed_array<sizeof(uint64_t)>,
                        build_uint64_array, decode_bit);
      break;
    case ObSetType:
      configure_mapping(mapping, CarrierKind::UINT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::uint64(), "INT(64,false)",
                        append_set, build_zero_copy_fixed_array<sizeof(uint64_t)>,
                        build_uint64_array, decode_set);
      break;
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
      // TODO: Borrow VEC_CONTINUOUS offsets and inline bytes for unskipped
      // batches when Arrow bounds, null bits, and buffer lifetime are valid.
      // Values requiring LOB materialization still need the builder path.
      if (is_arrow_utf8_charset(collation)) {
        configure_mapping(mapping, CarrierKind::UTF8, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::utf8(), "STRING",
                          append_utf8, nullptr, build_utf8_array, decode_utf8);
      } else {
        configure_mapping(mapping, CarrierKind::BINARY, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::binary(), nullptr,
                          append_binary, nullptr, build_binary_array, decode_binary);
      }
      break;
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
      if (is_arrow_utf8_charset(collation)) {
        configure_mapping(mapping, CarrierKind::LARGE_UTF8, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::large_utf8(), "STRING",
                          append_large_utf8, nullptr,
                          build_large_utf8_array, decode_large_utf8_text);
      } else {
        configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                          append_large_binary, nullptr,
                          build_large_binary_array, decode_large_binary_text);
      }
      break;
    case ObRawType:
    case ObHexStringType:
      configure_mapping(mapping, CarrierKind::BINARY, ENCODING_DIRECT_BYTES,
                        parquet::Type::BYTE_ARRAY, arrow::binary(), nullptr,
                        append_binary, nullptr, build_binary_array, decode_binary);
      break;
    case ObLobType:
      configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_DIRECT_BYTES,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        append_large_binary, nullptr,
                        build_large_binary_array, decode_large_binary_text);
      break;
    case ObURowIDType:
      configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_OPAQUE,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        append_opaque_scalar, nullptr,
                        build_large_binary_array, decode_opaque);
      break;
    case ObJsonType:
    case ObGeometryType:
    case ObRoaringBitmapType:
      configure_mapping(mapping, CarrierKind::LARGE_BINARY, ENCODING_OPAQUE,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        append_opaque_varlen, nullptr,
                        build_large_binary_array, decode_opaque);
      break;
    case ObCollectionSQLType:
      configure_mapping(mapping, CarrierKind::LARGE_BINARY,
                        ENCODING_COLLECTION_BINARY,
                        parquet::Type::BYTE_ARRAY, arrow::large_binary(), nullptr,
                        nullptr, nullptr,
                        build_large_binary_array, decode_collection_binary);
      mapping.ob_to_arrow_eval_func_ = append_collection_binary;
      break;
    case ObUserDefinedSQLType:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("user-defined SQL type is not supported by async result Arrow codec",
               K(ret));
      break;
    case ObExtendType:
    case ObUnknownType:
    case ObEnumInnerType:
    case ObSetInnerType:
    case ObMaxType:
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported OB type in async result Arrow codec", K(ret), K(type));
      break;
  }
  return ret;
}

int select_stored_v2_mapping(const ObObjType type,
                             const int32_t precision,
                             const int32_t scale,
                             const ObCollationType collation,
                             const bool legacy_sql_year,
                             MappingSpec &mapping)
{
  int ret = OB_SUCCESS;
  if (legacy_sql_year) {
    if (ObYearType != type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("legacy SQL YEAR mapping requested for another type", K(ret), K(type));
    } else {
      // Older v2 results stored the displayed SQL year as int16. New results
      // store the OB year byte directly as uint8.
      configure_mapping(mapping, CarrierKind::INT16, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int16(), "INT(16,true)",
                        nullptr, nullptr, build_int16_array, decode_legacy_sql_year);
    }
  } else if (OB_FAIL(select_v2_mapping(type, precision, scale, collation, mapping))) {
    LOG_WARN("failed to select stored async result mapping", K(ret), K(type));
  }
  return ret;
}

int select_v1_mapping(const ObObjType type,
                      const ObCollationType collation,
                      MappingSpec &mapping)
{
  int ret = OB_SUCCESS;
  mapping = MappingSpec();
  switch (type) {
    case ObTinyIntType:
      configure_mapping(mapping, CarrierKind::INT8, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int8(), "INT(8,true)",
                        append_tinyint, build_zero_copy_fixed_array<sizeof(int8_t)>,
                        build_int8_array, decode_int8);
      break;
    case ObSmallIntType:
      configure_mapping(mapping, CarrierKind::INT16, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int16(), "INT(16,true)",
                        append_smallint, build_zero_copy_fixed_array<sizeof(int16_t)>,
                        build_int16_array, decode_int16);
      break;
    case ObInt32Type:
      configure_mapping(mapping, CarrierKind::INT32, ENCODING_NATIVE,
                        parquet::Type::INT32, arrow::int32(), "INT(32,true)",
                        append_int32, build_zero_copy_fixed_array<sizeof(int32_t)>,
                        build_int32_array, decode_int32);
      break;
    case ObIntType:
      configure_mapping(mapping, CarrierKind::INT64, ENCODING_NATIVE,
                        parquet::Type::INT64, arrow::int64(), "INT(64,true)",
                        append_int64, build_zero_copy_fixed_array<sizeof(int64_t)>,
                        build_int64_array, decode_int64);
      break;
    case ObFloatType:
      configure_mapping(mapping, CarrierKind::FLOAT32, ENCODING_NATIVE,
                        parquet::Type::FLOAT, arrow::float32(), nullptr,
                        append_float, build_zero_copy_fixed_array<sizeof(float)>,
                        build_float_array, decode_float);
      break;
    case ObDoubleType:
      configure_mapping(mapping, CarrierKind::FLOAT64, ENCODING_NATIVE,
                        parquet::Type::DOUBLE, arrow::float64(), nullptr,
                        append_double, build_zero_copy_fixed_array<sizeof(double)>,
                        build_double_array, decode_double);
      break;
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
      if (CS_TYPE_BINARY == collation) {
        configure_mapping(mapping, CarrierKind::BINARY, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::binary(), nullptr,
                          append_binary, nullptr, build_binary_array, decode_binary);
      } else {
        configure_mapping(mapping, CarrierKind::UTF8, ENCODING_DIRECT_BYTES,
                          parquet::Type::BYTE_ARRAY, arrow::utf8(), "STRING",
                          append_utf8, nullptr, build_utf8_array, decode_utf8);
      }
      break;
    case ObRawType:
      configure_mapping(mapping, CarrierKind::BINARY, ENCODING_DIRECT_BYTES,
                        parquet::Type::BYTE_ARRAY, arrow::binary(), nullptr,
                        append_binary, nullptr, build_binary_array, decode_binary);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  return ret;
}

ObAsyncResultParquetValueBatch::PhysicalKind get_physical_kind(
    const parquet::Type::type physical_type)
{
  ObAsyncResultParquetValueBatch::PhysicalKind kind =
      ObAsyncResultParquetValueBatch::PhysicalKind::INVALID;
  switch (physical_type) {
    case parquet::Type::INT32:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::INT32;
      break;
    case parquet::Type::INT64:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::INT64;
      break;
    case parquet::Type::FLOAT:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::FLOAT;
      break;
    case parquet::Type::DOUBLE:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::DOUBLE;
      break;
    case parquet::Type::BYTE_ARRAY:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::BYTE_ARRAY;
      break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      kind = ObAsyncResultParquetValueBatch::PhysicalKind::FIXED_LEN_BYTE_ARRAY;
      break;
    default:
      break;
  }
  return kind;
}

const char *get_parquet_physical_name(const parquet::Type::type physical_type)
{
  const char *name = nullptr;
  switch (physical_type) {
    case parquet::Type::INT32:
      name = "INT32";
      break;
    case parquet::Type::INT64:
      name = "INT64";
      break;
    case parquet::Type::FLOAT:
      name = "FLOAT";
      break;
    case parquet::Type::DOUBLE:
      name = "DOUBLE";
      break;
    case parquet::Type::BYTE_ARRAY:
      name = "BYTE_ARRAY";
      break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      name = "FIXED_LEN_BYTE_ARRAY";
      break;
    default:
      break;
  }
  return name;
}

int get_codec_version(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata,
                      int64_t &codec_version)
{
  int ret = OB_SUCCESS;
  const std::string *codec = nullptr;
  codec_version = 0;
  if (OB_FAIL(get_metadata_value(metadata, CODEC_KEY, codec))) {
    LOG_WARN("failed to get async result codec version", K(ret));
  } else if (CODEC_V1 == *codec) {
    codec_version = 1;
  } else if (CODEC_V2 == *codec) {
    codec_version = 2;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result Arrow codec",
             K(ret), "codec", codec->c_str());
  }
  return ret;
}

struct StoredFieldDescriptor
{
  StoredFieldDescriptor()
      : field_id_(0), ob_type_id_(ObNullType), mapping_version_(0),
        precision_(-1), scale_(-1), collation_id_(CS_TYPE_INVALID),
        collation_level_(CS_LEVEL_INVALID), length_(-1), length_semantics_(-1),
        encoding_(nullptr)
  {}

  int64_t field_id_;
  int64_t ob_type_id_;
  int64_t mapping_version_;
  int64_t precision_;
  int64_t scale_;
  int64_t collation_id_;
  int64_t collation_level_;
  int64_t length_;
  int64_t length_semantics_;
  const std::string *encoding_;
};

int parse_stored_field_descriptor(const arrow::Field &field,
                                  const int64_t codec_version,
                                  StoredFieldDescriptor &descriptor)
{
  int ret = OB_SUCCESS;
  descriptor = StoredFieldDescriptor();
  if (OB_ISNULL(field.type())) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("async result Arrow field has no type",
             K(ret), "field", field.name().c_str());
  } else if (!field.nullable()) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("async result Arrow field must be nullable",
             K(ret), "field", field.name().c_str());
  } else if (OB_FAIL(parse_metadata_int64(
                 field.metadata(), FIELD_ID_KEY, descriptor.field_id_))) {
    LOG_WARN("failed to parse async result field id",
             K(ret), "field", field.name().c_str());
  } else if (descriptor.field_id_ <= 0 || descriptor.field_id_ > INT32_MAX) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid async result field id",
             K(ret), K(descriptor.field_id_), "field", field.name().c_str());
  } else if (OB_FAIL(parse_metadata_int64(
                 field.metadata(), OB_TYPE_ID_KEY, descriptor.ob_type_id_))) {
    LOG_WARN("failed to parse async result OB type id",
             K(ret), "field", field.name().c_str());
  } else if (descriptor.ob_type_id_ < ObNullType
             || descriptor.ob_type_id_ >= ObMaxType) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("invalid async result OB type id",
             K(ret), K(descriptor.ob_type_id_), "field", field.name().c_str());
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), MAPPING_VERSION_KEY,
                    descriptor.mapping_version_))) {
    LOG_WARN("failed to parse async result mapping version",
             K(ret), "field", field.name().c_str());
  } else if (2 == codec_version
             && descriptor.mapping_version_ !=
                    ObAsyncResultArrowCodec::CURRENT_CODEC_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result mapping version",
             K(ret), K(descriptor.mapping_version_), "field", field.name().c_str());
  } else if (2 == codec_version
             && OB_FAIL(get_metadata_value(
                    field.metadata(), ENCODING_KEY, descriptor.encoding_))) {
    LOG_WARN("failed to get async result encoding",
             K(ret), "field", field.name().c_str());
  } else if (2 == codec_version
             && *descriptor.encoding_ != ENCODING_NATIVE
             && *descriptor.encoding_ != ENCODING_DIRECT_BYTES
             && *descriptor.encoding_ != ENCODING_OPAQUE
             && *descriptor.encoding_ != ENCODING_COLLECTION_BINARY) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result value encoding",
             K(ret), "encoding", descriptor.encoding_->c_str(),
             "field", field.name().c_str());
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), PRECISION_KEY, descriptor.precision_))) {
    LOG_WARN("failed to parse async result precision", K(ret));
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), SCALE_KEY, descriptor.scale_))) {
    LOG_WARN("failed to parse async result scale", K(ret));
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), COLLATION_ID_KEY, descriptor.collation_id_))) {
    LOG_WARN("failed to parse async result collation", K(ret));
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), COLLATION_LEVEL_KEY,
                    descriptor.collation_level_))) {
    LOG_WARN("failed to parse async result collation level", K(ret));
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), LENGTH_KEY, descriptor.length_))) {
    LOG_WARN("failed to parse async result length", K(ret));
  } else if (2 == codec_version
             && OB_FAIL(parse_metadata_int64(
                    field.metadata(), LENGTH_SEMANTICS_KEY,
                    descriptor.length_semantics_))) {
    LOG_WARN("failed to parse async result length semantics", K(ret));
  }
  return ret;
}

int validate_field_codec(const arrow::Field &field,
                         const int64_t schema_codec_version,
                         StoredFieldDescriptor &descriptor)
{
  int ret = OB_SUCCESS;
  int64_t field_codec_version = 0;
  if (OB_FAIL(get_codec_version(field.metadata(), field_codec_version))) {
    LOG_WARN("failed to get async result field codec",
             K(ret), "field", field.name().c_str());
  } else if (field_codec_version != schema_codec_version) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("async result schema and field codecs differ",
             K(ret), K(schema_codec_version), K(field_codec_version),
             "field", field.name().c_str());
  } else if (OB_FAIL(parse_stored_field_descriptor(
                 field, field_codec_version, descriptor))) {
    LOG_WARN("invalid async result Arrow field descriptor",
             K(ret), "field", field.name().c_str());
  }
  return ret;
}

const char *get_v1_arrow_type_name(const ObObjType type,
                                   const ObCollationType collation)
{
  const char *name = nullptr;
  switch (type) {
    case ObTinyIntType:
      name = "int8";
      break;
    case ObSmallIntType:
      name = "int16";
      break;
    case ObInt32Type:
      name = "int32";
      break;
    case ObIntType:
      name = "int64";
      break;
    case ObFloatType:
      name = "float32";
      break;
    case ObDoubleType:
      name = "float64";
      break;
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType:
      name = CS_TYPE_BINARY == collation ? "binary" : "utf8";
      break;
    case ObRawType:
      name = "binary";
      break;
    default:
      break;
  }
  return name;
}

} // namespace

ObAsyncResultArrowReadPlan::ObAsyncResultArrowReadPlan()
    : arrow_type_(),
      physical_kind_(ObAsyncResultParquetValueBatch::PhysicalKind::INVALID),
      parquet_to_arrow_func_(nullptr),
      arrow_to_ob_func_(nullptr)
{
}

bool ObAsyncResultArrowReadPlan::is_valid() const
{
  return OB_NOT_NULL(arrow_type_)
      && ObAsyncResultParquetValueBatch::PhysicalKind::INVALID != physical_kind_
      && OB_NOT_NULL(parquet_to_arrow_func_)
      && OB_NOT_NULL(arrow_to_ob_func_);
}

int ObAsyncResultArrowReadPlan::decode_parquet_batch(
    const ObAsyncResultParquetValueBatch &batch,
    arrow::MemoryPool *memory_pool,
    ObEvalCtx &eval_ctx,
    ObExpr &expr,
    const int64_t row_offset) const
{
  int ret = OB_SUCCESS;
  std::shared_ptr<arrow::Array> array;
  if (!is_valid() || OB_ISNULL(memory_pool) || row_offset < 0
      || batch.kind_ != physical_kind_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result read plan invocation", K(ret),
             K(row_offset), K(batch.kind_), K(physical_kind_),
             KP(memory_pool));
  } else {
    try {
      if (OB_FAIL(parquet_to_arrow_func_(
              batch, arrow_type_, memory_pool, array))) {
        LOG_WARN("failed to reconstruct async result Arrow array", K(ret));
      } else if (OB_ISNULL(array)
                 || array->length() != batch.row_count_
                 || !array->type()->Equals(arrow_type_)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid reconstructed async result Arrow array",
                 K(ret), KP(array.get()), K(batch.row_count_));
      } else if (OB_FAIL(arrow_to_ob_func_(*array, eval_ctx, expr, row_offset))) {
        LOG_WARN("failed to decode async result Arrow array",
                 K(ret), K(expr.datum_meta_.type_));
      }
    } catch (const std::bad_alloc &ex) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("out of memory while decoding async result Arrow batch",
               K(ret), "info", ex.what());
    } catch (const std::exception &ex) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("caught exception while decoding async result Arrow batch",
               K(ret), "info", ex.what());
    } catch (...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught unknown exception while decoding async result Arrow batch", K(ret));
    }
  }
  return ret;
}

ObAsyncResultArrowWritePlan::ObAsyncResultArrowWritePlan()
    : schema_(), columns_()
{
  columns_.set_attr(ObMemAttr(MTL_ID(), "ResultPlan"));
}

void ObAsyncResultArrowWritePlan::reset()
{
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "ResultPlan"));
  schema_.reset();
  columns_.reset();
}

int ObAsyncResultArrowWritePlan::build_batch(
    ObEvalCtx &eval_ctx,
    const ObBatchRows &batch_rows,
    arrow::MemoryPool *memory_pool,
    std::shared_ptr<arrow::RecordBatch> &record_batch) const
{
  int ret = OB_SUCCESS;
  int64_t active_row_count = 0;
  record_batch.reset();
  if (OB_ISNULL(schema_) || columns_.count() != schema_->num_fields()
      || batch_rows.size_ < 0 || OB_ISNULL(batch_rows.skip_)
      || OB_ISNULL(memory_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result write plan invocation", K(ret),
             KP(schema_.get()), "column_count", columns_.count(),
             K(batch_rows.size_), KP(batch_rows.skip_), KP(memory_pool));
  } else {
    for (int64_t row_idx = 0; row_idx < batch_rows.size_; ++row_idx) {
      active_row_count += batch_rows.skip_->contain(row_idx) ? 0 : 1;
    }
    try {
      ObMallocHookAttrGuard guard(
          ObMemAttr(MTL_ID(), "ResultArrow"), false /* use_500 */);
      std::vector<std::shared_ptr<arrow::Array>> arrays;
      arrays.reserve(columns_.count());
      ObArenaAllocator value_allocator(
          "ResultArrow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < columns_.count(); ++col_idx) {
        const ColumnPlan &column = columns_.at(col_idx);
        ObIVector *vector = nullptr;
        std::shared_ptr<arrow::Array> array;
        bool zero_copy_used = false;
        if (OB_ISNULL(column.expr_)
            || OB_ISNULL(column.arrow_type_)
            || (OB_ISNULL(column.ob_to_arrow_func_)
                && OB_ISNULL(column.ob_to_arrow_eval_func_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async result write column plan", K(ret), K(col_idx));
        } else if (OB_FAIL(column.expr_->eval_vector(eval_ctx, batch_rows))) {
          LOG_WARN("failed to evaluate async result output vector",
                   K(ret), K(col_idx));
        } else if (OB_ISNULL(vector = column.expr_->get_vector(eval_ctx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("async result output vector is null", K(ret), K(col_idx));
        } else if (OB_NOT_NULL(column.zero_copy_func_)
                   && OB_FAIL(column.zero_copy_func_(
                          *vector, batch_rows, column.arrow_type_,
                          array, zero_copy_used))) {
          LOG_WARN("failed to build zero-copy async result Arrow array",
                   K(ret), K(col_idx));
        } else if (!zero_copy_used) {
          std::unique_ptr<arrow::ArrayBuilder> builder;
          if (OB_FAIL(arrow_status_to_ret(arrow::MakeBuilder(
                  memory_pool, column.arrow_type_, &builder)))) {
            LOG_WARN("failed to create async result Arrow builder",
                     K(ret), K(col_idx));
          } else if (OB_ISNULL(builder)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("async result Arrow builder is null", K(ret), K(col_idx));
          } else if (OB_FAIL(arrow_status_to_ret(builder->Reserve(active_row_count)))) {
            LOG_WARN("failed to reserve async result Arrow builder",
                     K(ret), K(col_idx), K(active_row_count));
          } else {
            value_allocator.reuse();
            if (OB_NOT_NULL(column.ob_to_arrow_eval_func_)) {
              ret = column.ob_to_arrow_eval_func_(
                  *column.expr_, eval_ctx, *vector, batch_rows,
                  *builder, value_allocator);
            } else {
              ret = column.ob_to_arrow_func_(
                  *column.expr_, *vector, batch_rows,
                  *builder, value_allocator);
            }
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to encode async result Arrow column",
                       K(ret), K(col_idx), K(column.expr_->datum_meta_.type_));
            } else if (OB_FAIL(arrow_status_to_ret(builder->Finish(&array)))) {
              LOG_WARN("failed to finish async result Arrow array",
                       K(ret), K(col_idx));
            }
          }
        }
        if (OB_SUCC(ret)
            && (OB_ISNULL(array) || array->length() != active_row_count
                || !array->type()->Equals(column.arrow_type_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async result encoded Arrow array",
                   K(ret), K(col_idx), KP(array.get()), K(active_row_count));
        } else if (OB_SUCC(ret)) {
          arrays.push_back(array);
        }
      }
      if (OB_SUCC(ret)) {
        record_batch = arrow::RecordBatch::Make(schema_, active_row_count, arrays);
        if (OB_ISNULL(record_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create async result Arrow record batch", K(ret));
        } else if (OB_FAIL(arrow_status_to_ret(record_batch->ValidateFull()))) {
          LOG_WARN("invalid async result Arrow record batch", K(ret));
        }
      }
    } catch (const std::bad_alloc &ex) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("out of memory while encoding async result Arrow batch",
               K(ret), "info", ex.what());
    } catch (const std::exception &ex) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception while encoding async result Arrow batch",
               K(ret), "info", ex.what());
    } catch (...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught unknown exception while encoding async result Arrow batch", K(ret));
    }
  }
  return ret;
}

int ObAsyncResultArrowCodec::build_write_plan(
    ObExecContext &exec_ctx,
    const ObIArray<ObExpr *> &select_exprs,
    const ObIArray<ObString> &alias_names,
    const ObIArray<uint64_t> &field_ids,
    ObAsyncResultArrowWritePlan &plan)
{
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "ResultPlan"));
  int ret = OB_SUCCESS;
  plan.reset();
  if (select_exprs.count() != alias_names.count()
      || (field_ids.count() != 0 && field_ids.count() != select_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result Arrow write-plan input", K(ret),
             "select_count", select_exprs.count(),
             "alias_count", alias_names.count(),
             "field_id_count", field_ids.count());
  } else {
    try {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      fields.reserve(select_exprs.count());
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
        ObExpr *expr = select_exprs.at(col_idx);
        const uint64_t field_id = field_ids.count() == select_exprs.count()
                                ? field_ids.at(col_idx)
                                : static_cast<uint64_t>(col_idx + 1);
        MappingSpec mapping;
        ObObjMeta semantic_meta;
        if (OB_ISNULL(expr) || field_id == 0 || field_id > INT32_MAX) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid async result Arrow output column",
                   K(ret), K(col_idx), KP(expr), K(field_id));
        } else if (OB_FAIL(resolve_semantic_obj_meta(
                       *expr, exec_ctx, semantic_meta))) {
          LOG_WARN("failed to resolve async result Arrow output metadata",
                   K(ret), K(col_idx), K(expr->datum_meta_));
        } else if (OB_FAIL(select_v2_mapping(
                       expr->datum_meta_.type_,
                       expr->datum_meta_.precision_,
                       expr->datum_meta_.scale_,
                       semantic_meta.get_collation_type(),
                       mapping))) {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "async result Arrow type");
          LOG_WARN("failed to select async result Arrow mapping",
                   K(ret), K(col_idx), K(expr->datum_meta_));
        } else {
          const int64_t precision = ob_is_string_type(expr->datum_meta_.type_)
                                  ? -1 : expr->datum_meta_.precision_;
          const int64_t length_semantics = ob_is_string_type(expr->datum_meta_.type_)
                                         ? expr->datum_meta_.length_semantics_ : -1;
          const std::shared_ptr<arrow::KeyValueMetadata> metadata =
              arrow::key_value_metadata(
                  {FIELD_ID_KEY, CODEC_KEY, MAPPING_VERSION_KEY, ENCODING_KEY,
                   OB_TYPE_ID_KEY, PRECISION_KEY, SCALE_KEY, COLLATION_ID_KEY,
                   COLLATION_LEVEL_KEY, LENGTH_KEY, LENGTH_SEMANTICS_KEY},
                  {std::to_string(field_id), CODEC_V2,
                   std::to_string(CURRENT_CODEC_VERSION), mapping.encoding_,
                   std::to_string(expr->datum_meta_.type_),
                   std::to_string(precision),
                   std::to_string(expr->datum_meta_.scale_),
                   std::to_string(semantic_meta.get_collation_type()),
                   std::to_string(semantic_meta.get_collation_level()),
                   std::to_string(expr->max_length_),
                   std::to_string(length_semantics)});
          fields.push_back(arrow::field(
              std::string(alias_names.at(col_idx).ptr(),
                          alias_names.at(col_idx).length()),
              mapping.arrow_type_, true, metadata));
          ObAsyncResultArrowWritePlan::ColumnPlan column;
          column.expr_ = expr;
          column.arrow_type_ = mapping.arrow_type_;
          column.ob_to_arrow_func_ = mapping.ob_to_arrow_func_;
          column.ob_to_arrow_eval_func_ = mapping.ob_to_arrow_eval_func_;
          column.zero_copy_func_ = mapping.zero_copy_func_;
          if (OB_FAIL(plan.columns_.push_back(column))) {
            LOG_WARN("failed to append async result Arrow write plan",
                     K(ret), K(col_idx));
          }
        }
      }
      if (OB_SUCC(ret)) {
        plan.schema_ = arrow::schema(
            fields,
            arrow::key_value_metadata(
                {CODEC_KEY, MAPPING_VERSION_KEY},
                {CODEC_V2, std::to_string(CURRENT_CODEC_VERSION)}));
        if (OB_ISNULL(plan.schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create async result Arrow schema", K(ret));
        }
      }
    } catch (const std::bad_alloc &ex) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("out of memory while building async result Arrow plan",
               K(ret), "info", ex.what());
    } catch (const std::exception &ex) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception while building async result Arrow plan",
               K(ret), "info", ex.what());
    } catch (...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught unknown exception while building async result Arrow plan", K(ret));
    }
  }
  return ret;
}

int ObAsyncResultArrowCodec::build_read_plan(
    const ObExecContext &exec_ctx,
    const ObExpr &expr,
    const arrow::Field &field,
    const parquet::ColumnDescriptor &column_descriptor,
    ObAsyncResultArrowReadPlan &plan)
{
  int ret = OB_SUCCESS;
  int64_t codec_version = 0;
  StoredFieldDescriptor descriptor;
  MappingSpec mapping;
  ObObjMeta semantic_meta;
  plan.arrow_type_.reset();
  plan.physical_kind_ = ObAsyncResultParquetValueBatch::PhysicalKind::INVALID;
  plan.parquet_to_arrow_func_ = nullptr;
  plan.arrow_to_ob_func_ = nullptr;
  try {
    if (OB_FAIL(get_codec_version(field.metadata(), codec_version))) {
      LOG_WARN("failed to get async result read codec",
               K(ret), "field", field.name().c_str());
    } else if (OB_FAIL(validate_field_codec(field, codec_version, descriptor))) {
      LOG_WARN("invalid async result field codec",
               K(ret), "field", field.name().c_str());
    } else if (OB_FAIL(resolve_semantic_obj_meta(
                   expr, exec_ctx, semantic_meta))) {
      LOG_WARN("failed to resolve async result relation metadata",
               K(ret), K(expr.datum_meta_));
    } else if (descriptor.ob_type_id_ != expr.datum_meta_.type_) {
      ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
      LOG_WARN("async result OB type descriptor differs from output expression",
               K(ret), K(descriptor.ob_type_id_), K(expr.datum_meta_.type_));
    } else if (2 == codec_version
               && (descriptor.precision_
                       != (ob_is_string_type(expr.datum_meta_.type_)
                               ? -1 : expr.datum_meta_.precision_)
                   || descriptor.scale_ != expr.datum_meta_.scale_
                   || descriptor.collation_id_ != semantic_meta.get_collation_type()
                   || descriptor.collation_level_
                          != semantic_meta.get_collation_level()
                   || descriptor.length_ != expr.max_length_
                   || descriptor.length_semantics_
                          != (ob_is_string_type(expr.datum_meta_.type_)
                                  ? expr.datum_meta_.length_semantics_ : -1))) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Arrow descriptor differs from relation descriptor",
               K(ret), K(descriptor.precision_), K(descriptor.scale_),
               K(descriptor.collation_id_), K(descriptor.collation_level_),
               K(descriptor.length_), K(descriptor.length_semantics_),
               K(expr.datum_meta_), K(expr.obj_meta_), K(expr.max_length_));
    } else if (1 == codec_version
               && OB_FAIL(select_v1_mapping(
                      expr.datum_meta_.type_, semantic_meta.get_collation_type(), mapping))) {
      LOG_WARN("unsupported v1 async result read mapping",
               K(ret), K(expr.datum_meta_));
    } else if (2 == codec_version
               && OB_FAIL(select_stored_v2_mapping(
                      expr.datum_meta_.type_, expr.datum_meta_.precision_,
                      expr.datum_meta_.scale_, semantic_meta.get_collation_type(),
                      ObYearType == expr.datum_meta_.type_
                          && arrow::Type::INT16 == field.type()->id(), mapping))) {
      LOG_WARN("unsupported v2 async result read mapping",
               K(ret), K(expr.datum_meta_));
    } else if (2 == codec_version
               && (OB_ISNULL(descriptor.encoding_)
                   || *descriptor.encoding_ != mapping.encoding_)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Arrow encoding differs from registered mapping",
               K(ret), "stored_encoding",
               OB_NOT_NULL(descriptor.encoding_)
                   ? descriptor.encoding_->c_str() : "(null)",
               "expected_encoding", mapping.encoding_);
    } else if (OB_ISNULL(field.type())
               || !field.type()->Equals(mapping.arrow_type_)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Arrow type differs from registered mapping",
               K(ret), "stored_type",
               OB_NOT_NULL(field.type()) ? field.type()->ToString().c_str() : "(null)",
               "expected_type", mapping.arrow_type_name_.c_str());
    } else if (column_descriptor.physical_type() != mapping.parquet_physical_
               || column_descriptor.max_repetition_level() != 0
               || column_descriptor.max_definition_level() != 1) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Parquet type differs from registered mapping",
               K(ret), "stored_physical", column_descriptor.physical_type(),
               "expected_physical", mapping.parquet_physical_,
               "max_rep_level", column_descriptor.max_repetition_level(),
               "max_def_level", column_descriptor.max_definition_level());
    } else {
      plan.arrow_type_ = mapping.arrow_type_;
      plan.physical_kind_ = get_physical_kind(mapping.parquet_physical_);
      plan.parquet_to_arrow_func_ = mapping.parquet_to_arrow_func_;
      plan.arrow_to_ob_func_ = mapping.arrow_to_ob_func_;
      if (!plan.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("registered async result read plan is incomplete",
                 K(ret), K(expr.datum_meta_));
      }
    }
  } catch (const std::bad_alloc &ex) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("out of memory while building async result read plan",
             K(ret), "info", ex.what());
  } catch (const std::exception &ex) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("caught exception while building async result read plan",
             K(ret), "info", ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught unknown exception while building async result read plan", K(ret));
  }
  return ret;
}

int ObAsyncResultArrowCodec::validate_schema(
    const arrow::Schema &schema,
    const int64_t parquet_column_count)
{
  int ret = OB_SUCCESS;
  int64_t codec_version = 0;
  int64_t schema_mapping_version = 0;
  ObSEArray<int64_t, 16> field_ids;
  try {
    if (parquet_column_count < 0
        || schema.num_fields() != parquet_column_count) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("async result Arrow and Parquet column counts differ",
               K(ret), "arrow_fields", schema.num_fields(),
               K(parquet_column_count));
    } else if (OB_FAIL(get_codec_version(schema.metadata(), codec_version))) {
      LOG_WARN("failed to get async result schema codec", K(ret));
    } else if (2 == codec_version
               && OB_FAIL(parse_metadata_int64(
                      schema.metadata(), MAPPING_VERSION_KEY,
                      schema_mapping_version))) {
      LOG_WARN("failed to parse async result schema mapping version", K(ret));
    } else if (2 == codec_version
               && schema_mapping_version != CURRENT_CODEC_VERSION) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported async result schema mapping version",
               K(ret), K(schema_mapping_version));
    }
    for (int64_t field_idx = 0;
         OB_SUCC(ret) && field_idx < schema.num_fields();
         ++field_idx) {
      const std::shared_ptr<arrow::Field> &field = schema.field(field_idx);
      StoredFieldDescriptor descriptor;
      MappingSpec mapping;
      if (OB_ISNULL(field)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("null field in async result Arrow schema", K(ret), K(field_idx));
      } else if (OB_FAIL(validate_field_codec(
                     *field, codec_version, descriptor))) {
        LOG_WARN("invalid field in async result Arrow schema",
                 K(ret), K(field_idx), "field", field->name().c_str());
      } else {
        for (int64_t id_idx = 0;
             OB_SUCC(ret) && id_idx < field_ids.count();
             ++id_idx) {
          if (field_ids.at(id_idx) == descriptor.field_id_) {
            ret = OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("duplicate async result Arrow field id",
                     K(ret), K(field_idx), K(descriptor.field_id_));
          }
        }
        if (OB_SUCC(ret)
            && OB_FAIL(field_ids.push_back(descriptor.field_id_))) {
          LOG_WARN("failed to remember async result Arrow field id", K(ret));
        } else if (OB_SUCC(ret) && 1 == codec_version
                   && OB_FAIL(select_v1_mapping(
                          static_cast<ObObjType>(descriptor.ob_type_id_),
                          CS_TYPE_BINARY, mapping))) {
          // V1 character fields require their actual collation, which is not
          // stored in Arrow metadata. Their exact mapping is checked against
          // the Result relation descriptor in build_read_plan().
          const ObObjType type = static_cast<ObObjType>(descriptor.ob_type_id_);
          if (ObVarcharType == type || ObCharType == type
              || ObNVarchar2Type == type || ObNCharType == type) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("unsupported v1 async result Arrow field", K(ret), K(type));
          }
        } else if (OB_SUCC(ret) && 2 == codec_version
                   && (descriptor.collation_id_ < CS_TYPE_INVALID
                       || descriptor.collation_id_ >= CS_TYPE_MAX)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid async result collation id",
                   K(ret), K(descriptor.collation_id_));
        } else if (OB_SUCC(ret) && 2 == codec_version
                   && OB_FAIL(select_stored_v2_mapping(
                          static_cast<ObObjType>(descriptor.ob_type_id_),
                          static_cast<int32_t>(descriptor.precision_),
                          static_cast<int32_t>(descriptor.scale_),
                          static_cast<ObCollationType>(descriptor.collation_id_),
                          ObYearType == descriptor.ob_type_id_
                              && arrow::Type::INT16 == field->type()->id(),
                          mapping))) {
          LOG_WARN("unsupported v2 async result Arrow field",
                   K(ret), K(descriptor.ob_type_id_));
        } else if (OB_SUCC(ret) && 2 == codec_version
                   && (OB_ISNULL(descriptor.encoding_)
                       || *descriptor.encoding_ != mapping.encoding_
                       || !field->type()->Equals(mapping.arrow_type_))) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("async result field does not match registered mapping",
                   K(ret), K(field_idx), K(descriptor.ob_type_id_));
        }
      }
    }
  } catch (const std::bad_alloc &ex) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("out of memory while validating async result Arrow schema",
             K(ret), "info", ex.what());
  } catch (const std::exception &ex) {
    ret = OB_INVALID_EXTERNAL_FILE;
    LOG_WARN("caught exception while validating async result Arrow schema",
             K(ret), "info", ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught unknown exception while validating async result Arrow schema", K(ret));
  }
  return ret;
}

int ObAsyncResultArrowCodec::fill_result_column_metadata(
    ObIAllocator &allocator,
    const ObExpr &expr,
    ObResultColumnMetadata &column)
{
  int ret = OB_SUCCESS;
  MappingSpec mapping;
  const char *physical_name = nullptr;
  try {
    if (OB_FAIL(select_v2_mapping(
            expr.datum_meta_.type_, expr.datum_meta_.precision_,
            expr.datum_meta_.scale_, expr.obj_meta_.get_collation_type(), mapping))) {
      LOG_WARN("failed to select async result metadata mapping",
               K(ret), K(expr.datum_meta_));
    } else if (OB_ISNULL(physical_name =
                   get_parquet_physical_name(mapping.parquet_physical_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown async result Parquet physical type",
               K(ret), K(mapping.parquet_physical_));
    } else if (OB_FAIL(deep_copy_string(
                   allocator, mapping.arrow_type_name_, column.arrow_type_))) {
      LOG_WARN("failed to copy async result Arrow type", K(ret));
    } else if (OB_FAIL(deep_copy_string(
                   allocator, std::string(physical_name),
                   column.parquet_physical_type_))) {
      LOG_WARN("failed to copy async result Parquet physical type", K(ret));
    } else if (OB_FAIL(deep_copy_string(
                   allocator, mapping.parquet_logical_,
                   column.parquet_logical_type_))) {
      LOG_WARN("failed to copy async result Parquet logical type", K(ret));
    } else if (OB_FAIL(deep_copy_string(
                   allocator, std::string("ob_async_result"),
                   column.codec_name_))) {
      LOG_WARN("failed to copy async result codec name", K(ret));
    } else {
      column.codec_version_ = CURRENT_CODEC_VERSION;
    }
  } catch (const std::bad_alloc &ex) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("out of memory while building async result column metadata",
             K(ret), "info", ex.what());
  } catch (const std::exception &ex) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught exception while building async result column metadata",
             K(ret), "info", ex.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("caught unknown exception while building async result column metadata", K(ret));
  }
  return ret;
}

bool ObAsyncResultArrowCodec::is_supported_result_column_metadata(
    const ObResultColumnMetadata &column)
{
  bool supported = false;
  try {
    const ObObjType type = static_cast<ObObjType>(column.ob_type_id_);
    const ObCollationType collation =
        static_cast<ObCollationType>(column.collation_id_);
    MappingSpec mapping;
    const char *physical_name = nullptr;
    if (0 != column.codec_name_.case_compare("ob_async_result")
        || column.ob_type_id_ < ObNullType
        || column.ob_type_id_ >= ObMaxType
        || column.collation_id_ < CS_TYPE_INVALID
        || column.collation_id_ >= CS_TYPE_MAX) {
    } else if (1 == column.codec_version_) {
      const char *arrow_name = get_v1_arrow_type_name(type, collation);
      if (OB_NOT_NULL(arrow_name)
          && OB_SUCCESS == select_v1_mapping(type, collation, mapping)
          && OB_NOT_NULL(physical_name =
                 get_parquet_physical_name(mapping.parquet_physical_))) {
        supported = 0 == column.arrow_type_.case_compare(arrow_name)
                 && 0 == column.parquet_physical_type_.case_compare(physical_name)
                 && metadata_string_case_equal(
                        column.parquet_logical_type_,
                        ObString::make_string(mapping.parquet_logical_.c_str()));
      }
    } else if (CURRENT_CODEC_VERSION == column.codec_version_
               && OB_SUCCESS == select_stored_v2_mapping(
                      type, static_cast<int32_t>(column.precision_),
                      static_cast<int32_t>(column.scale_), collation,
                      ObYearType == type
                          && 0 == column.arrow_type_.case_compare("int16"), mapping)
               && OB_NOT_NULL(physical_name =
                      get_parquet_physical_name(mapping.parquet_physical_))) {
      supported = 0 == column.arrow_type_.case_compare(
                         ObString::make_string(mapping.arrow_type_name_.c_str()))
               && 0 == column.parquet_physical_type_.case_compare(physical_name)
               && metadata_string_case_equal(
                      column.parquet_logical_type_,
                      ObString::make_string(mapping.parquet_logical_.c_str()));
    }
  } catch (...) {
    supported = false;
  }
  return supported;
}

} // namespace sql
} // namespace oceanbase
