/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/result_catalog/ob_async_result_parquet_table_row_iter.h"

#include <algorithm>
#include <exception>
#include <new>

#include <parquet/arrow/schema.h>

#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObAsyncResultParquetTableRowIterator::ObAsyncResultParquetTableRowIterator()
    : state_(),
      allocator_("ResultReader", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      arrow_alloc_(),
      read_props_(&arrow_alloc_),
      file_access_(),
      arrow_file_(),
      file_reader_(),
      file_metadata_(),
      arrow_schema_(),
      row_group_reader_(),
      column_indices_(),
      column_readers_(),
      read_plans_(),
      definition_levels_(),
      repetition_levels_(),
      file_url_ptrs_(),
      file_url_lens_(),
      skip_vector_(nullptr),
      value_buffer_(nullptr),
      url_(),
      next_row_group_idx_(0),
      end_row_group_idx_(0),
      row_group_row_count_(0),
      read_rows_in_group_(0)
{
}

ObAsyncResultParquetTableRowIterator::~ObAsyncResultParquetTableRowIterator()
{
  reset();
  read_plans_.release_array();
  column_readers_.release_array();
}

int ObAsyncResultParquetTableRowIterator::init(
    const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->op_)
      || !share::is_result_catalog_table(scan_param->lake_table_format_)
      || scan_param->external_file_format_.format_type_
             != ObExternalFileFormat::PARQUET_FORMAT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Result Catalog Parquet scan", K(ret), KP(scan_param));
  } else if (OB_NOT_NULL(scan_param->pd_storage_filters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Catalog reader does not support predicate pushdown", K(ret));
  } else if (!scan_param->sample_info_.is_no_sample()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async result reader does not support storage sampling",
             K(ret), K(scan_param->sample_info_));
  } else {
    arrow_alloc_.init(ObMemAttr(MTL_ID(), "ResultArrow"));
    if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
      LOG_WARN("failed to initialize Result Catalog expressions", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t column_count = file_column_exprs_.count();
    const int64_t batch_size = scan_param->op_->get_eval_ctx().max_batch_size_;
    if (OB_UNLIKELY(batch_size <= 0 || column_count < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result read shape", K(ret), K(batch_size), K(column_count));
    } else if (column_count > 0
               && OB_FAIL(column_indices_.allocate_array(allocator_, column_count))) {
      LOG_WARN("failed to allocate Result Catalog column indexes", K(ret));
    } else if (column_count > 0
               && OB_FAIL(column_readers_.allocate_array(allocator_, column_count))) {
      LOG_WARN("failed to allocate Result Catalog column readers", K(ret));
    } else if (column_count > 0
               && OB_FAIL(read_plans_.allocate_array(allocator_, column_count))) {
      LOG_WARN("failed to allocate Result Catalog read plans", K(ret));
    } else if (OB_FAIL(definition_levels_.allocate_array(allocator_, batch_size))) {
      LOG_WARN("failed to allocate Parquet definition levels", K(ret));
    } else if (OB_FAIL(repetition_levels_.allocate_array(allocator_, batch_size))) {
      LOG_WARN("failed to allocate Parquet repetition levels", K(ret));
    } else if (OB_ISNULL(value_buffer_ = allocator_.alloc(
                   batch_size * sizeof(parquet::ByteArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Result Catalog value buffer", K(ret), K(batch_size));
    } else if (OB_ISNULL(skip_vector_ = to_bit_vector(
                   allocator_.alloc(ObBitVector::memory_size(batch_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Result Catalog conversion bitmap", K(ret));
    } else if (!file_meta_column_exprs_.empty()
               && OB_FAIL(file_url_ptrs_.allocate_array(allocator_, batch_size))) {
      LOG_WARN("failed to allocate Result Catalog file URL pointers", K(ret));
    } else if (!file_meta_column_exprs_.empty()
               && OB_FAIL(file_url_lens_.allocate_array(allocator_, batch_size))) {
      LOG_WARN("failed to allocate Result Catalog file URL lengths", K(ret));
    } else {
      skip_vector_->reset(batch_size);
    }
  }
  return ret;
}

void ObAsyncResultParquetTableRowIterator::reset()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < column_readers_.count(); ++i) {
    column_readers_.at(i).reset();
  }
  row_group_reader_.reset();
  file_metadata_.reset();
  arrow_schema_.reset();
  file_reader_.reset();
  arrow_file_.reset();
  if (OB_FAIL(file_access_.reset())) {
    LOG_WARN("failed to reset Result Catalog file access", K(ret));
  }
  state_.reuse();
  url_.reset();
  next_row_group_idx_ = 0;
  end_row_group_idx_ = 0;
  row_group_row_count_ = 0;
  read_rows_in_group_ = 0;
}

int ObAsyncResultParquetTableRowIterator::next_file()
{
  int ret = OB_SUCCESS;
  const int64_t task_idx = state_.file_idx_++;
  row_group_reader_.reset();
  for (int64_t i = 0; i < column_readers_.count(); ++i) {
    column_readers_.at(i).reset();
  }
  file_metadata_.reset();
  arrow_schema_.reset();
  file_reader_.reset();
  arrow_file_.reset();
  url_.reset();
  if (OB_FAIL(file_access_.reset())) {
    LOG_WARN("failed to close previous Result Catalog file", K(ret));
  } else if (task_idx >= scan_param_->scan_tasks_.count()) {
    ret = OB_ITER_END;
  } else {
    ObFileScanTask *task =
        static_cast<ObFileScanTask *>(scan_param_->scan_tasks_.at(task_idx));
    int64_t file_size = -1;
    int64_t modify_time = 0;
    ObString file_content_digest;
    const ObString &location = scan_param_->external_file_location_;
    if (OB_ISNULL(task) || task->file_url_.empty()
        || share::ObExternalTableUtils::is_abs_url(task->file_url_)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("invalid Result Catalog file task", K(ret), K(task_idx), KP(task));
    } else if (OB_FAIL(url_.append_fmt(
                   "%.*s%s%.*s",
                   location.length(), location.ptr(),
                   (location.empty() || location[location.length() - 1] == '/')
                       ? "" : "/",
                   task->file_url_.length(), task->file_url_.ptr()))) {
      LOG_WARN("failed to build Result Catalog file URL", K(ret), K(task_idx));
    } else if (OB_FAIL(share::ObExternalTableUtils::collect_file_basic_info(
                   location,
                   scan_param_->external_file_access_info_,
                   url_.string(),
                   allocator_,
                   file_size,
                   modify_time,
                   file_content_digest))) {
      LOG_WARN("failed to stat Result Catalog file", K(ret), K(url_));
    } else if (file_size <= 0) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("published Result Catalog file is empty", K(ret), K(url_), K(file_size));
    } else {
      state_.cur_file_url_ = task->file_url_;
      state_.cur_file_id_ = task->file_id_;
      ObExternalFileUrlInfo file_info(
          location,
          scan_param_->external_file_access_info_,
          url_.string(),
          file_content_digest,
          file_size,
          modify_time);
      ObExternalFileCacheOptions cache_options(
          false /* memory cache */, false /* disk cache */);
      arrow_file_ = std::make_shared<ObArrowFile>(
          file_access_, url_.ptr(), &arrow_alloc_);
      arrow_file_->set_timeout_timestamp(scan_param_->timeout_);
      if (OB_FAIL(arrow_file_->open(file_info, cache_options))) {
        LOG_WARN("failed to open Result Catalog file", K(ret), K(url_));
      } else {
        file_reader_ = parquet::ParquetFileReader::Open(arrow_file_, read_props_);
        if (OB_ISNULL(file_reader_)
            || OB_ISNULL(file_metadata_ = file_reader_->metadata())) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("Result Catalog Parquet footer is absent", K(ret), K(url_));
        } else if (OB_FAIL(prepare_file_schema())) {
          LOG_WARN("failed to validate Result Catalog Parquet schema", K(ret), K(url_));
        } else if (OB_FAIL(prepare_selected_columns())) {
          LOG_WARN("failed to prepare Result Catalog projected columns", K(ret), K(url_));
        } else if (task->first_lineno_ <= 0
                   || task->first_lineno_ > file_metadata_->num_row_groups()
                   || task->last_lineno_ < task->first_lineno_
                   || (task->last_lineno_ != INT64_MAX
                       && task->last_lineno_ > file_metadata_->num_row_groups())) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("invalid Result Catalog row-group task",
                   K(ret), K(url_), K(task->first_lineno_),
                   K(task->last_lineno_), "row_group_count",
                   file_metadata_->num_row_groups());
        } else {
          // The hidden line number belongs to the physical file. Chunk scans
          // that start at a later row group must keep its original offset.
          int64_t first_row_number = 0;
          for (int64_t rg_idx = 0;
               OB_SUCC(ret) && rg_idx < task->first_lineno_ - 1;
               ++rg_idx) {
            const std::shared_ptr<parquet::RowGroupMetaData> row_group =
                file_metadata_->RowGroup(static_cast<int>(rg_idx));
            if (OB_ISNULL(row_group) || row_group->num_rows() <= 0
                || row_group->num_rows() > INT64_MAX - first_row_number) {
              ret = OB_INVALID_EXTERNAL_FILE;
              LOG_WARN("invalid preceding Result Catalog row group",
                       K(ret), K(url_), K(rg_idx));
            } else {
              first_row_number += row_group->num_rows();
            }
          }
          if (OB_SUCC(ret)) {
            state_.cur_line_number_ = first_row_number;
            next_row_group_idx_ = task->first_lineno_ - 1;
            end_row_group_idx_ = std::min<int64_t>(
                task->last_lineno_, file_metadata_->num_row_groups());
            row_group_row_count_ = 0;
            read_rows_in_group_ = 0;
          }
        }
      }
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::prepare_file_schema()
{
  int ret = OB_SUCCESS;
  arrow_schema_.reset();
  if (OB_ISNULL(file_metadata_) || OB_ISNULL(file_metadata_->schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Catalog Parquet schema is null", K(ret));
  } else {
    parquet::ArrowReaderProperties properties(false /* use_threads */);
    const arrow::Status status = parquet::arrow::FromParquetSchema(
        file_metadata_->schema(),
        properties,
        file_metadata_->key_value_metadata(),
        &arrow_schema_);
    if (!status.ok()) {
      ret = status.IsOutOfMemory()
          ? OB_ALLOCATE_MEMORY_FAILED : OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("failed to restore Result Catalog Arrow schema",
               K(ret), "arrow_status", status.ToString().c_str());
    } else if (OB_ISNULL(arrow_schema_)) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("Result Catalog Arrow schema is null", K(ret));
    } else if (OB_FAIL(ObAsyncResultArrowCodec::validate_schema(
                   *arrow_schema_, file_metadata_->schema()->num_columns()))) {
      LOG_WARN("invalid private Result Catalog Arrow schema", K(ret));
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::prepare_selected_columns()
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = scan_param_->op_->get_eval_ctx().exec_ctx_;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
    const int64_t parquet_idx = file_column_exprs_.at(i)->extra_ - 1;
    // Match the original Parquet reader: validate the codec against the
    // physical file expression. The mapped SQL output column may have a
    // different collation level.
    ObExpr *expr = file_column_exprs_.at(i);
    if (OB_ISNULL(expr) || parquet_idx < 0
        || parquet_idx >= file_metadata_->schema()->num_columns()
        || parquet_idx >= arrow_schema_->num_fields()) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("Result Catalog projection is absent from Parquet file",
               K(ret), K(i), K(parquet_idx), KP(expr));
    } else {
      const parquet::ColumnDescriptor *column_desc =
          file_metadata_->schema()->Column(static_cast<int>(parquet_idx));
      const parquet::schema::Node *root_node =
          file_metadata_->schema()->GetColumnRoot(static_cast<int>(parquet_idx));
      const std::shared_ptr<arrow::Field> &field =
          arrow_schema_->field(static_cast<int>(parquet_idx));
      if (OB_ISNULL(column_desc) || OB_ISNULL(root_node)
          || root_node->is_group() || OB_ISNULL(field)) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("Result Catalog column is not a primitive field",
                 K(ret), K(i), K(parquet_idx));
      } else if (OB_FAIL(ObAsyncResultArrowCodec::build_read_plan(
                     exec_ctx, *expr, *field, *column_desc, read_plans_.at(i)))) {
        LOG_WARN("failed to build private Result Catalog column plan",
                 K(ret), K(i), K(parquet_idx));
      } else {
        column_indices_.at(i) = static_cast<int>(parquet_idx);
      }
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::next_row_group()
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    if (OB_ISNULL(file_reader_) || next_row_group_idx_ >= end_row_group_idx_) {
      if (OB_FAIL(next_file())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to open next Result Catalog file", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      row_group_reader_ = file_reader_->RowGroup(
          static_cast<int>(next_row_group_idx_++));
      if (OB_ISNULL(row_group_reader_)
          || OB_ISNULL(row_group_reader_->metadata())
          || row_group_reader_->metadata()->num_rows() <= 0) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("invalid Result Catalog Parquet row group",
                 K(ret), K(url_), K(next_row_group_idx_));
      } else {
        row_group_row_count_ = row_group_reader_->metadata()->num_rows();
        read_rows_in_group_ = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_indices_.count(); ++i) {
          column_readers_.at(i) = row_group_reader_->Column(column_indices_.at(i));
          if (OB_ISNULL(column_readers_.at(i))) {
            ret = OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("missing projected Result Catalog column reader",
                     K(ret), K(i), K(column_indices_.at(i)));
          }
        }
        found = OB_SUCC(ret);
      }
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::read_column(
    const int64_t column_idx,
    const int64_t row_count,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  parquet::ColumnReader *reader = column_readers_.at(column_idx).get();
  ObExpr *expr = get_column_expr_by_id(column_idx);
  const ObAsyncResultArrowReadPlan &plan = read_plans_.at(column_idx);
  if (OB_ISNULL(reader) || OB_ISNULL(expr) || !plan.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid Result Catalog column reader", K(ret), K(column_idx));
  } else if (OB_FAIL(expr->init_vector_for_write(
                 eval_ctx, expr->get_default_res_format(), eval_ctx.max_batch_size_))) {
    LOG_WARN("failed to initialize Result Catalog output vector", K(ret), K(column_idx));
  } else {
    int64_t loaded = 0;
    while (OB_SUCC(ret) && loaded < row_count) {
      int64_t values_count = 0;
      int64_t read_count = 0;
      ObAsyncResultParquetValueBatch batch;
      batch.kind_ = plan.get_physical_kind();
      batch.values_ = value_buffer_;
      batch.definition_levels_ = definition_levels_.get_data();
      batch.max_definition_level_ = reader->descr()->max_definition_level();
      const int64_t wanted = row_count - loaded;
      switch (batch.kind_) {
        case ObAsyncResultParquetValueBatch::PhysicalKind::INT32:
          read_count = static_cast<parquet::Int32Reader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<int32_t *>(value_buffer_), &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::INT64:
          read_count = static_cast<parquet::Int64Reader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<int64_t *>(value_buffer_), &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::FLOAT:
          read_count = static_cast<parquet::FloatReader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<float *>(value_buffer_), &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::DOUBLE:
          read_count = static_cast<parquet::DoubleReader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<double *>(value_buffer_), &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::BYTE_ARRAY:
          read_count = static_cast<parquet::ByteArrayReader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<parquet::ByteArray *>(value_buffer_), &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::FIXED_LEN_BYTE_ARRAY:
          batch.fixed_byte_width_ = reader->descr()->type_length();
          read_count = static_cast<parquet::FixedLenByteArrayReader *>(reader)->ReadBatch(
              wanted, definition_levels_.get_data(),
              repetition_levels_.get_data(),
              static_cast<parquet::FixedLenByteArray *>(value_buffer_),
              &values_count);
          break;
        case ObAsyncResultParquetValueBatch::PhysicalKind::INVALID:
        default:
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("unsupported private Result Catalog Parquet physical type",
                   K(ret), K(column_idx), K(batch.kind_));
          break;
      }
      if (OB_FAIL(ret)) {
      } else if (read_count <= 0 || read_count > wanted
                 || values_count < 0 || values_count > read_count) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("Result Catalog column ended before row-group metadata",
                 K(ret), K(column_idx), K(read_count), K(values_count),
                 K(wanted), K(row_group_row_count_));
      } else {
        batch.row_count_ = read_count;
        batch.value_count_ = values_count;
        if (OB_FAIL(plan.decode_parquet_batch(
                batch, &arrow_alloc_, eval_ctx, *expr, loaded))) {
          LOG_WARN("failed to decode private Result Catalog column",
                   K(ret), K(column_idx), K(loaded), K(read_count));
        } else {
          loaded += read_count;
        }
      }
    }
    if (OB_SUCC(ret)) {
      expr->set_evaluated_projected(eval_ctx);
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::fill_file_meta_columns(
    const int64_t row_count,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); ++i) {
    ObExpr *expr = file_meta_column_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null Result Catalog file metadata expression", K(ret), K(i));
    } else if (T_PSEUDO_EXTERNAL_FILE_URL == expr->type_) {
      for (int64_t row_idx = 0; row_idx < row_count; ++row_idx) {
        file_url_ptrs_.at(row_idx) = state_.cur_file_url_.ptr();
        file_url_lens_.at(row_idx) = state_.cur_file_url_.length();
      }
      if (OB_FAIL(expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, row_count))) {
        LOG_WARN("failed to initialize Result Catalog file URL vector", K(ret));
      } else {
        StrDiscVec *vector = static_cast<StrDiscVec *>(expr->get_vector(eval_ctx));
        vector->set_ptrs(file_url_ptrs_.get_data());
        vector->set_lens(file_url_lens_.get_data());
        expr->set_evaluated_projected(eval_ctx);
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported Result Catalog file metadata expression",
               K(ret), K(expr->type_));
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::assign_converted_column(
    ObEvalCtx &eval_ctx,
    ObExpr &from,
    ObExpr &to,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  VectorHeader &to_header = to.get_vector_header(eval_ctx);
  VectorHeader &from_header = from.get_vector_header(eval_ctx);
  if (from_header.format_ == VEC_UNIFORM_CONST) {
    ObDatum *from_datum =
        static_cast<ObUniformBase *>(from.get_vector(eval_ctx))->get_datums();
    if (OB_FAIL(to.init_vector(eval_ctx, VEC_UNIFORM, row_count))) {
      LOG_WARN("failed to initialize converted Result Catalog vector", K(ret));
    } else {
      ObDatum *to_datums =
          static_cast<ObUniformBase *>(to.get_vector(eval_ctx))->get_datums();
      for (int64_t i = 0; i < row_count; ++i) {
        to_datums[i] = *from_datum;
      }
    }
  } else if (from_header.format_ == VEC_UNIFORM) {
    ObDatum *src =
        static_cast<ObUniformBase *>(from.get_vector(eval_ctx))->get_datums();
    ObDatum *dst = to.locate_batch_datums(eval_ctx);
    if (src != dst) {
      MEMCPY(dst, src, row_count * sizeof(ObDatum));
    }
    if (OB_FAIL(to.init_vector(eval_ctx, VEC_UNIFORM, row_count))) {
      LOG_WARN("failed to initialize converted Result Catalog vector", K(ret));
    }
  } else if (OB_FAIL(to_header.assign(from_header))) {
    LOG_WARN("failed to assign converted Result Catalog vector", K(ret));
  }
  if (OB_SUCC(ret)) {
    to.set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::convert_columns(
    const int64_t row_count,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &dependent_exprs = *scan_param_->ext_column_dependent_exprs_;
  skip_vector_->reset(eval_ctx.max_batch_size_);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
    if (column_need_conv_.at(i)) {
      ObExpr *from = dependent_exprs.at(i);
      ObExpr *to = column_exprs_.at(i);
      if (OB_ISNULL(from) || OB_ISNULL(to)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null Result Catalog conversion expression", K(ret), K(i));
      } else {
        if (!from->get_eval_info(eval_ctx).is_evaluated(eval_ctx)) {
          if (OB_FAIL(from->init_vector_default(eval_ctx, row_count))) {
            LOG_WARN("failed to initialize Result Catalog conversion", K(ret), K(i));
          } else if (OB_FAIL(from->eval_vector(
                         eval_ctx, *skip_vector_, row_count, true))) {
            LOG_WARN("failed to evaluate Result Catalog conversion", K(ret), K(i));
          } else {
            from->set_evaluated_projected(eval_ctx);
          }
        }
        if (OB_SUCC(ret)
            && OB_FAIL(assign_converted_column(eval_ctx, *from, *to, row_count))) {
          LOG_WARN("failed to assign Result Catalog conversion", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::read_batch(
    int64_t &count,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  count = std::min<int64_t>(
      std::min<int64_t>(capacity, eval_ctx.max_batch_size_),
      row_group_row_count_ - read_rows_in_group_);
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid Result Catalog batch length", K(ret), K(count), K(capacity));
  } else {
    scan_param_->op_->clear_evaluated_flag();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_readers_.count(); ++i) {
      if (OB_FAIL(read_column(i, count, eval_ctx))) {
        LOG_WARN("failed to read Result Catalog projected column", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_file_meta_columns(count, eval_ctx))) {
      LOG_WARN("failed to fill Result Catalog file metadata", K(ret));
    } else if (OB_FAIL(convert_columns(count, eval_ctx))) {
      LOG_WARN("failed to convert Result Catalog columns", K(ret));
    } else if (OB_FAIL(calc_exprs_for_rowid(count, state_))) {
      LOG_WARN("failed to fill Result Catalog row identifiers", K(ret));
    } else {
      read_rows_in_group_ += count;
    }
  }
  return ret;
}

int ObAsyncResultParquetTableRowIterator::get_next_rows(
    int64_t &count,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(scan_param_) || capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Result Catalog batch request", K(ret), K(capacity));
  } else {
    try {
      if (read_rows_in_group_ >= row_group_row_count_
          && OB_FAIL(next_row_group())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to advance Result Catalog row group", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(read_batch(count, capacity))) {
        LOG_WARN("failed to read Result Catalog batch", K(ret));
      }
    } catch (const ObErrorCodeException &ex) {
      ret = ex.get_error_code();
      LOG_WARN("Result Catalog object read failed", K(ret));
    } catch (const std::bad_alloc &ex) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("out of memory reading Result Catalog Parquet", K(ret), "info", ex.what());
    } catch (const std::exception &ex) {
      ret = OB_INVALID_EXTERNAL_FILE;
      LOG_WARN("caught exception reading Result Catalog Parquet",
               K(ret), "info", ex.what());
    } catch (...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught unknown exception reading Result Catalog Parquet", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
