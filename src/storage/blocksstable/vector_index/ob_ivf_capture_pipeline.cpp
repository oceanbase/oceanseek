/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_capture_pipeline.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/ddl/ob_cg_row_tmp_file.h"
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include <cmath>

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObIVFCaptureOperator::init(
    ObDDLTabletContext &tablet_context,
    const ObDDLTableSchema &ddl_schema,
    const int64_t slice_idx,
    const bool is_async_index_refresh)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam vector_param;
  const ObHcaTrainParam hca_train_param;
  static const int64_t IVF_ROWKEY_COLUMN_COUNT = 3;
  static const int64_t IVF_STORED_COLUMN_COUNT = 4;
  int64_t max_sample_count = 0;
  int64_t max_sample_clusters = 0;
  int64_t samples_per_cluster = 0;
  uint64_t sample_seed = 0;
  uint64_t data_column_id = OB_INVALID_ID;
  int64_t data_column_count = 0;
  int64_t stored_column_count = 0;
  // Incremental refresh retains its existing capture threshold. INITIAL_BUILD
  // always obtains the configured tenant-wide budget through ensure_capture_budget.
  int64_t replay_memory_limit = is_async_index_refresh
      ? 64L << 20 : ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT;
  if (is_inited_ || nullptr == tablet_context.ivf_ctx_ || slice_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture initialization argument", K(ret),
             K_(is_inited), KP(tablet_context.ivf_ctx_), K(slice_idx),
             "table_id", ddl_schema.table_id_);
  } else if (OB_UNLIKELY(ddl_schema.column_descs_.count()
                         != ddl_schema.column_items_.count())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF DDL column metadata count mismatch", K(ret),
             "column_desc_count", ddl_schema.column_descs_.count(),
             "column_item_count", ddl_schema.column_items_.count(), K(ddl_schema));
  } else if (OB_UNLIKELY(IVF_ROWKEY_COLUMN_COUNT
                         != ddl_schema.table_item_.rowkey_column_num_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF transport rowkey count mismatch", K(ret),
             "rowkey_column_count", ddl_schema.table_item_.rowkey_column_num_,
             K(ddl_schema));
  }

  // Physical async-IVF role bits are not persisted. The IVF transport
  // contract has exactly one non-rowkey VARCHAR payload column; locate it
  // from the transport metadata instead of relying on the transient DATA bit.
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_schema.column_descs_.count(); ++i) {
    const ObColumnSchemaItem &column_item = ddl_schema.column_items_.at(i);
    if (column_item.is_valid_) {
      ++stored_column_count;
      if (!column_item.is_rowkey_column_
          && ObVarcharType == column_item.col_type_.get_type()) {
        ++data_column_count;
        data_column_id = ddl_schema.column_descs_.at(i).col_id_;
        data_col_idx_ = i;
      }
    }
  }
  if (OB_SUCC(ret)
      && OB_UNLIKELY(IVF_STORED_COLUMN_COUNT != stored_column_count
                     || 1 != data_column_count)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF transport column contract mismatch", K(ret),
             K(stored_column_count), K(data_column_count), K(data_column_id),
             K(ddl_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_schema.column_descs_.count(); ++i) {
    const uint64_t column_id = ddl_schema.column_descs_.at(i).col_id_;
    if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
      if (OB_UNLIKELY(vid_col_idx_ >= 0)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("duplicate IVF VID transport column", K(ret), K(column_id),
                 K(vid_col_idx_), K(i), K(ddl_schema));
      } else {
        vid_col_idx_ = i;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (vid_col_idx_ < 0 || data_col_idx_ < 0
             || vid_col_idx_ == data_col_idx_
             || !ddl_schema.column_items_.at(vid_col_idx_).is_valid_
             || !ddl_schema.column_items_.at(vid_col_idx_).is_rowkey_column_
             || ObUInt64Type
                  != ddl_schema.column_items_.at(vid_col_idx_).col_type_.get_type()
             || !ddl_schema.column_items_.at(data_col_idx_).is_valid_
             || ddl_schema.column_items_.at(data_col_idx_).is_rowkey_column_
             || ObVarcharType
                  != ddl_schema.column_items_.at(data_col_idx_).col_type_.get_type()
             || ddl_schema.table_item_.vec_dim_ <= 0) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF transport columns are missing or invalid", K(ret),
             K(vid_col_idx_), K(data_col_idx_), K(data_column_id), K(ddl_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                 ddl_schema.table_item_.vec_idx_param_,
                 ObVectorIndexType::VIT_IVF_INDEX, vector_param, true))) {
    LOG_WARN("failed to parse IVF capture sample parameters", K(ret),
             "index_params", ddl_schema.table_item_.vec_idx_param_);
  } else if (FALSE_IT(max_sample_clusters = is_async_index_refresh
                 ? vector_param.nlist_ : hca_train_param.max_local_k_)) {
  } else if (FALSE_IT(samples_per_cluster = is_async_index_refresh
                 ? vector_param.sample_per_nlist_ : hca_train_param.samples_per_cluster_)) {
  } else if (OB_UNLIKELY(max_sample_clusters <= 0 || samples_per_cluster <= 0
             || INT64_MAX / samples_per_cluster < max_sample_clusters
             || (!is_async_index_refresh && !hca_train_param.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture sample parameters", K(ret),
             K(is_async_index_refresh), K(max_sample_clusters), K(samples_per_cluster));
  } else if (FALSE_IT(max_sample_count = max_sample_clusters * samples_per_cluster)) {
    // Initial build consumes HCA samples; incremental build still consumes
    // KMeans samples. Capture must use the budget of its downstream consumer.
  } else if (FALSE_IT(sample_seed = ObIVFReplayInput::build_sample_seed(
                 ddl_schema.table_id_, tablet_context.tablet_id_.id()))) {
  } else if (FALSE_IT(refresh_monitor_ = tablet_context.ivf_ctx_->refresh_monitor_)) {
  } else if (FALSE_IT(vector_dim_ = ddl_schema.table_item_.vec_dim_)) {
  } else if (FALSE_IT(is_cosine_ =
                 VIDA_COS == vector_param.dist_algorithm_)) {
  } else if (FALSE_IT(rotator_ =
                 &tablet_context.ivf_ctx_->get_rotator())) {
  } else if (OB_UNLIKELY(!rotator_->is_inited()
             || tablet_context.ivf_ctx_->get_rotation_dim() != vector_dim_
             || vector_dim_ > INT32_MAX / static_cast<int64_t>(sizeof(float)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF capture rotator is invalid", K(ret),
             K_(vector_dim),
             "rotation_dim", tablet_context.ivf_ctx_->get_rotation_dim());
  } else if (!is_async_index_refresh
             && OB_FAIL(tablet_context.ivf_ctx_->ensure_capture_budget(
                 tablet_context.slice_count_, replay_memory_limit))) {
    LOG_WARN("failed to reserve IVF capture replay cache budget", K(ret),
             K(slice_idx), K(tablet_context.slice_count_));
  } else if (OB_ISNULL(slice_input_ = OB_NEW(ObIVFSliceInput,
                                              ObMemAttr(MTL_ID(), "IVFSlice"),
                                              slice_idx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF slice input", K(ret), K(slice_idx));
  } else if (OB_FAIL(slice_input_->replay_.init(MTL_ID(),
                 vector_dim_, replay_memory_limit,
                 max_sample_count, sample_seed, slice_idx,
                 !is_async_index_refresh /* capture_vids */))) {
    LOG_WARN("failed to initialize IVF replay input", K(ret), K(slice_idx));
  } else if (OB_FAIL(tablet_context.ivf_ctx_->register_slice(slice_input_))) {
    LOG_WARN("failed to register IVF replay input", K(ret), K(slice_idx));
  } else {
    is_inited_ = true;
    LOG_DEBUG("IVF capture pipeline initialized", K(slice_idx),
              K(vid_col_idx_), K(data_col_idx_), K(data_column_id),
              K(ddl_schema.table_id_), K(max_sample_count), K(sample_seed));
  }
  if (OB_FAIL(ret) && nullptr != slice_input_) {
    OB_DELETE(ObIVFSliceInput, "IVFSlice", slice_input_);
    slice_input_ = nullptr;
  }
  return ret;
}

int ObIVFCaptureOperator::consume_batch_(const ObBatchDatumRows &batch)
{
  int ret = OB_SUCCESS;
  const int64_t before_rows = nullptr == slice_input_ ? 0 : slice_input_->replay_.get_row_count();
  if (OB_UNLIKELY(batch.row_count_ < 0
      || (batch.row_count_ > 0 && batch.vectors_.empty())
      || vid_col_idx_ < 0 || vid_col_idx_ >= batch.vectors_.count()
      || data_col_idx_ < 0 || data_col_idx_ >= batch.vectors_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture batch", K(ret), K(batch),
             K(vid_col_idx_), K(data_col_idx_));
  } else {
    row_allocator_.reuse();
    ObDatumRow row;
    if (OB_FAIL(row.init(row_allocator_, batch.vectors_.count()))) {
      LOG_WARN("failed to initialize IVF transport row", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < batch.row_count_; ++i) {
      if (OB_FAIL(batch.to_datum_row(i, row))) {
        LOG_WARN("failed to materialize IVF transport row", K(ret), K(i));
      } else if (row.storage_datums_[data_col_idx_].is_null()) {
        // A NULL source vector yields a NULL generated DATA value. It is not
        // part of the refresh input and must not enter replay/sampling.
      } else if (row.storage_datums_[vid_col_idx_].is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-null IVF DATA has null VID", K(ret), K(i),
                 K(vid_col_idx_), K(data_col_idx_));
      } else {
        ObString vector_bytes = row.storage_datums_[data_col_idx_].get_string();
        const int64_t stored_vector_bytes = vector_bytes.length();
        const int64_t vector_buffer_size =
            vector_dim_ * static_cast<int64_t>(sizeof(float));
        float *rotated_vector = nullptr;
        vector_allocator_.reuse();
        if (OB_UNLIKELY(vector_bytes.length()
                   != vector_dim_ * static_cast<int64_t>(sizeof(float)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("malformed IVF source vector", K(ret), K(i),
                   K_(vector_dim), "vector_bytes", vector_bytes.length());
        }
        bool is_finite = true;
        for (int64_t d = 0; OB_SUCC(ret) && is_finite && d < vector_dim_; ++d) {
          float value = 0.0F;
          MEMCPY(&value, vector_bytes.ptr() + d * sizeof(float), sizeof(value));
          is_finite = std::isfinite(value);
        }
        if (OB_SUCC(ret) && !is_finite) {
          // Like NULL, a source vector containing Inf/NaN is not indexable.
          // Skip before sampling/normalization/rotation; the deletion index
          // still invalidates any previously indexed version of this row.
          LOG_TRACE("non-finite IVF source vector", K(ret), K(i), K(row));
          continue;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(rotated_vector = static_cast<float *>(
                               vector_allocator_.alloc(vector_buffer_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate rotated IVF vector", K(ret), K(i),
                   K_(vector_dim), K(vector_buffer_size));
        } else {
          // The source datum belongs to the input batch. Keep a separate
          // writable copy for in-place rotation and replay ownership.
          MEMCPY(rotated_vector, vector_bytes.ptr(), vector_bytes.length());
          // Persist unit vectors for COSINE so later HCA merges use
          // the same normalized space as incremental training.
          if (is_cosine_ && OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                  vector_dim_, rotated_vector, rotated_vector, nullptr))) {
            LOG_WARN("failed to normalize captured IVF vector", K(ret),
                     K(i), K_(vector_dim));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rotator_->rotate(vector_dim_, rotated_vector))) {
          LOG_WARN("failed to rotate captured IVF vector", K(ret),
                   K(i), K_(vector_dim));
        } else {
          vector_bytes.assign_ptr(reinterpret_cast<char *>(rotated_vector),
                                  static_cast<int32_t>(vector_buffer_size));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(slice_input_->replay_.add_row(
                       row.storage_datums_[vid_col_idx_].get_uint(),
                       vector_bytes))) {
          LOG_WARN("failed to capture IVF row", K(ret), K(i),
                   K(stored_vector_bytes),
                   "real_vector_bytes", vector_bytes.length());
        }
      }
    }
  }
  if (nullptr != refresh_monitor_ && nullptr != slice_input_) {
    refresh_monitor_->add_completed(share::ObIVFRefreshProgress::CAPTURE,
        slice_input_->replay_.get_row_count() - before_rows);
  }
  return ret;
}

int ObIVFCaptureOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = NEED_MORE_INPUT;
  output_chunk.reset();
  if (!is_inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF capture operator is not initialized", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // The slice terminator seals the replay in try_execute_finish().
  } else if (!input_chunk.is_cg_row_tmp_files_type()
             || nullptr == input_chunk.cg_row_file_arr_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture input chunk", K(ret), K(input_chunk));
  } else {
    ObArray<ObCGRowFile *> &row_files = *input_chunk.cg_row_file_arr_;
    for (int64_t f = 0; OB_SUCC(ret) && f < row_files.count(); ++f) {
      ObCGRowFile *row_file = row_files.at(f);
      ObBatchDatumRows *batch = nullptr;
      if (OB_ISNULL(row_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF capture row file is null", K(ret), K(f));
      } else if (0 != row_file->get_cg_idx()) {
        continue;
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_file->get_next_batch(batch))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to read IVF capture batch", K(ret), K(f),
                     "cg_idx", row_file->get_cg_idx());
          }
        } else if (OB_ISNULL(batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("IVF capture batch is null", K(ret), K(f));
        } else if (OB_FAIL(consume_batch_(*batch))) {
          LOG_WARN("failed to consume IVF CG row batch", K(ret), K(f),
                   "cg_idx", row_file->get_cg_idx());
        }
      }
    }
  }
  return ret;
}

int ObIVFCaptureOperator::try_execute_finish(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(slice_input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF capture slice input is null", K(ret));
  } else if (!input_chunk.is_end_chunk()) {
    // Capture chunks remain writable until the explicit slice terminator.
  } else if (OB_FAIL(slice_input_->replay_.seal())) {
    LOG_WARN("failed to seal IVF capture replay", K(ret));
  } else {
    LOG_DEBUG("IVF capture replay sealed",
              "slice_idx", slice_input_->slice_idx_,
              "row_count", slice_input_->replay_.get_row_count(),
              "sample_count", slice_input_->replay_.get_sample_count(),
              "max_sample_count", slice_input_->replay_.get_max_sample_count());
  }
  if (OB_SUCC(ret)
      && OB_FAIL(ObPipelineOperator::try_execute_finish(
             input_chunk, result_state, output_chunk))) {
    LOG_WARN("failed to finish IVF capture operator", K(ret));
  }
  return ret;
}

int ObIVFCapturePipeline::init(
    ObDDLSlice *ddl_slice,
    ObDDLTabletContext *tablet_context,
    const ObDDLTableSchema &ddl_schema,
    const bool is_async_index_refresh)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_slice) || OB_ISNULL(tablet_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture pipeline argument", K(ret), KP(ddl_slice), KP(tablet_context));
  } else {
    ddl_slice_ = ddl_slice;
    if (OB_FAIL(capture_op_.init(*tablet_context, ddl_schema,
                                 ddl_slice->get_slice_idx(), is_async_index_refresh))) {
      LOG_WARN("failed to initialize IVF capture operator", K(ret));
    } else if (OB_FAIL(add_op(&capture_op_))) {
      LOG_WARN("failed to add IVF capture operator", K(ret));
    }
  }
  return ret;
}

int ObIVFCapturePipeline::set_remain_block()
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
  ObDDLTabletContext *tablet_context = nullptr;
  if (OB_ISNULL(dag) || OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF capture pipeline state", K(ret), KP(dag), KP_(ddl_slice));
  } else if (OB_FAIL(dag->get_tablet_context(ddl_slice_->get_tablet_id(),
                                             tablet_context))) {
    LOG_WARN("failed to get IVF tablet context", K(ret));
  } else if (OB_ISNULL(tablet_context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF tablet context is null", K(ret));
  } else {
    const ObStorageSchema *storage_schema = tablet_context->tablet_param_.with_cs_replica_
        ? tablet_context->tablet_param_.cs_replica_storage_schema_
        : tablet_context->tablet_param_.storage_schema_;
    if (OB_ISNULL(storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF storage schema is null", K(ret));
    } else {
      // Capture produces no remaining macro blocks for group write.
      // Keep the transport slice alive until the DDL tablet context reclaims it.
      for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < storage_schema->get_column_group_count(); ++cg_idx) {
        if (OB_FAIL(ddl_slice_->set_block_flushed(cg_idx))) {
          LOG_WARN("failed to mark captured IVF transport slice flushed", K(ret), K(cg_idx));
        }
      }
    }
  }
  return ret;
}

void ObIVFCapturePipeline::postprocess(int &ret_code)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
  if (OB_ITER_END != ret_code && OB_DAG_TASK_IS_SUSPENDED != ret_code) {
    FLOG_INFO("ret code not expected", K(ret_code), KPC(this), KPC(dag));
  } else if (OB_ITER_END == ret_code) {
    ret_code = OB_SUCCESS;
    if (OB_FAIL(set_remain_block())) {
      ret_code = ret;
      if (OB_NOT_NULL(dag)) {
        dag->set_ret_code(ret_code);
      }
      LOG_WARN("failed to finish IVF capture slice", K(ret_code));
    }
  }
  if (OB_DAG_TASK_IS_SUSPENDED != ret_code && OB_NOT_NULL(dag)) {
    dag->dec_pipeline_count();
  }
}
