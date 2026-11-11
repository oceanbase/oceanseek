/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_input_writer.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_capture_pipeline.h"
#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/direct_load/ob_direct_load_batch_datum_rows.h"
#include "share/ob_ddl_common.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;

// All slices reached by one PX worker have the same schema and snapshot.
// Reuse the input workspace after synchronously consuming the previous slice.
struct ObIVFInputWriter::InputBuffer
{
  InputBuffer() : arena_(ObMemAttr(MTL_ID(), "IVFInputView")), rows_(nullptr), owner_(nullptr) {}
  ~InputBuffer()
  {
    if (nullptr != rows_) {
      OB_DELETE(ObDirectLoadBatchDatumRows, "IVFInputRows", rows_);
    }
  }
  ObArenaAllocator arena_;
  blocksstable::ObBatchDatumRows batch_;
  ObDirectLoadBatchDatumRows *rows_;
  TabletWriter *owner_;
};

class ObIVFInputWriter::TabletWriter final : public ObCsSliceWriter
{
public:
  explicit TabletWriter(InputBuffer &input)
    : capture_(nullptr), input_(input), submitted_row_count_(0), borrowed_batch_count_(0) {}
  virtual ~TabletWriter()
  {
    if (input_.owner_ == this) {
      input_.owner_ = nullptr;
    }
  }

  int init(const ObWriteMacroParam &param, const bool direct_write_macro_block,
           const bool is_append_batch)
  {
    int ret = OB_SUCCESS;
    ObDDLTabletContext *tablet_context = nullptr;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
    } else if (!param.is_valid() || nullptr == param.ddl_dag_ || direct_write_macro_block
               || param.ddl_table_schema_.table_item_.vec_dim_ <= 0
               || !schema::is_async_ivf_index(param.ddl_table_schema_.table_item_.index_type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF batch writer arguments", K(ret), K(param));
    } else {
      // Reuse CS conversion for scalar/reshape input. Ordinary SQL batches
      // are consumed synchronously without an intermediate payload copy.
      writer_param_ = param;
      tablet_id_ = param.tablet_id_;
      slice_idx_ = param.slice_idx_;
      rowkey_column_count_ = param.ddl_table_schema_.table_item_.rowkey_column_num_;
      sql_column_count_ = param.ddl_table_schema_.column_items_.count()
          - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      need_convert_storage_column_ = !param.ddl_table_schema_.lob_column_idxs_.empty()
          || !param.ddl_table_schema_.reshape_column_idxs_.empty();
      need_check_rowkey_order_ = false;
      const int64_t estimated_row_bytes = param.ddl_table_schema_.table_item_.vec_dim_ * sizeof(float)
          + param.ddl_table_schema_.column_items_.count() * sizeof(ObDatum);
      const int64_t target_batch_bytes = 1L << 20;
      writer_param_.max_batch_size_ = MAX(1L, MIN(ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE,
                                                target_batch_bytes / estimated_row_bytes));
      if (!is_append_batch && need_convert_storage_column_
          && OB_FAIL(ObDDLUtil::init_datum_row_with_snapshot(
              param.ddl_table_schema_.column_items_.count(), rowkey_column_count_,
              param.snapshot_version_, current_row_))) {
        LOG_WARN("init IVF input conversion row failed", K(ret));
      } else if (OB_FAIL(param.ddl_dag_->get_tablet_context(param.tablet_id_, tablet_context))) {
        LOG_WARN("get IVF capture tablet context failed", K(ret));
      } else if (OB_ISNULL(tablet_context)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(capture_.init(*tablet_context, param.ddl_table_schema_, param.slice_idx_,
                         param.ddl_dag_->get_ddl_task_param().is_async_index_refresh_))) {
        LOG_WARN("init synchronous IVF capture failed", K(ret));
      } else {
        is_inited_ = true;
      }
    }
    return ret;
  }

  int get_row_buffer(ObDirectLoadBatchRows *&rows, blocksstable::ObBatchDatumRows *&batch) override
  {
    int ret = OB_SUCCESS;
    rows = nullptr;
    batch = nullptr;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (nullptr != input_.rows_ && input_.rows_->batch_rows_.size() > 0
               && input_.owner_ != this) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF input buffer belongs to another slice", K(ret), K_(tablet_id), K_(slice_idx));
    } else if (nullptr == input_.rows_) {
      ObDirectLoadBatchDatumRows *buffer = OB_NEW(ObDirectLoadBatchDatumRows, ObMemAttr(MTL_ID(), "IVFInputRows"));
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(ObDDLUtil::init_batch_rows(writer_param_.ddl_table_schema_,
                                                  writer_param_.max_batch_size_, buffer->batch_rows_))) {
        LOG_WARN("init IVF input row buffer failed", K(ret));
      } else if (OB_FAIL(init_storage_batch_rows(buffer->allocator_, buffer->datum_rows_))) {
        LOG_WARN("init IVF input storage vectors failed", K(ret));
      } else {
        const auto &vectors = buffer->batch_rows_.get_vectors();
        const int64_t extra_columns = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        for (int64_t i = 0; i < vectors.count(); ++i) {
          const int64_t stored_idx = i < rowkey_column_count_ ? i : i + extra_columns;
          buffer->datum_rows_.vectors_.at(stored_idx) = vectors.at(i)->get_vector();
        }
        input_.rows_ = buffer;
      }
      if (OB_FAIL(ret) && nullptr != buffer) {
        OB_DELETE(ObDirectLoadBatchDatumRows, "IVFInputRows", buffer);
      }
    }
    if (OB_SUCC(ret)) {
      input_.owner_ = this;
      rows = &input_.rows_->batch_rows_;
      batch = &input_.rows_->datum_rows_;
    }
    return ret;
  }

  int append_current_batch(const ObIArray<ObIVector *> &vectors, ObBatchSelector &selector) override
  {
    int ret = OB_SUCCESS;
    const bool contiguous = ObBatchSelector::CONTINIOUS_LENGTH == selector.get_type();
    const int64_t begin = contiguous ? selector.get_offset() : 0;
    const int64_t count = selector.size();
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(vectors.count() != sql_column_count_ || !selector.is_valid()
                          || !contiguous || begin < 0 || count < 0 || begin > INT64_MAX - count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF input batch", K(ret), K(selector), K(vectors.count()));
    } else if (need_convert_storage_column_
               || !writer_param_.ddl_table_schema_.table_item_.is_index_table_
               || writer_param_.ddl_table_schema_.table_item_.has_lob_rowkey_) {
      // Preserve conversion and rowkey validation on the existing buffered path.
      if (OB_FAIL(ObCsSliceWriter::append_current_batch(vectors, selector))) {
        LOG_WARN("convert IVF input batch failed", K(ret));
      } else if (OB_FAIL(flush_pending_batch())) {
        LOG_WARN("consume converted IVF input batch failed", K(ret));
      }
    } else if (OB_FAIL(flush_pending_batch())) {
      LOG_WARN("flush pending scalar IVF input failed", K(ret));
    } else if (input_.batch_.vectors_.empty()
               && OB_FAIL(init_storage_batch_rows(input_.arena_, input_.batch_))) {
      LOG_WARN("init IVF input column view failed", K(ret));
      input_.batch_.reset();
      input_.arena_.reset();
    } else {
      const int64_t extra_columns = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      for (int64_t i = 0; i < vectors.count(); ++i) {
        input_.batch_.vectors_.at(i < rowkey_column_count_ ? i : i + extra_columns) = vectors.at(i);
      }
      input_.batch_.row_count_ = begin + count;
      // The selector may start inside a SQL batch. Capture must inspect only
      // this slice's rows, and must finish before SQL reuses its vectors.
      if (OB_FAIL(consume_batch(input_.batch_, begin, begin + count))) {
        LOG_WARN("consume borrowed IVF input batch failed", K(ret), K(selector));
      } else {
        ++borrowed_batch_count_;
      }
    }
    return ret;
  }

  int output_row_buffer(const bool is_end) override
  {
    UNUSED(is_end);
    int ret = OB_SUCCESS;
    if (OB_ISNULL(input_.rows_) || input_.owner_ != this || input_.rows_->datum_rows_.row_count_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF input buffer owner or rows", K(ret), K_(tablet_id), K_(slice_idx));
    } else if (OB_FAIL(consume_batch(input_.rows_->datum_rows_, 0, input_.rows_->datum_rows_.row_count_))) {
      LOG_WARN("consume buffered IVF input batch failed", K(ret));
    } else {
      input_.rows_->batch_rows_.reuse();
      input_.rows_->datum_rows_.row_count_ = 0;
      input_.owner_ = nullptr;
    }
    return ret;
  }

  int flush_pending_batch()
  {
    return nullptr == input_.rows_ || input_.rows_->batch_rows_.size() == 0
        ? OB_SUCCESS : flush_row_buffer(false /*is_end*/);
  }

  int get_row_count(int64_t &row_count) override
  {
    row_count = submitted_row_count_ + (input_.owner_ == this ? input_.rows_->batch_rows_.size() : 0);
    return OB_SUCCESS;
  }

  int close() override
  {
    int ret = OB_SUCCESS;
    // Map iteration order need not match input order. Only the owning slice
    // may consume the last pending batch before sealing its Replay.
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (input_.owner_ == this && OB_FAIL(flush_pending_batch())) {
      LOG_WARN("flush last IVF input batch failed", K(ret));
    } else if (nullptr != lob_writer_ && OB_FAIL(lob_writer_->close())) {
      LOG_WARN("close IVF input lob writer failed", K(ret));
    } else if (OB_FAIL(capture_.seal())) {
      LOG_WARN("seal synchronous IVF capture failed", K(ret), K_(tablet_id), K_(slice_idx));
    } else {
      FLOG_INFO("synchronous IVF input captured", K_(tablet_id), K_(slice_idx),
                K_(submitted_row_count), K_(borrowed_batch_count));
      is_inited_ = false;
    }
    return ret;
  }
private:
  int consume_batch(const blocksstable::ObBatchDatumRows &batch, const int64_t begin, const int64_t end)
  {
    int ret = OB_SUCCESS;
    if (writer_param_.ddl_dag_->is_final_status()) {
      ret = writer_param_.ddl_dag_->get_dag_ret();
      ret = COVER_SUCC(OB_CANCELED);
    } else if (OB_FAIL(capture_.consume_batch(batch, begin, end))) {
      LOG_WARN("synchronous IVF capture failed", K(ret), K_(tablet_id), K_(slice_idx));
    } else {
      submitted_row_count_ += end - begin;
    }
    return ret;
  }

  ObIVFCaptureOperator capture_;
  InputBuffer &input_;
  int64_t submitted_row_count_;
  int64_t borrowed_batch_count_;
};

ObIVFInputWriter::ObIVFInputWriter()
    : dag_(nullptr), active_writer_(nullptr), input_buffer_(nullptr), is_inited_(false),
      is_append_batch_(false), is_closed_(false), close_ret_(OB_SUCCESS)
{}

ObIVFInputWriter::~ObIVFInputWriter()
{
  release_writers();
}

int ObIVFInputWriter::get_writer(ObColumnClusteredDag &dag,
                                 const ObTabletID &tablet_id,
                                 const int64_t slice_idx,
                                 const bool is_append_batch,
                                 ObISliceWriter *&writer)
{
  int ret = OB_SUCCESS;
  ObIVFInputWriter *input_writer = static_cast<ObIVFInputWriter *>(writer);
  if (nullptr == input_writer) {
    if (OB_ISNULL(input_writer = OB_NEW(ObIVFInputWriter, ObMemAttr(MTL_ID(), "IVFInputWriter")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate IVF input writer failed", K(ret));
    } else if (OB_FAIL(input_writer->init(dag, is_append_batch))) {
      LOG_WARN("init IVF input writer failed", K(ret), K(slice_idx));
      OB_DELETE(ObIVFInputWriter, "IVFInputWriter", input_writer);
      input_writer = nullptr;
    } else {
      writer = input_writer;
    }
  } else if (input_writer->dag_ != &dag
             || input_writer->is_append_batch_ != is_append_batch) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF input writer context changed", K(ret), K(slice_idx), KPC(input_writer));
  }
  if (OB_SUCC(ret) && OB_FAIL(input_writer->select_slice(tablet_id, slice_idx))) {
    LOG_WARN("select IVF input tablet failed", K(ret), K(tablet_id));
  }
  return ret;
}

int ObIVFInputWriter::init(ObColumnClusteredDag &dag, const bool is_append_batch)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!schema::is_async_ivf_index(dag.get_ddl_table_schema().table_item_.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF input writer arguments", K(ret));
  } else if (OB_FAIL(writers_.create(128, ObMemAttr(MTL_ID(), "IVFInputWriters")))) {
    LOG_WARN("create IVF input writer map failed", K(ret));
  } else if (OB_ISNULL(input_buffer_ = OB_NEW(InputBuffer, ObMemAttr(MTL_ID(), "IVFInputBuffer")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate IVF input buffer failed", K(ret));
  } else {
    dag_ = &dag;
    is_append_batch_ = is_append_batch;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFInputWriter::select_slice(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  TabletWriter *writer = nullptr;
  const SliceKey key(tablet_id, slice_idx);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (is_closed_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (!tablet_id.is_valid() || slice_idx < 0 || slice_idx >= ObTabletSliceParam::MAX_TABLET_SLICE_COUNT) {
    ret = OB_INVALID_ARGUMENT;
  } else if (nullptr != active_writer_ && tablet_id_ == tablet_id && slice_idx_ == slice_idx) {
    // Keep appending to the current tablet.
  } else if (nullptr != active_writer_ && OB_FAIL(active_writer_->flush_pending_batch())) {
    LOG_WARN("flush IVF input before tablet switch failed", K(ret), K_(tablet_id));
  } else {
    if (OB_FAIL(writers_.get_refactored(key, writer))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get IVF tablet writer failed", K(ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
        ObWriteMacroParam param;
        if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id, slice_idx, -1 /*cg_idx*/, dag_, param))) {
          LOG_WARN("prepare IVF tablet writer failed", K(ret), K(tablet_id));
        } else if (OB_ISNULL(writer = OB_NEW(TabletWriter, ObMemAttr(MTL_ID(), "IVFInputSlice"), *input_buffer_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate IVF tablet writer failed", K(ret));
        } else if (OB_FAIL(writer->init(param, false /*direct_write_macro_block*/, is_append_batch_))) {
          LOG_WARN("init IVF tablet writer failed", K(ret), K(tablet_id));
        } else if (OB_FAIL(writers_.set_refactored(key, writer))) {
          LOG_WARN("retain IVF tablet writer failed", K(ret), K(tablet_id));
        }
        if (OB_FAIL(ret) && nullptr != writer) {
          OB_DELETE(TabletWriter, "IVFInputSlice", writer);
          writer = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF tablet writer is null", K(ret), K(tablet_id));
      } else {
        active_writer_ = writer;
        tablet_id_ = tablet_id;
        slice_idx_ = slice_idx;
      }
    }
  }
  return ret;
}

int ObIVFInputWriter::append_current_row(const ObIArray<ObDatum *> &datums)
{
  return nullptr == active_writer_ ? OB_NOT_INIT : active_writer_->append_current_row(datums);
}

int ObIVFInputWriter::append_current_batch(const ObIArray<ObIVector *> &vectors,
                                           ObBatchSelector &selector)
{
  return nullptr == active_writer_ ? OB_NOT_INIT : active_writer_->append_current_batch(vectors, selector);
}

int ObIVFInputWriter::get_row_count(int64_t &row_count)
{
  row_count = 0;
  return nullptr == active_writer_ ? OB_NOT_INIT : active_writer_->get_row_count(row_count);
}

int ObIVFInputWriter::close()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (is_closed_) {
    ret = close_ret_;
  } else {
    for (WriterMap::iterator it = writers_.begin(); OB_SUCC(ret) && it != writers_.end(); ++it) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF tablet writer is null", K(ret), K(it->first));
      } else if (OB_FAIL(it->second->close())) {
        LOG_WARN("close IVF tablet writer failed", K(ret), K(it->first));
      }
    }
    if (OB_FAIL(ret)) {
      dag_->set_ret_code(ret);
    }
    // Release input buffers before the longer-running build starts.
    release_writers();
    is_closed_ = true;
    close_ret_ = ret;
  }
  return ret;
}

void ObIVFInputWriter::release_writers()
{
  if (writers_.created()) {
    for (WriterMap::iterator it = writers_.begin(); it != writers_.end(); ++it) {
      if (nullptr != it->second) {
        OB_DELETE(TabletWriter, "IVFInputSlice", it->second);
      }
    }
    writers_.destroy();
  }
  active_writer_ = nullptr;
  if (nullptr != input_buffer_) {
    OB_DELETE(InputBuffer, "IVFInputBuffer", input_buffer_);
    input_buffer_ = nullptr;
  }
}
