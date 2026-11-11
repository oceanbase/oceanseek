/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

ObIvfMacroBlockWriter::ObIvfMacroBlockWriter()
  : ObMacroBlockWriter(true /* is_need_macro_buffer */),
    row_desc_(),
    previous_row_type_(-1),
    appended_row_count_(0)
{
}

ObIvfMacroBlockWriter::~ObIvfMacroBlockWriter()
{
  reset();
}

void ObIvfMacroBlockWriter::reset()
{
  ObMacroBlockWriter::reset();
  row_desc_.reset();
  previous_row_type_ = -1;
  appended_row_count_ = 0;
}

int ObIvfMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObISSTableObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    ObIMacroBlockValidator *validator,
    ObIODevice *device_handle)
{
  row_desc_.reset();
  previous_row_type_ = -1;
  appended_row_count_ = 0;
  return ObMacroBlockWriter::open(data_store_desc,
                                  parallel_idx,
                                  macro_seq_param,
                                  pre_warm_param,
                                  object_cleaner,
                                  callback,
                                  validator,
                                  device_handle);
}

int ObIvfMacroBlockWriter::open(
    const ObIvfSSTableRowDesc &row_desc,
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObISSTableObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    ObIMacroBlockValidator *validator,
    ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row descriptor", K(ret), K(row_desc));
  } else {
    row_desc_ = row_desc;
    previous_row_type_ = -1;
    appended_row_count_ = 0;
    if (OB_FAIL(ObMacroBlockWriter::open(data_store_desc,
                                         parallel_idx,
                                         macro_seq_param,
                                         pre_warm_param,
                                         object_cleaner,
                                         callback,
                                         validator,
                                         device_handle))) {
      LOG_WARN("failed to open IVF macro block writer", K(ret), K(row_desc));
      row_desc_.reset();
      previous_row_type_ = -1;
      appended_row_count_ = 0;
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::append_row(
    const ObDatumRow &row,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (!row_desc_.is_valid()) {
    if (OB_FAIL(ObMacroBlockWriter::append_row(row, curr_macro_desc))) {
      LOG_WARN("failed to append generic IVF row", K(ret), K(row));
    } else {
      ++appended_row_count_;
    }
  } else {
    ObBlockWriterConcurrentGuard guard(concurrent_lock_);
    if (OB_FAIL(inner_append_row(row, curr_macro_desc))) {
      LOG_WARN("failed to append IVF row", K(ret), K(row), K_(row_desc));
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::append_batch(
    const ObBatchDatumRows &datum_rows,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  if (OB_UNLIKELY(datum_rows.row_count_ < 0
      || (datum_rows.row_count_ > 0 && datum_rows.get_column_count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF batch rows", K(ret), K(datum_rows));
  } else if (0 == datum_rows.row_count_) {
  } else if (!row_desc_.is_valid()) {
    if (OB_FAIL(ObMacroBlockWriter::append_batch(datum_rows, curr_macro_desc))) {
      LOG_WARN("failed to append generic IVF batch", K(ret), K(datum_rows));
    } else {
      appended_row_count_ += datum_rows.row_count_;
    }
  } else if (OB_FAIL(row.init(datum_rows.get_column_count()))) {
    LOG_WARN("failed to initialize IVF batch row", K(ret),
             K(datum_rows.get_column_count()));
  } else {
    ObBlockWriterConcurrentGuard guard(concurrent_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; ++i) {
      if (OB_FAIL(datum_rows.to_datum_row(i, row))) {
        LOG_WARN("failed to materialize IVF batch row", K(ret), K(i), K(datum_rows));
      } else if (OB_FAIL(inner_append_row(row, curr_macro_desc))) {
        LOG_WARN("failed to append IVF batch row", K(ret), K(i), K(row));
      }
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::inner_append_row(
    const ObDatumRow &row,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  int64_t row_type = -1;
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF macro block writer is not opened", K(ret));
  } else if (OB_UNLIKELY(!row_desc_.is_valid_for_row(row.get_column_count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row", K(ret), K(row), K_(row_desc));
  } else if (OB_UNLIKELY(
                 row.storage_datums_[row_desc_.row_type_col_idx_].is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF SSTable row type is null", K(ret), K(row), K_(row_desc));
  } else if (FALSE_IT(row_type =
                 row.storage_datums_[row_desc_.row_type_col_idx_].get_int())) {
  } else if (OB_UNLIKELY(row_type < IVF_ROW_TYPE_CENTROID
                         || row_type >= IVF_ROW_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row type", K(ret), K(row_type), K(row));
  } else if (previous_row_type_ >= 0
             && previous_row_type_ != row_type
             && OB_FAIL(inner_flush_micro_block())) {
    LOG_WARN("failed to flush IVF row-type boundary", K(ret),
             K_(previous_row_type), K(row_type));
  } else if (OB_FAIL(ObMacroBlockWriter::append_row_inner(row, curr_macro_desc))) {
    LOG_WARN("failed to append row to IVF macro block writer", K(ret), K(row));
  } else {
    previous_row_type_ = row_type;
    ++appended_row_count_;
  }
  return ret;
}

int ObIvfMacroBlockWriter::flush_micro_block()
{
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  return inner_flush_micro_block();
}

int ObIvfMacroBlockWriter::inner_flush_micro_block()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_writer_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("micro writer is null", K(ret));
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() == 0)) {
    LOG_INFO("micro block has no rows, skip flush");
  } else if (OB_FAIL(build_micro_block())) {
    LOG_WARN("fail to flush current micro block", K(ret));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
