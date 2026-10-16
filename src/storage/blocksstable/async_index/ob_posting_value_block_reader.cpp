/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObPostingValueBlockReader::ObPostingValueBlockReader()
  : header_(nullptr),
    row_data_(nullptr),
    row_index_(nullptr),
    row_idx_gen_(),
    row_idx_gen_inited_(false),
    is_inited_(false)
{
}

ObPostingValueBlockReader::~ObPostingValueBlockReader()
{
  reuse();
}

int ObPostingValueBlockReader::init(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting value block buffer", K(ret), KP(buf), K(size));
  } else if (OB_UNLIKELY(size < static_cast<int64_t>(sizeof(ObPostingValueBlockHeader)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting value block buffer too small", K(ret), K(size));
  } else {
    header_ = reinterpret_cast<const ObPostingValueBlockHeader *>(buf);
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid posting value block header", K(ret), KPC(header_));
    } else {
      const int64_t row_index_size = static_cast<int64_t>(header_->row_count_)
          * header_->row_idx_byte_width_;
      if (OB_UNLIKELY(header_->row_idx_offset_ < sizeof(ObPostingValueBlockHeader)
          || header_->row_idx_offset_ + row_index_size > static_cast<uint32_t>(size))) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid posting value block row index", K(ret), K(size), KPC(header_));
      } else {
        row_data_ = buf + sizeof(ObPostingValueBlockHeader);
        row_index_ = buf + header_->row_idx_offset_;
        if (OB_FAIL(row_idx_gen_.init(row_index_, header_->row_idx_byte_width_))) {
          STORAGE_LOG(WARN, "failed to init row index generator", K(ret), K(header_->row_idx_byte_width_));
        } else {
          row_idx_gen_inited_ = true;
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObPostingValueBlockReader::locate_row(
    const int64_t idx,
    const char *&row_buf,
    int64_t &row_len) const
{
  int ret = OB_SUCCESS;
  row_buf = nullptr;
  row_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting value block reader not inited", K(ret));
  } else if (OB_UNLIKELY(!row_idx_gen_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row index generator not inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row index", K(ret), K(idx), K(header_->row_count_));
  } else {
    const int64_t row_start = row_idx_gen_.get_array().at(idx);
    int64_t row_end = 0;
    if (idx + 1 < header_->row_count_) {
      row_end = row_idx_gen_.get_array().at(idx + 1);
    } else {
      row_end = static_cast<int64_t>(header_->row_idx_offset_)
                - static_cast<int64_t>(sizeof(ObPostingValueBlockHeader));
    }
    const int64_t row_data_size = static_cast<int64_t>(header_->row_idx_offset_)
        - static_cast<int64_t>(sizeof(ObPostingValueBlockHeader));
    if (OB_UNLIKELY(row_start < 0 || row_end < row_start || row_end > row_data_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected row offset", K(ret), K(row_start), K(row_end),
          K(row_data_size), K(idx));
    } else {
      row_buf = row_data_ + row_start;
      row_len = row_end - row_start;
    }
  }
  return ret;
}

int ObPostingValueBlockReader::get_col_value(
    const char *row_buf,
    const int64_t row_len,
    const int64_t col_idx,
    ObDatum &value) const
{
  int ret = OB_SUCCESS;
  value.reset();
  if (OB_ISNULL(row_buf) || OB_UNLIKELY(row_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row buffer", K(ret), KP(row_buf), K(row_len));
  } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= header_->column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index", K(ret), K(col_idx), K(header_->column_cnt_));
  } else if (OB_UNLIKELY(row_len < static_cast<int64_t>(sizeof(ObPostingValueRowHeader)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row buffer too small for row header", K(ret), K(row_len));
  } else {
    const ObPostingValueRowHeader *row_header =
        reinterpret_cast<const ObPostingValueRowHeader *>(row_buf);
    if (OB_UNLIKELY(!row_header->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid posting value row header", K(ret), KPC(row_header));
    } else {
      const int64_t col_idx_cnt = header_->column_cnt_ + 1;
      const int64_t col_idx_size = col_idx_cnt * row_header->col_idx_byte_width_;
      const int64_t col_data_offset = sizeof(ObPostingValueRowHeader) + col_idx_size;
      if (OB_UNLIKELY(row_len < col_data_offset)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "row buffer too small for column index", K(ret), K(row_len), K(col_data_offset));
      } else {
        ObIntegerArrayGenerator col_idx_gen;
        const char *col_idx_buf = row_buf + sizeof(ObPostingValueRowHeader);
        if (OB_FAIL(col_idx_gen.init(col_idx_buf, row_header->col_idx_byte_width_))) {
          STORAGE_LOG(WARN, "failed to init column index generator", K(ret), K(row_header->col_idx_byte_width_));
        } else {
          const int64_t col_start = col_idx_gen.get_array().at(col_idx);
          const int64_t col_end = col_idx_gen.get_array().at(col_idx + 1);
          if (OB_UNLIKELY(col_start < 0 || col_end < col_start
              || col_data_offset + col_end > row_len)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected column offset", K(ret), K(col_start), K(col_end), K(row_len));
          } else {
            value.ptr_ = row_buf + col_data_offset + col_start;
            value.len_ = static_cast<uint32_t>(col_end - col_start);
            value.pack_ = value.len_;
          }
        }
      }
    }
  }
  return ret;
}

int ObPostingValueBlockReader::get_position_list(const int64_t idx, ObDatum &value) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting value block reader not inited", K(ret));
  } else if (OB_UNLIKELY(!header_->has_position_list_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "posting value block has no position_list column", K(ret));
  } else {
    const char *row_buf = nullptr;
    int64_t row_len = 0;
    if (OB_FAIL(locate_row(idx, row_buf, row_len))) {
      STORAGE_LOG(WARN, "failed to locate value row", K(ret), K(idx));
    } else if (OB_FAIL(get_col_value(row_buf, row_len, header_->get_position_list_col_idx(), value))) {
      STORAGE_LOG(WARN, "failed to get position_list", K(ret), K(idx));
    }
  }
  return ret;
}

int ObPostingValueBlockReader::get_nested_key(const int64_t idx, ObDatum &value) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting value block reader not inited", K(ret));
  } else if (OB_UNLIKELY(!header_->has_nested_key_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "posting value block has no nested_key column", K(ret));
  } else {
    const char *row_buf = nullptr;
    int64_t row_len = 0;
    if (OB_FAIL(locate_row(idx, row_buf, row_len))) {
      STORAGE_LOG(WARN, "failed to locate value row", K(ret), K(idx));
    } else if (OB_FAIL(get_col_value(row_buf, row_len, header_->get_nested_key_col_idx(), value))) {
      STORAGE_LOG(WARN, "failed to get nested_key", K(ret), K(idx));
    } else if (0 == value.len_) {
      // A non-NULL nested-key list always contains a codec header and at least one key.
      // The posting-value format therefore uses zero bytes to preserve SQL NULL.
      value.set_null();
    }
  }
  return ret;
}

void ObPostingValueBlockReader::reuse()
{
  header_ = nullptr;
  row_data_ = nullptr;
  row_index_ = nullptr;
  row_idx_gen_inited_ = false;
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase
