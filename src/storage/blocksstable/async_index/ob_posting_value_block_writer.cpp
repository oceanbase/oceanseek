/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObPostingValueBlockWriter::append_binary_col(
    const ObDatumRow &row,
    const int16_t col_idx,
    const bool null_as_empty,
    const char *&ptr,
    int64_t &len)
{
  int ret = OB_SUCCESS;
  ptr = nullptr;
  len = 0;
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index", K(ret), K(col_idx), K(row.get_column_count()));
  } else {
    const ObDatum &datum = row.storage_datums_[col_idx];
    if (OB_UNLIKELY(datum.is_nop()
        || (!null_as_empty && datum.is_null())
        || (null_as_empty && !datum.is_null() && 0 == datum.len_))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid binary value column", K(ret), K(col_idx), K(null_as_empty), K(datum));
    } else if (!datum.is_null()) {
      ptr = datum.ptr_;
      len = datum.len_;
    }
  }
  return ret;
}

ObPostingValueBlockWriter::ObPostingValueBlockWriter()
  : param_(nullptr),
    allocator_(nullptr),
    data_buffer_(),
    row_offsets_(),
    has_header_reserved_(false),
    is_inited_(false)
{
}

ObPostingValueBlockWriter::~ObPostingValueBlockWriter()
{
  reset();
}

int ObPostingValueBlockWriter::reserve_block_header_if_needed()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!has_header_reserved_)) {
    if (OB_FAIL(data_buffer_.write_nop(sizeof(ObPostingValueBlockHeader), true /*is_zero*/))) {
      STORAGE_LOG(WARN, "failed to reserve value block header", K(ret));
    } else {
      has_header_reserved_ = true;
    }
  }
  return ret;
}

int ObPostingValueBlockWriter::init(
    const ObPostingValueBlockWriteParam &param,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting value block write param", K(ret), K(param));
  } else {
    param_ = &param;
    allocator_ = &allocator;
    new (&row_offsets_) RowOffsetArray(allocator);
    if (OB_FAIL(row_offsets_.init(param.block_capacity_))) {
      STORAGE_LOG(WARN, "failed to init row offsets array", K(ret), K(param.block_capacity_));
    } else if (OB_FAIL(data_buffer_.init(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
      STORAGE_LOG(WARN, "failed to init value block data buffer", K(ret));
    } else {
      has_header_reserved_ = false;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObPostingValueBlockWriter::append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting value block writer not inited", K(ret));
  } else if (OB_UNLIKELY(is_full())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(reserve_block_header_if_needed())) {
    STORAGE_LOG(WARN, "failed to reserve value block header before append row", K(ret));
  } else {
    const int64_t col_cnt = param_->get_column_cnt();
    const char *col_ptrs[2] = {nullptr, nullptr};
    int64_t col_lens[2] = {0, 0};
    int64_t col_idx = 0;
    if (param_->has_position_list()
        && OB_FAIL(append_binary_col(
            row, param_->position_list_col_idx_, false /* null */, col_ptrs[col_idx], col_lens[col_idx]))) {
      STORAGE_LOG(WARN, "failed to get position_list column", K(ret));
    } else {
      if (param_->has_position_list()) {
        ++col_idx;
      }
      if (OB_SUCC(ret) && param_->has_nested_key()
          && OB_FAIL(append_binary_col(
              row, param_->nested_key_col_idx_, true /* null */, col_ptrs[col_idx], col_lens[col_idx]))) {
        STORAGE_LOG(WARN, "failed to get nested_key column", K(ret));
      }
    }

    int64_t data_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      data_len += col_lens[i];
    }

    const int64_t col_idx_cnt = col_cnt + 1;
    uint64_t max_col_offset = static_cast<uint64_t>(data_len);
    const uint8_t col_idx_byte_width = static_cast<uint8_t>(get_byte_packed_int_size(max_col_offset));
    const int64_t row_size = sizeof(ObPostingValueRowHeader)
        + col_idx_cnt * col_idx_byte_width + data_len;
    const int64_t row_data_base = sizeof(ObPostingValueBlockHeader);
    const int64_t row_offset = data_buffer_.length() - row_data_base;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_buffer_.ensure_space(row_size))) {
      STORAGE_LOG(WARN, "failed to ensure space for value row", K(ret), K(row_size));
    } else if (OB_FAIL(row_offsets_.push_back(row_offset))) {
      STORAGE_LOG(WARN, "failed to push row offset", K(ret), K(row_offset));
    } else {
      char *row_buf = data_buffer_.current();
      ObPostingValueRowHeader *row_header = reinterpret_cast<ObPostingValueRowHeader *>(row_buf);
      row_header->col_idx_byte_width_ = col_idx_byte_width;
      char *col_idx_buf = row_buf + sizeof(ObPostingValueRowHeader);
      char *col_data_buf = col_idx_buf + col_idx_cnt * col_idx_byte_width;

      ObIntegerArrayGenerator col_idx_gen;
      if (OB_FAIL(col_idx_gen.init(col_idx_buf, col_idx_byte_width))) {
        STORAGE_LOG(WARN, "failed to init column index generator", K(ret), K(col_idx_byte_width));
      } else {
        ObIIntegerArray &col_idx_arr = col_idx_gen.get_array();
        int64_t offset = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
          col_idx_arr.set(i, offset);
          if (col_lens[i] > 0) {
            MEMCPY(col_data_buf + offset, col_ptrs[i], col_lens[i]);
          }
          offset += col_lens[i];
        }
        if (OB_SUCC(ret)) {
          col_idx_arr.set(col_cnt, offset);
          if (OB_FAIL(data_buffer_.advance(row_size))) {
            STORAGE_LOG(WARN, "failed to advance data buffer for value row", K(ret), K(row_size));
          }
        }
      }
    }
  }
  return ret;
}

int ObPostingValueBlockWriter::build_row_index()
{
  int ret = OB_SUCCESS;
  const int64_t row_count = row_offsets_.count();
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty value block row index", K(ret));
  } else {
    uint64_t max_row_offset = 0;
    for (int64_t i = 0; i < row_count; ++i) {
      const uint64_t row_offset = static_cast<uint64_t>(row_offsets_.at(i));
      if (row_offset > max_row_offset) {
        max_row_offset = row_offset;
      }
    }
    const uint8_t row_idx_byte_width = static_cast<uint8_t>(get_byte_packed_int_size(max_row_offset));
    const int64_t row_index_size = row_count * row_idx_byte_width;
    const uint32_t row_idx_offset = static_cast<uint32_t>(data_buffer_.length());
    if (OB_FAIL(data_buffer_.ensure_space(row_index_size))) {
      STORAGE_LOG(WARN, "failed to ensure space for row index", K(ret), K(row_index_size));
    } else {
      ObPostingValueBlockHeader *header = get_header();
      header->row_idx_offset_ = row_idx_offset;
      header->row_idx_byte_width_ = row_idx_byte_width;
      ObIntegerArrayGenerator row_idx_gen;
      char *row_idx_buf = data_buffer_.current();
      if (OB_FAIL(row_idx_gen.init(row_idx_buf, row_idx_byte_width))) {
        STORAGE_LOG(WARN, "failed to init row index generator", K(ret), K(row_idx_byte_width));
      } else {
        ObIIntegerArray &row_idx_arr = row_idx_gen.get_array();
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
          row_idx_arr.set(i, row_offsets_.at(i));
        }
        if (OB_SUCC(ret) && OB_FAIL(data_buffer_.advance(row_index_size))) {
          STORAGE_LOG(WARN, "failed to advance data buffer for row index", K(ret), K(row_index_size));
        }
      }
    }
  }
  return ret;
}

int ObPostingValueBlockWriter::build_block(ObPostingValueBlockDesc &value_block)
{
  int ret = OB_SUCCESS;
  value_block.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting value block writer not inited", K(ret));
  } else if (OB_UNLIKELY(row_offsets_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty posting value block", K(ret));
  } else if (OB_FAIL(build_row_index())) {
    STORAGE_LOG(WARN, "failed to build row index", K(ret));
  } else {
    const int64_t col_cnt = param_->get_column_cnt();
    ObPostingValueBlockHeader *header = get_header();
    header->version_ = ObPostingValueBlockHeader::VALUE_BLOCK_HEADER_VERSION;
    header->has_position_list_ = param_->has_position_list() ? 1 : 0;
    header->has_nested_key_ = param_->has_nested_key() ? 1 : 0;
    header->column_cnt_ = static_cast<uint8_t>(col_cnt);
    header->row_count_ = static_cast<uint16_t>(row_offsets_.count());
    value_block.buf_ = data_buffer_.data();
    value_block.size_ = data_buffer_.length();
    value_block.row_count_ = row_offsets_.count();
  }
  return ret;
}

void ObPostingValueBlockWriter::reuse()
{
  row_offsets_.clear();
  data_buffer_.reuse();
  has_header_reserved_ = false;
}

void ObPostingValueBlockWriter::reset()
{
  reuse();
  row_offsets_.reuse();
  data_buffer_.reset();
  param_ = nullptr;
  allocator_ = nullptr;
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase
