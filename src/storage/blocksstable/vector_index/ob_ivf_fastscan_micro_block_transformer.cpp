/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_fastscan_micro_block_transformer.h"

#include <climits>
#include <cstring>
#include "lib/allocator/ob_allocator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_fast_scan.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_macro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

namespace
{

const int64_t RABITQ_METADATA_COUNT = 3;

int64_t align_up(const int64_t value, const int64_t alignment)
{
  return value + (alignment - value % alignment) % alignment;
}

int calc_base_code_layout(
    const int64_t dim,
    int64_t &code_bytes,
    int64_t &metadata_offset,
    int64_t &code_size)
{
  int ret = OB_SUCCESS;
  code_bytes = 0;
  metadata_offset = 0;
  code_size = 0;
  if (OB_UNLIKELY(dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF quantized dimension", K(ret), K(dim));
  } else {
    code_bytes = dim / CHAR_BIT + (0 == dim % CHAR_BIT ? 0 : 1);
    metadata_offset = align_up(code_bytes, sizeof(float));
    code_size = metadata_offset + RABITQ_METADATA_COUNT * sizeof(float);
  }
  return ret;
}

bool is_valid_quantized_row(
    const ObDatumRow &row,
    const ObIvfSSTableRowDesc &row_desc,
    const int64_t expected_code_size)
{
  bool valid = row_desc.is_valid_for_row(row.get_column_count());
  if (valid) {
    const ObStorageDatum &type = row.storage_datums_[row_desc.row_type_col_idx_];
    const ObStorageDatum &cid = row.storage_datums_[row_desc.centroid_id_col_idx_];
    const ObStorageDatum &vid = row.storage_datums_[row_desc.vector_id_col_idx_];
    const ObStorageDatum &payload = row.storage_datums_[row_desc.payload_col_idx_];
    valid = !type.is_null() && OB_NOT_NULL(type.ptr_) && sizeof(int64_t) == type.len_
        && IVF_ROW_TYPE_LOW_PRECISION_VECTOR == type.get_int()
        && !cid.is_null() && OB_NOT_NULL(cid.ptr_) && sizeof(int64_t) == cid.len_
        && !vid.is_null() && OB_NOT_NULL(vid.ptr_) && sizeof(int64_t) == vid.len_
        && !payload.is_null() && OB_NOT_NULL(payload.ptr_)
        && expected_code_size == payload.len_;
  }
  return valid;
}

} // namespace

const uint32_t ObIvfFastScanBlockHeader::MAGIC;
const uint16_t ObIvfFastScanBlockHeader::VERSION;
const uint32_t ObIvfQuantizedRowBlockHeader::MAGIC;
const uint16_t ObIvfQuantizedRowBlockHeader::VERSION;

bool ObIvfQuantizedRowBlockHeader::is_valid(const int64_t buf_size) const
{
  const int64_t keys_size = static_cast<int64_t>(row_count_) * sizeof(int64_t);
  const int64_t code_bytes = (static_cast<int64_t>(dim_) + CHAR_BIT - 1) / CHAR_BIT;
  return buf_size >= sizeof(*this)
      && MAGIC == magic_ && VERSION == version_ && header_size_ == sizeof(*this)
      && dim_ > 0 && row_count_ >= 0
      && code_size_ == align_up(code_bytes, sizeof(float)) + RABITQ_METADATA_COUNT * sizeof(float)
      && cids_offset_ == sizeof(*this)
      && vids_offset_ == cids_offset_ + keys_size
      && payloads_offset_ == vids_offset_ + keys_size
      && total_size_ == payloads_offset_ + static_cast<int64_t>(row_count_) * code_size_
      && total_size_ == buf_size;
}

bool ObIvfFastScanBlockHeader::is_valid(const int64_t buf_size) const
{
  const int64_t padded_count = padded_row_count();
  bool valid = MAGIC == magic_
      && VERSION == version_
      && sizeof(*this) == header_size_
      && dim_ > 0
      && row_count_ >= 0
      && batch_count_ >= 0
      && (0 == row_count_ ? (INT64_MAX == min_vid_ && INT64_MIN == max_vid_)
          : min_vid_ <= max_vid_)
      && ((0 == row_count_ && 0 == batch_count_)
          || (row_count_ > 0
              && batch_count_ >= (row_count_ + 31) / 32
              && batch_count_ <= row_count_))
      && code_bytes_ == (dim_ + CHAR_BIT - 1) / CHAR_BIT
      && packed_code_size_ == padded_count * code_bytes_
      && batch_cids_offset_ == header_size_
      && batch_row_counts_offset_ == batch_cids_offset_
          + batch_count_ * static_cast<int64_t>(sizeof(int64_t))
      && vids_offset_ >= batch_row_counts_offset_
          + batch_count_ * static_cast<int64_t>(sizeof(int32_t))
      && codes_offset_ >= vids_offset_ + row_count_ * static_cast<int64_t>(sizeof(int64_t))
      && residual_norms_offset_ == codes_offset_ + packed_code_size_
      && corrections_offset_ == residual_norms_offset_ + padded_count * static_cast<int64_t>(sizeof(float))
      && raw_norms_offset_ == corrections_offset_ + padded_count * static_cast<int64_t>(sizeof(float))
      && total_size_ == raw_norms_offset_ + padded_count * static_cast<int64_t>(sizeof(float))
      && total_size_ == buf_size;
  if (valid) {
    const int32_t *batch_row_counts = reinterpret_cast<const int32_t *>(
        reinterpret_cast<const char *>(this) + batch_row_counts_offset_);
    int64_t actual_row_count = 0;
    for (int64_t i = 0; valid && i < batch_count_; ++i) {
      valid = batch_row_counts[i] > 0 && batch_row_counts[i] <= 32;
      actual_row_count += batch_row_counts[i];
    }
    valid = valid && actual_row_count == row_count_;
  }
  return valid;
}

ObIvfVectorMicroBlockTransformer::ObIvfVectorMicroBlockTransformer()
  : is_inited_(false),
    header_(),
    fastscan_ctx_(),
    row_desc_(),
    decoded_block_(),
    reader_helper_(),
    micro_reader_(nullptr),
    allocator_(nullptr),
    decoded_buf_(nullptr)
{}

ObIvfVectorMicroBlockTransformer::~ObIvfVectorMicroBlockTransformer()
{
  reset();
}

void ObIvfVectorMicroBlockTransformer::reset()
{
  if (OB_NOT_NULL(micro_reader_)) {
    micro_reader_->reset();
    micro_reader_ = nullptr;
  }
  reader_helper_.reset();
  if (OB_NOT_NULL(decoded_buf_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(decoded_buf_);
  }
  is_inited_ = false;
  header_.reset();
  fastscan_ctx_ = ObIvfFastScanTransformContext();
  row_desc_.reset();
  decoded_block_.reset();
  allocator_ = nullptr;
  decoded_buf_ = nullptr;
}

int ObIvfVectorMicroBlockTransformer::init(
    const ObMicroBlockHeader &header,
    const char *micro_block_buf,
    const int64_t micro_block_size,
    const ObMicroBlockDesMeta &des_meta,
    ObMacroBlockReader &reader,
    common::ObIAllocator &allocator,
    const ObIvfFastScanTransformContext &fastscan_ctx)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF vector block transformer has been initialized", K(ret));
  } else if (OB_ISNULL(micro_block_buf)
      || OB_UNLIKELY(micro_block_size <= 0 || !fastscan_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF vector block transformer argument",
        K(ret), KP(micro_block_buf), K(micro_block_size), K(fastscan_ctx));
  } else if (OB_FAIL(row_desc_.init(fastscan_ctx.format_version_))) {
    LOG_WARN("failed to initialize IVF vector row descriptor", K(ret), K(fastscan_ctx));
  } else {
    allocator_ = &allocator;
    header_ = header;
    fastscan_ctx_ = fastscan_ctx;
    if (ObStoreFormat::is_row_store_type_with_cs_encoding(
        static_cast<ObRowStoreType>(header.row_store_type_))) {
      if (OB_FAIL(reader.decrypt_and_full_transform_data(
          header, des_meta, micro_block_buf, micro_block_size,
          decoded_block_, allocator_))) {
        LOG_WARN("failed to fully decode IVF CS micro block", K(ret), K(header));
      }
    } else if (OB_FAIL(reader.do_decrypt_and_decompress_data(
        header, des_meta, micro_block_buf, micro_block_size,
        decoded_block_, is_compressed, true, allocator_))) {
      LOG_WARN("failed to decode IVF micro block", K(ret), K(header));
    }

    if (OB_SUCC(ret)) {
      decoded_buf_ = const_cast<char *>(decoded_block_.get_buf());
      if (OB_ISNULL(decoded_buf_) || OB_ISNULL(decoded_block_.get_micro_header())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("decoded IVF micro block is invalid", K(ret), K_(decoded_block));
      } else if (OB_FAIL(reader_helper_.init(allocator))) {
        LOG_WARN("failed to initialize IVF micro reader helper", K(ret));
      } else if (OB_FAIL(reader_helper_.get_reader(
          *decoded_block_.get_micro_header(), micro_reader_))) {
        LOG_WARN("failed to get IVF micro block reader", K(ret), K_(decoded_block));
      } else if (OB_FAIL(micro_reader_->init(decoded_block_, nullptr))) {
        LOG_WARN("failed to initialize IVF micro block reader", K(ret), K_(decoded_block));
      } else {
        is_inited_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObIvfVectorMicroBlockTransformer::count_batches_by_cid(
    const int64_t expected_code_size,
    int64_t &batch_count)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = quantization::ObRaBitQFastScanEncoder::BATCH_SIZE;
  int64_t batch_cid = 0;
  int64_t batch_row_count = 0;
  ObDatumRow row;
  batch_count = 0;
  if (header_.row_count_ > 0
      && OB_FAIL(row.init(*allocator_, row_desc_.payload_col_idx_ + 1))) {
    LOG_WARN("failed to initialize IVF datum row for batch counting", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < header_.row_count_; ++i) {
    if (OB_FAIL(micro_reader_->get_row(i, row))) {
      LOG_WARN("failed to read IVF micro block row for batch counting", K(ret), K(i));
    } else if (OB_UNLIKELY(!is_valid_quantized_row(
        row, row_desc_, expected_code_size))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF FastScan row while counting batches", K(ret), K(i), K(row));
    } else {
      const int64_t cid =
          row.storage_datums_[row_desc_.centroid_id_col_idx_].get_int();
      if (0 == batch_row_count || batch_size == batch_row_count || cid != batch_cid) {
        ++batch_count;
        batch_cid = cid;
        batch_row_count = 0;
      }
      ++batch_row_count;
    }
  }
  return ret;
}

int ObIvfVectorMicroBlockTransformer::pack_batch_codes(
    const uint8_t *codes,
    const int64_t code_count,
    const int64_t code_bytes,
    const int64_t batch_index,
    char *dst,
    const ObIvfFastScanBlockHeader &out_header)
{
  int ret = OB_SUCCESS;
  const int64_t packed_batch_size = code_bytes
      * quantization::ObRaBitQFastScanEncoder::BATCH_SIZE;
  if (OB_FAIL(quantization::ObRaBitQFastScanEncoder::pack_codes(
      codes, code_bytes, code_count, fastscan_ctx_.dim_,
      {reinterpret_cast<uint8_t *>(dst + out_header.codes_offset_)
          + batch_index * packed_batch_size, packed_batch_size}))) {
    LOG_WARN("failed to pack IVF RaBitQ FastScan batch",
        K(ret), K(code_count), K(code_bytes), K(batch_index), K_(fastscan_ctx));
  }
  return ret;
}

int ObIvfVectorMicroBlockTransformer::materialize_rows(
    char *dst,
    ObIvfFastScanBlockHeader &out_header)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  int64_t metadata_offset = 0;
  int64_t expected_code_size = 0;
  const int64_t batch_size = quantization::ObRaBitQFastScanEncoder::BATCH_SIZE;
  int64_t batch_index = -1;
  int64_t batch_cid = 0;
  int64_t batch_row_count = 0;
  uint8_t *batch_codes = nullptr;
  ObDatumRow row;
  int64_t *batch_cids = reinterpret_cast<int64_t *>(
      dst + out_header.batch_cids_offset_);
  int32_t *batch_row_counts = reinterpret_cast<int32_t *>(
      dst + out_header.batch_row_counts_offset_);
  int64_t *vids = reinterpret_cast<int64_t *>(dst + out_header.vids_offset_);
  float *residual_norms = reinterpret_cast<float *>(dst + out_header.residual_norms_offset_);
  float *corrections = reinterpret_cast<float *>(dst + out_header.corrections_offset_);
  float *raw_norms = reinterpret_cast<float *>(dst + out_header.raw_norms_offset_);

  if (OB_FAIL(calc_base_code_layout(
      fastscan_ctx_.dim_, code_bytes, metadata_offset, expected_code_size))) {
    LOG_WARN("failed to calculate IVF RaBitQ code layout", K(ret), K_(fastscan_ctx));
  } else if (out_header.row_count_ > 0
      && OB_ISNULL(batch_codes = static_cast<uint8_t *>(
          allocator_->alloc(batch_size * code_bytes)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF FastScan batch codes", K(ret), K(code_bytes));
  } else if (out_header.row_count_ > 0
      && OB_FAIL(row.init(*allocator_, row_desc_.payload_col_idx_ + 1))) {
    LOG_WARN("failed to initialize IVF datum row", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < header_.row_count_; ++i) {
    if (OB_FAIL(micro_reader_->get_row(i, row))) {
      LOG_WARN("failed to read IVF micro block row", K(ret), K(i));
    } else if (OB_UNLIKELY(!is_valid_quantized_row(
        row, row_desc_, expected_code_size))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF FastScan row while materializing", K(ret), K(i), K(row));
    } else {
      const ObStorageDatum &cid = row.storage_datums_[row_desc_.centroid_id_col_idx_];
      const ObStorageDatum &vid = row.storage_datums_[row_desc_.vector_id_col_idx_];
      const ObStorageDatum &payload = row.storage_datums_[row_desc_.payload_col_idx_];
      const int64_t cid_value = cid.get_int();
      if (batch_row_count > 0
          && (batch_size == batch_row_count || cid_value != batch_cid)) {
        if (OB_FAIL(pack_batch_codes(batch_codes, batch_row_count,
            code_bytes, batch_index, dst, out_header))) {
          LOG_WARN("failed to flush IVF FastScan batch", K(ret), K(batch_index));
        }
        batch_row_count = 0;
      }
      if (OB_SUCC(ret)) {
        if (0 == batch_row_count) {
          ++batch_index;
          batch_cid = cid_value;
          batch_cids[batch_index] = batch_cid;
        }
        vids[i] = vid.get_int();
        out_header.min_vid_ = MIN(out_header.min_vid_, vids[i]);
        out_header.max_vid_ = MAX(out_header.max_vid_, vids[i]);
        const int64_t padded_row_index = batch_index * batch_size + batch_row_count;
        MEMCPY(batch_codes + batch_row_count * code_bytes, payload.ptr_, code_bytes);
        MEMCPY(residual_norms + padded_row_index,
            payload.ptr_ + metadata_offset, sizeof(float));
        MEMCPY(corrections + padded_row_index,
            payload.ptr_ + metadata_offset + sizeof(float), sizeof(float));
        MEMCPY(raw_norms + padded_row_index,
            payload.ptr_ + metadata_offset + 2 * sizeof(float), sizeof(float));
        ++batch_row_count;
        batch_row_counts[batch_index] = batch_row_count;
      }
    }
  }

  if (OB_SUCC(ret) && batch_row_count > 0
      && OB_FAIL(pack_batch_codes(batch_codes, batch_row_count,
          code_bytes, batch_index, dst, out_header))) {
    LOG_WARN("failed to flush final IVF FastScan batch", K(ret), K(batch_index));
  } else if (OB_SUCC(ret) && batch_index + 1 != out_header.batch_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF FastScan materialized batch count",
        K(ret), K(batch_index), K(out_header.batch_count_));
  }

  if (OB_NOT_NULL(batch_codes)) {
    allocator_->free(batch_codes);
  }
  return ret;
}

int ObIvfVectorMicroBlockTransformer::transform_quantized_rows(char *&block_buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  int64_t metadata_offset = 0;
  int64_t code_size = 0;
  const int64_t row_count = header_.row_count_;
  ObDatumRow row;
  block_buf = nullptr;
  buf_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(calc_base_code_layout(fastscan_ctx_.dim_, code_bytes, metadata_offset, code_size))) {
    LOG_WARN("failed to calculate IVF quantized row layout", K(ret), K_(fastscan_ctx));
  } else {
    const int64_t total_size = sizeof(ObIvfQuantizedRowBlockHeader)
        + row_count * (2 * sizeof(int64_t) + code_size);
    if (OB_UNLIKELY(total_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
    } else if (row_count > 0 && OB_FAIL(row.init(*allocator_, row_desc_.payload_col_idx_ + 1))) {
      LOG_WARN("failed to initialize IVF quantized row", K(ret));
    } else if (OB_ISNULL(block_buf = static_cast<char *>(allocator_->alloc(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObIvfQuantizedRowBlockHeader *out = new (block_buf) ObIvfQuantizedRowBlockHeader();
      out->magic_ = ObIvfQuantizedRowBlockHeader::MAGIC;
      out->version_ = ObIvfQuantizedRowBlockHeader::VERSION;
      out->header_size_ = sizeof(*out);
      out->dim_ = fastscan_ctx_.dim_;
      out->row_count_ = row_count;
      out->code_size_ = code_size;
      out->cids_offset_ = sizeof(*out);
      out->vids_offset_ = out->cids_offset_ + row_count * sizeof(int64_t);
      out->payloads_offset_ = out->vids_offset_ + row_count * sizeof(int64_t);
      out->total_size_ = total_size;
      int64_t *cids = reinterpret_cast<int64_t *>(block_buf + out->cids_offset_);
      int64_t *vids = reinterpret_cast<int64_t *>(block_buf + out->vids_offset_);
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        if (OB_FAIL(micro_reader_->get_row(i, row))) {
          LOG_WARN("failed to read IVF quantized row", K(ret), K(i));
        } else if (OB_UNLIKELY(!is_valid_quantized_row(row, row_desc_, code_size))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF quantized row", K(ret), K(i));
        } else {
          cids[i] = row.storage_datums_[row_desc_.centroid_id_col_idx_].get_int();
          vids[i] = row.storage_datums_[row_desc_.vector_id_col_idx_].get_int();
          if (OB_UNLIKELY(cids[i] < 0 || (i > 0 && (cids[i] < cids[i - 1]
              || (cids[i] == cids[i - 1] && vids[i] < vids[i - 1]))))) {
            ret = OB_INVALID_DATA;
            LOG_WARN("unordered IVF quantized rows", K(ret), K(i));
          } else {
            MEMCPY(block_buf + out->payloads_offset_ + i * code_size,
                row.storage_datums_[row_desc_.payload_col_idx_].ptr_, code_size);
          }
        }
      }
      if (OB_SUCC(ret)) {
        buf_size = total_size;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(block_buf)) {
    allocator_->free(block_buf);
    block_buf = nullptr;
  }
  return ret;
}

int ObIvfVectorMicroBlockTransformer::transform(char *&block_buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = header_.row_count_;
  int64_t batch_count = 0;
  int64_t code_bytes = 0;
  int64_t metadata_offset = 0;
  int64_t expected_code_size = 0;
  int64_t packed_code_size = 0;
  int64_t batch_keys_size = 0;
  int64_t vids_size = 0;
  int64_t factor_size = 0;
  int64_t total_size = 0;
  int64_t cursor = sizeof(ObIvfFastScanBlockHeader);
  const int64_t batch_size = quantization::ObRaBitQFastScanEncoder::BATCH_SIZE;
  block_buf = nullptr;
  buf_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF vector block transformer is not initialized", K(ret));
  } else if (OB_FAIL(calc_base_code_layout(
      fastscan_ctx_.dim_, code_bytes, metadata_offset, expected_code_size))) {
    LOG_WARN("failed to calculate IVF RaBitQ code layout", K(ret), K_(fastscan_ctx));
  } else if (OB_FAIL(count_batches_by_cid(expected_code_size, batch_count))) {
    LOG_WARN("failed to count IVF FastScan batches by cid", K(ret), K(row_count));
  } else if (OB_FAIL(quantization::ObRaBitQFastScanEncoder::calc_packed_size(
      fastscan_ctx_.dim_, batch_count * batch_size, packed_code_size))) {
    LOG_WARN("failed to calculate IVF FastScan packed code size",
        K(ret), K(row_count), K(batch_count));
  } else {
    const int64_t padded_count = batch_count * batch_size;
    batch_keys_size = batch_count * (sizeof(int64_t) + sizeof(int32_t));
    vids_size = row_count * sizeof(int64_t);
    factor_size = padded_count * RABITQ_METADATA_COUNT * sizeof(float);
    cursor = align_up(cursor + batch_keys_size, sizeof(int64_t));
    cursor = align_up(cursor + vids_size, 32);
    cursor += packed_code_size;
    total_size = cursor + factor_size;
    if (OB_ISNULL(block_buf = static_cast<char *>(allocator_->alloc(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF FastScan output", K(ret), K(total_size));
    } else {
      MEMSET(block_buf, 0, total_size);
      ObIvfFastScanBlockHeader *out_header =
          reinterpret_cast<ObIvfFastScanBlockHeader *>(block_buf);
      out_header->magic_ = ObIvfFastScanBlockHeader::MAGIC;
      out_header->version_ = ObIvfFastScanBlockHeader::VERSION;
      out_header->header_size_ = sizeof(*out_header);
      out_header->dim_ = fastscan_ctx_.dim_;
      out_header->row_count_ = row_count;
      out_header->batch_count_ = batch_count;
      out_header->code_bytes_ = code_bytes;
      out_header->packed_code_size_ = packed_code_size;
      out_header->batch_cids_offset_ = sizeof(*out_header);
      out_header->batch_row_counts_offset_ = out_header->batch_cids_offset_
          + batch_count * sizeof(int64_t);
      out_header->vids_offset_ = align_up(out_header->batch_row_counts_offset_
          + batch_count * sizeof(int32_t), sizeof(int64_t));
      out_header->codes_offset_ = align_up(
          out_header->vids_offset_ + row_count * sizeof(int64_t), 32);
      out_header->residual_norms_offset_ = out_header->codes_offset_ + packed_code_size;
      out_header->corrections_offset_ =
          out_header->residual_norms_offset_ + padded_count * sizeof(float);
      out_header->raw_norms_offset_ =
          out_header->corrections_offset_ + padded_count * sizeof(float);
      out_header->total_size_ = total_size;
      out_header->min_vid_ = INT64_MAX;
      out_header->max_vid_ = INT64_MIN;
      if (OB_FAIL(materialize_rows(block_buf, *out_header))) {
        LOG_WARN("failed to materialize IVF FastScan rows", K(ret), KPC(out_header));
      } else if (OB_UNLIKELY(!out_header->is_valid(total_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constructed invalid IVF FastScan block", K(ret), KPC(out_header));
      } else {
        buf_size = total_size;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(block_buf)) {
    allocator_->free(block_buf);
    block_buf = nullptr;
    buf_size = 0;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
