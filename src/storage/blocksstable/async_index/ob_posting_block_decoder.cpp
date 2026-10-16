/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"

#include <algorithm>

#include "lib/codec/ob_codecs.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_double_delta_zigzag_pfor.h"
#include "lib/codec/ob_simd_fixed_pfor.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

STATIC_ASSERT(ObPostingBlockHeader::MAX_POSTING_COL_CNT * POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE
        + POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE
    == POSTING_DECODE_BUF_BYTES,
    "posting decode buf layout mismatch");

ObPostingBlockDecoder::ObPostingBlockDecoder()
  : header_(nullptr),
    col_metas_(nullptr),
    decode_buf_(nullptr),
    decode_arena_("PostBlkDec"),
    col_cnt_(0),
    is_inited_(false)
{
}

ObPostingBlockDecoder::~ObPostingBlockDecoder()
{
  reset();
}

int ObPostingBlockDecoder::validate_ob_codec_column_payload(
    const ObPostingColumnMeta &meta,
    const uint16_t row_count,
    const int64_t col_data_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_ob_codec_posting_col_encoding(meta.encoding_type_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "not an ob_codec posting column for payload validation",
        K(ret), K(meta.encoding_type_));
  } else if (OB_UNLIKELY(row_count <= 0
                         || row_count > DEFAULT_POSTING_BLOCK_CAPACITY)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row count for ob_codec column payload validation",
        K(ret), K(row_count), "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
  } else if (OB_UNLIKELY(col_data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ob_codec column payload size for validation",
        K(ret), K(col_data_size));
  } else {
    const int64_t plain_bytes = static_cast<int64_t>(row_count) * meta.packed_width_;
    const int64_t max_encoded = ObCodec::get_default_max_encoding_size(plain_bytes);
    if (OB_UNLIKELY(col_data_size > max_encoded)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ob_codec column payload exceeds max encoding size bound",
          K(ret), K(col_data_size), K(max_encoded), K(row_count), K(plain_bytes), K(meta));
    }
  }
  return ret;
}

int ObPostingBlockDecoder::get_column_data_size(
    const ObPostingColumnMeta &meta,
    const uint16_t row_count,
    const int64_t col_idx,
    const int64_t col_cnt,
    const int64_t block_size,
    const ObPostingColumnMeta *metas,
    int64_t &col_data_size)
{
  int ret = OB_SUCCESS;
  col_data_size = 0;
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= col_cnt
      || block_size <= 0
      || row_count <= 0
      || !meta.is_valid()
      || meta.offset_ >= block_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args for posting column data size",
        K(ret), K(col_idx), K(col_cnt), K(block_size), K(row_count), K(meta));
  } else if (is_packed_base_posting_col_encoding(meta.encoding_type_)) {
    if (OB_ISNULL(metas)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "null column metas for packed base column data size", K(ret), K(col_idx));
    } else {
      col_data_size = row_count * meta.packed_width_;
      const int64_t col_end = meta.offset_ + col_data_size;
      if (col_idx + 1 < col_cnt) {
        if (OB_UNLIKELY(metas[col_idx + 1].offset_ <= meta.offset_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "non-increasing posting column offsets",
              K(ret), K(col_idx), K(meta.offset_), K(metas[col_idx + 1].offset_));
        } else if (OB_UNLIKELY(metas[col_idx + 1].offset_ != col_end)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "packed base column size does not match next column offset",
              K(ret), K(col_idx), K(col_data_size), K(col_end), K(metas[col_idx + 1].offset_), K(meta));
        }
      } else if (OB_UNLIKELY(col_end != block_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "packed base column size does not match block end",
            K(ret), K(col_idx), K(col_data_size), K(col_end), K(block_size), K(meta));
      }
    }
  } else if (OB_ISNULL(metas)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "null column metas for ob_codec column data size", K(ret), K(col_idx));
  } else if (col_idx + 1 < col_cnt) {
    if (OB_UNLIKELY(metas[col_idx + 1].offset_ <= meta.offset_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "non-increasing posting column offsets",
          K(ret), K(col_idx), K(meta.offset_), K(metas[col_idx + 1].offset_));
    } else {
      col_data_size = metas[col_idx + 1].offset_ - meta.offset_;
    }
  } else {
    col_data_size = block_size - meta.offset_;
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(col_data_size <= 0 || col_data_size > block_size - meta.offset_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected posting column data size", K(ret), K(col_data_size),
        K(block_size), K(meta));
  } else if (OB_SUCC(ret) && is_ob_codec_posting_col_encoding(meta.encoding_type_)) {
    if (OB_FAIL(validate_ob_codec_column_payload(meta, row_count, col_data_size))) {
      STORAGE_LOG(WARN, "invalid ob_codec column payload bounds", K(ret), K(col_idx), K(row_count),
          K(col_data_size), K(meta));
    }
  }
  return ret;
}

int ObPostingBlockDecoder::init(
    const char *buf,
    const int64_t size,
    ObIAllocator &decode_alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(decode_alloc);
  reuse();
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting block buffer", K(ret), KP(buf), K(size));
  } else if (OB_UNLIKELY(size < static_cast<int64_t>(sizeof(ObPostingBlockHeader)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting block buffer too small", K(ret), K(size));
  } else {
    header_ = reinterpret_cast<const ObPostingBlockHeader *>(buf);
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid posting block header", K(ret), KPC(header_));
    } else if (OB_UNLIKELY(header_->row_count_ > DEFAULT_POSTING_BLOCK_CAPACITY)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "posting block row count exceeds capacity", K(ret),
          K(header_->row_count_), "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
    } else {
      const int64_t header_size = header_->get_header_size();
      if (OB_UNLIKELY(size < header_size)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "posting block buffer smaller than header", K(ret), K(size), K(header_size));
      } else {
        col_metas_ = reinterpret_cast<const ObPostingColumnMeta *>(
            buf + sizeof(ObPostingBlockHeader));
        const int64_t col_cnt = header_->get_column_count();
        for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
          col_ctxs_[i].reset();
          col_ctxs_[i].meta_ = &col_metas_[i];
          const ObPostingColumnMeta *meta = col_ctxs_[i].meta_;
          int64_t col_data_size = 0;
          if (OB_FAIL(get_column_data_size(
                  *meta, header_->row_count_, i, col_cnt, size, col_metas_, col_data_size))) {
            STORAGE_LOG(WARN, "failed to get posting column data size", K(ret), K(i), KPC(meta));
          } else if (OB_UNLIKELY(!meta->is_valid(header_size))) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "invalid posting column meta or data offset",
                K(ret), K(i), K(header_size), KPC(meta));
          } else if (OB_UNLIKELY(meta->offset_ + col_data_size > size)) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "posting column data exceeds block size",
                K(ret), K(i), K(meta->offset_), K(col_data_size), K(size));
          } else {
            col_ctxs_[i].col_data_ = buf + meta->offset_;
            col_ctxs_[i].col_data_size_ = col_data_size;
            col_ctxs_[i].decoded_values_ = nullptr;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(decode_buf_)) {
            decode_buf_ = static_cast<char *>(decode_arena_.alloc(POSTING_DECODE_BUF_BYTES));
            if (OB_ISNULL(decode_buf_)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to alloc posting decode buf", K(ret),
                  "bytes", POSTING_DECODE_BUF_BYTES);
            } else {
              MEMSET(decode_buf_, 0, POSTING_DECODE_BUF_BYTES);
            }
          }
        }
        if (OB_SUCC(ret)) {
          col_cnt_ = col_cnt;
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObPostingBlockDecoder::get_materialized_buf(
    const int64_t col_idx,
    uint64_t *&buf) const
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index for materialized buf", K(ret), K(col_idx));
  } else if (OB_ISNULL(decode_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "posting decode buf is null", K(ret), K(col_idx));
  } else {
    buf = reinterpret_cast<uint64_t *>(
        decode_buf_ + posting_decode_materialized_col_byte_offset(col_idx));
  }
  return ret;
}

int ObPostingBlockDecoder::get_ob_codec_scratch_buf(char *&buf) const
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  if (OB_ISNULL(decode_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "posting decode buf is null for ob_codec scratch", K(ret));
  } else {
    buf = decode_buf_ + posting_decode_ob_codec_scratch_byte_offset();
  }
  return ret;
}

int ObPostingBlockDecoder::get_packed_base_value(
    const ObPostingColumnDecodeCtx &col_ctx,
    const int64_t idx,
    uint64_t &value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column decode ctx", K(ret), KPC(col_ctx.meta_), KP(col_ctx.col_data_));
  } else if (OB_UNLIKELY(ObPostingColEncodingType::PACKED_BASE != col_ctx.meta_->encoding_type_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "unsupported posting column encoding type", K(ret), K(col_ctx.meta_->encoding_type_));
  } else if (OB_UNLIKELY(!col_ctx.meta_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid packed base column meta", K(ret), KPC(col_ctx.meta_));
  } else if (OB_UNLIKELY(idx < 0 || idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row index for packed base value", K(ret), K(idx), K(header_->row_count_));
  } else {
    ObIntegerArrayGenerator gen;
    if (OB_FAIL(gen.init(col_ctx.col_data_, col_ctx.meta_->packed_width_))) {
      STORAGE_LOG(WARN, "failed to init integer array generator", K(ret), K(col_ctx.meta_->packed_width_));
    } else {
      const uint64_t diff = static_cast<uint64_t>(gen.get_array().at(idx));
      value = static_cast<uint64_t>(col_ctx.meta_->base_value_) + diff;
    }
  }
  return ret;
}

int ObPostingBlockDecoder::decode_packed_base_column_batch(
    const ObPostingColumnMeta *meta,
    const char *col_data,
    const int64_t row_count,
    uint64_t *dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(meta) || OB_ISNULL(col_data) || OB_ISNULL(dst)
      || row_count <= 0
      || !meta->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args for packed base batch decode", K(ret),
        KPC(meta), KP(col_data), KP(dst), K(row_count));
  } else if (OB_UNLIKELY(ObPostingColEncodingType::PACKED_BASE != meta->encoding_type_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "unsupported posting column encoding type for batch decode",
        K(ret), K(meta->encoding_type_));
  } else {
    const uint64_t base = static_cast<uint64_t>(meta->base_value_);
    const uint8_t width = meta->packed_width_;
    switch (width) {
      case 1: {
        const uint8_t *src = reinterpret_cast<const uint8_t *>(col_data);
        for (int64_t i = 0; i < row_count; ++i) {
          dst[i] = base + src[i];
        }
        break;
      }
      case 2: {
        const uint16_t *src = reinterpret_cast<const uint16_t *>(col_data);
        for (int64_t i = 0; i < row_count; ++i) {
          dst[i] = base + src[i];
        }
        break;
      }
      case 4: {
        const uint32_t *src = reinterpret_cast<const uint32_t *>(col_data);
        for (int64_t i = 0; i < row_count; ++i) {
          dst[i] = base + src[i];
        }
        break;
      }
      case 8: {
        const uint64_t *src = reinterpret_cast<const uint64_t *>(col_data);
        for (int64_t i = 0; i < row_count; ++i) {
          dst[i] = base + src[i];
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected packed_width", K(ret), K(width));
        break;
      }
    }
  }
  return ret;
}

int ObPostingBlockDecoder::ensure_column_materialized(
    const int64_t col_idx,
    ObPostingColumnDecodeCtx &col_ctx) const
{
  int ret = OB_SUCCESS;
  if (col_ctx.is_decoded()) {
    // already materialized
  } else if (OB_UNLIKELY(col_idx < 0
                         || col_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT
                         || OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_)
                         || OB_ISNULL(header_)
                         || header_->row_count_ <= 0
                         || !col_ctx.meta_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column decode ctx for materialize", K(ret), K(col_idx),
        KP(col_ctx.meta_), KP(col_ctx.col_data_), KP(header_));
  } else {
    const int64_t row_count = header_->row_count_;
    uint64_t *values = nullptr;
    if (OB_FAIL(get_materialized_buf(col_idx, values))) {
      STORAGE_LOG(WARN, "failed to get materialized buf", K(ret), K(col_idx), K(row_count));
    } else if (OB_UNLIKELY(row_count > DEFAULT_POSTING_BLOCK_CAPACITY)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "row count exceeds decode buffer capacity", K(ret), K(row_count),
          "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
    } else if (is_packed_base_posting_col_encoding(col_ctx.meta_->encoding_type_)) {
      if (OB_FAIL(decode_packed_base_column_batch(
              col_ctx.meta_, col_ctx.col_data_, row_count, values))) {
        STORAGE_LOG(WARN, "failed to materialize packed base column", K(ret), K(row_count));
      } else {
        col_ctx.decoded_values_ = values;
      }
    } else if (is_ob_codec_posting_col_encoding(col_ctx.meta_->encoding_type_)) {
      if (OB_FAIL(decode_ob_codec_column_batch(col_ctx, row_count, values))) {
        STORAGE_LOG(WARN, "failed to materialize ob_codec column", K(ret), K(row_count));
      } else {
        col_ctx.decoded_values_ = values;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "unsupported posting column encoding type for materialize",
          K(ret), K(col_ctx.meta_->encoding_type_));
    }
  }
  return ret;
}

int ObPostingBlockDecoder::ensure_ob_codec_decoded(
    const int64_t col_idx,
    ObPostingColumnDecodeCtx &col_ctx) const
{
  int ret = OB_SUCCESS;
  if (col_ctx.is_decoded()) {
    // already materialized
  } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index for ob_codec decode", K(ret), K(col_idx));
  } else if (OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column decode ctx for ob_codec", K(ret), K(col_idx));
  } else if (OB_UNLIKELY(!is_ob_codec_posting_col_encoding(col_ctx.meta_->encoding_type_))) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not an ob_codec posting column encoding", K(ret), K(col_idx),
        K(col_ctx.meta_->encoding_type_));
  } else if (OB_FAIL(ensure_column_materialized(col_idx, col_ctx))) {
    STORAGE_LOG(WARN, "failed to materialize ob_codec column", K(ret), K(col_idx));
  }
  return ret;
}

int ObPostingBlockDecoder::lower_bound_uint64_array(
    const uint64_t *values,
    const uint64_t target,
    const int64_t start_idx,
    const int64_t end_idx,
    int64_t &target_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(values)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "null values array for lower_bound_uint64_array", K(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid lower_bound_uint64_array index range", K(ret), K(start_idx), K(end_idx));
  } else {
    const uint64_t *begin = values + start_idx;
    const uint64_t *end = values + end_idx;
    const uint64_t *it = std::lower_bound(begin, end, target);
    if (OB_UNLIKELY(it >= end)) {
      ret = OB_BEYOND_THE_RANGE;
    } else {
      target_idx = it - values;
    }
  }
  return ret;
}

int ObPostingBlockDecoder::decode_ob_codec_column_batch(
    ObPostingColumnDecodeCtx &col_ctx,
    const int64_t row_count,
    uint64_t *dst) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_) || OB_ISNULL(dst)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ctx for ob_codec batch decode", K(ret),
        KPC(col_ctx.meta_), KP(col_ctx.col_data_), KP(dst));
  } else if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row count for ob_codec batch decode", K(ret), K(row_count));
  } else if (OB_UNLIKELY(row_count > DEFAULT_POSTING_BLOCK_CAPACITY)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row count exceeds decode buffer capacity for ob_codec batch decode",
        K(ret), K(row_count), "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
  } else if (OB_UNLIKELY(!col_ctx.meta_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ob_codec column meta for batch decode", K(ret), KPC(col_ctx.meta_));
  } else if (OB_UNLIKELY(!is_ob_codec_posting_col_encoding(col_ctx.meta_->encoding_type_))) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not an ob_codec posting column encoding for batch decode",
        K(ret), K(col_ctx.meta_->encoding_type_));
  } else if (OB_ISNULL(header_) || OB_UNLIKELY(row_count != header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ob_codec batch decode row count mismatch with block header",
        K(ret), K(row_count), "header_row_count", header_ != nullptr ? header_->row_count_ : 0);
  } else {
    const uint8_t uint_bytes = col_ctx.meta_->packed_width_;
    const uint64_t encoded_size = static_cast<uint64_t>(col_ctx.col_data_size_);
    const uint64_t uint_count = static_cast<uint64_t>(row_count);
    const uint64_t tmp_buf_size = uint_count * uint_bytes;
    char *tmp_buf = nullptr;
    if (OB_UNLIKELY(0 == encoded_size)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid ob_codec encoded payload size for batch decode",
          K(ret), K(encoded_size), KPC(col_ctx.meta_));
    } else if (OB_FAIL(validate_ob_codec_column_payload(
            *col_ctx.meta_, static_cast<uint16_t>(row_count), col_ctx.col_data_size_))) {
      STORAGE_LOG(WARN, "invalid ob_codec column payload bounds for batch decode",
          K(ret), K(row_count), K(col_ctx.col_data_size_), KPC(col_ctx.meta_));
    } else if (OB_FAIL(get_ob_codec_scratch_buf(tmp_buf))) {
      STORAGE_LOG(WARN, "failed to get ob_codec scratch buf", K(ret), K(row_count));
    } else if (OB_ISNULL(tmp_buf)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null ob_codec scratch buf", K(ret), K(row_count));
    } else if (OB_UNLIKELY(tmp_buf_size > static_cast<uint64_t>(POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "ob_codec scratch buffer is too small", K(ret), K(tmp_buf_size));
    } else {
      uint64_t in_pos = 0;
      uint64_t out_pos = 0;
      switch (col_ctx.meta_->encoding_type_) {
        case ObPostingColEncodingType::SIMD_FIXEDPFOR: {
          ObCompositeCodec<ObSIMDFixedPFor, ObSimpleBitPacking> codec;
          codec.set_uint_bytes(uint_bytes);
          codec.set_pfor_packing_type(ObCodec::CPU_ARCH_INDEPENDANT_SCALAR);
          if (OB_FAIL(codec.decode(col_ctx.col_data_, encoded_size, in_pos, uint_count,
                  tmp_buf, tmp_buf_size, out_pos))) {
            STORAGE_LOG(WARN, "failed to decode posting column with simd fixedpfor", K(ret));
          }
          break;
        }
        case ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR: {
          ObDoubleDeltaZigzagPFor codec;
          codec.set_uint_bytes(uint_bytes);
          codec.set_pfor_packing_type(ObCodec::CPU_ARCH_INDEPENDANT_SCALAR);
          if (OB_FAIL(codec.decode(col_ctx.col_data_, encoded_size, in_pos, uint_count,
                  tmp_buf, tmp_buf_size, out_pos))) {
            STORAGE_LOG(WARN, "failed to decode posting column with double delta zigzag pfor", K(ret));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(WARN, "unsupported ob_codec encoding type for decode",
              K(ret), K(col_ctx.meta_->encoding_type_));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (1 == uint_bytes) {
          const uint8_t *src = reinterpret_cast<const uint8_t *>(tmp_buf);
          for (int64_t i = 0; i < row_count; ++i) {
            dst[i] = src[i];
          }
        } else if (2 == uint_bytes) {
          const uint16_t *src = reinterpret_cast<const uint16_t *>(tmp_buf);
          for (int64_t i = 0; i < row_count; ++i) {
            dst[i] = src[i];
          }
        } else if (4 == uint_bytes) {
          const uint32_t *src = reinterpret_cast<const uint32_t *>(tmp_buf);
          for (int64_t i = 0; i < row_count; ++i) {
            dst[i] = src[i];
          }
        } else if (8 == uint_bytes) {
          MEMCPY(dst, tmp_buf, row_count * sizeof(uint64_t));
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected uint bytes", K(ret), K(uint_bytes));
        }
      }
    }
  }
  return ret;
}

int ObPostingBlockDecoder::decode_column_batch(
    ObPostingColumnDecodeCtx &col_ctx,
    const int64_t row_count,
    uint64_t *dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_) || OB_ISNULL(dst)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column decode ctx for batch decode", K(ret),
        KPC(col_ctx.meta_), KP(col_ctx.col_data_), KP(dst));
  } else if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row count for batch decode", K(ret), K(row_count));
  } else if (is_packed_base_posting_col_encoding(col_ctx.meta_->encoding_type_)) {
    if (OB_FAIL(decode_packed_base_column_batch(
            col_ctx.meta_, col_ctx.col_data_, row_count, dst))) {
      STORAGE_LOG(WARN, "failed to batch decode PACKED_BASE column", K(ret));
    }
  } else if (is_ob_codec_posting_col_encoding(col_ctx.meta_->encoding_type_)) {
    if (OB_FAIL(decode_ob_codec_column_batch(col_ctx, row_count, dst))) {
      STORAGE_LOG(WARN, "failed to batch decode ob_codec column", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "unsupported posting column encoding type for batch decode",
        K(ret), K(col_ctx.meta_->encoding_type_));
  }
  return ret;
}

int ObPostingBlockDecoder::get_column_value(
    const int64_t col_idx,
    const int64_t row_idx,
    uint64_t &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= col_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index", K(ret), K(col_idx), K(col_cnt_));
  } else if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row index", K(ret), K(row_idx), K(header_->row_count_));
  } else {
    ObPostingColumnDecodeCtx &col_ctx = col_ctxs_[col_idx];
    if (OB_ISNULL(col_ctx.meta_) || OB_ISNULL(col_ctx.col_data_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null column decode ctx", K(ret), K(col_idx));
    } else if (is_packed_base_posting_col_encoding(col_ctx.meta_->encoding_type_)) {
      if (OB_FAIL(get_packed_base_value(col_ctx, row_idx, value))) {
        STORAGE_LOG(WARN, "failed to get packed base value", K(ret), K(col_idx), K(row_idx));
      }
    } else if (OB_FAIL(ensure_ob_codec_decoded(col_idx, col_ctx))) {
      STORAGE_LOG(WARN, "failed to ensure ob_codec decoded", K(ret), K(col_idx), K(row_idx));
    } else if (OB_ISNULL(col_ctx.decoded_values_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "decoded values is null", K(ret), K(col_idx));
    } else {
      value = col_ctx.decoded_values_[row_idx];
    }
  }
  return ret;
}

int ObPostingBlockDecoder::get_doc_id(const int64_t idx, uint64_t &doc_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row index", K(ret), K(idx), K(header_->row_count_));
  } else if (OB_FAIL(get_column_value(POSTING_DOC_ID_COL_IDX, idx, doc_id))) {
    STORAGE_LOG(WARN, "failed to get doc_id", K(ret), K(idx));
  }
  return ret;
}

int ObPostingBlockDecoder::get_relevance_params(
    const int64_t idx,
    uint64_t &token_freq,
    uint64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(!has_relevance_params())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "posting block has no relevance params", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row index", K(ret), K(idx), K(header_->row_count_));
  } else if (OB_FAIL(get_column_value(POSTING_TOKEN_FREQ_COL_IDX, idx, token_freq))) {
    STORAGE_LOG(WARN, "failed to get token_freq", K(ret), K(idx));
  } else if (OB_FAIL(get_column_value(POSTING_DOC_LENGTH_COL_IDX, idx, doc_length))) {
    STORAGE_LOG(WARN, "failed to get doc_length", K(ret), K(idx));
  }
  return ret;
}

int ObPostingBlockDecoder::lower_bound(
    const uint64_t target_doc_id,
    const int64_t start_idx,
    const int64_t end_idx,
    int64_t &target_idx) const
{
  int ret = OB_SUCCESS;
  target_idx = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx || end_idx > header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid lower_bound range", K(ret), K(start_idx), K(end_idx), K(header_->row_count_));
  } else {
    ObPostingColumnDecodeCtx &doc_ctx = col_ctxs_[POSTING_DOC_ID_COL_IDX];
    if (OB_ISNULL(doc_ctx.meta_) || OB_ISNULL(doc_ctx.col_data_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null doc_id decode ctx", K(ret));
    } else if (OB_FAIL(ensure_column_materialized(POSTING_DOC_ID_COL_IDX, doc_ctx))) {
      STORAGE_LOG(WARN, "failed to materialize doc_id column for lower_bound", K(ret));
    } else if (OB_ISNULL(doc_ctx.decoded_values_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "materialized doc_id values is null", K(ret));
    } else if (OB_FAIL(lower_bound_uint64_array(
            doc_ctx.decoded_values_, target_doc_id, start_idx, end_idx, target_idx))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "failed to lower_bound on doc_id column", K(ret), K(target_doc_id),
            K(start_idx), K(end_idx));
      }
    }
  }
  return ret;
}

int ObPostingBlockDecoder::decode_to_block_data(
    ObIAllocator &out_alloc, ObPostingBlockData &out)
{
  int ret = OB_SUCCESS;
  out.doc_count_ = 0;
  out.doc_ids_ = nullptr;
  out.token_freqs_ = nullptr;
  out.doc_lengths_ = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block decoder not inited", K(ret));
  } else {
    const int64_t row_count = header_->row_count_;
    const int64_t col_cnt = header_->get_column_count();
    const int64_t total_size = row_count * sizeof(uint64_t) * col_cnt;
    char *buf = static_cast<char *>(out_alloc.alloc(total_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc block data buffer", K(ret), K(total_size));
    } else {
      uint64_t *doc_ids = reinterpret_cast<uint64_t *>(buf);
      const bool has_relevance = has_relevance_params();
      uint64_t *token_freqs = has_relevance
          ? reinterpret_cast<uint64_t *>(buf + row_count * sizeof(uint64_t))
          : nullptr;
      uint64_t *doc_lengths = has_relevance
          ? reinterpret_cast<uint64_t *>(buf + 2 * row_count * sizeof(uint64_t))
          : nullptr;

      for (int64_t col = 0; OB_SUCC(ret) && col < col_cnt; ++col) {
        uint64_t *dst = reinterpret_cast<uint64_t *>(buf + col * row_count * sizeof(uint64_t));
        ObPostingColumnDecodeCtx &col_ctx = col_ctxs_[col];
        if (OB_FAIL(decode_column_batch(col_ctx, row_count, dst))) {
          STORAGE_LOG(WARN, "failed to batch decode posting column", K(ret), K(col));
        }
      }
      if (OB_SUCC(ret)) {
        out.doc_count_ = row_count;
        out.doc_ids_ = doc_ids;
        out.token_freqs_ = token_freqs;
        out.doc_lengths_ = doc_lengths;
      } else {
        out_alloc.free(buf);
      }
    }
  }
  return ret;
}

void ObPostingBlockDecoder::reuse()
{
  for (int64_t i = 0; i < col_cnt_; ++i) {
    col_ctxs_[i].reset();
  }
  header_ = nullptr;
  col_metas_ = nullptr;
  col_cnt_ = 0;
  is_inited_ = false;
}

void ObPostingBlockDecoder::reset()
{
  reuse();
  decode_arena_.reset();
  decode_buf_ = nullptr;
}

} // end namespace blocksstable
} // end namespace oceanbase
