/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"

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

int ObPostingBlockEncoder::assign_column_data_offset(
    const int64_t offset,
    ObPostingColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || offset > UINT16_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "posting column data offset exceeds UINT16_MAX", K(ret), K(offset));
  } else {
    meta.offset_ = static_cast<uint16_t>(offset);
  }
  return ret;
}

int ObPostingBlockEncoder::dispatch_ob_codec_encode(
    const ObPostingColEncodingType encoding_type,
    const uint8_t uint_bytes,
    const char *in_buf,
    const uint64_t in_len,
    char *out_buf,
    const uint64_t out_buf_len,
    uint64_t &out_pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_buf) || OB_ISNULL(out_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid codec encode buffer", K(ret), KP(in_buf), KP(out_buf));
  } else if (OB_UNLIKELY(0 == in_len || 0 == out_buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid codec encode buffer size", K(ret), K(in_len), K(out_buf_len));
  } else if (OB_UNLIKELY(1 != uint_bytes && 2 != uint_bytes && 4 != uint_bytes && 8 != uint_bytes)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid uint bytes for codec encode", K(ret), K(uint_bytes));
  } else if (OB_UNLIKELY(0 != in_len % uint_bytes)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "codec encode input length not aligned to uint bytes",
        K(ret), K(in_len), K(uint_bytes));
  } else {
    switch (encoding_type) {
      case ObPostingColEncodingType::SIMD_FIXEDPFOR: {
        ObCompositeCodec<ObSIMDFixedPFor, ObSimpleBitPacking> codec;
        codec.set_uint_bytes(uint_bytes);
        codec.set_pfor_packing_type(ObCodec::CPU_ARCH_INDEPENDANT_SCALAR);
        if (OB_FAIL(codec.encode(in_buf, in_len, out_buf, out_buf_len, out_pos))) {
          STORAGE_LOG(WARN, "failed to encode posting column with simd fixedpfor", K(ret));
        }
        break;
      }
      case ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR: {
        ObDoubleDeltaZigzagPFor codec;
        codec.set_uint_bytes(uint_bytes);
        codec.set_pfor_packing_type(ObCodec::CPU_ARCH_INDEPENDANT_SCALAR);
        if (OB_FAIL(codec.encode(in_buf, in_len, out_buf, out_buf_len, out_pos))) {
          STORAGE_LOG(WARN, "failed to encode posting column with double delta zigzag pfor", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "unsupported ob_codec encoding type", K(ret), K(encoding_type));
        break;
      }
    }
  }
  return ret;
}

ObPostingBlockEncoder::ObPostingBlockEncoder()
  : ctx_(nullptr),
    allocator_(nullptr),
    codec_scratch_arena_("PostBlkCodecScr"),
    doc_ids_(),
    token_freqs_(),
    doc_lengths_(),
    data_buffer_(),
    last_doc_id_(0),
    is_inited_(false)
{
}

ObPostingBlockEncoder::~ObPostingBlockEncoder()
{
  reset();
}

int ObPostingBlockEncoder::validate_col_desc(
    const ObPostingBlockEncodingCtx &ctx,
    const int16_t col_idx,
    const char *col_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(ctx.col_descs_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "col_descs is null", K(ret), K(col_name));
  } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= ctx.col_descs_->count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting column index in col_desc",
        K(ret), K(col_name), K(col_idx), "col_desc_cnt", ctx.col_descs_->count());
  } else if (OB_UNLIKELY(!ctx.col_descs_->at(col_idx).col_type_.is_uint64())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting column must be uint64 in col_desc",
        K(ret), K(col_name), K(col_idx), K(ctx.col_descs_->at(col_idx).col_type_));
  }
  return ret;
}

int ObPostingBlockEncoder::validate_col_descs(const ObPostingBlockEncodingCtx &ctx)
{
  int ret = OB_SUCCESS;
  // append_col_value() reads datum via get_uint64(), validate column types here.
  if (OB_FAIL(validate_col_desc(ctx, ctx.doc_id_col_idx_, "doc_id"))) {
    STORAGE_LOG(WARN, "failed to validate doc_id column desc",
        K(ret), K(ctx.doc_id_col_idx_), KPC(ctx.col_descs_));
  } else if (ctx.has_token_freq()
             && OB_FAIL(validate_col_desc(ctx, ctx.token_freq_col_idx_, "token_freq"))) {
    STORAGE_LOG(WARN, "failed to validate token_freq column desc",
        K(ret), K(ctx.token_freq_col_idx_), KPC(ctx.col_descs_));
  } else if (ctx.has_doc_length()
             && OB_FAIL(validate_col_desc(ctx, ctx.doc_length_col_idx_, "doc_length"))) {
    STORAGE_LOG(WARN, "failed to validate doc_length column desc",
        K(ret), K(ctx.doc_length_col_idx_), KPC(ctx.col_descs_));
  }
  return ret;
}

int ObPostingBlockEncoder::init(
    const ObPostingBlockEncodingCtx &ctx,
    ObIAllocator &allocator,
    const int64_t max_pair_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting block encoding ctx", K(ret), K(ctx));
  } else if (OB_FAIL(validate_col_descs(ctx))) {
    STORAGE_LOG(WARN, "failed to validate posting column desc", K(ret), K(ctx));
  } else if (ctx.has_token_freq() && OB_UNLIKELY(max_pair_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid bm25 max pair count for relevance posting block",
        K(ret), K(max_pair_count));
  } else {
    ctx_ = &ctx;
    allocator_ = &allocator;
    doc_ids_.set_allocator(allocator_);
    token_freqs_.set_allocator(allocator_);
    doc_lengths_.set_allocator(allocator_);
    if (OB_FAIL(doc_ids_.init(ctx.block_capacity_))) {
      STORAGE_LOG(WARN, "failed to init doc_ids array", K(ret), K(ctx.block_capacity_));
    } else if (ctx.has_token_freq() && OB_FAIL(token_freqs_.init(ctx.block_capacity_))) {
      STORAGE_LOG(WARN, "failed to init token_freqs array", K(ret), K(ctx.block_capacity_));
    } else if (ctx.has_doc_length() && OB_FAIL(doc_lengths_.init(ctx.block_capacity_))) {
      STORAGE_LOG(WARN, "failed to init doc_lengths array", K(ret), K(ctx.block_capacity_));
    } else if (ctx.has_token_freq()
               && OB_FAIL(bm25_max_score_aggregator_.init(allocator, max_pair_count))) {
      STORAGE_LOG(WARN, "failed to init bm25 max score aggregator", K(ret), K(max_pair_count));
    } else if (OB_FAIL(data_buffer_.init(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
      STORAGE_LOG(WARN, "failed to init posting block data buffer", K(ret));
    } else {
      last_doc_id_ = 0;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObPostingBlockEncoder::append_col_value(
    const ObDatumRow &row,
    const int64_t col_idx,
    ColumnValueArray &values)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_idx < 0 || col_idx >= row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column index", K(ret), K(col_idx), K(row.get_column_count()));
  } else if (OB_FAIL(values.push_back(row.storage_datums_[col_idx].get_uint64()))) {
    STORAGE_LOG(WARN, "failed to push column value", K(ret), K(col_idx));
  }
  return ret;
}

int ObPostingBlockEncoder::append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block encoder not inited", K(ret));
  } else if (OB_UNLIKELY(is_full())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting row", K(ret));
  } else if (OB_UNLIKELY(ctx_->doc_id_col_idx_ >= row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid doc_id column index", K(ret), KPC_(ctx), K(row.get_column_count()));
  } else {
    const uint64_t doc_id = row.storage_datums_[ctx_->doc_id_col_idx_].get_uint64();
    if (OB_UNLIKELY(doc_ids_.count() > 0 && doc_id <= last_doc_id_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "doc_id must be strictly increasing", K(ret), K(doc_id), K_(last_doc_id));
    } else if (OB_FAIL(append_col_value(row, ctx_->doc_id_col_idx_, doc_ids_))) {
    } else if (ctx_->has_token_freq()
               && OB_FAIL(append_col_value(row, ctx_->token_freq_col_idx_, token_freqs_))) {
    } else if (ctx_->has_doc_length()
               && OB_FAIL(append_col_value(row, ctx_->doc_length_col_idx_, doc_lengths_))) {
    } else {
      last_doc_id_ = doc_id;
    }
  }
  return ret;
}

uint8_t ObPostingBlockEncoder::calc_packed_width(const uint64_t max_diff)
{
  uint8_t width = 8;
  if (max_diff <= UINT8_MAX) {
    width = 1;
  } else if (max_diff <= UINT16_MAX) {
    width = 2;
  } else if (max_diff <= UINT32_MAX) {
    width = 4;
  }
  return width;
}

uint8_t ObPostingBlockEncoder::determine_uint_bytes(const ColumnValueArray &values)
{
  uint64_t max_value = 0;
  uint8_t uint_bytes = 8;
  for (int64_t i = 0; i < values.count(); ++i) {
    if (values.at(i) > max_value) {
      max_value = values.at(i);
    }
  }
  if (max_value <= UINT8_MAX) {
    uint_bytes = 1;
  } else if (max_value <= UINT16_MAX) {
    uint_bytes = 2;
  } else if (max_value <= UINT32_MAX) {
    uint_bytes = 4;
  }
  return uint_bytes;
}

int ObPostingBlockEncoder::fill_uint_buffer(
    const ColumnValueArray &values,
    const uint8_t uint_bytes,
    char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = values.count();
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty column values for uint buffer", K(ret), K(row_count));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size < row_count * uint_bytes)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid uint buffer", K(ret), KP(buf), K(buf_size), K(row_count), K(uint_bytes));
  } else if (1 == uint_bytes) {
    uint8_t *dst = reinterpret_cast<uint8_t *>(buf);
    for (int64_t i = 0; i < row_count; ++i) {
      dst[i] = static_cast<uint8_t>(values.at(i));
    }
  } else if (2 == uint_bytes) {
    uint16_t *dst = reinterpret_cast<uint16_t *>(buf);
    for (int64_t i = 0; i < row_count; ++i) {
      dst[i] = static_cast<uint16_t>(values.at(i));
    }
  } else if (4 == uint_bytes) {
    uint32_t *dst = reinterpret_cast<uint32_t *>(buf);
    for (int64_t i = 0; i < row_count; ++i) {
      dst[i] = static_cast<uint32_t>(values.at(i));
    }
  } else if (8 == uint_bytes) {
    uint64_t *dst = reinterpret_cast<uint64_t *>(buf);
    for (int64_t i = 0; i < row_count; ++i) {
      dst[i] = values.at(i);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid uint bytes", K(ret), K(uint_bytes));
  }
  return ret;
}

int ObPostingBlockEncoder::encode_packed_base(
    const ColumnValueArray &values,
    const int64_t col_meta_idx)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = values.count();
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty column to encode with packed base", K(ret));
  } else if (OB_UNLIKELY(col_meta_idx < 0
                         || col_meta_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column meta index for packed base encode", K(ret), K(col_meta_idx));
  } else if (OB_ISNULL(ctx_) || OB_UNLIKELY(row_count > ctx_->block_capacity_)
             || OB_UNLIKELY(row_count > DEFAULT_POSTING_BLOCK_CAPACITY)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row count exceeds posting block capacity for packed base encode",
        K(ret), K(row_count), "block_capacity", OB_ISNULL(ctx_) ? -1 : ctx_->block_capacity_);
  } else {
    uint64_t base_value = values.at(0);
    uint64_t max_diff = 0;
    if (POSTING_DOC_ID_COL_IDX == col_meta_idx) {
      // doc_id is strictly ascending, min/max are at both ends
      max_diff = values.at(row_count - 1) - base_value;
    } else {
      for (int64_t i = 1; i < row_count; ++i) {
        const uint64_t value = values.at(i);
        if (value < base_value) {
          base_value = value;
        }
      }
      for (int64_t i = 0; i < row_count; ++i) {
        const uint64_t diff = values.at(i) - base_value;
        if (diff > max_diff) {
          max_diff = diff;
        }
      }
    }
    const uint8_t packed_width = calc_packed_width(max_diff);
    const int64_t col_data_size = row_count * packed_width;
    if (OB_FAIL(data_buffer_.ensure_space(col_data_size))) {
      STORAGE_LOG(WARN, "failed to ensure space for column data", K(ret), K(col_data_size));
    } else {
      ObIntegerArrayGenerator gen;
      char *col_buf = data_buffer_.current();
      if (OB_FAIL(gen.init(col_buf, packed_width))) {
        STORAGE_LOG(WARN, "failed to init integer array generator", K(ret), K(packed_width));
      } else {
        ObIIntegerArray &arr = gen.get_array();
        for (int64_t i = 0; i < row_count; ++i) {
          arr.set(i, static_cast<int64_t>(values.at(i) - base_value));
        }
        if (OB_FAIL(data_buffer_.advance(col_data_size))) {
          STORAGE_LOG(WARN, "failed to advance data buffer", K(ret), K(col_data_size));
        } else {
          ObPostingColumnMeta *meta = get_col_meta(col_meta_idx);
          meta->encoding_type_ = ObPostingColEncodingType::PACKED_BASE;
          meta->packed_width_ = packed_width;
          if (OB_FAIL(assign_column_data_offset(data_buffer_.length() - col_data_size, *meta))) {
            STORAGE_LOG(WARN, "failed to assign packed base column offset", K(ret), K(col_data_size));
          } else if (OB_UNLIKELY(base_value > UINT32_MAX)) {
            ret = OB_SIZE_OVERFLOW;
            STORAGE_LOG(WARN, "packed base value exceeds UINT32_MAX", K(ret), K(base_value), K(col_meta_idx));
          } else {
            meta->base_value_ = static_cast<uint32_t>(base_value);
          }
        }
      }
    }
  }
  return ret;
}

int ObPostingBlockEncoder::encode_with_ob_codec(
    const ColumnValueArray &values,
    const ObPostingColEncodingType encoding_type,
    const int64_t col_meta_idx)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = values.count();
  const uint8_t uint_bytes = determine_uint_bytes(values);
  const uint64_t in_len = row_count * uint_bytes;
  char *in_buf = nullptr;
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args for ob_codec encoding", K(ret), K(row_count));
  } else if (OB_ISNULL(ctx_) || OB_UNLIKELY(row_count > ctx_->block_capacity_)
             || OB_UNLIKELY(row_count > DEFAULT_POSTING_BLOCK_CAPACITY)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row count exceeds posting block capacity for ob_codec encode",
        K(ret), K(row_count), "block_capacity", OB_ISNULL(ctx_) ? -1 : ctx_->block_capacity_);
  } else if (OB_UNLIKELY(col_meta_idx < 0
                         || col_meta_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column meta index for ob_codec encode", K(ret), K(col_meta_idx));
  } else if (OB_UNLIKELY(!is_ob_codec_posting_col_encoding(encoding_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ob_codec encoding type", K(ret), K(encoding_type));
  } else {
    in_buf = static_cast<char *>(codec_scratch_arena_.alloc(in_len));
    if (OB_ISNULL(in_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc input buffer for codec", K(ret), K(in_len));
    } else if (OB_FAIL(fill_uint_buffer(values, uint_bytes, in_buf, in_len))) {
      STORAGE_LOG(WARN, "failed to fill codec input buffer", K(ret));
    } else {
      const uint64_t max_encoded_size = ObCodec::get_default_max_encoding_size(in_len);
      if (OB_FAIL(data_buffer_.ensure_space(max_encoded_size))) {
        STORAGE_LOG(WARN, "failed to ensure space for codec output", K(ret), K(max_encoded_size));
      } else {
        char *out_buf = data_buffer_.current();
        const uint64_t out_buf_len = data_buffer_.remain_buffer_size();
        const int64_t col_offset = data_buffer_.length();
        uint64_t out_pos = 0;
        if (OB_FAIL(dispatch_ob_codec_encode(
                encoding_type, uint_bytes, in_buf, in_len, out_buf, out_buf_len, out_pos))) {
          STORAGE_LOG(WARN, "failed to encode posting column with ob_codec",
              K(ret), K(encoding_type), K(col_meta_idx), K(uint_bytes), K(in_len));
        } else if (OB_FAIL(data_buffer_.advance(static_cast<int64_t>(out_pos)))) {
          STORAGE_LOG(WARN, "failed to advance data buffer", K(ret), K(out_pos));
        } else {
          ObPostingColumnMeta *meta = get_col_meta(col_meta_idx);
          meta->encoding_type_ = encoding_type;
          meta->packed_width_ = uint_bytes;
          if (OB_FAIL(assign_column_data_offset(col_offset, *meta))) {
            STORAGE_LOG(WARN, "failed to assign ob_codec column offset", K(ret), K(col_offset));
          } else {
            meta->base_value_ = 0;
          }
        }
      }
    }
  }
  return ret;
}

int ObPostingBlockEncoder::encode_column(
    const ColumnValueArray &values,
    const ObPostingColEncodingType encoding_type,
    const int64_t col_meta_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(values.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty column to encode", K(ret));
  } else if (OB_UNLIKELY(col_meta_idx < 0
                         || col_meta_idx >= ObPostingBlockHeader::MAX_POSTING_COL_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column meta index to encode", K(ret), K(col_meta_idx));
  } else if (is_packed_base_posting_col_encoding(encoding_type)) {
    if (OB_FAIL(encode_packed_base(values, col_meta_idx))) {
      STORAGE_LOG(WARN, "failed to encode packed base column", K(ret));
    }
  } else if (is_ob_codec_posting_col_encoding(encoding_type)) {
    if (OB_FAIL(encode_with_ob_codec(values, encoding_type, col_meta_idx))) {
      STORAGE_LOG(WARN, "failed to encode column with ob_codec", K(ret), K(encoding_type));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting column encoding type", K(ret), K(encoding_type));
  }
  return ret;
}

int ObPostingBlockEncoder::validate_doc_ids_strictly_ascending() const
{
  int ret = OB_SUCCESS;
  const int64_t row_count = doc_ids_.count();
  for (int64_t i = 1; OB_SUCC(ret) && i < row_count; ++i) {
    if (OB_UNLIKELY(doc_ids_.at(i) <= doc_ids_.at(i - 1))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "doc_id must be strictly ascending in posting block",
          K(ret), K(i), "doc_id", doc_ids_.at(i), "prev_doc_id", doc_ids_.at(i - 1));
    }
  }
  return ret;
}

int ObPostingBlockEncoder::build_block(ObPostingBlockDesc &posting_block)
{
  int ret = OB_SUCCESS;
  posting_block.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting block encoder not inited", K(ret));
  } else if (OB_UNLIKELY(doc_ids_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "empty posting block", K(ret));
  } else if (OB_FAIL(validate_doc_ids_strictly_ascending())) {
    STORAGE_LOG(WARN, "invalid doc_id order in posting block encoder", K(ret));
  } else {
    codec_scratch_arena_.reset();
    data_buffer_.reuse();
    if (OB_FAIL(data_buffer_.write_nop(ctx_->get_header_size(), true /*is_zero*/))) {
      STORAGE_LOG(WARN, "failed to reserve posting block header", K(ret), "header_size", ctx_->get_header_size());
    } else if (OB_FAIL(encode_column(doc_ids_, ctx_->doc_id_enc_, POSTING_DOC_ID_COL_IDX))) {
      STORAGE_LOG(WARN, "failed to encode doc_id column", K(ret));
    } else if (ctx_->has_token_freq()
        && OB_FAIL(encode_column(token_freqs_, ctx_->tf_enc_, POSTING_TOKEN_FREQ_COL_IDX))) {
      STORAGE_LOG(WARN, "failed to encode token_freq column", K(ret));
    } else if (ctx_->has_doc_length()
        && OB_FAIL(encode_column(doc_lengths_, ctx_->doc_len_enc_, POSTING_DOC_LENGTH_COL_IDX))) {
      STORAGE_LOG(WARN, "failed to encode doc_length column", K(ret));
    } else {
      ObPostingBlockHeader *header = get_header();
      header->version_ = ObPostingBlockHeader::POSTING_BLOCK_HEADER_VERSION;
      header->is_uint_doc_id_ = 1;
      header->has_relevance_params_ = ctx_->has_relevance_params();
      header->row_count_ = static_cast<uint16_t>(doc_ids_.count());
      posting_block.buf_ = data_buffer_.data();
      posting_block.size_ = data_buffer_.length();
      posting_block.doc_count_ = doc_ids_.count();
      posting_block.min_id_ = doc_ids_.at(0);
      posting_block.max_id_ = doc_ids_.at(doc_ids_.count() - 1);
      posting_block.max_score_pairs_ = nullptr;
      posting_block.total_doc_length_ = 0;
      if (ctx_->has_token_freq() && ctx_->has_doc_length()) {
        const ObIArray<ObInvIdxBM25ParamPair> *frontier_pairs = nullptr;
        for (int64_t i = 0; i < token_freqs_.count(); ++i) {
          posting_block.total_doc_length_ += token_freqs_.at(i);
        }
        if (OB_FAIL(bm25_max_score_aggregator_.aggregate(token_freqs_, doc_lengths_, frontier_pairs))) {
          STORAGE_LOG(WARN, "failed to aggregate bm25 max score params", K(ret));
        } else {
          posting_block.max_score_pairs_ = frontier_pairs;
        }
      }
    }
  }
  return ret;
}

void ObPostingBlockEncoder::reuse()
{
  doc_ids_.clear();
  token_freqs_.clear();
  doc_lengths_.clear();
  data_buffer_.reuse();
  codec_scratch_arena_.reset();
  last_doc_id_ = 0;
  bm25_max_score_aggregator_.reuse();
}

void ObPostingBlockEncoder::reset()
{
  reuse();
  doc_ids_.reuse();
  token_freqs_.reuse();
  doc_lengths_.reuse();
  data_buffer_.reset();
  bm25_max_score_aggregator_.reset();
  ctx_ = nullptr;
  allocator_ = nullptr;
  codec_scratch_arena_.reset();
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase
