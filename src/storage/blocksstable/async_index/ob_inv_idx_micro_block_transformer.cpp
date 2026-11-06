/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_transformer.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObInvIdxMicroBlockTransformer::ObInvIdxMicroBlockTransformer()
  : is_inited_(false),
    header_(),
    reader_(),
    layout_plan_(),
    decode_allocator_(nullptr),
    decomp_buf_(nullptr)
{}

ObInvIdxMicroBlockTransformer::~ObInvIdxMicroBlockTransformer()
{
  reset();
}

void ObInvIdxMicroBlockTransformer::reset()
{
  if (OB_NOT_NULL(decomp_buf_) && OB_NOT_NULL(decode_allocator_)) {
    decode_allocator_->free(decomp_buf_);
  }
  is_inited_ = false;
  header_.reset();
  reader_.reuse();
  layout_plan_ = FlatCacheLayoutPlan();
  decode_allocator_ = nullptr;
  decomp_buf_ = nullptr;
}

int ObInvIdxMicroBlockTransformer::init(
    const ObMicroBlockHeader &header,
    const char *micro_block_buf,
    const int64_t micro_block_size,
    const ObMicroBlockDesMeta &des_meta,
    ObMacroBlockReader &reader,
    common::ObIAllocator &decode_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transformer has been inited", K(ret));
  } else if (OB_ISNULL(micro_block_buf) || OB_UNLIKELY(micro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(micro_block_buf), K(micro_block_size));
  } else {
    decode_allocator_ = &decode_allocator;
    header_ = header;
    decomp_buf_ = nullptr;
    if (header.is_compressed_data()) {
      if (OB_FAIL(decompress(micro_block_buf, des_meta, reader))) {
        LOG_WARN("failed to decompress posting micro block source", K(ret), K(micro_block_size));
      }
    } else if (OB_FAIL(reader_.init(micro_block_buf, micro_block_size, false /*is_posting_value*/))) {
      LOG_WARN("failed to init inv idx micro block reader", K(ret), K(micro_block_size));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::decompress(
    const char *micro_block_buf,
    const ObMicroBlockDesMeta &des_meta,
    ObMacroBlockReader &reader)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = static_cast<int64_t>(header_.header_size_);
  const int64_t uncomp_size = header_size + static_cast<int64_t>(header_.data_length_);
  int64_t pos = 0;
  ObMicroBlockHeader *new_header = nullptr;
  decomp_buf_ = nullptr;
  if (OB_UNLIKELY(header_size <= 0 || header_.data_length_ <= 0 || header_.data_zlength_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compressed posting header", K(ret), K(header_));
  } else if (OB_ISNULL(decomp_buf_ = static_cast<char *>(decode_allocator_->alloc(uncomp_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc decompress buf", K(ret), K(uncomp_size));
  } else if (OB_FAIL(reader.decompress_data_with_prealloc_buf(
      des_meta.compressor_type_,
      micro_block_buf + header_size,
      static_cast<int64_t>(header_.data_zlength_),
      decomp_buf_ + header_size,
      static_cast<int64_t>(header_.data_length_)))) {
    LOG_WARN("failed to decompress posting payload", K(ret), K(header_));
  } else if (OB_FAIL(header_.deep_copy(decomp_buf_, uncomp_size, pos, new_header))) {
    LOG_WARN("failed to copy posting micro block header", K(ret), K(header_), K(uncomp_size));
  } else if (OB_UNLIKELY(pos != header_size || OB_ISNULL(new_header))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected copied posting header", K(ret), K(pos), K(header_size), KP(new_header));
  } else {
    new_header->data_zlength_ = new_header->data_length_;
    new_header->original_length_ = new_header->data_length_;
    new_header->data_checksum_ = ob_crc64_sse42(
        0, decomp_buf_ + header_size, static_cast<int64_t>(new_header->data_length_));
    new_header->set_header_checksum();
    if (OB_FAIL(reader_.init(decomp_buf_, uncomp_size, false /*is_posting_value*/))) {
      LOG_WARN("failed to init inv idx micro block reader on decompressed source", K(ret), K(uncomp_size));
    }
  }
  if (OB_SUCCESS != ret && OB_NOT_NULL(decomp_buf_)) {
    decode_allocator_->free(decomp_buf_);
    decomp_buf_ = nullptr;
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::get_buf_size(int64_t &buf_size) const
{
  int ret = OB_SUCCESS;
  buf_size = 0;
  if (OB_FAIL(prepare_layout_plan())) {
    LOG_WARN("failed to prepare FlatCache layout", K(ret));
  } else {
    buf_size = layout_plan_.buf_size_;
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::prepare_layout_plan() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transformer not inited", K(ret));
  } else if (!layout_plan_.is_valid()) {
    FlatCacheLayoutPlan plan;
    if (OB_FAIL(build_layout_plan(plan))) {
      LOG_WARN("failed to build FlatCache layout", K(ret));
    } else {
      layout_plan_ = plan;
    }
  }
  return ret;
}

static int read_last_packed_base_doc_id(
    const char *encoded_block,
    const int64_t encoded_size,
    const ObPostingBlockHeader &block_header,
    const ObPostingColumnMeta *metas,
    uint64_t &last_doc_id)
{
  int ret = OB_SUCCESS;
  last_doc_id = 0;
  const ObPostingColumnMeta &meta = metas[POSTING_DOC_ID_COL_IDX];
  const int64_t doc_column_end = static_cast<int64_t>(meta.offset_)
      + static_cast<int64_t>(block_header.row_count_) * meta.packed_width_;
  if (OB_UNLIKELY(doc_column_end > encoded_size
      || doc_column_end != metas[POSTING_TOKEN_FREQ_COL_IDX].offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid packed doc id column size", K(ret), K(doc_column_end), K(encoded_size), K(meta));
  } else {
    ObIntegerArrayGenerator gen;
    if (OB_FAIL(gen.init(encoded_block + meta.offset_, meta.packed_width_))) {
      LOG_WARN("failed to read last packed doc id delta", K(ret), K(meta));
    } else {
      last_doc_id = static_cast<uint64_t>(meta.base_value_)
          + static_cast<uint64_t>(gen.get_array().at(block_header.row_count_ - 1));
    }
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::build_layout_plan(FlatCacheLayoutPlan &plan) const
{
  int ret = OB_SUCCESS;
  plan = FlatCacheLayoutPlan();
  uint32_t doc_id_width = sizeof(uint64_t);
  uint32_t relevance_width = 0;
  const int64_t block_count = reader_.get_block_count();
  int64_t total_doc_count = 0;
  int64_t column_count = 0;
  if (OB_UNLIKELY(block_count <= 0
      || block_count > ObFlatCacheHeader::MAX_BLOCK_COUNT
      || 0 == header_.row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected FlatCache source summary", K(ret), K(block_count), K(header_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
    const char *encoded_block = nullptr;
    int64_t encoded_size = 0;
    if (OB_FAIL(reader_.get_payload(i, encoded_block, encoded_size))) {
      LOG_WARN("failed to get encoded posting block", K(ret), K(i), K(block_count));
    } else if (OB_UNLIKELY(encoded_size < static_cast<int64_t>(sizeof(ObPostingBlockHeader)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encoded posting block is truncated", K(ret), K(i), K(encoded_size));
    } else {
      const ObPostingBlockHeader *block_header = reinterpret_cast<const ObPostingBlockHeader *>(encoded_block);
      if (OB_UNLIKELY(!block_header->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid encoded posting block header", K(ret), K(i), KPC(block_header));
      } else if (OB_UNLIKELY(encoded_size < block_header->get_header_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encoded posting column metadata is truncated", K(ret), K(i), K(encoded_size));
      } else if (OB_UNLIKELY(block_header->row_count_ > DEFAULT_POSTING_BLOCK_CAPACITY)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encoded posting row count exceeds capacity", K(ret), K(i),
                 K(block_header->row_count_), "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
      } else if (OB_UNLIKELY(i > 0 && column_count != block_header->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inconsistent FlatCache source column count", K(ret), K(i),
                  K(column_count), "current_column_count", block_header->get_column_count());
      } else if (OB_UNLIKELY(total_doc_count
                  > static_cast<int64_t>(header_.row_count_) - block_header->row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("FlatCache source exceeds outer row count", K(ret), K(i),
                  K(total_doc_count), K(block_header->row_count_), K(header_.row_count_));
      } else {
        if (0 == i) {
          column_count = block_header->get_column_count();
          if (block_header->has_relevance_params_) {
            // Keep doc-only SEARCH cache values uint64 for its physical range API.
            doc_id_width = sizeof(uint32_t);
            relevance_width = sizeof(uint32_t);
          }
        }
        total_doc_count += block_header->row_count_;
        const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
            encoded_block + sizeof(ObPostingBlockHeader));
        for (int64_t col = 0; OB_SUCC(ret) && col < column_count; ++col) {
          const ObPostingColumnMeta &meta = metas[col];
          uint32_t &width = POSTING_DOC_ID_COL_IDX == col ? doc_id_width : relevance_width;
          if (OB_UNLIKELY(!meta.is_valid(block_header->get_header_size())
              || meta.offset_ >= encoded_size
              || (col > 0 && meta.offset_ <= metas[col - 1].offset_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid posting column metadata while sizing cache", K(ret), K(i), K(col), K(meta));
          } else if (sizeof(uint64_t) == width) {
            // skip
          } else if (meta.packed_width_ > sizeof(uint32_t)) {
            width = sizeof(uint64_t);
          } else if (is_packed_base_posting_col_encoding(meta.encoding_type_)) {
            const uint64_t headroom = static_cast<uint64_t>(UINT32_MAX) - meta.base_value_;
            const uint64_t max_delta = (1ULL << (8 * meta.packed_width_)) - 1;
            if (max_delta > headroom) {
              if (POSTING_DOC_ID_COL_IDX == col) {
                uint64_t last_doc_id = 0;
                if (OB_FAIL(read_last_packed_base_doc_id(
                    encoded_block, encoded_size, *block_header, metas, last_doc_id))) {
                  LOG_WARN("failed to read packed doc id maximum", K(ret), K(i));
                } else if (last_doc_id > UINT32_MAX) {
                  width = sizeof(uint64_t);
                }
              } else {
                width = sizeof(uint64_t);
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(total_doc_count != header_.row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FlatCache source row count mismatch", K(ret), K(total_doc_count),
             K(header_.row_count_), K(block_count));
  } else if (OB_SUCC(ret)) {
    const uint64_t total_size = ObFlatCacheHeader::payload_size(
        block_count, header_.row_count_, doc_id_width, relevance_width);
    if (OB_UNLIKELY(total_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("FlatCache payload exceeds micro header capacity", K(ret),
               K(total_size), K(block_count), K(header_.row_count_));
    } else if (OB_UNLIKELY(static_cast<int64_t>(header_.header_size_)
               > INT64_MAX - static_cast<int64_t>(total_size))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("posting cache block size overflow", K(ret), K(header_), K(total_size));
    } else {
      plan.block_count_ = block_count;
      plan.column_count_ = column_count;
      plan.doc_id_width_ = doc_id_width;
      plan.relevance_width_ = relevance_width;
      plan.doc_ids_offset_ = ObFlatCacheHeader::doc_ids_offset(block_count);
      plan.token_freqs_offset_ = ObFlatCacheHeader::relevance_offset(
          block_count, header_.row_count_, doc_id_width);
      plan.doc_lengths_offset_ = plan.token_freqs_offset_
          + static_cast<int64_t>(header_.row_count_) * relevance_width;
      plan.payload_size_ = static_cast<int64_t>(total_size);
      plan.buf_size_ = static_cast<int64_t>(header_.header_size_) + plan.payload_size_;
    }
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::transform(char *block_buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMicroBlockHeader *out_header = nullptr;
  int64_t payload_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transformer not inited", K(ret));
  } else if (OB_ISNULL(block_buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(block_buf), K(buf_size));
  } else if (OB_FAIL(header_.deep_copy(block_buf, buf_size, pos, out_header))) {
    LOG_WARN("failed to deep copy physical header", K(ret), K(buf_size));
  } else if (OB_FAIL(prepare_layout_plan())) {
    LOG_WARN("failed to prepare FlatCache layout", K(ret));
  } else if (OB_FAIL(write_payload(layout_plan_, buf_size - pos, block_buf + pos, payload_size))) {
    LOG_WARN("failed to write FlatCache payload", K(ret), K(pos), K(buf_size));
  } else if (OB_UNLIKELY(pos + payload_size != buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transformed size mismatch", K(ret), K(pos), K(payload_size), K(buf_size));
  } else {
    // rewrite the physical header to describe the FlatCache payload
    out_header->data_length_ = static_cast<int32_t>(payload_size);
    out_header->data_zlength_ = static_cast<int32_t>(payload_size);
    out_header->original_length_ = static_cast<int32_t>(payload_size);
    out_header->data_checksum_ = ob_crc64_sse42(0, block_buf + pos, payload_size);
    out_header->set_header_checksum();
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::write_flat_cache_block(
    const int64_t block_idx,
    const FlatCacheLayoutPlan &plan,
    ObFlatCacheBlockDesc &desc,
    char *doc_ids,
    char *token_freqs,
    char *doc_lengths,
    int64_t &doc_offset)
{
  int ret = OB_SUCCESS;
  const char *encoded_block = nullptr;
  int64_t encoded_size = 0;
  ObPostingBlockDecoder decoder;
  if (OB_FAIL(reader_.get_payload(block_idx, encoded_block, encoded_size))) {
    LOG_WARN("failed to get encoded posting block", K(ret), K(block_idx));
  } else if (OB_UNLIKELY(encoded_size < static_cast<int64_t>(sizeof(ObPostingBlockHeader)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("encoded posting block is truncated", K(ret), K(block_idx), K(encoded_size));
  } else {
    const ObPostingBlockHeader *block_header = reinterpret_cast<const ObPostingBlockHeader *>(encoded_block);
    if (OB_UNLIKELY(!block_header->is_valid()
        || block_header->get_column_count() != plan.column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid FlatCache source block", K(ret), K(block_idx), KPC(block_header), K(plan.column_count_));
    } else if (OB_FAIL(decoder.init(encoded_block, encoded_size, *decode_allocator_))) {
      LOG_WARN("failed to init FlatCache decoder", K(ret), K(block_idx), K(encoded_size));
    } else if (OB_UNLIKELY(decoder.get_row_count() != block_header->row_count_
               || doc_offset < 0
               || doc_offset > static_cast<int64_t>(header_.row_count_)
               || block_header->row_count_
                  > static_cast<int64_t>(header_.row_count_) - doc_offset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("FlatCache block does not match planned rows", K(ret), K(block_idx),
               K(doc_offset), K(block_header->row_count_), K(header_.row_count_),
               "decoder_row_count", decoder.get_row_count());
    } else {
      desc.doc_offset_ = static_cast<uint32_t>(doc_offset);
      desc.doc_count_ = block_header->row_count_;
      doc_offset += block_header->row_count_;
      for (int64_t r = 0; OB_SUCC(ret) && r < block_header->row_count_; ++r) {
        const int64_t output_idx = static_cast<int64_t>(desc.doc_offset_) + r;
        uint64_t doc_id = 0;
        if (OB_FAIL(decoder.get_doc_id(r, doc_id))) {
          LOG_WARN("failed to decode FlatCache doc id", K(ret), K(block_idx), K(r));
        } else if (sizeof(uint32_t) == plan.doc_id_width_ && doc_id > UINT32_MAX) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("compact posting doc id exceeds planned width", K(ret), K(block_idx), K(r), K(doc_id));
        } else {
          if (sizeof(uint32_t) == plan.doc_id_width_) {
            reinterpret_cast<uint32_t *>(doc_ids)[output_idx] = static_cast<uint32_t>(doc_id);
          } else {
            reinterpret_cast<uint64_t *>(doc_ids)[output_idx] = doc_id;
          }
        }
        if (OB_SUCC(ret) && block_header->has_relevance_params_) {
          uint64_t token_freq = 0;
          uint64_t doc_length = 0;
          if (OB_FAIL(decoder.get_relevance_params(r, token_freq, doc_length))) {
            LOG_WARN("failed to decode FlatCache relevance params", K(ret), K(block_idx), K(r));
          } else if (sizeof(uint32_t) == plan.relevance_width_) {
            if (OB_UNLIKELY(token_freq > UINT32_MAX || doc_length > UINT32_MAX)) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("compact posting relevance exceeds planned width", K(ret),
                  K(block_idx), K(r), K(token_freq), K(doc_length));
            } else {
              reinterpret_cast<uint32_t *>(token_freqs)[output_idx] = static_cast<uint32_t>(token_freq);
              reinterpret_cast<uint32_t *>(doc_lengths)[output_idx] = static_cast<uint32_t>(doc_length);
            }
          } else {
            reinterpret_cast<uint64_t *>(token_freqs)[output_idx] = token_freq;
            reinterpret_cast<uint64_t *>(doc_lengths)[output_idx] = doc_length;
          }
        }
      }
    }
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::write_payload(
    const FlatCacheLayoutPlan &plan,
    const int64_t dst_size,
    char *dst,
    int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  payload_size = 0;
  const int64_t block_count = plan.block_count_;
  if (OB_ISNULL(dst) || OB_UNLIKELY(block_count <= 0
      || block_count > ObFlatCacheHeader::MAX_BLOCK_COUNT
      || 0 == header_.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FlatCache output", K(ret), KP(dst), K(dst_size),
             K(block_count), K(header_.row_count_));
  } else if (OB_UNLIKELY(plan.payload_size_ != dst_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("unexpected FlatCache output size", K(ret), K(dst_size), K(plan.payload_size_),
             K(block_count), K(header_.row_count_));
  } else if (OB_UNLIKELY(0 != reinterpret_cast<uintptr_t>(dst) % sizeof(uint64_t))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("misaligned FlatCache output", K(ret), KP(dst));
  } else {
    MEMSET(dst, 0, plan.doc_ids_offset_);
    ObFlatCacheHeader *flat_cache_header = new (dst) ObFlatCacheHeader();
    flat_cache_header->column_count_ = static_cast<uint8_t>(plan.column_count_);
    flat_cache_header->doc_id_width_ = static_cast<uint16_t>(plan.doc_id_width_);
    flat_cache_header->relevance_width_ = static_cast<uint16_t>(plan.relevance_width_);
    ObFlatCacheBlockDesc *descs = reinterpret_cast<ObFlatCacheBlockDesc *>(dst + sizeof(ObFlatCacheHeader));
    char *doc_ids = dst + plan.doc_ids_offset_;
    char *token_freqs = dst + plan.token_freqs_offset_;
    char *doc_lengths = dst + plan.doc_lengths_offset_;
    // The alignment bytes are deterministic when a compact micro has odd rows.
    MEMSET(doc_ids + static_cast<int64_t>(header_.row_count_) * plan.doc_id_width_, 0,
        token_freqs - (doc_ids + static_cast<int64_t>(header_.row_count_) * plan.doc_id_width_));
    int64_t doc_offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      ObFlatCacheBlockDesc *desc = new (&descs[i]) ObFlatCacheBlockDesc();
      if (OB_FAIL(write_flat_cache_block(i, plan, *desc, doc_ids, token_freqs, doc_lengths, doc_offset))) {
        LOG_WARN("failed to write FlatCache block", K(ret), K(i), K(block_count));
      } else if (i + 1 == block_count
                 && OB_UNLIKELY(doc_offset != header_.row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("FlatCache output doc count mismatch", K(ret), K(doc_offset), K(header_.row_count_));
      } else if (i + 1 == block_count) {
        flat_cache_header->block_count_ = static_cast<uint16_t>(block_count);
        payload_size = plan.payload_size_;
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
