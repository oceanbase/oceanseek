/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_transformer.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
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
  int64_t payload_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transformer not inited", K(ret));
  } else if (OB_FAIL(calc_payload_size(payload_size))) {
    LOG_WARN("failed to calc decoded payload size", K(ret));
  } else if (OB_UNLIKELY(static_cast<int64_t>(header_.header_size_)
             > INT64_MAX - payload_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("posting cache block size overflow", K(ret), K(header_), K(payload_size));
  } else {
    buf_size = static_cast<int64_t>(header_.header_size_) + payload_size;
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::calc_payload_size(int64_t &payload_size) const
{
  int ret = OB_SUCCESS;
  payload_size = 0;
  const int64_t block_count = reader_.get_block_count();
  int64_t total_doc_count = 0;
  int64_t column_count = 0;
  if (OB_UNLIKELY(block_count <= 0
      || block_count > ObFlatCacheHeader::MAX_BLOCK_COUNT
      || 0 == header_.row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected FlatCache source summary", K(ret), K(block_count), K(header_));
  } else if (OB_FAIL(get_flat_cache_column_count(column_count))) {
    LOG_WARN("failed to get FlatCache column count", K(ret), K(block_count));
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
      } else if (OB_UNLIKELY(block_header->row_count_ > DEFAULT_POSTING_BLOCK_CAPACITY)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encoded posting row count exceeds capacity", K(ret), K(i),
                 K(block_header->row_count_), "capacity", DEFAULT_POSTING_BLOCK_CAPACITY);
      } else {
        const int64_t current_column_count = block_header->get_column_count();
        if (OB_UNLIKELY(column_count != current_column_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inconsistent FlatCache source column count", K(ret), K(i),
                   K(column_count), K(current_column_count));
        } else if (OB_UNLIKELY(total_doc_count
                   > static_cast<int64_t>(header_.row_count_) - block_header->row_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("FlatCache source exceeds outer row count", K(ret), K(i),
                   K(total_doc_count), K(block_header->row_count_), K(header_.row_count_));
        } else {
          total_doc_count += block_header->row_count_;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(total_doc_count != header_.row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FlatCache source row count mismatch", K(ret), K(total_doc_count),
             K(header_.row_count_), K(block_count));
  } else if (OB_SUCC(ret)) {
    const uint64_t total_size = sizeof(ObFlatCacheHeader)
        + static_cast<uint64_t>(block_count) * sizeof(ObFlatCacheBlockDesc)
        + static_cast<uint64_t>(header_.row_count_) * sizeof(uint64_t) * column_count;
    if (OB_UNLIKELY(total_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("FlatCache payload exceeds micro header capacity", K(ret),
               K(total_size), K(block_count), K(header_.row_count_));
    } else {
      payload_size = static_cast<int64_t>(total_size);
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
  } else if (OB_FAIL(write_payload(block_buf + pos, buf_size - pos, payload_size))) {
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

int ObInvIdxMicroBlockTransformer::get_flat_cache_column_count(
    int64_t &column_count) const
{
  int ret = OB_SUCCESS;
  column_count = 0;
  const char *first_block = nullptr;
  int64_t first_block_size = 0;
  if (OB_FAIL(reader_.get_payload(0, first_block, first_block_size))) {
    LOG_WARN("failed to get first encoded posting block", K(ret));
  } else if (OB_UNLIKELY(first_block_size < static_cast<int64_t>(sizeof(ObPostingBlockHeader)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first encoded posting block is truncated", K(ret), K(first_block_size));
  } else {
    const ObPostingBlockHeader *block_header =
        reinterpret_cast<const ObPostingBlockHeader *>(first_block);
    if (OB_UNLIKELY(!block_header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid first encoded posting block header", K(ret), KPC(block_header));
    } else {
      column_count = block_header->get_column_count();
    }
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::write_flat_cache_block(
    const int64_t block_idx,
    ObFlatCacheBlockDesc &desc,
    uint64_t *doc_ids,
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
    if (OB_UNLIKELY(!block_header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid FlatCache source block", K(ret), K(block_idx), KPC(block_header));
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
      uint64_t *token_freqs = block_header->has_relevance_params_
                              ? doc_ids + header_.row_count_
                              : nullptr;
      uint64_t *doc_lengths = OB_NOT_NULL(token_freqs)
                              ? token_freqs + header_.row_count_
                              : nullptr;
      for (int64_t r = 0; OB_SUCC(ret) && r < block_header->row_count_; ++r) {
        const int64_t output_idx = static_cast<int64_t>(desc.doc_offset_) + r;
        if (OB_FAIL(decoder.get_doc_id(r, doc_ids[output_idx]))) {
          LOG_WARN("failed to decode FlatCache doc id", K(ret), K(block_idx), K(r));
        } else if (block_header->has_relevance_params_
                   && OB_FAIL(decoder.get_relevance_params(
                       r, token_freqs[output_idx], doc_lengths[output_idx]))) {
          LOG_WARN("failed to decode FlatCache relevance params", K(ret), K(block_idx), K(r));
        }
      }
    }
  }
  return ret;
}

int ObInvIdxMicroBlockTransformer::write_payload(
    char *dst,
    const int64_t dst_size,
    int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  payload_size = 0;
  const int64_t block_count = reader_.get_block_count();
  const uint64_t doc_ids_offset = sizeof(ObFlatCacheHeader) + static_cast<uint64_t>(block_count) * sizeof(ObFlatCacheBlockDesc);
  int64_t column_count = 0;
  uint64_t total_size = 0;
  if (OB_ISNULL(dst) || OB_UNLIKELY(block_count <= 0
      || block_count > ObFlatCacheHeader::MAX_BLOCK_COUNT
      || 0 == header_.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FlatCache output", K(ret), KP(dst), K(dst_size),
             K(block_count), K(header_.row_count_));
  } else if (OB_FAIL(get_flat_cache_column_count(column_count))) {
    LOG_WARN("failed to get FlatCache column count", K(ret), K(block_count));
  } else if (FALSE_IT(total_size = doc_ids_offset
           + static_cast<uint64_t>(header_.row_count_) * sizeof(uint64_t) * column_count)) {
  } else if (OB_UNLIKELY(total_size > INT32_MAX || total_size != static_cast<uint64_t>(dst_size))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("unexpected FlatCache output size", K(ret), K(dst_size), K(total_size),
             K(block_count), K(header_.row_count_));
  } else if (OB_UNLIKELY(0 != reinterpret_cast<uintptr_t>(dst) % sizeof(uint64_t))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("misaligned FlatCache output", K(ret), KP(dst));
  } else {
    MEMSET(dst, 0, doc_ids_offset);
    ObFlatCacheHeader *flat_cache_header = new (dst) ObFlatCacheHeader();
    flat_cache_header->column_count_ = static_cast<uint8_t>(column_count);
    ObFlatCacheBlockDesc *descs = reinterpret_cast<ObFlatCacheBlockDesc *>(dst + sizeof(ObFlatCacheHeader));
    uint64_t *doc_ids = reinterpret_cast<uint64_t *>(dst + doc_ids_offset);
    int64_t doc_offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      ObFlatCacheBlockDesc *desc = new (&descs[i]) ObFlatCacheBlockDesc();
      if (OB_FAIL(write_flat_cache_block(i, *desc, doc_ids, doc_offset))) {
        LOG_WARN("failed to write FlatCache block", K(ret), K(i), K(block_count));
      } else if (i + 1 == block_count
                 && OB_UNLIKELY(doc_offset != header_.row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("FlatCache output doc count mismatch", K(ret), K(doc_offset), K(header_.row_count_));
      } else if (i + 1 == block_count) {
        flat_cache_header->block_count_ = static_cast<uint16_t>(block_count);
        payload_size = static_cast<int64_t>(total_size);
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
