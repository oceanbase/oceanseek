/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_micro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{
ObInvIdxMicroBlockReader::ObInvIdxMicroBlockReader()
  : payload_(nullptr),
    payload_size_(0),
    header_(nullptr),
    offsets_(nullptr),
    flat_cache_reader_(),
    is_inited_(false)
{
}

ObInvIdxMicroBlockReader::~ObInvIdxMicroBlockReader()
{
}

int ObInvIdxMicroBlockReader::init_flat_cache(
    const char *micro_block_buf,
    const int64_t micro_block_size)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_FAIL(flat_cache_reader_.init(micro_block_buf, micro_block_size))) {
    LOG_WARN("failed to init FlatCache value", K(ret), K(micro_block_size));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInvIdxMicroBlockReader::init(
    const char *micro_block_buf,
    const int64_t micro_block_size,
    const bool is_posting_value)
{
  int ret = OB_SUCCESS;
  reuse();
  ObMicroBlockHeader header;
  int64_t pos = 0;
  if (OB_ISNULL(micro_block_buf) || OB_UNLIKELY(micro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(micro_block_buf), K(micro_block_size), K(is_posting_value));
  } else if (OB_FAIL(header.deserialize(micro_block_buf, micro_block_size, pos))) {
    LOG_WARN("failed to deserialize inv idx micro block header", K(ret), K(is_posting_value),
        KP(micro_block_buf), K(micro_block_size));
  } else if (OB_UNLIKELY(common::INV_IDX_ROW_STORE != header.get_row_store_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected micro block row store type", K(ret), K(header));
  } else if (OB_UNLIKELY(static_cast<ObMicroBlockInvertedIndexType>(header.inverted_index_type_)
      != (is_posting_value ? MICRO_BLOCK_INV_IDX_POSTING_VALUE : MICRO_BLOCK_INV_IDX_POSTING))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv idx micro block type mismatch", K(ret), K(is_posting_value), K(header));
  } else if (OB_UNLIKELY(header.data_length_ != header.data_zlength_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv idx micro block payload should be decompressed before reader init", K(ret),
        K(is_posting_value), K(header));
  } else if (OB_UNLIKELY(
      static_cast<int64_t>(header.header_size_) + static_cast<int64_t>(header.data_zlength_)
      != micro_block_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv idx micro block payload out of range", K(ret), K(is_posting_value), K(header),
        K(micro_block_size));
  } else {
    const char *payload = micro_block_buf + header.header_size_;
    const int64_t payload_size = header.data_length_;
    if (OB_FAIL(init_payload(payload, payload_size))) {
      LOG_WARN("failed to init inv idx data micro block payload", K(ret), K(is_posting_value),
          K(payload_size), K(header));
    }
  }
  return ret;
}

int ObInvIdxMicroBlockReader::init_payload(const char *payload, const int64_t payload_size)
{
  int ret = OB_SUCCESS;
  reuse();
  const int64_t payload_header_size = static_cast<int64_t>(sizeof(ObInvIdxMicroBlockPayloadHeader));
  if (OB_ISNULL(payload) || OB_UNLIKELY(payload_size < payload_header_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload), K(payload_size));
  } else {
    const ObInvIdxMicroBlockPayloadHeader *header =
        reinterpret_cast<const ObInvIdxMicroBlockPayloadHeader *>(payload);
    if (OB_UNLIKELY(!header->is_valid() || payload_size < header->total_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data micro block payload header", K(ret), K(payload_size), KPC(header));
    } else {
      const int64_t block_offsets_end =
          static_cast<int64_t>(header->block_offsets_offset_)
          + static_cast<int64_t>(header->block_count_ + 1) * static_cast<int64_t>(sizeof(uint32_t));
      if (OB_UNLIKELY(header->block_offsets_offset_ < payload_header_size
          || header->blocks_offset_ < payload_header_size
          || header->block_offsets_offset_ > header->total_size_
          || header->blocks_offset_ > header->total_size_
          || block_offsets_end > header->total_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid data micro block payload layout", K(ret), KPC(header), K(block_offsets_end));
      } else {
        payload_ = payload;
        payload_size_ = payload_size;
        header_ = header;
        offsets_ = reinterpret_cast<const uint32_t *>(payload + header->block_offsets_offset_);
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObInvIdxMicroBlockReader::get_payload(
    const int64_t block_idx,
    const char *&block_buf,
    int64_t &block_size) const
{
  int ret = OB_SUCCESS;
  block_buf = nullptr;
  block_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inv idx data micro block reader not inited", K(ret));
  } else if (OB_ISNULL(header_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("FlatCache layout has no per-block payload", K(ret), K(block_idx));
  } else if (OB_UNLIKELY(block_idx < 0 || block_idx >= header_->block_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block idx out of range", K(ret), K(block_idx), K(header_->block_count_));
  } else {
    const uint32_t start = offsets_[block_idx];
    const uint32_t end = offsets_[block_idx + 1];
    const int64_t block_start = static_cast<int64_t>(header_->blocks_offset_) + start;
    const int64_t block_end = static_cast<int64_t>(header_->blocks_offset_) + end;
    if (OB_UNLIKELY(end <= start
        || block_start < static_cast<int64_t>(header_->blocks_offset_)
        || block_end > static_cast<int64_t>(header_->total_size_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid block offset range", K(ret), K(block_idx), K(start), K(end),
          K(header_->blocks_offset_), K(header_->total_size_));
    } else {
      block_buf = payload_ + block_start;
      block_size = static_cast<int64_t>(end - start);
    }
  }

  return ret;
}

int ObInvIdxMicroBlockReader::get_posting_block_data(
    const int64_t block_idx,
    ObPostingBlockData &block_data) const
{
  int ret = OB_SUCCESS;
  block_data.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inv idx data micro block reader not inited", K(ret));
  } else if (OB_NOT_NULL(header_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("encoded posting block is not a cache value", K(ret), K(block_idx));
  } else if (OB_FAIL(flat_cache_reader_.get_posting_block_data(block_idx, block_data))) {
    LOG_WARN("failed to get FlatCache block", K(ret), K(block_idx));
  }
  return ret;
}

int ObInvIdxMicroBlockReader::get_doc_id_view(
    const int64_t first_block_idx,
    const int64_t last_block_idx,
    const uint64_t *&doc_ids,
    int64_t &doc_count) const
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  doc_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inv idx data micro block reader not inited", K(ret));
  } else if (OB_NOT_NULL(header_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("encoded posting block has no contiguous doc-id view", K(ret),
             K(first_block_idx), K(last_block_idx));
  } else if (OB_FAIL(flat_cache_reader_.get_doc_id_view(
             first_block_idx, last_block_idx, doc_ids, doc_count))) {
    LOG_WARN("failed to get FlatCache doc-id view", K(ret),
             K(first_block_idx), K(last_block_idx));
  }
  return ret;
}

int64_t ObInvIdxMicroBlockReader::get_block_count() const
{
  return !is_inited_
         ? 0
         : OB_NOT_NULL(header_)
           ? header_->block_count_
           : flat_cache_reader_.get_block_count();
}

void ObInvIdxMicroBlockReader::reuse()
{
  payload_ = nullptr;
  payload_size_ = 0;
  header_ = nullptr;
  offsets_ = nullptr;
  flat_cache_reader_.reset();
  is_inited_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase
