/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_micro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvIdxMicroBlockWriter::ObInvIdxMicroBlockWriter()
  : data_buf_("InvIdxMicroBlockWriter", 0),
    block_offsets_(),
    cur_blocks_size_(0),
    is_built_(false),
    is_inited_(false)
{
}

ObInvIdxMicroBlockWriter::~ObInvIdxMicroBlockWriter()
{
  reset();
}

int ObInvIdxMicroBlockWriter::init(common::ObIAllocator &allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(reserve_header())) {
    LOG_WARN("failed to reserve micro block header", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInvIdxMicroBlockWriter::estimate_size_after_append(
    const int64_t logical_block_size,
    int64_t &micro_block_size)
{
  int ret = OB_SUCCESS;
  micro_block_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(logical_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical block size", K(ret), K(logical_block_size));
  } else if (OB_UNLIKELY(is_built_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot append after micro block has been built", K(ret), KPC(this));
  } else if (OB_UNLIKELY(block_offsets_.count()
      >= ObInvIdxMicroBlockPayloadHeader::MAX_BLOCK_COUNT)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("too many logical blocks in micro block", K(ret),
        "block_count", block_offsets_.count());
  } else {
    const int64_t header_size = sizeof(ObInvIdxMicroBlockPayloadHeader);
    const int64_t offset_table_size
        = (block_offsets_.count() + 1) * ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE;
    const int64_t cur_size = header_size + cur_blocks_size_ + offset_table_size;
    const int64_t remaining_size = OB_DEFAULT_MACRO_BLOCK_SIZE - cur_size;
    if (OB_UNLIKELY(ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE > remaining_size
        || logical_block_size
          > remaining_size - ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_DEBUG("estimated micro block size exceeds limit", K(ret),
          K_(cur_blocks_size), K(offset_table_size), K(cur_size), K(logical_block_size));
    } else {
      micro_block_size
          = cur_size + logical_block_size + ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE;
    }
  }
  return ret;
}

int ObInvIdxMicroBlockWriter::append_logical_block(
    const char *buf,
    const int64_t size,
    int64_t &block_idx)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 0;
  block_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical block", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(estimate_size_after_append(size, micro_block_size))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("failed to estimate micro block size", K(ret), K(size), K(micro_block_size));
    }
  } else if (OB_FAIL(block_offsets_.push_back(static_cast<uint32_t>(cur_blocks_size_)))) {
    LOG_WARN("failed to push logical block offset", K(ret), K_(cur_blocks_size));
  } else if (OB_FAIL(data_buf_.write(buf, size))) {
    block_offsets_.pop_back();
    LOG_WARN("failed to write logical block data", K(ret), KP(buf), K(size));
  } else {
    block_idx = block_offsets_.count() - 1;
    cur_blocks_size_ += size;
  }
  return ret;
}

int ObInvIdxMicroBlockWriter::build_micro_block(const char *&buf, int64_t &size)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = sizeof(ObInvIdxMicroBlockPayloadHeader);
  const int64_t block_count = block_offsets_.count();
  const int64_t block_offsets_offset = header_size + cur_blocks_size_;
  const int64_t total_size =
      block_offsets_offset + (block_count + 1) * ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE;
  buf = nullptr;
  size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(block_count <= 0 || cur_blocks_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty micro block", K(ret), K(block_count), K_(cur_blocks_size));
  } else if (is_built_) {
    buf = data_buf_.data();
    size = data_buf_.length();
  } else if (OB_UNLIKELY(data_buf_.length() != block_offsets_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data buffer length", K(ret),
        K(data_buf_.length()), K(block_offsets_offset));
  } else if (OB_UNLIKELY(total_size > OB_DEFAULT_MACRO_BLOCK_SIZE)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("micro block size exceed limit",
        K(ret), K(total_size), K(OB_DEFAULT_MACRO_BLOCK_SIZE));
  } else if (OB_FAIL(data_buf_.ensure_space(total_size))) {
    LOG_WARN("failed to ensure micro block buffer space", K(ret), K(total_size));
  } else {
    ObInvIdxMicroBlockPayloadHeader *header =
        reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(data_buf_.data());
    header->version_ = ObInvIdxMicroBlockPayloadHeader::VERSION;
    header->reserved_ = 0;
    header->block_count_ = static_cast<uint16_t>(block_count);
    header->blocks_offset_ = static_cast<uint32_t>(header_size);
    header->block_offsets_offset_ = static_cast<uint32_t>(block_offsets_offset);
    header->total_size_ = static_cast<uint32_t>(total_size);
    if (OB_UNLIKELY(!header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid micro block header", K(ret), KPC(header));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && !is_built_ && i < block_count; ++i) {
    const uint32_t block_offset = block_offsets_.at(i);
    if (OB_FAIL(data_buf_.write(block_offset))) {
      LOG_WARN("failed to write logical block offset", K(ret), K(i), K(block_offset));
    }
  }
  if (OB_FAIL(ret) || is_built_) {
  } else {
    const uint32_t block_offset = static_cast<uint32_t>(cur_blocks_size_);
    if (OB_FAIL(data_buf_.write(block_offset))) {
      LOG_WARN("failed to write logical block offset sentinel", K(ret), K(block_offset));
    } else if (OB_UNLIKELY(data_buf_.length() != total_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected built micro block size", K(ret), K(data_buf_.length()), K(total_size));
    } else {
      is_built_ = true;
      buf = data_buf_.data();
      size = data_buf_.length();
    }
  }
  return ret;
}

int ObInvIdxMicroBlockWriter::reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(reserve_header())) {
    LOG_WARN("failed to reserve micro block header", K(ret));
  } else {
    block_offsets_.reuse();
    cur_blocks_size_ = 0;
    is_built_ = false;
  }
  return ret;
}

void ObInvIdxMicroBlockWriter::reset()
{
  data_buf_.reset();
  block_offsets_.reset();
  cur_blocks_size_ = 0;
  is_built_ = false;
  is_inited_ = false;
}

int ObInvIdxMicroBlockWriter::reserve_header()
{
  int ret = OB_SUCCESS;
  ObInvIdxMicroBlockPayloadHeader header;
  data_buf_.reuse();
  if (OB_FAIL(data_buf_.write(reinterpret_cast<const char *>(&header), sizeof(header)))) {
    LOG_WARN("failed to write empty micro block header", K(ret), K(header));
  } else if (OB_UNLIKELY(data_buf_.length() != static_cast<int64_t>(sizeof(header)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data buffer length after reserving header",
        K(ret), K(data_buf_.length()), "header_size", sizeof(header));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
