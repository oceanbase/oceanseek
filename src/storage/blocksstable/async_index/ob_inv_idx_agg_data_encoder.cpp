/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_agg_data_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

int ObInvIdxAggDataEncoder::check_meta(
    const ObInvIdxLogicalBlockMeta &meta,
    int64_t &frontier_count)
{
  int ret = OB_SUCCESS;
  frontier_count = 0;
  if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical block meta", K(ret), K(meta));
  } else if (OB_UNLIKELY(meta.block_idx_ > UINT16_MAX || meta.doc_count_ > UINT32_MAX
      || meta.frontier_pairs_.count() > UINT16_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("block idx or doc count overflows", K(ret), K(meta));
  } else {
    frontier_count = meta.frontier_pairs_.count();
  }
  return ret;
}

int ObInvIdxAggDataEncoder::estimate_size(
    const int64_t posting_value_offset,
    const int64_t posting_value_size,
    const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  const int64_t count = logical_block_metas.count();
  int64_t total = HEADER_SIZE;
  int64_t frontier_pool_size = 0;
  size = 0;
  if (OB_UNLIKELY(count <= 0 || posting_value_offset < 0 || posting_value_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count),
        K(posting_value_offset), K(posting_value_size));
  } else if (OB_UNLIKELY(count > UINT16_MAX
      || posting_value_offset > UINT32_MAX || posting_value_size > UINT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflows", K(ret), K(count),
        K(posting_value_offset), K(posting_value_size));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    int64_t frontier_count = 0;
    if (OB_FAIL(check_meta(logical_block_metas.at(i), frontier_count))) {
      LOG_WARN("failed to check logical block meta", K(ret), K(i));
    } else if (OB_UNLIKELY(total > MAX_AGG_DATA_SIZE - ENTRY_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("agg data size overflows", K(ret), K(i), K(total), K(ENTRY_SIZE));
    } else if (OB_UNLIKELY(frontier_count > (MAX_FRONTIER_POOL_SIZE - frontier_pool_size) / PAIR_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("frontier pool offset overflows", K(ret), K(i), K(frontier_count), K(frontier_pool_size));
    } else {
      total += ENTRY_SIZE;
      frontier_pool_size += frontier_count * PAIR_SIZE;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(total > MAX_AGG_DATA_SIZE - frontier_pool_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("total size overflows", K(ret), K(total), K(frontier_pool_size));
  } else {
    total += frontier_pool_size;
    size = total;
  }
  return ret;
}

int ObInvIdxAggDataEncoder::write_payload(
    const int64_t posting_value_offset,
    const int64_t posting_value_size,
    const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
    char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t count = logical_block_metas.count();
  int64_t frontier_pool_offset = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(buf_size - pos < HEADER_SIZE)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough for agg data header", K(ret), K(buf_size), K(pos));
  } else {
    ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(buf + pos);
    header->version_ = ObInvIdxAggDataHeader::VERSION;
    header->flags_ = 0;
    header->logical_block_count_ = static_cast<uint16_t>(count);
    header->posting_value_offset_ = static_cast<uint32_t>(posting_value_offset);
    header->posting_value_size_ = static_cast<uint32_t>(posting_value_size);
    header->total_size_ = static_cast<uint32_t>(buf_size);
    pos += HEADER_SIZE;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObInvIdxLogicalBlockMeta &meta = logical_block_metas.at(i);
    const int64_t frontier_count = meta.frontier_pairs_.count();
    const int64_t frontier_size = frontier_count * PAIR_SIZE;
    if (OB_UNLIKELY(buf_size - pos < ENTRY_SIZE)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer not enough for agg data entry", K(ret), K(i), K(buf_size), K(pos));
    } else {
      ObInvIdxAggDataEntry *desc = reinterpret_cast<ObInvIdxAggDataEntry *>(buf + pos);
      desc->block_idx_ = static_cast<uint16_t>(meta.block_idx_);
      desc->frontier_count_ = static_cast<uint16_t>(frontier_count);
      desc->frontier_offset_ = static_cast<uint16_t>(frontier_pool_offset);
      desc->doc_count_ = static_cast<uint32_t>(meta.doc_count_);
      desc->min_doc_id_ = meta.min_doc_id_;
      desc->max_doc_id_ = meta.max_doc_id_;
      desc->total_doc_length_ = meta.total_doc_length_;
      pos += ENTRY_SIZE;
      frontier_pool_offset += frontier_size;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObInvIdxLogicalBlockMeta &meta = logical_block_metas.at(i);
    const int64_t frontier_count = meta.frontier_pairs_.count();
    for (int64_t j = 0; OB_SUCC(ret) && j < frontier_count; ++j) {
      if (OB_UNLIKELY(buf_size - pos < PAIR_SIZE)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buffer not enough for frontier pair", K(ret), K(i), K(j), K(buf_size), K(pos));
      } else {
        ObInvIdxBM25ParamPair *pair = reinterpret_cast<ObInvIdxBM25ParamPair *>(buf + pos);
        pair->token_freq_ = meta.frontier_pairs_.at(j).token_freq_;
        pair->doc_length_ = meta.frontier_pairs_.at(j).doc_length_;
        pos += PAIR_SIZE;
      }
    }
  }
  return ret;
}

int ObInvIdxAggDataEncoder::build(
    common::ObIAllocator &allocator,
    const int64_t posting_value_offset,
    const int64_t posting_value_size,
    const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
    char *&buf,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  buf = nullptr;
  size = 0;
  if (OB_FAIL(estimate_size(
      posting_value_offset, posting_value_size, logical_block_metas, size))) {
    LOG_WARN("failed to estimate agg data size", K(ret),
        K(posting_value_offset), K(posting_value_size), "meta_count", logical_block_metas.count());
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc agg data buffer", K(ret), K(size));
  } else if (OB_FAIL(write_payload(
      posting_value_offset, posting_value_size, logical_block_metas, buf, size, pos))) {
    LOG_WARN("failed to write agg data payload", K(ret));
  } else if (OB_UNLIKELY(pos != size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected agg data written size", K(ret), K(pos), K(size));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
