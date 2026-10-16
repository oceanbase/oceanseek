/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObInvIdxAggDataDecoder::decode(
    const char *buf,
    const int64_t size,
    ObIArray<ObInvIdxLogicalBlockMeta> &metas,
    int64_t &posting_value_offset,
    int64_t &posting_value_size)
{
  int ret = OB_SUCCESS;
  metas.reset();
  posting_value_offset = 0;
  posting_value_size = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size < HEADER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid agg data buffer", K(ret), KP(buf), K(size), K(HEADER_SIZE));
  } else {
    const ObInvIdxAggDataHeader *header = reinterpret_cast<const ObInvIdxAggDataHeader *>(buf);
    const int64_t block_count = header->logical_block_count_;
    const int64_t total_size = header->total_size_;
    if (OB_UNLIKELY(ObInvIdxAggDataHeader::VERSION != header->version_)) {
      ret = OB_VERSION_NOT_MATCH;
      STORAGE_LOG(WARN, "unexpected agg data version", K(ret), "version", header->version_);
    } else if (OB_UNLIKELY(block_count <= 0
               // 先边界检查 block_count，确保 HEADER_SIZE + block_count * ENTRY_SIZE 不会 int64 溢出
               || block_count > (size - HEADER_SIZE) / ENTRY_SIZE
               || total_size > size)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "malformed agg data header", K(ret), K(size), K(block_count),
          K(total_size));
    } else if (OB_UNLIKELY(HEADER_SIZE + block_count * ENTRY_SIZE > total_size)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "agg data entries region exceeds total size", K(ret), K(size),
          K(block_count), K(total_size));
    } else if (OB_UNLIKELY((0 == header->posting_value_size_ && 0 != header->posting_value_offset_)
               || (header->posting_value_size_ > 0 && 0 == header->posting_value_offset_))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "malformed posting value locator", K(ret),
          "posting_value_offset", header->posting_value_offset_,
          "posting_value_size", header->posting_value_size_);
    } else if (OB_FAIL(metas.reserve(block_count))) {
      STORAGE_LOG(WARN, "fail to reserve logical block metas", K(ret), K(block_count));
    } else {
      posting_value_offset = header->posting_value_offset_;
      posting_value_size = header->posting_value_size_;
      const ObInvIdxAggDataEntry *entries =
          reinterpret_cast<const ObInvIdxAggDataEntry *>(buf + HEADER_SIZE);
      const char *frontier_pool = buf + HEADER_SIZE + block_count * ENTRY_SIZE;
      for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
        const ObInvIdxAggDataEntry &entry = entries[i];
        ObInvIdxLogicalBlockMeta meta;
        meta.block_idx_ = entry.block_idx_;
        meta.doc_count_ = entry.doc_count_;
        meta.min_doc_id_ = entry.min_doc_id_;
        meta.max_doc_id_ = entry.max_doc_id_;
        meta.total_doc_length_ = entry.total_doc_length_;
        // frontier pool 位于 entries 区之后，entry.frontier_offset_ 是池内字节偏移。
        // 逐对物化：compaction 逻辑块复用路径需要把源端 frontier 携带进输出 meta，
        // 查询侧 block-max 剪枝也依赖它，不能停在"seek 不需要"的最小解码。
        const int64_t frontier_count = entry.frontier_count_;
        const int64_t frontier_end = HEADER_SIZE + block_count * ENTRY_SIZE
            + entry.frontier_offset_ + frontier_count * PAIR_SIZE;
        if (OB_UNLIKELY(meta.doc_count_ <= 0 || meta.min_doc_id_ > meta.max_doc_id_)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid logical block meta entry", K(ret), K(i), K(meta));
        } else if (OB_UNLIKELY(frontier_count > ObInvIdxLogicalBlockMeta::MAX_FRONTIER_PAIR_COUNT
                               || frontier_end > total_size)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "malformed frontier locator", K(ret), K(i),
              "frontier_count", frontier_count, "frontier_offset", entry.frontier_offset_,
              K(total_size));
        } else {
          const ObInvIdxBM25ParamPair *pairs =
              reinterpret_cast<const ObInvIdxBM25ParamPair *>(frontier_pool + entry.frontier_offset_);
          for (int64_t j = 0; OB_SUCC(ret) && j < frontier_count; ++j) {
            if (OB_FAIL(meta.frontier_pairs_.push_back(pairs[j]))) {
              STORAGE_LOG(WARN, "fail to push back frontier pair", K(ret), K(i), K(j));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(metas.push_back(meta))) {
            STORAGE_LOG(WARN, "fail to push back logical block meta", K(ret), K(i));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    metas.reset();
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
