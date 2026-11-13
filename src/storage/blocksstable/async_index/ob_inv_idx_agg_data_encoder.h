/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_ENCODER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_ENCODER_H_

#include "ob_inv_idx_bm25_max_score_aggregator.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{

// Agg data byte layout for one (micro block M, term T) index row.
//
// The agg data is carried through the standard index row builder as a raw serialized payload.
// ObInvIdxAggDataHeader::total_size_ records the whole leaf payload size for its decoder.
// The standard index row parser uses the enclosing row boundary to carry opaque payload bytes.
//
//   agg data
//   - payload header (16B)
//   - logical block entries [logical_block_count]       // 34B each
//   - frontier pairs [sum of frontier_count]            // 8B each
//
// All multi-byte fields are little-endian with no padding. M-level min/max doc id are not stored;
// they are the min_doc_id of the first entry and the max_doc_id of the last entry.
// M's posting micro block offset/size are not stored here either: they go into the index row
// header (block_offset_ / block_size_), not the agg data.

// On-disk payload header, reinterpret_cast onto the agg data buffer.
// This header belongs to leaf (micro block, term) rows only. Inner-node BM25
// summaries share ObInvIdxAggDataDesc, but use ObInvIdxAggInfoSerializer's own
// version/mask/value encoding and do not reserve bytes for this header.
struct ObInvIdxAggDataHeader
{
  static const uint8_t VERSION = 1;
  uint8_t version_;
  uint8_t flags_;
  uint16_t logical_block_count_;
  uint32_t posting_value_offset_;
  uint32_t posting_value_size_;
  uint32_t total_size_;
} __attribute__((packed));

// On-disk per-logical-block entry, reinterpret_cast onto the agg data buffer. frontier_offset_ is
// the byte offset of this block's frontier pairs within the frontier pool.
struct ObInvIdxAggDataEntry
{
  uint16_t block_idx_;        // logical block index inside the micro block
  uint16_t frontier_count_;
  uint16_t frontier_offset_;  // byte offset of this block's frontier pairs in the frontier pool
  uint32_t doc_count_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  uint64_t total_doc_length_; // sum(token_freq) in this logical block
} __attribute__((packed));

struct ObInvIdxLogicalBlockMeta
{
  static constexpr int64_t MAX_FRONTIER_PAIR_COUNT = 12;

  ObInvIdxLogicalBlockMeta()
    : block_idx_(0),
      doc_count_(0),
      min_doc_id_(0),
      max_doc_id_(0),
      total_doc_length_(0),
      frontier_pairs_()
  {}
  bool is_valid() const
  {
    return block_idx_ >= 0 && doc_count_ > 0 && min_doc_id_ <= max_doc_id_;
  }
  TO_STRING_KV(K_(block_idx), K_(doc_count), K_(min_doc_id), K_(max_doc_id),
      K_(total_doc_length), K_(frontier_pairs));

  int64_t block_idx_;
  int64_t doc_count_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  uint64_t total_doc_length_;
  common::ObSEArray<ObInvIdxBM25ParamPair, MAX_FRONTIER_PAIR_COUNT> frontier_pairs_;
};

// Encoder of one (M, T) index-row agg data.
//
// Errors:
//   - OB_INVALID_ARGUMENT: empty logical_block_metas; non-positive posting value offset/size; or an
//     individual meta fails is_valid().
//   - OB_SIZE_OVERFLOW: block_idx exceeds uint16; doc_count / posting value offset/size exceeds
//     uint32; or the total agg data length exceeds the uint32 payload size limit.
class ObInvIdxAggDataEncoder final
{
public:
  static int estimate_size(
      const int64_t posting_value_offset,
      const int64_t posting_value_size,
      const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
      int64_t &size);
  static int build(common::ObIAllocator &allocator,
                   const int64_t posting_value_offset,
                   const int64_t posting_value_size,
                   const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
                   char *&buf,
                   int64_t &size);
private:
  static constexpr int64_t MAX_AGG_DATA_SIZE = INT16_MAX;
  static constexpr int64_t MAX_FRONTIER_POOL_SIZE = UINT16_MAX;
  static constexpr int64_t HEADER_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));
  static constexpr int64_t ENTRY_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));
  static constexpr int64_t PAIR_SIZE = static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));

  static int check_meta(const ObInvIdxLogicalBlockMeta &meta, int64_t &frontier_count);
  static int write_payload(
      const int64_t posting_value_offset,
      const int64_t posting_value_size,
      const common::ObIArray<ObInvIdxLogicalBlockMeta> &logical_block_metas,
      char *buf,
      const int64_t buf_size,
      int64_t &pos);
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxAggDataEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_ENCODER_H_
