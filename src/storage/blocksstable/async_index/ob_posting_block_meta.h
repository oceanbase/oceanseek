/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_META_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_META_H_

#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/async_index/ob_inv_idx_bm25_max_score_aggregator.h"

namespace oceanbase
{
namespace blocksstable
{

// Locator of one posting block (and its aligned value block) inside the
// (data micro block M, term T) leaf index row.
//   - block_idx_ indexes the posting block within M's posting data micro block,
//     as well as the aligned value block within M's posting value micro block.
//   - value_micro_offset_ / value_micro_size_ address the posting value micro
//     block within the macro block (carried in the leaf row agg data header).
// The posting data micro block itself is addressed by the leaf index row header
// and is delivered separately via ObSSTablePostingScanner::get_cur_micro_block().
struct ObPostingBlockLocator
{
  ObPostingBlockLocator()
    : block_idx_(-1), value_micro_offset_(0), value_micro_size_(0) {}
  OB_INLINE void reset()
  {
    block_idx_ = -1;
    value_micro_offset_ = 0;
    value_micro_size_ = 0;
  }
  OB_INLINE bool is_valid() const { return block_idx_ >= 0; }
  TO_STRING_KV(K_(block_idx), K_(value_micro_offset), K_(value_micro_size));

  int64_t block_idx_;
  uint32_t value_micro_offset_;
  uint32_t value_micro_size_;
};

// BM25 max-score frontier of one posting block. The frontier keeps the Pareto
// set of (token_freq, doc_length) pairs; the actual block max score is computed
// at query time once the average doc length is known. Pairs point into the leaf
// row agg data buffer and stay valid as long as that buffer is held.
struct ObPostingBlockMaxScore
{
  ObPostingBlockMaxScore() : pairs_(nullptr), pair_count_(0) {}
  OB_INLINE void reset()
  {
    pairs_ = nullptr;
    pair_count_ = 0;
  }
  OB_INLINE bool has_max_score() const { return nullptr != pairs_ && pair_count_ > 0; }
  TO_STRING_KV(KP_(pairs), K_(pair_count));

  const ObInvIdxBM25ParamPair *pairs_;
  int64_t pair_count_;
};

// Block-level meta of one posting block, the iteration unit exposed by
// ObPostingMetaRowScanner / ObSSTablePostingScanner.
struct ObPostingBlockMeta
{
  ObPostingBlockMeta() : min_id_(0), max_id_(0), doc_count_(0), idx_(-1), locator_(), block_max_() {}
  OB_INLINE void reset()
  {
    min_id_ = 0;
    max_id_ = 0;
    doc_count_ = 0;
    idx_ = -1;
    locator_.reset();
    block_max_.reset();
  }
  OB_INLINE bool is_valid() const
  {
    return idx_ >= 0 && doc_count_ > 0 && min_id_ <= max_id_ && locator_.is_valid();
  }
  TO_STRING_KV(K_(min_id), K_(max_id), K_(doc_count), K_(idx), K_(locator), K_(block_max));

  uint64_t min_id_;             // min doc_id in this posting block
  uint64_t max_id_;             // max doc_id in this posting block
  int64_t doc_count_;           // doc count in this posting block
  int64_t idx_;                 // sequential index within the current leaf index row
  ObPostingBlockLocator locator_;
  ObPostingBlockMaxScore block_max_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_META_H_
