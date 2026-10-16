/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_META_ROW_SCANNER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_META_ROW_SCANNER_H_

#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_meta.h"

namespace oceanbase
{
namespace blocksstable
{

// Parses the agg data of one (data micro block M, term T) leaf index row and
// iterates its posting blocks at block-meta granularity. The agg data buffer is
// produced by ObInvIdxAggDataEncoder and carried on the index row; it is borrowed
// (not owned) and must stay valid for the lifetime of the open scanner.
//
// The posting data micro block address (macro id / offset / size) is NOT part of
// the agg data; it lives in the index row header and is held by the upper layer.
// The posting value micro block address IS carried in the agg data header and is
// exposed through the per-block ObPostingBlockMeta::locator_.
class ObPostingMetaRowScanner final
{
public:
  ObPostingMetaRowScanner();
  ~ObPostingMetaRowScanner() = default;

  int init(const bool has_max_score);
  // Parse one leaf row agg data buffer and position at its first posting block.
  int open(const char *agg_buf, const int64_t agg_size);

  // Fill meta for the posting block at the current cursor.
  int get_cur_block_meta(ObPostingBlockMeta &meta) const;
  // Return the current block index; -1 if the scanner has not yet positioned to a valid block.
  int64_t get_cur_block_idx() const
  {
    return is_opened_ && cur_idx_ >= 0 && cur_idx_ < header_->logical_block_count_
        ? cur_idx_ : -1;
  }
  // Read block metadata by index without changing the forward cursor.
  int get_block_meta_at(const int64_t block_idx, ObPostingBlockMeta &meta) const;
  // Return the first and last physical block_idx from the already parsed agg row.
  int get_block_idx_range(int64_t &first_block_idx, int64_t &last_block_idx) const;
  // Advance the cursor to the next posting block; OB_ITER_END when exhausted.
  int next_block();
  // Forward-only advance based on current scan state. Search in
  // [cur_idx_, block_count) for the first block whose max_doc_id >= target and move
  // the cursor there. If not found, move cursor past end and set beyond=true.
  int advance_to(const uint64_t target, bool &beyond);

  void reuse();
  void reset();

  TO_STRING_KV(K_(is_opened), K_(has_max_score), K_(cur_idx), K_(frontier_pool_size), KP_(header));

private:
  struct ObPostingMetaMaxDocIdLess
  {
    bool operator()(const ObInvIdxAggDataEntry &entry, const uint64_t target) const
    {
      return entry.max_doc_id_ < target;
    }
  };

  const ObInvIdxAggDataHeader *header_;     // agg data header in agg buffer
  const ObInvIdxAggDataEntry *entries_;     // per-posting-block entry array in agg buffer
  const char *frontier_pool_;               // base of frontier pair pool in agg buffer
  int64_t frontier_pool_size_;              // byte size of frontier pair pool
  int64_t cur_idx_;                         // current posting block index within this row
  bool has_max_score_;                      // whether to expose frontier (block max) pairs
  bool is_inited_;
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObPostingMetaRowScanner);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_META_ROW_SCANNER_H_
