/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_DOC_ID_PREFETCHER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_DOC_ID_PREFETCHER_H_

#include "common/ob_common_types.h"
#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{

class ObInvSSTableDocIdPrefetcher final
{
public:
  struct FrontView
  {
    FrontView() { reset(); }
    void reset()
    {
      block_data_.reset();
      macro_id_.reset();
      block_offset_ = 0;
      block_size_ = 0;
      first_block_idx_ = 0;
      last_block_idx_ = -1;
    }
    bool is_valid() const { return block_data_.is_valid() && macro_id_.is_valid(); }

    // block_data_ borrows storage from the front slot. After take_front_handle(),
    // the caller's handle must outlive every view derived from block_data_.
    ObMicroBlockData block_data_;
    MacroBlockId macro_id_;
    int64_t block_offset_;
    int64_t block_size_;
    int64_t first_block_idx_;
    int64_t last_block_idx_;
  };

  ObInvSSTableDocIdPrefetcher();
  void init(common::ObFIFOAllocator &allocator);
  void quiesce();
  bool is_empty() const { return fetch_idx_ == prefetch_idx_; }
  bool is_full() const { return prefetch_idx_ - fetch_idx_ == MAX_PREFETCH_DEPTH; }
  int issue(
      const ObMicroIndexInfo &index_info,
      const common::ObTabletID &effective_tablet_id,
      const common::ObQueryFlag &query_flag,
      const int64_t first_block_idx,
      const int64_t last_block_idx);
  int peek_front(FrontView &front);
  // Transfer the front slot's handle. Keep data_handle alive while using the peeked block data.
  int take_front_handle(storage::ObMicroBlockDataHandle &data_handle);
private:
  struct Slot
  {
    Slot() : data_handle_(), first_block_idx_(0), last_block_idx_(-1) {}
    void reset()
    {
      data_handle_.reset();
      first_block_idx_ = 0;
      last_block_idx_ = -1;
    }

    storage::ObMicroBlockDataHandle data_handle_;
    int64_t first_block_idx_;
    int64_t last_block_idx_;
    DISABLE_COPY_ASSIGN(Slot);
  };

  static const int64_t MAX_PREFETCH_DEPTH = 5;
  common::ObFIFOAllocator *allocator_;
  Slot slots_[MAX_PREFETCH_DEPTH];
  int64_t fetch_idx_;
  int64_t prefetch_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObInvSSTableDocIdPrefetcher);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_SSTABLE_DOC_ID_PREFETCHER_H_
