/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_sstable_doc_id_prefetcher.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

namespace
{
void cancel_and_reset_data_handle(ObMicroBlockDataHandle &data_handle)
{
  if (data_handle.io_handle_.is_valid() && !data_handle.io_handle_.is_finished()) {
    data_handle.io_handle_.cancel();
  }
  data_handle.reset();
}
} // namespace

ObInvSSTableDocIdPrefetcher::ObInvSSTableDocIdPrefetcher()
  : allocator_(nullptr), slots_(), fetch_idx_(0), prefetch_idx_(0)
{}

void ObInvSSTableDocIdPrefetcher::init(ObFIFOAllocator &allocator)
{
  allocator_ = &allocator;
}

void ObInvSSTableDocIdPrefetcher::quiesce()
{
  for (int64_t i = 0; i < MAX_PREFETCH_DEPTH; ++i) {
    cancel_and_reset_data_handle(slots_[i].data_handle_);
    slots_[i].first_block_idx_ = 0;
    slots_[i].last_block_idx_ = -1;
  }
  fetch_idx_ = 0;
  prefetch_idx_ = 0;
  allocator_ = nullptr;
}

int ObInvSSTableDocIdPrefetcher::peek_front(FrontView &front)
{
  int ret = OB_SUCCESS;
  front.reset();
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("flat doc-id prefetch ring is empty", K(ret));
  } else {
    Slot &slot = slots_[fetch_idx_ % MAX_PREFETCH_DEPTH];
    if (OB_FAIL(slot.data_handle_.get_micro_block_data(nullptr, front.block_data_, true))) {
      LOG_WARN("failed to get front prefetched micro block data", K(ret), K(slot.data_handle_));
    } else {
      front.macro_id_ = slot.data_handle_.macro_block_id_;
      front.block_offset_ = slot.data_handle_.micro_info_.offset_;
      front.block_size_ = slot.data_handle_.micro_info_.size_;
      front.first_block_idx_ = slot.first_block_idx_;
      front.last_block_idx_ = slot.last_block_idx_;
    }
  }
  return ret;
}

int ObInvSSTableDocIdPrefetcher::take_front_handle(ObMicroBlockDataHandle &data_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("flat doc-id prefetch ring is empty", K(ret));
  } else {
    Slot &slot = slots_[fetch_idx_ % MAX_PREFETCH_DEPTH];
    data_handle.move_from(slot.data_handle_);
    slot.reset();
    ++fetch_idx_;
  }
  return ret;
}

int ObInvSSTableDocIdPrefetcher::issue(
    const ObMicroIndexInfo &index_info,
    const ObTabletID &effective_tablet_id,
    const ObQueryFlag &query_flag,
    const int64_t first_block_idx,
    const int64_t last_block_idx)
{
  int ret = OB_SUCCESS;
  bool issued = false;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("flat doc-id prefetcher is not initialized", K(ret));
  } else if (OB_UNLIKELY(!index_info.is_valid()
             || !index_info.is_data_block()
             || !effective_tablet_id.is_valid()
             || first_block_idx < 0
             || (last_block_idx >= 0 && last_block_idx < first_block_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flat doc-id prefetch argument",
             K(ret), K(index_info), K(effective_tablet_id),
             K(first_block_idx), K(last_block_idx));
  } else if (OB_UNLIKELY(is_full())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("flat doc-id prefetch ring is full", K(ret),
             K_(fetch_idx), K_(prefetch_idx));
  } else {
    Slot &slot = slots_[prefetch_idx_ % MAX_PREFETCH_DEPTH];
    ObMicroBlockDataHandle &data_handle = slot.data_handle_;
    ObMicroBlockCacheKey cache_key(MTL_ID(), index_info);
    ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
    data_handle.allocator_ = allocator_;
    data_handle.tenant_id_ = MTL_ID();
    data_handle.macro_block_id_ = index_info.get_macro_id();
    data_handle.micro_info_.set(
        index_info.get_block_offset(), index_info.get_block_size(),
        index_info.get_logic_micro_id(), index_info.get_data_checksum());
    if (OB_FAIL(cache.get_cache_block(cache_key, data_handle.cache_handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("failed to get prefetched flat doc-id cache block", K(ret), K(index_info));
      } else if (OB_FAIL(cache.prefetch(
                 MTL_ID(), index_info.get_macro_id(), index_info,
                 query_flag.is_use_block_cache(), effective_tablet_id,
                 data_handle.io_handle_, allocator_, true /*is_preread*/))) {
        if (OB_EAGAIN == ret) {
          LOG_TRACE("pause flat doc-id prefetch because IO queue is busy");
        } else {
          LOG_WARN("failed to issue flat doc-id prefetch", K(ret), K(index_info));
        }
      } else {
        data_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        issued = true;
      }
    } else {
      data_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      issued = true;
    }
    if (issued) {
      slot.first_block_idx_ = first_block_idx;
      slot.last_block_idx_ = last_block_idx;
      ++prefetch_idx_;
    } else {
      cancel_and_reset_data_handle(data_handle);
      slot.first_block_idx_ = 0;
      slot.last_block_idx_ = -1;
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
