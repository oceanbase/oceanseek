/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_tmp_file_append_callback.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_evict_entry.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/******************************** ObSSMacroCacheCkptBLKHeader **********************************/
OB_SERIALIZE_MEMBER(ObSSMacroCacheCkptBLKHeader,
    magic_, header_version_, header_checksum_,
    compress_type_, item_count_, payload_size_, payload_zlength_, payload_checksum_);

void ObSSMacroCacheCkptBLKHeader::reset()
{
  magic_ = SS_MACRO_CACHE_CKPT_BLK_HEADER_MAGIC;
  header_version_ = 0;
  header_checksum_ = 0;
  compress_type_ = -1;
  item_count_ = 0;
  payload_size_ = 0;
  payload_zlength_ = 0;
  payload_checksum_ = 0;
}

bool ObSSMacroCacheCkptBLKHeader::is_valid() const
{
  return SS_MACRO_CACHE_CKPT_BLK_HEADER_MAGIC == magic_
      && payload_size_ > 0;
}

int64_t ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size()
{
  // When serializing ObSSMacroCache, it's necessary to first serialize the ObSSMacroCacheMeta.
  // Only then can we construct ObSSMacroCacheCkptBLKHeader and determine the header's serialization length.
  // To identify the starting position of the payload,
  // it is serialized beginning at the get_fixed_serialize_size length.
  // Additionally, to ensure compatibility during upgrades,
  // a size significantly larger than sizeof(ObSSMacroCacheCkptBLKHeader) is used 
  // to ensure that the header size does not exceed this value even after upgrades.
  return 4096;
}

/********************************ObSSMacroCache************************************/
ObSSMacroCache::ObSSMacroCache() : lru_list_()
{
}

ObSSMacroCache::~ObSSMacroCache()
{
}

// 1. for write cache: push to flush queue, so as to change write cache to read cache.
// 2. for read cache: delete local file
int ObSSMacroCache::evict(
    const MacroBlockId &macro_id,
    const bool is_write_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (is_write_cache) { // 1. write cache
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->push_to_flush_map(macro_id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to push to flush map", KR(ret), K(macro_id));
    }
  } else { // 2. read cache
    if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(macro_id, 0/*ls_epoch_id*/,
                true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/))) {
      LOG_WARN("fail to delete local file", KR(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSMacroCache::add_into_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  if (OB_UNLIKELY(!lru_list_.fifo_list_.add_first(&macro_cache_meta))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add first fifo_list", KR(ret), K(macro_cache_meta));
  }
  return ret;
}

int ObSSMacroCache::remove_from_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  ObMutexGuard mutex_guard(lru_list_.lock_);
  return remove_from_lru_list_no_lock_(macro_cache_meta);
}

int ObSSMacroCache::remove_from_lru_list_no_lock_(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  if (macro_cache_meta.get_is_in_fifo_list()) { // 1. in fifo_list
    if (OB_ISNULL(lru_list_.fifo_list_.remove(&macro_cache_meta))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove from fifo_list", KR(ret), K(macro_cache_meta));
    }
  } else { // 2. in lru_list
    if (OB_ISNULL(lru_list_.lru_list_.remove(&macro_cache_meta))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove from lru_list", KR(ret), K(macro_cache_meta));
    }
  }
  return ret;
}

// 1. if in fifo_list currently, then move from fifo_list to lru_list
// 2. if in lru_list currently, then move to head of lru_list
int ObSSMacroCache::update_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  // update_lru_list every UPDATE_LRU_LIST_THRESHOLD times
  const bool is_need_update_lru_list = macro_cache_meta.inc_and_check_access_cnt();
  if (is_need_update_lru_list) {
    ObMutexGuard mutex_guard(lru_list_.lock_);
    if (macro_cache_meta.get_is_in_fifo_list()) { // 1. in fifo_list currently
      if (OB_ISNULL(lru_list_.fifo_list_.remove(&macro_cache_meta))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove from fifo_list", KR(ret), K(macro_cache_meta));
      } else if (OB_UNLIKELY(!lru_list_.lru_list_.add_first(&macro_cache_meta))) { // expect will never fail
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to add_first of lru_list", KR(ret), K(macro_cache_meta));
      } else {
        macro_cache_meta.set_is_in_fifo_list(false);
      }
    } else { // 2. in lru_list currently
      if (OB_UNLIKELY(!lru_list_.lru_list_.move_to_first(&macro_cache_meta))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to move_to_first of lru_list", KR(ret), K(macro_cache_meta));
      }
    }
  }
  return ret;
}

int64_t ObSSMacroCache::get_size()
{
  ObMutexGuard mutex_guard(lru_list_.lock_);
  return lru_list_.get_size();
}

int ObSSMacroCache::restore_entry_for_insert(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  return restore_entry_for_insert_no_lock_(macro_cache_meta);
}

int ObSSMacroCache::restore_entry_for_insert_no_lock_(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  if (macro_cache_meta.get_is_in_fifo_list()) {
    if (OB_UNLIKELY(!lru_list_.fifo_list_.add_first(&macro_cache_meta))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to restore into fifo_list", KR(ret), K(macro_cache_meta));
    }
  } else {
    if (OB_UNLIKELY(!lru_list_.lru_list_.add_first(&macro_cache_meta))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to restore into lru_list", KR(ret), K(macro_cache_meta));
    }
  }
  return ret;
}

int ObSSMacroCache::restore_entry_for_update(
    ObSSMacroCacheMeta &old_macro_cache_meta, ObSSMacroCacheMeta &new_macro_cache_meta)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  const bool old_in_fifo_list = old_macro_cache_meta.get_is_in_fifo_list();
  const bool new_in_fifo_list = new_macro_cache_meta.get_is_in_fifo_list();
  if (OB_UNLIKELY(false == old_in_fifo_list && new_in_fifo_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old meta in lru but new meta in fifo is impossible",
        KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
  } else {
    if (OB_FAIL(remove_from_lru_list_no_lock_(old_macro_cache_meta))) {
      LOG_WARN("fail to clean old meta",
          KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    } else if (OB_FAIL(restore_entry_for_insert_no_lock_(new_macro_cache_meta))) {
      LOG_ERROR("fail to restore new meta",
          KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    }
  }
  return ret;
}

int ObSSMacroCache::get_evict_entries(ObIArray<ObSSMacroCacheEvictEntry> &evict_entries)
{
  int ret = OB_SUCCESS;
  // evict up to 32MB or 128 entries
  static const int64_t BATCH_EVICT_SIZE = 32 * 1024 * 1024L; // 32MB
  static const int64_t BATCH_EVICT_CNT = 128;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  ObDList<ObSSMacroCacheMeta> &fifo_list = lru_list_.fifo_list_;
  ObDList<ObSSMacroCacheMeta> &lru_list = lru_list_.lru_list_;
  ObDList<ObSSMacroCacheMeta> *evict_list = ((fifo_list.get_size() > lru_list.get_size())
                                             ? &fifo_list
                                             : &lru_list);
  int64_t evict_size = 0;
  int64_t evict_cnt = 0;
  ObSSMacroCacheMeta *cur = evict_list->get_last();
  ObSSMacroCacheMeta *header = evict_list->get_header();
  while ((cur != header) && (evict_size < BATCH_EVICT_SIZE) && (evict_cnt < BATCH_EVICT_CNT) && OB_SUCC(ret)) {
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur meta is NULL", KR(ret), KP(cur));
    } else if (cur->is_ckpt_marker()) {
      // skip ckpt marker
    } else {
      ObSSMacroCacheEvictEntry evict_entry(cur->get_macro_id(), cur->get_cache_type(), cur->get_is_write_cache());
      if (OB_FAIL(evict_entries.push_back(evict_entry))) {
        LOG_WARN("fail to push back evict entry", KR(ret), K(evict_entry));
      } else {
        evict_size += cur->get_size();
        evict_cnt++;
      }
    }

    if (OB_SUCC(ret)) {
      cur = cur->get_prev();
    }
  }
  return ret;
}

int ObSSMacroCache::batch_get_tail_write_cache(
    const int64_t batch_cnt,
    ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  ObDList<ObSSMacroCacheMeta> &fifo_list = lru_list_.fifo_list_;
  ObDList<ObSSMacroCacheMeta> &lru_list = lru_list_.lru_list_;
  int64_t get_cnt = 0;
  if (OB_FAIL(inner_batch_get_tail_write_cache(batch_cnt, fifo_list, get_cnt, macro_ids))) {
    LOG_WARN("fail to inner batch get tail write cache", KR(ret), K(batch_cnt), K(get_cnt));
  } else if (OB_FAIL(inner_batch_get_tail_write_cache(batch_cnt, lru_list, get_cnt, macro_ids))) {
    LOG_WARN("fail to inner batch get tail write cache", KR(ret), K(batch_cnt), K(get_cnt));
  }
  return ret;
}

int ObSSMacroCache::inner_batch_get_tail_write_cache(
    const int64_t batch_cnt,
    common::ObDList<ObSSMacroCacheMeta> &list,
    int64_t &get_cnt,
    ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *cur = list.get_last();
  ObSSMacroCacheMeta *header = list.get_header();
  while ((cur != header) && OB_SUCC(ret) && (get_cnt < batch_cnt)) {
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur meta is NULL", KR(ret), KP(cur));
    } else if (cur->is_ckpt_marker()) {
      // skip ckpt marker
    } else if (cur->get_is_write_cache()) {
      if (OB_FAIL(macro_ids.push_back(cur->get_macro_id()))) {
        LOG_WARN("fail to push back", KR(ret), "macro_id", cur->get_macro_id());
      } else {
        get_cnt++;
      }
    }

    if (OB_SUCC(ret)) {
      cur = cur->get_prev();
    }
  }
  return ret;
}

/**
 * Serializes incremental data from the ObSSMacroCache into the provided buffer.
 * 
 * This function is invoked repeatedly during each checkpoint.
 * The serialization follows a specific traversal order:
 * fifo_list.header.prev -> fifo_list.header.next -> lru_list.header.prev -> fifo_list.header.next.
 * The first invocation during a checkpoint expects the marker's prev/next pointers to be null,
 * indicating that serialization should begin from fifo_list.header.prev.
 * 
 * Parameters:
 * - buf: The target buffer for serialization.
 * - buf_size: The size of the buffer.
 * - marker: A metadata marker indicating the current position in the serialization process.
 * - is_finished: A boolean reference indicating if the serialization has completed.
 * - item_count: count of serialized items.
 * 
 * Note: 
 * - Upon completion, indicated by is_finished being true,
 *   `clean_marker` should be invoked to remove any leftover marker nodes.
 * - An empty cache, indicated by (item_count == 0), means no data was processed during this function call.
 */
int ObSSMacroCache::incremental_serialize(
    char *buf, const int64_t buf_size,
    ObSSMacroCacheMeta &marker,
    bool &is_finished, int64_t &item_count)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  item_count = 0;
  ObMutexGuard mutex_guard(lru_list_.lock_);
  
  ObSSMacroCacheCkptBLKHeader header;
  ObSSMacroCacheMeta *cur_meta = &marker;
  bool is_in_fifo_list = marker.get_is_in_fifo_list();
  const int64_t HEADER_FIXED_SERIALIZE_SIZE = header.get_fixed_serialize_size();
  int64_t cur_serialize_offset = HEADER_FIXED_SERIALIZE_SIZE;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= cur_serialize_offset || !marker.is_ckpt_marker())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), K(cur_serialize_offset), K(marker));
  } else if (OB_ISNULL(marker.get_prev())) {  // First invocation of `incremental_serialize`
    if (!lru_list_.fifo_list_.is_empty()) {
      cur_meta = lru_list_.fifo_list_.get_header();
      is_in_fifo_list = true;
    } else if (!lru_list_.lru_list_.is_empty()) {
      cur_meta = lru_list_.lru_list_.get_header();
      is_in_fifo_list = false;
    } else {
      ret = OB_ITER_END;
      LOG_INFO("ObSSMacroCache is empty", KR(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(cur_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start marker or list header is empty", KR(ret), 
        K(is_in_fifo_list), K(marker),
        KP(lru_list_.fifo_list_.get_header()), KP(lru_list_.lru_list_.get_header()));
  }

  ObSSMacroCacheMeta *prev_meta = nullptr;
  int64_t serialize_size = -1;
  int64_t pos = 0;
  bool buf_is_full = false;
  while (OB_SUCC(ret) && !buf_is_full && cur_serialize_offset < buf_size) {
    prev_meta = nullptr;
    serialize_size = -1;
    pos = 0;
    if (OB_FAIL(get_valid_prev_meta_no_lock_(cur_meta, is_in_fifo_list, prev_meta))) {
      if (OB_ITER_END == ret) {
        LOG_INFO("iter end of prev meta", K(marker), KP(cur_meta), K(is_in_fifo_list), K(item_count),
          KP(lru_list_.fifo_list_.get_header()), KP(lru_list_.lru_list_.get_header()));
      } else {
        LOG_WARN("fail to get valid prev meta", KR(ret), K(marker), KP(cur_meta),
          K(is_in_fifo_list), K(item_count),
          KP(lru_list_.fifo_list_.get_header()), KP(lru_list_.lru_list_.get_header()));
      }
    } else {
      serialize_size = prev_meta->get_serialize_size();
      if (cur_serialize_offset + serialize_size > buf_size) {
        buf_is_full = true;
        // skip serialize
      } else if (OB_FAIL(prev_meta->serialize(
          buf + cur_serialize_offset,
          buf_size - cur_serialize_offset,
          pos))) {
        LOG_WARN("fail to serilize macro cache meta", KR(ret), K(marker), KPC(prev_meta));
      } else if (OB_UNLIKELY(serialize_size != pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("position 'pos' does not match the expected serialized size of macro cache meta",
            KR(ret), K(serialize_size), K(pos), K(marker), KPC(prev_meta));
      } else {
        cur_meta = prev_meta;
        cur_serialize_offset += serialize_size;
        prev_meta = nullptr;
        item_count++;
      }
    }
  }

  // Four possible return scenarios:
  // 1. Iteration finished with no data - cache is empty
  // 2. Iteration finished with data - partial serialization
  // 3. Not finished but no data processed - unexpected
  // 4. Not finished with data processed - buffer full
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == item_count)) { // Scenario 3
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to serialize any meta, may be buf size too small",
          KR(ret), K(buf_size), K(marker));
    } else {  // Scenario 4
    }
  } else if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_finished = true;

    if (0 == item_count) { // Scenario 1
    } else {  // Scenario 2
    }
  }

  if (OB_SUCC(ret) && (item_count > 0)) {
    header.set_item_count(item_count);
    header.set_payload_size(cur_serialize_offset - HEADER_FIXED_SERIALIZE_SIZE);
    const uint64_t payload_checksum =
        ob_crc64(buf + HEADER_FIXED_SERIALIZE_SIZE, header.get_payload_size());
    header.set_payload_checksum(static_cast<uint32_t>(payload_checksum));

    pos = 0;
    if (OB_FAIL(header.serialize(buf, HEADER_FIXED_SERIALIZE_SIZE, pos))) {
      LOG_WARN("fail to serilize ckpt data header", KR(ret), K(header));
    } else if (OB_UNLIKELY(header.get_serialize_size() != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position 'pos' does not match the expected serialized size of ckpt blk header",
          KR(ret), K(header), K(pos), K(header.get_serialize_size()));
    } else if (OB_FAIL(advance_marker_no_lock_(marker, cur_meta, is_in_fifo_list))) {
      LOG_WARN("fail to advance marker position", KR(ret),
          K(header), K(marker), KP(cur_meta), K(is_in_fifo_list));
    } 
  }

  return ret;
}

int ObSSMacroCache::advance_marker_no_lock_(
    ObSSMacroCacheMeta &marker,
    ObSSMacroCacheMeta *cur_meta,
    const bool is_in_fifo_list)
{
  int ret = OB_SUCCESS;
  // cur_meta may by header, no need to check validity
  if (OB_ISNULL(cur_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(marker), KP(cur_meta), K(is_in_fifo_list));
  // if in first serialization, marker is not in list, no need to remove it
  } else if (OB_NOT_NULL(marker.get_prev()) && OB_FAIL(remove_from_lru_list_no_lock_(marker))) {
    LOG_WARN("fail to remove marker", KR(ret), K(is_in_fifo_list), K(marker), KP(cur_meta));
  } else {
    // advance marker position
    if (is_in_fifo_list) {
      if (OB_UNLIKELY(!lru_list_.fifo_list_.add_before(cur_meta, &marker))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to re insert marker into fifo list", KR(ret), KP(cur_meta), K(marker));
      }
    } else {
      if (OB_UNLIKELY(!lru_list_.lru_list_.add_before(cur_meta, &marker))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to re insert marker into fifo list", KR(ret), KP(cur_meta), K(marker));
      }
    }
    marker.set_is_in_fifo_list(is_in_fifo_list);
  }
  return ret;
}

int ObSSMacroCache::clean_marker()
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(lru_list_.lock_);

  if (OB_FAIL(clean_marker_in_dlist_(lru_list_.fifo_list_))) {
    LOG_WARN("fail to clean fifo list", KR(ret));
  } else if (OB_FAIL(clean_marker_in_dlist_(lru_list_.lru_list_))) {
    LOG_WARN("fail to clean lru list", KR(ret));
  }
  return ret;
}

int ObSSMacroCache::clean_marker_in_dlist_(ObSSMacroCacheLRUList::DListType &dlist)
{
  int ret = OB_SUCCESS;
  if (!dlist.is_empty()) {
    const ObSSMacroCacheMeta *header = dlist.get_header();
    ObSSMacroCacheMeta *cur_meta = dlist.get_last();
    ObSSMacroCacheMeta *marker_meta = nullptr;

    while (OB_SUCC(ret) && cur_meta != header) {
      marker_meta = nullptr;
      if (OB_ISNULL(cur_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_meta is NULL", KR(ret), KP(cur_meta));
      } else if (cur_meta->is_ckpt_marker()) {
        marker_meta = cur_meta;
        cur_meta = cur_meta->get_prev();
        if (OB_UNLIKELY(!dlist.remove(marker_meta))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to remove marker", KR(ret), KP(cur_meta), KPC(marker_meta));
        }
      } else {
        cur_meta = cur_meta->get_prev();
      }
    }
  }
  return ret;
}

// Obtain the previous node of `cur_meta`.
// If the previous node is the header of the FIFO list,
// it indicates the end of the FIFO list traversal, switching to the LRU list to continue.
// If the previous node is the header of the LRU list,
// it signals the completion of the entire macro cache traversal.
//
// Returning OB_ITER_END indicates that ObSSMacroCache has been completely traversed.
// Returning OB_SUCCESS means a valid macro cache meta, which needs to be persisted, has been obtained.
int ObSSMacroCache::get_valid_prev_meta_no_lock_(
    ObSSMacroCacheMeta *&cur_meta,
    bool &is_in_fifo_list,
    ObSSMacroCacheMeta *&prev_meta)
{
  int ret = OB_SUCCESS;
  prev_meta = nullptr;
  if (OB_FAIL(check_and_get_prev_meta_no_lock_(cur_meta, prev_meta))) {
    LOG_WARN("fail to check and get prev meta", KR(ret), K(is_in_fifo_list), KP(cur_meta));
  } else if (is_in_fifo_list && prev_meta == lru_list_.fifo_list_.get_header()) {
    if (lru_list_.lru_list_.is_empty()) {
      ret = OB_ITER_END;
      LOG_INFO("iterate to fifo list end and lru_list_ is empty", KR(ret),
          K(is_in_fifo_list), KP(cur_meta), KP(prev_meta));
    } else {
      cur_meta = lru_list_.lru_list_.get_header();
      is_in_fifo_list = false;
      if (OB_FAIL(check_and_get_prev_meta_no_lock_(cur_meta, prev_meta))) {
        LOG_WARN("fail to check and get prev meta", KR(ret), K(is_in_fifo_list), KP(cur_meta));
      }
    }
  } else if (!is_in_fifo_list && prev_meta == lru_list_.lru_list_.get_header()) {
    ret = OB_ITER_END;
    LOG_INFO("iterate to lru list end", KR(ret),
        K(is_in_fifo_list), KP(cur_meta), KP(prev_meta));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(prev_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev macro cache meta is NULL", KR(ret),
          K(is_in_fifo_list), KP(cur_meta), KP(prev_meta));
    } else if (OB_UNLIKELY(prev_meta == lru_list_.fifo_list_.get_header()
        || prev_meta == lru_list_.lru_list_.get_header())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev macro cache meta is unexpected invalid", KR(ret),
          K(is_in_fifo_list), KP(cur_meta), KP(prev_meta),
          KP(lru_list_.fifo_list_.get_header()), KP(lru_list_.lru_list_.get_header()));
    } else if (OB_UNLIKELY(prev_meta->is_ckpt_marker())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev macro cache meta is ckpt marker", KR(ret),
          K(is_in_fifo_list), KP(cur_meta), KP(prev_meta), K(is_in_fifo_list));
    } else if (OB_UNLIKELY(!prev_meta->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev macro cache meta is invalid", KR(ret),
          K(is_in_fifo_list), KP(cur_meta), KP(prev_meta));
    }
  }
  return ret;
}

// Obtain the previous node of `cur_meta`. 
// A successful return indicates that the previous node is non-null.
// The provided `cur_meta` might be a header or marker node, so its validity check is unnecessary.
// Furthermore, validation is not required because only prev_meta is used during persistence.
int ObSSMacroCache::check_and_get_prev_meta_no_lock_(
    ObSSMacroCacheMeta *cur_meta,
    ObSSMacroCacheMeta *&prev_meta)
{
  int ret = OB_SUCCESS;
  prev_meta = nullptr;
  if (OB_ISNULL(cur_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cur meta is invalid", KR(ret), KP(cur_meta));
  } else if (OB_ISNULL(cur_meta->get_prev())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prev meta is invalid", KR(ret), KP(cur_meta));
  } else {
    prev_meta = cur_meta->get_prev();
  }
  return ret;
}


/********************************ObSSTmpFileCache************************************/
// 1. for write cache: check if segment has been appended. if has been appended, then push to flush
//    queue. if has not been appended, then delete local file.
// 2. for read cache: delete local file
int ObSSTmpFileCache::evict(
    const MacroBlockId &macro_id,
    const bool is_write_cache)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (is_write_cache) { // 1. write cache
    TmpFileSegId seg_id(macro_id.second_id(), macro_id.fourth_id());
    bool is_meta_exist = false;
    TmpFileMetaHandle meta_handle;
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), K(macro_id));
    } else if (OB_FAIL(file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to get seg meta", KR(ret), K(seg_id));
    } else if (!is_meta_exist) {
      // do nothing. seg_meta is not existed, maybe segment file has been gc
      LOG_INFO("segment file meta is not exist", K(seg_id));
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is invalid", KR(ret), K(meta_handle));
    } else {
      ObSSTmpFileEvictCallback evict_callback(macro_id);
      if (OB_FAIL(meta_handle.check_tmp_file_appending(evict_callback))) {
        LOG_WARN("fail to check tmp file appending", KR(ret), K(meta_handle), K(evict_callback));
      }
    }
  } else { // 2. read cache
    // Note: tmp file read cache has no TmpFileMeta. hence, there is no need to set is_in_local =
    // false in TmpFileMeta's lock.
    if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(macro_id, 0/*ls_epoch_id*/,
                true/*is_print_log*/, false/*is_del_seg_meta*/, false/*is_logical_delete*/))) {
      LOG_WARN("fail to delete local file", KR(ret), K(macro_id));
    }
  }
  return ret;
}


/********************************ObSSMacroCacheFactory************************************/
int ObSSMacroCacheFactory::create(
    const ObSSMacroCacheType cache_type,
    ObSSMacroCache *&macro_cache)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "SSMacroCache");
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret));
  } else {
    switch (cache_type) {
#define DEF_SS_MACRO_CACHE(type, min, max, weight, type_str, class_name) \
      case ObSSMacroCacheType::type: { \
        class_name *cache = nullptr; \
        if (OB_ISNULL(cache = static_cast<class_name *>(ob_malloc(sizeof(class_name), mem_attr)))) { \
          ret = OB_ALLOCATE_MEMORY_FAILED; \
          LOG_WARN("fail to alloc memory for ss macro cache", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type)); \
        } else { \
          cache = new (cache) class_name(); \
          macro_cache = cache; \
        } \
        break; \
      }
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#undef DEF_SS_MACRO_CACHE
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
        break;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
