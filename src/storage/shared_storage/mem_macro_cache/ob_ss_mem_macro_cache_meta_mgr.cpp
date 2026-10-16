/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/random/ob_random.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_meta_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_define.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_util.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMemMacroCacheGetMemBlkMacrosCb-----------------------------------------*/
ObSSMemMacroCacheGetMemBlkMacrosCb::ObSSMemMacroCacheGetMemBlkMacrosCb(
    const ObIArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles,
    ObIArray<blocksstable::MacroBlockId> &macro_ids)
  : mem_blk_handles_(mem_blk_handles), macro_ids_(macro_ids)
{}

int ObSSMemMacroCacheGetMemBlkMacrosCb::operator()(const SSMemMacroMetaMapPair &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.first.is_valid() || !entry.second.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_meta_map entry should be valid", KR(ret), "macro_id", entry.first, "macro_meta_handle",
      entry.second);
  } else {
    const int64_t cmp_mem_blk_cnt = mem_blk_handles_.count();
    bool is_match = false;
    for (int64_t i = 0; OB_SUCC(ret) && (i < cmp_mem_blk_cnt) && !is_match; ++i) {
      if (entry.second.get_ptr()->get_mem_blk_handle().get_ptr() == mem_blk_handles_.at(i).get_ptr()) {
        if (OB_FAIL(macro_ids_.push_back(entry.first))) {
          LOG_WARN("fail to push back macro_id", KR(ret), "macro_id", entry.first, "macro_meta_handle",
            entry.second);
        } else {
          is_match = true;
        }
      }
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMemMacroCacheEraseMetaPred-----------------------------------------*/
bool ObSSMemMacroCacheEraseMetaPred::operator()(const SSMemMacroMetaMapPair &entry)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.first.is_valid() || !entry.second.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_meta_map entry should be valid", KR(ret), "macro_id", entry.first, "macro_meta_handle",
      entry.second);
  } else if (OB_UNLIKELY(!entry.second.get_ptr()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_block meta should be valid", KR(ret), "macro_id", entry.first, KPC(entry.second.get_ptr()));
  } else {
    size_ = entry.second.get_ptr()->size_;
  }

  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------ObSSMemMacroCachePutMetaCb-----------------------------------------*/
int ObSSMemMacroCachePutMetaCb::operator()(const SSMemMacroMetaMapPair &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.first.is_valid() || !entry.second.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_meta_map entry should be valid", KR(ret), "macro_id", entry.first, "macro_meta_handle",
      entry.second);
  } else if (OB_UNLIKELY(!entry.second.get_ptr()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_block meta should be valid", KR(ret), "macro_id", entry.first, KPC(entry.second.get_ptr()));
  } else if (OB_UNLIKELY(!entry.second.get_ptr()->mem_blk_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_blk_handle in macro_block meta should be valid", KR(ret), "macro_id", entry.first, KPC(entry.second.get_ptr()));
  } else {
    entry.second.get_ptr()->mem_blk_handle_.get_ptr()->inc_macro_blk_cnt();
  }

  return ret;
}

/*-----------------------------------------ObSSMemMacroCacheMarkFastEvictCb-----------------------------------------*/
int ObSSMemMacroCacheMarkFastEvictCb::operator()(SSMemMacroMetaMapPair &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.first.is_valid() || !entry.second.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_meta_map entry should be valid", KR(ret), "macro_id", entry.first, "macro_meta_handle",
      entry.second);
  } else if (OB_UNLIKELY(!entry.second.get_ptr()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_block meta should be valid", KR(ret), "macro_id", entry.first, KPC(entry.second.get_ptr()));
  } else {
    entry.second.get_ptr()->set_need_persist(need_persist_);
    const int64_t ttl_time_us = SS_DEF_MACRO_BLOCK_TTL_TIME_US;
    const int64_t max_wait_time_us = SS_DEF_ASYNC_EVICT_TRIGGER_TIME_MAX_US;
    entry.second.get_ptr()->create_time_us_ = ObSSMemMacroCacheUtil::get_random_close_ttl_time(ttl_time_us, max_wait_time_us);
  }

  return ret;
}

/*-----------------------------------------ObSSMemMacroCacheMetaMgr-----------------------------------------*/
ObSSMemMacroCacheMetaMgr::ObSSMemMacroCacheMetaMgr(ObSSMemMacroCacheStat &cache_stat)
  : is_inited_(false), macro_meta_map_(), cache_stat_(cache_stat)
{}

int ObSSMemMacroCacheMetaMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_cnt = SS_DEF_MACRO_META_MAP_BUCKET_CNT;
  ObMemAttr map_attr(tenant_id, "MemMacMetaMap");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_FAIL(macro_meta_map_.create(bucket_cnt, map_attr))) {
    LOG_WARN("fail to create macro_meta map", KR(ret), K(bucket_cnt), K(map_attr));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSMemMacroCacheMetaMgr::destroy()
{
  macro_meta_map_.destroy();
  is_inited_ = false;
}

void ObSSMemMacroCacheMetaMgr::clear()
{
  macro_meta_map_.clear();
}

int ObSSMemMacroCacheMetaMgr::add_mem_macro_block_meta(
    const MacroBlockId &macro_id,
    const uint64_t effective_tablet_id,
    const ObSSMacroCacheMemBlkHandle &mem_blk_handle,
    const int32_t offset,
    const int32_t size,
    const uint32_t crc,
    const bool need_fast_persist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !mem_blk_handle.is_valid() || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(mem_blk_handle), K(offset), K(size));
  } else {
    ObSSMacroCacheBlkMetaHandle macro_meta_handle;
    if (OB_FAIL(inner_alloc_mem_macro_block_meta(macro_meta_handle))) {
      LOG_WARN("fail to alloc mem macro_block meta", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(!macro_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_meta handle should be valid", KR(ret), K(macro_meta_handle), K(macro_id));
    } else {
      macro_meta_handle()->crc_ = crc;
      macro_meta_handle()->size_ = size;
      macro_meta_handle()->offset_ = offset;
      macro_meta_handle()->effective_tablet_id_ = effective_tablet_id;
      macro_meta_handle()->mem_blk_handle_ = mem_blk_handle;
      // If need_fast_persist is true, use fast evict logic
      if (need_fast_persist) {
        const int64_t ttl_time_us = SS_DEF_MACRO_BLOCK_TTL_TIME_US;
        const int64_t max_wait_time_us = SS_DEF_FAST_PERSIST_TRIGGER_TIME_MAX_US;
        macro_meta_handle()->create_time_us_ = ObSSMemMacroCacheUtil::get_random_close_ttl_time(ttl_time_us, max_wait_time_us);
      } else {
        macro_meta_handle()->create_time_us_ = ObTimeUtility::current_time_us();
      }
      ObSSMemMacroCachePutMetaCb put_cb;

      if (OB_UNLIKELY(!macro_meta_handle()->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro_block meta should be valid", KR(ret), KPC(macro_meta_handle.get_ptr()));
      } else if (OB_FAIL(macro_meta_map_.set_refactored(macro_id, macro_meta_handle, 0/*overwrite*/, 0/*broadcast*/,
                         0/*overwrite_key*/, &put_cb))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to put macro_block meta", KR(ret), K(macro_id), K(macro_meta_handle));
        }
      } else {
        const bool is_small_macro = (size <= SS_SMALL_MACRO_BLOCK_SIZE);
        cache_stat_.macro_blk_stat().update_valid_macro_info(1, size, is_small_macro);
        LOG_TRACE("ss_mem_macro_cache: succ add macro meta", K(macro_id), KPC(macro_meta_handle.get_ptr()), K(need_fast_persist));
      }

      if (OB_FAIL(ret)) {
        macro_meta_handle()->mem_blk_handle_.reset();
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::inner_alloc_mem_macro_block_meta(ObSSMacroCacheBlkMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro_meta_handle should not be valid", KR(ret), K(meta_handle));
  } else {
    void *ptr = SSMemMacroMetaAlloc.alloc();
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc micro_meta", KR(ret));
    } else {
      ObSSMacroCacheBlockMeta *macro_meta = new(ptr) ObSSMacroCacheBlockMeta();
      macro_meta->reset();
      meta_handle.set_ptr(macro_meta);
      const int64_t macro_meta_usage = SSMemMacroMetaAlloc.hold();
      cache_stat_.mem_usage_stat().update_macro_meta_usage(1, macro_meta_usage);
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::get_mem_macro_block_meta(
    const MacroBlockId &macro_id, 
    ObSSMacroCacheBlkMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(meta_handle));
  } else if (OB_FAIL(macro_meta_map_.get_refactored(macro_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get mem macro_block meta", KR(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::get_macro_ids_by_mem_blk(
    const ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles,
    ObArray<blocksstable::MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(mem_blk_handles.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handles.count()));
  } else {
    ObSSMemMacroCacheGetMemBlkMacrosCb get_cb(mem_blk_handles, macro_ids);
    if (OB_FAIL(macro_meta_map_.foreach_refactored(get_cb))) {
      LOG_WARN("fail to handle each meta", KR(ret));
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::erase_mem_macro_block_meta(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    bool is_erased = false;
    ObSSMemMacroCacheEraseMetaPred erase_pred;
    if (OB_FAIL(macro_meta_map_.erase_if(macro_id, erase_pred, is_erased))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to erase mem macro_block meta", KR(ret), K(macro_id));
      }
    } else if (!is_erased) {
      LOG_WARN("fail to erase macro_meta", K(macro_id));
    } else {
      const int32_t size = erase_pred.get_size();
      const bool is_small_macro = (size <= SS_SMALL_MACRO_BLOCK_SIZE);
      cache_stat_.macro_blk_stat().update_valid_macro_info(-1, -1 * size, is_small_macro);
      LOG_TRACE("ss_mem_macro_cache: succ erase macro_meta", K(macro_id));
    }
  } 
  return ret;
}

int ObSSMemMacroCacheMetaMgr::erase_mem_macro_block_metas(const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t macro_id_cnt = macro_ids.count();
  if (OB_UNLIKELY(macro_id_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id_cnt));
  } else {
    for (int64_t i = 0; i < macro_id_cnt; ++i) {
      const MacroBlockId &macro_id = macro_ids.at(i);
      if (OB_TMP_FAIL(erase_mem_macro_block_meta(macro_id))) {
        LOG_WARN("fail to erase mem macro_block meta", KR(tmp_ret), K(i), K(macro_id_cnt), K(macro_id));
        tmp_ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::exist_mem_macro_block_meta(const MacroBlockId &macro_id, bool &already_exist)
{
  int ret = OB_SUCCESS;
  already_exist = false;
  ObSSMacroCacheBlkMetaHandle meta_handle;
  if (OB_FAIL(get_mem_macro_block_meta(macro_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get mem macro_block meta", KR(ret), K(macro_id));
    }
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_meta handle should be valid", KR(ret), K(meta_handle));
  } else {
    already_exist = true;
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::acquire_ttl_mem_blocks(
    const int64_t exp_mem_blk_cnt, 
    ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(exp_mem_blk_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exp_mem_blk_cnt));
  } else if (OB_LIKELY(exp_mem_blk_cnt > mem_blk_handles.count())) {
    SSMemMacroMetaMap::bucket_iterator bucket_iter = macro_meta_map_.bucket_begin();
    while (OB_SUCC(ret) && (bucket_iter != macro_meta_map_.bucket_end())) {
      SSMemMacroMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
      SSMemMacroMetaMap::hashtable::readlocker locker(blk.lock());
      SSMemMacroMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
      while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end()) && (mem_blk_handles.count() < exp_mem_blk_cnt)) {
        const MacroBlockId &macro_id = node_iter->first;
        const ObSSMacroCacheBlkMetaHandle &macro_meta_handle = node_iter->second;
        if (OB_UNLIKELY(!macro_meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("macro_meta handle is not valid", KR(ret), K(macro_id), K(macro_meta_handle));
        } else if (OB_UNLIKELY(!macro_meta_handle()->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("macro_meta is not valid", KR(ret), K(macro_id), KPC(macro_meta_handle.get_ptr()));
        } else if (macro_meta_handle()->is_expired()) {
          bool already_exist = false;
          for (int64_t i = 0; !already_exist && i < mem_blk_handles.count(); ++i) {
            if (macro_meta_handle()->get_mem_blk_handle().get_ptr() == mem_blk_handles.at(i).get_ptr()) {
              already_exist = true;
            }
          }
          if (!already_exist) {
            if (OB_FAIL(mem_blk_handles.push_back(macro_meta_handle()->get_mem_blk_handle()))) {
              LOG_WARN("fail to push back", KR(ret), KPC(macro_meta_handle.get_ptr()));
            }
          }
        }
        node_iter++;
      }
      ++bucket_iter;
    }
  }
  return ret;
}

int ObSSMemMacroCacheMetaMgr::mark_macro_block_fast_evict(
    const MacroBlockId &macro_id,
    const bool need_persist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    ObSSMemMacroCacheMarkFastEvictCb mark_cb(need_persist);
    if (OB_FAIL(macro_meta_map_.atomic_refactored(macro_id, mark_cb))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to mark macro_block fast evict", KR(ret), K(macro_id), K(need_persist));
      }
    } else {
      cache_stat_.macro_blk_stat().inc_async_evict_macro_cnt();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
