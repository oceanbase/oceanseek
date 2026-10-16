/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObSSMemMacroCache::ObSSMemMacroCache()
  : is_inited_(false), is_stopped_(false), is_enabled_(true), is_clear_(false), blk_size_(0), tg_id_(INVALID_TG_ID),
    tenant_id_(OB_INVALID_TENANT_ID), mem_limit_size_(0), flying_req_cnt_(0), cache_stat_(), meta_mgr_(cache_stat_), 
    buf_mgr_(cache_stat_), evict_task_(buf_mgr_, meta_mgr_, cache_stat_), meta_allocator_()
{}

int ObSSMemMacroCache::mtl_init(ObSSMemMacroCache *&mem_macro_cache)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t tenant_mem_size = MTL_MEM_SIZE();
  const int64_t mem_limit_size = tenant_mem_size * SS_DEF_MEM_MACRO_CACHE_LIMIT_PCT / 100;
  if (OB_FAIL(mem_macro_cache->init(tenant_id, mem_limit_size))) {
    LOG_WARN("fail to init mem_macro_cache", KR(ret), K(tenant_id), K(mem_limit_size), K(tenant_mem_size));
  }
  return ret;
}

int ObSSMemMacroCache::init(const uint64_t tenant_id, const int64_t memory_limit_size)
{
  int ret = OB_SUCCESS;
  const int32_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  int32_t mem_blk_size = (macro_block_size > 0) ? macro_block_size : SS_DEF_MEM_MACRO_CACHE_BLK_SIZE;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (mem_blk_size < 1) || (memory_limit_size < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(mem_blk_size), K(memory_limit_size));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MemMacroCacheTimer, tg_id_))) {
    LOG_WARN("fail to create thread", KR(ret));
  } else if (OB_FAIL(buf_mgr_.init(tenant_id, mem_blk_size, memory_limit_size / mem_blk_size))) {
    LOG_WARN("fail to init buf_mgr", KR(ret), K(tenant_id), K(mem_blk_size), K(memory_limit_size));
  } else if (OB_FAIL(meta_allocator_.init(sizeof(ObSSMacroCacheBlockMeta), "MemMacroMeta", tenant_id,
             SS_MACRO_META_ALLOCATOR_BLK_SIZE))) {
    LOG_WARN("fail to init macro_block meta allocator", KR(ret), K(sizeof(ObSSMacroCacheBlockMeta)), K(tenant_id));
  } else if (OB_FAIL(meta_mgr_.init(tenant_id))) {
    LOG_WARN("fail to init meta_mgr", KR(ret), K(tenant_id), K(memory_limit_size));
  } else if (OB_FAIL(evict_task_.init(tenant_id))) {
    LOG_WARN("fail to init evict_task", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    blk_size_ = mem_blk_size;
    mem_limit_size_ = memory_limit_size;
    is_inited_ = true;
    LOG_INFO("succ to init mem_macro_cache", K_(tg_id), K_(blk_size), K_(mem_limit_size));
  }
  return ret;
}

int ObSSMemMacroCache::start()
{
  int ret = OB_SUCCESS;
  const int64_t interval_us = SS_MEM_MACRO_CACHE_EVICT_TASK_INTERVAL_US;
  if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, evict_task_, interval_us, true/*repeat*/))) {
    LOG_WARN("fail to schedule evict task", KR(ret), K_(tg_id), K(interval_us));
  } else {
    LOG_INFO("succ to schedule mem_macro_cache evict task", K_(tg_id), K(interval_us));
  }
  return ret;
}

void ObSSMemMacroCache::stop()
{
  FLOG_INFO("begin ss_mem_macro_cache stop");
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_stopped_, true);
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  do {
    if (ATOMIC_LOAD(&is_clear_)) {
      ob_usleep(SS_MEM_LOOP_SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(SS_MEM_LOOP_PRINT_LOG_INTERVAL_US)) {
        FLOG_WARN("still wait for clear_mem_macro_cache finish", K_(is_clear));
      }
    } else {
      break;
    }
  } while (OB_SUCC(ret) && (ObTimeUtil::current_time_us() - start_time_us < SS_CLEAR_MEM_MACRO_CACHE_TIMEOUT_US));
  
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  FLOG_INFO("finish ss_mem_macro_cache stop");
}

int ObSSMemMacroCache::wait()
{
  FLOG_INFO("begin ss_mem_macro_cache wait");
  int ret = OB_SUCCESS;
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  FLOG_INFO("finish ss_mem_macro_cache wait");
  return ret;
}

void ObSSMemMacroCache::destroy()
{
  FLOG_INFO("begin ss_mem_macro_cache destroy");
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    bool fin_destroy = false;
    do {
      const int64_t flying_req_cnt = ATOMIC_LOAD(&flying_req_cnt_);
      if (flying_req_cnt > 0) {
        ob_usleep(SS_MEM_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_MEM_LOOP_PRINT_LOG_INTERVAL_US)) {
          FLOG_WARN("still exist request which may access macro_meta_map", K(flying_req_cnt));
        }
      } else {
        if (INVALID_TG_ID != tg_id_) {
          TG_DESTROY(tg_id_);
        }
        evict_task_.destroy();
        meta_mgr_.destroy();
        buf_mgr_.destroy();

        // must execute it at the last step
        inner_check_macro_meta_cnt();
        meta_allocator_.destroy();
        fin_destroy = true;
      }
    } while (!fin_destroy);
  }

  blk_size_ = 0;
  mem_limit_size_ = 0;
  flying_req_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
  is_enabled_ = true;
  is_clear_ = false;
  FLOG_INFO("finish ss_mem_macro_cache destroy", K_(cache_stat));
  cache_stat_.reset();
}

bool ObSSMemMacroCache::is_available() const
{
  return (!ATOMIC_LOAD(&is_stopped_)) && (ATOMIC_LOAD(&is_enabled_) && (!ATOMIC_LOAD(&is_clear_)) &&
         (ATOMIC_LOAD(&mem_limit_size_) > 0));
}

bool ObSSMemMacroCache::is_cache_enabled() const
{
  return ATOMIC_LOAD(&is_enabled_);
}

void ObSSMemMacroCache::enable_cache()
{
  ATOMIC_STORE(&is_enabled_, true);
  FLOG_INFO("enable mem_macro_cache", K_(tenant_id), K_(is_enabled));
}

void ObSSMemMacroCache::disable_cache()
{
  ATOMIC_STORE(&is_enabled_, false);
  FLOG_INFO("disable mem_macro_cache", K_(tenant_id), K_(is_enabled));
}

int ObSSMemMacroCache::clear_mem_macro_cache()
{
  int ret = OB_SUCCESS;
  if (IS_INIT && !ATOMIC_LOAD(&is_stopped_) && ATOMIC_LOAD(&is_enabled_)) {
    if (ATOMIC_BCAS(&is_clear_, false, true)) {
      FLOG_INFO("start clear mem_macro_cache", K_(tenant_id));
      disable_cache();
      evict_task_.close_task();

      bool can_clear = false;
      const int64_t start_time_us = ObTimeUtil::current_time_us();
      do {
        const int64_t flying_req_cnt = ATOMIC_LOAD(&flying_req_cnt_);
        const bool is_task_closed = evict_task_.is_task_closed();
        if (flying_req_cnt > 0 || !is_task_closed) {
          ob_usleep(SS_MEM_LOOP_SLEEP_INTERVAL_US);
          if (TC_REACH_TIME_INTERVAL(SS_MEM_LOOP_PRINT_LOG_INTERVAL_US)) {
            FLOG_WARN("still exist request or task not closed", K(flying_req_cnt), K(is_task_closed));
          }
        } else {
          can_clear = true;
        }
      } while (!can_clear && (ObTimeUtility::current_time_us() - start_time_us < SS_CLEAR_MEM_MACRO_CACHE_TIMEOUT_US));

      if (OB_UNLIKELY(!can_clear)) {
        ret = OB_EAGAIN;
        FLOG_WARN("wait for flying_req_cnt or evict_task to finish timeout", KR(ret), K_(tenant_id), K_(cache_stat));
      } else {
        meta_mgr_.clear();
        inner_check_macro_meta_cnt();
        buf_mgr_.clear();
        cache_stat_.reuse();
      }

      evict_task_.open_task();
      enable_cache();
      ATOMIC_STORE(&is_clear_, false);
      FLOG_INFO("finish clear mem_macro_cache", K_(tenant_id), K(can_clear), K_(cache_stat));
    } else {
      ret = OB_EAGAIN;
      FLOG_INFO("already in clear mem_macro_cache process, please wait", K_(tenant_id));
    }
  } else {
    FLOG_INFO("no need to clear mem_macro_cache", K_(is_inited), K_(is_stopped), K_(is_enabled), KP(this));
  }
  return ret;
}

void ObSSMemMacroCache::update_memory_limit_size(const int64_t new_mem_limit_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(blk_size_ <= 0 || new_mem_limit_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_mem_limit_size), K_(blk_size));
  } else if (ATOMIC_LOAD(&mem_limit_size_) != new_mem_limit_size) {
    ATOMIC_STORE(&mem_limit_size_, new_mem_limit_size);
    const int64_t new_total_cnt = new_mem_limit_size / blk_size_;
    buf_mgr_.update_total_mem_block_cnt(new_total_cnt);
    FLOG_INFO("succ to update mem_macro_cache mem limit", K(new_mem_limit_size), K(new_total_cnt));
  }
}

int ObSSMemMacroCache::put(
    const MacroBlockId &macro_id,
    const uint64_t effective_tablet_id,
    const char *buf,
    const int64_t buf_size,
    const bool need_fast_persist)
{
  int ret = OB_SUCCESS;
  SSMemMacroCacheReqGuard macro_req_guard(flying_req_cnt_);
  bool already_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || (buf_size <= 0)) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(buf_size), KP(buf));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_mem_macro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SERVICE_STOPPED;
    if (TC_REACH_TIME_INTERVAL(SS_MEM_MACRO_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_mem_macro_cache is disabled", KR(ret), K_(is_enabled));
    }
  } else if (FALSE_IT(cache_stat_.io_stat().update_io_stat_info(false, 1, buf_size))) {
  } else if (need_fast_persist) {
    cache_stat_.macro_blk_stat().inc_fast_persist_macro_cnt();
  } 
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta_mgr_.exist_mem_macro_block_meta(macro_id, already_exist))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to check exist mem macro_block meta", KR(ret), K(macro_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (already_exist) {
    LOG_TRACE("already exist this macro_block in mem_macro_cache", K(macro_id));
  } 
  
  if (OB_SUCC(ret) && !already_exist) {
    ObSSMacroCacheMemBlkHandle mem_blk_handle;
    int32_t offset = -1;
    uint32_t crc = 0;
    if (OB_FAIL(buf_mgr_.add_macro_block_data(macro_id, buf, buf_size, mem_blk_handle, offset, crc))) {
      if (OB_EAGAIN == ret) {
        cache_stat_.hit_stat().update_fail_add_info(1, buf_size);
        LOG_INFO("not exist enough mem_block to save macro_block", KR(ret), K(macro_id), K(buf_size), K_(cache_stat));
      } else {
        LOG_WARN("fail to add macro_block data", KR(ret), K(macro_id), K(buf_size), KP(buf));
      }
    } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_blk_handle should be valid", KR(ret), K(mem_blk_handle), K(macro_id), K_(cache_stat));
    } else if (OB_FAIL(meta_mgr_.add_mem_macro_block_meta(macro_id, effective_tablet_id, mem_blk_handle,
              offset, buf_size, crc, need_fast_persist))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to add mem macro_block meta", KR(ret), K(macro_id), K(effective_tablet_id), K(mem_blk_handle),
          K(offset), K(buf_size));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      const int64_t new_heat_val = ObTimeUtility::current_time_us() / 1000;
      mem_blk_handle()->update_heat(new_heat_val);
      cache_stat_.hit_stat().update_new_add_info(1, buf_size);
    }
  }
  return ret;
}

int ObSSMemMacroCache::check_exist(
    const MacroBlockId &macro_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  SSMemMacroCacheReqGuard macro_req_guard(flying_req_cnt_);
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_mem_macro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    // mem_macro_cache is disabled, treat as not exist without error
    if (TC_REACH_TIME_INTERVAL(SS_MEM_MACRO_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_mem_macro_cache is disabled", KR(ret), K_(is_enabled));
    }
  } else {
    ObSSMacroCacheBlkMetaHandle macro_meta_handle;
    if (OB_FAIL(meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get mem macro_block meta", KR(ret), K(macro_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      is_exist = true;
    }
  }
  return ret;
}

int ObSSMemMacroCache::async_evict_macro_block(
    const MacroBlockId &macro_id,
    const bool need_persist)
{
  int ret = OB_SUCCESS;
  SSMemMacroCacheReqGuard macro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_mem_macro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_mem_macro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(meta_mgr_.mark_macro_block_fast_evict(macro_id, need_persist))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to mark macro_block fast evict", KR(ret), K(macro_id), K(need_persist));
    }
  } else {
    LOG_TRACE("succ to async evict macro_block", K(macro_id), K(need_persist));
  }
  return ret;
}

int ObSSMemMacroCache::get(
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    bool &is_hit_cache)
{
  int ret = OB_SUCCESS;
  SSMemMacroCacheReqGuard macro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_mem_macro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SERVICE_STOPPED;
    if (TC_REACH_TIME_INTERVAL(SS_MEM_MACRO_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_mem_macro_cache is disabled", KR(ret), K_(is_enabled));
    }
  } else if (FALSE_IT(cache_stat_.io_stat().update_io_stat_info(true, 1, io_info.size_))) {
  } else {
    is_hit_cache = false;
    ObSSMacroCacheBlkMetaHandle macro_meta_handle;
    int64_t macro_size = 0;
    if (OB_FAIL(meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get mem macro_block meta", KR(ret), K(macro_id));
      } else {
        ret = OB_SUCCESS;
        cache_stat_.hit_stat().update_miss_info(1, io_info.size_);
      }
    } else if (OB_UNLIKELY(!macro_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_meta handle should be valid", KR(ret), K(macro_meta_handle), K(macro_id));
    } else if (FALSE_IT(macro_size = macro_meta_handle()->get_size())) {
    } else if (OB_FAIL(macro_meta_handle()->check_crc())) {
      LOG_WARN("fail to check crc", KR(ret), KPC(macro_meta_handle.get_ptr()));
    // We need to use the 'read content' address, 'macro_meta_handle()->get_data()' is the macro_block entry address.
    } else if (OB_FAIL(handle_hit_macro_block(macro_id, (macro_meta_handle()->get_data() + io_info.offset_), 
               macro_size, io_info, obj_handle))) {
      LOG_WARN("fail to handle ht macro_block", KR(ret), K(macro_id), KPC(macro_meta_handle.get_ptr()));
    } else {
      cache_stat_.hit_stat().update_hit_stat(1, macro_size);
      macro_meta_handle()->update_access_info(io_info.offset_, macro_size);
      is_hit_cache = true;
    }
  }
  return ret;
}

int ObSSMemMacroCache::handle_hit_macro_block(
    const MacroBlockId &macro_id,
    const char *io_buf,
    const int64_t io_buf_size,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  const int64_t data_size = MIN(io_buf_size, io_info.size_);
  if (OB_ISNULL(io_buf) || OB_UNLIKELY(io_buf_size <= 0 || data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), KP(io_buf), K(io_buf_size), K(io_info));
  } else if (OB_FAIL(ObSSLocalCacheUtil::handle_hit_cache_in_memory(io_buf, data_size, io_info, object_handle))) {
    LOG_WARN("fail to handle hit cache in memory", KR(ret), K(io_info));
  } else if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

void ObSSMemMacroCache::inner_check_macro_meta_cnt()
{
  int ret = OB_SUCCESS;
  int64_t cur_alloc_cnt = cache_stat_.mem_usage_stat().macro_meta_cnt_;
  while (cur_alloc_cnt > 0) {
    ob_usleep(SS_MEM_LOOP_SLEEP_INTERVAL_US);
    if (TC_REACH_TIME_INTERVAL(SS_MEM_LOOP_PRINT_LOG_INTERVAL_US)) {
      FLOG_WARN("still exist unfree macro_meta", KR(ret), K(cur_alloc_cnt));
    }
    cur_alloc_cnt = cache_stat_.mem_usage_stat().macro_meta_cnt_;
  }

  if (OB_UNLIKELY(cur_alloc_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected allocated macro_meta count", KR(ret), K(cur_alloc_cnt), K_(cache_stat));
  }
}

} // namespace storage
} // namespace oceanbase
