/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_evict_task.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_buf_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_meta_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "share/io/ob_io_manager.h"
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMemMacroCacheEvictWriteEntry-----------------------------------------*/
ObSSMemMacroCacheEvictWriteEntry::ObSSMemMacroCacheEvictWriteEntry()
  : macro_id_(), write_handle_()
{}

void ObSSMemMacroCacheEvictWriteEntry::reset()
{
  macro_id_.reset();
  write_handle_.reset();
}

int ObSSMemMacroCacheEvictWriteEntry::init(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    write_handle_.reset();
    macro_id_ = macro_id;
  }
  return ret;
}

/*-----------------------------------------ObSSMemMacroCacheEvictCtx-----------------------------------------*/
ObSSMemMacroCacheEvictCtx::ObSSMemMacroCacheEvictCtx()
  : need_check_ttl_(false), start_us_(0), cost_us_(0), exp_evict_cnt_(0), evict_blk_cnt_(0),
    evict_macro_cnt_(0), write_back_macro_cnt_(0), prev_check_ttl_us_(ObTimeUtility::current_time_us()),
    prev_fast_evict_cnt_(0)
{}

void ObSSMemMacroCacheEvictCtx::reset()
{
  need_check_ttl_ = false;
  start_us_ = 0;
  cost_us_ = 0;
  exp_evict_cnt_ = 0;
  evict_blk_cnt_ = 0;
  evict_macro_cnt_ = 0;
  write_back_macro_cnt_ = 0;
  prev_check_ttl_us_ = 0;
  prev_fast_evict_cnt_ = 0;
}

void ObSSMemMacroCacheEvictCtx::start_eviction(
    const int64_t exp_evict_cnt,
    const int64_t cur_fast_evict_cnt)
{
  const int64_t ori_check_ttl_us = prev_check_ttl_us_;
  reset();
  start_us_ = ObTimeUtility::current_time_us();
  prev_check_ttl_us_ = ori_check_ttl_us;
  exp_evict_cnt_ = exp_evict_cnt;
  if (exp_evict_cnt_ <= 0) {
    if (start_us_ - prev_check_ttl_us_ >= SS_DEF_CHECK_MACRO_TTL_INTERVAL_US) {
      need_check_ttl_ = true;
    } else if (cur_fast_evict_cnt > prev_fast_evict_cnt_) {
      need_check_ttl_ = true;
      prev_fast_evict_cnt_ = cur_fast_evict_cnt;
    }
  }
}

void ObSSMemMacroCacheEvictCtx::finish_eviction()
{
  cost_us_ = ObTimeUtility::current_time_us() - start_us_;
}

/*-----------------------------------------ObSSMemMacroCacheEvictTask-----------------------------------------*/
ObSSMemMacroCacheEvictTask::ObSSMemMacroCacheEvictTask(ObSSMemMacroCacheBufMgr &buf_mgr, 
    ObSSMemMacroCacheMetaMgr &meta_mgr, ObSSMemMacroCacheStat &cache_stat)
  : is_inited_(false), need_close_(false), is_closed_(false), prev_fail_add_cnt_(0),
    prev_print_stat_us_(ObTimeUtility::current_time_us()), tenant_id_(OB_INVALID_TENANT_ID), 
    evict_ctx_(), total_entry_arr_(nullptr), free_entry_list_(), write_entry_list_(), 
    buf_mgr_(buf_mgr), meta_mgr_(meta_mgr), cache_stat_(cache_stat)
{}

int ObSSMemMacroCacheEvictTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(free_entry_list_.init(MAX_WRITE_MACRO_PARALLEM))) {
    LOG_WARN("fail to init free_entry_list", KR(ret));
  } else if (OB_FAIL(write_entry_list_.init(MAX_WRITE_MACRO_PARALLEM))) {
    LOG_WARN("fail to init write_entry_list", KR(ret));
  } else if (OB_FAIL(alloc_all_entries(tenant_id))) {
    LOG_WARN("fail to alloc all entries", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::alloc_all_entries(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t total_size = sizeof(ObSSMemMacroCacheEvictWriteEntry) * MAX_WRITE_MACRO_PARALLEM;
  ObMemAttr mem_attr(tenant_id, "MemMCEvictEntry");
  if (OB_ISNULL(total_entry_arr_ = static_cast<ObSSMemMacroCacheEvictWriteEntry *>(ob_malloc(total_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(total_size), K(mem_attr));
  } else {
    for (int64_t i = 0; i < MAX_WRITE_MACRO_PARALLEM; ++i) {
      new (total_entry_arr_ + i) ObSSMemMacroCacheEvictWriteEntry();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_WRITE_MACRO_PARALLEM; ++i) {
      ObSSMemMacroCacheEvictWriteEntry *cur_entry = total_entry_arr_ + i;
      if (OB_FAIL(free_entry_list_.push(cur_entry))) {
        LOG_WARN("fail to push entry into list", K(ret), K(i), K(free_entry_list_.get_curr_total()));
      }
    }
  }

  if (OB_FAIL(ret)) {
    free_all_entries();
  }
  return ret;
}

void ObSSMemMacroCacheEvictTask::free_all_entries()
{
  if (OB_NOT_NULL(total_entry_arr_)) {
    for (int64_t i = 0; i < MAX_WRITE_MACRO_PARALLEM; ++i) {
      total_entry_arr_[i].~ObSSMemMacroCacheEvictWriteEntry();
    }
    ob_free(total_entry_arr_);
    total_entry_arr_ = nullptr;
  }
}

void ObSSMemMacroCacheEvictTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (FALSE_IT(is_closed_ = need_close_)) {
  } else if (!is_closed_) {
    const int64_t exp_evict_cnt = calc_evict_mem_blk_cnt();
    const int64_t cur_fast_evict_cnt = cache_stat_.macro_blk_stat().get_fast_evict_macro_cnt();
    evict_ctx_.start_eviction(exp_evict_cnt, cur_fast_evict_cnt);
    if (evict_ctx_.need_eviction()) {
      if (OB_FAIL(execute_eviction())) {
        LOG_WARN("fail to execute eviction", KR(ret), K_(evict_ctx));
      }
    }
    handle_extra_func();
    LOG_TRACE("ss_mem_macro_cache: finish current eviction", KR(ret), K_(evict_ctx), K_(cache_stat));
  }
}

void ObSSMemMacroCacheEvictTask::destroy()
{
  free_entry_list_.destroy();
  write_entry_list_.destroy();
  free_all_entries();
  need_close_ = false;
  is_closed_ = false;
  prev_print_stat_us_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

int64_t ObSSMemMacroCacheEvictTask::calc_evict_mem_blk_cnt()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t evict_cnt = 0;
  const int64_t evict_trigger_cnt = cache_stat_.mem_blk_stat().total_blk_cnt_ * SS_DEF_TRIGGER_EVICT_USAGE_PCT / 100;
  const int64_t alloc_blk_cnt = cache_stat_.mem_blk_stat().alloc_blk_cnt_;
  if (alloc_blk_cnt > evict_trigger_cnt) {
    evict_cnt = MIN(alloc_blk_cnt - evict_trigger_cnt, SS_DEF_EVICT_MEM_BLOCK_CNT);
  }

  // Check tenant memory usage info, If tenant memory is not enough, we need to evict more mem blocks.
  int64_t tnt_total_mem_size = 0;
  int64_t tnt_used_mem_size = 0;
  if (OB_TMP_FAIL(ObSSLocalCacheUtil::get_tenant_memory_info(tenant_id_, tnt_total_mem_size, tnt_used_mem_size))) {
    LOG_WARN("fail to get tenant memory info", KR(tmp_ret), K_(tenant_id));
    tmp_ret = OB_SUCCESS;
  } else if (tnt_total_mem_size > 0 && tnt_used_mem_size > 0) {
    const int64_t TNT_MEM_USAGE_THRESHOLD_PCT = 99; // tenant memoro usage reaches 99%, we think it is a high level
    if (tnt_used_mem_size * 100 >= tnt_total_mem_size * TNT_MEM_USAGE_THRESHOLD_PCT) {
      int64_t new_evict_cnt = MAX(evict_cnt, MIN(alloc_blk_cnt / 2, SS_DEF_EVICT_MEM_BLOCK_CNT));
      if (new_evict_cnt > evict_cnt) {
        cache_stat_.mem_blk_stat().update_extra_evict_blk_cnt(new_evict_cnt - evict_cnt);
        evict_cnt = new_evict_cnt;
        LOG_INFO("tenant memory usage is too high, will evict more mem blocks", K(tnt_used_mem_size),
          K(tnt_total_mem_size), "extra_cnt", new_evict_cnt - evict_cnt);
      }
    }
  }
  return evict_cnt;
}

int ObSSMemMacroCacheEvictTask::can_persist_entry(const ObSSMacroCacheBlkMetaHandle &macro_meta_handle, bool &need_persist)
{
  int ret = OB_SUCCESS;
  bool is_cold_tablet = false;
  need_persist = false;
  if (OB_UNLIKELY(!macro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_meta_handle));
  } else if (OB_FAIL(ObSSObjectAccessUtil::check_if_cold_tablet(macro_meta_handle()->effective_tablet_id_, is_cold_tablet))) {
    LOG_WARN("fail to check if cold tablet", KR(ret), K(macro_meta_handle));
  } else {
    need_persist = ((!is_cold_tablet) && (prev_fail_add_cnt_ == cache_stat_.hit_stat().get_fail_add_cnt()) && (!is_macro_read_cache_disabled()));
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::execute_eviction()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!evict_ctx_.need_eviction())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(evict_ctx));
  } else {
    prev_fail_add_cnt_ = cache_stat_.hit_stat().get_fail_add_cnt();
    const int64_t start_us = ObTimeUtility::current_time_us();
    ObArray<ObSSMacroCacheMemBlkHandle> mem_blk_handles;
    if (evict_ctx_.need_normal_evict()) {
      if (OB_FAIL(mem_blk_handles.reserve(evict_ctx_.exp_evict_cnt_))) {
        LOG_WARN("fail to reserve mem_blk_handles", KR(ret), K(evict_ctx_.exp_evict_cnt_));
      } else if (OB_FAIL(buf_mgr_.acquire_cold_mem_blocks(evict_ctx_.exp_evict_cnt_, mem_blk_handles))) {
        LOG_WARN("fail to acquire cold mem_blocks", KR(ret), K_(evict_ctx), K(mem_blk_handles));
      }
    } else if (evict_ctx_.need_ttl_expire()) {
      if (OB_FAIL(meta_mgr_.acquire_ttl_mem_blocks(SS_DEF_EVICT_MEM_BLOCK_CNT, mem_blk_handles))) {
        LOG_WARN("fail to acquire ttl mem_blocks", KR(ret), K_(evict_ctx), K(mem_blk_handles));
      } else {
        evict_ctx_.update_check_ttl_time();
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(!mem_blk_handles.empty())) {
      const int64_t mem_blk_cnt = mem_blk_handles.count();
      ObArray<MacroBlockId> cold_macro_ids;
      if (OB_FAIL(cold_macro_ids.reserve(mem_blk_cnt))) {
        LOG_WARN("fail to reserve cold_macro_ids", KR(ret), K(mem_blk_cnt));
      } else if (OB_FAIL(meta_mgr_.get_macro_ids_by_mem_blk(mem_blk_handles, cold_macro_ids))) {
        LOG_WARN("fail to get macro_ids by mem_blk", KR(ret), K(mem_blk_cnt));
      } else if (!cold_macro_ids.empty()) {
        const int64_t cold_macro_cnt = cold_macro_ids.count();
        int64_t write_macro_cnt = 0;
        int64_t handle_idx = 0;
        const int64_t start_evict_us = ObTimeUtility::current_time_us();
        bool is_slow_evict = false;
        do {
          bool need_persist = false;
          if (free_entry_list_.get_curr_total() > 0 && handle_idx < cold_macro_cnt) {
            const MacroBlockId &macro_id = cold_macro_ids.at(handle_idx);
            ObSSMacroCacheBlkMetaHandle macro_meta_handle;
            if (OB_FAIL(meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
              LOG_WARN("fail to get mem_macro_cache macro_meta", KR(ret), K(handle_idx), K(cold_macro_cnt), K(macro_id));
            } else if (OB_UNLIKELY(!macro_meta_handle.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("macro_meta handle should be valid", KR(ret), K(macro_meta_handle));
            } else if (OB_FAIL(can_persist_entry(macro_meta_handle, need_persist))) {
              LOG_WARN("fail to check whether can persist entry", KR(ret), K(macro_meta_handle));
            } else if (need_persist && macro_meta_handle()->need_persist()) {
              if (OB_FAIL(do_async_write_entry(macro_id, macro_meta_handle))) {
                LOG_WARN("fail to async write entry", KR(ret), K(macro_id), K(macro_meta_handle), 
                  KPC(macro_meta_handle.get_ptr()));
              } else {
                ++write_macro_cnt;
              }
            } else if (OB_FAIL(meta_mgr_.erase_mem_macro_block_meta(macro_id))) {
              LOG_WARN("fail to erase macro_block meta", KR(ret), K(handle_idx), K(cold_macro_cnt), K(macro_id));
            } else {
              cache_stat_.macro_blk_stat().update_erased_macro_info(evict_ctx_.need_ttl_expire(), 1);
              evict_ctx_.inc_evict_macro_cnt();
            }

            ++handle_idx;
          } else {
            ob_usleep(SS_MEM_LOOP_SLEEP_INTERVAL_US);
            if (!is_slow_evict && ObTimeUtility::current_time_us() - start_evict_us >= SS_MEM_LOOP_PRINT_LOG_INTERVAL_US) {
              is_slow_evict = true;
              FLOG_WARN("eviction cost too much time", K(handle_idx), K(cold_macro_cnt), K(free_entry_list_.get_curr_total()),
                K(write_entry_list_.get_curr_total()));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(handle_completed_entry())) {
            LOG_WARN("fail to handle completed entry", KR(ret), K(free_entry_list_.get_curr_total()),
              K(write_entry_list_.get_curr_total()));
          }

          ret = OB_SUCCESS; // continue to handle next
        } while (handle_idx < cold_macro_cnt || write_entry_list_.get_curr_total() > 0);

        if (OB_SUCC(ret) && is_slow_evict) {
          FLOG_WARN("slow mem_macro_cache eviction", K(mem_blk_cnt), K(cold_macro_cnt), K(write_macro_cnt),
            "cost_us", ObTimeUtility::current_time_us() - start_evict_us);
        }
      }
      
      if (FAILEDx(buf_mgr_.free_cold_mem_blocks(mem_blk_handles, evict_ctx_.evict_blk_cnt_))) {
        LOG_WARN("fail to free cold mem_blocks", KR(ret), K(mem_blk_handles));
      } else {
        cache_stat_.mem_blk_stat().update_mem_block_erased_cnt(evict_ctx_.need_ttl_expire(), evict_ctx_.evict_blk_cnt_);
        evict_ctx_.finish_eviction();
        LOG_INFO("succ to evict mem_block in mem_macro_cache", K_(evict_ctx), K(mem_blk_handles.count()),
          K(cold_macro_ids.count()), K(cache_stat_.mem_blk_stat()));
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::do_async_write_entry(
    const MacroBlockId &macro_id,
    ObSSMacroCacheBlkMetaHandle &macro_meta_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !macro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(macro_meta_handle));
  } else {
    ObSSMemMacroCacheEvictWriteEntry *entry = nullptr;
    bool need_recycle = true;
    if (OB_FAIL(free_entry_list_.pop(entry))) {
      LOG_WARN("fail to pop from queue", KR(ret), K(free_entry_list_.get_curr_total()));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("entry is nullptr", KR(ret), KP(entry), K(free_entry_list_.get_curr_total()));
    } else if (OB_FAIL(entry->init(macro_id))) {
      LOG_WARN("fail to init entry", KR(ret), K(macro_id));
    } else if (OB_FAIL(do_write_into_macro_cache(macro_id, macro_meta_handle, entry->write_handle_))) {
      LOG_WARN("fail to write macro_block into local macro_cache", KR(ret), K(macro_id), K(macro_meta_handle),
        KPC(macro_meta_handle.get_ptr()));
    } 
    
    if (OB_FAIL(ret)) {
      tmp_ret = (evict_ctx_.need_ttl_expire() ? OB_SUCCESS : ret);
      if (OB_TMP_FAIL(tmp_ret)) {
      } else if (OB_TMP_FAIL(meta_mgr_.erase_mem_macro_block_meta(macro_id))) {
        LOG_WARN("fail to erase macro_block meta", KR(tmp_ret), KR(ret), KP(entry), K(macro_id));
        tmp_ret = OB_SUCCESS;
      } else {
        cache_stat_.macro_blk_stat().update_erased_macro_info(evict_ctx_.need_ttl_expire(), 1);
        evict_ctx_.inc_evict_macro_cnt();
      }
    } else if (OB_FAIL(write_entry_list_.push(entry))) {
      LOG_WARN("fai to push into queue", KR(ret), KPC(entry), K(free_entry_list_.get_curr_total()),
        K(write_entry_list_.get_curr_total()));
    } else {
      need_recycle = false;
    }

    if (need_recycle && nullptr != entry) {
      if (OB_TMP_FAIL(recycle_entry(*entry))) {
        LOG_WARN("fail to recycle entry", KR(tmp_ret), KPC(entry));
        tmp_ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::do_write_into_macro_cache(
    const MacroBlockId &macro_id,
    ObSSMacroCacheBlkMetaHandle &macro_meta_handle,
    ObStorageObjectHandle &obj_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || !macro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(macro_meta_handle));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = macro_meta_handle()->get_data();
    write_info.offset_ = 0;
    write_info.size_ = macro_meta_handle()->get_size();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    // Warning: must set unsealed, otherwise tmpfile_write_cache thread will evict segment file to object storage
    write_info.io_desc_.set_unsealed();
    write_info.set_is_write_cache(false); // preread macro is read cache
    write_info.set_effective_tablet_id(macro_meta_handle()->effective_tablet_id_);
    const bool use_effective_tablet_id = SSObjUtil::has_effective_tablet_id(macro_id);
    if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(use_effective_tablet_id,
                macro_id, write_info, obj_handle))) {
      LOG_WARN("fail to async write file to macro cache", KR(ret), K(macro_id), K(write_info));
    }
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::add_failed_macro_to_prewarm_queue(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheBlkMetaHandle macro_meta_handle;
  ObTenantFileManager *tnt_file_mgr = nullptr;
  
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_mgr_.get_mem_macro_block_meta(macro_id, macro_meta_handle))) {
    LOG_WARN("fail to get macro meta", KR(ret), K(macro_id));
  } else if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret));
  } else {
    const uint64_t effective_tablet_id = macro_meta_handle()->effective_tablet_id_;
    const int64_t preread_queue_size = tnt_file_mgr->get_preread_cache_mgr().get_preread_queue_size();
    
    if (preread_queue_size >= ObPrereadCacheManager::PREREAD_QUEUE_SIZE_THRESHOLD) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        LOG_WARN("preread queue is busy, skip prewam macro block", K(preread_queue_size), K(macro_id));
      }
    } else if (OB_FAIL(tnt_file_mgr->push_to_preread_queue(macro_id, ObTabletID(effective_tablet_id)))) {
      LOG_WARN("fail to push failed macro to preread queue", KR(ret), K(macro_id), K(effective_tablet_id));
    } else {
      LOG_TRACE("succ to push failed macro to preread queue", K(macro_id), K(effective_tablet_id), 
              K(preread_queue_size), K_(evict_ctx));
    }
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::handle_completed_entry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t write_cnt = write_entry_list_.get_curr_total();
  for (int64_t i = 0; i < write_cnt; ++i) {
    ObSSMemMacroCacheEvictWriteEntry *entry = nullptr;
    bool need_recycle = true;
    bool is_io_finished = false;
    if (OB_FAIL(write_entry_list_.pop(entry))) {
      LOG_WARN("fail to pop from queue", KR(ret), K(i), K(write_cnt), KP(entry));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entry is nullptr", KR(ret), KP(entry), K(i), K(write_cnt));
    } else if (OB_FAIL(entry->write_handle_.check_is_finished(is_io_finished))) {
      LOG_WARN("fail to check io finish", KR(ret), KPC(entry), K(i), K(write_cnt));
    } else if (is_io_finished) {
      const MacroBlockId &macro_id = entry->macro_id_;
      const int64_t macro_blk_size = entry->write_handle_.get_data_size();
      if (OB_FAIL(entry->write_handle_.wait())) {
        LOG_WARN("fail to wait handle", KR(ret), K(i), K(write_cnt), KPC(entry), K(macro_blk_size));
        cache_stat_.macro_blk_stat().inc_failed_write_back_macro_cnt();
        // If async write failed, try to push it into prewarm queue to prewarm again
        if (OB_TMP_FAIL(add_failed_macro_to_prewarm_queue(macro_id))) {
          LOG_WARN("fail to add failed macro to prewarm queue", KR(tmp_ret), K(macro_id));
          tmp_ret = OB_SUCCESS;
        } else {
          cache_stat_.macro_blk_stat().inc_retry_prewarm_macro_cnt();
        }
      } else if (FALSE_IT(cache_stat_.macro_blk_stat().update_write_back_macro_info(1, macro_blk_size))) {
      } else if (FALSE_IT(evict_ctx_.inc_write_back_macro_cnt())) {
      }
      
      tmp_ret = (evict_ctx_.need_ttl_expire() ? OB_SUCCESS : ret);
      if (OB_TMP_FAIL(tmp_ret)) {
      } else if (OB_TMP_FAIL(meta_mgr_.erase_mem_macro_block_meta(macro_id))) {
        LOG_WARN("fail to erase macro_block meta", KR(tmp_ret), K(i), K(write_cnt), KPC(entry), K(macro_blk_size));
        tmp_ret = OB_SUCCESS;
      } else {
        cache_stat_.macro_blk_stat().update_erased_macro_info(evict_ctx_.need_ttl_expire(), 1);
        evict_ctx_.inc_evict_macro_cnt();
      }
    } else {
      if (OB_FAIL(write_entry_list_.push(entry))) {
        LOG_ERROR("fail to push into queue, may leak!", KR(ret), KP(entry), K(i), K(write_cnt));
      } else {
        need_recycle = false;
      }
    }

    if (need_recycle && nullptr != entry) {
      if (OB_TMP_FAIL(recycle_entry(*entry))) {
        LOG_WARN("fail to recycle entry", KR(tmp_ret), KPC(entry));
        tmp_ret = OB_SUCCESS;
      }
    }

    ret = OB_SUCCESS; // continue to handle next
  }
  return ret;
}

int ObSSMemMacroCacheEvictTask::recycle_entry(ObSSMemMacroCacheEvictWriteEntry &entry)
{
  int ret = OB_SUCCESS;
  entry.reset();
  if (OB_FAIL(free_entry_list_.push(&entry))) {
    LOG_ERROR("fail to push into queue", KR(ret), K(entry), K(free_entry_list_.get_curr_total()));
  }
  return ret;
}

void ObSSMemMacroCacheEvictTask::handle_extra_func()
{
  int ret = OB_SUCCESS;
  if (ObTimeUtility::current_time_us() - prev_print_stat_us_ >= SS_PRINT_MEM_MACRO_CACHE_STAT_INTERVAL_US) {
    FLOG_INFO("ss_mem_macro_cache stat", K_(cache_stat));
    prev_print_stat_us_ = ObTimeUtility::current_time_us();
  }
}

} // namespace storage
} // namespace oceanbase
