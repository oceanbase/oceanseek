/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------SSTryLoadPersistedMicroMetaGuard------------------------------------------*/
SSTryLoadPersistedMicroMetaGuard::SSTryLoadPersistedMicroMetaGuard(int &ret, bool &all_mem_meta,
    ObSSMicroInitRangeInfo *init_range, ObSSMicroSubRangeInfo *sub_range)
  : ret_(ret), all_mem_meta_(all_mem_meta), init_range_(init_range), sub_range_(sub_range)
{
  ret_ = start_load_meta();
}

SSTryLoadPersistedMicroMetaGuard::~SSTryLoadPersistedMicroMetaGuard()
{
  IGNORE_RETURN finish_load_meta();
}

int SSTryLoadPersistedMicroMetaGuard::start_load_meta()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(init_range_) || OB_ISNULL(sub_range_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(init_range), KP_(sub_range));
  } else {
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    bool is_loading_meta = false;
    do {
      // If 'is_loading_meta' is true, means there exists other thread is loading this sub_range's persisted micro_meta
      // If 'all_mem_meta' is true, means this sub_range's persisted micro_meta was already read.
      if (OB_FAIL(init_range_->start_load_persisted_micro_meta(sub_range_, all_mem_meta_, is_loading_meta))) {
        LOG_WARN("fail to start load persisted micro_meta", KR(ret), KPC_(sub_range), KPC_(init_range));
      } else if (is_loading_meta && !all_mem_meta_) {
        ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
          FLOG_WARN("still wait for loading persisted micro_meta", K(is_loading_meta), K_(all_mem_meta), KPC_(sub_range));
        }
        if (ObTimeUtility::current_time_us() - start_time_us >= SS_LOAD_PERSISTED_MICRO_META_MAX_TIME_US) {
          ret = OB_TIMEOUT;
          LOG_WARN("cost too much time to load persisted micro_meta", KR(ret));
        }
      }
    } while (OB_SUCC(ret) && (is_loading_meta) & (!all_mem_meta_));
  }
  return ret;
}

int SSTryLoadPersistedMicroMetaGuard::finish_load_meta()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(init_range_) || OB_ISNULL(sub_range_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(init_range), KP_(sub_range));
  } else if (OB_FAIL(init_range_->finish_load_persisted_micro_meta(sub_range_, all_mem_meta_))) {
    LOG_WARN("fail to finish load persisted micro_meta", KR(ret), K_(all_mem_meta), KPC_(sub_range),
      KPC_(init_range));
  }
  return ret;
}

/*-----------------------------------------ObSSMicroMetaManager------------------------------------------*/
ObSSMicroMetaManager::ObSSMicroMetaManager(ObSSMicroCacheStat &cache_stat, ObConcurrentFIFOAllocator &allocator)
    : is_inited_(false),
      mem_limited_(false),
      enable_save_meta_mem_(false),
      block_size_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      micro_cnt_limit_(0),
      mem_limit_size_(0),
      expiration_time_s_(0),
      arc_limit_pct_(0),
      max_micro_blk_size_(0),
      micro_meta_map_(),
      bg_micro_iter_(micro_meta_map_),
      fg_micro_iter_(micro_meta_map_),
      arc_info_(),
      micro_range_mgr_(nullptr),
      cache_stat_(cache_stat),
      replay_ctx_(),
      allocator_(allocator)
{}

int ObSSMicroMetaManager::init(
    const uint64_t tenant_id,
    const int32_t block_size, 
    const int64_t cache_limit_size,
    ObSSMicroRangeManager &micro_range_mgr)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "SSMicroMap");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0 || cache_limit_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size), K(cache_limit_size));
  } else if (OB_UNLIKELY(!micro_range_mgr.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(micro_meta_map_.init(attr))) {
    LOG_WARN("fail to init macro meta map", KR(ret), K(attr));
  } else {
    tenant_id_ = tenant_id;
    enable_save_meta_mem_ = SS_ENABLE_SAVE_META_MEMORY;
    block_size_ = block_size;
    micro_range_mgr_ = &micro_range_mgr;
    if (OB_FAIL(inner_update_cache_expiration_time(tenant_id))) {
      LOG_WARN("fail to inner update cache expiration_time", KR(ret), K(tenant_id));
    } else if (OB_FAIL(inner_update_mem_limit_size(tenant_id))) {
      LOG_WARN("fail to inner update mem_limit", KR(ret), K(tenant_id));
    } else if (OB_FAIL(init_arc_info(tenant_id, cache_limit_size, ATOMIC_LOAD(&micro_cnt_limit_)))) {
      LOG_WARN("fail to init arc_info", KR(ret), K(tenant_id), K(cache_limit_size));
    } else if (OB_FAIL(inner_update_max_micro_blk_size(tenant_id))) {
      LOG_WARN("fail to inner update max_micro_blk_size", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tablets_to_delete_.create(TABLETS_DELETE_BUCKET_NUM, ObMemAttr(tenant_id, "PendDelTabMap")))) {
      LOG_WARN("fail to create tablets_to_delete map", KR(ret), K(tenant_id));
    } else {
      is_inited_ = true;
      LOG_INFO("succ to init micro_meta_mgr", K_(micro_cnt_limit), K_(mem_limit_size), 
        K_(expiration_time_s), K_(arc_limit_pct), K_(max_micro_blk_size));
    }
  }
  return ret;
}

void ObSSMicroMetaManager::update_cache_expiration_time_by_config()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_cache_expiration_time(tenant_id_))) {
    LOG_WARN("fail to inner update cache expiration_time", KR(ret), K_(tenant_id));
  }
}

int ObSSMicroMetaManager::inner_update_cache_expiration_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t cur_expiration_time = ObSSMicroCacheUtil::calc_micro_cache_expiration_time(tenant_id_);
    if (cur_expiration_time != ATOMIC_LOAD(&expiration_time_s_)) {
      ATOMIC_STORE(&expiration_time_s_, cur_expiration_time);
      FLOG_INFO("succ to update ss_micro_cache expiration time", K_(expiration_time_s));
    }
  }
  return ret;
}

void ObSSMicroMetaManager::update_cache_mem_limit_by_config()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_mem_limit_size(tenant_id_))) {
    LOG_WARN("fail to inner update cache mem_limit size", KR(ret), K_(tenant_id));
  }
}

int ObSSMicroMetaManager::inner_update_mem_limit_size(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t cur_mem_limit_size = ObSSMicroCacheUtil::calc_micro_cache_mem_limit_size(tenant_id);
    if (cur_mem_limit_size != ATOMIC_LOAD(&mem_limit_size_)) {
      const int64_t cur_micro_cnt_limit = ObSSMicroCacheUtil::calc_micro_cnt_limit(tenant_id, cur_mem_limit_size);
      ATOMIC_STORE(&mem_limit_size_, cur_mem_limit_size);
      ATOMIC_STORE(&micro_cnt_limit_, cur_micro_cnt_limit);
      cache_stat_.mem_stat().set_micro_cnt_limit(cur_micro_cnt_limit);
      arc_info_.set_micro_cnt_limit(cur_micro_cnt_limit);
      SSMicroMetaAlloc.set_total_limit(cur_mem_limit_size);
      check_mem_limit();
      FLOG_INFO("succ to update ss_micro_cache mem_limit", K_(mem_limit_size), K_(micro_cnt_limit));
    }
  }
  return ret;
}

bool ObSSMicroMetaManager::check_mem_limit()
{
  bool reach_limit = false;
  const int64_t cur_micro_cnt = cache_stat_.micro_stat().get_total_micro_cnt();
  if (OB_UNLIKELY(cur_micro_cnt >= ATOMIC_LOAD(&micro_cnt_limit_))) {
    set_mem_limited(true);
    reach_limit = true;
  } else {
    set_mem_limited(false);
  }
  return reach_limit;
}

int ObSSMicroMetaManager::init_arc_info(
    const uint64_t tenant_id,
    const int64_t cache_limit_size,
    const int64_t micro_cnt_limit)
{
  int ret = OB_SUCCESS;
  bool succ_update = false;
  if (OB_UNLIKELY(cache_limit_size <= 0 || micro_cnt_limit <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_limit_size), K(micro_cnt_limit), K(tenant_id));
  } else if (OB_FAIL(inner_update_arc_limit_pct(tenant_id, succ_update))) {
    LOG_WARN("fail to inner update arc_limit pct", KR(ret), K(tenant_id));
  } else if (OB_FAIL(inner_update_arc_limit(cache_limit_size))) {
    LOG_WARN("fail to inner update arc_limit", KR(ret), K(cache_limit_size));
  } else {
    arc_info_.set_micro_cnt_limit(micro_cnt_limit);
  }
  return ret;
}

void ObSSMicroMetaManager::update_arc_limit_pct_by_config(bool &succ_update)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_arc_limit_pct(tenant_id_, succ_update))) {
    LOG_WARN("fail to inner update arc_limit pct", KR(ret), K_(tenant_id));
  } else if (succ_update) {
    FLOG_INFO("succ to update ss_micro_cache arc_limit pct", K_(arc_limit_pct));
  }
}

void ObSSMicroMetaManager::update_max_micro_blk_size_by_config()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_max_micro_blk_size(tenant_id_))) {
    LOG_WARN("fail to inner update max_micro_blk_size", KR(ret), K_(tenant_id));
  }
}

int ObSSMicroMetaManager::inner_update_arc_limit_pct(const uint64_t tenant_id, bool &succ_update)
{
  int ret = OB_SUCCESS;
  succ_update = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t cur_arc_limit_pct = ObSSMicroCacheUtil::calc_micro_cache_arc_limit_pct(tenant_id);
    if (OB_UNLIKELY(cur_arc_limit_pct < SS_MIN_ARC_LIMIT_PCT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arc_limit_pct is unexpected", KR(ret), K(cur_arc_limit_pct));
    } else if (cur_arc_limit_pct != ATOMIC_LOAD(&arc_limit_pct_)) {
      succ_update = true;
      ATOMIC_STORE(&arc_limit_pct_, cur_arc_limit_pct);
    }
  }
  return ret;
}

int ObSSMicroMetaManager::inner_update_arc_limit(const int64_t cache_limit_size)
{
  int ret = OB_SUCCESS;
  const int64_t arc_limit_pct = ATOMIC_LOAD(&arc_limit_pct_);
  const int64_t arc_limit = cache_limit_size * arc_limit_pct / 100;
  if (OB_UNLIKELY(arc_limit < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arc_limit is unexpected", KR(ret), K(cache_limit_size), K(arc_limit_pct));
  } else {
    arc_info_.update_arc_limit(arc_limit);
  }
  return ret;
}

int ObSSMicroMetaManager::inner_update_max_micro_blk_size(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t MAX_RETRY_TIMES = 3;
    bool is_updated = false;
    int64_t retry_cnt = 0;
    const int64_t target_value = ObSSMicroCacheUtil::calc_max_micro_blk_size(tenant_id);
    while (!is_updated && (retry_cnt < MAX_RETRY_TIMES)) {
      int64_t current_max_size = ATOMIC_LOAD(&max_micro_blk_size_);
      if (OB_LIKELY(current_max_size == target_value)) {
        is_updated = true;
      } else if (ATOMIC_BCAS(&max_micro_blk_size_, current_max_size, target_value)) {
        FLOG_INFO("succ to update ss_micro_cache max_micro_blk_size",
            K_(max_micro_blk_size), "original max_micro_blk_size", current_max_size);
        is_updated = true;
      } else {
        ob_usleep(100 * 1000);
      }
      ++retry_cnt;
    }
  }
  return ret;
}

void ObSSMicroMetaManager::destroy()
{
  micro_meta_map_.destroy();
  cache_stat_.mem_stat().set_micro_map_usage(0);
  arc_info_.reset();
  tablets_to_delete_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  block_size_ = 0;
  mem_limit_size_ = 0;
  expiration_time_s_ = 0;
  arc_limit_pct_ = 0;
  max_micro_blk_size_ = 0;
  micro_cnt_limit_ = 0;
  mem_limited_ = false;
  is_inited_ = false;
}

void ObSSMicroMetaManager::clear()
{
  micro_meta_map_.clear();
  arc_info_.reuse();
  FLOG_INFO("ss_micro_cache: clear micro_meta manager", K_(arc_info));
}

int ObSSMicroMetaManager::update_arc_limit(const int64_t new_cache_limit_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_cache_limit_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_cache_limit_size));
  } else if (OB_FAIL(inner_update_arc_limit(new_cache_limit_size))) {
    LOG_WARN("fail to inner update arc_limit", KR(ret), K(new_cache_limit_size));
  }
  return ret;
}

int ObSSMicroMetaManager::update_arc_work_limit_for_prewarm(const bool start_update)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (start_update) {
    arc_info_.dec_arc_work_limit_for_prewarm();
  } else {
    arc_info_.inc_arc_work_limit_for_prewarm();
  }
  return ret;
}

int ObSSMicroMetaManager::alloc_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_meta_handle should not be valid", KR(ret), K(micro_meta_handle));
  } else if (check_mem_limit()) {
    ret = OB_SS_CACHE_REACH_MEM_LIMIT;
    LOG_WARN("ss_micro_cache memory has reached limit", KR(ret), K_(micro_cnt_limit), K(cache_stat_.micro_stat()));
  }

  if (OB_SUCC(ret)) {
    void *ptr = SSMicroMetaAlloc.alloc();
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc micro_meta", KR(ret), K_(micro_cnt_limit), K(cache_stat_.micro_stat()));
    } else {
      ObSSMicroBlockMeta *micro_meta = new(ptr) ObSSMicroBlockMeta();
      micro_meta->reset();
      micro_meta_handle.set_ptr(micro_meta);
      const int64_t micro_meta_usage = SSMicroMetaAlloc.hold();
      SSMicroCacheStat.mem_stat().update_micro_mem_usage(1, micro_meta_usage);
    }
  }
  return ret;
}

int ObSSMicroMetaManager::add_or_update_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t micro_size, 
    const uint32_t micro_crc, 
    const uint64_t effective_tablet_id,
    ObSSMemBlockHandle &mem_blk_handle, 
    bool &is_first_add,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_size), K(mem_blk_handle));
  } else {
    if (OB_FAIL(do_add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
               effective_tablet_id, mem_blk_handle, is_first_add, access_type))) {
      LOG_WARN("fail to add_or_update meta", KR(ret), K(micro_key), K(micro_size), K(micro_crc),
               K(effective_tablet_id), K(mem_blk_handle), K(access_type));
    }

    // A micro_block in mem_block is marked as complete only when:
    // 1. micro data is written into mem_block
    // 2. micro meta is inserted into micro_meta_map
    if (OB_LIKELY(mem_blk_handle()->is_valid())) {
      mem_blk_handle()->mark_micro_complete();
    }
  }

  return ret;
}

int ObSSMicroMetaManager::do_add_or_update_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t micro_size,
    const uint32_t micro_crc, 
    const uint64_t effective_tablet_id,
    ObSSMemBlockHandle &mem_blk_handle,
    bool &is_first_add,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t cur_tablet_id = ((ObTabletID::INVALID_TABLET_ID == effective_tablet_id)
                               ? micro_key.get_macro_tablet_id().id()
                               : effective_tablet_id);
  if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_size), K(mem_blk_handle));
  } else {
    is_first_add = false;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    const uint64_t access_type_val = static_cast<uint64_t>(access_type);

    // 1. alloc and init micro_meta
    if (OB_FAIL(alloc_micro_block_meta(micro_meta_handle))) {
      LOG_WARN("fail to alloc micro_meta", KR(ret));
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("micro_meta_handle should be valid", KR(ret), K(micro_meta_handle));
    } else {
      // when micro_meta_handle destroy, it wll free relative momery source
      if (OB_FAIL(micro_meta_handle()->init(micro_key, mem_blk_handle, micro_size, micro_crc, cur_tablet_id))) {
        LOG_WARN("fail to init micro_meta", KR(ret), K(micro_key), K(micro_size), K(micro_crc), K(cur_tablet_id),
          K(mem_blk_handle));
      } else {
        micro_meta_handle()->update_access_type(access_type_val);
      }
    }
    
    // 2. insert or update meta
    if (OB_SUCC(ret)) {
      bool is_in_l1 = true;
      bool is_in_ghost = false;
      ObSSMicroArcOpType arc_op_type = ObSSMicroArcOpType::SS_NEW_INSERT;

      SSMicroMapUpdateMicroFunc update_func(micro_size, mem_blk_handle, micro_crc, cur_tablet_id, access_type_val);
      // NOTICE: don't use param @micro_key in 'insert_or_update'
      const ObSSMicroBlockCacheKey &inner_micro_key = micro_meta_handle()->get_micro_key();
      // if micro_key exists, execute 'update_func'; otherwise execute 'insert'
      if (OB_FAIL(micro_meta_map_.insert_or_operate(&inner_micro_key, micro_meta_handle, update_func))) {
        LOG_WARN("fail to add_or_update meta", KR(ret), K_(update_func.ret), K(inner_micro_key), K(micro_size), 
                                               K(micro_crc), K(mem_blk_handle));
        ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
      } else {
        if (update_func.is_deleted_micro_) {
          cache_stat_.micro_stat().update_mark_del_micro_info(update_func.is_in_ghost_, -1, micro_size * -1);
        }

        if (update_func.is_updated_) {
          arc_op_type = update_func.arc_op_type_;
          if (ObSSMicroArcOpType::SS_NEW_INSERT == arc_op_type) {
            is_in_l1 = true;
            is_in_ghost = false;
          } else {
            is_in_l1 = update_func.is_in_l1_;
            is_in_ghost = update_func.is_in_ghost_;
          }
        } else {
          cache_stat_.micro_stat().update_micro_map_total_info(1, micro_size);
          cache_stat_.micro_stat().set_max_micro_meta_cnt(micro_meta_map_.count());
          cache_stat_.mem_stat().update_micro_map_usage(SS_MICRO_META_MAP_ITEM_SIZE);
          // if micro_key not exist, means insert micro_block meta into micro_cache firstly, 
          // thus we need to link micro_meta in micro hash range.
          // if we fail to insert micro_meta into range, it doesn't matter, just means this
          // micro_meta can't participate in 'persist_micro_meta'.
          if (OB_TMP_FAIL(micro_range_mgr_->insert_micro_meta_into_range(micro_meta_handle))) {
            LOG_ERROR("fail to insert micro_meta into range", KR(tmp_ret), K(micro_key));
            tmp_ret = OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret)) {
          // if micro_key not exist、micro_meta is 'deleted' or micro_meta is 'invalid', will cuz cache miss,
          // thus we need to add micro_block data into micro_cache firstly.
          if (!update_func.is_updated_ || update_func.is_deleted_micro_ || update_func.is_invalid_micro_) {
            is_first_add = true;
            mem_blk_handle()->update_valid_micro_info(micro_size);
          }

          // TODO by shiyuhui.syh: update arc_info after insert micro_meta into sub_range, same as link_meta_and_update_arc logic
          if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, arc_op_type, micro_size, 1))) {
            LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_key), K(micro_size));
          }
        }
      }
    }
  }

  return ret;
}

int ObSSMicroMetaManager::do_add_persisted_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta_handle));
  // TODO @donglou.zl now, only support ghost micro_meta
  } else if (OB_LIKELY(micro_meta_handle()->is_in_ghost())) { 
    const int32_t micro_size = micro_meta_handle()->size();
    const ObSSMicroBlockCacheKey &micro_key = micro_meta_handle()->get_micro_key();
    if (OB_FAIL(micro_meta_map_.insert(&micro_key, micro_meta_handle))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_INFO("micro_meta already exist, no need to insert", KR(ret), K(micro_key), KPC(micro_meta_handle.get_ptr()));
      } else {
        LOG_WARN("fail to insert micro_map", KR(ret), K(micro_key), KPC(micro_meta_handle.get_ptr()));
      }
    } else {
      cache_stat_.micro_stat().update_micro_map_total_info(1, micro_size);
      cache_stat_.mem_stat().update_micro_map_usage(SS_MICRO_META_MAP_ITEM_SIZE);
    }

    // if we fail to insert micro_meta into range, it doesn't matter, just means this
    // micro_meta can't participate in 'persist_micro_meta'.
    if (OB_FAIL(ret)) {
    } else if (OB_TMP_FAIL(micro_range_mgr_->insert_micro_meta_into_range(micro_meta_handle))) {
      LOG_ERROR("fail to insert micro_meta into range", KR(tmp_ret), K(micro_key));
      tmp_ret = OB_SUCCESS;
    } 
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(micro_meta_handle.get_ptr()));
  }
  return ret;
}

int ObSSMicroMetaManager::update_persisted_micro_block_meta(
    ObSSMemBlockHandle &sealed_mem_blk_handle,
    const int64_t block_offset,
    const uint32_t reuse_version, 
    int64_t &updated_micro_size,
    int64_t &updated_micro_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(block_offset < 0 || !sealed_mem_blk_handle.is_valid() || !sealed_mem_blk_handle()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_offset), KPC(sealed_mem_blk_handle.get_ptr()));
  } else {
    updated_micro_size = 0;
    updated_micro_cnt = 0;
    
    const char *mem_blk_buf = sealed_mem_blk_handle()->mem_blk_buf_;
    const int64_t blk_valid_len = sealed_mem_blk_handle()->valid_val_;
    const int64_t valid_micro_cnt = sealed_mem_blk_handle()->valid_cnt_;
    const int64_t micro_cnt = sealed_mem_blk_handle()->micro_cnt_;
    const int64_t mem_blk_size = sealed_mem_blk_handle()->mem_blk_size_;
    const int64_t payload_offset = sealed_mem_blk_handle()->reserved_size_;
    const int64_t total_data_size = sealed_mem_blk_handle()->total_data_size_;
    const int64_t data_end_offset = payload_offset + total_data_size;
    int64_t cur_data_end_pos = data_end_offset;
    int64_t pos = data_end_offset;

    for (int64_t i = 0; OB_SUCC(ret) && i < micro_cnt; ++i) {
      ObSSMicroBlockIndex micro_index;
      if (OB_FAIL(micro_index.deserialize(mem_blk_buf, mem_blk_size, pos))) {
        LOG_WARN("fail to deserialize micro index", KR(ret), KPC(sealed_mem_blk_handle.get_ptr()));
      } else {
        const int32_t size = micro_index.get_size();
        const int64_t data_dest = block_offset + (cur_data_end_pos - size);
        SSMicroMapUpdateMicroDestFunc update_func(data_dest, reuse_version, sealed_mem_blk_handle);
        if (OB_FAIL(micro_meta_map_.operate(&(micro_index.micro_key_), update_func))) {
          ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
          // 1. memory usage of micro_meta may reach upper_limit, so this micro_meta fail to insert into meta_map.
          // 2. this micro_meta may be force deleted
          if (OB_ENTRY_NOT_EXIST == ret) { 
            LOG_WARN("micro_meta not exist", KR(ret), K(i), K(micro_index), K(valid_micro_cnt), K(micro_cnt));
          } else {
            LOG_WARN("fail to update data_dest", KR(ret), K(i), K(micro_index), K(valid_micro_cnt), K(micro_cnt));
          }
          ret = OB_SUCCESS; // If fail to update data_dest, continue to handle next one.
        } else if (OB_LIKELY(update_func.succ_update_)) {
          updated_micro_size += size;
          ++updated_micro_cnt;
        }
        cur_data_end_pos -= size;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(payload_offset != cur_data_end_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("cur_data_end_pos is abnormal", KR(ret), K(payload_offset), K(cur_data_end_pos));
      } else if (updated_micro_cnt == valid_micro_cnt) {
        if (OB_UNLIKELY(updated_micro_size != blk_valid_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("updated_micro_size is unexpected", KR(ret), K(updated_micro_cnt), K(valid_micro_cnt), 
                    K(updated_micro_size), K(blk_valid_len), K(micro_cnt), KPC(sealed_mem_blk_handle.get_ptr()));
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::get_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    const uint64_t effective_tablet_id,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_meta_handle));
  } else {
    ObSSMemBlockHandle mem_blk_handle;
    ObSSPhyBlockHandle phy_blk_handle;
    ObSSMicroBlockMetaInfo micro_info;
    ObSSMicroCacheAccessInfo access_info;
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_blk_handle, phy_blk_handle, update_arc, access_info);
    // When add micro_meta, we need to get micro_meta first, will invoke this func and update_arc=true
    const bool update_tablet_id = update_arc;
    if (OB_FAIL(inner_get_micro_block_meta(micro_key, micro_info, get_func, update_tablet_id, effective_tablet_id))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to inner get micro_block_meta", KR(ret), K(micro_key), K(get_func));
      }
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid() || (micro_info.size() < 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta should be valid", KR(ret), K(micro_info), K(micro_meta_handle), K(get_func));
    } else if (update_arc) {
      // Only update_arc && is_valid, we will need to update prewarm get_info
      if (access_info.is_valid()) {
        cache_stat_.prewarm_stat().update_get_info(access_info);
      }

      const bool is_in_l1 = micro_info.is_in_l1_;
      const bool is_in_ghost = micro_info.is_in_ghost_;
      const int32_t micro_size = micro_info.size_;
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, get_func.arc_op_type_, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key), K(get_func));
      }
    }
  }

  if (OB_FAIL(ret)) {
    micro_meta_handle.reset();
  }
  return ret;
}


int ObSSMicroMetaManager::get_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBlockMetaInfo &micro_info,
    ObSSMicroBlockMetaHandle &micro_meta_handle, 
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhyBlockHandle &phy_blk_handle,
    const uint64_t effective_tablet_id,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    ObSSMicroCacheAccessInfo access_info;
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_blk_handle, phy_blk_handle, update_arc, access_info);
    const bool update_tablet_id = true;
    if (OB_FAIL(inner_get_micro_block_meta(micro_key, micro_info, get_func, update_tablet_id, effective_tablet_id))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to inner get micro_block_meta", KR(ret), K(micro_key), K(get_func));
      }
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid() || (micro_info.size_ <= 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta should be valid", KR(ret), K(micro_info), K(micro_meta_handle), K(get_func));
    } else if (update_arc) {
      // Only update_arc && is_valid, we will need to update prewarm get_info
      if (access_info.is_valid()) {
        cache_stat_.prewarm_stat().update_get_info(access_info);
      }
      
      const bool is_in_l1 = micro_info.is_in_l1_;
      const bool is_in_ghost = micro_info.is_in_ghost_;
      const int32_t micro_size = micro_info.size_;
      // Ghost micro must be invalid, directly return OB_ENTRY_NOT_EXIST
      // If micro_block is in T2, no need to adjust, cuz just need to update access_time.
      // So just need to adjust when it is in T1.
      if (is_in_l1 && !is_in_ghost) {
        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, ObSSMicroArcOpType::SS_ARC_HIT_T1, micro_size, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(get_func), KPC(micro_meta_handle.get_ptr()));
        }
      }
    }

    if (OB_FAIL(ret)) {
      micro_meta_handle.reset();
      mem_blk_handle.reset();
      phy_blk_handle.reset();
    }
  }
  return ret;
}

int ObSSMicroMetaManager::inner_get_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBlockMetaInfo &micro_info,
    SSMicroMapGetMicroHandleFunc &get_func,
    const bool need_update_tablet_id,
    const uint64_t effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (need_update_tablet_id) {
    get_func.set_effective_tablet_id(effective_tablet_id);
  }

  // For prewarm & reorgan, this value is false. Thus we don't need to read persisted micro_meta
  const bool update_arc = get_func.update_arc_;
  if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
    ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get micro_block location", KR(ret), K(micro_key), K(get_func));
    // If there not exist micro_meta in micro_meta_map, we need to try get it from phy_block.
    } else if (!get_func.exist_meta_ && enable_save_meta_mem_ && update_arc && 
               (cache_stat_.phy_blk_stat().meta_blk_used_cnt_ > 0)) {
      // NOTICE: now, we only load persisted ghost micro_meta, thus no need to get it again.
      //         if there exists any bug, we treat it as entry_not_exist
      ret = OB_SUCCESS;
      if (OB_FAIL(try_load_persisted_micro_meta(micro_key, get_func, micro_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to try load persisted micro_meta", KR(ret), K(micro_key), K(get_func));
          ret = OB_ENTRY_NOT_EXIST; // TODO @donglou.zl can improve later
        }
      } else if (!get_func.micro_meta_handle_.is_valid() || !micro_info.is_valid_field()) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  } else {
    micro_info = get_func.micro_info_;
  }
  return ret;
}

int ObSSMicroMetaManager::inner_get_micro_block_meta_directly(
    const ObSSMicroBlockCacheKey &micro_key,
    SSMicroMapGetMicroHandleFunc &get_func,
    ObSSMicroBlockMetaInfo &micro_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
    ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get micro_block location", KR(ret), K(micro_key), K(get_func));
    }
  } else {
    micro_info = get_func.micro_info_;
  }
  return ret;
}

int ObSSMicroMetaManager::get_micro_meta_info(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaInfo &micro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    SSMicroMapGetMicroInfoFunc get_func(micro_info);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
      ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_meta info", KR(ret), K(micro_key));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::check_micro_key_exist(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    ObSSMicroBlockMetaHandle tmp_handle;
    if (OB_FAIL(micro_meta_map_.get(&micro_key, tmp_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to check micro key exist in micro_meta map", KR(ret), K(micro_key));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::get_micro_meta_info_for_serialize(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroRangeScanFilter &filter,
    ObSSMicroBlockMetaInfo &micro_info,
    bool &need_serialize)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    SSMicroMapGetMicroInfoForSerializeFunc get_func(filter, micro_info);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
      ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_meta info", KR(ret), K(micro_key));
      }
    } else {
      need_serialize = get_func.need_serialize_;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::update_micro_block_meta_heat(
    const ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
    const bool transfer_seg,
    const bool update_access_time,
    const int64_t time_delta_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(micro_keys.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_keys));
  } else {
    SSMicroMapUpdateMicroHeatFunc update_func(transfer_seg, update_access_time, time_delta_s);
    const int64_t total_micro_cnt = micro_keys.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < total_micro_cnt); ++i) {
      const ObSSMicroBlockCacheKey &micro_key = micro_keys.at(i);
      if (OB_UNLIKELY(!micro_key.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(i), K(total_micro_cnt), K(micro_key));
      } else if (OB_FAIL(micro_meta_map_.operate(&micro_key, update_func))) {
        ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
        if (OB_ENTRY_NOT_EXIST == ret) {
          // ignore error code, continue
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to update micro block meta heat", KR(ret), K(micro_key), K(transfer_seg), K(update_access_time),
                   K(time_delta_s));
        }
      } else if (update_func.arc_op_type_ == ObSSMicroArcOpType::SS_ARC_HIT_T1) {
        if (OB_FAIL(inner_adjust_arc_seg_info(true, false, ObSSMicroArcOpType::SS_ARC_HIT_T1, update_func.size_, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(i), K(micro_key), "size", update_func.size_);
        }
      }
      // ignore ret, continue to handle next one
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::acquire_cold_micro_blocks(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx), K(arc_iter_info));
  } else if (arc_iter_info.need_iterate_cold_micro(seg_idx)) {
    bg_micro_iter_.rewind_randomly();
    const int64_t op_cnt = arc_iter_info.get_op_micro_cnt(seg_idx);
    const int64_t exp_iter_cnt = arc_iter_info.get_expected_iter_micro_cnt(seg_idx);
    const int64_t micro_map_cnt = micro_meta_map_.count();
    const int64_t total_round = MIN(exp_iter_cnt + SS_ARC_HANDLE_OP_MAX_COUNT, micro_map_cnt);
    // If micro_map has few meta, we need to iterate the entire map to prevent missing some meta.
    const int64_t early_exit_round = MAX(total_round / 2, micro_map_cnt);
    int64_t round = 0;
    while (OB_SUCC(ret) && (round < total_round) && (arc_iter_info.need_iterate_cold_micro(seg_idx))) {
      const ObSSMicroBlockCacheKey *cur_micro_key = nullptr;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ObSSMicroBlockMetaInfo cur_micro_info;
      if (OB_FAIL(bg_micro_iter_.next(cur_micro_key, micro_meta_handle))) {
        if (OB_ITER_END == ret) {
          bg_micro_iter_.rewind();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to iter next micro_block meta", KR(ret), K(cur_micro_key));
        }
      } else if (OB_FALSE_IT(++round)) {
      } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta handle should be valid", KR(ret), K(micro_meta_handle));
      } else if (OB_FAIL(get_micro_meta_info(*cur_micro_key, cur_micro_info))) {
        LOG_WARN("fail to get micro_meta info", KR(ret), KPC(cur_micro_key));
      } else {
        const bool can_choose = ((seg_idx == SS_ARC_T1 || seg_idx == SS_ARC_T2)
                                 ? cur_micro_info.can_evict() : cur_micro_info.can_delete());
        if (can_choose) {
          const bool is_in_l1 = cur_micro_info.is_in_l1();
          const bool is_in_ghost = cur_micro_info.is_in_ghost();
          const int64_t cur_seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
          if (cur_seg_idx == seg_idx) {
            bool is_choosen = false;
            // avoid to choose the same micro_block twice.
            if (OB_FAIL(arc_iter_info.exist_cold_micro(*cur_micro_key, is_choosen))) {
              LOG_WARN("fail to check cold_micro exist", KR(ret), KPC(cur_micro_key), K(seg_idx), K(arc_iter_info));
            } else if (!is_choosen) {
              if (OB_FAIL(arc_iter_info.push_cold_micro(*cur_micro_key, cur_micro_info, seg_idx))) {
                LOG_WARN("fail to push", KR(ret), K(seg_idx), KPC(cur_micro_key), K(cur_micro_info));
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if ((round == early_exit_round) && (arc_iter_info.get_obtained_micro_cnt(seg_idx) < op_cnt)) {
          LOG_TRACE("it's hard to iterate cold micro in this seg", K(round), K(total_round), K(seg_idx), K(arc_iter_info));
          break;
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_evict_micro_block_meta(const ObSSMicroBlockMetaInfo &cold_micro_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    const bool is_in_l1 = cold_micro_info.is_in_l1();
    const bool is_in_ghost = cold_micro_info.is_in_ghost();
    const int64_t phy_blk_idx = cold_micro_info.data_loc() / block_size_;
    const ObSSMicroBlockCacheKey &micro_key = cold_micro_info.get_micro_key();
    SSMicroMapEvictMicroFunc evict_func(cold_micro_info);
    
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, evict_func))) {
      ret = ((OB_EAGAIN == ret) ? evict_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to evict micro_block meta", KR(ret), K(micro_key));
      } else {
        LOG_TRACE("micro_key dose not exist", KR(ret), K(micro_key));
      }
    } else {
      cache_stat_.task_stat().update_arc_evict_cnt(is_in_l1, is_in_ghost, 1);

      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ARC_EVICT;
      const int64_t micro_size = cold_micro_info.size();
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_key), K(cold_micro_info));
      }
      LOG_TRACE("ss_micro_cache: evict micro_block meta", KR(ret), K(micro_key), K(phy_blk_idx), K(is_in_l1),
        K(is_in_ghost), K(cache_stat_.task_stat()));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_micro_block_meta(ObSSMemBlockHandle &sealed_mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!sealed_mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sealed_mem_blk_handle));
  } else {
    const ObSSMemBlock::MemMicroLocInfoMap &micro_loc_map = sealed_mem_blk_handle()->micro_loc_map_;
    ObSSMemBlock::MemMicroLocInfoMap::const_iterator iter = micro_loc_map.begin();
    for (; OB_SUCC(ret) && (iter != micro_loc_map.end()); ++iter) {
      const ObSSMicroBlockCacheKey &micro_key = iter->first;
      SSMicroMapDeleteUnpersistedMicroFunc delete_func(sealed_mem_blk_handle);
      if (OB_FAIL(micro_meta_map_.operate(&micro_key, delete_func))) {
        ret = ((OB_EAGAIN == ret) ? delete_func.ret_ : ret);
        if (OB_ENTRY_NOT_EXIST == ret) {
          // skip deleted/erased micro_meta
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to try delete unpersisted micro_block meta", KR(ret), K(micro_key));
        }
      } else if (delete_func.succ_delete_) {
        const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_FORCE_DELETE;
        const bool is_in_l1 = delete_func.is_in_l1_;
        const bool is_in_ghost = delete_func.is_in_ghost_;
        const int64_t micro_size = delete_func.size_;
        cache_stat_.micro_stat().update_mark_del_micro_info(is_in_ghost, 1, micro_size);

        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
          LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key), K(sealed_mem_blk_handle));
        }
        LOG_INFO("ss_micro_cache: delete unpersisted meta", KR(ret), K(micro_key), K(is_in_l1), K(is_in_ghost));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_micro_block_meta(const ObSSMicroBlockMetaInfo &cold_micro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    const bool is_in_l1 = cold_micro_info.is_in_l1();
    const bool is_in_ghost = cold_micro_info.is_in_ghost();
    const int64_t micro_size = cold_micro_info.size();
    const int64_t phy_blk_idx = cold_micro_info.data_loc() / block_size_;
    const ObSSMicroBlockCacheKey &micro_key = cold_micro_info.get_micro_key();
    SSMicroMapDeleteMicroFunc delete_func(cold_micro_info);

    if (OB_FAIL(micro_meta_map_.operate(&micro_key, delete_func))) {
      ret = ((OB_EAGAIN == ret) ? delete_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_TRACE("fail to try delete micro_block meta", KR(ret), K(cold_micro_info));
      } else {
        LOG_WARN("fail to try delete micro_block meta", KR(ret), K(cold_micro_info));
      }
    } else {
      cache_stat_.task_stat().update_arc_delete_cnt(is_in_l1, is_in_ghost, 1);
      cache_stat_.micro_stat().update_mark_del_micro_info(is_in_ghost, 1, micro_size);

      // For ghost metas that are in the serialization/persistence window (is_meta_written=true),
      // defer the arc_info adjustment to try_delete_ghost_persisted_micro_meta, which will
      // decrement arc_info when processing the sub_range from disk. Adjusting here would cause
      // a double decrement: once now and once when try_delete_ghost processes the disk sub_range.
      const bool is_meta_written = cold_micro_info.is_meta_written();
      if (!(is_in_ghost && is_meta_written)) {
        const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ARC_DELETE;
        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
          LOG_ERROR("fail to adjust seg_info", KR(ret), K(micro_key), K(cold_micro_info));
        }
      }
      LOG_TRACE("ss_micro_cache: delete micro_block meta", KR(ret), K(micro_key), K(phy_blk_idx), K(is_in_l1),
        K(is_in_ghost), K(is_meta_written), K(cache_stat_.task_stat()));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_force_delete_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    ObSSMicroBlockMetaInfo micro_meta_info;
    SSMicroMapForceDeleteMicroFunc delete_func(micro_meta_info);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, delete_func))) {
      ret = ((OB_EAGAIN == ret) ? delete_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to invalidate micro_block meta", KR(ret), K(micro_key));
      }
    } else if (delete_func.succ_delete_) {
      if (OB_TMP_FAIL(inner_update_phy_blk_valid_len(micro_meta_info))) {
        LOG_WARN("fail to inner update phy_blk valid len", KR(tmp_ret), K(micro_meta_info));
        tmp_ret = OB_SUCCESS;
      }

      const bool is_in_l1 = micro_meta_info.is_in_l1();
      const bool is_in_ghost = micro_meta_info.is_in_ghost();
      const int64_t micro_size = micro_meta_info.size();
      cache_stat_.micro_stat().update_mark_del_micro_info(is_in_ghost, 1, micro_size);
      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_FORCE_DELETE;
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_meta_info));
      }
      LOG_INFO("ss_micro_cache: force delete micro_meta", KR(ret), K(micro_meta_info));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_erase_deleted_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    ObSSMicroBlockCacheKey cur_micro_key = micro_key; // for log print
    SSMicroMapEraseDeletedMicroFunc erase_func;
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, erase_func))) {
      ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to erase deleted micro_meta", KR(ret), K(cur_micro_key));
      }
    } else {
      const bool is_in_ghost = erase_func.is_in_ghost_;
      const int32_t micro_size = erase_func.micro_size_;
      // Now, deleted micro_meta can only be erased in this function, thus we update stat here
      cache_stat_.micro_stat().update_mark_del_micro_info(is_in_ghost, -1, micro_size * -1);
      cache_stat_.micro_stat().update_micro_map_total_info(-1, micro_size * -1);
      cache_stat_.mem_stat().update_micro_map_usage(-1 * SS_MICRO_META_MAP_ITEM_SIZE);
      LOG_INFO("ss_micro_cache: erase deleted micro_meta", KR(ret), K(cur_micro_key));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_erase_expired_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const int64_t expiration_time_s)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || expiration_time_s < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(expiration_time_s));
  } else {
    ObSSMicroBlockCacheKey cur_micro_key = micro_key; // for log print
    ObSSMicroBlockMetaInfo micro_meta_info;
    SSMicroMapEraseExpiredMicroFunc erase_func(expiration_time_s, micro_meta_info);
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, erase_func))) {
      ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to erase deleted micro_meta", KR(ret), K(cur_micro_key));
      }
    } else {
      if (OB_TMP_FAIL(inner_update_phy_blk_valid_len(micro_meta_info))) {
        LOG_WARN("fail to inner update phy_blk valid len", KR(tmp_ret), K(micro_meta_info));
        tmp_ret = OB_SUCCESS;
      }

      const bool is_in_l1 = micro_meta_info.is_in_l1();
      const bool is_in_ghost = micro_meta_info.is_in_ghost();
      const int64_t micro_size = micro_meta_info.size();
      cache_stat_.micro_stat().update_micro_map_total_info(-1, micro_size * -1);
      cache_stat_.mem_stat().update_micro_map_usage(-1 * SS_MICRO_META_MAP_ITEM_SIZE);
      cache_stat_.task_stat().update_expired_cnt(is_in_ghost, 1);
      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ERASE_EXPIRED;
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(cur_micro_key), K(is_in_l1), K(is_in_ghost));
      }
      LOG_INFO("ss_micro_cache: erase expired micro_meta", KR(ret), K(cur_micro_key), K(expiration_time_s));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_erase_abnormal_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    ObSSMicroBlockCacheKey cur_micro_key = micro_key;
    SSMicroMapEraseAbnormalMicroFunc erase_func;
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, erase_func))) {
      ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to try erase abnormal micro_block meta", KR(ret), K(cur_micro_key));
      }
    } else {
      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ERASE_ABNORMAL;
      const bool is_in_l1 = erase_func.is_in_l1_;
      const bool is_in_ghost = erase_func.is_in_ghost_;
      const int64_t micro_size = erase_func.size_;
      cache_stat_.micro_stat().update_micro_map_total_info(-1, micro_size * -1);
      cache_stat_.mem_stat().update_micro_map_usage(-1 * SS_MICRO_META_MAP_ITEM_SIZE);
      
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(cur_micro_key));
      }
      LOG_INFO("ss_micro_cache: erase abnormal micro_meta", KR(ret), K(cur_micro_key), K(is_in_l1), K(is_in_ghost));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_force_erase_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || tablet_id == ObTabletID::INVALID_TABLET_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(tablet_id));
  } else {
    ObSSMicroBlockCacheKey cur_micro_key = micro_key;
    ObSSMicroBlockMetaInfo micro_meta_info;
    SSMicroMapForceEraseMicroFunc erase_func(expiration_time_s_, tablet_id, micro_meta_info);
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, erase_func))) {
      ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to try force erase micro_block meta", KR(ret), K(cur_micro_key));
      }
    } else {
      if (OB_TMP_FAIL(inner_update_phy_blk_valid_len(micro_meta_info))) {
        LOG_WARN("fail to inner update phy_blk valid len", KR(tmp_ret), K(micro_meta_info));
        tmp_ret = OB_SUCCESS;
      }

      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_FORCE_ERASE;
      const bool is_in_l1 = micro_meta_info.is_in_l1();
      const bool is_in_ghost = micro_meta_info.is_in_ghost();
      const int64_t micro_size = micro_meta_info.size();

      cache_stat_.micro_stat().update_micro_map_total_info(-1, micro_size * -1);
      cache_stat_.mem_stat().update_micro_map_usage(-1 * SS_MICRO_META_MAP_ITEM_SIZE);
      cache_stat_.task_stat().update_force_erase_cnt(is_in_ghost, 1);
      
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_meta_info));
      }
      LOG_INFO("ss_micro_cache: force erase micro_meta", KR(ret), K(micro_meta_info));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_update_reorganizing_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const uint64_t exp_micro_data_loc,
    const uint32_t exp_micro_crc,
    const uint64_t exp_reuse_version,
    ObSSMemBlockHandle &mem_blk_handle,
    bool &succ_update)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || exp_micro_data_loc == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(mem_blk_handle), K(exp_micro_data_loc));
  } else if (OB_UNLIKELY(!(mem_blk_handle()->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), KPC(mem_blk_handle.get_ptr()));
  } else {
    SSMicroMapUpdateReorganizingMicroFunc update_func(exp_micro_data_loc, exp_micro_crc, exp_reuse_version, mem_blk_handle);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, update_func))) {
      ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to update reorganizing micro_block meta", KR(ret), K(micro_key));
      }
    } else {
      succ_update = update_func.succ_update_;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::link_meta_and_update_arc(
    const ObSSMicroBlockMetaInfo &micro_meta_info,
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(micro_range_mgr_->insert_micro_meta_into_range(micro_meta_handle))) {
    LOG_WARN("fail to insert micro_meta into sub_range", KR(ret), K(micro_meta_info.get_micro_key()));
  } else {
    const bool is_in_l1 = micro_meta_info.is_in_l1();
    const bool is_in_ghost = micro_meta_info.is_in_ghost();
    const int64_t micro_size = static_cast<int64_t>(micro_meta_info.size());
    arc_info_.update_seg_info(is_in_l1, is_in_ghost, micro_size, 1);
  }
  return ret;
}

int ObSSMicroMetaManager::adjust_arc_seg_info(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const ObSSMicroArcOpType &op_type,
    const int32_t delta_size, 
    const int32_t delta_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, delta_size, delta_cnt))) {
    LOG_WARN("fail to inner adjust arc seg_info", KR(ret), K(is_in_l1), K(is_in_ghost), K(op_type), 
      K(delta_size), K(delta_cnt));
  }
  return ret;
}

int ObSSMicroMetaManager::inner_adjust_arc_seg_info(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const ObSSMicroArcOpType &op_type,
    const int32_t delta_size, 
    const int32_t delta_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arc_info_.adjust_seg_info(is_in_l1, is_in_ghost, op_type, delta_size, delta_cnt))) {
    LOG_ERROR("fail to adjust seg_info", KR(ret), K(is_in_l1), K(is_in_ghost), K(op_type), K(delta_size),
      K(delta_cnt));
  }

  const int64_t arc_valid_cnt = arc_info_.get_valid_count();
  const int64_t arc_valid_size = arc_info_.get_valid_size();
  cache_stat_.micro_stat().update_micro_map_valid_info(arc_valid_cnt, arc_valid_size);
  return ret;
}

int ObSSMicroMetaManager::clear_tablet_micro_block_meta(const ObTabletID &tablet_id, int64_t &remain_micro_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    // Record tablet_id and current micro_ckpt_op_cnt_ for deletion during meta ckpt
    const int64_t cur_micro_ckpt_op_cnt = cache_stat_.task_stat().get_micro_ckpt_op_cnt();
    if (OB_FAIL(record_need_delete_tablet(tablet_id, cur_micro_ckpt_op_cnt))) {
      LOG_WARN("fail to record need delete tablet_id", KR(ret), K(tablet_id), K(cur_micro_ckpt_op_cnt));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::scan_ranges_micro_block_meta(
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObIArray<ObLSHandle> &ls_handles,
    const int64_t start_range_idx,
    const int64_t end_range_idx,
    int64_t &handled_range_idx,
    bool &is_empty_range)
{
  int ret = OB_SUCCESS;
  handled_range_idx = -1;
  is_empty_range = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((start_range_idx < 0) || (end_range_idx < 0) || (start_range_idx > end_range_idx)
             || (!micro_range_mgr_->is_valid_init_range_idx(start_range_idx))
             || (!micro_range_mgr_->is_valid_init_range_idx(end_range_idx)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_range_idx), K(end_range_idx));
  } else {
    ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr_->get_init_range_arr();
    for (int64_t i = start_range_idx; OB_SUCC(ret) && (i <= end_range_idx) && is_empty_range; ++i) {
      if (OB_FAIL(handle_init_range(i, init_range_arr[i], tablet_cache_map, ls_handles, is_empty_range))) {
        LOG_ERROR("fail to handle init_range micro_metas", KR(ret), K(i), K(start_range_idx), K(end_range_idx),
          KPC(init_range_arr[i]));
      } else {
        handled_range_idx = i;
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::handle_init_range(
    const int64_t init_range_idx,
    ObSSMicroInitRangeInfo *init_range,
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObIArray<ObLSHandle> &ls_handles,
    bool &empty_range)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  empty_range = true;
  int64_t init_rng_micro_cnt = 0;
  if (OB_ISNULL(init_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("initial_micro_range should not be null", KR(ret), KP(init_range));
  } else if (FALSE_IT(init_rng_micro_cnt = init_range->get_total_micro_cnt(true/*include_pmeta*/))) {
  } else if (init_rng_micro_cnt > 0) {
    ObSSMicroSubRangeInfo *new_range_header = nullptr;
    ObSSMicroSubRangeInfo *old_range_header = nullptr;
    // 1. All valid micro_metas will be readded into new-created sub_range(s)
    if (OB_FAIL(handle_init_range_micro_metas(init_range_idx, init_range, init_rng_micro_cnt, tablet_cache_map,
        ls_handles, new_range_header, old_range_header, empty_range))) {
      LOG_WARN("fail to handle init_range's all micro_metas", KR(ret), K(init_range_idx), K(init_rng_micro_cnt),
        KP(init_range));
    }

    // 2. Rebuild this init_range, relink these micro_metas or sub_ranges
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rebuild_initial_range(init_range, new_range_header, empty_range))) {
      LOG_WARN("fail to rebuild initial_micro_range", KR(ret), KPC(init_range), KP(new_range_header), K(empty_range));
    }

    // 3. handle range memory, avoid leak
    if ((nullptr != new_range_header) && OB_TMP_FAIL(free_tmp_sub_ranges(new_range_header))) {
      LOG_WARN("fail to free tmp sub_ranges", KR(ret), KR(tmp_ret), KP(new_range_header));
      tmp_ret = OB_SUCCESS;
    }
    if ((nullptr != old_range_header) && OB_TMP_FAIL(free_tmp_sub_ranges(old_range_header))) {
      LOG_WARN("fail to free tmp sub_ranges", KR(ret), KR(tmp_ret), KP(old_range_header));
      tmp_ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::handle_init_range_micro_metas(
    const int64_t init_range_idx,
    ObSSMicroInitRangeInfo *init_range,
    const int64_t init_rng_micro_cnt,
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObIArray<ObLSHandle> &ls_handles,
    ObSSMicroSubRangeInfo *&new_range_header,
    ObSSMicroSubRangeInfo *&old_range_header,
    bool &empty_range)
{
  int ret = OB_SUCCESS;
  int64_t new_sub_rng_cnt = 0;
  const double del_micro_pct = inner_calc_deleted_micro_pct();
  if (OB_ISNULL(init_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range));
  } else if (OB_FAIL(create_new_sub_ranges(init_range_idx, init_range, init_rng_micro_cnt, del_micro_pct,
             new_range_header, new_sub_rng_cnt))) {
    LOG_WARN("fail to create sub_range", KR(ret), K(init_range_idx), K(init_rng_micro_cnt), K(del_micro_pct), 
      KP(init_range));
  } else if (OB_ISNULL(new_range_header) || OB_UNLIKELY(new_sub_rng_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected new sub_range", KR(ret), K(new_sub_rng_cnt), KP(new_range_header));
  } else {
    init_range->remove_sub_ranges(old_range_header);
    ObSSMicroSubRangeInfo *cur_sub_range = old_range_header;
    int64_t scanned_micro_cnt = 0;
    int64_t remained_micro_cnt = 0;
    int64_t iter_cnt = 0;
    while (OB_SUCC(ret) && OB_NOT_NULL(cur_sub_range) && (iter_cnt < UINT32_MAX)) {
      if (cur_sub_range->exist_ghost_persisted_meta()) {
        bool all_mem_meta = true;
        SSTryLoadPersistedMicroMetaGuard load_guard(ret, all_mem_meta, init_range, cur_sub_range);

        if (OB_FAIL(ret)) {
        } else if (!all_mem_meta) {
          ObSEArray<ObSSMicroBlockMetaInfo, SS_DEF_ARRAY_CNT> parsed_micro_infos;
          int64_t parsed_micro_cnt = 0;
          // there exists failure handle in these two func
          if (OB_FAIL(inner_obtain_persisted_micro_metas(init_range, cur_sub_range, parsed_micro_infos))) {
            LOG_WARN("fail to obtain persisted micro_metas", KR(ret), KPC(cur_sub_range));
          } else if (FALSE_IT(parsed_micro_cnt = parsed_micro_infos.count())) {
          } else if (OB_FAIL(inner_add_persisted_micro_metas(parsed_micro_infos))) {
            LOG_WARN("fail to add persisted micro_metas", KR(ret), KPC(cur_sub_range), K(parsed_micro_cnt));
          } else {
            all_mem_meta = true;
            cache_stat_.range_stat().update_fe_sub_range_cnt(-1);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObSSMicroBlockMeta *first_micro_meta = nullptr;
        int64_t micro_cnt = 0;
        if (OB_FAIL(init_range->remove_sub_range_micro_info(cur_sub_range, first_micro_meta, micro_cnt))) {
          LOG_WARN("fail to remove sub_range micro_list", KR(ret), KPC(cur_sub_range));
        } else if (nullptr != first_micro_meta) {
          if (OB_FAIL(handle_sub_range_micro_metas(new_range_header, first_micro_meta, tablet_cache_map, ls_handles,
              scanned_micro_cnt, remained_micro_cnt))) {
            LOG_WARN("fail to handle sub_range micro_metas", KR(ret), KPC(cur_sub_range), K(micro_cnt), 
              KP(first_micro_meta), KP(new_range_header));
          } else if (remained_micro_cnt > 0) {
            empty_range = false;
          }
        }
      } else {
        // logic check
        LOG_WARN("exist persisted sub_range handle failure", K(init_range_idx), KP(cur_sub_range), KPC(cur_sub_range));
      }
      
      ret = OB_SUCCESS; // continue to handle next sub_range
      cur_sub_range = cur_sub_range->next_;
      ++iter_cnt;
    }

    if (OB_UNLIKELY(remained_micro_cnt >= SS_SUB_RNG_PERSIST_META_MAX_CNT * (new_sub_rng_cnt + 2))) {
      LOG_WARN("new_sub_range count is not accurate", K(init_range_idx), K(init_rng_micro_cnt), K(new_sub_rng_cnt),
        K(remained_micro_cnt), K(scanned_micro_cnt), K(del_micro_pct), K_(cache_stat));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::create_new_sub_ranges(
    const int64_t init_range_idx,
    ObSSMicroInitRangeInfo *init_range,
    const int64_t init_rng_micro_cnt,
    const double del_micro_pct,
    ObSSMicroSubRangeInfo *&new_range_header,
    int64_t &sub_rng_cnt)
{
  int ret = OB_SUCCESS;
  uint32_t init_rng_start_hval = 0;
  uint32_t init_rng_end_hval = 0;
  sub_rng_cnt = 0;
  const int64_t sub_rng_micro_cnt = SS_SUB_RNG_PERSIST_META_MAX_CNT;
  if (OB_UNLIKELY(init_range_idx < 0 || sub_rng_micro_cnt <= 0 || init_rng_micro_cnt <= 0) || OB_ISNULL(init_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(init_range_idx), K(sub_rng_micro_cnt), K(init_rng_micro_cnt), KP(init_range));
  } else {
    if (OB_FAIL(micro_range_mgr_->get_init_range_info(init_range_idx, init_rng_start_hval, init_rng_end_hval))) {
      LOG_WARN("fail to get init range info", KR(ret), K(init_range_idx));
    } else {
      if (init_rng_micro_cnt <= SS_SUB_RNG_PERSIST_META_MAX_CNT) {
        sub_rng_cnt = 1;
      } else {
        const int64_t exp_micro_cnt = init_rng_micro_cnt * (1.0 - del_micro_pct);
        sub_rng_cnt = (exp_micro_cnt % sub_rng_micro_cnt == 0)
                    ? (exp_micro_cnt / sub_rng_micro_cnt)
                    : (exp_micro_cnt / sub_rng_micro_cnt + 1);
        sub_rng_cnt = MAX(1, sub_rng_cnt); // ensure sub_rng_cnt must be larger than 0
      }

      // Here, we can also choose to set 'retry_alloc' as false, but it may cause lots of memory allocation
      // failure logs. Thus we set it as true, although it will make the thread hang up.
      if (OB_FAIL(micro_range_mgr_->create_sub_ranges(init_rng_start_hval, init_rng_end_hval, sub_rng_cnt, 
          new_range_header, true/*retry_alloc*/))) {
        LOG_WARN("fail to create sub_ranges", KR(ret), K(init_range_idx), K(init_rng_start_hval), K(init_rng_end_hval),
          K(sub_rng_cnt), K(init_rng_micro_cnt), K(del_micro_pct), KP(init_range));
      }
    }
  }
  return ret;
}

double ObSSMicroMetaManager::inner_calc_deleted_micro_pct() const
{
  double del_micro_pct = 0;
  int ret = OB_SUCCESS;

  const int64_t del_micro_cnt = cache_stat_.micro_stat().get_deleted_micro_cnt();
  const int64_t total_micro_cnt = cache_stat_.micro_stat().get_total_micro_cnt();
  if (del_micro_cnt > 0 && total_micro_cnt > 0) {
    if (OB_UNLIKELY(del_micro_cnt > total_micro_cnt)) {
      LOG_WARN("unexpected deleted micro count", K(del_micro_cnt), K(total_micro_cnt), K_(cache_stat));
    } else {
      del_micro_pct = del_micro_cnt / static_cast<double>(total_micro_cnt);
    }
  }
  return del_micro_pct;
}

int ObSSMicroMetaManager::handle_sub_range_micro_metas(
    ObSSMicroSubRangeInfo *new_range_header,
    ObSSMicroBlockMeta *first_micro_meta,
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObIArray<ObLSHandle> &ls_handles,
    int64_t &scanned_micro_cnt,
    int64_t &remained_micro_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(new_range_header) || OB_ISNULL(first_micro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(new_range_header), KP(first_micro_meta));
  } else {
    scanned_micro_cnt = 0;
    remained_micro_cnt = 0;

    ObSSMicroRangeTabletCacheScanFilter tablet_cache_filter;
    ObSSMicroBlockMeta *cur_micro_meta = first_micro_meta;
    while ((nullptr != cur_micro_meta) && (scanned_micro_cnt < UINT32_MAX)) {
      ++scanned_micro_cnt;
      // Notice: we promise that if 'cur_micro_meta' is not nullptr, it is safe to use it here, cuz there
      //         is no other func besides this one and calibration that can perform 'erase_micro_meta'.
      //         Futhermore, calibration and meta ckpt will not be executed concurrently, so it is safe to use it here.
      ObSSMicroBlockMeta *next_micro = cur_micro_meta->get_next_micro();
      cur_micro_meta->set_next_micro(nullptr);
      ObSSMicroBlockMetaInfo cur_micro_info;
      bool succ_erase = false;
      bool need_relink = true;
      if (OB_FAIL(get_micro_meta_info(cur_micro_meta->get_micro_key(), cur_micro_info))) {
        LOG_WARN("fail to get micro_meta info", KR(ret), KPC(cur_micro_meta));
      } else if (OB_FAIL(check_need_erase_micro_meta(cur_micro_info, tablet_cache_filter, tablet_cache_map,
                 ls_handles, succ_erase))) {
        LOG_WARN("fail to check need erase micro_meta", KR(ret), K(cur_micro_info));
      } else if (!succ_erase) {
        ++remained_micro_cnt;
        // Only if we didn't erase micro_meta, we need to link into sub_range temporarily
        ObSSMicroBlockMetaHandle cur_micro_handle;
        cur_micro_handle.set_ptr(cur_micro_meta);
        const uint32_t micro_hval = cur_micro_info.get_micro_key().micro_hash();
        ObSSMicroSubRangeInfo *target_sub_rng = nullptr;
        new_range_header->get_sub_range_by_hval(micro_hval, target_sub_rng);

        // Note: No need to use 'init_range lock' here to protect modifying 'target_sub_rng',
        // because 'target_sub_rng' is a newly created sub_range that hasn't been linked to any init_range yet.
        if (OB_ISNULL(target_sub_rng)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("target_sub_rng should not be null", KR(ret), K(micro_hval), KP(target_sub_rng));
        } else if (OB_FAIL(target_sub_rng->link_micro_meta(cur_micro_handle))) {
          LOG_WARN("fail to link micro_meta into range", KR(ret), K(scanned_micro_cnt), K(cur_micro_info), KPC(target_sub_rng));
        } else {
          need_relink = false;
        }
      }

      if (succ_erase) { // in case OB_FAIL(ret) but succ_erase=true
        need_relink = false;
        cur_micro_meta = nullptr;
      }

      // if micro_meta not erase and not relink into sub_range, we need to link this micro_meta into init_range again.
      if (need_relink) {
        ObSSMicroBlockMetaHandle cur_micro_handle;
        cur_micro_handle.set_ptr(cur_micro_meta);
        if (OB_TMP_FAIL(micro_range_mgr_->insert_micro_meta_into_range(cur_micro_handle))) {
          LOG_ERROR("fail to insert micro_meta into range", KR(ret), KR(tmp_ret), KPC(cur_micro_meta));
          tmp_ret = OB_SUCCESS;
        }
      }

      cur_micro_meta = next_micro;
      ret = OB_SUCCESS; // continue to handle next micro_meta
    }

    if (scanned_micro_cnt >= UINT32_MAX) {
      LOG_ERROR("micro_meta list may exist cycle", K(scanned_micro_cnt), KP(first_micro_meta));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::check_need_erase_micro_meta(
    const ObSSMicroBlockMetaInfo &cur_micro_info,
    ObSSMicroRangeScanFilter &tablet_cache_filter,
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObIArray<ObLSHandle> &ls_handles,
    bool &succ_erase)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  succ_erase = false;
  bool need_delete = false;
  const ObSSMicroBlockCacheKey &cur_micro_key = cur_micro_info.get_micro_key();
  const ObTabletID tablet_id(cur_micro_info.effective_tablet_id());
  if (cur_micro_info.is_meta_deleted()) {
    // If there exists mark_deleted micro_meta, erase it from meta_map
    if (OB_FAIL(try_erase_deleted_micro_block_meta(cur_micro_key))) {
      LOG_WARN("fail to erase deleted micro_block meta", KR(ret), K(cur_micro_info));
    } else {
      succ_erase = true;
    }
  } else if (cur_micro_info.is_expired(expiration_time_s_)) {
    // If there exists expired & data_persisted micro_meta, erase it from meta_map
    if (OB_FAIL(try_erase_expired_micro_block_meta(cur_micro_key, expiration_time_s_))) {
      LOG_WARN("fail to erase expired micro_block meta", KR(ret), K(cur_micro_info));
    } else {
      succ_erase = true;
    }
  } else if (OB_FAIL(is_tablet_need_delete(tablet_id, need_delete))) {
    LOG_WARN("fail to check if tablet need to be deleted", KR(ret), K(tablet_id));
  } else if (need_delete) {
    // If this tablet_id need to be deleted, erase it from meta_map
    if (OB_FAIL(try_force_erase_micro_block_meta(cur_micro_key, tablet_id.id()))) {
      LOG_WARN("fail to erase micro_block meta by tablet", KR(ret), K(cur_micro_info));
    } else {
      succ_erase = true;
    }
  } else {
    if (tablet_cache_filter.should_include(cur_micro_info)) {
      // Calculate each tablet cache size info
      bool need_erase = false;
      if (OB_TMP_FAIL(inner_update_tablet_cache_map(tablet_cache_map, cur_micro_key, cur_micro_info,
          ls_handles, need_erase))) {
        LOG_WARN("fail to update tablet cache map", KR(tmp_ret), K(cur_micro_info));
        tmp_ret = OB_SUCCESS;
      } else if (need_erase) {
        LOG_INFO("Will force erase this micro from cache", K(cur_micro_info));
        if (OB_FAIL(try_force_erase_micro_block_meta(cur_micro_key, cur_micro_info.effective_tablet_id()))) {
          LOG_WARN("fail to force erase micro_block meta", KR(ret), K(cur_micro_info));
        } else {
          succ_erase = true;
        }
      }
    } else {
      LOG_ERROR("exist abnormal micro_block in ss_micro_cache", K(cur_micro_info));
      // If there exists T1/T2 invalid micro_meta, erase it from meta_map
      if (OB_TMP_FAIL(try_erase_abnormal_micro_block_meta(cur_micro_key))) {
        LOG_WARN("fail to erase abnormal micro_block meta", KR(tmp_ret), K(cur_micro_info));
        tmp_ret = OB_SUCCESS;
      } else {
        succ_erase = true;
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::rebuild_initial_range(
    ObSSMicroInitRangeInfo *init_range,
    ObSSMicroSubRangeInfo *&new_range_header,
    bool &empty_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(init_range) || OB_ISNULL(new_range_header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range), KP(new_range_header));
  } else {
    // link new_sub_ranges into init_range and handle inc_sub_range
    ObSSMicroSubRangeInfo *inc_first_range = nullptr;
    if (OB_FAIL(init_range->link_sub_ranges(new_range_header, inc_first_range))) {
      LOG_ERROR("fail to link sub micro_range", KR(ret), KP(new_range_header));
    } else if (FALSE_IT(new_range_header = nullptr)) {
    } else if (OB_ISNULL(inc_first_range)) {
    } else if (OB_FAIL(handle_inc_sub_range(init_range, inc_first_range, empty_range))) {
      LOG_WARN("fail to insert micro_metas into range", KR(ret), KPC(inc_first_range));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::handle_inc_sub_range(
    ObSSMicroInitRangeInfo *init_range,
    ObSSMicroSubRangeInfo *&inc_sub_range,
    bool &empty_range)
{
  int ret = OB_SUCCESS;
  ObSSMicroBlockMeta *first_micro_meta = nullptr;
  int64_t micro_cnt = 0;
  if (OB_ISNULL(init_range) || OB_ISNULL(inc_sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range), KP(inc_sub_range));
  } else if (OB_FAIL(init_range->remove_sub_range_micro_info(inc_sub_range, first_micro_meta, micro_cnt))) {
    LOG_WARN("fail to remove sub_range micro_list", KR(ret), KPC(inc_sub_range));
  } else {
    ObSSMicroBlockMeta *cur_micro_meta = first_micro_meta;
    uint32_t iter_cnt = 0;
    uint32_t succ_insert_cnt = 0;
    while ((nullptr != cur_micro_meta) && (iter_cnt < UINT32_MAX)) {
      ObSSMicroBlockMeta *next_micro_meta = cur_micro_meta->get_next_micro();
      cur_micro_meta->set_next_micro(nullptr);
      ObSSMicroBlockMetaHandle tmp_micro_handle;
      tmp_micro_handle.set_ptr(cur_micro_meta);
      // NOTICE: init_range must have sub_range(s), no need to alloc sub_range here.
      if (OB_FAIL(micro_range_mgr_->insert_micro_meta_into_range(tmp_micro_handle))) {
        LOG_ERROR("fail to insert micro_meta into range", KR(ret), KPC(cur_micro_meta));
      } else {
        ++succ_insert_cnt;
      }
      cur_micro_meta = next_micro_meta;
      ret = OB_SUCCESS; // continue to handle next micro_meta
    }

    if (succ_insert_cnt > 0) {
      empty_range = false;
    }

    if (iter_cnt >= UINT32_MAX) {
      LOG_ERROR("micro_meta list may exist cycle", K(iter_cnt), KP(first_micro_meta));
    }

    if (OB_SUCC(ret)) {
      bool succ_freed = false;
      inc_sub_range->try_free(succ_freed);
      if (succ_freed) {
        inc_sub_range = nullptr;
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::free_tmp_sub_ranges(ObSSMicroSubRangeInfo *&first_sub_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(first_sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(first_sub_range));
  } else if (OB_FAIL(micro_range_mgr_->try_free_sub_ranges(first_sub_range))) {
    LOG_ERROR("fail to try free sub_ranges", KR(ret), KP(first_sub_range));
  } else {
    first_sub_range = nullptr;
  }
  return ret;
}

int ObSSMicroMetaManager::inner_update_tablet_cache_map(
    ObSSMCTabletInfoMap &tablet_cache_map,
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroBlockMetaInfo &micro_info,
    const ObIArray<ObLSHandle> &ls_handles,
    bool &need_erase)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    need_erase = false;
    ObSSMicroCacheTabletInfo tablet_cache_info;
    ObTabletID tablet_id(micro_info.effective_tablet_id());
    if (OB_FAIL(tablet_cache_map.get_refactored(tablet_id, tablet_cache_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        ObSSMicroCacheUtil::get_ls_id_by_tablet_id(tenant_id_, tablet_id, ls_handles, tablet_cache_info.ls_id_);
        if (!tablet_cache_info.ls_id_.is_valid()) {
          need_erase = true;
        }
      } else {
        LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
      }
    }
    
    if (OB_SUCC(ret) && !need_erase) {
      tablet_cache_info.tablet_id_ = tablet_id;
      tablet_cache_info.add_micro_size(micro_info.is_in_l1(), micro_info.is_in_ghost(), micro_info.size());
      if (OB_FAIL(tablet_cache_map.set_refactored(tablet_id, tablet_cache_info, true/*overwrite*/))) {
        LOG_WARN("fail to set refactored", KR(ret), K(tablet_id), K(tablet_cache_info));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::read_micro_meta_checkpoint(
    ObSSCkptPhyBlockItemReader &item_reader, 
    const int64_t micro_ckpt_time_us,
    ObIArray<int64_t> &ckpt_blk_id_list,
    bool need_reset_stat)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!item_reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    // If the checkpoint was generated two days ago, when we replay this ckpt, all replayed micro_meta
    // would be expired directly.
    const int64_t delta_time_us = MAX(ObTimeUtility::current_time_us() - micro_ckpt_time_us, 0);
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    if (need_reset_stat) {
      cache_stat_.micro_stat().reset();
      arc_info_.reset_seg_info();
    }
    replay_ctx_.reset();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next micro_meta item", KR(ret), K_(tenant_id), KP(item_buf), K(item_buf_len));
        }
        ret = OB_SUCCESS; // To make observer restart succ, thus return OB_SUCC.
        break;
      } else {
        replay_ctx_.inc_total_replay_cnt();
        ObSSMicroBlockMetaHandle micro_meta_handle;
        int64_t pos = 0;
        if (OB_FAIL(alloc_micro_block_meta(micro_meta_handle))) {
          LOG_WARN("fail to alloc micro_block_meta", KR(ret));
        } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_meta_handle should be valid", KR(ret), K(micro_meta_handle));
        } else if (OB_FAIL(micro_meta_handle()->deserialize(item_buf, item_buf_len, pos))) {
          LOG_WARN("fail to deserialize", KR(ret), K(item_buf_len), KP(item_buf));
        } else {
          const bool is_in_l1 = micro_meta_handle()->is_in_l1();
          const bool is_in_ghost = micro_meta_handle()->is_in_ghost();
          const uint64_t data_loc = micro_meta_handle()->data_loc();
          const uint64_t reuse_version = micro_meta_handle()->reuse_version();
          const uint64_t micro_size = micro_meta_handle()->size();
          if (micro_meta_handle()->is_valid_for_ckpt(true/*print_log*/)) {
            micro_meta_handle()->update_access_time_s(delta_time_us / 1000000L);
            int64_t phy_blk_idx = -1;
            if (OB_FAIL(inner_add_ckpt_micro_block_meta(micro_meta_handle))) {
              LOG_WARN("fail to inner add ckpt micro_block meta", KR(ret), KPC(micro_meta_handle.get_ptr()));
            } else if (is_in_ghost) {
              micro_meta_handle()->mark_invalid();
              replay_ctx_.inc_ghost_micro_cnt(is_in_l1, is_in_ghost);
            } else if (OB_FAIL(SSPhyBlockMgr.update_block_valid_length(data_loc, reuse_version, micro_size, phy_blk_idx))) {
              LOG_WARN("fail to update block valid length", KR(ret), KPC(micro_meta_handle.get_ptr()), K(phy_blk_idx));
            }
          } else {
            micro_meta_handle()->mark_invalid();
            replay_ctx_.inc_invalid_micro_cnt(is_in_l1, is_in_ghost);
          }
        }
        ret = OB_SUCCESS; // continue to handle next micro_meta
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_block_id_list(ckpt_blk_id_list))) {
        LOG_WARN("fail to get block_id list", KR(ret));
      }
    }

    FLOG_INFO("ss_micro_cache: finish read micro_meta ckpt", KR(ret), "ckpt_blk_cnt", ckpt_blk_id_list.count(), 
      K_(replay_ctx), K_(arc_info), "micro_stat", cache_stat_.micro_stat());
  }
  return ret;
}

int ObSSMicroMetaManager::inner_add_ckpt_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta_handle));
  } else if (OB_LIKELY(micro_meta_handle()->is_in_ghost() || micro_meta_handle()->is_valid_field())) { 
    const int32_t micro_size = micro_meta_handle()->size();
    const ObSSMicroBlockCacheKey &micro_key = micro_meta_handle()->get_micro_key();
    if (OB_FAIL(micro_meta_map_.insert(&micro_key, micro_meta_handle))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_INFO("micro_meta already exist, no need to insert again", KR(ret), K(micro_key));
      } else {
        LOG_WARN("fail to insert micro_map", KR(ret), K(micro_key));
      }
    } else {
      const bool is_in_l1 = micro_meta_handle()->is_in_l1();
      const bool is_in_ghost = micro_meta_handle()->is_in_ghost();
      const int32_t micro_size = micro_meta_handle()->size();
      // TODO by shiyuhui.syh: update arc_info after insert micro_meta into sub_range, same as link_meta_and_update_arc logic
      arc_info_.update_seg_info(is_in_l1, is_in_ghost, micro_size, 1);

      const int64_t arc_valid_cnt = arc_info_.get_valid_count();
      const int64_t arc_valid_size = arc_info_.get_valid_size();
      cache_stat_.micro_stat().update_micro_map_valid_info(arc_valid_cnt, arc_valid_size);
      cache_stat_.micro_stat().update_micro_map_total_info(1, micro_size);
      cache_stat_.micro_stat().set_max_micro_meta_cnt(micro_meta_map_.count());
      cache_stat_.mem_stat().update_micro_map_usage(SS_MICRO_META_MAP_ITEM_SIZE);
    }

    // if we fail to insert micro_meta into range, it doesn't matter, just means this
    // micro_meta can't participate in 'persist_micro_meta'.
    if (OB_FAIL(ret)) {
    } else if (OB_TMP_FAIL(micro_range_mgr_->insert_micro_meta_into_range(micro_meta_handle))) {
      LOG_ERROR("fail to insert micro_meta into range", KR(tmp_ret), K(micro_key));
      tmp_ret = OB_SUCCESS;
    } 
  }
  return ret;
}

int ObSSMicroMetaManager::inner_update_phy_blk_valid_len(const ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  const bool is_in_ghost = micro_meta_info.is_in_ghost();
  const bool is_data_persisted = micro_meta_info.is_data_persisted();
  const int64_t micro_size = micro_meta_info.size();
  if (!is_in_ghost && is_data_persisted) {
    const uint64_t data_loc = micro_meta_info.data_loc();
    const uint64_t reuse_version = micro_meta_info.reuse_version();
    int64_t phy_blk_idx = -1;
    if (OB_FAIL(SSPhyBlockMgr.update_block_valid_length(data_loc, reuse_version, micro_size * -1, 
        phy_blk_idx))) {
      if (OB_S2_REUSE_VERSION_MISMATCH == ret) {
        LOG_INFO("reuse_version mismatch, can't update blk valid len", K(micro_meta_info), K(phy_blk_idx));
      } else {
        LOG_ERROR("fail to update block valid length", KR(ret), K(micro_meta_info), K(phy_blk_idx));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_erase_persisted_micro_metas(
    ObSSMicroSubRangeInfo *sub_range,
    const ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr,
    int64_t &force_erase_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t micro_key_cnt = micro_key_arr.count();
  ObSSMicroInitRangeInfo *init_range = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(sub_range) || OB_UNLIKELY(micro_key_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range), K(micro_key_cnt));
  } else if (OB_FAIL(micro_range_mgr_->get_init_range_by_hval(sub_range->get_end_hval(), init_range))) {
    LOG_WARN("fail to get init_range by hval", KR(ret), KPC(sub_range));
  } else if (OB_ISNULL(init_range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init_range should not be null", KR(ret), KP(init_range));
  } else {
    ObSSMicroBlockMeta *rng_first_micro = nullptr;
    int64_t micro_cnt = 0;
    if (OB_FAIL(init_range->remove_sub_range_micro_list(sub_range, rng_first_micro, micro_cnt))) {
      LOG_WARN("fail to remove sub_range micro_list", KR(ret), KPC(sub_range), KPC(init_range));
    } else if (OB_ISNULL(rng_first_micro) || OB_UNLIKELY(micro_cnt < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("there should exist micro_meta in sub_range", KR(ret), KP(rng_first_micro), K(micro_cnt), KPC(sub_range));
    } else {
      force_erase_cnt = 0;
      ObSSMicroBlockMeta *cur_micro_meta = rng_first_micro;
      while (OB_SUCC(ret) && (nullptr != cur_micro_meta)) {
        ObSSMicroBlockMeta *next_micro_meta = cur_micro_meta->get_next_micro();
        cur_micro_meta->set_next_micro(nullptr);

        const ObSSMicroBlockCacheKey &cur_micro_key = cur_micro_meta->get_micro_key();
        ObSSMicroBlockMetaInfo micro_meta_info; // NOTICE: only ghost micro_meta will fill this struct.
        SSMicroMapEraseOrUpdateMicroMetaFunc erase_func(true/*can_erase*/, micro_meta_info);
        if (OB_FAIL(micro_meta_map_.erase_if(&cur_micro_key, erase_func))) {
          ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to erase or update micro_meta", KR(ret), KPC(cur_micro_meta));
          } else {
            // if not erase, need to be re-added into sub_range
            ObSSMicroBlockMetaHandle micro_meta_handle;
            micro_meta_handle.set_ptr(cur_micro_meta);
            if (OB_FAIL(init_range->link_micro_meta_directly(sub_range, micro_meta_handle))) {
              LOG_ERROR("fail to link micro_meta into sub_range directly", KR(ret), KPC(sub_range), KPC(init_range),
                KPC(cur_micro_meta));
            }
          }
          ret = OB_SUCCESS; // continue to handle next one.
        } else {
          ++force_erase_cnt;
          const int64_t micro_size = micro_meta_info.size();
          cache_stat_.micro_stat().update_micro_map_total_info(-1, micro_size * -1);
          cache_stat_.mem_stat().update_micro_map_usage(SS_MICRO_META_MAP_ITEM_SIZE * -1);
        }
        cur_micro_meta = next_micro_meta;
      }

      if (force_erase_cnt > 0) {
        FLOG_INFO("ss_micro_cache: force erase persisted micro_block meta", KR(ret), K(force_erase_cnt));
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_update_persisted_micro_meta(const ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr)
{
  int ret = OB_SUCCESS;
  const int64_t micro_key_cnt = micro_key_arr.count();
  if (OB_UNLIKELY(micro_key_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_key_cnt); ++i) {
      const ObSSMicroBlockCacheKey &cur_micro_key = micro_key_arr.at(i);
      ObSSMicroBlockMetaInfo micro_meta_info;
      SSMicroMapEraseOrUpdateMicroMetaFunc update_func(false/*can_erase*/, micro_meta_info);
      if (OB_FAIL(micro_meta_map_.operate(&cur_micro_key, update_func))) {
        ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to erase or update micro_meta", KR(ret), K(cur_micro_key));
        }
        ret = OB_SUCCESS; // continue to handle next one.
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::update_effective_tablet_id(
    const ObSSMicroBlockCacheKey &micro_key, 
    const ObTabletID &effective_tablet_id,
    bool &succ_update)
{
  int ret = OB_SUCCESS;
  succ_update = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !effective_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(effective_tablet_id));
  } else {
    SSMicroMapUpdateEffectiveTabletID update_func(effective_tablet_id.id());
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, update_func))) {
      ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_TRACE("not exist this micro_key", KR(ret), K(micro_key), K(effective_tablet_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to update effective tablet_id in micro_meta", KR(ret), K(micro_key), K(effective_tablet_id));
      }
    } else {
      succ_update = true;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_ghost_persisted_micro_meta(
    ObSSMicroInitRangeInfo *init_range,
    ObSSMicroSubRangeInfo *sub_range,
    const bool is_in_l1,
    int64_t &delete_ghost_cnt)
{
  int ret = OB_SUCCESS;
  ObSSMicroSubRangeInfo sub_range_snapshot;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(init_range) || OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range), KP(sub_range));
  } else if (OB_FAIL(init_range->get_sub_range_snapshot_info(sub_range, sub_range_snapshot))) {
    LOG_WARN("fail to get sub_range snapshot info", KR(ret), KP(init_range), KP(sub_range), KPC(init_range),
      KPC(sub_range));
  } else if (OB_UNLIKELY(sub_range_snapshot.get_end_hval() > init_range->get_end_hval())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sub_range_snapshot), KPC(init_range));
  } else if (OB_LIKELY(sub_range_snapshot.get_ghost_pmeta_cnt() > 0)) {
    bool all_mem_meta = true;
    SSTryLoadPersistedMicroMetaGuard load_guard(ret, all_mem_meta, init_range, sub_range);

    if (OB_FAIL(ret)) {
    } else if (!all_mem_meta) {
      ObSEArray<ObSSMicroBlockMetaInfo, SS_DEF_ARRAY_CNT> parsed_micro_infos;
      if (OB_FAIL(inner_obtain_persisted_micro_metas(init_range, sub_range, parsed_micro_infos))) {
        LOG_WARN("fail to obtain persisted micro_metas", KR(ret), KPC(sub_range));
      } else if (OB_LIKELY(parsed_micro_infos.count() > 0)) {
        const int64_t parsed_micro_cnt = parsed_micro_infos.count();
        ObSEArray<ObSSMicroBlockMetaInfo, SS_DEF_ARRAY_CNT> readd_micro_infos;
        delete_ghost_cnt = 0;
        int64_t delete_ghost_size = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (i < parsed_micro_cnt); ++i) {
          if (parsed_micro_infos.at(i).is_in_ghost()) {
            if (parsed_micro_infos.at(i).is_in_l1() != is_in_l1) {
              if (OB_FAIL(readd_micro_infos.push_back(parsed_micro_infos.at(i)))) {
                LOG_WARN("fail to push back", KR(ret), K(i), K(parsed_micro_infos.at(i)));
              }
            } else {
              ++delete_ghost_cnt;
              delete_ghost_size += parsed_micro_infos.at(i).size();
            }
          }
        }

        const int64_t readd_micro_cnt = readd_micro_infos.count();
        if (OB_FAIL(ret)) {
        } else if (readd_micro_cnt < 1) {
        } else if (OB_FAIL(inner_add_persisted_micro_metas(readd_micro_infos))) {
          LOG_WARN("fail to add persisted micro_metas", KR(ret), K(readd_micro_cnt), K(is_in_l1), KPC(sub_range),
            KPC(init_range));
        } 
        
        // Process when delete_ghost_cnt > 0 && delete_ghost_size > 0
        if (OB_SUCC(ret) && (delete_ghost_cnt > 0)) {
          const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ARC_DELETE;
          
          if (OB_UNLIKELY(delete_ghost_size <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("delete_ghost_size should > 0", KR(ret), K(delete_ghost_size), K(delete_ghost_cnt));
          } else if (OB_FAIL(arc_info_.adjust_seg_info(is_in_l1, true, op_type, delete_ghost_size, delete_ghost_cnt))) {
            LOG_ERROR("fail to adjust seg_info", KR(ret), K(is_in_l1), K(delete_ghost_size), K(delete_ghost_cnt),
              KPC(sub_range), KPC(init_range));
          } else {
            all_mem_meta = true;
            cache_stat_.range_stat().update_fe_sub_range_cnt(-1);
          }
        }
        readd_micro_infos.reset();
        LOG_INFO("finish to delete ghost persisted micro_meta", KR(ret), K(is_in_l1), K(delete_ghost_cnt),
          K(readd_micro_cnt));
      }
      parsed_micro_infos.reset();
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_load_persisted_micro_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    SSMicroMapGetMicroHandleFunc &get_func,
    ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroBlockMetaHandle &micro_meta_handle = get_func.micro_meta_handle_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT; // micro_meta_handle should be empty
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_meta_handle));
  } else {
    const uint32_t hval = micro_key.micro_hash();
    ObSSMicroInitRangeInfo *init_range = nullptr;
    ObSSMicroSubRangeHandle sub_range_handle;
    bool may_exist_micro = false;
    if (OB_FAIL(micro_range_mgr_->get_init_range_by_hval(hval, init_range))) {
      LOG_WARN("fail to get init_range by hval", KR(ret), K(hval), K(micro_key));
    } else if (OB_ISNULL(init_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("init_range should not be null", KR(ret), KP(init_range), K(hval));
    } else if (OB_FAIL(init_range->check_exist_persisted_micro_meta(hval, may_exist_micro, sub_range_handle))) {
      LOG_WARN("fail to check exist persisted micro_meta", KR(ret), K(hval), KPC(init_range));
    } else if (may_exist_micro && sub_range_handle.is_valid()) {
      bool all_mem_meta = true;
      SSTryLoadPersistedMicroMetaGuard load_guard(ret, all_mem_meta, init_range, sub_range_handle.get_ptr());

      if (OB_FAIL(ret)) {
      } else if (!all_mem_meta) { // if all_mem_meta=false, we need to load persisted meta into memory
        ObSEArray<ObSSMicroBlockMetaInfo, SS_DEF_ARRAY_CNT> parsed_micro_infos;
        if (OB_FAIL(inner_obtain_persisted_micro_metas(init_range, sub_range_handle.get_ptr(), parsed_micro_infos))) {
          LOG_WARN("fail to obtain persisted micro_metas", KR(ret), KPC(sub_range_handle.get_ptr()));
        } else {
          int64_t micro_info_idx = -1;
          if (ObSSMicroCacheUtil::contain_micro_key(parsed_micro_infos, micro_key, micro_info_idx)) {
            const int64_t parsed_micro_cnt = parsed_micro_infos.count();
            if (OB_FAIL(inner_add_persisted_micro_metas(parsed_micro_infos, micro_info_idx, micro_meta_handle, 
                micro_meta_info))) {
              LOG_WARN("fail to add persisted micro_metas", KR(ret), K(micro_info_idx), K(parsed_micro_cnt));
            } else {
              all_mem_meta = true;
              cache_stat_.range_stat().update_fe_sub_range_cnt(-1);
            }
          }
        }
        parsed_micro_infos.reset();
      } else { // if all_mem_meta=true, we need to get micro_meta from memory directly
        if (OB_FAIL(inner_get_micro_block_meta_directly(micro_key, get_func, micro_meta_info))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("fail to get micro_block_meta directly", KR(ret), K(micro_key), K(get_func));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::inner_obtain_persisted_micro_metas(
    ObSSMicroInitRangeInfo *init_range,
    ObSSMicroSubRangeInfo *sub_range,
    ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char *range_buf = nullptr;
  ObSSMicroSubRangeInfo sub_range_snapshot;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(init_range) || OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range), KP(sub_range));
  } else if (OB_FAIL(init_range->get_sub_range_snapshot_info(sub_range, sub_range_snapshot))) {
    LOG_WARN("fail to get sub_range snapshot info", KR(ret), KP(init_range), KP(sub_range), KPC(init_range),
      KPC(sub_range));
  } else if (OB_UNLIKELY(!sub_range_snapshot.exist_ghost_persisted_meta() || sub_range_snapshot.is_all_mem_meta())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sub_range_snapshot), KPC(sub_range));
  } else {
    const int64_t length = sub_range_snapshot.length_;
    if (OB_ISNULL(range_buf = static_cast<char *>(allocator_.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), KPC(sub_range));
    } else {
      const int64_t phy_blk_idx = sub_range_snapshot.phy_blk_idx_;
      const uint64_t reuse_version = sub_range_snapshot.reuse_version_;
      const int64_t offset = sub_range_snapshot.offset_;
      if (OB_FAIL(SSPhyBlockMgr.read_persisted_sub_range(range_buf, phy_blk_idx, offset, length, reuse_version))) {
        if (OB_S2_REUSE_VERSION_MISMATCH == ret) {
          LOG_ERROR("NOTICE: persisted micro_meta reuse_version mismatch", K(phy_blk_idx), K(offset), K(length),
            K(reuse_version), K(sub_range_snapshot));
        } else {
          LOG_WARN("fail to read persisted sub_range content", KR(ret), K(phy_blk_idx), K(offset), K(length), 
            K(reuse_version), K(sub_range_snapshot), KPC(sub_range));
        }
      } else if (OB_FAIL(ObSSMicroCacheUtil::parse_range_micro_metas(tenant_id_, range_buf, length, block_size_,
                parsed_micro_infos))) {
        LOG_WARN("fail to parse range micro_meta info", KR(ret), K_(tenant_id), KP(range_buf), K(length), K_(block_size),
          KP(sub_range), KPC(sub_range));
      }
    }

    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS; // if we can succ to handle b1 & b2, return succ.
      const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ARC_DELETE;
      const uint64_t b1_pmeta_size = sub_range_snapshot.b1_pmeta_size_;
      const uint64_t b1_pmeta_cnt = sub_range_snapshot.b1_pmeta_cnt_;
      const uint64_t b2_pmeta_size = sub_range_snapshot.b2_pmeta_size_;
      const uint64_t b2_pmeta_cnt = sub_range_snapshot.b2_pmeta_cnt_;
      // handle b1 & b2
      if ((b1_pmeta_size > 0) && (b1_pmeta_cnt > 0)) {
        if (OB_TMP_FAIL(arc_info_.adjust_seg_info(true, true, op_type, b1_pmeta_size, b1_pmeta_cnt))) {
          LOG_ERROR("fail to adjust seg_info", KR(tmp_ret), K(op_type), K(b1_pmeta_size), K(b1_pmeta_cnt));
          ret = tmp_ret;
          tmp_ret = OB_SUCCESS;
        }
      }
      
      if ((b2_pmeta_size > 0) && (b2_pmeta_cnt > 0)) {
        if (OB_TMP_FAIL(arc_info_.adjust_seg_info(false, true, op_type, b2_pmeta_size, b2_pmeta_cnt))) {
          LOG_ERROR("fail to adjust seg_info", KR(tmp_ret), K(op_type), K(b2_pmeta_size), K(b2_pmeta_cnt));
          ret = tmp_ret;
        }
      }
      
      FLOG_INFO("fin handle obtain persisted meta failure", KR(ret), K(b1_pmeta_size), K(b1_pmeta_cnt), K(b2_pmeta_size),
        K(b2_pmeta_cnt), K(sub_range_snapshot), K_(arc_info));
    }

    if (nullptr != range_buf) {
      allocator_.free(range_buf);
    }
  }
  return ret;
}

int ObSSMicroMetaManager::inner_add_persisted_micro_metas(
    const ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos,
    const int64_t micro_info_idx,
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t micro_info_cnt = parsed_micro_infos.count();
  int64_t succ_load_cnt = 0;
  int64_t entry_exist_cnt = 0;
  int64_t fail_load_cnt = 0;
  int64_t fail_load_size = 0;
  int64_t p_entry_exist_cnt = 0;    // pending-delete ghost: micro_map already has key, skip arc_info adjust
  int64_t p_arc_adj_cnt = 0;        // pending-delete ghost: arc_info adjust succ count
  int64_t p_get_fail_cnt = 0;       // pending-delete ghost: micro_map get failed
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(micro_info_cnt > 0 && (micro_info_idx < 0 || micro_info_idx >= micro_info_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_info_idx), K(micro_info_cnt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_info_cnt); ++i) {
    const ObSSMicroBlockMetaInfo &cur_micro_info = parsed_micro_infos.at(i);
    if (OB_UNLIKELY(cur_micro_info.is_meta_deleted())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected micro_info", KR(ret), K(i), K(cur_micro_info));
    // NOTICE: now, only support ghost persisted micro_meta re-added.
    // Thus, don't need to get @micro_meta_handle
    } else if (cur_micro_info.is_in_ghost()) {
      // Check if this meta belongs to a tablet_id that is pending deletion
      ObTabletID tablet_id(cur_micro_info.effective_tablet_id());
      bool need_delete = false;
      if (OB_FAIL(is_tablet_need_delete(tablet_id, need_delete))) {
        LOG_WARN("fail to check if tablet need to be deleted", KR(ret), K(tablet_id));
      } else if (need_delete) {
        // If the same micro_key already exists in memory, the persisted ghost snapshot is stale;
        // skip arc_info adjust to avoid double-decrement (same idea as OB_ENTRY_EXIST in do_add).
        const ObSSMicroBlockCacheKey &ghost_micro_key = cur_micro_info.get_micro_key();
        if (OB_TMP_FAIL(check_micro_key_exist(ghost_micro_key))) {
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            const bool is_in_l1 = cur_micro_info.is_in_l1();
            const int64_t micro_size = cur_micro_info.size();
            const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_FORCE_ERASE;
            tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(arc_info_.adjust_seg_info(is_in_l1, true/*is_in_ghost*/, op_type, micro_size, 1))) {
              LOG_ERROR("fail to adjust seg_info for pending delete meta", KR(tmp_ret), K(is_in_l1), K(micro_size), K(tablet_id));
            } else {
              ++p_arc_adj_cnt;
            }
          } else {
            LOG_WARN("fail to check micro_meta map for pending delete meta", KR(tmp_ret), K(ghost_micro_key), K(tablet_id));
            ++p_get_fail_cnt;
          }
          tmp_ret = OB_SUCCESS;
        } else {
          // Memory already holds this key; do not adjust arc_info for stale persisted ghost.
          ++p_entry_exist_cnt;
        }
      } else {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        if (OB_FAIL(alloc_micro_block_meta(tmp_micro_handle))) {
          LOG_WARN("fail to alloc micro_block_meta", KR(ret));
        } else if (OB_UNLIKELY(!tmp_micro_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_meta_handle should be valid", KR(ret), K(tmp_micro_handle));
        } else if (OB_FAIL(tmp_micro_handle()->assign_micro_info_for_loading(cur_micro_info))) {
          LOG_WARN("fail to assign micro_meta_info for loading", KR(ret), K(cur_micro_info));
        } else if (i == micro_info_idx) {
          tmp_micro_handle()->get_micro_meta_info(micro_meta_info);
        }
        
        if (FAILEDx(do_add_persisted_micro_block_meta(tmp_micro_handle))) {
          if (OB_UNLIKELY(!cur_micro_info.is_in_ghost())) {
            LOG_ERROR("unexpected: persisted micro_meta is not ghost when loading", K(i),K(cur_micro_info));
          } else if (OB_ENTRY_EXIST == ret) {
            LOG_INFO("ghost persisted micro_meta already exists in memory when loading from disk", K(i), K(cur_micro_info));
            ++entry_exist_cnt;
            cache_stat_.task_stat().update_entry_exist_persisted_cnt(1);
          } else {
            LOG_WARN("fail to add persisted micro_block_meta", KR(ret), K(i), KPC(tmp_micro_handle.get_ptr()));
            // if fail to add, we need to adjust arc_info to delete this ghost persisted micro_meta
            const bool is_in_l1 = cur_micro_info.is_in_l1();
            const int64_t micro_size = cur_micro_info.size();
            const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_ARC_DELETE;
            if (OB_TMP_FAIL(arc_info_.adjust_seg_info(is_in_l1, true/*is_in_ghost*/, op_type, micro_size, 1))) {
              LOG_ERROR("fail to adjust seg_info", KR(tmp_ret), K(is_in_l1), K(micro_size));
              tmp_ret = OB_SUCCESS;
            }
            ++fail_load_cnt;
            fail_load_size += micro_size;
          }
        } else {
          ++succ_load_cnt;
        }

        if (OB_FAIL(ret)) {
          micro_meta_info.reset();
          micro_meta_handle.reset();
        }
      }
    }
    ret = OB_SUCCESS; // continue to handle next
  }

  cache_stat_.task_stat().update_readd_persisted_cnt(succ_load_cnt);
  if (succ_load_cnt > 0 || entry_exist_cnt > 0 || fail_load_cnt > 0 
      || p_entry_exist_cnt > 0 || p_arc_adj_cnt > 0 || p_get_fail_cnt > 0) {
    FLOG_INFO("finish readd persisted ghost micro_metas", K(micro_info_cnt), K(succ_load_cnt), K(entry_exist_cnt),
      K(fail_load_cnt), K(fail_load_size), K(p_entry_exist_cnt), K(p_arc_adj_cnt), K(p_get_fail_cnt));
  }
  return ret;
}

int ObSSMicroMetaManager::inner_add_persisted_micro_metas(
    const ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos)
{
  int ret = OB_SUCCESS;
  if (parsed_micro_infos.count() > 0) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMicroBlockMetaInfo micro_meta_info;
    int64_t micro_info_idx = 0;
    if (OB_FAIL(inner_add_persisted_micro_metas(parsed_micro_infos, micro_info_idx, micro_meta_handle,
        micro_meta_info))) {
      LOG_WARN("fail to add persisted micro_metas", KR(ret), "parsed_micro_cnt", parsed_micro_infos.count());
    }
  }
  return ret;
}

int ObSSMicroMetaManager::record_need_delete_tablet(const ObTabletID &tablet_id, const int64_t micro_ckpt_op_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || micro_ckpt_op_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(micro_ckpt_op_cnt));
  } else {
    // Insert or update tablet_id and micro_ckpt_op_cnt (overwrite if exists)
    if (OB_FAIL(tablets_to_delete_.set_refactored(tablet_id, micro_ckpt_op_cnt, true/*overwrite*/))) {
      LOG_WARN("fail to set tablet_id into tablets_to_delete_", KR(ret), K(tablet_id), K(micro_ckpt_op_cnt));
    } else {
      cache_stat_.micro_stat().set_pending_delete_tablet_cnt(tablets_to_delete_.size());
      LOG_INFO("record tablet_id for deletion", K(tablet_id), K(micro_ckpt_op_cnt));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::is_tablet_need_delete(const ObTabletID &tablet_id, bool &need_delete) const
{
  int ret = OB_SUCCESS;
  need_delete = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    int64_t ckpt_op_cnt = 0;
    if (OB_FAIL(tablets_to_delete_.get_refactored(tablet_id, ckpt_op_cnt))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get tablet_id from tablets_to_delete map", KR(ret), K(tablet_id));
      }
    } else {
      need_delete = true;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::check_and_cleanup_pending_delete_tablets(
     const int64_t cur_micro_ckpt_op_cnt,
     const int64_t meta_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(cur_micro_ckpt_op_cnt < 0 || meta_ckpt_split_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cur_micro_ckpt_op_cnt), K(meta_ckpt_split_cnt));
  } else {
    // First, collect all tablet_ids that need to be deleted
    ObSEArray<ObTabletID, DEFAULT_TABLETS_NUM> tablets_to_cleanup;
    common::hash::ObHashMap<ObTabletID, int64_t>::iterator iter = tablets_to_delete_.begin();
    for (; OB_SUCC(ret) && (iter != tablets_to_delete_.end()); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      const int64_t recorded_ckpt_op_cnt = iter->second;
      if (ObSSMicroCacheUtil::should_delete_tablet_record(
          recorded_ckpt_op_cnt, cur_micro_ckpt_op_cnt, meta_ckpt_split_cnt)) {
        if (OB_FAIL(tablets_to_cleanup.push_back(tablet_id))) {
          LOG_WARN("fail to push tablet_id to cleanup array", KR(ret), K(tablet_id));
        }
      }
    }

    // Then, delete collected tablet_ids with double check
    SSMicroCacheTabletMapEraseFunc erase_func(cur_micro_ckpt_op_cnt, meta_ckpt_split_cnt);
    for (int64_t i = 0; OB_SUCC(ret) && (i < tablets_to_cleanup.count()); ++i) {
      const ObTabletID &tablet_id = tablets_to_cleanup.at(i);
      bool is_erased = false;
      if (OB_FAIL(tablets_to_delete_.erase_if(tablet_id, erase_func, is_erased))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to erase tablet_id from tablets_to_delete map", KR(ret), K(tablet_id));
        }
      } else if (is_erased) {
        LOG_INFO("cleanup completed tablet_id from tablets_to_delete map",
                 K(tablet_id), K(cur_micro_ckpt_op_cnt), K(meta_ckpt_split_cnt));
      } else {
        // The tablet_id was updated with a newer ckpt_op_cnt, skip deletion this round
        LOG_INFO("tablet_id was updated, skip cleanup this round", K(tablet_id),
                 K_(erase_func.recorded_ckpt_op_cnt), K(cur_micro_ckpt_op_cnt), K(meta_ckpt_split_cnt));
      }
    }
    cache_stat_.micro_stat().set_pending_delete_tablet_cnt(tablets_to_delete_.size());
  }
  return ret;
}

int ObSSMicroMetaManager::update_micro_stat_from_arc()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    int64_t t1_cnt = 0;
    int64_t t1_size = 0;
    int64_t t2_cnt = 0;
    int64_t t2_size = 0;
    int64_t b1_cnt = 0;
    int64_t b1_size = 0;
    int64_t b2_cnt = 0;
    int64_t b2_size = 0;
    arc_info_.get_seg_info(SS_ARC_T1, t1_size, t1_cnt);
    arc_info_.get_seg_info(SS_ARC_T2, t2_size, t2_cnt);
    arc_info_.get_seg_info(SS_ARC_B1, b1_size, b1_cnt);
    arc_info_.get_seg_info(SS_ARC_B2, b2_size, b2_cnt);

    const int64_t valid_cnt = t1_cnt + t2_cnt;
    const int64_t valid_size = t1_size + t2_size;
    const int64_t total_cnt = t1_cnt + t2_cnt + b1_cnt + b2_cnt;
    const int64_t total_size = t1_size + t2_size + b1_size + b2_size;
    const int64_t map_cnt = micro_meta_map_.count();
    cache_stat_.micro_stat().update_micro_map_valid_info(valid_cnt, valid_size);
    cache_stat_.micro_stat().set_total_micro_info(total_cnt, total_size);
    cache_stat_.micro_stat().set_mark_del_micro_info(0, 0);
    cache_stat_.micro_stat().set_mark_del_ghost_micro_info(0, 0);
    cache_stat_.micro_stat().set_max_micro_meta_cnt(map_cnt);
    cache_stat_.micro_stat().set_pending_delete_tablet_cnt(tablets_to_delete_.size());
    cache_stat_.mem_stat().set_micro_alloc_cnt(map_cnt);
    cache_stat_.mem_stat().set_micro_map_usage(map_cnt * SS_MICRO_META_MAP_ITEM_SIZE);
  }
  return ret;
}

int ObSSMicroMetaManager::traverse_meta_for_calibration(
    common::ObArray<int64_t> &expected_valid_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    SSCalibrationTraverseMetaFunc functor(this, expected_valid_len, block_size_);
    // NOTE: remove_if may erase entries (invalid / link-fail metas), not only meta_ckpt path.
    // Only return error when allocate memory failed, otherwise return success.
    if (OB_FAIL(micro_meta_map_.remove_if(functor))) {
      LOG_WARN("fail to traverse meta for calibration", KR(ret), K_(tenant_id));
    } else {
      ret = functor.ret_;
      FLOG_INFO("finish traverse meta for calibration", KR(ret),
          "link_fail_cnt", functor.link_fail_cnt_,
          "fail_alloc_cnt", functor.fail_alloc_cnt_,
          "processed_cnt", functor.processed_cnt_,
          "erased_cnt", functor.erased_cnt_);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
