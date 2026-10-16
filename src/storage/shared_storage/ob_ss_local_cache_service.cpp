/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "common/ob_version_def.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/shared_storage/task/ob_ss_write_lcache_task.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;


int get_evict_old_sstable_policy(
    const ObString &policy_str,
    ObSSLocalCacheEvictOldSSTablePolicy &policy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(policy_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid policy string", KR(ret), K(policy_str));
  } else if (0 == policy_str.case_compare("OFF")) {
    policy = ObSSLocalCacheEvictOldSSTablePolicy::OFF;
  } else if (0 == policy_str.case_compare("MAJOR_SSTABLE")) {
    policy = ObSSLocalCacheEvictOldSSTablePolicy::MAJOR_SSTABLE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid policy string", KR(ret), K(policy_str));
  }
  return ret;
}


/*-----------------------------ObSSMacroCachePrewarmEntryUpdateFn------------------------------*/
void ObSSMacroCachePrewarmEntryUpdateFn::operator()(SSMacroCachePrewarmMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCachePrewarmEntry &prewarm_entry = entry.second;
  prewarm_entry.cache_miss_cnt_ += 1;
  prewarm_entry.timestamp_us_ = ObTimeUtility::current_time_us();

  ObSSLocalCacheService *local_cache_service = nullptr;
  int64_t cache_miss_threshold = 0;
  if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local_cache_service should be valid", KR(ret), KP(local_cache_service));
  } else if (OB_UNLIKELY(threshold_shrink_factor_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("threshold_shrink_factor is unexpected", KR(ret), K_(threshold_shrink_factor));
  } else {
    cache_miss_threshold = MAX(1, (local_cache_service->get_miss_threshold_for_prefetch() / threshold_shrink_factor_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(cache_miss_threshold < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache_miss_threshold is unexpected", KR(ret), K(cache_miss_threshold));
  } else {
    const MacroBlockId &macro_id = entry.first;
    bool need_prewarm = false;
    if (is_support_preread_meta(macro_id) && 
        prewarm_entry.cache_miss_cnt_ >= 1 && 
        !prewarm_entry.is_prewarmed_) {
      // For meta macro blocks, the cache_miss_threshold is fixed as 1.
      // Currently we only handle meta blocks which can be iterated and splitted to micro blocks.
      need_prewarm = true;
    } else if (macro_id.is_data() && 
              (prewarm_entry.cache_miss_cnt_ >= cache_miss_threshold) && 
              !prewarm_entry.is_prewarmed_) {
      need_prewarm = true;
    }


    if (need_prewarm) {
      int64_t preread_queue_size = 0;
      ObTenantFileManager *tnt_file_mgr = nullptr;
      if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant file manager is null", KR(ret));
      } else if (FALSE_IT(preread_queue_size = tnt_file_mgr->get_preread_cache_mgr().get_preread_queue_size())) {
      } else if (preread_queue_size >= ObPrereadCacheManager::PREREAD_QUEUE_SIZE_THRESHOLD) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          LOG_INFO("preread queue is busy, do not add preread entry", K(preread_queue_size));
        }
      } else if (OB_FAIL(tnt_file_mgr->push_to_preread_queue(macro_id, effective_tablet_id_))) {
        LOG_WARN("fail to push to preread queue", KR(ret), K(macro_id), K_(effective_tablet_id));
      } else {
        prewarm_entry.is_prewarmed_ = true;
      }
    }
  }
  ret_ = ret;
}

/*-----------------------------ObSSLocalCacheParams------------------------------*/
ObSSLocalCacheParams::ObSSLocalCacheParams()
  : local_cache_control_mode_(), miss_threshold_for_prefetch_(DEF_MISS_THRESHOLD_FOR_PREFETCH),
    disk_space_cal_interval_us_(DEF_DISK_SPACE_CAL_INTERVAL_US),
    macro_cache_min_admit_block_size_(DEF_MACRO_CACHE_MIN_ADMIT_BLOCK_SIZE),
    evict_old_sstable_policy_(DEF_EVICT_OLD_SSTABLE_POLICY)
{
}

ObSSLocalCacheParams::~ObSSLocalCacheParams()
{
  reset();
}

void ObSSLocalCacheParams::reset()
{
  evict_old_sstable_policy_ = DEF_EVICT_OLD_SSTABLE_POLICY;
  disk_space_cal_interval_us_ = DEF_DISK_SPACE_CAL_INTERVAL_US;
  miss_threshold_for_prefetch_ = DEF_MISS_THRESHOLD_FOR_PREFETCH;
  macro_cache_min_admit_block_size_ = DEF_MACRO_CACHE_MIN_ADMIT_BLOCK_SIZE;
}


/*-----------------------------------------ObSSLocalCacheService-----------------------------------------*/
ObSSLocalCacheService::ObSSLocalCacheService()
  : is_inited_(false), is_stopped_(false), mid_tg_id_(INVALID_TG_ID), high_tg_id_(INVALID_TG_ID),
    local_cache_params_(), local_cache_param_refresh_task_(), macro_cache_prewarm_map_(),
    prewarm_map_clean_task_(is_stopped_), local_cache_tablet_stat_map_(),
    local_cache_stat_clean_task_(), object_type_cached_stat_refresh_task_(), local_cache_stat_(),
    write_lcache_task_(), local_cache_diagnose_stat_()
{
  reset_cache_stat();
}

void ObSSLocalCacheService::reset_cache_stat()
{
  for (uint8_t i = 0; i < SS_OBJECT_MAX_TYPE_VAL; i++) {
    object_type_remote_stats_[i].reset();
    object_type_local_stats_[i].reset();
    object_type_cached_local_stats_[i].reset();
    object_type_cached_remote_stats_[i].reset();
  }
  local_cache_stat_.reset();
  local_cache_diagnose_stat_.reset();
}

int ObSSLocalCacheService::mtl_init(ObSSLocalCacheService *&local_cache)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(local_cache->init())) {
    LOG_WARN("fail to init ss_local_cache", KR(ret));
  }
  return ret;
}

bool ObSSLocalCacheService::is_need_update_hit_stat()
{
  bool b_ret = true;
  // skip update hit stat for compaction, which may directly read from object storage by design
  if ((GET_FUNC_TYPE() == static_cast<uint8_t>(ObFunctionType::PRIO_COMPACTION_HIGH))
      || (GET_FUNC_TYPE() == static_cast<uint8_t>(ObFunctionType::PRIO_COMPACTION_MID))
      || (GET_FUNC_TYPE() == static_cast<uint8_t>(ObFunctionType::PRIO_COMPACTION_LOW))) {
    b_ret = false;
  }
  return b_ret;
}

int ObSSLocalCacheService::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr1(MTL_ID(), "MacroPWMap");
  // 9w buckets. actually cal_next_prime return 9.8w buckets, which cost 2.3MB memory
  const int64_t prewarm_map_bucket_num = 90000;
  // tablet_stat_bucket_num depends on tenant_mem
  int64_t tablet_stat_bucket_num = -1;
  const int64_t GB = 1 * 1024L * 1024L * 1024L;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  if ((tenant_mem / GB) <= 4) { // tenant_mem <= 4GB
    tablet_stat_bucket_num = 90000;
  } else { // tenant_mem > 4GB
    tablet_stat_bucket_num = 1000000;
  }
  ObMemAttr attr2(MTL_ID(), "LocalCacheTSMap");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_FAIL(local_cache_param_refresh_task_.init(MTL_ID()))) {
    LOG_WARN("fail to init local cache param refresh task", KR(ret));
  } else if (OB_FAIL(macro_cache_prewarm_map_.create(prewarm_map_bucket_num, attr1))) {
    LOG_WARN("fail to create macro cache prewarm map", KR(ret));
  } else if (OB_FAIL(prewarm_map_clean_task_.init(this))) {
    LOG_WARN("fail to init prewarm map clean task", KR(ret));
  } else if (OB_FAIL(local_cache_stat_clean_task_.init(MTL_ID()))) {
    LOG_WARN("fail to init local cache tablet stat map clean task", KR(ret));
  } else if (OB_FAIL(local_cache_tablet_stat_map_.create(tablet_stat_bucket_num, attr2))) {
    LOG_WARN("fail to create local cache tablet stat map", KR(ret));
  } else if (OB_FAIL(object_type_cached_stat_refresh_task_.init(MTL_ID()))) {
    LOG_WARN("fail to init object type cached stat refresh task", KR(ret));
  } else if (OB_FAIL(write_lcache_task_.init())) {
    LOG_WARN("fail to init write lcache task", KR(ret));
  } else if (OB_FAIL(local_cache_diagnose_stat_.init(MTL_ID()))) {
    LOG_WARN("fail to init local cache diagnose stat", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLocalCacheService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LoCacheMTimer, mid_tg_id_))) {
    LOG_WARN("fail to create mid timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(mid_tg_id_))) {
    LOG_WARN("fail to start mid timer thread", KR(ret), K_(mid_tg_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LoCacheHTimer, high_tg_id_))) {
    LOG_WARN("fail to create high timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(high_tg_id_))) {
    LOG_WARN("fail to start high timer thread", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(local_cache_param_refresh_task_.start(mid_tg_id_))) {
    LOG_WARN("fail to start local cache param refresh task", KR(ret), K_(mid_tg_id));
  } else if (OB_FAIL(prewarm_map_clean_task_.start(mid_tg_id_))) {
    LOG_WARN("fail to start prewarm map clean task", KR(ret), K_(mid_tg_id));
  } else if (OB_FAIL(local_cache_stat_clean_task_.start(mid_tg_id_))) {
    LOG_WARN("fail to start local cache tablet stat map clean task", KR(ret), K_(mid_tg_id));
  } else if (OB_FAIL(object_type_cached_stat_refresh_task_.start(mid_tg_id_))) {
    LOG_WARN("fail to start object type cached stat refresh task", KR(ret), K_(mid_tg_id));
  } else if (OB_FAIL(write_lcache_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start write lcache task", KR(ret), K_(high_tg_id));
  } else {
    LOG_INFO("succ to start local cache service", K_(mid_tg_id), K_(high_tg_id));
  }
  return ret;
}

void ObSSLocalCacheService::stop()
{
  LOG_INFO("start to stop local cache service");
  is_stopped_ = true;
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_STOP(mid_tg_id_);
  }
  if (INVALID_TG_ID != high_tg_id_) {
    TG_STOP(high_tg_id_);
  }
  LOG_INFO("finish to stop local cache service");
}

int ObSSLocalCacheService::wait()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  FLOG_INFO("start to wait local cache service");
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_WAIT(mid_tg_id_);
  }
  if (INVALID_TG_ID != high_tg_id_) {
    TG_WAIT(high_tg_id_);
  }
  // 1. free memory of entry in write_lcache_queue_.
  // 2. destructs storage_object_handles when destructs ObSSWriteLCacheEntry. This ensures that
  // ObPCachedExternalFileService::destroy() can successfully wait until all io are finished.
  write_lcache_task_.clear_write_lcache_queue();
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("finish to wait local cache service", KR(ret), K(cost_us));
  return ret;
}

void ObSSLocalCacheService::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to destroy local cache service");
  is_stopped_ = true;
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_DESTROY(mid_tg_id_);
  }
  if (INVALID_TG_ID != high_tg_id_) {
    TG_DESTROY(high_tg_id_);
  }
  mid_tg_id_ = INVALID_TG_ID;
  high_tg_id_ = INVALID_TG_ID;
  reset_cache_stat();
  if (IS_INIT) {
    write_lcache_task_.destroy();
    object_type_cached_stat_refresh_task_.destroy();
    local_cache_stat_clean_task_.destroy();
    prewarm_map_clean_task_.destroy();
    local_cache_tablet_stat_map_.destroy();
    macro_cache_prewarm_map_.destroy();
    local_cache_param_refresh_task_.destroy();
    local_cache_params_.reset();
    is_inited_ = false;
  }
  FLOG_INFO("finish to destroy local cache service");
}

void ObSSLocalCacheService::update_macro_cache_tablet_id(
    const ObIArray<MacroBlockId> &macro_id_arr,
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_cache_mgr should not be null", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->update_effective_tablet_id(macro_id_arr, effective_tablet_id))) {
    LOG_WARN("fail to update effective tablet_id in ss_macro_cache", KR(ret), K(effective_tablet_id),
      "macro_cnt", macro_id_arr.count(), K(macro_id_arr));
  }
}

int ObSSLocalCacheService::refresh_macro_cache_prewarm_map(
    const MacroBlockId &macro_id,
    const ObTabletID &effective_tablet_id,
    const int64_t threshold_shrink_factor)
{
  int ret = OB_SUCCESS;
  ObSSMacroCachePrewarmEntry prewarm_entry(effective_tablet_id);
  ObSSMacroCachePrewarmEntryUpdateFn update_fn(effective_tablet_id, threshold_shrink_factor);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (macro_cache_prewarm_map_.size() >= MACRO_CACHE_PREWARM_MAP_MAX_SIZE) {
    // 1. map oversize, update entries that are already exist in map
    LOG_TRACE("macro cache prewarm map is oversize");
    if (OB_FAIL(macro_cache_prewarm_map_.atomic_refactored(macro_id, update_fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
      } else {
        LOG_WARN("fail to atomic refactored", KR(ret), K(macro_id), K(update_fn));
      }
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = update_fn.get_ret()))) {
      LOG_WARN("fail to execute update_fn", KR(ret), K(update_fn));
    }
  } else {
    // 2. map not oversize, set or update
    // When cache_miss_cnt_ is 1, it may trigger the generation of a pre-read task. 
    // Since set_or_update can only be used for updates, and set_callback cannot modify the state of the value, 
    // it is split into set_refactored+atomic_refactored to ensure that the value state update can be triggered at the first miss.
     if (OB_FAIL(macro_cache_prewarm_map_.set_refactored(macro_id, prewarm_entry))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
      } else {
        LOG_WARN("fail to set refactored", KR(ret), K(macro_id));
      }
    } 
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_cache_prewarm_map_.atomic_refactored(macro_id, update_fn))) {
      LOG_WARN("fail to atomic refactored", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(OB_SUCCESS != (ret = update_fn.get_ret()))) {
      LOG_WARN("fail to execute update_fn", KR(ret), K(update_fn));
    }
  }
  return ret;
}

int ObSSLocalCacheService::do_prewarm_map_clean_work()
{
  int ret = OB_SUCCESS;
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 1024;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "PreMapClean"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else {
    SSMacroCachePrewarmMap::bucket_iterator bucket_iter = macro_cache_prewarm_map_.bucket_begin();
    while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != macro_cache_prewarm_map_.bucket_end())) {
      {
        SSMacroCachePrewarmMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        SSMacroCachePrewarmMap::hashtable::readlocker locker(blk.lock());
        SSMacroCachePrewarmMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
          const MacroBlockId &cur_macro_id = node_iter->first;
          const ObSSMacroCachePrewarmEntry &cur_prewarm_entry = node_iter->second;
          if (ObTimeUtility::current_time_us() - cur_prewarm_entry.timestamp_us_ > PREWARM_ENTRY_EXPIRATION_TIME_US) {
            if (OB_FAIL(macro_ids.push_back(cur_macro_id))) {
              LOG_WARN("fail to push back", KR(ret), K(cur_macro_id));
            }
          }
          node_iter++;
        }
        ++bucket_iter;
        ++bucket_cnt;
      }
      if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt)) {
        if (OB_FAIL(batch_erase_prewarm_entry(macro_ids))) {
          LOG_WARN("fail to batch erase prewarm entry", KR(ret), K(macro_ids));
        }
        macro_ids.reuse();
        bucket_cnt = 0;
      }
    }
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(batch_erase_prewarm_entry(macro_ids))) {
        LOG_WARN("fail to batch erase prewarm entry", KR(ret), K(macro_ids));
      }
    }
  }
  return ret;
}

int ObSSLocalCacheService::batch_erase_prewarm_entry(const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  // inner function, no need to judge is_inited_ and is_stopped_ again
  const int64_t macro_id_cnt = macro_ids.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_cnt); ++i) {
    if (OB_FAIL(macro_cache_prewarm_map_.erase_refactored(macro_ids.at(i)))) {
      LOG_WARN("fail to erase refactored", KR(ret), "macro_id", macro_ids.at(i));
    }
  }
  return ret;
}

void ObSSLocalCacheService::set_local_cache_control_mode(const ObSSLocalCacheControlMode &mode)
{
  local_cache_params_.local_cache_control_mode_.assign(mode);

  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "tenant macro block KVCACHE mgr is null");
  } else {
    const bool is_kvcache_enabled = local_cache_params_.local_cache_control_mode_.is_mem_macro_cache_enable();
    if (is_kvcache_enabled != tenant_kvcache_mgr->is_cache_enabled()) {
      if (is_kvcache_enabled) {
        tenant_kvcache_mgr->enable_cache();
      } else {
        tenant_kvcache_mgr->disable_cache();
      }
      FLOG_INFO("update macro block KVCACHE control mode", K(is_kvcache_enabled));
    }
  }
}

bool ObSSLocalCacheService::is_macro_read_cache_disabled() const
{
  return !local_cache_params_.local_cache_control_mode_.is_macro_read_cache_enable();
}

bool ObSSLocalCacheService::is_macro_write_cache_disabled() const
{
  return !local_cache_params_.local_cache_control_mode_.is_macro_write_cache_enable();
}

int ObSSLocalCacheService::refresh_local_cache_tablet_stat_map(
    const ObTabletID &effective_tablet_id,
    const bool &is_hit,
    const int64_t &size)
{
  int ret = OB_SUCCESS;
  ObSSTabletHitStatEntryUpdateFn update_fn(is_hit, size);
  ObStorageCacheHitStat stat_entry(is_hit, 1/*delta_cnt*/, size);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!effective_tablet_id.is_valid())) {
  } else if (is_need_update_hit_stat()) {
    if (OB_FAIL(local_cache_tablet_stat_map_.set_or_update(effective_tablet_id.id(), stat_entry, update_fn))) {
      LOG_WARN("fail to set or update local cache tablet stat map", KR(ret), K(effective_tablet_id));
    }
  } else { // !is_need_update_hit_stat
    // do nothing
  }
  return ret;
}

int ObSSLocalCacheService::get_local_cache_tablet_stat(
    const ObTabletID &effective_tablet_id,
    ObStorageCacheHitStat &stat_entry)
{
  int ret = OB_SUCCESS;
  stat_entry.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!effective_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(effective_tablet_id));
  } else if (OB_FAIL(local_cache_tablet_stat_map_.get_refactored(effective_tablet_id.id(), stat_entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. if no record, stat_entry is just 0.
      LOG_INFO("No local cache stat of this tablet", KR(ret), K(effective_tablet_id));
    } else {
      LOG_WARN("fail to get local_cache_tablet_stat from local_cache_tablet_stat_map", KR(ret), K(effective_tablet_id));
    }
  }
  return ret;
}

int ObSSLocalCacheService::filter_repeated_local_cache_keys(ObHALocalCacheKeys &local_cache_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!local_cache_keys.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(local_cache_keys));
  } else if (local_cache_keys.job_type_ == ObHACacheJobType::MACRO_CACHE_JOB_TYPE) {
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    common::ObSArray<ObSSMacroPrewarmMeta> &macro_meta_array = local_cache_keys.macro_meta_array_;

    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr should not be nullptr", KR(ret), KP(macro_cache_mgr));
    } else {
      int64_t new_macro_meta_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < macro_meta_array.count(); ++i) {
        const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array.at(i);
        bool is_exist = false;
        if (OB_FAIL(macro_cache_mgr->exist(cur_meta.macro_id_, is_exist))) {
          LOG_WARN("failed to exist macro cache", KR(ret), K(cur_meta));
        } else if (is_exist) {
          // do nothing
        } else if (OB_FAIL(macro_meta_array.at(new_macro_meta_count).assign(cur_meta))) {
          LOG_WARN("failed to assign new meta", KR(ret), K(cur_meta));
        } else {
          new_macro_meta_count++;
        }
      }
      if (OB_SUCC(ret)) {
        while (macro_meta_array.count() > new_macro_meta_count) {
          macro_meta_array.pop_back();
        }
      }
    }
  } else {
    // should not reach here
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("job type is unexpected", KR(ret), K(local_cache_keys));
  }
  return ret;
}

int ObSSLocalCacheService::get_local_cache_retention_rate(
    const ObSSLocalCacheLSInfo &cache_info,
    const uint64_t min_cluster_version,
    LocalCacheRetentionRateGroup &retention_rate_group)
{
  int ret = OB_SUCCESS;
  int64_t macro_cache_free_size = 0;
  int64_t macro_cache_fifo_size = 0;
  int64_t hot_tablet_macro_cache_free_size = 0;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantDiskSpaceManager *tenant_disk_space_manager = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be nullptr", KR(ret), KP(macro_cache_mgr));
  } else if (OB_ISNULL(tenant_disk_space_manager = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager should not be nullptr", KR(ret), KP(tenant_disk_space_manager));
  } else if (FALSE_IT(macro_cache_free_size = tenant_disk_space_manager->get_unit_config_shared_macro_cache_free_size())) {
  } else if (OB_FAIL(macro_cache_mgr->get_fifo_size_from_ls_cache_info(macro_cache_fifo_size))) {
    LOG_WARN("failed to get macro cache fifo size", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->get_max_free_size_of_cache_type(
             ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_tablet_macro_cache_free_size))) {
    LOG_WARN("failed to get hot tablet macro block max free size", KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(retention_rate_group.macro_cache_retention_rate_.calc_macro_cache_retention_rate(
                     cache_info, macro_cache_free_size, macro_cache_fifo_size,
                     hot_tablet_macro_cache_free_size, min_cluster_version))) {
    LOG_WARN("failed to calc macro cache retention rate", KR(ret), K(cache_info), K(macro_cache_free_size),
             K(macro_cache_fifo_size), K(hot_tablet_macro_cache_free_size), K(min_cluster_version));
  }
  return ret;
}

int ObSSLocalCacheService::update_object_type_stat_by_stat_type(const ObStorageObjectType &object_type,
  const ObSSObjectTypeStatType &stat_type, const int result, const int64_t delta_cnt,
  const int64_t delta_size, const int64_t delta_time_us, ObSSObjectTypeStat object_type_stats[])
{
  int ret = OB_SUCCESS;
  uint8_t idx = static_cast<uint8_t>(object_type);
  if (OB_UNLIKELY(idx >= SS_OBJECT_MAX_TYPE_VAL) || OB_UNLIKELY(stat_type >= ObSSObjectTypeStatType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss object type or stat type", KR(ret), K(object_type), K(stat_type));
  } else {
    object_type_stats[idx].add_base_info(stat_type, delta_cnt, delta_size, delta_time_us);
    if (result != OB_SUCCESS) {
      object_type_stats[idx].add_fail_info(stat_type, delta_cnt);
    }
  }
  return ret;
}

int ObSSLocalCacheService::update_object_type_stat(const ObStorageObjectType &object_type,
  const ObSSObjectTypeStatType &stat_type, const bool is_remote, const int result, const int64_t delta_cnt,
  const int64_t delta_size, const int64_t delta_time_us)
{
  int ret = OB_SUCCESS;
  uint8_t idx = static_cast<uint8_t>(object_type);
  uint8_t stat_idx = static_cast<uint8_t>(stat_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(idx >= SS_OBJECT_MAX_TYPE_VAL) || OB_UNLIKELY(stat_idx >= SS_OBJECT_TYPE_MAX_STAT_VAL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss object type or stat type", KR(ret), K(idx), "storage object type", STI(object_type), K(stat_idx));
  } else if (is_remote) {
    if (OB_FAIL(update_object_type_stat_by_stat_type(object_type, stat_type, result, delta_cnt, delta_size,
                                                     delta_time_us, object_type_remote_stats_))) {
      LOG_WARN("failed to update object type remote stat by stat type", KR(ret), K(object_type), K(stat_type),
        K(result), K(delta_cnt), K(delta_size), K(delta_time_us));
    }
  } else {
    if (OB_FAIL(update_object_type_stat_by_stat_type(object_type, stat_type, result, delta_cnt, delta_size,
                                                     delta_time_us, object_type_local_stats_))) {
      LOG_WARN("failed to update object type localstat by stat type", KR(ret), K(object_type), K(stat_type),
        K(result), K(delta_cnt), K(delta_size), K(delta_time_us));
    }
  }
  return ret;
}

int ObSSLocalCacheService::refresh_object_type_cached_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else {
    for (uint8_t i = 0; i < SS_OBJECT_MAX_TYPE_VAL; i++) {
      ObSSObjectTypeStat object_type_local_stat = object_type_local_stats_[i];
      ObSSObjectTypeStat object_type_remote_stat = object_type_remote_stats_[i];
      for (uint8_t j = 0; j < SS_OBJECT_TYPE_MAX_STAT_VAL; j++) {
        ObSSBaseStat stat;
        object_type_local_stat.get_stat(static_cast<ObSSObjectTypeStatType>(j), stat);
        object_type_cached_local_stats_[i].update_cached_stat(static_cast<ObSSObjectTypeStatType>(j),
          stat.get_time_us(), stat.get_cnt(), stat.get_size());
        object_type_remote_stat.get_stat(static_cast<ObSSObjectTypeStatType>(j), stat);
        object_type_cached_remote_stats_[i].update_cached_stat(static_cast<ObSSObjectTypeStatType>(j),
          stat.get_time_us(), stat.get_cnt(), stat.get_size());
      }
    }
  }
  return ret;
}

int ObSSLocalCacheService::get_object_type_stat(const ObStorageObjectType &object_type, const bool is_remote, ObSSObjectTypeStat &type_stat)
{
  int ret = OB_SUCCESS;
  uint8_t idx = static_cast<uint8_t>(object_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(idx >= SS_OBJECT_MAX_TYPE_VAL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss object type", KR(ret), K(idx), "storage object type", STI(object_type));
  } else {
    if (is_remote) {
      type_stat = object_type_remote_stats_[idx];
    } else {
      type_stat = object_type_local_stats_[idx];
    }
  }
  return ret;
}

int ObSSLocalCacheService::get_object_type_cached_stat(const ObStorageObjectType &object_type,
                                                       const bool is_remote, ObSSObjectTypeCachedStat &cached_stat)
{
  int ret = OB_SUCCESS;
  uint8_t idx = static_cast<uint8_t>(object_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(idx >= SS_OBJECT_MAX_TYPE_VAL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss object type", KR(ret), K(idx), "storage object type", STI(object_type));
  } else {
    if (is_remote) {
      cached_stat = object_type_cached_remote_stats_[idx];
    } else {
      cached_stat = object_type_cached_local_stats_[idx];
    }
  }
  return ret;
}

int ObSSLocalCacheService::check_exist_in_all_macro_cache(const MacroBlockId &macro_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
  } else if (OB_FAIL(
      tenant_kvcache_mgr->check_exist(macro_id, is_exist))) {
    LOG_WARN("fail to check exist in macro block KVCACHE", KR(ret), K(is_exist));
  } else if (!is_exist) {
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_cache_mgr is null", KR(ret), KP(macro_cache_mgr));
    } else if (OB_FAIL(macro_cache_mgr->exist(macro_id, is_exist))) {
      LOG_WARN("fail to check exist", KR(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSLocalCacheService::do_prewarm_tablets(
    const ObIArray<ObTabletID> &tablet_ids,
    const PolicyStatus &policy_status,
    int64_t &succ_prewarm_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t total_tablet_cnt = tablet_ids.count();
  ObStorageCachePolicyService *scp_service = nullptr;
  if (OB_UNLIKELY(total_tablet_cnt < 1)) {
    // skip
  } else if (OB_ISNULL(scp_service = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_cache_policy service is null", KR(ret));
  } else {
    for (int64_t i = 0; i < total_tablet_cnt; ++i) {
      if (OB_TMP_FAIL(scp_service->async_prewarm_tablet(tablet_ids.at(i).id(), policy_status))) {
        LOG_WARN("fail to prewarm tablet", KR(tmp_ret), K(i), K(tablet_ids.at(i)), K(policy_status));
        tmp_ret = OB_SUCCESS;
      } else {
        ++succ_prewarm_cnt;
      }
    }
  }
  return ret;
}

int ObSSLocalCacheService::prewarm_table_opt_index(
    const ObString &db_name, 
    const ObString &table_name, 
    const bool with_local_index,
    const bool with_global_index)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(db_name.empty() || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(db_name), K(table_name));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const schema::ObTableSchema *table_schema = nullptr;
    schema::ObSchemaGetterGuard schema_guard;
    ObSEArray<ObTabletID, 16> data_table_tablets;
    ObSEArray<ObTabletID, 16> index_table_tablets;
    const bool with_index = (with_local_index || with_global_index);
    if (OB_FAIL(ObSSLocalCacheUtil::get_table_schema(tenant_id, db_name, table_name, GCTX.schema_service_,
        schema_guard, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(db_name), K(table_name));
    } else if (OB_FAIL(table_schema->get_tablet_ids(data_table_tablets))) {
      LOG_WARN("fail to get tablet ids", KR(ret), K(tenant_id), KPC(table_schema));
    } else if (!with_index) {
    } else if (OB_FAIL(ObSSLocalCacheUtil::get_tablets_from_all_index_tables(tenant_id, with_local_index,
               with_global_index, table_schema, GCTX.schema_service_, schema_guard, index_table_tablets))) {
      LOG_WARN("fail to get tablets from all index tables", KR(ret), K(tenant_id), K(with_local_index),
        K(with_global_index), KPC(table_schema));
    }

    int64_t prewarm_cnt = 0;
    if (OB_TMP_FAIL(do_prewarm_tablets(data_table_tablets, PolicyStatus::AUTO, prewarm_cnt))) {
      LOG_WARN("fail to do_prewarm tablets", KR(tmp_ret), K(data_table_tablets));
      tmp_ret = OB_SUCCESS;
    }
    if (with_index && OB_TMP_FAIL(do_prewarm_tablets(index_table_tablets, PolicyStatus::AUTO, prewarm_cnt))) {
      LOG_WARN("fail to do_prewarm tablets", KR(tmp_ret), K(index_table_tablets));
      tmp_ret = OB_SUCCESS;
    }
    LOG_TRACE("finish to prewarm table opt_with_index", KR(ret), K(with_local_index), K(with_global_index),
      K(data_table_tablets.count()), K(index_table_tablets.count()), K(prewarm_cnt));
  }
  return ret;
}

int ObSSLocalCacheService::prewarm_tablet_opt_index(
    const ObTabletID &tablet_id, 
    const bool with_local_index)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t index_table_tablet_cnt = 0;
  int64_t prewarm_cnt = 0;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    ObSEArray<ObTabletID, 1> data_table_tablets;
    if (OB_FAIL(data_table_tablets.push_back(tablet_id))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_id));
    } else if (OB_TMP_FAIL(do_prewarm_tablets(data_table_tablets, PolicyStatus::AUTO, prewarm_cnt))) {
      LOG_WARN("fail to do_prewarm tablets", KR(tmp_ret), K(data_table_tablets));
      tmp_ret = OB_SUCCESS;
    }
  } 
  
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(ret)) {
  } else if (with_local_index) {
    ObSEArray<ObTabletID, 16> index_table_tablets;
    int64_t schema_version = OB_INVALID_VERSION;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get_schema_guard failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
      LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObSSLocalCacheUtil::get_tablet_from_all_local_index_tables(tenant_id, tablet_id,
               schema_version, GCTX.schema_service_, schema_guard, index_table_tablets))) {
      LOG_WARN("fail to get tablet from all local index tables", KR(ret), K(tenant_id), K(schema_version), K(tablet_id));
    } else if (FALSE_IT(index_table_tablet_cnt = index_table_tablets.count())) {
    } else if (OB_TMP_FAIL(do_prewarm_tablets(index_table_tablets, PolicyStatus::AUTO, prewarm_cnt))) {
      LOG_WARN("fail to do_prewarm tablets", KR(tmp_ret), K(index_table_tablets));
      tmp_ret = OB_SUCCESS;
    }
  }
  LOG_TRACE("finish to prewarm tablet opt_with_index", KR(ret), K(with_local_index), K(tablet_id),
      K(index_table_tablet_cnt), K(prewarm_cnt));
  return ret;
}

class ObSSLocalCacheCopyTabletIdFunc
{
public:
  ObSSLocalCacheCopyTabletIdFunc(ObIArray<uint64_t> &tablet_ids_in_map)
      : tablet_ids_in_map_(tablet_ids_in_map)
  {}

  int operator()(const SSTabletHitStatMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tablet_ids_in_map_.push_back(entry.first))) {
      LOG_WARN("fail to push tablet id to tablet_ids_in_map", KR(ret), K(tablet_ids_in_map_.count()));
    }
    return ret;
  }
private:
  ObIArray<uint64_t> &tablet_ids_in_map_;
};

int ObSSLocalCacheService::do_local_cache_tablet_stat_map_clean_work()
{
  int ret = OB_SUCCESS;
  int64_t cleaned_tablet_count = 0;
  hash::ObHashSet<uint64_t> active_tablet_ids;
  const int64_t tablet_stat_bucket_num = local_cache_tablet_stat_map_.bucket_count();
  ObMemAttr attr(MTL_ID(), "LCCActiveTablet");
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  ObSEArray<uint64_t, 8> tablet_ids_in_map;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_FAIL(active_tablet_ids.create(tablet_stat_bucket_num, attr))) {
    LOG_WARN("fail to create active_tablet_ids", KR(ret), K(tablet_stat_bucket_num), K(attr));
  } else if (OB_FAIL(get_active_tablet_ids(active_tablet_ids, attr))) {
    LOG_WARN("fail to get active_tablet_ids", KR(ret), K(attr));
  } else {
    // 1. copy tablet ids in local_cache_tablet_stat_map to tablet_ids_in_map
    ObMemAttr attr(MTL_ID(), "LCCleanTablet");
    tablet_ids_in_map.set_attr(attr);
    ObSSLocalCacheCopyTabletIdFunc copy_tablet_id_func(tablet_ids_in_map);
    if (OB_FAIL(tablet_ids_in_map.reserve(local_cache_tablet_stat_map_.size()))) {
      LOG_WARN("fail to reserve", KR(ret), "size", local_cache_tablet_stat_map_.size());
    } else if ((local_cache_tablet_stat_map_.size() > 0) &&
               OB_FAIL(local_cache_tablet_stat_map_.foreach_refactored(copy_tablet_id_func))) {
      LOG_WARN("fail to copy tablet_ids_in_map", KR(ret), K(local_cache_tablet_stat_map_.size()));
    }
    // 2. find tablet ids in tablet_ids_in_map but not in active_tablet_ids
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_in_map.count(); i++) {
      const uint64_t tablet_id = tablet_ids_in_map.at(i);
      if (OB_FAIL(active_tablet_ids.exist_refactored(tablet_id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret) {
          // 3. tablet id not in active_tablet_ids, erase it from local_cache_tablet_stat_map
          ret = OB_SUCCESS;
          if (OB_FAIL(local_cache_tablet_stat_map_.erase_refactored(tablet_id))) {
            LOG_WARN("fail to erase deleted tablet from local cache tablet stat map", KR(ret), K(tablet_id));
          } else {
            cleaned_tablet_count++;
          }
        } else {
          LOG_WARN("fail to check tablet exist", KR(ret), K(tablet_id));
        }
      }
    }
  }
  const int64_t used_time_us = ObTimeUtility::current_time_us() - start_time_us;
  LOG_INFO("finish clean deleted tablets from local cache tablet stat map", KR(ret), K(used_time_us), K(start_time_us),
        K(active_tablet_ids.size()), K(tablet_ids_in_map.count()), K(cleaned_tablet_count));
  return ret;
}

int ObSSLocalCacheService::get_ls_cache_info(
    const ObLSID &ls_id,
    ObSSLocalCacheLSInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_cache_mgr is null", KR(ret), KP(macro_cache_mgr));
  } else if (OB_FAIL(macro_cache_mgr->get_ls_cache_info(ls_id, ls_cache_info.macro_cache_ls_info_))) {
    LOG_WARN("fail to get ls cache info of macro cache", KR(ret), K(ls_id));
  }
  return ret;
}

int ObSSLocalCacheService::get_job_info(
    const ObLSID &ls_id,
    const ObHALocalCacheTaskCount &task_count,
    ObIArray<ObHACacheJobInfo> &job_infos)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObMemAttr attr(MTL_ID(), "GetJobInfo");
  ObSEArray<ObSSMacroCacheBucketIdxRange, 16> bucket_ranges;
  bucket_ranges.set_attr(attr);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_cache_mgr is null", KR(ret), KP(macro_cache_mgr));
  } else {
    if (OB_SUCC(ret) && (task_count.macro_cache_task_count_ > 0)) {
      if (OB_FAIL(macro_cache_mgr->divide_bucket_range(task_count.macro_cache_task_count_, bucket_ranges))) {
        LOG_WARN("fail to divide bucket range", KR(ret), K(task_count));
      } else if (OB_UNLIKELY(bucket_ranges.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro cache bucket_ranges should not be empty", KR(ret), K(task_count));
      } else {
        ObHACacheJobInfo job_info;
        job_info.job_type_ = ObHACacheJobType::MACRO_CACHE_JOB_TYPE;
        for (int64_t i = 0; i < bucket_ranges.count() && OB_SUCC(ret); ++i) {
          job_info.start_idx_ = bucket_ranges.at(i).start_bkt_idx_;
          job_info.end_idx_ = bucket_ranges.at(i).end_bkt_idx_;
          if (OB_FAIL(job_infos.push_back(job_info))) {
            LOG_WARN("fail to push back job info", KR(ret), K(job_info));
          }
        }
      }
    }
  }
  return ret;
}

bool is_macro_read_cache_disabled()
{
  bool is_disabled = false;
  ObSSLocalCacheService *local_cache_service = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *)) ||
      OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "local cache service or macro cache mgr is null",
                 KP(local_cache_service), KP(macro_cache_mgr));
  } else {
    is_disabled = (local_cache_service->is_macro_read_cache_disabled() ||
                   !macro_cache_mgr->is_ckpt_replayed());
  }
  return is_disabled;
}

bool is_macro_write_cache_disabled()
{
  bool is_disabled = false;
  ObSSLocalCacheService *local_cache_service = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *)) ||
      OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "local cache service or macro cache mgr is null",
                 KP(local_cache_service), KP(macro_cache_mgr));
  } else {
    is_disabled = (local_cache_service->is_macro_write_cache_disabled() ||
                   !macro_cache_mgr->is_ckpt_replayed());
  }
  return is_disabled;
}

int ObSSLocalCacheService::clear_ss_all_cache(const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_ss_macro_cache(abs_timeout_us))) {
    LOG_WARN("fail to clear ss_macro_cache", KR(ret));
  } else if (OB_FAIL(erase_ss_macro_block_kvcache_entries())) {
    LOG_WARN("fail to clear macro block KVCACHE", KR(ret));
  } else if (OB_FAIL(clear_ss_hot_macro_cache(abs_timeout_us))) {
    LOG_WARN("fail to clear ss_hot_macro_cache", KR(ret));
  }
  return ret;
}

int ObSSLocalCacheService::clear_ss_macro_cache(const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_macro_cache_mgr is nullptr", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->clear_macro_cache(abs_timeout_us, false/*is_hot_tablet*/))) {
    LOG_WARN("fail to clear ss_macro_cache", KR(ret));
  }
  return ret;
}

int ObSSLocalCacheService::erase_ss_macro_block_kvcache_entries()
{
  int ret = OB_SUCCESS;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
  } else if (OB_FAIL(tenant_kvcache_mgr->erase_tenant_entries())) {
    LOG_WARN("fail to clear macro block KVCACHE", KR(ret));
  }
  return ret;
}

int ObSSLocalCacheService::clear_ss_hot_macro_cache(const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_macro_cache_mgr is nullptr", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->clear_macro_cache(abs_timeout_us, true/*is_hot_tablet*/))) {
    LOG_WARN("fail to clear ss_hot_macro_cache", KR(ret));
  }
  return ret;
}

int ObSSLocalCacheService::async_write_macro_to_lcache(
    const blocksstable::MacroBlockId &macro_id,
    const char *buf,
    const int64_t size,
    const uint64_t effective_tablet_id,
    const common::ObIOFlag &io_desc)
{
  int ret = OB_SUCCESS;
  ObStorageObjectWriteInfo write_info;
  write_info.buffer_ = buf;
  write_info.offset_ = 0;
  write_info.size_ = size;
  write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  write_info.io_desc_ = io_desc;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_is_write_cache(false);
  write_info.set_effective_tablet_id(effective_tablet_id);
  const ObMemAttr attr(MTL_ID(), "WrLCacheEntry");
  ObSSWriteLCacheEntry *entry = nullptr;
  if (OB_ISNULL(entry = OB_NEW(ObSSWriteLCacheEntry, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc write lcache entry", KR(ret), K(macro_id), K(write_info));
  } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
      true/*use_effective_tablet_id*/, macro_id, write_info, entry->handle_,
      0/*hot_tablet_alloc_timeout_us*/))) {
    LOG_WARN("fail to async write to macro cache", KR(ret), K(macro_id), K(write_info));
  } else if (OB_FAIL(push_write_lcache_queue(*entry))) {
    LOG_WARN("fail to push write lcache queue", KR(ret), K(macro_id), K(write_info));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(entry)) {
    OB_DELETE(ObSSWriteLCacheEntry, attr, entry);
  }
  return ret;
}

int ObSSLocalCacheService::push_write_lcache_queue(ObSSWriteLCacheEntry &entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(!entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid handle", KR(ret), K(entry));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (OB_FAIL(write_lcache_task_.push_write_lcache_queue(entry))) {
    LOG_WARN("fail to push write lcache queue", KR(ret), K(entry));
  }
  return ret;
}

int ObSSLocalCacheService::push_old_sstable_evict_task(
    const ObTabletID &tablet_id,
    const ObSSHotTabletOldSSTableEvictInfo &evict_info)
{
  int ret = OB_SUCCESS;
  bool is_hot_tablet = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  const ObSSLocalCacheEvictOldSSTablePolicy evict_old_sstable_policy = get_evict_old_sstable_policy();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache service is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("local cache service is stopped", KR(ret));
  } else if (ObSSLocalCacheEvictOldSSTablePolicy::OFF == evict_old_sstable_policy) {
    LOG_TRACE("evict old sstable policy is off, no need to push old sstable evict task", K(tablet_id), K(evict_info));
  } else if (OB_FAIL(ObSSObjectAccessUtil::check_if_hot_tablet(tablet_id.id(), is_hot_tablet))) {
    LOG_WARN("fail to check if hot tablet", KR(ret), K(tablet_id));
  } else if (!is_hot_tablet) {
    LOG_TRACE("this tablet is not hot, no need to push old sstable evict task", K(tablet_id), K(evict_info));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_macro_cache_mgr is nullptr", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->push_old_sstable_evict_task(tablet_id, evict_info))) {
    LOG_WARN("fail to push old sstable evict task", KR(ret), K(tablet_id), K(evict_info));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
