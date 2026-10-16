/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_handler.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/io/ob_io_define.h"
#include "share/ob_io_device_helper.h"
#include "share/io/ob_io_manager.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ERRSIM_POINT_DEF(EN_MICRO_CACHE_FULL);
/*-----------------------------------------ObSSMicroCache-----------------------------------------*/
ObSSMicroCache::ObSSMicroCache()
  : is_inited_(false), is_stopped_(false), is_enabled_(true), is_operating_(false), is_write_allowed_(true),
    is_read_allowed_(true), enable_calibration_(true), phy_blk_size_(0), tenant_id_(OB_INVALID_TENANT_ID), 
    cache_file_size_(0), flying_add_req_cnt_(0), flying_get_req_cnt_(0), micro_ckpt_split_cnt_(0), cache_stat_(),
    allocator_(), meta_allocator_(), phy_blk_mgr_(cache_stat_, allocator_), mem_blk_mgr_(cache_stat_), 
    micro_range_mgr_(cache_stat_, allocator_), micro_meta_mgr_(cache_stat_, allocator_), la_micro_key_mgr_(), task_runner_()
{}

ERRSIM_POINT_DEF(EN_SS_MICRO_CACHE_FULL);

int ObSSMicroCache::mtl_init(ObSSMicroCache *&micro_cache)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_mgr = nullptr;
  
  if (OB_ISNULL(tnt_disk_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_mgr));
  } else {
    const uint64_t tenant_id = MTL_ID();
    // const int64_t cache_file_size = tnt_disk_mgr->get_micro_cache_file_size();
    if (OB_FAIL(micro_cache->init(tenant_id, cache_file_size))) {
      LOG_WARN("fail to init ss_micro_cache", KR(ret), K(tenant_id), K(cache_file_size));
    }
  }
  return ret;
}

int ObSSMicroCache::init(const uint64_t tenant_id, const int64_t cache_file_size, const uint32_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  const int32_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  int32_t phy_blk_size = (macro_block_size != 0) ? macro_block_size : SS_DEFAULT_PHY_BLK_SIZE;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((cache_file_size <= 0 || phy_blk_size <= 0 || !is_valid_tenant_id(tenant_id)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_file_size), K(phy_blk_size));
  } else {
    if (cache_file_size <= SS_MIN_CACHE_FILE_SIZE) {
      LOG_WARN("ss_micro_cache file size is too small, may exist some wrong state", K(cache_file_size));
    }

    tenant_id_ = tenant_id;
    cache_file_size_ = cache_file_size;
    phy_blk_size_ = phy_blk_size;
    // if it equals to 0, we will init it later
    micro_ckpt_split_cnt_ = micro_ckpt_split_cnt;
    const int64_t alloc_limit_size = ObSSMicroCacheUtil::calc_micro_cache_mem_limit_size(tenant_id);
    int64_t cache_limit_size = 0;
    ObTenantFileManager *tnt_file_mgr = nullptr;
    if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant disk file manager is null", KR(ret), KP(tnt_file_mgr));
    } else if (OB_FAIL(allocator_.init(phy_blk_size, ObMemAttr(tenant_id, "SSMicroCache"), alloc_limit_size))) {
      LOG_WARN("fail to init allocator", KR(ret), K(phy_blk_size), K(tenant_id), K(alloc_limit_size));
    } else if (OB_FAIL(mem_blk_mgr_.init(tenant_id, phy_blk_size))) {
      LOG_WARN("fail to init mem data manager", KR(ret), K(tenant_id), K(phy_blk_size));
    } else if (OB_FAIL(phy_blk_mgr_.init(tenant_id, cache_file_size, phy_blk_size))) {
      LOG_WARN("fail to init block manager", KR(ret), K(tenant_id), K(cache_file_size), K(phy_blk_size));
    } else if (FALSE_IT(inner_update_micro_ckpt_split_cnt(cache_file_size, true/*for_init*/))) {
    } else if (OB_FAIL(meta_allocator_.init(sizeof(ObSSMicroBlockMeta), "SSMetaAlloc", tenant_id_, SS_META_ALLOCATOR_BLOCK_SIZE))) {
      LOG_WARN("fail to init micro_meta allocator", KR(ret), K(sizeof(ObSSMicroBlockMeta)), K_(tenant_id));
    } else if (OB_FAIL(micro_range_mgr_.init(tenant_id, micro_ckpt_split_cnt_))) {
      LOG_WARN("fail to init micro range", KR(ret), K(tenant_id), K_(micro_ckpt_split_cnt));
    } else if (FALSE_IT(cache_limit_size = phy_blk_mgr_.get_data_block_max_cnt() * phy_blk_size_)) {
    } else if (OB_FAIL(micro_meta_mgr_.init(tenant_id_, phy_blk_size_, cache_limit_size, micro_range_mgr_))) {
      LOG_WARN("fail to init micro meta manager", KR(ret), K_(tenant_id), K(cache_limit_size), K_(phy_blk_size));
    } else if (OB_FAIL(la_micro_key_mgr_.init())) {
      LOG_WARN("fail to init latest access micro key manager", KR(ret));
    } else if (OB_FAIL(task_runner_.init(tenant_id, allocator_, mem_blk_mgr_, micro_meta_mgr_, 
                micro_range_mgr_, phy_blk_mgr_, *tnt_file_mgr, cache_stat_, la_micro_key_mgr_))) {
      LOG_WARN("fail to init micro_cache task runner", KR(ret), K(tenant_id));
    } else {
      flying_add_req_cnt_ = 0;
      flying_get_req_cnt_ = 0;
      is_inited_ = true;
      is_enabled_ = true;
      LOG_INFO("succ to init ss_micro_cache", K(tenant_id), K(cache_file_size), K(alloc_limit_size), K_(micro_ckpt_split_cnt));
    }
  }
  return ret;
}

int ObSSMicroCache::start()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    FLOG_INFO("begin ss_micro_cache start func");
    ATOMIC_STORE(&is_stopped_, false);
    if (OB_FAIL(read_or_format_super_block())) {
      LOG_WARN("fail to read or format ss_super_block", KR(ret));
    } else if (!is_cache_enabled()) {
      // If cache is disabled, we need to execute replay_ckpt_async. Disable extra tasks and
      // enable replay_ckpt BEFORE task_runner_.start(). These calls only control whether
      // the background task will run when it is scheduled - scheduling has not started yet.
      // Actual scheduling begins only after task_runner_.start() completes. We do this to
      // avoid: if we called start() first and then disable, the background task could already
      // be running before we have a chance to disable it.
      task_runner_.disable_extra_task_for_ckpt();
      task_runner_.enable_replay_ckpt();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(task_runner_.start())) {
      LOG_WARN("fail to start micro_cache task runner", KR(ret));
    }
    FLOG_INFO("finish ss_micro_cache start func", K_(is_enabled));
  }
  return ret;
}

void ObSSMicroCache::stop()
{
  FLOG_INFO("begin ss_micro_cache stop func", KP(this));
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtil::current_time_us();
  ATOMIC_STORE(&is_stopped_, true);
  do {
    if (ATOMIC_LOAD(&is_operating_)) {
      ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
        FLOG_WARN("still wait for clear_micro_cache/resize/calibration finish", K_(is_operating));
      }
    } else {
      break;
    }
  } while (OB_SUCC(ret) && (ObTimeUtil::current_time_us() - start_time_us < SS_OPERATE_MICRO_CACHE_TIMEOUT_US));
  task_runner_.stop();
  FLOG_INFO("finish ss_micro_cache stop func", KP(this));
}

int ObSSMicroCache::wait()
{
  FLOG_INFO("begin ss_micro_cache wait func", KP(this));
  int ret = OB_SUCCESS;
  task_runner_.wait();
  FLOG_INFO("finish ss_micro_cache wait func", KP(this));
  return ret;
}

void ObSSMicroCache::inner_check_micro_meta_cnt()
{
  int ret = OB_SUCCESS;
  int64_t cur_alloc_cnt = cache_stat_.mem_stat().get_micro_alloc_cnt();
  while (cur_alloc_cnt != 0) {
    ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
    if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
      FLOG_WARN("still exist unfree micro_meta", KR(ret), K(cur_alloc_cnt));
    }
    cur_alloc_cnt = cache_stat_.mem_stat().get_micro_alloc_cnt();
  }
}

void ObSSMicroCache::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin ss_micro_cache destroy func", KP(this), "arc_info", micro_meta_mgr_.get_arc_info());

  if (IS_INIT) {
    bool fin_destroy = false;
    do {
      const int64_t flying_add_req_cnt = ATOMIC_LOAD(&flying_add_req_cnt_);
      const int64_t flying_get_req_cnt = ATOMIC_LOAD(&flying_get_req_cnt_);
      const int64_t flying_req_cnt = flying_add_req_cnt + flying_get_req_cnt;
      if (flying_req_cnt > 0) {
        ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
          FLOG_WARN("still exist request which may access micro_meta_map", K(flying_add_req_cnt), K(flying_get_req_cnt), K(flying_req_cnt));
        }
      } else {
        la_micro_key_mgr_.destroy();
        task_runner_.destroy();
        micro_meta_mgr_.destroy();
        micro_range_mgr_.destroy();
        mem_blk_mgr_.destroy();
        phy_blk_mgr_.destroy();
        allocator_.reset();

        // must execute it at the last step
        inner_check_micro_meta_cnt();
        meta_allocator_.destroy();
        fin_destroy = true;
      }
    } while (!fin_destroy);
  }

  phy_blk_size_ = 0;
  cache_file_size_ = 0;
  flying_add_req_cnt_ = 0;
  flying_get_req_cnt_ = 0;
  micro_ckpt_split_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_operating_ = false;
  is_inited_ = false;
  is_enabled_ = true;
  FLOG_INFO("finish ss_micro_cache destroy func", KP(this), K_(cache_stat));
  cache_stat_.reset();
}

int ObSSMicroCache::read_or_format_super_block()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk file manager is null", KR(ret), KP(tnt_file_mgr));
  } else if (false) { // micro-cache file removed
    ObSSMicroCacheSuperBlk super_blk;
    if (OB_FAIL(phy_blk_mgr_.read_ss_super_block(super_blk))) {
      FLOG_WARN("fail to read ss_super_block", KR(ret));
    } else if (OB_UNLIKELY(!super_blk.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      FLOG_WARN("ss_super_block is invalid", KR(ret), K(super_blk));
    } 
    
    if (OB_SUCC(ret)) {
      admin_info_.set_micro_meta_compressor_type(super_blk.get_compressor_type());
      if (super_blk.exist_valid_checkpoint()) {
        disable_cache();
        FLOG_INFO("ss_micro_cache checkpoint will execute async, thus disable cache firstly", 
          K_(is_enabled), K(super_blk));
      } else {
        if (OB_FAIL(phy_blk_mgr_.reformat_ss_super_block(micro_ckpt_split_cnt_))) {
          LOG_WARN("fail to reformat ss_super_block", KR(ret), K_(micro_ckpt_split_cnt), K(super_blk));
        } else {
          FLOG_INFO("succ to reformat ss_super_block for invalid ckpt", K_(micro_ckpt_split_cnt), K(super_blk));
        }
      }
    } else {
      // if fail to read super_block when restart, will re-format super_block
      ret = OB_SUCCESS;
      FLOG_INFO("Cuz fail to read super_block, thus will re-format super_block");
      if (OB_FAIL(phy_blk_mgr_.format_ss_super_block(micro_ckpt_split_cnt_))) {
        LOG_WARN("fail to format ss_super_block", KR(ret), K_(micro_ckpt_split_cnt));
      }
    }
  } else if (OB_FAIL(phy_blk_mgr_.format_ss_super_block(micro_ckpt_split_cnt_))) {
    LOG_WARN("fail to format ss_super_block", KR(ret), K_(micro_ckpt_split_cnt));
  }
  return ret;
}

int ObSSMicroCache::read_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCacheSuperBlk super_blk;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
    LOG_WARN("fail to get ss_super_blk", KR(ret), K_(is_inited), K_(is_enabled));
  } else if (super_blk.is_valid()) {
    if (ObTimeUtility::current_time() - super_blk.micro_ckpt_time_us_ >= SS_MAX_CKPT_EXPIRATION_TIME_US) {
      LOG_WARN("ss_micro_cache checkpoint is too old", K(super_blk));
    }
    const int64_t ori_micro_ckpt_split_cnt = super_blk.get_ckpt_split_cnt();
    const bool reserve_ckpt_info = (ori_micro_ckpt_split_cnt == micro_ckpt_split_cnt_);

    if (super_blk.exist_blk_checkpoint()) {
      const int64_t blk_ckpt_entry = super_blk.get_blk_ckpt_entry();
      if (OB_FAIL(read_phy_block_checkpoint(blk_ckpt_entry, reserve_ckpt_info))) {
        LOG_WARN("fail to read phy_block checkpoint", KR(ret), K(blk_ckpt_entry), K(reserve_ckpt_info),
          K_(micro_ckpt_split_cnt), K(super_blk));
        ret = OB_SUCCESS; // must succ, otherwise skip read micro_meta checkpoint
      } else if (super_blk.exist_micro_checkpoint()) {
        const ObIArray<ObSSMicroCkptEntryItem> &micro_ckpt_entries = super_blk.get_micro_ckpt_entries();
        const int64_t micro_ckpt_time_us = super_blk.micro_ckpt_time_us_;
        if (OB_FAIL(read_micro_meta_checkpoint(micro_ckpt_entries, micro_ckpt_time_us, reserve_ckpt_info))) {
          LOG_WARN("fail to read micro_meta checkpoint", KR(ret), K(micro_ckpt_time_us), K(reserve_ckpt_info),
            K_(micro_ckpt_split_cnt), K(micro_ckpt_entries), K(super_blk));
        }
      }
    }

    if (!reserve_ckpt_info) {
      ObSSMicroCacheSuperBlk new_super_blk;
      new_super_blk.assign_basic_info(super_blk);
      if (OB_TMP_FAIL(new_super_blk.init_non_basic_info(tenant_id_, micro_ckpt_split_cnt_))) {
        LOG_WARN("fail to init super_blk non_basic info", KR(tmp_ret), K_(tenant_id), K_(micro_ckpt_split_cnt), K(new_super_blk));
      } else if (OB_TMP_FAIL(phy_blk_mgr_.update_ss_super_block(new_super_blk))) {
        LOG_WARN("fail to update ss_super_block", KR(tmp_ret), K(new_super_blk), K(super_blk));
      } else {
        FLOG_INFO("cuz micro_ckpt split_cnt changed, thus need to re-format ss_super_block", K(new_super_blk),
          K(super_blk));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_blk_mgr_.update_block_state_for_restart())) {
        LOG_ERROR("fail to update phy_block state", KR(ret), K_(tenant_id), K(super_blk));
      }
    }
  }
  return ret;
}

int ObSSMicroCache::read_phy_block_checkpoint(const int64_t entry_blk_idx, const bool reserve_ckpt_info)
{
  int ret = OB_SUCCESS;
  ObSSCkptPhyBlockItemReader item_reader;
  ObSEArray<int64_t, SS_DEF_ARRAY_CNT> ckpt_blk_id_list;
  const ObSSMicroCacheCkptType ckpt_type = ObSSMicroCacheCkptType::SS_CACHE_BLK_CKPT;
  if (OB_FAIL(item_reader.init(entry_blk_idx, tenant_id_, phy_blk_mgr_))) {
    LOG_WARN("fail to init ss_item_reader", KR(ret), K_(tenant_id), K(entry_blk_idx));
  } else if (OB_FAIL(phy_blk_mgr_.read_phy_block_checkpoint(item_reader, ckpt_blk_id_list))) {
    LOG_WARN("fail to read phy_block checkpoint", KR(ret), K(entry_blk_idx));
  } else if (reserve_ckpt_info && OB_FAIL(phy_blk_mgr_.update_blk_ckpt_used_block_list(ckpt_blk_id_list))) {
    LOG_WARN("fail to update ss_super_blk blk_ckpt entry list", KR(ret), K(ckpt_blk_id_list));
  }
  ckpt_blk_id_list.reset();
  return ret;
}

int ObSSMicroCache::read_micro_meta_checkpoint(
    const ObIArray<ObSSMicroCkptEntryItem> &ckpt_entries, 
    const int64_t micro_ckpt_time_us,
    const bool reserve_ckpt_info)
{
  int ret = OB_SUCCESS;
  const int64_t ckpt_entry_cnt = ckpt_entries.count();
  ObSEArray<int64_t, SS_DEF_ARRAY_CNT> ckpt_blk_id_list;
  for (int64_t i = 0; i < ckpt_entry_cnt; ++i) {
    const ObSSMicroCacheCkptType ckpt_type = ObSSMicroCacheCkptType::SS_CACHE_MICRO_CKPT;
    const bool need_reset_stat = (0 == i) ? true : false;
    if (ckpt_entries.at(i).exist_ckpt_) {
      ObSSCkptPhyBlockItemReader item_reader;
      const int64_t entry_blk_idx = ckpt_entries.at(i).entry_blk_idx_;
      if (OB_UNLIKELY(SS_INVALID_PHY_BLK_ID == entry_blk_idx)) { // skip it
        LOG_ERROR("invalid entry_blk_idx", KR(ret), K(i), K(ckpt_entry_cnt), K(ckpt_entries.at(i)));
      } else if (OB_FAIL(item_reader.init(entry_blk_idx, tenant_id_, phy_blk_mgr_))) {
        LOG_WARN("fail to init ss_item_reader", KR(ret), K(i), K(ckpt_entry_cnt), K_(tenant_id), K(entry_blk_idx));
      } else if (OB_FAIL(micro_meta_mgr_.read_micro_meta_checkpoint(item_reader, micro_ckpt_time_us, ckpt_blk_id_list,
                 need_reset_stat))) {
        LOG_WARN("fail to read micro_meta checkpoint", KR(ret), K(i), K(ckpt_entry_cnt), K_(tenant_id), K(micro_ckpt_time_us));
      } else if (reserve_ckpt_info && OB_FAIL(phy_blk_mgr_.update_micro_ckpt_used_block_list(i, ckpt_blk_id_list))) {
        LOG_WARN("fail to update ss_super_blk micro_ckpt entry list", KR(ret), K(i), K(ckpt_entry_cnt), K(ckpt_blk_id_list));
      }
      ckpt_blk_id_list.reuse();
    }

    ret = OB_SUCCESS; // continue to handle next entry
  }
  ckpt_blk_id_list.reset();
  return ret;
}

int ObSSMicroCache::is_ready_to_evict_cache(bool &prepare_eviction)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    prepare_eviction = micro_meta_mgr_.get_arc_info().close_to_evict();
  }
  return ret;
}

int ObSSMicroCache::resize_micro_cache_file_size(const int64_t new_file_size)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("fail to resize micro_cache_file", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_UNLIKELY(new_file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_file_size));
  } else {
    bool fin_resize = false;
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    do {
      if (try_start_operating()) {
        if (new_file_size > cache_file_size_) {
          FLOG_INFO("start to resize micro_cache file size", K_(cache_file_size), K(new_file_size));
          if (OB_FAIL(phy_blk_mgr_.resize_file_size(new_file_size, phy_blk_size_))) {
            LOG_WARN("fail to resize params about phy_blk_mgr", KR(ret), K_(tenant_id), K(new_file_size)); 
          } else if (OB_FAIL(this->inner_handle_after_resize(new_file_size))) {
            LOG_WARN("fail to inner handle after resize", KR(ret), K_(tenant_id), K(new_file_size));
          }
        }
        fin_resize = true;
        finish_operating();
        break;
      } else {
        ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
          FLOG_WARN("still wait for clear_micro_cache/resize/calibration finish", K_(is_operating));
        }
      }
    } while (OB_SUCC(ret) && (ObTimeUtility::current_time_us() - start_time_us < SS_OPERATE_MICRO_CACHE_TIMEOUT_US));

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!fin_resize)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("may executing clear micro_cache or calibration, can't resize", KR(ret), K_(is_operating), K(fin_resize), K(new_file_size));
    }
    FLOG_INFO("finish resizing ss_micro_cache_file size", KR(ret), K(fin_resize), K(new_file_size));
  }
  return ret;
}

int ObSSMicroCache::inner_handle_after_resize(const int64_t new_file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(new_file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_file_size));
  } else {
    inner_update_micro_ckpt_split_cnt(new_file_size, false/*for_init*/);
    const int64_t ori_file_size = cache_file_size_;
    cache_file_size_ = new_file_size;
    FLOG_INFO("succ to resize micro_cache_file size", K(ori_file_size), K(new_file_size));
  }
  return ret;
}

int ObSSMicroCache::add_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const char *micro_data,
    const int32_t size,
    const uint64_t effective_tablet_id,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  const int64_t start_us = ObTimeUtility::current_time();
  const bool is_prewarm_io = (access_type != ObSSMicroCacheAccessType::COMMON_IO_TYPE);
  const int64_t max_micro_block_size = get_max_micro_block_size();

  bool exist_valid_micro = false;
  bool is_first_add = false; // If this micro_block not exist in cache or is ghost state
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss_micro_cache not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
    }
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_key.is_valid() || (size <= 0) ||
             (ObSSMicroCacheAccessType::MAX_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), KP(micro_data), K(access_type));
  } else if (OB_UNLIKELY(size > max_micro_block_size)) {
    // Reject large micro blocks
    ret = OB_SS_CACHE_MICRO_BLOCK_TOO_LARGE;
    if (is_prewarm_io) {
      cache_stat_.prewarm_stat().update_reject_add_cnt(1);
    } else {
      cache_stat_.hit_stat().update_reject_add_cnt(1);
    }
    LOG_TRACE("micro block is too large, it will not be cached in micro cache", KR(ret), K(size), K(max_micro_block_size), K(micro_key));
  } else {
    cache_stat_.io_stat().update_io_stat_info(is_prewarm_io, false/*is_read*/, 1, size);

    ObSSMemBlockHandle mem_blk_handle;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    if (OB_UNLIKELY(OB_SUCCESS != (ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ADD_MICRO_BLOCK_ERR))) {
      LOG_WARN("add micro_block cache errsim", KR(ret));
    } else if (OB_FAIL(micro_meta_mgr_.get_micro_block_meta(micro_key, micro_meta_handle, effective_tablet_id, 
               true/*update_arc*/))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_block meta handle", KR(ret), K(micro_key), K(access_type), K(effective_tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      exist_valid_micro = true;
    }
    
    if (OB_SUCC(ret) && !exist_valid_micro) {
      if (OB_UNLIKELY(mem_blk_mgr_.get_free_mem_blk_cnt(true/*is_fg*/) < 1)) {
        ret = OB_EAGAIN;
        if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
          LOG_WARN("not exist enough free fg_mem_blk", KR(ret), K(micro_key), K(size), K(access_type), K(effective_tablet_id));
        }
      } else {
        uint32_t micro_crc = 0;
        bool alloc_new = false;
        if (OB_FAIL(mem_blk_mgr_.add_micro_block_data(micro_key, micro_data, size, mem_blk_handle, micro_crc, alloc_new))) {
          LOG_WARN("fail to add micro_block data", KR(ret), K(micro_key), K(size), K(access_type), K(effective_tablet_id));
          if (OB_ENTRY_NOT_EXIST == ret) { // no free mem_block, convert errno to OB_EAGAIN for upper module try again
            ret = OB_EAGAIN;
          }
        } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem_block handle should be valid", KR(ret), K(micro_key), K(size), K(mem_blk_handle));
        } else if (OB_FAIL(micro_meta_mgr_.add_or_update_micro_block_meta(micro_key, size, micro_crc,
                   effective_tablet_id, mem_blk_handle, is_first_add, access_type))) {
          if (OB_SS_CACHE_REACH_MEM_LIMIT == ret) {
            LOG_WARN("micro_meta memory usage reachs limit", KR(ret), K(micro_key), K(size), K(micro_crc), 
              K(effective_tablet_id), K(mem_blk_handle), K(access_type));
          } else {
            LOG_WARN("fail to add or update micro block meta", KR(ret), K(micro_key), K(size), K(micro_crc), 
              K(effective_tablet_id), K(mem_blk_handle), K(access_type));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_prewarm_io) {
        ObSSMicroCacheAccessInfo access_info(access_type, 1, size);
        cache_stat_.prewarm_stat().update_add_info(access_info, is_first_add);
      } else if (is_first_add) {
        cache_stat_.hit_stat().update_new_add_cnt(1);
      }
    } else {
      if (is_prewarm_io) {
        cache_stat_.prewarm_stat().update_fail_add_cnt(1);
      } else {
        cache_stat_.hit_stat().update_fail_add_cnt(1);
      }
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_TRACE("ss_micro_cache: finish add micro_block cache", KR(ret), K(cost_us), K(exist_valid_micro), K(is_first_add), 
    K(size), K(access_type), K(effective_tablet_id), K(micro_key));

  return ret;
}

int ObSSMicroCache::add_micro_block_cache_for_prewarm(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *micro_data,
    const int32_t size, 
    const uint64_t effective_tablet_id,
    const ObSSMicroCacheAccessType access_type, 
    const int64_t max_retry_times,
    const bool transfer_seg)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  bool is_add = false;
  int64_t retry_times = 0;
  while (OB_SUCC(ret) && !is_add && (retry_times < max_retry_times)) {
    if (OB_FAIL(add_micro_block_cache(micro_key, micro_data, size, effective_tablet_id, access_type))) {
      LOG_WARN("fail to add micro block cache", KR(ret), K(micro_key), KP(micro_data),
               K(size), K(effective_tablet_id), K(access_type), K(max_retry_times), K(retry_times));
    } else {
      is_add = true;
      // transfer from T1 to T2 if need
      if (transfer_seg) {
        int tmp_ret = OB_SUCCESS;
        ObSEArray<ObSSMicroBlockCacheKey, 1> micro_keys;
        micro_keys.set_attr(ObMemAttr(MTL_ID(), "SSMicroCache"));
        if (OB_TMP_FAIL(micro_keys.push_back(micro_key))) {
          LOG_WARN("fail to push back micro key", KR(tmp_ret), K(micro_key));
        } else if (OB_TMP_FAIL(update_micro_block_heat(micro_keys, true/*need to transfer T1 -> T2*/,
                                true/*need to update access_time to current_time*/))) {
          LOG_WARN("fail to update micro block heat", KR(tmp_ret), K(micro_keys));
        }
      }
    }
    // Note: OB_EAGAIN means space is not enough now and can retry again
    // Other errors, don't retry
    if (!is_add && (OB_EAGAIN == ret) && (retry_times < max_retry_times)) {
      ret = OB_SUCCESS; // ignore ret and retry again
      const int64_t retry_interval_ms = MIN(25 * (1 << retry_times), 1000); // [25ms, 1s]
      LOG_INFO("will retry add micro block cache", K(retry_times), K(retry_interval_ms),
                K(micro_key), K(access_type));
      ob_usleep(retry_interval_ms * 1000L);
    }
    retry_times++;
  }
  // OB_EAGAIN is already handled in the retry loop above.
  // OB_SS_CACHE_MICRO_BLOCK_TOO_LARGE (micro block too large, rejected) is a normal case.
  if (OB_SS_CACHE_MICRO_BLOCK_TOO_LARGE == ret) {
    ret = OB_SUCCESS; // such cases is normal
  }
  return ret;
}

int ObSSMicroCache::get_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroBlockId &phy_micro_id,
    const ObSSMicroCacheGetType get_type,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    const ObSSMicroCacheAccessType access_type,
    bool &hit_cache)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
    }
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_UNLIKELY(!micro_key.is_valid() || (ObSSMicroCacheAccessType::MAX_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(phy_micro_id), K(access_type));
  } else if ((ObSSMicroCacheGetType::GET_CACHE_HIT_DATA != get_type) && !phy_micro_id.is_valid()) {
    // GET_CACHE_HIT_DATA mode (e.g., replica prewarm) allows empty phy_micro_id because:
    // 1. Cache miss: No need to read from object storage, so macro_id is not required
    // 2. Cache hit: set_macro_block_id() failure (due to invalid macro_id) doesn't affect data retrieval
    // Only other modes (GET_CACHE_MISS_DATA, FORCE_GET_DATA) require valid phy_micro_id for object storage access
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(phy_micro_id), K(access_type));
  } else if (OB_FAIL(inner_get_micro_block_cache(micro_key, phy_micro_id, get_type, io_info,
             obj_handle, access_type, hit_cache))) {
    LOG_WARN("fail to inner get micro block cache", KR(ret), K(micro_key), K(phy_micro_id),
      K(get_type), K(io_info), K(obj_handle), K(access_type));
  }
  return ret;
}

int ObSSMicroCache::inner_get_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroBlockId &phy_micro_id,
    const ObSSMicroCacheGetType get_type,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    const ObSSMicroCacheAccessType access_type,
    bool &hit_cache)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t max_micro_block_size = 0;
  const bool is_prewarm_io = (access_type != ObSSMicroCacheAccessType::COMMON_IO_TYPE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (FALSE_IT(max_micro_block_size = get_max_micro_block_size())) {
  } else if (OB_UNLIKELY(io_info.size_ > max_micro_block_size)) {
    // Reject large micro blocks, but don't return error code
    if (is_prewarm_io) {
      cache_stat_.prewarm_stat().update_reject_get_cnt(1);
    } else {
      cache_stat_.hit_stat().update_reject_get_cnt(1);
    }
    LOG_WARN("micro block is too large", K(io_info), K(max_micro_block_size)); // ignore ret
  } else {
    hit_cache = false;
    const MacroBlockId &macro_id = phy_micro_id.get_macro_id();
    ObSSMemBlockHandle mem_blk_handle;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMicroCacheHitType hit_type = ObSSMicroCacheHitType::SS_CACHE_MISS;
    const bool is_common_io = (ObSSMicroCacheAccessType::COMMON_IO_TYPE == access_type);
    cache_stat_.io_stat().update_io_stat_info(!is_common_io, true/*is_read*/, 1, io_info.size_);
    const int32_t req_delta_cnt = (is_common_io ? 1 : 0);
    const int32_t req_delta_size = (is_common_io ? io_info.size_ : 0);
    // Update_arc will be set to false if is_write_allowed_ is false,
    // which means neither update arc_info nor read meta from the disk.
    const bool update_arc = (is_common_io && ATOMIC_LOAD(&is_write_allowed_)); 
    // Detail: 
    ObSSMicroBlockMetaInfo micro_info;
    if (OB_FAIL(inner_get_micro_block_handle(micro_key, micro_info, micro_meta_handle, mem_blk_handle, 
        io_info.phy_block_handle_, hit_type, io_info.effective_tablet_id_, update_arc))) {
      LOG_WARN("fail to inner get micro_block handle", KR(ret), K(micro_key), K(update_arc), K(io_info));
    } else {
      ObSSMicroCacheHandler handler;
      if (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type) {
        if (ObSSMicroCacheGetType::GET_CACHE_HIT_DATA == get_type) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_FAIL(handler.handle_not_hit(micro_key, macro_id, io_info, obj_handle, access_type))) {
          LOG_WARN("fail to handle not hit", KR(ret), K(micro_key), K(phy_micro_id), K(access_type));
        } else if (OB_ISNULL(io_info.fd_.device_handle_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("device handle is null", KR(ret), K(micro_key), K(phy_micro_id), K(get_type),
                   K(access_type), K(io_info));
        } else if (io_info.fd_.device_handle_->is_object_device()) {
          // read private macro from disk should not update cache miss cnt,
          // only read from object storage should update cache miss cnt
          cache_stat_.hit_stat().update_cache_miss(req_delta_cnt, req_delta_size);
          LOG_INFO("ss_cache: not hit micro_cache", K(micro_key), "size", io_info.size_, K(access_type));
        }
      } else if (ObSSMicroCacheHitType::SS_CACHE_HIT_MEM == hit_type) {
        hit_cache = true;
        char *io_buf = nullptr;
        // Due to the mem_block already inc_ref_cnt, thus we can promise when we get_micro_data, this mem_block is valid.
        if (ObSSMicroCacheGetType::GET_CACHE_MISS_DATA == get_type) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem_block handle is invalid", KR(ret), K(micro_key));
        } else if (OB_FAIL(mem_blk_handle()->get_micro_data(micro_key, io_info.size_, io_buf))) {
          LOG_WARN("fail to get micro data from memory", KR(ret), K(micro_key), "size",
                   io_info.size_, K(micro_info), K(mem_blk_handle));
        } else if (OB_FAIL(handler.handle_hit_memory(micro_key, get_type, macro_id, io_buf, micro_info.crc_,
                   io_info, obj_handle))) {
          LOG_WARN("fail to handle hit memory", KR(ret), K(get_type), K(micro_key), K(phy_micro_id),
                   K(micro_info), K(mem_blk_handle));
        } else {
          if (is_common_io && OB_TMP_FAIL(push_la_micro_key(micro_key, hit_type, micro_info))) {
            LOG_WARN("fail to push latest access micro key", KR(tmp_ret), K(micro_key), K(hit_type), K(micro_info));
          }
          cache_stat_.hit_stat().update_cache_hit(req_delta_cnt, req_delta_size);
          LOG_INFO("ss_cache: get micro_cache from memory", K(micro_key), "size", io_info.size_,
                   K(micro_info), K(mem_blk_handle), K(access_type));
        }
      } else {
        hit_cache = true;
        const uint64_t data_loc = micro_info.data_loc_;
        const uint32_t crc = micro_info.crc_;
        const HandleHitDiskParam param(micro_key, macro_id, data_loc, crc, get_type, access_type);
        if (((ObSSMicroCacheGetType::GET_CACHE_MISS_DATA == get_type)
             && (ObSSMicroCacheHitType::SS_CACHE_HIT_DISK == hit_type))) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_UNLIKELY(!io_info.phy_block_handle_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block handle in io_info is invalid", KR(ret), K(micro_key), K(data_loc));
        } else if (OB_FAIL(handler.handle_hit_disk(param, io_info, obj_handle))) {
          LOG_WARN("fail to handle hit disk", KR(ret), K(param));
        } else {
          if (is_common_io && OB_TMP_FAIL(push_la_micro_key(micro_key, hit_type, micro_info))) {
            LOG_WARN("fail to push latest access micro key", KR(tmp_ret), K(micro_key), K(hit_type), K(micro_info));
          }
          cache_stat_.hit_stat().update_cache_hit(req_delta_cnt, req_delta_size);
          LOG_INFO("ss_cache: get micro_cache from disk", K(micro_key), K(phy_micro_id), K(micro_info), K(access_type));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    cache_stat_.hit_stat().update_fail_get_cnt(1);
  }
  return ret;
}

int ObSSMicroCache::inner_get_micro_block_handle(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaInfo &micro_info,
    ObSSMicroBlockMetaHandle &micro_meta_handle, 
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhyBlockHandle &phy_blk_handle, 
    ObSSMicroCacheHitType &hit_type,
    const uint64_t effective_tablet_id,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  hit_type = ObSSMicroCacheHitType::SS_CACHE_MISS;
  if (OB_FAIL(micro_meta_mgr_.get_micro_block_meta(micro_key, micro_info, micro_meta_handle, mem_blk_handle, 
      phy_blk_handle, effective_tablet_id, update_arc))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get micro_block_meta handle", KR(ret), K(micro_key), K(update_arc), K(effective_tablet_id));
    }
  } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_meta handle must be valid", KR(ret), K(micro_key), K(micro_meta_handle), K(update_arc));
  } else if (micro_info.is_in_ghost_) {  // Exist in B1/B2
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fetched micro_info should be in T1/T2", KR(ret), K(micro_key), K(micro_info));
  } else if (micro_info.is_data_persisted_) {  // Exist in T1/T2, in phy_block
    if (OB_LIKELY(phy_blk_handle.is_valid())) {
      hit_type = ObSSMicroCacheHitType::SS_CACHE_HIT_DISK;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_block handle should be valid", KR(ret), K(micro_key), K(micro_info), K(update_arc));
    }
  } else {  // Exist in T1/T2, in mem_block
    if (OB_LIKELY(mem_blk_handle.is_valid())) {
      hit_type = ObSSMicroCacheHitType::SS_CACHE_HIT_MEM;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_block handle should be valid", KR(ret), K(micro_key), K(micro_info), K(update_arc));
    }
  }

  if (OB_FAIL(ret) || (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type)) {
    micro_meta_handle.reset();
    mem_blk_handle.reset();
    phy_blk_handle.reset();
  }
  return ret;
}

int ObSSMicroCache::delete_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else if (OB_FAIL(micro_meta_mgr_.try_force_delete_micro_block_meta(micro_key))) {
    LOG_WARN("fail to try force delete micro_block meta", KR(ret), K(micro_key));
  }
  return ret;
}

int ObSSMicroCache::simulate_io_result(
    const ObSSMicroCacheGetType get_type,
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle) const
{
  int ret = OB_SUCCESS;
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
  ObIOResult *io_result = nullptr;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(io_info.tenant_id_, tenant_holder))) {
    LOG_WARN("fail to get tenant io manager", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant holder ptr is null", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->alloc_and_init_result(io_info, io_result))) {
    LOG_WARN("fail to alloc and init result", KR(ret), K(io_info));
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", KR(ret));
  } else if (OB_FAIL(obj_handle.get_io_handle().set_result(*io_result))) {
    LOG_WARN("fail to set result", KR(ret));
  } else if (FALSE_IT(io_result->finish_without_accumulate(ret))) {
  } else if (ObSSMicroCacheGetType::GET_CACHE_HIT_DATA == get_type) {
    // do nothing. there is no valid MacroBlockId when GET_CACHE_HIT_DATA (e.g., replica prewarm)
  } else if (OB_FAIL(obj_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSMicroCache::get_cached_micro_block(
    const ObSSMicroBlockCacheKey &micro_key, 
    common::ObIOInfo &io_info,
    blocksstable::ObStorageObjectHandle &obj_handle, 
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  const ObSSMicroBlockId tmp_phy_micro_id;
  bool hit_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_UNLIKELY(!micro_key.is_valid() || (ObSSMicroCacheAccessType::MAX_TYPE == access_type) ||
            (ObSSMicroCacheAccessType::COMMON_IO_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(access_type));
  } else if (OB_FAIL(inner_get_micro_block_cache(micro_key, tmp_phy_micro_id, ObSSMicroCacheGetType::GET_CACHE_HIT_DATA,
             io_info, obj_handle, access_type, hit_cache))) {
    LOG_WARN("fail to inner get micro block cache", KR(ret), K(micro_key), K(io_info), K(obj_handle), K(access_type));
  }
  return ret;
}

int ObSSMicroCache::get_not_exist_micro_blocks(
    const ObIArray<ObSSMicroPrewarmMeta> &in_micro_warmup_infos,
    ObIArray<ObSSMicroPrewarmMeta> &out_micro_warmup_infos)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    const int64_t micro_block_key_cnt = in_micro_warmup_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_block_key_cnt); ++i) {
      const ObSSMicroPrewarmMeta &micro_prewarm_meta = in_micro_warmup_infos.at(i);
      ObSSMicroBlockMetaInfo micro_meta_info;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ObSSMemBlockHandle mem_blk_handle;
      ObSSPhyBlockHandle phy_blk_handle;
      ObSSMicroCacheHitType hit_type = ObSSMicroCacheHitType::SS_CACHE_MISS;
      if (OB_FAIL(inner_get_micro_block_handle(micro_prewarm_meta.micro_key_, micro_meta_info, micro_meta_handle,
          mem_blk_handle, phy_blk_handle, hit_type, ObTabletID::INVALID_TABLET_ID, false/*update_arc*/))) {
        LOG_WARN("fail to inner get micro_block handle", KR(ret), K(i), K(micro_prewarm_meta));
      } else if ((ObSSMicroCacheHitType::SS_CACHE_HIT_MEM == hit_type) ||
                 (ObSSMicroCacheHitType::SS_CACHE_HIT_DISK == hit_type)) {
        // do nothing, already in micro cache
      } else if (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type) {
        if (OB_FAIL(out_micro_warmup_infos.push_back(micro_prewarm_meta))) {
          LOG_WARN("fail to push back", KR(ret), K(micro_prewarm_meta));
        }
      }
    }
  }

  return ret;
}

int ObSSMicroCache::check_micro_block_exist(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaInfo &micro_info,
    ObSSMicroCacheHitType &hit_type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMemBlockHandle mem_blk_handle;
    ObSSPhyBlockHandle phy_blk_handle;
    if (OB_FAIL(inner_get_micro_block_handle(micro_key, micro_info, micro_meta_handle, mem_blk_handle,
        phy_blk_handle, hit_type, ObTabletID::INVALID_TABLET_ID, false/*update_arc*/))) {
      LOG_WARN("fail to inner get micro_block handle", KR(ret), K(micro_key));
    }
  }
  return ret;
}

int ObSSMicroCache::update_micro_block_heat(
    const ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
    const bool transfer_seg,
    const bool update_access_time,
    const int64_t time_delta_s)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else {
    if (OB_FAIL(micro_meta_mgr_.update_micro_block_meta_heat(micro_keys, transfer_seg, update_access_time,
        time_delta_s))) {
      LOG_WARN("fail to update micro_block meta heat", KR(ret), K(micro_keys), K(transfer_seg), K(update_access_time),
        K(time_delta_s));
    } else {
      LOG_INFO("succ to update micro_block heat", K(micro_keys.count()), K(transfer_seg), K(update_access_time));
    }
  }
  return ret;
}

int ObSSMicroCache::is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id, bool &is_filter)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_FAIL(la_micro_key_mgr_.is_tablet_id_need_filter(ls, tablet_id, is_filter))) {
    LOG_WARN("fail to judge is tablet id need filter", KR(ret));
  }
  return ret;
}

int ObSSMicroCache::divide_phy_block_range(
    const ObLSID &ls_id, 
    const int64_t split_count,
    ObIArray<ObSSPhyBlockIdxRange> &block_ranges)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_FAIL(phy_blk_mgr_.divide_cache_data_block_range(ls_id, split_count, block_ranges))) {
    LOG_WARN("fail to divide cache_data_block range", KR(ret), K(ls_id), K(split_count));
  }
  return ret;
}

int ObSSMicroCache::get_batch_la_micro_keys(ObLS *ls, ObIArray<ObSSMicroPrewarmMeta> &keys)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_FAIL(la_micro_key_mgr_.get_batch_micro_keys(ls, keys))) {
    LOG_WARN("fail to get batch micro keys", KR(ret));
  }
  return ret;
}

int ObSSMicroCache::push_la_micro_key(
    const ObSSMicroBlockCacheKey &micro_key, 
    const ObSSMicroCacheHitType &hit_type, 
    const ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if ((ObSSMicroCacheHitType::SS_CACHE_HIT_MEM == hit_type) ||
             (ObSSMicroCacheHitType::SS_CACHE_HIT_DISK == hit_type)) {
    ObSSMicroPrewarmMeta micro_prewarm_meta(micro_key, micro_meta_info.effective_tablet_id_,
                                            micro_meta_info.crc_, micro_meta_info.size_,
                                            micro_meta_info.is_in_l1_);
    if (OB_FAIL(la_micro_key_mgr_.push_la_micro_key_to_hashset(micro_prewarm_meta))) {
      LOG_WARN("fail to push micro_prewarm_meta", KR(ret), K(micro_prewarm_meta));
    }
  }
  return ret;
}

int ObSSMicroCache::update_effective_tablet_id(
    const ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr, 
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_UNLIKELY(micro_key_arr.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key_arr));
  } else {
    const int64_t micro_key_cnt = micro_key_arr.count();
    for (int64_t i = 0; i < micro_key_cnt; ++i) {
      const ObSSMicroBlockCacheKey &micro_key = micro_key_arr.at(i);
      bool succ_update = false;
      if (OB_FAIL(micro_meta_mgr_.update_effective_tablet_id(micro_key, effective_tablet_id, succ_update))) {
        LOG_WARN("fail to update effective tablet_id", KR(ret), K(micro_key), K(effective_tablet_id));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

void ObSSMicroCache::inner_update_micro_ckpt_split_cnt(const int64_t cache_file_size, const bool for_init)
{
  int ret = OB_SUCCESS;
  const int64_t ori_split_cnt = micro_ckpt_split_cnt_;
  const bool is_mini_cache = ObSSMicroCacheUtil::check_mini_cache(cache_file_size);
  const int64_t meta_blk_min_cnt = phy_blk_mgr_.get_meta_block_min_cnt();
  if (for_init) {
    if (OB_UNLIKELY(0 == micro_ckpt_split_cnt_)) { // if it is larger than 0 when init, keep it. Now only used for test
      micro_ckpt_split_cnt_ = ObSSMicroCacheUtil::calc_micro_ckpt_split_cnt(is_mini_cache, meta_blk_min_cnt);
    }
  } else {
    const uint32_t cur_split_cnt = ObSSMicroCacheUtil::calc_micro_ckpt_split_cnt(is_mini_cache, meta_blk_min_cnt);
    if (OB_LIKELY(cur_split_cnt > micro_ckpt_split_cnt_)) {
      micro_ckpt_split_cnt_ = cur_split_cnt;
    }
  }

  if (OB_UNLIKELY(0 == micro_ckpt_split_cnt_)) {
    LOG_WARN("unexpected micro_ckpt_split_cnt", K(is_mini_cache), K(for_init), K_(micro_ckpt_split_cnt));
    micro_ckpt_split_cnt_ = SS_INIT_RNG_MIN_CNT;
  }
  FLOG_INFO("update micro_ckpt split count", K_(micro_ckpt_split_cnt), K(ori_split_cnt), K(cache_file_size), K(for_init));
}

/*-----------------------------------------For prewarm-----------------------------------------*/
void ObSSMicroCache::begin_free_space_for_prewarm()
{
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  inner_update_free_space_for_prewarm(true/*is_start*/);
}

void ObSSMicroCache::finish_free_space_for_prewarm()
{
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  inner_update_free_space_for_prewarm(false/*is_start*/);
}

int ObSSMicroCache::inner_update_free_space_for_prewarm(const bool is_start_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (OB_FAIL(micro_meta_mgr_.update_arc_work_limit_for_prewarm(is_start_op))) {
    LOG_WARN("fail to update arc_work_limit for prewarm", KR(ret), K(is_start_op));
  }
  return ret;
}

int ObSSMicroCache::get_available_space_for_prewarm(int64_t &available_size)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  available_size = 0;
  int64_t cur_available_size = 0;
  int64_t micro_cache_file_max_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (FALSE_IT(micro_cache_file_max_size = 0)) {
  } else if (OB_FAIL(get_cur_available_space_for_prewarm_(cur_available_size))) {
    LOG_WARN("fail to get cur avaiable space", KR(ret), K(micro_cache_file_max_size));
  } else {
    // available_size = current free space + potential grow space.
    // In shrink scenarios or with small tenant disk quota, micro_cache_file_max_size may be smaller than cache_file_size_
    // (micro cache file is initialized to 810MB regardless of disk size) and the file cannot be shrunk, so clamp to 0.
    available_size = cur_available_size + MAX(0, micro_cache_file_max_size - cache_file_size_);
  }
  return ret;
}

int ObSSMicroCache::get_cur_available_space_for_prewarm_(int64_t &available_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
#ifdef ERRSIM
  } else if (OB_SUCCESS != EN_SS_MICRO_CACHE_FULL) {
    available_size = 0;
    LOG_INFO("[ERRSIM] fake EN_MICRO_CACHE_FULL", K(available_size));
#endif
  } else {
    available_size = 0;
    int64_t data_block_max_cnt = 0;
    int64_t data_blk_usage_pct = 0;
    phy_blk_mgr_.get_data_block_info(data_block_max_cnt, data_blk_usage_pct);
    const int64_t cache_limit_size = data_block_max_cnt * phy_blk_size_;

    const ObSSARCInfo &arc_info = micro_meta_mgr_.get_arc_info();
    const int64_t arc_work_limit = arc_info.get_arc_work_limit();
    const int64_t valid_size = arc_info.get_valid_size();

    const int64_t available_pct = MAX(0, SS_LIMIT_PREWARM_SHRINK_PCT - data_blk_usage_pct);
    available_size = cache_limit_size * available_pct / 100;
    available_size = MIN(available_size, arc_work_limit);
    // if throughput of common_io is high, we need to reduce available_size for prewarm
    // TODO: common_io_high_throughput is set 6MB/s temporarily
    if ((available_size > 0) && cache_stat_.cache_load().is_common_io_high_throughput()) {
      available_size = MAX(SS_CLOSE_EVICTION_DIFF_SIZE, available_size - SS_CLOSE_EVICTION_DIFF_SIZE);
    }
    LOG_INFO("get ss_micro_cache space for prewarm", K(available_pct), K(available_size));
  }
  return ret;
}

int ObSSMicroCache::cal_expand_size_for_ls_migration(
    const ObSSMicroCacheLSInfo &src_ls_micro_cache_info,
    int64_t &min_expand_size, int64_t &best_expand_size)
{
  int ret = OB_SUCCESS;
  min_expand_size = 0;
  best_expand_size = 0;
  int64_t available_size = 0;
  int64_t data_cached_size = 0;
  int64_t hot_data_cached_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    // micro cache is disabled. Treat it as no extra space requirement
    // and keep min_expand_size and best_expand_size as 0.
    LOG_INFO("ss_micro_cache is disabled, treat expand size as zero", KR(ret));
  } else if (OB_UNLIKELY(!src_ls_micro_cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls micro cache info is invalid", KR(ret), K(src_ls_micro_cache_info));
  } else if (OB_FAIL(get_cur_available_space_for_prewarm_(available_size))) {
    LOG_WARN("fail to get available space for prewarm", KR(ret), K(src_ls_micro_cache_info));
  } else if (OB_FAIL(get_data_cached_size(data_cached_size, hot_data_cached_size))) {
    LOG_WARN("fail to get data cached size", KR(ret), K(src_ls_micro_cache_info));
  } else {
    const int64_t src_micro_cache_hot_size = src_ls_micro_cache_info.get_hot_size();
    const int64_t src_micro_cache_total_size = src_ls_micro_cache_info.get_valid_size();
    const int64_t dst_size_except_hot = MAX(0, data_cached_size - hot_data_cached_size);
    
    min_expand_size = MAX(0, src_micro_cache_hot_size - available_size - dst_size_except_hot);
    best_expand_size = MAX(0, src_micro_cache_total_size - available_size);
    LOG_INFO("micro cache cal expand size for ls migration",
        KR(ret), K(min_expand_size), K(best_expand_size), 
        K(src_ls_micro_cache_info), K(dst_size_except_hot),
        K(cache_stat_.phy_blk_stat_), K(hot_data_cached_size), K(available_size));
  }
  return ret;
}

void ObSSMicroCache::update_hot_micro_lack_count(const bool full_scan, const int64_t lack_count)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (lack_count >= 0) {
    cache_stat_.hit_stat().update_hot_micro_lack_cnt_info(full_scan, lack_count);
  }
}

int ObSSMicroCache::get_phy_block_handle(const int64_t phy_blk_idx, ObSSPhyBlockHandle &phy_blk_handle)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_FAIL(phy_blk_mgr_.get_block_handle(phy_blk_idx, phy_blk_handle))) {
    LOG_WARN("fail to get phy_block handle", KR(ret), K(phy_blk_idx));
  }
  return ret;
}

int ObSSMicroCache::get_phy_block_size(int64_t &phy_blk_size)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    phy_blk_size = phy_blk_size_;
  }
  return ret;
}

int ObSSMicroCache::get_tablet_cache_info(
    const ObTabletID &tablet_id, 
    ObSSMicroCacheTabletInfo &tablet_cache_info)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (!ATOMIC_LOAD(&is_enabled_) || !ATOMIC_LOAD(&is_read_allowed_)) {
    // When cache is disabled, skip read logic and return with default output.
    tablet_cache_info.reset();
    tablet_cache_info.tablet_id_ = tablet_id;
  } else {
    tablet_cache_info.tablet_id_ = tablet_id;
    ObSSMicroCacheSuperBlk super_blk;
    if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
      LOG_WARN("fail to get ss_super_block", KR(ret));
    } else if (OB_LIKELY(super_blk.is_valid())) {
      if (super_blk.exist_tablet_cache_info_list()) {
        if (OB_FAIL(super_blk.get_tablet_cache_info(tablet_id, tablet_cache_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get tablet_cache_info", KR(ret), K(tablet_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCache::get_ls_cache_info(const ObLSID &ls_id, ObSSMicroCacheLSInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (!ATOMIC_LOAD(&is_enabled_) || !ATOMIC_LOAD(&is_read_allowed_)) {
    // When cache is disabled, skip read logic and return with default output.
    ls_cache_info.reset();
    ls_cache_info.ls_id_ = ls_id;
  } else {
    ls_cache_info.ls_id_ = ls_id;
    ObSSMicroCacheSuperBlk super_blk;
    if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
      LOG_WARN("fail to get ss_super_block", KR(ret));
    } else if (OB_LIKELY(super_blk.is_valid())) {
      // first, try to get ls_cache info from super_block
      if (super_blk.exist_ls_cache_info_list()) {
        if (OB_FAIL(super_blk.get_ls_cache_info(ls_id, ls_cache_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get ls_cache_info", KR(ret), K(ls_id));
          }
        }
      } else {
        // if not exist, use average size
        ObLSService *ls_service = MTL(ObLSService *);
        if (OB_NOT_NULL(ls_service)) {
          ObSEArray<ObLSID, 8> ls_ids;
          if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
            LOG_WARN("fail to get ls_ids", KR(ret), K(ls_id));
          } else if (ls_ids.count() > 0) {
            const int64_t valid_micro_size = micro_meta_mgr_.get_arc_info().get_valid_size();
            ls_cache_info.ls_id_ = ls_id;
            ls_cache_info.t1_size_ = valid_micro_size / 2 / ls_ids.count();
            ls_cache_info.t2_size_ = valid_micro_size / 2 / ls_ids.count();
            ls_cache_info.t1_hot_size_ = 0;
            ls_cache_info.t2_hot_size_ = 0;
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCache::get_data_cached_size(int64_t &data_cached_size, int64_t &hot_data_cached_size)
{
  int ret = OB_SUCCESS;
  data_cached_size = 0;
  hot_data_cached_size = 0;
  ObLSService *ls_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_NOT_NULL(ls_service = MTL(ObLSService *))) {
    ObSEArray<share::ObLSID, 8> ls_ids;
    ls_ids.set_attr(ObMemAttr(tenant_id_, "MicroCacheLSIDs"));
    if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) { 
      LOG_WARN("fail to get ls ids", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < ls_ids.count()); ++i) {
      ObSSMicroCacheLSInfo ls_cache_info;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(get_ls_cache_info(ls_ids.at(i), ls_cache_info))) {
        LOG_WARN("fail to get ls cache info", KR(ret), KR(tmp_ret), K(i), K(ls_ids.at(i)));
        tmp_ret = OB_SUCCESS;
      } else {
        data_cached_size += ls_cache_info.get_valid_size();
        hot_data_cached_size += ls_cache_info.get_hot_size();
      }
    }
  }
  return ret;
}

/*-----------------------------------------For Operation/Maintenance-----------------------------------------*/
bool ObSSMicroCache::is_normal_state() const
{
  return (!is_stopped_ && task_runner_.is_task_open() && cache_file_size_ > 0);
}

void ObSSMicroCache::enable_cache()
{
  ATOMIC_STORE(&is_enabled_, true);
  FLOG_INFO("enable micro cache", K_(tenant_id), K_(is_enabled));
}

void ObSSMicroCache::disable_cache()
{
  ATOMIC_STORE(&is_enabled_, false);
  FLOG_INFO("disable micro cache", K_(tenant_id), K_(is_enabled));
}

bool ObSSMicroCache::is_cache_enabled() const
{
  return ATOMIC_LOAD(&is_enabled_);
}

void ObSSMicroCache::enable_write()
{
  ATOMIC_STORE(&is_write_allowed_, true);
  FLOG_INFO("enable write of ss_micro_cache", K_(tenant_id), K_(is_write_allowed));
}

void ObSSMicroCache::disable_write()
{
  ATOMIC_STORE(&is_write_allowed_, false);
  FLOG_INFO("disable write of ss_micro_cache", K_(tenant_id), K_(is_write_allowed));
}

bool ObSSMicroCache::is_write_allowed() const
{
  return ATOMIC_LOAD(&is_write_allowed_);
}

void ObSSMicroCache::enable_read()
{
  ATOMIC_STORE(&is_read_allowed_, true);
  FLOG_INFO("enable read of ss_micro_cache", K_(tenant_id), K_(is_read_allowed));
}

void ObSSMicroCache::disable_read()
{
  ATOMIC_STORE(&is_read_allowed_, false);
  FLOG_INFO("disable read of ss_micro_cache", K_(tenant_id), K_(is_read_allowed));
}

bool ObSSMicroCache::is_read_allowed() const 
{ 
  return ATOMIC_LOAD(&is_read_allowed_); 
}

void ObSSMicroCache::enable_calibration() 
{ 
  ATOMIC_STORE(&enable_calibration_, true); 
  FLOG_INFO("enable micro cache calibration", K_(tenant_id), K_(enable_calibration));
}

void ObSSMicroCache::disable_calibration() 
{ 
  ATOMIC_STORE(&enable_calibration_, false); 
  FLOG_INFO("disable micro cache calibration", K_(tenant_id), K_(enable_calibration));
}

bool ObSSMicroCache::is_calibration_enabled() const 
{ 
  return ATOMIC_LOAD(&enable_calibration_);
}

bool ObSSMicroCache::try_start_operating()
{
  bool succ_start = false;
  if (ATOMIC_BCAS(&is_operating_, false, true)) {
    succ_start = true;
  }
  return succ_start;
}

// Caller must call disable_write() before this. Otherwise, even if flying_add_cnt drops to 0
// during the wait, new add requests can be submitted immediately after the wait returns.
int ObSSMicroCache::wait_for_add_requests_finish(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t effective_timeout_us = (timeout_us > 0) ? timeout_us : SS_OPERATE_MICRO_CACHE_TIMEOUT_US;
  const int64_t start_time_us = ObTimeUtil::current_time_us();
  bool need_wait = true;
  while (need_wait && (ObTimeUtil::current_time_us() - start_time_us < effective_timeout_us)) {
    const int64_t flying_add_cnt = ATOMIC_LOAD(&flying_add_req_cnt_);
    if (flying_add_cnt <= 0) {
      need_wait = false;
    } else {
      ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
        LOG_INFO("wait for flying add request to finish", K(flying_add_cnt), K_(tenant_id));
      }
    }
  }
  if (need_wait) {
    ret = OB_TIMEOUT;
    LOG_WARN("wait for add requests finish timeout", KR(ret), K_(tenant_id), 
        K_(flying_add_req_cnt), K(effective_timeout_us));
  }
  return ret;
}

int ObSSMicroCache::start_calibration()
{
  int ret = OB_SUCCESS;
  enable_read();
  disable_write();
  if (OB_FAIL(wait_for_add_requests_finish(SS_CALIBRATION_WAIT_TIMEOUT_US))) {
    LOG_WARN("failed to wait for add requests finish", KR(ret), K_(tenant_id));
  } else {
    task_runner_.disable_tasks_for_calibration();
    if (OB_FAIL(OB_E(EventTable::EN_SS_MICRO_CACHE_CALIBRATION_FAIL_IN_START) OB_SUCCESS)) {
      LOG_WARN("errsim: calibration fail in start_calibration", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(task_runner_.wait_tasks_close_for_calibration())) {
      LOG_WARN("failed to wait tasks close for calibration", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

void ObSSMicroCache::finish_calibration(const bool calibration_succeeded)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_operating())) {
    if (calibration_succeeded) {
      if (OB_SUCCESS == EventTable::EN_SS_MICRO_CACHE_CALIBRATION_SKIP_ENABLE_BG_TASKS) {
        task_runner_.enable_tasks_for_calibration();
      }
      enable_write();
      cache_stat_.task_stat().clear_abnormal_flag();
      cache_stat_.task_stat().add_calibration_cnt(1);
    } else {
      LOG_WARN("fail to calibrate ss_micro_cache, write operation will not be allowed", K_(tenant_id));
    }
    finish_operating();
  } else {
    LOG_ERROR("ss_micro_cache is not operating, but calibration is in progress", K_(tenant_id));
  }
}

int ObSSMicroCache::clear_micro_cache(bool for_replay)
{
  int ret = OB_SUCCESS;
  // If not init, or finish stop, or finish start but not finish replay ckpt/resize/calibration, won't clear micro_cache
  if (IS_INIT && !ATOMIC_LOAD(&is_stopped_) && ATOMIC_LOAD(&is_enabled_)) {
    if (try_start_operating()) {
      FLOG_INFO("start clear micro_cache", K_(tenant_id), KP(this));
      disable_cache();
      task_runner_.disable_task();
      
      const int64_t start_time_us = ObTimeUtil::current_time_us();
      bool need_wait = true;
      do {
        const int64_t flying_add_req_cnt = ATOMIC_LOAD(&flying_add_req_cnt_);
        const int64_t flying_get_req_cnt = ATOMIC_LOAD(&flying_get_req_cnt_);
        const int64_t flying_req_cnt = flying_add_req_cnt + flying_get_req_cnt;
        // During replay failure(perform by meta_ckpt_task), meta_ckpt_task, release_cache_task and calibration_task all
        // share the same timer thread. Waiting for them here would deadlock
        // because they can never run while this thread is blocked.
        const bool is_task_closed = for_replay
            ? task_runner_.is_task_closed_for_replay()
            : task_runner_.is_task_closed();
        if (flying_req_cnt > 0 || !is_task_closed) {
          ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
          if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
            FLOG_WARN("still exist request or task not closed", K(flying_add_req_cnt), K(flying_get_req_cnt), K(flying_req_cnt), K_(task_runner));
          }
        } else {
          need_wait = false;
        }
      } while (need_wait && (ObTimeUtil::current_time_us() - start_time_us < SS_OPERATE_MICRO_CACHE_TIMEOUT_US));

      if (OB_UNLIKELY(need_wait)) {
        FLOG_WARN("wait for flying_req_cnt or task_runner to finish timeout", KR(ret), K_(tenant_id), K_(cache_stat));
      } else if (OB_FAIL(phy_blk_mgr_.reformat_ss_super_block(micro_ckpt_split_cnt_))) {
        FLOG_WARN("fail to persist super_blk before clearing micro_cache", KR(ret), K_(micro_ckpt_split_cnt));
      } else {
        micro_meta_mgr_.clear();
        micro_range_mgr_.clear();
        inner_check_micro_meta_cnt();

        mem_blk_mgr_.clear();
        phy_blk_mgr_.clear();
        la_micro_key_mgr_.clear();
        cache_stat_.clear_cache_stat_dynamic_info();
      }
      
      task_runner_.enable_task();
      enable_cache();
      finish_operating();
      FLOG_INFO("finish clear micro_cache", KR(ret), K_(tenant_id), K_(cache_stat), KP(this));
    } else {
      ret = OB_EAGAIN;
      FLOG_INFO("already in clear_micro_cache/resize/calibration process, please wait", KP(this));
    }
  } else {
    FLOG_INFO("no need to clear micro_cache", K_(is_inited), K_(is_stopped), K_(is_enabled), KP(this));
  }
  return ret;
}

int64_t ObSSMicroCache::get_max_micro_block_size()
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  int64_t max_micro_block_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    max_micro_block_size = micro_meta_mgr_.get_max_micro_block_size();
  }
  if (max_micro_block_size <= 0) {
    max_micro_block_size = SS_MICRO_CACHE_MAX_BLOCK_SIZE;
  }
  return max_micro_block_size;
}

int ObSSMicroCache::get_phy_block_info(const int64_t phy_blk_idx, ObSSPhysicalBlock& phy_blk_info)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else {
    ObSSPhyBlockHandle phy_blk_handle;
    if (OB_FAIL(phy_blk_mgr_.get_block_handle(phy_blk_idx, phy_blk_handle))) {
      LOG_WARN("fail to get phy_block info", KR(ret), K(phy_blk_idx));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle should be valid", KR(ret), K(phy_blk_idx), K(phy_blk_handle));
    } else {
      phy_blk_info = *(phy_blk_handle.get_ptr());
    }
  }
  return ret;
}

int ObSSMicroCache::get_micro_meta_info(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_read_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to read", KR(ret), K_(is_read_allowed));
    }
  } else if (OB_FAIL(micro_meta_mgr_.get_micro_meta_info(micro_key, micro_meta_info))) {
    LOG_WARN("fail to get micro_meta info", KR(ret), K(micro_key));
  }
  return ret;
}

int ObSSMicroCache::get_micro_cache_info(
    ObSSMicroCacheStat &cache_stat, 
    ObSSMicroCacheSuperBlk &super_blk, 
    ObSSARCInfo &arc_info)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_get_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (!ATOMIC_LOAD(&is_enabled_) || !ATOMIC_LOAD(&is_read_allowed_)) {
    // When cache is disabled, skip read logic and return with default output.
    cache_stat.reset();
    super_blk.reset();
    arc_info.reset();
  } else if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
    LOG_WARN("fail to get ss_super_block", KR(ret), K(super_blk));
  } else {
    cache_stat = cache_stat_;
    arc_info = micro_meta_mgr_.get_arc_info();
  }
  return ret;
}

int ObSSMicroCache::clear_micro_meta_by_tablet_id(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  int64_t cur_micro_ckpt_op_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (FALSE_IT(cur_micro_ckpt_op_cnt = cache_stat_.task_stat().get_micro_ckpt_op_cnt())) {
  } else if (OB_FAIL(micro_meta_mgr_.record_need_delete_tablet(tablet_id, cur_micro_ckpt_op_cnt))) {
    LOG_WARN("fail to record need delete tablet_id", KR(ret), K(tablet_id));
  }
  return ret;
}

// Not support set compressor type parallelly
void ObSSMicroCache::set_micro_meta_compressor_type(const ObCompressorType type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_add_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_write_allowed_))) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
      LOG_WARN("ss_micro_cache is not allowed to write", KR(ret), K_(is_write_allowed));
    }
  } else if (type != admin_info_.get_micro_meta_compressor_type()) {
    if (OB_FAIL(phy_blk_mgr_.update_compressor_type(type))) {
      LOG_WARN("fail to update compressor_type in super_blk", KR(ret), K(type));
    } else {
      admin_info_.set_micro_meta_compressor_type(type);
    }
  }
}

ObCompressorType ObSSMicroCache::get_micro_meta_compressor_type() const
{
  return admin_info_.get_micro_meta_compressor_type();
}

} // namespace storage
} // namespace oceanbase
