/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/thread_mgr.h"
#include "share/io/ob_io_define.h"
#include "share/config/ob_server_config.h"
#include "share/ob_thread_define.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_evict_entry.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_evict_policy.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/shared_storage/ob_dir_manager.h" 
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::lib;
using namespace oceanbase::share;
/****************************** ObSSMacroCacheBaseCallback ********************************/
ObSSMacroCacheBaseCallback::ObSSMacroCacheBaseCallback(ObSSMacroCacheMgr &macro_cache_mgr)
  : macro_cache_mgr_(macro_cache_mgr)
{
}

ObSSMacroCacheBaseCallback::~ObSSMacroCacheBaseCallback()
{
}


/****************************** ObSSMacroCacheInsertCallback ********************************/
ObSSMacroCacheInsertCallback::ObSSMacroCacheInsertCallback(ObSSMacroCacheMgr &macro_cache_mgr)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr)
{
}

ObSSMacroCacheInsertCallback::~ObSSMacroCacheInsertCallback()
{
}

// Note: if this insert lru_list callback failed, this entry will also be erased from meta_map.
// this ensures consistency between meta_map and lru_list.
int ObSSMacroCacheInsertCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (OB_FAIL(macro_cache_mgr_.add_into_lru_list(*meta_ptr))) {
    LOG_WARN("fail to add into lru list", KR(ret), KPC(meta_ptr), K(entry));
  }
  return ret;
}


/****************************** ObSSMacroCacheGetFdCallback ********************************/
ObSSMacroCacheGetFdCallback::ObSSMacroCacheGetFdCallback(
    const ObTabletID &effective_tablet_id,
    ObSSMacroCacheMgr &macro_cache_mgr,
    ObSSFdCacheHandle &fd_handle)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr), ret_(OB_SUCCESS),
    effective_tablet_id_(effective_tablet_id), is_hit_cache_(false), fd_handle_(fd_handle),
    cache_type_(ObSSMacroCacheType::MAX_TYPE), macro_block_size_(-1)
{
}

ObSSMacroCacheGetFdCallback::~ObSSMacroCacheGetFdCallback()
{
}

int ObSSMacroCacheGetFdCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  io_info.tenant_id_ = MTL_ID();
  io_info.server_id_ = GCONF.observer_id;
  ObSSIOCommonOp common_op;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  // step 1: get fd
  if (OB_FAIL(common_op.get_local_cache_read_device_and_fd(entry.first, 0/*ls_epoch_id*/, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) { // case: 'delete meta' is not recorded into checkpoint
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to get local cache read device and fd", KR(ret), K(entry), K(io_info));
    }
  } else if (OB_FAIL(fd_handle_.assign(io_info.fd_cache_handle_))) {
    LOG_WARN("fail to assign fd cache handle", KR(ret), K(io_info));
  // step 2: update lru when cache hit
  } else if (OB_FAIL(update_lru_list(entry))) {
    LOG_WARN("fail to update lru list", KR(ret), K(entry));
  } else if (OB_FAIL(update_last_access_time(entry))) {
    LOG_WARN("fail to update last access time", KR(ret), K(entry));
  } else if (OB_FAIL(try_update_effective_tablet_id(entry))) {
    LOG_WARN("fail to try update effective tablet id", KR(ret), K(entry));
  } else {
    is_hit_cache_ = true;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else {
    cache_type_ = meta_ptr->get_cache_type();
    macro_block_size_ = meta_ptr->get_size();
  }
  ret_ = ret;
  return ret;
}

int ObSSMacroCacheGetFdCallback::update_lru_list(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (OB_FAIL(macro_cache_mgr_.update_lru_list(*meta_ptr))) {
    LOG_WARN("fail to update lru list", KR(ret), KPC(meta_ptr), K(entry));
  }
  return ret;
}

int ObSSMacroCacheGetFdCallback::update_last_access_time(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else {
    meta_ptr->set_last_access_time_us(ObTimeUtility::current_time_us());
  }
  return ret;
}

int ObSSMacroCacheGetFdCallback::try_update_effective_tablet_id(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (ObTabletID::INVALID_TABLET_ID == effective_tablet_id_.id()) {
    // do nothing. do not update effective_tablet_id.
    // e.g., TMP_FILE, ObIndexBlockScanEstimator, and ObSSTableSecMetaIterator
  } else if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (meta_ptr->get_effective_tablet_id() == effective_tablet_id_.id()) {
    // do nothing. effective_tablet_id has not changed, no need to update effective_tablet_id.
  } else { // meta_ptr->get_effective_tablet_id() != effective_tablet_id_.id()
    meta_ptr->set_effective_tablet_id(effective_tablet_id_.id());
  }
  return ret;
}


/****************************** ObSSMacroCacheGetCacheTypeCallback ********************************/
ObSSMacroCacheGetCacheTypeCallback::ObSSMacroCacheGetCacheTypeCallback(ObSSMacroCacheMgr &macro_cache_mgr)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr), cache_type_(ObSSMacroCacheType::MAX_TYPE),
    ret_(OB_SUCCESS)
{
}

ObSSMacroCacheGetCacheTypeCallback::~ObSSMacroCacheGetCacheTypeCallback()
{
}

int ObSSMacroCacheGetCacheTypeCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else {
    cache_type_ = meta_ptr->get_cache_type();
  }
  ret_ = ret;
  return ret;
}

/****************************** ObSSMacroCacheDeletePred ********************************/
ObSSMacroCacheDeletePred::ObSSMacroCacheDeletePred(ObSSMacroCacheMgr &macro_cache_mgr)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr), is_write_cache_(false), size_(0), ret_(OB_SUCCESS)
{
}

ObSSMacroCacheDeletePred::~ObSSMacroCacheDeletePred()
{
}

bool ObSSMacroCacheDeletePred::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (FALSE_IT(is_write_cache_ = meta_ptr->get_is_write_cache())) {
  } else if (FALSE_IT(size_ = meta_ptr->get_size())) {
  } else if (OB_FAIL(macro_cache_mgr_.remove_from_lru_list(*meta_ptr))) {
    LOG_WARN("fail to remove from lru list", KR(ret), KPC(meta_ptr), K(entry));
  }
  ret_ = ret;
  return (OB_SUCCESS == ret);
}


/****************************** ObSSMacroCacheUpdateParam ********************************/
ObSSMacroCacheUpdateParam::ObSSMacroCacheUpdateParam(
    const bool is_update_lru,
    const bool is_update_last_access_time,
    const bool is_update_effective_tablet_id,
    const bool is_update_size,
    const bool is_update_marked,
    const bool is_update_write_cache)
  : is_update_lru_(is_update_lru), is_update_last_access_time_(is_update_last_access_time),
    is_update_effective_tablet_id_(is_update_effective_tablet_id), is_update_size_(is_update_size),
    is_update_marked_(is_update_marked), is_update_write_cache_(is_update_write_cache)
{
}

ObSSMacroCacheUpdateParam::ObSSMacroCacheUpdateParam(const ObSSMacroCacheUpdateParam &other)
{
  *this = other;
}

ObSSMacroCacheUpdateParam::~ObSSMacroCacheUpdateParam()
{
}

ObSSMacroCacheUpdateParam &ObSSMacroCacheUpdateParam::operator=(const ObSSMacroCacheUpdateParam &other)
{
  if (this != &other) {
    is_update_lru_ = other.is_update_lru_;
    is_update_last_access_time_ = other.is_update_last_access_time_;
    is_update_effective_tablet_id_ = other.is_update_effective_tablet_id_;
    is_update_size_ = other.is_update_size_;
    is_update_marked_ = other.is_update_marked_;
    is_update_write_cache_ = other.is_update_write_cache_;
  }
  return *this;
}


/****************************** ObSSMacroCacheUpdateCallback ********************************/
ObSSMacroCacheUpdateCallback::ObSSMacroCacheUpdateCallback(
    const ObSSMacroCacheInfo &cache_info,
    const ObSSMacroCacheUpdateParam &update_param,
    ObSSMacroCacheMgr &macro_cache_mgr)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr), cache_info_(cache_info),
    update_param_(update_param), ori_size_(0), is_size_updated_(false), ret_(OB_SUCCESS)
{
}

ObSSMacroCacheUpdateCallback::~ObSSMacroCacheUpdateCallback()
{
}

void ObSSMacroCacheUpdateCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else {
    ori_size_ = meta_ptr->get_size();
    if (update_param_.is_update_effective_tablet_id_) {
      meta_ptr->set_effective_tablet_id(cache_info_.effective_tablet_id_);
    }
    if (update_param_.is_update_size_) {
      meta_ptr->set_size(cache_info_.size_);
      is_size_updated_ = true;
    }
    if (update_param_.is_update_last_access_time_) {
      meta_ptr->set_last_access_time_us(ObTimeUtility::current_time_us());
    }
    if (update_param_.is_update_marked_) {
      meta_ptr->set_is_marked(cache_info_.is_marked_);
    }
    if (update_param_.is_update_write_cache_) {
      meta_ptr->set_is_write_cache(cache_info_.is_write_cache_);
    }
    if (update_param_.is_update_lru_) {
      if (OB_FAIL(macro_cache_mgr_.update_lru_list(*meta_ptr))) {
        LOG_WARN("fail to update lru list", KR(ret), KPC(meta_ptr), K(entry));
      }
    }
  }
  ret_ = ret;
}


/*************************** ObSSMacroCacheModifyCacheTypeCallback *****************************/
ObSSMacroCacheModifyCacheTypeCallback::ObSSMacroCacheModifyCacheTypeCallback(
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type,
    const ObSSMacroCacheModifyCacheTypeParam &modify_param,
    ObSSMacroCacheMgr &macro_cache_mgr)
  : ObSSMacroCacheBaseCallback(macro_cache_mgr), ori_cache_type_(ori_cache_type),
    new_cache_type_(new_cache_type), modify_param_(modify_param), ret_(OB_SUCCESS)
{
}

ObSSMacroCacheModifyCacheTypeCallback::~ObSSMacroCacheModifyCacheTypeCallback()
{
}

void ObSSMacroCacheModifyCacheTypeCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (OB_UNLIKELY(!is_valid_ss_macro_cache_type(ori_cache_type_) ||
                         !is_valid_ss_macro_cache_type(new_cache_type_) ||
                         (ori_cache_type_ == new_cache_type_) ||
                         !modify_param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(ori_cache_type_),
             K_SS_MACRO_CACHE_TYPE(new_cache_type_), K(modify_param_));
  } else if (meta_ptr->get_cache_type() != ori_cache_type_) {
    // do nothing
  } else if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == new_cache_type_ && meta_ptr->get_macro_id().is_shared_tablet_sub_meta()) {
    // do nothing, shared sub meta is always treated as ObSSMacroCacheType::MACRO_BLOCK
  } else {
    // Publish before changing meta: scanning uses its type before accounting is updated.
    macro_cache_mgr_.begin_cache_type_change_();
    if (OB_FAIL(macro_cache_mgr_.shift_lru_list(ori_cache_type_, new_cache_type_, *meta_ptr))) {
      LOG_WARN("fail to shift lru list", KR(ret), KPC(meta_ptr),
          K_SS_MACRO_CACHE_TYPE(ori_cache_type_), K_SS_MACRO_CACHE_TYPE(new_cache_type_));
    } else if (OB_FAIL(tnt_disk_space_mgr->shift_file_size(
        ori_cache_type_, new_cache_type_, meta_ptr->get_size()))) {
      LOG_WARN("fail to shift file size", KR(ret), KPC(meta_ptr),
          K_SS_MACRO_CACHE_TYPE(ori_cache_type_), K_SS_MACRO_CACHE_TYPE(new_cache_type_));
    } else {
      // Only a real AUTO-to-HOT conversion transfers cache ownership to the requesting tablet.
      if (modify_param_.update_effective_tablet_id_
          && ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == new_cache_type_) {
        meta_ptr->set_effective_tablet_id(modify_param_.effective_tablet_id_);
      }
      if (modify_param_.touch_cache_) {
        meta_ptr->set_last_access_time_us(ObTimeUtility::current_time_us());
      }
    }
    // Also invalidate scans on failure, since the preceding operation may have
    // partially changed meta.
    macro_cache_mgr_.end_cache_type_change_();
  }
  ret_ = ret;
}


/******************************** ObSSMacroCacheClearMarkedFn **********************************/
int ObSSMacroCacheClearMarkedFn::operator()(SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  if (is_stopped_) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("stop to clear marked", KR(ret));
  } else {
    ObSSMacroCacheMetaHandle &meta_handle = entry.second;
    if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle should not be invalid", KR(ret));
    } else {
      meta_handle()->set_is_marked(false);
    }
  }
  return ret;
}

/******************************** ObSSMacroCacheBatchModifyCacheTypeFn **********************************/
int ObSSMacroCacheBatchModifyCacheTypeFn::operator()(const common::hash::HashSetTypes<blocksstable::MacroBlockId>::pair_type &entry)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &cur_block_id = entry.first;
  if (OB_FAIL(macro_cache_mgr_.modify_macro_cache_type(cur_block_id, ori_cache_type_, new_cache_type_))) {
    LOG_WARN("fail to modify macro cache type", KR(ret), K(cur_block_id), 
             K_SS_MACRO_CACHE_TYPE(ori_cache_type_), K_SS_MACRO_CACHE_TYPE(new_cache_type_));
  }
  return ret;
}

/******************************** ObSSMacroCacheRestoreInsertCallback **********************************/
ObSSMacroCacheRestoreInsertCallback::ObSSMacroCacheRestoreInsertCallback(
    ObSSMacroCacheMgr &macro_cache_mgr)
    : ObSSMacroCacheBaseCallback(macro_cache_mgr)
{
}

ObSSMacroCacheRestoreInsertCallback::~ObSSMacroCacheRestoreInsertCallback()
{
}

int ObSSMacroCacheRestoreInsertCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else if (OB_FAIL(macro_cache_mgr_.restore_entry_for_insert_(*meta_ptr))) {
    LOG_WARN("fail to add into lru list", KR(ret), KPC(meta_ptr), K(entry));
  }
  return ret;
}


/******************************** ObSSMacroCacheRestoreUpdateCallback **********************************/
ObSSMacroCacheRestoreUpdateCallback::ObSSMacroCacheRestoreUpdateCallback(
    ObSSMacroCacheMgr &macro_cache_mgr,
    const ObSSMacroCacheMetaHandle &new_meta_handle)
    : ObSSMacroCacheBaseCallback(macro_cache_mgr), new_meta_handle_(new_meta_handle),
      ret_(OB_SUCCESS), ori_size_(0), is_ori_write_cache_(false), is_executed_(false)
{
}

ObSSMacroCacheRestoreUpdateCallback::~ObSSMacroCacheRestoreUpdateCallback()
{
}

int ObSSMacroCacheRestoreUpdateCallback::operator()(SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *old_meta_ptr = nullptr;
  ObSSMacroCacheMeta *new_meta_ptr = nullptr;
  if (OB_ISNULL(old_meta_ptr = entry.second.get_ptr())
      || OB_ISNULL(new_meta_ptr = new_meta_handle_.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry), K(new_meta_handle_));
  } else if (FALSE_IT(ori_size_ = old_meta_ptr->get_size())) {
  } else if (FALSE_IT(is_ori_write_cache_ = old_meta_ptr->get_is_write_cache())) {
  } else if (OB_FAIL(macro_cache_mgr_.restore_entry_for_update_(*old_meta_ptr, *new_meta_ptr))) {
    LOG_WARN("fail to add into lru list", KR(ret), KPC(old_meta_ptr), KPC(new_meta_ptr), K(entry));
  } else {
    entry.second.set_ptr(new_meta_ptr);
  }
  is_executed_ = true;
  ret_ = ret;
  return ret;
}

/******************************** ObSSMacroCacheTabletStat **********************************/
ObSSMacroCacheTabletStat::ObSSMacroCacheTabletStat()
{
  reset();
}

ObSSMacroCacheTabletStat::~ObSSMacroCacheTabletStat()
{
  reset();
}

void ObSSMacroCacheTabletStat::reset()
{
  queue_space_.reset();
  ObStorageCacheHitStat::reset();
}

void ObSSMacroCacheTabletStat::update_cached_space(
    const bool is_hot,
    const bool is_in_fifo,
    const int64_t block_count,
    const int64_t required_size)
{
  queue_space_.add(is_hot, is_in_fifo, block_count, required_size);
}

void ObSSMacroCacheTabletStat::merge_cached_space(const ObSSMacroCacheQueueSpaceStat &queue_space)
{
  queue_space_.merge(queue_space);
}

/******************************** ObSSMacroCacheMgr **********************************/
ObSSMacroCacheMgr::ObSSMacroCacheMgr()
  : is_inited_(false), is_stopped_(false), is_ckpt_replayed_(false), clear_flag_(), fg_evict_trigger_(false),
    tenant_id_(OB_INVALID_TENANT_ID), high_tg_id_(INVALID_TG_ID), low_tg_id_(INVALID_TG_ID),
    write_cache_size_(0), flying_req_cnt_(0),
    cache_type_change_epoch_(0), cache_type_change_inflight_(0),
    macro_meta_cnt_(0), macro_meta_allocator_(), meta_map_(),
    macro_cache_lock_(), flush_map_(false/*use_promise*/), flush_task_(*this, is_stopped_),
    write_cache_flush_map_(true/*use_promise*/), write_cache_flush_task_(*this, is_stopped_),
    evict_task_(is_stopped_), expire_task_(is_stopped_), hot_tablet_macro_cache_gc_task_(is_stopped_),
    hot_tablet_old_sstable_evict_task_(is_stopped_), write_cache_ctrl_task_(is_stopped_),
    ckpt_task_(is_stopped_), replay_task_(is_stopped_), tablet_stat_map_(), tablet_stat_task_(is_stopped_),
    ls_info_map_(), external_table_cache_stat_()
{
  for (uint8_t i = 0; i < SS_MACRO_CACHE_MAX_TYPE_VAL; ++i) {
    macro_caches_[i] = nullptr;
    macro_caches_hit_stat_[i].reset();
  }
  for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; ++i) {
    macro_blocks_stat_[i].reset();
  }
}

ObSSMacroCacheMgr::~ObSSMacroCacheMgr()
{
  destroy();
}

int ObSSMacroCacheMgr::mtl_init(ObSSMacroCacheMgr *&macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_cache_mgr->init(MTL_ID()))) {
    LOG_WARN("fail to init macro cache mgr", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t meta_map_bucket_num = -1;
  int64_t tablet_stat_bucket_num = -1;
  const int64_t GB = 1 * 1024L * 1024L * 1024L;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  int64_t ckpt_parallelism = 0;
  if ((tenant_mem / GB) <= 4) { // tenant_mem <= 4GB
    // 9w buckets. actually cal_next_prime return 9.8w buckets, which cost 2.3MB memory
    meta_map_bucket_num = 90000;
    ckpt_parallelism = 4;
    // 6000 buckets. actually cal_next_prime return 6151 buckets, which cost 0.144MB memory
    tablet_stat_bucket_num = 6000;
  } else { // tenant_mem > 4GB
    // 100w buckets.  actually cal_next_prime return 157w buckets, which cost 37MB memory
    meta_map_bucket_num = 1000000;
    ckpt_parallelism = 16;
    // 9w buckets. actually cal_next_prime return 9.8w buckets, which cost 2.3MB memory
    tablet_stat_bucket_num = 90000;
  }
  ObMemAttr attr(tenant_id, "MacroCacheMgr");
  const int64_t macro_cache_meta_size = sizeof(ObSSMacroCacheMeta);
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro cache mgr init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(macro_meta_allocator_.init(macro_cache_meta_size, "MacroMetaAlloc",
                                                tenant_id, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("fail to init macro meta allocator", KR(ret), K(macro_cache_meta_size), K(tenant_id));
  } else if (OB_FAIL(meta_map_.create(meta_map_bucket_num, attr))) {
    LOG_WARN("fail to create meta map", KR(ret), K(meta_map_bucket_num));
  } else if (OB_FAIL(tablet_stat_map_.create(tablet_stat_bucket_num, attr))) {
    LOG_WARN("fail to create tablet stat map", KR(ret), K(tablet_stat_bucket_num));
  } else if (OB_FAIL(ls_info_map_.create(10/*bucket_num*/, attr))) {
    LOG_WARN("fail to create ls info map", KR(ret));
  } else if (OB_FAIL(create_macro_caches())) {
    LOG_WARN("fail to create macro caches", KR(ret), K(tenant_id));
  } else if (OB_FAIL(flush_map_.create(100/*bucket_num*/, attr))) {
    LOG_WARN("fail to create write cache flush map", KR(ret));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(flush_task_.init(file_manager))) {
    LOG_WARN("fail to init flush task", KR(ret), KP(file_manager));
  } else if (OB_FAIL(write_cache_flush_map_.create(100/*bucket_num*/, attr))) {
    LOG_WARN("fail to create write cache flush map", KR(ret));
  } else if (OB_FAIL(write_cache_flush_task_.init(file_manager))) {
    LOG_WARN("fail to init write cache flush task", KR(ret), KP(file_manager));
  } else if (OB_FAIL(evict_task_.init(this))) {
    LOG_WARN("fail to init evict task", KR(ret));
  } else if (OB_FAIL(expire_task_.init(this))) {
    LOG_WARN("fail to init expire task", KR(ret));
  } else if (OB_FAIL(hot_tablet_macro_cache_gc_task_.init(this))) {
    LOG_WARN("fail to init hot tablet macro cache gc task", KR(ret));
  } else if (OB_FAIL(hot_tablet_old_sstable_evict_task_.init(this))) {
    LOG_WARN("fail to init hot tablet old major gc task", KR(ret));
  } else if (OB_FAIL(write_cache_ctrl_task_.init(this))) {
    LOG_WARN("fail to init write cache ctrl task", KR(ret));
  } else if (OB_FAIL(ckpt_task_.init(this, ckpt_parallelism))) {
    LOG_WARN("fail to init ckpt task", KR(ret), K(ckpt_parallelism));
  } else if (OB_FAIL(replay_task_.init(this))) {
    LOG_WARN("fail to init replay task", KR(ret));
  } else if (OB_FAIL(tablet_stat_task_.init())) {
    LOG_WARN("fail to init tablet stat task", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init macro cache mgr", K(tenant_id));
  }
  return ret;
}

int ObSSMacroCacheMgr::start()
{
  int ret = OB_SUCCESS;
  int64_t file_mgr_tg_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not init", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MacroCacheHTimer, high_tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MacroCacheLTimer, low_tg_id_))) {
    LOG_WARN("fail to create low priority timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(high_tg_id_))) {
    LOG_WARN("fail to start high priority timer thread", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(TG_START(low_tg_id_))) {
    LOG_WARN("fail to start low priority timer thread", KR(ret), K_(low_tg_id));
  } else if (OB_FAIL(flush_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start flush task", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(write_cache_flush_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start write cache flush task", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(evict_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start evict task", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(expire_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start expire task", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(write_cache_ctrl_task_.start(high_tg_id_))) {
    LOG_WARN("fail to start write cache ctrl task", KR(ret), K_(high_tg_id));
  } else if (OB_FAIL(hot_tablet_macro_cache_gc_task_.start(low_tg_id_))) {
    LOG_WARN("fail to start hot tablet macro cache gc task", KR(ret), K_(low_tg_id));
  } else if (OB_FAIL(hot_tablet_old_sstable_evict_task_.start(low_tg_id_))) {
    LOG_WARN("fail to start hot tablet old major gc task", KR(ret), K_(low_tg_id));
  } else if (OB_FAIL(ckpt_task_.start(low_tg_id_))) {
    LOG_WARN("fail to start ckpt task", KR(ret), K_(low_tg_id));
  } else if (OB_FAIL(replay_task_.start(low_tg_id_))) {
    LOG_WARN("fail to start replay task", KR(ret), K_(low_tg_id));
  } else if (OB_FAIL(tablet_stat_task_.start(low_tg_id_))) {
    LOG_WARN("fail to start tablet stat task", KR(ret), K_(low_tg_id));
  } else {
    is_stopped_ = false;
    LOG_INFO("succ to start macro cache mgr", K(file_mgr_tg_id), K_(high_tg_id), K_(low_tg_id));
  }
  return ret;
}

void ObSSMacroCacheMgr::stop()
{
  LOG_INFO("start to stop macro cache mgr");
  is_stopped_ = true;
  if (INVALID_TG_ID != high_tg_id_) {
    TG_STOP(high_tg_id_);
  }
  if (INVALID_TG_ID != low_tg_id_) {
    TG_STOP(low_tg_id_);
  }
  LOG_INFO("finish to stop macro cache mgr");
}

int ObSSMacroCacheMgr::wait()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to wait macro cache mgr");
  if (INVALID_TG_ID != high_tg_id_) {
    TG_WAIT(high_tg_id_);
  }
  if (INVALID_TG_ID != low_tg_id_) {
    TG_WAIT(low_tg_id_);
  }
  LOG_INFO("finish to wait macro cache mgr");
  return ret;
}

void ObSSMacroCacheMgr::destroy()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::fast_current_time();
  LOG_INFO("start to destroy macro cache mgr");
  is_stopped_ = true;

  if (INVALID_TG_ID != high_tg_id_) {
    TG_DESTROY(high_tg_id_);
  }
  high_tg_id_ = INVALID_TG_ID;
  if (INVALID_TG_ID != low_tg_id_) {
    TG_DESTROY(low_tg_id_);
  }
  low_tg_id_ = INVALID_TG_ID;

  if (IS_INIT) {
    tablet_stat_task_.destroy();
    replay_task_.destroy();
    ckpt_task_.destroy();
    write_cache_ctrl_task_.destroy();
    hot_tablet_old_sstable_evict_task_.destroy();
    hot_tablet_macro_cache_gc_task_.destroy();
    expire_task_.destroy();
    evict_task_.destroy();
    write_cache_flush_task_.destroy();
    write_cache_flush_map_.destroy();
    flush_task_.destroy();
    flush_map_.destroy();
    // wait all the access of macro_caches finish
    while (ATOMIC_LOAD(&flying_req_cnt_) > 0) {
      if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
        LOG_WARN("there still exists req which may access macro caches", "flying_req_cnt",
                 ATOMIC_LOAD(&flying_req_cnt_));
      }
      ob_usleep(100 * 1000L); // 100ms
    }
    // destroy macro caches, after all the access of macro_caches finish
    for (uint8_t i = 0; (i < SS_MACRO_CACHE_MAX_TYPE_VAL); ++i) {
      OB_DELETE(ObSSMacroCache, "SSMacroCache", macro_caches_[i]);
      macro_caches_hit_stat_[i].reset();
    }
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; ++i) {
      macro_blocks_stat_[i].reset();
    }

    ls_info_map_.destroy();
    tablet_stat_map_.destroy();
    meta_map_.destroy();
    // wait destruction of all macro metas
    while (get_macro_meta_alloc_cnt() > 0) {
      if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
        LOG_WARN("there still exists macro meta which has not been destructed",
                "macro_meta_alloc_cnt", get_macro_meta_alloc_cnt());
      }
      ob_usleep(100 * 1000L); // 100ms
    }
    // destroy macro meta allocator, after destruction of all macro metas
    macro_meta_allocator_.destroy();
  }

  external_table_cache_stat_.reset();
  macro_meta_cnt_ = 0;
  flying_req_cnt_ = 0;
  ATOMIC_STORE(&cache_type_change_epoch_, 0);
  ATOMIC_STORE(&cache_type_change_inflight_, 0);
  write_cache_size_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  fg_evict_trigger_ = false;
  clear_flag_.reset();
  is_ckpt_replayed_ = false;
  is_inited_ = false;
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to destroy macro cache mgr", K(cost_us));
}

bool ObSSMacroCacheMgr::is_valid() const
{
  return (is_inited_ && !is_stopped_);
}

int ObSSMacroCacheMgr::put(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheMetaHandle meta_handle;
  ObSSMacroCacheInsertCallback insert_callback(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(cache_info));
  } else if (((ObSSMacroCacheType::MACRO_BLOCK == cache_info.cache_type_) ||
              (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_info.cache_type_)) &&
              (SSObjUtil::has_effective_tablet_id(macro_id)) &&
              (ObTabletID::INVALID_TABLET_ID == cache_info.effective_tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet id", KR(ret), K(macro_id), K(cache_info), K(lbt()));
  } else if (OB_FAIL(construct_meta_handle(macro_id, cache_info, meta_handle))) {
    LOG_WARN("fail to construct meta handle", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta handle", KR(ret), K(macro_id), K(meta_handle), K(cache_info));
  } else if (OB_FAIL(meta_map_.set_refactored(macro_id, meta_handle, 0/*overwrite*/, 0/*broadcast*/,
                                              0/*overwrite_key*/, &insert_callback))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. may put concurrently
      LOG_INFO("macro cache is already exist", K(macro_id), K(meta_handle), K(cache_info));
    } else {
      LOG_WARN("fail to set refactored meta map", KR(ret), K(macro_id), K(meta_handle), K(cache_info));
    }
  } else if (cache_info.is_write_cache_) {
    inc_write_cache_size(macro_id, cache_info.size_);
  }
  return ret;
}

int ObSSMacroCacheMgr::get(
    const MacroBlockId &macro_id,
    const ObTabletID &effective_tablet_id,
    const int64_t size,
    bool &is_hit_cache,
    ObSSFdCacheHandle &fd_handle)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  is_hit_cache = false;
  ObSSMacroCacheGetFdCallback get_fd_callback(effective_tablet_id, *this, fd_handle);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.read_atomic(macro_id, get_fd_callback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to read atomic", KR(ret), K(macro_id));
    }
  } else if (OB_FAIL(get_fd_callback.get_ret())) {
    LOG_WARN("fail to execute get fd callback", KR(ret), K(get_fd_callback), K(macro_id));
  } else {
    is_hit_cache = get_fd_callback.is_hit_cache();
  }

  if (OB_SUCC(ret)) {
    try_update_hit_stat_(macro_id, effective_tablet_id, size, get_fd_callback);
  }
  return ret;
}

int ObSSMacroCacheMgr::check_if_write_cache_exist(const MacroBlockId &macro_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  is_exist = false; 
  ObSSMacroCacheMetaHandle meta_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.get_refactored(macro_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(macro_id));
    }
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta handle", KR(ret), K(macro_id), K(meta_handle));
  } else if (meta_handle()->get_is_write_cache()) {
    is_exist = true;
  }
  return ret;
}

void ObSSMacroCacheMgr::try_update_hit_stat_(
    const MacroBlockId &macro_id,
    const ObTabletID &effective_tablet_id,
    const int64_t size,
    const ObSSMacroCacheGetFdCallback &get_fd_callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || (size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(size));
  } else if (ObSSLocalCacheService::is_need_update_hit_stat()) {
    const bool is_hit_cache = get_fd_callback.is_hit_cache();
    const int64_t hit_size = (is_hit_cache ? MIN(size, get_fd_callback.get_macro_block_size()) : size);
    update_block_hit_stat_(is_hit_cache, macro_id, hit_size);
    update_tablet_hit_stat_(is_hit_cache, macro_id, effective_tablet_id, hit_size);
    if (is_hit_cache) {
      update_cache_hit_stat_(is_hit_cache, get_fd_callback.get_cache_type(), hit_size);
    } else {
      ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
      if (OB_FAIL(get_macro_cache_type(macro_id.storage_object_type(), effective_tablet_id.id(),
                                       true/*use_effective_tablet_id*/, cache_type))) {
        LOG_WARN("fail to get_macro_cache_type", KR(ret), K(macro_id), K(effective_tablet_id));
      } else {
        if (ObSSMacroCacheType::TMP_FILE == cache_type) {
          if (size < ObPrereadCacheManager::NOT_PREREAD_IO_SIZE) {
            update_cache_hit_stat_(is_hit_cache, cache_type, hit_size);
          } else {
            // do not update hit stat, as (size >= NOT_PREREAD_IO_SIZE) TMP_FILE io does not preread
          }
        } else {
          update_cache_hit_stat_(is_hit_cache, cache_type, hit_size);
        }
      }
    }
  } else { // !is_need_update_hit_stat
    // do nothing
  }
}

// internal func, skips initialization check
void ObSSMacroCacheMgr::update_cache_hit_stat_(
    const bool is_hit, const ObSSMacroCacheType cache_type, const int64_t size)
{
  if (OB_LIKELY(is_valid_ss_macro_cache_type(cache_type) && size >= 0)) {
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    ObStorageCacheHitStat &hit_stat = macro_caches_hit_stat_[idx];
    if (is_hit) {
      hit_stat.update_cache_hit(1/*delta_cnt*/, size);
    } else {
      hit_stat.update_cache_miss(1/*delta_cnt*/, size);
    }
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid call of update_cache_hit_stat_",
        K(is_hit), K_SS_MACRO_CACHE_TYPE(cache_type), K(size));
  }
}

// internal func, skips initialization check
void ObSSMacroCacheMgr::update_block_hit_stat_(
    const bool is_hit, const blocksstable::MacroBlockId &macro_id, const int64_t size)
{
  if (OB_LIKELY(macro_id.is_valid() && size >= 0)) {
    ObSSMacroBlockType block_type = ObSSMacroBlockType::MAX_TYPE;
    SSObjUtil::get_ss_macro_block_type(macro_id, block_type);
    if (OB_LIKELY(is_valid_ss_macro_block_type(block_type))) {
      const uint8_t idx = static_cast<uint8_t>(block_type);
      ObSSCacheStatInfo &hit_stat = macro_blocks_stat_[idx];
      hit_stat.update_hit_stat(is_hit, 1/*delta_cnt*/, size);
    }
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid call of update_block_hit_stat_",
        K(is_hit), K(macro_id), K(size));
  }
}

class ObSSMacroCacheTabletStatAddFunc
{
public:
  ObSSMacroCacheTabletStatAddFunc(const ObSSMacroCacheTabletStat &tablet_stat)
      : tablet_stat_(tablet_stat)
  {}

  void operator()(SSMacroCacheTabletStatMapPairType &entry)
  {
    entry.second.update_cache_hit(tablet_stat_.cache_hit_cnt_, tablet_stat_.cache_hit_bytes_);
    entry.second.update_cache_miss(tablet_stat_.cache_miss_cnt_, tablet_stat_.cache_miss_bytes_);
  }

private:
  const ObSSMacroCacheTabletStat &tablet_stat_;
};

void ObSSMacroCacheMgr::update_tablet_hit_stat_(
    const bool is_hit, const blocksstable::MacroBlockId &macro_id,
    const common::ObTabletID &tablet_id, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(tablet_id.is_valid() && size >= 0)) {
    ObSSMacroCacheTabletStat tablet_stat;
    if (is_hit) {
      tablet_stat.update_cache_hit(1/*delta_cnt*/, size);
    } else {
      tablet_stat.update_cache_miss(1/*delta_cnt*/, size);
    }
    ObSSMacroCacheTabletStatAddFunc add_func(tablet_stat);

    if (OB_FAIL(tablet_stat_map_.set_or_update(tablet_id.id(), tablet_stat, add_func))) {
      LOG_WARN("fail to update tablet_stat_map_ hit stat", KR(ret),
          K(is_hit), K(tablet_id), K(tablet_stat), K(macro_id));
    }
  } else if (OB_UNLIKELY(size < 0)) {
    // tablet_id may be invalid in some path,
    // such as ObIndexBlockScanEstimator/ObSSTableSecMetalIterator/external table...
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid call of update_tablet_hit_stat_",
        KR(ret), K(is_hit), K(tablet_id), K(size), K(macro_id));
  }
}

int ObSSMacroCacheMgr::exist(const MacroBlockId &macro_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  is_exist = false;
  ObSSMacroCacheMetaHandle meta_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.get_refactored(macro_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(macro_id));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObSSMacroCacheMgr::erase(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  bool is_erased = false;
  ObSSMacroCacheDeletePred delete_pred(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.erase_if(macro_id, delete_pred, is_erased))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. may be already erased
    } else {
      LOG_WARN("fail to erase if meta map", KR(ret), K(macro_id));
    }
  } else if (!is_erased) {
    ret = delete_pred.get_ret();
    LOG_WARN("fail to erase", KR(ret), K(macro_id), K(delete_pred));
  } else if (is_erased && delete_pred.get_is_write_cache()) {
    dec_write_cache_size(macro_id, delete_pred.get_size());
  }
  return ret;
}

int ObSSMacroCacheMgr::update(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info,
    const ObSSMacroCacheUpdateParam &update_param)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheUpdateCallback update_callback(cache_info, update_param, *this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_FAIL(meta_map_.atomic_refactored(macro_id, update_callback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to atomic refactored meta map", KR(ret), K(macro_id), K(cache_info));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = update_callback.get_ret()))) {
    LOG_WARN("fail to execute meta map update callback", KR(ret), K(macro_id), K(update_callback));
  } else if (update_param.is_update_write_cache_ && !cache_info.is_write_cache_) {
    dec_write_cache_size(macro_id, update_callback.get_ori_size());
  }
  return ret;
}

int ObSSMacroCacheMgr::update_effective_tablet_id(
    const ObIArray<MacroBlockId> &macro_id_arr,
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  // NOTICE: to make cache_info is valid, we need to put some fake value here.
  ObSSMacroCacheInfo cache_info(effective_tablet_id.id(), 1/*size*/,
                                ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ObSSMacroCacheUpdateParam update_param(false/*is_update_lru*/,
                                         false/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         false/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  const int64_t macro_id_cnt = macro_id_arr.count();
  for (int64_t i = 0; i < macro_id_cnt; ++i) {
    const MacroBlockId &cur_macro_id = macro_id_arr.at(i);
    if (OB_FAIL(update(cur_macro_id, cache_info, update_param))) {
      LOG_WARN("fail to update macro cache meta", KR(ret), K(cur_macro_id), K(cache_info), K(update_param));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::clear_macro_cache(const int64_t abs_timeout_us, const bool is_hot_tablet)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 10000;
  int64_t evict_cnt = 0;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "ClearMacroC"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    bool &clear_flag = (is_hot_tablet ? clear_flag_.is_clear_hot_macro_cache_
                                      : clear_flag_.is_clear_macro_cache_);
    if (ATOMIC_BCAS(&clear_flag, false, true)) {
      SSMacroCacheMetaMap::bucket_iterator bucket_iter = meta_map_.bucket_begin();
      while (OB_SUCC(ret) && !is_stopped_ && (ObTimeUtility::current_time_us() < abs_timeout_us) &&
            (bucket_iter != meta_map_.bucket_end())) {
        {
          SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
          SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
          SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
          while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
            const MacroBlockId &macro_id = node_iter->first;
            const ObSSMacroCacheMetaHandle &meta_handle = node_iter->second;
            if (macro_id.is_private_macro() && macro_id.is_meta()) {
              // skip private meta macro
            } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
            } else if (is_hot_tablet) { // 1. hot tablet
              if ((meta_handle()->get_cache_type() != ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) ||
                   meta_handle()->get_is_write_cache()) { // do not clear write cache
                // do nothing
              } else if (OB_FAIL(macro_ids.push_back(macro_id))) {
                LOG_WARN("fail to push back", KR(ret), K(macro_id));
              }
            } else { // 2. non-hot tablet
              if ((meta_handle()->get_cache_type() == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) ||
                  meta_handle()->get_is_write_cache()) { // do not clear write cache
                // do nothing
              } else if (OB_FAIL(macro_ids.push_back(macro_id))) {
                LOG_WARN("fail to push back", KR(ret), K(macro_id));
              }
            }
            node_iter++;
          }
          ++bucket_iter;
          ++bucket_cnt;
        }
        if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt) && !macro_ids.empty()) {
          if (OB_FAIL(batch_evict_by_macro_id_(macro_ids))) {
            LOG_WARN("fail to batch evict by macro id", KR(ret), K(macro_ids));
          } else {
            evict_cnt += macro_ids.count();
          }
          macro_ids.reuse();
          bucket_cnt = 0;
        }
      }
      if (OB_SUCC(ret) && !macro_ids.empty()) {
        if (OB_FAIL(batch_evict_by_macro_id_(macro_ids))) {
          LOG_WARN("fail to batch evict by macro id", KR(ret), K(macro_ids));
        } else {
          evict_cnt += macro_ids.count();
        }
      }
      ATOMIC_STORE(&clear_flag, false); // restore clear_flag
    } else {
      ret = OB_EAGAIN;
      FLOG_INFO("already in clear_macro_cache process, please wait", KP(this));
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("finish to clear macro cache", KR(ret), K(abs_timeout_us), K(cost_us));
  return ret;
}

int ObSSMacroCacheMgr::clear_macro_cache_meta()
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 10000;
  int64_t erased_cnt = 0;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "ClearMCMeta"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    SSMacroCacheMetaMap::bucket_iterator bucket_iter = meta_map_.bucket_begin();
    while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != meta_map_.bucket_end())) {
      {
        SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
        SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
          if (OB_FAIL(macro_ids.push_back(node_iter->first))) {
            LOG_WARN("fail to push back", KR(ret), "macro_id", node_iter->first);
          }
          node_iter++;
        }
        ++bucket_iter;
        ++bucket_cnt;
      }
      if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt)) {
        if (OB_FAIL(batch_erase(macro_ids))) {
          LOG_WARN("fail to batch erase", KR(ret), K(macro_ids));
        } else {
          erased_cnt += macro_ids.count();
        }
        macro_ids.reuse();
        bucket_cnt = 0;
      }
    }
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(batch_erase(macro_ids))) {
        LOG_WARN("fail to batch erase", KR(ret), K(macro_ids));
      } else {
        erased_cnt += macro_ids.count();
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (uint8_t i = 0; (i < SS_MACRO_CACHE_MAX_TYPE_VAL) && OB_SUCC(ret); ++i) {
      ObSSMacroCache *cur_macro_cache = macro_caches_[i];
      if (OB_ISNULL(cur_macro_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro cache is null", KR(ret), K(i));
      } else if (OB_UNLIKELY(cur_macro_cache->get_size() > 0)) {
        LOG_ERROR("macro cache is not empty", K(i));
      }
    }
  }

  FLOG_INFO("finish to clear macro cache meta", KR(ret), K(erased_cnt), K(meta_map_.size()));
  return ret;
}

int ObSSMacroCacheMgr::update_is_marked(const MacroBlockId &macro_id, const bool is_marked)
{
  // NOTICE: to make cache_info is valid, we need to put some fake value here.
  ObSSMacroCacheInfo cache_info(1/*effective_tablet_id*/, 1/*size*/,
                                ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ObSSMacroCacheUpdateParam update_param(false/*is_update_lru*/,
                                         false/*is_update_last_access_time*/,
                                         false/*is_update_effective_tablet_id*/,
                                         false/*is_update_size*/,
                                         true/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  cache_info.is_marked_ = is_marked;
  return update(macro_id, cache_info, update_param);
}

int ObSSMacroCacheMgr::clear_is_marked()
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheClearMarkedFn clear_marked_fn(is_stopped_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(meta_map_.foreach_refactored(clear_marked_fn))) {
    LOG_WARN("fail to foreach refactored", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheMgr::batch_modify_cache_type(const common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_ids, 
                                               const ObSSMacroCacheType ori_cache_type,
                                               const ObSSMacroCacheType new_cache_type)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheBatchModifyCacheTypeFn modify_fn(*this, ori_cache_type, new_cache_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(macro_block_ids.foreach_refactored(modify_fn))) {
    LOG_WARN("fail to foreach refactored", KR(ret), K_SS_MACRO_CACHE_TYPE(ori_cache_type), K_SS_MACRO_CACHE_TYPE(new_cache_type));
  }
  return ret;
}


int ObSSMacroCacheMgr::erase_unmarked_macro_cache_meta(const int64_t start_calc_size_time_us)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  const int64_t SAFE_ERASE_INTERVAL_US = 3600 * 1000L * 1000L; // 1h
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 10000;
  int64_t erased_cnt = 0;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "EraseUnmark"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    SSMacroCacheMetaMap::bucket_iterator bucket_iter = meta_map_.bucket_begin();
    while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != meta_map_.bucket_end())) {
      {
        SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
        SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
          const ObSSMacroCacheMetaHandle &meta_handle = node_iter->second;
          if (OB_UNLIKELY(!meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
          } else if ((false == meta_handle()->get_is_marked()) &&
                     ((start_calc_size_time_us - meta_handle()->get_last_access_time_us()) > SAFE_ERASE_INTERVAL_US)) {
            if (OB_FAIL(macro_ids.push_back(node_iter->first))) {
              LOG_WARN("fail to push back", KR(ret), "macro_id", node_iter->first);
            }
          }
          node_iter++;
        }
        ++bucket_iter;
        ++bucket_cnt;
      }
      if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt)) {
        if (OB_FAIL(batch_erase(macro_ids))) {
          LOG_WARN("fail to batch erase", KR(ret), K(macro_ids));
        } else {
          erased_cnt += macro_ids.count();
        }
        macro_ids.reuse();
        bucket_cnt = 0;
      }
    }
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(batch_erase(macro_ids))) {
        LOG_WARN("fail to batch erase", KR(ret), K(macro_ids));
      } else {
        erased_cnt += macro_ids.count();
      }
    }
  }
  FLOG_INFO("finish to erase unmarked macro cache meta", KR(ret), K(erased_cnt));
  return ret;
}

int ObSSMacroCacheMgr::put_or_update(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheMetaHandle meta_handle;
  ObSSMacroCacheInsertCallback insert_callback(*this);
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  ObSSMacroCacheUpdateCallback update_callback(cache_info, update_param, *this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_FAIL(construct_meta_handle(macro_id, cache_info, meta_handle))) {
    LOG_WARN("fail to construct meta handle", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta handle", KR(ret), K(macro_id), K(meta_handle), K(cache_info));
  } else if (OB_FAIL(meta_map_.set_or_update(macro_id, meta_handle, insert_callback, update_callback))) {
    LOG_WARN("fail to set or update meta map", KR(ret), K(macro_id), K(meta_handle), K(cache_info));
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = update_callback.get_ret()))) {
    LOG_WARN("fail to execute meta map update callback", KR(ret), K(macro_id), K(update_callback));
  } else if (cache_info.is_write_cache_) {
    if (update_callback.is_size_udpated()) { // 1. update_callback
      inc_write_cache_size(macro_id, cache_info.size_ - update_callback.get_ori_size());
    } else { // 2. insert_callback
      inc_write_cache_size(macro_id, cache_info.size_);
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache_meta(
    const MacroBlockId &macro_id,
    bool &is_exist,
    ObSSMacroCacheMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.get_refactored(macro_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(macro_id));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache_type(
    const MacroBlockId &macro_id,
    ObSSMacroCacheType &cache_type)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheGetCacheTypeCallback get_cache_type_callback(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(meta_map_.read_atomic(macro_id, get_cache_type_callback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
      // for macro cache without meta, return base macro cache type according to object_type
      const ObStorageObjectType object_type = macro_id.storage_object_type();
      if (OB_FAIL(ObSSMacroCacheMgr::get_macro_cache_type(
                                     object_type,
                                     ObTabletID::INVALID_TABLET_ID/*effective_tablet_id*/,
                                     false/*use_effective_tablet_id*/,
                                     cache_type))) {
        LOG_WARN("fail to get macro cache type", KR(ret), "storage object type", STI(macro_id.storage_object_type()));
      }
    } else {
      LOG_WARN("fail to read atomic", KR(ret), K(macro_id));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = get_cache_type_callback.get_ret()))) {
    LOG_WARN("fail to execute get cache type callback", KR(ret), K(macro_id), K(get_cache_type_callback));
  } else {
    cache_type = get_cache_type_callback.get_cache_type();
  }
  return ret;
}

int ObSSMacroCacheMgr::force_evict_by_macro_id(const MacroBlockId &macro_id)
{
  ObSSMacroCacheMetaHandle meta_handle;
  bool is_exist = false;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  ObSSMacroCache *macro_cache = nullptr;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_macro_cache_meta(macro_id, is_exist, meta_handle))) {
    LOG_WARN("fail to get ss macro cache meta", KR(ret));
  } else if (!is_exist) {
    LOG_INFO("ss macro cache does not exist", K(macro_id));
  } else if (OB_FAIL(get_macro_cache_type(macro_id, cache_type))) {
    LOG_WARN("fail to get ss macro cache type", KR(ret));
  } else if (OB_FAIL(get_macro_cache(cache_type, macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else {
    // if it is a write cache, first evict will not delete it but push it to be a read cache. only read cache can be evicted acturally. 
    const bool is_write_cache = meta_handle()->get_is_write_cache();
    if (OB_FAIL(macro_cache->evict(macro_id, is_write_cache))) {
      LOG_WARN("fail to delete ss macro cache meta", KR(ret));
    } else if (is_write_cache) {
      const int64_t TIMEOUT_US = 10000000; // 10s
      const int64_t SLEEP_TIMEOUT_US = 100000; // 100ms
      const int64_t start_time_us = ObTimeUtility::current_time_us();
      const int64_t end_time_us = start_time_us + TIMEOUT_US;
      bool is_complete = false;
      while (OB_SUCC(ret) && !is_complete) {
        ob_usleep(SLEEP_TIMEOUT_US); // sleep 100ms, wait write cache to be read cache
        is_exist = false;
        meta_handle.reset();
        if (OB_UNLIKELY(is_stopped_)) {
          ret = OB_SERVICE_STOPPED;
          LOG_WARN("macro cache mgr is stopped", KR(ret));
        } else if (OB_FAIL(get_macro_cache_meta(macro_id, is_exist, meta_handle))) {
          LOG_WARN("fail to get ss macro cache meta", KR(ret));
        } else if (!is_exist) {
          LOG_INFO("ss macro cache does not exist", K(macro_id));
          is_complete = true; // cache which not exist is no need to be evicted
        } else if (!meta_handle()->get_is_write_cache()) {
          if (OB_FAIL(macro_cache->evict(macro_id, meta_handle()->get_is_write_cache()))) {
            LOG_WARN("fail to delete ss macro cache", KR(ret));
          }
          is_complete = true; // read cache is just evicted once whether succeed or fail
        } else if (ObTimeUtility::current_time_us() > end_time_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("fail to wait ss macro cache to be read cache", KR(ret), K(macro_id), K(TIMEOUT_US));
        }
      }
      const int64_t cost_us = ObTimeUtility::current_time_us() - start_time_us;
      LOG_INFO("end to evict by macro id", KR(ret), K(macro_id), K(cost_us));
    }
  }
  return ret;
}

/*
This function is used to evict a macro block from the macro cache and macro block KVCACHE.
The following scenarios are involved.
1. ob_admin del_ss_tablet_macro_cache/del_ss_macro_cache
2. clear ss_macro_cache/clear ss_hot_macro_cache
*/
int ObSSMacroCacheMgr::evict_by_macro_id(const MacroBlockId &macro_id, bool &is_write_cache)
{
  ObSSMacroCacheMetaHandle meta_handle;
  bool is_exist = false;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  ObSSMacroCache *macro_cache = nullptr;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  is_write_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_macro_cache_meta(macro_id, is_exist, meta_handle))) {
    LOG_WARN("fail to get ss macro cache meta", KR(ret));
  } else if (!is_exist) {
    LOG_INFO("ss macro cache does not exist", K(macro_id));
  } else if (OB_FAIL(get_macro_cache_type(macro_id, cache_type))) {
    LOG_WARN("fail to get ss macro cache type", KR(ret));
  } else if (OB_FAIL(get_macro_cache(cache_type, macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret));
  } else if (FALSE_IT(is_write_cache = meta_handle()->get_is_write_cache())) {
  } else if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
  } else if (OB_FAIL(tenant_kvcache_mgr->erase_macro_block(macro_id))) {
    if (OB_SERVICE_STOPPED == ret) {
      LOG_TRACE("no need to erase macro block from KVCACHE",
          KR(ret), K(macro_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase macro block from KVCACHE",
          KR(ret), K(macro_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_exist) {
    // no exist in macro cache, no need to evict
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else if (OB_FAIL(macro_cache->evict(macro_id, is_write_cache))) {
    LOG_WARN("fail to evict ss macro cache", KR(ret));
  }
  return ret;
}


int ObSSMacroCacheMgr::batch_evict_by_macro_id_(const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  const int64_t macro_id_cnt = macro_ids.count();
  bool is_write_cache = false;
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_cnt); ++i) {
    const MacroBlockId &cur_macro_id = macro_ids.at(i);
    if (OB_FAIL(evict_by_macro_id(cur_macro_id, is_write_cache))) {
      LOG_WARN("fail to evict macro cache by id", KR(ret), K(cur_macro_id));
    } else {
      LOG_TRACE("succeed to evict macro cache by id", KR(ret), K(cur_macro_id), K(is_write_cache));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::evict_macro_id_array(
    const ObIArray<MacroBlockId> &read_cache_macro_ids,
    const ObIArray<MacroBlockId> &write_cache_macro_ids,
    int64_t &macro_read_cache_cnt,
    int64_t &macro_write_cache_cnt)
{
  int ret = OB_SUCCESS;
  if (read_cache_macro_ids.empty()) {
  } else if (OB_FAIL(batch_evict_by_macro_id_(read_cache_macro_ids))) {
    LOG_WARN("fail to batch evict macro read cache", KR(ret), K(read_cache_macro_ids.count()));
  } else {
    macro_read_cache_cnt += read_cache_macro_ids.count();
  }
  if (OB_FAIL(ret)) {
  } else if (write_cache_macro_ids.empty()) {
  } else if (OB_FAIL(batch_evict_by_macro_id_(write_cache_macro_ids))) {
    LOG_WARN("fail to batch evict macro write cache", KR(ret), K(write_cache_macro_ids.count()));
  } else {
    macro_write_cache_cnt += write_cache_macro_ids.count();
  }
  return ret;
}

int ObSSMacroCacheMgr::handle_evict_tablet_node_(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheMetaHandle &meta_handle,
    const ObTabletID *tablet_id,
    const common::hash::ObHashSet<uint64_t> *tablet_id_set,
    common::hash::ObHashSet<uint64_t> *evicted_tablet_ids,
    ObIArray<MacroBlockId> &read_cache_macro_ids,
    ObIArray<MacroBlockId> &write_cache_macro_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
  } else {
    const uint64_t effective_tablet_id = meta_handle()->get_effective_tablet_id();
    bool need_evict = false;
    if (OB_NOT_NULL(tablet_id)) {
      need_evict = (tablet_id->id() == effective_tablet_id);
    } else if (OB_NOT_NULL(tablet_id_set) && OB_FAIL(tablet_id_set->exist_refactored(effective_tablet_id))) {
      if (OB_HASH_EXIST == ret) {
        need_evict = true;
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check exist in need evict tablet ids", KR(ret), K(effective_tablet_id));
      }
    }
    if (OB_SUCC(ret) && need_evict) {
      if (meta_handle()->get_is_write_cache()) {
        if (OB_FAIL(write_cache_macro_ids.push_back(macro_id))) {
          LOG_WARN("fail to push back write_cache_macro_ids", KR(ret), K(macro_id));
        } else if (OB_NOT_NULL(evicted_tablet_ids)) {
          if (OB_FAIL(evicted_tablet_ids->erase_refactored(effective_tablet_id))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to erase evicted tablet id", KR(ret), K(effective_tablet_id));
            }
          }
        }
      } else if (OB_FAIL(read_cache_macro_ids.push_back(macro_id))) {
        LOG_WARN("fail to push back read_cache_macro_ids", KR(ret), K(macro_id));
      }
    }
  }
  return ret;
}


int ObSSMacroCacheMgr::handle_evict_bucket_batch_(
    const int64_t bucket_batch_cnt,
    int64_t &bucket_cnt,
    ObIArray<MacroBlockId> &read_cache_macro_ids,
    ObIArray<MacroBlockId> &write_cache_macro_ids,
    int64_t &macro_read_cache_cnt,
    int64_t &macro_write_cache_cnt)
{
  int ret = OB_SUCCESS;
  ++bucket_cnt;
  if (bucket_batch_cnt == bucket_cnt) {
    if (OB_FAIL(evict_macro_id_array(read_cache_macro_ids, write_cache_macro_ids,
                                     macro_read_cache_cnt, macro_write_cache_cnt))) {
      LOG_WARN("fail to evict macro id array", KR(ret),
               K(read_cache_macro_ids.count()), K(write_cache_macro_ids.count()));
    }
    read_cache_macro_ids.reuse();
    write_cache_macro_ids.reuse();
    bucket_cnt = 0;
  }
  return ret;
}

int ObSSMacroCacheMgr::evict_tablets(
    const ObTabletID *tablet_id,
    const common::hash::ObHashSet<uint64_t> *tablet_id_set,
    common::hash::ObHashSet<uint64_t> *evicted_tablet_ids,
    int64_t &macro_read_cache_cnt,
    int64_t &macro_write_cache_cnt)
{
  int ret = OB_SUCCESS;
  int64_t bucket_cnt = 0;
  const int64_t BUCKET_BATCH_CNT = 10000;
  ObSEArray<MacroBlockId, 64> read_cache_macro_ids;
  ObSEArray<MacroBlockId, 64> write_cache_macro_ids;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  macro_read_cache_cnt = 0;
  macro_write_cache_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_ISNULL(tablet_id) && OB_ISNULL(tablet_id_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id and tablet id set are both null", KR(ret));
  } else if (OB_NOT_NULL(tablet_id) && OB_UNLIKELY(!tablet_id->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", KR(ret), KPC(tablet_id));
  } else if (OB_NOT_NULL(tablet_id_set) && tablet_id_set->empty()) {
    // nothing to do
  } else {
    SS_FOREACH_MAP_NODE_BASE(meta_map_, SSMacroCacheMetaMap, node_iter, !is_stopped_, {
      if (OB_FAIL(handle_evict_tablet_node_(node_iter->first,
                                            node_iter->second,
                                            tablet_id,
                                            tablet_id_set,
                                            evicted_tablet_ids,
                                            read_cache_macro_ids,
                                            write_cache_macro_ids))) {
        LOG_WARN("fail to handle evict tablet node", KR(ret), K(node_iter->first));
      }
    }, {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(handle_evict_bucket_batch_(BUCKET_BATCH_CNT,
                                                    bucket_cnt,
                                                    read_cache_macro_ids,
                                                    write_cache_macro_ids,
                                                    macro_read_cache_cnt,
                                                    macro_write_cache_cnt))) {
        LOG_WARN("fail to handle evict bucket batch", KR(ret), K(bucket_cnt));
      }
    });
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(evict_macro_id_array(read_cache_macro_ids, write_cache_macro_ids, macro_read_cache_cnt, macro_write_cache_cnt))) {
      LOG_WARN("fail to evict macro id array", KR(ret), K(read_cache_macro_ids.count()), K(write_cache_macro_ids.count()));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::evict_by_tablet_id(
  const ObTabletID &tablet_id,
  int64_t &macro_read_cache_cnt,
  int64_t &macro_write_cache_cnt)
{
  int ret = evict_tablets(&tablet_id, nullptr /*tablet_id_set*/, nullptr /*evicted_tablet_ids*/,
                          macro_read_cache_cnt, macro_write_cache_cnt);
  if (OB_SUCC(ret)) {
    if (macro_write_cache_cnt > 0) {
      LOG_TRACE("remain macro write cache to be deleted", KR(ret), K(tablet_id), K(macro_write_cache_cnt), K(macro_read_cache_cnt));
    } else {
      LOG_TRACE("all macro write cache have been successfully evicted", KR(ret), K(tablet_id), K(macro_write_cache_cnt), K(macro_read_cache_cnt));
    }
  }
  return ret;
}


int ObSSMacroCacheMgr::push_old_sstable_evict_task(
    const ObTabletID &tablet_id,
    const ObSSHotTabletOldSSTableEvictInfo &evict_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(hot_tablet_old_sstable_evict_task_.push_old_sstable_evict_task(tablet_id, evict_info))) {
    LOG_WARN("fail to push old sstable evict task", KR(ret), K(tablet_id), K(evict_info));
  }
  return ret;
}

int ObSSMacroCacheMgr::add_into_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(get_macro_cache(macro_cache_meta.get_cache_type(), macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K(macro_cache_meta));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else if (OB_FAIL(macro_cache->add_into_lru_list(macro_cache_meta))) {
    LOG_WARN("fail to add into lru list", KR(ret), K(macro_cache_meta));
  }
  return ret;
}

int ObSSMacroCacheMgr::remove_from_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(get_macro_cache(macro_cache_meta.get_cache_type(), macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K(macro_cache_meta));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else if (OB_FAIL(macro_cache->remove_from_lru_list(macro_cache_meta))) {
    LOG_WARN("fail to remove from lru list", KR(ret), K(macro_cache_meta));
  }
  return ret;
}

int ObSSMacroCacheMgr::update_lru_list(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(get_macro_cache(macro_cache_meta.get_cache_type(), macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K(macro_cache_meta));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else if (OB_FAIL(macro_cache->update_lru_list(macro_cache_meta))) {
    LOG_WARN("fail to update lru list", KR(ret), K(macro_cache_meta));
  }
  return ret;
}

int ObSSMacroCacheMgr::shift_lru_list(
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type,
    ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *ori_macro_cache = nullptr;
  ObSSMacroCache *new_macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_ss_macro_cache_type(ori_cache_type) ||
                         !is_valid_ss_macro_cache_type(new_cache_type) ||
                         (ori_cache_type != macro_cache_meta.get_cache_type()) ||
                         (ori_cache_type == new_cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K_SS_MACRO_CACHE_TYPE(ori_cache_type),
             K_SS_MACRO_CACHE_TYPE(new_cache_type), K(macro_cache_meta));
  } else if (OB_FAIL(get_macro_cache(ori_cache_type, ori_macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K_SS_MACRO_CACHE_TYPE(ori_cache_type));
  } else if (OB_FAIL(get_macro_cache(new_cache_type, new_macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K_SS_MACRO_CACHE_TYPE(new_cache_type));
  } else if (OB_ISNULL(ori_macro_cache) || OB_ISNULL(new_macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret), KP(ori_macro_cache), KP(new_macro_cache));
  } else if (OB_FAIL(ori_macro_cache->remove_from_lru_list(macro_cache_meta))) {
    LOG_WARN("fail to remove from lru list", KR(ret), K(macro_cache_meta));
  } else {
    // retry until success. otherwise, macro_cache_meta only exists in Map, but not exists in LRU-List.
    // this will lead to:
    // 1. ObSSMacroCacheMgr::put/erase/update/get interfaces cannot execute successfully.
    // 2. this macro cache cannot be evicted.
    bool is_succ = false;
    int64_t retry_cnt = 0;
    while (!is_succ) {
      if (OB_FAIL(new_macro_cache->add_into_lru_list(macro_cache_meta))) {
        if (TC_REACH_TIME_INTERVAL(60 * 1000L * 1000L)) { // 1min
          LOG_ERROR("fail to add into lru list", KR(ret), K(macro_cache_meta), K(retry_cnt));
        } else {
          LOG_WARN("fail to add into lru list", KR(ret), K(macro_cache_meta), K(retry_cnt));
        }
        if (++retry_cnt > 100) {
          ob_usleep(100 * 1000L); // sleep 100ms
        }
      } else {
        is_succ = true;
      }
    }
    if (OB_SUCC(ret)) {
      macro_cache_meta.set_is_in_fifo_list(true);
      macro_cache_meta.set_cache_type(new_cache_type);
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::modify_macro_cache_type_(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type,
    const ObSSMacroCacheModifyCacheTypeParam &modify_param)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCacheModifyCacheTypeCallback modify_cache_type_callback(
      ori_cache_type, new_cache_type, modify_param, *this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() ||
                         !is_valid_ss_macro_cache_type(ori_cache_type) ||
                         !is_valid_ss_macro_cache_type(new_cache_type) ||
                         (ori_cache_type == new_cache_type) ||
                         !modify_param.is_valid() ||
                         (modify_param.update_effective_tablet_id_
                             && ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK != new_cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K_SS_MACRO_CACHE_TYPE(ori_cache_type),
             K_SS_MACRO_CACHE_TYPE(new_cache_type), K(modify_param));
  } else if (OB_FAIL(meta_map_.atomic_refactored(macro_id, modify_cache_type_callback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to atomic refactored", KR(ret), K(macro_id), K(modify_cache_type_callback));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = modify_cache_type_callback.get_ret()))) {
    LOG_WARN("fail to execute modify cache type callback", KR(ret), K(macro_id), K(modify_cache_type_callback));
  }
  return ret;
}

int ObSSMacroCacheMgr::modify_macro_cache_type_(
    const ObIArray<MacroBlockId> &macro_ids,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type,
    const ObSSMacroCacheModifyCacheTypeParam &modify_param)
{
  int ret = OB_SUCCESS;
  const int64_t macro_ids_cnt = macro_ids.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_ids_cnt); i++) {
    if (OB_FAIL(modify_macro_cache_type_(
        macro_ids.at(i), ori_cache_type, new_cache_type, modify_param))) {
      LOG_WARN("fail to modify macro cache type", KR(ret), "macro_id", macro_ids.at(i),
               K_SS_MACRO_CACHE_TYPE(ori_cache_type), K_SS_MACRO_CACHE_TYPE(new_cache_type));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::modify_macro_cache_type(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type)
{
  const ObSSMacroCacheModifyCacheTypeParam modify_param(
      false/*touch_cache*/, false/*update_effective_tablet_id*/, ObTabletID::INVALID_TABLET_ID);
  return modify_macro_cache_type_(macro_id, ori_cache_type, new_cache_type, modify_param);
}

int ObSSMacroCacheMgr::modify_macro_cache_type(
    const ObIArray<MacroBlockId> &macro_ids,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type)
{
  const ObSSMacroCacheModifyCacheTypeParam modify_param(
      false/*touch_cache*/, false/*update_effective_tablet_id*/, ObTabletID::INVALID_TABLET_ID);
  return modify_macro_cache_type_(macro_ids, ori_cache_type, new_cache_type, modify_param);
}

int ObSSMacroCacheMgr::modify_macro_cache_type_and_touch(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type)
{
  const ObSSMacroCacheModifyCacheTypeParam modify_param(
      true/*touch_cache*/, false/*update_effective_tablet_id*/, ObTabletID::INVALID_TABLET_ID);
  return modify_macro_cache_type_(macro_id, ori_cache_type, new_cache_type, modify_param);
}

int ObSSMacroCacheMgr::modify_macro_cache_type_and_touch(
    const ObIArray<MacroBlockId> &macro_ids,
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type)
{
  const ObSSMacroCacheModifyCacheTypeParam modify_param(
      true/*touch_cache*/, false/*update_effective_tablet_id*/, ObTabletID::INVALID_TABLET_ID);
  return modify_macro_cache_type_(macro_ids, ori_cache_type, new_cache_type, modify_param);
}

int ObSSMacroCacheMgr::promote_macro_cache_to_hot(
    const MacroBlockId &macro_id,
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!effective_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid effective tablet id for promoting macro cache",
        KR(ret), K(macro_id), K(effective_tablet_id));
  } else {
    const ObSSMacroCacheModifyCacheTypeParam modify_param(
        false/*touch_cache*/, true/*update_effective_tablet_id*/, effective_tablet_id.id());
    if (OB_FAIL(modify_macro_cache_type_(
        macro_id,
        ObSSMacroCacheType::MACRO_BLOCK,
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
        modify_param))) {
      LOG_WARN("fail to promote macro cache to hot",
          KR(ret), K(macro_id), K(effective_tablet_id));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::alloc_file_size(
    const ObSSMacroCacheType cache_type,
    const int64_t alloc_size,
    const ObDiskSpaceType disk_space_type)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY((ObSSMacroCacheType::MAX_TYPE == cache_type) || (alloc_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(alloc_size));
  } else {
    // ERRSIM for test
    ret = OB_E(EventTable::EN_SHARED_STORAGE_DISK_OUTOF_SPACE_ERR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_INFO("ERRSIM EN_SHARED_STORAGE_DISK_OUTOF_SPACE_ERR", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(alloc_size));
  } else if (OB_FAIL(disk_space_mgr->alloc_file_size(alloc_size, cache_type, disk_space_type))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
      set_fg_evict_trigger();
    }
    if (OB_FAIL(ret) && TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("fail to alloc file size", KR(ret), K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::alloc_file_size(
    const ObStorageObjectType object_type,
    const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id,
    const int64_t alloc_size,
    const ObDiskSpaceType disk_space_type)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (OB_FAIL(get_macro_cache_type(object_type,
                                   effective_tablet_id,
                                   use_effective_tablet_id,
                                   cache_type))) {
    LOG_WARN("fail to get macro cache type", KR(ret), "storage object type", STI(object_type),
            K(effective_tablet_id), K(use_effective_tablet_id));
  } else if (OB_FAIL(alloc_file_size(cache_type, alloc_size, disk_space_type))) {
    LOG_WARN("fail to alloc file size", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(alloc_size),
             K(disk_space_type));
  }
  return ret;
}

int ObSSMacroCacheMgr::get_max_free_size_of_cache_type(
    const ObSSMacroCacheType &macro_cache_type,
    int64_t &max_free_size) const
{
  int ret = OB_SUCCESS;
  max_free_size = 0;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret));
  } else if (OB_FAIL(disk_space_mgr->get_theoretical_free_disk_size(macro_cache_type, max_free_size))) {
    LOG_WARN("fail to get_theoretical_free_disk_size", KR(ret), K(macro_cache_type));
  }
  return ret;
}

int ObSSMacroCacheMgr::alloc_file_size_for_hot_tablet_with_retry(
    const blocksstable::ObStorageObjectType object_type,
    const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id,
    const int64_t alloc_size,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  const int64_t end_time_us = start_time_us + timeout_us;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id),
        K(use_effective_tablet_id), K(alloc_size), K(timeout_us), K(end_time_us));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id), K(use_effective_tablet_id),
        K(alloc_size), K(timeout_us), K(end_time_us));
  } else if (OB_FAIL(get_macro_cache_type(object_type, effective_tablet_id, use_effective_tablet_id, cache_type))) {
    LOG_WARN("fail to get macro cache type", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id), K(use_effective_tablet_id),
        K(alloc_size), K(timeout_us), K(end_time_us));
  }  else if (OB_UNLIKELY(timeout_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id), K(use_effective_tablet_id),
        K(alloc_size), K(timeout_us), K(end_time_us), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    int64_t cur_hot_tablet_max_free_size = 0;
    do {
      ret = OB_SUCCESS;
      cur_hot_tablet_max_free_size = 0;
      if (OB_UNLIKELY(is_stopped_)) {
        ret = OB_SERVICE_STOPPED;
        LOG_WARN("macro cache mgr is stopped", KR(ret), K(alloc_size), K(end_time_us));
      } else if (OB_FAIL(get_max_free_size_of_cache_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, cur_hot_tablet_max_free_size))) {
        LOG_WARN("fail to get_max_free_size_of_cache_type", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id),
            K(use_effective_tablet_id), K(alloc_size), K(timeout_us), K(end_time_us));
      } else if (cur_hot_tablet_max_free_size < alloc_size) {
        ret = OB_SERVER_OUTOF_DISK_SPACE;
        LOG_WARN("hot tablet max free size is not enough", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id),
            K(use_effective_tablet_id), K(alloc_size), K(timeout_us), K(end_time_us),
            K(cur_hot_tablet_max_free_size));
      } else if (OB_FAIL(alloc_file_size(cache_type, alloc_size, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to alloc file size for hot tablet", KR(ret), "storage object type", STI(object_type), K(effective_tablet_id),
            K(use_effective_tablet_id), K(alloc_size), K(timeout_us), K(end_time_us),
            K(cur_hot_tablet_max_free_size));
        
        // if `alloc_file_size` failed bacause of OB_SERVER_OUTOF_DISK_SPACE,
        // it will trigger foreground eviction.
        // waiting for eviction here
        if (OB_SERVER_OUTOF_DISK_SPACE == ret && timeout_us > 0) {
          ob_usleep(evict_task_.get_schedule_interval_us());
        }
      }
    } while (OB_SERVER_OUTOF_DISK_SPACE == ret 
        && ObTimeUtility::fast_current_time() < end_time_us
        && cur_hot_tablet_max_free_size >= alloc_size);
  }
  return ret;
}

int ObSSMacroCacheMgr::free_file_size(
    const ObStorageObjectType object_type,
    const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id,
    const int64_t free_size,
    const ObDiskSpaceType disk_space_type)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_FAIL(get_macro_cache_type(object_type,
                                   effective_tablet_id,
                                   use_effective_tablet_id,
                                   cache_type))) {
    LOG_WARN("fail to get macro cache type", KR(ret), "storage object type", STI(object_type),
             K(effective_tablet_id), K(use_effective_tablet_id));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else if (OB_FAIL(disk_space_mgr->free_file_size(free_size, cache_type, disk_space_type))) {
    LOG_WARN("fail to free file size", KR(ret), K(free_size), K_SS_MACRO_CACHE_TYPE(cache_type),
             K(disk_space_type));
  }
  return ret;
}

int ObSSMacroCacheMgr::is_macro_cache_space_full(bool &is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else {
    const int64_t macro_cache_size = disk_space_mgr->get_unit_config_shared_macro_cache_size();
    const int64_t macro_cache_used_size = disk_space_mgr->get_shared_macro_cache_used_size();
    if (OB_UNLIKELY((macro_cache_size <= 0) || (macro_cache_used_size < 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro cache size or macro cache used size", KR(ret), K(macro_cache_size),
               K(macro_cache_used_size));
    } else {
      const int64_t macro_cache_used_percent = ((1.0 * macro_cache_used_size / macro_cache_size) * 100);
      is_full = ((macro_cache_used_percent >= CACHE_EVICT_THRESHOLD) ? true : false);
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::modify_from_write_cache_to_read_cache(const MacroBlockId &macro_id)
{
  // NOTICE: to make cache_info is valid, we need to put some fake value here.
  ObSSMacroCacheInfo cache_info(1/*effective_tablet_id*/, 1/*size*/,
                                ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ObSSMacroCacheUpdateParam update_param(false/*is_update_lru*/,
                                         false/*is_update_last_access_time*/,
                                         false/*is_update_effective_tablet_id*/,
                                         false/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         true/*is_update_write_cache*/);
  return update(macro_id, cache_info, update_param);
}

int ObSSMacroCacheMgr::push_to_flush_map(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheFlushStatus flush_status(GET_FUNC_TYPE(), false/*is_flushing*/);
  if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(flush_map_.set_refactored(file_id, flush_status))) {
    LOG_WARN("fail to set refactored flush map", KR(ret), K(file_id), K(flush_status));
  }
  return ret;
}

int ObSSMacroCacheMgr::seal_and_push_to_flush_map(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (!SSObjUtil::can_append_write(file_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object type can not append write", KR(ret), "storage object type", STI(file_id.storage_object_type()));
  } else if (SSObjUtil::is_tmp_file(file_id)) {
    ObTenantFileManager *file_mgr = nullptr;
    if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_mgr->get_segment_file_mgr().set_sealed(TmpFileSegId(
                                                                   file_id.second_id(),
                                                                   file_id.fourth_id())))) {
      LOG_WARN("fail to set sealed", KR(ret), K(file_id));
    } else if (OB_FAIL(push_to_flush_map(file_id, ls_epoch_id))) {
      LOG_WARN("fail to push to flush map", KR(ret), K(file_id), K(ls_epoch_id));
    }
  } else {
    if (OB_FAIL(push_to_flush_map(file_id, ls_epoch_id))) {
      LOG_WARN("fail to push to flush map", KR(ret), K(file_id), K(ls_epoch_id), "storage object type", STI(file_id.storage_object_type()));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::flush_write_cache(const MacroBlockId &macro_id, ObPromise<int> &promise)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMetaHandle meta_handle;
  bool is_exist = false;
  const ObSSMacroCacheFlushStatus flush_status(GET_FUNC_TYPE(), false/*is_flushing*/);
  bool need_flush = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !promise.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), "is_promise_valid", promise.is_valid());
  } else if (OB_FAIL(get_macro_cache_meta(macro_id, is_exist, meta_handle))) {
    LOG_WARN("fail to get macro cache meta", KR(ret), K(macro_id));
  } else if (!is_exist) { // 1. meta not exist
    // scenario 1: may be already flushed to object storage by eviction. no need to flush in this case.
    // scenario 2: observer restart, macro cache ckpt replay and calibration are not finished. need to flush in this case.
    // as we cannot distinguish these two scenarios, flush uniformly here to avoid data loss.
    need_flush = true;
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta handle", KR(ret), K(macro_id), K(meta_handle));
  } else if (meta_handle()->get_is_write_cache()) { // 2. write cache meta
    need_flush = true;
  } else { // 3. read cache meta
    need_flush = false;
    if (OB_FAIL(promise.set(OB_SUCCESS))) {
      LOG_ERROR("fail to set promise", KR(ret), K(macro_id));
    }
  }
  if (OB_SUCC(ret) && need_flush) {
    if (OB_FAIL(write_cache_flush_map_.set_refactored(macro_id, flush_status, promise))) {
      LOG_WARN("fail to set refactored write cache flush map", KR(ret), K(macro_id), K(flush_status));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::do_expire_work()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  LOG_INFO("start to do expire work");
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    const int64_t expiration_time_us = get_expiration_time_us();
    const int64_t min_retention_time_us = ObTimeUtility::current_time_us() - expiration_time_us;
    if (OB_FAIL(expire_macro_cache_(min_retention_time_us))) {
      LOG_WARN("fail to expire macro cache", KR(ret), K(min_retention_time_us));
    }
    LOG_INFO("finish to expire macro cache", KR(ret), K(expiration_time_us), K(min_retention_time_us));
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to do expire work", KR(ret), K(cost_us));
  return ret;
}

int64_t ObSSMacroCacheMgr::get_expiration_time_us()
{
  // Note: default value of _local_cache_expiration_time is 0
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  int64_t expiration_time_us = (tenant_config.is_valid()
                                ? (tenant_config->_local_cache_expiration_time)
                                : 0L);
  if (0L == expiration_time_us) {
    expiration_time_us = DEFAULT_CACHE_EXPIRATION_TIME_US;
  }
  return expiration_time_us;
}

int ObSSMacroCacheMgr::do_write_cache_ctrl_work()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  LOG_INFO("start to do write cache ctrl work");
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  bool is_exceed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (flush_map_.size() > WRITE_CAHCE_BATCH_FLUSH_CNT) {
    LOG_INFO("there are already many entries waiting to be flushed in the flush_map",
             "flush_map_size", flush_map_.size());
  } else if (OB_FAIL(check_write_cache_threshold(is_exceed))) {
    LOG_WARN("fail to check write cache threshold", KR(ret));
  } else if (!is_exceed) {
    // do nothing. no need to flush write cache
  } else if (OB_FAIL(find_and_flush_write_cache())) {
    LOG_WARN("fail to find and flush write cache", KR(ret));
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to do write cache ctrl work", KR(ret), K(cost_us), K(is_exceed),
           "flush_map_size", flush_map_.size());
  return ret;
}

// 1. batch pop from evict_queue
// 2. push to flush queue for write cache, and directly delete_local_file for read cache
// 3. check whether there exists completed write cache eviction
int ObSSMacroCacheMgr::do_evict_work()
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  bool need_evict = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(check_if_need_evict(need_evict))) {
    LOG_WARN("fail to check if need evict", KR(ret));
  } else if (!need_evict) {
    // do nothing.
  } else if (OB_FAIL(batch_evict())) {
    LOG_WARN("fail to batch evict", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheMgr::get_evict_entries(
    const ObSSMacroCacheType evict_macro_cache_type,
    ObIArray<ObSSMacroCacheEvictEntry> &evict_entries)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == evict_macro_cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid evict macro cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(evict_macro_cache_type));
  } else if (OB_FAIL(get_macro_cache(evict_macro_cache_type, macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K_SS_MACRO_CACHE_TYPE(evict_macro_cache_type));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret));
  } else if (OB_FAIL(macro_cache->get_evict_entries(evict_entries))) {
    LOG_WARN("fail to get evict entries", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheMgr::process_hot_macro_cache_candidates_(
    const ObIArray<MacroBlockId> &macro_ids,
    const bool convert_to_auto)
{
  int ret = OB_SUCCESS;
  if (macro_ids.empty()) {
  } else if (convert_to_auto) {
    if (OB_FAIL(modify_macro_cache_type(
        macro_ids,
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
        ObSSMacroCacheType::MACRO_BLOCK))) {
      LOG_WARN("fail to convert unreferenced hot macro cache to auto",
          KR(ret), "macro_count", macro_ids.count());
    }
  } else if (OB_FAIL(batch_evict_by_macro_id_(macro_ids))) {
    LOG_WARN("fail to batch evict unreferenced hot macro cache",
        KR(ret), "macro_count", macro_ids.count());
  }

  return ret;
}

int ObSSMacroCacheMgr::gc_hot_tablet_macro_cache(
    const ObHashSet<MacroBlockId> &macro_block_set,
    const int64_t idle_time_us,
    const bool convert_to_auto)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 10000;
  int64_t processed_macro_count = 0;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "GCHotTabMC"));
  const int64_t start_us = ObTimeUtility::current_time_us();
  const int64_t current_time_us = start_us;
  LOG_INFO("start to gc hot tablet macro cache");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(idle_time_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hot tablet macro cache gc idle time", KR(ret), K(idle_time_us));
  } else {
    SSMacroCacheMetaMap::bucket_iterator bucket_iter = meta_map_.bucket_begin();
    while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != meta_map_.bucket_end())) {
      {
        SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
        SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
          const MacroBlockId &macro_id = node_iter->first;
          const ObSSMacroCacheMetaHandle &meta_handle = node_iter->second;
          if (OB_UNLIKELY(!meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid meta handle", KR(ret), K(macro_id));
          } else if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK != meta_handle()->get_cache_type()) {
            // 先过滤非 HOT cache，避免额外的时间计算和引用集合查询。
          } else if (current_time_us - meta_handle()->get_last_access_time_us() <= idle_time_us) {
            // 未超过 idle time 的 HOT cache 直接保留，不查询引用集合。
          } else if (OB_FAIL(macro_block_set.exist_refactored(macro_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              if (OB_FAIL(macro_ids.push_back(macro_id))) {
                LOG_WARN("fail to collect unreferenced hot macro cache", KR(ret), K(macro_id));
              }
            } else {
              LOG_WARN("fail to exist refactored", KR(ret), K(macro_id));
            }
          }
          node_iter++;
        }
        ++bucket_iter;
        ++bucket_cnt;
      }
      if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt)) {
        if (OB_FAIL(process_hot_macro_cache_candidates_(macro_ids, convert_to_auto))) {
          LOG_WARN("fail to process hot macro cache candidates", KR(ret),
              K(convert_to_auto), "macro_count", macro_ids.count());
        } else {
          processed_macro_count += macro_ids.count();
        }
        macro_ids.reuse();
        bucket_cnt = 0;
      }
    }
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(process_hot_macro_cache_candidates_(macro_ids, convert_to_auto))) {
        LOG_WARN("fail to process remaining hot macro cache candidates", KR(ret),
            K(convert_to_auto), "macro_count", macro_ids.count());
      } else {
        processed_macro_count += macro_ids.count();
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("finish to gc hot tablet macro cache", KR(ret), K(processed_macro_count),
      K(convert_to_auto), K(cost_us), K(idle_time_us));
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache_type(
    const ObStorageObjectType object_type,
    const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id,
    ObSSMacroCacheType &macro_cache_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSObjUtil::get_macro_cache_type(object_type, effective_tablet_id, use_effective_tablet_id, macro_cache_type))) {
    LOG_WARN("fail to get macro cache type", KR(ret), "storage object type", STI(object_type));
  }
  return ret;
}

uint64_t ObSSMacroCacheMgr::get_effective_tablet_id(
    const bool is_write_cache,
    const uint64_t io_ctx_tablet_id,
    const blocksstable::MacroBlockId &macro_id)
{
  uint64_t effective_tablet_id = ObTabletID::INVALID_TABLET_ID;
  if (is_write_cache) {
    // directly obtain effective_tablet_id from macro_id. because write cache macro must has not
    // been splitted.
    int ret = OB_SUCCESS;
    if (OB_FAIL(SSObjUtil::get_effective_tablet_id(macro_id, effective_tablet_id))) {
      LOG_WARN("fail to get effective tablet id", KR(ret), K(macro_id));
    }
  } else { // prewarm or preread
    // obtain effective_tablet_id from io context, instead of directly obtaining effective_tablet_id
    // from macro_id. because may prewarm/preread one macro that has already been splitted, whose
    // tablet_id in macro_id is born_tablet_id. thus, effective_tablet_id must be delivered through
    // prewarm/preread io context.
    effective_tablet_id = io_ctx_tablet_id;
  }
  return effective_tablet_id;
}

int ObSSMacroCacheMgr::divide_bucket_range(
    const int64_t split_count,
    ObIArray<ObSSMacroCacheBucketIdxRange> &bucket_ranges)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(split_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split count", KR(ret), K(split_count));
  } else {
    const int64_t bucket_cnt = meta_map_.bucket_count();
    const int64_t bucket_cnt_per_range = (bucket_cnt + split_count - 1) / split_count;
    int64_t start_bkt_idx = -1;
    int64_t end_bkt_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && (i < split_count) && (end_bkt_idx < (bucket_cnt - 1)); ++i) {
      start_bkt_idx = (i * bucket_cnt_per_range) - 1;
      end_bkt_idx = MIN(((i + 1) * bucket_cnt_per_range) - 1, bucket_cnt - 1);
      ObSSMacroCacheBucketIdxRange bucket_range(start_bkt_idx, end_bkt_idx);
      if (OB_UNLIKELY(!bucket_range.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid bucket range", KR(ret), K(bucket_range), K(bucket_cnt),
                 K(bucket_cnt_per_range), K(split_count), K(i));
      } else if (OB_FAIL(bucket_ranges.push_back(bucket_range))) {
        LOG_WARN("fail to push back bucket range", KR(ret), K(bucket_range), K(i));
      }
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::copy_next_meta_map_bucket(const int64_t bucket_pos, ObIArray<ObSSMacroPrewarmMeta> &prewarm_metas)
{
  int ret = OB_SUCCESS;
  SSMacroCacheMetaMap::bucket_iterator bucket_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(bucket_pos < 0 || bucket_pos >= meta_map_.bucket_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket pos", KR(ret), K(bucket_pos));
  } else if (OB_FAIL(meta_map_.get_bucket_iterator(bucket_pos, bucket_iter))) {
    LOG_WARN("fail to get bucket iterator", KR(ret), K(bucket_pos));
  } else {
    SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
    SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
    SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
    while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
      ObSSMacroPrewarmMeta prewarm_meta;
      ObSSMacroCacheMeta *cur_meta = node_iter->second.get_ptr();
      // only shared data block and meta block need to be prewarmed
      if (cur_meta->get_macro_id().is_shared_data_block_or_meta_block()) {
        prewarm_meta.macro_id_ = cur_meta->get_macro_id();
        prewarm_meta.effective_tablet_id_ = cur_meta->get_effective_tablet_id();
        prewarm_meta.size_ = cur_meta->get_size();
        prewarm_meta.is_in_fifo_list_ = cur_meta->get_is_in_fifo_list();
        prewarm_meta.cache_type_ = cur_meta->get_cache_type();
        if (OB_FAIL(prewarm_metas.push_back(prewarm_meta))) {
          LOG_WARN("fail to push back prewarm meta", KR(ret), K(node_iter->first));
        }
      }
      node_iter++;
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::get_fifo_size_from_ls_cache_info(int64_t &fifo_size)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    SSMacroCacheLSInfoMap::bucket_iterator bucket_iter = ls_info_map_.bucket_begin();
    while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != ls_info_map_.bucket_end())) {
      SSMacroCacheLSInfoMap::hashtable::bucket_lock_cond blk(*bucket_iter);
      SSMacroCacheLSInfoMap::hashtable::readlocker locker(blk.lock());
      SSMacroCacheLSInfoMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
      while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
        const ObSSMacroCacheLSInfo &ls_info = node_iter->second;
        fifo_size += ls_info.get_fifo_required_size();
        node_iter++;
      }
      ++bucket_iter;
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::create_macro_caches()
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret));
  } else {
    for (uint8_t i = 0; (i < SS_MACRO_CACHE_MAX_TYPE_VAL) && OB_SUCC(ret); ++i) {
      if (OB_FAIL(ObSSMacroCacheFactory::create(static_cast<ObSSMacroCacheType>(i), macro_caches_[i]))) {
        LOG_WARN("fail to create macro cache", KR(ret), K(i));
      } else if (OB_FAIL(disk_space_mgr->update_macro_cache_desc(static_cast<ObSSMacroCacheType>(i),
                                                                 OB_SS_MACRO_CACHE_DESCS[i]))) {
        LOG_WARN("fail to update macro cache desc", K(i));
      } else {
        LOG_INFO("succ to create macro cache", K(i), KP(macro_caches_[i]));
      }
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache(
    const ObSSMacroCacheType cache_type,
    ObSSMacroCache *&macro_cache)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    if (OB_ISNULL(macro_caches_[idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache is null", KR(ret), K(idx));
    } else {
      macro_cache = macro_caches_[idx];
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache_hit_stat_by_cache_type(
    const ObSSMacroCacheType cache_type, ObStorageCacheHitStat &hit_stat) const
{
  int ret = OB_SUCCESS;
  hit_stat.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_ss_macro_cache_type(cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    hit_stat = macro_caches_hit_stat_[idx];
  }
  return ret;
}

int ObSSMacroCacheMgr::get_macro_cache_hit_stat_by_block_type(
    const ObSSMacroBlockType block_type, ObStorageCacheHitStat &hit_stat) const
{
  int ret = OB_SUCCESS;
  hit_stat.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_ss_macro_block_type(block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block type", KR(ret), K(block_type));
  } else {
    const uint8_t idx = static_cast<uint8_t>(block_type);
    hit_stat = macro_blocks_stat_[idx].hit_stat_;
  }
  return ret;
}

int ObSSMacroCacheMgr::get_external_table_cache_stat(ObStorageCacheStat &cache_stat) const
{
  int ret = OB_SUCCESS;
  cache_stat.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    cache_stat = external_table_cache_stat_;
  }
  return ret;
}

int ObSSMacroCacheMgr::cal_expand_size_for_ls_migration(
    const ObSSMacroCacheLSInfo &src_ls_macro_cache_info,
    int64_t &min_expand_size, int64_t &best_expand_size)
{
  int ret = OB_SUCCESS;
  min_expand_size = 0;
  best_expand_size = 0;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObSSMacroCacheStat hot_cache_stat;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!src_ls_macro_cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls macro cache info is invalid", KR(ret), K(src_ls_macro_cache_info));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret), K(src_ls_macro_cache_info));
  } else if (OB_FAIL(disk_space_mgr->get_macro_cache_stat(
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_cache_stat))) {
    LOG_WARN("fail to get hot tablet macro block cache stat", KR(ret), K(src_ls_macro_cache_info));
  } else {
    const int64_t src_macro_cache_hot_size = src_ls_macro_cache_info.get_hot_size();
    const int64_t src_macro_cache_total_size = src_ls_macro_cache_info.get_valid_size();
    const int64_t dst_macro_cache_total_size =
        disk_space_mgr->get_tenant_macro_cache_used_size();
    const int64_t tenant_macro_cache_size =
        disk_space_mgr->get_allocated_shared_macro_cache_size();

    min_expand_size =
        MAX(0, hot_cache_stat.used_ + src_macro_cache_hot_size - tenant_macro_cache_size);
    min_expand_size = min_expand_size / CACHE_EVICT_THRESHOLD * 100;

    best_expand_size =
        MAX(0, dst_macro_cache_total_size + src_macro_cache_total_size - tenant_macro_cache_size);
    best_expand_size = best_expand_size / CACHE_EVICT_THRESHOLD * 100;

    LOG_INFO("macro cache cal expand size for ls migration",
        KR(ret), K(min_expand_size), K(best_expand_size), 
        K(dst_macro_cache_total_size), K(tenant_macro_cache_size),
        K(src_ls_macro_cache_info), K(hot_cache_stat), KPC(disk_space_mgr));
  }
  return ret;
}

int ObSSMacroCacheMgr::cal_cur_min_shrinkable_size(int64_t &cur_min_shrinkable_size)
{
  int ret = OB_SUCCESS;
  cur_min_shrinkable_size = INT64_MAX;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret));
  } else {
    // Estimate the shared macro-cache capacity needed at CACHE_EVICT_THRESHOLD usage.
    const int64_t used_size = disk_space_mgr->get_shared_macro_cache_used_size();
    cur_min_shrinkable_size = used_size / CACHE_EVICT_THRESHOLD * 100;
  }

  return ret;
}

int ObSSMacroCacheMgr::get_tablet_status_from_map(const uint64_t tablet_id, ObSSMacroCacheTabletStat &tablet_stat)
{
  int ret = OB_SUCCESS;
  tablet_stat.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret), K(tablet_id));
  } else if (common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id is invalid", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_stat_map_.get_refactored(tablet_id, tablet_stat))) {
     if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. may be already erased
    } else {
      LOG_WARN("fail to get tablet_stat from tablet_stat_map", KR(ret), K(tablet_id));
    }
  } 
  return ret;
}

int ObSSMacroCacheMgr::get_ls_cache_info(const ObLSID &ls_id, ObSSMacroCacheLSInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  ls_cache_info.reset();
  SSMacroCacheReqGuard guard(flying_req_cnt_); // in order to protect ls_info_map_
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_info_map_.get_refactored(ls_id, ls_cache_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("ls cache info is not exist", K(ls_id));
      ls_cache_info.set_ls_id(ls_id);
    } else {
      LOG_WARN("fail to get refactored ls info map", KR(ret), K(ls_id));
    }
  }
  LOG_INFO("finish to get ls cache info", KR(ret), K(ls_id), K(ls_cache_info));
  return ret;
}

int ObSSMacroCacheMgr::construct_meta_handle(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info,
    ObSSMacroCacheMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid() || !cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(cache_info));
  } else if (OB_ISNULL(meta = static_cast<ObSSMacroCacheMeta *>(macro_meta_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for macro cache meta", KR(ret), K(cache_info));
  } else {
    meta = new (meta) ObSSMacroCacheMeta(macro_id, cache_info);
    meta_handle.set_ptr(meta);
    update_macro_meta_alloc_cnt(1);
  }
  return ret;
}

int ObSSMacroCacheMgr::batch_erase(const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  const int64_t macro_id_cnt = macro_ids.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_cnt); ++i) {
    const MacroBlockId &cur_macro_id = macro_ids.at(i);
    if (OB_FAIL(erase(cur_macro_id))) {
      LOG_WARN("fail to erase", KR(ret), K(cur_macro_id));
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::check_write_cache_threshold(bool &is_exceed)
{
  int ret = OB_SUCCESS;
  is_exceed = false;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else {
    const int64_t write_cache_size = get_write_cache_size();
    const int64_t macro_cache_size =
        tnt_disk_space_mgr->get_unit_config_tnt_macro_cache_size_by_usage_ratio();
    if (OB_UNLIKELY(macro_cache_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache size should be larger than zero",
          KR(ret), K(macro_cache_size), KPC(tnt_disk_space_mgr));
    } else {
      const int64_t write_cache_used_percent = (write_cache_size * 100 / macro_cache_size);
      if (write_cache_used_percent >= WRITE_CACHE_THRESHOLD) {
        is_exceed = true;
        LOG_INFO("write cache exceeds threshold", K(write_cache_used_percent),
            K(write_cache_size), K(macro_cache_size), LITERAL_K(WRITE_CACHE_THRESHOLD));
      }
    }
  }
  return ret;
}

// Note: instead of storing write cache in one independent LRU List, we store write cache and
// read cache store in the same LRU List. find one batch of write cache from the tail of the LRU
// List, which will be evicted earlier than the entries near the head of the LRU List.
int ObSSMacroCacheMgr::find_and_flush_write_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  // Note:
  // 1. expect META_FILE and TMP_FILE would not occupy too much disk space.
  // 2. only find and flush MACRO_BLOCK/HOT_TABLET_MACRO_BLOCK's write cache,
  // and modify these blocks from write cache to read cache after flush.
  } else if (OB_FAIL(batch_flush_tail_write_cache(ObSSMacroCacheType::MACRO_BLOCK))) {
    LOG_WARN("fail to batch flush tail write cache", KR(ret));
  } else if (OB_FAIL(batch_flush_tail_write_cache(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK))) {
    LOG_WARN("fail to batch flush tail write cache", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheMgr::batch_flush_tail_write_cache(const ObSSMacroCacheType cache_type)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "FlushWCache"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    ObSSMacroCache *macro_cache = macro_caches_[idx];
    if (OB_ISNULL(macro_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache is null", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
    } else if (OB_FAIL(macro_cache->batch_get_tail_write_cache(WRITE_CAHCE_BATCH_FLUSH_CNT, macro_ids))) {
      LOG_WARN("fail to batch get tail write cache", KR(ret));
    } else if (macro_ids.empty()) {
      LOG_INFO("not find write cache", K_SS_MACRO_CACHE_TYPE(cache_type));
    } else {
      const int64_t macro_id_cnt = macro_ids.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_cnt); ++i) {
        if (OB_FAIL(push_to_flush_map(macro_ids.at(i), 0/*ls_epoch_id*/))) {
          LOG_WARN("fail to push to flush map", KR(ret), K(i), "macro_id", macro_ids.at(i));
        }
      }
    }
  }
  return ret;
}

// trigger eviction when:
// 1. total macro cache used size reaches CACHE_EVICT_THRESHOLD
// 2. one kind of macro cache used size exceeds its max size
int ObSSMacroCacheMgr::check_if_need_evict(bool &need_evict)
{
  int ret = OB_SUCCESS;
  need_evict = false;
  ObTenantDiskSpaceManager *disk_space_manager = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_ISNULL(disk_space_manager = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space manager is null", KR(ret));
  } else if (ATOMIC_LOAD(&fg_evict_trigger_)) { // 1. trigger by foreground
    need_evict = true;
    ATOMIC_STORE(&fg_evict_trigger_, false); // reset flag
  } else { // 2. trigger by background
    // Check whether shared cache usage reaches CACHE_EVICT_THRESHOLD.
    int64_t macro_cache_used_percent = 0;
    const int64_t macro_cache_size = disk_space_manager->get_unit_config_shared_macro_cache_size();
    const int64_t macro_cache_used_size = disk_space_manager->get_shared_macro_cache_used_size();
    if (OB_UNLIKELY((macro_cache_size <= 0) || (macro_cache_used_size < 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro cache size or macro cache used size", KR(ret), K(macro_cache_size),
               K(macro_cache_used_size), KPC(disk_space_manager));
    } else {
      macro_cache_used_percent = ((1.0 * macro_cache_used_size / macro_cache_size) * 100);
      need_evict = ((macro_cache_used_percent >= CACHE_EVICT_THRESHOLD) ? true : false);
    }
  }

  // Each tenant evicts only its own macro cache entries (tenant-local meta_map/LRU).
  // Meta-tenant caches are typically hot; to avoid evicting them too early under overall
  // macro-cache pressure, the meta tenant starts eviction only when its own cache usage
  // exceeds META_TENANT_CACHE_EVICT_THRESHOLD% of the total macro cache size.
  if (OB_SUCC(ret) && need_evict && is_meta_tenant(tenant_id_)) {
    const int64_t tenant_macro_cache_used_size =
        disk_space_manager->get_tenant_macro_cache_used_size();
    const int64_t macro_cache_size = disk_space_manager->get_unit_config_shared_macro_cache_size();
    need_evict =
        (tenant_macro_cache_used_size > macro_cache_size / 100 * META_TENANT_CACHE_EVICT_THRESHOLD);
  }

  return ret;
}

int ObSSMacroCacheMgr::batch_evict()
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheDefaultEvictPolicy default_evict_policy;
  ObSEArray<ObSSMacroCacheEvictEntry, 32> evict_entries;
  if (OB_FAIL(default_evict_policy.get_evict_candidates(evict_entries))) {
    LOG_WARN("fail to get evict candidates", KR(ret));
  } else {
    const int64_t evict_entry_cnt = evict_entries.count();
    int64_t i = 0;
    for (; (i < evict_entry_cnt) && OB_SUCC(ret) && !is_stopped_; ++i) {
      const ObSSMacroCacheEvictEntry &evict_entry = evict_entries.at(i);
      const uint8_t idx = static_cast<uint8_t>(evict_entry.cache_type_);
      ObSSMacroCache *cur_macro_cache = macro_caches_[idx];
      if (OB_FAIL(cur_macro_cache->evict(evict_entry.macro_id_, evict_entry.is_write_cache_))) {
        LOG_WARN("fail to evict", KR(ret), K(evict_entry));
      }
    }
    if (TC_REACH_TIME_INTERVAL(10 * 1000L * 1000L)) { // 10s
      LOG_INFO("do batch evict", KR(ret), K(i), K(evict_entry_cnt), K(evict_entries), KPC(this));
    }
  }
  // set fg_evict_trigger_ to true, if all evict entries are write cache
  if (OB_SUCC(ret)) {
    bool is_all_wr_cache = true;
    const int64_t evict_entry_cnt = evict_entries.count();
    for (int64_t i = 0; (i < evict_entry_cnt) && !is_stopped_; ++i) {
      const ObSSMacroCacheEvictEntry &evict_entry = evict_entries.at(i);
      if (!evict_entry.is_write_cache_) {
        is_all_wr_cache = false;
        break;
      }
    }
    if (is_all_wr_cache && (evict_entry_cnt > 0)) {
      ATOMIC_STORE(&fg_evict_trigger_, true);
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::expire_macro_cache_(const int64_t min_retention_time_us)
{
  int ret = OB_SUCCESS;
  int64_t bucket_cnt = 0;
  const int64_t BUCKET_BATCH_CNT = 1000;
  ObSEArray<MacroBlockId, 32> expired_entries;
  expired_entries.set_attr(ObMemAttr(MTL_ID(), "ExpiredEntry"));
  SS_FOREACH_MAP_NODE_BASE(meta_map_, SSMacroCacheMetaMap, node_iter, !is_stopped_, {
    if (OB_FAIL(filter_expired_macro_cache_(node_iter->second, min_retention_time_us, expired_entries))) {
      LOG_WARN("fail to filter expired macro cache", KR(ret), "macro_cache_meta", node_iter->second,
               K(min_retention_time_us));
    }
  }, {
    if (FAILEDx(batch_evict_expired_macro_cache_(BUCKET_BATCH_CNT, bucket_cnt, expired_entries))) {
      LOG_WARN("fail to batch evict expired macro cache", KR(ret), K(bucket_cnt));
    }
  });
  if (OB_FAIL(ret)) {
  } else if (expired_entries.empty()) {
    // do nothing
  } else if (OB_FAIL(batch_evict_by_macro_id_(expired_entries))) {
    LOG_WARN("fail to batch evict by macro id", KR(ret), K(expired_entries));
  } else {
    LOG_INFO("succ to batch evict expired macro cache", KR(ret), "expired_cnt", expired_entries.count());
  }
  return ret;
}

int ObSSMacroCacheMgr::filter_expired_macro_cache_(
    const ObSSMacroCacheMetaHandle &meta_handle,
    const int64_t min_retention_time_us,
    ObIArray<MacroBlockId> &expired_entries)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid() || (min_retention_time_us <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache meta handle", KR(ret), K(meta_handle), K(min_retention_time_us));
  } else if (meta_handle()->is_ckpt_marker() ||
             (meta_handle()->get_cache_type() == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) ||
             (SSObjUtil::is_private(meta_handle()->get_macro_id()) && SSObjUtil::is_macro_meta(meta_handle()->get_macro_id()))) {
    // skip ckpt marker, hot tablet macro block, and private meta macro
  } else if (meta_handle()->get_last_access_time_us() < min_retention_time_us) {
    if (OB_FAIL(expired_entries.push_back(meta_handle()->get_macro_id()))) {
      LOG_WARN("fail to push back expired macro id", KR(ret), "macro_id", meta_handle()->get_macro_id());
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::batch_evict_expired_macro_cache_(
    const int64_t bucket_batch_cnt,
    int64_t &bucket_cnt,
    ObIArray<MacroBlockId> &expired_entries)
{
  int ret = OB_SUCCESS;
  ++bucket_cnt;
  if (bucket_batch_cnt == bucket_cnt) {
    if (expired_entries.empty()) {
      // do nothing
    } else if (OB_FAIL(batch_evict_by_macro_id_(expired_entries))) {
      LOG_WARN("fail to batch evict by macro id", KR(ret), K(expired_entries));
    } else {
      LOG_INFO("succ to batch evict expired macro cache", KR(ret), "expired_cnt", expired_entries.count());
    }
    expired_entries.reuse();
    bucket_cnt = 0;
  }
  return ret;
}

int ObSSMacroCacheMgr::replay_ckpt_(uint64_t &next_ckpt_version)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  next_ckpt_version = UINT64_MAX;
  char ckpt_meta_dir[MAX_PATH_SIZE] = { 0 };
  ObMCGetCkptMetaVersionsOp op(tenant_id_);
  ObTenantFileManager *tfm = nullptr;
  ObSSMacroCacheCkptMeta ckpt_meta(tenant_id_);
  int64_t replay_item_cnt = 0;

  char *root_dir = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(root_dir))) {
    ret = OB_SUCCESS;
    next_ckpt_version = 0;
    LOG_WARN("server not bootstrapped yet, no object storage root dir", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_macro_cache_ckpt_meta_dir(
      ckpt_meta_dir, sizeof(ckpt_meta_dir), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get ckpt meta file dir", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(tfm = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", KR(ret));
  } else if (OB_FAIL(tfm->list_remote_files(ckpt_meta_dir, op))) {
    LOG_WARN("fail to list ckpt metas", KR(ret), K(ckpt_meta_dir));
  } else if (op.is_empty()) {
    // no checkpoint, skip replaying
    next_ckpt_version = 0;
  } else if (OB_UNLIKELY(!op.is_max_ckpt_version_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ckpt version invalid", KR(ret), K(ckpt_meta_dir), K(op));
  } else if (FALSE_IT(next_ckpt_version = op.max_ckpt_version_ + 1)) {
  } else if (OB_FAIL(ObSSMacroCacheCkptTask::read_ckpt_meta(op.max_ckpt_version_, op.meta_file_size_, ckpt_meta))) {
    LOG_WARN("fail to read ckpt meta", KR(ret), K(ckpt_meta_dir), K(op));
  } else if (OB_FAIL(restore_cache_from_ckpt_data_(op.max_ckpt_version_, ckpt_meta, replay_item_cnt))) {
    LOG_WARN("fail to restore cache from ckpt data", KR(ret), K(ckpt_meta_dir), K(op), K(ckpt_meta));
  } else {
    LOG_INFO("succ to replay macro cache ckpt", K(op), K(ckpt_meta), K(meta_map_.size()));
  }

  const int64_t ckpt_used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  FLOG_INFO("finish replaying macro cache ckpt", KR(ret), K(op), K(next_ckpt_version),
      K(meta_map_.size()), K(start_time_us), K(ckpt_used_time_us),
      K(replay_item_cnt), K(ckpt_meta), KPC(this));
  return ret;
}

int ObSSMacroCacheMgr::restore_cache_from_ckpt_data_(
    const uint64_t version_id,
    const ObSSMacroCacheCkptMeta &ckpt_meta,
    int64_t &replay_item_cnt)
{
  int ret = OB_SUCCESS;
  replay_item_cnt = 0;
  const int64_t PARALLELISM = 4;
  const ObSSMacroCacheCkptMeta::CkptDataBlockSeqArr &ckpt_data_seq_arr = ckpt_meta.get_ckpt_data_seq_arr();
  const int64_t ckpt_data_block_num = ckpt_data_seq_arr.count();
  const int64_t ckpt_time_us = ckpt_meta.get_ckpt_time();
  const int64_t ckpt_time_diff_us = ObTimeUtility::fast_current_time() - ckpt_time_us;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(version_id == UINT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("version id is invalid", KR(ret), K(ckpt_meta), K(version_id));
  } else if (OB_UNLIKELY(ckpt_time_us <= 0 || ckpt_time_diff_us <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ckpt time is invalid", KR(ret), K(ckpt_meta), K(ckpt_time_diff_us), K(version_id));
  } else if (OB_LIKELY(ckpt_data_block_num > 0)) {
    ObStorageObjectHandle read_handles[PARALLELISM];
    char *read_bufs[PARALLELISM];
    const int64_t buf_size = ckpt_task_.get_data_blk_size();
    ObMemAttr attr(MTL_ID(), "MC_REPLAY_CKPT");
    ObSEArray<ObSSMacroCacheMetaHandle, 64> macro_cache_metas;
    macro_cache_metas.set_attr(attr);

    ObArenaAllocator allocator(attr);
    for (int64_t i = 0; OB_SUCC(ret) && i < PARALLELISM; i++) {
      if (OB_ISNULL(read_bufs[i] = static_cast<char *>(allocator.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc read buf", KR(ret), K(buf_size), K(ckpt_meta), K(version_id));
      }
    }

    int64_t batch_size = PARALLELISM;
    for (int64_t i = 0; OB_SUCC(ret) && i < ckpt_data_block_num; i += batch_size) {
      batch_size = MIN(PARALLELISM, ckpt_data_block_num - i);
      for (int64_t handle_idx = 0; OB_SUCC(ret) && handle_idx < batch_size; handle_idx++) {
        read_handles[handle_idx].reset();
      }

      for (int64_t handle_idx = 0; OB_SUCC(ret) && handle_idx < batch_size; handle_idx++) {
        if (OB_FAIL(ObSSMacroCacheCkptTask::async_read_ckpt_data(
            read_bufs[handle_idx], buf_size,
            version_id,
            ckpt_data_seq_arr.at(i + handle_idx),
            read_handles[handle_idx]))) {
          LOG_WARN("fail to async read ckpt data block", KR(ret), K(i), K(handle_idx),
              K(buf_size), K(ckpt_meta), K(version_id), K(ckpt_data_seq_arr.at(i + handle_idx)));
        }
      }

      for (int64_t handle_idx = 0; OB_SUCC(ret) && handle_idx < batch_size; handle_idx++) {
        macro_cache_metas.reuse();
        if (OB_FAIL(read_handles[handle_idx].wait())) {
          LOG_WARN("fail to async read ckpt data block", KR(ret), K(i), K(handle_idx),
              K(buf_size), K(ckpt_meta), K(version_id), K(ckpt_data_seq_arr.at(i + handle_idx)));
        } else if (OB_FAIL(deserialize_metas_from_buf_(
            read_bufs[handle_idx], read_handles[handle_idx].get_data_size(), macro_cache_metas))) {
          LOG_WARN("fail to deserialize metas from ckpt data block", KR(ret), K(i), K(handle_idx),
              K(buf_size), K(ckpt_meta), K(version_id), K(read_handles[handle_idx]),
              K(ckpt_data_seq_arr.at(i + handle_idx)));
        } else if (OB_FAIL(restore_cache_entries_(ckpt_time_diff_us, macro_cache_metas))) {
          LOG_WARN("fail to restore cache entries", KR(ret), K(i), K(handle_idx),
              K(buf_size), K(ckpt_meta), K(version_id), K(read_handles[handle_idx]),
              K(ckpt_data_seq_arr.at(i + handle_idx)));
        } else {
          replay_item_cnt += macro_cache_metas.count();
        }
      }
    }
  }

  return ret;
}

int ObSSMacroCacheMgr::deserialize_metas_from_buf_(
    const char *buf, const int64_t buf_size,
    common::ObIArray<ObSSMacroCacheMetaHandle> &macro_cache_metas)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheCkptBLKHeader header;
  int64_t pos = 0;
  const int64_t HEADER_FIXED_SERIALIZE_SIZE = header.get_fixed_serialize_size();
  macro_cache_metas.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ckpt block header", KR(ret), KP(buf), K(buf_size));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header invalid", KR(ret), KP(buf), K(buf_size), K(header));
  } else if (OB_UNLIKELY(HEADER_FIXED_SERIALIZE_SIZE + header.get_payload_size() > buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the read content size is smaller than the expected block content size",
        KR(ret), KP(buf), K(buf_size), K(header), K(HEADER_FIXED_SERIALIZE_SIZE));
  } else {
    const uint64_t payload_checksum =
        ob_crc64(buf + HEADER_FIXED_SERIALIZE_SIZE, header.get_payload_size());
    if (OB_UNLIKELY(header.get_payload_checksum() != static_cast<uint32_t>(payload_checksum))) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("payload checksum mismatch", KR(ret), KP(buf), K(buf_size),
          K(header), K(HEADER_FIXED_SERIALIZE_SIZE), K(payload_checksum));
    }
  }

  int64_t cur_offset = HEADER_FIXED_SERIALIZE_SIZE;
  ObSSMacroCacheMeta *meta = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < header.get_item_count(); i++) {
    meta = nullptr;
    if (OB_ISNULL(meta = static_cast<ObSSMacroCacheMeta *>(macro_meta_allocator_.alloc()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for macro cache meta",
          KR(ret), K(i), K(header), K(cur_offset));
    } else {
      // Use meta_handle to manage the meta lifecycle, avoiding manual deallocation
      ObSSMacroCacheMetaHandle meta_handle;
      meta = new (meta) ObSSMacroCacheMeta();
      meta_handle.set_ptr(meta);
      update_macro_meta_alloc_cnt(1);

      pos = 0;
      if (OB_FAIL(meta->deserialize(buf + cur_offset, buf_size - cur_offset, pos))) {
        LOG_WARN("fail to deserialize macro cache meta", KR(ret),
            KP(buf), K(buf_size), K(i), K(header), K(cur_offset));
      } else if (OB_FAIL(macro_cache_metas.push_back(meta_handle))) {
        LOG_WARN("deserialized pos unexpected", KR(ret), KP(buf), K(buf_size),
            K(header), K(i), K(cur_offset), K(pos), KPC(meta), K(macro_cache_metas.count()));
      } else {
        cur_offset += pos;
      }

      LOG_DEBUG("deserialize a meta", KR(ret), KPC(meta), KPC(meta_handle()), K(macro_cache_metas.count()));
    }
  }
  LOG_INFO("ObSSMacroCacheMgr: deserialize metas", KR(ret), K(macro_cache_metas.count()));
  return ret;
}

int ObSSMacroCacheMgr::restore_cache_entries_(
    const int64_t ckpt_time_diff_us,
    common::ObIArray<ObSSMacroCacheMetaHandle> &macro_cache_metas)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(ckpt_time_diff_us <= 0 || macro_cache_metas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ckpt_time_diff_us), K(macro_cache_metas.count()));
  }

  ObSSMacroCacheMetaHandle meta_handle;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_cache_metas.count(); i++) {
    meta_handle.set_ptr(macro_cache_metas.at(i).get_ptr());
    if (OB_UNLIKELY(!meta_handle.is_valid() || !meta_handle()->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache meta is invalid", KR(ret),
          K(ckpt_time_diff_us), K(i), KPC(meta_handle()), K(macro_cache_metas));
    } else {
      meta_handle()->set_last_access_time_us(
          meta_handle()->get_last_access_time_us() + ckpt_time_diff_us);
      ObSSMacroCacheRestoreInsertCallback insert_callback(*this);
      ObSSMacroCacheRestoreUpdateCallback update_callback(*this, meta_handle);
      if (OB_FAIL(meta_map_.set_or_update(
          meta_handle()->get_macro_id(), meta_handle,
          insert_callback, update_callback))) {
        LOG_WARN("fail to set or update meta map", KR(ret),
            K(ckpt_time_diff_us), K(i), KPC(meta_handle()), K(macro_cache_metas));
      } else if (OB_UNLIKELY(OB_SUCCESS != (ret = update_callback.get_ret()))) {
        LOG_WARN("fail to execute meta map update callback", KR(ret), K(update_callback));
      } else if (OB_FAIL(update_write_cache_size_for_restore_(meta_handle, update_callback))) {
        LOG_WARN("fail to update write cache size for restore", KR(ret), K(meta_handle),
                 K(update_callback));
      }
    }
  }
  LOG_INFO("ObSSMacroCacheMgr: restore cache entries", KR(ret), K(macro_cache_metas.count()));
  return ret;
}

int ObSSMacroCacheMgr::update_write_cache_size_for_restore_(
    const ObSSMacroCacheMetaHandle &meta_handle,
    const ObSSMacroCacheRestoreUpdateCallback &update_callback)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else if (FALSE_IT(macro_id = meta_handle()->get_macro_id())) {
  } else if (update_callback.is_executed()) { // 1. update_callback is executed
    if (update_callback.is_ori_write_cache()) {
      if (meta_handle()->get_is_write_cache()) { // 1.1 ori write cache & cur write cache
        inc_write_cache_size(macro_id, meta_handle()->get_size() - update_callback.get_ori_size());
      } else { // 1.2 ori write_cache & cur read cache
        dec_write_cache_size(macro_id, update_callback.get_ori_size());
      }
    } else {
      if (meta_handle()->get_is_write_cache()) { // 1.3 ori read cache & cur write cache
        inc_write_cache_size(macro_id, meta_handle()->get_size());
      } else { // 1.4 ori read cache & cur read cache
        // no need to update write_cache_size
      }
    }
  } else { // 2. insert_callback is executed
    if (meta_handle()->get_is_write_cache()) {
      inc_write_cache_size(macro_id, meta_handle()->get_size());
    }
  }
  return ret;
}

int ObSSMacroCacheMgr::restore_entry_for_insert_(ObSSMacroCacheMeta &macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *macro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_FAIL(get_macro_cache(macro_cache_meta.get_cache_type(), macro_cache))) {
    LOG_WARN("fail to get macro cache", KR(ret), K(macro_cache_meta));
  } else if (OB_ISNULL(macro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache is null", KR(ret), K(macro_cache_meta));
  } else if (OB_FAIL(macro_cache->restore_entry_for_insert(macro_cache_meta))) {
    LOG_WARN("fail to restore_entry_for_insert", KR(ret), K(macro_cache_meta));
  }
  return ret;
}

int ObSSMacroCacheMgr::restore_entry_for_update_(
    ObSSMacroCacheMeta &old_macro_cache_meta, ObSSMacroCacheMeta &new_macro_cache_meta)
{
  int ret = OB_SUCCESS;
  SSMacroCacheReqGuard guard(flying_req_cnt_);
  ObSSMacroCache *old_macro_cache = nullptr;
  ObSSMacroCache *new_macro_cache = nullptr;
  const ObSSMacroCacheType old_cache_type = old_macro_cache_meta.get_cache_type();
  const ObSSMacroCacheType new_cache_type = new_macro_cache_meta.get_cache_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (old_cache_type == new_cache_type) { // 1. old_cache_type == new_cache_type
    // update cur MacroCache's LRU
    if (OB_FAIL(get_macro_cache(new_cache_type, new_macro_cache))) {
      LOG_WARN("fail to get macro cache", KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    } else if (OB_ISNULL(new_macro_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache is null", KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    } else if (OB_FAIL(new_macro_cache->restore_entry_for_update(
        old_macro_cache_meta, new_macro_cache_meta))) {
      LOG_WARN("fail to restore_entry_for_update", KR(ret),
          K(old_macro_cache_meta), K(new_macro_cache_meta));
    }
  } else { // 2. old_cache_type != new_cache_type
    // remove from old MacroCache's LRU, insert into new MacroCache's LRU
    if (OB_FAIL(get_macro_cache(old_cache_type, old_macro_cache))) {
      LOG_WARN("fail to get macro cache", KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    } else if (OB_FAIL(get_macro_cache(new_cache_type, new_macro_cache))) {
      LOG_WARN("fail to get macro cache", KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta));
    } else if (OB_ISNULL(old_macro_cache) || OB_ISNULL(new_macro_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache is null", KR(ret), K(old_macro_cache_meta), K(new_macro_cache_meta),
               KP(old_macro_cache), KP(new_macro_cache));
    } else if (OB_FAIL(old_macro_cache->remove_from_lru_list(old_macro_cache_meta))) {
      LOG_WARN("fail to remove old macro cache meta", KR(ret), K(old_macro_cache_meta));
    } else if (OB_FAIL(new_macro_cache->restore_entry_for_insert(new_macro_cache_meta))) {
      LOG_ERROR("fail to insert new macro cache meta", KR(ret), K(new_macro_cache_meta));
    }
  }
  return ret;
}

class ObSSMacroCacheTabletStatUpdateCachedSizeFunc
{
public:
  ObSSMacroCacheTabletStatUpdateCachedSizeFunc(const ObSSMacroCacheQueueSpaceStat &queue_space)
    : queue_space_(queue_space)
  {}

  void operator()(SSMacroCacheTabletStatMapPairType &entry)
  {
    entry.second.merge_cached_space(queue_space_);
  }

private:
  const ObSSMacroCacheQueueSpaceStat &queue_space_;
};

class ObSSMacroCacheTabletStatSetCachedSizeFunc
{
public:
  ObSSMacroCacheTabletStatSetCachedSizeFunc(const ObSSMacroCacheQueueSpaceStat &queue_space)
    : queue_space_(queue_space)
  {}

  void operator()(SSMacroCacheTabletStatMapPairType &entry)
  {
    entry.second.set_cached_space(queue_space_);
  }

private:
  const ObSSMacroCacheQueueSpaceStat &queue_space_;
};

class ObSSMacroCacheAggregateTabletStatFunc
{
public:
  ObSSMacroCacheAggregateTabletStatFunc(SSMacroCacheTabletStatMap &tablet_stat_map)
      : tablet_stat_map_(tablet_stat_map), external_table_cache_stat_()
  {
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      macro_blocks_stat_[i].reset();
    }
  }
  void update_macro_blocks_stat(const ObSSMacroCacheMeta *meta_ptr)
  {
    ObSSMacroBlockType block_type = ObSSMacroBlockType::MAX_TYPE;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(meta_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta ptr should not be null", KR(ret));
    } else {
      SSObjUtil::get_ss_macro_block_type(meta_ptr->get_macro_id(), block_type);
      if (OB_LIKELY(is_valid_ss_macro_block_type(block_type))) {
        const uint8_t idx = static_cast<uint8_t>(block_type);
        macro_blocks_stat_[idx].update_used_disk_size(meta_ptr->get_size());
      }
    }
  }
  int operator()(const SSMacroCacheMetaMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheMeta *meta_ptr = nullptr;
    if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
    } else {
      ObSSMacroCacheTabletStat tablet_stat;
      const bool is_hot_macro =
          (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == meta_ptr->get_cache_type());
      tablet_stat.update_cached_space(
          is_hot_macro, meta_ptr->get_is_in_fifo_list(), 1/*block_count*/, meta_ptr->get_size());
      ObSSMacroCacheTabletStatUpdateCachedSizeFunc update_func(tablet_stat.get_queue_space());

      if (OB_FAIL(tablet_stat_map_.set_or_update(
          meta_ptr->get_effective_tablet_id(), tablet_stat, update_func))) {
        LOG_WARN("fail to set or update tablet stat", KR(ret), KPC(meta_ptr));
      } else if (ObStorageObjectType::EXTERNAL_TABLE_FILE ==
          meta_ptr->get_macro_id().storage_object_type()) {
        external_table_cache_stat_.update_cache_stat(
            1, meta_ptr->get_size(), meta_ptr->get_size());
      }
      if (OB_SUCC(ret)) {
        update_macro_blocks_stat(meta_ptr);
      }
    }
    return ret;
  }

  const ObStorageCacheStat &external_table_cache_stat() const
  {
    return external_table_cache_stat_;
  }

  const ObSSCacheStatInfo &get_macro_blocks_cache_stat(const ObSSMacroBlockType block_type) const
  {
    const uint8_t idx = static_cast<uint8_t>(block_type);
    return macro_blocks_stat_[idx];
  }

private:
  SSMacroCacheTabletStatMap &tablet_stat_map_;
  ObStorageCacheStat external_table_cache_stat_;
  ObSSCacheStatInfo macro_blocks_stat_[SS_MACRO_BLOCK_MAX_TYPE_VAL];
};

int ObSSMacroCacheMgr::batch_clean_old_tablet_stat_cached_size_(const ObIArray<uint64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_id_cnt = tablet_ids.count();
  ObSSMacroCacheTabletStat tablet_stat;
  ObSSMacroCacheQueueSpaceStat queue_space;
  ObSSMacroCacheTabletStatSetCachedSizeFunc set_func(queue_space);
  LOG_INFO("begin to batch clean old tablet stat cached size", KR(ret), K(tablet_id_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_id_cnt); ++i) {
    const uint64_t cur_tablet_id = tablet_ids.at(i);
    LOG_INFO("try to clean old tablet stat cached size", KR(ret), K(i), K(cur_tablet_id));
    tablet_stat.reset();
    if (OB_FAIL(tablet_stat_map_.set_or_update(cur_tablet_id, tablet_stat, set_func))) {
      LOG_WARN("fail to update tablet stat cached size to 0", KR(ret), K(cur_tablet_id));
    }
  }
  LOG_INFO("end to batch clean old tablet stat cached size", KR(ret));
  return ret;
}
/*
1. if tablet_stat_map_ does not have the tablet_id which is in tmp_tablet_stat_map, set cached size to 0
2. if tmp_tablet_stat_map is empty, set all cached size in tablet_stat_map_ to 0
*/
int ObSSMacroCacheMgr::clean_old_tablet_stat_cached_size_(const SSMacroCacheTabletStatMap &tmp_tablet_stat_map)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheTabletStat tablet_stat;
  int64_t bucket_cnt = 0;
  const int64_t BUCKET_BATCH_CNT = 10000;
  ObSEArray<uint64_t, 64> clean_cached_size_tablet_ids;
  SSMacroCacheTabletStatMap::bucket_iterator bucket_iter = tablet_stat_map_.bucket_begin();
  while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != tablet_stat_map_.bucket_end())) {
    {
      SSMacroCacheTabletStatMap::hashtable::bucket_lock_cond blk(*bucket_iter);
      SSMacroCacheTabletStatMap::hashtable::readlocker locker(blk.lock());
      SSMacroCacheTabletStatMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
      while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
        const uint64_t tablet_id = node_iter->first;
        if (tmp_tablet_stat_map.size() == 0) {
          clean_cached_size_tablet_ids.push_back(tablet_id);
        } else {
          if (OB_FAIL(tmp_tablet_stat_map.get_refactored(tablet_id, tablet_stat))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              clean_cached_size_tablet_ids.push_back(tablet_id);
            } else {
              LOG_WARN("fail to get tablet stat from tmp_tablet_stat_map", KR(ret), K(tablet_id));
            }
          }
        }
        node_iter++;
      }
      ++bucket_cnt;
      ++bucket_iter;
    }
    if (OB_SUCC(ret) && (BUCKET_BATCH_CNT == bucket_cnt)) {
      if (clean_cached_size_tablet_ids.empty()) {
      } else if (OB_FAIL(batch_clean_old_tablet_stat_cached_size_(clean_cached_size_tablet_ids))) {
        LOG_WARN("fail to batch clean old tablet stat cached size", KR(ret), K(clean_cached_size_tablet_ids.count()));
      }
      bucket_cnt = 0;
      clean_cached_size_tablet_ids.reuse();
    }
  }
  if (OB_SUCC(ret) && !clean_cached_size_tablet_ids.empty()) {
    if (OB_FAIL(batch_clean_old_tablet_stat_cached_size_(clean_cached_size_tablet_ids))) {
      LOG_WARN("fail to batch clean old tablet stat cached size", KR(ret), K(clean_cached_size_tablet_ids.count()));
    }
    clean_cached_size_tablet_ids.reuse();
  }
  return ret;
}

int ObSSMacroCacheMgr::calibrate_tablet_stat_cached_size_()
{
  int ret = OB_SUCCESS;
  const int64_t tablet_stat_bucket_num = tablet_stat_map_.bucket_count();
  SSMacroCacheTabletStatMap tmp_tablet_stat_map;
  ObMemAttr attr(MTL_ID(), "TabletStatTask");
  ObSSMacroCacheAggregateTabletStatFunc aggregate_func(tmp_tablet_stat_map);
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(tmp_tablet_stat_map.create(tablet_stat_bucket_num, attr))) {
    LOG_WARN("fail to create new tablet stat map", KR(ret), K(tablet_stat_bucket_num));
  } else if (meta_map_.size() > 0 && OB_FAIL(meta_map_.foreach_refactored(aggregate_func))) {
    LOG_WARN("fail to aggregate tablet stat", KR(ret), K(tablet_stat_bucket_num));
  } else if (OB_LIKELY(tmp_tablet_stat_map.size() > 0)) {
    external_table_cache_stat_ = aggregate_func.external_table_cache_stat();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      macro_blocks_stat_[i].set_used_disk_size(aggregate_func.get_macro_blocks_cache_stat(static_cast<ObSSMacroBlockType>(i)).get_used_disk_size());
    }
    SSMacroCacheTabletStatMap::const_iterator iter = tmp_tablet_stat_map.begin();
    ObSSMacroCacheTabletStat tablet_stat;
    for (; OB_SUCC(ret) && iter != tmp_tablet_stat_map.end(); ++iter) {
      const ObSSMacroCacheTabletStat &cur_tablet_stat = iter->second;
      ObSSMacroCacheTabletStatSetCachedSizeFunc set_func(cur_tablet_stat.get_queue_space());
      tablet_stat.reset();
      tablet_stat.merge_cached_space(cur_tablet_stat.get_queue_space());
      if (OB_FAIL(tablet_stat_map_.set_or_update(iter->first, tablet_stat, set_func))) {
        LOG_WARN("fail to update tablet stat", KR(ret), K(iter->first), K(iter->second));
      }
    } // end for
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(clean_old_tablet_stat_cached_size_(tmp_tablet_stat_map))) {
      LOG_WARN("fail to clean old tablet stat cached size", KR(ret));
    }
  }
  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheMgr: finish calibrating cached size",
      KR(ret), K(used_time_us), K(start_time_us), K(external_table_cache_stat_),
      K(tablet_stat_bucket_num), K(tmp_tablet_stat_map.size()), K(tablet_stat_map_.size()));
  return ret;
}

int ObSSMacroCacheMgr::clean_deleted_tablet_()
{
  int ret = OB_SUCCESS;
  bool enable_private_gc_hot_macro_cache_convert_to_auto = false;
  hash::ObHashSet<uint64_t> active_tablet_ids;
  ObMemAttr attr(MTL_ID(), "MCCleanTablet");
  const int64_t tablet_stat_bucket_num = tablet_stat_map_.bucket_count();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(active_tablet_ids.create(tablet_stat_bucket_num, attr))) {
    LOG_WARN("fail to create active_tablet_ids", KR(ret), K(tablet_stat_bucket_num), K(attr));
  } else if (OB_FAIL(get_active_tablet_ids(active_tablet_ids, attr))) {
    LOG_WARN("fail to get active_tablet_ids", KR(ret), K(attr), K(tablet_stat_bucket_num));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant config for deleted tablet cleanup", KR(ret), K(tenant_id_));
    } else {
      enable_private_gc_hot_macro_cache_convert_to_auto =
          tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto;
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(clean_deleted_tablet_in_meta_map_(
        active_tablet_ids, enable_private_gc_hot_macro_cache_convert_to_auto))) {
      LOG_WARN("fail to clean_deleted_tablet_in_meta_map_", KR(tmp_ret),
          K(enable_private_gc_hot_macro_cache_convert_to_auto));
      ret = tmp_ret;
    }

    if (OB_TMP_FAIL(clean_deleted_tablet_in_tablet_stat_map_(active_tablet_ids))) {
      LOG_WARN("fail to clean_deleted_tablet_in_tablet_stat_map_", KR(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

class ObSSMacroCacheCopyTabletIdFunc
{
public:
  ObSSMacroCacheCopyTabletIdFunc(ObIArray<uint64_t> &tablet_ids_in_tablet_stat_map)
      : tablet_ids_in_tablet_stat_map_(tablet_ids_in_tablet_stat_map)
  {}

  int operator()(const SSMacroCacheTabletStatMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tablet_ids_in_tablet_stat_map_.push_back(entry.first))) {
      LOG_WARN("fail to push tablet id to tablet_ids_in_tablet_stat_map",
          KR(ret), K(tablet_ids_in_tablet_stat_map_.count()));
    }
    return ret;
  }

private:
  ObIArray<uint64_t> &tablet_ids_in_tablet_stat_map_;
};

int get_active_tablet_ids(hash::ObHashSet<uint64_t> &active_tablet_ids, const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = nullptr;
  int64_t cleaned_tablet_count = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  active_tablet_ids.reuse();
  if (OB_ISNULL(t3m = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", KR(ret));
  } else {
    ObArenaAllocator allocator(attr);
    ObTenantTabletIterator tablet_iterator(*t3m, allocator, nullptr/*no op*/);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      tablet = nullptr;
      if (OB_FAIL(tablet_iterator.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next tablet", KR(ret));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is null", KR(ret), K(tablet_handle));
      } else if (OB_FAIL(active_tablet_ids.set_refactored(tablet->get_tablet_id().id()))) {
        LOG_WARN("fail to push back tablet id", KR(ret), KPC(tablet), K(active_tablet_ids.size()));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheMgr: finish get_active_tablet_ids", KR(ret),
      K(used_time_us), K(start_time_us), K(active_tablet_ids.size()));
  return ret;
}

int ObSSMacroCacheMgr::clean_deleted_tablet_in_tablet_stat_map_(
    const common::hash::ObHashSet<uint64_t> &active_tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t cleaned_tablet_count = 0;
  // If a tablet in tablet_stat_map is neither active nor referenced by a retained shared macro
  // cache meta, erase it from tablet_stat_map.
  ObSEArray<uint64_t, 8> tablet_ids_in_tablet_stat_map;
  ObMemAttr attr(MTL_ID(), "MCCleanTablet");
  tablet_ids_in_tablet_stat_map.set_attr(attr);
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else {
    // 1. copy tablet ids in tablet_stat_map to tablet_ids_in_tablet_stat_map
    ObSSMacroCacheCopyTabletIdFunc copy_tablet_id_func(tablet_ids_in_tablet_stat_map);
    if (OB_FAIL(tablet_ids_in_tablet_stat_map.reserve(tablet_stat_map_.size()))) {
      LOG_WARN("fail to reserve", KR(ret), "size", tablet_stat_map_.size());
    } else if ((tablet_stat_map_.size() > 0) &&
               OB_FAIL(tablet_stat_map_.foreach_refactored(copy_tablet_id_func))) {
      LOG_WARN("fail to copy tablet ids", KR(ret), K(tablet_stat_map_.size()));
    }

    // 2. find tablet ids in tablet_ids_in_tablet_stat_map but not in active_tablet_ids
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_in_tablet_stat_map.count(); i++) {
      const uint64_t tablet_id = tablet_ids_in_tablet_stat_map.at(i);
      if (OB_FAIL(active_tablet_ids.exist_refactored(tablet_id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret) {
          // 3. tablet id not in active_tablet_ids, erase it from tablet_stat_map
          ret = OB_SUCCESS;
          if (OB_FAIL(tablet_stat_map_.erase_refactored(tablet_id))) {
            LOG_WARN("fail to erase deleted tablet from stat map", KR(ret), K(tablet_id));
          } else {
            cleaned_tablet_count++;
          }
        } else {
          LOG_WARN("fail to check exist", KR(ret), K(tablet_id));
        }
      }
    }
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheMgr: finish clean deleted tablets", KR(ret),
      K(used_time_us), K(start_time_us), K(active_tablet_ids.size()),
      K(tablet_ids_in_tablet_stat_map.count()), K(cleaned_tablet_count));
  return ret;
}

int ObSSMacroCacheMgr::get_macro_blocks_stat(
    const ObSSMacroBlockType block_type,
    ObSSCacheStatInfo &cache_stat_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_ss_macro_block_type(block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block type", KR(ret), K(block_type));
  } else {
    cache_stat_info = macro_blocks_stat_[static_cast<uint8_t>(block_type)];
  }
  return ret;
}

class ObSSMacroCacheCopyMacroIdFunc
{
public:
  ObSSMacroCacheCopyMacroIdFunc(ObIArray<MacroBlockId> &macro_ids_in_meta_map)
      : macro_ids_in_meta_map_(macro_ids_in_meta_map)
  {}

  int operator()(const SSMacroCacheMetaMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(macro_ids_in_meta_map_.push_back(entry.first))) {
      LOG_WARN("fail to push macro id to macro_ids_in_meta_map",
          KR(ret), K(macro_ids_in_meta_map_.count()));
    }
    return ret;
  }

private:
  ObIArray<MacroBlockId> &macro_ids_in_meta_map_;
};

int ObSSMacroCacheMgr::clean_deleted_tablet_in_meta_map_(
    const hash::ObHashSet<uint64_t> &active_tablet_ids,
    const bool enable_private_gc_hot_macro_cache_convert_to_auto)
{
  int ret = OB_SUCCESS;
  int64_t cleaned_meta_count = 0;
  int64_t retained_by_born_tablet_count = 0;
  int64_t converted_to_auto_count = 0;
  ObSEArray<MacroBlockId, 8> macro_ids_in_meta_map;
  ObMemAttr attr(MTL_ID(), "MCCleanTablet");
  macro_ids_in_meta_map.set_attr(attr);
  const int64_t meta_map_size = meta_map_.size();
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (meta_map_size > 0) {
    macro_ids_in_meta_map.reserve(meta_map_size);
    ObSSMacroCacheCopyMacroIdFunc copy_macro_id_func(macro_ids_in_meta_map);
    if (OB_FAIL(meta_map_.foreach_refactored(copy_macro_id_func))) {
      LOG_WARN("fail to copy macro ids", KR(ret), K(meta_map_size));
    }

    ObSSMacroCacheMetaHandle meta_handle;
    const int64_t DELETE_TIME_THRESHOLD_US = 1800LL * 1000LL * 1000LL; // 30m
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_ids_in_meta_map.count(); i++) {
      const MacroBlockId &macro_id = macro_ids_in_meta_map.at(i);
      meta_handle.reset(); 
      if (SSObjUtil::has_effective_tablet_id(macro_id)) {
        if (OB_FAIL(meta_map_.get_refactored(macro_id, meta_handle))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // ignore ret. may be already erased
          } else {
            LOG_WARN("fail to get meta handle", KR(ret), K(macro_id));
          }
        } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle invalid", KR(ret), K(macro_id), K(meta_handle));
        } else if (ObSSMacroCacheType::MACRO_BLOCK == meta_handle()->get_cache_type()
            || ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == meta_handle()->get_cache_type()) {
          // Private macros are owned by their born Tablet for GC. Other macros use the effective Tablet.
          const uint64_t tablet_id = ((macro_id.is_private())
              ? static_cast<uint64_t>(macro_id.second_id())
              : meta_handle()->get_effective_tablet_id());
          if (OB_UNLIKELY(!ObTabletID(tablet_id).is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid tablet id", KR(ret), K(macro_id), K(tablet_id), KPC(meta_handle()));
          } else if (meta_handle()->get_last_access_time_us() + DELETE_TIME_THRESHOLD_US > ObTimeUtility::fast_current_time()) {
            // skip
          } else if (OB_FAIL(active_tablet_ids.exist_refactored(tablet_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              if (macro_id.is_private()) {
                retained_by_born_tablet_count++;
              }
            } else if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              if (enable_private_gc_hot_macro_cache_convert_to_auto
                  && macro_id.is_shared_data_or_meta()) {
                // effective owner 已删除时不能据此删除 fork 共享缓存。HOT 只降为 AUTO，
                // 已是 AUTO 时交给普通 LRU/expire 后续回收。
                if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == meta_handle()->get_cache_type()) {
                  if (OB_FAIL(modify_macro_cache_type_and_touch(
                      macro_id,
                      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                      ObSSMacroCacheType::MACRO_BLOCK))) {
                    LOG_WARN("fail to convert deleted tablet shared macro cache to auto",
                        KR(ret), K(macro_id), K(tablet_id), KPC(meta_handle()));
                  } else {
                    converted_to_auto_count++;
                  }
                } else {
                  // 由 AUTO LRU/expire 负责后续回收。
                }
              } else if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(macro_id))) {
                LOG_WARN("fail to delete local file", KR(ret),
                    K(macro_id), K(tablet_id), KPC(meta_handle()));
              } else {
                cleaned_meta_count++;
              }
            } else {
              LOG_WARN("fail to check exist", KR(ret),
                  K(macro_id), K(tablet_id), KPC(meta_handle()));
            }
          }
        }
      }
    } // end for
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheMgr: finish clean deleted tablets in meta map", KR(ret),
      K(used_time_us), K(start_time_us), K(active_tablet_ids.size()), K(meta_map_size),
      K(meta_map_.size()), K(cleaned_meta_count), K(retained_by_born_tablet_count),
      K(converted_to_auto_count), K(enable_private_gc_hot_macro_cache_convert_to_auto));
  return ret;
}


class ObSSMacroCacheLSInfoUpdateFunc
{
public:
  ObSSMacroCacheLSInfoUpdateFunc(
      const int64_t delta_tablet_cnt,
      const ObSSMacroCacheQueueSpaceStat &queue_space)
    : delta_tablet_cnt_(delta_tablet_cnt),
      queue_space_(queue_space)
  {}

  void operator()(SSMacroCacheLSInfoMapPairType &entry)
  {
    entry.second.add_tablet_cnt(delta_tablet_cnt_);
    entry.second.merge_queue_space(queue_space_);
  }

  TO_STRING_KV(K(delta_tablet_cnt_), K(queue_space_));

private:
  const int64_t delta_tablet_cnt_;
  const ObSSMacroCacheQueueSpaceStat &queue_space_;
};


class ObSSMacroCacheAggregateLSInfoFunc
{
public:
  ObSSMacroCacheAggregateLSInfoFunc(SSMacroCacheLSInfoMap &ls_info_map)
    : is_inited_(false), ls_handles_(), ls_info_map_(ls_info_map)
  {
    ObMemAttr attr(MTL_ID(), "AggLSInfoFunc");
    ls_handles_.set_attr(attr);
  }

  int init()
  {
    int ret = OB_SUCCESS;
    ObLSService *ls_service = nullptr;
    ObSEArray<ObLSID, 8> ls_ids;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("aggregate ls info func init twice", KR(ret));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_service should not be null", KR(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
      LOG_WARN("fail to get ls ids", KR(ret));
    } else {
      ObLSHandle ls_handle;
      for (int64_t i = 0; OB_SUCC(ret) && (i < ls_ids.count()); ++i) {
        ls_handle.reset();
        if (OB_FAIL(ls_service->get_ls(ls_ids.at(i), ls_handle, ObLSGetMod::SS_MACRO_CACHE_MOD))) {
          LOG_WARN("fail to get ls", KR(ret), K(i), "ls_id", ls_ids.at(i).id());
        } else if (OB_FAIL(ls_handles_.push_back(ls_handle))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
    return ret;
  }

  int operator()(const SSMacroCacheTabletStatMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    bool is_ls_id_exist = false;
    ObLSID ls_id;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("aggregate ls info func is not inited", KR(ret));
    } else if (OB_FAIL(get_ls_id_by_tablet_id_(ObTabletID(entry.first), is_ls_id_exist, ls_id))) {
      LOG_WARN("fail to get ls id by tablet id", KR(ret), K(entry));
    } else if (is_ls_id_exist) {
      ObSSMacroCacheLSInfo ls_info;
      ls_info.set_ls_id(ls_id);
      ls_info.set_tablet_cnt(1);
      ls_info.set_queue_space(entry.second.get_queue_space());
      ObSSMacroCacheLSInfoUpdateFunc update_func(1/*delta_tablet_cnt*/, entry.second.get_queue_space());
      if (OB_FAIL(ls_info_map_.set_or_update(ls_id, ls_info, update_func))) {
        LOG_WARN("fail to set or update ls info map", KR(ret), K(ls_id), K(ls_info), K(update_func));
      }
    } else { // !is_ls_id_exist
      LOG_DEBUG("tablet id has no relative ls id now", K(entry));
    }
    return ret;
  }

  int get_ls_id_by_tablet_id_(const ObTabletID &tablet_id, bool &is_ls_id_exist, ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    is_ls_id_exist = false;
    ls_id = INVALID_LS;
    const int64_t ls_handle_cnt = ls_handles_.count();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("aggregate ls info func is not inited", KR(ret));
    } else if (tablet_id.is_reserved_tablet()) {
      is_ls_id_exist = true;
      ls_id = SYS_LS;
    } else if (!tablet_id.is_reserved_tablet() && (ls_handle_cnt > 0)) {
      ObLSTabletService *tablet_svr = nullptr;
      for (int64_t i = 0; (i < ls_handle_cnt) && OB_SUCC(ret) && !is_ls_id_exist; ++i) {
        const ObLSHandle &ls_handle = ls_handles_.at(i);
        ObLSID cur_ls_id;
        if (OB_ISNULL(ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is null", KR(ret), K(i), K(ls_handle_cnt), K(ls_handle));
        } else if (FALSE_IT(cur_ls_id = ls_handle.get_ls()->get_ls_id())) {
        } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get ls tablet svr", KR(ret), K(i), K(cur_ls_id));
        } else if (OB_FAIL(tablet_svr->is_tablet_exist(tablet_id, is_ls_id_exist))) {
          LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id), K(cur_ls_id));
        } else if (is_ls_id_exist) {
          ls_id = cur_ls_id;
        } else {
          ls_id.reset();
        }
      }
    }
    return ret;
  }

private:
  bool is_inited_;
  ObSEArray<ObLSHandle, 8> ls_handles_;
  SSMacroCacheLSInfoMap &ls_info_map_;
};


class ObSSMacroCacheGetNotExistLSInfoFunc
{
public:
  ObSSMacroCacheGetNotExistLSInfoFunc(SSMacroCacheLSInfoMap &ls_info_map)
    : ls_info_map_(ls_info_map), not_exist_ls_ids_()
  {
    ObMemAttr attr(MTL_ID(), "GetNoExtLSInfo");
    not_exist_ls_ids_.set_attr(attr);
  }

  int operator()(SSMacroCacheLSInfoMapPairType &entry)
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheLSInfo ls_info;
    if (OB_FAIL(ls_info_map_.get_refactored(entry.first, ls_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
        if (OB_FAIL(not_exist_ls_ids_.push_back(entry.first))) {
          LOG_WARN("fail to push back ls id", KR(ret), K(entry));
        }
      } else {
        LOG_WARN("fail to get refactored", KR(ret), K(entry));
      }
    } else {
      // exist. do nothing
    }
    return ret;
  }

public:
  SSMacroCacheLSInfoMap &ls_info_map_;
  ObSEArray<ObLSID, 8> not_exist_ls_ids_;
};


int ObSSMacroCacheMgr::update_ls_info_()
{
  int ret = OB_SUCCESS;
  SSMacroCacheLSInfoMap tmp_ls_info_map;
  ObMemAttr attr(MTL_ID(), "UpdateLSInfo");
  ObSSMacroCacheAggregateLSInfoFunc aggregate_func(tmp_ls_info_map);
  const int64_t start_us = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(tmp_ls_info_map.create(10/*bucket_num*/, attr))) {
    LOG_WARN("fail to create tmp ls info map", KR(ret));
  } else if (OB_FAIL(aggregate_func.init())) {
    LOG_WARN("fail to init aggregate func", KR(ret));
  } else if (OB_FAIL(tablet_stat_map_.foreach_refactored(aggregate_func))) {
    LOG_WARN("fail to aggregate ls info", KR(ret));
  } else if (OB_LIKELY(tmp_ls_info_map.size() > 0)) {
    // Note: tmp_ls_info_map has no concurrent operation, thus no need to add bucket lock
    SSMacroCacheLSInfoMap::const_iterator iter = tmp_ls_info_map.begin();
    for (; OB_SUCC(ret) && iter != tmp_ls_info_map.end(); ++iter) {
      if (OB_FAIL(ls_info_map_.set_refactored(iter->first, iter->second, true/*overwrite*/))) {
        LOG_WARN("fail to set refactored ls info map", KR(ret), K(iter->first), K(iter->second));
      }
    }
  }

  // clean old ls info
  if (OB_SUCC(ret)) {
    ObSSMacroCacheGetNotExistLSInfoFunc func(tmp_ls_info_map);
    if (OB_FAIL(ls_info_map_.foreach_refactored(func))) {
      LOG_WARN("fail to foreach refactored", KR(ret));
    } else if (!func.not_exist_ls_ids_.empty()) {
      const int64_t not_exist_ls_id_cnt = func.not_exist_ls_ids_.count();
      for (int64_t i = 0; (i < not_exist_ls_id_cnt) && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ls_info_map_.erase_refactored(func.not_exist_ls_ids_.at(i)))) {
          LOG_WARN("fail to erase refactored", KR(ret), "ls_id", func.not_exist_ls_ids_.at(i));
        }
      }
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("ObSSMacroCacheMgr: finish to update ls info", KR(ret), K(cost_us), K(start_us),
           "ls_info_map_size", ls_info_map_.size());
  print_ls_info_();
  return ret;
}

void ObSSMacroCacheMgr::print_ls_info_()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[2048] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos, "ObSSMacroCacheMgr ls info: "))) {
    LOG_WARN("fail to print macro cache mgr ls info", KR(ret));
  }
  SSMacroCacheLSInfoMap::bucket_iterator bucket_iter = ls_info_map_.bucket_begin();
  while (OB_SUCC(ret) && !is_stopped_ && (bucket_iter != ls_info_map_.bucket_end())) {
    SSMacroCacheLSInfoMap::hashtable::bucket_lock_cond blk(*bucket_iter);
    SSMacroCacheLSInfoMap::hashtable::readlocker locker(blk.lock());
    SSMacroCacheLSInfoMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
    while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
      const ObSSMacroCacheLSInfo &ls_info = node_iter->second;
      if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
          "{ls_id:%ld, tablet_cnt:%ld, valid_required_size:%ld, valid_block_count:%ld, "
          "hot_required_size:%ld, hot_block_count:%ld} ",
          ls_info.get_ls_id().id(), ls_info.get_tablet_cnt(), ls_info.get_valid_required_size(),
          ls_info.get_valid_block_count(), ls_info.get_hot_required_size(), ls_info.get_hot_block_count()))) {
        LOG_WARN("fail to print ls info", KR(ret), K(pos), K(ls_info));
      }
      node_iter++;
    }
    ++bucket_iter;
  }

  LOG_INFO("ObSSMacroCacheMgr print ls info", KR(ret), K(pos), K(buf), "write_cache_size",
           get_write_cache_size());
}

int ObSSMacroCacheMgr::get_meta_by_bucket(
    const int64_t bucket_pos,
    ObIArray<ObSSMacroCacheMetaHandle> &meta_handles)
{
  int ret = OB_SUCCESS;
  SSMacroCacheMetaMap::bucket_iterator bucket_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY((bucket_pos < 0) || (bucket_pos >= meta_map_.bucket_count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket pos", KR(ret), K(bucket_pos));
  } else if (OB_FAIL(meta_map_.get_bucket_iterator(bucket_pos, bucket_iter))) {
    LOG_WARN("fail to get bucket iterator", KR(ret), K(bucket_pos));
  } else {
    SSMacroCacheMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
    SSMacroCacheMetaMap::hashtable::readlocker locker(blk.lock());
    SSMacroCacheMetaMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
    while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
      if (OB_FAIL(meta_handles.push_back(node_iter->second))) {
        LOG_WARN("fail to push back meta handle", KR(ret), K(node_iter->first));
      }
      node_iter++;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
