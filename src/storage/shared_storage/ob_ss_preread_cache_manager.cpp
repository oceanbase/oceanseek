/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_ss_preread_cache_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

bool is_support_preread_meta(const blocksstable::MacroBlockId &file_id)
{
  ObStorageObjectType object_type = file_id.storage_object_type();
  return file_id.is_meta();
}

int ObPutPrereadMapCallback::operator()(const PrereadMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_manager = nullptr;
  ObMemAttr attr(MTL_ID(), "PrereadMeta");
  ObPreReadFileMeta *preread_file_meta = nullptr;
  if (OB_ISNULL(preread_file_meta = OB_NEW(ObPreReadFileMeta, attr, macro_id_,
                                           effective_tablet_id_, GET_FUNC_TYPE()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(macro_id), K_(effective_tablet_id));
  } else if (OB_ISNULL(tnt_file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret));
  } else if (OB_FAIL(tnt_file_manager->get_preread_cache_mgr().preread_queue_.push(preread_file_meta))) {
    LOG_WARN("fail to push", KR(ret), KPC(preread_file_meta));
  }

  // free memory on fail
  if (OB_FAIL(ret) && OB_NOT_NULL(preread_file_meta)) {
    OB_DELETE(ObPreReadFileMeta, attr, preread_file_meta);
  }
  return ret;
}


int ObUpdatePrereadStatusCallback::operator()(PrereadMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObPrereadNode &preread_node = entry.second;
  if (ObPrereadNodeStatus::FAKE == preread_node.node_status_) {
    preread_node.file_length_ = file_length_;
    preread_node.node_status_ = ObPrereadNodeStatus::NORMAL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node status", KR(ret), K(preread_node));
  }
  return ret;
}


int ObUpdateNeedPrereadCallback::operator()(PrereadMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObPrereadNode &preread_node = entry.second;
  preread_node.is_need_preread_ = is_need_preread_;
  return ret;
}


ObPrereadCacheManager::ObPrereadCacheManager()
  : is_inited_(false),
    is_stopped_(false),
    file_manager_(nullptr),
    preread_map_(),
    tg_id_(INVALID_TG_ID),
    preread_task_(),
    preread_map_clear_task_(is_stopped_),
    preread_queue_()
{
}

ObPrereadCacheManager::~ObPrereadCacheManager() { destroy(); }

int ObPrereadCacheManager::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "PreReadLRU_Map");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("preread cache manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_manager));
  } else if (OB_FAIL(preread_map_.create(ObBaseFileManager::OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create map", KR(ret));
  } else if (OB_FAIL(preread_task_.init(this))) {
    LOG_WARN("fail to init preread task", KR(ret));
  } else if (OB_FAIL(preread_map_clear_task_.init(this))) {
    LOG_WARN("fail to init preread map clear task", KR(ret));
  } else {
    file_manager_ = file_manager;
    is_inited_ = true;
  }
  return ret;
}

int ObPrereadCacheManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFilePrereadTimer, tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
  } else if (OB_FAIL(preread_task_.start(tg_id_))) {
    LOG_WARN("fail to start preread task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(preread_map_clear_task_.start(tg_id_))) {
    LOG_WARN("fail to start preread map clear task", KR(ret), K_(tg_id));
  }
  return ret;
}

void ObPrereadCacheManager::destroy()
{
  int ret = OB_SUCCESS;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  const int64_t queue_size = preread_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); ++i) {
    ObLink *ptr = nullptr;
    ObPreReadFileMeta *preread_file_meta = nullptr;
    if (OB_FAIL(preread_queue_.pop(ptr))) {
      LOG_WARN("fail to pop segment_file", KR(ret), K(i), K(queue_size));
    } else {
      preread_file_meta = static_cast<ObPreReadFileMeta *>(ptr);
      OB_DELETE(ObPreReadFileMeta, attr, preread_file_meta);
    }
  }
  preread_map_clear_task_.destroy();
  preread_task_.destroy();
  preread_map_.destroy();
  file_manager_ = nullptr;
  is_stopped_ = true;
  is_inited_ = false;
}

void ObPrereadCacheManager::stop()
{
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  preread_task_.stop();
}

void ObPrereadCacheManager::wait()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  preread_task_.wait();
}

int ObPrereadCacheManager::push_to_preread_queue(
    const MacroBlockId &file_id,
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTabletID cur_effective_tablet_id(effective_tablet_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else if (!cur_effective_tablet_id.is_valid() && SSObjUtil::has_effective_tablet_id(file_id)) {
    // Scenario: io triggered in background thread (e.g., MAJOR, MINOR, ATTACH, GC, etc.) has not set
    // effective_tablet_id. we take tablet_id in MacroBlockId as effective_tablet_id for this case.
    // Influence: if this tablet has triggered split, then tablet_id in MacroBlockId may be
    // in-consistent with effective_tablet_id. this may lead to ls migration prewarm not 100% accurate.
    // Besides: if this macro is accessed by foreground thread in the future, it's
    // effective_tablet_id will be corrected.
    uint64_t tmp_tablet_id = ObTabletID::INVALID_TABLET_ID;
    if (OB_FAIL(SSObjUtil::get_effective_tablet_id(file_id, tmp_tablet_id))) {
      LOG_WARN("fail to get effective tablet id", KR(ret), K(file_id));
    } else if (OB_UNLIKELY(ObTabletID::INVALID_TABLET_ID == tmp_tablet_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet id", KR(ret), K(file_id));
    } else {
      cur_effective_tablet_id = ObTabletID(tmp_tablet_id);
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((file_id.is_private_data_or_meta() || file_id.is_shared_data_block_or_meta_block()) &&
             !cur_effective_tablet_id.is_valid()) {
    // do nothing, and do not return errno.
    LOG_INFO("do not preread macro block without effective tablet id", K(file_id), K(cur_effective_tablet_id));
  } else if (!is_read_cache_support_object_type(file_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object type not support push to lru list", KR(ret), K(file_id));
  } else if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(tenant_kvcache_mgr->check_exist(file_id, is_exist))) {
    LOG_WARN("fail to check exist", KR(ret), K(file_id));
  } else if (is_exist) {
    // already exist in macro cache, no need to preread
  } else if (OB_FAIL(macro_cache_mgr->exist(file_id, is_exist))) {
    LOG_WARN("fail to check exist", KR(ret), K(file_id));
  } else if (is_exist) {
    // already exist in macro cache, no need to preread
  } else {
    ObPrereadNode preread_node(ObPrereadNodeStatus::FAKE, 0/*file_length*/);
    ObPutPrereadMapCallback callback(file_id, cur_effective_tablet_id);
    if (OB_FAIL(preread_map_.set_refactored(file_id, preread_node, 0/*overwrite*/, 0/*broadcast*/,
                                            0/*overwrite_key*/, &callback))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret. may be concurrent preread
      } else {
        LOG_WARN("fail to set refactored", KR(ret), K(file_id), K(preread_node));
      }
    } else {
      LOG_INFO("succ to push to preread queue", K(file_id), K(cur_effective_tablet_id));
    }
  }
  return ret;
}

// Note: preread tmp file must be sealed and without TmpFileMeta. hence, there is no need to set
// is_in_local = false in TmpFileMeta's lock.
int ObPrereadCacheManager::evict_preread_node(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(file_id))) {
    LOG_WARN("fail to delete local file", KR(ret), K(file_id));
  } else {
    // delete it from macro block KVCACHE if exists
    ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
    if (OB_ISNULL(
        tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant macro block KVCACHE mgr is null", KR(tmp_ret));
    } else if (OB_TMP_FAIL(
        tenant_kvcache_mgr->erase_macro_block(file_id))) {
      if (OB_SERVICE_STOPPED == tmp_ret) {
        tmp_ret = OB_SUCCESS;
        LOG_TRACE("no need to erase macro block from KVCACHE", K(tmp_ret), K(file_id));
      } else {
        LOG_WARN("fail to erase macro_block", KR(tmp_ret), K(file_id));
      }
    }
    LOG_INFO("succ to evict preread node", K(tmp_ret), K(file_id));
  }
  return ret;
}

int ObPrereadCacheManager::remove_preread_node(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else if (OB_FAIL(preread_map_.erase_refactored(file_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // maybe has already been gc
    } else {
      LOG_WARN("fail to erase map node", KR(ret), K(file_id));
    }
  } else {
    LOG_INFO("succ to remove preread node", K(file_id));
  }
  return ret;
}

int ObPrereadCacheManager::refresh_preread_node_if_need(const ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const MacroBlockId file_id = read_info.macro_block_id_;
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info));
  // only tmp_file and preread macro need refresh preread node
  } else if ((SSObjUtil::is_tmp_file(file_id))) {
    ObPrereadNode preread_node;
    if (OB_FAIL(preread_map_.get_refactored(file_id, preread_node))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
      } else {
        LOG_WARN("fail to get refactored", KR(ret), K(file_id));
      }
    } else if (ObPrereadNodeStatus::NORMAL == preread_node.node_status_) {
      if ((read_info.offset_ + read_info.size_) > preread_node.file_length_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(preread_node), K(read_info));
      } else if ((read_info.offset_ + read_info.size_) == preread_node.file_length_) {
        // if evict preread node failed, it doesn't matter.
        // because this preread cache will be deleted by macro cache evict or gc
        if (OB_FAIL(evict_preread_node(file_id))) {
          LOG_WARN("fail to evict preread node", KR(ret), K(file_id), K(read_info));
        }
      }
    } else if (ObPrereadNodeStatus::FAKE == preread_node.node_status_) {
      // when read more than half of this macro, set is_need_preread to false. this means
      // this macro is no need to preread any more.
      if (is_need_cancel_preread(read_info)) {
        if (OB_FAIL(set_need_preread(file_id, false/*is_need_preread*/))) {
          LOG_WARN("fail to set need preread", KR(ret), K(file_id), K(read_info));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected preread node status", KR(ret), K(preread_node));
    }
  }
  return ret;
}

bool ObPrereadCacheManager::is_need_cancel_preread(const ObStorageObjectReadInfo &read_info)
{
  // if preread node status is FAKE, tmp_file and major_macro Node has been read half of segment file, do not need preread
  bool is_cancel_preread = ((read_info.offset_ + read_info.size_) >= OB_DEFAULT_MACRO_BLOCK_SIZE / 2);
  return is_cancel_preread;
}

int ObPrereadCacheManager::update_to_normal_status(const MacroBlockId &file_id, const int64_t file_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid() || file_length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id), K(file_length));
  } else {
    // 1. TMP_FILE: reserve node here, which will be evicted when read to the end of this TMP_FILE
    if (SSObjUtil::is_tmp_file(file_id)) {
      ObUpdatePrereadStatusCallback callback(file_length);
      if (OB_FAIL(preread_map_.atomic_refactored(file_id, callback))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // ignore ret
        } else {
          LOG_WARN("fail to atomic refactored preread map", KR(ret), K(file_id), K(callback));
        }
      } else if (OB_UNLIKELY(OB_SUCCESS != (ret = callback.get_ret()))) {
        LOG_WARN("fail to execute update preread map callback", KR(ret), K(file_id), K(callback));
      }
    } else {
      // 2. non-TMP_FILE: no need to reserve node here, which will be evicted by MacroCacheMgr's LRU
      if (OB_FAIL(preread_map_.erase_refactored(file_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // ignore ret. maybe has been gc
        } else {
          LOG_WARN("fail to erase refactored", KR(ret), K(file_id));
        }
      }
    }
  }
  return ret;
}

int ObPrereadCacheManager::is_file_id_need_preread(const MacroBlockId &file_id, bool &is_need_preread)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObPrereadNode preread_node;
    ret = preread_map_.get_refactored(file_id, preread_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_need_preread = false;
    } else if (preread_node.node_status_ == ObPrereadNodeStatus::FAKE) {
      is_need_preread = preread_node.is_need_preread_;
      // if preread node is fake and !is_need_preread, remove preread node.
      if (!is_need_preread) {
        if (OB_FAIL(remove_preread_node(file_id))) {
          LOG_WARN("fail to remove preread node", KR(ret), K(file_id));
        }
      }
    } else {
      is_need_preread = false;
    }
  }
  return ret;
}

int ObPrereadCacheManager::is_exist_in_preread_map(const MacroBlockId &file_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObPrereadNode preread_node;
    ret = preread_map_.get_refactored(file_id, preread_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      is_exist = true;
    }
  }
  return ret;
}

// When file_id has been read whole, set is_need_preread is false, means the file_id do not need preread
int ObPrereadCacheManager::set_need_preread(const blocksstable::MacroBlockId &file_id, const bool is_need_preread)
{
  int ret = OB_SUCCESS;
  ObUpdateNeedPrereadCallback callback(is_need_preread);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else if (OB_FAIL(preread_map_.atomic_refactored(file_id, callback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to atomic refactored preread map", KR(ret), K(file_id), K(callback));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = callback.get_ret()))) {
    LOG_WARN("fail to execute update preread map callback", KR(ret), K(file_id), K(callback));
  }
  return ret;
}

bool ObPrereadCacheManager::is_read_cache_support_object_type(const MacroBlockId &file_id)
{
  return (file_id.is_data() || is_support_preread_meta(file_id) || SSObjUtil::is_tmp_file(file_id));
}

int ObPrereadCacheManager::batch_erase_preread_entry(const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  // inner function, no need to judge is_inited_ again
  const int64_t macro_id_cnt = macro_ids.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_cnt); ++i) {
    if (OB_FAIL(preread_map_.erase_refactored(macro_ids.at(i)))) {
      LOG_WARN("fail to erase refactored", KR(ret), "macro_id", macro_ids.at(i));
    }
  }
  return ret;
}

int ObPrereadCacheManager::do_preread_map_clear_work()
{
  int ret = OB_SUCCESS;
  // batch process to avoid occupy too much memory
  const int64_t BUCKET_BATCH_CNT = 1024;
  int64_t bucket_cnt = 0;
  ObSEArray<MacroBlockId, 128> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "PrereadMapClear"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache mgr is not inited", KR(ret));
  } else {
    PrereadMap::bucket_iterator bucket_iter = preread_map_.bucket_begin();
    while (OB_SUCC(ret) && (bucket_iter != preread_map_.bucket_end())) {
      {
        PrereadMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        PrereadMap::hashtable::readlocker locker(blk.lock());
        PrereadMap::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
          const MacroBlockId &cur_macro_id = node_iter->first;
          const ObPrereadNode &cur_preread_entry = node_iter->second;
          if (ObTimeUtility::current_time_us() - cur_preread_entry.timestamp_us_ > PREREAD_ENTRY_EXPIRATION_TIME_US) {
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
        if (OB_FAIL(batch_erase_preread_entry(macro_ids))) {
          LOG_WARN("fail to batch erase preread entry", KR(ret), K(macro_ids));
        }
        macro_ids.reuse();
        bucket_cnt = 0;
      }
    }
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(batch_erase_preread_entry(macro_ids))) {
        LOG_WARN("fail to batch erase preread entry", KR(ret), K(macro_ids));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
