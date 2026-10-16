/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_hot_tablet_old_sstable_evict_task.h"
#include "lib/thread/thread_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/ob_bucket_num_helper.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;


/***************************** ObSSHotTabletOldSSTableEvictCheckFn *********************************/
int ObSSHotTabletOldSSTableEvictCheckFn::operator()(
    const HashMapPair<ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &entry)
{
  int ret = OB_SUCCESS;
  bool need_skip = false;
  ObTenantMetaMemMgr *t3m = nullptr;
  const ObTabletID &tablet_id = entry.first;
  const ObSSHotTabletOldSSTableEvictInfo &info = entry.second;
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  int64_t current_tablet_version = -1;
  int64_t tablet_last_gc_version = -1;
  int64_t current_tablet_transfer_seq = -1;
  uintptr_t tablet_fingerprint = 0;
  bool allow_tablet_version_gc = false;
  bool is_transfer_out_deleted = false;
  int64_t unused_reorganization_scn = 0;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hot tablet old sstable evict info is not valid", KR(ret), K(tablet_id), K(info));
  } else if (OB_ISNULL(t3m = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant meta mem mgr should not be null", KR(ret));
  } else if (OB_FAIL(get_tablet_ls_id(MTL_ID(), tablet_id.id(), ls_id))) {
    LOG_WARN("fail to get tablet ls id", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(ls_id != info.ls_id_.id())) {
    LOG_INFO("tablet ls_id is not match, skip this tablet", K(tablet_id), K(info), K(ls_id));
    need_skip = true;
  } else if (OB_FAIL(t3m->get_current_version_for_tablet(info.ls_id_, tablet_id,
                     current_tablet_version, tablet_last_gc_version, current_tablet_transfer_seq,
                     tablet_fingerprint, allow_tablet_version_gc, is_transfer_out_deleted,
                     unused_reorganization_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. skip this tablet
      need_skip = true;
    } else {
      LOG_WARN("fail to get current version for tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_UNLIKELY(current_tablet_transfer_seq != info.transfer_seq_)) {
    LOG_INFO("tablet transfer seq is not match, skip this tablet", K(tablet_id), K(info),
             K(current_tablet_transfer_seq));
    need_skip = true;
  } else if (allow_tablet_version_gc) { // all previous tablet versions' ref_cnt are reduced to 0
    if (OB_FAIL(allow_evict_tablet_map_.set_refactored(tablet_id, info))) {
      // here won't return OB_HASH_EXIST, as tablet_ids come from another HashMap
      // (ObSSHotTabletOldSSTableEvictTask::tablet_info_map_) whose key is tablet_id
      LOG_WARN("fail to set allow evict tabelt map", KR(ret), K(tablet_id), K(info));
    }
  }

  if (OB_SUCC(ret) && need_skip) {
    if (OB_FAIL(skip_tablet_set_.set_refactored(tablet_id))) {
      LOG_WARN("fail to set skip tablet set", KR(ret), K(tablet_id));
    }
  }
  return ret;
}


/***************************** ObFindExpiredTabletFn *********************************/
int ObSSHotTabletOldSSTableEvictTask::ObFindExpiredTabletFn::operator()(
    const HashMapPair<ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &entry)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = entry.first;
  const ObSSHotTabletOldSSTableEvictInfo &info = entry.second;
  if (ObTimeUtility::current_time_us() - info.timestamp_us_ > EXPIRED_TIME_US) {
    if (OB_FAIL(expired_tablet_set_.set_refactored(tablet_id))) {
      LOG_WARN("fail to set expired tablet set", KR(ret), K(tablet_id), K(info));
    }
  }
  return ret;
}


/***************************** ObSSHotTabletOldSSTableEvictTask *********************************/
ObSSHotTabletOldSSTableEvictTask::ObSSHotTabletOldSSTableEvictTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(nullptr), tablet_info_map_(),
    allow_evict_tablet_map_(), skip_tablet_set_(), last_clear_timestamp_us_(0)
{
}

ObSSHotTabletOldSSTableEvictTask::~ObSSHotTabletOldSSTableEvictTask()
{
  destroy();
}

int ObSSHotTabletOldSSTableEvictTask::init(ObSSMacroCacheMgr *macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::TABLET_TYPE);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init hot tablet old sstable evict task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr));
  } else if (OB_FAIL(tablet_info_map_.create(BUCKET_NUM, ObMemAttr(MTL_ID(), "HotTabOldSSTTab")))) {
    LOG_WARN("fail to create tablet info map", KR(ret));
  } else if (OB_FAIL(allow_evict_tablet_map_.create(BUCKET_NUM, ObMemAttr(MTL_ID(), "HotTabOldSSTAlw")))) {
    LOG_WARN("fail to create allow evict tablet map", KR(ret));
  } else if (OB_FAIL(skip_tablet_set_.create(BUCKET_NUM, ObMemAttr(MTL_ID(), "HotTabOldSSTSkp")))) {
    LOG_WARN("fail to create skip tablet set", KR(ret));
  } else {
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init hot tablet old sstable evict task", KR(ret));
  return ret;
}

int ObSSHotTabletOldSSTableEvictTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hot tablet old sstable evict task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule hot tablet old sstable evict task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule hot tablet old sstable evict task", K(tg_id));
  }
  return ret;
}

void ObSSHotTabletOldSSTableEvictTask::destroy()
{
  if (IS_INIT) {
    skip_tablet_set_.destroy();
    allow_evict_tablet_map_.destroy();
    tablet_info_map_.destroy();
    macro_cache_mgr_ = nullptr;
    is_inited_ = false;
  }
  FLOG_INFO("finish to destroy hot tablet old sstable evict task");
}

void ObSSHotTabletOldSSTableEvictTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (FALSE_IT(try_clear_expired_tablet())) {
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (!macro_cache_mgr_->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do hot tablet old sstable evict task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_work())) {
    LOG_WARN("fail to do work", KR(ret));
  }
}

int ObSSHotTabletOldSSTableEvictTask::push_old_sstable_evict_task(
    const ObTabletID &tablet_id,
    const ObSSHotTabletOldSSTableEvictInfo &evict_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_FAIL(tablet_info_map_.set_refactored(tablet_id, evict_info))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("already exist in tablet info map", KR(ret), K(tablet_id), K(evict_info));
    } else {
      LOG_WARN("fail to set tablet info map", KR(ret), K(tablet_id), K(evict_info));
    }
  }
  return ret;
}

int ObSSHotTabletOldSSTableEvictTask::do_work()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  allow_evict_tablet_map_.reuse();
  skip_tablet_set_.reuse();
  ObSSHotTabletOldSSTableEvictCheckFn check_fn(allow_evict_tablet_map_, skip_tablet_set_);
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (tablet_info_map_.empty()) {
    LOG_TRACE("no hot tablet old sstable evict info");
  } else if (OB_FAIL(tablet_info_map_.foreach_refactored(check_fn))) {
    LOG_WARN("fail to foreach tablet info map", KR(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", KR(ret));
  } else if (allow_evict_tablet_map_.size() > 0) {
    const int64_t total_start_us = ObTimeUtility::current_time_us();
    int64_t total_unused_blk_cnt = 0; // for print log
    // here create last_major_block_set only once, when there exists allow evict tablets
    ObHashSet<MacroBlockId> last_major_block_set;
    const int64_t BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::MACRO_TYPE);
    if (OB_FAIL(last_major_block_set.create(BUCKET_NUM, ObMemAttr(MTL_ID(), "HotTabOldSSTBlk")))) {
      LOG_WARN("fail to create last major block set", KR(ret));
    }
    ObHashMap<ObTabletID, ObSSHotTabletOldSSTableEvictInfo>::const_iterator iter;
    // Note: need check is_stopped_ flag, so as to avoid blocking unit gc
    for (iter = allow_evict_tablet_map_.begin(); OB_SUCC(ret) && !is_stopped_
         && (iter != allow_evict_tablet_map_.end()) ; ++iter) {
      const int64_t start_us = ObTimeUtility::current_time_us();
      int64_t unused_blk_cnt = 0; // for print log
      const ObTabletID &tablet_id = iter->first;
      const ObSSHotTabletOldSSTableEvictInfo &info = iter->second;
      ObTraceIDGuard trace_guard(info.trace_id_);
      bool need_skip = false;
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      ObLSTabletService *ls_tablet_svr = nullptr;
      ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
      const ObTabletTableStore *table_store = nullptr;
      if (OB_FAIL(ls_service->get_ls(info.ls_id_, ls_handle, ObLSGetMod::SS_MACRO_CACHE_MOD))) {
        LOG_WARN("fail to get ls", KR(ret), K(info));
      } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls handle is invalid", KR(ret), K(info));
      } else if (OB_ISNULL(ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls tablet service is null", KR(ret), K(info));
      } else if (OB_FAIL(ls_tablet_svr->get_tablet(tablet_id, tablet_handle, 0/*timeout_us*/,
                                                   ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_skip = true;
          LOG_INFO("tablet is already not exist, skip this tablet", KR(ret), K(tablet_id), K(info));
        } else {
          LOG_WARN("fail to get tablet", KR(ret), K(tablet_id), K(info));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle is invalid", KR(ret), K(info));
      } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
        LOG_WARN("fail to fetch table store", KR(ret), K(info), K(tablet_id));
      } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
        LOG_WARN("fail to get table store", KR(ret), K(info), K(tablet_id));
      } else if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table store is null", KR(ret), K(info), K(tablet_id));
      } else {
        const ObSSTableArray &major_sstables = table_store->get_major_sstables();
        if (OB_FAIL(evict_old_sstable(tablet_id, major_sstables, last_major_block_set, unused_blk_cnt, need_skip))) {
          LOG_WARN("fail to evict old sstable", KR(ret), K(info), K(tablet_id), K(unused_blk_cnt), K(need_skip));
        } else {
          total_unused_blk_cnt += unused_blk_cnt;
        }
      }
      if (OB_SUCC(ret) && need_skip) {
        if (OB_FAIL(skip_tablet_set_.set_refactored(tablet_id))) {
          LOG_WARN("fail to set skip tablet set", KR(ret), K(tablet_id));
        }
      }
      if (TC_REACH_TIME_INTERVAL(1 * 1000L * 1000L)) { // print log every 1s, avoid too much log
        LOG_INFO("finish to evict old sstable", KR(ret), K(tablet_id), K(info), K(need_skip),
                 K(unused_blk_cnt), "cost_us", ObTimeUtility::current_time_us() - start_us);
      }
      LOG_TRACE("finish to evict old sstable for tablet", KR(ret), K(tablet_id), K(info), K(need_skip),
                K(unused_blk_cnt), "cost_us", ObTimeUtility::current_time_us() - start_us);
    }
    LOG_INFO("finish to evict old sstable for tablets", KR(ret), "tablet_cnt", allow_evict_tablet_map_.size(),
             K(total_unused_blk_cnt), "total_cost_us", ObTimeUtility::current_time_us() - total_start_us);
  }

  erase_skipped_tablet(); // erase skipped tablet, regardless of ret above

  return ret;
}

int ObSSHotTabletOldSSTableEvictTask::evict_old_sstable(
    const ObTabletID &tablet_id,
    const ObSSTableArray &major_sstables,
    ObHashSet<MacroBlockId> &last_major_block_set,
    int64_t &unused_blk_cnt,
    bool &need_skip)
{
  int ret = OB_SUCCESS;
  const int64_t major_sstables_cnt = major_sstables.count();
  if (major_sstables_cnt >= 2) {
    int64_t last_major_snapshot_version = -1;
    if (OB_FAIL(build_last_major_block_set(major_sstables, last_major_snapshot_version, last_major_block_set))) {
      LOG_WARN("fail to build last major block set", KR(ret));
    } else {
      ObSEArray<MacroBlockId, 64> unused_data_blocks;
      unused_data_blocks.set_attr(ObMemAttr(MTL_ID(), "HotTabOldSSTBlk"));
      for (int64_t i = 0; OB_SUCC(ret) && (i < (major_sstables_cnt - 1)); ++i) {
        unused_data_blocks.reuse();
        int64_t cur_major_snapshot_version = -1;
        ObSSTable *cur_major_sstable = static_cast<ObSSTable *>(major_sstables.at(i));
        if (OB_ISNULL(cur_major_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur major sstable is null", KR(ret));
        } else if (FALSE_IT(cur_major_snapshot_version = cur_major_sstable->get_snapshot_version())) {
        } else if (OB_UNLIKELY(cur_major_snapshot_version >= last_major_snapshot_version)) { // defence
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected snapshot version", KR(ret), K(cur_major_snapshot_version), K(last_major_snapshot_version));
        } else {
          if (OB_FAIL(find_unused_data_blocks(*cur_major_sstable, last_major_block_set, unused_data_blocks))) {
            LOG_WARN("fail to find unused data blocks", KR(ret));
          } else if (FALSE_IT(unused_blk_cnt = unused_data_blocks.count())) {
          } else if (unused_blk_cnt > 0) {
            if (OB_ISNULL(macro_cache_mgr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("macro cache mgr should not be null", KR(ret));
            } else if (OB_FAIL(macro_cache_mgr_->modify_macro_cache_type(unused_data_blocks,
                       ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObSSMacroCacheType::MACRO_BLOCK))) {
              LOG_WARN("fail to convert to auto", KR(ret), "unsed_data_blocks_cnt", unused_data_blocks.count());
            }
          }
        }
      }
      // successfully evict old major sstables, erase from tablet info map
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_info_map_.erase_refactored(tablet_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("already erased from tablet info map", KR(ret), K(tablet_id));
          } else {
            LOG_WARN("fail to erase from tablet info map", KR(ret), K(tablet_id));
          }
        }
      }
    }
  } else { // less than 2 major sstables
    need_skip = true;
    LOG_INFO("already less than two major sstables, skip this tablet", KR(ret), K(tablet_id));
  }
  return ret;
}

void ObSSHotTabletOldSSTableEvictTask::erase_skipped_tablet()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (skip_tablet_set_.size() > 0) {
    ObHashSet<ObTabletID>::const_iterator iter;
    for (iter = skip_tablet_set_.begin(); iter != skip_tablet_set_.end(); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      if (OB_TMP_FAIL(tablet_info_map_.erase_refactored(tablet_id))) {
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          tmp_ret = OB_SUCCESS;
          LOG_INFO("already erased from tablet info map", K(tablet_id));
        } else {
          LOG_WARN("fail to erase from tablet info map", KR(tmp_ret), K(tablet_id));
        }
      }
    }
  }
}

// Note: only obtain SHARED_MAJOR_DATA_MACRO, and ignore SHARED_MAJOR_META_MACRO (which occupies
// a little amount of space compared with SHARED_MAJOR_DATA_MACRO)
int ObSSHotTabletOldSSTableEvictTask::build_last_major_block_set(
    const ObSSTableArray &major_sstables,
    int64_t &last_major_snapshot_version,
    ObHashSet<MacroBlockId> &last_major_block_set)
{
  int ret = OB_SUCCESS;
  last_major_block_set.reuse();
  ObSSTableMetaHandle last_major_meta_handle;
  ObSSTable *last_major_sstable = nullptr;
  if (OB_ISNULL(last_major_sstable = static_cast<ObSSTable *>(major_sstables.get_boundary_table(true/*is_last*/)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get last major sstable", KR(ret));
  } else if (FALSE_IT(last_major_snapshot_version = last_major_sstable->get_snapshot_version())) {
  } else if (OB_FAIL(last_major_sstable->get_meta(last_major_meta_handle))) {
    LOG_WARN("fail to get last major sstable meta", KR(ret));
  } else {
    const ObSSTableMacroInfo &last_major_macro_info = last_major_meta_handle.get_sstable_meta().get_macro_info();
    ObMacroIdIterator last_major_data_block_iter;
    if (OB_FAIL(last_major_macro_info.get_data_block_iter(last_major_data_block_iter))) {
      LOG_WARN("fail to get last major data block iterator", KR(ret));
    } else {
      bool is_completed = false;
      MacroBlockId block_id;
      while (OB_SUCC(ret) && !is_completed) {
        if (OB_FAIL(last_major_data_block_iter.get_next_macro_id(block_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_completed = true;
          } else {
            LOG_WARN("fail to get next macro id", KR(ret));
          }
        } else if (SSObjUtil::is_major(block_id) && block_id.is_data()) {
          if (OB_FAIL(last_major_block_set.set_refactored(block_id))) {
            LOG_WARN("fail to set last major block set", KR(ret), K(block_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSHotTabletOldSSTableEvictTask::find_unused_data_blocks(
    const ObSSTable &sstable,
    const ObHashSet<MacroBlockId> &last_major_block_set,
    ObIArray<MacroBlockId> &unused_data_blocks)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("fail to get sstable meta", KR(ret));
  } else {
    const ObSSTableMacroInfo &macro_info = meta_handle.get_sstable_meta().get_macro_info();
    ObMacroIdIterator data_block_iter;
    if (OB_FAIL(macro_info.get_data_block_iter(data_block_iter))) {
      LOG_WARN("fail to get data block iterator", KR(ret));
    } else {
      bool is_completed = false;
      MacroBlockId block_id;
      while (OB_SUCC(ret) && !is_completed) {
        if (OB_FAIL(data_block_iter.get_next_macro_id(block_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_completed = true;
          } else {
            LOG_WARN("fail to get next macro id", KR(ret));
          }
        } else if (SSObjUtil::is_major(block_id) && block_id.is_data()) {
          if (OB_FAIL(last_major_block_set.exist_refactored(block_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              if (OB_FAIL(unused_data_blocks.push_back(block_id))) {
                LOG_WARN("fail to push back unused data blocks", KR(ret), K(block_id));
              }
            } else {
              LOG_WARN("fail to check whether block is in last major block set", KR(ret), K(block_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObSSHotTabletOldSSTableEvictTask::try_clear_expired_tablet()
{
  int ret = OB_SUCCESS;
  ObHashSet<ObTabletID> expired_tablet_set;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if ((ObTimeUtility::current_time_us() - last_clear_timestamp_us_) < EXPIRED_TIME_US) {
    // do nothing
  } else if (tablet_info_map_.empty()) {
    LOG_TRACE("no hot tablet old sstable evict info");
  } else if (OB_FAIL(expired_tablet_set.create(tablet_info_map_.bucket_count(), ObMemAttr(MTL_ID(), "HotTabOldSSTExp")))) {
    LOG_WARN("fail to create expired tablet set", KR(ret));
  } else {
    ObFindExpiredTabletFn find_expired_tablet_fn(expired_tablet_set);
    if (OB_FAIL(tablet_info_map_.foreach_refactored(find_expired_tablet_fn))) {
      LOG_WARN("fail to foreach tablet info map", KR(ret));
    } else if (expired_tablet_set.size() > 0) {
      ObHashSet<ObTabletID>::const_iterator iter;
      for (iter = expired_tablet_set.begin(); OB_SUCC(ret) && (iter != expired_tablet_set.end()); ++iter) {
        const ObTabletID &tablet_id = iter->first;
        if (OB_FAIL(tablet_info_map_.erase_refactored(tablet_id))) {
          LOG_WARN("fail to erase from tablet info map", KR(ret), K(tablet_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      last_clear_timestamp_us_ = ObTimeUtility::current_time_us();
      if (expired_tablet_set.size() > 0) {
        LOG_INFO("succ to clear expired tablets", "expired_tablet_cnt", expired_tablet_set.size());
      }
    }
  }
}

} // namespace storage
} // namespace oceanbase
