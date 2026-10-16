/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_hot_tablet_macro_cache_gc_task.h"
#include "lib/thread/thread_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "share/ob_bucket_num_helper.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

ObSSHotTabletMacroCacheGCTask::ObSSHotTabletMacroCacheGCTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(nullptr), last_gc_time_us_(0)
{
}

ObSSHotTabletMacroCacheGCTask::~ObSSHotTabletMacroCacheGCTask()
{
  destroy();
}

int ObSSHotTabletMacroCacheGCTask::init(ObSSMacroCacheMgr *macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init hot tablet macro cache gc task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr));
  } else {
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init hot tablet macro cache gc task", KR(ret));
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hot tablet macro cache gc task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule hot tablet macro cache gc task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule hot tablet macro cache gc task", K(tg_id));
  }
  return ret;
}

void ObSSHotTabletMacroCacheGCTask::destroy()
{
  last_gc_time_us_ = 0;
}

void ObSSHotTabletMacroCacheGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t current_time_us = ObTimeUtility::fast_current_time_us();
  int64_t gc_interval_us = 1_day;
  int64_t idle_time_us = 1_day;
  bool convert_to_auto = false;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (!macro_cache_mgr_->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do hot tablet macro cache gc task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
          "tenant config is invalid, use default hot tablet macro cache gc interval",
          K(tenant_id), K(gc_interval_us), K(idle_time_us));
    } else {
      gc_interval_us = tenant_config->_hot_macro_cache_gc_interval;
      idle_time_us = tenant_config->_hot_macro_cache_gc_idle_time;
      convert_to_auto = tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto;
    }

    if (0 == last_gc_time_us_) {
      // 首次调度只记录计时基准，避免租户启动后立即触发 HOT macro cache GC。
      last_gc_time_us_ = current_time_us;
    } else if (current_time_us < last_gc_time_us_ + gc_interval_us) {
      LOG_TRACE("hot tablet macro cache gc is not due",
          K(gc_interval_us), K(idle_time_us), K(current_time_us), K(last_gc_time_us_));
    } else if (OB_FAIL(gc_hot_tablet_macro_cache(idle_time_us, convert_to_auto))) {
      const int64_t elapsed_time_us = current_time_us - last_gc_time_us_;
      if (elapsed_time_us > 2 * gc_interval_us) {
        LOG_ERROR("hot tablet macro cache gc has not succeeded for over twice the configured interval",
            KR(ret), K(tenant_id), K(gc_interval_us), K(idle_time_us), K(convert_to_auto),
            K(elapsed_time_us), K(last_gc_time_us_));
      } else {
        LOG_WARN("fail to gc hot tablet macro cache",
            KR(ret), K(gc_interval_us), K(idle_time_us), K(convert_to_auto), K(last_gc_time_us_));
      }
    } else {
      last_gc_time_us_ = current_time_us;
    }
  }
}

int ObSSHotTabletMacroCacheGCTask::gc_macro_hot_cache(const ObHashMap<ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
                                                      const ObHashMap<ObTabletID, ObLSID> &macro_hot_tablet_map)
{
  int ret = OB_SUCCESS;
  int64_t batch_count = 0;
  int64_t processed_tablet_count = 0;
  ObMemAttr attr(MTL_ID(), "MacroHotCacheGC");
  ObHashMap<ObTabletID, ObLSID> batch_map;
  ObHashSet<MacroBlockId> macro_block_set;
  const int64_t TABLET_BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::TABLET_TYPE);
  const int64_t MACRO_BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::MACRO_TYPE);
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (OB_FAIL(batch_map.create(TABLET_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create batch map", KR(ret));
  } else if (OB_FAIL(macro_block_set.create(MACRO_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create macro block set", KR(ret));
  } else {
    ObHashMap<ObTabletID, ObLSID>::const_iterator cur_iter = macro_hot_tablet_map.begin();
    const ObHashMap<ObTabletID, ObLSID>::const_iterator end_iter = macro_hot_tablet_map.end();
    while (OB_SUCC(ret) && (cur_iter != end_iter)) {
      const ObTabletID &tablet_id = cur_iter->first;
      const ObLSID &ls_id = cur_iter->second;
      bool is_get = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(get_macro_block_set_for_single_tablet(ls_id_epoch_map, tablet_id, ls_id, macro_block_set, is_get))) {
        LOG_WARN("fail to get macro block set for single tablet", KR(tmp_ret), K(tablet_id), K(ls_id));
        // ignore ret. skip this tablet
      } else if (is_get) {
        // add valid tablet to batch map
        if (OB_FAIL(batch_map.set_refactored(tablet_id, ls_id))) {
          LOG_WARN("fail to set tablet to batch map", KR(ret), K(tablet_id), K(ls_id));
        } else {
          processed_tablet_count++;
          // if macro_block_set size is greater than or equal to MACRO_BUCKET_NUM, trigger a gc
          if (macro_block_set.size() >= MACRO_BUCKET_NUM) {
            ++batch_count;
            if (OB_FAIL(macro_cache_mgr_->gc_hot_tablet_macro_cache(
                macro_block_set, 1_day, false /*convert_to_auto*/))) {
              LOG_WARN("fail to gc macro hot tablet macro cache", 
                       KR(ret), K(batch_count), K(processed_tablet_count), 
                       "tablet_count", macro_hot_tablet_map.size(),
                       "macro_block_count", macro_block_set.size(),
                       "batch_tablet_count", batch_map.size());
            } else {
              FLOG_INFO("finish gc macro hot tablets batch", 
                       K(batch_count), K(processed_tablet_count), 
                       "tablet_count", macro_hot_tablet_map.size(),
                       "macro_block_count", macro_block_set.size(),
                       "batch_tablet_count", batch_map.size());
            }
            // clear batch map and macro block set, prepare for next batch
            batch_map.reuse();
            macro_block_set.reuse();
          }
        }
      }
      cur_iter++;
    }
    
    // process remaining tablets (if there are any unprocessed)
    if (OB_SUCC(ret) && !batch_map.empty() && !macro_block_set.empty()) {
      ++batch_count;
      if (OB_FAIL(macro_cache_mgr_->gc_hot_tablet_macro_cache(
          macro_block_set, 1_day, false /*convert_to_auto*/))) {
        LOG_WARN("fail to gc macro hot tablet macro cache for remaining tablets", 
                 KR(ret), K(batch_count), K(processed_tablet_count), 
                 "tablet_count", macro_hot_tablet_map.size(),
                 "macro_block_count", macro_block_set.size(),
                 "batch_tablet_count", batch_map.size());
      } else {
        FLOG_INFO("finish gc macro hot tablets batch for remaining tablets", 
                 K(batch_count), K(processed_tablet_count), 
                 "tablet_count", macro_hot_tablet_map.size(),
                 "macro_block_count", macro_block_set.size(),
                 "batch_tablet_count", batch_map.size());
      }
    }
  }
  FLOG_INFO("finish gc all macro hot tablets", KR(ret), K(batch_count), K(processed_tablet_count), 
            "tablet_count", macro_hot_tablet_map.size());
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::gc_hot_tablet_macro_cache(
    const int64_t idle_time_us,
    const bool convert_to_auto)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "HotTabCacheGC");
  ObHashSet<uint64_t> active_tablet_ids;
  ObHashMap<ObTabletID, ObLSID> hot_tablet_map;
  ObHashMap<ObTabletID, ObLSID> macro_hot_tablet_map;
  ObHashMap<ObLSID, int64_t/*ls_epoch*/> ls_id_epoch_map;
  ObHashSet<MacroBlockId> macro_block_set;
  const int64_t TABLET_BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::TABLET_TYPE);
  const int64_t MACRO_BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(ObGetBucketNumType::MACRO_TYPE);
  ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_UNLIKELY(idle_time_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idle_time_us));
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage cache policy service should not be null", KR(ret));
  } else if (OB_FAIL(active_tablet_ids.create(TABLET_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create active tablet snapshot", KR(ret), K(TABLET_BUCKET_NUM));
  } else if (OB_FAIL(hot_tablet_map.create(TABLET_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create hot tablet map", KR(ret));
  } else if (OB_FAIL(macro_hot_tablet_map.create(TABLET_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create macro hot tablet map", KR(ret));
  } else if (OB_FAIL(ls_id_epoch_map.create(8/*bucket_num*/, attr))) {
    LOG_WARN("fail to create ls id epoch map", KR(ret));
  } else if (OB_FAIL(macro_block_set.create(MACRO_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create macro block set", KR(ret));
  } else if (OB_FAIL(get_active_tablet_ids(active_tablet_ids, attr))) {
    LOG_WARN("fail to get active tablet snapshot", KR(ret), K(attr));
  } else if (OB_FAIL(storage_cache_policy_service->get_hot_tablets(
      active_tablet_ids, hot_tablet_map, macro_hot_tablet_map))) {
    LOG_WARN("fail to get hot tablets", KR(ret));
  } else if (OB_FAIL(get_ls_id_epoch_map(ls_id_epoch_map))) {
    LOG_WARN("fail to get ls id epoch map", KR(ret));
  } else if (OB_FAIL(get_macro_block_set(ls_id_epoch_map, hot_tablet_map, macro_block_set))) {
    LOG_WARN("fail to get macro block set for hot tablets", KR(ret));
  } else if (OB_FAIL(get_ls_inner_macro_block_set_(ls_id_epoch_map, macro_block_set))) {
    LOG_WARN("fail to get ls inner tablet macro block set", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr_->gc_hot_tablet_macro_cache(
      macro_block_set, idle_time_us, convert_to_auto))) {
    LOG_WARN("fail to gc hot tablet macro cache", KR(ret), K(idle_time_us), K(convert_to_auto));
  }
  FLOG_INFO("finish to gc hot tablet macro cache", KR(ret), K(convert_to_auto),
      "active_tablet_cnt", active_tablet_ids.size(), "hot_tablet_cnt", hot_tablet_map.size());
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::get_ls_inner_macro_block_set_(
    const ObHashMap<ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
    ObHashSet<MacroBlockId> &macro_block_set)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t MAX_ATTEMPT_COUNT = 3;
  static constexpr int64_t RETRY_INTERVAL_US = 1_s;
  for (int64_t tablet_idx = 0;
      OB_SUCC(ret) && !is_stopped_ && tablet_idx < ObLS::TOTAL_INNER_TABLET_NUM;
      ++tablet_idx) {
    const ObTabletID tablet_id(ObLS::INNER_TABLET_ID_LIST[tablet_idx]);
    for (ObHashMap<ObLSID, int64_t>::const_iterator iter = ls_id_epoch_map.begin();
        OB_SUCC(ret) && !is_stopped_ && iter != ls_id_epoch_map.end();
        ++iter) {
      const ObLSID &ls_id = iter->first;
      bool is_get = false;
      int64_t attempt_count = 0;
      do {
        ++attempt_count;
        ret = OB_SUCCESS;
        if (OB_FAIL(get_macro_block_set_for_single_tablet(
            ls_id_epoch_map, tablet_id, ls_id, macro_block_set, is_get))) {
          if (OB_EAGAIN == ret && attempt_count < MAX_ATTEMPT_COUNT) {
            ob_usleep(RETRY_INTERVAL_US);
          }
        }
      } while (attempt_count < MAX_ATTEMPT_COUNT && !is_stopped_ && OB_EAGAIN == ret);

      if (OB_SUCC(ret) && !is_get) {
        ret = OB_EAGAIN;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get macro block set for single tablet",
            KR(ret), K(tablet_id), K(ls_id), K(attempt_count));
      }
    }
  }
  if (OB_SUCC(ret) && is_stopped_) {
    ret = OB_SERVICE_STOPPED;
    const int64_t ls_count = ls_id_epoch_map.size();
    LOG_WARN("stop to get ls inner tablet macro block set", KR(ret), K(ls_count));
  }
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::get_ls_id_epoch_map(
    ObHashMap<ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObSEArray<ObLSID, 8> ls_ids;
  ls_ids.set_attr(ObMemAttr(MTL_ID(), "GetLSID"));
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
    LOG_WARN("fail to get ls ids", KR(ret));
  } else {
    ObLSHandle ls_handle;
    const int64_t ls_id_cnt = ls_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < ls_id_cnt); ++i) {
      ls_handle.reset();
      const ObLSID &ls_id = ls_ids.at(i);
      int64_t ls_epoch = 0;
      if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SS_MACRO_CACHE_MOD))) {
        LOG_WARN("fail to get ls", KR(ret), K(ls_id));
      } else if (OB_ISNULL(ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", KR(ret), K(ls_handle));
      } else if (FALSE_IT(ls_epoch = ls_handle.get_ls()->get_ls_epoch())) {
        LOG_WARN("fail to get ls epoch", KR(ret), K(ls_id));
      } else if (OB_UNLIKELY(ls_epoch < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls epoch is invalid", KR(ret), K(ls_id), K(ls_epoch));
      } else if (OB_FAIL(ls_id_epoch_map.set_refactored(ls_id, ls_epoch))) {
        LOG_WARN("fail to set ls id epoch map", KR(ret), K(ls_id), K(ls_epoch));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_HOT_CACHE_GC_INCOMPLETE_TABLET_VERSIONS);

int ObSSHotTabletMacroCacheGCTask::get_macro_block_set_for_single_tablet(
    const ObHashMap<ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    ObHashSet<MacroBlockId> &macro_block_set,
    bool &is_get)
{
  int ret = OB_SUCCESS;
  is_get = false;
  ObTenantMetaMemMgr *t3m = nullptr;
  ObSEArray<int64_t, 10> tablet_versions;
  tablet_versions.set_attr(ObMemAttr(MTL_ID(), "TabletVers"));
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else if (OB_ISNULL(t3m = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant meta mem mgr should not be null", KR(ret));
  } else {
    int64_t ls_epoch = 0;
    int64_t current_tablet_version = -1;
    int64_t tablet_last_gc_version = -1;
    int64_t current_tablet_transfer_seq = -1;
    int64_t reorganization_scn = -1;
    uintptr_t tablet_fingerprint = 0;
    bool allow_tablet_version_gc = false;
    bool is_cur_tablet_version_exist = false;
    bool is_transfer_out_deleted = false;
    tablet_versions.reuse();
    if (OB_FAIL(ls_id_epoch_map.get_refactored(ls_id, ls_epoch))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret. skip this tablet
        LOG_INFO("ls is not exist", K(ls_id), K(tablet_id));
      } else {
        LOG_WARN("fail to get ls epoch", KR(ret), K(ls_id));
      }
    } else if (OB_UNLIKELY(ls_epoch < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls epoch is invalid", KR(ret), K(ls_id), K(ls_epoch));
    } else if (OB_FAIL(t3m->get_current_version_for_tablet(ls_id, tablet_id,
                       current_tablet_version, tablet_last_gc_version, current_tablet_transfer_seq,
                       tablet_fingerprint, allow_tablet_version_gc, is_transfer_out_deleted,
                       reorganization_scn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret. skip this tablet
        LOG_INFO("tablet is not exist", K(ls_id), K(tablet_id));
      } else {
        LOG_WARN("failed to get_current_version_for_tablet", KR(ret), K(ls_id), K(tablet_id));
      }
    } else if (OB_FAIL(ObStorageMetaIOUtil::collect_private_tablet_versions(ls_id, ls_epoch, tablet_id, reorganization_scn,
                       tablet_last_gc_version + 1, current_tablet_version, true /*allow_stale_versions*/, tablet_versions))) {
      LOG_WARN("failed to collect tablet versions", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
               K(current_tablet_transfer_seq), K(tablet_last_gc_version), K(current_tablet_version));
    // 仅用于 errsim 用例模拟版本快照暂时不完整，不修改真实 T3M 状态。
    } else if (OB_FAIL(ERRSIM_HOT_CACHE_GC_INCOMPLETE_TABLET_VERSIONS)) {
      LOG_INFO("inject incomplete tablet versions for hot cache gc retry by errsim",
          KR(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(current_tablet_version));
    } else if (tablet_versions.empty()) {
      // T3M 中仍存在当前 tablet，此时持久化版本列表为空，引用集合不完整，不能按已删除 tablet 处理。
      ret = OB_EAGAIN;
      LOG_INFO("tablet versions are incomplete, retry hot tablet macro cache gc",
          KR(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(current_tablet_version));
    } else if (OB_FAIL(check_cur_tablet_version(tablet_versions, current_tablet_version,
                                                is_cur_tablet_version_exist))) {
      LOG_WARN("failed to check current tablet version existence", KR(ret), K(tablet_versions),
               K(current_tablet_version));
    } else if (!is_cur_tablet_version_exist) {
      // current_tablet_version 可能已并发推进，旧快照不能用于判断宏块是否仍被引用，等待下一轮重新收集。
      ret = OB_EAGAIN;
      LOG_INFO("current tablet version is changing, retry hot tablet macro cache gc",
          KR(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(current_tablet_version), K(tablet_versions));
    } else {
      bool is_completed = false;
      const int64_t tablet_version_cnt = tablet_versions.count();
      for (int64_t i = 0; OB_SUCC(ret) && !is_completed && (i < tablet_version_cnt); ++i) {
        const int64_t ver = tablet_versions.at(i);
        if (OB_FAIL(get_macro_blocks(ls_id, ls_epoch, tablet_id, ver, reorganization_scn,
                                     macro_block_set))) {
          if (OB_OBJECT_NOT_EXIST == ret) { // already gc
            if (ver == current_tablet_version) {
              // T3M 返回的 current_tablet_version 对应 meta 暂时不可见，本轮引用集合不完整，禁止执行删除。
              ret = OB_EAGAIN;
              is_completed = true;
              LOG_INFO("current tablet meta is not available, retry hot tablet macro cache gc",
                  KR(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(ver), K(current_tablet_transfer_seq));
            } else {
              ret = OB_SUCCESS; // ignore ret, and process other tablet versions
              LOG_INFO("old tablet meta has been gc", K(ls_id), K(ls_epoch), K(tablet_id), K(ver),
                  K(current_tablet_transfer_seq));
            }
          } else {
            LOG_WARN("failed to get macro blocks", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
                     K(ver), K(current_tablet_transfer_seq));
          }
        }
      }
      if (OB_SUCC(ret) && !is_completed) {
        is_get = true;
      }
    }
  }
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::get_macro_block_set(
    const ObHashMap<ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
    ObHashMap<ObTabletID, ObLSID> &hot_tablet_map,
    ObHashSet<MacroBlockId> &macro_block_set)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t MAX_ATTEMPT_COUNT = 3;
  static constexpr int64_t RETRY_INTERVAL_US = 1_s;
  ObSEArray<ObTabletID, 16> tablets_to_erase;
  tablets_to_erase.set_attr(ObMemAttr(MTL_ID(), "TabToErase"));
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret));
  } else {
    ObHashMap<ObTabletID, ObLSID>::const_iterator iter = hot_tablet_map.begin();
    for (; OB_SUCC(ret) && !is_stopped_ && (iter != hot_tablet_map.end()); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      const ObLSID &ls_id = iter->second;
      bool is_get = false;
      int64_t attempt_count = 0;
      do {
        attempt_count++;
        ret = OB_SUCCESS;
        if (OB_FAIL(get_macro_block_set_for_single_tablet(
            ls_id_epoch_map, tablet_id, ls_id, macro_block_set, is_get))) {
          if (OB_EAGAIN == ret && attempt_count < MAX_ATTEMPT_COUNT) {
            ob_usleep(RETRY_INTERVAL_US);
          } else {
            LOG_WARN("fail to get macro block set for single tablet",
                KR(ret), K(tablet_id), K(ls_id), K(attempt_count));
          }
        }
      } while (attempt_count < MAX_ATTEMPT_COUNT && !is_stopped_ && OB_EAGAIN == ret);

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get macro block set for single tablet", KR(ret), K(tablet_id), K(ls_id));
      } else if (is_stopped_) {
        ret = OB_SERVICE_STOPPED;
        LOG_WARN("stop to get macro block set", KR(ret), K(tablet_id), K(ls_id));
      } else if (!is_get) {
        if (OB_FAIL(tablets_to_erase.push_back(tablet_id))) {
          LOG_WARN("fail to push back tablet id", KR(ret), K(tablet_id));
        }
      }
    } // end of map iteration

    // Erase tablets after iteration to avoid iterator invalidation
    for (int64_t i = 0; OB_SUCC(ret) && (i < tablets_to_erase.count()); ++i) {
      const ObTabletID &tablet_id = tablets_to_erase.at(i);
      if (OB_FAIL(hot_tablet_map.erase_refactored(tablet_id))) {
        LOG_WARN("fail to erase hot tablet map", KR(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::check_cur_tablet_version(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t current_tablet_version,
    bool &is_cur_tablet_version_exist)
{
  int ret = OB_SUCCESS;
  is_cur_tablet_version_exist = false;
  if (OB_UNLIKELY(tablet_versions.empty() || (current_tablet_version < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_versions), K(current_tablet_version));
  } else {
    const int64_t tablet_version_cnt = tablet_versions.count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_cur_tablet_version_exist && (i < tablet_version_cnt); ++i) {
      const int64_t ver = tablet_versions.at(i);
      if (ver == current_tablet_version) {
        is_cur_tablet_version_exist = true;
      }
    }
  }
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::get_macro_blocks(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    const int64_t reorganization_scn,
    ObHashSet<MacroBlockId> &macro_block_set)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_macro_blocks(ls_id, ls_epoch, tablet_id, tablet_version,
                               reorganization_scn,
                               true/*is_shared*/, macro_block_set))) {
    LOG_WARN("fail to get macro blocks", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
             K(tablet_version), K(reorganization_scn));
  } else if (OB_FAIL(get_macro_blocks(ls_id, ls_epoch, tablet_id, tablet_version,
                                      reorganization_scn,
                                      false/*is_shared*/, macro_block_set))) {
    LOG_WARN("fail to get macro blocks", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
             K(tablet_version), K(reorganization_scn));
  }
  return ret;
}

int ObSSHotTabletMacroCacheGCTask::get_macro_blocks(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    const int64_t reorganization_scn,
    const bool is_shared,
    ObHashSet<MacroBlockId> &macro_block_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, 32> block_ids;
  block_ids.set_attr(ObMemAttr(MTL_ID(), "BlockIds"));
  ObTenantStorageMetaService *tsms = nullptr;
  if (OB_ISNULL(tsms = MTL(ObTenantStorageMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_blocks_from_private_tablet_meta(ls_id, ls_epoch, tablet_id,
                     tablet_version, reorganization_scn, is_shared, block_ids))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      LOG_INFO("tablet meta is deleted", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
               K(tablet_version), K(reorganization_scn));
    } else {
      LOG_WARN("fail to get_blocks_from_private_tablet_meta", K(ret), K(is_shared));
    }
  } else {
    const int64_t block_id_cnt = block_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < block_id_cnt); ++i) {
      const MacroBlockId &block_id = block_ids.at(i);
      if (OB_FAIL(macro_block_set.set_refactored(block_id))) {
        LOG_WARN("fail to set macro block set", KR(ret), K(block_id));
      }
    }
  }
  return ret;
}
  
} // namespace storage
} // namespace oceanbase
