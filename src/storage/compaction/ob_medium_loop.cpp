// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_medium_loop.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/hash/ob_hashset.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/compaction_v2/ob_ss_schedule_tablet_func.h"
#endif

namespace oceanbase
{
namespace compaction
{
/********************************************ObMediumLoop impl******************************************/
int ObMediumLoop::start_merge(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(merge_version));
  } else {
    {
      ObMutexGuard guard(lock_);
      merge_version_ = merge_version;
    }
    schedule_stats_.start_merge();

    const int64_t last_merged_version = ObBasicMergeScheduler::get_merge_scheduler()->get_merged_version();
    ADD_COMPACTION_EVENT(
        merge_version,
        ObServerCompactionEvent::RECEIVE_BROADCAST_SCN,
        schedule_stats_.start_timestamp_,
        K(last_merged_version));
  }
  return ret;
}

int ObMediumLoop::init(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge_version", KR(ret), K_(merge_version));
  } else if (!ls_tablet_iter_.is_valid()) { // new loop
    ObMutexGuard guard(lock_);
    loop_version_ = merge_version_;
  }
  if (FAILEDx(ls_tablet_iter_.build_iter(batch_size))) {
    LOG_WARN("failed to init ls iterator", K(ret));
  }
  return ret;
}

int ObMediumLoop::loop()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObScheduleTabletFunc *func = nullptr;
  // merge_version_ may be larger than loop_version_ by other thread,
  // keep using merge_version_ can make the medium loop faster
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    func = MTL_NEW(ObSSScheduleTabletFunc, "SS_ScheTablet", merge_version_, ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE, true, loop_cnt_);
  } else {
    func = MTL_NEW(ObScheduleTabletFunc, "SN_SchedTablet", merge_version_, ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE, loop_cnt_);
  }
#else
  func = MTL_NEW(ObScheduleTabletFunc, "SN_SchedTablet", merge_version_, ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE, loop_cnt_);
#endif
  if (OB_ISNULL(func)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc SchedTabletFunc fail", K(ret));
  }
  ObLSID ls_id;
  schedule_stats_.all_ls_weak_read_ts_ready_ = true;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tablet_iter_.get_next_ls(ls_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), K_(ls_tablet_iter));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else if (FALSE_IT(ls_id = ls->get_ls_id())) {
    } else if (OB_TMP_FAIL(loop_in_ls(ls_handle, *func))) {
      LOG_TRACE("failed to loop ls", KR(ret), KPC(ls), KPC(func));
      ls_tablet_iter_.skip_cur_ls(); // for any errno, skip cur ls
      ls_tablet_iter_.update_merge_finish(false);
      if (OB_SIZE_OVERFLOW == tmp_ret) {
        break;
      } else if (!schedule_ignore_error(tmp_ret)) {
        LOG_WARN("failed to schedule ls merge", K(tmp_ret), K(ls_id));
      }
    }
    if (OB_SUCC(ret) && ls_tablet_iter_.need_report_scn()) {
      // loop tablet_meta table to update smaller report_scn because of migration
      tmp_ret = update_report_scn_as_ls_leader(*ls, *func);
#ifndef ERRSIM
      LOG_INFO("try to update report scn as ls leader", K(tmp_ret), K(ls_id)); // low printing frequency
#endif
    }
  } // while
  LOG_TRACE("finish schedule ls medium merge", K(tmp_ret), K(ret), K_(ls_tablet_iter), K(ls_id));
  if (func) {
    add_event_and_diagnose(*func);
#ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()) {
      ((ObSSScheduleTabletFunc*)func)->~ObSSScheduleTabletFunc();
    } else {
      func->~ObScheduleTabletFunc();
    }
#else
    func->~ObScheduleTabletFunc();
#endif
    mtl_free(func);
  }
  return ret;
}

int ObMediumLoop::loop_in_ls(
  ObLSHandle &ls_handle,
  ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();

  if (OB_FAIL(func.switch_ls(ls_handle))) {
    if (OB_STATE_NOT_MATCH != ret) {
      LOG_WARN("failed to switch ls", KR(ret), K(ls_id), K(func));
    } else {
      ls_tablet_iter_.update_merge_finish(false);
      schedule_stats_.all_ls_weak_read_ts_ready_ = false;
    }
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletID tablet_id;
    bool tablet_merge_finish = false;
    while (OB_SUCC(ret)) { // loop all tablet in ls
      if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid()
        || nullptr == (tablet = tablet_handle.get_obj()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle is invalid", KR(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
      } else if (!tablet_id.can_major_merge()) {
        // do nothing
      } else if (OB_TMP_FAIL(func.switch_and_schedule_tablet(tablet_handle, tablet_merge_finish))) {
        if (OB_STATE_NOT_MATCH != tmp_ret) {
          LOG_WARN("failed to schedule tablet", KR(tmp_ret), K(ls_id), K(tablet_id));
        }
        ls_tablet_iter_.update_merge_finish(false);
      } else {
        ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
      }
    } // while
    if (OB_TMP_FAIL(func.post_process_ls())) {
      LOG_WARN("post process ls fail", K(tmp_ret), K(ls_id));
    }
  }
  return ret;
}

void ObMediumLoop::add_event_and_diagnose(const ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!ls_tablet_iter_.tenant_merge_finish() && func.get_merge_version() > ObBasicMergeScheduler::INIT_COMPACTION_SCN) {
    // not finish cur merge_version
    if (schedule_stats_.all_ls_weak_read_ts_ready_) { // check schedule Timer Task
      if (schedule_stats_.add_weak_read_ts_event_flag_ && ls_tablet_iter_.is_scan_finish()) { // all ls scan finish
        schedule_stats_.add_weak_read_ts_event_flag_ = false;
        ADD_COMPACTION_EVENT(
            func.get_merge_version(),
            ObServerCompactionEvent::WEAK_READ_TS_READY,
            ObTimeUtility::fast_current_time(),
            "check_weak_read_ts_cnt", schedule_stats_.check_weak_read_ts_cnt_ + 1);
      }
    } else {
      schedule_stats_.check_weak_read_ts_cnt_++;
    }

    if (ls_tablet_iter_.is_scan_finish()) {
      loop_cnt_++;
      if (REACH_THREAD_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
          loop_version_,
          ObServerCompactionEvent::SCHEDULER_LOOP,
          ObTimeUtility::fast_current_time(),
          "schedule_stats", schedule_stats_,
          "schedule_tablet_cnt", func.get_schedule_tablet_cnt(),
          "current_merge_version", func.get_merge_version());
      }
    }
  }

  const int64_t merged_version = ObBasicMergeScheduler::get_merge_scheduler()->get_merged_version();
  if (ls_tablet_iter_.tenant_merge_finish() && loop_version_ > merged_version) {
    ObBasicMergeScheduler::get_merge_scheduler()->update_merged_version(loop_version_);
    LOG_INFO("all tablet major merge finish", K(merged_version), K_(loop_cnt), K(func.get_merge_version()));

    DEL_SUSPECT_INFO(MEDIUM_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE);
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->finish_progress(loop_version_))) {
      LOG_WARN("failed to finish progress", K(tmp_ret), K_(loop_version));
    }

    const int64_t current_time = ObTimeUtility::fast_current_time();
    ADD_COMPACTION_EVENT(
          loop_version_,
          ObServerCompactionEvent::TABLET_COMPACTION_FINISHED,
          current_time,
          "cost_time", current_time - schedule_stats_.start_timestamp_,
          "current_merge_version", func.get_merge_version());
  }

  LOG_INFO("finish schedule all tablet merge", K(loop_version_), K(schedule_stats_), K_(loop_cnt),
      "current_merge_version", func.get_merge_version(),
      "tenant_merge_finish", ls_tablet_iter_.tenant_merge_finish(),
      "is_scan_all_tablet_finish", ls_tablet_iter_.is_scan_finish(),
      "schedule_tablet_cnt", func.get_schedule_tablet_cnt(),
      "time_guard", func.get_time_guard());
}

void ObMediumLoop::clear()
{
  loop_cnt_ = 0;
  schedule_stats_.reset();
}

int ObMediumLoop::update_report_scn_as_ls_leader(ObLS &ls, const ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const int64_t inner_table_merged_scn = ObBasicMergeScheduler::get_merge_scheduler()->get_inner_table_merged_scn();
  const ObLSStatusCache &ls_status = func.get_ls_status();
  if (ls_status.can_merge() && ls_status.is_leader_) {
    ObSEArray<ObTabletID, 200> tablet_id_array;
    struct TabletIDFilter final : public ObITabletIDFilter {
      bool operator()(const common::ObTabletID &tablet_id) const override {
        return tablet_id.can_major_merge();
      }
    } tablet_id_filter;
    if (OB_FAIL(ls.get_tablet_svr()->get_all_tablet_ids_with_filter(tablet_id_filter, tablet_id_array))) {
      LOG_WARN("failed to get tablet id", K(ret), K(ls_id));
    } else if (inner_table_merged_scn > ObBasicMergeScheduler::INIT_COMPACTION_SCN
        && OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_unequal_report_scn_tablet(
          MTL_ID(), ls_id, inner_table_merged_scn, tablet_id_array))) {
      LOG_WARN("failed to get unequal report scn", K(ret), K(ls_id), K(inner_table_merged_scn));
    }
  } else {
    ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  }
  return ret;
}

/********************************************ObScheduleNewMediumLoop impl******************************************/
struct ObTabletCheckInfoProcessor
{
  int operator()(ObScheduleTabletFunc &func, const ObTabletCheckInfo &tablet_check_info, ObTabletHandle &tablet_handle) {
    int ret = OB_SUCCESS;
    UNUSED(tablet_check_info);
    if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
    // 在调度主表 tablet 时, 会同时触发异步索引 tablet 的调度
    // 所以这里不需要单独对异步索引 tablet 进行二次调度
    } else if (OB_NOT_NULL(tablet_handle.get_obj()) && tablet_handle.get_obj()->is_async_index_storage_tablet()) {
      LOG_TRACE("async index tablet can only be scheduled via schedule_for_async_index on data tablet", K(tablet_handle));
    } else if (OB_FAIL(func.request_schedule_new_round(tablet_handle, false/*user_request*/, false/*need_load_tablet_status*/))) {
      LOG_WARN("failed to request schedule new round", KR(ret), K(tablet_handle));
    }
    return ret;
  }
};

int ObScheduleNewMediumLoop::schedule_async_index_medium(ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t schedule_cnt = 0;
  int64_t dedup_cnt = 0;
  bool current_ls_ready = false;
  storage::ObLSHandle ls_handle;
  share::ObLSID current_ls_id;
  hash::ObHashSet<share::ObTabletLSPair, hash::NoPthreadDefendMode> data_tablet_set;
  if (tablet_ls_infos_.empty()) {
  } else if (OB_FAIL(data_tablet_set.create(tablet_ls_infos_.count(), ObMemAttr(MTL_ID(), "AsyncIdxWake")))) {
    LOG_WARN("failed to create async index data tablet set", K(ret), K_(tablet_ls_infos));
  } else {
    for (int64_t i = 0; i < tablet_ls_infos_.count(); ++i) {
      tmp_ret = OB_SUCCESS;
      const ObTabletCheckInfo &check_info = tablet_ls_infos_.at(i);
      ObTabletHandle index_tablet_handle;
      ObTabletHandle data_tablet_handle;
      ObTablet *index_tablet = nullptr;
      if (OB_UNLIKELY(!check_info.is_valid())) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid finished medium check info", K(tmp_ret), K(check_info));
      } else {
        if (!current_ls_id.is_valid() || current_ls_id != check_info.get_ls_id()) {
          // 切换到新的 LS
          current_ls_id = check_info.get_ls_id();
          current_ls_ready = false;
          ls_handle.reset();
          if (OB_TMP_FAIL(MTL(storage::ObLSService *)->get_ls(current_ls_id, ls_handle, storage::ObLSGetMod::COMPACT_MODE))) {
            if (OB_LS_NOT_EXIST == tmp_ret) {
              LOG_TRACE("ls not exist when scheduling async index medium", K(tmp_ret), K(current_ls_id));
            } else {
              LOG_WARN("failed to get ls when scheduling async index medium", K(tmp_ret), K(current_ls_id));
            }
          } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid ls handle when scheduling async index medium", K(tmp_ret), K(current_ls_id));
          } else if (OB_TMP_FAIL(func.switch_ls(ls_handle))) {
            LOG_WARN("failed to switch ls when scheduling async index medium", K(tmp_ret), K(current_ls_id));
          } else {
            current_ls_ready = true;
          }
        }

        if (!current_ls_ready) {
        } else if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(check_info.get_tablet_id(), index_tablet_handle, 0 /*timeout_us*/))) {
          LOG_WARN("failed to get finished tablet when scheduling async index medium", K(tmp_ret), K(check_info));
        } else if (OB_ISNULL(index_tablet = index_tablet_handle.get_obj())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("finished tablet is null", K(tmp_ret), K(check_info));
        } else if (index_tablet->get_tablet_id() != check_info.get_tablet_id()
            || index_tablet->get_ls_id() != check_info.get_ls_id()) {
          tmp_ret = OB_STATE_NOT_MATCH;
          LOG_WARN("finished tablet and check info are not match", K(tmp_ret), K(check_info), KPC(index_tablet));
        } else if (index_tablet->is_async_index_storage_tablet()) {
          const ObTabletID &data_tablet_id = index_tablet->get_data_tablet_id();
          const share::ObTabletLSPair data_tablet_pair(data_tablet_id, current_ls_id);
          if (OB_UNLIKELY(!data_tablet_pair.is_valid() || data_tablet_id == check_info.get_tablet_id())) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid data tablet pair for async index medium", K(tmp_ret), K(check_info), K(data_tablet_id));
          } else if (OB_TMP_FAIL(data_tablet_set.set_refactored(data_tablet_pair, 0 /*flag = 0 not overwrite*/))) {
            if (OB_HASH_EXIST == tmp_ret) {
              tmp_ret = OB_SUCCESS;
              ++dedup_cnt;
            } else {
              LOG_WARN("failed to add async index data tablet pair", K(tmp_ret), K(check_info), K(data_tablet_pair));
            }
          } else if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(data_tablet_id, data_tablet_handle, 0 /*timeout_us*/))) {
            LOG_WARN("failed to get data tablet when scheduling async index medium", K(tmp_ret), K(check_info), K(data_tablet_id));
          } else if (OB_TMP_FAIL(func.request_schedule_async_index_new_round(data_tablet_handle))) {
            if (OB_NO_NEED_MERGE == tmp_ret) {
              LOG_TRACE("no need to schedule async index medium", K(tmp_ret), K(check_info), K(data_tablet_id));
              tmp_ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to schedule async index medium", K(tmp_ret), K(check_info), K(data_tablet_id));
            }
          } else {
            ++schedule_cnt;
          }
        }
      }
    }
  }
  LOG_INFO("finish scheduling async index medium", K(ret), K(schedule_cnt), K(dedup_cnt),
      "finish_tablet_cnt", tablet_ls_infos_.count());
  return ret;
}

int ObScheduleNewMediumLoop::loop()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSSortedIterator<ObTabletCheckInfo> iter;
  ObTabletCheckInfoProcessor processor;
  const int64_t frozen_version = ObBasicMergeScheduler::get_merge_scheduler()->get_frozen_version();
  ObScheduleTabletFunc func(frozen_version);
  // sort tablet ls info
  if (OB_FAIL(sort_tablet_ls_info())) {
    LOG_WARN("failed to sort", KR(ret));
  } else {
    if (OB_TMP_FAIL(iter.iterate(func, tablet_ls_infos_, processor, true /*skip_follower*/))) {
      LOG_WARN("failed to iterate tablet check info", K(tmp_ret));
    }
    if (OB_TMP_FAIL(schedule_async_index_medium(func))) {
      LOG_WARN("failed to schedule async index medium", K(tmp_ret));
    }
  }
  ret = OB_SUCCESS;
  LOG_INFO("end of ObScheduleNewMediumLoop", KR(ret), "finish_tablet_cnt", tablet_ls_infos_.count(), K(func));
  return ret;
}

struct ObTabletLSInfoComparator final {
public:
  ObTabletLSInfoComparator(int &sort_ret)
    : result_code_(sort_ret)
  {}
  bool operator()(const ObTabletCheckInfo &lhs, const ObTabletCheckInfo &rhs)
  {
    return lhs.get_ls_id() < rhs.get_ls_id();
  }
  int &result_code_;
};

int ObScheduleNewMediumLoop::sort_tablet_ls_info()
{
  int ret = OB_SUCCESS;
  if (tablet_ls_infos_.count() > 2) {
    ObTabletLSInfoComparator cmp(ret);
    ob_sort(tablet_ls_infos_.begin(), tablet_ls_infos_.end(), cmp);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
