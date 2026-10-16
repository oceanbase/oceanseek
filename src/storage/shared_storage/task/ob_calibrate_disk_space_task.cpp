/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_calibrate_disk_space_task.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCalibrateDiskSpaceTask::ObCalibrateDiskSpaceTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), is_stop_(false), run_cnt_(0),
    last_exec_timestamp_us_(0), is_mandatory_calibrate_(false)
{
}

int ObCalibrateDiskSpaceTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCalibrateDiskSpaceTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    is_mandatory_calibrate_ = false;
    tenant_id_ = tenant_id;
    LOG_INFO("succ to init calibrate disk space task", K(tenant_id), K_(is_stop));
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::start()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  const int64_t INTERVAL_US = 5 * 1000L * 1000L; // 5s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("calibrate disk space task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to start calibrate disk space task", K_(tenant_id), K_(is_stop));
  }
  return ret;
}

void ObCalibrateDiskSpaceTask::stop()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    is_stop_ = true;
    LOG_INFO("ObCalibrateDiskSpaceTask stop finished", K_(is_stop));
  }
}

void ObCalibrateDiskSpaceTask::wait()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObCalibrateDiskSpaceTask wait finished");
  }
}

void ObCalibrateDiskSpaceTask::destroy()
{
  stop();
  wait();
  is_mandatory_calibrate_ = false;
  last_exec_timestamp_us_ = 0;
  run_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObCalibrateDiskSpaceTask::set_mandatory_calibrate()
{
  is_mandatory_calibrate_ = true;
}

void ObCalibrateDiskSpaceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_need_exec = false;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTenantStorageMetaService *tsms = nullptr;
  if (!OB_SERVER_DISK_SPACE_MGR.is_share_tenant_disk_space_quota_initialized(tenant_id_)) {
    // if meta/user tenant pair is not fully initialized,
    // disk space share/quota between the tenant pair is incomplete,
    // which can lead to calibrate disk space failed, so skip and retry later
    LOG_INFO("tenant pair is not ready, skip calibrate disk space", K(tenant_id_));
    if (OB_FAIL(schedule_calibrate_disk_space())) {
      LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret), K(tenant_id_));
    }
  } else {
    ObMandatoryCalibrateGuard guard(is_mandatory_calibrate_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret), K_(is_inited));
    } else if (OB_ISNULL(tsms = MTL(ObTenantStorageMetaService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObTenantStorageMetaService is null", KR(ret), KP(tsms), K_(tenant_id));
    } else if (!tsms->is_started()) {
      // if ObTenantStorageMetaService do not started, do not calibrate disk space, because it will pause gc when calibrate disk space,
      // tablet_current_version must delete success when ObTenantStorageMetaService is starting, otherwise server restart failed
      if (OB_FAIL(schedule_calibrate_disk_space())) {
        LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret));
      }
    } else if (OB_FAIL(check_if_need_calibration(guard.is_cur_calibrate_mandatory(), is_need_exec))) {
      LOG_WARN("fail to check if need calibration", KR(ret));
    } else if (!is_need_exec) {
      // do nothing
    } else if (OB_FAIL(calibrate_disk_space())) {
      LOG_WARN("fail to calibrate tenant disk space", KR(ret), K_(tenant_id));
    }
  }
}

int ObCalibrateDiskSpaceTask::schedule_calibrate_disk_space()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  const int64_t INTERVAL_US = 5 * 1000 * 1000L; // 5s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("calibrate disk space task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US/*delay*/, false/*schedule no repeatly*/))) {
    LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to schedule calibrate disk space");
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::check_if_need_calibration(
    const bool is_mandatory_calibrate,
    bool &is_need_exec)
{
  int ret = OB_SUCCESS;
  is_need_exec = false;
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (is_mandatory_calibrate) {
    // mandatory calibrate need execute as soon as possible
    is_need_exec = true;
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service should not be null", KR(ret));
  } else {
    const int64_t disk_space_cal_interval_us = local_cache_service->get_disk_space_cal_interval_us();
    if ((ObTimeUtility::current_time_us() - last_exec_timestamp_us_) > disk_space_cal_interval_us) {
      is_need_exec = true;
      last_exec_timestamp_us_ = ObTimeUtility::current_time_us();
    }
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::calibrate_disk_space()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObTenantFileManager *file_mgr = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  bool is_all_calibrate_succ = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *)) ||
             OB_ISNULL(file_mgr = MTL(ObTenantFileManager *)) ||
             OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager or file manager or macro cache mgr is null", KR(ret),
             KP(tnt_disk_space_mgr), KP(file_mgr), KP(macro_cache_mgr), K_(tenant_id));
  } else if (!macro_cache_mgr->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache calibrate task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else {
    // if choose start_calc_size_time_s failed, retry 10 times
    int64_t retry_count = 10;
    int64_t start_calc_size_time_s = 0;
    do {
      if (OB_FAIL(choose_start_calc_size_time_s(start_calc_size_time_s))) {
        LOG_WARN("fail to choose start calculate size time", KR(ret));
        ob_usleep(10 * 1000L); // 10ms
      }
    } while (OB_FAIL(ret) && (--retry_count > 0));

    if (OB_FAIL(ret)) {
      LOG_WARN("fail to choose start_calc_size_time_s within 10 retry times", KR(ret));
    }

    if (OB_SUCC(ret)) {
      file_mgr->set_tmp_file_cache_pause_gc();
      file_mgr->set_pause_gc();
      ObFileStatInfo meta_file_stat_info;
      ObFileStatInfo tmp_file_stat_info;
      ObFileStatInfo macro_block_stat_info;
      ObFileStatInfo hot_tablet_macro_block_stat_info;
      if (OB_FAIL(get_macro_cache_used_size(meta_file_stat_info, tmp_file_stat_info,
                                            macro_block_stat_info, hot_tablet_macro_block_stat_info))) {
        LOG_WARN("fail to get macro cache used size", KR(ret));
        file_mgr->set_tmp_file_cache_allow_gc();
        file_mgr->set_allow_gc();
      } else {
        ObCalibrateDiskSpaceTimeLog time_log;
        time_log.start_calc_us_ = ObTimeUtility::current_time_us();
        FLOG_INFO("begin to calibrate data disk space", K_(tenant_id),
                  "meta_file", meta_file_stat_info,
                  "tmp_file", tmp_file_stat_info,
                  "macro_block", macro_block_stat_info,
                  "hot_tablet_macro_block", hot_tablet_macro_block_stat_info);

        // step 1: calibrate TMP_FILE used size
        if (!is_stop_ && OB_TMP_FAIL(calibrate_macro_cache_used_size(ObSSMacroCacheType::TMP_FILE,
                                                                      start_calc_size_time_s,
                                                                      tmp_file_stat_info))) {
          is_all_calibrate_succ = false;
          LOG_WARN("fail to calibrate tmp file used size", KR(tmp_ret));
        }
        time_log.finish_tmp_file_calc_us_ = ObTimeUtility::current_time_us();
        file_mgr->set_tmp_file_cache_allow_gc();

        // step 2: calibrate META_FILE used size
        if (!is_stop_ && OB_TMP_FAIL(calibrate_macro_cache_used_size(ObSSMacroCacheType::META_FILE,
                                                                      start_calc_size_time_s,
                                                                      meta_file_stat_info))) {
          is_all_calibrate_succ = false;
          LOG_WARN("fail to calibrate meta file used size", KR(tmp_ret));
        }
        time_log.finish_meta_file_calc_us_ = ObTimeUtility::current_time_us();

        // step 3: calibrate MACRO_BLOCK and HOT_TABLET_MACRO_BLOCK used size
        if (!is_stop_ && OB_TMP_FAIL(calibrate_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK,
                                                                      start_calc_size_time_s,
                                                                      macro_block_stat_info,
                                                                      hot_tablet_macro_block_stat_info.total_size_))) {
          is_all_calibrate_succ = false;
          LOG_WARN("fail to calibrate macro block and hot tablet macro block used size", KR(tmp_ret));
        }
        time_log.finish_macro_block_calc_us_ = ObTimeUtility::current_time_us();
        file_mgr->set_allow_gc();

        if (OB_FAIL(get_macro_cache_used_size(meta_file_stat_info, tmp_file_stat_info,
                                              macro_block_stat_info, hot_tablet_macro_block_stat_info))) {
          LOG_WARN("fail to get macro cache used size", KR(ret));
        } else {
          if (is_all_calibrate_succ) {
            tnt_disk_space_mgr->set_first_calibration_done(true);
          }
          FLOG_INFO("finish to calibrate data disk space", KR(tmp_ret), K_(tenant_id),
                    "meta_file", meta_file_stat_info,
                    "tmp_file", tmp_file_stat_info,
                    "macro_block", macro_block_stat_info,
                    "hot_tablet_macro_block", hot_tablet_macro_block_stat_info,
                    K(time_log));
        }
      }
    }
    LOG_INFO("finish to calibrate data disk space", KR(ret), KR(tmp_ret), K_(tenant_id));
    // delete macro cache meta, whose last_access_time falls behind start_calc_size_time and 'is_marked=false'
    // Note: only mark_and_sweep when succ to calibrate all directories. otherwise, mark is incomplete,
    // and will sweep macro_caches that should not be sweeped.
    if (!is_stop_ && is_all_calibrate_succ &&
        OB_TMP_FAIL(macro_cache_mgr->erase_unmarked_macro_cache_meta(start_calc_size_time_s * 1000L * 1000L))) {
      LOG_WARN("fail to erase unmarked macro cache meta", KR(tmp_ret), K(start_calc_size_time_s));
    }
    // try clear is_marked, no matter erase_unmarked_macro_cache_meta succ or fail
    if (!is_stop_ && OB_TMP_FAIL(macro_cache_mgr->clear_is_marked())) {
      LOG_WARN("fail to clear is marked", KR(tmp_ret));
    }
    // delete tenant_id dir's .tmp.seq files, for example SERVER_META and TENANT_DISK_SPACE_META
    ObDirCalcSizeOp del_tmp_seq_file_op(is_stop_);
    del_tmp_seq_file_op.set_for_del_tmp_seq_files();
    if (!is_stop_ && OB_TMP_FAIL(delete_tmp_seq_files(del_tmp_seq_file_op))) {
      LOG_WARN("fail to delete tmp seq files", KR(tmp_ret));
    }
    // delete tmp_data dir's .deleted files and empty scatter & tmp_file directories
    if (!is_stop_ && OB_TMP_FAIL(rm_logical_deleted_file())) {
      LOG_WARN("fail to rm logical deleted files", KR(tmp_ret));
    }

    if (OB_TMP_FAIL(tnt_disk_space_mgr->sync_used_size_to_macro_cache_stats())) {
      LOG_WARN("fail to sync used size to macro cache stats", KR(ret), KR(tmp_ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::calibrate_macro_cache_used_size_forcely(const ObSSMacroCacheType cache_type,
                                                                    const ObFileStatInfo &initial_stat_info,
                                                                    const int64_t initial_hot_tablet_used_size,
                                                                    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  int64_t final_target_file_size = 0;
  int64_t observed_used_size = 0;
  int64_t final_hot_tablet_target = 0;
  int64_t observed_hot_tablet_used = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else {
    // Force calibrate when first start or restart or is_mandatory_calibrate_ is true.
    // After restart, persisted macro_cache_used_size may be incorrect and dir_num/dir_size
    // are reset to 0, so rebuild from disk scan.
    // Normalize dir size to 4KB per dir to avoid calibration drift from directory space
    // bloat, especially on XFS.
    // |disk_file_size|disk_dir_size(calibrated to disk_dir_num * 4kb)|
    FLOG_INFO("force calibrate macro cache used size", KR(ret), K(tenant_id_),
        K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res), K(initial_stat_info),
        K(initial_hot_tablet_used_size));
    calibrate_res.total_file_size_ = calibrate_res.total_file_size_ - calibrate_res.total_dir_size_ + calibrate_res.total_dir_num_ * ObDirManager::DEFAULT_DIR_SIZE;
    if (OB_FAIL(tnt_disk_space_mgr->calibrate_dir_info_with_initial(
        initial_stat_info.dir_num_, calibrate_res.total_dir_num_, cache_type))) {
      LOG_WARN("fail to calibrate dir info", KR(ret), K_(tenant_id), K(calibrate_res));
    } else if (OB_FAIL(tnt_disk_space_mgr->calibrate_alloc_size_with_initial(
        initial_stat_info.total_size_, calibrate_res.total_file_size_, cache_type,
        final_target_file_size, observed_used_size))) {
      LOG_WARN("fail to calibrate alloc disk size", KR(ret), K_(tenant_id),
          K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res));
    } else if (ObSSMacroCacheType::MACRO_BLOCK == cache_type) {
      // hot_tablet_macro_block and macro_block share the same directory, no need to calibrate dir info.
      if (OB_FAIL(tnt_disk_space_mgr->calibrate_alloc_size_with_initial(
          initial_hot_tablet_used_size, calibrate_res.hot_tablet_total_used_size_,
          ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
          final_hot_tablet_target, observed_hot_tablet_used))) {
        LOG_WARN("fail to calibrate alloc disk size", KR(ret), K_(tenant_id),
            K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res));
      }
    }
  }
  FLOG_INFO("finish to force calibrate macro cache used size", KR(ret), K_(tenant_id),
      K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res), K(initial_stat_info),
      K(initial_hot_tablet_used_size), K(final_target_file_size), K(observed_used_size),
      K(final_hot_tablet_target), K(observed_hot_tablet_used));
  return ret;
}


int ObCalibrateDiskSpaceTask::calibrate_macro_cache_used_size_normally(const ObSSMacroCacheType cache_type,
                                                                       const ObFileStatInfo &initial_stat_info,
                                                                       const int64_t initial_hot_tablet_used_size,
                                                                       ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  int64_t dir_calibrate_size = 0;
  int64_t base_target_file_size = 0;
  int64_t deviation_alloc_size = 0;
  int64_t deviation_threshold = 0;
  int64_t final_target_file_size = 0;
  int64_t observed_used_size = 0;
  int64_t final_hot_tablet_target = 0;
  int64_t observed_hot_tablet_used = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_FAIL(calibrate_res.get_dir_calibrate_size(initial_stat_info.dir_num_, dir_calibrate_size))) {
    LOG_WARN("fail to get dir calibrate size", KR(ret), K(initial_stat_info.dir_size_), K(calibrate_res));
  } else {
    // base_target_file_size = disk_scan_total + dir_calibrate_size. The (cur - initial) delta
    // that the old code added here is now folded in atomically by
    // calibrate_alloc_size_with_initial under the disk-space lock, so allocs/frees racing
    // with the calibration are not silently dropped.
    base_target_file_size = calibrate_res.total_file_size_ + dir_calibrate_size;
    // Deviation check: in the original formula
    //   deviation = (disk_scan + dir_cal + (cur - initial)) + del_tmp_seq - cur
    //             = disk_scan + dir_cal + del_tmp_seq - initial
    // so cur cancels and the check is identical using only initial.
    if (ObSSMacroCacheType::MACRO_BLOCK == cache_type) {
      deviation_alloc_size = base_target_file_size + calibrate_res.hot_tablet_total_used_size_
                           + calibrate_res.del_tmp_seq_file_size_
                           - initial_stat_info.total_size_ - initial_hot_tablet_used_size;
    } else {
      deviation_alloc_size = base_target_file_size + calibrate_res.del_tmp_seq_file_size_
                           - initial_stat_info.total_size_;
    }
    if (OB_FAIL(ObCalibrateDiskSpaceUtil::get_deviation_threshold(deviation_alloc_size, deviation_threshold))) {
      LOG_WARN("fail to get deviation threshold", KR(ret), K(deviation_alloc_size));
    } else if (OB_UNLIKELY(std::abs(deviation_alloc_size) > deviation_threshold)) {
      const int deviation_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("deviation is too large between calculated size and recoreded size, continue to calibrate once",
                KR(deviation_ret),
                K_SS_MACRO_CACHE_TYPE(cache_type), K(deviation_alloc_size), K(deviation_threshold),
                K(calibrate_res), K(base_target_file_size), K(initial_stat_info),
                K(initial_hot_tablet_used_size), K(dir_calibrate_size));
    }

    if (FAILEDx(tnt_disk_space_mgr->calibrate_alloc_size_with_initial(
            initial_stat_info.total_size_, base_target_file_size, cache_type,
            final_target_file_size, observed_used_size))) {
      LOG_WARN("fail to calibrate alloc size atomically", KR(ret), K(tenant_id_),
               K_SS_MACRO_CACHE_TYPE(cache_type), K(base_target_file_size),
               K(initial_stat_info), K(calibrate_res), K(initial_hot_tablet_used_size));
    } else if (ObSSMacroCacheType::MACRO_BLOCK == cache_type) {
      // HOT_TABLET shares the directory with MACRO_BLOCK; calibrate separately. The
      // small window between the two atomic applies is accepted to keep the API
      // per-cache-type.
      if (OB_FAIL(tnt_disk_space_mgr->calibrate_alloc_size_with_initial(
              initial_hot_tablet_used_size, calibrate_res.hot_tablet_total_used_size_,
              ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
              final_hot_tablet_target, observed_hot_tablet_used))) {
        LOG_WARN("fail to calibrate hot tablet alloc size atomically", KR(ret), K(tenant_id_),
                 K(calibrate_res), K(initial_hot_tablet_used_size), K(initial_stat_info));
      }
    }
  }
  FLOG_INFO("finish to calibrate alloc disk size", KR(ret), K_(tenant_id),
            K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res), K(initial_stat_info),
            K(base_target_file_size), K(final_target_file_size), K(observed_used_size),
            K(deviation_alloc_size), K(deviation_threshold), K(dir_calibrate_size),
            K(initial_hot_tablet_used_size), K(final_hot_tablet_target),
            K(observed_hot_tablet_used));
  return ret;
}

int ObCalibrateDiskSpaceTask::calibrate_macro_cache_used_size(
    const ObSSMacroCacheType cache_type,
    const int64_t start_calc_size_time_s,
    const ObFileStatInfo &initial_stat_info,
    const int64_t initial_hot_tablet_used_size)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = nullptr;
  ObCalibrateDiskSpaceResult calibrate_res;
  const int64_t initial_used_size = initial_stat_info.total_size_;
  const bool is_forcely = (1 == run_cnt_ || is_mandatory_calibrate_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((ObSSMacroCacheType::MAX_TYPE == cache_type) ||
                         (start_calc_size_time_s <= 0) || (initial_used_size < 0) ||
                         (initial_hot_tablet_used_size < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type),
             K(start_calc_size_time_s), K(initial_used_size), K(initial_hot_tablet_used_size));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr), K_(tenant_id));
  } else if (OB_FAIL(file_mgr->calc_macro_cache_disk_space(cache_type, start_calc_size_time_s, calibrate_res))) {
    LOG_WARN("fail to calc macro cache disk space", KR(ret), K_(tenant_id), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else if (is_forcely) {
    // Force calibrate on first-run / restart / mandatory: rebuilds dir info from disk and
    // applies alloc_size atomically via calibrate_*_with_initial, same as the normal path.
    FLOG_INFO("begin to calibrate macro cache used size forcely", K_(run_cnt), K_(is_mandatory_calibrate));
    if (OB_FAIL(calibrate_macro_cache_used_size_forcely(cache_type, initial_stat_info,
                                                         initial_hot_tablet_used_size, calibrate_res))) {
      LOG_WARN("fail to calibrate macro cache used size forcely", KR(ret), K_(tenant_id), K(calibrate_res));
    }
    FLOG_INFO("finish to calibrate macro cache used size forcely", KR(ret), K_(tenant_id),
        K_SS_MACRO_CACHE_TYPE(cache_type), K(calibrate_res), K(initial_stat_info),
        K(initial_hot_tablet_used_size));
  } else if (OB_FAIL(calibrate_macro_cache_used_size_normally(cache_type, initial_stat_info,
                                                              initial_hot_tablet_used_size, calibrate_res))) {
    LOG_WARN("fail to calibrate macro cache used size normally", KR(ret), K_(tenant_id), K(calibrate_res));
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::choose_start_calc_size_time_s(int64_t &start_calc_size_time_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    // because statbuf.mtime_s_ is second level, maybe at or before start_calc_time_s has written some files,
    // ObDirCalcSizeOp statistics file size when statbuf.mtime_s_ < start_calc_size_time_s_
    // so when calibrate disk space, need sleep a while to ensure reading initial_stat_info before next second starts.
    // this ensures that when reading initial_stat_info (after this function returns), we try our best to be before next second,
    // providing a buffer to prefer over-estimation rather than under-estimation in calibration.
    const int64_t initial_time_us = ObTimeUtility::current_time_us();
    const int64_t initial_time_s = initial_time_us / 1000000;
    const int64_t initial_ms = (initial_time_us / 1000) % 1000;
    int64_t need_sleep_us = (1000 - initial_ms - 3) * 1000;
    if (need_sleep_us <= 0) {
      // it means initial_ms >= 997, need_sleep_us <= 0, no need to sleep.
    } else {
      ob_usleep(need_sleep_us);
    }
    const int64_t current_time_us = ObTimeUtility::current_time_us();
    const int64_t current_time_s = current_time_us / 1000000;
    const int64_t current_ms = (current_time_us / 1000) % 1000;
    // initial_time_s + [997ms, 999ms]
    if (current_time_s == initial_time_s && current_ms >= 997) {
      start_calc_size_time_s = initial_time_s + 1;
    } else {
      // Note: maybe there exists some time cost in 'ob_usleep' or 'ObTimeUtility::current_time_us()',
      // which leads to current_time out of expected range. it will automatically retry choose start_calc_size_time_s 10 times.
      ret = OB_EAGAIN;
      LOG_WARN("retry choose start_calc_size_time_s again", KR(ret), K(start_calc_size_time_s),
               K(initial_time_us), K(initial_time_s), K(current_time_s), K(current_ms), K(current_time_us), K(need_sleep_us));
    }
    FLOG_INFO("finish to choose start calc size time", KR(ret), K(initial_ms), K(need_sleep_us),
             K(initial_time_us), K(initial_time_s), K(current_time_us), K(current_time_s), K(start_calc_size_time_s), K(current_ms));
  }
  return ret;
}

/*
 * delete tenant_id dir's .tmp.seq files, for example SERVER_META.tmp.seq, TENANT_DISK_SPACE_META.tmp.seq.
 * if the server is killed when writing the .tmp.seq file, the rename function will not be called,
 * which results in leaving these .tmp.seq files.
 */
int ObCalibrateDiskSpaceTask::delete_tmp_seq_files(ObDirCalcSizeOp &del_tmp_seq_file_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    char tenant_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(tenant_path, sizeof(tenant_path), MTL_ID(), MTL_EPOCH_ID()))) {
      LOG_WARN("fail to get local tenant dir", KR(ret));
    } else if (OB_FAIL(del_tmp_seq_file_op.set_dir(tenant_path))) {
      LOG_WARN("fail to set dir", KR(ret), K(tenant_path));
    } else if (OB_FAIL(share::ObIODeviceLocalFileOp::scan_dir(tenant_path, del_tmp_seq_file_op))) {
      LOG_WARN("fail to scan dir", KR(ret), K(tenant_path));
    }
  }
  return ret;
}

/*
 delete tmp_data dir's .deleted files.
 because if it delete tmp file when calibrating disk space, rename the file to .deleted file.
 after calibrating disk space, list all tmp files and delete the .deleted file.
 */
int ObCalibrateDiskSpaceTask::rm_logical_deleted_file()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObRMLogicalDeletedFileOp rm_logical_deleted_file_op(is_stop_);
    ObDelTmpFileDirOp del_dir_op(is_stop_);
    char tmp_data_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path, sizeof(tmp_data_path), MTL_ID(), MTL_EPOCH_ID()))) {
      LOG_WARN("fail to get local tmp data dir", KR(ret));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir_rec(tmp_data_path, rm_logical_deleted_file_op, del_dir_op))) {
      LOG_WARN("fail to scan dir rec", KR(ret), K(tmp_data_path));
    } else {
      LOG_INFO("succ to rm logical deleted file", "total_file_count", rm_logical_deleted_file_op.get_total_file_count());
    }
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::get_macro_cache_used_size(ObFileStatInfo &meta_file_stat_info, ObFileStatInfo &tmp_file_stat_info,
                                                        ObFileStatInfo &macro_block_stat_info, ObFileStatInfo &hot_tablet_macro_block_stat_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFileStatInfo, SS_MACRO_CACHE_MAX_TYPE_VAL> cache_stat_info;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_FAIL(tnt_disk_space_mgr->get_macro_cache_stats(cache_stat_info))) {
    LOG_WARN("fail to get macro cache stats", KR(ret));
  } else {
    meta_file_stat_info = cache_stat_info.at(static_cast<uint8_t>(ObSSMacroCacheType::META_FILE));
    tmp_file_stat_info = cache_stat_info.at(static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE));
    macro_block_stat_info = cache_stat_info.at(static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK));
    hot_tablet_macro_block_stat_info = cache_stat_info.at(static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::get_macro_cache_used_size(
    const ObSSMacroCacheType cache_type,
    ObFileStatInfo &stat_info)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheStat cache_stat;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache type", KR(ret));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_FAIL(tnt_disk_space_mgr->get_macro_cache_stat(cache_type, cache_stat))) {
    LOG_WARN("fail to get macro cache stat", KR(ret));
  } else if (FALSE_IT(stat_info.total_size_ = cache_stat.used_)) {
  } else if (OB_FAIL(get_macro_cache_used_dir_info(cache_type, stat_info.dir_size_, stat_info.dir_num_))) {
    LOG_WARN("fail to get macro cache used dir info", KR(ret));
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::get_macro_cache_used_size(
    const ObSSMacroCacheType cache_type,
    int64_t &used_size)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheStat cache_stat;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache type", KR(ret));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_FAIL(tnt_disk_space_mgr->get_macro_cache_stat(cache_type, cache_stat))) {
    LOG_WARN("fail to get macro cache stat", KR(ret));
  } else {
    used_size = cache_stat.used_;
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::get_macro_cache_used_dir_info(
  const ObSSMacroCacheType cache_type,
  int64_t &dir_size,
  int64_t &dir_num)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheStat cache_stat;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro cache type", KR(ret));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (OB_FAIL(tnt_disk_space_mgr->get_dir_info(cache_type, dir_size, dir_num))) {
    LOG_WARN("fail to get dir info", KR(ret));
  }
  return ret;
}

} // storage
} // oceanbase
