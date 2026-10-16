/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_calibration_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "share/ob_force_print_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

ObSSMicroCacheCalibrationOp::ObSSMicroCacheCalibrationOp()
  : ObSSMicroCacheBaseOp(), expected_valid_len_arr_()
{
}

void ObSSMicroCacheCalibrationOp::destroy()
{
  expected_valid_len_arr_.reset();
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSMicroCacheCalibrationOp::init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init base op", KR(ret), K(tenant_id));
  } else {
    op_type_ = ObSSMicroCacheTaskOpType::SS_CALIBRATION_OP;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

bool ObSSMicroCacheCalibrationOp::need_calibration(const uint64_t abnormal_flag) const
{
  return (abnormal_flag & ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK) != 0;
}

int ObSSMicroCacheCalibrationOp::execute()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (check_need_execute()) {
    execute_calibration_op(); // ignore ret, log is printed in execute_calibration_op
  }
  return ret;
}

int ObSSMicroCacheCalibrationOp::execute_calibration_op()
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret), K_(tenant_id));
  } else if (micro_cache->is_calibration_enabled()) {
    const uint64_t ori_abnormal_flag = cache_stat()->task_stat().get_abnormal_flag();
    if (OB_LIKELY(!need_calibration(ori_abnormal_flag))) {
    } else if (OB_UNLIKELY(!micro_cache->try_start_operating())) {
      if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
        LOG_INFO("fail to start operating, clear/resize/calibration already in progress", K_(tenant_id));
      }
    } else {
      const ObSSARCInfo &arc_info = micro_meta_mgr()->get_arc_info();
      const int64_t start_time_us = ObTimeUtility::current_time_us();
      LOG_INFO("start ss_micro_cache calibration", K_(tenant_id), K(ori_abnormal_flag), K(arc_info),
               KPC(cache_stat()));
      if (OB_FAIL(prepare_calibration())) {
        LOG_WARN("fail to prepare for calibration", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(do_calibration())) {
        LOG_WARN("fail to do calibration", KR(ret), K_(tenant_id));
      } else {
        finish_calibration(true/* calibration_succeeded */);
        FLOG_INFO("successfully finish ss_micro_cache calibration", K_(tenant_id),
                  "cost_time_us", ObTimeUtility::current_time_us() - start_time_us,
                  K(ori_abnormal_flag), K(arc_info), KPC(cache_stat()));
      }

      if (OB_FAIL(ret)) {
        finish_calibration(false/* calibration_succeeded */);
        FLOG_WARN("fail to do ss_micro_cache calibration", KR(ret), K_(tenant_id),
                  K(ori_abnormal_flag), K(arc_info), KPC(cache_stat()));
      }
    }
  }
  return ret;
}

int ObSSMicroCacheCalibrationOp::prepare_calibration()
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(micro_cache->start_calibration())) {
    LOG_WARN("fail to start calibration", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObSSMicroCacheCalibrationOp::finish_calibration(const bool calibration_succeeded)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    LOG_WARN("micro_cache is null", KP(micro_cache)); // ignore ret
  } else {
    micro_cache->finish_calibration(calibration_succeeded);
  }
}

int ObSSMicroCacheCalibrationOp::do_calibration()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  expected_valid_len_arr_.reset();
  if (OB_FAIL(OB_E(EventTable::EN_SS_MICRO_CACHE_CALIBRATION_FAIL_ALLOC_VALID_LEN_ALLOC) OB_SUCCESS)) {
    LOG_WARN("errsim: calibration fail when allocatin valid_len arr", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(expected_valid_len_arr_.prepare_allocate(phy_blk_mgr()->get_total_blk_cnt()))) {
    LOG_WARN("fail to prepare_allocate expected_valid_len", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(micro_range_mgr()->clear_all_sub_ranges())) {
    LOG_WARN("fail to clear all sub_ranges", KR(ret), K_(tenant_id));
    // Calculate sub_range_cnt only
    // fe_sub_range_cnt still the same as before (no logic depends on it, affects logging only)
    int64_t total_sub_range_cnt = 0;
    if (OB_TMP_FAIL(micro_range_mgr()->get_total_sub_range_cnt(total_sub_range_cnt))) {
      LOG_WARN("fail to get total sub range cnt", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    } else {
      cache_stat()->range_stat().set_sub_range_cnt(total_sub_range_cnt);
    }
  } else {
    reset_for_calibration();
    if (OB_FAIL(traverse_meta_and_rebuild())) {
      LOG_WARN("fail to traverse meta and rebuild", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(rebuild_cache_status())) {
      LOG_WARN("fail to rebuild cache status", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

void ObSSMicroCacheCalibrationOp::reset_for_calibration()
{
  // Safe to reset stats/ARC here.
  // Read requests may trigger ARC updates, but ARC adjustment requires write permission.
  // Calibration disables write first, so read path won't change ARC during this window.
  cache_stat()->reset_for_calibration();   // reset micro_stat, range_stat;
  micro_meta_mgr()->reset_arc_seg_info();  // T1/T2/B1/B2 segment state
}

int ObSSMicroCacheCalibrationOp::traverse_meta_and_rebuild()
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager *meta_mgr = micro_meta_mgr();
  if (OB_ISNULL(meta_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_meta_mgr is null", KR(ret), KP(meta_mgr));
  } else if (OB_FAIL(OB_E(EventTable::EN_SS_MICRO_CACHE_CALIBRATION_FAIL_TRAVERSE_META) OB_SUCCESS)) {
    LOG_WARN("errsim: calibration fail when traversing meta", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(meta_mgr->traverse_meta_for_calibration(expected_valid_len_arr_))) {
    // Only return error when allocate memory failed.
    // In such case, the calibration will be regarded as failed, 
    // and bg tasks and write operation will not be operated.
    // Such meta will be retried in the next round of calibration.
    LOG_WARN("fail to traverse meta for calibration", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMicroCacheCalibrationOp::rebuild_cache_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(phy_blk_mgr()) || OB_ISNULL(micro_meta_mgr()) || OB_ISNULL(micro_range_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), KP(phy_blk_mgr()), KP(micro_meta_mgr()), KP(micro_range_mgr()));
  } else if (OB_FAIL(phy_blk_mgr()->rebuild_phy_blk_valid_len(expected_valid_len_arr_))) {
    LOG_WARN("fail to rebuild phy_blk valid_len after calibration", KR(ret));
  } else {
    int64_t total_sub_range_cnt = 0;
    if (OB_TMP_FAIL(micro_meta_mgr()->update_micro_stat_from_arc())) {
      LOG_WARN("fail to recalc micro_stat after calibration", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }
    if (OB_TMP_FAIL(micro_range_mgr()->get_total_sub_range_cnt(total_sub_range_cnt))) {
      LOG_WARN("fail to get total sub range cnt", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    } else {
      cache_stat()->range_stat().set_sub_range_cnt(total_sub_range_cnt);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
