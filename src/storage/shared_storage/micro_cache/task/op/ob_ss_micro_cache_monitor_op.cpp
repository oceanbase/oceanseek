/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_monitor_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------SSMicroCacheMonitorCtx------------------------------------------*/
void SSMicroCacheMonitorCtx::reset()
{
  pre_add_cnt_ = 0;
  cur_add_cnt_ = 0;
  pre_fail_add_cnt_ = 0;
  cur_fail_add_cnt_ = 0;
  write_stuck_cnt_ = 0;
  last_check_time_us_ = 0;
  prev_check_config_time_us_ = 0;
  prev_update_load_time_us_ = 0;
  prev_print_stat_time_us_ = 0;
  print_stat_interval_us_ = SS_PRINT_CACHE_STAT_INTERVAL_US;
  prev_abnormal_log_time_us_ = 0;
}

void SSMicroCacheMonitorCtx::update_newest_add_info(const int64_t cur_add_cnt, const int64_t cur_fail_add_cnt)
{
  pre_add_cnt_ = cur_add_cnt_;
  pre_fail_add_cnt_ = cur_fail_add_cnt_;
  cur_add_cnt_ = cur_add_cnt;
  cur_fail_add_cnt_ = cur_fail_add_cnt;
}

bool SSMicroCacheMonitorCtx::exist_increament_io(const int64_t min_increament_cnt) const
{
  bool is_exist = false;
  if ((pre_add_cnt_ >= 0) && (cur_add_cnt_ > 0) && (cur_add_cnt_ - pre_add_cnt_ >= min_increament_cnt)) {
    is_exist = true;
  }
  return is_exist;
}

bool SSMicroCacheMonitorCtx::is_all_increament_io_failed() const
{
  const int64_t inc_add = cur_add_cnt_ - pre_add_cnt_;
  const int64_t inc_fail = cur_fail_add_cnt_ - pre_fail_add_cnt_;
  const int64_t diff = (inc_add >= inc_fail) ? (inc_add - inc_fail) : (inc_fail - inc_add);
  return (diff <= INCREMENT_ADD_FAIL_DELTA_TOLERANCE);
}

bool SSMicroCacheMonitorCtx::need_check_config() const
{
  return (ObTimeUtility::current_time_us() >= (prev_check_config_time_us_ + SS_CHECK_CONFIG_INTERVAL_US));
}

bool SSMicroCacheMonitorCtx::need_update_cache_load() const
{
  return (ObTimeUtility::current_time_us() >= (prev_update_load_time_us_ + SS_UPDATE_CACHE_LOAD_INTERVAL_US));
}

bool SSMicroCacheMonitorCtx::need_print_cache_stat() const
{
  return (ObTimeUtility::current_time_us() >= (prev_print_stat_time_us_ + print_stat_interval_us_));
}

bool SSMicroCacheMonitorCtx::need_print_abnormal_log() const
{
  return (ObTimeUtility::current_time_us() >= (prev_abnormal_log_time_us_ + SS_MONITOR_ABNORMAL_LOG_INTERVAL_US));
}

void SSMicroCacheMonitorCtx::finish_check_config()
{
  prev_check_config_time_us_ = ObTimeUtility::current_time_us();
}

void SSMicroCacheMonitorCtx::finish_update_cache_load()
{
  prev_update_load_time_us_ = ObTimeUtility::current_time_us();
}

void SSMicroCacheMonitorCtx::finish_print_cache_stat()
{
  prev_print_stat_time_us_ = ObTimeUtility::current_time_us();
}

void SSMicroCacheMonitorCtx::finish_print_abnormal_log()
{
  prev_abnormal_log_time_us_ = ObTimeUtility::current_time_us();
}

/*-----------------------------------------ObSSMicroCacheMonitorOp------------------------------------------*/
ObSSMicroCacheMonitorOp::ObSSMicroCacheMonitorOp()
  : ObSSMicroCacheBaseOp(), monitor_ctx_(), enable_update_arc_limit_(false)
{
}

void ObSSMicroCacheMonitorOp::destroy()
{
  monitor_ctx_.reset();
  enable_update_arc_limit_ = false;
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSMicroCacheMonitorOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("failed to init base op", KR(ret), K(tenant_id));
  } else {
    enable_update_arc_limit_ = true;
    op_type_ = ObSSMicroCacheTaskOpType::SS_MONITOR_OP;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObSSMicroCacheMonitorOp::execute()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (check_need_execute()) {
    execute_monitor_op(); // ignore ret, log is printed in execute_monitor_op
  }
  return ret;
}

int ObSSMicroCacheMonitorOp::execute_monitor_op()
{
  int ret = OB_SUCCESS;
  bool is_abnormal = false;
  const uint64_t initial_flag = cache_stat()->task_stat().get_abnormal_flag();
  if (OB_FAIL(check_write_abnormal(is_abnormal))) {
    LOG_WARN("failed to check write abnormal", KR(ret));
  } else if (OB_FAIL(check_arguments_abnormal(is_abnormal))) {
    LOG_WARN("failed to check arguments abnormal", KR(ret));
  } else if (OB_FAIL(check_task_abnormal(is_abnormal))) {
    LOG_WARN("failed to check task abnormal", KR(ret));
  }

  // Update diagnostic table and print status: only when successfully checked or detected abnormal
  if (OB_SUCC(ret) || is_abnormal) {
    report_micro_cache_status(is_abnormal, initial_flag);
  }
  // Periodic tasks: check_config, update_cache_load, print_cache_stat
  handle_periodic_extra_tasks();
  return ret;
}

int ObSSMicroCacheMonitorOp::check_write_abnormal(bool &is_abnormal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(task_ctx));
  } else {
    const int64_t now_us = ObTimeUtility::current_time_us();
    const int64_t last_check_time_us = monitor_ctx_.last_check_time_us_;
    if (last_check_time_us > 0 && (now_us - last_check_time_us < WRITE_STUCK_CHECK_INTERVAL_US)) {
      // Skip this round to enforce the check interval.
    } else {
      // Check if write is stuck
      monitor_ctx_.last_check_time_us_ = now_us;
      const int64_t cur_add_cnt = cache_stat()->io_stat().get_add_cnt();
      const int64_t cur_fail_add_cnt = cache_stat()->hit_stat().get_fail_add_cnt() + 
                                       cache_stat()->prewarm_stat().get_fail_add_cnt();
      monitor_ctx_.update_newest_add_info(cur_add_cnt, cur_fail_add_cnt);
      if (monitor_ctx_.exist_increament_io(10/*at least 10 add_micro_cache requests*/)) {
        if (monitor_ctx_.is_all_increament_io_failed()) {
          ++monitor_ctx_.write_stuck_cnt_;
        } else {
          monitor_ctx_.write_stuck_cnt_ = 0;
        }
      }

      if (monitor_ctx_.write_stuck_cnt_ >= MICRO_CACHE_ABNORMAL_WRITE_STUCK_CNT) {
        is_abnormal = true;
        cache_stat()->task_stat().add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_WRITE_STUCK);
        monitor_ctx_.write_stuck_cnt_ = 0;
      }
    }
  }
  return ret;
}

int ObSSMicroCacheMonitorOp::check_arguments_abnormal(bool &is_abnormal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(task_ctx));
  } else if (OB_FAIL(inner_check_arc_info(is_abnormal))) {
    LOG_WARN("failed to check ss_micro_cache arc_info", KR(ret));
  } else if (OB_FAIL(inner_check_cache_stat(is_abnormal))) {
    LOG_WARN("failed to check ss_micro_cache stat", KR(ret));
  }
  return ret;
}

int ObSSMicroCacheMonitorOp::inner_check_arc_info(bool &is_abnormal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(task_ctx));
  } else {
    const ObSSARCInfo &arc_info = task_ctx_->micro_meta_mgr_->get_arc_info();
    if (OB_UNLIKELY(!arc_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arc info", KR(ret), K(arc_info));
    } else if (arc_info.is_abnormal()) {
      is_abnormal = true;
      cache_stat()->task_stat().add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_INVALID_STAT);
    }
  }
  return ret;
}

int ObSSMicroCacheMonitorOp::inner_check_cache_stat(bool &is_abnormal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(task_ctx));
  } else if (cache_stat()->is_state_abnormal()) {
    is_abnormal = true;
    cache_stat()->task_stat().add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_INVALID_STAT);
  }  
  return ret;
}

int ObSSMicroCacheMonitorOp::check_task_abnormal(bool &is_abnormal)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(task_ctx));
  } else if (cache_stat()->task_stat().is_task_abnormal(TASK_STUCK_TIMEOUT_US)) {
    is_abnormal = true;
    cache_stat()->task_stat().add_abnormal_flag(ObSSMicroCacheTaskStat::FLAG_TASK_STUCK);
  }
  return ret;
}

void ObSSMicroCacheMonitorOp::report_micro_cache_status(const bool is_abnormal, const uint64_t initial_flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
  } else {
    ObSSLocalCacheModStatus mod_status = ObSSLocalCacheModStatus::INVALID_STATUS;
    char diagnose_info[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
    const char *extra_info = nullptr;

    if (is_abnormal) {
      if (monitor_ctx_.need_print_abnormal_log()) {
        const uint64_t abnormal_flag = cache_stat()->task_stat().get_abnormal_flag();
        const ObSSARCInfo &arc_info = task_ctx_->micro_meta_mgr_->get_arc_info();
        char stuck_tasks[128] = ""; // Used to store stuck task names
        if (cache_stat()->task_stat().is_task_abnormal(TASK_STUCK_TIMEOUT_US)) {
          cache_stat()->task_stat().get_stuck_task_names(TASK_STUCK_TIMEOUT_US, stuck_tasks, sizeof(stuck_tasks));
        }
        LOG_ERROR("ss_micro_cache is in abnormal status", K(abnormal_flag), K(initial_flag),
                  "write_stuck_cnt", monitor_ctx_.write_stuck_cnt_,
                  "stuck_tasks", stuck_tasks, K(arc_info), KPC(cache_stat()));
        task_ctx_->phy_blk_mgr_->check_all_blocks_length();
        monitor_ctx_.finish_print_abnormal_log();
      }
      
      mod_status = ObSSLocalCacheModStatus::ERROR_STATUS;
      const int64_t data_blk_cnt = cache_stat()->phy_blk_stat().data_blk_cnt_;
      const int64_t data_blk_used_cnt = cache_stat()->phy_blk_stat().data_blk_used_cnt_;
      const int64_t reorgan_free_cnt = cache_stat()->task_stat().reorgan_free_blk_cnt_;
      if (OB_TMP_FAIL(databuff_printf(diagnose_info, common::OB_DIAGNOSE_INFO_LENGTH,
          "data_blk_cnt:%ld, data_blk_used_cnt:%ld, reorgan_free_cnt: %ld",
          data_blk_cnt, data_blk_used_cnt, reorgan_free_cnt))) {
        LOG_WARN("fail to print diagnose_info", K(tmp_ret), K(data_blk_cnt), K(data_blk_used_cnt), K(reorgan_free_cnt));
        tmp_ret = OB_SUCCESS;
      }
      extra_info = "can't add new micro_block into cache for a long time";
    } else {
      mod_status = ObSSLocalCacheModStatus::NORMAL_STATUS;
      const int64_t valid_micro_cnt = cache_stat()->micro_stat().valid_micro_cnt_;
      const int64_t valid_micro_size = cache_stat()->micro_stat().valid_micro_size_;
      if (OB_TMP_FAIL(databuff_printf(diagnose_info, common::OB_DIAGNOSE_INFO_LENGTH,
          "valid_micro_cnt: %ld, valid_micro_size: %ld", valid_micro_cnt, valid_micro_size))) {
        LOG_WARN("fail to print diagnose_info", K(tmp_ret), K(valid_micro_cnt), K(valid_micro_size));
        tmp_ret = OB_SUCCESS;
      }
    }

    ADD_LOCAL_CACHE_DIAGNOSE(OB_SS_MICRO_CACHE_NAME, "micro_cache", mod_status, diagnose_info, extra_info);
  }
}

void ObSSMicroCacheMonitorOp::handle_periodic_extra_tasks()
{
  if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
  } else {
    // Every 2 sec: update config
    if (monitor_ctx_.need_check_config()) {
      task_ctx_->micro_meta_mgr_->update_cache_mem_limit_by_config();
      task_ctx_->micro_meta_mgr_->update_cache_expiration_time_by_config();
      task_ctx_->micro_meta_mgr_->update_max_micro_blk_size_by_config();
      bool succ_update_pct = false;
      task_ctx_->micro_meta_mgr_->update_arc_limit_pct_by_config(succ_update_pct);
      if (succ_update_pct) {
        task_ctx_->phy_blk_mgr_->get_and_update_cache_limit_size(true/*update blk_reorgan_pct*/);
      }
      monitor_ctx_.finish_check_config();
    }

    // Every 5 sec: update cache load
    if (monitor_ctx_.need_update_cache_load()) {
      task_ctx_->la_micro_mgr_->check_stop_record_la_micro_key();
      const int64_t interval_s = SS_UPDATE_CACHE_LOAD_INTERVAL_US / 1000000;
      cache_stat()->cache_load().update_common_io_load_info(cache_stat()->io_stat().common_io_param_, interval_s);
      cache_stat()->cache_load().update_prewarm_io_load_info(cache_stat()->io_stat().prewarm_io_param_, interval_s);
      cache_stat()->phy_blk_stat().set_reusable_block_cnt(task_ctx_->phy_blk_mgr_->get_reusable_blocks_cnt());
      cache_stat()->phy_blk_stat().set_sparse_block_cnt(task_ctx_->phy_blk_mgr_->get_sparse_block_cnt());
      monitor_ctx_.finish_update_cache_load();
    }

    // Every 60 sec: print cache stat
    if (monitor_ctx_.need_print_cache_stat()) {
      task_ctx_->phy_blk_mgr_->adjust_micro_meta_block();
      if (enable_update_arc_limit_) {
        task_ctx_->phy_blk_mgr_->get_and_update_cache_limit_size();
      }
      const ObSSARCInfo &arc_info = task_ctx_->micro_meta_mgr_->get_arc_info();
      const uint64_t micro_meta_map_cnt = task_ctx_->micro_meta_mgr_->get_micro_meta_map_cnt();
      monitor_ctx_.finish_print_cache_stat();
      LOG_INFO("ss_micro_cache stat", KPC(cache_stat()), K(arc_info), K(micro_meta_map_cnt));
    }
  }
}

} // namespace storage
} // namespace oceanbase