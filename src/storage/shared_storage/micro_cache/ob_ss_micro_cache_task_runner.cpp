/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/time/ob_time_utility.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::share;
using namespace oceanbase::common;

ObSSMicroCacheTaskRunner::ObSSMicroCacheTaskRunner()
    : is_inited_(false),
      is_stopped_(true),
      high_prio_tg_id_(INVALID_TG_ID),
      mid_prio_tg_id_(INVALID_TG_ID),
      low_prio_tg_id_(INVALID_TG_ID),
      task_ctx_(),
      persist_data_task_(*this),
      persist_meta_task_(*this),
      release_cache_task_(*this),
      blk_ckpt_task_(*this),
      monitor_task_(*this),
      calibration_task_(*this)
{}

int ObSSMicroCacheTaskRunner::init(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    ObSSMemBlockManager &mem_blk_mgr, 
    ObSSMicroMetaManager &micro_meta_mgr,
    ObSSMicroRangeManager &micro_range_mgr,
    ObSSPhysicalBlockManager &phy_blk_mgr, 
    ObTenantFileManager &tnt_file_mgr,
    ObSSMicroCacheStat &cache_stat,
    ObSSLAMicroKeyManager &la_micro_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(task_ctx_.init(allocator, mem_blk_mgr, micro_meta_mgr, micro_range_mgr,
             phy_blk_mgr, tnt_file_mgr, cache_stat, la_micro_mgr))) {
    LOG_WARN("fail to init task_ctx", KR(ret));
  } else if (OB_FAIL(init_task(tenant_id))) {
    LOG_WARN("fail to init task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_timer_thread())) {
    LOG_WARN("fail to init timer thread", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::init_task(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persist_data_task_.init(tenant_id, SS_PERSIST_DATA_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init persist micro data task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(persist_meta_task_.init(tenant_id, SS_PERSIST_META_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init persist micro meta task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(release_cache_task_.init(tenant_id, SS_RELEASE_CACHE_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init release cache task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(blk_ckpt_task_.init(tenant_id, SS_DO_BLK_CKPT_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init do blk_checkpoint task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(monitor_task_.init(tenant_id, SS_MONITOR_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init monitor task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(calibration_task_.init(tenant_id, SS_CALIBRATION_TASK_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init calibration task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::init_timer_thread()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheHTimer, high_prio_tg_id_))) {
    LOG_WARN("fail to create high priority thread", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheMTimer, mid_prio_tg_id_))) {
    LOG_WARN("fail to create mid priority thread", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheLTimer, low_prio_tg_id_))) {
    LOG_WARN("fail to create low priority thread", KR(ret));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    is_stopped_ = false;
    if (OB_FAIL(start_timer_thread())) {
      LOG_WARN("fail to start timer thread", KR(ret));
    } else if (OB_FAIL(schedule_persist_data_task(SS_PERSIST_DATA_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule persist_micro_data task", KR(ret), K_(high_prio_tg_id));
    } else if (OB_FAIL(schedule_do_blk_ckpt_task(SS_DO_BLK_CKPT_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule do_blk_ckpt task", KR(ret), K_(mid_prio_tg_id));
    } else if (OB_FAIL(schedule_release_cache_task(SS_RELEASE_CACHE_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule release_cache task", KR(ret), K_(low_prio_tg_id));
    } else if (OB_FAIL(schedule_persist_meta_task(SS_PERSIST_META_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule persist_meta task", KR(ret), K_(low_prio_tg_id));
    } else if (OB_FAIL(schedule_monitor_task(SS_MONITOR_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule monitor task", KR(ret), K_(mid_prio_tg_id));
    } else if (OB_FAIL(schedule_calibration_task(SS_CALIBRATION_TASK_INTERVAL_US))) {
      LOG_WARN("fail to schedule calibration task", KR(ret), K_(low_prio_tg_id));
    } else {
      LOG_INFO("ObSSMicroCacheTaskRunner start succ");
    }
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::start_timer_thread()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_START(high_prio_tg_id_))) {
    LOG_WARN("fail to start high priority timer", KR(ret), K_(high_prio_tg_id));
  } else if (OB_FAIL(TG_START(mid_prio_tg_id_))) {
    LOG_WARN("fail to start mid priority timer", KR(ret), K_(mid_prio_tg_id));
  } else if (OB_FAIL(TG_START(low_prio_tg_id_))) {
    LOG_WARN("fail to start low priority timer", KR(ret), K_(low_prio_tg_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_persist_data_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(high_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(high_prio_tg_id_, persist_data_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule persist micro data task", KR(ret), K_(high_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule persist micro data task", K_(high_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_persist_meta_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(low_prio_tg_id_, persist_meta_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule persist micro meta task", KR(ret), K_(low_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule persist micro meta task", K_(low_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_do_blk_ckpt_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(mid_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(mid_prio_tg_id_, blk_ckpt_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule blk_checkpoint task", KR(ret), K_(mid_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule blk_checkpoint task", K_(mid_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_release_cache_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(low_prio_tg_id_, release_cache_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule release cache task", KR(ret), K_(low_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule release cache task", K_(low_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_monitor_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(mid_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(mid_prio_tg_id_, monitor_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule monitor task", KR(ret), K_(mid_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule monitor task", K_(mid_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_calibration_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(low_prio_tg_id_, calibration_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule calibration task", KR(ret), K_(low_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule calibration task", K_(low_prio_tg_id), K(interval_us));
  }
  return ret;
}

void ObSSMicroCacheTaskRunner::disable_common_bg_tasks()
{
  disable_blk_ckpt();
  disable_persist_data();
  disable_release_cache();
  disable_monitor();
}

void ObSSMicroCacheTaskRunner::enable_common_bg_tasks()
{
  enable_blk_ckpt();
  enable_persist_data();
  enable_release_cache();
  enable_monitor();
}

int ObSSMicroCacheTaskRunner::wait_tasks_close_for_calibration(const int64_t exp_timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t actual_timeout_us = (exp_timeout_us > 0) ? exp_timeout_us : SS_CALIBRATION_WAIT_TIMEOUT_US;
  const int64_t start_time_us = ObTimeUtil::current_time_us();
  bool need_wait = true;
  while (need_wait && (ObTimeUtil::current_time_us() - start_time_us < actual_timeout_us)) {
    // Don't wait for meta_ckpt and release_cache
    // because they share the same thread group with calibration task
    // - they won't be scheduled when calibration runs, so waiting would deadlock.
    if (is_persist_data_closed() && is_blk_ckpt_closed() && is_monitor_closed()) {
      need_wait = false;
    } else {
      ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
        LOG_INFO("wait for tasks close for calibration",
            K(is_persist_data_closed()), K(is_blk_ckpt_closed()), K(is_monitor_closed()), K_(tenant_id));
      }
    }
  }
  if (need_wait) {
    ret = OB_TIMEOUT;
    LOG_WARN("wait tasks close for calibration timeout", KR(ret), K_(tenant_id), K(actual_timeout_us));
  }
  return ret;
}

void ObSSMicroCacheTaskRunner::stop()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to stop ss_micro_cache task runner", KP(this));
  if (!ATOMIC_LOAD(&is_stopped_)) {
    ATOMIC_STORE(&is_stopped_, true);
    if (INVALID_TG_ID != low_prio_tg_id_) {
      TG_STOP(low_prio_tg_id_);
    }
    if (INVALID_TG_ID != mid_prio_tg_id_) {
      TG_STOP(mid_prio_tg_id_);
    }
    if (INVALID_TG_ID != high_prio_tg_id_) {
      TG_STOP(high_prio_tg_id_);
    }
    FLOG_INFO("finish to stop ss_micro_cache task runner", KP(this));
  }
}

void ObSSMicroCacheTaskRunner::wait()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to wait ss_micro_cache task runner", KP(this));
  if (INVALID_TG_ID != low_prio_tg_id_) {
    TG_WAIT(low_prio_tg_id_);
  }
  if (INVALID_TG_ID != mid_prio_tg_id_) {
    TG_WAIT(mid_prio_tg_id_);
  }
  if (INVALID_TG_ID != high_prio_tg_id_) {
    TG_WAIT(high_prio_tg_id_);
  }
  FLOG_INFO("finish to wait ss_micro_cache task runner", KP(this));
}

void ObSSMicroCacheTaskRunner::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to destroy ss_micro_cache task runner", KP(this));
  if (INVALID_TG_ID != low_prio_tg_id_) {
    TG_DESTROY(low_prio_tg_id_);
  }
  if (INVALID_TG_ID != mid_prio_tg_id_) {
    TG_DESTROY(mid_prio_tg_id_);
  }
  if (INVALID_TG_ID != high_prio_tg_id_) {
    TG_DESTROY(high_prio_tg_id_);
  }
  persist_data_task_.destroy();
  persist_meta_task_.destroy();
  blk_ckpt_task_.destroy();
  monitor_task_.destroy();
  calibration_task_.destroy();
  release_cache_task_.destroy();
  task_ctx_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_stopped_ = true;
  is_inited_ = false;
  FLOG_INFO("finish to destroy ss_micro_cache task runner", KP(this));
}

} // storage
} // oceanbase
