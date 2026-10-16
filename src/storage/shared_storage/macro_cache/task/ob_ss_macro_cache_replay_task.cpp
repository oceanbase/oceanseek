/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_replay_task.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_force_print_log.h"
#include "share/config/ob_server_config.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ObSSMacroCacheReplayTask::ObSSMacroCacheReplayTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(nullptr)
{
}

ObSSMacroCacheReplayTask::~ObSSMacroCacheReplayTask()
{
  destroy();
}

int ObSSMacroCacheReplayTask::init(ObSSMacroCacheMgr *macro_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init macro cache ckpt task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr));
  } else {
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("finish to init macro cache replay task", KR(ret));
  return ret;
}

int ObSSMacroCacheReplayTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 10 * 1000L; // 10ms
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache replay task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, INTERVAL_US, false/*repeat*/))) {
    LOG_WARN("fail to schedule macro cache replay task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to start macro cache replay task", K(tg_id));
  }
  return ret;
}

void ObSSMacroCacheReplayTask::destroy()
{
}

void ObSSMacroCacheReplayTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t next_ckpt_version = UINT64_MAX;
  ObCurTraceId::init(GCONF.self_addr_);
  const int64_t start_us = ObTimeUtility::current_time_us();
  FLOG_INFO("start to do macro cache replay task", KR(ret), KPC_(macro_cache_mgr));
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else {
    bool is_completed = false;
    int64_t retry_cnt = 0;
    int64_t MAX_RETRY_CNT = 10;
    // replay ckpt with retry
    while (!is_completed && (retry_cnt < MAX_RETRY_CNT) && !is_stopped_) {
      ret = OB_SUCCESS; // overwrite ret
      if (OB_FAIL(macro_cache_mgr_->clear_macro_cache_meta())) {
        LOG_WARN("fail to clear macro cache meta", KR(ret));
      } else if (OB_FAIL(macro_cache_mgr_->replay_ckpt_(next_ckpt_version))) {
        LOG_WARN("fail to replay ckpt", KR(ret));
      } else {
        // ERRSIM for test
        static int64_t errsim_fail_cnt = 0;
        if (errsim_fail_cnt < 3) {
          ret = OB_E(EventTable::EN_SHARED_STORAGE_MACRO_CACHE_REPLAY_CKPT_ERR) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            errsim_fail_cnt++;
            FLOG_INFO("ERRSIM EN_SHARED_STORAGE_MACRO_CACHE_REPLAY_CKPT_ERR", KR(ret));
          } else {
            is_completed = true;
          }
        } else {
          is_completed = true;
        }
      }
      if (OB_FAIL(ret)) {
        retry_cnt++;
      }
      FLOG_INFO("finish one round of macro cache replay task", KR(ret), K(next_ckpt_version), K(retry_cnt));
    }

    if (OB_UNLIKELY(UINT64_MAX == next_ckpt_version)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected next ckpt version");
    }
    macro_cache_mgr_->set_next_ckpt_version(next_ckpt_version); // set next_ckpt_version for ckpt_task
    macro_cache_mgr_->set_ckpt_replayed(); // set is_ckpt_replayed flag

    // when ckpt replayed, schedule one calibrate disk space task and one refresh policy task at once
    int tmp_ret = OB_SUCCESS;
    ObTenantFileManager *file_manager = nullptr;
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager is null", KR(tmp_ret));
    } else if (OB_TMP_FAIL(file_manager->schedule_calibrate_disk_space_task())) {
      LOG_WARN("fail to schedule calibrate disk space task", KR(tmp_ret));
    } else {
      FLOG_INFO("succ to schedule calibrate disk space task");
    }
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(tmp_ret));
    } else if (OB_TMP_FAIL(storage_cache_policy_service->schedule_refresh_policy_task())) {
      LOG_WARN("fail to schedule refresh policy task", KR(tmp_ret));
    } else {
      FLOG_INFO("succ to schedule refresh policy task");
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  if (OB_SUCC(ret)) {
    FLOG_INFO("succ to do macro cache replay task", KR(ret), KPC_(macro_cache_mgr),
              K(next_ckpt_version), K(cost_us));
  } else if (OB_SERVICE_STOPPED == ret) {
    FLOG_INFO("service stopped, skip macro cache replay task");
  } else {
    LOG_ERROR("fail to do macro cache replay task", KR(ret), KPC_(macro_cache_mgr),
              K(next_ckpt_version), K(cost_us));
  }
}

} // namespace storage
} // namespace oceanbase
