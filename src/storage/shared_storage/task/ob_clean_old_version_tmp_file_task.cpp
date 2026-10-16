/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/task/ob_clean_old_version_tmp_file_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "observer/omt/ob_multi_tenant.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCleanOldVersionTmpFileTask::ObCleanOldVersionTmpFileTask()
  : is_inited_(false), retry_count_(0), segment_file_mgr_(nullptr)
{
}

int ObCleanOldVersionTmpFileTask::init(ObSegmentFileManager *segment_file_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCleanOldVersionTmpFileTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(segment_file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSegmentFileManager is NULL", KR(ret), KP(segment_file_mgr));
  } else {
    segment_file_mgr_ = segment_file_mgr;
    is_inited_ = true;
    LOG_INFO("succ to init clean old version tmp file task");
  }
  return ret;
}

int ObCleanOldVersionTmpFileTask::start()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("clean old version tmp file task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US, false/*run once*/))) {
    LOG_WARN("fail to schedule ObCleanOldVersionTmpFileTask", KR(ret));
  } else {
    LOG_INFO("succ to start clean old version tmp file task");
  }
  return ret;
}

void ObCleanOldVersionTmpFileTask::stop()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObCleanOldVersionTmpFileTask stop finished");
  }
}

void ObCleanOldVersionTmpFileTask::wait()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObCleanOldVersionTmpFileTask wait finished");
  }
}

void ObCleanOldVersionTmpFileTask::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  retry_count_ = 0;
  segment_file_mgr_ = nullptr;
}

void ObCleanOldVersionTmpFileTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(segment_file_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment file manager is null", KR(ret), KP(segment_file_mgr_));
  } else {
    retry_count_++;
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    if (OB_FAIL(segment_file_mgr_->clean_old_version_tmp_files())) {
      LOG_WARN("fail to clean old version tmp files, will retry", KR(ret), K_(retry_count));
      
      // Schedule retry
      omt::ObSharedTimer *timer = nullptr;
      if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
        LOG_ERROR("ObSharedTimer is NULL, cannot schedule retry", KP(timer));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US, false/*run once*/))) {
          LOG_ERROR("fail to schedule retry for clean old version tmp files", KR(tmp_ret), K_(retry_count));
        } else {
          LOG_INFO("scheduled retry for cleaning old version tmp files", K_(retry_count));
        }
      }
    } else {
      const int64_t cost_time_us = ObTimeUtility::current_time_us() - start_time_us;
      FLOG_INFO("successfully cleaned old version tmp files, task completed", 
                K(cost_time_us), K_(retry_count));
    }
  }
}

} // storage
} // oceanbase

