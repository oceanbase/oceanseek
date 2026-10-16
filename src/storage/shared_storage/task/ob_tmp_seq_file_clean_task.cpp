/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/task/ob_tmp_seq_file_clean_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_file_manager.h"
 
namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
ObTmpSeqFileCleanTask::ObTmpSeqFileCleanTask()
  : is_inited_(false)
{}
 
ObTmpSeqFileCleanTask::~ObTmpSeqFileCleanTask()
{
  destroy();
}
 
int ObTmpSeqFileCleanTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tmp seq file clean task has already been inited", KR(ret));
  } else {
    is_inited_ = true;
  }
  LOG_INFO("finish to init tmp seq file clean task", KR(ret));
  return ret;
}
 
int ObTmpSeqFileCleanTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp seq file clean task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule tmp seq file clean task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to schedule tmp seq file clean task", KR(ret), K(tg_id));
  }
  return ret;
}
 
void ObTmpSeqFileCleanTask::destroy()
{
  is_inited_ = false;
}
 
void ObTmpSeqFileCleanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  const int64_t CLEAN_MAP_SAFE_DEL_TIME_S = 5L; // 5s
  const int64_t MAP_SAFE_DEL_TIME_S = 5L * 60L; // 5min
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp seq file clean task is not inited", KR(ret));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager should not be null", KR(ret));
  } else if (OB_FAIL(file_manager->do_tmp_seq_file_clean_work(true/*is_clean*/, CLEAN_MAP_SAFE_DEL_TIME_S))) {
    LOG_WARN("fail to do tmp seq file map clean work", KR(ret));
  } else if (OB_FAIL(file_manager->do_tmp_seq_file_clean_work(false/*is_clean*/, MAP_SAFE_DEL_TIME_S))) {
    LOG_WARN("fail to do tmp seq file map clean work", KR(ret));
  }
}
 
} // storage
} // oceanbase

