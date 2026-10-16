/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_io_detect_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/io/ob_io_manager.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

ObTenantIODetectTask::ObTenantIODetectTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(INVALID_TG_ID)
{
}

int ObTenantIODetectTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantIODetectTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init tenant io detect task", K(tenant_id));
  }
  return ret;
}

int ObTenantIODetectTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 1000L * 1000L; // 1s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant io detect task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObTenantIODetectTask", KR(ret), K_(tenant_id), K(tg_id));
  } else {
    LOG_INFO("succ to start tenant io detect task", K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObTenantIODetectTask::destroy()
{
  tg_id_ = INVALID_TG_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObTenantIODetectTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObTenantFileManager *tenant_file_mgr = nullptr;
    if (OB_ISNULL(tenant_file_mgr = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager is null", KR(ret), K(tenant_id_));
    } else {
      const int io_detect_fd = tenant_file_mgr->get_io_detect_file_fd();
      if (OB_INVALID_FD == io_detect_fd) {
        // skip, io detect file not created yet
      } else if (OB_FAIL(OB_IO_MANAGER.get_device_health_detector().record_timing_task(
          tenant_id_, ObIOFd::NORMAL_FILE_ID, io_detect_fd))) {
        LOG_WARN("fail to record timing task", KR(ret), K(tenant_id_), K(io_detect_fd));
      }
    }
  }
}

} // storage
} // oceanbase
