/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_gc_history_cleanup_task.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_server_struct.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"

namespace oceanbase
{
namespace storage
{

ObSSGCHistoryCleanupTask::ObSSGCHistoryCleanupTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSGCHistoryCleanupTask::~ObSSGCHistoryCleanupTask()
{
  destroy();
}

int ObSSGCHistoryCleanupTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("gc history cleanup task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSGCHistoryCleanupTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule gc history cleanup task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSGCHistoryCleanupTask::destroy()
{
  reset_task_state_();
}

void ObSSGCHistoryCleanupTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_need = false;
  if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do gc history cleanup work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_cleanup_())) {
    LOG_WARN("fail to do gc history cleanup", KR(ret), K_(tenant_id));
  }
}

int ObSSGCHistoryCleanupTask::do_cleanup_()
{
  int ret = OB_SUCCESS;
  // Step 1: read retention config
  int64_t retention_us = DEFAULT_RETENTION_US;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN("tenant config invalid, use default retention",
               K_(tenant_id), K(retention_us));
    } else {
      retention_us = tenant_config->_shared_gc_task_history_expiration_time;
    }
  }

  if (OB_SUCC(ret)) {
    // Step 2: calculate expiration threshold
    const int64_t now_us       = ObTimeUtility::current_time();
    const int64_t expire_ts_us = now_us - retention_us;

    // Step 3: reuse the primary-key range sweep and recheck expiration in DELETE.
    if (OB_FAIL(ObSharedGCTaskTableOperator::batch_delete_history(
        tenant_id_, is_stopped_, expire_ts_us))) {
      LOG_WARN("history cleanup tick failed", KR(ret), K_(tenant_id), K(expire_ts_us));
    } else {
      LOG_INFO("history cleanup tick finished", K_(tenant_id), K(expire_ts_us));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
