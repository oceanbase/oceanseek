/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/incremental/ob_sswriter_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

ObSSGCTaskBase::ObSSGCTaskBase(volatile bool &is_stopped)
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_stopped_(is_stopped),
    run_cnt_(0),
    last_exec_time_us_(0)
{
}

ObSSGCTaskBase::~ObSSGCTaskBase()
{
}

void ObSSGCTaskBase::stop()
{
}

int ObSSGCTaskBase::check_init_and_stop_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc task is not init", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss gc task runner is stopped", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSGCTaskBase::schedule_task_(
    ObTimerTask &task,
    const int tg_id,
    const int64_t interval_us,
    const bool repeat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((INVALID_TG_ID == tg_id) || (interval_us <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id), K(interval_us), K(repeat));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, task, interval_us, repeat))) {
    LOG_WARN("fail to schedule ss gc task", KR(ret), K(tg_id), K(interval_us), K(repeat));
  }
  return ret;
}

int ObSSGCTaskBase::start_task_(const int tg_id, const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc task is not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, interval_us, true/*repeat*/))) {
    LOG_WARN("fail to schedule ss gc task", KR(ret), K_(tenant_id), K(tg_id), K(interval_us));
  } else {
    // Wait until the configured gc interval elapses before the first real execution.
    last_exec_time_us_ = ObTimeUtility::current_time_us();
  }
  return ret;
}

int ObSSGCTaskBase::check_if_need_do_gc_work(bool &is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  ObCurTraceId::init(GCONF.self_addr_);
  bool enable_gc_service = false;
  bool is_gc_sswriter = false;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::check_enable_gc_service(tenant_id_, enable_gc_service))) {
    LOG_WARN("fail to check enable gc service", KR(ret), K_(tenant_id));
  } else if (!enable_gc_service) {
    // do nothing. is_need = false.
  } else if (OB_FAIL(check_is_gc_sswriter_(is_gc_sswriter))) {
    LOG_WARN("fail to check is gc sswriter", KR(ret), K_(tenant_id));
  } else if (!is_gc_sswriter) {
    // do nothing. is_need = false.
  } else { // enable_gc_service && is_gc_sswriter
    int64_t exec_interval_us = DEFAULT_GC_EXEC_INTERVAL_US;
    get_gc_exec_interval_us_(exec_interval_us);
    const int64_t current_time_us = ObTimeUtility::current_time_us();
    if ((current_time_us - last_exec_time_us_) >= exec_interval_us) {
      is_need = true;
      last_exec_time_us_ = current_time_us;
    }
  }
  return ret;
}

// No need to consider the leader-switch ABA problem (epoch).
// For mark sweep gc, the main control logic is implemented via the row lock on the
// __all_shared_gc_control table. the sswriter lease is auxiliary (it merely helps ensure that
// in most scenarios only one sswriter node contends for the executor privilege in the table).
// Other types of gc, such as fail gc, also do not need to consider ABA problem; just let the old
// leader (A with expired epoch) continue its work.
int ObSSGCTaskBase::check_is_gc_sswriter_(bool &is_gc_sswriter) const
{
  int ret = OB_SUCCESS;
  is_gc_sswriter = false;
  ObSSWriterKey sswriter_key(ObSSWriterType::GC);
  int64_t unused_epoch = 0;
  ObSSWriterService *sswriter_service = nullptr;
  if (OB_ISNULL(sswriter_service = MTL(ObSSWriterService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service should not be null", KR(ret));
  } else if (OB_FAIL(sswriter_service->check_lease(sswriter_key, is_gc_sswriter, unused_epoch))) {
    LOG_WARN("fail to check lease", KR(ret), K(sswriter_key));
  }
  return ret;
}

void ObSSGCTaskBase::reset_task_state_()
{
  last_exec_time_us_ = 0;
  run_cnt_ = 0;
  // is_stopped is a reference, which should be set by original owner and no need to be reset here
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

// A zero interval uses retention time / 7, capped at 8h.
// Use a fallback interval if the tenant configuration is unavailable.
void ObSSGCTaskBase::get_gc_exec_interval_us_(int64_t &exec_interval_us) const
{
  exec_interval_us = DEFAULT_GC_EXEC_INTERVAL_US;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    LOG_WARN_RET(OB_INVALID_CONFIG, "tenant config is invalid, use fallback gc interval",
        K_(tenant_id), K(exec_interval_us));
  } else {
    const int64_t configured_interval_us = tenant_config->_shared_gc_interval;
    if (0 == configured_interval_us) {
      const int64_t retention_time_us = GCONF._shared_gc_retention_time;
      // Note: in one round of mark sweep gc, some tablets may cannot be gc'ed (e.g., existing
      // flying/external tablet versions). thus, set a max auto interval (8h), so as to
      // do mark sweep gc three times every day and reduce the probability of not gc'ed tablets.
      const int64_t max_auto_interval_us = 8LL * 3600LL * 1000LL * 1000LL; // 8h
      exec_interval_us = MIN(retention_time_us / 7, max_auto_interval_us);
    } else {
      exec_interval_us = configured_interval_us;
    }
  }
}

} // namespace storage
} // namespace oceanbase
