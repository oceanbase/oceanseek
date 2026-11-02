/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_task_base.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

ObSSGCDagTaskBase::ObSSGCDagTaskBase(
    volatile bool &is_stopped,
    const int64_t dag_type,
    const char *submit_event_name)
  : ObSSGCTaskBase(is_stopped),
    dag_type_(dag_type),
    submit_event_name_(submit_event_name),
    dag_schedule_controller_(),
    last_period_commit_time_us_(0)
{
}

ObSSGCDagTaskBase::~ObSSGCDagTaskBase()
{
}

int ObSSGCDagTaskBase::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("gc dag task init twice", KR(ret), K_(tenant_id), K_(dag_type));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K_(dag_type));
  } else if (OB_ISNULL(submit_event_name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc dag submit event name is null", KR(ret), K(tenant_id), K_(dag_type));
  } else {
    tenant_id_ = tenant_id;
    dag_schedule_controller_.reset();
    last_period_commit_time_us_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSGCDagTaskBase::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule gc dag task", KR(ret), K_(tenant_id),
        K_(dag_type), K(tg_id));
  }
  return ret;
}

void ObSSGCDagTaskBase::stop()
{
  reset_dag_schedule_controller_();
}

void ObSSGCDagTaskBase::destroy()
{
  dag_schedule_controller_.reset();
  last_period_commit_time_us_ = 0;
  reset_task_state_();
}

void ObSSGCDagTaskBase::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id), K_(dag_type));
  } else {
    const ObSSGCDagScheduleController::State state = dag_schedule_controller_.state();
    switch (state) {
      case ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH: {
        bool exist = true;
        if (OB_FAIL(check_gc_dag_exist_(exist))) {
          LOG_WARN("fail to check gc dag exist, keep waiting", KR(ret),
              K_(tenant_id), K_(dag_type));
        } else if (exist) {
          // Keep waiting without consuming the period anchor.
        } else if (OB_FAIL(dag_schedule_controller_.switch_to_wait_for_next_schedule())) {
          LOG_WARN("fail to switch gc dag schedule state", KR(ret),
              K_(tenant_id), K_(dag_type), K(state));
        } else {
          LOG_INFO("gc dag has finished, wait for next timer tick",
              K_(tenant_id), K_(dag_type));
        }
        break;
      }
      case ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE: {
        if (OB_FAIL(try_schedule_gc_dag_())) {
          LOG_WARN("fail to try schedule gc dag", KR(ret),
              K_(tenant_id), K_(dag_type));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected gc dag schedule state", KR(ret), K(state),
            K_(tenant_id), K_(dag_type));
        break;
      }
    }
  }
}

int ObSSGCDagTaskBase::try_schedule_gc_dag_()
{
  int ret = OB_SUCCESS;
  bool need_schedule = false;
  int64_t check_ts = 0;
  const ObSSGCDagScheduleController::State state = dag_schedule_controller_.state();
  if (ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE
      != state) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("gc dag is not waiting for the next schedule", KR(ret),
        K_(tenant_id), K_(dag_type), K(state));
  } else if (OB_FAIL(check_need_schedule_dag_(need_schedule, check_ts))) {
    LOG_WARN("fail to check whether gc dag needs scheduling", KR(ret),
        K_(tenant_id), K_(dag_type));
  } else if (!need_schedule) {
    // do nothing
  } else {
    bool need_wait_for_dag_finish = false;
    bool dag_submitted = false;
    const int submit_ret = submit_gc_dag_(check_ts);
    switch (submit_ret) {
      case OB_SUCCESS: {
        need_wait_for_dag_finish = true;
        dag_submitted = true;
        LOG_INFO("succ to create and add gc dag", K_(tenant_id), K_(dag_type));
        break;
      }
      case OB_EAGAIN: {
        bool exist = false;
        const int probe_ret = check_gc_dag_exist_(exist);
        if (OB_SUCCESS != probe_ret) {
          ret = probe_ret;
          LOG_WARN("fail to confirm gc dag after eagain", K(submit_ret),
              K(probe_ret), K_(tenant_id), K_(dag_type));
        } else if (exist) {
          need_wait_for_dag_finish = true;
          LOG_INFO("confirmed that gc dag already exists", K(submit_ret),
              K_(tenant_id), K_(dag_type));
        } else {
          ret = submit_ret;
          LOG_WARN("gc dag is absent after eagain, retry next tick",
              K(submit_ret), K_(tenant_id), K_(dag_type));
        }
        break;
      }
      case OB_SIZE_OVERFLOW: {
        ret = submit_ret;
        LOG_WARN("tenant dag scheduler is full", K(submit_ret),
            K_(tenant_id), K_(dag_type));
        break;
      }
      default: {
        ret = submit_ret;
        LOG_WARN("fail to create and add gc dag", K(submit_ret),
            K_(tenant_id), K_(dag_type));
        break;
      }
    }

    if (OB_SUCC(ret) && need_wait_for_dag_finish) {
      if (OB_FAIL(dag_schedule_controller_.switch_to_wait_for_dag_finish())) {
        LOG_WARN("fail to switch gc dag schedule state", KR(ret),
            K_(tenant_id), K_(dag_type));
      } else {
        commit_gc_dag_schedule_time_(check_ts);
        if (dag_submitted) {
          SERVER_EVENT_ADD(
              "SHARED_GC", submit_event_name_,
              "tenant_id", tenant_id_,
              "schedule_ts", check_ts,
              "last_period_commit_time_us", last_period_commit_time_us_,
              "controller_state", static_cast<int64_t>(
                  dag_schedule_controller_.state()),
              "trace_id", ObCurTraceId::get_trace_id_str());
        }
      }
    }
  }
  return ret;
}

int ObSSGCDagTaskBase::check_can_schedule_gc_dag_common_(bool &can_schedule) const
{
  int ret = OB_SUCCESS;
  can_schedule = false;
  bool enable_gc_service = false;
  bool is_gc_sswriter = false;
  if (OB_FAIL(ObSSGCUtil::check_enable_gc_service(tenant_id_, enable_gc_service))) {
    LOG_WARN("fail to check enable gc service", KR(ret),
        K_(tenant_id), K_(dag_type));
  } else if (!enable_gc_service) {
    // do nothing
  } else if (OB_FAIL(check_is_gc_sswriter_(is_gc_sswriter))) {
    LOG_WARN("fail to check is gc sswriter", KR(ret),
        K_(tenant_id), K_(dag_type));
  } else {
    can_schedule = is_gc_sswriter;
  }
  return ret;
}

int64_t ObSSGCDagTaskBase::get_gc_dag_schedule_check_time_us_() const
{
  return ObTimeUtility::current_time_us();
}

int ObSSGCDagTaskBase::check_gc_dag_retry_state_(
    const int64_t check_ts,
    bool &has_pending_retry,
    bool &need_retry) const
{
  UNUSED(check_ts);
  has_pending_retry = false;
  need_retry = false;
  return OB_SUCCESS;
}

int ObSSGCDagTaskBase::check_need_schedule_dag_(
    bool &need_schedule,
    int64_t &check_ts)
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  check_ts = 0;
  ObCurTraceId::init(GCONF.self_addr_);
  bool can_schedule = false;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id), K_(dag_type));
  } else if (OB_FAIL(check_can_schedule_gc_dag_(can_schedule))) {
    LOG_WARN("fail to check whether gc dag can be scheduled", KR(ret),
        K_(tenant_id), K_(dag_type));
  } else if (!can_schedule) {
    // do nothing
  } else {
    int64_t exec_interval_us = DEFAULT_GC_EXEC_INTERVAL_US;
    bool has_pending_retry = false;
    bool need_retry = false;
    get_gc_exec_interval_us_(exec_interval_us);
    check_ts = get_gc_dag_schedule_check_time_us_();
    if (OB_FAIL(check_gc_dag_retry_state_(check_ts, has_pending_retry, need_retry))) {
      LOG_WARN("fail to check gc dag retry state", KR(ret), K_(tenant_id), K_(dag_type), K(check_ts));
    } else if (has_pending_retry) {
      need_schedule = need_retry;
    } else {
      need_schedule = (check_ts - last_period_commit_time_us_) >= exec_interval_us;
    }
  }
  return ret;
}

void ObSSGCDagTaskBase::commit_gc_dag_schedule_time_(const int64_t schedule_ts)
{
  last_period_commit_time_us_ = schedule_ts;
}

void ObSSGCDagTaskBase::reset_dag_schedule_controller_()
{
  dag_schedule_controller_.reset();
}

void ObSSGCDagTaskBase::commit_dag_schedule_start_time_(const int64_t start_ts)
{
  commit_gc_dag_schedule_time_(start_ts);
}

} // namespace storage
} // namespace oceanbase
