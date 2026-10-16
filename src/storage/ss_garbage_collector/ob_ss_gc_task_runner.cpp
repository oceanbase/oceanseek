/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_task_runner.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread/thread_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_force_print_log.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSGCTaskRunner::ObSSGCTaskRunner()
  : is_inited_(false),
    is_stopped_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    low_tg_id_(INVALID_TG_ID),
    mid_tg_id_(INVALID_TG_ID),
    mark_sweep_gc_gen_task_(is_stopped_),
    fail_gc_gen_task_(is_stopped_),
    data_check_gen_task_(is_stopped_),
    ls_meta_gc_task_(is_stopped_),
    ckpt_gc_task_(is_stopped_),
    lock_row_gc_task_(is_stopped_),
    mark_sweep_gc_exec_task_(is_stopped_),
    fail_gc_exec_task_(is_stopped_),
    history_cleanup_task_(is_stopped_),
    tablet_study_info_cleanup_task_(is_stopped_),
    mark_sweep_gc_diagnose_cleanup_task_(is_stopped_),
    mark_sweep_gc_diagnose_record_cleanup_task_(is_stopped_)
{
}

ObSSGCTaskRunner::~ObSSGCTaskRunner()
{
  destroy();
}

int ObSSGCTaskRunner::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss gc task runner init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mark_sweep_gc_gen_task_.init(tenant_id))) {
    LOG_WARN("fail to init mark sweep gc gen task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fail_gc_gen_task_.init(tenant_id))) {
    LOG_WARN("fail to init fail gc gen task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(data_check_gen_task_.init(tenant_id))) {
    LOG_WARN("fail to init data check gen task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_meta_gc_task_.init(tenant_id))) {
    LOG_WARN("fail to init ls meta gc task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ckpt_gc_task_.init(tenant_id))) {
    LOG_WARN("fail to init ckpt gc task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(lock_row_gc_task_.init(tenant_id))) {
    LOG_WARN("fail to init lock row gc task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mark_sweep_gc_exec_task_.init(tenant_id))) {
    LOG_WARN("fail to init mark sweep gc exec task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fail_gc_exec_task_.init(tenant_id))) {
    LOG_WARN("fail to init fail gc exec task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(history_cleanup_task_.init(tenant_id))) {
    LOG_WARN("fail to init gc history cleanup task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_study_info_cleanup_task_.init(tenant_id))) {
    LOG_WARN("fail to init tablet study info cleanup task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mark_sweep_gc_diagnose_cleanup_task_.init(tenant_id))) {
    LOG_WARN("fail to init mark sweep gc diagnose cleanup task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mark_sweep_gc_diagnose_record_cleanup_task_.init(tenant_id))) {
    LOG_WARN("fail to init mark sweep gc diagnose record cleanup task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSGCLTimer, low_tg_id_))) {
    LOG_WARN("fail to create ss gc low priority timer", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSGCMTimer, mid_tg_id_))) {
    LOG_WARN("fail to create ss gc middle priority timer", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSGCTaskRunner::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc task runner is not init", KR(ret));
  } else {
    ATOMIC_STORE(&is_stopped_, true);
    fail_gc_gen_task_.reset_dag_schedule_controller_();
    data_check_gen_task_.reset_dag_schedule_controller_();
    ls_meta_gc_task_.reset_dag_schedule_controller_();
    lock_row_gc_task_.reset_dag_schedule_controller_();
    if (OB_FAIL(TG_START(low_tg_id_))) {
      LOG_WARN("fail to start ss gc timer", KR(ret), K_(low_tg_id));
    } else if (OB_FAIL(TG_START(mid_tg_id_))) {
      LOG_WARN("fail to start ss gc timer", KR(ret), K_(mid_tg_id));
    } else if (OB_FAIL(mark_sweep_gc_gen_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start mark sweep gc gen task", KR(ret), K_(low_tg_id));
    } else if (OB_FAIL(fail_gc_gen_task_.start(mid_tg_id_))) {
      LOG_WARN("fail to start fail gc gen task", KR(ret), K_(mid_tg_id));
    } else if (OB_FAIL(data_check_gen_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start data check gen task", KR(ret), K_(low_tg_id));
    } else if (OB_FAIL(ls_meta_gc_task_.start(mid_tg_id_))) {
      LOG_WARN("fail to start ls meta gc task", KR(ret), K_(tenant_id), K_(mid_tg_id));
    } else if (OB_FAIL(start_ckpt_gc_task_())) {
      LOG_WARN("fail to start ckpt gc task", KR(ret), K_(tenant_id), K_(mid_tg_id));
    } else if (OB_FAIL(lock_row_gc_task_.start(mid_tg_id_))) {
      LOG_WARN("fail to start lock row gc task", KR(ret), K_(tenant_id), K_(mid_tg_id));
    } else if (OB_FAIL(mark_sweep_gc_exec_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start mark sweep gc exec task", KR(ret), K_(tenant_id), K_(low_tg_id));
    } else if (OB_FAIL(fail_gc_exec_task_.start(mid_tg_id_))) {
      LOG_WARN("fail to start fail gc exec task", KR(ret), K_(tenant_id), K_(mid_tg_id));
    } else if (OB_FAIL(history_cleanup_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start gc history cleanup task", KR(ret), K_(tenant_id), K_(low_tg_id));
    } else if (OB_FAIL(tablet_study_info_cleanup_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start tablet study info cleanup task", KR(ret),
          K_(tenant_id), K_(low_tg_id));
    } else if (OB_FAIL(mark_sweep_gc_diagnose_cleanup_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start mark sweep gc diagnose cleanup task", KR(ret), K_(tenant_id), K_(low_tg_id));
    } else if (OB_FAIL(mark_sweep_gc_diagnose_record_cleanup_task_.start(low_tg_id_))) {
      LOG_WARN("fail to start mark sweep gc diagnose record cleanup task",
          KR(ret), K_(tenant_id), K_(low_tg_id));
    } else {
      const int64_t start_commit_ts = ObTimeUtility::current_time_us();
      fail_gc_gen_task_.commit_dag_schedule_start_time_(start_commit_ts);
      data_check_gen_task_.commit_dag_schedule_start_time_(start_commit_ts);
      ls_meta_gc_task_.commit_dag_schedule_start_time_(start_commit_ts);
      lock_row_gc_task_.commit_dag_schedule_start_time_(start_commit_ts);
      ATOMIC_STORE(&is_stopped_, false);
      FLOG_INFO("succ to start ss gc task runner", K_(tenant_id), K_(low_tg_id), K_(mid_tg_id));
    }
    if (OB_FAIL(ret)) {
      const int start_ret = ret;
      stop();
      ret = start_ret;
    }
  }
  return ret;
}

int ObSSGCTaskRunner::start_ckpt_gc_task_()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id_)) {
    if (OB_FAIL(ckpt_gc_task_.start(mid_tg_id_))) {
      LOG_WARN("fail to start ckpt gc task", KR(ret), K_(tenant_id), K_(mid_tg_id));
    }
  } else {
    FLOG_INFO("skip start ckpt gc task for non-sys tenant", K_(tenant_id));
  }
  return ret;
}

void ObSSGCTaskRunner::stop()
{
  FLOG_INFO("begin to stop ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
  ATOMIC_STORE(&is_stopped_, true);
  stop_timer_groups_();
  // 先等待定时任务退出，避免 stop 钩子重置 runTimerTask() 正在访问的状态。
  wait_timer_groups_();
  mark_sweep_gc_gen_task_.stop();
  fail_gc_gen_task_.stop();
  data_check_gen_task_.stop();
  ls_meta_gc_task_.stop();
  ckpt_gc_task_.stop();
  lock_row_gc_task_.stop();
  mark_sweep_gc_exec_task_.stop();
  fail_gc_exec_task_.stop();
  history_cleanup_task_.stop();
  tablet_study_info_cleanup_task_.stop();
  mark_sweep_gc_diagnose_cleanup_task_.stop();
  mark_sweep_gc_diagnose_record_cleanup_task_.stop();
  FLOG_INFO("finish to stop ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
}

void ObSSGCTaskRunner::wait()
{
  FLOG_INFO("begin to wait ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
  wait_timer_groups_();
  FLOG_INFO("finish to wait ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
}

void ObSSGCTaskRunner::destroy()
{
  FLOG_INFO("begin to destroy ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
  ATOMIC_STORE(&is_stopped_, true);
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_DESTROY(mid_tg_id_);
    mid_tg_id_ = INVALID_TG_ID;
  }
  if (INVALID_TG_ID != low_tg_id_) {
    TG_DESTROY(low_tg_id_);
    low_tg_id_ = INVALID_TG_ID;
  }
  mark_sweep_gc_diagnose_record_cleanup_task_.destroy();
  mark_sweep_gc_diagnose_cleanup_task_.destroy();
  tablet_study_info_cleanup_task_.destroy();
  history_cleanup_task_.destroy();
  fail_gc_exec_task_.destroy();
  mark_sweep_gc_exec_task_.destroy();
  lock_row_gc_task_.destroy();
  ckpt_gc_task_.destroy();
  ls_meta_gc_task_.destroy();
  data_check_gen_task_.destroy();
  fail_gc_gen_task_.destroy();
  mark_sweep_gc_gen_task_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
  FLOG_INFO("finish to destroy ss gc task runner", KP(this), K_(tenant_id),
      K_(low_tg_id), K_(mid_tg_id));
}

void ObSSGCTaskRunner::stop_timer_groups_()
{
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_STOP(mid_tg_id_);
  }
  if (INVALID_TG_ID != low_tg_id_) {
    TG_STOP(low_tg_id_);
  }
}

void ObSSGCTaskRunner::wait_timer_groups_()
{
  if (INVALID_TG_ID != mid_tg_id_) {
    TG_WAIT(mid_tg_id_);
  }
  if (INVALID_TG_ID != low_tg_id_) {
    TG_WAIT(low_tg_id_);
  }
}

bool ObSSGCTaskRunner::need_retry_mark_sweep_dag() const
{
  return mark_sweep_gc_gen_task_.need_retry_mark_sweep_dag();
}

void ObSSGCTaskRunner::set_need_retry_mark_sweep_dag(const bool need_retry)
{
  mark_sweep_gc_gen_task_.set_need_retry_mark_sweep_dag(need_retry);
}

void ObSSGCTaskRunner::report_data_check_dag_result(
    const bool need_retry,
    const int64_t finish_ts)
{
  data_check_gen_task_.report_data_check_dag_result(need_retry, finish_ts);
}

} // namespace storage
} // namespace oceanbase
