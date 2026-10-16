/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_gen_task.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "logservice/ob_log_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

ObSSDataCheckGenTaskOptions::ObSSDataCheckGenTaskOptions()
  : memory_limit_(0),
    file_buf_size_(0),
    io_timeout_us_(-1)
{
}

int ObSSDataCheckGenTaskOptions::init(
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check gen task options initialized twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || (memory_limit / file_buf_size - 1) / 2 < 2 || io_timeout_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check gen task options", KR(ret),
        K(memory_limit), K(file_buf_size), K(io_timeout_us));
  } else {
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_timeout_us_ = io_timeout_us;
  }
  return ret;
}

bool ObSSDataCheckGenTaskOptions::is_valid() const
{
  return memory_limit_ >= ObExternalSortConstant::MIN_MEMORY_LIMIT
      && ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size_)
      && (memory_limit_ / file_buf_size_ - 1) / 2 >= 2 && io_timeout_us_ >= 0;
}

void ObSSDataCheckGenTaskOptions::reset()
{
  memory_limit_ = 0;
  file_buf_size_ = 0;
  io_timeout_us_ = -1;
}

ObSSDataCheckGenTask::ObSSDataCheckGenTask(volatile bool &is_stopped)
  : ObSSGCDagTaskBase(
      is_stopped,
      static_cast<int64_t>(ObSSDataCheckDag::DAG_TYPE),
      "SUBMIT_DATA_CHECK_DAG"),
    options_(),
    retry_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK),
    retry_not_before_us_(0),
    retry_count_(0),
    pending_check_mode_(ObSSDataCheckMode::MAX)
{
}

ObSSDataCheckGenTask::~ObSSDataCheckGenTask()
{
  destroy();
}

int ObSSDataCheckGenTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckGenTaskOptions options;
  if (OB_FAIL(options.init(ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT,
      ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE, 0 /*io_timeout_us*/))) {
    LOG_WARN("failed to initialize default data check options", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init(tenant_id, options))) {
    LOG_WARN("failed to initialize data check gen task with default options",
        KR(ret), K(tenant_id), K(options));
  }
  return ret;
}

int ObSSDataCheckGenTask::init(
    const uint64_t tenant_id,
    const ObSSDataCheckGenTaskOptions &options)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!options.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check gen task options", KR(ret), K(tenant_id), K(options));
  } else if (OB_FAIL(ObSSGCDagTaskBase::init(tenant_id))) {
    LOG_WARN("failed to initialize data check gen task", KR(ret), K(tenant_id));
  } else {
    options_ = options;
  }
  return ret;
}

int ObSSDataCheckGenTask::start(const int tg_id)
{
  return ObSSGCDagTaskBase::start(tg_id);
}

void ObSSDataCheckGenTask::destroy()
{
  reset_data_check_dag_retry_();
  pending_check_mode_ = ObSSDataCheckMode::MAX;
  options_.reset();
  ObSSGCDagTaskBase::destroy();
}

void ObSSDataCheckGenTask::runTimerTask()
{
  ObSSGCDagTaskBase::runTimerTask();
}

void ObSSDataCheckGenTask::report_data_check_dag_result(
    const bool need_retry,
    const int64_t finish_ts)
{
  int64_t exec_interval_us = DEFAULT_EXEC_INTERVAL_US;
  get_gc_exec_interval_us_(exec_interval_us);
  if (OB_UNLIKELY(finish_ts < 0 || exec_interval_us <= 0)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid data check dag result feedback",
        K(need_retry), K(finish_ts), K(exec_interval_us), K_(tenant_id));
  } else {
    int64_t retry_count = 0;
    int64_t retry_interval_us = 0;
    int64_t retry_not_before_us = 0;
    bool is_ignored = false;
    {
      ObSpinLockGuard guard(retry_lock_);
      if (ATOMIC_LOAD(&is_stopped_)) {
        is_ignored = true;
      } else if (!need_retry) {
        retry_not_before_us_ = 0;
        retry_count_ = 0;
      } else {
        if (retry_count_ < MAX_ACCELERATED_RETRY_COUNT + 1) {
          ++retry_count_;
        }
        retry_interval_us = get_retry_interval_locked_(exec_interval_us);
        retry_not_before_us_ = add_interval_safely_(finish_ts, retry_interval_us);
        retry_count = retry_count_;
        retry_not_before_us = retry_not_before_us_;
      }
    }
    if (is_ignored) {
      LOG_INFO("ignore data check dag result after task runner stopped",
          K(need_retry), K(finish_ts), K_(tenant_id));
    } else if (need_retry) {
      LOG_WARN_RET(OB_EAGAIN, "data check dag will retry after execution failure",
          K_(tenant_id), K(retry_count), K(retry_interval_us), K(retry_not_before_us));
    }
  }
}

int ObSSDataCheckGenTask::check_can_schedule_gc_dag_(
    bool &can_schedule) const
{
  return check_sys_ls_strong_leader_(can_schedule);
}

int ObSSDataCheckGenTask::check_gc_dag_retry_state_(
    const int64_t check_ts,
    bool &has_pending_retry,
    bool &need_retry) const
{
  int ret = OB_SUCCESS;
  has_pending_retry = false;
  need_retry = false;
  if (OB_UNLIKELY(check_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check retry timestamp", KR(ret), K(check_ts));
  } else {
    ObSpinLockGuard guard(retry_lock_);
    has_pending_retry = retry_not_before_us_ > 0;
    need_retry = has_pending_retry && check_ts >= retry_not_before_us_;
  }
  return ret;
}

int ObSSDataCheckGenTask::check_need_schedule_dag_(
    bool &need_schedule,
    int64_t &check_ts)
{
  int ret = OB_SUCCESS;
  pending_check_mode_ = ObSSDataCheckMode::MAX;
  if (OB_FAIL(ObSSGCDagTaskBase::check_need_schedule_dag_(need_schedule, check_ts))) {
    LOG_WARN("failed to check whether data check dag needs scheduling", KR(ret), K_(tenant_id));
  } else if (need_schedule) {
    ObSSDataCheckMode check_mode = ObSSDataCheckMode::MAX;
    if (OB_FAIL(get_data_check_mode_(check_mode))) {
      LOG_WARN("failed to get shared storage data check mode", KR(ret), K_(tenant_id));
    } else if (ObSSDataCheckMode::OFF == check_mode) {
      need_schedule = false;
    } else if (OB_UNLIKELY(!ObSSDataCheckModeUtil::is_enabled(check_mode))) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid enabled shared storage data check mode", KR(ret), K_(tenant_id), K(check_mode));
    } else {
      // pending_check_mode_ 仅在本次串行定时任务调用栈内传递给提交阶段。
      pending_check_mode_ = check_mode;
    }
  }
  return ret;
}

int ObSSDataCheckGenTask::check_sys_ls_strong_leader_(bool &is_leader) const
{
  int ret = OB_SUCCESS;
  is_leader = false;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
  ObRole role = INVALID_ROLE;
  int64_t unused_proposal_id = -1;
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant log service is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(log_service->get_palf_role(SYS_LS, role, unused_proposal_id))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get SYS_LS role", KR(ret), K_(tenant_id));
    }
  } else {
    is_leader = is_strong_leader(role);
  }
  return ret;
}

int ObSSDataCheckGenTask::build_dag_param_(
    const int64_t check_ts,
    const share::SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode,
    ObSSDataCheckDagParam &param) const
{
  int ret = OB_SUCCESS;
  int64_t io_expire_timestamp = 0;
  if (OB_UNLIKELY(check_ts < 0 || !snapshot_meta_scn.is_valid()
      || !ObSSDataCheckModeUtil::is_enabled(check_mode) || !options_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check dag parameter source", KR(ret),
        K(check_ts), K(snapshot_meta_scn), K(check_mode), K_(options));
  } else {
    if (options_.io_timeout_us_ > 0) {
      io_expire_timestamp = check_ts > INT64_MAX - options_.io_timeout_us_ ? INT64_MAX
          : check_ts + options_.io_timeout_us_;
    }
    if (OB_FAIL(param.init(tenant_id_, snapshot_meta_scn, check_mode, options_.memory_limit_,
        options_.file_buf_size_, io_expire_timestamp))) {
      LOG_WARN("failed to initialize data check dag param", KR(ret),
          K_(tenant_id), K(check_ts), K(check_mode), K(io_expire_timestamp), K_(options));
    }
  }
  return ret;
}

int ObSSDataCheckGenTask::get_data_check_mode_(
    ObSSDataCheckMode &check_mode) const
{
  int ret = OB_SUCCESS;
  check_mode = ObSSDataCheckMode::MAX;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("tenant config is invalid", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSDataCheckModeUtil::parse(
      tenant_config->_shared_storage_data_check_mode.get_value_string(), check_mode))) {
    LOG_WARN("failed to parse shared storage data check mode", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSDataCheckGenTask::get_shared_meta_snapshot_(
    share::SCN &snapshot_meta_scn) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t stmt_timeout = 0;
  SCN tmp_snapshot_meta_scn;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObSharedMetaReservedMgr reserved_mgr;
  const ObSharedMetaReservedType data_check_type(ObSharedMetaReservedTypeEnum::DATA_CHECK);
  const ObString owner = ObString::make_string("SS_DATA_CHECK");
  snapshot_meta_scn.reset();
  if (OB_FAIL(reserved_mgr.init(tenant_id_))) {
    LOG_WARN("failed to initialize data check reserved scn manager", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("failed to get data check reserved scn transaction timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
      tenant_id_, stmt_timeout, false /*with_snapshot*/, share::OBCG_STORAGE, timeout_ctx, trans))) {
    LOG_WARN("failed to start data check reserved scn transaction",
        KR(ret), K_(tenant_id), K(stmt_timeout));
  } else {
    // 同一事务内 insert + delete 不残留 DATA_CHECK 行，但会更新 MAX_RESERVED 行记录的最大 meta_scn。
    if (OB_FAIL(reserved_mgr.create_reserved_scn(
        trans, data_check_type, owner, tmp_snapshot_meta_scn))) {
      LOG_WARN("failed to create data check reserved scn", KR(ret), K_(tenant_id), K(owner));
    } else if (OB_FAIL(reserved_mgr.remove_reserved_scn(trans, data_check_type, owner))) {
      LOG_WARN("failed to remove data check reserved scn",
          KR(ret), K_(tenant_id), K(owner), K(tmp_snapshot_meta_scn));
    }

    const bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      LOG_WARN("failed to end data check reserved scn transaction",
          KR(tmp_ret), KR(ret), K(commit), K_(tenant_id), K(tmp_snapshot_meta_scn));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!tmp_snapshot_meta_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data check reservation returned invalid snapshot",
        KR(ret), K_(tenant_id), K(tmp_snapshot_meta_scn));
  } else {
    snapshot_meta_scn = tmp_snapshot_meta_scn;
  }
  return ret;
}

int ObSSDataCheckGenTask::submit_gc_dag_(const int64_t check_ts)
{
  int ret = OB_SUCCESS;
  share::SCN snapshot_meta_scn;
  const ObSSDataCheckMode check_mode = pending_check_mode_;
  ObSSDataCheckDagParam param;
  pending_check_mode_ = ObSSDataCheckMode::MAX;
  if (OB_UNLIKELY(!ObSSDataCheckModeUtil::is_enabled(check_mode))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check mode was not admitted before dag submission",
        KR(ret), K_(tenant_id), K(check_mode));
  } else if (OB_FAIL(check_init_and_stop_())) {
    LOG_INFO("skip data check dag submission after task runner stopped", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_shared_meta_snapshot_(snapshot_meta_scn))) {
    LOG_WARN("failed to freeze shared meta snapshot", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(build_dag_param_(check_ts, snapshot_meta_scn, check_mode, param))) {
    LOG_WARN("failed to build data check dag param", KR(ret),
        K_(tenant_id), K(check_ts), K(snapshot_meta_scn), K(check_mode));
  } else if (OB_FAIL(check_init_and_stop_())) {
    LOG_INFO("skip data check dag submission after task runner stopped", KR(ret), K_(tenant_id));
  } else {
    ++run_cnt_;
    ret = submit_data_check_dag_(param);
  }
  return ret;
}

void ObSSDataCheckGenTask::reset_data_check_dag_retry_()
{
  ObSpinLockGuard guard(retry_lock_);
  retry_not_before_us_ = 0;
  retry_count_ = 0;
}

int64_t ObSSDataCheckGenTask::get_retry_interval_locked_(
    const int64_t exec_interval_us) const
{
  int64_t retry_interval_us = exec_interval_us;
  if (retry_count_ <= MAX_ACCELERATED_RETRY_COUNT) {
    const int64_t retry_shift = retry_count_ > 0 ? retry_count_ - 1 : 0;
    retry_interval_us = INITIAL_RETRY_INTERVAL_US << retry_shift;
    retry_interval_us = MIN(retry_interval_us, exec_interval_us);
  }
  return retry_interval_us;
}

int64_t ObSSDataCheckGenTask::add_interval_safely_(
    const int64_t base_ts,
    const int64_t interval_us)
{
  return base_ts > INT64_MAX - interval_us ? INT64_MAX : base_ts + interval_us;
}

int ObSSDataCheckGenTask::submit_data_check_dag_(
    const ObSSDataCheckDagParam &param)
{
  return submit_typed_gc_dag_<ObSSDataCheckDag>(param);
}

int ObSSDataCheckGenTask::check_gc_dag_exist_(bool &exist)
{
  return check_data_check_dag_exist_(exist);
}

int ObSSDataCheckGenTask::check_data_check_dag_exist_(bool &exist)
{
  return check_typed_gc_dag_exist_<ObSSDataCheckDag>(exist);
}

void ObSSDataCheckGenTask::get_gc_exec_interval_us_(
    int64_t &exec_interval_us) const
{
  exec_interval_us = DEFAULT_EXEC_INTERVAL_US;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    LOG_WARN_RET(OB_INVALID_CONFIG,
        "tenant config is invalid, fallback to default data check interval",
        K_(tenant_id), K(exec_interval_us));
  } else {
    exec_interval_us = tenant_config->_shared_storage_data_check_interval;
  }
}

void ObSSDataCheckGenTask::reset_dag_schedule_controller_()
{
  ObSSGCDagTaskBase::reset_dag_schedule_controller_();
}

void ObSSDataCheckGenTask::commit_dag_schedule_start_time_(
    const int64_t start_ts)
{
  ObSSGCDagTaskBase::commit_dag_schedule_start_time_(start_ts);
}

} // namespace storage
} // namespace oceanbase
