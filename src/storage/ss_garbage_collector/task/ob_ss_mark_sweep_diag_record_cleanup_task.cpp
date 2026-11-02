/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_record_cleanup_task.h"
#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_DIAG_RECORD_CLEANUP_INTERVAL);
#endif

ObSSMarkSweepDiagRecordCleanupTask::ObSSMarkSweepDiagRecordCleanupTask(
    volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSMarkSweepDiagRecordCleanupTask::~ObSSMarkSweepDiagRecordCleanupTask()
{
  destroy();
}

int ObSSMarkSweepDiagRecordCleanupTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mark sweep gc diagnose record cleanup task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepDiagRecordCleanupTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval_us = SCHEDULE_INTERVAL_US;
#ifdef ERRSIM
  // Allow interval injection after the tenant timer has already been scheduled.
  schedule_interval_us = ERRSIM_INTERVAL_US;
#endif
  if (OB_FAIL(start_task_(tg_id, schedule_interval_us))) {
    LOG_WARN("fail to schedule mark sweep gc diagnose record cleanup task",
        KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSMarkSweepDiagRecordCleanupTask::destroy()
{
  reset_task_state_();
}

void ObSSMarkSweepDiagRecordCleanupTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_need = false;
  ObTimePiece time_piece;
  if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do diagnose record cleanup work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(SERVER_EVENT_ADD("SHARED_GC", "START_MARK_SWEEP_DIAG_RECORD_CLEANUP",
      "tenant_id", tenant_id_,
      "trace_id", ObCurTraceId::get_trace_id_str()))) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_cleanup_())) {
    LOG_WARN("fail to do mark sweep gc diagnose record cleanup", KR(ret), K_(tenant_id));
  }

  if (is_need) {
    const int64_t cost_us = time_piece.fly_us();
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_DIAG_RECORD_CLEANUP",
        "tenant_id", tenant_id_, "ret", ret,
        "cost_us", cost_us, "trace_id", ObCurTraceId::get_trace_id_str());
    LOG_INFO("finish mark sweep gc diagnose record cleanup timer task",
        KR(ret), K_(tenant_id), K_(run_cnt), K(cost_us),
        "trace_id", ObCurTraceId::get_trace_id_str());
  }
}

int ObSSMarkSweepDiagRecordCleanupTask::do_cleanup_()
{
  int ret = OB_SUCCESS;
  bool has_min_round_id = false;
  int64_t min_round_id = -1;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose record cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (cluster_version < CLUSTER_VERSION_4_6_3_0) {
    LOG_INFO("skip mark sweep gc diagnose record cleanup, cluster version is less than 4.6.3.0",
        K_(tenant_id), K(cluster_version));
  } else if (OB_FAIL(get_min_round_id_(has_min_round_id, min_round_id))) {
    LOG_WARN("fail to get min round id", KR(ret), K_(tenant_id));
  } else if (!has_min_round_id) {
    LOG_INFO("no mark sweep round id now, skip diagnose record cleanup", K_(tenant_id));
  } else if (OB_FAIL(cleanup_stale_rounds_by_task_(min_round_id))) {
    LOG_WARN("fail to cleanup stale diagnose records", KR(ret), K_(tenant_id), K(min_round_id));
  }
  if (OB_SUCC(ret) && cluster_version >= CLUSTER_VERSION_4_6_3_0) {
    LOG_INFO("finish mark sweep gc diagnose record cleanup", K_(tenant_id), K(min_round_id));
  }
  return ret;
}

int ObSSMarkSweepDiagRecordCleanupTask::get_min_round_id_(
    bool &has_min_round_id,
    int64_t &min_round_id)
{
  int ret = OB_SUCCESS;
  has_min_round_id = false;
  min_round_id = -1;
  bool active_empty = true;
  bool history_empty = true;
  int64_t active_min = -1;
  int64_t history_min = -1;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_min_round_id(
                 tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC, OB_ALL_SHARED_GC_TASK_TNAME,
                 *GCTX.sql_proxy_, active_empty, active_min))) {
    LOG_WARN("fail to get min round id from active table", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_min_round_id(
                 tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC,
                 OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
                 *GCTX.sql_proxy_, history_empty, history_min))) {
    LOG_WARN("fail to get min round id from history table", KR(ret), K_(tenant_id));
  } else if (active_empty && history_empty) {
    has_min_round_id = false;
  } else {
    has_min_round_id = true;
    if (!active_empty && !history_empty) {
      min_round_id = MIN(active_min, history_min);
    } else if (!active_empty) {
      min_round_id = active_min;
    } else {
      min_round_id = history_min;
    }
  }
  return ret;
}

int ObSSMarkSweepDiagRecordCleanupTask::cleanup_stale_rounds_by_task_(
    const int64_t min_round_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int first_fail_ret = OB_SUCCESS;
  ObTimePiece time_piece;
  ObSharedGCTaskTypeFilter type_filter;
  ObSharedGCTaskRoundIdFilter round_filter;
  ObSharedGCTaskTableFullIterator iterator;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagClean"));
  ObSharedGCTask task;
  int64_t gmt_create = 0;
  int64_t gmt_modified = 0;
  type_filter.is_filter_enabled_ = true;
  round_filter.is_filter_enabled_ = true;
  round_filter.round_id_low_ = 1;
  round_filter.round_id_high_ = min_round_id - 1;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose record cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (min_round_id <= 1) {
    LOG_INFO("no stale diagnose record below min_round_id, skip",
        K_(tenant_id), K(min_round_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(type_filter.types_.push_back(ObSharedGCTaskType::MARK_SWEEP_GC_DIAG))) {
    LOG_WARN("fail to set diagnose task type filter", KR(ret));
  } else if (OB_FAIL(iterator.init(tenant_id_, OB_ALL_SHARED_GC_TASK_TNAME,
      type_filter, round_filter, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init diagnose task iterator", KR(ret), K_(tenant_id), K(min_round_id));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      task.reset();
      allocator.reuse();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("diagnose record cleanup task is stopped", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(iterator.get_next(allocator, task, gmt_create, gmt_modified))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to get diagnose cleanup task", KR(ret), K_(tenant_id));
        }
      } else if (OB_TMP_FAIL(cleanup_task_(task))) {
        // Keep scanning later markers and retain the failed marker for retry.
        LOG_WARN("fail to cleanup diagnose record, continue",
            KR(tmp_ret), K(task.key_), K(min_round_id));
        if (OB_SUCCESS == first_fail_ret) {
          first_fail_ret = tmp_ret;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_SUCCESS != first_fail_ret) {
    ret = first_fail_ret;
  }
  LOG_INFO("finish cleanup stale diagnose records", KR(ret), K_(tenant_id), K(min_round_id),
      "cost_us", time_piece.fly_us());
  return ret;
}

int ObSSMarkSweepDiagRecordCleanupTask::cleanup_task_(
    const ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose record cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!task.is_valid() || task.key_.tenant_id_ != tenant_id_
      || !is_mark_sweep_gc_diag_type(task.key_.type_)
      || task.key_.round_id_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid diagnose cleanup task", KR(ret), K(task.key_), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(delete_diag_dir_(task.key_.round_id_))) {
    LOG_WARN("fail to delete diagnose round directory, retain task", KR(ret), K(task.key_));
  } else if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose cleanup stopped, retain task", KR(ret), K(task.key_));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::delete_task(task.key_, *GCTX.sql_proxy_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete completed diagnose task", KR(ret), K(task.key_));
    }
  }
  return ret;
}

int ObSSMarkSweepDiagRecordCleanupTask::delete_diag_dir_(const int64_t round_id)
{
  int ret = OB_SUCCESS;
  ObBackupDest storage_dest;
  uint64_t storage_id = OB_INVALID_ID;
  char diag_root[MAX_PATH_SIZE] = {0};
  char round_uri[MAX_PATH_SIZE] = {0};
  ObBackupIoAdapter io_adapter;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose record cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (round_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid diagnose round id", KR(ret), K(round_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::get_storage_dest_and_id(storage_dest, storage_id))) {
    LOG_WARN("fail to get diagnose storage destination", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::build_diag_root_dir(
      tenant_id_, diag_root, sizeof(diag_root)))) {
    LOG_WARN("fail to build diagnose root directory", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(databuff_printf(
      round_uri, sizeof(round_uri), "%s/round%ld", diag_root, round_id))) {
    LOG_WARN("fail to build diagnose round directory", KR(ret), K_(tenant_id), K(round_id));
  } else if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose cleanup stopped before delete", KR(ret), K_(tenant_id), K(round_id));
  } else if (OB_FAIL(io_adapter.del_dir(
      ObString(round_uri), storage_dest.get_storage_info(), true/*recursive*/))) {
    if (OB_OBJECT_NOT_EXIST == ret || OB_DIR_NOT_EXIST == ret
        || OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete diagnose round directory", KR(ret), K_(tenant_id), K(round_id));
    }
  }
  return ret;
}

void ObSSMarkSweepDiagRecordCleanupTask::get_gc_exec_interval_us_(
    int64_t &exec_interval_us) const
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_DIAG_RECORD_CLEANUP_INTERVAL, tenant_id_);
  exec_interval_us = OB_SUCC(ret) ? SCHEDULE_INTERVAL_US : ERRSIM_INTERVAL_US;
#else
  exec_interval_us = SCHEDULE_INTERVAL_US;
#endif
}

} // namespace storage
} // namespace oceanbase
