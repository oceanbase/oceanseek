/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/worker.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_cleanup_task.h"
#include "lib/allocator/page_arena.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_server_struct.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_DIAG_CLEANUP_INTERVAL);
#endif

ObSSMarkSweepDiagCleanupTask::ObSSMarkSweepDiagCleanupTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped)
{
}

ObSSMarkSweepDiagCleanupTask::~ObSSMarkSweepDiagCleanupTask()
{
  destroy();
}

int ObSSMarkSweepDiagCleanupTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mark sweep gc diagnose cleanup task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepDiagCleanupTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval_us = SCHEDULE_INTERVAL_US;
#ifdef ERRSIM
  // Allow interval injection after the tenant timer has already been scheduled.
  schedule_interval_us = ERRSIM_INTERVAL_US;
#endif
  if (OB_FAIL(start_task_(tg_id, schedule_interval_us))) {
    LOG_WARN("fail to schedule mark sweep gc diagnose cleanup task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSMarkSweepDiagCleanupTask::destroy()
{
  reset_task_state_();
}

void ObSSMarkSweepDiagCleanupTask::runTimerTask()
{
  lib::ErrsimModuleGuard errsim_guard(common::ObErrsimModuleType::ERRSIM_MODULE_SS_GC);
  int ret = OB_SUCCESS;
  bool is_need = false;
  if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do diagnose cleanup work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(SERVER_EVENT_ADD("SHARED_GC", "START_MARK_SWEEP_DIAG_CLEANUP",
      "tenant_id", tenant_id_))) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(do_cleanup_())) {
    LOG_WARN("fail to do mark sweep gc diagnose cleanup", KR(ret), K_(tenant_id));
  }
  if (is_need) {
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_DIAG_CLEANUP",
        "tenant_id", tenant_id_, "ret", ret);
  }
}

int ObSSMarkSweepDiagCleanupTask::do_cleanup_()
{
  int ret = OB_SUCCESS;
  bool has_min_round_id = false;
  int64_t min_round_id = -1;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_min_round_id_(has_min_round_id, min_round_id))) {
    LOG_WARN("fail to get min round id", KR(ret), K_(tenant_id));
  } else if (!has_min_round_id) {
    // No mark_sweep round known in either table -> conservatively skip this round.
    LOG_INFO("no mark sweep round id now, skip diagnose cleanup", K_(tenant_id));
  } else if (OB_FAIL(cleanup_stale_rounds_by_dir_(min_round_id))) {
    LOG_WARN("fail to cleanup stale diagnose round directories",
        KR(ret), K_(tenant_id), K(min_round_id));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("finish mark sweep gc diagnose cleanup", K_(tenant_id), K(min_round_id));
  }
  return ret;
}

int ObSSMarkSweepDiagCleanupTask::get_min_round_id_(
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
                 tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME,
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

int ObSSMarkSweepDiagCleanupTask::cleanup_stale_rounds_by_dir_(const int64_t min_round_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int first_fail_ret = OB_SUCCESS;
  ObTimePiece time_piece;
  char diag_root[common::MAX_PATH_SIZE] = {0};
  ObBackupDest storage_dest;
  uint64_t storage_id = OB_INVALID_ID;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagClean"));
  ObArray<ObString> round_dirs;
  ObFileListArrayOp list_op(round_dirs, allocator);
  ObBackupIoAdapter io_adapter;
  if (OB_FAIL(check_init_and_stop_())) {
    LOG_WARN("diagnose cleanup task is unavailable", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::build_diag_root_dir(
              tenant_id_, diag_root, sizeof(diag_root)))) {
    LOG_WARN("fail to build diag root dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::get_storage_dest_and_id(storage_dest, storage_id))) {
    LOG_WARN("fail to get storage dest and id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(io_adapter.list_directories(ObString(diag_root),
                     storage_dest.get_storage_info(), list_op))) {
    if (OB_OBJECT_NOT_EXIST == ret || OB_DIR_NOT_EXIST == ret
        || OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("diagnose root directory does not exist", K_(tenant_id), K(diag_root));
    } else {
      LOG_WARN("fail to list round directories", KR(ret), K(diag_root));
    }
  } else {
    time_piece.click();
    FLOG_INFO("start cleanup stale rounds", K_(tenant_id), K(min_round_id), "list_dir_cost_us",
        time_piece.last_fly_us(), "round_dirs_count", round_dirs.count());
    for (int64_t i = 0; !ATOMIC_LOAD(&is_stopped_) && (i < round_dirs.count()); ++i) {
      const ObString &name = round_dirs.at(i);
      int64_t round_id = -1;
      tmp_ret = OB_SUCCESS;
      // entry name looks like "round123"; parse the trailing round id.
      if (!name.prefix_match(ROUND_DIR_STR)) {
        // unexpected entry, skip.
        LOG_ERROR("skip non-round entry under diag root", K(name), K(diag_root));
      } else {
        const int64_t prefix_len = STRLEN(ROUND_DIR_STR);
        char num_buf[32] = {0};
        const int64_t num_len = name.length() - prefix_len;
        if (OB_UNLIKELY(num_len <= 0 || num_len >= static_cast<int64_t>(sizeof(num_buf)))) {
          LOG_INFO("skip round entry with invalid id length", K(name));
        } else {
          MEMCPY(num_buf, name.ptr() + prefix_len, num_len);
          num_buf[num_len] = '\0';
          char *endptr = nullptr;
          round_id = strtoll(num_buf, &endptr, 10/*base*/);
          if (OB_NOT_NULL(endptr) && ('\0' != *endptr)) {
            LOG_ERROR("skip round entry with non-numeric id", K(name));
          } else if (OB_UNLIKELY((round_id < 0) || (INT64_MAX == round_id))) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected round id", KR(tmp_ret), K(round_id), K(name));
          } else if (round_id < min_round_id) {
            char round_uri[common::MAX_PATH_SIZE] = {0};
            if (OB_TMP_FAIL(databuff_printf(round_uri, sizeof(round_uri), "%s/%.*s",
                    diag_root, name.length(), name.ptr()))) {
              LOG_WARN("fail to build round uri", KR(tmp_ret), K(name));
            } else if (OB_TMP_FAIL(io_adapter.del_dir(ObString(round_uri),
                       storage_dest.get_storage_info(), true /*recursive*/))) {
              if (OB_OBJECT_NOT_EXIST == tmp_ret || OB_DIR_NOT_EXIST == tmp_ret
                  || OB_BACKUP_FILE_NOT_EXIST == tmp_ret) {
                tmp_ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to del round dir", KR(tmp_ret), K(round_uri));
              }
            } else {
              time_piece.click();
              FLOG_INFO("delete stale mark sweep gc diagnose round dir",
                  K_(tenant_id), K(round_id), K(min_round_id), K(round_uri), "del_dir_cost_us",
                  time_piece.last_fly_us());
            }
          }
        }
      }
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == first_fail_ret) {
        first_fail_ret = tmp_ret;
      }
    }
  }
  if (OB_SUCC(ret) && OB_SUCCESS != first_fail_ret) {
    ret = first_fail_ret;
  }
  return ret;
}

void ObSSMarkSweepDiagCleanupTask::get_gc_exec_interval_us_(int64_t &exec_interval_us) const
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_DIAG_CLEANUP_INTERVAL, tenant_id_);
  exec_interval_us = OB_SUCC(ret) ? SCHEDULE_INTERVAL_US : ERRSIM_INTERVAL_US;
#else
  exec_interval_us = SCHEDULE_INTERVAL_US;
#endif
}

} // namespace storage
} // namespace oceanbase
