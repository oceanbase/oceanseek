/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "ob_ss_ha_macro_block_task_mgr.h"
#include "rootserver/backup/ob_backup_task_scheduler.h"
#include "share/config/ob_server_config.h"
#include "rootserver/backup/ob_backup_service.h"
#include "rootserver/restore/ob_restore_service.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "rootserver/backup/ob_backup_schedule_task.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{

struct ObMacroIdWriteProcessor {
  explicit ObMacroIdWriteProcessor(backup::ObBackupBlockFileItemWriter &writer)
      : writer_(writer) {}
  int operator()(const share::ObSSHAMacroBlockBatch &batch)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
      const share::ObSSHAMacroBlockInfo &info = batch.at(i);
      backup::ObBackupBlockFileMacroIdItem item(info.macro_block_id_, info.occupy_size_);
      if (OB_FAIL(writer_.write_item(item))) {
        LOG_WARN("failed to write macro id item", KR(ret), K(info));
      }
    }
    return ret;
  }
  backup::ObBackupBlockFileItemWriter &writer_;
};

/* ObSSHAMacroTaskMgrStatus */

bool ObSSHAMacroTaskMgrStatus::is_valid() const
{
  return status_ >= Status::INIT && status_ < Status::MAX_STATUS;
}

const char* ObSSHAMacroTaskMgrStatus::get_str() const
{
  const char *str = "UNKNOWN";
  const char *status_strs[] = {
    "INIT",
    "RUNNING",
    "COMPLETE",
    "FAILED",
  };
  STATIC_ASSERT(MAX_STATUS == ARRAYSIZEOF(status_strs), "status count mismatch");
  if (status_ < INIT || status_ >= MAX_STATUS) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid ObSSHAMacroTaskMgrStatus status", K(status_));
  } else {
    str = status_strs[status_];
  }
  return str;
}

/* ObSSHAMacroBlockTaskMgr */

ObSSHAMacroBlockTaskMgr::ObSSHAMacroBlockTaskMgr()
  : is_inited_(false),
    is_finished_(false),
    is_failed_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    parent_task_id_(0),
    task_type_(),
    task_scheduler_(nullptr),
    sql_proxy_(nullptr),
    snapshot_(),
    total_macro_blocks_(0),
    finish_macro_blocks_(0),
    total_task_count_(0),
    tasks_in_scheduler_(0),
    finish_task_count_(0),
    total_bytes_(0),
    finish_bytes_(0),
    last_refill_time_(0),
    ls_id_array_()
{
}

ObSSHAMacroBlockTaskMgr::~ObSSHAMacroBlockTaskMgr()
{
}

int ObSSHAMacroBlockTaskMgr::get_backup_stats(share::ObBackupStats &backup_stats) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block task mgr not init", K(ret));
  } else if (!is_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("macro block task is not finished", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
  } else if (total_task_count_ < 0 || finish_task_count_ < 0
      || total_macro_blocks_ < 0 || finish_macro_blocks_ < 0
      || total_bytes_ < 0 || finish_bytes_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro block task statistics", K(ret), K_(total_task_count), K_(finish_task_count),
        K_(total_macro_blocks), K_(finish_macro_blocks), K_(total_bytes), K_(finish_bytes));
  } else {
    backup_stats.input_bytes_ = total_bytes_;
    backup_stats.output_bytes_ = finish_bytes_;
    backup_stats.finish_tablet_count_ = backup_stats.tablet_count_;
    backup_stats.macro_block_count_ = total_macro_blocks_;
    backup_stats.finish_macro_block_count_ = finish_macro_blocks_;
    // SS 一个宏块对应一个数据文件，文件数与宏块总数使用同一数据源。
    backup_stats.finish_file_count_ = total_macro_blocks_;
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::process(bool &is_finished, bool &is_failed)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  is_failed = false;  // is the macro block task failed and should not retry
  ObSSHAMacroTaskMgrStatus task_status;
  bool is_recovered_from_history = false;
  int64_t task_result = OB_SUCCESS;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block task mgr not init", K(ret));
  }
  
  if (FAILEDx(load_persistent_status_(task_status, is_recovered_from_history, task_result))) {
    LOG_WARN("failed to load persistent state", K(ret));
  } else if (is_recovered_from_history) {
    is_finished_ = true;
    if (task_status.is_failed()) {
      ret = task_result;
      is_failed_ = true;
      LOG_ERROR("recovered failed macro block task from history", K(ret),
          K_(tenant_id), K_(parent_task_id), K_(task_type));
    } else {
      LOG_INFO("recovered completed macro block task statistics from history",
          K_(tenant_id), K_(parent_task_id), K_(task_type), K_(total_task_count),
          K_(finish_task_count), K_(total_macro_blocks), K_(finish_macro_blocks),
          K_(total_bytes), K_(finish_bytes));
    }
  } else {
    FLOG_INFO("process macro block backup task", K(task_status), K(tenant_id_), K(parent_task_id_),
             K(total_task_count_), K(tasks_in_scheduler_), K(finish_task_count_));
    
    switch (task_status.status_) {
      case ObSSHAMacroTaskMgrStatus::INIT: {
        if (OB_FAIL(process_init_stage_())) {
          LOG_WARN("failed to process init stage", K(ret));
        }
        DEBUG_SYNC(HA_MACRO_TASK_STATUS_BEFORE_DOING);
        break;
      }
      
      case ObSSHAMacroTaskMgrStatus::RUNNING: {
        if (OB_FAIL(process_running_stage_())) {
          LOG_WARN("failed to process running stage", K(ret));
        }
        break;
      }
      
      case ObSSHAMacroTaskMgrStatus::COMPLETE: {
        if (OB_FAIL(process_complete_stage_())) {
          LOG_WARN("failed to process complete stage", K(ret));
        }
        break;
      }

      case ObSSHAMacroTaskMgrStatus::FAILED: {
        if (OB_FAIL(process_failed_stage_())) {
          LOG_WARN("failed to process failed stage", K(ret));
        }
        break;
      }
      
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stage", K(ret), K(task_status));
      }
    }
  }
  is_finished = is_finished_;
  is_failed = is_failed_;
  return ret;
}

ERRSIM_POINT_DEF(EN_SS_HA_MACRO_TASK_INIT_STAGE_FAILED);
int ObSSHAMacroBlockTaskMgr::process_init_stage_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_stage_delete_existing_tasks_())) {
    LOG_WARN("failed to delete existing tasks in init stage", K(ret));
  } else if (OB_FAIL(init_stage_generate_tasks_())) {
    LOG_WARN("failed to generate tasks in init stage", K(ret));
  } else if (OB_FAIL(EN_SS_HA_MACRO_TASK_INIT_STAGE_FAILED)) {
    LOG_WARN("errsim: backup init stage failed", K(ret));
  } else if (OB_FAIL(init_stage_write_macro_block_list_())) {
    LOG_WARN("failed to write macro block list in init stage", K(ret));
  } else if (OB_FAIL(init_stage_advance_status_())) {
    LOG_WARN("failed to advance status in init stage", K(ret));
  }

  if (OB_FAIL(ret) && !ObBackupUtils::is_need_retry_error(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(advance_init_status_to_failed_with_trans_(ret))) {
      LOG_WARN("failed to advance init status to failed", K(tmp_ret), K(ret));
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::batch_delete_tasks_with_trans_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::delete_tasks(
      *sql_proxy_, tenant_id_, parent_task_id_, task_type_, this))) {
    LOG_WARN("failed to batch delete tasks", KR(ret), K_(tenant_id),
        K_(parent_task_id), K_(task_type));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::init_stage_delete_existing_tasks_()
{
  int ret = OB_SUCCESS;

  // Add debug sync point before delete existing tasks
  ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_delete_existing_tasks",
                        "tenant_id", tenant_id_,
                        "parent_task_id", parent_task_id_);
  DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_DELETE_EXISTING_TASKS);

  if (OB_FAIL(lock_and_verify_state_(ObSSHAMacroTaskMgrStatus::INIT))) {
    LOG_WARN("failed to lock and verify state", KR(ret), K_(tenant_id), K_(parent_task_id));
  } else if (OB_FAIL(batch_delete_tasks_with_trans_())) {
    LOG_WARN("failed to batch delete existing tasks", K(ret), K_(tenant_id), K_(parent_task_id));
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::lock_and_verify_state_(const int64_t expected_status)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("start transaction failed", KR(ret), K_(tenant_id));
  } else {
    ObSSHAMacroTaskMgrState lock_mgr_state;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id_, parent_task_id_, task_type_, expected_status, lock_mgr_state))) {
      LOG_WARN("failed to load and verify mgr state for update", KR(ret), K(expected_status));
    }

    // End transaction
    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::init_stage_generate_tasks_()
{
  int ret = OB_SUCCESS;

  // Phase 2: Generate all macro block tasks (not in transaction, may take long time)
  if (OB_FAIL(generate_all_macro_tasks_to_table_())) {
    LOG_WARN("failed to generate all macro tasks to table", K(ret));
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::init_stage_advance_status_()
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  // Phase 3: Lock, verify state again, and advance status (in transaction)
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("start transaction failed", KR(ret), K_(tenant_id));
  } else {
    // 1. Lock the progress row and verify state again
    ObSSHAMacroTaskMgrState lock_mgr_state;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id_, parent_task_id_, task_type_, ObSSHAMacroTaskMgrStatus::INIT, lock_mgr_state))) {
      LOG_WARN("failed to load and verify mgr state for update", K(ret));
    }

    // 2. Advance status based on task count
    if (OB_SUCC(ret)) {
      ObSSHAMacroTaskMgrStatus next_status = (0 == total_task_count_) ?
          ObSSHAMacroTaskMgrStatus::COMPLETE : ObSSHAMacroTaskMgrStatus::RUNNING;
      // Add debug sync point before state transition
      if (next_status.is_running()) {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_init_to_running",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_INIT_TO_RUNNING);
      } else {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_init_to_complete",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_INIT_TO_COMPLETE);
      }
      if (OB_FAIL(advance_task_status_in_trans_(trans,
          ObSSHAMacroTaskMgrStatus::INIT, next_status, OB_SUCCESS))) {
        LOG_WARN("failed to advance task status", K(ret));
      } else {
        if (0 == total_task_count_) {
          LOG_INFO("no macro blocks to backup, mark COMPLETE");
        } else {
          LOG_INFO("generated all macro tasks, entering running stage",
                   K(total_task_count_), K(total_macro_blocks_));
        }
      }
    }

    // End transaction with check_leader
    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    } else {
      wakeup_service_();
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::process_running_stage_()
{
  int ret = OB_SUCCESS;

  // First read state without lock
  ObSSHAMacroTaskMgrState mgr_state;
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_mgr_state(
      *sql_proxy_, tenant_id_, parent_task_id_, task_type_, mgr_state))) {
    LOG_WARN("failed to load mgr state", K(ret));
  } else if (mgr_state.result_ != OB_SUCCESS) {
    // Found non-SUCCESS result, need transaction to advance to FAILED stage
    ObSSHAMacroTaskMgrState lock_state;
    if (OB_FAIL(advance_running_status_with_trans_(
        ObSSHAMacroTaskMgrStatus::FAILED,
        mgr_state.result_,
        &lock_state))) {
      LOG_WARN("failed to advance to FAILED", K(ret));
    } else {
      // Update local statistics from lock_state
      total_task_count_ = lock_state.total_task_count_;
      finish_task_count_ = lock_state.finish_task_count_;
      total_macro_blocks_ = lock_state.total_macro_block_count_;
      finish_macro_blocks_ = lock_state.finish_macro_block_count_;
      total_bytes_ = lock_state.total_bytes_;
      finish_bytes_ = lock_state.finish_bytes_;
      LOG_ERROR("macro block task failed", K(lock_state));
    }
  }
  // Sliding window task refill
  else if (OB_FAIL(refill_scheduler_if_needed_())) {
    LOG_WARN("failed to refill scheduler", K(ret));
  }
  // Check task completion status (updates in-memory counters from DB)
  else if (OB_FAIL(check_tasks_completion_())) {
    LOG_WARN("failed to check tasks completion", K(ret));
  } else if (is_all_tasks_finished_()) {
    // In-memory counters indicate all done, use check_all_tasks_finished as final verification
    bool all_finished = false;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
        *sql_proxy_, tenant_id_, parent_task_id_, task_type_, all_finished))) {
      LOG_WARN("failed to verify all tasks finished from task table", K(ret),
          K_(tenant_id), K_(parent_task_id), K_(task_type));
    } else if (!all_finished) {
      ret = OB_EAGAIN;
      LOG_WARN("in-memory counters indicate all finished but task table disagrees",
          K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type),
          K_(total_task_count), K_(finish_task_count));
    } else if (OB_FAIL(advance_running_status_with_trans_(
        ObSSHAMacroTaskMgrStatus::COMPLETE,
        OB_SUCCESS/*result*/,
        nullptr/*out_lock_state*/))) {
      LOG_WARN("failed to advance to COMPLETE", K(ret));
    } else {
      LOG_INFO("all macro block tasks completed",
              K(total_task_count_), K(finish_task_count_));
      wakeup_service_();
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::execute_cleanup_transaction_(const bool is_complete)
{
  int ret = OB_SUCCESS;
  const int64_t expected_status = is_complete ?
      ObSSHAMacroTaskMgrStatus::COMPLETE : ObSSHAMacroTaskMgrStatus::FAILED;

  // Step 1: Verify state before any destructive operation (with check_leader)
  if (OB_FAIL(lock_and_verify_state_(expected_status))) {
    LOG_WARN("failed to lock and verify state before cleanup", KR(ret), K_(tenant_id), K_(parent_task_id), K_(task_type), K(expected_status));
  // Step 2: Batch delete tasks (with check_leader per batch)
  } else if (OB_FAIL(batch_delete_tasks_with_trans_())) {
    LOG_WARN("failed to batch delete tasks", KR(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
  // Step 3: Lock, re-verify state, and move to history (with check_leader)
  } else if (OB_FAIL(execute_cleanup_move_to_history_(is_complete))) {
    LOG_WARN("failed to move mgr to history", KR(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::execute_cleanup_move_to_history_(const bool is_complete)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("start transaction failed", KR(ret), K_(tenant_id), K_(parent_task_id));
  } else {
    ObSSHAMacroTaskMgrState lock_state;
    const ObSSHAMacroTaskMgrStatus::Status expected_status = is_complete ?
        ObSSHAMacroTaskMgrStatus::COMPLETE : ObSSHAMacroTaskMgrStatus::FAILED;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id_, parent_task_id_, task_type_, expected_status, lock_state))) {
      LOG_WARN("failed to load and verify mgr state for update", K(ret));
    } else {
      if (is_complete) {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_complete_cleanup",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_COMPLETE_CLEANUP);
      } else {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_failed_cleanup",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_FAILED_CLEANUP);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::move_mgr_to_his(trans, tenant_id_, parent_task_id_, task_type_))) {
      LOG_WARN("failed to move job to his", K(ret), K_(tenant_id), K_(parent_task_id));
    } else {
      if (is_complete) {
        LOG_INFO("macro block backup completed");
      } else {
        LOG_ERROR("macro block backup failed", K(total_task_count_), K(finish_task_count_));
      }
    }

    // 3. End transaction with check_leader
    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::process_complete_stage_()
{
  int ret = OB_SUCCESS;
  ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_complete_cleanup",
                        "tenant_id", tenant_id_,
                        "parent_task_id", parent_task_id_);
  DEBUG_SYNC(SS_HA_MACRO_TASK_CANCEL_BEFORE_COMPLETE_CLEANUP);
  if (OB_FAIL(execute_cleanup_transaction_(true /* is_complete */))) {
    LOG_WARN("failed to execute cleanup transaction", K(ret));
  } else {
    is_finished_ = true;
    wakeup_service_();
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::process_failed_stage_()
{
  int ret = OB_SUCCESS;

  // Load the error information from progress table
  ObSSHAMacroTaskMgrState mgr_state;
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_mgr_state(
      *sql_proxy_, tenant_id_, parent_task_id_, task_type_, mgr_state))) {
    LOG_WARN("failed to load mgr state", K(ret));
  } else if (OB_FAIL(execute_cleanup_transaction_(false /* is_complete */))) {
    LOG_WARN("failed to execute cleanup transaction", K(ret));
  } else {
    // Return the stored error code to caller
    ret = mgr_state.result_;
    is_finished_ = true;
    is_failed_ = true;
    LOG_ERROR("macro block task manager in FAILED stage", K(mgr_state),
             K(total_task_count_), K(finish_task_count_));
    wakeup_service_();
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::cancel()
{
  int ret = OB_SUCCESS;
  BackupJobType job_type = task_type_.is_backup_clean() ? BackupJobType::BACKUP_CLEAN_JOB : BackupJobType::BACKUP_DATA_JOB;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block task mgr not init", K(ret));
  } else {
    if (task_type_.is_backup_clean()) {
      if (OB_FAIL(task_scheduler_->cancel_tasks(job_type, tenant_id_))) { // for ss clean, we use tenant_id to cancel the macro block task
        LOG_WARN("failed to cancel macro tasks by tenant_id", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
      }
    } else {
      if (OB_FAIL(task_scheduler_->cancel_tasks(job_type, parent_task_id_, tenant_id_))) {
        LOG_WARN("failed to cancel macro tasks by parent_task_id", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_delete_tasks_with_trans_())) {
    LOG_WARN("failed to batch delete macro block tasks", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
  } else if (OB_FAIL(cancel_delete_mgr_state_with_trans_())) {
    LOG_WARN("failed to delete mgr state", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::cancel_delete_mgr_state_with_trans_()
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::delete_mgr_state(
        trans, tenant_id_, parent_task_id_, task_type_))) {
      LOG_WARN("failed to delete mgr state", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
    } else {
      LOG_INFO("cancel macro block tasks success", K_(tenant_id), K_(parent_task_id), K_(task_type));
    }

    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("failed to end transaction", K(trans_ret), K(ret));
      ret = COVER_SUCC(trans_ret);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::generate_all_macro_tasks_to_table_()
{
  int ret = OB_SUCCESS;
  
  LOG_INFO("start generate all macro tasks to table", K(tenant_id_), K(parent_task_id_), K(snapshot_));
  
  // Step 1: Collect all macro blocks, batch them, and create tasks
  // total_bytes_ is accumulated during collect_and_batch_all_macro_blocks_
  if (OB_FAIL(collect_and_batch_all_macro_blocks_())) {
    LOG_WARN("failed to collect and batch all macro blocks", K(ret));
  } else {
    finish_bytes_ = 0;
    if (0 == total_bytes_ && total_macro_blocks_ > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("total_bytes is 0 despite having macro blocks; "
               "backup set may lack occupy_size in block file items",
               K(total_macro_blocks_), K(total_task_count_), K(total_bytes_), K(task_type_));
    } else {
      LOG_INFO("generate all macro tasks to table success",
        K(total_macro_blocks_), K(total_task_count_), K(total_bytes_));
    }
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::collect_and_batch_all_macro_blocks_()
{
  int ret = OB_SUCCESS;
  // Collect macro block information using ObSSHAMacroCopyTaskGenerator
  // Note: delete_tasks is already called in process_init_stage_ before this function
  share::ObBackupDest backup_dest;
  if (OB_FAIL(get_backup_dest_(backup_dest))) {
    LOG_WARN("failed to get backup dest and set desc", K(ret));
  } else if (OB_FAIL(ObMacroBlockCollector::collect_macro_blocks_with_generator(
            task_type_, tenant_id_, parent_task_id_, backup_dest,
            snapshot_, ls_id_array_, this, total_task_count_, total_macro_blocks_, total_bytes_))) {
      LOG_WARN("failed to collect macro blocks with producer", K(ret),
                K(tenant_id_), K(snapshot_), K(ls_id_array_), K(backup_dest));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::refill_scheduler_if_needed_()
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  
  // Step 1: Update scheduler task count first (important: recalculate after RS restart)
  if (OB_FAIL(update_scheduler_task_count_())) {
    LOG_WARN("failed to update scheduler task count", K(ret));
  }
  // Step 2: Rate limiting - refill at most once per second (but allow immediate refill after restart)
  else if (last_refill_time_ > 0 && current_time - last_refill_time_ < min_refill_interval()) {
    // Skip refill due to rate limiting
  }
  // Step 3: Check if refill is needed
  else if (tasks_in_scheduler_ < refill_threshold()) {
    const int64_t need_refill = max_concurrent_tasks() - tasks_in_scheduler_;
    static const int64_t BATCH_REFILL_SIZE = 50;        // Number of tasks to refill each time
    const int64_t actual_refill = std::min(need_refill, BATCH_REFILL_SIZE);
    
    ObArray<ObSSHAMacroBlockTaskAttr> next_batch_tasks;
    
    // Step 4: Get limited INIT status tasks from internal table
    ObBackupTaskStatus init_status;
    if (OB_FAIL(init_status.set_status(ObBackupTaskStatus::Status::INIT))) {
      LOG_WARN("failed to set status", K(ret));
    } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::get_tasks_by_status_limit(
            *sql_proxy_, tenant_id_, parent_task_id_, task_type_, init_status, actual_refill, next_batch_tasks))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // No more tasks, normal case
        LOG_DEBUG("no more init tasks to refill", K(tasks_in_scheduler_));
      } else {
        LOG_WARN("failed to get limited init tasks", K(ret), K(actual_refill));
      }
    } else if (!next_batch_tasks.empty()) {
      if (OB_FAIL(submit_tasks_to_scheduler_(next_batch_tasks))) {
        LOG_WARN("failed to submit tasks to scheduler", K(ret));
      } else {
        tasks_in_scheduler_ += next_batch_tasks.count();
        last_refill_time_ = current_time;
        LOG_DEBUG("refilled scheduler with tasks", 
                K(next_batch_tasks.count()), K(tasks_in_scheduler_), K(actual_refill));
      }
    }
  } else {
    LOG_DEBUG("scheduler has enough tasks, no need to refill", K(tasks_in_scheduler_));
  }
  
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SS_HA_MACRO_BLOCK_SKIP_ADD_TASK);
int ObSSHAMacroBlockTaskMgr::submit_tasks_to_scheduler_(
    const common::ObIArray<share::ObSSHAMacroBlockTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const ObSSHAMacroBlockTaskAttr &task_attr = tasks.at(i);
    HEAP_VAR(ObSSHAMacroBlockScheduleTask, schedule_task) {
      if (OB_FAIL(build_schedule_task_(task_attr, schedule_task))) {
        LOG_WARN("failed to build schedule task", K(ret), K(task_attr));
#ifdef ERRSIM
      } else if (OB_SUCCESS != ERRSIM_SS_HA_MACRO_BLOCK_SKIP_ADD_TASK) {
        LOG_INFO("errsim skip add task", K(task_attr));
#endif
      } else if (OB_FAIL(task_scheduler_->add_task(schedule_task))) { // Submit to scheduler (scheduler will automatically select Observer and send RPC)
        LOG_WARN("failed to add task to scheduler", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObSSHAMacroBlockTaskOperator::mark_task_pending_if_init(
            *sql_proxy_, task_attr.tenant_id_, task_attr.parent_task_id_, task_attr.task_type_,
            task_attr.ls_id_.id(), task_attr.task_id_, task_attr.retry_cnt_))) {
          LOG_WARN("failed to update task status to pending", K(ret));
        } else {
          LOG_DEBUG("submitted task to scheduler", K(task_attr.task_id_));
        }
      }
    }
  }
  if (OB_SUCC(ret) && tasks.count() > 0) {
    ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "after_submit_to_scheduler",
                          "tenant_id", tenant_id_,
                          "parent_task_id", parent_task_id_,
                          "task_count", tasks.count());
    DEBUG_SYNC(SS_HA_MACRO_TASK_AFTER_SUBMIT_TO_SCHEDULER);
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::update_scheduler_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_FAIL(task_scheduler_->get_task_count(
      tenant_id_, parent_task_id_, true/*only_macro_block_task*/, count))) {
    LOG_WARN("failed to get task count from scheduler", K(ret));
  } else {
    tasks_in_scheduler_ = count;
    LOG_DEBUG("updated scheduler task count", K(tasks_in_scheduler_));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::check_tasks_completion_()
{
  int ret = OB_SUCCESS;
  
  // Get latest task statistics from internal table
  if (OB_FAIL(get_task_statistics_(total_task_count_, finish_task_count_))) {
    LOG_WARN("failed to get task statistics", K(ret));
  } else {
    LOG_DEBUG("checked tasks completion", K(total_task_count_), 
             K(finish_task_count_), K(total_macro_blocks_),
             K(finish_macro_blocks_), K(tasks_in_scheduler_));
  }
  
  return ret;
}

bool ObSSHAMacroBlockTaskMgr::is_all_tasks_finished_() const
{
  return finish_task_count_ >= total_task_count_ && total_task_count_ > 0;
}

int ObSSHAMacroBlockTaskMgr::advance_task_status_(
    const ObSSHAMacroTaskMgrStatus &prev_task_status,
    const ObSSHAMacroTaskMgrStatus &next_task_status,
    const int64_t result)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroTaskMgrState mgr_state;

  mgr_state.tenant_id_ = tenant_id_;
  mgr_state.parent_task_id_ = parent_task_id_;
  mgr_state.task_status_ = next_task_status.status_;
  mgr_state.task_type_ = task_type_;
  mgr_state.total_task_count_ = total_task_count_;
  mgr_state.finish_task_count_ = finish_task_count_;
  mgr_state.total_macro_block_count_ = total_macro_blocks_;
  mgr_state.finish_macro_block_count_ = finish_macro_blocks_;
  mgr_state.result_ = result;

  // Validate state before saving
  if (!mgr_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task mgr state", K(ret), K(mgr_state));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::save_mgr_state(*sql_proxy_, mgr_state))) {
    LOG_WARN("failed to save manager state", K(ret), K(mgr_state));
  } else {
    ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "advance_task_status",
                          "tenant_id", tenant_id_,
                          "prev_task_status", prev_task_status.get_str(),
                          "next_task_status", next_task_status.get_str(),
                          "total_task_count", total_task_count_,
                          "finish_task_count", finish_task_count_,
                          "result", result);
    LOG_DEBUG("save current state success", K(mgr_state));
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::advance_task_status_in_trans_(
    common::ObISQLClient &trans,
    const ObSSHAMacroTaskMgrStatus &prev_task_status,
    const ObSSHAMacroTaskMgrStatus &next_task_status,
    const int64_t result)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroTaskMgrState mgr_state;

  mgr_state.tenant_id_ = tenant_id_;
  mgr_state.parent_task_id_ = parent_task_id_;
  mgr_state.task_status_ = next_task_status.status_;
  mgr_state.task_type_ = task_type_;
  mgr_state.total_task_count_ = total_task_count_;
  mgr_state.finish_task_count_ = finish_task_count_;
  mgr_state.total_macro_block_count_ = total_macro_blocks_;
  mgr_state.finish_macro_block_count_ = finish_macro_blocks_;
  mgr_state.total_bytes_ = total_bytes_;
  mgr_state.finish_bytes_ = finish_bytes_;
  mgr_state.result_ = result;

  // Validate state before saving
  if (!mgr_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task mgr state", K(ret), K(mgr_state));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::save_mgr_state(trans, mgr_state))) {
    LOG_WARN("failed to save manager state in transaction", K(ret), K(mgr_state));
  } else {
    ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "advance_task_status_in_trans",
                          "tenant_id", tenant_id_,
                          "prev_task_status", prev_task_status.get_str(),
                          "next_task_status", next_task_status.get_str(),
                          "total_task_count", total_task_count_,
                          "finish_task_count", finish_task_count_,
                          "result", result);
    LOG_DEBUG("save current state in transaction success", K(mgr_state));
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::advance_running_status_with_trans_(
    const ObSSHAMacroTaskMgrStatus::Status next_status,
    const int result,
    ObSSHAMacroTaskMgrState *out_lock_state)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("start transaction failed", KR(ret));
  } else {
    ObSSHAMacroTaskMgrState lock_state;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id_, parent_task_id_, task_type_, ObSSHAMacroTaskMgrStatus::RUNNING, lock_state))) {
      LOG_WARN("failed to load and verify mgr state for update", K(ret));
    } else {
      // Add debug sync point before state transition
      if (next_status == ObSSHAMacroTaskMgrStatus::COMPLETE) {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_running_to_complete",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_RUNNING_TO_COMPLETE);
      } else if (next_status == ObSSHAMacroTaskMgrStatus::FAILED) {
        ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "before_running_to_failed",
                              "tenant_id", tenant_id_,
                              "parent_task_id", parent_task_id_);
        DEBUG_SYNC(SS_HA_MACRO_TASK_BEFORE_RUNNING_TO_FAILED);
      }
      if (OB_FAIL(advance_task_status_in_trans_(trans,
          ObSSHAMacroTaskMgrStatus::RUNNING,
          next_status,
          result))) {
        LOG_WARN("failed to advance status", K(ret), K(next_status));
      } else if (OB_NOT_NULL(out_lock_state)) {
        *out_lock_state = lock_state;
      }
    }

    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::advance_init_status_to_failed_with_trans_(const int result)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;

  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("start transaction failed", KR(ret));
  } else {
    ObSSHAMacroTaskMgrState lock_state;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id_, parent_task_id_, task_type_, ObSSHAMacroTaskMgrStatus::INIT, lock_state))) {
      LOG_WARN("failed to load and verify mgr state for update", K(ret));
    } else if (OB_FAIL(advance_task_status_in_trans_(trans,
          ObSSHAMacroTaskMgrStatus::INIT,
          ObSSHAMacroTaskMgrStatus::FAILED,
          result))) {
      LOG_WARN("failed to advance init status to failed", K(ret), K(result));
    }

    int trans_ret = end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }

    if (OB_SUCC(ret)) {
      ROOTSERVICE_EVENT_ADD("ss_ha_macro_block_task_mgr", "init_to_failed",
                            "tenant_id", tenant_id_,
                            "parent_task_id", parent_task_id_,
                            "result", result);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskMgr::load_persistent_status_(
    ObSSHAMacroTaskMgrStatus &task_status,
    bool &is_recovered_from_history,
    int64_t &task_result)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroTaskMgrState mgr_state;
  is_recovered_from_history = false;
  task_result = OB_SUCCESS;

  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_mgr_state(
      *sql_proxy_, tenant_id_, parent_task_id_, task_type_, mgr_state))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_mgr_state_from_history(
          *sql_proxy_, tenant_id_, parent_task_id_, task_type_, mgr_state))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to load mgr state from history", K(ret), K_(tenant_id), K_(parent_task_id), K_(task_type));
        }
      } else if (ObSSHAMacroTaskMgrStatus::COMPLETE != mgr_state.task_status_
          && ObSSHAMacroTaskMgrStatus::FAILED != mgr_state.task_status_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("manager state in history is not terminal", K(ret), K(mgr_state));
      } else {
        is_recovered_from_history = true;
        task_result = mgr_state.result_;
      }
    } else {
      LOG_WARN("failed to load manager state", K(ret), K(tenant_id_), K(parent_task_id_), K(task_type_));
    }
  }
  if (OB_SUCC(ret)) {
    // Recover state
    task_status.status_ = static_cast<ObSSHAMacroTaskMgrStatus::Status>(mgr_state.task_status_);
    task_type_ = mgr_state.task_type_;
    total_task_count_ = mgr_state.total_task_count_;
    finish_task_count_ = mgr_state.finish_task_count_;
    total_macro_blocks_ = mgr_state.total_macro_block_count_;
    finish_macro_blocks_ = mgr_state.finish_macro_block_count_;
    total_bytes_ = mgr_state.total_bytes_;
    finish_bytes_ = mgr_state.finish_bytes_;
    LOG_INFO("loaded persistent state", K(mgr_state));
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::get_task_statistics_(int64_t &total, int64_t &completed)
{
  int ret = OB_SUCCESS;
  int64_t total_macro_blocks = 0;
  int64_t finished_macro_blocks = 0;
  int64_t total_bytes = 0;
  int64_t finish_bytes = 0;

  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::get_task_statistics(
      *sql_proxy_, tenant_id_, parent_task_id_, task_type_, total, completed,
      total_macro_blocks, finished_macro_blocks, total_bytes, finish_bytes))) {
    LOG_WARN("failed to get task statistics", K(ret));
  } else {
    total_macro_blocks_ = total_macro_blocks;
    finish_macro_blocks_ = finished_macro_blocks;
    total_bytes_ = total_bytes;
    finish_bytes_ = finish_bytes;
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::create_macro_task_attr_(
    const share::ObLSID &ls_id,
    const share::ObSSHAMacroBlockBatch &batch,
    share::ObSSHAMacroBlockTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  
  task_attr.reset();
  
  if (OB_FAIL(get_next_task_id_(task_id))) {
    LOG_WARN("failed to get next task id", K(ret));
  } else if (OB_FAIL(task_attr.macro_id_list_.set_macro_block_batch(batch))) {
    LOG_WARN("failed to assign macro block batch", K(ret), K(batch));
  } else {
    task_attr.tenant_id_ = tenant_id_;
    task_attr.parent_task_id_ = parent_task_id_;
    task_attr.ls_id_ = ls_id;
    task_attr.task_id_ = task_id;
    task_attr.task_type_ = task_type_;
    task_attr.start_time_ = ObTimeUtility::current_time();
    task_attr.end_time_ = 0;
    task_attr.result_ = OB_SUCCESS;
    task_attr.macro_block_cnt_ = batch.count();
    task_attr.total_bytes_ = batch.get_total_bytes();
    task_attr.finish_bytes_ = 0;
    
    // Set task status to INIT
    if (OB_FAIL(task_attr.status_.set_status(ObBackupTaskStatus::Status::INIT))) {
      LOG_WARN("failed to set task status", K(ret));
    }
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::get_next_task_id_(int64_t &task_id)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::get_next_task_id(*sql_proxy_, tenant_id_, parent_task_id_, task_type_, task_id))) {
    LOG_WARN("failed to get next task id", K(ret), K(tenant_id_));
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::construct_backup_set_dest_from_path_(
    const common::ObString &backup_path,
    const int64_t backup_set_id,
    share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupSetDesc backup_set_desc;
  
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret), KP(sql_proxy_));
  } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest(
                 *sql_proxy_, tenant_id_, backup_path, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id_), K(backup_path));
  } else {
    backup_set_desc.backup_set_id_ = backup_set_id;
    backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
    if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
            backup_dest, backup_set_desc, backup_set_dest))) {
      LOG_WARN("failed to construct backup set dest", K(ret), K(backup_dest), K(backup_set_desc));
    }
  }
  
  return ret;
}

int ObSSHAMacroBlockTaskMgr::handle_execute_over(
    ObBackupService &backup_service,
    common::ObISQLClient &sql_proxy,
    const ObSSHAMacroBlockScheduleTask *&macro_task,
    const share::ObHAResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("macro task is null", K(ret), KP(macro_task));
  } else if (!result_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid result info", K(ret), K(result_info));
  } else {
    const uint64_t tenant_id = macro_task->get_tenant_id();
    const uint64_t ls_id = macro_task->get_ls_id();
    const uint64_t task_id = macro_task->get_task_id();
    const ObSSHAMacroBlockTaskAttr &task_attr = macro_task->get_macro_task_attr();
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !task_attr.ls_id_.is_valid()
        || 0 == task_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("macro task has invalid identifiers", K(ret), K(tenant_id), K(ls_id), K(task_id), K(task_attr));
    } else if (OB_UNLIKELY(!task_attr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("macro task attr is invalid", K(ret), K(task_attr));
    } else {
      const int64_t parent_task_id = task_attr.parent_task_id_;
      const ObSSHAMacroTaskType &task_type = task_attr.task_type_;
      const int64_t end_time = ObTimeUtility::current_time();

      FLOG_INFO("ss ha macro block task mgr handle execute over", K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id),
          "result", result_info.result_);

      // Check execution result
      if (OB_SUCCESS == result_info.result_) {
        if (OB_FAIL(handle_execute_over_success_(sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, result_info, end_time, macro_task))) {
          LOG_WARN("failed to handle success case", K(ret), K(tenant_id), K(ls_id), K(task_id));
        }
      } else {
        const bool is_retryable_error = ObBackupUtils::is_need_retry_error(result_info.result_);
        int64_t retry_cnt = 0;
        if (OB_FAIL(get_task_retry_count_(sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, retry_cnt))) {
          LOG_WARN("failed to get task retry count", K(ret), K(tenant_id), K(ls_id), K(task_id));
        } else {
#ifdef ERRSIM
          const int64_t max_retry_count = GCONF.errsim_shared_ha_macro_task_retry_count;
          LOG_INFO("errsim ss ha macro task retry count", K(max_retry_count));
#else
          const int64_t max_retry_count = MAX_RETRY_COUNT;
#endif
          if (is_retryable_error && retry_cnt <= max_retry_count) {
            if (OB_FAIL(handle_execute_over_retryable_failure_(sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, macro_task, result_info, retry_cnt))) {
              LOG_WARN("failed to handle retryable failure", K(ret), K(tenant_id), K(ls_id), K(task_id), K(retry_cnt));
            }
          } else {
            if (OB_FAIL(handle_execute_over_non_retryable_failure_(backup_service, sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, result_info, retry_cnt))) {
              LOG_WARN("failed to handle non-retryable failure", K(ret), K(tenant_id), K(ls_id), K(task_id), K(retry_cnt));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    backup_service.wakeup();
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::handle_execute_over_success_(
    common::ObISQLClient &sql_proxy,
    uint64_t tenant_id,
    int64_t parent_task_id,
    const share::ObSSHAMacroTaskType &task_type,
    uint64_t ls_id,
    uint64_t task_id,
    const share::ObHAResultInfo &result_info,
    const int64_t end_time,
    const ObSSHAMacroBlockScheduleTask *macro_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
          sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, end_time, result_info.result_, ""))) {
    LOG_WARN("failed to update task completion with progress", K(ret), K(tenant_id), K(ls_id), K(task_id), K(result_info));
  } else {
    LOG_INFO("macro task completed successfully", KPC(macro_task), K(result_info));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::get_task_retry_count_(
    common::ObISQLClient &sql_proxy,
    uint64_t tenant_id,
    int64_t parent_task_id,
    const share::ObSSHAMacroTaskType &task_type,
    uint64_t ls_id,
    uint64_t task_id,
    int64_t &retry_cnt)
{
  int ret = OB_SUCCESS;
  retry_cnt = 0;
  if (OB_FAIL(ObSSHAMacroBlockTaskOperator::get_task_retry_count(
          sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, retry_cnt))) {
    LOG_WARN("failed to get retry count from database", K(ret), K(tenant_id), K(ls_id), K(task_id));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::handle_execute_over_retryable_failure_(
    common::ObISQLClient &sql_proxy,
    uint64_t tenant_id,
    int64_t parent_task_id,
    const share::ObSSHAMacroTaskType &task_type,
    uint64_t ls_id,
    uint64_t task_id,
    const ObSSHAMacroBlockScheduleTask *macro_task,
    const share::ObHAResultInfo &result_info,
    const int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  ObBackupTaskStatus init_status;
  if (OB_FAIL(init_status.set_status(ObBackupTaskStatus::Status::INIT))) {
    LOG_WARN("failed to set finish status", K(ret));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::update_task_for_retry_with_lock(
                 sql_proxy, tenant_id, parent_task_id, task_type, ls_id, task_id, init_status.get_str()))) {
    LOG_WARN("failed to update task for retry with lock", K(ret), KPC(macro_task), K(result_info), K(retry_cnt));
  } else {
    LOG_INFO("macro task reset for retry with lock", KPC(macro_task), K(result_info), K(retry_cnt));
  }
  return ret;
}

int ObSSHAMacroBlockTaskMgr::handle_execute_over_non_retryable_failure_(
    ObBackupService &backup_service,
    common::ObISQLClient &sql_proxy,
    uint64_t tenant_id,
    int64_t parent_task_id,
    const share::ObSSHAMacroTaskType &task_type,
    uint64_t ls_id,
    uint64_t task_id,
    const share::ObHAResultInfo &result_info,
    const int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  char comment_buf[1024] = {0};
  int64_t pos = 0;

  if (OB_FAIL(databuff_printf(comment_buf, sizeof(comment_buf), pos,
                               "task_id=%ld ls_id=%ld result=%d retry_cnt=%ld",
                               task_id, ls_id, result_info.result_, retry_cnt))) {
    LOG_WARN("failed to build comment string", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    // 1. Lock the progress row for dual-master protection
    ObSSHAMacroTaskMgrState lock_state;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
        trans, tenant_id, parent_task_id, task_type, ObSSHAMacroTaskMgrStatus::RUNNING, lock_state))) {
      LOG_WARN("failed to load and verify mgr state for update", K(ret));
    } else {
      const ObString comment_str(pos, comment_buf);
      if (OB_FAIL(ObSSHAMacroBlockTaskOperator::update_progress_result(
              trans, tenant_id, parent_task_id, task_type, result_info.result_, comment_str))) {
        LOG_WARN("failed to update progress result", K(ret), K(tenant_id), K(result_info));
      } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::update_tasks_result(
                     trans, tenant_id, parent_task_id, ObLSID(ls_id), task_id, task_type, result_info.result_, comment_str))) {
        LOG_WARN("failed to update tasks result", K(ret), K(tenant_id), K(result_info));
      } else {
        LOG_INFO("updated both tables for task failure", K(tenant_id), K(result_info), K(comment_str));
      }
    }

    // 2. End transaction with check_leader
    int trans_ret = backup_service.end_transaction(trans, ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
      ret = COVER_SUCC(trans_ret);
    }
  }
  return ret;
}

/* ObSSHAMacroBlockBackupTaskMgr */

ObSSHAMacroBlockBackupTaskMgr::ObSSHAMacroBlockBackupTaskMgr()
  : ObSSHAMacroBlockTaskMgr(),
    job_attr_(),
    set_task_attr_(),
    backup_service_(nullptr)
{
}

ObSSHAMacroBlockBackupTaskMgr::~ObSSHAMacroBlockBackupTaskMgr()
{
}

int ObSSHAMacroBlockBackupTaskMgr::init(
    const ObSSHAMacroTaskType &task_type,
    share::ObBackupJobAttr &job_attr,
    share::ObBackupSetTaskAttr &set_task_attr,
    ObBackupTaskScheduler &task_scheduler,
    common::ObISQLClient &sql_proxy,
    const common::ObIArray<share::ObLSID> &ls_id_array,
    const share::SCN &snapshot,
    ObBackupService &backup_service)
{
  int ret = OB_SUCCESS;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block task mgr init twice", K(ret));
  } else if (!task_type.is_valid() || !job_attr.is_valid() || !set_task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_type), K(job_attr), K(set_task_attr));
  } else if (OB_FAIL(job_attr_.assign(job_attr))) {
    LOG_WARN("fail to assign job_attr",  K(ret), K(job_attr));
  } else if (OB_FAIL(set_task_attr_.assign(set_task_attr))) {
    LOG_WARN("fail to assign set task attr", K(ret), K(set_task_attr));
  } else if (OB_FAIL(ls_id_array_.assign(ls_id_array))) {
    LOG_WARN("failed to assign ls id array", K(ret));
  } else {
    tenant_id_ = job_attr.tenant_id_;
    parent_task_id_ = job_attr.job_id_;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    backup_service_ = &backup_service;
    snapshot_ = snapshot;
    task_type_ = task_type;
    is_inited_ = true;
    LOG_INFO("init ObSSHAMacroBlockTaskMgr", K(tenant_id_), K(parent_task_id_), K(snapshot_));
  }
  
  return ret;
}

int ObSSHAMacroBlockBackupTaskMgr::init_stage_write_macro_block_list_()
{
  int ret = OB_SUCCESS;
  // NOTE: This step writes macro_block_list files to object storage outside of
  // any transaction. If process_init_stage_ is retried (e.g., due to RS failover or
  // init_stage_advance_status_ failure), files from a previous attempt may already exist.
  // This is safe because:
  //   1. Object storage supports overwrite (PUT is idempotent for same-named objects).
  //   2. The writer uses deterministic file_ids starting from 0, and tasks are regenerated
  //      from the same snapshot, so the same files are produced on retry.
  if (OB_FAIL(for_each_macro_block_task_by_ls_ids_())) {
    LOG_WARN("failed to write macro block list files", K(ret));
  }
  return ret;
}

int ObSSHAMacroBlockBackupTaskMgr::for_each_macro_block_task_by_ls_ids_()
{
  int ret = OB_SUCCESS;

  const int64_t batch_size = MAX_BATCH_SIZE;
  const backup::ObBackupBlockFileDataType data_type = backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID;
  int64_t dest_id = 0;
  ObBackupDest backup_dest;
  ObBackupDest backup_set_dest;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = set_task_attr_.backup_set_id_;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  int64_t accumulated_item_count = 0;
  int64_t accumulated_block_count = 0;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret), KP(sql_proxy_));
  } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest(
                 *sql_proxy_, tenant_id_, set_task_attr_.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id_), K(set_task_attr_.backup_path_));
  } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_dest_id(
                 *sql_proxy_, tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("failed to get dest id", K(ret), K(tenant_id_), K(backup_dest));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
      backup_dest, backup_set_desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array_.count(); ++i) {
    const share::ObLSID &ls_id = ls_id_array_.at(i);
    backup::ObBackupBlockFileItemWriter writer;
    backup::ObBackupBlockFileAddr entry_block_addr;
    int64_t total_item_count = 0;
    int64_t total_block_count = 0;
    bool writer_inited = false;

    // 1. Init writer for this LS
    ObBackupPath file_list_dir;
    if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(
        backup_set_dest, ls_id, file_list_dir))) {
      LOG_WARN("failed to get ls backup dir path", K(ret), K(ls_id));
    } else if (OB_FAIL(writer.init(backup_set_dest.get_storage_info(),
                                   data_type,
                                   share::ObBackupFileSuffix::BACKUP,
                                   file_list_dir,
                                   dest_id,
                                   OB_BACKUP_MACRO_BLOCK_LIST_MAX_FILE_SIZE /*max_file_size*/))) {
      LOG_WARN("failed to init writer", K(ret), K(ls_id), K(data_type));
    } else {
      writer_inited = true;
    }

    // 2. Build processor: write macro block IDs to backup file
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    if (OB_SUCC(ret)) {
      ObMacroIdWriteProcessor write_processor(writer);
      if (OB_FAIL(processor.assign(write_processor))) {
        LOG_WARN("failed to assign task processor", K(ret), K(ls_id));
      }
    }

    // 3. Iterate tasks from DB and invoke processor
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
              *sql_proxy_, tenant_id_, parent_task_id_, task_type_,
              ls_id, batch_size, processor))) {
        LOG_WARN("failed to iterate macro block tasks for ls", K(ret), K(ls_id), K(data_type));
      }
    }

    // 4. Close writer.
    // Only close (and thus write the FileTailer) when iteration succeeded.
    if (writer_inited && OB_SUCC(ret)) {
      const int close_ret = writer.close(entry_block_addr, total_item_count, total_block_count);
      if (OB_SUCCESS != close_ret) {
        ret = close_ret;
        LOG_WARN("failed to close writer", K(ret), K(ls_id));
      } else {
        accumulated_item_count += total_item_count;
        accumulated_block_count += total_block_count;
        LOG_INFO("write macro block list for ls done",
            "data_type", backup::ob_backup_block_file_data_type_str(data_type),
            K(ls_id), K(total_item_count), K(total_block_count), K(entry_block_addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("for_each_macro_block_task_by_ls_ids done",
        "data_type", backup::ob_backup_block_file_data_type_str(data_type),
        K(ls_id_array_), K(accumulated_item_count), K(accumulated_block_count));
  }

  return ret;
}

int ObSSHAMacroBlockBackupTaskMgr::get_backup_dest_(
    share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObString backup_path = set_task_attr_.backup_path_.str();
  
  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest(
          *sql_proxy_, tenant_id_, backup_path, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id_), K(backup_path));
  }
  
  return ret;
}

int ObSSHAMacroBlockBackupTaskMgr::build_schedule_task_(
    const share::ObSSHAMacroBlockTaskAttr &task_attr,
    ObSSHAMacroBlockScheduleTask &schedule_task)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(schedule_task.build(job_attr_, set_task_attr_, task_attr))) {
    LOG_WARN("failed to build schedule task for backup", K(ret), K(task_attr));
  }
  
  return ret;
}

BackupJobType ObSSHAMacroBlockBackupTaskMgr::get_job_type_() const
{
  return BackupJobType::BACKUP_DATA_JOB;
}

void ObSSHAMacroBlockBackupTaskMgr::wakeup_service_()
{
  if (nullptr != backup_service_) {
    backup_service_->wakeup();
  }
}

int ObSSHAMacroBlockBackupTaskMgr::end_transaction(
    common::ObMySQLTransaction &trans,
    const int upstream_ret)
{
  int ret = OB_SUCCESS;
  if (nullptr != backup_service_) {
    ret = backup_service_->end_transaction(trans, upstream_ret);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_service_ is null", K(ret));
  }
  return ret;
}

/* ObSSHAMacroBlockRestoreTaskMgr */

ObSSHAMacroBlockRestoreTaskMgr::ObSSHAMacroBlockRestoreTaskMgr()
  : ObSSHAMacroBlockTaskMgr(),
    job_attr_(),
    set_task_attr_(),
    restore_service_(nullptr)
{
}

ObSSHAMacroBlockRestoreTaskMgr::~ObSSHAMacroBlockRestoreTaskMgr()
{
}

int ObSSHAMacroBlockRestoreTaskMgr::init(
    const ObSSHAMacroTaskType &task_type,
    share::ObBackupJobAttr &job_attr,
    share::ObBackupSetTaskAttr &set_task_attr,
    ObBackupTaskScheduler &task_scheduler,
    common::ObISQLClient &sql_proxy,
    ObRestoreService &restore_service,
    const common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block restore task mgr init twice", K(ret));
  } else if (!task_type.is_restore() || !job_attr.is_valid() || !set_task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_type), K(job_attr), K(set_task_attr));
  } else if (OB_FAIL(job_attr_.assign(job_attr))) {
    LOG_WARN("fail to assign job_attr",  K(ret), K(job_attr));
  } else if (OB_FAIL(set_task_attr_.assign(set_task_attr))) {
    LOG_WARN("fail to assign set task attr", K(ret), K(set_task_attr));
  } else {
    tenant_id_ = job_attr.tenant_id_;
    parent_task_id_ = job_attr.job_id_;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    restore_service_ = &restore_service;
    snapshot_ = set_task_attr.start_scn_;
    task_type_ = task_type;
    
    // Store log stream ID array for restore tasks
    if (OB_FAIL(ls_id_array_.assign(ls_id_array))) {
      LOG_WARN("failed to assign ls id array", K(ret));
      is_inited_ = false;
    } else {
      is_inited_ = true;
      LOG_INFO("init ObSSHAMacroBlockRestoreTaskMgr for restore", K(tenant_id_), K(parent_task_id_), K(snapshot_));
    }
  }
  
  return ret;
}

int ObSSHAMacroBlockRestoreTaskMgr::get_backup_dest_(
    share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObString backup_path = set_task_attr_.backup_path_.str();
  
  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest(
          *sql_proxy_, tenant_id_, backup_path, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id_), K(backup_path));
  }
  
  return ret;
}

int ObSSHAMacroBlockRestoreTaskMgr::build_schedule_task_(
    const share::ObSSHAMacroBlockTaskAttr &task_attr,
    ObSSHAMacroBlockScheduleTask &schedule_task)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(schedule_task.build(job_attr_, set_task_attr_, task_attr))) {
    LOG_WARN("failed to build schedule task for restore", K(ret), K(task_attr));
  }
  
  return ret;
}

BackupJobType ObSSHAMacroBlockRestoreTaskMgr::get_job_type_() const
{
  return BackupJobType::BACKUP_DATA_JOB;
}

void ObSSHAMacroBlockRestoreTaskMgr::wakeup_service_()
{
  if (nullptr != restore_service_) {
    restore_service_->wakeup();
  }
}

int ObSSHAMacroBlockRestoreTaskMgr::end_transaction(
    common::ObMySQLTransaction &trans,
    const int upstream_ret)
{
  int ret = OB_SUCCESS;
  if (nullptr != restore_service_) {
    ret = restore_service_->end_transaction(trans, upstream_ret);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore_service_ is null", K(ret));
  }
  return ret;
}

/* ObSSHAMacroBlockCleanTaskMgr */

ObSSHAMacroBlockCleanTaskMgr::ObSSHAMacroBlockCleanTaskMgr()
  : ObSSHAMacroBlockTaskMgr(),
    clean_job_attr_(),
    clean_task_attr_(),
    backup_service_(nullptr)
{
}

ObSSHAMacroBlockCleanTaskMgr::~ObSSHAMacroBlockCleanTaskMgr()
{
}

int ObSSHAMacroBlockCleanTaskMgr::init(
  const ObSSHAMacroTaskType &task_type,
  const share::ObBackupCleanJobAttr &clean_job_attr,
  const share::ObBackupCleanTaskAttr &clean_task_attr,
  ObBackupTaskScheduler &task_scheduler,
  common::ObISQLClient &sql_proxy,
  ObBackupService &backup_service)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block task mgr init twice", K(ret));
  } else if (!task_type.is_backup_clean() || !clean_job_attr.is_valid() || !clean_task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_type), K(clean_job_attr), K(clean_task_attr));
  } else {
    tenant_id_ = clean_job_attr.tenant_id_;
    parent_task_id_ = clean_task_attr.task_id_;
    task_type_ = task_type;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    backup_service_ = &backup_service;
    if (OB_FAIL(clean_job_attr_.assign(clean_job_attr))) {
      LOG_WARN("failed to assign clean job attr", K(ret));
    } else if (OB_FAIL(clean_task_attr_.assign(clean_task_attr))) {
      LOG_WARN("failed to assign clean task attr", K(ret));
    } else if (OB_FAIL(snapshot_.convert_from_ts(clean_task_attr.start_ts_))) {
      LOG_WARN("fail to convert start scn", K(ret), K(clean_task_attr));
    } else {
      is_inited_ = true;
      LOG_INFO("init ObSSHAMacroBlockCleanTaskMgr for backup clean", K(clean_job_attr), K(clean_task_attr));
    }
  }
  LOG_DEBUG("init of ObSSHAMacroBlockCleanTaskMgr for backup clean",
            K(ret), K(task_type), K(clean_job_attr), K(clean_task_attr));
  return ret;
}

int ObSSHAMacroBlockCleanTaskMgr::get_backup_dest_(
    share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObString backup_path = clean_task_attr_.backup_path_.str();
  if (OB_FAIL(construct_backup_set_dest_from_path_(
          backup_path, clean_task_attr_.backup_set_id_, backup_dest))) {
    LOG_WARN("failed to construct backup clean set dest", 
              K(ret), K(tenant_id_), K(backup_path), "set_id", clean_task_attr_.backup_set_id_);
  } else {
    LOG_INFO("constructed backup clean set dest", K(backup_dest));
  }
  
  return ret;
}

int ObSSHAMacroBlockCleanTaskMgr::build_schedule_task_(
    const share::ObSSHAMacroBlockTaskAttr &task_attr,
    ObSSHAMacroBlockScheduleTask &schedule_task)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(schedule_task.build(clean_job_attr_, clean_task_attr_, task_attr))) {
    LOG_WARN("failed to build schedule task for backup clean", K(ret), K(task_attr));
  }
  
  return ret;
}

BackupJobType ObSSHAMacroBlockCleanTaskMgr::get_job_type_() const
{
  return BackupJobType::BACKUP_CLEAN_JOB;
}

void ObSSHAMacroBlockCleanTaskMgr::wakeup_service_()
{
  if (nullptr != backup_service_) {
    backup_service_->wakeup();
  }
}

int ObSSHAMacroBlockCleanTaskMgr::end_transaction(
    common::ObMySQLTransaction &trans,
    const int upstream_ret)
{
  int ret = OB_SUCCESS;
  if (nullptr != backup_service_) {
    ret = backup_service_->end_transaction(trans, upstream_ret);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_service_ is null", K(ret));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
