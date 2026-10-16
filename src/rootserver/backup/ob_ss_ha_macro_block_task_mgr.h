/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_SS_HA_MACRO_BLOCK_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_SS_HA_MACRO_BLOCK_TASK_MGR_H_

#include "rootserver/backup/ob_backup_data_scheduler.h"
#include "rootserver/backup/ob_macro_block_collector.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "share/backup/ob_ss_ha_macro_block_table_operator.h"

namespace oceanbase
{
namespace rootserver
{
class ObBackupService;
class ObRestoreService;
class ObBackupBaseService;

struct ObSSHAMacroTaskMgrStatus final
{
  enum Status
  {
    INIT = 0,           // Initialization stage: generate all macro block tasks to internal table
    RUNNING = 1,        // Running stage: sliding window task refill
    COMPLETE = 2,       // Complete stage: all tasks finished
    FAILED = 3,         // Failed stage: task execution failed
    MAX_STATUS,
  };
  ObSSHAMacroTaskMgrStatus(): status_(Status::MAX_STATUS) {}
  ObSSHAMacroTaskMgrStatus(const Status &status): status_(status) {}
  ~ObSSHAMacroTaskMgrStatus() {}
  bool is_valid() const;
  const char* get_str() const;
  bool is_init() const { return Status::INIT == status_; }
  bool is_running() const { return Status::RUNNING == status_; }
  bool is_complete() const { return Status::COMPLETE == status_; }
  bool is_failed() const { return Status::FAILED == status_; }
  TO_STRING_KV(K_(status));
  Status status_;
};

// Macro block task manager base class: replaces original ObBackupDataLSTaskMgr
class ObSSHAMacroBlockScheduleTask;
// Base class for macro block task manager
class ObSSHAMacroBlockTaskMgr : public share::ObHATransEndHelper
{
public:
  ObSSHAMacroBlockTaskMgr();
  virtual ~ObSSHAMacroBlockTaskMgr();

  // Main processing logic: perform different operations based on set_task status
  // is_finished: true if all tasks are finished or has failed
  // is_failed: true if task failed and should not retry (outer caller should not retry this task mgr)
  int process(bool &is_finished, bool &is_failed);
  int cancel();
  bool is_inited() const { return is_inited_; }

  // Static method: handle task execution results
  static int handle_execute_over(
      ObBackupService &backup_service,
      common::ObISQLClient &sql_proxy,
      const ObSSHAMacroBlockScheduleTask *&macro_task,
      const share::ObHAResultInfo &result_info);
  int64_t get_finish_task_count() const { return finish_task_count_; }
  int64_t get_total_task_count() const { return total_task_count_; }
  int64_t get_total_bytes() const { return total_bytes_; }
  int64_t get_finish_bytes() const { return finish_bytes_; }
  int get_backup_stats(share::ObBackupStats &backup_stats) const;

protected:
  virtual int get_backup_dest_(share::ObBackupDest &backup_dest) = 0;
  virtual int build_schedule_task_(
      const share::ObSSHAMacroBlockTaskAttr &task_attr,
      ObSSHAMacroBlockScheduleTask &schedule_task) = 0;
  virtual BackupJobType get_job_type_() const = 0;
  virtual void wakeup_service_() = 0;

  // Core processing logic
  int generate_all_macro_tasks_to_table_();
  int refill_scheduler_if_needed_();
  int submit_tasks_to_scheduler_(const common::ObIArray<share::ObSSHAMacroBlockTaskAttr> &tasks);
  int update_scheduler_task_count_();
  int check_tasks_completion_();
  
  // Process stage handlers
  int process_init_stage_();
  int process_running_stage_();
  int process_complete_stage_();
  int process_failed_stage_();

  // Init stage sub-phases
  int init_stage_delete_existing_tasks_();
  int init_stage_generate_tasks_();
  // Write macro_block_list files to object storage. Default no-op; Backup overrides.
  virtual int init_stage_write_macro_block_list_() { return OB_SUCCESS; }
  int init_stage_advance_status_();
  // Lock and verify state with check_leader (short transaction)
  int lock_and_verify_state_(const int64_t expected_status);

  // Transaction operations for stage completion/failure
  int execute_cleanup_transaction_(const bool is_complete);
  // Lock, verify state, and move mgr to history (short transaction)
  int execute_cleanup_move_to_history_(const bool is_complete);
  // Delete mgr state in transaction (for cancel scenario)
  int cancel_delete_mgr_state_with_trans_();
  // Batch delete tasks in transaction (for init cleanup and cancel)
  int batch_delete_tasks_with_trans_();

  // Get backup base service for end_transaction (check_leader). Returns nullptr for Restore.
  virtual ObBackupBaseService *get_backup_base_service_() { return nullptr; }

  // Macro block information collection and task generation
  int collect_and_batch_all_macro_blocks_();
  
  // State management
  bool is_all_tasks_finished_() const;
  int get_task_statistics_(int64_t &total, int64_t &completed);

  // Helper methods
  int create_macro_task_attr_(const share::ObLSID &ls_id,
                              const share::ObSSHAMacroBlockBatch &batch,
                              share::ObSSHAMacroBlockTaskAttr &task_attr);
  int get_next_task_id_(int64_t &task_id);
  // Construct backup set dest from backup path and backup set id
  int construct_backup_set_dest_from_path_(
      const common::ObString &backup_path,
      const int64_t backup_set_id,
      share::ObBackupDest &backup_set_dest);
  
  // State persistence methods
  int advance_task_status_(
      const ObSSHAMacroTaskMgrStatus &prev_task_status,
      const ObSSHAMacroTaskMgrStatus &next_task_status,
      const int64_t result);
  // Advance task status within a transaction (for dual-master protection)
  int advance_task_status_in_trans_(
      common::ObISQLClient &trans,
      const ObSSHAMacroTaskMgrStatus &prev_task_status,
      const ObSSHAMacroTaskMgrStatus &next_task_status,
      const int64_t result);
  // Advance from RUNNING status with transaction wrapper
  int advance_running_status_with_trans_(
      const ObSSHAMacroTaskMgrStatus::Status next_status,
      const int result,
      share::ObSSHAMacroTaskMgrState *out_lock_state);
  // Advance from INIT status to FAILED with transaction wrapper
  int advance_init_status_to_failed_with_trans_(const int result);
  int load_persistent_status_(
      ObSSHAMacroTaskMgrStatus &task_status,
      bool &is_recovered_from_history,
      int64_t &task_result);

  static int handle_execute_over_success_(
      common::ObISQLClient &sql_proxy,
      uint64_t tenant_id,
      int64_t parent_task_id,
      const share::ObSSHAMacroTaskType &task_type,
      uint64_t ls_id,
      uint64_t task_id,
      const share::ObHAResultInfo &result_info,
      const int64_t end_time,
      const ObSSHAMacroBlockScheduleTask *macro_task);

  static int handle_execute_over_retryable_failure_(
      common::ObISQLClient &sql_proxy,
      uint64_t tenant_id,
      int64_t parent_task_id,
      const share::ObSSHAMacroTaskType &task_type,
      uint64_t ls_id,
      uint64_t task_id,
      const ObSSHAMacroBlockScheduleTask *macro_task,
      const share::ObHAResultInfo &result_info,
      const int64_t retry_cnt);

  static int handle_execute_over_non_retryable_failure_(
      ObBackupService &backup_service,
      common::ObISQLClient &sql_proxy,
      uint64_t tenant_id,
      int64_t parent_task_id,
      const share::ObSSHAMacroTaskType &task_type,
      uint64_t ls_id,
      uint64_t task_id,
      const share::ObHAResultInfo &result_info,
      const int64_t retry_cnt);

  static int get_task_retry_count_(
      common::ObISQLClient &sql_proxy,
      uint64_t tenant_id,
      int64_t parent_task_id,
      const share::ObSSHAMacroTaskType &task_type,
      uint64_t ls_id,
      uint64_t task_id,
      int64_t &retry_cnt);

protected:
  // Sliding window parameters
  static const int64_t MAX_CONCURRENT_TASKS = 100;    // Maximum concurrent tasks in scheduler
  static const int64_t REFILL_THRESHOLD = 20;         // Refill when task count below this value 
  static const int64_t TASK_TIMEOUT_THRESHOLD = 30 * 60 * 1000000LL; // 30 minutes timeout

  // Other missing constants and parameters
  static const int64_t MIN_BATCH_SIZE = 1;              // Minimum batch size
  static const int64_t MAX_BATCH_SIZE = 256;
  static const int64_t DEFAULT_MAX_BATCH_BYTES = 256 * 1024 * 1024LL; // Default maximum bytes 256MB
  static const int64_t MAX_RETRY_COUNT = 64;            // Maximum retry times
  static const int64_t MIN_REFILL_INTERVAL = 1000000LL;      // Refill at most once per second

protected:
  bool is_inited_;
  bool is_finished_;
  bool is_failed_;  // is the macro block task failed and should not retry
  uint64_t tenant_id_;
  int64_t parent_task_id_;
  ObSSHAMacroTaskType task_type_;  // Task type enum
  ObBackupTaskScheduler *task_scheduler_;
  common::ObISQLClient *sql_proxy_;

  share::SCN snapshot_;
  
  // Task statistics
  int64_t total_macro_blocks_;
  int64_t finish_macro_blocks_;
  int64_t total_task_count_;                  // Total number of tasks (batch count)
  int64_t tasks_in_scheduler_;           // Current number of tasks in scheduler
  int64_t finish_task_count_;              // Number of completed tasks
  int64_t total_bytes_;                   // Total bytes of all macro blocks
  int64_t finish_bytes_;                  // Finished bytes of completed tasks
  
  // Time control
  int64_t last_refill_time_;

  // Log stream ID array for backup tasks
  common::ObSArray<share::ObLSID> ls_id_array_;

  // Accessors to allow future config-driven tuning
  static inline int64_t max_concurrent_tasks() { return MAX_CONCURRENT_TASKS; }
  static inline int64_t refill_threshold() { return REFILL_THRESHOLD; }
  static inline int64_t task_timeout_threshold() { return TASK_TIMEOUT_THRESHOLD; }
  static inline int64_t min_refill_interval() { return MIN_REFILL_INTERVAL; }

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockTaskMgr);
};

// Task manager for backup scenarios
class ObSSHAMacroBlockBackupTaskMgr : public ObSSHAMacroBlockTaskMgr
{
public:
  ObSSHAMacroBlockBackupTaskMgr();
  virtual ~ObSSHAMacroBlockBackupTaskMgr();

  int init(const ObSSHAMacroTaskType &task_type,
           share::ObBackupJobAttr &job_attr,
           share::ObBackupSetTaskAttr &set_task_attr,
           ObBackupTaskScheduler &task_scheduler,
           common::ObISQLClient &sql_proxy,
           const common::ObIArray<share::ObLSID> &ls_id_array,
           const share::SCN &snapshot,
           ObBackupService &backup_service);

protected:
  virtual int get_backup_dest_(
      share::ObBackupDest &backup_dest) override;
  virtual int build_schedule_task_(
      const share::ObSSHAMacroBlockTaskAttr &task_attr,
      ObSSHAMacroBlockScheduleTask &schedule_task) override;
  virtual BackupJobType get_job_type_() const override;
  virtual void wakeup_service_() override;
  int end_transaction(common::ObMySQLTransaction &trans, const int upstream_ret) override;
  int init_stage_write_macro_block_list_() override;

private:
  int for_each_macro_block_task_by_ls_ids_();

private:
  share::ObBackupJobAttr job_attr_;
  share::ObBackupSetTaskAttr set_task_attr_;
  ObBackupService *backup_service_;

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockBackupTaskMgr);
};

// Task manager for restore scenarios
class ObSSHAMacroBlockRestoreTaskMgr : public ObSSHAMacroBlockTaskMgr
{
public:
  ObSSHAMacroBlockRestoreTaskMgr();
  virtual ~ObSSHAMacroBlockRestoreTaskMgr();

  int init(const ObSSHAMacroTaskType &task_type,
           share::ObBackupJobAttr &job_attr,
           share::ObBackupSetTaskAttr &set_task_attr,
           ObBackupTaskScheduler &task_scheduler,
           common::ObISQLClient &sql_proxy,
           ObRestoreService &restore_service,
           const common::ObIArray<share::ObLSID> &ls_id_array);

protected:
  virtual int get_backup_dest_(
      share::ObBackupDest &backup_dest) override;
  virtual int build_schedule_task_(
      const share::ObSSHAMacroBlockTaskAttr &task_attr,
      ObSSHAMacroBlockScheduleTask &schedule_task) override;
  virtual BackupJobType get_job_type_() const override;
  virtual void wakeup_service_() override;
  int end_transaction(common::ObMySQLTransaction &trans, const int upstream_ret) override;
private:
  share::ObBackupJobAttr job_attr_;
  share::ObBackupSetTaskAttr set_task_attr_;
  ObRestoreService *restore_service_;

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockRestoreTaskMgr);
};

// Task manager for backup clean scenarios
class ObSSHAMacroBlockCleanTaskMgr : public ObSSHAMacroBlockTaskMgr
{
public:
  ObSSHAMacroBlockCleanTaskMgr();
  virtual ~ObSSHAMacroBlockCleanTaskMgr();

  int init(const ObSSHAMacroTaskType &task_type,
           const share::ObBackupCleanJobAttr &clean_job_attr,
           const share::ObBackupCleanTaskAttr &clean_task_attr,
           ObBackupTaskScheduler &task_scheduler,
           common::ObISQLClient &sql_proxy,
           ObBackupService &backup_service);

protected:
  virtual int get_backup_dest_(
      share::ObBackupDest &backup_dest) override;
  virtual int build_schedule_task_(
      const share::ObSSHAMacroBlockTaskAttr &task_attr,
      ObSSHAMacroBlockScheduleTask &schedule_task) override;
  virtual BackupJobType get_job_type_() const override;
  virtual void wakeup_service_() override;
  int end_transaction(common::ObMySQLTransaction &trans, const int upstream_ret) override;
private:
  share::ObBackupCleanJobAttr clean_job_attr_;
  share::ObBackupCleanTaskAttr clean_task_attr_;
  ObBackupService *backup_service_;

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockCleanTaskMgr);
};
} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_BACKUP_OB_SS_HA_MACRO_BLOCK_TASK_MGR_H_
