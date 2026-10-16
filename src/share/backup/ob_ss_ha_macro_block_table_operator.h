/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_TABLE_OPERATOR_H_

#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/function/ob_function.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace rootserver
{
class ObBackupBaseService;
} // namespace rootserver
namespace share
{

// Interface for transaction end with check_leader. Implemented by MacroBlockTaskMgr
class ObHATransEndHelper
{
public:
  ObHATransEndHelper() = default;
  virtual ~ObHATransEndHelper() = default;
  virtual int end_transaction(common::ObMySQLTransaction &trans, int upstream_ret) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHATransEndHelper);
};

struct ObSSHAMacroTaskRowKey final
{
  ObSSHAMacroTaskRowKey();
  void reset();
  void reuse();
  TO_STRING_KV(K_(tenant_id), K_(parent_task_id), K_(task_type), K_(ls_id), K_(task_id));
  uint64_t tenant_id_;
  int64_t parent_task_id_;
  ObSSHAMacroTaskType task_type_;
  int64_t ls_id_;
  int64_t task_id_;
};

// Macro block task internal table operator
class ObSSHAMacroBlockTaskOperator final
{
public:
  using TaskProcessor = common::ObFunction<int(
      const share::ObSSHAMacroBlockBatch &)>;

  static const int64_t DELETE_BATCH_SIZE = 100;

  ObSSHAMacroBlockTaskOperator() = default;
  ~ObSSHAMacroBlockTaskOperator() = default;

  // Batch insert macro block task records
  static int batch_insert_macro_block_tasks(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObIArray<ObSSHAMacroBlockTaskAttr> &task_attrs);

  static int mark_task_pending_if_init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t ls_id,
      const int64_t task_id,
      const int64_t expected_retry_cnt);

  // Update task for retry with SELECT FOR UPDATE to ensure accurate retry count
  static int update_task_for_retry_with_lock(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t ls_id,
      const int64_t task_id,
      const common::ObString &task_status);
  
  // Get retry count from database
  static int get_task_retry_count(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t ls_id,
      const int64_t task_id,
      int64_t &retry_cnt);

  // Update task execution server
  static int update_dst_and_status(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t task_id,
      const int64_t ls_id,
      const common::ObAddr &svr_addr,
      const ObBackupTaskStatus &status,
      share::ObTaskId task_trace_id);

  // Update task completion and progress in transaction
  static int update_task_completion_with_progress(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t ls_id,
      const int64_t task_id,
      const int64_t end_time,
      const int64_t result,
      const common::ObString &comment);
  
  // Get all unfinished macro block tasks
  static int get_pending_or_doing_tasks(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks);

  // Get macro block tasks by specific status
  static int get_tasks_by_status_limit(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const ObBackupTaskStatus &status,
      const int64_t limit,
      common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks);

  // Get task statistics
  static int get_task_statistics(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      int64_t &total_tasks,
      int64_t &finished_tasks,
      int64_t &total_macro_blocks,
      int64_t &finished_macro_blocks,
      int64_t &total_bytes,
      int64_t &finish_bytes);

  // Check if all tasks are finished (no non-FINISH task exists in task table)
  static int check_all_tasks_finished(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      bool &is_finished);

  // Delete all macro block tasks batch by batch. Each batch in its own transaction.
  // When trans_end_helper is non-null, uses trans_end_helper->end_transaction() for
  // check_leader protection; otherwise uses trans.end() directly.
  static int delete_tasks(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      ObHATransEndHelper *trans_end_helper);

  // Get next available task ID
  static int get_next_task_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      int64_t &task_id);

  // Manager state persistence related methods
  static int save_mgr_state(
      common::ObISQLClient &sql_proxy,
      const ObSSHAMacroTaskMgrState &mgr_state);

  static int load_mgr_state(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      ObSSHAMacroTaskMgrState &mgr_state);

  // Load manager state with FOR UPDATE lock for dual-master concurrency protection
  static int load_mgr_state_for_update(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      ObSSHAMacroTaskMgrState &mgr_state);

  // Load manager state with FOR UPDATE lock and verify expected status
  static int load_and_verify_mgr_state_for_update(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t expected_status,
      ObSSHAMacroTaskMgrState &mgr_state);

  static int insert_mgr_state_init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t total_task_count);

  static int move_mgr_to_his(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type);

  static int load_mgr_state_from_history(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      ObSSHAMacroTaskMgrState &mgr_state);

  // Load total_bytes and finish_bytes from history table
  static int load_finish_bytes_from_history(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      int64_t &total_bytes,
      int64_t &finish_bytes);

  // Delete manager state from progress table (for cancel scenario, allows not exist)
  static int delete_mgr_state(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type);

  // Update progress table result and comment
  static int update_progress_result(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t result,
      const common::ObString &comment);

  // Update tasks table result and comment for all tasks
  static int update_tasks_result(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const share::ObLSID &ls_id,
      const int64_t task_id,
      const ObSSHAMacroTaskType &task_type,
      const int64_t result,
      const common::ObString &comment);

  static int for_each_macro_block_task_for_ls(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const share::ObLSID &ls_id,
      const int64_t batch_size,
      const TaskProcessor &processor);

  // Get end key for next batch of tasks to delete (returns OB_ITER_END when no more)
  static int get_next_end_key_for_remove(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const ObSSHAMacroTaskRowKey &prev_row_key,
      ObSSHAMacroTaskRowKey &next_row_key);

  // Delete a batch of tasks between two row keys (should be called within a transaction)
  static int batch_remove_tasks(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObSSHAMacroTaskRowKey &left_row_key,
      const ObSSHAMacroTaskRowKey &right_row_key);

private:
  // Internal helper methods
  static int fill_task_attr_from_result_(
      sqlclient::ObMySQLResult &result,
      ObSSHAMacroBlockTaskAttr &task_attr);
  static int fetch_macro_block_tasks_for_ls_(
      common::ObISQLClient &sql_proxy,
      const uint64_t exec_tenant_id,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const ObSSHAMacroTaskType &task_type,
      const share::ObLSID &ls_id,
      const int64_t batch_size,
      const int64_t last_task_id,
      common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks);
  static int parse_macro_block_ids_(
      const ObSSHAMacroBlockTaskAttr &task_attr,
      common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockTaskOperator);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_BACKUP_OB_SS_HA_MACRO_BLOCK_TABLE_OPERATOR_H_
