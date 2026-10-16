/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_GC_TASK_UTIL_H_
#define OCEANBASE_STORAGE_OB_SHARED_GC_TASK_UTIL_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace storage
{

// ===========================================================================
// __all_shared_gc_task / __all_shared_gc_task_history 辅助工具。
// 无状态，所有成员均为静态成员。
//
// Shared-data GC 任务 ID 通过 GCTX.sql_proxy_ 在独立提交的事务中分配
//（ObMaxIdFetcher）。租户/Server 删除任务使用目标 ID 作为确定性 task_id。
// 每行 INSERT 均通过调用方传入的 client 执行，因此调用方可以将其纳入更大的事务。
// ===========================================================================
class ObSharedGCTaskUtil
{
public:
  // Fetch a new task_id and insert one MarkSweep GC task row into
  // __all_shared_gc_task for (tenant_id, round_id).
  static int generate_mark_sweep_gc_task(
      const uint64_t tenant_id,
      const int64_t round_id,
      const ObSharedGCMacroIdList &macro_id_list,
      common::ObISQLClient &client);
  // Register a round marker with task_id=0 and an empty value before Study; duplicate keys succeed.
  // The caller must commit the client transaction before uploading diagnose objects.
  static int generate_mark_sweep_gc_diag_task(
      const uint64_t tenant_id,
      const int64_t round_id,
      common::ObISQLClient &client);
  static int generate_fail_gc_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key,
      const ObSSMetaCtrlValue &ctrl_value,
      common::ObISQLClient &client);
  // 将 FAIL_GC 检测并删除的宏块 ID 记录为 __all_shared_gc_task_history 中的一行FAIL_GC_MACRO_ID 历史记录。
  // 该行主键为 (tenant_id, FAIL_GC_MACRO_ID, round_id=0, task_id=由 ObMaxIdFetcher 分配的新唯一 ID)，
  // extra_info 中记录 "fail_gc_task_id=<fail_gc_task_id>"，用于关联源 FAIL_GC 任务。
  // 调用方负责将过大的宏块 ID 列表拆分为多次调用，并保证每次生成的 value 小于 ObSharedGCTask::MAX_VALUE_LENGTH。
  static int generate_fail_gc_macro_id_history(
      const uint64_t tenant_id,
      const int64_t fail_gc_task_id,
      const ObSharedGCMacroIdList &macro_id_list,
      common::ObISQLClient &client);
  // Fetch a new task_id and insert one LS Meta GC task row into
  // __all_shared_gc_task for one stale ls_meta version (ls_id, meta_scn).
  static int generate_ls_meta_gc_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn,
      common::ObISQLClient &client);
  static int generate_ls_ckpt_gc_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn,
      common::ObISQLClient &client);

  // 向 SYS 租户的 __all_shared_gc_task 插入确定性的 DELETE_TENANT 任务。
  // key.tenant_id_ 为 OB_SYS_TENANT_ID，task_id_ 为目标租户 ID。调用方可传入
  // SYS 租户事务，使任务生成和租户元数据删除原子提交。
  static int generate_delete_tenant_task(
      const uint64_t target_tenant_id,
      const int64_t delete_ts,
      common::ObISQLClient &client);

  // 向 SYS 租户的 __all_shared_gc_task 插入确定性的 DELETE_SERVER 任务。
  // task_id_ 为目标 Server ID。
  static int generate_delete_server_task(
      const uint64_t target_server_id,
      const common::ObAddr &server_addr,
      const int64_t delete_ts,
      common::ObISQLClient &client);

private:
  static int fetch_task_id_(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      int64_t &task_id);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SHARED_GC_TASK_UTIL_H_
