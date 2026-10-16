/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_OPERATOR_H_
#define OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_OPERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace share
{
class ObDMLSqlSplicer;
}
namespace storage
{

class ObSharedGCTaskTableRangeIterator;
class ObSharedGCTaskTableFullIterator;
class ObDeleteTenantTaskIterator;
class ObDeleteServerTaskIterator;

class ObSharedGCTaskTableOperator
{
  friend class ObSharedGCTaskTableRangeIterator;
  friend class ObSharedGCTaskTableFullIterator;
  friend class ObSharedGCTaskLSMetaIterator;
  friend class ObDeleteTenantTaskIterator;
  friend class ObDeleteServerTaskIterator;
public:
  // ===== __all_shared_gc_task =====

  // INSERT
  static int insert_task(
      const ObSharedGCTask &task,
      common::ObISQLClient &client);
  static int batch_insert_tasks(
      const uint64_t tenant_id,
      const ObSharedGCTaskIArray &tasks,
      common::ObISQLClient &client);

  // Read an active task by primary key; deep-copy value and other variable fields into allocator.
  static int get_task(
      const ObSharedGCTaskKey &key,
      common::ObIAllocator &allocator,
      common::ObISQLClient &client,
      ObSharedGCTask &task);

  // MarkSweep GC only. Returns OB_INVALID_ARGUMENT for other types.
  // MIN(round_id) in (tenant_id, type) under the watermark:
  //   round_id <= allow_gc_round_id
  // is_empty=true  : no eligible row exists; min_round_id is undefined.
  // is_empty=false : eligible row found; min_round_id carries MIN(round_id).
  // One timer cycle only drains the rows under this min_round_id; the next
  // cycle will pick up the next smallest round_id, so the worker chain never
  // needs to advance across rounds.
  static int get_min_round_id(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t allow_gc_round_id,
      common::ObISQLClient &client,
      bool &is_empty,
      int64_t &min_round_id);

  // Planner Step 2: MIN/MAX(task_id) for one specific round_id.
  // is_empty=true  : no rows match the round; round_range is undefined.
  // is_empty=false : rows found; round_range carries [MIN(task_id), MAX(task_id)].
  static int get_task_id_range(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t round_id,
      common::ObISQLClient &client,
      bool &is_empty,
      ObSSGCTaskIdRange &round_range);

  // MIN(round_id), MAX(round_id) over the whole (tenant_id, type) bucket, with
  // no upper-bound watermark filter. Intended for unconditional sweeps (e.g.
  // batch_delete) that drain every row regardless of allow_gc_round_id.
  // Accepts any valid ObSharedGCTaskType (MARK_SWEEP_GC or FAIL_GC).
  // is_empty=true  : no rows exist; min/max are undefined.
  // is_empty=false : rows found; min_round_id and max_round_id are set.
  static int get_round_id_range(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      common::ObISQLClient &client,
      bool &is_empty,
      int64_t &min_round_id,
      int64_t &max_round_id);

  // MIN(round_id) over (tenant_id, type) in the given table (active or history), with no
  // watermark filter. Used by the mark-sweep diagnose cleanup to compute the round_id below
  // which diagnostic files on object storage are safe to delete.
  // is_empty=true  : no rows exist; min_round_id is undefined.
  // is_empty=false : rows found; min_round_id is set.
  static int get_min_round_id(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const char *table_name,
      common::ObISQLClient &client,
      bool &is_empty,
      int64_t &min_round_id);

  // UPDATE - 通过写入 server_id/trace_id 认领一行。未认领的行、同 server
  // 重试，以及 owner 已超过 max(两倍 shared GC 扫描周期, 10min) 的行可以被认领。
  // 不同 live owner 返回 OB_ENTRY_EXIST；active 行不存在返回 OB_ENTRY_NOT_EXIST。
  static int update_server_id_and_trace_id(
      const ObSharedGCTaskKey &key,
      const int64_t server_id,
      const common::ObString &trace_id,
      common::ObISQLClient &client);
  // UPDATE - extra_info only
  static int update_extra_info(
      const ObSharedGCTaskKey &key,
      const common::ObString &extra_info,
      common::ObISQLClient &client);

  // DELETE - 按主键删除。行不存在时返回 OB_ENTRY_NOT_EXIST。
  static int delete_task(
      const ObSharedGCTaskKey &key,
      common::ObISQLClient &client);

  // 将一行从 __all_shared_gc_task 原子迁移到 __all_shared_gc_task_history。
  // 在解析后的真实租户上用单事务包裹 DELETE + INSERT。容忍并发完成：
  //   * DELETE 遇 OB_ENTRY_NOT_EXIST（peer 已删）-> 跳过 history INSERT，
  //     事务空提交；
  //   * INSERT 遇 OB_ERR_PRIMARY_KEY_DUPLICATE（history 已被 peer 写入）->
  //     视为成功，DELETE 照常提交。
  static int delete_task_and_insert_history(
      const ObSharedGCTask &task,
      common::ObMySQLProxy &sql_proxy);

  // 批量版本：单事务内对所有 task 做 DELETE + INSERT INTO history。所有 task
  // 必须属于同一 tenant_id。若并发 Executor 已接管，某些行的 affected_rows
  // 可能小于 tasks.count()，整批仍然提交。
  // History SQL is split into bounded statements within the same transaction.
  static int batch_delete_tasks_and_insert_histories(
      const uint64_t tenant_id,
      const ObSharedGCTaskIArray &tasks);

  // ===== __all_shared_gc_task_history =====

  // INSERT。同 key 的 history 行已存在时返回 OB_ERR_PRIMARY_KEY_DUPLICATE。
  static int insert_history(
      const ObSharedGCTask &task,
      common::ObISQLClient &client);

  // SELECT - by primary key (single row)
  static int get_history(
      const ObSharedGCTaskKey &key,
      common::ObIAllocator &allocator,
      common::ObISQLClient &client,
      ObSharedGCTask &task);

  // DELETE - by primary key
  static int delete_history(
      const ObSharedGCTaskKey &key,
      common::ObISQLClient &client);

  // Delete up to `limit` rows from `table_name` whose gmt_create < usec_to_time(expire_ts_us).
  // Only acts on the calling tenant's rows (WHERE tenant_id = ?).
  // Callers are responsible for looping until affected_rows < limit.
  // `table_name` must be OB_ALL_SHARED_GC_TASK_TNAME or OB_ALL_SHARED_GC_TASK_HISTORY_TNAME.
  static int batch_delete_expired(
      const uint64_t tenant_id,
      const char *table_name,
      const int64_t expire_ts_us,
      const int64_t limit,
      common::ObISQLClient &client,
      int64_t &affected_rows);

  // Unconditional sweep of __all_shared_gc_task: deletes every row owned by
  // tenant_id across all ObSharedGCTaskType buckets (MARK_SWEEP_GC and
  // FAIL_GC). Mirrors the ObMarkSweepPlanTask drain pattern but ignores the
  // allow_gc_round_id watermark. Steps:
  //   1. For each type, resolve [MIN(round_id), MAX(round_id)] via
  //      get_round_id_range.
  //   2. For each round_id in the closed range, fetch its task_id range and
  //      delete it in DEFAULT_SLICE_SIZE-sized slices using
  //      task_id BETWEEN ? AND ?.
  //   3. Check `is_stop` before/after every slice; on stop, return
  //      OB_CANCELED. Slices already committed are left as-is — each slice is
  //      its own auto-commit DELETE.
  // Empty rounds are skipped silently (a concurrent worker may have drained
  // them between the planning and execute step).
  static int batch_delete(
      const uint64_t tenant_id,
      const volatile bool &is_stop);

private:
  // A task and its statistics rows must fit both limits. Validate task-size and
  // batch-limit changes with the maximum-input history batch test.
  static constexpr int64_t MAX_HISTORY_BATCH_ROW_COUNT = 100;
  static constexpr int64_t MAX_HISTORY_BATCH_SQL_LENGTH = 2 * 1024 * 1024;

  // Append one encoded row and release its DML memory before returning.
  static int build_history_values_sql_(
      const ObSharedGCTask &task,
      common::ObSqlString &values);
  // Build one bounded INSERT using the task tenant's current data version.
  // Advance task_idx only after successful construction.
  static int build_batch_insert_histories_sql_(
      const ObSharedGCTaskIArray &tasks,
      int64_t &task_idx,
      common::ObSqlString &sql,
      int64_t &history_row_count);
  static int batch_insert_histories_(
      const uint64_t real_tenant_id,
      const ObSharedGCTaskIArray &tasks,
      common::ObISQLClient &client,
      int64_t &affected_rows);

  // Resolve the SQL execution tenant for a shared GC table operation:
  //   user tenant  -> meta tenant
  //   meta tenant  -> sys tenant
  //   sys tenant   -> sys tenant
  // Returns OB_ERR_UNEXPECTED for unknown tenant categories.
  static int get_real_tenant_id_(
      const uint64_t tenant_id,
      uint64_t &real_tenant_id);
  static int insert_into_table_(
      const char *table_name,
      const ObSharedGCTask &task,
      common::ObISQLClient &client);
  static int delete_from_table_by_key_(
      const char *table_name,
      const ObSharedGCTaskKey &key,
      common::ObISQLClient &client);
  static int get_one_row_by_key_(
      const char *table_name,
      const ObSharedGCTaskKey &key,
      common::ObIAllocator &allocator,
      common::ObISQLClient &client,
      ObSharedGCTask &task);
  // Keep the column order in sync with the header in build_batch_insert_histories_sql_().
  // Add absent optional values as NULL so all task and statistics rows have the same columns.
  static int fill_dml_(
      share::ObDMLSqlSplicer &dml,
      const ObSharedGCTask &task);
  static int build_mark_sweep_gc_stat_row_sql_(
      const ObSharedGCTask &task,
      const ObSharedGCTaskType stat_type,
      const int64_t macro_block_count,
      common::ObSqlString &values);
  static int build_mark_sweep_gc_stat_sql_(
      const ObSharedGCTask &task,
      int64_t &stat_row_count,
      common::ObSqlString &values);
  static int fill_pk_dml_(
      share::ObDMLSqlSplicer &dml,
      const ObSharedGCTaskKey &key);
  static int parse_row_(
      common::sqlclient::ObMySQLResult &result,
      common::ObIAllocator &allocator,
      ObSharedGCTask &task);
  static int parse_row_(
      common::sqlclient::ObMySQLResult &result,
      common::ObIAllocator &allocator,
      ObSharedGCTask &task,
      int64_t &gmt_create,
      int64_t &gmt_modified);
  static int build_get_min_round_id_sql_(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t allow_gc_round_id,
      common::ObSqlString &sql);
  static int read_min_round_id_(
      common::ObISQLClient &client,
      const uint64_t real_tenant_id,
      const common::ObSqlString &sql,
      bool &is_empty,
      int64_t &min_round_id);
  static int build_get_task_id_range_sql_(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t round_id,
      common::ObSqlString &sql);
  static int read_task_id_range_(
      common::ObISQLClient &client,
      const uint64_t real_tenant_id,
      const common::ObSqlString &sql,
      bool &is_empty,
      ObSSGCTaskIdRange &round_range);
  static int build_get_round_id_range_sql_(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      common::ObSqlString &sql);
  static int read_round_id_range_(
      common::ObISQLClient &client,
      const uint64_t real_tenant_id,
      const common::ObSqlString &sql,
      bool &is_empty,
      int64_t &min_round_id,
      int64_t &max_round_id);
  // DELETE one slice from __all_shared_gc_task by
  // (tenant_id, type, round_id, task_id BETWEEN start AND end).
  static int batch_delete_slice_(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t round_id,
      const ObSSGCTaskIdRange &slice_range,
      int64_t &affected_rows);
  static int get_sql_timeout_us_(const uint64_t tenant_id, int64_t &timeout_us);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_OPERATOR_H_
