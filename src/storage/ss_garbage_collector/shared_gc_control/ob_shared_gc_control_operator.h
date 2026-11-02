/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_OPERATOR_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_OPERATOR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/scn.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_define.h"

namespace oceanbase
{
namespace storage
{

/** Operator of system table __all_shared_gc_control (rowkey: tenant_id).
 *
 * Tenant routing:
 *   sys tenant  -> SQL executes in SYS tenant
 *   meta tenant -> SQL executes in SYS tenant
 *   user tenant -> SQL executes in the corresponding meta tenant
 * The `tenant_id` parameter on each interface is the business rowkey written into
 * the row, not the SQL execution tenant; routing is transparent to the caller.
 *
 * Server-Condition update_* interfaces (update_row / update_allow_gc_round_id /
 * update_status / update_gc_meta_scn / update_last_succ_gc_meta_scn) append
 * `AND executor_server_id = <cur_server_id>` to the WHERE clause, where
 * `cur_server_id = GCTX.get_server_id()`.
 *
 * `update_executor_state` is the Non-Server-Condition takeover interface; the caller
 * must hold the row lock via `select_tenant_id_for_update` inside an ObMySQLTransaction first
 * and pass the same trans as the `sql_client` argument to the takeover interface.
 */
class ObSharedGCControlOperator final
{
public:
  static int insert_row(common::ObISQLClient &sql_client,
                        const ObSharedGCControlInfo &info,
                        int64_t &affected_rows);
  static int insert_default_row(common::ObISQLClient &sql_client,
                                const uint64_t tenant_id,
                                int64_t &affected_rows);
  static int delete_row(common::ObISQLClient &sql_client,
                        const uint64_t tenant_id,
                        int64_t &affected_rows);
  static int read_row(common::ObISQLClient &sql_client,
                      const uint64_t tenant_id,
                      ObSharedGCControlInfo &info);
  static int read_row(common::ObISQLClient &sql_client,
                      const uint64_t tenant_id,
                      ObSharedGCControlInfo &info,
                      int64_t &gmt_create,
                      int64_t &gmt_modified);

  // Server-Condition update: WHERE tenant_id = ? AND executor_server_id = cur_server_id
  static int update_row(common::ObISQLClient &sql_client,
                        const ObSharedGCControlInfo &info,
                        int64_t &affected_rows);
  // Advance allow_gc_round_id under a self-managed transaction. After locking the row via
  // select_tenant_id_for_update, the update happens ONLY when both the row's executor_server_id
  // equals cur_server_id AND the row's gc_round_id equals the passed round_id.
  // if condition match, allow_gc_round_id will be updated. allow_gc_round_id will never falls back.
  // if round_id <= the row's current allow_gc_round_id, unexpected.
  // if condition mismatch, allow_gc_round_id will not be updated, and affected_rows is 0
  // and ret stays OB_SUCCESS.
  static int update_allow_gc_round_id(common::ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      const int64_t round_id,
                                      int64_t &affected_rows);
  static int update_status(common::ObISQLClient &sql_client,
                           const uint64_t tenant_id,
                           const ObSharedGCControlStatus status,
                           int64_t &affected_rows);

  // Non-Server-Condition takeover update. Caller must hold row lock via select_tenant_id_for_update
  // in the same ObMySQLTransaction passed as sql_client here. Also writes gc_round_id, which is
  // fetched by the caller at the moment it acquires/takes-over the executor role.
  static int update_executor_state(common::ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   const int64_t executor_server_id,
                                   const ObSharedGCControlStatus status,
                                   const int64_t gc_round_id,
                                   int64_t &affected_rows);

  // Monotonically advance gc_meta_scn / last_succ_gc_meta_scn (never roll back).
  // Self-managed transaction: select_tenant_id_for_update -> only UPDATE when the row's
  // executor_server_id equals cur_server_id (= GCTX.get_server_id()) AND the row's
  // gc_round_id equals the passed round_id AND new_scn is larger than the current value.
  // This guards against a stale dag (left over from a previous round / before a
  // switch-leader) advancing the watermark after another server / round has taken over.
  // On executor/round_id mismatch it is a success no-op (ret OB_SUCCESS, is_updated false).
  // `is_updated` is set to true only when an UPDATE actually advanced the column; it stays
  // false on every other path (invalid arg, row not exist, executor/round_id mismatch,
  // monotonic no-op, etc.).
  static int update_gc_meta_scn(common::ObISQLClient &sql_client,
                                const uint64_t tenant_id,
                                const int64_t round_id,
                                const share::SCN &new_gc_meta_scn,
                                bool &is_updated);
  // Same monotonic, executor/round-guarded gc_meta_scn advance as update_gc_meta_scn,
  // but runs inside the caller's already-started transaction (the caller commits).
  // Re-locks the row via select_tenant_id_for_update; reentrant when the caller
  // already holds the lock. On executor/round_id mismatch or a non-larger new_scn it
  // is a success no-op (is_updated false).
  static int update_gc_meta_scn_in_trans(common::ObMySQLTransaction &trans,
                                         const uint64_t tenant_id,
                                         const int64_t round_id,
                                         const share::SCN &new_gc_meta_scn,
                                         bool &is_updated);
  static int update_last_succ_gc_meta_scn(common::ObISQLClient &sql_client,
                                          const uint64_t tenant_id,
                                          const int64_t round_id,
                                          const share::SCN &new_last_succ_gc_meta_scn);

  // Lock the row exclusively for the duration of the surrounding transaction.
  // Returns OB_ENTRY_NOT_EXIST if no row matches.
  static int select_tenant_id_for_update(common::ObISQLClient &sql_client,
                                         const uint64_t tenant_id,
                                         ObSharedGCControlInfo &info);

private:
  enum class GCMetaScnColumn : uint8_t
  {
    GC_META_SCN = 0,
    LAST_SUCC_GC_META_SCN = 1
  };
  // The __all_shared_gc_control column name backing each GCMetaScnColumn.
  // Returns nullptr for an unknown column; callers must treat that as an error.
  static const char *gc_meta_scn_column_to_str(const GCMetaScnColumn column);

  // Self-managed-transaction wrapper: starts a trans, then delegates to
  // update_scn_in_trans_, then commits.
  static int update_gc_meta_scn_(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const int64_t round_id,
                                 const share::SCN &new_scn,
                                 const GCMetaScnColumn column,
                                 bool &is_updated);
  // Core monotonic advance inside an already-started transaction: lock the row,
  // verify executor_server_id == cur_server_id AND gc_round_id == round_id, then
  // UPDATE only when new_scn is strictly larger than the current column value.
  static int update_scn_in_trans_(common::ObMySQLTransaction &trans,
                                  const uint64_t tenant_id,
                                  const int64_t round_id,
                                  const share::SCN &new_scn,
                                  const GCMetaScnColumn column,
                                  bool &is_updated);
  static int update_gc_meta_scn_(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const share::SCN gc_meta_scn,
                                 int64_t &affected_rows);
  static int update_last_succ_gc_meta_scn_(common::ObISQLClient &sql_client,
                                           const uint64_t tenant_id,
                                           const share::SCN last_succ_gc_meta_scn,
                                           int64_t &affected_rows);
  static int update_allow_gc_round_id_(common::ObISQLClient &sql_client,
                                       const uint64_t tenant_id,
                                       const int64_t allow_gc_round_id,
                                       int64_t &affected_rows);
  static int get_real_tenant_id_(const uint64_t tenant_id, uint64_t &real_tenant_id);
  static int do_sql_write_(common::ObISQLClient &sql_client,
                           const uint64_t real_tenant_id,
                           const char *sql,
                           int64_t &affected_rows);
  static int do_sql_read_(common::ObISQLClient &sql_client,
                          const uint64_t real_tenant_id,
                          const char *sql,
                          common::ObMySQLProxy::MySQLResult &result);
  static int do_update_set_clause_(common::ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   const common::ObSqlString &set_clause,
                                   const bool with_server_condition,
                                   int64_t &affected_rows);
  static int parse_result_row_(common::ObMySQLProxy::MySQLResult &result,
                               ObSharedGCControlInfo &info,
                               int64_t &gmt_create,
                               int64_t &gmt_modified);

  ObSharedGCControlOperator() = delete;
  ~ObSharedGCControlOperator() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCControlOperator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_OPERATOR_H_
